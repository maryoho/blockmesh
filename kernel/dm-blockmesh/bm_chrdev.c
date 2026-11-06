// SPDX-License-Identifier: GPL-2.0
/* Node-wide /dev/blockmesh endpoint and per-DM-Target queue registry. */
#include <linux/atomic.h>
#include <linux/byteorder/generic.h>
#include <linux/mm.h>
#include <linux/compat.h>
#include <linux/fs.h>
#include <linux/kref.h>
#include <linux/miscdevice.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/poll.h>
#include <linux/random.h>
#include <linux/slab.h>
#include <linux/uaccess.h>
#include <linux/vmalloc.h>
#include <linux/wait.h>
#include <linux/xarray.h>
#include "blockmesh_uapi.h"
#include "bm_chrdev.h"

static_assert(sizeof(struct bm_uapi_ring_ctrl) == BM_UAPI_RING_CTRL_SIZE);
static_assert(sizeof(struct bm_uapi_request) == BM_UAPI_REQUEST_SIZE);
static_assert(sizeof(struct bm_uapi_completion) == BM_UAPI_COMPLETION_SIZE);
static_assert(sizeof(struct bm_uapi_mapping) == BM_UAPI_MAPPING_SIZE);
static_assert(sizeof(struct bm_uapi_setup) == 80);
static_assert(sizeof(struct bm_uapi_queue_query) == 40);
static_assert(sizeof(struct bm_uapi_cancel) == 40);
static_assert(sizeof(struct bm_uapi_abi_info) == 64);

struct bm_service_session {
	u64 id;
	u64 registry_seen;
};
struct bm_queue_mem {
	struct kref ref;
	void *area;
	size_t bytes;
};
struct bm_queue {
	struct kref ref; /* registry reference plus transient ioctl/mmap lookups */
	struct mutex lock;
	wait_queue_head_t wait;
	u32 id;
	u32 ring_order;
	u64 generation;
	u64 target_cookie;
	bool attached;
	bool dead;
	struct bm_queue_mem *mem; /* current service generation only */
};

static DEFINE_XARRAY(bm_queues);
static DEFINE_MUTEX(bm_queues_lock);
static DECLARE_WAIT_QUEUE_HEAD(bm_poll_wait);
static atomic_t bm_service_open = ATOMIC_INIT(0);
static atomic64_t bm_session_generation = ATOMIC64_INIT(0);
static atomic64_t bm_registry_generation = ATOMIC64_INIT(1);
static u32 bm_next_queue_id = 1; /* monotonic; removed IDs are never reused */

static void bm_mem_release(struct kref *ref)
{
	struct bm_queue_mem *mem = container_of(ref, struct bm_queue_mem, ref);
	vfree(mem->area);
	kfree(mem);
}
static void bm_mem_put(struct bm_queue_mem *mem)
{
	if (mem)
		kref_put(&mem->ref, bm_mem_release);
}
static void bm_queue_release(struct kref *ref)
{
	struct bm_queue *q = container_of(ref, struct bm_queue, ref);
	bm_mem_put(q->mem);
	kfree(q);
}
static struct bm_queue *bm_queue_lookup(u32 id)
{
	struct bm_queue *q;
	mutex_lock(&bm_queues_lock);
	q = xa_load(&bm_queues, id);
	if (q && !kref_get_unless_zero(&q->ref))
		q = NULL;
	mutex_unlock(&bm_queues_lock);
	return q;
}

static int bm_layout(u32 order, size_t *total)
{
	u64 entries, payload, bytes;
	if (order < BM_UAPI_MIN_RING_ORDER || order > BM_UAPI_MAX_RING_ORDER)
		return -EINVAL;
	entries = 1ULL << order;
	payload = entries * BM_UAPI_MAX_BATCH * BM_UAPI_MAPPING_SIZE;
	bytes = BM_UAPI_MMAP_PAYLOAD + payload;
	bytes = PAGE_ALIGN(bytes);
	if (bytes > SIZE_MAX)
		return -EOVERFLOW;
	*total = (size_t)bytes;
	return 0;
}
static void bm_init_ring_ctrl(struct bm_queue *q, u64 offset, u32 entries)
{
	struct bm_uapi_ring_ctrl *ctrl;
	ctrl = (void *)((char *)q->mem->area + offset);
	memset(ctrl, 0, sizeof(*ctrl));
	WRITE_ONCE(ctrl->ring_size, cpu_to_le32(entries));
	WRITE_ONCE(ctrl->generation, cpu_to_le64(q->generation));
}

int bm_queue_create(u32 *queue_id, u64 *target_cookie)
{
	struct bm_queue *q;
	int ret;
	if (!queue_id || !target_cookie)
		return -EINVAL;
	q = kzalloc(sizeof(*q), GFP_KERNEL);
	if (!q)
		return -ENOMEM;
	kref_init(&q->ref); /* xarray owns this reference */
	mutex_init(&q->lock);
	init_waitqueue_head(&q->wait);
	q->generation = 1;
	do {
		q->target_cookie = get_random_u64();
	} while (!q->target_cookie);

	mutex_lock(&bm_queues_lock);
	if (!bm_next_queue_id || bm_next_queue_id > BM_UAPI_MAX_QUEUE_ID) {
		ret = -ENOSPC;
		goto err_unlock;
	}
	q->id = bm_next_queue_id++;
	ret = xa_err(xa_store(&bm_queues, q->id, q, GFP_KERNEL));
	if (ret)
		goto err_unlock;
	*queue_id = q->id;
	*target_cookie = q->target_cookie;
	atomic64_inc(&bm_registry_generation);
	mutex_unlock(&bm_queues_lock);
	wake_up_all(&bm_poll_wait);
	return 0;
err_unlock:
	mutex_unlock(&bm_queues_lock);
	kref_put(&q->ref, bm_queue_release);
	return ret;
}

void bm_queue_destroy(u32 queue_id, u64 target_cookie)
{
	struct bm_queue *q;
	struct bm_queue_mem *old_mem;
	mutex_lock(&bm_queues_lock);
	q = xa_load(&bm_queues, queue_id);
	if (!q || q->target_cookie != target_cookie) {
		mutex_unlock(&bm_queues_lock);
		return;
	}
	xa_erase(&bm_queues, queue_id);
	mutex_lock(&q->lock);
	q->dead = true;
	q->attached = false;
	old_mem = q->mem;
	q->mem = NULL;
	mutex_unlock(&q->lock);
	mutex_unlock(&bm_queues_lock);
	bm_mem_put(old_mem);
	wake_up_all(&q->wait);
	atomic64_inc(&bm_registry_generation);
	wake_up_all(&bm_poll_wait);
	kref_put(&q->ref, bm_queue_release);
}

static int bm_open(struct inode *inode, struct file *file)
{
	unsigned long id;
	struct bm_queue *q;
	struct bm_service_session *session;
	(void)inode;
	session = kzalloc(sizeof(*session), GFP_KERNEL);
	if (!session)
		return -ENOMEM;
	if (atomic_cmpxchg(&bm_service_open, 0, 1)) {
		kfree(session);
		return -EBUSY;
	}

	/* Preflight first: do not partially advance generations on overflow. */
	mutex_lock(&bm_queues_lock);
	xa_for_each(&bm_queues, id, q) {
		mutex_lock(&q->lock);
		if (q->generation == U64_MAX) {
			mutex_unlock(&q->lock);
			mutex_unlock(&bm_queues_lock);
			atomic_set(&bm_service_open, 0);
			kfree(session);
			return -EOVERFLOW;
		}
		mutex_unlock(&q->lock);
	}
	xa_for_each(&bm_queues, id, q) {
		struct bm_queue_mem *old_mem;
		mutex_lock(&q->lock);
		q->generation++;
		q->attached = false;
		old_mem = q->mem;
		q->mem = NULL; /* Never reuse pages mapped by a previous service FD. */
		mutex_unlock(&q->lock);
		bm_mem_put(old_mem); /* Old VMAs retain their own memory references. */
	}
	mutex_unlock(&bm_queues_lock);
	session->id = atomic64_inc_return(&bm_session_generation);
	if (!session->id)
		session->id = atomic64_inc_return(&bm_session_generation);
	WRITE_ONCE(session->registry_seen, atomic64_read(&bm_registry_generation));
	file->private_data = session;
	return 0;
}

static int bm_release_file(struct inode *inode, struct file *file)
{
	struct bm_service_session *session = file->private_data;
	unsigned long id;
	struct bm_queue *q;
	(void)inode;
	(void)file;
	mutex_lock(&bm_queues_lock);
	xa_for_each(&bm_queues, id, q) {
		struct bm_queue_mem *old_mem;
		mutex_lock(&q->lock);
		q->attached = false;
		if (q->generation != U64_MAX)
			q->generation++;
		old_mem = q->mem;
		q->mem = NULL;
		mutex_unlock(&q->lock);
		bm_mem_put(old_mem);
		wake_up_all(&q->wait);
	}
	mutex_unlock(&bm_queues_lock);
	wake_up_all(&bm_poll_wait);
	atomic_set(&bm_service_open, 0);
	kfree(session);
	file->private_data = NULL;
	return 0;
}

static long bm_ioctl(struct file *file, unsigned int cmd, unsigned long arg)
{
	void __user *up = (void __user *)arg;
	if (!file->private_data)
		return -ENOTCONN;
	switch (cmd) {
	case BM_IOC_GET_ABI: {
		struct bm_uapi_abi_info info = { };
		if (copy_from_user(&info, up, sizeof(info)))
			return -EFAULT;
		if (info.size && info.size != sizeof(info))
			return -EINVAL;
		info.size = sizeof(info);
		info.version = BM_UAPI_VERSION;
		info.request_size = BM_UAPI_REQUEST_SIZE;
		info.completion_size = BM_UAPI_COMPLETION_SIZE;
		info.mapping_size = BM_UAPI_MAPPING_SIZE;
		info.ring_ctrl_size = BM_UAPI_RING_CTRL_SIZE;
		info.max_batch = BM_UAPI_MAX_BATCH;
		info.max_ring_order = BM_UAPI_MAX_RING_ORDER;
		info.features = 0; /* No request submission/cancellation feature yet. */
		memset(info.reserved, 0, sizeof(info.reserved));
		return copy_to_user(up, &info, sizeof(info)) ? -EFAULT : 0;
	}
	case BM_IOC_NEXT_QUEUE: {
		struct bm_uapi_queue_query query;
		struct bm_queue *q;
		unsigned long index;
		if (copy_from_user(&query, up, sizeof(query)))
			return -EFAULT;
		if (query.size != sizeof(query) || query.reserved ||
		    query.after_queue_id > BM_UAPI_MAX_QUEUE_ID)
			return -EINVAL;
		if (!query.after_queue_id) {
			struct bm_service_session *session = file->private_data;
			WRITE_ONCE(session->registry_seen, atomic64_read(&bm_registry_generation));
		}
		index = (unsigned long)query.after_queue_id + 1;
		mutex_lock(&bm_queues_lock);
		q = xa_find(&bm_queues, &index, BM_UAPI_MAX_QUEUE_ID, XA_PRESENT);
		if (!q) {
			mutex_unlock(&bm_queues_lock);
			return -ENOENT;
		}
		mutex_lock(&q->lock);
		query.queue_id = q->id;
		query.ring_order = q->ring_order;
		query.queue_generation = q->generation;
		query.target_cookie = q->target_cookie;
		mutex_unlock(&q->lock);
		mutex_unlock(&bm_queues_lock);
		return copy_to_user(up, &query, sizeof(query)) ? -EFAULT : 0;
	}
	case BM_IOC_SETUP_QUEUE: {
		struct bm_uapi_setup setup;
		struct bm_queue *q;
		struct bm_queue_mem *mem;
		size_t bytes;
		u32 entries;
		int ret;
		if (copy_from_user(&setup, up, sizeof(setup)))
			return -EFAULT;
		if (setup.size != sizeof(setup) || setup.version != BM_UAPI_VERSION ||
		    setup.flags || setup.reserved[0] || setup.reserved[1] ||
		    !setup.queue_id || setup.queue_id > BM_UAPI_MAX_QUEUE_ID)
			return -EINVAL;
		ret = bm_layout(setup.ring_order, &bytes);
		if (ret)
			return ret;
		q = bm_queue_lookup(setup.queue_id);
		if (!q)
			return -ENOENT;
		mutex_lock(&q->lock);
		if (q->dead)
			ret = -ENODEV;
		else if (q->attached)
			ret = -EBUSY;
		else {
			mem = kzalloc(sizeof(*mem), GFP_KERNEL);
			if (!mem)
				ret = -ENOMEM;
			else {
				kref_init(&mem->ref); /* Queue owns one reference. */
				mem->area = vmalloc_user(bytes);
				mem->bytes = bytes;
				if (!mem->area) {
					kfree(mem);
					ret = -ENOMEM;
				} else {
					q->mem = mem;
					q->ring_order = setup.ring_order;
					entries = 1U << setup.ring_order;
					bm_init_ring_ctrl(q, BM_UAPI_MMAP_REQUEST_CTRL, entries);
					bm_init_ring_ctrl(q, BM_UAPI_MMAP_COMP_CTRL, entries);
					q->attached = true;
					ret = 0;
				}
			}
		}
		if (!ret) {
			setup.queue_generation = q->generation;
			setup.request_ctrl_offset = ((u64)q->id << BM_UAPI_MMAP_QUEUE_SHIFT) | BM_UAPI_MMAP_REQUEST_CTRL;
			setup.request_ring_offset = ((u64)q->id << BM_UAPI_MMAP_QUEUE_SHIFT) | BM_UAPI_MMAP_REQUEST_RING;
			setup.completion_ctrl_offset = ((u64)q->id << BM_UAPI_MMAP_QUEUE_SHIFT) | BM_UAPI_MMAP_COMP_CTRL;
			setup.completion_ring_offset = ((u64)q->id << BM_UAPI_MMAP_QUEUE_SHIFT) | BM_UAPI_MMAP_COMP_RING;
			setup.payload_offset = ((u64)q->id << BM_UAPI_MMAP_QUEUE_SHIFT) | BM_UAPI_MMAP_PAYLOAD;
			setup.max_batch = BM_UAPI_MAX_BATCH;
			setup.queue_id = q->id;
		}
		mutex_unlock(&q->lock);
		kref_put(&q->ref, bm_queue_release);
		if (ret)
			return ret;
		return copy_to_user(up, &setup, sizeof(setup)) ? -EFAULT : 0;
	}
	case BM_IOC_RESET_QUEUE: {
		u64 raw;
		struct bm_queue *q;
		if (copy_from_user(&raw, up, sizeof(raw)))
			return -EFAULT;
		if (!raw || raw > BM_UAPI_MAX_QUEUE_ID)
			return -EINVAL;
		q = bm_queue_lookup((u32)raw);
		if (!q)
			return -ENOENT;
		mutex_lock(&q->lock);
		if (!q->attached) {
			mutex_unlock(&q->lock);
			kref_put(&q->ref, bm_queue_release);
			return -ENOTCONN;
		}
		if (q->generation == U64_MAX) {
			mutex_unlock(&q->lock);
			kref_put(&q->ref, bm_queue_release);
			return -EOVERFLOW;
		}
		q->generation++;
		memset(q->mem->area, 0, q->mem->bytes);
		bm_init_ring_ctrl(q, BM_UAPI_MMAP_REQUEST_CTRL, 1U << q->ring_order);
		bm_init_ring_ctrl(q, BM_UAPI_MMAP_COMP_CTRL, 1U << q->ring_order);
		mutex_unlock(&q->lock);
		wake_up_all(&q->wait);
		wake_up_all(&bm_poll_wait);
		kref_put(&q->ref, bm_queue_release);
		return 0;
	}
	case BM_IOC_CANCEL:
		return -EOPNOTSUPP; /* Request registry is introduced with ring submission. */
	default:
		return -ENOTTY;
	}
}

static void bm_mem_vma_open(struct vm_area_struct *vma)
{
	struct bm_queue_mem *mem = vma->vm_private_data;
	kref_get(&mem->ref);
}
static void bm_mem_vma_close(struct vm_area_struct *vma)
{
	struct bm_queue_mem *mem = vma->vm_private_data;
	bm_mem_put(mem);
}
static const struct vm_operations_struct bm_vm_ops = {
	.open = bm_mem_vma_open,
	.close = bm_mem_vma_close,
};

static int bm_mmap(struct file *file, struct vm_area_struct *vma)
{
	u64 encoded, local;
	u32 id;
	size_t length = vma->vm_end - vma->vm_start;
	struct bm_queue *q;
	int ret;
	if (!file->private_data || vma->vm_pgoff > (U64_MAX >> PAGE_SHIFT))
		return -EINVAL;
	encoded = (u64)vma->vm_pgoff << PAGE_SHIFT;
	id = encoded >> BM_UAPI_MMAP_QUEUE_SHIFT;
	local = encoded & BM_UAPI_MMAP_LOCAL_MASK;
	if (!id || id > BM_UAPI_MAX_QUEUE_ID || (local & (PAGE_SIZE - 1)))
		return -EINVAL;
	q = bm_queue_lookup(id);
	if (!q)
		return -ENOENT;
	mutex_lock(&q->lock);
	if (q->dead || !q->attached || !q->mem)
		ret = -ENOTCONN;
	else if (local > q->mem->bytes || length > q->mem->bytes - local)
		ret = -EINVAL;
	else {
		struct bm_queue_mem *mem = q->mem;
		kref_get(&mem->ref); /* Initial VMA reference; fork uses vm_ops.open. */
		vma->vm_flags |= VM_DONTEXPAND | VM_DONTDUMP;
		ret = remap_vmalloc_range(vma, mem->area, local >> PAGE_SHIFT);
		if (!ret) {
			vma->vm_ops = &bm_vm_ops;
			vma->vm_private_data = mem;
		} else {
			bm_mem_put(mem);
		}
	}
	mutex_unlock(&q->lock);
	kref_put(&q->ref, bm_queue_release);
	return ret;
}

static __poll_t bm_poll(struct file *file, poll_table *wait)
{
	struct bm_service_session *session = file->private_data;
	unsigned long id;
	struct bm_queue *q;
	__poll_t mask = 0;
	if (!file->private_data)
		return EPOLLERR;
	poll_wait(file, &bm_poll_wait, wait);
	if (atomic64_read(&bm_registry_generation) != READ_ONCE(session->registry_seen))
		mask |= EPOLLPRI;
	mutex_lock(&bm_queues_lock);
	xa_for_each(&bm_queues, id, q) {
		struct bm_uapi_ring_ctrl *ctrl;
		u32 head, tail;
		mutex_lock(&q->lock);
		if (q->attached && !q->dead && q->mem) {
			ctrl = q->mem->area + BM_UAPI_MMAP_REQUEST_CTRL;
			head = le32_to_cpu((__force __le32)READ_ONCE(ctrl->head));
			tail = le32_to_cpu((__force __le32)READ_ONCE(ctrl->tail));
			if (head != tail)
				mask |= EPOLLIN | EPOLLRDNORM;
		}
		mutex_unlock(&q->lock);
		if (mask & (EPOLLIN | EPOLLRDNORM))
			break;
	}
	mutex_unlock(&bm_queues_lock);
	return mask;
}

static const struct file_operations bm_fops = {
	.owner = THIS_MODULE,
	.open = bm_open,
	.release = bm_release_file,
	.unlocked_ioctl = bm_ioctl,
	.compat_ioctl = compat_ptr_ioctl,
	.mmap = bm_mmap,
	.poll = bm_poll,
	.llseek = no_llseek,
};
static struct miscdevice bm_miscdev = {
	.minor = MISC_DYNAMIC_MINOR,
	.name = "blockmesh",
	.fops = &bm_fops,
	.mode = 0600,
};

int bm_chrdev_init(void)
{
	return misc_register(&bm_miscdev);
}
void bm_chrdev_exit(void)
{
	misc_deregister(&bm_miscdev);
}
