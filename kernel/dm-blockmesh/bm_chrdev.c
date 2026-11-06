// SPDX-License-Identifier: GPL-2.0
/* Node-wide /dev/blockmesh endpoint and per-DM-Target queue registry. */
#include <linux/atomic.h>
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

struct bm_queue {
	struct kref ref;
	struct mutex lock;
	wait_queue_head_t wait;
	u32 id;
	u32 ring_order;
	u64 generation;
	u64 target_cookie;
	bool attached;
	bool dead;
	void *area;
	size_t area_bytes;
};
static DEFINE_XARRAY(bm_queues);
static DEFINE_MUTEX(bm_queues_lock);
static DECLARE_WAIT_QUEUE_HEAD(bm_poll_wait);
static atomic_t bm_service_open = ATOMIC_INIT(0);
static u32 bm_next_queue_id = 1;
static atomic64_t bm_session_generation = ATOMIC64_INIT(0);

static void bm_queue_release(struct kref *ref)
{
	struct bm_queue *q = container_of(ref, struct bm_queue, ref);
	vfree(q->area);
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
	struct bm_uapi_ring_ctrl *ctrl = (void *)((char *)q->area + offset);
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
	do { q->target_cookie = get_random_u64(); } while (!q->target_cookie);

	mutex_lock(&bm_queues_lock);
	if (!bm_next_queue_id || bm_next_queue_id > BM_UAPI_MAX_QUEUE_ID) {
		ret = -ENOSPC; /* IDs are deliberately never reused. */
		goto out_unlock;
	}
	q->id = bm_next_queue_id++;
	ret = xa_err(xa_store(&bm_queues, q->id, q, GFP_KERNEL));
	if (ret)
		goto out_unlock;
	*queue_id = q->id;
	*target_cookie = q->target_cookie;
	mutex_unlock(&bm_queues_lock);
	return 0;
out_unlock:
	mutex_unlock(&bm_queues_lock);
	kref_put(&q->ref, bm_queue_release);
	return ret;
}

void bm_queue_destroy(u32 queue_id, u64 target_cookie)
{
	struct bm_queue *q;
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
	mutex_unlock(&q->lock);
	mutex_unlock(&bm_queues_lock);
	wake_up_all(&q->wait);
	wake_up_all(&bm_poll_wait);
	kref_put(&q->ref, bm_queue_release);
}

static int bm_open(struct inode *inode, struct file *file)
{
	u32 id;
	struct bm_queue *q;
	(void)inode;
	if (atomic_cmpxchg(&bm_service_open, 0, 1))
		return -EBUSY;
	mutex_lock(&bm_queues_lock);
	xa_for_each(&bm_queues, id, q) {
		mutex_lock(&q->lock);
		if (q->generation == U64_MAX) {
			mutex_unlock(&q->lock);
			mutex_unlock(&bm_queues_lock);
			atomic_set(&bm_service_open, 0);
			return -EOVERFLOW;
		}
		q->generation++;
		q->attached = false;
		mutex_unlock(&q->lock);
	}
	mutex_unlock(&bm_queues_lock);
	file->private_data = (void *)(unsigned long)atomic64_inc_return(&bm_session_generation);
	return 0;
}

static int bm_release_file(struct inode *inode, struct file *file)
{
	u32 id;
	struct bm_queue *q;
	(void)inode; (void)file;
	mutex_lock(&bm_queues_lock);
	xa_for_each(&bm_queues, id, q) {
		mutex_lock(&q->lock);
		q->attached = false;
		if (q->generation != U64_MAX)
			q->generation++;
		mutex_unlock(&q->lock);
		wake_up_all(&q->wait);
	}
	mutex_unlock(&bm_queues_lock);
	wake_up_all(&bm_poll_wait);
	atomic_set(&bm_service_open, 0);
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
		info.features = 0; /* No request submission/cancellation feature advertised yet. */
		memset(info.reserved, 0, sizeof(info.reserved));
		return copy_to_user(up, &info, sizeof(info)) ? -EFAULT : 0;
	}
	case BM_IOC_NEXT_QUEUE: {
		struct bm_uapi_queue_query query;
		struct bm_queue *q;
		unsigned long index;
		if (copy_from_user(&query, up, sizeof(query)))
			return -EFAULT;
		if (query.size != sizeof(query) || query.reserved)
			return -EINVAL;
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
		size_t bytes;
		u32 entries;
		int ret;
		if (copy_from_user(&setup, up, sizeof(setup)))
			return -EFAULT;
		if (setup.size != sizeof(setup) || setup.version != BM_UAPI_VERSION ||
		    setup.flags || setup.reserved[0] || setup.reserved[1] ||
		    !setup.queue_id)
			return -EINVAL;
		ret = bm_layout(setup.ring_order, &bytes);
		if (ret)
			return ret;
		q = bm_queue_lookup(setup.queue_id);
		if (!q)
			return -ENOENT;
		mutex_lock(&q->lock);
		if (q->dead) ret = -ENODEV;
		else if (q->attached) ret = -EBUSY;
		else if (q->area) ret = -EALREADY;
		else {
			q->area = vmalloc_user(bytes);
			if (!q->area) ret = -ENOMEM;
			else {
				q->area_bytes = bytes;
				q->ring_order = setup.ring_order;
				entries = 1U << setup.ring_order;
				bm_init_ring_ctrl(q, BM_UAPI_MMAP_REQUEST_CTRL, entries);
				bm_init_ring_ctrl(q, BM_UAPI_MMAP_COMP_CTRL, entries);
				q->attached = true;
				ret = 0;
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
		if (ret) return ret;
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
		if (!q) return -ENOENT;
		mutex_lock(&q->lock);
		if (q->generation == U64_MAX) {
			mutex_unlock(&q->lock);
			kref_put(&q->ref, bm_queue_release);
			return -EOVERFLOW;
		}
		q->generation++;
		if (q->area) {
			memset(q->area, 0, q->area_bytes);
			bm_init_ring_ctrl(q, BM_UAPI_MMAP_REQUEST_CTRL, 1U << q->ring_order);
			bm_init_ring_ctrl(q, BM_UAPI_MMAP_COMP_CTRL, 1U << q->ring_order);
		}
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

static void bm_vma_open(struct vm_area_struct *vma)
{
	struct bm_queue *q = vma->vm_private_data;
	kref_get(&q->ref);
}
static void bm_vma_close(struct vm_area_struct *vma)
{
	struct bm_queue *q = vma->vm_private_data;
	kref_put(&q->ref, bm_queue_release);
}
static const struct vm_operations_struct bm_vm_ops = {
	.open = bm_vma_open,
	.close = bm_vma_close,
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
	if (q->dead || !q->attached || !q->area)
		ret = -ENOTCONN;
	else if (local > q->area_bytes || length > q->area_bytes - local)
		ret = -EINVAL;
	else {
		vma->vm_flags |= VM_DONTEXPAND | VM_DONTDUMP;
		ret = remap_vmalloc_range(vma, q->area, local >> PAGE_SHIFT);
		if (!ret) {
			kref_get(&q->ref); /* Initial VMA reference; fork uses .open. */
			vma->vm_ops = &bm_vm_ops;
			vma->vm_private_data = q;
		}
	}
	mutex_unlock(&q->lock);
	kref_put(&q->ref, bm_queue_release);
	return ret;
}

static __poll_t bm_poll(struct file *file, poll_table *wait)
{
	u32 id;
	struct bm_queue *q;
	__poll_t mask = 0;
	if (!file->private_data)
		return EPOLLERR;
	poll_wait(file, &bm_poll_wait, wait);
	mutex_lock(&bm_queues_lock);
	xa_for_each(&bm_queues, id, q) {
		struct bm_uapi_ring_ctrl *ctrl;
		u32 head, tail;
		mutex_lock(&q->lock);
		if (q->attached && !q->dead && q->area) {
			ctrl = q->area + BM_UAPI_MMAP_REQUEST_CTRL;
			head = le32_to_cpu((__force __le32)READ_ONCE(ctrl->head));
			tail = le32_to_cpu((__force __le32)READ_ONCE(ctrl->tail));
			if (head != tail) mask |= EPOLLIN | EPOLLRDNORM;
		}
		mutex_unlock(&q->lock);
		if (mask) break;
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
