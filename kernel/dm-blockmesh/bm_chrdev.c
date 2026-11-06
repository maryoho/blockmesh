// SPDX-License-Identifier: GPL-2.0
/* Node-wide /dev/blockmesh endpoint and per-DM-Target queue registry. */
#include <linux/atomic.h>
#include <linux/bio.h>
#include <linux/blkdev.h>
#include <linux/list.h>
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
struct bm_pending {
	struct list_head list;
	struct bio *bio;
	struct block_device *bdev;
	u64 request_id, generation, logical_start, base_sector, sector_offset, owner_epoch;
	u32 block_count;
};
struct bm_queue_mem {
	struct kref ref;
	void *area;
	size_t bytes;
};
struct bm_queue {
	struct kref ref; /* registry reference plus transient ioctl/mmap lookups */
	struct mutex lock;
	struct list_head pending;
	u32 pending_count;
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
static atomic64_t bm_next_request_id = ATOMIC64_INIT(0);
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


static void bm_abort_pending_list(struct list_head *list)
{
	struct bm_pending *p, *tmp;
	list_for_each_entry_safe(p, tmp, list, list) {
		list_del(&p->list);
		bio_io_error(p->bio);
		kfree(p);
	}
}
static void bm_abort_queue_pending(struct bm_queue *q)
{
	LIST_HEAD(aborted);
	mutex_lock(&q->lock);
	list_splice_init(&q->pending, &aborted);
	q->pending_count = 0;
	mutex_unlock(&q->lock);
	bm_abort_pending_list(&aborted);
}
int bm_queue_submit(u32 queue_id, u64 target_cookie,
		    struct bm_uapi_request *request, struct bio *bio,
		    struct block_device *bdev, u64 base_sector, u64 sector_offset,
		    u32 block_count)
{
	struct bm_queue *q;
	struct bm_uapi_ring_ctrl *ctrl;
	struct bm_uapi_request *slot;
	struct bm_pending *pending;
	u32 head, tail, entries;
	u64 request_id;
	int ret = 0;

	if (!request || !bio || !bdev)
		return -EINVAL;
	pending = kzalloc(sizeof(*pending), GFP_NOIO);
	if (!pending)
		return -ENOMEM;
	request_id = atomic64_inc_return(&bm_next_request_id);
	if (!request_id) {
		kfree(pending);
		return -EOVERFLOW;
	}
	q = bm_queue_lookup(queue_id);
	if (!q) {
		kfree(pending);
		return -ENODEV;
	}
	mutex_lock(&q->lock);
	if (q->dead || q->target_cookie != target_cookie)
		ret = -ESTALE;
	else if (!q->attached || !q->mem)
		ret = -ENOTCONN;
	else {
		entries = 1U << q->ring_order;
		ctrl = q->mem->area + BM_UAPI_MMAP_REQUEST_CTRL;
		head = le32_to_cpu((__force __le32)smp_load_acquire((u32 *)&ctrl->head));
		tail = le32_to_cpu((__force __le32)READ_ONCE(ctrl->tail));
		if ((u32)(tail - head) > entries)
			ret = -EPROTO;
		else if ((u32)(tail - head) == entries || q->pending_count >= entries)
			ret = -EAGAIN;
		else {
			request->version = cpu_to_le16(BM_UAPI_VERSION);
			request->size = cpu_to_le16(BM_UAPI_REQUEST_SIZE);
			request->request_id = cpu_to_le64(request_id);
			request->queue_generation = cpu_to_le64(q->generation);
			pending->bio = bio;
			pending->bdev = bdev;
			pending->request_id = request_id;
			pending->generation = q->generation;
			pending->logical_start = le64_to_cpu(request->logical_start);
			pending->base_sector = base_sector;
			pending->sector_offset = sector_offset;
			pending->owner_epoch = le64_to_cpu(request->owner_epoch);
			pending->block_count = block_count;
			list_add_tail(&pending->list, &q->pending);
			q->pending_count++;
			slot = q->mem->area + BM_UAPI_MMAP_REQUEST_RING +
				(tail & (entries - 1)) * BM_UAPI_REQUEST_SIZE;
			memcpy(slot, request, sizeof(*request));
			smp_store_release((u32 *)&ctrl->tail,
					  (__force u32)cpu_to_le32(tail + 1));
			pending = NULL;
		}
	}
	mutex_unlock(&q->lock);
	if (ret == -EPROTO)
		bm_abort_queue_pending(q);
	kref_put(&q->ref, bm_queue_release);
	kfree(pending);
	if (!ret)
		wake_up_all(&bm_poll_wait);
	return ret;
}
static struct bm_pending *bm_take_pending(struct bm_queue *q, u64 request_id,
					 u64 generation)
{
	struct bm_pending *p;
	list_for_each_entry(p, &q->pending, list) {
		if (p->request_id == request_id && p->generation == generation) {
			list_del(&p->list);
			if (q->pending_count)
				q->pending_count--;
			return p;
		}
	}
	return NULL;
}
static void bm_finish_pending(struct bm_pending *p,
			      const struct bm_uapi_completion *completion,
			      const struct bm_uapi_mapping *mapping)
{
	struct bio *bio = p->bio;
	u16 code = le16_to_cpu(completion->code);
	u64 phys_sector, phys_block, map_logical, map_physical;
	u32 map_count;

	if (code != BM_UAPI_C_OK ||
	    le64_to_cpu(completion->owner_epoch) != p->owner_epoch)
		goto io_error;
	if (!p->block_count) {
		if (le32_to_cpu(completion->mapping_count) ||
		    le32_to_cpu(completion->payload_bytes))
			goto io_error;
		bio_set_dev(bio, p->bdev);
		bio->bi_iter.bi_sector = p->base_sector;
		submit_bio_noacct(bio);
		kfree(p);
		return;
	}
	if (!mapping || le32_to_cpu(completion->mapping_count) != 1 ||
	    le32_to_cpu(completion->payload_bytes) != BM_UAPI_MAPPING_SIZE)
		goto io_error;
	map_logical = le64_to_cpu(mapping->logical_block);
	map_physical = le64_to_cpu(mapping->physical_block);
	map_count = le32_to_cpu(mapping->block_count);
	if (le32_to_cpu(mapping->reserved) || !map_count ||
	    map_logical > U64_MAX - map_count ||
	    map_physical > U64_MAX - map_count ||
	    p->logical_start < map_logical ||
	    p->logical_start > U64_MAX - p->block_count ||
	    p->logical_start + p->block_count > map_logical + map_count)
		goto io_error;
	phys_block = map_physical + (p->logical_start - map_logical);
	if (phys_block > div_u64(U64_MAX, BM_UAPI_BLOCK_SECTORS))
		goto io_error;
	if (p->base_sector > U64_MAX - p->sector_offset ||
	    phys_block > div_u64(U64_MAX - p->base_sector - p->sector_offset, BM_UAPI_BLOCK_SECTORS))
		goto io_error;
	phys_sector = p->base_sector + phys_block * BM_UAPI_BLOCK_SECTORS + p->sector_offset;
	if (phys_sector > bdev_nr_sectors(p->bdev) ||
	    bio_sectors(bio) > bdev_nr_sectors(p->bdev) - phys_sector)
		goto io_error;
	bio_set_dev(bio, p->bdev);
	bio->bi_iter.bi_sector = phys_sector;
	submit_bio_noacct(bio);
	kfree(p);
	return;
io_error:
	bio_io_error(bio);
	kfree(p);
}
static int bm_consume_completions(struct bm_queue *q)
{
	struct bm_uapi_ring_ctrl *ctrl;
	u32 head, tail, entries, mask;
	u64 generation;
	int consumed = 0;

	mutex_lock(&q->lock);
	if (!q->attached || !q->mem || q->dead) {
		mutex_unlock(&q->lock);
		return -ENOTCONN;
	}
	entries = 1U << q->ring_order;
	mask = entries - 1;
	generation = q->generation;
	ctrl = q->mem->area + BM_UAPI_MMAP_COMP_CTRL;
	head = le32_to_cpu((__force __le32)READ_ONCE(ctrl->head));
	tail = le32_to_cpu((__force __le32)smp_load_acquire((u32 *)&ctrl->tail));
	if ((u32)(tail - head) > entries) {
		mutex_unlock(&q->lock);
		bm_abort_queue_pending(q);
		return -EPROTO;
	}
	while (head != tail) {
		struct bm_uapi_completion c;
		struct bm_uapi_mapping mapping_copy, *mapping = NULL;
		struct bm_pending *p;
		u32 slot = head & mask, payload_offset, payload_bytes;
		struct bm_uapi_completion *src;

		src = q->mem->area + BM_UAPI_MMAP_COMP_RING +
			slot * BM_UAPI_COMPLETION_SIZE;
		memcpy(&c, src, sizeof(c));
		payload_offset = le32_to_cpu(c.payload_offset);
		payload_bytes = le32_to_cpu(c.payload_bytes);
		if (le16_to_cpu(c.version) != BM_UAPI_VERSION ||
		    le16_to_cpu(c.size) != BM_UAPI_COMPLETION_SIZE ||
		    le64_to_cpu(c.queue_generation) != generation ||
		    !le64_to_cpu(c.request_id) ||
		    le32_to_cpu(c.reserved[0]) || le32_to_cpu(c.reserved[1]) ||
		    le32_to_cpu(c.reserved[2]) || le16_to_cpu(c.flags) ||
		    le16_to_cpu(c.code) > BM_UAPI_C_INTERNAL) {
			p = bm_take_pending(q, le64_to_cpu(c.request_id), generation);
			head++;
			smp_store_release((u32 *)&ctrl->head,
					  (__force u32)cpu_to_le32(head));
			mutex_unlock(&q->lock);
			if (p) { bio_io_error(p->bio); kfree(p); }
			consumed++;
			mutex_lock(&q->lock);
			if (!q->attached || !q->mem || q->generation != generation)
				break;
			ctrl = q->mem->area + BM_UAPI_MMAP_COMP_CTRL;
			tail = le32_to_cpu((__force __le32)smp_load_acquire((u32 *)&ctrl->tail));
			continue;
		}
		if (payload_bytes) {
			u32 expected = slot * BM_UAPI_MAX_BATCH * BM_UAPI_MAPPING_SIZE;
			u64 end = (u64)payload_offset + payload_bytes;
			if (payload_offset != expected ||
			    le32_to_cpu(c.mapping_count) != 1 ||
			    payload_bytes != BM_UAPI_MAPPING_SIZE ||
			    end > (u64)entries * BM_UAPI_MAX_BATCH * BM_UAPI_MAPPING_SIZE) {
				p = bm_take_pending(q, le64_to_cpu(c.request_id), generation);
				head++;
				smp_store_release((u32 *)&ctrl->head,
						  (__force u32)cpu_to_le32(head));
				mutex_unlock(&q->lock);
				if (p) { bio_io_error(p->bio); kfree(p); }
				consumed++;
				mutex_lock(&q->lock);
				if (!q->attached || !q->mem || q->generation != generation)
					break;
				ctrl = q->mem->area + BM_UAPI_MMAP_COMP_CTRL;
				tail = le32_to_cpu((__force __le32)smp_load_acquire((u32 *)&ctrl->tail));
				continue;
			}
			memcpy(&mapping_copy, q->mem->area + BM_UAPI_MMAP_PAYLOAD +
			       payload_offset, sizeof(mapping_copy));
			mapping = &mapping_copy;
		} else if (le32_to_cpu(c.mapping_count) != 0 || payload_offset != 0) {
			p = bm_take_pending(q, le64_to_cpu(c.request_id), generation);
			head++;
			smp_store_release((u32 *)&ctrl->head,
					  (__force u32)cpu_to_le32(head));
			mutex_unlock(&q->lock);
			if (p) { bio_io_error(p->bio); kfree(p); }
			consumed++;
			mutex_lock(&q->lock);
			if (!q->attached || !q->mem || q->generation != generation)
				break;
			ctrl = q->mem->area + BM_UAPI_MMAP_COMP_CTRL;
			tail = le32_to_cpu((__force __le32)smp_load_acquire((u32 *)&ctrl->tail));
			continue;
		}
		p = bm_take_pending(q, le64_to_cpu(c.request_id), generation);
		head++;
		smp_store_release((u32 *)&ctrl->head,
				  (__force u32)cpu_to_le32(head));
		mutex_unlock(&q->lock);
		if (p)
			bm_finish_pending(p, &c, mapping);
		consumed++;
		mutex_lock(&q->lock);
		if (!q->attached || !q->mem || q->generation != generation)
			break;
		ctrl = q->mem->area + BM_UAPI_MMAP_COMP_CTRL;
		tail = le32_to_cpu((__force __le32)smp_load_acquire((u32 *)&ctrl->tail));
	}
	mutex_unlock(&q->lock);
	return consumed;
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
	INIT_LIST_HEAD(&q->pending);
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
	bm_abort_queue_pending(q);
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
		bm_abort_queue_pending(q);
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
		info.features = BM_UAPI_FEATURE_REQUEST_RING | BM_UAPI_FEATURE_COMPLETION_KICK;
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
		LIST_HEAD(aborted);
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
		list_splice_init(&q->pending, &aborted);
		q->pending_count = 0;
		memset(q->mem->area, 0, q->mem->bytes);
		bm_init_ring_ctrl(q, BM_UAPI_MMAP_REQUEST_CTRL, 1U << q->ring_order);
		bm_init_ring_ctrl(q, BM_UAPI_MMAP_COMP_CTRL, 1U << q->ring_order);
		mutex_unlock(&q->lock);
		bm_abort_pending_list(&aborted);
		wake_up_all(&q->wait);
		wake_up_all(&bm_poll_wait);
		kref_put(&q->ref, bm_queue_release);
		return 0;
	}

	case BM_IOC_KICK: {
		u32 queue_id;
		struct bm_queue *q;
		int ret;
		if (copy_from_user(&queue_id, up, sizeof(queue_id)))
			return -EFAULT;
		if (!queue_id || queue_id > BM_UAPI_MAX_QUEUE_ID)
			return -EINVAL;
		q = bm_queue_lookup(queue_id);
		if (!q)
			return -ENOENT;
		ret = bm_consume_completions(q);
		kref_put(&q->ref, bm_queue_release);
		return ret < 0 ? ret : 0;
	}
	case BM_IOC_CANCEL:
		return -EOPNOTSUPP; /* Cancellation state machine is a later increment. */
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
			head = le32_to_cpu((__force __le32)smp_load_acquire((u32 *)&ctrl->head));
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
