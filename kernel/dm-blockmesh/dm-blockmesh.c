// SPDX-License-Identifier: GPL-2.0
/* BlockMesh linear DM target with kernel-enforced RW OwnerEpoch admission. */
#include <linux/bio.h>
#include <linux/kernel.h>
#include <linux/string.h>
#include <linux/blkdev.h>
#include <linux/device-mapper.h>
#include <linux/module.h>
#include <linux/random.h>
#include <linux/mutex.h>
#include <linux/slab.h>
#include <linux/spinlock.h>
#include <linux/wait.h>
#include "blockmesh_uapi.h"

static_assert(sizeof(struct bm_uapi_ring_ctrl) == BM_UAPI_RING_CTRL_SIZE);
static_assert(sizeof(struct bm_uapi_request) == BM_UAPI_REQUEST_SIZE);
static_assert(sizeof(struct bm_uapi_completion) == BM_UAPI_COMPLETION_SIZE);
static_assert(sizeof(struct bm_uapi_mapping) == BM_UAPI_MAPPING_SIZE);
#define DM_MSG_PREFIX "blockmesh"
struct bm_target {
	struct dm_dev *dev;
	sector_t start;
	spinlock_t state_lock;
	struct mutex control_lock;
	wait_queue_head_t drain_wait;
	atomic_t inflight_writes;
	u64 owner_epoch;
	u64 instance_id;
	bool gate_open;
	bool fenced;
};
struct bm_bio_ctx { bool tracked; };
static bool bm_is_mutating_bio(struct bio *bio) { return bio_op(bio) != REQ_OP_READ; }
static int bm_map(struct dm_target *ti, struct bio *bio)
{
	struct bm_target *bm = ti->private;
	struct bm_bio_ctx *pb;
	unsigned long flags;
	if (bm_is_mutating_bio(bio)) {
		pb = dm_per_bio_data(bio, sizeof(*pb));
		pb->tracked = false;
		spin_lock_irqsave(&bm->state_lock, flags);
		if (!bm->gate_open || bm->fenced || !bm->owner_epoch) {
			spin_unlock_irqrestore(&bm->state_lock, flags);
			return DM_MAPIO_KILL;
		}
		atomic_inc(&bm->inflight_writes);
		pb->tracked = true;
		spin_unlock_irqrestore(&bm->state_lock, flags);
	}
	bio_set_dev(bio, bm->dev->bdev);
	if (bio_sectors(bio))
		bio->bi_iter.bi_sector = bm->start + dm_target_offset(ti, bio->bi_iter.bi_sector);
	return DM_MAPIO_REMAPPED;
}
static int bm_end_io(struct dm_target *ti, struct bio *bio, blk_status_t *error)
{
	struct bm_target *bm = ti->private;
	struct bm_bio_ctx *pb;
	(void)error;
	if (!bm_is_mutating_bio(bio)) return DM_ENDIO_DONE;
	pb = dm_per_bio_data(bio, sizeof(*pb));
	if (pb->tracked) {
		pb->tracked = false;
		if (atomic_dec_and_test(&bm->inflight_writes)) wake_up_all(&bm->drain_wait);
	}
	return DM_ENDIO_DONE;
}
static void bm_close_gate(struct bm_target *bm, bool fenced)
{
	unsigned long flags;
	spin_lock_irqsave(&bm->state_lock, flags);
	bm->gate_open = false;
	if (fenced) bm->fenced = true;
	spin_unlock_irqrestore(&bm->state_lock, flags);
}
static void bm_wait_for_drain(struct bm_target *bm)
{
	wait_event(bm->drain_wait, atomic_read(&bm->inflight_writes) == 0);
}
/* owner_prepare E: close+drain, then install strictly newer E while closed. */
static int bm_owner_prepare(struct bm_target *bm, const char *arg, const char *instance_arg)
{
	u64 epoch, instance;
	unsigned long flags;
	int ret = kstrtou64(arg, 10, &epoch);
	if (ret || !epoch) return -EINVAL;
	ret = kstrtou64(instance_arg, 16, &instance);
	if (ret || !instance) return -EINVAL;
	if (instance != bm->instance_id) return -ESTALE;
	/* Reject stale prepare before closing a newer Owner's open gate. */
	spin_lock_irqsave(&bm->state_lock, flags);
	if (epoch <= bm->owner_epoch) {
		spin_unlock_irqrestore(&bm->state_lock, flags);
		return -ESTALE;
	}
	bm->gate_open = false;
	bm->fenced = true;
	spin_unlock_irqrestore(&bm->state_lock, flags);
	bm_wait_for_drain(bm);
	/* bm_message serializes all control transitions with control_lock. */
	spin_lock_irqsave(&bm->state_lock, flags);
	if (epoch <= bm->owner_epoch) ret = -ESTALE;
	else { bm->owner_epoch = epoch; bm->fenced = false; ret = 0; }
	spin_unlock_irqrestore(&bm->state_lock, flags);
	return ret;
}
/* owner_enable E: exact epoch only; no epoch mutation and no fenced reopen. */
static int bm_owner_enable(struct bm_target *bm, const char *arg, const char *instance_arg)
{
	u64 epoch, instance;
	unsigned long flags;
	int ret = kstrtou64(arg, 10, &epoch);
	if (ret || !epoch) return -EINVAL;
	ret = kstrtou64(instance_arg, 16, &instance);
	if (ret || !instance) return -EINVAL;
	if (instance != bm->instance_id) return -ESTALE;
	spin_lock_irqsave(&bm->state_lock, flags);
	if (epoch != bm->owner_epoch) ret = -ESTALE;
	else if (bm->fenced || bm->gate_open || atomic_read(&bm->inflight_writes)) ret = -EPERM;
	else { bm->gate_open = true; ret = 0; }
	spin_unlock_irqrestore(&bm->state_lock, flags);
	return ret;
}
/* owner_fence E: close+drain; older delayed fences cannot affect newer Owner. */
static int bm_owner_fence(struct bm_target *bm, const char *arg, const char *instance_arg)
{
	u64 epoch, instance;
	unsigned long flags;
	int ret = kstrtou64(arg, 10, &epoch);
	if (ret || !epoch) return -EINVAL;
	ret = kstrtou64(instance_arg, 16, &instance);
	if (ret || !instance) return -EINVAL;
	if (instance != bm->instance_id) return -ESTALE;
	spin_lock_irqsave(&bm->state_lock, flags);
	if (epoch < bm->owner_epoch) ret = -ESTALE;
	else {
		bm->gate_open = false;
		bm->fenced = true;
		if (epoch > bm->owner_epoch) bm->owner_epoch = epoch;
		ret = 0;
	}
	spin_unlock_irqrestore(&bm->state_lock, flags);
	if (!ret) bm_wait_for_drain(bm);
	return ret;
}
static int bm_message(struct dm_target *ti, unsigned int argc, char **argv,
		      char *result, unsigned int maxlen)
{
	struct bm_target *bm = ti->private;
	int ret = -EINVAL;
	(void)result; (void)maxlen;
	mutex_lock(&bm->control_lock);
	if (argc == 3 && !strcmp(argv[0], "owner_prepare")) ret = bm_owner_prepare(bm, argv[1], argv[2]);
	else if (argc == 3 && !strcmp(argv[0], "owner_enable")) ret = bm_owner_enable(bm, argv[1], argv[2]);
	else if (argc == 3 && !strcmp(argv[0], "owner_fence")) ret = bm_owner_fence(bm, argv[1], argv[2]);
	mutex_unlock(&bm->control_lock);
	return ret;
}
static void bm_status(struct dm_target *ti, status_type_t type,
		      unsigned int status_flags, char *result, unsigned int maxlen)
{
	struct bm_target *bm = ti->private;
	unsigned long flags;
	u64 epoch;
	bool open, fenced;
	int inflight;
	unsigned int sz = 0;
	(void)status_flags;
	if (type == STATUSTYPE_TABLE) {
		DMEMIT("%s %llu", bm->dev->name, (unsigned long long)bm->start);
		return;
	}
	spin_lock_irqsave(&bm->state_lock, flags);
	epoch = bm->owner_epoch; open = bm->gate_open; fenced = bm->fenced;
	inflight = atomic_read(&bm->inflight_writes);
	spin_unlock_irqrestore(&bm->state_lock, flags);
	DMEMIT("instance=%016llx epoch=%llu gate=%u fenced=%u inflight=%d",
	       (unsigned long long)bm->instance_id, (unsigned long long)epoch,
	       open ? 1 : 0, fenced ? 1 : 0, inflight);
}
static int bm_iterate_devices(struct dm_target *ti, iterate_devices_callout_fn fn, void *data)
{
	struct bm_target *bm = ti->private;
	return fn(ti, bm->dev, bm->start, ti->len, data);
}
static void bm_dtr(struct dm_target *ti)
{
	struct bm_target *bm = ti->private;
	bm_close_gate(bm, true);
	bm_wait_for_drain(bm);
	dm_put_device(ti, bm->dev);
	kfree(bm);
}
static int bm_ctr(struct dm_target *ti, unsigned int argc, char **argv)
{
	struct bm_target *bm;
	unsigned long long start;
	int ret;
	if (argc != 2) { ti->error = "requires: <device> <start_sector>"; return -EINVAL; }
	ret = kstrtoull(argv[1], 10, &start);
	if (ret) { ti->error = "invalid start sector"; return ret; }
	bm = kzalloc(sizeof(*bm), GFP_KERNEL);
	if (!bm) return -ENOMEM;
	bm->start = (sector_t)start;
	spin_lock_init(&bm->state_lock);
	mutex_init(&bm->control_lock);
	init_waitqueue_head(&bm->drain_wait);
	atomic_set(&bm->inflight_writes, 0);
	bm->owner_epoch = 0; bm->gate_open = false; bm->fenced = true;
	do { bm->instance_id = get_random_u64(); } while (!bm->instance_id);
	ret = dm_get_device(ti, argv[0], dm_table_get_mode(ti->table), &bm->dev);
	if (ret) { ti->error = "cannot open underlying device"; kfree(bm); return ret; }
	if (bm->start > bdev_nr_sectors(bm->dev->bdev) ||
	    ti->len > bdev_nr_sectors(bm->dev->bdev) - bm->start) {
		ti->error = "target range exceeds underlying device";
		dm_put_device(ti, bm->dev); kfree(bm); return -EINVAL;
	}
	ti->private = bm;
	ti->per_io_data_size = sizeof(struct bm_bio_ctx);
	ti->num_flush_bios = 1;
	ti->num_discard_bios = 1;
	ti->num_secure_erase_bios = 1;
	ti->num_write_same_bios = 1;
	ti->num_write_zeroes_bios = 1;
	return 0;
}
static struct target_type bm_target = {
	.name = "blockmesh", .version = {1, 0, 0}, .module = THIS_MODULE,
	.ctr = bm_ctr, .dtr = bm_dtr, .map = bm_map, .end_io = bm_end_io,
	.message = bm_message, .status = bm_status, .iterate_devices = bm_iterate_devices,
};
static int __init bm_init(void) { return dm_register_target(&bm_target); }
static void __exit bm_exit(void) { dm_unregister_target(&bm_target); }
module_init(bm_init);
module_exit(bm_exit);
MODULE_DESCRIPTION("BlockMesh DM target with kernel OwnerEpoch write admission");
MODULE_AUTHOR("BlockMesh");
MODULE_LICENSE("GPL");
