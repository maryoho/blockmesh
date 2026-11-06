/* SPDX-License-Identifier: GPL-2.0 */
#ifndef BM_CHRDEV_H
#define BM_CHRDEV_H
#include <linux/types.h>
#include <linux/bio.h>
#include <linux/blk_types.h>
struct bm_uapi_request;
int bm_chrdev_init(void);
void bm_chrdev_exit(void);
int bm_queue_create(u32 *queue_id, u64 *target_cookie);
void bm_queue_destroy(u32 queue_id, u64 target_cookie);
int bm_queue_submit(u32 queue_id, u64 target_cookie,
		    struct bm_uapi_request *request, struct bio *bio,
		    struct block_device *bdev, u64 base_sector, u64 sector_offset,
		    u32 block_count);
#endif
