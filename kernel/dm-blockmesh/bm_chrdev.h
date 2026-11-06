/* SPDX-License-Identifier: GPL-2.0 */
#ifndef BM_CHRDEV_H
#define BM_CHRDEV_H
#include <linux/types.h>
int bm_chrdev_init(void);
void bm_chrdev_exit(void);
int bm_queue_create(u32 *queue_id, u64 *target_cookie);
void bm_queue_destroy(u32 queue_id, u64 target_cookie);
#endif
