/* SPDX-License-Identifier: GPL-2.0 WITH Linux-syscall-note */
#ifndef BLOCKMESH_UAPI_H
#define BLOCKMESH_UAPI_H
#include <linux/ioctl.h>
#include <linux/types.h>
#define BM_UAPI_VERSION 1U
#define BM_UAPI_MAX_BATCH 256U
#define BM_UAPI_MIN_RING_ORDER 4U
#define BM_UAPI_MAX_RING_ORDER 10U
#define BM_UAPI_REQUEST_SIZE 128U
#define BM_UAPI_COMPLETION_SIZE 64U
#define BM_UAPI_MAPPING_SIZE 24U
#define BM_UAPI_RING_CTRL_SIZE 64U
#define BM_UAPI_OP_LOOKUP_BATCH 1U
#define BM_UAPI_OP_ALLOCATE_ZERO_PUBLISH 2U
#define BM_UAPI_OP_DISCARD 3U
#define BM_UAPI_OP_FLUSH 4U
#define BM_UAPI_C_OK 0U
#define BM_UAPI_C_UNMAPPED 1U
#define BM_UAPI_C_STALE_EPOCH 2U
#define BM_UAPI_C_READ_ONLY 3U
#define BM_UAPI_C_NO_SPACE 4U
#define BM_UAPI_C_RETRY 5U
#define BM_UAPI_C_FENCED 6U
#define BM_UAPI_C_INVALID 7U
#define BM_UAPI_C_INTERNAL 8U
#define BM_UAPI_MMAP_REQUEST_CTRL 0x00000000ULL
#define BM_UAPI_MMAP_REQUEST_RING 0x00010000ULL
#define BM_UAPI_MMAP_COMP_CTRL 0x00020000ULL
#define BM_UAPI_MMAP_COMP_RING 0x00030000ULL
#define BM_UAPI_MMAP_PAYLOAD 0x00040000ULL

/* All multibyte wire fields are little-endian. Indices are aligned __u32. */
struct bm_uapi_ring_ctrl {
	__u32 head, tail, ring_size, flags;
	__u64 generation;
	__u64 reserved[5];
};
struct bm_uapi_request {
	__u16 version, size, operation, flags;
	__u64 request_id, queue_generation, owner_epoch, reader_epoch;
	__u64 root_generation, logical_start;
	__u32 block_count, reserved0;
	__u8 idempotency_key[16], volume_id[16], node_id[16], boot_id[16];
};
struct bm_uapi_completion {
	__u16 version, size, code, flags;
	__u64 request_id, queue_generation, owner_epoch, root_generation;
	__u32 payload_offset, mapping_count, payload_bytes;
	__u32 reserved[3];
};
struct bm_uapi_mapping {
	__u64 logical_block, physical_block;
	__u32 block_count, reserved;
};
struct bm_uapi_setup {
	__u32 size, version, ring_order, flags;
	__u64 queue_generation;
	__u64 request_ctrl_offset, request_ring_offset;
	__u64 completion_ctrl_offset, completion_ring_offset, payload_offset;
	__u32 max_batch;
	__u32 reserved[3];
};
struct bm_uapi_cancel {
	__u32 size, flags;
	__u64 request_id, queue_generation, reserved[2];
};
struct bm_uapi_abi_info {
	__u32 size, version, request_size, completion_size;
	__u32 mapping_size, ring_ctrl_size, max_batch, max_ring_order;
	__u64 features, reserved[3];
};
#define BM_IOC_MAGIC 'B'
#define BM_IOC_GET_ABI _IOWR(BM_IOC_MAGIC, 0x00, struct bm_uapi_abi_info)
#define BM_IOC_SETUP_QUEUE _IOWR(BM_IOC_MAGIC, 0x01, struct bm_uapi_setup)
#define BM_IOC_RESET_QUEUE _IOW(BM_IOC_MAGIC, 0x02, __u64)
#define BM_IOC_CANCEL _IOW(BM_IOC_MAGIC, 0x03, struct bm_uapi_cancel)
#endif
