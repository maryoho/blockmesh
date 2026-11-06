# P4-1.2.5.9 — bmctl Pool operations

Pool configuration is stored at /etc/blockmesh/pools/<pool-id>.json. Required fields:
- device_path: one multipath, single-path iSCSI, or local block device
- lockspace, lock_device, lock_offset_bytes: sanlock resource location (the offset must be unique for this Pool)
- volume_slots and logical_sector_bytes: used by explicit format

Optional usage telemetry fields:
- metadata_usage_file: JSON snapshot with used_bytes, growth_bytes and observed_at (RFC3339)
- metadata_usage_max_age_seconds: snapshot freshness limit (default 3600)

Commands:
- bmctl pool format --pool pool-001 --confirm-format
- bmctl pool rescan --pool pool-001
- bmctl pool metadata-expand --pool pool-001
- bmctl pool metadata-expand-status --pool pool-001

Metadata expansion never accepts a target size. It uses a fresh usage/growth snapshot and the internal planner. The metadata service must maintain the snapshot; this command does not fabricate usage metrics.

The command implementation acquires the configured SAN sanlock resource. The current CLI does not construct a complete OwnerCoordinator (PR fencer, reservation manager, authoritative OwnerRecord CAS and old-owner I/O drain), so its mutating calls fail closed until a live fenced Owner lease is injected by the runtime integration. Do not bypass BootstrapStore.SetOwnerLease or substitute a config-file epoch. The status command is read-only.
