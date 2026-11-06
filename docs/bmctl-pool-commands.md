# bmctl Pool commands (BMPOOL03)

Pool configuration is stored at /etc/blockmesh/pools/<pool-id>.json. Required fields:

- device_path: one multipath, single-path iSCSI, or local block device
- lockspace, lock_device, lock_offset_bytes: sanlock resource location (the offset must be unique for this Pool)
- volume_slots and logical_sector_bytes: used by the explicit format operation

Optional metadata usage fields:

- metadata_usage_file: JSON snapshot path containing used_bytes, growth_bytes and observed_at (RFC3339)
- metadata_usage_max_age_seconds: snapshot freshness limit (default 3600)

Commands:

- bmctl pool format --pool pool-001 --confirm-format
- bmctl pool rescan --pool pool-001
- bmctl pool metadata-expand --pool pool-001
- bmctl pool metadata-expand-status --pool pool-001

The metadata-expand command never accepts a target size. It uses a fresh usage/growth snapshot and the internal planner. The metadata service must maintain that snapshot; this repository change does not fabricate usage metrics.

These commands use a SAN-backed sanlock resource lock. The online service integration must additionally bind this lock to the current fenced Pool Owner lease/epoch before production multi-node deployment.
