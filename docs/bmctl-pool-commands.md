# bmctl Pool commands (BMPOOL03)

Pool configuration lives at /etc/blockmesh/pools/<pool-id>.json.

Required:
- device_path: multipath, single-path iSCSI, or local block device
- lockspace, lock_device, lock_offset_bytes: unique sanlock resource location
- owner_lease_helper: helper implementing the fenced Owner lease contract in owner-lease-helper.md

For format, also set volume_slots and optionally logical_sector_bytes. If the logical sector field is omitted, bmctl detects it from the block device.

For metadata expansion, configure metadata_usage_file with a fresh JSON snapshot:
{"used_bytes": 123456, "growth_bytes": 123456, "observed_at": "2026-11-06T09:00:00Z"}
The usage and growth values are observations in the dynamic Metadata area; the CLI never accepts a target capacity. metadata_usage_max_age_seconds defaults to 3600.

Commands:
- bmctl pool format --pool pool-001 --confirm-format
- bmctl pool rescan --pool pool-001
- bmctl pool metadata-expand --pool pool-001
- bmctl pool metadata-expand-status --pool pool-001

Mutating commands hold the helper-provided fenced Owner lease and the distributed Bootstrap sanlock lock. Status is read-only. The metadata service must provide authoritative usage telemetry; this CLI does not invent it.
