# P3 Reader Lease, Shared WAL and SAN Fencing

This integration adds concrete Linux/sanlock and SCSI-3 Persistent Reservation (PR) backends. NoopFencer, MemoryLease, and FileLocker remain development/test scaffolding and are not safe for SAN use.

## 1. Shared sanlock resources

Every host must join the same sanlock lockspace before BlockMesh starts:

    sanlock client add_lockspace -s bm-lockspace:<host_id>:/dev/mapper/bm-lock-lun:0

Provision the lockspace and resource leases offline, once, on the shared lease device. Allocate distinct resources for:
- each volume's reader-set barrier;
- each AG's WAL serialization lock;
- each Reader Table slot's process-lifetime lease.

A Reader Slot resource is stable across BootID/Epoch reuse. The durable Reader Table identity (NodeID, BootID, Epoch) distinguishes process instances; the sanlock resource for the slot prevents a second process from joining that slot while the previous process still holds its lease.

Resource strings use sanlock's lockspace:resource_name:path:offset format. All nodes must resolve the same logical resource names to the same shared lease LUN and offsets. Do not use FileLocker on multiple SAN-connected hosts.

lock.SanlockLocker holds a resource with sanlock client command and a long-running helper. The helper is cmd/bm-lock-helper. It sets PR_SET_PDEATHSIG and checks the expected parent PID before announcing readiness, so death of the BlockMesh process terminates the helper and releases its sanlock resource. Build and install this helper on every host; configure HelperBinary if it is not in PATH. The host's sanlock daemon must be healthy and the lockspace joined.

## 2. Reader join and lease-loss handling

Use volume.ReaderLeaseConfig with a per-slot resource resolver and a
fence.SCSIPRRegistrar configured with this node's local initiator keys.
JoinReaderSession registers and verifies those keys before marking the slot
Active:

    session, err := metastore.JoinReaderSession(
        ctx, readerTable, readerLeaseConfig, prRegistrar,
        slot, nodeID, bootID, epoch,
        func(err error) {
            // Mandatory fail-stop action:
            // stop new reads and suspend/fail the local DM data path.
        },
    )

The callback is mandatory. Detecting local sanlock lease loss is not enough: the node must stop issuing new reads immediately. Other nodes must not mark the reader Fenced until they acquire the same slot lease and complete SAN PR fencing.

Call session.Heartbeat() periodically. Heartbeat does not advance the reader generation. After Tree.Refresh(), stop old-generation reads, drain all in-flight I/O, then call session.AdvanceGeneration(newSequence, true). Graceful shutdown calls session.CloseDrained(ctx, true), which unregisters local PR keys, persists Closed, and releases the slot lease, only after the same stop-and-drain sequence.

## 3. SCSI-3 PR fencing

All initiators allowed to access the metadata/data LUN must register unique nonzero SCSI PR keys. For a registrants-only access policy, configure the LU with an active reservation type 6 (Exclusive Access - Registrants Only), and ensure every legitimate reader and writer is registered before it opens the volume.

Install sg_persist from sg3_utils. Configure fence.SCSIPRFencer with:
- one PRPath per local initiator path, including that path's local registration key;
- a resolver returning every registration key belonging to the exact (NodeID, BootID) being fenced;
- the reservation type actually active on the LU (normally 6).

The fencer issues PREEMPT AND ABORT for each target key and then executes PRIN READ KEYS on every configured path. It returns success only if all target keys are absent on all paths. Any command error, inaccessible path, unknown target key, or remaining key fails closed. It never uses PR CLEAR, which would remove registrations unrelated to the failed node.

Before marking a failed Reader slot Fenced, use ReaderTable.FenceExpiredReader with a SanlockReaderLeaseProbe. The probe tries to acquire the exact per-slot sanlock resource:
- if the old reader still holds it, acquisition fails/times out and fencing does not begin;
- if acquisition succeeds, the recovery controller holds that resource while it preempts the target's PR keys and persists ReaderFenced.

This prevents a new process from reusing the same slot while fencing is in progress. The reader-set barrier serializes this operation against registration, root publication, generation updates, and retire collection.

## 4. Multi-process AG WAL serialization

Do not share a long-lived wal.Journal object between processes. Its append offset, next TxID, active bank, and checkpoint view are process-local.

Use wal.SharedCommitter on every node that can commit into the same AG WAL. It acquires one exclusive sanlock resource for that AG, opens a fresh Journal (which reloads the checkpoint descriptor and replays committed records), then commits and releases the resource. Checkpoint and recovery use the same lock. Every process must use the same resource name for the same AG. Do not mix SharedCommitter and direct Journal.Commit against one AG segment.

Use a different sanlock resource for the reader-set barrier and for each AG WAL lock. Lock ordering in metadata paths is reader-set barrier first, AG WAL resource second.

## 5. Reclamation sequence

The recovery controller must follow this order:

1. Confirm the Reader's sanlock slot resource can be acquired.
2. Hold that probe lease to prevent slot reuse.
3. Execute SCSI PR PREEMPT AND ABORT for every key belonging to the failed boot instance.
4. Verify target keys are absent on every configured path.
5. Persist ReaderFenced through the shared WAL committer.
6. Release the probe lease.
7. Confirm global I/O drain, then call PersistentArena.CollectWithReaders(readerTable, true).

A lease timeout alone is never a Fenced state. If sanlock acquisition, PREEMPT AND ABORT, path verification, WAL commit, or I/O drain cannot be proven successful, the reader remains generation-pinning and its pages are not reclaimed.

## 6. Operational boundaries

- The sanlock lockspace and every resource must be initialized and joined correctly on every host. Provisioning/offset allocation is an operator responsibility.
- Reader PR keys must be registered by the storage initiators before the Reader becomes Active. The resolver must be backed by trusted node configuration/identity, not user-supplied values.
- The local lease-loss callback must actually suspend/fail the DM device and prevent new bios; the library cannot infer that action.
- SCSI PR behavior must be qualified against the exact SAN firmware, multipath topology, and initiator stack. A failed path verification blocks reclamation.
- The controller that calls FenceExpiredReader must be trusted to run the configured SCSI PR fencer. FenceAndClose remains a lower-level trusted API and must not be called merely because a heartbeat expired.
- CollectWithReaders still requires an independently established global I/O-drained condition. PR task abort does not replace draining the local BlockMesh/DM queues.
- NoopFencer, MemoryLease, and FileLocker are development/test scaffolding only.


## 7. Current API wiring notes

The current JoinReaderSession signature requires the PR registrar:

    session, err := metastore.JoinReaderSession(
        ctx, readerTable, readerLeaseConfig, prRegistrar,
        slot, nodeID, bootID, epoch, onLeaseLost,
    )

The registrar must be a fence.SCSIPRRegistrar configured with this host's
per-initiator local keys. Join registers and verifies those keys before the
Reader Table slot becomes Active. CloseDrained takes a context:
session.CloseDrained(ctx, true). It unregisters the local PR keys, persists
ReaderClosed, then releases the sanlock slot lease.

Build cmd/bm-lock-helper and install it on every host. SanlockLocker invokes
this helper through sanlock client command. The helper sets Linux
PR_SET_PDEATHSIG, checks the expected parent PID to close the parent-death
race, and only then creates the readiness marker. Set HelperBinary to its
installed path when it is not available in PATH.

ReaderLeaseConfig.ResourceFor and SanlockReaderLeaseProbe.ResourceFor both
receive only volume ID and stable slot number. They must resolve to the exact
same pre-initialized sanlock resource. BootID and Epoch are intentionally not
part of the lock resource name.


## 8. P3-3.8 Reader Recovery Controller

Reader Table page version 2 persists `HeartbeatUnixNano` in the previously
reserved bytes. Version 1 records remain readable, but their timestamp is
reported as zero. The controller deliberately skips those legacy records
until their owner writes a version 2 heartbeat; it never fabricates a
timestamp during migration.

Configure all nodes with synchronized wall clocks (for example, chrony).
The timeout decision is:

    age > HeartbeatTimeout + MaxClockSkew

A timestamp farther in the future than MaxClockSkew is treated as a clock
anomaly and skipped. A timestamp slightly in the future is also not stale.
Heartbeat timeout only makes a record a recovery candidate; it does not
change Reader state.

Example controller wiring:

    controller := &metastore.ReaderRecoveryController{
        Table: readerTable,
        Probe: &metastore.SanlockReaderLeaseProbe{
            Locker: sanlockLocker,
            ResourceFor: readerSlotResource,
        },
        Fencer: scsiPRFencer,
        Arena: persistentArena,
        Drain: metadataIODrainBarrier,
        Config: metastore.ReaderRecoveryConfig{
            HeartbeatTimeout: 15 * time.Second,
            MaxClockSkew: 2 * time.Second,
            ScanInterval: time.Second,
            RetryInterval: 5 * time.Second,
            OnError: reportRecoveryError,
        },
    }
    go controller.Run(ctx)

The resource resolver used by ReaderLeaseConfig and
SanlockReaderLeaseProbe must resolve to exactly the same stable per-slot
sanlock resource. It must not include BootID or Epoch in the resource name;
those values identify the durable reader instance, not the lock resource.

RecoverOnce is also exposed for deterministic tests and externally managed
controllers. It returns counts for scanned/stale/fenced/failed/deferred
records. Failed candidates remain Active/Leaving and continue pinning their
generation. Retry backoff is process-local; after controller restart, stale
candidates are reconsidered immediately, which is safe because every attempt
repeats the sanlock probe and PR verification.

If Arena is configured, a successful fencing pass is followed by the
configured MetadataIODrainBarrier and then CollectWithReaders. The drain
barrier must cover metadata-page IOs and must be an independently trusted
cluster-wide proof; SAN PREEMPT AND ABORT alone is not sufficient. If no
drain barrier is configured, fencing may complete but reclamation is skipped.
