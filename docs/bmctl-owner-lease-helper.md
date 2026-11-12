# bmctl Owner lease helper contract

Mutating bmctl Pool commands require owner_lease_helper in the Pool JSON config. The helper must:
1. Perform the real Pool Owner acquisition/takeover sequence, including sanlock, old-owner SCSI PR PREEMPT AND ABORT on all paths, reservation verification, OwnerRecord CAS and old-owner I/O drain.
2. Print one JSON line to stdout after acquisition: {"epoch": <non-zero OwnerEpoch>}.
3. Remain alive while bmctl operates. If the helper exits, the lease is considered invalid.
4. Release the Owner lease when bmctl closes helper stdin (or on process termination).

bmctl passes --pool <pool-id> --device <device-path>. It does not manufacture OwnerEpoch or treat the Bootstrap lock as an Owner lease. A helper that merely prints an epoch without holding a real fenced Owner lease violates this contract and is unsafe.
