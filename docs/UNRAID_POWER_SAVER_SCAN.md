# Unraid Disk-Aware Power Saver Scan

PMDA can optionally scan Unraid arrays one source disk at a time. The goal is to avoid waking every HDD in a large user share when discovery, tag reads, ffprobe, fingerprinting, and provider matching only need the albums on the current disk.

This is v1 behavior: off by default, Unraid only, one active source disk, no forced spindown commands.

## Why This Exists

Unraid user shares such as `/mnt/user/Music` hide the physical disk layout. A normal recursive scan can touch paths spread across many disks and wake the whole array. In power saver mode PMDA reads the physical disk paths directly:

- Canonical PMDA path: `/music/Music_dump/Artist/Album`
- Read-only scan path: `/host_mnt/disk7/MURRAY/Music/Music_dump/Artist/Album`

PMDA keeps `/music/...` in the UI, cache, database, moves, and library rows. `/host_mnt/diskN/...` is only the temporary read path for the active disk bucket.

## Container Mount

Add this optional Unraid template mapping:

| Host path | Container path | Mode |
|---|---|---|
| `/mnt` | `/host_mnt` | `ro` |

Do not add Docker socket access and do not make the container privileged for this feature.

## Settings

Advanced Settings exposes:

| Setting | Default | Meaning |
|---|---:|---|
| `STORAGE_POWER_SAVER_ENABLED` | `false` | Enables disk-aware scan validation and scheduling. |
| `STORAGE_PROVIDER` | `unraid` | v1 provider. Other providers are not implemented yet. |
| `UNRAID_HOST_MNT_ROOT` | `/host_mnt` | Container path where host `/mnt` is mounted read-only. |
| `UNRAID_USER_SHARE_HOST_ROOT` | `/host_mnt/user/MURRAY/Music` | Host-side user share equivalent to PMDA `/music`. |
| `UNRAID_CONTAINER_SHARE_ROOT` | `/music` | Canonical PMDA music root. |
| `STORAGE_MAX_ACTIVE_DEVICES` | `1` | Strict one-source-disk scheduler in v1. |
| `STORAGE_SPINDOWN_POLICY` | `none` | PMDA does not force spindown in v1. |

For the user share setting, adjust the share path to your real Unraid topology. If your PMDA `/music` maps to `/mnt/user/Music`, set `UNRAID_USER_SHARE_HOST_ROOT=/host_mnt/user/Music`. If it maps to `/mnt/user/MURRAY/Music`, keep `/host_mnt/user/MURRAY/Music`.

## Validation

When enabled, PMDA refuses to start a scan if:

- `/host_mnt` does not exist.
- no `/host_mnt/disk*` folders are visible.
- the configured user share root is not under `/host_mnt/user`.
- a configured PMDA source root is not under `/music`.
- no physical disk contains the requested source root.

This is intentional. Silent fallback to `/mnt/user` would defeat the power-saving goal.

## Scan Scheduling

PMDA builds a disk bucket plan before the main worker stage:

1. Discover source folders through `/host_mnt/diskN/...`.
2. Convert every discovered album back to canonical `/music/...` for PMDA state.
3. Group albums by disk bucket.
4. Run `SCAN_THREADS` only inside the active disk bucket.
5. Finish the bucket, then move to the next disk.
6. Reconcile duplicate groups globally after all buckets.

Artists split across multiple disks are temporarily split by disk during scanning. Duplicate reconciliation happens after the bucket pass so cross-disk duplicate groups are still detected.

## Writes And Limits

This feature minimizes source disk wakeups. It does not guarantee that every write avoids the array.

Writes can still wake a destination disk if these targets live on the HDD array:

- `Music_matched`
- duplicate review root
- incomplete quarantine root
- export target

For maximum savings, put destinations and PMDA cache on SSD/NVMe cache or accept that destination writes can wake one additional disk.

## UI And MCP

The Scan page shows a compact disk-aware section only during a power saver scan:

- active disk
- bucket albums done/total
- buckets done/total
- active disks `1/N`
- estimated avoided HDD load

MCP exposes read-only storage telemetry:

- `pmda.storage.current`
- `pmda.storage.plan`
- `pmda.scan.analytics.storage`

These tools expose state and plan details only. They do not allow disk mutation, spindown, deletes, or direct DB writes.
