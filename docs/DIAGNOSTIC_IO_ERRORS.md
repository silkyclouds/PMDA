# Diagnostic: I/O errors on PMDA server (192.168.3.2)

**Date:** 2026-01-25  
**Symptom:** Many `OSError: [Errno 5] Input/output error` when PMDA (and the host) access paths under `/mnt/user/MURRAY/Music/Music_dump/`.

---

## 0. Short answer: it's the **cache** disk (apps_cache), not the array

Your **Array Devices** screenshot shows all array disks with **0 errors** – that's correct, the array is fine.  
The failing device is **not in the array**: it's **/dev/sda1** mounted as **/mnt/apps_cache** (BTRFS). In Unraid this is a **cache** (or pool) disk, shown in **Main → Cache** (or Pool), not in "Array Devices".

**Proof:**
- `ls /mnt/apps_cache` → **Input/output error** (the whole filesystem fails).
- `ls /mnt/user0/.../08-11/` (array only) → OK, 24 entries, **no** "Trailblazer.
- `stat "…/Trailblazer [2018, FLAC]"` on **/mnt/user** → **Input/output error** (user = array + cache; the entry comes from cache).
- None of the array disks (disk1…23) contain that folder; it exists only on the broken cache.

So: **the I/O errors are 100% from the apps_cache disk (sda1)**. Fix or replace that disk and the errors should stop.

---

## 1. Cause: real storage issues, not PMDA

The I/O errors are returned by the **kernel** when reading certain paths. The same `stat()` on the **host** fails with "Input/output error" (tested on `Trailblazer [2018, FLAC]`). So this is **not** a Docker or PMDA bug: the filesystem or disk is returning errors.

---

## 2. What was found on the server

### 2.1 BTRFS errors on `/dev/sda1` (apps cache)

- **Mount:** `/dev/sda1 on /mnt/apps_cache type btrfs (ro,...)`  
  The volume is mounted **read-only** (`ro`), which often happens after BTRFS detects I/O errors.
- **dmesg:** Multiple lines like:
  - `BTRFS error (device sda1): bdev /dev/sda1 errs: wr X, rd Y`
  - `BTRFS: error (device sda1 state EA) in ... errno=-5 IO failure`
  - `Transaction aborted (error -5)`
- So **one of your cache drives (sda1 = /mnt/apps_cache)** has real read/write errors and BTRFS has put it in read-only.

### 2.2 Other kernel messages

- **Synchronize Cache(10) failed** for `sda` and `sdb` (hostbyte=0x07) — suggests communication or state issues with those drives.
- **BTRFS read errors** increasing over time (rd 1 → 15) on `sda1`.

### 2.3 Unraid array status (from your config)

- **mdNumDisabled=1**, **mdNumInvalid=1** — one array slot is disabled and one is invalid. That can mean a missing or failed data disk.
- If the user share **MURRAY/Music** (or part of it) was on that disabled/invalid disk, any access to those inodes can return I/O errors.

### 2.4 User share vs disk7

- The path `.../11-2025/08-11/` exists on **disk7** (XFS, `/dev/md7p1`); listing this path on disk7 works.
- The specific directory `Trailblazer [2018, FLAC]` is **not** under that path on disk7 (it is "No such file or directory" on disk7).
- So "Trailblazer" likely lies on **another disk** in the user share. When the union (shfs) tries to read that inode — e.g. from a failed/disabled disk or from a disk with bad sectors — the kernel returns **Input/output error**.

---

## 3. Conclusion

| Source | Finding |
|--------|--------|
| **/dev/sda1** | BTRFS I/O errors; mounted **read-only**. Likely failing or degraded drive used for apps cache. |
| **Unraid array** | One disabled + one invalid disk. Files that were on that disk (or on a disk with bad sectors) will trigger I/O errors when accessed via `/mnt/user`. |
| **Music_dump paths** | Affected entries are probably on the problematic disk(s) or on sectors that return I/O errors. |

So: **yes, there are real disk/filesystem problems**; they are not caused by PMDA.

---

## 4. What you should do (in order)

1. **Unraid dashboard**
   - Open **Main → Array status** and see which disk is **disabled** and which is **invalid**.
   - Note the serial/model of the affected disk(s).

2. **Fix the array (if a disk failed)**
   - If a data disk has been disabled, replace it and rebuild per Unraid docs, or remove it from the array if it is no longer present.
   - Do not ignore "invalid" or "disabled" slots; that can explain I/O errors on part of the share.

3. **Check /mnt/apps_cache (sda1)**
   - This is your **apps cache** (BTRFS). It is **read-only** because of errors.
   - Run SMART on the drive behind `sda` (see below).
   - Consider replacing that drive if SMART or scrub report problems.
   - If the disk is OK, you can try a BTRFS scrub and then remount rw only if Unraid/BTRFS documentation says it is safe.

4. **Run SMART on the affected drives**
   - From Unraid: **Main → click the disk → Health** (or use the smartctl method below).
   - From CLI (adapt device names to your system):
     ```bash
     smartctl -H /dev/sda
     smartctl -a /dev/sda
     ```
   - For array disks, use the block device Unraid uses (e.g. `/dev/sdX` for the disk in question).
   - If SMART shows "FAILED" or many reallocated/pending sectors, plan to replace the disk.

5. **Optional: BTRFS scrub on apps_cache**
   - Only if the filesystem is still considered usable and you have backups:
     ```bash
     btrfs scrub start /mnt/apps_cache
     btrfs scrub status /mnt/apps_cache
     ```
   - Do not remount read-write until you understand why it was set to ro and have addressed the cause.

6. **PMDA in the meantime**
   - PMDA has been made **tolerant** to these I/O errors (it skips bad paths and continues).
   - You can also exclude the most affected tree from scanning, e.g. set **SKIP_FOLDERS** to a path that avoids the worst part of `Music_dump` if you know it (e.g. a subfolder that is on the bad disk).
   - Fixing the underlying disks and array is the real fix; until then, expect more I/O errors on other paths that sit on the same disk(s).

---

## 5. Quick reference: devices seen

| Device | Mount | Notes |
|--------|--------|--------|
| **/dev/sda1** | /mnt/apps_cache | BTRFS, **read-only**, multiple I/O errors in dmesg |
| /dev/nvme0n1p1 | /mnt/cache | BTRFS (main cache) |
| /dev/sdb1 | /mnt/disks/Micron_CT4000X6SSD9 | BTRFS (Unassigned?) |
| /dev/md7p1 | /mnt/disk7 | XFS, array disk 7 (17T, ST18000NM000J) |
| … | /mnt/disk* | Other array disks (XFS); one slot disabled, one invalid |

If you want, we can add a step-by-step Unraid-only checklist (only UI, no CLI) in a separate doc.
