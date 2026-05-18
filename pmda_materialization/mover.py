"""Robust filesystem move primitives used by PMDA materialization jobs."""

from __future__ import annotations

import errno
import logging
import os
import shutil
import time
from pathlib import Path


def safe_move(src: str, dst: str) -> None:
    """Move ``src`` to ``dst`` robustly, including cross-device moves.

    The function first tries an atomic same-device rename. When that fails
    because the source and destination are on different devices, or because a
    NAS/filesystem rejects the rename, it falls back to copy + remove with
    retries for common transient NAS errors.
    """

    src_path = Path(src)
    dst_path = Path(dst)
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        os.replace(src, dst)
        return
    except OSError as exc:
        if exc.errno != errno.EXDEV:
            logging.warning("safe_move(): os.replace failed (%s) - falling back to copy", exc)

    final_dst = dst_path
    if final_dst.exists():
        base = final_dst.name
        parent = final_dst.parent
        n = 1
        while (parent / f"{base} ({n})").exists():
            n += 1
        final_dst = parent / f"{base} ({n})"
        logging.warning("safe_move(): destination exists, using %s", final_dst)

    try:
        if src_path.is_dir():
            shutil.copytree(src_path, final_dst, dirs_exist_ok=False)
        else:
            shutil.copy2(src_path, final_dst)
    except Exception as copy_err:
        logging.error("safe_move(): copy failed %s -> %s - %s", src_path, final_dst, copy_err)
        raise

    for attempt in range(5):
        try:
            if src_path.is_dir():
                shutil.rmtree(src_path)
            else:
                try:
                    src_path.unlink()
                except FileNotFoundError:
                    pass
            break
        except OSError as exc:
            if exc.errno in (errno.ENOTEMPTY, errno.EBUSY):
                logging.warning("safe_move(): rmtree(%s) failed (%s) - retry %d/5", src, exc, attempt + 1)
                time.sleep(1.5)
                continue
            raise
    else:
        logging.warning("safe_move(): forcing removal of residual files in %s", src)
        shutil.rmtree(src_path, ignore_errors=True)

    if os.path.exists(src):
        shutil.rmtree(src, ignore_errors=True)
