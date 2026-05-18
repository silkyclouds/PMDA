"""Disabled Plex source-database compatibility helpers.

PMDA no longer uses Plex as a source database. Plex remains supported only as a
post-publication player refresh target through :mod:`pmda_integrations.player_sync`.
These helpers exist only to keep old settings/import paths non-destructive.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from pathlib import Path
from typing import Any


PLEX_DB_FILENAME = "com.plexapp.plugins.library.db"

_PLEX_DB_RELATIVE_PATHS = (
    "Library/Application Support/Plex Media Server/Plug-in Support/Databases",
    os.path.join("Plex Media Server", "Plug-in Support", "Databases"),
)


def resolve_plex_db_from_base(base_path: str) -> str | None:
    """Find a legacy Plex DB directory below *base_path* without opening the DB."""
    base = Path(base_path).resolve()
    if not base.exists() or not base.is_dir():
        return None
    for rel in _PLEX_DB_RELATIVE_PATHS:
        candidate = base / rel
        if (candidate / PLEX_DB_FILENAME).exists():
            return str(candidate)
    base_str = str(base)
    for root, dirs, files in os.walk(base, topdown=True):
        try:
            depth = len(Path(root).relative_to(base).parts) if root != base_str else 0
        except ValueError:
            continue
        if depth > 10:
            dirs.clear()
            continue
        if PLEX_DB_FILENAME in files:
            return root
    return None


def ensure_plex_db_path_resolved(runtime: Any) -> str | None:
    """Compatibility resolver disabled by default for files-only PMDA builds."""
    if not bool(getattr(runtime, "_ALLOW_PLEX_DB_IN_FILES_MODE", False)):
        return None
    library_mode = str(getattr(runtime, "_startup_library_mode", "files") or "files").strip().lower()
    if library_mode != "plex":
        return None

    get_setting = getattr(runtime, "_get_from_sqlite")
    db_path = get_setting("PLEX_DB_PATH")
    if db_path and (Path(db_path) / PLEX_DB_FILENAME).exists():
        return str(db_path).strip()

    base = (get_setting("PLEX_BASE_PATH") or "").strip()
    if not base:
        base = "/plex" if Path("/plex").exists() else "/database"
    resolved = resolve_plex_db_from_base(base)
    if not resolved:
        return None

    try:
        getattr(runtime, "init_settings_db")()
        settings_db_file = Path(getattr(runtime, "SETTINGS_DB_FILE"))
        con = sqlite3.connect(str(settings_db_file), timeout=5)
        con.execute("INSERT OR REPLACE INTO settings(key, value) VALUES(?, ?)", ("PLEX_DB_PATH", resolved))
        con.commit()
        con.close()
        logging.info("Legacy Plex DB path discovered at %s (saved to settings.db)", resolved)
    except Exception as exc:
        logging.debug("Could not persist legacy PLEX_DB_PATH to settings.db: %s", exc)
    return resolved
