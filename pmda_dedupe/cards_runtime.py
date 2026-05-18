"""Runtime-owned duplicate card payload builder."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any


def _bind_runtime(runtime: Any) -> None:
    for name, value in vars(runtime).items():
        if name in {
            "_bind_runtime",
            "build_card_list_for_runtime",
            "_build_card_list",
        }:
            continue
        globals()[name] = value


def build_card_list_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> list[dict]:
    _bind_runtime(runtime)
    return _build_card_list(*args, **kwargs)


def _build_card_list(dup_dict) -> list[dict]:
    """
    Convert the nested `state["duplicates"]` dict into the flat list of
    cards expected by both the main page and /api/duplicates.

    Files mode must stay metadata-only here. This endpoint is polled by the UI
    and may be opened during large scans; probing folder existence, formats or
    sizes on demand can wake disks and make review pages look empty while the
    persisted duplicate evidence is valid.
    """
    cards = []
    files_mode = _get_library_mode() == "files"
    for artist, groups in dup_dict.items():
        for g in groups:
            if "best" not in g or "losers" not in g:
                continue
            best = g["best"]
            losers = list(g.get("losers") or [])
            if files_mode:
                existing_losers = losers
            else:
                # Only consider losers whose folder still exists in legacy mode.
                existing_losers = []
                for loser in losers:
                    loser_folder = path_for_fs_access(Path(loser["folder"])) if loser.get("folder") else None
                    if loser_folder and loser_folder.exists():
                        existing_losers.append(loser)
            # Skip groups with no duplicate edition left to review.
            if not existing_losers:
                continue
            best_folder_raw = str(best.get("folder") or "").strip()
            folder_path = path_for_fs_access(Path(best_folder_raw)) if best_folder_raw else None
            if not files_mode and (folder_path is None or not folder_path.exists()):
                continue
            best_fmt = (
                best.get("fmt_text")
                or best.get("fmt")
                or (get_primary_format(folder_path) if folder_path is not None and not files_mode else "")
                or "—"
            )
            formats = [best_fmt]
            for loser in existing_losers:
                loser_fmt = loser.get("fmt_text") or loser.get("fmt") or ""
                if not loser_fmt and not files_mode and loser.get("folder"):
                    try:
                        loser_fmt = get_primary_format(path_for_fs_access(Path(loser["folder"])))
                    except Exception:
                        loser_fmt = ""
                formats.append(loser_fmt or "—")
            display_title = (
                str(best.get("title_raw") or "").strip()
                or str(best.get("album_norm") or "").strip().title()
                or "Unknown album"
            )
            # Ensure used_ai groups have provider/model for METHOD column (backfill from globals if missing)
            used_ai = best.get("used_ai", False)
            ai_provider = best.get("ai_provider") or ""
            ai_model = best.get("ai_model") or ""
            if used_ai and (not ai_provider or not ai_model):
                mod = sys.modules[__name__]
                ai_provider = ai_provider or (getattr(mod, "AI_PROVIDER", None) or "")
                ai_model = ai_model or (getattr(mod, "RESOLVED_MODEL", None) or getattr(mod, "OPENAI_MODEL", None) or "")
            # Use persisted size_mb/track_count when available (so Unduper shows data after reload)
            if best.get("size_mb") is not None:
                size_mb = int(best["size_mb"])
                size_bytes = size_mb * (1024 * 1024)
            else:
                size_bytes = 0 if files_mode or folder_path is None else safe_folder_size(folder_path)
                size_mb = size_bytes // (1024 * 1024) if size_bytes else 0
            if best.get("track_count") is not None:
                track_count = int(best["track_count"])
            else:
                track_count = 0
            cards.append({
                "artist_key": artist.replace(" ", "_"),
                "artist": artist,
                "album_id": best["album_id"],
                "n": len(existing_losers) + 1,
                "best_thumb": _duplicate_album_thumb_url(best["album_id"], best_folder_raw if files_mode else folder_path),
                "best_title": display_title,
                "best_fmt": best_fmt,
                "formats": formats,
                "used_ai": used_ai,
                "ai_provider": ai_provider,
                "ai_model": ai_model,
                "size": size_bytes,
                "size_mb": size_mb,
                "track_count": track_count,
                "path": str(folder_path or best_folder_raw),
                "no_move": bool(g.get("no_move") or g.get("manual_review") or g.get("same_folder")),
                "match_verified_by_ai": bool(best.get("match_verified_by_ai", False)),
            })
    return cards
