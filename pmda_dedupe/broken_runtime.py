"""Runtime-owned duplicate-to-broken heuristics."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any


def _bind_runtime(runtime: Any) -> None:
    for name, value in vars(runtime).items():
        if name in {
            "_bind_runtime",
            "mark_broken_from_dupe_groups_for_runtime",
            "_mark_broken_from_dupe_groups",
        }:
            continue
        globals()[name] = value


def mark_broken_from_dupe_groups_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> int:
    _bind_runtime(runtime)
    return _mark_broken_from_dupe_groups(*args, **kwargs)


def _mark_broken_from_dupe_groups(
    all_results: dict,
    editions_by_artist: dict[str, list[dict]] | None,
    *,
    ratio_threshold: float = 0.90,
    require_exact_identity: bool = True,
) -> int:
    """
    Heuristic: if a dupe group's best edition has notably more tracks than a loser,
    mark that loser as broken so the incomplete-move step can quarantine it.

    Safety:
    - By default requires at least one exact provider identity token overlap
      (Discogs release, Last.fm MBID, Bandcamp URL, or MB release ID).
    - This catches tail-truncated albums while avoiding broad "same title" false positives.
    """
    if not all_results or not editions_by_artist:
        return 0
    try:
        thr = float(ratio_threshold)
    except Exception:
        thr = 0.90
    thr = max(0.50, min(0.99, thr))

    by_id: dict[int, dict] = {}
    for _artist, eds in (editions_by_artist or {}).items():
        for e in (eds or []):
            try:
                aid = int(e.get("album_id") or 0)
            except Exception:
                aid = 0
            if aid > 0:
                by_id[aid] = e

    def _identity_tokens(edition: dict) -> set[str]:
        tokens: set[str] = set()
        mb_rel = (_dupe_get_mb_release_id(edition) or "").strip().lower()
        if mb_rel:
            tokens.add(f"mb_rel:{mb_rel}")
        discogs_id = (_dupe_get_discogs_id(edition) or "").strip()
        if discogs_id:
            tokens.add(f"discogs:{discogs_id}")
        lastfm_mbid = (_dupe_get_lastfm_mbid(edition) or "").strip().lower()
        if lastfm_mbid:
            tokens.add(f"lastfm:{lastfm_mbid}")
        bandcamp_url = (_dupe_get_bandcamp_url(edition) or "").strip().rstrip("/").lower()
        if bandcamp_url:
            tokens.add(f"bandcamp:{bandcamp_url}")
        return tokens

    def _track_indices(edition: dict) -> list[int]:
        idxs: list[int] = []
        try:
            for t in (edition.get("tracks") or []):
                try:
                    v = int(getattr(t, "idx", None) or 0)
                except Exception:
                    try:
                        v = int((t or {}).get("idx") or 0) if isinstance(t, dict) else 0
                    except Exception:
                        v = 0
                if v > 0:
                    idxs.append(v)
        except Exception:
            pass
        idxs = sorted(set(idxs))
        if idxs:
            return idxs

        # Files-mode editions often do not carry rich Track objects through all codepaths.
        # Fall back to parsing track numbers from filenames inside the edition folder.
        cached = edition.get("_fs_track_indices")
        if isinstance(cached, list) and cached:
            try:
                return sorted(set(int(x) for x in cached if int(x) > 0))
            except Exception:
                pass

        folder_raw = edition.get("folder")
        if not folder_raw:
            return []
        try:
            folder_path = path_for_fs_access(Path(str(folder_raw)))
        except Exception:
            try:
                folder_path = Path(str(folder_raw))
            except Exception:
                folder_path = None
        if not folder_path or (not folder_path.exists()) or (not folder_path.is_dir()):
            return []

        def _parse_track_idx(name: str) -> int:
            base = os.path.basename(name or "")
            if not base:
                return 0
            stem = Path(base).stem
            s = stem.strip()
            if not s:
                return 0
            # Common patterns:
            # - "01 Title"
            # - "1-01 Title" (disc-track)
            # - "CD1 01 Title"
            m = re.match(r"^\s*(?:cd\s*\d+\s*)?(\d{1,3})\b", s, flags=re.IGNORECASE)
            if m:
                try:
                    return int(m.group(1))
                except Exception:
                    return 0
            m2 = re.match(r"^\s*(\d{1,2})\s*[-_. ]\s*(\d{1,2})\b", s)
            if m2:
                try:
                    return int(m2.group(2))
                except Exception:
                    return 0
            return 0

        found: list[int] = []
        try:
            for p in folder_path.rglob("*"):
                if not p.is_file():
                    continue
                if not AUDIO_RE.search(p.name):
                    continue
                idx = _parse_track_idx(p.name)
                if idx > 0:
                    found.append(idx)
        except Exception:
            found = []
        found = sorted(set(found))
        try:
            if found:
                edition["_fs_track_indices"] = found
        except Exception:
            pass
        return found

    marked = 0
    for _artist, groups in (all_results or {}).items():
        for g in (groups or []):
            best = g.get("best") if isinstance(g, dict) else None
            losers = (g.get("losers") or []) if isinstance(g, dict) else []
            if not isinstance(best, dict) or not losers:
                continue
            best_ids = _identity_tokens(best)
            best_track_set = _dupe_track_title_set(best.get("tracks") or [])
            best_idxs = _track_indices(best)
            try:
                best_count = int(
                    best.get("actual_track_count")
                    or best.get("track_count")
                    or best.get("file_count")
                    or len(best.get("tracks") or [])
                )
            except Exception:
                best_count = 0
            if best_count < 3 and len(best_idxs) < 3:
                continue
            for loser in losers:
                if not isinstance(loser, dict):
                    continue
                try:
                    if loser.get("is_broken", False):
                        continue
                except Exception:
                    pass
                loser_idxs = _track_indices(loser)
                try:
                    loser_count = int(
                        loser.get("actual_track_count")
                        or loser.get("track_count")
                        or loser.get("file_count")
                        or len(loser.get("tracks") or [])
                    )
                except Exception:
                    loser_count = 0
                if loser_count <= 0:
                    continue
                if loser_count >= best_count:
                    continue
                if require_exact_identity:
                    loser_ids = _identity_tokens(loser)
                    if not (best_ids and loser_ids and (best_ids & loser_ids)):
                        continue

                # Prefer an index-based "missing in the middle" signal: missing indices <= max(loser_idx).
                missing_mid = []
                if best_idxs and loser_idxs:
                    try:
                        best_set = set(best_idxs)
                        loser_set = set(loser_idxs)
                        missing = sorted(best_set - loser_set)
                        if missing and (min(missing) <= max(loser_idxs)):
                            missing_mid = missing
                    except Exception:
                        missing_mid = []

                # Fallback: only mark as broken on track-count ratio when indices are unavailable.
                if (not missing_mid) and best_count > 0:
                    if (loser_count / best_count) >= thr:
                        continue
                    # Additional containment guard when using count-ratio fallback.
                    loser_track_set = _dupe_track_title_set(loser.get("tracks") or [])
                    containment = _dupe_track_title_containment(best_track_set, loser_track_set)
                    if containment < 0.98:
                        continue
                # Mark loser (group dict) and the canonical edition dict (by album_id) so later steps agree.
                try:
                    loser["is_broken"] = True
                    loser["expected_track_count"] = int(max(best_idxs) if best_idxs else best_count)
                    loser["actual_track_count"] = loser_count
                    if missing_mid:
                        loser["missing_indices"] = missing_mid[:5000]
                    else:
                        loser.setdefault("missing_indices", [])
                except Exception:
                    pass
                try:
                    loser_id = int(loser.get("album_id") or 0)
                except Exception:
                    loser_id = 0
                if loser_id > 0 and loser_id in by_id:
                    try:
                        e = by_id[loser_id]
                        e["is_broken"] = True
                        e["expected_track_count"] = int(max(best_idxs) if best_idxs else best_count)
                        e["actual_track_count"] = loser_count
                        if missing_mid:
                            e["missing_indices"] = missing_mid[:5000]
                        else:
                            e.setdefault("missing_indices", [])
                    except Exception:
                        pass
                marked += 1
    return marked


# ───────────────────────────── BACKGROUND TASKS (WEB) ─────────────────────────────
