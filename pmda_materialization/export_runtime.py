"""Runtime-owned materialization of verified albums into the matched library."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any


def _bind_runtime(runtime: Any) -> None:
    for name, value in vars(runtime).items():
        if name in {
            "_bind_runtime",
            "move_publish_items_to_matched_library_for_runtime",
            "_move_publish_items_to_matched_library",
        }:
            continue
        globals()[name] = value


def move_publish_items_to_matched_library_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> list[dict]:
    _bind_runtime(runtime)
    return _move_publish_items_to_matched_library(*args, **kwargs)


def _move_publish_items_to_matched_library(
    artist_name: str,
    items: list[dict],
    *,
    export_root: str | None = None,
    scan_id_override: int | None = None,
) -> list[dict]:
    if library_is_audit_mode():
        return items
    if not items or _get_library_mode() != "files" or not PIPELINE_ENABLE_EXPORT:
        return items
    export_root_raw = str(export_root or EXPORT_ROOT or "").strip()
    if not export_root_raw:
        return items
    export_strategy = str(EXPORT_LINK_STRATEGY or "hardlink").strip().lower()
    if export_strategy not in {"move", "hardlink", "symlink", "copy"}:
        export_strategy = "hardlink"
    export_root_path = path_for_fs_access(Path(export_root_raw))
    updated_items: list[dict] = []
    for item in items:
        folder_raw = str(item.get("folder") or "").strip()
        if not folder_raw:
            updated_items.append(item)
            continue
        confidence_policy = _materialization_confidence_policy(item)
        item["materialization_confidence_tier"] = confidence_policy.get("tier")
        item["materialization_confidence"] = confidence_policy.get("confidence")
        item["materialization_decision_reason"] = confidence_policy.get("reason")
        if not bool(confidence_policy.get("auto_materialize")):
            updated_items.append(item)
            continue
        src_folder = path_for_fs_access(Path(folder_raw))
        if not src_folder.exists() or not src_folder.is_dir():
            updated_items.append(item)
            continue
        if _path_is_under_root(src_folder, export_root_path):
            updated_items.append(item)
            continue
        if _path_is_under_root(src_folder, DUPE_ROOT):
            updated_items.append(item)
            continue
        incomplete_root = Path(str(_get_config_from_db("INCOMPLETE_ALBUMS_TARGET_DIR") or "/dupes/incomplete_albums").strip() or "/dupes/incomplete_albums")
        if _path_is_under_root(src_folder, incomplete_root):
            updated_items.append(item)
            continue

        folder_name = src_folder.name
        artist_resolved, title_resolved = _apply_resolved_identity_to_edition(
            item,
            default_artist=str(item.get("artist_name") or item.get("artist") or artist_name or "").strip(),
            default_title=str(item.get("album_title") or item.get("title_raw") or folder_name or "").strip(),
            folder_name=folder_name,
        )
        artist_clean = str(artist_resolved or artist_name or "").strip() or "Unknown Artist"
        album_clean = _sanitize_album_title_display(str(title_resolved or item.get("album_title") or item.get("title_raw") or folder_name or "").strip())
        format_value = str(item.get("fmt_text") or get_primary_format(src_folder) or "").strip()
        decision_provider = _normalize_identity_provider(
            str(
                item.get("strict_match_provider")
                or item.get("metadata_source")
                or item.get("primary_metadata_source")
                or ""
            )
        )
        decision_provider = decision_provider or str(confidence_policy.get("provider") or "")
        album_type = _derive_album_type_for_folder_name(
            track_count=int(item.get("actual_track_count") or len(item.get("tracks") or [])),
            tags=dict(item.get("meta") or {}),
            album_title=album_clean,
            artist_name=artist_clean,
        )
        target_folder = _matched_export_target_folder(
            artist_name=artist_clean,
            album_title=album_clean,
            format_value=format_value,
            album_type=album_type,
            export_root=export_root_raw,
        )
        source_candidate = _build_source_dupe_candidate_from_item(item, src_folder, artist_name=artist_clean, album_title=album_clean)
        if source_candidate is None:
            updated_items.append(item)
            continue
        existing_folders = [
            p for p in _matched_destination_conflict_folders(target_folder, artist_name=artist_clean, album_title=album_clean)
            if str(p) != str(src_folder)
        ]
        existing_candidates: list[dict] = []
        for existing_folder in existing_folders:
            candidate = _build_dupe_candidate_from_folder(existing_folder, artist_hint=artist_clean, album_hint=album_clean)
            if candidate is not None:
                existing_candidates.append(candidate)
        if export_strategy in {"move", "copy"} and target_folder.exists() and _folders_are_hardlink_mirror(src_folder, target_folder):
            moved_to = _materialize_hardlink_mirror(src_folder, target_folder, export_strategy)
            try:
                _files_watcher_suppress_folder(moved_to, seconds=180.0, reason="pmda_matched_export_materialize")
            except Exception:
                pass
            if export_strategy == "move":
                _files_forget_album_folder_global(src_folder)
            item["folder"] = str(moved_to)
            item["ordered_paths"] = []
            _record_scan_move_event(
                scan_id_override=_export_item_scan_id(item, scan_id_override),
                artist_name=artist_clean,
                album_id=int(item.get("album_id") or 0),
                album_title=album_clean,
                fmt_text=format_value,
                original_path=str(src_folder),
                moved_to_path=str(moved_to),
                size_mb=max(0, folder_size(moved_to) // (1024 * 1024)),
                move_reason="matched_export",
                decision_source="pipeline_export",
                decision_provider=decision_provider,
                decision_reason=f"{export_strategy}_{confidence_policy.get('tier')}_replace_hardlink_mirror",
                decision_confidence=float(confidence_policy.get("confidence") or item.get("strict_tracklist_score") or 1.0),
                materialization_strategy=export_strategy,
                arbitration_result="materialized_existing",
                details={
                    "export_strategy": export_strategy,
                    "materialized_existing_hardlink_mirror": True,
                    "source": str(src_folder),
                    "destination": str(moved_to),
                    "provider": decision_provider,
                    "confidence_tier": confidence_policy.get("tier"),
                    "confidence_reason": confidence_policy.get("reason"),
                },
            )
            logging.info(
                "[MATERIALIZE] [V✅] %s materialized existing library album: %s -> %s",
                export_strategy,
                src_folder,
                moved_to,
            )
            updated_items.append(item)
            continue
        if existing_candidates:
            best, rationale, conflict_confident = _choose_matched_export_conflict_winner(source_candidate, existing_candidates)
            if best is None or not conflict_confident:
                item["export_conflict_review"] = True
                item["export_conflict_reason"] = str(rationale or "matched destination conflict needs review")
                _record_matched_export_conflict_review(
                    item=item,
                    source_candidate=source_candidate,
                    existing_candidates=existing_candidates,
                    artist_name=artist_clean,
                    album_title=album_clean,
                    fmt_text=format_value,
                    rationale=str(rationale or "matched destination conflict needs review"),
                    decision_provider=decision_provider,
                    export_strategy=export_strategy,
                    scan_id_override=scan_id_override,
                )
                logging.warning(
                    "[LIBRARY] [!⚠] held matched destination conflict for review; no folders moved for %s – %s (%s)",
                    artist_clean,
                    album_clean,
                    rationale,
                )
                updated_items.append(item)
                continue
            if best is not None and str(best.get("folder") or "") != str(source_candidate.get("folder") or ""):
                winner_folder = path_for_fs_access(Path(str(best.get("folder") or "").strip()))
                for loser in existing_candidates:
                    loser_folder_raw = str(loser.get("folder") or "").strip()
                    if not loser_folder_raw or loser_folder_raw == str(winner_folder):
                        continue
                    _move_folder_to_dupes(
                        Path(loser_folder_raw),
                        artist_hint=artist_clean,
                        album_hint=album_clean,
                        reason="pmda_matched_destination_dupe",
                    )
                moved_dupe = _move_folder_to_dupes(
                    src_folder,
                    artist_hint=artist_clean,
                    album_hint=album_clean,
                    reason="pmda_matched_destination_dupe",
                )
                _record_scan_move_event(
                    scan_id_override=_export_item_scan_id(item, scan_id_override),
                    artist_name=artist_clean,
                    album_id=int(item.get("album_id") or 0),
                    album_title=album_clean,
                    fmt_text=format_value,
                    original_path=str(src_folder),
                    moved_to_path=str(moved_dupe or ""),
                    size_mb=max(0, folder_size(path_for_fs_access(Path(str(moved_dupe)))) // (1024 * 1024)) if moved_dupe else 0,
                    move_reason="matched_export_conflict",
                    winner_album_id=int(best.get("album_id") or 0) or None,
                    winner_title=str(best.get("title_raw") or album_clean),
                    winner_path=str(winner_folder),
                    decision_source="pipeline_export",
                    decision_provider=decision_provider or _normalize_identity_provider(str(best.get("strict_match_provider") or best.get("metadata_source") or "")),
                    decision_reason=str(rationale or "existing_destination_winner"),
                    decision_confidence=float(best.get("strict_tracklist_score") or confidence_policy.get("confidence") or item.get("strict_tracklist_score") or 1.0),
                    materialization_strategy=export_strategy,
                    arbitration_result="kept_existing",
                    details={
                        "export_strategy": export_strategy,
                        "source": str(src_folder),
                        "winner_folder": str(winner_folder),
                        "provider": decision_provider,
                        "kind": "destination_conflict_loser",
                        "confidence_tier": confidence_policy.get("tier"),
                        "confidence_reason": confidence_policy.get("reason"),
                    },
                )
                item["folder"] = str(winner_folder)
                item["ordered_paths"] = []
                logging.info(
                    "[LIBRARY] [»⏭] kept existing destination winner for %s – %s (%s)",
                    artist_clean,
                    album_clean,
                    rationale,
                )
                updated_items.append(item)
                continue
            for loser in existing_candidates:
                loser_folder_raw = str(loser.get("folder") or "").strip()
                if not loser_folder_raw:
                    continue
                _move_folder_to_dupes(
                    Path(loser_folder_raw),
                    artist_hint=artist_clean,
                    album_hint=album_clean,
                    reason="pmda_matched_destination_dupe",
                )
        moved_to = _place_folder_with_strategy(src_folder, target_folder, export_strategy)
        try:
            _files_watcher_suppress_folder(moved_to, seconds=180.0, reason="pmda_matched_export")
        except Exception:
            pass
        if export_strategy == "move":
            _files_forget_album_folder_global(src_folder)
        item["folder"] = str(moved_to)
        item["ordered_paths"] = []
        _record_scan_move_event(
            scan_id_override=_export_item_scan_id(item, scan_id_override),
            artist_name=artist_clean,
            album_id=int(item.get("album_id") or 0),
            album_title=album_clean,
            fmt_text=format_value,
            original_path=str(src_folder),
            moved_to_path=str(moved_to),
            size_mb=max(0, folder_size(moved_to) // (1024 * 1024)),
            move_reason="matched_export",
            decision_source="pipeline_export",
            decision_provider=decision_provider,
            decision_reason=f"{export_strategy}_{confidence_policy.get('tier')}",
            decision_confidence=float(confidence_policy.get("confidence") or item.get("strict_tracklist_score") or 1.0),
            materialization_strategy=export_strategy,
            arbitration_result="promoted",
            details={
                "export_strategy": export_strategy,
                "source": str(src_folder),
                "destination": str(moved_to),
                "provider": decision_provider,
                "strict_match_verified": True,
                "confidence_tier": confidence_policy.get("tier"),
                "confidence_reason": confidence_policy.get("reason"),
            },
        )
        action_label = {
            "hardlink": "hardlinked",
            "copy": "copied",
            "move": "moved",
            "symlink": "symlinked",
        }.get(export_strategy, export_strategy)
        logging.info(
            "[LIBRARY] [V✅] %s album into library: %s -> %s",
            action_label,
            src_folder,
            moved_to,
        )
        updated_items.append(item)
    return updated_items
