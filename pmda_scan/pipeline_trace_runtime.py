"""Runtime-owned scan pipeline trace persistence and move reconciliation."""

from __future__ import annotations

import json
import logging
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List


_LOCAL_NAMES = {
    '_apply_scan_move_to_trace_rows',
    '_apply_scan_move_to_trace_rows_for_runtime',
    '_bind_runtime',
    '_edition_cached_format_text',
    '_edition_cached_format_text_for_runtime',
    '_edition_cached_has_cover',
    '_edition_cached_has_cover_for_runtime',
    '_reconcile_scan_move_trace_backlog',
    '_reconcile_scan_move_trace_backlog_for_runtime',
    '_scan_move_trace_pending_scan_ids',
    '_scan_move_trace_pending_scan_ids_for_runtime',
    '_scan_pipeline_trace_build_rows',
    '_scan_pipeline_trace_build_rows_for_runtime',
    '_scan_pipeline_trace_columns',
    '_scan_pipeline_trace_columns_for_runtime',
    '_scan_pipeline_trace_duplicate_lookup',
    '_scan_pipeline_trace_duplicate_lookup_for_runtime',
    '_scan_pipeline_trace_incomplete_lookup',
    '_scan_pipeline_trace_incomplete_lookup_for_runtime',
    '_scan_pipeline_trace_move_lookup',
    '_scan_pipeline_trace_move_lookup_for_runtime',
    '_scan_pipeline_trace_status',
    '_scan_pipeline_trace_status_for_runtime',
    '_scan_pipeline_trace_timeline',
    '_scan_pipeline_trace_timeline_for_runtime',
    '_scan_pipeline_trace_write_rows',
    '_scan_pipeline_trace_write_rows_for_runtime',
    '_sync_scan_pipeline_trace_move_rows',
    '_sync_scan_pipeline_trace_move_rows_for_runtime',
    'save_scan_pipeline_trace_artist_to_db',
    'save_scan_pipeline_trace_artist_to_db_for_runtime',
    'save_scan_pipeline_trace_to_db',
    'save_scan_pipeline_trace_to_db_for_runtime',
}


def _bind_runtime(runtime: Any) -> None:
    for name, value in vars(runtime).items():
        if name in _LOCAL_NAMES:
            continue
        globals()[name] = value

def _scan_pipeline_trace_columns_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _scan_pipeline_trace_columns(*args, **kwargs)

def _scan_pipeline_trace_move_lookup_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _scan_pipeline_trace_move_lookup(*args, **kwargs)

def _apply_scan_move_to_trace_rows_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _apply_scan_move_to_trace_rows(*args, **kwargs)

def _sync_scan_pipeline_trace_move_rows_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _sync_scan_pipeline_trace_move_rows(*args, **kwargs)

def _scan_move_trace_pending_scan_ids_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _scan_move_trace_pending_scan_ids(*args, **kwargs)

def _reconcile_scan_move_trace_backlog_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _reconcile_scan_move_trace_backlog(*args, **kwargs)

def _scan_pipeline_trace_incomplete_lookup_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _scan_pipeline_trace_incomplete_lookup(*args, **kwargs)

def _scan_pipeline_trace_duplicate_lookup_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _scan_pipeline_trace_duplicate_lookup(*args, **kwargs)

def _scan_pipeline_trace_status_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _scan_pipeline_trace_status(*args, **kwargs)

def _scan_pipeline_trace_timeline_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _scan_pipeline_trace_timeline(*args, **kwargs)

def _edition_cached_has_cover_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _edition_cached_has_cover(*args, **kwargs)

def _edition_cached_format_text_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _edition_cached_format_text(*args, **kwargs)

def _scan_pipeline_trace_build_rows_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _scan_pipeline_trace_build_rows(*args, **kwargs)

def _scan_pipeline_trace_write_rows_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _scan_pipeline_trace_write_rows(*args, **kwargs)

def save_scan_pipeline_trace_artist_to_db_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return save_scan_pipeline_trace_artist_to_db(*args, **kwargs)

def save_scan_pipeline_trace_to_db_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return save_scan_pipeline_trace_to_db(*args, **kwargs)


def _scan_pipeline_trace_columns(cur: sqlite3.Cursor) -> set[str]:
    try:
        cur.execute("PRAGMA table_info(scan_pipeline_trace)")
        return {str(r[1]) for r in cur.fetchall() if len(r) > 1}
    except Exception:
        return set()


def _scan_pipeline_trace_move_lookup(scan_id: int) -> dict[int, dict[str, Any]]:
    sid = int(scan_id or 0)
    if sid <= 0:
        return {}
    con = _state_connect_readonly(timeout=15)
    try:
        cur = con.cursor()
        cols_present = _scan_moves_columns(cur)
        source_expr = "COALESCE(source_path, '') AS source_path" if "source_path" in cols_present else "'' AS source_path"
        destination_expr = "COALESCE(destination_path, '') AS destination_path" if "destination_path" in cols_present else "'' AS destination_path"
        strategy_expr = "COALESCE(materialization_strategy, '') AS materialization_strategy" if "materialization_strategy" in cols_present else "'' AS materialization_strategy"
        arbitration_expr = "COALESCE(arbitration_result, '') AS arbitration_result" if "arbitration_result" in cols_present else "'' AS arbitration_result"
        cur.execute(
            f"""
            SELECT album_id,
                   COALESCE(move_reason, '') AS move_reason,
                   COALESCE(original_path, '') AS original_path,
                   COALESCE(moved_to_path, '') AS moved_to_path,
                   COALESCE(restored, 0) AS restored,
                   COALESCE(decision_provider, '') AS decision_provider,
                   COALESCE(decision_reason, '') AS decision_reason,
                   decision_confidence,
                   {source_expr},
                   {destination_expr},
                   {strategy_expr},
                   {arbitration_expr}
            FROM scan_moves
            WHERE scan_id = ?
            ORDER BY moved_at DESC
            """,
            (sid,),
        )
        rows = cur.fetchall()
    finally:
        con.close()
    out: dict[int, dict[str, Any]] = {}
    for row in rows:
        try:
            album_id = int(row["album_id"] or 0)
        except Exception:
            album_id = 0
        if album_id <= 0 or album_id in out:
            continue
        status = _scan_move_status(
            bool(row["restored"]),
            str(row["original_path"] or ""),
            str(row["moved_to_path"] or ""),
        )
        out[album_id] = {
            "move_reason": str(row["move_reason"] or "").strip().lower(),
            "move_status": status,
            "moved_to_path": str(row["moved_to_path"] or "").strip(),
            "decision_provider": str(row["decision_provider"] or "").strip(),
            "decision_reason": str(row["decision_reason"] or "").strip(),
            "decision_confidence": float(row["decision_confidence"]) if row["decision_confidence"] is not None else None,
            "source_path": str(row["source_path"] or row["original_path"] or "").strip(),
            "destination_path": str(row["destination_path"] or row["moved_to_path"] or "").strip(),
            "materialization_strategy": str(row["materialization_strategy"] or "").strip().lower(),
            "arbitration_result": str(row["arbitration_result"] or "").strip().lower(),
        }
    return out


def _apply_scan_move_to_trace_rows(
    cur: sqlite3.Cursor,
    *,
    scan_id: int,
    album_id: int,
    move_reason: str,
    moved_to_path: str,
    decision_provider: str,
    decision_reason: str,
    decision_confidence: float | None,
    move_status: str,
    source_path: str = "",
    destination_path: str = "",
    materialization_strategy: str = "",
    arbitration_result: str = "",
) -> bool:
    def _load_json_list(raw: Any) -> list[Any]:
        parsed = _safe_json_load(raw, fallback=[])
        return parsed if isinstance(parsed, list) else []

    def _load_json_obj(raw: Any) -> dict[str, Any]:
        parsed = _safe_json_load(raw, fallback={})
        return parsed if isinstance(parsed, dict) else {}

    trace_cols = _scan_pipeline_trace_columns(cur)
    if "scan_id" not in trace_cols or "album_id" not in trace_cols:
        return False
    cur.execute(
        """
        SELECT *
        FROM scan_pipeline_trace
        WHERE scan_id = ? AND album_id = ?
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        (int(scan_id or 0), int(album_id or 0)),
    )
    trace_row = cur.fetchone()
    if trace_row is None:
        return False
    metadata_source = str(trace_row["metadata_source"] or "").strip()
    strict_match_verified = bool(trace_row["strict_match_verified"])
    is_broken = bool(trace_row["is_broken"])
    dupe_role = str(trace_row["dupe_role"] or "").strip().lower()
    pipeline_status = _scan_pipeline_trace_status(
        move_reason=str(move_reason or "").strip().lower(),
        move_status=str(move_status or "none").strip().lower() or "none",
        is_broken=is_broken,
        dupe_role=dupe_role,
        strict_match_verified=strict_match_verified,
        metadata_source=metadata_source,
    )
    timeline = _load_json_list(trace_row["timeline_json"]) if "timeline_json" in trace_cols else []
    timeline = [entry for entry in timeline if str((entry or {}).get("stage") or "") != "move"]
    move_label = f"{str(move_status or '').title()} {str(move_reason or '').strip().lower()}".strip()
    if str(move_reason or "").strip().lower() == "matched_export" and str(move_status or "").strip().lower() == "moved":
        move_label = "Exported to library"
    elif str(move_reason or "").strip().lower() == "matched_export_conflict" and str(move_status or "").strip().lower() == "moved":
        move_label = "Moved duplicate on export"
    timeline.append(
        {
            "stage": "move",
            "label": move_label,
            "tone": "success" if str(move_status or "").strip().lower() == "moved" else "info",
        }
    )
    meta_summary = _load_json_obj(trace_row["meta_summary_json"]) if "meta_summary_json" in trace_cols else {}
    meta_summary["move"] = {
        "move_reason": str(move_reason or "").strip().lower(),
        "move_status": str(move_status or "").strip().lower() or "none",
        "moved_to_path": str(moved_to_path or "").strip(),
        "source_path": str(source_path or "").strip() or None,
        "destination_path": str(destination_path or moved_to_path or "").strip() or None,
        "materialization_strategy": str(materialization_strategy or "").strip().lower() or None,
        "arbitration_result": str(arbitration_result or "").strip().lower() or None,
        "decision_provider": _normalize_identity_provider(str(decision_provider or "")) or None,
        "decision_reason": str(decision_reason or "").strip() or None,
        "decision_confidence": float(decision_confidence) if decision_confidence is not None else None,
    }
    cur.execute(
        """
        UPDATE scan_pipeline_trace
        SET move_reason = ?,
            move_status = ?,
            moved_to_path = ?,
            decision_provider = ?,
            decision_reason = ?,
            decision_confidence = ?,
            pipeline_status = ?,
            timeline_json = ?,
            meta_summary_json = ?,
            updated_at = ?
        WHERE scan_id = ? AND album_id = ?
        """,
        (
            str(move_reason or "").strip().lower(),
            str(move_status or "").strip().lower() or "none",
            str(moved_to_path or "").strip(),
            _normalize_identity_provider(str(decision_provider or "")),
            str(decision_reason or "").strip(),
            float(decision_confidence) if decision_confidence is not None else None,
            pipeline_status,
            json.dumps(timeline, ensure_ascii=False),
            json.dumps(meta_summary, ensure_ascii=False, default=str),
            time.time(),
            int(scan_id or 0),
            int(album_id or 0),
        ),
    )
    return bool(cur.rowcount)


def _sync_scan_pipeline_trace_move_rows(
    scan_id: int,
    *,
    album_ids: Iterable[int] | None = None,
    wait_timeout_sec: float = 0.0,
    poll_interval_sec: float = 0.25,
) -> int:
    sid = int(scan_id or 0)
    if sid <= 0:
        return 0
    move_lookup = _scan_pipeline_trace_move_lookup(sid)
    if not move_lookup:
        return 0
    if album_ids is None:
        pending = {int(k) for k in move_lookup.keys() if int(k or 0) > 0}
    else:
        pending = {int(k) for k in album_ids if int(k or 0) > 0 and int(k) in move_lookup}
    if not pending:
        return 0
    deadline = time.time() + max(0.0, float(wait_timeout_sec or 0.0))
    synced = 0
    while pending:
        def _write_round() -> int:
            con = _state_connect()
            try:
                cur = con.cursor()
                round_synced = 0
                for album_id in list(pending):
                    move = move_lookup.get(int(album_id))
                    if not isinstance(move, dict):
                        pending.discard(album_id)
                        continue
                    updated = _apply_scan_move_to_trace_rows(
                        cur,
                        scan_id=sid,
                        album_id=int(album_id),
                        move_reason=str(move.get("move_reason") or "").strip().lower(),
                        moved_to_path=str(move.get("moved_to_path") or "").strip(),
                        decision_provider=str(move.get("decision_provider") or "").strip(),
                        decision_reason=str(move.get("decision_reason") or "").strip(),
                        decision_confidence=move.get("decision_confidence"),
                        move_status=str(move.get("move_status") or "none").strip().lower() or "none",
                        source_path=str(move.get("source_path") or "").strip(),
                        destination_path=str(move.get("destination_path") or "").strip(),
                        materialization_strategy=str(move.get("materialization_strategy") or "").strip().lower(),
                        arbitration_result=str(move.get("arbitration_result") or "").strip().lower(),
                    )
                    if updated:
                        pending.discard(album_id)
                        round_synced += 1
                if round_synced:
                    con.commit()
                else:
                    con.rollback()
                return round_synced
            finally:
                con.close()

        try:
            round_synced = _state_db_write_retry(
                _write_round,
                label=f"sync_scan_pipeline_trace_moves:{sid}",
                attempts=10,
            )
            synced += int(round_synced or 0)
        except sqlite3.OperationalError:
            logging.warning(
                "[Trace] X❌ Move trace sync failed after SQLite retries scan_id=%s pending=%s",
                sid,
                len(pending),
                exc_info=True,
            )
            break
        if not pending or time.time() >= deadline:
            break
        time.sleep(max(0.05, float(poll_interval_sec or 0.25)))
    return synced


def _scan_move_trace_pending_scan_ids(limit: int = 32) -> list[int]:
    con = _state_connect_readonly(timeout=15)
    try:
        cur = con.cursor()
        cur.execute(
            """
            SELECT DISTINCT sm.scan_id
            FROM scan_moves sm
            LEFT JOIN scan_pipeline_trace tr
              ON tr.scan_id = sm.scan_id
             AND tr.album_id = sm.album_id
            WHERE COALESCE(tr.move_status, 'none') = 'none'
            ORDER BY sm.scan_id DESC
            LIMIT ?
            """,
            (max(1, int(limit or 32)),),
        )
        rows = cur.fetchall()
    except Exception:
        logging.debug("Failed to enumerate pending scan move trace backlog", exc_info=True)
        return []
    finally:
        con.close()
    out: list[int] = []
    for row in rows:
        try:
            scan_id = int(row[0] or 0)
        except Exception:
            scan_id = 0
        if scan_id > 0 and scan_id not in out:
            out.append(scan_id)
    return out


def _reconcile_scan_move_trace_backlog(*, reason: str = "manual", limit_scans: int = 32) -> int:
    pending_scan_ids = _scan_move_trace_pending_scan_ids(limit=limit_scans)
    if not pending_scan_ids:
        return 0
    total_synced = 0
    for scan_id in pending_scan_ids:
        try:
            synced = _sync_scan_pipeline_trace_move_rows(int(scan_id))
        except Exception:
            logging.warning(
                "[Trace] X❌ Backlog move trace reconcile failed scan_id=%s reason=%s",
                int(scan_id),
                str(reason or "manual"),
                exc_info=True,
            )
            continue
        total_synced += int(synced or 0)
    if total_synced:
        logging.info(
            "[Trace] V✅ Reconciled %s move trace row(s) from backlog across %s scan(s) (%s)",
            int(total_synced),
            len(pending_scan_ids),
            str(reason or "manual"),
        )
    else:
        logging.info(
            "[Trace] »⏭ No pending move trace backlog rows required reconciliation (%s)",
            str(reason or "manual"),
        )
    return int(total_synced)


def _scan_pipeline_trace_incomplete_lookup(scan_id: int) -> dict[int, dict[str, Any]]:
    sid = int(scan_id or 0)
    if sid <= 0:
        return {}
    con = _state_connect_readonly(timeout=15)
    try:
        cur = con.cursor()
        cur.execute(
            """
            SELECT album_id,
                   COALESCE(classification, '') AS classification,
                   COALESCE(missing_in_plex, '[]') AS missing_in_plex,
                   COALESCE(missing_on_disk, '[]') AS missing_on_disk,
                   COALESCE(track_titles, '[]') AS track_titles,
                   expected_track_count,
                   actual_track_count
            FROM incomplete_album_diagnostics
            WHERE run_id = ?
            """,
            (sid,),
        )
        rows = cur.fetchall()
    finally:
        con.close()
    out: dict[int, dict[str, Any]] = {}
    for row in rows:
        try:
            album_id = int(row["album_id"] or 0)
        except Exception:
            album_id = 0
        if album_id <= 0:
            continue
        def _json_list(raw: Any) -> list[Any]:
            try:
                parsed = json.loads(str(raw or "[]"))
                return parsed if isinstance(parsed, list) else []
            except Exception:
                return []
        out[album_id] = {
            "classification": str(row["classification"] or "").strip(),
            "missing_in_plex": _json_list(row["missing_in_plex"]),
            "missing_from_index": _json_list(row["missing_in_plex"]),
            "missing_on_disk": _json_list(row["missing_on_disk"]),
            "track_titles": _json_list(row["track_titles"]),
            "expected_track_count": _parse_int_loose(row["expected_track_count"], 0),
            "actual_track_count": _parse_int_loose(row["actual_track_count"], 0),
        }
    return out


def _scan_pipeline_trace_duplicate_lookup(groups: list[dict] | None) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    for group in list(groups or []):
        best = group.get("best") if isinstance(group.get("best"), dict) else None
        losers = [e for e in list(group.get("losers") or []) if isinstance(e, dict)]
        editions = [e for e in list(group.get("editions") or []) if isinstance(e, dict)]
        peer_count = 0
        if best or losers:
            peer_count = (1 if best else 0) + len(losers)
        elif editions:
            peer_count = len(editions)
        if peer_count <= 0:
            continue
        ai_used = False
        ai_provider = ""
        ai_model = ""
        if best:
            ai_used = bool(best.get("used_ai"))
            ai_provider = str(best.get("ai_provider") or "").strip()
            ai_model = str(best.get("ai_model") or "").strip()
        if ai_used and (not ai_provider or not ai_model):
            mod = sys.modules[__name__]
            ai_provider = ai_provider or str(getattr(mod, "AI_PROVIDER", "") or "")
            ai_model = ai_model or str(getattr(mod, "RESOLVED_MODEL", None) or getattr(mod, "OPENAI_MODEL", None) or "")
        common = {
            "dupe_signal": str(group.get("dupe_signal") or "").strip(),
            "dupe_peer_count": int(peer_count),
            "dupe_needs_ai": 1 if bool(group.get("needs_ai")) else 0,
            "no_move": 1 if bool(group.get("no_move")) else 0,
            "manual_review": 1 if bool(group.get("manual_review")) else 0,
            "same_folder": 1 if bool(group.get("same_folder")) else 0,
            "winner_album_id": int(best.get("album_id") or 0) if best else None,
            "winner_title": str((best or {}).get("title_raw") or (best or {}).get("album_norm") or "").strip(),
            "ai_used": 1 if ai_used else 0,
            "ai_provider": ai_provider,
            "ai_model": ai_model,
        }
        if best and losers:
            try:
                best_album_id = int(best.get("album_id") or 0)
            except Exception:
                best_album_id = 0
            if best_album_id > 0:
                out[best_album_id] = {**common, "dupe_role": "winner"}
            for loser in losers:
                try:
                    loser_album_id = int(loser.get("album_id") or 0)
                except Exception:
                    loser_album_id = 0
                if loser_album_id > 0:
                    out[loser_album_id] = {**common, "dupe_role": "loser"}
            continue
        for edition in editions:
            try:
                album_id = int(edition.get("album_id") or 0)
            except Exception:
                album_id = 0
            if album_id <= 0:
                continue
            out[album_id] = {**common, "dupe_role": "candidate"}
    return out


def _scan_pipeline_trace_status(
    *,
    move_reason: str,
    move_status: str,
    is_broken: bool,
    dupe_role: str,
    strict_match_verified: bool,
    metadata_source: str,
) -> str:
    mr = str(move_reason or "").strip().lower()
    ms = str(move_status or "").strip().lower()
    role = str(dupe_role or "").strip().lower()
    if mr == "dedupe" and ms == "moved":
        return "moved_duplicate"
    if mr == "incomplete" and ms == "moved":
        return "moved_incomplete"
    if mr == "matched_export_conflict" and ms == "moved":
        return "moved_duplicate"
    if mr == "matched_export" and ms == "moved":
        return "exported"
    if mr == "dedupe" and ms == "restored":
        return "restored_duplicate"
    if mr == "incomplete" and ms == "restored":
        return "restored_incomplete"
    if bool(is_broken):
        return "incomplete"
    if role == "loser":
        return "duplicate_loser"
    if role == "winner":
        return "duplicate_winner"
    if role == "candidate":
        return "duplicate_candidate"
    if bool(strict_match_verified):
        return "matched"
    if str(metadata_source or "").strip():
        return "provider_only"
    return "unmatched"


def _scan_pipeline_trace_timeline(
    *,
    metadata_source: str,
    strict_match_verified: bool,
    strict_match_provider: str,
    strict_reject_reason: str,
    has_cover: bool,
    missing_required_tags: list[str],
    is_broken: bool,
    expected_track_count: int,
    actual_track_count: int,
    dupe_role: str,
    dupe_signal: str,
    move_reason: str,
    move_status: str,
    ai_used: bool,
    ai_provider: str,
) -> list[dict[str, str]]:
    timeline: list[dict[str, str]] = [{"stage": "scan", "label": "Detected", "tone": "neutral"}]
    provider_label = _normalize_identity_provider(strict_match_provider or metadata_source or "").strip() or "provider"
    if strict_match_verified:
        timeline.append({"stage": "match", "label": f"Strict via {provider_label}", "tone": "success"})
    elif metadata_source:
        timeline.append({"stage": "match", "label": f"Matched via {_normalize_identity_provider(metadata_source)}", "tone": "info"})
    elif strict_reject_reason:
        timeline.append({"stage": "match", "label": str(strict_reject_reason), "tone": "warning"})
    else:
        timeline.append({"stage": "match", "label": "Unmatched", "tone": "warning"})
    timeline.append({"stage": "cover", "label": "Cover" if has_cover else "No cover", "tone": "success" if has_cover else "warning"})
    if missing_required_tags:
        timeline.append(
            {
                "stage": "tags",
                "label": f"{len(missing_required_tags)} missing tag(s)",
                "tone": "warning",
            }
        )
    else:
        timeline.append({"stage": "tags", "label": "Tags complete", "tone": "success"})
    role = str(dupe_role or "").strip().lower()
    if role == "winner":
        label = "Duplicate winner"
        if dupe_signal:
            label = f"{label} · {dupe_signal}"
        timeline.append({"stage": "dupe", "label": label, "tone": "info"})
    elif role == "loser":
        label = "Duplicate loser"
        if dupe_signal:
            label = f"{label} · {dupe_signal}"
        timeline.append({"stage": "dupe", "label": label, "tone": "warning"})
    elif role == "candidate":
        timeline.append({"stage": "dupe", "label": "Duplicate candidate", "tone": "warning"})
    if is_broken:
        counts = ""
        if expected_track_count or actual_track_count:
            counts = f" {actual_track_count}/{expected_track_count}".strip()
        timeline.append({"stage": "quality", "label": f"Incomplete {counts}".strip(), "tone": "warning"})
    mr = str(move_reason or "").strip().lower()
    ms = str(move_status or "").strip().lower()
    if mr and mr != "none" and ms and ms != "none":
        move_label = f"{ms.title()} {mr}"
        if mr == "matched_export" and ms == "moved":
            move_label = "Exported to library"
        elif mr == "matched_export_conflict" and ms == "moved":
            move_label = "Moved duplicate on export"
        timeline.append({"stage": "move", "label": move_label, "tone": "success" if ms == "moved" else "info"})
    if ai_used:
        timeline.append({"stage": "ai", "label": f"AI {ai_provider or 'used'}", "tone": "info"})
    return timeline


def _edition_cached_has_cover(edition: dict | None, meta: dict | None = None) -> bool:
    """Return cover state from scan metadata without touching the album folder."""
    edition = edition if isinstance(edition, dict) else {}
    meta = meta if isinstance(meta, dict) else {}
    for key in ("has_cover", "cover", "album_has_cover"):
        value = edition.get(key)
        if isinstance(value, bool):
            if value:
                return True
        elif isinstance(value, (int, float)):
            if value:
                return True
        elif isinstance(value, str) and value.strip():
            if _parse_bool(value):
                return True
    for key in ("cover_path", "album_cover_path", "artwork_path", "image_path"):
        if str(edition.get(key) or meta.get(key) or "").strip():
            return True
    try:
        if _pmda_bool_from_str(str(meta.get(PMDA_COVER_TAG) or "")):
            return True
    except Exception:
        pass
    return False


def _edition_cached_format_text(edition: dict | None, meta: dict | None = None) -> str:
    """Return the primary format from already-collected metadata only."""
    edition = edition if isinstance(edition, dict) else {}
    meta = meta if isinstance(meta, dict) else {}
    for value in (
        edition.get("fmt_text"),
        edition.get("fmt"),
        edition.get("format"),
        edition.get("primary_format"),
        meta.get("fmt_text"),
        meta.get("format"),
        meta.get("FORMAT"),
    ):
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _scan_pipeline_trace_build_rows(
    *,
    scan_id: int,
    artist_name: str,
    editions_list: list[dict],
    groups: list[dict] | None = None,
    move_lookup: dict[int, dict[str, Any]] | None = None,
    incomplete_lookup: dict[int, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    dupe_lookup = _scan_pipeline_trace_duplicate_lookup(groups)
    move_lookup = dict(move_lookup or {})
    incomplete_lookup = dict(incomplete_lookup or {})
    for edition in list(editions_list or []):
        if not isinstance(edition, dict):
            continue
        try:
            album_id = int(edition.get("album_id") or 0)
        except Exception:
            album_id = 0
        if album_id <= 0:
            continue
        folder = str(edition.get("folder") or "").strip()
        folder_name = ""
        if folder:
            try:
                folder_name = (Path(folder).name or "").strip()
            except Exception:
                folder_name = ""
        artist_resolved, title_resolved = _apply_resolved_identity_to_edition(
            edition,
            default_artist=str(artist_name or ""),
            default_title=str(edition.get("title_raw") or edition.get("album_title") or ""),
            folder_name=folder_name,
        )
        meta = dict(edition.get("meta", {}) or {})
        if edition.get("primary_metadata_source"):
            meta["primary_metadata_source"] = edition.get("primary_metadata_source")
        try:
            meta_json = json.dumps(meta, default=str)
        except Exception:
            meta_json = "{}"
        has_cover = 1 if _edition_cached_has_cover(edition, meta) else 0
        try:
            missing_required_tags = _check_required_tags(meta, REQUIRED_TAGS, edition=edition)
        except Exception:
            missing_required_tags = []
        missing_required_json = json.dumps(missing_required_tags, default=str) if missing_required_tags else None
        discogs_release_id = str(edition.get("discogs_release_id") or meta.get("discogs_release_id") or "").strip()
        lastfm_album_mbid = str(edition.get("lastfm_album_mbid") or meta.get("lastfm_album_mbid") or "").strip()
        bandcamp_album_url = str(edition.get("bandcamp_album_url") or meta.get("bandcamp_album_url") or "").strip()
        musicbrainz_release_id = str(
            edition.get("musicbrainz_release_id")
            or meta.get("musicbrainz_releaseid")
            or meta.get("musicbrainz_release_id")
            or meta.get("musicbrainz_albumid")
            or ""
        ).strip()
        metadata_source = _normalize_identity_provider(
            str(
                edition.get("primary_metadata_source")
                or edition.get("metadata_source")
                or meta.get("primary_metadata_source")
                or meta.get(PMDA_MATCH_PROVIDER_TAG)
                or ""
            )
        )
        strict_match_verified = 1 if bool(edition.get("strict_match_verified")) else 0
        strict_match_provider = _normalize_identity_provider(str(edition.get("strict_match_provider") or ""))
        strict_reject_reason = str(edition.get("strict_reject_reason") or "").strip()
        strict_tracklist_score = float(edition.get("strict_tracklist_score") or 0.0)
        dupe_info = dict(dupe_lookup.get(album_id) or {})
        move_info = dict(move_lookup.get(album_id) or {})
        incomplete_info = dict(incomplete_lookup.get(album_id) or {})
        if incomplete_info:
            expected_track_count = _parse_int_loose(
                incomplete_info.get("expected_track_count"),
                edition.get("expected_track_count"),
            )
            actual_track_count = _parse_int_loose(
                incomplete_info.get("actual_track_count"),
                edition.get("actual_track_count") or len(edition.get("tracks", [])),
            )
            missing_indices_value = incomplete_info.get("missing_on_disk") or edition.get("missing_indices") or []
        else:
            expected_track_count = _parse_int_loose(edition.get("expected_track_count"), 0)
            actual_track_count = _parse_int_loose(edition.get("actual_track_count"), len(edition.get("tracks", [])))
            missing_indices_value = edition.get("missing_indices") or []
        try:
            missing_indices_json = json.dumps(list(missing_indices_value or []), default=str)
        except Exception:
            missing_indices_json = "[]"
        is_broken = 1 if bool(edition.get("is_broken")) or bool(incomplete_info) else 0
        dupe_role = str(dupe_info.get("dupe_role") or "none").strip().lower() or "none"
        dupe_signal = str(dupe_info.get("dupe_signal") or "").strip()
        move_reason = str(move_info.get("move_reason") or "").strip().lower()
        move_status = str(move_info.get("move_status") or "none").strip().lower() or "none"
        ai_used = 1 if bool(dupe_info.get("ai_used")) else 0
        ai_provider = str(dupe_info.get("ai_provider") or "").strip()
        ai_model = str(dupe_info.get("ai_model") or "").strip()
        pipeline_status = _scan_pipeline_trace_status(
            move_reason=move_reason,
            move_status=move_status,
            is_broken=bool(is_broken),
            dupe_role=dupe_role,
            strict_match_verified=bool(strict_match_verified),
            metadata_source=metadata_source,
        )
        timeline = _scan_pipeline_trace_timeline(
            metadata_source=metadata_source,
            strict_match_verified=bool(strict_match_verified),
            strict_match_provider=strict_match_provider,
            strict_reject_reason=strict_reject_reason,
            has_cover=bool(has_cover),
            missing_required_tags=list(missing_required_tags or []),
            is_broken=bool(is_broken),
            expected_track_count=expected_track_count,
            actual_track_count=actual_track_count,
            dupe_role=dupe_role,
            dupe_signal=dupe_signal,
            move_reason=move_reason,
            move_status=move_status,
            ai_used=bool(ai_used),
            ai_provider=ai_provider,
        )
        meta_summary = {
            "providers": {
                "musicbrainz": bool(musicbrainz_release_id),
                "discogs": bool(discogs_release_id),
                "lastfm": bool(lastfm_album_mbid),
                "bandcamp": bool(bandcamp_album_url),
            },
            "provider_identity": {
                "confidence": edition.get("provider_identity_confidence"),
                "confidence_tier": str(edition.get("provider_identity_confidence_tier") or "").strip(),
                "reason": str(edition.get("provider_identity_confidence_reason") or "").strip(),
                "soft_reason": str(edition.get("provider_identity_soft_reason") or "").strip(),
            },
            "move": move_info or None,
            "duplicate": dupe_info or None,
            "incomplete": incomplete_info or None,
            "metadata_source": metadata_source,
        }
        rows.append(
            {
                "scan_id": int(scan_id),
                "artist": artist_resolved,
                "album_id": album_id,
                "album_title": title_resolved,
                "folder": folder,
                "folder_name": folder_name,
                "fmt_text": _edition_cached_format_text(edition, meta),
                "metadata_source": metadata_source,
                "strict_match_verified": strict_match_verified,
                "strict_match_provider": strict_match_provider,
                "strict_reject_reason": strict_reject_reason,
                "strict_tracklist_score": strict_tracklist_score,
                "has_cover": has_cover,
                "is_broken": is_broken,
                "expected_track_count": expected_track_count,
                "actual_track_count": actual_track_count,
                "missing_indices": missing_indices_json,
                "missing_required_tags": missing_required_json,
                "has_musicbrainz": 1 if musicbrainz_release_id else 0,
                "has_discogs": 1 if discogs_release_id else 0,
                "has_lastfm": 1 if lastfm_album_mbid else 0,
                "has_bandcamp": 1 if bandcamp_album_url else 0,
                "musicbrainz_release_id": musicbrainz_release_id,
                "discogs_release_id": discogs_release_id,
                "lastfm_album_mbid": lastfm_album_mbid,
                "bandcamp_album_url": bandcamp_album_url,
                "dupe_role": dupe_role,
                "dupe_signal": dupe_signal,
                "dupe_peer_count": int(dupe_info.get("dupe_peer_count") or 0),
                "dupe_needs_ai": 1 if bool(dupe_info.get("dupe_needs_ai")) else 0,
                "no_move": 1 if bool(dupe_info.get("no_move")) else 0,
                "manual_review": 1 if bool(dupe_info.get("manual_review")) else 0,
                "same_folder": 1 if bool(dupe_info.get("same_folder")) else 0,
                "winner_album_id": _parse_int_loose(dupe_info.get("winner_album_id"), 0) or None,
                "winner_title": str(dupe_info.get("winner_title") or "").strip(),
                "ai_used": ai_used,
                "ai_provider": ai_provider,
                "ai_model": ai_model,
                "pipeline_status": pipeline_status,
                "move_reason": move_reason,
                "move_status": move_status,
                "moved_to_path": str(move_info.get("moved_to_path") or "").strip(),
                "decision_provider": str(move_info.get("decision_provider") or "").strip(),
                "decision_reason": str(move_info.get("decision_reason") or "").strip(),
                "decision_confidence": move_info.get("decision_confidence"),
                "timeline_json": json.dumps(timeline, ensure_ascii=False),
                "meta_summary_json": json.dumps(meta_summary, ensure_ascii=False, default=str),
                "updated_at": time.time(),
            }
        )
    return rows


def _scan_pipeline_trace_write_rows(
    cur: sqlite3.Cursor,
    rows: list[dict[str, Any]],
) -> int:
    cols_present = _scan_pipeline_trace_columns(cur)
    written = 0
    for row in rows:
        ordered_keys = [key for key in row.keys() if key in cols_present]
        placeholders = ", ".join("?" for _ in ordered_keys)
        values = [row.get(key) for key in ordered_keys]
        cur.execute(
            f"""
            INSERT OR REPLACE INTO scan_pipeline_trace ({", ".join(ordered_keys)})
            VALUES ({placeholders})
            """,
            values,
        )
        written += 1
    return written


def save_scan_pipeline_trace_artist_to_db(
    scan_id: int,
    artist_name: str,
    editions_list: List[dict],
    groups: List[dict] | None = None,
) -> int:
    sid = int(scan_id or 0)
    move_lookup = _scan_pipeline_trace_move_lookup(sid)
    incomplete_lookup = _scan_pipeline_trace_incomplete_lookup(sid)
    rows = _scan_pipeline_trace_build_rows(
        scan_id=sid,
        artist_name=str(artist_name or ""),
        editions_list=list(editions_list or []),
        groups=list(groups or []),
        move_lookup=move_lookup,
        incomplete_lookup=incomplete_lookup,
    )
    def _write() -> int:
        con = _state_connect()
        try:
            cur = con.cursor()
            cur.execute("DELETE FROM scan_pipeline_trace WHERE scan_id = ? AND artist = ?", (sid, str(artist_name or "")))
            written = _scan_pipeline_trace_write_rows(cur, rows)
            con.commit()
            return written
        finally:
            con.close()

    return _state_db_write_retry(
        _write,
        label=f"save_scan_pipeline_trace_artist_to_db:{sid}:{artist_name}",
        attempts=12,
    )


def save_scan_pipeline_trace_to_db(
    scan_id: int,
    all_editions_by_artist: Dict[str, List[dict]],
    all_results: Dict[str, List[dict]] | None = None,
    progress_callback=None,
) -> int:
    sid = int(scan_id or 0)
    if sid <= 0:
        return 0
    all_results = dict(all_results or {})
    move_lookup = _scan_pipeline_trace_move_lookup(sid)
    incomplete_lookup = _scan_pipeline_trace_incomplete_lookup(sid)
    artist_items = list((all_editions_by_artist or {}).items())
    artist_total = len(artist_items)
    if progress_callback:
        try:
            progress_callback(0, artist_total, "Preparing pipeline trace rows")
        except Exception:
            pass
    def _write() -> int:
        con = _state_connect()
        try:
            cur = con.cursor()
            cur.execute("DELETE FROM scan_pipeline_trace WHERE scan_id = ?", (sid,))
            total = 0
            for idx, (artist_name, editions_list) in enumerate(artist_items, start=1):
                rows = _scan_pipeline_trace_build_rows(
                    scan_id=sid,
                    artist_name=str(artist_name or ""),
                    editions_list=list(editions_list or []),
                    groups=list(all_results.get(artist_name) or []),
                    move_lookup=move_lookup,
                    incomplete_lookup=incomplete_lookup,
                )
                total += _scan_pipeline_trace_write_rows(cur, rows)
                if progress_callback and (idx == 1 or idx % 25 == 0 or idx == artist_total):
                    try:
                        progress_callback(
                            idx,
                            artist_total,
                            f"Saving pipeline trace ({idx:,}/{artist_total:,} artists)",
                        )
                    except Exception:
                        pass
            con.commit()
            logging.debug("save_scan_pipeline_trace_to_db: scan_id=%s rows=%s", sid, total)
            return total
        finally:
            con.close()

    return _state_db_write_retry(_write, label=f"save_scan_pipeline_trace_to_db:{sid}", attempts=12)
