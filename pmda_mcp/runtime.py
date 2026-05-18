"""Runtime-bound MCP access, analytics, and tool dispatch helpers."""
from __future__ import annotations

from typing import Any

_EXTRACTED_NAMES = {
    '_mcp_token_hash',
    '_mcp_enabled',
    '_mcp_normalize_scopes',
    '_mcp_row_to_token',
    '_mcp_active_token_snapshot',
    '_mcp_latest_audit',
    '_mcp_status_summary',
    '_mcp_generate_token',
    '_mcp_revoke_active_tokens',
    '_mcp_authenticate_token',
    '_mcp_request_tool_and_args',
    '_mcp_auth_guard',
    '_mcp_scrub_args',
    '_mcp_audit',
    '_mcp_require_scope',
    '_mcp_route_payload',
    '_mcp_scan_status',
    '_mcp_scan_history',
    '_mcp_logs_tail',
    '_mcp_duplicate_groups',
    '_mcp_incomplete_albums',
    '_mcp_safe_limit',
    '_mcp_percent',
    '_mcp_scan_id_from_args',
    '_mcp_scan_history_row',
    '_mcp_scan_trace_where',
    '_mcp_scan_trace_summary',
    '_mcp_scan_pipeline_trace',
    '_mcp_scan_moves',
    '_mcp_scan_resume_state',
    '_mcp_cache_stats',
    '_mcp_provider_cache_stats',
    '_mcp_musicbrainz_cache_stats',
    '_mcp_review_proposals',
    '_mcp_sqlite_columns',
    '_mcp_review_stats',
    '_mcp_iso',
    '_mcp_pg_table_columns',
    '_mcp_pg_group_counts',
    '_mcp_enrichment_stats_from_cursor',
    '_mcp_enrichment_stats',
    '_mcp_library_stats',
    '_mcp_scan_analytics',
    '_mcp_scan_results',
    '_mcp_library_search',
    '_mcp_create_review_proposal',
    '_mcp_storage_current',
    '_mcp_storage_plan',
    '_mcp_dispatch_tool',
}


def _bind_runtime(runtime: Any) -> None:
    for name, value in vars(runtime).items():
        if name in _EXTRACTED_NAMES:
            if getattr(value, "__module__", "") != getattr(runtime, "__name__", ""):
                globals()[name] = value
            else:
                original = _ORIGINAL_EXTRACTED_FUNCTIONS.get(name)
                if original is not None:
                    globals()[name] = original
            continue
        own_wrapper = name.endswith("_for_runtime") and name[: -len("_for_runtime")] in _EXTRACTED_NAMES
        if name == "_bind_runtime" or own_wrapper:
            continue
        globals()[name] = value

def _mcp_token_hash(raw_token: str) -> str:
    token = str(raw_token or "").strip()
    pepper = str(os.getenv("AUTH_TOKEN_PEPPER", "") or "")
    return hashlib.sha256(f"pmda-mcp:{pepper}:{token}".encode("utf-8")).hexdigest()

def _mcp_enabled(settings_snapshot: dict[str, Any] | None = None) -> bool:
    if isinstance(settings_snapshot, dict) and "MCP_ENABLED" in settings_snapshot:
        return bool(_parse_bool(settings_snapshot.get("MCP_ENABLED")))
    try:
        return bool(_parse_bool(_get_config_from_db("MCP_ENABLED", "false")))
    except Exception:
        return False

def _mcp_normalize_scopes(scopes: Any) -> list[str]:
    if isinstance(scopes, str):
        try:
            parsed = json.loads(scopes)
            scopes = parsed
        except Exception:
            scopes = [part.strip() for part in scopes.split(",")]
    if not isinstance(scopes, list):
        scopes = []
    allowed = set(MCP_DEFAULT_SCOPES)
    normalized: list[str] = []
    for scope in scopes:
        value = str(scope or "").strip()
        if value in allowed and value not in normalized:
            normalized.append(value)
    return normalized or list(MCP_DEFAULT_SCOPES)

def _mcp_row_to_token(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    try:
        scopes = _mcp_normalize_scopes(row["scopes_json"])
    except Exception:
        scopes = list(MCP_DEFAULT_SCOPES)
    return {
        "token_id": str(row["token_id"] or ""),
        "name": str(row["name"] or "default"),
        "scopes": scopes,
        "active": bool(row["active"]),
        "created_at": row["created_at"],
        "expires_at": row["expires_at"],
        "last_used_at": row["last_used_at"],
        "revoked_at": row["revoked_at"],
    }

def _mcp_active_token_snapshot() -> dict[str, Any] | None:
    try:
        init_settings_db()
        con = _auth_db_connect()
        row = con.execute(
            """
            SELECT token_id, name, scopes_json, active, created_at, expires_at, last_used_at, revoked_at
            FROM mcp_service_tokens
            WHERE active = 1 AND revoked_at IS NULL
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()
        con.close()
        return _mcp_row_to_token(row)
    except Exception:
        logging.debug("Failed to load MCP token snapshot", exc_info=True)
        return None

def _mcp_latest_audit(limit: int = 20) -> list[dict[str, Any]]:
    try:
        safe_limit = max(1, min(200, int(limit or 20)))
    except Exception:
        safe_limit = 20
    try:
        init_settings_db()
        con = _auth_db_connect()
        rows = con.execute(
            """
            SELECT audit_id, token_id, tool, status, message, args_json, duration_ms, created_at, ip, user_agent
            FROM mcp_audit_log
            ORDER BY created_at DESC, audit_id DESC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()
        con.close()
        return [
            {
                "audit_id": int(row["audit_id"] or 0),
                "token_id": row["token_id"],
                "tool": str(row["tool"] or ""),
                "status": str(row["status"] or ""),
                "message": str(row["message"] or ""),
                "args": _json_loads_safe(row["args_json"], {}),
                "duration_ms": int(row["duration_ms"] or 0),
                "created_at": int(row["created_at"] or 0),
                "ip": str(row["ip"] or ""),
                "user_agent": str(row["user_agent"] or ""),
            }
            for row in rows
        ]
    except Exception:
        logging.debug("Failed to load MCP audit log", exc_info=True)
        return []

def _mcp_status_summary(*, include_audit: bool = False, audit_limit: int = 20) -> dict[str, Any]:
    enabled = _mcp_enabled()
    token = _mcp_active_token_snapshot()
    last_access = token.get("last_used_at") if isinstance(token, dict) else None
    payload = {
        "enabled": bool(enabled),
        "active_token": token,
        "token_active": bool(enabled and token),
        "scopes": list(token.get("scopes") if isinstance(token, dict) else MCP_DEFAULT_SCOPES),
        "last_access_at": last_access,
    }
    if include_audit:
        payload["audit"] = _mcp_latest_audit(audit_limit)
    return payload

def _mcp_generate_token(scopes: Any = None) -> tuple[str, dict[str, Any]]:
    now = int(time.time())
    token_id = f"mcp_{uuid.uuid4().hex}"
    raw_token = MCP_TOKEN_PREFIX + secrets.token_urlsafe(32)
    normalized_scopes = _mcp_normalize_scopes(scopes or list(MCP_DEFAULT_SCOPES))
    init_settings_db()
    con = _auth_db_connect()
    con.execute("UPDATE mcp_service_tokens SET active = 0, revoked_at = ? WHERE active = 1 AND revoked_at IS NULL", (now,))
    con.execute(
        """
        INSERT INTO mcp_service_tokens(token_id, name, token_hash, scopes_json, active, created_at, expires_at, last_used_at, revoked_at)
        VALUES(?, ?, ?, ?, 1, ?, NULL, NULL, NULL)
        """,
        (token_id, "default", _mcp_token_hash(raw_token), json.dumps(normalized_scopes), now),
    )
    con.commit()
    row = con.execute(
        """
        SELECT token_id, name, scopes_json, active, created_at, expires_at, last_used_at, revoked_at
        FROM mcp_service_tokens
        WHERE token_id = ?
        """,
        (token_id,),
    ).fetchone()
    con.close()
    return raw_token, (_mcp_row_to_token(row) or {})

def _mcp_revoke_active_tokens() -> int:
    now = int(time.time())
    init_settings_db()
    con = _auth_db_connect()
    cur = con.execute("UPDATE mcp_service_tokens SET active = 0, revoked_at = ? WHERE active = 1 AND revoked_at IS NULL", (now,))
    count = int(cur.rowcount or 0)
    con.commit()
    con.close()
    return count

def _mcp_authenticate_token(raw_token: str) -> tuple[dict[str, Any] | None, str]:
    if not _mcp_enabled():
        return None, "mcp_disabled"
    token_hash = _mcp_token_hash(raw_token)
    now = int(time.time())
    try:
        init_settings_db()
        con = _auth_db_connect()
        row = con.execute(
            """
            SELECT token_id, name, scopes_json, active, created_at, expires_at, last_used_at, revoked_at
            FROM mcp_service_tokens
            WHERE token_hash = ?
            LIMIT 1
            """,
            (token_hash,),
        ).fetchone()
        if row is None:
            con.close()
            return None, "invalid_mcp_token"
        token = _mcp_row_to_token(row)
        if not token or not bool(token.get("active")) or token.get("revoked_at"):
            con.close()
            return None, "invalid_mcp_token"
        expires_at = token.get("expires_at")
        if expires_at is not None and int(expires_at or 0) <= now:
            con.close()
            return None, "expired_mcp_token"
        con.execute("UPDATE mcp_service_tokens SET last_used_at = ? WHERE token_id = ?", (now, token["token_id"]))
        con.commit()
        con.close()
        token["last_used_at"] = now
        return token, ""
    except Exception:
        logging.exception("MCP token authentication failed")
        return None, "mcp_auth_failed"

def _mcp_request_tool_and_args() -> tuple[str, dict[str, Any]]:
    try:
        data = request.get_json(silent=True) or {}
    except Exception:
        data = {}
    if not isinstance(data, dict):
        data = {}
    tool = str(data.get("tool") or data.get("name") or request.path or "unknown").strip()
    args = data.get("args")
    if args is None:
        args = data.get("arguments")
    if not isinstance(args, dict):
        args = {}
    return tool or "unknown", args

def _mcp_auth_guard(path: str):
    tool, args = _mcp_request_tool_and_args()
    if not _mcp_enabled():
        _mcp_audit(tool, "denied", "mcp_disabled", args, 0)
        return jsonify({"error": "mcp_disabled", "code": "mcp_disabled"}), 403
    raw_token = _auth_get_bearer_token()
    if not raw_token:
        _mcp_audit(tool, "denied", "mcp_token_required", args, 0)
        return jsonify({"error": "mcp_token_required", "code": "mcp_token_required"}), 401
    token, reason = _mcp_authenticate_token(raw_token)
    if not token:
        status = 403 if reason == "mcp_disabled" else 401
        _mcp_audit(tool, "denied", reason or "invalid_mcp_token", args, 0)
        return jsonify({"error": reason or "invalid_mcp_token", "code": reason or "invalid_mcp_token"}), status
    g.mcp_token = token
    g.current_user = None
    return None

def _mcp_scrub_args(args: Any) -> Any:
    if isinstance(args, dict):
        out: dict[str, Any] = {}
        for key, value in args.items():
            key_s = str(key)
            if any(secret in key_s.lower() for secret in ("token", "secret", "password", "api_key")):
                out[key_s] = "[redacted]"
            else:
                out[key_s] = _mcp_scrub_args(value)
        return out
    if isinstance(args, list):
        return [_mcp_scrub_args(item) for item in args[:100]]
    if isinstance(args, (str, int, float, bool)) or args is None:
        return args
    return str(args)

def _mcp_audit(tool: str, status: str, message: str = "", args: Any = None, duration_ms: int = 0) -> None:
    try:
        token = getattr(g, "mcp_token", None)
        token_id = str((token or {}).get("token_id") or "") or None
        scrubbed = _mcp_scrub_args(args if args is not None else {})
        args_json = json.dumps(scrubbed, ensure_ascii=False)
        if len(args_json) > 4000:
            args_json = args_json[:3997] + "..."
        init_settings_db()
        con = _auth_db_connect()
        con.execute(
            """
            INSERT INTO mcp_audit_log(token_id, tool, status, message, args_json, duration_ms, created_at, ip, user_agent)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                token_id,
                str(tool or ""),
                str(status or ""),
                str(message or "")[:1000],
                args_json,
                int(max(0, duration_ms or 0)),
                int(time.time()),
                str(request.remote_addr or "")[:128],
                str(request.headers.get("User-Agent") or "")[:512],
            ),
        )
        con.commit()
        con.close()
    except Exception:
        logging.debug("Failed to write MCP audit log", exc_info=True)

def _mcp_require_scope(token: dict[str, Any], scope: str) -> None:
    scopes = set(_mcp_normalize_scopes(token.get("scopes")))
    if scope not in scopes:
        raise PermissionError(f"scope_required:{scope}")

def _mcp_route_payload(route_func, path: str, body: dict[str, Any] | None = None, method: str = "POST") -> dict[str, Any]:
    with app.test_request_context(path, method=method, json=body or {}):
        g.current_user = {
            "id": 0,
            "username": "mcp_agent",
            "is_admin": True,
            "can_download": True,
            "can_view_statistics": True,
            "allow_ai_calls": False,
            "is_active": True,
        }
        response = app.make_response(route_func())
        payload = response.get_json(silent=True)
        return {
            "status_code": int(response.status_code or 200),
            "payload": payload if payload is not None else response.get_data(as_text=True),
        }

def _mcp_scan_status() -> dict[str, Any]:
    with lock:
        active = dict(state.get("scan_active_artists") or {})
        return {
            "scan_id": _int_or_none(state.get("scan_id")),
            "scanning": bool(state.get("scanning") or state.get("scan_starting")),
            "paused": bool(scan_is_paused.is_set()),
            "stopping": bool(scan_should_stop.is_set()),
            "scan_type": str(state.get("scan_type") or "full"),
            "artists_processed": int(state.get("scan_artists_processed") or 0),
            "artists_total": int(state.get("scan_artists_total") or 0),
            "albums_processed": int(state.get("scan_processed_albums_count") or 0),
            "albums_total": int(state.get("scan_total_albums") or state.get("scan_detected_albums_total") or 0),
            "stage_done": int(state.get("scan_step_progress") or 0),
            "stage_total": int(state.get("scan_step_total") or 0),
            "active_artists": {
                str(name): {
                    "albums_processed": int((info or {}).get("albums_processed") or 0),
                    "total_albums": int((info or {}).get("total_albums") or 0),
                    "current_album": (info or {}).get("current_album"),
                }
                for name, info in active.items()
                if not str(name).startswith("_") and isinstance(info, dict)
            },
        }

def _mcp_scan_history(limit: int = 20) -> dict[str, Any]:
    safe_limit = max(1, min(100, int(limit or 20)))
    con = sqlite3.connect(str(STATE_DB_FILE), timeout=5)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        """
        SELECT scan_id, start_time, end_time, duration_seconds, scan_type, albums_scanned,
               duplicates_found, artists_processed, artists_total, status,
               duplicate_groups_count, total_duplicates_count, broken_albums_count,
               space_saved_mb, albums_moved
        FROM scan_history
        ORDER BY start_time DESC
        LIMIT ?
        """,
        (safe_limit,),
    ).fetchall()
    con.close()
    return {"items": [dict(row) for row in rows], "limit": safe_limit}

def _mcp_logs_tail(lines: int = 200, scan_mode: bool = True) -> dict[str, Any]:
    safe_lines = max(20, min(1000, int(lines or 200)))
    entries = _recent_log_tail_entries(safe_lines, scan_mode=bool(scan_mode))
    return {"entries": entries, "lines": safe_lines, "path": str(LOG_FILE)}

def _mcp_duplicate_groups(limit: int = 50) -> dict[str, Any]:
    safe_limit = max(1, min(200, int(limit or 50)))
    registry = _global_duplicate_review_registry(include_live=True)
    flat: list[dict[str, Any]] = []
    for artist, groups in (registry.get("groups") or {}).items():
        for group in groups or []:
            if not isinstance(group, dict):
                continue
            best = dict(group.get("best") or {})
            losers = [dict(loser or {}) for loser in (group.get("losers") or [])]
            loser_size_mb = sum(_parse_int_loose(loser.get("size_mb") or loser.get("size"), 0) for loser in losers)
            flat.append(
                {
                    "artist": str(artist or group.get("artist") or "").strip(),
                    "album_id": _parse_int_loose(group.get("album_id") or best.get("album_id"), 0),
                    "title_raw": str(best.get("title_raw") or group.get("title_raw") or "").strip(),
                    "folder": str(best.get("folder") or ""),
                    "rationale": str(best.get("rationale") or ""),
                    "ai_used": bool(best.get("used_ai")),
                    "ai_provider": str(best.get("ai_provider") or ""),
                    "ai_model": str(best.get("ai_model") or ""),
                    "size_mb": _parse_int_loose(best.get("size_mb"), 0),
                    "track_count": _parse_int_loose(best.get("track_count"), 0),
                    "evidence": list(best.get("dupe_evidence") or []),
                    "dupe_signal": str(group.get("dupe_signal") or ""),
                    "no_move": bool(group.get("no_move")),
                    "manual_review": bool(group.get("manual_review")),
                    "same_folder": bool(group.get("same_folder")),
                    "loser_count": len(losers),
                    "loser_size_mb": int(loser_size_mb),
                    "losers": [
                        {
                            "loser_album_id": _parse_int_loose(loser.get("loser_album_id") or loser.get("album_id"), 0),
                            "folder": str(loser.get("folder") or ""),
                            "fmt_text": str(loser.get("fmt_text") or loser.get("fmt") or ""),
                            "br": _parse_int_loose(loser.get("br"), 0),
                            "sr": _parse_int_loose(loser.get("sr"), 0),
                            "bd": _parse_int_loose(loser.get("bd"), 0),
                            "size_mb": _parse_int_loose(loser.get("size_mb") or loser.get("size"), 0),
                        }
                        for loser in losers[:25]
                    ],
                }
            )
    flat.sort(key=lambda item: (-int(item.get("loser_count") or 0), str(item.get("artist") or "").lower()))
    return {"items": flat[:safe_limit], "limit": safe_limit, "sources": registry.get("sources") or {}}

def _mcp_incomplete_albums(limit: int = 50) -> dict[str, Any]:
    safe_limit = max(1, min(200, int(limit or 50)))
    con = sqlite3.connect(str(STATE_DB_FILE), timeout=5)
    con.row_factory = sqlite3.Row
    items: list[dict[str, Any]] = []
    seen: set[tuple[str, int]] = set()
    sources = {"broken_albums": 0, "incomplete_album_diagnostics": 0}
    try:
        cur = con.cursor()
        if _sqlite_table_exists(cur, "broken_albums"):
            rows = cur.execute(
                """
                SELECT artist, album_id, album_title, folder_path, expected_track_count, actual_track_count,
                       missing_indices, review_status, strict_match_provider, strict_reject_reason,
                       reason_summary, classification, classification_confidence, detected_at
                FROM broken_albums
                ORDER BY detected_at DESC
                LIMIT ?
                """,
                (safe_limit,),
            ).fetchall()
            for row in rows:
                item = dict(row)
                key = (str(item.get("artist") or "").strip(), _parse_int_loose(item.get("album_id"), 0))
                if not key[0] or key[1] <= 0:
                    continue
                item["source"] = "broken_albums"
                items.append(item)
                seen.add(key)
            sources["broken_albums"] = len(items)
        remaining = max(0, safe_limit - len(items))
        if remaining and _sqlite_table_exists(cur, "incomplete_album_diagnostics"):
            rows = cur.execute(
                """
                SELECT d.artist, d.album_id, d.title_raw AS album_title, d.folder AS folder_path,
                       d.expected_track_count, d.actual_track_count, d.missing_on_disk AS missing_indices,
                       '' AS review_status, '' AS strict_match_provider, '' AS strict_reject_reason,
                       d.classification AS reason_summary, d.classification, 0.0 AS classification_confidence,
                       d.detected_at
                FROM incomplete_album_diagnostics d
                JOIN (
                    SELECT artist, album_id, MAX(rowid) AS rowid
                    FROM incomplete_album_diagnostics
                    GROUP BY artist, album_id
                ) latest ON latest.rowid = d.rowid
                ORDER BY d.detected_at DESC
                LIMIT ?
                """,
                (safe_limit * 2,),
            ).fetchall()
            for row in rows:
                item = dict(row)
                key = (str(item.get("artist") or "").strip(), _parse_int_loose(item.get("album_id"), 0))
                if not key[0] or key[1] <= 0 or key in seen:
                    continue
                item["source"] = "incomplete_album_diagnostics"
                items.append(item)
                seen.add(key)
                sources["incomplete_album_diagnostics"] += 1
                if len(items) >= safe_limit:
                    break
    finally:
        con.close()
    return {"items": items[:safe_limit], "limit": safe_limit, "sources": sources}

def _mcp_safe_limit(value: Any, default: int = 100, maximum: int = 1000) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = int(default)
    return max(1, min(int(maximum), parsed))

def _mcp_percent(done: Any, total: Any) -> float:
    try:
        done_f = float(done or 0)
        total_f = float(total or 0)
    except Exception:
        return 0.0
    if total_f <= 0:
        return 0.0
    return round((done_f / total_f) * 100.0, 3)

def _mcp_scan_id_from_args(args: dict[str, Any] | None = None, con: sqlite3.Connection | None = None) -> int | None:
    args = args if isinstance(args, dict) else {}
    explicit = _int_or_none(args.get("scan_id"))
    if explicit:
        return explicit
    with lock:
        current = _int_or_none(state.get("scan_id"))
    if current:
        return current
    close_con = False
    if con is None:
        con = sqlite3.connect(str(STATE_DB_FILE), timeout=5)
        close_con = True
    try:
        row = con.execute(
            """
            SELECT scan_id
            FROM scan_history
            ORDER BY COALESCE(end_time, start_time) DESC, scan_id DESC
            LIMIT 1
            """
        ).fetchone()
        if row:
            return int(row[0] or 0) or None
    except Exception:
        logging.debug("MCP could not resolve latest scan_id", exc_info=True)
    finally:
        if close_con:
            try:
                con.close()
            except Exception:
                pass
    return None

def _mcp_scan_history_row(cur: sqlite3.Cursor, scan_id: int | None) -> dict[str, Any] | None:
    if not scan_id or not _sqlite_table_exists(cur, "scan_history"):
        return None
    try:
        cur.execute("SELECT * FROM scan_history WHERE scan_id = ?", (int(scan_id),))
        row = cur.fetchone()
        return dict(row) if row is not None else None
    except Exception:
        logging.debug("MCP scan history row lookup failed", exc_info=True)
        return None

def _mcp_scan_trace_where(args: dict[str, Any], scan_id: int) -> tuple[str, list[Any]]:
    where = ["scan_id = ?"]
    params: list[Any] = [int(scan_id)]
    q = str(args.get("q") or "").strip().lower()
    if q:
        needle = f"%{q}%"
        where.append(
            "("
            "LOWER(COALESCE(artist, '')) LIKE ? OR "
            "LOWER(COALESCE(album_title, '')) LIKE ? OR "
            "LOWER(COALESCE(folder_name, '')) LIKE ? OR "
            "LOWER(COALESCE(folder, '')) LIKE ?"
            ")"
        )
        params.extend([needle, needle, needle, needle])
    provider = str(args.get("provider") or "").strip().lower()
    provider = _normalize_identity_provider(provider) or provider
    if provider:
        if provider in {"musicbrainz", "discogs", "lastfm", "bandcamp"}:
            where.append(f"(COALESCE(has_{provider}, 0) = 1 OR LOWER(COALESCE(metadata_source, '')) = ? OR LOWER(COALESCE(strict_match_provider, '')) = ?)")
            params.extend([provider, provider])
        elif provider == "none":
            where.append(
                "COALESCE(has_musicbrainz, 0) = 0 AND COALESCE(has_discogs, 0) = 0 "
                "AND COALESCE(has_lastfm, 0) = 0 AND COALESCE(has_bandcamp, 0) = 0 "
                "AND TRIM(COALESCE(metadata_source, '')) = ''"
            )
        else:
            where.append("(LOWER(COALESCE(metadata_source, '')) = ? OR LOWER(COALESCE(strict_match_provider, '')) = ?)")
            params.extend([provider, provider])
    outcome = str(args.get("outcome") or "").strip().lower()
    if outcome:
        if outcome in {"matched", "strict_matched"}:
            where.append("COALESCE(strict_match_verified, 0) = 1")
        elif outcome in {"provider_only", "provider_matched"}:
            where.append("COALESCE(strict_match_verified, 0) = 0 AND TRIM(COALESCE(metadata_source, '')) <> ''")
        elif outcome in {"unmatched", "miss"}:
            where.append(
                "COALESCE(strict_match_verified, 0) = 0 "
                "AND TRIM(COALESCE(metadata_source, '')) = '' "
                "AND COALESCE(has_musicbrainz, 0) = 0 "
                "AND COALESCE(has_discogs, 0) = 0 "
                "AND COALESCE(has_lastfm, 0) = 0 "
                "AND COALESCE(has_bandcamp, 0) = 0"
            )
        elif outcome in {"incomplete", "broken"}:
            where.append("COALESCE(is_broken, 0) = 1")
        elif outcome in {"duplicate_winner", "duplicate_loser", "duplicate_candidate"}:
            where.append("LOWER(COALESCE(dupe_role, 'none')) = ?")
            params.append(outcome.replace("duplicate_", ""))
        elif outcome:
            where.append("LOWER(COALESCE(pipeline_status, 'active')) = ?")
            params.append(outcome)
    return " AND ".join(where), params

def _mcp_scan_trace_summary(cur: sqlite3.Cursor, scan_id: int | None, args: dict[str, Any] | None = None) -> dict[str, Any]:
    if not scan_id or not _sqlite_table_exists(cur, "scan_pipeline_trace"):
        return {"available": False, "scan_id": scan_id, "total": 0}
    args = args if isinstance(args, dict) else {}
    where_sql, params = _mcp_scan_trace_where(args, int(scan_id))
    cur.execute(
        f"""
        SELECT
            COUNT(*) AS total,
            COALESCE(SUM(CASE WHEN COALESCE(strict_match_verified, 0) = 1 THEN 1 ELSE 0 END), 0) AS strict_matches,
            COALESCE(SUM(CASE WHEN TRIM(COALESCE(metadata_source, '')) <> '' THEN 1 ELSE 0 END), 0) AS metadata_source_matches,
            COALESCE(SUM(CASE WHEN COALESCE(strict_match_verified, 0) = 0 AND TRIM(COALESCE(metadata_source, '')) <> '' THEN 1 ELSE 0 END), 0) AS provider_only_matches,
            COALESCE(SUM(CASE WHEN COALESCE(is_broken, 0) = 1 THEN 1 ELSE 0 END), 0) AS incomplete_or_broken,
            COALESCE(SUM(CASE WHEN LOWER(COALESCE(dupe_role, 'none')) = 'winner' THEN 1 ELSE 0 END), 0) AS duplicate_winners,
            COALESCE(SUM(CASE WHEN LOWER(COALESCE(dupe_role, 'none')) = 'loser' THEN 1 ELSE 0 END), 0) AS duplicate_losers,
            COALESCE(SUM(CASE WHEN COALESCE(ai_used, 0) = 1 THEN 1 ELSE 0 END), 0) AS ai_touched,
            COALESCE(SUM(CASE WHEN COALESCE(strict_match_verified, 0) = 1 AND LOWER(COALESCE(strict_match_provider, '')) = 'musicbrainz' THEN 1 ELSE 0 END), 0) AS tier_strict_mb,
            COALESCE(SUM(CASE WHEN COALESCE(strict_match_verified, 0) = 1 AND LOWER(COALESCE(strict_match_provider, '')) <> 'musicbrainz' THEN 1 ELSE 0 END), 0) AS tier_strong_provider,
            COALESCE(SUM(CASE WHEN COALESCE(strict_match_verified, 0) = 0 AND COALESCE(ai_used, 0) = 1 THEN 1 ELSE 0 END), 0) AS tier_ai_review,
            COALESCE(SUM(CASE WHEN COALESCE(strict_match_verified, 0) = 0 AND COALESCE(ai_used, 0) = 0 AND (
                TRIM(COALESCE(metadata_source, '')) <> ''
                OR COALESCE(has_musicbrainz, 0) = 1
                OR COALESCE(has_discogs, 0) = 1
                OR COALESCE(has_lastfm, 0) = 1
                OR COALESCE(has_bandcamp, 0) = 1
            ) THEN 1 ELSE 0 END), 0) AS tier_soft_provider,
            COALESCE(SUM(CASE WHEN COALESCE(strict_match_verified, 0) = 0 AND COALESCE(ai_used, 0) = 0 AND TRIM(COALESCE(metadata_source, '')) = '' AND COALESCE(has_musicbrainz, 0) = 0 AND COALESCE(has_discogs, 0) = 0 AND COALESCE(has_lastfm, 0) = 0 AND COALESCE(has_bandcamp, 0) = 0 THEN 1 ELSE 0 END), 0) AS tier_unresolved,
            COALESCE(SUM(CASE WHEN COALESCE(has_musicbrainz, 0) = 1 OR TRIM(COALESCE(musicbrainz_release_id, '')) <> '' THEN 1 ELSE 0 END), 0) AS musicbrainz_identity,
            COALESCE(SUM(CASE WHEN COALESCE(has_discogs, 0) = 1 OR TRIM(COALESCE(discogs_release_id, '')) <> '' THEN 1 ELSE 0 END), 0) AS discogs_identity,
            COALESCE(SUM(CASE WHEN COALESCE(has_lastfm, 0) = 1 OR TRIM(COALESCE(lastfm_album_mbid, '')) <> '' THEN 1 ELSE 0 END), 0) AS lastfm_identity,
            COALESCE(SUM(CASE WHEN COALESCE(has_bandcamp, 0) = 1 OR TRIM(COALESCE(bandcamp_album_url, '')) <> '' THEN 1 ELSE 0 END), 0) AS bandcamp_identity,
            COALESCE(SUM(CASE WHEN COALESCE(has_cover, 0) = 1 THEN 1 ELSE 0 END), 0) AS with_cover,
            COALESCE(SUM(CASE WHEN COALESCE(has_cover, 0) = 0 THEN 1 ELSE 0 END), 0) AS missing_cover,
            COALESCE(SUM(CASE WHEN TRIM(COALESCE(missing_required_tags, '')) NOT IN ('', '[]', '{{}}') THEN 1 ELSE 0 END), 0) AS missing_tags_rows,
            COALESCE(SUM(CASE WHEN COALESCE(manual_review, 0) = 1 THEN 1 ELSE 0 END), 0) AS manual_review_rows,
            COALESCE(SUM(CASE WHEN COALESCE(dupe_needs_ai, 0) = 1 THEN 1 ELSE 0 END), 0) AS dupe_needs_ai_rows,
            COALESCE(SUM(CASE WHEN COALESCE(no_move, 0) = 1 THEN 1 ELSE 0 END), 0) AS no_move_rows,
            COALESCE(SUM(CASE WHEN COALESCE(same_folder, 0) = 1 THEN 1 ELSE 0 END), 0) AS same_folder_rows,
            COALESCE(SUM(CASE WHEN expected_track_count IS NOT NULL AND actual_track_count IS NOT NULL AND actual_track_count < expected_track_count THEN 1 ELSE 0 END), 0) AS track_count_deficit_rows,
            COUNT(DISTINCT CASE WHEN COALESCE(is_broken, 0) = 1 THEN artist ELSE NULL END) AS artists_with_incompletes,
            COUNT(DISTINCT CASE WHEN LOWER(COALESCE(dupe_role, 'none')) IN ('winner', 'loser', 'candidate') THEN artist ELSE NULL END) AS artists_with_duplicates,
            COUNT(DISTINCT CASE WHEN COALESCE(manual_review, 0) = 1 OR COALESCE(dupe_needs_ai, 0) = 1 OR COALESCE(is_broken, 0) = 1 OR LOWER(COALESCE(dupe_role, 'none')) IN ('winner', 'loser', 'candidate') THEN artist ELSE NULL END) AS artists_with_review_or_issue
        FROM scan_pipeline_trace
        WHERE {where_sql}
        """,
        tuple(params),
    )
    row = cur.fetchone() or {}
    total = int(row["total"] or 0)
    strict_matches = int(row["strict_matches"] or 0)
    metadata_matches = int(row["metadata_source_matches"] or 0)
    provider_only = int(row["provider_only_matches"] or 0)
    matched_rows = int(max(strict_matches, metadata_matches))
    unmatched_rows = max(0, total - matched_rows)
    confidence_tiers = {
        "strict_mb": int(row["tier_strict_mb"] or 0),
        "strong_provider": int(row["tier_strong_provider"] or 0),
        "soft_provider": int(row["tier_soft_provider"] or 0),
        "ai_review": int(row["tier_ai_review"] or 0),
        "unresolved": int(row["tier_unresolved"] or 0),
    }
    cur.execute(
        f"""
        SELECT LOWER(COALESCE(metadata_source, '')) AS provider, COUNT(*) AS count
        FROM scan_pipeline_trace
        WHERE {where_sql}
        GROUP BY LOWER(COALESCE(metadata_source, ''))
        ORDER BY count DESC, provider ASC
        """,
        tuple(params),
    )
    metadata_provider_counts = {str(r["provider"] or "none") or "none": int(r["count"] or 0) for r in cur.fetchall()}
    cur.execute(
        f"""
        SELECT LOWER(COALESCE(strict_match_provider, '')) AS provider, COUNT(*) AS count
        FROM scan_pipeline_trace
        WHERE {where_sql} AND COALESCE(strict_match_verified, 0) = 1
        GROUP BY LOWER(COALESCE(strict_match_provider, ''))
        ORDER BY count DESC, provider ASC
        """,
        tuple(params),
    )
    strict_provider_counts = {str(r["provider"] or "unknown") or "unknown": int(r["count"] or 0) for r in cur.fetchall()}
    cur.execute(
        f"""
        SELECT LOWER(COALESCE(pipeline_status, 'active')) AS status, COUNT(*) AS count
        FROM scan_pipeline_trace
        WHERE {where_sql}
        GROUP BY LOWER(COALESCE(pipeline_status, 'active'))
        ORDER BY count DESC, status ASC
        """,
        tuple(params),
    )
    status_counts = {str(r["status"] or "active"): int(r["count"] or 0) for r in cur.fetchall()}
    cur.execute(
        f"""
        SELECT LOWER(COALESCE(move_reason, '')) AS move_reason,
               LOWER(COALESCE(move_status, 'none')) AS move_status,
               COUNT(*) AS count
        FROM scan_pipeline_trace
        WHERE {where_sql} AND TRIM(COALESCE(move_reason, '')) <> ''
        GROUP BY LOWER(COALESCE(move_reason, '')), LOWER(COALESCE(move_status, 'none'))
        ORDER BY count DESC, move_reason ASC, move_status ASC
        """,
        tuple(params),
    )
    move_counts = [
        {
            "move_reason": str(r["move_reason"] or ""),
            "move_status": str(r["move_status"] or "none"),
            "count": int(r["count"] or 0),
        }
        for r in cur.fetchall()
    ]
    return {
        "available": True,
        "scan_id": int(scan_id),
        "total": total,
        "strict_matches": strict_matches,
        "metadata_source_matches": metadata_matches,
        "provider_only_matches": provider_only,
        "matched_rows": matched_rows,
        "unmatched_rows": unmatched_rows,
        "matched_percent": _mcp_percent(matched_rows, total),
        "unmatched_percent": _mcp_percent(unmatched_rows, total),
        "confidence_tiers": confidence_tiers,
        "confidence_tier_percent": {
            key: _mcp_percent(value, total)
            for key, value in confidence_tiers.items()
        },
        "incomplete_or_broken": int(row["incomplete_or_broken"] or 0),
        "duplicate_winners": int(row["duplicate_winners"] or 0),
        "duplicate_losers": int(row["duplicate_losers"] or 0),
        "ai_touched": int(row["ai_touched"] or 0),
        "identity_counts": {
            "musicbrainz": int(row["musicbrainz_identity"] or 0),
            "discogs": int(row["discogs_identity"] or 0),
            "lastfm": int(row["lastfm_identity"] or 0),
            "bandcamp": int(row["bandcamp_identity"] or 0),
        },
        "artwork": {
            "album_rows_with_cover": int(row["with_cover"] or 0),
            "album_rows_missing_cover": int(row["missing_cover"] or 0),
            "cover_percent": _mcp_percent(row["with_cover"], total),
        },
        "quality": {
            "missing_tags_rows": int(row["missing_tags_rows"] or 0),
            "track_count_deficit_rows": int(row["track_count_deficit_rows"] or 0),
        },
        "reviews": {
            "manual_review_rows": int(row["manual_review_rows"] or 0),
            "dupe_needs_ai_rows": int(row["dupe_needs_ai_rows"] or 0),
            "no_move_rows": int(row["no_move_rows"] or 0),
            "same_folder_rows": int(row["same_folder_rows"] or 0),
            "artists_with_incompletes": int(row["artists_with_incompletes"] or 0),
            "artists_with_duplicates": int(row["artists_with_duplicates"] or 0),
            "artists_with_review_or_issue": int(row["artists_with_review_or_issue"] or 0),
        },
        "metadata_provider_counts": metadata_provider_counts,
        "strict_provider_counts": strict_provider_counts,
        "status_counts": status_counts,
        "move_counts": move_counts,
    }

def _mcp_scan_pipeline_trace(args: dict[str, Any]) -> dict[str, Any]:
    args = args if isinstance(args, dict) else {}
    page = max(1, _parse_int_loose(args.get("page"), 1))
    page_size = _mcp_safe_limit(args.get("page_size") or args.get("limit"), default=100, maximum=1000)
    con = _state_connect_readonly(timeout=15)
    try:
        cur = con.cursor()
        scan_id = _mcp_scan_id_from_args(args, con)
        if not scan_id:
            return {"available": False, "reason": "no_scan_id", "items": [], "summary": {"total": 0}}
        if not _sqlite_table_exists(cur, "scan_pipeline_trace"):
            return {"available": False, "reason": "scan_pipeline_trace_missing", "scan_id": scan_id, "items": [], "summary": {"total": 0}}
        where_sql, params = _mcp_scan_trace_where(args, int(scan_id))
        offset = (page - 1) * page_size
        cur.execute(f"SELECT COUNT(*) AS total FROM scan_pipeline_trace WHERE {where_sql}", tuple(params))
        total = int((cur.fetchone() or {"total": 0})["total"] or 0)
        cur.execute(
            f"""
            SELECT *
            FROM scan_pipeline_trace
            WHERE {where_sql}
            ORDER BY updated_at DESC, artist COLLATE NOCASE ASC, album_title COLLATE NOCASE ASC
            LIMIT ? OFFSET ?
            """,
            tuple(params + [page_size, offset]),
        )
        rows = [_scan_pipeline_trace_row_to_api(row) for row in cur.fetchall()]
        summary = _mcp_scan_trace_summary(cur, int(scan_id), args)
        return {
            "available": True,
            "scan_id": int(scan_id),
            "page": page,
            "page_size": page_size,
            "total": total,
            "items": rows,
            "summary": summary,
        }
    finally:
        con.close()

def _mcp_scan_moves(args: dict[str, Any]) -> dict[str, Any]:
    args = args if isinstance(args, dict) else {}
    limit = _mcp_safe_limit(args.get("limit"), default=100, maximum=1000)
    con = sqlite3.connect(str(STATE_DB_FILE), timeout=15)
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()
        scan_id = _mcp_scan_id_from_args(args, con)
        if not scan_id:
            return {"available": False, "reason": "no_scan_id", "items": [], "summary": {}}
        if not _sqlite_table_exists(cur, "scan_moves"):
            return {"available": False, "reason": "scan_moves_missing", "scan_id": scan_id, "items": [], "summary": {}}
        cur.execute("PRAGMA table_info(scan_moves)")
        cols = {str(r[1]) for r in cur.fetchall() if len(r) > 1}
        reason_expr = "LOWER(COALESCE(move_reason, 'dedupe'))" if "move_reason" in cols else "'dedupe'"
        where = ["scan_id = ?"]
        params: list[Any] = [int(scan_id)]
        reason = str(args.get("reason") or "").strip().lower()
        if reason:
            where.append(f"{reason_expr} = ?")
            params.append(reason)
        status = str(args.get("status") or "all").strip().lower()
        if status == "active":
            where.append("COALESCE(restored, 0) = 0")
        elif status == "restored":
            where.append("COALESCE(restored, 0) = 1")
        where_sql = " AND ".join(where)
        cur.execute(
            f"""
            SELECT
                COUNT(*) AS total_moved,
                COALESCE(SUM(CASE WHEN COALESCE(restored, 0) = 0 THEN 1 ELSE 0 END), 0) AS pending,
                COALESCE(SUM(CASE WHEN COALESCE(restored, 0) = 1 THEN 1 ELSE 0 END), 0) AS restored,
                COALESCE(SUM(COALESCE(size_mb, 0)), 0) AS size_mb,
                MIN(moved_at) AS first_moved_at,
                MAX(moved_at) AS last_moved_at
            FROM scan_moves
            WHERE {where_sql}
            """,
            tuple(params),
        )
        row = cur.fetchone() or {}
        cur.execute(
            f"""
            SELECT {reason_expr} AS move_reason,
                   COUNT(*) AS total,
                   COALESCE(SUM(CASE WHEN COALESCE(restored, 0) = 0 THEN 1 ELSE 0 END), 0) AS pending,
                   COALESCE(SUM(CASE WHEN COALESCE(restored, 0) = 1 THEN 1 ELSE 0 END), 0) AS restored,
                   COALESCE(SUM(COALESCE(size_mb, 0)), 0) AS size_mb
            FROM scan_moves
            WHERE {where_sql}
            GROUP BY {reason_expr}
            ORDER BY move_reason ASC
            """,
            tuple(params),
        )
        by_reason = {
            str(r["move_reason"] or "dedupe"): {
                "total_moved": int(r["total"] or 0),
                "pending": int(r["pending"] or 0),
                "restored": int(r["restored"] or 0),
                "size_mb": int(r["size_mb"] or 0),
            }
            for r in cur.fetchall()
        }
        optional_cols = [
            "move_id",
            "scan_id",
            "artist",
            "album_id",
            "album_title",
            "fmt_text",
            "original_path",
            "moved_to_path",
            "source_path",
            "destination_path",
            "winner_album_id",
            "winner_title",
            "winner_path",
            "size_mb",
            "move_reason",
            "restored",
            "moved_at",
            "decision_source",
            "decision_provider",
            "decision_reason",
            "decision_confidence",
            "materialization_strategy",
            "arbitration_result",
            "details_json",
        ]
        select_cols = [col for col in optional_cols if col in cols]
        cur.execute(
            f"""
            SELECT {", ".join(select_cols)}
            FROM scan_moves
            WHERE {where_sql}
            ORDER BY moved_at DESC, move_id DESC
            LIMIT ?
            """,
            tuple(params + [limit]),
        )
        items = []
        for move in cur.fetchall():
            item = dict(move)
            if item.get("details_json"):
                item["details"] = _json_loads_safe(item.get("details_json"), {})
                item.pop("details_json", None)
            items.append(item)
        return {
            "available": True,
            "scan_id": int(scan_id),
            "limit": limit,
            "summary": {
                "total_moved": int(row["total_moved"] or 0),
                "pending": int(row["pending"] or 0),
                "restored": int(row["restored"] or 0),
                "size_mb": int(row["size_mb"] or 0),
                "size_gb": round((int(row["size_mb"] or 0) / 1024.0), 3),
                "first_moved_at": float(row["first_moved_at"]) if row["first_moved_at"] is not None else None,
                "last_moved_at": float(row["last_moved_at"]) if row["last_moved_at"] is not None else None,
                "by_reason": by_reason,
            },
            "items": items,
        }
    finally:
        con.close()

def _mcp_scan_resume_state(*args: Any, **kwargs: Any) -> Any:
    return _scan_resume_runtime._mcp_scan_resume_state_for_runtime(sys.modules[__name__], *args, **kwargs)

def _mcp_cache_stats() -> dict[str, Any]:
    now = int(time.time())
    out: dict[str, Any] = {
        "cache_db": {"path": str(CACHE_DB_FILE), "available": CACHE_DB_FILE.exists()},
        "state_db": {"path": str(STATE_DB_FILE), "available": STATE_DB_FILE.exists()},
        "provider_gateway_live": _provider_gateway_stats_snapshot(),
    }
    if CACHE_DB_FILE.exists():
        con = sqlite3.connect(str(CACHE_DB_FILE), timeout=15)
        con.row_factory = sqlite3.Row
        try:
            cur = con.cursor()
            if _sqlite_table_exists(cur, "audio_cache"):
                cur.execute("SELECT COUNT(*) AS rows FROM audio_cache")
                out["audio_cache"] = {"rows": int((cur.fetchone() or {"rows": 0})["rows"] or 0)}
            if _sqlite_table_exists(cur, "musicbrainz_cache"):
                cur.execute("SELECT COUNT(*) AS rows, MIN(created_at) AS oldest_created_at, MAX(created_at) AS newest_created_at FROM musicbrainz_cache")
                out["musicbrainz_cache"] = dict(cur.fetchone() or {})
            if _sqlite_table_exists(cur, "musicbrainz_album_lookup"):
                cur.execute(
                    """
                    SELECT COUNT(*) AS rows,
                           COALESCE(SUM(CASE WHEN TRIM(COALESCE(mbid, '')) <> '' THEN 1 ELSE 0 END), 0) AS found,
                           COALESCE(SUM(CASE WHEN TRIM(COALESCE(mbid, '')) = '' THEN 1 ELSE 0 END), 0) AS not_found,
                           MIN(created_at) AS oldest_created_at,
                           MAX(created_at) AS newest_created_at
                    FROM musicbrainz_album_lookup
                    """
                )
                out["musicbrainz_album_lookup"] = dict(cur.fetchone() or {})
            if _sqlite_table_exists(cur, "provider_album_lookup"):
                cur.execute(
                    """
                    SELECT COUNT(*) AS rows,
                           COALESCE(SUM(CASE WHEN status = 'found' THEN 1 ELSE 0 END), 0) AS found,
                           COALESCE(SUM(CASE WHEN status = 'not_found' THEN 1 ELSE 0 END), 0) AS not_found,
                           COALESCE(SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END), 0) AS error,
                           COALESCE(SUM(CASE WHEN expires_at > ? THEN 1 ELSE 0 END), 0) AS valid,
                           COALESCE(SUM(CASE WHEN expires_at <= ? THEN 1 ELSE 0 END), 0) AS expired
                    FROM provider_album_lookup
                    """,
                    (now, now),
                )
                out["provider_album_lookup"] = dict(cur.fetchone() or {})
        finally:
            con.close()
    if STATE_DB_FILE.exists():
        con = sqlite3.connect(str(STATE_DB_FILE), timeout=15)
        con.row_factory = sqlite3.Row
        try:
            cur = con.cursor()
            for table in ("files_album_scan_cache", "files_dir_scan_cache", "files_pending_changes", "scan_resume_runs", "scan_resume_artists", "scan_pipeline_trace"):
                if _sqlite_table_exists(cur, table):
                    cur.execute(f"SELECT COUNT(*) AS rows FROM {table}")
                    out.setdefault("state_tables", {})[table] = int((cur.fetchone() or {"rows": 0})["rows"] or 0)
        finally:
            con.close()
    return out

def _mcp_provider_cache_stats(args: dict[str, Any]) -> dict[str, Any]:
    args = args if isinstance(args, dict) else {}
    limit = _mcp_safe_limit(args.get("limit"), default=50, maximum=500)
    now = int(time.time())
    if not CACHE_DB_FILE.exists():
        return {"available": False, "reason": "cache_db_missing", "items": []}
    con = sqlite3.connect(str(CACHE_DB_FILE), timeout=15)
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()
        if not _sqlite_table_exists(cur, "provider_album_lookup"):
            return {"available": False, "reason": "provider_album_lookup_missing", "items": []}
        cur.execute(
            """
            SELECT provider,
                   status,
                   COUNT(*) AS rows,
                   COALESCE(SUM(CASE WHEN expires_at > ? THEN 1 ELSE 0 END), 0) AS valid,
                   COALESCE(SUM(CASE WHEN expires_at <= ? THEN 1 ELSE 0 END), 0) AS expired,
                   MIN(created_at) AS oldest_created_at,
                   MAX(created_at) AS newest_created_at
            FROM provider_album_lookup
            GROUP BY provider, status
            ORDER BY provider ASC, status ASC
            """,
            (now, now),
        )
        summary = [dict(row) for row in cur.fetchall()]
        provider = str(args.get("provider") or "").strip().lower()
        where = []
        params: list[Any] = []
        if provider:
            where.append("LOWER(provider) = ?")
            params.append(provider)
        status = str(args.get("status") or "").strip().lower()
        if status:
            where.append("LOWER(status) = ?")
            params.append(status)
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        cur.execute(
            f"""
            SELECT provider, artist_norm, album_norm, status, created_at, expires_at
            FROM provider_album_lookup
            {where_sql}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            tuple(params + [limit]),
        )
        return {"available": True, "summary": summary, "items": [dict(row) for row in cur.fetchall()], "limit": limit}
    finally:
        con.close()

def _mcp_musicbrainz_cache_stats(args: dict[str, Any]) -> dict[str, Any]:
    args = args if isinstance(args, dict) else {}
    limit = _mcp_safe_limit(args.get("limit"), default=50, maximum=500)
    if not CACHE_DB_FILE.exists():
        return {"available": False, "reason": "cache_db_missing", "items": []}
    con = sqlite3.connect(str(CACHE_DB_FILE), timeout=15)
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()
        summary: dict[str, Any] = {}
        if _sqlite_table_exists(cur, "musicbrainz_album_lookup"):
            cur.execute(
                """
                SELECT COUNT(*) AS rows,
                       COALESCE(SUM(CASE WHEN TRIM(COALESCE(mbid, '')) <> '' THEN 1 ELSE 0 END), 0) AS found,
                       COALESCE(SUM(CASE WHEN TRIM(COALESCE(mbid, '')) = '' THEN 1 ELSE 0 END), 0) AS not_found,
                       MIN(created_at) AS oldest_created_at,
                       MAX(created_at) AS newest_created_at
                FROM musicbrainz_album_lookup
                """
            )
            summary["album_lookup"] = dict(cur.fetchone() or {})
        if _sqlite_table_exists(cur, "musicbrainz_cache"):
            cur.execute("SELECT COUNT(*) AS rows, MIN(created_at) AS oldest_created_at, MAX(created_at) AS newest_created_at FROM musicbrainz_cache")
            summary["release_group_cache"] = dict(cur.fetchone() or {})
        if not _sqlite_table_exists(cur, "musicbrainz_album_lookup"):
            return {"available": False, "reason": "musicbrainz_album_lookup_missing", "summary": summary, "items": []}
        status = str(args.get("status") or "").strip().lower()
        if status == "found":
            where_sql = "WHERE TRIM(COALESCE(mbid, '')) <> ''"
        elif status in {"not_found", "miss"}:
            where_sql = "WHERE TRIM(COALESCE(mbid, '')) = ''"
        else:
            where_sql = ""
        cur.execute(
            f"""
            SELECT artist_norm, album_norm, mbid, created_at
            FROM musicbrainz_album_lookup
            {where_sql}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        return {"available": True, "summary": summary, "items": [dict(row) for row in cur.fetchall()], "limit": limit}
    finally:
        con.close()

def _mcp_review_proposals(args: dict[str, Any]) -> dict[str, Any]:
    args = args if isinstance(args, dict) else {}
    limit = _mcp_safe_limit(args.get("limit"), default=100, maximum=500)
    status = str(args.get("status") or "").strip().lower()
    con = sqlite3.connect(str(STATE_DB_FILE), timeout=15)
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()
        if not _sqlite_table_exists(cur, "mcp_review_proposals"):
            return {"available": False, "reason": "mcp_review_proposals_missing", "items": []}
        where = []
        params: list[Any] = []
        if status:
            where.append("LOWER(status) = ?")
            params.append(status)
        scan_id = _int_or_none(args.get("scan_id"))
        if scan_id:
            where.append("scan_id = ?")
            params.append(scan_id)
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        cur.execute(
            f"""
            SELECT *
            FROM mcp_review_proposals
            {where_sql}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            tuple(params + [limit]),
        )
        items = []
        for row in cur.fetchall():
            item = dict(row)
            item["evidence"] = _json_loads_safe(item.pop("evidence_json", None), {})
            item["proposed_actions"] = _json_loads_safe(item.pop("proposed_actions_json", None), [])
            items.append(item)
        cur.execute(
            """
            SELECT LOWER(COALESCE(status, 'pending')) AS status, COUNT(*) AS count
            FROM mcp_review_proposals
            GROUP BY LOWER(COALESCE(status, 'pending'))
            """
        )
        status_counts = {str(r["status"] or "pending"): int(r["count"] or 0) for r in cur.fetchall()}
        return {"available": True, "status_counts": status_counts, "items": items, "limit": limit}
    finally:
        con.close()

def _mcp_sqlite_columns(cur: sqlite3.Cursor, table: str) -> set[str]:
    try:
        cur.execute(f"PRAGMA table_info({table})")
        return {str(row[1]) for row in cur.fetchall() if len(row) > 1}
    except Exception:
        return set()

def _mcp_review_stats(args: dict[str, Any] | None = None) -> dict[str, Any]:
    args = args if isinstance(args, dict) else {}
    con = sqlite3.connect(str(STATE_DB_FILE), timeout=15)
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()
        scan_id = _mcp_scan_id_from_args(args, con)
        out: dict[str, Any] = {"available": True, "scan_id": scan_id}
        if scan_id and _sqlite_table_exists(cur, "scan_pipeline_trace"):
            trace_summary = _mcp_scan_trace_summary(cur, scan_id, {})
            out["scan_trace"] = {
                "total": trace_summary.get("total", 0),
                "unmatched_rows": trace_summary.get("unmatched_rows", 0),
                "unmatched_percent": trace_summary.get("unmatched_percent", 0),
                "incomplete_or_broken": trace_summary.get("incomplete_or_broken", 0),
                "duplicate_winners": trace_summary.get("duplicate_winners", 0),
                "duplicate_losers": trace_summary.get("duplicate_losers", 0),
                "quality": trace_summary.get("quality") or {},
                "reviews": trace_summary.get("reviews") or {},
            }
        if _sqlite_table_exists(cur, "duplicates_best"):
            best_cols = _mcp_sqlite_columns(cur, "duplicates_best")
            size_expr = "COALESCE(SUM(COALESCE(size_mb, 0)), 0)" if "size_mb" in best_cols else "0"
            cur.execute(
                f"""
                SELECT COUNT(*) AS groups,
                       COALESCE(SUM(CASE WHEN COALESCE(manual_review, 0) = 1 THEN 1 ELSE 0 END), 0) AS manual_review_groups,
                       COALESCE(SUM(CASE WHEN COALESCE(no_move, 0) = 1 THEN 1 ELSE 0 END), 0) AS no_move_groups,
                       COALESCE(SUM(CASE WHEN COALESCE(same_folder, 0) = 1 THEN 1 ELSE 0 END), 0) AS same_folder_groups,
                       {size_expr} AS winner_size_mb
                FROM duplicates_best
                """
            )
            row = dict(cur.fetchone() or {})
            loser_summary = {"losers": 0, "loser_size_mb": 0}
            if _sqlite_table_exists(cur, "duplicates_loser"):
                loser_cols = _mcp_sqlite_columns(cur, "duplicates_loser")
                loser_size_expr = "COALESCE(SUM(COALESCE(size_mb, 0)), 0)" if "size_mb" in loser_cols else "0"
                cur.execute(f"SELECT COUNT(*) AS losers, {loser_size_expr} AS loser_size_mb FROM duplicates_loser")
                loser_summary = dict(cur.fetchone() or loser_summary)
            out["duplicates"] = {
                "groups": int(row.get("groups") or 0),
                "manual_review_groups": int(row.get("manual_review_groups") or 0),
                "no_move_groups": int(row.get("no_move_groups") or 0),
                "same_folder_groups": int(row.get("same_folder_groups") or 0),
                "winner_size_mb": int(row.get("winner_size_mb") or 0),
                "losers": int(loser_summary.get("losers") or 0),
                "loser_size_mb": int(loser_summary.get("loser_size_mb") or 0),
                "loser_size_gb": round(int(loser_summary.get("loser_size_mb") or 0) / 1024.0, 3),
            }
            try:
                registry = _global_duplicate_review_registry(include_live=True)
                merged_counts = ((registry.get("sources") or {}).get("merged") or {})
                recovered_group_count = int(merged_counts.get("groups") or 0)
                recovered_loser_count = int(merged_counts.get("losers") or 0)
                out["duplicates"]["open_registry_groups"] = int(row.get("groups") or 0)
                out["duplicates"]["trace_recovered_groups"] = max(0, recovered_group_count - int(row.get("groups") or 0))
                out["duplicates"]["sources"] = registry.get("sources") or {}
                out["duplicates"]["groups"] = max(int(out["duplicates"]["groups"]), int(recovered_group_count))
                out["duplicates"]["losers"] = max(int(out["duplicates"]["losers"]), int(recovered_loser_count))
            except Exception:
                logging.debug("Failed to merge trace-recovered duplicate stats", exc_info=True)
        if _sqlite_table_exists(cur, "broken_albums"):
            cur.execute(
                """
                SELECT COUNT(*) AS albums,
                       COALESCE(SUM(CASE WHEN TRIM(COALESCE(review_status, '')) <> '' THEN 1 ELSE 0 END), 0) AS with_review_status,
                       COALESCE(SUM(CASE WHEN COALESCE(quarantine_eligible, 0) = 1 THEN 1 ELSE 0 END), 0) AS quarantine_eligible,
                       COALESCE(SUM(CASE WHEN expected_track_count IS NOT NULL AND actual_track_count IS NOT NULL AND actual_track_count < expected_track_count THEN 1 ELSE 0 END), 0) AS track_count_deficits,
                       COUNT(DISTINCT artist) AS artists
                FROM broken_albums
                """
            )
            row = dict(cur.fetchone() or {})
            cur.execute(
                """
                SELECT LOWER(COALESCE(classification, 'unknown')) AS classification, COUNT(*) AS count
                FROM broken_albums
                GROUP BY LOWER(COALESCE(classification, 'unknown'))
                ORDER BY count DESC, classification ASC
                """
            )
            classification_counts = {str(r["classification"] or "unknown"): int(r["count"] or 0) for r in cur.fetchall()}
            cur.execute(
                """
                SELECT LOWER(COALESCE(review_status, 'pending')) AS status, COUNT(*) AS count
                FROM broken_albums
                GROUP BY LOWER(COALESCE(review_status, 'pending'))
                ORDER BY count DESC, status ASC
                """
            )
            status_counts = {str(r["status"] or "pending"): int(r["count"] or 0) for r in cur.fetchall()}
            out["incompletes"] = {
                "albums": int(row.get("albums") or 0),
                "artists": int(row.get("artists") or 0),
                "with_review_status": int(row.get("with_review_status") or 0),
                "quarantine_eligible": int(row.get("quarantine_eligible") or 0),
                "track_count_deficits": int(row.get("track_count_deficits") or 0),
                "classification_counts": classification_counts,
                "status_counts": status_counts,
            }
        out["review_proposals"] = _mcp_review_proposals({"scan_id": scan_id, "limit": 25}) if _sqlite_table_exists(cur, "mcp_review_proposals") else {"available": False}
        return out
    finally:
        con.close()

def _mcp_iso(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    return value

def _mcp_pg_table_columns(cur: Any, table: str) -> set[str]:
    try:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
              AND table_schema = ANY(current_schemas(false))
            """,
            (table,),
        )
        return {str(row[0]) for row in cur.fetchall()}
    except Exception:
        logging.debug("MCP could not inspect PostgreSQL table columns for %s", table, exc_info=True)
        return set()

def _mcp_pg_group_counts(cur: Any, table: str, column: str, columns: set[str], *, limit: int = 25) -> dict[str, int]:
    if column not in columns:
        return {}
    try:
        cur.execute(
            f"""
            SELECT LOWER(COALESCE({column}, '')) AS key, COUNT(*) AS count
            FROM {table}
            GROUP BY LOWER(COALESCE({column}, ''))
            ORDER BY count DESC, key ASC
            LIMIT %s
            """,
            (int(limit),),
        )
        return {str(row[0] or "none"): int(row[1] or 0) for row in cur.fetchall()}
    except Exception:
        logging.debug("MCP PostgreSQL group count failed for %s.%s", table, column, exc_info=True)
        return {}

def _mcp_enrichment_stats_from_cursor(cur: Any) -> dict[str, Any]:
    out: dict[str, Any] = {"available": True}

    artist_cols = _mcp_pg_table_columns(cur, "files_artists")
    if artist_cols:
        has_image_expr = "COALESCE(SUM(CASE WHEN COALESCE(has_image, false) = true THEN 1 ELSE 0 END), 0)" if "has_image" in artist_cols else "0"
        image_path_expr = "COALESCE(SUM(CASE WHEN TRIM(COALESCE(image_path, '')) <> '' THEN 1 ELSE 0 END), 0)" if "image_path" in artist_cols else "0"
        mbid_expr = "COALESCE(SUM(CASE WHEN TRIM(COALESCE(canonical_mbid, '')) <> '' THEN 1 ELSE 0 END), 0)" if "canonical_mbid" in artist_cols else "0"
        entity_expr = "LOWER(COALESCE(entity_kind, 'artist'))" if "entity_kind" in artist_cols else "'artist'"
        cur.execute(
            f"""
            SELECT COUNT(*) AS total,
                   {has_image_expr} AS with_image,
                   {image_path_expr} AS with_image_path,
                   {mbid_expr} AS with_mbid
            FROM files_artists
            """
        )
        row = cur.fetchone() or [0, 0, 0, 0]
        out["artists"] = {
            "total": int(row[0] or 0),
            "with_image": int(row[1] or 0),
            "missing_image": max(0, int(row[0] or 0) - int(row[1] or 0)),
            "image_percent": _mcp_percent(row[1], row[0]),
            "with_image_path": int(row[2] or 0),
            "with_musicbrainz_mbid": int(row[3] or 0),
        }
        try:
            cur.execute(
                f"""
                SELECT {entity_expr} AS entity_kind, COUNT(*) AS count
                FROM files_artists
                GROUP BY {entity_expr}
                ORDER BY count DESC, entity_kind ASC
                """
            )
            out["artists"]["entity_kind_counts"] = {str(row[0] or "artist"): int(row[1] or 0) for row in cur.fetchall()}
        except Exception:
            out["artists"]["entity_kind_counts"] = {}

    album_cols = _mcp_pg_table_columns(cur, "files_albums")
    if album_cols:
        has_cover_expr = "COALESCE(SUM(CASE WHEN COALESCE(has_cover, false) = true THEN 1 ELSE 0 END), 0)" if "has_cover" in album_cols else "0"
        cover_path_expr = "COALESCE(SUM(CASE WHEN TRIM(COALESCE(cover_path, '')) <> '' THEN 1 ELSE 0 END), 0)" if "cover_path" in album_cols else "0"
        strict_expr = "COALESCE(SUM(CASE WHEN COALESCE(strict_match_verified, false) = true THEN 1 ELSE 0 END), 0)" if "strict_match_verified" in album_cols else "0"
        broken_expr = "COALESCE(SUM(CASE WHEN COALESCE(is_broken, false) = true THEN 1 ELSE 0 END), 0)" if "is_broken" in album_cols else "0"
        missing_tags_expr = "COALESCE(SUM(CASE WHEN TRIM(COALESCE(missing_required_tags_json, '[]')) NOT IN ('', '[]', '{}') THEN 1 ELSE 0 END), 0)" if "missing_required_tags_json" in album_cols else "0"
        id_exprs = []
        for col in ("musicbrainz_release_group_id", "musicbrainz_release_id", "discogs_release_id", "lastfm_album_mbid", "bandcamp_album_url"):
            if col in album_cols:
                id_exprs.append(f"COALESCE(SUM(CASE WHEN TRIM(COALESCE({col}, '')) <> '' THEN 1 ELSE 0 END), 0) AS {col}_count")
            else:
                id_exprs.append(f"0 AS {col}_count")
        cur.execute(
            f"""
            SELECT COUNT(*) AS total,
                   {has_cover_expr} AS with_cover,
                   {cover_path_expr} AS with_cover_path,
                   {strict_expr} AS strict_matched,
                   {broken_expr} AS broken,
                   {missing_tags_expr} AS missing_tags,
                   {", ".join(id_exprs)}
            FROM files_albums
            """
        )
        row = cur.fetchone() or [0] * 11
        total = int(row[0] or 0)
        with_cover = int(row[1] or 0)
        out["albums"] = {
            "total": total,
            "with_cover": with_cover,
            "missing_cover": max(0, total - with_cover),
            "cover_percent": _mcp_percent(with_cover, total),
            "with_cover_path": int(row[2] or 0),
            "strict_matched": int(row[3] or 0),
            "strict_matched_percent": _mcp_percent(row[3], total),
            "broken": int(row[4] or 0),
            "missing_required_tags": int(row[5] or 0),
            "identity_counts": {
                "musicbrainz_release_group": int(row[6] or 0),
                "musicbrainz_release": int(row[7] or 0),
                "discogs": int(row[8] or 0),
                "lastfm": int(row[9] or 0),
                "bandcamp": int(row[10] or 0),
            },
            "metadata_source_counts": _mcp_pg_group_counts(cur, "files_albums", "metadata_source", album_cols),
            "strict_provider_counts": _mcp_pg_group_counts(cur, "files_albums", "strict_match_provider", album_cols),
        }

    artist_profile_cols = _mcp_pg_table_columns(cur, "files_artist_profiles")
    if artist_profile_cols:
        bio_expr = "COALESCE(SUM(CASE WHEN TRIM(COALESCE(bio, '')) <> '' THEN 1 ELSE 0 END), 0)" if "bio" in artist_profile_cols else "0"
        short_bio_expr = "COALESCE(SUM(CASE WHEN TRIM(COALESCE(short_bio, '')) <> '' THEN 1 ELSE 0 END), 0)" if "short_bio" in artist_profile_cols else "0"
        tags_expr = "COALESCE(SUM(CASE WHEN TRIM(COALESCE(tags_json, '[]')) NOT IN ('', '[]', '{}') THEN 1 ELSE 0 END), 0)" if "tags_json" in artist_profile_cols else "0"
        similar_expr = "COALESCE(SUM(CASE WHEN TRIM(COALESCE(similar_json, '[]')) NOT IN ('', '[]', '{}') THEN 1 ELSE 0 END), 0)" if "similar_json" in artist_profile_cols else "0"
        updated_expr = "MAX(updated_at)" if "updated_at" in artist_profile_cols else "NULL"
        cur.execute(
            f"""
            SELECT COUNT(*) AS rows,
                   {bio_expr} AS with_bio,
                   {short_bio_expr} AS with_short_bio,
                   {tags_expr} AS with_tags,
                   {similar_expr} AS with_similar,
                   {updated_expr} AS newest_updated_at
            FROM files_artist_profiles
            """
        )
        row = cur.fetchone() or [0, 0, 0, 0, 0, None]
        out["artist_profiles"] = {
            "rows": int(row[0] or 0),
            "with_bio": int(row[1] or 0),
            "with_short_bio": int(row[2] or 0),
            "with_tags": int(row[3] or 0),
            "with_similar": int(row[4] or 0),
            "newest_updated_at": _mcp_iso(row[5]),
            "source_counts": _mcp_pg_group_counts(cur, "files_artist_profiles", "source", artist_profile_cols),
        }

    album_profile_cols = _mcp_pg_table_columns(cur, "files_album_profiles")
    if album_profile_cols:
        desc_expr = "COALESCE(SUM(CASE WHEN TRIM(COALESCE(description, '')) <> '' THEN 1 ELSE 0 END), 0)" if "description" in album_profile_cols else "0"
        short_desc_expr = "COALESCE(SUM(CASE WHEN TRIM(COALESCE(short_description, '')) <> '' THEN 1 ELSE 0 END), 0)" if "short_description" in album_profile_cols else "0"
        tags_expr = "COALESCE(SUM(CASE WHEN TRIM(COALESCE(tags_json, '[]')) NOT IN ('', '[]', '{}') THEN 1 ELSE 0 END), 0)" if "tags_json" in album_profile_cols else "0"
        rating_expr = "COALESCE(SUM(CASE WHEN public_rating IS NOT NULL THEN 1 ELSE 0 END), 0)" if "public_rating" in album_profile_cols else "0"
        heat_expr = "COALESCE(SUM(CASE WHEN heat_score IS NOT NULL THEN 1 ELSE 0 END), 0)" if "heat_score" in album_profile_cols else "0"
        supporter_expr = "COALESCE(SUM(CASE WHEN COALESCE(bandcamp_supporter_count, 0) > 0 THEN 1 ELSE 0 END), 0)" if "bandcamp_supporter_count" in album_profile_cols else "0"
        lastfm_expr = "COALESCE(SUM(CASE WHEN COALESCE(lastfm_listeners, 0) > 0 OR COALESCE(lastfm_scrobbles, 0) > 0 THEN 1 ELSE 0 END), 0)" if {"lastfm_listeners", "lastfm_scrobbles"}.issubset(album_profile_cols) else "0"
        discogs_expr = "COALESCE(SUM(CASE WHEN COALESCE(discogs_have_count, 0) > 0 OR COALESCE(discogs_want_count, 0) > 0 THEN 1 ELSE 0 END), 0)" if {"discogs_have_count", "discogs_want_count"}.issubset(album_profile_cols) else "0"
        updated_expr = "MAX(updated_at)" if "updated_at" in album_profile_cols else "NULL"
        cur.execute(
            f"""
            SELECT COUNT(*) AS rows,
                   {desc_expr} AS with_description,
                   {short_desc_expr} AS with_short_description,
                   {tags_expr} AS with_tags,
                   {rating_expr} AS with_public_rating,
                   {heat_expr} AS with_heat_score,
                   {supporter_expr} AS with_bandcamp_supporters,
                   {lastfm_expr} AS with_lastfm_popularity,
                   {discogs_expr} AS with_discogs_market,
                   {updated_expr} AS newest_updated_at
            FROM files_album_profiles
            """
        )
        row = cur.fetchone() or [0] * 10
        out["album_profiles"] = {
            "rows": int(row[0] or 0),
            "with_description": int(row[1] or 0),
            "with_short_description": int(row[2] or 0),
            "with_tags": int(row[3] or 0),
            "with_public_rating": int(row[4] or 0),
            "with_heat_score": int(row[5] or 0),
            "with_bandcamp_supporters": int(row[6] or 0),
            "with_lastfm_popularity": int(row[7] or 0),
            "with_discogs_market": int(row[8] or 0),
            "newest_updated_at": _mcp_iso(row[9]),
            "source_counts": _mcp_pg_group_counts(cur, "files_album_profiles", "source", album_profile_cols),
        }

    external_image_cols = _mcp_pg_table_columns(cur, "files_external_artist_images")
    if external_image_cols:
        path_expr = "COALESCE(SUM(CASE WHEN TRIM(COALESCE(image_path, '')) <> '' THEN 1 ELSE 0 END), 0)" if "image_path" in external_image_cols else "0"
        url_expr = "COALESCE(SUM(CASE WHEN TRIM(COALESCE(image_url, '')) <> '' THEN 1 ELSE 0 END), 0)" if "image_url" in external_image_cols else "0"
        updated_expr = "MAX(updated_at)" if "updated_at" in external_image_cols else "NULL"
        cur.execute(
            f"""
            SELECT COUNT(*) AS rows,
                   {path_expr} AS with_image_path,
                   {url_expr} AS with_image_url,
                   {updated_expr} AS newest_updated_at
            FROM files_external_artist_images
            """
        )
        row = cur.fetchone() or [0, 0, 0, None]
        out["external_artist_images"] = {
            "rows": int(row[0] or 0),
            "with_image_path": int(row[1] or 0),
            "with_image_url": int(row[2] or 0),
            "newest_updated_at": _mcp_iso(row[3]),
            "provider_counts": _mcp_pg_group_counts(cur, "files_external_artist_images", "provider", external_image_cols),
        }

    return out

def _mcp_enrichment_stats() -> dict[str, Any]:
    conn = _files_pg_connect(autocommit=True, acquire_timeout_sec=2.0)
    if conn is None:
        return {"available": False, "reason": "files_postgres_unavailable"}
    try:
        with conn.cursor() as cur:
            return _mcp_enrichment_stats_from_cursor(cur)
    except Exception as exc:
        logging.debug("MCP enrichment stats failed", exc_info=True)
        return {"available": False, "reason": str(exc)}
    finally:
        try:
            conn.close()
        except Exception:
            pass

def _mcp_library_stats() -> dict[str, Any]:
    conn = _files_pg_connect(autocommit=True, acquire_timeout_sec=2.0)
    if conn is None:
        return {"available": False, "reason": "files_postgres_unavailable"}
    try:
        with conn.cursor() as cur:
            payload: dict[str, Any] = {"available": True}
            for table, key in (("files_artists", "artists"), ("files_albums", "albums"), ("files_tracks", "tracks")):
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    payload[key] = int((cur.fetchone() or [0])[0] or 0)
                except Exception:
                    payload[key] = None
            try:
                cur.execute(
                    """
                    SELECT LOWER(COALESCE(metadata_source, '')) AS provider, COUNT(*) AS count
                    FROM files_albums
                    GROUP BY LOWER(COALESCE(metadata_source, ''))
                    ORDER BY count DESC, provider ASC
                    """
                )
                payload["album_metadata_source_counts"] = {str(row[0] or "none"): int(row[1] or 0) for row in cur.fetchall()}
            except Exception:
                payload["album_metadata_source_counts"] = {}
            try:
                cur.execute(
                    """
                    SELECT LOWER(COALESCE(strict_match_provider, '')) AS provider, COUNT(*) AS count
                    FROM files_albums
                    WHERE COALESCE(strict_match_verified, false) = true
                    GROUP BY LOWER(COALESCE(strict_match_provider, ''))
                    ORDER BY count DESC, provider ASC
                    """
                )
                payload["album_strict_provider_counts"] = {str(row[0] or "unknown"): int(row[1] or 0) for row in cur.fetchall()}
            except Exception:
                payload["album_strict_provider_counts"] = {}
            try:
                payload["enrichment"] = _mcp_enrichment_stats_from_cursor(cur)
            except Exception:
                logging.debug("MCP library enrichment stats failed", exc_info=True)
                payload["enrichment"] = {"available": False, "reason": "query_failed"}
            try:
                payload["export_backlog"] = _strict_export_backlog_summary()
            except Exception:
                logging.debug("MCP strict export backlog stats failed", exc_info=True)
                payload["export_backlog"] = {"available": False, "reason": "query_failed"}
            try:
                payload["smart_provider_promotion"] = _smart_provider_promotion_status()
            except Exception:
                payload["smart_provider_promotion"] = {"running": False, "reason": "query_failed"}
            return payload
    finally:
        try:
            conn.close()
        except Exception:
            pass

def _mcp_scan_analytics(args: dict[str, Any] | None = None) -> dict[str, Any]:
    args = args if isinstance(args, dict) else {}
    current = _mcp_scan_status()
    now = time.time()
    with lock:
        active_snapshot = dict(state.get("scan_active_artists") or {})
        scan_start_time = state.get("scan_start_time") or state.get("scan_start_requested_at")
        provider_matches = _normalize_scan_provider_matches(
            dict(state.get("scan_provider_matches") or {}),
            legacy_discogs=int(state.get("scan_discogs_matched") or 0),
            legacy_lastfm=int(state.get("scan_lastfm_matched") or 0),
            legacy_bandcamp=int(state.get("scan_bandcamp_matched") or 0),
        )
        raw_state = {
            "scan_audio_cache_hits": int(state.get("scan_audio_cache_hits") or 0),
            "scan_audio_cache_misses": int(state.get("scan_audio_cache_misses") or 0),
            "scan_mb_cache_hits": int(state.get("scan_mb_cache_hits") or 0),
            "scan_mb_cache_misses": int(state.get("scan_mb_cache_misses") or 0),
            "scan_format_done_count": int(state.get("scan_format_done_count") or 0),
            "scan_mb_done_count": int(state.get("scan_mb_done_count") or 0),
            "scan_incomplete_moved_count": int(state.get("scan_incomplete_moved_count") or 0),
            "scan_incomplete_moved_mb": int(state.get("scan_incomplete_moved_mb") or 0),
            "scan_dupe_moved_count": int(state.get("scan_dupe_moved_count") or 0),
            "scan_dupe_moved_mb": int(state.get("scan_dupe_moved_mb") or 0),
            "scan_published_albums_count": int(state.get("scan_published_albums_count") or 0),
            "scan_resume_run_id": state.get("scan_resume_run_id") or state.get("scan_resume_requested_run_id"),
        }
    partial_album_progress = 0
    active_artists_started = 0
    active_artists = []
    for name, info in active_snapshot.items():
        if str(name or "").startswith("_") or not isinstance(info, dict):
            continue
        albums_done = int(info.get("albums_processed") or 0)
        total_artist_albums = max(albums_done, int(info.get("total_albums") or 0))
        current_album = info.get("current_album") if isinstance(info.get("current_album"), dict) else None
        current_album_index = _parse_int_loose((current_album or {}).get("album_index"), 0) if current_album else 0
        current_album_total = max(total_artist_albums, _parse_int_loose((current_album or {}).get("album_total"), 0) if current_album else 0)
        preview_done = albums_done
        if current_album_index > 0:
            preview_done = max(preview_done, max(0, current_album_index - 1))
        partial_album_progress += preview_done
        if albums_done > 0 or current_album:
            active_artists_started += 1
        active_artists.append(
            {
                "artist": str(name or ""),
                "albums_processed": albums_done,
                "total_albums": total_artist_albums,
                "effective_album_progress": preview_done,
                "current_album": current_album,
                "current_album_index": current_album_index,
                "current_album_total": current_album_total,
            }
        )
    albums_done = int(current.get("albums_processed") or 0)
    albums_total = int(current.get("albums_total") or 0)
    artists_done = int(current.get("artists_processed") or 0)
    artists_total = int(current.get("artists_total") or 0)
    albums_advanced = max(albums_done, min(albums_total or albums_done + partial_album_progress, albums_done + partial_album_progress))
    artists_advanced = max(artists_done, min(artists_total or artists_done + active_artists_started, artists_done + active_artists_started))
    try:
        start_ts = float(scan_start_time or 0)
    except Exception:
        start_ts = 0.0
    elapsed_seconds = max(0.0, now - start_ts) if start_ts > 0 else 0.0
    albums_per_hour = round((albums_advanced * 3600.0 / elapsed_seconds), 2) if elapsed_seconds > 0 and albums_advanced > 0 else 0.0
    eta_seconds = int(((albums_total - albums_advanced) / albums_per_hour) * 3600.0) if albums_per_hour > 0 and albums_total > albums_advanced else None
    con = sqlite3.connect(str(STATE_DB_FILE), timeout=15)
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()
        scan_id = _mcp_scan_id_from_args(args, con)
        history = _mcp_scan_history_row(cur, scan_id)
        trace_summary = _mcp_scan_trace_summary(cur, scan_id, {}) if scan_id else {"available": False, "total": 0}
    finally:
        con.close()
    provider_hit_events = int(sum(int(v or 0) for v in provider_matches.values()))
    review_stats = _mcp_review_stats({"scan_id": scan_id}) if scan_id else {"available": False, "reason": "no_scan_id"}
    enrichment_stats = _mcp_enrichment_stats()
    return {
        "scan_id": scan_id,
        "current": current,
        "progress": {
            "artists_committed": artists_done,
            "artists_advanced": artists_advanced,
            "artists_total": artists_total,
            "artists_committed_percent": _mcp_percent(artists_done, artists_total),
            "artists_advanced_percent": _mcp_percent(artists_advanced, artists_total),
            "albums_committed": albums_done,
            "albums_advanced": albums_advanced,
            "albums_total": albums_total,
            "albums_committed_percent": _mcp_percent(albums_done, albums_total),
            "albums_advanced_percent": _mcp_percent(albums_advanced, albums_total),
            "mb_done": raw_state["scan_mb_done_count"],
            "mb_percent": _mcp_percent(raw_state["scan_mb_done_count"], albums_total),
            "stage_done": int(current.get("stage_done") or 0),
            "stage_total": int(current.get("stage_total") or 0),
            "stage_percent": _mcp_percent(current.get("stage_done"), current.get("stage_total")),
        },
        "timing": {
            "started_at": start_ts or None,
            "elapsed_seconds": int(elapsed_seconds),
            "albums_per_hour": albums_per_hour,
            "eta_seconds": eta_seconds,
            "eta_hours": round(eta_seconds / 3600.0, 2) if eta_seconds is not None else None,
        },
        "matches": {
            "provider_hit_events": provider_hit_events,
            "provider_hit_events_by_provider": provider_matches,
            "trace": trace_summary,
        },
        "reviews": review_stats,
        "enrichment": enrichment_stats,
        "cache_live": {
            "audio_hits": raw_state["scan_audio_cache_hits"],
            "audio_misses": raw_state["scan_audio_cache_misses"],
            "musicbrainz_hits": raw_state["scan_mb_cache_hits"],
            "musicbrainz_misses": raw_state["scan_mb_cache_misses"],
        },
        "moves_live": {
            "duplicate_moved_count": raw_state["scan_dupe_moved_count"],
            "duplicate_moved_mb": raw_state["scan_dupe_moved_mb"],
            "incomplete_moved_count": raw_state["scan_incomplete_moved_count"],
            "incomplete_moved_mb": raw_state["scan_incomplete_moved_mb"],
            "total_moved_count": raw_state["scan_dupe_moved_count"] + raw_state["scan_incomplete_moved_count"],
            "total_moved_mb": raw_state["scan_dupe_moved_mb"] + raw_state["scan_incomplete_moved_mb"],
        },
        "storage": _mcp_storage_current(),
        "jobs": _pipeline_job_snapshot(),
        "active_artists": active_artists,
        "provider_gateway": _provider_gateway_stats_snapshot(),
        "history": history,
    }

def _mcp_scan_results(args: dict[str, Any]) -> dict[str, Any]:
    args = args if isinstance(args, dict) else {}
    limit = _mcp_safe_limit(args.get("limit"), default=100, maximum=1000)
    trace_args = dict(args)
    trace_args["limit"] = limit
    trace_args["page_size"] = limit
    trace_payload = _mcp_scan_pipeline_trace(trace_args)
    scan_id = trace_payload.get("scan_id")
    con = sqlite3.connect(str(STATE_DB_FILE), timeout=15)
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()
        history = _mcp_scan_history_row(cur, _int_or_none(scan_id))
    finally:
        con.close()
    moves = _mcp_scan_moves({"scan_id": scan_id, "limit": min(limit, 250)}) if scan_id else {"available": False}
    proposals = _mcp_review_proposals({"scan_id": scan_id, "limit": min(limit, 250)}) if scan_id else {"available": False}
    return {
        "scan_id": scan_id,
        "history": history,
        "trace": trace_payload,
        "moves": moves,
        "review_stats": _mcp_review_stats({"scan_id": scan_id}) if scan_id else {"available": False},
        "enrichment": _mcp_enrichment_stats(),
        "duplicates": _mcp_duplicate_groups(min(limit, 200)),
        "incompletes": _mcp_incomplete_albums(min(limit, 200)),
        "review_proposals": proposals,
    }

def _mcp_library_search(query: str = "", limit: int = 20) -> dict[str, Any]:
    q = str(query or "").strip()
    safe_limit = max(1, min(50, int(limit or 20)))
    if not q:
        return {"query": q, "artists": [], "albums": [], "message": "query_required"}
    conn = _files_pg_connect(autocommit=True, acquire_timeout_sec=2.0)
    if conn is None:
        return {"query": q, "artists": [], "albums": [], "unavailable": True, "reason": "files_postgres_unavailable"}
    like = f"%{q.lower()}%"
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, name, canonical_mbid, album_count, track_count, has_image
                FROM files_artists
                WHERE LOWER(name) LIKE %s OR LOWER(COALESCE(canonical_name, '')) LIKE %s
                ORDER BY album_count DESC, name ASC
                LIMIT %s
                """,
                (like, like, safe_limit),
            )
            artists = [
                {
                    "id": row[0],
                    "name": row[1],
                    "canonical_mbid": row[2],
                    "album_count": row[3],
                    "track_count": row[4],
                    "has_image": bool(row[5]),
                }
                for row in cur.fetchall()
            ]
            cur.execute(
                """
                SELECT alb.id, alb.title, art.name, alb.year, alb.folder_path,
                       alb.strict_match_provider, alb.musicbrainz_release_group_id,
                       alb.musicbrainz_release_id, alb.discogs_release_id, alb.metadata_source
                FROM files_albums alb
                JOIN files_artists art ON art.id = alb.artist_id
                WHERE LOWER(alb.title) LIKE %s OR LOWER(art.name) LIKE %s
                ORDER BY alb.updated_at DESC
                LIMIT %s
                """,
                (like, like, safe_limit),
            )
            albums = [
                {
                    "id": row[0],
                    "title": row[1],
                    "artist": row[2],
                    "year": row[3],
                    "folder_path": row[4],
                    "strict_match_provider": row[5],
                    "musicbrainz_release_group_id": row[6],
                    "musicbrainz_release_id": row[7],
                    "discogs_release_id": row[8],
                    "metadata_source": row[9],
                }
                for row in cur.fetchall()
            ]
        return {"query": q, "artists": artists, "albums": albums, "limit": safe_limit}
    finally:
        try:
            conn.close()
        except Exception:
            pass

def _mcp_create_review_proposal(args: dict[str, Any], token: dict[str, Any]) -> dict[str, Any]:
    kind = str(args.get("kind") or "").strip().lower()
    if kind not in {"duplicate", "incomplete", "batch"}:
        raise ValueError("kind must be duplicate, incomplete, or batch")
    title = str(args.get("title") or "").strip()
    recommendation = str(args.get("recommendation") or "").strip()
    if not title or not recommendation:
        raise ValueError("title and recommendation are required")
    try:
        confidence = float(args.get("confidence")) if args.get("confidence") is not None else None
    except Exception:
        confidence = None
    if confidence is not None:
        confidence = max(0.0, min(1.0, confidence))
    proposal_id = f"mcp_prop_{uuid.uuid4().hex}"
    now = time.time()
    scan_id = _int_or_none(args.get("scan_id"))
    target_key = str(args.get("target_key") or "").strip()[:500]
    evidence = args.get("evidence") if isinstance(args.get("evidence"), (dict, list)) else {}
    proposed_actions = args.get("proposed_actions") if isinstance(args.get("proposed_actions"), list) else []
    con = sqlite3.connect(str(STATE_DB_FILE), timeout=5)
    con.execute(
        """
        INSERT INTO mcp_review_proposals(
            proposal_id, kind, status, scan_id, target_key, title, recommendation, confidence,
            evidence_json, proposed_actions_json, created_by_token_id, created_at, updated_at
        )
        VALUES(?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            proposal_id,
            kind,
            scan_id,
            target_key,
            title,
            recommendation,
            confidence,
            json.dumps(evidence, ensure_ascii=False),
            json.dumps(proposed_actions[:100], ensure_ascii=False),
            str(token.get("token_id") or ""),
            now,
            now,
        ),
    )
    con.commit()
    con.close()
    return {
        "proposal_id": proposal_id,
        "kind": kind,
        "status": "pending",
        "scan_id": scan_id,
        "target_key": target_key,
        "title": title,
        "message": "Proposal created for human validation. No files were moved.",
    }

def _mcp_storage_current() -> dict[str, Any]:
    with lock:
        return {
            "enabled": bool(state.get("storage_power_saver_enabled")),
            "provider": str(state.get("storage_provider") or STORAGE_PROVIDER or "unraid"),
            "active_devices": int(state.get("storage_active_devices") or 0),
            "devices_total": int(state.get("storage_devices_total") or 0),
            "current_device_id": state.get("storage_current_device_id"),
            "current_device_label": state.get("storage_current_device_label"),
            "bucket_done": int(state.get("storage_bucket_done") or 0),
            "bucket_total": int(state.get("storage_bucket_total") or 0),
            "buckets_done": int(state.get("storage_buckets_done") or 0),
            "buckets_total": int(state.get("storage_buckets_total") or 0),
            "estimated_watts_saved": float(state.get("storage_estimated_watts_saved") or 0.0),
            "current_bucket": dict(state.get("storage_current_bucket") or {}),
            "validation_error": str(state.get("storage_validation_error") or ""),
            "started_at": state.get("storage_started_at"),
            "bucket_started_at": state.get("storage_bucket_started_at"),
        }

def _mcp_storage_plan() -> dict[str, Any]:
    run_id = ""
    with lock:
        run_id = str(state.get("scan_resume_run_id") or state.get("scan_resume_requested_run_id") or "").strip()
        plan = copy.deepcopy(state.get("storage_scan_plan") or [])
        history = copy.deepcopy(state.get("storage_bucket_history") or [])
    db_buckets: list[dict[str, Any]] = []
    if run_id:
        try:
            con = _state_connect_readonly(timeout=5)
            con.row_factory = sqlite3.Row
            rows = con.execute(
                """
                SELECT bucket_order, storage_provider, storage_device_id, storage_device_label,
                       canonical_root, access_root, albums_total, albums_done,
                       started_at, finished_at, status, message
                FROM scan_storage_buckets
                WHERE run_id = ?
                ORDER BY bucket_order, storage_device_id, canonical_root
                """,
                (run_id,),
            ).fetchall()
            con.close()
            db_buckets = [dict(row) for row in rows]
        except Exception:
            logging.debug("MCP storage plan DB read failed", exc_info=True)
    return {
        "current": _mcp_storage_current(),
        "run_id": run_id or None,
        "plan": plan,
        "history": history,
        "persisted_buckets": db_buckets,
    }

def _mcp_dispatch_tool(tool: str, args: dict[str, Any], token: dict[str, Any]) -> dict[str, Any]:
    name = str(tool or "").strip()
    args = args if isinstance(args, dict) else {}
    read_tools = {
        "pmda.status": lambda: {"pmda": "ok", "mcp": _mcp_status_summary(include_audit=False), "scan": _mcp_scan_status()},
        "pmda.jobs.status": _pmda_jobs_status_snapshot,
        "pmda.scan.current": _mcp_scan_status,
        "pmda.scan.analytics": lambda: _mcp_scan_analytics(args),
        "pmda.storage.current": _mcp_storage_current,
        "pmda.storage.plan": _mcp_storage_plan,
        "pmda.pipeline.jobs": _pipeline_job_snapshot,
        "pmda.scan.results": lambda: _mcp_scan_results(args),
        "pmda.scan.pipeline_trace": lambda: _mcp_scan_pipeline_trace(args),
        "pmda.scan.moves": lambda: _mcp_scan_moves(args),
        "pmda.scan.resume_state": lambda: _mcp_scan_resume_state(args),
        "pmda.scan.history": lambda: _mcp_scan_history(int(args.get("limit") or 20)),
        "pmda.logs.tail": lambda: _mcp_logs_tail(int(args.get("lines") or 200), bool(args.get("scan_mode", True))),
        "pmda.providers.stats": _provider_gateway_stats_snapshot,
        "pmda.providers.cache": lambda: _mcp_provider_cache_stats(args),
        "pmda.cache.stats": _mcp_cache_stats,
        "pmda.musicbrainz.cache": lambda: _mcp_musicbrainz_cache_stats(args),
        "pmda.runtime.status": lambda: _managed_runtime_status_snapshot(include_candidates=False),
        "pmda.musicbrainz.health": lambda: {"target": _musicbrainz_target_settings(), "runtime": _managed_runtime_status_snapshot(include_candidates=False).get("bundles", {}).get(_MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE)},
        "pmda.ollama.health": lambda: {"runtime": _managed_runtime_status_snapshot(include_candidates=False).get("bundles", {}).get(_MANAGED_RUNTIME_OLLAMA_BUNDLE), "model": _ollama_model_configured(), "hard_model": _ollama_complex_model_configured()},
        "pmda.library.stats": _mcp_library_stats,
        "pmda.enrichment.stats": _mcp_enrichment_stats,
        "pmda.library.search": lambda: _mcp_library_search(str(args.get("query") or ""), int(args.get("limit") or 20)),
        "pmda.duplicates.list": lambda: _mcp_duplicate_groups(int(args.get("limit") or 50)),
        "pmda.incompletes.list": lambda: _mcp_incomplete_albums(int(args.get("limit") or 50)),
        "pmda.review.stats": lambda: _mcp_review_stats(args),
        "pmda.review.proposals": lambda: _mcp_review_proposals(args),
    }
    if name in read_tools:
        _mcp_require_scope(token, "read")
        return read_tools[name]()

    if name == "pmda.scan.start":
        _mcp_require_scope(token, "scan_control")
        scan_type = str(args.get("scan_type") or "").strip().lower()
        body = {"scan_type": scan_type} if scan_type else {}
        if "run_improve_after" in args:
            body["run_improve_after"] = bool(args.get("run_improve_after"))
        return _mcp_route_payload(start_scan, "/scan/start", body=body)
    if name == "pmda.scan.pause":
        _mcp_require_scope(token, "scan_control")
        return _mcp_route_payload(pause_scan, "/scan/pause", body={})
    if name == "pmda.scan.resume":
        _mcp_require_scope(token, "scan_control")
        body = {"scan_type": str(args.get("scan_type") or "").strip().lower()} if args.get("scan_type") else {}
        return _mcp_route_payload(resume_scan, "/scan/resume", body=body)
    if name == "pmda.scan.stop":
        _mcp_require_scope(token, "scan_control")
        return _mcp_route_payload(stop_scan, "/scan/stop", body={})

    if name in {"pmda.index.rebuild", "pmda.musicbrainz.repair", "pmda.ollama.pull"}:
        _mcp_require_scope(token, "runtime_repair")
        if name == "pmda.index.rebuild":
            return _mcp_route_payload(api_library_files_index_rebuild, "/api/library/files-index/rebuild", body={})
        if name == "pmda.musicbrainz.repair":
            return _mcp_route_payload(
                api_runtime_managed_action,
                "/api/runtime/managed/action",
                body={"bundle_type": _MANAGED_RUNTIME_MUSICBRAINZ_BUNDLE, "action": "repair-search-index"},
            )
        model = str(args.get("model") or "").strip()
        return _mcp_route_payload(
            api_runtime_managed_action,
            "/api/runtime/managed/action",
            body={"bundle_type": _MANAGED_RUNTIME_OLLAMA_BUNDLE, "action": "pull-model", "model": model or _ollama_model_configured()},
        )

    if name == "pmda.review.propose":
        _mcp_require_scope(token, "review_propose")
        return _mcp_create_review_proposal(args, token)

    raise KeyError(f"unknown_tool:{name}")

_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals().get(name) for name in _EXTRACTED_NAMES}

def _mcp_token_hash_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_token_hash(*args, **kwargs)

def _mcp_enabled_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_enabled(*args, **kwargs)

def _mcp_normalize_scopes_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_normalize_scopes(*args, **kwargs)

def _mcp_row_to_token_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_row_to_token(*args, **kwargs)

def _mcp_active_token_snapshot_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_active_token_snapshot(*args, **kwargs)

def _mcp_latest_audit_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_latest_audit(*args, **kwargs)

def _mcp_status_summary_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_status_summary(*args, **kwargs)

def _mcp_generate_token_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_generate_token(*args, **kwargs)

def _mcp_revoke_active_tokens_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_revoke_active_tokens(*args, **kwargs)

def _mcp_authenticate_token_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_authenticate_token(*args, **kwargs)

def _mcp_request_tool_and_args_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_request_tool_and_args(*args, **kwargs)

def _mcp_auth_guard_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_auth_guard(*args, **kwargs)

def _mcp_scrub_args_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_scrub_args(*args, **kwargs)

def _mcp_audit_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_audit(*args, **kwargs)

def _mcp_require_scope_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_require_scope(*args, **kwargs)

def _mcp_route_payload_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_route_payload(*args, **kwargs)

def _mcp_scan_status_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_scan_status(*args, **kwargs)

def _mcp_scan_history_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_scan_history(*args, **kwargs)

def _mcp_logs_tail_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_logs_tail(*args, **kwargs)

def _mcp_duplicate_groups_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_duplicate_groups(*args, **kwargs)

def _mcp_incomplete_albums_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_incomplete_albums(*args, **kwargs)

def _mcp_safe_limit_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_safe_limit(*args, **kwargs)

def _mcp_percent_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_percent(*args, **kwargs)

def _mcp_scan_id_from_args_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_scan_id_from_args(*args, **kwargs)

def _mcp_scan_history_row_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_scan_history_row(*args, **kwargs)

def _mcp_scan_trace_where_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_scan_trace_where(*args, **kwargs)

def _mcp_scan_trace_summary_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_scan_trace_summary(*args, **kwargs)

def _mcp_scan_pipeline_trace_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_scan_pipeline_trace(*args, **kwargs)

def _mcp_scan_moves_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_scan_moves(*args, **kwargs)

def _mcp_scan_resume_state_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_scan_resume_state(*args, **kwargs)

def _mcp_cache_stats_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_cache_stats(*args, **kwargs)

def _mcp_provider_cache_stats_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_provider_cache_stats(*args, **kwargs)

def _mcp_musicbrainz_cache_stats_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_musicbrainz_cache_stats(*args, **kwargs)

def _mcp_review_proposals_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_review_proposals(*args, **kwargs)

def _mcp_sqlite_columns_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_sqlite_columns(*args, **kwargs)

def _mcp_review_stats_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_review_stats(*args, **kwargs)

def _mcp_iso_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_iso(*args, **kwargs)

def _mcp_pg_table_columns_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_pg_table_columns(*args, **kwargs)

def _mcp_pg_group_counts_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_pg_group_counts(*args, **kwargs)

def _mcp_enrichment_stats_from_cursor_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_enrichment_stats_from_cursor(*args, **kwargs)

def _mcp_enrichment_stats_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_enrichment_stats(*args, **kwargs)

def _mcp_library_stats_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_library_stats(*args, **kwargs)

def _mcp_scan_analytics_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_scan_analytics(*args, **kwargs)

def _mcp_scan_results_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_scan_results(*args, **kwargs)

def _mcp_library_search_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_library_search(*args, **kwargs)

def _mcp_create_review_proposal_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_create_review_proposal(*args, **kwargs)

def _mcp_storage_current_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_storage_current(*args, **kwargs)

def _mcp_storage_plan_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_storage_plan(*args, **kwargs)

def _mcp_dispatch_tool_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _mcp_dispatch_tool(*args, **kwargs)
