"""Runtime-owned scheduler rules, task events, and post-scan job orchestration."""

from __future__ import annotations

from typing import Any

_RUNTIME: Any | None = None

_EXTRACTED_NAMES = {
    '_normalize_task_job_type',
    '_normalize_task_scope',
    '_normalize_scheduler_trigger',
    '_task_event_start',
    '_task_event_finish',
    '_scheduler_get_paused_from_db',
    '_scheduler_set_paused',
    '_parse_days_of_week',
    '_parse_time_local',
    '_scheduler_compute_next_run',
    '_scheduler_rule_scope_matches',
    '_scheduler_rules_fetch',
    '_scheduler_rule_update_runtime',
    '_scheduler_insert_default_rules_if_empty',
    '_scheduler_migrate_legacy_scan_changed_default',
    '_scheduler_migrate_legacy_scan_full_default',
    '_scheduler_ensure_post_scan_chain_defaults',
    '_pipeline_migrate_legacy_post_scan_async_default',
    '_library_migrate_legacy_include_unmatched_default',
    '_provider_gateway_migrate_legacy_discogs_rpm_default',
    '_web_search_migrate_legacy_provider_default',
    '_scheduler_job_insert',
    '_scheduler_job_update',
    '_scheduler_record_skipped_job',
    '_scheduler_get_latest_scan_entry',
    '_scheduler_job_key',
    '_scheduler_pool_for_job',
    '_scheduler_pool_limit',
    '_scheduler_can_start_job',
    '_scheduler_start_scan',
    '_scheduler_wait_for_scan_completion',
    '_scheduler_build_improve_candidates',
    '_scheduler_run_enrich_batch',
    '_scheduler_run_dedupe',
    '_scheduler_run_incomplete_move',
    '_scheduler_run_export',
    '_scheduler_run_player_sync',
    '_scheduler_execute_job',
    '_scheduler_worker',
    '_scheduler_launch_job',
    '_scheduler_is_enabled_rule_for_chain',
    '_scheduler_chain_max_concurrency',
    '_scheduler_loop',
    '_start_scheduler_if_needed',
    '_stop_scheduler',
    '_scheduler_chain_post_scan',
    '_scheduler_rule_to_dict',
    '_scheduler_rules_replace',
}

_MUTABLE_GLOBAL_NAMES = {
    '_scheduler_paused',
    '_scheduler_thread',
    'PIPELINE_POST_SCAN_ASYNC',
    'LIBRARY_INCLUDE_UNMATCHED',
    'PROVIDER_GATEWAY_DISCOGS_RPM',
    'WEB_SEARCH_PROVIDER',
}


def _bind_runtime(runtime: Any) -> None:
    global _RUNTIME
    _RUNTIME = runtime
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


def _runtime_module() -> Any:
    if _RUNTIME is None:
        raise RuntimeError("scheduler runtime is not bound")
    return _RUNTIME


def _sync_runtime_globals(runtime: Any) -> None:
    for name in _MUTABLE_GLOBAL_NAMES:
        if name in globals():
            try:
                setattr(runtime, name, globals()[name])
            except Exception:
                pass


def _normalize_task_job_type(value: str | None) -> str:
    raw = str(value or "").strip().lower()
    return raw if raw in TASK_JOB_TYPES else ""


def _normalize_task_scope(value: str | None, *, default: str = "both") -> str:
    raw = str(value or "").strip().lower()
    if raw in TASK_SCOPES:
        return raw
    return default


def _normalize_scheduler_trigger(value: str | None) -> str:
    raw = str(value or "").strip().lower()
    return raw if raw in SCHEDULER_TRIGGER_TYPES else "interval"


def _task_event_start(
    *,
    run_id: str,
    job_type: str,
    scope: str,
    source: str,
    message: str = "",
) -> int:
    started = time.time()
    con = _state_connect(timeout=15)
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO task_events
        (run_id, job_type, scope, status, message, source, started_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (run_id, job_type, scope, "started", str(message or ""), str(source or ""), started),
    )
    event_id = int(cur.lastrowid or 0)
    con.commit()
    con.close()
    _task_events_cache_merge(
        [
            {
                "event_id": event_id,
                "run_id": str(run_id or ""),
                "job_type": str(job_type or ""),
                "scope": str(scope or "both"),
                "status": "started",
                "message": str(message or ""),
                "metrics": {},
                "summary": {},
                "error": "",
                "source": str(source or ""),
                "started_at": float(started or 0.0),
                "ended_at": None,
                "duration_ms": None,
            }
        ],
        max_id=event_id,
    )
    return event_id


def _task_event_finish(
    event_id: int,
    *,
    status: str,
    message: str = "",
    metrics: dict | None = None,
    summary: dict | None = None,
    error: str = "",
) -> None:
    ended = time.time()
    status_norm = str(status or "").strip().lower()
    if status_norm not in TASK_EVENT_STATUSES:
        status_norm = "failed"
    con = _state_connect(timeout=15)
    cur = con.cursor()
    cur.execute("SELECT started_at FROM task_events WHERE event_id = ?", (int(event_id or 0),))
    row = cur.fetchone()
    started_at = float(row["started_at"]) if row and row["started_at"] is not None else ended
    duration_ms = int(max(0.0, (ended - started_at) * 1000.0))
    cur.execute(
        """
        UPDATE task_events
        SET status = ?, message = ?, metrics_json = ?, summary_json = ?, error = ?,
            ended_at = ?, duration_ms = ?
        WHERE event_id = ?
        """,
        (
            status_norm,
            str(message or ""),
            _json_dumps_safe(metrics or {}),
            _json_dumps_safe(summary or {}),
            str(error or ""),
            ended,
            duration_ms,
            int(event_id or 0),
        ),
    )
    con.commit()
    con.close()
    _task_events_cache_merge(
        [
            {
                "event_id": int(event_id or 0),
                "run_id": "",
                "job_type": "",
                "scope": "both",
                "status": status_norm,
                "message": str(message or ""),
                "metrics": metrics or {},
                "summary": summary or {},
                "error": str(error or ""),
                "source": "",
                "started_at": float(started_at or 0.0),
                "ended_at": float(ended or 0.0),
                "duration_ms": int(duration_ms or 0),
            }
        ],
        max_id=int(event_id or 0),
    )


def _scheduler_get_paused_from_db() -> bool:
    try:
        val = _get_config_from_db("SCHEDULER_PAUSED")
    except Exception:
        val = None
    return bool(_parse_bool(val or False))


def _scheduler_set_paused(paused: bool) -> None:
    global _scheduler_paused
    _scheduler_paused = bool(paused)
    try:
        init_settings_db()
        con = sqlite3.connect(str(SETTINGS_DB_FILE), timeout=5)
        con.execute(
            "INSERT OR REPLACE INTO settings(key, value) VALUES(?, ?)",
            ("SCHEDULER_PAUSED", "true" if paused else "false"),
        )
        con.commit()
        con.close()
    except Exception:
        logging.debug("Failed to persist SCHEDULER_PAUSED", exc_info=True)


def _parse_days_of_week(value: str | None) -> list[int]:
    raw = str(value or "").strip()
    if not raw:
        return []
    out: list[int] = []
    for token in raw.split(","):
        tok = token.strip().lower()
        if not tok:
            continue
        if tok.isdigit():
            n = int(tok)
            if 0 <= n <= 6 and n not in out:
                out.append(n)
            continue
        aliases = {
            "mon": 0, "monday": 0,
            "tue": 1, "tuesday": 1,
            "wed": 2, "wednesday": 2,
            "thu": 3, "thursday": 3,
            "fri": 4, "friday": 4,
            "sat": 5, "saturday": 5,
            "sun": 6, "sunday": 6,
        }
        if tok in aliases and aliases[tok] not in out:
            out.append(aliases[tok])
    return sorted(out)


def _parse_time_local(value: str | None) -> tuple[int, int]:
    raw = str(value or "").strip()
    m = re.fullmatch(r"([01]?\d|2[0-3]):([0-5]\d)", raw)
    if not m:
        return (2, 0)
    return (int(m.group(1)), int(m.group(2)))


def _scheduler_compute_next_run(rule: sqlite3.Row | dict, now_ts: float | None = None) -> float:
    now = float(now_ts if now_ts is not None else time.time())
    trigger_type = _normalize_scheduler_trigger((rule["trigger_type"] if isinstance(rule, sqlite3.Row) else rule.get("trigger_type")))
    if trigger_type == "weekly":
        days = _parse_days_of_week((rule["days_of_week"] if isinstance(rule, sqlite3.Row) else rule.get("days_of_week")))
        if not days:
            days = [6]  # Sunday default
        hh, mm = _parse_time_local((rule["time_local"] if isinstance(rule, sqlite3.Row) else rule.get("time_local")))
        local_now = datetime.now().astimezone()
        tz = local_now.tzinfo
        # Find next allowed weekday/time in local timezone.
        for offset in range(0, 14):
            cand_date = (local_now + timedelta(days=offset)).date()
            if cand_date.weekday() not in days:
                continue
            cand_dt = datetime(
                cand_date.year,
                cand_date.month,
                cand_date.day,
                hh,
                mm,
                0,
                tzinfo=tz,
            )
            cand_ts = cand_dt.timestamp()
            if cand_ts > now + 1.0:
                return cand_ts
        return now + (24 * 3600)
    interval_min = 20
    try:
        interval_min = int((rule["interval_min"] if isinstance(rule, sqlite3.Row) else rule.get("interval_min")) or 20)
    except Exception:
        interval_min = 20
    interval_min = max(1, min(interval_min, 24 * 60))
    base = now
    try:
        last_run = float((rule["last_run_ts"] if isinstance(rule, sqlite3.Row) else rule.get("last_run_ts")) or 0.0)
        if last_run > 0:
            base = max(base, last_run)
    except Exception:
        pass
    return base + (interval_min * 60.0)


def _scheduler_rule_scope_matches(rule_scope: str, requested_scope: str) -> bool:
    rs = _normalize_task_scope(rule_scope, default="both")
    rq = _normalize_task_scope(requested_scope, default="both")
    if rs == "both" or rq == "both":
        return True
    return rs == rq


def _scheduler_rules_fetch() -> list[sqlite3.Row]:
    con = _state_connect(timeout=10)
    cur = con.cursor()
    cur.execute(
        """
        SELECT rule_id, job_type, enabled, trigger_type, interval_min, days_of_week, time_local,
               scope, post_scan_chain, priority, max_concurrency, next_run_ts, last_run_ts, created_at, updated_at
        FROM scheduler_rules
        ORDER BY priority ASC, rule_id ASC
        """
    )
    rows = cur.fetchall()
    con.close()
    return rows


def _scheduler_rule_update_runtime(rule_id: int, *, last_run_ts: float | None = None, next_run_ts: float | None = None) -> None:
    sets = []
    args: list[Any] = []
    if last_run_ts is not None:
        sets.append("last_run_ts = ?")
        args.append(float(last_run_ts))
    if next_run_ts is not None:
        sets.append("next_run_ts = ?")
        args.append(float(next_run_ts))
    if not sets:
        return
    args.append(int(rule_id))
    con = _state_connect(timeout=10)
    cur = con.cursor()
    cur.execute(
        f"UPDATE scheduler_rules SET {', '.join(sets)}, updated_at = ? WHERE rule_id = ?",
        (*args[:-1], time.time(), args[-1]),
    )
    con.commit()
    con.close()


def _scheduler_insert_default_rules_if_empty() -> None:
    now = time.time()
    con = _state_connect(timeout=10)
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM scheduler_rules")
    row = cur.fetchone()
    count = int((row["c"] if row else 0) or 0)
    if count > 0:
        con.close()
        return
    defaults = [
        # Changed-only scans are useful, but surprise background scans are confusing for most users.
        # Keep the stock rule present so it is easy to enable later from Settings, but default it to OFF.
        ("scan_changed", 0, "interval", 20, "", "", "new", 0, 10, 1),
        # Keep the stock weekly full-scan rule visible but OFF by default; surprise 02:00 full scans
        # are unacceptable on large libraries unless the user explicitly opts in.
        ("scan_full", 0, "weekly", None, "6", "02:00", "full", 0, 20, 1),
        # Post-scan chain templates. These are *not* periodic schedule jobs; the scheduler loop
        # ignores rules flagged `post_scan_chain=1` and they are used only as queue templates
        # after a successful scan. This keeps the default pipeline fast and predictable while
        # still enabling background enrichment/export by default.
        ("enrich_batch", 1, "interval", 30, "", "", "both", 1, 30, 1),
        ("dedupe", 1, "interval", 30, "", "", "both", 1, 35, 1),
        ("incomplete_move", 1, "interval", 30, "", "", "both", 1, 45, 1),
        ("export", 1, "interval", 30, "", "", "both", 1, 50, 1),
        ("player_sync", 1, "interval", 30, "", "", "both", 1, 60, 1),
    ]
    if bool(getattr(_runtime_module(), "SCHEDULER_ALLOW_NON_SCAN_JOBS", SCHEDULER_ALLOW_NON_SCAN_JOBS)):
        defaults.extend(
            [
                # Enrich in the background (continuous batches).
                ("enrich_batch", 1, "interval", 30, "", "", "both", 0, 30, 1),
                # Dedupe: chain for new changes + nightly full sweep.
                ("dedupe", 1, "interval", 24 * 60, "", "", "full", 0, 40, 1),
                ("dedupe", 1, "interval", 30, "", "", "new", 0, 35, 1),
                # Incomplete move on new changes.
                ("incomplete_move", 1, "interval", 30, "", "", "new", 0, 45, 1),
                # Export and sync in batches + chain after full scans.
                ("export", 1, "interval", 30, "", "", "new", 0, 50, 1),
                ("export", 1, "interval", 30, "", "", "full", 0, 50, 1),
                ("player_sync", 1, "interval", 30, "", "", "new", 0, 60, 1),
                ("player_sync", 1, "interval", 30, "", "", "full", 0, 60, 1),
            ]
        )
    for job_type, enabled, trigger_type, interval_min, days_of_week, time_local, scope, post_scan_chain, priority, max_concurrency in defaults:
        cur.execute(
            """
            INSERT INTO scheduler_rules
            (job_type, enabled, trigger_type, interval_min, days_of_week, time_local, scope, post_scan_chain, priority, max_concurrency, next_run_ts, last_run_ts, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)
            """,
            (
                job_type,
                int(enabled),
                trigger_type,
                interval_min,
                days_of_week,
                time_local,
                scope,
                int(post_scan_chain),
                int(priority),
                int(max_concurrency),
                now,
                now,
            ),
        )
    con.commit()
    con.close()


def _scheduler_migrate_legacy_scan_changed_default() -> None:
    """
    Older builds created an enabled stock `scan_changed` rule every 20 minutes.
    That surprised users because scans could restart without explicit action.
    Migrate that legacy stock rule to disabled once, while preserving any non-stock custom rules.
    """
    marker_key = "SCHEDULER_LEGACY_SCAN_CHANGED_DEFAULT_MIGRATED"
    try:
        if str(_get_config_from_db(marker_key, "") or "").strip() == "1":
            return
    except Exception:
        pass
    con = _state_connect(timeout=10)
    changed = 0
    try:
        cur = con.cursor()
        cur.execute(
            """
            SELECT rule_id, enabled
            FROM scheduler_rules
            WHERE job_type = 'scan_changed'
              AND trigger_type = 'interval'
              AND COALESCE(interval_min, 0) = 20
              AND COALESCE(days_of_week, '') = ''
              AND COALESCE(time_local, '') = ''
              AND COALESCE(scope, 'both') = 'new'
              AND COALESCE(post_scan_chain, 0) = 0
              AND COALESCE(priority, 0) = 10
              AND COALESCE(max_concurrency, 1) = 1
            """
        )
        rows = cur.fetchall()
        now = time.time()
        for row in rows:
            rule_id = int((row["rule_id"] if isinstance(row, sqlite3.Row) else row[0]) or 0)
            enabled = int((row["enabled"] if isinstance(row, sqlite3.Row) else row[1]) or 0)
            if rule_id <= 0 or enabled <= 0:
                continue
            cur.execute(
                "UPDATE scheduler_rules SET enabled = 0, updated_at = ? WHERE rule_id = ?",
                (now, rule_id),
            )
            changed += 1
        con.commit()
    finally:
        con.close()
    if changed > 0:
        logging.info(
            "Scheduler migration: disabled %d legacy auto scan_changed rule(s); users can re-enable them manually in Settings.",
            changed,
        )
    try:
        _settings_db_set_value(marker_key, "1")
    except Exception:
        pass


def _scheduler_migrate_legacy_scan_full_default() -> None:
    """
    Older builds also shipped an enabled stock weekly `scan_full` rule at 02:00 on Saturday.
    That can unexpectedly restart a heavy full scan on large libraries. Disable that stock rule
    once, while preserving any custom or edited weekly full-scan schedules.
    """
    marker_key = "SCHEDULER_LEGACY_SCAN_FULL_DEFAULT_MIGRATED"
    try:
        if str(_get_config_from_db(marker_key, "") or "").strip() == "1":
            return
    except Exception:
        pass
    con = _state_connect(timeout=10)
    changed = 0
    try:
        cur = con.cursor()
        cur.execute(
            """
            SELECT rule_id, enabled
            FROM scheduler_rules
            WHERE job_type = 'scan_full'
              AND trigger_type = 'weekly'
              AND COALESCE(interval_min, 0) = 0
              AND COALESCE(days_of_week, '') = '6'
              AND COALESCE(time_local, '') = '02:00'
              AND COALESCE(scope, 'full') = 'full'
              AND COALESCE(post_scan_chain, 0) = 0
              AND COALESCE(priority, 0) = 20
              AND COALESCE(max_concurrency, 1) = 1
            """
        )
        rows = cur.fetchall()
        now = time.time()
        for row in rows:
            rule_id = int((row["rule_id"] if isinstance(row, sqlite3.Row) else row[0]) or 0)
            enabled = int((row["enabled"] if isinstance(row, sqlite3.Row) else row[1]) or 0)
            if rule_id <= 0 or enabled <= 0:
                continue
            cur.execute(
                "UPDATE scheduler_rules SET enabled = 0, updated_at = ? WHERE rule_id = ?",
                (now, rule_id),
            )
            changed += 1
        con.commit()
    finally:
        con.close()
    if changed > 0:
        logging.info(
            "Scheduler migration: disabled %d legacy auto scan_full rule(s); users can re-enable them manually in Settings.",
            changed,
        )
    try:
        _settings_db_set_value(marker_key, "1")
    except Exception:
        pass


def _scheduler_ensure_post_scan_chain_defaults() -> None:
    """
    Ensure chain-only scheduler templates exist even when periodic non-scan jobs are disabled.
    This lets the default pipeline finish scans quickly and continue enrichment/export in the
    background without enabling surprise scheduled jobs.
    """
    defaults = [
        ("enrich_batch", "both", 30, 1),
        ("dedupe", "both", 35, 1),
        ("incomplete_move", "both", 45, 1),
        ("export", "both", 50, 1),
        ("player_sync", "both", 60, 1),
    ]
    now = time.time()
    con = _state_connect(timeout=10)
    inserted = 0
    try:
        cur = con.cursor()
        for job_type, scope, priority, max_concurrency in defaults:
            cur.execute(
                """
                SELECT rule_id
                FROM scheduler_rules
                WHERE enabled = 1
                  AND post_scan_chain = 1
                  AND job_type = ?
                  AND COALESCE(scope, 'both') = ?
                LIMIT 1
                """,
                (job_type, scope),
            )
            if cur.fetchone():
                continue
            cur.execute(
                """
                INSERT INTO scheduler_rules
                (job_type, enabled, trigger_type, interval_min, days_of_week, time_local, scope, post_scan_chain, priority, max_concurrency, next_run_ts, last_run_ts, created_at, updated_at)
                VALUES (?, 1, 'interval', 30, '', '', ?, 1, ?, ?, NULL, NULL, ?, ?)
                """,
                (job_type, scope, int(priority), int(max_concurrency), now, now),
            )
            inserted += 1
        con.commit()
    finally:
        con.close()
    if inserted > 0:
        logging.info(
            "Scheduler defaults: inserted %d post-scan chain template rule(s) for async pipeline jobs.",
            inserted,
        )


def _pipeline_migrate_legacy_post_scan_async_default() -> None:
    """
    Older installs could persist PIPELINE_POST_SCAN_ASYNC=False from the slow synchronous
    pipeline default. Migrate that legacy default once so scans finish fast by default.
    Users can still disable it manually afterwards.
    """
    marker_key = "PIPELINE_POST_SCAN_ASYNC_DEFAULT_MIGRATED"
    raw_value = None
    marker_value = ""
    try:
        init_settings_db()
        con = sqlite3.connect(str(SETTINGS_DB_FILE), timeout=5)
        cur = con.cursor()
        cur.execute(
            "SELECT key, value FROM settings WHERE key IN (?, ?)",
            (marker_key, "PIPELINE_POST_SCAN_ASYNC"),
        )
        for row in cur.fetchall():
            key = str(row[0] or "").strip()
            value = row[1]
            if key == marker_key:
                marker_value = str(value or "").strip()
            elif key == "PIPELINE_POST_SCAN_ASYNC":
                raw_value = value
        con.close()
    except Exception:
        marker_value = ""
        raw_value = raw_value
    if marker_value == "1":
        return
    changed = False
    if raw_value is not None and not bool(_parse_bool(raw_value)):
        try:
            _settings_db_set_value("PIPELINE_POST_SCAN_ASYNC", "true")
            changed = True
        except Exception:
            changed = False
    try:
        _settings_db_set_value(marker_key, "1")
    except Exception:
        pass
    if changed:
        try:
            global PIPELINE_POST_SCAN_ASYNC
            PIPELINE_POST_SCAN_ASYNC = True
            _runtime_module().merged["PIPELINE_POST_SCAN_ASYNC"] = True
        except Exception:
            pass
        logging.info(
            "Settings migration: enabled PIPELINE_POST_SCAN_ASYNC by default for legacy installs; users can disable it manually in Settings."
        )


def _library_migrate_legacy_include_unmatched_default() -> None:
    """
    Older installs could persist LIBRARY_INCLUDE_UNMATCHED=False from an earlier
    Files browse default, which hides legitimate local albums from the library.
    Migrate that legacy default once so users see their library immediately
    while PMDA continues matching/enriching in the background.
    """
    marker_key = "LIBRARY_INCLUDE_UNMATCHED_DEFAULT_MIGRATED"
    raw_value = None
    marker_value = ""
    try:
        init_settings_db()
        con = sqlite3.connect(str(SETTINGS_DB_FILE), timeout=5)
        cur = con.cursor()
        cur.execute(
            "SELECT key, value FROM settings WHERE key IN (?, ?)",
            (marker_key, "LIBRARY_INCLUDE_UNMATCHED"),
        )
        for row in cur.fetchall():
            key = str(row[0] or "").strip()
            value = row[1]
            if key == marker_key:
                marker_value = str(value or "").strip()
            elif key == "LIBRARY_INCLUDE_UNMATCHED":
                raw_value = value
        con.close()
    except Exception:
        marker_value = ""
        raw_value = raw_value
    if marker_value == "1":
        return
    changed = False
    if raw_value is not None and not bool(_parse_bool(raw_value)):
        try:
            _settings_db_set_value("LIBRARY_INCLUDE_UNMATCHED", "true")
            changed = True
        except Exception:
            changed = False
    try:
        _settings_db_set_value(marker_key, "1")
    except Exception:
        pass
    if changed:
        try:
            global LIBRARY_INCLUDE_UNMATCHED
            LIBRARY_INCLUDE_UNMATCHED = True
            _runtime_module().merged["LIBRARY_INCLUDE_UNMATCHED"] = True
        except Exception:
            pass
        logging.info(
            "Settings migration: enabled LIBRARY_INCLUDE_UNMATCHED by default for legacy installs; users can disable it manually in Settings."
        )


def _provider_gateway_migrate_legacy_discogs_rpm_default() -> None:
    """
    Older installs persisted Discogs RPM at 55, which is too aggressive for long-running artist
    backfills and often trips 429s. Migrate that legacy stock default once to 40 while preserving
    any user-customized values.
    """
    marker_key = "PROVIDER_GATEWAY_DISCOGS_RPM_DEFAULT_MIGRATED"
    raw_value = None
    marker_value = ""
    try:
        init_settings_db()
        con = sqlite3.connect(str(SETTINGS_DB_FILE), timeout=5)
        cur = con.cursor()
        cur.execute(
            "SELECT key, value FROM settings WHERE key IN (?, ?)",
            (marker_key, "PROVIDER_GATEWAY_DISCOGS_RPM"),
        )
        for row in cur.fetchall():
            key = str(row[0] or "").strip()
            value = row[1]
            if key == marker_key:
                marker_value = str(value or "").strip()
            elif key == "PROVIDER_GATEWAY_DISCOGS_RPM":
                raw_value = value
        con.close()
    except Exception:
        marker_value = ""
    if marker_value == "1":
        return
    changed = False
    try:
        parsed = int(raw_value) if raw_value not in (None, "") else 0
    except Exception:
        parsed = 0
    if parsed in {0, 55}:
        try:
            _settings_db_set_value("PROVIDER_GATEWAY_DISCOGS_RPM", "40")
            changed = True
        except Exception:
            changed = False
    try:
        _settings_db_set_value(marker_key, "1")
    except Exception:
        pass
    if changed:
        try:
            global PROVIDER_GATEWAY_DISCOGS_RPM
            PROVIDER_GATEWAY_DISCOGS_RPM = 40
            _runtime_module().merged["PROVIDER_GATEWAY_DISCOGS_RPM"] = 40
        except Exception:
            pass
        logging.info(
            "Settings migration: lowered legacy PROVIDER_GATEWAY_DISCOGS_RPM default from 55 to 40."
        )


def _web_search_migrate_legacy_provider_default() -> None:
    """
    Remove legacy `searxng` provider values from older installs now that the runtime
    only supports auto/serper/ollama/ai_only/disabled.
    """
    marker_key = "WEB_SEARCH_PROVIDER_LEGACY_MIGRATED"
    raw_value = None
    marker_value = ""
    try:
        init_settings_db()
        con = sqlite3.connect(str(SETTINGS_DB_FILE), timeout=5)
        cur = con.cursor()
        cur.execute(
            "SELECT key, value FROM settings WHERE key IN (?, ?)",
            (marker_key, "WEB_SEARCH_PROVIDER"),
        )
        for row in cur.fetchall():
            key = str(row[0] or "").strip()
            value = row[1]
            if key == marker_key:
                marker_value = str(value or "").strip()
            elif key == "WEB_SEARCH_PROVIDER":
                raw_value = value
        con.close()
    except Exception:
        marker_value = ""
    if marker_value == "1":
        return
    changed = False
    raw_provider = str(raw_value or "").strip()
    normalized_provider = _normalize_web_search_provider(raw_provider)
    if raw_provider and raw_provider.lower() != normalized_provider:
        try:
            _settings_db_set_value("WEB_SEARCH_PROVIDER", normalized_provider)
            changed = True
        except Exception:
            changed = False
    try:
        _settings_db_set_value(marker_key, "1")
    except Exception:
        pass
    if changed:
        try:
            global WEB_SEARCH_PROVIDER
            WEB_SEARCH_PROVIDER = normalized_provider
            _runtime_module().merged["WEB_SEARCH_PROVIDER"] = normalized_provider
        except Exception:
            pass
        logging.info(
            "Settings migration: normalized legacy WEB_SEARCH_PROVIDER=%r to %r.",
            raw_provider,
            normalized_provider,
        )


def _scheduler_job_insert(
    run_id: str,
    *,
    rule_id: int | None,
    job_type: str,
    scope: str,
    source: str,
    origin_scan_id: int | None = None,
) -> None:
    con = _state_connect(timeout=15)
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO scheduler_jobs
        (job_run_id, rule_id, job_type, scope, source, status, message, origin_scan_id, created_at)
        VALUES (?, ?, ?, ?, ?, 'queued', '', ?, ?)
        """,
        (run_id, rule_id, job_type, scope, source, _int_or_none(origin_scan_id), time.time()),
    )
    if origin_scan_id is not None:
        try:
            _ai_refresh_rollup_for_scan(cur, int(origin_scan_id))
        except Exception:
            logging.debug("Failed to refresh AI rollup after scheduler job insert", exc_info=True)
    con.commit()
    con.close()


def _scheduler_job_update(
    run_id: str,
    *,
    status: str,
    message: str = "",
    metrics: dict | None = None,
    error: str = "",
    started_at: float | None = None,
    ended_at: float | None = None,
) -> None:
    status_norm = str(status or "").strip().lower()
    if status_norm not in {"queued", "running", "completed", "failed", "skipped"}:
        status_norm = "failed"
    con = _state_connect(timeout=15)
    cur = con.cursor()
    duration_ms = None
    if started_at is not None and ended_at is not None:
        duration_ms = int(max(0.0, (ended_at - started_at) * 1000.0))
    cur.execute(
        """
        UPDATE scheduler_jobs
        SET status = ?, message = ?, metrics_json = ?, error = ?,
            started_at = COALESCE(?, started_at),
            ended_at = COALESCE(?, ended_at),
            duration_ms = COALESCE(?, duration_ms)
        WHERE job_run_id = ?
        """,
        (
            status_norm,
            str(message or ""),
            _json_dumps_safe(metrics or {}),
            str(error or ""),
            started_at,
            ended_at,
            duration_ms,
            run_id,
        ),
    )
    con.commit()
    con.close()


def _scheduler_record_skipped_job(
    *,
    rule_id: int | None,
    job_type: str,
    scope: str,
    source: str,
    reason: str,
    origin_scan_id: int | None = None,
) -> str:
    run_id = str(uuid.uuid4())
    now = time.time()
    _scheduler_job_insert(
        run_id,
        rule_id=rule_id,
        job_type=job_type,
        scope=scope,
        source=source,
        origin_scan_id=_int_or_none(origin_scan_id),
    )
    _scheduler_job_update(
        run_id,
        status="skipped",
        message=str(reason or "skipped"),
        started_at=now,
        ended_at=now,
    )
    return run_id


def _scheduler_get_latest_scan_entry(min_scan_id: int | None = None) -> dict | None:
    con = _state_connect(timeout=10)
    cur = con.cursor()
    if min_scan_id is not None:
        cur.execute(
            """
            SELECT scan_id, status, summary_json, start_time, end_time
            FROM scan_history
            WHERE COALESCE(entry_type, 'scan') = 'scan' AND scan_id > ?
            ORDER BY scan_id DESC
            LIMIT 1
            """,
            (int(min_scan_id),),
        )
    else:
        cur.execute(
            """
            SELECT scan_id, status, summary_json, start_time, end_time
            FROM scan_history
            WHERE COALESCE(entry_type, 'scan') = 'scan'
            ORDER BY scan_id DESC
            LIMIT 1
            """
        )
    row = cur.fetchone()
    con.close()
    if not row:
        return None
    summary = {}
    try:
        summary = json.loads(row["summary_json"] or "{}")
    except Exception:
        summary = {}
    return {
        "scan_id": int(row["scan_id"] or 0),
        "status": str(row["status"] or ""),
        "summary": summary,
        "start_time": row["start_time"],
        "end_time": row["end_time"],
    }


def _scheduler_job_key(job_type: str, scope: str) -> str:
    return f"{_normalize_task_job_type(job_type)}:{_normalize_task_scope(scope)}"


def _scheduler_pool_for_job(job_type: str, source: str) -> str:
    jt = _normalize_task_job_type(job_type)
    src = str(source or "").strip().lower()
    if jt in {"scan_changed", "scan_full"}:
        return "scan"
    if src == "post_scan_chain":
        return "post_scan"
    if jt in {"enrich_batch", "player_sync"}:
        return "network"
    if jt in {"dedupe", "incomplete_move", "export", "managed_musicbrainz_update"}:
        return "io"
    return "io"


def _scheduler_pool_limit(pool_name: str) -> int:
    p = str(pool_name or "").strip().lower()
    if p == "scan":
        return int(SCHEDULER_POOL_LIMIT_SCAN)
    if p == "network":
        return int(SCHEDULER_POOL_LIMIT_NETWORK)
    if p == "post_scan":
        return int(SCHEDULER_POOL_LIMIT_POST_SCAN)
    return int(SCHEDULER_POOL_LIMIT_IO)


def _scheduler_can_start_job(job_type: str, scope: str, *, source: str = "", max_concurrency: int = 1) -> tuple[bool, str]:
    jt = _normalize_task_job_type(job_type)
    if not jt:
        return False, "Invalid job type"
    source_norm = str(source or "").strip().lower()
    if (
        jt in {"enrich_batch", "dedupe", "incomplete_move", "export", "player_sync"}
        and source_norm != "post_scan_chain"
        and not bool(getattr(_runtime_module(), "SCHEDULER_ALLOW_NON_SCAN_JOBS", SCHEDULER_ALLOW_NON_SCAN_JOBS))
    ):
        return False, "Non-scan scheduler jobs are disabled"
    if library_is_audit_mode() and jt in {"dedupe", "incomplete_move", "export", "player_sync"}:
        return False, "Audit mode: automatic writes are disabled"
    sc = _normalize_task_scope(scope, default="both")
    key = _scheduler_job_key(jt, sc)
    pool_name = _scheduler_pool_for_job(jt, source)
    pool_limit = max(1, int(_scheduler_pool_limit(pool_name)))
    max_per_job = max(1, int(max_concurrency or 1))
    with _scheduler_lock:
        if key in _scheduler_running_keys:
            return False, "Same job/scope already running"
        running_for_job = sum(
            1
            for meta in _scheduler_running_meta.values()
            if _normalize_task_job_type(meta.get("job_type")) == jt
        )
        if running_for_job >= max_per_job:
            return False, f"Max concurrency reached for {jt}"
        running_in_pool = sum(
            1
            for meta in _scheduler_running_meta.values()
            if _scheduler_pool_for_job(meta.get("job_type") or "", meta.get("source") or "") == pool_name
        )
        if running_in_pool >= pool_limit:
            return False, f"Scheduler {pool_name} pool is busy"
    with lock:
        scanning_now = bool(state.get("scanning"))
        finalizing_now = bool(state.get("scan_finalizing"))
        starting_now = bool(state.get("scan_starting"))
        deduping_now = bool(state.get("deduping"))
        improve_all_now = bool((state.get("improve_all") or {}).get("running"))
        export_now = bool((state.get("export_progress") or {}).get("running"))
        incomplete_now = bool((state.get("incomplete_scan") or {}).get("running"))
    if jt in {"scan_changed", "scan_full"} and (scanning_now or finalizing_now or starting_now):
        return False, "Scan already running"
    if jt in {"enrich_batch", "dedupe", "incomplete_move", "export", "player_sync"} and (scanning_now or finalizing_now or starting_now):
        return False, "Scan/finalization in progress"
    if jt == "enrich_batch" and _get_library_mode() == "files" and _files_index_is_running():
        return False, "Files library rebuild in progress"
    if jt == "dedupe" and deduping_now:
        return False, "Deduplication already running"
    if jt == "enrich_batch" and improve_all_now:
        return False, "Improve-all already running"
    if jt == "export" and export_now:
        return False, "Export already running"
    if jt == "incomplete_move" and incomplete_now:
        return False, "Incomplete scan already running"
    return True, ""


def _scheduler_start_scan(scan_type: str, source: str, *, run_id: str | None = None) -> tuple[bool, str]:
    r = _requires_config()
    if r is not None:
        return False, "Missing source folders configuration"
    # Keep scheduler-triggered scan starts non-blocking; AI reinit happens in background_scan().
    scan_should_stop.clear()
    scan_is_paused.clear()
    ok, meta = _try_begin_scan(
        scan_type=scan_type,
        source=source,
        run_improve_after=False,
        scheduler_run_id=str(run_id or "").strip() or None,
    )
    if not ok:
        reason = str(meta.get("reason") or "").strip().lower()
        if reason == "scan_already_running":
            return False, "Scan already running"
        return False, str(reason or "Scan already running")
    return True, f"{scan_type} scan started"


def _scheduler_wait_for_scan_completion(max_wait_sec: int = 48 * 3600) -> tuple[bool, str, dict]:
    started = time.time()
    while True:
        with lock:
            scanning_now = bool(state.get("scanning"))
            finalizing_now = bool(state.get("scan_finalizing"))
            starting_now = bool(state.get("scan_starting"))
        if not scanning_now and not finalizing_now and not starting_now:
            break
        if (time.time() - started) > max_wait_sec:
            return False, "Timed out waiting for scan completion", {}
        time.sleep(2.0)
    last_scan = _scheduler_get_latest_scan_entry()
    if not last_scan:
        return False, "Scan completed but no history row was found", {}
    status = str(last_scan.get("status") or "").strip().lower()
    if status != "completed":
        return False, f"Scan ended with status: {status or 'unknown'}", dict(last_scan.get("summary") or {})
    return True, "Scan finished", dict(last_scan.get("summary") or {})


def _scheduler_build_improve_candidates() -> list[dict]:
    def _to_clean_text(value) -> str:
        if value is None:
            return ""
        try:
            return str(value).strip()
        except Exception:
            return ""

    best_albums: list[dict] = []
    seen_ids: set[int] = set()
    with lock:
        if not state.get("duplicates"):
            state["duplicates"] = load_scan_from_db()
        groups_map = dict(state.get("duplicates") or {})
    for artist_name, groups in groups_map.items():
        for g in (groups or []):
            best = g.get("best") or {}
            album_id = int(best.get("album_id") or 0)
            if not album_id or album_id in seen_ids:
                continue
            seen_ids.add(album_id)
            best_albums.append(
                {
                    "artist": artist_name,
                    "album_id": album_id,
                    "album_title": best.get("title_raw") or best.get("album_norm") or f"Album {album_id}",
                    "musicbrainz_id": best.get("musicbrainz_id"),
                    "folder": _to_clean_text(best.get("folder")),
                    "strict_match_verified": bool(best.get("strict_match_verified")),
                    "strict_match_provider": best.get("strict_match_provider") or "",
                    "strict_reject_reason": best.get("strict_reject_reason") or "",
                    "strict_tracklist_score": float(best.get("strict_tracklist_score") or 0.0),
                }
            )
    scan_id = get_last_completed_scan_id()
    if scan_id is not None:
        con = _state_connect(timeout=10)
        cur = con.cursor()
        try:
            cur.execute(
                """
                SELECT artist, album_id, title_raw, musicbrainz_id, folder,
                       strict_match_verified, strict_match_provider, strict_reject_reason, strict_tracklist_score
                FROM scan_editions
                WHERE scan_id = ?
                """,
                (scan_id,),
            )
            for row in cur.fetchall():
                album_id = int(row["album_id"] or 0)
                if not album_id or album_id in seen_ids:
                    continue
                seen_ids.add(album_id)
                best_albums.append(
                    {
                        "artist": row["artist"],
                        "album_id": album_id,
                        "album_title": (row["title_raw"] or "").strip() or f"Album {album_id}",
                        "musicbrainz_id": (row["musicbrainz_id"] or "").strip(),
                        "folder": (row["folder"] or "").strip(),
                        "strict_match_verified": bool(row["strict_match_verified"]),
                        "strict_match_provider": str(row["strict_match_provider"] or "").strip(),
                        "strict_reject_reason": str(row["strict_reject_reason"] or "").strip(),
                        "strict_tracklist_score": float(row["strict_tracklist_score"] or 0.0),
                    }
                )
        finally:
            con.close()
    return best_albums


def _scheduler_run_enrich_batch() -> tuple[bool, str, dict]:
    before = time.time()
    result: dict[str, Any] = {}
    albums: list[dict] = []
    if _get_library_mode() == "files":
        logging.info("[Scan Pipeline] enrich_batch: Files mode skips legacy improve-all; running background profile/review backfill only")
    else:
        albums = _scheduler_build_improve_candidates()
        if albums:
            _run_improve_all_albums_global(albums)
            with lock:
                improve_state = dict(state.get("improve_all") or {})
            result = improve_state.get("result") if isinstance(improve_state.get("result"), dict) else {}
            err = str(improve_state.get("error") or "").strip()
            if err:
                return False, err, {"albums": len(albums), "duration_s": round(time.time() - before, 2), "result": result or {}}
        else:
            logging.info("[Scan Pipeline] enrich_batch: no album-level improve candidates; continuing with profile backfill only")

    profile_metrics: dict[str, Any] = {}
    if _get_library_mode() == "files":
        logging.info("[Scan Pipeline] enrich_batch: queueing background profile backfill")
        spawned = _trigger_files_profile_backfill_async(reason="scheduler_enrich_batch")
        with _files_profile_backfill_lock:
            profile_state = dict(_files_profile_backfill_state or {})
        profile_metrics = {
            "queued": bool(spawned),
            "artists_total": int(profile_state.get("total") or 0),
            "artists_done": int(profile_state.get("current") or 0),
            "errors": int(profile_state.get("errors") or 0),
            "finished_at": profile_state.get("finished_at"),
        }
        logging.info(
            "[Scan Pipeline] enrich_batch: profile backfill queued=%s artists=%d/%d errors=%d",
            bool(spawned),
            int(profile_metrics.get("artists_done") or 0),
            int(profile_metrics.get("artists_total") or 0),
            int(profile_metrics.get("errors") or 0),
        )

    return True, "Enrichment batch finished", {
        "albums": len(albums),
        "duration_s": round(time.time() - before, 2),
        "result": result or {},
        "profiles": profile_metrics,
    }


def _scheduler_run_dedupe() -> tuple[bool, str, dict]:
    scan_results = load_scan_from_db()
    flat_groups = [g for groups in (scan_results or {}).values() for g in (groups or [])]
    if not flat_groups:
        return True, "No duplicate group to dedupe", {"groups": 0, "moved": 0, "space_saved_mb": 0}
    moved_before = int(get_stat("removed_dupes") or 0)
    saved_before = int(get_stat("space_saved") or 0)
    background_dedupe(flat_groups)
    moved_delta = max(0, int(get_stat("removed_dupes") or 0) - moved_before)
    saved_delta = max(0, int(get_stat("space_saved") or 0) - saved_before)
    return True, "Deduplication finished", {"groups": len(flat_groups), "moved": moved_delta, "space_saved_mb": saved_delta}


def _scheduler_run_incomplete_move() -> tuple[bool, str, dict]:
    if _get_library_mode() == "files":
        return True, "Incomplete-move scheduler skipped in Files mode (handled by the scan pipeline)", {"moved": 0, "run_id": None}
    _run_incomplete_albums_scan()
    with lock:
        inc = dict(state.get("incomplete_scan") or {})
    run_id = inc.get("run_id")
    err = str(inc.get("error") or "").strip()
    if err:
        return False, err, {"moved": 0, "run_id": run_id}
    if not run_id:
        return True, "Incomplete scan finished (no run id)", {"moved": 0}
    con = _state_connect(timeout=10)
    cur = con.cursor()
    cur.execute("SELECT artist, album_id, title_raw FROM incomplete_album_diagnostics WHERE run_id = ? ORDER BY artist, album_id", (int(run_id),))
    rows = cur.fetchall()
    con.close()
    if not rows:
        return True, "No incomplete album found", {"moved": 0, "run_id": run_id}
    payload = {"run_id": int(run_id), "items": [{"artist": r["artist"], "album_id": int(r["album_id"]), "title_raw": r["title_raw"] or ""} for r in rows]}
    with app.test_request_context("/api/incomplete-albums/move", method="POST", json=payload):
        resp = api_incomplete_albums_move()
    if isinstance(resp, tuple):
        response_obj, status_code = resp
    else:
        response_obj, status_code = resp, 200
    if int(status_code) >= 400:
        try:
            data = response_obj.get_json() if hasattr(response_obj, "get_json") else {}
        except Exception:
            data = {}
        return False, str((data or {}).get("error") or "Failed to move incomplete albums"), {"run_id": run_id}
    data = response_obj.get_json() if hasattr(response_obj, "get_json") else {}
    moved = len((data or {}).get("moved") or [])
    return True, "Incomplete-move finished", {"run_id": run_id, "moved": moved}


def _scheduler_run_export() -> tuple[bool, str, dict]:
    if _get_library_mode() != "files":
        return True, "Export skipped (not in Files mode)", {"running": False}
    _run_export_library()
    with lock:
        prog = dict(state.get("export_progress") or {})
    err = str(prog.get("error") or "").strip()
    if err:
        return False, err, {"tracks_done": int(prog.get("tracks_done") or 0), "total_tracks": int(prog.get("total_tracks") or 0)}
    return True, "Export finished", {"tracks_done": int(prog.get("tracks_done") or 0), "total_tracks": int(prog.get("total_tracks") or 0)}


def _scheduler_run_player_sync() -> tuple[bool, str, dict]:
    target = _normalize_player_target(getattr(_runtime_module(), "PIPELINE_PLAYER_TARGET", "none"))
    if target == "none":
        return True, "Player sync skipped (no target)", {"target": target}
    ok, msg = _trigger_player_refresh_by_target(target)
    if ok:
        return True, msg or "Player sync finished", {"target": target, "ok": True}
    return False, msg or "Player sync failed", {"target": target, "ok": False}


def _scheduler_execute_job(job_type: str, scope: str, source: str, *, run_id: str | None = None) -> tuple[bool, str, dict]:
    jt = _normalize_task_job_type(job_type)
    if not jt:
        return False, "Invalid job type", {}
    if jt == "scan_changed":
        ok, msg = _scheduler_start_scan("changed_only", source, run_id=run_id)
        if not ok:
            return False, msg, {}
        return _scheduler_wait_for_scan_completion()
    if jt == "scan_full":
        ok, msg = _scheduler_start_scan("full", source, run_id=run_id)
        if not ok:
            return False, msg, {}
        return _scheduler_wait_for_scan_completion()
    if jt == "enrich_batch":
        return _scheduler_run_enrich_batch()
    if jt == "dedupe":
        return _scheduler_run_dedupe()
    if jt == "incomplete_move":
        return _scheduler_run_incomplete_move()
    if jt == "export":
        return _scheduler_run_export()
    if jt == "player_sync":
        return _scheduler_run_player_sync()
    if jt == "managed_musicbrainz_update":
        return _managed_runtime_run_musicbrainz_update()
    return False, f"Unhandled job type: {jt}", {}


def _scheduler_worker(
    run_id: str,
    *,
    rule_id: int | None,
    job_type: str,
    scope: str,
    source: str,
    origin_scan_id: int | None = None,
) -> None:
    started = time.time()
    key = _scheduler_job_key(job_type, scope)
    event_id = 0
    prev_ai_ctx = _ai_usage_context_push(
        run_id=run_id,
        scheduler_job_id=run_id,
        source=source,
        job_type=job_type,
        scope=scope,
        origin_scan_id=_int_or_none(origin_scan_id),
    )
    try:
        with _scheduler_lock:
            _scheduler_running_keys.add(key)
            _scheduler_running_meta[run_id] = {
                "run_id": run_id,
                "rule_id": rule_id,
                "job_type": job_type,
                "scope": scope,
                "source": source,
                "origin_scan_id": _int_or_none(origin_scan_id),
                "started_at": started,
            }
        logging.info(
            "[Scheduler] start run=%s job=%s scope=%s source=%s origin_scan_id=%s",
            run_id,
            job_type,
            scope,
            source,
            _int_or_none(origin_scan_id),
        )
        _scheduler_job_update(run_id, status="running", started_at=started, message=f"{job_type} started")
        event_id = _task_event_start(
            run_id=run_id,
            job_type=job_type,
            scope=scope,
            source=source,
            message=f"{job_type} started",
        )
        ok, msg, metrics = _scheduler_execute_job(job_type, scope, source, run_id=run_id)
        ended = time.time()
        if ok:
            logging.info(
                "[Scheduler] done run=%s job=%s scope=%s source=%s elapsed=%.2fs message=%s metrics=%s",
                run_id,
                job_type,
                scope,
                source,
                max(0.0, ended - started),
                _log_preview_text(msg, 160),
                _log_preview_text(json.dumps(metrics or {}, ensure_ascii=True), 220),
            )
            _scheduler_job_update(
                run_id,
                status="completed",
                message=msg,
                metrics=metrics,
                started_at=started,
                ended_at=ended,
            )
            if event_id:
                _task_event_finish(
                    event_id,
                    status="completed",
                    message=msg,
                    metrics=metrics,
                    summary=metrics,
                )
        else:
            logging.warning(
                "[Scheduler] failed run=%s job=%s scope=%s source=%s elapsed=%.2fs message=%s metrics=%s",
                run_id,
                job_type,
                scope,
                source,
                max(0.0, ended - started),
                _log_preview_text(msg, 160),
                _log_preview_text(json.dumps(metrics or {}, ensure_ascii=True), 220),
            )
            _scheduler_job_update(
                run_id,
                status="failed",
                message=msg,
                metrics=metrics,
                error=msg,
                started_at=started,
                ended_at=ended,
            )
            if event_id:
                _task_event_finish(
                    event_id,
                    status="failed",
                    message=msg,
                    metrics=metrics,
                    summary=metrics,
                    error=msg,
                )
    except Exception as e:
        logging.exception("Scheduler job %s failed unexpectedly", run_id)
        ended = time.time()
        _scheduler_job_update(
            run_id,
            status="failed",
            message=str(e),
            error=str(e),
            started_at=started,
            ended_at=ended,
        )
        if event_id:
            _task_event_finish(
                event_id,
                status="failed",
                message=str(e),
                error=str(e),
            )
    finally:
        _ai_usage_context_restore(prev_ai_ctx)
        with _scheduler_lock:
            _scheduler_running_keys.discard(key)
            _scheduler_running_meta.pop(run_id, None)
        if rule_id is not None:
            now = time.time()
            try:
                con = _state_connect(timeout=10)
                cur = con.cursor()
                cur.execute("SELECT * FROM scheduler_rules WHERE rule_id = ?", (int(rule_id),))
                row = cur.fetchone()
                con.close()
                if row:
                    next_ts = _scheduler_compute_next_run(row, now_ts=now)
                    _scheduler_rule_update_runtime(int(rule_id), last_run_ts=now, next_run_ts=next_ts)
            except Exception:
                logging.debug("Failed to update scheduler rule runtime after run", exc_info=True)
        if origin_scan_id is not None:
            try:
                con = _state_connect(timeout=10)
                cur = con.cursor()
                _ai_refresh_rollup_for_scan(cur, int(origin_scan_id))
                con.commit()
                con.close()
            except Exception:
                logging.debug("Failed to refresh AI rollup after scheduler worker run", exc_info=True)


def _scheduler_launch_job(
    job_type: str,
    scope: str,
    source: str,
    *,
    rule_id: int | None = None,
    max_concurrency: int = 1,
    origin_scan_id: int | None = None,
) -> tuple[bool, str, str | None]:
    jt = _normalize_task_job_type(job_type)
    sc = _normalize_task_scope(scope, default="both")
    if not jt:
        return False, "Invalid job type", None
    allowed, reason = _scheduler_can_start_job(
        jt,
        sc,
        source=source,
        max_concurrency=max_concurrency,
    )
    if not allowed:
        return False, reason, None
    run_id = str(uuid.uuid4())
    _scheduler_job_insert(
        run_id,
        rule_id=rule_id,
        job_type=jt,
        scope=sc,
        source=source,
        origin_scan_id=_int_or_none(origin_scan_id),
    )
    th = threading.Thread(
        target=_scheduler_worker,
        args=(run_id,),
        kwargs={
            "rule_id": rule_id,
            "job_type": jt,
            "scope": sc,
            "source": source,
            "origin_scan_id": _int_or_none(origin_scan_id),
        },
        daemon=True,
        name=f"scheduler-{jt}-{sc}",
    )
    th.start()
    return True, "started", run_id


def _scheduler_is_enabled_rule_for_chain(job_type: str, scope: str) -> bool:
    jt = _normalize_task_job_type(job_type)
    sc = _normalize_task_scope(scope, default="both")
    if not jt:
        return False
    con = _state_connect(timeout=10)
    cur = con.cursor()
    cur.execute(
        """
        SELECT COUNT(*) AS c
        FROM scheduler_rules
        WHERE enabled = 1
          AND post_scan_chain = 1
          AND job_type = ?
        """,
        (jt,),
    )
    row = cur.fetchone()
    con.close()
    if not row or int(row["c"] or 0) <= 0:
        return False
    con = _state_connect(timeout=10)
    cur = con.cursor()
    cur.execute(
        """
        SELECT scope
        FROM scheduler_rules
        WHERE enabled = 1
          AND post_scan_chain = 1
          AND job_type = ?
        ORDER BY priority ASC, rule_id ASC
        """,
        (jt,),
    )
    scopes = [str(r["scope"] or "both").strip().lower() for r in cur.fetchall()]
    con.close()
    return any(_scheduler_rule_scope_matches(s, sc) for s in scopes)


def _scheduler_chain_max_concurrency(job_type: str, scope: str) -> int:
    jt = _normalize_task_job_type(job_type)
    sc = _normalize_task_scope(scope, default="both")
    if not jt:
        return 1
    con = _state_connect(timeout=10)
    cur = con.cursor()
    cur.execute(
        """
        SELECT scope, max_concurrency
        FROM scheduler_rules
        WHERE enabled = 1
          AND post_scan_chain = 1
          AND job_type = ?
        ORDER BY priority ASC, rule_id ASC
        """,
        (jt,),
    )
    rows = cur.fetchall()
    con.close()
    selected = [
        max(1, int(r["max_concurrency"] or 1))
        for r in rows
        if _scheduler_rule_scope_matches(str(r["scope"] or "both"), sc)
    ]
    if not selected:
        return 1
    return max(selected)


def _scheduler_loop() -> None:
    global _scheduler_paused
    logging.info("Scheduler loop started (poll=%ss).", SCHEDULER_POLL_SEC)
    while not _scheduler_stop_event.is_set():
        try:
            if _scheduler_paused:
                time.sleep(SCHEDULER_POLL_SEC)
                continue
            now = time.time()
            _managed_runtime_maybe_enqueue_due_jobs(now)
            _files_profile_backfill_maybe_start_idle(now_ts=now)
            rules = _scheduler_rules_fetch()
            for rule in rules:
                if not int(rule["enabled"] or 0):
                    continue
                if bool(int(rule["post_scan_chain"] or 0)):
                    # Chain rules are templates for async post-scan jobs, not periodic schedule jobs.
                    continue
                job_type = _normalize_task_job_type(rule["job_type"])
                scope = _normalize_task_scope(rule["scope"], default="both")
                if not job_type:
                    continue
                if job_type == "scan_changed" and bool(_pipeline_bootstrap_status().get("bootstrap_required")):
                    next_normal = _scheduler_compute_next_run(rule, now_ts=now)
                    _scheduler_rule_update_runtime(
                        int(rule["rule_id"]),
                        next_run_ts=next_normal,
                    )
                    continue
                next_run_raw = rule["next_run_ts"]
                next_run_ts = float(next_run_raw) if next_run_raw is not None else 0.0
                if next_run_ts <= 0.0:
                    next_run_ts = _scheduler_compute_next_run(rule, now_ts=now)
                    _scheduler_rule_update_runtime(int(rule["rule_id"]), next_run_ts=next_run_ts)
                    continue
                if now < next_run_ts:
                    continue
                ok, reason, _run_id = _scheduler_launch_job(
                    job_type,
                    scope,
                    "schedule",
                    rule_id=int(rule["rule_id"]),
                    max_concurrency=max(1, int(rule["max_concurrency"] or 1)),
                )
                if not ok:
                    reason_norm = str(reason or "").strip().lower()
                    skip_to_next = bool(
                        job_type in {"scan_changed", "scan_full"}
                        and reason_norm in {"same job/scope already running", "scan already running", "bootstrap_required"}
                    )
                    if skip_to_next:
                        next_normal = _scheduler_compute_next_run(rule, now_ts=now)
                        _scheduler_rule_update_runtime(
                            int(rule["rule_id"]),
                            next_run_ts=next_normal,
                        )
                        if "bootstrap_required" in reason_norm:
                            pass
                        else:
                            skipped_run = _scheduler_record_skipped_job(
                                rule_id=int(rule["rule_id"]),
                                job_type=job_type,
                                scope=scope,
                                source="schedule",
                                reason=str(reason or "Skipped"),
                            )
                            notice_key = (int(rule["rule_id"] or 0), reason_norm or "skip")
                            notice_until = float(_scheduler_skip_notice_until.get(notice_key, 0.0) or 0.0)
                            if now >= notice_until:
                                logging.debug(
                                    "Scheduler skipped %s/%s run=%s (%s); next run scheduled normally.",
                                    job_type,
                                    scope,
                                    skipped_run,
                                    reason,
                                )
                                _scheduler_skip_notice_until[notice_key] = now + 21600.0
                    else:
                        _scheduler_rule_update_runtime(
                            int(rule["rule_id"]),
                            next_run_ts=now + SCHEDULER_BUSY_RETRY_SEC,
                        )
                        logging.debug("Scheduler deferred %s/%s: %s", job_type, scope, reason)
        except Exception:
            logging.exception("Scheduler loop error")
        time.sleep(SCHEDULER_POLL_SEC)
    logging.info("Scheduler loop stopped.")


def _start_scheduler_if_needed() -> None:
    global _scheduler_thread, _scheduler_paused
    _scheduler_insert_default_rules_if_empty()
    _scheduler_migrate_legacy_scan_changed_default()
    _scheduler_migrate_legacy_scan_full_default()
    _scheduler_ensure_post_scan_chain_defaults()
    _pipeline_migrate_legacy_post_scan_async_default()
    _library_migrate_legacy_include_unmatched_default()
    _provider_gateway_migrate_legacy_discogs_rpm_default()
    _web_search_migrate_legacy_provider_default()
    _scheduler_paused = _scheduler_get_paused_from_db()
    with _scheduler_lock:
        if _scheduler_thread is not None and _scheduler_thread.is_alive():
            return
        _scheduler_stop_event.clear()
        t = threading.Thread(target=_scheduler_loop, daemon=True, name="scheduler-loop")
        _scheduler_thread = t
        t.start()


def _stop_scheduler() -> None:
    _scheduler_stop_event.set()
    with _scheduler_lock:
        t = _scheduler_thread
    if t is not None and t.is_alive():
        t.join(timeout=5.0)


def _scheduler_chain_post_scan(
    scan_type: str,
    *,
    origin_scan_id: int | None = None,
    include_enrich: bool,
    enabled_jobs: set[str] | None = None,
) -> None:
    scope = "new" if str(scan_type or "").strip().lower() == "changed_only" else "full"
    enabled = {str(j or "").strip().lower() for j in (enabled_jobs or set()) if str(j or "").strip()}
    chain_jobs = ["incomplete_move", "dedupe", "export", "player_sync"]
    if include_enrich:
        chain_jobs.insert(0, "enrich_batch")
    for job_type in chain_jobs:
        if enabled and job_type not in enabled:
            continue
        if not _scheduler_is_enabled_rule_for_chain(job_type, scope):
            continue
        max_concurrency = _scheduler_chain_max_concurrency(job_type, scope)
        ok, reason, run_id = _scheduler_launch_job(
            job_type,
            scope,
            "post_scan_chain",
            rule_id=None,
            max_concurrency=max_concurrency,
            origin_scan_id=_int_or_none(origin_scan_id),
        )
        if ok:
            logging.info("Post-scan chain queued %s (%s) run=%s", job_type, scope, run_id)
        else:
            logging.info("Post-scan chain skipped %s (%s): %s", job_type, scope, reason)


def _scheduler_rule_to_dict(row: sqlite3.Row | dict) -> dict:
    rule = row if isinstance(row, dict) else dict(row)
    return {
        "rule_id": int(rule.get("rule_id") or 0),
        "job_type": _normalize_task_job_type(rule.get("job_type")),
        "enabled": bool(int(rule.get("enabled") or 0)),
        "trigger_type": _normalize_scheduler_trigger(rule.get("trigger_type")),
        "interval_min": int(rule.get("interval_min") or 0) if rule.get("interval_min") is not None else None,
        "days_of_week": str(rule.get("days_of_week") or ""),
        "time_local": str(rule.get("time_local") or ""),
        "scope": _normalize_task_scope(rule.get("scope"), default="both"),
        "post_scan_chain": bool(int(rule.get("post_scan_chain") or 0)),
        "priority": int(rule.get("priority") or 50),
        "max_concurrency": int(rule.get("max_concurrency") or 1),
        "next_run_ts": float(rule.get("next_run_ts")) if rule.get("next_run_ts") is not None else None,
        "last_run_ts": float(rule.get("last_run_ts")) if rule.get("last_run_ts") is not None else None,
        "created_at": float(rule.get("created_at") or 0.0),
        "updated_at": float(rule.get("updated_at") or 0.0),
    }


def _scheduler_rules_replace(rules_payload: list[dict]) -> list[dict]:
    now = time.time()
    existing = _scheduler_rules_fetch()
    existing_ids = {int(r["rule_id"]) for r in existing}
    seen_ids: set[int] = set()
    con = _state_connect(timeout=15)
    cur = con.cursor()
    for raw in (rules_payload or []):
        job_type = _normalize_task_job_type(raw.get("job_type"))
        if not job_type:
            continue
        trigger_type = _normalize_scheduler_trigger(raw.get("trigger_type"))
        scope = _normalize_task_scope(raw.get("scope"), default="both")
        enabled = 1 if bool(raw.get("enabled", True)) else 0
        interval_min = None
        if trigger_type == "interval":
            try:
                interval_min = max(1, min(24 * 60, int(raw.get("interval_min") or 20)))
            except Exception:
                interval_min = 20
        days_of_week = ""
        time_local = ""
        if trigger_type == "weekly":
            days_vals = _parse_days_of_week(raw.get("days_of_week"))
            days_of_week = ",".join(str(d) for d in days_vals) if days_vals else "6"
            hh, mm = _parse_time_local(raw.get("time_local"))
            time_local = f"{hh:02d}:{mm:02d}"
        post_scan_chain = 1 if bool(raw.get("post_scan_chain", False)) else 0
        try:
            priority = max(1, min(999, int(raw.get("priority") or 50)))
        except Exception:
            priority = 50
        try:
            max_concurrency = max(1, min(8, int(raw.get("max_concurrency") or 1)))
        except Exception:
            max_concurrency = 1
        rule_id = raw.get("rule_id")
        if rule_id is not None:
            try:
                rule_id_int = int(rule_id)
            except Exception:
                rule_id_int = 0
        else:
            rule_id_int = 0
        if rule_id_int > 0 and rule_id_int in existing_ids:
            seen_ids.add(rule_id_int)
            cur.execute(
                """
                UPDATE scheduler_rules
                SET job_type = ?, enabled = ?, trigger_type = ?, interval_min = ?, days_of_week = ?, time_local = ?,
                    scope = ?, post_scan_chain = ?, priority = ?, max_concurrency = ?, updated_at = ?
                WHERE rule_id = ?
                """,
                (
                    job_type,
                    enabled,
                    trigger_type,
                    interval_min,
                    days_of_week,
                    time_local,
                    scope,
                    post_scan_chain,
                    priority,
                    max_concurrency,
                    now,
                    rule_id_int,
                ),
            )
        else:
            cur.execute(
                """
                INSERT INTO scheduler_rules
                (job_type, enabled, trigger_type, interval_min, days_of_week, time_local, scope, post_scan_chain, priority, max_concurrency, next_run_ts, last_run_ts, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)
                """,
                (
                    job_type,
                    enabled,
                    trigger_type,
                    interval_min,
                    days_of_week,
                    time_local,
                    scope,
                    post_scan_chain,
                    priority,
                    max_concurrency,
                    now,
                    now,
                ),
            )
            seen_ids.add(int(cur.lastrowid or 0))
    to_delete = [rid for rid in existing_ids if rid not in seen_ids]
    if to_delete:
        placeholders = ",".join("?" for _ in to_delete)
        cur.execute(f"DELETE FROM scheduler_rules WHERE rule_id IN ({placeholders})", to_delete)
    con.commit()
    con.close()
    # Reset next run for active rules after replacement.
    rules_after = _scheduler_rules_fetch()
    for row in rules_after:
        next_ts = _scheduler_compute_next_run(row)
        _scheduler_rule_update_runtime(int(row["rule_id"]), next_run_ts=next_ts)
    return [_scheduler_rule_to_dict(r) for r in _scheduler_rules_fetch()]


_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals()[name] for name in _EXTRACTED_NAMES}


def _normalize_task_job_type_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _normalize_task_job_type(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _normalize_task_scope_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _normalize_task_scope(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _normalize_scheduler_trigger_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _normalize_scheduler_trigger(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _task_event_start_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _task_event_start(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _task_event_finish_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _task_event_finish(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_get_paused_from_db_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_get_paused_from_db(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_set_paused_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_set_paused(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _parse_days_of_week_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _parse_days_of_week(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _parse_time_local_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _parse_time_local(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_compute_next_run_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_compute_next_run(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_rule_scope_matches_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_rule_scope_matches(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_rules_fetch_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_rules_fetch(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_rule_update_runtime_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_rule_update_runtime(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_insert_default_rules_if_empty_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_insert_default_rules_if_empty(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_migrate_legacy_scan_changed_default_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_migrate_legacy_scan_changed_default(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_migrate_legacy_scan_full_default_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_migrate_legacy_scan_full_default(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_ensure_post_scan_chain_defaults_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_ensure_post_scan_chain_defaults(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _pipeline_migrate_legacy_post_scan_async_default_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _pipeline_migrate_legacy_post_scan_async_default(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _library_migrate_legacy_include_unmatched_default_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _library_migrate_legacy_include_unmatched_default(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _provider_gateway_migrate_legacy_discogs_rpm_default_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _provider_gateway_migrate_legacy_discogs_rpm_default(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _web_search_migrate_legacy_provider_default_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _web_search_migrate_legacy_provider_default(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_job_insert_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_job_insert(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_job_update_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_job_update(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_record_skipped_job_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_record_skipped_job(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_get_latest_scan_entry_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_get_latest_scan_entry(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_job_key_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_job_key(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_pool_for_job_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_pool_for_job(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_pool_limit_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_pool_limit(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_can_start_job_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_can_start_job(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_start_scan_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_start_scan(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_wait_for_scan_completion_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_wait_for_scan_completion(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_build_improve_candidates_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_build_improve_candidates(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_run_enrich_batch_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_run_enrich_batch(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_run_dedupe_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_run_dedupe(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_run_incomplete_move_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_run_incomplete_move(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_run_export_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_run_export(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_run_player_sync_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_run_player_sync(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_execute_job_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_execute_job(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_worker_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_worker(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_launch_job_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_launch_job(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_is_enabled_rule_for_chain_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_is_enabled_rule_for_chain(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_chain_max_concurrency_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_chain_max_concurrency(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_loop_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_loop(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _start_scheduler_if_needed_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _start_scheduler_if_needed(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _stop_scheduler_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _stop_scheduler(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_chain_post_scan_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_chain_post_scan(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_rule_to_dict_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_rule_to_dict(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)

def _scheduler_rules_replace_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    try:
        return _scheduler_rules_replace(*args, **kwargs)
    finally:
        _sync_runtime_globals(runtime)
