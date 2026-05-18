"""Authentication, session, and RBAC runtime helpers for PMDA."""

from __future__ import annotations

import logging
from typing import Any

_RUNTIME: Any | None = None

_EXTRACTED_NAMES = {
    '_auth_now_ts',
    '_auth_db_connect',
    '_auth_should_touch_session',
    '_auth_session_cache_get',
    '_auth_session_cache_set',
    '_auth_session_cache_drop',
    '_auth_session_cache_clear',
    '_auth_password_hash',
    '_auth_make_password_hash',
    '_auth_verify_password',
    '_auth_token_hash',
    '_auth_public_user',
    '_auth_validate_username',
    '_auth_validate_password',
    '_auth_validate_avatar_data_url',
    '_auth_normalize_concerts_filter_enabled',
    '_auth_normalize_concerts_radius_km',
    '_auth_normalize_concerts_coord',
    '_auth_get_user_by_username',
    '_auth_get_user_by_id',
    '_auth_bootstrap_required',
    '_auth_count_admin_users',
    '_auth_create_user',
    '_auth_create_session',
    '_auth_touch_login',
    '_auth_rate_limit_key',
    '_auth_rate_limit_prune',
    '_auth_rate_limit_is_blocked',
    '_auth_rate_limit_record_failure',
    '_auth_rate_limit_clear',
    '_auth_normalize_ip',
    '_auth_should_exempt_persistent_ip_ban',
    '_auth_failure_retention_sec',
    '_auth_cleanup_failure_tracking',
    '_auth_persistent_ip_block_info',
    '_auth_record_persistent_ip_failure',
    '_auth_clear_persistent_ip_failures',
    '_auth_retry_response',
    '_auth_get_bearer_token',
    '_auth_client_ip',
    '_auth_log_safe',
    '_auth_security_event',
    '_auth_apply_failure_delay',
    '_auth_ip_is_private_or_loopback',
    '_auth_resolve_session',
    '_auth_delete_session',
    '_current_user_id_or_zero',
    '_current_user_or_empty',
    '_current_username_or_blank',
    '_auth_user_snapshot',
    '_auth_active_users_list',
    '_auth_user_can_use_ai',
    '_require_admin_json',
    '_auth_resolve_public_user_scope',
    '_auth_is_protected_path',
    '_auth_is_public_path',
    '_auth_is_self_path',
    '_auth_non_admin_read_allowed',
    '_auth_non_admin_write_allowed',
    '_auth_guard',
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
        raise RuntimeError("Authentication runtime is not bound")
    return _RUNTIME

def _auth_now_ts() -> int:
    return int(time.time())


def _auth_db_connect() -> sqlite3.Connection:
    timeout_sec = max(0.5, float(AUTH_DB_BUSY_TIMEOUT_MS) / 1000.0)
    con = sqlite3.connect(str(SETTINGS_DB_FILE), timeout=timeout_sec)
    con.row_factory = sqlite3.Row
    try:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute(f"PRAGMA busy_timeout={int(AUTH_DB_BUSY_TIMEOUT_MS)};")
        con.execute("PRAGMA synchronous=NORMAL;")
        con.execute("PRAGMA temp_store=MEMORY;")
    except Exception:
        pass
    return con


def _auth_should_touch_session(token_hash: str, now_ts: int) -> bool:
    """
    Reduce SQLite write contention by throttling auth_sessions.last_used_at updates.
    Session activity is still tracked, but at most once per token per interval.
    """
    key = str(token_hash or "").strip()
    if not key:
        return False
    now = int(now_ts or _auth_now_ts())
    min_interval = max(5, int(AUTH_SESSION_TOUCH_MIN_INTERVAL_SEC or 60))
    with _AUTH_SESSION_TOUCH_LOCK:
        prev = int(_AUTH_SESSION_TOUCH_CACHE.get(key) or 0)
        if prev > 0 and (now - prev) < min_interval:
            return False
        _AUTH_SESSION_TOUCH_CACHE[key] = now
        # Keep the in-memory map bounded in long-running sessions.
        if len(_AUTH_SESSION_TOUCH_CACHE) > 10000:
            cutoff = now - (min_interval * 4)
            stale = [k for k, ts in _AUTH_SESSION_TOUCH_CACHE.items() if int(ts or 0) < cutoff]
            for k in stale[:5000]:
                _AUTH_SESSION_TOUCH_CACHE.pop(k, None)
    return True


def _auth_session_cache_get(token_hash: str, now_ts: int) -> Optional[dict]:
    key = str(token_hash or "").strip()
    if not key:
        return None
    now = int(now_ts or _auth_now_ts())
    with _AUTH_SESSION_CACHE_LOCK:
        entry = _AUTH_SESSION_USER_CACHE.get(key)
        if not isinstance(entry, dict):
            return None
        expires_at = int(entry.get("expires_at") or 0)
        if expires_at <= now:
            _AUTH_SESSION_USER_CACHE.pop(key, None)
            return None
        user_obj = entry.get("user")
        if isinstance(user_obj, dict):
            return dict(user_obj)
        return None


def _auth_session_cache_set(token_hash: str, user_obj: dict, expires_at: int) -> None:
    key = str(token_hash or "").strip()
    if not key or not isinstance(user_obj, dict):
        return
    now = _auth_now_ts()
    with _AUTH_SESSION_CACHE_LOCK:
        _AUTH_SESSION_USER_CACHE[key] = {
            "user": dict(user_obj),
            "expires_at": int(expires_at or 0),
            "updated_at": now,
        }
        if len(_AUTH_SESSION_USER_CACHE) > 5000:
            stale = sorted(
                _AUTH_SESSION_USER_CACHE.items(),
                key=lambda item: int((item[1] or {}).get("updated_at") or 0),
            )
            for stale_key, _ in stale[:2000]:
                _AUTH_SESSION_USER_CACHE.pop(stale_key, None)


def _auth_session_cache_drop(token_hash: str) -> None:
    key = str(token_hash or "").strip()
    if not key:
        return
    with _AUTH_SESSION_CACHE_LOCK:
        _AUTH_SESSION_USER_CACHE.pop(key, None)


def _auth_session_cache_clear() -> None:
    with _AUTH_SESSION_CACHE_LOCK:
        _AUTH_SESSION_USER_CACHE.clear()


def _auth_password_hash(password: str, salt_hex: str) -> str:
    pwd = (password or "").encode("utf-8", errors="ignore")
    salt = bytes.fromhex(salt_hex)
    dk = hashlib.pbkdf2_hmac("sha256", pwd, salt, AUTH_PBKDF2_ITERATIONS, dklen=32)
    return dk.hex()


def _auth_make_password_hash(password: str) -> tuple[str, str]:
    salt_hex = secrets.token_hex(16)
    return _auth_password_hash(password, salt_hex), salt_hex


def _auth_verify_password(password: str, password_hash: str, salt_hex: str) -> bool:
    if not password_hash or not salt_hex:
        return False
    try:
        calc = _auth_password_hash(password, salt_hex)
        return bool(secrets.compare_digest(calc, str(password_hash)))
    except Exception:
        return False


def _auth_token_hash(raw_token: str) -> str:
    payload = f"{AUTH_TOKEN_PEPPER}:{raw_token}" if AUTH_TOKEN_PEPPER else raw_token
    return hashlib.sha256(payload.encode("utf-8", errors="ignore")).hexdigest()


def _auth_public_user(row: sqlite3.Row | dict | None) -> dict:
    if not row:
        return {}
    def _get(name: str, default: Any = None) -> Any:
        try:
            if isinstance(row, dict):
                return row.get(name, default)
            keys = row.keys() if hasattr(row, "keys") else ()
            if name in keys:
                return row[name]
        except Exception:
            pass
        return default
    return {
        "id": int(_get("id", 0) or 0),
        "username": str(_get("username", "") or ""),
        "is_admin": bool(int(_get("is_admin", 0) or 0)),
        "can_download": bool(int(_get("can_download", 0) or 0)),
        "can_view_statistics": bool(int(_get("can_view_statistics", 0) or 0)),
        "allow_ai_calls": bool(int(_get("allow_ai_calls", 1) or 0)),
        "is_active": bool(int(_get("is_active", 0) or 0)),
        "accept_shares": bool(int(_get("accept_shares", 1) or 0)),
        "share_liked_public": bool(int(_get("share_liked_public", 0) or 0)),
        "share_recommendations_public": bool(int(_get("share_recommendations_public", 0) or 0)),
        "avatar_data_url": str(_get("avatar_data_url", "") or "").strip() or None,
        "concerts_filter_enabled": bool(int(_get("concerts_filter_enabled", 0) or 0)),
        "concerts_home_lat": str(_get("concerts_home_lat", "") or "").strip(),
        "concerts_home_lon": str(_get("concerts_home_lon", "") or "").strip(),
        "concerts_radius_km": str(_get("concerts_radius_km", "150") or "").strip() or "150",
        "created_at": int(_get("created_at", 0) or 0),
        "updated_at": int(_get("updated_at", 0) or 0),
        "last_login_at": int(_get("last_login_at", 0) or 0) if _get("last_login_at") is not None else None,
    }


def _auth_validate_username(username: str) -> tuple[bool, str]:
    raw = str(username or "").strip()
    if not raw:
        return False, "Username is required"
    if len(raw) < 3 or len(raw) > 48:
        return False, "Username must be between 3 and 48 characters"
    if not re.fullmatch(r"[a-zA-Z0-9._-]+", raw):
        return False, "Username may only contain letters, numbers, dot, underscore, and dash"
    return True, raw


def _auth_validate_password(password: str) -> tuple[bool, str]:
    raw = str(password or "")
    if len(raw) < AUTH_PASSWORD_MIN_LEN:
        return False, f"Password must be at least {AUTH_PASSWORD_MIN_LEN} characters"
    if len(raw) > AUTH_PASSWORD_MAX_LEN:
        return False, f"Password must be at most {AUTH_PASSWORD_MAX_LEN} characters"
    return True, ""


def _auth_validate_avatar_data_url(value: Any) -> tuple[bool, Optional[str], str]:
    raw = str(value or "").strip()
    if not raw:
        return True, None, ""
    if len(raw) > 400_000:
        return False, None, "Avatar image is too large"
    m = re.match(r"^data:image/(png|jpeg|jpg|webp|gif);base64,([A-Za-z0-9+/=\\s]+)$", raw, flags=re.IGNORECASE)
    if not m:
        return False, None, "Avatar must be a PNG, JPEG, WEBP, or GIF image"
    try:
        payload = re.sub(r"\s+", "", m.group(2))
        decoded = base64.b64decode(payload, validate=True)
    except Exception:
        return False, None, "Avatar image payload is invalid"
    if len(decoded) > 1024 * 1024:
        return False, None, "Avatar image must be 1 MB or smaller after PMDA processing"
    mime = m.group(1).lower()
    if mime == "jpg":
        mime = "jpeg"
    return True, f"data:image/{mime};base64,{payload}", ""


def _auth_normalize_concerts_filter_enabled(value: Any) -> bool:
    return bool(_parse_bool(value))


def _auth_normalize_concerts_radius_km(value: Any) -> str:
    try:
        return str(max(1, min(2000, int(float(value)))))
    except (ValueError, TypeError):
        return "150"


def _auth_normalize_concerts_coord(value: Any, *, axis: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        numeric = float(raw)
        if axis == "lat":
            numeric = max(-90.0, min(90.0, numeric))
        else:
            numeric = max(-180.0, min(180.0, numeric))
        return f"{numeric:.6f}".rstrip("0").rstrip(".")
    except Exception:
        return ""


def _auth_get_user_by_username(username: str) -> Optional[sqlite3.Row]:
    u = str(username or "").strip()
    if not u:
        return None
    con = _auth_db_connect()
    try:
        cur = con.cursor()
        cur.execute(
            """
            SELECT id, username, password_hash, password_salt, is_admin, can_download, can_view_statistics, allow_ai_calls, is_active, accept_shares, share_liked_public, share_recommendations_public, avatar_data_url, concerts_filter_enabled, concerts_home_lat, concerts_home_lon, concerts_radius_km, created_at, updated_at, last_login_at
            FROM auth_users
            WHERE username = ?
            LIMIT 1
            """,
            (u,),
        )
        row = cur.fetchone()
        return row
    finally:
        con.close()


def _auth_get_user_by_id(user_id: int) -> Optional[sqlite3.Row]:
    try:
        uid = int(user_id)
    except Exception:
        return None
    if uid <= 0:
        return None
    con = _auth_db_connect()
    try:
        cur = con.cursor()
        cur.execute(
            """
            SELECT id, username, password_hash, password_salt, is_admin, can_download, can_view_statistics, allow_ai_calls, is_active, accept_shares, share_liked_public, share_recommendations_public, avatar_data_url, concerts_filter_enabled, concerts_home_lat, concerts_home_lon, concerts_radius_km, created_at, updated_at, last_login_at
            FROM auth_users
            WHERE id = ?
            LIMIT 1
            """,
            (uid,),
        )
        row = cur.fetchone()
        return row
    finally:
        con.close()


def _auth_bootstrap_required() -> bool:
    if AUTH_DISABLE:
        return False
    con = _auth_db_connect()
    try:
        cur = con.cursor()
        cur.execute(
            """
            SELECT COUNT(*)
            FROM auth_users
            WHERE is_active = 1 AND is_admin = 1
            """
        )
        row = cur.fetchone()
        count = int((row[0] if row else 0) or 0)
        return count <= 0
    except Exception:
        # Fail closed: force bootstrap flow if auth tables are unexpectedly unavailable.
        return True
    finally:
        con.close()


def _auth_count_admin_users(con: sqlite3.Connection) -> int:
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM auth_users WHERE is_active = 1 AND is_admin = 1")
    row = cur.fetchone()
    return int((row[0] if row else 0) or 0)


def _auth_create_user(
    *,
    username: str,
    password: str,
    is_admin: bool,
    can_download: bool,
    can_view_statistics: bool,
    allow_ai_calls: bool = True,
    is_active: bool = True,
) -> tuple[bool, str, Optional[dict]]:
    ok_u, user_or_msg = _auth_validate_username(username)
    if not ok_u:
        return False, user_or_msg, None
    ok_p, p_msg = _auth_validate_password(password)
    if not ok_p:
        return False, p_msg, None
    username_clean = user_or_msg
    password_hash, salt_hex = _auth_make_password_hash(password)
    now = _auth_now_ts()
    con = _auth_db_connect()
    try:
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO auth_users(
                username, password_hash, password_salt, is_admin, can_download, can_view_statistics, allow_ai_calls, is_active, concerts_filter_enabled, concerts_radius_km, created_at, updated_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                username_clean,
                password_hash,
                salt_hex,
                1 if is_admin else 0,
                1 if can_download else 0,
                1 if can_view_statistics else 0,
                1 if allow_ai_calls else 0,
                1 if is_active else 0,
                0,
                "150",
                now,
                now,
            ),
        )
        user_id = int(cur.lastrowid or 0)
        con.commit()
    except sqlite3.IntegrityError:
        con.rollback()
        return False, "Username already exists", None
    except Exception as e:
        con.rollback()
        return False, f"User creation failed: {e}", None
    finally:
        con.close()
    row = _auth_get_user_by_id(user_id)
    return True, "ok", _auth_public_user(row)


def _auth_create_session(
    user_id: int,
    *,
    ip: str = "",
    user_agent: str = "",
    ttl_sec: int | None = None,
) -> tuple[str, int]:
    raw_token = secrets.token_urlsafe(48)
    token_hash = _auth_token_hash(raw_token)
    now = _auth_now_ts()
    ttl = AUTH_SESSION_TTL_SEC if ttl_sec is None else int(ttl_sec)
    ttl = max(900, ttl)
    expires = now + ttl
    con = _auth_db_connect()
    try:
        cur = con.cursor()
        cur.execute("DELETE FROM auth_sessions WHERE expires_at <= ?", (now,))
        cur.execute(
            """
            INSERT OR REPLACE INTO auth_sessions(
                token_hash, user_id, created_at, expires_at, last_used_at, ip, user_agent
            )
            VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            (token_hash, int(user_id), now, expires, now, str(ip or "")[:120], str(user_agent or "")[:240]),
        )
        con.commit()
    finally:
        con.close()
    return raw_token, expires


def _auth_touch_login(user_id: int, *, ip: str = "") -> None:
    con = _auth_db_connect()
    try:
        cur = con.cursor()
        cur.execute(
            "UPDATE auth_users SET last_login_at = ?, updated_at = ? WHERE id = ?",
            (_auth_now_ts(), _auth_now_ts(), int(user_id)),
        )
        con.commit()
    except Exception:
        con.rollback()
    finally:
        con.close()


def _auth_rate_limit_key(kind: str, key: str) -> tuple[str, str]:
    return (str(kind or "").strip().lower(), str(key or "").strip().lower())


def _auth_rate_limit_prune(now_ts: int) -> None:
    cutoff = now_ts - AUTH_LOGIN_RATE_LIMIT_WINDOW_SEC
    stale_keys: list[tuple[str, str]] = []
    for bucket_key, timestamps in _AUTH_RATE_LIMIT_BUCKETS.items():
        while timestamps and int(timestamps[0]) < cutoff:
            timestamps.popleft()
        if not timestamps:
            stale_keys.append(bucket_key)
    for bucket_key in stale_keys:
        _AUTH_RATE_LIMIT_BUCKETS.pop(bucket_key, None)


def _auth_rate_limit_is_blocked(kind: str, key: str, *, max_attempts: int) -> bool:
    now_ts = _auth_now_ts()
    bucket_key = _auth_rate_limit_key(kind, key)
    if not bucket_key[1]:
        return False
    with _AUTH_RATE_LIMIT_LOCK:
        _auth_rate_limit_prune(now_ts)
        timestamps = _AUTH_RATE_LIMIT_BUCKETS.get(bucket_key)
        if not timestamps:
            return False
        return len(timestamps) >= max(1, int(max_attempts or 1))


def _auth_rate_limit_record_failure(kind: str, key: str) -> None:
    now_ts = _auth_now_ts()
    bucket_key = _auth_rate_limit_key(kind, key)
    if not bucket_key[1]:
        return
    with _AUTH_RATE_LIMIT_LOCK:
        _auth_rate_limit_prune(now_ts)
        timestamps = _AUTH_RATE_LIMIT_BUCKETS.get(bucket_key)
        if timestamps is None:
            timestamps = deque()
            _AUTH_RATE_LIMIT_BUCKETS[bucket_key] = timestamps
        timestamps.append(now_ts)


def _auth_rate_limit_clear(kind: str, key: str) -> None:
    bucket_key = _auth_rate_limit_key(kind, key)
    if not bucket_key[1]:
        return
    with _AUTH_RATE_LIMIT_LOCK:
        _AUTH_RATE_LIMIT_BUCKETS.pop(bucket_key, None)


def _auth_normalize_ip(ip_value: str) -> str:
    raw = str(ip_value or "").strip()
    if not raw:
        return ""
    if raw.lower().startswith("::ffff:"):
        raw = raw[7:]
    return raw.strip().lower()


def _auth_should_exempt_persistent_ip_ban(ip_value: str) -> bool:
    normalized = _auth_normalize_ip(ip_value)
    if not normalized:
        return True
    return bool(AUTH_IP_BAN_EXEMPT_PRIVATE and _auth_ip_is_private_or_loopback(normalized))


def _auth_failure_retention_sec() -> int:
    return max(
        24 * 3600,
        int(AUTH_IP_BAN_WINDOW_SEC or 0) * 4,
        int(AUTH_LOGIN_RATE_LIMIT_WINDOW_SEC or 0) * 4,
        int(AUTH_IP_BAN_LONG_DURATION_SEC or 0) * 2,
    )


def _auth_cleanup_failure_tracking(con: sqlite3.Connection, now_ts: int) -> None:
    cutoff = int(now_ts or 0) - _auth_failure_retention_sec()
    cur = con.cursor()
    cur.execute("DELETE FROM auth_failure_events WHERE failure_ts < ?", (cutoff,))
    cur.execute("DELETE FROM auth_ip_bans WHERE ban_until <= ? AND updated_at < ?", (int(now_ts or 0), cutoff))


def _auth_persistent_ip_block_info(ip_value: str) -> tuple[bool, int]:
    if not AUTH_IP_BAN_ENABLED:
        return False, 0
    ip_key = _auth_normalize_ip(ip_value)
    if not ip_key or _auth_should_exempt_persistent_ip_ban(ip_key):
        return False, 0
    now_ts = _auth_now_ts()
    con = _auth_db_connect()
    try:
        cur = con.cursor()
        _auth_cleanup_failure_tracking(con, now_ts)
        cur.execute("SELECT ban_until FROM auth_ip_bans WHERE ip = ? LIMIT 1", (ip_key,))
        row = cur.fetchone()
        if not row:
            con.commit()
            return False, 0
        ban_until = int((row["ban_until"] if isinstance(row, sqlite3.Row) else row[0]) or 0)
        if ban_until > now_ts:
            con.commit()
            return True, max(1, ban_until - now_ts)
        cur.execute("DELETE FROM auth_ip_bans WHERE ip = ?", (ip_key,))
        con.commit()
        return False, 0
    except Exception:
        con.rollback()
        return False, 0
    finally:
        con.close()


def _auth_record_persistent_ip_failure(ip_value: str, *, username: str = "", path: str = "", reason: str = "") -> tuple[bool, int, int]:
    if not AUTH_IP_BAN_ENABLED:
        return False, 0, 0
    ip_key = _auth_normalize_ip(ip_value)
    if not ip_key or _auth_should_exempt_persistent_ip_ban(ip_key):
        return False, 0, 0
    now_ts = _auth_now_ts()
    con = _auth_db_connect()
    try:
        cur = con.cursor()
        _auth_cleanup_failure_tracking(con, now_ts)
        cur.execute(
            """
            INSERT INTO auth_failure_events(kind, subject_key, failure_ts, path, username)
            VALUES('ip', ?, ?, ?, ?)
            """,
            (ip_key, now_ts, str(path or "")[:128], str(username or "")[:80]),
        )
        cutoff = now_ts - int(AUTH_IP_BAN_WINDOW_SEC or 0)
        cur.execute(
            "DELETE FROM auth_failure_events WHERE kind = 'ip' AND subject_key = ? AND failure_ts < ?",
            (ip_key, cutoff),
        )
        cur.execute(
            "SELECT COUNT(*) FROM auth_failure_events WHERE kind = 'ip' AND subject_key = ? AND failure_ts >= ?",
            (ip_key, cutoff),
        )
        row = cur.fetchone()
        failure_count = int((row[0] if row else 0) or 0)
        ban_duration = 0
        ban_reason = ""
        if failure_count >= int(AUTH_IP_BAN_LONG_THRESHOLD or 0):
            ban_duration = int(AUTH_IP_BAN_LONG_DURATION_SEC or 0)
            ban_reason = "persistent_ip_ban_long"
        elif failure_count >= int(AUTH_IP_BAN_SHORT_THRESHOLD or 0):
            ban_duration = int(AUTH_IP_BAN_SHORT_DURATION_SEC or 0)
            ban_reason = "persistent_ip_ban_short"
        if ban_duration > 0:
            ban_until = now_ts + ban_duration
            cur.execute("SELECT ban_until, created_at FROM auth_ip_bans WHERE ip = ? LIMIT 1", (ip_key,))
            existing = cur.fetchone()
            existing_until = int((existing["ban_until"] if isinstance(existing, sqlite3.Row) else existing[0]) or 0) if existing else 0
            created_at = int((existing["created_at"] if isinstance(existing, sqlite3.Row) else existing[1]) or now_ts) if existing else now_ts
            if existing_until > ban_until:
                ban_until = existing_until
            cur.execute(
                """
                INSERT INTO auth_ip_bans(ip, ban_until, fail_count, window_sec, last_reason, created_at, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ip) DO UPDATE SET
                    ban_until = excluded.ban_until,
                    fail_count = excluded.fail_count,
                    window_sec = excluded.window_sec,
                    last_reason = excluded.last_reason,
                    updated_at = excluded.updated_at
                """,
                (
                    ip_key,
                    ban_until,
                    failure_count,
                    int(AUTH_IP_BAN_WINDOW_SEC or 0),
                    str(reason or ban_reason or "persistent_ip_ban")[:160],
                    created_at,
                    now_ts,
                ),
            )
            con.commit()
            return True, max(1, ban_until - now_ts), failure_count
        con.commit()
        return False, 0, failure_count
    except Exception:
        con.rollback()
        return False, 0, 0
    finally:
        con.close()


def _auth_clear_persistent_ip_failures(ip_value: str) -> None:
    if not AUTH_IP_BAN_ENABLED:
        return
    ip_key = _auth_normalize_ip(ip_value)
    if not ip_key or _auth_should_exempt_persistent_ip_ban(ip_key):
        return
    con = _auth_db_connect()
    try:
        cur = con.cursor()
        cur.execute("DELETE FROM auth_failure_events WHERE kind = 'ip' AND subject_key = ?", (ip_key,))
        cur.execute("DELETE FROM auth_ip_bans WHERE ip = ?", (ip_key,))
        con.commit()
    except Exception:
        con.rollback()
    finally:
        con.close()


def _auth_retry_response(message: str, *, retry_after_sec: int = 0) -> Response:
    payload = {"error": str(message or "Too many failed login attempts. Please wait and retry.")}
    if int(retry_after_sec or 0) > 0:
        payload["retry_after_sec"] = int(retry_after_sec)
    response = jsonify(payload)
    response.status_code = 429
    if int(retry_after_sec or 0) > 0:
        response.headers["Retry-After"] = str(int(retry_after_sec))
    return response


def _auth_get_bearer_token() -> str:
    authz = (request.headers.get("Authorization") or "").strip()
    if authz.lower().startswith("bearer "):
        return authz[7:].strip()
    # Browser media tags (<img src=...>) cannot set Authorization headers.
    # Accept HttpOnly session cookie only for safe read methods to limit CSRF surface.
    if request.method in {"GET", "HEAD"}:
        query_token = str(request.args.get("auth_token") or "").strip()
        safe_media_paths = (
            "/api/library/track/",
            "/api/library/files/album/",
            "/api/library/files/artist/",
            "/api/library/external/artist-image/",
            "/api/library/external/label-image/",
        )
        if query_token and request.path.startswith(safe_media_paths):
            return query_token
        cookie_token = str(request.cookies.get(AUTH_SESSION_COOKIE_NAME) or "").strip()
        if cookie_token:
            return cookie_token
    return ""


def _auth_client_ip() -> str:
    remote_ip = _auth_normalize_ip(request.remote_addr or "")
    if AUTH_TRUST_PROXY_HEADERS:
        cf_ip = _auth_normalize_ip(request.headers.get("CF-Connecting-IP") or "")
        if cf_ip:
            return cf_ip
        forwarded = str(request.headers.get("X-Forwarded-For") or "").strip()
        if forwarded:
            return _auth_normalize_ip(forwarded.split(",", 1)[0] or "")
        real_ip = _auth_normalize_ip(request.headers.get("X-Real-IP") or "")
        if real_ip:
            return real_ip
    return remote_ip


def _auth_log_safe(value: str, *, max_len: int = 160) -> str:
    raw = str(value or "").replace("\n", " ").replace("\r", " ").strip()
    if not raw:
        return "-"
    raw = re.sub(r"\s+", " ", raw)
    return raw[:max_len]


def _auth_security_event(
    event: str,
    *,
    ip: str = "",
    username: str = "",
    path: str = "/api/auth/login",
    outcome: str = "",
    reason: str = "",
    level: int = logging.WARNING,
) -> None:
    logging.log(
        level,
        "[AUTH_EVENT] event=%s path=%s ip=%s user=%s outcome=%s reason=%s",
        _auth_log_safe(event, max_len=64),
        _auth_log_safe(path, max_len=128),
        _auth_log_safe(ip, max_len=80),
        _auth_log_safe(username, max_len=80),
        _auth_log_safe(outcome, max_len=48),
        _auth_log_safe(reason, max_len=160),
    )


def _auth_apply_failure_delay() -> None:
    delay_ms = int(AUTH_LOGIN_FAILURE_DELAY_MS or 0)
    if delay_ms <= 0:
        return
    time.sleep(float(delay_ms) / 1000.0)


def _auth_ip_is_private_or_loopback(ip_value: str) -> bool:
    raw = str(ip_value or "").strip()
    if not raw:
        return False
    if raw.lower().startswith("::ffff:"):
        raw = raw[7:]
    try:
        addr = ipaddress.ip_address(raw)
        return bool(addr.is_private or addr.is_loopback)
    except ValueError:
        return False


def _auth_resolve_session(raw_token: str) -> Optional[dict]:
    token = str(raw_token or "").strip()
    if not token:
        return None
    now = _auth_now_ts()
    token_hash = _auth_token_hash(token)
    should_touch = bool(AUTH_SESSION_TOUCH_DB_WRITES and _auth_should_touch_session(token_hash, now))
    cached = _auth_session_cache_get(token_hash, now)
    if cached and not should_touch:
        return cached
    lock_error = None
    for attempt in range(2):
        con = _auth_db_connect()
        try:
            cur = con.cursor()
            cur.execute(
                """
                SELECT
                    s.user_id,
                    s.expires_at,
                    u.id,
                    u.username,
                    u.is_admin,
                    u.can_download,
                    u.can_view_statistics,
                    u.allow_ai_calls,
                    u.is_active,
                    u.accept_shares,
                    u.share_liked_public,
                    u.share_recommendations_public,
                    u.avatar_data_url,
                    u.concerts_filter_enabled,
                    u.concerts_home_lat,
                    u.concerts_home_lon,
                    u.concerts_radius_km,
                    u.created_at,
                    u.updated_at,
                    u.last_login_at
                FROM auth_sessions s
                JOIN auth_users u ON u.id = s.user_id
                WHERE s.token_hash = ?
                LIMIT 1
                """,
                (token_hash,),
            )
            row = cur.fetchone()
            if not row:
                _auth_session_cache_drop(token_hash)
                return None
            expires_at = int(row["expires_at"] or 0)
            if expires_at <= now:
                try:
                    cur.execute("DELETE FROM auth_sessions WHERE token_hash = ?", (token_hash,))
                    con.commit()
                except Exception:
                    try:
                        con.rollback()
                    except Exception:
                        pass
                _auth_session_cache_drop(token_hash)
                return None
            if not bool(int(row["is_active"] or 0)):
                _auth_session_cache_drop(token_hash)
                return None
            user_obj = _auth_public_user(row)
            _auth_session_cache_set(token_hash, user_obj, expires_at)
            # Best effort touch: never fail auth resolution because SQLite is briefly locked.
            if should_touch:
                try:
                    cur.execute("UPDATE auth_sessions SET last_used_at = ? WHERE token_hash = ?", (now, token_hash))
                    con.commit()
                except sqlite3.OperationalError as exc:
                    msg = str(exc or "").lower()
                    if "locked" in msg or "busy" in msg:
                        try:
                            con.rollback()
                        except Exception:
                            pass
                        logging.debug("Auth session touch skipped due to SQLite lock")
                    else:
                        raise
                except Exception:
                    try:
                        con.rollback()
                    except Exception:
                        pass
            return user_obj
        except sqlite3.OperationalError as exc:
            msg = str(exc or "").lower()
            if "locked" in msg or "busy" in msg:
                lock_error = exc
                try:
                    con.rollback()
                except Exception:
                    pass
                if attempt == 0:
                    time.sleep(0.05)
                    continue
                cached = _auth_session_cache_get(token_hash, now)
                if cached:
                    logging.debug("Auth session served from cache after SQLite lock")
                    return cached
                logging.warning("Auth session lookup failed due to SQLite lock and no cache entry")
                return None
            raise
        finally:
            con.close()
    if lock_error is not None:
        cached = _auth_session_cache_get(token_hash, now)
        if cached:
            logging.debug("Auth session served from cache after lock retry exhaustion")
            return cached
    return None


def _auth_delete_session(raw_token: str) -> None:
    token = str(raw_token or "").strip()
    if not token:
        return
    token_hash = _auth_token_hash(token)
    _auth_session_cache_drop(token_hash)
    con = _auth_db_connect()
    try:
        cur = con.cursor()
        cur.execute("DELETE FROM auth_sessions WHERE token_hash = ?", (token_hash,))
        con.commit()
    finally:
        con.close()


def _current_user_id_or_zero() -> int:
    user = dict(getattr(g, "current_user", {}) or {}) if has_request_context() else {}
    try:
        uid = int(user.get("id") or 0)
    except Exception:
        uid = 0
    return max(0, uid)


def _current_user_or_empty() -> dict[str, Any]:
    return dict(getattr(g, "current_user", {}) or {}) if has_request_context() else {}


def _current_username_or_blank() -> str:
    user = dict(getattr(g, "current_user", {}) or {}) if has_request_context() else {}
    return str(user.get("username") or "").strip()


def _auth_user_snapshot(user_id: int) -> dict[str, Any]:
    uid = max(0, int(user_id or 0))
    if uid <= 0:
        return {"id": 0, "username": ""}
    row = _auth_get_user_by_id(uid)
    pub = _auth_public_user(row)
    return {
        "id": uid,
        "username": str(pub.get("username") or "").strip(),
        "avatar_data_url": str(pub.get("avatar_data_url") or "").strip() or None,
    }


def _auth_active_users_list(
    *,
    exclude_user_id: int = 0,
    require_accept_shares: bool = False,
    require_public_likes: bool = False,
    require_public_recommendations: bool = False,
) -> list[dict[str, Any]]:
    con = _auth_db_connect()
    try:
        cur = con.cursor()
        cur.execute(
            """
            SELECT id, username, is_admin, can_download, can_view_statistics, allow_ai_calls, is_active, accept_shares, share_liked_public, share_recommendations_public, avatar_data_url, concerts_filter_enabled, concerts_home_lat, concerts_home_lon, concerts_radius_km, created_at, updated_at, last_login_at
            FROM auth_users
            WHERE is_active = 1
            ORDER BY username COLLATE NOCASE ASC
            """
        )
        rows = cur.fetchall()
        users: list[dict[str, Any]] = []
        for row in rows:
            pub = _auth_public_user(row)
            if int(pub.get("id") or 0) == int(exclude_user_id or 0):
                continue
            if require_accept_shares and not bool(pub.get("accept_shares")):
                continue
            if require_public_likes and not bool(pub.get("share_liked_public")):
                continue
            if require_public_recommendations and not bool(pub.get("share_recommendations_public")):
                continue
            users.append(pub)
        return users
    finally:
        con.close()


def _auth_user_can_use_ai(user: dict[str, Any] | None) -> bool:
    if not user:
        return False
    if bool(user.get("is_admin")):
        return True
    return bool(user.get("allow_ai_calls", True))


def _require_admin_json() -> Optional[tuple[Response, int]]:
    user = _current_user_or_empty()
    if bool(user.get("is_admin")):
        return None
    return jsonify({"error": "Administrator only"}), 403


def _auth_resolve_public_user_scope(
    requested_user_id: Any,
    *,
    current_user_id: int,
    visibility_key: str,
) -> tuple[int, Optional[dict], Optional[tuple[str, int]]]:
    target_user_id = _parse_int_loose(requested_user_id, 0)
    if target_user_id <= 0 or target_user_id == int(current_user_id or 0):
        row = _auth_get_user_by_id(int(current_user_id or 0))
        pub = _auth_public_user(row)
        return int(current_user_id or 0), pub if pub else None, None
    row = _auth_get_user_by_id(int(target_user_id))
    pub = _auth_public_user(row)
    if not pub or not bool(pub.get("is_active")):
        return 0, None, ("User not found", 404)
    if not bool(pub.get(visibility_key)):
        return 0, None, ("This user does not share this view", 403)
    return int(target_user_id), pub, None


def _auth_is_protected_path(path: str) -> bool:
    p = str(path or "")
    return (
        p.startswith("/api/")
        or p.startswith("/scan/")
        or p.startswith("/dedupe/")
        or p.startswith("/details/")
    )


def _auth_is_public_path(path: str) -> bool:
    p = str(path or "")
    return p in {
        "/api/auth/bootstrap/status",
        "/api/auth/bootstrap",
        "/api/auth/login",
        "/api/ui/build",
    }


def _auth_is_self_path(path: str) -> bool:
    p = str(path or "")
    if p in {
        "/api/auth/me",
        "/api/auth/profile",
        "/api/auth/logout",
        "/api/ai/providers/preferences",
    }:
        return True
    if p.startswith("/api/openai/oauth/"):
        return True
    return p.startswith("/api/ai/providers/openai-codex/oauth/")


def _auth_non_admin_read_allowed(path: str, method: str, can_download: bool, can_view_statistics: bool) -> bool:
    p = str(path or "")
    m = str(method or "GET").upper()
    if m != "GET":
        return False
    if p == "/api/config":
        return True
    if p.startswith("/api/library/"):
        # Optional permission gate for explicit download endpoint.
        if p.endswith("/download") and not bool(can_download):
            return False
        # Statistics-heavy endpoints under /api/library remain restricted unless explicitly allowed.
        if p.startswith("/api/library/stats/library") or p.startswith("/api/library/playback/stats"):
            return bool(can_view_statistics)
        return True
    if not bool(can_view_statistics):
        return False
    return (
        p == "/api/progress"
        or p == "/api/files/watcher/status"
        or p.startswith("/api/statistics/")
        or p.startswith("/api/scan-history")
        or p.startswith("/api/scans/")
    )


def _auth_non_admin_write_allowed(path: str, method: str) -> bool:
    p = str(path or "")
    m = str(method or "").upper()
    if m not in {"POST", "PUT", "DELETE"}:
        return False
    allowed_exact = {
        ("PUT", "/api/library/likes"),
        ("POST", "/api/library/playlists"),
        ("POST", "/api/library/reco/event"),
        ("POST", "/api/library/playback/event"),
        ("POST", "/api/assistant/chat"),
        ("POST", "/api/library/entity-discover"),
    }
    if (m, p) in allowed_exact:
        return True
    allowed_prefixes = (
        ("PUT", "/api/library/album/"),
        ("POST", "/api/library/share"),
        ("POST", "/api/library/recommendations/"),
        ("POST", "/api/library/notifications/"),
        ("DELETE", "/api/library/playlists/"),
        ("POST", "/api/library/playlists/"),
    )
    for allowed_method, allowed_prefix in allowed_prefixes:
        if m == allowed_method and p.startswith(allowed_prefix):
            if allowed_prefix == "/api/library/album/" and not (p.endswith("/rating") or p.endswith("/review")):
                continue
            if allowed_prefix == "/api/library/recommendations/" and not p.endswith("/like"):
                continue
            if allowed_prefix == "/api/library/notifications/" and not p.endswith("/read"):
                continue
            return True
    return False


def _auth_guard():
    # CORS preflight handled by dedicated hook.
    if request.method == "OPTIONS":
        return None

    # Default anonymous context.
    g.current_user = None

    path = request.path or ""
    if path.startswith("/api/mcp/"):
        return _mcp_auth_guard(path)

    if AUTH_DISABLE:
        g.current_user = {
            "id": 0,
            "username": "auth_disabled",
            "is_admin": True,
            "can_download": True,
            "can_view_statistics": True,
            "allow_ai_calls": True,
            "is_active": True,
        }
        return None

    if not _auth_is_protected_path(path):
        return None

    if _auth_is_public_path(path):
        return None

    if _auth_bootstrap_required():
        return jsonify({"error": "Bootstrap required. Create the initial admin user first."}), 428

    raw_token = _auth_get_bearer_token()
    if not raw_token:
        return jsonify({"error": "Authentication required"}), 401

    user = _auth_resolve_session(raw_token)
    if not user:
        return jsonify({"error": "Invalid or expired session"}), 401

    g.current_user = user

    # Allow current-user endpoints regardless of role.
    if _auth_is_self_path(path):
        return None

    if bool(user.get("is_admin")):
        return None

    if _auth_non_admin_read_allowed(
        path,
        request.method,
        bool(user.get("can_download")),
        bool(user.get("can_view_statistics")),
    ):
        return None

    if _auth_non_admin_write_allowed(path, request.method):
        return None

    return jsonify({"error": "Forbidden: admin access required"}), 403

_ORIGINAL_EXTRACTED_FUNCTIONS = {
    '_auth_now_ts': _auth_now_ts,
    '_auth_db_connect': _auth_db_connect,
    '_auth_should_touch_session': _auth_should_touch_session,
    '_auth_session_cache_get': _auth_session_cache_get,
    '_auth_session_cache_set': _auth_session_cache_set,
    '_auth_session_cache_drop': _auth_session_cache_drop,
    '_auth_session_cache_clear': _auth_session_cache_clear,
    '_auth_password_hash': _auth_password_hash,
    '_auth_make_password_hash': _auth_make_password_hash,
    '_auth_verify_password': _auth_verify_password,
    '_auth_token_hash': _auth_token_hash,
    '_auth_public_user': _auth_public_user,
    '_auth_validate_username': _auth_validate_username,
    '_auth_validate_password': _auth_validate_password,
    '_auth_validate_avatar_data_url': _auth_validate_avatar_data_url,
    '_auth_normalize_concerts_filter_enabled': _auth_normalize_concerts_filter_enabled,
    '_auth_normalize_concerts_radius_km': _auth_normalize_concerts_radius_km,
    '_auth_normalize_concerts_coord': _auth_normalize_concerts_coord,
    '_auth_get_user_by_username': _auth_get_user_by_username,
    '_auth_get_user_by_id': _auth_get_user_by_id,
    '_auth_bootstrap_required': _auth_bootstrap_required,
    '_auth_count_admin_users': _auth_count_admin_users,
    '_auth_create_user': _auth_create_user,
    '_auth_create_session': _auth_create_session,
    '_auth_touch_login': _auth_touch_login,
    '_auth_rate_limit_key': _auth_rate_limit_key,
    '_auth_rate_limit_prune': _auth_rate_limit_prune,
    '_auth_rate_limit_is_blocked': _auth_rate_limit_is_blocked,
    '_auth_rate_limit_record_failure': _auth_rate_limit_record_failure,
    '_auth_rate_limit_clear': _auth_rate_limit_clear,
    '_auth_normalize_ip': _auth_normalize_ip,
    '_auth_should_exempt_persistent_ip_ban': _auth_should_exempt_persistent_ip_ban,
    '_auth_failure_retention_sec': _auth_failure_retention_sec,
    '_auth_cleanup_failure_tracking': _auth_cleanup_failure_tracking,
    '_auth_persistent_ip_block_info': _auth_persistent_ip_block_info,
    '_auth_record_persistent_ip_failure': _auth_record_persistent_ip_failure,
    '_auth_clear_persistent_ip_failures': _auth_clear_persistent_ip_failures,
    '_auth_retry_response': _auth_retry_response,
    '_auth_get_bearer_token': _auth_get_bearer_token,
    '_auth_client_ip': _auth_client_ip,
    '_auth_log_safe': _auth_log_safe,
    '_auth_security_event': _auth_security_event,
    '_auth_apply_failure_delay': _auth_apply_failure_delay,
    '_auth_ip_is_private_or_loopback': _auth_ip_is_private_or_loopback,
    '_auth_resolve_session': _auth_resolve_session,
    '_auth_delete_session': _auth_delete_session,
    '_current_user_id_or_zero': _current_user_id_or_zero,
    '_current_user_or_empty': _current_user_or_empty,
    '_current_username_or_blank': _current_username_or_blank,
    '_auth_user_snapshot': _auth_user_snapshot,
    '_auth_active_users_list': _auth_active_users_list,
    '_auth_user_can_use_ai': _auth_user_can_use_ai,
    '_require_admin_json': _require_admin_json,
    '_auth_resolve_public_user_scope': _auth_resolve_public_user_scope,
    '_auth_is_protected_path': _auth_is_protected_path,
    '_auth_is_public_path': _auth_is_public_path,
    '_auth_is_self_path': _auth_is_self_path,
    '_auth_non_admin_read_allowed': _auth_non_admin_read_allowed,
    '_auth_non_admin_write_allowed': _auth_non_admin_write_allowed,
    '_auth_guard': _auth_guard,
}

def _auth_now_ts_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_now_ts(*args, **kwargs)

def _auth_db_connect_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_db_connect(*args, **kwargs)

def _auth_should_touch_session_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_should_touch_session(*args, **kwargs)

def _auth_session_cache_get_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_session_cache_get(*args, **kwargs)

def _auth_session_cache_set_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_session_cache_set(*args, **kwargs)

def _auth_session_cache_drop_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_session_cache_drop(*args, **kwargs)

def _auth_session_cache_clear_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_session_cache_clear(*args, **kwargs)

def _auth_password_hash_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_password_hash(*args, **kwargs)

def _auth_make_password_hash_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_make_password_hash(*args, **kwargs)

def _auth_verify_password_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_verify_password(*args, **kwargs)

def _auth_token_hash_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_token_hash(*args, **kwargs)

def _auth_public_user_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_public_user(*args, **kwargs)

def _auth_validate_username_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_validate_username(*args, **kwargs)

def _auth_validate_password_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_validate_password(*args, **kwargs)

def _auth_validate_avatar_data_url_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_validate_avatar_data_url(*args, **kwargs)

def _auth_normalize_concerts_filter_enabled_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_normalize_concerts_filter_enabled(*args, **kwargs)

def _auth_normalize_concerts_radius_km_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_normalize_concerts_radius_km(*args, **kwargs)

def _auth_normalize_concerts_coord_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_normalize_concerts_coord(*args, **kwargs)

def _auth_get_user_by_username_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_get_user_by_username(*args, **kwargs)

def _auth_get_user_by_id_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_get_user_by_id(*args, **kwargs)

def _auth_bootstrap_required_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_bootstrap_required(*args, **kwargs)

def _auth_count_admin_users_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_count_admin_users(*args, **kwargs)

def _auth_create_user_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_create_user(*args, **kwargs)

def _auth_create_session_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_create_session(*args, **kwargs)

def _auth_touch_login_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_touch_login(*args, **kwargs)

def _auth_rate_limit_key_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_rate_limit_key(*args, **kwargs)

def _auth_rate_limit_prune_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_rate_limit_prune(*args, **kwargs)

def _auth_rate_limit_is_blocked_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_rate_limit_is_blocked(*args, **kwargs)

def _auth_rate_limit_record_failure_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_rate_limit_record_failure(*args, **kwargs)

def _auth_rate_limit_clear_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_rate_limit_clear(*args, **kwargs)

def _auth_normalize_ip_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_normalize_ip(*args, **kwargs)

def _auth_should_exempt_persistent_ip_ban_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_should_exempt_persistent_ip_ban(*args, **kwargs)

def _auth_failure_retention_sec_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_failure_retention_sec(*args, **kwargs)

def _auth_cleanup_failure_tracking_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_cleanup_failure_tracking(*args, **kwargs)

def _auth_persistent_ip_block_info_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_persistent_ip_block_info(*args, **kwargs)

def _auth_record_persistent_ip_failure_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_record_persistent_ip_failure(*args, **kwargs)

def _auth_clear_persistent_ip_failures_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_clear_persistent_ip_failures(*args, **kwargs)

def _auth_retry_response_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_retry_response(*args, **kwargs)

def _auth_get_bearer_token_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_get_bearer_token(*args, **kwargs)

def _auth_client_ip_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_client_ip(*args, **kwargs)

def _auth_log_safe_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_log_safe(*args, **kwargs)

def _auth_security_event_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_security_event(*args, **kwargs)

def _auth_apply_failure_delay_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_apply_failure_delay(*args, **kwargs)

def _auth_ip_is_private_or_loopback_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_ip_is_private_or_loopback(*args, **kwargs)

def _auth_resolve_session_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_resolve_session(*args, **kwargs)

def _auth_delete_session_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_delete_session(*args, **kwargs)

def _current_user_id_or_zero_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _current_user_id_or_zero(*args, **kwargs)

def _current_user_or_empty_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _current_user_or_empty(*args, **kwargs)

def _current_username_or_blank_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _current_username_or_blank(*args, **kwargs)

def _auth_user_snapshot_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_user_snapshot(*args, **kwargs)

def _auth_active_users_list_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_active_users_list(*args, **kwargs)

def _auth_user_can_use_ai_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_user_can_use_ai(*args, **kwargs)

def _require_admin_json_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _require_admin_json(*args, **kwargs)

def _auth_resolve_public_user_scope_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_resolve_public_user_scope(*args, **kwargs)

def _auth_is_protected_path_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_is_protected_path(*args, **kwargs)

def _auth_is_public_path_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_is_public_path(*args, **kwargs)

def _auth_is_self_path_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_is_self_path(*args, **kwargs)

def _auth_non_admin_read_allowed_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_non_admin_read_allowed(*args, **kwargs)

def _auth_non_admin_write_allowed_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_non_admin_write_allowed(*args, **kwargs)

def _auth_guard_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _auth_guard(*args, **kwargs)
