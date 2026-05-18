"""Last.fm integration runtime helpers for PMDA."""

from __future__ import annotations

from typing import Any

_RUNTIME: Any | None = None

_EXTRACTED_NAMES = {
    '_run_lastfm_preflight',
    '_lastfm_credentials_effective',
    '_lastfm_session_name',
    '_lastfm_session_key',
    '_lastfm_pending_token',
    '_lastfm_has_stored_session_ciphertext',
    '_lastfm_has_stored_pending_ciphertext',
    '_lastfm_status_snapshot',
    '_lastfm_try_complete_pending_authorization_if_needed',
    '_lastfm_store_session_payload',
    '_lastfm_try_complete_pending_authorization',
    '_lastfm_scrobble_enabled',
    '_lastfm_now_playing_enabled',
    '_lastfm_auth_callback_url',
    '_lastfm_auth_url_for_token',
    '_lastfm_callback_html',
    '_lastfm_api_sig',
    '_lastfm_signed_post',
    '_lastfm_get',
    '_lastfm_playback_track_payload',
    '_lastfm_loved_sync_setting',
    '_lastfm_track_identity_map',
    '_lastfm_set_track_love',
    '_lastfm_sync_loved_tracks_to_pmda',
    '_lastfm_scrobble_threshold_seconds',
    '_lastfm_submit_now_playing',
    '_lastfm_submit_scrobble',
    '_lastfm_handle_playback_event_async',
    '_lastfm_pick_search_candidate',
    '_lastfm_payload_has_album_page',
    '_fetch_lastfm_album_info',
    '_cleanup_lastfm_bio_text',
    '_fetch_lastfm_artist_info',
    '_lastfm_cover_url_candidates',
    '_fetch_artist_image_lastfm',
    'api_lastfm_auth_status',
    'api_lastfm_auth_start',
    'api_lastfm_auth_callback',
    'api_lastfm_auth_complete',
    'api_lastfm_auth_disconnect',
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
        raise RuntimeError("Last.fm runtime is not bound")
    return _RUNTIME

def _run_lastfm_preflight() -> tuple[bool, str]:
    """Test Last.fm API connectivity. Returns (ok, message)."""
    if not USE_LASTFM or not (getattr(_runtime_module(), "LASTFM_API_KEY", "") or "").strip():
        return False, "Disabled (no API key)"
    try:
        api_key = (getattr(_runtime_module(), "LASTFM_API_KEY", "") or "").strip()
        resp = _provider_gateway_http_get(
            "lastfm",
            "https://ws.audioscrobbler.com/2.0/",
            params={"method": "album.getInfo", "artist": "Cher", "album": "Believe", "api_key": api_key, "format": "json"},
            timeout=10,
            context="lastfm preflight",
            cache_ttl_sec=60,
        )
        if resp.status_code == 200:
            data = resp.json()
            if "error" in data and data.get("error") != 0:
                return False, f"Last.fm API error: {data.get('message', 'Unknown')}"
            return True, "Last.fm reachable"
        return False, f"Last.fm HTTP {resp.status_code}"
    except Exception as e:
        return False, f"Last.fm unreachable: {e}"


def _lastfm_credentials_effective() -> tuple[str, str]:
    api_key = str(_get_config_from_db("LASTFM_API_KEY", getattr(_runtime_module(), "LASTFM_API_KEY", "")) or "").strip()
    api_secret = str(_get_config_from_db("LASTFM_API_SECRET", getattr(_runtime_module(), "LASTFM_API_SECRET", "")) or "").strip()
    return api_key, api_secret


def _lastfm_session_name() -> str:
    return str(_get_config_from_db(_LASTFM_SESSION_NAME_SETTING, "") or "").strip()


def _lastfm_session_key() -> str:
    return _settings_db_get_secret(_LASTFM_SESSION_KEY_SETTING)


def _lastfm_pending_token() -> str:
    return _settings_db_get_secret(_LASTFM_AUTH_TOKEN_SETTING)


def _lastfm_has_stored_session_ciphertext() -> bool:
    raw = str(_get_config_from_db(_LASTFM_SESSION_KEY_SETTING, "") or "").strip()
    return bool(raw)


def _lastfm_has_stored_pending_ciphertext() -> bool:
    raw = str(_get_config_from_db(_LASTFM_AUTH_TOKEN_SETTING, "") or "").strip()
    return bool(raw)


def _lastfm_status_snapshot() -> dict[str, Any]:
    api_key, api_secret = _lastfm_credentials_effective()
    pending_token = _lastfm_pending_token()
    session_name = _lastfm_session_name()
    session_key = _lastfm_session_key()
    pending_cipher = _lastfm_has_stored_pending_ciphertext()
    session_cipher = _lastfm_has_stored_session_ciphertext()
    configured = bool(api_key and api_secret)
    connected = bool(configured and session_key)
    pending = bool(pending_token and not connected)
    corrupt_session = bool(configured and session_cipher and not session_key)
    corrupt_pending = bool(configured and pending_cipher and not pending_token and not connected)
    reconnect_required = bool(corrupt_session or corrupt_pending)
    auth_url = _lastfm_auth_url_for_token(pending_token) if pending_token and api_key else ""
    message = ""
    if reconnect_required:
        message = "Stored Last.fm authorization can no longer be decrypted. Reconnect Last.fm."
    elif pending:
        message = "Authorize PMDA on Last.fm. PMDA will complete the connection automatically."
    elif connected:
        message = f"Connected to Last.fm as {session_name or 'your account'}."
    elif configured:
        message = "Last.fm credentials are configured. Connect a user session to enable scrobbling."
    else:
        message = "Configure Last.fm API key and secret first."
    return {
        "configured": configured,
        "connected": connected,
        "pending": pending,
        "session_name": session_name or "",
        "auth_url": auth_url,
        "error": "",
        "reconnect_required": reconnect_required,
        "corrupt_session": corrupt_session,
        "message": message,
    }


def _lastfm_try_complete_pending_authorization_if_needed(*, enqueue_loved_sync: bool = False) -> dict[str, Any]:
    snapshot = _lastfm_status_snapshot()
    if bool(snapshot.get("connected")) or not bool(snapshot.get("pending")):
        return snapshot
    ok, message = _lastfm_try_complete_pending_authorization(enqueue_loved_sync=enqueue_loved_sync)
    refreshed = _lastfm_status_snapshot()
    if not ok and message:
        refreshed["message"] = message
    return refreshed


def _lastfm_store_session_payload(session: dict[str, Any]) -> tuple[bool, str]:
    if not isinstance(session, dict):
        return False, "Last.fm did not return a session"
    session_key = str(session.get("key") or "").strip()
    session_name = str(session.get("name") or "").strip()
    if not session_key:
        return False, "Last.fm session key is missing"
    _settings_db_set_secret(_LASTFM_SESSION_KEY_SETTING, session_key)
    _settings_db_set_value(_LASTFM_SESSION_NAME_SETTING, session_name)
    _settings_db_delete_keys(_LASTFM_AUTH_TOKEN_SETTING)
    if _get_config_from_db("LASTFM_SCROBBLE_ENABLED") is None:
        _settings_db_set_value("LASTFM_SCROBBLE_ENABLED", "1")
        try:
            _runtime_module().LASTFM_SCROBBLE_ENABLED = True
        except Exception:
            pass
    if _get_config_from_db("LASTFM_NOW_PLAYING_ENABLED") is None:
        _settings_db_set_value("LASTFM_NOW_PLAYING_ENABLED", "1")
        try:
            _runtime_module().LASTFM_NOW_PLAYING_ENABLED = True
        except Exception:
            pass
    return True, session_name


def _lastfm_try_complete_pending_authorization(*, enqueue_loved_sync: bool = False) -> tuple[bool, str]:
    api_key, api_secret = _lastfm_credentials_effective()
    if not api_key or not api_secret:
        return False, "Last.fm API key/secret are required"
    if _lastfm_session_key():
        return True, _lastfm_session_name()
    token = _lastfm_pending_token()
    if not token:
        return False, "No Last.fm authorization is pending."
    try:
        data = _lastfm_signed_post({"method": "auth.getSession", "token": token}, timeout=12.0)
        ok, session_name = _lastfm_store_session_payload(data.get("session") if isinstance(data, dict) else {})
        if not ok:
            return False, session_name
        if enqueue_loved_sync:
            try:
                uid = max(0, int(_current_user_id_or_zero() or 0))
                if uid > 0:
                    threading.Thread(
                        target=_lastfm_sync_loved_tracks_to_pmda,
                        args=(uid,),
                        kwargs={"force": True},
                        name="lastfm-loved-sync-auth",
                        daemon=True,
                    ).start()
            except Exception:
                logging.debug("Failed to enqueue Last.fm loved-track sync after auth", exc_info=True)
        logging.info("Last.fm authorization completed automatically for session=%s", session_name or "unknown")
        return True, session_name
    except Exception as exc:
        message = str(exc or "").strip() or "Last.fm authorization is not complete yet"
        lowered = message.lower()
        if "not been authorized" in lowered or "unauthorized token" in lowered:
            return False, "Authorize PMDA on Last.fm first."
        if "invalid method signature" in lowered or "invalid api key" in lowered:
            return False, message
        logging.info("Last.fm pending authorization not complete yet: %s", message)
        return False, message


def _lastfm_scrobble_enabled() -> bool:
    return bool(_parse_bool(_get_config_from_db("LASTFM_SCROBBLE_ENABLED", getattr(_runtime_module(), "LASTFM_SCROBBLE_ENABLED", False))))


def _lastfm_now_playing_enabled() -> bool:
    return bool(_parse_bool(_get_config_from_db("LASTFM_NOW_PLAYING_ENABLED", getattr(_runtime_module(), "LASTFM_NOW_PLAYING_ENABLED", False))))


def _lastfm_auth_callback_url(base_url: str) -> str:
    base = str(base_url or "").strip().rstrip("/")
    if not base:
        return ""
    return f"{base}/api/lastfm/auth/callback"


def _lastfm_auth_url_for_token(token: str, callback_url: str = "") -> str:
    api_key, _ = _lastfm_credentials_effective()
    url = f"{_LASTFM_AUTH_ROOT}?api_key={quote_plus(api_key)}&token={quote_plus(str(token or '').strip())}"
    cb = str(callback_url or "").strip()
    if cb:
        url += f"&cb={quote_plus(cb)}"
    return url


def _lastfm_callback_html(*, ok: bool, message: str, session_name: str = "") -> str:
    safe_message = html.escape(str(message or "").strip() or ("Last.fm connected." if ok else "Last.fm authorization failed."))
    safe_session_name = html.escape(str(session_name or "").strip())
    status = "connected" if ok else "error"
    title = "Last.fm connected" if ok else "Last.fm authorization failed"
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{html.escape(title)}</title>
    <style>
      :root {{
        color-scheme: dark light;
        font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      }}
      body {{
        margin: 0;
        min-height: 100vh;
        display: grid;
        place-items: center;
        background: #0b1020;
        color: #f5f7fb;
      }}
      .card {{
        width: min(92vw, 440px);
        padding: 24px;
        border-radius: 18px;
        background: rgba(16, 24, 40, 0.96);
        border: 1px solid rgba(148, 163, 184, 0.18);
        box-shadow: 0 24px 64px rgba(0, 0, 0, 0.35);
      }}
      h1 {{ margin: 0 0 10px; font-size: 1.2rem; }}
      p {{ margin: 0; line-height: 1.5; color: #cbd5e1; }}
      .hint {{ margin-top: 14px; font-size: 0.9rem; color: #94a3b8; }}
    </style>
  </head>
  <body>
    <div class="card">
      <h1>{html.escape(title)}</h1>
      <p>{safe_message}</p>
      <p class="hint">{'PMDA will return you automatically.' if ok else 'You can close this window and try again from PMDA settings.'}</p>
    </div>
    <script>
      (function() {{
        var payload = {{
          type: 'pmda:lastfm-auth-complete',
          status: {json.dumps(status)},
          ok: {json.dumps(bool(ok))},
          message: {json.dumps(str(message or "").strip())},
          session_name: {json.dumps(str(session_name or "").strip())}
        }};
        try {{
          if (window.opener && !window.opener.closed) {{
            window.opener.postMessage(payload, window.location.origin);
            setTimeout(function() {{ window.close(); }}, 150);
            return;
          }}
        }} catch (err) {{}}
        setTimeout(function() {{
          window.location.replace('/settings#settings-providers');
        }}, {250 if ok else 1500});
      }})();
    </script>
  </body>
</html>"""


def _lastfm_api_sig(params: dict[str, Any], api_secret: str) -> str:
    parts: list[str] = []
    for key in sorted(str(k) for k in params.keys()):
        if key in {"format", "callback", "api_sig"}:
            continue
        value = params.get(key)
        if value is None:
            continue
        parts.append(f"{key}{value}")
    parts.append(str(api_secret or ""))
    return hashlib.md5("".join(parts).encode("utf-8", errors="ignore")).hexdigest()


def _lastfm_signed_post(params: dict[str, Any], *, timeout: float = 15.0) -> dict:
    api_key, api_secret = _lastfm_credentials_effective()
    if not api_key or not api_secret:
        raise RuntimeError("Last.fm API key/secret are required")
    payload = {str(k): str(v) for k, v in (params or {}).items() if v not in (None, "")}
    payload["api_key"] = api_key
    payload["api_sig"] = _lastfm_api_sig(payload, api_secret)
    payload["format"] = "json"
    resp = requests.post(_LASTFM_API_ROOT, data=payload, timeout=timeout)
    data = resp.json() if resp.content else {}
    if resp.status_code != 200:
        message = ""
        if isinstance(data, dict):
            message = str(data.get("message") or data.get("error") or "").strip()
        raise RuntimeError(message or f"Last.fm HTTP {resp.status_code}")
    if isinstance(data, dict) and data.get("error"):
        raise RuntimeError(str(data.get("message") or f"Last.fm error {data.get('error')}").strip())
    return data if isinstance(data, dict) else {}


def _lastfm_get(params: dict[str, Any], *, timeout: float = 15.0) -> dict:
    api_key, _ = _lastfm_credentials_effective()
    if not api_key:
        raise RuntimeError("Last.fm API key is required")
    query = {str(k): str(v) for k, v in (params or {}).items() if v not in (None, "")}
    query["api_key"] = api_key
    query["format"] = "json"
    resp = requests.get(_LASTFM_API_ROOT, params=query, timeout=timeout)
    data = resp.json() if resp.content else {}
    if resp.status_code != 200:
        message = ""
        if isinstance(data, dict):
            message = str(data.get("message") or data.get("error") or "").strip()
        raise RuntimeError(message or f"Last.fm HTTP {resp.status_code}")
    if isinstance(data, dict) and data.get("error"):
        raise RuntimeError(str(data.get("message") or f"Last.fm error {data.get('error')}").strip())
    return data if isinstance(data, dict) else {}


def _lastfm_playback_track_payload(track_id: int) -> Optional[dict[str, Any]]:
    conn = _files_pg_connect()
    if conn is None:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COALESCE(tr.title, ''),
                    COALESCE(tr.duration_sec, 0),
                    COALESCE(tr.track_num, 0),
                    COALESCE(alb.title, ''),
                    COALESCE(ar.name, '')
                FROM files_tracks tr
                JOIN files_albums alb ON alb.id = tr.album_id
                JOIN files_artists ar ON ar.id = alb.artist_id
                WHERE tr.id = %s
                LIMIT 1
                """,
                (int(track_id),),
            )
            row = cur.fetchone()
        if not row:
            return None
        return {
            "track": str(row[0] or "").strip(),
            "duration_sec": int(row[1] or 0),
            "track_num": int(row[2] or 0),
            "album": str(row[3] or "").strip(),
            "artist": str(row[4] or "").strip(),
        }
    except Exception:
        logging.debug("Last.fm playback payload lookup failed for track_id=%s", track_id, exc_info=True)
        return None
    finally:
        conn.close()


def _lastfm_loved_sync_setting(user_id: int) -> str:
    return f"{_LASTFM_LOVED_SYNC_AT_PREFIX}{max(0, int(user_id or 0))}"


def _lastfm_track_identity_map() -> dict[tuple[str, str], int]:
    conn = _files_pg_connect()
    if conn is None:
        return {}
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT tr.id, COALESCE(tr.title, ''), COALESCE(ar.name, '')
                FROM files_tracks tr
                JOIN files_albums alb ON alb.id = tr.album_id
                JOIN files_artists ar ON ar.id = alb.artist_id
                """
            )
            rows = cur.fetchall() or []
    finally:
        conn.close()
    out: dict[tuple[str, str], int] = {}
    for row in rows:
        track_id = int(row[0] or 0)
        if track_id <= 0:
            continue
        title_norm = _normalize_identity_text_strict(str(row[1] or ""))
        artist_norm = _normalize_identity_text_strict(str(row[2] or ""))
        if not title_norm or not artist_norm:
            continue
        out.setdefault((artist_norm, title_norm), track_id)
    return out


def _lastfm_set_track_love(track_id: int, loved: bool) -> None:
    session_key = _lastfm_session_key()
    action = "track.love" if loved else "track.unlove"
    if not session_key:
        snapshot = _lastfm_try_complete_pending_authorization_if_needed(enqueue_loved_sync=True)
        session_key = _lastfm_session_key()
        if not session_key:
            logging.info(
                "Last.fm %s skipped for track_id=%s: session key missing%s",
                action,
                track_id,
                f" ({snapshot.get('message')})" if snapshot.get("message") else "",
            )
            return
    payload = _lastfm_playback_track_payload(track_id)
    if not payload or not payload.get("artist") or not payload.get("track"):
        logging.info("Last.fm %s skipped for track_id=%s: track payload unavailable", action, track_id)
        return
    try:
        _lastfm_signed_post(
            {
                "method": action,
                "sk": session_key,
                "artist": payload["artist"],
                "track": payload["track"],
            },
            timeout=10.0,
        )
        logging.info("Last.fm %s ok for track_id=%s artist=%s track=%s", action, track_id, payload.get("artist"), payload.get("track"))
    except Exception as exc:
        logging.info("Last.fm %s failed for track_id=%s: %s", action, track_id, exc)


def _lastfm_sync_loved_tracks_to_pmda(user_id: int, *, force: bool = False, max_pages: int = 5) -> dict[str, int]:
    uid = max(0, int(user_id or 0))
    if uid <= 0:
        return {"imported": 0, "matched": 0, "pages": 0}
    session_name = _lastfm_session_name()
    api_key, _ = _lastfm_credentials_effective()
    if not session_name or not api_key:
        return {"imported": 0, "matched": 0, "pages": 0}
    sync_key = _lastfm_loved_sync_setting(uid)
    if not force:
        try:
            last_sync = int(_get_config_from_db(sync_key, 0) or 0)
        except Exception:
            last_sync = 0
        if last_sync > 0 and (time.time() - last_sync) < 900:
            return {"imported": 0, "matched": 0, "pages": 0}
    loved_pairs: set[tuple[str, str]] = set()
    pages_fetched = 0
    for page in range(1, max(1, int(max_pages or 1)) + 1):
        data = _lastfm_get(
            {
                "method": "user.getLovedTracks",
                "user": session_name,
                "limit": 50,
                "page": page,
            },
            timeout=12.0,
        )
        lovedtracks = data.get("lovedtracks") if isinstance(data, dict) else {}
        raw_tracks = lovedtracks.get("track") if isinstance(lovedtracks, dict) else []
        if isinstance(raw_tracks, dict):
            raw_tracks = [raw_tracks]
        if not isinstance(raw_tracks, list):
            raw_tracks = []
        pages_fetched = page
        if not raw_tracks:
            break
        for item in raw_tracks:
            if not isinstance(item, dict):
                continue
            title_norm = _normalize_identity_text_strict(str(item.get("name") or ""))
            artist_info = item.get("artist")
            if isinstance(artist_info, dict):
                artist_name = str(artist_info.get("name") or artist_info.get("#text") or "").strip()
            else:
                artist_name = str(artist_info or "").strip()
            artist_norm = _normalize_identity_text_strict(artist_name)
            if artist_norm and title_norm:
                loved_pairs.add((artist_norm, title_norm))
        attr = lovedtracks.get("@attr") if isinstance(lovedtracks, dict) else {}
        total_pages = _parse_int_loose(attr.get("totalPages") if isinstance(attr, dict) else None, 1)
        if page >= max(1, total_pages):
            break
    _settings_db_set_value(sync_key, int(time.time()))
    if not loved_pairs:
        return {"imported": 0, "matched": 0, "pages": pages_fetched}
    identity_map = _lastfm_track_identity_map()
    matched_track_ids = sorted(
        {
            int(identity_map[pair])
            for pair in loved_pairs
            if pair in identity_map and int(identity_map[pair] or 0) > 0
        }
    )
    if not matched_track_ids:
        logging.info("Last.fm loved sync complete for user_id=%s: imported=0 matched=%s pages=%s", uid, len(loved_pairs), pages_fetched)
        return {"imported": 0, "matched": len(loved_pairs), "pages": pages_fetched}
    conn = _files_pg_connect()
    if conn is None:
        return {"imported": 0, "matched": len(loved_pairs), "pages": pages_fetched}
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                for track_id in matched_track_ids:
                    cur.execute(
                        """
                        INSERT INTO files_user_entity_likes(user_id, entity_type, entity_id, entity_key, liked, source, created_at, updated_at)
                        VALUES (%s, 'track', %s, NULL, TRUE, 'lastfm_import', NOW(), NOW())
                        ON CONFLICT (user_id, entity_type, entity_id, entity_key) DO UPDATE SET
                            liked = TRUE,
                            source = 'lastfm_import',
                            updated_at = NOW()
                        """,
                        (int(uid), int(track_id)),
                    )
        logging.info(
            "Last.fm loved sync complete for user_id=%s: imported=%s matched=%s pages=%s",
            uid,
            len(matched_track_ids),
            len(loved_pairs),
            pages_fetched,
        )
        return {"imported": len(matched_track_ids), "matched": len(loved_pairs), "pages": pages_fetched}
    finally:
        conn.close()


def _lastfm_scrobble_threshold_seconds(duration_sec: int) -> int:
    dur = max(0, int(duration_sec or 0))
    if dur <= 0:
        return 30
    return max(30, min(240, int(math.ceil(dur / 2.0))))


def _lastfm_submit_now_playing(track_id: int) -> None:
    if not _lastfm_now_playing_enabled():
        return
    session_key = _lastfm_session_key()
    if not session_key:
        snapshot = _lastfm_try_complete_pending_authorization_if_needed(enqueue_loved_sync=True)
        session_key = _lastfm_session_key()
        if not session_key:
            logging.info(
                "Last.fm now-playing skipped for track_id=%s: session key missing%s",
                track_id,
                f" ({snapshot.get('message')})" if snapshot.get("message") else "",
            )
            return
    payload = _lastfm_playback_track_payload(track_id)
    if not payload or not payload.get("artist") or not payload.get("track"):
        logging.info("Last.fm now-playing skipped for track_id=%s: track payload unavailable", track_id)
        return
    try:
        _lastfm_signed_post(
            {
                "method": "track.updateNowPlaying",
                "sk": session_key,
                "artist": payload["artist"],
                "track": payload["track"],
                "album": payload.get("album") or "",
                "trackNumber": payload.get("track_num") or "",
                "duration": payload.get("duration_sec") or "",
            },
            timeout=10.0,
        )
        logging.info("Last.fm now-playing ok for track_id=%s artist=%s track=%s", track_id, payload.get("artist"), payload.get("track"))
    except Exception as exc:
        logging.info("Last.fm now-playing update failed for track_id=%s: %s", track_id, exc)


def _lastfm_submit_scrobble(track_id: int, played_seconds: int) -> None:
    if not _lastfm_scrobble_enabled():
        return
    session_key = _lastfm_session_key()
    if not session_key:
        snapshot = _lastfm_try_complete_pending_authorization_if_needed(enqueue_loved_sync=True)
        session_key = _lastfm_session_key()
        if not session_key:
            logging.info(
                "Last.fm scrobble skipped for track_id=%s: session key missing%s",
                track_id,
                f" ({snapshot.get('message')})" if snapshot.get("message") else "",
            )
            return
    payload = _lastfm_playback_track_payload(track_id)
    if not payload or not payload.get("artist") or not payload.get("track"):
        logging.info("Last.fm scrobble skipped for track_id=%s: track payload unavailable", track_id)
        return
    duration_sec = int(payload.get("duration_sec") or 0)
    if duration_sec > 0 and played_seconds < _lastfm_scrobble_threshold_seconds(duration_sec):
        logging.info(
            "Last.fm scrobble skipped for track_id=%s: played=%ss threshold=%ss duration=%ss",
            track_id,
            played_seconds,
            _lastfm_scrobble_threshold_seconds(duration_sec),
            duration_sec,
        )
        return
    try:
        _lastfm_signed_post(
            {
                "method": "track.scrobble",
                "sk": session_key,
                "artist": payload["artist"],
                "track": payload["track"],
                "album": payload.get("album") or "",
                "trackNumber": payload.get("track_num") or "",
                "duration": duration_sec or "",
                "timestamp": max(1, int(time.time()) - max(0, int(played_seconds or 0))),
            },
            timeout=12.0,
        )
        logging.info(
            "Last.fm scrobble ok for track_id=%s artist=%s track=%s played=%ss",
            track_id,
            payload.get("artist"),
            payload.get("track"),
            played_seconds,
        )
    except Exception as exc:
        logging.info("Last.fm scrobble failed for track_id=%s: %s", track_id, exc)


def _lastfm_handle_playback_event_async(track_id: int, event_type: str, played_seconds: int) -> None:
    try:
        event = str(event_type or "").strip().lower()
        if event == "play_start":
            _lastfm_submit_now_playing(track_id)
            return
        if event in {"play_complete", "play_partial", "stop"}:
            _lastfm_submit_scrobble(track_id, max(0, int(played_seconds or 0)))
    except Exception:
        logging.debug("Last.fm playback hook failed", exc_info=True)


def _lastfm_pick_search_candidate(
    artist_name: str,
    album_title: str,
    album_rows: list[Any],
) -> tuple[str, str] | None:
    best_row: tuple[float, str, str] | None = None
    for idx, row in enumerate(album_rows[:5] if isinstance(album_rows, list) else []):
        if not isinstance(row, dict):
            continue
        search_artist = str(row.get("artist") or "").strip() or artist_name
        search_album = str(row.get("name") or row.get("title") or "").strip() or album_title
        title_score = _provider_identity_album_score(
            album_title,
            search_album,
            artist_hints=[artist_name, search_artist],
        )
        artist_score = _provider_identity_artist_score(artist_name, search_artist)
        combined = (title_score * 0.62) + (artist_score * 0.38)
        if _normalize_identity_album_strict(album_title) == _normalize_identity_album_strict(search_album):
            combined += 0.06
        combined -= min(0.05, idx * 0.01)
        if best_row is None or combined > best_row[0]:
            best_row = (combined, search_artist, search_album)
    if best_row is None:
        return None
    combined, search_artist, search_album = best_row
    title_score = _provider_identity_album_score(
        album_title,
        search_album,
        artist_hints=[artist_name, search_artist],
    )
    artist_score = _provider_identity_artist_score(artist_name, search_artist)
    if combined < 0.88 and not (title_score >= 0.92 and artist_score >= 0.74):
        return None
    return (search_artist, search_album)


def _lastfm_payload_has_album_page(payload: dict | None) -> bool:
    if not isinstance(payload, dict):
        return False
    url = str(payload.get("url") or "").strip()
    if url:
        return True
    if str(payload.get("wiki_summary") or payload.get("wiki_content") or "").strip():
        return True
    if int(payload.get("lastfm_scrobbles") or 0) > 0:
        return True
    if int(payload.get("lastfm_listeners") or 0) > 0:
        return True
    return False


def _fetch_lastfm_album_info(*args, **kwargs):
    return _public_album_providers_runtime._fetch_lastfm_album_info_for_runtime(_runtime_module(), *args, **kwargs)


def _cleanup_lastfm_bio_text(text: str) -> str:
    """Strip common Last.fm boilerplate from the end of bios/summaries."""
    t = _strip_html_text(text or "")
    if not t:
        return ""
    lowered = t.lower()
    # Fast path: remove common suffixes.
    for pat in _LASTFM_BIO_GARBAGE_PATTERNS:
        try:
            if re.search(pat, lowered, flags=re.IGNORECASE):
                t = re.sub(pat, "", t, flags=re.IGNORECASE).strip()
                lowered = t.lower()
        except Exception:
            continue
    # Remove any trailing "Read more on Last.fm" segment even when preceded by ellipsis.
    m = re.search(r"\bread more on last\.?fm\b", lowered)
    if m:
        t = t[: m.start()].strip(" .·\u2026")
    return t.strip()


def _fetch_lastfm_artist_info(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._fetch_lastfm_artist_info_for_runtime(_runtime_module(), *args, **kwargs)


def _lastfm_cover_url_candidates(cover_url: str) -> List[str]:
    """
    Expand a Last.fm image URL to likely larger variants.
    Last.fm often uses path chunks like /34s/, /64s/, /300x300/.
    """
    base = (cover_url or "").strip()
    if not base:
        return []
    candidates = [base]
    if re.search(r"/\d+s/", base):
        candidates.extend([
            re.sub(r"/\d+s/", "/300x300/", base),
            re.sub(r"/\d+s/", "/600x600/", base),
            re.sub(r"/\d+s/", "/1000x1000/", base),
        ])
    elif re.search(r"/\d+x\d+/", base):
        candidates.extend([
            re.sub(r"/\d+x\d+/", "/600x600/", base),
            re.sub(r"/\d+x\d+/", "/1000x1000/", base),
        ])
    return _dedupe_keep_order(candidates)


def _fetch_artist_image_lastfm(*args: Any, **kwargs: Any) -> Any:
    return _artist_profile_runtime._fetch_artist_image_lastfm_for_runtime(_runtime_module(), *args, **kwargs)


def api_lastfm_auth_status():
    snapshot = _lastfm_try_complete_pending_authorization_if_needed(enqueue_loved_sync=True)
    if snapshot.get("pending") and snapshot.get("auth_url") and has_request_context():
        base_url = request.url_root.rstrip("/")
        callback_url = _lastfm_auth_callback_url(base_url)
        token = _lastfm_pending_token()
        if token:
            snapshot["auth_url"] = _lastfm_auth_url_for_token(token, callback_url)
    return jsonify(snapshot)


def api_lastfm_auth_start():
    api_key, api_secret = _lastfm_credentials_effective()
    if not api_key or not api_secret:
        return jsonify({"ok": False, "message": "Configure Last.fm API key and secret first."}), 400
    try:
        # If an old encrypted token/session is no longer decryptable, clear it before starting a new auth flow.
        snapshot = _lastfm_status_snapshot()
        if snapshot.get("reconnect_required"):
            _settings_db_delete_keys(_LASTFM_AUTH_TOKEN_SETTING, _LASTFM_SESSION_KEY_SETTING, _LASTFM_SESSION_NAME_SETTING)
        data = _lastfm_signed_post({"method": "auth.getToken"}, timeout=12.0)
        token = str((data.get("token") if isinstance(data, dict) else "") or "").strip()
        if not token:
            raise RuntimeError("Last.fm did not return an auth token")
        _settings_db_set_secret(_LASTFM_AUTH_TOKEN_SETTING, token)
        callback_url = _lastfm_auth_callback_url(request.url_root.rstrip("/"))
        return jsonify({"ok": True, "pending": True, "token": token, "auth_url": _lastfm_auth_url_for_token(token, callback_url)})
    except Exception as exc:
        logging.warning("Last.fm auth start failed: %s", exc)
        return jsonify({"ok": False, "message": str(exc) or "Last.fm authorization failed"}), 500


def api_lastfm_auth_callback():
    api_key, api_secret = _lastfm_credentials_effective()
    if not api_key or not api_secret:
        html_body = _lastfm_callback_html(ok=False, message="Configure Last.fm API key and secret first.")
        return Response(html_body, status=400, mimetype="text/html")
    token_from_query = str(request.args.get("token") or "").strip()
    if token_from_query and not _lastfm_pending_token():
        try:
            _settings_db_set_secret(_LASTFM_AUTH_TOKEN_SETTING, token_from_query)
        except Exception:
            logging.debug("Failed to restore Last.fm pending token from callback query", exc_info=True)
    try:
        ok, message = _lastfm_try_complete_pending_authorization(enqueue_loved_sync=True)
        if ok:
            session_name = _lastfm_session_name()
            html_body = _lastfm_callback_html(
                ok=True,
                message=f"Connected to Last.fm as {session_name or 'your account'}.",
                session_name=session_name,
            )
            return Response(html_body, status=200, mimetype="text/html")
        html_body = _lastfm_callback_html(ok=False, message=message or "Last.fm authorization is not complete yet.")
        return Response(html_body, status=409, mimetype="text/html")
    except Exception as exc:
        message = str(exc or "").strip() or "Last.fm authorization failed"
        logging.warning("Last.fm auth callback failed: %s", exc)
        html_body = _lastfm_callback_html(ok=False, message=message)
        return Response(html_body, status=500, mimetype="text/html")


def api_lastfm_auth_complete():
    api_key, api_secret = _lastfm_credentials_effective()
    if not api_key or not api_secret:
        return jsonify({"ok": False, "message": "Configure Last.fm API key and secret first."}), 400
    token = _lastfm_pending_token()
    if not token:
        return jsonify({"ok": False, "message": "No Last.fm authorization is pending."}), 409
    try:
        ok, message = _lastfm_try_complete_pending_authorization(enqueue_loved_sync=True)
        if not ok:
            lowered = str(message or "").strip().lower()
            if "authorize pmda on last.fm first" in lowered or "no last.fm authorization is pending" in lowered:
                return jsonify({"ok": False, "message": message or "Authorize PMDA on Last.fm first."}), 409
            if "invalid method signature" in lowered or "invalid api key" in lowered:
                return jsonify({"ok": False, "message": message}), 400
            return jsonify({"ok": False, "message": message or "Last.fm authorization is not complete yet"}), 409
        session_name = _lastfm_session_name()
        return jsonify({"ok": True, "connected": True, "session_name": session_name, "message": f"Connected to Last.fm as {session_name or 'your account'}"})
    except Exception as exc:
        message = str(exc or "").strip() or "Last.fm authorization is not complete yet"
        lowered = message.lower()
        if "not been authorized" in lowered or "unauthorized token" in lowered:
            return jsonify({"ok": False, "message": "Authorize PMDA on Last.fm first."}), 409
        if "invalid method signature" in lowered or "invalid api key" in lowered:
            return jsonify({"ok": False, "message": message}), 400
        logging.warning("Last.fm auth completion failed: %s", exc)
        return jsonify({"ok": False, "message": message}), 500


def api_lastfm_auth_disconnect():
    try:
        _settings_db_delete_keys(_LASTFM_AUTH_TOKEN_SETTING, _LASTFM_SESSION_KEY_SETTING, _LASTFM_SESSION_NAME_SETTING)
        return jsonify({"ok": True, "message": "Last.fm disconnected"})
    except Exception as exc:
        return jsonify({"ok": False, "message": str(exc) or "Failed to disconnect Last.fm"}), 500

_ORIGINAL_EXTRACTED_FUNCTIONS = {
    '_run_lastfm_preflight': _run_lastfm_preflight,
    '_lastfm_credentials_effective': _lastfm_credentials_effective,
    '_lastfm_session_name': _lastfm_session_name,
    '_lastfm_session_key': _lastfm_session_key,
    '_lastfm_pending_token': _lastfm_pending_token,
    '_lastfm_has_stored_session_ciphertext': _lastfm_has_stored_session_ciphertext,
    '_lastfm_has_stored_pending_ciphertext': _lastfm_has_stored_pending_ciphertext,
    '_lastfm_status_snapshot': _lastfm_status_snapshot,
    '_lastfm_try_complete_pending_authorization_if_needed': _lastfm_try_complete_pending_authorization_if_needed,
    '_lastfm_store_session_payload': _lastfm_store_session_payload,
    '_lastfm_try_complete_pending_authorization': _lastfm_try_complete_pending_authorization,
    '_lastfm_scrobble_enabled': _lastfm_scrobble_enabled,
    '_lastfm_now_playing_enabled': _lastfm_now_playing_enabled,
    '_lastfm_auth_callback_url': _lastfm_auth_callback_url,
    '_lastfm_auth_url_for_token': _lastfm_auth_url_for_token,
    '_lastfm_callback_html': _lastfm_callback_html,
    '_lastfm_api_sig': _lastfm_api_sig,
    '_lastfm_signed_post': _lastfm_signed_post,
    '_lastfm_get': _lastfm_get,
    '_lastfm_playback_track_payload': _lastfm_playback_track_payload,
    '_lastfm_loved_sync_setting': _lastfm_loved_sync_setting,
    '_lastfm_track_identity_map': _lastfm_track_identity_map,
    '_lastfm_set_track_love': _lastfm_set_track_love,
    '_lastfm_sync_loved_tracks_to_pmda': _lastfm_sync_loved_tracks_to_pmda,
    '_lastfm_scrobble_threshold_seconds': _lastfm_scrobble_threshold_seconds,
    '_lastfm_submit_now_playing': _lastfm_submit_now_playing,
    '_lastfm_submit_scrobble': _lastfm_submit_scrobble,
    '_lastfm_handle_playback_event_async': _lastfm_handle_playback_event_async,
    '_lastfm_pick_search_candidate': _lastfm_pick_search_candidate,
    '_lastfm_payload_has_album_page': _lastfm_payload_has_album_page,
    '_fetch_lastfm_album_info': _fetch_lastfm_album_info,
    '_cleanup_lastfm_bio_text': _cleanup_lastfm_bio_text,
    '_fetch_lastfm_artist_info': _fetch_lastfm_artist_info,
    '_lastfm_cover_url_candidates': _lastfm_cover_url_candidates,
    '_fetch_artist_image_lastfm': _fetch_artist_image_lastfm,
    'api_lastfm_auth_status': api_lastfm_auth_status,
    'api_lastfm_auth_start': api_lastfm_auth_start,
    'api_lastfm_auth_callback': api_lastfm_auth_callback,
    'api_lastfm_auth_complete': api_lastfm_auth_complete,
    'api_lastfm_auth_disconnect': api_lastfm_auth_disconnect,
}

def _run_lastfm_preflight_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _run_lastfm_preflight(*args, **kwargs)

def _lastfm_credentials_effective_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_credentials_effective(*args, **kwargs)

def _lastfm_session_name_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_session_name(*args, **kwargs)

def _lastfm_session_key_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_session_key(*args, **kwargs)

def _lastfm_pending_token_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_pending_token(*args, **kwargs)

def _lastfm_has_stored_session_ciphertext_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_has_stored_session_ciphertext(*args, **kwargs)

def _lastfm_has_stored_pending_ciphertext_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_has_stored_pending_ciphertext(*args, **kwargs)

def _lastfm_status_snapshot_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_status_snapshot(*args, **kwargs)

def _lastfm_try_complete_pending_authorization_if_needed_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_try_complete_pending_authorization_if_needed(*args, **kwargs)

def _lastfm_store_session_payload_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_store_session_payload(*args, **kwargs)

def _lastfm_try_complete_pending_authorization_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_try_complete_pending_authorization(*args, **kwargs)

def _lastfm_scrobble_enabled_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_scrobble_enabled(*args, **kwargs)

def _lastfm_now_playing_enabled_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_now_playing_enabled(*args, **kwargs)

def _lastfm_auth_callback_url_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_auth_callback_url(*args, **kwargs)

def _lastfm_auth_url_for_token_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_auth_url_for_token(*args, **kwargs)

def _lastfm_callback_html_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_callback_html(*args, **kwargs)

def _lastfm_api_sig_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_api_sig(*args, **kwargs)

def _lastfm_signed_post_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_signed_post(*args, **kwargs)

def _lastfm_get_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_get(*args, **kwargs)

def _lastfm_playback_track_payload_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_playback_track_payload(*args, **kwargs)

def _lastfm_loved_sync_setting_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_loved_sync_setting(*args, **kwargs)

def _lastfm_track_identity_map_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_track_identity_map(*args, **kwargs)

def _lastfm_set_track_love_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_set_track_love(*args, **kwargs)

def _lastfm_sync_loved_tracks_to_pmda_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_sync_loved_tracks_to_pmda(*args, **kwargs)

def _lastfm_scrobble_threshold_seconds_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_scrobble_threshold_seconds(*args, **kwargs)

def _lastfm_submit_now_playing_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_submit_now_playing(*args, **kwargs)

def _lastfm_submit_scrobble_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_submit_scrobble(*args, **kwargs)

def _lastfm_handle_playback_event_async_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_handle_playback_event_async(*args, **kwargs)

def _lastfm_pick_search_candidate_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_pick_search_candidate(*args, **kwargs)

def _lastfm_payload_has_album_page_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_payload_has_album_page(*args, **kwargs)

def _fetch_lastfm_album_info_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_lastfm_album_info(*args, **kwargs)

def _cleanup_lastfm_bio_text_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _cleanup_lastfm_bio_text(*args, **kwargs)

def _fetch_lastfm_artist_info_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_lastfm_artist_info(*args, **kwargs)

def _lastfm_cover_url_candidates_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _lastfm_cover_url_candidates(*args, **kwargs)

def _fetch_artist_image_lastfm_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_artist_image_lastfm(*args, **kwargs)

def api_lastfm_auth_status_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_lastfm_auth_status(*args, **kwargs)

def api_lastfm_auth_start_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_lastfm_auth_start(*args, **kwargs)

def api_lastfm_auth_callback_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_lastfm_auth_callback(*args, **kwargs)

def api_lastfm_auth_complete_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_lastfm_auth_complete(*args, **kwargs)

def api_lastfm_auth_disconnect_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return api_lastfm_auth_disconnect(*args, **kwargs)
