from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any

import requests


@dataclass(slots=True)
class DeviceStartResult:
    ok: bool
    session_id: str = ""
    verification_url: str = ""
    user_code: str = ""
    interval: int = 5
    message: str = ""
    warning: str = ""


@dataclass(slots=True)
class DevicePollResult:
    status: str
    message: str = ""
    retry_after: int = 0
    api_key_saved: bool | None = None


@dataclass(slots=True)
class OAuthStatusResult:
    connected: bool
    provider_id: str
    auth_mode: str
    account_id: str = ""
    expires_at: int | None = None
    expires_in_sec: int | None = None
    has_refresh_token: bool = False
    metadata: dict[str, Any] | None = None


class OpenAIAuthService:
    def __init__(
        self,
        *,
        settings_db_path: str | Path,
        issuer: str,
        client_id: str,
        encrypt: callable,
        decrypt: callable,
        apply_api_key: callable,
        set_legacy_refresh: callable,
        get_legacy_refresh: callable,
        clear_derived_api_key: callable,
        get_user_id: callable,
    ) -> None:
        self.settings_db_path = Path(settings_db_path)
        self.issuer = str(issuer or "https://auth.openai.com").rstrip("/")
        self.client_id = str(client_id or "").strip()
        self.encrypt = encrypt
        self.decrypt = decrypt
        self.apply_api_key = apply_api_key
        self.set_legacy_refresh = set_legacy_refresh
        self.get_legacy_refresh = get_legacy_refresh
        self.clear_derived_api_key = clear_derived_api_key
        self.get_user_id = get_user_id
        self._lock = Lock()
        self._sessions: dict[str, dict[str, Any]] = {}

    def _effective_user_id(self, user_id: int | None) -> int:
        if user_id is not None and int(user_id) > 0:
            return int(user_id)
        try:
            resolved = int(self.get_user_id() or 0)
        except Exception:
            resolved = 0
        return max(0, resolved)

    def _conn(self) -> sqlite3.Connection:
        con = sqlite3.connect(str(self.settings_db_path), timeout=15)
        con.row_factory = sqlite3.Row
        try:
            con.execute("PRAGMA journal_mode=WAL;")
            con.execute("PRAGMA busy_timeout=30000;")
            con.execute("PRAGMA synchronous=NORMAL;")
            con.execute("PRAGMA temp_store=MEMORY;")
        except Exception:
            pass
        return con

    @staticmethod
    def _http_error_message(prefix: str, resp: requests.Response) -> str:
        status = int(getattr(resp, "status_code", 0) or 0)
        detail = ""
        try:
            payload = resp.json() if resp.content else {}
        except Exception:
            payload = {}
        if isinstance(payload, dict):
            detail = str(
                payload.get("error_description")
                or payload.get("error")
                or payload.get("message")
                or ""
            ).strip()
        if not detail:
            try:
                detail = str((resp.text or "").strip())
            except Exception:
                detail = ""
        if detail:
            detail = detail.replace("\n", " ").strip()
            if len(detail) > 220:
                detail = detail[:220].rstrip() + "..."
            return f"{prefix} (status {status}): {detail}"
        return f"{prefix} (status {status})"

    def _save_profile(
        self,
        *,
        user_id: int,
        provider_id: str,
        mode: str,
        account_id: str,
        access_token: str,
        refresh_token: str,
        expires_at: int | None,
        meta: dict[str, Any] | None,
    ) -> None:
        meta_s = json.dumps(meta or {}, ensure_ascii=False)
        with self._conn() as con:
            cur = con.cursor()
            cur.execute(
                """
                UPDATE ai_auth_profiles
                SET is_active = 0, updated_at = ?
                WHERE user_id = ? AND provider_id = ? AND is_active = 1
                """,
                (int(time.time()), int(user_id), str(provider_id)),
            )
            cur.execute(
                """
                INSERT INTO ai_auth_profiles
                (user_id, provider_id, mode, account_id, access_token_enc, refresh_token_enc, expires_at, meta_json, is_active, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (
                    int(user_id),
                    str(provider_id),
                    str(mode),
                    str(account_id or ""),
                    str(self.encrypt(access_token) if access_token else ""),
                    str(self.encrypt(refresh_token) if refresh_token else ""),
                    int(expires_at) if expires_at else None,
                    meta_s,
                    int(time.time()),
                    int(time.time()),
                ),
            )
            con.commit()

    def _save_profile_with_retry(self, *, retries: int = 3, retry_sleep: float = 0.15, **kwargs: Any) -> None:
        last_exc: Exception | None = None
        for attempt in range(max(1, int(retries))):
            try:
                self._save_profile(**kwargs)
                return
            except sqlite3.OperationalError as exc:
                last_exc = exc
                msg = str(exc or "").lower()
                if ("locked" not in msg and "busy" not in msg) or attempt >= max(1, int(retries)) - 1:
                    raise
                time.sleep(max(0.01, float(retry_sleep)))
            except Exception as exc:
                last_exc = exc
                raise
        if last_exc is not None:
            raise last_exc

    def _active_profile(self, user_id: int, provider_id: str) -> dict[str, Any] | None:
        with self._conn() as con:
            cur = con.cursor()
            cur.execute(
                """
                SELECT id, user_id, provider_id, mode, account_id, access_token_enc, refresh_token_enc,
                       expires_at, meta_json, is_active, created_at, updated_at
                FROM ai_auth_profiles
                WHERE user_id = ? AND provider_id = ? AND is_active = 1
                ORDER BY updated_at DESC, id DESC
                LIMIT 1
                """,
                (int(user_id), str(provider_id)),
            )
            row = cur.fetchone()
            if not row:
                return None
            out = dict(row)
            try:
                out["meta"] = json.loads(str(out.get("meta_json") or "{}"))
            except Exception:
                out["meta"] = {}
            return out

    def _latest_active_profile(self, provider_id: str) -> dict[str, Any] | None:
        with self._conn() as con:
            cur = con.cursor()
            cur.execute(
                """
                SELECT id, user_id, provider_id, mode, account_id, access_token_enc, refresh_token_enc,
                       expires_at, meta_json, is_active, created_at, updated_at
                FROM ai_auth_profiles
                WHERE provider_id = ? AND is_active = 1
                ORDER BY updated_at DESC, id DESC
                LIMIT 1
                """,
                (str(provider_id),),
            )
            row = cur.fetchone()
            if not row:
                return None
            out = dict(row)
            try:
                out["meta"] = json.loads(str(out.get("meta_json") or "{}"))
            except Exception:
                out["meta"] = {}
            return out

    def _refresh_access_token(self, refresh_token: str) -> dict[str, Any]:
        token_url = f"{self.issuer}/oauth/token"
        resp = requests.post(
            token_url,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": self.client_id,
            },
            timeout=20,
        )
        if not resp.ok:
            raise RuntimeError(self._http_error_message("OpenAI OAuth refresh failed", resp))
        return resp.json() if resp.content else {}

    def _meta_with_tokens(self, meta: dict[str, Any] | None, *, id_token: str = "") -> dict[str, Any]:
        payload = dict(meta or {})
        tok = str(id_token or "").strip()
        if tok:
            try:
                payload["id_token_enc"] = str(self.encrypt(tok) or "")
            except Exception:
                logging.debug("Failed to encrypt OpenAI OAuth id_token", exc_info=True)
        return payload

    def start_device_flow(self, user_id: int | None = None) -> DeviceStartResult:
        _ = self._effective_user_id(user_id)
        url = f"{self.issuer}/api/accounts/deviceauth/usercode"
        resp = requests.post(url, json={"client_id": self.client_id}, timeout=15)
        if not resp.ok:
            return DeviceStartResult(ok=False, message=f"OpenAI device-code start failed (status {resp.status_code})")
        data = resp.json() if resp.content else {}
        device_auth_id = str(data.get("device_auth_id") or "").strip()
        user_code = str(data.get("user_code") or data.get("usercode") or "").strip()
        if not device_auth_id or not user_code:
            return DeviceStartResult(ok=False, message="Device auth response missing required fields")
        interval = 5
        try:
            interval = max(1, min(30, int(data.get("interval") or 5)))
        except Exception:
            interval = 5
        session_id = uuid.uuid4().hex
        with self._lock:
            self._sessions[session_id] = {
                "user_id": self._effective_user_id(user_id),
                "status": "pending",
                "created_at": time.time(),
                "last_poll_at": 0.0,
                "device_auth_id": device_auth_id,
                "user_code": user_code,
                "interval": interval,
            }
        return DeviceStartResult(
            ok=True,
            session_id=session_id,
            verification_url=f"{self.issuer}/codex/device",
            user_code=user_code,
            interval=interval,
            message="Enter the code in the OpenAI page, then return here.",
            warning="ChatGPT subscription and API billing are separate.",
        )

    def _exchange_authorization_code(self, authorization_code: str, code_verifier: str) -> dict[str, Any]:
        token_url = f"{self.issuer}/oauth/token"
        redirect_uri = f"{self.issuer}/deviceauth/callback"
        resp = requests.post(
            token_url,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "authorization_code",
                "code": authorization_code,
                "redirect_uri": redirect_uri,
                "client_id": self.client_id,
                "code_verifier": code_verifier,
            },
            timeout=20,
        )
        if not resp.ok:
            msg = self._http_error_message("OpenAI token exchange failed", resp)
            logging.warning("[OpenAI OAuth] authorization_code exchange failed: %s", msg)
            raise RuntimeError(msg)
        return resp.json() if resp.content else {}

    def _exchange_id_token_for_api_key(self, id_token: str) -> str:
        token_url = f"{self.issuer}/oauth/token"
        resp = requests.post(
            token_url,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
                "client_id": self.client_id,
                "requested_token": "openai-api-key",
                "subject_token": id_token,
                "subject_token_type": "urn:ietf:params:oauth:token-type:id_token",
            },
            timeout=20,
        )
        if not resp.ok:
            raise RuntimeError(self._http_error_message("OpenAI API key exchange failed", resp))
        data = resp.json() if resp.content else {}
        return str(data.get("access_token") or "").strip()

    def poll_device_flow(self, session_id: str, user_id: int | None = None) -> DevicePollResult:
        sid = str(session_id or "").strip()
        if not sid:
            return DevicePollResult(status="error", message="session_id is required")
        with self._lock:
            sess = self._sessions.get(sid)
            if not isinstance(sess, dict):
                return DevicePollResult(status="error", message="Unknown session_id")
            sess_status = str(sess.get("status") or "").strip().lower()
            if sess_status == "completed":
                return DevicePollResult(status="completed", message="Connected")
            if sess_status == "completing":
                return DevicePollResult(status="pending", message="Finalizing authorization…", retry_after=1)
            if sess_status == "error":
                return DevicePollResult(status="error", message=str(sess.get("error") or "OAuth flow failed"))

            now = time.time()
            if now - float(sess.get("created_at") or 0) > 15 * 60:
                sess["status"] = "error"
                sess["error"] = "Device auth timed out"
                sess["polling"] = False
                return DevicePollResult(status="error", message="Device auth timed out")

            if bool(sess.get("polling")):
                return DevicePollResult(status="pending", message="Checking authorization…", retry_after=1)

            retry_after = int((float(sess.get("last_poll_at") or 0) + int(sess.get("interval") or 5)) - now)
            if retry_after > 0:
                return DevicePollResult(status="pending", message="Waiting for authorization…", retry_after=retry_after)

            sess["last_poll_at"] = now
            sess["polling"] = True
            device_auth_id = str(sess.get("device_auth_id") or "")
            user_code = str(sess.get("user_code") or "")
            sess_user_id = sess.get("user_id")

        poll_url = f"{self.issuer}/api/accounts/deviceauth/token"
        try:
            resp = requests.post(
                poll_url,
                json={
                    "device_auth_id": device_auth_id,
                    "user_code": user_code,
                },
                timeout=15,
            )
            if resp.status_code in (403, 404):
                return DevicePollResult(status="pending", message="Waiting for authorization…")
            if resp.status_code in (429, 500, 502, 503, 504):
                return DevicePollResult(
                    status="pending",
                    message=f"OpenAI OAuth temporary error (status {resp.status_code}), retrying…",
                    retry_after=max(2, int(self._sessions.get(sid, {}).get("interval") or 5)),
                )
            if not resp.ok:
                msg = self._http_error_message("OpenAI device-code poll failed", resp)
                with self._lock:
                    sess_err = self._sessions.get(sid)
                    if isinstance(sess_err, dict):
                        sess_err["status"] = "error"
                        sess_err["error"] = msg
                return DevicePollResult(status="error", message=msg)

            payload = resp.json() if resp.content else {}
            authorization_code = str(payload.get("authorization_code") or "").strip()
            code_verifier = str(payload.get("code_verifier") or "").strip()
            if not authorization_code or not code_verifier:
                return DevicePollResult(status="pending", message="Waiting for authorization…", retry_after=2)

            try:
                tokens = self._exchange_authorization_code(authorization_code, code_verifier)
            except Exception as exc:
                msg = str(exc) or "OAuth exchange failed"
                lowered = msg.lower()
                recovered = False
                if "invalid_grant" in lowered or "already" in lowered or "expired" in lowered:
                    try:
                        eff_uid = self._effective_user_id(user_id if user_id is not None else sess_user_id)
                        existing = self._active_profile(eff_uid, "openai-codex")
                        if not existing and eff_uid != 0:
                            existing = self._active_profile(0, "openai-codex")
                        if isinstance(existing, dict):
                            updated_at = int(existing.get("updated_at") or 0)
                            if updated_at > 0 and (int(time.time()) - updated_at) <= 900:
                                recovered = True
                    except Exception:
                        recovered = False
                if recovered:
                    with self._lock:
                        sess_done = self._sessions.get(sid)
                        if isinstance(sess_done, dict):
                            sess_done["status"] = "completed"
                            sess_done["error"] = ""
                            sess_done["polling"] = False
                    return DevicePollResult(status="completed", message="Connected")
                with self._lock:
                    sess_now = self._sessions.get(sid)
                    # If another overlapping poll already completed, keep success.
                    if isinstance(sess_now, dict) and str(sess_now.get("status") or "").strip().lower() == "completed":
                        return DevicePollResult(status="completed", message="Connected")
                    if isinstance(sess_now, dict):
                        sess_now["status"] = "error"
                        sess_now["error"] = msg
                return DevicePollResult(status="error", message=msg)

            access_token = str(tokens.get("access_token") or "").strip()
            refresh_token = str(tokens.get("refresh_token") or "").strip()
            id_token = str(tokens.get("id_token") or "").strip()
            account_id = str(tokens.get("account_id") or tokens.get("sub") or "").strip()
            expires_in = 0
            try:
                expires_in = int(tokens.get("expires_in") or 0)
            except Exception:
                expires_in = 0
            expires_at = int(time.time()) + max(0, expires_in)

            eff_user_id = self._effective_user_id(user_id if user_id is not None else sess_user_id)
            with self._lock:
                sess_now = self._sessions.get(sid)
                if isinstance(sess_now, dict):
                    sess_now["status"] = "completing"
            try:
                self._save_profile_with_retry(
                    user_id=eff_user_id,
                    provider_id="openai-codex",
                    mode="oauth",
                    account_id=account_id,
                    access_token=access_token,
                    refresh_token=refresh_token,
                    expires_at=expires_at,
                    meta=self._meta_with_tokens(
                        {"issuer": self.issuer, "client_id": self.client_id},
                        id_token=id_token,
                    ),
                )
            except Exception as exc:
                msg = str(exc) or "OAuth profile save failed"
                with self._lock:
                    sess_fail = self._sessions.get(sid)
                    if isinstance(sess_fail, dict):
                        sess_fail["status"] = "error"
                        sess_fail["error"] = msg
                        sess_fail["polling"] = False
                return DevicePollResult(status="error", message=msg)

            if refresh_token:
                # Legacy indicator for compatibility with old UI modes.
                self.set_legacy_refresh(refresh_token)

            with self._lock:
                sess_done = self._sessions.get(sid)
                if isinstance(sess_done, dict):
                    sess_done["status"] = "completed"
                    sess_done["error"] = ""
                    sess_done["polling"] = False
                    sess_done["user_id"] = eff_user_id

            return DevicePollResult(
                status="completed",
                message="Connected",
                api_key_saved=False,
            )
        finally:
            with self._lock:
                sess_final = self._sessions.get(sid)
                if isinstance(sess_final, dict) and str(sess_final.get("status") or "").strip().lower() in {"pending", "completing"}:
                    sess_final["polling"] = False

    def get_valid_access_token(self, user_id: int | None, provider_id: str = "openai-codex") -> str:
        return self.get_valid_access_token_for_runtime(user_id, provider_id=provider_id, ensure_runtime_key=False)

    def get_valid_access_token_for_runtime(
        self,
        user_id: int | None,
        *,
        provider_id: str = "openai-codex",
        ensure_runtime_key: bool = False,
    ) -> str:
        tokens = self.get_runtime_tokens(
            user_id,
            provider_id=provider_id,
            require_id_token=False,
            ensure_runtime_key=ensure_runtime_key,
        )
        return str(tokens.get("access_token") or "").strip()

    def get_runtime_tokens(
        self,
        user_id: int | None,
        *,
        provider_id: str = "openai-codex",
        require_id_token: bool = False,
        ensure_runtime_key: bool = False,
    ) -> dict[str, Any]:
        uid = self._effective_user_id(user_id)
        profile = self._active_profile(uid, provider_id)
        if not profile:
            if uid != 0:
                profile = self._active_profile(0, provider_id)
            else:
                profile = self._latest_active_profile(provider_id)
        if not profile:
            raise RuntimeError("No active OpenAI Codex OAuth profile")

        access_token = str(self.decrypt(str(profile.get("access_token_enc") or "")) or "").strip()
        refresh_token = str(self.decrypt(str(profile.get("refresh_token_enc") or "")) or "").strip()
        profile_had_encrypted_refresh = bool(str(profile.get("refresh_token_enc") or "").strip())
        meta = profile.get("meta") if isinstance(profile.get("meta"), dict) else {}
        id_token = ""
        try:
            id_token = str(self.decrypt(str(meta.get("id_token_enc") or "")) or "").strip()
        except Exception:
            id_token = ""
        if not id_token:
            id_token = str(meta.get("id_token") or "").strip()
        if not refresh_token:
            try:
                refresh_token = str(self.get_legacy_refresh() or "").strip()
            except Exception:
                refresh_token = ""
        expires_at = int(profile.get("expires_at") or 0)
        now = int(time.time())
        force_refresh = bool((ensure_runtime_key or (require_id_token and not id_token)) and refresh_token)
        if access_token and expires_at > now + 120 and not force_refresh:
            return {
                "access_token": access_token,
                "refresh_token": refresh_token,
                "id_token": id_token,
                "account_id": str(profile.get("account_id") or ""),
                "expires_at": expires_at,
                "provider_id": str(profile.get("provider_id") or provider_id),
                "user_id": int(profile.get("user_id") or uid),
                "meta": meta,
            }

        if not refresh_token:
            if access_token and expires_at > now + 30:
                return {
                    "access_token": access_token,
                    "refresh_token": refresh_token,
                    "id_token": id_token,
                    "account_id": str(profile.get("account_id") or ""),
                    "expires_at": expires_at,
                    "provider_id": str(profile.get("provider_id") or provider_id),
                    "user_id": int(profile.get("user_id") or uid),
                    "meta": meta,
                }
            if profile_had_encrypted_refresh:
                raise RuntimeError("OpenAI Codex OAuth profile is present but its refresh token cannot be decrypted; reconnect Codex OAuth")
            raise RuntimeError("OpenAI Codex access token expired and no refresh token is available")

        refreshed = self._refresh_access_token(refresh_token)
        new_access = str(refreshed.get("access_token") or "").strip()
        new_refresh = str(refreshed.get("refresh_token") or refresh_token).strip()
        new_id_token = str(refreshed.get("id_token") or id_token).strip()
        if not new_access:
            raise RuntimeError("OpenAI OAuth refresh returned empty access token")
        expires_in = 0
        try:
            expires_in = int(refreshed.get("expires_in") or 0)
        except Exception:
            expires_in = 0
        new_expires_at = int(time.time()) + max(0, expires_in)
        self._save_profile_with_retry(
            user_id=int(profile.get("user_id") or uid),
            provider_id=str(profile.get("provider_id") or provider_id),
            mode=str(profile.get("mode") or "oauth"),
            account_id=str(profile.get("account_id") or ""),
            access_token=new_access,
            refresh_token=new_refresh,
            expires_at=new_expires_at,
            meta=self._meta_with_tokens(meta, id_token=new_id_token),
        )
        if new_refresh:
            try:
                self.set_legacy_refresh(new_refresh)
            except Exception:
                logging.debug("Failed to persist refreshed OPENAI_OAUTH_REFRESH_TOKEN", exc_info=True)
        if new_id_token and ensure_runtime_key:
            try:
                api_key = self._exchange_id_token_for_api_key(new_id_token)
                if api_key:
                    self.apply_api_key(api_key)
            except Exception as exc:
                logging.info("[OpenAI OAuth] refresh id_token -> API key exchange skipped: %s", exc)
        return {
            "access_token": new_access,
            "refresh_token": new_refresh,
            "id_token": new_id_token,
            "account_id": str(profile.get("account_id") or ""),
            "expires_at": new_expires_at,
            "provider_id": str(profile.get("provider_id") or provider_id),
            "user_id": int(profile.get("user_id") or uid),
            "meta": self._meta_with_tokens(meta, id_token=new_id_token),
        }

    def disconnect(self, user_id: int | None, provider_id: str = "openai-codex") -> None:
        uid = self._effective_user_id(user_id)
        now = int(time.time())
        with self._conn() as con:
            cur = con.cursor()
            cur.execute(
                """
                UPDATE ai_auth_profiles
                SET is_active = 0, updated_at = ?
                WHERE provider_id = ? AND user_id IN (?, 0) AND is_active = 1
                """,
                (now, str(provider_id), uid),
            )
            # Keep API-key mode untouched, but clear legacy OAuth refresh marker.
            cur.execute("DELETE FROM settings WHERE key = 'OPENAI_OAUTH_REFRESH_TOKEN'")
            con.commit()
        try:
            self.clear_derived_api_key()
        except Exception:
            logging.debug("Failed to clear derived OpenAI Codex runtime key", exc_info=True)

    def status(self, user_id: int | None, provider_id: str = "openai-codex") -> OAuthStatusResult:
        uid = self._effective_user_id(user_id)
        profile = self._active_profile(uid, provider_id) or (self._active_profile(0, provider_id) if uid != 0 else None)
        if not profile and uid == 0:
            profile = self._latest_active_profile(provider_id)
        if not profile:
            return OAuthStatusResult(connected=False, provider_id=provider_id, auth_mode="none")
        expires_at = int(profile.get("expires_at") or 0) or None
        now = int(time.time())
        expires_in = max(0, expires_at - now) if expires_at else None
        metadata = profile.get("meta") if isinstance(profile.get("meta"), dict) else {}
        metadata_public = {
            str(k): v
            for k, v in dict(metadata or {}).items()
            if str(k) not in {"id_token", "id_token_enc", "access_token", "refresh_token"}
        }
        return OAuthStatusResult(
            connected=True,
            provider_id=provider_id,
            auth_mode="oauth",
            account_id=str(profile.get("account_id") or ""),
            expires_at=expires_at,
            expires_in_sec=expires_in,
            has_refresh_token=bool(str(profile.get("refresh_token_enc") or "").strip()),
            metadata=metadata_public,
        )


__all__ = [
    "OpenAIAuthService",
    "DeviceStartResult",
    "DevicePollResult",
    "OAuthStatusResult",
]
