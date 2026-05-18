"""Authentication, profile, and admin user management API routes."""

from __future__ import annotations

import logging
import sqlite3
from typing import Any

from flask import Blueprint, g, jsonify, request


def create_auth_admin_blueprint(*, runtime: Any) -> Blueprint:
    """Create auth and admin-user routes while preserving the public API."""

    blueprint = Blueprint("pmda_auth_admin", __name__)

    @blueprint.get("/api/auth/bootstrap/status", endpoint="api_auth_bootstrap_status")
    def api_auth_bootstrap_status():
        return jsonify({"bootstrap_required": bool(runtime._auth_bootstrap_required())})

    @blueprint.post("/api/auth/bootstrap", endpoint="api_auth_bootstrap")
    def api_auth_bootstrap():
        if runtime.AUTH_DISABLE:
            return jsonify({"error": "Auth is disabled by PMDA_AUTH_DISABLE"}), 400
        if not runtime._auth_bootstrap_required():
            return jsonify({"error": "Bootstrap already completed"}), 409
        client_ip = runtime._auth_client_ip().lower()
        username_hint = str((request.get_json(silent=True) or {}).get("username") or "")
        if not runtime.AUTH_ALLOW_PUBLIC_BOOTSTRAP and not runtime._auth_ip_is_private_or_loopback(client_ip):
            runtime._auth_security_event(
                "bootstrap_blocked",
                ip=client_ip,
                username=username_hint,
                path="/api/auth/bootstrap",
                outcome="blocked",
                reason="public_bootstrap_disabled",
            )
            return jsonify({"error": "Bootstrap is restricted to private/local network addresses."}), 403
        ip_banned, ip_retry_after = runtime._auth_persistent_ip_block_info(client_ip)
        if ip_banned:
            runtime._auth_security_event(
                "bootstrap_blocked",
                ip=client_ip,
                username=username_hint,
                path="/api/auth/bootstrap",
                outcome="blocked",
                reason="persistent_ip_ban",
            )
            return runtime._auth_retry_response(
                "Too many failed bootstrap attempts. This IP is temporarily blocked.",
                retry_after_sec=ip_retry_after,
            )
        if runtime._auth_rate_limit_is_blocked(
            "bootstrap_ip",
            client_ip,
            max_attempts=runtime.AUTH_BOOTSTRAP_RATE_LIMIT_IP_MAX_ATTEMPTS,
        ):
            banned_now, ban_retry_after, _ = runtime._auth_record_persistent_ip_failure(
                client_ip,
                username=username_hint,
                path="/api/auth/bootstrap",
                reason="rate_limit_ip",
            )
            runtime._auth_security_event(
                "bootstrap_blocked",
                ip=client_ip,
                username=username_hint,
                path="/api/auth/bootstrap",
                outcome="blocked",
                reason="persistent_ip_ban" if banned_now else "rate_limit_ip",
            )
            retry_after = ban_retry_after if banned_now else runtime.AUTH_LOGIN_RATE_LIMIT_WINDOW_SEC
            message = (
                "Too many failed bootstrap attempts. This IP is temporarily blocked."
                if banned_now
                else "Too many bootstrap attempts. Please wait and retry."
            )
            return runtime._auth_retry_response(message, retry_after_sec=retry_after)

        data = request.get_json(silent=True) or {}
        username = str(data.get("username") or "").strip()
        password = str(data.get("password") or "")
        password_confirm = str(data.get("password_confirm") or password)
        if password != password_confirm:
            runtime._auth_rate_limit_record_failure("bootstrap_ip", client_ip)
            runtime._auth_record_persistent_ip_failure(
                client_ip,
                username=username,
                path="/api/auth/bootstrap",
                reason="password_mismatch",
            )
            runtime._auth_security_event(
                "bootstrap_failed",
                ip=client_ip,
                username=username,
                path="/api/auth/bootstrap",
                outcome="failed",
                reason="password_mismatch",
            )
            return jsonify({"error": "Passwords do not match"}), 400
        ok, msg, user = runtime._auth_create_user(
            username=username,
            password=password,
            is_admin=True,
            can_download=True,
            can_view_statistics=True,
            is_active=True,
        )
        if not ok:
            runtime._auth_rate_limit_record_failure("bootstrap_ip", client_ip)
            runtime._auth_record_persistent_ip_failure(
                client_ip,
                username=username,
                path="/api/auth/bootstrap",
                reason=str(msg or "create_user_failed"),
            )
            runtime._auth_security_event(
                "bootstrap_failed",
                ip=client_ip,
                username=username,
                path="/api/auth/bootstrap",
                outcome="failed",
                reason=str(msg or "create_user_failed"),
            )
            return jsonify({"error": msg}), 400
        runtime._auth_rate_limit_clear("bootstrap_ip", client_ip)
        runtime._auth_clear_persistent_ip_failures(client_ip)
        runtime._auth_security_event(
            "bootstrap_success",
            ip=client_ip,
            username=username,
            path="/api/auth/bootstrap",
            outcome="ok",
            reason="admin_created",
            level=logging.INFO,
        )
        return jsonify({"ok": True, "message": "Admin account created", "user": user})

    @blueprint.post("/api/auth/login", endpoint="api_auth_login")
    def api_auth_login():
        if runtime.AUTH_DISABLE:
            return jsonify({"error": "Auth is disabled by PMDA_AUTH_DISABLE"}), 400
        if runtime._auth_bootstrap_required():
            return jsonify({"error": "Bootstrap required. Create the initial admin user first."}), 428
        data = request.get_json(silent=True) or {}
        username = str(data.get("username") or "").strip()
        password = str(data.get("password") or "")
        client_ip = runtime._auth_client_ip().lower()
        username_key = username.lower()
        remember_me = bool(runtime._parse_bool(data.get("remember_me")))

        ip_banned, ip_retry_after = runtime._auth_persistent_ip_block_info(client_ip)
        if ip_banned:
            runtime._auth_security_event(
                "login_blocked",
                ip=client_ip,
                username=username,
                outcome="blocked",
                reason="persistent_ip_ban",
            )
            return runtime._auth_retry_response(
                "Too many failed login attempts. This IP is temporarily blocked.",
                retry_after_sec=ip_retry_after,
            )
        if runtime._auth_rate_limit_is_blocked(
            "login_ip",
            client_ip,
            max_attempts=runtime.AUTH_LOGIN_RATE_LIMIT_IP_MAX_ATTEMPTS,
        ):
            banned_now, ban_retry_after, _ = runtime._auth_record_persistent_ip_failure(
                client_ip,
                username=username,
                path="/api/auth/login",
                reason="rate_limit_ip",
            )
            runtime._auth_security_event(
                "login_blocked",
                ip=client_ip,
                username=username,
                outcome="blocked",
                reason="persistent_ip_ban" if banned_now else "rate_limit_ip",
            )
            retry_after = ban_retry_after if banned_now else runtime.AUTH_LOGIN_RATE_LIMIT_WINDOW_SEC
            message = (
                "Too many failed login attempts. This IP is temporarily blocked."
                if banned_now
                else "Too many failed login attempts. Please wait and retry."
            )
            return runtime._auth_retry_response(message, retry_after_sec=retry_after)
        if username_key and runtime._auth_rate_limit_is_blocked(
            "login_user",
            username_key,
            max_attempts=runtime.AUTH_LOGIN_RATE_LIMIT_USER_MAX_ATTEMPTS,
        ):
            banned_now, ban_retry_after, _ = runtime._auth_record_persistent_ip_failure(
                client_ip,
                username=username,
                path="/api/auth/login",
                reason="rate_limit_user",
            )
            runtime._auth_security_event(
                "login_blocked",
                ip=client_ip,
                username=username,
                outcome="blocked",
                reason="persistent_ip_ban" if banned_now else "rate_limit_user",
            )
            retry_after = ban_retry_after if banned_now else runtime.AUTH_LOGIN_RATE_LIMIT_WINDOW_SEC
            message = (
                "Too many failed login attempts. This IP is temporarily blocked."
                if banned_now
                else "Too many failed login attempts. Please wait and retry."
            )
            return runtime._auth_retry_response(message, retry_after_sec=retry_after)
        if not username or not password:
            runtime._auth_security_event(
                "login_invalid_request",
                ip=client_ip,
                username=username,
                outcome="failed",
                reason="username_or_password_missing",
            )
            return jsonify({"error": "username and password are required"}), 400

        row = runtime._auth_get_user_by_username(username)
        if not row:
            runtime._auth_rate_limit_record_failure("login_ip", client_ip)
            runtime._auth_rate_limit_record_failure("login_user", username_key)
            banned_now, _, _ = runtime._auth_record_persistent_ip_failure(
                client_ip,
                username=username,
                path="/api/auth/login",
                reason="invalid_credentials",
            )
            runtime._auth_apply_failure_delay()
            runtime._auth_security_event(
                "login_failed",
                ip=client_ip,
                username=username,
                outcome="failed",
                reason="persistent_ip_ban" if banned_now else "invalid_credentials",
            )
            return jsonify({"error": "Invalid credentials"}), 401
        if not bool(int(row["is_active"] or 0)):
            runtime._auth_rate_limit_record_failure("login_ip", client_ip)
            runtime._auth_rate_limit_record_failure("login_user", username_key)
            banned_now, _, _ = runtime._auth_record_persistent_ip_failure(
                client_ip,
                username=username,
                path="/api/auth/login",
                reason="inactive_user",
            )
            runtime._auth_apply_failure_delay()
            runtime._auth_security_event(
                "login_failed",
                ip=client_ip,
                username=username,
                outcome="failed",
                reason="persistent_ip_ban" if banned_now else "inactive_user",
            )
            return jsonify({"error": "Invalid credentials"}), 401
        if not runtime._auth_verify_password(password, str(row["password_hash"] or ""), str(row["password_salt"] or "")):
            runtime._auth_rate_limit_record_failure("login_ip", client_ip)
            runtime._auth_rate_limit_record_failure("login_user", username_key)
            banned_now, _, _ = runtime._auth_record_persistent_ip_failure(
                client_ip,
                username=username,
                path="/api/auth/login",
                reason="invalid_credentials",
            )
            runtime._auth_apply_failure_delay()
            runtime._auth_security_event(
                "login_failed",
                ip=client_ip,
                username=username,
                outcome="failed",
                reason="persistent_ip_ban" if banned_now else "invalid_credentials",
            )
            return jsonify({"error": "Invalid credentials"}), 401

        session_ttl = runtime.AUTH_SESSION_REMEMBER_TTL_SEC if remember_me else runtime.AUTH_SESSION_TTL_SEC
        token, expires_at = runtime._auth_create_session(
            int(row["id"]),
            ip=runtime._auth_client_ip(),
            user_agent=str(request.headers.get("User-Agent") or ""),
            ttl_sec=session_ttl,
        )
        runtime._auth_touch_login(int(row["id"]), ip=runtime._auth_client_ip())
        runtime._auth_rate_limit_clear("login_ip", client_ip)
        runtime._auth_rate_limit_clear("login_user", username_key)
        runtime._auth_clear_persistent_ip_failures(client_ip)
        runtime._auth_security_event(
            "login_success",
            ip=client_ip,
            username=username,
            outcome="ok",
            reason="remember_me" if remember_me else "session",
            level=logging.INFO,
        )
        user = runtime._auth_public_user(runtime._auth_get_user_by_id(int(row["id"])))
        response = jsonify(
            {
                "ok": True,
                "token": token,
                "expires_at": int(expires_at),
                "remember_me": bool(remember_me),
                "user": user,
            }
        )
        cookie_kwargs = {
            "httponly": True,
            "secure": bool(runtime.AUTH_SESSION_COOKIE_SECURE),
            "samesite": runtime.AUTH_SESSION_COOKIE_SAMESITE,
            "path": "/",
        }
        if remember_me:
            cookie_kwargs["max_age"] = int(session_ttl)
        response.set_cookie(runtime.AUTH_SESSION_COOKIE_NAME, token, **cookie_kwargs)
        return response

    @blueprint.get("/api/auth/me", endpoint="api_auth_me")
    def api_auth_me():
        user = dict(getattr(g, "current_user", {}) or {})
        if not user:
            return jsonify({"error": "Not authenticated"}), 401
        return jsonify({"ok": True, "user": user})

    @blueprint.put("/api/auth/profile", endpoint="api_auth_profile_update")
    def api_auth_profile_update():
        user = dict(getattr(g, "current_user", {}) or {})
        uid = int(user.get("id") or 0)
        if uid <= 0:
            return jsonify({"error": "Not authenticated"}), 401
        data = request.get_json(silent=True) or {}
        if not isinstance(data, dict):
            data = {}

        updates: list[str] = []
        params: list[Any] = []

        if "accept_shares" in data:
            updates.append("accept_shares = ?")
            params.append(1 if bool(runtime._parse_bool(data.get("accept_shares"))) else 0)
        if "share_liked_public" in data:
            updates.append("share_liked_public = ?")
            params.append(1 if bool(runtime._parse_bool(data.get("share_liked_public"))) else 0)
        if "share_recommendations_public" in data:
            updates.append("share_recommendations_public = ?")
            params.append(1 if bool(runtime._parse_bool(data.get("share_recommendations_public"))) else 0)
        if "avatar_data_url" in data:
            ok_avatar, avatar_data_url, avatar_err = runtime._auth_validate_avatar_data_url(data.get("avatar_data_url"))
            if not ok_avatar:
                return jsonify({"error": avatar_err}), 400
            updates.append("avatar_data_url = ?")
            params.append(avatar_data_url)
        if "concerts_filter_enabled" in data:
            updates.append("concerts_filter_enabled = ?")
            params.append(1 if runtime._auth_normalize_concerts_filter_enabled(data.get("concerts_filter_enabled")) else 0)
        if "concerts_home_lat" in data:
            updates.append("concerts_home_lat = ?")
            params.append(runtime._auth_normalize_concerts_coord(data.get("concerts_home_lat"), axis="lat"))
        if "concerts_home_lon" in data:
            updates.append("concerts_home_lon = ?")
            params.append(runtime._auth_normalize_concerts_coord(data.get("concerts_home_lon"), axis="lon"))
        if "concerts_radius_km" in data:
            updates.append("concerts_radius_km = ?")
            params.append(runtime._auth_normalize_concerts_radius_km(data.get("concerts_radius_km")))

        if not updates:
            row = runtime._auth_get_user_by_id(uid)
            return jsonify({"ok": True, "user": runtime._auth_public_user(row)})

        con = runtime._auth_db_connect()
        try:
            cur = con.cursor()
            updates.append("updated_at = ?")
            params.append(runtime._auth_now_ts())
            params.append(uid)
            cur.execute(f"UPDATE auth_users SET {', '.join(updates)} WHERE id = ?", tuple(params))
            con.commit()
        except Exception as exc:
            try:
                con.rollback()
            except Exception:
                pass
            return jsonify({"error": f"Profile update failed: {exc}"}), 500
        finally:
            con.close()

        raw_token = runtime._auth_get_bearer_token()
        if raw_token:
            runtime._auth_session_cache_drop(runtime._auth_token_hash(raw_token))
        row = runtime._auth_get_user_by_id(uid)
        updated_user = runtime._auth_public_user(row)
        g.current_user = updated_user
        return jsonify({"ok": True, "user": updated_user})

    @blueprint.put("/api/auth/password", endpoint="api_auth_password_update")
    def api_auth_password_update():
        user = dict(getattr(g, "current_user", {}) or {})
        uid = int(user.get("id") or 0)
        if uid <= 0:
            return jsonify({"error": "Not authenticated"}), 401

        data = request.get_json(silent=True) or {}
        if not isinstance(data, dict):
            data = {}

        current_password = str(data.get("current_password") or "")
        new_password = str(data.get("new_password") or "")
        new_password_confirm = str(data.get("new_password_confirm") or "")
        username = str(user.get("username") or "")
        client_ip = runtime._auth_client_ip()

        if not current_password or not new_password or not new_password_confirm:
            return jsonify({"error": "Current password, new password, and confirmation are required"}), 400
        if new_password != new_password_confirm:
            return jsonify({"error": "Passwords do not match"}), 400

        con = runtime._auth_db_connect()
        try:
            cur = con.cursor()
            cur.execute(
                """
                SELECT id, username, password_hash, password_salt
                FROM auth_users
                WHERE id = ?
                LIMIT 1
                """,
                (uid,),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "User not found"}), 404
            if not runtime._auth_verify_password(
                current_password,
                str(row["password_hash"] or ""),
                str(row["password_salt"] or ""),
            ):
                runtime._auth_security_event(
                    "password_change_failed",
                    ip=client_ip,
                    username=username,
                    path="/api/auth/password",
                    outcome="failed",
                    reason="current_password_invalid",
                )
                return jsonify({"error": "Current password is incorrect"}), 401

            ok_password, password_msg = runtime._auth_validate_password(new_password)
            if not ok_password:
                return jsonify({"error": password_msg}), 400

            password_hash, password_salt = runtime._auth_make_password_hash(new_password)
            cur.execute(
                """
                UPDATE auth_users
                SET password_hash = ?, password_salt = ?, updated_at = ?
                WHERE id = ?
                """,
                (password_hash, password_salt, runtime._auth_now_ts(), uid),
            )
            cur.execute("DELETE FROM auth_sessions WHERE user_id = ?", (uid,))
            con.commit()
        except Exception as exc:
            try:
                con.rollback()
            except Exception:
                pass
            return jsonify({"error": f"Password update failed: {exc}"}), 500
        finally:
            con.close()

        raw_token = runtime._auth_get_bearer_token()
        if raw_token:
            runtime._auth_session_cache_drop(runtime._auth_token_hash(raw_token))
        runtime._auth_security_event(
            "password_changed",
            ip=client_ip,
            username=username,
            path="/api/auth/password",
            outcome="success",
            reason="sessions_revoked",
            level=logging.INFO,
        )
        response = jsonify({"ok": True, "reauth_required": True})
        response.delete_cookie(
            runtime.AUTH_SESSION_COOKIE_NAME,
            path="/",
            secure=bool(runtime.AUTH_SESSION_COOKIE_SECURE),
            httponly=True,
            samesite=runtime.AUTH_SESSION_COOKIE_SAMESITE,
        )
        return response

    @blueprint.post("/api/auth/logout", endpoint="api_auth_logout")
    def api_auth_logout():
        raw_token = runtime._auth_get_bearer_token()
        if raw_token:
            runtime._auth_delete_session(raw_token)
        response = jsonify({"ok": True})
        response.delete_cookie(
            runtime.AUTH_SESSION_COOKIE_NAME,
            path="/",
            secure=bool(runtime.AUTH_SESSION_COOKIE_SECURE),
            httponly=True,
            samesite=runtime.AUTH_SESSION_COOKIE_SAMESITE,
        )
        return response

    @blueprint.get("/api/admin/users", endpoint="api_admin_users_get")
    def api_admin_users_get():
        if runtime.AUTH_DISABLE:
            return jsonify({"users": []})
        con = runtime._auth_db_connect()
        try:
            cur = con.cursor()
            cur.execute(
                """
                SELECT id, username, is_admin, can_download, can_view_statistics, allow_ai_calls, is_active, accept_shares, share_liked_public, share_recommendations_public, avatar_data_url, concerts_filter_enabled, concerts_home_lat, concerts_home_lon, concerts_radius_km, created_at, updated_at, last_login_at
                FROM auth_users
                ORDER BY is_admin DESC, username ASC
                """
            )
            rows = cur.fetchall()
            return jsonify({"users": [runtime._auth_public_user(row) for row in rows]})
        finally:
            con.close()

    @blueprint.post("/api/admin/users", endpoint="api_admin_users_create")
    def api_admin_users_create():
        if runtime.AUTH_DISABLE:
            return jsonify({"error": "Auth is disabled by PMDA_AUTH_DISABLE"}), 400
        data = request.get_json(silent=True) or {}
        username = str(data.get("username") or "").strip()
        password = str(data.get("password") or "")
        password_confirm = str(data.get("password_confirm") or password)
        if password != password_confirm:
            return jsonify({"error": "Passwords do not match"}), 400
        ok, msg, user = runtime._auth_create_user(
            username=username,
            password=password,
            is_admin=bool(runtime._parse_bool(data.get("is_admin"))),
            can_download=bool(runtime._parse_bool(data.get("can_download"))),
            can_view_statistics=bool(runtime._parse_bool(data.get("can_view_statistics"))),
            allow_ai_calls=True if data.get("allow_ai_calls") is None else bool(runtime._parse_bool(data.get("allow_ai_calls"))),
            is_active=True if data.get("is_active") is None else bool(runtime._parse_bool(data.get("is_active"))),
        )
        if not ok:
            return jsonify({"error": msg}), 400
        return jsonify({"ok": True, "user": user})

    @blueprint.put("/api/admin/users/<int:user_id>", endpoint="api_admin_users_update")
    def api_admin_users_update(user_id: int):
        if runtime.AUTH_DISABLE:
            return jsonify({"error": "Auth is disabled by PMDA_AUTH_DISABLE"}), 400
        data = request.get_json(silent=True) or {}
        con = runtime._auth_db_connect()
        try:
            cur = con.cursor()
            cur.execute(
                """
                SELECT id, username, password_hash, password_salt, is_admin, can_download, can_view_statistics, allow_ai_calls, is_active, accept_shares, share_liked_public, share_recommendations_public, avatar_data_url, concerts_filter_enabled, concerts_home_lat, concerts_home_lon, concerts_radius_km, created_at, updated_at, last_login_at
                FROM auth_users
                WHERE id = ?
                LIMIT 1
                """,
                (int(user_id),),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "User not found"}), 404

            username = str(row["username"] or "")
            if "username" in data:
                ok_u, u_msg = runtime._auth_validate_username(data.get("username"))
                if not ok_u:
                    return jsonify({"error": u_msg}), 400
                username = u_msg

            is_admin = bool(int(row["is_admin"] or 0))
            can_download = bool(int(row["can_download"] or 0))
            can_view_statistics = bool(int(row["can_view_statistics"] or 0))
            allow_ai_calls = bool(int(row["allow_ai_calls"] or 1))
            is_active = bool(int(row["is_active"] or 0))

            if "is_admin" in data:
                is_admin = bool(runtime._parse_bool(data.get("is_admin")))
            if "can_download" in data:
                can_download = bool(runtime._parse_bool(data.get("can_download")))
            if "can_view_statistics" in data:
                can_view_statistics = bool(runtime._parse_bool(data.get("can_view_statistics")))
            if "allow_ai_calls" in data:
                allow_ai_calls = bool(runtime._parse_bool(data.get("allow_ai_calls")))
            if "is_active" in data:
                is_active = bool(runtime._parse_bool(data.get("is_active")))

            current_user = dict(getattr(g, "current_user", {}) or {})
            current_user_id = int(current_user.get("id") or 0)

            if bool(int(row["is_admin"] or 0)) and (not is_admin or not is_active):
                admin_count = runtime._auth_count_admin_users(con)
                if admin_count <= 1:
                    return jsonify({"error": "At least one active admin user is required"}), 400
            if current_user_id > 0 and int(row["id"]) == current_user_id and not is_admin:
                return jsonify({"error": "You cannot remove your own admin role"}), 400

            password_hash = str(row["password_hash"] or "")
            password_salt = str(row["password_salt"] or "")
            password_changed = False
            if "password" in data:
                password = str(data.get("password") or "")
                if password:
                    ok_p, p_msg = runtime._auth_validate_password(password)
                    if not ok_p:
                        return jsonify({"error": p_msg}), 400
                    password_hash, password_salt = runtime._auth_make_password_hash(password)
                    password_changed = True

            try:
                cur.execute(
                    """
                    UPDATE auth_users
                    SET
                        username = ?,
                        password_hash = ?,
                        password_salt = ?,
                        is_admin = ?,
                        can_download = ?,
                        can_view_statistics = ?,
                        allow_ai_calls = ?,
                        is_active = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        username,
                        password_hash,
                        password_salt,
                        1 if is_admin else 0,
                        1 if can_download else 0,
                        1 if can_view_statistics else 0,
                        1 if allow_ai_calls else 0,
                        1 if is_active else 0,
                        runtime._auth_now_ts(),
                        int(user_id),
                    ),
                )
                if password_changed:
                    cur.execute("DELETE FROM auth_sessions WHERE user_id = ?", (int(user_id),))
                con.commit()
                runtime._auth_session_cache_clear()
            except sqlite3.IntegrityError:
                con.rollback()
                return jsonify({"error": "Username already exists"}), 400

            cur.execute(
                """
                SELECT id, username, is_admin, can_download, can_view_statistics, allow_ai_calls, is_active, accept_shares, share_liked_public, share_recommendations_public, avatar_data_url, concerts_filter_enabled, concerts_home_lat, concerts_home_lon, concerts_radius_km, created_at, updated_at, last_login_at
                FROM auth_users
                WHERE id = ?
                LIMIT 1
                """,
                (int(user_id),),
            )
            updated = cur.fetchone()
            return jsonify({"ok": True, "user": runtime._auth_public_user(updated)})
        finally:
            con.close()

    @blueprint.delete("/api/admin/users/<int:user_id>", endpoint="api_admin_users_delete")
    def api_admin_users_delete(user_id: int):
        if runtime.AUTH_DISABLE:
            return jsonify({"error": "Auth is disabled by PMDA_AUTH_DISABLE"}), 400

        current_user = dict(getattr(g, "current_user", {}) or {})
        current_user_id = int(current_user.get("id") or 0)

        con = runtime._auth_db_connect()
        try:
            cur = con.cursor()
            cur.execute(
                """
                SELECT id, username, is_admin, is_active
                FROM auth_users
                WHERE id = ?
                LIMIT 1
                """,
                (int(user_id),),
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "User not found"}), 404

            target_id = int(row["id"] or 0)
            target_is_admin = bool(int(row["is_admin"] or 0))
            target_is_active = bool(int(row["is_active"] or 0))
            target_username = str(row["username"] or "")

            if current_user_id > 0 and target_id == current_user_id:
                return jsonify({"error": "You cannot delete your own account"}), 400

            if target_is_admin and target_is_active:
                admin_count = runtime._auth_count_admin_users(con)
                if admin_count <= 1:
                    return jsonify({"error": "At least one active admin user is required"}), 400

            cur.execute("DELETE FROM auth_users WHERE id = ?", (target_id,))
            con.commit()
            return jsonify({"ok": True, "deleted_user_id": target_id, "username": target_username})
        finally:
            con.close()

    return blueprint
