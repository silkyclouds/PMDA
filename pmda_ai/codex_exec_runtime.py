"""Codex CLI execution helpers for PMDA AI providers."""

from __future__ import annotations

import base64
import json
import os
import subprocess
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, List, Optional
from urllib.parse import urlparse

import requests


def openai_codex_token_health_for_runtime(
    runtime: Any,
    user_id: int | None = None,
    *,
    force_refresh: bool = False,
) -> tuple[bool, str]:
    uid = int(user_id or 0)
    now_ts = time.time()
    health_lock = runtime._openai_codex_health_lock
    health_cache = runtime._openai_codex_health_cache
    ttl_ok = float(getattr(runtime, "_OPENAI_CODEX_HEALTH_TTL_OK_SEC", 60.0) or 60.0)
    ttl_err = float(getattr(runtime, "_OPENAI_CODEX_HEALTH_TTL_ERR_SEC", 15.0) or 15.0)
    status_timeout = max(1.0, float(getattr(runtime, "PMDA_OPENAI_CODEX_STATUS_TIMEOUT_SEC", 5.0) or 5.0))

    if not force_refresh:
        with health_lock:
            cached = health_cache.get(uid)
            if cached and float(cached[0]) > now_ts:
                return bool(cached[1]), str(cached[2] or "")
    if not runtime._openai_codex_profile_present(uid):
        reason = "No active OpenAI Codex OAuth profile"
        with health_lock:
            health_cache[uid] = (float(now_ts + ttl_err), False, reason)
        return False, reason
    if not runtime._codex_cli_available():
        reason = "Codex CLI is not installed in the PMDA runtime"
        with health_lock:
            health_cache[uid] = (float(now_ts + ttl_err), False, reason)
        return False, reason
    try:
        runtime_tokens_fn = getattr(runtime, "_openai_codex_runtime_tokens", None)
        if not callable(runtime_tokens_fn):
            raise RuntimeError("OpenAI Codex runtime is still initializing")
        bounded_runner = getattr(runtime, "_run_callable_bounded", None)
        if callable(bounded_runner):
            tokens = bounded_runner(
                runtime_tokens_fn,
                uid,
                force_refresh=bool(force_refresh),
                require_id_token=True,
                timeout_sec=status_timeout,
                log_prefix="[OpenAI Codex status]",
            )
        else:
            tokens = runtime_tokens_fn(
                uid,
                force_refresh=bool(force_refresh),
                require_id_token=True,
            )
        ok = bool(str((tokens or {}).get("access_token") or "").strip())
        reason = "" if ok else "OpenAI Codex OAuth access token is unavailable; reconnect Codex OAuth"
    except TimeoutError:
        ok = False
        reason = f"OpenAI Codex status check timed out after {int(status_timeout)}s"
    except Exception as exc:
        ok = False
        reason = str(exc or "").strip() or "OpenAI Codex OAuth token is unavailable"
    ttl = ttl_ok if ok else ttl_err
    with health_lock:
        health_cache[uid] = (float(now_ts + ttl), bool(ok), str(reason or ""))
    return bool(ok), str(reason or "")


def openai_codex_connected_for_runtime(
    runtime: Any,
    user_id: int | None = None,
    *,
    require_token: bool = False,
) -> bool:
    if not require_token:
        return runtime._openai_codex_profile_present(user_id)
    ok, _reason = openai_codex_token_health_for_runtime(runtime, user_id, force_refresh=False)
    return bool(ok)


def openai_usage_dict_from_codex(raw_usage: dict[str, Any] | None) -> dict[str, Any]:
    usage = dict(raw_usage or {})
    input_tokens = int(usage.get("input_tokens") or 0)
    cached_input_tokens = int(usage.get("cached_input_tokens") or 0)
    output_tokens = int(usage.get("output_tokens") or 0)
    total_tokens = int(usage.get("total_tokens") or 0)
    if total_tokens <= 0:
        total_tokens = max(0, input_tokens + output_tokens)
    return {
        "input_tokens": input_tokens,
        "prompt_tokens": input_tokens,
        "completion_tokens": output_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "prompt_tokens_details": {"cached_tokens": cached_input_tokens},
    }


def codex_extract_final_text(stdout_text: str) -> tuple[str, dict[str, Any], str | None]:
    final_text = ""
    usage: dict[str, Any] = {}
    request_id = None
    for raw_line in str(stdout_text or "").splitlines():
        line = str(raw_line or "").strip()
        if not line:
            continue
        try:
            evt = json.loads(line)
        except Exception:
            continue
        evt_type = str(evt.get("type") or "").strip()
        if evt_type == "thread.started":
            request_id = str(evt.get("thread_id") or request_id or "").strip() or request_id
            continue
        if evt_type == "item.completed":
            item = evt.get("item") if isinstance(evt.get("item"), dict) else {}
            if str(item.get("type") or "").strip() == "agent_message":
                text = str(item.get("text") or "").strip()
                if text:
                    final_text = text
            continue
        if evt_type == "turn.completed":
            usage = evt.get("usage") if isinstance(evt.get("usage"), dict) else usage
    return final_text, usage, request_id


def build_codex_prompt(system_msg: str, user_msg: str) -> str:
    return (
        "You are PMDA's internal AI worker. Follow the instructions exactly.\n\n"
        "System instructions:\n"
        f"{str(system_msg or '').strip()}\n\n"
        "Task:\n"
        f"{str(user_msg or '').strip()}\n"
    ).strip()


def materialize_codex_images(
    temp_dir: Path,
    image_urls: Optional[List[str]] = None,
    image_base64: Optional[List[dict]] = None,
) -> list[Path]:
    out: list[Path] = []
    seen: set[str] = set()
    index = 0
    for raw_url in list(image_urls or [])[:10]:
        url = str(raw_url or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        index += 1
        ext = ".img"
        try:
            parsed = urlparse(url)
            suffix = Path(parsed.path or "").suffix.lower()
            if suffix:
                ext = suffix
        except Exception:
            pass
        target = temp_dir / f"img_{index}{ext}"
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        target.write_bytes(resp.content)
        out.append(target)
    for item in list(image_base64 or [])[:10]:
        if not isinstance(item, dict):
            continue
        blob = str(((item.get("image_url") or {}) if isinstance(item.get("image_url"), dict) else {}).get("url") or "").strip()
        if not blob or not blob.startswith("data:"):
            continue
        header, _, encoded = blob.partition(",")
        if not encoded:
            continue
        mime = header.partition(";")[0].replace("data:", "").strip().lower()
        ext = ".png"
        if "jpeg" in mime or "jpg" in mime:
            ext = ".jpg"
        elif "webp" in mime:
            ext = ".webp"
        elif "gif" in mime:
            ext = ".gif"
        index += 1
        target = temp_dir / f"img_{index}{ext}"
        target.write_bytes(base64.b64decode(encoded))
        out.append(target)
    return out


def run_openai_codex_exec_for_runtime(
    runtime: Any,
    *,
    system_msg: str,
    user_msg: str,
    analysis_type: str,
    request_timeout_sec: float | None = None,
    web_search: bool = False,
    image_urls: Optional[List[str]] = None,
    image_base64: Optional[List[dict]] = None,
) -> tuple[str, dict[str, Any]]:
    codex_bin = runtime._codex_cli_path()
    if not codex_bin:
        raise RuntimeError("Codex CLI is not installed in the PMDA runtime")
    uid = runtime._current_user_id_or_zero()
    runtime._write_codex_auth_json(uid, force_refresh=False)
    codex_home = runtime._codex_home_for_user(uid)
    env = os.environ.copy()
    env["CODEX_HOME"] = str(codex_home)
    prompt = build_codex_prompt(system_msg, user_msg)
    request_timeout = max(
        10.0,
        float(request_timeout_sec if request_timeout_sec is not None else runtime._openai_request_timeout_seconds()),
    )
    web_search_mode = '"live"' if web_search else '"disabled"'
    cmd = [
        codex_bin,
        "exec",
        "-C",
        "/tmp",
        "--skip-git-repo-check",
        "--sandbox",
        "read-only",
        "--ephemeral",
        "--json",
        "-c",
        "features.shell_tool=false",
        "-c",
        "features.apps=false",
        "-c",
        'history.persistence="none"',
        "-c",
        "hide_agent_reasoning=true",
        "-c",
        f"web_search={web_search_mode}",
        "-",
    ]
    with tempfile.TemporaryDirectory(prefix="pmda_codex_") as tmp_dir_str:
        tmp_dir = Path(tmp_dir_str)
        for img_path in materialize_codex_images(tmp_dir, image_urls=image_urls, image_base64=image_base64):
            cmd.extend(["--image", str(img_path)])
        try:
            proc = subprocess.run(
                cmd,
                input=prompt,
                text=True,
                capture_output=True,
                env=env,
                timeout=request_timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise TimeoutError(f"Codex CLI timed out after {int(request_timeout)}s ({analysis_type})") from exc
    stdout_text = str(proc.stdout or "")
    stderr_text = str(proc.stderr or "").strip()
    final_text, usage, request_id = codex_extract_final_text(stdout_text)
    if proc.returncode != 0:
        detail = stderr_text or stdout_text.strip().splitlines()[-1] if stdout_text.strip() else ""
        raise RuntimeError(f"Codex CLI failed (exit {proc.returncode}){': ' + detail if detail else ''}")
    if not final_text:
        raise RuntimeError("Codex CLI returned empty output")
    response_obj = {
        "id": request_id or f"codex-exec-{uuid.uuid4().hex}",
        "usage": openai_usage_dict_from_codex(usage),
        "provider": "openai-codex",
    }
    return final_text, response_obj
