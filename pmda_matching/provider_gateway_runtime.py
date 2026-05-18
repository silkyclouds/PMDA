"""Provider gateway cache, coalescing, throttling, and statistics runtime."""

from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import threading
import time
from typing import Any

import requests
from requests import exceptions as requests_exceptions

_RUNTIME: Any | None = None


_LOCAL_NAMES = {
    "_bind_runtime",
    "provider_gateway_runtime_settings_for_runtime",
    "provider_gateway_reconfigure_for_runtime",
    "provider_gateway_record_lookup_request_for_runtime",
    "provider_gateway_record_lookup_network_request_for_runtime",
    "provider_gateway_record_result_for_runtime",
    "provider_gateway_record_lookup_cache_hit_for_runtime",
    "provider_gateway_record_lookup_coalesced_wait_for_runtime",
    "provider_gateway_http_get_for_runtime",
    "provider_gateway_stats_snapshot_for_runtime",
    "provider_gateway_stats_snapshot_best_effort_for_runtime",
    "get_cached_provider_album_lookup_for_runtime",
    "set_cached_provider_album_lookup_for_runtime",
    "fetch_provider_album_lookup_cached_for_runtime",
    "_ProviderGatewayResponse",
    "_PROVIDER_GATEWAY_LOCK",
    "_PROVIDER_GATEWAY_CACHE",
    "_PROVIDER_GATEWAY_ERROR_CACHE",
    "_PROVIDER_GATEWAY_INFLIGHT_REQUESTS",
    "_PROVIDER_GATEWAY_BUCKETS",
    "_PROVIDER_GATEWAY_STATS",
    "_provider_gateway_semaphore",
    "_provider_gateway_semaphore_limit",
    "_provider_gateway_inflight",
    "_lock_try_acquire_nonblocking",
    "_provider_cache_norm",
    "_PROVIDER_LOOKUP_INFLIGHT_LOCK",
    "_PROVIDER_LOOKUP_INFLIGHT",
    "get_cached_provider_album_lookup",
    "set_cached_provider_album_lookup",
    "fetch_provider_album_lookup_cached",
}


def _bind_runtime(runtime: Any) -> None:
    global _RUNTIME
    _RUNTIME = runtime
    for name, value in vars(runtime).items():
        if name in _LOCAL_NAMES or name.startswith("_provider_gateway_") or name.startswith("_PROVIDER_GATEWAY"):
            continue
        globals()[name] = value


def provider_gateway_runtime_settings_for_runtime(runtime: Any) -> dict[str, Any]:
    _bind_runtime(runtime)
    return _provider_gateway_runtime_settings()


def provider_gateway_reconfigure_for_runtime(runtime: Any) -> None:
    _bind_runtime(runtime)
    return _provider_gateway_reconfigure()


def provider_gateway_record_lookup_request_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> None:
    _bind_runtime(runtime)
    return _provider_gateway_record_lookup_request(*args, **kwargs)


def provider_gateway_record_lookup_network_request_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> None:
    _bind_runtime(runtime)
    return _provider_gateway_record_lookup_network_request(*args, **kwargs)


def provider_gateway_record_result_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> None:
    _bind_runtime(runtime)
    return _provider_gateway_record_result(*args, **kwargs)


def provider_gateway_record_lookup_cache_hit_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> None:
    _bind_runtime(runtime)
    return _provider_gateway_record_lookup_cache_hit(*args, **kwargs)


def provider_gateway_record_lookup_coalesced_wait_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> None:
    _bind_runtime(runtime)
    return _provider_gateway_record_lookup_coalesced_wait(*args, **kwargs)


def provider_gateway_http_get_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> _ProviderGatewayResponse:
    _bind_runtime(runtime)
    return _provider_gateway_http_get(*args, **kwargs)


def provider_gateway_stats_snapshot_for_runtime(runtime: Any) -> dict[str, Any]:
    _bind_runtime(runtime)
    return _provider_gateway_stats_snapshot()


def provider_gateway_stats_snapshot_best_effort_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> dict[str, Any]:
    _bind_runtime(runtime)
    return _provider_gateway_stats_snapshot_best_effort(*args, **kwargs)


def get_cached_provider_album_lookup_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> tuple[str | None, dict | None]:
    _bind_runtime(runtime)
    return get_cached_provider_album_lookup(*args, **kwargs)


def set_cached_provider_album_lookup_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> None:
    _bind_runtime(runtime)
    return set_cached_provider_album_lookup(*args, **kwargs)


def fetch_provider_album_lookup_cached_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> dict | None:
    _bind_runtime(runtime)
    return fetch_provider_album_lookup_cached(*args, **kwargs)


class _ProviderGatewayResponse:
    __slots__ = ("status_code", "text", "headers", "url", "_json_payload")

    def __init__(
        self,
        *,
        status_code: int,
        text: str = "",
        headers: dict[str, Any] | None = None,
        url: str = "",
        json_payload: Any = None,
    ):
        self.status_code = int(status_code or 0)
        self.text = str(text or "")
        self.headers = dict(headers or {})
        self.url = str(url or "")
        self._json_payload = copy.deepcopy(json_payload)

    @classmethod
    def from_requests_response(cls, resp: requests.Response, *, json_payload: Any = None) -> "_ProviderGatewayResponse":
        parsed_json = json_payload
        if parsed_json is None:
            try:
                parsed_json = resp.json()
            except Exception:
                parsed_json = None
        return cls(
            status_code=int(getattr(resp, "status_code", 0) or 0),
            text=str(getattr(resp, "text", "") or ""),
            headers=dict(getattr(resp, "headers", {}) or {}),
            url=str(getattr(resp, "url", "") or ""),
            json_payload=parsed_json,
        )

    def clone(self) -> "_ProviderGatewayResponse":
        return _ProviderGatewayResponse(
            status_code=self.status_code,
            text=self.text,
            headers=dict(self.headers),
            url=self.url,
            json_payload=copy.deepcopy(self._json_payload),
        )

    def json(self) -> Any:
        if self._json_payload is not None:
            return copy.deepcopy(self._json_payload)
        if not self.text:
            return {}
        return json.loads(self.text)


_PROVIDER_GATEWAY_LOCK = threading.Lock()
_PROVIDER_GATEWAY_CACHE: dict[str, tuple[float, _ProviderGatewayResponse]] = {}
_PROVIDER_GATEWAY_ERROR_CACHE: dict[str, tuple[float, str, str]] = {}
_PROVIDER_GATEWAY_INFLIGHT_REQUESTS: dict[str, dict[str, Any]] = {}
_PROVIDER_GATEWAY_BUCKETS: dict[str, dict[str, float]] = {}
_PROVIDER_GATEWAY_STATS: dict[str, Any] = {
    "providers": {},
    "max_inflight_observed": 0,
}
_provider_gateway_semaphore: threading.BoundedSemaphore | None = None
_provider_gateway_semaphore_limit: int = 0
_provider_gateway_inflight: int = 0
_PROVIDER_LOOKUP_INFLIGHT_LOCK = threading.Lock()
_PROVIDER_LOOKUP_INFLIGHT: dict[str, dict[str, Any]] = {}


def _provider_gateway_runtime_settings() -> dict[str, Any]:
    discogs_rpm_resolver = globals().get("_discogs_effective_rpm")
    if callable(discogs_rpm_resolver):
        try:
            discogs_rpm = float(discogs_rpm_resolver())
        except Exception:
            discogs_rpm = float(max(1, PROVIDER_GATEWAY_DISCOGS_RPM or 1))
    else:
        discogs_rpm = float(max(1, PROVIDER_GATEWAY_DISCOGS_RPM or 1))
    return {
        "enabled": bool(PROVIDER_GATEWAY_ENABLED),
        "cache_enabled": bool(PROVIDER_GATEWAY_CACHE_ENABLED),
        "max_inflight": int(max(1, PROVIDER_GATEWAY_MAX_INFLIGHT or 1)),
        "providers": {
            "discogs": {"rpm": int(max(1, int(discogs_rpm or 1))), "cache_ttl_sec": 60 * 60 * 12},
            "lastfm": {"rpm": int(max(1, PROVIDER_GATEWAY_LASTFM_RPM or 1)), "cache_ttl_sec": 60 * 60 * 6},
            "itunes": {"rpm": int(max(1, PROVIDER_GATEWAY_ITUNES_RPM or 1)), "cache_ttl_sec": 60 * 60 * 12},
            "deezer": {"rpm": int(max(1, PROVIDER_GATEWAY_DEEZER_RPM or 1)), "cache_ttl_sec": 60 * 60 * 12},
            "spotify": {"rpm": int(max(1, PROVIDER_GATEWAY_SPOTIFY_RPM or 1)), "cache_ttl_sec": 60 * 60 * 12},
            "qobuz": {"rpm": int(max(1, PROVIDER_GATEWAY_QOBUZ_RPM or 1)), "cache_ttl_sec": 60 * 60 * 12},
            "tidal": {"rpm": int(max(1, PROVIDER_GATEWAY_TIDAL_RPM or 1)), "cache_ttl_sec": 60 * 60 * 12},
            "audiodb": {"rpm": int(max(1, PROVIDER_GATEWAY_AUDIODB_RPM or 1)), "cache_ttl_sec": 60 * 60 * 12},
            "bandcamp": {"rpm": int(max(1, PROVIDER_GATEWAY_BANDCAMP_RPM or 1)), "cache_ttl_sec": 60 * 60 * 12},
        },
    }


def _provider_gateway_reconfigure() -> None:
    global _provider_gateway_semaphore, _provider_gateway_semaphore_limit
    settings = _provider_gateway_runtime_settings()
    limit = int(settings["max_inflight"] or 1)
    with _PROVIDER_GATEWAY_LOCK:
        if _provider_gateway_semaphore is None or _provider_gateway_semaphore_limit != limit:
            _provider_gateway_semaphore = threading.BoundedSemaphore(limit)
            _provider_gateway_semaphore_limit = limit


def _provider_gateway_stats_bucket(provider: str) -> dict[str, Any]:
    provider_key = str(provider or "unknown").strip().lower() or "unknown"
    with _PROVIDER_GATEWAY_LOCK:
        bucket = _PROVIDER_GATEWAY_STATS["providers"].get(provider_key)
        if bucket is None:
            bucket = {
                "request_count": 0,
                "network_request_count": 0,
                "cache_hits": 0,
                "negative_cache_hits": 0,
                "error_cache_hits": 0,
                "coalesced_waits": 0,
                "lookup_cache_hits": 0,
                "lookup_negative_hits": 0,
                "lookup_error_hits": 0,
                "lookup_coalesced_waits": 0,
                "lookup_request_count": 0,
                "lookup_network_request_count": 0,
                "rate_limited_count": 0,
                "failure_count": 0,
                "timeout_count": 0,
                "total_latency_ms": 0,
                "last_status": None,
                "last_error": "",
                "last_context": "",
                "last_request_at": 0.0,
            }
            _PROVIDER_GATEWAY_STATS["providers"][provider_key] = bucket
        return bucket


def _provider_gateway_record_lookup_request(provider: str) -> None:
    bucket = _provider_gateway_stats_bucket(provider)
    with _PROVIDER_GATEWAY_LOCK:
        bucket["lookup_request_count"] = int(bucket.get("lookup_request_count") or 0) + 1
        bucket["last_request_at"] = time.time()


def _provider_gateway_record_lookup_network_request(provider: str) -> None:
    bucket = _provider_gateway_stats_bucket(provider)
    with _PROVIDER_GATEWAY_LOCK:
        bucket["lookup_network_request_count"] = int(bucket.get("lookup_network_request_count") or 0) + 1
        bucket["last_request_at"] = time.time()


def _provider_gateway_cache_key(provider: str, method: str, url: str, params: dict[str, Any] | None) -> str:
    encoded = ""
    if params:
        try:
            encoded = json.dumps(params, sort_keys=True, ensure_ascii=False, default=str)
        except Exception:
            encoded = str(sorted((str(k), str(v)) for k, v in dict(params).items()))
    raw = f"{str(provider or '').strip().lower()}|{str(method or 'GET').strip().upper()}|{str(url or '').strip()}|{encoded}"
    return hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()


def _provider_gateway_wait_for_slot(provider: str) -> None:
    settings = _provider_gateway_runtime_settings()
    provider_key = str(provider or "unknown").strip().lower() or "unknown"
    provider_settings = settings["providers"].get(provider_key) or {"rpm": 120}
    rpm = float(max(1, int(provider_settings.get("rpm") or 1)))
    refill_per_sec = rpm / 60.0
    capacity = max(1.0, min(10.0, rpm))
    while True:
        wait_for = 0.0
        with _PROVIDER_GATEWAY_LOCK:
            bucket = _PROVIDER_GATEWAY_BUCKETS.get(provider_key)
            now = time.time()
            if bucket is None:
                bucket = {"tokens": capacity, "last_refill": now}
                _PROVIDER_GATEWAY_BUCKETS[provider_key] = bucket
            elapsed = max(0.0, now - float(bucket.get("last_refill") or now))
            tokens = min(capacity, float(bucket.get("tokens") or 0.0) + (elapsed * refill_per_sec))
            if tokens >= 1.0:
                bucket["tokens"] = tokens - 1.0
                bucket["last_refill"] = now
                return
            bucket["tokens"] = tokens
            bucket["last_refill"] = now
            wait_for = max(0.05, (1.0 - tokens) / refill_per_sec)
        time.sleep(min(wait_for, 2.0))


def _provider_gateway_record_result(
    provider: str,
    *,
    status_code: int | None = None,
    cache_hit: bool = False,
    negative_cache_hit: bool = False,
    error_cache_hit: bool = False,
    coalesced_wait: bool = False,
    latency_ms: int | None = None,
    error: Exception | None = None,
    context: str = "",
) -> None:
    bucket = _provider_gateway_stats_bucket(provider)
    with _PROVIDER_GATEWAY_LOCK:
        bucket["request_count"] = int(bucket.get("request_count") or 0) + 1
        if cache_hit:
            bucket["cache_hits"] = int(bucket.get("cache_hits") or 0) + 1
        else:
            bucket["network_request_count"] = int(bucket.get("network_request_count") or 0) + 1
        if negative_cache_hit:
            bucket["negative_cache_hits"] = int(bucket.get("negative_cache_hits") or 0) + 1
        if error_cache_hit:
            bucket["error_cache_hits"] = int(bucket.get("error_cache_hits") or 0) + 1
        if coalesced_wait:
            bucket["coalesced_waits"] = int(bucket.get("coalesced_waits") or 0) + 1
        if latency_ms is not None:
            bucket["total_latency_ms"] = int(bucket.get("total_latency_ms") or 0) + max(0, int(latency_ms))
        if status_code is not None:
            bucket["last_status"] = int(status_code)
            if int(status_code) == 429:
                bucket["rate_limited_count"] = int(bucket.get("rate_limited_count") or 0) + 1
        if error is not None:
            bucket["failure_count"] = int(bucket.get("failure_count") or 0) + 1
            if isinstance(error, requests.exceptions.Timeout):
                bucket["timeout_count"] = int(bucket.get("timeout_count") or 0) + 1
            bucket["last_error"] = str(error)
        if context:
            bucket["last_context"] = str(context)
        bucket["last_request_at"] = time.time()


def _provider_gateway_record_lookup_cache_hit(provider: str, status: str) -> None:
    bucket = _provider_gateway_stats_bucket(provider)
    status_norm = str(status or "").strip().lower()
    with _PROVIDER_GATEWAY_LOCK:
        if status_norm == "found":
            bucket["lookup_cache_hits"] = int(bucket.get("lookup_cache_hits") or 0) + 1
        elif status_norm == "not_found":
            bucket["lookup_negative_hits"] = int(bucket.get("lookup_negative_hits") or 0) + 1
        elif status_norm == "error":
            bucket["lookup_error_hits"] = int(bucket.get("lookup_error_hits") or 0) + 1
        bucket["last_request_at"] = time.time()


def _provider_gateway_record_lookup_coalesced_wait(provider: str, *, context: str = "") -> None:
    bucket = _provider_gateway_stats_bucket(provider)
    with _PROVIDER_GATEWAY_LOCK:
        bucket["lookup_coalesced_waits"] = int(bucket.get("lookup_coalesced_waits") or 0) + 1
        if context:
            bucket["last_context"] = str(context)
        bucket["last_request_at"] = time.time()


def _provider_gateway_rebuild_error(kind: str, message: str) -> requests.exceptions.RequestException:
    text = str(message or "Provider request failed")
    if str(kind or "").strip().lower() == "timeout":
        return requests.exceptions.Timeout(text)
    return requests.exceptions.RequestException(text)


def _provider_gateway_http_get(
    provider: str,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float | tuple[float, float] | tuple[int, int] | None = None,
    allow_redirects: bool = True,
    context: str = "",
    cache_ttl_sec: int | None = None,
) -> _ProviderGatewayResponse:
    global _provider_gateway_inflight
    provider_key = str(provider or "unknown").strip().lower() or "unknown"
    settings = _provider_gateway_runtime_settings()
    gateway_enabled = bool(settings["enabled"])
    cache_enabled = bool(settings["cache_enabled"])
    provider_defaults = settings["providers"].get(provider_key) or {}
    effective_ttl = int(cache_ttl_sec if cache_ttl_sec is not None else provider_defaults.get("cache_ttl_sec") or 0)
    cache_key = _provider_gateway_cache_key(provider_key, "GET", url, params)
    now = time.time()
    cached_response: _ProviderGatewayResponse | None = None
    cached_error: tuple[str, str] | None = None
    if gateway_enabled and cache_enabled and effective_ttl > 0:
        with _PROVIDER_GATEWAY_LOCK:
            cached = _PROVIDER_GATEWAY_CACHE.get(cache_key)
            if cached and float(cached[0] or 0.0) > now:
                cached_response = cached[1].clone()
            elif cached:
                _PROVIDER_GATEWAY_CACHE.pop(cache_key, None)
            error_cached = _PROVIDER_GATEWAY_ERROR_CACHE.get(cache_key)
            if error_cached and float(error_cached[0] or 0.0) > now:
                cached_error = (str(error_cached[1] or ""), str(error_cached[2] or ""))
            elif error_cached:
                _PROVIDER_GATEWAY_ERROR_CACHE.pop(cache_key, None)
    if cached_response is not None:
        _provider_gateway_record_result(
            provider_key,
            status_code=cached_response.status_code,
            cache_hit=True,
            negative_cache_hit=bool(cached_response.status_code in {204, 404, 410}),
            context=context,
        )
        return cached_response
    if cached_error is not None:
        exc = _provider_gateway_rebuild_error(cached_error[0], cached_error[1])
        _provider_gateway_record_result(provider_key, cache_hit=True, error_cache_hit=True, error=exc, context=context)
        raise exc
    inflight_entry: dict[str, Any] | None = None
    inflight_owner = False
    if gateway_enabled:
        with _PROVIDER_GATEWAY_LOCK:
            existing = _PROVIDER_GATEWAY_INFLIGHT_REQUESTS.get(cache_key)
            if existing is not None:
                inflight_entry = existing
                inflight_entry["waiters"] = int(inflight_entry.get("waiters") or 0) + 1
            else:
                inflight_entry = {
                    "event": threading.Event(),
                    "response": None,
                    "error_kind": "",
                    "error_message": "",
                    "status_code": None,
                    "created_at": time.time(),
                    "waiters": 0,
                }
                _PROVIDER_GATEWAY_INFLIGHT_REQUESTS[cache_key] = inflight_entry
                inflight_owner = True
    if gateway_enabled and inflight_entry is not None and not inflight_owner:
        wait_timeout = 0.0
        if isinstance(timeout, (tuple, list)):
            try:
                wait_timeout = float(sum(float(v or 0) for v in timeout))
            except Exception:
                wait_timeout = 0.0
        elif timeout is not None:
            try:
                wait_timeout = float(timeout)
            except Exception:
                wait_timeout = 0.0
        wait_timeout = max(5.0, wait_timeout + 5.0)
        event = inflight_entry["event"]
        if event.wait(timeout=wait_timeout):
            status_code = inflight_entry.get("status_code")
            if isinstance(inflight_entry.get("response"), _ProviderGatewayResponse):
                shared_response = inflight_entry["response"].clone()
                _provider_gateway_record_result(
                    provider_key,
                    status_code=int(status_code or shared_response.status_code or 0),
                    cache_hit=True,
                    negative_cache_hit=bool(int(status_code or shared_response.status_code or 0) in {204, 404, 410}),
                    coalesced_wait=True,
                    context=context,
                )
                return shared_response
            if inflight_entry.get("error_message"):
                shared_exc = _provider_gateway_rebuild_error(
                    str(inflight_entry.get("error_kind") or ""),
                    str(inflight_entry.get("error_message") or ""),
                )
                _provider_gateway_record_result(
                    provider_key,
                    cache_hit=True,
                    error_cache_hit=True,
                    coalesced_wait=True,
                    error=shared_exc,
                    context=context,
                )
                raise shared_exc
        with _PROVIDER_GATEWAY_LOCK:
            current = _PROVIDER_GATEWAY_INFLIGHT_REQUESTS.get(cache_key)
            if current is inflight_entry:
                _PROVIDER_GATEWAY_INFLIGHT_REQUESTS.pop(cache_key, None)
                inflight_owner = True
    _provider_gateway_reconfigure()
    if gateway_enabled:
        _provider_gateway_wait_for_slot(provider_key)
    sem = _provider_gateway_semaphore
    acquired = False
    start = time.time()
    try:
        if gateway_enabled and sem is not None:
            sem.acquire()
            acquired = True
            with _PROVIDER_GATEWAY_LOCK:
                _provider_gateway_inflight += 1
                _PROVIDER_GATEWAY_STATS["max_inflight_observed"] = max(
                    int(_PROVIDER_GATEWAY_STATS.get("max_inflight_observed") or 0),
                    _provider_gateway_inflight,
                )
        request_kwargs = {
            "headers": headers,
            "timeout": timeout,
            "allow_redirects": allow_redirects,
        }
        if params is not None:
            request_kwargs["params"] = params
        resp = requests.get(url, **request_kwargs)
        latency_ms = int(max(0.0, (time.time() - start) * 1000.0))
        response = _ProviderGatewayResponse.from_requests_response(resp)
        _provider_gateway_record_result(provider_key, status_code=response.status_code, latency_ms=latency_ms, context=context)
        if gateway_enabled and cache_enabled:
            with _PROVIDER_GATEWAY_LOCK:
                if effective_ttl > 0 and response.status_code == 200:
                    _PROVIDER_GATEWAY_CACHE[cache_key] = (time.time() + effective_ttl, response.clone())
                elif response.status_code in {204, 404, 410}:
                    ttl = max(60, int(PROVIDER_CACHE_NOT_FOUND_TTL_SEC or (60 * 60 * 24 * 30)))
                    _PROVIDER_GATEWAY_CACHE[cache_key] = (time.time() + ttl, response.clone())
                if inflight_entry is not None:
                    inflight_entry["response"] = response.clone()
                    inflight_entry["status_code"] = int(response.status_code or 0)
                    inflight_entry["event"].set()
                    _PROVIDER_GATEWAY_INFLIGHT_REQUESTS.pop(cache_key, None)
        return response
    except requests_exceptions.RequestException as exc:
        latency_ms = int(max(0.0, (time.time() - start) * 1000.0))
        _provider_gateway_record_result(provider_key, latency_ms=latency_ms, error=exc, context=context)
        if gateway_enabled and cache_enabled:
            error_kind = "timeout" if isinstance(exc, requests_exceptions.Timeout) else "request"
            ttl = max(30, int(PROVIDER_CACHE_ERROR_TTL_SEC or (60 * 60 * 6)))
            with _PROVIDER_GATEWAY_LOCK:
                _PROVIDER_GATEWAY_ERROR_CACHE[cache_key] = (time.time() + ttl, error_kind, str(exc))
                if inflight_entry is not None:
                    inflight_entry["error_kind"] = error_kind
                    inflight_entry["error_message"] = str(exc)
                    inflight_entry["event"].set()
                    _PROVIDER_GATEWAY_INFLIGHT_REQUESTS.pop(cache_key, None)
        raise
    finally:
        if acquired and sem is not None:
            sem.release()
            with _PROVIDER_GATEWAY_LOCK:
                _provider_gateway_inflight = max(0, int(_provider_gateway_inflight or 0) - 1)


def _provider_gateway_stats_snapshot() -> dict[str, Any]:
    settings = _provider_gateway_runtime_settings()
    with _PROVIDER_GATEWAY_LOCK:
        providers_out: dict[str, Any] = {}
        for provider, stats in dict(_PROVIDER_GATEWAY_STATS.get("providers") or {}).items():
            request_count = int(stats.get("request_count") or 0)
            lookup_request_count = int(stats.get("lookup_request_count") or 0)
            lookup_network_request_count = int(stats.get("lookup_network_request_count") or 0)
            lookup_cache_hits = int(stats.get("lookup_cache_hits") or 0)
            lookup_negative_hits = int(stats.get("lookup_negative_hits") or 0)
            lookup_error_hits = int(stats.get("lookup_error_hits") or 0)
            lookup_coalesced_waits = int(stats.get("lookup_coalesced_waits") or 0)
            lookup_saved_count = max(0, lookup_request_count - lookup_network_request_count)
            lookup_cached_count = lookup_cache_hits + lookup_negative_hits + lookup_error_hits
            lookup_saved_rate = round((lookup_saved_count / lookup_request_count) * 100.0, 1) if lookup_request_count else 0.0
            lookup_cache_resolution_rate = round((lookup_cached_count / lookup_request_count) * 100.0, 1) if lookup_request_count else 0.0
            total_latency_ms = int(stats.get("total_latency_ms") or 0)
            providers_out[provider] = {
                "rpm_limit": int((settings["providers"].get(provider) or {}).get("rpm") or 0),
                "request_count": request_count,
                "network_request_count": int(stats.get("network_request_count") or 0),
                "cache_hits": int(stats.get("cache_hits") or 0),
                "negative_cache_hits": int(stats.get("negative_cache_hits") or 0),
                "error_cache_hits": int(stats.get("error_cache_hits") or 0),
                "coalesced_waits": int(stats.get("coalesced_waits") or 0),
                "lookup_request_count": lookup_request_count,
                "lookup_network_request_count": lookup_network_request_count,
                "lookup_saved_count": lookup_saved_count,
                "lookup_cache_hits": lookup_cache_hits,
                "lookup_negative_hits": lookup_negative_hits,
                "lookup_error_hits": lookup_error_hits,
                "lookup_coalesced_waits": lookup_coalesced_waits,
                "lookup_hit_rate": lookup_cache_resolution_rate,
                "lookup_saved_rate": lookup_saved_rate,
                "lookup_cache_resolution_rate": lookup_cache_resolution_rate,
                "avg_network_requests_per_lookup": round((int(stats.get("network_request_count") or 0) / lookup_network_request_count), 2) if lookup_network_request_count else 0.0,
                "cache_hit_rate": round((int(stats.get("cache_hits") or 0) / request_count) * 100.0, 1) if request_count else 0.0,
                "rate_limited_count": int(stats.get("rate_limited_count") or 0),
                "failure_count": int(stats.get("failure_count") or 0),
                "timeout_count": int(stats.get("timeout_count") or 0),
                "avg_latency_ms": round(total_latency_ms / request_count, 1) if request_count else 0.0,
                "last_status": stats.get("last_status"),
                "last_error": str(stats.get("last_error") or ""),
                "last_context": str(stats.get("last_context") or ""),
                "last_request_at": float(stats.get("last_request_at") or 0.0),
            }
        return {
            "enabled": bool(settings["enabled"]),
            "cache_enabled": bool(settings["cache_enabled"]),
            "max_inflight": int(settings["max_inflight"] or 1),
            "inflight": int(_provider_gateway_inflight or 0),
            "max_inflight_observed": int(_PROVIDER_GATEWAY_STATS.get("max_inflight_observed") or 0),
            "providers": providers_out,
        }


def _lock_try_acquire_nonblocking(lock_obj: Any) -> bool:
    try:
        return bool(lock_obj.acquire(blocking=False))
    except TypeError:
        try:
            return bool(lock_obj.acquire(False))
        except Exception:
            return False
    except Exception:
        return False


def _runtime_lock_try_acquire_nonblocking(lock_obj: Any) -> bool:
    helper = getattr(_RUNTIME, "_lock_try_acquire_nonblocking", None) if _RUNTIME is not None else None
    if callable(helper) and helper is not _lock_try_acquire_nonblocking:
        try:
            return bool(helper(lock_obj))
        except RecursionError:
            return _lock_try_acquire_nonblocking(lock_obj)
        except Exception:
            return False
    return _lock_try_acquire_nonblocking(lock_obj)


def _provider_gateway_stats_snapshot_best_effort(
    cached_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    settings = _provider_gateway_runtime_settings()
    cached = dict(cached_snapshot or {}) if isinstance(cached_snapshot, dict) else {}
    if not _runtime_lock_try_acquire_nonblocking(_PROVIDER_GATEWAY_LOCK):
        return {
            "enabled": bool(settings["enabled"]),
            "cache_enabled": bool(settings["cache_enabled"]),
            "max_inflight": int(settings["max_inflight"] or 1),
            "inflight": int(cached.get("inflight") or 0),
            "max_inflight_observed": int(cached.get("max_inflight_observed") or 0),
            "providers": dict(cached.get("providers") or {}),
            "stale": True,
        }
    try:
        providers_out: dict[str, Any] = {}
        for provider, stats in dict(_PROVIDER_GATEWAY_STATS.get("providers") or {}).items():
            request_count = int(stats.get("request_count") or 0)
            lookup_request_count = int(stats.get("lookup_request_count") or 0)
            lookup_network_request_count = int(stats.get("lookup_network_request_count") or 0)
            lookup_cache_hits = int(stats.get("lookup_cache_hits") or 0)
            lookup_negative_hits = int(stats.get("lookup_negative_hits") or 0)
            lookup_error_hits = int(stats.get("lookup_error_hits") or 0)
            lookup_coalesced_waits = int(stats.get("lookup_coalesced_waits") or 0)
            lookup_saved_count = max(0, lookup_request_count - lookup_network_request_count)
            lookup_cached_count = lookup_cache_hits + lookup_negative_hits + lookup_error_hits
            lookup_saved_rate = round((lookup_saved_count / lookup_request_count) * 100.0, 1) if lookup_request_count else 0.0
            lookup_cache_resolution_rate = round((lookup_cached_count / lookup_request_count) * 100.0, 1) if lookup_request_count else 0.0
            total_latency_ms = int(stats.get("total_latency_ms") or 0)
            providers_out[provider] = {
                "rpm_limit": int((settings["providers"].get(provider) or {}).get("rpm") or 0),
                "request_count": request_count,
                "network_request_count": int(stats.get("network_request_count") or 0),
                "cache_hits": int(stats.get("cache_hits") or 0),
                "negative_cache_hits": int(stats.get("negative_cache_hits") or 0),
                "error_cache_hits": int(stats.get("error_cache_hits") or 0),
                "coalesced_waits": int(stats.get("coalesced_waits") or 0),
                "lookup_request_count": lookup_request_count,
                "lookup_network_request_count": lookup_network_request_count,
                "lookup_saved_count": lookup_saved_count,
                "lookup_cache_hits": lookup_cache_hits,
                "lookup_negative_hits": lookup_negative_hits,
                "lookup_error_hits": lookup_error_hits,
                "lookup_coalesced_waits": lookup_coalesced_waits,
                "lookup_hit_rate": lookup_cache_resolution_rate,
                "lookup_saved_rate": lookup_saved_rate,
                "lookup_cache_resolution_rate": lookup_cache_resolution_rate,
                "avg_network_requests_per_lookup": round((int(stats.get("network_request_count") or 0) / lookup_network_request_count), 2) if lookup_network_request_count else 0.0,
                "cache_hit_rate": round((int(stats.get("cache_hits") or 0) / request_count) * 100.0, 1) if request_count else 0.0,
                "rate_limited_count": int(stats.get("rate_limited_count") or 0),
                "failure_count": int(stats.get("failure_count") or 0),
                "timeout_count": int(stats.get("timeout_count") or 0),
                "avg_latency_ms": round(total_latency_ms / request_count, 1) if request_count else 0.0,
                "last_status": stats.get("last_status"),
                "last_error": str(stats.get("last_error") or ""),
                "last_context": str(stats.get("last_context") or ""),
                "last_request_at": float(stats.get("last_request_at") or 0.0),
            }
        return {
            "enabled": bool(settings["enabled"]),
            "cache_enabled": bool(settings["cache_enabled"]),
            "max_inflight": int(settings["max_inflight"] or 1),
            "inflight": int(_provider_gateway_inflight or 0),
            "max_inflight_observed": int(_PROVIDER_GATEWAY_STATS.get("max_inflight_observed") or 0),
            "providers": providers_out,
            "stale": False,
        }
    finally:
        try:
            _PROVIDER_GATEWAY_LOCK.release()
        except Exception:
            pass


def _provider_cache_norm(value: str) -> str:
    txt = " ".join((value or "").strip().split())
    if not txt:
        return ""
    norm = norm_album(txt)
    return norm or txt.lower()


def get_cached_provider_album_lookup(provider: str, artist_name: str, album_title: str) -> tuple[str | None, dict | None]:
    """
    Return cached provider lookup:
    - (None, None): cache miss or expired
    - ("not_found", None): cached negative lookup
    - ("error", None): cached transient provider error
    - ("found", payload_dict): cached provider payload
    """
    provider_key = (provider or "").strip().lower()
    artist_norm = _provider_cache_norm(artist_name)
    album_norm = _provider_cache_norm(album_title)
    if not provider_key or not artist_norm or not album_norm:
        return (None, None)
    now = int(time.time())
    con = sqlite3.connect(str(CACHE_DB_FILE), timeout=30)
    cur = con.cursor()
    cur.execute(
        """
        SELECT status, payload_json, expires_at
        FROM provider_album_lookup
        WHERE provider = ? AND artist_norm = ? AND album_norm = ?
        """,
        (provider_key, artist_norm, album_norm),
    )
    row = cur.fetchone()
    con.close()
    if row is None:
        return (None, None)
    status = str(row[0] or "").strip().lower()
    payload_raw = row[1]
    expires_at = int(row[2] or 0)
    if expires_at and expires_at < now:
        try:
            con = sqlite3.connect(str(CACHE_DB_FILE), timeout=30)
            cur = con.cursor()
            cur.execute(
                "DELETE FROM provider_album_lookup WHERE provider = ? AND artist_norm = ? AND album_norm = ?",
                (provider_key, artist_norm, album_norm),
            )
            con.commit()
            con.close()
        except Exception:
            pass
        return (None, None)
    if status == "found":
        try:
            payload = json.loads(payload_raw) if payload_raw else None
        except Exception:
            payload = None
        return ("found", payload if isinstance(payload, dict) else None)
    if status in {"not_found", "error"}:
        return (status, None)
    return (None, None)


def set_cached_provider_album_lookup(
    provider: str,
    artist_name: str,
    album_title: str,
    status: str,
    payload: dict | None = None,
) -> None:
    provider_key = (provider or "").strip().lower()
    artist_norm = _provider_cache_norm(artist_name)
    album_norm = _provider_cache_norm(album_title)
    status_norm = (status or "").strip().lower()
    if not provider_key or not artist_norm or not album_norm:
        return
    if status_norm not in {"found", "not_found", "error"}:
        return
    if status_norm == "found":
        ttl = max(60, int(globals().get("PROVIDER_CACHE_FOUND_TTL_SEC", 60 * 60 * 24 * 30) or (60 * 60 * 24 * 30)))
    elif status_norm == "not_found":
        ttl = max(60, int(globals().get("PROVIDER_CACHE_NOT_FOUND_TTL_SEC", 60 * 60 * 24 * 30) or (60 * 60 * 24 * 30)))
    else:
        ttl = max(30, int(globals().get("PROVIDER_CACHE_ERROR_TTL_SEC", 60 * 60 * 6) or (60 * 60 * 6)))
    now = int(time.time())
    expires_at = now + int(ttl)
    payload_json = json.dumps(payload) if (status_norm == "found" and isinstance(payload, dict)) else None
    con = sqlite3.connect(str(CACHE_DB_FILE), timeout=30)
    cur = con.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO provider_album_lookup
        (provider, artist_norm, album_norm, status, payload_json, created_at, expires_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (provider_key, artist_norm, album_norm, status_norm, payload_json, now, expires_at),
    )
    con.commit()
    con.close()


def fetch_provider_album_lookup_cached(
    provider: str,
    artist_name: str,
    album_title: str,
    fetcher,
) -> dict | None:
    """Cache wrapper around provider album fetchers."""
    _provider_gateway_record_lookup_request(provider)
    if SCAN_DISABLE_CACHE:
        _provider_gateway_record_lookup_network_request(provider)
        return fetcher(artist_name, album_title)
    cached_status, cached_payload = get_cached_provider_album_lookup(provider, artist_name, album_title)
    if cached_status == "found":
        _provider_gateway_record_lookup_cache_hit(provider, "found")
        return cached_payload
    if cached_status in {"not_found", "error"}:
        _provider_gateway_record_lookup_cache_hit(provider, cached_status)
        return None
    provider_key = str(provider or "").strip().lower()
    artist_norm = _provider_cache_norm(artist_name)
    album_norm = _provider_cache_norm(album_title)
    inflight_key = f"{provider_key}|{artist_norm}|{album_norm}"
    inflight_owner = False
    inflight_entry: dict[str, Any] | None = None
    with _PROVIDER_LOOKUP_INFLIGHT_LOCK:
        existing = _PROVIDER_LOOKUP_INFLIGHT.get(inflight_key)
        if existing is not None:
            inflight_entry = existing
            inflight_entry["waiters"] = int(inflight_entry.get("waiters") or 0) + 1
        else:
            inflight_entry = {"event": threading.Event(), "waiters": 0}
            _PROVIDER_LOOKUP_INFLIGHT[inflight_key] = inflight_entry
            inflight_owner = True
    if inflight_entry is not None and not inflight_owner:
        _provider_gateway_record_lookup_coalesced_wait(provider, context=f"{artist_name} / {album_title}")
        if inflight_entry["event"].wait(timeout=30.0):
            cached_status, cached_payload = get_cached_provider_album_lookup(provider, artist_name, album_title)
            if cached_status == "found":
                _provider_gateway_record_lookup_cache_hit(provider, "found")
                return cached_payload
            if cached_status in {"not_found", "error"}:
                _provider_gateway_record_lookup_cache_hit(provider, cached_status)
                return None
        with _PROVIDER_LOOKUP_INFLIGHT_LOCK:
            current = _PROVIDER_LOOKUP_INFLIGHT.get(inflight_key)
            if current is inflight_entry:
                _PROVIDER_LOOKUP_INFLIGHT.pop(inflight_key, None)
                inflight_owner = True
    try:
        _provider_gateway_record_lookup_network_request(provider)
        payload = fetcher(artist_name, album_title)
    except Exception:
        set_cached_provider_album_lookup(provider, artist_name, album_title, "error", None)
        if inflight_entry is not None:
            inflight_entry["event"].set()
            with _PROVIDER_LOOKUP_INFLIGHT_LOCK:
                _PROVIDER_LOOKUP_INFLIGHT.pop(inflight_key, None)
        raise
    if payload:
        set_cached_provider_album_lookup(provider, artist_name, album_title, "found", payload if isinstance(payload, dict) else None)
        if inflight_entry is not None:
            inflight_entry["event"].set()
            with _PROVIDER_LOOKUP_INFLIGHT_LOCK:
                _PROVIDER_LOOKUP_INFLIGHT.pop(inflight_key, None)
        return payload
    set_cached_provider_album_lookup(provider, artist_name, album_title, "not_found", None)
    if inflight_entry is not None:
        inflight_entry["event"].set()
        with _PROVIDER_LOOKUP_INFLIGHT_LOCK:
            _PROVIDER_LOOKUP_INFLIGHT.pop(inflight_key, None)
    return None
