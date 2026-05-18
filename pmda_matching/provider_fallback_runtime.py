"""Runtime-owned provider fallback lookup orchestration."""

from __future__ import annotations

import logging
import sys
from typing import Any


_LOCAL_NAMES = {
    '_bind_runtime',
    '_fetch_album_provider_fallbacks_parallel',
    '_fetch_album_provider_fallbacks_parallel_for_runtime',
}


def _bind_runtime(runtime: Any) -> None:
    for name, value in vars(runtime).items():
        if name in _LOCAL_NAMES:
            continue
        globals()[name] = value

def _fetch_album_provider_fallbacks_parallel_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _fetch_album_provider_fallbacks_parallel(*args, **kwargs)


def _fetch_album_provider_fallbacks_parallel(
    artist: str,
    album_title: str,
    *,
    scan_inline: bool | None = None,
) -> dict:
    """
    Fetch album metadata fallbacks concurrently.
    Metadata-only providers (Spotify, Qobuz, TIDAL, TheAudioDB) are used as
    supplementary title/artist/year/cover signals and never become strict identity
    authorities by themselves.
    Returns a dict with raw provider payloads and an `extra_sources` list for AI disambiguation.
    """
    out = {
        "discogs": None,
        "itunes": None,
        "deezer": None,
        "spotify": None,
        "qobuz": None,
        "tidal": None,
        "lastfm": None,
        "bandcamp": None,
        "audiodb": None,
        "extra_sources": [],
    }
    artist_name = (artist or "").strip()
    title = (album_title or "").strip()
    if not artist_name or not title:
        return out
    scan_inline_mode = _scan_inline_matching_active() if scan_inline is None else bool(scan_inline)
    scan_lifecycle_active = _ai_scan_lifecycle_phase_active()

    def _bandcamp_fetch_inline(current_artist: str, current_title: str):
        return _fetch_bandcamp_album_info(
            current_artist,
            current_title,
            allow_web_fallback=not scan_inline_mode and not scan_lifecycle_active,
        )

    try:
        from concurrent.futures import ThreadPoolExecutor, as_completed
    except Exception:
        # Fallback to sequential when concurrent futures is unavailable.
        if USE_DISCOGS:
            try:
                out["discogs"] = fetch_provider_album_lookup_cached(
                    "discogs",
                    artist_name,
                    title,
                    _fetch_discogs_release,
                )
            except Exception as e:
                logging.debug("[Providers] discogs fetch failed for %r - %r: %s", artist_name, title, e)
        if USE_ITUNES:
            try:
                out["itunes"] = fetch_provider_album_lookup_cached(
                    "itunes",
                    artist_name,
                    title,
                    _fetch_itunes_album_info,
                )
            except Exception as e:
                logging.debug("[Providers] itunes fetch failed for %r - %r: %s", artist_name, title, e)
        if USE_DEEZER:
            try:
                out["deezer"] = fetch_provider_album_lookup_cached(
                    "deezer",
                    artist_name,
                    title,
                    _fetch_deezer_album_info,
                )
            except Exception as e:
                logging.debug("[Providers] deezer fetch failed for %r - %r: %s", artist_name, title, e)
        if USE_SPOTIFY:
            try:
                out["spotify"] = fetch_provider_album_lookup_cached(
                    "spotify",
                    artist_name,
                    title,
                    _fetch_spotify_album_info,
                )
            except Exception as e:
                logging.debug("[Providers] spotify fetch failed for %r - %r: %s", artist_name, title, e)
        if USE_QOBUZ:
            try:
                out["qobuz"] = fetch_provider_album_lookup_cached(
                    "qobuz",
                    artist_name,
                    title,
                    _fetch_qobuz_album_info,
                )
            except Exception as e:
                logging.debug("[Providers] qobuz fetch failed for %r - %r: %s", artist_name, title, e)
        if USE_TIDAL:
            try:
                out["tidal"] = fetch_provider_album_lookup_cached(
                    "tidal",
                    artist_name,
                    title,
                    _fetch_tidal_album_info,
                )
            except Exception as e:
                logging.debug("[Providers] tidal fetch failed for %r - %r: %s", artist_name, title, e)
        if USE_BANDCAMP:
            try:
                out["bandcamp"] = fetch_provider_album_lookup_cached(
                    "bandcamp",
                    artist_name,
                    title,
                    _bandcamp_fetch_inline,
                )
            except Exception as e:
                logging.debug("[Providers] bandcamp fetch failed for %r - %r: %s", artist_name, title, e)
        if USE_LASTFM:
            try:
                out["lastfm"] = fetch_provider_album_lookup_cached(
                    "lastfm",
                    artist_name,
                    title,
                    _fetch_lastfm_album_info,
                )
            except Exception as e:
                logging.debug("[Providers] lastfm fetch failed for %r - %r: %s", artist_name, title, e)
        if THEAUDIODB_API_KEY:
            try:
                out["audiodb"] = fetch_provider_album_lookup_cached(
                    "audiodb",
                    artist_name,
                    title,
                    _fetch_audiodb_album_info,
                )
            except Exception as e:
                logging.debug("[Providers] audiodb fetch failed for %r - %r: %s", artist_name, title, e)
    else:
        tasks = {}
        timeout_total = float(
            getattr(
                sys.modules[__name__],
                "PROVIDER_FALLBACK_PARALLEL_TIMEOUT_SEC",
                PROVIDER_FALLBACK_PARALLEL_TIMEOUT_SEC,
            )
            or PROVIDER_FALLBACK_PARALLEL_TIMEOUT_SEC
        )
        pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="pmda-provider-fallback")
        try:
            if USE_DISCOGS:
                tasks[pool.submit(fetch_provider_album_lookup_cached, "discogs", artist_name, title, _fetch_discogs_release)] = "discogs"
            if USE_ITUNES:
                tasks[pool.submit(fetch_provider_album_lookup_cached, "itunes", artist_name, title, _fetch_itunes_album_info)] = "itunes"
            if USE_DEEZER:
                tasks[pool.submit(fetch_provider_album_lookup_cached, "deezer", artist_name, title, _fetch_deezer_album_info)] = "deezer"
            if USE_SPOTIFY:
                tasks[pool.submit(fetch_provider_album_lookup_cached, "spotify", artist_name, title, _fetch_spotify_album_info)] = "spotify"
            if USE_QOBUZ:
                tasks[pool.submit(fetch_provider_album_lookup_cached, "qobuz", artist_name, title, _fetch_qobuz_album_info)] = "qobuz"
            if USE_TIDAL:
                tasks[pool.submit(fetch_provider_album_lookup_cached, "tidal", artist_name, title, _fetch_tidal_album_info)] = "tidal"
            if USE_BANDCAMP:
                tasks[pool.submit(fetch_provider_album_lookup_cached, "bandcamp", artist_name, title, _bandcamp_fetch_inline)] = "bandcamp"
            if USE_LASTFM:
                tasks[pool.submit(fetch_provider_album_lookup_cached, "lastfm", artist_name, title, _fetch_lastfm_album_info)] = "lastfm"
            if THEAUDIODB_API_KEY:
                tasks[pool.submit(fetch_provider_album_lookup_cached, "audiodb", artist_name, title, _fetch_audiodb_album_info)] = "audiodb"
            if tasks:
                done, not_done = wait(set(tasks.keys()), timeout=max(1.0, timeout_total))
                for fut in done:
                    key = tasks[fut]
                    try:
                        out[key] = fut.result()
                    except Exception as e:
                        logging.debug("[Providers] %s fetch failed for %r - %r: %s", key, artist_name, title, e)
                if not_done:
                    timed_out_keys = [tasks[fut] for fut in not_done if fut in tasks]
                    logging.warning(
                        "[Providers] Timed out after %.1fs while checking %r - %r. Still waiting on: %s",
                        timeout_total,
                        artist_name,
                        title,
                        ", ".join(sorted(set(timed_out_keys))) or "unknown",
                    )
                    for fut in not_done:
                        key = tasks.get(fut)
                        if key:
                            try:
                                set_cached_provider_album_lookup(key, artist_name, title, "error", None)
                            except Exception:
                                pass
                        try:
                            fut.cancel()
                        except Exception:
                            pass
        finally:
            try:
                pool.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass

    discogs_info = out.get("discogs")
    if discogs_info:
        out["extra_sources"].append(
            {
                "source": "Discogs",
                "title": discogs_info.get("title"),
                "artist_name": discogs_info.get("artist_name"),
            }
        )
    itunes_info = out.get("itunes")
    if itunes_info:
        out["extra_sources"].append(
            {
                "source": "iTunes / Apple Music",
                "title": itunes_info.get("title"),
                "artist_name": itunes_info.get("artist_name"),
            }
        )
    deezer_info = out.get("deezer")
    if deezer_info:
        out["extra_sources"].append(
            {
                "source": "Deezer",
                "title": deezer_info.get("title"),
                "artist_name": deezer_info.get("artist_name"),
            }
        )
    spotify_info = out.get("spotify")
    if spotify_info:
        out["extra_sources"].append(
            {
                "source": "Spotify",
                "title": spotify_info.get("title"),
                "artist_name": spotify_info.get("artist_name"),
            }
        )
    qobuz_info = out.get("qobuz")
    if qobuz_info:
        out["extra_sources"].append(
            {
                "source": "Qobuz",
                "title": qobuz_info.get("title"),
                "artist_name": qobuz_info.get("artist_name"),
            }
        )
    bandcamp_info = out.get("bandcamp")
    if bandcamp_info:
        out["extra_sources"].append(
            {
                "source": "Bandcamp",
                "title": bandcamp_info.get("title"),
                "artist_name": bandcamp_info.get("artist_name"),
            }
        )
    lastfm_info = out.get("lastfm")
    if lastfm_info:
        out["extra_sources"].append(
            {
                "source": "Last.fm",
                "title": lastfm_info.get("title"),
                "artist": lastfm_info.get("artist"),
            }
        )
    tidal_info = out.get("tidal")
    if tidal_info:
        out["extra_sources"].append(
            {
                "source": "TIDAL",
                "title": tidal_info.get("title"),
                "artist_name": tidal_info.get("artist_name"),
            }
        )
    audiodb_info = out.get("audiodb")
    if audiodb_info:
        out["extra_sources"].append(
            {
                "source": "TheAudioDB",
                "title": audiodb_info.get("title"),
                "artist_name": audiodb_info.get("artist_name"),
            }
        )
    return out
