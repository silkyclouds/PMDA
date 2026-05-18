"""Duplicate detection signal helpers."""

from __future__ import annotations

from typing import Any

_RUNTIME: Any | None = None


def _bind_runtime(runtime: Any) -> None:
    """Bind live PMDA globals for duplicate-signal helpers."""
    global _RUNTIME
    _RUNTIME = runtime
    blocked = {
        '_fold_music_ascii_for_runtime',
        '_dupe_norm_track_title_for_runtime',
        '_dupe_track_title_set_for_runtime',
        '_dupe_jaccard_for_runtime',
        '_dupe_track_title_containment_for_runtime',
        '_dupe_track_count_ratio_for_runtime',
        '_edition_track_count_for_dupe_for_runtime',
        '_edition_total_duration_for_dupe_for_runtime',
        '_dupe_group_has_exact_provider_trackcount_signal_for_runtime',
        '_dupe_get_mb_release_group_id_for_runtime',
        '_dupe_get_mb_release_id_for_runtime',
        '_dupe_get_discogs_id_for_runtime',
        '_dupe_get_lastfm_mbid_for_runtime',
        '_dupe_get_bandcamp_url_for_runtime',
        '_dupe_audio_fp_set_for_edition_for_runtime',
        '_dupe_audio_sig_for_edition_for_runtime',
        '_dupe_split_editions_by_similarity_for_runtime',
        'editions_share_confident_signal_for_runtime',
        "_bind_runtime",
    }
    globals().update({key: value for key, value in vars(runtime).items() if key not in blocked})


def _fold_music_ascii_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_fold_music_ascii`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _fold_music_ascii_impl(*args, **kwargs)

def _dupe_norm_track_title_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_dupe_norm_track_title`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _dupe_norm_track_title_impl(*args, **kwargs)

def _dupe_track_title_set_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_dupe_track_title_set`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _dupe_track_title_set_impl(*args, **kwargs)

def _dupe_jaccard_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_dupe_jaccard`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _dupe_jaccard_impl(*args, **kwargs)

def _dupe_track_title_containment_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_dupe_track_title_containment`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _dupe_track_title_containment_impl(*args, **kwargs)

def _dupe_track_count_ratio_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_dupe_track_count_ratio`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _dupe_track_count_ratio_impl(*args, **kwargs)

def _edition_track_count_for_dupe_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_edition_track_count_for_dupe`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _edition_track_count_for_dupe_impl(*args, **kwargs)

def _edition_total_duration_for_dupe_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_edition_total_duration_for_dupe`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _edition_total_duration_for_dupe_impl(*args, **kwargs)

def _dupe_group_has_exact_provider_trackcount_signal_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_dupe_group_has_exact_provider_trackcount_signal`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _dupe_group_has_exact_provider_trackcount_signal_impl(*args, **kwargs)

def _dupe_get_mb_release_group_id_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_dupe_get_mb_release_group_id`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _dupe_get_mb_release_group_id_impl(*args, **kwargs)

def _dupe_get_mb_release_id_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_dupe_get_mb_release_id`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _dupe_get_mb_release_id_impl(*args, **kwargs)

def _dupe_get_discogs_id_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_dupe_get_discogs_id`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _dupe_get_discogs_id_impl(*args, **kwargs)

def _dupe_get_lastfm_mbid_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_dupe_get_lastfm_mbid`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _dupe_get_lastfm_mbid_impl(*args, **kwargs)

def _dupe_get_bandcamp_url_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_dupe_get_bandcamp_url`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _dupe_get_bandcamp_url_impl(*args, **kwargs)

def _dupe_audio_fp_set_for_edition_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_dupe_audio_fp_set_for_edition`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _dupe_audio_fp_set_for_edition_impl(*args, **kwargs)

def _dupe_audio_sig_for_edition_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_dupe_audio_sig_for_edition`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _dupe_audio_sig_for_edition_impl(*args, **kwargs)

def _dupe_split_editions_by_similarity_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``_dupe_split_editions_by_similarity`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return _dupe_split_editions_by_similarity_impl(*args, **kwargs)

def editions_share_confident_signal_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    """Run ``editions_share_confident_signal`` against the live PMDA runtime."""
    _bind_runtime(runtime)
    return editions_share_confident_signal_impl(*args, **kwargs)


def _fold_music_ascii_impl(value: str | None) -> str:
    """Fold music-name characters that vanilla NFKD drops entirely."""
    return str(value or "").translate(_MUSIC_ASCII_FOLD)


def _dupe_norm_track_title_impl(s: str) -> str:
    """Normalize a track title for dupe similarity (robust to tags/filename noise)."""
    cleaned = _clean_track_title_from_text(str(s or ""), 1)
    raw = (cleaned or s or "").strip().lower()
    if not raw:
        return ""
    raw = raw.replace("…", "...").replace("…", "...")
    raw = raw.replace("&", " and ")
    raw = _fold_music_ascii(raw)
    raw = (
        unicodedata.normalize("NFKD", raw)
        .encode("ascii", "ignore")
        .decode("ascii")
    )
    # Drop bracket/parenthetical segments and leading album/file prefix patterns.
    raw = re.sub(r"[\(\[][^)\]]*[\)\]]", " ", raw)
    raw = re.sub(r"^\s*[^-]+?\s*-\s*[^-]+?\s*-\s*", "", raw)
    raw = re.sub(r"^\s*[^-]+?\s*-\s*", "", raw)
    raw = re.sub(r"^\s*\d+\s*[-.)]\s*", "", raw)
    raw = re.sub(r"^\s*\d{1,2}\s*[-_.]\s*\d{1,3}\s*[-_. ]*", "", raw)
    raw = re.sub(r"\s+", " ", raw)
    raw = raw.strip()
    # Strip punctuation, keep letters/numbers/spaces.
    raw = re.sub(r"[^\w\s]+", " ", raw)
    raw = " ".join(raw.split())
    return raw[:240]


def _dupe_track_title_set_impl(tracks: list) -> set[str]:
    """Return a normalized set of track titles for an edition."""
    out: set[str] = set()
    for t in tracks or []:
        title = ""
        try:
            if isinstance(t, dict):
                title = str(t.get("title") or t.get("name") or "")
            else:
                title = str(getattr(t, "title", "") or "")
        except Exception:
            title = ""
        nt = _dupe_norm_track_title(title)
        if nt:
            out.add(nt)
    return out


def _dupe_jaccard_impl(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return (float(inter) / float(union)) if union else 0.0


def _dupe_track_title_containment_impl(a: set[str], b: set[str]) -> float:
    """
    Containment score for truncated releases:
    |A∩B| / min(|A|, |B|). 1.0 means the smaller set is fully contained.
    """
    if not a or not b:
        return 0.0
    inter = len(a & b)
    denom = min(len(a), len(b))
    return (float(inter) / float(denom)) if denom else 0.0


def _dupe_track_count_ratio_impl(a_tracks: list, b_tracks: list) -> float:
    a_n = len(a_tracks or [])
    b_n = len(b_tracks or [])
    if a_n <= 0 and b_n <= 0:
        return 1.0
    if a_n <= 0 or b_n <= 0:
        return 0.0
    return float(min(a_n, b_n)) / float(max(a_n, b_n))


def _edition_track_count_for_dupe_impl(e: dict) -> int:
    tracks = e.get("tracks") or []
    if isinstance(tracks, list) and tracks:
        return int(len(tracks))
    for key in ("track_count", "actual_track_count", "expected_track_count"):
        try:
            parsed = int(e.get(key) or 0)
        except Exception:
            parsed = 0
        if parsed > 0:
            return parsed
    return 0


def _edition_total_duration_for_dupe_impl(e: dict) -> int:
    try:
        parsed = int(e.get("dur") or 0)
    except Exception:
        parsed = 0
    if parsed > 0:
        return parsed
    total = 0
    for tr in (e.get("tracks") or []):
        try:
            total += int(getattr(tr, "dur", 0) or (tr.get("dur") if isinstance(tr, dict) else 0) or 0)
        except Exception:
            continue
    return max(0, int(total))


def _dupe_group_has_exact_provider_trackcount_signal_impl(group: list[dict]) -> bool:
    if len(group or []) < 2:
        return False
    shared_provider_id = False
    provider_sets = [
        {e.get("_dupe_mb_rg") or _dupe_get_mb_release_group_id(e) for e in group if (e.get("_dupe_mb_rg") or _dupe_get_mb_release_group_id(e))},
        {e.get("_dupe_mb_rel") or _dupe_get_mb_release_id(e) for e in group if (e.get("_dupe_mb_rel") or _dupe_get_mb_release_id(e))},
        {e.get("_dupe_discogs") or _dupe_get_discogs_id(e) for e in group if (e.get("_dupe_discogs") or _dupe_get_discogs_id(e))},
        {e.get("_dupe_lastfm") or _dupe_get_lastfm_mbid(e) for e in group if (e.get("_dupe_lastfm") or _dupe_get_lastfm_mbid(e))},
        {e.get("_dupe_bandcamp") or _dupe_get_bandcamp_url(e) for e in group if (e.get("_dupe_bandcamp") or _dupe_get_bandcamp_url(e))},
    ]
    for values in provider_sets:
        if len(values) == 1:
            shared_provider_id = True
            break
    if not shared_provider_id:
        return False
    counts = {_edition_track_count_for_dupe(e) for e in group if _edition_track_count_for_dupe(e) > 0}
    if len(counts) != 1:
        return False
    durations = [_edition_total_duration_for_dupe(e) for e in group if _edition_total_duration_for_dupe(e) > 0]
    if len(durations) >= 2:
        lo = min(durations)
        hi = max(durations)
        if lo <= 0 or ((hi - lo) / float(hi)) > 0.03:
            return False
    return True


def _dupe_get_mb_release_group_id_impl(e: dict) -> str:
    try:
        rg = (e.get("rg_info") or {}).get("id")
    except Exception:
        rg = None
    if rg:
        return str(rg).strip()
    if e.get("musicbrainz_id"):
        return str(e.get("musicbrainz_id") or "").strip()
    meta = e.get("meta") or {}
    if isinstance(meta, dict):
        for k in ("musicbrainz_releasegroupid", "musicbrainz_release_group_id"):
            v = (meta.get(k) or "").strip()
            if v:
                return v
        # Some pipelines store RGID into albumid; accept as last resort.
        for k in ("musicbrainz_albumid", "musicbrainz_releaseid"):
            v = (meta.get(k) or "").strip()
            if v and re.fullmatch(r"[0-9a-fA-F-]{36}", v):
                return v
    return ""


def _dupe_get_mb_release_id_impl(e: dict) -> str:
    meta = e.get("meta") or {}
    if isinstance(meta, dict):
        for k in ("musicbrainz_releaseid", "musicbrainz_albumid", "musicbrainz_originalreleaseid"):
            v = (meta.get(k) or "").strip()
            if v and re.fullmatch(r"[0-9a-fA-F-]{36}", v):
                return v
    return ""


def _dupe_get_discogs_id_impl(e: dict) -> str:
    v = (e.get("discogs_release_id") or "").strip()
    if v:
        return v
    meta = e.get("meta") or {}
    if isinstance(meta, dict):
        v = (meta.get("discogs_release_id") or meta.get("discogs_releaseid") or "").strip()
        if v:
            return v
    fb = e.get("fallback_discogs")
    if isinstance(fb, dict):
        v = str(fb.get("release_id") or fb.get("master_id") or "").strip()
        if v:
            return v
    return ""


def _dupe_get_lastfm_mbid_impl(e: dict) -> str:
    v = (e.get("lastfm_album_mbid") or "").strip()
    if v:
        return v
    meta = e.get("meta") or {}
    if isinstance(meta, dict):
        v = (meta.get("lastfm_album_mbid") or "").strip()
        if v:
            return v
    fb = e.get("fallback_lastfm")
    if isinstance(fb, dict):
        v = str(fb.get("mbid") or "").strip()
        if v:
            return v
    return ""


def _dupe_get_bandcamp_url_impl(e: dict) -> str:
    v = (e.get("bandcamp_album_url") or "").strip()
    if v:
        return v
    meta = e.get("meta") or {}
    if isinstance(meta, dict):
        v = (meta.get("bandcamp_album_url") or "").strip()
        if v:
            return v
    fb = e.get("fallback_bandcamp")
    if isinstance(fb, dict):
        v = str(fb.get("album_url") or fb.get("url") or "").strip()
        if v:
            return v
    return ""


def _dupe_audio_fp_set_for_edition_impl(e: dict, *, max_tracks: int = 12) -> set[str]:
    """
    Return a set of chromaprint fingerprints for the edition (cached in cache.db).
    Used as high-precision evidence for duplicates when titles/tags are messy.
    """
    cached = e.get("_dupe_audio_fp_set")
    if isinstance(cached, set):
        return cached

    folder = e.get("folder")
    if not folder:
        e["_dupe_audio_fp_set"] = set()
        return set()
    try:
        folder_path = path_for_fs_access(Path(folder))
    except Exception:
        folder_path = Path(folder)
    if not folder_path or not folder_path.exists():
        e["_dupe_audio_fp_set"] = set()
        return set()

    paths: list[Path] = []
    try:
        ordered = e.get("ordered_paths") or []
        if ordered:
            for p in ordered:
                try:
                    pp = path_for_fs_access(Path(p))
                except Exception:
                    pp = Path(p)
                if pp and pp.is_file() and AUDIO_RE.search(pp.name):
                    paths.append(pp)
    except Exception:
        paths = []
    if not paths:
        try:
            paths = sorted([p for p in folder_path.rglob("*") if p.is_file() and AUDIO_RE.search(p.name)], key=lambda p: str(p))[: max(1, int(max_tracks or 12))]
        except Exception:
            paths = []

    fps: set[str] = set()
    for p in paths[: max(1, int(max_tracks or 12))]:
        try:
            path_str = str(p)
            cached_track = get_cached_acoustid(path_str)
            if cached_track:
                _dur, fp = cached_track
                if fp:
                    fps.add(str(fp))
                    continue
            # Compute via fpcalc (subprocess) and store.
            res = _fpcalc_fingerprint_file(path_str, length_sec=120, timeout_sec=45)
            if not res:
                continue
            dur, fp = res
            if fp:
                set_cached_acoustid(path_str, dur, fp)
                fps.add(str(fp))
        except Exception:
            continue

    e["_dupe_audio_fp_set"] = fps
    return fps


def _dupe_audio_sig_for_edition_impl(
    e: dict,
    *,
    max_tracks: int = 10,
    min_fps: int = 3,
    compute_missing: bool = False,
) -> str:
    """
    Return a stable-ish album audio signature derived from a small set of track fingerprints.
    - When compute_missing=False, only uses cached fingerprints (cheap).
    - When compute_missing=True, will compute missing fingerprints via fpcalc and cache them.
    """
    cached_sig = e.get("_dupe_audio_sig")
    if isinstance(cached_sig, str) and cached_sig:
        return cached_sig

    folder = e.get("folder")
    if not folder:
        e["_dupe_audio_sig"] = ""
        return ""
    try:
        folder_path = path_for_fs_access(Path(folder))
    except Exception:
        folder_path = Path(folder)
    if not folder_path or not folder_path.exists():
        e["_dupe_audio_sig"] = ""
        return ""

    paths: list[Path] = []
    try:
        ordered = e.get("ordered_paths") or []
        if ordered:
            for p in ordered:
                try:
                    pp = path_for_fs_access(Path(p))
                except Exception:
                    pp = Path(p)
                if pp and pp.is_file() and AUDIO_RE.search(pp.name):
                    paths.append(pp)
    except Exception:
        paths = []
    if not paths:
        try:
            paths = sorted(
                [p for p in folder_path.rglob("*") if p.is_file() and AUDIO_RE.search(p.name)],
                key=lambda p: str(p),
            )
        except Exception:
            paths = []

    fps: list[str] = []
    for p in (paths or [])[: max(1, int(max_tracks or 10))]:
        try:
            path_str = str(p)
            cached_track = get_cached_acoustid(path_str)
            if cached_track:
                _dur, fp = cached_track
                if fp:
                    fps.append(str(fp))
                    continue
            if not compute_missing:
                continue
            res = _fpcalc_fingerprint_file(path_str, length_sec=120, timeout_sec=45)
            if not res:
                continue
            dur, fp = res
            if fp:
                set_cached_acoustid(path_str, dur, fp)
                fps.append(str(fp))
        except Exception:
            continue

    # Require some fingerprints to avoid collisions.
    if len(fps) < max(1, int(min_fps or 3)):
        e["_dupe_audio_sig"] = ""
        return ""

    # Hash fingerprints to keep payload small and stable.
    digests: list[str] = []
    for fp in fps:
        try:
            digests.append(hashlib.sha1(fp.encode("utf-8", errors="ignore")).hexdigest())
        except Exception:
            continue
    digests = sorted(set(digests))
    payload = "audio_sig_v1\n" + "\n".join(digests) + f"\nN={len(digests)}"
    sig = hashlib.sha1(payload.encode("utf-8", errors="ignore")).hexdigest()
    e["_dupe_audio_sig"] = sig
    return sig


def _dupe_split_editions_by_similarity_impl(
    editions: list[dict],
    *,
    min_jaccard: float = 0.82,
    min_ratio: float = 0.75,
    allow_audio_fp: bool = True,
    audio_min_overlap: float = 0.87,
    partial_containment_min: float = 0.98,
    partial_ratio_max: float = 0.45,
) -> list[list[dict]]:
    """
    Split a noisy candidate group into one or more coherent clusters using track-title
    similarity (and optionally chromaprint overlap as a tie-break).
    """
    if not editions:
        return []
    if len(editions) <= 1:
        return [editions]

    # Local union-find over list indices.
    n = len(editions)
    parent = list(range(n))
    rank = [0] * n

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(i: int, j: int) -> None:
        ri = find(i)
        rj = find(j)
        if ri == rj:
            return
        if rank[ri] < rank[rj]:
            parent[ri] = rj
        elif rank[ri] > rank[rj]:
            parent[rj] = ri
        else:
            parent[rj] = ri
            rank[ri] += 1

    title_sets: list[set[str]] = []
    track_lists: list[list] = []
    for e in editions:
        tr = e.get("tracks") or []
        track_lists.append(tr)
        cached_titles = e.get("_dupe_track_title_set")
        if isinstance(cached_titles, set):
            title_sets.append(cached_titles)
        else:
            ts = _dupe_track_title_set(tr)
            e["_dupe_track_title_set"] = ts
            title_sets.append(ts)

    # Pairwise comparisons within this small candidate set only.
    for i in range(n):
        for j in range(i + 1, n):
            ratio = _dupe_track_count_ratio(track_lists[i], track_lists[j])
            if ratio < 0.55:
                # Truncated duplicate rescue: strong title containment + very low count ratio.
                # This catches 1-track/3-track partial copies of full releases.
                containment = _dupe_track_title_containment(title_sets[i], title_sets[j])
                if containment >= float(partial_containment_min) and ratio <= float(partial_ratio_max):
                    union(i, j)
                continue
            jac = _dupe_jaccard(title_sets[i], title_sets[j])
            if jac >= float(min_jaccard) and ratio >= float(min_ratio):
                union(i, j)
                continue
            # Optional tie-break: if track titles are messy but audio overlaps, keep together.
            if allow_audio_fp and jac >= 0.55 and ratio >= 0.60:
                a = _dupe_audio_fp_set_for_edition(editions[i])
                b = _dupe_audio_fp_set_for_edition(editions[j])
                if a and b:
                    ov = (len(a & b) / max(len(a), len(b))) if max(len(a), len(b)) else 0.0
                    if ov >= float(audio_min_overlap):
                        union(i, j)

    comps: dict[int, list[dict]] = defaultdict(list)
    for i, e in enumerate(editions):
        comps[find(i)].append(e)
    return [c for c in comps.values() if c]


def editions_share_confident_signal_impl(ed_list: List[dict]) -> bool:
    """
    Determine whether a potential duplicate group has enough evidence to be trusted.
    Accept when at least two editions have high-confidence titles, all track
    signatures match, they share the same MusicBrainz release-group ID, or
    all have the same album_norm (e.g. folder-derived titles like "Album [dupe]").
    """
    if len(ed_list) < 2:
        return False

    high_conf_prefixes = {"plex", "tag"}
    high_conf_titles = sum(
        1 for e in ed_list
        if e.get("title_source", "").partition(":")[0] in high_conf_prefixes
    )
    if high_conf_titles >= 2:
        return True

    sigs = {e.get("sig") for e in ed_list if e.get("sig")}
    if len(sigs) == 1 and sigs:
        return True

    rg_ids = {e.get("rg_info", {}).get("id") for e in ed_list if e.get("rg_info", {}).get("id")}
    if len(rg_ids) == 1 and rg_ids:
        return True

    # Dupe Detection v2: accept when provider IDs match even if rg_info is absent.
    mb_rg_ids = {_dupe_get_mb_release_group_id(e) for e in ed_list}
    mb_rg_ids = {x for x in mb_rg_ids if x}
    if len(mb_rg_ids) == 1 and mb_rg_ids:
        return True

    discogs_ids = {_dupe_get_discogs_id(e) for e in ed_list}
    discogs_ids = {x for x in discogs_ids if x}
    if len(discogs_ids) == 1 and discogs_ids:
        return True

    lastfm_mbids = {_dupe_get_lastfm_mbid(e) for e in ed_list}
    lastfm_mbids = {x for x in lastfm_mbids if x}
    if len(lastfm_mbids) == 1 and lastfm_mbids:
        return True

    bandcamp_urls = {_dupe_get_bandcamp_url(e) for e in ed_list}
    bandcamp_urls = {x for x in bandcamp_urls if x}
    if len(bandcamp_urls) == 1 and bandcamp_urls:
        return True

    # Same normalized title (e.g. "Night Cycle" and "Night Cycle [dupe]" -> "night cycle")
    norms = {e.get("album_norm") for e in ed_list if e.get("album_norm")}
    if len(norms) == 1 and norms:
        return True

    # Same Plex-normalized title (we grouped by this; accept so scan results match library)
    plex_norms = {e.get("plex_norm") for e in ed_list if e.get("plex_norm")}
    if len(plex_norms) == 1 and plex_norms:
        return True

    # Dupe Detection v2: same loose normalized title (aggressive noise stripping).
    loose_norms = {(e.get("_dupe_title_norm_loose") or "").strip() for e in ed_list if e.get("_dupe_title_norm_loose")}
    loose_norms = {x for x in loose_norms if x and not x.startswith("__untitled__")}
    if len(loose_norms) == 1 and loose_norms:
        return True

    return False
