"""Runtime-owned logging format helpers for PMDA."""
from __future__ import annotations

from typing import Any

_RUNTIME: Any | None = None
_EXTRACTED_NAMES = {
    '_ansi_256_fg',
    '_ansi_256_bg',
    'log_header',
    'colour',
    '_humanize_log_thread_name',
    '_plain_log_record_line',
    '_pad_log_label',
    '_styled_log_pill',
    '_log_level_badge',
    '_log_thread_pill',
    '_parse_log_tag_body',
    '_parse_album_profile_progress',
    '_log_state_from_domain_body',
    '_log_marker_visual',
    '_log_domain_parts',
    '_summarize_pipeline_flags_for_log',
    '_log',
    'log_scan',
    'log_mb',
    'log_provider',
    'log_acoustid',
    'log_match',
    'log_soft',
    'log_miss',
    'log_ai',
    'log_dupes',
    'log_live',
    'log_path',
    'log_cfg',
    'log_cov',
    'log_art',
    'log_tag',
    '_compact_mb_rejection_reason',
    '_log_mb_candidate_rejection',
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
        raise RuntimeError("Logging runtime is not bound")
    return _RUNTIME

def _ansi_256_fg(code: int) -> str:
    return f"\033[38;5;{max(0, min(255, int(code)))}m"
def _ansi_256_bg(code: int) -> str:
    return f"\033[48;5;{max(0, min(255, int(code)))}m"
def log_header(title: str) -> None:
    """Print a bold cyan header like `----- TITLE -----`."""
    logging.info("\n" + colour(f"----- {title.upper()} -----", ANSI_BOLD + ANSI_CYAN))
def colour(txt: str, code: str) -> str:
    """Wrap *txt* in an ANSI colour code unless NO_COLOR env var is set."""
    if os.getenv("NO_COLOR"):
        return txt
    return f"{code}{txt}{ANSI_RESET}"
def _humanize_log_thread_name(thread_name: str) -> str:
    raw = str(thread_name or "").strip() or "thread"
    lowered = raw.lower()

    def _suffix_num(label: str) -> str:
        m = re.search(r"_(\d+)$", raw)
        if not m:
            return label
        try:
            return f"{label} {int(m.group(1)) + 1}"
        except Exception:
            return label

    if raw == "MainThread":
        return "startup"
    if raw.startswith("scan-"):
        return raw.replace("scan-", "scan:", 1)
    if raw.startswith("pmda-ai-bounded"):
        return _suffix_num("ai worker")
    if raw.startswith("pmda-call-bounded"):
        return _suffix_num("task worker")
    if raw.startswith("pmda-provider-fallback"):
        return _suffix_num("provider fallback")
    if raw.startswith("pmda-files-discovery"):
        return _suffix_num("files discovery")
    if raw.startswith("pmda-preflight"):
        return _suffix_num("preflight")
    if raw == "files-index-rebuild":
        return "index rebuild"
    if raw == "files-watcher-manager":
        return "watcher"
    if "process_request_thread" in lowered:
        return "http"
    if raw.startswith("ThreadPoolExecutor-"):
        return _suffix_num("worker")
    if raw.startswith("Thread-"):
        return "background"
    return raw.replace("_", " ")
def _plain_log_record_line(record: logging.LogRecord) -> str:
    return _logging_core.plain_log_record_line(record)
def _pad_log_label(value: str, width: int) -> str:
    text = str(value or "").strip()
    if len(text) > width:
        if width <= 1:
            return text[:width]
        return text[: width - 1] + "…"
    return text.ljust(width)
def _styled_log_pill(text: str, *, fg: str, bg: str, width: int) -> str:
    label = _pad_log_label(text, width)
    if os.getenv("NO_COLOR"):
        return f"[{label}]"
    return f"{ANSI_BOLD}{fg}{bg}[{label}]{ANSI_RESET}"
def _log_level_badge(levelno: int) -> str:
    if levelno >= logging.CRITICAL:
        return _styled_log_pill("CRIT", fg=ANSI_WHITE, bg=ANSI_BG_RED, width=5)
    if levelno >= logging.ERROR:
        return _styled_log_pill("ERROR", fg=ANSI_WHITE, bg=ANSI_BG_RED, width=5)
    if levelno >= logging.WARNING:
        return _styled_log_pill("WARN", fg=ANSI_BLACK, bg=ANSI_BG_YELLOW, width=5)
    if levelno <= logging.DEBUG:
        return _styled_log_pill("DEBUG", fg=ANSI_BLACK, bg=ANSI_BG_CYAN, width=5)
    return _styled_log_pill("INFO", fg=ANSI_BLACK, bg=ANSI_BG_GREEN, width=5)
def _log_thread_pill(thread_label: str) -> str:
    raw = str(thread_label or "").strip() or "thread"
    worker_palette: list[tuple[str, str]] = [
        (_ansi_256_fg(16), _ansi_256_bg(51)),   # aqua
        (_ansi_256_fg(16), _ansi_256_bg(226)),  # vivid yellow
        (_ansi_256_fg(231), _ansi_256_bg(196)), # red
        (_ansi_256_fg(231), _ansi_256_bg(27)),  # blue
        (_ansi_256_fg(16), _ansi_256_bg(46)),   # green
        (_ansi_256_fg(231), _ansi_256_bg(129)), # purple
        (_ansi_256_fg(16), _ansi_256_bg(208)),  # orange
        (_ansi_256_fg(231), _ansi_256_bg(161)), # pink
        (_ansi_256_fg(16), _ansi_256_bg(118)),  # lime
        (_ansi_256_fg(231), _ansi_256_bg(20)),  # indigo
        (_ansi_256_fg(16), _ansi_256_bg(87)),   # cyan-green
        (_ansi_256_fg(231), _ansi_256_bg(166)), # burnt orange
    ]
    generic_palette: list[tuple[str, str]] = [
        (_ansi_256_fg(16), _ansi_256_bg(45)),
        (_ansi_256_fg(231), _ansi_256_bg(57)),
        (_ansi_256_fg(16), _ansi_256_bg(150)),
        (_ansi_256_fg(231), _ansi_256_bg(88)),
        (_ansi_256_fg(16), _ansi_256_bg(220)),
        (_ansi_256_fg(231), _ansi_256_bg(25)),
        (_ansi_256_fg(16), _ansi_256_bg(111)),
        (_ansi_256_fg(231), _ansi_256_bg(90)),
    ]
    m = re.search(r"(\d+)$", raw)
    if raw.startswith("worker ") and m:
        try:
            slot = (int(m.group(1)) - 1) % len(worker_palette)
        except Exception:
            slot = zlib.crc32(raw.encode("utf-8", errors="ignore")) % len(worker_palette)
        fg, bg = worker_palette[slot]
    else:
        if m:
            try:
                slot = (int(m.group(1)) - 1) % len(generic_palette)
            except Exception:
                slot = zlib.crc32(raw.encode("utf-8", errors="ignore")) % len(generic_palette)
        else:
            slot = zlib.crc32(raw.encode("utf-8", errors="ignore")) % len(generic_palette)
        fg, bg = generic_palette[slot]
    if raw in {"startup", "http"}:
        fg, bg = (ANSI_BLACK, ANSI_BG_WHITE)
    elif raw in {"scan:full", "scan:quick", "scan:interactive"}:
        fg, bg = (ANSI_BLACK, ANSI_BG_GREEN)
    return _styled_log_pill(raw, fg=fg, bg=bg, width=_THREAD_PILL_WIDTH)
def _parse_log_tag_body(levelno: int, message: str) -> tuple[str, str]:
    raw = str(message or "").strip()
    tag = ""
    body = raw
    tag_match = _LOG_TAG_RE.match(raw)
    if tag_match:
        tag = str(tag_match.group("tag") or "").strip()
        body = raw[tag_match.end():].strip()
    elif raw.startswith("Processing artist:"):
        tag = "ARTIST"
        body = raw[len("Processing artist:"):].strip()
    elif levelno >= logging.ERROR:
        tag = "ERROR"
    elif levelno >= logging.WARNING:
        tag = "WARN"
    else:
        tag = "LOG"
    return tag, body
def _parse_album_profile_progress(body_lower: str) -> tuple[int, int] | None:
    m = re.search(r"album_profiles=(\d+)\s*/\s*(\d+)", body_lower)
    if not m:
        return None
    try:
        return int(m.group(1)), int(m.group(2))
    except Exception:
        return None
def _log_state_from_domain_body(levelno: int, domain_norm: str, body: str) -> str:
    lowered = str(body or "").strip().lower()
    if domain_norm == "match":
        return "success"
    if domain_norm == "miss":
        return "failure"
    if domain_norm == "soft":
        return "partial"
    if levelno >= logging.ERROR:
        return "failure"
    if levelno >= logging.WARNING:
        return "warning"
    if lowered.startswith("config "):
        return "info"

    if lowered.startswith("done artist='"):
        progress = _parse_album_profile_progress(lowered)
        profile_true = "artist_profile=true" in lowered
        image_true = "artist_image=true" in lowered
        profile_false = "artist_profile=false" in lowered
        image_false = "artist_image=false" in lowered
        if progress:
            done_count, total_count = progress
        else:
            done_count, total_count = (0, 0)
        if image_true or profile_true or (total_count > 0 and done_count > 0):
            return "success"
        if profile_false and image_false and (total_count == 0 or done_count == 0):
            return "failure"
        return "partial"

    failure_phrases = (
        "no verified candidate",
        "could not verify",
        "no usable ",
        "no provider candidates survived",
        "no matches for folder",
        "no release-groups",
        "failed:",
        " failed ",
        "exception on ",
        "traceback",
        "timed out after",
        "timed out.",
        "error=",
        "artist_image=false artist_profile=false",
        "image unavailable",
        "missing cover",
    )
    if any(phrase in lowered for phrase in failure_phrases):
        return "failure"

    skip_phrases = (
        "skipping ",
        "skip ",
        "allow soft ",
        "fallback sources ready",
        "deferred artist enrichment",
        "classical context detected",
        "reaped ",
        "still waiting on:",
        "suppressing further",
    )
    if any(phrase in lowered for phrase in skip_phrases):
        return "skip"

    progress_phrases = (
        "start artist=",
        "processing artist:",
        "prefiltered ",
        "candidate(s) from search",
        "file(s) fingerprinted",
        "from lookup",
        "starting full scan",
        "scan pipeline",
        "building album candidates",
        "checkpoint starting",
        "refresh queued",
    )
    if any(phrase in lowered for phrase in progress_phrases):
        return "progress"

    success_checks = (
        lowered.startswith("matched "),
        bool(re.search(r"\baccepted\b", lowered)),
        "selected=" in lowered,
        "trusted via " in lowered,
        "probable via " in lowered,
        "provider identity accepted" in lowered,
        lowered.startswith("found "),
        " upserted " in f" {lowered} ",
        lowered.startswith("cached "),
        "promoted " in lowered,
        lowered.startswith("merged "),
        "completed in " in lowered,
        lowered.startswith("refreshed "),
        lowered.startswith("wrote "),
        lowered.startswith("saved "),
        "cover selected" in lowered,
        "index is ready" in lowered,
    )
    if any(success_checks):
        return "success"

    if lowered.startswith("folder ") or lowered.startswith("album "):
        return "progress"
    return "info"
def _log_marker_visual(state: str) -> tuple[str, str, str]:
    mapping = {
        "success": ("V✅", ANSI_BLACK, ANSI_BG_GREEN),
        "failure": ("X❌", ANSI_WHITE, ANSI_BG_RED),
        "partial": ("~⚠", ANSI_BLACK, ANSI_BG_YELLOW),
        "warning": ("!⚠", ANSI_BLACK, ANSI_BG_YELLOW),
        "skip": ("»⏭", ANSI_BLACK, ANSI_BG_YELLOW),
        "progress": ("↻🔄", ANSI_BLACK, ANSI_BG_CYAN),
        "info": ("·ℹ", ANSI_BLACK, ANSI_BG_WHITE),
    }
    return mapping.get(state, mapping["info"])
def _log_domain_parts(levelno: int, message: str) -> tuple[str, str, str]:
    tag, body = _parse_log_tag_body(levelno, message)
    state = _log_state_from_domain_body(levelno, tag.lower(), body)

    norm = tag.lower()
    domain_label = tag.upper()
    fg = ANSI_WHITE
    bg = ANSI_BG_BLACK

    if norm == "match":
        domain_label = "MATCH"
        fg, bg = ANSI_BLACK, ANSI_BG_GREEN
    elif norm == "soft":
        domain_label = "SOFT"
        fg, bg = ANSI_BLACK, ANSI_BG_YELLOW
    elif norm == "miss":
        domain_label = "MISS"
        fg, bg = ANSI_WHITE, ANSI_BG_RED
    elif norm in {"mb", "musicbrainz"}:
        domain_label = "MB"
        fg, bg = ANSI_BLACK, ANSI_BG_CYAN
    elif norm in {"scan pipeline", "scan_pipeline"}:
        domain_label = "PIPELINE"
        fg, bg = ANSI_WHITE, ANSI_BG_BLUE
    elif norm == "providers":
        domain_label = "PROVIDER"
        fg, bg = ANSI_WHITE, ANSI_BG_BLUE
    elif norm == "scan":
        domain_label = "SCAN"
        fg, bg = ANSI_BLACK, ANSI_BG_GREEN
    elif norm == "ai":
        domain_label = "AI"
        fg, bg = ANSI_WHITE, ANSI_BG_MAGENTA
    elif norm == "path":
        domain_label = "PATH"
        fg, bg = ANSI_BLACK, ANSI_BG_GREEN
    elif norm == "cfg":
        domain_label = "CFG"
        fg, bg = ANSI_WHITE, ANSI_BG_MAGENTA
    elif norm in {"cov", "cover"}:
        domain_label = "COVER"
        fg, bg = ANSI_WHITE, ANSI_BG_BLUE
    elif norm in {"art", "artist", "artist image", "artist_image"}:
        domain_label = "ARTIST"
        fg, bg = ANSI_BLACK, ANSI_BG_CYAN
    elif norm == "tag":
        domain_label = "TAG"
        fg, bg = ANSI_BLACK, ANSI_BG_CYAN
    elif norm == "acoustid":
        domain_label = "ACOUSTID"
        fg, bg = ANSI_WHITE, ANSI_BG_MAGENTA
    elif norm in {"auth_event", "auth"}:
        domain_label = "AUTH"
        fg, bg = ANSI_BLACK, ANSI_BG_WHITE
    elif norm in {"warn", "warning"}:
        domain_label = "WARN"
        fg, bg = ANSI_BLACK, ANSI_BG_YELLOW
    elif norm == "error":
        domain_label = "ERROR"
        fg, bg = ANSI_WHITE, ANSI_BG_RED

    marker, marker_fg, marker_bg = _log_marker_visual(state)
    if os.getenv("NO_COLOR"):
        domain_pill = f"[{_pad_log_label(domain_label, _DOMAIN_PILL_WIDTH)}]"
        marker_pill = f"[{_pad_log_label(marker, 3)}]"
    else:
        domain_pill = _styled_log_pill(domain_label, fg=fg, bg=bg, width=_DOMAIN_PILL_WIDTH)
        marker_pill = _styled_log_pill(marker, fg=marker_fg, bg=marker_bg, width=3)
    return domain_pill, marker_pill, body
def _summarize_pipeline_flags_for_log(flags: dict[str, Any] | None) -> str:
    return _scan_orchestrator_core.summarize_pipeline_flags(flags)
def _log(domain: str, level: int, msg: str, *args, **kwargs) -> None:
    """Internal helper to log with a [TAG] prefix and domain-aware colouring."""
    prefix = f"[{domain}] "
    logging.log(level, prefix + msg, *args, **kwargs)
def log_scan(msg: str, *args, **kwargs) -> None:
    _log("SCAN", logging.INFO, msg, *args, **kwargs)
def log_mb(msg: str, *args, **kwargs) -> None:
    _log("MB", logging.INFO, msg, *args, **kwargs)
def log_provider(msg: str, *args, **kwargs) -> None:
    _log("Providers", logging.INFO, msg, *args, **kwargs)
def log_acoustid(msg: str, *args, **kwargs) -> None:
    _log("AcousticID", logging.INFO, msg, *args, **kwargs)
def log_match(msg: str, *args, **kwargs) -> None:
    _log("MATCH", logging.INFO, msg, *args, **kwargs)
def log_soft(msg: str, *args, **kwargs) -> None:
    _log("SOFT", logging.INFO, msg, *args, **kwargs)
def log_miss(msg: str, *args, **kwargs) -> None:
    _log("MISS", logging.INFO, msg, *args, **kwargs)
def log_ai(msg: str, *args, **kwargs) -> None:
    _log("AI", logging.INFO, msg, *args, **kwargs)
def log_dupes(msg: str, *args, **kwargs) -> None:
    _log("DUPES", logging.INFO, msg, *args, **kwargs)
def log_live(msg: str, *args, **kwargs) -> None:
    _log("LIVE", logging.INFO, msg, *args, **kwargs)
def log_path(msg: str, *args, **kwargs) -> None:
    _log("PATH", logging.INFO, msg, *args, **kwargs)
def log_cfg(msg: str, *args, **kwargs) -> None:
    _log("CFG", logging.INFO, msg, *args, **kwargs)
def log_cov(msg: str, *args, **kwargs) -> None:
    """Logging helper for cover artwork operations."""
    _log("COV", logging.INFO, msg, *args, **kwargs)
def log_art(msg: str, *args, **kwargs) -> None:
    """Logging helper for artist-image operations."""
    _log("ART", logging.INFO, msg, *args, **kwargs)
def log_tag(msg: str, *args, **kwargs) -> None:
    """Logging helper for tag read/write operations."""
    _log("TAG", logging.INFO, msg, *args, **kwargs)
def _compact_mb_rejection_reason(reason: str) -> str:
    raw = str(reason or "").strip()
    lowered = raw.lower()
    flags: list[str] = []
    checks = [
        ("artist partial overlap", "artist~"),
        ("artist mismatch", "artist"),
        ("title mismatch", "title"),
        ("track_count_mismatch", "tracks"),
        ("classical_performance_mismatch", "classical-perf"),
        ("classical_composer_mismatch", "classical-composer"),
        ("classical_track_count_mismatch", "classical-tracks"),
        ("provider_no_tracklist", "no-tracklist"),
        ("provider_id_missing", "missing-id"),
        ("album_mismatch", "album"),
    ]
    for needle, label in checks:
        if needle in lowered and label not in flags:
            flags.append(label)
    if flags:
        return ", ".join(flags)
    if len(raw) > 96:
        return raw[:93] + "..."
    return raw or "?"
def _log_mb_candidate_rejection(local_artist: str, local_title: str, candidate_id: str, reason: str) -> None:
    key = (
        _normalize_loose_string(local_artist),
        _normalize_loose_string(local_title),
    )
    with _MB_CANDIDATE_REJECTION_LOCK:
        count = _MB_CANDIDATE_REJECTION_COUNTS.get(key, 0) + 1
        _MB_CANDIDATE_REJECTION_COUNTS[key] = count
    if count <= _MB_CANDIDATE_REJECTION_LIMIT:
        log_mb(
            "Album %s – \"%s\": reject #%d candidate=%s (%s)",
            local_artist,
            local_title,
            count,
            candidate_id or "?",
            _compact_mb_rejection_reason(reason),
        )
        return
    if count == _MB_CANDIDATE_REJECTION_LIMIT + 1:
        log_mb(
            "Album %s – \"%s\": suppressing further MB rejects after %d entry (latest=%s, flags=%s)",
            local_artist,
            local_title,
            _MB_CANDIDATE_REJECTION_LIMIT,
            candidate_id or "?",
            _compact_mb_rejection_reason(reason),
        )
        return
    logging.debug(
        "[MB] Album %s – %r: candidate %s rejected before arbitration (%s)",
        local_artist,
        local_title,
        candidate_id or "?",
        reason or "?",
    )

_ORIGINAL_EXTRACTED_FUNCTIONS = {name: globals().get(name) for name in _EXTRACTED_NAMES}

def _ansi_256_fg_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ansi_256_fg(*args, **kwargs)

def _ansi_256_bg_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _ansi_256_bg(*args, **kwargs)

def log_header_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return log_header(*args, **kwargs)

def colour_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return colour(*args, **kwargs)

def _humanize_log_thread_name_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _humanize_log_thread_name(*args, **kwargs)

def _plain_log_record_line_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _plain_log_record_line(*args, **kwargs)

def _pad_log_label_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _pad_log_label(*args, **kwargs)

def _styled_log_pill_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _styled_log_pill(*args, **kwargs)

def _log_level_badge_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _log_level_badge(*args, **kwargs)

def _log_thread_pill_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _log_thread_pill(*args, **kwargs)

def _parse_log_tag_body_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _parse_log_tag_body(*args, **kwargs)

def _parse_album_profile_progress_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _parse_album_profile_progress(*args, **kwargs)

def _log_state_from_domain_body_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _log_state_from_domain_body(*args, **kwargs)

def _log_marker_visual_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _log_marker_visual(*args, **kwargs)

def _log_domain_parts_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _log_domain_parts(*args, **kwargs)

def _summarize_pipeline_flags_for_log_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _summarize_pipeline_flags_for_log(*args, **kwargs)

def _log_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _log(*args, **kwargs)

def log_scan_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return log_scan(*args, **kwargs)

def log_mb_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return log_mb(*args, **kwargs)

def log_provider_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return log_provider(*args, **kwargs)

def log_acoustid_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return log_acoustid(*args, **kwargs)

def log_match_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return log_match(*args, **kwargs)

def log_soft_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return log_soft(*args, **kwargs)

def log_miss_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return log_miss(*args, **kwargs)

def log_ai_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return log_ai(*args, **kwargs)

def log_dupes_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return log_dupes(*args, **kwargs)

def log_live_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return log_live(*args, **kwargs)

def log_path_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return log_path(*args, **kwargs)

def log_cfg_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return log_cfg(*args, **kwargs)

def log_cov_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return log_cov(*args, **kwargs)

def log_art_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return log_art(*args, **kwargs)

def log_tag_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return log_tag(*args, **kwargs)

def _compact_mb_rejection_reason_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _compact_mb_rejection_reason(*args, **kwargs)

def _log_mb_candidate_rejection_for_runtime(runtime: Any, *args: Any, **kwargs: Any) -> Any:
    _bind_runtime(runtime)
    return _log_mb_candidate_rejection(*args, **kwargs)
