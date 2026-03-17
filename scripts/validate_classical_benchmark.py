#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib import error, request


HOST = "root@192.168.3.2"
PMDA_BASE = "http://192.168.3.2:5005"
BENCH_ROOT = Path("/mnt/user/MURRAY/Music/pmda_scan_benchmark_classical")
ACTIVE_SCAN_ROOT = Path("/mnt/user/MURRAY/Music/pmda_scan_benchmark")
REPORT_ROOT = Path(__file__).resolve().parents[1] / ".tmp" / "classical-validation"
MANIFEST_PATH = BENCH_ROOT / "manifest.json"
DEDUPE_TRANSFORMS = {"dupe_exact", "title_variant", "no_tags", "no_cover"}
INCOMPLETE_TRANSFORMS = {"incomplete_missing_track"}


def _norm_text(value: object) -> str:
    raw = str(value or "").strip().lower()
    raw = re.sub(r"\[(?:hd|flac|cd|disc|disk)[^\]]*\]", " ", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\((?:hd|flac|cd|disc|disk)[^)]*\)", " ", raw, flags=re.IGNORECASE)
    raw = re.sub(r"[^a-z0-9]+", " ", raw)
    return re.sub(r"\s+", " ", raw).strip()


def _display_values(payload: object, key: str) -> list[str]:
    if not isinstance(payload, dict):
        return []
    raw = payload.get(key)
    if isinstance(raw, list):
        return [str(v or "").strip() for v in raw if str(v or "").strip()]
    if isinstance(raw, str) and raw.strip():
        return [raw.strip()]
    return []


def _token_overlap_ratio(left: object, right: object) -> float:
    left_tokens = {tok for tok in _norm_text(left).split() if len(tok) >= 3}
    right_tokens = {tok for tok in _norm_text(right).split() if len(tok) >= 3}
    if not left_tokens or not right_tokens:
        return 0.0
    shared = left_tokens & right_tokens
    if not shared:
        return 0.0
    return len(shared) / float(min(len(left_tokens), len(right_tokens)))


def _load_expected_groups(manifest_cases: list[dict]) -> list[dict]:
    groups: dict[str, dict] = {}
    for case in manifest_cases:
        if not isinstance(case, dict):
            continue
        source_key = str(case.get("source_key") or case.get("case_id") or "").strip()
        if not source_key:
            continue
        transform = str(case.get("transform") or "").strip().lower()
        summary = case.get("summary") if isinstance(case.get("summary"), dict) else {}
        placeholder_album = _norm_text(summary.get("album")) in {"", _norm_text(source_key)}
        placeholder_artist = _norm_text(summary.get("artist")) in {"", "template source", "templatesource"}
        source_hint = source_key.replace("_", " ").strip()
        hint_title = source_hint
        hint_artist = ""
        if source_hint:
            if source_hint.startswith("debussy "):
                hint_artist = "Claude Debussy"
            elif source_hint.startswith("tchaikovsky "):
                hint_artist = "Peter Tchaikovsky"
            elif source_hint.startswith("amadeus quartet "):
                hint_artist = "Amadeus Quartet"
        if source_key == "tchaikovsky_iolanta_live_opera":
            hint_title = "Tchaikovsky Iolanta Live"
            hint_artist = "Peter Ilyich Tchaikovsky"
        elif source_key == "debussy_complete_piano_works_recital":
            hint_title = "Complete Piano Works"
            hint_artist = "Claude Debussy"
        elif source_key == "amadeus_quartet_modernism_multicomposer_box":
            hint_title = "Bartok Tippett Britten RIAS Amadeus Quartet Modernism"
            hint_artist = "Amadeus Quartet"
        group = groups.setdefault(
            source_key,
            {
                "source_key": source_key,
                "cases": [],
                "published_expected": True,
                "expected_title": hint_title if placeholder_album else str(summary.get("album") or "").strip(),
                "expected_artist": hint_artist if placeholder_artist else str(summary.get("albumartist") or summary.get("artist") or "").strip(),
                "expected_track_count": int(summary.get("track_count") or 0),
                "expected_composer": str(summary.get("composer") or "").strip(),
                "expected_conductor": str(summary.get("conductor") or "").strip(),
                "expected_orchestra": str(summary.get("orchestra") or "").strip(),
            },
        )
        group["cases"].append(case)
        if transform in INCOMPLETE_TRANSFORMS:
            continue
        if (not group["expected_title"] or placeholder_album) and summary:
            fallback_title = str(summary.get("album") or "").strip()
            if fallback_title and _norm_text(fallback_title) != _norm_text(source_key):
                group["expected_title"] = fallback_title
        if (not group["expected_artist"] or placeholder_artist) and summary:
            fallback_artist = str(summary.get("albumartist") or summary.get("artist") or "").strip()
            if fallback_artist and _norm_text(fallback_artist) not in {"", "template source", "templatesource"}:
                group["expected_artist"] = fallback_artist
        if not group["expected_title"] and source_hint:
            group["expected_title"] = hint_title
        if not group["expected_artist"] and hint_artist:
            group["expected_artist"] = hint_artist
        if not group["expected_composer"] and hint_artist:
            group["expected_composer"] = hint_artist
        if not group["expected_title"] and summary:
            group["expected_title"] = str(summary.get("album") or "").strip()
        if not group["expected_composer"] and summary:
            group["expected_composer"] = str(summary.get("composer") or "").strip()
        if not group["expected_conductor"] and summary:
            group["expected_conductor"] = str(summary.get("conductor") or "").strip()
        if not group["expected_orchestra"] and summary:
            group["expected_orchestra"] = str(summary.get("orchestra") or "").strip()
        if not group["expected_track_count"] and summary:
            group["expected_track_count"] = int(summary.get("track_count") or 0)
    return list(groups.values())


@dataclass
class ValidationResult:
    started_at: str
    ended_at: str
    ok: bool
    failures: list[str]
    summary: dict


def run_ssh(script: str) -> str:
    proc = subprocess.run(
        ["ssh", HOST, script],
        capture_output=True,
        text=True,
        timeout=900,
    )
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or proc.stdout or "ssh command failed").strip())
    return proc.stdout.strip()


def load_manifest_json() -> dict:
    try:
        if MANIFEST_PATH.exists():
            return json.loads(MANIFEST_PATH.read_text())
    except Exception:
        pass
    raw = run_ssh(f"cat {MANIFEST_PATH}")
    try:
        return json.loads(raw)
    except Exception as exc:
        raise RuntimeError(f"failed to load classical benchmark manifest: {exc}") from exc


def build_expectations(manifest: dict) -> dict:
    manifest_cases = [case for case in list((manifest or {}).get("cases") or []) if isinstance(case, dict)]
    expected_groups = _load_expected_groups(manifest_cases)
    return {
        "scanned": int(len(manifest_cases) or 11),
        "published": len(expected_groups),
        "dedupe_moves": sum(
            1
            for case in manifest_cases
            if str((case or {}).get("transform") or "").strip().lower() in DEDUPE_TRANSFORMS
        ),
        "incomplete_moves": sum(
            1
            for case in manifest_cases
            if str((case or {}).get("transform") or "").strip().lower() in INCOMPLETE_TRANSFORMS
        ),
        "groups": expected_groups,
    }


def create_admin_token() -> str:
    script = r"""docker exec -i PMDA python -u - <<'PY'
import pmda
row = pmda._auth_get_user_by_username("admin")
if not row:
    raise SystemExit("admin user not found")
token, _ = pmda._auth_create_session(int(row["id"]), ip="127.0.0.1", user_agent="classical-validation")
print(f"PMDA_TOKEN:{token}")
PY"""
    out = run_ssh(script)
    token = ""
    for line in reversed([ln.strip() for ln in out.splitlines() if ln.strip()]):
        if line.startswith("PMDA_TOKEN:"):
            token = line.split(":", 1)[1].strip()
            break
    if not token:
        raise RuntimeError("failed to create admin token")
    return token


def api_json(
    path: str,
    *,
    token: str,
    method: str = "GET",
    payload: dict | None = None,
    timeout_sec: int = 120,
) -> tuple[int, dict]:
    data = None
    headers = {"Authorization": f"Bearer {token}"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = request.Request(f"{PMDA_BASE}{path}", data=data, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=timeout_sec) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, json.loads(body)
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(body)
        except Exception:
            parsed = {"raw": body}
        return exc.code, parsed


def restore_benchmark_source() -> None:
    script = rf"""python3 - <<'PY'
from pathlib import Path
import shutil
base = Path("{BENCH_ROOT}")
active = Path("{ACTIVE_SCAN_ROOT}")
template = base / "template_source"
run = active / "run_source"
run.mkdir(parents=True, exist_ok=True)
for child in list(run.iterdir()):
    if child.is_dir():
        shutil.rmtree(child)
    else:
        child.unlink()
for child in sorted(template.iterdir()):
    target = run / child.name
    if child.is_dir():
        shutil.copytree(child, target)
    else:
        shutil.copy2(child, target)
for cleanup in [
    Path("/mnt/user/MURRAY/Music/Music_dupes/C"),
    Path("/mnt/user/MURRAY/Music/Music_dupes/P"),
    Path("/mnt/user/MURRAY/Music/Music_dupes/incomplete_albums/C"),
    Path("/mnt/user/MURRAY/Music/Music_dupes/incomplete_albums/P"),
]:
    if cleanup.exists():
        for item in sorted(cleanup.rglob("*"), reverse=True):
            try:
                if item.is_symlink() or item.is_file():
                    item.unlink(missing_ok=True)
                elif item.is_dir():
                    item.rmdir()
            except Exception:
                pass
        try:
            cleanup.rmdir()
        except Exception:
            pass
print("ok")
PY"""
    run_ssh(script)


def reset_pmda(token: str) -> dict:
    status, payload = api_json(
        "/api/admin/maintenance/reset",
        token=token,
        method="POST",
        payload={
            "actions": ["media_cache", "state_db", "cache_db", "files_index"],
            "restart": False,
        },
        timeout_sec=300,
    )
    if status != 200 or payload.get("status") not in {"ok", "partial"}:
        raise RuntimeError(f"maintenance reset failed: {status} {payload}")
    return payload


def start_full_scan(token: str) -> dict:
    status, payload = api_json(
        "/scan/start",
        token=token,
        method="POST",
        payload={"scan_type": "full"},
    )
    if status != 200 or payload.get("status") != "started":
        raise RuntimeError(f"scan start failed: {status} {payload}")
    return payload


def wait_scan_end(token: str, timeout_sec: int = 2400) -> dict:
    deadline = time.time() + timeout_sec
    saw_running = False
    startup_deadline = time.time() + 120
    last = {}
    while time.time() < deadline:
        status, payload = api_json("/api/scan/progress", token=token)
        if status != 200:
            raise RuntimeError(f"/api/scan/progress failed: {status} {payload}")
        last = payload
        phase = str(payload.get("phase") or "").strip().lower()
        running = bool(
            payload.get("scanning")
            or payload.get("scan_starting")
            or payload.get("status") in {"starting", "running", "scanning"}
            or (phase and phase not in {"ready", "idle", "stopped", "completed"})
            or str(payload.get("current_step") or "").strip()
        )
        if running:
            saw_running = True
        if saw_running and not running:
            return payload
        if not saw_running and time.time() > startup_deadline:
            raise TimeoutError(f"scan never entered running state; last={last}")
        time.sleep(8)
    raise TimeoutError(f"scan did not complete within {timeout_sec}s; last={last}")


def fetch_published_state(token: str) -> dict:
    status_albums, albums_payload = api_json("/api/library/albums?limit=100&include_unmatched=1", token=token)
    if status_albums != 200:
        raise RuntimeError(f"library albums fetch failed: {status_albums} {albums_payload}")
    status_artists, artists_payload = api_json("/api/library/artists?limit=100&include_unmatched=1", token=token)
    if status_artists != 200:
        raise RuntimeError(f"library artists fetch failed: {status_artists} {artists_payload}")
    albums = []
    for item in list((albums_payload or {}).get("albums") or []):
        album_id = int(item.get("album_id") or 0)
        detail_payload = {}
        if album_id > 0:
            detail_status, detail_data = api_json(f"/api/library/album/{album_id}", token=token)
            if detail_status == 200 and isinstance(detail_data, dict):
                detail_payload = detail_data
        albums.append(
            {
                "id": album_id,
                "artist": str(item.get("artist_name") or "").strip(),
                "title": str(item.get("title") or "").strip(),
                "year": item.get("year"),
                "tracks": int(item.get("track_count") or 0),
                "thumb": item.get("thumb"),
                "metadata_source": item.get("profile_source"),
                "mb_identified": bool(item.get("mb_identified")),
                "public_rating": item.get("public_rating"),
                "classical": detail_payload.get("classical") if isinstance(detail_payload, dict) else None,
                "label": detail_payload.get("label") if isinstance(detail_payload, dict) else None,
            }
        )
    return {
        "artist_count": int((artists_payload or {}).get("total") or 0),
        "albums": albums,
        "albums_total": int((albums_payload or {}).get("total") or len(albums)),
    }


def fetch_scan_history(token: str, limit: int = 5) -> list[dict]:
    status, payload = api_json(f"/api/scan-history?limit={int(limit)}", token=token)
    if status != 200:
        raise RuntimeError(f"scan history fetch failed: {status} {payload}")
    if isinstance(payload, list):
        return payload
    return list(payload or [])


def fetch_moves(token: str, scan_id: int) -> list[dict]:
    status, payload = api_json(f"/api/scan-history/{scan_id}/moves?status=all", token=token)
    if status != 200:
        raise RuntimeError(f"moves fetch failed: {status} {payload}")
    return payload


def fetch_log_health(since_iso: str) -> dict:
    py = (
        "import json,sys; "
        "lines=sys.stdin.read().splitlines(); "
        "bad_patterns={"
        "'missing_album_folder':'Files publication skipped missing album folder',"
        "'publication_incomplete':'Files publication rebuild incomplete',"
        "'missing_release_group':'improve-folder: failed to fetch release group'"
        "}; "
        "counts={k:sum(1 for line in lines if v in line) for k,v in bad_patterns.items()}; "
        "summary={"
        "'counts':counts,"
        "'filtered_missing_targets':[line for line in lines if 'Scan profile/publication targets filtered missing folders' in line][-10:],"
        "'publication_rebuilds':[line for line in lines if 'Files publication rebuild for scan_id=' in line][-10:]"
        "}; "
        "print(json.dumps(summary, ensure_ascii=False))"
    )
    script = rf'docker logs PMDA --since "{since_iso}" 2>&1 | python3 -c {json.dumps(py)}'
    raw = run_ssh(script)
    return json.loads(raw.splitlines()[-1])


def validate(
    final_progress: dict,
    latest_scan: dict,
    published: dict,
    moves: list[dict],
    log_health: dict,
    expectations: dict,
) -> ValidationResult:
    failures: list[str] = []
    expected_published = int(expectations.get("published") or 0)
    expected_groups = list(expectations.get("groups") or [])
    expected_dedupe_moves = int(expectations.get("dedupe_moves") or 0)
    expected_incomplete_moves = int(expectations.get("incomplete_moves") or 0)
    expected_scanned = int(expectations.get("scanned") or 0)
    if int(published.get("albums_total") or 0) != expected_published:
        failures.append(f"expected {expected_published} published albums, got {published.get('albums_total')}")

    published_albums = list(published.get("albums") or [])

    def _album_matches_expected(item: dict, expected: dict) -> bool:
        title_norm = _norm_text(item.get("title"))
        artist_norm = _norm_text(item.get("artist"))
        expected_title_norm = _norm_text(expected.get("expected_title"))
        expected_artist_norm = _norm_text(expected.get("expected_artist"))
        if int(item.get("tracks") or 0) and int(expected.get("expected_track_count") or 0):
            if int(item.get("tracks") or 0) != int(expected.get("expected_track_count") or 0):
                return False
        if expected_title_norm and expected_title_norm not in title_norm and title_norm not in expected_title_norm:
            work_values = _display_values(item.get("classical"), "work")
            work_norms = {_norm_text(value) for value in work_values if _norm_text(value)}
            title_overlap = _token_overlap_ratio(expected_title_norm, title_norm)
            work_overlap = max((_token_overlap_ratio(expected_title_norm, value) for value in work_norms), default=0.0)
            if expected_title_norm not in work_norms and max(title_overlap, work_overlap) < 0.55:
                return False
        classical_payload = item.get("classical") if isinstance(item.get("classical"), dict) else {}
        composer_norms = {_norm_text(value) for value in _display_values(classical_payload, "composer")}
        conductor_norms = {_norm_text(value) for value in _display_values(classical_payload, "conductor")}
        orchestra_norms = {_norm_text(value) for value in _display_values(classical_payload, "orchestra")}
        expected_composer = _norm_text(expected.get("expected_composer"))
        expected_conductor = _norm_text(expected.get("expected_conductor"))
        expected_orchestra = _norm_text(expected.get("expected_orchestra"))
        if expected_composer and expected_composer not in composer_norms and expected_composer not in artist_norm:
            return False
        if expected_conductor and expected_conductor not in conductor_norms:
            return False
        if expected_orchestra and expected_orchestra not in orchestra_norms:
            return False
        artist_overlap = _token_overlap_ratio(expected_artist_norm, artist_norm)
        composer_overlap = max((_token_overlap_ratio(expected_artist_norm, value) for value in composer_norms), default=0.0)
        if expected_artist_norm and expected_artist_norm not in artist_norm and expected_artist_norm not in composer_norms:
            if max(artist_overlap, composer_overlap) < 0.55:
                return False
        return True

    matched_ids: set[int] = set()
    for expected in expected_groups:
        matches = [item for item in published_albums if _album_matches_expected(item, expected)]
        if len(matches) != 1:
            failures.append(
                f"expected exactly one published match for {expected.get('source_key')}, got {len(matches)}"
            )
            continue
        matched_ids.add(int(matches[0].get("id") or 0))
    extra_ids = [int(item.get("id") or 0) for item in published_albums if int(item.get("id") or 0) not in matched_ids]
    if extra_ids:
        failures.append(f"unexpected published albums remained after matching expected groups: {extra_ids}")

    def _move_kind(move: dict) -> str:
        return str(
            move.get("kind")
            or move.get("move_reason")
            or (move.get("details") or {}).get("kind")
            or ""
        ).strip()

    dedupe_moves = [m for m in moves if _move_kind(m) == "dedupe"]
    incomplete_moves = [m for m in moves if _move_kind(m) == "incomplete"]
    if len(dedupe_moves) != expected_dedupe_moves:
        failures.append(f"expected {expected_dedupe_moves} dedupe moves, got {len(dedupe_moves)}")
    if len(incomplete_moves) != expected_incomplete_moves:
        failures.append(f"expected {expected_incomplete_moves} incomplete moves, got {len(incomplete_moves)}")

    albums_scanned = (
        (final_progress or {}).get("albums_scanned")
        or ((final_progress or {}).get("last_scan_summary") or {}).get("albums_scanned")
        or (latest_scan or {}).get("albums_scanned")
        or ((latest_scan or {}).get("summary_json") or {}).get("albums_scanned")
        or (final_progress or {}).get("total_albums")
    )
    if int(albums_scanned or 0) != expected_scanned:
        failures.append(f"expected {expected_scanned} albums scanned, got {albums_scanned}")

    counts = (log_health or {}).get("counts") or {}
    for key in ("missing_album_folder", "publication_incomplete", "missing_release_group"):
        if int(counts.get(key) or 0) != 0:
            failures.append(f"log health failure: {key}={counts.get(key)}")

    now_iso = datetime.now(UTC).isoformat()
    return ValidationResult(
        started_at="",
        ended_at=now_iso,
        ok=not failures,
        failures=failures,
        summary={
            "progress": final_progress,
            "latest_scan": latest_scan,
            "published": published,
            "moves": moves,
            "log_health": log_health,
            "expectations": expectations,
        },
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate the PMDA classical benchmark corpus on Unraid.")
    parser.add_argument("--report-dir", default=str(REPORT_ROOT), help="Where to write JSON validation reports.")
    args = parser.parse_args()

    report_dir = Path(args.report_dir).expanduser().resolve()
    report_dir.mkdir(parents=True, exist_ok=True)

    manifest = load_manifest_json()
    expectations = build_expectations(manifest)
    token = create_admin_token()
    started_at = datetime.now(UTC)
    restore_benchmark_source()
    reset_pmda(token)
    start_full_scan(token)
    final_progress = wait_scan_end(token)
    published = fetch_published_state(token)
    history = fetch_scan_history(token, limit=3)
    latest_scan = dict(history[0]) if history else {}
    latest_scan_id = int(
        final_progress.get("latest_scan_id")
        or final_progress.get("scan_id")
        or latest_scan.get("scan_id")
        or 0
    )
    moves = fetch_moves(token, latest_scan_id) if latest_scan_id > 0 else []
    log_health = fetch_log_health(started_at.isoformat())
    result = validate(final_progress, latest_scan, published, moves, log_health, expectations)
    result.started_at = started_at.isoformat()

    stamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    report_path = report_dir / f"classical-validation-{stamp}.json"
    report_path.write_text(
        json.dumps(
            {
                "started_at": result.started_at,
                "ended_at": result.ended_at,
                "ok": result.ok,
                "failures": result.failures,
                "summary": result.summary,
            },
            ensure_ascii=False,
            indent=2,
        ) + "\n",
        encoding="utf-8",
    )
    print(report_path)
    if result.failures:
        for failure in result.failures:
            print(f"FAIL: {failure}", file=sys.stderr)
        return 1
    print("Classical benchmark validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
