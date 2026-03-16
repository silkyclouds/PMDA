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

EXPECTED_PUBLISHED = 6
EXPECTED_ARTISTS = 3
EXPECTED_DEDUPE_MOVES = 4
EXPECTED_INCOMPLETE_MOVES = 1
EXPECTED_TITLES = {
    'Britten; Debussy: La Mer',
    'Debussy: La Mer / Images [HD] (Flac)',
    'Debussy: La Mer / Nocturnes',
    'Tchaikovsky: Symphonie n° 6 "Pathétique"',
    'Tchaikovsky: Symphony no. 6 "Pathétique"',
    'The Tchaikovsky Project: Complete Symphonies and Piano Concertos',
}


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


def create_admin_token() -> str:
    script = r"""docker exec -i PMDA python -u - <<'PY'
import pmda
row = pmda._auth_get_user_by_username("admin")
if not row:
    raise SystemExit("admin user not found")
token, _ = pmda._auth_create_session(int(row["id"]), ip="127.0.0.1", user_agent="classical-validation")
print(token)
PY"""
    out = run_ssh(script)
    token = ""
    for line in reversed([ln.strip() for ln in out.splitlines() if ln.strip()]):
        if re.fullmatch(r"[A-Za-z0-9_-]{40,}", line):
            token = line
            break
    if not token:
        raise RuntimeError("failed to create admin token")
    return token


def api_json(path: str, *, token: str, method: str = "GET", payload: dict | None = None) -> tuple[int, dict]:
    data = None
    headers = {"Authorization": f"Bearer {token}"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = request.Request(f"{PMDA_BASE}{path}", data=data, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=90) as resp:
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
        albums.append(
            {
                "id": int(item.get("album_id") or 0),
                "artist": str(item.get("artist_name") or "").strip(),
                "title": str(item.get("title") or "").strip(),
                "year": item.get("year"),
                "tracks": int(item.get("track_count") or 0),
                "thumb": item.get("thumb"),
                "metadata_source": item.get("profile_source"),
                "mb_identified": bool(item.get("mb_identified")),
                "public_rating": item.get("public_rating"),
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
) -> ValidationResult:
    failures: list[str] = []
    published_titles = {str(item.get("title") or "").strip() for item in published.get("albums", [])}
    if int(published.get("albums_total") or 0) != EXPECTED_PUBLISHED:
        failures.append(f"expected {EXPECTED_PUBLISHED} published albums, got {published.get('albums_total')}")
    if int(published.get("artist_count") or 0) != EXPECTED_ARTISTS:
        failures.append(f"expected {EXPECTED_ARTISTS} published artists, got {published.get('artist_count')}")
    if published_titles != EXPECTED_TITLES:
        failures.append(f"published titles mismatch: {sorted(published_titles)}")

    def _move_kind(move: dict) -> str:
        return str(
            move.get("kind")
            or move.get("move_reason")
            or (move.get("details") or {}).get("kind")
            or ""
        ).strip()

    dedupe_moves = [m for m in moves if _move_kind(m) == "dedupe"]
    incomplete_moves = [m for m in moves if _move_kind(m) == "incomplete"]
    if len(dedupe_moves) != EXPECTED_DEDUPE_MOVES:
        failures.append(f"expected {EXPECTED_DEDUPE_MOVES} dedupe moves, got {len(dedupe_moves)}")
    if len(incomplete_moves) != EXPECTED_INCOMPLETE_MOVES:
        failures.append(f"expected {EXPECTED_INCOMPLETE_MOVES} incomplete moves, got {len(incomplete_moves)}")

    albums_scanned = (
        (final_progress or {}).get("albums_scanned")
        or ((final_progress or {}).get("last_scan_summary") or {}).get("albums_scanned")
        or (latest_scan or {}).get("albums_scanned")
        or ((latest_scan or {}).get("summary_json") or {}).get("albums_scanned")
        or (final_progress or {}).get("total_albums")
    )
    if int(albums_scanned or 0) != 11:
        failures.append(f"expected 11 albums scanned, got {albums_scanned}")

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
        },
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate the PMDA classical benchmark corpus on Unraid.")
    parser.add_argument("--report-dir", default=str(REPORT_ROOT), help="Where to write JSON validation reports.")
    args = parser.parse_args()

    report_dir = Path(args.report_dir).expanduser().resolve()
    report_dir.mkdir(parents=True, exist_ok=True)

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
    result = validate(final_progress, latest_scan, published, moves, log_health)
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
