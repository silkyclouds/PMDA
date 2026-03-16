#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib import error, request

HOST = "root@192.168.3.2"
PMDA_BASE = "http://192.168.3.2:5005"
REPO_ROOT = Path(__file__).resolve().parents[1]
REPORT_ROOT = REPO_ROOT / ".tmp" / "ocr-delta"
ACTIVE_BENCH_ROOT = Path("/mnt/user/MURRAY/Music/pmda_scan_benchmark")
NONCLASSICAL_BENCH_ROOT = Path("/mnt/user/MURRAY/Music/pmda_scan_benchmark_backup_nonclassical_20260316_070352")
CLASSICAL_BENCH_ROOT = Path("/mnt/user/MURRAY/Music/pmda_scan_benchmark_classical")
NONCLASSICAL_REPORT_ROOT = REPO_ROOT / ".tmp" / "overnight-validation"
CLASSICAL_REPORT_ROOT = REPO_ROOT / ".tmp" / "classical-validation"


@dataclass
class RunSummary:
    benchmark: str
    mode: str
    ok: bool
    report_path: str
    ai_used_count: int
    ai_tokens_total: int
    duration_seconds: float
    albums_published: int
    artists_published: int


def run_ssh(script: str) -> str:
    proc = subprocess.run(["ssh", HOST, script], capture_output=True, text=True, timeout=1800)
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or proc.stdout or "ssh failed").strip())
    return proc.stdout.strip()


def create_admin_token() -> str:
    script = r'''docker exec -i PMDA python -u - <<"PY"
import pmda
row = pmda._auth_get_user_by_username("admin")
if not row:
    raise SystemExit("admin user not found")
token, _ = pmda._auth_create_session(int(row["id"]), ip="127.0.0.1", user_agent="ocr-delta")
print(f"PMDA_TOKEN:{token}")
PY'''
    out = run_ssh(script)
    token = ""
    for line in reversed([ln.strip() for ln in out.splitlines() if ln.strip()]):
        if line.startswith("PMDA_TOKEN:"):
            token = line.split(":", 1)[1].strip()
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
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(body)
        except Exception:
            parsed = {"raw": body}
        return exc.code, parsed


def put_config(token: str, payload: dict) -> None:
    status, body = api_json("/api/config", token=token, method="PUT", payload=payload)
    if status != 200:
        raise RuntimeError(f"config update failed: {status} {body}")


def sync_active_benchmark(src_root: Path) -> None:
    script = rf'''python3 - <<"PY"
from pathlib import Path
import shutil
src = Path({json.dumps(str(src_root))})
dst = Path({json.dumps(str(ACTIVE_BENCH_ROOT))})
dst.mkdir(parents=True, exist_ok=True)
for name in ("template_source", "run_source"):
    src_path = src / name
    dst_path = dst / name
    if dst_path.exists():
        if dst_path.is_dir():
            shutil.rmtree(dst_path)
        else:
            dst_path.unlink()
    if src_path.exists():
        if src_path.is_dir():
            shutil.copytree(src_path, dst_path)
        else:
            shutil.copy2(src_path, dst_path)
manifest_src = src / "manifest.json"
manifest_dst = dst / "manifest.json"
if manifest_dst.exists():
    manifest_dst.unlink()
if manifest_src.exists():
    shutil.copy2(manifest_src, manifest_dst)
print("ok")
PY'''
    run_ssh(script)


def newest_report(report_root: Path, since_ts: float) -> Path:
    files = [p for p in report_root.glob("*.json") if p.stat().st_mtime >= since_ts - 2]
    if not files:
        raise RuntimeError(f"no report created in {report_root}")
    return max(files, key=lambda p: p.stat().st_mtime)


def load_summary(report_path: Path, benchmark: str, mode: str) -> RunSummary:
    data = json.loads(report_path.read_text())
    summary = data.get("summary") or {}
    progress = summary.get("progress") or {}
    published = summary.get("published") or {}
    started_at = data.get("started_at") or summary.get("started_at")
    ended_at = data.get("ended_at") or summary.get("ended_at")
    duration_seconds = 0.0
    if started_at and ended_at:
        try:
            start_dt = datetime.fromisoformat(str(started_at).replace("Z", "+00:00"))
            end_dt = datetime.fromisoformat(str(ended_at).replace("Z", "+00:00"))
            duration_seconds = max(0.0, (end_dt - start_dt).total_seconds())
        except Exception:
            duration_seconds = 0.0
    return RunSummary(
        benchmark=benchmark,
        mode=mode,
        ok=bool(data.get("ok")),
        report_path=str(report_path),
        ai_used_count=int(progress.get("ai_used_count") or 0),
        ai_tokens_total=int(progress.get("ai_tokens_total") or 0),
        duration_seconds=float(duration_seconds),
        albums_published=int(published.get("albums_total") or 0),
        artists_published=int(published.get("artist_count") or 0),
    )


def run_validation(command: list[str], report_root: Path, benchmark: str, mode: str) -> RunSummary:
    since = time.time()
    proc = subprocess.run(command, cwd=REPO_ROOT, text=True)
    if proc.returncode != 0:
        # still try to load the report before failing hard
        report = newest_report(report_root, since)
        result = load_summary(report, benchmark, mode)
        result.ok = False
        return result
    report = newest_report(report_root, since)
    return load_summary(report, benchmark, mode)


def summarize_delta(a: RunSummary, b: RunSummary) -> dict:
    def delta(before: int | float, after: int | float) -> dict:
        change = after - before
        pct = 0.0 if before in {0, 0.0} else (change / before) * 100.0
        return {"before": before, "after": after, "change": change, "pct": round(pct, 2)}
    return {
        "ai_used_count": delta(a.ai_used_count, b.ai_used_count),
        "ai_tokens_total": delta(a.ai_tokens_total, b.ai_tokens_total),
        "duration_seconds": delta(round(a.duration_seconds, 2), round(b.duration_seconds, 2)),
    }


def main() -> int:
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    token = create_admin_token()
    results: dict[str, dict[str, dict]] = {"non_classical": {}, "classical": {}}
    for mode in ("off", "smart"):
        put_config(token, {"MATCH_COVER_OCR_MODE": mode})
        sync_active_benchmark(NONCLASSICAL_BENCH_ROOT)
        non_classical = run_validation(
            [sys.executable, "scripts/overnight_scan_validation.py", "--iterations", "1"],
            NONCLASSICAL_REPORT_ROOT,
            "non_classical",
            mode,
        )
        classical = run_validation(
            [sys.executable, "scripts/validate_classical_benchmark.py"],
            CLASSICAL_REPORT_ROOT,
            "classical",
            mode,
        )
        results["non_classical"][mode] = non_classical.__dict__
        results["classical"][mode] = classical.__dict__
    put_config(token, {"MATCH_COVER_OCR_MODE": "smart"})
    report = {
        "generated_at": datetime.now().isoformat(),
        "results": results,
        "deltas": {
            "non_classical": summarize_delta(
                RunSummary(**results["non_classical"]["off"]),
                RunSummary(**results["non_classical"]["smart"]),
            ),
            "classical": summarize_delta(
                RunSummary(**results["classical"]["off"]),
                RunSummary(**results["classical"]["smart"]),
            ),
        },
    }
    out = REPORT_ROOT / f"ocr-delta-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False))
    print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
