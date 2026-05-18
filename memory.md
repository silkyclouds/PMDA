# PMDA Working Memory

## Operator + Runtime Notes

- The Unraid deployment on `srv-murray` uses `network=host`.
- The live PMDA container is `PMDA`.
- Autonomous refactor safety baseline:
  - local backup: `.tmp/backups/autonomy-20260515-070058`
  - Unraid backup: `/mnt/cache/appdata/PMDA/backups/autonomy-20260515-070058`
  - phase plan: `docs/AUTONOMOUS_REFACTOR_PLAN.md`
  - phase guard: `scripts/autonomy_guard.py`
  - never recreate/restart the live Unraid container while a long rebuild/scan is active unless the user explicitly asks for it.
- Disk-aware power saver mode depends on a read-only bind mount:
  - host `/mnt`
  - container `/host_mnt`
- Keep existing canonical library paths untouched:
  - source paths stay `/music/...`
  - disk-aware access paths under `/host_mnt/diskN/...` are read-only implementation details.

## Disk-Aware Scan Invariants

- `STORAGE_POWER_SAVER_ENABLED` stays opt-in.
- Discovery may need a full-picture pass across all disks, but it must walk them one at a time.
- During disk-aware discovery, PMDA should expose the current source root and current disk in runtime state.
- `storage_current_device_id` / `storage_current_device_label` must be updated during discovery and bucket processing.
- Background enrichment must not fan out across the whole array while a disk-aware scan is active.
- Scan-time post-processing/backfill workers must keep disk-aware materialization deferral state valid across the whole scan.
  - A worker crash here can leave the main scan running but remove live enrichment/backfill and make finalization look frozen.
- While disk-aware power saver is active, matched-library filesystem materialization must not run mid-scan.
  - Publishing rows to PostgreSQL/live UI is fine.
  - Moving/copying/hardlinking winners into `/music/Music_matched` during the scan wakes destination disks across the array.
  - Defer that filesystem export until the scan has settled or a dedicated disk-scoped export pass exists.

## Backfill Rules

- While `scan_discovery_running=true` and disk-aware mode is enabled:
  - no files profile backfill should start
  - delayed start is correct behavior
- Once a real active scan disk exists, files profile backfill may run only inside the allowed storage scope.
- If storage scope changes, the current backfill pass should stop and only restart inside the new allowed scope.
- Idle/global backfill outside scans still needs the same disk-discipline treatment later if we want full-array power savings even when no scan is running.
- While disk-aware power saver is enabled, idle autobackfill should stay off unless it also becomes disk-scoped.
- Opportunistic artist/profile refresh jobs must honor the same storage scope rules as backfill.

## UI / Observability

- The Scan page is the only place that should show detailed storage/runtime power-saver state.
- Home / Artists / Albums should not get intrusive scan-panels.
- Normal library browsing must stay fast during scans:
  - default browse mode is `auto`
  - while a scan is running, `auto` should read the published snapshot for ordinary library pages instead of probing the live rebuilding index
  - only explicitly live scan-page widgets should request `browse_source=live`
  - keep cache keys/source separation between `live` and `published` browse payloads so scan widgets never reuse stale published browse caches
  - infinite-scroll append/load-more failures are not fatal page-load failures
  - keep destructive timeout/error banners for initial/reset fetch failures only
  - clear stale browse errors on the next successful page fetch so a transient load-more timeout does not linger after content is already rendering
- Album pages should support a private personal review/note in addition to star ratings.
  - This note is user-owned metadata, separate from public/provider album reviews.
  - Clearing stars must not delete a saved personal review.
  - Clearing the review must not delete stars.
- The most important runtime indicators during discovery are:
  - current stage
  - current root / current disk
  - folders or albums done / total
- `album_candidates` must be surfaced as its own explicit stage, not mislabeled as “restoring resume plan”.
- During pre-scan, concrete discovery state must override stale resume wording:
  - `filesystem` => `discovering_filesystem`
  - `album_candidates` => `building_album_candidates`
  - only use `restoring_resume_plan` when no concrete discovery stage is active yet.
- The scan-page `LibraryLiveNowPanel` must stay hidden until PMDA is actually publishing or enriching visible library rows.
  - Do not poll `/api/library/artists|albums?refresh=1` during `pre_scan`, scope prep, format analysis, or identification-only phases.
  - Before publication starts, that panel only creates timeout noise and fake “library rebuild” signals.
- During `album_candidates`, PMDA must keep `scan_resume_runs.discovery_state_json` fresh with folder/artist/album counts and current folder progress, without rewriting the full `scan_resume_discovery_files` snapshot on every heartbeat.
- The scan UI must treat an active resume run as live/running whenever a `scan_resume_run_id` exists together with discovery/run-scope evidence, even if the raw `scanning` boolean arrives late after a restart.
  - Never show the idle/completed workspace while `scan_resume_runs.status='running'` is effectively represented by live discovery state.
- During `album_candidates`, PMDA must also persist partial rows into `scan_resume_files_plan` before the full plan is finalized.
  - `discovery_state_json.folders_done` must never advance far ahead of persisted `scan_resume_files_plan` rows.
  - On restart, PMDA must resume from the persisted partial-plan boundary, not from folder `0`.
  - A correct restart proof is: `scan_resume_files_plan` row count remains non-zero and the resumed `folders_done` / `albums_found` continue from that boundary instead of collapsing back to zero.
- When `STORAGE_POWER_SAVER_ENABLED=true`, `album_candidates` itself must obey strict disk bucketing.
  - Discovery must not bounce between `diskN` roots in the same window while claiming `active disks 1/N`.
  - The storage scheduler currently kicks in later during artist/worker processing; discovery needs the same constraint.
  - If discovery hops between disks, it hurts both HDD sleep behavior and throughput.
- While disk-aware discovery is constrained to one active disk, `album_candidates` must still use the normal scan worker budget inside that disk.
  - Do not collapse the hot path to a single effective worker just because only one disk is active.
  - The correct model is: one disk bucket at a time, many folder-prep workers inside that bucket, ordered commits for deterministic resume.
- Do not call `Path.resolve()` for every `/music/...`, `/host_mnt/...`, or `/dupes/...` album folder during Files scan planning.
  - Those paths are already canonical enough for cache keys.
  - Bulk `resolve()` calls in `album_candidates` can wake disks and stall bucket preparation for minutes.
- Storage access-path lookups must use fast cached prefix/device maps during Files discovery and resume.
  - Repeated linear scans over all storage roots for every audio file turn `album_candidates` resume into minutes of pure Python churn.
  - Fast-path lookup by `diskN` and normalized root prefix is required for acceptable restart time.
- `album_candidates` bucket preparation must log its own milestones.
  - At minimum: grouped audio files, restored partial candidates, candidate-planning-ready, first pending folder/device, bucket start/end.
  - Without these logs it is too easy to misdiagnose “stuck” discovery versus slow preparation.
- `_update_scan_storage_bucket_row()` is a side-effect helper and must never carry stray result-aggregation code or references to unrelated locals.
  - A bug there can crash the entire scan exactly when the first storage bucket starts, which defeats resume and makes disk-aware validation impossible.
- Avoid null-access crashes in scan progress components; missing `activeFocus` must be safe.
- If a files scan resume row is still `running` after a container restart, PMDA must auto-resume it on startup instead of leaving a dead in-memory worker with a live DB snapshot.

## MCP / Security

- Never expose PMDA admin bearer tokens in user-facing output.
- MCP access is intended to stay non-destructive by default.
- If MCP is disabled in settings, external access must fail cleanly.

## Things To Keep Fixing

- Ensure disk-aware discovery and disk-aware backfill never wake unrelated disks.
- Make discovery/runtime labels match the real stage instead of misleading “restoring resume plan” messaging.
- Keep DockerMan template aligned with the actual running Unraid container configuration after live redeploys.
- `state.db` must stay in WAL mode with a long busy timeout. Scan-time writes must go through PMDA's state DB write retry/serialization helpers so transient SQLite writer contention becomes wait/retry, not dropped scan progress.
- Files publication is scan truth, not filesystem truth only.
  - Never let `scan_history` refresh destructively rebuild/delete `files_library_published_albums`.
  - If `scan_editions` and `scan_moves` prove an album passed the pipeline and was exported to `Music_matched`, publication recovery must be able to remap the existing published row to the final destination without rescanning tags/tracks.
  - Old source publication rows from `Music_dump` must be hidden/deleted only after the destination row is safely published.
  - Publication reconcile should expose progress phases and prefer direct row remap before any slower cache/filesystem fallback.
- Files library index publication must not stay blocked behind optional enrichments.
  - Once PostgreSQL browse rows are written, the index should publish `done` and keep library pages usable.
  - Recommendation embeddings, media cache generation, and artist/profile backfill are background work.
  - Those background jobs must not reuse `files_index_lock` or make ordinary browse pages think the main library rebuild is still running.
- Scan finalization must always expose measurable work when a finalizing task has item progress.
  - Saving editions / pipeline trace / track reconciliation should show `item done / item total`, not “Waiting for measurable work”.
  - Finalization labels should include the current subtask label when available.
  - If no item counter exists, fall back to finalizing task `N / total`, never a blank/stuck stage.
- Scan post-processing queues must never use unbounded `Queue.join()` during finalization.
  - Optional enrichments may continue in background, but the operator-facing scan must settle after a bounded drain timeout.
  - A dead worker or massive optional backlog must produce a clear warning log, not a multi-day fake 80% freeze.
- Operator-facing job state must come from a structured backend snapshot, not inferred log parsing.
  - `GET /api/jobs/status` and MCP `pmda.jobs.status` should expose scan, publication, materialization, library_index, media_cache, profile_backfill, embeddings, metadata_workers, runtime_repair, and storage.
  - The Scan page can show this panel, but Home/Artists/Albums should not get intrusive job panels.
  - This snapshot must clearly mark post-publication jobs as non-blocking for browse and scan completion.
- Library browse endpoints must never wait behind long PostgreSQL rebuild/count work.
  - Request-path browse counts should use fast best-effort metadata/short-timeout reads.
  - Unfiltered album grids should reuse snapshot totals instead of running a fresh `COUNT(*)` on every page.
  - Add/maintain visible/recent indexes for both live PostgreSQL browse rows and SQLite published snapshot rows.
  - Published SQLite snapshot reads are HTTP read paths, not durable writer paths; they must use short busy timeouts and fall back quickly instead of inheriting long state DB writer waits.
- Lidarr is parked legacy unless `PMDA_LIDARR_ENABLED=true`.
  - Keep stored config for future use, but default UI/API must not make Lidarr look active.
  - Config should expose `LIDARR_FEATURE_ENABLED` so clients can hide or disable any legacy controls explicitly.
- Mirror/trusted-library startup must not force a full filesystem rebuild when a usable PostgreSQL browse index already exists.
  - A startup `existing_pg_index` source in mirror workflow is not by itself a reason to walk `/music/Music_matched`, `/music/incomming`, and `/music/Music_dump`.
  - Use the existing index at startup; require a manual filesystem rebuild or `PMDA_FORCE_MIRROR_TRUSTED_STARTUP_REBUILD=true` for root-level reconciliation.
  - `startup_published_rows_catchup`, `scan_completed`, and `export_library_complete` must prefer published rows / row remap paths, not an automatic full disk crawl.

## 2026-05-15 Autonomous Refactor Gate

- Safety backups were created before the autonomous pass:
  - local: `.tmp/backups/autonomy-20260515-070058`
  - Unraid: `/mnt/cache/appdata/PMDA/backups/autonomy-20260515-070058`
- `scripts/autonomy_guard.py --phase 10 --check` passed the final release gate:
  - Python compile passed.
  - Focused files/index/scan progress tests passed.
  - Provider/matching/web-search tests passed.
  - Storage power saver/backfill/index tests passed.
  - Static `scripts/pipeline_audit_gate.py` passed 20/20 pipeline invariants.
  - Frontend typecheck passed.
- Full regression suite passed after the gate:
  - `.venv-codex-tests/bin/python -m pytest -q`
  - `510 passed, 3 warnings, 7 subtests passed`
- Docker images were built and pushed after the green gate:
  - `meaning/pmda:beta`
  - `meaning/pmda:latest`
  - `meaning/pmda:nightly`
- The Dockerfile no longer bakes `PMDA_PG_PASSWORD` as an `ENV`.
  - Runtime defaults still exist in the entrypoint/Python fallback.
  - The final Docker build completed without the previous `SecretsUsedInArgOrEnv` warning.
- The live Unraid PMDA container was not restarted automatically after this build; redeploy remains an explicit operational step.

## 2026-05-15 Legacy Integration Cleanup Pass

- PMDA is files-only for source/library discovery. Plex DB/source-library access stays blocked in files mode.
- Plex remains supported only as a post-publication player refresh target, through `pmda_integrations.player_sync`.
- Lidarr/Autobrr acquisition workflows are removed from active code paths.
  - Old API routes remain as disabled compatibility endpoints returning HTTP 410.
  - No active Lidarr/Autobrr network calls are allowed by `scripts/legacy_cleanup_gate.py`.
- User-facing UI/docs must not advertise Lidarr/Autobrr as current PMDA features.
- Build releases now run both static gates:
  - `scripts/pipeline_audit_gate.py`
  - `scripts/legacy_cleanup_gate.py`
- Validation after this cleanup:
  - `python3 -m py_compile pmda.py pmda_integrations/player_sync.py scripts/pipeline_audit_gate.py scripts/legacy_cleanup_gate.py`
  - `python3 scripts/pipeline_audit_gate.py`
  - `python3 scripts/legacy_cleanup_gate.py`
  - `.venv-codex-tests/bin/python -m pytest -q` -> `514 passed, 3 warnings, 7 subtests passed`
  - `cd frontend && npm run build`

## 2026-05-15 Monolith Modularization Pass

- Added a guarded modularization runner: `scripts/autonomous_refactor_guard.py`.
  - It compiles the runtime modules, runs the pipeline/static cleanup gates, runs focused tests, records completed phases in `.tmp/autonomy/modularization_state.json`, and can run full backend/frontend verification with `--full`.
- Extracted pure or low-side-effect business modules from `pmda.py`:
  - `pmda_core.config`: config parsing and library mode/path normalization.
  - `pmda_core.state_db`: SQLite lock detection, retry policy, and WAL setup helpers.
  - `pmda_core.logging_utils`: plain log formatting, recent-log buffer, and quiet polling filter.
  - `pmda_core.legacy_integrations`: disabled Lidarr/Autobrr compatibility stubs.
  - `pmda_core.library_index`: library index progress/status helpers.
  - `pmda_core.scan_progress`: scan phase, ETA, percent, pre-scan labels, active-worker summaries.
  - `pmda_core.scan_orchestrator`: pipeline flag resolution, async decision, lifecycle state helpers.
  - `pmda_core.pipeline_jobs`: durable job heartbeat normalization and stale-status payloads.
  - `pmda_core.materialization_policy`: strict/strong/soft/AI/unresolved materialization confidence policy.
  - `pmda_core.provider_matching`: provider candidate confidence-tier read model.
  - `pmda_core.scan_moves`: audited move payload construction and schema-compatible insert ordering.
- Kept compatibility wrappers in `pmda.py` where existing tests/API/MCP expect historical function names.
- Updated `scripts/pipeline_audit_gate.py` so static release gates validate the new module boundaries instead of forcing all tokens to remain in `pmda.py`.
- Validation after this modularization pass:
  - `python3 scripts/autonomous_refactor_guard.py --phase scan_moves_core --full`
  - focused guard tests: `99 passed, 2 warnings, 7 subtests passed`
  - full backend suite: `563 passed, 3 warnings, 7 subtests passed`
  - frontend build: `npm run build` passed.

## 2026-05-16 Refactor Deployment Checkpoint

- The modularization guard currently reports all registered phases complete:
  - state/status file: `.tmp/autonomy/modularization_state.json`
  - last phase: `settings_config_blueprint`
  - status: `green`
  - resume instruction: all registered phases are complete.
- The deployed image checkpoint is:
  - image: `meaning/pmda:latest`
  - digest: `meaning/pmda@sha256:f8fdb3a9f5008a711514e03c7908a261a7cbe85b8455b85f01fd0c2cbb7933e3`
  - image id: `sha256:9fc21a5ac57c3e568da1e7ca23360eb571c779b17421e85154618ee38ccf1015`
- The live Unraid deployment backup created before recreating the container is:
  - `/mnt/cache/appdata/PMDA/backups/refactor-deploy-20260516-070637`
- The previous live container was preserved as:
  - `PMDA_pre_refactor_20260516-070637`
- The previous image observed before this deploy was:
  - `sha256:292cd53c2c475524de8cf65d8fc166e6cadd41fa83c2b3eb8b2cb3b1da285753`
- The recreated container is:
  - name: `PMDA`
  - id: `7e1503dcb61cab8893cca8a99df014422ca866151bada338acb20db82ff56334`
  - network: `host`
  - important preserved mounts:
    - `/mnt/cache/appdata/PMDA:/config`
    - `/mnt/cache/appdata/PMDA:/mnt/cache/appdata/PMDA`
    - `/mnt/user/MURRAY/Music:/music`
    - `/mnt/user/MURRAY/Music/Music_dupes:/dupes`
    - `/mnt:/host_mnt:ro`
    - `/var/run/docker.sock:/var/run/docker.sock`
- Smoke validation immediately after deploy passed:
  - `/api/scan/progress` returned HTTP 200 in about `0.129s` with status `stopped`.
  - `/api/config` returned HTTP 200 in about `0.033s`.
  - `/api/runtime/managed/status?skip_candidates=true` returned HTTP 200 in about `0.194s`.
  - `/api/library/albums` returned HTTP 200 in about `0.299s`, 12 albums, total around `61,096`, source `published`.
  - `/api/library/artists` returned HTTP 200 in about `0.380s`, 12 artists, total around `39,253`, source `published`.
  - `/api/library/files-index/status` returned HTTP 200 in about `0.006s`, indexed around `65,708` albums, `49,010` artists, `627,512` tracks, `running=false`.
  - `/api/admin/mcp/status` returned HTTP 200 in about `0.005s`, `enabled=true`.
  - Log grep found no actual `Traceback`, `Killed`, `statement timeout`, or runtime error after deploy.
- Rollback command sequence if this deploy is proven bad:
  - `docker stop PMDA`
  - `docker rm PMDA`
  - `docker rename PMDA_pre_refactor_20260516-070637 PMDA`
  - `docker start PMDA`
- If the preserved container has been removed, use the backup under `/mnt/cache/appdata/PMDA/backups/refactor-deploy-20260516-070637` to reconstruct the previous container inspect/log context before recreating from the previous image.
- Do not delete `PMDA_pre_refactor_20260516-070637` until this refactor checkpoint has survived scan, browse, rebuild, MCP, and player-sync smoke tests.
- The scan was not relaunched during this deployment checkpoint.
- Local validation rerun after recording this checkpoint:
  - command: `python3 scripts/autonomous_refactor_guard.py --phase settings_config_blueprint --full`
  - static gates passed: `pipeline_audit_gate.py`, `legacy_cleanup_gate.py`, `pmda_bootstrap_gate.py`
  - focused tests: `196 passed, 2 warnings, 7 subtests passed`
  - full backend suite: `644 passed, 3 warnings, 7 subtests passed`
  - frontend production build: `npm run build` passed.
- Authenticated Unraid smoke rerun after local validation:
  - A temporary admin session token was created for the smoke test and revoked immediately after use.
  - `/api/scan/progress`: HTTP 200 in about `0.114s`, payload status `stopped`, `scanning=false`.
  - `/api/config`: HTTP 200 in about `0.029s`.
  - `/api/runtime/managed/status?skip_candidates=true`: HTTP 200 in about `0.240s`.
  - `/api/library/albums`: HTTP 200 in about `0.274s`, 12 albums, total around `61,096`, source `published`.
  - `/api/library/artists`: HTTP 200 in about `0.373s`, 12 artists, total around `39,253`, source `published`.
  - `/api/library/files-index/status`: HTTP 200 in about `0.006s`, indexed around `65,708` albums, `49,010` artists, `627,512` tracks, `running=false`.
  - `/api/admin/mcp/status`: HTTP 200 in about `0.004s`, `enabled=true`.
  - `/api/jobs/status`: HTTP 200 in about `0.255s`, no running jobs.
  - Recent container logs showed no `Traceback`, `Killed`, `statement timeout`, `OperationalError`, `FATAL`, `panic`, or unhandled `Exception`.

## 2026-05-16 Album Grid Pagination Fix Checkpoint

- User-visible bug addressed:
  - Unraid Albums page could show only a couple cards with a persistent `Loading more...`.
  - Root causes were both frontend and backend:
    - frontend browse deduped by `artist/title/year`, collapsing legitimate rows
    - published snapshot payload could reuse legacy `source_id` values like `4` and `6` as `album_id`, collapsing pages even after frontend dedupe changed
- Code changes:
  - `frontend/src/lib/albumDisplayDedupe.ts` now dedupes paginated rows by a display identity (`album_id`, normalized artist, normalized title, year) instead of collapsing the browse grid by `artist/title/year` or by a reused legacy ID alone.
  - `frontend/src/pages/LibraryAlbums.tsx` now stops infinite scroll when a page adds no new IDs and displays explicit terminal states instead of spinning forever.
  - `frontend/src/pages/LibraryArtists.tsx` accepts backend `has_more` and has non-blocking load-more error handling.
  - `/api/library/albums` and `/api/library/artists` now include non-breaking `has_more`.
  - `pmda_core.pagination.page_has_more()` centralizes page continuation logic.
  - Published album browse resolves the real live album id by `folder_path` when allowed; it falls back safely to `source_id` if live resolution is unavailable.
- Local validation:
  - `npm run test -- albumDisplayDedupe` passed.
  - `npm run build` passed.
  - `python3 -m py_compile pmda.py pmda_core/pagination.py` passed.
  - `python3 scripts/pipeline_audit_gate.py` passed.
  - `python3 scripts/legacy_cleanup_gate.py` passed.
  - `python3 scripts/pmda_bootstrap_gate.py` passed.
  - `.venv-codex-tests/bin/python -m pytest -q` passed after the backend ID fix: `646 passed, 2 warnings, 7 subtests passed`.
- Published Docker image:
  - image: `meaning/pmda:latest`
  - digest: `meaning/pmda@sha256:54e9c8edef299df37ad64ebffa7a215b988ca563800b1eec502babd8214d54cf`
  - image id: `sha256:730c9b15d0fbcfcb63006322eb5f5f1636dbc8a8c8428b46458e798c16d1f84e`
  - also pushed as `meaning/pmda:beta`
- Live Unraid deployment:
  - active container: `PMDA`
  - active image id: `sha256:730c9b15d0fbcfcb63006322eb5f5f1636dbc8a8c8428b46458e798c16d1f84e`
  - command cleaned: `cmd=null`, entrypoint `["/app/scripts/entrypoint_allinone.sh"]`
  - no scan was relaunched.
- Rollback containers/backups created during this checkpoint:
  - first frontend-only deploy backup: `/mnt/cache/appdata/PMDA/backups/album-grid-fix-20260516-082157`
  - first preserved container: `PMDA_pre_albumgrid_20260516-082157`
  - backend ID-fix deploy backup: `/mnt/cache/appdata/PMDA/backups/album-grid-id-fix-20260516-083420`
  - backend ID-fix preserved container: `PMDA_pre_albumgrid_idfix_20260516-083420`
  - final clean-command backup: `/mnt/cache/appdata/PMDA/backups/album-grid-clean-cmd-20260516-083721`
  - final clean-command preserved container: `PMDA_pre_albumgrid_cleancmd_20260516-083721`
  - final display-identity deploy backup: `/mnt/cache/appdata/PMDA/backups/album-grid-final-20260516-084659`
  - final display-identity preserved container: `PMDA_pre_albumgrid_final_20260516-084659`
- Final authenticated Unraid smoke:
  - temporary admin session token was created and revoked inside the smoke script.
  - `/api/library/albums?sort=recent&limit=96&offset=0&include_unmatched=1&scope=library`: HTTP 200, about `1056ms`, `96` albums, `96` unique display identities, `92` raw unique album IDs, total `61,096`, `has_more=true`.
  - `/api/library/albums?sort=recent&limit=96&offset=96&include_unmatched=1&scope=library`: HTTP 200, about `386ms`, `96` albums, `96` unique display identities, `96` raw unique album IDs, total `61,096`, `has_more=true`.
  - `/api/library/artists?sort=alpha&limit=120&offset=0&include_unmatched=1&scope=library`: HTTP 200, about `419ms`, `120` artists, total `39,253`, `has_more=true`.
  - `/api/scan/progress`: HTTP 200, about `165ms`, status `stopped`.
  - `/api/jobs/status`: HTTP 200, about `183ms`, no running jobs, job keys include scan, publication, materialization, library index, media cache, profile backfill, embeddings, runtime repair, storage.
  - `/api/library/files-index/status`: HTTP 200, about `8ms`, `running=false`, indexed around `65,708` albums, `49,010` artists, `627,512` tracks.
  - Recent logs showed no `Traceback`, `Killed`, `statement timeout`, `OperationalError`, `FATAL`, `panic`, or unhandled `Exception`.
- Browser validation note:
  - Playwright navigation to `/PMDA/library/albums` redirected to `/login`; no browser session was available.
  - Visual smoke was therefore not completed through browser tools.
  - The authenticated API smoke validates the exact regression source: full 96-row pages and 96 display identities instead of the previous 2 visible albums.

## 2026-05-16 Internal Refactor Checkpoint: Jobs, Metadata Queue, Storage Progress

- Scope:
  - Continued extraction of side-effect-heavy status code out of `pmda.py` after the album-grid deployment.
  - No Docker image was built and no Unraid redeploy was performed for this checkpoint; this is a local refactor checkpoint only.
  - No scan was launched or resumed.
- Code extracted:
  - `pmda_core/job_status.py` now owns operator-facing job payload construction for UI/MCP.
  - `pmda.py` only snapshots live state under locks and calls `pmda_core.job_status.build_jobs_status_snapshot(...)`.
  - `pmda_enrichment/metadata_jobs.py` now owns metadata job enqueue and queue summary SQL access.
  - `pmda_discovery/storage_buckets.py` now owns storage/disk-aware scan progress payload construction.
- Regression guards added:
  - `scripts/pmda_bootstrap_gate.py` now requires `pmda_core/job_status.py`, `pmda_enrichment/metadata_jobs.py`, and the storage progress helper.
  - The gate blocks reintroducing job payload construction, direct `INSERT INTO metadata_jobs`, and storage progress payload assembly into `pmda.py`.
  - New maximum `pmda.py` line budget at this checkpoint: `111,520` lines.
- Tests added:
  - `tests/test_job_status_core.py`
  - `tests/test_metadata_jobs.py`
  - Additional storage progress payload coverage in `tests/test_storage_buckets.py`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_core/job_status.py pmda_enrichment/metadata_jobs.py pmda_discovery/storage_buckets.py` passed.
  - Focused tests passed: `23 passed, 1 warning`.
  - Full backend suite passed: `653 passed, 3 warnings, 7 subtests passed`.
  - Frontend production build passed: `npm run build`.
  - Static gates passed: `pipeline_audit_gate.py`, `legacy_cleanup_gate.py`, `pmda_bootstrap_gate.py`.
- Current state:
  - `pmda.py` remains large, but three more status/persistence payload responsibilities are now extracted and guarded.
  - Remaining high-impact extraction targets are still the large scan runner, filesystem discovery/resume plan, matching/provider lookup, publication rebuild, and materialization/move orchestration.

## 2026-05-16 Internal Refactor Checkpoint: Log Tail, Scan Progress, Snapshot SQL, Provider Stats

- Scope:
  - Continued local-only extraction work after the jobs/metadata/storage checkpoint.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed.
- Code extracted:
  - `pmda_core/log_tail.py` now owns backend log tail reading, parsing, relevance filtering, and stable thread slot generation.
  - `pmda_core/scan_progress.py` now owns scan phase labels, progress cache key construction, ETA confidence, progress mode, and hot scanning payload construction.
  - `pmda_publication/snapshot.py` now owns published-snapshot SQL builders and published/fallback browse source policy.
  - `pmda_core/provider_matching.py` now owns scan provider match key ordering and provider match counter normalization.
- Compatibility:
  - Existing HTTP routes and MCP contracts were not changed.
  - `pmda.py` keeps compatibility wrappers where tests or blueprint wiring still call historical helper names.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now blocks reintroducing the extracted log-tail, scan-progress, publication snapshot SQL, and provider-match-stat logic into `pmda.py`.
  - Current `pmda.py` line budget: `110,900` lines.
- Tests added or extended:
  - `tests/test_log_tail_core.py`
  - `tests/test_scan_progress_core.py`
  - `tests/test_publication_snapshot.py`
  - `tests/test_provider_matching_core.py`
- Validation:
  - `python3 -m py_compile pmda.py pmda_core/log_tail.py pmda_core/scan_progress.py pmda_publication/snapshot.py pmda_core/provider_matching.py` passed.
  - Static gates passed: `pipeline_audit_gate.py`, `legacy_cleanup_gate.py`, `pmda_bootstrap_gate.py`.
  - Full backend suite passed: `666 passed, 3 warnings, 7 subtests passed`.
  - Frontend production build passed: `npm run build`.
- Current state:
  - `pmda.py` is at `110,865` lines after this checkpoint.
  - Remaining largest extraction targets are still `background_scan`, `scan_duplicates`, `_build_files_editions`, `_rebuild_files_library_index`, `_api_progress_lock_fallback_payload`, and the large library browse/search route bodies.

## 2026-05-16 Internal Refactor Checkpoint: Progress Summary, AI Usage, Library Visibility, Scheduler Jobs

- Scope:
  - Continued local-only extraction work in the `/api/progress` area.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed.
- Code extracted:
  - `pmda_scan/progress_summary.py` now owns latest completed scan summary loading and chart-field normalization.
  - `pmda_scan/progress_ai.py` now owns current scan AI rollup loading and effective AI usage counters.
  - `pmda_publication/snapshot.py` now also owns progress-visible library count selection.
  - `pmda_core/pipeline_jobs.py` now also owns normalized scheduler running-job snapshots and background enrichment activity detection.
- Compatibility:
  - Existing `/api/progress` and `/api/scan/progress` payload fields were preserved.
  - `pmda.py` still owns transport/wiring and passes runtime callbacks where DB access is required.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires the new progress summary, progress AI, library visibility, and scheduler job helpers.
  - Current `pmda.py` line budget: `110,740` lines.
- Tests added or extended:
  - `tests/test_progress_summary.py`
  - `tests/test_progress_ai.py`
  - `tests/test_publication_snapshot.py`
  - `tests/test_pipeline_jobs_core.py`
- Validation:
  - `python3 -m py_compile pmda.py pmda_scan/progress_summary.py pmda_scan/progress_ai.py pmda_publication/snapshot.py pmda_core/pipeline_jobs.py` passed.
  - Static gates passed: `pipeline_audit_gate.py`, `legacy_cleanup_gate.py`, `pmda_bootstrap_gate.py`.
  - Full backend suite passed: `677 passed, 3 warnings, 7 subtests passed`.
  - Frontend production build passed: `npm run build`.
- Current state:
  - `pmda.py` is at `110,668` lines after this checkpoint.
  - `scripts/pmda_bootstrap_gate.py` line budget is now `110,680`.
  - `/api/progress` is still large, but several DB reads and derived payload sections are now in focused modules with tests.
  - Remaining largest extraction targets are still `background_scan`, `scan_duplicates`, `_build_files_editions`, `_rebuild_files_library_index`, `_api_progress_lock_fallback_payload`, and the large library browse/search route bodies.

## 2026-05-17 Internal Refactor Checkpoint: Pre-Work Progress And ETA Helpers

- Scope:
  - Continued the local-only `/api/progress` extraction after the previous pause.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed.
- Code extracted:
  - `pmda_core/scan_progress.py` now owns pre-worker stage counters for resume-scope work, filesystem discovery, published catch-up, and album candidate building.
  - `pmda_core/scan_progress.py` now owns worker/post-worker stage counters for format analysis, identification, AI batches, duplicate moves, exports, profile enrichment, background enrichment, and finalizing.
  - `pmda_core/scan_progress.py` now owns effective runtime seconds, library-ready decisions, in-flight worker progress counters, and stage ETA/rate updates.
- Compatibility:
  - Existing `/api/progress` and `/api/scan/progress` fields were preserved.
  - The normal progress path and the lock-contention fallback path now call the same tested helpers for more consistent UI semantics.
- Tests added or extended:
  - `tests/test_scan_progress_core.py` covers pre-work stage progress, worker stage progress, in-flight effective counters, runtime/library readiness, and stage ETA/rate behavior.
- Validation so far:
  - `python3 -m py_compile pmda.py pmda_core/scan_progress.py` passed.
  - Focused progress tests passed: `82 passed, 1 warning` across `tests/test_scan_progress_core.py` and `tests/test_scan_progress_state.py`.
  - `python3 scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pipeline_audit_gate.py`, `legacy_cleanup_gate.py`, `pmda_bootstrap_gate.py`.
  - Full backend suite passed: `692 passed, 3 warnings, 7 subtests passed`.
  - Frontend production build passed: `npm run build`.
- Current state:
  - `pmda.py` is at `110,574` lines after this checkpoint.
  - `scripts/pmda_bootstrap_gate.py` line budget is now `110,600`.
  - `api_progress` is down to `1,043` lines in the AST size report.
  - `_api_progress_lock_fallback_payload` is at `897` lines.
  - Remaining largest extraction targets are still `background_scan`, `scan_duplicates`, `_build_files_editions`, `_rebuild_files_library_index`, matching/provider lookup, materialization/moves, and large library browse/search route bodies.

## 2026-05-17 Internal Refactor Checkpoint: Route Blueprint Cleanup And Unraid Docker Prune

- Scope:
  - Cleaned Unraid Docker storage because many stopped pre-refactor PMDA containers/images filled the Docker vdisk.
  - Continued local-only route extraction from `pmda.py`.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Unraid cleanup:
  - Preserved running container `PMDA` using `meaning/pmda:latest`.
  - Preserved `meaning/pmda:latest`, `meaning/pmda:beta`, and `meaning/pmda:nightly`.
  - Removed stopped `PMDA_*` backup/pre-refactor containers, old `meaning/pmda:backup-*`, old `meaning/pmda:nightly-plan*`, dangling PMDA images, and Docker build cache.
  - Docker vdisk `/var/lib/docker` recovered to roughly `55G used / 41G free / 58%`.
- Code extracted:
  - `pmda_api/statistics.py` now owns `/api/review/stats` and `/api/pipeline/jobs`.
  - `pmda_api/files_export.py` now owns Files export/materialization routes:
    `/api/files/export/rebuild`, `/api/files/export/reconcile`, `/api/files/export/scan-strict`, `/api/files/export/strict-backlog`, `/api/files/match/smart-promote`, `/api/files/match/smart-promote/status`, `/api/files/export/status`.
  - `pmda_api/scheduler.py` now owns `/api/events/tasks` and `/api/scheduler/*`.
  - `pmda_api/files_sources.py` now also owns `/api/files/structure/overview` and `/api/fs/list`.
  - `pmda_api/dedupe_details.py` now owns `/api/duplicates` and the dead Plex DB library-only fallback was removed from that route.
  - `pmda_api/incomplete_albums.py` now owns broken-album backfill controls, incomplete AI-review queue controls, AI queue status/enqueue, AI overview, and `/api/broken-albums/review`.
  - `pmda_api/progress.py` now owns the `/api/progress` and `/api/scan/progress` route registration while the heavy payload builder remains in `pmda.py` for incremental extraction.
  - `pmda_api/broken_albums.py` now owns the `/api/broken-albums` and `/api/broken-albums/detail` route registration while their heavy payload builders remain in `pmda.py` for incremental extraction.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now blocks reintroducing the extracted files export, scheduler/task-event, file navigation, duplicate list, and incomplete auxiliary routes into `pmda.py`.
  - `scripts/pmda_bootstrap_gate.py` also now blocks direct progress and broken-album route decorators from returning to `pmda.py`.
  - Current `pmda.py` line budget: `109,825` lines.
- Validation so far:
  - `python3 -m py_compile` passed for touched modules.
  - Static gates passed: `pipeline_audit_gate.py`, `legacy_cleanup_gate.py`, `pmda_bootstrap_gate.py`.
  - Focused progress tests passed: `87 passed, 1 warning` across `tests/test_scan_progress_core.py` and `tests/test_scan_progress_state.py`.
- Current state:
  - `pmda.py` is at `109,821` lines after this checkpoint.
  - Direct Flask routes in `pmda.py`: `97`.
  - Remaining largest extraction targets are still `/api/progress`, `/api/broken-albums` detail/list, library discover/search/detail/social/playlists routes, `background_scan`, `scan_duplicates`, `_build_files_editions`, `_rebuild_files_library_index`, matching/provider lookup, and materialization/moves.

## 2026-05-17 Internal Refactor Checkpoint: All Flask Routes Moved To Blueprints

- Scope:
  - Continued after the MacBook RAM interruption.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_api/library_personal.py` now owns personal/social/listening routes: recently played, liked summary, social users/context/share, recommendations, notifications, playlists, playback events/stats, and reco-for-you.
  - `pmda_api/scan_history.py` now also owns scan detail, pipeline trace, trace export, and AI-cost route registration.
  - `pmda_api/scan_moves.py` now also owns scan move list/summary/artwork/detail/restore/dedupe route registration.
  - `pmda_api/assistant.py` now owns assistant status/session/chat route registration.
  - `pmda_api/dedupe_details.py` now also owns dedupe status/detail/manual action route registration.
  - `pmda_api/library_catalog.py` now owns catalog/search/facet/entity-discover route registration.
  - `pmda_api/library_detail.py` now owns artist/album/media/detail route registration.
  - `pmda_api/library_improve.py` now owns match-detail, review generation, cover selection, rematch, improve, drop improve, improve-all, and MusicBrainz tag-fix route registration.
  - `pmda_api/tools.py` now owns trash-release tool route registration.
  - `pmda_api/frontend.py` now owns integrated SPA/static/prefixed route registration.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires the new route modules and fails if any direct Flask route decorators return to `pmda.py`.
  - Current `pmda.py` line budget: `109,626` lines.
- Validation so far:
  - `python3 -m py_compile` passed for touched modules.
  - Static gates passed: `pipeline_audit_gate.py`, `legacy_cleanup_gate.py`, `pmda_bootstrap_gate.py`.
  - Flask import/url-map smoke passed using `.venv-codex-tests/bin/python`; representative routes existed after blueprint registration:
    `/api/library/albums`, `/api/library/discover`, `/api/library/album/<int:album_id>`, `/api/scan-history/<int:scan_id>`, `/api/tools/trash-releases`, `/api/ui/build`.
- Current state:
  - `pmda.py` is at `109,626` lines after this checkpoint.
  - Direct Flask routes in `pmda.py`: `0`.
  - Important caveat: most heavy route bodies still live in `pmda.py` as runtime handler functions. The route registration layer is now clean; the next work is extracting those heavy handlers and core side-effect functions by domain.
  - Remaining largest extraction targets are still `api_library_album_tracks_detail`, `background_scan`, `scan_duplicates`, `_build_files_editions`, `_rebuild_files_library_index`, matching/provider lookup, materialization/moves, and large library browse/search payload builders.

## 2026-05-17 Internal Refactor Checkpoint: Central Blueprint Registry

- Scope:
  - Continued after another MacBook RAM interruption.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_api/__init__.py` now owns the central `register_api_blueprints(...)` registry.
  - `pmda.py` now imports only `register_api_blueprints` from `pmda_api` instead of every individual `create_*_blueprint`.
  - `pmda.py` no longer calls `app.register_blueprint(...)` directly.
  - `pmda_api.scan_control.bind_scan_control_compat_aliases(...)` now owns the legacy module-level scan-control callable aliases required by the MCP dispatcher.
  - `pmda.py` no longer reads `app.view_functions` directly.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires central API blueprint registration through `pmda_api.register_api_blueprints`.
  - `scripts/pmda_bootstrap_gate.py` now requires scan-control compatibility aliases to remain in `pmda_api.scan_control`.
  - Current `pmda.py` line budget: `109,540` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_api/__init__.py pmda_api/scan_control.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pipeline_audit_gate.py`, `legacy_cleanup_gate.py`, `pmda_bootstrap_gate.py`.
  - Focused backend suite passed: `153 passed, 2 warnings` across `tests/test_scan_progress_core.py`, `tests/test_scan_progress_state.py`, `tests/test_files_publication_regressions.py`, and `tests/test_auth_rbac.py`.
  - Scan-control/MCP/log focused suite passed: `13 passed, 1 warning` across `tests/test_scan_control.py`, `tests/test_api_scan_control_blueprint.py`, `tests/test_mcp_access.py`, and `tests/test_api_logs_blueprint.py`.
  - Flask import/url-map smoke passed using `.venv-codex-tests/bin/python`: `263` rules, no missing representative routes, no missing scan-control MCP compatibility aliases.
  - Full backend suite passed after the registry extraction: `694 passed, 3 warnings, 7 subtests passed`.
  - Frontend production build passed after the registry extraction: `npm run build`.
- Current state:
  - `pmda.py` is at `109,540` lines after this checkpoint.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Direct `app.view_functions` aliasing in `pmda.py`: `0`.
  - Remaining largest extraction targets are still the actual business side-effect functions: `background_scan`, `scan_duplicates`, `_build_files_editions`, `_rebuild_files_library_index`, matching/provider lookup, materialization/moves, and large library browse/search payload builders.

## 2026-05-17 Internal Refactor Checkpoint: Progress Runtime And Browse Helper Extraction

- Scope:
  - Continued after another interruption with no Docker image build, no Unraid redeploy, and no scan launch/resume.
  - Kept the pass intentionally small after the previous central blueprint registry checkpoint.
- Code extracted:
  - `pmda_scan/progress_runtime.py` now owns shared `/api/progress` runtime adapters:
    - provider gateway live-stat normalization and cached fallback handling.
    - resume availability snapshot lookup for `full` and `changed_only` scans.
  - `pmda_core/library_browse.py` now owns pure library browse cache helpers:
    - active scan live-cache generation.
    - stable/live cache-key construction for Artists and Albums browse endpoints.
  - `pmda_enrichment/status.py` now owns `live_status_context(...)` for browse-card publication/enrichment state.
  - `pmda.py` now delegates these pieces instead of duplicating them in progress and library browse handlers.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires the new progress-runtime, browse-cache, and enrichment live-status helpers to remain extracted.
  - Current `pmda.py` line budget: `109,506` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_scan/progress_runtime.py pmda_core/library_browse.py pmda_enrichment/status.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `pipeline_audit_gate.py`, `legacy_cleanup_gate.py`.
  - Focused progress/library/enrichment suites passed:
    - `tests/test_progress_runtime.py`, `tests/test_library_browse_core.py`, `tests/test_scan_progress_core.py`, `tests/test_scan_progress_state.py`, `tests/test_progress_summary.py`.
    - `tests/test_enrichment_services.py`, `tests/test_library_scan_safe_fallbacks.py`, `tests/test_api_library_browse_blueprint.py`.
  - One full pytest run initially hit a transient tempdir cleanup failure in `tests/test_scaling_runtime.py::ScalingRuntimeTests::test_fetch_provider_album_lookup_cached_coalesces_identical_fetches`; the isolated test and the full scaling suite then passed.
  - Final full backend suite passed: `707 passed, 3 warnings, 7 subtests passed`.
  - Frontend production build passed: `npm run build`.
- Current state:
  - `pmda.py` is at `109,506` lines.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Remaining largest extraction targets are still the actual heavy business functions: `background_scan`, `scan_duplicates`, `_build_files_editions`, `_rebuild_files_library_index`, `search_mb_release_group_by_metadata`, `api_progress`, `_api_progress_lock_fallback_payload`, settings application, library browse/detail payload builders, matching/provider lookup, and materialization/moves.

## 2026-05-17 Internal Refactor Checkpoint: Progress Payload State Extraction

- Scope:
  - Continued the autonomous extraction after `progress_runtime`, browse-cache, and enrichment status helpers.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_core/scan_progress.py` now owns two additional pure progress-payload normalizers:
    - `run_scope_payload_state(...)` resets stale run-scope counters when the scan has moved past scope preparation.
    - `post_processing_payload_state(...)` resets stale post-processing counters and falls back to artist progress when post-processing is not active.
  - `pmda.py` delegates those decisions instead of keeping the stale-counter reset logic inline inside `api_progress`.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires both new helpers to stay in `pmda_core.scan_progress` and to be used by `pmda.py`.
  - Current `pmda.py` line budget: `109,497` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_core/scan_progress.py scripts/pmda_bootstrap_gate.py` passed.
  - Focused progress tests passed: `91 passed, 1 warning` across `tests/test_scan_progress_core.py` and `tests/test_scan_progress_state.py`.
  - Static gates passed: `pmda_bootstrap_gate.py`, `pipeline_audit_gate.py`, `legacy_cleanup_gate.py`.
  - Full backend suite passed: `711 passed, 3 warnings, 7 subtests passed`.
  - Frontend production build passed: `npm run build`.
- Current state:
  - `pmda.py` is at `109,497` lines.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Remaining largest extraction targets are still the actual heavy business functions: `background_scan`, `scan_duplicates`, `_build_files_editions`, `_rebuild_files_library_index`, `search_mb_release_group_by_metadata`, `_api_progress_lock_fallback_payload`, settings application, library browse/detail payload builders, matching/provider lookup, and materialization/moves.

## 2026-05-17 Internal Refactor Checkpoint: Progress Fallback Phase And Legacy Settings Filter

- Scope:
  - Continued autonomous extraction after the progress payload state checkpoint.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_core.scan_progress.fallback_phase(...)` now owns phase selection for the lock-contention `/api/progress` fallback path, including idle, pre-scan, AI batch, primary worker, export, profile enrichment, and post-processing cases.
  - `pmda_core.config.filter_disabled_external_updates(...)` now owns filtering for retired Plex source-DB, Lidarr, and Autobrr settings while still allowing Plex player refresh settings.
  - `pmda.py` delegates both decisions instead of duplicating the logic inline.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires `fallback_phase(...)` and `filter_disabled_external_updates(...)` to remain extracted.
  - The legacy cleanup gate initially rejected a literal retired Plex DB key in `pmda_core/config.py`; the helper was adjusted so extracted modules do not look like they reintroduce Plex source-DB access.
  - Current `pmda.py` line budget: `109,465` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_core/config.py pmda_core/scan_progress.py scripts/pmda_bootstrap_gate.py` passed.
  - Focused config/progress tests passed: `102 passed, 1 warning` across `tests/test_config_core.py`, `tests/test_scan_progress_core.py`, and `tests/test_scan_progress_state.py`.
  - Static gates passed: `pmda_bootstrap_gate.py`, `pipeline_audit_gate.py`, `legacy_cleanup_gate.py`.
  - Full backend suite passed: `716 passed, 3 warnings, 7 subtests passed`.
  - Frontend production build passed: `npm run build`.
- Current state:
  - `pmda.py` is at `109,465` lines.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Remaining largest extraction targets are still the actual heavy business functions: `background_scan`, `scan_duplicates`, `_build_files_editions`, `_rebuild_files_library_index`, `search_mb_release_group_by_metadata`, `_api_progress_lock_fallback_payload`, settings application, library browse/detail payload builders, matching/provider lookup, and materialization/moves.

## 2026-05-17 Internal Refactor Checkpoint: Config Update Allow-List Extraction

- Scope:
  - Continued autonomous extraction after progress fallback and legacy settings filtering.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_core.config.CONFIG_UPDATE_ALLOWED_KEYS` now owns the `/api/config` update allow-list.
  - `api_config_put()` now filters incoming settings through `_config_core.CONFIG_UPDATE_ALLOWED_KEYS` instead of keeping the large allow-list inline in `pmda.py`.
  - `pmda_core.config.filter_disabled_external_updates(...)` remains the single place filtering retired Plex source-DB, Lidarr, and Autobrr settings.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires the config update allow-list to stay extracted.
  - Current `pmda.py` line budget: `109,416` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_core/config.py scripts/pmda_bootstrap_gate.py` passed.
  - Focused config/settings tests passed: `13 passed, 1 warning` across `tests/test_config_core.py`, `tests/test_api_settings_config_blueprint.py`, and `tests/test_files_sources_api.py`.
  - Static gates passed: `pmda_bootstrap_gate.py`, `pipeline_audit_gate.py`, `legacy_cleanup_gate.py`.
  - Full backend suite passed: `717 passed, 3 warnings, 7 subtests passed`.
  - Frontend production build passed: `npm run build`.
- Current state:
  - `pmda.py` is at `109,416` lines.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Remaining largest extraction targets are still the actual heavy business functions: `background_scan`, `scan_duplicates`, `_build_files_editions`, `_rebuild_files_library_index`, `search_mb_release_group_by_metadata`, `_api_progress_lock_fallback_payload`, settings application normalization, library browse/detail payload builders, matching/provider lookup, and materialization/moves.

## 2026-05-17 Internal Refactor Checkpoint: Settings Normalization Extraction

- Scope:
  - Continued autonomous extraction after the config allow-list checkpoint.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_core.config.normalize_storage_power_saver_settings(...)` now owns disk-aware/Unraid setting normalization for both `/api/config` writes and runtime application.
  - `pmda_core.config.normalize_task_notification_settings(...)` now owns task notification boolean and cooldown normalization for both `/api/config` writes and runtime application.
  - `pmda_core.config.normalize_pipeline_bool_settings(...)` now owns pipeline boolean normalization for both `/api/config` writes and runtime application.
  - `pmda.py` still applies runtime side effects and globals, but no longer keeps those parsing rules inline.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires the extracted storage, notification, and pipeline normalization helpers to stay in `pmda_core.config`.
  - Current `pmda.py` line budget: `109,308` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_core/config.py scripts/pmda_bootstrap_gate.py` passed.
  - Focused config/settings/storage/pipeline tests passed: `33 passed, 1 warning`.
  - Static gates passed: `pmda_bootstrap_gate.py`, `pipeline_audit_gate.py`, `legacy_cleanup_gate.py`.
  - Full backend suite passed: `722 passed, 3 warnings, 7 subtests passed`.
  - Frontend production build passed: `npm run build`.
- Current state:
  - `pmda.py` is at `109,308` lines.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Remaining largest extraction targets are still the actual heavy business functions: `background_scan`, `scan_duplicates`, `_build_files_editions`, `_rebuild_files_library_index`, `search_mb_release_group_by_metadata`, `_api_progress_lock_fallback_payload`, remaining settings application side effects, library browse/detail payload builders, matching/provider lookup, and materialization/moves.

## 2026-05-17 Internal Refactor Checkpoint: Metadata Worker Settings Extraction

- Scope:
  - Continued autonomous extraction after settings normalization extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_core.config.normalize_metadata_worker_settings(...)` now owns metadata queue mode/count/batch parsing for both `/api/config` writes and runtime application.
  - `pmda.py` applies the normalized runtime values through module globals and `merged`, without keeping metadata worker parsing rules inline.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires metadata worker normalization to stay in `pmda_core.config`.
  - Current `pmda.py` line budget: `109,275` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_core/config.py scripts/pmda_bootstrap_gate.py` passed.
  - Focused config/settings/metadata/scaling tests passed: `52 passed, 1 warning`.
  - Static gates passed: `pmda_bootstrap_gate.py`, `pipeline_audit_gate.py`, `legacy_cleanup_gate.py`.
  - Full backend suite passed: `724 passed, 3 warnings, 7 subtests passed`.
  - Frontend production build passed: `npm run build`.
- Current state:
  - `pmda.py` is at `109,275` lines.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Remaining largest extraction targets are still the actual heavy business functions: `background_scan`, `scan_duplicates`, `_build_files_editions`, `_rebuild_files_library_index`, `search_mb_release_group_by_metadata`, `_api_progress_lock_fallback_payload`, remaining settings application side effects, library browse/detail payload builders, matching/provider lookup, and materialization/moves.

## 2026-05-17 Internal Refactor Checkpoint: Progress + Library Runtime Handler Extraction

- Scope:
  - Switched from small normalization passes to larger domain extraction blocks.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_scan.progress_payload` now owns the heavy `/api/progress` payload builder, including lock fallback, hot scan payloads, provider/runtime snapshots, publication visibility, AI rollup, and storage progress composition.
  - `pmda_library.browse_runtime` now owns the heavy library browse implementations for discover, artists, and albums. Public routes remain in `pmda_api`; `pmda.py` only delegates.
  - `pmda_library.detail_runtime` now owns heavy library detail/profile/match implementations for genre profile, label profile, artist detail, artist summary AI, album detail, artist similar, album match detail, and artist match detail.
  - Runtime adapters deliberately accept the live `pmda` module at the boundary as an intermediate extraction step; they block importing wrapper names back into extracted modules to avoid recursive calls.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires progress payload handlers to stay in `pmda_scan.progress_payload`.
  - `scripts/pmda_bootstrap_gate.py` now requires library browse handlers to stay in `pmda_library.browse_runtime`.
  - `scripts/pmda_bootstrap_gate.py` now requires library detail/match handlers to stay in `pmda_library.detail_runtime`.
  - Current `pmda.py` line budget: `102,531` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_scan/progress_payload.py pmda_library/browse_runtime.py pmda_library/detail_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Progress/runtime focused tests passed: `110 passed, 1 warning`.
  - Library browse/detail focused tests passed: `129 passed, 2 warnings, 7 subtests passed`.
  - Static gates passed: `pmda_bootstrap_gate.py`, `pipeline_audit_gate.py`, `legacy_cleanup_gate.py`.
  - Full backend suite passed: `724 passed, 3 warnings, 7 subtests passed`.
  - Frontend production build passed: `npm run build`.
- Current state:
  - `pmda.py` is at `102,531` lines, down `6,744` lines from the previous checkpoint.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Remaining largest extraction targets are the actual heavy business functions: `background_scan`, `scan_duplicates`, `_build_files_editions`, `_rebuild_files_library_index`, `search_mb_release_group_by_metadata`, `_improve_single_album`, `_run_files_profile_enrichment_job`, remaining settings application side effects, `api_library_search_suggest`, matching/provider lookup, and materialization/moves.

## 2026-05-17 Internal Refactor Checkpoint: Dedupe + Files Discovery Runtime Extraction

- Scope:
  - Continued the larger domain extraction pass after progress and library handlers.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_dedupe.scan_runtime` now owns the heavy duplicate scan workers: `scan_artist_duplicates` and `scan_duplicates`.
  - `pmda_discovery.files_editions_runtime` now owns the heavy Files edition discovery implementation previously in `_build_files_editions`, including filesystem album candidate construction, resume-plan checkpointing calls, canonical path handling, and Unraid disk-aware storage bucket setup.
  - `pmda.py` keeps only compatibility wrappers that delegate to the extracted runtime modules.
  - While validating the extraction, the legacy cleanup gate caught a moved Plex source-DB fallback inside the extracted duplicate worker. That fallback was removed instead of relaxing the gate; PMDA files mode still forbids Plex DB/source discovery. Plex remains allowed only as a post-pipeline player refresh integration.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires duplicate scan workers to stay in `pmda_dedupe.scan_runtime`.
  - `scripts/pmda_bootstrap_gate.py` now requires Files edition discovery to stay in `pmda_discovery.files_editions_runtime`.
  - `scripts/legacy_cleanup_gate.py` passes with the extracted modules and confirms no active Plex source-DB path was reintroduced.
  - Current `pmda.py` line budget: `97,645` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_dedupe/scan_runtime.py pmda_discovery/files_editions_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `pipeline_audit_gate.py`, `legacy_cleanup_gate.py`.
  - Storage/discovery/files focused tests passed: `124 passed, 2 warnings, 7 subtests passed`.
  - Dedupe/incomplete/progress/library focused tests passed: `134 passed, 1 warning`.
  - Full backend suite passed: `724 passed, 3 warnings, 7 subtests passed`.
  - Frontend production build passed from `frontend/`: `npm run build`.
- Current state:
  - `pmda.py` is at `97,645` lines, down `11,630` lines from the previous normalization checkpoint and down `4,886` lines from the progress/library checkpoint.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Remaining largest extraction targets are now: `background_scan`, `_rebuild_files_library_index`, `search_mb_release_group_by_metadata`, `_improve_single_album`, `_run_files_profile_enrichment_job`, `_apply_settings_in_memory`, `_improve_folder_by_path`, `_rebuild_files_library_index_for_artist`, `_fetch_bandcamp_album_info`, and `api_library_search_suggest`.

## 2026-05-17 Internal Refactor Checkpoint: Files Library Index Runtime Extraction

- Scope:
  - Continued the large-block extraction after dedupe and Files discovery.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_publication.index_rebuild_runtime` now owns the heavy Files library index rebuild jobs:
    - full `_rebuild_files_library_index`;
    - per-artist `_rebuild_files_library_index_for_artist`.
  - `pmda.py` keeps only compatibility wrappers delegating to the extracted runtime module.
  - Existing pure helpers in `pmda_publication.index_rebuild` still own state/progress helpers; this new runtime module owns the effectful DB/filesystem job bodies.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires full and per-artist Files index rebuild jobs to stay in `pmda_publication.index_rebuild_runtime`.
  - Current `pmda.py` line budget: `95,774` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_publication/index_rebuild_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `pipeline_audit_gate.py`, `legacy_cleanup_gate.py`.
  - Publication/index focused tests passed: `134 passed, 2 warnings, 7 subtests passed`.
  - Full backend suite passed: `724 passed, 3 warnings, 7 subtests passed`.
  - Frontend production build passed from `frontend/`: `npm run build`.
- Current state:
  - `pmda.py` is at `95,774` lines, down `13,501` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Remaining largest extraction targets are now: `background_scan`, `search_mb_release_group_by_metadata`, `_improve_single_album`, `_run_files_profile_enrichment_job`, `_apply_settings_in_memory`, `_improve_folder_by_path`, `_fetch_bandcamp_album_info`, `api_library_search_suggest`, `_files_try_artist_image_refresh`, and `api_config_get`.

## 2026-05-17 Internal Refactor Checkpoint: MusicBrainz Runtime Matching Extraction

- Scope:
  - Continued extraction of heavy matching logic after publication/index runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_matching.musicbrainz_runtime` now owns the heavy `search_mb_release_group_by_metadata` implementation.
  - `pmda.py` keeps only a compatibility wrapper delegating to `pmda_matching.musicbrainz_runtime`.
  - The extracted implementation replaces direct `sys.modules[__name__]` setting reads with a bound runtime helper so it still reads PMDA runtime config, not the extracted module namespace.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires MusicBrainz release-group search to stay in `pmda_matching.musicbrainz_runtime`.
  - Current `pmda.py` line budget: `94,800` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_matching/musicbrainz_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `pipeline_audit_gate.py`, `legacy_cleanup_gate.py`.
  - Matching/provider focused tests passed: `79 passed, 1 warning`.
  - Full backend suite passed: `724 passed, 3 warnings, 7 subtests passed`.
  - Frontend production build passed from `frontend/`: `npm run build`.
- Current state:
  - `pmda.py` is at `94,800` lines, down `14,475` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Remaining largest extraction targets are now: `background_scan`, `_improve_single_album`, `_run_files_profile_enrichment_job`, `_apply_settings_in_memory`, `_improve_folder_by_path`, `_fetch_bandcamp_album_info`, `api_library_search_suggest`, `_files_try_artist_image_refresh`, `api_config_get`, and `_run_files_profile_backfill`.

## 2026-05-17 Internal Refactor Checkpoint: Profile Enrichment Runtime Extraction

- Scope:
  - Continued extraction after MusicBrainz runtime matching.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_enrichment.profile_runtime` now owns:
    - `_files_try_artist_image_refresh`;
    - `_run_files_profile_enrichment_job`;
    - `_run_files_profile_backfill`.
  - `pmda.py` keeps only compatibility wrappers delegating to `pmda_enrichment.profile_runtime`.
  - Internal calls inside the extracted enrichment runtime were rewired to call extracted implementations directly, avoiding wrapper recursion while keeping external PMDA calls compatible.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires profile enrichment runtime jobs to stay in `pmda_enrichment.profile_runtime`.
  - Current `pmda.py` line budget: `93,112` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_enrichment/profile_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `pipeline_audit_gate.py`, `legacy_cleanup_gate.py`.
  - Profile/backfill/image focused tests passed: `141 passed, 1 warning`.
  - Full backend suite passed: `724 passed, 3 warnings, 7 subtests passed`.
  - Frontend production build passed from `frontend/`: `npm run build`.
- Current state:
  - `pmda.py` is at `93,112` lines, down `16,163` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Remaining largest extraction targets are now: `background_scan`, `_improve_single_album`, `_apply_settings_in_memory`, `_improve_folder_by_path`, `_fetch_bandcamp_album_info`, `api_library_search_suggest`, `api_config_get`, `api_config_put`, `api_broken_albums`, `api_broken_album_detail`, and `api_library_entity_discover`.

## 2026-05-17 Internal Refactor Checkpoint: Background Scan Runtime Extraction

- Scope:
  - Extracted the largest remaining orchestration function after profile enrichment runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_scan.background_runtime` now owns the heavy `background_scan` implementation and nested scan post-processing worker.
  - `pmda.py` keeps only a compatibility wrapper delegating to `pmda_scan.background_runtime`.
  - Transitional runtime helpers mirror `ai_provider_ready` and `AI_FUNCTIONAL_ERROR_MSG` writes back to the bound PMDA runtime module, preserving existing scan preflight behavior after extraction.
  - Direct `sys.modules[__name__]` reads inside the extracted scan body are routed through the bound runtime module.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires background scan orchestration to stay in `pmda_scan.background_runtime`.
  - Current `pmda.py` line budget: `89,991` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_scan/background_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `pipeline_audit_gate.py`, `legacy_cleanup_gate.py`.
  - Scan/files focused tests passed: `185 passed, 2 warnings, 7 subtests passed`.
  - Scan route/progress focused tests passed: `58 passed, 1 warning`.
  - Full backend suite passed: `724 passed, 3 warnings, 7 subtests passed`.
  - Frontend production build passed from `frontend/`: `npm run build`.
- Current state:
  - `pmda.py` is at `89,991` lines, down `19,284` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Remaining largest extraction targets are now: `_improve_single_album`, `_apply_settings_in_memory`, `_improve_folder_by_path`, `_fetch_bandcamp_album_info`, `api_library_search_suggest`, `api_config_get`, `api_broken_albums`, `api_config_put`, `api_broken_album_detail`, and `api_library_entity_discover`.

## 2026-05-17 Internal Refactor Checkpoint: Library Improvement Runtime Extraction

- Scope:
  - Continued extraction after background scan runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_library.improve_runtime` now owns the effectful album/folder improvement operations:
    - `_improve_single_album`;
    - `_improve_folder_by_path`.
  - `pmda.py` keeps only compatibility wrappers delegating to `pmda_library.improve_runtime`.
  - Direct `sys.modules[__name__]` reads inside the extracted improvement code are routed through the bound runtime module.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires library improvement operations to stay in `pmda_library.improve_runtime`.
  - Current `pmda.py` line budget: `88,241` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_library/improve_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `pipeline_audit_gate.py`, `legacy_cleanup_gate.py`.
  - Matching/provider/publication/image focused tests passed: `193 passed, 1 warning`.
  - API/library focused tests passed: `56 passed, 2 warnings, 7 subtests passed`.
  - Full backend suite passed: `724 passed, 3 warnings, 7 subtests passed`.
  - Frontend production build passed from `frontend/`: `npm run build`.
- Current state:
  - `pmda.py` is at `88,241` lines, down `21,034` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Remaining largest extraction targets are now: `_apply_settings_in_memory`, `_fetch_bandcamp_album_info`, `api_library_search_suggest`, `api_config_get`, `api_broken_albums`, `api_config_put`, `api_broken_album_detail`, `api_library_entity_discover`, `_run_files_similar_images_warm_job`, and `_scan_postprocess_worker` inside the extracted background runtime.

## 2026-05-17 Internal Refactor Checkpoint: Materialization, Dedupe, Incomplete, And Scan-Control Runtime Extraction

- Scope:
  - Continued bootstrap reduction after library improvement extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_materialization.export_rebuild_runtime` now owns global export-library rebuild/materialization queueing.
  - `pmda_incompletes.move_runtime` now owns automatic incomplete quarantine moves.
  - `pmda_scan.reconciliation_runtime` now owns scan track reconciliation counters.
  - `pmda_discovery.storage_bucket_runtime` now owns durable `scan_storage_buckets` row updates.
  - `pmda_dedupe.broken_runtime` now owns duplicate-to-broken heuristics.
  - `pmda_dedupe.move_runtime` now owns duplicate move background worker orchestration.
  - `pmda_dedupe.perform_runtime` now owns duplicate filesystem move helpers, cache cleanup, winner placement, hardlink/copy/move placement, and duplicate artwork/track payload helpers.
  - `pmda_dedupe.cards_runtime` now owns duplicate card payload construction; the old `plex_connect()` fallback was removed during extraction.
  - `pmda_scan.control_runtime` now owns scan start/preflight/runtime launch helpers.
  - `pmda.py` keeps only compatibility wrappers delegating to these runtime modules.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires all newly extracted runtimes to stay outside `pmda.py`.
  - Legacy cleanup gate blocks Plex source DB access in extracted modules; this caught and removed the old duplicate-card `plex_connect()` fallback.
  - Current `pmda.py` line budget: `79,000` lines.
- Validation:
  - Static gates passed after each extraction block: `pmda_bootstrap_gate.py`, `pipeline_audit_gate.py`, `legacy_cleanup_gate.py`.
  - Materialization/export focused tests passed: `62 passed, 1 warning`.
  - Incomplete/move focused tests passed: `69 passed, 1 warning`.
  - Storage/reconciliation/dedupe focused tests passed: `76 passed, 1 warning`.
  - Duplicate move/card/materialization focused tests passed after Plex fallback removal: `20 passed, 1 warning`.
  - Scan-control focused tests passed: `49 passed`.
- Current state:
  - `pmda.py` is at `78,054` lines, down `31,221` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Remaining large extraction targets include provider-specific runtimes, remaining scan-history/move detail helpers, Discogs/provider throttling, and remaining support helpers still living in `pmda.py`.

## 2026-05-17 Internal Refactor Checkpoint: Provider Gateway And Scan Audit Runtime Extraction

- Scope:
  - Continued bootstrap reduction after materialization/dedupe/scan-control extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_matching.provider_gateway_runtime` now owns provider gateway cache/coalescing/throttling, provider lookup counters, HTTP GET wrapper timing, runtime settings, and stats snapshots.
  - `pmda_scan.history_runtime` now owns scan history detail payloads, metadata rollups, pipeline trace read/export handlers, and scan AI cost summaries.
  - `pmda_scan.move_audit_runtime` now owns scan move audit/detail/restore/dedupe handlers, artwork prewarm/response helpers, track extraction from moved folders, and expected-track payload construction.
  - `pmda.py` keeps compatibility wrappers delegating to these runtime modules while later extraction passes continue.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires provider gateway runtime, scan history runtime, and scan move audit runtime logic to stay outside `pmda.py`.
  - Current `pmda.py` line budget: `76,800` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_scan/history_runtime.py pmda_scan/move_audit_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Provider gateway/progress focused tests passed earlier in this pass: `92 passed, 1 warning`.
  - Scan history/move audit focused tests passed: `57 passed, 2 warnings, 7 subtests passed`.
- Current state:
  - `pmda.py` is at `76,270` lines, down `33,005` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Album Artwork Gallery Runtime Extraction

- Scope:
  - Continued bootstrap reduction after managed runtime API handler extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_library.album_media_runtime` now owns album artwork gallery helpers: gallery cache key, Cover Art Archive gallery item lookup, Discogs gallery item lookup, gallery manifest construction, album artwork gallery endpoint, and individual artwork item serving endpoint.
  - `pmda.py` keeps only compatibility wrappers that delegate to `pmda_library.album_media_runtime`.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires artwork gallery manifest and endpoints to stay in `pmda_library.album_media_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `34,950`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_library/album_media_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Library browse/user-feedback/album-feedback/artist browse focused tests passed: `30 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `34,829` lines, down `74,446` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Managed Runtime API Handler Extraction

- Scope:
  - Continued bootstrap reduction after recommendation runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_core.managed_runtime` now also owns managed runtime API/probe handlers: MusicBrainz test, Ollama model list/discovery/probe/pull status/pull start, managed runtime status/logs/bootstrap/adopt/action handlers, and common managed-runtime root parsing.
  - `pmda.py` keeps only compatibility wrappers that delegate to `pmda_core.managed_runtime`.
  - `_reload_musicbrainz_settings_from_db` intentionally remains in `pmda.py` for now because it mutates live PMDA globals and needs a dedicated synchronized extraction pass.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires managed runtime API action handlers, MusicBrainz test, and Ollama pull handlers to stay in `pmda_core.managed_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `35,300`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_core/managed_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Runtime-AI/settings/MCP/auth focused tests passed: `26 passed, 2 warnings`.
- Current state:
  - `pmda.py` is at `35,165` lines, down `74,110` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Recommendation Runtime Extraction

- Scope:
  - Continued bootstrap reduction after Last.fm runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_library.recommendation_runtime` now owns deterministic recommendation event weights, track embedding rebuild/upsert helpers, entity discovery AI summaries, genre tokenization, embedding map reads, session profile construction, candidate fetching, recommendation ranking, and recommendation event persistence.
  - `pmda.py` keeps only compatibility wrappers that delegate to `pmda_library.recommendation_runtime`.
  - Recommendation embedding SQL and discovery summary prompt text are no longer duplicated in the bootstrap.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires recommendation scoring, embedding rebuild, and discovery summary helpers to stay in `pmda_library.recommendation_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `35,750`.
  - `scripts/autonomous_refactor_guard.py` now compiles `pmda_library/recommendation_runtime.py`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_library/recommendation_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Library browse/user-feedback/album-feedback/auth focused tests passed: `29 passed, 2 warnings`.
- Current state:
  - `pmda.py` is at `35,606` lines, down `73,669` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.
  - Remaining large extraction targets include scan pipeline trace persistence, remaining provider-specific runtimes, Discogs/provider throttling, and remaining support helpers still living in `pmda.py`.

## 2026-05-17 Internal Refactor Checkpoint: Scan Pipeline Trace Persistence Extraction

- Scope:
  - Continued bootstrap reduction after provider gateway and scan audit runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_scan.pipeline_trace_runtime` now owns scan pipeline trace persistence, trace row construction, move trace synchronization, move backlog reconciliation, duplicate/incomplete trace lookups, cached edition cover/format helpers, and scan trace status/timeline generation.
  - `pmda.py` keeps compatibility wrappers delegating to `pmda_scan.pipeline_trace_runtime`.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires scan pipeline trace persistence to stay outside `pmda.py`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - Current `pmda.py` line budget: `75,900` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_scan/pipeline_trace_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Focused trace/move/history tests passed: `5 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `75,433` lines, down `33,842` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.
  - Remaining large extraction targets include scan profile/publication target collection, remaining provider-specific runtimes, Discogs/provider throttling, and remaining support helpers still living in `pmda.py`.

## 2026-05-17 Internal Refactor Checkpoint: Scan Profile Target Collection Extraction

- Scope:
  - Continued bootstrap reduction after scan pipeline trace persistence extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_enrichment.scan_targets_runtime` now owns `_scan_collect_profile_enrich_targets`, including scan-edition target loading, duplicate-loser filtering, live publication hint merge, and missing-folder filtering for post-scan enrichment/publication.
  - `pmda.py` keeps only a compatibility wrapper delegating to `pmda_enrichment.scan_targets_runtime`.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires scan profile/publication target collection to stay outside `pmda.py`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - Current `pmda.py` line budget: `75,500` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_enrichment/scan_targets_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Publication/enrichment/files focused tests passed: `74 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `75,142` lines, down `34,133` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.
  - Remaining large extraction targets include artist person merge publication maintenance, remaining provider-specific runtimes, Discogs/provider throttling, and remaining support helpers still living in `pmda.py`.

## 2026-05-17 Internal Refactor Checkpoint: Artist Person Merge Runtime Extraction

- Scope:
  - Continued bootstrap reduction after scan profile target extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_publication.artist_merge_runtime` now owns `_files_merge_duplicate_person_artists`, including person-like artist clustering, alias/name/role/image promotion, winner/loser DB updates, alias cache refresh, and schema stamping.
  - `pmda.py` keeps only a compatibility wrapper delegating to `pmda_publication.artist_merge_runtime`.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires artist/person merge maintenance to stay outside `pmda.py`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - Current `pmda.py` line budget: `75,200` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_publication/artist_merge_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Publication artist/schema/regression focused tests passed: `54 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `74,835` lines, down `34,440` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.
  - Remaining large extraction targets include provider fallback/arbitration runtimes, Discogs/provider throttling, library digest/personal handlers, and support helpers still living in `pmda.py`.

## 2026-05-17 Internal Refactor Checkpoint: Provider Fallback Runtime Extraction

- Scope:
  - Continued bootstrap reduction after artist/person merge runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_matching.provider_fallback_runtime` now owns `_fetch_album_provider_fallbacks_parallel`, including parallel provider fallback lookup orchestration, provider enablement checks, timeout/error cache handling, provider match stat recording, extra-source assembly, and timed-out provider suppression.
  - `pmda.py` keeps only a compatibility wrapper delegating to `pmda_matching.provider_fallback_runtime`.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires provider fallback lookup orchestration to stay outside `pmda.py`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - Current `pmda.py` line budget: `74,900` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_matching/provider_fallback_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Provider fallback/arbitration focused tests passed: `69 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `74,560` lines, down `34,715` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.
  - Remaining large extraction targets include provider identity arbitration/confidence runtimes, Discogs/provider-specific runtimes, library digest/personal handlers, and support helpers still living in `pmda.py`.

## 2026-05-17 Internal Refactor Checkpoint: Provider Identity Runtime Extraction

- Scope:
  - Continued bootstrap reduction after provider fallback runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_matching.provider_identity_runtime` now owns provider identity scoring, strict/soft provider verdicts, tracklist similarity, secondary identity signals, confidence tier classification, AI tiebreak selection, and final `_arbitrate_provider_identity`.
  - `pmda.py` keeps compatibility wrappers delegating to `pmda_matching.provider_identity_runtime`.
  - The runtime binder preserves monkeypatch compatibility for existing tests while restoring the original extracted functions after patches are removed.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires provider identity arbitration and confidence helpers to stay outside `pmda.py`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - Current `pmda.py` line budget: `73,500` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_matching/provider_identity_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Provider identity/matching focused tests passed: `79 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `73,056` lines, down `36,219` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.
  - Remaining large extraction targets include Discogs/provider-specific runtimes, library digest/personal handlers, assistant/chat runtimes, and support helpers still living in `pmda.py`.

## 2026-05-17 Internal Refactor Checkpoint: Discogs Runtime Extraction

- Scope:
  - Continued bootstrap reduction after provider identity runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_matching.discogs_runtime` now owns Discogs provider error normalization, rate-limit throttling/penalties, client creation, direct API JSON fetches, release/master hydration, guarded Discogs calls, Discogs preflight, candidate caps/scoring, release search, and known-release-id fetching.
  - `pmda.py` keeps compatibility wrappers delegating to `pmda_matching.discogs_runtime`.
  - The runtime binder preserves monkeypatch compatibility for existing tests while restoring original extracted functions after patches are removed.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires Discogs throttling/client/preflight/release lookup to stay outside `pmda.py`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - Current `pmda.py` line budget: `72,900` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_matching/discogs_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Discogs/provider focused tests passed: `70 passed, 1 warning`.
  - Additional provider lookup / scan-control focused tests passed: `12 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `72,474` lines, down `36,801` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.
  - Remaining large extraction targets include public album provider runtimes, library digest/personal handlers, assistant/chat runtimes, runtime management helpers, and support helpers still living in `pmda.py`.

## 2026-05-17 Internal Refactor Checkpoint: Public Album Provider Runtime Extraction

- Scope:
  - Continued bootstrap reduction after Discogs runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_matching.public_album_providers_runtime` now owns public provider album-page parsing, JSON-LD extraction, provider meta fallback parsing, provider album search scoring, page URL generation, iTunes cover candidate normalization, and album lookup fetchers for iTunes, Deezer, Spotify, Qobuz, TIDAL, TheAudioDB, and Last.fm.
  - `pmda.py` keeps compatibility wrappers delegating to `pmda_matching.public_album_providers_runtime`.
  - The runtime binder preserves monkeypatch compatibility for existing tests while restoring original extracted functions after patches are removed.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires public album provider page parsing and lookup helpers to stay outside `pmda.py`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - Current `pmda.py` line budget: `72,000` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_matching/public_album_providers_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Web/provider focused tests passed: `111 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `71,674` lines, down `37,601` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.
  - Remaining large extraction targets include library digest/personal handlers, artist/profile provider runtimes, assistant/chat runtimes, runtime management helpers, and support helpers still living in `pmda.py`.

## 2026-05-17 Internal Refactor Checkpoint: Library Catalog Stats Runtime Extraction

- Scope:
  - Continued bootstrap reduction after public album provider runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_library.catalog_stats_runtime` now owns library digest, top artists, facets, genre/label suggestions, genre/label pages, recently played album summaries, liked summary, and playback statistics handlers.
  - `pmda.py` keeps compatibility wrappers delegating to `pmda_library.catalog_stats_runtime`.
  - Existing blueprints continue to call the runtime through the PMDA compatibility object, so public URLs remain unchanged.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires library digest/facets/genres/labels/personal stats handlers to stay outside `pmda.py`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - Current `pmda.py` line budget: `70,000` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_library/catalog_stats_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Library/API focused tests passed: `10 passed`.
- Current state:
  - `pmda.py` is at `69,701` lines, down `39,574` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.
  - Remaining large extraction targets include recommendation/playlist/playback event handlers, artist/profile provider runtimes, assistant/chat runtimes, runtime management helpers, and support helpers still living in `pmda.py`.

## 2026-05-17 Internal Refactor Checkpoint: Library Personal Runtime Extraction

- Scope:
  - Continued bootstrap reduction after library catalog stats runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_library.personal_runtime` now owns recommendation endpoints, recommendation likes, notifications, playlist CRUD/item/reorder handlers, recommendation event recording, playback event recording, and for-you recommendation payloads.
  - `pmda.py` keeps compatibility wrappers delegating to `pmda_library.personal_runtime`.
  - Existing library personal blueprints continue to call the runtime through the PMDA compatibility object, so public URLs remain unchanged.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires personal library recommendation/playlist/playback handlers to stay outside `pmda.py`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - Current `pmda.py` line budget: `69,300` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_library/personal_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Library/auth/user feedback focused tests passed: `27 passed, 2 warnings`.
- Current state:
  - `pmda.py` is at `69,063` lines, down `40,212` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.
  - Remaining large extraction targets include artist/profile provider runtimes, album detail/media handlers, assistant/chat runtimes, runtime management helpers, and support helpers still living in `pmda.py`.

## 2026-05-17 Internal Refactor Checkpoint: Assistant Chat Runtime Extraction

- Scope:
  - Continued bootstrap reduction after library personal runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_ai.assistant_chat_runtime` now owns assistant status/session/chat handlers, assistant DB-tool dispatch, playlist/recommendation helpers, SQL-agent query generation/execution/formatting, citation/link formatting, and assistant runtime readiness resolution.
  - `pmda.py` keeps compatibility wrappers delegating to `pmda_ai.assistant_chat_runtime`.
  - The runtime binder preserves monkeypatch compatibility for tests while allowing legitimate runtime helpers such as `_resolve_model_for_runtime` to be copied through.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires assistant chat, DB tools, SQL-agent handlers, and runtime status to stay outside `pmda.py`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - Current `pmda.py` line budget after this checkpoint was `66,800` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_ai/assistant_chat_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Assistant/AI focused tests passed: `7 passed, 76 deselected, 1 warning`.
- Current state:
  - `pmda.py` reached `66,513` lines after this extraction.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-17 Internal Refactor Checkpoint: Web Search Runtime Extraction

- Scope:
  - Continued bootstrap reduction after assistant chat runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_ai.web_search_runtime` now owns web result normalization, in-memory/persistent AI search cache helpers, run-level search dedupe, Ollama web-search validation, DuckDuckGo/Serper search helpers, OpenAI/Codex web-search fallback, review-hit filtering, and JSON extraction helpers used by web-search/review flows.
  - `pmda.py` keeps compatibility wrappers delegating to `pmda_ai.web_search_runtime`.
  - Cache objects and runtime configuration remain bound from `pmda.py`, preserving existing patch points and state behavior.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires web search orchestration and AI web fallbacks to stay outside `pmda.py`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - Current `pmda.py` line budget: `65,400` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_ai/web_search_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Web-search/provider/AI-cost focused tests passed: `109 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `65,147` lines, down `44,128` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.
  - Remaining large extraction targets include artist/profile provider runtimes, album detail/media handlers, managed runtime helpers, scan resume helpers, and support helpers still living in `pmda.py`.

## 2026-05-17 Internal Refactor Checkpoint: Managed Runtime Extraction

- Scope:
  - Continued bootstrap reduction after web search runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_core.managed_runtime` now owns managed Docker/Ollama/MusicBrainz runtime orchestration, including bundle persistence/logging, Docker/Compose probing, GPU profile selection, candidate adoption, bootstrap workers, MusicBrainz update/repair jobs, bundle start/stop/restart/reset helpers, managed runtime status snapshots, and Ollama model pull progress.
  - `pmda.py` keeps compatibility wrappers delegating to `pmda_core.managed_runtime`.
  - The runtime binder preserves monkeypatch compatibility for tests and keeps live PMDA globals as the source of truth for locks, state, settings, subprocess helpers, and constants.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires managed runtime orchestration to stay outside `pmda.py`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - Current `pmda.py` line budget: `63,200` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_core/managed_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Managed runtime/API/AI overview focused tests passed: `42 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `62,932` lines, down `46,343` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.
  - Remaining large extraction targets include artist/profile provider runtimes, album detail/media handlers, scan resume helpers, publication browse helpers, and support helpers still living in `pmda.py`.

## 2026-05-17 Internal Refactor Checkpoint: Artist Profile Runtime Extraction

- Scope:
  - Continued bootstrap reduction after managed runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_enrichment.artist_profile_runtime` now owns artist profile payload construction, Discogs/Bandcamp/MusicBrainz/Last.fm/Wikipedia artist profile fetch helpers, artist fact extraction handlers, artist image URL validation, MusicBrainz/Wikimedia/Last.fm/Discogs/fanart.tv/TheAudioDB/web artist image fetchers, and artist image API handlers.
  - `pmda.py` keeps compatibility wrappers delegating to `pmda_enrichment.artist_profile_runtime`.
  - `_files_try_artist_image_refresh` continues to delegate directly to `pmda_enrichment.profile_runtime` because that module still owns the profile enrichment job implementation.
- Cleanup:
  - Removed the extracted artist-image endpoint's old Plex DB fallback. Artist image lookup is now files-library-only; Plex remains player refresh only via `pmda_integrations.player_sync`.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires artist profile payloads and artist image enrichment to stay outside `pmda.py`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - Current `pmda.py` line budget: `59,800` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_enrichment/artist_profile_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Artist/profile/enrichment focused tests passed: `112 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `59,519` lines, down `49,756` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.
  - Remaining large extraction targets include album detail/media handlers, scan resume helpers, publication browse helpers, MusicBrainz fix/review handlers, and support helpers still living in `pmda.py`.

## 2026-05-18 Internal Refactor Checkpoint: Album Media Runtime Extraction

- Scope:
  - Continued bootstrap reduction after artist profile runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_library.album_media_runtime` now owns album track list/detail handlers, album tag inspection, album cover serving, album review generation, album cover selection, album profile/review provider fallback helpers, PMDA tag-writing helpers, and single-album MusicBrainz tag fix handling.
  - `pmda.py` keeps compatibility wrappers delegating to `pmda_library.album_media_runtime`.
- Cleanup:
  - Removed old Plex DB fallback branches from album track lookup, album tag inspection, and single-album tag writing. These endpoints are now files-library-only; Plex remains player refresh only via `pmda_integrations.player_sync`.
  - Moved `_USER_ALBUM_REVIEW_MAX_CHARS` lookup from function default evaluation into runtime-bound function body to keep module import safe before PMDA globals are bound.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires album media/tag/review/cover handlers to stay outside `pmda.py`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - Current `pmda.py` line budget: `58,100` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_library/album_media_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Album review/user feedback/web-search focused tests passed: `54 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `57,790` lines, down `51,485` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.
  - Remaining large extraction targets include scan resume helpers, publication browse helpers, MusicBrainz artist fix/review handlers, MCP analytics helpers, and support helpers still living in `pmda.py`.

## 2026-05-18 Internal Refactor Checkpoint: Scan Resume Runtime Extraction

- Scope:
  - Continued bootstrap reduction after album media runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_scan.resume_runtime` now owns scan resume snapshots, persistent resume run state, scan-resume artist statuses, resume files plan persistence/restore, files album scan cache row persistence, scan history refresh from publication rows, duplicate scan DB loading, and files-mode scan-plan construction.
  - `pmda.py` keeps compatibility wrappers delegating to `pmda_scan.resume_runtime`.
  - Existing publication-owned helpers stayed with their correct modules: `_recalculate_files_album_scan_cache_quality` delegates to `pmda_publication.cache_quality_runtime`, and `_reconcile_files_publication_from_scan_editions` delegates to `pmda_publication.reconcile_runtime`.
- Cleanup:
  - Removed the extracted duplicate-load Plex title fallback. Legacy duplicate rows now fall back to the stored group title instead of opening a Plex source DB.
  - Removed the extracted Plex-backed scan-plan branch. Scan planning is files-library-only; Plex remains player refresh only via `pmda_integrations.player_sync`.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires scan resume/cache/plan persistence to stay outside `pmda.py`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - Current `pmda.py` line budget: `55,100` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_scan/resume_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Scan/resume/storage focused tests passed: `140 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `54,822` lines, down `54,453` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.
  - Remaining large extraction targets include publication browse helpers, MusicBrainz artist fix/review handlers, MCP analytics helpers, artist browse entity backfill, self-diagnostics, and support helpers still living in `pmda.py`.

## 2026-05-18 Internal Refactor Checkpoint: Published Browse Runtime Extraction

- Scope:
  - Continued bootstrap reduction after scan resume runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_library.published_browse_runtime` now owns published snapshot browse reads for row counts, published artist enumeration, published catchup enqueueing, published browse counts, published artists/albums/genres/labels payloads, and underbuilt-index catchup scheduling.
  - `pmda.py` keeps compatibility wrappers delegating to `pmda_library.published_browse_runtime` for those browse helpers.
  - Snapshot policy helpers intentionally stayed with their existing owners: published scope/album SQL builders and browse-source selection delegate to `pmda_publication.snapshot`, and live enrichment status delegates to `pmda_enrichment.status`.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires published snapshot browse/count/fallback helpers to stay outside `pmda.py`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - Current `pmda.py` line budget: `54,300` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_library/published_browse_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Browse/index/publication focused tests passed: `70 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `54,079` lines, down `55,196` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.
  - Remaining large extraction targets include MusicBrainz artist fix/review handlers, MCP analytics helpers, artist browse entity backfill, self-diagnostics, and support helpers still living in `pmda.py`.

## 2026-05-18 Internal Refactor Checkpoint: MCP Runtime Extraction

- Scope:
  - Continued bootstrap reduction after published browse runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_mcp.runtime` now owns MCP token helpers, MCP auth/audit helpers, MCP scan/history/cache/provider/review/enrichment/library/storage analytics, MCP library search, review proposal creation, and MCP tool dispatch.
  - `pmda.py` keeps compatibility wrappers delegating to `pmda_mcp.runtime`, preserving existing API blueprint calls and test patch points.
  - `pmda_mcp.server` remains the local stdio bridge only; it still opens no network listener.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires MCP access, analytics, and tool dispatch helpers to stay outside `pmda.py`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - `scripts/pipeline_audit_gate.py` now reads `pmda_mcp.runtime` and `pmda_scan.resume_runtime` for invariants that moved out of the bootstrap.
  - Current `pmda.py` line budget: `52,500` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_mcp/runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py scripts/pipeline_audit_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - MCP/jobs/statistics focused tests passed: `18 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `52,275` lines, down `57,000` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.
  - Remaining large extraction targets include MusicBrainz artist fix/review handlers, artist browse entity backfill, self-diagnostics, provider/AI review helpers, and support helpers still living in `pmda.py`.

## 2026-05-18 Internal Refactor Checkpoint: Artist Browse Runtime Extraction

- Scope:
  - Continued bootstrap reduction after MCP runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_publication.artist_browse_runtime` now owns Files artist browse entity backfill/build helpers, canonical artist resolution, artist-credit splitting, contributor extraction, primary album-link repair, and duplicate artist-album link cleanup.
  - `pmda.py` keeps compatibility wrappers delegating to `pmda_publication.artist_browse_runtime`, preserving existing internal call sites and test patch points.
  - Canonical `/music/...` browse semantics and published library API contracts are unchanged.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires artist browse entity extraction/backfill helpers to stay outside `pmda.py`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - Current `pmda.py` line budget: `51,600` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_publication/artist_browse_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Artist browse/publication focused tests passed: `74 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `51,376` lines, down `57,899` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.
  - Remaining large extraction targets include MusicBrainz artist fix/review handlers, self-diagnostics, provider/AI review helpers, and support helpers still living in `pmda.py`.

## 2026-05-18 Internal Refactor Checkpoint: Legacy Plex Startup Cleanup

- Scope:
  - Removed remaining legacy Plex source/startup code paths from the bootstrap after artist browse extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code changed:
  - `_validate_plex_connection` is now a compatibility no-op that logs that Plex is supported only as a player refresh target.
  - `_self_diag` now validates files-mode writable paths/config only; it no longer opens or cross-checks a Plex database.
  - `api_musicbrainz_fix_artist_tags` now returns a `410 Gone` compatibility response instead of touching legacy Plex/MusicBrainz tag-fix flows.
  - `run_startup_checks` and `__main__` startup were simplified so the all-in-one files runtime never enters legacy Plex source mode.
- Regression guards:
  - `scripts/legacy_cleanup_gate.py` now rejects reintroduction of legacy Plex startup checks and the legacy MusicBrainz artist tag fixer.
  - `scripts/pmda_bootstrap_gate.py` line budget was tightened to `51,100`.
- Validation:
  - `python3 -m py_compile pmda.py scripts/pmda_bootstrap_gate.py scripts/legacy_cleanup_gate.py` passed.
  - Static gates passed: `legacy_cleanup_gate.py`, `pmda_bootstrap_gate.py`, `pipeline_audit_gate.py`.
  - Legacy/files fallback focused tests passed: `50 passed, 2 warnings, 7 subtests passed`.
- Current state:
  - Plex DB/source access is no longer part of startup/files mode. Plex remains allowed only through player sync/refresh integration.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Dedupe Best-Choice Runtime Extraction

- Scope:
  - Continued bootstrap reduction after legacy Plex startup cleanup.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_dedupe.choose_best_runtime` now owns duplicate AI cache helpers, heuristic winner selection, AI group batch processing, and final duplicate best-choice arbitration.
  - `pmda.py` keeps compatibility wrappers delegating to `pmda_dedupe.choose_best_runtime`, preserving existing internal call sites and test patch points.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires duplicate best-choice runtime helpers to stay outside `pmda.py`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - Current `pmda.py` line budget: `50,500` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_dedupe/choose_best_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Dedupe/API/files fallback focused tests passed: `56 passed, 2 warnings, 7 subtests passed`.
- Current state:
  - `pmda.py` was reduced to `50,368` lines after this checkpoint.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Album Review Lookup Runtime Extraction

- Scope:
  - Continued bootstrap reduction after dedupe best-choice extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_library.album_review_lookup_runtime` now owns album-review web lookup query planning, candidate scoring, source page fetch/excerpt extraction, AI candidate validation, source inference, and summary fallback helpers.
  - `pmda.py` keeps compatibility wrappers delegating to `pmda_library.album_review_lookup_runtime`, preserving API and test patch compatibility.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires album review lookup helpers to stay outside `pmda.py`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - Current `pmda.py` line budget: `49,950` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_library/album_review_lookup_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Review/web-search/files fallback focused tests passed: `92 passed, 2 warnings, 7 subtests passed`.
- Current state:
  - `pmda.py` is at `49,828` lines, down `59,447` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Audio Discovery Runtime Extraction

- Scope:
  - Continued bootstrap reduction after album review lookup extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_discovery.audio_runtime` now owns audio tag extraction, the non-checkpointed filesystem audio walker, release-segment child folder detection, and nested album-folder collapse heuristics.
  - `pmda.py` keeps compatibility wrappers delegating to `pmda_discovery.audio_runtime`, preserving existing internal call sites and tests that patch `pmda.extract_tags`.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires audio discovery helpers to stay outside `pmda.py`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - Current `pmda.py` line budget: `49,350` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_discovery/audio_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Audio discovery/files publication/release-segment/provider identity tests passed: `129 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `49,260` lines, down `60,015` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Matching Identity Runtime Extraction

- Scope:
  - Continued bootstrap reduction after audio discovery extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_matching.identity_runtime` now owns AI MusicBrainz candidate verification, AcousticID album identification, MusicBrainz artist identity lookup, classical identity context/matching, strict identity gates, MusicBrainz strict payload fetching, edition display identity resolution, AI local-context identity inference, Files identity field extraction, and album provider cross-check payload construction.
  - `pmda.py` keeps compatibility wrappers delegating to `pmda_matching.identity_runtime`, preserving existing internal call sites and tests that patch identity helpers on the `pmda` module.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires matching identity helpers to stay outside `pmda.py`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - Current `pmda.py` line budget: `47,650` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_matching/identity_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Provider identity/artist image/files publication/matching tests passed: `190 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `47,555` lines, down `61,720` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: AI Usage Runtime Extraction

- Scope:
  - Continued bootstrap reduction after matching identity extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_ai.usage_runtime` now owns AI token extraction, provider price lookup, cost computation, per-scan AI usage breakdowns, lifecycle completion checks, cost rollup refresh, batched AI usage persistence, and scan cost summary reads.
  - `pmda.py` keeps compatibility wrappers delegating to `pmda_ai.usage_runtime`, preserving existing internal call sites and tests that patch AI accounting helpers on the `pmda` module.
  - The AI usage worker thread, queue, stop event, and context stack intentionally remain in `pmda.py` for now because they are process-local mutable state; extracting them safely requires a state-aware runtime object instead of the generic wrapper binder.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires AI usage accounting and cost rollups to stay outside `pmda.py`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - Current `pmda.py` line budget: `47,000` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_ai/usage_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - AI usage/progress/history focused tests passed: `13 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `46,896` lines, down `62,379` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: External Image Cache Runtime Extraction

- Scope:
  - Continued bootstrap reduction after AI usage extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_enrichment.external_image_cache_runtime` now owns external label/artist image cache reads and writes, stale detection, label-logo prewarm, artist cache-name resolution, mirrored media-cache validation, orphan purge, reference-folder lookup, external artist image download/cache, similar-artist image URL attachment, artist alias cache promotion, and artist media map refresh.
  - `pmda.py` keeps compatibility wrappers delegating to `pmda_enrichment.external_image_cache_runtime`, preserving existing internal call sites and tests that patch image-cache helpers on the `pmda` module.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires external artist/label image cache persistence to stay outside `pmda.py`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - Current `pmda.py` line budget: `45,850` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_enrichment/external_image_cache_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Artist image/storage power saver/files publication/files index focused tests passed: `140 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `45,750` lines, down `63,525` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Publication Cover Runtime Extraction

- Scope:
  - Continued bootstrap reduction after external image cache extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_publication.cover_runtime` now owns authoritative publication tag construction, provider-refresh checks for covers, cover-provider parsing from primary tag blobs, provider identity validation for artwork, and authoritative publication cover selection/downloading/caching.
  - `pmda.py` keeps compatibility wrappers delegating to `pmda_publication.cover_runtime`, preserving existing internal call sites and tests that patch publication cover helpers on the `pmda` module.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires publication cover selection and authoritative tag construction to stay outside `pmda.py`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - Current `pmda.py` line budget: `45,550` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_publication/cover_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Files publication/provider identity/materialization focused tests passed: `116 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `45,463` lines, down `63,812` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Media Cache Runtime Extraction

- Scope:
  - Continued bootstrap reduction after publication cover extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_enrichment.media_cache_runtime` now owns media-cache root/path helpers, media-cache file detection, MIME/ETag/cache-control helpers, RAM artwork cache helpers, cached image serving, transparent image responses, WebP cache generation from paths/bytes, existing-file resolution, files-library media path promotion, and background media asset precache.
  - `pmda.py` keeps compatibility wrappers delegating to `pmda_enrichment.media_cache_runtime`, preserving existing internal call sites and tests that patch media-cache helpers on the `pmda` module.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires media-cache image generation, serving, promotion, and precache helpers to stay outside `pmda.py`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - Current `pmda.py` line budget: `45,150` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_enrichment/media_cache_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Storage power saver/artist image/files publication/files cache/job status focused tests passed: `137 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `45,071` lines, down `64,204` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Source Roots Runtime Extraction

- Scope:
  - Continued bootstrap reduction after media cache extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_discovery.source_roots_runtime` now owns source-root normalization, files source-root fetch/replace/seed helpers, effective files roots/scan roots, winner-source lookup, source-id lookup, and legacy host/container binding compatibility hooks.
  - `pmda.py` keeps compatibility wrappers delegating to `pmda_discovery.source_roots_runtime`, preserving existing internal call sites and tests that patch source-root helpers on the `pmda` module.
- Legacy cleanup:
  - Removed the old Plex DB-backed path verification, content binding discovery, single binding discovery, and PATH_MAP cross-check implementation from the extracted module.
  - The retained compatibility helpers now validate configured filesystem roots or return explicit `skipped` results. They never open `Plex Media Server.db`; Plex remains player-sync only.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires source roots, path verification, and host/container binding checks to stay outside `pmda.py`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - Current `pmda.py` line budget: `44,400` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_discovery/source_roots_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Files async/files sources/config/storage power saver focused tests passed: `41 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `44,313` lines, down `64,962` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Cache Telemetry Runtime Extraction

- Scope:
  - Continued bootstrap reduction after source roots extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_core.cache_telemetry_runtime` now owns media-cache usage telemetry, SQLite cache/state/settings DB metrics, Redis telemetry including idle CPU drift detection, PostgreSQL telemetry, and the combined cache-control metrics payload.
  - `pmda.py` keeps compatibility wrappers delegating to `pmda_core.cache_telemetry_runtime`, preserving existing internal call sites and tests that patch cache telemetry helpers on the `pmda` module.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires cache control telemetry and Redis/PostgreSQL metrics to stay outside `pmda.py`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - Current `pmda.py` line budget: `43,950` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_core/cache_telemetry_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Statistics/files cache/admin ops/RBAC/backup focused tests passed: `25 passed, 2 warnings`.
- Current state:
  - `pmda.py` is at `43,881` lines, down `65,394` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Library Workflow Runtime Extraction

- Scope:
  - Continued bootstrap reduction after cache telemetry extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_core.library_workflow_runtime` now owns Files tag-write mode resolution, audit-mode detection, normalized root-list serialization, workflow scope root calculation, trusted/intake workflow state, and settings update preparation for managed/mirror/inplace/audit modes.
  - `pmda.py` keeps compatibility wrappers delegating to `pmda_core.library_workflow_runtime`, preserving existing internal call sites and tests that patch workflow helpers on the `pmda` module.
- Deliberate non-extraction:
  - `_reload_library_mode_and_files_roots_from_db` still remains in `pmda.py` because it mutates process globals (`FILES_ROOTS`, storage power-saver settings, etc.). It should be extracted later with an explicit runtime-state sync, not through the generic wrapper binder.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires library workflow state/scopes/settings preparation to stay outside `pmda.py`.
  - `scripts/pipeline_audit_gate.py` now validates trusted destination/intake workflow across `pmda.py` plus `pmda_core.library_workflow_runtime`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - Current `pmda.py` line budget: `43,620` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_core/library_workflow_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py scripts/pipeline_audit_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Config/settings/files sources/library browse/storage focused tests passed: `84 passed, 2 warnings, 7 subtests passed`.
- Current state:
  - `pmda.py` is at `43,554` lines, down `65,721` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Materialization Helpers Runtime Extraction

- Scope:
  - Continued bootstrap reduction after library workflow extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_materialization.helpers_runtime` now owns export path component sanitization, canonical export path building, matched folder naming, album-family keys, export target folder selection, materialization confidence policy wrapper, duplicate candidate construction from destination folders, duplicate quarantine moves, scan move audit event writes, hardlink mirror detection/materialization, destination conflict detection, conflict winner selection, stable review IDs, and matched export conflict review persistence.
  - `pmda.py` keeps compatibility wrappers delegating to `pmda_materialization.helpers_runtime`, preserving existing internal call sites and tests that patch materialization helpers on the `pmda` module.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires materialization path naming, move audit, and conflict review helpers to stay outside `pmda.py`.
  - `scripts/autonomous_refactor_guard.py` compiles the new module.
  - Current `pmda.py` line budget: `42,950` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_materialization/helpers_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Materialization/move audit/files publication/provider identity focused tests passed: `122 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `42,880` lines, down `66,395` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Library Improve Batch Runtime Extraction

- Scope:
  - Continued bootstrap reduction after materialization helper extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted and cleaned:
  - `pmda_library.improve_batch_runtime` now owns improve-all album item execution, scan-edition-to-improve item construction, global improve-all worker state/progress, scan-inline profile enrichment, the legacy per-artist improve worker compatibility stub, and the MusicBrainz missing-release-group cache helper.
  - `pmda.py` keeps compatibility wrappers delegating to `pmda_library.improve_batch_runtime`, preserving existing internal call sites and tests that patch improve workers on the `pmda` module.
  - The old Plex-source improve branches were removed from the active API flow. Files-mode album/folder improvement is now the only active improvement path.
  - `pmda_library.improve_runtime._improve_single_album_impl` remains only as a compatibility stub and no longer opens Plex DB or references `metadata_items`.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires the improve batch worker and scan-inline profile enrichment to stay outside `pmda.py`.
  - `scripts/pmda_bootstrap_gate.py` also verifies `pmda_library.improve_runtime` contains no Plex source DB references.
  - `scripts/autonomous_refactor_guard.py` compiles the new improve batch module.
  - Current `pmda.py` line budget: `42,250` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_library/improve_runtime.py pmda_library/improve_batch_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Improve/files async/RBAC/scan progress/provider identity focused tests passed: `241 passed, 2 warnings, 7 subtests passed`.
- Current state:
  - `pmda.py` is at `42,222` lines, down `67,053` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Files-Only Dedupe Detail Cleanup

- Scope:
  - Continued legacy cleanup after improve batch extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code cleaned:
  - `get_duplicate_groups_from_library` is now a compatibility stub; duplicate review truth comes from Files scan/state registries only.
  - `_build_library_duplicate_group_for_artist_album` is now a compatibility stub and no longer opens Plex DB.
  - `/api/library/track/<id>/stream` is files-only and no longer falls back to Plex proxy streaming.
  - Duplicate detail payloads now build track lists from filesystem edition folders only.
  - Bonus-track merge now reads loser folders directly and no longer opens Plex DB.
  - Startup path cross-check no longer tries the retired Plex source binding cross-check path.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now checks duplicate detail/manual-build/track-stream/startup cross-check paths stay files-only.
  - Current `pmda.py` line budget: `41,950` lines.
- Validation:
  - `python3 -m py_compile pmda.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Dedupe details/library safe fallback/RBAC/library browse/files publication/materialization/scan-move tests passed: `122 passed, 2 warnings, 7 subtests passed`.
- Current state:
  - `pmda.py` is at `41,904` lines, down `67,371` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Plex Source DB Runtime Eradication Pass

- Scope:
  - Continued cleanup after files-only dedupe detail cleanup.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code cleaned:
  - Removed the remaining Plex DB fallback branches from `pmda_library.browse_runtime._api_library_artists_impl`.
  - Removed the remaining Plex DB fallback branches from `pmda_library.detail_runtime._api_library_artist_detail_impl` and `_api_library_artist_similar_impl`.
  - Converted the old `pmda.py` source-database track/title/path helpers (`get_tracks`, `get_tracks_with_ids`, `get_tracks_for_details`, `album_title`, `first_part_path`, `_album_path_under_dupes`) into explicit compatibility stubs.
  - `derive_album_title` no longer describes Plex DB as a priority source.
  - `pmda_dedupe.cards_runtime` no longer tries to infer track counts through source-database helpers.
  - `pmda_dedupe.scan_runtime` now explicitly rejects legacy source-database duplicate scans unless Files-mode prebuilt editions are supplied.
- Regression guards:
  - `scripts/legacy_cleanup_gate.py` now scans `pmda_library` for forbidden source DB references.
  - `scripts/pmda_bootstrap_gate.py` now verifies browse/detail runtimes do not contain `plex_connect` or `metadata_items`, duplicate cards do not call `get_tracks`, duplicate scan workers contain the explicit legacy-source rejection, and `pmda.py` no longer contains `metadata_items`.
  - Current `pmda.py` line budget: `41,800` lines.
- Validation:
  - `python3 -m py_compile pmda.py pmda_library/detail_runtime.py pmda_library/browse_runtime.py pmda_dedupe/scan_runtime.py pmda_dedupe/cards_runtime.py scripts/pmda_bootstrap_gate.py scripts/legacy_cleanup_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Browse/detail/dedupe/files publication/materialization focused tests passed: `127 passed, 2 warnings, 7 subtests passed`.
- Current state:
  - `pmda.py` is at `41,758` lines, down `67,517` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Library Catalog, Social, and Concert Handlers Extraction

- Scope:
  - Continued bootstrap reduction after Plex source DB runtime eradication.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_library.catalog_runtime` now owns artist typeahead suggestions and genre-to-label drilldown handlers in addition to the existing catalog/search handlers.
  - `pmda_library.personal_runtime` now owns social share/recommendation entity snapshot creation and the share endpoint implementation.
  - `pmda_library.detail_runtime` now owns artist summary reads, artist concert endpoint handling, Bandsintown/Songkick provider helpers, and OSM geocoding for concert maps.
  - `pmda.py` keeps only compatibility wrappers for the public route functions used by the blueprints.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires these catalog/detail/personal handlers to stay outside `pmda.py` and lowers the bootstrap line budget to `40,800`.
  - `scripts/autonomous_refactor_guard.py` now compiles `pmda_library/browse_runtime.py`, `pmda_library/catalog_runtime.py`, and `pmda_library/detail_runtime.py` explicitly.
- Validation:
  - `python3 -m py_compile pmda.py pmda_library/catalog_runtime.py pmda_library/personal_runtime.py pmda_library/detail_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Catalog/detail/social/browse/auth focused tests passed: `97 passed, 2 warnings, 7 subtests passed`.
- Current state:
  - `pmda.py` is at `40,734` lines, down `68,541` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Incomplete AI Review Runtime Extraction

- Scope:
  - Continued bootstrap reduction after catalog/social/concert extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_incompletes.ai_runtime` now owns incomplete-album deterministic assessment payload construction, AI evidence/diff payloads, AI schemas, verdict/conflict normalization, retry policy, staged Ollama calls, shadow verdict worker orchestration, prewarm logic, AI review status snapshots, and payload-to-assessment reconstruction.
  - `pmda.py` keeps only compatibility wrappers that delegate to `pmda_incompletes.ai_runtime`.
  - The extracted runtime was made import-safe by removing the import-time dependency on `_INCOMPLETE_AI_MAX_TRACK_ROWS`.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires incomplete AI prompts and staged review logic to stay in `pmda_incompletes.ai_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `39,900`.
  - `scripts/autonomous_refactor_guard.py` now compiles `pmda_incompletes/ai_runtime.py`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_incompletes/ai_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Incomplete/AI/progress/auth focused tests passed: `104 passed, 2 warnings`.
- Current state:
  - `pmda.py` is at `39,805` lines, down `69,470` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Scheduler Runtime Extraction

- Scope:
  - Continued bootstrap reduction after incomplete AI review runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_core.scheduler_runtime` now owns scheduler/task job normalization, task event persistence, paused-state handling, rule parsing, default/migration rules, job status updates, scan/enrichment/dedupe/export/player-sync job execution, scheduler worker orchestration, post-scan chaining, scheduler loop lifecycle, and rule replacement.
  - `pmda.py` keeps only compatibility wrappers that delegate to `pmda_core.scheduler_runtime`.
  - Mutable scheduler globals (`_scheduler_paused`, `_scheduler_thread`, and migrated config globals) are synchronized back to the PMDA runtime after extracted scheduler calls.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires scheduler workers, rules, task events, and post-scan chain logic to stay in `pmda_core.scheduler_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `38,300`.
  - `scripts/autonomous_refactor_guard.py` now compiles `pmda_core/scheduler_runtime.py`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_core/scheduler_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Scheduler/post-scan/profile-backfill/scaling/auth focused tests passed: `80 passed, 2 warnings`.
- Current state:
  - `pmda.py` is at `38,189` lines, down `71,086` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: AI Provider Config Runtime Extraction

- Scope:
  - Continued bootstrap reduction after scheduler runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_ai.provider_config_runtime` now owns AI client reinitialization from settings, AI config reload/reinit, Codex OAuth scan preflight wait, OpenAI model probing, and direct OpenAI chat-text calls used by provider runtime paths.
  - `pmda.py` keeps only compatibility wrappers that delegate to `pmda_ai.provider_config_runtime`.
  - Mutable AI provider globals (`openai_client`, `anthropic_client`, `google_client`, `google_client_configured`, `ollama_url`, `ai_provider_ready`, resolved OpenAI probe state, and probe error state) are synchronized back to the PMDA runtime after extracted calls.
  - The extracted runtime was made import-safe by avoiding import-time references to PMDA-only globals such as `OPENAI_MODEL`.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires AI client reinitialization, Codex preflight, OpenAI model probing, and OpenAI chat-text calls to stay in `pmda_ai.provider_config_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `37,900`.
  - `scripts/autonomous_refactor_guard.py` now compiles `pmda_ai/provider_config_runtime.py`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_ai/provider_config_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - AI/runtime/usage/overview/auth/progress/web-search focused tests passed: `73 passed, 2 warnings`.
- Current state:
  - `pmda.py` is at `37,792` lines, down `71,483` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Auth Session Runtime Extraction

- Scope:
  - Continued bootstrap reduction after AI provider config runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_core.auth_runtime` now owns authentication/session/RBAC helpers: auth DB access, session cache/touch logic, password/token hashing, user validation/public payloads, admin bootstrap/user creation, login session creation/deletion, rate limits, persistent IP ban tracking, bearer/cookie token resolution, auth security logging, session resolution, current-user helpers, public-user scope resolution, and request guard allow/deny rules.
  - `pmda.py` keeps only compatibility wrappers that delegate to `pmda_core.auth_runtime`.
  - The auth guard is registered explicitly with `app.before_request(_auth_guard)` so Flask cannot accidentally attach an extracted decorator to the next function.
  - The extracted runtime imports `logging` directly for import-safe default parameters and binds all remaining runtime dependencies from `pmda.py` at call time.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires auth session/RBAC helpers to stay in `pmda_core.auth_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `37,100`.
  - `scripts/autonomous_refactor_guard.py` now compiles `pmda_core/auth_runtime.py`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_core/auth_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Auth/admin/user-feedback/runtime-AI/settings/MCP/user-album-feedback focused tests passed: `35 passed, 2 warnings`.
- Current state:
  - `pmda.py` is at `36,910` lines, down `72,365` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Last.fm Runtime Extraction

- Scope:
  - Continued bootstrap reduction after auth session runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_integrations.lastfm_runtime` now owns Last.fm preflight, credential/session status, pending auth completion, session persistence, auth callback HTML, signed API calls, now-playing/scrobble/love operations, loved-track sync, lookup candidate selection, Last.fm album/artist lookup helpers, cover URL candidates, artist image lookup, and Last.fm auth API handlers.
  - `pmda.py` keeps only compatibility wrappers that delegate to `pmda_integrations.lastfm_runtime`.
  - Last.fm remains an external user/player integration; this extraction does not reintroduce Plex/Lidarr source behavior and does not affect scan source selection.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires Last.fm OAuth/scrobble/love/lookup helpers to stay in `pmda_integrations.lastfm_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `36,400`.
  - `scripts/autonomous_refactor_guard.py` now compiles `pmda_integrations/lastfm_runtime.py`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_integrations/lastfm_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Runtime-AI/Last.fm auth, user feedback, album feedback, and auth/RBAC focused tests passed: `25 passed, 2 warnings`.
- Current state:
  - `pmda.py` is at `36,284` lines, down `72,991` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Logging Runtime Extraction

- Scope:
  - Continued bootstrap reduction after album artwork gallery runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_core.logging_runtime` now owns ANSI color helpers, log tag/body parsing, domain/state inference, level/thread badges, scan/provider/MB/match/miss/AI/dupe/live/path/config/cover/art/tag logging helpers, provider flag summaries, MusicBrainz rejection compaction, and MusicBrainz candidate rejection logging.
  - `pmda.py` keeps only compatibility wrappers plus the logger setup/formatter wiring that still belongs to process bootstrap.
  - The extracted runtime binds existing PMDA globals at call time, so the log format remains compatible while the implementation is no longer in the monolith.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires logging helpers to stay in `pmda_core.logging_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `34,600`.
  - `scripts/autonomous_refactor_guard.py` now compiles `pmda_core/logging_runtime.py`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_core/logging_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Logging/log-tail/logs-API/auth focused tests passed: `25 passed, 2 warnings`.
- Current state:
  - `pmda.py` is at `34,500` lines, down `74,775` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Publication Row Runtime Extraction

- Scope:
  - Continued bootstrap reduction after logging runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_publication.row_runtime` now owns Files publication row helpers: track-entry construction from scan items, published-row upsert/delete, existing-folder filtering, strict album identity key construction, publication candidate scoring/collapse, publication row path remapping, scan-move maps, scan-edition-to-publication item conversion, live publication batching, and per-scan publication rebuild.
  - `pmda.py` keeps only compatibility wrappers that delegate to `pmda_publication.row_runtime`.
  - Existing public behavior remains unchanged for artist publishing, publication reconciliation, dedupe/materialization helpers, and tests that call the historical `pmda._...` helper names.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires publication row construction, collapse, upsert, and scan-edition conversion to stay in `pmda_publication.row_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `33,800`.
  - `scripts/autonomous_refactor_guard.py` now compiles `pmda_publication/row_runtime.py`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_publication/row_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Publication/snapshot/reconcile focused tests passed: `60 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `33,669` lines, down `75,606` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Scan Persistence Runtime Extraction

- Scope:
  - Continued bootstrap reduction after publication row runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_scan.persistence_runtime` now owns duplicate registry persistence (`save_scan_to_db`, `save_scan_artist_to_db`), duplicate-group refresh deletion, provider-no-tracklist rollups, duplicate recovery from `scan_pipeline_trace`, filesystem dir-cache reads/writes, pre-scan cache row construction, dir-cache snapshotting, and async pre-scan cache snapshot triggering.
  - `pmda.py` keeps only compatibility wrappers that delegate to `pmda_scan.persistence_runtime`.
  - The extracted runtime keeps existing resume/cache monkeypatch compatibility because historical `pmda._...` helper names still route through wrappers.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires duplicate scan persistence and pre-scan cache snapshots to stay in `pmda_scan.persistence_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `32,950`.
  - `scripts/autonomous_refactor_guard.py` now compiles `pmda_scan/persistence_runtime.py`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_scan/persistence_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Scan/cache/duplicate/provider arbitration focused tests passed: `165 passed, 2 warnings, 7 subtests passed`.
- Current state:
  - `pmda.py` is at `32,852` lines, down `76,423` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Profile Support Runtime Extraction

- Scope:
  - Continued bootstrap reduction after scan persistence extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_enrichment.profile_support_runtime` now owns profile staleness checks, cached artist/album profile reads, profile upserts, local artist-profile fallback construction, profile-enrichment queueing/state helpers, post-publish backfill pending-work probes, storage-aware profile backfill scoping, and external artist image relinking helpers.
  - `pmda.py` keeps only compatibility wrappers that delegate to `pmda_enrichment.profile_support_runtime`.
  - Disk-aware profile backfill gates remain enforced by the pipeline audit gate after the strings moved out of `pmda.py`.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires profile support persistence, pending-work probes, and storage gating to stay in `pmda_enrichment.profile_support_runtime`.
  - `scripts/autonomous_refactor_guard.py` now compiles `pmda_enrichment/profile_support_runtime.py`.
  - `scripts/pipeline_audit_gate.py` now checks disk-aware profile backfill behavior across `pmda.py` and `pmda_enrichment.profile_support_runtime`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_enrichment/profile_support_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Profile/backfill/storage/image/web-search focused tests passed: `190 passed, 2 warnings, 7 subtests passed`.
- Current state:
  - `pmda.py` was at `31,635` lines after this checkpoint.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: MusicBrainz Helper Extraction

- Scope:
  - Continued bootstrap reduction after profile support extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_matching.musicbrainz_runtime` now owns release/release-group helper lookups, release-group info fetching, MB track-title extraction, track-count extraction, live-album heuristic, tracklist cross-checks, MB submission payload preparation, MB release-group search/prefilter helpers, artist release-group indexing, album-to-MB index matching, and artist MB browsing.
  - `pmda.py` keeps only compatibility wrappers that delegate to `pmda_matching.musicbrainz_runtime`.
  - `_reload_musicbrainz_settings_from_db` intentionally remains in `pmda.py` for now because it mutates many live runtime globals and should be moved only with a dedicated settings-runtime pass.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires MusicBrainz release-group search and matching helpers to stay in `pmda_matching.musicbrainz_runtime`.
  - `scripts/autonomous_refactor_guard.py` now compiles `pmda_matching/musicbrainz_runtime.py`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `31,300`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_matching/musicbrainz_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Provider/MusicBrainz/library-scan/scan-progress focused tests passed: `173 passed, 2 warnings, 7 subtests passed`.
- Current state:
  - `pmda.py` was at `31,192` lines after this checkpoint.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Library Improve Runtime Expansion

- Scope:
  - Continued bootstrap reduction after MusicBrainz helper extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_library.improve_runtime` now also owns Files-mode manual rematch endpoints, artist rematch scheduling, one-album improve handler, drop-zone improve handler, improve-all scheduling/progress, match audit row persistence, match audit serialization, track-index inference, and folder artist/album inference.
  - `pmda.py` keeps only compatibility wrappers that delegate to `pmda_library.improve_runtime`.
  - Existing route contracts remain unchanged through `pmda_api.library_improve`.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires improve/rematch/audit helpers to stay in `pmda_library.improve_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `30,550`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_library/improve_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Provider arbitration, release-segment regressions, auth/RBAC, async pipeline, library-scan fallback, and scan-progress focused tests passed: `193 passed, 2 warnings, 7 subtests passed`.
- Current state:
  - `pmda.py` is at `30,441` lines, down `78,834` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Scan Bootstrap Runtime Extraction

- Scope:
  - Continued bootstrap reduction after library improve runtime expansion.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_scan.bootstrap_runtime` now owns pipeline bootstrap state reads, full-scan completion detection, bootstrap-state refresh from scan history, full-scan completion marking, bootstrap reset, changed-only mode gating, autonomous scan mode gating, and default scan type resolution.
  - `pmda.py` keeps only compatibility wrappers that delegate to `pmda_scan.bootstrap_runtime`.
  - The runtime binding intentionally leaves historical `pmda._pipeline_bootstrap_status` monkeypatch compatibility intact for progress and scan-control tests.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires pipeline bootstrap status/reset/default scan type helpers to stay in `pmda_scan.bootstrap_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `30,350`.
  - `scripts/autonomous_refactor_guard.py` now compiles `pmda_scan/bootstrap_runtime.py`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_scan/bootstrap_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Scan progress, scan control, files source, and library-scan fallback focused tests passed: `110 passed, 2 warnings, 7 subtests passed`.
- Current state:
  - `pmda.py` is at `30,248` lines, down `79,027` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Files Index Readiness Runtime Extraction

- Scope:
  - Continued bootstrap reduction after scan bootstrap runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_publication.index_status_runtime` now owns `_ensure_files_index_ready` behavior: files-mode readiness checks, startup live-count reconciliation, published-row catch-up scheduling, mirror-workflow startup handling, one-time trusted match flag backfill, artist browse-entity backfill, recommendation embedding backfill scheduling, bootstrap-empty state handling, and auto-bootstrap rebuild fallback.
  - `pmda.py` keeps only a compatibility wrapper that delegates to `pmda_publication.index_status_runtime`.
  - Runtime binding preserves monkeypatch compatibility for tests and API modules that patch PMDA helpers directly.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires files-index readiness logic to stay in `pmda_publication.index_status_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `30,250`.
  - `scripts/autonomous_refactor_guard.py` now compiles `pmda_publication/index_status_runtime.py`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_publication/index_status_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Library-scan fallback, auth/RBAC, user feedback, library normalization, library stats, library index status, and scan-progress focused tests passed: `131 passed, 2 warnings, 7 subtests passed`.
- Current state:
  - `pmda.py` is at `30,128` lines, down `79,147` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Classical Library Runtime Extraction

- Scope:
  - Continued bootstrap reduction after files-index readiness runtime extraction.
  - This pass deliberately extracted a larger coherent block instead of small helper-only moves.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_library.classical_runtime` now owns classical display payloads, album classical payload aggregation, classical-like browse detection, composer extraction/display, linked composer application, browse entity-kind resolution, generated classical person aliases, primary lookup-name selection, same-recording duplicate details/clustering, and classical sibling incomplete marking.
  - `pmda.py` keeps only compatibility wrappers that delegate to `pmda_library.classical_runtime`.
  - The runtime binding intentionally leaves PMDA wrapper names visible inside the extracted module to preserve test monkeypatch behavior and call-site compatibility.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires classical browse payloads, composer aliases, and same-recording duplicate helpers to stay in `pmda_library.classical_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `29,650`.
  - `scripts/autonomous_refactor_guard.py` now compiles `pmda_library/classical_runtime.py`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_library/classical_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Classical artist browse, provider arbitration, release segment, files publication, library-scan fallback, and scan-progress focused tests passed: `236 passed, 2 warnings, 7 subtests passed`.
- Current state:
  - `pmda.py` is at `29,522` lines, down `79,753` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Dedupe Signal Runtime Extraction

- Scope:
  - Continued bootstrap reduction after classical library runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_dedupe.signal_runtime` now owns music-character folding, duplicate track-title normalization, track title set/jaccard/containment helpers, track-count and duration ratio helpers, provider-ID duplicate signals, audio fingerprint sets, album audio signatures, similarity clustering, and confident duplicate signal arbitration.
  - `pmda.py` keeps only compatibility wrappers that delegate to `pmda_dedupe.signal_runtime`.
  - Runtime binding preserves live PMDA access to cache, filesystem path mapping, fpcalc, and monkeypatchable helpers.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires duplicate title normalization, provider ID signals, audio signatures, and similarity clustering to stay in `pmda_dedupe.signal_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `29,180`.
  - `scripts/autonomous_refactor_guard.py` now compiles `pmda_dedupe/signal_runtime.py`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_dedupe/signal_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Files publication regressions, duplicate details, global reviews, scan moves, provider arbitration, and artist browse focused tests passed: `136 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `29,061` lines, down `80,214` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Pipeline Job Persistence Runtime Extraction

- Scope:
  - Continued bootstrap reduction after dedupe signal runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_core.pipeline_jobs_runtime` now owns durable pipeline job heartbeat writes and pipeline job snapshot reads.
  - `pmda.py` keeps only compatibility wrappers that delegate to `pmda_core.pipeline_jobs_runtime`.
  - The pure payload helpers remain in `pmda_core.pipeline_jobs`; the new runtime module owns SQLite side effects.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires durable pipeline job heartbeat persistence to stay in `pmda_core.pipeline_jobs_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `29,060`.
  - `scripts/autonomous_refactor_guard.py` now compiles `pmda_core/pipeline_jobs_runtime.py`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_core/pipeline_jobs_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Pipeline jobs, files index status, library-scan fallback, statistics blueprint, and scan-progress focused tests passed: `119 passed, 2 warnings, 7 subtests passed`.
- Current state:
  - `pmda.py` is at `28,945` lines, down `80,330` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Audio Discovery Runtime Expansion

- Scope:
  - Continued bootstrap reduction after pipeline job persistence runtime extraction.
  - This pass extracted a coherent audio/discovery side-effect block instead of line-by-line helper moves.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_discovery.audio_runtime` now also owns changed-path album folder resolution, direct audio-folder grouping, ffprobe subprocess parsing, duration probing, folder format analysis, and `fpcalc` fingerprint subprocess isolation.
  - `pmda.py` keeps compatibility wrappers for `_resolve_album_folders_from_event_path`, `_group_audio_files_by_folder_under_roots`, `_run_ffprobe`, `_run_ffprobe_duration_sec`, `analyse_format`, and `_fpcalc_fingerprint_file`.
  - `get_ffprobe_pool` intentionally remains in `pmda.py` for now so existing settings-runtime pool reset behavior and shutdown hooks keep their current ownership.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires the expanded audio discovery and fingerprint helpers to stay in `pmda_discovery.audio_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `28,620`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_discovery/audio_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Release-segment, storage bucket, files-index status, files-publication regression, dedupe details, library-scan fallback, and scan-progress focused tests passed: `177 passed, 2 warnings, 7 subtests passed`.
- Current state:
  - `pmda.py` is at `28,566` lines, down `80,709` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Settings Reload Runtime Extraction

- Scope:
  - Continued bootstrap reduction after audio discovery runtime expansion.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_core.settings_runtime` now owns runtime reload helpers for auto-move, MusicBrainz queue/search settings, section IDs, path maps, and files-library roots/storage settings.
  - `pmda.py` keeps compatibility wrappers for `_reload_auto_move_from_db`, `_reload_musicbrainz_settings_from_db`, `_reload_section_ids_from_db`, `_reload_path_map_from_db`, and `_reload_library_mode_and_files_roots_from_db`.
  - Runtime setting reload wrappers call `_sync_runtime_globals()` after execution so changed globals such as `_mb_queue`, storage settings, roots, and scan toggles remain synchronized back to the bootstrap module.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires runtime setting reload helpers to stay in `pmda_core.settings_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `28,380`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_core/settings_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Config/settings, storage power saver/buckets, scan control, and files source focused tests passed: `43 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `28,330` lines, down `80,945` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Runtime Auto-Tune Extraction

- Scope:
  - Continued bootstrap reduction after settings reload runtime extraction.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_core.runtime_tuning` now owns runtime auto-tune snapshots, Discogs effective RPM calculation, rate-limit feedback, automatic provider/MusicBrainz tuning, and the auto-tune background worker.
  - `pmda.py` keeps compatibility wrappers for `_runtime_auto_tune_snapshot`, `_discogs_effective_rpm`, `_runtime_auto_tune_note_discogs_rate_limited`, `_runtime_auto_tune_apply`, `_runtime_auto_tune_worker`, and `_start_runtime_auto_tune_worker`.
  - Extracted tuning writes synchronize mutable runtime globals such as `PROVIDER_GATEWAY_MAX_INFLIGHT` and `MB_MIRROR_QUEUE_RPS` back to the bootstrap runtime module.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires runtime auto-tune side effects to stay in `pmda_core.runtime_tuning`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `28,200`.
  - `scripts/autonomous_refactor_guard.py` now compiles `pmda_core/runtime_tuning.py`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_core/runtime_tuning.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Scan progress, scaling runtime, files profile backfill runtime, statistics blueprint, and settings config focused tests passed: `110 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `28,149` lines, down `81,126` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Artist Identity Runtime Extraction

- Scope:
  - Continued bootstrap reduction after runtime auto-tune extraction.
  - This pass extracted a coherent publication identity block instead of small line-by-line helpers.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_publication.artist_identity_runtime` now owns files-library artist display-name selection, MusicBrainz artist identity cache lookup, alias merging, canonical artist upserts, alias-row generation, alias-table synchronization, alias-table backfill, person entity-kind selection, artist-album link merging, alias candidate lookup, and external alias upserts.
  - `pmda.py` keeps compatibility wrappers for the extracted functions so existing publication, profile enrichment, browse, and tests continue to call the same symbols.
  - `_files_merge_duplicate_person_artists` remains owned by `pmda_publication.artist_merge_runtime`; the wrapper removed during the first extraction attempt was restored and validated.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires artist canonical identity and alias-table side effects to stay in `pmda_publication.artist_identity_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `27,750`.
  - `scripts/autonomous_refactor_guard.py` now compiles `pmda_publication/artist_identity_runtime.py`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_publication/artist_identity_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Artist browse entity, artist image selection, scan progress, files profile backfill runtime, and files publication regression tests passed: `213 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `27,713` lines, down `81,562` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Tools Trash Release Runtime Extraction

- Scope:
  - Continued bootstrap reduction after artist identity runtime extraction.
  - This pass moved the trash-release tool implementation behind the existing Tools blueprint without changing public API paths.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_api.tools_runtime` now owns trash-release candidate scoring helpers, library snapshot candidate generation, per-album candidate reconstruction, destination path calculation, curation audit writes, and the admin list/action handlers.
  - `pmda.py` keeps compatibility wrappers for `_trash_release_safe_json`, `_trash_release_tags_text`, `_trash_release_compilation_flag`, `_trash_release_candidate_from_album_row`, `_trash_release_candidates_snapshot`, `_trash_release_fetch_library_album_row`, `_trash_release_destination`, `_record_library_curation_action`, `api_tools_trash_releases`, and `api_tools_trash_releases_action`.
  - The extracted action handler deliberately resolves mutable helpers through the runtime module wrappers so existing tests and operational monkeypatches still affect move/audit behavior.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires trash-release runtime handlers to stay in `pmda_api.tools_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `27,360`.
  - `scripts/autonomous_refactor_guard.py` now compiles `pmda_api/tools_runtime.py`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_api/tools_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Trash-release curation, auth/RBAC, and settings config focused tests passed: `21 passed, 2 warnings`.
- Current state:
  - `pmda.py` is at `27,317` lines, down `81,958` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: AI Guardrails And Artist Roles Runtime Extraction

- Scope:
  - Continued bootstrap reduction after tools trash-release runtime extraction.
  - This pass extracted a coherent AI runtime block: scan budget guardrails, usage recording, legacy scan AI counters, and the MusicBrainz artist-role classifier.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_ai.guardrails_runtime` now owns AI runtime context inference, scan budget ID resolution, guardrail scan snapshot reads, live guardrail state updates, scan-budget prechecks, scan guard resets, legacy scan call counters, and `record_ai_usage`.
  - `pmda_ai.artist_roles_runtime` now owns `ai_suggest_artist_roles`, including prompt construction, bounded provider call, JSON normalization, and non-fatal scan error reporting.
  - `pmda.py` keeps compatibility wrappers for all extracted functions so existing provider runtime calls, web-search calls, tests, and scan code continue using the same public symbols.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires AI guardrails/usage recording to stay in `pmda_ai.guardrails_runtime`.
  - `scripts/pmda_bootstrap_gate.py` now requires artist-role classification to stay in `pmda_ai.artist_roles_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `26,970`.
  - `scripts/autonomous_refactor_guard.py` now compiles `pmda_ai/guardrails_runtime.py` and `pmda_ai/artist_roles_runtime.py`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_ai/guardrails_runtime.py pmda_ai/artist_roles_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - AI cost tracking, provider identity arbitration, runtime AI blueprint, and scan-progress AI focused tests passed: `70 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `26,925` lines, down `82,350` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Artwork Runtime Extraction

- Scope:
  - Continued bootstrap reduction after AI guardrails and artist roles extraction.
  - This pass extracted a larger coherent artwork block instead of one helper at a time.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_enrichment.artwork_runtime` now owns embedded artwork extraction, album cover asset resolution, embedded-cover fallback lookup, local cover data URI generation, cover resizing, cover OCR execution, smart OCR trigger/context construction, web cover fetching, best cover candidate downloading, provider reference links, and pre-injection cover vision verification.
  - `pmda.py` keeps compatibility wrappers for the extracted artwork helpers so existing publication, profile enrichment, library detail, album media, and improve runtimes continue using the same public symbols.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires artwork, cover OCR, cover downloads, provider reference links, and cover-vision helpers to stay in `pmda_enrichment.artwork_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `26,280`.
  - `scripts/autonomous_refactor_guard.py` now compiles `pmda_enrichment/artwork_runtime.py`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_enrichment/artwork_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Files publication regressions, artist image selection, enrichment services, library browse blueprint, and library stats blueprint focused tests passed: `130 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `26,235` lines, down `83,040` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Dedupe Actions Runtime Extraction

- Scope:
  - Continued bootstrap reduction after artwork runtime extraction.
  - This pass moved the remaining duplicate review/action handlers out of `pmda.py` while keeping the existing `pmda_api.dedupe_details` routes unchanged.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_dedupe.actions_runtime` now owns `/api/dedupe` status payload generation, duplicate detail payloads, manual single-group dedupe start, bonus-track move, dedupe-all shared logic, merge-and-dedupe, and selected-group dedupe.
  - `pmda.py` keeps compatibility wrappers for `api_dedupe`, `details`, `_normalize_edition_as_best`, `_run_dedupe_artist_one`, `dedupe_artist`, `_merge_bonus_tracks_for_group`, `dedupe_move_track`, `_dedupe_all_impl`, `api_dedupe_all`, `dedupe_all`, `dedupe_merge_and_dedupe`, and `dedupe_selected`.
  - `pmda_api.dedupe_details` still owns routing; filesystem side effects are now under the dedupe runtime package.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires duplicate detail/action handlers to stay in `pmda_dedupe.actions_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `25,860`.
  - `scripts/autonomous_refactor_guard.py` now compiles `pmda_dedupe/actions_runtime.py`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_dedupe/actions_runtime.py scripts/pmda_bootstrap_gate.py scripts/autonomous_refactor_guard.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Dedupe details blueprint, global reviews, dedupe AI audit, release segment regressions, and scan moves focused tests passed: `21 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `25,810` lines, down `83,465` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Recommendation Embeddings Runtime Expansion

- Scope:
  - Continued bootstrap reduction after dedupe actions runtime extraction.
  - This pass expanded the existing recommendation runtime with the remaining recommendation-adjacent side effects still left in `pmda.py`.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_library.recommendation_runtime` now also owns full files recommendation embedding rebuilds, trusted match/completeness flag backfill, and the local genre-overlap similar-artist fallback.
  - `pmda.py` keeps compatibility wrappers for `_rebuild_files_reco_embeddings`, `_files_backfill_trusted_match_flags`, and `_files_similar_artists_by_genre`.
  - The existing recommendation runtime binding pattern is preserved so tests and monkeypatches can still replace runtime dependencies through the bootstrap module.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires embedding rebuild, trusted flag backfill, and similar-artist fallback helpers to stay in `pmda_library.recommendation_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `25,650`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_library/recommendation_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Library browse core, library browse blueprint, enrichment services, files index status, files publication regressions, and library index core focused tests passed: `74 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `25,606` lines, down `83,669` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Broken Album Backfill Runtime Expansion

- Scope:
  - Continued bootstrap reduction after recommendation runtime expansion.
  - This pass moved the remaining broken-album snapshot/backfill helpers into the existing incomplete diagnostics runtime.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_incompletes.broken_album_runtime` now also owns broken-album row deletion, local folder snapshot resolution, backfill candidate selection, the backfill worker, and async backfill trigger.
  - `pmda.py` keeps compatibility wrappers for `_broken_album_delete_rows`, `_broken_album_resolve_folder_snapshot`, `_broken_album_backfill_candidates`, `_run_broken_album_backfill`, and `_trigger_broken_album_backfill_async`.
  - The runtime binding now preserves extracted implementations while still allowing tests to monkeypatch the PMDA bootstrap wrappers.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires broken-album snapshot and async backfill helpers to stay in `pmda_incompletes.broken_album_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `25,530`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_incompletes/broken_album_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Incomplete detection, incomplete albums blueprint, library scan safe fallbacks, and global reviews focused tests passed: `62 passed, 2 warnings, 7 subtests passed`.
- Current state:
  - `pmda.py` is at `25,486` lines, down `83,789` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Personal Social Runtime Expansion

- Scope:
  - Continued bootstrap reduction after broken-album backfill runtime expansion.
  - This pass moved the remaining social-context helpers and social API handlers into the existing personal library runtime.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_library.personal_runtime` now also owns social entity normalization, social notification inserts, social recommendation payload normalization, `/api/library/social/users`, and `/api/library/social/context`.
  - `pmda.py` keeps compatibility wrappers for `_social_entity_type_allowed`, `_social_entity_key_norm`, `_social_notification_insert`, `_social_recommendation_payload`, `api_library_social_users`, and `api_library_social_context`.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires social helpers and social API handlers to stay in `pmda_library.personal_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `25,410`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_library/personal_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Auth/RBAC, user feedback blueprint, library browse blueprint, and library browse core focused tests passed: `25 passed, 2 warnings`.
- Current state:
  - `pmda.py` is at `25,369` lines, down `83,906` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Album Media Runtime Expansion

- Scope:
  - Continued bootstrap reduction after personal social runtime expansion.
  - This pass moved the remaining album media/download/enrichment handlers out of the bootstrap and into the existing album media runtime.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_library.album_media_runtime` now also owns missing track-duration repair, background album detail enrichment, album ZIP download, and external label image serving.
  - `pmda.py` keeps compatibility wrappers for `_files_fix_missing_album_track_durations`, `_run_album_detail_enrichment`, `_schedule_album_detail_enrichment`, `api_library_album_download`, and `api_library_external_label_image`.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires those album media handlers to stay in `pmda_library.album_media_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `25,160`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_library/album_media_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Auth/RBAC, album user feedback, user feedback blueprint, library browse blueprint, library stats blueprint, enrichment services, and files publication regression focused tests passed: `83 passed, 2 warnings`.
- Current state:
  - `pmda.py` is at `25,118` lines, down `84,157` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Catalog Status And Artist Enrichment Expansion

- Scope:
  - Continued bootstrap reduction after album media runtime expansion.
  - This pass moved remaining catalog-status and artist-enrichment endpoint implementations out of the bootstrap.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_library.catalog_stats_runtime` now also owns recent artists and missing-tags API handlers.
  - `pmda_enrichment.artist_profile_runtime` now also owns the artist AI enrichment trigger.
  - `pmda.py` keeps compatibility wrappers for `api_library_recent_artists`, `api_library_missing_tags`, and `api_library_artist_ai_enrich`.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires those handlers to stay in their runtime modules.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `24,950`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_library/catalog_stats_runtime.py pmda_enrichment/artist_profile_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Library browse blueprint, library stats blueprint, library browse core, enrichment services, artist image selection, user feedback blueprint, and MCP access focused tests passed: `94 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `24,907` lines, down `84,368` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Provider Lookup Cache Runtime Expansion

- Scope:
  - Continued bootstrap reduction after catalog status and artist enrichment expansion.
  - This pass moved provider album lookup cache/coalescing side effects out of the bootstrap and into the provider gateway runtime.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_matching.provider_gateway_runtime` now also owns provider album lookup normalization, SQLite cache get/set, and coalesced `fetch_provider_album_lookup_cached`.
  - `pmda.py` keeps compatibility wrappers for `_provider_cache_norm`, `get_cached_provider_album_lookup`, `set_cached_provider_album_lookup`, and `fetch_provider_album_lookup_cached`.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires provider album lookup cache helpers to stay in `pmda_matching.provider_gateway_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `24,820`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_matching/provider_gateway_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Scaling runtime, provider lookup optimization, provider matching core, provider identity arbitration, web search runtime, and MCP access focused tests passed: `156 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `24,772` lines, down `84,503` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Identity Normalization Runtime Expansion

- Scope:
  - Continued bootstrap reduction after provider lookup cache runtime expansion.
  - This pass moved strict identity normalization, album/artist equivalence scoring, MusicBrainz artist-credit extraction, and related helper constants out of the bootstrap.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_matching.identity_runtime` now also owns strict identity text/album normalization, artist-credit splitting, feature-clause stripping, various-artists detection, compilation tag checks, album-variant scoring, provider title/artist identity scoring, and MusicBrainz artist-name extraction.
  - `pmda.py` keeps compatibility wrappers for these helpers so existing tests and runtime modules can continue to patch or call `pmda.*`.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires the identity normalization helpers and MusicBrainz artist extraction helper to stay in `pmda_matching.identity_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `24,530`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_matching/identity_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Provider matching core, provider identity arbitration, matching confidence, provider lookup optimization, web search runtime, and files publication regression focused tests passed: `171 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `24,488` lines, down `84,787` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Strict Provider Helper Runtime Expansion

- Scope:
  - Continued bootstrap reduction after identity normalization runtime expansion.
  - This pass moved strict provider ID lookup, strict provider payload fetching, strict cold-fetch gating, and strict identity clearing out of the bootstrap.
  - `_strict_validate_edition_match` intentionally remains in `pmda.py` for now because existing tests and scan code patch `pmda._strict_payload_for_provider` and `pmda._strict_provider_match_100`; moving that orchestration loop requires a dedicated compatibility-safe pass.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_matching.identity_runtime` now also owns `_strict_expected_provider_id`, `_strict_payload_for_provider`, `_strict_provider_cold_fetch_allowed`, and `_strict_clear_identity_on_reject`.
  - `pmda.py` keeps compatibility wrappers for those helpers.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires these strict provider helpers to stay in `pmda_matching.identity_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `24,320`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_matching/identity_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Provider lookup optimization, web search runtime, files publication regressions, and provider identity arbitration focused tests passed: `161 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `24,287` lines, down `84,988` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Broken Album Detection Runtime Expansion

- Scope:
  - Continued bootstrap reduction after strict provider helper runtime expansion.
  - This pass moved incomplete/broken album detection and filesystem cross-check diagnostics out of the bootstrap.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_incompletes.broken_album_runtime` now also owns `detect_broken_album` and `_incomplete_album_disk_crosscheck`.
  - `pmda.py` keeps compatibility wrappers for both helpers so existing scan code, API code, and tests can continue calling `pmda.*`.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires broken album detection and disk cross-check wrappers to stay in `pmda_incompletes.broken_album_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `24,180`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_incompletes/broken_album_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Incomplete detection, release-segment regressions, incomplete album API, files publication regressions, and global review focused tests passed: `71 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `24,150` lines, down `85,125` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: AI Provider Runtime API Expansion

- Scope:
  - Continued bootstrap reduction after broken album detection runtime expansion.
  - This pass moved OpenAI key checks, OpenAI/Anthropic/Google model-list handlers, AI provider model routing, and local IPv4 candidate discovery out of the bootstrap.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_ai.provider_config_runtime` now also owns `api_openai_check`, `api_openai_models`, `api_anthropic_models`, `api_google_models`, `_local_network_ipv4_candidates`, and `api_ai_models`.
  - `pmda.py` keeps compatibility wrappers for those functions.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires the AI check/model handlers and local network helper to stay in `pmda_ai.provider_config_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `23,900`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_ai/provider_config_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Runtime AI blueprint, MCP access, provider lookup optimization, and web search runtime focused tests passed: `59 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `23,867` lines, down `85,408` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Admin Maintenance Runtime Extraction

- Scope:
  - Continued bootstrap reduction after AI provider runtime API expansion.
  - This pass moved admin maintenance side effects out of the bootstrap: media cache reset, artwork RAM cache reset, export root cleanup, and Files index reset.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_core.maintenance_runtime` now owns `_maintenance_clear_artwork_ram_cache`, `_maintenance_clear_media_cache`, `_maintenance_clear_export_root`, and `_maintenance_clear_files_index`.
  - `pmda.py` keeps compatibility wrappers for the admin ops blueprint and any internal callers.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires admin maintenance reset side effects to stay in `pmda_core.maintenance_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `23,740`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_core/maintenance_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Admin ops blueprint, runtime AI blueprint, and MCP access focused tests passed: `12 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `23,710` lines, down `85,565` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Scan Move Insert Runtime Extraction

- Scope:
  - Continued bootstrap reduction after admin maintenance runtime extraction.
  - This pass moved scan move schema inspection and audit-row insertion out of the bootstrap.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_scan.move_audit_runtime` now also owns `_scan_moves_columns` and `_insert_scan_move_row`.
  - `pmda.py` keeps compatibility wrappers for scan move audit callers.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires scan move insertion/schema fallback code to stay in `pmda_scan.move_audit_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `23,670`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_scan/move_audit_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Scan moves API/core, incomplete albums API, and files publication regression focused tests passed: `56 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `23,641` lines, down `85,634` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Incremental Scan History Runtime Extraction

- Scope:
  - Continued bootstrap reduction after scan move insert runtime extraction.
  - This pass moved running `scan_history` counter updates out of the bootstrap and into scan persistence runtime.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_scan.persistence_runtime` now also owns `update_scan_history_incremental`.
  - `pmda.py` keeps a compatibility wrapper for scan workers and API callers.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires incremental scan history persistence to stay in `pmda_scan.persistence_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `23,610`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_scan/persistence_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Scan history API, scan progress state, scan runner, and scan orchestrator focused tests passed: `68 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `23,585` lines, down `85,690` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Final Scan Summary Runtime Extraction

- Scope:
  - Continued bootstrap reduction after incremental scan history runtime extraction.
  - This pass moved final summary counters, DB count deltas, library count reads, and final Discord notification emission out of the bootstrap.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_scan.summary_runtime` now owns process-start summary baseline capture and `emit_final_summary`.
  - `pmda.py` keeps a compatibility wrapper registered with `atexit`.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires final summary side effects to stay in `pmda_scan.summary_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `23,520`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_scan/summary_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Scan progress state, progress summary, scan history API, and scan runner focused tests passed: `64 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `23,497` lines, down `85,778` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: AI Overview Runtime Expansion

- Scope:
  - Continued bootstrap reduction after final scan summary runtime extraction.
  - This pass moved AI overview benchmark parsing, AI domain usage SQL rollups, and the AI overview snapshot into the AI usage runtime.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_ai.usage_runtime` now also owns `_analysis_dir_path`, `_latest_ai_benchmark_for_domain`, `_ai_domain_usage_summary`, and `_ai_overview_snapshot`.
  - `pmda.py` keeps compatibility wrappers for tests, API callers, and runtime callers.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires AI overview/benchmark/usage summary logic to stay in `pmda_ai.usage_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `23,390`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_ai/usage_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - AI overview, AI cost tracking, AI usage level, progress AI, and runtime AI blueprint focused tests passed: `21 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `23,358` lines, down `85,917` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Provider Gateway Best-Effort Stats Runtime Extraction

- Scope:
  - Continued bootstrap reduction after AI overview runtime expansion.
  - This pass moved provider gateway best-effort stats snapshot assembly and the nonblocking lock helper out of the bootstrap.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_matching.provider_gateway_runtime` now owns `_provider_gateway_stats_snapshot_best_effort` and the default `_lock_try_acquire_nonblocking` implementation.
  - `pmda.py` keeps compatibility wrappers for existing callers and tests.
  - The extracted runtime preserves old monkeypatch behavior by consulting the bound PMDA runtime helper when tests or callers patch `pmda._lock_try_acquire_nonblocking`.
- Regression found and fixed:
  - The first extraction removed the bootstrap compatibility lock helper too aggressively, which broke progress hot-path tests.
  - The fix keeps a thin wrapper in `pmda.py` and makes the provider runtime respect patched runtime helpers before falling back to its internal default.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires provider gateway best-effort stats logic to stay in `pmda_matching.provider_gateway_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `23,310`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_matching/provider_gateway_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Provider gateway, progress hot-path, scaling runtime, and provider lookup focused tests passed: `108 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `23,283` lines, down `85,992` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Codex CLI Runtime Extraction

- Scope:
  - Continued bootstrap reduction after provider gateway best-effort stats runtime extraction.
  - This pass moved Codex CLI subprocess execution, Codex JSON event parsing, Codex prompt construction, usage normalization, and temporary image materialization out of the bootstrap.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_ai.codex_exec_runtime` now owns `run_openai_codex_exec_for_runtime`, `materialize_codex_images`, `codex_extract_final_text`, `build_codex_prompt`, and `openai_usage_dict_from_codex`.
  - `pmda.py` keeps thin compatibility wrappers so existing provider/web-search runtimes and tests keep the same call surface.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires Codex CLI subprocess and image materialization logic to stay in `pmda_ai.codex_exec_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `23,180`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_ai/codex_exec_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - AI overview, AI cost tracking, runtime AI blueprint, web search runtime, and progress AI focused tests passed: `62 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `23,145` lines, down `86,130` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: SQLite Cache DB Runtime Extraction

- Scope:
  - Continued bootstrap reduction after Codex CLI runtime extraction.
  - This pass moved SQLite cache schema setup and audio/AcousticID cache accessors out of the bootstrap.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_core.cache_db_runtime` now owns cache DB initialization for `audio_cache`, `musicbrainz_cache`, `musicbrainz_album_lookup`, and `provider_album_lookup`.
  - `pmda_core.cache_db_runtime` also owns `get/set_cached_info` and `get/set_cached_acoustid`.
  - `pmda.py` keeps compatibility wrappers so tests, discovery runtime, matching runtime, and dedupe runtime keep the same runtime surface.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires SQLite cache schema and accessors to stay in `pmda_core.cache_db_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `23,070`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_core/cache_db_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Scan progress, library safe fallbacks, AI cost tracking, auth/RBAC, files sources API, scan control API, scaling runtime, and MCP access focused tests passed: `173 passed, 3 warnings, 7 subtests passed`.
- Current state:
  - `pmda.py` is at `23,034` lines, down `86,241` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: AI Domain Queue Runtime Extraction

- Scope:
  - Continued bootstrap reduction after SQLite cache DB runtime extraction.
  - This pass moved AI domain queue status, worker loop, matching/dedupe/review processors, queue metrics, and async trigger orchestration out of the bootstrap.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_ai.domain_queue_runtime` now owns matching/dedupe/review AI queue processors, worker side effects, status snapshots, and queue metrics.
  - `pmda.py` keeps compatibility wrappers so existing runtime callers and tests keep the same call names.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires AI domain queue processing and worker side effects to stay in `pmda_ai.domain_queue_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `22,850`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_ai/domain_queue_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - AI overview/cost/usage/progress, dedupe AI audit, provider identity/matching, and runtime AI blueprint focused tests passed: `92 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `22,810` lines, down `86,465` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Library Browse State And Box-Set Runtime Extraction

- Scope:
  - Continued bootstrap reduction after AI domain queue runtime extraction.
  - This pass moved live-vs-published library browse snapshot/counter logic and album box-set collapse/reindex helpers out of the bootstrap.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_library.browse_state_runtime` now owns live PostgreSQL browse counts, published snapshot fallback decisions, API lightweight browse snapshots, scan-busy checks, and browse source selection.
  - `pmda_library.box_set_runtime` now owns album box-set grouping, display artist selection, member ordering, and box-set track reindexing.
  - `pmda.py` keeps thin compatibility wrappers so existing callers and tests keep the same function names.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires browse state and box-set logic to stay in their runtime modules.
  - `scripts/pipeline_audit_gate.py` now checks the extracted browse runtime surface for lightweight/published fallback behavior instead of assuming that logic lives in `pmda.py`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `22,520`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_library/browse_state_runtime.py pmda_library/box_set_runtime.py scripts/pmda_bootstrap_gate.py scripts/pipeline_audit_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Box-set, library browse core, scan-safe fallbacks, scan progress, library browse/stats API, and pagination focused tests passed: `117 passed, 2 warnings, 7 subtests passed`.
- Current state:
  - `pmda.py` is at `22,494` lines, down `86,781` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: External Artist Image Refresh Runtime Extraction

- Scope:
  - Continued bootstrap reduction after library browse state and box-set runtime extraction.
  - This pass moved external artist-image authoritative refresh rules, classical/entity image heuristics, Wikipedia language selection, and the SQL predicate builder out of the bootstrap.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_enrichment.external_image_cache_runtime` now owns `_artist_entity_is_classical_like`, `_artist_wikipedia_lang_candidates`, `_artist_external_image_requires_authoritative_refresh`, and `_artist_external_image_requires_authoritative_refresh_sql`.
  - `pmda.py` keeps thin compatibility wrappers so profile/enrichment runtimes and tests keep the same call surface.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires external artist-image refresh logic and SQL predicates to stay in `pmda_enrichment.external_image_cache_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `22,400`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_enrichment/external_image_cache_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Artist image selection, enrichment service, files profile backfill, and publication artist maintenance focused tests passed: `95 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `22,378` lines, down `86,897` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Similar Artist MusicBrainz Runtime Extraction

- Scope:
  - Continued bootstrap reduction after external artist-image refresh runtime extraction.
  - This pass moved MusicBrainz similar-artist lookup logic out of the bootstrap and into the artist profile enrichment runtime.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_enrichment.artist_profile_runtime` now owns `get_similar_artists_mb`, including MusicBrainz artist relations, tag-based expansion, duplicate filtering, and error logging.
  - `pmda.py` keeps a thin compatibility wrapper so existing artist profile and similar image callers keep the same function name.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires `get_similar_artists_mb_for_runtime` to stay in `pmda_enrichment.artist_profile_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `22,320`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_enrichment/artist_profile_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Artist image selection, enrichment service, files profile backfill, and library browse blueprint focused tests passed: `94 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `22,310` lines, down `86,965` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Ollama Routing Runtime Extraction

- Scope:
  - Continued bootstrap reduction after similar-artist MusicBrainz runtime extraction.
  - This pass moved Ollama model prewarm and hard-case routing decisions out of the bootstrap and into the AI provider configuration runtime.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_ai.provider_config_runtime` now owns `_ollama_prewarm_model` and `_ollama_route_for_analysis`, including keep-alive selection, prewarm cache state, model availability checks, hard-model escalation scoring, and route metadata.
  - `pmda.py` keeps thin compatibility wrappers so provider calls keep the same function names.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires Ollama prewarm and routing wrappers to stay in `pmda_ai.provider_config_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `22,220`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_ai/provider_config_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Runtime AI, AI overview, AI usage level, and web search focused tests passed: `54 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `22,213` lines, down `87,062` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Strict Edition Match Runtime Extraction

- Scope:
  - Continued bootstrap reduction after Ollama routing runtime extraction.
  - This pass moved final strict edition validation out of the bootstrap and into the album identity matching runtime.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_matching.identity_runtime` now owns `_strict_validate_edition_match`, including provider order, prefetched provider payload handling, cold-fetch policy, strict provider verdict calls, provider failure accounting, and final reject reason selection.
  - `pmda.py` keeps a thin compatibility wrapper so existing scan/matching callers and tests keep the same function name.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires strict edition validation to stay in `pmda_matching.identity_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `22,170`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_matching/identity_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Web search runtime, provider lookup optimization, provider identity arbitration, and provider matching core focused tests passed: `117 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `22,162` lines, down `87,113` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Provider Tracklist Overlay Runtime Extraction

- Scope:
  - Continued bootstrap reduction after strict edition match runtime extraction.
  - This pass moved provider tracklist cache lookup and display overlay logic out of the bootstrap and into the album media runtime.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_library.album_media_runtime` now owns `_provider_track_titles_cached` and `_display_tracks_with_provider_overlay`, including provider tracklist cache keys, strict-provider fetch fallback, cache-only behavior, Discogs rate-limit propagation, track title overlay, sorting, and disc label fill-in.
  - `pmda.py` keeps thin compatibility wrappers so tests and existing detail handlers keep the same function names.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires provider tracklist overlay helpers to stay in `pmda_library.album_media_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `22,120`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_library/album_media_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Scaling runtime and files publication regression focused tests passed: `83 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `22,112` lines, down `87,163` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Track Display Helpers Runtime Extraction

- Scope:
  - Continued bootstrap reduction after provider tracklist overlay runtime extraction.
  - This pass moved album track title cleanup and display-field selection helpers out of the bootstrap and into the album media runtime.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_library.album_media_runtime` now owns `_clean_track_title_from_text`, `_strip_album_artist_prefixes_from_track_title`, and `_track_display_fields_from_sources`, including raw filename/title normalization, album-artist prefix stripping, provider overlay fallback selection, disc/track-number display fields, and display title fallback behavior.
  - `pmda.py` keeps thin compatibility wrappers so existing album detail/media callers keep the same function names.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires the track display helpers to stay in `pmda_library.album_media_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `22,010`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_library/album_media_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Scaling runtime, files publication regressions, and provider identity arbitration focused tests passed: `142 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `22,001` lines, down `87,274` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Fanart Artist MBID Resolution Runtime Extraction

- Scope:
  - Continued bootstrap reduction after track display helper extraction.
  - This pass moved Fanart.tv artist MBID resolution out of the bootstrap and into the artist profile runtime.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_enrichment.artist_profile_runtime` now owns `_extract_artist_mbid_from_mb_payload` and `_resolve_artist_mbid_for_fanart`, including trusted album identity gating, MusicBrainz release/release-group artist-credit lookup, and Last.fm artist MBID fallback.
  - `pmda.py` keeps thin compatibility wrappers so existing fanart and artist image callers keep the same function names.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires Fanart artist MBID resolution to stay in `pmda_enrichment.artist_profile_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `21,950`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_enrichment/artist_profile_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Artist image selection and enrichment service focused tests passed: `79 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `21,945` lines, down `87,330` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Genre Runtime Extraction

- Scope:
  - Continued bootstrap reduction after Fanart artist MBID resolution extraction.
  - This pass moved genre parsing, genre list merging, Bandcamp genre inference, dominant-genre calculation, and payload genre defaulting out of the bootstrap.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_library.genre_runtime` now owns `_split_genre_values`, `_merge_album_genre_lists`, `_infer_genre_from_bandcamp_tags`, `_dominant_genre_by_artist`, and `_apply_genre_defaults_to_albums_payload`.
  - `pmda.py` keeps thin compatibility wrappers so publication, browse, improve, and detail runtimes keep the same function names.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires genre normalization and payload-default helpers to stay in `pmda_library.genre_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `21,770`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_library/genre_runtime.py scripts/pmda_bootstrap_gate.py tests/test_library_genre_runtime.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Genre runtime, library browse core, publication snapshot, files publication regression, and library scan safe-fallback focused tests passed: `112 passed, 2 warnings, 7 subtests passed`.
- Current state:
  - `pmda.py` is at `21,762` lines, down `87,513` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Admin Ops Runtime Extraction

- Scope:
  - Continued bootstrap reduction after genre runtime extraction.
  - This pass moved admin operations snapshot, backup bundle creation, SQLite backup, PostgreSQL dump orchestration, backup listing, storage target snapshots, and bounded directory usage scanning out of the bootstrap.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_core.ops_runtime` now owns `_scan_dir_usage`, `_ops_storage_target_snapshot`, `_ops_backups_root_dir`, `_ops_backup_dir_size_bytes`, `_ops_list_backups`, `_ops_snapshot_payload`, `_ops_backup_sqlite_db`, `_ops_backup_pg_dump`, and `_ops_create_backup_bundle`.
  - `pmda.py` keeps thin compatibility wrappers so admin ops routes, statistics/cache telemetry, and tests keep the same function names.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires admin ops snapshot, backup, and directory usage side effects to stay in `pmda_core.ops_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `21,490`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_core/ops_runtime.py scripts/pmda_bootstrap_gate.py tests/test_ops_backup_snapshot.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Ops backup snapshot, admin ops blueprint, statistics blueprint, job status core, and auth RBAC focused tests passed: `27 passed, 2 warnings`.
- Current state:
  - `pmda.py` is at `21,485` lines, down `87,790` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: AI Functional Probe Runtime Extraction

- Scope:
  - Continued bootstrap reduction after admin ops runtime extraction.
  - This pass moved the OpenAI choose-best functional probe out of the bootstrap and into the AI provider configuration runtime.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_ai.provider_config_runtime` now owns `_probe_ai_choose_best_response`, including strict `index|rationale|extras` response validation and actionable `OPENAI_MODEL_PROBE_LAST_ERROR` updates.
  - `pmda.py` keeps a thin compatibility wrapper so startup and runtime AI preflight callers keep the same function name.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires the functional probe to stay in `pmda_ai.provider_config_runtime`.
  - The gate also blocks the probe prompt and parse-error strings from reappearing in `pmda.py`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `21,435`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_ai/provider_config_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - AI usage, runtime AI blueprint, provider lookup optimization, and AI overview focused tests passed: `20 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `21,429` lines, down `87,846` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Bounded Execution Runtime Extraction

- Scope:
  - Continued bootstrap reduction after AI functional probe extraction.
  - This pass moved the generic bounded callable executor out of the bootstrap and into a small core runtime module.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_core.execution_runtime` now owns `_run_callable_bounded`, including timeout handling, logging, future cancellation, and non-blocking executor shutdown.
  - `pmda.py` keeps a thin compatibility wrapper so Codex OAuth token checks and runtime client resolution keep the same function name.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires bounded callable execution to stay in `pmda_core.execution_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `21,415`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_core/execution_runtime.py scripts/pmda_bootstrap_gate.py tests/test_execution_runtime.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Execution runtime, runtime AI blueprint, and auth RBAC focused tests passed: `19 passed, 2 warnings`.
- Current state:
  - `pmda.py` is at `21,408` lines, down `87,867` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: AI Usage Policy Runtime Extraction

- Scope:
  - Continued bootstrap reduction after bounded execution runtime extraction.
  - This pass moved the AI usage level policy and feature-flag application out of the bootstrap and into the AI usage runtime.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_ai.usage_runtime` now owns `_ai_usage_level_overrides` and `_apply_ai_usage_level`, including the `limited`, `medium`, `auto`, and `aggressive` flag matrices.
  - `pmda_ai.usage_runtime` now syncs the mutable AI feature flags back to the runtime module after applying a level.
  - `pmda.py` keeps thin compatibility wrappers so startup, settings reload, and provider routing callers keep the same function names.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires AI usage policy to stay in `pmda_ai.usage_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `21,340`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_ai/usage_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - AI usage level, AI overview, settings config blueprint, and provider lookup optimization focused tests passed: `20 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `21,332` lines, down `87,943` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Codex OAuth Health Runtime Extraction

- Scope:
  - Continued bootstrap reduction after AI usage policy runtime extraction.
  - This pass moved OpenAI Codex OAuth token-health checks and connected-state evaluation out of the bootstrap and into the Codex execution runtime.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_ai.codex_exec_runtime` now owns `openai_codex_token_health_for_runtime` and `openai_codex_connected_for_runtime`, including cache TTL handling, CLI availability checks, bounded token fetches, and user-facing failure reasons.
  - `pmda.py` keeps thin compatibility wrappers for `_openai_codex_token_health` and `_openai_codex_connected`.
  - The now-unused `FutureTimeout` import was removed from `pmda.py`.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires Codex OAuth health logic to stay in `pmda_ai.codex_exec_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `21,285`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_ai/codex_exec_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Runtime AI blueprint, auth RBAC, AI usage level, and MCP access focused tests passed: `25 passed, 2 warnings`.
- Current state:
  - `pmda.py` is at `21,278` lines, down `87,997` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: AI Provider Preferences Runtime Extraction

- Scope:
  - Continued bootstrap reduction after Codex OAuth health runtime extraction.
  - This pass moved AI provider preference loading/saving out of the bootstrap and into the AI provider configuration runtime.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_ai.provider_config_runtime` now owns `_get_ai_provider_preferences` and `_save_ai_provider_preferences`, including default fallback lookup, per-user/default preference fallback, provider ID normalization, and SQLite persistence.
  - `pmda.py` keeps thin compatibility wrappers so settings, runtime AI, and scan-control callers keep the same function names.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires AI provider preference loading/saving to stay in `pmda_ai.provider_config_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `21,225`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_ai/provider_config_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Settings config blueprint, runtime AI blueprint, AI usage level, and AI overview focused tests passed: `12 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `21,220` lines, down `88,055` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: AI Runtime Provider Resolution Extraction

- Scope:
  - Continued bootstrap reduction after AI provider preference extraction.
  - This pass moved effective AI provider selection and runtime availability checks into the AI provider configuration runtime.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_ai.provider_config_runtime` now owns `_resolve_provider_for_runtime` and `_resolve_ai_runtime_availability`.
  - The extracted logic preserves user/provider preferences, local-first overrides, OpenAI API vs Codex OAuth fallback, Anthropic/Google/Ollama availability checks, and provider disabled reasons.
  - `pmda.py` keeps thin compatibility wrappers for callers that still use the historical function names.
  - The now-unused direct `select_provider_id` import was removed from `pmda.py`.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires runtime provider resolution to stay in `pmda_ai.provider_config_runtime`.
  - The gate blocks the explicit provider fallback prose and unsupported-provider fallback string from reappearing in `pmda.py`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `21,150`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_ai/provider_config_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Scan-control, runtime AI, AI overview, web-search runtime, and settings config focused tests passed: `56 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `21,145` lines, down `88,130` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: OpenAI Provider Mode And Client Resolution Extraction

- Scope:
  - Continued bootstrap reduction after AI runtime provider resolution extraction.
  - This pass moved OpenAI/Codex provider mode checks and OpenAI runtime client resolution into the AI provider configuration runtime.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_ai.provider_config_runtime` now owns `_openai_api_key_mode_enabled`, `_openai_codex_oauth_mode_enabled`, `_provider_mode_enabled`, `_provider_mode_disabled_reason`, `_openai_api_runtime_available`, `_openai_codex_runtime_available`, and `_resolve_openai_client_for_runtime`.
  - The extracted client resolver preserves Codex OAuth runtime-key derivation, optional Codex base URL handling, bounded token refresh, API-key fallback, disabled-mode reasons, and logging behavior.
  - `pmda.py` keeps thin compatibility wrappers for existing AI, scan, web-search, and runtime callers.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires OpenAI/Codex mode checks and runtime client resolution to stay in `pmda_ai.provider_config_runtime`.
  - The gate blocks Codex fallback and OpenAI API-key disabled strings from reappearing in `pmda.py`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `21,095`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_ai/provider_config_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Scan-control, runtime AI, AI overview, web-search runtime, settings config, and AI usage focused tests passed: `58 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `21,088` lines, down `88,187` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: AI Provider Helper Extraction

- Scope:
  - Continued bootstrap reduction after OpenAI provider mode and client resolution extraction.
  - This pass moved shared AI provider helper functions into the AI provider configuration runtime.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - `pmda_ai.provider_config_runtime` now owns `_openai_request_timeout_seconds`, `_normalize_provider_id`, `_provider_auth_mode`, `_openai_error_allows_codex_fallback`, and `_ai_context_from_analysis_type`.
  - The extracted helpers are still exposed through thin `pmda.py` compatibility wrappers for AI provider calls, web search, assistant chat, settings, and scan routing.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires these provider helper functions to stay in `pmda_ai.provider_config_runtime`.
  - The gate blocks the OpenAI timeout helper prose and Codex fallback marker strings from reappearing in `pmda.py`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `21,045`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_ai/provider_config_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Scan-control, runtime AI, AI overview, web-search runtime, settings config, and AI usage focused tests passed: `58 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `21,040` lines, down `88,235` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Plex Source Compatibility Isolation

- Scope:
  - Continued bootstrap reduction after AI provider helper extraction.
  - This pass moved the disabled legacy Plex source DB resolver out of `pmda.py` and into an isolated integration compatibility module.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - Added `pmda_integrations.plex_source_compat`.
  - The module owns `PLEX_DB_FILENAME`, legacy Plex DB path probing, and the disabled-by-default `ensure_plex_db_path_resolved` compatibility path.
  - `pmda.py` keeps only thin wrappers and the files-only `_ALLOW_PLEX_DB_IN_FILES_MODE = False` guard.
  - Plex remains player-refresh-only through `pmda_integrations.player_sync`; this extraction does not restore Plex source DB scanning.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires disabled Plex source compatibility helpers to stay isolated in `pmda_integrations.plex_source_compat`.
  - The gate blocks legacy Plex DB relative-path probing and legacy discovery logging from reappearing in `pmda.py`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `20,990`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_integrations/plex_source_compat.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Files-mode/Plex-safety, dedupe details, config core, and player blueprint focused tests passed: `70 passed, 2 warnings, 7 subtests passed`.
- Current state:
  - `pmda.py` is at `20,985` lines, down `88,290` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Job Status Runtime Aggregation Extraction

- Scope:
  - Continued bootstrap reduction after Plex source compatibility isolation.
  - This pass moved operator-facing job status aggregation out of `pmda.py` and into `pmda_core.job_status_runtime`.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - Added `pmda_core.job_status_runtime`.
  - `pmda_core.job_status_runtime` now owns `pmda_jobs_status_snapshot_for_runtime`.
  - `pmda.py` keeps a thin `_pmda_jobs_status_snapshot` compatibility wrapper.
  - The pure payload builder remains in `pmda_core.job_status`; the runtime adapter collects files-index, profile backfill, metadata, managed runtime, published rows, storage, and Plex-source guard state before calling the builder.
  - Restored the `select_provider_id` runtime import after validation caught that `pmda_core.settings_runtime` still expects it from the bound PMDA runtime namespace.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires job status runtime aggregation to stay outside `pmda.py`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `20,940`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_core/job_status_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Job status, statistics blueprint, and MCP access focused tests passed: `10 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `20,934` lines, down `88,341` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Wikipedia Artist Profile Helper Extraction

- Scope:
  - Continued bootstrap reduction after job status runtime aggregation extraction.
  - This pass moved Wikipedia/Wikidata artist-profile helper code out of `pmda.py` and into a dedicated enrichment helper module.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - Added `pmda_enrichment.wikipedia_runtime`.
  - The module owns Wikimedia Commons file URL creation, Wikidata media claim lookup, Wikipedia page metadata lookup, Wikipedia page-image resolution, Wikipedia intro extract lookup, provider-first artist image cache policy, and ordered string de-duplication.
  - `pmda_enrichment.artist_profile_runtime` imports these helpers directly.
  - `pmda.py` keeps thin compatibility wrappers for existing tests and runtime callers.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires Wikipedia/Wikidata artist-profile helpers to stay in `pmda_enrichment.wikipedia_runtime`.
  - The gate blocks MediaWiki/Wikidata API URL strings from reappearing in `pmda.py`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `20,750`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_enrichment/wikipedia_runtime.py pmda_enrichment/artist_profile_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Artist image/profile, statistics blueprint, and MCP access focused tests passed: `80 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `20,741` lines, down `88,534` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Album Match And Tagging Helper Extraction

- Scope:
  - Continued bootstrap reduction after Wikipedia artist profile helper extraction.
  - This pass moved album match-link helpers, PMDA tag-writing helpers, provider-attempt display helpers, and audio cover embedding out of `pmda.py`.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - Added `pmda_library.album_match_runtime`.
  - The module owns PMDA tag constants, PMDA tag writes for FLAC/MP3/MP4, cover embedding, artist-credit split helpers, match provider labels, match-attempt payloads, match-link payloads, provider payload title/artist/year/version extraction, and safe JSON loading.
  - `pmda.py` keeps thin compatibility wrappers for existing library detail, improve, and album media callers.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires album match/tagging helpers to stay in `pmda_library.album_match_runtime`.
  - The gate blocks mutagen cover embedding and ID3 `TXXX` tag-writing implementation details from reappearing in `pmda.py`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `20,500`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_library/album_match_runtime.py pmda_library/album_media_runtime.py pmda_library/detail_runtime.py pmda_library/improve_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Album user feedback, statistics blueprint, and MCP access focused tests passed: `11 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `20,488` lines, down `88,787` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Shared Image Utility Extraction

- Scope:
  - Continued bootstrap reduction after album match/tagging helper extraction.
  - This pass moved shared remote-image, path comparison, artist-image SQL, and perceptual hash helpers out of `pmda.py`.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - Added `pmda_enrichment.image_utils_runtime`.
  - The module owns remote og:image/image URL resolution, artist-folder image existence checks, canonical path comparison through an injected filesystem mapper, true artist-image SQL construction, image average-hash generation, and hex Hamming distance.
  - `pmda.py` keeps thin compatibility wrappers for browse/detail/enrichment callers.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires shared image/path/hash helpers to stay in `pmda_enrichment.image_utils_runtime`.
  - The gate blocks `og:image` scraping and media-cache artist SQL details from reappearing in `pmda.py`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `20,425`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_enrichment/image_utils_runtime.py pmda_enrichment/artist_profile_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Artist image/profile, statistics blueprint, and MCP access focused tests passed: `80 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `20,413` lines, down `88,862` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: MusicBrainz Client And Release-Group Route Extraction

- Scope:
  - Continued bootstrap reduction after shared image utility extraction.
  - This pass moved MusicBrainz client/mirror target configuration and the release-group label lookup route implementation out of `pmda.py`.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - Added `pmda_matching.musicbrainz_client_runtime`.
  - The module owns effective MusicBrainz target calculation, mirror health fallback, user-agent setup, and `musicbrainzngs` hostname configuration.
  - Added `pmda_library.release_group_runtime`.
  - The module owns `/api/library/release-group/<mbid>/labels` logic while preserving the public route via the existing blueprint/runtime wrapper.
  - `pmda.py` keeps thin compatibility wrappers for settings, statistics, MCP, and route callers.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires MusicBrainz client configuration to stay in `pmda_matching.musicbrainz_client_runtime`.
  - `scripts/pmda_bootstrap_gate.py` now requires release-group label lookup implementation to stay in `pmda_library.release_group_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `20,335`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_matching/musicbrainz_client_runtime.py pmda_library/release_group_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - MusicBrainz/settings, statistics blueprint, and MCP access focused tests passed: `40 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `20,323` lines, down `88,952` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Identity Hint Helper Extraction

- Scope:
  - Continued bootstrap reduction after MusicBrainz client/release-group route extraction.
  - This pass moved local identity fallback heuristics and filename-derived artist/album hint logic out of `pmda.py`.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - Added `pmda_matching.identity_hints_runtime`.
  - The module owns generic identity detection, container-folder detection, track-prefix artist cleanup, artist/album fallback usability, missing-required-tag extraction, verified-provider checks, identity-hint safety, resolved identity application, album-title inference from track filenames, and filename-pattern identity extraction.
  - `pmda.py` keeps thin compatibility wrappers for matching, improve, and existing tests.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires identity fallback and filename hint helpers to stay in `pmda_matching.identity_hints_runtime`.
  - The gate blocks filename-pattern implementation details from reappearing in `pmda.py`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `20,080`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_matching/identity_hints_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Provider identity arbitration, statistics blueprint, and MCP access focused tests passed: `66 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `20,069` lines, down `89,206` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Config And Number Parsing Helper Extraction

- Scope:
  - Continued bootstrap reduction after identity hint helper extraction.
  - This pass moved pure config scalar/list normalizers and loose numeric/duration/disc-track parsing out of `pmda.py`.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - Extended `pmda_core.config` with AI usage, web-search provider, scan AI policy, classical-name preference, and ordered-list normalization helpers.
  - Added `pmda_core.number_parsing`.
  - The module owns loose int/float parsing, clamped integer coercion, duration parsing, and disc/track parsing.
  - `pmda.py` keeps thin compatibility wrappers for existing runtime modules and tests.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires pure config scalar/list normalizers to stay in `pmda_core.config`.
  - `scripts/pmda_bootstrap_gate.py` now requires loose numeric/duration/disc-track parsing to stay in `pmda_core.number_parsing`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `19,935`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_core/config.py pmda_core/number_parsing.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Provider identity arbitration, publication regressions, statistics blueprint, and MCP access focused tests passed: `116 passed, 1 warning`.
- Current state:
  - `pmda.py` is at `19,922` lines, down `89,353` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Classical Person/Signal Helper Extraction

- Scope:
  - Continued bootstrap reduction after config and number parsing helper extraction.
  - This pass moved classical person alias/signature heuristics, classical signal detection, work-token parsing, title-composer preference, role-hint parsing, person-like detection, and classical gap-anomaly tolerance out of `pmda.py`.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - Extended `pmda_library.classical_runtime` with the extracted classical helpers.
  - `pmda.py` keeps thin compatibility wrappers for matching, publication, enrichment, and existing tests.
  - The runtime binder now protects the extracted helpers from being overwritten by live `pmda.py` globals.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires classical person aliases/signals and work-token helpers to stay in `pmda_library.classical_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `19,560`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_library/classical_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Classical/artist focused tests passed: `149 passed, 1 warning`.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
- Current state:
  - `pmda.py` is at `19,546` lines, down `89,729` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Files PostgreSQL Runtime Extraction

- Scope:
  - Continued bootstrap reduction after classical person/signal helper extraction.
  - This pass moved files-library PostgreSQL connection-pool side effects out of `pmda.py`.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during this checkpoint.
- Code extracted:
  - Added `pmda_core.files_pg_runtime`.
  - The module owns files PostgreSQL connection kwargs, connection registration/touch/release, stale idle reaping, connection proxy wrappers, connection acquisition, dropped-connection detection, and the connection context manager.
  - `pmda.py` keeps thin compatibility wrappers for existing runtime modules and tests that patch `pmda._files_pg_connect`.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires the files PostgreSQL connection pool to stay in `pmda_core.files_pg_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `19,295`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_core/files_pg_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Files publication/schema/library stats/dedupe focused tests passed: `104 passed, 2 warnings, 7 subtests passed`.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
- Current state:
  - `pmda.py` is at `19,280` lines, down `89,995` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - Full backend suite and frontend production build have not been rerun for this checkpoint yet; run them before Docker image publication or Unraid redeploy.

## 2026-05-18 Internal Refactor Checkpoint: Files Watcher Runtime Extraction

- Scope:
  - Continued bootstrap reduction after files PostgreSQL runtime extraction.
  - This pass moved filesystem watcher pending-change persistence, suppression, watchdog handler construction, restart/reconcile logic, and manager retry loop out of `pmda.py`.
  - No Docker image was built, no Unraid redeploy was performed, and no scan was launched or resumed during the extraction itself.
- Code extracted:
  - Added `pmda_discovery.files_watcher_runtime`.
  - The module owns `files_pending_changes` SQLite writes/reads, watcher suppression windows, ignored-path filtering, watcher state snapshots, watchdog event handling, observer restart, and reconcile retry backoff.
  - `pmda.py` keeps thin compatibility wrappers for existing callers and tests.
- Regression guards:
  - `scripts/pmda_bootstrap_gate.py` now requires filesystem watcher side effects to stay in `pmda_discovery.files_watcher_runtime`.
  - `scripts/pmda_bootstrap_gate.py` lowered the bootstrap line budget to `18,935`.
- Validation:
  - `python3 -m py_compile pmda.py pmda_discovery/files_watcher_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `pmda_bootstrap_gate.py`, `legacy_cleanup_gate.py`, `pipeline_audit_gate.py`.
  - Watcher/files focused tests passed: `62 passed, 2 warnings, 7 subtests passed`.
  - Full backend suite passed: `732 passed, 3 warnings, 7 subtests passed`.
  - Frontend production build passed: `npm run build`.
- Current state:
  - `pmda.py` is at `18,920` lines, down `90,355` lines from the start of the larger extraction run.
  - Direct Flask routes in `pmda.py`: `0`.
  - Direct blueprint registration in `pmda.py`: `0`.
  - This checkpoint is ready for Docker image publication and Unraid smoke validation.

## 2026-05-18 Publish Checkpoint: Runtime Refactor Beta/Latest And Unraid Deploy

- GitHub safety:
  - Backed up the pre-publish `origin/main` state to `backup/pre-refactor-publish-20260518-152628`.
  - Published the refactor branch `silk/pmda-runtime-refactor-20260518` at commit `c4a3ad66232863ea45761b6f89e93c10fa1d3528`.
  - Also backed up the refactor state before attempting to reconcile with remote main as `backup/refactor-before-origin-main-merge-20260518-154052`.
  - Important: `origin/main` is still `42` commits ahead of the local refactor branch lineage. A direct merge produced many conflicts across frontend/runtime files and was aborted; do not fast-forward or force-update `main` until that reconciliation is done deliberately.
- Validation before image publish:
  - `python3 -m py_compile pmda.py pmda_discovery/files_watcher_runtime.py scripts/pmda_bootstrap_gate.py` passed.
  - Static gates passed: `scripts/pmda_bootstrap_gate.py`, `scripts/legacy_cleanup_gate.py`, `scripts/pipeline_audit_gate.py`.
  - Watcher/files focused tests passed: `62 passed, 2 warnings, 7 subtests passed`.
  - Full backend suite passed: `732 passed, 3 warnings, 7 subtests passed`.
  - Frontend production build passed: `npm run build`.
- Docker publication:
  - Built with `scripts/build_with_classical_gate.sh --with-latest`.
  - Pushed `meaning/pmda:beta` and `meaning/pmda:latest`.
  - Published image digest: `meaning/pmda@sha256:5b8ec04a126f44c6f8738a1787f008ff358aca75f508a176f2cf7e6e11b35dbc`.
  - Local image id: `sha256:85907d824f2935f4d59d635342ae323866f41a326d0e0bc652372acc65b8a040`.
- Unraid deployment:
  - Recreated the Unraid `PMDA` container from `meaning/pmda:latest`, preserving the previous live container as `PMDA_pre_runtime_refactor_20260518-153543`.
  - Deployment backup directory: `/mnt/cache/appdata/PMDA/backups/runtime-refactor-20260518-153543`.
  - New container id prefix: `73d3ed2e979e`.
  - Existing mounts/env were preserved, including `/mnt:/host_mnt:ro`, `STORAGE_POWER_SAVER_ENABLED=true`, and the current files workflow roots.
- Unraid smoke validation:
  - Boot logs show files mode active, Plex DB source checks disabled, watcher manager started, MusicBrainz queue initialized, and files index ready.
  - Error scan since deploy found no `Traceback`, `Killed`, `statement timeout`, `OperationalError`, `RuntimeError`, or segmentation fault.
  - Authenticated API smoke:
    - `/api/library/files-index/status`: `200` in `0.006s`, `indexed_albums=65708`, `indexed_artists=49010`, `indexed_tracks=627512`.
    - `/api/library/albums?sort=recent&limit=96&offset=0&include_unmatched=1&scope=library`: `200`, `96` albums, `total=61096`, `has_more=true`, roughly `3.3-4.7s`.
    - Album pagination offsets `0`, `96`, `192` each returned `96` albums under the `12s` UI timeout.
    - `/api/library/artists?sort=alpha&limit=120&offset=0&include_unmatched=1&scope=library`: `200`, `120` artists, `total=39253`, `has_more=true`, roughly `0.5-0.6s`.
    - `/api/scan/progress` and `/api/progress` returned `200`; first uncached progress call was slow once, then repeated calls were sub-second.
    - `/api/runtime/managed/status?skip_candidates=true` returned `200` in `0.09s`; full runtime managed status is still slow (`~33s`) and should not be used as a frequent UI polling endpoint.
- Rollback:
  - If this image misbehaves on Unraid, restore the previous container with:
    - `docker stop PMDA`
    - `docker rm PMDA`
    - `docker rename PMDA_pre_runtime_refactor_20260518-153543 PMDA`
    - `docker start PMDA`

## 2026-05-18 Reconciliation Checkpoint: Refactor Rebased Onto Current `origin/main`

- Context:
  - The first runtime-refactor deployment was built from the refactor lineage before it was reconciled with the newest `origin/main`. This risked missing newer PMDA features and was replaced.
  - Work stopped on the deployed refactor branch, then a reconciliation branch was created: `silk/pmda-refactor-on-latest-20260518`.
  - The branch now contains the latest `origin/main` plus the runtime extraction/refactor work.
- GitHub safety:
  - Previous safety branches remain:
    - `backup/pre-refactor-publish-20260518-152628`
    - `backup/refactor-before-origin-main-merge-20260518-154052`
  - Current reconciliation branch pushed to GitHub:
    - `silk/pmda-refactor-on-latest-20260518`
  - Key commits on this branch:
    - `8d76084` Reconcile runtime refactor with latest main
    - `49a6511` Fix runtime image Codex CLI install
    - `df337d7` Use slim Node runtime for Codex CLI
- Validation before deploy:
  - Full backend suite passed: `732 passed, 3 warnings, 7 subtests passed`.
  - Frontend production build passed: `npm run build`.
  - Static gates passed:
    - `python3 scripts/pipeline_audit_gate.py`
    - `python3 scripts/legacy_cleanup_gate.py`
    - `python3 scripts/pmda_bootstrap_gate.py`
  - `git diff --check` passed.
  - `pmda.py` bootstrap gate state: `19,136` lines with no direct Flask routes.
- Docker publication:
  - Built with `scripts/build_with_classical_gate.sh --with-latest`.
  - Pushed both `meaning/pmda:beta` and `meaning/pmda:latest`.
  - Published digest for both tags:
    - `meaning/pmda@sha256:efddf956c60dad43572823e0a02d0a3ac7e3416d9d77110666ecfd4aedd36fe9`
  - The Dockerfile now copies Node from `node:20-bookworm-slim` and recreates npm/npx symlinks, avoiding the much heavier Debian `nodejs/npm` package tree while keeping `@openai/codex` available for PMDA's Codex CLI runtime.
- Unraid deployment:
  - Recreated the Unraid `PMDA` container from `meaning/pmda:latest`.
  - Preserved the previous live container as:
    - `PMDA_pre_reconciled_refactor_20260518-182638`
  - Inspect backup:
    - `/mnt/cache/appdata/PMDA/pmda-deploy-inspect-20260518-182638.json`
  - Rollback script:
    - `/mnt/cache/appdata/PMDA/pmda-rollback-20260518-182638.sh`
  - Confirmed deployed image digest:
    - `meaning/pmda@sha256:efddf956c60dad43572823e0a02d0a3ac7e3416d9d77110666ecfd4aedd36fe9`
- Unraid smoke validation:
  - Container is running and boot logs show files mode active, Plex DB source checks skipped, Ollama verified, watcher manager started, MusicBrainz queue initialized, and files index ready.
  - Error scan after deploy found no `Traceback`, `Killed`, `statement timeout`, `OperationalError`, `RuntimeError`, `Exception`, or segmentation fault.
  - Authenticated API smoke:
    - `/api/progress`: `200` in `0.129s`, `exports_so_far=65708`, `has_completed_full_scan=True`.
    - `/api/jobs/status`: `200` in `0.155s`.
    - `/api/library/files-index/status`: `200` in `0.007s`, `indexed_albums=65708`, `indexed_artists=49010`, `indexed_tracks=627512`.
    - `/api/library/albums?sort=recent&limit=96&offset=0&include_unmatched=1&scope=library`: `200` in `3.10s`, `96` albums.
    - `/api/library/artists?sort=alpha&limit=120&offset=0&include_unmatched=1&scope=library`: `200` in `0.585s`, `120` artists.
    - `/api/runtime/managed/status?skip_candidates=true`: `200` in `0.107s`.
    - Full `/api/runtime/managed/status` is still intentionally heavier at `18.2s`; UI polling should continue using `skip_candidates=true`.
- Rollback:
  - If this reconciled image misbehaves on Unraid:
    - `docker stop PMDA`
    - `docker rm PMDA`
    - `docker rename PMDA_pre_reconciled_refactor_20260518-182638 PMDA`
    - `docker start PMDA`
  - Or run:
    - `/mnt/cache/appdata/PMDA/pmda-rollback-20260518-182638.sh`
