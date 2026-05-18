"""Flask API blueprint registry for PMDA.

Routes are moved here incrementally from the historical monolith. Public URLs
must remain unchanged while handlers become thin adapters over domain services.
"""

from __future__ import annotations

from typing import Any, Callable, Mapping

from pmda_api.admin_ops import create_admin_ops_blueprint
from pmda_api.assistant import create_assistant_blueprint
from pmda_api.auth_admin import create_auth_admin_blueprint
from pmda_api.broken_albums import create_broken_albums_blueprint
from pmda_api.dedupe_details import create_dedupe_details_blueprint
from pmda_api.files_cache import create_files_cache_blueprint
from pmda_api.files_export import create_files_export_blueprint
from pmda_api.files_sources import create_files_sources_blueprint
from pmda_api.frontend import create_frontend_blueprint
from pmda_api.incomplete_albums import create_incomplete_albums_blueprint
from pmda_api.legacy_acquisition import create_legacy_acquisition_blueprint
from pmda_api.library_browse import create_library_browse_blueprint
from pmda_api.library_catalog import create_library_catalog_blueprint
from pmda_api.library_detail import create_library_detail_blueprint
from pmda_api.library_improve import create_library_improve_blueprint
from pmda_api.library_index_control import create_library_index_control_blueprint
from pmda_api.library_index_status import create_library_index_status_blueprint
from pmda_api.library_normalization import create_library_normalization_blueprint
from pmda_api.library_personal import create_library_personal_blueprint
from pmda_api.library_stats import create_library_stats_blueprint
from pmda_api.logs import create_logs_blueprint
from pmda_api.mcp_admin import create_mcp_admin_blueprint
from pmda_api.player import create_player_blueprint
from pmda_api.profile_backfill import create_profile_backfill_blueprint
from pmda_api.progress import create_progress_blueprint
from pmda_api.publication_reconcile import create_publication_reconcile_blueprint
from pmda_api.runtime_ai import create_runtime_ai_blueprint
from pmda_api.scheduler import create_scheduler_blueprint
from pmda_api.scan_control import bind_scan_control_compat_aliases, create_scan_control_blueprint
from pmda_api.scan_history import create_scan_history_blueprint
from pmda_api.scan_moves import create_scan_moves_blueprint
from pmda_api.settings_config import create_settings_config_blueprint
from pmda_api.statistics import create_statistics_blueprint
from pmda_api.tools import create_tools_blueprint
from pmda_api.user_feedback import create_user_feedback_blueprint


def register_api_blueprints(
    app: Any,
    *,
    runtime: Any,
    log_routes: Mapping[str, Any],
    set_lidarr_progress: Callable[[list[tuple]], None],
    include_frontend: bool = False,
    frontend_dist: str = "",
) -> None:
    """Register all public HTTP routes while keeping pmda.py as bootstrap wiring."""

    app.register_blueprint(
        create_logs_blueprint(
            get_log_file=log_routes["get_log_file"],
            parse_bool=log_routes["parse_bool"],
            recent_log_tail_entries=log_routes["recent_log_tail_entries"],
            tail_log_entries=log_routes["tail_log_entries"],
            tail_log_lines=log_routes["tail_log_lines"],
        )
    )
    app.register_blueprint(create_legacy_acquisition_blueprint(set_lidarr_progress=set_lidarr_progress))
    app.register_blueprint(create_statistics_blueprint(runtime=runtime))
    app.register_blueprint(create_files_cache_blueprint(runtime=runtime))
    app.register_blueprint(create_files_export_blueprint(runtime=runtime))
    app.register_blueprint(create_files_sources_blueprint(runtime=runtime))
    app.register_blueprint(create_library_browse_blueprint(runtime=runtime))
    app.register_blueprint(create_library_catalog_blueprint(runtime=runtime))
    app.register_blueprint(create_library_detail_blueprint(runtime=runtime))
    app.register_blueprint(create_library_improve_blueprint(runtime=runtime))
    app.register_blueprint(create_library_personal_blueprint(runtime=runtime))
    app.register_blueprint(create_library_stats_blueprint(runtime=runtime))
    app.register_blueprint(create_library_normalization_blueprint(runtime=runtime))
    app.register_blueprint(create_library_index_control_blueprint(runtime=runtime))
    app.register_blueprint(create_library_index_status_blueprint(runtime=runtime))
    app.register_blueprint(create_profile_backfill_blueprint(runtime=runtime))
    app.register_blueprint(create_progress_blueprint(runtime=runtime))
    app.register_blueprint(create_publication_reconcile_blueprint(runtime=runtime))
    app.register_blueprint(create_runtime_ai_blueprint(runtime=runtime))
    app.register_blueprint(create_scheduler_blueprint(runtime=runtime))
    app.register_blueprint(create_scan_control_blueprint(runtime=runtime))
    app.register_blueprint(create_scan_history_blueprint(runtime=runtime))
    app.register_blueprint(create_scan_moves_blueprint(runtime=runtime))
    app.register_blueprint(create_settings_config_blueprint(runtime=runtime))
    app.register_blueprint(create_tools_blueprint(runtime=runtime))
    app.register_blueprint(create_incomplete_albums_blueprint(runtime=runtime))
    app.register_blueprint(create_user_feedback_blueprint(runtime=runtime))
    app.register_blueprint(create_player_blueprint(runtime=runtime))
    app.register_blueprint(create_admin_ops_blueprint(runtime=runtime))
    app.register_blueprint(create_assistant_blueprint(runtime=runtime))
    app.register_blueprint(create_mcp_admin_blueprint(runtime=runtime))
    app.register_blueprint(create_auth_admin_blueprint(runtime=runtime))
    app.register_blueprint(create_broken_albums_blueprint(runtime=runtime))
    app.register_blueprint(create_dedupe_details_blueprint(runtime=runtime))
    if include_frontend:
        app.register_blueprint(create_frontend_blueprint(runtime=runtime, frontend_dist=frontend_dist))
    bind_scan_control_compat_aliases(runtime=runtime, app=app)
