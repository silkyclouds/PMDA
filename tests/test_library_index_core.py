from pmda_core import library_index


def test_merge_index_state_resets_eta_when_phase_changes():
    current = {
        "phase": "discovering",
        "phase_progress": 50.0,
        "phase_eta_seconds": 120,
        "phase_rate_per_sec": 5.0,
    }
    merged = library_index.merge_index_state(current, {"phase": "collapsing"}, now=100.0)
    assert merged["phase"] == "collapsing"
    assert merged["phase_started_at"] == 100.0
    assert merged["updated_at"] == 100.0
    assert merged["phase_progress"] is None
    assert merged["phase_eta_seconds"] is None
    assert merged["phase_rate_per_sec"] is None


def test_merge_index_state_keeps_metrics_with_same_phase():
    current = {"phase": "parsing", "phase_eta_seconds": 120}
    merged = library_index.merge_index_state(current, {"phase_progress": 25.0}, now=200.0)
    assert merged["phase"] == "parsing"
    assert merged["phase_progress"] == 25.0
    assert merged["phase_eta_seconds"] == 120
    assert merged["updated_at"] == 200.0


def test_index_running_phase_filter():
    state = {"running": True, "phase": "media_cache"}
    assert library_index.index_is_running(state)
    assert library_index.index_is_running(state, phases={"media_cache"})
    assert not library_index.index_is_running(state, phases={"artist_enrichment"})
    assert not library_index.index_is_running({"running": False, "phase": "media_cache"})


def test_progress_metrics_with_eta():
    progress, eta, rate = library_index.progress_metrics(50, 100, started_at=100.0, now=110.0)
    assert progress == 50.0
    assert eta == 10
    assert rate == 5.0


def test_progress_metrics_handles_terminal_or_unknown_totals():
    assert library_index.progress_metrics(0, 0, started_at=100.0, now=110.0) == (None, None, None)
    assert library_index.progress_metrics(100, 100, started_at=100.0, now=110.0) == (100.0, None, None)


def test_status_payload_adds_counts_and_embeddings():
    payload = library_index.status_payload(
        {"running": True, "phase": "parsing"},
        indexed_artists=1,
        indexed_albums=2,
        indexed_tracks=3,
        reco_embeddings={"running": False},
    )
    assert payload["running"] is True
    assert payload["phase"] == "parsing"
    assert payload["indexed_artists"] == 1
    assert payload["indexed_albums"] == 2
    assert payload["indexed_tracks"] == 3
    assert payload["reco_embeddings"] == {"running": False}
