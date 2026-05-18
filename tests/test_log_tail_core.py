from pathlib import Path

from pmda_core.log_tail import LogTailParser, stable_log_thread_slot, tail_log_lines


def _classifier(level: str, message: str) -> tuple[str, str]:
    lowered = f"{level} {message}".lower()
    if "accepted" in lowered:
        return "match", "V"
    if "warning" in lowered:
        return "warning", "!"
    if "no match" in lowered:
        return "miss", "X"
    return "info", "."


def test_tail_log_lines_strips_ansi_and_limits(tmp_path: Path):
    log_path = tmp_path / "pmda.log"
    log_path.write_text("\x1b[31mone\x1b[0m\ntwo\nthree\n", encoding="utf-8")

    assert tail_log_lines(log_path, lines=2) == ["two", "three"]


def test_log_tail_parser_keeps_structured_fields():
    parser = LogTailParser(classify_message=_classifier)
    entries = parser.entries_from_lines(
        [
            "04:44:14 │ INFO │ worker 2 │ [MB] Album accepted",
            "unstructured warning line",
        ]
    )

    assert entries[0]["timestamp"] == "04:44:14"
    assert entries[0]["level"] == "INFO"
    assert entries[0]["thread"] == "worker 2"
    assert entries[0]["thread_slot"] == 2
    assert entries[0]["kind"] == "match"
    assert entries[1]["timestamp"] == ""
    assert entries[1]["kind"] == "warning"


def test_log_tail_scan_relevance_filters_postgres_noise():
    parser = LogTailParser(classify_message=_classifier)
    scan_entry = parser.entries_from_lines(["04:44:14 │ INFO │ scan:full │ [SCAN] Heartbeat"])[0]
    checkpoint = parser.entries_from_lines(["04:44:14 │ INFO │ postgres │ checkpoint starting: time"])[0]

    assert parser.entry_is_scan_relevant(scan_entry)
    assert not parser.entry_is_scan_relevant(checkpoint)


def test_stable_log_thread_slot_is_deterministic():
    assert stable_log_thread_slot("worker 7") == 7
    assert stable_log_thread_slot("profile-enrich-artist") == stable_log_thread_slot("profile-enrich-artist")
