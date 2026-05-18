from __future__ import annotations

import errno
import os

from pmda_materialization.mover import safe_move


def test_safe_move_falls_back_to_copy_for_cross_device_file(tmp_path, monkeypatch):
    src = tmp_path / "source.txt"
    dst = tmp_path / "nested" / "target.txt"
    src.write_text("payload", encoding="utf-8")

    def fail_replace(_src, _dst):
        raise OSError(errno.EXDEV, "cross-device link")

    monkeypatch.setattr(os, "replace", fail_replace)

    safe_move(str(src), str(dst))

    assert not src.exists()
    assert dst.read_text(encoding="utf-8") == "payload"


def test_safe_move_uses_non_clobbering_destination_on_fallback(tmp_path, monkeypatch):
    src = tmp_path / "album"
    src.mkdir()
    (src / "track.flac").write_text("audio", encoding="utf-8")
    dst = tmp_path / "published"
    dst.mkdir()
    (dst / "existing.flac").write_text("existing", encoding="utf-8")

    def fail_replace(_src, _dst):
        raise OSError(errno.EXDEV, "cross-device link")

    monkeypatch.setattr(os, "replace", fail_replace)

    safe_move(str(src), str(dst))

    fallback = tmp_path / "published (1)"
    assert not src.exists()
    assert (dst / "existing.flac").read_text(encoding="utf-8") == "existing"
    assert (fallback / "track.flac").read_text(encoding="utf-8") == "audio"
