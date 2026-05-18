import sys
import types
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

sys.modules.setdefault(
    "musicbrainzngs",
    types.SimpleNamespace(
        set_rate_limit=lambda *args, **kwargs: None,
        set_useragent=lambda *args, **kwargs: None,
    ),
)

import pmda


class ReleaseSegmentRegressionTests(unittest.TestCase):
    def _build_segmented_release(self, tmpdir: str) -> tuple[Path, dict[Path, list[Path]]]:
        root = Path(tmpdir) / "incoming"
        root.mkdir(parents=True, exist_ok=True)
        parent = root / "(1978) Symphonies 32, 35, 36, 38, 39, 40, 41 (Karajan)"
        parent.mkdir(parents=True, exist_ok=True)
        (parent / "cover.jpg").write_bytes(b"cover")
        by_folder: dict[Path, list[Path]] = {}
        for name in ("Symphony No32", "Symphony No35", "Symphony No38"):
            child = parent / name
            child.mkdir(parents=True, exist_ok=True)
            files: list[Path] = []
            for index in (1, 2):
                track = child / f"{index:02d} - Movement {index}.flac"
                track.write_bytes(b"flac")
                files.append(track)
            by_folder[child] = files
        return root, by_folder

    def _build_multi_disc_release(self, tmpdir: str) -> tuple[Path, dict[Path, list[Path]]]:
        root = Path(tmpdir) / "incoming"
        root.mkdir(parents=True, exist_ok=True)
        parent = root / "Massive Box Set"
        parent.mkdir(parents=True, exist_ok=True)
        (parent / "folder.jpg").write_bytes(b"cover")
        by_folder: dict[Path, list[Path]] = {}
        for disc_name in ("CD1", "CD2"):
            child = parent / disc_name
            child.mkdir(parents=True, exist_ok=True)
            files: list[Path] = []
            for index in (1, 2):
                track = child / f"{index:02d} - Track {index}.flac"
                track.write_bytes(b"flac")
                files.append(track)
            by_folder[child] = files
        return root, by_folder

    def test_collapse_nested_album_folder_groups_collapses_segment_children(self):
        with TemporaryDirectory() as tmpdir:
            root, by_folder = self._build_segmented_release(tmpdir)

            def _fake_tags(path: Path) -> dict:
                child_name = Path(path).parent.name
                return {
                    "albumartist": "Mozart",
                    "album": child_name,
                    "title": Path(path).stem,
                }

            with mock.patch.object(pmda, "extract_tags", side_effect=_fake_tags):
                collapsed = pmda._collapse_nested_album_folder_groups(
                    by_folder,
                    root_dirs={str(root.resolve())},
                )

            self.assertEqual(len(collapsed), 1)
            parent_folder = next(iter(collapsed.keys()))
            self.assertEqual(parent_folder.name, "(1978) Symphonies 32, 35, 36, 38, 39, 40, 41 (Karajan)")
            self.assertEqual(len(collapsed[parent_folder]), 6)

    def test_infer_artist_album_from_folder_keeps_parent_release_title(self):
        with TemporaryDirectory() as tmpdir:
            _root, by_folder = self._build_segmented_release(tmpdir)
            parent = next(iter(by_folder.keys())).parent
            audio_files = [path for paths in by_folder.values() for path in paths]

            with mock.patch.object(
                pmda,
                "extract_tags",
                return_value={"albumartist": "Mozart", "title": "Allegro spiritoso-Andante-Tempo primo"},
            ):
                artist_name, album_title = pmda._infer_artist_album_from_folder(parent, audio_files)

            self.assertEqual(artist_name, "Mozart")
            self.assertEqual(album_title, "(1978) Symphonies 32, 35, 36, 38, 39, 40, 41 (Karajan)")

    def test_collapse_nested_album_folder_groups_collapses_multi_disc_children(self):
        with TemporaryDirectory() as tmpdir:
            root, by_folder = self._build_multi_disc_release(tmpdir)

            def _fake_tags(path: Path) -> dict:
                child_name = Path(path).parent.name
                return {
                    "albumartist": "Test Artist",
                    "album": child_name,
                    "title": Path(path).stem,
                }

            with mock.patch.object(pmda, "extract_tags", side_effect=_fake_tags):
                collapsed = pmda._collapse_nested_album_folder_groups(
                    by_folder,
                    root_dirs={str(root.resolve())},
                )

            self.assertEqual(len(collapsed), 1)
            parent_folder = next(iter(collapsed.keys()))
            self.assertEqual(parent_folder.name, "Massive Box Set")
            self.assertEqual(len(collapsed[parent_folder]), 4)

    def test_detect_broken_album_ignores_release_segment_child_folder(self):
        with TemporaryDirectory() as tmpdir:
            _root, by_folder = self._build_segmented_release(tmpdir)
            segment_folder = next(iter(by_folder.keys()))
            tracks = [pmda.Track(title="movement", idx=1, disc=1, dur=60_000)]

            is_broken, expected, actual, missing = pmda.detect_broken_album(
                None,
                album_id=1,
                tracks=tracks,
                mb_release_group_info={"track_count": 5, "source": "provider_tracklist"},
                tags={"albumartist": "Mozart"},
                folder_path=segment_folder,
                album_title="Symphony No32",
            )

            self.assertFalse(is_broken)
            self.assertIsNone(expected)
            self.assertEqual(actual, 1)
            self.assertEqual(missing, [])

    def test_detect_broken_album_allows_small_tail_delta_from_provider_tracklist(self):
        tracks = [
            pmda.Track(title=f"Track {index}", idx=index, disc=1, dur=60_000)
            for index in range(1, 9)
        ]

        is_broken, expected, actual, missing = pmda.detect_broken_album(
            None,
            album_id=1,
            tracks=tracks,
            mb_release_group_info={"track_count": 9, "source": "provider_tracklist"},
            tags={"albumartist": "Test Artist", "album": "Likely Alternate Edition"},
            folder_path="/music/incomming/Test Artist/Likely Alternate Edition",
            album_title="Likely Alternate Edition",
        )

        self.assertFalse(is_broken)
        self.assertIsNone(expected)
        self.assertEqual(actual, 8)
        self.assertEqual(missing, [])


if __name__ == "__main__":
    unittest.main()
