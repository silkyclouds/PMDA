import unittest
import sys
from types import SimpleNamespace

sys.modules.setdefault(
    "musicbrainzngs",
    SimpleNamespace(
        set_rate_limit=lambda *args, **kwargs: None,
        set_useragent=lambda *args, **kwargs: None,
    ),
)

import pmda


class AlbumUserFeedbackTests(unittest.TestCase):
    def test_normalize_user_album_review_text_trims_and_normalizes_line_endings(self):
        raw = "  Great album.\r\nLove side B.  \r\n\r\n"
        self.assertEqual(
            pmda._normalize_user_album_review_text(raw),
            "Great album.\nLove side B.",
        )

    def test_merge_user_album_feedback_keeps_review_when_rating_is_cleared(self):
        next_rating, next_review, delete_row = pmda._merge_user_album_feedback(
            4,
            "Still a favorite.",
            rating=0,
        )
        self.assertEqual(next_rating, 0)
        self.assertEqual(next_review, "Still a favorite.")
        self.assertFalse(delete_row)

    def test_merge_user_album_feedback_deletes_row_only_when_both_rating_and_review_are_empty(self):
        next_rating, next_review, delete_row = pmda._merge_user_album_feedback(
            2,
            "Needs another listen.",
            rating=0,
            review_text="   ",
        )
        self.assertEqual(next_rating, 0)
        self.assertEqual(next_review, "")
        self.assertTrue(delete_row)

    def test_non_admin_write_allows_album_review_endpoint(self):
        self.assertTrue(pmda._auth_non_admin_write_allowed("/api/library/album/42/review", "PUT"))
        self.assertTrue(pmda._auth_non_admin_write_allowed("/api/library/album/42/rating", "PUT"))


if __name__ == "__main__":
    unittest.main()
