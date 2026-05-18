from pmda_core.pagination import page_has_more


def test_page_has_more_uses_returned_rows_not_requested_limit():
    assert page_has_more(total=250, offset=96, returned=96) is True
    assert page_has_more(total=192, offset=96, returned=96) is False
    assert page_has_more(total=250, offset=192, returned=12) is True


def test_page_has_more_is_safe_for_empty_or_bad_inputs():
    assert page_has_more(total=0, offset=0, returned=0) is False
    assert page_has_more(total=None, offset=None, returned=None) is False
    assert page_has_more(total="bad", offset=0, returned=96) is False
