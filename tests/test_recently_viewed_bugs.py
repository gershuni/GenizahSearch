# -*- coding: utf-8 -*-
"""Regression tests for the four "Recently Viewed" defects.

W1 (web): system-list display name leaked English under a Hebrew UI.
W2 (web): recent system list selected by its numeric Supabase id read the empty
          list_items table instead of recent_items.
D1 (desktop): lists tab re-sorted the recent list by shelfmark, destroying recency.
D2 (desktop): same manuscript with no distinguishing image appeared as true dupes.
"""

from unittest.mock import patch

import pytest


# --------------------------------------------------------------------------- #
# W2 — web recent system list routed to recent_items by numeric id
# --------------------------------------------------------------------------- #

def _make_mgr(lists_data):
    """Build a UserListsManager whose _get_cached_data returns lists_data."""
    from web.user_lists import UserListsManager
    mgr = UserListsManager()
    mgr._get_cached_data = lambda: {'lists': lists_data}
    return mgr


def test_w2_recent_system_list_read_by_numeric_id_routes_to_recent_items():
    """Selecting the recent system list by its numeric DB id must read recent_items,
    not get_list_items(id) on the empty list_items table."""
    lists = {
        '42': {'name': 'Recently Viewed', 'name_en': 'Recently Viewed', 'is_system': True},
        '7': {'name': 'My Joins', 'name_en': 'My Joins', 'is_system': False},
    }
    mgr = _make_mgr(lists)

    recent_rows = [
        {'sys_id': '111', 'shelfmark': 'T-S 1.1', 'title': 'A', 'fl_id': ''},
        {'sys_id': '222', 'shelfmark': 'T-S 2.2', 'title': 'B', 'fl_id': ''},
    ]

    with patch.object(type(mgr), 'is_authenticated', property(lambda self: True)), \
         patch.object(type(mgr), 'user_id', property(lambda self: 'u1')), \
         patch('web.user_lists.get_recent_items', return_value=recent_rows) as m_recent, \
         patch('web.user_lists.get_list_items', return_value=[]) as m_list_items:
        items = mgr.get_items_in_list_sync('42')  # numeric id of the recent system list

    m_recent.assert_called_once_with('u1')
    m_list_items.assert_not_called()
    assert [it['sys_id'] for it in items] == ['111', '222']


def test_w2_literal_recent_still_works():
    """The literal 'recent' sentinel (add-to-joins / comment tabs) must still route
    to recent_items."""
    mgr = _make_mgr({})
    with patch.object(type(mgr), 'is_authenticated', property(lambda self: True)), \
         patch.object(type(mgr), 'user_id', property(lambda self: 'u1')), \
         patch('web.user_lists.get_recent_items', return_value=[{'sys_id': '9'}]) as m_recent:
        items = mgr.get_items_in_list_sync('recent')
    m_recent.assert_called_once_with('u1')
    assert items[0]['sys_id'] == '9'


def test_w2_regular_user_list_unaffected():
    """A non-system list keyed by numeric id still reads list_items."""
    lists = {'7': {'name': 'My Joins', 'name_en': 'My Joins', 'is_system': False}}
    mgr = _make_mgr(lists)
    with patch.object(type(mgr), 'is_authenticated', property(lambda self: True)), \
         patch.object(type(mgr), 'user_id', property(lambda self: 'u1')), \
         patch('web.user_lists.get_recent_items', return_value=[]) as m_recent, \
         patch('web.user_lists.get_list_items', return_value=[{'sys_id': '5'}]) as m_list_items:
        items = mgr.get_items_in_list_sync('7')
    m_recent.assert_not_called()
    m_list_items.assert_called_once_with(7)
    assert items[0]['sys_id'] == '5'


def test_w2_recent_count_uses_recent_items():
    """The sidebar count badge for the recent system list reflects recent_items."""
    lists = {'42': {'name': 'Recently Viewed', 'name_en': 'Recently Viewed', 'is_system': True}}
    mgr = _make_mgr(lists)
    with patch.object(type(mgr), 'is_authenticated', property(lambda self: True)), \
         patch.object(type(mgr), 'user_id', property(lambda self: 'u1')), \
         patch('web.user_lists.get_recent_items', return_value=[{}, {}, {}]), \
         patch('web.user_lists.get_list_items', return_value=[]) as m_list_items:
        count = mgr._get_list_item_count('42')
    assert count == 3
    m_list_items.assert_not_called()


# --------------------------------------------------------------------------- #
# W1 — system-list name is translatable (key exists in the shared TRANSLATIONS)
# --------------------------------------------------------------------------- #

def test_w1_recently_viewed_translation_key_exists():
    from genizah_translations import TRANSLATIONS
    assert TRANSLATIONS.get('Recently Viewed') == 'נצפו לאחרונה'


def test_w1_web_tr_localizes_recently_viewed_under_hebrew():
    from web import translations as web_tr
    prev = web_tr.get_language()
    try:
        web_tr.set_language('he')
        assert web_tr.tr('Recently Viewed') == 'נצפו לאחרונה'
        web_tr.set_language('en')
        assert web_tr.tr('Recently Viewed') == 'Recently Viewed'
    finally:
        web_tr.set_language(prev)


def test_w1_joins_lab_recent_detector():
    from web.pages.joins_lab import _is_recent_system_list
    assert _is_recent_system_list({'is_system': True, 'name_en': 'Recently Viewed'})
    assert _is_recent_system_list({'is_system': True, 'name': 'Recently Viewed'})
    assert not _is_recent_system_list({'is_system': False, 'name_en': 'Recently Viewed'})
    assert not _is_recent_system_list({'is_system': True, 'name_en': 'My Joins'})
    assert not _is_recent_system_list(None)


# --------------------------------------------------------------------------- #
# W3 (ROUND 2) — count badge routes through recent_items, not the batched RPC
# --------------------------------------------------------------------------- #

def test_w3_resolve_count_recent_list_ignores_stale_batched_zero():
    """The recent list must resolve via _get_list_item_count (recent_items) EVEN
    when a batched counts dict is present. The batched RPC counts list_items rows,
    where the recent list has zero — it is absent from the dict, so the old
    counts.get(id, 0) returned a stale 0 ("(0)" badge on a full list)."""
    from web.components.project_tree import _resolve_list_item_count

    class _Mgr:
        def _is_recent_list(self, list_id):
            return str(list_id) == '42'

        def _get_list_item_count(self, list_id):
            return 7  # authoritative recent_items count

    mgr = _Mgr()
    # Batched counts dict present and does NOT contain the recent list (id 42).
    counts = {7: 3}  # some other list
    assert _resolve_list_item_count('42', mgr, counts) == 7


def test_w3_resolve_count_non_recent_still_uses_batched_dict():
    """Non-recent lists keep using the batched dict (no extra per-list fetch)."""
    from web.components.project_tree import _resolve_list_item_count

    class _Mgr:
        def _is_recent_list(self, list_id):
            return False

        def _get_list_item_count(self, list_id):  # pragma: no cover - must not be called
            raise AssertionError("should not fall back for a batched, non-recent list")

    assert _resolve_list_item_count('7', _Mgr(), {7: 5}) == 5


def test_w3_resolve_count_legacy_path_recent_still_routes():
    """When counts is None (legacy fallback), the recent list still routes through
    the recent-aware _get_list_item_count."""
    from web.components.project_tree import _resolve_list_item_count

    class _Mgr:
        def _is_recent_list(self, list_id):
            return True

        def _get_list_item_count(self, list_id):
            return 9

    assert _resolve_list_item_count('42', _Mgr(), None) == 9


# --------------------------------------------------------------------------- #
# W4 (ROUND 2) — system AND default list names localized generically
# --------------------------------------------------------------------------- #

def test_w4_general_translation_key_exists():
    from genizah_translations import TRANSLATIONS
    assert TRANSLATIONS.get('General') == 'כללי'


def test_w4_localize_list_name_translates_default_and_system():
    """localize_list_name() translates the default ('General') AND system lists,
    mirroring the desktop's is_system-OR-is_default rule. User-created lists are
    never translated."""
    from web import translations as web_tr
    from web.user_lists import localize_list_name

    prev = web_tr.get_language()
    try:
        web_tr.set_language('he')
        # Default list "General" -> כללי (W4: was leaking English).
        assert localize_list_name({'name': 'General', 'is_default': True}) == 'כללי'
        # System list "Recently Viewed" -> נצפו לאחרונה (W1 class).
        assert localize_list_name(
            {'name': 'Recently Viewed', 'is_system': True}
        ) == 'נצפו לאחרונה'
        # A user-created list literally named "General" is NOT translated.
        assert localize_list_name(
            {'name': 'General', 'is_default': False, 'is_system': False}
        ) == 'General'
        # An arbitrary user list name passes through unchanged.
        assert localize_list_name({'name': 'My Joins'}) == 'My Joins'

        web_tr.set_language('en')
        # Under English UI the canonical English name shows as-is.
        assert localize_list_name({'name': 'General', 'is_default': True}) == 'General'
    finally:
        web_tr.set_language(prev)


def test_w4_localize_list_name_falls_back_to_name_en():
    from web.user_lists import localize_list_name
    # No 'name' but a 'name_en' present.
    assert localize_list_name({'name_en': 'My Joins'}) == 'My Joins'
    # Nothing at all -> empty string (no crash).
    assert localize_list_name({}) == ''


# --------------------------------------------------------------------------- #
# D1 / D2 — desktop recency order preserved + true duplicates collapsed
# --------------------------------------------------------------------------- #

def _dedup_recent(items):
    """Mirror of GenizahGUI._get_recent_items_deduped's collapse logic so it can be
    unit-tested without Qt. Items are already most-recent-first."""
    seen = set()
    out = []
    for item in items:
        img = item.get('img')
        img_key = img if img not in (None, "") else None
        key = (item.get('sys_id'), img_key)
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def test_d1_recency_order_preserved_no_shelfmark_sort():
    """Most-recent-first order must survive (not be re-sorted by shelfmark)."""
    from genizah_core import ListsManager
    mgr = ListsManager.__new__(ListsManager)
    mgr.MAX_RECENT_ITEMS = 50
    mgr.data = {'items': {}, 'recent_items': []}
    mgr.save = lambda: None

    # View order: ZZZ (oldest) then AAA (newest). Shelfmark sort would flip them.
    mgr.add_to_recent('200')  # ZZZ-library
    mgr.add_to_recent('100')  # AAA-library (most recent)

    items = mgr.get_items_in_list('recent')
    assert [it['sys_id'] for it in items] == ['100', '200']  # newest first, NOT sorted


def test_d2_true_duplicates_collapse_keep_most_recent():
    """Same sys_id, no distinguishing image (differ only by fl_id) -> one row,
    most-recent kept. Distinct images stay as separate rows."""
    from genizah_core import ListsManager
    mgr = ListsManager.__new__(ListsManager)
    mgr.MAX_RECENT_ITEMS = 50
    mgr.data = {'items': {}, 'recent_items': []}
    mgr.save = lambda: None

    # Same manuscript viewed via three different fl_ids (no img) = true dupes.
    mgr.add_to_recent('3207', fl_id='1r')
    mgr.add_to_recent('3207', fl_id='2r')
    mgr.add_to_recent('3207', fl_id='3r')  # most recent of the dupes
    # A genuinely different image variant — must be kept.
    mgr.add_to_recent('3207', img='special_image')
    # An unrelated manuscript.
    mgr.add_to_recent('9999')

    items = mgr.get_items_in_list('recent')
    deduped = _dedup_recent(items)

    sys_ids = [it['sys_id'] for it in deduped]
    # 9999 (newest) first; then the image-variant 3207; then ONE collapsed 3207 (no img)
    assert sys_ids.count('9999') == 1
    # 3207 appears exactly twice: the distinct image variant + one collapsed no-img row
    assert sys_ids.count('3207') == 2
    # Recency preserved: 9999 is the most recent overall
    assert sys_ids[0] == '9999'

    # The collapsed no-img 3207 keeps the most-recent (fl_id='3r').
    no_img_3207 = [it for it in deduped if it['sys_id'] == '3207' and not it.get('img')]
    assert len(no_img_3207) == 1
    assert no_img_3207[0].get('fl_id') == '3r'


def test_d2_distinct_images_not_collapsed():
    from genizah_core import ListsManager
    mgr = ListsManager.__new__(ListsManager)
    mgr.MAX_RECENT_ITEMS = 50
    mgr.data = {'items': {}, 'recent_items': []}
    mgr.save = lambda: None

    mgr.add_to_recent('500', img='imgA')
    mgr.add_to_recent('500', img='imgB')

    deduped = _dedup_recent(mgr.get_items_in_list('recent'))
    imgs = sorted(it.get('img') for it in deduped if it['sys_id'] == '500')
    assert imgs == ['imgA', 'imgB']


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
