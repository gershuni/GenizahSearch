# -*- coding: utf-8 -*-
"""The canonical-work title classifier.

Every title string below is a real catalogue title from the 44 graded cards of
`deck_ref_wider_v1`, kept verbatim so the test pins the classifier against the
evidence that produced it rather than against invented examples.
"""
from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from shared.canonical_works import (  # noqa: E402
    is_canonical_sys_id, is_canonical_title, partition_rows,
)

# graded `canonical` -- the returned manuscript IS a canonical work
CANONICAL = [
    'תלמוד בבלי [טקסט]. ; Talmud Bavli: Gittin ; תלמוד בבלי: גיטין',
    'משנה [טקסט];תלמוד בבלי [טקסט]. ; Talmud Bavli: Ta\'anit 25 b – 27 a',
    'תורה.',
    'תורה (שמות, קטעים).',
    'משנה סדר טהרות (בלתי שלם).',
    'משנה סדר נשים (יבמות וכתובות, קטעים).',
    'הלכות גדולות.',
    'הלכות הרי"ף (שבת, קטע).',
    'משנה תורה (ספר שופטים).',
    'פרוש המשנה לרמב"ם בערבית (יבמות, קטע).',
    'נביאים (יהושע) וכתובים (קטעים).',
    'מקרא;מקרא [טקסט]. ; Bible: Daniel 9:24 – 11:6',
]

# graded `same_text` -- genuine parallels, must NOT be demoted
GENUINE = [
    'Common Prayers: Weekday Amidah ; תפילות קבע: חול עמידה',
    'ערוך (קטעים).',
    'הגדה של פסח;פיוט;תוספות של סידור;תפילה וברכות;תפילות קבע. ; Piyyut',
    'פיוט. ; Piyyut',
    'דרשות;ספרות הלכתית ופרשנות תלמודית;ספרות חז"ל.',
    'Liturgical additions:; Other ; תוספות של סידור',
    'פיוט;תחום לא מזוהה. ; Piyyut',
]

# graded `canonical` but NOT title-detectable -- the eight the rule misses.
# Pinned so a future widening of the pattern has to face them deliberately.
MISSED = [
    '',
    'כתאב אלשראיע (ח"ג) : ; בערבית יהודית.',
    'ספר השרשים (קטע).',
    'קבץ הלכות גאונים (שבת, קטעים).',
    'קובץ.',
    'עניני ירושה ושטרות.',
]


def test_canonical_titles_are_recognised():
    missed = [t for t in CANONICAL if not is_canonical_title(t)]
    assert not missed, f'failed to recognise: {missed}'


def test_genuine_parallel_titles_are_not_demoted():
    wrong = [t for t in GENUINE if is_canonical_title(t)]
    assert not wrong, f'wrongly demoted: {wrong}'


def test_the_known_misses_stay_missed_deliberately():
    # Not a wish -- a record. Widening the pattern to catch these would have
    # to be justified against the genuine-loss side.
    assert not any(is_canonical_title(t) for t in MISSED)


def test_empty_and_missing_titles_are_never_canonical():
    # 24.8% of the corpus is untitled; demoting on an empty string would hide
    # ~63,000 manuscripts on no evidence at all.
    for t in ('', '   ', None):
        assert is_canonical_title(t) is False


def test_two_genuine_losses_are_accepted_and_pinned():
    # Measured, irreducible for a title rule: these manuscripts really are a
    # Prophets and a Bavli codex that really did carry the searched text.
    # The feature must therefore DEMOTE, never delete.
    assert is_canonical_title('נביאים (ישעיה-יחזקאל).') is True
    assert is_canonical_title(
        'תלמוד בבלי סדר מועד (ערובין, יומא, חולין, קטעים).') is True


class _Meta:
    def __init__(self, mapping, raises=False):
        self.mapping = mapping
        self.raises = raises

    def get_meta_for_id(self, sys_id):
        if self.raises:
            raise RuntimeError('catalogue unavailable')
        return ('SHELF', self.mapping.get(sys_id, ''))


def test_sys_id_lookup_uses_the_shared_metadata_manager():
    mm = _Meta({'111': 'תורה.', '222': 'פיוט. ; Piyyut'})
    assert is_canonical_sys_id('111', mm) is True
    assert is_canonical_sys_id('222', mm) is False


def test_a_lookup_failure_fails_toward_showing_the_row():
    # This feature HIDES results, so a false positive costs a find the user
    # never learns existed. Errors must not demote.
    assert is_canonical_sys_id('111', _Meta({}, raises=True)) is False
    assert is_canonical_sys_id('111', None) is False
    assert is_canonical_sys_id('', _Meta({})) is False


def test_partition_splits_without_losing_rows():
    mm = _Meta({'111': 'תלמוד בבלי [טקסט].', '222': 'פיוט.'})
    rows = [{'raw_header': '111_IE1_P000001_FL1'},
            {'raw_header': '222_IE2_P000002_FL2'},
            {'uid': '222_IE3_P000003_FL3'}]
    kept, demoted = partition_rows(rows, mm)
    assert len(kept) == 2 and len(demoted) == 1
    assert len(kept) + len(demoted) == len(rows), 'a row was lost'
    assert demoted[0]['raw_header'].startswith('111')


# --- service integration ----------------------------------------------------

def test_the_service_option_is_off_by_default_and_demotes_when_on():
    """`hide_canonical` must change nothing unless explicitly requested, and
    when requested must MOVE rows rather than drop them."""
    import asyncio

    from shared import parallels_service as ps

    rows = [
        {'uid': 'a', 'raw_header': '111_IE1_P000001_FL1', 'src_lbl': '',
         'source_ctx': '', 'text': 't', 'score': 9, 'final_score': 9,
         'chunk_count': 1, 'chunk_hits': []},
        {'uid': 'b', 'raw_header': '222_IE2_P000002_FL2', 'src_lbl': '',
         'source_ctx': '', 'text': 't', 'score': 8, 'final_score': 8,
         'chunk_count': 1, 'chunk_hits': []},
    ]

    class _Searcher:
        def search_composition_logic(self, *a, **k):
            return {'main': list(rows), 'filtered': []}

    class _MM:
        def get_meta_for_id(self, sys_id):
            return ('S', 'תלמוד בבלי [טקסט].' if sys_id == '111' else 'פיוט.')

        def parse_full_id_components(self, uid):
            return None

    def run(**kw):
        return asyncio.run(ps.fetch_parallels_results(
            searcher=_Searcher(), meta_mgr=_MM(), text='irrelevant',
            chunk_size=3, mode='exact', **kw))

    off = run()
    assert len(off.main_results) == 2, 'default must not hide anything'
    assert off.canonical_hidden == 0

    on = run(hide_canonical=True)
    assert len(on.main_results) == 1
    assert on.main_results[0]['raw_header'].startswith('222')
    assert on.canonical_hidden == 1
    # moved, not deleted -- the rule loses genuine finds and the user must
    # be able to reach them
    assert any(r['raw_header'].startswith('111')
               for r in on.filtered_results), 'demoted row was lost'
