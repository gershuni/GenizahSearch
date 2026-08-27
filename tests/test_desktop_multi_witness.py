# -*- coding: utf-8 -*-
"""Desktop multi-witness: the render cap, and the index-generation pin.

Both are about a BATCH being one answer assembled from N separate engine
calls. Each call is independently correct; what these guard is the two ways
the assembly can be wrong without any single call failing.

Qt-free: nothing here constructs a QApplication, so it runs in the default
(non-gui) lane. The engine is stubbed -- these test the desktop's plumbing,
not the matcher, and a real index costs minutes.
"""
import pytest

from desktop import passage_lifecycle as pl


class _Sentinel:
    """Stands in for a live PassageIndex. Never dereferenced."""


class _RecordingSearcher:
    """Captures the kwargs `PassageSearchAdapter` builds it with."""

    seen = []

    def __init__(self, **kwargs):
        _RecordingSearcher.seen.append(kwargs)

    def search_composition_logic(self, *a, **kw):
        return {'main': [], 'filtered': []}


@pytest.fixture
def leased(monkeypatch):
    """A lease that always succeeds, and a stubbed engine.

    `search_composition_logic` refuses before it takes a lease when no index
    is installed, so a test of what it does AFTER the lease has to grant one.
    """
    import shared.passage_parallels as pp

    _RecordingSearcher.seen = []
    monkeypatch.setattr(pl, '_try_acquire_lease', lambda: _Sentinel())
    monkeypatch.setattr(pl, '_release_lease', lambda: None)
    monkeypatch.setattr(pp, 'PassageSearcher', _RecordingSearcher)
    monkeypatch.setattr(pl, 'compose', lambda *a, **k: 'POLICY')
    return _RecordingSearcher


class _NullFetcher:
    def get_full_text_by_header(self, header):
        return None


# ---------------------------------------------------------------------------
# The render cap. Three-valued, and the distinction decides what a
# multi-witness search is able to find.
# ---------------------------------------------------------------------------

def test_the_single_witness_path_says_nothing_about_the_cap(leased):
    """v9.0.0 behaviour, byte for byte: no `render_cap` kwarg at all, so
    PassageSearcher applies its own PARALLELS_GROUP_CAP default. Passing
    `render_cap=None` explicitly would override that default WITH None and
    break the `> 0` comparison inside the searcher."""
    pl.get_passage_searcher(_NullFetcher()).search_composition_logic('text')
    assert 'render_cap' not in leased.seen[0]


def test_the_multi_witness_path_asks_for_no_cap_at_all(leased):
    """Each witness must come back WHOLE, because the cap has to be applied
    once to the FUSED list. Capping each witness first fuses N already-
    truncated lists and silently drops every contributor past rank 200 in its
    own witness -- which is exactly where a rare witness of a widely-copied
    work sits."""
    pl.get_passage_searcher(
        _NullFetcher(), render_cap=0).search_composition_logic('text')
    assert leased.seen[0]['render_cap'] == 0


def test_zero_is_a_real_value_and_not_a_falsy_stand_in_for_unset(leased):
    """The caps are applied as `if self.render_cap and self.render_cap > 0`,
    so 0 means NO CAP while None means NO OPINION. Any truthiness test on
    this parameter collapses the two, and the collapse is silent: the
    multi-witness path would quietly get the 200-group cap back."""
    pl.get_passage_searcher(_NullFetcher(), render_cap=0).search_composition_logic('t')
    pl.get_passage_searcher(_NullFetcher()).search_composition_logic('t')
    with_zero, with_none = leased.seen
    assert with_zero.get('render_cap', 'ABSENT') == 0
    assert 'render_cap' not in with_none


def test_an_explicit_cap_is_passed_through(leased):
    pl.get_passage_searcher(
        _NullFetcher(), render_cap=50).search_composition_logic('text')
    assert leased.seen[0]['render_cap'] == 50


def test_the_cap_does_not_disturb_the_other_axes(leased):
    """The three policy axes are still validated independently and still
    reach `compose`; adding a fourth parameter must not shift them."""
    pl.get_passage_searcher(
        _NullFetcher(), 'max-40', 'short', 'deep',
        render_cap=0).search_composition_logic('text')
    assert leased.seen[0]['policy'] == 'POLICY'
    assert leased.seen[0]['render_cap'] == 0


def test_the_adapter_still_exposes_no_index_with_a_cap_set():
    """`__slots__` rules out a stray attribute holding a live index. Adding
    `_render_cap` must not have opened an instance dict."""
    adapter = pl.get_passage_searcher(_NullFetcher(), render_cap=0)
    with pytest.raises(AttributeError):
        adapter.__dict__


# ---------------------------------------------------------------------------
# The generation pin. A batch that straddles an index swap fuses ranks that
# were never comparable.
# ---------------------------------------------------------------------------

def test_an_unchanged_generation_is_not_a_change():
    assert pl.generation_changed(pl.current_state_generation()) is False


def test_a_bumped_generation_is_a_change(monkeypatch):
    pinned = pl.current_state_generation()
    monkeypatch.setattr(pl, '_state_generation', pinned + 1)
    assert pl.generation_changed(pinned) is True


def test_no_pin_is_not_a_change(monkeypatch):
    """A batch that never captured a generation started before anything was
    installed. Its first lease fails with PassageSearchUnavailableError,
    which is the honest error for that case -- not this one."""
    monkeypatch.setattr(pl, '_state_generation', 99)
    assert pl.generation_changed(None) is False


def test_the_replacement_error_is_catchable_and_not_a_witness_failure():
    """A swap voids the whole BATCH. Reporting it as a per-witness failure
    would tell the reader a witness found nothing, when in fact none of the
    fused numbers mean anything."""
    assert issubclass(pl.PassageIndexReplaced, RuntimeError)
    assert not issubclass(pl.PassageIndexReplaced, pl.PassageSearchError)
