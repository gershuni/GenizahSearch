# -*- coding: utf-8 -*-
"""Phase 146 Tasks 3+4: the desktop passage-index lifecycle state machine.

Every test drives the module's PUBLIC functions directly -- no QApplication,
no Qt import anywhere in this file. `desktop/passage_lifecycle.py`'s own
docstring explains why: the whole point of factoring it as pure functions
plus injected filesystem seams is that the state machine is provable without
a GUI event loop.
"""
from __future__ import annotations

import itertools
import os
import shutil
import subprocess
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import desktop.passage_lifecycle as pl  # noqa: E402
import shared.passage_builder as passage_builder  # noqa: E402
from shared.passage_index import BuildCancelled, open_index  # noqa: E402

ALEF = 0x05D0
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(autouse=True)
def _reset_module_globals():
    """`_state` and `_freshness` are module globals -- every test starts and
    ends with a clean slate so results never leak across tests."""
    pl.close_passage_state()
    yield
    pl.close_passage_state()


# ---------------------------------------------------------------------------
# Fixtures: a tiny synthetic corpus, on disk in the transcriptions format
# `shared/passage_corpus.py::iter_records` expects.
# ---------------------------------------------------------------------------

def _letters(seq) -> str:
    return ''.join(chr(ALEF + (v % 22)) for v in seq)


def synthetic_records(n_records: int = 20, base_len: int = 200) -> list:
    out = []
    for r in range(n_records):
        text = _letters(((r + 1) * (i + 1) for i in range(base_len)))
        out.append((f'rec{r:04d}', text))
    return out


def write_corpus(path: str, records: list) -> None:
    with open(path, 'w', encoding='utf-8') as fh:
        for rid, text in records:
            fh.write(f'==> {rid} <==\n{text}\n')


def _cancel_after(n):
    """False for the first n-1 calls, True from the n-th call on."""
    calls = {'i': 0}

    def cancel():
        calls['i'] += 1
        return calls['i'] >= n

    cancel.calls = calls
    return cancel


class _Corpus:
    """A ready-to-use (root, corpus_path, records) triple under tmp_path."""

    def __init__(self, tmp_path, n_records=20):
        self.root = str(tmp_path / 'passage_index')
        self.corpus_path = str(tmp_path / 'corpus.txt')
        self.records = synthetic_records(n_records)
        write_corpus(self.corpus_path, self.records)

    @property
    def source_paths(self):
        return [self.corpus_path]

    @property
    def live_dir(self):
        return os.path.join(self.root, pl.LIVE_DIRNAME)

    def build(self, **kwargs):
        """Mirrors the real caller contract: `run_build_and_swap` never
        installs on its own (see its docstring), so a test that wants a
        SECOND build to be able to rename `live` away must install -- and
        therefore later release, via `close_passage_state` -- whatever the
        first one returned. Skipping this is exactly the bug that produced
        the very first version of these rebuild tests: an un-installed
        `res.index` kept a live memmap open for the rest of the test
        function, and the next rebuild's rename blocked on it."""
        kwargs.setdefault('release_live_state', pl.close_passage_state)
        res = pl.run_build_and_swap(
            self.root, self.records, self.source_paths, self.corpus_path,
            partitions=2, **kwargs)
        if res.index is not None:
            pl.install_passage_state(
                pl.PassageState(index=res.index, live_dir=res.live_dir))
        return res


def _join_background_threads(result, extra_seconds=0.5):
    """Background generation/quarantine deletes are fire-and-forget; give
    them a moment before a test asserts on the directory listing."""
    time.sleep(extra_seconds)


# ---------------------------------------------------------------------------
# 0. install_passage_state must release the state it REPLACES, not just the
#    one close_passage_state releases at teardown.
# ---------------------------------------------------------------------------

def test_install_passage_state_releases_the_state_it_replaces(tmp_path):
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    pl.close_passage_state()  # undo the helper's own install; start clean

    # Two INDEPENDENT copies of the same artifact, each opened separately --
    # isolates "did the OUTGOING state's handles get released" from "is the
    # new state's own directory still open", which a single shared directory
    # could not distinguish.
    copy1 = str(tmp_path / 'copy1')
    copy2 = str(tmp_path / 'copy2')
    shutil.copytree(c.live_dir, copy1)
    shutil.copytree(c.live_dir, copy2)

    idx1 = open_index(copy1)
    assert idx1 is not None
    pl.install_passage_state(pl.PassageState(index=idx1, live_dir=copy1))

    idx2 = open_index(copy2)
    assert idx2 is not None
    pl.install_passage_state(pl.PassageState(index=idx2, live_dir=copy2))

    # idx1 is no longer installed -- its handles must be gone even though
    # idx2 (the currently-installed state) is still open on copy2.
    renamed = copy1 + '-renamed'
    os.rename(copy1, renamed)  # raises PermissionError if idx1 leaked
    shutil.rmtree(renamed)

    pl.close_passage_state()  # release idx2 too


def test_install_passage_state_same_object_is_a_noop(tmp_path):
    """Installing the SAME state object it already holds must not release
    its own handles out from under it."""
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    state = pl._state
    pl.install_passage_state(state)
    assert open_index(c.live_dir) is not None, (
        're-installing the same state must not close its own handles')


def test_install_passage_state_from_none_releases_nothing():
    """No outgoing state at all -- must not raise."""
    pl.close_passage_state()
    pl.install_passage_state(None)  # no-op, must not raise
    assert not pl.passage_available()


# ---------------------------------------------------------------------------
# 1. Cancel propagation: pass 1 / spooling / scatter / partition / hash.
# ---------------------------------------------------------------------------

def test_hash_cancel_reports_cancelled_before_any_build_work(tmp_path):
    """cancel on the very first hash call: build_index never even starts, and
    staging must never have been created at all."""
    c = _Corpus(tmp_path)
    outcome = pl.execute_build(c.root, c.records, c.source_paths,
                               cancel_check=_cancel_after(1))
    assert outcome.status == 'cancelled'
    assert not os.path.isdir(outcome.staging_dir)


def test_cancel_check_none_is_a_noop_for_execute_build(tmp_path):
    c = _Corpus(tmp_path)
    outcome = pl.execute_build(c.root, c.records, c.source_paths,
                               cancel_check=None)
    assert outcome.status == 'built'
    assert outcome.stats.n_records_indexed == len(c.records)


def test_cancel_inside_pass1_propagates_as_cancelled(tmp_path, monkeypatch):
    """The hash step is bypassed here (a stub with no cancel_check calls of
    its own) so every observed call is pass 1's -- proving execute_build
    plumbs cancel_check all the way into build_index, not just to the hash."""
    monkeypatch.setattr(passage_builder, 'CANCEL_CHECK_RECORDS', 3)
    monkeypatch.setattr(pl, 'source_manifest',
                        lambda paths, cancel_check=None: [{'stub': True}])
    c = _Corpus(tmp_path, n_records=20)
    outcome = pl.execute_build(c.root, c.records, c.source_paths,
                               cancel_check=_cancel_after(1))
    assert outcome.status == 'cancelled'
    assert not os.path.isdir(outcome.staging_dir)


_HUGE_BATCH = 1 << 40  # forces one batch per partition; see test_passage_builder.py


def test_cancel_inside_spool_spooling_loop_propagates(tmp_path, monkeypatch):
    monkeypatch.setattr(pl, 'source_manifest',
                        lambda paths, cancel_check=None: [{'stub': True}])
    c = _Corpus(tmp_path, n_records=10)
    outcome = pl.execute_build(c.root, c.records, c.source_paths,
                               construction='spool', partitions=3,
                               batch_grams=_HUGE_BATCH,
                               cancel_check=_cancel_after(1))
    assert outcome.status == 'cancelled'
    assert not os.path.isdir(outcome.staging_dir)


def test_cancel_inside_scatter_partition_loop_propagates(tmp_path, monkeypatch):
    monkeypatch.setattr(pl, 'source_manifest',
                        lambda paths, cancel_check=None: [{'stub': True}])
    c = _Corpus(tmp_path, n_records=10)
    outcome = pl.execute_build(c.root, c.records, c.source_paths,
                               construction='scatter', partitions=3,
                               batch_grams=_HUGE_BATCH,
                               cancel_check=_cancel_after(1))
    assert outcome.status == 'cancelled'
    assert not os.path.isdir(outcome.staging_dir)


def test_cancel_in_begin_swap_hash_propagates_and_cleans_staging(tmp_path):
    """begin_swap's own corpus re-fingerprint is cancellable too, and a
    cancel there must PROPAGATE (not be swallowed into a SwapValidation)."""
    c = _Corpus(tmp_path)
    built = pl.execute_build(c.root, c.records, c.source_paths)
    assert built.status == 'built'
    with pytest.raises(BuildCancelled):
        pl.begin_swap(built.staging_dir, c.source_paths,
                      built.pre_build_manifest, cancel_check=_cancel_after(1))
    assert not os.path.isdir(built.staging_dir)


# ---------------------------------------------------------------------------
# 2. Staging is deletable immediately after a cancel (Windows handle case).
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('construction', ['scatter', 'spool'])
def test_staging_is_immediately_deletable_after_a_mid_build_cancel(
        tmp_path, construction):
    """No ignore_errors=True anywhere on this path: if a memmap handle
    leaked, shutil.rmtree would raise PermissionError right here."""
    c = _Corpus(tmp_path, n_records=10)
    outcome = pl.execute_build(c.root, c.records, c.source_paths,
                               construction=construction, partitions=3,
                               batch_grams=_HUGE_BATCH,
                               cancel_check=_cancel_after(2))
    assert outcome.status == 'cancelled'
    assert not os.path.isdir(outcome.staging_dir)
    # Cleanup already happened; a SECOND attempt on the same (now-absent)
    # path must not be needed, but proves nothing is still pinning it.
    if os.path.isdir(outcome.staging_dir):
        shutil.rmtree(outcome.staging_dir)  # would raise if anything leaked


# ---------------------------------------------------------------------------
# 3. Any unswapped failure cleans staging; the ownership flag prevents the
#    finally/swap race.
# ---------------------------------------------------------------------------

def test_build_error_cleans_staging(tmp_path):
    c = _Corpus(tmp_path)

    def _boom(*a, **k):
        raise RuntimeError('synthetic build failure')

    orig = pl.build_index
    pl.build_index = _boom
    try:
        outcome = pl.execute_build(c.root, c.records, c.source_paths)
    finally:
        pl.build_index = orig
    assert outcome.status == 'error'
    assert not os.path.isdir(outcome.staging_dir)


def test_ownership_flag_prevents_the_finally_swap_race(tmp_path):
    """`owns_staging` flips to False the instant `build_index` returns, BEFORE
    `execute_build` does anything else -- so by the time a caller receives a
    'built' outcome and starts consuming staging (the real swap step,
    simulated here by just reading the artifact out of it), nothing left
    inside `execute_build`'s own `finally` can still delete it out from
    under that consumer. Contrast with the cancel/error paths, which still
    own staging throughout and must clean it up themselves."""
    c = _Corpus(tmp_path)
    outcome = pl.execute_build(c.root, c.records, c.source_paths)
    assert outcome.status == 'built'
    # execute_build has already returned; staging must STILL be there, fully
    # readable, exactly as a swap step consuming it would need.
    assert os.path.isdir(outcome.staging_dir)
    assert os.path.exists(os.path.join(outcome.staging_dir, 'manifest.json'))
    shutil.rmtree(outcome.staging_dir)  # test cleanup, not part of the assertion


# ---------------------------------------------------------------------------
# 4. First build: staging renames directly to live; live untouched by a
#    cancelled or failed build.
# ---------------------------------------------------------------------------

def test_first_build_renames_staging_directly_to_live_no_generation(tmp_path):
    c = _Corpus(tmp_path)
    res = c.build()
    assert res.status == 'installed'
    assert os.path.isdir(c.live_dir)
    entries = os.listdir(c.root)
    assert not any(n.startswith(pl.PREV_PREFIX) for n in entries), entries


def test_live_untouched_by_a_cancelled_first_build(tmp_path):
    c = _Corpus(tmp_path)
    res = c.build(cancel_check=lambda: True)
    assert res.status == 'cancelled'
    assert not os.path.isdir(c.live_dir)


def test_live_untouched_by_a_failed_first_build(tmp_path):
    c = _Corpus(tmp_path)

    def _boom(*a, **k):
        raise RuntimeError('boom')

    orig = pl.build_index
    pl.build_index = _boom
    try:
        res = c.build()
    finally:
        pl.build_index = orig
    assert res.status == 'error'
    assert not os.path.isdir(c.live_dir)


# ---------------------------------------------------------------------------
# 5. Rebuild: unique generation naming; back-to-back rebuilds don't collide;
#    a lagging background delete cannot hit the next generation.
# ---------------------------------------------------------------------------

def test_rebuild_creates_a_unique_prev_generation(tmp_path):
    c = _Corpus(tmp_path)
    res1 = c.build()
    assert res1.status == 'installed'
    res2 = c.build()
    assert res2.status == 'installed'
    _join_background_threads(res2)
    # The prev generation from res2's rebuild is background-deleted; by now
    # it should be gone, and `current` must hold the SECOND build.
    assert os.path.isdir(c.live_dir)


def test_back_to_back_rebuilds_never_collide_on_generation_name(tmp_path,
                                                                 monkeypatch):
    """Freeze the deletion of every generation so all of them coexist on
    disk at once, proving the NAMES themselves never collide even when nanosecond
    resolution alone would risk it."""
    seen_names = []
    orig_token = pl._gen_token

    def _tracking_token():
        tok = orig_token()
        seen_names.append(tok)
        return tok

    monkeypatch.setattr(pl, '_gen_token', _tracking_token)
    monkeypatch.setattr(pl, '_delete_generation_background',
                        lambda path: None)  # freeze: never actually delete

    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    for _ in range(4):
        assert c.build().status == 'installed'

    prevs = [n for n in os.listdir(c.root) if n.startswith(pl.PREV_PREFIX)]
    assert len(prevs) == 4, prevs  # one per rebuild, none collided/overwrote
    assert len(set(prevs)) == 4


def test_a_lagging_background_delete_cannot_hit_the_next_generation(
        tmp_path, monkeypatch):
    """Hold the FIRST rebuild's background delete thread before it runs, do
    a SECOND rebuild in the meantime, then release the first delete -- it
    must remove only its own (now-stale) generation, never the second
    rebuild's live directory or its own new generation."""
    release = {'go': False}
    started = []

    def _held_delete(path):
        def _do():
            while not release['go']:
                time.sleep(0.01)
            shutil.rmtree(path, ignore_errors=True)
        import threading
        t = threading.Thread(target=_do, daemon=True)
        started.append((path, t))
        t.start()
        return t

    monkeypatch.setattr(pl, '_delete_generation_background', _held_delete)

    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    res2 = c.build()
    assert res2.status == 'installed'
    first_prev = started[0][0]
    # The held delete has not run yet, so res2's own prev generation must
    # still be sitting on disk under the EXACT name it was queued with.
    current_prevs = [n for n in os.listdir(c.root) if n.startswith(pl.PREV_PREFIX)]
    assert current_prevs == [os.path.basename(first_prev)], current_prevs

    res3 = c.build()
    assert res3.status == 'installed'

    release['go'] = True
    for _path, t in started:
        t.join(timeout=5)

    assert os.path.isdir(c.live_dir), 'a lagging delete removed live itself'
    idx = open_index(c.live_dir)
    assert idx is not None, 'live must still open after the lagging deletes settle'


# ---------------------------------------------------------------------------
# 6. A None reload after the swap: retry, then rollback (old index works
#    again); the first-install variant quarantines and reports.
# ---------------------------------------------------------------------------

def _flaky_open_index(live_dir, fail_times):
    """Fails exactly `fail_times` calls made against `live_dir`, then defers
    to the real open_index -- models a genuinely bad reload (both retry
    attempts in `_open_with_retry` exhausted) without touching the fixture
    on disk."""
    real = pl.open_index
    calls = {'n': 0}

    def stub(path):
        if os.path.normpath(path) == os.path.normpath(live_dir):
            calls['n'] += 1
            if calls['n'] <= fail_times:
                return None
        return real(path)
    return stub, calls


def test_none_reload_on_rebuild_retries_then_rolls_back(tmp_path, monkeypatch):
    c = _Corpus(tmp_path)
    res1 = c.build()
    assert res1.status == 'installed'
    live = c.live_dir

    built = pl.execute_build(c.root, c.records, c.source_paths, partitions=2)
    assert built.status == 'built'
    validation = pl.begin_swap(built.staging_dir, c.source_paths,
                               built.pre_build_manifest)
    assert validation.ok

    stub, calls = _flaky_open_index(live, fail_times=2)  # both attempts fail
    monkeypatch.setattr(pl, 'open_index', stub)
    pl.close_passage_state()
    swap = pl.perform_swap(c.root, built.staging_dir)

    assert swap.status == 'reload_failed_rolled_back'
    assert swap.index is not None
    assert calls['n'] >= 2
    # The rollback quarantine is a proven-good-live rollback -- deleted
    # immediately, never surfaced.
    assert not any(n.startswith(pl.FAILED_PREFIX) for n in os.listdir(c.root))
    assert open_index(live) is not None, 'old index must work again'


def test_none_reload_on_first_install_quarantines_and_reports(tmp_path,
                                                               monkeypatch):
    c = _Corpus(tmp_path)
    built = pl.execute_build(c.root, c.records, c.source_paths, partitions=2)
    assert built.status == 'built'
    validation = pl.begin_swap(built.staging_dir, c.source_paths,
                               built.pre_build_manifest)
    assert validation.ok

    stub, calls = _flaky_open_index(c.live_dir, fail_times=2)
    monkeypatch.setattr(pl, 'open_index', stub)
    swap = pl.perform_swap(c.root, built.staging_dir)

    assert swap.status == 'reload_failed_first_install'
    assert swap.quarantine_dir
    assert os.path.isdir(swap.quarantine_dir)
    assert not os.path.isdir(c.live_dir)


def test_a_transient_open_failure_does_not_downgrade_a_valid_reload(
        tmp_path, monkeypatch):
    """One flaky failure, then success -- must NOT be treated as a genuine
    None reload."""
    c = _Corpus(tmp_path)
    res1 = c.build()
    assert res1.status == 'installed'
    live = c.live_dir

    built = pl.execute_build(c.root, c.records, c.source_paths, partitions=2)
    validation = pl.begin_swap(built.staging_dir, c.source_paths,
                               built.pre_build_manifest)
    assert validation.ok

    stub, calls = _flaky_open_index(live, fail_times=1)  # first attempt only
    monkeypatch.setattr(pl, 'open_index', stub)
    pl.close_passage_state()
    swap = pl.perform_swap(c.root, built.staging_dir)

    assert swap.status == 'installed', swap.status
    assert swap.index is not None


# ---------------------------------------------------------------------------
# 6b. The two catastrophic double-failure branches: the new build's reload
#     fails AND the rollback itself cannot be completed cleanly.
# ---------------------------------------------------------------------------

# A marker file travels with a directory through every os.rename (rename is
# metadata-only -- it never touches contents), so tying "this build is
# broken" to the marker's presence, rather than to whatever path the content
# currently sits under, lets the SAME simulated-broken build follow staging
# -> live -> quarantine while a never-marked previous generation opens fine
# wherever IT ends up, including after the terminal recovery walk promotes
# it back into `current/`. A stub keyed on the "live" path instead would
# also block the recovery walk's own post-promotion reopen of that exact
# path, which cannot be told apart from a genuinely unrecoverable machine.
_BROKEN_BUILD_MARKER = '.simulated_broken'


def test_rollback_failed_terminal_recovery_still_finds_the_previous_generation(
        tmp_path, monkeypatch):
    """New live renamed in, fails to open, gets quarantined -- then the
    restore of the known-good previous generation is blocked long enough to
    exhaust `_swap_rebuild`'s own bounded retry (`rollback_failed`). Nothing
    may be deleted: not the quarantine (the only copy of the broken build)
    and not the previous generation. And -- the point of this test --
    `run_build_and_swap`'s terminal path must not stop at a bare open of
    `current/`: it must route through the same recovery walk startup uses,
    which gets its OWN separate bounded retry at the very same restore
    rename and (here, modelling a transient block that clears a moment
    later, not a permanently cursed path) succeeds where the swap's narrower
    retry gave up -- promoting the previous generation into `current/` and
    returning it as a working index, not leaving the caller with nothing."""
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    live = c.live_dir

    real_open = pl.open_index

    def _wont_open_if_marked_broken(path):
        if os.path.isfile(os.path.join(path, _BROKEN_BUILD_MARKER)):
            return None
        return real_open(path)

    monkeypatch.setattr(pl, 'open_index', _wont_open_if_marked_broken)

    # Mark the NEW build broken only once `begin_swap`'s own validation of
    # staging has already passed -- otherwise the marker would fail that
    # earlier, unrelated check instead of the swap's post-promotion reload.
    real_begin_swap = pl.begin_swap

    def _mark_new_build_broken(staging_dir, source_paths, pre_build_manifest,
                               cancel_check=None):
        result = real_begin_swap(staging_dir, source_paths, pre_build_manifest,
                                 cancel_check=cancel_check)
        if result.ok:
            open(os.path.join(staging_dir, _BROKEN_BUILD_MARKER), 'w').close()
        return result

    monkeypatch.setattr(pl, 'begin_swap', _mark_new_build_broken)
    monkeypatch.setattr(pl, 'RENAME_RETRY_ATTEMPTS', 2)
    monkeypatch.setattr(pl, 'RENAME_RETRY_DELAY_SECONDS', 0)

    real_rename = os.rename
    blocked_calls = {'n': 0}

    def _restore_blocked_then_clears(src, dst):
        # Only the prev->live restore rename is blocked -- live->prev and
        # staging->live (both earlier in the same swap) must still succeed.
        # Blocked for exactly RENAME_RETRY_ATTEMPTS calls -- enough to
        # exhaust the swap's own retry loop -- then it clears, exactly like
        # a lagging antivirus scan rather than a rename that can never land.
        if os.path.basename(src).startswith(pl.PREV_PREFIX) \
                and os.path.normpath(dst) == os.path.normpath(live):
            blocked_calls['n'] += 1
            if blocked_calls['n'] <= 2:
                raise PermissionError('synthetic: prev->live restore blocked')
        return real_rename(src, dst)

    monkeypatch.setattr(pl.os, 'rename', _restore_blocked_then_clears)

    res = c.build()

    assert res.status == 'rollback_failed'
    assert res.quarantine_dir
    assert os.path.isdir(res.quarantine_dir), (
        'the unopenable build must be preserved, not deleted')
    assert res.index is not None, (
        'the terminal recovery walk must still return the previous '
        'generation as a working index, not leave the caller with nothing')
    assert res.index.n_records == len(c.records)
    assert res.live_dir == c.live_dir
    prevs = [n for n in os.listdir(c.root) if n.startswith(pl.PREV_PREFIX)]
    assert prevs == [], (
        'the recovered generation was promoted into current/ by the '
        'recovery walk, not deleted and not left behind under its own name')
    assert open_index(c.live_dir) is not None


def test_reload_failed_no_recovery_terminal_recovery_still_finds_live(
        tmp_path, monkeypatch):
    """The rollback rename itself succeeds, but re-opening the restored
    directory ALSO fails within `_swap_rebuild`'s own bounded retry
    (`reload_failed_no_recovery`) -- modelling a transient inability to open
    that specific location that clears a moment later, not a directory that
    is genuinely, permanently unopenable. Both copies (the quarantined
    broken build and the restored live) must survive; nothing is deleted.
    And -- the point of this test -- `run_build_and_swap`'s terminal path
    must route through the SAME recovery walk startup uses, whose own
    separate bounded retry succeeds where the swap's narrower one gave up,
    returning the restored generation as a working index rather than
    leaving the caller with nothing despite `current/` holding a perfectly
    good artifact."""
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    live = c.live_dir

    real_open = pl.open_index

    real_begin_swap = pl.begin_swap

    def _mark_new_build_broken(staging_dir, source_paths, pre_build_manifest,
                               cancel_check=None):
        result = real_begin_swap(staging_dir, source_paths, pre_build_manifest,
                                 cancel_check=cancel_check)
        if result.ok:
            open(os.path.join(staging_dir, _BROKEN_BUILD_MARKER), 'w').close()
        return result

    monkeypatch.setattr(pl, 'begin_swap', _mark_new_build_broken)

    live_open_calls = {'n': 0}

    def _stub_open(path):
        if os.path.isfile(os.path.join(path, _BROKEN_BUILD_MARKER)):
            return None  # the new build's own data, wherever it currently sits
        if os.path.normpath(path) == os.path.normpath(live):
            live_open_calls['n'] += 1
            if live_open_calls['n'] <= 2:
                # Transient: fails exactly as many times as `_swap_rebuild`'s
                # own reload check attempts, then clears before the terminal
                # recovery walk's SEPARATE retry gets to it.
                return None
        return real_open(path)

    monkeypatch.setattr(pl, 'open_index', _stub_open)

    res = c.build()

    assert res.status == 'reload_failed_no_recovery'
    assert res.quarantine_dir
    assert os.path.isdir(res.quarantine_dir), (
        'the unopenable new build must be preserved, not deleted')
    assert res.index is not None, (
        'the terminal recovery walk must still return the restored '
        'generation as a working index')
    assert res.index.n_records == len(c.records)
    assert res.live_dir == c.live_dir
    assert open_index(c.live_dir) is not None
    prevs = [n for n in os.listdir(c.root) if n.startswith(pl.PREV_PREFIX)]
    assert prevs == [], (
        'the previous generation was RENAMED into live, not left behind '
        'under its own name')


# ---------------------------------------------------------------------------
# 7. Every pre-promotion failure ends with the old state reinstalled.
# ---------------------------------------------------------------------------

def test_invalid_staging_leaves_old_live_reopenable(tmp_path):
    c = _Corpus(tmp_path)
    res1 = c.build()
    assert res1.status == 'installed'

    fake_staging = os.path.join(c.root, pl.STAGING_DIRNAME)
    os.makedirs(fake_staging, exist_ok=True)  # empty: not a real artifact
    validation = pl.begin_swap(fake_staging, c.source_paths, [])
    assert not validation.ok
    assert validation.reason == pl.VALIDATION_INVALID_STAGING
    assert not os.path.isdir(fake_staging)
    assert open_index(c.live_dir) is not None, 'old live must still open'


def test_a_transient_staging_open_failure_does_not_discard_a_finished_build(
        tmp_path, monkeypatch):
    """A single flaky open of a genuinely valid staging artifact must not be
    judged invalid -- and must not be deleted -- before the same bounded
    retry every other reload check in this module gets."""
    c = _Corpus(tmp_path)
    built = pl.execute_build(c.root, c.records, c.source_paths, partitions=2)
    assert built.status == 'built'

    stub, calls = _flaky_open_index(built.staging_dir, fail_times=1)
    monkeypatch.setattr(pl, 'open_index', stub)

    validation = pl.begin_swap(built.staging_dir, c.source_paths,
                               built.pre_build_manifest)

    assert validation.ok, validation.reason
    assert calls['n'] >= 2
    assert os.path.isdir(built.staging_dir), (
        'a candidate must not be deleted before the retry gives up on it')


def test_corpus_changed_mismatch_discards_staging_leaves_live_untouched(
        tmp_path):
    c = _Corpus(tmp_path)
    res1 = c.build()
    assert res1.status == 'installed'

    built = pl.execute_build(c.root, c.records, c.source_paths, partitions=2)
    assert built.status == 'built'
    # Corpus changes on disk AFTER the build read it.
    with open(c.corpus_path, 'a', encoding='utf-8') as fh:
        fh.write('==> extra0000 <==\nאבגדהוזחטיכלמנסעפצקרשת\n')

    validation = pl.begin_swap(built.staging_dir, c.source_paths,
                               built.pre_build_manifest)
    assert not validation.ok
    assert validation.reason == pl.VALIDATION_CORPUS_CHANGED
    assert not os.path.isdir(built.staging_dir)
    assert open_index(c.live_dir) is not None


def test_rename_blocked_on_rebuild_rolls_back_to_old_live(tmp_path,
                                                           monkeypatch):
    c = _Corpus(tmp_path)
    res1 = c.build()
    assert res1.status == 'installed'
    live = c.live_dir

    built = pl.execute_build(c.root, c.records, c.source_paths, partitions=2)
    validation = pl.begin_swap(built.staging_dir, c.source_paths,
                               built.pre_build_manifest)
    assert validation.ok

    monkeypatch.setattr(pl, 'RENAME_RETRY_ATTEMPTS', 2)
    monkeypatch.setattr(pl, 'RENAME_RETRY_DELAY_SECONDS', 0)

    real_rename = os.rename

    def _blocked_rename(src, dst):
        # Allow live->prev (the first rename), block staging->live (the
        # second) -- this is the mid-swap block scenario.
        if os.path.normpath(src) == os.path.normpath(built.staging_dir):
            raise PermissionError('synthetic: staging->live blocked')
        return real_rename(src, dst)

    monkeypatch.setattr(pl.os, 'rename', _blocked_rename)
    pl.close_passage_state()
    swap = pl.perform_swap(c.root, built.staging_dir)

    assert swap.status == 'rename_blocked'
    assert open_index(live) is not None, 'rollback must restore the old live'


def test_cancelled_latch_before_any_rename_leaves_live_untouched(tmp_path):
    c = _Corpus(tmp_path)
    res1 = c.build()
    assert res1.status == 'installed'

    built = pl.execute_build(c.root, c.records, c.source_paths, partitions=2)
    validation = pl.begin_swap(built.staging_dir, c.source_paths,
                               built.pre_build_manifest)
    assert validation.ok

    pl.close_passage_state()
    swap = pl.perform_swap(c.root, built.staging_dir, cancel_check=lambda: True)

    assert swap.status == 'cancelled'
    assert not os.path.isdir(built.staging_dir)
    assert open_index(c.live_dir) is not None


def test_cancelled_latch_on_a_first_build_leaves_no_live_promoted(tmp_path):
    """FIRST build: no `live/` exists, so `perform_swap` routes to
    `_swap_first_build`. A latch already set before `perform_swap` is even
    entered is caught by `perform_swap`'s own entry check, before
    `_swap_first_build` is reached at all."""
    c = _Corpus(tmp_path)
    assert not os.path.isdir(c.live_dir)

    built = pl.execute_build(c.root, c.records, c.source_paths, partitions=2)
    assert built.status == 'built'
    validation = pl.begin_swap(built.staging_dir, c.source_paths,
                               built.pre_build_manifest)
    assert validation.ok

    swap = pl.perform_swap(c.root, built.staging_dir, cancel_check=lambda: True)

    assert swap.status == 'cancelled'
    assert not os.path.isdir(built.staging_dir), 'staging must be discarded'
    assert not os.path.isdir(c.live_dir), (
        'a cancelled FIRST build must never promote staging to live')


def test_cancel_race_before_first_build_promotion_rename_is_still_caught(
        tmp_path):
    """The latch is NOT set when `perform_swap`'s entry check runs, but IS
    set by the time `_swap_first_build` reaches its own destructive rename --
    proving the re-check immediately before THAT rename, not merely the entry
    check, is what stands between a race and a promoted first build."""
    c = _Corpus(tmp_path)
    assert not os.path.isdir(c.live_dir)

    built = pl.execute_build(c.root, c.records, c.source_paths, partitions=2)
    assert built.status == 'built'
    validation = pl.begin_swap(built.staging_dir, c.source_paths,
                               built.pre_build_manifest)
    assert validation.ok

    cancel = _cancel_after(2)  # False on perform_swap's entry check, True after
    swap = pl.perform_swap(c.root, built.staging_dir, cancel_check=cancel)

    assert swap.status == 'cancelled'
    assert not os.path.isdir(built.staging_dir), 'staging must be discarded'
    assert not os.path.isdir(c.live_dir), (
        'a cancel racing the promotion rename must never let it land')


# ---------------------------------------------------------------------------
# 7b. `_reopen_live` directly -- every pre-promotion failure above routes
#     through it, but nothing exercised its own three outcomes in isolation.
# ---------------------------------------------------------------------------

def test_reopen_live_returns_a_usable_index_for_a_healthy_live(tmp_path):
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    pl.close_passage_state()  # release before _reopen_live re-opens the same dir

    result = pl._reopen_live(c.root)
    assert result.status == 'reinstalled'
    assert result.index is not None
    assert result.index.n_records == len(c.records)
    assert result.live_dir == c.live_dir


def test_reopen_live_retries_a_transient_failure_rather_than_giving_up(
        tmp_path, monkeypatch):
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    pl.close_passage_state()

    stub, calls = _flaky_open_index(c.live_dir, fail_times=1)  # one flake, then real
    monkeypatch.setattr(pl, 'open_index', stub)

    result = pl._reopen_live(c.root)
    assert result.status == 'reinstalled'
    assert result.index is not None
    assert calls['n'] >= 2, 'a single flaky open must not be treated as unopenable'


def test_reopen_live_reports_no_prior_state_when_genuinely_unopenable(
        tmp_path):
    c = _Corpus(tmp_path)
    os.makedirs(c.live_dir)  # present but not a real artifact -- open_index fails
    result = pl._reopen_live(c.root)
    assert result.status == 'no_prior_state'
    assert result.index is None


# ---------------------------------------------------------------------------
# 8. Corpus manifest differs pre/post build -> staging discarded, live
#    untouched (same as one of the tests above, kept here as its own item
#    per the acceptance list, using run_build_and_swap end to end).
# ---------------------------------------------------------------------------

def test_end_to_end_corpus_drift_during_build_discards_staging(tmp_path,
                                                                monkeypatch):
    c = _Corpus(tmp_path)
    res1 = c.build()
    assert res1.status == 'installed'

    # Simulate the corpus changing WHILE build_index runs: monkeypatch
    # source_manifest to return the pre-build fingerprint on its first call
    # (inside execute_build) and a DIFFERENT one on the second (inside
    # begin_swap) without touching the file at all.
    calls = {'n': 0}
    real = pl.source_manifest

    def _drifting(paths, cancel_check=None):
        calls['n'] += 1
        out = real(paths, cancel_check=cancel_check)
        if calls['n'] >= 2:
            out = [dict(out[0], sha256='deadbeef' * 8)]
        return out

    monkeypatch.setattr(pl, 'source_manifest', _drifting)
    res2 = c.build()
    assert res2.status == pl.VALIDATION_CORPUS_CHANGED
    # Assert on the function's OWN returned index/live_dir, not a side-channel
    # open_index(c.live_dir) -- a broken `_reopen_live` wiring that always
    # returns a state-less result must fail HERE.
    assert res2.index is not None, (
        'run_build_and_swap must itself report a usable reopened index')
    assert res2.index.n_records == len(c.records)
    assert res2.live_dir == c.live_dir


# ---------------------------------------------------------------------------
# 9. Recovery at startup.
# ---------------------------------------------------------------------------

def test_recovery_live_unopenable_falls_back_to_valid_prev_generation(
        tmp_path, monkeypatch):
    monkeypatch.setattr(pl, '_delete_generation_background', lambda p: None)
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    assert c.build().status == 'installed'  # creates a _prev-* generation
    pl.close_passage_state()  # release live's handles before mutating it

    # Corrupt live for real.
    os.remove(os.path.join(c.live_dir, 'manifest.json'))
    # A leftover quarantine must survive being tried and NOT be deleted by
    # this promotion.
    quarantine = os.path.join(c.root, f'{pl.FAILED_PREFIX}sentinel')
    os.makedirs(quarantine)

    result = pl.recover_at_startup(c.root)
    assert result.status == 'recovered_from_prev'
    assert result.index is not None
    assert open_index(c.live_dir) is not None
    assert os.path.isdir(quarantine), 'an untried quarantine must survive'


def test_recovery_newest_generation_corrupt_older_one_valid(tmp_path,
                                                             monkeypatch):
    c = _Corpus(tmp_path)
    monkeypatch.setattr(pl, '_delete_generation_background', lambda p: None)
    assert c.build().status == 'installed'
    assert c.build().status == 'installed'
    assert c.build().status == 'installed'
    prevs = sorted(n for n in os.listdir(c.root) if n.startswith(pl.PREV_PREFIX))
    assert len(prevs) == 2, prevs
    # Corrupt the NEWER one (higher gen token -> sorts later ascending).
    newest = sorted(prevs, key=lambda n: pl._gen_sort_key(n, pl.PREV_PREFIX))[-1]
    os.remove(os.path.join(c.root, newest, 'manifest.json'))
    pl.close_passage_state()  # release live's handles before removing it
    shutil.rmtree(c.live_dir)

    result = pl.recover_at_startup(c.root)
    assert result.status == 'recovered_from_prev'
    assert open_index(c.live_dir) is not None
    # The corrupt newest generation is swept away with the other _prev-*.
    remaining = [n for n in os.listdir(c.root) if n.startswith(pl.PREV_PREFIX)]
    assert remaining == [], remaining


def test_recovery_no_live_no_generation_only_quarantine_is_tried(tmp_path):
    c = _Corpus(tmp_path)
    built = pl.execute_build(c.root, c.records, c.source_paths, partitions=2)
    assert built.status == 'built'
    quarantine = os.path.join(c.root, f'{pl.FAILED_PREFIX}{pl._gen_token()}')
    os.rename(built.staging_dir, quarantine)

    result = pl.recover_at_startup(c.root)
    assert result.status == 'recovered_from_quarantine'
    assert open_index(c.live_dir) is not None


def test_recovery_transient_open_failure_does_not_downgrade_valid_live(
        tmp_path, monkeypatch):
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    stub, calls = _flaky_open_index(c.live_dir, fail_times=1)
    monkeypatch.setattr(pl, 'open_index', stub)

    result = pl.recover_at_startup(c.root)
    assert result.status == 'live_ok'
    assert result.index is not None
    assert calls['n'] >= 2


def test_recovery_nothing_recoverable_reports_cleanly(tmp_path):
    c = _Corpus(tmp_path)
    os.makedirs(c.root, exist_ok=True)
    result = pl.recover_at_startup(c.root)
    assert result.status == 'nothing_recoverable'
    assert result.index is None


# ---------------------------------------------------------------------------
# 10. Quarantine matrix (rollback quarantine deleted; no-proven-copy
#     quarantine kept until a later proven build).
# ---------------------------------------------------------------------------

def test_kept_quarantine_survives_until_a_later_proven_build(tmp_path,
                                                              monkeypatch):
    c = _Corpus(tmp_path)
    built = pl.execute_build(c.root, c.records, c.source_paths, partitions=2)
    validation = pl.begin_swap(built.staging_dir, c.source_paths,
                               built.pre_build_manifest)
    assert validation.ok
    stub, _calls = _flaky_open_index(c.live_dir, fail_times=2)
    monkeypatch.setattr(pl, 'open_index', stub)
    swap = pl.perform_swap(c.root, built.staging_dir)
    assert swap.status == 'reload_failed_first_install'
    quarantine = swap.quarantine_dir
    assert os.path.isdir(quarantine)

    # An UNRELATED successful build later must sweep it.
    monkeypatch.undo()
    res = c.build()
    assert res.status == 'installed'
    _join_background_threads(res, extra_seconds=0.8)
    assert not os.path.isdir(quarantine), 'a later proven build must sweep it'


# ---------------------------------------------------------------------------
# 11. Lock.
# ---------------------------------------------------------------------------

def test_a_second_in_process_acquire_is_refused(tmp_path):
    lock1 = pl.acquire_lock(str(tmp_path))
    assert lock1 is not None
    try:
        lock2 = pl.acquire_lock(str(tmp_path))
        assert lock2 is None
    finally:
        pl.release_lock(lock1)


def test_lock_released_after_a_release_lock_call(tmp_path):
    lock1 = pl.acquire_lock(str(tmp_path))
    pl.release_lock(lock1)
    lock2 = pl.acquire_lock(str(tmp_path))
    assert lock2 is not None
    pl.release_lock(lock2)


def test_lock_released_on_a_build_error(tmp_path):
    c = _Corpus(tmp_path)

    def _boom(*a, **k):
        raise RuntimeError('boom')

    orig = pl.build_index
    pl.build_index = _boom
    try:
        res = c.build()
    finally:
        pl.build_index = orig
    assert res.status == 'error'
    lock = pl.acquire_lock(c.root)
    assert lock is not None, 'lock must be released after a build error'
    pl.release_lock(lock)


def test_lock_released_on_a_thread_start_style_failure(tmp_path, monkeypatch):
    """Any exception raised before the worker's own body even starts must
    still release the lock -- `run_build_and_swap`'s `finally` covers the
    WHOLE run, not only the paths this module explicitly names."""
    c = _Corpus(tmp_path)

    def _explode(root, corpus_path):
        raise RuntimeError('synthetic thread-start failure')

    monkeypatch.setattr(pl, 'prepare_staging', _explode)
    with pytest.raises(RuntimeError):
        c.build()
    lock = pl.acquire_lock(c.root)
    assert lock is not None, 'lock must be released even on an early raise'
    pl.release_lock(lock)


def test_startup_recovery_skips_when_lock_unavailable(tmp_path):
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    held = pl.acquire_lock(c.root)
    try:
        before = sorted(os.listdir(c.root))
        result = pl.recover_at_startup(c.root)
        assert result.status == 'locked'
        assert result.index is None
        after = sorted(os.listdir(c.root))
        assert before == after, 'a skipped recovery must touch nothing'
    finally:
        pl.release_lock(held)


def _lock_holder_script() -> str:
    return (
        "import sys, time\n"
        f"sys.path.insert(0, {REPO_ROOT!r})\n"
        "import desktop.passage_lifecycle as pl\n"
        "lock = pl.acquire_lock(sys.argv[1])\n"
        "print('LOCKED' if lock is not None else 'FAILED', flush=True)\n"
        "time.sleep(30)\n"
    )


def test_lock_refuses_a_real_second_process_and_os_releases_it_on_kill(
        tmp_path):
    root = str(tmp_path)
    proc = subprocess.Popen([sys.executable, '-c', _lock_holder_script(), root],
                            stdout=subprocess.PIPE, text=True)
    try:
        line = proc.stdout.readline().strip()
        assert line == 'LOCKED', line
        # Our own process must now be refused.
        ours = pl.acquire_lock(root)
        assert ours is None
    finally:
        proc.kill()
        proc.wait(timeout=10)

    # The OS must release the lock the instant the holder dies -- poll
    # briefly rather than assuming instantaneous propagation.
    deadline = time.time() + 5
    got = None
    while time.time() < deadline:
        got = pl.acquire_lock(root)
        if got is not None:
            break
        time.sleep(0.1)
    assert got is not None, 'lock was not released after the holder was killed'
    pl.release_lock(got)


def _mmap_holder_script() -> str:
    return (
        "import sys, time\n"
        f"sys.path.insert(0, {REPO_ROOT!r})\n"
        "from shared.passage_index import open_index\n"
        "idx = open_index(sys.argv[1])\n"
        "print('MAPPED' if idx is not None else 'FAILED', flush=True)\n"
        "time.sleep(30)\n"
    )


@pytest.mark.skipif(
    sys.platform != 'win32',
    reason='os.rename over a directory another process has memory-mapped '
           'only raises PermissionError on Windows -- POSIX rename() is a '
           'metadata-only operation unaffected by open file handles inside '
           'the directory, so this is a Windows filesystem property, not a '
           'code property, and the sibling test below pins the POSIX side')
def test_a_second_process_mapped_reader_causes_a_persistent_rename_block(
        tmp_path, monkeypatch):
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    live = c.live_dir

    proc = subprocess.Popen(
        [sys.executable, '-c', _mmap_holder_script(), live],
        stdout=subprocess.PIPE, text=True)
    try:
        line = proc.stdout.readline().strip()
        assert line == 'MAPPED', line
        time.sleep(0.3)

        built = pl.execute_build(c.root, c.records, c.source_paths, partitions=2)
        assert built.status == 'built'
        validation = pl.begin_swap(built.staging_dir, c.source_paths,
                                   built.pre_build_manifest)
        assert validation.ok

        monkeypatch.setattr(pl, 'RENAME_RETRY_ATTEMPTS', 3)
        monkeypatch.setattr(pl, 'RENAME_RETRY_DELAY_SECONDS', 0.1)
        pl.close_passage_state()
        swap = pl.perform_swap(c.root, built.staging_dir)
        assert swap.status == 'rename_blocked', swap.status
    finally:
        proc.kill()
        proc.wait(timeout=10)

    # Once the reader is gone, the SAME live directory must still open fine
    # -- the block cost nothing permanent.
    assert open_index(live) is not None


@pytest.mark.skipif(
    sys.platform == 'win32',
    reason='pins the POSIX side of the scenario above: renaming a directory '
           "out from under another process's open mmap is NOT blocked on "
           'POSIX, so the rebuild must complete rather than roll back')
def test_a_second_process_mapped_reader_does_not_block_rename_on_posix(
        tmp_path):
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    live = c.live_dir

    proc = subprocess.Popen(
        [sys.executable, '-c', _mmap_holder_script(), live],
        stdout=subprocess.PIPE, text=True)
    try:
        line = proc.stdout.readline().strip()
        assert line == 'MAPPED', line
        time.sleep(0.3)

        built = pl.execute_build(c.root, c.records, c.source_paths, partitions=2)
        assert built.status == 'built'
        validation = pl.begin_swap(built.staging_dir, c.source_paths,
                                   built.pre_build_manifest)
        assert validation.ok

        pl.close_passage_state()
        swap = pl.perform_swap(c.root, built.staging_dir)
        assert swap.status == 'installed', swap.status
    finally:
        proc.kill()
        proc.wait(timeout=10)

    assert open_index(live) is not None


# ---------------------------------------------------------------------------
# 12. Cancelled latch: a cancel landing after the build but before the swap
#     still prevents the swap (covered above as
#     test_cancelled_latch_before_any_rename_leaves_live_untouched; this adds
#     the run_build_and_swap end-to-end variant).
# ---------------------------------------------------------------------------

def test_end_to_end_cancel_between_build_and_swap_prevents_the_swap(
        tmp_path):
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    live_before = open_index(c.live_dir).manifest

    # cancel_check that is False during the build and True from then on --
    # models "cancel requested right after the build finished".
    state = {'building_done': False}

    def cancel():
        return state['building_done']

    orig_execute_build = pl.execute_build

    def _tracking_execute_build(*a, **k):
        result = orig_execute_build(*a, **k)
        state['building_done'] = True
        return result

    import unittest.mock as mock
    with mock.patch.object(pl, 'execute_build', _tracking_execute_build):
        res = c.build(cancel_check=cancel)

    assert res.status == 'cancelled'
    # Assert on the function's OWN returned index, not a side-channel
    # open_index(c.live_dir) -- a broken `_reopen_live` wiring that always
    # returns a state-less result must fail HERE.
    assert res.index is not None, (
        'run_build_and_swap must itself report a usable reopened index')
    assert res.index.manifest == live_before, (
        'live must be byte-for-byte the pre-rebuild artifact')
    assert res.live_dir == c.live_dir


# ---------------------------------------------------------------------------
# 12b. Two-phase swap handoff: `run_build_and_swap` must not mutate `_state`
#      itself -- it only ever requests the release through the injected seam.
# ---------------------------------------------------------------------------

def test_run_build_and_swap_uses_the_injected_seam_not_close_passage_state(
        tmp_path, monkeypatch):
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'

    orig_close = pl.close_passage_state
    close_calls = []

    def _tracking_close():
        close_calls.append(1)
        return orig_close()

    monkeypatch.setattr(pl, 'close_passage_state', _tracking_close)

    seam_calls = []

    def _seam():
        seam_calls.append(1)
        orig_close()  # the real release, driven by the seam -- not by the
                      # module reaching for close_passage_state on its own

    res2 = pl.run_build_and_swap(c.root, c.records, c.source_paths,
                                 c.corpus_path, partitions=2,
                                 release_live_state=_seam)
    assert res2.status == 'installed'
    if res2.index is not None:
        pl.install_passage_state(
            pl.PassageState(index=res2.index, live_dir=res2.live_dir))
    assert seam_calls == [1]
    assert close_calls == [], (
        'run_build_and_swap must never call close_passage_state directly '
        '-- only the injected seam may release _state')


@pytest.mark.skipif(
    sys.platform != 'win32',
    reason='the seam-skipped rename is only caught by the OS on Windows -- '
           'POSIX rename() does not care that this process still holds an '
           'open mmap on the directory being renamed, so a broken seam is '
           'NOT stopped there (a filesystem property, not a code property); '
           'the sibling test below pins the POSIX side')
def test_run_build_and_swap_relies_on_the_seam_actually_releasing(tmp_path):
    """A seam that does nothing must leave the OLD live's handles open --
    proving the rename that follows depends on the seam's own effect, not on
    some direct release the worker performs behind its back."""
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    assert pl.passage_available()

    res2 = pl.run_build_and_swap(c.root, c.records, c.source_paths,
                                 c.corpus_path, partitions=2,
                                 release_live_state=lambda: None)
    assert res2.status == 'rename_blocked', res2.status
    assert pl.passage_available(), (
        'a no-op seam must leave the old state exactly as installed')
    pl.close_passage_state()


@pytest.mark.skipif(
    sys.platform == 'win32',
    reason='pins the POSIX side of the scenario above: a broken seam is NOT '
           'caught by the OS here, so the rebuild goes through regardless '
           'of the still-open old mmap')
def test_run_build_and_swap_seam_not_running_is_not_caught_by_the_os_on_posix(
        tmp_path):
    c = _Corpus(tmp_path)
    assert c.build().status == 'installed'
    assert pl.passage_available()

    res2 = pl.run_build_and_swap(c.root, c.records, c.source_paths,
                                 c.corpus_path, partitions=2,
                                 release_live_state=lambda: None)
    assert res2.status == 'installed', res2.status
    pl.close_passage_state()


# ---------------------------------------------------------------------------
# 13. Freshness.
# ---------------------------------------------------------------------------

def test_freshness_missing_corpus_sources_yields_unknown():
    assert pl.compute_freshness([], ['/does/not/matter']) == pl.UNKNOWN


def test_freshness_fresh_and_stale(tmp_path):
    c = _Corpus(tmp_path)
    res = c.build()
    assert res.status == 'installed'
    sources = res.index.manifest['corpus']['sources']
    assert sources, 'fixture must actually carry a corpus fingerprint'

    assert pl.compute_freshness(sources, c.source_paths) == pl.FRESH
    with open(c.corpus_path, 'a', encoding='utf-8') as fh:
        fh.write('==> extra0000 <==\nאבגדהוזחטיכלמנסעפצקרשת\n')
    assert pl.compute_freshness(sources, c.source_paths) == pl.STALE


def test_freshness_result_computed_against_a_since_changed_artifact_is_discarded():
    token = pl.start_freshness_check()
    # The artifact was replaced (e.g. install_passage_state ran) WHILE this
    # check was in flight -- its token is now stale.
    pl.install_passage_state(None)
    pl.finish_freshness_check(token, pl.FRESH)
    snap = pl.freshness_snapshot()
    assert snap.result is None, 'a stale-token result must be discarded'


def test_freshness_banner_clears_after_a_successful_rebuild(tmp_path):
    token = pl.start_freshness_check()
    pl.finish_freshness_check(token, pl.STALE)
    assert pl.freshness_snapshot().result == pl.STALE

    c = _Corpus(tmp_path)
    res = c.build()
    pl.install_passage_state(pl.PassageState(index=res.index,
                                             live_dir=res.live_dir))
    snap = pl.freshness_snapshot()
    assert snap.result is None
    assert snap.checking is False


def test_freshness_cancelled_check_leaves_nothing_stuck(tmp_path):
    real_file = tmp_path / 'corpus.txt'
    real_file.write_text('placeholder', encoding='utf-8')
    token = pl.start_freshness_check()
    assert pl.freshness_snapshot().checking is True
    result = pl.compute_freshness([{'path': 'x', 'bytes': 1, 'sha256': 'x'}],
                                  [str(real_file)], cancel_check=lambda: True)
    assert result is None
    pl.finish_freshness_check(token, result)
    snap = pl.freshness_snapshot()
    assert snap.checking is False
    assert snap.result is None


# ---------------------------------------------------------------------------
# 14. Free-space preflight: the letters upper bound, never raw bytes.
# ---------------------------------------------------------------------------

def test_estimate_build_bytes_uses_letters_upper_bound_not_raw_bytes(
        tmp_path):
    corpus = tmp_path / 'corpus.txt'
    corpus.write_bytes(b'x' * 10_000)
    n_bytes = 10_000
    letters_ub = n_bytes // 2

    from shared.passage_builder import estimate_artifact_bytes
    expected = estimate_artifact_bytes(letters_ub) + 8 * letters_ub
    raw_bytes_formula = estimate_artifact_bytes(n_bytes) + 8 * n_bytes

    got = pl.estimate_build_bytes(str(corpus))
    assert got == expected
    assert got < raw_bytes_formula, (
        'using raw file bytes as n_letters roughly doubles the demand')


def test_prepare_staging_points_free_space_check_at_the_parent(tmp_path,
                                                                monkeypatch):
    c = _Corpus(tmp_path)
    seen = {}

    def _capture(index_dir, needed_bytes):
        seen['dir'] = index_dir
        seen['needed'] = needed_bytes

    monkeypatch.setattr(pl, 'check_free_space', _capture)
    pl.prepare_staging(c.root, c.corpus_path)
    assert seen['dir'] == c.root
    assert not os.path.isdir(c.live_dir), (
        'preflight must never create the live directory'
    )
    assert seen['needed'] == pl.estimate_build_bytes(c.corpus_path)


def test_prepare_staging_clears_a_stale_tree_before_preflighting(tmp_path,
                                                                  monkeypatch):
    """Preflight-after-clear, not before: a crashed staging tree eating the
    headroom must not permanently refuse every retry."""
    c = _Corpus(tmp_path)
    stale = os.path.join(c.root, pl.STAGING_DIRNAME)
    os.makedirs(stale)
    with open(os.path.join(stale, 'junk.bin'), 'wb') as fh:
        fh.write(b'0' * 1000)

    order = []
    orig_check = pl.check_free_space

    def _tracking_check(index_dir, needed_bytes):
        order.append(('preflight', os.path.isdir(stale)))
        return orig_check(index_dir, needed_bytes)

    monkeypatch.setattr(pl, 'check_free_space', _tracking_check)
    pl.prepare_staging(c.root, c.corpus_path)
    assert order == [('preflight', False)], (
        'the stale tree must be gone BEFORE the preflight check runs')


# ---------------------------------------------------------------------------
# Task 4: the pure readiness gate.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    'passage_ready,scope,lab_on,build_in_flight,main_ready,expected', [
        # Scope wins over every other condition.
        (True, 'local', False, False, True, pl.REASON_SCOPE),
        (True, 'all', False, False, True, pl.REASON_SCOPE),
        (False, 'local', True, True, False, pl.REASON_SCOPE),
        # Genizah scope, one blocker at a time.
        (False, 'genizah', False, False, True, pl.REASON_NOT_BUILT),
        (True, 'genizah', True, False, True, pl.REASON_LAB_ACTIVE),
        (True, 'genizah', False, True, True, pl.REASON_BUILD_IN_FLIGHT),
        (True, 'genizah', False, False, False, pl.REASON_MAIN_INDEX_MISSING),
        # Fully ready.
        (True, 'genizah', False, False, True, None),
        # Precedence boundaries: not-built beats everything after it.
        (False, 'genizah', True, True, False, pl.REASON_NOT_BUILT),
        # lab-active beats build-in-flight and main-index-missing.
        (True, 'genizah', True, True, False, pl.REASON_LAB_ACTIVE),
        # build-in-flight beats main-index-missing.
        (True, 'genizah', False, True, False, pl.REASON_BUILD_IN_FLIGHT),
    ])
def test_passage_disabled_reason_precedence(passage_ready, scope, lab_on,
                                            build_in_flight, main_ready,
                                            expected):
    got = pl.passage_disabled_reason(passage_ready, scope, lab_on,
                                     build_in_flight, main_ready)
    assert got == expected


def test_passage_disabled_reason_full_truth_table_is_deterministic():
    """Every combination of the five inputs must return SOME value from the
    known reason set (or None) -- no crash, no unexpected key, for all 32
    combinations including every main-index-missing pairing."""
    known = {pl.REASON_SCOPE, pl.REASON_NOT_BUILT, pl.REASON_LAB_ACTIVE,
            pl.REASON_BUILD_IN_FLIGHT, pl.REASON_MAIN_INDEX_MISSING, None}
    for passage_ready, scope, lab_on, build_in_flight, main_ready in \
            itertools.product([True, False], ['genizah', 'local'],
                              [True, False], [True, False], [True, False]):
        got = pl.passage_disabled_reason(passage_ready, scope, lab_on,
                                         build_in_flight, main_ready)
        assert got in known


def test_passage_disabled_reason_reads_no_module_state(monkeypatch):
    """Every input is explicit -- poisoning the module's OWN globals must not
    change the answer."""
    monkeypatch.setattr(pl, '_state', 'poisoned', raising=False)
    got = pl.passage_disabled_reason(True, 'genizah', False, False, True)
    assert got is None
