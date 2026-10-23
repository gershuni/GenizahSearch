"""Building the main index beside the live one and swapping it in (shared/index_swap.py,
``Indexer.create_index(beside=True)``), on the fixture corpus of
tests/responsa_index_fixture.py, in both formats.

The live index keeps answering while a build runs beside it; the swap puts the new one in
place and the old one away; a cancel or a failure mid-build leaves the live index as it was
and nothing of the build; what a crash leaves (a half-built ``tantivy_db.new``, an
interrupted swap) is set right at the next start; and the free-space plan says when a build
beside the live index does not fit.
"""
import os
import shutil
from unittest.mock import patch

import pytest

from shared import index_swap
from shared.config import Config
from shared.index_format import read_index_format
from shared.indexer import IndexBuildCancelled
from tests.responsa_index_fixture import build, engine

A = [("9900000001", 1, 1, "ראובן הלך לשוק"), ("9900000001", 1, 2, "ושמעון בא אחריו")]
B = [("9900000001", 1, 1, "ראובן הלך לשוק"), ("9900000001", 1, 2, "ושמעון בא אחריו"),
     ("9900000002", 2, 1, "לוי ישב בבית")]
OLD_FORMAT = dict(seams=False, reversed_field=False)


def _count(eng, word):
    """Page documents holding *word* (the whole-manuscript document of its page is left out)."""
    query = eng.index.parse_query(f'scope:page AND content:{word}', ['content'])
    return eng.searcher.search(query, 10, count=True).count


def _browse_map(root):
    return os.path.join(root, "browse_map.pkl")


def _reload(eng, root):
    """``reload_index`` reads Config.INDEX_DIR, which the fixture's engine() patches only
    while it constructs the engine: pointed at the fixture again here, never the real index."""
    with patch.object(Config, "INDEX_DIR", str(root)):
        return eng.reload_index()


@pytest.fixture
def live(tmp_path):
    """Corpus A at tmp_path, with an engine open on it. Yields (root, engine)."""
    root = build(tmp_path, A)
    eng = engine(root)
    try:
        yield root, eng
    finally:
        eng.close_index()


# --- the build beside the live index, and the swap ------------------------------------------

@pytest.mark.parametrize("live_kwargs, new_kwargs", [({}, {}), (OLD_FORMAT, {}), (OLD_FORMAT, OLD_FORMAT)],
                         ids=["2-over-2", "2-over-1", "1-over-1"])
def test_the_live_index_serves_through_the_build_and_the_swap_installs_the_new_one(tmp_path, live_kwargs, new_kwargs):
    root = build(tmp_path, A, **live_kwargs)
    eng = engine(root)
    try:
        marker_before = read_index_format(index_swap.live_index_path(root))
        assert _count(eng, "לוי") == 0
        build(root, B, beside=True, **new_kwargs)
        # The staged build is complete and the live one untouched and still answering.
        staged = index_swap.staged_index_path(root)
        assert os.path.isfile(os.path.join(staged, "meta.json"))
        assert os.path.isfile(index_swap.staged_browse_map_path(_browse_map(root)))
        assert read_index_format(index_swap.live_index_path(root)) == marker_before
        assert _count(eng, "לוי") == 0 and _count(eng, "ראובן") == 1
        if not new_kwargs:
            assert os.path.isdir(os.path.join(staged, "seams"))
            assert read_index_format(staged)["format"] == 2

        with open(index_swap.staged_browse_map_path(_browse_map(root)), "rb") as f:
            staged_map = f.read()
        with open(_browse_map(root), "wb") as f:
            f.write(b"the map of the index being replaced")
        eng.close_index()
        old = index_swap.swap_in_staged_index(root, _browse_map(root))
        assert old == index_swap.old_index_path(root) and os.path.isdir(old)
        assert not os.path.exists(staged)
        assert not os.path.exists(index_swap.staged_browse_map_path(_browse_map(root)))
        assert _reload(eng, root)
        assert _count(eng, "לוי") == 1
        assert eng.index_format_status()["current"] is (not new_kwargs)
        with open(_browse_map(root), "rb") as f:
            assert f.read() == staged_map                      # the new build's browse map
        assert index_swap.remove_old_index(root, attempts=3, delay=0.1)
        assert not os.path.exists(old)
    finally:
        eng.close_index()


def test_a_fresh_build_beside_nothing_leaves_no_old_folder(tmp_path):
    root = str(tmp_path)
    build(root, B, beside=True)
    assert index_swap.swap_in_staged_index(root, _browse_map(root)) is None
    eng = engine(root)
    try:
        assert _count(eng, "לוי") == 1
    finally:
        eng.close_index()


def test_the_swap_undoes_its_first_rename_when_the_second_fails(live):
    root, eng = live
    build(root, B, beside=True)
    eng.close_index()
    real_rename = os.rename

    def failing(src, dst):
        if src == index_swap.staged_index_path(root):
            raise OSError("refused")
        real_rename(src, dst)

    with patch("shared.index_swap.os.rename", side_effect=failing):
        with pytest.raises(OSError):
            index_swap.swap_in_staged_index(root, _browse_map(root))
    assert os.path.isdir(index_swap.live_index_path(root))
    assert not os.path.exists(index_swap.old_index_path(root))
    assert _reload(eng, root) and _count(eng, "לוי") == 0      # the index the user had


def test_a_browse_map_that_refuses_to_move_undoes_both_renames(live):
    # Codex round 1 on PR #446: the index renames were done, the browse map's os.replace
    # failed, and the caller said "the index you had is still in use" over the NEW index.
    root, eng = live
    build(root, B, beside=True)
    eng.close_index()
    with patch("shared.index_swap.os.replace", side_effect=PermissionError("map in use")):
        with pytest.raises(PermissionError):
            index_swap.swap_in_staged_index(root, _browse_map(root))
    assert os.path.isdir(index_swap.staged_index_path(root))          # the build is still staged
    assert not os.path.exists(index_swap.old_index_path(root))
    assert os.path.isfile(index_swap.staged_browse_map_path(_browse_map(root)))
    assert _reload(eng, root) and _count(eng, "לוי") == 0            # the index the user had


def test_a_swap_interrupted_before_its_browse_map_is_finished_at_the_next_start(live):
    root, eng = live
    build(root, B, beside=True)
    eng.close_index()
    with open(index_swap.staged_browse_map_path(_browse_map(root)), "rb") as f:
        staged_map = f.read()
    with open(_browse_map(root), "wb") as f:
        f.write(b"the old index's map")
    # The crash came after both renames and before the browse map moved.
    os.rename(index_swap.live_index_path(root), index_swap.old_index_path(root))
    os.rename(index_swap.staged_index_path(root), index_swap.live_index_path(root))
    actions = index_swap.recover_index_dirs(root, _browse_map(root))
    assert actions == ['applied-staged-browse-map', 'discarded-old']
    with open(_browse_map(root), "rb") as f:
        assert f.read() == staged_map
    assert _reload(eng, root) and _count(eng, "לוי") == 1


def test_a_half_built_index_never_gets_its_map_applied_even_beside_a_stale_old_folder(live):
    root, eng = live
    os.makedirs(index_swap.old_index_path(root))                     # a swap whose cleanup failed
    staged = index_swap.staged_index_path(root)
    os.makedirs(staged)                                               # a build that crashed
    with open(index_swap.staged_browse_map_path(_browse_map(root)), "wb") as f:
        f.write(b"a half-built index's map")
    with open(_browse_map(root), "rb") as f:
        before = f.read()
    actions = index_swap.recover_index_dirs(root, _browse_map(root))
    assert actions == ['discarded-staged', 'discarded-old', 'discarded-staged-browse-map']
    with open(_browse_map(root), "rb") as f:
        assert f.read() == before


def test_a_rename_waits_out_a_lazily_released_handle(tmp_path):
    src, dst = tmp_path / "a", tmp_path / "b"
    src.mkdir()
    real_rename = os.rename
    refusals = []

    def flaky(s, d):
        if len(refusals) < 2:
            refusals.append(1)
            raise PermissionError("in use")
        real_rename(s, d)

    with patch("shared.index_swap.os.rename", side_effect=flaky), \
            patch("shared.index_swap.time.sleep"):
        index_swap._rename_retry(str(src), str(dst))
    assert dst.is_dir() and len(refusals) == 2


# --- a cancel or a failure mid-build --------------------------------------------------------

def test_a_cancel_at_the_first_page_leaves_the_live_index_and_nothing_staged(live):
    root, eng = live
    with pytest.raises(IndexBuildCancelled):
        build(root, B, beside=True, cancel=lambda: True)
    assert not os.path.exists(index_swap.staged_index_path(root))
    assert not os.path.exists(index_swap.staged_browse_map_path(_browse_map(root)))
    assert _count(eng, "ראובן") == 1


def test_a_cancel_after_the_pages_stops_before_the_commit(live):
    root, eng = live
    seen, flag = [], []

    def phase(name, done, total):
        seen.append(name)
        if name == 'aggregates':
            flag.append(1)

    with pytest.raises(IndexBuildCancelled):
        build(root, B, beside=True, cancel=lambda: bool(flag), phase_callback=phase)
    assert 'aggregates' in seen and 'commit' not in seen
    assert not os.path.exists(index_swap.staged_index_path(root))
    assert _count(eng, "ראובן") == 1 and _count(eng, "לוי") == 0


def test_a_failure_mid_build_leaves_the_live_index_and_nothing_staged(live):
    root, eng = live

    def boom(name, done, total):
        if name == 'seams':
            raise RuntimeError("disk gone")

    with pytest.raises(RuntimeError, match="disk gone"):
        build(root, B, beside=True, phase_callback=boom)
    assert not os.path.exists(index_swap.staged_index_path(root))
    assert _count(eng, "ראובן") == 1 and eng.index_format_status()["current"]


def test_an_in_place_build_still_replaces_the_index_as_before(tmp_path):
    root = build(tmp_path, A)
    build(root, B)
    assert not os.path.exists(index_swap.staged_index_path(root))
    eng = engine(root)
    try:
        assert _count(eng, "לוי") == 1
    finally:
        eng.close_index()


def test_the_phases_are_reported_in_order_with_their_totals(tmp_path):
    seen = []
    build(tmp_path, B, phase_callback=lambda n, d, t: seen.append((n, d, t)))
    names = [n for n, _, _ in seen]
    assert names[0] == 'aggregates' and names[-1] == 'finish'
    assert [n for n in ('aggregates', 'commit', 'seams', 'finish') if n in names] == ['aggregates', 'commit', 'seams', 'finish']
    assert sorted(set(names), key=names.index) == ['aggregates', 'commit', 'seams', 'finish']
    agg = [(d, t) for n, d, t in seen if n == 'aggregates']
    assert agg[0] == (0, 2) and agg[-1] == (2, 2)                 # two manuscripts, no parts
    seams = [(d, t) for n, d, t in seen if n == 'seams']
    assert seams[0][0] == 0 and seams[-1][0] == seams[-1][1] >= 1


# --- what the next start does with what a crash left ----------------------------------------

def test_a_half_built_staged_index_is_discarded_at_the_next_start(live):
    root, eng = live
    staged = index_swap.staged_index_path(root)
    os.makedirs(staged)
    open(os.path.join(staged, "meta.json"), "w").write("{}")
    open(index_swap.staged_browse_map_path(_browse_map(root)), "wb").write(b"x")
    actions = index_swap.recover_index_dirs(root, _browse_map(root))
    assert actions == ['discarded-staged', 'discarded-staged-browse-map']
    assert not os.path.exists(staged)
    assert not os.path.exists(index_swap.staged_browse_map_path(_browse_map(root)))
    assert _count(eng, "ראובן") == 1                      # the live index, untouched
    gone = index_swap.remove_discarded(root)
    assert len(gone) == 1 and index_swap.discarded_dirs(root) == []


def test_an_interrupted_swap_is_finished_at_the_next_start(live):
    root, eng = live
    build(root, B, beside=True)
    eng.close_index()
    # The crash came after the first rename and before the second.
    os.rename(index_swap.live_index_path(root), index_swap.old_index_path(root))
    actions = index_swap.recover_index_dirs(root, _browse_map(root))
    assert actions == ['finished-swap', 'discarded-old']
    assert _reload(eng, root) and _count(eng, "לוי") == 1
    assert not os.path.exists(index_swap.staged_browse_map_path(_browse_map(root)))
    assert not os.path.exists(index_swap.old_index_path(root))


def test_an_old_folder_without_a_live_index_is_restored(live):
    root, eng = live
    eng.close_index()
    os.rename(index_swap.live_index_path(root), index_swap.old_index_path(root))
    assert index_swap.recover_index_dirs(root, _browse_map(root)) == ['restored-old']
    assert _reload(eng, root) and _count(eng, "ראובן") == 1


def test_an_old_folder_beside_a_live_index_is_discarded(live):
    root, eng = live
    old = index_swap.old_index_path(root)
    os.makedirs(old)
    assert index_swap.recover_index_dirs(root, _browse_map(root)) == ['discarded-old']
    assert not os.path.exists(old) and len(index_swap.discarded_dirs(root)) == 1
    assert index_swap.recover_index_dirs(root, _browse_map(root)) == []
    assert index_swap.delete_leftovers_in_background(root).join(5) is None
    assert index_swap.discarded_dirs(root) == []
    assert index_swap.delete_leftovers_in_background(root) is None


def test_a_leftover_staged_build_is_removed_before_a_new_one_starts(live):
    root, eng = live
    staged = index_swap.staged_index_path(root)
    os.makedirs(staged)
    open(os.path.join(staged, "junk"), "w").write("x")
    build(root, B, beside=True)
    assert not os.path.exists(os.path.join(staged, "junk"))
    assert os.path.isfile(os.path.join(staged, "meta.json"))


# --- the FL-id lookup after the browse map was replaced -------------------------------------

def _wait_fl_build(eng, seconds=15):
    import time
    deadline = time.time() + seconds
    while eng._fl_id_index_building and time.time() < deadline:
        time.sleep(0.05)
    assert not eng._fl_id_index_building


def test_the_fl_lookup_is_rebuilt_from_the_new_browse_map(live):
    # Codex round 2 on PR #446: Browse by FL id answered from the old map's offsets.
    root, eng = live
    from shared.search_engine import SearchEngine
    kept = SearchEngine._shared_browse_map
    SearchEngine._shared_browse_map = None
    try:
        with patch.object(Config, "BROWSE_MAP", _browse_map(root)):
            assert eng._build_fl_id_index() is True
            before = dict(eng._fl_id_index)
            assert before and all(sid == "9900000001" for pairs in before.values() for sid, _ in pairs)
            build(root, B, beside=True)
            eng.close_index()
            index_swap.swap_in_staged_index(root, _browse_map(root))
            _reload(eng, root)
            eng.browse_map_replaced()
            _wait_fl_build(eng)
            after = eng._fl_id_index
        assert after is not None and after != before
        assert any(sid == "9900000002" for pairs in after.values() for sid, _ in pairs)
    finally:
        SearchEngine._shared_browse_map = kept


def test_a_lookup_built_from_a_map_replaced_meanwhile_is_not_kept(live):
    root, eng = live
    from shared.search_engine import SearchEngine
    kept = SearchEngine._shared_browse_map
    SearchEngine._shared_browse_map = None
    real_load = eng._load_browse_map

    def load_and_replace():
        browse_map = real_load()
        eng._fl_id_index_generation += 1            # the swap landed while this build read
        return browse_map

    try:
        with patch.object(Config, "BROWSE_MAP", _browse_map(root)), \
                patch.object(eng, "_load_browse_map", load_and_replace):
            assert eng._build_fl_id_index() is False
            assert eng._fl_id_index is None
    finally:
        SearchEngine._shared_browse_map = kept


# --- the free-space plan --------------------------------------------------------------------

class _Usage:
    def __init__(self, free):
        self.free = free


def test_no_live_index_means_a_fresh_build(tmp_path):
    plan = index_swap.plan_build(str(tmp_path))
    assert plan.mode == 'fresh' and plan.live == 0 and plan.needed == 0
    os.makedirs(index_swap.live_index_path(str(tmp_path)))
    assert index_swap.plan_build(str(tmp_path)).mode == 'fresh'      # an empty folder is no index


def test_the_plan_needs_the_live_size_plus_the_headroom(tmp_path):
    root = build(tmp_path, A)
    size = index_swap.dir_size(index_swap.live_index_path(root))
    assert size > 0
    with patch("shared.index_swap.shutil.disk_usage", return_value=_Usage(size + 1000 - 1)):
        plan = index_swap.plan_build(root, headroom=1000)
    assert plan.mode == 'inplace' and plan.needed == size + 1000 and plan.free == size + 999
    with patch("shared.index_swap.shutil.disk_usage", return_value=_Usage(size + 1000)):
        assert index_swap.plan_build(root, headroom=1000).mode == 'beside'
    assert index_swap.BESIDE_BUILD_HEADROOM == int(1.5 * 1024 ** 3)
    assert index_swap.gib(int(1.5 * 1024 ** 3)) == "1.5"


def test_free_space_is_read_from_the_nearest_existing_parent(tmp_path):
    missing = os.path.join(str(tmp_path), "not", "yet")
    assert index_swap.free_bytes(missing) == shutil.disk_usage(str(tmp_path)).free


def test_build_index_dot_py_still_builds_in_place_without_the_flag(tmp_path):
    """build_index.py calls create_index without *beside*: the in-place build it has
    always run (the server builds into a relocated INDEX_DIR, not a staged folder).
    Read as text, never imported: importing build_index.py creates ``Genizah_Index``
    beside the code, which the app then prefers over the real index."""
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(here, "build_index.py"), encoding="utf-8") as f:
        src = f.read()
    assert "create_index(progress_callback=progress_callback, **kwargs)" in src
    assert "beside" not in src
    assert not os.path.isdir(os.path.join(here, "Genizah_Index.made-by-this-test"))
    root = build(tmp_path, A)
    with patch.object(Config, "INDEX_DIR", root):
        assert index_swap.live_index_path() == os.path.join(root, "tantivy_db")
