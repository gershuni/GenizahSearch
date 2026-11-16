# -*- coding: utf-8 -*-
"""The desktop builds, promotes and opens the scholarly transcriptions' index (editions plan
section 3.7, decision D3; PR 4a): desktop/editions_index.py, its QThread passes, and the
main engine searching with ``sources`` once the index is installed.

A small main index built by the real Indexer (tests/responsa_index_fixture.py), small FGP and
PGP sidecars and an NLI crossref written here (tests/test_editions_indexer.py's builders) --
never the real ones, and never the real INDEX_DIR: every lifecycle is given its own folder.
"""
import ast
import os
import shutil
import sqlite3
import threading
import time
from unittest.mock import patch

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest  # noqa: E402

tantivy = pytest.importorskip("tantivy")

from PyQt6.QtWidgets import QApplication  # noqa: E402

import desktop.editions_index as ei  # noqa: E402
from desktop.editions_index import EditionsIndex, EditionsInputs  # noqa: E402
from desktop.variant_run_settings import engine_with_variant_settings  # noqa: E402
from shared.config import Config  # noqa: E402
from shared.editions_indexer import BuildCancelled, META_FILENAME, build_generation, read_generation  # noqa: E402
from shared.editions_search import check_generation  # noqa: E402
from shared.search_engine import SearchEngine, consume_editions_unavailable  # noqa: E402
from tests.responsa_index_fixture import build, engine as make_engine, uid  # noqa: E402
from tests.test_editions_indexer import MS_A, MS_F, make_fgp, make_pgp  # noqa: E402

pytestmark = pytest.mark.gui  # QThreads and their queued signals: the gui bucket

_APP = QApplication.instance() or QApplication([])

P_F = uid(600, 1)
MAIN = [
    (MS_A, 100, 1, "שלום לכל אחינו הזקנים"),
    (MS_A, 100, 2, "קדשך שבעו עמים"),
    (MS_F, 600, 1, "בית דין ואמא שלום"),
]


def _make_nli(path):
    """An NLI crossref with its image table (the build checks it: check_nli_crossref)."""
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE nli_images (NLI_AlmaId TEXT, FGPImageNumberId TEXT, FGPNumber TEXT, "
                "ImageName TEXT, ImageSourceName TEXT, Shelfmark TEXT)")
    con.executemany("INSERT INTO nli_images VALUES (?,?,?,?,?,?)",
                    [(MS_A, "11", "F1", "X_L1r.jpg", "x", "T-S 1"),
                     (MS_A, "12", "F1", "X_L1v.jpg", "x", "T-S 1")])
    con.commit()
    con.close()


@pytest.fixture(scope="module")
def main_root(tmp_path_factory):
    root = build(tmp_path_factory.mktemp("editions-desktop") / "main", MAIN)
    browse = patch.object(Config, "BROWSE_MAP", os.path.join(root, "browse_map.pkl"))
    browse.start()
    SearchEngine._shared_browse_map = None
    try:
        yield root
    finally:
        SearchEngine._shared_browse_map = None
        browse.stop()


@pytest.fixture
def world(main_root, tmp_path):
    """Fresh sidecars and an empty folder for the generation, and a main engine with no
    editions open."""
    fgp, pgp, nli = str(tmp_path / "fgp.db"), str(tmp_path / "pgp.db"), str(tmp_path / "nli.db")
    make_fgp(fgp)
    make_pgp(pgp)
    _make_nli(nli)
    inputs = EditionsInputs(fgp, pgp, os.path.join(main_root, "browse_map.pkl"), nli)
    eng = make_engine(main_root)
    w = {"engine": eng, "inputs": inputs, "live": str(tmp_path / "index" / "Editions"), "tmp": tmp_path,
         "lifecycles": [], "root": main_root}
    try:
        yield w
    finally:
        for life in w["lifecycles"]:
            life.close()
            _settle(life)
        eng.close_index()


def _lifecycle(world, **kw):
    life = EditionsIndex(world["engine"], live_dir=world["live"], inputs=world["inputs"], **kw)
    world["lifecycles"].append(life)
    return life


def _settle(life, timeout=90.0):
    """Run the event loop until the lifecycle has nothing running or waiting."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        _APP.processEvents()
        if not life.busy() and life._pending is None and life._later is None:
            # The finished() slot ran; let the keeper's release run too.
            _APP.processEvents()
            return life.status
        time.sleep(0.01)
    raise AssertionError(f"the editions lifecycle did not settle: {life.status}")


def _search(engine, query, sources, mode="literal"):
    consume_editions_unavailable()
    rows = engine.execute_search(query, mode, 0, corpus_scope="genizah", sources=sources)
    return rows, consume_editions_unavailable()


def _edition_members(rows):
    return [m["uid"] for r in rows for m in (r.get("members") or []) if m.get("edition")]


class _BuildSpy:
    """build_generation, recording each call and the thread it ran on."""

    def __init__(self):
        self.calls = []

    def __call__(self, out_dir, **kw):
        self.calls.append({"out": out_dir, "main_thread": threading.current_thread() is threading.main_thread()})
        return build_generation(out_dir, **kw)


# --- first start: build, promote, open ---------------------------------------------------

def test_nothing_is_built_or_opened_at_construction(world):
    life = _lifecycle(world)
    _APP.processEvents()
    assert not life.busy() and life.status == "idle"
    assert not os.path.exists(world["live"]) and not os.path.exists(world["live"] + ".new")
    assert not world["engine"].editions_ready()


def test_first_start_builds_beside_promotes_and_opens_off_the_ui_thread(world):
    spy = _BuildSpy()
    with patch.object(ei, "build_generation", spy):
        life = _lifecycle(world)
        life.check()
        assert life.busy()                               # running in its thread
        assert _settle(life) == "ready"
    assert [c["out"] for c in spy.calls] == [world["live"] + ".new"]
    assert spy.calls[0]["main_thread"] is False          # never on the UI thread
    live = world["live"]
    assert check_generation(live)[0]
    assert not os.path.exists(live + ".new") and not os.path.exists(live + ".prev")
    engine = world["engine"]
    assert engine.editions_ready()
    assert engine._editions._editions_dir == live
    assert life.build_id == read_generation(live)[0]["build_id"]


def test_once_installed_the_engine_and_every_per_request_copy_search_the_editions(world):
    engine = world["engine"]
    rows, unavailable = _search(engine, "בית דין", "all")
    assert unavailable and _edition_members(rows) == []   # before: the main leg alone, said
    assert rows and all("members" not in r for r in rows)
    life = _lifecycle(world)
    life.check()
    assert _settle(life) == "ready"
    rows, unavailable = _search(engine, "בית דין", "all")
    assert not unavailable
    assert _edition_members(rows) == [f"ED:pgp:105:{MS_F}"]
    assert [r["uid"] for r in rows] == [P_F]              # the page and its edition: one result
    # The desktop runs each search on a copy of the engine bound to the run's variant
    # settings (desktop/variant_run_settings.py): the copy reads the editions too.
    from types import SimpleNamespace
    settings = SimpleNamespace(variant_pairs_count=150, variant_max_changes=3, variant_min_word_len=2,
                               variant_aggressive=False, custom_variants={})
    view = engine_with_variant_settings(engine, settings, {"variant_pairs_count": 30, "variant_max_changes": 1})
    assert view is not engine and view.var_mgr is not engine.var_mgr
    rows, unavailable = _search(view, "בית דין", "all")
    assert not unavailable and _edition_members(rows) == [f"ED:pgp:105:{MS_F}"]
    rows, _ = _search(view, "בית דין", "editions")
    assert [m["uid"] for r in rows for m in r["members"]] == [f"ED:pgp:105:{MS_F}"]
    rows, _ = _search(engine, "בית דין", "automatic")
    assert rows and all("members" not in r for r in rows)


# --- later starts: current, stale, built earlier -------------------------------------------

def test_a_current_generation_is_opened_not_rebuilt(world):
    inputs = world["inputs"]
    meta = build_generation(world["live"], fgp_db=inputs.fgp_db, pgp_db=inputs.pgp_db,
                            browse_map_path=inputs.browse_map, nli_db=inputs.nli_db, progress=lambda m: None)
    spy = _BuildSpy()
    with patch.object(ei, "build_generation", spy):
        life = _lifecycle(world)
        life.check()
        assert _settle(life) == "ready"
        assert spy.calls == []
        assert life.build_id == meta["build_id"] and world["engine"].editions_ready()
        # Checked again with the same generation open: nothing is opened a second time.
        child = world["engine"]._editions
        life.check()
        assert _settle(life) == "ready"
        assert world["engine"]._editions is child and spy.calls == []


def _add_pgp_edition(pgp_db):
    """A new edition in the PGP sidecar (a refresh): its file changes."""
    con = sqlite3.connect(pgp_db)
    con.execute("INSERT INTO document_sources (id, pgpid, doc_relation, content) VALUES "
                "(106, 4, 'Digital Edition', 'ונכתב בשנת זכריהו הסופר')")
    con.execute("INSERT INTO document_fragments (document_id, sys_id) VALUES (4, ?)", (MS_F,))
    con.commit()
    con.close()


def test_a_changed_sidecar_is_rebuilt_and_replaces_the_generation(world):
    life = _lifecycle(world)
    life.check()
    assert _settle(life) == "ready"
    first = life.build_id
    old_child = world["engine"]._editions
    assert _edition_members(_search(world["engine"], "זכריהו", "all")[0]) == []
    _add_pgp_edition(world["inputs"].pgp_db)
    spy = _BuildSpy()
    with patch.object(ei, "build_generation", spy):
        life.check()
        assert _settle(life) == "ready"
    assert len(spy.calls) == 1
    assert life.build_id != first
    assert read_generation(world["live"])[0]["build_id"] == life.build_id
    assert old_child.searcher is None                     # the old one was closed first
    assert _edition_members(_search(world["engine"], "זכריהו", "all")[0]) == [f"ED:pgp:106:{MS_F}"]
    live = world["live"]
    assert not os.path.exists(live + ".new") and not os.path.exists(live + ".prev")


def test_an_unchanged_input_is_not_read_again(world):
    """The check at each start fingerprints about 0.9 GB of inputs: a file whose size and time
    are those it was read at is not read again, and a changed one is."""
    life = _lifecycle(world)
    life.check()
    assert _settle(life) == "ready"
    reads = []
    real = ei.file_fingerprint

    def counted(path):
        reads.append(os.path.basename(path))
        return real(path)
    record = world["live"] + ".inputs.json"
    assert os.path.isfile(record)               # kept by the build's own check
    with patch.object(ei, "file_fingerprint", counted):
        life.check()
        assert _settle(life) == "ready" and reads == []
        os.remove(record)                       # no record: every input is read, and kept
        life.check()
        assert _settle(life) == "ready"
        assert sorted(reads) == ["browse_map.pkl", "fgp.db", "nli.db", "pgp.db"]
        reads.clear()
        life.check()
        assert _settle(life) == "ready" and reads == []
        first = life.build_id
        _add_pgp_edition(world["inputs"].pgp_db)
        life.check()
        assert _settle(life) == "ready"
    assert reads == ["pgp.db"] and life.build_id != first


def test_a_sidecar_replaced_during_the_build_is_not_put_in_place_and_is_built_again(world):
    """The build stamps the inputs as they were when it started; the PGP sidecar changes
    while it runs (a sidecar update). That build is stale already: removed, not promoted,
    and the lifecycle builds again from the new file (Codex review of PR #25)."""
    metas = []

    def build_then_update(out_dir, **kw):
        metas.append(build_generation(out_dir, **kw))
        if len(metas) == 1:
            _add_pgp_edition(world["inputs"].pgp_db)
        return metas[-1]
    with patch.object(ei, "build_generation", build_then_update):
        life = _lifecycle(world)
        life.check()
        assert _settle(life) == "ready"
    assert len(metas) == 2
    assert life.build_id == metas[1]["build_id"] != metas[0]["build_id"]
    assert read_generation(world["live"])[0]["build_id"] == metas[1]["build_id"]
    assert _edition_members(_search(world["engine"], "זכריהו", "all")[0]) == [f"ED:pgp:106:{MS_F}"]


def test_a_build_finished_in_an_earlier_session_is_promoted_without_building_again(world):
    inputs = world["inputs"]
    new = world["live"] + ".new"
    meta = build_generation(new, fgp_db=inputs.fgp_db, pgp_db=inputs.pgp_db, browse_map_path=inputs.browse_map,
                            nli_db=inputs.nli_db, progress=lambda m: None)
    spy = _BuildSpy()
    with patch.object(ei, "build_generation", spy):
        life = _lifecycle(world)
        life.check()
        assert _settle(life) == "ready"
    assert spy.calls == []
    assert life.build_id == meta["build_id"] and not os.path.exists(new)


def test_a_stale_build_does_not_count_against_the_free_space(world, monkeypatch):
    """An unfinished Editions.new (and a leftover .prev) are deleted before the free space is
    measured: their space is what the new build needs (Codex review of PR #25)."""
    new, prev = world["live"] + ".new", world["live"] + ".prev"
    for d in (new, prev):
        os.makedirs(os.path.join(d, "tantivy_db"))
    monkeypatch.setattr(ei, "_free_bytes", lambda path: ei.BUILD_HEADROOM - 1
                        if os.path.exists(new) or os.path.exists(prev) else ei.BUILD_HEADROOM)
    life = _lifecycle(world)
    life.check()
    assert _settle(life) == "ready", life.reason
    assert not os.path.exists(prev) and not os.path.exists(new)


def test_too_little_free_space_builds_nothing(world, monkeypatch):
    monkeypatch.setattr(ei, "_free_bytes", lambda path: ei.BUILD_HEADROOM - 1)
    life = _lifecycle(world)
    life.check()
    assert _settle(life) == "unavailable" and "free" in life.reason
    assert not os.path.exists(world["live"]) and not os.path.exists(world["live"] + ".new")


def test_an_unfinished_build_is_replaced(world):
    """A build stopped by a crash leaves a folder with no meta: never promoted, rebuilt."""
    os.makedirs(os.path.join(world["live"] + ".new", "tantivy_db"))
    life = _lifecycle(world)
    life.check()
    assert _settle(life) == "ready"
    assert check_generation(world["live"])[0] and not os.path.exists(world["live"] + ".new")


# --- promotion waits for the readers --------------------------------------------------------

def test_the_promotion_waits_while_a_search_reads_the_index(world, monkeypatch):
    monkeypatch.setattr(EditionsIndex, "PROMOTE_RETRY_MS", 20)
    busy = ["search"]
    life = _lifecycle(world, readers_busy=lambda: list(busy))
    deadline = time.monotonic() + 90
    life.check()
    while life.status != "waiting" and time.monotonic() < deadline:
        _APP.processEvents()
        time.sleep(0.01)
    assert life.status == "waiting"
    for _ in range(20):                                   # retries come and go: still waiting
        _APP.processEvents()
        time.sleep(0.01)
    assert life.status == "waiting" and life._pending == world["live"] + ".new"
    assert not os.path.exists(world["live"]) and not world["engine"].editions_ready()
    busy.clear()
    assert _settle(life) == "ready"
    assert world["engine"].editions_ready() and not os.path.exists(world["live"] + ".new")


# --- stopping: a main-index rebuild, the exit -------------------------------------------------

@pytest.fixture
def held_build():
    """build_generation that waits, once it has started, until the test lets it go on."""
    started, go = threading.Event(), threading.Event()

    def held(out_dir, **kw):
        started.set()
        assert go.wait(30)
        return build_generation(out_dir, **kw)
    with patch.object(ei, "build_generation", held):
        yield started, go


def test_a_main_index_rebuild_stops_the_build_and_its_end_checks_again(world, held_build):
    started, go = held_build
    life = _lifecycle(world)
    life.check()
    assert started.wait(30)
    life.suspend()                                        # _start_index_build
    go.set()
    assert _settle(life) == "cancelled"
    assert not os.path.exists(world["live"] + ".new") and not os.path.exists(world["live"])
    assert not world["engine"].editions_ready()
    life.check()                                          # asked while the main build runs
    assert not life.busy()
    life.resume()                                         # _end_index_build
    assert _settle(life) == "ready" and world["engine"].editions_ready()


def test_a_built_generation_waiting_for_its_promotion_is_dropped_by_a_main_index_rebuild(world, monkeypatch):
    monkeypatch.setattr(EditionsIndex, "PROMOTE_RETRY_MS", 20)
    busy = ["search"]
    life = _lifecycle(world, readers_busy=lambda: list(busy))
    life.check()
    deadline = time.monotonic() + 90
    while life.status != "waiting" and time.monotonic() < deadline:
        _APP.processEvents()
        time.sleep(0.01)
    life.suspend()
    busy.clear()
    for _ in range(20):
        _APP.processEvents()
        time.sleep(0.01)
    assert life._pending is None and not os.path.exists(world["live"])
    life.resume()                                         # the build is still current: promoted
    assert _settle(life) == "ready" and world["engine"].editions_ready()


def test_a_sidecar_update_during_a_main_index_build_waits_for_both(world):
    """Two reasons to wait (a main-index build, a sidecar update): the end of one alone
    starts nothing."""
    life = _lifecycle(world)
    life.suspend("main index")
    life.suspend("sidecars")
    life.check()
    life.resume("sidecars")
    _APP.processEvents()
    assert not life.busy() and not os.path.exists(world["live"])
    life.resume("main index")
    assert _settle(life) == "ready" and world["engine"].editions_ready()


def test_the_end_of_a_main_index_build_checks_even_when_its_start_was_missed(world):
    """A lifecycle made after the build began never heard suspend(); the build's end must
    still check the editions against the browse map it left (Codex review of PR #25)."""
    life = _lifecycle(world)
    life.resume("main index")
    assert life.busy()
    assert _settle(life) == "ready" and world["engine"].editions_ready()


def test_the_generation_built_from_replaced_inputs_is_closed_until_the_new_one_is_installed(world):
    """After a sidecar update (or a main-index rebuild) the open generation was built from the
    old files: a search asked for the editions must run without them and say so, not read the
    old text, until the rebuilt one is installed (Codex review of PR #25)."""
    life = _lifecycle(world)
    life.check()
    assert _settle(life) == "ready"
    engine = world["engine"]
    life.suspend("sidecars")
    _add_pgp_edition(world["inputs"].pgp_db)                # the update replaces the sidecar
    assert engine.editions_ready()                        # meanwhile, the open one serves
    life.resume("sidecars")
    assert not engine.editions_ready()                    # closed at once: nothing reads it
    rows, unavailable = _search(engine, "בית דין", "all")
    assert unavailable and _edition_members(rows) == []
    assert _settle(life) == "ready" and engine.editions_ready()
    assert _edition_members(_search(engine, "זכריהו", "all")[0]) == [f"ED:pgp:106:{MS_F}"]


def test_the_previous_generation_closes_only_when_no_search_reads_it(world, monkeypatch):
    monkeypatch.setattr(EditionsIndex, "PROMOTE_RETRY_MS", 20)
    busy = []
    life = _lifecycle(world, readers_busy=lambda: list(busy))
    life.check()
    assert _settle(life) == "ready"
    engine = world["engine"]
    child = engine._editions
    life.suspend("main index")
    busy.append("search")                                 # a search reads the open generation
    life.resume("main index")
    for _ in range(10):
        _APP.processEvents()
        time.sleep(0.01)
    assert engine._editions is child and child.searcher is not None and not life.busy()
    life.suspend("sidecars")                              # suspended again while it waits
    busy.clear()
    for _ in range(10):
        _APP.processEvents()
        time.sleep(0.01)
    assert engine._editions is child and not life.busy()
    life.resume("sidecars")                               # its end closes it, then checks
    assert child.searcher is None
    assert _settle(life) == "ready" and engine.editions_ready()


def test_the_main_index_reopen_does_not_reopen_the_editions_while_suspended(world):
    """The end of a main-index build reopens the main index (reload_index) on the UI thread
    before it resumes this lifecycle: the editions generation, about to be closed as stale,
    must not be reopened there; a background pass installs a checked one (Codex review of
    PR #25)."""
    import shared.editions_search as es
    life = _lifecycle(world)
    life.check()
    assert _settle(life) == "ready"
    engine = world["engine"]
    life.suspend("main index")
    assert engine.editions_ready()                        # the build runs: it keeps serving
    opened_on = []
    real_open = es.open_editions_engine

    def spy(*a, **kw):
        opened_on.append(threading.current_thread() is threading.main_thread())
        return real_open(*a, **kw)
    with patch.object(es, "open_editions_engine", spy), patch.object(ei, "open_editions_engine", spy):
        engine.close_index()                              # _install_staged_index: close, swap,
        with patch.object(Config, "INDEX_DIR", world["root"]):
            assert engine.reload_index()                  # ... and reopen the main index
        assert not engine.editions_ready() and opened_on == []
        life.resume("main index")
        assert _settle(life) == "ready" and engine.editions_ready()
    assert opened_on == [False]                           # opened once, in the background


@pytest.fixture
def held_open():
    """open_editions_engine that waits, once it has started, until the test lets it go on."""
    started, go = threading.Event(), threading.Event()
    real = ei.open_editions_engine

    def held(*a, **kw):
        started.set()
        assert go.wait(30)
        return real(*a, **kw)
    with patch.object(ei, "open_editions_engine", held):
        yield started, go


def test_a_generation_opened_while_suspended_is_not_installed(world, held_open):
    inputs = world["inputs"]
    build_generation(world["live"], fgp_db=inputs.fgp_db, pgp_db=inputs.pgp_db,
                     browse_map_path=inputs.browse_map, nli_db=inputs.nli_db, progress=lambda m: None)
    started, go = held_open
    life = _lifecycle(world)
    life.check()                                          # current: the pass opens it
    assert started.wait(30)
    life.suspend("main index")                            # a main-index build starts meanwhile
    go.set()
    _settle(life)
    assert not world["engine"].editions_ready() and life.status == "suspended"
    life.resume("main index")
    assert _settle(life) == "ready" and world["engine"].editions_ready()


def _failing_promotions(n):
    """promote_with_lease failing (as Windows' renames of a just-closed index can) *n* times."""
    real = ei.promote_with_lease
    calls = []

    def promote(new, live):
        calls.append(1)
        if len(calls) <= n:
            raise PermissionError("the folder is in use")
        return real(new, live)
    return promote, calls


def test_a_failed_promotion_never_reopens_the_stale_generation_and_tries_again(world, monkeypatch):
    """After an input was replaced the generation at Editions is stale: a promotion whose
    renames all fail must not reopen it; the checked build is kept and put in place later
    (Codex review of PR #25)."""
    monkeypatch.setattr(ei, "PROMOTE_RETRY_S", 0)
    monkeypatch.setattr(EditionsIndex, "PROMOTE_LATER_MS", 20)
    life = _lifecycle(world)
    life.check()
    assert _settle(life) == "ready"
    stale = life.build_id
    engine = world["engine"]
    installed = []
    real_install = engine.install_editions

    def spy(child, gen_dir=None):
        installed.append(read_generation(gen_dir)[0]["build_id"])
        return real_install(child, gen_dir)
    monkeypatch.setattr(engine, "install_editions", spy)
    promote, calls = _failing_promotions(ei.PROMOTE_ATTEMPTS)    # one whole pass fails
    monkeypatch.setattr(ei, "promote_with_lease", promote)
    life.suspend("sidecars")
    _add_pgp_edition(world["inputs"].pgp_db)
    life.resume("sidecars")
    assert _settle(life, timeout=60) == "ready"
    assert len(calls) == ei.PROMOTE_ATTEMPTS + 1
    assert installed == [life.build_id] and life.build_id != stale
    assert not os.path.exists(world["live"] + ".new")


@pytest.mark.parametrize("intervening", ["check", "suspend and resume"])
def test_a_later_try_is_dropped_when_another_pass_puts_the_build_in_place(world, monkeypatch, intervening):
    """A failed promotion's later try must not fire after a check (or a suspend and resume)
    has put the kept build in place meanwhile: it would close the good index and promote a
    build that is gone (Codex review of PR #25)."""
    monkeypatch.setattr(ei, "PROMOTE_RETRY_S", 0)
    monkeypatch.setattr(EditionsIndex, "PROMOTE_LATER_MS", 400)
    promote, calls = _failing_promotions(ei.PROMOTE_ATTEMPTS)    # the first pass fails
    monkeypatch.setattr(ei, "promote_with_lease", promote)
    life = _lifecycle(world)
    life.check()
    deadline = time.monotonic() + 60
    while life.status != "retrying" and time.monotonic() < deadline:
        _APP.processEvents()
        time.sleep(0.01)
    assert life.status == "retrying"
    if intervening == "check":
        life.check()
    else:
        life.suspend("sidecars")
        life.resume("sidecars")
    assert _settle(life) == "ready" and world["engine"].editions_ready()
    child = world["engine"]._editions
    end = time.monotonic() + 1.0                          # past the later try's time
    while time.monotonic() < end:
        _APP.processEvents()
        time.sleep(0.02)
    assert world["engine"]._editions is child and child.searcher is not None
    assert len(calls) == ei.PROMOTE_ATTEMPTS + 1 and life.status == "ready"


def test_a_promotion_that_keeps_failing_leaves_the_editions_unavailable(world, monkeypatch):
    monkeypatch.setattr(ei, "PROMOTE_RETRY_S", 0)
    monkeypatch.setattr(EditionsIndex, "PROMOTE_LATER_MS", 20)
    promote, calls = _failing_promotions(10 ** 6)
    monkeypatch.setattr(ei, "promote_with_lease", promote)
    life = _lifecycle(world)
    life.check()
    deadline = time.monotonic() + 60
    while len(calls) < EditionsIndex.PROMOTE_LATER_TRIES * ei.PROMOTE_ATTEMPTS and time.monotonic() < deadline:
        _APP.processEvents()
        time.sleep(0.01)
    assert _settle(life) == "failed"
    for _ in range(10):                                   # no further try is scheduled
        _APP.processEvents()
        time.sleep(0.03)
    assert len(calls) == EditionsIndex.PROMOTE_LATER_TRIES * ei.PROMOTE_ATTEMPTS
    assert not world["engine"].editions_ready()
    assert os.path.isdir(world["live"] + ".new")          # kept: the next start puts it in place


def test_the_exit_stops_the_build_and_installs_nothing(world, held_build):
    started, go = held_build
    life = _lifecycle(world)
    life.check()
    assert started.wait(30)
    life.close()                                          # closeEvent
    go.set()
    _settle(life)
    assert life.status != "ready"
    assert not os.path.exists(world["live"] + ".new") and not os.path.exists(world["live"])
    assert not world["engine"].editions_ready()
    life.check()
    assert not life.busy()


def test_the_build_stops_when_asked():
    """shared/editions_indexer.py: should_stop ends the build before anything reads as a
    generation (the desktop's stop at exit and at a main-index rebuild)."""
    from tests.test_editions_indexer import IMAGES
    import tempfile
    d = tempfile.mkdtemp()
    try:
        fgp, pgp, bm = os.path.join(d, "fgp.db"), os.path.join(d, "pgp.db"), os.path.join(d, "bm.pkl")
        make_fgp(fgp)
        make_pgp(pgp)
        import pickle
        with open(bm, "wb") as f:
            pickle.dump({}, f)
        asked = []

        def stop():
            asked.append(1)
            return len(asked) > 2
        out = os.path.join(d, "Editions.new")
        with pytest.raises(BuildCancelled):
            build_generation(out, fgp_db=fgp, pgp_db=pgp, browse_map_path=bm,
                             folio_images_for=lambda s: [dict(i) for i in IMAGES.get(s, [])],
                             progress=lambda m: None, should_stop=stop)
        assert not os.path.exists(os.path.join(out, META_FILENAME))
    finally:
        shutil.rmtree(d, ignore_errors=True)


# --- inputs ---------------------------------------------------------------------------------

def test_a_missing_input_builds_nothing(world):
    os.remove(world["inputs"].nli_db)
    life = _lifecycle(world)
    life.check()
    assert _settle(life) == "unavailable" and "nli_db" in life.reason
    assert not os.path.exists(world["live"] + ".new") and not os.path.exists(world["live"])


def test_an_empty_placeholder_counts_as_missing(tmp_path):
    empty = tmp_path / "pgp.db"
    empty.write_bytes(b"")
    present = tmp_path / "x"
    present.write_bytes(b"1")
    assert EditionsInputs(str(present), str(empty), str(present), None).missing() == ["pgp_db", "nli_db"]


# --- the search thread and the main window --------------------------------------------------

def test_the_search_thread_passes_the_source_choice_and_says_when_the_editions_were_not_read(world):
    from desktop.gui_threads import SearchThread

    def run(sources):
        t = SearchThread(world["engine"], "בית דין", "literal", 0, corpus_scope="genizah", sources=sources)
        got = {}
        t.editions_signal.connect(lambda v: got.setdefault("unavailable", v))
        t.results_signal.connect(lambda rows: got.setdefault("rows", rows))
        t.run()                     # this thread: the signals are delivered directly
        return got
    before = run("all")
    assert before["unavailable"] is True and _edition_members(before["rows"]) == []
    life = _lifecycle(world)
    life.check()
    assert _settle(life) == "ready"
    after = run("all")
    assert after["unavailable"] is False and _edition_members(after["rows"]) == [f"ED:pgp:105:{MS_F}"]
    today = run(None)               # no choice: today's search, nothing said
    assert today["unavailable"] is False and _edition_members(today["rows"]) == []


def _window_function(name):
    import genizah_app
    with open(genizah_app.__file__, encoding="utf-8") as f:
        tree = ast.parse(f.read())
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "GenizahGUI")
    return ast.unparse(next(f for f in cls.body if isinstance(f, ast.FunctionDef) and f.name == name))


def test_startup_schedules_the_first_check_and_never_runs_it_in_place():
    src = _window_function("on_startup_finished")
    assert "EditionsIndex(lambda: self.searcher, readers_busy=self._index_readers_busy" in src
    # Made before the "Index Missing" offer, which can start a main-index build that must
    # suspend it (Codex review of PR #25).
    assert src.index("EditionsIndex(") < src.index("self.run_indexing()")
    assert "QTimer.singleShot(self._EDITIONS_CHECK_DELAY_MS, self._editions_index.check)" in src
    assert "self._editions_index.check()" not in src


def test_the_exit_stops_the_editions_index():
    src = _window_function("closeEvent")
    assert "editions = getattr(self, '_editions_index', None)" in src and "editions.close()" in src


def test_the_search_sends_its_source_choice_and_its_results_keep_the_note():
    """The window's half of the note: each run starts without it, its thread's signal sets
    it, and the rows it shows keep it (the line itself: tests/test_search_cutoff_empty_status.py)."""
    import genizah_app
    with open(genizah_app.__file__, encoding="utf-8") as f:
        src = f.read()
    assert "_run_sources = self._search_sources()" in src and "sources=_run_sources)" in src
    assert "self.search_thread.editions_signal.connect(_live(self._on_editions_unavailable))" in src
    assert "self._run_editions_unavailable = False" in src
    assert ("self._results_editions_unavailable = bool(getattr(self, '_run_editions_unavailable', False))"
            in _window_function("on_search_finished"))
