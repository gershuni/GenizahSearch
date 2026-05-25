# -*- coding: utf-8 -*-
"""Phase 96-09 iteration 10 regression tests.

Root cause (fix-10):
  _restoring_session was only set to True INSIDE _restore_session(), not at
  __init__ time.  Any background event (rescan completion triggering
  populate_for_folder, debounce timer, or the direct _save_session() call
  added in iter-9 to _on_corpus_scope_changed) that fired during the 200ms
  startup window between __init__ and the QTimer.singleShot(200, …) could
  call _save_session() with empty last_results, overwriting the valid on-disk
  session.  On the next startup, load_session_state() would return the
  overwritten file (results=[]), has_data=False, and the 'ask' prompt would
  never appear — and opt-outs would be absent from both memory and the tree.

Fix:
  1. Set self._restoring_session = True in __init__ (before init_ui).
  2. Guard _save_session() with an early return when _restoring_session=True.
  _restore_session()'s finally block already resets it to False.

Tests:
  F10-A  — _save_session() returns immediately when _restoring_session=True
  F10-B  — _save_session() proceeds normally when _restoring_session=False
  F10-C  — GenizahGUI.__init__ sets _restoring_session=True before init_ui
  F10-D  — AST guard: _restoring_session=True appears before init_ui() call
  F10-E  — restore_mode='ask' with has_data=True: opt-outs and corpus
            restored even on the code path that previously broke (structural
            check that _apply_persistent_session_preferences is called before
            the has_data gate and before the 'ask' prompt block).
"""
import ast
import types
import pytest


# ---------------------------------------------------------------------------
# F10-A  — _save_session skips when _restoring_session=True
# ---------------------------------------------------------------------------

def test_save_session_skipped_during_restore(monkeypatch):
    """_save_session() must NOT write to disk while _restoring_session=True.

    This prevents background events (rescan callback, debounce timer, corpus
    scope combo signal) from overwriting the valid on-disk session during the
    startup window before _restore_session() reads it.
    """
    genizah_app = pytest.importorskip("genizah_app")
    import shared.session_persistence as session_persistence

    write_calls = []
    monkeypatch.setattr(
        session_persistence,
        "save_session_state",
        lambda state: write_calls.append(state) or True,
    )

    fake = types.SimpleNamespace(
        _restoring_session=True,
        _local_file_optouts={"some/path.pdf"},
        _search_corpus_scope="genizah",
        _is_browsing_local=lambda: False,
    )
    genizah_app.GenizahGUI._save_session(fake)

    assert len(write_calls) == 0, (
        "_save_session() must not write to disk when _restoring_session=True "
        "(fix-10: startup window race condition guard)"
    )


# ---------------------------------------------------------------------------
# F10-B  — _save_session proceeds when _restoring_session=False
# ---------------------------------------------------------------------------

def test_save_session_proceeds_after_restore(monkeypatch):
    """_save_session() must write normally once _restoring_session=False."""
    genizah_app = pytest.importorskip("genizah_app")
    import shared.session_persistence as session_persistence

    write_calls = []
    monkeypatch.setattr(
        session_persistence,
        "save_session_state",
        lambda state: write_calls.append(state) or True,
    )

    fake = types.SimpleNamespace(
        _restoring_session=False,
        _local_file_optouts={"some/path.pdf"},
        _search_corpus_scope="genizah",
        _is_browsing_local=lambda: False,
    )
    monkeypatch.setattr(genizah_app, "load_app_config", lambda: {"restore_mode": "ask"})

    genizah_app.GenizahGUI._save_session(fake)

    assert len(write_calls) == 1, (
        "_save_session() must write to disk when _restoring_session=False "
        "(fix-10: normal post-restore save path must not be blocked)"
    )


# ---------------------------------------------------------------------------
# F10-C  — restore_mode='ask' path: opt-outs applied before has_data gate
# ---------------------------------------------------------------------------

def test_restore_mode_ask_applies_prefs_before_has_data(monkeypatch):
    """restore_mode='ask' with results: _apply_persistent_session_preferences
    must be called (opt-outs + corpus set) even on paths where has_data=False.

    This is the structural correctness test for the 'ask' restore code path.
    We monkeypatch QMessageBox to auto-accept so the test is headless.
    """
    genizah_app = pytest.importorskip("genizah_app")
    import shared.session_persistence as session_persistence

    # Session with opt-outs but no search results (has_data=False path).
    saved_state = {
        "version": 1,
        "local_file_optouts": [r"c:\users\h\docs\scan.pdf"],
        "local_browse_view_mode": "per_page",
        "local_browse_sys_id": None,
        "local_browse_p_num": None,
        "regular_search": {
            "search_corpus_scope": "local",
            "results": [],
            "query": "",
            "mode_index": 0,
            "gap": 0,
            "text_position": 0,
            "variant_preset": 70,
            "domain_exclusions": [],
            "printed_filter": "all",
            "local_filter": "all",
            "printed_ids": [],
            "excluded_sys_ids": [],
            "excluded_shelfmarks": [],
            "excluded_raw_entries": [],
            "exclusion_sources": [],
            "results_filters": {},
            "filter_sources": {},
            "filter_enabled_sources": [],
            "refinement_chain": [],
        },
        "composition_search": {
            "results": [],
            "filtered_results": [],
            "source_text": "",
            "title": "",
            "chunk_size": 5,
            "max_freq": 10,
            "mode_index": 0,
            "domain_exclusions": [],
            "printed_filter": "all",
            "local_filter_composition": "all",
            "local_filter_parallels": "all",
            "excluded_sys_ids": [],
            "excluded_shelfmarks": [],
            "sort_mode": "score",
            "sort_reverse": True,
            "flat_mode": False,
            "appendix_threshold": 5,
            "summary_text": "",
        },
        "was_interrupted": False,
        "pre_search_filters": {},
        "post_measurement_filters": {},
        "word_excluded_sys_ids": [],
        "active_tab": 0,
        "browse_shelfmark": {
            "sys_id": "",
            "shelfmark": "",
            "fl_id": "",
            "last_field": "shelf",
            "volume_ie": None,
        },
        "browse_catalog": {
            "domain": None,
            "author": None,
            "work": None,
            "date_from": None,
            "date_to": None,
            "include_undated": False,
            "text_all": [],
            "text_any": [],
            "text_not": [],
        },
    }
    monkeypatch.setattr(
        genizah_app,
        "load_app_config",
        lambda: {"restore_mode": "ask"},
    )
    monkeypatch.setattr(
        session_persistence,
        "load_session_state",
        lambda: saved_state,
    )

    class ComboStub:
        def __init__(self):
            self.values = ["genizah", "local", "all"]
            self.index = 0
            self.blocked = False

        def findData(self, value):
            return self.values.index(value) if value in self.values else -1

        def blockSignals(self, blocked):
            self.blocked = blocked

        def setCurrentIndex(self, index):
            self.index = index

        def currentData(self):
            return self.values[self.index]

    class ProgressStub:
        def setVisible(self, _v):
            pass

        def setRange(self, *a):
            pass

        def setValue(self, *a):
            pass

        def setFormat(self, *a):
            pass

    class MyLibStub:
        def __init__(self):
            self.notified = False

        def notify_session_restored(self):
            self.notified = True

    fake = types.SimpleNamespace(
        _restoring_session=False,
        corpus_scope_combo=ComboStub(),
        search_progress=ProgressStub(),
        my_library_tab=MyLibStub(),
    )
    fake._apply_persistent_session_preferences = types.MethodType(
        genizah_app.GenizahGUI._apply_persistent_session_preferences,
        fake,
    )

    genizah_app.GenizahGUI._restore_session(fake)

    # With has_data=False (no results), restore returns early — but prefs
    # must still have been applied by _apply_persistent_session_preferences.
    assert fake._local_file_optouts == {r"c:\users\h\docs\scan.pdf"}, (
        "fix-10: opt-outs must be applied even on the has_data=False early-return "
        "path (restore_mode='ask').  If _apply_persistent_session_preferences is "
        "called before the has_data gate, this will pass."
    )
    assert fake._search_corpus_scope == "local", (
        "fix-10: corpus scope must be applied before the has_data gate "
        "(restore_mode='ask')"
    )
    assert fake.my_library_tab.notified, (
        "fix-10: notify_session_restored() must fire in the finally block even "
        "on the has_data=False early-return path so the folder tree is populated "
        "with correct opt-out checkboxes."
    )


# ---------------------------------------------------------------------------
# F10-D  — AST guard: _restoring_session=True is set in __init__ before init_ui
# ---------------------------------------------------------------------------

def test_restoring_session_set_true_in_init_before_init_ui():
    """GenizahGUI.__init__ must set _restoring_session=True before calling
    init_ui().  This closes the 200ms startup window race condition.

    AST check: find the first assignment to self._restoring_session inside
    __init__ and verify it comes before the call to self.init_ui().
    """
    try:
        import genizah_app as _ga
        with open(_ga.__file__, encoding="utf-8") as fh:
            src = fh.read()
    except ImportError:
        pytest.skip("genizah_app not importable")

    tree = ast.parse(src)

    init_fn = None
    for node in ast.walk(tree):
        # Find GenizahGUI.__init__
        if isinstance(node, ast.ClassDef) and node.name == "GenizahGUI":
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.name == "__init__":
                    init_fn = item
                    break
        if init_fn is not None:
            break

    assert init_fn is not None, "GenizahGUI.__init__ not found"

    fn_src = ast.unparse(init_fn)

    # Find positions of the assignment and the init_ui() call.
    assign_pos = fn_src.find("_restoring_session = True")
    init_ui_pos = fn_src.find("self.init_ui()")

    assert assign_pos != -1, (
        "fix-10: `_restoring_session = True` not found in GenizahGUI.__init__ — "
        "the startup-window guard was not applied."
    )
    assert init_ui_pos != -1, (
        "self.init_ui() not found in GenizahGUI.__init__"
    )
    assert assign_pos < init_ui_pos, (
        "fix-10: `_restoring_session = True` must appear BEFORE `self.init_ui()` "
        f"in GenizahGUI.__init__ (got assign_pos={assign_pos}, init_ui_pos={init_ui_pos}).  "
        "Without this ordering, signals connected during init_ui (e.g. corpus combo) "
        "can fire _save_session() before _restore_session() reads the session file."
    )


# ---------------------------------------------------------------------------
# F10-E  — _save_session guard: early return when _restoring_session=True
# ---------------------------------------------------------------------------

def test_save_session_has_restoring_guard():
    """_save_session() must contain an early-return guard that checks
    _restoring_session before writing anything to disk.

    AST check: verify the guard pattern is present.
    """
    try:
        import genizah_app as _ga
        with open(_ga.__file__, encoding="utf-8") as fh:
            src = fh.read()
    except ImportError:
        pytest.skip("genizah_app not importable")

    tree = ast.parse(src)

    save_fn = None
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "_save_session":
            save_fn = node
            break

    assert save_fn is not None, "_save_session not found"

    fn_src = ast.unparse(save_fn)

    assert "_restoring_session" in fn_src, (
        "fix-10: _save_session() must check _restoring_session before writing — "
        "guard not found in function body."
    )

    # The guard must come before any save_session_state call
    guard_pos = fn_src.find("_restoring_session")
    save_call_pos = fn_src.find("save_session_state(")
    assert guard_pos < save_call_pos, (
        "fix-10: _restoring_session guard must appear BEFORE the save_session_state() "
        "call in _save_session()."
    )
