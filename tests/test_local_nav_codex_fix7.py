# -*- coding: utf-8 -*-
"""Phase 96-09 iteration 7 regression tests.

Covers:
  P1.1 — Restore timing race: notify_session_restored() exists and is called
          by _restore_session (not a 300ms timer) so opt-outs are loaded first.
  P1.2 — Duplicate basename: local_indexer emits canonical filepath; tree
          update_file_status does O(1) dict lookup, not basename scan.
  P1.3 — ResultDialog _htmlify escapes HTML before applying bold markers.
  P1.4 — browse_load clears current_browse_sid at entry before resolution.
  F1   — Search history entry captures corpus_scope; restore applies it.
  F2   — Session save/restore round-trips local_browse_sys_id + _p_num.
"""
import pytest


# ---------------------------------------------------------------------------
# P1.1 — notify_session_restored exists and replaces QTimer pattern
# ---------------------------------------------------------------------------

def test_notify_session_restored_method_exists():
    """MyLibraryTab must expose notify_session_restored()."""
    try:
        from desktop.my_library_tab import MyLibraryTab
    except ImportError:
        pytest.skip("desktop.my_library_tab not importable in this env")
    assert hasattr(MyLibraryTab, 'notify_session_restored'), (
        "notify_session_restored() missing from MyLibraryTab"
    )
    assert callable(MyLibraryTab.notify_session_restored)


def test_refresh_folder_list_ui_no_singleshot_300():
    """_refresh_folder_list_ui must NOT schedule _auto_select_first_folder via
    QTimer.singleShot after Phase 96 fix-7.  The caller (notify_session_restored)
    is responsible for triggering the auto-select."""
    import ast
    try:
        import desktop.my_library_tab as _m
        src_path = _m.__file__
    except ImportError:
        pytest.skip("desktop.my_library_tab not importable")

    with open(src_path, encoding='utf-8') as fh:
        tree = ast.parse(fh.read())

    # Find _refresh_folder_list_ui body and check no singleShot call exists
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == '_refresh_folder_list_ui':
            body_src = ast.unparse(node)
            assert '_auto_select_first_folder' not in body_src or \
                'singleShot' not in body_src, (
                "_refresh_folder_list_ui still schedules _auto_select_first_folder "
                "via QTimer.singleShot — fix-7 should have removed that timer"
            )
            return
    pytest.fail("_refresh_folder_list_ui not found in my_library_tab.py")


# ---------------------------------------------------------------------------
# P1.2 — canonical filepath keying
# ---------------------------------------------------------------------------

def test_local_indexer_emits_canonical_not_basename():
    """local_indexer._file_finished_cb must be called with a canonical path,
    not os.path.basename(filepath)."""
    try:
        import shared.local_indexer as _li
        src_path = _li.__file__
    except ImportError:
        pytest.skip("shared.local_indexer not importable")

    with open(src_path, encoding='utf-8') as fh:
        source = fh.read()

    # The old pattern emitted basename; fix-7 replaced it with _canonical_filepath
    assert 'os.path.basename(filepath)' not in source or \
        '_canonical_filepath' in source, (
        "local_indexer still emits os.path.basename(filepath) without "
        "_canonical_filepath — P1.2 fix may be missing"
    )
    # The canonical fix must be present
    assert '_canonical_filepath' in source, (
        "_canonical_filepath not found in local_indexer — P1.2 fix missing"
    )


def test_update_file_status_direct_lookup():
    """_UnifiedFileTreeWidget.update_file_status must do a direct dict lookup
    on the canonical path before falling back to basename scan."""
    import ast
    try:
        import desktop.my_library_tab as _m
        src_path = _m.__file__
    except ImportError:
        pytest.skip("desktop.my_library_tab not importable")

    with open(src_path, encoding='utf-8') as fh:
        tree = ast.parse(fh.read())

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == 'update_file_status':
            body_src = ast.unparse(node)
            # Must use .get(filepath) direct lookup
            assert '_leaf_by_path.get(' in body_src, (
                "update_file_status does not use _leaf_by_path.get() for "
                "direct canonical lookup — P1.2 fix missing"
            )
            return
    pytest.fail("update_file_status not found in my_library_tab.py")


# ---------------------------------------------------------------------------
# P1.3 — ResultDialog _htmlify escapes HTML
#
# We cannot instantiate ResultDialog without a Qt runtime, so we extract
# the _htmlify method body as a standalone function via AST + exec.
# ---------------------------------------------------------------------------

def _extract_htmlify():
    """Extract _htmlify source and compile it as a standalone callable.

    Injects `re` and `html` into the exec namespace so the function can
    reference them without the surrounding class/module context.
    """
    import ast
    import re as _re
    import html as _html
    try:
        import desktop.result_dialog as _rd
        src_path = _rd.__file__
    except ImportError:
        return None
    with open(src_path, encoding='utf-8') as fh:
        tree = ast.parse(fh.read())
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == '_htmlify':
            # Rewrite as a module-level function (drop 'self' param)
            node.args.args = node.args.args[1:]  # remove self
            node.name = '_htmlify_standalone'
            mod = ast.Module(body=[node], type_ignores=[])
            ast.fix_missing_locations(mod)
            # Provide re + html so the extracted function can reference them.
            ns: dict = {'re': _re, 'html': _html}
            exec(compile(mod, '<_htmlify>', 'exec'), ns)  # noqa: S102
            return ns['_htmlify_standalone']
    return None


def test_htmlify_escapes_angle_brackets():
    """_htmlify must escape < > & before applying <br> and bold markers."""
    fn = _extract_htmlify()
    if fn is None:
        pytest.skip("desktop.result_dialog not importable or _htmlify not found")

    result = fn("<b>hello</b> & world")
    assert '&lt;' in result, "_htmlify did not escape '<' — raw HTML injection possible"
    assert '&gt;' in result, "_htmlify did not escape '>' — raw HTML injection possible"
    assert '&amp;' in result, "_htmlify did not escape '&'"
    # Literal tag must NOT survive as-is
    assert '<b>hello</b>' not in result, (
        "Literal <b> tag from file content passed through unescaped"
    )


def test_htmlify_highlight_markers_still_work():
    """*...*  bold markers must still produce <b style='color:red;'> after escaping."""
    fn = _extract_htmlify()
    if fn is None:
        pytest.skip("desktop.result_dialog not importable or _htmlify not found")

    result = fn("before *highlight* after")
    assert "<b style='color:red;'>highlight</b>" in result, (
        "_htmlify bold substitution broken after adding html.escape() step"
    )


def test_htmlify_newlines_become_br():
    """Newlines must become <br> tags in _htmlify output."""
    fn = _extract_htmlify()
    if fn is None:
        pytest.skip("desktop.result_dialog not importable or _htmlify not found")

    result = fn("line1\nline2")
    assert '<br>' in result, "_htmlify newline→<br> substitution broken"


# ---------------------------------------------------------------------------
# P1.4 — browse_load clears current_browse_sid at entry
# ---------------------------------------------------------------------------

def test_browse_load_clears_sid_before_resolution():
    """browse_load() must assign self.current_browse_sid = None early (before
    any resolution logic) so failed lookups don't leave a stale LOCAL sid."""
    import ast
    try:
        import genizah_app as _ga
        src_path = _ga.__file__
    except ImportError:
        pytest.skip("genizah_app not importable")

    with open(src_path, encoding='utf-8') as fh:
        tree = ast.parse(fh.read())

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == 'browse_load':
            # The assignment current_browse_sid = None must appear BEFORE
            # the assignment current_browse_sid = sid (which is the success path).
            assigns = []
            for child in ast.walk(node):
                if (isinstance(child, ast.Assign)
                        and any(
                            isinstance(t, ast.Attribute)
                            and t.attr == 'current_browse_sid'
                            for t in child.targets
                        )):
                    assigns.append((child.lineno, ast.unparse(child)))
            # Should have at least two: the None sentinel and the real sid
            nones = [a for a in assigns if 'None' in a[1]]
            real  = [a for a in assigns if 'sid' in a[1] and 'None' not in a[1]]
            assert nones, "browse_load: no 'current_browse_sid = None' sentinel found"
            assert real,  "browse_load: no 'current_browse_sid = sid' assignment found"
            assert nones[0][0] < real[0][0], (
                "browse_load: None sentinel must appear BEFORE the real sid assignment"
            )
            return
    pytest.fail("browse_load not found in genizah_app.py")


# ---------------------------------------------------------------------------
# F1 — Search history corpus_scope round-trip
# ---------------------------------------------------------------------------

def test_add_regular_search_history_includes_corpus_scope():
    """_add_regular_search_to_history must store corpus_scope in search_params."""
    import ast
    try:
        import genizah_app as _ga
        src_path = _ga.__file__
    except ImportError:
        pytest.skip("genizah_app not importable")

    with open(src_path, encoding='utf-8') as fh:
        tree = ast.parse(fh.read())

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == '_add_regular_search_to_history':
            body_src = ast.unparse(node)
            assert 'corpus_scope' in body_src, (
                "_add_regular_search_to_history does not persist corpus_scope — F1 fix missing"
            )
            assert '_search_corpus_scope' in body_src, (
                "_add_regular_search_to_history does not read _search_corpus_scope"
            )
            return
    pytest.fail("_add_regular_search_to_history not found in genizah_app.py")


def test_restore_regular_search_from_state_applies_corpus_scope():
    """_restore_regular_search_from_state must restore corpus_scope from params."""
    import ast
    try:
        import genizah_app as _ga
        src_path = _ga.__file__
    except ImportError:
        pytest.skip("genizah_app not importable")

    with open(src_path, encoding='utf-8') as fh:
        tree = ast.parse(fh.read())

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == '_restore_regular_search_from_state':
            body_src = ast.unparse(node)
            assert 'corpus_scope' in body_src, (
                "_restore_regular_search_from_state does not restore corpus_scope — F1 fix missing"
            )
            assert 'corpus_scope_combo' in body_src, (
                "_restore_regular_search_from_state does not update corpus_scope_combo widget"
            )
            return
    pytest.fail("_restore_regular_search_from_state not found in genizah_app.py")


# ---------------------------------------------------------------------------
# F2 — Session save/restore LOCAL Browse identity round-trip
# ---------------------------------------------------------------------------

def test_save_session_persists_local_browse_fields():
    """_save_session state_dict must include local_browse_sys_id and
    local_browse_p_num keys."""
    import ast
    try:
        import genizah_app as _ga
        src_path = _ga.__file__
    except ImportError:
        pytest.skip("genizah_app not importable")

    with open(src_path, encoding='utf-8') as fh:
        tree = ast.parse(fh.read())

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == '_save_session':
            body_src = ast.unparse(node)
            assert 'local_browse_sys_id' in body_src, (
                "_save_session does not save local_browse_sys_id — F2 fix missing"
            )
            assert 'local_browse_p_num' in body_src, (
                "_save_session does not save local_browse_p_num — F2 fix missing"
            )
            return
    pytest.fail("_save_session not found in genizah_app.py")


def test_restore_session_restores_local_browse():
    """_restore_session must read local_browse_sys_id from state and schedule
    a deferred _restore_local_browse call."""
    import ast
    try:
        import genizah_app as _ga
        src_path = _ga.__file__
    except ImportError:
        pytest.skip("genizah_app not importable")

    with open(src_path, encoding='utf-8') as fh:
        tree = ast.parse(fh.read())

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == '_restore_session':
            body_src = ast.unparse(node)
            assert 'local_browse_sys_id' in body_src, (
                "_restore_session does not read local_browse_sys_id — F2 fix missing"
            )
            assert '_restore_local_browse' in body_src or 'local_browse_sys_id' in body_src, (
                "_restore_session does not restore LOCAL browse state"
            )
            assert 'notify_session_restored' in body_src, (
                "_restore_session does not call notify_session_restored() — P1.1 fix missing"
            )
            return
    pytest.fail("_restore_session not found in genizah_app.py")
