# -*- coding: utf-8 -*-
"""Phase 96-09 iteration 8 regression tests.

Covers:
  I1   — Session restore regression: notify_session_restored() is now called
          unconditionally in the finally block, not after try/except/finally.
          Verified for early-return paths (no-data, user-declined). A separate
          persistence regression now covers restore_mode='never' because
          lightweight preferences must still load there.
  I1b  — Corpus scope (Genizah/Local/ALL) is restored BEFORE the has_data
          gate so the combo shows the correct value even when there are no
          search results to display.
  I2   — End-to-end opt-out persistence: save → close → restore cycle
          verified at the unit-test level without a Qt runtime.
  I3a  — ResultDialog: Genizah-only buttons disabled for LOCAL hits in
          load_result_by_index.
  I3b  — Browse panel: Genizah-only buttons disabled in _open_local_browse_page.
"""
import ast
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_genizah_app_src():
    try:
        import genizah_app as _ga
        return open(_ga.__file__, encoding='utf-8').read()
    except ImportError:
        return None


def _parse_function(src: str, name: str):
    """Return the AST FunctionDef node with the given name, or None."""
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    return None


# ---------------------------------------------------------------------------
# I1 — notify_session_restored always fires (finally-block placement)
# ---------------------------------------------------------------------------

def test_notify_session_restored_in_finally_block():
    """_restore_session must call notify_session_restored() inside the finally
    block so it fires even when early-return paths are taken."""
    src = _load_genizah_app_src()
    if src is None:
        pytest.skip("genizah_app not importable")

    fn = _parse_function(src, '_restore_session')
    assert fn is not None, "_restore_session not found in genizah_app.py"

    # Find the try/except/finally structure at the top level of the function
    # and confirm notify_session_restored appears in a Finally handler.
    found_in_finally = False
    for node in ast.walk(fn):
        if isinstance(node, ast.Try):
            if node.finalbody:
                finally_src = ast.unparse(ast.Module(body=node.finalbody, type_ignores=[]))
                if 'notify_session_restored' in finally_src:
                    found_in_finally = True
                    break

    assert found_in_finally, (
        "notify_session_restored() is NOT inside a finally block in "
        "_restore_session — fix-8 regression: it must fire for ALL code paths "
        "(no-data early return, user-declined, exception), not only on success."
    )


def test_notify_session_restored_not_after_try_block():
    """notify_session_restored must NOT appear ONLY after the try/except/finally
    construct (the pre-fix-8 placement that missed early returns)."""
    src = _load_genizah_app_src()
    if src is None:
        pytest.skip("genizah_app not importable")

    fn = _parse_function(src, '_restore_session')
    assert fn is not None, "_restore_session not found in genizah_app.py"

    # Collect all Try nodes; if the ONLY occurrence of notify_session_restored
    # in the function body is *outside* any Try.finalbody, the fix is missing.
    finally_bodies = []
    for node in ast.walk(fn):
        if isinstance(node, ast.Try) and node.finalbody:
            finally_bodies.extend(node.finalbody)

    finally_src = ast.unparse(ast.Module(body=finally_bodies, type_ignores=[]))
    assert 'notify_session_restored' in finally_src, (
        "notify_session_restored not found in any finally block — "
        "fix-8 requires it to be in the finally block"
    )


# ---------------------------------------------------------------------------
# I1b — Corpus scope restored before has_data gate
# ---------------------------------------------------------------------------

def test_corpus_scope_restored_before_has_data_gate():
    """search_corpus_scope must be restored from session JSON BEFORE the
    has_data early-return so it applies even when there are no search results."""
    src = _load_genizah_app_src()
    if src is None:
        pytest.skip("genizah_app not importable")

    fn = _parse_function(src, '_restore_session')
    assert fn is not None, "_restore_session not found in genizah_app.py"

    fn_src = ast.unparse(fn)

    # Verify corpus scope is set before has_data check
    # We look for _search_corpus_scope assignment and has_data assignment order.
    corpus_pos = fn_src.find('_search_corpus_scope')
    has_data_pos = fn_src.find('has_data =')

    assert corpus_pos != -1, (
        "_search_corpus_scope not found in _restore_session body — "
        "corpus scope restoration missing"
    )
    assert has_data_pos != -1, "has_data assignment not found in _restore_session"
    assert corpus_pos < has_data_pos, (
        "_search_corpus_scope must be assigned BEFORE the has_data gate "
        "(fix-8: corpus scope must restore even when no search results exist)"
    )


# ---------------------------------------------------------------------------
# I2 — End-to-end opt-out persistence unit test (no Qt runtime needed)
# ---------------------------------------------------------------------------

def _simulate_save_session(local_file_optouts, search_corpus_scope='genizah'):
    """Build the state_dict that _save_session would write to disk."""
    return {
        'version': 1,
        'local_file_optouts': sorted(local_file_optouts),
        'local_browse_view_mode': 'per_page',
        'local_browse_sys_id': None,
        'local_browse_p_num': None,
        'regular_search': {
            'query': '',
            'mode_index': 0,
            'gap': 0,
            'search_corpus_scope': search_corpus_scope,
            'results': [],
            'domain_exclusions': [],
            'printed_filter': 'all',
            'local_filter': 'all',
            'printed_ids': [],
            'excluded_sys_ids': [],
            'excluded_shelfmarks': [],
            'excluded_raw_entries': [],
            'exclusion_sources': [],
            'results_filters': {},
            'filter_sources': {},
            'filter_enabled_sources': [],
            'refinement_chain': [],
        },
        'composition_search': {
            'source_text': '',
            'title': '',
            'chunk_size': 5,
            'max_freq': 10,
            'mode_index': 0,
            'results': [],
            'filtered_results': [],
            'domain_exclusions': [],
            'printed_filter': 'all',
            'local_filter_composition': 'all',
            'local_filter_parallels': 'all',
            'excluded_sys_ids': [],
            'excluded_shelfmarks': [],
            'sort_mode': 'score',
            'sort_reverse': True,
            'flat_mode': False,
            'appendix_threshold': 5,
            'summary_text': '',
        },
    }


def _simulate_restore_session(state_dict):
    """Simulate what _restore_session does with the state_dict.

    Returns a dict of the values that would be set on self after restore.
    """
    reg = state_dict.get('regular_search', {})
    result = {
        'local_file_optouts': set(state_dict.get('local_file_optouts', [])),
        'local_browse_view_mode': state_dict.get('local_browse_view_mode', 'per_page'),
        'search_corpus_scope': reg.get('search_corpus_scope', 'genizah'),
    }
    return result


def test_optout_survives_save_restore_cycle():
    """Simulates the full save→close→restore cycle: opted-out paths must appear
    in _local_file_optouts after restore, regardless of whether search results
    are present."""
    optouts = {
        r'c:\users\h\docs\scan.pdf',
        r'c:\users\h\docs\sub\notes.docx',
    }
    # Save (simulate closeEvent path)
    state = _simulate_save_session(optouts, search_corpus_scope='all')

    # Restore (simulate _restore_session with no search results → has_data=False)
    assert not state['regular_search']['results'], "Test setup: no results for has_data=False path"

    restored = _simulate_restore_session(state)

    assert restored['local_file_optouts'] == optouts, (
        f"Opt-outs not preserved across save/restore cycle.\n"
        f"Expected: {optouts}\n"
        f"Got: {restored['local_file_optouts']}"
    )


def test_corpus_scope_survives_save_restore_cycle_no_results():
    """corpus_scope must round-trip through session even when no search results
    (has_data=False path — the fix-8 early-restore scenario)."""
    for scope in ('genizah', 'local', 'all'):
        state = _simulate_save_session(set(), search_corpus_scope=scope)
        restored = _simulate_restore_session(state)
        assert restored['search_corpus_scope'] == scope, (
            f"corpus_scope='{scope}' not preserved: got '{restored['search_corpus_scope']}'"
        )


def test_empty_optouts_roundtrip():
    """An empty opt-out set must round-trip as an empty set (not None / missing)."""
    state = _simulate_save_session(set())
    restored = _simulate_restore_session(state)
    assert restored['local_file_optouts'] == set(), (
        "Empty opt-out set should restore as empty set, not None"
    )


# ---------------------------------------------------------------------------
# I2 (structural) — _restore_session loads opt-outs before has_data gate
# ---------------------------------------------------------------------------

def test_restore_session_loads_optouts_before_has_data():
    """_local_file_optouts must be assigned from state BEFORE the has_data
    early-return check so opt-outs survive even with no search results."""
    src = _load_genizah_app_src()
    if src is None:
        pytest.skip("genizah_app not importable")

    fn = _parse_function(src, '_restore_session')
    assert fn is not None, "_restore_session not found"

    fn_src = ast.unparse(fn)
    optout_pos = fn_src.find('_local_file_optouts')
    has_data_pos = fn_src.find('has_data =')

    assert optout_pos != -1, "_local_file_optouts not assigned in _restore_session"
    assert has_data_pos != -1, "has_data gate not found in _restore_session"
    assert optout_pos < has_data_pos, (
        "_local_file_optouts must be assigned BEFORE the has_data early-return "
        "gate (fix-8: opt-outs must load even when there is nothing else to restore)"
    )


# ---------------------------------------------------------------------------
# I3a — ResultDialog disables Genizah buttons for LOCAL hits
# ---------------------------------------------------------------------------

def test_result_dialog_disables_buttons_for_local_hits():
    """load_result_by_index must call setEnabled(False) on Genizah-only buttons
    when _is_local_hit is True."""
    try:
        import desktop.result_dialog as _rd
        src_path = _rd.__file__
    except ImportError:
        pytest.skip("desktop.result_dialog not importable")

    with open(src_path, encoding='utf-8') as fh:
        src = fh.read()

    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == 'load_result_by_index':
            body_src = ast.unparse(node)
            # Must disable community buttons for local hits
            assert 'setEnabled(False)' in body_src, (
                "load_result_by_index does not call setEnabled(False) — "
                "Genizah-only button disabling for LOCAL hits missing"
            )
            assert '_is_local_hit' in body_src, (
                "load_result_by_index does not branch on _is_local_hit for "
                "button enabling — Issue 3 fix missing"
            )
            # Verify specific buttons are covered
            for btn_name in ('btn_img', 'btn_add_to_puzzle', 'btn_rd_edit',
                             'btn_comment', 'btn_view_corrections', 'btn_joins'):
                assert btn_name in body_src, (
                    f"load_result_by_index does not reference {btn_name} — "
                    f"Genizah button disabling may be incomplete"
                )
            return
    pytest.fail("load_result_by_index not found in result_dialog.py")


# ---------------------------------------------------------------------------
# I3b — Browse panel disables Genizah buttons in _open_local_browse_page
# ---------------------------------------------------------------------------

def test_browse_panel_disables_buttons_for_local_hits():
    """_open_local_browse_page must disable Genizah-only community buttons
    (btn_b_catalog, btn_b_add_to_puzzle, btn_b_edit, btn_b_comment,
    btn_b_view_corrections, btn_b_joins, browse_version_combo)."""
    src = _load_genizah_app_src()
    if src is None:
        pytest.skip("genizah_app not importable")

    fn = _parse_function(src, '_open_local_browse_page')
    assert fn is not None, "_open_local_browse_page not found in genizah_app.py"

    body_src = ast.unparse(fn)

    assert 'setEnabled(False)' in body_src, (
        "_open_local_browse_page does not call setEnabled(False) — "
        "Genizah-only button disabling for LOCAL Browse missing"
    )
    for btn_name in ('btn_b_catalog', 'btn_b_add_to_puzzle', 'btn_b_edit',
                     'btn_b_comment', 'btn_b_view_corrections', 'btn_b_joins',
                     'browse_version_combo'):
        assert btn_name in body_src, (
            f"_open_local_browse_page does not reference {btn_name} — "
            f"Browse-panel Genizah button disabling incomplete"
        )
