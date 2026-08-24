# -*- coding: utf-8 -*-
"""The multi-witness witness panel on web/pages/parallels.py.

Source-text/AST assertions, in the same style and for the same reason as
tests/test_parallels_page_passage_controls.py: `create_parallels_page` has
heavy NiceGUI/page side effects and is never imported by any test, so its
handlers are extracted and inspected at the AST level rather than run.

That style cannot prove the UI renders. What it CAN prove -- and what this
file is for -- is that the specific decisions the feature rests on are still
encoded in the handler, because each of them is a one-line edit away from a
silent regression:

* the panel is letter-level only (a measured finding, not a preference);
* the panel is HIDDEN, never cleared, by a method switch;
* a witness is searched at most ONCE, which is what makes an R-round
  expansion linear rather than quadratic;
* the page never concatenates witnesses into one query;
* removing a witness strips its rows;
* the restored snapshot actually rebuilds the witness list;
* group order follows the sort control (it used to be hard-coded).
"""
from __future__ import annotations

import ast
import os
import re

import pytest

PAGE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'web', 'pages', 'parallels.py',
)


def _source() -> str:
    with open(PAGE_PATH, encoding='utf-8') as fh:
        return fh.read()


def _func_source(name: str) -> str:
    src = _source()
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if (isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == name):
            return ast.get_source_segment(src, node) or ''
    raise AssertionError(f'{name} not found in web/pages/parallels.py')


def _func_names() -> set:
    return {node.name for node in ast.walk(ast.parse(_source()))
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}


def _calls_in(name: str) -> set:
    """Every function/method NAME called inside `name`, from the AST.

    Substring assertions over a function's source cannot tell a real call
    from a mention in its own docstring -- and a docstring that names the
    function the test greps for makes the test permanently green. (Proven:
    the promotion test passed with `get_full_manuscript` deleted, because
    the docstring above it still said the words.)
    """
    src = _source()
    for node in ast.walk(ast.parse(src)):
        if (isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == name):
            out = set()
            for sub in ast.walk(node):
                if isinstance(sub, ast.Call):
                    fn = sub.func
                    if isinstance(fn, ast.Name):
                        out.add(fn.id)
                    elif isinstance(fn, ast.Attribute):
                        out.add(fn.attr)
            return out
    raise AssertionError(f'{name} not found in web/pages/parallels.py')


def _call_sites_of(name: str) -> int:
    """How many times `name` is CALLED anywhere in the page, excluding its
    own definition -- so "the function exists" can never stand in for "the
    function is wired in"."""
    src = _source()
    count = 0
    for node in ast.walk(ast.parse(src)):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == name):
            count += 1
    return count


# ---------------------------------------------------------------------------
# The handlers exist, exactly once each.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('name', [
    '_fuse_and_store',
    '_add_witness',
    '_remove_witness',
    '_refresh_witness_panel',
    '_open_add_witness_dialog',
    '_run_one_witness_search',
    '_search_pending_witnesses',
    '_promote_checked',
    '_run_auto_expand',
    '_restore_witnesses_from_snapshot',
    '_sort_groups',
    '_source_heading_for',
])
def test_handler_is_defined_exactly_once(name):
    src = _source()
    matches = [n for n in ast.walk(ast.parse(src))
               if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
               and n.name == name]
    assert len(matches) == 1, f'expected exactly one {name} definition'


# ---------------------------------------------------------------------------
# Letter-level only -- and hidden, never cleared.
# ---------------------------------------------------------------------------

def test_the_panel_is_shown_only_for_letter_level_search():
    """Multi-witness on the CHUNK engine measured +2 positives of 74 with
    zero frontier gain at 4-6x the time, because concatenation and union
    there return the identical manuscript set. Showing the panel for chunk
    would offer a control that does almost nothing, slowly."""
    src = _func_source('on_passage_mode_change')
    assert 'witness_panel.set_visibility(True)' in src
    assert 'witness_panel.set_visibility(False)' in src


def test_a_method_switch_never_clears_the_witness_list():
    """A user who switches to chunk to check something and back must not
    find seventeen pasted texts gone."""
    src = _func_source('on_passage_mode_change')
    assert 'p_state.witnesses = []' not in src
    assert 'witness_rows' not in src, (
        'a method switch must hide the panel, never discard its contents'
    )


# ---------------------------------------------------------------------------
# Never concatenate. The finding the whole design rests on.
# ---------------------------------------------------------------------------

def test_the_page_searches_one_witness_text_per_call():
    """The passage engine spends a per-query posting budget, so a joined
    query starves -- 59% of a reachable census against 85% fused. The page
    must pass ONE witness's text, never a join of several."""
    src = _func_source('_run_one_witness_search')
    assert "entry['text']" in src, (
        'the witness search must pass that witness\'s own text'
    )
    assert not re.search(r"\.join\(.*witness", src, re.I | re.S), (
        'a join over witnesses inside the search call is a concatenated query'
    )


def test_the_page_never_passes_a_witness_list_to_the_engine():
    """The PAGE fuses across N separate calls; only the stateless API uses
    the engine's own `witnesses=` fan-out. Mixing the two would give one
    witness its own budget slot and the rest none."""
    src = _func_source('_run_one_witness_search')
    assert 'witnesses=' not in src


def test_each_witness_gets_its_own_budget_slot():
    """One acquire/release per witness, so the 30s ceiling bounds ONE
    witness rather than a whole batch and the shared pool of 4 interleaves
    with other users between witnesses."""
    src = _func_source('_run_one_witness_search')
    assert 'run_passage_search(' in src, (
        'witness searches must go through the bounded passage budget, not '
        'run.io_bound\'s generic unbounded pool'
    )
    assert 'run.io_bound' not in src


def test_a_failed_witness_is_skipped_and_the_run_continues():
    src = _func_source('_search_pending_witnesses')
    assert "'failed'" in src
    assert 'continue' in src, (
        'a witness that times out or hits a busy budget must not abort the '
        'remaining witnesses'
    )


# ---------------------------------------------------------------------------
# Additive by construction -- what makes the cost linear.
# ---------------------------------------------------------------------------

def test_only_pending_witnesses_are_searched():
    """A witness is searched at most once. Re-running every witness on every
    addition would make an R-round auto-expansion quadratic instead of
    linear -- which would falsify the premise the feature rests on."""
    src = _func_source('_search_pending_witnesses')
    assert re.search(
        r"pending\s*=\s*\[w for w in p_state\.witnesses\s*"
        r"if w\['status'\] == 'pending'\]", src), (
        'the additive path must select ONLY pending witnesses -- searching '
        'the whole list on every addition makes an R-round expansion '
        'quadratic instead of linear'
    )


def test_auto_expand_reuses_the_additive_path():
    """One implementation behind both manual promotion and each auto-expand
    round -- two would drift, and only one of them would be the linear one."""
    src = _func_source('_promote_checked')
    assert '_search_pending_witnesses()' in src
    assert '_promote_checked()' in _func_source('_run_auto_expand')


def test_auto_expand_refuses_a_round_rather_than_shrinking_top_k():
    """Silently searching fewer than top-K would make the control a lie."""
    src = _func_source('_run_auto_expand')
    assert 'witness cap reached' in src
    assert 'break' in src


def test_auto_expand_is_not_wired_into_the_find_parallels_button():
    """An explicit button. A user who wanted one search must not get
    twenty."""
    src = _func_source('execute_parallels')
    assert '_run_auto_expand' not in src


# ---------------------------------------------------------------------------
# Removal, promotion, provenance.
# ---------------------------------------------------------------------------

def test_removing_a_witness_strips_its_rows():
    """Otherwise the panel says the witness is gone while its rows -- up to
    a few thousand, for a witness that found nothing useful -- stay on
    screen with no way to attribute or remove them."""
    src = _func_source('_remove_witness')
    assert 'witness_rows.pop' in src
    assert 'witness_filtered.pop' in src
    assert '_fuse_and_store()' in src


def test_promotion_state_lives_in_p_state_not_on_the_widget():
    """The checkbox is destroyed and rebuilt on every re-render, so a
    selection held on the widget would vanish whenever anything
    re-rendered."""
    src = _func_source('create_manuscript_group')
    assert 'p_state.checked_for_promotion' in src


def test_promotion_fetches_every_page_of_the_manuscript():
    """A result GROUP spans several page-level hits; promoting only the
    best-scoring page would throw away most of the witness."""
    calls = _calls_in('_promote_checked')
    assert 'get_full_manuscript' in calls, (
        'promotion must fetch EVERY page of the manuscript'
    )
    assert 'io_bound' in calls, (
        'Tantivy lookups on the single uvicorn event loop stall every other '
        'request'
    )


def test_promotion_skips_manuscripts_already_in_the_witness_list():
    """Two witnesses with identical text would BOTH contribute to
    `witness_count` and to the RRF sum, so a manuscript found by ONE witness
    would report two. That is a wrong number, not merely a redundant search.
    (Auto-expand already filtered these; the checkbox path did not.)"""
    src = _func_source('_promote_checked')
    assert re.search(
        r"_already\s*=\s*\{w\.get\('sys_id'\) for w in p_state\.witnesses",
        src), 'promotion does not de-duplicate against the existing witnesses'
    assert 's not in _already' in src, (
        'the de-duplication set is computed but never applied'
    )


def test_the_source_excerpt_is_attributed_to_its_own_witness():
    """A span offset is a position in ONE witness's text, so labelling the
    excerpt "Your text" when it came from a promoted manuscript would
    misattribute the evidence."""
    # Both renderers, checked individually: asserting the helper is called
    # "somewhere" passes while one of the two call sites still hard-codes
    # the old label.
    for renderer in ('create_parallel_item', 'create_result_card'):
        body = _func_source(renderer)
        assert '_source_heading_for(item)' in body, (
            f'{renderer} does not attribute the excerpt to its witness')
        assert "tr('Your text')" not in body, (
            f'{renderer} still hard-codes the seed label')
    heading = _func_source('_source_heading_for')
    assert "tr('Your text')" in heading, (
        'a single-witness search still says "Your text"'
    )
    assert 'witness_label' in heading


# ---------------------------------------------------------------------------
# Fusion, and the single-witness short circuit.
# ---------------------------------------------------------------------------

def test_the_page_fuses_through_the_shared_module():
    """One definition of the ranking, two callers. A second implementation
    on the page would drift from the API's."""
    src = _func_source('_fuse_and_store')
    assert 'from shared.passage_fusion import fuse, tag_rows' in src


def test_a_single_witness_passes_through_unfused():
    """RRF over one list is a 1/(k+rank) rescale that carries no
    information, and it would silently change `score` from matched letters
    to ~0.03 -- the number the Max:/Avg: badges and every export column
    read."""
    src = _func_source('_fuse_and_store')
    assert 'if len(order) == 1:' in src
    assert 'return' in src


def test_ranks_are_assigned_over_both_buckets_together():
    """The engine's own fan-out tags the FULL result list before splitting it
    into main/filtered, so a filtered row consumes a rank. Ranking each
    bucket from 1 independently gave a filtered row the rank of a top hit and
    left every main row short by however many rows were demoted ahead of it
    -- so the page and the API computed different RRF sums for the same
    witnesses. Measured on 17 Birkat Hamazon witnesses before the fix: the
    two paths agreed on total census recall but not on the top 20."""
    src = _func_source('_fuse_and_store')
    assert 'combined = sorted(main + filt' in src, (
        'ranks must be assigned over both buckets in score order'
    )
    assert re.search(r"tag_rows\(combined,", src)
    # ... and NOT per bucket, which is the shape this replaced.
    assert 'tag_rows(list(p_state.witness_rows' not in src


def test_a_record_is_filtered_only_when_every_witness_filters_it():
    """Otherwise the "known source text" filter gets STRICTER the more
    witnesses you add -- the opposite of what the control says."""
    src = _func_source('_fuse_and_store')
    assert 'in_main' in src
    assert re.search(r"not in in_main", src)


# ---------------------------------------------------------------------------
# Group order -- and the pre-existing bug this repairs.
# ---------------------------------------------------------------------------

def test_group_order_follows_the_sort_control():
    """Group order was hard-coded to max_score regardless of the control, so
    two of its three existing options -- 'shelfmark' and 'matches' -- had no
    visible effect at all."""
    src = _func_source('render_results')
    assert '_sort_groups(grouped.items(), sort_by)' in src
    assert '_sort_groups(filtered_grouped.items(), sort_by)' in src
    assert "key=lambda x: x[1]['max_score']" not in src, (
        'the hard-coded group order is back'
    )


@pytest.mark.parametrize('mode', ['shelfmark', 'matches', 'fused', 'witnesses'])
def test_every_sort_mode_has_a_branch(mode):
    src = _func_source('_sort_groups')
    assert f"'{mode}'" in src


def test_group_witness_count_is_a_union_not_a_sum():
    """Two pages of one manuscript found by the same witness is ONE
    witness. The union lives in the shared module and is unit-tested
    there."""
    src = _func_source('_group_witness_stats')
    assert 'from shared.passage_fusion import group_stats' in src


# ---------------------------------------------------------------------------
# Restore. The worst failure mode this feature could have.
# ---------------------------------------------------------------------------

def test_the_snapshot_stores_the_witness_list():
    src = _func_source('_persist_active_snapshot')
    assert "'witnesses':" in src
    assert "w.get('kind') == 'manuscript'" in src, (
        'a promoted witness is stored without its text (re-fetchable); only '
        'a pasted one keeps it'
    )


def test_the_restore_has_an_actual_consumer():
    """Storing witnesses in the snapshot does nothing on its own: the
    restore path applies known primitive controls only. Without this, a
    restored multi-witness search would silently re-run as seed-only while
    LOOKING identical."""
    # The definition itself contains its own name, so "the name appears in
    # the file" is not evidence of anything. Count real call sites.
    assert _call_sites_of('_restore_witnesses_from_snapshot') >= 1, (
        'the restore function is defined but never called -- a restored '
        'multi-witness search would silently re-run as seed-only'
    )
    src = _func_source('_restore_witnesses_from_snapshot')
    assert 'p_state.witnesses = restored' in src


def test_restored_witnesses_come_back_pending_with_no_stale_rows():
    """The snapshot holds the FUSED rows, from which per-witness ranks
    cannot be recovered. A fusion rebuilt from incomplete inputs would be
    quietly wrong rather than visibly absent."""
    src = _func_source('_restore_witnesses_from_snapshot')
    assert "'status': 'pending'" in src
    assert 'p_state.witness_rows = {}' in src


def test_witnesses_are_not_part_of_searched_config():
    """`_apply_restored_search_config` validates every value against a
    widget's `.options`, and a witness is not a select. Pinned so a later
    "tidy-up" does not move it there and break the restore."""
    src = _func_source('execute_parallels')
    # Bound the slice at the NEXT statement, not a fixed character count --
    # a fixed window silently grows into whatever is written after it.
    cfg_start = src.index('p_state.searched_config = {')
    cfg_end = src.index('_parallels_search_meta = {', cfg_start)
    cfg = src[cfg_start:cfg_end]
    assert 'witness' not in cfg


# ---------------------------------------------------------------------------
# A fresh search is a fresh run.
# ---------------------------------------------------------------------------

def test_find_parallels_resets_every_witness_to_pending():
    """Rows found under the previous settings must never be fused with rows
    found under the new ones."""
    src = _func_source('execute_parallels')
    assert 'p_state.witness_rows = {}' in src
    # The loop TARGET matters, not just its body: `for _w in []:` keeps the
    # body verbatim and resets nothing.
    assert re.search(
        r"for _w in p_state\.witnesses:\s*\n\s*_w\['status'\] = 'pending'",
        src), 'every witness must go back to pending on a fresh run'


def test_the_seed_is_modelled_as_a_witness():
    """So "found by 3 of 5" needs no +1 special case anywhere."""
    src = _func_source('execute_parallels')
    assert 'p_state.witness_rows[WITNESS_SEED_ID]' in src


def test_a_witness_added_later_is_searched_with_the_same_settings():
    """A witness run at a different width or depth than the rows beside it
    would be fused into one list with them and be invisible as an anomaly."""
    src = _func_source('execute_parallels')
    assert 'p_state.last_passage_ctx = {' in src
    run_src = _func_source('_run_one_witness_search')
    assert 'p_state.last_passage_ctx' in run_src
