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


def _call_lineno(func: str, name: str) -> int:
    """Line of the first CALL to `name` inside `func`.

    From the AST, not a string index: a docstring mentioning the function
    would otherwise decide the answer -- which is exactly what happened when
    this file first tried to assert the order of two calls (the stale
    docstring said "Text comes from get_full_manuscript", putting it at
    character 139 of a function that calls it at character 2,900).
    """
    src = _source()
    for node in ast.walk(ast.parse(src)):
        if (isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == func):
            for sub in ast.walk(node):
                if (isinstance(sub, ast.Call)
                        and isinstance(sub.func, ast.Attribute)
                        and sub.func.attr == name):
                    return sub.lineno
    raise AssertionError(f'{func} does not call {name}')


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


def test_promotion_reads_the_matched_pages_not_the_browse_map():
    """`get_full_manuscript` resolves through Config.BROWSE_MAP, which is not
    guaranteed to be populated for an arbitrary manuscript -- owner-reported,
    every promotion failed because that map held two entries.

    The primary source is the matched pages' own `raw_header`s through
    `get_full_text_by_header`, the same fetcher that just rendered those rows,
    which needs no auxiliary map and cannot fail for a row on screen.
    `get_full_manuscript` stays only as a fallback."""
    from web.pages.parallels import collect_witness_texts

    seen = []

    def fetch_header(h):
        seen.append(h)
        return 'text-of-' + h

    def fetch_manuscript(sid):
        raise AssertionError('the browse-map path must not be the primary')

    rows = [
        {'raw_header': 'S1_IE1_P1_FL1'},
        {'raw_header': 'S1_IE1_P2_FL2'},
        {'raw_header': 'OTHER_IE9_P1_FL9'},
    ]
    texts, failed = collect_witness_texts(
        ['9900000001'], rows, fetch_header=fetch_header,
        fetch_manuscript=fetch_manuscript)
    # Nothing matched: the fixture headers carry no sys_id, so this proves the
    # extraction is really applied rather than assumed.
    assert texts == {} and failed == ['9900000001']

    seen.clear()
    rows = [
        {'raw_header': '9900000001_IE1_P000001_FL1'},
        {'raw_header': '9900000001_IE1_P000002_FL2'},
        {'raw_header': '9900000002_IE9_P000001_FL9'},
    ]
    texts, failed = collect_witness_texts(
        ['9900000001'], rows, fetch_header=fetch_header,
        fetch_manuscript=fetch_manuscript)

    assert failed == []
    assert seen == ['9900000001_IE1_P000001_FL1',
                    '9900000001_IE1_P000002_FL2'], (
        'a result GROUP spans several page-level hits and ALL of them are '
        'the witness -- one page is usually a fraction of it'
    )
    assert texts['9900000001'] == ('text-of-9900000001_IE1_P000001_FL1'
                                   + chr(10)
                                   + 'text-of-9900000001_IE1_P000002_FL2')
    # And the wiring is still off the event loop.
    assert 'io_bound' in _calls_in('_promote_checked'), (
        'Tantivy lookups on the single uvicorn event loop stall every other '
        'request'
    )


def test_a_97_prefixed_manuscript_can_be_promoted():
    """A 99-only pattern silently skips every 97-prefixed manuscript, so
    auto-expand could never promote one. Three copies of that regex existed
    on this page; there is now one."""
    from web.pages.parallels import collect_witness_texts, witness_sys_id

    assert witness_sys_id({'raw_header': '9700000001_IE1_P1_FL1'}) == '9700000001'
    texts, failed = collect_witness_texts(
        ['9700000001'], [{'raw_header': '9700000001_IE1_P000001_FL1'}],
        fetch_header=lambda h: 'ok')
    assert texts == {'9700000001': 'ok'} and failed == []


def test_promotion_falls_back_to_the_whole_manuscript_only_when_needed():
    from web.pages.parallels import collect_witness_texts

    texts, failed = collect_witness_texts(
        ['9900000001'], [{'raw_header': '9900000001_IE1_P000001_FL1'}],
        fetch_header=lambda h: None,
        fetch_manuscript=lambda sid: [{'text': 'whole'}, {'text': 'thing'}])
    assert texts == {'9900000001': 'whole' + chr(10) + 'thing'}
    assert failed == []


def test_promotion_reports_a_manuscript_it_could_not_load():
    """Returned, never logged-and-dropped: the caller has to be able to name
    what failed instead of emitting one anonymous toast per manuscript."""
    from web.pages.parallels import collect_witness_texts

    texts, failed = collect_witness_texts(
        ['9900000001', '9900000002'],
        [{'raw_header': '9900000001_IE1_P000001_FL1'}],
        fetch_header=lambda h: 'ok', fetch_manuscript=lambda sid: [])
    assert texts == {'9900000001': 'ok'}
    assert failed == ['9900000002']


def test_promotion_survives_a_fetcher_that_raises():
    from web.pages.parallels import collect_witness_texts

    def boom(_h):
        raise RuntimeError('tantivy hiccup')

    texts, failed = collect_witness_texts(
        ['9900000001'], [{'raw_header': '9900000001_IE1_P000001_FL1'}],
        fetch_header=boom,
        fetch_manuscript=lambda sid: [{'text': 'recovered'}])
    assert texts == {'9900000001': 'recovered'} and failed == []


def test_promotion_reports_failures_once_and_by_name():
    """Fifteen identical toasts naming no manuscript was the owner's first
    experience of this feature."""
    src = _func_source('_promote_checked')
    assert 'Could not load text for {n} manuscripts: {names}' in src
    # ONE notify for the whole batch, driven by the list the helper returns.
    assert 'texts, failed = await run.io_bound(_fetch)' in src
    # NONE of the notifies may sit inside the per-manuscript loop -- that is
    # the shape this replaced (fifteen identical anonymous toasts). Asked of
    # the AST, not of a substring span: this test used to find the loop's end
    # by searching for the literal `if failed:`, so a summary added between
    # the loop and that line counted as being inside it.
    for node in ast.walk(ast.parse(_source())):
        if not (isinstance(node, ast.AsyncFunctionDef)
                and node.name == '_promote_checked'):
            continue
        for sub in ast.walk(node):
            if not (isinstance(sub, ast.For)
                    and isinstance(sub.target, ast.Name)
                    and sub.target.id == 'sid'):
                continue
            for inner in ast.walk(sub):
                assert not (isinstance(inner, ast.Call)
                            and isinstance(inner.func, ast.Attribute)
                            and inner.func.attr == 'notify'), (
                    'a notify inside the per-manuscript loop fires once per '
                    'manuscript'
                )
        break
    else:
        raise AssertionError('_promote_checked not found')


def test_there_is_exactly_one_sys_id_pattern_on_the_page():
    """Three copies existed, one of them 99-only. Behaviour is pinned by
    test_a_97_prefixed_manuscript_can_be_promoted; this pins that the copies
    do not come back."""
    src = _source()
    assert src.count("(?:99|97)") == 1, (
        'the sys_id pattern has been duplicated again'
    )
    for closure in ('_ranked_sys_ids', '_row_sys_id'):
        assert 're.search' not in _func_source(closure), (
            f'{closure} builds its own pattern instead of using '
            f'witness_sys_id()'
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
    quietly wrong rather than visibly absent.

    The `pending` half is checked by CALLING the normaliser (see
    `test_every_restored_witness_comes_back_pending`); what is left here is
    the closure's own job -- dropping the row caches it cannot rebuild.
    """
    src = _func_source('_restore_witnesses_from_snapshot')
    assert 'p_state.witness_rows = {}' in src
    assert 'p_state.witness_filtered = {}' in src


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

def test_find_parallels_resets_every_witness_of_this_text_to_pending():
    """Rows found under the previous settings must never be fused with rows
    found under the new ones."""
    src = _func_source('execute_parallels')
    assert 'p_state.witness_rows = {}' in src
    # The loop TARGET matters, not just its body: `for _w in []:` keeps the
    # body verbatim and resets nothing.
    assert 'for _w in p_state.witnesses:' in src
    assert "_w['status'] = 'stale' if _stale else 'pending'" in src


def test_a_witness_of_a_different_source_text_is_not_searched():
    """Owner-reported: pasting Birkat Hamazon over the Antiochus source text
    and searching also searched the fifteen Antiochus witnesses, fusing one
    work's witnesses into another work's results. A witness belongs to the
    work it was gathered for."""
    src = _func_source('execute_parallels')
    assert "_w.get('seed_digest') not in (None, '', _digest_now)" in src, (
        'staleness must be decided by the seed the witness was gathered under'
    )
    # Marked, never deleted -- a typo edit must not destroy seventeen
    # hand-pasted texts.
    assert "'stale'" in src
    assert 'p_state.witnesses = []' not in src
    assert "'seed_digest': seed_digest or _seed_digest()" in _func_source(
        '_add_witness'), (
        'a witness with no explicit stamp still takes the live one -- see '
        'test_a_promoted_witness_is_stamped_with_the_search_it_came_from '
        'for why a promoted one must not'
    )


def test_a_stale_witness_is_not_in_the_pending_set():
    """`_search_pending_witnesses` searches `pending`; stale must not be it,
    or marking them changes nothing."""
    src = _func_source('_search_pending_witnesses')
    assert "if w['status'] == 'pending'" in src
    assert "'stale'" not in src


def test_stale_witnesses_offer_both_answers():
    """Keeping them is the reported bug; deleting them silently is data loss.
    The panel asks."""
    src = _func_source('_refresh_witness_panel')
    assert '_revive_stale_witnesses' in src
    assert '_remove_stale_witnesses' in src
    # Driven by whether any witness IS stale -- a hard-coded False hides the
    # only route back and leaves the list silently unsearched.
    assert 'witness_stale_row.set_visibility(bool(stale))' in src
    # Adopting must RE-STAMP, or they go stale again on the next search and
    # the user answers the same question twice.
    revive = _func_source('_revive_stale_witnesses')
    assert "w['seed_digest'] = digest" in revive


def test_the_stale_stamp_survives_a_reload():
    """Without it a restored list would look native to whatever text happens
    to be in the box after the reload."""
    assert "'seed_digest': w.get('seed_digest') or ''" in _func_source(
        '_persist_active_snapshot')
    # The read half is checked by calling the normaliser, which a substring
    # match cannot do -- see `test_the_seed_stamp_survives_the_round_trip`.
    from web.pages.parallels import restore_witness_entries
    got = restore_witness_entries(
        [{'kind': 'pasted', 'text': 'aleph bet gimel',
          'seed_digest': 'deadbeefcafe0001'}], 'Pasted text')
    assert got[0]['seed_digest'] == 'deadbeefcafe0001'


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


# ---------------------------------------------------------------------------
# A standing Stop stops the witnesses too.
# ---------------------------------------------------------------------------

def _clears_flag_false(func: str, attr: str) -> bool:
    """Does `func` assign `p_state.<attr> = False` anywhere inside it?

    An AST test, because the point is an ASSIGNMENT: the function's own
    comment explains at length why the flag must not be cleared here, so any
    grep for the text would match the explanation forever.
    """
    for node in ast.walk(ast.parse(_source())):
        if (isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == func):
            for sub in ast.walk(node):
                if not isinstance(sub, ast.Assign):
                    continue
                if not (isinstance(sub.value, ast.Constant)
                        and sub.value.value is False):
                    continue
                for tgt in sub.targets:
                    if (isinstance(tgt, ast.Attribute) and tgt.attr == attr
                            and isinstance(tgt.value, ast.Name)
                            and tgt.value.id == 'p_state'):
                        return True
            return False
    raise AssertionError(f'{func} not found in web/pages/parallels.py')


def test_the_shared_witness_search_never_clears_a_standing_stop():
    """It cleared `is_cancelled` on entry -- and it is reached from the SEED
    search and from every auto-expand round, not only from a button. So Stop
    pressed while the seed was running was silently undone and the run
    carried on through every pending witness."""
    assert not _clears_flag_false('_search_pending_witnesses', 'is_cancelled'), (
        '_search_pending_witnesses must not clear is_cancelled: a Stop the '
        'user already pressed would be undone'
    )


def test_the_shared_witness_search_returns_early_on_a_standing_stop():
    """Not clearing the flag is only half of it -- it must also be OBEYED
    before any witness is dispatched."""
    for node in ast.walk(ast.parse(_source())):
        if (isinstance(node, ast.AsyncFunctionDef)
                and node.name == '_search_pending_witnesses'):
            guards = [
                n for n in node.body
                if isinstance(n, ast.If)
                and isinstance(n.test, ast.Attribute)
                and n.test.attr == 'is_cancelled'
                and any(isinstance(b, ast.Return) for b in n.body)
            ]
            assert guards, (
                'no top-level `if p_state.is_cancelled: return` guard -- a '
                'standing Stop would be ignored'
            )
            # Before the witness loop, not after it.
            loops = [n.lineno for n in node.body
                     if isinstance(n, (ast.For, ast.Try))]
            assert not loops or guards[0].lineno < min(loops)
            return
    raise AssertionError('_search_pending_witnesses not found')


def test_only_explicit_user_actions_clear_the_stop():
    """`_clear_stop` exists so the clearing sites are countable. The three
    explicit entry points -- Retry, Run auto-expand, and the two buttons --
    clear it; nothing on the seed or auto-expand-round path does."""
    assert '_clear_stop' in _func_names()
    assert '_clear_stop' in _calls_in('_retry_witness')
    assert '_clear_stop' in _calls_in('_run_auto_expand')
    # ...and the round loop itself must not, or Stop between rounds dies.
    assert not _clears_flag_false('_run_auto_expand', 'is_cancelled')
    assert not _clears_flag_false('_promote_checked', 'is_cancelled')
    # Both witness buttons route through it (Search now, Search with these too).
    assert _call_sites_of('_clear_stop') >= 4


# ---------------------------------------------------------------------------
# A restored manuscript witness must not search the empty string.
# ---------------------------------------------------------------------------

def test_a_restored_manuscript_witness_needs_its_text_back():
    """The snapshot drops a manuscript witness's text on purpose (the corpus
    still has it). Nothing re-fetched it, so after a reload that witness
    searched '' and came back `searched, 0 matches` -- a false negative
    indistinguishable from a real one."""
    from web.pages.parallels import witnesses_needing_text
    restored = {'id': 'w1', 'kind': 'manuscript', 'sys_id': '990001234560',
                'text': '', 'status': 'pending'}
    assert witnesses_needing_text([restored]) == [restored]


def test_a_pasted_witness_is_never_refetched():
    """Its text existed nowhere but the snapshot. There is nothing to fetch,
    and asking the corpus for one would silently return someone else's."""
    from web.pages.parallels import witnesses_needing_text
    assert witnesses_needing_text(
        [{'id': 'w1', 'kind': 'pasted', 'text': '', 'sys_id': None}]) == []


def test_a_manuscript_witness_that_still_has_text_is_left_alone():
    """Re-fetching a witness that already has its text would spend a Tantivy
    read per witness on every single search."""
    from web.pages.parallels import witnesses_needing_text
    assert witnesses_needing_text([{
        'id': 'w1', 'kind': 'manuscript', 'sys_id': '990001234560',
        'text': '\u05d1\u05e8\u05d0\u05e9\u05d9\u05ea'}]) == []


def test_a_manuscript_witness_with_no_sys_id_cannot_be_refetched():
    from web.pages.parallels import witnesses_needing_text
    assert witnesses_needing_text(
        [{'id': 'w1', 'kind': 'manuscript', 'sys_id': None, 'text': ''}]) == []


def test_whitespace_only_text_counts_as_no_text():
    from web.pages.parallels import witnesses_needing_text
    got = witnesses_needing_text(
        [{'id': 'w1', 'kind': 'manuscript', 'sys_id': '990001234560',
          'text': '   \n  '}])
    assert len(got) == 1


def test_the_rehydrator_is_wired_in_before_any_witness_is_dispatched():
    """A predicate nothing calls is a comment."""
    assert '_rehydrate_manuscript_witnesses' in _calls_in(
        '_search_pending_witnesses')
    calls = _calls_in('_rehydrate_manuscript_witnesses')
    assert 'witnesses_needing_text' in calls, 'the rule must be the shared one'
    assert 'collect_witness_texts' in calls
    assert 'io_bound' in calls, (
        'Tantivy lookups on the single uvicorn loop stall every other request'
    )


def test_an_empty_witness_is_failed_rather_than_reported_as_zero_matches():
    """The backstop for anything the re-fetch could not recover. Reporting
    "0 matches" for a search that never ran is the failure this whole fix is
    about; the witness must say it could not be searched."""
    found = False
    for node in ast.walk(ast.parse(_source())):
        if (isinstance(node, ast.AsyncFunctionDef)
                and node.name == '_search_pending_witnesses'):
            for sub in ast.walk(node):
                if not isinstance(sub, ast.If):
                    continue
                if "'strip'" not in ast.dump(sub.test):
                    continue
                body = ast.dump(ast.Module(body=sub.body, type_ignores=[]))
                if "'failed'" in body and 'Continue' in body:
                    found = True
    assert found, 'no empty-text guard that fails the witness and skips it'


# ---------------------------------------------------------------------------
# Restoring the witness list from a tab snapshot.
#
# These exist because a mutation sweep found the rules covered by NOTHING:
# reverting the drop rule to the obvious `if not text.strip()` -- which
# deletes every restored manuscript witness, since the snapshot stores those
# without text on purpose -- left the whole page suite green.
# ---------------------------------------------------------------------------

def _restore(raw, cap=None):
    from web.pages.parallels import restore_witness_entries
    return restore_witness_entries(raw, 'Pasted text', cap)


def test_a_manuscript_witness_survives_the_snapshot_without_its_text():
    """`_persist_active_snapshot` drops a manuscript witness's text on
    purpose. If restore treats missing text as "unusable", a reloaded
    17-witness search silently comes back with only the pasted ones."""
    got = _restore([{'id': 'w1', 'kind': 'manuscript',
                     'sys_id': '990001234560', 'text': '', 'label': 'T-S 1.1'}])
    assert len(got) == 1
    assert got[0]['sys_id'] == '990001234560'
    assert got[0]['status'] == 'pending'


def test_a_pasted_witness_with_no_text_is_dropped():
    """Nothing in the world can recover it, so it must not sit in the list
    pretending it can be searched."""
    assert _restore([{'id': 'w1', 'kind': 'pasted', 'text': '   '}]) == []


def test_a_manuscript_witness_with_no_sys_id_and_no_text_is_dropped():
    assert _restore([{'id': 'w1', 'kind': 'manuscript',
                      'sys_id': None, 'text': ''}]) == []


def test_ids_are_renumbered_over_the_survivors():
    """Reusing the stored ids would leave gaps that `_witness_new_id` can
    re-issue, and two witnesses sharing an id corrupt the per-witness row
    cache -- one silently overwrites the other's rows."""
    got = _restore([
        {'id': 'w1', 'kind': 'pasted', 'text': 'aleph bet gimel'},
        {'id': 'w2', 'kind': 'pasted', 'text': ''},            # dropped
        {'id': 'w3', 'kind': 'pasted', 'text': 'dalet he vav'},
    ])
    assert [w['id'] for w in got] == ['w1', 'w2']


def test_the_seed_stamp_survives_the_round_trip():
    """Without it every restored witness looks like it belongs to whatever
    text is in the box -- the bug the owner hit with Antiochus witnesses
    sitting under a Birkat Hamazon query."""
    got = _restore([{'id': 'w1', 'kind': 'pasted', 'text': 'aleph bet gimel',
                     'seed_digest': 'deadbeefcafe0001'}])
    assert got[0]['seed_digest'] == 'deadbeefcafe0001'


def test_a_witness_with_no_label_falls_back_to_its_shelfmark_then_the_default():
    got = _restore([
        {'id': 'w1', 'kind': 'manuscript', 'sys_id': '990001234560', 'text': ''},
        {'id': 'w2', 'kind': 'pasted', 'text': 'aleph bet gimel'},
    ])
    assert got[0]['label'] == '990001234560'
    assert got[1]['label'] == 'Pasted text'


def test_the_cap_bounds_the_restored_list():
    got = _restore([{'id': f'w{i}', 'kind': 'pasted', 'text': 'aleph bet gimel'}
                    for i in range(40)], cap=3)
    assert len(got) == 3


def test_junk_in_the_snapshot_costs_the_entry_not_the_page():
    """A snapshot is user-adjacent storage; one bad entry must not take the
    restore down with it."""
    assert _restore(None) == []
    assert _restore('not a list') == []
    got = _restore(['junk', 42, {'kind': 'pasted', 'text': 'aleph bet gimel'}])
    assert len(got) == 1


def test_every_restored_witness_comes_back_pending():
    """The snapshot holds the FUSED rows, so per-witness ranks cannot be
    recovered. A fusion rebuilt from partial inputs would be quietly wrong
    rather than visibly absent."""
    got = _restore([{'id': 'w1', 'kind': 'pasted', 'text': 'aleph bet gimel',
                     'status': 'searched', 'hits': 99}])
    assert got[0]['status'] == 'pending'
    assert got[0]['hits'] == 0


def test_the_restore_closure_uses_the_shared_normaliser():
    """A pure function nothing calls is a comment."""
    assert 'restore_witness_entries' in _calls_in(
        '_restore_witnesses_from_snapshot')


# ---------------------------------------------------------------------------
# Promotion: the right stamp, and only once.
# ---------------------------------------------------------------------------

def test_a_promoted_witness_is_stamped_with_the_search_it_came_from():
    """`_seed_digest()` reads the LIVE textarea. Search X, edit the box to Y
    without searching, then promote a row from X's list: the witness was
    stamped digest(Y) and the staleness check called it native -- the same
    cross-work contamination the stamp exists to prevent, entering by a
    second door. It must take the digest of the search on screen."""
    src = _func_source('_promote_checked')
    assert "(p_state.last_passage_ctx or {}).get(" in src, (
        'the stamp must come from the dispatched search, not the live box'
    )
    assert 'seed_digest=promoted_digest' in src, (
        'the captured digest must actually be passed to _add_witness'
    )


def test_the_promoted_stamp_is_captured_before_the_await():
    """This repo has a recorded class of bug where a value read after an
    `await` describes a configuration the search never used. The digest is
    read from state that the user can change during the fetch."""
    src = _func_source('_promote_checked')
    capture = src.index('promoted_digest = ')
    await_at = src.index('await run.io_bound')
    assert capture < await_at, (
        'promoted_digest is read after the await -- it would describe the '
        'wrong search'
    )


def test_the_search_context_carries_the_seed_digest():
    """Nothing else records which text produced the rows on screen."""
    src = _func_source('execute_parallels')
    assert "'seed_digest': _text_digest_of(text)" in src, (
        'last_passage_ctx must carry the dispatched seed digest'
    )


def test_promotion_is_not_re_entrant():
    """Two overlapping runs both read the pre-fetch witness list and both add
    the same manuscripts. A duplicate witness contributes twice to
    witness_count and to the RRF sum -- a wrong number, not a wasted search
    (the function's own comment says so about the checkbox path)."""
    src = _func_source('_promote_checked')
    assert 'if p_state.promoting:' in src
    assert 'p_state.promoting = True' in src
    assert 'p_state.promoting = False' in src
    # Released on the failure path too, or one exception disables promotion
    # for the rest of the session.
    guard = src[src.index('p_state.promoting = True'):]
    assert 'finally:' in guard[:guard.index('p_state.promoting = False')], (
        'the flag must be released in a finally, or a failed fetch wedges it'
    )


def test_the_witness_list_is_re_read_after_the_fetch():
    """The re-entrancy guard stops a second promotion, but a witness can also
    arrive from the Add dialog while the fetch is in flight."""
    src = _func_source('_promote_checked')
    after = src[src.index('await run.io_bound'):]
    assert '_already = {' in after, (
        'the already-a-witness set must be recomputed after the await'
    )
    assert 'sid in _already' in after, 'and actually consulted'


# ---------------------------------------------------------------------------
# Reset means reset, and a mutation reaches storage.
# ---------------------------------------------------------------------------

def test_reset_clears_the_witness_list():
    """Reset clears the source text, the results, the exclusions, every
    persisted key and the tab snapshot, then says "Composition reset". The
    witnesses survived all of it -- and since `_clear_active_snapshot()` ran,
    the screen and storage then disagreed: a reload resurrected nothing while
    the panel still showed seventeen witnesses."""
    src = _func_source('_reset_parallels')
    assert 'p_state.witnesses = []' in src
    assert 'p_state.witness_rows = {}' in src
    assert 'p_state.witness_filtered = {}' in src
    assert 'p_state.checked_for_promotion = set()' in src
    assert 'p_state.witness_seq = 0' in src, (
        'a stale sequence would re-issue ids the row cache still knows'
    )


def test_reset_repaints_the_panel():
    """Clearing the list without repainting leaves the old witnesses on
    screen -- the state is right and the user cannot tell."""
    calls = _calls_in('_reset_parallels')
    assert '_refresh_witness_panel' in calls
    assert '_refresh_promotion_bar' in calls


def test_reset_drops_the_search_context():
    """`last_passage_ctx` is what a witness search runs with. Left behind, a
    witness added after a Reset would be searched at the width, depth and
    library scope of a search the user just deleted."""
    assert 'p_state.last_passage_ctx = {}' in _func_source('_reset_parallels')


def test_adding_and_removing_a_witness_reaches_storage():
    """`_persist_witness_state` had ONE caller, at the end of
    `_search_pending_witnesses`. So the snapshot was only in step at the end
    of a witness search: paste three witnesses and reload before searching
    and they are gone; remove one after a search and reload and it is back,
    rows and all."""
    assert '_persist_witness_state' in _calls_in('_remove_witness')
    assert '_persist_witness_state' in _calls_in('_open_add_witness_dialog')
    assert '_persist_witness_state' in _calls_in('_revive_stale_witnesses')
    assert '_persist_witness_state' in _calls_in('_search_pending_witnesses')


def test_the_panel_refresh_does_not_persist():
    """`_refresh_witness_panel` runs once per witness during a search -- it
    renders the progress line -- so persisting there would serialise the
    whole result set seventeen times in a 17-witness run. The mutation
    boundaries are called explicitly for exactly this reason."""
    assert '_persist_witness_state' not in _calls_in('_refresh_witness_panel')


# ---------------------------------------------------------------------------
# The add dialog says what it dropped, and refuses what cannot be searched.
# ---------------------------------------------------------------------------

def test_a_bulk_paste_reports_what_it_skipped():
    """`skipped` was computed and then never mentioned unless the user pressed
    Preview. Paste seventeen witnesses of which three are under the three-word
    floor and fourteen appear, with nothing to say the file was not fully
    read."""
    src = _func_source('_open_add_witness_dialog')
    commit = src[src.index('def _commit'):]
    assert 'if skipped:' in commit, (
        'the commit path must report the drop, not only the Preview button'
    )
    assert "'({n} skipped: too short)'" in commit


def test_an_over_long_witness_is_refused_before_it_is_searched():
    src = _func_source('_open_add_witness_dialog')
    assert 'MAX_WITNESS_CHARS' in src
    assert "'Witness text is too long (max {cap} characters)'" in src


def test_an_over_long_promoted_manuscript_is_skipped_and_named():
    """Truncating it would search half a manuscript as if it were the whole
    one -- a worse answer than none, and an invisible one."""
    src = _func_source('_promote_checked')
    assert 'MAX_WITNESS_CHARS' in src
    assert 'too_long' in src
    assert "'Witness text is too long (max {cap} characters)'" in src


def test_the_export_meta_records_that_the_rows_are_fused():
    """Without it the JSON export re-ranks the groups by summed matched
    letters -- a different order than the page showed -- and drops the witness
    facts, though every row still carries them."""
    assert "meta['multi_witness']" in _func_source('_refresh_export_payload')


def _guard_reports(func: str, flag: str) -> bool:
    """Is there an `if <flag>:` in `func` whose body notifies?

    From the AST, because the claim is that the report FIRES. A substring
    assertion cannot tell `if too_long:` from `if False:` -- both still
    contain the notify and its message literal, which is how three of these
    mutations came back green the first time.
    """
    for node in ast.walk(ast.parse(_source())):
        if not (isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == func):
            continue
        for sub in ast.walk(node):
            if not (isinstance(sub, ast.If) and isinstance(sub.test, ast.Name)
                    and sub.test.id == flag):
                continue
            for inner in ast.walk(ast.Module(body=sub.body, type_ignores=[])):
                if (isinstance(inner, ast.Call)
                        and isinstance(inner.func, ast.Attribute)
                        and inner.func.attr == 'notify'):
                    return True
        return False
    raise AssertionError(f'{func} not found in web/pages/parallels.py')


def test_the_paste_path_uses_the_shared_length_rule():
    """Inline, the filter was covered only by a substring: replacing it with
    `[]` -- which searches every over-long text -- left the suite green,
    because the constant's name still appeared two lines above."""
    assert 'split_by_length' in _calls_in('_open_add_witness_dialog')
    assert _guard_reports('_open_add_witness_dialog', '_long'), (
        'the over-long paste is dropped without a word'
    )


def test_the_promotion_path_uses_the_same_length_rule():
    """A manuscript fetched from the corpus and one pasted by hand must not
    disagree about what is searchable."""
    assert 'split_by_length' in _calls_in('_promote_checked')
    assert _guard_reports('_promote_checked', 'too_long')


def test_the_bulk_paste_drop_is_reported_from_a_real_guard():
    assert _guard_reports('_open_add_witness_dialog', 'skipped')


# ---------------------------------------------------------------------------
# PR #329 review round 1.
# ---------------------------------------------------------------------------


def _parallels_ast():
    import ast

    src = _source()
    return ast.parse(src), src


def _enclosing_ifs(tree, needle_fn):
    """Every `ast.If` on the path from the module root to the node
    `needle_fn` selects, outermost first.

    Written as a walk rather than a source-text search because the defect
    being pinned IS a nesting relationship: `'_search_pending_witnesses' in
    src` is true whether the call sits inside the result guard or outside it,
    which is exactly how this one shipped.
    """
    import ast
    found = []

    def walk(node, chain):
        if needle_fn(node):
            found.append(list(chain))
        for child in ast.iter_child_nodes(node):
            walk(child, chain + [node] if isinstance(node, ast.If) else chain)

    walk(tree, [])
    return found


def test_the_seed_witness_dispatch_is_not_nested_under_the_result_guard():
    """A seed that matched nothing left every witness unsearched.

    The dispatch sat inside `if main_results or filtered_results:`, so the one
    case the whole feature exists for -- this witness finds what that one
    missed -- reported "No results" and stopped. Measured: a single BH witness
    reaches 56.7% of the census where the fused seventeen reach 74.1%, so a
    seed on the empty side of that gap is ordinary.

    RED when the `if captured_passage_mode:` block is moved back inside the
    guard. A substring assertion cannot fail that mutation, since the call
    survives it verbatim.
    """
    import ast
    tree, _src = _parallels_ast()

    def is_dispatch(node):
        return (isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == '_search_pending_witnesses')

    chains = _enclosing_ifs(tree, is_dispatch)
    assert chains, 'no call to _search_pending_witnesses found at all'

    def guard_names(if_node):
        return {n.id for n in ast.walk(if_node.test) if isinstance(n, ast.Name)}

    # The dispatch reached from `update_ui` is the one under test: the others
    # are the Retry and auto-expand paths, which are button handlers and were
    # never inside the guard.
    offenders = []
    for chain in chains:
        for if_node in chain:
            names = guard_names(if_node)
            if 'main_results' in names or 'filtered_results' in names:
                offenders.append(ast.unparse(if_node.test))
    assert not offenders, (
        'the witness dispatch is nested under a seed-result guard '
        f'({offenders}) -- an empty seed will leave every witness unsearched'
    )


def test_an_empty_seed_is_still_stored_as_a_searched_witness():
    """`witness_rows[WITNESS_SEED_ID]` must be assigned on BOTH paths.

    Skipping it for an empty seed would drop the seed out of
    `_searched_witness_count()`, which hides the fusion sort options and, with
    exactly one other witness, turns the fusion into a single-witness
    passthrough -- the rows would render, unfused, with no sign anything was
    wrong.

    RED if the assignment is made conditional on the seed having hits.
    """
    import ast
    tree, _src = _parallels_ast()
    assigns = [
        n for n in ast.walk(tree)
        if isinstance(n, ast.Assign)
        and any(isinstance(t, ast.Subscript)
                and isinstance(t.value, ast.Attribute)
                and t.value.attr == 'witness_rows'
                and isinstance(t.slice, ast.Name)
                and t.slice.id == 'WITNESS_SEED_ID'
                for t in n.targets)
    ]
    assert len(assigns) == 1, (
        f'expected exactly one seed-row assignment, found {len(assigns)}'
    )
    chains = _enclosing_ifs(tree, lambda n: n is assigns[0])
    guards = {name for chain in chains for if_node in chain
              for name in {x.id for x in ast.walk(if_node.test)
                           if isinstance(x, ast.Name)}}
    assert 'main_results' not in guards and 'filtered_results' not in guards, (
        'the seed row-set is stored only when the seed had hits'
    )


def test_the_dispatch_cap_follows_the_searched_depth_not_the_dropdown():
    """A witness runs at the depth of the LAST SEARCH.

    `_run_one_witness_search` takes its depth from `last_passage_ctx`, so
    capping against the dropdown would bound a depth nothing was going to
    use. RED if the two arguments are swapped.
    """
    from web.pages.parallels import witness_depth_cap
    # ctx wins outright...
    assert witness_depth_cap({'depth': 'normal'}, 'deepest') == 25
    assert witness_depth_cap({'depth': 'deepest'}, 'normal') == 4
    # ...and the dropdown is consulted only when there is no ctx yet.
    assert witness_depth_cap({}, 'deep') == 8
    assert witness_depth_cap(None, None) == 25
    # An unknown depth falls back to the flat cap rather than to zero, which
    # would refuse every batch.
    assert witness_depth_cap({'depth': 'nonsense'}, None) == 25


def test_a_pending_batch_over_the_depth_cap_is_refused():
    """25 witnesses added at normal depth all reset to `pending` when the seed
    is re-run, so re-running at `deepest` dispatched 25 x ~19 s from one
    click, against a documented deepest cap of 4.

    RED on `>` -> `>=` (which would refuse an exactly-full batch), on
    `>` -> `<`, and on deleting the check.
    """
    from web.pages.parallels import witnesses_over_dispatch_cap
    deepest = {'depth': 'deepest'}
    assert witnesses_over_dispatch_cap([1] * 25, deepest, None) == (25, 4)
    assert witnesses_over_dispatch_cap([1] * 5, deepest, None) == (5, 4)
    # Exactly at the cap is allowed -- the cap is a maximum, not a limit
    # below it.
    assert witnesses_over_dispatch_cap([1] * 4, deepest, None) is None
    assert witnesses_over_dispatch_cap([1] * 3, deepest, None) is None
    # The same batch is fine at the depth it was gathered at.
    assert witnesses_over_dispatch_cap([1] * 25, {'depth': 'normal'},
                                       None) is None
    assert witnesses_over_dispatch_cap([], deepest, None) is None
    assert witnesses_over_dispatch_cap(None, deepest, None) is None


def test_the_dispatch_actually_consults_the_cap():
    """The rule above is only worth anything if `_search_pending_witnesses`
    calls it. RED if the call is removed from the dispatch path.
    """
    import ast
    tree, _src = _parallels_ast()
    fn = next(
        (n for n in ast.walk(tree)
         if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
         and n.name == '_search_pending_witnesses'), None)
    assert fn is not None, '_search_pending_witnesses not found'
    called = {n.func.id for n in ast.walk(fn)
              if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
    assert 'witnesses_over_dispatch_cap' in called, (
        'the dispatch path does not check the depth cap; enforcing it only '
        'while ADDING witnesses bounds the wrong quantity'
    )
    # ...and it must refuse BEFORE dispatching any witness, not after.
    body_src = ast.unparse(fn)
    i_cap = body_src.find('witnesses_over_dispatch_cap')
    i_run = body_src.find('_run_one_witness_search')
    assert 0 <= i_cap < i_run, (
        'the cap is checked after the first witness has already been searched'
    )


def test_the_export_identity_is_built_whether_or_not_the_seed_matched():
    """`last_export_meta` / `last_fingerprint_kwargs` describe the search that
    RAN, and they are built from dispatch-time captures alone -- so nesting
    them under `if main_results or filtered_results:` made a search's identity
    depend on its results.

    Harmless while an empty seed ended the run. It stopped being harmless the
    moment witnesses were dispatched past an empty seed: `_refresh_export_payload`
    returns early on empty meta, so the fused rows rendered with the export
    buttons enabled and no payload behind them (the download 400s) -- and where
    a PREVIOUS search had left meta behind, the new rows were published under
    that search's identity, which is precisely the mix-up the fingerprint
    exists to prevent.

    RED if the block is moved back inside the guard.
    """
    import ast
    tree, _src = _parallels_ast()

    def sets_export_meta(node):
        return (isinstance(node, ast.Assign)
                and any(isinstance(t, ast.Attribute)
                        and t.attr == 'last_export_meta'
                        for t in node.targets))

    chains = _enclosing_ifs(tree, sets_export_meta)
    assert chains, 'nothing assigns p_state.last_export_meta'
    offenders = []
    for chain in chains:
        for if_node in chain:
            names = {n.id for n in ast.walk(if_node.test)
                     if isinstance(n, ast.Name)}
            if 'main_results' in names or 'filtered_results' in names:
                offenders.append(ast.unparse(if_node.test))
    assert not offenders, (
        'the export identity is built only when the seed matched '
        f'({offenders}) -- a witness run past an empty seed then publishes '
        'its rows under a stale identity, or under none'
    )


def test_the_search_fingerprint_is_stamped_whether_or_not_the_seed_matched():
    """Same rule for the identity itself: `last_fingerprint_kwargs` is what
    `_recompute_search_identity` re-runs with the witnesses that produced
    rows. Left unset by an empty seed, that helper falls back to whatever
    `search_fingerprint` already held -- another search's."""
    import ast
    tree, _src = _parallels_ast()

    def sets_kwargs(node):
        return (isinstance(node, ast.Assign)
                and any(isinstance(t, ast.Attribute)
                        and t.attr == 'last_fingerprint_kwargs'
                        for t in node.targets))

    chains = _enclosing_ifs(tree, sets_kwargs)
    assert chains, 'nothing assigns p_state.last_fingerprint_kwargs'
    guards = {name for chain in chains for if_node in chain
              for name in {x.id for x in ast.walk(if_node.test)
                           if isinstance(x, ast.Name)}}
    assert 'main_results' not in guards and 'filtered_results' not in guards, (
        'the search identity is stamped only when the seed matched'
    )


def test_rehydrated_witness_text_is_capped_like_every_other_witness():
    """`MAX_WITNESS_CHARS` was enforced in the add dialog and at promotion,
    but a manuscript witness is stored WITHOUT its text and re-fetched on
    restore -- and that path applied no cap at all.

    What comes back is not what was promoted: the refetch reads whatever
    headers the RESTORED row set holds, and falls back to
    `get_full_manuscript`, which returns the whole manuscript. So a reload
    could turn a capped witness into an uncapped one, and the cap exists
    because an over-long witness spends its entire 30 s ceiling and fails.

    Same shape as the depth-cap finding: enforced at the doors, absent at
    dispatch. RED if the rehydrate stops consulting the rule.
    """
    import ast
    src = _func_source('_rehydrate_manuscript_witnesses')
    tree = ast.parse(src.lstrip())
    # A CALL, not the name: `'split_by_length' in src` stayed true when the
    # call was replaced, because the import line above still carries the
    # name -- the mutation run caught this test passing on the defect.
    calls = [n for n in ast.walk(tree)
             if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
             and n.func.id == 'split_by_length']
    assert calls, (
        'the rehydrate path does not CALL the witness length cap (the name '
        'may still be imported, which is what made an earlier version of '
        'this assertion vacuous)'
    )
    # ...and it must be applied to what was FETCHED, not to a leftover.
    args = ' '.join(ast.unparse(a) for c in calls for a in c.args)
    assert '_text' in args, (
        f'the cap is applied to something other than the fetched text: {args!r}'
    )


def test_an_over_long_rehydrated_witness_is_emptied_not_truncated():
    """Half a manuscript searched as if it were the whole one is a worse
    answer than none, and an invisible one -- the rule `split_by_length`'s
    own docstring states. RED if the branch starts assigning a slice."""
    import ast
    src = _func_source('_rehydrate_manuscript_witnesses')
    tree = ast.parse(src.lstrip())
    slices = [n for n in ast.walk(tree)
              if isinstance(n, ast.Subscript) and isinstance(n.slice, ast.Slice)]
    assert not slices, (
        'the rehydrate path truncates a witness instead of refusing it: '
        + repr([ast.unparse(n) for n in slices])
    )


def test_the_generic_load_failure_never_overwrites_a_specific_one():
    """Rehydration empties an over-long witness and records WHY. The dispatch
    loop then fails it -- and used to stamp "Could not load text for this
    manuscript" over the true reason, so the panel offered Retry for a
    condition retrying cannot fix.

    RED if the guard around the generic message is removed.
    """
    import ast
    src = _func_source('_search_pending_witnesses')
    tree = ast.parse(src.lstrip())
    generic = [
        n for n in ast.walk(tree)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
        and n.func.id == 'tr'
        and n.args and isinstance(n.args[0], ast.Constant)
        and 'Could not load text' in str(n.args[0].value)
    ]
    assert len(generic) == 1, f'expected one generic message, got {len(generic)}'

    # It must sit under a test that checks for an existing error.
    guarded = False
    for node in ast.walk(tree):
        if not isinstance(node, ast.If):
            continue
        test_src = ast.unparse(node.test)
        if 'error' not in test_src:
            continue
        if any(g is n for n in ast.walk(node) for g in generic):
            guarded = True
    assert guarded, (
        'the generic load-failure message is unconditional, so it replaces '
        'the specific reason rehydration recorded'
    )


def test_promotion_says_when_it_drops_manuscripts_over_the_cap():
    """The add-witness dialog truncates a bulk paste and SAYS SO; promotion
    did `sys_ids = sys_ids[:room]` with a message only when room was already
    zero. Check ten manuscripts with room for three and seven vanished --
    the silent shrinking the auto-expand round refuses to do on the grounds
    that a control quietly doing less than it says is a lie.

    RED if the notify is removed or moved after the truncation.
    """
    import ast
    src = _func_source('_promote_checked')
    tree = ast.parse(src.lstrip())

    trunc = [n for n in ast.walk(tree)
             if isinstance(n, ast.Assign)
             and any(isinstance(t, ast.Name) and t.id == 'sys_ids'
                     for t in n.targets)
             and isinstance(n.value, ast.Subscript)
             and isinstance(n.value.slice, ast.Slice)]
    assert len(trunc) == 1, f'expected one cap truncation, got {len(trunc)}'

    # Some `if` guarding a notify must compare the request against the room.
    reported = False
    for node in ast.walk(tree):
        if not isinstance(node, ast.If):
            continue
        test_src = ast.unparse(node.test)
        if 'room' not in test_src or 'sys_ids' not in test_src:
            continue
        calls = [ast.unparse(c.func) for c in ast.walk(node)
                 if isinstance(c, ast.Call)]
        if any('notify' in c for c in calls):
            reported = True
    assert reported, (
        'promotion truncates to the cap without telling the user how many '
        'of their checked manuscripts were dropped'
    )
