# -*- coding: utf-8 -*-
"""The shelf picker's limits when ticking, and the chips' names (``shared/shelfmark_tree_limits.py``;
owner, 2026-11-21: thousands of shelfmarks at once is not reasonable; the Reading Desk is limited by
pages, not by a number of shelfmarks; the chips carry the library's code)."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from shared import shelfmark_tree_limits as lim
from shared.shelfmark_tree_selection import LEAF, NODE, Chip

PAGES = {'a': 600, 'b': 100, 'c': 500, 'd': 1, 'e': 0}


def _pages(sid):
    return PAGES[sid]


def test_the_union_is_counted_once_with_what_the_host_holds():
    # the desk holds A (600 pages); A+B ticked is 700, not 1,300
    limit = lim.SelectionLimit(max_pages=1000, pages_of=_pages, base_ids=lambda: {'a'})
    assert limit.totals({'a', 'b'}) == (2, 700)
    assert limit.refusal(set(), {'a', 'b'}) is None
    # the manuscript shown that a new desk begins with counts too: 600 + 500 passes 1,000
    refused = limit.refusal(set(), {'c'})
    assert refused == lim.Refusal('pages', 1000, 1100)


def test_the_base_is_read_at_each_question():
    held = {'ids': set()}
    limit = lim.SelectionLimit(max_items=2, base_ids=lambda: held['ids'])
    assert limit.refusal(set(), {'a', 'b'}) is None
    held['ids'] = {'c'}                                   # the desk grew meanwhile
    assert limit.refusal(set(), {'a', 'b'}) == lim.Refusal('items', 2, 3)


def test_removals_always_pass_even_over_a_limit():
    limit = lim.SelectionLimit(max_items=2)
    over = {'a', 'b', 'c', 'd'}                           # kept from before the limits
    assert limit.refusal(over, {'a', 'b', 'c'}) is None   # unticking one
    assert limit.refusal(over, over) is None              # nothing added
    assert limit.refusal(over, over | {'e'}) == lim.Refusal('items', 2, 5)


def test_an_unknown_or_broken_page_count_is_one_page():
    def broken(sid):
        raise RuntimeError('no index')
    limit = lim.SelectionLimit(max_pages=10, pages_of=broken)
    assert limit.totals({'a', 'b'}) == (2, 2)
    assert lim.SelectionLimit(max_pages=10, pages_of=_pages).totals({'e'}) == (1, 1)
    with pytest.raises(ValueError):
        lim.SelectionLimit(max_pages=10)


def test_a_host_counting_items_counts_each_tick_as_one_more():
    # Codex on #82: a Puzzle canvas holding two folios of one manuscript holds two fragments, and a
    # manuscript ticked again is one more fragment
    canvas = {'items': 28}
    limit = lim.limit_for('puzzle', base_items=lambda: canvas['items'])
    assert limit.totals({'a', 'b'}) == (30, 0)
    assert limit.refusal(set(), {'a', 'b'}) is None
    assert limit.refusal({'a', 'b'}, {'a', 'b', 'c'}) == lim.Refusal('fragments', 30, 31)
    assert limit.take(['a', 'b', 'c']) == ['a', 'b']
    canvas['items'] = 30                                   # full: nothing more, removals still pass
    assert limit.take(['a']) == []
    assert limit.refusal({'a', 'b'}, {'a'}) is None
    with pytest.raises(ValueError):
        lim.SelectionLimit(max_items=3, base_ids=lambda: (), base_items=lambda: 1)

    def broken():
        raise RuntimeError
    assert lim.SelectionLimit(max_items=1, base_items=broken).totals({'a'}) == (1, 0)


def test_an_untick_passes_however_the_host_moves_meanwhile():
    # Codex on #82: the host's count read twice (28 for "before", 30 for "after" as typed adds
    # landed) refused an untick. A change that only takes out passes before anything is counted.
    reads = iter([28, 30, 30, 30])
    limit = lim.limit_for('puzzle', base_items=lambda: next(reads))
    assert limit.refusal({'a', 'b'}, {'a'}) is None
    assert limit.refusal({'a', 'b'}, {'a', 'b'}) is None
    counts = {'n': 0}

    def counted():
        counts['n'] += 1
        return 28
    once = lim.limit_for('puzzle', base_items=counted)
    assert once.refusal({'a'}, {'a', 'b'}) is None and counts['n'] == 1     # read once for both totals


def test_a_broken_base_holds_nothing():
    def broken():
        raise RuntimeError('desk gone')
    assert lim.SelectionLimit(max_items=1, base_ids=broken).base() == frozenset()


def test_a_node_too_big_is_read_from_its_stored_count():
    desk = lim.limit_for('reading_desk', pages_of=_pages)
    assert desk.node_too_big(SimpleNamespace(manuscripts=31))          # past 30 manuscripts
    assert not desk.node_too_big(SimpleNamespace(manuscripts=30))
    lists = lim.limit_for('lists')
    assert lists.node_too_big(SimpleNamespace(manuscripts=67_891))     # T-S AS
    assert not lists.node_too_big(SimpleNamespace(manuscripts=2_000))
    assert not lists.node_too_big(None)
    pages_only = lim.SelectionLimit(max_pages=1000, pages_of=_pages)
    assert pages_only.node_too_big(SimpleNamespace(manuscripts=1001))  # each at least one page
    assert desk.count_too_big(31) and not desk.count_too_big(30) and not desk.count_too_big(0)


def test_select_all_over_a_mixed_node_counts_its_own_shelfmarks():
    # Codex on #80: a collection with thousands in its volumes and a few shelfmarks of its own;
    # "Select all" (scope_for: DIRECT) takes those few, which fit
    from shared.shelfmark_tree_selection import ALL, DIRECT
    desk = lim.limit_for('reading_desk', pages_of=_pages)
    mixed = SimpleNamespace(id='col:1', manuscripts=5000, direct_leaves=10)
    assert desk.node_too_big(mixed) and desk.node_too_big(mixed, ALL)
    assert not desk.node_too_big(mixed, DIRECT)
    # direct_leaves counts placements: 40 rows naming 25 distinct manuscripts fit 30 (Codex on #80)
    dup = SimpleNamespace(id='col:2', manuscripts=5000, direct_leaves=40)
    service = SimpleNamespace(members_under=lambda node_id, direct=False: frozenset(f's{i}' for i in range(25)))
    assert not desk.node_too_big(dup, DIRECT, service)
    service.members_under = lambda node_id, direct=False: frozenset(f's{i}' for i in range(31))
    assert desk.node_too_big(dup, DIRECT, service)
    assert not desk.node_too_big(dup, DIRECT)                         # no service: the tick's check decides


def test_the_real_tree_counts_a_mixed_nodes_own_shelfmarks(tmp_path):
    from shared.shelfmark_tree_selection import DIRECT
    from shared.shelfmark_tree_service import ShelfmarkTreeService
    from tests.shelfmark_tree_fixture import build_fixture
    db, _libraries, _meta = build_fixture(tmp_path)
    svc = ShelfmarkTreeService(str(db))
    try:
        mixed = next(n for n in (svc.node(i) for i in ('col:10', 'col:11', 'col:12')) if n and n.direct_leaves)
        own = len(svc.members_under(mixed.id, True))
        limit = lim.SelectionLimit(max_items=own)
        assert not limit.node_too_big(mixed, DIRECT, svc)
        assert lim.SelectionLimit(max_items=own - 1).node_too_big(mixed, DIRECT, svc)
    finally:
        svc.close()


def test_take_applies_a_beginning_of_the_choice_that_fits():
    limit = lim.SelectionLimit(max_items=3, max_pages=1000, pages_of=_pages, base_ids=lambda: {'b'})
    # b is held (taken at no cost); a fits (700); c would pass 1,000: it and everything after stay
    assert limit.take(['b', 'a', 'c', 'd']) == ['b', 'a']
    assert lim.SelectionLimit(max_items=2).take(['a', 'b', 'c']) == ['a', 'b']
    assert lim.SelectionLimit().take(['a', 'b']) == ['a', 'b']


def test_the_limits_of_each_purpose():
    assert lim.limit_for('search_exclude') is None and lim.limit_for('composition_exclude') is None
    desk = lim.limit_for('reading_desk', pages_of=_pages)
    assert (desk.max_items, desk.max_pages) == (30, 1000)
    assert lim.limit_for('reading_desk').max_pages is None            # no counter: manuscripts only
    assert lim.limit_for('puzzle').max_items == 30
    assert lim.limit_for('join').max_items == 30
    assert lim.limit_for('lists').max_items == 2000


def test_the_desk_counts_never_below_what_it_draws():
    pages_of = lim.desk_pages_function(lambda sid: {'t': 412, 'one': 1}.get(sid, 0))
    assert pages_of('t') == 412
    assert pages_of('untranscribed') == lim.UNTRANSCRIBED_DESK_PAGES == 2
    # Codex on #80: one indexed page whose text is empty is drawn as the two images of one leaf
    assert pages_of('one') == 2

    def broken(sid):
        raise RuntimeError
    assert lim.desk_pages_function(broken)('x') == 2
    # the desktop draws every image of a manuscript without pages: it gives that count
    images = {'big': 600}
    desktop = lim.desk_pages_function(lambda sid: 0, fallback=lambda sid: images.get(sid, 2))
    assert desktop('big') == 600
    limit = lim.SelectionLimit(max_pages=1000, pages_of=desktop)
    assert limit.refusal(set(), {'big', 'big2'}) is None             # 600 + 2
    images['big2'] = 600
    assert limit.refusal(set(), {'big', 'big2'}) == lim.Refusal('pages', 1000, 1200)
    assert lim.desk_pages_function(lambda sid: 0, fallback=broken)('x') == 2


def test_the_refusal_messages():
    tr = str
    assert lim.refusal_message(lim.Refusal('items', 30, 31), tr) == 'Up to 30 manuscripts here'
    assert lim.refusal_message(lim.Refusal('fragments', 30, 31), tr) == 'Up to 30 fragments here'
    assert lim.refusal_message(lim.Refusal('pages', 1000, 1100), tr) == (
        'The Reading Desk holds up to 1,000 pages; these would make 1,100')


def test_the_puzzle_and_a_join_count_fragments_and_say_so():
    # Codex on #80: the Puzzle counts its canvas's fragments, and a join joins fragments
    puzzle = lim.limit_for('puzzle', base_items=lambda: 30)
    assert puzzle.refusal(set(), {'a'}) == lim.Refusal('fragments', 30, 31)
    join = lim.limit_for('join')
    assert join.refusal(set(), {f's{i}' for i in range(31)}) == lim.Refusal('fragments', 30, 31)
    assert lim.refusal_message(join.refusal(set(), {f's{i}' for i in range(31)}), str) == 'Up to 30 fragments here'
    lists = lim.limit_for('lists')
    assert lists.refusal(set(), {f's{i}' for i in range(2001)}).unit == 'items'
    desk = lim.limit_for('reading_desk', pages_of=_pages)
    assert desk.refusal(set(), {f's{i}' for i in range(31)} | {'d'}).unit == 'items'


# -- the chips' names ------------------------------------------------------------------------------

class _Tree:
    """A stand-in for the service: one library whose short name is not one of its codes (St.
    Petersburg's "St. Peterburg", coded RNL) and one whose short name is (CUL)."""

    nodes = {
        'lib:229': SimpleNamespace(id='lib:229', short='St. Peterburg', name_en='St. Petersburg', name_he=''),
        'col:1': SimpleNamespace(id='col:1', short='', name_en='Yevr.-Arab. I', name_he=''),
        'lib:233': SimpleNamespace(id='lib:233', short='CUL', name_en='Cambridge', name_he=''),
        'col:2': SimpleNamespace(id='col:2', short='', name_en='CUL Add.', name_he='אוסף נוסף'),
    }
    parents = {'col:1': 'lib:229', 'col:2': 'lib:233'}
    codes = {'lib:229': ['RNL'], 'lib:233': ['CUL', 'Mosseri', 'Westminster']}
    leaves = {'inv:1:99': SimpleNamespace(library_id='lib:229')}

    def node(self, node_id):
        return self.nodes.get(node_id)

    def ancestors(self, node_id):
        out = [self.nodes[node_id]]
        while node_id in self.parents:
            node_id = self.parents[node_id]
            out.insert(0, self.nodes[node_id])
        return tuple(out)

    def codes_of_library(self, node_id):
        return self.codes.get(node_id, [])

    def leaf(self, placement_id):
        return self.leaves.get(placement_id)


def test_a_chip_carries_its_library_code():
    tree = _Tree()
    assert lim.library_code(tree, 'lib:229') == 'RNL'                 # the code, not "St. Peterburg"
    assert lim.library_code(tree, 'lib:233') == 'CUL'                 # the short name, one of its codes
    assert lim.library_code(tree, 'code:Benayahu') == 'Benayahu'      # a code-only library
    assert lim.library_code(tree, '') == ''
    assert lim.chip_name(tree, Chip(NODE, 'col:1', 'Yevr.-Arab. I')) == 'RNL Yevr.-Arab. I'
    assert lim.chip_name(tree, Chip(LEAF, 'inv:1:99', 'Yevr.-Arab. I 1700', '99', 'Yevr.-Arab. I 1700')) == (
        'RNL Yevr.-Arab. I 1700')
    # a name that already begins with the code keeps it once; the reader's language for a node
    assert lim.chip_name(tree, Chip(NODE, 'col:2', 'CUL Add.')) == 'CUL Add.'
    assert lim.chip_name(tree, Chip(NODE, 'col:2', 'CUL Add.'), 'he') == 'CUL אוסף נוסף'
    # a chip whose node or placement is gone keeps the name it was chosen with
    assert lim.chip_name(tree, Chip(NODE, 'col:404', 'Gone')) == 'Gone'
    assert lim.chip_name(tree, Chip(LEAF, 'inv:404', 'T-S 1.1', '1', 'T-S 1.1')) == 'T-S 1.1'


def test_the_manuscripts_own_code_comes_first():
    # Codex on #80: Penn's library node holds "Katz" and "UPenn"; the first A-Z named a UPenn
    # manuscript Katz. The host says the manuscript's own code.
    tree = _Tree()
    tree.nodes['lib:90'] = SimpleNamespace(id='lib:90', short='Penn U.', name_en='Philadelphia', name_he='')
    tree.nodes['col:9'] = SimpleNamespace(id='col:9', short='', name_en='E', name_he='')
    tree.parents['col:9'] = 'lib:90'
    tree.codes['lib:90'] = ['Katz', 'UPenn']
    tree.leaves['inv:9:990053956330205171'] = SimpleNamespace(library_id='lib:90')
    tree.members_under = lambda node_id, direct=False: frozenset({'990053956330205171'})
    codes = {'990053956330205171': 'UPenn'}
    leaf = Chip(LEAF, 'inv:9:990053956330205171', 'E 16275', '990053956330205171', 'E 16275')
    assert lim.chip_name(tree, leaf) == 'Katz E 16275'                       # the node's guess alone
    assert lim.chip_name(tree, leaf, code_of=codes.get) == 'UPenn E 16275'
    assert lim.chip_name(tree, Chip(NODE, 'col:9', 'E'), code_of=codes.get) == 'UPenn E'

    # a node chip takes its code from a manuscript it still holds, not one unticked inside it
    tree.members_under = lambda node_id, direct=False: frozenset({'990026445670205171', '990053956320205171'})
    codes.update({'990026445670205171': 'UPenn', '990053956320205171': 'Katz'})
    museum = Chip(NODE, 'col:9', 'Museum', removed={'990026445670205171'})
    assert lim.chip_name(tree, museum, code_of=codes.get) == 'Katz E'

    def broken(sid):
        raise RuntimeError
    assert lim.chip_name(tree, leaf, code_of=broken) == 'Katz E 16275'      # its node's code instead
    assert lim.chip_name(tree, leaf, code_of=lambda sid: '') == 'Katz E 16275'


def test_the_real_tree_names_its_libraries_by_their_codes(tmp_path):
    from shared.shelfmark_tree_service import ShelfmarkTreeService
    from tests.shelfmark_tree_fixture import SYS, build_fixture
    db, _libraries, _meta = build_fixture(tmp_path)
    svc = ShelfmarkTreeService(str(db))
    try:
        [place] = svc.path_of(SYS['as2'][0])
        volume = place.path[-1]
        library = place.path[0]
        assert 'CUL' in svc.codes_of_library(library.id)
        assert lim.chip_name(svc, Chip(NODE, volume.id, volume.name_en)).startswith('CUL ')
        leaf = place.leaf
        assert lim.chip_name(svc, Chip(LEAF, leaf.id, leaf.label, leaf.sys_id, leaf.shelfmark)) == f'CUL {leaf.label}'
        assert svc.codes_of_library('lib:does-not-exist') == []
    finally:
        svc.close()


def test_the_engine_counts_a_manuscripts_pages_from_its_browse_map():
    from shared.search_engine import SearchEngine
    engine = SimpleNamespace(_load_browse_map=lambda: {'t': [{'uid': 1}, {'uid': 2}, {'uid': 3}], 'empty': []})
    assert SearchEngine.page_count(engine, 't') == 3
    assert SearchEngine.page_count(engine, 'empty') == 0
    assert SearchEngine.page_count(engine, 'absent') == 0
    assert SearchEngine.page_count(engine, '') == 0
    assert SearchEngine.page_count(SimpleNamespace(_load_browse_map=lambda: None), 't') == 0
