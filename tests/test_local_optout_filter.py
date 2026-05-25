# -*- coding: utf-8 -*-
"""Phase 96 D-F1: opt-out filter composition with three-state LOCAL filter.

Implementation plan: 96-05-PLAN.md

REVISION 2026-05-24 -- W10: added three explicit cascade-interaction tests
covering Phase 95 `_local_filter_inactive_chip_visible` state transitions.
"""


class _Stub:
    """Mirrors tests/test_local_filter_cascade.py:_Stub pattern.
    Copies _apply_local_filter + _apply_local_optout_filter for isolated
    unit testing without the full genizah_app.py import."""

    _local_filter_inactive_chip_visible = False

    def __init__(self):
        self._local_file_optouts = set()
        self._filepath_by_sys_id = {}

    def _lookup_local_filepath(self, sys_id):
        return self._filepath_by_sys_id.get(sys_id)

    def _apply_local_filter(self, results, state):
        if state == 'all':
            self._local_filter_inactive_chip_visible = False
            return results
        has_local = any((r.get('display', {}) or {}).get('source') == 'LOCAL' for r in results)
        if not has_local:
            self._local_filter_inactive_chip_visible = True
            return results
        self._local_filter_inactive_chip_visible = False
        if state == 'only_local':
            return [r for r in results if (r.get('display', {}) or {}).get('source') == 'LOCAL']
        if state == 'no_local':
            return [r for r in results if (r.get('display', {}) or {}).get('source') != 'LOCAL']
        return results

    def _apply_local_optout_filter(self, results):
        """Phase 96 D-F1: drop LOCAL hits whose canonical filepath is in
        _local_file_optouts. Non-LOCAL hits passthrough unchanged.

        NOTE: when plan 96-05 introduces the real method, REMOVE this stub
        and import from genizah_app.py.
        """
        if not self._local_file_optouts:
            return results
        out = []
        for r in results:
            src = (r.get('display', {}) or {}).get('source')
            if src != 'LOCAL':
                out.append(r); continue
            sid = (r.get('display', {}) or {}).get('id', '')
            fp = self._lookup_local_filepath(sid)
            if fp and fp in self._local_file_optouts:
                continue
            out.append(r)
        return out


def _hits():
    return [
        {'display': {'source': 'V0.8', 'id': 'g1'}},
        {'display': {'source': 'LOCAL', 'id': 'L1'}},
        {'display': {'source': 'LOCAL', 'id': 'L2'}},
    ]


def test_optout_filter_alone():
    """D-F1: opt-out drops the named LOCAL hit only."""
    stub = _Stub()
    stub._filepath_by_sys_id = {'L1': '/p/a.pdf', 'L2': '/p/b.pdf'}
    stub._local_file_optouts = {'/p/a.pdf'}
    out = stub._apply_local_optout_filter(_hits())
    ids = [r['display']['id'] for r in out]
    assert 'L1' not in ids
    assert 'L2' in ids
    assert 'g1' in ids


def test_optout_composes_with_only_local():
    """D-F1: opt-out + three-state 'only_local' composes — LOCAL set minus opt-outs."""
    stub = _Stub()
    stub._filepath_by_sys_id = {'L1': '/p/a.pdf', 'L2': '/p/b.pdf'}
    stub._local_file_optouts = {'/p/a.pdf'}
    step1 = stub._apply_local_filter(_hits(), 'only_local')
    step2 = stub._apply_local_optout_filter(step1)
    ids = [r['display']['id'] for r in step2]
    assert ids == ['L2']


def test_optout_composes_with_no_local():
    """D-F1: opt-out + three-state 'no_local' composes — opt-outs are
    redundant (already excluded by no_local) but must not error."""
    stub = _Stub()
    stub._filepath_by_sys_id = {'L1': '/p/a.pdf', 'L2': '/p/b.pdf'}
    stub._local_file_optouts = {'/p/a.pdf'}
    step1 = stub._apply_local_filter(_hits(), 'no_local')
    step2 = stub._apply_local_optout_filter(step1)
    ids = [r['display']['id'] for r in step2]
    assert ids == ['g1']


def test_optout_with_zero_local_hits_preserves_chip_visible_state():
    """W10 cascade interaction (REVISION 2026-05-24): when results contain
    NO LOCAL hits AND three-state is 'only_local' or 'no_local', the
    Phase 95 chip-visible flag must be True (chip shown 'LOCAL filter
    inactive -- no LOCAL hits in this query'). Opt-out passes through
    without resetting the chip state.
    """
    stub = _Stub()
    genizah_only = [{'display': {'source': 'V0.8', 'id': 'g1'}}]
    stub._local_file_optouts = {'/p/a.pdf'}  # opt-out set non-empty
    step1 = stub._apply_local_filter(genizah_only, 'only_local')
    # Phase 95 invariant: chip is visible because there are no LOCAL hits.
    assert stub._local_filter_inactive_chip_visible is True
    step2 = stub._apply_local_optout_filter(step1)
    # Opt-out filter does NOT reset chip-visible flag.
    assert stub._local_filter_inactive_chip_visible is True
    # No surprises in the result set itself.
    assert [r['display']['id'] for r in step2] == ['g1']


def test_optout_with_only_local_hits_chip_invisible():
    """W10 cascade interaction: when results contain ONLY LOCAL hits and
    three-state is engaged, chip is invisible. Opt-out filter does not
    flip the chip flag."""
    stub = _Stub()
    local_only = [
        {'display': {'source': 'LOCAL', 'id': 'L1'}},
        {'display': {'source': 'LOCAL', 'id': 'L2'}},
    ]
    stub._filepath_by_sys_id = {'L1': '/p/a.pdf', 'L2': '/p/b.pdf'}
    stub._local_file_optouts = {'/p/a.pdf'}
    step1 = stub._apply_local_filter(local_only, 'only_local')
    assert stub._local_filter_inactive_chip_visible is False
    step2 = stub._apply_local_optout_filter(step1)
    assert stub._local_filter_inactive_chip_visible is False
    assert [r['display']['id'] for r in step2] == ['L2']


def test_optout_with_mixed_hits_chip_invisible():
    """W10 cascade interaction: mixed Genizah + LOCAL hits, three-state
    engaged -> chip invisible; opt-out applies on top without flipping chip."""
    stub = _Stub()
    stub._filepath_by_sys_id = {'L1': '/p/a.pdf', 'L2': '/p/b.pdf'}
    stub._local_file_optouts = {'/p/a.pdf'}
    step1 = stub._apply_local_filter(_hits(), 'only_local')
    assert stub._local_filter_inactive_chip_visible is False
    step2 = stub._apply_local_optout_filter(step1)
    assert stub._local_filter_inactive_chip_visible is False
    assert [r['display']['id'] for r in step2] == ['L2']
