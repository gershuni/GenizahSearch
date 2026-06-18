"""Phase 118 UAT follow-up: line-start ⊢ / line-end ⊣ anchors are mutually
exclusive toggles, and either can be cleared (both off allowed).

The UI shows them as selected (filled) buttons; this locks the underlying
toggle LOGIC (extracted to the pure `_toggle_line_anchor` helper) so a regression
in the exclusivity rule fails here rather than only in a browser.
"""

from __future__ import annotations

from web.components.joins_builder import _default_line, _toggle_line_anchor


def test_toggle_line_start_sets_it():
    line = _default_line()
    _toggle_line_anchor(line, 'line_start')
    assert line['line_start'] is True
    assert line['line_end'] is False


def test_toggle_active_clears_it_both_off():
    line = _default_line()
    _toggle_line_anchor(line, 'line_start')  # on
    _toggle_line_anchor(line, 'line_start')  # off again
    assert line['line_start'] is False
    assert line['line_end'] is False


def test_selecting_one_deselects_the_other():
    line = _default_line()
    _toggle_line_anchor(line, 'line_start')   # ⊢ on
    _toggle_line_anchor(line, 'line_end')     # ⊣ on -> ⊢ must turn off
    assert line['line_end'] is True
    assert line['line_start'] is False


def test_never_both_active_across_sequence():
    line = _default_line()
    for which in ['line_start', 'line_end', 'line_start', 'line_end', 'line_end']:
        _toggle_line_anchor(line, which)
        # invariant: at most one anchor active at any time
        assert not (line['line_start'] and line['line_end'])
