# -*- coding: utf-8 -*-
"""`ElidingLabel` must truncate VISIBLY, and never hand out the truncated text.

WHY THIS WIDGET EXISTS. The desktop citation strip is one fixed 30px line, and
a plain QLabel with word-wrap off does not elide -- it paints what fits and
stops, with no "..." to say anything is missing. At the old ~197-character
citation the strip was already at that edge; the owner's full 17-author list
takes it to ~428, so roughly half is lost at an ordinary window width including
the DOI, which is the part of a citation a reader most needs.

A citation cut in the middle that LOOKS complete is worse than one obviously
cut, because it gets pasted. Hence: elide with an ellipsis, keep the whole
string in `full_text`, and let the tooltip and the copy button read that.

`gui`-marked because it constructs real widgets; CI runs
`-m "not gui and not render_smoke and not atlas_bake"`. The properties that can
be checked WITHOUT Qt -- that the strip uses this class, and that selection is
off -- are asserted in `tests/test_desktop_citation_bar.py`, which does run in
CI.
"""

from __future__ import annotations

import pytest
from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QLabel

from desktop.ui_widgets import ElidingLabel

pytestmark = pytest.mark.gui  # imports PyQt6: gui bucket only.

LONG = (
    "Stoekl Ben Ezra, D., Bambaci, L., Kiessling, B., Lapin, H., Ezer, N., "
    "Lolli, E., Rustow, M., Dershowitz, N., Kurar Barakat, B., Gogawale, S., "
    "Shmidman, A., Lavee, M., Siew, T., Raziel Kretzmer, V., "
    "Vasyutinsky Shapira, D., Olszowy-Schlanger, J., & Gila, Y. (2025). "
    "MiDRASH Automatic Transcriptions. Zenodo. "
    "https://doi.org/10.5281/zenodo.17734473"
)


def _narrow(text=LONG, width=320):
    """A SHOWN label at a known width.

    `show()` is not decoration. Qt does not deliver resize events to a hidden
    widget -- it coalesces them until the widget is shown -- so a parentless
    label that is only `resize()`d never runs `resizeEvent` and keeps whatever
    elision it computed at its default width. A test that skipped this would
    measure the constructor and call it a resize.
    """
    label = ElidingLabel(text)
    label.resize(width, 20)
    label.show()
    return label


def test_it_is_a_qlabel():
    """Callers style it, add it to layouts and set tooltips on it as a QLabel."""
    assert isinstance(_narrow(), QLabel)


def test_a_long_string_is_cut_and_says_so():
    label = _narrow()
    shown = label.text()
    assert len(shown) < len(LONG), 'nothing was elided'
    assert '…' in shown, (
        'the text was cut with no ellipsis, which is the silent truncation this '
        'widget exists to replace: %r' % shown)


def test_the_whole_string_survives_on_the_widget():
    """THE PROPERTY THAT MATTERS. `text()` is lossy by design, so anything that
    copies, exports or tooltips the value must read `full_text`."""
    label = _narrow()
    assert label.full_text == LONG
    assert label.text() != LONG


def test_a_short_string_is_left_alone():
    """The CONTROL. Without it, a widget that elided everything to "..." would
    satisfy the two tests above."""
    label = ElidingLabel('T-S 12.123')
    label.resize(400, 20)
    label.show()
    assert label.text() == 'T-S 12.123'
    assert '…' not in label.text()


def test_a_wider_widget_shows_more():
    """Re-elides on resize rather than measuring once at construction.

    A label that elided only in `__init__` would keep a 320px-worth of text
    forever after the window was maximised -- and would be indistinguishable
    from a correct one in a test that never resized.
    """
    label = _narrow(width=200)
    short = label.text()
    # No second show(): the label is already visible, and a VISIBLE widget does
    # get its resize event -- which is the behaviour under test.
    label.resize(900, 20)
    longer = label.text()
    assert len(longer) > len(short), (
        'the label did not re-elide when it grew: %r -> %r' % (short, longer))
    assert label.full_text == LONG


def test_setText_replaces_the_full_string_not_the_elided_one():
    """A second `setText` must not elide an already-elided value.

    That is the compounding bug this shape invites: `setText(self.text())`
    anywhere in a caller, or simply two updates in a row, would otherwise chew
    the string down a little more each time.
    """
    label = _narrow()
    label.setText(LONG)
    assert label.full_text == LONG
    label.setText(LONG)
    assert label.full_text == LONG


def test_a_zero_width_label_shows_the_WHOLE_string_not_an_ellipsis():
    """Before the first layout pass there is nothing to elide against.

    The branch must show the full string, not a placeholder: the label is
    painted at least once at that width in a real window, and a bar that
    flashed "..." on start-up would look broken. The first version of this test
    checked only `full_text` and never read `text()`, so a branch returning a
    bare ellipsis left it green -- Codex review, confirmed by mutation.
    """
    # Resized to zero EXPLICITLY. A freshly-constructed unparented QLabel is
    # 640px wide, not 0 -- Qt's default -- so "just constructed" does not reach
    # this branch, and the first version of this test asserted it did. The
    # branch is still real: a layout can assign a child no width at all.
    label = ElidingLabel(LONG)
    label.resize(0, 20)
    label.show()
    assert label.width() == 0, 'the fixture is no longer zero-width'
    assert label.full_text == LONG
    assert label.text() == LONG, (
        'the zero-width branch displays %r rather than the whole string'
        % label.text())


def test_clearing_it_clears_both_the_display_and_the_stored_string():
    label = _narrow()
    label.setText('')
    assert label.full_text == ''
    assert label.text() == ''


def test_the_elide_mode_is_configurable_and_defaults_to_the_right_one():
    """ElideRight keeps the START of the citation, which is the author names.

    ElideMiddle would keep the DOI at the cost of the names; neither shows
    everything, and the button is what gives the whole string either way. The
    default is stated here so a change to it is a decision, not a drift.
    """
    label = _narrow()
    assert label.text().endswith('…')
    middle = ElidingLabel(LONG, mode=Qt.TextElideMode.ElideMiddle)
    middle.resize(320, 20)
    middle.show()
    assert '…' in middle.text()
    assert middle.text().endswith('17734473')
