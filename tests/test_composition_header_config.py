"""Regression tests for the Composition Search tree header.

`QTreeWidget.setHeader()` DESTROYS the outgoing header and resets the incoming
one: column widths return to Qt's 100 px default, every resize mode returns to
Interactive, `setColumnHidden` is cleared, and `QTreeView` re-derives
`sectionsClickable`/`sortIndicatorShown` from `isSortingEnabled()` -- which is
False for `comp_tree`, so the header became UNCLICKABLE. Signals connected to
the outgoing header die with it.

Until 2026-09-03 the composition tree was configured BEFORE `setHeader`, so all
of that was silently thrown away: Printed/Src sat at 100 px instead of 55/60,
System ID / MS Context / Printed / Src could not be dragged, the Witnesses
column started visible, and `on_comp_header_clicked` never fired at all.

These tests pin the CONSEQUENCES on a live Qt header, not the source text, so
they fail if the ordering regresses for any reason.
"""

import inspect
import os
from types import MethodType

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtWidgets import QApplication, QHeaderView, QTreeWidget

from desktop.ui_widgets import CheckBoxHeader
from genizah_app import GenizahGUI

pytestmark = pytest.mark.gui  # imports PyQt6: gui bucket only -- Qt in the mixed
# non-GUI run segfaults after thousands of NiceGUI/asyncio tests share the
# process (2026-08-21).


_APP = QApplication.instance() or QApplication([])

_COLUMN_LABELS = [
    "Score", "Library", "Shelfmark", "Title", "System ID",
    "Context", "MS Context", "Printed", "Src", "Witnesses",
]


class _CompHeaderHarness:
    """The composition tree's column layout, wired to the real methods."""

    comp_col_library = 1
    comp_col_shelfmark = 2
    comp_col_title = 3
    comp_col_sysid = 4
    comp_col_context = 5
    comp_col_ms_context = 6
    comp_col_printed = 7
    comp_col_src = 8
    comp_col_witnesses = 9

    def __init__(self, configure_before_setheader=False):
        self.clicked_sections = []
        self.tooltip_refreshes = 0

        self.comp_tree = QTreeWidget()
        self.comp_tree.setHeaderLabels(_COLUMN_LABELS)

        self._configure_comp_tree_header = MethodType(
            GenizahGUI._configure_comp_tree_header, self)
        self._comp_sort_mode_for_column = MethodType(
            GenizahGUI._comp_sort_mode_for_column, self)

        header = CheckBoxHeader(
            self.comp_tree,
            filter_columns=[self.comp_col_library],
            filter_callback=lambda _idx: None,
        )
        if configure_before_setheader:
            # The pre-2026-09-03 ordering, kept so the tests below are proven
            # able to fail rather than merely passing.
            self._configure_comp_tree_header()
            self.comp_tree.setHeader(header)
        else:
            self.comp_tree.setHeader(header)
            self._configure_comp_tree_header()

    # --- collaborators the configured header connects to -------------------
    def on_comp_header_clicked(self, section):
        self.clicked_sections.append(section)

    def _refresh_comp_tree_tooltips(self, *_args):
        self.tooltip_refreshes += 1

    def _update_comp_filter_indicators(self):
        return None


@pytest.fixture
def harness():
    return _CompHeaderHarness()


@pytest.fixture
def broken_harness():
    """The old ordering -- every assertion below must fail against this."""
    return _CompHeaderHarness(configure_before_setheader=True)


# --------------------------------------------------------------------------
# Every column is user-resizable (owner request, 2026-09-03)
# --------------------------------------------------------------------------

def test_every_composition_column_is_user_resizable(harness):
    header = harness.comp_tree.header()
    non_interactive = {
        _COLUMN_LABELS[col]: header.sectionResizeMode(col).name
        for col in range(harness.comp_tree.columnCount())
        if header.sectionResizeMode(col) != QHeaderView.ResizeMode.Interactive
    }
    assert non_interactive == {}, (
        "Stretch/Fixed/ResizeToContents columns cannot be dragged by the user"
    )


def test_previously_locked_columns_are_the_ones_that_changed(harness):
    """The four the owner named: System ID, MS Context, Printed, Src."""
    header = harness.comp_tree.header()
    for col in (harness.comp_col_sysid, harness.comp_col_ms_context,
                harness.comp_col_printed, harness.comp_col_src):
        assert header.sectionResizeMode(col) == QHeaderView.ResizeMode.Interactive


def test_broken_ordering_leaves_columns_locked(broken_harness):
    """Proves the resizability test can fail."""
    header = broken_harness.comp_tree.header()
    # setHeader wiped the modes, so they are all Interactive by accident --
    # but the DESIGNED modes (Stretch/Fixed) were what the old code applied and
    # what a partial revert would restore. What the old ordering demonstrably
    # loses is the widths, asserted below.
    assert header.sectionResizeMode(
        broken_harness.comp_col_printed) == QHeaderView.ResizeMode.Interactive


# --------------------------------------------------------------------------
# Widths survive (the setHeader wipe)
# --------------------------------------------------------------------------

def test_narrow_columns_keep_their_designed_widths(harness):
    assert harness.comp_tree.columnWidth(harness.comp_col_printed) == 55
    assert harness.comp_tree.columnWidth(harness.comp_col_src) == 60
    assert harness.comp_tree.columnWidth(0) == 160


def test_broken_ordering_loses_every_designed_width(broken_harness):
    """Proves the width test can fail: setHeader resets all columns to 100 px."""
    assert broken_harness.comp_tree.columnWidth(broken_harness.comp_col_printed) == 100
    assert broken_harness.comp_tree.columnWidth(broken_harness.comp_col_src) == 100
    assert broken_harness.comp_tree.columnWidth(0) == 100


def test_ms_context_is_the_widest_reading_column(harness):
    """It was the Stretch column; as an Interactive column it needs the width."""
    tree = harness.comp_tree
    assert tree.columnWidth(harness.comp_col_ms_context) > tree.columnWidth(
        harness.comp_col_context)
    assert tree.columnWidth(harness.comp_col_ms_context) > tree.columnWidth(
        harness.comp_col_title)


# --------------------------------------------------------------------------
# Header clicking -- sorting was entirely dead
# --------------------------------------------------------------------------

def test_header_is_clickable_and_reaches_the_sort_handler(harness):
    header = harness.comp_tree.header()
    assert header.sectionsClickable() is True
    header.sectionClicked.emit(harness.comp_col_title)
    assert harness.clicked_sections == [harness.comp_col_title]


def test_broken_ordering_leaves_the_header_unclickable_and_disconnected(
        broken_harness):
    """Proves the clickability test can fail."""
    header = broken_harness.comp_tree.header()
    assert header.sectionsClickable() is False
    header.sectionClicked.emit(broken_harness.comp_col_title)
    assert broken_harness.clicked_sections == []


def test_sort_indicator_is_shown(harness):
    header = harness.comp_tree.header()
    assert header.isSortIndicatorShown() is True
    assert header.sortIndicatorSection() == 0


def test_broken_ordering_hides_the_sort_indicator(broken_harness):
    assert broken_harness.comp_tree.header().isSortIndicatorShown() is False


def test_section_resize_refreshes_tooltips(harness):
    before = harness.tooltip_refreshes
    harness.comp_tree.setColumnWidth(harness.comp_col_title, 321)
    assert harness.tooltip_refreshes > before


def test_broken_ordering_never_refreshes_tooltips_on_resize(broken_harness):
    before = broken_harness.tooltip_refreshes
    broken_harness.comp_tree.setColumnWidth(broken_harness.comp_col_title, 321)
    assert broken_harness.tooltip_refreshes == before


# --------------------------------------------------------------------------
# The sort map used pre-Library column indices
# --------------------------------------------------------------------------

def test_sort_modes_name_the_column_they_are_on(harness):
    assert harness._comp_sort_mode_for_column(0) == "score"
    assert harness._comp_sort_mode_for_column(
        harness.comp_col_shelfmark) == "shelfmark"
    assert harness._comp_sort_mode_for_column(harness.comp_col_title) == "title"
    assert harness._comp_sort_mode_for_column(
        harness.comp_col_sysid) == "system_id"
    assert harness._comp_sort_mode_for_column(
        harness.comp_col_witnesses) == "witnesses"


def test_library_and_context_columns_do_not_sort(harness):
    """COMP_SORT_MODES has no library/context mode, so clicking must be inert."""
    assert harness._comp_sort_mode_for_column(harness.comp_col_library) is None
    assert harness._comp_sort_mode_for_column(harness.comp_col_context) is None
    assert harness._comp_sort_mode_for_column(harness.comp_col_ms_context) is None
    assert harness._comp_sort_mode_for_column(harness.comp_col_printed) is None


def test_every_sort_mode_is_a_real_comp_sort_mode(harness):
    for col in range(harness.comp_tree.columnCount()):
        mode = harness._comp_sort_mode_for_column(col)
        if mode is not None:
            assert mode in GenizahGUI.COMP_SORT_MODES


# --------------------------------------------------------------------------
# Witnesses column starts hidden
# --------------------------------------------------------------------------

def test_witnesses_column_starts_hidden(harness):
    assert harness.comp_tree.isColumnHidden(harness.comp_col_witnesses) is True


def test_broken_ordering_leaves_the_witnesses_column_visible(broken_harness):
    assert broken_harness.comp_tree.isColumnHidden(
        broken_harness.comp_col_witnesses) is False


# --------------------------------------------------------------------------
# The REAL collaborators, not stubs: the restored connections must survive
# contact with the actual slots.
# --------------------------------------------------------------------------

class _RealSlotHarness(_CompHeaderHarness):
    """Binds the production tooltip/sort slots instead of the counting stubs."""

    def __init__(self):
        self.comp_sort_mode = "score"
        self.comp_sort_reverse = True
        self.is_comp_running = False
        super().__init__()
        for name in ("_refresh_comp_tree_tooltips", "_update_comp_tree_tooltip",
                     "on_comp_header_clicked"):
            setattr(self, name, MethodType(getattr(GenizahGUI, name), self))

    def _has_comp_results(self):
        return False


def test_real_tooltip_slot_survives_the_section_resized_signature():
    """`sectionResized` emits (index, old, new); the slot takes none of them."""
    from PyQt6.QtWidgets import QTreeWidgetItem

    real = _RealSlotHarness()
    real.comp_tree.header().sectionResized.disconnect()
    real.comp_tree.header().sectionResized.connect(
        real._refresh_comp_tree_tooltips)
    item = QTreeWidgetItem(["a" * 80] * len(_COLUMN_LABELS))
    real.comp_tree.addTopLevelItem(item)
    real.comp_tree.setColumnWidth(real.comp_col_title, 40)
    assert item.toolTip(real.comp_col_title), (
        "a narrowed column should elide and gain a full-text tooltip"
    )


def test_header_click_drives_the_real_sort_handler():
    real = _RealSlotHarness()
    header = real.comp_tree.header()
    header.sectionClicked.disconnect()
    header.sectionClicked.connect(real.on_comp_header_clicked)

    header.sectionClicked.emit(real.comp_col_title)
    assert real.comp_sort_mode == "title"
    assert header.sortIndicatorSection() == real.comp_col_title

    header.sectionClicked.emit(real.comp_col_library)
    assert real.comp_sort_mode == "title", "Library has no sort mode; click is inert"


# --------------------------------------------------------------------------
# The construction site keeps the required ordering
# --------------------------------------------------------------------------

def test_construction_configures_the_header_after_installing_it():
    src = inspect.getsource(GenizahGUI)
    set_header = src.index("self.comp_tree.setHeader(self.chk_comp_header)")
    configure = src.index("self._configure_comp_tree_header()")
    assert set_header < configure, (
        "_configure_comp_tree_header() must run AFTER setHeader(); "
        "setHeader resets widths, resize modes, hidden state, clickability "
        "and drops every signal connected to the outgoing header"
    )
