"""Letter-level composition ignores spelling variants (passage matching is edit
distance on a letter stream), yet the Variant intensity slider stayed on screen
when Variants had been chosen before the switch: `_apply_passage_mode_ui` forces
the mode combo to Exact with signals blocked, so the combo's own handler, the
only thing that hid the slider, never ran (owner, 2026-10-12). Qt-free: the
handler reads its widgets through getattr, so stand-ins suffice.
"""

from types import SimpleNamespace

from genizah_app import GenizahGUI


class _Widget:
    def __init__(self, index=0, value=0):
        self.index, self.value_, self.visible, self.enabled = index, value, True, True
        self.text = ''

    def currentIndex(self):
        return self.index

    def setCurrentIndex(self, i):
        self.index = i

    def value(self):
        return self.value_

    def setValue(self, v):
        self.value_ = v

    def blockSignals(self, _flag):
        pass

    def setEnabled(self, flag):
        self.enabled = flag

    def setVisible(self, flag):
        self.visible = flag

    def setText(self, text):
        self.text = text

    def setToolTip(self, _text):
        pass


def _window(mode_index):
    w = SimpleNamespace(
        _PASSAGE_FORCED_CONTROLS=GenizahGUI._PASSAGE_FORCED_CONTROLS,
        _PASSAGE_CONTROLS_LAB_ALSO_OWNS=GenizahGUI._PASSAGE_CONTROLS_LAB_ALSO_OWNS,
        comp_mode_combo=_Widget(index=mode_index),
        spin_chunk=_Widget(value=7),
        spin_freq=_Widget(value=80),
        spin_min_chunks=_Widget(value=2),
        boundary_mode_combo=_Widget(index=0),
        comp_variant_slider_container=_Widget(),
        btn_comp_recursive=_Widget(),
        _set_passage_options_visible=lambda _on: None,
        _set_boundary_row_visible=lambda _on: None,
        # Freezes the Composition conditions strip; no strip in this stub.
        _refresh_comp_conditions_available=lambda: None,
    )
    w.comp_variant_slider_container.visible = (mode_index == 1)
    return w


def _apply(w, on):
    GenizahGUI._apply_passage_mode_ui(w, on)


def test_switching_to_letter_level_hides_the_variant_slider():
    w = _window(mode_index=1)
    assert w.comp_variant_slider_container.visible
    _apply(w, True)
    assert w.comp_mode_combo.index == 0           # forced to Exact, as before
    assert not w.comp_variant_slider_container.visible


def test_switching_back_shows_the_slider_only_for_the_restored_variants_mode():
    w = _window(mode_index=1)
    _apply(w, True)
    _apply(w, False)
    assert w.comp_mode_combo.index == 1           # restored
    assert w.comp_variant_slider_container.visible

    w = _window(mode_index=0)
    _apply(w, True)
    _apply(w, False)
    assert w.comp_mode_combo.index == 0
    assert not w.comp_variant_slider_container.visible


def test_a_window_without_the_slider_is_left_alone():
    w = _window(mode_index=1)
    del w.comp_variant_slider_container
    _apply(w, True)
    _apply(w, False)
