# -*- coding: utf-8 -*-
"""Top-level modal dialogs (extracted from genizah_app.py, Phase 126 D1).

Provides five modal QDialog subclasses moved verbatim out of the
28K-line ``genizah_app.py`` god file:

  - LabScoringDialog(QDialog)         — advanced Lab-mode scoring weights
  - SearchSettingsDialog(QDialog)     — variant search configuration
  - HelpDialog(QDialog)               — bundled Help.html viewer
  - TabularQueryBuilderDialog(QDialog)— visual Responsa-syntax composer
  - SettingsDialog(QDialog)           — General + About tabs (D-07b telemetry strip)

ZERO behavior change vs. the originals. ``genizah_app.py`` re-exports these
via a ``# noqa: F401`` shim (MOVE-and-shim, mirroring genizah_core 122-125).

GUARD-01: NO module-level ``import genizah_app`` — shared symbols come from the
``genizah_core`` facade only.

D-07b (T-126D1-01): the SettingsDialog telemetry-consent snapshot strip
(``_TELEMETRY_SNAPSHOT_EXCLUDE`` / ``self._config_snapshot`` / ``_on_cancel``)
is moved VERBATIM. It is load-bearing: ``save_app_config`` is additive-merge,
so Cancel must NOT overwrite freshly-set consent keys.
"""
from __future__ import annotations

import os

from PyQt6.QtWidgets import (
    QDialog,
    QVBoxLayout,
    QHBoxLayout,
    QGridLayout,
    QLabel,
    QPushButton,
    QDoubleSpinBox,
    QSpinBox,
    QCheckBox,
    QSlider,
    QWidget,
    QTextEdit,
    QScrollArea,
    QFrame,
    QLineEdit,
    QTabWidget,
    QComboBox,
    QProgressBar,
    QTextBrowser,
    QMessageBox,
    QApplication,
)
from PyQt6.QtCore import Qt, QTimer, QUrl, QEvent
from PyQt6.QtGui import QIcon, QCursor, QPalette, QDesktopServices

from version import APP_VERSION

from genizah_core import (
    Config, tr, CURRENT_LANG,
    load_app_config, save_app_config, generate_tabular_syntax, get_logger,
)
from desktop.vs_cache import VSDownloadThread

logger = get_logger(__name__)


class LabScoringDialog(QDialog):
    """Configuration for Lab Mode Scoring (Advanced)."""
    def __init__(self, parent, lab_engine):
        super().__init__(parent)
        self.setWindowTitle(tr("Advanced Scoring"))
        self.resize(500, 500)
        self.lab_engine = lab_engine
        self.settings = lab_engine.settings
        if CURRENT_LANG == 'he':
            self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)

        layout = QVBoxLayout()
        layout.addWidget(QLabel(tr("Adjust how the algorithm prioritizes results.")))

        grid = QGridLayout()

        # Order Bonus
        self.spin_order_bonus = QDoubleSpinBox(); self.spin_order_bonus.setRange(0.0, 100.0); self.spin_order_bonus.setSingleStep(1.0); self.spin_order_bonus.setValue(getattr(self.settings, 'order_bonus', 10.0))
        lbl_order = QLabel(tr("Sequential Order Bonus:")); lbl_order.setStyleSheet("color: #2980b9; font-weight: bold;")
        grid.addWidget(lbl_order, 0, 0); grid.addWidget(self.spin_order_bonus, 0, 1)

        # Coverage
        self.spin_coverage_power = QDoubleSpinBox(); self.spin_coverage_power.setRange(1.0, 10.0); self.spin_coverage_power.setValue(self.settings.coverage_power)
        grid.addWidget(QLabel(tr("Coverage Penalty Power:")), 1, 0); grid.addWidget(self.spin_coverage_power, 1, 1)

        # Noise Suppression
        lbl_noise = QLabel(tr("Stop-Word Suppression:")); lbl_noise.setStyleSheet("font-weight: bold; margin-top: 10px;")
        grid.addWidget(lbl_noise, 2, 0, 1, 2)

        # Short Word Score
        self.spin_stop_score = QDoubleSpinBox(); self.spin_stop_score.setRange(0.0, 50.0); self.spin_stop_score.setSingleStep(0.5); self.spin_stop_score.setValue(getattr(self.settings, 'stop_word_score', 1.0))
        self.spin_stop_score.setToolTip(tr("Points given for very short words (<3 letters). Keep low to reduce noise."))
        grid.addWidget(QLabel(tr("Score for Short Words (<3):")), 3, 0); grid.addWidget(self.spin_stop_score, 3, 1)

        # Common 3-Char Score
        self.spin_common3_score = QDoubleSpinBox(); self.spin_common3_score.setRange(0.0, 50.0); self.spin_common3_score.setSingleStep(0.5); self.spin_common3_score.setValue(getattr(self.settings, 'common_3char_score', 2.0))
        self.spin_common3_score.setToolTip(tr("Points for common 3-letter words (e.g. 'ליה', 'הכי')."))
        grid.addWidget(QLabel(tr("Score for Common 3-Letter:")), 4, 0); grid.addWidget(self.spin_common3_score, 4, 1)

        # Other Weights
        lbl_other = QLabel(tr("Standard Weights:")); lbl_other.setStyleSheet("font-weight: bold; margin-top: 10px;")
        grid.addWidget(lbl_other, 5, 0, 1, 2)

        self.spin_len_bonus = QDoubleSpinBox(); self.spin_len_bonus.setRange(1.0, 10.0); self.spin_len_bonus.setValue(self.settings.length_bonus_factor)
        grid.addWidget(QLabel(tr("Long Word Bonus:")), 6, 0); grid.addWidget(self.spin_len_bonus, 6, 1)

        self.spin_unique_base = QSpinBox(); self.spin_unique_base.setRange(10, 1000); self.spin_unique_base.setValue(self.settings.unique_bonus_base)
        grid.addWidget(QLabel(tr("Unique Match Base Score:")), 7, 0); grid.addWidget(self.spin_unique_base, 7, 1)

        self.spin_density = QDoubleSpinBox(); self.spin_density.setRange(0.0, 5.0); self.spin_density.setValue(self.settings.density_penalty)
        grid.addWidget(QLabel(tr("Distance Penalty:")), 8, 0); grid.addWidget(self.spin_density, 8, 1)

        self.spin_common_factor = QDoubleSpinBox(); self.spin_common_factor.setRange(0.0, 1.0); self.spin_common_factor.setValue(self.settings.common_penalty_factor)
        grid.addWidget(QLabel(tr("Repeated Word Factor:")), 9, 0); grid.addWidget(self.spin_common_factor, 9, 1)

        # Display Limit
        self.spin_display_limit = QSpinBox(); self.spin_display_limit.setRange(50, 1000); self.spin_display_limit.setValue(getattr(self.settings, 'lab_display_limit', 500))
        self.spin_display_limit.setToolTip(tr("Lower values prevent the app from freezing. All results are still exported."))
        grid.addWidget(QLabel(tr("Max Results to Display:")), 10, 0); grid.addWidget(self.spin_display_limit, 10, 1)

        layout.addLayout(grid)
        layout.addStretch()

        btn_box = QHBoxLayout()
        # Help Button
        btn_help = QPushButton("?")
        btn_help.setFixedWidth(30)
        btn_help.setStyleSheet("background-color: #f39c12; color: white; font-weight: bold; border-radius: 15px;")
        # Find main window to call open_help_center
        def open_help():
            main = parent
            while main and not hasattr(main, 'open_help_center'):
                main = main.parent()
            if main: main.open_help_center(anchor="lab")

        btn_help.clicked.connect(open_help)
        btn_box.addWidget(btn_help)

        btn_box.addStretch()
        self.btn_save = QPushButton(tr("Save & Close")); self.btn_save.clicked.connect(self.save_and_close)
        self.btn_cancel = QPushButton(tr("Cancel")); self.btn_cancel.clicked.connect(self.reject)
        btn_box.addStretch(); btn_box.addWidget(self.btn_cancel); btn_box.addWidget(self.btn_save)
        layout.addLayout(btn_box)
        self.setLayout(layout)

    def save_and_close(self):
        self.settings.coverage_power = self.spin_coverage_power.value()
        self.settings.length_bonus_factor = self.spin_len_bonus.value()
        self.settings.common_penalty_factor = self.spin_common_factor.value()
        self.settings.density_penalty = self.spin_density.value()
        self.settings.unique_bonus_base = self.spin_unique_base.value()
        if hasattr(self.settings, 'order_bonus'): self.settings.order_bonus = self.spin_order_bonus.value()
        if hasattr(self.settings, 'stop_word_score'):
            self.settings.stop_word_score = self.spin_stop_score.value()
            self.settings.common_3char_score = self.spin_common3_score.value()

        self.settings.lab_display_limit = self.spin_display_limit.value()
        self.settings.save()
        self.accept()


class SearchSettingsDialog(QDialog):
    """Settings for Standard Search - Variant configuration and custom pairs."""
    def __init__(self, parent, settings):
        super().__init__(parent)
        self.setWindowTitle(tr("Search Settings"))
        self.resize(450, 400)
        self.settings = settings
        if CURRENT_LANG == 'he':
            self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)

        layout = QVBoxLayout()
        layout.addWidget(QLabel(tr("Configure variant search behavior for Standard Search modes.")))

        grid = QGridLayout()

        # --- Variant Limits Section ---
        lbl_variant = QLabel(tr("Variant Search Limits:"))
        lbl_variant.setStyleSheet("font-weight: bold; margin-top: 10px; color: #8e44ad;")
        grid.addWidget(lbl_variant, 0, 0, 1, 2)

        # Min Word Length for limiting changes
        self.spin_variant_min_len = QSpinBox()
        self.spin_variant_min_len.setRange(1, 5)
        self.spin_variant_min_len.setValue(getattr(self.settings, 'variant_min_word_len', 2))
        self.spin_variant_min_len.setToolTip(tr("Words with this length or less get only 1 character change. Increase to be more conservative."))
        grid.addWidget(QLabel(tr("Limit Short Words (≤N chars):")), 1, 0)
        grid.addWidget(self.spin_variant_min_len, 1, 1)

        # Max Changes
        self.spin_variant_max_changes = QSpinBox()
        self.spin_variant_max_changes.setRange(1, 3)
        self.spin_variant_max_changes.setValue(getattr(self.settings, 'variant_max_changes', 2))
        self.spin_variant_max_changes.setToolTip(tr("Maximum character substitutions per word. Higher = more results but slower."))
        grid.addWidget(QLabel(tr("Max Changes per Word:")), 2, 0)
        grid.addWidget(self.spin_variant_max_changes, 2, 1)

        # Aggressive Mode
        self.chk_variant_aggressive = QCheckBox(tr("Aggressive Mode (ignore word length limits)"))
        self.chk_variant_aggressive.setChecked(getattr(self.settings, 'variant_aggressive', False))
        self.chk_variant_aggressive.setToolTip(tr("Like old behavior: apply max changes to all words regardless of length. More results, more noise."))
        grid.addWidget(self.chk_variant_aggressive, 3, 0, 1, 2)

        # Use slider instead of presets
        self.chk_use_slider = QCheckBox(tr("Use slider instead of preset buttons (Basic, Extended, Maximum)"))
        self.chk_use_slider.setChecked(getattr(self.settings, 'variant_use_slider', False))
        self.chk_use_slider.setToolTip(tr("When enabled, shows a slider in the search bar instead of preset buttons"))
        grid.addWidget(self.chk_use_slider, 4, 0, 1, 2)

        # --- Variant Pairs Slider (shown only when slider mode is enabled) ---
        self.slider_container = QWidget()
        slider_container_layout = QVBoxLayout(self.slider_container)
        slider_container_layout.setContentsMargins(0, 0, 0, 0)

        lbl_pairs = QLabel(tr("Variant Pairs Level:"))
        lbl_pairs.setStyleSheet("font-weight: bold; margin-top: 10px; color: #2980b9;")
        slider_container_layout.addWidget(lbl_pairs)

        # Slider for number of variant pairs to use
        slider_layout = QHBoxLayout()
        self.slider_variant_pairs = QSlider(Qt.Orientation.Horizontal)
        self.slider_variant_pairs.setRange(10, 300)
        self.slider_variant_pairs.setValue(getattr(self.settings, 'variant_pairs_count', 70))
        self.slider_variant_pairs.setToolTip(tr("Number of variant pairs to use. Higher = more substitutions but slower search.\nBased on frequency: top pairs are most common HTR confusions."))

        self.lbl_pairs_value = QLabel(str(self.slider_variant_pairs.value()))
        self.lbl_pairs_value.setMinimumWidth(40)
        self.slider_variant_pairs.valueChanged.connect(
            lambda v: self.lbl_pairs_value.setText(str(v))
        )

        slider_layout.addWidget(QLabel(tr("10")))
        slider_layout.addWidget(self.slider_variant_pairs)
        slider_layout.addWidget(QLabel(tr("300")))
        slider_layout.addWidget(self.lbl_pairs_value)
        slider_container_layout.addLayout(slider_layout)

        lbl_pairs_help = QLabel(tr("Controls how many character substitution pairs to use. Higher values find more variants but are slower."))
        lbl_pairs_help.setStyleSheet("font-size: 10px; color: gray; font-style: italic;")
        lbl_pairs_help.setWordWrap(True)
        slider_container_layout.addWidget(lbl_pairs_help)

        grid.addWidget(self.slider_container, 5, 0, 1, 2)

        # Show/hide slider container based on checkbox
        self.slider_container.setVisible(self.chk_use_slider.isChecked())
        self.chk_use_slider.toggled.connect(self.slider_container.setVisible)

        layout.addLayout(grid)

        # --- Custom Variants Section ---
        lbl_custom = QLabel(tr("Custom Variant Pairs:"))
        lbl_custom.setStyleSheet("font-weight: bold; margin-top: 15px; color: #27ae60;")
        layout.addWidget(lbl_custom)

        lbl_custom_help = QLabel(tr("Add character pairs that should be treated as interchangeable (e.g. ק=א means ק↔א)."))
        lbl_custom_help.setStyleSheet("font-size: 10px; color: gray; font-style: italic;")
        lbl_custom_help.setWordWrap(True)
        layout.addWidget(lbl_custom_help)

        # Custom variants text edit
        self.txt_custom_variants = QTextEdit()
        self.txt_custom_variants.setPlaceholderText(tr("Enter one pair per line:\nק=א\nכו=מ\nב=פ"))
        self.txt_custom_variants.setMaximumHeight(120)

        # Load existing custom variants
        custom = getattr(self.settings, 'custom_variants', {})
        if custom:
            lines = [k for k in custom.keys()]
            self.txt_custom_variants.setPlainText('\n'.join(lines))

        layout.addWidget(self.txt_custom_variants)

        layout.addStretch()

        # Buttons
        btn_box = QHBoxLayout()
        btn_box.addStretch()
        self.btn_cancel = QPushButton(tr("Cancel"))
        self.btn_cancel.clicked.connect(self.reject)
        self.btn_save = QPushButton(tr("Save & Close"))
        self.btn_save.clicked.connect(self.save_and_close)
        btn_box.addWidget(self.btn_cancel)
        btn_box.addWidget(self.btn_save)
        layout.addLayout(btn_box)
        self.setLayout(layout)

    def save_and_close(self):
        # Save variant limits
        self.settings.variant_min_word_len = self.spin_variant_min_len.value()
        self.settings.variant_max_changes = self.spin_variant_max_changes.value()
        self.settings.variant_aggressive = self.chk_variant_aggressive.isChecked()
        self.settings.variant_pairs_count = self.slider_variant_pairs.value()
        self.settings.variant_use_slider = self.chk_use_slider.isChecked()

        # Parse custom variants
        text = self.txt_custom_variants.toPlainText().strip()
        custom = {}
        if text:
            for line in text.split('\n'):
                line = line.strip()
                if '=' in line:
                    custom[line] = True
        self.settings.custom_variants = custom

        self.settings.save()

        # Update VariantManager if available
        main = self.parent()
        while main and not hasattr(main, 'var_mgr'):
            main = main.parent()
        if main and main.var_mgr:
            main.var_mgr.set_settings(self.settings)

        self.accept()


class HelpDialog(QDialog):
    """Display HTML help content from the bundled Help.html file with graceful fallback."""
    def __init__(self, parent, title, source_path=None, anchor=None, fallback_html="", lang="en"):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setWindowIcon(QIcon(os.path.join(Config.BASE_DIR, "icon.ico")))
        self.resize(900, 700)
        layout = QVBoxLayout()
        self.text = QTextBrowser()
        self.text.setOpenExternalLinks(True)
        layout.addWidget(self.text)

        self._load_content(source_path, anchor, fallback_html, lang)

        btn = QPushButton(tr("Close"))
        btn.clicked.connect(self.close)
        layout.addWidget(btn)
        self.setLayout(layout)

    def _load_content(self, source_path, anchor, fallback_html, lang):
        if source_path and os.path.exists(source_path):
            try:
                with open(source_path, "r", encoding="utf-8") as f:
                    content = f.read()

                # --- 1. LANGUAGE FILTERING (Content Stripping) ---
                # Since QTextBrowser ignores "display: none", we must remove the unused language block manually.
                # We rely on markers added to Help.html.
                if lang == 'he':
                    # Keep Hebrew -> Remove English
                    start_marker = "<!-- START_LANG_EN -->"
                    end_marker = "<!-- END_LANG_EN -->"
                else:
                    # Keep English -> Remove Hebrew
                    start_marker = "<!-- START_LANG_HE -->"
                    end_marker = "<!-- END_LANG_HE -->"

                s_idx = content.find(start_marker)
                e_idx = content.find(end_marker)

                if s_idx != -1 and e_idx != -1:
                    # Remove the block including markers
                    content = content[:s_idx] + content[e_idx + len(end_marker):]

                # Removed explicit Dark Mode CSS injection to allow "native" palette behavior
                # as requested by the user ("do what the previous version did").
                # By removing explicit background colors in HTML, QTextBrowser uses QPalette.

                self.text.setHtml(content)
                if anchor:
                    QTimer.singleShot(0, lambda: self.text.scrollToAnchor(anchor))
                return
            except Exception as e:
                logger.warning("Failed to load help file %s: %s", source_path, e)
        # Fallback: prefer clean content without warning if we have a fallback HTML snippet
        if fallback_html:
            self.text.setHtml(fallback_html)
        else:
            notice = "<p style='color:#c0392b;'><b>Help file is missing or could not be loaded.</b></p>"
            self.text.setHtml(notice)
        if anchor:
            QTimer.singleShot(0, lambda: self.text.scrollToAnchor(anchor))


class TabularQueryBuilderDialog(QDialog):
    """Tabular Query Builder for Responsa syntax composition.

    Provides a visual interface for composing Responsa queries using
    2-4 component columns with per-word modifiers and distance controls.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle(tr("Tabular Search"))
        self.setMinimumSize(750, 500)
        self.resize(800, 550)

        self._syntax = ''
        self._negated_words = []
        self._active_word = None  # (comp_idx, word_idx)
        self._updating_modifiers = False
        self._max_components = 4
        self._max_words_per_component = 4
        self._initial_words_visible = 2
        self._initial_components = 2

        # Internal state
        self._component_data = []  # List of component state dicts
        self._distance_spinners = []  # QSpinBox list
        self._component_widgets = []  # List of component UI widget groups
        self._distance_containers = []  # Container widgets for distance spinners

        # Dark mode detection
        palette = self.palette()
        self._is_dark = palette.color(palette.ColorRole.Window).lightness() < 128

        self._setup_ui()
        self._initialize_components()

    def _setup_ui(self):
        """Build the complete dialog UI."""
        main_layout = QVBoxLayout(self)
        main_layout.setSpacing(8)

        # --- Scope Row ---
        scope_row = QHBoxLayout()
        scope_row.addWidget(QLabel(tr("Scope") + ":"))
        from PyQt6.QtWidgets import QRadioButton, QButtonGroup
        self._scope_group = QButtonGroup(self)
        self._rb_word_range = QRadioButton(tr("Word Range"))
        self._rb_word_range.setChecked(True)
        self._rb_within_doc = QRadioButton(tr("Within Document"))
        self._rb_lines = QRadioButton(tr("Lines"))
        self._scope_group.addButton(self._rb_word_range, 0)
        self._scope_group.addButton(self._rb_within_doc, 1)
        self._scope_group.addButton(self._rb_lines, 2)
        scope_row.addWidget(self._rb_word_range)
        scope_row.addWidget(self._rb_within_doc)
        scope_row.addWidget(self._rb_lines)
        scope_row.addStretch()
        self._scope_group.idToggled.connect(self._on_scope_changed)
        main_layout.addLayout(scope_row)

        # --- Components Area (scrollable) ---
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        scroll.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        scroll.setMinimumHeight(250)

        self._components_container = QWidget()
        self._components_layout = QHBoxLayout(self._components_container)
        self._components_layout.setSpacing(6)
        self._components_layout.setContentsMargins(4, 4, 4, 4)
        scroll.setWidget(self._components_container)
        main_layout.addWidget(scroll)

        # --- Add Component Button ---
        self._btn_add_component = QPushButton("+ " + tr("Add Component"))
        self._btn_add_component.setFixedWidth(160)
        self._btn_add_component.clicked.connect(self._add_component)
        add_comp_row = QHBoxLayout()
        add_comp_row.addWidget(self._btn_add_component)
        add_comp_row.addStretch()
        main_layout.addLayout(add_comp_row)

        # --- Modifiers Row ---
        mod_row = QHBoxLayout()
        mod_row.addWidget(QLabel(tr("Modifiers") + ":"))

        self.chk_prefix = QCheckBox(tr("Prefixes #_"))
        self.chk_prefix.setToolTip(tr("Grammatical prefixes tooltip"))
        mod_row.addWidget(self.chk_prefix)

        self.chk_suffix = QCheckBox(tr("Suffixes _#"))
        self.chk_suffix.setToolTip(tr("Grammatical suffixes tooltip"))
        mod_row.addWidget(self.chk_suffix)

        self.chk_wild_start = QCheckBox(tr("Wildcard *_"))
        self.chk_wild_start.setToolTip(tr("Words ending with..."))
        mod_row.addWidget(self.chk_wild_start)

        self.chk_wild_end = QCheckBox(tr("Wildcard _*"))
        self.chk_wild_end.setToolTip(tr("Words starting with..."))
        mod_row.addWidget(self.chk_wild_end)

        self.chk_plene = QCheckBox(tr("Plene/Defective %"))
        self.chk_plene.setToolTip(tr("Plene/defective spelling tooltip"))
        mod_row.addWidget(self.chk_plene)

        self.chk_negation = QCheckBox(tr("Negation −"))
        self.chk_negation.setToolTip(tr("Negation tooltip"))
        mod_row.addWidget(self.chk_negation)

        self.chk_line_start = QCheckBox(tr("Start of line |_"))
        self.chk_line_start.setToolTip(tr("Word must appear at start of line"))
        mod_row.addWidget(self.chk_line_start)
        self.chk_line_start.setVisible(False)  # Only visible in Lines scope

        self.chk_line_end = QCheckBox(tr("End of line _|"))
        self.chk_line_end.setToolTip(tr("Word must appear at end of line"))
        mod_row.addWidget(self.chk_line_end)
        self.chk_line_end.setVisible(False)  # Only visible in Lines scope

        mod_row.addStretch()
        main_layout.addLayout(mod_row)

        # Connect modifier checkboxes
        for chk in [self.chk_prefix, self.chk_suffix, self.chk_wild_start,
                     self.chk_wild_end, self.chk_plene, self.chk_negation,
                     self.chk_line_start, self.chk_line_end]:
            chk.stateChanged.connect(self._on_modifier_changed)

        # --- Search Options Row ---
        opts_row = QHBoxLayout()
        opts_row.addWidget(QLabel(tr("Search Options") + ":"))
        self.chk_opt_variants = QCheckBox(tr("Variants"))
        self.chk_opt_ja = QCheckBox(tr("Judeo-Arabic"))
        self.chk_opt_flex = QCheckBox(tr("Flex Spacing"))
        self.chk_opt_bidir = QCheckBox(tr("Bidirectional"))
        opts_row.addWidget(self.chk_opt_variants)
        opts_row.addWidget(self.chk_opt_ja)
        opts_row.addWidget(self.chk_opt_flex)
        opts_row.addWidget(self.chk_opt_bidir)
        opts_row.addStretch()
        main_layout.addLayout(opts_row)

        # --- Preview Row ---
        preview_row = QHBoxLayout()
        preview_row.addWidget(QLabel(tr("Preview") + ":"))
        self._preview_label = QLabel("")
        if self._is_dark:
            preview_bg = '#2d2d2d'
            preview_border = '#555'
        else:
            preview_bg = '#f8f9fa'
            preview_border = '#dee2e6'
        self._preview_label.setStyleSheet(
            f"font-family: 'Consolas', 'Courier New', monospace; font-size: 13px; "
            f"padding: 4px 8px; background: {preview_bg}; border: 1px solid {preview_border}; border-radius: 4px; "
            f"min-height: 22px;"
        )
        self._preview_label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self._preview_label.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        preview_row.addWidget(self._preview_label, 1)
        main_layout.addLayout(preview_row)

        # --- Buttons Row ---
        btn_row = QHBoxLayout()
        btn_clear = QPushButton(tr("Clear All"))
        btn_clear.clicked.connect(self._clear_all)
        btn_row.addWidget(btn_clear)
        btn_row.addStretch()
        btn_cancel = QPushButton(tr("Cancel"))
        btn_cancel.clicked.connect(self.reject)
        btn_row.addWidget(btn_cancel)
        btn_search = QPushButton(tr("Search"))
        btn_search.setStyleSheet("background-color: #27ae60; color: white; font-weight: bold; padding: 6px 20px;")
        btn_search.clicked.connect(self._apply)
        btn_row.addWidget(btn_search)
        main_layout.addLayout(btn_row)

    def _initialize_components(self):
        """Create the initial 2 components with distance spinner between them."""
        for i in range(self._initial_components):
            self._create_component(i)
            if i < self._initial_components - 1:
                self._create_distance_spinner(i)
        self._update_add_component_visibility()

    def _create_component(self, index):
        """Create a component card (QFrame with word inputs)."""
        # Data
        comp_data = {
            'words': [{'text': '', 'mods': {}} for _ in range(self._max_words_per_component)]
        }
        self._component_data.append(comp_data)

        # UI
        frame = QFrame()
        frame.setFrameStyle(QFrame.Shape.Box | QFrame.Shadow.Plain)
        if self._is_dark:
            frame_bg = '#2a2a2a'
            frame_border = '#555'
        else:
            frame_bg = '#fafafa'
            frame_border = '#bdc3c7'
        frame.setStyleSheet(
            f"QFrame {{ border: 1px solid {frame_border}; border-radius: 6px; background: {frame_bg}; }}"
        )
        frame_layout = QVBoxLayout(frame)
        frame_layout.setSpacing(4)
        frame_layout.setContentsMargins(8, 6, 8, 6)

        # Title
        title_label = QLabel(tr("Component") + f" {index + 1}")
        title_color = '#ddd' if self._is_dark else '#333'
        title_label.setStyleSheet(f"font-weight: bold; font-size: 12px; border: none; background: transparent; color: {title_color};")
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        frame_layout.addWidget(title_label)

        # Word inputs and modifier indicators
        inputs = []
        indicators = []
        ind_color = '#7cabd4' if self._is_dark else '#2980b9'
        for wi in range(self._max_words_per_component):
            inp = QLineEdit()
            inp.setPlaceholderText(tr("Word") + f" {wi + 1}")
            inp.setMinimumWidth(120)
            if self._is_dark:
                inp_bg = '#3a3a3a'
                inp_border = '#666'
                inp_color = '#eee'
            else:
                inp_bg = 'white'
                inp_border = '#ccc'
                inp_color = '#333'
            inp.setStyleSheet(f"border: 1px solid {inp_border}; border-radius: 3px; padding: 3px; background: {inp_bg}; color: {inp_color};")
            inp.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
            inp.installEventFilter(self)
            inp.textChanged.connect(self._on_word_text_changed)
            frame_layout.addWidget(inp)
            # Modifier indicator label below input
            mod_ind = QLabel("")
            mod_ind.setStyleSheet(f"font-size: 9px; color: {ind_color}; border: none; background: transparent; margin-top: -2px;")
            mod_ind.setVisible(False)
            frame_layout.addWidget(mod_ind)
            indicators.append(mod_ind)
            inputs.append(inp)
            # Hide extra word slots
            if wi >= self._initial_words_visible:
                inp.setVisible(False)
                mod_ind.setVisible(False)

        # Add word button
        btn_add_word = QPushButton("+ " + tr("Add Word"))
        add_word_color = '#5dade2' if self._is_dark else '#2980b9'
        btn_add_word.setStyleSheet(f"font-size: 10px; border: none; color: {add_word_color}; background: transparent;")
        btn_add_word.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        frame_layout.addWidget(btn_add_word)

        # Remove button (only for components 3+)
        btn_remove = QPushButton(tr("Remove"))
        remove_color = '#e74c3c' if self._is_dark else '#c0392b'
        btn_remove.setStyleSheet(f"font-size: 10px; color: {remove_color}; border: none; background: transparent;")
        btn_remove.setCursor(QCursor(Qt.CursorShape.PointingHandCursor))
        btn_remove.setVisible(index >= self._initial_components)
        frame_layout.addWidget(btn_remove)

        frame_layout.addStretch()

        comp_widget = {
            'frame': frame,
            'inputs': inputs,
            'indicators': indicators,
            'btn_add_word': btn_add_word,
            'btn_remove': btn_remove,
            'title_label': title_label,
            'visible_words': self._initial_words_visible,
        }
        self._component_widgets.append(comp_widget)

        # Connect buttons with closure over index
        ci = len(self._component_widgets) - 1
        btn_add_word.clicked.connect(lambda checked=False, idx=ci: self._show_next_word(idx))
        btn_remove.clicked.connect(lambda checked=False, idx=ci: self._remove_component(idx))

        self._components_layout.addWidget(frame)
        self._update_add_word_visibility(ci)

    def _create_distance_spinner(self, pair_index):
        """Create a distance spinner between components pair_index and pair_index+1."""
        container = QWidget()
        container_layout = QVBoxLayout(container)
        container_layout.setContentsMargins(2, 0, 2, 0)
        container_layout.setSpacing(2)
        container_layout.addStretch()

        dist_label = QLabel(tr("Distance"))
        dist_color = '#aab' if self._is_dark else '#7f8c8d'
        dist_label.setStyleSheet(f"font-size: 10px; color: {dist_color};")
        dist_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        container_layout.addWidget(dist_label)

        spinner = QSpinBox()
        spinner.setRange(0, 50)
        spinner.setValue(0)
        spinner.setFixedWidth(60)
        spinner.setAlignment(Qt.AlignmentFlag.AlignCenter)
        spinner.valueChanged.connect(lambda v: self._update_preview())
        container_layout.addWidget(spinner)

        words_label = QLabel(tr("words"))
        words_sub_color = '#999' if self._is_dark else '#95a5a6'
        words_label.setStyleSheet(f"font-size: 9px; color: {words_sub_color};")
        words_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        container_layout.addWidget(words_label)

        container_layout.addStretch()

        self._distance_spinners.append(spinner)
        self._distance_containers.append(container)

        # Insert in layout before the next component
        # The layout has: comp0, dist0, comp1, dist1, comp2, ...
        # We insert at position 2*pair_index + 1
        insert_pos = 2 * pair_index + 1
        self._components_layout.insertWidget(insert_pos, container)

        # Hide if scope is Within Document
        if self._rb_within_doc.isChecked():
            container.setVisible(False)

    def _update_add_word_visibility(self, comp_idx):
        """Show/hide the + button based on how many word slots are visible."""
        if comp_idx >= len(self._component_widgets):
            return
        cw = self._component_widgets[comp_idx]
        visible_count = cw['visible_words']
        cw['btn_add_word'].setVisible(visible_count < self._max_words_per_component)

    def _update_add_component_visibility(self):
        """Show/hide the + Component button based on current count."""
        active_count = len(self._component_widgets)
        self._btn_add_component.setVisible(active_count < self._max_components)

    def _show_next_word(self, comp_idx):
        """Reveal the next hidden word input in the given component."""
        if comp_idx >= len(self._component_widgets):
            return
        cw = self._component_widgets[comp_idx]
        visible = cw['visible_words']
        if visible < self._max_words_per_component:
            cw['inputs'][visible].setVisible(True)
            cw['visible_words'] = visible + 1
            self._update_add_word_visibility(comp_idx)

    def _add_component(self):
        """Add a new component (up to max 4)."""
        current_count = len(self._component_widgets)
        if current_count >= self._max_components:
            return
        # Add distance spinner before the new component
        self._create_distance_spinner(current_count - 1)
        self._create_component(current_count)
        self._update_add_component_visibility()
        self._update_preview()

    def _remove_component(self, comp_idx):
        """Remove a component (cannot go below 2)."""
        if len(self._component_widgets) <= self._initial_components:
            return
        if comp_idx < self._initial_components:
            return

        # Remove the component widget
        cw = self._component_widgets.pop(comp_idx)
        cw['frame'].setParent(None)
        cw['frame'].deleteLater()

        # Remove component data
        self._component_data.pop(comp_idx)

        # Remove the distance spinner before this component
        dist_idx = comp_idx - 1
        if dist_idx >= 0 and dist_idx < len(self._distance_spinners):
            self._distance_spinners.pop(dist_idx)
            container = self._distance_containers.pop(dist_idx)
            container.setParent(None)
            container.deleteLater()

        # Reset active word if it was in the removed component
        if self._active_word and self._active_word[0] >= len(self._component_widgets):
            self._active_word = None

        # Renumber component titles
        for i, cw in enumerate(self._component_widgets):
            cw['title_label'].setText(tr("Component") + f" {i + 1}")
            cw['btn_remove'].setVisible(i >= self._initial_components)

        # Reconnect button lambdas (re-bind indices)
        for i, cw in enumerate(self._component_widgets):
            try:
                cw['btn_add_word'].clicked.disconnect()
            except TypeError:
                pass
            try:
                cw['btn_remove'].clicked.disconnect()
            except TypeError:
                pass
            cw['btn_add_word'].clicked.connect(lambda checked=False, idx=i: self._show_next_word(idx))
            cw['btn_remove'].clicked.connect(lambda checked=False, idx=i: self._remove_component(idx))

        self._update_add_component_visibility()
        self._update_preview()

    def _on_scope_changed(self, button_id, checked):
        """Toggle distance spinner/modifier visibility based on scope."""
        if not checked:
            return
        is_word_range = (button_id == 0)
        is_lines = (button_id == 2)
        # Distance spinners: visible for word_range and lines, hidden for within_document
        for container in self._distance_containers:
            container.setVisible(is_word_range or is_lines)
        # Update distance labels: "words" vs "lines"
        for container in self._distance_containers:
            labels = container.findChildren(QLabel)
            for lbl in labels:
                if lbl.text() in (tr("words"), tr("lines")):
                    lbl.setText(tr("lines") if is_lines else tr("words"))
        # Line position modifiers: only visible in Lines scope
        self.chk_line_start.setVisible(is_lines)
        self.chk_line_end.setVisible(is_lines)
        self._update_preview()

    def _on_word_focus(self, comp_idx, word_idx):
        """Handle focus on a word input -- update modifier checkboxes."""
        self._active_word = (comp_idx, word_idx)
        self._updating_modifiers = True
        try:
            mods = self._component_data[comp_idx]['words'][word_idx].get('mods', {})
            self.chk_prefix.setChecked(mods.get('prefix', False))
            self.chk_suffix.setChecked(mods.get('suffix', False))
            self.chk_wild_start.setChecked(mods.get('wildcard_prefix', False))
            self.chk_wild_end.setChecked(mods.get('wildcard_suffix', False))
            self.chk_plene.setChecked(mods.get('plene', False))
            self.chk_negation.setChecked(mods.get('negation', False))
            self.chk_line_start.setChecked(mods.get('line_start', False))
            self.chk_line_end.setChecked(mods.get('line_end', False))
        finally:
            self._updating_modifiers = False

    _MOD_DISPLAY = {
        'prefix': '#_', 'suffix': '_#',
        'wildcard_prefix': '*_', 'wildcard_suffix': '_*',
        'plene': '%', 'negation': '−',
        'line_start': '|_', 'line_end': '_|',
    }

    def _update_mod_indicator(self, ci, wi):
        """Update the modifier indicator label for a specific word."""
        if ci < len(self._component_widgets) and wi < len(self._component_widgets[ci].get('indicators', [])):
            mods = self._component_data[ci]['words'][wi]['mods']
            parts = [v for k, v in self._MOD_DISPLAY.items() if mods.get(k)]
            text = ' '.join(parts)
            ind = self._component_widgets[ci]['indicators'][wi]
            ind.setText(text)
            ind.setVisible(bool(text))

    def _on_modifier_changed(self):
        """Save modifier state to the active word's data."""
        if self._updating_modifiers or self._active_word is None:
            return
        ci, wi = self._active_word
        if ci >= len(self._component_data):
            return
        mods = {
            'prefix': self.chk_prefix.isChecked(),
            'suffix': self.chk_suffix.isChecked(),
            'wildcard_prefix': self.chk_wild_start.isChecked(),
            'wildcard_suffix': self.chk_wild_end.isChecked(),
            'plene': self.chk_plene.isChecked(),
            'negation': self.chk_negation.isChecked(),
            'line_start': self.chk_line_start.isChecked(),
            'line_end': self.chk_line_end.isChecked(),
        }
        self._component_data[ci]['words'][wi]['mods'] = mods
        self._update_mod_indicator(ci, wi)
        self._update_preview()

    def _on_word_text_changed(self, text):
        """Sync QLineEdit text back to component data and update preview."""
        sender = self.sender()
        if sender is None:
            return
        for ci, cw in enumerate(self._component_widgets):
            for wi, inp in enumerate(cw['inputs']):
                if inp is sender:
                    self._component_data[ci]['words'][wi]['text'] = text
                    self._update_preview()
                    return

    def _update_preview(self):
        """Regenerate syntax from current state and update preview label."""
        # Build components list in generate_tabular_syntax format
        components = []
        for ci, comp in enumerate(self._component_data):
            words = []
            for wi, word_data in enumerate(comp['words']):
                # Only include words from visible slots
                if ci < len(self._component_widgets) and wi < self._component_widgets[ci]['visible_words']:
                    words.append({
                        'text': word_data.get('text', ''),
                        'mods': word_data.get('mods', {}),
                    })
            components.append({'words': words})

        # Build distances list
        distances = [s.value() for s in self._distance_spinners]

        # Get scope
        if self._rb_lines.isChecked():
            scope = 'lines'
        elif self._rb_within_doc.isChecked():
            scope = 'within_document'
        else:
            scope = 'word_range'

        try:
            syntax, negated = generate_tabular_syntax(components, distances, scope)
            self._syntax = syntax
            self._negated_words = negated
        except Exception:
            self._syntax = ''  # Syntax parse failed; reset to empty
            self._negated_words = []

        self._preview_label.setText(self._syntax if self._syntax else "")

    def _clear_all(self):
        """Reset all inputs, modifiers, spinners, and components to initial state."""
        # Remove extra components (keep only initial 2)
        while len(self._component_widgets) > self._initial_components:
            idx = len(self._component_widgets) - 1
            cw = self._component_widgets.pop(idx)
            cw['frame'].setParent(None)
            cw['frame'].deleteLater()
            self._component_data.pop(idx)

        # Remove extra distance spinners
        while len(self._distance_spinners) > self._initial_components - 1:
            self._distance_spinners.pop()
            container = self._distance_containers.pop()
            container.setParent(None)
            container.deleteLater()

        # Reset remaining components
        for ci, cw in enumerate(self._component_widgets):
            for wi, inp in enumerate(cw['inputs']):
                inp.blockSignals(True)
                inp.clear()
                inp.blockSignals(False)
                inp.setVisible(wi < self._initial_words_visible)
            cw['visible_words'] = self._initial_words_visible
            self._update_add_word_visibility(ci)
            # Reset data
            self._component_data[ci] = {
                'words': [{'text': '', 'mods': {}} for _ in range(self._max_words_per_component)]
            }

        # Reset spinners
        for spinner in self._distance_spinners:
            spinner.blockSignals(True)
            spinner.setValue(0)
            spinner.blockSignals(False)

        # Reset modifiers
        self._active_word = None
        self._updating_modifiers = True
        for chk in [self.chk_prefix, self.chk_suffix, self.chk_wild_start,
                     self.chk_wild_end, self.chk_plene, self.chk_negation]:
            chk.setChecked(False)
        self._updating_modifiers = False

        # Reset scope
        self._rb_word_range.setChecked(True)

        self._update_add_component_visibility()
        self._update_preview()

    def _apply(self):
        """Generate final syntax and accept the dialog."""
        self._update_preview()
        self.accept()

    def get_syntax(self) -> str:
        """Return the generated Responsa syntax string."""
        self._update_preview()
        return self._syntax

    def get_negated_words(self) -> list:
        """Return list of words marked for exclusion."""
        self._update_preview()
        return self._negated_words

    def eventFilter(self, obj, event):
        """Catch focus events on word inputs to update modifier checkboxes."""
        if event.type() == QEvent.Type.FocusIn:
            for ci, comp in enumerate(self._component_widgets):
                for wi, inp in enumerate(comp['inputs']):
                    if inp is obj:
                        self._on_word_focus(ci, wi)
                        return super().eventFilter(obj, event)
        return super().eventFilter(obj, event)


class SettingsDialog(QDialog):
    """Modal settings dialog with General and About tabs."""

    FULL_CITATION = (
        "Stoekl Ben Ezra, D., Bambaci, L., Kiessling, B., Lapin, H., Ezer, N., "
        "Lolli, E., Rustow, M., Dershowitz, N., Kurar Barakat, B., Gogawale, S., "
        "Shmidman, A., Lavee, M., Siew, T., Raziel Kretzmer, V., "
        "Vasyutinsky Shapira, D., Olszowy-Schlanger, J., & Gila, Y. (2025). "
        "MiDRASH Automatic Transcriptions. Zenodo. "
        "https://doi.org/10.5281/zenodo.17734473"
    )

    def __init__(self, parent):
        super().__init__(parent)
        self.main_win = parent
        is_heb = CURRENT_LANG == 'he'
        self.setWindowTitle(tr("הגדרות") if is_heb else "Settings")
        self.setFixedSize(650, 500)
        self.setModal(True)
        if is_heb:
            self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)

        # Palette-aware colors
        pal = QApplication.palette()
        self._is_dark = pal.color(QPalette.ColorRole.Window).lightness() < 128
        self._text = pal.color(QPalette.ColorRole.Text).name()
        self._base = pal.color(QPalette.ColorRole.Base).name()
        self._muted = '#888' if self._is_dark else '#666'
        self._border = '#555' if self._is_dark else '#d0d0d0'
        self._combo_w = 140
        self._cit_bg = '#1e2a36' if self._is_dark else '#eef3f8'
        self._cit_border = '#2c3e50' if self._is_dark else '#c8d6e0'

        # Snapshot config on open so Cancel can restore it.
        # D-07b / T-112-CancelDesync: telemetry keys are stripped from the snapshot
        # so that save_app_config(self._config_snapshot) in _on_cancel cannot
        # overwrite them. save_app_config is additive-merge (genizah_core.py:2882-2890:
        # cfg.update(new_data) — keys absent from new_data are left untouched),
        # so omitted keys remain whatever set_consent() last wrote. DO NOT "fix" this
        # back to a full dict() — the strip is intentional.
        from desktop.telemetry import (  # noqa: PLC0415
            TELEMETRY_ENABLED_KEY, FIRST_RUN_SHOWN_KEY as _FRSKEY,
            TELEMETRY_INSTALL_ID_KEY, CONSENT_TIMESTAMP_KEY,
            CONSENT_APP_VERSION_KEY, CONSENT_UI_VERSION_KEY, IDENTIFIED_USER_KEY,
        )
        _TELEMETRY_SNAPSHOT_EXCLUDE = frozenset({
            TELEMETRY_ENABLED_KEY, _FRSKEY, TELEMETRY_INSTALL_ID_KEY,
            CONSENT_TIMESTAMP_KEY, CONSENT_APP_VERSION_KEY,
            CONSENT_UI_VERSION_KEY, IDENTIFIED_USER_KEY,
        })
        self._config_snapshot = {
            k: v for k, v in load_app_config().items()
            if k not in _TELEMETRY_SNAPSHOT_EXCLUDE
        }

        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)

        self._tabs = QTabWidget()
        self._tabs.addTab(self._build_general_tab(), tr("כללי") if is_heb else "General")
        self._tabs.addTab(self._build_about_tab(), tr("אודות") if is_heb else "About")
        outer.addWidget(self._tabs)

        # OK / Cancel buttons. The OK/Cancel click paths route through the thin
        # GenizahGUI.apply_settings()/cancel_settings() API (DESK-01 / SP-4
        # boundary) instead of reaching into GUI internals; the dialog still owns
        # the D-07b config snapshot via _on_cancel.
        btn_layout = QHBoxLayout()
        btn_layout.setContentsMargins(12, 4, 12, 12)
        btn_layout.addStretch()
        btn_cancel = QPushButton(tr("ביטול") if is_heb else tr("Cancel"))
        btn_cancel.setFixedWidth(90)
        btn_cancel.clicked.connect(self.main_win.cancel_settings)
        btn_layout.addWidget(btn_cancel)
        btn_ok = QPushButton(tr("אישור") if is_heb else tr("OK"))
        btn_ok.setFixedWidth(90)
        btn_ok.setDefault(True)
        btn_ok.clicked.connect(self.main_win.apply_settings)
        btn_layout.addWidget(btn_ok)
        outer.addLayout(btn_layout)

    def _on_cancel(self):
        """Restore config snapshot and close."""
        save_app_config(self._config_snapshot)
        self.reject()

    # ── General Tab ──────────────────────────────────────────────
    def _build_general_tab(self):
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(20, 16, 20, 16)
        layout.setSpacing(0)

        # — Preferences —
        layout.addWidget(self._section_label(tr("העדפות") if CURRENT_LANG == 'he' else "Preferences"))
        layout.addSpacing(6)

        # Use HBoxLayout rows so controls sit right next to labels
        def _pref_row(label_text, widget):
            row = QHBoxLayout()
            row.setSpacing(8)
            row.addWidget(QLabel(label_text))
            row.addWidget(widget)
            row.addStretch()
            return row

        # Language
        self.combo_language = QComboBox()
        self.combo_language.addItems(["עברית", "English"])
        self.combo_language.setCurrentIndex(0 if CURRENT_LANG == 'he' else 1)
        self.combo_language.setFixedWidth(self._combo_w)
        self.combo_language.currentIndexChanged.connect(self.main_win._on_language_combo_changed)
        layout.addLayout(_pref_row(tr("שפה:") if CURRENT_LANG == 'he' else "Language:", self.combo_language))
        layout.addSpacing(4)

        # Desktop Notifications
        from PyQt6.QtWidgets import QCheckBox
        self.main_win.chk_notifications = QCheckBox(tr("Desktop Notifications"))
        self.main_win.chk_notifications.setChecked(load_app_config().get('notifications_enabled', True))
        self.main_win.chk_notifications.setToolTip(tr("Flash taskbar when search completes while app is in background"))
        self.main_win.chk_notifications.stateChanged.connect(
            lambda state: save_app_config({'notifications_enabled': state == 2})
        )
        notif_row = QHBoxLayout()
        notif_row.addWidget(self.main_win.chk_notifications)
        notif_row.addStretch()
        layout.addLayout(notif_row)
        layout.addSpacing(4)

        # Restore State
        self.main_win.combo_restore_mode = QComboBox()
        self.main_win.combo_restore_mode.addItems([tr("Ask"), tr("Always"), tr("Never")])
        restore_mode = load_app_config().get('restore_mode', 'ask')
        self.main_win.combo_restore_mode.setCurrentIndex({'ask': 0, 'always': 1, 'never': 2}.get(restore_mode, 0))
        self.main_win.combo_restore_mode.setToolTip(tr("Whether to restore previous search state on startup"))
        self.main_win.combo_restore_mode.currentIndexChanged.connect(
            lambda idx: save_app_config({'restore_mode': ['ask', 'always', 'never'][idx]})
        )
        self.main_win.combo_restore_mode.setFixedWidth(self._combo_w)
        layout.addLayout(_pref_row(tr("Restore State:"), self.main_win.combo_restore_mode))
        layout.addSpacing(4)

        # History Limit
        self.main_win.spin_history_limit = QSpinBox()
        self.main_win.spin_history_limit.setRange(5, 100)
        self.main_win.spin_history_limit.setValue(load_app_config().get('history_limit', 20))
        self.main_win.spin_history_limit.setToolTip(tr("Maximum search history entries per type"))
        self.main_win.spin_history_limit.valueChanged.connect(
            lambda val: save_app_config({'history_limit': val})
        )
        self.main_win.spin_history_limit.setFixedWidth(80)
        layout.addLayout(_pref_row(tr("History Limit:"), self.main_win.spin_history_limit))
        layout.addSpacing(4)

        # Show Translations toggle (Phase 46)
        self.main_win.chk_show_translations = QCheckBox(tr("Show translations"))
        self.main_win.chk_show_translations.setChecked(load_app_config().get('show_translations', False))
        self.main_win.chk_show_translations.setToolTip(tr("Show translated descriptions when available"))
        def _on_settings_trans_changed(state):
            checked = state == 2
            save_app_config({'show_translations': checked})
            _label = tr('Translations ON') if checked else tr('Translations OFF')
            if hasattr(self.main_win, 'btn_b_translations'):
                self.main_win.btn_b_translations.setChecked(checked)
                self.main_win.btn_b_translations.setText(_label)
            if hasattr(self.main_win, 'btn_search_translations'):
                self.main_win.btn_search_translations.setChecked(checked)
                self.main_win.btn_search_translations.setText(_label)
        self.main_win.chk_show_translations.stateChanged.connect(_on_settings_trans_changed)
        trans_row = QHBoxLayout()
        trans_row.addWidget(self.main_win.chk_show_translations)
        trans_row.addStretch()
        layout.addLayout(trans_row)

        layout.addSpacing(4)

        # Telemetry toggle (Phase 112, CONSENT-04)
        # D-08: consent routes ONLY through set_consent() — never raw save_app_config.
        # D-07a: confirm-on-change → immediate set_consent(); revert on cancel-of-confirm.
        from desktop.telemetry import is_enabled as _tel_is_enabled, set_consent as _tel_set_consent  # noqa: PLC0415
        from desktop.consent_dialog import PrivacyDialog  # noqa: PLC0415

        # UI-language single string (only the one-time startup consent dialog is
        # bilingual; Settings follows CURRENT_LANG).
        _he_tel = CURRENT_LANG == 'he'
        self.chk_telemetry = QCheckBox(
            "עזרו לשפר את האפליקציה — שליחת נתוני שימוש תוך שמירה על הפרטיות"
            if _he_tel else
            "Help improve the app — send privacy-preserving usage data"
        )
        # Pitfall 5: block signals during initial setChecked to prevent spurious
        # stateChanged on dialog open.
        self.chk_telemetry.blockSignals(True)
        self.chk_telemetry.setChecked(_tel_is_enabled())
        self.chk_telemetry.blockSignals(False)

        def _on_telemetry_changed(state):
            # Read the widget directly rather than interpreting `state` as int 2 —
            # PyQt6's stateChanged may deliver a Qt.CheckState enum in newer bindings,
            # where `state == 2` is False even when checked (WR-01, privacy-critical).
            new_val = self.chk_telemetry.isChecked()
            prior = _tel_is_enabled()
            if new_val == prior:
                return
            _he = CURRENT_LANG == 'he'
            if new_val:
                if _he:
                    title = "הפעלת טלמטריה?"
                    msg = (
                        "נתוני שימוש יתחילו להישלח כעת, תוך שמירה על הפרטיות.\n"
                        "אפשר לכבות בכל עת בהגדרות."
                    )
                else:
                    title = "Enable telemetry?"
                    msg = (
                        "Privacy-preserving usage data will start being sent now.\n"
                        "You can turn this off at any time in Settings."
                    )
            else:
                if _he:
                    title = "כיבוי טלמטריה?"
                    msg = (
                        "איסוף נתוני השימוש ייפסק כעת.\n"
                        "אירועים בתור יימחקו מיד."
                    )
                else:
                    title = "Disable telemetry?"
                    msg = (
                        "Privacy-preserving usage data collection will stop now.\n"
                        "Queued events will be discarded immediately."
                    )
            # Build an instance (not the static question()) so the Yes/No buttons
            # can be localized — Qt ships English standard-button text; tr() maps
            # "Yes"/"No" → "כן"/"לא" in the Hebrew UI.
            box = QMessageBox(self)
            box.setIcon(QMessageBox.Icon.Question)
            box.setWindowTitle(title)
            box.setText(msg)
            box.setStandardButtons(
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            box.setDefaultButton(QMessageBox.StandardButton.No)
            _yes_btn = box.button(QMessageBox.StandardButton.Yes)
            _no_btn = box.button(QMessageBox.StandardButton.No)
            if _yes_btn is not None:
                _yes_btn.setText(tr("Yes"))
            if _no_btn is not None:
                _no_btn.setText(tr("No"))
            box.exec()
            if box.standardButton(box.clickedButton()) == QMessageBox.StandardButton.Yes:
                _tel_set_consent(new_val)  # D-08: sole write path
                # D-13/HIGH-4: mid-session opt-in — re-run coordinator so
                # identify fires for the logged-in user and session_start fires
                # (once) before any subsequent usage event.
                if new_val:
                    self._run_startup_telemetry_coordinator()
            else:
                # D-07a: revert visual, do NOT call set_consent
                self.chk_telemetry.blockSignals(True)
                self.chk_telemetry.setChecked(prior)
                self.chk_telemetry.blockSignals(False)

        self.chk_telemetry.stateChanged.connect(_on_telemetry_changed)

        btn_privacy = QPushButton("פרטיות" if _he_tel else "Privacy details")
        btn_privacy.setFlat(True)
        btn_privacy.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_privacy.setStyleSheet("color: #2563eb; text-decoration: underline; border: none;")
        btn_privacy.clicked.connect(lambda: PrivacyDialog(self).exec())

        telemetry_row = QHBoxLayout()
        telemetry_row.addWidget(self.chk_telemetry)
        telemetry_row.addSpacing(8)
        telemetry_row.addWidget(btn_privacy)
        telemetry_row.addStretch()
        layout.addLayout(telemetry_row)
        layout.addSpacing(4)

        layout.addSpacing(8)

        # — Updates —
        sep1 = QFrame(); sep1.setFrameShape(QFrame.Shape.HLine)
        sep1.setStyleSheet(f"color: {self._border};")
        layout.addWidget(sep1)
        layout.addSpacing(8)
        layout.addWidget(self._section_label(tr("עדכונים") if CURRENT_LANG == 'he' else "Updates"))
        layout.addSpacing(6)

        ver_row = QHBoxLayout()
        self.main_win.lbl_version = QLabel(f"Version: {APP_VERSION}")
        self.main_win.lbl_version.setStyleSheet(f"color: {self._muted};")
        ver_row.addWidget(self.main_win.lbl_version)
        ver_row.addSpacing(12)
        self.main_win.btn_check_updates = QPushButton(tr("Check for Updates"))
        self.main_win.btn_check_updates.clicked.connect(self.main_win.check_updates_manual)
        ver_row.addWidget(self.main_win.btn_check_updates)
        ver_row.addStretch()
        layout.addLayout(ver_row)

        layout.addSpacing(12)

        # — Data & Indexing (bottom half) —
        sep2 = QFrame(); sep2.setFrameShape(QFrame.Shape.HLine)
        sep2.setStyleSheet(f"color: {self._border};")
        layout.addWidget(sep2)
        layout.addSpacing(8)
        layout.addWidget(self._section_label(tr("Data & Index")))
        layout.addSpacing(6)

        btn_row1 = QHBoxLayout()
        btn_dl = QPushButton(tr("Download Transcriptions (Zenodo)"))
        btn_dl.clicked.connect(lambda: QDesktopServices.openUrl(QUrl("https://doi.org/10.5281/zenodo.17734473")))
        btn_row1.addWidget(btn_dl)
        btn_row1.addStretch()
        layout.addLayout(btn_row1)
        layout.addSpacing(4)

        btn_row2 = QHBoxLayout()
        self.main_win.btn_build_index = QPushButton(tr("Build / Rebuild Index"))
        self.main_win.btn_build_index.clicked.connect(self.main_win.run_indexing)
        self.main_win.btn_build_index.setEnabled(False)
        btn_row2.addWidget(self.main_win.btn_build_index)
        btn_row2.addStretch()
        layout.addLayout(btn_row2)
        layout.addSpacing(4)

        self.main_win.index_progress = QProgressBar()
        self.main_win.index_progress.setVisible(False)
        layout.addWidget(self.main_win.index_progress)

        layout.addSpacing(8)

        # Data Sources list
        sources = []
        try:
            from shared.document_service import get_pgp_service
            pgp_svc = get_pgp_service()
            pgp_ver = pgp_svc.get_version() if pgp_svc.is_available() else None
        except Exception:
            pgp_ver = None  # Version query failed; show None in About dialog
        sources.append(("PGP Documents", pgp_ver))
        try:
            from shared.fjms_service import get_fjms_service
            fjms_svc = get_fjms_service()
            fjms_ver = fjms_svc.get_version() if fjms_svc.is_available() else None
        except Exception:
            fjms_ver = None  # Version query failed; show None in About dialog
        sources.append(("FJMS Catalog", fjms_ver))
        try:
            from shared.nli_crossref_service import get_nli_crossref_service
            nli_svc = get_nli_crossref_service()
            nli_ver = nli_svc.get_version() if nli_svc.is_available() else None
        except Exception:
            nli_ver = None  # Version query failed; show None in About dialog
        sources.append(("NLI Crossref", nli_ver))

        for name, ver in sources:
            row = QHBoxLayout()
            row.setSpacing(6)
            icon = QLabel("\u2713" if ver else "\u2014")
            icon.setStyleSheet(("color: #27ae60; font-weight: bold;" if ver else f"color: {self._muted};") + " font-size: 13px;")
            icon.setFixedWidth(16)
            row.addWidget(icon)
            row.addWidget(QLabel(name))
            vlbl = QLabel(f"v{ver}" if ver else "")
            vlbl.setStyleSheet(f"color: {self._muted}; font-size: 11px;")
            row.addWidget(vlbl)
            row.addStretch()
            layout.addLayout(row)

        # — Visual Similarity Download — deferred (nginx proxy_max_temp_file_size blocks 1.3GB)
        # TODO: re-enable after fixing nginx config for large file downloads

        layout.addStretch()
        return page

    def _start_vs_download(self):
        """Start downloading the full visual_similarity.db with robustness checks."""
        dest_dir = os.path.join(
            os.environ.get('LOCALAPPDATA', os.path.expanduser('~')),
            'GenizahSearchPro', 'data'
        )
        server_url = "https://genizahsearch.com"
        self._vs_download_btn.setEnabled(False)
        self._vs_download_btn.setText(tr("Downloading..."))
        self._vs_download_progress.setVisible(True)
        self._vs_download_progress.setRange(0, 100)
        self._vs_download_progress.setValue(0)
        self._vs_download_status.setText(tr("Downloading..."))

        self._vs_dl_thread = VSDownloadThread(server_url, dest_dir, self)
        self._vs_dl_thread.progress.connect(self._on_vs_download_progress)
        self._vs_dl_thread.finished.connect(self._on_vs_download_complete)
        self._vs_dl_thread.error.connect(self._on_vs_download_error)
        self._vs_dl_thread.start()

    def _on_vs_download_progress(self, downloaded, total):
        if total > 0:
            pct = int(downloaded * 100 / total)
            self._vs_download_progress.setValue(pct)
            mb_done = downloaded / (1024 * 1024)
            mb_total = total / (1024 * 1024)
            self._vs_download_status.setText(f"{mb_done:.0f} / {mb_total:.0f} MB ({pct}%)")

    def _on_vs_download_complete(self, path):
        self._vs_download_btn.setEnabled(True)
        self._vs_download_btn.setText(tr("Download complete"))
        self._vs_download_progress.setValue(100)
        self._vs_download_status.setText(f"\u2713 {tr('Download complete')}")
        self._vs_download_status.setStyleSheet("color: #27ae60; font-size: 11px;")
        # Reset service singleton to pick up new local DB
        from shared.visual_similarity_service import reset_vs_service
        reset_vs_service()

    def _on_vs_download_error(self, error_msg):
        self._vs_download_btn.setEnabled(True)
        self._vs_download_btn.setText(tr("Download full visual similarity database"))
        self._vs_download_progress.setVisible(False)
        self._vs_download_status.setText(f"\u2717 {tr('Download failed')}: {error_msg}")
        self._vs_download_status.setStyleSheet("color: #e74c3c; font-size: 11px;")

    # ── About Tab ────────────────────────────────────────────────
    def _build_about_tab(self):
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(20, 16, 20, 16)
        layout.setSpacing(8)

        about_html_en = f"""
        <style>
            h3 {{ margin-bottom: 2px; margin-top: 10px; }}
            p {{ margin-top: 4px; margin-bottom: 4px; line-height: 1.4; }}
            a {{ color: #2980b9; text-decoration: none; }}
        </style>
        <div style='font-family: Arial; font-size: 12px; color: {self._text};'>
            <div style='text-align:center;'>
                <h2 style='margin-bottom:4px;'>Dicta Genizah Search Pro {APP_VERSION}</h2>
                <p style='color: {self._muted};'>Developed by Hillel Gershuni
                (<a href='mailto:gershuni@gmail.com'>gershuni@gmail.com</a>)</p>
                <p>Web: <a href='https://genizahsearch.com'>GenizahSearch.com</a> &mdash; Dicta Genizah Search</p>
            </div>
            <hr>
            <h3>Dedicated to the memory of our beloved teacher, Prof. Menachem Kahana z"l</h3>

            <h3>Credits</h3>
            <p>This tool was built with the coding assistance of <b>Claude</b> (Anthropic),
            <b>Gemini</b> (Google), and <b>ChatGPT</b> (OpenAI).</p>
            <p>Many thanks to Prof. Moshe Koppel and <a href='https://dicta.org.il/'>Dicta</a>
            for their generous support and guidance.</p>
            <p>Thanks also to Avi Shmidman, Josh Guedalia, Elisha Rosenzweig, Ephraim Meiri,
            Elazar Gershuni, Itai Kagan, Elnatan Chen, and Adiel Breuer
            for their advice and support.</p>
            <p>Searching in local files ("My Library" feature) was inspired by Yehuda Seewald GenizahLocal prototype.</p>
            <h3>Local Index Cache Privacy</h3>
            <p>Your indexed document text is stored in <code>local_index.sqlite3</code> inside your LOCAL index folder.
            The text is compressed with <b>zstd</b> (compression, not encryption).
            This cached data is <b>never uploaded</b> to GenizahSearch servers.
            For at-rest encryption, use OS-level disk encryption (BitLocker / FileVault).</p>

            <h3>Data Source &amp; Acknowledgments</h3>
            <p>This software is built on the transcription dataset produced by the <b>MiDRASH Project</b>.
            I am grateful to the project leaders &ndash; Daniel Stoekl Ben Ezra, Marina Rustow,
            Nachum Dershowitz, Avi Shmidman, and Judith Olszowy-Schlanger &ndash; and to
            Tsafra Siew and Yitzchak Gila from the National Library of Israel.
            Many thanks also to the rest of the project team: Luigi Bambaci, Benjamin Kiessling,
            Hayim Lapin, Nurit Ezer, Elena Lolli, Berat Kurar Barakat, Sharva Gogawale,
            Moshe Lavee, Vered Raziel Kretzmer, and Daria Vasyutinsky Shapira.</p>
            <p>Making such a complex and valuable dataset freely available to the public is a
            significant step for Open Science, and I deeply appreciate their generosity.</p>

            <h3>License</h3>
            <p>The underlying dataset is licensed under the Creative Commons Attribution 4.0
            International (<a href='https://creativecommons.org/licenses/by/4.0/'>CC BY 4.0</a>) license.</p>

            <h3>Citation</h3>
            <p style='background-color: {self._cit_bg}; border: 1px solid {self._cit_border};
               border-radius: 4px; padding: 8px; font-size: 11px;'>
            If you use these results in your research, please cite:<br>
            <b>{self.FULL_CITATION}</b></p>
        </div>
        """
        about_txt = tr("ABOUT_HTML") if CURRENT_LANG == 'he' else about_html_en
        browser = QTextBrowser()
        browser.setHtml(about_txt)
        browser.setOpenExternalLinks(True)
        browser.setStyleSheet("border: none; background: transparent;")
        layout.addWidget(browser)

        # PRIV-05: telemetry disclosure block below the About QTextBrowser.
        # Single-language per UI (CURRENT_LANG) — only the one-time startup consent
        # dialog is bilingual; Settings/About follow the interface language.
        _tel_style = """
        <style>
            h3 { margin-bottom: 2px; margin-top: 8px; font-size: 12px; }
            p { margin-top: 3px; margin-bottom: 3px; line-height: 1.4; font-size: 11px; }
            ul { margin: 3px 0; padding-left: 18px; font-size: 11px; }
        </style>
        """
        if CURRENT_LANG == 'he':
            telemetry_disclosure_html = _tel_style + f"""
        <div dir='rtl' style='font-family: Arial; font-size: 11px; color: {self._text};'>
            <hr style='border:0; border-top:1px solid {self._border}; margin: 4px 0 8px 0;'>
            <h3 style='font-size:12px;'>טלמטריית שימוש</h3>
            <p>האפליקציה שולחת באופן אופציונלי <b>נתוני שימוש</b> (תוך שמירה על הפרטיות) לשיפורה.
            טלמטריה היא <b>הצטרפות בלבד</b> — שום דבר לא נשלח אלא אם כן תפעילו זאת
            בהגדרות &larr; כללי &larr; העדפות.</p>
            <p><b>מה נשלח:</b> ספירות תכונות (ללא תוכן), גרסת האפליקציה ומערכת ההפעלה,
            קטגוריות זמן תגובה, סימני קריסה (סוג השגיאה בלבד).</p>
            <p><b>מה לעולם לא נשלח:</b> שאילתות החיפוש שלכם, נתיבי קבצים ושמות קבצים
            מ&#x2018;הספרייה שלי&#x2019;, שמכם או כתובת הדוא&#x05F4;ל. כשאתם מחוברים,
            המזהה היחיד הוא ה-<code>user.id</code> הפסאודו-אנונימי של Supabase — כמו באתר.</p>
            <p>הנתונים מעובדים על ידי
            <a href='https://posthog.com/privacy'>PostHog</a> (אזור האיחוד האירופי) ודיקטה.</p>
        </div>
        """
        else:
            telemetry_disclosure_html = _tel_style + f"""
        <div dir='ltr' style='font-family: Arial; font-size: 11px; color: {self._text};'>
            <hr style='border:0; border-top:1px solid {self._border}; margin: 4px 0 8px 0;'>
            <h3 style='font-size:12px;'>Usage Telemetry</h3>
            <p>This app optionally sends <b>privacy-preserving usage data</b> to help improve it.
            Telemetry is <b>opt-in only</b> — nothing is sent unless you enable it in Settings
            &rarr; General &rarr; Preferences.</p>
            <p><b>What IS sent:</b> feature counts (no content), app &amp; OS version,
            performance timing buckets, crash signals (exception type only).</p>
            <p><b>What is NEVER sent:</b> your search queries, My Library file paths or
            filenames, your name or email. When signed in, the only identity attached is your
            bare Supabase <code>user.id</code> — a pseudonymous identifier, the same one
            the website already uses.</p>
            <p>Data is processed by
            <a href='https://posthog.com/privacy'>PostHog</a> (EU region) and Dicta.</p>
        </div>
        """
        tel_browser = QTextBrowser()
        tel_browser.setHtml(telemetry_disclosure_html)
        tel_browser.setOpenExternalLinks(True)
        tel_browser.setStyleSheet("border: none; background: transparent;")
        tel_browser.setMaximumHeight(200)
        layout.addWidget(tel_browser)

        # "Privacy details" button to open the canonical bilingual PrivacyDialog (PRIV-05)
        from desktop.consent_dialog import PrivacyDialog  # noqa: PLC0415
        btn_privacy_about = QPushButton("פרטיות" if CURRENT_LANG == 'he' else "Privacy details")
        btn_privacy_about.setFlat(True)
        btn_privacy_about.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_privacy_about.setStyleSheet("color: #2563eb; text-decoration: underline; border: none;")
        btn_privacy_about.clicked.connect(lambda: PrivacyDialog(self).exec())
        privacy_row = QHBoxLayout()
        privacy_row.addWidget(btn_privacy_about)
        privacy_row.addStretch()
        layout.addLayout(privacy_row)

        return page

    # ── Helpers ───────────────────────────────────────────────────
    def _section_label(self, text):
        lbl = QLabel(text)
        lbl.setStyleSheet(f"font-weight: bold; font-size: 13px; color: {self._text};")
        return lbl
