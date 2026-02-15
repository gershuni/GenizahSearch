---
phase: quick-13
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - genizah_app.py
  - genizah_translations.py
autonomous: true
must_haves:
  truths:
    - "Preview thumbnail (lbl_thumb) no longer appears in ResultDialog header"
    - "Domain info appears inline on the same row as sys_id/fl_id info"
    - "A 'Compact' toggle button exists that collapses full header into a single compact bar"
    - "Compact bar shows: Prev, shelfmark, image nav, Add to list, Extended info, Joins, Show More, Next"
    - "Clicking 'Show More' in compact mode restores full header"
    - "All existing functionality (edit mode, navigation, image viewer) still works in both modes"
  artifacts:
    - path: "genizah_app.py"
      provides: "ResultDialog with removed thumbnail, inlined domains, compact mode toggle"
    - path: "genizah_translations.py"
      provides: "Hebrew translations for new compact mode labels"
  key_links:
    - from: "compact toggle button"
      to: "header rows visibility"
      via: "_toggle_compact_mode method"
      pattern: "def _toggle_compact_mode"
    - from: "lbl_info"
      to: "domain display"
      via: "inline domain text in info_html"
      pattern: "domain.*lbl_info|info_html.*domain"
---

<objective>
Overhaul the ResultDialog header in the desktop app to reduce visual clutter by:
1. Removing the redundant 120x120 preview thumbnail
2. Inlining domain info onto the sys_id/fl_id info row (eliminating a separate row)
3. Adding a "Compact Mode" toggle that collapses the full header into a single essential-controls bar

Purpose: Maximize manuscript text + image viewing area by reducing header chrome.
Output: Modified genizah_app.py with cleaner ResultDialog, updated translations.
</objective>

<execution_context>
@C:/Users/gersh/.claude/get-shit-done/workflows/execute-plan.md
@C:/Users/gersh/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@genizah_app.py (lines 2331-2627: ResultDialog class, init_ui and header layout)
@genizah_app.py (lines 3820-3910: _update_rd_domain_label, info label updates)
@genizah_app.py (lines 4218-4296: fetch_image, _on_thumb_resolved, on_img_loaded, on_img_failed - thumbnail methods)
@genizah_translations.py
</context>

<tasks>

<task type="auto">
  <name>Task 1: Remove thumbnail and inline domain info</name>
  <files>genizah_app.py, genizah_translations.py</files>
  <action>
  **1a. Remove preview thumbnail from ResultDialog header (init_ui):**

  In `init_ui()` around line 2552-2555:
  - Remove the `self.lbl_thumb` creation line (line 2553)
  - Change `header_layout.addLayout(meta_col, 1); header_layout.addWidget(self.lbl_thumb)` (line 2555) to just `header_layout.addLayout(meta_col, 1)`
  - Keep the `lbl_thumb` attribute as a hidden dummy to avoid AttributeError in existing methods, OR update all references. The safest approach: create `self.lbl_thumb` as a hidden QLabel (not added to layout):
    ```python
    self.lbl_thumb = QLabel()  # Kept as no-op for compatibility
    self.lbl_thumb.setVisible(False)
    ```
    This ensures `fetch_image`, `on_img_loaded`, `on_img_failed`, `_on_thumb_resolved` all still run without error (they set pixmap/text on a hidden widget -- harmless).

  **1b. Remove domain_info_row and inline domain into info_row:**

  In `init_ui()` around lines 2402-2410:
  - Remove the `domain_info_row` layout creation entirely (the QHBoxLayout, QLabel("Domain:"), lbl_rd_domains, addStretch)
  - Keep `self.lbl_rd_domains` but add it to `info_row` instead. After `self.lbl_meta_loading` on info_row (line 2400), insert `self.lbl_rd_domains`:
    ```python
    self.lbl_rd_domains = QLabel("")
    self.lbl_rd_domains.setStyleSheet("color: #8e44ad; font-size: 11px;")
    self.lbl_rd_domains.setVisible(False)
    info_row.addWidget(self.lbl_rd_domains)
    ```
  - Remove `self.rd_domain_label_row = domain_info_row` (no longer needed)
  - In `meta_col` assembly (line 2550), remove `meta_col.addLayout(domain_info_row)` from the chain

  **1c. Update `_update_rd_domain_label` (line 3885):**

  The domain label is now on info_row. Update the method so it prepends "Domain: " to the text when visible:
  ```python
  def _update_rd_domain_label(self):
      parent = self.parent()
      if not parent or not hasattr(parent, '_result_domain_map'):
          self.lbl_rd_domains.setVisible(False)
          return
      domain_names = parent._result_domain_map.get(self.current_sys_id, [])
      if domain_names:
          display_names = [parent._domain_display_name(d) for d in domain_names] if hasattr(parent, '_domain_display_name') else domain_names
          self.lbl_rd_domains.setText(" | " + tr("Domain") + ": " + ", ".join(display_names))
          self.lbl_rd_domains.setVisible(True)
      else:
          self.lbl_rd_domains.setVisible(False)
  ```
  The " | " prefix visually separates it from the sys/fl info on the same row.

  **1d. Add Hebrew translations in genizah_translations.py:**

  Add these entries to the TRANSLATIONS dict (in a logical location near other ResultDialog strings):
  ```python
  "Compact": "תצוגה מצומצמת",
  "Show More": "הצג עוד",
  ```
  </action>
  <verify>
  - Launch desktop app: `python genizah_app.py`
  - Search for any term, open a result
  - Confirm: no 120x120 thumbnail on the right side of the header
  - Confirm: domain info appears inline on the same row as "Sys: X | FL: Y" (separated by " | Domain: ...")
  - Confirm: all buttons still work (Ktiv, navigation, edit, joins, etc.)
  - Confirm: no Python errors in console related to lbl_thumb or domain layout
  </verify>
  <done>
  Thumbnail removed from ResultDialog header. Domain info displayed inline with sys/fl metadata on a single row. No separate domain_info_row. All existing functionality intact.
  </done>
</task>

<task type="auto">
  <name>Task 2: Add compact mode toggle with collapsible header</name>
  <files>genizah_app.py</files>
  <action>
  **2a. Create the compact bar widget in init_ui(), inserted AFTER the top_bar and BEFORE the full header_widget:**

  After line 2379 (`main_layout.addLayout(top_bar)`) and the splitter separator, create a compact bar that is initially hidden:

  ```python
  # --- Compact Bar (initially hidden, shown in compact mode) ---
  self.compact_bar = QWidget()
  self.compact_bar.setVisible(False)
  compact_layout = QHBoxLayout(self.compact_bar)
  compact_layout.setContentsMargins(4, 2, 4, 2)
  compact_layout.setSpacing(6)

  # Prev result
  self.btn_compact_prev = QPushButton(tr("Prev Result"))
  self.btn_compact_prev.setFixedWidth(80)
  self.btn_compact_prev.clicked.connect(lambda: self.navigate_results(-1))
  compact_layout.addWidget(self.btn_compact_prev)

  # Shelfmark (compact)
  self.lbl_compact_shelf = QLabel()
  self.lbl_compact_shelf.setFont(QFont("Arial", 13, QFont.Weight.Bold))
  self.lbl_compact_shelf.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
  compact_layout.addWidget(self.lbl_compact_shelf)

  compact_layout.addWidget(QLabel(" | "))

  # Image navigation (compact) - reuses the same spinbox/buttons via forwarding
  compact_layout.addWidget(QLabel(tr("Image:")))
  self.btn_compact_pg_prev = QPushButton("<")
  self.btn_compact_pg_prev.setFixedWidth(25)
  self.btn_compact_pg_prev.clicked.connect(lambda: self.load_page(offset=-1))
  compact_layout.addWidget(self.btn_compact_pg_prev)

  self.lbl_compact_page = QLabel("1 / ?")
  self.lbl_compact_page.setMinimumWidth(50)
  self.lbl_compact_page.setAlignment(Qt.AlignmentFlag.AlignCenter)
  compact_layout.addWidget(self.lbl_compact_page)

  self.btn_compact_pg_next = QPushButton(">")
  self.btn_compact_pg_next.setFixedWidth(25)
  self.btn_compact_pg_next.clicked.connect(lambda: self.load_page(offset=1))
  compact_layout.addWidget(self.btn_compact_pg_next)

  compact_layout.addWidget(QLabel(" | "))

  # Add to List (compact)
  self.btn_compact_add_list = QPushButton(_format_add_to_list_label(False))
  self.btn_compact_add_list.clicked.connect(self.add_current_to_list)
  compact_layout.addWidget(self.btn_compact_add_list)

  # Extended Info (compact)
  self.btn_compact_ext_info = QPushButton(tr("Show Extended Info"))
  self.btn_compact_ext_info.setCheckable(True)
  self.btn_compact_ext_info.setVisible(False)  # shown when extended info available
  self.btn_compact_ext_info.toggled.connect(self.toggle_extended_info)
  compact_layout.addWidget(self.btn_compact_ext_info)

  # Joins (compact)
  self.btn_compact_joins = QToolButton()
  self.btn_compact_joins.setText("Joins")
  self.btn_compact_joins.setToolTip(tr("View joined fragments"))
  self.btn_compact_joins.setPopupMode(QToolButton.ToolButtonPopupMode.MenuButtonPopup)
  self.btn_compact_joins.clicked.connect(self._rd_view_joins)
  # Share the same joins menu
  self.btn_compact_joins.setMenu(self.rd_joins_menu)
  compact_layout.addWidget(self.btn_compact_joins)

  compact_layout.addStretch()

  # Show More button
  self.btn_show_more = QPushButton(tr("Show More") + " ...")
  self.btn_show_more.clicked.connect(lambda: self._toggle_compact_mode(False))
  compact_layout.addWidget(self.btn_show_more)

  # Next result
  self.btn_compact_next = QPushButton(tr("Next Result"))
  self.btn_compact_next.setFixedWidth(80)
  self.btn_compact_next.clicked.connect(lambda: self.navigate_results(1))
  compact_layout.addWidget(self.btn_compact_next)
  ```

  **2b. Add the "Compact" toggle button to the existing top_bar:**

  In the top_bar section (around line 2374-2378), add a compact toggle button between the result count label and the next button:
  ```python
  self.btn_compact_toggle = QPushButton(tr("Compact"))
  self.btn_compact_toggle.setCheckable(True)
  self.btn_compact_toggle.setChecked(False)
  self.btn_compact_toggle.setFixedWidth(70)
  self.btn_compact_toggle.clicked.connect(lambda checked: self._toggle_compact_mode(checked))
  ```
  Insert it into top_bar: `top_bar.addWidget(self.btn_compact_toggle)` right before `top_bar.addWidget(self.btn_res_next)`.

  **2c. Add compact_bar to main_layout:**

  After creating the compact_bar widget, add it to main_layout:
  ```python
  main_layout.addWidget(self.compact_bar)
  ```
  This goes right after the top_bar and before `header_widget`. Store header_widget as `self.header_widget` so it can be toggled:
  ```python
  self.header_widget = header_widget  # add this around line 2556
  ```

  **2d. Implement `_toggle_compact_mode` method in ResultDialog class:**

  ```python
  def _toggle_compact_mode(self, compact):
      """Toggle between compact and full header mode."""
      self.compact_bar.setVisible(compact)
      self.header_widget.setVisible(not compact)
      self.btn_compact_toggle.setChecked(compact)

      if compact:
          # Sync compact bar state from full header
          self.lbl_compact_shelf.setText(self.lbl_shelf.text())
          page_num = self.spin_page.value()
          total_text = self.lbl_total.text()  # "/ N"
          self.lbl_compact_page.setText(f"{page_num} {total_text}")

          # Sync extended info button state
          self.btn_compact_ext_info.setVisible(self.btn_ext_info.isVisible())
          self.btn_compact_ext_info.blockSignals(True)
          self.btn_compact_ext_info.setChecked(self.btn_ext_info.isChecked())
          self.btn_compact_ext_info.blockSignals(False)
          self.btn_compact_ext_info.setText(self.btn_ext_info.text())
  ```

  **2e. Keep compact bar in sync when navigating pages/results:**

  In the `load_page_data` method (around line 3838 where spin_page and lbl_total are updated), add after those updates:
  ```python
  # Sync compact bar page label
  if hasattr(self, 'lbl_compact_page') and self.compact_bar.isVisible():
      self.lbl_compact_page.setText(f"{self.current_p_num} {self.lbl_total.text()}")
  ```

  In the area where `lbl_shelf` is set during result loading (search for `self.lbl_shelf.setText` in the class), add:
  ```python
  if hasattr(self, 'lbl_compact_shelf'):
      self.lbl_compact_shelf.setText(self.lbl_shelf.text())
  ```

  In `toggle_extended_info` (line 3945), sync the compact button:
  ```python
  def toggle_extended_info(self, checked):
      self.extended_info_visible = checked
      self.txt_extended_info.setVisible(checked)
      label = tr("Hide Extended Info") if checked else tr("Show Extended Info")
      self.btn_ext_info.setText(label)
      if hasattr(self, 'btn_compact_ext_info'):
          self.btn_compact_ext_info.blockSignals(True)
          self.btn_compact_ext_info.setChecked(checked)
          self.btn_compact_ext_info.setText(label)
          self.btn_compact_ext_info.blockSignals(False)
  ```

  Also sync btn_ext_info visibility → btn_compact_ext_info visibility. Search for where `self.btn_ext_info.setVisible(True)` is called (in on_enriched_data_loaded or on_metadata_loaded) and add:
  ```python
  if hasattr(self, 'btn_compact_ext_info'):
      self.btn_compact_ext_info.setVisible(True)
  ```

  **2f. Keep "Add to list" label in sync:**

  Search for all places where `self.btn_add_to_list.setText(` is called and add a mirror call for `self.btn_compact_add_list.setText(` with the same value.

  **Important:** The extended info text browser (`txt_extended_info`) must remain visible in compact mode when toggled on -- it is part of `meta_col` which is inside `header_widget`. Since we hide `header_widget`, the extended info browser needs to be moved outside header_widget or handled differently. The cleanest approach: move `self.txt_extended_info` out of `meta_col` and add it directly to `main_layout` (between header_widget and main_splitter). This way it is independent of compact/full toggle. In line 2550, remove `meta_col.addWidget(self.txt_extended_info)`. After line 2556 (`main_layout.addWidget(header_widget)`), add:
  ```python
  main_layout.addWidget(self.txt_extended_info)
  ```
  This ensures extended info is always accessible regardless of compact mode.
  </action>
  <verify>
  - Launch desktop app: `python genizah_app.py`
  - Search and open a result
  - Confirm: "Compact" button visible in the top navigation bar
  - Click "Compact" -- verify full header collapses, compact bar appears with: Prev, shelfmark, image nav, Add to list, Extended Info (if available), Joins, Show More, Next
  - Click page navigation in compact mode -- verify page label updates
  - Click "Show More" -- verify full header restores
  - Navigate to next/prev result in compact mode -- verify shelfmark and page info update
  - Toggle extended info in compact mode -- verify the info panel appears below
  - Switch between compact and full mode multiple times -- verify no visual glitches or errors
  - Test Add to List in compact mode -- verify it works
  - Test Joins dropdown in compact mode -- verify it works
  </verify>
  <done>
  Compact mode toggle fully functional. "Compact" button in top bar collapses full header to a single row with essential controls. "Show More" restores full view. All controls in compact bar are wired to the same underlying methods. State syncs correctly when navigating results/pages.
  </done>
</task>

</tasks>

<verification>
- Desktop app launches without errors
- ResultDialog opens with no thumbnail on the right
- Domain info appears inline with sys_id/fl_id metadata
- Compact toggle works: collapses header, shows compact bar
- All navigation (prev/next result, prev/next page) works in both modes
- Edit mode, version selector, comments, joins all accessible from full mode
- Add to list, extended info, joins accessible from compact mode
- No console errors or widget crashes
</verification>

<success_criteria>
1. Preview thumbnail removed from ResultDialog
2. Domain info inlined into info row (one fewer header row)
3. Compact mode toggle collapses full header into single essential-controls bar
4. All existing functionality preserved in full mode
5. Essential controls (navigation, add to list, extended info, joins) available in compact mode
</success_criteria>

<output>
After completion, create `.planning/quick/13-resultdialog-compact-mode-remove-preview/13-SUMMARY.md`
</output>
