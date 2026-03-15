---
phase: quick
plan: 21
type: execute
wave: 1
depends_on: []
files_modified: [genizah_app.py]
autonomous: true
requirements: [QUICK-21]
must_haves:
  truths:
    - "Action row buttons show icon + short text instead of long text"
    - "Compact bar buttons match action row icon+text format"
    - "Image toolbar Reset/External/Ktiv buttons show icon + short text"
    - "Community row View Corrections button shows icon + short text"
    - "All shortened buttons have tooltips with full original text"
    - "Dynamic setText calls (bib counts, translation toggle, catalog count) use the same icon+short format"
  artifacts:
    - path: "genizah_app.py"
      provides: "All ResultDialog button label updates"
  key_links:
    - from: "button creation (lines 2703-2763, 2570-2656)"
      to: "dynamic setText calls (lines 4599, 4831-4858)"
      via: "must use same icon prefix in both places"
      pattern: "setText.*tr\\("
---

<objective>
Convert ResultDialog action row, compact bar, community row, and image toolbar buttons from long text-only labels to icon+short text format (Option B compact). This makes the button rows more compact and visually scannable.

Purpose: Reduce horizontal overflow in ResultDialog button rows, improve visual scanning
Output: Updated genizah_app.py with icon+short text buttons and tooltips
</objective>

<execution_context>
@C:/Users/gersh/.claude/get-shit-done/workflows/execute-plan.md
@C:/Users/gersh/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@genizah_app.py (lines 1563-1615 image toolbar, 2570-2656 compact bar, 2703-2763 action row, 2767-2843 community row, 4593-4607 translation toggle sync, 4825-4864 bib/catalog dynamic setText, 5132-5134 _format_add_to_list_label)
</context>

<tasks>

<task type="auto">
  <name>Task 1: Convert action row, compact bar, and community row buttons to icon+short text</name>
  <files>genizah_app.py</files>
  <action>
Update button labels and add tooltips in the ResultDialog class. Changes grouped by area:

**Action Row (lines ~2703-2763):**
- Line 2704: `QPushButton(tr("Browse manuscript"))` -> `QPushButton(f"📖 {tr('Browse')}")` and add `.setToolTip(tr("Browse manuscript"))`
- Line 2706: `QPushButton(tr("Search for parallels"))` -> `QPushButton(f"🔍 {tr('Parallels')}")` and add `.setToolTip(tr("Search for parallels"))`
- Line 2713: `QPushButton(tr("Show Extended Info"))` -> `QPushButton(f"ℹ️ {tr('Info')}")` and add `.setToolTip(tr("Show Extended Info"))`
- Line 2734: `QPushButton(f"{tr('Catalog Records')} (0)")` -> `QPushButton(f"📋 {tr('Catalog')} (0)")` and add `.setToolTip(tr("Catalog Records"))`
- Line 2743-2747: Translation button initial text: change `tr('Translations ON')` / `tr('Translations OFF')` to `tr('Trans ON')` / `tr('Trans OFF')`, prefix with "🌐 ", add `.setToolTip(tr("Toggle translations"))`

**Compact Bar (lines ~2570-2656) — mirror action row changes:**
- Line 2610: `QPushButton(tr("Show Extended Info"))` -> `QPushButton(f"ℹ️ {tr('Info')}")` and add `.setToolTip(tr("Show Extended Info"))`
- Line 2647: Translation button: same "🌐 " prefix + shortened text as action row
- Compact bib/catalog buttons get their text dynamically, handled below

**Community Row (lines ~2767-2843):**
- Line 2818: `QPushButton(tr("View Corrections"))` -> `QPushButton(f"📝 {tr('Corrections')}")` and add `.setToolTip(tr("View Corrections"))`

**Dynamic setText calls — CRITICAL, must match init format:**
- Lines 4597-4602 (_rd_toggle_translations): Change `tr('Translations ON')` / `tr('Translations OFF')` to `f"🌐 {tr('Trans ON')}"` / `f"🌐 {tr('Trans OFF')}"`
- Lines 4830-4831 (FJMS bib): Change `f"{tr('Bibliography FJMS')} ({len(fjms_bib)})"` to `f"📚 {tr('Bib FJMS')} ({len(fjms_bib)})"` and add tooltip on the button when first shown: `self.btn_rd_bib_fjms.setToolTip(tr("Bibliography FJMS"))`
- Lines 4834 (compact FJMS bib): Same label format, add tooltip
- Lines 4838-4839 (NLI bib): Change to `f"📚 {tr('Bib Ktiv')} ({len(marc_bib)})"`, add tooltip `tr("Bibliography Ktiv")`
- Lines 4842 (compact NLI bib): Same label format, add tooltip
- Lines 4854, 4858 (catalog records dynamic): Change to `f"📋 {tr('Catalog')} ({catalog_count})"` — matches init format

**_format_add_to_list_label (line 5132-5134):**
This already has star emoji prefix. Shorten the text: change `tr('Add to List')` to `tr('List')`. Add tooltip separately at button creation sites (lines 2710, 2605): `.setToolTip(tr("Add to List..."))`

**Image Toolbar (lines ~1589-1602):**
- Line 1589: `QPushButton(tr("Reset"))` -> `QPushButton(f"↩️ {tr('Reset')}")` (already short, just add icon)
- Line 1594: `QPushButton(tr("External Website"))` -> `QPushButton(f"🔗 {tr('External')}")` and add `.setToolTip(tr("External Website"))`
- Line 1599: `QPushButton(tr("View on Ktiv"))` -> `QPushButton(f"🔗 {tr('Ktiv')}")` and add `.setToolTip(tr("View on Ktiv"))`

IMPORTANT: Do NOT change buttons that are already icon+text (Edit, Save, Submit, Comment, toggle image, zoom +/-, rotation arrows, joins chain icon). Only change the ones listed above.
  </action>
  <verify>
    <automated>python -c "import genizah_app; print('Import OK')" 2>&1 | head -5</automated>
    Also grep to confirm changes:
    - grep for "📖.*Browse" in genizah_app.py (action row browse button)
    - grep for "🔍.*Parallels" (parallels button)
    - grep for "📋.*Catalog" (catalog button)
    - grep for "🌐.*Trans" (translation toggle)
    - grep for "📚.*Bib" (bibliography buttons)
    - grep for "📝.*Corrections" (view corrections)
    - grep for "setToolTip" near each changed button to confirm tooltips added
  </verify>
  <done>
    All ResultDialog buttons in action row, compact bar, community row, and image toolbar show icon+short text format. All dynamic setText calls match. All shortened buttons have setToolTip with full original text. No changes to already-compact buttons (Edit, Save, Submit, Comment, zoom, rotation, joins, toggle image).
  </done>
</task>

</tasks>

<verification>
- Open the desktop app and navigate to any search result -> ResultDialog
- Verify action row buttons show icons: browse (book), parallels (magnifier), list (star), info, bib, catalog, translations
- Toggle compact mode and verify compact bar buttons match
- Check image toolbar for External/Ktiv/Reset icons
- Hover over shortened buttons to confirm tooltips show full text
- Load a result with FJMS bib data to verify dynamic bib button label includes icon
- Toggle translations to verify ON/OFF label updates include icon
</verification>

<success_criteria>
All ResultDialog button rows use icon+short text format. Tooltips present on all shortened buttons. Dynamic label updates (bib counts, translation toggle, catalog count, list star) all include icon prefix consistently. App imports and runs without errors.
</success_criteria>

<output>
After completion, create `.planning/quick/21-desktop-resultdialog-convert-buttons-to-/21-SUMMARY.md`
</output>
