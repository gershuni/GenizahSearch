---
phase: 107
slug: desktop-join-workbench-anchor-entry-points-actions-join-model
status: draft
design_system: PyQt6 (Qt Widgets + QSS)
shadcn_initialized: false
preset: not applicable — PyQt6 desktop
created: 2026-06-04
---

# Phase 107 — UI Design Contract

> Visual and interaction contract for the Desktop Join Workbench SHELL.
> Produced by gsd-ui-researcher; verified by gsd-ui-checker.

**Platform:** PyQt6 desktop (Windows). NOT a web/React/shadcn application.
**Scope guard:** this contract covers ONLY Phase 107 — the Workbench window, three entry points,
anchor pane, known-joins group panel, anchor action-row, and per-join-row actions. Query builders,
candidate grid/table, Compare dialog, VS source, and parallels (Phases 108–110) are explicitly OUT.

---

## Design System

| Property | Value |
|----------|-------|
| Tool | not applicable — PyQt6 desktop; no component registry |
| Preset | not applicable — PyQt6 desktop |
| Component library | PyQt6 Qt Widgets (`QDialog`, `QSplitter`, `QLabel`, `QPushButton`, `QScrollArea`, `QTextBrowser`, `QHBoxLayout`, `QVBoxLayout`) |
| Icon library | Unicode emoji inline (existing app pattern: 📖 🧩 📋 🔗 ⚓). No external icon library. |
| Font | System default Qt font (Windows: "Segoe UI"); Hebrew text uses system Unicode font (renders RTL automatically via Qt). No custom font loading. |
| Styling | Per-widget `setStyleSheet()` QSS with explicit light/dark variants; dark-mode detected via `palette.color(QPalette.ColorRole.Window).lightness() < 128` (existing app pattern). |
| Registry Safety | not applicable — no component registry; reuse only existing desktop widgets. |

---

## Spacing Scale

Qt layout margins and spacing in px. Matches the app's existing dense/compact idiom (no web-style airy 8-point scale).

| Token | Value | Qt call | Usage |
|-------|-------|---------|-------|
| xs | 2px | `setSpacing(2)` | Inner row spacing (builder rows, action button rows, known-join row gaps) |
| sm | 4px | `setContentsMargins(4,4,4,4)` / `setSpacing(4)` | Card contents, header lines, chip bar (existing `desktop/widgets/__init__.py` pattern: `setContentsMargins(2,2,2,2)` / `setSpacing(4)`) |
| md | 6–8px | `setSpacing(6)`, `setContentsMargins(8,8,8,8)` | Panel-level layout spacing (matches `desktop/my_library_tab.py:1229`: margins 8, spacing 6; `desktop/result_dialog.py:121`: spacing 6) |
| lg | 10px | `setContentsMargins(10,10,10,10)` | Dialog-level outer margins (existing dialogs: `desktop/dialogs_scholarly.py:28`) |
| xl | 18px | `setContentsMargins(18,0,0,0)` | Nested sub-section indent (other-side builder indent; matches sketch line 811) |

**Exceptions:**
- Anchor tag strip (ANCHOR / teal label): `setContentsMargins(0,0,0,0)`, no extra margin — the tag is full-width.
- Toolbar row (zoom + folio nav): `setSpacing(4)` with `addStretch()` between zoom and folio groups.
- Known-joins panel is HIDDEN ENTIRELY when empty — no spacer or placeholder is added; the panel widget's `setVisible(False)` collapses its space.
- Modeless window minimum size: 900×680px; initial resize: `resize(1000, 720)`.

---

## Typography

QSS font sizes in px (same units as the validated sketch). Exactly 4 roles, 2 weights.

| Role | QSS | Weight | Usage |
|------|-----|--------|-------|
| Heading / shelfmark | `font-size:15px; font-weight:bold;` | Bold (700) | Anchor shelfmark label (`anchor_shelf`) |
| Body / label | `font-size:12px;` | Normal (400) | Known-join row shelfmark + title; candidate table text; general labels |
| Meta / muted | `font-size:11px;` | Normal (400) | Anchor meta-brief line (library · img · title); tag strip labels; status bar; folio counter |
| Small / detail | `font-size:10px;` | Normal (400) | Snippet text inside candidate cards (deferred to 108); known-join row source badge text |

Line height: Qt default (approximately 1.3×). `setWordWrap(True)` on multi-line labels.

---

## Color

Two palettes — light and dark — detected at render time. Use QSS hex values; never hardcode a single value that only works in one mode.

**Pattern:** detect once in `__init__` via `palette.color(QPalette.ColorRole.Window).lightness() < 128`; pass `is_dark: bool` to helper methods that set colors.

### Teal anchor accent (10% — Phase 107 primary accent)

| Element | Light hex | Dark hex | Notes |
|---------|-----------|----------|-------|
| ANCHOR tag label text | `#14b8a6` | `#14b8a6` | Same on both — teal is readable on light AND dark surfaces; validated across 6 UAT iterations (sketch constant `#14b8a6`) |
| "⚓ ANCHOR" tag QSS text color | `#14b8a6` | `#14b8a6` | Bold 11px label strip |
| Known-joins group header label | `#0f766e` | `#14b8a6` | Light: darker teal for contrast; dark: same teal |
| "⚓ make anchor" button (per known-join row) | border-only; no background fill | border-only | Use `border: 1px solid #14b8a6; background: transparent; border-radius: 3px;` — dark-mode safe (sketch iteration B item 3) |

### 60 / 30 / 10 surface split

| Role | Light value | Dark value | Usage |
|------|-------------|------------|-------|
| Dominant (60%) surface | Qt system window color (`QPalette.Window`) | Qt system window color | Window background, anchor pane, known-joins panel background — let Qt manage; do NOT set an explicit background on outer containers |
| Secondary (30%) surface | `#f8fafc` | `#1e293b` | Anchor image loading area; known-join thumbnail placeholder; QScrollArea backgrounds |
| Accent (10%) | `#14b8a6` / `#0f766e` | `#14b8a6` | ANCHOR tag, known-joins header, "⚓ make anchor" border — the ONLY elements that use teal |

### Semantic colors (reserved for specific roles only)

| Role | Light hex | Dark hex | Reserved for |
|------|-----------|----------|-------------|
| Muted / meta text | `#94a3b8` | `#94a3b8` | Anchor meta-brief (library · img · title); status bar; folio counter label (sketch line 751) |
| Destructive / remove | `#dc2626` | `#dc2626` | "Remove" / destructive confirmation only (not used in Phase 107) |
| Loading placeholder bg | `#e2e8f0` | `#374151` | Anchor image QLabel background while loading (sketch line 774; dark variant via `is_dark`) |
| Loading placeholder text | `#64748b` | `#9ca3af` | "…" / "(no image)" text in anchor image QLabel |
| Known-join source badge (PGP) | `#0ea5e9` sky-blue | `#38bdf8` | Source badge background tint for PGP joins |
| Known-join source badge (FJMS) | `#8b5cf6` violet | `#a78bfa` | Source badge for FJMS scholarly joins (reuses `META_COLOR = "#8b5cf6"` from sketch) |
| Known-join source badge (user/community) | `#10b981` green | `#34d399` | Source badge for user / community puzzle joins |
| Known-join source badge (generic fallback) | `#6b7280` gray | `#9ca3af` | When per-edge provenance is not available (D-09 degradation) |

**Accent reserved for:** ANCHOR tag text, known-joins group header text, "⚓ make anchor" button border. Nothing else uses teal `#14b8a6` in Phase 107.

---

## Component Inventory

All reused existing widgets — no new custom widget classes introduced in Phase 107 beyond the `JoinWorkbenchWindow` host.

| Component | Type | Source |
|-----------|------|--------|
| `JoinWorkbenchWindow` | new `QDialog` (modeless, `setModal(False)`) | Phase 107 — the shell |
| Anchor pane | `QWidget` with `QVBoxLayout` | From sketch `_build_anchor_pane()` — transplant behavior |
| Anchor image area | `QScrollArea` → `QLabel` | Sketch-proven; `setMinimumSize(360, 280)` |
| Anchor transcription | `QTextBrowser` | Reuse `apply_line_numbered_text` (RTL gutter) |
| Known-joins group panel | `QWidget` with `QVBoxLayout` | NEW in Phase 107 (sketch never built it) — see Known-Joins Panel spec below |
| Anchor action-row | `QHBoxLayout` of `QPushButton` | NEW in Phase 107 — always visible |
| Per-known-join row | `QWidget` with `QHBoxLayout` | NEW in Phase 107 |
| `JoinsDialog` | `corrections_ui.py:3278` | Reused for Add-as-Join (D-14) |
| `ImageLoaderThread` | `desktop/image_loader.py` | Anchor image load (D-05) |
| `apply_line_numbered_text` | `desktop/widgets/line_number_text_edit.py` | Anchor transcription (D-06) |

---

## Layout Contract

### Window structure

```
JoinWorkbenchWindow (QDialog, modeless)
 └─ QHBoxLayout (root)
     └─ QSplitter (Horizontal)
         ├─ Anchor Pane [width ~420px initial]
         └─ Known-Joins + Actions Pane [width ~540px initial; grows with window]
```

Initial splitter sizes: `[420, 540]`. User-resizable. Minimum window: 900×680px.

### Anchor pane (left side of splitter)

Top-to-bottom stacking with `QVBoxLayout`, outer margins `setContentsMargins(8,8,8,8)`, spacing `setSpacing(6)`:

1. **ANCHOR tag strip** — `QLabel`, bold 11px, teal `#14b8a6`, text: `tr("ANCHOR")`. Full-width.
2. **Shelfmark** — `QLabel`, bold 15px, `TextSelectableByMouse`. Displays anchor sys_id's shelfmark.
3. **Meta-brief** — `QLabel`, 11px muted (`#94a3b8`), word-wrap. Shows library · img · title (from `meta_brief()` helper).
4. **Toolbar row** — `QHBoxLayout`, spacing 4:
   - Zoom out `QPushButton("−")`, fixedWidth 30
   - Zoom in `QPushButton("+")`, fixedWidth 30
   - `addStretch()`
   - `◀` prev-folio button
   - folio counter `QLabel` (e.g. "1/4"), 11px muted
   - `▶` next-folio button
5. **Image area** — `QScrollArea` (not resizable, scrollable), stretches vertically (stretch factor 1). Inner `QLabel`: min 360×280, center-aligned, loading BG `#e2e8f0` light / `#374151` dark.
6. **Transcription** — `QTextBrowser`, RTL, via `apply_line_numbered_text`. Stretches vertically (stretch factor 1).

### Right pane (known-joins + actions)

Top-to-bottom stacking with `QVBoxLayout`, `setContentsMargins(8,8,8,8)`, `setSpacing(6)`:

1. **Anchor action-row** — always visible even when joins panel is hidden (D-13, D-11). See Actions Contract.
2. **Known-joins group panel** — visible ONLY when anchor has ≥1 known join. See Known-Joins Panel spec.
3. **[Phase 108 placeholder area]** — empty in Phase 107; the query builder and candidate pane land here in Phase 108.

---

## Known-Joins Panel Contract

**New UI in Phase 107** (the sketch never built this).

### Panel visibility rule

`panel.setVisible(len(connected_fragments) > 0)`

When empty → the entire panel widget is hidden. No empty-state prompt, no "no known joins" message (D-11).

### Panel header

`QLabel`, bold 11px, `color: #0f766e` (light) / `#14b8a6` (dark), text:

```
tr("Known Joins") + " (" + str(count) + ")"
```

Hebrew: `"צירופים ידועים"` — see Copywriting Contract.

### Per-join row layout

`QWidget` with `QHBoxLayout`, spacing 4, `setContentsMargins(4,2,4,2)`.

Left to right (LTR layout order; Hebrew text inside labels renders RTL naturally via Qt):

1. **Thumbnail** — `QLabel`, fixed 48×48px, background `#e2e8f0` / `#374151`, loaded via `meta_mgr.get_thumbnail(sys_id)` in a batched worker (D-10). "(no img)" text fallback.
2. **Source badge** — `QLabel`, 10px, border-radius 3px, colored per source (see Color section). Text: `"PGP"` / `"FJMS"` / `tr("User")` / `tr("Community")` / `tr("Known join")` (generic fallback per D-09). Fixed width ~60px.
3. **Shelfmark + title** — `QVBoxLayout` (stretch 1):
   - Shelfmark: `QLabel`, 12px, normal weight, `TextSelectableByMouse`
   - Title: `QLabel`, 10px, muted color, word-wrap, truncated at 60 chars + "…"
4. **Action buttons row** — `QHBoxLayout`, spacing 2, no stretch:
   - Browse: `QPushButton("📖")`, fixedWidth 28, tooltip `tr("Browse manuscript")`
   - Puzzle: `QPushButton("🧩")`, fixedWidth 28, tooltip `tr("Add to Puzzle")`
   - Add to List: `QPushButton("📋")`, fixedWidth 28, tooltip `tr("Add to List")`
   - Re-anchor: `QPushButton("⚓")`, fixedWidth 28, tooltip `tr("Make anchor")`, QSS: `border: 1px solid #14b8a6; background: transparent; border-radius: 3px;` — dark-mode safe

### Row rendering

`QScrollArea` wrapping a `QVBoxLayout` of per-join rows. Maximum visible height before scroll: 320px. `setWidgetResizable(True)`.

Rows are rendered after `JoinsManager.get_connected_fragments_by_id(sys_id)` returns. Thumbnail fetches are batched: collect all sys_ids → fire a single `ThumbBatchWorker(QThread)` that iterates the list and emits per-item signals (mirrors `ThumbResolver` in the sketch; batch constraint from D-10 / D-18).

---

## Anchor Action-Row Contract

Always visible, positioned at the top of the right pane (above the known-joins panel). `QHBoxLayout`, spacing 4.

| Button | Label | Tooltip (EN) | Tooltip (HE) | Callback |
|--------|-------|-------------|-------------|----------|
| Browse anchor | `"📖"` | `tr("Browse manuscript")` | `"עיין בכתב היד"` | `app.open_result_in_browse_from_table(anchor_result)` |
| Open in Puzzle | `"🧩"` | `tr("Add to Puzzle")` | `"הוסף לפאזל"` | `app.open_anchor_in_puzzle(anchor_sid)` (public wrapper for `_vs_add_to_puzzle`) |
| Add to List | `"📋"` | `tr("Add to List")` | `"הוסף לרשימה"` | `app.show_add_to_list_menu([{sys_id, fl_id, img}], source="join_workbench", anchor_widget=btn)` |
| Add as Join | `"🔗"` | `tr("Add as Join")` | `"הוסף כצירוף"` | opens `JoinsDialog` pre-filled anchor=A (D-14) |

`addStretch()` after the four buttons pushes them to the left.

All buttons: `fixedWidth=28`, no background fill (inherit Qt theme default), `setToolTip(tr(...))`.

---

## Entry Points Contract

### 1. ResultDialog "Find joins" button

**Where:** ResultDialog action row, inside the `actions_widget.add_btn(...)` block.
**Button spec:** `self._create_action_button("🔗", tr("Find joins"), callback)` — matches existing pattern at `genizah_app.py:17063`.
**Behavior:** opens / re-anchors the workbench with the live page state (`current_sys_id`, `p_num`, `page_text`, `uid`). ResultDialog closes after launch (`self.close()`).

### 2. Browse tab "Find joins" button

**Where:** Browse tab's `ext_info_row` panel.
**Button spec:** same `_create_action_button` pattern; label `tr("Find joins")`.
**Behavior:** opens / re-anchors using `current_browse_sid`, `p` + browse original text. ResultDialog does NOT close (Browse is a persistent tab).

### 3. Cold start by shelfmark

**Where:** inside the workbench window — a `QLineEdit` (placeholder text `tr("Enter shelfmark…")`) + `QPushButton(tr("Open"))`.
**Behavior:** calls `meta.resolve_system_by_shelfmark(q)`. If result has `options` (ambiguous), open a `QInputDialog.getItem` picker showing `[shelfmark — title]` strings. On selection, call `get_browse_page(sid, 1)` to build the anchor result dict. No-match → inline `QMessageBox.warning`.

---

## i18n Contract (D-16, SC#6)

All new strings MUST be wrapped in `tr()`. The Workbench renders fully in Hebrew under `lang=he` with NO hardcoded English — this is an acceptance criterion (SC#6), not a cleanup item.

### New strings to add to `genizah_translations.py` TRANSLATIONS dict

| English (key) | Hebrew (value) |
|---------------|----------------|
| `"Find joins"` | `"מצא צירופים"` |
| `"Join Workbench"` | `"מעבדת צירופים"` |
| `"ANCHOR"` | `"עוגן"` |
| `"Known Joins"` | `"צירופים ידועים"` |
| `"Make anchor"` | `"הגדר כעוגן"` |
| `"Add as Join"` | already present: `"הוסף כצירוף"` |
| `"Browse manuscript"` | already present: `"עיין בכתב היד"` |
| `"Add to Puzzle"` | already present: `"הוסף לפאזל"` |
| `"Add to List"` | already present: `"הוסף לרשימה"` |
| `"Enter shelfmark…"` | `"הזן סימת מדף…"` |
| `"Open"` | `"פתח"` (existing key likely present; verify) |
| `"User"` (badge) | `"משתמש"` |
| `"Community"` (badge) | `"קהילה"` |
| `"Known join"` (generic badge) | `"צירוף ידוע"` |
| `"(no image)"` | `"(אין תמונה)"` |
| `"img"` (in meta-brief) | `"תמונה"` |
| `"◀ img"` folio prev button | `"תמונה ▶"` (direction-flipped for RTL UI) |
| `"img ▶"` folio next button | `"◀ תמונה"` |

**RTL note:** folio prev/next button labels flip direction under `lang=he` because the reading direction is mirrored. Implement via `tr("◀ img")` / `tr("img ▶")` with reversed Hebrew values, OR use layout direction (`setLayoutDirection(Qt.LayoutDirection.RightToLeft)` on the toolbar row) — Claude's discretion per CONTEXT.md.

---

## Copywriting Contract

### Primary actions

| Element | EN copy | HE copy |
|---------|---------|---------|
| Window title | `"Join Workbench"` | `"מעבדת צירופים"` |
| Entry button (ResultDialog + Browse) | `"🔗 Find joins"` | `"🔗 מצא צירופים"` |
| Anchor tag | `"ANCHOR"` | `"עוגן"` |
| Known-joins panel header | `"Known Joins (N)"` | `"צירופים ידועים (N)"` |
| Add as Join action | `"🔗 Add as Join"` (tooltip) | `"🔗 הוסף כצירוף"` |
| Make anchor (per-join row) | `"⚓ Make anchor"` (tooltip) | `"⚓ הגדר כעוגן"` |

### Empty states

| State | EN | HE |
|-------|----|----|
| Known-joins panel empty | Panel is HIDDEN — no copy shown (D-11) | same — hide, no copy |
| Anchor image loading | `"…"` (QLabel text while loading) | `"…"` |
| Anchor image failed / no image | `"(no image)"` | `"(אין תמונה)"` |
| No shelfmark match (cold start) | `"No manuscript found for '{q}'"` (QMessageBox.warning) | `"לא נמצא כתב יד עבור '{q}'"` |

### Error states

| Error | EN | HE | Approach |
|-------|----|----|---------|
| Known-joins load failed | `"Could not load joins."` (inline QLabel, muted color) | `"לא ניתן לטעון צירופים."` | Non-blocking; shown in panel header area |
| Image load failed | `"(no image)"` placeholder | `"(אין תמונה)"` | Graceful degradation; no dialog |
| Add-as-Join persistence error | Handled by existing `JoinsDialog` error path | same | Delegated to JoinsDialog |

### Destructive actions

Phase 107 has NO destructive actions. No confirmation dialogs are needed.
(Known-joins display is read-only; "Add as Join" persists new data but does not delete anything;
re-anchor replaces the hunt context but the prior state is not persisted — no undo needed.)

---

## Interaction Contract

### Window lifecycle (D-01, D-02)

- `JoinWorkbenchWindow` is modeless: `setModal(False)`. Opens with `show()` (not `exec()`).
- Single reusable instance: `GenizahGUI` holds one `self._join_workbench` ref. A second "Find joins" call to the same instance calls `set_anchor(new_result)` and brings the window to front via `raise_()` + `activateWindow()`.
- On close: cancel all in-flight `ImageLoaderThread`s, cancel `ThumbBatchWorker`, cancel any `_AnchorLoadWorker` (call `.cancel()` on each; guard with `try/except RuntimeError`).

### Anchor loading sequence

1. Entry point builds an `anchor_result` dict (standard result dict shape).
2. `set_anchor(res)` updates `anchor_shelf`, `anchor_meta`, clears image; fires `_AnchorLoadWorker`.
3. `_AnchorLoadWorker.done` → populates `_anchor_images`, updates folio counter, fires `ImageLoaderThread` for the current page URL.
4. `ImageLoaderThread.image_loaded(QImage)` → scale to anchor pane width × zoom factor, display.
5. Text: `apply_line_numbered_text(anchor_text, htmlify(text, pattern), source_text=text, is_html=True)`.
6. Known-joins: `JoinsManager.get_connected_fragments_by_id(anchor_sid)` is called from a `QThread`; on return, panel visibility is set and rows are rendered.

### Folio navigation

- `◀` / `▶` buttons navigate `_anchor_idx` within `_anchor_images` (same fragment, different images).
- Anchor identity (sys_id) does NOT change. Known-joins panel does NOT reload.
- Folio counter format: `f"{idx+1}/{total}"`, 11px muted.
- Edge case: at first image, `◀` is `setEnabled(False)`; at last image, `▶` is `setEnabled(False)`.

### Zoom

- `+` multiplies zoom by 1.25; `−` divides by 1.25. Min zoom 0.25×, max 4.0×.
- On zoom change, rescale the cached `_anchor_full_pix` QPixmap (do not re-fetch).
- `QScrollArea` handles overflow (scroll bars appear automatically).

### Re-anchor (per known-join row)

- "⚓ make anchor" button is an EXPLICIT action (D-15) — not a single click on the row.
- Tooltip: `tr("Make anchor")` — clarifies this replaces the current hunt context.
- Calls `set_anchor(fragment_result_dict)` on the workbench itself (reuses D-02 machinery).

### Known-joins refresh after Add-as-Join (SC#4)

- `JoinsDialog` closes → workbench calls `_reload_known_joins()`.
- `_reload_known_joins()` runs `JoinsManager.get_connected_fragments_by_id(anchor_sid)` off-thread.
- On return: clear rows, rebuild panel, show/hide panel per new count.

---

## Dark-Mode Contract

The app uses the OS-reported palette. All color choices are BORDER-ONLY or THEME-INHERITED where possible, following sketch iteration B item 3 ("dark-mode safe border-only styling"):

| Rule | Implementation |
|------|---------------|
| Teal accent `#14b8a6` on tag and "⚓" border | Readable on both surfaces — no adaptation needed |
| Image loading area | `is_dark` → `#374151`; else `#e2e8f0` |
| Secondary surface (scroll areas) | `is_dark` → `#1e293b`; else `#f8fafc` |
| Known-joins header | `is_dark` → `#14b8a6`; else `#0f766e` |
| Source badge backgrounds | Use translucent variants or border-only if contrast insufficient in dark (Claude's discretion) |
| Outer container backgrounds | Do NOT set explicit background on `JoinWorkbenchWindow` itself — inherit Qt palette (`QPalette.Window`) |
| Text colors (body, meta) | Do NOT hardcode `#0f172a` (dark text) — inherit Qt palette text color for body; only override muted/accent explicitly |

---

## Registry Safety

Not applicable. This phase introduces no third-party component registries, no npm packages, no shadcn blocks. All components are reused from PyQt6 and the existing codebase. No registry vetting gate needed.

---

## Checker Sign-Off

- [ ] Dimension 1 Copywriting: PASS
- [ ] Dimension 2 Visuals: PASS
- [ ] Dimension 3 Color: PASS
- [x] Dimension 1 Copywriting: PASS (FLAG — non-blocking, see below)
- [x] Dimension 2 Visuals: PASS (FLAG — non-blocking, see below)
- [x] Dimension 3 Color: PASS
- [x] Dimension 4 Typography: PASS
- [x] Dimension 5 Spacing: PASS
- [x] Dimension 6 Registry Safety: not applicable — PyQt6 desktop

**Approval:** approved 2026-06-04 (gsd-ui-checker — 6/6 dimensions, 0 blocks, 2 non-blocking FLAGs)

### Checker Recommendations (non-blocking — fold into planning)

- **REC-1 (Copywriting):** the cold-start **"Open"** button is a bare verb. Use **"Open fragment" / "פתח קטע"** so the label stands alone for keyboard users; and **confirm the `"Open"` tr() key actually exists** in `genizah_translations.py` (the spec marked it "likely present; verify") — add it explicitly if missing.
- **REC-2 (Copywriting):** the **"Could not load joins."** inline error states no recovery path. Add a retry affordance — e.g. `tr("Could not load joins. Click to retry.")` or a small retry icon-button beside the label — consistent with the app's other fetch-failure handling.
- **REC-3 (Visuals/a11y):** the eight emoji icon-only action buttons (📖🧩📋🔗 anchor row + per known-join row, plus ⚓ make-anchor) carry `setToolTip()` only. Add `setAccessibleName(tr(...))` per button at zero layout cost (screen-reader / keyboard discovery) — especially the ⚓ re-anchor glyph, whose role isn't obvious.

---

## Source Traceability

| Decision | Source |
|----------|--------|
| Modeless window host | CONTEXT.md D-01 |
| Single reusable instance | CONTEXT.md D-02 |
| Three entry points | CONTEXT.md D-03 |
| Anchor pane components | CONTEXT.md D-04..D-07; sketch `_build_anchor_pane()` |
| Image route (enrich_metadata → iiif_full) | CONTEXT.md D-05; sketch iteration D |
| apply_line_numbered_text | CONTEXT.md D-06; DESKTOP-INTEGRATION-NOTES |
| Folio nav = viewer only (anchor stays sys_id) | CONTEXT.md D-07 |
| Known-joins source | CONTEXT.md D-08 |
| Per-row badge with provenance degradation | CONTEXT.md D-09 |
| Batch thumbnail fetch | CONTEXT.md D-10; build constraints |
| Panel hidden when empty | CONTEXT.md D-11 |
| Public named action methods | CONTEXT.md D-12 |
| Add-as-Join on anchor row always visible | CONTEXT.md D-13 |
| JoinsDialog reuse for Add-as-Join | CONTEXT.md D-14; R-02 research flag |
| Explicit re-anchor (not single-click) | CONTEXT.md D-15 |
| i18n from line one (SC#6) | CONTEXT.md D-16; REQUIREMENTS build constraints |
| No candidate search in Phase 107 | CONTEXT.md D-17 |
| Teal `#14b8a6` | sketch `_tag()` + `CandidateCard._restyle()` + UAT validated |
| Compact spacing (2–8px) | sketch `QVBoxLayout.setSpacing(2)`, `setContentsMargins(4,4,4,4)`; desktop/result_dialog.py compact idiom |
| 15px bold shelfmark | sketch `anchor_shelf.setStyleSheet("font-weight:bold;font-size:15px;")` |
| 11px muted meta | sketch `anchor_meta.setStyleSheet("font-size:11px;color:#94a3b8;")` |
| Border-only dark-mode styling | sketch iteration B item 3; DESKTOP-INTEGRATION-NOTES |
| Known-joins panel (new UI) | REQUIREMENTS JWB-04; CONTEXT D-08..D-11; NOT in sketch — designed here |
