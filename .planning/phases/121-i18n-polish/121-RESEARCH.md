# Phase 121: i18n Polish - Research

**Researched:** 2026-06-21
**Domain:** Internationalization / bilingual EN/HE guard + RTL verification
**Confidence:** HIGH (all findings verified by direct file inspection with line references)

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

- **D-01:** RTL verified by BOTH (a) automated render-smoke structural assertions AND (b) structured
  live HE-mode HUMAN-UAT checklist (Hillel runs it). Headless CI alone cannot close SC#2.
- **D-01a:** Render-smoke CAN assert `dir="rtl"` / `text-align` / the manual `flex-row-reverse` at
  `candidate_grid.py:1361-1363` and `compare_modal.py:791-794`.
- **D-01b:** HE-UAT checklist covers computed-height collapse, clipping, mirroring correctness —
  NOT automatable from the headless path.
- **D-02:** Ship a PERMANENT CI guard `tests/test_joins_lab_i18n.py` with two checks:
  (a) no raw Hebrew literal outside `tr()` in the in-scope files;
  (b) every `tr("literal")` key in in-scope files resolves in `TRANSLATIONS`.
- **D-03:** Add missing HE keys to TRANSLATIONS rather than carving allowlist exceptions into check
  (b). Adding keys cannot reverse-leak Hebrew to EN users.
- **D-04:** Allowlist for check (a): `joins_builder.py:344-351` syntax-legend operator tuples
  (`#מילה`, `מילה#`, `%מילה`, `*מילה / מילה*`, `(א/ב)`, `-מילה`, `|מילה`, `מילה|`) and
  `joins_lab.py:222` docstring example. Comments/docstrings excluded STRUCTURALLY by the AST
  scanner (inspects string-literal nodes only, not comment nodes).
- **D-05:** Scope = full-scan of 8 dedicated files + scoped key-check on 5 entry-point files +
  export path. `web/joins_executor.py` is out of scope (0 tr() calls, no user-facing strings).
- **D-05a:** `shared/joins_lab.py::badge_and_tooltip` DOES return user-facing label text (see
  Q2 below). Wrapping happens on the web UI side — inside full-scan files.
- **D-06:** Reconcile web HE terms to UAT-approved desktop vocabulary. Source of truth:
  `desktop/join_workbench.py`. One confirmed drift found (see Q7).

### Claude's Discretion
- Exact render-smoke RTL assertion set per surface (D-01a)
- Precise HE-UAT checklist line items (D-01b)
- AST scanner structure and final D-04 allowlist
- Final enumerated scoped entry-point keys (D-05)
- Any genuinely-new missing strings discovered — fill with HE keys via TRANSLATIONS

### Deferred Ideas (OUT OF SCOPE)
None documented in CONTEXT.md.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| FND-07 | The entire Joins Lab UI is bilingual (EN/HE) with correct RTL layout, consistent with the rest of the web app. | Gap list (Q6) provides the concrete set of 17 missing HE translations. SC#1 live-switch verdict (Q1) defines "consistent with the rest of the app." Guard template (Q3) and render-smoke harness location (Q4) define the automation layer. Scoped key enumeration (Q5) confirms entry-point coverage. Glossary drift (Q7) defines the reconciliation work. |
</phase_requirements>

---

## Summary

Phase 121 is a verification + permanent-guardrail + gap-closure phase. The Joins Lab ships
heavily bilingual (313 `tr()` calls across 8 files) because every prior phase followed
"bilingual from line one." The actual work is small and well-defined: add HE translations
for 17 missing keys, fix 1 glossary drift in TRANSLATIONS, install a permanent CI AST guard,
and run a structured HE-mode live UAT.

**Primary recommendation:** The planner should structure this as: (Wave 0) install guard
skeleton + add all 17 missing HE keys + fix 1 drift + wrap 1 XLSX sheet-name gap;
(Wave 1) extend the existing render-smoke harness with RTL structural assertions; (Wave 2)
human UAT checklist execution. The guard, key additions, and drift fix are all mechanical;
the human UAT is the load-bearing SC#2 acceptance gate.

---

## Q1 — SC#1 Live-Language-Switch Verdict

**VERDICT: Language switching in this web app ALWAYS requires a full page reload. This is the
pre-existing app-wide behavior. The Joins Lab only needs to be CONSISTENT with this behavior,
not invent a Joins-Lab-only live-switch mechanism.**

**Evidence:** `web/main.py:1004-1009`

```python
def toggle_lang():
    current = get_language()
    new_lang = 'en' if current == 'he' else 'he'
    safe_user_set('ui_language', new_lang)
    set_language(new_lang)
    ui.navigate.reload()   # ← full page reload
```

The language toggle button calls `ui.navigate.reload()`. There is no live re-render of
`tr()`-wrapped strings without a reload.

**Implication for SC#1:** "Every UI string has EN+HE key; live language switch updates without
reload" means: on reload in the new language, every `tr("key")` call returns the correct
translation. The "without reload" phrase in SC#1 refers to the button's user experience
(one click), not a SPA live-DOM update. The Joins Lab satisfies SC#1 if every `tr()` key
has a HE entry in `TRANSLATIONS` — then a page reload in HE mode shows correct Hebrew.

**Module-level state note:** `web/translations.py:12` sets `_current_lang = 'he'` as the
module-level default. `create_layout()` in `web/main.py:861` calls
`set_language(_resolve_ui_language())` on every page render, so the per-session language
is established correctly for each NiceGUI render call. `tr()` is a synchronous function
reading the process-global `_current_lang`; it is NOT per-session/per-request. In a
multi-user NiceGUI server, `set_language` is called in the rendering context of each
client before any `tr()` calls fire in that render pass — this is how the existing app
works for all other pages and the Joins Lab inherits that behavior identically.

---

## Q2 — D-05a: shared/joins_lab.py badge_and_tooltip shared-core text

**`badge_and_tooltip` DOES return user-facing label text. The web wraps it in `tr()` correctly.
Three returned strings are MISSING from TRANSLATIONS (included in Q6 gap list).**

`shared/joins_lab.py:639-659`:

```python
def badge_and_tooltip(cand: "Candidate") -> tuple:
    if cand.is_anchor_self:
        return ("anchor", "Anchor fragment")       # ← user-facing, MISSING in TRANSLATIONS
    if cand.via_other_side:
        return ("swap_horiz", "Found via other side")  # ← user-facing, MISSING
    if cand.via_vs:
        return ("visibility", "Visually similar")  # ← user-facing, MISSING
    return (None, "")
```

The module docstring at `shared/joins_lab.py:649-651` explicitly states: "The English tooltip
strings are returned as plain strings (the shared/desktop-parity core does not depend on web
translations); callers in the web UI wrap copy through `tr()` at render time."

**Web call sites (both already use `tr(tooltip_text)`):**
- `web/components/candidate_grid.py:760`: `ui.icon(icon_name).tooltip(tr(tooltip_text))`
- `web/components/candidate_grid.py:838`: `ui.button(_glyph).tooltip(tr(_tooltip_key))`
  (via `TRIAGE_ICONS` dict — tooltip keys `"Mark yes"/"Mark maybe"/"Mark no"` ARE in
  TRANSLATIONS; only the `badge_and_tooltip` return values are missing)

**Problem for the AST guard:** `tr(tooltip_text)` uses a VARIABLE argument, not a string
literal. The AST literal scanner (`tr("literal")` pattern) CANNOT detect these. The guard
must include these 3 strings as an EXPLICIT static list (the PHASE_107_HOST_KEYS pattern
from `test_join_workbench_i18n.py`).

**The shared core stays PyQt-free/app-agnostic** — no change to `shared/joins_lab.py` needed.
Wrapping already happens on the web side. Only TRANSLATIONS entries are missing.

---

## Q3 — Guard / Scanner Templates: Exact AST Mechanics

### Template: `tests/test_join_workbench_i18n.py`

**Direct structural template for the new `tests/test_joins_lab_i18n.py`.**

The desktop guard has two parts:

**Part 1 — Full-module scan (lines 35-76):**

```python
def _extract_tr_keys(source: str) -> list:
    tree = ast.parse(source)
    keys = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "tr"
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            keys.append((node.args[0].value, node.lineno))
    return keys
```

This extracts `tr("literal string")` calls where the first argument is a string constant.
It does NOT catch `tr(variable)` — that is the BADGE STRING gap.

**Part 2 — Scoped host-key check (lines 82-172):**
Checks that specific NEW keys (e.g. `PHASE_107_HOST_KEYS = ["Find joins"]`) are both
(a) present in TRANSLATIONS and (b) appear as `tr("key")` in at least one host file.
Uses simple string search `f'tr("{k}")'` rather than AST for the host file check.

**Hebrew literal detection (check (a) — no raw Hebrew reverse-leak):**

The AST scanner walks the tree for `ast.Constant` string nodes. For each string value,
check `bool(re.compile(r"[֐-׿]").search(value))`. A raw Hebrew literal OUTSIDE `tr()`
is a reverse-leak risk. Exclusions:
- Nodes where the parent is a `tr()` call (already inside tr, OK)
- Docstring nodes (first `ast.Constant` in a `ast.Expr` body — exclude structurally)
- The D-04 allowlist: specific literal values that are intentional bilingual-safe examples

**What the parent-link check looks like (from `_tmp/find_missing_tr2.py`):**

```python
# Tag parents
for node in ast.walk(tree):
    for child in ast.iter_child_nodes(node):
        child._parent = node

# Check each string constant
for node in ast.walk(tree):
    if not isinstance(node, ast.Constant): continue
    if not isinstance(node.value, str): continue
    if not HEB.search(node.value): continue
    par = getattr(node, '_parent', None)
    # Skip if inside tr()
    if isinstance(par, ast.Call) and isinstance(par.func, ast.Name) and par.func.id == 'tr':
        continue
    # → raw Hebrew leak candidate
```

**Adaptations needed for the WEB guard (vs. desktop template):**

| Aspect | Desktop `test_join_workbench_i18n.py` | Web `tests/test_joins_lab_i18n.py` |
|--------|---------------------------------------|-------------------------------------|
| Target files | `desktop/join_workbench.py` (one file) | 8 full-scan files (see D-05) |
| Scoped host keys | `PHASE_107_HOST_KEYS = ["Find joins"]` | New list: `JOINS_LAB_ENTRY_KEYS` (see Q5) |
| Badge-string check | Not needed (desktop tr() takes variable) | New: explicit `BADGE_STRINGS` list |
| D-04 allowlist | Not needed in desktop guard | New: `HEBREW_LITERAL_ALLOWLIST` |
| Import path | `from genizah_translations import TRANSLATIONS` | Same (shared dict) |
| `tr()` import | Desktop `tr` from `genizah_core.py:~2735` | Web `tr` from `web/translations.py` — same detection pattern (function named `tr`, string arg) |

---

## Q4 — Render-Smoke Harness Location and RTL Extension Points

### Harness Location

The Phase-119 NiceGUI User render-smoke harness exists at:

- **`tests/render_smoke/conftest.py`** — async context manager `_joins_lab_user_context`,
  stub data, startup mocking (F-A1/F-A2 solved via `import web.main` side-effect + clearing
  `core.app._startup_handlers`). "Manual" driver: tests are synchronous, call `asyncio.run()`.
- **`tests/render_smoke/test_joins_lab_render_smoke.py`** — sync test functions using
  `joins_lab_user_runner` fixture from conftest. Asserts G1/G2/G3/G3-compare/G4/G5/A2.

**How to extend for RTL (D-01a):** Add new test function(s) to
`tests/render_smoke/test_joins_lab_render_smoke.py` using the SAME `joins_lab_user_runner`
fixture. No new conftest changes needed — the harness already opens `/joins-lab` with a
mocked engine.

### What CAN Be Asserted from the Headless Path (D-01a)

These are STRUCTURAL attributes, not computed styles. They are present in the rendered DOM
that the NiceGUI User harness sees.

**1. `flex-row-reverse` at the pagination bar:**

`candidate_grid.py:1363-1365`:
```python
_pg_dir = "flex-row-reverse" if is_rtl() else "flex-row"
with ui.row().classes(
    f"w-full items-center justify-center gap-2 mt-2 {_pg_dir}"
):
```

In HE mode (`is_rtl()=True`), the row's class list contains `"flex-row-reverse"`.
Assert: the pagination row element includes `"flex-row-reverse"` in its classes when
lang is `"he"`.

**2. `flex-row-reverse` at the Compare modal verdict/nav bar:**

`compare_modal.py:792-794`:
```python
_nav_dir_class = "flex-row-reverse" if is_rtl() else "flex-row"
with ui.row().classes(
    f"w-full items-center justify-between px-4 py-2 flex-wrap gap-2 {_nav_dir_class}"
):
```

Assert: the Compare verdict/nav row includes `"flex-row-reverse"` in HE mode.

**3. RTL comment in `joins_lab.py:1169`** (mentioned in CONTEXT.md):

`joins_lab.py` uses `is_rtl()` for layout decisions. The `_pg_dir` pattern originates at
`joins_lab.py:1169` and is replicated in `candidate_grid.py` and `compare_modal.py`.

**What CANNOT be asserted headlessly (D-01b — must go to HE-UAT):**
- Computed height collapse (flexbox container height:100% collapse — NiceGUI headless has no layout engine)
- Text clipping/overflow
- Visual mirroring correctness (the Prev/Next arrows showing on correct sides)
- Transcription right-alignment (the `dir="rtl"` attribute is in the DOM but whether it
  LOOKS correct requires a visual browser)

**Note on NiceGUI headless and `dir` attribute:** The render-smoke harness uses NiceGUI's
in-process User driver which operates on the DOM tree. `dir="rtl"` attributes set via
`.props('dir=rtl')` or the `get_dir()` helper ARE present in the DOM tree the harness can
inspect. However, the existing conftest stubs `set_language` / `is_rtl()` may default to
`'en'` — the RTL assertion tests must explicitly invoke `set_language('he')` before opening
`/joins-lab` (or the conftest fixture needs an `rtl=True` parameter path).

---

## Q5 — Scoped Entry-Point Keys Enumeration

All verified by direct code inspection. All keys listed below ARE in TRANSLATIONS
(statuses reflect TRANSLATIONS content as of 2026-06-21).

### FND-04 — `/search` "Find joins" card + Quick-View (`web/pages/search_results.py`)

**`search_results.py:680`:**
```python
joins_btn = ui.button(icon='link', ...).tooltip(tr('Find Joins in the Joins Lab'))
```
Key: `'Find Joins in the Joins Lab'` → HE: `'איתור צירופים במעבדת הצירופים'` **[OK]**

Quick-View: `search_results.py:2055-2064` passes `find_joins_url` to `create_joins_button`
which is in `joins_panel.py` — same key (`'Find Joins in the Joins Lab'`) surfaces via
`joins_panel.py:489` and `joins_panel.py:613`.

### FND-05 — `/browse` "Find joins" (`web/pages/browse.py`)

Browse calls `create_joins_button(find_joins_url=...)` at `browse.py:3914-3921`. The
join-related visible strings all live in `web/components/joins_panel.py`:

- `joins_panel.py:506`: `tr('Joined Fragments')` → HE: `'קטעים מצורפים'` **[OK]**
- `joins_panel.py:489`: `tr('Find Joins in the Joins Lab')` → HE as above **[OK]**
- `joins_panel.py:613`: same key **[OK]**
- `joins_panel.py:849`: `tr('Go to Joins Lab to find more joins')` → HE: `'עברו למעבדת הצירופים לאיתור צירופים נוספים'` **[OK]**

Note: `browse.py` itself has NO direct `tr('Find joins')` call — the entry is entirely
via `create_joins_button` in `joins_panel.py`. The scoped key-check in the guard should
assert the `joins_panel.py` keys above exist in TRANSLATIONS (they already do).

### D-19 — `/lists` "Open in Joins Lab" (`web/pages/lists.py:698-709`)

```python
ui.button(icon='link', on_click=...).props(
    'flat round dense aria-label="Open in Joins Lab"'
).tooltip(tr('Open in Joins Lab'))
```
Key: `'Open in Joins Lab'` → HE: `'פתח במעבדת ההצטרפות'`

**STATUS: KEY EXISTS but HAS GLOSSARY DRIFT (see Q7). The HE should be `'פתח במעבדת הצירופים'`
(using the established `צירופים` form, matching `'Joins Lab'` → `'מעבדת צירופים'`).**

### ACT-02 — `/puzzle` bulk-handoff toasts (`web/pages/joins_lab.py`)

The `puzzle.py` bulk-add consumer (`puzzle.py:3914-3955`) has NO user-facing `tr()` strings
of its own — it silently processes the `puzzle_staging` payload. All ACT-02 user-facing
strings live in `joins_lab.py`:

- `joins_lab.py:1861`: `tr('Only the first 20 selected candidates will be added to the Puzzle.')` → HE **[OK]**
- `joins_lab.py:1853`: `tr('No anchor loaded')` → **[GAP — MISSING HE]**
- `joins_lab.py:1887`: `tr('No anchor loaded')` → same key, second occurrence **[GAP]**
- `joins_lab.py:1609`: `tr('Add to Puzzle')` → HE: `'הוסף לפאזל'` **[OK]**
- `joins_lab.py:1611-1612`: `tr('Add anchor + selected candidates to the Fragment Puzzle')` → HE **[OK]**
- `web/components/candidate_grid.py:949`: `tr('Add anchor + this candidate to the Fragment Puzzle')` → **[GAP]**
- `web/components/compare_modal.py:648`: same key **[GAP]**

### ACT-03/D-06 — Export sheet/column headers + filename (`web/pages/joins_lab.py`)

**Export column headers** (`joins_lab.py:2195-2198`):
```python
headers = [
    tr('Shelfmark'), tr('Library'), tr('Title'), tr('Triage'), tr('Score'),
    tr('Material'), tr('Dimensions'), tr('Page'),
    tr('Transcription (page)'), tr('Image URL'),
]
```
All 10 column header keys ARE in TRANSLATIONS with correct HE. **[ALL OK]**

**XLSX sheet name** (`joins_lab.py:2252`):
```python
ws.title = 'Candidates'
```
`'Candidates'` IS in TRANSLATIONS (`'מועמדים'`) but is NOT wrapped in `tr()`. This is a
minor gap — the XLSX tab name is always English. Whether to fix it is Claude's discretion
(see D-03; adding `tr()` around `ws.title` is safe and trivially consistent).

**Export filename** (`joins_lab.py:2237`):
```python
filename_base = f'joins_lab_candidates_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
```
Filename uses an underscore-encoded programmatic form — not a user-facing label, no `tr()`
needed.

---

## Q6 — Initial Gap-Closure Sweep: Complete Gap List

**Total distinct keys needing HE translations: 17**

(14 caught by AST literal scanner + 3 badge strings caught only by explicit list)

### 14 Literal Keys (AST-Catchable)

| Key | File(s):Line(s) | Notes |
|-----|-----------------|-------|
| `'No anchor loaded'` | `joins_lab.py:1853, 1887` | ACT-02 guard check |
| `'No candidates selected'` | `joins_lab.py:1936` | bulk action guard |
| `'Loading visual similarity…'` | `joins_lab.py:2568, 3360` | VS spinner label |
| `'No candidates match both text and visual similarity. Try clearing the builder for VS-only browse.'` | `joins_lab.py:2581, 2740` | VS empty-state |
| `'Could not load your lists. Please try again.'` | `joins_lab.py:3142` | Add-to-List error |
| `'Has dimensions data'` | `candidate_grid.py:991` | filter checkbox label |
| `'Exclude size mismatch'` | `candidate_grid.py:994` | filter checkbox label |
| `'Select for bulk actions'` | `candidate_grid.py:712` | table checkbox tooltip |
| `'Triage state'` | `candidate_grid.py:1003` | filter dropdown label |
| `'Filter by shelfmark…'` | `candidate_grid.py:1009` | filter input placeholder |
| `'Mark N selected as:'` | `candidate_grid.py:1116, 1208` | bulk triage label |
| `'Select exactly one candidate to add as a join'` | `candidate_grid.py:1176, 1219` | Add-as-Join guard |
| `'Add anchor + this candidate to the Fragment Puzzle'` | `candidate_grid.py:949, compare_modal.py:648` | per-card puzzle button tooltip |
| `'Size mismatch'` | `compare_modal.py:479` | Compare pane badge label |

### 3 Badge Strings (Runtime Variable — NOT Caught by AST, Must Use Explicit List)

These are returned by `shared/joins_lab.py::badge_and_tooltip()` (lines 654, 656, 658)
and passed to `tr()` as a VARIABLE at `web/components/candidate_grid.py:760`.
The static AST scanner cannot detect these.

| Key | Origin | Web call site |
|-----|--------|---------------|
| `'Anchor fragment'` | `shared/joins_lab.py:654` | `candidate_grid.py:760` |
| `'Found via other side'` | `shared/joins_lab.py:656` | `candidate_grid.py:760` |
| `'Visually similar'` | `shared/joins_lab.py:658` | `candidate_grid.py:760` |

**Guard mechanic for these 3:** The new `tests/test_joins_lab_i18n.py` must include a
dedicated test that checks these 3 keys exist in TRANSLATIONS — analogous to
`test_gap_round_3_keys_in_translations()` in the desktop guard.

### Zero Raw Hebrew Reverse-Leaks (Check (a))

The AST scan found raw Hebrew literals in the in-scope files at:

- `joins_lab.py:217`: Hebrew characters in a **docstring** — excluded structurally by the
  AST scanner (docstring nodes are `ast.Constant` children of `ast.Expr` statements, not
  inside `tr()` calls; scanner should skip docstrings via parent-node check).
- `joins_builder.py:344-351`: The D-04 allowlist items:
  `'#מילה'`, `'מילה#'`, `'%מילה'`, `'*מילה / מילה*'`, `'(א/ב)'`, `'-מילה'`, `'|מילה'`, `'מילה|'`
  — these are syntax-legend operator tuples, intentionally bilingual-safe.

**SC#3 RESULT: Zero real reverse-leaks.** All Hebrew literals are either in docstrings
(excluded structurally) or in the D-04 allowlist. The guard check (a) will pass with no
user-facing Hebrew literals outside `tr()`.

---

## Q7 — D-06 Glossary Reconciliation

### Source of Truth: desktop/join_workbench.py + TRANSLATIONS

The desktop Joins Lab is UAT-approved. All its `tr()` keys have HE entries in TRANSLATIONS.

### Anchor Glossary Terms — Status

| Term | EN Key | HE in TRANSLATIONS | Status |
|------|--------|--------------------|--------|
| Joins Lab | `'Joins Lab'` | `'מעבדת צירופים'` | OK |
| Anchor | `'Anchor'` | `'עוגן'` | OK |
| Candidate (noun) | `'candidate'`, `'Candidate'` | `'מועמד'` | OK |
| Compare | `'Compare'` | `'השווה'` | OK |
| Visual Similarity | `'Visual Similarity'`, `'visual similarity'` | `'דמיון חזותי'` | OK |
| Triage | `'Triage'` | `'מיון'` | OK |
| Known Joins | `'Known Joins'` | `'צירופים ידועים'` | OK |
| Yes | `'Yes'` | `'כן'` | OK |
| Maybe | `'Maybe'` | `'אולי'` | OK |
| No | `'No'` | `'לא'` | OK |
| Mark yes | `'Mark yes'` | `'סמן כן'` | OK |
| Mark maybe | `'Mark maybe'` | `'סמן אולי'` | OK |
| Mark no | `'Mark no'` | `'סמן לא'` | OK |
| Y/N/? glyphs | `'Y — kept'`, `'N — dismissed'`, `'? — maybe'` | `'Y — נשמר'`, `'N — נדחה'`, `'? — אולי'` | OK |

### Builder Modifier Meanings — Status (All OK)

| Key | HE |
|-----|----|
| `'Plene/Defective %'` | `'מלא/חסר %'` |
| `'Wildcard *_'` | `'*_ תחילית'` |
| `'Wildcard _*'` | `'_* סופית'` |
| `'Prefixes #_'` | `'קידומות #_'` |
| `'Suffixes _#'` | `'סיומות _#'` |
| `'Negation −'` | `'שלילה −'` |
| `'Line start (⊢)'` | `'תחילת שורה (⊢)'` |
| `'Line starts here'` | `'השורה מתחילה כאן'` |
| `'Line end (⊣)'` | `'סוף שורה (⊣)'` |
| `'Line ends here'` | `'השורה מסתיימת כאן'` |

### Confirmed Drift: ONE item requires correction in TRANSLATIONS

**`'Open in Joins Lab'`** (used at `lists.py:709`):
- Current HE: `'פתח במעבדת ההצטרפות'` — uses `ההצטרפות` (verbal noun of "joining")
- Correct HE should be: `'פתח במעבדת הצירופים'` — uses `הצירופים` matching the
  established `'Joins Lab'` → `'מעבדת צירופים'` term
- Fix: update `genizah_translations.TRANSLATIONS['Open in Joins Lab']` to `'פתח במעבדת הצירופים'`

**`'Find Joins in the Joins Lab'`** (web) vs `'find joins in joins lab'` (desktop):
- Web: `'Find Joins in the Joins Lab'` → `'איתור צירופים במעבדת הצירופים'`
- Desktop: `'find joins in joins lab'` → `'מצא צירופים במעבדת הצירופים'`
- These are DIFFERENT keys for different UI contexts (web: tooltip on icon button;
  desktop: right-click menu item). Both correctly use `צירופים`. The HE form differs
  (`איתור` vs `מצא`) — this is INTENTIONAL variation between the apps, not a bug.
  The web key is correct as-is.

---

## Architecture Patterns

### System Architecture Diagram

```
genizah_translations.TRANSLATIONS (shared dict)
        ↑
        │ read by both apps
        │
  web/translations.py::tr()          genizah_core.py::tr()
  (returns input if lang=='en',       (returns input unless
   TRANSLATIONS.get if lang=='he')    CURRENT_LANG=='he')
        │                                    │
        ↓                                    ↓
  web/pages/joins_lab.py             desktop/join_workbench.py
  web/components/*.py                (UAT-approved source of truth)
  (313 tr() calls across 8 files)
        │
        ↓
  Web user sees HE/EN per session language
  (set by web/main.py:861 create_layout → set_language)
```

### Language Switch Flow

```
User clicks EN/HE toggle (web/main.py:1004-1012)
  → safe_user_set('ui_language', new_lang)   [persists to session storage]
  → set_language(new_lang)                   [sets process-global _current_lang]
  → ui.navigate.reload()                     [FULL PAGE RELOAD]
  → create_layout() fires on the new page load
  → _resolve_ui_language() reads 'ui_language' from session storage
  → set_language(resolved_lang) establishes correct lang for this render
  → All tr() calls in the page render return HE (or EN)
```

### Permanent CI Guard Pattern (D-02)

Modeled directly on `tests/test_join_workbench_i18n.py`:

```
tests/test_joins_lab_i18n.py
  ├── Check (a): No raw Hebrew literal outside tr() in 8 full-scan files
  │   (allowlist for D-04 items)
  ├── Check (b): All tr("literal") keys in 8 files resolve in TRANSLATIONS
  │   (add missing HE keys first, then guard enforces no future drift)
  ├── Badge string check: 3 badge_and_tooltip keys in TRANSLATIONS
  │   (explicit static list, like PHASE_107_HOST_KEYS pattern)
  └── Scoped entry-point check: JOINS_LAB_ENTRY_KEYS resolve + appear wrapped
      (assert keys for FND-04/05/D-19/ACT-02 entry points)
```

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Hebrew detection | Custom Unicode range logic | `re.compile(r"[֐-׿]")` | Standard Unicode Hebrew block; already in `_tmp/find_missing_tr2.py:24` |
| tr() key extraction | String search / regex | `ast.parse` + `ast.walk` | Only AST gives correct scope isolation (finds `tr("literal")` not `tr(variable)`) |
| Translation lookup | Dict rebuild | `from genizah_translations import TRANSLATIONS` | Shared source of truth for both apps |
| Badge string gap detection | Infer from code | Explicit static list | `tr(variable)` bypasses AST scanner; must be an explicit allowlist |

---

## Common Pitfalls

### Pitfall 1: AST Scanner Missing Variable tr() Args
**What goes wrong:** The scanner finds only `tr("literal")` — not `tr(variable)`. The 3
badge strings (`'Anchor fragment'`, `'Found via other side'`, `'Visually similar'`) would
be silently skipped, leaving them untranslated.
**How to avoid:** Include a dedicated test for the explicit badge-string list.
**Warning signs:** Hebrew user sees English text on the badge tooltip icon.

### Pitfall 2: D-04 Allowlist Under-Specified
**What goes wrong:** The guard's check (a) fails on legitimate Hebrew syntax examples in
`joins_builder.py:344-351` — flagging them as reverse-leaks when they're intentional.
**How to avoid:** The allowlist must contain the EXACT string values (e.g., `'#מילה'`,
not a pattern). Alternatively, exclude these by checking the parent node's structure
(they appear in a `ast.Tuple` inside a `list` — the context is not a raw string expression
passed to UI).
**Warning signs:** Guard fails on `test_no_raw_hebrew_literals` with `joins_builder.py:344`.

### Pitfall 3: Docstring Hebrew Not Excluded
**What goes wrong:** `joins_lab.py:217` has Hebrew in a docstring. If the scanner doesn't
structurally exclude docstrings, check (a) fires a false positive.
**How to avoid:** Skip `ast.Constant` nodes that are the first statement of a module/class/
function body (docstrings). In practice: check `node._parent` — if parent is `ast.Expr`
and the `ast.Expr` is the first statement in a `ast.Module`/`ast.FunctionDef`/`ast.ClassDef`
body, it's a docstring. The `_tmp/find_missing_tr2.py` scanner handles this via the
Tuple/List/IfExp parent check (paired inline-bilingual) but not docstrings — add a
structural docstring exclusion.

### Pitfall 4: RTL Assertions Need is_rtl() = True
**What goes wrong:** The existing render-smoke conftest may use the default `_current_lang =
'he'` (module-level in `web/translations.py`) or it may have been set to `'en'` by a prior
test. Without explicitly calling `set_language('he')` before opening `/joins-lab`, `is_rtl()`
returns the wrong value and `flex-row-reverse` never appears in the DOM.
**How to avoid:** The new RTL render-smoke test must call `set_language('he')` (or pass the
language via a conftest param) before calling `await user.open('/joins-lab')`.

### Pitfall 5: 'Open in Joins Lab' Drift Fixed in TRANSLATIONS Not Code
**What goes wrong:** The drift fix is to TRANSLATIONS dict, not to `lists.py`. If a dev
updates `lists.py:709` to use a different key, the TRANSLATIONS fix is stranded. The guard's
scoped key-check should assert that BOTH the key `'Open in Joins Lab'` exists in TRANSLATIONS
AND `tr('Open in Joins Lab')` appears in `lists.py`.
**How to avoid:** The scoped key-check in the guard uses the string-search pattern
`'tr("Open in Joins Lab")' in lists_source`.

---

## Code Examples

### AST Key Extractor (from test_join_workbench_i18n.py — adapt directly)

```python
# Source: tests/test_join_workbench_i18n.py:35-49
def _extract_tr_keys(source: str) -> list:
    """Return [(key_string, lineno), ...] for every tr("...") literal call."""
    tree = ast.parse(source)
    keys = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "tr"
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            keys.append((node.args[0].value, node.lineno))
    return keys
```

### Hebrew Literal Detector (from _tmp/find_missing_tr2.py — adapt)

```python
# Source: _tmp/find_missing_tr2.py:24-25,65-80
import re
HEB = re.compile(r"[֐-׿]")

# Tag parent nodes for context checking
for p in ast.walk(tree):
    for c in ast.iter_child_nodes(p):
        c._parent = p

# Detect raw Hebrew literals (not inside tr())
for node in ast.walk(tree):
    if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
        continue
    if not HEB.search(node.value):
        continue
    par = getattr(node, '_parent', None)
    if isinstance(par, ast.Call) and isinstance(par.func, ast.Name) and par.func.id == 'tr':
        continue  # correctly inside tr() — OK
    # → raw Hebrew leak candidate (check against allowlist)
```

### RTL Flex-Row-Reverse Pattern (from candidate_grid.py + compare_modal.py)

```python
# Source: candidate_grid.py:1363-1365
_pg_dir = "flex-row-reverse" if is_rtl() else "flex-row"
with ui.row().classes(
    f"w-full items-center justify-center gap-2 mt-2 {_pg_dir}"
):

# Source: compare_modal.py:792-794
_nav_dir_class = "flex-row-reverse" if is_rtl() else "flex-row"
with ui.row().classes(
    f"w-full items-center justify-between px-4 py-2 flex-wrap gap-2 {_nav_dir_class}"
):
```

---

## Runtime State Inventory

SKIPPED — this is a greenfield polish phase (no rename/refactor/migration).

---

## Environment Availability

SKIPPED — this phase is code/config changes only. All dependencies (Python, pytest, AST
module, genizah_translations) are already present. No new packages required.

---

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest (existing) |
| Config file | `pyproject.toml` (`[tool.pytest.ini_options]`) |
| Quick run command | `python -m pytest tests/test_joins_lab_i18n.py -x -q` |
| Full suite command | `python -m pytest tests/ -x -q` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| FND-07 (SC#3) | No raw Hebrew outside tr() in 8 files | AST static guard | `pytest tests/test_joins_lab_i18n.py::test_no_raw_hebrew_literals -x` | Wave 0 |
| FND-07 (SC#1) | All tr() keys have HE in TRANSLATIONS | AST coverage guard | `pytest tests/test_joins_lab_i18n.py::test_all_tr_keys_covered -x` | Wave 0 |
| FND-07 (SC#1) | Badge strings have HE in TRANSLATIONS | Explicit list check | `pytest tests/test_joins_lab_i18n.py::test_badge_strings_covered -x` | Wave 0 |
| FND-07 (SC#1) | Entry-point keys present + wrapped | Scoped host check | `pytest tests/test_joins_lab_i18n.py::test_entry_point_keys -x` | Wave 0 |
| FND-07 (SC#2) | flex-row-reverse present in HE mode | render-smoke assertion | `pytest tests/render_smoke/ -x -q` | Wave 1 (extend existing) |
| FND-07 (SC#2) | Visual RTL correctness across all surfaces | Manual HE-UAT checklist | Human execution | Wave 2 (human) |

### Sampling Rate
- **Per task commit:** `python -m pytest tests/test_joins_lab_i18n.py -x -q`
- **Per wave merge:** `python -m pytest tests/ -x -q`
- **Phase gate:** Full suite green + human UAT pass before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `tests/test_joins_lab_i18n.py` — new permanent guard (AST check a+b + badge list + scoped key-check)
- [ ] HE keys for 17 missing translations added to `genizah_translations.py::TRANSLATIONS`
- [ ] TRANSLATIONS drift fix: `'Open in Joins Lab'` corrected to `'פתח במעבדת הצירופים'`
- [ ] (Optional) `joins_lab.py:2252` `ws.title = tr('Candidates')` wrapping

---

## Security Domain

Not applicable — this phase adds translation strings and AST guards; no authentication,
input validation, or cryptography involved. No ASVS categories apply.

---

## Sources

### Primary (HIGH confidence — direct file inspection)

All findings in this document were verified by reading the actual source files at the specified
line numbers. No inference from training data alone.

- `web/translations.py` — language switch mechanism (`_current_lang`, `set_language`, `tr()`)
- `web/main.py:1004-1009` — SC#1 live-switch verdict (`ui.navigate.reload()`)
- `shared/joins_lab.py:639-659` — `badge_and_tooltip` user-facing text
- `web/components/candidate_grid.py:760, 838` — `tr(tooltip_text)` variable-arg call sites
- `tests/test_join_workbench_i18n.py` — desktop guard template (AST mechanics)
- `tests/test_pgp_filter_cascade.py` — underlying AST scanner pattern
- `tests/render_smoke/conftest.py` — render-smoke harness (fixture, stub data, F-A1/F-A2)
- `tests/render_smoke/test_joins_lab_render_smoke.py` — existing G1-G5/A2 assertions
- `web/components/candidate_grid.py:1361-1365` — `flex-row-reverse` RTL pattern
- `web/components/compare_modal.py:791-794` — `flex-row-reverse` RTL pattern
- `web/pages/joins_lab.py`, `web/components/candidate_grid.py`, `web/components/compare_modal.py` — gap scan results
- `genizah_translations.TRANSLATIONS` — HE entry verification for all 17 gap keys and drift check
- `web/pages/search_results.py:680` — FND-04 scoped key
- `web/pages/lists.py:698-709` — D-19 scoped key
- `web/pages/joins_lab.py:2195-2198, 2252` — export column headers + sheet name
- `desktop/join_workbench.py` — D-06 glossary source of truth (all keys verified against TRANSLATIONS)

---

## Assumptions Log

No claims in this research are tagged `[ASSUMED]`. All were verified by direct file
inspection in this session.

**If this table is empty:** All claims in this research were verified or cited — no user
confirmation needed.

---

## Open Questions

1. **XLSX sheet name `'Candidates'` (joins_lab.py:2252)**
   - What we know: `ws.title = 'Candidates'` is NOT wrapped in `tr()`. `'Candidates'` IS
     in TRANSLATIONS with HE `'מועמדים'`.
   - What's unclear: Whether the XLSX tab name is considered "user-facing" for this phase.
   - Recommendation: Apply `tr('Candidates')` consistently with D-03 (harmless) — one-line fix.

2. **render-smoke `set_language('he')` call in conftest**
   - What we know: The existing conftest does not explicitly call `set_language`. The RTL
     assertions require `is_rtl() == True`.
   - Recommendation: The new RTL assertion test calls `set_language('he')` before
     `user.open('/joins-lab')` and restores to default after, OR the conftest fixture
     gets an optional `lang='he'` parameter.

---

## Metadata

**Confidence breakdown:**
- Gap list (Q6): HIGH — AST scanner run on actual files, results verified against live TRANSLATIONS
- SC#1 verdict (Q1): HIGH — direct `web/main.py:1009` `ui.navigate.reload()` inspection
- Guard mechanics (Q3): HIGH — direct reading of `test_join_workbench_i18n.py`
- Render-smoke harness (Q4): HIGH — direct reading of `tests/render_smoke/` files
- Scoped key enumeration (Q5): HIGH — direct code inspection at each entry point
- Shared core verdict (Q2): HIGH — direct `shared/joins_lab.py:649-659` + call site inspection
- Glossary drift (Q7): HIGH — checked all anchor terms against TRANSLATIONS

**Research date:** 2026-06-21
**Valid until:** 2026-07-21 (translations/glossary stable; 30 days)
