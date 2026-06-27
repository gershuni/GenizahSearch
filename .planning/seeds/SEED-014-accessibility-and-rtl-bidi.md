---
id: SEED-014
status: shipped
planted: 2026-06-23
planted_during: 2026-06-23 product-quality fan-out audit (6 agents + Codex). Register: .planning/audit-2026-06-23-product-quality/MASTER.md
trigger_when: CLOUD-AUTO for the code; needs a HUMAN VISUAL CHECK at the end (NiceGUI headless render gap — see the "render-smoke" lesson). Round-1 PARALLEL-SAFE: file-disjoint from SEED-013 (no web/pages), SEED-016 (shared), SEED-018-noncore. Owns web/pages/search.py, search_results.py, components/anchor_viewer.py, filter_panel.py, accessibility.py. One sub-item (#27) is decision-gated.
scope: medium (a11y + RTL/bidi correctness across the search results surface, all web)
---

# SEED-014: Accessibility & RTL/bidi

> From the 2026-06-23 audit. Web-only. The corpus is Hebrew/Judeo-Arabic with mixed-script shelfmarks, so
> bidi correctness is real user-facing correctness, not polish. Codex elevated #14/#15/#27 + added M3.

## Findings (file:line + fix direction)

### #14 — `aria-label` missing on search-toolbar icon buttons (MED · EASY)
`web/pages/search.py:567-569, 573-575, 1520-1540` have `.tooltip()` but no `aria-label`. Local convention
to copy: `web/components/anchor_viewer.py:590,599,608,619,626` (`.props('... aria-label="..."')`).
**Fix:** add `aria-label` to every icon-only button in the search toolbar + bulk-action row. Tooltips are
NOT accessible names.

### #15 — Shelfmarks & mixed-script titles not bidi-isolated (MED · MED)
`web/pages/search_results.py:241-247, 288-294, 473, 557-564` — Hebrew rows forced RTL with shelfmarks
(`T-S NS 192.21`) and Latin/digits not isolated → they float to the wrong side.
**Fix:** wrap shelfmarks + mixed-script fragments in isolated bidi (`dir="auto"` + `unicode-bidi:isolate`,
or `<bdi>`). Add visual tests for Hebrew/English shelfmark+title mixtures.

### M3 — Expansion click target is not a semantic button (MED · MED)
`web/pages/search_results.py:388-389` — expansion attached to a generic content column via `.on('click')`;
no button role, focus, keyboard activation, `aria-expanded`/`aria-controls`.
**Fix:** make it a real button OR add role+tabindex+keyboard handler+aria-expanded/controls.

### #22 — `except NameError` masks real NameErrors (LOW-MED · EASY)
`web/pages/search.py:660-664` catches all NameErrors from `_update_chip_bar()` to handle "not built yet".
**Fix:** explicit readiness flag, or init `_update_chip_bar` to a no-op; do not blanket-catch.

### #25 — `display:none` toggle vs visibility API (LOW · EASY)
`web/pages/search_results.py:94-112, 736` set `display:none/block` directly → SR may still narrate hidden
content. **Fix:** use NiceGUI visibility/binding or a state flag.

### #26 — Expanded panel: no boundary/collapse affordance (LOW · EASY)
`web/pages/search_results.py:779-826` (toggled at 388-389). **Fix:** subtle boundary + explicit Collapse
icon-button with focus + aria-label (pairs with M3).

### #11 — Long async ops: no progress/error feedback (MED · MED)
`web/pages/search_results.py:686-700` (join counts), `832-849` (lazy text), `web/components/filter_panel.py:345-434`
(filter recompute) run via `run.io_bound` with no per-op loading/error state. **Fix:** local pending +
failure states (don't block the whole page; main search spinner already exists elsewhere).

### #41 — Folio-arrow not direction-aware (LOW · EASY)
`web/components/anchor_viewer.py:588-599` uses fixed `chevron_left/right`; browse already RTL-aware
(`browse.py:3835,3888`). **Fix:** mirror browse's direction-aware logic in AnchorViewer.

### #9 — CLS pagination placeholder (LOW · EASY · OPTIONAL)
`web/pages/search_results.py:188-218` already inserts a hidden placeholder (Codex: not a clear CLS bug).
**Fix (optional):** render a stable pagination container always, toggle inner controls.

## DECISION-GATED sub-item (do NOT auto-fix)
### #27 — Accessibility statement claims unenforced features (MED · MED)
`web/pages/accessibility.py:44-49` claims keyboard nav / focus indicators / labels / semantic headings,
contradicted by #14 + M3 + `typography.py` wrappers. **Hillel decides:** soften the statement to match
reality, OR commit to implementing WCAG AA (then this seed grows). Until decided, leave the statement; the
#14/#15/M3/#25/#26 fixes move reality toward the claims regardless.

## Tests required
- Render-smoke / DOM assertions where possible: aria-label present on toolbar buttons (#14); shelfmark
  span carries isolation attrs (#15); expansion control exposes button role + aria-expanded (M3/#26).
- ⚠ Headless pytest CANNOT see computed-height/bidi rendering — REQUIRE a manual Hebrew-UI visual pass
  (toolbar SR labels, shelfmark/title in mixed script, expand/collapse keyboard) before merge.

## Done when
Code fixes landed + tests green + ruff clean; #27 logged as a separate decision; human visual check signed off.

---

## Codex review corrections (2026-06-23) — apply before execution
- **#14 add more buttons:** also `web/pages/search.py:343-345` and `:370-372` (icon-only, tooltip but no
  aria-label). Test must assert ALL icon-only controls in the affected toolbar/panel have accessible names,
  not just the originally cited rows.
- **#15 audit globally + escape:** more shelfmark/title renderings exist later in `search_results.py`
  (advanced-dialog labels) — audit the whole file, not only the 4 cited rows. If using `ui.html`/`<bdi>`,
  **escape the shelfmark/title text first** (injection-safe).
- **#25 rationale corrected:** `display:none` IS normally hidden from screen readers — the real issue is
  imperative DOM state with no semantic expansion control / aria state / NiceGUI-visible state. Reframe.
- **M3/#26:** require keyboard activation + `aria-expanded` (and `aria-controls`) state updates, not just a
  static `role`.
- **#27 / accessibility.py:** do NOT list `accessibility.py` as an auto-owned file — #27 is decision-gated.
  Remove it from the owned-file set until Hillel decides soften-vs-implement.
- **#9 stays optional** — placeholders already reserve 40px (`search_results.py:197-198,217-218`); not a clear
  CLS bug.
- **MASTER fix:** SEED-014 row must include #9/#11/#22 + `web/components/filter_panel.py`, and drop
  `accessibility.py`. (Reconciled in MASTER.)
- **Tests:** DOM/source assertions for aria-label, real-button-or-`role=button`+`tabindex`, `aria-expanded`,
  `aria-controls`; bidi-wrapper attrs (`dir="auto"` + `unicode-bidi:isolate` / `<bdi>`); a small async-state
  test for filter-recompute pending/failure (makes #11 more than visual). Manual Hebrew/mixed-script +
  keyboard pass still REQUIRED before merge.
