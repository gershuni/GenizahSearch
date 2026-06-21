# Phase 121: i18n Polish - Context

**Gathered:** 2026-06-21
**Status:** Ready for planning

<domain>
## Phase Boundary

The closing phase of the **v8.2.0 Web Joins Lab** milestone. It guarantees the Joins Lab is
**fully bilingual EN/HE with correct RTL layout** and **no string leaks in either direction** —
across `/joins-lab`, the six Joins Lab components, and the Joins-Lab strings that live in shared
entry-point pages.

**Key finding from scouting (frames the whole phase):** the Joins Lab is *already* mostly
bilingual. `tr()` is used heavily (157 calls in `joins_lab.py`, 40–60 in most components) because
every prior phase shipped "bilingual from line one." A Hebrew-literal scan found **zero real
reverse-leaks** — the only Hebrew literals are in comments, one docstring search-syntax example
(`joins_lab.py:222`), and the syntax-legend operator tuples (`joins_builder.py:344-351`,
`('#מילה','prefix')` …) where the Hebrew is an intentional bilingual-safe *example* and the
*meaning* goes through `tr()`. **So Phase 121 is a verification + permanent-guardrail +
gap-closure + consistency phase, NOT a translate-from-scratch effort.**

**In scope (the one requirement):** FND-07 — entire Joins Lab UI bilingual EN/HE with correct RTL,
consistent with the rest of the web app. Realized as the 3 ROADMAP success criteria:
SC#1 (every UI string has EN+HE key; live language switch updates without reload),
SC#2 (Hebrew interface fully RTL across every surface),
SC#3 (static/AST audit confirms no raw Hebrew literal outside `tr()`).

**Explicitly NOT in this phase:**
- New Joins Lab features/behavior — this is polish/verification only.
- Re-translating or re-auditing the rest of the web app (non-Joins-Lab surfaces) beyond the
  glossary terms the Joins Lab shares with them.
- Desktop i18n work — the desktop Joins Lab is already UAT-approved bilingual (it is the
  glossary *source of truth* here, not a target).
- Cloud sync / cross-device persistence (PST-F1, future) and any other deferred-milestone items.

</domain>

<decisions>
## Implementation Decisions

### RTL verification method (SC#2)
- **D-01:** Verify RTL by **BOTH** (a) automated render-smoke assertions in CI AND (b) a structured
  live **HE-mode HUMAN-UAT checklist** (Hillel runs it). Rationale: per memory
  `feedback_nicegui_render_smoke_gap` + `reference_nicegui_flex_height_css`, headless pytest cannot
  see the async render path or computed-height collapse, and Phases 119/120 both shipped "green"
  then accumulated real RTL fixes only during live HE-mode UAT (Compare outer-scroll layout, RTL
  prev/next counter+arrows, height-collapse traps). Tests alone cannot honestly close SC#2.
- **D-01a (what render-smoke CAN assert):** the RTL *structural* attributes the headless path can
  reach — `dir="rtl"` / `text-align` / the **manual** `flex-row-reverse` ordering that 119/120
  already had to hand-code (browser auto-flip is not relied on; see `candidate_grid.py:1361-1362`,
  `compare_modal.py:791-799`). It is a regression guard for those specific structural choices,
  explicitly NOT a proof of visual correctness.
- **D-01b (what the HE-UAT checklist covers — what tests structurally cannot):** computed-height
  collapse, clipping/overlap, and mirroring correctness across every surface — anchor transcription
  right-aligned, builder rows RTL, candidate grid + table headers and cells, Compare panes mirrored
  (incl. LTR prev/next counter), dialogs, and toasts. Planner writes the checklist as a concrete
  per-surface line-item list; this is the load-bearing acceptance artifact for SC#2.

### Static i18n guard (SC#3)
- **D-02:** Ship a **PERMANENT CI guard** (new `tests/test_joins_lab_i18n.py`), adapted from the
  existing desktop AST scanner `tests/test_join_workbench_i18n.py` (pattern source:
  `tests/test_pgp_filter_cascade.py`). It runs **TWO checks**:
  - **(a) No raw Hebrew reverse-leak** — no string *literal* containing Hebrew (U+0590–U+05FF)
    appears outside a `tr()` call in the in-scope files. This is the ONLY real
    English-user-sees-Hebrew vector (memory `reference_i18n_audit_method`).
  - **(b) HE-coverage completeness** — every `tr("literal")` key in the in-scope files resolves to
    an entry in `genizah_translations.TRANSLATIONS` (the gap that shows English to HE users).
- **D-03 (coverage-check nuance, must be honored):** both `tr()` impls are **language-gated**
  (desktop returns input unless `CURRENT_LANG=='he'`; web returns input when `lang=='en'`), so the
  lang-gated inline-bilingual pattern `tr("עברית") if is_heb else tr("English")` has an English-half
  key that may legitimately lack a HE entry. Per `reference_i18n_audit_method`, **adding a HE key for
  those is harmless** (lang-gating prevents any reverse-leak). **Resolution: ADD the missing HE keys**
  rather than carve allowlist exceptions into check (b) — keeps the guard's coverage assertion clean
  and total. (Adding keys can never cause an English-user-sees-Hebrew leak.)
- **D-04 (allowlist — narrow, for check (a) only):** the no-raw-Hebrew check needs a small explicit
  allowlist for **intentional bilingual-safe Hebrew literals that are not user-facing prose**: the
  `joins_builder.py:344-351` syntax-legend operator tuples (`מילה`/`א`/`ב` are syntax *examples*,
  shown identically in both languages; meaning goes through `tr()`) and the `joins_lab.py:222`
  docstring search-syntax example. Comments and docstrings should be excluded **structurally** (the
  AST scanner inspects string-literal `tr()` args + flagged literals, not comments). Planner
  finalizes the exact allowlist.

### Audit & guard scope — which files (SC#3)
- **D-05:** Scope = **dedicated Joins Lab files (full scan) + scoped entry-point keys**, mirroring
  the desktop guard's two-part structure (full scan of the new module + scoped key-check on the huge
  host files):
  - **Full scan:** `web/pages/joins_lab.py`, `web/components/candidate_grid.py`,
    `web/components/compare_modal.py`, `web/components/anchor_viewer.py`,
    `web/components/joins_panel.py`, `web/components/joins_builder.py`,
    `web/components/known_joins_group.py`, `web/joins_lab_storage.py`.
  - **Scoped key-check** (assert the SPECIFIC NEW Joins-Lab keys both resolve in `TRANSLATIONS` and
    appear `tr()`-wrapped — do NOT scan these huge files wholesale): the `/search` "Find joins"
    card + Quick-View strings (FND-04), `/browse` "Find joins" (FND-05), the `/lists` "Open in Joins
    Lab" button label + tooltip (D-19), the `/puzzle` bulk-handoff toasts/messages (ACT-02), and the
    candidate **Export** sheet/column headers + filename (ACT-03 / D-06).
- **D-05a:** `web/joins_executor.py` is a pure off-loop adapter with **0 `tr()` calls / no
  user-facing strings** → out of guard scope. `shared/joins_lab.py` is pure logic — researcher must
  confirm whether `badge_and_tooltip` (or any helper) returns user-facing label text that the UI
  must wrap in `tr()`; if so, the wrapping happens on the web UI side (the shared core stays
  PyQt-free / app-agnostic), and those wrapping sites are already inside the full-scan files.

### HE glossary consistency (SC#1)
- **D-06:** Run a **cross-app reconciliation pass**: align web Joins Lab HE terms with the
  **UAT-approved DESKTOP Joins Lab** vocabulary. The shared `genizah_translations.TRANSLATIONS` dict
  makes this natural — where a web string lacks a HE key or drifted from the desktop term, fix it to
  the desktop term. **Source of truth:** the desktop Joins Lab (`desktop/join_workbench.py` + its
  existing keys in `genizah_translations.py`). Anchor terms to verify match: Joins Lab = מעבדת
  צירופים, plus anchor / candidate / Compare / look-alike (Visual Similarity) / triage
  Yes·Maybe·No, and the builder modifier *meanings*. Reuse the established glossary anchors from
  `reference_i18n_audit_method` (Reading Desk = שולחן עיון, List = רשימה, Correction = תיקון, etc.).

### Claude's Discretion
- The exact set of render-smoke RTL assertions per surface (D-01a) and the precise HE-UAT checklist
  line items (D-01b) — planner drafts both.
- The AST scanner structure and the final D-04 allowlist entries (reuse the desktop scanner).
- The final enumerated list of scoped entry-point keys (D-05) — derived from what FND-04/05, D-19,
  ACT-02, and D-06 actually introduced.
- Whether any genuinely-new Joins-Lab strings are discovered missing during the pass — fill them
  with HE keys via `TRANSLATIONS` (the default; harmless per D-03).

### Research flag (for the planner/researcher to confirm — not a user decision)
- **SC#1 "live language switch without page reload":** confirm how the web app switches language
  today (does `tr()` re-render on a language toggle, or does it require a reload?). If the web app
  has always required a reload to switch language, that is a **pre-existing app-wide behavior**, not
  Joins-Lab scope — note it and verify the Joins Lab is consistent with the rest of the app rather
  than inventing a Joins-Lab-only live-switch mechanism. Web default lang is `he`
  (`reference_i18n_audit_method`).

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Milestone requirement, roadmap & cross-phase constraints (read first)
- `.planning/REQUIREMENTS.md` — **FND-07** (line 22) is the sole requirement; carries the 5 hard
  cross-phase constraints (safe_storage chokepoint, proxy+breaker, off-loop, **bilingual EN/HE +
  RTL**, no new Supabase schema).
- `.planning/ROADMAP.md` §"Phase 121: i18n Polish" — the 3 success criteria this phase is verified
  against (lines 187–192); §"Hard constraints across all phases" (lines 58–64).

### The audit/guard templates to adapt (do not re-invent)
- `tests/test_join_workbench_i18n.py` — the desktop Joins Lab AST guard: full-module scan of the new
  module + scoped host-key check. **Direct structural template for the new web guard** (D-02/D-05).
- `tests/test_pgp_filter_cascade.py` — the underlying AST-scanner pattern both guards descend from.
- `_tmp/find_missing_tr2.py` — the reusable one-time `tr()`-gap scanner (AST-extract every
  `tr("literal")` arg, diff vs `TRANSLATIONS` keys). Useful for the initial gap-closure sweep; the
  PERMANENT guard (D-02) supersedes it. **Note:** `_tmp/` is gitignored — copy/adapt, don't depend.

### Translation system semantics (the load-bearing facts)
- `genizah_translations.py::TRANSLATIONS` — the single shared EN→HE dict used by BOTH apps.
- `web/translations.py` — web `tr()` (returns input when `lang=='en'`, else `TRANSLATIONS.get`;
  web default lang = `he`).
- `genizah_core.py` (~line 2735) — desktop `tr()` (returns input unless `CURRENT_LANG=='he'`).
  Both are **language-gated** — adding a HE key can NEVER reverse-leak Hebrew to EN users (D-03).

### Glossary source of truth (D-06)
- `desktop/join_workbench.py` — the UAT-approved desktop Joins Lab; its existing `tr()` keys define
  the cross-app HE vocabulary the web must match.

### Files under audit — full scan (D-05)
- `web/pages/joins_lab.py` (157 `tr()`) · `web/components/candidate_grid.py` (53) ·
  `web/components/compare_modal.py` (42) · `web/components/joins_panel.py` (61) ·
  `web/components/joins_builder.py` (53) · `web/components/anchor_viewer.py` (17) ·
  `web/components/known_joins_group.py` (7) · `web/joins_lab_storage.py` (6).

### Files under audit — scoped entry-point keys (D-05)
- `web/pages/search.py` — "Find joins" card + Quick-View entry (FND-04).
- `web/pages/browse.py` — "Find joins" entry (FND-05).
- `web/pages/lists.py` — "Open in Joins Lab" button label + tooltip (D-19, `:694`–`:701` action row).
- `web/pages/puzzle.py` — bulk-handoff toasts/messages (ACT-02).
- The candidate **Export** path (ACT-03 / D-06) — sheet/column headers + filename.

### Shared core to verify (D-05a)
- `shared/joins_lab.py` — confirm `badge_and_tooltip`/helpers emit no untranslated user-facing text
  (PyQt-free, app-agnostic; wrapping must happen on the web UI side, inside the full-scan files).

### Prior-phase context for the surfaces being polished
- `.planning/phases/117-vertical-spine/117-CONTEXT.md`,
  `.planning/phases/118-joins-entry-full-builders/118-CONTEXT.md`,
  `.planning/phases/119-candidates-compare-visual-similarity/119-CONTEXT.md`,
  `.planning/phases/120-actions-persistence/120-CONTEXT.md` — establish the surfaces, the FND-04/05
  / D-19 / ACT-02 / D-06 entry points, and the manual RTL choices (119/120 Compare + grid prev/next).

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **`tests/test_join_workbench_i18n.py`** — desktop AST i18n guard; the web guard is a direct
  adaptation (full scan of dedicated files + scoped host-key check on entry-point files).
- **`_tmp/find_missing_tr2.py`** — `tr()`-gap AST scanner for the initial sweep (gitignored temp).
- **`genizah_translations.py::TRANSLATIONS`** — shared dict; adding HE keys here serves both apps
  and the glossary reconciliation (D-06).
- **The 6 components + page already carry heavy `tr()` coverage** — the work is closing residual
  gaps + reconciling terms + locking the invariant, not bulk translation.

### Established Patterns
- **Bilingual from line one** — every Joins Lab string already routes through `tr()`; only residual
  gaps + glossary drift remain.
- **`tr()` is language-gated both ways** — only a *raw* Hebrew literal shown unconditionally leaks to
  EN users; adding HE keys is always safe (D-03).
- **Manual `flex-row-reverse` for RTL ordering** (not browser auto-flip) at the Compare + grid
  prev/next sites (119/120) — the render-smoke assertions target these explicit choices.
- **AST static guards as permanent CI invariants** (`test_pgp_filter_cascade.py`,
  `test_no_raw_storage_access.py`, the desktop i18n guard) — the precedent for D-02.

### Integration Points
- New `tests/test_joins_lab_i18n.py` (permanent guard) + render-smoke RTL assertions (extend the
  Phase-119 NiceGUI `User` render-smoke harness, `119-08-PLAN.md`).
- HE-key additions land in `genizah_translations.py::TRANSLATIONS` (shared).
- Scoped key-checks reference NEW keys introduced in `web/pages/{search,browse,lists,puzzle}.py`
  + the export path.

</code_context>

<specifics>
## Specific Ideas

- **"Render-smoke can't see RTL — it shipped green twice and still broke."** Verification must pair
  CI assertions with a real HE-mode UAT pass (D-01). Verbatim concern grounded in
  `feedback_nicegui_render_smoke_gap` and the 119/120 UAT history.
- **"Lock it forever, don't just audit once."** A permanent dual-check CI guard, not a throwaway
  script (D-02).
- **"Both apps should read the same in Hebrew."** Reconcile web HE terms against the UAT-approved
  desktop Joins Lab glossary (D-06).
- **The guard must check both directions of failure** — no Hebrew leaking to EN (raw-literal check)
  AND no English showing to HE users (every `tr()` key resolves) (D-02 a+b).

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope. (The one genuine web todo match,
`2026-06-18-joins-lab-search-results-survive-navigation.md`, was already folded into Phase 120; the
other `todo.match-phase 121` hits are spurious keyword coincidences for desktop/data/search areas
and were reviewed-and-rejected in Phases 117–120.)

</deferred>

---

*Phase: 121-i18n-polish*
*Context gathered: 2026-06-21*
