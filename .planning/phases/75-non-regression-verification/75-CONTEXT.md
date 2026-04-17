# Phase 75: Non-Regression Verification - Context

**Gathered:** 2026-04-17
**Status:** Ready for planning
**Discussion basis:** User delegated all four gray areas to Claude's discretion. Decisions below are Claude's calls, grounded in the milestone's established patterns (Phase 71 smoke checklist, Phase 74 UAT format).

<domain>
## Phase Boundary

Manual, qualitative sign-off that the v7.9 decomposition (Phases 67–74) did **not** degrade responsiveness on the four user-facing surfaces called out in ROADMAP Phase 75 success criteria:

1. **Web search** — initial render, result paging, result interaction (expand, browse, export)
2. **Web browse** — manuscript load, enrichment panel population, volume switching
3. **Desktop search** — basic search, composition search, ResultDialog interaction
4. **Desktop browse** — page navigation, image loading, folio switching

Bar (per ROADMAP): **no obvious slowdown versus pre-refactor**. No quantitative thresholds, no benchmark harness. Plus `pytest tests/` baseline green (1067 passed, 8 skipped).

In scope:
- Executor walks through a fixed checklist per surface.
- User provides explicit yes/no sign-off per surface.
- Pytest baseline is re-confirmed green on the working tree.
- Any regression found is triaged (blocker → gap plan; minor → deferred note).

Out of scope:
- Adding a benchmark harness (explicitly rejected per REQUIREMENTS.md non-goals).
- Feature changes, bug fixes for issues unrelated to decomposition-induced regressions.
- Documentation refresh (belongs to Phase 76).
- Desktop functional smoke test authoring — already exists as `docs/desktop-smoke-checklist.md` (Phase 71). Phase 75 reuses it as the desktop functional baseline and layers a responsiveness overlay on top.

</domain>

<decisions>
## Implementation Decisions

### GA1 — Baseline Reference Method

- **D-01:** Primary comparison basis is the **user's memory / gut feel**. The user exercises the app daily and has a clear sense of "this used to be snappier." The ROADMAP bar is explicitly qualitative; there is no pre-refactor timing dataset to diff against.
- **D-02:** Fallback when the user is uncertain on any surface: executor creates a **pre-refactor worktree at commit `56facc3d`** (parent of `ca4995bc`, which is the first Phase 67 refactor commit — the last commit where `genizah_app.py` was monolithic and `web/pages/search.py` / `web/pages/browse.py` were un-split). This commit's code state is identical to `37aeba29` (milestone v7.9 kickoff) since the intervening commits only touch `.planning/`. User can run both versions side-by-side to A/B.
  - Worktree path: `C:/tmp/gsd-review/v7.8-baseline/` (reusing the agreed scratch area per this session's environment).
  - Command sketch: `git worktree add C:/tmp/gsd-review/v7.8-baseline 56facc3d`
- **D-03:** Baseline data assets (SQLite sidecars, Tantivy index, `libraries_translations.db`, etc.) are **shared between working tree and baseline worktree** — the refactor did not touch data paths, so no duplication needed. Baseline worktree uses the same `pgp_data/`, `fist_data/`, `nli_data/`, and `Genizah_Index/` as the live tree.
- **D-04:** The A/B fallback is executor-initiated only when the user flags uncertainty on a specific surface during sign-off. It is **not** required for every surface. Most surfaces are expected to sign off on gut feel alone.

### GA2 — Canonical Test Script Per Surface

- **D-05:** Adopt a **fixed minimal script per surface**, pre-populated into `75-UAT.md`. Reproducible, but short. Each success criterion from ROADMAP maps to ≤4 checklist items. Executor runs the script; user watches and signs off.

- **D-06:** **Desktop surfaces** reuse `docs/desktop-smoke-checklist.md` (Phase 71 artifact) as the **functional** baseline. Phase 75's script references sections 2 (Basic Search), 4 (Browse Navigation) from that doc and adds a **responsiveness overlay**:
  - "Subjective load time for results table ≤ pre-refactor recall"
  - "Subjective image load time on Browse ≤ pre-refactor recall"
  - "No new visible hitches/stalls during pagination"

- **D-07:** **Web surfaces** have no existing checklist. Executor authors the equivalent overlay inline in `75-UAT.md` during plan/execute. Minimum items per web surface:
  - **Web search:** (a) cold-load `/` → first paint; (b) execute text query `"שלום"` → first results visible; (c) expand one result accordion; (d) click "Browse" on one result; (e) click "Export" on one result; (f) paginate forward twice.
  - **Web browse:** (a) navigate `/browse?sys_id={FIXED_NLI_SHELFMARK}` → manuscript visible; (b) enrichment panel populates (FJMS catalog, bibliography); (c) navigate `/browse?sys_id={FIXED_CAMBRIDGE_SHELFMARK}` → CUDL image loads; (d) navigate `/browse?sys_id={FIXED_MULTI_IE_SYS_ID}` → volume selector renders and switching a volume updates image + text; (e) folio Prev/Next updates URL bar (Phase 74 E2E regression check — confirms D-20 of Phase 74 holds under real use).

- **D-08:** **Fixed test manuscripts** (locked during planning, used every run):
  - One Cambridge T-S (e.g. `T-S 12.123` — executor confirms it's in the index).
  - One NLI-only record with crossref images (executor picks from `nli_crossref.db`).
  - One multi-IE record from `.planning/debug/multi_ie_fl_validation.csv` (first row: `sys_id=990000412990205171`, 2 IEs, 7 transcription FLs).
  - One JTS DPUL record (confirms Princeton DPUL image path still works — v7.2.3 regression surface).
  - The planner records concrete sys_ids in `75-UAT.md` so reruns are identical.

- **D-09:** **Composition search** test case: a short composition query (e.g. 2–3 short chunks, ≤50 chunks target) against the index. Cancel-with-partial-results is NOT exercised here — that's feature behavior, not a responsiveness regression surface. Responsiveness check is: timer starts, ETA appears within ~2s, results stream in.

### GA3 — Sign-off Artifact Format

- **D-10:** Single canonical UAT file: `.planning/phases/75-non-regression-verification/75-UAT.md` (**not** `75-HUMAN-UAT.md` — Phase 74 used `74-HUMAN-UAT.md` but `75-UAT.md` matches the broader project naming in recent phases like the `/260318-kk1-*-UAT.md` files). The format mirrors Phase 74's YAML frontmatter + sectioned checklist.

- **D-11:** `75-UAT.md` structure (pre-populated by executor, signed by user):
  ```
  ---
  status: in-progress | passed | failed
  phase: 75-non-regression-verification
  source: [75-VERIFICATION.md]
  started: YYYY-MM-DD
  updated: YYYY-MM-DD
  ---

  ## Current Test
  [item in progress, or "none"]

  ## Tests

  ### 1. Web Search Responsiveness
  expected: [items from D-07 web-search bullet]
  result: [passed / failed (notes) — user's explicit yes or no]

  ### 2. Web Browse Responsiveness
  expected: [items from D-07 web-browse bullet, using FIXED_* sys_ids from D-08]
  result: ...

  ### 3. Desktop Search Responsiveness
  expected: [items from docs/desktop-smoke-checklist.md §2 + D-06 overlay]
  result: ...

  ### 4. Desktop Browse Responsiveness
  expected: [items from docs/desktop-smoke-checklist.md §4 + D-06 overlay]
  result: ...

  ### 5. pytest baseline
  expected: 1067 passed, 8 skipped (no new failures, no new skips)
  result: ...

  ## Summary
  total: 5
  passed: N
  issues: N
  pending: N
  skipped: N
  blocked: N

  ## Gaps
  [if any — linked to `/gsd-plan-phase 75 --gaps`]
  ```

- **D-12:** Sign-off granularity: **per-surface** (criteria 1–4 in ROADMAP). User says "yes" or "no (notes)" for each of the four surfaces, plus a boolean on the pytest baseline. No finer-grained sign-off is required — this matches ROADMAP's "explicit yes/no per surface" instruction literally.

- **D-13:** `75-VERIFICATION.md` is the final phase-gate report, produced by the verifier agent after all five `75-UAT.md` tests pass. It reads the UAT, pytest output, and provides the phase-close summary. Mirrors the pattern in `74-VERIFICATION.md`.

- **D-14:** The UAT is a **living file** during execution — executor updates the `updated:` field and checks off items as the user confirms each. Final state is `status: passed` with all 5 tests passed. Partial states (`issues`, `pending`) are permitted mid-run.

### GA4 — Regression-Found Escape Hatch

- **D-15:** **Two-tier triage**, user has final say on severity:
  - **Blocker regression** — surface is demonstrably slower in a way that impacts real workflow (e.g. result expansion takes >1s longer than recall, image loads visibly stall where they didn't before, URL bar stops updating on navigation, pagination blocks the UI). User explicitly labels it "slow enough to fix now."
    - **Action:** halt phase, run `/gsd-plan-phase 75 --gaps` to generate a gap plan, executor fixes in a new `75-02-PLAN.md` (or higher), re-verify on the affected surface only, user re-signs the affected `75-UAT.md` row.
  - **Minor perceptual difference** — something feels subtly different but workflow is fine. User labels it "noted, not blocking."
    - **Action:** append to `docs/OPEN_ISSUES.md` under a new "v7.9 decomposition — cosmetic perf observations" section. Phase 75 continues and closes. Future milestone can pick them up if prioritized.

- **D-16:** **Do NOT defer blockers to Phase 76.** Phase 76 is documentation close — it must not become a dumping ground for deferred fixes. If something is a blocker, it gets fixed in Phase 75 via the gap plan mechanism.

- **D-17:** **Pytest failure = automatic blocker.** If `pytest tests/` does not come back 1067 passed + 8 skipped on the final verification run, that's a hard failure, not a subjective regression. Treat as a gap; do not sign off until resolved.

- **D-18:** **Test 5 (pytest) runs last**, after the four manual surfaces sign off. Rationale: (a) manual surfaces need a running app; pytest tears down databases and can leave fixtures in inconsistent states on interrupt; (b) if a manual surface fails and triggers a gap plan, the pytest run after the fix is the authoritative baseline — running it before the fix is wasted.

### Verification

- **D-19:** Phase gate satisfied when `75-UAT.md` reaches `status: passed` with all 5 tests `result: passed`, plus a `75-VERIFICATION.md` report confirming same.
- **D-20:** No CI gate is added for this phase beyond the existing `.github/workflows/ci.yml` pytest run — the qualitative sign-off is inherently human.

### Claude's Discretion

- Exact number of plans (likely 1 plan: pre-populate UAT + walk through with user + produce VERIFICATION.md — single linear flow, no wave-able parallelism).
- Whether to author a separate `docs/web-smoke-checklist.md` mirroring the desktop one, or keep web checklist inline in `75-UAT.md`. Default: inline for now; promote to `docs/` if it turns out to be reusable for future milestones.
- Whether to include a Hebrew query AND English query (minor coverage bump) or just one. Default: one Hebrew query (`"שלום"`) per surface — matches the existing desktop-smoke-checklist.md convention.
- How the executor drives the walkthrough — offering to share screen, running commands verbally, or pasting URLs for the user to click. Default: executor runs the app, reports what it sees, user confirms.
- Whether to tee pytest output to `.planning/phases/75-non-regression-verification/75-pytest-baseline.txt` for the verification report or rely on inline summary. Default: tee it — cheap and gives the verifier concrete evidence.

### Folded Todos

None — pending todos in STATE.md are feature work, not regression verification.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Roadmap & Requirements
- `.planning/ROADMAP.md` — Phase 75 entry (explicit success criteria 1–5; "manual checklist only — no benchmark suite" mandate)
- `.planning/REQUIREMENTS.md` — NREG-01 (manual non-regression check; no benchmark harness introduced)
- `.planning/PROJECT.md` — v7.9 Decomposition milestone
- `.planning/STATE.md` — current phase tracking

### Phase 75 Inputs
- `.planning/phases/74-page-scoped-state-refactor/74-CONTEXT.md` — URL-bar E2E test (D-20) that must still pass under real use
- `.planning/phases/74-page-scoped-state-refactor/74-HUMAN-UAT.md` — YAML-frontmatter UAT format that `75-UAT.md` mirrors
- `.planning/phases/74-page-scoped-state-refactor/74-VERIFICATION.md` — VERIFICATION.md structure to mirror

### Existing Checklists (reuse, don't rewrite)
- `docs/desktop-smoke-checklist.md` — Phase 71 deliverable. Sections 2 (Basic Search) and 4 (Browse Navigation) are the functional baseline for desktop surfaces. Phase 75 layers a responsiveness overlay on these.

### Baseline Code Reference
- `56facc3d` (git SHA) — last commit before any Phase 67 code changes. Use `git worktree add C:/tmp/gsd-review/v7.8-baseline 56facc3d` for A/B fallback comparison (D-02).
- `ca4995bc` — first Phase 67 refactor commit, for reference.
- `37aeba29` — milestone v7.9 kickoff (docs-only; code state identical to 56facc3d).

### Fixed Test Data
- `.planning/debug/multi_ie_fl_validation.csv` — first row (`sys_id=990000412990205171`, 2 IEs) is the locked multi-IE test manuscript (D-08).
- `nli_crossref.db` — source of NLI-only test manuscript; planner picks and records sys_id in UAT.
- `Genizah_Index/` — Tantivy index, required for web/desktop search tests.
- `pgp_data/pgp.db`, `fist_data/fjms_enrichment.db`, `nli_data/nli_crossref.db` — enrichment sources needed for browse tests.

### Subjects of Verification (changed during v7.9)
- `desktop/result_dialog.py`, `desktop/puzzle.py`, `desktop/dialogs_filter.py`, `desktop/dialogs_scholarly.py`, `desktop/viewers.py`, `desktop/vs_cache.py`, `desktop/widgets.py`, `desktop/title_helpers.py`, `desktop/image_loader.py` — new desktop modules (Phases 67–71)
- `web/pages/search.py`, `web/pages/search_state.py`, `web/search_bootstrap.py` — Phase 72 split
- `web/pages/browse.py`, `web/pages/browse_state.py`, `web/pages/browse_enrichment.py`, `web/browse_bootstrap.py` — Phase 73 split
- `web/components/filter_panel.py` — Phase 74 persistence boundary
- `web/pages/search_state.py`, `web/pages/browse_state.py` — Phase 74 `restore_/persist_/clear_*_snapshot` helpers
- `genizah_app.py` — Phase 67–71 extraction coordinator (re-export shims intact)

### CI & Baseline
- `.github/workflows/ci.yml` — Ubuntu + Windows matrix
- `tests/` — pytest baseline 1067 passed, 8 skipped (must remain exact)

</canonical_refs>

<code_context>
## Existing Code Insights

### Desktop Smoke Checklist Already Exists
`docs/desktop-smoke-checklist.md` was created in Phase 71 as a desktop functional smoke test. Phase 75 does NOT rewrite it — it reuses sections 2 and 4, adding a responsiveness overlay. This is the single biggest "reusable asset" for the phase.

### Phase 74 Established the UAT Format
`74-HUMAN-UAT.md` is the precedent: YAML frontmatter (`status`, `phase`, `source`, `started`, `updated`), `## Current Test`, `## Tests`, `## Summary`, `## Gaps`. Phase 75 mirrors this literally. Dropping the `HUMAN-` prefix aligns with the broader project's `*-UAT.md` naming (seen in `.planning/quick/260318-kk1-*/260318-kk1-UAT.md`).

### URL-Bar E2E Regression Is a Known Risk
Phase 74's D-20 added `tests/` coverage for URL-bar updates on browse navigation. This was the detached-task failure mode that Cat-1 `ensure_future` cleanup fixed. Phase 75's desktop browse and web browse manual walkthroughs must exercise page navigation to confirm the fix holds in real use, not just in the test harness.

### Git Worktree Is the Right Fallback Mechanism
The project already uses `C:/tmp/gsd-review/` for review worktrees. Adding a `v7.8-baseline` worktree at `56facc3d` follows the established pattern. No new tooling needed.

### Pytest Baseline Is Concrete
1067 passed, 8 skipped is the exact target from ROADMAP success criterion #5 and Phase 74's D-21. Any drift is a hard failure.

</code_context>

<specifics>
## Specific Ideas

- **"No obvious slowdown versus pre-refactor"** is the exact ROADMAP bar. Not "measurably faster." Not "within X ms." Just: does it feel the same or better? The user's gut is the measurement instrument.
- **Pre-refactor worktree as a crutch, not a requirement.** The goal is to sign off fast on things the user already knows are fine. The worktree is only for the handful of surfaces where recall is fuzzy.
- **Reuse Phase 71's desktop checklist** — don't retype it. Reference by section number.
- **Four surfaces + pytest = five items.** Not twenty. Keep the UAT scannable in one screen.
- **Gap plan mechanism for blockers** — `/gsd-plan-phase 75 --gaps` is the existing project tool for exactly this. Don't invent something new.
- **OPEN_ISSUES.md for deferred perceptual notes** — the project already has this doc, already has the "v7.9 decomposition" as an active topic. Append, don't create a new doc.

</specifics>

<deferred>
## Deferred Ideas

### For Future Milestones
- **Web smoke checklist in `docs/`** — if it turns out to be reusable (e.g. for a v8.0 web refactor), promote the inline web checklist from `75-UAT.md` to `docs/web-smoke-checklist.md` as a Phase 75 byproduct or a v8.x task. Not required for this phase.
- **Quantitative performance baseline harness** — explicitly rejected for v7.9 per REQUIREMENTS.md non-goals. Revisit only if future refactor risk warrants the engineering cost.
- **Automated perceptual regression (e.g. Playwright with screenshot diffs)** — interesting but way beyond v7.9 scope.

### Reviewed Todos (not folded)
None — no pending todos are in scope for Phase 75.

</deferred>

---

*Phase: 75-non-regression-verification*
*Context gathered: 2026-04-17*
