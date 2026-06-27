# Master Issues Register — Product-Quality Audit (2026-06-23)

> Source: 6-agent parallel audit (web↔desktop discrepancies, bugs, UI/UX, code quality,
> architecture/resilience, Joins-Lab parity) + independent **Codex** non-sandboxed verification of all
> findings. Severity/effort columns are **Codex-reconciled**. Full per-finding evidence:
> `_tmp/codex-audit-output.md`; original write-up: `~/.claude/plans/silly-juggling-seal.md`.
> 2 findings refuted (#4, #24) and removed. 5 new issues found by Codex (M1–M5).

## How to use this file
This is the source of truth for the audit backlog. Each finding has an **execution mode** and a **seed
assignment**. Seeds (SEED-013…019) are the plannable units; this register is the index. A one-line
pointer lives in `docs/OPEN_ISSUES.md`.

## Execution-mode legend
- **CLOUD-AUTO** — self-contained, unambiguous, testable end-to-end on a cloud branch off `origin`; no
  human decision needed. Lands as a PR with tests.
- **PARALLEL-CLOUD** — CLOUD-AUTO *and* touches a file-set disjoint from other in-flight seeds, so it can
  run as a concurrent cloud session. See the conflict map.
- **NEEDS-ASSESSMENT** — blocked on a product / design / risk decision from Hillel before it can be
  planned. Listed in "Decision gates" below.
- **DEFER** — real but not now (massive or low-value).

---

## Theme → Seed map (the plannable units)

| Seed | Theme | Findings | Mode | Primary files |
|------|-------|----------|------|---------------|
| **SEED-013** | Defensive guards & exception hygiene | #7 (incl. genizah_core:1860-1861 + result_dialog:1957/2202 siblings), #12, #13, #39 | CLOUD-AUTO (LOCAL-LAB degraded flag = contract decision) | genizah_core.py, genizah_app.py, desktop/result_dialog.py, gui_threads.py |
| **SEED-014** | Accessibility & RTL/bidi | #14, #15, #22, #25, #26, M3, #41, #11, #9(opt) | CLOUD-AUTO + human visual check | web/pages/search.py, search_results.py, web/components/anchor_viewer.py, web/components/filter_panel.py |
| **SEED-015** | Image-loading & NLI resilience unification (KEYSTONE) | #1, #2, M1, M4, M2, #36, #34(timeouts), #38 | NEEDS-ASSESSMENT → HARD plan | desktop/image_loader.py, join_workbench.py, web/pages/puzzle.py, web/api.py, shared/puzzle_image_service.py, genizah_core.py |
| **SEED-016** | Layering + browse executor | #3, #29 | CLOUD-AUTO (careful: test callers) | shared/browse_service.py, parallels_service.py |
| **SEED-017** | Web↔desktop parity & polish | #6, #10, #16, #17, #18, #19, #20, #30, #42, #43, #21 | NEEDS-ASSESSMENT (design/product) → then cloud | components/anchor_viewer.py, compare_modal.py, candidate_grid.py, joins_builder.py, desktop/puzzle.py, join_workbench.py, web/pages/puzzle.py |
| **SEED-018** | Code-neatness & cleanup | #32 (incl. _rrf_merge default :7667), #33 (separate triage vs status tokens), #34, #40 (keep tantivy TODO — pinned 0.25.1), #44/M5 (full caller list via rg) | CLOUD-AUTO (mechanical) | genizah_core.py, genizah_app.py, web/components/candidate_grid.py, desktop/join_workbench.py, shared/joins_lab.py, shared_export_utils.py→shared/export_utils.py (+shim), build_app.bat, .spec |
| **SEED-021** | Image-fetch observability + desktop viewer polish | #23, #36, #37, #38, #42, #43 | CLOUD-AUTO | web/api.py, web/components/image_resolution.py, desktop/join_workbench.py |
| **#31 (split out)** | `_tmp/` gitignore policy | #31 | ✅ DONE 2026-06-25 — `/_tmp/` ignored (cleared 340 scratch files); sole tracked probe doc relocated to `.planning/phases/118-…` | .gitignore |
| **SEED-019** | Stale-index diagnostics + desktop export | #28, #5 | ✅ DONE — #5 via SEED-024, #28 2026-06-25 (staleness helper + index_staleness_report + LOCAL parity) | genizah_core.py, desktop/join_workbench.py |
| **SEED-020** | Resume decomposition (v7.9 follow-on) | #45 | SEQUENTIAL (own milestone) | genizah_app.py, genizah_core.py → desktop/, shared/ |
| **DROP** | Refuted | ~~#4~~, ~~#24~~ | — | (already correct in code) |
| **N/A** | By design | #8 (server-guard exists), #35 (CLI self-test) | — | — |

---

## ⚠️ Conflict / parallelization map (critical for cloud)

`genizah_core.py` is touched by **SEED-013, SEED-015, SEED-018, SEED-019** → these **cannot** be parallel
cloud sessions (merge conflicts + worktree-forks-from-session-base gotcha). Sequence them, or let ONE cloud
session own all `genizah_core.py` edits. (Codex: SEED-019's #28 also touches it — added.)

`genizah_app.py` is touched by **SEED-013 (#12/#13), SEED-018 (#33 status dicts, #34 :5947?)** → same
sequential session. `web/components/candidate_grid.py` is touched by **SEED-018 (#33 glyph map)** and later
**SEED-017** → run 018 before 017. `web/components/anchor_viewer.py`: **SEED-014 (#41)** vs later **SEED-017**
→ 014 before 017. `desktop/join_workbench.py`: **SEED-021 (#42/#43)** vs later **SEED-017** → 021 before 017.

**Safe parallel set (round 1, verified disjoint):**
- **SEED-014** (web/pages/search.py + search_results.py + anchor_viewer.py + filter_panel.py)
  ‖ **SEED-016** (shared/*services **+ its callers in `web/search_api.py:56,59,1333-1338,1543-1550` + tests**
  — Codex: this file list was missing; still disjoint from 014/018-noncore)
  ‖ **SEED-018 non-core** (shared_export_utils→shared/export_utils move + #33 candidate_grid glyph map).
- **SEED-021** is NOT round-1 (web/api.py overlaps the blocked keystone 015; image_resolution.py was moved
  here from 013 to give it one owner) — run it as its own session before 015/017.

**Sequential session (share genizah_core.py / genizah_app.py):** SEED-013 + SEED-018-core (RRF_K incl.
default arg, timeouts, comments, #33 desktop+app tokens) as ONE branch → then SEED-015 (keystone, after
decisions) → then SEED-019.

**Blocked until decisions:** SEED-015, SEED-017, SEED-019.

**SEED-020 (decomposition) runs LAST, as its own milestone** — it rewrites the very files
(`genizah_app.py`, `genizah_core.py`) that 013/015/018/019 edit, so it must follow them to avoid
constant conflicts. **Proven approach (resume v7.9, CHANGELOG `[7.9.0]` 2026-04-19):** extract cohesive
units, behind tests, incrementally — never a big-bang rewrite (Codex agrees). Desktop: continue pulling
dialogs/panels/tabs into the `desktop/` package (v7.9 already moved ResultDialog, filter/scholarly
dialogs, viewers, puzzle canvas, VS cache). Core: extract search / index / metadata / ranking-merge
helpers from `genizah_core.py` into `shared/` modules. Each extraction = one atomic, test-guarded commit.

---

## Full finding register (Codex-reconciled)

Legend: Sev = CRITICAL/HIGH/MED/LOW · Eff = 1LINE/EASY/MED/HARD/MASSIVE · V = Codex verdict.

| # | Finding | Sev | Eff | V | Seed | Mode |
|---|---------|-----|-----|---|------|------|
| 1 | Desktop image loader bypasses NLI breaker (+long timeouts, no failure record) | HIGH | MED | CONFIRMED | 015 | needs-assess |
| 2 | 4 divergent image-loading impls + incompatible cache keys | HIGH | HARD | PARTIAL | 015 | needs-assess |
| 3 | `shared/` imports `web/` (layering violation) | HIGH | MED | CONFIRMED | 016 | cloud-auto |
| 5 | Desktop Joins Lab has no candidate export | MED | HARD | CONFIRMED | 019 | needs-assess |
| 6 | Fit/Reset icon mismatch `fit_screen` vs `restart_alt` (your example) | LOW | 1LINE | CONFIRMED | 017 | needs-assess(taste) |
| 7 | Silent exception swallowing on data paths (4 sites) | MED | EASY | PARTIAL | 013 | cloud-auto |
| 9 | CLS pagination placeholder | LOW | EASY | PARTIAL(already reserves) | 014 | cloud-auto(opt) |
| 10 | Anchor viewer lacks Rotate+Fullscreen | MED | MED | CONFIRMED | 017 | needs-assess |
| 11 | Long async ops no progress/error feedback | MED | MED | PARTIAL | 014 | cloud-auto |
| 12 | LOCAL-filter cycle ValueError on corrupt state | MED | 1LINE | CONFIRMED | 013 | cloud-auto |
| 13 | text_position_combo `-1`→wrong value | MED | 1LINE | CONFIRMED | 013 | cloud-auto |
| 14 | aria-label missing on search-toolbar icon buttons | MED | EASY | CONFIRMED | 014 | cloud-auto |
| 15 | RTL/bidi shelfmarks & mixed-script titles not isolated | MED | MED | CONFIRMED | 014 | cloud-auto |
| 16 | Toast(web) vs status-bar(desktop) message policy | LOW | EASY | PARTIAL | 017 | needs-assess |
| 17 | Puzzle toolbar raw emoji(desktop) vs Material icons(web) | LOW | EASY | CONFIRMED | 017 | needs-assess |
| 18 | Candidate-grid sorting enabled web / disabled desktop | LOW-MED | MED | CONFIRMED | 017 | needs-assess |
| 19 | Per-line modifier badge missing on web | LOW-MED | MED | PARTIAL | 017 | needs-assess |
| 20 | Known-joins not surfaced in candidate grid (page render exists) | LOW-MED | MED | PARTIAL | 017 | needs-assess |
| 21 | Image fit-mode CSS contain vs Qt KeepAspectRatio | LOW-MED | MED | PARTIAL | 017 | needs-assess |
| 22 | `except NameError` masks real NameErrors (_update_chip_bar) | LOW-MED | EASY | CONFIRMED | 014 | cloud-auto |
| 23 | NLI snapshot persisted outside lock (non-atomic, not unlocked) | LOW-MED | EASY | PARTIAL | 021 | cloud-auto |
| 25 | display:none toggle vs visibility API | LOW | EASY | CONFIRMED | 014 | cloud-auto |
| 26 | Expanded panel no boundary/collapse affordance | LOW | EASY | CONFIRMED | 014 | cloud-auto |
| 27 | Accessibility statement claims unenforced features | MED | MED | ✅ DONE 2026-06-25 | 014 | softened "fully/all" claims + date bump (web) |
| 28 | `_index_has_field` gate can hide stale index (main path logs) | MED | MED | ✅ DONE 2026-06-25 | 019 | helper + index_staleness_report + LOCAL warn |
| 29 | Default ThreadPoolExecutor browse fan-out (unkillable) | MED | MED | CONFIRMED | 016 | cloud-auto |
| 30 | "Reset"(web) vs "Clear"(desktop) label | LOW | EASY | CONFIRMED | 017 | needs-assess |
| 31 | `_tmp/` only partially gitignored | LOW | EASY | ✅ DONE 2026-06-25 | split-out | `/_tmp/` ignored; probe doc → .planning/ |
| 32 | RRF k=60 hardcoded (incl. default arg :7667) → RRF_K const | LOW | EASY | CONFIRMED | 018 | cloud-auto(core) |
| 33 | Icons/colors duplicated (separate triage vs status tokens; :508 already shared) | LOW-MED | MED | PARTIAL | 018 | cloud-auto |
| 34 | Magic timeout literals alongside NLI_* consts | LOW | EASY | CONFIRMED | 018 | cloud-auto(core) |
| 36 | Image fetch non-200/429/5xx unlogged | LOW | EASY | CONFIRMED | 021 | cloud-auto |
| 37 | resolve_external_images warning lacks exc_info | LOW | 1LINE | CONFIRMED | 021 | cloud-auto |
| 38 | resolve_external_images network I/O no io_bound guard | LOW | EASY | CONFIRMED(guardrail) | 021 | cloud-auto |
| 39 | Sleep-prevention bare except:pass | LOW | EASY | CONFIRMED | 013 | cloud-auto |
| 40 | Commented-out code + stale tantivy TODO (search_tokenizer line was wrong) | LOW | EASY | PARTIAL | 018 | cloud-auto(core) |
| 41 | Folio-arrow RTL: web fixed chevrons vs desktop direction-aware | LOW | EASY | CONFIRMED | 014/017 | cloud-auto |
| 42 | Desktop Compare lacks zoom-% label | LOW | EASY | CONFIRMED | 021 | cloud-auto |
| 43 | Loading: web skeleton vs desktop text | LOW | EASY | CONFIRMED | 021 | cloud-auto |
| 44 | shared_export_utils.py at repo root | LOW | MED | CONFIRMED | 018 | cloud-auto |
| 45 | God files (24.4K / 10.7K lines) — regrew from ~22.5K after v7.9 | MED-HIGH(maint) | MASSIVE | CONFIRMED | 020 | sequential milestone |
| M1 | Browser-side direct-NLI fallback bypasses server policy | MED | MED | NEW | 015 | needs-assess |
| M2 | TLS verify=False in desktop image fetches (security) | MED | MED | NEW | 015 | needs-assess |
| M3 | Expansion click target not a semantic button (kbd/aria) | MED | MED | NEW | 014 | cloud-auto |
| M4 | Image-fetch failure taxonomy split (unify ImageFetchResult) | MED | HARD | NEW | 015 | needs-assess |
| M5 | Root-level shared modules blur package ownership | LOW-MED | MED | NEW | 018 | cloud-auto |
| ~~4~~ | ~~Triage bleed~~ REFUTED (clear() exists at joins_lab.py:2460) | — | — | REFUTED | DROP | — |
| ~~24~~ | ~~nli_cache no lock~~ REFUTED (RLock at genizah_core.py:3752) | — | — | REFUTED | DROP | — |
| 8 | Search button not disabled — BY DESIGN (server `is_running` guard) | LOW | — | PARTIAL | N/A | optional polish |
| 35 | print() in telemetry self-test — BY DESIGN (CLI output) | LOW | — | by-design | N/A | skip |

---

## Decision gates — ANSWERED 2026-06-23

1. **SEED-015 image-loading:** ✅ **MINIMAL NOW** — wire desktop loader into the NLI breaker + short
   timeouts; defer the full 4-path unification. **TLS:** ✅ **keep `verify=False` but restrict to known
   NLI/Rosetta hosts + suppress warnings explicitly + document** (don't chase the cert chain now).
   → SEED-015 re-scoped to: #1 (breaker-wire desktop), M2 (TLS host-restrict+document), and the small
   resilience hygiene; #2/M1/M4 (full unification) DEFERRED to a later milestone.
2. **SEED-017 parity:** ✅ **ONLY #10 (add Rotate + Fullscreen to the Lab/Compare viewer).** NOT the
   reset/fit icon (#6), NOT desktop sorting (#18), NOT puzzle-toolbar icons (#17), NOT the others — those
   stay logged as low-pri/skip. SEED-017 collapses to a single-item viewer-control parity seed.
3. **SEED-019 export:** ✅ **XLSX** (Hillel: "CSV will probably not be much used") — do the XLSX export
   (prefer the shared research-export builder for column parity with web), skip CSV-only.
4. **#27 accessibility statement:** ⏳ STILL OPEN (not asked this round; default = soften to match reality
   as SEED-014's fixes land). Also still open: #31 `_tmp/` repo policy; SEED-020 decomposition scope
   (both god files vs genizah_app.py only); SEED-013 LOCAL-LAB UI contract (defaulted to logging-only).

**Sequencing note:** SEED-015/017/019 are now decision-unblocked but FILE-blocked on the running PRs —
015 & 019 touch `desktop/join_workbench.py`+`web/api.py`+`genizah_core.py` (owned by SEED-021 / 013+018core);
017 touches `anchor_viewer.py`+`compare_modal.py` (014 owns anchor_viewer). Generate + Codex-review them,
but LAUNCH only after round-1 + sequential + 021 PRs merge, to avoid base conflicts.

## STATUS — SEED-017 implemented + UAT-passed (2026-06-25)
SEED-017 (#10 rotate/fullscreen) on branch `audit/seed-017-viewer-rotate-fullscreen` (PR #310),
**UAT-passed on both apps**. Web `AnchorViewer` (Joins-Lab anchor pane + Compare modal) AND desktop
`join_workbench.py` (main workbench anchor pane + CompareDialog panes) got Rotate L/R + Reset +
Fullscreen — desktop reuses the ResultDialog controls (↺ ↻ ↩ ⛶, no brightness sliders) + its
FullscreenImageWindow. Scope expanded post-UAT per user: web reset icon → `restart_alt`; web
rotate-left signed (-90, not 270); desktop parity added. Codex-reviewed (3 findings fixed: HIGH
client-side-fullscreen, MED JS-state-sync, MED fullscreen-vs-inline-cap). Seed:
`.planning/seeds/SEED-017-lab-viewer-rotate-fullscreen.md`. Remaining audit work: SEED-015, SEED-019,
SEED-020, #27, #31.

## STATUS — CLOUD-AUTO batch SHIPPED (2026-06-24)
All 5 CLOUD-AUTO seeds merged to `master-main`: #296 (018-noncore), #298 (013+018-core), #297 (016),
#299 (021), #300 (014). Each Codex-reviewed; 2 real bugs caught + fixed pre-merge (#297 semaphore-lifetime,
#300 aria/lazy-load). **#300 awaits Hillel's local Hebrew UAT** (1 open nit: result-card `role=button`
contains nested buttons — drop the role if it misbehaves). **Next:** SEED-022 (transcription tag) now
unblocked; decision-gated SEED-015/017/019/020 + #27/#31 remain (see Decision-gates section).

## Recommended kickoff (post Codex seed-review, 2026-06-23)
SEED-013/014/016/018/021 written + Codex-reviewed (`_tmp/codex-seed-review-output.md`). Verdicts:
013/014/016 READY-WITH-FIXES (applied as "Codex review corrections" sections), 018 re-scoped (#31 split out),
021 created to home orphaned items. Remaining corrections are folded into the seed files.
1. **Round-1 parallel cloud:** SEED-014 ‖ SEED-016 (incl. its `web/search_api.py` callers) ‖ SEED-018-noncore
   (export-utils move + candidate_grid glyph map).
2. **Sequential session:** SEED-013 + SEED-018-core (one branch — shared genizah_core.py/genizah_app.py).
3. **Own session:** SEED-021 (web/api.py + image_resolution.py + desktop polish) before 015/017.
4. **Decisions unlock the rest:** the 4 gates + #31 (`_tmp/` repo policy) + SEED-013's LOCAL-LAB degraded
   contract → then plan SEED-015, 017, 019, 020.
