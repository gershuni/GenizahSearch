---
phase: 117
slug: vertical-spine
status: planned
nyquist_compliant: true
wave_0_complete: false
created: 2026-06-17
updated: 2026-06-17
---

# Phase 117 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x |
| **Config file** | pytest.ini / pyproject.toml (existing) |
| **Quick run command** | `pytest tests/test_web_search_executor.py tests/test_joins_lab_storage.py tests/test_joins_lab_off_loop.py tests/test_image_resolution.py tests/test_typography_promotion.py tests/test_candidate_grid.py tests/test_anchor_viewer.py tests/test_joins_lab_page.py tests/test_no_raw_storage_access.py -q` |
| **Full suite command** | `pytest tests/ -q` (CI: `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen`) |
| **Estimated runtime** | quick set: seconds (pure-Python, no Tantivy/Qt); full suite per conftest |

---

## Sampling Rate

- **After every task commit:** Run the quick run command (or the per-plan subset)
- **After every plan wave:** Run the full suite command
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** quick set < ~30s (no engine load, no Qt)

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| P01-T1 | 117-01 | 1 | FND-01 | T-117-02, T-117-03 | Adapter wraps `state.searcher` directly (no `/api/search`/HTTP), no raw `app.storage.user` | unit/source | `python -c "from web.joins_executor import WebSearchExecutor; from shared.joins_lab import SearchExecutor; assert isinstance(WebSearchExecutor(), SearchExecutor)"` | No — created here | ⬜ pending |
| P01-T2 | 117-01 | 1 | FND-01 | T-117-02 | Protocol compliance + `[]`/None/('','')/'' on failure + kwarg passthrough | unit | `pytest tests/test_web_search_executor.py -x -q` | No — Wave 0 | ⬜ pending |
| P01-T3 | 117-01 | 1 | FND-01 (SC#3) | T-117-01 | `execute_search` never on the event loop (static AST on joins_lab.py) | static/AST | `pytest tests/test_joins_lab_off_loop.py -x -q` | No — Wave 0 | ⬜ pending |
| P02-T1 | 117-02 | 1 | FND-06 | T-117-03, T-117-05, T-117-06 | All state via `safe_user_*`; `schema_version` invalidation; no result blobs | source | `python -c "assert 'app.storage.user' not in open('web/joins_lab_storage.py',encoding='utf-8').read()"` | No — created here | ⬜ pending |
| P02-T2 | 117-02 | 1 | FND-06 (SC#5) | T-117-04, T-117-05 | Two anonymous sessions no state-bleed; stale schema → cold start | unit | `pytest tests/test_joins_lab_storage.py -x -q` | No — Wave 0 | ⬜ pending |
| P03-T1 | 117-03 | 1 | ANC-03 | T-117-08 | RTL transcription helper promoted; XSS escape preserved | unit | `pytest tests/test_line_numbers_web.py -x -q` | Yes (existing) | ⬜ pending |
| P03-T2 | 117-03 | 1 | ANC-02 | T-117-07, T-117-09 | Per-provider resolver: proxy-only URLs, never direct IIIF | source/import | `python -c "import web.pages.browse"` | refactor | ⬜ pending |
| P03-T3 | 117-03 | 1 | ANC-02, ANC-03 | T-117-07, T-117-08 | 5-provider URL forms + no `iiif.nli.org.il` + gutter toggle | unit | `pytest tests/test_image_resolution.py tests/test_typography_promotion.py -x -q` | No — Wave 0 | ⬜ pending |
| P05-T1 | 117-05 | 1 | CND-02 | T-117-10, T-117-07, T-117-03 | Read-only grid; no triage/Compare/VS; no raw storage; no direct IIIF | source | `python -c "import ast; ast.parse(open('web/components/candidate_grid.py',encoding='utf-8').read())"` | No — created here | ⬜ pending |
| P05-T2 | 117-05 | 1 | CND-02 | T-117-10 | Browse-URL building, truncation, empty-state, chip-gating | unit | `pytest tests/test_candidate_grid.py -x -q` | No — Wave 0 | ⬜ pending |
| P06-T1 | 117-06 | 2 | ANC-01 | — | Per-instance zoom; idempotent head-HTML injection guard | source | `grep -n "window._msViewerLoaded" web/components/anchor_viewer.py` | No — created here | ⬜ pending |
| P06-T2 | 117-06 | 2 | ANC-01, ANC-02, ANC-03 | T-117-07, T-117-08, T-117-11 | Image (proxy-only) + folio nav + RTL transcription; no browse extras | source | `python -c "import ast; ast.parse(open('web/components/anchor_viewer.py',encoding='utf-8').read())"` | No — created here | ⬜ pending |
| P06-T3 | 117-06 | 2 | ANC-01, ANC-02 | T-117-07, T-117-11 | Zoom clamps; proxy-only URL; None-boundary no-raise; guard present | unit | `pytest tests/test_anchor_viewer.py -x -q` | No — Wave 0 | ⬜ pending |
| P04-T1 | 117-04 | 2 | FND-02, FND-08 | — | Route + nav + documented URL contract | source/parse | `grep -n "@ui.page('/joins-lab'" web/main.py` | modify | ⬜ pending |
| P04-T2 | 117-04 | 2 | FND-03, FND-08 | T-117-04, T-117-11, T-117-14 | Empty state; off-loop shelfmark resolve; persist/restore (URL wins); login-gated list prompt; no raw storage | source/unit | `pytest tests/test_joins_lab_page.py -x -q` | No — Wave 0 | ⬜ pending |
| P04-T3 | 117-04 | 2 | BLD-01, BLD-05, CND-01 | T-117-01, T-117-12, T-117-13 | Off-loop search (statically enforced); compose→dedup; re-entrancy + latest-wins + progress_cb guard | source/AST | `pytest tests/test_joins_lab_off_loop.py -x -q` | No (Plan 01) | ⬜ pending |
| P04-T4 | 117-04 | 2 | FND-03, FND-08, BLD-05, CND-01 | T-117-04 | SideQuery mapping, compose+dedup pipeline, URL-wins, off-loop guard enforces | unit | `pytest tests/test_joins_lab_page.py tests/test_joins_lab_off_loop.py -x -q` | No — Wave 0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

Test files created BEFORE / alongside their plan's implementation tasks (TDD tasks create them
RED first). All are derived from RESEARCH.md "Validation Architecture":

- [ ] `tests/test_web_search_executor.py` (Plan 01) — Protocol compliance, `[]`/None fallback, kwarg passthrough
- [ ] `tests/test_joins_lab_off_loop.py` (Plan 01) — static AST: `execute_search` not on the event loop; skip-when-absent, enforce after Plan 04
- [ ] `tests/test_joins_lab_storage.py` (Plan 02) — schema_version invalidation, round-trip, two-session isolation
- [ ] `tests/test_image_resolution.py` (Plan 03) — 5-provider URL forms, multi-IE offset, synthetic-sys_id, no direct IIIF
- [ ] `tests/test_typography_promotion.py` (Plan 03) — gutter toggle + XSS escape
- [ ] `tests/test_candidate_grid.py` (Plan 05) — browse-URL building, truncation, empty-state, chip gating
- [ ] `tests/test_anchor_viewer.py` (Plan 06) — zoom clamps, proxy-only URL, None-boundary, idempotency guard
- [ ] `tests/test_joins_lab_page.py` (Plan 04) — SideQuery mapping, compose+dedup, URL-wins-over-storage
- [ ] Existing `tests/test_no_raw_storage_access.py` — allowlist stays `[]` (CI guard, every plan)
- [ ] Existing `tests/test_line_numbers_web.py` — stays green after the typography promotion (Plan 03)

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Anchor image renders with zoom/pan + folio nav in a real browser | ANC-01 | Visual / IIIF-dependent rendering | Open `/joins-lab?sys_id=<known id>` and confirm image + zoom/pan controls + prev/next folio |
| RTL numbered transcription right-aligned alongside image | ANC-03 | Visual / RTL layout | Same page — confirm transcription is right-aligned numbered lines next to the image |
| Full slice: type lines + Run Search → deduped candidate grid renders | BLD-05, CND-01, CND-02 | End-to-end visual confirmation | On a loaded anchor, type 2–3 Hebrew lines, click Run Search, confirm a one-per-image candidate grid appears |
| Two anonymous sessions show no cross-session anchor bleed in a live browser | FND-06 (SC#5) | Live multi-session behavior (unit test covers the storage layer) | Open `/joins-lab` in two private windows; load different anchors; confirm each keeps its own |

*Remaining behaviors target automated verification — see the Per-Task Verification Map.*

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < threshold (quick set is pure-Python)
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** planned 2026-06-17
