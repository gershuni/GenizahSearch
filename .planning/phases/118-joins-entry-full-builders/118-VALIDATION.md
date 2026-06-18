---
phase: 118
slug: joins-entry-full-builders
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-06-18
---

# Phase 118 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Source: `118-RESEARCH.md` § Validation Architecture (verified against live code).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (existing) |
| **Config file** | `pytest.ini` / `pyproject.toml` (existing) |
| **Quick run command** | `pytest tests/test_joins_lab*.py tests/test_no_raw_storage_access.py -x` |
| **Full suite command** | `pytest tests/ -x` (CI uses marker-based `gui-tests` split; see `feedback_full_suite_testing_windows`) |
| **Estimated runtime** | quick ~15s · full suite minutes (Tantivy load per worker — do NOT `-n auto` locally) |

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/test_joins_lab*.py tests/test_no_raw_storage_access.py -x`
- **After every plan wave:** Run `pytest tests/ -x`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** ~15 seconds (quick run)

---

## Per-Task Verification Map

> Filled by the planner with concrete task IDs. The Requirement → test-file mapping is fixed below
> (from RESEARCH.md); the planner maps each task to its requirement + test command.

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 118-NN-NN | NN | 0 | ANC-05 | T-118-01 | only `status='confirmed'`/public joins reach process-global path; cache key carries `:confirmed` | unit | `pytest tests/test_joins_anc05_rls.py -x` | ❌ W0 | ⬜ pending |
| 118-NN-NN | NN | — | ANC-04 | — | N/A | unit | `pytest tests/test_known_joins_group.py -x` | ❌ W0 | ⬜ pending |
| 118-NN-NN | NN | — | BLD-02 | — | N/A | unit | `pytest tests/test_other_side_page_contract.py -x` | ❌ W0 | ⬜ pending |
| 118-NN-NN | NN | — | BLD-03 | — | N/A | unit | `pytest tests/test_builder_modifier_hoist.py -x` | ❌ W0 | ⬜ pending |
| 118-NN-NN | NN | — | BLD-04 | — | N/A | unit | `pytest tests/test_merge_globals_web.py -x` | ❌ W0 | ⬜ pending |
| 118-NN-NN | NN | — | FND-04/05 | — | off-loop discipline preserved | static/AST | `pytest tests/test_joins_lab_off_loop.py -x` | ✅ existing | ⬜ pending |
| 118-NN-NN | NN | — | ANC-05/FND | T-118-02 | no raw `app.storage.user` access | static/AST | `pytest tests/test_no_raw_storage_access.py -x` | ✅ existing | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

**Success Criterion 5 (ROADMAP):** a test enables each global toggle and asserts the executed query
string changes — covered by `test_merge_globals_web.py` (`flex_spacing` + `bidirectional` re-injection
into BOTH anchor `ro` and other-side `b_ro`; `ja` stays False; `variants` flows via `SideQuery.variants`).

---

## Wave 0 Requirements

Scaffold these test stubs (RED) before implementation:

- [ ] `tests/test_other_side_page_contract.py` — BLD-02: `p_num` (1-based) not `internal_index`; `total_pages=None` for metadata-only; `total_pages=0` treated as unknown; multi-IE `volume_ie` scoping; Genizah dense `page ± 1`.
- [ ] `tests/test_merge_globals_web.py` — BLD-04: `_merge_globals_web` mutates `ro` correctly; does NOT merge `ja`; `variants` unchanged; applied to BOTH anchor and other-side `ro`.
- [ ] `tests/test_builder_modifier_hoist.py` — BLD-03: each modifier type → correct term; line_start/line_end flow through `compose()`; multi-token wildcard handling.
- [ ] `tests/test_known_joins_group.py` — ANC-04: source attribution badges, dedup, connected-group membership.
- [ ] `tests/test_joins_anc05_rls.py` — ANC-05: confirmed-only filter; cache-key separation (`:confirmed` vs all); no cross-user cache poisoning.
- [ ] **Wave 0 SQL probe (open question 1):** confirm `fragment_joins.status` column exists in the live Supabase schema before relying on a `status='confirmed'` filter.

*Existing infrastructure (`test_joins_lab_off_loop.py`, `test_no_raw_storage_access.py`) must stay green throughout.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Builder auto-collapses to a readable summary bar on search; re-expands via Edit (D-14) | BLD-03 | Visual/interaction state, RTL layout | Load `/joins-lab?sys_id=…`, build ≥2 lines, run search, confirm summary bar + Edit affordance |
| Joins icon color hints joins-present vs none on `/search` cards + recolored `/browse` button (D-19/D-21) | FND-04/FND-05 | Color/glyph rendering | Open `/search` + `/browse` on a fragment with joins and one without; confirm distinct color + tooltip |
| "Find joins" opens `/joins-lab` in a NEW tab pre-loaded on the fragment (D-18) | FND-04/FND-05 | New-tab navigation | Click from card / browse / Quick View; confirm new tab, correct anchor, original context preserved |
| Bilingual `tr()` strings render in Hebrew (RTL) and English | all | i18n/RTL visual | Toggle language; confirm every new 118 string is translated |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references (5 new test files + schema probe)
- [ ] No watch-mode flags
- [ ] Feedback latency < 15s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
