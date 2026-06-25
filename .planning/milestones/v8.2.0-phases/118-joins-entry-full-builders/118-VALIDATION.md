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
| 118-01-02 | 01 | 0 | ANC-05 | T-118-01 | Lab path passes `status='confirmed'` to `get_fragment_joins` (excludes unconfirmed `proposed` joins); cache key carries `:confirmed`; default path passes NO status filter; no cross-user poisoning | unit | `pytest tests/test_joins_anc05_rls.py -x` | ❌ W0 | ⬜ pending |
| 118-01-02 | 01 | 0 | ANC-04 | — | four-source group: user/PGP/FJMS dedup-merge + published community puzzle joins surface as `sources=['community']` on the Lab path | unit | `pytest tests/test_known_joins_group.py -x` | ❌ W0 | ⬜ pending |
| 118-01-01 | 01 | 0 | BLD-02 | — | `resolve_other_side_pages` p_num/None/0 rules + fake-executor `apply_cross_side` integration (p_num not internal_index, volume_ie passthrough, total_pages=0→None) | unit | `pytest tests/test_other_side_page_contract.py -x` | ❌ W0 | ⬜ pending |
| 118-01-01 | 01 | 0 | BLD-03 | — | N/A | unit | `pytest tests/test_builder_modifier_hoist.py -x` | ❌ W0 | ⬜ pending |
| 118-01-01 | 01 | 0 | BLD-04 | — | N/A | unit | `pytest tests/test_merge_globals_web.py -x` | ❌ W0 | ⬜ pending |
| 118-05-02 | 05 | 2 | FND-04/05 | T-118-06 | off-loop discipline preserved; per-card joins-count fetch via `run.io_bound` (no event-loop blocking) | static/AST | `pytest tests/test_joins_lab_off_loop.py -x` | ✅ existing | ⬜ pending |
| 118-NN-NN | NN | — | ANC-05/FND | T-118-02 | no raw `app.storage.user` access | static/AST | `pytest tests/test_no_raw_storage_access.py -x` | ✅ existing | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

**Success Criterion 5 (ROADMAP):** a test enables each global toggle and asserts the executed query
string changes — covered by `test_merge_globals_web.py` (`flex_spacing` + `bidirectional` re-injection
into BOTH anchor `ro` and other-side `b_ro`; `ja` stays False; `variants` flows via `SideQuery.variants`).

---

## Wave 0 Requirements

Scaffold these test stubs (RED) before implementation:

- [ ] `tests/test_other_side_page_contract.py` — BLD-02: `p_num` (1-based) not `internal_index`; `total_pages=None` for metadata-only; `total_pages=0` treated as unknown; multi-IE `volume_ie` scoping; Genizah dense `page ± 1`; PLUS a fake-executor integration test against `apply_cross_side` (`test_cross_side_uses_p_num_and_handles_metadata_only`) proving p_num/volume_ie/total_pages=0→None flow end-to-end (green now — exercises existing shared core).
- [ ] `tests/test_merge_globals_web.py` — BLD-04: `_merge_globals_web` mutates `ro` correctly; does NOT merge `ja`; `variants` unchanged; applied to BOTH anchor and other-side `ro`.
- [ ] `tests/test_builder_modifier_hoist.py` — BLD-03: each modifier type → correct term; line_start/line_end flow through `compose()`; multi-token wildcard handling.
- [ ] `tests/test_known_joins_group.py` — ANC-04: source attribution badges (user/PGP/FJMS), multi-source dedup-merge, AND the four-source completeness test `test_community_member_appears_in_lab_group` (published community puzzle joins surface as `sources=['community']` on the Lab/`confirmed_only` path — RED until Plan 02 wires the Lab community merge).
- [ ] `tests/test_joins_anc05_rls.py` — ANC-05: `status='confirmed'` query filter passed on the Lab path (exact value `'confirmed'`); default path passes NO status filter; cache-key separation (`:confirmed` vs unconfirmed); no cross-user cache poisoning.
- [ ] **Wave 0 schema probe (open question 1):** CONFIRM `fragment_joins.status` exists in the live Supabase deployment (it does per `supabase_setup.sql:162` + the `get_fragment_joins` signature); record `status='confirmed'` as the PRIMARY ANC-05 mechanism with a conditional app-layer fallback only if the live probe contradicts the canonical SQL.

*Existing infrastructure (`test_joins_lab_off_loop.py`, `test_no_raw_storage_access.py`) must stay green throughout.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Builder auto-collapses to a readable summary bar on search; re-expands via Edit (D-14) | BLD-03 | Visual/interaction state, RTL layout | Load `/joins-lab?sys_id=…`, build ≥2 lines, run search, confirm summary bar + Edit affordance |
| Known-joins group shows all four sources (PGP/FJMS/user/community) with distinct source badges; re-anchor reloads the pane (D-15/D-16) | ANC-04 | Color/glyph rendering + navigation | Load a fragment with PGP + community joins; confirm four-color badges; click a member's push_pin → anchor reloads |
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
