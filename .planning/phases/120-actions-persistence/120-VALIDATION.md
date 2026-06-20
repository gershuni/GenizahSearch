---
phase: 120
slug: actions-persistence
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-06-20
---

# Phase 120 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Derived from `120-RESEARCH.md` § Validation Architecture (all behaviors code-verified, HIGH confidence).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest |
| **Config file** | none — env-gated: `PYTHONUTF8=1 GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen` |
| **Quick run command** | `PYTHONUTF8=1 GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_joins_lab.py tests/test_joins_lab_storage.py tests/test_no_raw_storage_access.py -q --tb=short` |
| **Full suite command** | `PYTHONUTF8=1 GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/render_smoke/test_joins_lab_render_smoke.py tests/test_joins_lab.py tests/test_joins_lab_off_loop.py tests/test_joins_lab_page.py tests/test_joins_lab_storage.py tests/test_candidate_grid.py tests/test_candidate_surface.py tests/test_candidate_triage.py tests/test_candidate_filters.py tests/test_compare_modal.py tests/test_anchor_viewer.py tests/test_no_raw_storage_access.py tests/test_no_server_side_stop_propagation.py -q --tb=short` |
| **Estimated runtime** | ~60–90 seconds (targeted files only) |

> **Project testing reality (CLAUDE.md / [[feedback_full_suite_testing_windows]]):** bare `pytest tests/`
> aborts on Windows (Qt headless segfault; Tantivy per-worker OOM under `-n auto`). Run ONLY the targeted
> files above. Any NEW dialog/GUI test file must be added to `_GUI_TEST_FILES` in `conftest.py` for the
> CI marker-based `gui-tests` split. NiceGUI async render path is missed by plain headless pytest — the
> render-smoke file is the required live-client layer ([[feedback_nicegui_render_smoke_gap]]).

---

## Sampling Rate

- **After every task commit:** Run `{quick run command}` above
- **After every plan wave:** Run `{full suite command}` above
- **Before `/gsd:verify-work`:** Full suite must be green, PLUS the deferred-119 verification command
  (item 1 of `119-DEFERRED-VERIFICATION.md`, per D-21)
- **Max feedback latency:** ~90 seconds

---

## Per-Task Verification Map

> Task IDs are assigned by the planner; rows below are keyed by requirement + behavior. The planner MUST
> attach each behavior to a task's `<acceptance_criteria>` and the executor wires it to the command shown.
> All target test files already EXIST (✅) — new behaviors are added as new test *functions*, not new files.

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| TBD | TBD | TBD | ACT-01 | T-120-cache | `create_fragment_join` leaves status `'proposed'` (NOT `'confirmed'`); Lab known-joins fetch uses `confirmed_only=False` so the proposed join shows (parity with `/browse`) | unit | `pytest tests/test_joins_lab.py -k "test_add_join"` | ✅ | ⬜ pending |
| TBD | TBD | TBD | ACT-01 | V2 auth | Anonymous user sees login gate, not the write | render-smoke | `pytest tests/render_smoke/test_joins_lab_render_smoke.py -k "test_anon_add_join_gate"` | ✅ | ⬜ pending |
| TBD | TBD | TBD | ACT-01 | — | New join appears in known-joins after `force_refresh=True` | unit | `pytest tests/test_joins_lab.py -k "test_add_join_visible"` | ✅ | ⬜ pending |
| TBD | TBD | TBD | ACT-01 (D-03) | V4 access | "Remove my join" deletes own join only (self-scope; RLS) | unit | `pytest tests/test_joins_lab.py -k "test_remove_own_join"` | ✅ | ⬜ pending |
| TBD | TBD | TBD | ACT-02 | T-120-stale | `puzzle_staging` key written + cleared one-shot on puzzle load | unit | `pytest tests/test_joins_lab.py -k "test_bulk_puzzle_staging"` | ✅ | ⬜ pending |
| TBD | TBD | TBD | ACT-02 | V5 input | Anchor always included; candidate list capped at 20 | unit | `pytest tests/test_joins_lab.py -k "test_bulk_anchor_always_included"` | ✅ | ⬜ pending |
| TBD | TBD | TBD | ACT-03 | — | Export CSV has triage verdict + capped text column (10 cols) | unit | `pytest tests/test_joins_lab.py -k "test_export_csv_columns"` | ✅ | ⬜ pending |
| TBD | TBD | TBD | ACT-03 | — | Export text = matched page (text hits) / first text page (VS-only) | unit | `pytest tests/test_joins_lab.py -k "test_export_text_page_selection"` | ✅ | ⬜ pending |
| TBD | TBD | TBD | ACT-03 | V2 auth | Add-to-List login gate for anonymous | render-smoke | `pytest tests/render_smoke/test_joins_lab_render_smoke.py -k "test_anon_add_list_gate"` | ✅ | ⬜ pending |
| TBD | TBD | TBD | PST-01 | T-120-blob | Builder/triage state serializes with NO result blobs (`full_text` absent) | unit | `pytest tests/test_joins_lab_storage.py -k "test_write_full_state_no_blobs"` | ✅ | ⬜ pending |
| TBD | TBD | TBD | PST-01 | — | Restore re-runs search from persisted inputs (not a stored blob) | unit | `pytest tests/test_joins_lab_page.py -k "test_restore_reruns_search"` | ✅ | ⬜ pending |
| TBD | TBD | TBD | PST-02 | V3 session | All storage writes go through `safe_user_*` (allowlist `[]`) | static AST | `pytest tests/test_no_raw_storage_access.py` | ✅ | ⬜ pending |
| TBD | TBD | TBD | PST-03 | — | `clear_joins_lab_state()` leaves `safe_user_get('joins_lab')` == None | unit | `pytest tests/test_joins_lab_storage.py -k "test_clear_leaves_empty"` | ✅ | ⬜ pending |
| TBD | TBD | TBD | D-11 | — | Stop applies partials; superseded run still discards | unit | `pytest tests/test_joins_lab.py -k "test_stop_applies_partials"` | ✅ | ⬜ pending |
| TBD | TBD | TBD | D-18 | V2 auth | Sign-in opens `create_login_dialog()` (no `/settings` navigate) | static/unit | `pytest tests/test_joins_lab.py -k "test_signin_opens_dialog"` | ✅ | ⬜ pending |
| TBD | TBD | TBD | D-20 / SEED-008 | — | Fire-and-forget tasks swallow `RuntimeError` on client-deleted | render-smoke | `pytest tests/render_smoke/test_joins_lab_render_smoke.py -k "test_client_deleted_guard"` | ✅ | ⬜ pending |
| TBD | TBD | TBD | D-20 / SEED-008 | — | `_load_known_joins` does not propagate `RuntimeError` | unit | `pytest tests/test_joins_lab.py -k "test_load_known_joins_client_deleted"` | ✅ | ⬜ pending |
| TBD | TBD | TBD | (off-loop) | V5/SSRF | New search/VS/export/prefetch stay off the event loop | static AST | `pytest tests/test_joins_lab_off_loop.py` | ✅ | ⬜ pending |
| TBD | TBD | TBD | (D-11 server-prop) | Tampering | No Python-side `e.stop_propagation()` introduced | static AST | `pytest tests/test_no_server_side_stop_propagation.py` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

All target test files already exist — **no new framework, no new test files required**. Wave 0 work is
limited to adding new test *functions* to the existing files as each behavior is implemented:

- [ ] `tests/test_joins_lab_storage.py` — add PST write/read/clear cases (file exists; extend it)
- [ ] `tests/render_smoke/test_joins_lab_render_smoke.py` — add SEED-008 client-deleted guard + the two login-gate cases (file + conftest exist; extend it)
- [ ] `tests/test_joins_lab.py` / `tests/test_joins_lab_page.py` — add ACT/D-11/D-18 unit cases (files exist; extend them)

*No `_GUI_TEST_FILES` conftest change needed — all target files are already registered.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Live "added join shows to everyone immediately" (SC#1) on real Supabase | ACT-01 | Requires real Supabase auth + RLS round-trip; headless mocks can't prove prod parity | Log in on a staging/prod build, Add-as-Join on a candidate, confirm it appears in the known-joins group after refresh AND in `/browse` for another user |
| Deferred Phase-119 HUMAN-UAT re-runs (R2-3 / R2-6 / R2-8 / R2-10) on real corpus | D-21 | Needs real corpus data + human judgment of result quality | Per `119-DEFERRED-VERIFICATION.md`; run during 120 close-out |
| Compare image prefetch makes flip-through feel instant (D-10) | (SEED-007) | Perceptual latency; no deterministic signal | Open Compare, flip next/prev rapidly, confirm adjacent images are already loaded |
| Bilingual (HE) copy renders RTL correctly for new strings | FND-07 | Visual RTL correctness | Switch UI to Hebrew, verify each new affordance label/tooltip/dialog renders RTL with no English leak (full audit deferred to Phase 121) |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references (none — all files exist)
- [ ] No watch-mode flags
- [ ] Feedback latency < 90s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
