---
phase: 120-actions-persistence
verified: 2026-06-21T10:41:00Z
status: passed
score: 5/5 goal elements verified (the 1 WARNING was fixed same-day — commit 3fa79cb5)
overrides_applied: 0
resolution: >-
  The single WARNING (the _cap_triage all-decided edge case) was FIXED immediately
  after verification in commit 3fa79cb5: `_cap_triage` now guards `remaining_slots > 0`
  before slicing the undecided tail, so the persisted triage stays ≤ 500 in every
  case (including ≥500 decided yes/no). Regression test added:
  `tests/test_joins_lab_storage.py::test_triage_capped_at_500_all_decided`
  (600 decided + 50 maybe → ≤500). Storage suite 27 passed. Gap below kept for record.
gaps:
  - truth: "Persisted payload size cap is enforced (PST-02 / PST-05 size discipline)"
    status: resolved
    reason: >-
      The triage size cap (_cap_triage) is correctly enforced for all realistic cases
      (mixed verdicts, all-maybe, fewer than 500 decided). But when the number of
      DECIDED (yes/no) verdicts is >= 500, remaining_slots becomes 0 and the slice
      `undecided_values[-0:]` evaluates to `list[0:]` — the WHOLE undecided list —
      so the dict is persisted UNCAPPED (e.g. 1000 entries instead of 500). The leak
      is bounded in bytes (sys_id key + short verdict ~30 bytes/entry, so ~30-60 KB
      even at 1000 entries; NOT a result/full_text blob), requires a scholar to have
      explicitly triaged 500+ candidates Yes/No on a single anchor, and does not
      affect restore correctness or cross-session isolation. The existing test
      `test_triage_capped_at_500` does not catch it (it uses only 100 decided).
    artifacts:
      - path: "web/joins_lab_storage.py"
        issue: >-
          _cap_triage() lines ~191-195: `remaining_slots = _MAX_TRIAGE_ENTRIES - len(result)`
          can be 0; `undecided_values[-remaining_slots:]` is `[-0:]` == whole list.
    missing:
      - "Guard remaining_slots <= 0 -> append nothing (e.g. `undecided_values[-remaining_slots:] if remaining_slots > 0 else []`)"
      - "Add a regression test with >=500 decided verdicts asserting len(stored_triage) == 500"
---

# Phase 120: Actions & Persistence — Verification Report

**Phase Goal:** Scholars can act on confirmed candidates (add-as-join, add anchor+candidates to the Puzzle, add-to-list / export) and find their builder inputs, triage, filters, and view restored on return — search re-run from persisted inputs — with a clear/reset escape hatch.
**Verified:** 2026-06-21T10:41:00Z
**Status:** gaps_found (1 WARNING — non-blocking size-cap edge case)
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths (ROADMAP Success Criteria + folded SEED-007/Lists items)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| SC1 | Add-as-Join is login-gated; anon sees login prompt; after login the join appears in known-joins | ✓ VERIFIED | `joins_lab.py:1648` `_on_add_as_join_click` — anon path persists pending descriptor + `create_login_dialog().open()`; logged-in path calls `create_fragment_join` (no status → 'proposed') then `_load_known_joins(force_refresh=True, confirmed_only=False)`; replay via `_replay_pending_action` (`:4042`, TTL + anchor-match guards). |
| SC2 | Add-to-Puzzle sends anchor + all selected candidates via NEW multi-fragment bulk staging; anchor always included | ✓ VERIFIED | `joins_lab.py:1838` writes `puzzle_staging` (`fragments=[anchor]+capped`, schema_version=1, ≤21); `puzzle.py:3921` pops one-shot, validates, schedules `auto_add_bulk` adding anchor (index 0) first sequentially. |
| SC3 | Add-to-List (login-gated cloud write) + Export of candidate set (CSV/XLSX with shelfmark/library/triage/metadata) | ✓ VERIFIED | Add-to-List `:1905` login gate → list picker → `add_list_item` off-loop; Export `:2058` `_export_candidates` produces 10-col CSV (utf-8-sig) / XLSX (openpyxl) over the FULL filtered set (≤500), off-loop batched text via `run.io_bound(fetch_export_text_batch)`, cancel flag, `ui.download`. |
| SC4 | After refresh, anchor + builder inputs + triage + filter + view restored; search re-run from persisted INPUTS (not stale blob) | ✓ VERIFIED | `_bootstrap_anchor` stored branch (`:3871-4019`): inputs restored from `read_full_state()`; results restored from a separately-versioned bounded snapshot (`read_results_snapshot`) when anchor-matched, else `execute_joins_search()` re-run fallback; triage re-attached by sys_id; Step 9b re-persists full state. Inputs ALWAYS drive restore; no result blob in the main `joins_lab` key. |
| SC5 | Server-side per-session safe_storage; versioned schema; no full_text/image/result blobs (size cap); no cross-session leak; Clear/Reset wipes state | ✓ VERIFIED (cap edge-case → WARNING) | `joins_lab_storage.write_full_state` whitelists named params only (`schema_version=1`); all I/O via `safe_user_*`; `clear_joins_lab_state()` pops joins_lab + puzzle_staging + results snapshot; Reset control `:1315` → `clear_joins_lab_state()`. `test_no_raw_storage_access` allowlist stays []. Size cap WARNING: `_cap_triage` leaks when ≥500 decided (see gaps). |
| D-07 | Set-as-Anchor pivots workbench (triage resets on re-anchor) | ✓ VERIFIED | `_on_set_as_anchor` `:766` → `load_anchor` (clears `_triage`); wired into grid (`candidate_grid.py:896`). |
| D-08 | Browse-in-Compare per pane (new tab) | ✓ VERIFIED | `compare_modal.py:426/683` `build_browse_url` + js_handler new-tab open for candidate + anchor panes. |
| D-09 | Compare FJMS + PGP/bib info buttons fed by PREFETCHED off-loop metadata; disabled when no data | ✓ VERIFIED | `compare_modal.py:843` `_build_info_buttons` consumes prefetched `catalog_detail` + `fjms_bib`; `_metadata_prefetcher_sync` (`joins_lab.py:775`) dispatched off-loop; `show_catalog_dialog` passed prefetched detail (no sync fetch on open). |
| D-10 | Adjacent candidate images prefetched off-loop (bounded, proxy-only) | ✓ VERIFIED | `compare_modal.py:514` schedules prefetch on flip via `metadata_prefetcher`/image resolver; off-loop guard test green. |
| D-11 | Stop-with-partials on explicit user stop; superseded run discards | ✓ VERIFIED | `_stop_requested` flag (`:551`); Stop button `:3268`; partials applied without bumping generation; `test_stop_applies_partials` green. |
| D-12 | VS toggle HIDDEN (not just disabled) when anchor has zero VS data | ✓ VERIFIED | Off-loop probe `svc.get_suggestions(anchor_sid, 1)` (`:2540`); `set_visibility(False)` on empty, generation-guarded. |
| D-17 | Logged-in user picks a fragment from saved lists, loads as anchor (picker) | ✓ VERIFIED | `_on_lists_btn_click` `:2803` two-level drill-down picker; fragment click → `load_anchor(sid)` (`:3049`). Plan 120-08 checkbox is `[ ]` but the code is present and tested (implemented during live UAT). |
| D-18 | Anonymous on choose-from-lists sees in-page login flow | ✓ VERIFIED | `_on_lists_btn_click` anon branch → `create_login_dialog().open()` (in-page overlay, replaced /settings navigate). |
| D-19 | /lists item has "Open in Joins Lab" link between Browse and Add-to-Puzzle | ✓ VERIFIED | `lists.py:702` new-tab deep link `/joins-lab?sys_id={sid}` (icon=link, between Browse and Puzzle). Minor cosmetic deviation from spec'd `icon=science` — not a gap. |
| D-20 | Fire-and-forget UI mutators swallow RuntimeError on client-deleted | ✓ VERIFIED | `except RuntimeError: return` guards throughout (add-join, remove, export, list-picker); `test_load_known_joins_client_deleted` green. |

**Score:** 5/5 ROADMAP success criteria verified + all 10 folded SEED-007/Lists items verified. 1 WARNING on the SC5 size-cap edge case.

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `web/joins_lab_storage.py` | write/read_full_state + size caps + extended clear + results snapshot | ✓ VERIFIED | 426 lines; schema_version=1 kept; explicit-whitelist payload; `persist/read/clear_results_snapshot` (bounded: 300 cands, 500-char full_text cap). Caps work except triage ≥500-decided edge (WARNING). |
| `web/pages/joins_lab.py` | All actions + persistence + restore + Clear/Reset + lists picker | ✓ VERIFIED | 4181 lines; every goal element wired (see truths). |
| `web/pages/puzzle.py` | one-shot pop + validate puzzle_staging + deferred bulk add | ✓ VERIFIED | `:3921` synchronous one-shot pop, schema check, `auto_add_bulk` sequential. |
| `web/components/candidate_grid.py` | triage persist hook + selection + bulk actions + Set-as-Anchor | ✓ VERIFIED | 1383 lines; all callbacks present (`on_triage_change`, `on_selection_change`, `on_set_as_anchor`, `on_add_*`). |
| `web/components/compare_modal.py` | Browse/info/prefetch/add actions, viewport-bound card | ✓ VERIFIED | 1083 lines; D-08/D-09/D-10 + Add-as-Join/Add-to-Puzzle wired. |
| `web/components/joins_builder.py` | get_state/set_state round-trip | ✓ VERIFIED | 1151 lines; used by `_persist_state` / `_bootstrap_anchor`. |
| `web/pages/lists.py` | Open-in-Joins-Lab button | ✓ VERIFIED | `:702`. |

### Key Link Verification

| From | To | Via | Status |
|------|----|----|--------|
| joins_lab `_persist_state` | joins_builder `get_state` | snapshot inputs for blob | ✓ WIRED (`:3785`) |
| joins_lab `_bootstrap_anchor` | `read_full_state` + builder `set_state` | restore inputs → re-run/snapshot | ✓ WIRED (`:3873`) |
| joins_lab Reset | `clear_joins_lab_state` | confirm dialog | ✓ WIRED (`:1334`) |
| joins_lab | `create_fragment_join` | login-gated, no status kwarg | ✓ WIRED (`:1734`) |
| joins_lab | puzzle `create_puzzle_page` | `safe_user_set('puzzle_staging')` → navigate | ✓ WIRED (`:1868` → puzzle `:3921`) |
| joins_lab export | `executor.get_browse_page` | off-loop batched text | ✓ WIRED (`:2130`) |
| joins_lab add-to-list | `add_list_item` | login-gated per candidate | ✓ WIRED (`:2010`) |
| joins_lab picker | `load_anchor` | fragment click loads anchor | ✓ WIRED (`:3049`) |
| joins_lab D-12 probe | `get_vs_service().get_suggestions` | off-loop hide-probe | ✓ WIRED (`:2540`) |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|---------------|--------|--------------------|--------|
| Restore (SC4) | builder inputs / toggles / filter / view / triage | `read_full_state()` (persisted INPUTS, no blobs) | Yes — inputs drive set_state + re-run/snapshot | ✓ FLOWING |
| Restore results | `_raw_text_candidates` / `_vs_candidates` | `read_results_snapshot()` (anchor-matched, bounded) OR `execute_joins_search()` fallback | Yes — instant snapshot or fresh re-run | ✓ FLOWING |
| Export | CSV/XLSX rows | live `_filtered_candidates` + `_enrichment` + off-loop `get_browse_page` text | Yes — real candidate + transcription data | ✓ FLOWING |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| Storage module API + blob discipline | `python -c` import + signature/whitelist asserts | schema=1; no blob params; word-model preserved; term capped 200 | ✓ PASS |
| Triage cap (common cases) | `_cap_triage` mixed / all-maybe / <500 decided | all == 500 | ✓ PASS |
| Triage cap (≥500 decided boundary) | `_cap_triage` 600 decided + 400 maybe | returned 900 (cap violated) | ✗ FAIL → WARNING (see gaps) |

### Probe Execution

| Probe | Command | Result | Status |
|-------|---------|--------|--------|
| Phase test suites | `pytest test_joins_lab_storage/test_joins_lab/test_compare_modal/test_candidate_grid` | 363 passed | PASS |
| CI guards | `pytest test_no_raw_storage_access/test_joins_lab_off_loop/render_smoke` | 59 passed, 1 skipped | PASS |
| Remaining joins_lab suites | `pytest test_joins_lab_{enter,new_search,options,page,render,render_contract,search_types}` | 167 passed | PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| ACT-01 | 120-04 | Add-as-Join (login-gated community write) | ✓ SATISFIED | SC1 |
| ACT-02 | 120-05 | Bulk Add-to-Puzzle (multi-fragment staging) | ✓ SATISFIED | SC2 |
| ACT-03 | 120-06, 120-08 | Add-to-List + Export | ✓ SATISFIED | SC3 |
| PST-01 | 120-01/03 | Persist inputs/triage/filter/view, restore | ✓ SATISFIED | SC4 (snapshot-first + re-run fallback) |
| PST-02 | 120-01/03 | Server-side per-session, versioned, capped, isolated | ✓ SATISFIED (cap edge → WARNING) | SC5 |
| PST-03 | 120-01/03 | Clear/Reset | ✓ SATISFIED | SC5 / Reset |

No orphaned requirements — REQUIREMENTS.md maps exactly ACT-01..03 + PST-01..03 to Phase 120, all claimed in plans.

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| (none) | — | No unreferenced TBD/FIXME/XXX in modified files | — | — |
| joins_lab.py | 2811 | comment: picker "replaces the old Go-to-Lists placeholder" | ℹ️ Info | Confirms the D-17 picker replaced a prior stub — not a current stub. |

### Persistence-Approach Evolution Assessment (task focus)

The plan/REQUIREMENTS PST-01 wording said "search re-run on restore." The shipped implementation
is **snapshot-first restore, re-run fallback** + a separate per-user bounded results snapshot.
**This is a faithful (and superior) realization, NOT a gap:**
- Builder INPUTS, toggles, filter, view, and triage are always restored from the persisted INPUTS
  blob (`read_full_state`) — never from a result blob. The main `joins_lab` key still contains
  zero `full_text`/image/result data (blob discipline enforced by explicit-whitelist payload).
- The results snapshot is a SEPARATE, independently-versioned, strictly-bounded key (≤300
  candidates, 500-char full_text cap) — explicitly NOT the 778 MB search-history-bloat pattern.
- Snapshot restore is anchor-keyed and validated; on any mismatch/deserialise error it falls back
  to the original `execute_joins_search()` re-run path. So the goal ("not a stale result blob;
  re-run from persisted inputs") is preserved, with an instant-restore optimization layered on top
  that fixes the round-5 UAT defect (results vanishing on a Browse/Add-to-Puzzle round-trip).

### Gaps Summary

One non-blocking WARNING: `_cap_triage()` fails to cap when a single anchor session accumulates
≥500 DECIDED (yes/no) verdicts — the `undecided_values[-remaining_slots:]` slice degenerates to
`[-0:]` (the whole undecided list) when `remaining_slots == 0`. The persisted triage dict is then
uncapped. Impact is bounded: triage entries are tiny (sys_id + short verdict, ~30 bytes), so even
1000 entries ≈ 30-60 KB — far from the result-blob class of bug the cap exists to prevent — and it
requires extreme power-user behavior (500+ explicit Yes/No on one anchor). It does not affect
restore correctness, cross-session isolation, or any of the 5 goal elements. Recommended fix:
guard `remaining_slots > 0` before slicing, plus a regression test at the ≥500-decided boundary.

**Verdict per goal element:** SC1 PASS · SC2 PASS · SC3 PASS · SC4 PASS · SC5 PASS (with size-cap
WARNING) · SEED-007 (D-07..D-12) PASS · Lists (D-17/18/19) PASS.

**Overall verdict:** Phase goal ACHIEVED. All 5 ROADMAP success criteria and all folded
SEED-007/Lists items are delivered in code, wired end-to-end, and covered by 589 passing tests
(363 + 59 + 167) plus CI guards (no-raw-storage allowlist [], off-loop, render-smoke). The single
finding is a narrow, byte-bounded size-cap edge case that does not block the goal — surfaced as a
WARNING for a quick follow-up fix.

---

_Verified: 2026-06-21T10:41:00Z_
_Verifier: Claude (gsd-verifier)_
