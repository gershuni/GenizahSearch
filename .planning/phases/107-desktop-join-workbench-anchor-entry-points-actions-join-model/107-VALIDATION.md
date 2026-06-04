---
phase: 107
slug: desktop-join-workbench-anchor-entry-points-actions-join-model
status: approved
nyquist_compliant: true
wave_0_complete: false
created: 2026-06-04
---

# Phase 107 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Derived from 107-RESEARCH.md § "Validation Architecture".

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (existing project suite) |
| **Config file** | implicit (project root; no `pytest.ini` required) |
| **Quick run command** | `pytest tests/test_join_workbench.py -x` |
| **Full suite command** | `pytest tests/ -x` |
| **Estimated runtime** | ~5 seconds (quick / unit-only) |

**PyQt6 testing reality:** `JoinWorkbenchWindow` is a `QDialog` with QThread workers. Full UI tests
need a `QApplication` + event loop (not available headlessly without `pytest-qt`). Two tiers:
- **Tier 1 (automated, no QApplication):** pure helpers (`iiif_full`, `meta_brief`, `htmlify`),
  source-badge mapping, known-joins dedup/merge logic, and AST guards (no `_vs_*` on workbench path;
  every `tr()` key present in TRANSLATIONS).
- **Tier 2 (manual smoke on Windows):** window open, anchor image+text, folio nav, zoom, Add-as-Join,
  re-anchor, cold start, Hebrew mode, dark mode. Listed under Manual-Only Verifications.

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/test_join_workbench.py -x` (Tier 1 unit + AST, < 5s)
- **After every plan wave:** Run `pytest tests/ -x` (full suite)
- **Before `/gsd-verify-work`:** Full suite green + Tier 2 manual smoke checklist passed on Windows
- **Max feedback latency:** 5 seconds (per-task)

---

## Per-Task Verification Map

Task IDs are assigned during planning; this requirement→test map seeds the executor's per-task rows.
Tier-2 (smoke) requirements have no automated command and are validated via the Manual-Only table.

| Requirement | Behavior | Test Type | Automated Command | File Exists | Status |
|-------------|----------|-----------|-------------------|-------------|--------|
| JWB-01 | `JoinWorkbenchWindow` opens modeless, single-instance re-anchor (D-01/D-02) | Smoke (manual) | — | ❌ W0 | ⬜ pending |
| JWB-02 | Three entry hooks build the correct `anchor_result` dict (D-03) | Unit + smoke | `pytest tests/test_join_workbench.py::test_entry_hook_anchor_dicts -x` | ❌ W0 | ⬜ pending |
| JWB-03 | Anchor pane image route resolves `iiif_full` URL (D-05); text via `apply_line_numbered_text` (D-06) | Unit (`iiif_full`) + smoke | `pytest tests/test_join_workbench.py::test_iiif_full -x` | ❌ W0 | ⬜ pending |
| JWB-04 | Known-joins panel: correct count, per-row source badge from join `source` field across all four sources (D-08/D-09) | Unit (badge + merge) + smoke | `pytest tests/test_join_workbench.py::test_source_badge_mapping -x` | ❌ W0 | ⬜ pending |
| JWB-09 | Add-as-Join opens `JoinsDialog` anchor-only (Fragment A pre-filled, B empty); group refreshes after close (D-14, SC#4) | Smoke (manual) | — | ❌ W0 | ⬜ pending |
| SC#3 | Known-joins group includes user + PGP + FJMS + community puzzle joins | Unit (merge) + smoke | `pytest tests/test_join_workbench.py::TestDedupJoinRows -x` | ❌ W0 | ⬜ pending |
| SC#5 | No `_vs_*` private calls on `join_workbench.py` path (D-12) | AST guard | `pytest tests/test_join_workbench_no_private.py -x` | ❌ W0 | ⬜ pending |
| SC#6 | Every `tr()` key in `join_workbench.py` present in TRANSLATIONS (D-16) | AST guard | `pytest tests/test_join_workbench_i18n.py -x` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_join_workbench.py` — Tier-1 helpers: `iiif_full`, `meta_brief`, `htmlify`, source-badge mapping (join `source` → label+color, incl. community→green), known-joins merge/dedup across user/PGP/FJMS/community
- [ ] `tests/test_join_workbench_no_private.py` — AST guard: no `_vs_*` attribute access in `join_workbench.py` (SC#5)
- [ ] `tests/test_join_workbench_i18n.py` — AST guard: every `tr("…")` literal in `join_workbench.py` resolves to a TRANSLATIONS key (SC#6)
- [ ] Manual smoke checklist captured for `/gsd-verify-work` (see Manual-Only Verifications)

*New test files because Phase 107 introduces a new module (`desktop/join_workbench.py`); the helpers
are written test-first so they are importable without a `QApplication`. All three Wave-0 files are
planned in Plan-01 (Tasks 3 + 4); `wave_0_complete` flips true once they are executed and green.*

---

## i18n Closed-Set Invariant (single bootstrap)

Every `tr("…")` literal used anywhere in Phase 107 is bootstrapped in ONE place — Plan-01 Task-1's
`TRANSLATIONS.update({...})` block. No later task adds an i18n key, and `genizah_translations.py`
appears in NO plan's `files_modified` except Plan-01. This guarantees the i18n AST guard
(`tests/test_join_workbench_i18n.py`) is green at every task boundary, not just at the end. The
closed set: `Find joins`, `Join Workbench`, `ANCHOR`, `Known Joins`, `Make anchor`, `Add as Join`,
`Known join`, `Open fragment`, `Enter shelfmark…`, `img`, `Could not load joins. Click to retry.`,
`No manuscript found for '{}'` (plus pre-existing reused keys: `Browse manuscript`, `Add to Puzzle`,
`Add to List`, `User`, `Community`, `No image`).

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Window opens modeless + single-instance re-anchor | JWB-01 | Needs `QApplication` + live window manager | From ResultDialog click "Find joins" → window appears; click again on a different fragment → same window re-anchors (no 2nd window), raises to front |
| Anchor image + numbered transcription render | JWB-03 | Network IIIF fetch + Qt paint | Anchor image loads within ~10s; transcription shows RTL line-numbered gutter; "(no image)" placeholder on failure |
| Folio prev/next pages the same fragment | JWB-03 (D-07) | Qt widget state | Click next/prev → image+text update; folio counter `idx/total`; anchor sys_id and known-joins panel do NOT reload |
| Zoom +/- | JWB-03 | Qt pixmap rescale | Click + → image scales up (no refetch); − scales down; bounds 0.25×–4.0× |
| Known-joins group includes community joins | JWB-04 / SC#3 | Supabase fetch + Qt render | A fragment with a published community puzzle join shows that join in the panel with the green community badge |
| Add-as-Join anchor-only open + refresh | JWB-09 | Modal `JoinsDialog` + Supabase write | Click "Add as Join" → JoinsDialog opens with Fragment A pre-filled, B empty; create a join → on close, known-joins group includes the new join |
| Re-anchor from a known-join row | JWB-04 (D-15) | Qt widget state | Click "make anchor" on a known-join row → workbench re-anchors to that fragment and reloads ITS known joins |
| Cold start by shelfmark | JWB-02 | Resolver + Qt input | Enter a valid shelfmark → workbench opens with that anchor; ambiguous → picker; no match → warning |
| Hebrew mode coverage | SC#6 | Runtime `lang=he` render | With `CURRENT_LANG='he'`, every workbench label/tooltip renders in Hebrew; no English visible |
| Dark mode coverage | JWB-03 | OS palette render | Under a dark window palette, teal accent + border-only buttons + loading placeholders remain legible |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies (Tier-2 behaviors mapped to Manual-Only table)
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references (3 new test files above, planned in Plan-01 Tasks 3+4)
- [x] No watch-mode flags
- [x] Feedback latency < 5s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** approved (planning sign-off; `wave_0_complete` flips true during execution once the 3 Wave-0 test files run green).
