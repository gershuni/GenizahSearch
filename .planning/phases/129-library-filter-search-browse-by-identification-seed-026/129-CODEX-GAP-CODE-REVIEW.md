---
phase: 129-library-filter-search-browse-by-identification-seed-026
gate: codex-code-review (gap-closure execution, plans 129-05/06/07)
reviewer: codex (gpt-5.x via codex exec)
reviewed: 2026-06-28
verdict: APPROVE (converged round 2; round 1 APPROVE WITH CHANGES)
findings: { blocker: 0, high: 0, medium: 2, low: 0 }
status: resolved
---

# Phase 129 Gap-Closure — Codex CODE Review (cross-AI gate)

Independent Codex review of the EXECUTED gap-closure diff (menu/dropdown → checkbox-dialog
redesign + "search within results" wiring), run alongside the internal gsd-code-review.

**Round 1: APPROVE WITH CHANGES** — 0 BLOCKER/HIGH. Codex confirmed the central invariant
(all-unchecked never collapses to `[]`/"show all") holds on all three surfaces with layered
guards, safe_storage used for new state, and new visible strings translated. Two MEDIUM findings
(both fixed; they overlapped with internal findings WR-01..WR-04):

- **MEDIUM (filter_panel.py)** — `consume_incoming_filters` persisted the incoming library filter
  under the flat `search_library_filter` key for BOTH the `search` and `parallels` prefixes, so a
  catalog→parallels handoff silently altered the next `/search` page. → Fixed `5d8c7e39`: persist
  gated on `storage_prefix == 'search'`; dead `try/except AttributeError` removed.
- **MEDIUM (genizah_app.py)** — after a desktop catalog→search "search in these results" with a
  library restriction, `pre_search_filters['library']` was active but not rendered as a removable
  chip on the search side (invisible/unremovable scope). → Fixed `120b756a`: per-code removable
  chips keyed `('library', code)` in the filter chip bar, wired to the existing recompute.

Internal review (`129-GAP-REVIEW.md`) additionally fixed: WR-02 desktop empty-resolution now
fail-OPEN to match the data layer (`8d3c6481`), WR-03 re-added `LIBRARY_CODES` validation in the
catalog dialog Apply (`10b25819`), WR-04 documented the JS disable as cosmetic / Python guard
authoritative (`165ad935`), and IN-01/02 removed an orphaned i18n key + a no-op shim (`8fa4a104`).

## Round 2 (convergence confirmation): VERDICT APPROVE — no findings
Codex re-reviewed the fix diff and confirmed: (a) the parallels→search persistence leak is closed;
(b) desktop library chips are per-code removable and wired to the recompute; (c) fail-open is
consistent across search-within, parallels-within, and `FilterCountWorker`; (d) no new bug
introduced. 64 targeted + GUARD-02 regression tests pass; ruff clean. Gate satisfied.
