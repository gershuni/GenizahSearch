---
phase: 106-joins-lab-shared-core-pure-logic-service-adapters-unit-tests
verified: 2026-06-03T15:01:19Z
status: passed
score: 6/6 must-haves verified
overrides_applied: 0
---

# Phase 106: Joins Lab Shared Core Verification Report

**Phase Goal:** A new shared, web-reusable, unit-tested module encapsulates the validated Joins Lab domain logic — anchor/candidate identity, line-by-line query composition into the engine's line-break syntax, cross-side (sys_id, page±1) membership, candidate dedup/compaction, text/visual merge ordering with provenance, self-match detection, and snippet/page helpers — behind a SearchExecutor adapter and the existing shared services, with no PyQt and no direct fist_data/*.db access. No UI.
**Verified:** 2026-06-03T15:01:19Z
**Status:** passed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | A pure function composes a multi-row builder spec (per-row line START/END anchors + "↓ N lines" gaps) into the engine's line-break query (`\|` groups, `[\|N]` line-gaps), unit-tested for round-trips against `genizah_core._parse_line_break_query` including RTL line-start-on-right | ✓ VERIFIED | `compose()` in shared/joins_lab.py:686. `TestCompose` (12 tests) covers round-trip, leading-pipe RTL orientation, gap markers, page-anchor 3-tuple, ValueError guards, and all-empty returns-None. `test_multiline_round_trip` imports `_parse_line_break_query` directly and asserts `line_groups[0].line_start is True`, `line_groups[1].line_end is True`, `line_gaps[0] == 1`. `test_line_start_leading_pipe` asserts `"\|שהדותא" in qs`. All 12 pass. |
| 2 | A pure function resolves the "other side" page set (first→+1, last→−1, middle→both) and decides cross-side AND/OR candidate membership by `(sys_id, page±1)` set logic, unit-tested with AND-narrows / OR-widens fixtures | ✓ VERIFIED | `resolve_other_side_pages()` at line 283, `cross_side_membership()` at line 306, `apply_cross_side()` at line 344. `TestResolveOtherSide` (5 tests): first→{p+1}, last→{p-1}, middle→both, single-page→empty, total_pages=None. `TestCrossSide` (6 tests): AND-narrows verified fixture result=={('A',3)}, OR-widens, total-pages clamp, FakeSearchExecutor-based apply_cross_side tests verify corpus_scope='genizah' and via_other_side=True on synthesized neighbor. All 11 pass. |
| 3 | Candidate dedup/compaction collapses one-result-per-image via a canonical candidate key, unit-tested | ✓ VERIFIED | `dedup_candidates()` at line 469. Canonical key is `Candidate.key = (sys_id, page)` (VS-only→(sys_id, None)). `TestDedup` (6 tests): same-page collapse, distinct-pages kept, VS-uid key=(sid, None), via_text marked, anchor self excluded by default (anchor_matched flag), anchor self included with flag. Note: ROADMAP SC#3 says "side image / adjacent-side membership" — Plan 01/RESEARCH notes these are provenance attributes in the merged result, not key components; the canonical image-dedup key is (sys_id, page), and the tests verify this cleanly. All 6 pass. |
| 4 | The text/visual-similarity merge yields a stable both-first → text → VS-only ordering with provenance tags, unit-tested | ✓ VERIFIED | `merge_candidates()` at line 511. Uses `dataclasses.replace()` (frozen-safe). `TestMerge` (5 tests): text-only passthrough, VS-only, overlap annotated (via_text AND via_vs AND vs_rank), ordering (both→text→VS), both-tier sorts before text regardless of input order. All 5 pass. |
| 5 | Self-match detection plus the centered snippet/page helpers are unit-tested | ✓ VERIFIED | `detect_self_match()` at line 558 (sys_id membership, bracket-agnostic by construction per RESEARCH R-02). `_match_line()` at line 584. `snippet_html()` at line 641, `snippet_plain()` at line 663, `htmlify()` at line 614. `TestSelfMatch` (4 tests), `TestMatchLine` (6 tests), `TestSnippet` (6 tests). XSS mitigation via sentinel-before-escape ordering verified by `test_html_escapes`. All 16 pass. |
| 6 | A static import test proves the module imports with no PyQt symbols and opens no `fist_data/*.db` directly — all data flows through shared services or the SearchExecutor adapter | ✓ VERIFIED | `TestStaticImport` (5 tests): AST walk over Import/ImportFrom nodes asserts no name startswith ('PyQt6','PyQt5','PySide6'); no 'PyQt'/'PySide' substring; no 'fist_data' substring; no 'sqlite3.connect' substring; importlib.import_module("shared.joins_lab") succeeds. `python -c "import shared.joins_lab"` returns 0 with no engine init. All 5 pass. |

**Score:** 6/6 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `shared/joins_lab.py` | BuilderRow/SideQuery/Candidate/MergeResult frozen dataclasses, SearchExecutor Protocol, normalize_candidate(), page_of(), compose() | ✓ VERIFIED | 761 lines. 4 `@dataclass(frozen=True)` confirmed (grep count=4). All expected functions present. `import dataclasses`, `import html` present. `corpus_scope: str = "all"` in Protocol. Ruff clean (0 violations). |
| `tests/test_joins_lab.py` | TestCompose, TestPageOf, TestNormalize, TestStaticImport, TestResolveOtherSide, TestCrossSide, TestDedup, TestMerge, TestSelfMatch, TestMatchLine, TestSnippet | ✓ VERIFIED | 681 lines. All 11 test classes present. 66 tests, all passing (pytest -q: "66 passed in 0.29s"). |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `compose()` in shared/joins_lab.py | `genizah_core._parse_line_break_query` | Round-trip test imports _parse_line_break_query directly | ✓ WIRED | `test_multiline_round_trip` at tests/test_joins_lab.py:179 does `from genizah_core import _parse_line_break_query` and asserts parse(compose(rows)) reproduces line_start/line_end/gaps. Test passes. |
| `TestStaticImport` | `shared/joins_lab.py` source text | ast.parse + substring scan for 'fist_data' | ✓ WIRED | Tests read the file, parse AST, walk nodes, and assert no Qt/fist_data/sqlite3.connect. All 5 StaticImport tests pass. |
| `apply_cross_side()` | `SearchExecutor.execute_search / get_browse_page` | FakeSearchExecutor returns canned B-side results; test verifies corpus_scope='genizah' in recorded call | ✓ WIRED | `test_and_filters_base` asserts `call[2].get("corpus_scope") == "genizah"`. `test_or_synthesizes_neighbor` asserts `cand.via_other_side is True`. Both pass. |
| `merge_candidates()` | `Candidate.via_text / via_vs / vs_rank` | `dataclasses.replace()` annotates provenance | ✓ WIRED | `grep "dataclasses.replace" shared/joins_lab.py` finds 2 occurrences (lines 503 and 538). `test_overlap_annotated` asserts both via_text and via_vs True, vs_rank 4. Passes. |

### Data-Flow Trace (Level 4)

Not applicable — `shared/joins_lab.py` is a pure-logic module with no UI rendering, no state management, and no database I/O. All data is passed in as function arguments; there are no `useState`/`useQuery`/fetch patterns to trace. The module's "data source" is its callers (future UI phases 107–109), and the static guard (SC#6) confirms no direct DB access exists in this module.

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| resolve_other_side_pages(1, 10) == frozenset({2}) | python -c "..." | frozenset({2}) | ✓ PASS |
| resolve_other_side_pages(10, 10) == frozenset({9}) | python -c "..." | frozenset({9}) | ✓ PASS |
| resolve_other_side_pages(5, 10) == frozenset({4,6}) | python -c "..." | frozenset({4,6}) | ✓ PASS |
| dedup collapses two same-page dicts to one Candidate | python -c "..." | len(out)==1 True | ✓ PASS |
| merge_candidates both-tier sorts before text-only | python -c "..." | sids.index('X')<sids.index('Y') True | ✓ PASS |
| detect_self_match returns True/False correctly | python -c "..." | True/False correct | ✓ PASS |
| compose() leading-pipe RTL orientation | python -c "..." | '\|שהדותא' in query True | ✓ PASS |
| `python -c "import shared.joins_lab"` | direct import | exits 0, no engine init, no PyQt | ✓ PASS |
| Full test suite | `python -m pytest tests/test_joins_lab.py -q` | 66 passed in 0.29s | ✓ PASS |
| Ruff clean | `python -m ruff check shared/joins_lab.py tests/test_joins_lab.py` | All checks passed | ✓ PASS |

### Requirements Coverage

| Requirement | Source Plans | Description | Status | Evidence |
|-------------|-------------|-------------|--------|----------|
| JWB-10 | 106-01 | Line-by-line query builder — composes engine's line-break syntax, RTL line-START on right | ✓ SATISFIED | `compose()` + `TestCompose` (12 tests) including round-trip via `_parse_line_break_query` and RTL leading-pipe test. Full JWB-10 UI is Phase 108; foundational pure logic verified here. |
| JWB-11 | 106-02 | Cross-side AND/OR — (sys_id, page±1) set membership, AND narrows, OR widens | ✓ SATISFIED | `resolve_other_side_pages()` + `cross_side_membership()` + `apply_cross_side()` + `TestResolveOtherSide` + `TestCrossSide` (11 tests). Full JWB-11 UI is Phase 108; foundational pure logic verified here. |
| JWB-12 | 106-02, 106-03 | Unified candidate sources — dedup/compaction, text/VS merge, provenance, self-match, snippet helpers | ✓ SATISFIED | `dedup_candidates()` + `merge_candidates()` + `detect_self_match()` + `snippet_html/plain()` + all associated tests (27 tests covering SC#3–SC#5). Full JWB-12 UI surface is Phases 108–109; foundational pure logic verified here. |
| ARCH-NO-PYQT | 106-01 | Module has no PyQt/PySide6 import | ✓ SATISFIED | `TestStaticImport::test_no_pyqt_import` (AST walk) + `test_no_pyside_or_qt_substring` (substring). Both pass. Module-level `python -c "import shared.joins_lab"` exits clean. |
| ARCH-NO-FIST-SQLITE | 106-01 | Module opens no fist_data/*.db directly | ✓ SATISFIED | `TestStaticImport::test_no_fist_data_direct` + `test_no_sqlite3_connect`. Both pass. |
| ARCH-WEB-REUSABLE | 106-01, 106-03 | Module reusable in web context — no desktop-only deps | ✓ SATISFIED | No PyQt import (static guard). `python -c "import shared.joins_lab"` succeeds without any desktop app init. |
| ARCH-SEARCHEXECUTOR-ADAPTER | 106-01, 106-02 | All I/O flows through the injected SearchExecutor Protocol, not direct engine calls | ✓ SATISFIED | `SearchExecutor` Protocol defined (line 150). `apply_cross_side()` takes `executor: SearchExecutor` and all engine calls go through it. `TestCrossSide::test_and_filters_base` verifies corpus_scope='genizah' on the recorded executor call. FakeSearchExecutor is a plain-class Protocol implementation (not MagicMock). |

Note on ARCH-* IDs: These identifiers appear only in the plan frontmatter `requirements:` fields. They correspond to the build constraints stated in REQUIREMENTS.md (lines 136–143: "Extract the pure logic... into a shared, tested module (web-reusable; no PyQt, no direct fist_data/*.db) behind a SearchExecutor adapter") under the "Design-Critique Conclusions & Amendments" section. They are not numbered items in the REQUIREMENTS.md table but are verifiable architectural constraints, and all are satisfied.

Note on REQUIREMENTS.md traceability entry: The table at line 195 reads `(foundational logic for JWB-10/11/12 + build constraints) | 106 (shared core) | Active` — the word "foundational" is deliberate. The full JWB-10/11/12 requirements complete across Phases 107–109; Phase 106 delivers only the shared pure-logic layer. This is the correct interpretation and is satisfied.

### Anti-Patterns Found

Scanned `shared/joins_lab.py` and `tests/test_joins_lab.py`.

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| None found | — | — | — | — |

No TODO/FIXME/placeholder comments, no stub return patterns, no hardcoded empty data flowing to rendering, no `return null`/`return {}` stubs, no `sqlite3.connect`, no PyQt. The module is a complete, substantive implementation.

Known non-blocking findings from 106-REVIEW.md (already logged, not new gaps):
- WR-01: `htmlify` sentinel forgery (SOH/STX bytes could theoretically appear in adversarial corpus text). Accepted — corpus text is trusted in-process content; sentinels are non-printable control characters not valid in the manuscript transcription corpus.
- WR-02: `merge_candidates` drops `vs_score` on text/VS overlap (only `vs_rank` is annotated, not the SVM score). Accepted — `vs_score` is preserved on the VS-only candidates; the annotation path adds `vs_rank` which is the display-relevant field.

Neither warning affects the 6 success criteria.

### Human Verification Required

None — all 6 success criteria are verifiable programmatically. The module is pure logic with no UI, no visual appearance, no external service integration, and no real-time behavior. All verification was completed via `pytest`, `ruff`, and direct Python imports.

### Gaps Summary

No gaps. All 6 ROADMAP success criteria are verified against the actual codebase:

- SC#1 (compose): Implementation at shared/joins_lab.py:686, 12 tests in TestCompose, round-trip via `_parse_line_break_query` confirmed, RTL leading-pipe confirmed.
- SC#2 (cross-side): Implementation at lines 283/306/344, 11 tests in TestResolveOtherSide + TestCrossSide, AND-narrows/OR-widens fixtures confirmed, FakeSearchExecutor I/O path confirmed.
- SC#3 (dedup): Implementation at line 469, 6 tests in TestDedup, canonical key (sys_id, page) confirmed, VS-only (sys_id, None) confirmed.
- SC#4 (merge): Implementation at line 511, 5 tests in TestMerge, both-first ordering confirmed, dataclasses.replace() provenance confirmed.
- SC#5 (self-match + snippets): Implementation at lines 558/584/614/641/663, 16 tests across TestSelfMatch/TestMatchLine/TestSnippet, XSS-escape ordering confirmed, bracket-agnostic membership confirmed.
- SC#6 (static guard): 5 tests in TestStaticImport, AST walk + substring scan, direct import confirmed.

The phase goal is fully achieved: a new shared, web-reusable, unit-tested module exists at `shared/joins_lab.py` encapsulating the Joins Lab domain logic with no PyQt and no direct fist_data/*.db access, backed by the SearchExecutor Protocol and tested with 66 passing tests.

---

_Verified: 2026-06-03T15:01:19Z_
_Verifier: Claude (gsd-verifier)_
