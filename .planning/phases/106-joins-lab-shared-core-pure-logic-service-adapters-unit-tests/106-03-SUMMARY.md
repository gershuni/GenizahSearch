---
phase: 106-joins-lab-shared-core-pure-logic-service-adapters-unit-tests
plan: "03"
subsystem: shared-core
tags: [joins-lab, self-match, snippet-helpers, htmlify, tdd, pure-logic, sc5]
dependency_graph:
  requires:
    - shared/joins_lab.py detect_self_match / _match_line (this plan)
    - shared/joins_lab.py _r_sid / page_of / Candidate (Plan 01)
    - shared/joins_lab.py dedup_candidates / merge_candidates (Plan 02)
    - tests/test_joins_lab.py TestSelfMatch / TestMatchLine / TestSnippet (this plan)
  provides:
    - shared/joins_lab.py detect_self_match() — pure sys_id membership check (SC#5)
    - shared/joins_lab.py _match_line() — first-hit line index locator (SC#5)
    - shared/joins_lab.py htmlify() — HTML escape + RTL wrapper + regex highlight (SC#5)
    - shared/joins_lab.py snippet_html() — centered HTML snippet for candidate cards (SC#5)
    - shared/joins_lab.py snippet_plain() — centered plain-text snippet for table cells (SC#5)
    - tests/test_joins_lab.py TestSelfMatch / TestMatchLine / TestSnippet
  affects:
    - Phase 107 desktop JWB (uses detect_self_match + snippet_html/snippet_plain for candidate display)
    - Phase 108 web JWB (same snippet helpers; the "include anchor itself" toggle consumes detect_self_match)
tech_stack:
  added:
    - "import html (stdlib) — html.escape() in htmlify (T-106-08 XSS mitigation)"
    - "MARK_A / MARK_B sentinel constants (SOH/STX bytes) for highlight-before-escape ordering"
  patterns:
    - pure functions take already-fetched data (D-06)
    - sentinel-before-escape ordering in htmlify (apply regex substitution → html.escape → replace sentinels)
    - re.error swallowed in all three pattern-using helpers (degrade to no-highlight/-1)
key_files:
  created: []
  modified:
    - shared/joins_lab.py (detect_self_match, _match_line, MARK_A/MARK_B, htmlify,
        snippet_html, snippet_plain; import html added; 633→750 lines approx)
    - tests/test_joins_lab.py (TestSelfMatch 4 tests, TestMatchLine 6 tests,
        TestSnippet 6 tests; 50→66 tests total)
decisions:
  - "htmlify uses sentinel-before-escape ordering (MARK_A/MARK_B substituted before html.escape)
     so corpus text with '<'/'>'/'&' is escaped and cannot inject markup — T-106-08 mitigated"
  - "MARK_A/MARK_B chosen as SOH (\\x01) / STX (\\x02) — not valid UTF-8 in corpus text and
     survive html.escape() unchanged; simpler than UUID sentinels"
  - "htmlify import removed from test file (F401 fix — htmlify is exercised through snippet_html,
     not tested directly as a public API in this plan's test class)"
  - "test_html_no_match assertion changed from '<b' not in out to '<b style=' not in out
     because '<br>' tags contain the substring '<b'; the precise check is for the highlight tag"
  - "detect_self_match docstring follows RESEARCH R-02 corrected scope note exactly —
     does NOT claim engine-bracket guarantee; explicitly states that is out of Phase 106 scope"
metrics:
  duration: "~6 minutes"
  completed: "2026-06-03"
  tasks_completed: 2
  tasks_total: 2
  files_created: 0
  files_modified: 2
  tests_added: 16
  lines_added: ~117
---

# Phase 106 Plan 03: Self-Match Detection + Centered Snippet Helpers Summary

detect_self_match (pure sys_id membership, bracket-agnostic by construction) + _match_line (first-hit locator) + htmlify / snippet_html / snippet_plain (centered display helpers, XSS-escaped, RTL-wrapped) appended to shared/joins_lab.py; 66 tests green; SC#1–SC#6 complete.

## What Was Built

### Task 1 — Self-Match Detection + First-Hit Line Locator (SC#5)

Two functions appended to `shared/joins_lab.py`:

- **`detect_self_match(raw_results, anchor_sid) -> bool`** — PURE. Reports whether the anchor's own sys_id appears among an already-fetched result list. Transplanted from sketch `_anchor_matched` (L1100): `return any(_r_sid(r) == anchor_sid for r in raw_results)`. Bracket-agnostic by construction — keys on sys_id only, never runs a position-sensitive regex, so a leading tear-bracket token in any result's text cannot affect the boolean. Explicitly does NOT prove engine-level bracket handling (RESEARCH R-02, corrected 2026-06-03). The Phase 108 "include anchor itself" UI toggle consumes this alongside `dedup_candidates(include_self=...)`.

- **`_match_line(lines, pattern) -> int`** — PURE. Returns the index of the first line matching the pattern (re.IGNORECASE). Returns -1 for None/empty pattern, no match, or malformed pattern (re.error swallowed). Transplanted from sketch L113-123.

Tests: `TestSelfMatch` (4 tests — anchor_present, anchor_absent, empty_results, membership_is_sysid_only), `TestMatchLine` (6 tests — first_hit, no_match, no_pattern empty/None, bad_regex, case_insensitive). All 10 green.

### Task 2 — Centered Snippet Helpers (SC#5)

`import html` added to the module imports. Two module-level sentinel constants defined:
- `MARK_A = "\x01"` (SOH), `MARK_B = "\x02"` (STX) — survive `html.escape()`, not valid in corpus text.

Three functions appended to `shared/joins_lab.py`:

- **`htmlify(text, pattern=None) -> str`** (transplanted sketch L98-110) — HTML escape + RTL wrapper + optional regex highlight. Sentinel-before-escape ordering: regex substitution marks regions with MARK_A/MARK_B → `html.escape(text)` → replace `\n` with `<br>` → replace sentinels with `<b style='color:#dc2626'>...</b>`. This ordering prevents XSS: corpus text containing `<`/`>`/`&` is escaped before any HTML tags are inserted (T-106-08 mitigated, verified by `TestSnippet::test_html_escapes`).

- **`snippet_html(text, pattern, max_lines=8) -> str`** (sketch L126-135) — centers the display window on the first regex match via `_match_line`. Hit found → window `lines[max(0, hit-2) : hit-2+max_lines]`. No hit → first non-blank lines up to `max_lines`. Returns `htmlify(window, pattern)`.

- **`snippet_plain(text, pattern, max_chars=220) -> str`** (sketch L138-148) — plain-text centering for table cells. Hit found → up to 3 stripped lines from `lines[max(0, hit-1) : hit+2]`. No hit → first 3 non-blank stripped lines. Joined with `"  /  "`, truncated to `max_chars` + `"…"`.

Tests: `TestSnippet` (6 tests — html_centers_on_match, html_no_match_takes_first_lines, html_escapes, html_max_lines_window, plain_centers_and_caps, plain_no_match). All 6 green.

## Commits

| Hash | Type | Description |
|------|------|-------------|
| `2a2f69e3` | test | RED: failing tests for detect_self_match + _match_line (SC#5) |
| `e13d861b` | feat | GREEN: detect_self_match + _match_line (SC#5) |
| `e1e8b40a` | test | RED: failing tests for htmlify + snippet_html + snippet_plain (SC#5) |
| `b26d81c1` | feat | GREEN: htmlify + snippet_html + snippet_plain + import html (SC#5) |

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Test assertion `"<b" not in out` matched `<br>` tags**
- **Found during:** Task 2 GREEN phase (first test run)
- **Issue:** `test_html_no_match_takes_first_lines` asserted `"<b" not in out`, but `<br>` tags (present in every htmlify output) contain the substring `<b`. The assertion was too broad.
- **Fix:** Changed assertion to `"<b style=" not in out` — the highlight-specific tag is `<b style='color:#dc2626'>`, not a generic `<b>`. `<br>` does not contain `style=`.
- **Files modified:** `tests/test_joins_lab.py`
- **Commit:** `b26d81c1`

**2. [Rule 2 - Missing] F401 unused import `htmlify` in test file**
- **Found during:** Task 2 ruff check
- **Issue:** `htmlify` was imported in `tests/test_joins_lab.py` but not used directly by any test (it is exercised through `snippet_html`). Ruff flagged F401.
- **Fix:** Removed the `htmlify` import from the test file. `htmlify` is still tested indirectly via `snippet_html` (centering, escaping, RTL wrapper all pass through it).
- **Files modified:** `tests/test_joins_lab.py`
- **Commit:** `b26d81c1`

## TDD Gate Compliance

| Gate | Commit | Status |
|------|--------|--------|
| RED (Task 1) | `2a2f69e3` | PASS — ImportError on missing detect_self_match / _match_line |
| GREEN (Task 1) | `e13d861b` | PASS — 10 new tests pass (60 total) |
| RED (Task 2) | `e1e8b40a` | PASS — ImportError on missing htmlify / snippet_html / snippet_plain |
| GREEN (Task 2) | `b26d81c1` | PASS — 6 more tests pass; 66 total (SC#1–SC#6 complete) |

## Phase 106 Completion

All six SC units are now implemented and tested:

| SC | Unit | Plan | Tests |
|----|------|------|-------|
| SC#1 | `compose()` + line-break syntax | 01 | TestCompose (10) |
| SC#2 | `resolve_other_side_pages` + `cross_side_membership` + `apply_cross_side` | 02 | TestResolveOtherSide/TestCrossSide (11) |
| SC#3 | `dedup_candidates` | 02 | TestDedup (6) |
| SC#4 | `merge_candidates` | 02 | TestMerge (5) |
| SC#5 | `detect_self_match` + `_match_line` + `snippet_html` + `snippet_plain` + `htmlify` | 03 | TestSelfMatch/TestMatchLine/TestSnippet (16) |
| SC#6 | Static import guard (no PyQt, no sqlite3.connect, importable without engine init) | 01 | TestStaticImport (5) + TestDataclasses/TestProtocol (3) |

**Total: 66 tests, all green. Module is web-reusable (no PyQt, no direct sqlite).**

## Known Stubs

None — all implementations are complete and data-wired.

## Threat Flags

No new network endpoints, auth paths, file access patterns, or schema changes.

- T-106-07 (ReDoS via highlight pattern): accepted per plan. Pattern is engine-produced `highlight_pattern` from an already-returned result (same regex the engine already compiled and ran). `try/except re.error` degrades to no-highlight/-1 on malformed patterns.
- T-106-08 (XSS via snippet HTML): mitigated. `htmlify` applies sentinel substitution BEFORE `html.escape()`, so corpus `<`/`>`/`&` is always escaped. Only the module's own fixed `<b style=...>` / `<br>` / `<div dir=...>` tags survive. Verified by `TestSnippet::test_html_escapes` (`&lt;` present in output).

## Self-Check: PASSED

- `shared/joins_lab.py` exists: FOUND
- `tests/test_joins_lab.py` exists: FOUND
- `grep 'def detect_self_match' shared/joins_lab.py`: PASS
- `grep 'def _match_line' shared/joins_lab.py`: PASS
- `grep 'def htmlify' shared/joins_lab.py`: PASS
- `grep 'def snippet_html' shared/joins_lab.py`: PASS
- `grep 'def snippet_plain' shared/joins_lab.py`: PASS
- `grep 'import html' shared/joins_lab.py`: PASS
- `grep 'MARK_A' shared/joins_lab.py`: PASS
- Commit `2a2f69e3` exists: FOUND (RED Task 1)
- Commit `e13d861b` exists: FOUND (GREEN Task 1)
- Commit `e1e8b40a` exists: FOUND (RED Task 2)
- Commit `b26d81c1` exists: FOUND (GREEN Task 2)
- `pytest tests/test_joins_lab.py -x -q` → 66 passed (SC#1–SC#6)
- `python -m ruff check shared/joins_lab.py tests/test_joins_lab.py` → All checks passed
- `python -c "import shared.joins_lab"` → success (no PyQt, no engine init)
- `class TestSelfMatch` in `tests/test_joins_lab.py`: FOUND
- `class TestMatchLine` in `tests/test_joins_lab.py`: FOUND
- `class TestSnippet` in `tests/test_joins_lab.py`: FOUND
