---
phase: 134-discovery-data-spine
plan: 02
subsystem: infra
tags: [masking, ci-guard, sqlite, security, docs, discovery]

# Dependency graph
requires:
  - phase: 133-visual-atlas-preview-early-quick-win
    provides: "scripts/check_atlas_masking.py D-07 forerunner (build_matcher/PatternMatcher.scan, scan_repo, scan_asset, load_patterns, _require_patterns, ScanError) this plan extends"
provides:
  - "scan_sqlite(db_path, patterns) -> list[Issue]: cell-level (schema + every str/BLOB cell) SQLite leak scanner, reusing the ONE canonical matcher, fail-closed"
  - "--scan-sqlite PATH CLI mode composing with --scan-asset/--scan-repo/--strict in one invocation"
  - "docs/specs/discovery-budgets.md: the PERF-01 acceptance-budget exit artifact (initial caps + DATA-06 discretion defaults for 134-06)"
  - "/discovery_data/ gitignore entry for the future discovery.db sidecar"
affects: [134-04-offline-distillation, 134-06-discovery-service, 134-08-release-contract-and-measurement]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "scan_sqlite mirrors scan_asset's frozen signature shape (path, patterns) and builds its own matcher internally -- never a pre-built matcher passed in"
    - "sqlite3.connect('file:...?mode=ro', uri=True) for a strictly read-only cell-level scan, distinct from scan_asset's raw-byte view of the same .db file"
    - "surface strings (schema/cell tags) are redacted via matcher.redact_path BEFORE becoming Issue.path, not just at .format() time -- closes a repr()-bypasses-sanitizer gap"

key-files:
  created:
    - tests/test_masking_sqlite.py
    - docs/specs/discovery-budgets.md
  modified:
    - scripts/check_atlas_masking.py
    - .gitignore

key-decisions:
  - "Skipped requirements mark-complete for DATA-05/PERF-01 (shared frontmatter IDs across 134-02/04/08) -- premature to flip Complete until the sidecar-wired scan (134-04) and measured actuals (134-08) land"
  - "R-source token pre-registration in the gitignored MASKING_SCAN_PATTERNS_FILE (D-03c) deferred as an owner-only operational step -- the executor has no access to and must not fabricate the real R-source name/aliases/sigla; the ingestion mechanism itself (one pattern per non-comment line) is proven with fabricated tokens instead"
  - "A pre-existing, out-of-scope masking leak recurrence was found in untracked tmp/ Codex-review scratch files during this plan's own verification step; left unfixed per the scope-boundary rule and logged in deferred-items.md + a STATE.md blocker, rather than editing large freeform review transcripts outside this plan's file list"

patterns-established:
  - "Cell-level (not byte-blob) SQLite masking scan: sqlite_master.sql for schema/DDL/identifiers, PRAGMA table_info + SELECT * per table for str/BLOB cells, all routed through the same PatternMatcher.scan pipeline"

requirements-completed: []  # DATA-05, PERF-01 both shared across later 134-0x plans (134-04, 134-08) -- see key-decisions

# Metrics
duration: 50min
completed: 2026-07-21
---

# Phase 134 Plan 02: Masking SQLite Extension + PERF-01 Budgets Summary

**Extended the permanent DATA-05 masking CI guard with a cell-level `--scan-sqlite` mode (schema + every str/BLOB cell, fail-closed) and committed the PERF-01 `discovery-budgets.md` acceptance-budget exit artifact, so both leak-detection and performance contracts exist BEFORE `discovery.db` is built.**

## Performance

- **Duration:** ~50 min
- **Started:** 2026-07-21 (session start; see git log for exact commit times)
- **Completed:** 2026-07-21T22:04Z
- **Tasks:** 3 completed (4 commits -- Task 2 produced a test commit + a Rule-1 fix commit)
- **Files modified:** 4 (`scripts/check_atlas_masking.py`, `tests/test_masking_sqlite.py`, `docs/specs/discovery-budgets.md`, `.gitignore`)

## Accomplishments

- `scan_sqlite(db_path, patterns) -> list[Issue]` added to `scripts/check_atlas_masking.py`: connects `file:<path>?mode=ro` (strictly read-only), scans `sqlite_master.sql` (schema/DDL, including a leaky column NAME) tagged `<db>::schema`, then every table's every row/column -- BOTH str/TEXT and bytes/BLOB cells -- tagged `<db>::<table>.<column>`, all through the SAME canonical `build_matcher`/`PatternMatcher.scan` pipeline (no new matcher). Any connect/read/decode failure raises `ScanError` (fail-closed).
- `--scan-sqlite PATH` wired into `parse_args()`/`main()` via the `is not None` presence-test convention (HIGH-10), composing with `--scan-asset`/`--scan-repo`/`--strict` in one invocation; sqlite issues participate in the combined exit code. The existing `--strict` gate (requires both `--scan-repo` and `--scan-asset`) needed no change, per plan.
- `tests/test_masking_sqlite.py` (17 tests): fabricated-token proof that `scan_sqlite(db, [FAKE, ...])` flags the schema surface (incl. a leaky column NAME baked into a `CREATE TABLE` statement), str cells, and BLOB cells, with `_assert_never_echoes` holding over every returned `Issue`; a `-k strict`-selectable pair of tests proves the combined `--scan-sqlite <db> --scan-asset <db> --scan-repo --strict` form is accepted end-to-end (in-process, `ROOT_DIR` pointed at a throwaway repo, never the real project tree) both on a leaky fixture (rc=1) and a clean one (rc=0); a `-k blob` test proves BLOB-cell scanning; the unset-pattern-file fail-safe (`--scan-sqlite` alone with `MASKING_SCAN_PATTERNS_FILE` unset exits 1, never silently 0) is proven as a property; fail-closed tests cover a missing file and a non-SQLite file.
- `docs/specs/discovery-budgets.md` committed: the PERF-01 initial numeric caps copied verbatim (browse-enrichment p95 ≤150ms/timeout 2s; work/leads ≤200 rows/page/≤500KB/p95≤1.5s/timeout 5s; atlas drill-down ≤1,500 nodes/6,000 edges/≤2MB/p95≤3s/timeout 10s; discovery adds ≤250MB RSS), each framed as "tunable only by versioning this artifact"; ratifies concrete DATA-06 discretion defaults (per-surface query timeouts equal to the hard caps, bounded concurrency=4, browse-enrichment LRU=5,000 entries, page size default 50/max 200) with a proposed `DISCOVERY_*` env-var naming convention for 134-06 to implement against; a MEASURED ACTUALS section is explicitly left PENDING for 134-08.
- `.gitignore` gained a `/discovery_data/` entry mirroring `/atlas_data/`, so the future built `discovery.db` sidecar is never committed.

## Task Commits

Each task was committed atomically:

1. **Task 1: Add the --scan-sqlite cell-level mode** - `f247e42a` (feat)
2. **Task 2: Fabricated-token SQLite scan test + R-source token pre-registration note** - `84c93d18` (test) + `6b925817` (fix -- Rule-1 auto-fix discovered by the new test)
3. **Task 3: Commit docs/specs/discovery-budgets.md + gitignore discovery_data/** - `b043be0a` (docs)

**Plan metadata:** (this commit, following SUMMARY write)

## Files Created/Modified

- `scripts/check_atlas_masking.py` - Added `scan_sqlite(db_path, patterns)`, `_quote_ident`, and the `--scan-sqlite` CLI wiring; module docstring/usage updated
- `tests/test_masking_sqlite.py` - 17 fabricated-token tests covering schema/str/BLOB surfaces, never-echo, CLI composition + strict combination, fail-closed connect/read errors, zero-pattern refusal, and the pattern-file ingestion mechanism
- `docs/specs/discovery-budgets.md` - PERF-01 exit artifact: initial caps + DATA-06 discretion defaults + pending measured-actuals section
- `.gitignore` - `/discovery_data/` entry

## Decisions Made

- Skipped `requirements mark-complete` for DATA-05 and PERF-01: both requirement IDs are shared across this plan's frontmatter AND later plans (DATA-05 also 134-04; PERF-01 also 134-08) -- following the exact precedent set in 134-01's SUMMARY for DATA-01/02/03/10, marking Complete now would be premature since the sidecar-wired scan and the measured-actuals section don't exist yet.
- Redacted `scan_sqlite`'s constructed surface strings (`<db>::schema`, `<db>::<table>.<column>`) via `matcher.redact_path()` BEFORE they become `Issue.path`, not only at `.format()` time -- see Deviations below.
- Did not create or touch the real, owner-held `MASKING_SCAN_PATTERNS_FILE`/`.masking_patterns` to pre-register R-source tokens (D-03c): the executor has no access to the actual R-source name/aliases/sigla and fabricating a stand-in would defeat the purpose of the guard. Proved the ingestion mechanism itself (one pattern per non-comment line, already generic across corpora) is unchanged and sufficient with a fabricated multi-line pattern-file test instead. **This remains an outstanding manual step for the owner** (see "User Setup Required" below).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `scan_sqlite` surface strings reached `Issue.path` unredacted, leaking via `repr()`**
- **Found during:** Task 2, while writing the never-echo assertion for the leaky-column-name test
- **Issue:** `scan_sqlite`'s constructed `<db>::schema` / `<db>::<table>.<column>` surface strings were passed straight into `matcher.scan()` as the `rel_path` argument without first being redacted, unlike every other surface in the file (`scan_repo`/`scan_asset` always compute `display = matcher.redact_path(raw_display)` before scanning). `Issue.format()` still redacted the leak correctly (it routes through `_sanitize`), but the raw dataclass `path` field -- visible via `repr(issue)` -- could echo a token embedded in a table/column identifier.
- **Fix:** Route the schema surface and each column's surface string through `matcher.redact_path()` once (column surfaces precomputed per-column outside the row loop, not per-cell, for efficiency) before any `matcher.scan()` call, mirroring the existing `scan_repo`/`scan_asset` convention exactly.
- **Files modified:** `scripts/check_atlas_masking.py`
- **Verification:** `tests/test_masking_sqlite.py::test_scan_sqlite_flags_schema_surface_via_leaky_column_name` (which failed before the fix, confirming it was load-bearing) now passes; full `_assert_never_echoes` (covering both `.format()` and `repr()`) holds across all 17 tests.
- **Committed in:** `6b925817`

---

**Total deviations:** 1 auto-fixed (Rule 1 - bug)
**Impact on plan:** Necessary for the never-echo guarantee the whole masking guard exists to provide; no scope creep -- fix is confined to `scan_sqlite`, the function this plan added.

## Issues Encountered

- **Pre-existing, out-of-scope masking leak recurrence found in `tmp/`.** Running this plan's own required verification step (`check_atlas_masking.py --scan-repo`) surfaced 19 hits, all inside two UNTRACKED Codex-CLI review-transcript files unrelated to this plan (`tmp/CODEX-REVIEW-134-replan-r2.md`, `tmp/CODEX-REVIEW-134-replan-r3.md` -- artifacts from the 134-CONTEXT.md re-plan/owner-gate Codex-review rounds, predating this session). `tmp/` is still not gitignored -- the exact systemic gap the 134-01 SUMMARY had already flagged (a 1st occurrence with a different file). Per the scope-boundary rule, these large freeform review transcripts (outside this plan's `files_modified` list, and risky to hand-redact without directly viewing the matched restricted string) were left untouched. This plan's own 4 deliverables were confirmed clean individually via `--scan-asset` on each file (all `no matches -- clean`). Logged as a new dated entry in `deferred-items.md` and as a STATE.md blocker recommending the owner either gitignore `tmp/**/*.md`/`tmp/**/*.log` or manually redact/delete the two flagged files.

## User Setup Required

**One manual, owner-only step remains (D-03c, not automatable by the executor):**
- Append the real R-source name + known aliases/sigla, one per line, to your local gitignored pattern file (the path referenced by `MASKING_SCAN_PATTERNS_FILE`, e.g. `.masking_patterns` at repo root). No code change is needed -- `load_patterns()` already ingests any non-comment line unconditionally. Do this before Phase 134-04's distillation could ever reference R-source, even though R-source text itself does not enter the v9.0 launch spine (D-01/D-02) -- D-03c is defense-in-depth.

Separately, not required by this plan but worth the owner's attention: `tmp/CODEX-REVIEW-134-replan-r2.md` and `tmp/CODEX-REVIEW-134-replan-r3.md` contain restricted-corpus mask hits and are untracked/uncommitted -- consider redacting or deleting them, and/or gitignoring `tmp/**/*.md`+`tmp/**/*.log` to prevent recurrence (see Issues Encountered above and `deferred-items.md`).

## Next Phase Readiness

- The DATA-05 guard can now scan a SQLite sidecar cell-by-cell (schema + str + BLOB) before `discovery.db` exists, ready for 134-04 (offline distillation) to run `--scan-sqlite` against its output as a ship gate, and for 134-08 to include it in the full release-contract verification.
- `docs/specs/discovery-budgets.md` gives 134-06 (`DiscoveryService`) concrete, versioned default values (timeouts, concurrency, LRU size, pagination) to implement against, closing the "Claude's Discretion" open item from `134-CONTEXT.md`.
- `discovery_data/` is gitignored, so 134-04's distillation output can be built locally without any risk of an accidental commit.
- Outstanding: the owner-only R-source pattern pre-registration (above), and the pre-existing `tmp/` masking-leak recurrence (unrelated to this plan, tracked separately).

## Self-Check: PASSED

- FOUND: scripts/check_atlas_masking.py
- FOUND: tests/test_masking_sqlite.py
- FOUND: docs/specs/discovery-budgets.md
- FOUND: .gitignore
- FOUND commit: f247e42a
- FOUND commit: 84c93d18
- FOUND commit: 6b925817
- FOUND commit: b043be0a

---
*Phase: 134-discovery-data-spine*
*Completed: 2026-07-21*
