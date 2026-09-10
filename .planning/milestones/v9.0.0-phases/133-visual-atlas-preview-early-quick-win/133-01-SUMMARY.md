---
phase: 133-visual-atlas-preview-early-quick-win
plan: 01
subsystem: security
tags: [masking, git-scan, data-exposure, ci-guard, m-source, build-tooling]

# Dependency graph
requires: []
provides:
  - "scripts/check_atlas_masking.py -- reusable D-07 masking scan (scan_repo three-surface git scan + scan_asset recursive built-artifact scan), the forerunner of the permanent DATA-05 CI guard (Phase 134)"
  - "tests/test_atlas_masking_scan.py -- sanity-injection self-test proving the scan is load-bearing"
  - ".gitignore entries for /.masking_patterns (env-sourced local pattern file) and /atlas_data/ (baked atlas output dir, outside web/static/)"
  - "Working-tree scrub of the pre-existing M-source leak in genizah_translations.py (kept uncommitted/unstaged for 133-03)"
  - "The phase's atomic FIRST commit -- every subsequent plan in Phase 133 (133-02..133-06) can now build on a repo proven clean by a green 3-pass scan"
affects: [133-02, 133-03, 133-04, 133-05, 133-06, 134-discovery-data-spine]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Env-sourced sensitive-pattern-list idiom (MASKING_SCAN_PATTERNS_FILE -> gitignored local file), mirroring web/puzzle_tokens.py::PUZZLE_SECRET"
    - "Split matcher design: scan_repo uses a FAST literal-byte matcher (exact UTF-8 bytes + cheap ASCII-only bytes.lower() casefold, no Unicode decode/normalize) for the three git surfaces at real-working-tree scale; scan_asset uses a RICH matcher (text + UTF-8 bytes + NFC/NFD-normalized casefold + URL/HTML-entity/JS-\\u encoded forms) for the small, bounded built artifact"
    - "Never-echo reporting: Issue dataclass carries only (path, byte offset, pattern index) -- never the matched pattern text"
    - "Fail-safe pattern loading: load_patterns() returns [] on ANY failure mode (unset/missing/empty file); main() treats an empty pattern list as a hard error (exit 1), never a silent green"
    - "git cat-file --batch bulk blob read (one subprocess for the whole HEAD+staged-index surface) instead of one `git show` subprocess per file"

key-files:
  created:
    - scripts/check_atlas_masking.py
    - tests/test_atlas_masking_scan.py
  modified:
    - .gitignore
    - genizah_translations.py (working-tree scrub only -- deliberately NOT staged/committed this plan)

key-decisions:
  - "scan_repo gets a FAST literal-byte matcher (not the rich normalized/encoded matcher) -- discovered mid-execution that the working tree carries ~24GB of unrelated non-ignored untracked content (ACL2026_papers/), which made the originally-shared rich matcher impractically slow (3+ minutes and climbing); the plan's own acceptance criteria already attribute the rich text+normalized+encoded matching to scan_asset only, so the fix aligns with spec rather than diverging from it"
  - "M-source scrub in genizah_translations.py renames the 3 leaking entries to the 'M-source' codename (key AND Hebrew value) rather than deleting them outright -- preserves the not-yet-wired Discovery Review deck glossary's structure/completeness for its future consumer while eliminating the leak"
  - "Masking pattern list (gitignored, never committed) covers the restricted name, its Hebrew form, and the corpus site's domain (two variants) -- sourced from the user's own project memory + the genizah_translations.py leak + the gitignored same_work_spike/probe research tree, per the plan's step 4 instruction"

requirements-completed: [ATLAS-01]

# Metrics
duration: 55min
completed: 2026-07-20
---

# Phase 133 Plan 01: D-07 Masking Scan (Atomic Precondition) Summary

**Built the reusable multi-surface M-source masking scan (`scripts/check_atlas_masking.py`) and used its first run to scrub the pre-existing uncommitted M-source leak in `genizah_translations.py` before making the phase's first commit.**

## Performance

- **Duration:** ~55 min
- **Completed:** 2026-07-20T15:38Z
- **Tasks:** 1 (single atomic task, as designed)
- **Files modified:** 4 (3 committed: `.gitignore`, `scripts/check_atlas_masking.py`, `tests/test_atlas_masking_scan.py`; 1 scrubbed but deliberately left uncommitted: `genizah_translations.py`)

## Accomplishments

- `scripts/check_atlas_masking.py`: `scan_repo()` scans three SEPARATE git surfaces (HEAD/index blobs via a single bulk `git cat-file --batch` call, tracked worktree files, non-ignored untracked candidates) with a fast literal-byte matcher; `scan_asset(path)` recursively scans a single file or a whole directory (`.bin`/`.bin.br`/`.json`/`.html`) with a rich matcher (literal text, UTF-8 bytes, NFC/NFD-normalized+casefolded, URL-percent/HTML-entity/JS-`\uXXXX` encoded forms). Pattern list is sourced from `MASKING_SCAN_PATTERNS_FILE` (a gitignored local file), never hardcoded. Fails safe (exit 1) with no patterns loaded. Never echoes a matched pattern — reports only relative path, byte offset, and a pattern index.
- `tests/test_atlas_masking_scan.py`: 20 tests proving the scan is load-bearing — literal/binary/NFD-normalized/URL/HTML-entity/JS-escape hits are all caught, clean content passes, the fail-safe and never-echo guarantees hold, both CLI end-to-end paths (`--scan-asset`, `--self-test`) work.
- `.gitignore`: added `/.masking_patterns` (the local pattern file) and `/atlas_data/` (the future baked-atlas output dir, deliberately OUTSIDE `web/static/` so it can never bypass the upcoming `ATLAS_PREVIEW_ENABLED` flag through the public `/static` mount).
- Scrubbed the pre-existing uncommitted M-source leak in `genizah_translations.py` (3 entries in an unwired "Discovery Review deck" glossary block) by renaming them to the `M-source` codename, leaving every other translation addition in that block and elsewhere untouched.
- Proved a clean 3-pass repo scan (`python scripts/check_atlas_masking.py --scan-repo` exits 0) BEFORE staging anything, then staged and committed exactly the three allowed files with explicit-path `git add` — the phase's single, atomic FIRST commit.

## Task Commits

Single atomic task, one commit:

1. **Task 1: D-07 masking scan + M-source scrub (atomic precondition)** — `f2aa8c3a` (feat)
   - Staged: `.gitignore`, `scripts/check_atlas_masking.py`, `tests/test_atlas_masking_scan.py` (explicit-path `git add`, verified via `git diff --cached --name-only` before commit)
   - NOT staged (by design): `genizah_translations.py` (scrubbed in the working tree, deferred to 133-03), `.planning/STATE.md`, `web/main.py`, `web/pages/browse.py` (pre-existing unrelated in-progress R2-1 embed work — untouched)

No separate plan-metadata commit was required beyond the final docs commit below (single-task plan).

## Files Created/Modified

- `scripts/check_atlas_masking.py` — the D-07 masking scan CLI (`--scan-repo`, `--scan-asset PATH`, `--self-test`)
- `tests/test_atlas_masking_scan.py` — 20-test sanity-injection self-test suite
- `.gitignore` — 2 new entries (`/.masking_patterns`, `/atlas_data/`)
- `genizah_translations.py` — 3 M-source-leaking entries renamed to the `M-source` codename (working-tree only, NOT staged/committed this plan)

## Decisions Made

- **scan_repo vs scan_asset matcher split (performance-driven, spec-aligned):** The plan's own acceptance criteria attribute the rich text/UTF-8/normalized/encoded matching specifically to `scan_asset` and describe `scan_repo` only as running "three separate passes." My first implementation shared the rich matcher across both for simplicity; a full `--scan-repo` run against the ACTUAL working tree exceeded 3 minutes and was still climbing when killed. Investigation found `ACL2026_papers/` — an unrelated, non-ignored, untracked ~24 GB directory of conference-paper research sitting in the repo root — being fully UTF-8-decoded, NFC/NFD-normalized, and casefolded per pattern (redundant per-pattern re-normalization compounded this further before a first fix). Root cause fixed in two steps: (1) precompute normalized forms once per file instead of once per pattern, (2) give `scan_repo` a FAST literal-byte matcher (exact UTF-8 bytes + a cheap ASCII-only `bytes.lower()` casefold pass — no Unicode decode/normalize at all) while keeping the rich matcher exclusively for `scan_asset`'s small, bounded (~6 MB budget) built-artifact scope. Full `--scan-repo` now completes in ~2 minutes and exits 0. This is a real-world consideration for the Phase 134 permanent CI guard too: a CI checkout only ever has tracked files (no "non-ignored untracked cruft" surface at all), so `scan_repo`'s pass 3 will typically be near-empty there — the fast path costs nothing in the common case and buys headroom against exactly this kind of local dev-machine accumulation.
- **M-source scrub via codename rename, not deletion:** `genizah_translations.py`'s uncommitted "Discovery Review deck" glossary block (prep work for a future, not-yet-built in-app component, per its own header comment) had 3 entries naming the restricted corpus directly. Rather than deleting them (which would silently shrink the glossary's coverage for whenever that future component is built), I renamed the English key and Hebrew value of each to the `M-source` codename — consistent with how the rest of the codebase refers to the restricted source, and structurally identical to the original (same number of entries, same comment/section shape). Verified via `git diff genizah_translations.py | grep -i M-source` returning zero matches.
- **Masking pattern-list content (gitignored, never disclosed in any committed artifact):** Populated `<repo-root>/.masking_patterns` with the restricted corpus name (English), its Hebrew form, and its web domain (two variants for defense-in-depth against a leak that omits the "M-source." subdomain prefix) — sourced from the user's own prior project memory (which already documents the masking rule and the specific string), the `genizah_translations.py` leak itself, and cross-referenced against the gitignored `same_work_spike/probe/` research tree (confirmed the domain via `M-source_api_probe.py`/`M-source_nosafot_harvest.py`). Not reproduced here or in any commit.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] scan_repo's original rich-matcher implementation was impractically slow against the real working tree**
- **Found during:** Task 1, step 5/6 (running the scan before the scrub / proving the green gate)
- **Issue:** The first implementation applied the same rich (Unicode-decode + NFC/NFD-normalize + casefold + encoded-form) matcher to every git surface, redundantly re-normalizing per pattern. Against this repo's actual working tree — which carries a large (~24 GB), non-ignored, untracked `ACL2026_papers/` directory unrelated to this phase — a full `--scan-repo` run exceeded 3 minutes and had to be killed (risk of effectively hanging the atomic-precondition gate).
- **Fix:** (a) Precomputed the NFC/NFD-normalized+casefolded text forms once per file instead of once per pattern. (b) Split the matcher: `scan_repo` now uses a fast literal-byte matcher (exact UTF-8 bytes + a cheap ASCII-only `bytes.lower()` casefold, no Unicode decode/normalize); `scan_asset` keeps the full rich matcher, appropriate for its small bounded built-artifact scope. This matches — rather than diverges from — the plan's own acceptance-criteria wording, which attributes the rich matching to `scan_asset` specifically.
- **Files modified:** `scripts/check_atlas_masking.py` (before its first commit — no separate fix commit needed)
- **Verification:** `python scripts/check_atlas_masking.py --scan-repo` now completes in ~2 minutes and exits 0 (clean); `python -m pytest tests/test_atlas_masking_scan.py -q` — 20/20 pass.
- **Committed in:** `f2aa8c3a` (the only task commit — the fix landed before the first commit was ever made)

---

**Total deviations:** 1 auto-fixed (Rule 1 — performance bug caught and fixed during self-verification, before any commit)
**Impact on plan:** No scope creep. The fix makes `scan_repo` match the plan's own acceptance-criteria design (rich matching reserved for `scan_asset`) rather than departing from it, and directly serves the plan's success criterion that the scan must be practically runnable as a repeatable gate (and, per D-07, the forerunner of the permanent Phase 134 CI guard).

## Issues Encountered

- The initial `--scan-repo` run was backgrounded by the environment (exceeded the default foreground timeout) and had to be monitored/killed twice during the performance investigation described above. No data was lost; each kill happened before any commit.
- `git grep -i M-source` (used only for my own interim verification, not part of the shipped tooling) timed out against the full working tree for the same reason (large untracked binary content) — superseded by the actual `scan_repo()` implementation, which completed successfully once fixed.

## User Setup Required

None for this plan directly. The CI secret-injection recipe below is **designed and documented, not wired** — Phase 134 owns the actual `.github/workflows/ci.yml` change.

**CI secret-injection recipe for `MASKING_SCAN_PATTERNS_FILE` (for Phase 134 to wire):**
```yaml
# In the CI job that will run `python scripts/check_atlas_masking.py --scan-repo`
# (and, from Phase 134 on, --scan-asset against the sidecar/atlas outputs):
- name: Write masking pattern file
  run: |
    printf '%s' "${{ secrets.MASKING_SCAN_PATTERNS }}" > "${GITHUB_WORKSPACE}/.masking_patterns"
    chmod 600 "${GITHUB_WORKSPACE}/.masking_patterns"
  # ${GITHUB_WORKSPACE} IS the repo root on the runner, so this lands at the
  # same repo-root-anchored path the local .gitignore rule (/.masking_patterns)
  # already covers -- NOT the filesystem root `/`, which is unwritable.
- name: Run masking scan
  env:
    MASKING_SCAN_PATTERNS_FILE: ${{ github.workspace }}/.masking_patterns
  run: python scripts/check_atlas_masking.py --scan-repo
- name: Remove masking pattern file
  if: always()
  run: rm -f "${GITHUB_WORKSPACE}/.masking_patterns"
```
Requires a new repository secret `MASKING_SCAN_PATTERNS` (a newline-delimited list, same format as the local `.masking_patterns` file — comments starting with `#` and blank lines are ignored) to be added in GitHub repo settings before Phase 134 wires this in. Not created in this plan (out of scope — Phase 134 owns the permanent DATA-05 CI guard).

**Phase-wide COMMIT-DISCIPLINE rule (binding for the rest of Phase 133):** every commit in this phase MUST use explicit-path/hunk `git add` (never `git add -A`, `git add .`, or `git commit -a`). This working tree currently carries pre-existing unrelated in-progress edits (`web/main.py`, `web/pages/browse.py` — R2-1 discovery-review iframe embed work) and the scrubbed-but-uncommitted `genizah_translations.py` that must NEVER be swept into a Phase 133 commit by a blanket add. Every subsequent plan's task-commit step must name each file explicitly and verify `git diff --cached --name-only` immediately before committing.

## Next Phase Readiness

- Plans 133-02 through 133-06 can now build on a repo whose masking-scan gate is proven green (3-pass scan exits 0) and whose scanner (`scripts/check_atlas_masking.py`) is directly reusable: `scan_asset()` is ready to be pointed at the future `atlas_data/` bake output as an exit gate in a later plan (per the phase's own `<verification>`), and Phase 134 can lift `scan_repo`/`scan_asset` near-verbatim into the permanent DATA-05 CI guard (the designed-but-not-wired CI recipe above is the starting point).
- `genizah_translations.py` remains scrubbed-but-uncommitted in the working tree, exactly as the plan requires — 133-03 must stage it explicitly (one path, one commit) rather than relying on any blanket add.
- No blockers. The one operational note for future plans: `--scan-repo` takes ~2 minutes locally on this machine because of the large non-ignored `ACL2026_papers/`/`pgp_data/`/etc. scratch content already present in the working tree outside git's purview — this is a pre-existing environmental condition, not something introduced by this plan, and does not affect a real CI checkout (which only ever has tracked files).

## Self-Check: PASSED

- FOUND: `scripts/check_atlas_masking.py`
- FOUND: `tests/test_atlas_masking_scan.py`
- FOUND: `.gitignore`
- FOUND: commit `f2aa8c3a` in `git log --oneline --all`

---
*Phase: 133-visual-atlas-preview-early-quick-win*
*Completed: 2026-07-20*
