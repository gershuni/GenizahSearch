---
phase: 81B
plan: 04
subsystem: skill-documentation
tags: [skill, anthropic-skill, SKILL.md, documentation, wave-2, R1, R2, R6]

requires:
  - phase: 81B-01
    provides: skill source-tree skeleton, fixture corpus, RED tests
  - phase: 79-api-browse-drill-down
    provides: text_source enum (pgp_transcription|snippet|none), browse envelope
  - phase: 80-api-parallels
    provides: parallels envelope (filtered[], matches[]), mode enum
  - phase: 81A-api-contract-expansion
    provides: search_mode enum (5 values), request echo block, responsa_options

provides:
  - skills/cairo-genizah-research/SKILL.md: Anthropic Skill instruction file (Level 2); 204 lines, valid YAML frontmatter
  - skills/cairo-genizah-research/README.md: Human-facing install/usage guide with acceptance run procedure
  - skills/cairo-genizah-research/references/api_contract.md: Level-3 locked envelope shapes for debugging
  - .planning/REQUIREMENTS.md: SKILL-04 R2 enum mismatch closed (pgp_transcription replaces 'full')

affects:
  - 81B-05 (acceptance run — SKILL.md is the instruction the model loads on trigger)

tech-stack:
  added: []
  patterns:
    - "Anthropic Skill progressive disclosure: L1 frontmatter / L2 SKILL.md body / L3 on-demand references"
    - "R2 enum mapping: pgp_transcription -> no annotation (equiv-of-full); snippet|none -> annotate"
    - "D-09 env-wins: GENIZAH_API_BASE overrides --base-url (inverted CLI convention)"

key-files:
  created:
    - skills/cairo-genizah-research/SKILL.md
    - skills/cairo-genizah-research/README.md
    - skills/cairo-genizah-research/references/api_contract.md
  modified:
    - .planning/REQUIREMENTS.md (SKILL-04 line only — 1 line changed)

key-decisions:
  - "SKILL.md body 204 lines (well under 500-line cap); description 625 chars (under 1024 cap)"
  - "R2 mapping: SKILL.md step 5 explicitly documents pgp_transcription as the equiv-of-full trigger; references _FULL_TEXT_SOURCE in format_output.py"
  - "R1 surface compatibility: Surface table in SKILL.md + NOT SUPPORTED in README; Claude API explicitly excluded"
  - "D-09 env-wins documented in both SKILL.md Configuration section and README"
  - "api_contract.md uses 81A D-09 locked enum (5 values: exact|variants|responsa|title|shelfmark, no regex)"
  - "REQUIREMENTS.md SKILL-04 patch scoped to one line; git diff shows 1 insertion + 1 deletion"

metrics:
  duration: ~4min
  completed: 2026-05-04T16:22:55Z
  tasks: 3
  files_created: 3
  files_modified: 1
---

# Phase 81B Plan 04: SKILL.md + README + API Contract Reference Summary

**Anthropic Skill instruction file (SKILL.md, 204 lines) + human-facing README + Level-3 api_contract.md reference, with REQUIREMENTS.md SKILL-04 R2 enum mismatch closed**

## Performance

- **Duration:** ~4 min
- **Started:** 2026-05-04T16:18:38Z
- **Completed:** 2026-05-04T16:22:55Z
- **Tasks:** 3
- **Files created:** 3
- **Files modified:** 1 (.planning/REQUIREMENTS.md, 1 line)

## Accomplishments

- Authored `skills/cairo-genizah-research/SKILL.md` (204 lines, under 500-line cap) with valid YAML frontmatter (`name: cairo-genizah-research`, description 625 chars under 1024 cap), 9 Level-2 headings as model navigation anchors
- SKILL.md documents all required constraints: R1 surface compatibility table (Claude Code YES, Claude API NO), R2 text_source mapping (pgp_transcription = no annotation, snippet|none = annotate), D-09 env-wins precedence, D-08 no-retry policy, full error-code → inline-note table, Tier A/B/C ranking schema
- Created `skills/cairo-genizah-research/README.md` (91 lines) with installation steps for Claude Code and Desktop, explicit "NOT SUPPORTED" note for Claude API (R1), acceptance run procedure, and architecture table showing three-level progressive disclosure
- Created `skills/cairo-genizah-research/references/api_contract.md` (155 lines) as the Level-3 on-demand reference covering locked envelope shapes for all three endpoints, R2 mapping note, 81A search_mode enum (5 values, regex dropped per D-09), and full error code catalogue
- Patched `.planning/REQUIREMENTS.md` SKILL-04: replaced `text_source != 'full'` trigger with `text_source != 'pgp_transcription'` plus R2 mapping attribution note (Phase 81B Plan 04, 2026-05-04); git diff shows exactly 1 line changed

## Task Commits

1. **Task 1: SKILL.md instruction file** - `a6b09981` (feat)
2. **Task 2: README install guide + references/api_contract.md** - `371561ff` (feat)
3. **Task 3: Patch REQUIREMENTS.md SKILL-04** - `aca04967` (fix)

## Files Created/Modified

- `skills/cairo-genizah-research/SKILL.md` — 204-line Anthropic Skill instruction file; YAML frontmatter valid; R1/R2/R6/D-08/D-09 all documented
- `skills/cairo-genizah-research/README.md` — 91-line human-facing guide; installation + smoke test + acceptance procedure
- `skills/cairo-genizah-research/references/api_contract.md` — 155-line Level-3 reference; locked envelope shapes for /api/search, /api/browse, /api/parallels
- `.planning/REQUIREMENTS.md` — SKILL-04 line patched; 1 insertion + 1 deletion; all other requirements byte-unchanged

## Decisions Made

- **SKILL.md line count: 204** (well under 500-line cap). Body structure uses 9 H2 headings as anchors the model navigates quickly during skill execution.
- **R2 mapping documented in SKILL.md step 5 and api_contract.md:** Phase 79 D-10 enum `pgp_transcription | snippet | none` has no `'full'` value. Skill maps `pgp_transcription` as the "full text available" signal. Both documents explain the mismatch with REQUIREMENTS.md SKILL-04 original wording.
- **api_contract.md uses 81A D-09 locked enum:** 5 values only (`exact | variants | responsa | title | shelfmark`); regex was dropped by Phase 81A D-09. The plan template showed 6 values including regex, but the locked contract from 81A-CONTEXT.md is authoritative.
- **REQUIREMENTS.md patch attribution:** Added `(R2 mapping note added 2026-05-04 by Phase 81B Plan 04: ...)` parenthetical so future contributors understand the change origin without needing git blame.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] api_contract.md search_mode enum corrected from 6 to 5 values**
- **Found during:** Task 2 authoring
- **Issue:** The plan's template for api_contract.md showed `"exact" | "variants" | "regex" | "responsa" | "title" | "shelfmark"` (6 values). However Phase 81A CONTEXT.md D-09 explicitly dropped `regex` from the v7.10 enum — the locked contract has 5 values. The plan template was written before 81A locked this decision.
- **Fix:** api_contract.md uses the 5-value enum (`exact | variants | responsa | title | shelfmark`) and notes "Phase 81A D-09 dropped regex". Also noted that `regex_pattern_too_long` error code is NOT in v7.10.
- **Files modified:** `skills/cairo-genizah-research/references/api_contract.md`
- **Commit:** `371561ff`

## Known Stubs

None — this is a documentation plan. SKILL.md references `scripts/format_output.py` and `scripts/stage.py` which will be implemented in Plan 03 (running in parallel as Wave 2). The references are forward pointers to Plan 03's deliverables, not stubs in the rendering sense.

## Threat Surface Scan

No new network endpoints, auth paths, or schema changes introduced. All three files are read-only documentation consumed by the model at skill-trigger time. The REQUIREMENTS.md patch is a single-line wording correction with no behavioral impact on running code.

## Self-Check: PASSED

Files created:
- `skills/cairo-genizah-research/SKILL.md` FOUND
- `skills/cairo-genizah-research/README.md` FOUND
- `skills/cairo-genizah-research/references/api_contract.md` FOUND
- `.planning/REQUIREMENTS.md` (modified) FOUND

Commits:
- `a6b09981` FOUND (Task 1)
- `371561ff` FOUND (Task 2)
- `aca04967` FOUND (Task 3)

Verification results:
- SKILL.md: 204 lines (<=500), description 625 chars (<=1024), YAML valid, 9 H2 sections
- README.md: 91 lines (>=30), Installation present, NOT SUPPORTED present
- api_contract.md: 155 lines (>=40), search_mode present (6 occurrences), pgp_transcription present (3 occurrences), R2 mapping present, error codes >= 6
- REQUIREMENTS.md: SKILL-04 has pgp_transcription, Phase 79 reference, no old 'full' trigger; all 6 SKILL requirements present; git diff shows 1 line changed
- pytest tests/test_skill_throttle.py: 7 passed (Plan 02 tests still GREEN)

---
*Phase: 81B-claude-skill-consumer*
*Completed: 2026-05-04*
