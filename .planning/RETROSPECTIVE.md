# Project Retrospective

*A living document updated after each milestone. Lessons feed forward into future planning.*

## Milestone: v6.5.0 — Search UX & Filtered Search

**Shipped:** 2026-03-14
**Phases:** 5 | **Plans:** 26

### What Was Built
- Search UX overhaul with elapsed timer, ETA, partial results on cancel, 3-state printed filter
- Session persistence with full state restore including 5K+ exclusions, search/composition history
- Bidirectional filtered search by domain/author/work/date/material across all modes
- ~580K Dicta translations (libraries, PGP, FJMS catalog, descriptions, running titles) with translation toggle
- Translation QA infrastructure with 10-heuristic checks and 12,827 automated data fixes

### What Worked
- **User-feedback-driven scoping**: The power user letter (17 requests) gave clear priorities and concrete acceptance criteria
- **UAT gap closure pattern**: Phases 42's 6 gap closure plans (42-04 through 42-09) caught real issues that would have shipped as bugs
- **Batch translation pipeline**: Checkpointing + resume + SIGINT handling made 580K translations reliable with zero failures
- **Translation QA heuristics**: Automated detection of hallucinations, script mismatches, and anomalies prevented bad translations from reaching users
- **Quick task workflow**: 5 quick tasks (15-19) handled urgent fixes without disrupting phase flow

### What Was Inefficient
- **REQUIREMENTS.md scoping**: Requirements were written as "v7.0.0" covering both v6.5.0 and v7.0.0 work, creating confusion during milestone completion about which requirements belonged where
- **Translation toggle scope creep**: Initially added translated-match badges to main search results, then had to remove them (46-05) — should have been clearer about translation display scope upfront
- **Session restore bugs**: Three quick tasks (18-19) and three commits for session restore fixes after the feature was "complete" — indicates insufficient edge case testing

### Patterns Established
- **3-state filter toggle**: all/hide/only pattern for binary properties (printed, domain exclusion)
- **restrict_sys_ids pattern**: Pre-search filtering via sys_id set, applied BEFORE regex for performance
- **Dicta scholarly few-shot**: 16-pair specialized prompts for genizah terminology (JA transliteration, etc.)
- **Translation QC pipeline**: Batch translate → QC audit → stratified sampling → data fixes → user report button
- **Browse-to-search navigation**: incoming_filters via storage/state for cross-page filter carry-over

### Key Lessons
1. **Write requirements scoped to exactly one milestone** — broader requirements docs create confusion at completion time
2. **Translation display scope should be decided before building** — adding then removing features wastes effort
3. **Session persistence needs adversarial testing** — state restore with complex UI (browse tabs, composition summaries, filtered results) has many edge cases

## Cross-Milestone Trends

| Milestone | Phases | Plans | Days | Plans/Day | Key Theme |
|-----------|--------|-------|------|-----------|-----------|
| v1 | 9 | 18 | 3 | 6.0 | PGP integration |
| v5.6.0 | 5 | 25 | 2 | 12.5 | Desktop parity |
| v5.7.0 | 4 | 14 | 2 | 7.0 | Responsa search |
| v5.7.2 | 4 | 11 | 1 | 11.0 | Cleanup |
| v5.7.3 | 3 | 3 | 1 | 3.0 | Corrections |
| v5.8.0 | 4 | 12 | 3 | 4.0 | FJMS integration |
| v5.9.0 | 6 | 22 | 6 | 3.7 | Multi-source images |
| v6.0.0 | 6 | 21 | 6 | 3.5 | Local data architecture |
| v6.1.0 | 1 | 4 | 1 | 4.0 | Catalog browse |
| v6.5.0 | 5 | 26 | 15 | 1.7 | Search UX + translations |

**Observations:**
- v6.5.0 had the lowest plans/day ratio — reflects the large batch translation work (multi-day server jobs) and bug-fix tail
- Gap closure plans (UAT-driven) continue to be valuable but add ~30% overhead to phase count
- Quick tasks are an effective escape valve for urgent fixes during milestone execution
