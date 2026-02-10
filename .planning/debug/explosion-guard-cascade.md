---
status: diagnosed
trigger: "Investigate why the explosion guard in Responsa mode jumps straight to a ValueError instead of cascade-downgrading (variants->basic->off->JA off->error)"
created: 2026-02-10T00:00:00Z
updated: 2026-02-10T00:00:00Z
---

## Current Focus

hypothesis: CONFIRMED - Two root causes found
test: Complete
expecting: N/A
next_action: Return diagnosis

## Symptoms

expected: Cascade downgrade - variants->basic->off->JA off->error
actual: ValueError thrown immediately with "estimated 6000 terms"
errors: "Query exceeds the limit of 500 expanded terms (estimated 6000 terms)"
reproduction: Search `#%שלום# #%עולם#` with Variants + JA ON in Responsa mode
started: Unknown - may have always been this way

## Eliminated

- hypothesis: Cascade logic is missing or broken
  evidence: _apply_explosion_guard (line 4732-4825) correctly implements all 4 cascade steps. It does try downgrading variants and JA before raising ValueError.
  timestamp: 2026-02-10

- hypothesis: The estimate is so inflated that cascade downgrades don't bring it below limit
  evidence: Partially true, but the real issue is that the cascade only controls 2 of 5 expansion dimensions. Even with accurate estimates, prefix+suffix+plene exceeds 500 per word.
  timestamp: 2026-02-10

## Evidence

- timestamp: 2026-02-10
  checked: _apply_explosion_guard implementation (genizah_core.py:4732-4825)
  found: Cascade has 3 downgrade steps (variants basic -> variants off -> JA off) but never touches plene_defective, grammatical_prefixes, or grammatical_suffixes
  implication: For queries using prefix+suffix+plene, cascade is powerless - all 3 steps are irrelevant

- timestamp: 2026-02-10
  checked: _count_expanded_terms implementation (genizah_core.py:4674-4729)
  found: Uses multiplicative estimate (24 * 25 * 5 = 3000 per word). Overestimates vs actual (2400 per word after dedup) but both exceed 500.
  implication: Even perfect estimation wouldn't help; actual terms exceed limit

- timestamp: 2026-02-10
  checked: Actual expansion for שלום with #%word# syntax
  found: plene gives 4 variants, then 24 prefixes each = 96, then 25 suffixes each = 2400 after dedup. Two words = 4800.
  implication: prefix+suffix alone gives 600/word (exceeds 500). Adding plene gives 2400/word.

- timestamp: 2026-02-10
  checked: Web UI error handling in run_core_search (web/pages/search.py:1670-1694)
  found: Generic `except Exception` catches ValueError, prints to console, returns []. No ui.notify() for the error.
  implication: User sees 0 results with no explanation

- timestamp: 2026-02-10
  checked: Multiple traceback issue
  found: execute_search can be triggered from: search button click (line 472), Enter keypress (line 323), URL initial_query timer (line 3160), builder dialog submit (line 1420). If query is in URL and user also clicks search, it fires twice.
  implication: Multiple tracebacks likely from URL auto-trigger + manual trigger, or page reloads

## Resolution

root_cause: TWO ROOT CAUSES
  1. INCOMPLETE CASCADE: _apply_explosion_guard() only controls variants_on, variant_mode, and ja_on. It has NO cascade steps for plene_defective, grammatical_prefixes, or grammatical_suffixes (which are component-level properties). For `#%word#` queries, even one word produces 600+ terms (prefix*suffix = 24*25=600) or 2400+ (with plene). The cascade runs through all 3 steps but they're all irrelevant for this query type, so it falls through to ValueError every time.
  2. SILENT ERROR SWALLOWING: web/pages/search.py:1690-1694 catches ALL exceptions (including ValueError from the guard) with a bare `except Exception`, prints to console, and returns empty list. The user sees 0 results with no error message.

fix:
verification:
files_changed: []
