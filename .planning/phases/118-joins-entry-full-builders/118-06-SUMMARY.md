---
phase: 118-joins-entry-full-builders
plan: "06"
subsystem: joins-lab-builder
tags: [joins-lab, builder, word-model, tdd, ui, i18n]
dependency_graph:
  requires: ["118-03", "118-04"]
  provides: [word-level-builder, compose-mapping, symbol-indicators]
  affects: [web/components/joins_builder.py, web/pages/joins_lab.py]
tech_stack:
  added: []
  patterns: [tdd-red-green, closure-local-state, in-place-ui-update]
key_files:
  created:
    - tests/test_word_builder_model.py
    - tests/test_joins_builder_word_ui.py
  modified:
    - web/components/joins_builder.py
    - genizah_translations.py
decisions:
  - "Word gap [N] in term string (not a BuilderRow field): words with gap>0 emit '[N]' token into line term; compose() treats it as part of the term string, which is correct since compose() only adds [|N] gaps between rows"
  - "Symbol row refreshed in-place via _sym_rows registry keyed by 'sym_{li}_{wi}' to avoid full re-render on modifier toggle (Guardrail 3 WR-05)"
  - "line_start/line_end moved from per-word mods to per-LINE fields in lines_state (plan design); _apply_modifiers_to_term unchanged (no line anchor responsibility)"
metrics:
  duration: "30min"
  completed: "2026-06-18"
  tasks: 2
  files: 4
---

# Phase 118 Plan 06: Word-Level Builder Redesign Summary

One-liner: Word-box builder with per-word modifier menus + responsa symbol indicators + Add word/Add line, composing to engine syntax via [N] word-gaps and [|N] line-gaps using the unchanged compose().

## Tasks Completed

| # | Task | Commit | Files |
|---|------|--------|-------|
| 1 | RED word-model tests | faa5a275 | tests/test_word_builder_model.py |
| 1 | GREEN: word-level build_side_query + hoist mapping | 2c5d9315 | web/components/joins_builder.py, tests/test_word_builder_model.py |
| 2 | Word-box builder UI + per-word symbols + Add word/line | 1c45c142 | tests/test_joins_builder_word_ui.py, genizah_translations.py |

## What Was Built

### Task 1 — Word-level model + compose mapping (TDD)

Rewrote `build_side_query()` in `web/components/joins_builder.py` to accept the
new lines-with-words shape:

```python
lines_state = [
  {
    'words': [
      {'term': str, 'mods': dict, 'gap_to_next_word': int},
      ...
    ],
    'line_start': bool,
    'line_end': bool,
    'gap_to_next_line': int,
  },
  ...
]
```

For each line: each word is hoisted via `_apply_modifiers_to_term`, then joined
with `[N]` gap tokens (gap > 0) or plain space (gap == 0) to form `line_term`.
ONE `BuilderRow(term=line_term, line_start=..., line_end=..., gap_to_next=...)` per line.
`compose()` in `shared/joins_lab.py` is reused UNCHANGED.

Added helpers: `_default_word()`, `_default_line()`, `_normalize_word_mods()`.

12 RED tests written first (all failing against old row-shape API), then 4 more
added during GREEN for edge cases. All 16 pass.

### Task 2 — Word-builder UI + symbol indicators + Add word/Add line

Rewrote the row area of `create_joins_builder()` (top Text Position + mode selector
kept as-is; handle contract preserved):

- **Per line**: a column with a header row (line number, ⊢/⊣ toggle buttons, line gap,
  remove-line) and a word row (word units + gap boxes + Add word button)
- **Per word unit**: text input (RTL, Hebrew font) + tune button (modifier menu) + 
  remove-word button + symbol indicator row beneath
- **Symbol indicators**: active modifiers shown as responsa symbols (`#_`, `_#`, `%`,
  `*_`, `_*`, `−`) with hover tooltips, updated in-place via `_sym_rows` registry
- **In-place updates**: state updated on value/modifier change; UI re-rendered only
  on structural add/remove (Guardrail 3 WR-05)
- **All new strings via tr()** with Hebrew keys added to genizah_translations.py:
  Prefix, Suffix, Plene / defective, Wildcard before/after, Negation (labels),
  May carry a prefix/suffix, Plene / defective spelling variants, Must NOT appear
  (tooltips), Line start/end (⊢/⊣), Line starts/ends here, + Add word, + Add line,
  Remove word, Remove line, Word options, ↕ gap, Fuzzy search is slower...

10 new UI construction tests added.

## Test Results

```
73 passed total (all regression tests green):
  - tests/test_word_builder_model.py: 16 passed
  - tests/test_joins_builder_word_ui.py: 10 passed
  - tests/test_joins_lab_render.py: 1 passed
  - tests/test_joins_lab_options_inline.py: 5 passed
  - tests/test_text_position_summary.py: 10 passed
  - tests/test_builder_modifier_hoist.py: 13 passed
  - tests/test_merge_globals_web.py: 10 passed
  - tests/test_other_side_page_contract.py: 8 passed
```

## Deviations from Plan

None — plan executed exactly as written.

`line_start`/`line_end` were already removed from `_apply_modifiers_to_term`'s
responsibility in Phase 118-04 (the existing code had that comment). The Plan noted
this explicitly. No deviation.

## Known Stubs

None — the word boxes accept real term input; the compose mapping is fully wired.

## Threat Flags

None — no new network endpoints, auth paths, or schema changes. Builder state
is closure-local (Phase 87). No raw app.storage.user introduced.

## Self-Check: PASSED

- web/components/joins_builder.py: FOUND
- tests/test_word_builder_model.py: FOUND
- tests/test_joins_builder_word_ui.py: FOUND
- genizah_translations.py: FOUND
- Commits faa5a275, 2c5d9315, 1c45c142: VERIFIED in git log
- 73 tests green
- ruff clean on all changed files
- No raw app.storage.user (Phase 87 guard test passes)
- No .add() on NiceGUI elements
