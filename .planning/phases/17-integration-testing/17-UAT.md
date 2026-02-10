---
status: diagnosed
phase: 17-integration-testing
source: 17-01-SUMMARY.md, 17-02-SUMMARY.md
started: 2026-02-10T16:00:00Z
updated: 2026-02-10T17:00:00Z
---

## Current Test

[testing complete]

## Tests

### 1. Automated Tests Pass
expected: Run `pytest tests/test_responsa_core.py tests/test_responsa_integration.py tests/test_responsa_parity.py tests/test_responsa_edge_cases.py tests/test_responsa_regression.py -v` and all ~186 tests pass (green). No failures or errors.
result: pass

### 2. Web: Responsa Mode Activation
expected: Open web app. In the search mode dropdown, select "Responsa (R)". Sub-options row appears below with checkboxes: Variants, Judeo-Arabic, Flex Spacing. Bidirectional is in Advanced Options. A syntax legend/help text is visible explaining Responsa operators (#, %, *).
result: issue
reported: "When I choose in dropdown it works. BUT when I write R, space and another character (shouldn't be - R+Space should switch and delete the R) it changes to Responsa without the other row"
severity: major

### 3. Web: Basic Responsa Search
expected: With Responsa mode active, type a Hebrew word (e.g., שלום) and search. Results appear showing documents containing that word. Result count and highlighted matches are shown.
result: pass

### 4. Web: Prefix Expansion (#word)
expected: With Responsa mode active, search for `#שלום`. Results should include documents containing forms with Hebrew prefixes like בשלום, השלום, ושלום, לשלום, etc. Result count should be higher than searching for plain שלום.
result: issue
reported: "Search got stuck in animation, 'Connection lost'. Console shows 42,213 Tantivy hits, 18,858 after dedup. Core engine works but web UI chokes rendering that many results. Stop button did not help."
severity: blocker

### 5. Web: Suffix Expansion (word#)
expected: With Responsa mode active, search for `שלום#`. Results should include documents containing forms with Hebrew suffixes like שלומו, שלומם, שלומנו, etc.
result: pass

### 6. Web: Plene/Defective Variants (%word)
expected: With Responsa mode active, search for `%שלום`. Results should include both plene (with ו/י) and defective (without) spelling variants.
result: pass

### 7. Web: Wildcard Search (*word / word*)
expected: With Responsa mode active, search for `שלום*` (suffix wildcard). Results should include words starting with שלום followed by any characters. Try `*שלום` for prefix wildcard too.
result: issue
reported: "Only gives שלום results. Tantivy query has only base term (\"שלום\"^5), regex is (שלום\S*) with final ם — doesn't match שלומו which has regular מ. Sofit-to-normal conversion not applied before wildcard pattern."
severity: major

### 8. Web: Judeo-Arabic Expansion
expected: With Responsa mode active and JA checkbox ON, search for `#כלמה`. Results should include Judeo-Arabic article forms like אלכלמה, ואלכלמה, etc.
result: pass

### 9. Web: Flex Spacing
expected: With Responsa mode active and Flex Spacing checkbox ON, search for a multi-word query. The search should be more tolerant of spacing variations in the source text (OCR artifacts, unusual whitespace).
result: pass
note: Console error "client this element belongs to has been deleted" (residual from Test 4 crash). Search toolbar disappeared after search — likely same cause.

### 10. Web: Variants Checkbox
expected: With Responsa mode active and Variants checkbox ON, search for a word. Results should include paleographic/orthographic variant forms in addition to the Responsa expansions.
result: issue
reported: "Toolbar disappeared again after search. Repeatable bug — not residual from Test 4 crash. Happens after Responsa search completes."
severity: major

### 11. Web: Tabular Query Builder
expected: With Responsa mode active, click the "Query Builder" button. A dialog/panel opens with 2-4 component columns. Each column has word inputs and per-word modifier checkboxes (prefix #, suffix #, wildcard *, plene %). There are distance spinners between components. Click "Apply" and the generated syntax appears in the search field and search executes.
result: pass

### 12. Web: URL State Persistence
expected: After performing a Responsa search with some options checked, look at the browser URL. It should contain parameters like `?mode=responsa&variants=1&ja=1&flex_spaces=1`. Refreshing the page should restore the search with those options.
result: pass

### 13. Web: Mode Switching (Responsa Off)
expected: Switch from Responsa mode back to Exact or Variants mode. The Responsa sub-options row disappears. Search works normally in the selected mode. No errors or leftover Responsa behavior.
result: pass

### 14. Web: Explosion Guard
expected: With Responsa mode + Variants + JA all ON, search for something that would generate many expanded terms (e.g., `#%שלום# #%עולם#`). If expansion exceeds 500 terms, a warning message appears indicating the cascade downgrade (variants->basic->off->JA off).
result: issue
reported: "Guard triggers but jumps straight to ValueError (6000 terms) instead of cascade-downgrading. Error only in console, not shown in web UI — user sees 0 results with no explanation. Also fired multiple times (repeated tracebacks)."
severity: major

### 15. Desktop: Responsa Mode Activation
expected: Open desktop app. In the search mode dropdown/combo, select "Responsa (R)". Sub-option checkboxes appear (Variants, JA, Flex Spacing, Bidirectional). Syntax legend is visible.
result: pass

### 16. Desktop: Responsa Search with Prefix
expected: In desktop with Responsa mode, search for `#שלום`. Results appear with prefix-expanded matches. Highlighted text shows the matched forms.
result: pass

### 17. Desktop: Tabular Query Builder
expected: In desktop with Responsa mode, click "Query Builder" button. A QDialog opens with component columns, word inputs, modifier checkboxes, and distance spinners. Construct a query and click Apply. The syntax appears in the search field and search executes.
result: issue
reported: "Works but should be RTL even in English, just like the web"
severity: minor

### 18. Desktop: Existing Modes Unchanged
expected: Switch desktop to Exact mode and search for a known term. Results are correct. Switch to Variants mode -- results include variants. Switch to Fuzzy -- fuzzy results appear. No regressions from Responsa additions.
result: pass

## Summary

total: 18
passed: 12
issues: 6
pending: 0
skipped: 0

## Gaps

- truth: "R+Space shortcut activates Responsa mode with sub-options row visible"
  status: failed
  reason: "User reported: When I write R, space and another character, it changes to Responsa without the other row (sub-options not shown)"
  severity: major
  test: 2
  root_cause: "on_query_input_change (search.py:350) sets mode_select.value programmatically but NiceGUI doesn't fire update:model-value event, so on_mode_change() (line 573) never executes and responsa_sub_row visibility is never toggled"
  artifacts:
    - path: "web/pages/search.py"
      issue: "Line 350: mode_select.value = target_mode without calling on_mode_change()"
  missing:
    - "Call on_mode_change() after setting mode_select.value in shortcut handler"
  debug_session: ".planning/debug/responsa-shortcut-suboptions.md"

- truth: "Prefix expansion (#word) returns results without UI freeze"
  status: failed
  reason: "User reported: Search got stuck in animation, Connection lost. 42,213 Tantivy hits, 18,858 after dedup. Core engine works but web UI chokes rendering that many results."
  severity: blocker
  test: 4
  root_cause: "app.storage.user['search_results'] = results (search.py:1719) stores ALL 18,858 results with full_text into NiceGUI user storage, which serializes 37-94MB JSON over WebSocket. Also unbounded batch Supabase lookup for 18,858 IDs and missing [:200] limit in apply_filters/clear_filters."
  artifacts:
    - path: "web/pages/search.py"
      issue: "Line 1719: stores unbounded results in app.storage.user"
    - path: "web/pages/search.py"
      issue: "Lines 868, 879: apply_filters/clear_filters missing [:200] render limit"
    - path: "web/pages/search.py"
      issue: "Lines 1698-1706: unbounded batch transcription lookup for all result IDs"
  missing:
    - "Cap app.storage.user results to [:200] and strip full_text"
    - "Cap get_sys_ids_with_transcriptions to displayed result IDs only"
    - "Add [:200] limit to apply_filters and clear_filters render calls"
  debug_session: ".planning/debug/responsa-connection-lost.md"

- truth: "Suffix wildcard (word*) matches words starting with stem regardless of sofit letters"
  status: failed
  reason: "User reported: Only gives שלום results. Regex שלום\\S* has final ם but text has regular מ in שלומו. Sofit-to-normal conversion not applied before wildcard pattern."
  severity: major
  test: 7
  root_cause: "_build_wildcard_regex() (genizah_core.py:4861-4868) takes regex_terms with sofit letters and builds regex without converting sofit-to-normal. Also build_tantivy_query has no wildcard-aware handling for recall."
  artifacts:
    - path: "genizah_core.py"
      issue: "Lines 4861-4868: _build_wildcard_regex has no sofit conversion"
    - path: "genizah_core.py"
      issue: "Lines 4988-5028: build_tantivy_query has no wildcard support"
  missing:
    - "Replace trailing sofit in suffix wildcard with [םמ] char class (and leading sofit for prefix wildcard)"
    - "Add sofit-converted stem to Tantivy query for better recall"
  debug_session: ".planning/debug/suffix-wildcard-sofit-mismatch.md"

- truth: "Search toolbar remains visible after Responsa search completes"
  status: failed
  reason: "User reported: Toolbar disappears after Responsa search. Repeatable."
  severity: major
  test: 10
  root_cause: "Symptom of connection instability from Gap 2 (large result set WebSocket overload). JS scroll auto-collapse handler (search.py:786-812) sets CSS display:none on expanded_panel; after connection loss/reconnect the CSS state becomes stale."
  artifacts:
    - path: "web/pages/search.py"
      issue: "Lines 786-812: JS scroll collapse handler CSS persists after connection issues"
  missing:
    - "Fix Gap 2 (root cause). Toolbar issue resolves when connection stays stable."
  debug_session: ".planning/debug/responsa-connection-lost.md"

- truth: "Explosion guard cascade-downgrades before erroring, and shows warning in web UI"
  status: failed
  reason: "User reported: Guard jumps straight to ValueError (6000 terms) instead of cascade-downgrading. Error only in console, not shown in web UI."
  severity: major
  test: 14
  root_cause: "Cascade only controls 2 of 5 expansion dimensions (variants, JA) but ignores 3 component-level flags (prefixes 24x, suffixes 25x, plene ~4x). A single #word# = 600 terms exceeds 500 limit. Web UI run_core_search (search.py:1690-1694) swallows ValueError silently, returns empty list."
  artifacts:
    - path: "genizah_core.py"
      issue: "Lines 4732-4825: _apply_explosion_guard missing cascade steps for plene, suffixes, prefixes"
    - path: "web/pages/search.py"
      issue: "Lines 1690-1694: run_core_search swallows ValueError, shows 0 results"
  missing:
    - "Add cascade steps 4-6: disable plene_defective, then suffixes, then prefixes"
    - "Surface ValueError in web UI via ui.notify instead of silent empty results"
  debug_session: ".planning/debug/explosion-guard-cascade.md"

- truth: "Desktop tabular query builder uses RTL layout matching web version"
  status: failed
  reason: "User reported: Works but should be RTL even in English, just like the web"
  severity: minor
  test: 17
  root_cause: "genizah_app.py:4367-4368 sets RTL only when CURRENT_LANG == 'he', but web version sets RTL unconditionally. Same at line 4503-4504 for preview label."
  artifacts:
    - path: "genizah_app.py"
      issue: "Lines 4367-4368, 4503-4504: conditional RTL check should be unconditional"
  missing:
    - "Remove if CURRENT_LANG == 'he' condition, always set RightToLeft"
  debug_session: ".planning/debug/desktop-tabular-rtl.md"
