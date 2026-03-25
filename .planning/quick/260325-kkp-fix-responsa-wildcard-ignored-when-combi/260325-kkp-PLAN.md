---
phase: quick
plan: 260325-kkp
type: execute
wave: 1
depends_on: []
files_modified:
  - genizah_core.py
autonomous: true
requirements: [BUG-FIX]

must_haves:
  truths:
    - "Responsa suffix wildcard (e.g., הושיע*) combined with line-break pipe (|) produces a regex that matches suffixed forms like הושיענו on the target line"
    - "Responsa prefix wildcard (e.g., *נדר) combined with pipe produces a regex with \\S* prefix pattern"
    - "Non-wildcard line-break searches remain unaffected by the fix"
    - "Tantivy prefilter for wildcard components uses content field (not line_starts/line_ends) and adds sofit-converted stems for suffix wildcards"
  artifacts:
    - path: "genizah_core.py"
      provides: "Fixed _build_line_break_regex and _execute_line_break_search"
  key_links:
    - from: "_build_line_break_regex"
      to: "_build_wildcard_regex"
      via: "Dispatches wildcard components to _build_wildcard_regex instead of re.escape"
    - from: "_execute_line_break_search"
      to: "Tantivy query building"
      via: "Uses content field + sofit stems for wildcard components"
---

<objective>
Fix `_build_line_break_regex` and `_execute_line_break_search` in genizah_core.py where Responsa wildcard searches (suffix *, prefix *, pattern *a*b*) are silently ignored when combined with line-break pipe syntax (|) or line position anchors.

The active line-break execution path is:
1. `_parse_line_break_query` parses `|` syntax into `LineGroup` objects (components preserve `.wildcard` info) -- CORRECT, no fix needed
2. `_execute_line_break_search` builds Tantivy prefilter + calls `_build_line_break_regex` -- NEEDS FIX (Tantivy recall + regex)
3. `_build_line_break_regex` builds compiled regex from expanded_groups -- NEEDS FIX (ignores wildcards)
4. `regex.search(content)` on each Tantivy candidate -- works once regex is correct

NOT in scope: `_validate_line_break_match` (dead code -- defined but never called on the active path).
</objective>

<execution_context>
@C:\Users\gersh\.claude\get-shit-done\workflows\execute-plan.md
@C:\Users\gersh\.claude\get-shit-done\templates\summary.md
</execution_context>

<context>
@genizah_core.py (lines 5531-5579: _build_wildcard_regex, lines 5792-5802: sofit Tantivy handling in normal path, lines 6240-6302: _build_line_break_regex, lines 6304-6400: _execute_line_break_search)
</context>

<tasks>

<task type="auto">
  <name>Task 1: Fix _build_line_break_regex to dispatch wildcard components</name>
  <files>genizah_core.py</files>
  <action>
**Fix `_build_line_break_regex` (line ~6250)**

The function already receives `line_groups` (List[LineGroup]) which contain `.components` (List[ResponsaComponent]) with `.wildcard` info. Currently, the inner loop iterates `expanded_groups[gi]` and blindly does `re.escape(w)` on every word.

Change the inner loop to check the corresponding component's wildcard type:

```python
for ci, word_set in enumerate(expanded_groups[gi]):
    comp = group.components[ci] if ci < len(group.components) else None
    if comp and comp.wildcard:
        # Build wildcard regex using the existing helper
        comp_dict = {
            'wildcard': comp.wildcard,
            'wildcard_pattern': comp.wildcard_pattern,
            'regex_terms': sorted(word_set, key=len, reverse=True),
            'original_words': comp.words,
        }
        wc_pat = _build_wildcard_regex(comp_dict)
        if wc_pat:
            comp_parts.append(wc_pat)
        else:
            # Fallback: treat as plain words
            sorted_words = sorted(word_set, key=len, reverse=True)
            escaped = [make_mark_tolerant_pattern(re.escape(w)) for w in sorted_words]
            if escaped:
                comp_parts.append(f"({'|'.join(escaped)})")
    else:
        # Existing plain-word path (unchanged)
        sorted_words = sorted(word_set, key=len, reverse=True)
        escaped = [make_mark_tolerant_pattern(re.escape(w)) for w in sorted_words]
        if not escaped:
            continue
        comp_parts.append(f"({'|'.join(escaped)})")
```

**Fix `_execute_line_break_search` Tantivy prefilter (line ~6326-6337)**

Replace the Tantivy query building block to handle wildcards:

```python
is_first = (comp == group.components[0])
is_last = (comp == group.components[-1])

if is_first and group.line_start:
    field = 'line_starts'
elif is_last and group.line_end:
    field = 'line_ends'
else:
    field = 'content'

# For wildcard components, search in content field (not positional)
# since the matched word may differ from the base stem
if comp.wildcard in ('suffix', 'prefix', 'pattern'):
    for w in comp.words:
        tantivy_parts.append(f'content:"{w}"')
    # Suffix wildcards: add sofit-converted stem for better recall
    # (matches normal path behavior at build_tantivy_query lines 5796-5802)
    if comp.wildcard == 'suffix':
        for w in comp.words:
            if w and w[-1] in _SOFIT_TO_NORMAL:
                converted = w[:-1] + _SOFIT_TO_NORMAL[w[-1]]
                tantivy_parts.append(f'content:"{converted}"')
else:
    for w in comp.words:
        tantivy_parts.append(f'{field}:"{w}"')
```

Verify: run existing Responsa tests to ensure no regression, then manually test with a debug print that `_build_line_break_regex` produces `\S*` patterns for wildcard queries.
  </action>
  <verify>
    <automated>cd C:/GenizahSearch && python -m pytest tests/test_responsa_core.py tests/test_responsa_integration.py tests/test_responsa_edge_cases.py tests/test_responsa_regression.py -x -v 2>&1</automated>
  </verify>
  <done>Both functions fixed. _build_line_break_regex dispatches wildcard components to _build_wildcard_regex. _execute_line_break_search uses content field + sofit stems for wildcard Tantivy queries. All existing Responsa tests pass.</done>
</task>

<task type="auto">
  <name>Task 2: Verify fix with targeted regex output test</name>
  <files>genizah_core.py</files>
  <action>
Write a quick inline verification: import the fixed functions and confirm:

1. Parse `הושיע*|` through `_parse_line_break_query` -- verify component has `wildcard='suffix'`
2. Build line-break regex for the parsed groups -- verify regex pattern contains `\S*` (not just escaped literal)
3. Verify the compiled regex matches text like `הושיענו\nשלום` (suffix extended form at end of line)
4. Verify the compiled regex does NOT match text like `שלום\nעולם` (no match at all)
5. Parse `|*נדר` -- verify prefix wildcard produces `\S*` prefix pattern in regex

Run via: `python -c "..."` inline test script.

Also run the full test suite to check for regressions: `python -m pytest tests/ -x --timeout=120`
  </action>
  <verify>
    <automated>cd C:/GenizahSearch && python -m pytest tests/ -x --timeout=120 2>&1</automated>
  </verify>
  <done>Inline verification confirms wildcard patterns appear in line-break regex. Full test suite passes with no regressions.</done>
</task>

</tasks>

<verification>
1. All existing Responsa test files pass (no regression):
   - test_responsa_core.py
   - test_responsa_integration.py
   - test_responsa_edge_cases.py
   - test_responsa_regression.py
2. Inline script confirms wildcard regex patterns for suffix/prefix
3. Full test suite: `python -m pytest tests/ -x --timeout=120`
</verification>

<success_criteria>
- Suffix wildcard (e.g., הושיע*) combined with | pipe produces regex with \S* pattern, not escaped literal
- Prefix wildcard (e.g., *נדר) combined with | pipe produces regex with \S* prefix pattern
- Tantivy query for wildcard components in line-break uses content field and includes sofit-converted stems for suffix
- All 221+ existing Responsa tests pass unchanged
</success_criteria>

<output>
After completion, create `.planning/quick/260325-kkp-fix-responsa-wildcard-ignored-when-combi/260325-kkp-SUMMARY.md`
</output>
