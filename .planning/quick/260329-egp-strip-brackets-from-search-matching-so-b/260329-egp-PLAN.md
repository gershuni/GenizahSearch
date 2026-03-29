---
phase: quick
plan: 260329-egp
type: execute
wave: 1
depends_on: []
files_modified:
  - genizah_core.py
  - tests/test_bracket_search.py
autonomous: true
requirements: []
must_haves:
  truths:
    - "Searching הנתשנ finds documents containing ]הנתשנ"
    - "Searching ]הנתשנ only finds the bracketed form (literal match)"
    - "All search modes handle brackets correctly (text, Responsa, composition, line-break)"
    - "Highlighted snippets preserve original brackets for scholarly notation"
  artifacts:
    - path: "genizah_core.py"
      provides: "Bracket-aware search matching"
    - path: "tests/test_bracket_search.py"
      provides: "Tests for bracket handling in search"
  key_links:
    - from: "build_tantivy_query"
      to: "Tantivy index"
      via: "Bracket-variant terms in OR clause"
      pattern: "bracket.*variant|\\[.*term|term.*\\]"
    - from: "execute_search regex.search(content)"
      to: "content field"
      via: "Bracket stripping before regex match"
      pattern: "_strip_brackets"
---

<objective>
Fix bracket handling in search so bracket-free queries match text containing scholarly brackets
(e.g., searching הנתשנ finds ]הנתשנ) while bracket-containing queries match literally.

Purpose: Genizah transcriptions use square brackets for uncertain/reconstructed text (e.g., [ד]ל[ך]).
Currently the whitespace tokenizer stores bracketed words as single tokens (e.g., `]הנתשנ`), so
Tantivy doesn't return them as candidates for bracket-free queries like `הנתשנ`.

Output: Modified search pipeline in genizah_core.py + test file
</objective>

<execution_context>
@.claude/get-shit-done/workflows/execute-plan.md
@.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@genizah_core.py (lines 5764-5880: build_tantivy_query, lines 5882-6008: build_regex_pattern,
  lines 6605-6910: execute_search, lines 6328-6475: _execute_line_break_search,
  lines 7003-7130: search_composition_logic, lines 882-920: _calculate_match_metrics,
  lines 6010-6040: highlight)

Root cause analysis:
- The main Tantivy index uses `tokenizer_name="whitespace"` (line 4339), so `]הנתשנ` is stored
  as a single token. Query for `"הנתשנ"` does NOT match token `]הנתשנ`.
- The regex phase (line 6902: `regex.search(content)`) would match `הנתשנ` inside `]הנתשנ`
  because brackets are non-word chars, BUT the document never reaches the regex phase because
  Tantivy filtered it out.
- The `_calculate_match_metrics` tokenizer (line 913) uses `[\w\u0590-\u05FF\']+` which
  already ignores brackets naturally.

Two-part fix required:
1. Tantivy query: add bracket-variant terms so bracketed tokens appear in candidate set
2. Regex matching: strip brackets from content for bracket-free queries
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: Add bracket-aware search matching</name>
  <files>genizah_core.py, tests/test_bracket_search.py</files>
  <behavior>
    - Test: _add_bracket_variants("word") returns {"[word", "word]", "[word]", "]word", "word["} plus the original
    - Test: _query_has_brackets("[word") returns True; _query_has_brackets("word") returns False
    - Test: _strip_brackets("[word] test ]other") returns "word test other"
    - Test: build_tantivy_query for term "הנתשנ" includes bracket variants in OR clause
    - Test: regex.search on bracket-stripped content matches bracket-free query
    - Test: regex.search on original content matches bracket-containing query literally
  </behavior>
  <action>
1. Add helper function `_add_bracket_variants(term: str) -> list[str]` near the search helpers section
   (~line 5200). For a given term, returns a list of bracket-adorned variants:
   `[term`, `term]`, `[term]`, `]term`, `term[`. Used to expand Tantivy OR queries.

2. Add helper `_query_has_brackets(query_str: str) -> bool` that returns True if query contains `[` or `]`.

3. Add helper `_strip_brackets(text: str) -> str` that removes all `[` and `]` from text.
   Simple: `text.replace('[', '').replace(']', '')`.

4. Modify `build_tantivy_query` (line 5764) — in BOTH the Responsa branch and the existing path:
   - In the existing path (~line 5840-5878): after building `clean_vars` for each term, also add
     bracket variants of the original term. For each variant from `_add_bracket_variants(term)`,
     add `"{variant}"` to `clean_vars` (no boost needed — they're recall aids).
   - In the Responsa branch (~line 5766-5831): for each component's `tantivy_terms`, also add
     bracket variants of each `original_words` entry.

5. Modify `execute_search` (~line 6897-6905): Before `regex.search(content)`, check if query
   has brackets. If NOT, create `match_content = _strip_brackets(content)` and use that for
   regex matching. Use original `content` for highlighting/display. Specifically:
   ```python
   # Bracket handling: strip brackets from content for bracket-free queries
   match_content = content if _query_has_brackets(query_str) else _strip_brackets(content)
   match_obj = regex.search(match_content)
   ```
   IMPORTANT: For highlighting (lines 6920-6921, `self.highlight(content, regex, ...)`),
   continue using the original `content` WITH brackets so scholarly notation is preserved
   in displayed snippets. If match was found on stripped content, re-search on original
   content for the highlight span. If the re-search on original also matches (likely),
   use that span. If not, fall back to the stripped-content span positions.

6. Apply the same bracket-stripping pattern to `search_composition_logic` (~line 7099-7102):
   ```python
   match_content = content if _query_has_brackets(chunk_terms_str) else _strip_brackets(content)
   if regex.search(match_content):
   ```
   For composition search, the "query" is the chunk text from the source. Chunks from user's
   source text won't contain scholarly brackets, so use a flag or check the original query_str.
   Actually, composition chunks come from user-provided text (not from the index), so they
   won't have brackets. Always strip brackets from index content in composition search.

7. Apply the same pattern to `_execute_line_break_search` (~line 6449-6455):
   ```python
   match_content = content if _query_has_brackets(query_str) else _strip_brackets(content)
   match_obj = regex.search(match_content)
   ```

8. For the `highlight` function (line 6010): when called with stripped content for matching
   but original content for display, re-run `regex.search(content)` on original content.
   If it matches (bracket was between words, not inside), use that. Otherwise, the highlight
   may be slightly offset — acceptable tradeoff. Add a comment explaining this.

9. Handle bracket-containing queries: when user types `]הנתשנ`, the `]` will be in the query.
   - In `build_tantivy_query`: the term `]הנתשנ` should be quoted as `"]הנתשנ"` which Tantivy
     handles correctly (confirmed by testing).
   - In regex matching: `_query_has_brackets` returns True, so content is NOT stripped,
     and regex matches the literal brackets.
   - In `strip_search_diacritics` (line 6617): brackets are NOT diacritical marks, so they
     pass through unchanged. No change needed.
   - IMPORTANT: `re.escape` in `build_regex_pattern` (line 5992) will escape `[` and `]` to
     `\[` and `\]`, so the regex will match them literally. This is correct behavior.

10. Create `tests/test_bracket_search.py` with tests for:
    - `_add_bracket_variants` returns correct variants
    - `_query_has_brackets` detection
    - `_strip_brackets` removes brackets
    - `build_tantivy_query` includes bracket variants (parse output string)
    - Regex matching on stripped vs original content
  </action>
  <verify>
    <automated>cd C:/GenizahSearch && python -m pytest tests/test_bracket_search.py -x -v 2>&1 | tail -30</automated>
  </verify>
  <done>
    - Three helper functions added: _add_bracket_variants, _query_has_brackets, _strip_brackets
    - build_tantivy_query adds bracket variants for each term in both Responsa and standard paths
    - execute_search, search_composition_logic, and _execute_line_break_search strip brackets
      from content before regex matching when query has no brackets
    - Highlighting preserves original brackets in displayed snippets
    - All tests pass
  </done>
</task>

</tasks>

<verification>
1. `python -m pytest tests/test_bracket_search.py -x -v` — all bracket search tests pass
2. `python -m pytest tests/ -x --timeout=60` — no regressions in existing tests
3. Manual verification (if index available): search for a common Hebrew word and confirm
   results include documents where that word appears with brackets
</verification>

<success_criteria>
- Bracket-free queries find documents containing bracketed words (Tantivy returns them as candidates)
- Bracket-containing queries match only the literal bracketed form
- Highlighted snippets preserve scholarly bracket notation
- No regression in existing search tests
</success_criteria>

<output>
After completion, create `.planning/quick/260329-egp-strip-brackets-from-search-matching-so-b/260329-egp-SUMMARY.md`
</output>
