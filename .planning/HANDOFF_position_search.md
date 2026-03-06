# Handoff: Position Search / Join Detection

**Date:** 2026-03-06
**Status:** Core + line-break syntax implemented, search fast, **highlighting needs fix**

## What Was Built

### Refinement 1: Exact position post-filter (DONE — previous session)

Tantivy uses broad fields (first 10 words for `content_head`, last 10 for `content_tail`) for fast candidate retrieval. `Indexer._validate_position_match()` validates exact position.

### Refinement 2: Per-line positional tokens (DONE — previous session)

Index includes `L{n}:word` positional tokens alongside plain tokens.

### Refinement 3: Consecutive-line search with | syntax (DONE — this session)

**Syntax (in Responsa mode):**
- `|word` — line starts with word
- `word|` — line ends with word
- `|word1 word2 |word3` — line starts with "word1 word2", next line starts with "word3"
- `|x y [|2] |z` — line starts with "x y", skip 2 lines, then line starts with "z"
- `word1 | word2` — word1 on one line, word2 on next line (any position)

**Architecture:** Regex-based matching. `_build_line_break_regex()` builds a multiline regex from line groups (e.g. `^\s*גדול.*$\n^\s*אני.*$`). Used for both filtering AND highlighting. No post-filter needed — regex handles everything.

**Tabular builder (both apps):**
- New "Lines" scope radio button (alongside Word Range / Within Document)
- Distance spinners = lines to skip between groups (label changes to "lines")
- New "Start of line |_" and "End of line _|" modifier checkboxes (visible only in Lines scope)

### Refinement 4: Translation strings (DONE)

7 Hebrew translation strings added.

## Known Issues — Must Fix Next Session

### 1. Regex highlighting broken in snippet and ResultDialog
The multiline regex pattern matches correctly (search results are accurate and fast), but the `self.highlight()` method doesn't produce correct highlighted snippets for multiline matches. The match spans across newlines, and the existing highlight logic (designed for single-line word matches) doesn't handle this well.

**Root cause:** `self.highlight()` at genizah_core.py:~5536 uses `regex.search(text)` to find the match span, then builds a snippet around it with `*markers*`. For multiline patterns with `^` and `$` anchors, the snippet extraction and `\n` → space flattening can misalign the highlight markers.

**Fix needed:**
- Either adapt `self.highlight()` to handle multiline regex spans correctly
- Or build a custom highlight method for line-break results that highlights each matched line separately
- Also need to verify the `highlight_pattern` stored in results works for re-highlighting in ResultDialog (currently stores the multiline regex pattern string)

### 2. ResultDialog re-highlighting
The `highlight_pattern` field in search results stores the regex pattern string. ResultDialog uses this to re-highlight when the user opens a result. The multiline pattern may not work correctly with ResultDialog's highlighting logic.

## Files Changed

| File | Changes |
|------|---------|
| `genizah_core.py` | `_has_line_break_syntax()`, `_parse_line_break_query()`, `LineGroup` dataclass, `_build_line_break_regex()`, `_expand_responsa_component()`, `_execute_line_break_search()`, `generate_tabular_syntax()` Lines scope, `_validate_line_break_match()` (kept but unused — regex approach replaced it) |
| `genizah_app.py` | Lines radio button, line_start/line_end modifier checkboxes, scope change handler, _update_preview, _on_modifier_changed, _on_word_focus, _MOD_DISPLAY |
| `web/pages/search.py` | Lines toggle, line_start/line_end checkboxes, on_scope_change, make_word defaults, MOD_DISPLAY, _build_mod_indicator_text |
| `genizah_translations.py` | 7 new Hebrew translation strings |

## Key Code Locations

- `_has_line_break_syntax()`: genizah_core.py ~line 4393
- `_parse_line_break_query()`: genizah_core.py ~line 4437
- `LineGroup` dataclass: genizah_core.py ~line 4427
- `_build_line_break_regex()`: genizah_core.py ~line 6057 (SearchEngine static method)
- `_execute_line_break_search()`: genizah_core.py ~line 6120
- `generate_tabular_syntax()` Lines support: genizah_core.py ~line 4535
- `self.highlight()`: genizah_core.py ~line 5536 — **needs multiline fix**
- Desktop Lines radio: genizah_app.py `_rb_lines` ~line 6997
- Web Lines toggle: web/pages/search.py scope_toggle ~line 2418

## UAT Remaining

1. **Fix highlighting** — snippet and ResultDialog (priority)
2. Tabular builder Lines scope — verify preview and search
3. Gap syntax `[|N]` — verify with actual search
4. Responsa modifiers within line-break syntax
5. Position dropdown + line-break search combo
6. Hebrew UI labels
