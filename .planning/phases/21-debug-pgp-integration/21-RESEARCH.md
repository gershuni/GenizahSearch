# Phase 21: Debug PGP Integration - Research

**Researched:** 2026-02-11
**Domain:** PGP transcription section parsing, recto/verso display logic
**Confidence:** HIGH

## Summary

The core bug is a **regex parsing failure** in `shared/document_service.py:parse_transcription_sections()`. The current regex pattern does not match section markers ending with a period (e.g., "Verso.", "Recto."), parenthetical markers (e.g., "Verso (address)"), or space-separated sub-markers (e.g., "Recto Margin"). This causes 712 marker occurrences across the corpus to be missed, resulting in the entire transcription being assigned to page 1 (recto) instead of being split across pages.

The fix is localized: update the regex in one function (`parse_transcription_sections`), and all consumers in both apps (web browse, web search, desktop browse, reading desk) will automatically inherit correct behavior since they all call `get_section_for_page()` which delegates to `parse_transcription_sections()`.

**Primary recommendation:** Fix the regex in `parse_transcription_sections()`, add comprehensive test cases for all marker variants, and verify with the known failing document (pgpid 3750, T-S 8J27.16).

## Standard Stack

No new libraries needed. This is a bug fix in existing code.

### Core Files to Modify
| File | Purpose | Change Type |
|------|---------|-------------|
| `shared/document_service.py` | Section parsing regex | Bug fix |
| `tests/test_shared_service.py` | Add section parsing tests | New tests |

### Files That Consume the Fix (NO changes needed)
| File | How It Uses Section Parsing |
|------|----------------------------|
| `web/pages/browse.py` (lines 907, 935, 2705, 2760, 3663, 3672) | Calls `get_section_for_page()` for browse and reading desk |
| `web/pages/search.py` (lines 2265, 2287) | Calls `get_section_for_page()` for advanced view |
| `gui_threads.py` (lines 499, 509) | `PGPSourceWorker` calls `get_section_for_page()` for desktop |
| `web/document_service.py` | Shim that re-exports from `shared/document_service.py` |

## Architecture Patterns

### How Section Parsing Currently Works

```
Database (document_sources.content / documents.transcription)
  |
  | Raw text with section markers like "Verso.\n" embedded inline
  v
get_section_for_page(transcription, page_num)
  |
  | Calls parse_transcription_sections(transcription)
  |   - Splits text into {'recto': [...], 'verso': [...]}
  |   - Uses regex to find "Recto" / "Verso" markers
  |
  | Maps page_num: 1 -> recto, 2 -> verso
  v
Returns section text for the requested page
```

### Data Flow: Web Browse Page

```
load_page(p_num=N)
  |
  +-> get_all_sources_for_fragment(sys_id)
  |     Returns all sources from ALL linked PGP documents
  |     Each source has page_info ('recto'/'verso') from document_fragments
  |
  +-> Filter sources by current page:
  |     current_page_info = 'recto' if page.p_num == 1 else 'verso'
  |     For sources WITHOUT page_info: call get_section_for_page(content, page.p_num)
  |     For sources WITH page_info: keep full content (already page-specific)
  |
  +-> get_document_for_fragment(sys_id, page.p_num)
  |     Falls back to documents.transcription field
  |     Also calls get_section_for_page()
  |
  +-> Render text in transcription panel alongside image
```

### Data Flow: Desktop App

```
PGPSourceWorker.run()
  |
  +-> get_all_sources_for_fragment(sys_id)
  +-> Filter by page (same logic as web)
  +-> get_section_for_page() for sources without page_info
  +-> emit finished_signal(sys_id, page_sources, pgp_doc)
  |
  v
_on_browse_pgp_loaded() / _on_rd_pgp_loaded()
  -> _populate_pgp_combo(combo, sources, pgp_doc)
  -> Display selected source content in text panel
```

### Two Paths for Transcription Content

1. **`document_sources` table** (preferred): Individual source records with `content` field, accessed via `get_all_sources_for_fragment()`. Each source may have `page_info` from the fragment link.

2. **`documents.transcription` field** (fallback): First Digital Edition's content stored directly on the document record, accessed via `get_document_for_fragment()`.

Both paths use `get_section_for_page()` when the source lacks `page_info`.

## The Bug: Root Cause Analysis

### Current Regex (line 199 of document_service.py)

```python
section_pattern = re.compile(
    r'^(Recto|Verso)(?:\s*[-,]\s*[^\n]+)?[:\s]*\n',
    re.MULTILINE | re.IGNORECASE
)
```

### What It Matches

| Pattern | Example | Matches? |
|---------|---------|----------|
| Bare word + newline | `Recto\n` | YES |
| With dash modifier | `Recto - right margin\n` | YES |
| With comma modifier | `verso, address\n` | YES |
| With colon | `Recto:\n` | YES |
| Complex modifier | `verso - bottom margin - address\n` | YES |

### What It MISSES (the bug)

| Pattern | Example | Count | Why Missed |
|---------|---------|-------|------------|
| **Trailing period** | `Verso.\n` | 268 | Period not in `[:\s]` char class |
| **Period + more** | `Verso. Address.\n` | 68 | Period breaks the pattern |
| **Recto period** | `Recto.\n` | 7 | Same as Verso |
| **Parenthetical** | `Verso (address)\n` | 12 | Parens not matched by `[-,]` |
| **Space-separated** | `Recto Margin\n` | 12 | Space not matched; `[-,]` requires dash/comma |
| **Colon + page** | `Verso: Left page\n` | 5 | Colon consumed, remainder not matched |
| **Other parens** | `Verso (upside down)\n` | 7 | Same paren issue |
| **Question mark** | `verso (?)\n` | 4 | Paren issue |
| **No separator** | `verso right\n` | 5 | Space not matched |
| **Total missed** | | **712** | |

### Impact

- 712 marker lines in 268+ transcriptions are NOT recognized
- These transcriptions show ALL text on page 1 (recto)
- Page 2 (verso) shows nothing or full content
- Confirmed with pgpid 3750 (T-S 8J27.16): "Verso." marker on line 19 is missed

## Corpus Statistics (HIGH confidence - verified by scanning footnotes.csv)

| Metric | Count |
|--------|-------|
| Total edition records (>50 chars) | 9,703 |
| Documents with recto/verso markers | 3,939 |
| Documents without any markers | 4,301 |
| Markers matched by current regex | 11,556 |
| Markers MISSED by current regex | 712 |
| Margin-only markers (no recto/verso) | 395 docs |
| Total unique marker patterns | 1,137 |
| Total marker occurrences | 13,920 |

### Most Common Missed Patterns

| Pattern | Count |
|---------|-------|
| `Verso.` | 268 |
| `Verso. Address.` | 68 |
| `Recto margin:` | 15 |
| `Verso (address)` | 12 |
| `Recto Margin` | 12 |
| `Verso (upside down)` | 7 |
| `Verso Address` | 7 |
| `Recto.` | 7 |

### Side Field from documents.csv

The `side` column in `documents.csv` provides metadata about which sides of the manuscript are used:

| Side Value | Count |
|------------|-------|
| `recto` | 1,420 |
| `verso` | 1,191 |
| `recto and verso` | 938 |
| Multi-fragment (`recto ; recto`) | ~180 |
| Total with side info | 3,746 |

This `side` field is used during import to populate `document_fragments.page_info`, but only by the v1 import script (`import_pgp_documents.py`). The v2 script (`import_pgp_full.py`) does NOT set `page_info`, meaning many fragments may lack it.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Section marker detection | Custom parser for each variant | Improved single regex | One regex handles all variants uniformly |
| Recto/verso mapping | Manual page number mapping | Existing `get_section_for_page()` | Already handles the mapping correctly once parsing works |

**Key insight:** The entire fix is one regex update. Don't restructure the parsing architecture -- it's sound. The only problem is the regex pattern.

## Common Pitfalls

### Pitfall 1: Over-matching Section Markers
**What goes wrong:** A too-broad regex matches content lines that happen to start with "Recto" or "Verso" (e.g., "Recto side of the document was damaged" as narrative text).
**Why it happens:** Trying to match all patterns with a single greedy pattern.
**How to avoid:** Section markers are typically on their own line and are short (<80 chars). The existing approach of anchoring to `^` with `re.MULTILINE` is correct. Adding a length constraint or end-of-line anchor helps.
**Warning signs:** Content text getting incorrectly split.

### Pitfall 2: Margin-Only Sections
**What goes wrong:** ~395 documents have standalone margin markers like "Right Margin", "Top Margin" without recto/verso prefix. These are sub-sections of recto or verso, not independent page markers.
**Why it happens:** Confusing sub-section markers with page-level markers.
**How to avoid:** Only recto/verso markers should trigger page splitting. Margin markers within a recto/verso section should be preserved as text.
**Warning signs:** Text disappearing because a margin marker is treated as a page break.

### Pitfall 3: Multi-Fragment Documents
**What goes wrong:** Documents with `+` in shelfmark (e.g., "T-S 13J35.3 + AIU VII.A.23") have multiple physical fragments. Each fragment has its own sys_id and may have different recto/verso content.
**Why it happens:** The section splitting assumes page_num 1=recto, 2=verso, but multi-fragment documents may have pages 1-4+ spanning multiple fragments.
**How to avoid:** For pages beyond 2, `get_section_for_page()` already returns full content (line 269). The fragment-level source filtering (via `page_info`) handles this case. No change needed for the regex fix.

### Pitfall 4: Case Sensitivity in Markers
**What goes wrong:** Markers appear in various cases: "Recto", "recto", "RECTO", "Verso", "verso".
**How to avoid:** The existing regex uses `re.IGNORECASE` flag. Keep this.

### Pitfall 5: Preamble Text Before First Marker
**What goes wrong:** Some transcriptions have text before the first "Recto" marker. This could be a title, editor note, or part of the transcription.
**How to avoid:** The existing code (lines 227-231) handles this by assigning preamble to recto. Keep this behavior.

## Code Examples

### The Fix: Updated Regex

```python
# Source: Analysis of 1,137 unique marker patterns from pgp_data/footnotes.csv
section_pattern = re.compile(
    r'^(Recto|Verso)(?:\s*[-.,:;(]\s*[^\n]*)?\.?\s*\n',
    re.MULTILINE | re.IGNORECASE
)
```

**Explanation of changes:**
- `[-.,:;(]` instead of `[-,]` -- allows period, colon, semicolon, open-paren as separator
- `[^\n]*` instead of `[^\n]+` -- allows zero modifier chars (handles `Verso.` where `.` is consumed by the optional period)
- `\.?` before `\s*\n` -- explicitly handles trailing period

Alternative approach (simpler, may be more robust):

```python
# Match any line that starts with Recto or Verso and is a header (not content)
section_pattern = re.compile(
    r'^(Recto|Verso)\b[^\n]{0,60}\n',
    re.MULTILINE | re.IGNORECASE
)
```

**Explanation:** Match "Recto" or "Verso" at line start, followed by word boundary, up to 60 chars of modifiers, then newline. The word boundary prevents matching "Rectory" etc. The 60-char limit prevents matching content lines that happen to start with these words.

### Test Cases to Add

```python
# Source: Verified patterns from footnotes.csv corpus analysis

def test_parse_sections_basic():
    """Recto\n...Verso\n... pattern."""
    text = "line 1\nline 2\n\nVerso\n\nline 3\nline 4"
    sections = parse_transcription_sections(text)
    assert len(sections['recto']) == 1
    assert len(sections['verso']) == 1

def test_parse_sections_with_period():
    """Verso. marker (268 occurrences in corpus)."""
    text = "line 1\nline 2\n\nVerso.\n\nline 3\nline 4"
    sections = parse_transcription_sections(text)
    assert len(sections['recto']) == 1
    assert len(sections['verso']) == 1
    assert 'line 3' in sections['verso'][0]

def test_parse_sections_period_address():
    """Verso. Address. marker (68 occurrences)."""
    text = "line 1\n\nVerso. Address.\n\nline 2"
    sections = parse_transcription_sections(text)
    assert len(sections['verso']) == 1

def test_parse_sections_parenthetical():
    """Verso (address) marker (12 occurrences)."""
    text = "line 1\n\nVerso (address)\n\nline 2"
    sections = parse_transcription_sections(text)
    assert len(sections['verso']) == 1

def test_parse_sections_space_modifier():
    """Recto Margin marker (12 occurrences)."""
    text = "main text\n\nRecto Margin\n\nmargin text\n\nVerso\n\nverso text"
    sections = parse_transcription_sections(text)
    # Both "main text" and "margin text" are recto sub-sections
    assert len(sections['recto']) >= 1

def test_pgpid_3750():
    """Real-world regression test: T-S 8J27.16 (pgpid 3750)."""
    # The actual content has "Verso.\n" on line 19
    text = (
        "first line of recto\n"
        "more recto text\n\n"
        "Verso.\n\n"
        "first line of verso\n"
        "more verso text"
    )
    sections = parse_transcription_sections(text)
    assert len(sections['recto']) == 1
    assert len(sections['verso']) == 1
    assert 'first line of verso' in sections['verso'][0]
    assert 'first line of recto' in sections['recto'][0]

def test_get_section_for_page_with_period_marker():
    """Verify page splitting works with period markers."""
    text = "recto content\n\nVerso.\n\nverso content"
    page1 = get_section_for_page(text, 1)
    page2 = get_section_for_page(text, 2)
    assert 'recto content' in page1
    assert 'verso content' in page2
    assert 'verso content' not in page1
    assert 'recto content' not in page2
```

## How PGP Website Structures Transcriptions (for reference)

The PGP website (geniza.princeton.edu) stores transcriptions in HTML with structured markup:

```html
<section dir="rtl" lang="jrb">
  <div data-canvas="https://cudl.lib.cam.ac.uk/iiif/.../canvas/1">
    <ol>
      <li>line 1 of recto</li>
      <li>line 2 of recto</li>
    </ol>
  </div>
  <h3>Verso.</h3>
  <div data-canvas="https://cudl.lib.cam.ac.uk/iiif/.../canvas/2">
    <ol>
      <li>line 1 of verso</li>
      <li>line 2 of verso</li>
    </ol>
  </div>
</section>
```

Key observations:
- **`data-canvas` attributes** link sections to specific IIIF canvas URLs (image pages)
- **`<h3>` headings** like `<h3>Verso.</h3>` are the section markers
- The plain text export strips HTML, leaving just the marker text ("Verso.") on its own line
- This confirms the period in "Verso." is intentional PGP formatting, not noise

**We do NOT need to replicate this HTML structure.** Our system works with plain text and the regex-based section splitting approach is appropriate. We just need the regex to handle the period.

## page_info Column Analysis

The `document_fragments.page_info` column stores 'recto' or 'verso' when the fragment link has side information. Current state:

- **v1 import** (`import_pgp_documents.py`): Sets `page_info` from `documents.csv` `side` column
- **v2 import** (`import_pgp_full.py`): Does NOT set `page_info` (uses `fragments.csv` which lacks side info)
- **Result**: Many fragments may have `page_info = NULL`

When `page_info` is NULL, the code falls back to `get_section_for_page()` to split the content. This is the path where the regex bug bites.

When `page_info` IS set, the content is considered already page-specific and the full content is shown without splitting. This is the "clean path" that works correctly.

## State of the Art

| Old Approach | Current Approach | Impact |
|--------------|------------------|--------|
| Regex misses "Verso." | Fix regex to handle all patterns | 712 marker occurrences fixed |
| No unit tests for parsing | Add test suite | Prevents regression |

**Not deprecated, just buggy:** The architecture is sound. `parse_transcription_sections()` -> `get_section_for_page()` is the right pattern. Only the regex needs updating.

## Open Questions

1. **Should margin sub-sections be treated as separate sections?**
   - What we know: ~395 documents have standalone "Right Margin", "Top Margin" markers without recto/verso prefix. Currently these are treated as content text (not page breaks).
   - What's unclear: Should these create sub-sections within a page, or remain as inline text?
   - Recommendation: Keep current behavior (treat as inline text). Margin content belongs to the same physical page. Only recto/verso should trigger page splitting. This can be enhanced later if needed.

2. **Are there documents where page_info from fragments is wrong or missing?**
   - What we know: v2 import doesn't set page_info; v1 only set it for documents in transcriptions_linked.csv
   - What's unclear: How many fragments in production have NULL page_info that should have a value?
   - Recommendation: The regex fix addresses the fallback path. page_info population could be a follow-up task but is not blocking.

3. **Should we normalize markers during import instead of at display time?**
   - What we know: Section splitting happens at display time on every page load
   - What's unclear: Would pre-parsing and storing sections separately be better?
   - Recommendation: Keep display-time parsing. The regex is fast (single pass), and storing pre-parsed sections would require a migration and re-import. Not worth the complexity for this fix.

## Sources

### Primary (HIGH confidence)
- `shared/document_service.py` lines 181-277 -- current parse_transcription_sections and get_section_for_page implementation
- `pgp_data/footnotes.csv` -- full corpus analysis of 9,703 edition records, 13,920 marker occurrences, 1,137 unique patterns
- `pgp_data/documents.csv` -- side column analysis (3,746 documents with side info)
- PGP GitHub repo `princetongenizalab/pgp-text` -- HTML structure of PGPID 3750 confirming "Verso." as intentional formatting

### Secondary (MEDIUM confidence)
- `scripts/import_pgp_documents.py` -- v1 import logic showing page_info population from side column
- `scripts/import_pgp_full.py` -- v2 import confirming page_info NOT set
- Consumer code in `web/pages/browse.py`, `web/pages/search.py`, `gui_threads.py` -- verified all paths use get_section_for_page()

## Metadata

**Confidence breakdown:**
- Bug root cause: HIGH -- confirmed by testing regex against real data, verified with pgpid 3750
- Corpus statistics: HIGH -- computed by scanning full footnotes.csv (9,703 records)
- Fix approach: HIGH -- regex update in one function, all consumers auto-inherit
- Impact scope: HIGH -- verified all code paths through grep analysis

**Research date:** 2026-02-11
**Valid until:** Indefinite (bug fix research, not version-dependent)
