# Feature Plan: User-Added Text Search

**Status:** Planning
**Date:** 2026-02-05
**Priority:** Future

---

## Overview

Allow users to add their own text files/collections to the search index, enabling them to search for parallels between their personal texts and the Genizah corpus.

## Use Cases

1. **Researcher with unpublished transcription** - User has their own transcription of a manuscript and wants to find parallels in the Genizah
2. **Comparative text study** - User has a non-Genizah text (e.g., Talmudic commentary, medieval poetry) and wants to find Genizah parallels
3. **Personal library** - User maintains a collection of texts they frequently reference

## Key Decisions Needed

### Text Division Strategy

When user uploads long text, it needs to be divided into searchable "pages" for the index. Two approaches discussed:

#### Option A: Delimiter-Based Division
- User specifies delimiter (blank line, period, colon, custom)
- Each section becomes a "page" in the index
- **Pros:** Respects natural text structure (paragraphs, verses)
- **Cons:** Uneven page sizes, may create very short or very long pages

#### Option B: Fixed Word Count Division
- Divide text every X words (e.g., 100, 200, 500)
- **Pros:** Consistent page sizes, predictable indexing
- **Cons:** May cut in middle of sentences, loses structural meaning

#### Option C: Hybrid (Recommended)
- Primary: Divide by delimiter (user-chosen)
- Fallback: If section exceeds X words, subdivide by word count
- If section is too short, merge with next section
- **Parameters:**
  - `delimiter`: What marks a section break
  - `max_words`: Maximum words per page (default: 500)
  - `min_words`: Minimum words per page before merging (default: 20)

### UI Considerations

1. **Upload interface**
   - File upload (txt, docx, pdf?)
   - Paste text directly
   - Import from URL?

2. **Preview before indexing**
   - Show how text will be divided
   - Let user adjust parameters
   - Show word count, page count

3. **Management**
   - List user's imported texts
   - Edit/delete
   - Re-index with different parameters

### Technical Implementation

1. **Storage**: Where to store user text?
   - Supabase (cloud) - syncs across devices
   - Local only - privacy-focused option

2. **Index**: How to integrate with search?
   - Separate Tantivy index per user
   - Single index with user_id field
   - Search user texts + Genizah together or separately

3. **Search result display**
   - Clear indication that result is from user's text
   - Link back to original document/page

## Precedent in Codebase

The **Boundary Search** feature (`BOUNDARY_SEARCH_SPEC.md`) already implements similar text division logic:

```python
def parse_boundaries(text: str, delimiter: str, min_distance: int = 3) -> list[int]:
    """
    Find word indices where boundaries occur.
    """
```

This code can be adapted for user text pagination.

## Related Features

- **Parallel Search** - User text could be source for parallel search
- **Cross-paragraph search** - Boundary detection already implemented
- **External transcriptions** - Similar import workflow for PGP data

## Open Questions

1. Should user texts be searchable by other users (with permission)?
2. What file formats to support? (txt, docx, pdf, html)
3. Maximum text size limit?
4. Should divisions create actual "pages" or just index entries?
5. How to handle Hebrew + English mixed texts?

## Next Steps

1. [ ] Decide on text division strategy (A, B, or C)
2. [ ] Design UI mockups for upload + preview
3. [ ] Decide storage location (cloud vs local)
4. [ ] Create database schema
5. [ ] Implement prototype

---

*Created: 2026-02-05*
