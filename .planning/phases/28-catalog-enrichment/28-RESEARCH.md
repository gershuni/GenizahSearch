# Phase 28: Catalog Enrichment - Research

**Researched:** 2026-02-15
**Domain:** FJMS catalog metadata display (SQLite sidecar to UI)
**Confidence:** HIGH (based on direct database analysis of actual sidecar data)

## Summary

This research investigated the actual data in `fjms_enrichment.db` to answer six specific research questions from the CONTEXT.md. The findings significantly reshape the implementation approach compared to initial expectations.

**Key discoveries:**
1. The `DescriptionEng` and `DescriptionHeb` columns are **completely empty** (0 rows with content). They were exported from `IdentificationTextEng`/`IdentificationTextHeb` in FIST, which are also empty at the source level. The actual "description" content lives in `TextualFrameEng`/`TextualFrameHeb` (29% population rate, ~93K rows).
2. Only 30.7% of AlmaIds (69,453 of 226,456) have ANY non-empty catalog field. 65.9% of all rows are entirely empty.
3. Multiple records per AlmaId represent **different scholarly identifications of textual content** on the manuscript, not competing metadata. The TextualFrame field is the primary differentiator (87% of multi-record AlmaIds differ on this field).
4. There is **no source attribution** in the current sidecar export. The FIST database has `SourceId` on `dbo_Signature` linking to scholarly teams (e.g., "Catalogs", "Institution", "Talmudic Literature"), but this was not exported. Re-exporting with source info is feasible but requires modifying `export_fist_enrichment.py`.
5. There is **no NLI Aleph data overlap** to worry about. NLI MARC data (fetched live by GenizahSearch) contains bibliography, notes, subjects, people, and dimensions. FJMS catalog data contains titles, author, copy date/place, and textual content identification. These are complementary, not overlapping.

**Primary recommendation:** Treat TextualFrame as the primary "catalog description" field. Display it as "Content Identification" rather than "Description." The multi-record approach should present distinct textual identifications as a list, with deduplication of empty/redundant records. Add SourceName to the sidecar export to enable per-record attribution.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- FJMS catalog data lives in its own section, distinct from existing PGP metadata
- Web: FJMS data appears within the existing metadata card area
- Desktop: FJMS data appears as a sub-section INSIDE the existing "Extended Information" collapsible area
- Empty state: If no FJMS catalog data, show nothing
- Show title matching the app's current interface language (Hebrew or English)
- Multiple FJMS descriptions: Show all, stacked vertically with source labels
- Author display: Show manuscript author prominently AND cataloger in smaller text
- Primary fields: Title and content description most important
- Secondary fields: Author, date, place, source attribution
- Adaptive display: Only show fields that have data
- Purple badge pattern for FJMS source distinction

### Claude's Discretion
- Section ordering (FJMS above or below PGP)
- Web sub-header divider vs inline labeled fields
- Purple badge per-field vs section header attribution
- Whether to add "PGP" labels to existing metadata
- Flagging PGP/FJMS disagreements
- Description truncation strategy
- Visual distinction between PGP and FJMS descriptions
- Title role: header vs section field (research informs this)

### Deferred Ideas (OUT OF SCOPE)
- FTS5 catalog search UI
- NLI crossreference import (~424K PartOf relationships)
- FJMS display mode selector (combined/table/by-source)
</user_constraints>

## Research Question Answers

### Q1: Multi-Record Distribution

**Confidence: HIGH** (direct SQLite queries against actual sidecar)

| Records per AlmaId | Count | Percentage |
|---------------------|-------|-----------|
| 1 record | 168,173 | 74.3% |
| 2 records | 39,836 | 17.6% |
| 3 records | 11,893 | 5.3% |
| 4 records | 3,324 | 1.5% |
| 5+ records | 5,230 | 2.3% |
| Max | 129 records | 1 AlmaId |

**Among multi-record AlmaIds (58,283 total), what differs:**
- TextualFrame: 87% differ (50,760 AlmaIds) -- this is the dominant differentiator
- CopyDate: 18% differ (10,686 AlmaIds)
- Title: 3% differ (1,587 AlmaIds)
- AuthorText: 3% differ (1,538 AlmaIds)
- CopyPlace: 2% differ (904 AlmaIds)

**Key insight:** Multiple records represent different scholarly content identifications for the same physical manuscript. Each record describes what textual content the scholar identified on the manuscript (e.g., one says "Bible: Exodus," another says "Biblical Exegesis: Leviticus 25:29-32"). These are complementary, not conflicting.

**Practical impact on UI:** For multi-record AlmaIds, the TextualFrame entries should be displayed as a list of content identifications. Title, Author, and CopyDate are mostly consistent across records, so these can be deduplicated/merged. The 65.9% of completely empty rows should be filtered out before display.

### Q2: NLI Overlap Analysis

**Confidence: HIGH** (compared actual NLI MARC fetch code with FJMS catalog schema)

**There is NO meaningful overlap.** The data is complementary:

| Field | NLI MARC (live fetch) | FJMS Catalog (sidecar) |
|-------|----------------------|----------------------|
| Title | Hebrew title from CSV/MARC | Title, TitleHeb (rare: 0.6%) |
| English Title | MARC 246 field | None |
| Author | None | AuthorText (0.5%) |
| Date | MARC 260/264 $c | CopyDate (6.5%) |
| Place | None | CopyPlace (0.4%) |
| Description | None | TextualFrame (28.9%) |
| Physical | MARC 300 dimensions | None |
| Subjects | MARC 650 | None (domains are separate table) |
| Bibliography | MARC 581 | None |
| Notes | MARC 500 | None |
| People | MARC 700 | None |

**Conclusion:** NLI MARC provides bibliographic/physical metadata. FJMS catalog provides scholarly content identification (what the text IS). No deduplication needed. No conflict detection needed.

### Q3: Sidecar Data Scope

**Confidence: HIGH** (analyzed schema and FIST source tables)

The current sidecar export maps to FJMS's **catalog record** level (the second tab on the FJMS website). It does NOT contain "identification" level data (the first tab) because `IdentificationTextEng`/`IdentificationTextHeb` are empty at the FIST source.

**Schema mapping:**

| Sidecar Column | FIST Source Column | FJMS Tab | Population Rate |
|---------------|-------------------|----------|-----------------|
| Title | cat.Title | Catalog Record | 0.6% (1,843 rows) |
| TitleHeb | cat.GenizahTitleText | Catalog Record | 0.4% (1,306 rows) |
| AuthorText | cat.AuthorText | Catalog Record | 0.5% (1,616 rows) |
| CopyDate | cat.CopyDate | Catalog Record | 6.5% (20,958 rows) |
| CopyPlace | cat.CopyPlace | Catalog Record | 0.4% (1,361 rows) |
| DescriptionEng | cat.IdentificationTextEng | Identification | **0% (empty)** |
| DescriptionHeb | cat.IdentificationTextHeb | Identification | **0% (empty)** |
| TextualFrameHeb | cat.BI_TextualFrameHeb | Catalog Record | 28.9% (93,312 rows) |
| TextualFrameEng | cat.BI_TextualFrameEng | Catalog Record | 28.9% (93,312 rows) |

**What IS NOT in the export but exists in FIST:**
- `SourceId` on `dbo_Signature` -> `dbo_CodeSource.EngDesc`/`HebDesc` (scholarly team name)
- `CopyToDate` on `dbo_UnitCatalogRec` (end of date range)
- `Comment` on `dbo_UnitCatalogRec` (cataloger notes)
- `Colophon`, `ColophonFolio` (colophon info)

**Recommendation:** Modify the export to include `SourceName` and `SourceNameHeb` from `dbo_CodeSource` via `dbo_Signature.SourceId`. This enables per-record attribution without a major schema change.

### Q4: Description/TextualFrame Patterns

**Confidence: HIGH** (sampled 30+ entries, analyzed length distribution)

TextualFrame uses a structured notation: `[$Category$]: Specific Reference`

**Examples:**
- `[$Bible$]: Leviticus 23:40 - 41`
- `[$Talmud Bavli$]: Shabbat 5 a - 7 a`
- `[$Mishneh Torah$]: Shehitah 1:1 - 4`
- `@[$Piyyut$]: "Title of the poem" (author)` (the `@` prefix appears on some entries)
- `[$Documents$]: 13th Dhu al-Hijja, 596 A.H.`
- `[$Biblical Exegesis - Karaite$]: Deuteronomy`

**Length distribution:**

| Range | Count | % of TextualFrame records |
|-------|-------|--------------------------|
| 0-30 chars | 15,768 | 17% |
| 31-60 chars | 62,206 | 67% |
| 61-100 chars | 11,190 | 12% |
| 101-200 chars | 1,791 | 2% |
| 201-500 chars | 1,097 | 1% |
| 500+ chars | 1,260 | 1% |

**Average:** 62 chars (English), 57 chars (Hebrew). Most entries are short, structured references.

**Format considerations:**
- The `[$...$]` notation could be parsed to extract the category and make it visually distinct (e.g., bold category, then content reference)
- Hebrew and English versions exist in parallel (same structure, different language)
- Some entries have multiple semicolon-separated references (e.g., `Bava Batra 75 a - 78 a; 82 b - 87 a`)
- The `@` prefix on some entries appears to mark entries with embedded work titles in quotes

**Recommendation:** Display TextualFrame as "Content Identification." Parse the `[$...$]` notation to bold the category portion. No truncation needed since 96% are under 100 chars. Show the language matching the current interface language.

### Q5: Title Comparison

**Confidence: HIGH** (direct analysis)

FJMS titles are **extremely rare** (0.6% of records = 1,843 rows with English Title, 0.4% = 1,306 with Hebrew Title). This means:

- **For 99.4% of manuscripts:** Only PGP/NLI titles will exist. FJMS title is irrelevant.
- **For the 0.6% with FJMS titles:** Titles tend to be Hebrew work names (e.g., "Shir ha-Shirim," specific piyyut titles). These are complementary to the NLI Hebrew title (which is often a generic shelfmark-based title).
- **Only 254 records** have both Title AND TextualFrame.

**Recommendation:** FJMS title should NOT be used as a page header or replace the main title. It should appear as a field within the FJMS catalog section when available. It's too rare to warrant special header treatment.

### Q6: Field Population Rates

**Confidence: HIGH** (direct counts, distinct AlmaIds)

| Field | Distinct AlmaIds | % of all AlmaIds | Notes |
|-------|-----------------|------------------|-------|
| TextualFrame | 57,563 | 25.4% | **Primary useful field** |
| CopyDate (real) | 13,119 | 5.8% | Excluding 0 and -99 sentinel values |
| Title (Eng) | 1,674 | 0.7% | Very rare |
| AuthorText | 1,600 | 0.7% | Very rare |
| CopyPlace | 1,255 | 0.6% | Very rare |
| TitleHeb | 1,276 | 0.6% | Very rare |
| DescriptionEng | 0 | 0% | **Completely empty** |
| DescriptionHeb | 0 | 0% | **Completely empty** |

**Special CopyDate values:**
- `0`: 3,820 records (likely "unknown date")
- `-99`: 468 records (likely "not applicable")

**Overall:** Only 69,453 AlmaIds (30.7%) have any non-empty field. Of the 204,039 AlmaIds that overlap with libraries.csv, approximately 28% have useful data (extrapolated from 500-ID sample).

**AlmaIds with data-only counts:**
- TextualFrame only (no title/author/date/place): 50,570 AlmaIds
- The vast majority of useful catalog data is TextualFrame content identification

## Standard Stack

### Core (Already Exists)
| Library | Version | Purpose | Status |
|---------|---------|---------|--------|
| shared/fjms_service.py | Current | FjmsService singleton for SQLite sidecar access | Exists, needs enhancement |
| web/fjms_service.py | Current | Backward-compatibility shim | Exists |
| scripts/export_fist_enrichment.py | 1.0.0 | Sidecar export script | Needs SourceName column added |

### No New Dependencies Required
This phase requires NO new libraries. All work is:
1. Modifying the export script to add `SourceName`/`SourceNameHeb` columns
2. Adding a `get_catalog_records()` method to FjmsService (returns list, not single)
3. Adding UI rendering code to web/pages/browse.py and genizah_app.py

## Architecture Patterns

### Recommended Changes

```
scripts/export_fist_enrichment.py    # Add SourceName, SourceNameHeb to catalog table
shared/fjms_service.py               # Add get_catalog_records() returning list[dict]
web/pages/browse.py                  # Add FJMS catalog section to metadata panel
genizah_app.py                       # Add FJMS catalog HTML to extended info
genizah_translations.py              # Add translation keys
```

### Pattern 1: get_catalog_records() (replaces get_catalog())

The existing `get_catalog()` method uses `fetchone()` and returns a single dict. This is wrong for multi-record AlmaIds. The new method should:

```python
def get_catalog_records(self, sys_id: str) -> list[dict]:
    """Get all non-empty catalog records for a manuscript.

    Returns list of dicts with keys: title, title_heb, author_text,
    copy_date, copy_place, textual_frame_heb, textual_frame_eng,
    source_name, source_name_heb.

    Filters out completely empty records. Deduplicates identical records.
    """
```

Keep `get_catalog()` for backward compatibility but deprecate it.

### Pattern 2: Record Deduplication and Merging

Since multi-record AlmaIds share metadata (title, author) but differ on TextualFrame:

```python
def merge_catalog_records(records: list[dict]) -> dict:
    """Merge multiple catalog records into display-ready structure.

    Returns:
        {
            'title': str or None,          # First non-empty title
            'title_heb': str or None,      # First non-empty Hebrew title
            'author_text': str or None,    # First non-empty author
            'copy_date': str or None,      # Most common non-sentinel date
            'copy_place': str or None,     # First non-empty place
            'textual_frames': [            # All distinct TextualFrames
                {'eng': str, 'heb': str, 'source_name': str},
                ...
            ],
            'record_count': int,           # Total records before dedup
        }
    """
```

### Pattern 3: TextualFrame Rendering

Parse the `[$Category$]: Content` notation for visual formatting:

```python
import re

def parse_textual_frame(text: str) -> tuple[str, str]:
    """Parse '[$Category$]: Content' into (category, content).

    Returns ('', full_text) if no pattern match.
    """
    text = text.strip().lstrip('@')  # Remove optional @ prefix
    match = re.match(r'\[\$(.+?)\$\]\s*:\s*(.*)', text, re.DOTALL)
    if match:
        return match.group(1).strip(), match.group(2).strip()
    return '', text
```

### Pattern 4: Web Metadata Section Placement

Current browse.py metadata panel structure:
```
ui.card (metadata panel)
  ui.grid (2-col) -- Library, Shelfmark, SysID, Title, Pages, FL ID, Oxford metadata
  ui.separator
  h3('External link') -- NLI Ktiv, Oxford, Cambridge, PGP links
  ui.separator (if PGP data)
  h3('Princeton Geniza Project') -- PGP metadata section
  FJMS Domain Classifications -- domain links
  Related Fragments -- join links
```

FJMS catalog data should be inserted as a new section **between PGP metadata and FJMS domains:**
```
  ... PGP metadata section ...
  ui.separator (if FJMS catalog data)
  h3('FJMS Catalog') with purple accent -- FJMS catalog section
  ... FJMS domains ...
```

### Pattern 5: Desktop Extended Info Placement

Current desktop `on_enriched_data_loaded` builds HTML:
```
<div wrapper>
  KTI/Oxford/Cambridge enrichment HTML
  PGP metadata HTML (green left-border)
  FJMS domain HTML (purple left-border)   <-- added in Phase 26
</div>
```

FJMS catalog should be added between PGP and domains:
```
<div wrapper>
  KTI/Oxford/Cambridge enrichment HTML
  PGP metadata HTML (green left-border)
  FJMS catalog HTML (purple left-border)  <-- NEW
  FJMS domain HTML (purple left-border)
</div>
```

### Anti-Patterns to Avoid
- **Displaying empty rows:** 65.9% of catalog rows are completely empty. Filter these out.
- **Showing CopyDate = 0 or -99:** These are sentinel values meaning "unknown" or "N/A."
- **Single-record assumption:** The existing `get_catalog()` returns one record. Must use `get_catalog_records()` returning a list.
- **Showing DescriptionEng/DescriptionHeb:** These fields are 100% empty. Don't include them in display logic.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| TextualFrame parsing | Custom parser | Simple regex `\[\$(.+?)\$\]` | Pattern is consistent, no edge cases beyond @ prefix |
| Record deduplication | Complex merge logic | Simple filter + group by TextualFrame | Records share metadata, only TextualFrame truly differs |
| Empty field detection | Per-field null checks | `any(v for v in record.values() if v and str(v).strip())` | Consistent pattern across all fields |
| Source attribution | Complex FIST re-export | Add two columns to existing export SQL | Just join through Signature -> CodeSource |

## Common Pitfalls

### Pitfall 1: Empty Records Dominating Display
**What goes wrong:** Showing "FJMS Catalog" section with no content for 70% of manuscripts.
**Why it happens:** 65.9% of catalog rows are entirely empty, and 69.3% of AlmaIds have no useful data.
**How to avoid:** Filter empty records in `get_catalog_records()`. Only render the FJMS catalog section if there's at least one non-empty field.
**Warning signs:** FJMS section appearing with just a header and no content.

### Pitfall 2: Sentinel Date Values
**What goes wrong:** Displaying "Date: 0" or "Date: -99" for manuscripts.
**Why it happens:** FIST uses 0 for "unknown date" and -99 for "not applicable."
**How to avoid:** Filter dates: skip if CopyDate in ('0', '-99', '-1', '') or is None.
**Warning signs:** Strange date values in the UI.

### Pitfall 3: Missing Source Attribution
**What goes wrong:** Multiple TextualFrame entries with no indication of which scholar/team provided each one.
**Why it happens:** The current export does not include SourceName from dbo_CodeSource.
**How to avoid:** Add SourceName/SourceNameHeb to the export SQL, joining through `dbo_Signature.SourceId -> dbo_CodeSource.TeamCode`.
**Warning signs:** Multiple content entries with no way to distinguish sources.

### Pitfall 4: fetchone() vs fetchall() in get_catalog()
**What goes wrong:** Only one catalog record displayed for manuscripts with multiple scholarly identifications.
**Why it happens:** The existing `get_catalog()` method uses `cursor.fetchone()`.
**How to avoid:** Create `get_catalog_records()` that uses `cursor.fetchall()` and filters empty records.
**Warning signs:** Missing content identification entries for multi-record manuscripts.

### Pitfall 5: Console Encoding for Hebrew
**What goes wrong:** Hebrew text appears as mojibake in Windows console output during testing.
**Why it happens:** Windows console defaults to cp1252, not UTF-8.
**How to avoid:** This is a display artifact only -- the actual SQLite data is correctly UTF-8 encoded. Don't "fix" the data based on console output.
**Warning signs:** Hebrew looking like `�` in console but rendering correctly in the app.

### Pitfall 6: Thread Safety for Web
**What goes wrong:** SQLite "ProgrammingError: SQLite objects created in a thread can only be used in that same thread."
**Why it happens:** NiceGUI serves requests from multiple threads.
**How to avoid:** Use the existing `thread_safe=True` pattern when getting the FjmsService singleton in web context.
**Warning signs:** Intermittent database errors in production.

## Code Examples

### Export Script Modification (add SourceName)

```python
# In export_catalog() function, modify the SQL:
cursor = source.execute("""
    SELECT DISTINCT
        TRIM(CAST(alma.AlmaId AS TEXT)) as AlmaId,
        cat.Title,
        cat.GenizahTitleText as TitleHeb,
        cat.AuthorText,
        cat.CopyDate,
        cat.CopyPlace,
        cat.IdentificationTextEng as DescriptionEng,
        cat.IdentificationTextHeb as DescriptionHeb,
        cat.BI_TextualFrameHeb as TextualFrameHeb,
        cat.BI_TextualFrameEng as TextualFrameEng,
        cs.EngDesc as SourceName,
        cs.HebDesc as SourceNameHeb
    FROM dbo_InventoryAlma alma
    JOIN dbo_Inventory inv ON alma.InventoryId = inv.InventoryId
    JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
    JOIN dbo_Signature sig ON isig.SetSignatureId = sig.SetSignatureId
    JOIN dbo_UnitCatalogRec cat ON sig.SignatureId = cat.SignatureId
    LEFT JOIN dbo_CodeSource cs ON sig.SourceId = cs.TeamCode
""")
```

### FjmsService.get_catalog_records()

```python
def get_catalog_records(self, sys_id: str) -> list[dict]:
    """Get all non-empty catalog records for a manuscript."""
    if self._conn is None:
        return []
    try:
        cursor = self._conn.execute(
            "SELECT * FROM catalog WHERE AlmaId = ?",
            (sys_id,),
        )
        results = []
        seen = set()  # For deduplication
        for row in cursor:
            record = {
                "title": row["Title"],
                "title_heb": row["TitleHeb"],
                "author_text": row["AuthorText"],
                "copy_date": row["CopyDate"],
                "copy_place": row["CopyPlace"],
                "textual_frame_heb": row["TextualFrameHeb"],
                "textual_frame_eng": row["TextualFrameEng"],
                "source_name": row["SourceName"] if "SourceName" in row.keys() else None,
                "source_name_heb": row["SourceNameHeb"] if "SourceNameHeb" in row.keys() else None,
            }
            # Filter completely empty records
            if not any(v for k, v in record.items()
                       if k not in ('source_name', 'source_name_heb')
                       and v and str(v).strip()):
                continue
            # Deduplicate
            key = (record["textual_frame_eng"], record["copy_date"], record["title"])
            if key in seen:
                continue
            seen.add(key)
            results.append(record)
        return results
    except Exception as e:
        logger.error(f"FjmsService.get_catalog_records error for {sys_id}: {e}")
        return []
```

### Web Browse Page FJMS Catalog Section

```python
# Inside browse.py metadata panel, after PGP section, before domains:
from shared.fjms_service import get_fjms_service

fjms = get_fjms_service(thread_safe=True)
if fjms.is_available():
    catalog_records = fjms.get_catalog_records(page.sys_id)
    if catalog_records:
        ui.separator().classes('my-3')
        with ui.row().classes('items-center gap-2 mb-2'):
            h3(tr('FJMS Catalog'), classes='text-xs font-bold',
               style='color: var(--text-secondary);')
            ui.badge('FJMS', color='purple').props('outline dense').classes('text-xs')

        # Merge metadata from first non-empty record
        merged = merge_catalog_records(catalog_records)
        lang = get_language()

        # Title (if available and different from main title)
        title = merged.get('title_heb') if lang == 'he' else merged.get('title')
        if title:
            with ui.column().classes('gap-1 mb-2'):
                ui.label(tr('Title')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                ui.label(title).classes('text-sm').style('color: var(--text-primary);')

        # Author
        if merged.get('author_text'):
            with ui.column().classes('gap-1 mb-2'):
                ui.label(tr('Author')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                ui.label(merged['author_text']).classes('text-sm').style('color: var(--text-primary);')

        # Date and Place (inline)
        date = merged.get('copy_date')
        place = merged.get('copy_place')
        if date or place:
            with ui.row().classes('gap-4 mb-2'):
                if date:
                    with ui.column().classes('gap-1'):
                        ui.label(tr('Date')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                        ui.label(date).classes('text-sm').style('color: var(--text-primary);')
                if place:
                    with ui.column().classes('gap-1'):
                        ui.label(tr('Place')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                        ui.label(place).classes('text-sm').style('color: var(--text-primary);')

        # Content Identifications (TextualFrames)
        frames = merged.get('textual_frames', [])
        if frames:
            with ui.column().classes('gap-1 mb-2'):
                ui.label(tr('Content Identification')).classes('text-xs font-bold').style('color: var(--text-secondary);')
                for frame in frames:
                    text = frame.get('heb') if lang == 'he' else frame.get('eng')
                    if text:
                        category, content = parse_textual_frame(text)
                        if category:
                            with ui.row().classes('gap-1'):
                                ui.label(category).classes('text-xs font-bold').style('color: #9b59b6;')
                                ui.label(content).classes('text-sm').style('color: var(--text-primary);')
                        else:
                            ui.label(text).classes('text-sm').style('color: var(--text-primary);')
```

### Desktop Extended Info FJMS Catalog HTML Builder

```python
def _build_fjms_catalog_html(self, sys_id, text_color):
    """Build HTML for FJMS catalog metadata in extended info."""
    from shared.fjms_service import get_fjms_service
    fjms = get_fjms_service()
    if not fjms.is_available():
        return ""

    records = fjms.get_catalog_records(sys_id)
    if not records:
        return ""

    merged = merge_catalog_records(records)
    lang = CURRENT_LANG  # Global language setting

    html = (
        f"<div style='color:{text_color}; padding: 10px; margin-bottom: 10px; "
        "border-left: 3px solid #9b59b6; text-align: left;' dir='ltr'>"
        f"<p style='margin-top:0;'><b>FJMS Catalog</b></p>"
    )

    title = merged.get('title_heb') if lang == 'he' else merged.get('title')
    if title:
        html += f"<p><b>{tr('Title')}:</b> {title}</p>"

    if merged.get('author_text'):
        html += f"<p><b>{tr('Author')}:</b> {merged['author_text']}</p>"

    date = merged.get('copy_date')
    place = merged.get('copy_place')
    if date or place:
        parts = []
        if date:
            parts.append(f"<b>{tr('Date')}:</b> {date}")
        if place:
            parts.append(f"<b>Place:</b> {place}")
        html += f"<p>{' | '.join(parts)}</p>"

    frames = merged.get('textual_frames', [])
    if frames:
        html += f"<p><b>Content:</b></p><ul>"
        for frame in frames:
            text = frame.get('heb') if lang == 'he' else frame.get('eng')
            if text:
                html += f"<li>{text}</li>"
        html += "</ul>"

    html += "</div>"
    return html
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| get_catalog() returns one dict | get_catalog_records() returns list | Phase 28 | Correctly handles multi-record AlmaIds |
| DescriptionEng/DescriptionHeb | TextualFrameEng/TextualFrameHeb | Discovery during research | DescriptionEng/Heb are 100% empty; TextualFrame is the real content |
| No source attribution | SourceName added to export | Phase 28 | Enables per-record scholarly source labels |

**Deprecated/outdated:**
- `get_catalog()` with `fetchone()` -- returns only first record, misses multi-record data
- `DescriptionEng`/`DescriptionHeb` columns -- completely empty, should be ignored in display logic

## Sidecar Export Modification

The export script needs a minor update to add two columns:

**Current catalog table schema:**
```sql
CREATE TABLE catalog (
    AlmaId TEXT NOT NULL,
    Title TEXT, TitleHeb TEXT, AuthorText TEXT,
    CopyDate TEXT, CopyPlace TEXT,
    DescriptionEng TEXT, DescriptionHeb TEXT,
    TextualFrameHeb TEXT, TextualFrameEng TEXT
)
```

**Required catalog table schema:**
```sql
CREATE TABLE catalog (
    AlmaId TEXT NOT NULL,
    Title TEXT, TitleHeb TEXT, AuthorText TEXT,
    CopyDate TEXT, CopyPlace TEXT,
    DescriptionEng TEXT, DescriptionHeb TEXT,
    TextualFrameHeb TEXT, TextualFrameEng TEXT,
    SourceName TEXT, SourceNameHeb TEXT
)
```

The SQL join adds: `LEFT JOIN dbo_CodeSource cs ON sig.SourceId = cs.TeamCode`

**Source attribution values** (top sources from FIST):
- "Catalogs" (304,843 records) -- institutional catalog data
- "Instatution" [sic] (267,170 records) -- holding institution data
- "Nuscha" (28,013 records) -- Nuscha project
- "Inventory" (21,629 records) -- inventory-level data
- "Firkovitch Collections" (11,106 records)
- "Talmudic Literature" (8,319 records)
- "Books" (7,964 records)
- Various scholarly teams (Documentary Material, Biblical Exegesis, etc.)

## Data Coverage Summary

**For planning task sizing:**
- ~204,000 AlmaIds overlap between sidecar and libraries.csv (the manuscripts users can browse)
- ~28% of those (~57,000) have any useful catalog data
- Of those with data, ~82% have TextualFrame, ~19% have CopyDate, and ~2-3% have Title/Author/Place
- 74.3% of AlmaIds with data have only 1 record; 17.6% have 2; 8.1% have 3+
- The UI must handle gracefully: 0 records (show nothing), 1 record (simple), 2-5 records (list), 5+ records (list with possible truncation)

## Open Questions

1. **Re-export timing:** Adding SourceName requires re-running the export script against FIST.db. Should this be done as a prerequisite step or as part of the implementation?
   - What we know: FIST.db backup exists at `FIST_DB_BACKUP/FIST.db`. Export takes ~2 minutes.
   - Recommendation: Make it the first task in the plan. The service code can gracefully handle the missing columns.

2. **DescriptionEng/DescriptionHeb removal:** These columns are 100% empty. Should they be dropped from the schema?
   - What we know: They take up space in the schema but no data. FTS5 indexes them (wasting index space).
   - Recommendation: Keep columns for forward compatibility (FIST might populate them later), but exclude from display logic and FTS5.

3. **Extreme multi-record cases:** 1 AlmaId has 129 records. How to display?
   - What we know: Only 0.3% of AlmaIds have 6+ records. The 129-record case is unique.
   - Recommendation: Show first 10 TextualFrame entries, with "Show all N identifications" expansion for edge cases.

## Sources

### Primary (HIGH confidence)
- Direct SQLite queries against `fist_data/fjms_enrichment.db` -- all statistics
- Direct SQLite queries against `FIST_DB_BACKUP/FIST.db` -- source attribution analysis
- `shared/fjms_service.py` -- existing service API
- `scripts/export_fist_enrichment.py` -- export SQL and schema
- `web/pages/browse.py` lines 1816-1991 -- web metadata panel structure
- `genizah_app.py` lines 4140-4280 -- desktop extended info builder
- `genizah_core.py` lines 3080-3183 -- NLI MARC data fetch

### Secondary (MEDIUM confidence)
- `genizah_app.py` lines 8702-8798 -- PGP and FJMS domain HTML patterns (used as reference for consistent styling)

## Metadata

**Confidence breakdown:**
- Data analysis: HIGH -- direct queries against actual production data
- Multi-record patterns: HIGH -- analyzed 58,283 multi-record AlmaIds
- NLI overlap: HIGH -- compared actual fetch code with actual sidecar schema
- Source attribution: HIGH -- verified FIST source tables and join paths
- Architecture recommendations: HIGH -- based on existing patterns in codebase
- UI placement: HIGH -- based on reading actual browse.py and genizah_app.py

**Research date:** 2026-02-15
**Valid until:** Indefinite (sidecar data is static export; UI patterns are stable)
