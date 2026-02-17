# Phase 37: FJMS Catalog Descriptions - Research

**Researched:** 2026-02-17
**Domain:** FJMS catalog data display (NiceGUI web + PyQt6 desktop)
**Confidence:** HIGH

## Summary

This phase surfaces FJMS scholarly catalog descriptions from the existing `catalog` table in `fjms_enrichment.db` via a dedicated button and dialog in both web and desktop apps. The data already exists -- no new export or table is needed. The codebase already has comprehensive infrastructure for this: `FjmsService.get_catalog_records()` returns deduplicated records, `merge_catalog_records()` aggregates metadata, and `parse_textual_frame()` / `split_textual_frames()` handle the `[$...$]` and `@` markup. The existing Bibliography FJMS dialog pattern (both `web/components/bibliography_dialog.py` and desktop `FjmsBibliographyDialog` class) provides a direct template.

The catalog table has 500,888 rows across ~227K distinct AlmaIds, with ~96K rows containing TextualFrame data across ~57.6K distinct AlmaIds. Records come from 14 distinct source names, with "Catalogs" (49K records) and "Institution" (32.6K) being the largest. Most entries are short (<100 chars), but some reach 2,688 chars (compound piyyut identifications). Some manuscripts have up to 128 records from the same source, so the dialog must handle scrolling gracefully.

**Primary recommendation:** Create a new `create_catalog_records_dialog()` in `web/components/` following the bibliography dialog pattern, add a new `FjmsCatalogDialog` QDialog class in the desktop app, and wire buttons in all four locations (web browse, web search, desktop browse ext_info_row, desktop ResultDialog action_row). Add a batch count method `get_catalog_record_counts()` to FjmsService for efficient search result enrichment.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

#### Data Source
- Query directly from existing `catalog` table's `TextualFrameEng`/`TextualFrameHeb` columns -- no new `full_texts` table needed
- ~96K records with TextualFrame data, ~500K total catalog rows
- Also display `Title`/`TitleHeb`, `AuthorText`, `CopyDate`, `CopyPlace` when available

#### Description Presentation
- **Dialog/modal popup** -- matches existing Bibliography FJMS pattern
- **Follow app language** -- show TextualFrameHeb when app is Hebrew, TextualFrameEng when English
- **Language fallback** -- if preferred language version is empty, fall back to the other language
- **Markup rendering** -- preserve `[$...$]` and `@` markup data but render it nicely (styled/emphasized text), not raw
- **Title as heading** -- show Title/TitleHeb as a heading above the description when present
- **Author field** -- show AuthorText when available (after title, before frame text)
- **Extra metadata** -- show CopyDate and CopyPlace in the dialog when available
- **Desktop parity** -- QDialog popup in desktop app, same content layout as web modal

#### Button Design & Placement
- **Label:** "Catalog Records (N)" in English / "מידע קטלוגי (N)" in Hebrew, with entry count
- **Icon:** `description` (Material doc icon) -- distinct from bibliography's `menu_book`
- **Style:** `outline dense` -- matches existing Bibliography FJMS/Ktiv buttons
- **Web browse:** In the bibliography buttons row (near Bibliography FJMS / Bibliography Ktiv)
- **Web search results:** Button in the metadata section of search result cards
- **Desktop browse:** Same row as bibliography buttons (ext_info_row)
- **Desktop Result Dialog:** Button follows bibliography button pattern (visible when data exists)
- **Empty state:** Button always visible but disabled with (0) count when no records -- consistent across all locations

#### Attribution Display
- **Source header per group** -- group entries by SourceName, show source name once as a section header
- **Source language** -- follow app language (SourceNameHeb in Hebrew mode, SourceName in English)
- **Author placement** -- after title, before frame text: Title -> Author -> Description

#### Multiple Descriptions
- **Show all, scrollable** -- dialog scrolls, no truncation or cap regardless of entry count
- **No deduplication** -- show all entries from all sources as-is, even if overlapping
- **Dialog title** -- "Catalog Records -- {shelfmark}" (count only on the button, not in dialog title)

### Claude's Discretion
- Truncation strategy for very long individual descriptions (up to 2,688 chars)
- Handling of identical TextualFrameEng/TextualFrameHeb content (dedup display or just show chosen language)
- Exact spacing, typography, and scroll behavior in the dialog

### Deferred Ideas (OUT OF SCOPE)
- **Clickable [$reference$] links** -- clicking a `[$Sifra$]` or `[$Talmud Yerushalmi$]` reference to trigger a search -- future phase
- **FJMS FTS5 search of descriptions** -- already tracked as FJMS-04 in requirements (future)
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| FJMS-01 | FJMS catalog descriptions (65K records) exported to `fjms_enrichment.db` | **Already satisfied.** The `catalog` table exists with ~96K TextualFrame records across ~57.6K distinct AlmaIds. `FjmsService.get_catalog_records()` already queries and deduplicates them. No export needed. |
| FJMS-02 | User can view FJMS scholarly descriptions from browse page via dedicated button in both apps | Requires: new dialog component (web + desktop), button wiring in browse page (web: near bibliography buttons row at line ~2165, desktop: ext_info_row at line ~8547), and button in search results. Service layer already exists. |
| FJMS-03 | Descriptions show source attribution (which catalog/scholar) | `catalog` table has `SourceName`/`SourceNameHeb` columns. `get_catalog_records()` already returns `source_name` and `source_name_heb` fields. Dialog groups entries by source, showing source as section header. |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| NiceGUI | 2.x | Web dialog (ui.dialog, ui.card, ui.html) | Already used for bibliography dialogs |
| PyQt6 | 6.x | Desktop dialog (QDialog, QTextBrowser, QVBoxLayout) | Already used for FjmsBibliographyDialog |
| SQLite3 | stdlib | Query fjms_enrichment.db catalog table | Already used via FjmsService |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| shared/fjms_service.py | existing | get_catalog_records(), merge_catalog_records(), parse_textual_frame() | All catalog data retrieval |
| web/translations.py | existing | tr() for bilingual labels | All UI text |
| genizah_translations.py | existing | TRANSLATIONS dict (both apps) | New keys for "Catalog Records", etc. |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Scrollable dialog content | ui.table (like bibliography) | Not suitable -- catalog descriptions are free-text paragraphs, not tabular data. Scrollable HTML/label content is better. |
| Individual record cards | Single merged view | Decision is "show all entries from all sources" -- card-per-entry grouped by source is the right UX. |

## Architecture Patterns

### Recommended File Structure
```
shared/fjms_service.py          # Add: get_catalog_record_counts() batch method
web/components/catalog_dialog.py  # NEW: create_catalog_records_dialog()
web/pages/browse.py             # Modify: add button in bibliography row
web/pages/search.py             # Modify: add button in result card
genizah_app.py                  # Modify: FjmsCatalogDialog class + button wiring
genizah_translations.py         # Add: new translation keys
tests/test_fjms_service.py      # Add: test for batch counts
tests/test_catalog_dialog.py    # NEW: test markup rendering
```

### Pattern 1: Web Dialog (following bibliography_dialog.py pattern)
**What:** NiceGUI dialog with scrollable card, header bar, close button, and content area.
**When to use:** For the web catalog records popup.
**Example:**
```python
# Source: web/components/bibliography_dialog.py (existing pattern)
def create_catalog_records_dialog(records, sys_id, shelfmark=""):
    dialog = ui.dialog().props('maximized=false full-width')
    with dialog, ui.card().classes('w-full max-w-[900px] max-h-[90vh]').style(
        'overflow: hidden; display: flex; flex-direction: column;'
    ):
        # Header with purple gradient (FJMS brand color)
        with ui.row().classes('w-full items-center justify-between p-3 rounded-t').style(
            'background: linear-gradient(135deg, #6c3483, #9b59b6); color: white;'
        ):
            # ... icon, title, close button

        # Scrollable content area
        with ui.scroll_area().classes('w-full').style('flex: 1;'):
            # Group by source_name, render entries
    return dialog
```

### Pattern 2: Desktop Dialog (following FjmsBibliographyDialog pattern)
**What:** QDialog with QScrollArea containing HTML-rendered entries.
**When to use:** For the desktop catalog records popup.
**Example:**
```python
# Source: genizah_app.py:4971 (FjmsBibliographyDialog pattern)
class FjmsCatalogDialog(QDialog):
    def __init__(self, records, sys_id='', shelfmark='', parent=None):
        super().__init__(parent)
        self.setWindowTitle(f"{tr('Catalog Records')} — {shelfmark}")
        self.setMinimumSize(700, 500)
        layout = QVBoxLayout(self)
        # QTextBrowser with HTML content (scrollable by default)
        self.text_browser = QTextBrowser()
        self.text_browser.setHtml(self._build_html(records))
        layout.addWidget(self.text_browser, 1)
        # Close button
```

### Pattern 3: Button Wiring (browse page)
**What:** Add button next to existing bibliography buttons with disabled-when-empty state.
**When to use:** All four button locations.
**Example (web browse):**
```python
# In the bibliography buttons row (browse.py ~line 2167)
catalog_count = len(catalog_records)  # Already fetched above for metadata display
cat_dlg = create_catalog_records_dialog(catalog_records, page.sys_id, shelfmark=page.shelfmark)
cat_btn = ui.button(
    f'{tr("Catalog Records")} ({catalog_count})',
    icon='description',
    on_click=cat_dlg.open,
).props('outline dense').classes('text-sm')
if catalog_count == 0:
    cat_btn.props('disable')
```

### Pattern 4: Batch Count Method for Search Results
**What:** Efficient batch query returning record counts per sys_id.
**When to use:** When rendering search result cards that need to show "(N)" on the button.
**Example:**
```python
def get_catalog_record_counts(self, sys_ids: list[str]) -> dict[str, int]:
    """Get catalog record counts for multiple sys_ids in batch."""
    if not self._conn or not sys_ids:
        return {}
    result = {}
    batch_size = 500
    for i in range(0, len(sys_ids), batch_size):
        batch = sys_ids[i:i + batch_size]
        placeholders = ','.join('?' * len(batch))
        cursor = self._conn.execute(
            f"SELECT AlmaId, COUNT(*) as cnt FROM catalog "
            f"WHERE AlmaId IN ({placeholders}) "
            f"AND ((TextualFrameEng IS NOT NULL AND TextualFrameEng != '') "
            f"  OR (TextualFrameHeb IS NOT NULL AND TextualFrameHeb != '')) "
            f"GROUP BY AlmaId",
            batch,
        )
        for row in cursor:
            result[row["AlmaId"]] = row["cnt"]
    return result
```

### Pattern 5: Markup Rendering ([$...$] and @)
**What:** Transform `[$Category$]: Content` markup into styled HTML.
**When to use:** Rendering TextualFrame text in the dialog.
**Key insight:** `parse_textual_frame()` and `split_textual_frames()` already exist in `shared/fjms_service.py` and handle all markup patterns. The browse page already uses them (lines 2101, 2121). Reuse these functions -- do NOT hand-roll parsing.
**Example:**
```python
from shared.fjms_service import split_textual_frames, parse_textual_frame

def render_frame_text(text: str, lang: str) -> str:
    """Render a TextualFrame string as styled HTML."""
    parts = split_textual_frames(text)
    if not parts:
        return f'<p>{html_escape(text)}</p>'
    html_parts = []
    for part in parts:
        category, content = parse_textual_frame(part)
        if category:
            html_parts.append(
                f'<p><b style="color:#9b59b6;">{category}:</b> {content}</p>'
            )
        else:
            html_parts.append(f'<p>{content}</p>')
    return '\n'.join(html_parts)
```

### Anti-Patterns to Avoid
- **Don't duplicate merge_catalog_records logic** -- it already handles deduplication and metadata aggregation. However, for the dialog we show ALL entries (per decision: "no deduplication"), so use `get_catalog_records()` directly, NOT `merge_catalog_records()`.
- **Don't create new database tables** -- the `catalog` table already has everything needed.
- **Don't fetch catalog data twice on browse** -- catalog_records is already fetched for the metadata panel (line 2047). Reuse it for the button/dialog.
- **Don't parse markup manually** -- `split_textual_frames()` and `parse_textual_frame()` already handle all patterns including `@[$...$]`, `[$...$]`, parenthetical sub-types, and plain text.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| TextualFrame parsing | Custom regex | `split_textual_frames()` + `parse_textual_frame()` | Already handles `;` splitting, `@` prefix, `[$...$]` notation, parenthetical sub-types |
| Record deduplication | Custom dedup | `get_catalog_records()` (has built-in dedup) | Filters empty records, normalizes sentinel dates, deduplicates by key tuple |
| Language switching | Custom lang detection | `get_language()` (web) / `CURRENT_LANG` (desktop) | Consistent with all other bilingual components |
| Batch queries | N+1 individual queries | Batched IN clause (see `get_domains_for_sys_ids` pattern) | SQLite limit of 999 variables already handled by 500-batch pattern |

**Key insight:** The service layer (`shared/fjms_service.py`) already has 90% of the data logic. The only new service method needed is `get_catalog_record_counts()` for batch counting in search results.

## Common Pitfalls

### Pitfall 1: Double-fetching catalog data on browse page
**What goes wrong:** The browse page already fetches `catalog_records` at line 2047 for the metadata panel. Fetching again for the button/dialog wastes a SQLite query.
**Why it happens:** Code for the button might be added in a separate section without checking what's already loaded.
**How to avoid:** Reuse the `catalog_records` variable already fetched. Pass it to the dialog constructor directly.
**Warning signs:** Two calls to `fjms.get_catalog_records(page.sys_id)` in browse.py.

### Pitfall 2: Using merge_catalog_records() in the dialog
**What goes wrong:** Decision says "show all entries from all sources as-is, even if overlapping" and "no deduplication". `merge_catalog_records()` deduplicates frames and merges metadata.
**Why it happens:** Browse page currently uses merged data. Tempting to reuse the same approach.
**How to avoid:** In the DIALOG, use raw `get_catalog_records()` output, group by source_name, render each entry individually. Only use `merge_catalog_records()` for the browse metadata panel (as it already does).
**Warning signs:** Dialog shows fewer entries than the button count indicates.

### Pitfall 3: Forgetting disabled-when-empty for button
**What goes wrong:** Button disappears when no catalog data exists, breaking the "always visible, disabled with (0)" decision.
**Why it happens:** The existing bibliography buttons use `setVisible(False)` / `setVisible(True)` pattern. The catalog button has a DIFFERENT pattern (always visible, disabled when 0).
**How to avoid:** Always render the button. Set `props('disable')` (web) or `setEnabled(False)` (desktop) when count is 0.
**Warning signs:** Button not visible for manuscripts without catalog data.

### Pitfall 4: Search results performance
**What goes wrong:** Fetching full catalog records for each of 200 search results causes slow rendering.
**Why it happens:** `get_catalog_records()` fetches all fields and does Python-side dedup -- overkill when you just need a count.
**How to avoid:** Use the new `get_catalog_record_counts()` batch method that returns just `{sys_id: count}`. Only fetch full records when the user opens the dialog.
**Warning signs:** Search results take >2s to render after adding catalog buttons.

### Pitfall 5: Hebrew/English text direction in dialog
**What goes wrong:** Hebrew TextualFrame content rendered LTR, or English content rendered RTL.
**Why it happens:** Dialog doesn't set `dir` attribute based on language.
**How to avoid:** Set `dir='rtl'` for Hebrew content, `dir='ltr'` for English. The language-aware fallback means the displayed text could be in either language.
**Warning signs:** Text alignment looks wrong for Hebrew descriptions.

### Pitfall 6: Missing translation keys
**What goes wrong:** English text appears in Hebrew mode because translation key is not in TRANSLATIONS dict.
**Why it happens:** New UI text added without corresponding translation entries.
**How to avoid:** Add all new keys to `genizah_translations.py` before wiring UI.
**Warning signs:** Hebrew users see English labels.

### Pitfall 7: Desktop test schema missing SourceName columns
**What goes wrong:** Tests fail because test fixture database doesn't have `SourceName`/`SourceNameHeb` columns.
**Why it happens:** `tests/test_fjms_service.py` creates the catalog table without these columns (they were added later).
**How to avoid:** Update the test fixture to include SourceName/SourceNameHeb columns. Note: `get_catalog_records()` already handles missing columns gracefully with `has_source = "SourceName" in col_names`.
**Warning signs:** Tests pass but production behavior differs.

## Code Examples

### Existing Data Flow (browse page, already working)
```python
# web/pages/browse.py lines 2044-2131 (already in production)
from shared.fjms_service import get_fjms_service, merge_catalog_records, parse_textual_frame
fjms = get_fjms_service(thread_safe=True)
catalog_records = fjms.get_catalog_records(page.sys_id)
if catalog_records:
    merged = merge_catalog_records(catalog_records)
    # ... renders title, author, date, place, frames in metadata panel
```

### Existing Button Pattern (bibliography, already working)
```python
# web/pages/browse.py lines 2168-2177 (existing pattern to follow)
fjms_dlg = create_fjms_bibliography_dialog(fjms_bib, page.sys_id, shelfmark=page.shelfmark or '')
ui.button(
    f'{tr("Bibliography FJMS")} ({len(fjms_bib)})',
    icon='menu_book',
    on_click=fjms_dlg.open,
).props('outline dense').classes('text-sm')
```

### Source Grouping (for dialog content)
```python
# Group records by source_name for section headers
from itertools import groupby
from operator import itemgetter

sorted_records = sorted(records, key=lambda r: r.get('source_name') or '')
for source_name, group in groupby(sorted_records, key=lambda r: r.get('source_name') or ''):
    entries = list(group)
    # Render source header
    display_source = (entry['source_name_heb'] if lang == 'he' else entry['source_name']) or source_name
    # Render each entry: title -> author -> textual_frame
```

### TextualFrame Rendering (existing functions)
```python
# shared/fjms_service.py (already exists)
from shared.fjms_service import split_textual_frames, parse_textual_frame

text = "@[$Piyyut$] (Yotzer): \"poem title\"; @[$Piyyut$]: \"another\""
parts = split_textual_frames(text)
# -> ["@[$Piyyut$] (Yotzer): \"poem title\"", "@[$Piyyut$]: \"another\""]
for part in parts:
    category, content = parse_textual_frame(part)
    # -> ("Piyyut (Yotzer)", "\"poem title\"")
    # -> ("Piyyut", "\"another\"")
```

## Data Characteristics

### Catalog Table Schema
```
AlmaId TEXT NOT NULL      -- sys_id (Alma ID)
Title TEXT                -- English title (only ~2.9K distinct AlmaIds have titles)
TitleHeb TEXT             -- Hebrew title
AuthorText TEXT           -- Author name
CopyDate TEXT             -- Date of copy (sentinel values: 0, -99, -1 -> None)
CopyPlace TEXT            -- Place of copy
DescriptionEng TEXT       -- NOT used for this phase
DescriptionHeb TEXT       -- NOT used for this phase
TextualFrameHeb TEXT      -- Hebrew description (the main content)
TextualFrameEng TEXT      -- English description (the main content)
SourceName TEXT           -- Source catalog name in English
SourceNameHeb TEXT        -- Source catalog name in Hebrew
```

### Data Statistics
| Metric | Value |
|--------|-------|
| Total catalog rows | 500,888 |
| Rows with TextualFrame | 96,419 |
| Distinct AlmaIds with TextualFrame | 57,563 |
| Distinct AlmaIds with Title | 2,878 |
| Distinct SourceNames | 30 (14 with TextualFrame data) |
| Average records per AlmaId | 2.2 |
| Max records per AlmaId | 128 |
| Max TextualFrame length | 2,688 chars |
| Text length distribution | <100: 92K, 100-500: 2.9K, 500-1000: 770, >1000: 490 |

### Top Sources (with TextualFrame data)
| Source | Records | Hebrew Name |
|--------|---------|-------------|
| Catalogs | 49,376 | קטלוגים |
| Institution | 32,597 | מוסדות |
| Talmudic Literature | 7,705 | ספרות תלמודית |
| Judeo-Arabic Biblical Exegesis | 3,633 | פרשנות המקרא בערבית-יהודית |
| Firkovitch Collections | 1,106 | אוספי פירקוביץ' |

### Markup Patterns
1. **`@[$Category$]: Content`** -- 27K records with `@` prefix
2. **`[$Category$]: Content`** -- Without `@` prefix
3. **`@[$Category$] (Sub-type): Content`** -- With parenthetical qualifier
4. **Plain text** -- No markup (e.g., "Hosea; Joel", "Esther 9:22 - 28")
5. **Compound** -- Multiple entries separated by `; ` followed by `@[$` or `[$`

### Translation Keys Needed
```python
"Catalog Records": "מידע קטלוגי",      # Button label / dialog title
# Existing keys that will be reused:
# "FJMS Catalog", "Title", "Author", "Copy Date", "Place",
# "Content Identification", "Close"
```

## Discretion Recommendations

### Truncation Strategy
**Recommendation:** No truncation. The longest text is 2,688 chars (compound piyyut list with 20+ entries). In a scrollable dialog, this renders as roughly 40-50 lines -- entirely manageable. The `split_textual_frames()` function breaks compound entries into individual items, making even the longest entries readable. Setting `word-break: break-word` and `white-space: pre-wrap` handles edge cases.

### Identical Eng/Heb Content
**Recommendation:** Just show the chosen language per app setting. Don't cross-compare eng/heb for identity. The fallback logic (show other language if preferred is empty) already handles the main case. If eng and heb happen to be identical, showing one is fine -- the user chose their language preference.

### Dialog Typography and Layout
**Recommendation:**
- **Web:** Purple gradient header (matches FJMS brand), `description` icon, scrollable content area with `max-h-[90vh]`
- **Desktop:** Standard QDialog with QTextBrowser (inherently scrollable), purple left-border on source sections
- **Entry layout:** Each record as a card/block: Title (bold, if present) -> Author (italic, if present) -> CopyDate + CopyPlace (small, inline, if present) -> TextualFrame content (parsed and styled)
- **Source grouping:** Horizontal rule between groups, source name as bold heading with count
- **Spacing:** 12px gap between entries within a group, 16px between groups

## Open Questions

1. **Should the button appear on search result cards?**
   - What we know: Decision says "Web search results: Button in the metadata section of search result cards"
   - What's unclear: Search result cards are currently compact. Adding another button increases visual noise.
   - Recommendation: Add it, but only show when catalog data exists (disabled-when-empty on search cards could be too noisy with 200 results). For search results, use visible-when-data-exists pattern instead of always-visible-disabled. Reserve the always-visible-disabled pattern for browse page only.

2. **Performance of batch catalog counts for search results**
   - What we know: Need to query counts for up to 200 sys_ids per search page
   - What's unclear: Whether a single batch SQL is fast enough
   - Recommendation: Use `get_catalog_record_counts()` with the same 500-batch pattern as domains. Should complete in <50ms based on indexed AlmaId column. Fetch counts alongside domain data in the existing enrichment flow.

## Sources

### Primary (HIGH confidence)
- `shared/fjms_service.py` -- Full service layer, all methods verified by reading code
- `web/components/bibliography_dialog.py` -- Dialog pattern, verified by reading code
- `genizah_app.py:4971-5138` -- Desktop FjmsBibliographyDialog, verified by reading code
- `web/pages/browse.py:2043-2131` -- Existing catalog metadata rendering, verified by reading code
- `fist_data/fjms_enrichment.db` -- Schema and data verified by direct SQL queries

### Secondary (MEDIUM confidence)
- `genizah_app.py:2436-2680` -- ResultDialog layout and button patterns
- `genizah_app.py:8540-8558` -- Desktop browse ext_info_row layout
- `web/pages/search.py:2174-2313` -- Search result card layout

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all libraries already in use, no new dependencies
- Architecture: HIGH -- direct extrapolation from existing bibliography dialog pattern
- Pitfalls: HIGH -- identified from actual code inspection and data analysis
- Data model: HIGH -- verified by direct SQL queries against production database

**Research date:** 2026-02-17
**Valid until:** 2026-03-17 (stable -- data model and UI patterns are established)
