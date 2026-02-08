# Phase 12: Desktop PGP Discovery - Research

**Researched:** 2026-02-08
**Domain:** PyQt6 desktop UI, NiceGUI web UI, Supabase PGP data display
**Confidence:** HIGH

## Summary

Phase 12 adds PGP content discovery features to the desktop app and extends search indicators/filters to both apps. The core work involves: (1) adding a PGP metadata section to the existing Extended Info panel in both Browse tab and ResultDialog, (2) adding a "PGP" text badge to search results and browse lists in both web and desktop, (3) adding a PGP filter toggle to both apps, (4) adding tag search with a dropdown of known tags in the desktop Search tab, and (5) integrating PGP multi-fragment joins into the desktop JoinsDialog.

The codebase already has strong foundations: the web app already shows a PGP icon indicator in search results (using `get_sys_ids_with_transcriptions` batch lookup), the web browse page already displays PGP metadata in a dedicated section, and the desktop already loads PGP data via `PGPSourceWorker` in both Browse and ResultDialog. The `shared/document_service.py` provides all needed data access functions. The desktop `JoinsDialog` (in `corrections_ui.py`) currently only shows user-created joins; the web's `joins_panel.py` already merges PGP joins and can serve as a reference pattern.

**Primary recommendation:** Leverage existing patterns -- the PGP metadata is already loaded by `PGPSourceWorker` in both Browse and ResultDialog contexts. Add a PGP section to the extended info HTML builder. Use `get_sys_ids_with_transcriptions` for batch badge determination. Add a new `get_all_distinct_tags()` function to `shared/document_service.py` for the tag dropdown.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Follow the **existing "Show Extended Info" pattern** from ResultDialog -- inline expand (not popup)
- Add a "Show Extended Info" button to the **Browse tab top bar**, matching the ResultDialog behavior
- PGP metadata displayed as a **separate "Princeton Geniza Project" section** within the expanded info (not merged into existing fields)
- Shows: document type, tags, dates, description
- PGP tags are **clickable** -- clicking a tag launches a tag search in the Search tab
- PGP extended info appears in **both Browse tab and ResultDialog** (not just Browse)
- Aggregates info from all sources (NLI, PGP, Cambridge, Oxford) as ResultDialog already does
- **Text badge** style -- small "PGP" label next to manuscripts with PGP transcriptions
- Badge appears in **both web and desktop** apps
- Badge appears in **search results and browse lists** (everywhere manuscripts are listed)
- Badge is **informational only** -- no click action
- Badge **remains visible** even when PGP filter is active (consistent appearance)
- Filter toggle to show **only PGP-available manuscripts** in search results
- Available in **both web and desktop** apps
- Desktop: lives alongside **existing desktop filter system**
- Clicking a PGP tag in metadata -> **switches to Search tab** and runs tag search automatically
- **Dedicated tag search input** available (not just click-to-search)
- Tag input uses **dropdown of known tags** (shows all available PGP tags, user picks one)
- Tag search dropdown lives **in the Search tab** as a new search mode option alongside text search

### Claude's Discretion
- PGP badge placement on web cards (corner, below shelfmark, etc.)
- PGP badge column placement in desktop search result tables
- Data source approach for PGP badge (pre-cached set vs per-batch query)
- Web PGP filter UX (checkbox in filters, toggle chip, etc.)
- PGP joins display in Related Fragments dialog (not discussed -- full discretion)
- Exact visual styling of PGP section in extended info

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope
</user_constraints>

## Standard Stack

### Core (already in project)
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| PyQt6 | 6.x | Desktop UI framework | Already used for entire desktop app |
| NiceGUI | 2.x | Web UI framework | Already used for entire web app |
| supabase-py | 2.x | Database client | Already used via `shared/supabase_provider.py` |

### Supporting (already in project)
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `shared/document_service.py` | N/A | PGP data access layer | All PGP queries -- badges, metadata, tags, joins |
| `gui_threads.py` | N/A | QThread workers | Background Supabase calls from desktop |
| `corrections_ui.py` | N/A | JoinsDialog | Desktop fragment joins UI |
| `web/components/joins_panel.py` | N/A | Web joins panel | Reference for PGP join merging pattern |

### No new dependencies needed
All required libraries are already in the project.

## Architecture Patterns

### Existing Pattern: PGP Data Flow in Desktop

```
User navigates to manuscript
    |
    v
browse_load() / load_result_by_index()
    |
    v
PGPSourceWorker(sys_id, page_num)  -- QThread in gui_threads.py
    |
    v
shared/document_service.py
  - get_all_sources_for_fragment(sys_id) -> sources
  - get_document_for_fragment(sys_id, page_num) -> pgp_doc
    |
    v
_on_browse_pgp_loaded(sys_id, sources, pgp_doc)
  - self._browse_pgp_doc = pgp_doc    <-- PGP metadata available here
  - self._browse_pgp_sources = sources <-- edition/translation sources
  - _populate_pgp_combo() -> version selector
```

**Key insight:** `pgp_doc` already contains all metadata needed for the PGP section: `document_type`, `tags`, `doc_date_original`, `doc_date_standard`, `inferred_date_display`, `description`, `pgp_url`, `pgpid`. No new Supabase queries needed for metadata display.

### Existing Pattern: Extended Info in ResultDialog

```python
# genizah_app.py lines 3862-3975
# on_enriched_data_loaded() builds Extended Info HTML:
#   1. KTI Info section (date, dimensions, subjects, notes, people, bibliography)
#   2. Oxford/Cambridge Info section (codicological parts)
#   3. Two-column table layout when both external + KTI info exist
#   4. Sets self.txt_extended_info.setHtml(html)
#   5. Shows self.btn_ext_info when data available
```

**Phase 12 addition:** Add a third section "Princeton Geniza Project" to this HTML builder. The PGP data comes from `self._rd_pgp_doc` (ResultDialog) or `self._browse_pgp_doc` (Browse tab).

### Existing Pattern: PGP Badge in Web Search

```python
# web/pages/search.py lines 1212-1217
# In create_result_card():
sys_id = display.get('id')
if sys_id and sys_id in search_state.transcription_sys_ids:
    ui.icon('description').classes('text-sm').style(
        'color: var(--success-600);'
    ).tooltip(tr('Has PGP Transcription'))
```

The web already uses `get_sys_ids_with_transcriptions()` to batch-check which sys_ids have PGP data. This same function should be used for the desktop badge. Currently the web uses an icon; the user wants a text badge "PGP" instead (both apps).

### Existing Pattern: Desktop Search Results Table

```python
# 9 columns: Checkbox, Actions, SysID, Library, Shelfmark, Img, Title, Snippet, Src
# Constants: COL_CHECKBOX=0, COL_ACTIONS=1, COL_SYS_ID=2, COL_LIBRARY=3,
#            COL_SHELF=4, COL_IMG=5, COL_TITLE=6, COL_SNIPPET=7, COL_SRC=8
```

### Existing Pattern: Desktop Filter System

```python
# genizah_app.py lines 4289-4291
self.results_filters = {}  # dict of {column: {"text": str, "exclude": bool}}
self.list_filter_state = {'active': False, 'mode': 'in', 'lists': 'all'}
# Filters applied via _apply_results_table_filters() which checks row visibility
```

The desktop uses per-column text filters and a list-based star filter. The PGP filter is a different type -- a boolean toggle that hides rows where the manuscript has no PGP data.

### Existing Pattern: Web Search Filters

```python
# web/pages/search.py lines 537-567
# Filters panel with text inputs: shelfmark, title, snippet
# Toggle visibility via toggle_filters()
# Applied via apply_filters() which re-renders filtered results
```

### Existing Pattern: Desktop Tab Navigation

```python
# Tab indices:
# 0 = Search, 1 = Composition Search, 2 = Browse, 3 = Lists, 4 = Community, 5 = Settings
self.tabs.setCurrentWidget(self.search_tab)  # Switch to Search tab
self.tabs.setCurrentWidget(self.browse_tab)  # Switch to Browse tab
```

### Pattern: Desktop Search Mode Selection

```python
# genizah_app.py lines 5970-5978
self.mode_combo = QComboBox()
self.mode_combo.addItems([
    tr("Exact (=)"), tr("Variants (?)"), tr("Fuzzy (~)"),
    tr("Regex (/)"), tr("Title ($)"), tr("Shelfmark (#)")
])
```

Tag search will be a new mode added to this combo, or a separate control. Since the user decided on a **dedicated tag dropdown** as a "new search mode option alongside text search," a tag-specific QComboBox should be added to the Search tab UI.

### Recommended Architecture for Phase 12

```
Plan 12-01: PGP Metadata Panel
  - Add "Show Extended Info" button to Browse tab top bar
  - Add PGP section to Extended Info HTML in both Browse + ResultDialog
  - Build PGP HTML from _browse_pgp_doc / _rd_pgp_doc
  - Make PGP tags clickable (links that trigger tag search)
  - Tag click: switch to Search tab + execute tag search

Plan 12-02: Search Result Indicators + Tag Search + Filter
  - Add new get_all_distinct_tags() to shared/document_service.py
  - Desktop: Add PGP badge column to search results table
  - Desktop: Add PGP filter checkbox alongside existing filters
  - Desktop: Add tag search dropdown + button to Search tab
  - Web: Change icon indicator to text "PGP" badge
  - Web: Add PGP filter checkbox to filters panel
  - Batch lookup via get_sys_ids_with_transcriptions()

Plan 12-03: PGP Joins in Desktop JoinsDialog
  - Merge PGP multi-fragment joins into desktop JoinsDialog
  - Follow web joins_panel.py pattern for PGP join merging
  - Show PGP source badge on PGP-sourced joins
```

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Check which sys_ids have PGP data | Per-result Supabase queries | `get_sys_ids_with_transcriptions(sys_ids)` | Already batch-optimized with `.in_()` query |
| Tag search by tag name | Custom SQL | `get_fragments_by_tag(tag)` | Already uses GIN-indexed JSONB `@>` query |
| PGP fragment joins | Custom join logic | `get_fragments_for_document(pgpid)` | Already returns ordered fragments |
| PGP metadata access | New Supabase queries | `self._browse_pgp_doc` / `self._rd_pgp_doc` | Already loaded by PGPSourceWorker |
| Web PGP transcription check | New function | `search_state.transcription_sys_ids` | Already populated on search |

**Key insight:** Almost all needed data is already being fetched. The PGP metadata is loaded by `PGPSourceWorker` and stored in `_browse_pgp_doc`/`_rd_pgp_doc`. The badge data is available via `get_sys_ids_with_transcriptions`. The only new function needed is `get_all_distinct_tags()` for the tag dropdown.

## Common Pitfalls

### Pitfall 1: QThread Requirement for Supabase Calls
**What goes wrong:** Calling Supabase directly from the UI thread freezes the desktop app.
**Why it happens:** Supabase calls are network I/O.
**How to avoid:** All new Supabase calls must use QThread workers. The `get_sys_ids_with_transcriptions` batch call for badges and `get_all_distinct_tags()` for the tag dropdown must both run in background threads.
**Warning signs:** UI freeze when loading search results or opening tag dropdown.

### Pitfall 2: Race Conditions with Stale PGP Data
**What goes wrong:** PGP data from a previous manuscript displayed for the current one.
**Why it happens:** Background worker completes after user navigated away.
**How to avoid:** Follow the existing stale-request guard pattern:
```python
def _on_browse_pgp_loaded(self, sys_id, sources, pgp_doc):
    if sys_id != self.current_browse_sid:  # Guard!
        return
```
**Warning signs:** PGP metadata showing wrong document type or tags.

### Pitfall 3: PGP Badge Batch Query Timing
**What goes wrong:** Badges not shown because batch query runs before results are populated, or results render before batch query completes.
**Why it happens:** Search results load incrementally (lazy loading with scroll).
**How to avoid:** Run `get_sys_ids_with_transcriptions` once after search completes with all result sys_ids, then apply badges when rendering rows. Store the set as instance state (similar to web's `search_state.transcription_sys_ids`).
**Warning signs:** Badges appearing only after scroll or not at all.

### Pitfall 4: Extended Info Button Missing in Browse Tab
**What goes wrong:** User can see extended info in ResultDialog but not in Browse tab.
**Why it happens:** Browse tab has a different layout structure than ResultDialog. The Browse tab currently has NO "Show Extended Info" button or QTextBrowser for extended info.
**How to avoid:** Add both `btn_b_ext_info` (QPushButton, checkable) and `txt_b_extended_info` (QTextBrowser) to Browse tab, mirroring ResultDialog's pattern.
**Warning signs:** Extended info button visible in ResultDialog but missing in Browse tab.

### Pitfall 5: Tag Search Tab Navigation UX
**What goes wrong:** User clicks a PGP tag but nothing happens visually because the search tab switch and result population happen asynchronously.
**Why it happens:** Tab switch is synchronous but tag search results come from Supabase (async via QThread).
**How to avoid:** (1) Switch to Search tab immediately, (2) show loading state, (3) populate results when worker completes. Consider setting the tag dropdown value and triggering search.
**Warning signs:** User confused by empty search tab after clicking tag.

### Pitfall 6: Desktop JoinsDialog Missing PGP Joins
**What goes wrong:** Desktop JoinsDialog shows only user-created joins, missing PGP multi-fragment joins.
**Why it happens:** `JoinsDialog.load_joins()` in `corrections_ui.py` only queries user joins via `corrections_client`. The web's `joins_panel.py` separately queries PGP joins via `get_fragments_for_document()`.
**How to avoid:** Modify `JoinsDialog.load_joins()` to also call `get_document_for_fragment()` and `get_fragments_for_document()` to merge PGP joins, following the web pattern in `joins_panel.py` lines 105-168.
**Warning signs:** Fragment joins visible in web but not in desktop.

### Pitfall 7: Tag Dropdown Needs Initial Population
**What goes wrong:** Tag dropdown is empty when user first opens Search tab.
**Why it happens:** No function currently exists to fetch all distinct PGP tags from Supabase.
**How to avoid:** Create `get_all_distinct_tags()` in `shared/document_service.py`. Call it once at app startup or lazily when Search tab first loads. Cache the result (tags don't change often).
**Warning signs:** Empty tag dropdown or long loading delay.

## Code Examples

### Example 1: PGP Section HTML for Extended Info
```python
# Pattern: Add to on_enriched_data_loaded() in ResultDialog (and equivalent in Browse)
# Source: Existing pattern from genizah_app.py lines 3862-3975

pgp_doc = getattr(self, '_rd_pgp_doc', {})  # or _browse_pgp_doc
pgp_html = ""
if pgp_doc:
    pgp_html += f"<div style='margin-bottom: 10px; text-align: left;' dir='ltr'>"
    pgp_html += f"<p><b>Princeton Geniza Project</b></p>"

    doc_type = pgp_doc.get('document_type')
    if doc_type:
        pgp_html += f"<p><b>{tr('Document Type')}:</b> {doc_type}</p>"

    tags = pgp_doc.get('tags', [])
    if tags:
        tag_links = []
        for tag in tags:
            # Clickable tag using anchor link
            tag_links.append(f"<a href='tag:{tag}' style='color: #27ae60;'>{tag}</a>")
        pgp_html += f"<p><b>{tr('Tags')}:</b> {', '.join(tag_links)}</p>"

    description = pgp_doc.get('description')
    if description:
        pgp_html += f"<p><b>{tr('Description')}:</b> {description}</p>"

    # Dates
    date = pgp_doc.get('inferred_date_display') or pgp_doc.get('doc_date_standard')
    if date:
        pgp_html += f"<p><b>{tr('Date')}:</b> {date}</p>"

    pgp_url = pgp_doc.get('pgp_url')
    if pgp_url:
        pgp_html += f"<p><a href='{pgp_url}'>{tr('View on PGP')}</a></p>"

    pgp_html += "</div>"
```

### Example 2: Handling Clickable Tags in QTextBrowser
```python
# Pattern: mouseReleaseEvent + anchorAt() per DEC-11-03-01
# The txt_extended_info is a QTextBrowser which supports anchorClicked

# QTextBrowser has anchorClicked signal (unlike QTextEdit)
self.txt_extended_info.setOpenLinks(False)  # Prevent default link handling
self.txt_extended_info.anchorClicked.connect(self._on_extended_info_link_clicked)

def _on_extended_info_link_clicked(self, url):
    url_str = url.toString()
    if url_str.startswith('tag:'):
        tag = url_str[4:]  # Extract tag name
        self._search_by_tag(tag)
    elif url_str.startswith('http'):
        QDesktopServices.openUrl(url)
```

### Example 3: Batch PGP Badge Lookup
```python
# Pattern: Follow web's approach from search.py lines 1110-1117
# Source: shared/document_service.py get_sys_ids_with_transcriptions

# In a QThread worker:
from shared.document_service import get_sys_ids_with_transcriptions

class PGPBadgeWorker(QThread):
    finished = pyqtSignal(set)  # set of sys_ids with PGP

    def __init__(self, sys_ids):
        super().__init__()
        self.sys_ids = sys_ids

    def run(self):
        try:
            result = get_sys_ids_with_transcriptions(self.sys_ids)
            self.finished.emit(result)
        except Exception:
            self.finished.emit(set())
```

### Example 4: Get All Distinct PGP Tags (new function needed)
```python
# New function for shared/document_service.py
def get_all_distinct_tags() -> List[str]:
    """
    Get all distinct PGP tags from the documents table.

    Returns:
        Sorted list of unique tag strings.
    """
    try:
        client = get_client()
        # Fetch all non-null tags arrays
        response = client.table('documents').select('tags').not_.is_('tags', 'null').execute()

        all_tags = set()
        for row in (response.data or []):
            tags = row.get('tags', [])
            if tags:
                for tag in tags:
                    all_tags.add(tag)

        return sorted(all_tags)
    except Exception as e:
        print(f"Error getting distinct tags: {e}")
        return []
```

### Example 5: PGP Joins Merge in Desktop JoinsDialog
```python
# Pattern: Follow web/components/joins_panel.py lines 105-168
# Merge PGP joins into the existing joins data structure

from shared.document_service import get_document_for_fragment, get_fragments_for_document

# In JoinsDialog.load_joins(), after loading user joins:
pgp_doc = get_document_for_fragment(self.document_id)
if pgp_doc:
    pgpid = pgp_doc.get('pgpid')
    if pgpid:
        pgp_fragments = get_fragments_for_document(pgpid)
        unique_sys_ids = set(f.get('sys_id') for f in pgp_fragments if f.get('sys_id'))

        if len(unique_sys_ids) > 1:
            for pf in pgp_fragments:
                pf_shelfmark = pf.get('shelfmark', '')
                if pf_shelfmark.upper() not in existing_shelfmarks_upper:
                    fragments_set.add(pf_shelfmark)
                    formatted_joins.append({
                        'fragment_a': plain_shelfmark,
                        'fragment_b': pf_shelfmark,
                        'relationship_type': 'same_composition',
                        'source': 'PGP',
                        'created_by_username': '',
                        'created_at': '',
                        'id': None  # Prevents delete button
                    })
```

### Example 6: Browse Tab Extended Info (new UI elements)
```python
# Pattern: Mirror ResultDialog's btn_ext_info + txt_extended_info
# Add to create_browse_tab() in row1 or a new row

self.btn_b_ext_info = QPushButton(tr("Show Extended Info"))
self.btn_b_ext_info.setCheckable(True)
self.btn_b_ext_info.toggled.connect(self._browse_toggle_extended_info)
self.btn_b_ext_info.setVisible(False)
row1.addWidget(self.btn_b_ext_info)  # Add to top bar

self.txt_b_extended_info = QTextBrowser()
self.txt_b_extended_info.setVisible(False)
self.txt_b_extended_info.setMaximumHeight(200)
self.txt_b_extended_info.setStyleSheet("border: 1px solid #ccc; padding: 5px;")
self.txt_b_extended_info.setOpenLinks(False)
self.txt_b_extended_info.anchorClicked.connect(self._on_browse_ext_info_link_clicked)
top_layout.addWidget(self.txt_b_extended_info)
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| No PGP indicator in search | Web has green icon indicator | Phase 8-10 (Jan 2026) | Web users can see which results have PGP data |
| No PGP metadata display in desktop | Web shows PGP metadata in browse | Phase 10-11 (Feb 2026) | Web users can see document type, tags, dates |
| Desktop joins = user-only | Web merges user + PGP joins | Phase 11 (Feb 2026) | Web shows PGP multi-fragment docs in joins |
| No tag search | Web has tag search via URL param | Phase 10 (Feb 2026) | Web users can click tags to search |

**Current gaps (what Phase 12 fills):**
- Desktop has NO extended info in Browse tab (only ResultDialog has it)
- Desktop has NO PGP badge in search results
- Desktop has NO PGP filter
- Desktop has NO tag search
- Desktop JoinsDialog shows NO PGP joins
- Web PGP indicator is an icon, not a text "PGP" badge (user wants text badge)
- Web has NO PGP filter toggle

## Discretion Recommendations

### PGP Badge Placement (Desktop)
**Recommendation:** Add a narrow column after COL_LIBRARY (index 3) or before COL_SHELF (index 4). A dedicated column keeps the badge visible and separate from shelfmark text. Column header could be empty or "PGP", width ~40px, containing "PGP" text in green for matching rows.

**Alternative considered:** Prepending "PGP" text to shelfmark cell. Rejected because it interferes with shelfmark filtering and sorting.

### PGP Badge Placement (Web)
**Recommendation:** Replace the current green `description` icon with a text badge "PGP" styled like the existing library_code badge. Place it in the same row as library code badge and shelfmark (line 1209 area). Use green color scheme to match existing icon color.

### Data Source for Badge
**Recommendation:** Use **pre-cached set per search batch**. When search completes, run `get_sys_ids_with_transcriptions(all_result_sys_ids)` in a background thread, store the resulting set, then apply badges. This matches the web pattern exactly and avoids per-row queries.

For browse lists (Lists tab items), the same batch approach works: when list items load, collect sys_ids and batch-check.

### Web PGP Filter UX
**Recommendation:** Add a checkbox or toggle in the existing filters panel (alongside shelfmark/title/snippet filters). Label: "PGP Transcriptions Only". When active, `apply_filters()` additionally checks `sys_id in search_state.transcription_sys_ids`. This requires zero additional Supabase calls since the set is already populated.

### PGP Joins in Desktop JoinsDialog
**Recommendation:** Modify `JoinsDialog.load_joins()` to also call `get_document_for_fragment(document_id)` and `get_fragments_for_document(pgpid)`, merging PGP joins into the existing data display. PGP-sourced joins should show "PGP" in the Source column and have no delete button (id=None). This requires a QThread since it makes Supabase calls.

### PGP Section Styling in Extended Info
**Recommendation:** Use a green left-border style similar to Oxford's blue left-border (line 3914), with a PGP header. Keep LTR direction since PGP metadata is in English. Tags should be rendered as clickable links with `tag:` URL scheme.

## Open Questions

1. **Tag dropdown population timing**
   - What we know: Need `get_all_distinct_tags()` function, likely ~50-200 distinct tags
   - What's unclear: Should tags be fetched at app startup, lazily on first Search tab visit, or on demand when dropdown opens?
   - Recommendation: Fetch lazily on first access, cache for session. Tags are static PGP data that doesn't change during a session.

2. **PGP filter for browse lists (Lists tab)**
   - What we know: User said badge in "search results and browse lists"
   - What's unclear: Does "browse lists" mean the Lists tab items table, or the Browse tab's list panel?
   - Recommendation: Apply to Lists tab items table (it already has Library, Shelfmark, Title columns). The Browse tab's list panel is a QListWidget with simple text items -- harder to badge.

3. **Extended info in Browse tab -- data source timing**
   - What we know: PGP data comes from `_browse_pgp_doc` (loaded by PGPSourceWorker). Non-PGP extended info comes from `on_enriched_data_loaded()` which is triggered by NLI/metadata enrichment.
   - What's unclear: PGP worker and metadata enrichment run independently. Need to coordinate so extended info shows both PGP and non-PGP data.
   - Recommendation: Build extended info HTML in two stages -- initial build from metadata enrichment, then append PGP section when PGP worker completes. Or rebuild the full HTML in `_on_browse_pgp_loaded()`.

## Existing Code Locations Reference

| Component | File | Line | Description |
|-----------|------|------|-------------|
| ResultDialog class | genizah_app.py | 2292 | Desktop manuscript viewer dialog |
| ResultDialog extended info toggle | genizah_app.py | 3786 | Toggle button handler |
| ResultDialog extended info HTML builder | genizah_app.py | 3862-3975 | Builds KTI/Oxford/Cambridge HTML |
| ResultDialog PGP worker launch | genizah_app.py | 3727-3736 | `_rd_pgp_worker` creation |
| ResultDialog PGP loaded handler | genizah_app.py | 3209-3253 | `_on_rd_pgp_loaded` |
| Browse tab creation | genizah_app.py | 6516-6802 | `create_browse_tab()` |
| Browse PGP worker launch | genizah_app.py | 7181-7190 | `_browse_pgp_worker` creation |
| Browse PGP loaded handler | genizah_app.py | 7195-7241 | `_on_browse_pgp_loaded` |
| Search tab creation | genizah_app.py | 5938 | `create_search_tab()` |
| Search results table | genizah_app.py | 6127-6177 | 9 columns, column constants |
| Search result row population | genizah_app.py | 12260-12368 | Lazy-loaded batches |
| Search mode combo | genizah_app.py | 5970-5978 | Exact/Variants/Fuzzy/Regex/Title/Shelfmark |
| Desktop filter system | genizah_app.py | 12404-12466 | `_open_results_filter_dialog`, `_apply_results_table_filters` |
| Desktop JoinsDialog | corrections_ui.py | 3234-3483 | User-created joins only |
| Desktop JoinsDialog load_joins | corrections_ui.py | 3490-3549 | Loads user joins (no PGP) |
| Desktop JoinsDialog _display_cached_joins | corrections_ui.py | 3627-3726 | Renders joins in fragments list + table |
| PGPSourceWorker | gui_threads.py | 470-528 | Background PGP data fetch |
| shared/document_service.py | shared/document_service.py | 1-507 | All PGP data access functions |
| get_sys_ids_with_transcriptions | shared/document_service.py | 426-451 | Batch badge check |
| get_fragments_by_tag | shared/document_service.py | 453-507 | Tag search with GIN index |
| Web search PGP indicator | web/pages/search.py | 1212-1217 | Green icon for PGP results |
| Web search transcription_sys_ids | web/pages/search.py | 46, 217, 1115 | Batch PGP check set |
| Web browse PGP metadata section | web/pages/browse.py | 1903-1968 | PGP type, tags, dates, description |
| Web browse PGP tag click | web/pages/browse.py | 1937-1940 | Navigate to /search?tag=X |
| Web joins PGP merge | web/components/joins_panel.py | 105-168 | Merges PGP joins with user joins |
| Web tag search handler | web/pages/search.py | 2310-2460 | Tag-based search results |
| Tab indices | genizah_app.py | 4491-4496 | Search=0, Comp=1, Browse=2, Lists=3 |
| Translation strings | genizah_translations.py | 105-106 | "Show/Hide Extended Info" already translated |

## Sources

### Primary (HIGH confidence)
- Direct codebase inspection of genizah_app.py (17,067 lines)
- Direct codebase inspection of shared/document_service.py
- Direct codebase inspection of gui_threads.py (PGPSourceWorker)
- Direct codebase inspection of corrections_ui.py (JoinsDialog)
- Direct codebase inspection of web/pages/search.py
- Direct codebase inspection of web/pages/browse.py
- Direct codebase inspection of web/components/joins_panel.py

### Secondary (MEDIUM confidence)
- MEMORY.md project patterns and decisions
- STATE.md accumulated context

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all libraries already in use, no new dependencies
- Architecture: HIGH -- all patterns observed directly in codebase
- Pitfalls: HIGH -- derived from actual codebase patterns and known Qt threading requirements
- Code examples: HIGH -- derived from existing code patterns with specific line references

**Research date:** 2026-02-08
**Valid until:** 2026-03-08 (stable codebase, patterns unlikely to change)
