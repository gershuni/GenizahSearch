# Phase 56: Exclude Known Manuscripts - Research

**Researched:** 2026-03-29
**Domain:** Post-search result filtering with multi-source exclusion sets (NiceGUI web + PyQt6 desktop)
**Confidence:** HIGH

## Summary

This phase adds the ability to exclude manuscripts from search results using two sources: saved Supabase lists and imported shelfmark files (text/CSV). The codebase already has substantial infrastructure for both approaches. The desktop app has a working `ExcludeDialog` (line 2890) with sys_id/shelfmark text areas, file import, auto-resolution, and `_apply_manual_exclusions()` (line 27184). The web app has `_apply_domain_exclusions()` (line 3197) and `_apply_word_search_exclusions_and_render()` (line 3152) as established patterns for post-search filtering with collapsible excluded sections, count displays, and re-rendering without re-searching.

The primary work is: (1) adding multi-source tracking (list name vs. imported file as separate exclusion sources with per-source clear), (2) building the web exclusion picker dialog with list selection and file import tabs, (3) wiring into the existing web post-search filter pipeline, (4) enhancing the desktop ExcludeDialog to support list selection alongside the existing text/file import, and (5) session persistence for both apps.

**Primary recommendation:** Reuse the desktop `ExcludeDialog` pattern and web `_apply_domain_exclusions()` pipeline. Add a shared data structure for multi-source exclusion tracking (source label + set of sys_ids per source). Shelfmark resolution uses the existing `resolve_system_by_shelfmark()` method and `normalize_shelfmark()` pipeline.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** Two entry points: (1) button in filter panel alongside domain/material/measurement filters, (2) post-search action button near results count (like "Search within N manuscripts" from Phase 55). Both apps.
- **D-02:** Picker opens a dialog with two modes: select a saved Supabase list, or import a shelfmark file.
- **D-03:** Accepted formats: plain text (one shelfmark per line) and CSV (auto-detect shelfmark column).
- **D-04:** Resolution report: table showing resolved items with sys_ids (following the desktop composition search table pattern), unresolved shelfmarks clearly marked. Claude's discretion on exact layout.
- **D-05:** Collapsible excluded section at bottom of results (reusing the domain exclusion pattern from `_apply_domain_exclusions()` in search.py). Shows excluded manuscripts with which source excluded them.
- **D-06:** Per-source clear buttons so user can remove one exclusion source without clearing all.
- **D-07:** Count displayed in results header with source breakdown (e.g., "3 excluded from 'My reviewed list'").
- **D-08:** Exclusions are post-search filters, independent of the refinement chain (Phase 55 breadcrumb chips). Exclusions do NOT feed into `restrict_sys_ids` -- they filter displayed results after search completes.
- **D-09:** Clearing the refinement chain does not clear exclusions, and vice versa.
- **D-10:** Exclusion state persists within the session (web: SearchUIState + app.storage.user; desktop: session state JSON) so switching between searches does not lose the active exclude set.

### Claude's Discretion
- Exact dialog layout (tabs vs sections for list picker / file import)
- Resolution report table column layout
- Button placement details within filter panel
- Desktop dialog implementation (QDialog subclass design)

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| EXCL-01 | User can exclude manuscripts from a saved Supabase list from search results (web + desktop) | `UserListsManager.get_items_in_list_sync()` returns items with `sys_id` field; desktop `ListsManager.get_items_in_list()` provides same. Desktop already has `ListFilterDialog` for list selection. |
| EXCL-02 | User can import a shelfmark file (text/CSV) to create an exclusion set (web + desktop) | Desktop `ExcludeDialog` already has "Load from File" with file parsing. Web needs NiceGUI `ui.upload()` component. CSV auto-detect via column header heuristics. |
| EXCL-03 | Exclusion resolves shelfmarks across conventions (CUL T-S vs T-S, Yevr/EVR, full library names) | `normalize_shelfmark()` at genizah_core.py:125 handles all listed aliases. `resolve_system_by_shelfmark()` at line 4066 provides full resolution with exact+partial matching. Desktop `_ensure_shelf_map()` builds norm->sys_id index. |
| EXCL-04 | Excluded manuscripts are hidden from results but exclusion count is shown | Web: `_apply_domain_exclusions()` pattern at line 3197 (filter + count + collapsible section + re-render). Desktop: `_apply_manual_exclusions()` at line 27184 already filters main+appendix results. |
</phase_requirements>

## Standard Stack

No new libraries needed. This phase uses only existing project dependencies.

### Core (Already in Project)
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| NiceGUI | current | Web UI dialogs, upload component, reactive state | Project web framework |
| PyQt6 | current | Desktop dialogs (QDialog, QFileDialog, QTableWidget) | Project desktop framework |
| Supabase (postgrest-py) | current | List items fetch via `get_list_items()` | Existing auth-aware list system |

### Supporting (Already Available)
| Library | Purpose | When to Use |
|---------|---------|-------------|
| `csv` (stdlib) | CSV file parsing for imported shelfmark files | EXCL-02 file import |
| `io.StringIO` (stdlib) | In-memory CSV parsing from upload bytes | Web file upload processing |

**Installation:** None required -- all dependencies already present.

## Architecture Patterns

### Exclusion Source Data Model

The key new abstraction is a multi-source exclusion container. Each source has a label and a set of resolved sys_ids.

```python
# In SearchUIState (web) and desktop state
# Data structure for multi-source exclusion tracking
class ExclusionSource:
    """One exclusion source (a list or imported file)."""
    label: str           # Display name ("My reviewed list" or "imported_shelfmarks.txt")
    source_type: str     # 'list' or 'file'
    source_id: str       # List ID (for Supabase lists) or filename
    sys_ids: set[str]    # Resolved sys_ids to exclude
    unresolved: list[str] # Shelfmarks that couldn't be resolved (for file imports)

# SearchUIState additions:
exclusion_sources: list[ExclusionSource] = []  # All active exclusion sources
# Computed property: union of all sys_ids across sources
```

### Post-Search Filter Pipeline Integration

The existing filter pipeline in search.py applies filters in this order:
1. Word search exclusions (`_apply_word_search_exclusions_and_render`)
2. Domain exclusions (`_apply_domain_exclusions`)
3. Printed filter (`_apply_printed_filter`)
4. Measurement post-filters (`_apply_measurement_post_filters`)

**Manuscript exclusions should be applied FIRST**, before domain exclusions, because they are the broadest filter (entire manuscripts, not domain-based). The pipeline becomes:

1. **Manuscript exclusions** (new -- filter by sys_id from exclusion sources)
2. Word search exclusions
3. Domain exclusions
4. Printed filter
5. Measurement post-filters

### Shelfmark Resolution Strategy

For imported shelfmark files, resolution follows this path:

```python
# 1. Normalize input shelfmark
norm = normalize_shelfmark(raw_shelfmark)

# 2. Look up in pre-built norm->sys_id map (fast O(1))
#    Desktop: self._shelf_to_sys built by _ensure_shelf_map()
#    Web: build equivalent from meta_mgr.csv_bank

# 3. For unresolved items, try resolve_system_by_shelfmark() (slower, handles partial matches)

# 4. Report: resolved count, unresolved list with original text
```

### CSV Column Auto-Detection

For CSV imports, detect the shelfmark column by:
1. Check headers for keywords: `shelfmark`, `shelf_mark`, `call_number`, `signature`, `classmark`
2. If no header match, check first column (most common export format)
3. If ambiguous, use the first non-numeric text column

### Web Dialog Layout (Recommended: Tabs)

```
+--[ Exclude Manuscripts ]----------------------------+
|  [From List] tab  |  [From File] tab                |
|                                                      |
|  Tab 1: List Picker                                  |
|  +------------------------------------------------+  |
|  | [ ] My reviewed list (42 items)                |  |
|  | [ ] Halper collection (180 items)              |  |
|  | [ ] (no lists -- please log in)                |  |
|  +------------------------------------------------+  |
|                                                      |
|  Tab 2: File Import                                  |
|  [Upload .txt or .csv file]                          |
|  Resolution report table:                            |
|  | Shelfmark      | sys_id     | Status  |          |
|  | T-S 12.123     | 990051...  | Found   |          |
|  | XYZ 999        | --         | Not found|         |
|  Resolved: 80/100                                    |
|                                                      |
|  [Cancel]  [Apply]                                   |
+------------------------------------------------------+
```

### Desktop Dialog Enhancement

The existing `ExcludeDialog` (line 2890) has text areas for sys_ids and shelfmarks. Enhance it by adding:
1. A "From List" tab/section with list selection (reuse pattern from `ListFilterDialog`)
2. Source tracking metadata so session persistence knows which source each exclusion came from
3. Per-source clear buttons in the main search UI

### Recommended Project Structure (Changes Only)

```
web/pages/search.py          # Add exclusion state fields, filter function, UI buttons
web/components/filter_panel.py # Add "Exclude manuscripts" button builder
genizah_app.py               # Enhance ExcludeDialog, add list tab, per-source tracking
shared/                      # No new shared module needed -- logic is UI-specific
```

### Anti-Patterns to Avoid
- **Pre-search filtering for exclusions:** D-08 explicitly says post-search. Do NOT feed exclusion sys_ids into `restrict_sys_ids`.
- **Re-searching on exclusion change:** Follow `_apply_domain_exclusions()` pattern -- filter and re-render displayed results without re-running the search.
- **Single flat exclusion set:** Must track per-source to enable D-06 (per-source clear buttons) and D-07 (source breakdown in count).

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Shelfmark normalization | Custom normalization | `normalize_shelfmark()` from genizah_core.py:125 | Handles all known aliases (Yevr/EVR, Halper/Genizah, ENA-MS, CUL T-S, full library names) |
| Shelfmark-to-sys_id resolution | Custom lookup | `_ensure_shelf_map()` pattern (desktop) / build from `meta_mgr.csv_bank` (web) | Pre-normalized index covers 255K records |
| List item fetching | Direct Supabase queries | `UserListsManager.get_items_in_list_sync()` (web) / `ListsManager.get_items_in_list()` (desktop) | Auth-aware, handles local fallback |
| Session persistence | Custom storage | `persist_value()` (web) / `_schedule_session_save()` (desktop) | Established patterns with debounce |
| Post-search filter rendering | Custom result filtering | Follow `_apply_domain_exclusions()` pipeline | Count display, collapsible section, re-render without re-search |

**Key insight:** The desktop already has ~80% of the exclusion infrastructure (ExcludeDialog, _apply_manual_exclusions, _item_matches_exclusion, shelf_to_sys map, session persistence of excluded_sys_ids/excluded_shelfmarks). The main gaps are multi-source tracking and list selection integration. The web has the filter pipeline pattern but needs the actual exclusion data model and UI.

## Common Pitfalls

### Pitfall 1: Race Condition on List Items Fetch
**What goes wrong:** Supabase list items fetch is synchronous/blocking in NiceGUI async context.
**Why it happens:** `get_list_items()` makes a network call to Supabase.
**How to avoid:** Use `await run.io_bound(get_list_items, list_id)` in the web dialog. Desktop can call directly (already on separate thread pattern).
**Warning signs:** UI freezes when selecting a large list.

### Pitfall 2: Stale Exclusion After List Modification
**What goes wrong:** User excludes a list, then adds/removes items from that list -- exclusion set becomes stale.
**Why it happens:** Exclusion snapshots sys_ids at apply time, doesn't track list changes.
**How to avoid:** This is acceptable behavior for session-scoped exclusions. Document that exclusions reflect list state at time of selection. If user wants updated exclusions, they re-apply.
**Warning signs:** None -- this is expected behavior per the session-scoped design.

### Pitfall 3: Large Exclusion Sets Performance
**What goes wrong:** Iterating results to check membership in exclusion set is slow for large sets.
**Why it happens:** O(n*m) if using list membership checks.
**How to avoid:** Store exclusion sys_ids as a `set()` (O(1) lookup). The existing `_apply_manual_exclusions` pattern in desktop already does this correctly.
**Warning signs:** Noticeable delay when applying exclusions to large result sets (>5000 results).

### Pitfall 4: CSV Encoding Issues
**What goes wrong:** Imported CSV files may have BOM markers, different encodings, or inconsistent line endings.
**Why it happens:** Excel exports often use `utf-8-sig` (with BOM), and different OS produce different line endings.
**How to avoid:** Use `utf-8-sig` encoding for reading (handles BOM transparently). The project already uses this pattern per CLAUDE.md conventions.
**Warning signs:** First shelfmark in CSV has garbage characters prepended.

### Pitfall 5: Exclusion State Lost on Search Mode Change
**What goes wrong:** Switching between regular/Responsa/composition search modes clears exclusion state.
**Why it happens:** Each mode may reinitialize state differently.
**How to avoid:** Per D-10, exclusion state lives at the `SearchUIState` level (not per-mode). Ensure mode switches preserve `exclusion_sources`. Desktop already persists `excluded_sys_ids` at the top level (not per-search-type).
**Warning signs:** Exclusions disappear when changing search modes.

### Pitfall 6: Anonymous Users and List Selection
**What goes wrong:** Anonymous users click "From List" but have no Supabase lists.
**Why it happens:** Lists require authentication for Supabase storage.
**How to avoid:** Check `is_authenticated` before showing list picker. Show "Log in to use saved lists" message. Local lists (desktop `ListsManager`) should still be available. Web local-storage lists are available for anonymous users per `UserListsManager` fallback.
**Warning signs:** Empty list picker with no explanation.

## Code Examples

### Web: Adding Exclusion Source from List

```python
# Pattern: fetch list items and create exclusion source
async def _add_list_exclusion(list_id: str, list_name: str):
    """Add a saved list as an exclusion source."""
    lists_mgr = get_lists_manager()
    items = await run.io_bound(lists_mgr.get_items_in_list_sync, list_id)
    sys_ids = {item['sys_id'] for item in items if item.get('sys_id')}

    source = {
        'label': list_name,
        'source_type': 'list',
        'source_id': list_id,
        'sys_ids': sys_ids,
        'unresolved': [],
    }
    search_state.exclusion_sources.append(source)
    persist_value('search_exclusion_sources', _serialize_sources(search_state.exclusion_sources))
    _apply_manuscript_exclusions()  # Re-render without re-searching
```

### Web: Shelfmark File Resolution

```python
# Pattern: resolve imported shelfmarks to sys_ids
def _resolve_shelfmarks(raw_lines: list[str], meta_mgr) -> tuple[set, list]:
    """Resolve shelfmarks to sys_ids. Returns (resolved_sys_ids, unresolved_lines)."""
    resolved = set()
    unresolved = []

    # Build norm->sys_id map from csv_bank (same pattern as desktop _ensure_shelf_map)
    shelf_map = {}
    for sys_id, meta in meta_mgr.csv_bank.items():
        shelf = meta.get('shelfmark', '')
        if shelf:
            norm = normalize_shelfmark(shelf)
            if norm and norm not in shelf_map:
                shelf_map[norm] = sys_id

    for line in raw_lines:
        line = line.strip()
        if not line:
            continue
        norm = normalize_shelfmark(line)
        if norm in shelf_map:
            resolved.add(shelf_map[norm])
        else:
            # Try full resolution (handles partial matches)
            result = meta_mgr.resolve_system_by_shelfmark(line, limit=1)
            if result.get('sys_id'):
                resolved.add(result['sys_id'])
            else:
                unresolved.append(line)

    return resolved, unresolved
```

### Web: Post-Search Exclusion Filter

```python
# Pattern: follows _apply_domain_exclusions structure
def _apply_manuscript_exclusions():
    """Filter displayed results based on manuscript exclusion sources."""
    all_excluded_ids = set()
    for source in search_state.exclusion_sources:
        all_excluded_ids.update(source['sys_ids'])

    if not all_excluded_ids:
        search_state.manuscript_excluded_results = []
        # Continue to next filter in pipeline
        _apply_word_search_exclusions_and_render()
        return

    filtered = []
    excluded_items = []
    for r in search_state.results:
        sys_id = r.get('display', {}).get('id')
        if sys_id and sys_id in all_excluded_ids:
            # Find which source(s) excluded this
            sources = [s['label'] for s in search_state.exclusion_sources if sys_id in s['sys_ids']]
            excluded_items.append({
                'result': r,
                'reason': ', '.join(sources),
            })
        else:
            filtered.append(r)

    search_state.manuscript_excluded_results = excluded_items
    # Pass filtered results to next stage in pipeline
    # (domain exclusions, printed filter, measurement filters operate on filtered set)
```

### Desktop: Enhanced ExcludeDialog with List Tab

```python
# Pattern: add QTabWidget to existing ExcludeDialog
# Tab 1: existing sys_id/shelfmark text areas + file import
# Tab 2: list selection (reuse ListFilterDialog tree pattern)
tabs = QTabWidget()
tabs.addTab(self._build_text_tab(), tr("From File / Manual"))
tabs.addTab(self._build_list_tab(), tr("From List"))
```

### CSV Auto-Detect Column

```python
import csv
import io

def _parse_csv_shelfmarks(content: str) -> list[str]:
    """Extract shelfmarks from CSV content, auto-detecting the column."""
    reader = csv.reader(io.StringIO(content))
    headers = next(reader, None)
    if not headers:
        return []

    # Find shelfmark column by header name
    shelfmark_keywords = {'shelfmark', 'shelf_mark', 'call_number', 'signature', 'classmark', 'shelf mark'}
    col_idx = None
    for i, h in enumerate(headers):
        if h.strip().lower() in shelfmark_keywords:
            col_idx = i
            break

    # Fallback: first non-numeric text column
    if col_idx is None:
        col_idx = 0  # Default to first column

    return [row[col_idx].strip() for row in reader if len(row) > col_idx and row[col_idx].strip()]
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Desktop flat text exclusion | Needs multi-source tracking | This phase | Per-source clear, source labels in count |
| Web has no manuscript exclusion | Adding via established post-filter pipeline | This phase | Aligns with domain/printed/measurement filter patterns |

**Existing desktop infrastructure (can be enhanced, not rewritten):**
- `ExcludeDialog` (line 2890) -- text area input + file import
- `_apply_manual_exclusions()` (line 27184) -- filters results by sys_id/shelfmark
- `_item_matches_exclusion()` (line 27158) -- per-item check including Part/folio handling
- `_ensure_shelf_map()` (line 27119) -- builds norm_shelfmark->sys_id index
- Session persistence of `excluded_sys_ids`, `excluded_shelfmarks`, `excluded_raw_entries`

## Open Questions

1. **Composition search exclusions**
   - What we know: Desktop already applies `excluded_sys_ids` to composition search results (line 30473). The exclusion state is shared across regular and composition search.
   - What's unclear: Should web composition search (if it exists) also respect manuscript exclusions?
   - Recommendation: Apply exclusions to all search modes consistently. The exclusion_sources list is session-global.

2. **Maximum exclusion set size**
   - What we know: A user's list could have thousands of items. csv_bank has 255K entries.
   - What's unclear: What's a reasonable upper bound for imported file size?
   - Recommendation: No artificial limit. Use `set()` for O(1) membership checks. Show a warning for files >10,000 lines.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest |
| Config file | tests/ directory with conftest.py |
| Quick run command | `pytest tests/test_shelfmark_normalization.py -x -q` |
| Full suite command | `pytest tests/ -x -q` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| EXCL-01 | List items fetched and converted to exclusion sys_id set | unit | `pytest tests/test_exclusion.py::test_list_to_exclusion_source -x` | Wave 0 |
| EXCL-02 | File parsing: txt (one per line) and CSV (auto-detect column) | unit | `pytest tests/test_exclusion.py::test_file_parsing -x` | Wave 0 |
| EXCL-03 | Shelfmark resolution across conventions | unit | `pytest tests/test_exclusion.py::test_shelfmark_resolution -x` | Wave 0 |
| EXCL-04 | Post-search filtering excludes correct sys_ids, count displayed | unit | `pytest tests/test_exclusion.py::test_post_search_filter -x` | Wave 0 |

### Sampling Rate
- **Per task commit:** `pytest tests/test_exclusion.py -x -q`
- **Per wave merge:** `pytest tests/ -x -q`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `tests/test_exclusion.py` -- covers EXCL-01 through EXCL-04 (file parsing, resolution, filtering)
- [ ] Test fixtures for mock csv_bank with known shelfmarks and normalization variants

## Sources

### Primary (HIGH confidence)
- `web/pages/search.py` lines 58-140 -- SearchUIState with all existing exclusion/filter fields
- `web/pages/search.py` line 3197 -- `_apply_domain_exclusions()` post-search filter pattern
- `web/pages/search.py` line 3152 -- `_apply_word_search_exclusions_and_render()` per-result exclusion
- `genizah_core.py` line 125 -- `normalize_shelfmark()` canonical implementation
- `genizah_core.py` line 4066 -- `resolve_system_by_shelfmark()` full resolution
- `genizah_app.py` line 2890 -- `ExcludeDialog` existing desktop implementation
- `genizah_app.py` line 27158 -- `_item_matches_exclusion()` desktop exclusion check
- `genizah_app.py` line 27119 -- `_ensure_shelf_map()` norm->sys_id index builder
- `genizah_app.py` line 30433 -- `_save_session()` desktop session persistence with exclusion fields
- `web/user_lists.py` line 522 -- `get_items_in_list()` / `get_items_in_list_sync()`
- `web/supabase_client.py` line 595 -- `get_list_items()` Supabase query
- `web/components/filter_panel.py` line 220 -- `persist_value()` session persistence helper
- `list_filter_dialog.py` -- Desktop list selection dialog pattern

### Secondary (MEDIUM confidence)
- `56-CONTEXT.md` -- User decisions and canonical references

## Project Constraints (from CLAUDE.md)

- Python 3.10+, NiceGUI for web, PyQt6 for desktop
- Both apps must be maintained (shared service layer where possible)
- Hebrew RTL text handling required
- `utf-8-sig` encoding for CSV file reading (handles BOM)
- Session persistence patterns: `persist_value()` (web), `_schedule_session_save()` (desktop)
- No FastAPI, no backend server -- all local computation
- Test with `pytest tests/`

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- no new libraries, all existing infrastructure
- Architecture: HIGH -- existing post-filter pipeline + desktop ExcludeDialog are well-understood patterns
- Pitfalls: HIGH -- based on direct code reading of existing exclusion and session persistence patterns

**Research date:** 2026-03-29
**Valid until:** 2026-04-28 (stable -- internal patterns unlikely to change)
