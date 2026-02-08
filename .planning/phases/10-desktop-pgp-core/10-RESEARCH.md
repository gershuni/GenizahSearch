# Phase 10: Desktop PGP Core - Research

**Researched:** 2026-02-08
**Domain:** PyQt6 desktop integration with shared PGP document service
**Confidence:** HIGH

## Summary

This phase wires PGP edition and translation data from the shared `document_service.py` into the desktop app's existing version selector and transcription display. The desktop app (`genizah_app.py`, 15.8K lines) already has two independent version selectors -- one in `ResultDialog` (search results viewer) and one in the Browse tab -- each with its own combo box (`rd_version_combo` and `browse_version_combo`), cache, and loading logic. Both currently show V0.8 as default plus user corrections/versions from the corrections API.

The web app already implements the full PGP version selector pattern in `web/components/version_selector.py`. It groups items as: PGP Editions (with scholar names) -> separator -> Translations (grouped by language with translator names) -> separator -> V0.8 -> User Corrections. The desktop must replicate this grouping using QComboBox with `insertSeparator()`.

**Primary recommendation:** Add a `PGPSourceWorker(QThread)` in `gui_threads.py` that calls `get_all_sources_for_fragment()` and `get_document_for_fragment()` in a background thread. On completion, populate the version combo with PGP editions and translations before the existing HTR/correction entries. Auto-select the first PGP edition when available.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- PGP editions appear in their own group with a "PGP Editions" separator/header, placed above HTR entries
- Visual separator line between groups (PGP Editions, HTR, User Corrections)
- Each PGP edition shows scholar name as the label
- Show translator name + language in the selector entry
- When a manuscript has a PGP edition available, it auto-selects as the default (replacing HTR V0.8)
- HTR V0.8 remains available in the selector but is not pre-selected when PGP exists
- If no PGP edition exists, existing behavior unchanged (HTR default)
- Desktop calls the exact same `shared/document_service.py` functions the web uses
- All Supabase calls wrapped in QThread workers (matching existing desktop patterns with 14+ QThread worker examples)
- No desktop-specific data layer -- shared service is the single source
- Should follow whatever pattern the web app already uses for consistency (editions/translations grouping)

### Claude's Discretion
- Exact QThread worker class design (can follow existing patterns in genizah_app.py)
- How to handle loading states while fetching PGP data
- Error handling for network failures during PGP fetch
- Whether to pre-fetch PGP data or fetch on-demand when user opens a manuscript

### Deferred Ideas (OUT OF SCOPE)
- Offline PGP data cache (SQLite local store) -- POLISH-04, future v5.7.0+
</user_constraints>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `shared/document_service.py` | current | PGP data access (editions, translations, fragments) | Already extracted in Phase 8, web uses same module |
| `shared/supabase_provider.py` | current | Supabase client singleton | Shared between web and desktop |
| `PyQt6` | 6.x | Desktop UI framework | Already used throughout genizah_app.py |
| `gui_threads.py` | current | QThread worker definitions | Established pattern for all background operations |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `supabase-py` | sync | Supabase Python client (sync mode) | Underlying data access, already in dependencies |

### No New Dependencies
This phase requires zero new libraries. Everything needed is already in the codebase.

## Architecture Patterns

### Recommended Project Structure (Changes Only)
```
gui_threads.py          # ADD: PGPSourceWorker(QThread)
genizah_app.py          # MODIFY: Both version selectors + data loading
shared/document_service.py  # NO CHANGES (use as-is)
```

### Pattern 1: PGPSourceWorker QThread
**What:** A background thread that fetches PGP sources for a fragment.
**When to use:** Every time a manuscript is loaded in Browse tab or ResultDialog.
**Example:**
```python
# In gui_threads.py
class PGPSourceWorker(QThread):
    """Fetch PGP edition/translation sources for a fragment."""
    finished_signal = pyqtSignal(str, list, dict)  # sys_id, all_sources, pgp_doc
    error_signal = pyqtSignal(str, str)  # sys_id, error_message

    def __init__(self, sys_id: str, page_num: int = 1):
        super().__init__()
        self.sys_id = sys_id
        self.page_num = page_num

    def run(self):
        try:
            from shared.document_service import (
                get_all_sources_for_fragment,
                get_document_for_fragment,
                get_section_for_page
            )
            # Get all sources (editions + translations)
            all_sources = get_all_sources_for_fragment(self.sys_id)

            # Filter by page (recto/verso)
            current_page_info = 'recto' if self.page_num == 1 else 'verso'
            page_sources = []
            for source in all_sources:
                source_page = source.get('page_info')
                is_translation = 'Translation' in (source.get('doc_relation') or '')
                if source_page == current_page_info or not source_page:
                    if not is_translation and not source_page:
                        content = source.get('content')
                        if content:
                            source['content'] = get_section_for_page(content, self.page_num)
                    page_sources.append(source)

            # Get document metadata
            pgp_doc = get_document_for_fragment(self.sys_id, self.page_num)
            pgp_doc_dict = pgp_doc if pgp_doc else {}

            self.finished_signal.emit(self.sys_id, page_sources, pgp_doc_dict)
        except Exception as e:
            self.error_signal.emit(self.sys_id, str(e))
```

### Pattern 2: Version Combo Population with Groups
**What:** Building the QComboBox items with separators and headers matching the web app's grouping.
**When to use:** After PGPSourceWorker completes.
**Example:**
```python
def _populate_pgp_versions(self, combo, sources, pgp_doc):
    """Add PGP editions and translations to a version combo box.

    Web app pattern: Editions -> separator -> Translations -> separator -> V0.8 -> corrections
    """
    editions = [s for s in sources
                if 'Edition' in (s.get('doc_relation') or '') and s.get('content')]
    translations = [s for s in sources
                    if 'Translation' in (s.get('doc_relation') or '') and s.get('content')]

    if not editions and not translations:
        return False  # No PGP data

    combo.blockSignals(True)
    # Remember existing non-PGP items (V0.8 + corrections)
    # Rebuild combo: PGP first, then existing items

    # Clear and start fresh
    combo.clear()

    # === PGP Editions Group ===
    if editions:
        # Add non-selectable header item
        combo.addItem("-- PGP Editions --", {"source": "header"})
        # Make header item non-selectable
        model = combo.model()
        model.item(combo.count() - 1).setEnabled(False)

        for edition in editions:
            scholar = edition.get('source_scholar', 'Unknown')
            label = f"  {scholar}"  # Indented under header
            combo.addItem(label, {
                "source": "pgp_edition",
                "content": edition.get('content', ''),
                "scholar": scholar,
                "pgpid": edition.get('pgpid'),
                "source_id": edition.get('id')
            })

    # === Translations Group ===
    if translations:
        combo.insertSeparator(combo.count())
        combo.addItem("-- Translations --", {"source": "header"})
        model = combo.model()
        model.item(combo.count() - 1).setEnabled(False)

        for trans in translations:
            scholar = trans.get('source_scholar', 'Unknown')
            language = trans.get('language', '')
            label = f"  {language} - {scholar}"
            combo.addItem(label, {
                "source": "pgp_translation",
                "content": trans.get('content', ''),
                "scholar": scholar,
                "language": language,
                "pgpid": trans.get('pgpid'),
                "source_id": trans.get('id')
            })

    # === Separator before HTR ===
    combo.insertSeparator(combo.count())

    # === HTR V0.8 (always present) ===
    combo.addItem("V0.8", {"source": "original"})

    combo.blockSignals(False)
    return True  # PGP data was added
```

### Pattern 3: Auto-Selection Priority
**What:** When PGP editions exist, auto-select the first edition instead of V0.8.
**When to use:** After populating the combo.
**Example:**
```python
# After populating PGP editions:
if has_pgp_editions:
    # Find first edition index (skip header items)
    for i in range(combo.count()):
        data = combo.itemData(i)
        if data and data.get('source') == 'pgp_edition':
            combo.setCurrentIndex(i)
            # Display the edition content
            self._display_version_content(data)
            break
else:
    # No PGP - keep V0.8 as default (existing behavior)
    combo.setCurrentIndex(0)
```

### Pattern 4: Stale-Request Guard
**What:** Prevent displaying results from a previous request when user navigates quickly.
**When to use:** In the PGPSourceWorker finished callback.
**Example:**
```python
def _on_pgp_sources_loaded(self, sys_id, sources, pgp_doc):
    """Handle PGP sources loaded from background thread."""
    # Guard: user may have navigated to a different manuscript
    if sys_id != self.current_browse_sid:
        return  # Discard stale result

    # Populate version combo with PGP sources
    self._populate_pgp_versions(self.browse_version_combo, sources, pgp_doc)
```

### Anti-Patterns to Avoid
- **Blocking UI with Supabase calls:** Never call `get_all_sources_for_fragment()` on the main thread. Always use QThread.
- **Duplicate version combo rebuilding:** The existing `_check_document_community_status()` rebuilds the combo for corrections. PGP loading must coordinate with this -- either combine into one flow or ensure PGP data is populated first, then corrections are added after.
- **Losing corrections on PGP refresh:** When rebuilding the combo with PGP items, the existing corrections/versions must be re-added afterward. Do not clear corrections data.
- **Import at module level:** Import `shared.document_service` inside the QThread `run()` method (lazy import), not at module top. This prevents import issues if supabase is not configured.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| PGP data access | Custom Supabase queries | `shared/document_service.py` functions | Already tested, handles errors, same as web |
| Page filtering (recto/verso) | Manual page logic | `get_section_for_page()` from document_service | Handles edge cases, multi-section docs |
| Source ordering | Custom sort logic | `get_all_sources_for_fragment()` already sorts | Editions before translations, by sequence_order |
| Background threading | `threading.Thread` or `asyncio` | `QThread` with `pyqtSignal` | Must integrate with Qt event loop for UI updates |

**Key insight:** The shared document_service already does all the heavy lifting. The desktop phase is purely about wiring: call the service in a QThread, populate QComboBox items, and handle selection changes.

## Common Pitfalls

### Pitfall 1: UI Freeze on Supabase Calls
**What goes wrong:** Calling Supabase from the main thread freezes the UI for 200-2000ms.
**Why it happens:** Network I/O blocks the Qt event loop.
**How to avoid:** All Supabase calls in QThread workers with pyqtSignal callbacks.
**Warning signs:** UI becomes unresponsive when navigating between manuscripts.

### Pitfall 2: Race Condition Between PGP and Corrections Loading
**What goes wrong:** `_check_document_community_status()` (which loads corrections) runs synchronously and rebuilds the version combo. If PGP loading finishes after, it overwrites corrections. If before, corrections overwrite PGP items.
**Why it happens:** Two independent data sources populate the same combo.
**How to avoid:** Either: (A) Make PGP loading happen first (synchronous is currently the pattern for corrections), then corrections append to PGP items; or (B) Combine both into a single flow. Recommendation: **Option A** -- PGP worker runs first, then corrections are appended after PGP combo population, modifying `_check_document_community_status()` to append rather than rebuild from scratch.
**Warning signs:** PGP editions disappear after corrections load, or corrections disappear.

### Pitfall 3: Stale Results from Fast Navigation
**What goes wrong:** User opens manuscript A, PGP fetch starts. User navigates to manuscript B. PGP results for A arrive and overwrite B's combo.
**Why it happens:** Async callback doesn't check if the target manuscript has changed.
**How to avoid:** Store `sys_id` in the worker, check against `current_browse_sid` (or `current_sys_id` for ResultDialog) in the callback.
**Warning signs:** Wrong scholar/edition displayed for the current manuscript.

### Pitfall 4: Two Independent Version Selectors
**What goes wrong:** Implementing PGP in Browse tab but forgetting ResultDialog (or vice versa).
**Why it happens:** The app has two entirely separate viewers with separate combos: `browse_version_combo` in the Browse tab and `rd_version_combo` in ResultDialog.
**How to avoid:** Both viewers must get PGP support. Extract shared helper methods that both can call.
**Warning signs:** PGP works in Browse but not when clicking search results.

### Pitfall 5: QComboBox Header Items Being Selectable
**What goes wrong:** User selects the "-- PGP Editions --" header item, causing a crash or blank display.
**Why it happens:** QComboBox items are selectable by default.
**How to avoid:** Use `combo.model().item(index).setEnabled(False)` to make header items non-selectable. Or use `insertSeparator()` instead of text headers.
**Warning signs:** Selecting a separator/header item triggers version change callback with null content.

### Pitfall 6: Translation Text Directionality
**What goes wrong:** English translations render left-to-right but the display widget is hardcoded RTL.
**Why it happens:** `text_ms.setLayoutDirection(Qt.LayoutDirection.RightToLeft)` and the HTML wraps in `<div dir='rtl'>`.
**How to avoid:** When displaying English translations, change the display direction. Check `language` field: if 'English', use LTR; if 'Hebrew' or default, use RTL.
**Warning signs:** English translation text is right-aligned and hard to read.

## Code Examples

### Example 1: Web App's Grouping Pattern (Reference Implementation)
```python
# From web/components/version_selector.py (lines 200-286)
# Web groups as: PGP Transcriptions header -> editions -> separator ->
# Translations header -> Hebrew -> English -> Other -> separator ->
# V0.8 -> User Corrections

# The desktop should mirror this structure:
# -- PGP Editions --
#   Scholar Name 1
#   Scholar Name 2
# ────────────────── (separator)
# -- Translations --
#   Hebrew - Scholar Name
#   English - Scholar Name
# ────────────────── (separator)
# V0.8
# ────────────────── (separator)
# by Username (2026-01-15) [corrections]
```

### Example 2: Existing QThread Pattern (from gui_threads.py)
```python
# Source: gui_threads.py lines 309-325
class EnrichMetadataThread(QThread):
    """Fetch extended metadata in the background."""
    finished_signal = pyqtSignal(str, dict)

    def __init__(self, meta_mgr, system_id):
        super().__init__()
        self.meta_mgr = meta_mgr
        self.system_id = system_id

    def run(self):
        try:
            data = self.meta_mgr.enrich_metadata(self.system_id)
            self.finished_signal.emit(self.system_id, data)
        except Exception:
            self.finished_signal.emit(self.system_id, {})
```

### Example 3: Existing Browse Version Combo Flow
```python
# Source: genizah_app.py lines 5153-5323
# Current flow in _check_document_community_status():
# 1. Store original text in browse_original_page_text
# 2. Reset combo: clear -> addItem("V0.8", {"source": "original"})
# 3. Check server availability
# 4. Fetch versions (V0.7, user versions) -> add to combo
# 5. Fetch corrections -> add to combo
# 6. Enable combo if count > 1
# 7. Auto-select default if exists

# New flow must INSERT PGP items BEFORE step 2's V0.8 item.
```

### Example 4: document_source Dict Structure
```python
# From shared/document_service.py get_sources_for_document()
# Each source dict contains:
{
    'id': 123,                          # Source record ID
    'pgpid': 45678,                     # PGP document ID
    'source_scholar': 'Goitein, S.D.',  # Scholar name (label)
    'doc_relation': 'Digital Edition',  # or 'Digital Translation'
    'content': '...',                   # Transcription/translation text
    'language': 'Hebrew',              # Language (for translations)
    'content_length': 1500,            # Character count
    'sequence_order': 1,               # Display ordering
    'created_at': '2026-02-07T...',    # Timestamp
    'page_info': 'recto'              # Added by get_all_sources_for_fragment()
}
```

### Example 5: QComboBox insertSeparator Usage
```python
# QComboBox.insertSeparator(index) adds a visual divider line
combo = QComboBox()
combo.addItem("Edition 1", {"source": "pgp_edition", ...})
combo.addItem("Edition 2", {"source": "pgp_edition", ...})
combo.insertSeparator(combo.count())  # Separator after editions
combo.addItem("V0.8", {"source": "original"})
combo.insertSeparator(combo.count())  # Separator before corrections
combo.addItem("by User (2026-01-15)", {"source": "correction", ...})
```

## Discretion Recommendations

### QThread Worker Design
**Recommendation:** Create a single `PGPSourceWorker(QThread)` class in `gui_threads.py` following the `EnrichMetadataThread` pattern. It takes `sys_id` and `page_num`, emits `(sys_id, page_sources_list, pgp_doc_dict)`. Both ResultDialog and Browse tab use the same worker class.
**Confidence:** HIGH -- this follows established project patterns exactly.

### Loading States
**Recommendation:** Lightweight approach -- when PGP fetch starts, the combo is populated with just "V0.8" (existing behavior). When PGP data arrives, the combo is rebuilt with PGP items prepended. No spinner or "Loading..." placeholder needed since the HTR text is already visible. The user sees V0.8 text immediately and the combo updates silently within 200-500ms.
**Confidence:** HIGH -- matches the existing pattern where `_check_document_community_status()` runs after page is already displayed.

### Error Handling for Network Failures
**Recommendation:** On PGP fetch failure, silently fall back to existing behavior (V0.8 + corrections). Log the error with `logger.debug()`. No user-facing error message -- PGP is an enhancement, not a core feature. The corrections flow already handles server-down gracefully.
**Confidence:** HIGH -- consistent with existing error handling patterns throughout the desktop app.

### Pre-fetch vs On-demand
**Recommendation:** Fetch on-demand when the user opens a manuscript. Start the PGPSourceWorker in `browse_load()` (Browse tab) and `load_result_by_index()` (ResultDialog) alongside the existing metadata enrichment thread. Do not pre-fetch for search results lists -- that would make 50+ Supabase calls per search.
**Confidence:** HIGH -- matches how `EnrichMetadataThread` is triggered (on manuscript load, not in batch).

### Translation Grouping
**Recommendation:** Use separate groups for editions and translations, matching the web app exactly. The web app has: "PGP Transcriptions" header -> editions -> separator -> "Translations" header -> grouped by language. The desktop should mirror this with QComboBox disabled header items + separators. This provides consistent cross-app experience.
**Confidence:** HIGH -- directly follows the web app's established pattern in `web/components/version_selector.py`.

## Integration Points

### Where PGP Loading Must Be Triggered

1. **Browse Tab -- `browse_load()` / `on_browse_enriched_loaded()`** (line ~6804)
   - Currently calls `_check_document_community_status()` which rebuilds the version combo
   - Must also trigger PGP source fetch here
   - PGP worker should start alongside or before community status check

2. **ResultDialog -- `_rd_on_data_loaded()` / `_rd_refresh_versions()`** (line ~2880)
   - Currently calls `_rd_refresh_versions(select_latest=True)` which rebuilds combo
   - Must also trigger PGP source fetch here
   - Same coordination challenge as Browse tab

### Coordination Strategy
The cleanest approach is to modify the existing `_check_document_community_status()` and `_rd_refresh_versions()` methods to:
1. First populate PGP items (from worker callback)
2. Then append corrections/versions (existing logic)
3. Finally apply auto-selection (PGP edition > corrections > V0.8)

Alternatively, the PGP worker callback can populate PGP items and then call the existing corrections-loading method to append corrections after.

### Data Flow Summary
```
User opens manuscript
    |
    v
browse_load() / load_result_by_index()
    |
    +-> PGPSourceWorker.start(sys_id, page_num)
    |       |
    |       v (background)
    |   get_all_sources_for_fragment(sys_id)
    |   get_document_for_fragment(sys_id, page_num)
    |       |
    |       v (signal -> main thread)
    |   _on_pgp_sources_loaded(sys_id, sources, pgp_doc)
    |       |
    |       v
    |   Populate combo: PGP editions -> translations -> separator
    |   Auto-select first edition
    |   Display edition content in text browser
    |       |
    |       v
    +-> _check_document_community_status() [existing]
            |
            v
        Append corrections/versions to combo after PGP items
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Single PGP transcription per document | Multi-source: multiple editions + translations | Phase 9 (Feb 2026) | Version selector must show all sources, not just one |
| `get_document_for_fragment()` only | `get_all_sources_for_fragment()` for multi-source | Phase 9 (Feb 2026) | Richer data for version selector |
| Corrections API only for versions | PGP + Corrections as version sources | This phase | Version selector grows from 2-3 items to potentially 6+ |

## Open Questions

1. **Combo width with longer labels**
   - What we know: Current combo width is `setFixedWidth(180)`. PGP labels like "Hebrew - Goitein, S.D." may exceed this.
   - What's unclear: Whether to increase width or use tooltip for full name.
   - Recommendation: Increase to 220-250px, or use `setMinimumWidth()` instead of fixed width. Test with real scholar names from the data.

2. **Ordering of PGP worker vs corrections loading**
   - What we know: Corrections are currently loaded synchronously in `_check_document_community_status()`. PGP is async via QThread.
   - What's unclear: Whether to make corrections also async, or ensure PGP finishes first.
   - Recommendation: Keep corrections synchronous (they're fast, under 100ms), run PGP worker first, then call corrections loading in the PGP completion callback. This ensures PGP items are in the combo before corrections are appended.

3. **ResultDialog parent access pattern**
   - What we know: ResultDialog accesses `parent.corrections_client` for corrections. PGP uses `shared.document_service` directly (no client needed).
   - What's unclear: Whether PGP worker reference should be stored on parent or on dialog.
   - Recommendation: Store the worker reference on the dialog itself (`self.pgp_worker = PGPSourceWorker(...)`), similar to how `self.preload_meta_worker` is used.

## Sources

### Primary (HIGH confidence)
- `C:\GenizahSearch\shared\document_service.py` -- Full API examined, all functions documented
- `C:\GenizahSearch\web\components\version_selector.py` -- Complete web implementation of version grouping
- `C:\GenizahSearch\genizah_app.py` lines 2416-2424, 2880-3150, 5060-5323, 6430-6443 -- Both version selectors fully examined
- `C:\GenizahSearch\gui_threads.py` lines 309-325 -- EnrichMetadataThread pattern (canonical QThread pattern)
- `C:\GenizahSearch\web\pages\browse.py` lines 880-950 -- Web PGP data loading flow

### Secondary (MEDIUM confidence)
- PyQt6 QComboBox.insertSeparator() -- Verified via Qt documentation
- QStandardItemModel.item().setEnabled(False) -- Standard Qt pattern for non-selectable combo items

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- no new libraries, all existing codebase components
- Architecture: HIGH -- follows established QThread + signal patterns with 14+ existing examples
- Pitfalls: HIGH -- identified from reading actual code flow and coordination requirements
- Web parity: HIGH -- web implementation fully examined and documented

**Research date:** 2026-02-08
**Valid until:** 2026-03-08 (stable codebase, no external dependencies changing)
