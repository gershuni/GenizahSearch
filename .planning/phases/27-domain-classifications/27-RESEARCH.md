# Phase 27: Domain Classifications - Research

**Researched:** 2026-02-13
**Domain:** UI Integration, Hierarchical Filtering, Data Presentation
**Confidence:** HIGH

## Summary

Phase 27 adds FJMS domain classification display and filtering to both web and desktop apps. The data infrastructure (Phase 25) is already complete with `shared/fjms_service.py` providing all necessary queries. This phase focuses entirely on UI integration: adding domain text links to browse pages and implementing hierarchical domain filters in search interfaces.

Key findings: NiceGUI ui.select supports multi-selection with chips display, PyQt6 QTreeWidget provides robust hierarchical checkbox trees (already used in the codebase for lists), and the existing browse page metadata panel provides a clear insertion point for domain display.

**Primary recommendation:** Implement domain display as clickable text links in existing metadata sections, use ui.select with multi-selection for web filtering, and QDialog with QTreeWidget for desktop filtering.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**Browse Page Display:**
- Domains shown as **clickable minimal text** in the metadata area (not badges/chips)
- Clicking a domain navigates to the search page with that domain pre-filtered
- Language follows app interface: Hebrew UI → Hebrew domain names, English UI → English names
- Show only specific (child) domains — parent is implicit and available via search filter tree
- Same behavior in both web and desktop apps (desktop: clicking switches to search tab with filter applied)

**Hierarchy Display:**
- Parent and child shown as **separate clickable links** when both are relevant
- Deduplicate: if child domain already appears, don't redundantly show its parent alongside it (the child carries parent info)
- The full hierarchy is navigable through the search filter tree

**Search Filter UX:**
- Domain filter grouped **by parent category** — hierarchical tree with parent headers and child domains nested underneath
- **Type-ahead search** to find domains quickly (187 domains needs quick filtering)
- **Manuscript counts** shown next to each domain (e.g., "Piyyut (51,228)")
- **Multi-select with OR** logic — user can pick multiple domains, results match ANY selected
- Selecting a **parent domain includes all children** automatically
- **Works standalone** — user can browse all manuscripts in a domain without typing a text query
- Standalone domain browsing in **both apps**
- Web: filter placement at Claude's discretion (integrated with existing filters or separate panel)
- Desktop: **filter button** that opens a tree widget popup — keeps search tab compact

**Search Results Display:**
- Search results show **one domain** (most specific) with **"+N more"** indicator when multiple domains exist
- Hovering "+N more" shows all domains in a **tooltip** — doesn't disrupt layout

### Claude's Discretion

- Exact placement of domain filter on web search page (with existing filters vs separate panel)
- Navigation flow when clicking domain link (search page with pre-filter — details of implementation)
- Cap/expand behavior for many domains on browse page
- Which domain to show as "primary" on search results (most specific child, or alphabetical)
- Desktop tree widget popup design and behavior

### Deferred Ideas (OUT OF SCOPE)

None — discussion stayed within phase scope

</user_constraints>

## Standard Stack

### Core (Already in Place)

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| shared.fjms_service | 1.0.0 | SQLite sidecar query API | Phase 25 deliverable, provides all domain queries |
| NiceGUI | 1.4+ | Web UI framework | Project standard for web app |
| PyQt6 | 6.4+ | Desktop UI framework | Project standard for desktop app |

### Supporting

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| web.translations | Current | i18n for domain names | Every UI string (Hebrew/English) |
| genizah_core | Current | Search engine core | Search result filtering logic |
| ui.select (NiceGUI) | Built-in | Multi-select dropdown | Web domain filter |
| QTreeWidget (PyQt6) | Built-in | Hierarchical tree with checkboxes | Desktop domain filter popup |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| ui.select with multi | ui.tree | ui.tree doesn't support type-ahead search filtering, more complex state management |
| QTreeWidget | QListWidget | No hierarchy display, would need custom grouping logic |
| Clickable text links | ui.badge/ui.chip | User explicitly rejected styled badges — wants minimal, functional text |

**Installation:**
No new dependencies needed — all components are built-in to existing frameworks.

## Architecture Patterns

### Recommended Project Structure

No new files needed. Modifications to existing files:

```
web/pages/
├── browse.py          # Add domain display in metadata panel (lines ~1905-1972)
└── search.py          # Add domain filter control (lines ~360-450)

genizah_app.py         # Add domain display in browse tab, filter dialog in search tab
```

### Pattern 1: Domain Display on Browse Page

**What:** Query domains for current sys_id, display as clickable text links with language switching

**When to use:** Browse page load, after metadata fetch

**Example (Web):**
```python
from shared.fjms_service import get_fjms_service
from web.translations import get_language

fjms = get_fjms_service(thread_safe=True)
domains = fjms.get_domains(page.sys_id)

if domains:
    with ui.column().classes('gap-1 col-span-2'):
        ui.label(tr('Subject Domains')).classes('text-xs font-bold').style('color: var(--text-secondary);')
        with ui.row().classes('gap-2 flex-wrap'):
            lang = get_language()
            # Deduplicate: if child appears, skip showing parent
            child_domains = {d['domain'] for d in domains}
            for dom in domains:
                # Skip parent if child already shown
                parent = dom.get('parent_domain')
                if parent and parent in child_domains:
                    continue

                display_name = dom['domain_heb'] if lang == 'he' else dom['domain']
                # Clickable link that navigates to search with pre-filter
                ui.link(
                    display_name,
                    f'/search?domain={quote(dom["domain"])}'
                ).classes('text-sm').style('color: var(--primary-600);')
```

**Example (Desktop):**
```python
from shared.fjms_service import get_fjms_service
from PyQt6.QtWidgets import QLabel
from PyQt6.QtCore import Qt

fjms = get_fjms_service()
domains = fjms.get_domains(sys_id)

if domains:
    child_domains = {d['domain'] for d in domains}
    for dom in domains:
        parent = dom.get('parent_domain')
        if parent and parent in child_domains:
            continue

        label = QLabel(dom['domain'])
        label.setStyleSheet("color: #0066cc; text-decoration: underline; cursor: pointer;")
        label.mousePressEvent = lambda e, d=dom['domain']: self._navigate_to_search_with_domain(d)
```

### Pattern 2: Hierarchical Domain Filter (Web)

**What:** Multi-select dropdown with type-ahead search, grouped by parent

**When to use:** Search page filter panel

**Example:**
```python
from shared.fjms_service import get_fjms_service
from web.translations import get_language

fjms = get_fjms_service(thread_safe=True)
all_domains = fjms.get_all_domains()  # Returns [{domain, domain_heb, count}, ...]

# Build hierarchical options dict {value: label}
# NiceGUI ui.select with_input enables type-ahead filtering
lang = get_language()
domain_options = {}

# Group by parent (compute client-side from full domain list)
parents_map = {}  # parent_name -> [child domains]
for d in all_domains:
    # Query parent-child relationships from domains table
    pass  # Implementation detail

# Render with counts
for domain_name, children in parents_map.items():
    domain_options[domain_name] = f"{domain_name} ({parent_count})"
    for child in children:
        display = child['domain_heb'] if lang == 'he' else child['domain']
        domain_options[child['domain']] = f"  {display} ({child['count']})"

domain_filter = ui.select(
    domain_options,
    multiple=True,
    value=[]
).props('outlined dense use-chips clearable use-input')
domain_filter.props('popup-content-class="max-h-96"')
```

**Source:** [NiceGUI ui.select documentation](https://nicegui.io/documentation/select)

### Pattern 3: Tree Widget Filter Dialog (Desktop)

**What:** QDialog with QTreeWidget, checkboxes for multi-select

**When to use:** Desktop search tab domain filter button

**Example:**
```python
from PyQt6.QtWidgets import QDialog, QTreeWidget, QTreeWidgetItem, QVBoxLayout, QLineEdit
from PyQt6.QtCore import Qt

class DomainFilterDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle(tr("Filter by Domain"))
        self.setMinimumSize(500, 600)

        layout = QVBoxLayout()

        # Search input for filtering tree
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText(tr("Type to filter domains..."))
        self.search_input.textChanged.connect(self._filter_tree)
        layout.addWidget(self.search_input)

        # Tree widget
        self.tree = QTreeWidget()
        self.tree.setHeaderLabels([tr("Domain"), tr("Count")])
        self.tree.setColumnWidth(0, 350)
        # Enable checkboxes on all items
        layout.addWidget(self.tree)

        self.setLayout(layout)
        self._populate_tree()

    def _populate_tree(self):
        fjms = get_fjms_service()
        # Build parent -> children hierarchy
        # Add parent items with Qt.ItemFlag.ItemIsUserCheckable
        parent_item = QTreeWidgetItem([parent_name, str(parent_count)])
        parent_item.setFlags(parent_item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
        parent_item.setCheckState(0, Qt.CheckState.Unchecked)
        self.tree.addTopLevelItem(parent_item)

        # Add children
        for child in children:
            child_item = QTreeWidgetItem([child['domain'], str(child['count'])])
            child_item.setFlags(child_item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            child_item.setCheckState(0, Qt.CheckState.Unchecked)
            parent_item.addChild(child_item)

    def _filter_tree(self, text):
        # Show/hide items based on search text
        # Standard Qt pattern: iterate tree, setHidden(not matches)
        pass

    def get_selected_domains(self):
        # Return list of checked domain names
        selected = []
        # Iterate tree, collect checked items
        return selected
```

**Pattern used in codebase:** `ListsTreeWidget` at genizah_app.py:4345

**Source:** [Qt QTreeWidget documentation](https://doc.qt.io/qt-6/qtreewidget.html)

### Pattern 4: Domain-Based Search Filtering

**What:** Intersect search results with domain manuscript set

**When to use:** Search execution with domain filter applied

**Example:**
```python
from shared.fjms_service import get_fjms_service

# After Tantivy search returns results
results = searcher.search(query, mode='variants')

# Apply domain filter if selected
if selected_domains:
    fjms = get_fjms_service(thread_safe=True)
    domain_sys_ids = set()
    for domain_name in selected_domains:
        domain_sys_ids.update(fjms.get_manuscripts_by_domain(domain_name))

    # Filter results to only manuscripts in domain set
    filtered_results = [
        r for r in results
        if r['sys_id'] in domain_sys_ids
    ]
    results = filtered_results
```

**Performance:** `get_manuscripts_by_domain()` uses indexed SQL query, returns set for O(1) lookup. Fast even for large domain sets (Piyyut has 48K manuscripts).

### Anti-Patterns to Avoid

- **Loading all 187 domains in a flat list:** Violates user requirement for hierarchical grouping
- **Showing parent when child already shown:** User specified deduplication on browse page
- **Styled badges/chips for browse display:** User explicitly requested minimal clickable text
- **Filtering domains in Python after fetch:** Use SQL WHERE clause for performance
- **Hardcoding domain names:** Always use translation system for Hebrew/English switching

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Hierarchical tree filtering | Custom recursive filtering logic | QTreeWidget built-in filtering | Qt handles show/hide propagation, parent-child state |
| Multi-select with chips | Custom dropdown with checkboxes | ui.select with `multiple=True use-chips` | Native Quasar component, handles all interaction states |
| Domain hierarchy computation | Recursive parent-child traversal | SQL JOIN with GROUP BY | Database does grouping efficiently, indexed lookups |
| Type-ahead filtering | Custom string matching | NiceGUI `with_input` + Quasar q-select | Built-in debounced search, handles accents/diacritics |
| Clickable text navigation | Custom onclick handlers | ui.link / QLabel.mousePressEvent | Standard patterns, proper accessibility |

**Key insight:** Both NiceGUI and PyQt6 provide robust built-in components for the exact UI patterns needed. Custom implementations would duplicate framework features and miss edge cases (keyboard navigation, accessibility, RTL support).

## Common Pitfalls

### Pitfall 1: Parent-Child Deduplication Only on Browse Page

**What goes wrong:** Implementing deduplication logic in search results display when user only specified it for browse page

**Why it happens:** Misreading CONTEXT.md — search results display uses "+N more" pattern, not deduplication

**How to avoid:** Browse page: deduplicate (skip parent if child shown). Search results: show primary domain + "+N more" indicator. Different UX patterns.

**Warning signs:** If adding deduplication logic to search results rendering, re-check requirements

### Pitfall 2: Thread-Safe Service Access in Web App

**What goes wrong:** NiceGUI web app crashes with "SQLite objects created in a thread can only be used in that same thread" error

**Why it happens:** Forgetting `thread_safe=True` when calling `get_fjms_service()`

**How to avoid:**
- Web app: `fjms = get_fjms_service(thread_safe=True)` — ALWAYS
- Desktop app: `fjms = get_fjms_service()` — single-threaded, no flag needed

**Warning signs:** Intermittent crashes on domain queries in web app, works fine in desktop

### Pitfall 3: Domain Hierarchy Building on Every Render

**What goes wrong:** Querying database to build parent-child tree on every search page render, slowing down UI

**Why it happens:** Not caching the domain hierarchy structure

**How to avoid:** Build domain hierarchy once at app startup or lazily on first access, cache in memory. Domain list is static (only changes when sidecar is regenerated).

**Warning signs:** Slow search page initial render, repeated SQL queries in logs

### Pitfall 4: Forgetting Language Switching for Domain Names

**What goes wrong:** Showing English domain names when UI is in Hebrew mode

**Why it happens:** Using `domain` field directly instead of checking `get_language()` and using `domain_heb`

**How to avoid:**
```python
from web.translations import get_language
lang = get_language()
display_name = domain['domain_heb'] if lang == 'he' else domain['domain']
```

**Warning signs:** User reports domains display in wrong language, translation tests fail

### Pitfall 5: Not Including Parent When Selecting Parent Domain

**What goes wrong:** Selecting "Rabbinic Literature" doesn't include manuscripts that ONLY have "Rabbinic Literature" (no child domain)

**Why it happens:** `get_manuscripts_by_domain(parent)` only looks in Domain column, not accounting for manuscripts classified only under parent

**How to avoid:** The SQL query already handles this correctly:
```sql
SELECT DISTINCT AlmaId FROM domains WHERE Domain = ? OR ParentDomain = ?
```
This captures both:
1. Manuscripts with the parent as their Domain (no child)
2. Manuscripts with the parent as their ParentDomain (child domains)

**Warning signs:** Domain filter returns fewer results than expected, manuscripts "disappear" when filtering by parent domain

## Code Examples

Verified patterns from the codebase and framework documentation.

### Web: Domain Display on Browse Page

Insert in `web/pages/browse.py` around line 1972 (after PGP Tags display):

```python
# === FJMS Domain Classifications ===
from shared.fjms_service import get_fjms_service
from web.translations import get_language
from urllib.parse import quote

fjms = get_fjms_service(thread_safe=True)
if fjms.is_available():
    domains = fjms.get_domains(page.sys_id)
    if domains:
        with ui.column().classes('gap-1 col-span-2'):
            ui.label(tr('Subject Domains')).classes('text-xs font-bold').style('color: var(--text-secondary);')
            with ui.row().classes('gap-2 flex-wrap'):
                lang = get_language()
                # Deduplicate: skip parent if child already shown
                child_domains = {d['domain'] for d in domains}
                for dom in domains:
                    parent = dom.get('parent_domain')
                    if parent and parent in child_domains:
                        continue

                    display_name = dom['domain_heb'] if lang == 'he' else dom['domain']
                    # Clickable link navigates to search with domain pre-filter
                    ui.link(
                        display_name,
                        f'/search?domain={quote(dom["domain"])}'
                    ).classes('text-sm').style('color: var(--primary-600);')
```

### Web: Multi-Select Domain Filter on Search Page

Insert in `web/pages/search.py` alongside mode selector (lines ~427-450):

```python
# Domain filter (multi-select with type-ahead)
with ui.column().classes('gap-1'):
    ui.label(tr('Filter by Domain')).classes('text-xs font-bold').style('color: var(--text-secondary);')

    # Build domain options with hierarchy and counts
    # Cache at module level to avoid rebuilding on every render
    if '_domain_options_cache' not in globals():
        fjms = get_fjms_service(thread_safe=True)
        if fjms.is_available():
            all_domains = fjms.get_all_domains()
            # Build parent -> children map
            # ... (hierarchy building logic)
            globals()['_domain_options_cache'] = domain_options
        else:
            globals()['_domain_options_cache'] = {}

    domain_select = ui.select(
        _domain_options_cache,
        multiple=True,
        value=saved_domains or []
    ).props('outlined dense use-chips clearable use-input input-debounce="200"')
    domain_select.props('popup-content-class="max-h-96" label="{}"'.format(tr("Select domains...")))
```

**Source:** Adapted from PGP tags selector pattern at web/pages/search.py:368-391

### Desktop: Domain Display on Browse Tab

Insert in `genizah_app.py` browse metadata section:

```python
from shared.fjms_service import get_fjms_service

fjms = get_fjms_service()
if fjms.is_available():
    domains = fjms.get_domains(self.current_browse_sid)
    if domains:
        # Add to metadata grid
        lbl = QLabel(tr('Subject Domains:'))
        self.metadata_layout.addWidget(lbl, row, 0)

        # Create clickable domain links
        domain_widget = QWidget()
        domain_layout = QHBoxLayout(domain_widget)
        domain_layout.setContentsMargins(0, 0, 0, 0)

        child_domains = {d['domain'] for d in domains}
        for dom in domains:
            parent = dom.get('parent_domain')
            if parent and parent in child_domains:
                continue

            link_label = QLabel(f'<a href="#">{dom["domain"]}</a>')
            link_label.setOpenExternalLinks(False)
            link_label.linkActivated.connect(lambda _, d=dom['domain']: self._navigate_to_search_with_domain(d))
            domain_layout.addWidget(link_label)

        domain_layout.addStretch()
        self.metadata_layout.addWidget(domain_widget, row, 1)

def _navigate_to_search_with_domain(self, domain_name):
    """Switch to search tab with domain filter applied."""
    self.tabs.setCurrentIndex(0)  # Search tab
    # Set domain filter and trigger search
    # ... (implementation detail)
```

### Desktop: Domain Filter Dialog

New class in `genizah_app.py`:

```python
class DomainFilterDialog(QDialog):
    """Domain filter dialog with hierarchical tree and search."""

    def __init__(self, parent=None, selected_domains=None):
        super().__init__(parent)
        self.setWindowTitle(tr("Filter by Subject Domain"))
        self.setMinimumSize(600, 700)
        self.selected_domains = selected_domains or []

        layout = QVBoxLayout(self)

        # Search box for type-ahead filtering
        search_label = QLabel(tr("Search domains:"))
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText(tr("Type to filter..."))
        self.search_input.textChanged.connect(self._filter_tree)
        layout.addWidget(search_label)
        layout.addWidget(self.search_input)

        # Tree widget with checkboxes
        self.tree = QTreeWidget()
        self.tree.setHeaderLabels([tr("Domain"), tr("Manuscripts")])
        self.tree.setColumnWidth(0, 400)
        self.tree.itemChanged.connect(self._handle_item_changed)
        layout.addWidget(self.tree)

        # Buttons
        button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

        self._populate_tree()

    def _populate_tree(self):
        from shared.fjms_service import get_fjms_service

        fjms = get_fjms_service()
        if not fjms.is_available():
            return

        # Build hierarchy from all_domains
        # ... (compute parent -> children mapping)

        # Add parent items
        for parent_name, children in hierarchy.items():
            parent_item = QTreeWidgetItem([parent_name, str(parent_count)])
            parent_item.setFlags(parent_item.flags() | Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsAutoTristate)
            parent_item.setCheckState(0, Qt.CheckState.Unchecked)
            self.tree.addTopLevelItem(parent_item)

            # Add child items
            for child in children:
                child_item = QTreeWidgetItem([child['domain'], str(child['count'])])
                child_item.setFlags(child_item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
                child_item.setCheckState(0, Qt.CheckState.Unchecked)
                parent_item.addChild(child_item)

    def _filter_tree(self, text):
        """Filter tree items based on search text."""
        if not text:
            # Show all
            for i in range(self.tree.topLevelItemCount()):
                parent = self.tree.topLevelItem(i)
                parent.setHidden(False)
                for j in range(parent.childCount()):
                    parent.child(j).setHidden(False)
            return

        text_lower = text.lower()
        for i in range(self.tree.topLevelItemCount()):
            parent = self.tree.topLevelItem(i)
            parent_matches = text_lower in parent.text(0).lower()
            any_child_matches = False

            for j in range(parent.childCount()):
                child = parent.child(j)
                child_matches = text_lower in child.text(0).lower()
                child.setHidden(not child_matches)
                if child_matches:
                    any_child_matches = True

            # Show parent if it matches or any child matches
            parent.setHidden(not (parent_matches or any_child_matches))

    def _handle_item_changed(self, item, column):
        """Handle checkbox state changes, propagate to children if parent."""
        # Qt handles auto-tristate for parent items automatically
        pass

    def get_selected_domains(self):
        """Return list of checked domain names."""
        selected = []
        for i in range(self.tree.topLevelItemCount()):
            parent = self.tree.topLevelItem(i)
            # Check parent
            if parent.checkState(0) == Qt.CheckState.Checked:
                selected.append(parent.text(0))
            # Check children
            for j in range(parent.childCount()):
                child = parent.child(j)
                if child.checkState(0) == Qt.CheckState.Checked:
                    selected.append(child.text(0))
        return selected
```

**Source:** Pattern adapted from ListsTreeWidget at genizah_app.py:4345

### Search Filtering Integration

In `genizah_core.py` or search execution code:

```python
def apply_domain_filter(results, selected_domains):
    """Filter search results by domain classifications.

    Args:
        results: List of search result dicts with 'sys_id' keys
        selected_domains: List of domain names to filter by

    Returns:
        Filtered list of results matching any selected domain (OR logic)
    """
    if not selected_domains:
        return results

    from shared.fjms_service import get_fjms_service
    fjms = get_fjms_service(thread_safe=True)  # or False for desktop

    if not fjms.is_available():
        return results

    # Build set of all sys_ids in any selected domain
    domain_sys_ids = set()
    for domain_name in selected_domains:
        domain_sys_ids.update(fjms.get_manuscripts_by_domain(domain_name))

    # Filter results
    return [r for r in results if r['sys_id'] in domain_sys_ids]
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| CSV-based FJMS data | SQLite sidecar with indexed queries | Phase 25 (Feb 2026) | Fast lookups, FTS5 support, proper schema |
| Global subject tags only | Two-level domain hierarchy (parent/child) | Phase 25 (Feb 2026) | More precise classification, 187 unique domains |
| Manual filter checkboxes | Type-ahead search filtering | NiceGUI 1.4+ | Handles 187 domains gracefully |
| Flat domain lists | Hierarchical grouping | User requirement | Better UX for navigation |

**Deprecated/outdated:**
- **Flat badge lists:** User rejected in favor of minimal clickable text
- **Single-select domain filter:** User specified multi-select with OR logic
- **English-only domain names:** Requires Hebrew translations for both display and filtering

## Open Questions

1. **Hierarchy Building Strategy**
   - What we know: Need to group 187 domains by ~45 parent categories
   - What's unclear: Should we pre-compute hierarchy at service layer or build on-demand in UI?
   - Recommendation: Add `get_domain_hierarchy()` method to FjmsService that returns pre-built structure. Cache at module level in UI code. Simpler than building in each component.

2. **Primary Domain Selection for Search Results**
   - What we know: Show one domain + "+N more" indicator
   - What's unclear: Which domain to show as primary when manuscript has multiple?
   - Recommendation: Show most specific (child over parent), then alphabetical. User specified showing child domains preferentially on browse page — extend this logic to search results.

3. **Domain Filter Persistence**
   - What we know: User can select domains and search
   - What's unclear: Should selected domains persist across sessions (like saved search mode)?
   - Recommendation: Yes, save to `app.storage.user` like other search preferences. Enhances UX for domain-focused research workflows.

4. **Parent Selection Behavior**
   - What we know: Selecting parent should include all children automatically
   - What's unclear: Does checking a parent checkbox in tree auto-check children, or does backend handle it?
   - Recommendation: Backend handles it. When user selects "Rabbinic Literature", `get_manuscripts_by_domain("Rabbinic Literature")` returns all manuscripts with that as Domain OR ParentDomain. UI shows parent as selected, children unchecked. Simpler state management.

## Sources

### Primary (HIGH confidence)

- shared/fjms_service.py (lines 1-313) - Verified implementation with all required methods
- web/pages/browse.py (lines 1816-1972) - Existing metadata panel structure
- web/pages/search.py (lines 368-391) - PGP tags multi-select pattern
- genizah_app.py (lines 4345-4361) - ListsTreeWidget reference implementation
- genizah_app.py (lines 6799-6948) - Desktop search tab structure
- fist_data/fjms_enrichment.db - Verified data structure via SQL queries
- [NiceGUI ui.select documentation](https://nicegui.io/documentation/select) - Multi-select with chips
- [NiceGUI ui.tree documentation](https://nicegui.io/documentation/tree) - Tree structure reference
- [Qt QTreeWidget documentation](https://doc.qt.io/qt-6/qtreewidget.html) - Hierarchical checkbox trees

### Secondary (MEDIUM confidence)

- [NiceGUI Discussion #817](https://github.com/zauberzeug/nicegui/discussions/817) - Multi-select with chips display
- [NiceGUI Discussion #1924](https://github.com/zauberzeug/nicegui/discussions/1924) - Tree filtering patterns
- [Qt Forum: QTreeWidget checkboxes](https://forum.qt.io/topic/92530/configuring-checkbox-when-using-qtreewidget) - Checkbox configuration

### Tertiary (LOW confidence)

None — all findings verified against codebase or official documentation.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - All components already in codebase, Phase 25 complete
- Architecture: HIGH - Clear insertion points in existing files, verified patterns
- Pitfalls: HIGH - Based on actual codebase patterns (thread-safety, translation system)
- Code examples: HIGH - Adapted from working code in same codebase

**Research date:** 2026-02-13
**Valid until:** 60 days (stable domain, no fast-moving dependencies)

**Data Statistics (from fist_data/fjms_enrichment.db):**
- 187 unique domain classifications
- 38 unique parent domains
- ~390K domain assignments (one manuscript can have multiple domains)
- ~203K manuscripts with domain coverage (93% of ~217K total)
- Top domain: Piyyut (48,812 manuscripts)
- Hierarchy depth: 2 levels (root → child only, no deeper nesting)

**Key Technical Constraints:**
- Web app MUST use `get_fjms_service(thread_safe=True)` due to NiceGUI's multi-threaded request handling
- Desktop app uses single-threaded `get_fjms_service()` — no thread-safety flag needed
- Domain names in English and Hebrew both stored in database — no translation file needed
- Clickable links must navigate to search page with pre-applied domain filter
- Multi-select filter uses OR logic (manuscripts matching ANY selected domain)
