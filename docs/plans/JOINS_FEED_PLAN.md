# Joins in Discovery Center - Implementation Plan

> ⚠️ **NOTE (2026-02-03):** This plan was written before the FastAPI backend was removed.
> References to `backend/schemas/discovery.py` and `backend/services/discovery_service.py`
> are outdated. The implementation should use Supabase functions instead of the old backend.

## Overview

Add joins as a new category in the community feed system, showing user-created joins alongside discoveries, corrections, and comments.

---

## Requirements Summary

### Display Information
- Fragment A and Fragment B shelfmarks
- Relationship type (Physical join / Same composition)
- Creator username and creation date

### Desktop Layout
- Add 4th panel "Joins" (צירופים) in Community tab
- Separate from Discoveries, Corrections, Comments panels

### Statistics (Highlights)
- **Discovery Center**: Total joins by users (user-created only)
- **Homepage** (future): Total joins including imported (FGP, catalogs)

### Actions
- Navigate to Fragment A or Fragment B
- View full cluster (open joins dialog)
- Delete join (admin only)
- Future: Link to joined fragments entity (for transcriptions)

---

## Implementation Plan

### Phase 1: Backend Changes

#### 1.1 Update Feed Endpoint Schema
**File**: `backend/schemas/discovery.py`

```python
# Add to item_type pattern
item_type: str  # "discovery", "question", "correction", "comment", "join"

# Add join-specific fields to FeedItem
fragment_a: Optional[str] = None
fragment_b: Optional[str] = None
document_id_a: Optional[str] = None
document_id_b: Optional[str] = None
relationship_type: Optional[str] = None  # "physical_join" or "same_composition"
join_source: Optional[str] = None  # "user", "FGP", "catalog"
```

#### 1.2 Update Feed Service
**File**: `backend/services/discovery_service.py`

In `get_feed()` method, add joins query section:

```python
# D. Joins (user-created only for feed, filter by source='user')
if item_type in (None, "all", "join"):
    joins_query = db.query(FragmentJoin).filter(
        FragmentJoin.source == "user"  # Only user-created joins in feed
    )

    if period_filter:
        joins_query = joins_query.filter(FragmentJoin.created_at >= period_filter)

    for join in joins_query.all():
        feed_items.append(FeedItem(
            id=f"join_{join.id}",
            item_type="join",
            title=f"{join.fragment_a} ↔ {join.fragment_b}",
            content_preview=get_relationship_display(join.relationship_type),
            author=AuthorInfo(username=join.created_by.username, ...),
            document_id=join.document_id_a,
            shelfmark=join.fragment_a,
            created_at=join.created_at,
            fragment_a=join.fragment_a,
            fragment_b=join.fragment_b,
            document_id_a=join.document_id_a,
            document_id_b=join.document_id_b,
            relationship_type=join.relationship_type,
            join_source=join.source
        ))
```

#### 1.3 Update Stats Endpoint
**File**: `backend/services/discovery_service.py`

Add to `get_stats_summary()`:

```python
# Count user-created joins only
user_joins_count = db.query(FragmentJoin).filter(
    FragmentJoin.source == "user"
).count()

# Add to response
stats["user_joins"] = user_joins_count
```

---

### Phase 2: Client Changes

#### 2.1 Update FeedItem Dataclass
**File**: `corrections_client.py`

```python
@dataclass
class FeedItem:
    # ... existing fields ...

    # Join-specific fields
    fragment_a: Optional[str] = None
    fragment_b: Optional[str] = None
    document_id_a: Optional[str] = None
    document_id_b: Optional[str] = None
    relationship_type: Optional[str] = None
    join_source: Optional[str] = None
```

#### 2.2 Update Feed Item Parser
**File**: `corrections_client.py`

In `_parse_feed_item()`, add:

```python
fragment_a=data.get('fragment_a'),
fragment_b=data.get('fragment_b'),
document_id_a=data.get('document_id_a'),
document_id_b=data.get('document_id_b'),
relationship_type=data.get('relationship_type'),
join_source=data.get('join_source'),
```

---

### Phase 3: Web UI Changes

#### 3.1 Update Type Filter Dropdown
**File**: `web/pages/discoveries.py`

Add to type filter options:

```python
ui.select(
    options={
        'all': tr('All'),
        'discovery': tr('Discoveries'),
        'question': tr('Questions'),
        'correction': tr('Corrections'),
        'comment': tr('Comments'),
        'join': tr('Joins')  # NEW
    },
    ...
)
```

#### 3.2 Add Join Card Rendering
**File**: `web/pages/discoveries.py`

In the card rendering section, add join-specific display:

```python
if item['item_type'] == 'join':
    # Icon: link chain
    icon = 'link'
    icon_color = 'green'

    with ui.row().classes('gap-2 items-center'):
        # Fragment A link
        ui.link(item['fragment_a'], target=f'/browse?q={item["fragment_a"]}')
        ui.icon('sync_alt').classes('text-gray-500')
        # Fragment B link
        ui.link(item['fragment_b'], target=f'/browse?q={item["fragment_b"]}')

    # Relationship badge
    rel_display = {
        'physical_join': tr('Physical join'),
        'same_composition': tr('Same composition')
    }.get(item.get('relationship_type'), '')
    if rel_display:
        ui.badge(rel_display).classes('bg-green-100 text-green-800')

    # View cluster button
    ui.button(tr('View Cluster'), on_click=lambda: open_joins_dialog(...))
```

#### 3.3 Update Highlights Section
**File**: `web/pages/discoveries.py`

Add joins stat card:

```python
with ui.card().classes('p-4 text-center'):
    ui.label(str(stats.get('user_joins', 0))).classes('text-2xl font-bold text-green-600')
    ui.label(tr('Joins')).classes('text-sm text-gray-500')
```

---

### Phase 4: Desktop UI Changes

#### 4.1 Add Joins Panel to Community Tab
**File**: `genizah_app.py`

In `_setup_community_tab()`, add 4th panel:

```python
# Create 4-panel splitter instead of 3
community_splitter = QSplitter(Qt.Orientation.Horizontal)

# Panel 1: Discoveries (existing)
# Panel 2: Corrections (existing)
# Panel 3: Comments (existing)

# Panel 4: Joins (NEW)
joins_panel = QWidget()
joins_layout = QVBoxLayout(joins_panel)

# Header
joins_header = QHBoxLayout()
joins_header.addWidget(QLabel(f"<b>{tr('Joins')}</b>"))
joins_header.addStretch()
btn_refresh_joins = QPushButton("↻")
btn_refresh_joins.setFixedSize(28, 28)
btn_refresh_joins.clicked.connect(lambda: self._refresh_joins_panel())
joins_header.addWidget(btn_refresh_joins)
joins_layout.addLayout(joins_header)

# Joins list
self.joins_list = QListWidget()
self.joins_list.itemDoubleClicked.connect(self._on_community_join_double_click)
joins_layout.addWidget(self.joins_list)

# View all button
btn_view_all_joins = QPushButton(tr("View All Joins..."))
btn_view_all_joins.clicked.connect(self._view_all_joins)
joins_layout.addWidget(btn_view_all_joins)

community_splitter.addWidget(joins_panel)
```

#### 4.2 Add Joins Panel Refresh Method
**File**: `genizah_app.py`

```python
def _refresh_joins_panel(self, use_cache_first=True, skip_api_calls=False):
    """Refresh the joins panel with recent user-created joins."""
    self.joins_list.clear()

    if skip_api_calls:
        # Use cached data only
        cached = self.corrections_client.get_cached_data('joins')
        if cached:
            self._populate_joins_list(cached)
        return

    # Fetch recent user joins from feed
    try:
        items = self.corrections_client.get_feed_items(
            item_type='join',
            limit=20
        )
        joins = [item for item in items if item.item_type == 'join']
        self._populate_joins_list(joins)
        # Cache for offline use
        self.corrections_client.set_cached_data('joins', joins)
    except Exception as e:
        print(f"[ERROR] Failed to refresh joins panel: {e}")

def _populate_joins_list(self, joins):
    """Populate the joins list widget."""
    for join in joins:
        # Format: "T-S 8.99 ↔ ENA 1055.10 (Physical join)"
        rel = {
            'physical_join': tr('Physical join'),
            'same_composition': tr('Same composition')
        }.get(join.relationship_type, '')

        text = f"{join.fragment_a} ↔ {join.fragment_b}"
        if rel:
            text += f" ({rel})"

        item = QListWidgetItem(text)
        item.setData(Qt.ItemDataRole.UserRole, {
            'fragment_a': join.fragment_a,
            'fragment_b': join.fragment_b,
            'document_id_a': join.document_id_a,
            'document_id_b': join.document_id_b,
            'join_id': join.id.replace('join_', '')
        })
        # Green color for joins
        item.setForeground(QColor('#27ae60'))
        self.joins_list.addItem(item)

def _on_community_join_double_click(self, item):
    """Handle double-click on join in community panel."""
    data = item.data(Qt.ItemDataRole.UserRole)
    if not data:
        return

    # Open joins dialog for first fragment
    dialog = JoinsDialog(
        self,
        self.corrections_client,
        document_id=data.get('document_id_a'),
        shelfmark=data.get('fragment_a'),
        on_browse=self._browse_shelfmark,
        joins_mgr=self.joins_mgr,
        meta_mgr=self.meta_mgr,
        lists_mgr=self.lists_mgr
    )
    dialog.exec()
```

#### 4.3 Update Community Panels Refresh
**File**: `genizah_app.py`

In `_refresh_community_panels()`, add:

```python
try:
    self._refresh_joins_panel(use_cache_first, skip_api_calls=skip_api_calls)
except Exception as e:
    print(f"Error refreshing joins panel: {e}")
```

---

### Phase 5: Translations

#### 5.1 Web Translations
**File**: `web/translations.py`

```python
"Joins": "צירופים",
"View Cluster": "צפה בקבוצה",
"User Joins": "צירופי משתמשים",
```

#### 5.2 Desktop Translations
**File**: `genizah_translations.py`

```python
"View All Joins...": "צפה בכל הצירופים...",
```

---

## File Changes Summary

| File | Changes |
|------|---------|
| `backend/schemas/discovery.py` | Add join fields to FeedItem schema |
| `backend/services/discovery_service.py` | Add joins query to get_feed(), add to stats |
| `corrections_client.py` | Extend FeedItem, update parser |
| `web/pages/discoveries.py` | Add join filter, card rendering, stats |
| `web/translations.py` | Add join-related translations |
| `genizah_app.py` | Add 4th panel, refresh method, handlers |
| `genizah_translations.py` | Add join-related translations |

---

## Future Considerations

1. **Joined Fragments Entity**: When ready, joins in feed should link to the unified transcription view
2. **Import Source Display**: Could show FGP/catalog joins separately with different styling
3. **Join Suggestions**: AI-powered suggestions for potential joins
4. **Join Verification**: Community voting on join accuracy

---

## Testing Checklist

- [ ] Backend: Feed endpoint returns joins with correct structure
- [ ] Backend: Stats include user_joins count
- [ ] Web: Join filter works in discovery center
- [ ] Web: Join cards display correctly with navigation
- [ ] Web: Highlights show user joins count
- [ ] Desktop: 4th panel displays joins
- [ ] Desktop: Double-click opens joins dialog
- [ ] Desktop: Refresh works online and offline
- [ ] Admin: Can delete joins from feed
