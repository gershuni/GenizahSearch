# Technical Specification: Joins System
## A System for Managing Relationships Between Cairo Genizah Manuscripts

**Date:** January 2026 (updated 2026-03-13)
**Version:** 1.0 — Superseded by JOINS_SIMPLIFIED_SPEC.md v2.0
**Purpose:** This document captures the experience of implementing a Joins system, including goals, methods, problems encountered, and recommendations for future implementation.

---

## Table of Contents

1. [Overview](#overview)
2. [System Goals](#system-goals)
3. [Architecture](#architecture)
4. [Data Model](#data-model)
5. [API Endpoints](#api-endpoints)
6. [User Interface](#user-interface)
7. [Problems Encountered and Pitfalls](#problems-encountered-and-pitfalls)
8. [Recommendations for Future Implementation](#recommendations-for-future-implementation)

---

## Overview

### What is a Join?

A Join is a relationship between two or more manuscripts from the Cairo Genizah. This relationship can be:

- **Physical Join** (`physical_join`): Manuscript fragments that belong to the same original document that was torn apart
- **Same Page** (`same_page`): Different photographs or copies of the same physical page
- **Same Composition** (`same_composition`): Different manuscripts of the same literary work
- **Same Scribe** (`same_scribe`): Manuscripts written by the same scribe

### Why is this Important?

Genizah researchers invest significant effort in identifying relationships between fragments. The Joins system enables:

1. Collaborative documentation of these identifications
2. Easy navigation between related fragments
3. Knowledge sharing between researchers
4. Gradual building of a verified relationships database

---

## System Goals

### Core Goals

1. **Document Joins**: Allow users to specify relationships between manuscripts
2. **Quick Navigation**: Easy transition between related manuscripts
3. **Metadata**: Add notes, academic sources, and confidence levels
4. **Collaboration**: Any registered user can add joins

### Secondary Goals

5. **Statistics**: Track activity and join statistics
6. **Search**: Search joins by shelfmark or text
7. **Verification**: Confidence levels to mark joins requiring review

---

## Architecture

### Main Components

```
┌─────────────────────────────────────────────────────────────┐
│                    Frontend (Desktop/Web)                    │
├─────────────────────────────────────────────────────────────┤
│  Desktop (PyQt6)          │        Web (NiceGUI)            │
│  - genizah_app.py         │        - joins_panel.py         │
│  - joins_ui.py            │                                 │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    API Layer (FastAPI)                       │
│                  backend/api/routes/joins.py                 │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Service Layer                              │
│                backend/services/join_service.py              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Data Layer (SQLAlchemy)                    │
│  - backend/models/join.py (JoinGroup, JoinGroupMember)       │
│  - backend/schemas/join.py (Pydantic schemas)                │
└─────────────────────────────────────────────────────────────┘
```

### Main Files

| File | Purpose |
|------|---------|
| `backend/models/join.py` | SQLAlchemy models for DB tables |
| `backend/schemas/join.py` | Pydantic schemas for validation |
| `backend/services/join_service.py` | Business logic |
| `backend/api/routes/joins.py` | API endpoints |
| `corrections_client.py` | Python client for API |
| `genizah_app.py` | Desktop application integration |
| `joins_ui.py` | Dedicated Desktop UI components |
| `web/components/joins_panel.py` | Web UI components |

---

## Data Model

### Tables

#### `join_groups` - Join Groups

```sql
CREATE TABLE join_groups (
    id VARCHAR(36) PRIMARY KEY,           -- UUID
    relationship_type ENUM(...) NOT NULL, -- Relationship type
    title VARCHAR(500),                   -- Optional title
    notes TEXT,                           -- Notes
    source_reference TEXT,                -- Academic source
    confidence ENUM(...) DEFAULT 'confirmed',
    created_by INTEGER REFERENCES users(id),
    created_at DATETIME,
    updated_at DATETIME,
    is_active BOOLEAN DEFAULT TRUE        -- soft delete
);
```

#### `join_group_members` - Group Members

```sql
CREATE TABLE join_group_members (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id VARCHAR(36) REFERENCES join_groups(id),
    shelfmark VARCHAR(200) NOT NULL,      -- Primary identifier!
    document_id VARCHAR(100),             -- Index identifier (optional)
    sequence_order INTEGER,               -- Order in group
    member_notes TEXT,
    added_by INTEGER REFERENCES users(id),
    added_at DATETIME
);
```

### Relationships

```
JoinGroup 1 ──────< JoinGroupMember
     │
     └── members: List[JoinGroupMember] (one-to-many)
```

### Enums

```python
class RelationshipType(str, Enum):
    PHYSICAL_JOIN = "physical_join"       # Physical join
    SAME_PAGE = "same_page"               # Same page
    SAME_COMPOSITION = "same_composition" # Same composition
    SAME_SCRIBE = "same_scribe"           # Same scribe

class ConfidenceLevel(str, Enum):
    CONFIRMED = "confirmed"       # Confirmed
    PROBABLE = "probable"         # Probable
    SUGGESTED = "suggested"       # Suggested
    UNCERTAIN = "uncertain"       # Uncertain
    ALGORITHMIC = "algorithmic"   # Automatically identified
```

---

## API Endpoints

### Main Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/joins/groups` | Create new join group |
| `GET` | `/joins/groups/{id}` | Get group by ID |
| `PUT` | `/joins/groups/{id}` | Update metadata |
| `DELETE` | `/joins/groups/{id}` | Delete group |
| `GET` | `/joins/groups` | List groups (with filters) |
| `POST` | `/joins/groups/{id}/members` | Add member to group |
| `DELETE` | `/joins/groups/{id}/members/{mid}` | Remove member |
| `GET` | `/joins/document/{shelfmark}` | Get all joins for document |
| `GET` | `/joins/search` | Search joins |
| `GET` | `/joins/stats` | Statistics |

### Example: Create Join

```json
POST /joins/groups
{
    "relationship_type": "physical_join",
    "members": [
        {"shelfmark": "T-S 8J6.1", "document_id": "1234"},
        {"shelfmark": "T-S 8J6.2", "document_id": "1235"}
    ],
    "confidence": "confirmed",
    "title": "Two parts of a letter",
    "source_reference": "Goitein, Mediterranean Society, vol. 1"
}
```

### Example: Get Document Joins

```json
GET /joins/document/T-S%208J6.1

Response:
{
    "shelfmark": "T-S 8J6.1",
    "total_joins": 2,
    "physical_joins": [
        {
            "id": "abc-123",
            "shelfmarks": ["T-S 8J6.1", "T-S 8J6.2"],
            "confidence": "confirmed"
        }
    ],
    "same_page": [],
    "same_composition": [...],
    "same_scribe": []
}
```

---

## User Interface

### Desktop (PyQt6)

#### Joins Button in Browse Page

- Location: In the toolbar of the Browse page
- States:
  - Gray: No joins or server unavailable
  - Green + number: There are N joins
- Click opens joins dialog
- Dropdown menu for quick navigation to related manuscript

#### Joins Dialog

- List of join cards
- Each card displays: type, members, confidence level, source
- Option to add new join
- Navigate to another manuscript by clicking

### Web (NiceGUI)

#### Joins Panel

- Collapsible panel on document view page
- Display similar to Desktop
- Add join form with:
  - Shelfmark autocomplete
  - Selection from personal lists
  - Confidence level

---

## Problems Encountered and Pitfalls

### Problem 1: Document Identification by Shelfmark Only

**Description:** The system uses `shelfmark` as the primary identifier for group members.

**Issues:**
- Shelfmarks can vary in format (spaces, punctuation)
- Same document can appear with different shelfmarks
- Exact match search fails if format differs

**Example:**
```
"T-S 8J6.1" ≠ "T-S 8J 6.1" ≠ "TS 8J6.1"
```

**Recommendation:** Use `document_id` (sys_id) as primary identifier, store shelfmark only for display.

### Problem 2: Server Availability Check with Cache

**Description:** The `is_server_available()` function uses a 30-second cache.

**Issue:** If the server was unavailable on first check, the joins button stays grayed out for 30 seconds even after the server returns.

**Solution Implemented:** Remove the preliminary check and rely on the actual request failure.

### Problem 3: Empty Shelfmarks in Data

**Description:** Group members with empty shelfmarks caused incorrect display.

**Solution Implemented:** Filter empty shelfmarks everywhere:
- Backend: `[m.shelfmark for m in group.members if m.shelfmark and m.shelfmark.strip()]`
- Frontend: Similar filter in display

### Problem 4: Non-blocking API Calls

**Description:** API calls to server blocked the UI in Desktop.

**Solution Implemented:**
```python
import threading
from PyQt6.QtCore import QTimer

def fetch_joins():
    # ... blocking API call
    return data

def on_result(data):
    # Update UI on main thread
    pass

def run_in_background():
    result = fetch_joins()
    QTimer.singleShot(0, lambda: on_result(result))

thread = threading.Thread(target=run_in_background, daemon=True)
thread.start()
```

### Problem 5: Enum Serialization

**Description:** `confidence` returned from API can be `str` or `Enum`.

**Issue:** Code assuming `confidence.value` fails when confidence is already a string.

**Solution:**
```python
if hasattr(confidence, 'value'):
    confidence_str = confidence.value
else:
    confidence_str = str(confidence)
```

### Problem 6: NiceGUI Dialog Flow

**Description:** Closing dialog and opening new dialog didn't work well with `ui.timer`.

**Solution:** Direct function call after `dialog.close()` instead of using timer.

### Problem 7: SQLAlchemy N+1 Queries

**Description:** Loading groups with `joinedload` caused duplicates.

**Solution:** Use `selectinload` instead:
```python
db.query(JoinGroup).options(
    selectinload(JoinGroup.members)
).filter(JoinGroup.id.in_(group_ids))
```

---

## Recommendations for Future Implementation

### 1. Primary Identifier: sys_id Instead of Shelfmark

**Proposal:** Change the data model so `document_id` (sys_id) is the primary identifier.

```python
class JoinGroupMember:
    document_id = Column(String(100), nullable=False, index=True)  # PRIMARY
    shelfmark = Column(String(200), nullable=True)  # DISPLAY ONLY
```

**Advantages:**
- sys_id is a unique and stable identifier
- Shelfmark used only for display
- Avoids format issues

### 2. Shelfmark Normalization

If still using shelfmark, implement normalization:

```python
def normalize_shelfmark(shelfmark: str) -> str:
    """Normalize shelfmark for comparison."""
    # Remove extra spaces
    s = ' '.join(shelfmark.split())
    # Standardize separators
    s = s.replace('–', '-').replace('—', '-')
    # ... more normalization
    return s
```

### 3. Fuzzy Search

Instead of exact match, use fuzzy search:

```python
# Search by similar shelfmark, not identical
member_query = db.query(JoinGroupMember).filter(
    func.similarity(JoinGroupMember.shelfmark, shelfmark) > 0.8
)
```

### 4. Smarter Cache

Manage cache at the join level:
- Store joins for each document in local storage
- Sync in background
- Don't rely on global server availability check

### 5. Import Existing Joins

Build import tools from existing sources:
- CUL joins database
- FGP databases
- Academic lists

### 6. Automatic Identification

Add automatic identification capabilities:
- Handwriting comparison (same scribe)
- Content comparison (same composition)
- Integration with AI tools

### 7. Improved UI

- Drag & drop to add documents to join
- Graph view of joins network
- Export to academic formats

---

## Summary

The Joins system is important infrastructure for Genizah researchers. The current implementation provides a working foundation but suffers from several fundamental issues, mainly:

1. **Reliance on shelfmark** - Creates matching problems
2. **Inconsistency between platforms** - Desktop and Web with different logic
3. **Complex state management** - Multiple places need updating

For successful future implementation, it's recommended to:
- Switch to sys_id as primary identifier
- Build a unified abstraction layer
- Simplify the UI to a simple "add document to join" process

---

## Appendix: Example Code

### Backend Code: Create Group

```python
@staticmethod
def create_group(
    db: Session,
    data: JoinGroupCreate,
    user: Optional[User] = None
) -> Tuple[Optional[JoinGroup], Optional[str]]:
    """Create a new join group with members."""
    if len(data.members) < 2:
        return None, "Join group requires at least 2 members"

    # Check for duplicate shelfmarks
    shelfmarks = [m.shelfmark for m in data.members]
    if len(shelfmarks) != len(set(shelfmarks)):
        return None, "Duplicate shelfmarks in join group"

    group = JoinGroup(
        relationship_type=data.relationship_type,
        title=data.title,
        confidence=data.confidence,
        created_by=user.id if user else None
    )

    db.add(group)
    db.flush()

    for i, member_data in enumerate(data.members):
        member = JoinGroupMember(
            group_id=group.id,
            shelfmark=member_data.shelfmark,
            document_id=member_data.document_id,
            sequence_order=i,
            added_by=user.id if user else None
        )
        db.add(member)

    db.commit()
    return group, None
```

### Desktop Code: Non-blocking Loading

```python
def _update_joins_button(self, shelfmark: str, document_id: str = None):
    """Update joins button indicator (non-blocking)."""
    # Set loading state
    self.btn_joins.setEnabled(False)
    self.btn_joins.setToolTip(tr("Loading..."))

    def fetch_joins():
        try:
            data = self.corrections_client.get_joins_for_document(
                shelfmark, document_id
            )
            return data
        except Exception as e:
            return {'error': str(e)}

    def on_result(data):
        if 'error' in data:
            self.btn_joins.setEnabled(False)
            return

        total = data.get('total_joins', 0)
        self.btn_joins.setEnabled(True)
        self.btn_joins.setText(f"🔗{total}" if total > 0 else "🔗")

    import threading
    def run_in_background():
        result = fetch_joins()
        QTimer.singleShot(0, lambda: on_result(result))

    thread = threading.Thread(target=run_in_background, daemon=True)
    thread.start()
```

### Web Code: Join Card Creation

```python
def create_join_card(join: dict, current_shelfmark: str,
                     on_navigate: Callable = None, refresh_callback: Callable = None):
    """Create a visual card for a join group."""
    confidence = join.get('confidence', 'confirmed')
    title = join.get('title')
    shelfmarks = join.get('shelfmarks', [])

    # Filter out empty shelfmarks
    shelfmarks = [sm for sm in shelfmarks if sm and sm.strip()]

    conf_info = CONFIDENCE_LEVELS.get(confidence, CONFIDENCE_LEVELS['confirmed'])

    with ui.card().classes('w-full p-3'):
        # Header with icon and title
        with ui.row().classes('items-center gap-2'):
            ui.icon('link').classes(f'text-{conf_info["color"]}-500')
            if title:
                ui.label(title).classes('font-medium text-sm')
            else:
                ui.label(f"{len(shelfmarks)} manuscripts").classes('text-sm')

        # Member shelfmarks as clickable chips
        with ui.row().classes('flex-wrap gap-1 mt-2'):
            for sm in shelfmarks:
                is_current = sm == current_shelfmark

                def make_navigate(target=sm):
                    def navigate():
                        if on_navigate:
                            on_navigate(target)
                    return navigate

                chip = ui.chip(
                    sm,
                    icon='description',
                    on_click=None if is_current else make_navigate()
                )
                if is_current:
                    chip.props('color=green')
```

---

*This document was written based on implementation experience in branch `claude/add-manuscript-joins-5Lxpy`*
