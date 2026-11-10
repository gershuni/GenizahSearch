# Simplified Joins System
## Pairwise Fragment Joins with Connected Components

**Date:** January 2026 (updated 2026-03-13)
**Version:** 2.0 (Simplified) — Active/Implemented
**Branch:** `claude/searchable-corrections-sync`
**Supersedes:** JOINS_TECHNICAL_SPEC.md (v1.0 - never implemented)

---

## Overview

A simplified system for joining related Genizah fragments.

**Key simplifications:**
1. **Pairwise joins instead of groups** - Each join connects exactly two fragments
2. **Relationship type is optional** - User doesn't have to decide
3. **Connected components** - If A→B and B→C, viewing any shows all three

---

## Core Concept: Connected Components

Joins form a graph. When viewing any fragment, we show all fragments in its **connected component**.

```
Example:
  A ── B ── C
       │
       D

Joins stored: A-B, B-C, B-D

When viewing A: Shows A, B, C, D (all connected)
When viewing B: Shows A, B, C, D (all connected)
When viewing C: Shows A, B, C, D (all connected)
When viewing D: Shows A, B, C, D (all connected)

Separate cluster:
  X ── Y

When viewing X: Shows X, Y only
When viewing Y: Shows X, Y only
```

---

## Data Model

### Table: `fragment_joins`

```sql
CREATE TABLE fragment_joins (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- The two joined fragments (stored alphabetically for deduplication)
    fragment_a VARCHAR(200) NOT NULL,          -- shelfmark (normalized)
    fragment_b VARCHAR(200) NOT NULL,          -- shelfmark (normalized)

    -- Optional: sys_ids for faster lookups
    document_id_a VARCHAR(100),                -- sys_id if known
    document_id_b VARCHAR(100),                -- sys_id if known

    -- Relationship (optional - user may not know)
    relationship_type VARCHAR(50),             -- NULL, 'physical_join', 'same_composition'

    -- Metadata
    notes TEXT,
    source VARCHAR(50) DEFAULT 'user',         -- 'user', 'princeton', 'cambridge', etc.
    source_url TEXT,                           -- for imports: original URL

    -- Tracking
    created_by INTEGER REFERENCES users(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,

    -- Soft delete
    is_active BOOLEAN DEFAULT TRUE,

    -- Prevent duplicates (A-B is same as B-A)
    UNIQUE(fragment_a, fragment_b)
);

CREATE INDEX idx_joins_fragment_a ON fragment_joins(fragment_a);
CREATE INDEX idx_joins_fragment_b ON fragment_joins(fragment_b);
CREATE INDEX idx_joins_source ON fragment_joins(source);
```

### Relationship Types (Optional)

```python
class RelationshipType(str, Enum):
    PHYSICAL_JOIN = "physical_join"       # Same original document, torn apart
    SAME_COMPOSITION = "same_composition" # Different MSS of same text
    # NULL = not sure / unspecified
```

### Normalization Rules

**1. Shelfmark normalization:**
```python
def normalize_shelfmark(shelfmark: str) -> str:
    s = shelfmark.strip().upper()
    s = re.sub(r'^TS[\s\-]*', 'T-S ', s)
    s = re.sub(r'\s+', ' ', s)
    return s.strip()
```

**2. Join ordering (prevent A-B and B-A duplicates):**
```python
def normalize_join_order(frag_a: str, frag_b: str) -> tuple[str, str]:
    a_norm = normalize_shelfmark(frag_a)
    b_norm = normalize_shelfmark(frag_b)
    return (a_norm, b_norm) if a_norm <= b_norm else (b_norm, a_norm)
```

---

## Service Layer

### JoinService

```python
class JoinService:

    @staticmethod
    def create_join(
        db: Session,
        fragment_a: str,
        fragment_b: str,
        relationship_type: Optional[str] = None,
        notes: Optional[str] = None,
        source: str = "user",
        source_url: Optional[str] = None,
        user: Optional[User] = None
    ) -> Tuple[Optional[FragmentJoin], Optional[str]]:
        """Create a join between two fragments."""

        # Normalize and order
        frag_a, frag_b = normalize_join_order(fragment_a, fragment_b)

        # Check if same fragment
        if frag_a == frag_b:
            return None, "Cannot join a fragment to itself"

        # Check if already exists
        existing = db.query(FragmentJoin).filter(
            FragmentJoin.fragment_a == frag_a,
            FragmentJoin.fragment_b == frag_b,
            FragmentJoin.is_active == True
        ).first()

        if existing:
            return None, f"Join already exists (id: {existing.id})"

        join = FragmentJoin(
            fragment_a=frag_a,
            fragment_b=frag_b,
            relationship_type=relationship_type,
            notes=notes,
            source=source,
            source_url=source_url,
            created_by=user.id if user else None
        )

        db.add(join)
        db.commit()
        db.refresh(join)

        return join, None

    @staticmethod
    def get_connected_fragments(db: Session, shelfmark: str) -> dict:
        """
        Get all fragments in the same connected component.
        Returns the full cluster with relationship info.
        """
        normalized = normalize_shelfmark(shelfmark)

        # BFS to find all connected fragments
        visited = set()
        to_visit = [normalized]
        joins_found = []

        while to_visit:
            current = to_visit.pop(0)
            if current in visited:
                continue
            visited.add(current)

            # Get all joins involving current fragment
            direct_joins = db.query(FragmentJoin).filter(
                FragmentJoin.is_active == True,
                or_(
                    FragmentJoin.fragment_a == current,
                    FragmentJoin.fragment_b == current
                )
            ).all()

            for join in direct_joins:
                joins_found.append(join)
                other = join.fragment_b if join.fragment_a == current else join.fragment_a
                if other not in visited:
                    to_visit.append(other)

        # Deduplicate joins
        unique_joins = {join.id: join for join in joins_found}.values()

        return {
            "shelfmark": shelfmark,
            "shelfmark_normalized": normalized,
            "fragments": sorted(list(visited)),
            "joins": list(unique_joins),
            "total_fragments": len(visited),
            "total_joins": len(unique_joins)
        }

    @staticmethod
    def delete_join(db: Session, join_id: int, user: Optional[User] = None) -> bool:
        """Soft delete a join."""
        join = db.query(FragmentJoin).filter(FragmentJoin.id == join_id).first()
        if not join:
            return False

        join.is_active = False
        join.updated_at = datetime.utcnow()
        db.commit()
        return True

    @staticmethod
    def update_join(
        db: Session,
        join_id: int,
        relationship_type: Optional[str] = None,
        notes: Optional[str] = None
    ) -> Optional[FragmentJoin]:
        """Update join metadata."""
        join = db.query(FragmentJoin).filter(
            FragmentJoin.id == join_id,
            FragmentJoin.is_active == True
        ).first()

        if not join:
            return None

        if relationship_type is not None:
            join.relationship_type = relationship_type
        if notes is not None:
            join.notes = notes
        join.updated_at = datetime.utcnow()

        db.commit()
        db.refresh(join)
        return join
```

---

## API Endpoints

### Create Join

```
POST /api/v1/joins

Request:
{
    "fragment_a": "T-S 13J35.3",
    "fragment_b": "AIU VII.A.23",
    "relationship_type": "physical_join",  // optional, can be null
    "notes": "Identified by Gil"           // optional
}

Response (201):
{
    "id": 123,
    "fragment_a": "AIU VII.A.23",          // normalized & sorted
    "fragment_b": "T-S 13J35.3",
    "relationship_type": "physical_join",
    "source": "user",
    "created_by": {"id": 5, "username": "researcher1"},
    "created_at": "2026-01-19T12:00:00Z"
}

Error (409):
{
    "detail": "Join already exists (id: 45)"
}
```

### Get Connected Fragments

```
GET /api/v1/joins/connected/{shelfmark}

Example: GET /api/v1/joins/connected/T-S%2013J35.3

Response:
{
    "shelfmark": "T-S 13J35.3",
    "fragments": [
        "AIU VII.A.23",
        "T-S 13J35.3",
        "T-S 13J35.4"
    ],
    "joins": [
        {
            "id": 123,
            "fragment_a": "AIU VII.A.23",
            "fragment_b": "T-S 13J35.3",
            "relationship_type": "physical_join",
            "notes": "Identified by Gil",
            "source": "user",
            "created_by": {"username": "researcher1"}
        },
        {
            "id": 124,
            "fragment_a": "T-S 13J35.3",
            "fragment_b": "T-S 13J35.4",
            "relationship_type": null,
            "notes": null,
            "source": "princeton",
            "created_by": null
        }
    ],
    "total_fragments": 3,
    "total_joins": 2
}
```

### Delete Join

```
DELETE /api/v1/joins/{id}

Response (200):
{ "success": true }

Error (404):
{ "detail": "Join not found" }
```

### Update Join

```
PATCH /api/v1/joins/{id}

Request:
{
    "relationship_type": "same_composition",
    "notes": "Updated note"
}

Response (200):
{
    "id": 123,
    "fragment_a": "AIU VII.A.23",
    "fragment_b": "T-S 13J35.3",
    "relationship_type": "same_composition",
    "notes": "Updated note",
    ...
}
```

### Search Joins

```
GET /api/v1/joins?q=13J35&source=princeton

Response:
{
    "results": [
        {
            "id": 124,
            "fragment_a": "T-S 13J35.3",
            "fragment_b": "T-S 13J35.4",
            "relationship_type": null,
            "source": "princeton"
        }
    ],
    "total": 1
}
```

---

## User Interface

### Design Principles

1. **Show the full cluster** - All connected fragments, not just direct joins
2. **Minimal decisions** - Relationship type is optional, default to "not sure"
3. **One simple dialog** - No multi-step wizards
4. **Quick navigation** - Click any fragment to go there

### Desktop UI (PyQt6)

#### Joins Button in Document View

```
┌─────────────────────────────────────────────────────────────┐
│ T-S 13J35.3                                                 │
│ Cambridge University Library                                │
│                                                             │
│ [📷 Images] [📝 Edit] [🔗 3] [⭐ Star]                       │
│                        ↑                                    │
│              Shows count of joined fragments                │
└─────────────────────────────────────────────────────────────┘
```

- Number shows total fragments in cluster (including current)
- Gray `[🔗]` if no joins, colored `[🔗 3]` if has joins

#### Joins Panel (Click Button → Opens Panel)

```
┌─────────────────────────────────────────┐
│ Joined Fragments                    [×] │
├─────────────────────────────────────────┤
│                                         │
│ This fragment is part of a group of 3:  │
│                                         │
│ ┌─────────────────────────────────────┐ │
│ │ 📄 AIU VII.A.23                    →│ │
│ │    physical join                    │ │
│ └─────────────────────────────────────┘ │
│                                         │
│ ┌─────────────────────────────────────┐ │
│ │ 📄 T-S 13J35.3 (current)            │ │
│ │    ↳ joined via AIU VII.A.23        │ │
│ └─────────────────────────────────────┘ │
│                                         │
│ ┌─────────────────────────────────────┐ │
│ │ 📄 T-S 13J35.4                     →│ │
│ │    (relationship unknown)           │ │
│ └─────────────────────────────────────┘ │
│                                         │
│ [+ Join Another Fragment]               │
└─────────────────────────────────────────┘
```

- Current fragment highlighted
- Click any other fragment → Navigate to it
- Shows relationship type if known

#### Add Join Dialog

```
┌─────────────────────────────────────────────────────────────┐
│ Join Fragment                                           [×] │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│ Join T-S 13J35.3 to another fragment:                       │
│                                                             │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ Enter shelfmark...                              [🔍]    │ │
│ └─────────────────────────────────────────────────────────┘ │
│   Autocomplete suggestions appear as you type               │
│                                                             │
│ Relationship (optional):                                    │
│   ○ Physical join - fragments of same original document     │
│   ○ Same composition - different copies of same text        │
│   ● Not sure / just related                                 │
│                                                             │
│ Notes (optional):                                           │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │                                                         │ │
│ └─────────────────────────────────────────────────────────┘ │
│                                                             │
│                              [Cancel]  [Create Join]        │
└─────────────────────────────────────────────────────────────┘
```

### Web UI (NiceGUI)

Same design, using NiceGUI components:

```python
async def show_joins_panel(shelfmark: str):
    """Show joined fragments panel."""

    # Fetch connected fragments
    data = await api.get(f"/joins/connected/{quote(shelfmark)}")
    fragments = data["fragments"]
    joins = data["joins"]

    with ui.card().classes('w-80'):
        ui.label('Joined Fragments').classes('text-lg font-bold')

        if len(fragments) <= 1:
            ui.label('No joins yet').classes('text-gray-500')
        else:
            ui.label(f'Part of a group of {len(fragments)}:').classes('text-sm text-gray-600')

            for frag in fragments:
                is_current = normalize_shelfmark(frag) == normalize_shelfmark(shelfmark)

                with ui.card().classes('w-full p-2 cursor-pointer hover:bg-gray-100'):
                    with ui.row().classes('items-center'):
                        ui.icon('description')
                        ui.label(frag).classes('font-medium' if is_current else '')
                        if is_current:
                            ui.badge('current').classes('ml-auto')
                        else:
                            ui.icon('arrow_forward').classes('ml-auto')

                    # Show relationship if known
                    rel = get_relationship_for_fragment(frag, joins)
                    if rel:
                        ui.label(rel).classes('text-xs text-gray-500 ml-6')

                if not is_current:
                    ui.on('click', lambda f=frag: navigate_to(f))

        ui.button('+ Join Another', on_click=lambda: show_add_join_dialog(shelfmark))
```

---

## Desktop Client Integration

Add to `corrections_client.py`:

```python
class CorrectionsClient:

    # ... existing methods ...

    def get_connected_fragments(self, shelfmark: str) -> dict:
        """Get all fragments joined to this one."""
        response = self._request(
            "GET",
            f"/joins/connected/{quote(shelfmark)}"
        )
        return response

    def create_join(
        self,
        fragment_a: str,
        fragment_b: str,
        relationship_type: Optional[str] = None,
        notes: Optional[str] = None
    ) -> dict:
        """Create a join between two fragments."""
        response = self._request(
            "POST",
            "/joins",
            json={
                "fragment_a": fragment_a,
                "fragment_b": fragment_b,
                "relationship_type": relationship_type,
                "notes": notes
            }
        )
        return response

    def delete_join(self, join_id: int) -> bool:
        """Delete a join."""
        response = self._request("DELETE", f"/joins/{join_id}")
        return response.get("success", False)
```

---

## Princeton Import Integration

When importing Princeton transcriptions:

```python
def import_princeton_document(
    shelfmarks: list[str],
    transcription_text: str,
    source_url: str,
    source_reference: str
) -> dict:
    """
    Import a Princeton joined document.

    1. Creates pairwise joins between all fragments
    2. Stores the transcription (separate table, covered in searchable corrections spec)
    """

    # Create joins between all pairs
    joins_created = []
    for i, frag_a in enumerate(shelfmarks):
        for frag_b in shelfmarks[i+1:]:
            join, error = JoinService.create_join(
                db=db,
                fragment_a=frag_a,
                fragment_b=frag_b,
                relationship_type="physical_join",
                source="princeton",
                source_url=source_url,
                notes=source_reference
            )
            if join:
                joins_created.append(join)

    # Store transcription (see SEARCHABLE_CORRECTIONS_SPEC.md)
    # ...

    return {
        "shelfmarks": shelfmarks,
        "joins_created": len(joins_created),
        "transcription_stored": True
    }
```

---

## Implementation Plan

### Phase 1: Backend ✅ COMPLETE

- [x] Create migration: `fragment_joins` table
- [x] Create model: `backend/models/fragment_join.py`
- [x] Create schemas: `backend/schemas/join.py`
- [x] Create service: `backend/services/join_service.py`
- [x] Create routes: `backend/api/routes/joins.py`
- [x] Add shelfmark normalization utility
- [x] Add to `corrections_client.py`

### Phase 2: Desktop UI ✅ COMPLETE

- [x] Add "Joins" button to browse toolbar (`genizah_app.py`)
- [x] Create `JoinsDialog` widget (`corrections_ui.py`)
- [x] Create join form in dialog
- [x] API integration via `corrections_client.py`
- [x] Navigation to joined fragments

### Phase 3: Web UI ✅ COMPLETE

- [x] Add joins button to document view (`web/pages/browse.py`)
- [x] Create joins panel component (`web/components/joins_panel.py`)
- [x] Create add join dialog
- [x] Navigation integration
- [x] Add translations (Hebrew/English)

### Phase 4: Testing & Polish

- [ ] Integration testing
- [ ] Edge cases (self-join, duplicates, large clusters)
- [ ] Performance testing (large connected components)
- [ ] UI polish

---

## File Structure

```
backend/
├── models/
│   └── fragment_join.py        # ✅ DONE
├── schemas/
│   └── join.py                 # ✅ DONE
├── services/
│   └── join_service.py         # ✅ DONE
├── api/routes/
│   └── joins.py                # ✅ DONE
└── migrations/
    └── add_fragment_joins.py   # ✅ DONE

desktop/
└── (existing genizah_app.py - add joins UI)

web/
├── components/
│   └── joins_panel.py          # TODO
└── pages/
    └── (existing - add joins integration)
```

---

*Last updated: January 2026*
