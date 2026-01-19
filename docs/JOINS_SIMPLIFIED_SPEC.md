# Simplified Joins System
## Pairwise Fragment Links with Connected Components

**Date:** January 2026
**Version:** 2.0 (Simplified)
**Branch:** `claude/searchable-corrections-sync`
**Supersedes:** JOINS_TECHNICAL_SPEC.md (v1.0 - never implemented)

---

## Overview

A simplified system for linking related Genizah fragments.

**Key simplifications:**
1. **Pairwise links instead of groups** - Each link connects exactly two fragments
2. **Relationship type is optional** - User doesn't have to decide
3. **Connected components** - If A→B and B→C, viewing any shows all three

---

## Core Concept: Connected Components

Links form a graph. When viewing any fragment, we show all fragments in its **connected component**.

```
Example:
  A ── B ── C
       │
       D

Links stored: A-B, B-C, B-D

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

### Table: `fragment_links`

```sql
CREATE TABLE fragment_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- The two linked fragments (stored alphabetically for deduplication)
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

CREATE INDEX idx_links_fragment_a ON fragment_links(fragment_a);
CREATE INDEX idx_links_fragment_b ON fragment_links(fragment_b);
CREATE INDEX idx_links_source ON fragment_links(source);
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

**2. Link ordering (prevent A-B and B-A duplicates):**
```python
def normalize_link(frag_a: str, frag_b: str) -> tuple[str, str]:
    a_norm = normalize_shelfmark(frag_a)
    b_norm = normalize_shelfmark(frag_b)
    return (a_norm, b_norm) if a_norm <= b_norm else (b_norm, a_norm)
```

---

## Service Layer

### LinkService

```python
class LinkService:

    @staticmethod
    def create_link(
        db: Session,
        fragment_a: str,
        fragment_b: str,
        relationship_type: Optional[str] = None,
        notes: Optional[str] = None,
        source: str = "user",
        source_url: Optional[str] = None,
        user: Optional[User] = None
    ) -> Tuple[Optional[FragmentLink], Optional[str]]:
        """Create a link between two fragments."""

        # Normalize and order
        frag_a, frag_b = normalize_link(fragment_a, fragment_b)

        # Check if same fragment
        if frag_a == frag_b:
            return None, "Cannot link a fragment to itself"

        # Check if already exists
        existing = db.query(FragmentLink).filter(
            FragmentLink.fragment_a == frag_a,
            FragmentLink.fragment_b == frag_b,
            FragmentLink.is_active == True
        ).first()

        if existing:
            return None, f"Link already exists (id: {existing.id})"

        link = FragmentLink(
            fragment_a=frag_a,
            fragment_b=frag_b,
            relationship_type=relationship_type,
            notes=notes,
            source=source,
            source_url=source_url,
            created_by=user.id if user else None
        )

        db.add(link)
        db.commit()
        db.refresh(link)

        return link, None

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
        links_found = []

        while to_visit:
            current = to_visit.pop(0)
            if current in visited:
                continue
            visited.add(current)

            # Get all links involving current fragment
            direct_links = db.query(FragmentLink).filter(
                FragmentLink.is_active == True,
                or_(
                    FragmentLink.fragment_a == current,
                    FragmentLink.fragment_b == current
                )
            ).all()

            for link in direct_links:
                links_found.append(link)
                other = link.fragment_b if link.fragment_a == current else link.fragment_a
                if other not in visited:
                    to_visit.append(other)

        # Deduplicate links
        unique_links = {link.id: link for link in links_found}.values()

        return {
            "shelfmark": shelfmark,
            "shelfmark_normalized": normalized,
            "fragments": sorted(list(visited)),
            "links": list(unique_links),
            "total_fragments": len(visited),
            "total_links": len(unique_links)
        }

    @staticmethod
    def delete_link(db: Session, link_id: int, user: Optional[User] = None) -> bool:
        """Soft delete a link."""
        link = db.query(FragmentLink).filter(FragmentLink.id == link_id).first()
        if not link:
            return False

        link.is_active = False
        link.updated_at = datetime.utcnow()
        db.commit()
        return True

    @staticmethod
    def update_link(
        db: Session,
        link_id: int,
        relationship_type: Optional[str] = None,
        notes: Optional[str] = None
    ) -> Optional[FragmentLink]:
        """Update link metadata."""
        link = db.query(FragmentLink).filter(
            FragmentLink.id == link_id,
            FragmentLink.is_active == True
        ).first()

        if not link:
            return None

        if relationship_type is not None:
            link.relationship_type = relationship_type
        if notes is not None:
            link.notes = notes
        link.updated_at = datetime.utcnow()

        db.commit()
        db.refresh(link)
        return link
```

---

## API Endpoints

### Create Link

```
POST /api/v1/links

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
    "detail": "Link already exists (id: 45)"
}
```

### Get Connected Fragments

```
GET /api/v1/links/connected/{shelfmark}

Example: GET /api/v1/links/connected/T-S%2013J35.3

Response:
{
    "shelfmark": "T-S 13J35.3",
    "fragments": [
        "AIU VII.A.23",
        "T-S 13J35.3",
        "T-S 13J35.4"
    ],
    "links": [
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
    "total_links": 2
}
```

### Delete Link

```
DELETE /api/v1/links/{id}

Response (200):
{ "success": true }

Error (404):
{ "detail": "Link not found" }
```

### Update Link

```
PATCH /api/v1/links/{id}

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

### Search Links

```
GET /api/v1/links/search?q=13J35&source=princeton

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

1. **Show the full cluster** - All connected fragments, not just direct links
2. **Minimal decisions** - Relationship type is optional, default to "not sure"
3. **One simple dialog** - No multi-step wizards
4. **Quick navigation** - Click any fragment to go there

### Desktop UI (PyQt6)

#### Links Button in Document View

```
┌─────────────────────────────────────────────────────────────┐
│ T-S 13J35.3                                                 │
│ Cambridge University Library                                │
│                                                             │
│ [📷 Images] [📝 Edit] [🔗 3] [⭐ Star]                       │
│                        ↑                                    │
│              Shows count of linked fragments                │
└─────────────────────────────────────────────────────────────┘
```

- Number shows total fragments in cluster (including current)
- Gray `[🔗]` if no links, colored `[🔗 3]` if has links

#### Links Panel (Click Button → Opens Panel)

```
┌─────────────────────────────────────────┐
│ Linked Fragments                    [×] │
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
│ │    ↳ linked via AIU VII.A.23        │ │
│ └─────────────────────────────────────┘ │
│                                         │
│ ┌─────────────────────────────────────┐ │
│ │ 📄 T-S 13J35.4                     →│ │
│ │    (relationship unknown)           │ │
│ └─────────────────────────────────────┘ │
│                                         │
│ [+ Link Another Fragment]               │
└─────────────────────────────────────────┘
```

- Current fragment highlighted
- Click any other fragment → Navigate to it
- Shows relationship type if known

#### Add Link Dialog

```
┌─────────────────────────────────────────────────────────────┐
│ Link Fragment                                           [×] │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│ Link T-S 13J35.3 to another fragment:                       │
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
│                              [Cancel]  [Create Link]        │
└─────────────────────────────────────────────────────────────┘
```

### Web UI (NiceGUI)

Same design, using NiceGUI components:

```python
async def show_links_panel(shelfmark: str):
    """Show linked fragments panel."""

    # Fetch connected fragments
    data = await api.get(f"/links/connected/{quote(shelfmark)}")
    fragments = data["fragments"]
    links = data["links"]

    with ui.card().classes('w-80'):
        ui.label('Linked Fragments').classes('text-lg font-bold')

        if len(fragments) <= 1:
            ui.label('No links yet').classes('text-gray-500')
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
                    rel = get_relationship_for_fragment(frag, links)
                    if rel:
                        ui.label(rel).classes('text-xs text-gray-500 ml-6')

                if not is_current:
                    ui.on('click', lambda f=frag: navigate_to(f))

        ui.button('+ Link Another', on_click=lambda: show_add_link_dialog(shelfmark))
```

---

## Desktop Client Integration

Add to `corrections_client.py`:

```python
class CorrectionsClient:

    # ... existing methods ...

    def get_linked_fragments(self, shelfmark: str) -> dict:
        """Get all fragments linked to this one."""
        response = self._request(
            "GET",
            f"/links/connected/{quote(shelfmark)}"
        )
        return response

    def create_link(
        self,
        fragment_a: str,
        fragment_b: str,
        relationship_type: Optional[str] = None,
        notes: Optional[str] = None
    ) -> dict:
        """Create a link between two fragments."""
        response = self._request(
            "POST",
            "/links",
            json={
                "fragment_a": fragment_a,
                "fragment_b": fragment_b,
                "relationship_type": relationship_type,
                "notes": notes
            }
        )
        return response

    def delete_link(self, link_id: int) -> bool:
        """Delete a link."""
        response = self._request("DELETE", f"/links/{link_id}")
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

    1. Creates pairwise links between all fragments
    2. Stores the transcription (separate table, covered in searchable corrections spec)
    """

    # Create links between all pairs
    links_created = []
    for i, frag_a in enumerate(shelfmarks):
        for frag_b in shelfmarks[i+1:]:
            link, error = LinkService.create_link(
                db=db,
                fragment_a=frag_a,
                fragment_b=frag_b,
                relationship_type="physical_join",
                source="princeton",
                source_url=source_url,
                notes=source_reference
            )
            if link:
                links_created.append(link)

    # Store transcription (see SEARCHABLE_CORRECTIONS_SPEC.md)
    # ...

    return {
        "shelfmarks": shelfmarks,
        "links_created": len(links_created),
        "transcription_stored": True
    }
```

---

## Implementation Plan

### Phase 1: Backend (1-2 days)

- [ ] Create migration: `fragment_links` table
- [ ] Create model: `backend/models/fragment_link.py`
- [ ] Create schemas: `backend/schemas/link.py`
- [ ] Create service: `backend/services/link_service.py`
- [ ] Create routes: `backend/api/routes/links.py`
- [ ] Add shelfmark normalization utility
- [ ] Unit tests

### Phase 2: Desktop UI (2 days)

- [ ] Add "Links" button to browse toolbar
- [ ] Create `LinksPanel` widget
- [ ] Create `AddLinkDialog`
- [ ] Add to `corrections_client.py`
- [ ] Non-blocking API calls
- [ ] Navigation to linked fragments

### Phase 3: Web UI (1-2 days)

- [ ] Add links indicator to document view
- [ ] Create links panel component
- [ ] Create add link dialog
- [ ] Navigation integration

### Phase 4: Testing & Polish (1 day)

- [ ] Integration testing
- [ ] Edge cases (self-link, duplicates, large clusters)
- [ ] Performance testing (large connected components)
- [ ] UI polish

**Total: ~6-8 days**

---

## File Structure

```
backend/
├── models/
│   └── fragment_link.py        # NEW
├── schemas/
│   └── link.py                 # NEW
├── services/
│   └── link_service.py         # NEW
├── api/routes/
│   └── links.py                # NEW
└── migrations/
    └── add_fragment_links.py   # NEW

desktop/
└── (existing genizah_app.py - add links UI)

web/
├── components/
│   └── links_panel.py          # NEW
└── pages/
    └── (existing - add links integration)
```

---

*Last updated: January 2026*
