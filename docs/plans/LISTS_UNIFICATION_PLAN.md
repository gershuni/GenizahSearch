# Lists & Projects Unification Plan
## "The Pro Programmer" Architecture

**Date:** 2026-01-30
**Goal:** Unified cloud-synced lists with project-based color inheritance

---

## Executive Summary

Create a **single source of truth** for lists and projects stored in the backend API, with consistent UI across web and desktop apps. Projects act as color-coded groups - users don't choose list colors directly; colors are inherited from parent projects.

---

## Current State Analysis

### Desktop App (genizah_app.py)
```
Projects (with auto-assigned colors)
├── Project A (color: #4CAF50)
│   ├── List 1 (inherits green)
│   └── List 2 (inherits green)
├── Project B (color: #2196F3)
│   └── List 3 (inherits blue)
└── Standalone Lists
    ├── General (default, gold)
    └── Recently Viewed (system, gray)
```

**Key functions:**
- `_get_list_display_color()` - Returns project color for lists
- `_get_next_project_color()` - Auto-assigns from palette
- Lists have `project_id` to link to parent project

### Web App (current)
- Flat list structure (no project hierarchy)
- User picks colors for each list (wrong!)
- Projects exist in data but not exposed in UI

### Backend API
- Full projects support exists
- `/lists/migrate` handles project migration
- Models support `project_id` on lists

---

## Target Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Backend API                            │
│  - Single source of truth for all lists/projects           │
│  - Handles auth, sync, conflict resolution                  │
│  - Projects have colors, lists inherit from projects       │
└─────────────────────────────────────────────────────────────┘
                    │                    │
        ┌───────────┴───────────┐       ┌┴──────────────────┐
        │      Web App          │       │   Desktop App     │
        │  (NiceGUI/Quasar)     │       │   (PyQt6)         │
        │                       │       │                   │
        │  ┌─────────────────┐  │       │  ┌─────────────┐  │
        │  │ Projects Panel  │  │       │  │ Projects    │  │
        │  │ ├── Project A   │  │       │  │ (existing)  │  │
        │  │ │   ├── List 1  │  │       │  └─────────────┘  │
        │  │ │   └── List 2  │  │       │                   │
        │  │ └── Project B   │  │       │  Uses API instead │
        │  │     └── List 3  │  │       │  of local storage │
        │  └─────────────────┘  │       │                   │
        └───────────────────────┘       └───────────────────┘
```

---

## Implementation Plan

### Phase 1: Backend Verification (30 min)
**Files:** `backend/services/lists_service.py`, `backend/api/routes/lists.py`

1. [ ] Verify project CRUD endpoints exist and work:
   - `POST /lists/projects` - Create project
   - `GET /lists/projects` - List projects
   - `PUT /lists/projects/{id}` - Update project
   - `DELETE /lists/projects/{id}` - Delete project

2. [ ] Verify list-project relationship:
   - `PUT /lists/{id}` accepts `project_id`
   - Lists API returns `project_id`

3. [ ] Add project color auto-assignment if missing

---

### Phase 2: Web Frontend - Projects UI (2-3 hours)
**Files:** `web/pages/lists.py`, `web/user_lists.py`, `web/components/`

#### 2.1 Update Lists Page Layout
```
┌──────────────────────────────────────────────────────────┐
│  Personal Lists                           [+ New Project]│
├──────────────────────────────────────────────────────────┤
│  ┌─────────────────────┐  ┌────────────────────────────┐ │
│  │ SIDEBAR             │  │ CONTENT                    │ │
│  │                     │  │                            │ │
│  │ ▼ Research (green)  │  │ List: Hebrew Manuscripts   │ │
│  │   ├── Hebrew MSS    │  │ ─────────────────────────  │ │
│  │   └── Arabic MSS    │  │ • T-S 12.123              │ │
│  │                     │  │ • T-S 13.456              │ │
│  │ ▼ Teaching (blue)   │  │ • T-S NS 102.4            │ │
│  │   └── Course Texts  │  │                            │ │
│  │                     │  │                            │ │
│  │ ─── Standalone ───  │  │                            │ │
│  │ ☆ General           │  │                            │ │
│  │ ⏱ Recently Viewed  │  │                            │ │
│  └─────────────────────┘  └────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
```

#### 2.2 New Components to Create

**`web/components/project_tree.py`**
```python
def render_project_tree(lists_mgr, selected_list_id, on_select):
    """
    Renders collapsible project tree with lists.
    - Projects shown as expandable headers
    - Lists shown as items under projects
    - Standalone lists shown at bottom
    - Colors inherited from projects
    """
```

**`web/components/create_project_dialog.py`**
```python
def show_create_project_dialog(lists_mgr, on_success):
    """
    Simple dialog for creating a project.
    - Name input only
    - Color auto-assigned (show preview)
    - No color picker!
    """
```

#### 2.3 Modify Existing Components

**`web/components/add_to_list_dialog.py`**
- Remove color picker from list creation
- Add project selection dropdown
- List inherits project's color automatically

**`web/pages/lists.py`**
- Replace flat list with project tree
- Add "New Project" button
- Add context menu: "Move to Project"

#### 2.4 Update UserListsManager
**`web/user_lists.py`**
```python
# Add project methods
async def create_project(self, name: str) -> Optional[str]
async def get_projects(self) -> List[Dict]
async def update_project(self, project_id: str, name: str) -> bool
async def delete_project(self, project_id: str, delete_lists: bool) -> bool
async def move_list_to_project(self, list_id: str, project_id: Optional[str]) -> bool

# Update get_list_color to inherit from project
def get_list_color(self, list_id: str) -> str:
    list_data = self.data['lists'].get(list_id)
    if list_data and list_data.get('project_id'):
        project = self.data['projects'].get(list_data['project_id'])
        if project:
            return project.get('color', '#FFD700')
    return list_data.get('color', '#FFD700') if list_data else '#FFD700'
```

---

### Phase 3: Desktop App - Cloud Sync (2-3 hours)
**Files:** `genizah_app.py`, `genizah_core.py`

#### 3.1 Add API Client to Desktop
```python
# genizah_core.py - New class
class CloudListsManager:
    """
    API-backed lists manager for desktop app.
    Replaces local ListsManager when user is logged in.
    """
    def __init__(self, api_client, fallback_local_mgr):
        self.api = api_client
        self.local = fallback_local_mgr
        self._cache = None
        self._cache_time = 0

    # Same interface as ListsManager but calls API
```

#### 3.2 Desktop Login Integration
- On login: Switch from local ListsManager to CloudListsManager
- On logout: Fall back to local storage
- Offer migration dialog (like web)

#### 3.3 Offline Support
- Cache API responses locally
- Queue changes when offline
- Sync when connection restored

---

### Phase 4: Cleanup & Polish (1 hour)

1. [ ] Remove color picker from all list creation dialogs
2. [ ] Add color preview in project creation
3. [ ] Update translations for new UI strings
4. [ ] Remove debug prints added during bug fixes
5. [ ] Update documentation

---

## Color Palette

Auto-assigned project colors (same as desktop):
```python
PROJECT_COLORS = [
    '#4CAF50',  # Green
    '#2196F3',  # Blue
    '#9C27B0',  # Purple
    '#FF5722',  # Deep Orange
    '#00BCD4',  # Cyan
    '#E91E63',  # Pink
    '#795548',  # Brown
    '#607D8B',  # Blue Gray
    '#FF9800',  # Orange
    '#009688',  # Teal
]

def get_next_project_color(used_colors: Set[str]) -> str:
    for color in PROJECT_COLORS:
        if color not in used_colors:
            return color
    # Cycle if all used
    return PROJECT_COLORS[len(used_colors) % len(PROJECT_COLORS)]
```

---

## Data Model

### Project
```json
{
  "id": "project_abc123",
  "name": "Research",
  "color": "#4CAF50",
  "created": "2026-01-30T12:00:00Z",
  "user_id": 123
}
```

### List
```json
{
  "id": 456,
  "name": "Hebrew Manuscripts",
  "project_id": "project_abc123",  // Optional - inherits color
  "color": null,  // Ignored when project_id is set
  "is_default": false,
  "is_system": false,
  "user_id": 123
}
```

### Display Color Logic
```python
def get_display_color(list_data, projects):
    # System lists use their own color
    if list_data.get('is_system'):
        return list_data.get('color', '#9E9E9E')

    # Lists in projects inherit project color
    project_id = list_data.get('project_id')
    if project_id and project_id in projects:
        return projects[project_id].get('color', '#FFD700')

    # Default/standalone lists use gold
    return '#FFD700'
```

---

## Migration Strategy

### Existing Users with Local Lists
1. Show migration dialog on login
2. Migrate lists AND projects to cloud
3. Preserve project-list relationships
4. Clear local storage after success

### Existing Users with Cloud Lists
- No action needed
- New project UI just reveals existing data

---

## Testing Checklist

- [ ] Create project (web) - auto-color assigned
- [ ] Create list in project (web) - inherits color
- [ ] Move list to different project - color updates
- [ ] Move list out of project - uses default color
- [ ] Delete project - lists become standalone
- [ ] Delete project with lists - lists deleted
- [ ] Desktop sync - projects appear
- [ ] Desktop create project - syncs to cloud
- [ ] Offline mode - changes queued
- [ ] Cross-device sync - changes appear

---

## Files to Modify

### Backend
- `backend/api/routes/lists.py` - Verify/add project endpoints
- `backend/services/lists_service.py` - Verify project logic
- `backend/models/user_list.py` - Verify project model

### Web
- `web/pages/lists.py` - Major rewrite for project tree
- `web/user_lists.py` - Add project methods
- `web/components/add_to_list_dialog.py` - Remove color picker
- `web/components/project_tree.py` - NEW
- `web/components/create_project_dialog.py` - NEW
- `web/translations.py` - New strings

### Desktop
- `genizah_core.py` - Add CloudListsManager
- `genizah_app.py` - Integrate cloud sync
- API client module - NEW

---

## Priority Order

1. **Backend verification** - Ensure API is ready
2. **Web project UI** - Biggest visible change
3. **Desktop cloud sync** - Unify storage
4. **Polish** - Remove old color pickers, cleanup

---

## Success Criteria

1. User creates project on web, sees it on desktop
2. List colors match parent project everywhere
3. No color picker in list creation dialogs
4. Offline desktop works, syncs when online
5. Migration from local to cloud is seamless

---

**Estimated Total Time:** 6-8 hours
**Complexity:** Medium-High
**Risk:** Medium (affects existing user data)

---

*"The best code is code that makes complex things look simple."*
