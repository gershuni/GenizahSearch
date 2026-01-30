# Lists & Projects Implementation Plan
## Approach 1: Supabase Direct

**Date:** 2026-01-30
**Status:** In Progress

---

## Overview

Implement unified cloud-synced lists with project hierarchy:
- **Supabase** = Single source of truth
- **Both apps** connect directly to Supabase
- **Projects** have auto-assigned colors
- **Lists** inherit colors from parent projects

---

## Current State

| Component | Status |
|-----------|--------|
| Supabase Schema | ✅ Complete (projects, user_lists, list_items tables exist) |
| Web App - Basic Lists | ✅ Working |
| Web App - Projects UI | ✅ Complete (Phase 1 done) |
| Desktop App - Local Lists | ✅ Working |
| Desktop App - Supabase | ⚠️ Partially done (buggy) |
| Cross-device Sync | ❌ Not working |

---

## Implementation Phases

### Phase 1: Web App Projects UI (Priority: HIGH) ✅ COMPLETE
**Goal:** Add project tree to web app like desktop has

**Tasks:**
1. [x] Create `web/components/project_tree.py` - Collapsible project/list tree
2. [x] Project creation dialogs included in project_tree.py (no separate file needed)
3. [x] Update `web/pages/lists.py` - Replace flat list with project tree
4. [x] Update `web/user_lists.py` - Add project CRUD methods
5. [x] Update `web/components/add_to_list_dialog.py` - Add project selector, remove color picker
6. [ ] Test: Create project → Create list in project → Verify color inheritance

**Files modified:**
- `web/pages/lists.py` - Now uses project_tree component
- `web/user_lists.py` - Added PROJECT_COLORS, project CRUD, get_lists_by_project, get_list_display_color
- `web/components/add_to_list_dialog.py` - Replaced color picker with project selector

**New files:**
- `web/components/project_tree.py` - Full project tree with CRUD dialogs

---

### Phase 2: Desktop App Supabase Client (Priority: HIGH) ✅ COMPLETE
**Goal:** Fix desktop Supabase integration without data loss

**Tasks:**
1. [x] Create proper backup before ANY sync operation - `_backup_local_data()` in lists_sync.py
2. [x] Add sync dialog after login showing local vs cloud lists with options
3. [x] Implement safe sync: never overwrite local with empty cloud
4. [x] Add projects sync to lists_sync.py (both download and upload)
5. [x] Add `get_cloud_lists_preview()` to preview before sync
6. [ ] Test: Login → Sync Dialog → Verify data in both apps

**Safety rules (implemented):**
- ✅ ALWAYS backup before sync (rotating backups)
- ✅ NEVER delete local data if cloud is empty
- ✅ Show sync dialog with preview before any sync action
- ✅ User chooses: Download / Upload / Merge / Skip

---

### Phase 3: Cross-Device Sync (Priority: MEDIUM)
**Goal:** Changes in one app appear in the other

**Tasks:**
1. [ ] Web: Add "Refresh" button to lists page
2. [ ] Desktop: Add periodic sync (every 5 min when online)
3. [ ] Handle conflicts: last-modified wins
4. [ ] Test: Add item on web → See it on desktop (and vice versa)

---

### Phase 4: Polish & Edge Cases (Priority: LOW)
**Tasks:**
1. [ ] Remove color picker from all list creation dialogs
2. [ ] Add loading states during sync
3. [ ] Handle offline gracefully (show cached data)
4. [ ] Add "Recently Viewed" as system list in cloud
5. [ ] Update translations

---

## Data Flow

```
User Action (Web/Desktop)
         │
         ▼
┌─────────────────────┐
│   Supabase Client   │
│   (Direct API)      │
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│     SUPABASE        │
│  ┌───────────────┐  │
│  │   projects    │  │ ◀── Auto-color assignment
│  └───────────────┘  │
│         │           │
│         ▼           │
│  ┌───────────────┐  │
│  │  user_lists   │  │ ◀── project_id (inherits color)
│  └───────────────┘  │
│         │           │
│         ▼           │
│  ┌───────────────┐  │
│  │  list_items   │  │
│  └───────────────┘  │
└─────────────────────┘
```

---

## Color Logic

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

def get_list_display_color(list_data, projects):
    # System lists (Recently Viewed) - gray
    if list_data.get('is_system'):
        return '#9E9E9E'

    # Lists in projects - inherit project color
    project_id = list_data.get('project_id')
    if project_id and project_id in projects:
        return projects[project_id]['color']

    # Standalone lists (General, etc.) - gold
    return '#FFD700'
```

---

## API Endpoints Needed

Already exist in Supabase (via auto-generated REST API):

| Operation | Method | Endpoint |
|-----------|--------|----------|
| List projects | GET | `/rest/v1/projects?user_id=eq.{uuid}` |
| Create project | POST | `/rest/v1/projects` |
| Update project | PATCH | `/rest/v1/projects?id=eq.{id}` |
| Delete project | DELETE | `/rest/v1/projects?id=eq.{id}` |
| List user_lists | GET | `/rest/v1/user_lists?user_id=eq.{uuid}` |
| Move list to project | PATCH | `/rest/v1/user_lists?id=eq.{id}` (set project_id) |

---

## Future Options (Parked)

### Option 2: Restore Backend API
- Restore FastAPI backend as middleware
- Better for complex business logic
- Consider if Supabase becomes limiting

### Option 3: Local-First with Sync
- SQLite on desktop, IndexedDB on web
- Sync engine with conflict resolution
- Consider if offline support becomes critical

---

## Success Criteria

1. ✅ User creates project on web → sees it on desktop
2. ✅ List colors match parent project everywhere
3. ✅ No color picker in list creation dialogs
4. ✅ No data loss during sync
5. ✅ Works when offline (shows cached data)

---

## Next Action

**Phase 1 & 2 Complete!** Next steps:
- Test: Log in on desktop → Sync dialog should appear → Download cloud lists
- Verify lists appear in desktop app after sync
- Start **Phase 3**: Cross-device sync (periodic refresh)
