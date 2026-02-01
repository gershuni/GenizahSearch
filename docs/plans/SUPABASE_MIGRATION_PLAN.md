# Supabase Migration Plan
## GenizahSearch - Moving to Cloud Backend

**Date:** 2026-01-30
**Goal:** Replace self-hosted backend with Supabase for reliability and simplicity
**Status:** ALL PHASES COMPLETE (Web App + Desktop App Migrated)

---

## Why Supabase?

1. **Data Safety** - Automatic backups, replicated storage, 99.9% uptime
2. **Simplicity** - No server to maintain, they handle security/updates
3. **Free Tier** - Generous limits (50K users, 500MB DB, 1GB storage)
4. **Easy Migration** - PostgreSQL-compatible, REST API auto-generated
5. **Built-in Auth** - User registration, login, password reset included

---

## Current Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Web App       │     │  Backend Server │     │    SQLite DB    │
│   (NiceGUI)     │────▶│   (FastAPI)     │────▶│ corrections.db  │
│   Port 8080     │     │   Port 8000     │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
        │                       │
        │               ┌───────┴───────┐
        │               │   Features:   │
        │               │ - Auth/JWT    │
        │               │ - Lists API   │
        │               │ - Corrections │
        │               │ - Comments    │
        │               │ - Discoveries │
        │               └───────────────┘
        │
┌───────┴─────────┐
│   Desktop App   │
│   (PyQt6)       │──── Local storage only (no cloud sync)
└─────────────────┘
```

---

## Target Architecture

```
┌─────────────────┐
│   Web App       │─────────┐
│   (NiceGUI)     │         │
└─────────────────┘         │
                            ▼
                    ┌───────────────────┐
                    │     SUPABASE      │
                    │                   │
                    │ ┌───────────────┐ │
                    │ │  PostgreSQL   │ │ ◀── Auto-backups
                    │ │   Database    │ │
                    │ └───────────────┘ │
                    │ ┌───────────────┐ │
                    │ │     Auth      │ │ ◀── User management
                    │ │   (GoTrue)    │ │
                    │ └───────────────┘ │
                    │ ┌───────────────┐ │
                    │ │   Storage     │ │ ◀── Files/images (future)
                    │ └───────────────┘ │
                    │ ┌───────────────┐ │
                    │ │   REST API    │ │ ◀── Auto-generated
                    │ └───────────────┘ │
                    └───────────────────┘
                            ▲
┌─────────────────┐         │
│   Desktop App   │─────────┘
│   (PyQt6)       │
└─────────────────┘
```

---

## Current Database Schema

### From `backend/models/`

**Users** (`user.py`)
```sql
- id: Integer (PK)
- email: String (unique)
- username: String (unique)
- hashed_password: String
- full_name: String
- affiliation: String
- bio: Text
- role: Enum (user, editor, reviewer, admin)
- is_active: Boolean
- created_at: DateTime
- last_login: DateTime
- reputation: Integer
- avatar_url: String
- api_key: String
- settings: JSON
```

**UserLists** (`user_list.py`)
```sql
- id: Integer (PK)
- user_id: Integer (FK → users)
- name: String
- name_en: String
- color: String
- is_default: Boolean
- is_system: Boolean
- project_id: Integer (FK → projects)
- created_at: DateTime
```

**Projects** (`user_list.py`)
```sql
- id: Integer (PK)
- user_id: Integer (FK → users)
- name: String
- color: String
- created_at: DateTime
```

**ListItems** (`user_list.py`)
```sql
- id: Integer (PK)
- list_id: Integer (FK → user_lists)
- sys_id: String
- shelfmark: String
- title: String
- fl_id: String
- note: Text
- tags: JSON (array)
- added_at: DateTime
```

**RecentItems** (`user_list.py`)
```sql
- id: Integer (PK)
- user_id: Integer (FK → users)
- sys_id: String
- shelfmark: String
- title: String
- fl_id: String
- viewed_at: DateTime
```

**Corrections** (`correction.py`)
```sql
- id: Integer (PK)
- author_id: Integer (FK → users)
- sys_id: String
- shelfmark: String
- page_number: Integer
- original_text: Text
- corrected_text: Text
- notes: Text
- status: Enum (draft, pending, under_review, approved, rejected, merged)
- created_at: DateTime
- updated_at: DateTime
- reviewed_at: DateTime
- reviewed_by: Integer (FK → users)
- rejection_reason: Text
- upvotes: Integer
- downvotes: Integer
```

**Comments** (`comment.py`)
```sql
- id: Integer (PK)
- author_id: Integer (FK → users)
- sys_id: String
- shelfmark: String
- page_number: Integer
- content: Text
- scope: Enum (page, manuscript, general)
- is_public: Boolean
- parent_id: Integer (FK → comments, self-reference)
- created_at: DateTime
- updated_at: DateTime
- reply_count: Integer
```

**Discoveries** (`discovery.py`)
```sql
- id: Integer (PK)
- user_id: Integer (FK → users)
- type: Enum (discovery, question, observation, correction, join, comment)
- title: String
- content: Text
- shelfmarks: JSON (array)
- is_anonymous: Boolean
- is_pinned: Boolean
- is_hidden: Boolean
- status: Enum (active, answered, closed, featured)
- upvotes: Integer
- downvotes: Integer
- view_count: Integer
- created_at: DateTime
- updated_at: DateTime
```

**FragmentJoins** (`fragment_join.py`)
```sql
- id: Integer (PK)
- user_id: Integer (FK → users)
- fragment_a_sys_id: String
- fragment_a_shelfmark: String
- fragment_b_sys_id: String
- fragment_b_shelfmark: String
- join_type: Enum (physical, content, uncertain)
- confidence: Enum (certain, probable, possible)
- notes: Text
- evidence: Text
- status: Enum (proposed, confirmed, rejected)
- created_at: DateTime
- confirmed_by: Integer (FK → users)
- confirmed_at: DateTime
```

---

## Supabase Setup Steps

### Step 1: Create Supabase Project (5 min)

1. Go to https://supabase.com
2. Sign up / Log in
3. Click "New Project"
4. Choose organization, name it "GenizahSearch"
5. Set database password (save it!)
6. Select region closest to your users
7. Wait for project to initialize (~2 min)

### Step 2: Get Connection Details

From Supabase Dashboard → Settings → API:
- **Project URL**: `https://xxxxx.supabase.co`
- **Anon Key**: `eyJhbGc...` (public, safe for frontend)
- **Service Key**: `eyJhbGc...` (secret, for admin only)

### Step 3: Create Database Tables

Run these SQL commands in Supabase SQL Editor:

```sql
-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Users table (extends Supabase auth.users)
CREATE TABLE public.profiles (
    id UUID PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
    username TEXT UNIQUE,
    full_name TEXT,
    affiliation TEXT,
    bio TEXT,
    role TEXT DEFAULT 'user' CHECK (role IN ('user', 'editor', 'reviewer', 'admin')),
    reputation INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_login TIMESTAMPTZ
);

-- Projects table
CREATE TABLE public.projects (
    id SERIAL PRIMARY KEY,
    user_id UUID REFERENCES auth.users(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    color TEXT DEFAULT '#4CAF50',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- User Lists table
CREATE TABLE public.user_lists (
    id SERIAL PRIMARY KEY,
    user_id UUID REFERENCES auth.users(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    name_en TEXT,
    color TEXT DEFAULT '#FFD700',
    is_default BOOLEAN DEFAULT FALSE,
    is_system BOOLEAN DEFAULT FALSE,
    project_id INTEGER REFERENCES projects(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- List Items table
CREATE TABLE public.list_items (
    id SERIAL PRIMARY KEY,
    list_id INTEGER REFERENCES user_lists(id) ON DELETE CASCADE,
    sys_id TEXT NOT NULL,
    shelfmark TEXT,
    title TEXT,
    fl_id TEXT,
    note TEXT,
    tags JSONB DEFAULT '[]',
    added_at TIMESTAMPTZ DEFAULT NOW()
);

-- Recent Items table
CREATE TABLE public.recent_items (
    id SERIAL PRIMARY KEY,
    user_id UUID REFERENCES auth.users(id) ON DELETE CASCADE,
    sys_id TEXT NOT NULL,
    shelfmark TEXT,
    title TEXT,
    fl_id TEXT,
    viewed_at TIMESTAMPTZ DEFAULT NOW()
);

-- Corrections table
CREATE TABLE public.corrections (
    id SERIAL PRIMARY KEY,
    author_id UUID REFERENCES auth.users(id) ON DELETE SET NULL,
    sys_id TEXT NOT NULL,
    shelfmark TEXT,
    page_number INTEGER,
    original_text TEXT,
    corrected_text TEXT,
    notes TEXT,
    status TEXT DEFAULT 'draft' CHECK (status IN ('draft', 'pending', 'under_review', 'approved', 'rejected', 'merged')),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    reviewed_at TIMESTAMPTZ,
    reviewed_by UUID REFERENCES auth.users(id),
    rejection_reason TEXT,
    upvotes INTEGER DEFAULT 0,
    downvotes INTEGER DEFAULT 0
);

-- Comments table
CREATE TABLE public.comments (
    id SERIAL PRIMARY KEY,
    author_id UUID REFERENCES auth.users(id) ON DELETE SET NULL,
    sys_id TEXT,
    shelfmark TEXT,
    page_number INTEGER,
    content TEXT NOT NULL,
    scope TEXT DEFAULT 'page' CHECK (scope IN ('page', 'manuscript', 'general')),
    is_public BOOLEAN DEFAULT TRUE,
    parent_id INTEGER REFERENCES comments(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    reply_count INTEGER DEFAULT 0
);

-- Discoveries table
CREATE TABLE public.discoveries (
    id SERIAL PRIMARY KEY,
    user_id UUID REFERENCES auth.users(id) ON DELETE SET NULL,
    type TEXT DEFAULT 'discovery' CHECK (type IN ('discovery', 'question', 'observation', 'correction', 'join', 'comment')),
    title TEXT,
    content TEXT,
    shelfmarks JSONB DEFAULT '[]',
    is_anonymous BOOLEAN DEFAULT FALSE,
    is_pinned BOOLEAN DEFAULT FALSE,
    is_hidden BOOLEAN DEFAULT FALSE,
    status TEXT DEFAULT 'active' CHECK (status IN ('active', 'answered', 'closed', 'featured')),
    upvotes INTEGER DEFAULT 0,
    downvotes INTEGER DEFAULT 0,
    view_count INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Fragment Joins table
CREATE TABLE public.fragment_joins (
    id SERIAL PRIMARY KEY,
    user_id UUID REFERENCES auth.users(id) ON DELETE SET NULL,
    fragment_a_sys_id TEXT NOT NULL,
    fragment_a_shelfmark TEXT,
    fragment_b_sys_id TEXT NOT NULL,
    fragment_b_shelfmark TEXT,
    join_type TEXT DEFAULT 'uncertain' CHECK (join_type IN ('physical', 'content', 'uncertain')),
    confidence TEXT DEFAULT 'possible' CHECK (confidence IN ('certain', 'probable', 'possible')),
    notes TEXT,
    evidence TEXT,
    status TEXT DEFAULT 'proposed' CHECK (status IN ('proposed', 'confirmed', 'rejected')),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    confirmed_by UUID REFERENCES auth.users(id),
    confirmed_at TIMESTAMPTZ
);

-- Votes tables (for corrections and discoveries)
CREATE TABLE public.correction_votes (
    id SERIAL PRIMARY KEY,
    correction_id INTEGER REFERENCES corrections(id) ON DELETE CASCADE,
    user_id UUID REFERENCES auth.users(id) ON DELETE CASCADE,
    vote INTEGER CHECK (vote IN (-1, 1)),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(correction_id, user_id)
);

CREATE TABLE public.discovery_votes (
    id SERIAL PRIMARY KEY,
    discovery_id INTEGER REFERENCES discoveries(id) ON DELETE CASCADE,
    user_id UUID REFERENCES auth.users(id) ON DELETE CASCADE,
    vote INTEGER CHECK (vote IN (-1, 1)),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(discovery_id, user_id)
);

-- Create indexes for performance
CREATE INDEX idx_user_lists_user_id ON user_lists(user_id);
CREATE INDEX idx_list_items_list_id ON list_items(list_id);
CREATE INDEX idx_recent_items_user_id ON recent_items(user_id);
CREATE INDEX idx_corrections_author_id ON corrections(author_id);
CREATE INDEX idx_corrections_sys_id ON corrections(sys_id);
CREATE INDEX idx_comments_author_id ON comments(author_id);
CREATE INDEX idx_comments_sys_id ON comments(sys_id);
CREATE INDEX idx_discoveries_user_id ON discoveries(user_id);
CREATE INDEX idx_fragment_joins_user_id ON fragment_joins(user_id);
```

### Step 4: Set Up Row Level Security (RLS)

```sql
-- Enable RLS on all tables
ALTER TABLE profiles ENABLE ROW LEVEL SECURITY;
ALTER TABLE projects ENABLE ROW LEVEL SECURITY;
ALTER TABLE user_lists ENABLE ROW LEVEL SECURITY;
ALTER TABLE list_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE recent_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE corrections ENABLE ROW LEVEL SECURITY;
ALTER TABLE comments ENABLE ROW LEVEL SECURITY;
ALTER TABLE discoveries ENABLE ROW LEVEL SECURITY;
ALTER TABLE fragment_joins ENABLE ROW LEVEL SECURITY;

-- Profiles: Users can read all, update own
CREATE POLICY "Profiles are viewable by everyone" ON profiles FOR SELECT USING (true);
CREATE POLICY "Users can update own profile" ON profiles FOR UPDATE USING (auth.uid() = id);
CREATE POLICY "Users can insert own profile" ON profiles FOR INSERT WITH CHECK (auth.uid() = id);

-- Projects: Users can CRUD their own
CREATE POLICY "Users can view own projects" ON projects FOR SELECT USING (auth.uid() = user_id);
CREATE POLICY "Users can create own projects" ON projects FOR INSERT WITH CHECK (auth.uid() = user_id);
CREATE POLICY "Users can update own projects" ON projects FOR UPDATE USING (auth.uid() = user_id);
CREATE POLICY "Users can delete own projects" ON projects FOR DELETE USING (auth.uid() = user_id);

-- User Lists: Users can CRUD their own
CREATE POLICY "Users can view own lists" ON user_lists FOR SELECT USING (auth.uid() = user_id);
CREATE POLICY "Users can create own lists" ON user_lists FOR INSERT WITH CHECK (auth.uid() = user_id);
CREATE POLICY "Users can update own lists" ON user_lists FOR UPDATE USING (auth.uid() = user_id);
CREATE POLICY "Users can delete own lists" ON user_lists FOR DELETE USING (auth.uid() = user_id);

-- List Items: Users can CRUD items in their lists
CREATE POLICY "Users can view own list items" ON list_items FOR SELECT
    USING (EXISTS (SELECT 1 FROM user_lists WHERE user_lists.id = list_items.list_id AND user_lists.user_id = auth.uid()));
CREATE POLICY "Users can create own list items" ON list_items FOR INSERT
    WITH CHECK (EXISTS (SELECT 1 FROM user_lists WHERE user_lists.id = list_items.list_id AND user_lists.user_id = auth.uid()));
CREATE POLICY "Users can update own list items" ON list_items FOR UPDATE
    USING (EXISTS (SELECT 1 FROM user_lists WHERE user_lists.id = list_items.list_id AND user_lists.user_id = auth.uid()));
CREATE POLICY "Users can delete own list items" ON list_items FOR DELETE
    USING (EXISTS (SELECT 1 FROM user_lists WHERE user_lists.id = list_items.list_id AND user_lists.user_id = auth.uid()));

-- Recent Items: Users can CRUD their own
CREATE POLICY "Users can view own recent items" ON recent_items FOR SELECT USING (auth.uid() = user_id);
CREATE POLICY "Users can create own recent items" ON recent_items FOR INSERT WITH CHECK (auth.uid() = user_id);
CREATE POLICY "Users can delete own recent items" ON recent_items FOR DELETE USING (auth.uid() = user_id);

-- Corrections: Anyone can view non-draft, users can CRUD their own
CREATE POLICY "Anyone can view public corrections" ON corrections FOR SELECT
    USING (status != 'draft' OR auth.uid() = author_id);
CREATE POLICY "Users can create corrections" ON corrections FOR INSERT WITH CHECK (auth.uid() = author_id);
CREATE POLICY "Users can update own corrections" ON corrections FOR UPDATE USING (auth.uid() = author_id);
CREATE POLICY "Users can delete own draft corrections" ON corrections FOR DELETE
    USING (auth.uid() = author_id AND status = 'draft');

-- Comments: Public comments viewable by all, private by owner
CREATE POLICY "Anyone can view public comments" ON comments FOR SELECT
    USING (is_public = true OR auth.uid() = author_id);
CREATE POLICY "Users can create comments" ON comments FOR INSERT WITH CHECK (auth.uid() = author_id);
CREATE POLICY "Users can update own comments" ON comments FOR UPDATE USING (auth.uid() = author_id);
CREATE POLICY "Users can delete own comments" ON comments FOR DELETE USING (auth.uid() = author_id);

-- Discoveries: Non-hidden viewable by all
CREATE POLICY "Anyone can view public discoveries" ON discoveries FOR SELECT
    USING (is_hidden = false OR auth.uid() = user_id);
CREATE POLICY "Users can create discoveries" ON discoveries FOR INSERT WITH CHECK (auth.uid() = user_id);
CREATE POLICY "Users can update own discoveries" ON discoveries FOR UPDATE USING (auth.uid() = user_id);

-- Fragment Joins: All can view confirmed, users can CRUD their own
CREATE POLICY "Anyone can view confirmed joins" ON fragment_joins FOR SELECT
    USING (status = 'confirmed' OR auth.uid() = user_id);
CREATE POLICY "Users can create joins" ON fragment_joins FOR INSERT WITH CHECK (auth.uid() = user_id);
CREATE POLICY "Users can update own joins" ON fragment_joins FOR UPDATE USING (auth.uid() = user_id);
```

### Step 5: Create Database Functions

```sql
-- Function to create default list for new users
CREATE OR REPLACE FUNCTION public.handle_new_user()
RETURNS TRIGGER AS $$
BEGIN
    -- Create profile
    INSERT INTO public.profiles (id, username)
    VALUES (NEW.id, NEW.email);

    -- Create default "General" list
    INSERT INTO public.user_lists (user_id, name, name_en, is_default)
    VALUES (NEW.id, 'General', 'General', true);

    RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Trigger for new user signup
CREATE TRIGGER on_auth_user_created
    AFTER INSERT ON auth.users
    FOR EACH ROW EXECUTE FUNCTION public.handle_new_user();

-- Function to get next project color
CREATE OR REPLACE FUNCTION public.get_next_project_color(p_user_id UUID)
RETURNS TEXT AS $$
DECLARE
    colors TEXT[] := ARRAY['#4CAF50', '#2196F3', '#9C27B0', '#FF5722', '#00BCD4', '#E91E63', '#795548', '#607D8B', '#FF9800', '#009688'];
    used_colors TEXT[];
    color TEXT;
BEGIN
    SELECT ARRAY_AGG(p.color) INTO used_colors FROM projects p WHERE p.user_id = p_user_id;

    FOREACH color IN ARRAY colors LOOP
        IF used_colors IS NULL OR NOT (color = ANY(used_colors)) THEN
            RETURN color;
        END IF;
    END LOOP;

    -- If all colors used, cycle
    RETURN colors[1 + (COALESCE(array_length(used_colors, 1), 0) % array_length(colors, 1))];
END;
$$ LANGUAGE plpgsql;
```

---

## Code Changes Required

### New File: `web/supabase_client.py`

```python
"""
Supabase client for GenizahSearch.
Replaces the FastAPI backend for data operations.
"""
from supabase import create_client, Client
from typing import Optional, Dict, List, Any
import os

# Configuration - move to environment variables
SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://xxxxx.supabase.co')
SUPABASE_ANON_KEY = os.environ.get('SUPABASE_ANON_KEY', 'eyJ...')

_client: Optional[Client] = None

def get_client() -> Client:
    global _client
    if _client is None:
        _client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    return _client

# Auth functions
async def sign_up(email: str, password: str) -> Dict:
    client = get_client()
    return client.auth.sign_up({"email": email, "password": password})

async def sign_in(email: str, password: str) -> Dict:
    client = get_client()
    return client.auth.sign_in_with_password({"email": email, "password": password})

async def sign_out() -> None:
    client = get_client()
    client.auth.sign_out()

async def get_user() -> Optional[Dict]:
    client = get_client()
    return client.auth.get_user()

# Lists functions
async def get_user_lists(user_id: str) -> List[Dict]:
    client = get_client()
    response = client.table('user_lists').select('*').eq('user_id', user_id).execute()
    return response.data

async def create_list(user_id: str, name: str, project_id: Optional[int] = None) -> Dict:
    client = get_client()
    data = {'user_id': user_id, 'name': name}
    if project_id:
        data['project_id'] = project_id
    response = client.table('user_lists').insert(data).execute()
    return response.data[0] if response.data else None

# ... more functions
```

### Update: `web/auth_state.py`

Replace JWT-based auth with Supabase auth:

```python
from web.supabase_client import get_client, sign_in, sign_up, sign_out, get_user

class GlobalAuthState:
    _user = None
    _session = None

    @classmethod
    async def login(cls, email: str, password: str) -> Dict:
        result = await sign_in(email, password)
        if result.user:
            cls._user = result.user
            cls._session = result.session
            return {"success": True, "user": result.user}
        return {"error": result.error.message}

    @classmethod
    async def register(cls, email: str, password: str) -> Dict:
        result = await sign_up(email, password)
        if result.user:
            return {"success": True, "user": result.user}
        return {"error": result.error.message}

    @classmethod
    def is_logged_in(cls) -> bool:
        return cls._user is not None

    @classmethod
    def get_user(cls) -> Optional[Dict]:
        return cls._user
```

### Update: `web/user_lists.py`

Replace API calls with Supabase calls:

```python
from web.supabase_client import get_client

class UserListsManager:
    def __init__(self):
        self.client = get_client()
        self._cache = None
        self._cache_time = 0

    async def get_all_lists(self) -> List[Dict]:
        user = self.client.auth.get_user()
        if not user:
            return []
        response = self.client.table('user_lists').select('*').eq('user_id', user.id).execute()
        return response.data

    async def create_list(self, name: str, project_id: Optional[int] = None) -> Optional[int]:
        user = self.client.auth.get_user()
        if not user:
            return None
        data = {'user_id': user.id, 'name': name}
        if project_id:
            data['project_id'] = project_id
        response = self.client.table('user_lists').insert(data).execute()
        return response.data[0]['id'] if response.data else None

    # ... etc
```

---

## Migration Steps

### Phase 1: Setup - COMPLETE
1. [x] Create Supabase project
2. [x] Run SQL to create tables
3. [x] Run SQL to set up RLS policies
4. [x] Run SQL to create functions/triggers
5. [x] Test in Supabase dashboard

### Phase 2: Python Client - COMPLETE
1. [x] Install supabase-py: `pip install supabase`
2. [x] Create `web/supabase_client.py`
3. [x] Update `web/auth_state.py` for Supabase auth
4. [x] Update `web/user_lists.py` for Supabase
5. [x] Test auth flow (register, login, logout)
6. [x] Test lists CRUD

### Phase 3: Migrate Other Features - COMPLETE
1. [x] Update corrections to use Supabase
2. [x] Update comments to use Supabase
3. [x] Update discoveries to use Supabase
4. [x] Update fragment joins to use Supabase
5. [x] Test all features

### Phase 4: Data Migration - COMPLETE
1. [x] Export data from SQLite
2. [x] Transform to Supabase format
3. [x] Import to Supabase
4. [x] Verify data integrity

### Phase 5: Desktop App - COMPLETE
1. [x] Add supabase-py to desktop app
2. [x] Create desktop Supabase client (`supabase_corrections_client.py`)
3. [x] Update desktop auth (uses Supabase auth via SupabaseCorrectionsClient)
4. [x] Update desktop lists to use Supabase (`lists_sync.py` + ListsManager integration)
5. [x] Cross-device sync enabled (bidirectional sync between desktop and web)

**Implementation Notes:**
- Created `supabase_corrections_client.py` - Drop-in replacement for REST API client
- Created `lists_sync.py` - Handles bidirectional list sync with cloud
- Updated `corrections_client.py` to use Supabase client by default (with REST fallback)
- Updated `genizah_core.py` ListsManager with cloud sync methods
- Updated `genizah_app.py` login/logout to enable/disable cloud sync
- Desktop app now connects directly to Supabase (no backend server needed)

### Phase 6: Cleanup - COMPLETE
1. [x] Archive FastAPI backend code (renamed to backend_legacy/)
2. [x] Update deployment scripts (.env.production.example)
3. [x] Update documentation
4. [x] Remove backend imports from web/api.py

---

## Environment Variables

Add to `.env` file:
```
SUPABASE_URL=https://xxxxx.supabase.co
SUPABASE_ANON_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

---

## Files to Delete (After Migration)

```
backend/
├── api/           # No longer needed
├── models/        # No longer needed
├── schemas/       # No longer needed
├── services/      # No longer needed
├── config.py      # No longer needed
├── main.py        # No longer needed
└── ...
```

---

## Files to Modify

```
web/
├── supabase_client.py    # NEW - Supabase client
├── auth_state.py         # UPDATE - Use Supabase auth
├── user_lists.py         # UPDATE - Use Supabase
├── pages/
│   ├── corrections.py    # UPDATE - Use Supabase
│   ├── discoveries.py    # UPDATE - Use Supabase
│   └── ...
└── components/
    └── ...

genizah_app.py            # UPDATE - Add Supabase client for desktop
```

---

## Rollback Plan

If issues arise:
1. Keep backend code in separate branch
2. Environment variable to switch between backends
3. Database backup before migration

---

## Cost Estimate

**Supabase Free Tier:**
- 50,000 monthly active users
- 500 MB database
- 1 GB file storage
- 2 GB bandwidth

**Pro Tier ($25/month) if needed:**
- 100,000 MAU
- 8 GB database
- 100 GB storage

For GenizahSearch's expected usage, **free tier should be sufficient**.

---

## Security Considerations

1. **Row Level Security** - Users can only access their own data
2. **HTTPS** - All Supabase connections are encrypted
3. **JWT Tokens** - Handled by Supabase, auto-refresh
4. **Password Hashing** - Handled by Supabase (bcrypt)
5. **Rate Limiting** - Built into Supabase

---

## Testing Checklist

### Web App (Phase 3)
- [x] User can register
- [x] User can login
- [x] User can logout
- [x] User can create list
- [x] User can add items to list
- [x] User can view their lists
- [x] User cannot view other users' lists
- [x] User can create project
- [x] List inherits project color
- [x] Corrections work
- [x] Comments work
- [x] Discoveries work

### Desktop App (Phase 5)
- [x] Desktop app connects to Supabase directly
- [x] Desktop app auth works (login/register/logout)
- [x] Desktop lists sync to cloud on login
- [x] Desktop lists sync from cloud on login
- [x] Changes sync bidirectionally between web and desktop
- [x] Offline mode works (with local cache)

---

## Files Created/Modified in Migration

### New Files
```
web/supabase_client.py          # Web app Supabase client (Phase 2-3)
supabase_corrections_client.py  # Desktop app Supabase client (Phase 5)
lists_sync.py                   # Desktop lists cloud sync (Phase 5)
supabase_setup.sql              # Database setup SQL
migrate_to_supabase.py          # Data migration script
```

### Modified Files
```
web/auth_state.py               # Updated for Supabase auth
web/user_lists.py               # Updated for Supabase lists
web/api.py                      # Removed backend dependencies
corrections_client.py           # Uses Supabase client by default
genizah_core.py                 # Added cloud sync to ListsManager
genizah_app.py                  # Added cloud sync on login/logout
.env.production.example         # Added Supabase config
.gitignore                      # Updated for new files
```

### Deleted Files (moved to backend_legacy/)
```
backend/                        # Entire FastAPI backend (no longer needed)
```

---

**Total Time:** Migration completed
**Status:** ALL PHASES COMPLETE
**Architecture:** Direct Supabase connection (no backend server needed)

---

*"Simplicity is the ultimate sophistication." - Leonardo da Vinci*
