# מפרט טכני: מערכת תיקוני משתמשים, הערות וחידושים

## תוכן עניינים

1. [סקירה כללית](#סקירה-כללית)
2. [הערכת מורכבות](#הערכת-מורכבות)
3. [ארכיטקטורה](#ארכיטקטורה)
4. [מודל נתונים](#מודל-נתונים)
5. [API Endpoints](#api-endpoints)
6. [אינטגרציה עם המערכת הקיימת](#אינטגרציה-עם-המערכת-הקיימת)
7. [סנכרון דסקטופ](#סנכרון-דסקטופ)
8. [אבטחה](#אבטחה)
9. [שלבי מימוש מומלצים](#שלבי-מימוש-מומלצים)
10. [תלויות טכניות](#תלויות-טכניות)

---

## סקירה כללית

### מטרות המערכת

1. **תיקוני תעתיקים** - חוקרים מאושרים יכולים לתקן תעתיקים שגויים
2. **גרסאות מרובות** - שמירת כל הגרסאות (V0.7, V0.8, גרסאות משתמשים)
3. **הערות** - אישיות ופומביות על מסמכים
4. **חידושים** - דף מרכזי לתגליות שנבעו מהאתר
5. **סנכרון דסקטופ** - עבודה אופליין עם סנכרון

### עקרונות תכנון

- הגרסה האחרונה המאושרת היא ברירת המחדל
- כל הגרסאות נשמרות ומאונדקסות לחיפוש
- תיקונים נכנסים ישירות מחוקר מאושר (ללא workflow של אישור)
- הערות פומביות ניתנות לעריכה רק על ידי הכותב, אחרים יכולים להגיב

---

## הערכת מורכבות

### סיכום: מורכבות בינונית-גבוהה

| רכיב | מורכבות | הערות |
|------|----------|-------|
| מערכת משתמשים | בינונית | JWT + SQLite/PostgreSQL |
| תיקוני תעתיקים | בינונית | שינוי באינדקס הקיים |
| הערות | בינונית | CRUD פשוט + תגובות |
| חידושים | קלה | דומה להערות |
| אינדוקס מורחב | בינונית | הוספת שדות ל-Tantivy |
| סנכרון דסקטופ | גבוהה | ניהול קונפליקטים, תור מקומי |
| UI חדש | בינונית | כ-5 דפים חדשים |

### משאבים נדרשים

**זמן פיתוח משוער:**
- שלב 1 (משתמשים + תיקונים): עבודה משמעותית
- שלב 2 (הערות + חידושים): עבודה משמעותית נוספת
- שלב 3 (סנכרון דסקטופ): הרכיב המורכב ביותר

**דרישות שרת:**
- Database: SQLite (לקטן) או PostgreSQL (לגדול)
- אחסון נוסף: ~1KB לכל גרסת תעתיק, ~500B לכל הערה
- עומס: תלוי במספר המשתמשים הפעילים

---

## ארכיטקטורה

### מבנה כללי

```
┌─────────────────────────────────────────────────────────────────┐
│                         Frontend                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │  Web (NiceGUI)│  │Desktop(PyQt6)│  │  Local Storage      │  │
│  └──────┬───────┘  └──────┬───────┘  │  (Desktop offline)   │  │
│         │                 │          └──────────┬───────────┘  │
└─────────┼─────────────────┼─────────────────────┼──────────────┘
          │                 │                     │
          ▼                 ▼                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                      API Layer (FastAPI)                         │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌───────────┐ │
│  │ Auth API   │  │ Content API│  │ Notes API  │  │ Sync API  │ │
│  └────────────┘  └────────────┘  └────────────┘  └───────────┘ │
└─────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Data Layer                                  │
│  ┌────────────────────┐  ┌────────────────────────────────────┐ │
│  │  SQLite/PostgreSQL │  │  Tantivy Index                     │ │
│  │  - Users           │  │  - Transcriptions (all versions)   │ │
│  │  - Versions        │  │  - Public Notes                    │ │
│  │  - Notes           │  │  - Discoveries                     │ │
│  │  - Discoveries     │  │                                    │ │
│  └────────────────────┘  └────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### קבצים חדשים נדרשים

```
web/
├── auth/
│   ├── __init__.py
│   ├── models.py          # User, Session models
│   ├── jwt_handler.py     # JWT creation/validation
│   └── middleware.py      # Auth middleware
├── db/
│   ├── __init__.py
│   ├── database.py        # DB connection, migrations
│   ├── models.py          # SQLAlchemy models
│   └── migrations/        # Schema migrations
├── pages/
│   ├── auth.py            # Login/Register pages (NEW)
│   ├── profile.py         # User profile (NEW)
│   ├── discoveries.py     # Discoveries page (NEW)
│   └── admin.py           # User approval (NEW)
├── api.py                 # Extend with new endpoints
└── services.py            # Extend with new services

desktop/
└── sync/
    ├── local_store.py     # SQLite for offline changes
    ├── sync_manager.py    # Sync logic
    └── conflict_resolver.py
```

---

## מודל נתונים

### סכמת Database

```sql
-- משתמשים
CREATE TABLE users (
    id TEXT PRIMARY KEY,  -- UUID
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    name TEXT NOT NULL,
    institution TEXT,
    research_field TEXT,
    status TEXT DEFAULT 'pending',  -- pending, approved, admin
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    approved_at TIMESTAMP,
    approved_by TEXT REFERENCES users(id)
);

-- גרסאות תעתיקים
CREATE TABLE transcription_versions (
    id TEXT PRIMARY KEY,  -- UUID
    sys_id TEXT NOT NULL,
    page_num INTEGER NOT NULL,
    user_id TEXT REFERENCES users(id),  -- NULL for V0.7/V0.8
    source TEXT NOT NULL,  -- 'V0.7', 'V0.8', 'user'
    content TEXT NOT NULL,
    change_description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_current_default BOOLEAN DEFAULT FALSE,

    -- Index for quick lookup
    UNIQUE(sys_id, page_num, user_id, created_at)
);

-- מטא-דטה על ברירת מחדל לכל עמוד
CREATE TABLE page_defaults (
    sys_id TEXT NOT NULL,
    page_num INTEGER NOT NULL,
    default_version_id TEXT REFERENCES transcription_versions(id),
    PRIMARY KEY (sys_id, page_num)
);

-- הערות
CREATE TABLE notes (
    id TEXT PRIMARY KEY,
    sys_id TEXT NOT NULL,
    user_id TEXT NOT NULL REFERENCES users(id),
    note_type TEXT NOT NULL,  -- 'public', 'private'
    content TEXT NOT NULL,
    parent_id TEXT REFERENCES notes(id),  -- for replies
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,
    is_deleted BOOLEAN DEFAULT FALSE
);

-- חידושים
CREATE TABLE discoveries (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id),
    discovery_type TEXT NOT NULL,  -- 'identification', 'correction', 'link', 'other'
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,
    is_published BOOLEAN DEFAULT TRUE
);

-- קישור חידושים למסמכים
CREATE TABLE discovery_documents (
    discovery_id TEXT REFERENCES discoveries(id),
    sys_id TEXT NOT NULL,
    PRIMARY KEY (discovery_id, sys_id)
);

-- תור סנכרון (לדסקטופ - נשמר גם מקומית)
CREATE TABLE sync_queue (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    change_type TEXT NOT NULL,  -- 'transcription', 'note', 'discovery'
    payload TEXT NOT NULL,  -- JSON
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    synced_at TIMESTAMP,
    sync_status TEXT DEFAULT 'pending'  -- pending, synced, conflict, failed
);

-- אינדקסים
CREATE INDEX idx_versions_sysid ON transcription_versions(sys_id, page_num);
CREATE INDEX idx_versions_user ON transcription_versions(user_id);
CREATE INDEX idx_notes_sysid ON notes(sys_id);
CREATE INDEX idx_notes_user ON notes(user_id);
CREATE INDEX idx_discoveries_user ON discoveries(user_id);
```

### Python Models (SQLAlchemy)

```python
# web/db/models.py

from sqlalchemy import Column, String, Text, Boolean, DateTime, Integer, ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime
import uuid

class User(Base):
    __tablename__ = 'users'

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    email = Column(String, unique=True, nullable=False)
    password_hash = Column(String, nullable=False)
    name = Column(String, nullable=False)
    institution = Column(String)
    research_field = Column(String)
    status = Column(String, default='pending')  # pending, approved, admin
    created_at = Column(DateTime, default=datetime.utcnow)
    approved_at = Column(DateTime)
    approved_by = Column(String, ForeignKey('users.id'))

    # Relationships
    versions = relationship('TranscriptionVersion', back_populates='user')
    notes = relationship('Note', back_populates='user')
    discoveries = relationship('Discovery', back_populates='user')


class TranscriptionVersion(Base):
    __tablename__ = 'transcription_versions'

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    sys_id = Column(String, nullable=False, index=True)
    page_num = Column(Integer, nullable=False)
    user_id = Column(String, ForeignKey('users.id'))
    source = Column(String, nullable=False)  # 'V0.7', 'V0.8', 'user'
    content = Column(Text, nullable=False)
    change_description = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    is_current_default = Column(Boolean, default=False)

    user = relationship('User', back_populates='versions')


class Note(Base):
    __tablename__ = 'notes'

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    sys_id = Column(String, nullable=False, index=True)
    user_id = Column(String, ForeignKey('users.id'), nullable=False)
    note_type = Column(String, nullable=False)  # 'public', 'private'
    content = Column(Text, nullable=False)
    parent_id = Column(String, ForeignKey('notes.id'))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime)
    is_deleted = Column(Boolean, default=False)

    user = relationship('User', back_populates='notes')
    replies = relationship('Note', backref='parent', remote_side=[id])


class Discovery(Base):
    __tablename__ = 'discoveries'

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String, ForeignKey('users.id'), nullable=False)
    discovery_type = Column(String, nullable=False)
    title = Column(String, nullable=False)
    content = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime)
    is_published = Column(Boolean, default=True)

    user = relationship('User', back_populates='discoveries')
    documents = relationship('DiscoveryDocument', back_populates='discovery')
```

---

## API Endpoints

### Authentication API

```python
# POST /api/auth/register
# Request:
{
    "email": "researcher@university.edu",
    "password": "securepassword",
    "name": "ד\"ר שרה כהן",
    "institution": "האוניברסיטה העברית",
    "research_field": "גניזת קהיר"
}
# Response: { "user_id": "...", "status": "pending" }

# POST /api/auth/login
# Request: { "email": "...", "password": "..." }
# Response: { "access_token": "JWT...", "refresh_token": "...", "user": {...} }

# POST /api/auth/refresh
# Request: { "refresh_token": "..." }
# Response: { "access_token": "...", "refresh_token": "..." }

# GET /api/auth/me
# Headers: Authorization: Bearer JWT...
# Response: { "user": {...} }
```

### Transcription Versions API

```python
# GET /api/versions/{sys_id}/{page_num}
# Response:
{
    "current_default": {
        "id": "...",
        "source": "user",
        "user_name": "ד\"ר שרה כהן",
        "content": "...",
        "created_at": "2026-01-15T10:30:00Z"
    },
    "all_versions": [
        { "id": "...", "source": "user", "user_name": "...", ... },
        { "id": "...", "source": "V0.8", "user_name": null, ... },
        { "id": "...", "source": "V0.7", "user_name": null, ... }
    ]
}

# POST /api/versions/{sys_id}/{page_num}
# Headers: Authorization: Bearer JWT...
# Request:
{
    "content": "הטקסט המתוקן המלא...",
    "change_description": "תיקנתי 'בר' ל'בן' בשורה 3"
}
# Response: { "version_id": "...", "is_now_default": true }

# GET /api/versions/{sys_id}/{page_num}/diff?v1=id1&v2=id2
# Response: { "diff": [...] }  # unified diff format
```

### Notes API

```python
# GET /api/notes/{sys_id}
# Headers: Authorization: Bearer JWT... (optional, for private notes)
# Query: ?type=public|private|all
# Response:
{
    "public_notes": [
        {
            "id": "...",
            "user": { "id": "...", "name": "פרופ' יוסי לוי" },
            "content": "...",
            "created_at": "...",
            "replies": [...]
        }
    ],
    "private_notes": [...]  # only if authenticated
}

# POST /api/notes/{sys_id}
# Headers: Authorization: Bearer JWT...
# Request:
{
    "note_type": "public",
    "content": "הערה חדשה...",
    "parent_id": null  # or note_id for reply
}
# Response: { "note_id": "..." }

# PUT /api/notes/{note_id}
# Headers: Authorization: Bearer JWT...
# Request: { "content": "תוכן מעודכן..." }
# Response: { "success": true }

# DELETE /api/notes/{note_id}
# Headers: Authorization: Bearer JWT...
# Response: { "success": true }
```

### Discoveries API

```python
# GET /api/discoveries
# Query: ?page=1&limit=20&type=identification
# Response:
{
    "discoveries": [
        {
            "id": "...",
            "user": { "name": "...", "institution": "..." },
            "discovery_type": "identification",
            "title": "זיהוי קטע חדש...",
            "content": "...",
            "documents": ["T-S 13J6.5", "T-S 10J12.3"],
            "created_at": "...",
            "comments_count": 3
        }
    ],
    "total": 45,
    "page": 1
}

# POST /api/discoveries
# Headers: Authorization: Bearer JWT...
# Request:
{
    "discovery_type": "identification",
    "title": "זיהוי קטע חדש מספר המצוות",
    "content": "תיאור מפורט...",
    "document_ids": ["sys_id_1", "sys_id_2"],
    "also_add_as_note": true
}
# Response: { "discovery_id": "...", "note_ids": [...] }

# GET /api/discoveries/{discovery_id}
# Response: { full discovery with comments }
```

### Sync API (לדסקטופ)

```python
# POST /api/sync/push
# Headers: Authorization: Bearer JWT...
# Request:
{
    "changes": [
        {
            "local_id": "...",
            "change_type": "transcription",
            "payload": { ... },
            "created_at": "..."
        }
    ]
}
# Response:
{
    "results": [
        { "local_id": "...", "status": "synced", "server_id": "..." },
        { "local_id": "...", "status": "conflict", "conflict_data": {...} }
    ]
}

# GET /api/sync/pull
# Headers: Authorization: Bearer JWT...
# Query: ?since=2026-01-14T00:00:00Z
# Response:
{
    "transcription_versions": [...],  # new versions since timestamp
    "notes": [...],
    "discoveries": [...]
}

# GET /api/sync/status
# Headers: Authorization: Bearer JWT...
# Response: { "last_sync": "...", "pending_count": 0 }
```

### Admin API

```python
# GET /api/admin/users/pending
# Headers: Authorization: Bearer JWT... (admin only)
# Response: { "users": [...] }

# POST /api/admin/users/{user_id}/approve
# Headers: Authorization: Bearer JWT... (admin only)
# Response: { "success": true }

# POST /api/admin/users/{user_id}/reject
# Headers: Authorization: Bearer JWT... (admin only)
# Response: { "success": true }
```

---

## אינטגרציה עם המערכת הקיימת

### שינויים ב-Tantivy Index

הסכמה הנוכחית:
```python
# genizah_core.py - Indexer
schema = {
    'unique_id': TEXT,
    'content': TEXT,
    'source': TEXT,      # "V0.7" or "V0.8"
    'full_header': TEXT,
    'scope': TEXT,
    'boundaries': TEXT
}
```

סכמה חדשה:
```python
schema = {
    'unique_id': TEXT,
    'content': TEXT,
    'source': TEXT,           # "V0.7", "V0.8", "user"
    'full_header': TEXT,
    'scope': TEXT,
    'boundaries': TEXT,
    # New fields
    'version_id': TEXT,       # DB version ID
    'user_id': TEXT,          # NULL for V0.7/V0.8
    'user_name': TEXT,        # For display
    'is_default': BOOLEAN,    # Is this the default version?
    'content_type': TEXT,     # "transcription", "note", "discovery"
}
```

### שינויים ב-GenizahService

```python
# web/services.py - additions

class GenizahService:
    # Existing methods...

    # New methods
    def get_page_versions(self, sys_id: str, page_num: int) -> List[TranscriptionVersion]:
        """Get all versions for a page, ordered by date desc"""
        pass

    def get_default_version(self, sys_id: str, page_num: int) -> TranscriptionVersion:
        """Get the current default version for display"""
        pass

    def create_version(self, sys_id: str, page_num: int,
                       content: str, user_id: str,
                       change_description: str) -> TranscriptionVersion:
        """Create a new user version and set as default"""
        # 1. Save to DB
        # 2. Update Tantivy index
        # 3. Set as new default
        pass

    def get_notes(self, sys_id: str, user_id: Optional[str] = None,
                  note_type: str = 'all') -> List[Note]:
        """Get notes for a document"""
        pass

    def search_notes(self, query: str) -> List[Note]:
        """Search in indexed notes"""
        pass
```

### שינויים ב-Browse Page

```python
# web/pages/browse.py - additions

def create_version_selector(versions: List[dict], current: dict):
    """Create UI for selecting between versions"""
    with ui.expansion('גרסאות תעתיק', icon='history'):
        for v in versions:
            with ui.row():
                is_current = v['id'] == current['id']
                ui.radio(
                    value=v['id'],
                    on_change=lambda e: switch_version(e.value)
                ).props('dense')

                if v['source'] == 'user':
                    ui.label(f"{v['user_name']} ({v['created_at']})")
                else:
                    ui.label(f"{v['source']} (מקור)")

                if is_current:
                    ui.badge('מוצג', color='primary')

def create_edit_button(sys_id: str, page_num: int, current_content: str):
    """Create edit transcription button for authorized users"""
    if not is_user_approved():
        return

    with ui.button('תקן תעתיק', icon='edit'):
        # Open edit dialog...
```

---

## סנכרון דסקטופ

### ארכיטקטורת סנכרון

```
Desktop App
    │
    ▼
┌─────────────────────────────────────────┐
│  Local SQLite DB                        │
│  ├── local_changes (pending sync)       │
│  ├── cached_versions (from server)      │
│  └── cached_notes                       │
└─────────────────────────────────────────┘
    │
    │  On connect / manual sync
    ▼
┌─────────────────────────────────────────┐
│  Sync Manager                           │
│  1. Push local changes                  │
│  2. Handle conflicts                    │
│  3. Pull new data from server           │
│  4. Update local cache                  │
└─────────────────────────────────────────┘
    │
    ▼
Server API (/api/sync/*)
```

### קוד Sync Manager

```python
# desktop/sync/sync_manager.py

import sqlite3
import requests
from datetime import datetime
from typing import List, Optional
import json

class SyncManager:
    def __init__(self, db_path: str, server_url: str):
        self.db_path = db_path
        self.server_url = server_url
        self.token: Optional[str] = None
        self._init_db()

    def _init_db(self):
        """Initialize local SQLite database"""
        conn = sqlite3.connect(self.db_path)
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS local_changes (
                id TEXT PRIMARY KEY,
                change_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                sync_status TEXT DEFAULT 'pending'
            );

            CREATE TABLE IF NOT EXISTS sync_state (
                key TEXT PRIMARY KEY,
                value TEXT
            );

            CREATE TABLE IF NOT EXISTS cached_versions (
                sys_id TEXT,
                page_num INTEGER,
                version_data TEXT,
                updated_at TEXT,
                PRIMARY KEY (sys_id, page_num)
            );
        ''')
        conn.commit()
        conn.close()

    def login(self, email: str, password: str) -> bool:
        """Login and store token"""
        try:
            resp = requests.post(
                f'{self.server_url}/api/auth/login',
                json={'email': email, 'password': password}
            )
            if resp.ok:
                data = resp.json()
                self.token = data['access_token']
                self._save_token(self.token)
                return True
        except requests.RequestException:
            pass
        return False

    def queue_change(self, change_type: str, payload: dict) -> str:
        """Queue a change for sync (works offline)"""
        change_id = str(uuid.uuid4())
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            'INSERT INTO local_changes VALUES (?, ?, ?, ?, ?)',
            (change_id, change_type, json.dumps(payload),
             datetime.utcnow().isoformat(), 'pending')
        )
        conn.commit()
        conn.close()
        return change_id

    def sync(self) -> dict:
        """Full sync: push local changes, pull remote updates"""
        if not self.token:
            return {'error': 'Not logged in'}

        results = {
            'pushed': [],
            'pulled': [],
            'conflicts': [],
            'errors': []
        }

        # 1. Push local changes
        push_results = self._push_changes()
        results['pushed'] = push_results.get('synced', [])
        results['conflicts'] = push_results.get('conflicts', [])

        # 2. Pull remote updates
        pull_results = self._pull_updates()
        results['pulled'] = pull_results

        return results

    def _push_changes(self) -> dict:
        """Push pending local changes to server"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute(
            'SELECT id, change_type, payload, created_at FROM local_changes WHERE sync_status = ?',
            ('pending',)
        )
        changes = [
            {
                'local_id': row[0],
                'change_type': row[1],
                'payload': json.loads(row[2]),
                'created_at': row[3]
            }
            for row in cursor.fetchall()
        ]
        conn.close()

        if not changes:
            return {'synced': [], 'conflicts': []}

        try:
            resp = requests.post(
                f'{self.server_url}/api/sync/push',
                json={'changes': changes},
                headers={'Authorization': f'Bearer {self.token}'}
            )

            if resp.ok:
                results = resp.json()['results']

                # Update local status
                conn = sqlite3.connect(self.db_path)
                for r in results:
                    conn.execute(
                        'UPDATE local_changes SET sync_status = ? WHERE id = ?',
                        (r['status'], r['local_id'])
                    )
                conn.commit()
                conn.close()

                synced = [r for r in results if r['status'] == 'synced']
                conflicts = [r for r in results if r['status'] == 'conflict']
                return {'synced': synced, 'conflicts': conflicts}

        except requests.RequestException as e:
            return {'error': str(e)}

        return {'synced': [], 'conflicts': []}

    def _pull_updates(self) -> List[dict]:
        """Pull updates from server since last sync"""
        last_sync = self._get_last_sync_time()

        try:
            resp = requests.get(
                f'{self.server_url}/api/sync/pull',
                params={'since': last_sync},
                headers={'Authorization': f'Bearer {self.token}'}
            )

            if resp.ok:
                data = resp.json()

                # Update local cache
                self._update_cache(data)

                # Update last sync time
                self._set_last_sync_time(datetime.utcnow().isoformat())

                return data

        except requests.RequestException:
            pass

        return []

    def get_pending_count(self) -> int:
        """Get count of pending changes"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute(
            'SELECT COUNT(*) FROM local_changes WHERE sync_status = ?',
            ('pending',)
        )
        count = cursor.fetchone()[0]
        conn.close()
        return count

    def is_online(self) -> bool:
        """Check if server is reachable"""
        try:
            resp = requests.get(f'{self.server_url}/api/health', timeout=5)
            return resp.ok
        except:
            return False
```

### טיפול בקונפליקטים

```python
# desktop/sync/conflict_resolver.py

class ConflictResolver:
    """Handle sync conflicts between local and server versions"""

    @staticmethod
    def resolve_transcription_conflict(
        local_version: dict,
        server_version: dict,
        strategy: str = 'keep_both'
    ) -> dict:
        """
        Resolve transcription conflict.

        Strategies:
        - 'keep_both': Save both versions (default)
        - 'local_wins': Overwrite server with local
        - 'server_wins': Discard local changes
        - 'merge': Attempt automatic merge (future)
        """

        if strategy == 'keep_both':
            # Both versions are saved, server version becomes default
            # Local version is saved as additional version
            return {
                'action': 'save_as_new_version',
                'local_version': local_version,
                'server_version': server_version,
                'default': 'server'
            }

        elif strategy == 'local_wins':
            return {
                'action': 'overwrite_server',
                'version': local_version
            }

        elif strategy == 'server_wins':
            return {
                'action': 'discard_local',
                'version': server_version
            }

        return {'action': 'manual_resolution_required'}
```

---

## אבטחה

### JWT Authentication

```python
# web/auth/jwt_handler.py

from datetime import datetime, timedelta
from jose import jwt, JWTError
from passlib.context import CryptContext
import os

SECRET_KEY = os.getenv('JWT_SECRET_KEY', 'your-secret-key-change-in-production')
ALGORITHM = 'HS256'
ACCESS_TOKEN_EXPIRE_MINUTES = 60
REFRESH_TOKEN_EXPIRE_DAYS = 30

pwd_context = CryptContext(schemes=['bcrypt'], deprecated='auto')

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)

def create_access_token(data: dict) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({'exp': expire, 'type': 'access'})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def create_refresh_token(data: dict) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    to_encode.update({'exp': expire, 'type': 'refresh'})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def verify_token(token: str, token_type: str = 'access') -> dict:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        if payload.get('type') != token_type:
            return None
        return payload
    except JWTError:
        return None
```

### Middleware

```python
# web/auth/middleware.py

from fastapi import Request, HTTPException, Depends
from fastapi.security import HTTPBearer
from .jwt_handler import verify_token
from ..db.database import get_db
from ..db.models import User

security = HTTPBearer()

async def get_current_user(request: Request, token = Depends(security)):
    payload = verify_token(token.credentials)
    if not payload:
        raise HTTPException(status_code=401, detail='Invalid token')

    db = get_db()
    user = db.query(User).filter(User.id == payload['user_id']).first()
    if not user:
        raise HTTPException(status_code=401, detail='User not found')

    return user

async def require_approved_user(user: User = Depends(get_current_user)):
    if user.status not in ('approved', 'admin'):
        raise HTTPException(status_code=403, detail='Account pending approval')
    return user

async def require_admin(user: User = Depends(get_current_user)):
    if user.status != 'admin':
        raise HTTPException(status_code=403, detail='Admin access required')
    return user
```

### הרשאות לפי פעולה

| פעולה | אורח | ממתין | מאושר | מנהל |
|-------|------|-------|-------|------|
| צפייה בתעתיקים | ✓ | ✓ | ✓ | ✓ |
| צפייה בהערות פומביות | ✓ | ✓ | ✓ | ✓ |
| צפייה בחידושים | ✓ | ✓ | ✓ | ✓ |
| חיפוש | ✓ | ✓ | ✓ | ✓ |
| הערות אישיות | ✗ | ✓ | ✓ | ✓ |
| תיקון תעתיקים | ✗ | ✗ | ✓ | ✓ |
| הערות פומביות | ✗ | ✗ | ✓ | ✓ |
| פרסום חידושים | ✗ | ✗ | ✓ | ✓ |
| אישור משתמשים | ✗ | ✗ | ✗ | ✓ |

---

## שלבי מימוש מומלצים

### שלב 1: תשתית (בסיסי)

1. **Database Setup**
   - הוספת SQLite/PostgreSQL
   - יצירת סכמה
   - Migration infrastructure

2. **Authentication**
   - מודל User
   - JWT handling
   - Login/Register pages
   - Auth middleware

3. **Admin Panel**
   - דף אישור משתמשים
   - ניהול בסיסי

**תוצר:** משתמשים יכולים להירשם ולהתחבר

### שלב 2: תיקוני תעתיקים

1. **Backend**
   - מודל TranscriptionVersion
   - API endpoints לגרסאות
   - שינויים ב-Tantivy schema

2. **Frontend**
   - בורר גרסאות בדף browse
   - טופס עריכת תעתיק
   - הצגת היסטוריית גרסאות

3. **Indexing**
   - אינדוקס כל הגרסאות
   - עדכון חיפוש לכלול source filter

**תוצר:** חוקרים יכולים לתקן תעתיקים

### שלב 3: הערות

1. **Backend**
   - מודל Note
   - API endpoints
   - אינדוקס הערות

2. **Frontend**
   - תצוגת הערות בדף מסמך
   - טופס הוספת הערה
   - מערכת תגובות

3. **חיפוש**
   - הוספת הערות לתוצאות חיפוש
   - פילטר לפי סוג תוכן

**תוצר:** מערכת הערות מלאה

### שלב 4: חידושים

1. **Backend**
   - מודל Discovery
   - API endpoints

2. **Frontend**
   - דף חידושים
   - טופס פרסום חידוש
   - קישור לדפי מסמכים

**תוצר:** דף חידושים פעיל

### שלב 5: סנכרון דסקטופ

1. **Desktop**
   - Local SQLite store
   - Sync manager
   - UI לסטטוס סנכרון

2. **Server**
   - Sync API endpoints
   - Conflict detection

3. **Integration**
   - Login flow בדסקטופ
   - Offline queue
   - Conflict resolution UI

**תוצר:** עבודה אופליין מלאה

---

## תלויות טכניות

### Dependencies חדשות

```
# requirements.txt additions

# Database
sqlalchemy>=2.0.0
alembic>=1.12.0        # migrations

# Authentication
python-jose[cryptography]>=3.3.0
passlib[bcrypt]>=1.7.4

# Optional: PostgreSQL
# psycopg2-binary>=2.9.0
```

### Environment Variables

```bash
# .env

# Database
DATABASE_URL=sqlite:///./genizah.db
# DATABASE_URL=postgresql://user:pass@localhost/genizah

# JWT
JWT_SECRET_KEY=your-very-long-random-secret-key
JWT_ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=60
REFRESH_TOKEN_EXPIRE_DAYS=30

# Admin
ADMIN_EMAIL=admin@example.com
ADMIN_INITIAL_PASSWORD=change-this-password
```

### Database Migrations

```python
# web/db/migrations/001_initial.py

def upgrade(conn):
    conn.executescript('''
        CREATE TABLE users (...);
        CREATE TABLE transcription_versions (...);
        CREATE TABLE notes (...);
        CREATE TABLE discoveries (...);
        -- ... indexes
    ''')

def downgrade(conn):
    conn.executescript('''
        DROP TABLE IF EXISTS discoveries;
        DROP TABLE IF EXISTS notes;
        DROP TABLE IF EXISTS transcription_versions;
        DROP TABLE IF EXISTS users;
    ''')
```

---

## סיכום

### מה המערכת תספק

1. **לחוקרים:** יכולת לתקן שגיאות, להוסיף הערות, לפרסם תגליות
2. **לקהילה:** גרסאות משופרות של תעתיקים, ידע משותף
3. **למחקר:** תיעוד של חידושים שנבעו מהאתר
4. **לדסקטופ:** עבודה רציפה גם ללא חיבור

### מורכבות כוללת

- **קוד חדש:** ~2000-3000 שורות Python
- **דפים חדשים:** 4-5 דפים (auth, profile, discoveries, admin)
- **שינויים בקיים:** browse, search, services
- **תשתית:** Database, Auth, Sync

המערכת בנויה מרכיבים עצמאיים שאפשר לממש בהדרגה.
