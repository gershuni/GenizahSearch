# GenizahSearch Developer Guide

> Quick start guide for developers working on GenizahSearch

---

## Prerequisites

- **Python 3.10+**
- **Git**
- **~10GB disk space** (for indexes and transcription data)

---

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/gershuni/GenizahSearch.git
cd GenizahSearch
```

### 2. Create Virtual Environment

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Set Up Environment Variables

Create a `.env` file in the project root:

```bash
# Required for user features (lists, corrections, etc.)
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_ANON_KEY=your-anon-key

# Optional
GENIZAH_PORT=8081
```

> **Note:** You can get Supabase credentials from the project admin, or set up your own Supabase project for development.

### 5. Download Transcription Data

Download `Transcriptions.txt` from [Zenodo](https://zenodo.org/records/17734473) and place it in the project root.

### 6. Build Search Index (First Time Only)

```bash
python build_index.py
```

This takes ~1 hour and creates the `Genizah_Index/` directory.

### 7. Run the Web Application

```bash
python -m web.main
```

Open http://localhost:8081 in your browser.

---

## Running Without Supabase

If you don't have Supabase credentials, the app will still work for:
- Search
- Browse
- Parallels

User features (lists, corrections, comments) will be disabled.

---

## Running the Desktop App

```bash
python genizah_app.py
```

---

## Project Structure

```
GenizahSearch/
├── web/                      # Web application (NiceGUI)
│   ├── main.py              # Entry point
│   ├── pages/               # Page components
│   │   ├── home.py
│   │   ├── search.py
│   │   ├── browse.py
│   │   ├── parallels.py
│   │   ├── lists.py
│   │   └── ...
│   ├── components/          # Reusable UI components
│   └── supabase_client.py   # Supabase integration
│
├── genizah_core.py          # Core logic (search, indexing, data models)
├── genizah_app.py           # Desktop application (PyQt6)
├── build_index.py           # Index builder script
│
├── Genizah_Index/           # Search indexes (generated)
│   ├── tantivy_db/          # Main search index
│   ├── lab_index/           # Parallels index
│   └── *.pkl                # Cached data
│
├── docs/                    # Documentation
│   ├── guides/              # How-to guides
│   ├── plans/               # Implementation plans
│   ├── specs/               # Technical specs
│   └── archive/             # Historical docs
│
└── tests/                   # Test files
```

---

## Key Concepts

### Shelfmarks

Manuscript identifiers like `T-S 12.123`, `MS Heb c 57`.

### sys_id

Internal unique identifier for manuscripts (from the MiDRASH dataset).

### fl_id

Fragment/leaf identifier. Format: `{shelfmark}.{folio}{side}`
- Example: `T-S 12.123.1r` = T-S 12.123, folio 1, recto

### Page vs Folio

- **Page**: A single side (recto or verso)
- **Folio**: A physical sheet (has recto and verso)

---

## Development Workflow

### Making Changes

1. Create a feature branch: `git checkout -b feature/my-feature`
2. Make changes
3. Test locally
4. Commit with descriptive message
5. Push and create PR

### Code Style

- Use type hints
- Hebrew comments are acceptable
- Follow existing patterns in the codebase

### Testing

```bash
pytest tests/
```

---

## Common Tasks

### Add a New Page

1. Create `web/pages/my_page.py`:
   ```python
   from nicegui import ui

   def create_page():
       with ui.column().classes('w-full'):
           ui.label('My Page')
   ```

2. Register in `web/main.py`:
   ```python
   @ui.page('/my-page')
   def my_page():
       create_page()
   ```

### Add a New Component

Create `web/components/my_component.py`:
```python
from nicegui import ui

def my_component(text: str):
    with ui.card():
        ui.label(text)
```

### Query Supabase

```python
from web.supabase_client import get_client

client = get_client()

# Select
result = client.table('corrections').select('*').eq('status', 'pending').execute()

# Insert
result = client.table('comments').insert({'content': 'Hello', 'sys_id': '123'}).execute()

# Update
result = client.table('profiles').update({'full_name': 'New Name'}).eq('id', user_id).execute()
```

### Search the Index

```python
from genizah_core import SearchEngine

engine = SearchEngine()
results = engine.execute_search('שלום', mode='variants', gap=2, limit=100)
```

---

## Environment Variables Reference

| Variable | Required | Description |
|----------|----------|-------------|
| `SUPABASE_URL` | For user features | Supabase project URL |
| `SUPABASE_ANON_KEY` | For user features | Supabase anonymous key |
| `GENIZAH_PORT` | No | Web app port (default: 8081) |
| `NICEGUI_RELOAD` | No | Hot reload (default: true in dev) |

---

## Troubleshooting

### "Index not found"

Run `python build_index.py` to create the search index.

### "SUPABASE_ANON_KEY not set"

Create a `.env` file with Supabase credentials, or run without user features.

### "Port already in use"

Change the port: `GENIZAH_PORT=8082 python -m web.main`

### PyQt6 Issues (Desktop App)

```bash
pip install PyQt6 PyQt6-WebEngine
```

---

## Useful Commands

```bash
# Run web app with hot reload
NICEGUI_RELOAD=true python -m web.main

# Build main index only
python build_index.py main

# Build lab index only
python build_index.py lab

# Run tests
pytest tests/ -v

# Check code style
flake8 web/ genizah_core.py
```

---

## Resources

- [NiceGUI Documentation](https://nicegui.io/documentation)
- [Supabase Python Client](https://supabase.com/docs/reference/python)
- [Tantivy Search](https://github.com/quickwit-oss/tantivy-py)
- [Project Documentation](../DOCUMENTATION_INDEX.md)
