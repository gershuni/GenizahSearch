# GenizahSearch Developer Guide

> Quick start guide for developers working on GenizahSearch
> Last updated: 2026-09-18

---

## Prerequisites

- **Python 3.11+**
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

> **Note:** `requirements.txt` contains direct dependencies with pinned versions. `requirements-lock.txt` contains the full transitive dependency tree (used by CI). For local development, installing from `requirements.txt` is sufficient.

### 4. Set Up Environment Variables

Create a `.env` file in the project root:

```bash
# Required for user features (lists, corrections, etc.)
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_ANON_KEY=your-anon-key

# Required to run the web app at all (it refuses to start without it);
# any random 32+ character value works locally:
#   python -c "import secrets; print(secrets.token_urlsafe(32))"
GENIZAH_STORAGE_SECRET=

# Optional
GENIZAH_PORT=8081
GENIZAH_FJMS_DB_PATH=  # optional absolute fjms_enrichment.db path; useful from git worktrees
WEB_PUZZLE_ENABLED=true   # enables web puzzle page (set false to hide)
ATLAS_PREVIEW_ENABLED=false  # Phase 133 Visual Atlas Preview beta (/atlas); default OFF, ANDed with baked-asset readiness
MASKING_SCAN_PATTERNS_FILE=  # path to a gitignored restricted-string pattern file for scripts/check_atlas_masking.py (dev/CI only)
PUZZLE_UPLOAD_SECRET=xxx  # HMAC secret for puzzle upload tokens (auto-generated if unset)
NLI_DISK_CACHE_TTL=2592000  # restart-persistent NLI FL-ID cache TTL in seconds (default 30 days)
NLI_MAX_CONCURRENT_FETCHES=4  # concurrent NLI manifest fetch cap
NLI_SEMAPHORE_TIMEOUT=20  # seconds to wait for an NLI fetch slot
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

The authoritative description of where code and data live -- with a decision for every
top-level directory and root file, the dependency rules and the tests that enforce them -- is
[docs/architecture/OVERVIEW.md](../architecture/OVERVIEW.md); data artifacts are in
[docs/architecture/DATA_LIFECYCLE.md](../architecture/DATA_LIFECYCLE.md). The short form:

```
GenizahSearch/
├── web/            # NiceGUI web app; web/main.py is the entry point; FastAPI /api/* in web/api.py
├── desktop/        # PyQt6 modules; genizah_app.py at the root is the desktop entry point
├── shared/         # code both apps use (search, metadata, sidecar services, discovery, passage)
├── scripts/        # tooling and data pipelines (flat namespace package)
├── tests/          # flat suite + lane subdirectories; run via scripts/run_local_tests.py
├── docs/           # documentation by class (architecture/, decisions/, guides/, specs/, archive/)
├── genizah_core.py # permanent facade over shared/
├── build_index.py  # Tantivy index builder (main | lab)
└── libraries.csv, oxford_full_db.json, Help.html, ...   # root data assets Config resolves
```

Gitignored on every checkout, provisioned by hand: the transcription corpus
(`Transcriptions.txt`), the Tantivy indexes under `Genizah_Index/`, and the SQLite sidecars
(`pgp_data/pgp.db`, `fist_data/fjms_enrichment.db`, `nli_data/nli_crossref.db`,
`fgp_data/fgp_transcriptions.db`, `libraries_translations.db`). See DATA_LIFECYCLE for each.

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
6. CI runs automatically on push/PR (lint + docs + tests on Ubuntu and Windows)

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

See **[`ENV_VARS.md`](ENV_VARS.md)** — the single source of truth for every variable.

The partial table that used to sit here was a second copy of the same information and had
already drifted from it (it still gave `NLI_SEMAPHORE_TIMEOUT` a default of 20, changed to 1
in Phase 98). One file, so there is nothing to keep in step.

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

## Continuous Integration

The project uses GitHub Actions for CI. The workflow is defined in `.github/workflows/ci.yml`.

### Triggers

- **Push** to `master-main` branch
- **Pull requests** to any branch

### Jobs

| Job | Runner | What it does |
|-----|--------|-------------|
| `lint-and-docs` | `ubuntu-latest` | Installs `ruff==0.15.10`, runs `ruff check .`, runs `python scripts/check_docs.py` |
| `tests` | `ubuntu-latest` + `windows-latest` | Installs from `requirements-lock.txt`, runs `pytest tests/` |

The `tests` job depends on `lint-and-docs` -- tests only run if lint and docs checks pass first.

Both test runners use Python 3.11 (matching the pinned dependency set).

### Local Pre-Push Checks

Before pushing, run these locally to catch CI failures early:

```bash
ruff check .                          # Lint (same as CI)
PYTHONIOENCODING=utf-8 python scripts/check_docs.py  # Doc health
pytest tests/ -x -q                   # Tests (quick mode)
```

> **Windows note:** `scripts/check_docs.py` uses emoji in output. Set `PYTHONIOENCODING=utf-8` or pipe through `| Out-Null` if your terminal uses cp1255/cp1252.

---

## Dependency Management

### Adding a New Dependency

1. Add the package with `==` pin to `requirements.txt`:
   ```
   new-package==1.2.3
   ```
2. Install it: `pip install -r requirements.txt`
3. Regenerate the lock file: `pip freeze > requirements-lock.txt`
4. Run tests: `pytest tests/`
5. Commit both `requirements.txt` and `requirements-lock.txt`

### Upgrading a Dependency

1. Edit the version in `requirements.txt` (e.g., `requests==2.32.5` -> `requests==2.33.0`)
2. Install: `pip install -r requirements.txt`
3. Regenerate lock: `pip freeze > requirements-lock.txt`
4. Run tests: `pytest tests/`
5. Verify CI passes, then commit both files

### Two-File Strategy

| File | Purpose | Maintained by |
|------|---------|---------------|
| `requirements.txt` | Direct dependencies, exact `==` pins | Human (edit manually) |
| `requirements-lock.txt` | Full transitive tree (`pip freeze` output) | Machine (regenerate after any change) |

CI installs from `requirements-lock.txt` to ensure fully reproducible builds.

### Dev Tools (pytest, ruff)

`pytest` and `ruff` are **CI-only dev tools** and are NOT listed in `requirements.txt` (which is for runtime dependencies only). They are installed separately in the CI workflow:

- `pip install ruff==0.15.10` (in the lint-and-docs job)
- `pip install pytest` (in the tests job)

For local development, install them manually: `pip install ruff pytest`

### Known Limitations

The lock file is generated from a single environment (`pip freeze` on the developer's machine). Platform-specific transitive dependencies (e.g., Windows-only packages) may differ between Ubuntu and Windows CI runners. In practice this has not caused issues because the 14 direct dependencies all have cross-platform wheels. If a CI runner fails to install from the lock file, regenerate it on that platform or remove the platform-specific line from the lock file.

---

## Linting

The project uses [ruff](https://docs.astral.sh/ruff/) for linting, configured in `ruff.toml` at the project root.

```bash
# Check for violations
ruff check .

# Auto-fix what can be fixed
ruff check --fix .
```

Current ruleset (Phase 63): syntax errors (`E9xx`) and import hygiene (`F401`, `F811`, `F821`). The ruleset will expand in future milestones.

Configuration is in `ruff.toml` at the project root. Key settings:
- `line-length = 120` (not enforced yet, future-friendly default)
- `extend-exclude`: `.claude`, `extension`, `dist`, `build`
- `select`: `E9` (syntax errors), `F401` (unused imports), `F811` (redefined unused), `F821` (undefined name)

CI runs `ruff check .` on every push and PR. All violations must be fixed before merging.

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

# Check code style (ruff)
ruff check .
```

---

## Resources

- [NiceGUI Documentation](https://nicegui.io/documentation)
- [Supabase Python Client](https://supabase.com/docs/reference/python)
- [Tantivy Search](https://github.com/quickwit-oss/tantivy-py)
- [Project Documentation](../DOCUMENTATION_INDEX.md)
