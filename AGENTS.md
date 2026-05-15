# AGENTS.md

## Purpose
Quick, repo-grounded command/workflow reference for AI agents and maintainers.

## Setup
```bash
python -m venv venv
# Windows
venv\Scripts\activate
# Linux/macOS
source venv/bin/activate
pip install -r requirements.txt
```

## Run
```bash
python -m web.main
GENIZAH_PORT=8082 python -m web.main
NICEGUI_RELOAD=true python -m web.main
python genizah_app.py
```

## Index Build
```bash
python build_index.py
python build_index.py main
python build_index.py lab
```

## Quality Checks
```bash
pip install ruff pytest
ruff check .
ruff check --fix .
pytest tests/
pytest tests/ -x -q
PYTHONIOENCODING=utf-8 python scripts/check_docs.py
```

## Release/Packaging
```bash
python scripts/bump_version.py X.Y.Z --dry-run
python scripts/bump_version.py X.Y.Z
python extension/build.py
build_app.bat
pip freeze > requirements-lock.txt
```

## Data Maintenance Workflows
```bash
python scripts/pgp_transcriptions_export.py
python scripts/import_pgp_full.py --execute
python scripts/export_pgp_sidecar.py
python scripts/fix_nli_oxford_mislabel.py --dry-run
python scripts/fix_nli_oxford_mislabel.py --apply
python scripts/soak_search_api.py --url https://genizahsearch.com/api/search
python scripts/import_jts_dpul_v2.py
```

## TODO / Caution
- TODO: `start_servers.sh` exists but references legacy backend flow in historical docs; verify current intent before use.
- TODO: `deploy.sh` includes server-ops and destructive git sync patterns; do not run locally without explicit confirmation.
- TODO: `flake8 web/ genizah_core.py` appears in historical docs but `flake8` is not pinned in `requirements.txt`; prefer Ruff unless explicitly required.
