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
pip install ruff==0.15.10 pytest
ruff check .
python scripts/run_local_tests.py            # the test suite, in bounded lanes (CI's marker expression)
python scripts/run_local_tests.py -m slow    # one marker lane, still through the runner
python scripts/run_local_tests.py -m render_smoke   # the NiceGUI lane (CI job render-smoke-tests); -m REPLACES the default expression
python scripts/run_gui_tests.py              # the Qt lane (CI job gui-tests)
pytest tests/test_some_file.py               # one named file directly is fine
PYTHONUTF8=1 python scripts/check_docs.py
```

**Test-command policy.** Broad selections go through the bounded runner
(`python scripts/run_local_tests.py`; `-m <expr>` picks a marker lane and *replaces* the default
expression). Direct `pytest` is for a named file or a directory narrower than `tests/`
(`pytest tests/test_x.py`, `pytest tests/atlas_bake -m atlas_bake`). Never one `pytest tests/`
process, with or without a marker: measured 2026-09-09, it paged for hours without finishing
(see [tests/README.md](tests/README.md)). Gates are their exit codes -- `ruff check .`,
`python scripts/check_docs.py` and the runner each exit non-zero on failure; `scripts/init.ps1`
prints their state for orientation and never fails.

## Where things live
- `shared/` -- code both apps use; imports neither PyQt6 nor NiceGUI.
- `desktop/` -- PyQt6 code; `genizah_app.py` at the root is the desktop entry point.
- `web/` -- the NiceGUI app; `web/main.py` is its entry point (FastAPI routes under `/api/*` live in `web/api.py`).
- `scripts/` -- tooling and data pipelines; never imported by `shared/`.
- `tests/` -- flat, one file per concern; run through the runner above.
- The root keeps entry points, build inputs (`GenizahSearchPro.spec`, `build_app.bat`,
  `CompileScriptGenizah.iss`), the permanent `genizah_core.py` facade, and the data assets that
  `shared/config.py` resolves next to it (`libraries.csv`, `oxford_full_db.json`, `Help.html`, ...).
- The layering is enforced by tests, not by convention: [tests/test_no_back_edges_core.py](tests/test_no_back_edges_core.py),
  [tests/test_no_back_edges_desktop.py](tests/test_no_back_edges_desktop.py) and
  [tests/test_seed016_layering_executor.py](tests/test_seed016_layering_executor.py). Register a new
  `shared/` or `desktop/` module in their lists.
- The full picture, including a decision table for every top-level directory and root file, is
  [docs/architecture/OVERVIEW.md](docs/architecture/OVERVIEW.md).

## Release/Packaging
```bash
python scripts/bump_version.py X.Y.Z --dry-run
python scripts/bump_version.py X.Y.Z
python scripts/checkpoint_sidecars.py
python extension/build.py
build_app.bat
pip freeze > requirements-lock.txt
```

After the build, prove the packaged app from the repository root -- **PowerShell, so the variable is set
separately; the `VAR=1 cmd` prefix is bash and fails here**:

```powershell
$env:GENIZAH_PACKAGING_SMOKE='1'; python -m pytest tests/test_local_pyinstaller_smoke.py
Remove-Item Env:\GENIZAH_PACKAGING_SMOKE
```

Without that variable a missing or stale EXE SKIPS instead of failing. `GENIZAH_PACKAGING_EXE=<path>`
(same PowerShell form) points the check at a build made elsewhere with `--distpath`, so a trial build
never has to overwrite `dist/`.

`deploy.sh` runs on the server (`deploy.bat` is its Windows trigger); never run it locally.
`build_app.bat` builds from the checked-in `GenizahSearchPro.spec`; commit spec edits before building.

## Data Maintenance Workflows
```bash
python scripts/export_fist_enrichment.py
python scripts/import_nli_crossref.py
python scripts/import_manchester_luna.py
python scripts/import_jts_dpul.py
python scripts/import_jts_dpul_v2.py
python scripts/pgp_transcriptions_export.py
python scripts/import_pgp_documents.py
python scripts/import_pgp_full.py --execute
python scripts/import_document_sources.py
python scripts/import_pgp_sections.py
python scripts/export_pgp_sidecar.py
python scripts/fix_nli_oxford_mislabel.py --dry-run
python scripts/fix_nli_oxford_mislabel.py --apply
python scripts/soak_search_api.py --url https://genizahsearch.com/api/search
```
