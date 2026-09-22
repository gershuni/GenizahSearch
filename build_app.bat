@echo off
REM Build GenizahSearchPro desktop application
REM Run from the project root directory with venv activated

REM Checkpoint any WAL journals into main .db files before bundling.
REM PyInstaller copies only the .db file — WAL/SHM journals are lost,
REM which can cause empty tables in the installed copy.
echo Checkpointing sidecar databases...
python scripts\checkpoint_sidecars.py
if errorlevel 1 exit /b 1
echo Done.

REM Refuse to bundle a pgp.db that must not ship. pgp_data\*.db is gitignored, so the
REM 2026-09-21 removal of the known-bad pgp_translations table is LOCAL FILE STATE, not a
REM property of the repo -- a build host that kept the old sidecar, or anyone who ran
REM scripts\restore_pgp_translations.py to measure against it, would otherwise quietly
REM ship the owner's withheld data. Also rejects a pre-1.1.0 sidecar, which lacks
REM documents.doc_relation and would present 891 translations as transcriptions.
REM Runs AFTER the checkpoint above so it sees committed rows, not a WAL journal.
echo Checking the sidecar is fit to ship...
python scripts\check_shipping_sidecar.py
if errorlevel 1 exit /b 1

REM Build from the CHECKED-IN spec, never from command-line flags.
REM Command-line PyInstaller regenerates GenizahSearchPro.spec on every run and
REM strips the maintained collect_all() calls for pymupdf / zstandard / lxml plus
REM the fitz, openpyxl and defusedxml hidden imports -- the very things that keep
REM the C-extensions in the bundle. The spec carries every --add-data and
REM --exclude-module the old invocation passed, so nothing is lost by using it.
REM Only --noconfirm and --clean are legal alongside a spec file; every other
REM flag the old command used is rejected or ignored when building from one.
python -m PyInstaller --noconfirm --clean GenizahSearchPro.spec
if errorlevel 1 exit /b 1

echo.
echo Build complete! Output in dist\GenizahSearchPro
echo.