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