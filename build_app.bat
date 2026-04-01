@echo off
REM Build GenizahSearchPro desktop application
REM Run from the project root directory with venv activated

REM Checkpoint any WAL journals into main .db files before bundling.
REM PyInstaller copies only the .db file — WAL/SHM journals are lost,
REM which can cause empty tables in the installed copy.
echo Checkpointing sidecar databases...
python scripts\checkpoint_sidecars.py
echo Done.

python -m PyInstaller --noconfirm --noconsole --onedir --clean ^
 --name "GenizahSearchPro" ^
 --icon "icon.ico" ^
 --version-file "version_info.txt" ^
 --hidden-import "tantivy" ^
 --hidden-import "numpy" ^
 --hidden-import "PIL" ^
 --collect-all "tantivy" ^
 --add-data "icon.ico;." ^
 --add-data "Help.html;." ^
 --add-data "oxford_full_db.json;." ^
 --add-data "libraries.csv;." ^
 --add-data "ie_volume_map.json;." ^
 --add-data "bodleian_master_index.csv;." ^
 --add-data "pgp_tag_translations.py;." ^
 --add-data "shared_export_utils.py;." ^
 --add-data "shared;shared" ^
 --add-data "libraries_translations.db;." ^
 --add-data "fist_data\fjms_enrichment.db;fist_data" ^
 --add-data "fist_data\vs_manifest.txt;fist_data" ^
 --add-data "nli_data\nli_crossref.db;nli_data" ^
 --add-data "pgp_data\pgp.db;pgp_data" ^
 --exclude-module "tkinter" ^
 --exclude-module "matplotlib" ^
 --exclude-module "scipy" ^
 --exclude-module "pandas" ^
 --exclude-module "notebook" ^
 --exclude-module "ipython" ^
 --exclude-module "jedi" ^
 --exclude-module "curses" ^
 --exclude-module "nicegui" ^
 --noupx ^
 genizah_app.py

echo.
echo Build complete! Output in dist\GenizahSearchPro
echo.