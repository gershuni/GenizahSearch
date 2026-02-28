@echo off
REM Build GenizahSearchPro desktop application
REM Run from the project root directory with venv activated
REM
REM NOTE: If antivirus software flags this application, see ANTIVIRUS_INFO.md
REM for instructions on submitting the app for whitelisting.

REM Checkpoint any WAL journals into main .db files before bundling.
REM PyInstaller copies only the .db file — WAL/SHM journals are lost,
REM which can cause empty tables in the installed copy.
echo Checkpointing sidecar databases...
python -c "import sqlite3, os;^
dbs=['fist_data/fjms_enrichment.db','pgp_data/pgp.db','nli_data/nli_crossref.db'];^
[exec('c=sqlite3.connect(p);c.execute(\"PRAGMA wal_checkpoint(TRUNCATE)\");c.execute(\"PRAGMA journal_mode=DELETE\");c.close();print(f\"  {p}: ok\")') for p in dbs if os.path.exists(p)]"
echo Done.

pyinstaller --noconfirm --noconsole --onedir --clean ^
 --name "GenizahSearchPro" ^
 --icon "icon.ico" ^
 --version-file "version_info.txt" ^
 --hidden-import "tantivy" ^
 --collect-all "tantivy" ^
 --add-data "icon.ico;." ^
 --add-data "Help.html;." ^
 --add-data "oxford_full_db.json;." ^
 --add-data "libraries.csv;." ^
 --add-data "bodleian_master_index.csv;." ^
 --add-data "pgp_tag_translations.py;." ^
 --add-data "shared_export_utils.py;." ^
 --add-data "shared;shared" ^
 --add-data "fist_data\fjms_enrichment.db;fist_data" ^
 --add-data "nli_data\nli_crossref.db;nli_data" ^
 --add-data "pgp_data\pgp.db;pgp_data" ^
 --exclude-module "tkinter" ^
 --exclude-module "matplotlib" ^
 --exclude-module "scipy" ^
 --exclude-module "pandas" ^
 --exclude-module "numpy" ^
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
echo IMPORTANT: If antivirus software flags this application as a false positive,
echo see ANTIVIRUS_INFO.txt for instructions on submitting the app for whitelisting.
