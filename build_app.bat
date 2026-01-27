@echo off
REM Build GenizahSearchPro desktop application
REM Run from the project root directory with venv activated
REM
REM NOTE: If antivirus software flags this application, see ANTIVIRUS_INFO.md
REM for instructions on submitting the app for whitelisting.

pyinstaller --noconfirm --noconsole --onedir --clean ^
 --name "GenizahSearchPro" ^
 --icon "icon.ico" ^
 --version-file "version_info.txt" ^
 --hidden-import "tantivy" ^
 --collect-all "tantivy" ^
 --hidden-import "google.genai" ^
 --collect-all "google.genai" ^
 --add-data "icon.ico;." ^
 --add-data "Help.html;." ^
 --add-data "oxford_full_db.json;." ^
 --add-data "libraries.csv;." ^
 --add-data "bodleian_master_index.csv;." ^
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
echo see ANTIVIRUS_INFO.md for instructions on submitting the app for whitelisting.
