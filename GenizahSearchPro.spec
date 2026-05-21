# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_all

datas = [('icon.ico', '.'), ('Help.html', '.'), ('oxford_full_db.json', '.'), ('libraries.csv', '.'), ('ie_volume_map.json', '.'), ('bodleian_master_index.csv', '.'), ('pgp_tag_translations.py', '.'), ('shared_export_utils.py', '.'), ('shared', 'shared'), ('libraries_translations.db', '.'), ('fist_data\\fjms_enrichment.db', 'fist_data'), ('fist_data\\vs_manifest.txt', 'fist_data'), ('nli_data\\nli_crossref.db', 'nli_data'), ('pgp_data\\pgp.db', 'pgp_data')]
binaries = []
hiddenimports = ['tantivy', 'numpy', 'PIL', 'fitz', 'pymupdf']
tmp_ret = collect_all('tantivy')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]
# Phase 95 D-43 — PyMuPDF C-extension binaries must be explicitly collected.
# Without this, dist/GenizahSearch.exe raises ModuleNotFoundError: fitz._fitz
# at runtime (95-RESEARCH.md Pitfall #5).
tmp_ret = collect_all('pymupdf')
datas += tmp_ret[0]; binaries += tmp_ret[1]; hiddenimports += tmp_ret[2]


a = Analysis(
    ['genizah_app.py'],
    pathex=[],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['tkinter', 'matplotlib', 'scipy', 'pandas', 'notebook', 'ipython', 'jedi', 'curses', 'nicegui'],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='GenizahSearchPro',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    version='version_info.txt',
    icon=['icon.ico'],
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name='GenizahSearchPro',
)
