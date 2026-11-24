# -*- coding: utf-8 -*-
"""The real Tantivy index and catalogue, for the few tests that drive them end to end.

The index is looked for where both apps look for it, shared/config.py's
Config.INDEX_DIR: a checkout's Genizah_Index, the legacy folder in the home folder,
or the LOCALAPPDATA folder. Tests that looked only at <checkout>/Genizah_Index
skipped on a development machine that keeps the index elsewhere, so they ran
nowhere. CI has no index anywhere, so these tests skip there.

`real_web_state` fills web.state as web/main.py's startup does (catalogue,
variants, the search engine; no worker processes) for one module, then puts back
what was there before. tests/conftest.py registers it.
"""
from __future__ import annotations

import os

import pytest

NO_INDEX = "no Tantivy index where the apps look for it (Config.INDEX_DIR/tantivy_db)"


def real_index_path():
    """Config.INDEX_DIR's tantivy_db when it holds a committed index, else None.
    shared/config.py creates INDEX_DIR at import, so the folder alone proves nothing:
    meta.json is the index's own commit record."""
    from shared.config import Config
    db = os.path.join(Config.INDEX_DIR, "tantivy_db")
    return db if os.path.isfile(os.path.join(db, "meta.json")) else None


@pytest.fixture(scope="module")
def real_web_state():
    """web.state with the real catalogue and search engine. Skips without the index;
    fails when the index is there but does not open, or the catalogue is empty."""
    if real_index_path() is None:
        pytest.skip(NO_INDEX)
    from genizah_core import MetadataManager, SearchEngine, VariantManager
    from web.state import state

    saved = (state.meta_mgr, state.var_mgr, state.searcher)
    meta_mgr = MetadataManager()
    meta_mgr.load_catalogue_now()
    var_mgr = VariantManager()
    # No FL-ID lookup build: it runs over the whole browse map in a daemon thread that
    # close_index() does not stop, so it would outlive the module and load the files after
    # it. A browse by FL id with its sys_id (every call these tests make) scans only that
    # manuscript's pages without it (SearchEngine.get_browse_page_by_fl).
    engine = SearchEngine(meta_mgr, var_mgr, build_fl_index=False)
    assert engine.searcher is not None, "the index is there but did not open"
    assert meta_mgr.csv_bank, "libraries.csv gave no catalogue"
    state.meta_mgr, state.var_mgr, state.searcher = meta_mgr, var_mgr, engine
    try:
        yield state
    finally:
        state.meta_mgr, state.var_mgr, state.searcher = saved
        engine.close_index()
