"""Regression contracts for the second /start handoff pass."""

from pathlib import Path

from web.pages.atlas import _embedded_atlas_bootstrap_js


def test_dynamic_atlas_loader_executes_a_real_script_element():
    config = {
        'canvasId': 'start-atlas-canvas',
        'loadingId': 'start-atlas-loading',
        'labels': {'loadError': 'Could not load'},
    }
    script = _embedded_atlas_bootstrap_js(config, 'start-atlas')

    assert 'document.createElement("script")' in script
    assert 'document.head.appendChild(script)' in script
    assert 'window.AtlasDecode.init(config)' in script
    assert 'start-atlas-canvas' in script
    assert '<script' not in script  # never rely on inert insertAdjacentHTML markup


def test_catalog_initial_filter_is_guarded_from_stale_async_repaints():
    source = Path('web/pages/catalog_browse.py').read_text(encoding='utf-8')

    assert "initial_load_complete = {'value': False}" in source
    assert "refresh_serial = {'value': 0}" in source
    assert "if not initial_load_complete['value']" in source
    assert "if this_refresh != refresh_serial['value']" in source
    assert "initial_load_complete['value'] = True" in source
