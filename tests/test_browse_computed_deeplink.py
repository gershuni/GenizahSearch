"""Focused contract tests for ``/browse?computed=1``.

The deep link opens the existing discovery panel; it must not create a second
computed-identification surface or change ordinary browse behavior.
"""

from __future__ import annotations

import ast
from pathlib import Path

from web.pages.browse_enrichment import (
    BrowsePageRefs,
    _consume_initial_panel_open,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def _source(relative: str) -> str:
    return (REPO_ROOT / relative).read_text(encoding='utf-8')


def test_initial_open_request_is_one_shot_and_normal_browse_stays_collapsed():
    ordinary_refs = BrowsePageRefs()
    ordinary_state = {'value': False}
    _consume_initial_panel_open(ordinary_refs, ordinary_state)
    assert ordinary_state == {'value': False}

    requested_refs = BrowsePageRefs(open_discovery_panel_requested=True)
    requested_state = {'value': False}
    _consume_initial_panel_open(requested_refs, requested_state)
    assert requested_state == {'value': True}
    assert requested_refs.open_discovery_panel_requested is False

    # Closing the panel and re-rendering must not reopen it.
    requested_state['value'] = False
    _consume_initial_panel_open(requested_refs, requested_state)
    assert requested_state == {'value': False}


def test_request_is_consumed_only_after_a_panel_model_is_built():
    tree = ast.parse(_source('web/pages/browse_enrichment.py'))
    function = next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
        and node.name == 'update_discovery_panel_section'
    )
    calls = {
        node.func.id: node.lineno
        for node in ast.walk(function)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        and node.func.id in {'build_panel_rows', '_consume_initial_panel_open'}
    }
    assert calls['build_panel_rows'] < calls['_consume_initial_panel_open']


def test_route_gates_and_forwards_the_computed_request():
    source = _source('web/main.py')
    tree = ast.parse(source)
    route = next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == 'browse_page_route'
    )
    assert any(arg.arg == 'computed' for arg in route.args.args)

    route_source = ast.get_source_segment(source, route) or ''
    assert 'and discovery_available()' in route_source
    assert 'open_computed=_open_computed' in route_source


def test_browse_page_plumbs_request_into_ephemeral_refs():
    source = _source('web/pages/browse.py')
    tree = ast.parse(source)
    create = next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == 'create_browse_page'
    )
    assert any(arg.arg == 'open_computed' for arg in create.args.args)
    create_source = ast.get_source_segment(source, create) or ''
    assert 'BrowsePageRefs(open_discovery_panel_requested=bool(open_computed))' in create_source
