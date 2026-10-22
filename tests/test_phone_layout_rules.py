# -*- coding: utf-8 -*-
"""The phone layout's hooks stay where its CSS expects them (owner, 2026-10-22).

Below 640px the site is laid out by rules in ``web/static/common.css`` and in
the /browse page's own style block, keyed to class names the page modules set
and to one call in ``execute_search``. Each rule is only as good as its hook:
a renamed class or a moved call would silently put a phone back on the stacked
desktop layout (measured before the change at 390x844: the /browse image
958px down the page, 300px of /search left to results, the English header
wrapped out of its bar). These tests read the sources; the render-smoke
counterpart drives the live /browse page.
"""
from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def _read(rel: str) -> str:
    return (REPO / rel).read_text(encoding='utf-8')


def _phone_rules(css: str, max_width: int = 640) -> str:
    """The text of every ``@media (max-width: <N>px)`` block for N up to ``max_width``.

    The phone rules live in 640px blocks, the narrow-screen toolbar rules in a
    1023px block; a hook may be served by either.
    """
    blocks = []
    widths = [640, 1023] if max_width == 640 else [max_width]
    for m in re.finditer(r'@media \(max-width: (%s)px\) \{' % '|'.join(str(w) for w in widths), css):
        depth, i = 1, m.end()
        while depth and i < len(css):
            depth += {'{': 1, '}': -1}.get(css[i], 0)
            i += 1
        blocks.append(css[m.end():i])
    assert blocks, f'no @media (max-width: {max_width}px) block'
    return '\n'.join(blocks)


def test_the_header_row_does_not_wrap():
    main = _read('web/main.py')
    assert "ui.row(wrap=False).classes('w-full h-full items-center justify-between px-6 app-header')" in main


def test_login_and_register_are_named_icon_buttons_on_a_phone():
    auth = _read('web/auth_state.py')
    for name, icon in (('Login', 'login'), ('Register', 'person_add')):
        pattern = (r"ui\.button\(tr\('%s'\), icon='%s'.*?aria-label=\"\{tr\(\"%s\"\)\}\".*?"
                   r"\.classes\('text-sm phone-icon-only'\)" % (name, icon, name))
        assert re.search(pattern, auth, re.S), name
    rules = _phone_rules(_read('web/static/common.css'))
    assert '.phone-icon-only .q-btn__content > .block { font-size: 0;' in rules


def test_the_search_form_folds_once_the_search_is_marked_running():
    """The fold is execute_search's first await, so it must sit behind the guard.

    Before `search_state.is_running = True`, a double-click or a repeated Enter
    suspended two calls at the same await with the guard still open, and both
    searches ran over one result state (Codex on #434).
    """
    search = _read('web/pages/search.py')
    body = search[search.index('    async def execute_search():'):]
    ready = body.index('if not state.is_ready():')
    running = body.index('search_state.is_running = True')
    fold = body.index('await _collapse_panel_on_phone()')
    feedback = body.index("search_btn.style('display: none;')")
    assert ready < running < fold < feedback, 'the fold must come after the running guard is set and before the search shows'
    assert 'await' not in body[body.index('if search_state.is_running:'):running], 'no await may sit between the guard and the running flag'
    assert "await ui.run_javascript('window.innerWidth'" in search
    assert 'width <= 640' in search


def test_the_expanded_result_browse_link_keeps_the_matched_folio():
    results = _read('web/pages/search_results.py')
    block = results[results.index("'w-full items-center expand-actions'"):results.index('def _render_full_text(')]
    assert 'ui.link(target=_card_browse_url)' in block, 'the expanded card must reuse the deep link with fl_id, volume_ie and page'
    assert "/browse?sys_id={sys_id}'" not in block, 'a bare sys_id opens the first folio of the first volume'


def test_quick_view_info_shortcuts_select_the_info_tab_on_a_phone():
    results = _read('web/pages/search_results.py')
    for key in ('pgp', 'fjms'):
        assert f"on_click=lambda: _open_info_section('{key}')" in results, key
        assert f"_info_sections['{key}'] = " in results, key
    assert 'q-expansion-item__toggle' not in results, 'the sections are toggled from Python, not by a DOM click'
    handler = results[results.index('async def _open_info_section('):results.index('section.value = True if switched else not section.value')]
    assert "width <= 640 and adv_tabs.value != 'info'" in handler
    assert "adv_tabs.value = 'info'" in handler


def test_browse_metadata_button_names_the_action_it_performs():
    browse = _read('web/pages/browse.py')
    assert "_meta_label = tr('Hide Metadata') if show_metadata['value'] else tr('Show Metadata')" in browse
    assert 'aria-label="{_meta_label}"' in browse
    assert '.tooltip(_meta_label)' in browse
    assert 'aria-label="{tr("Show Metadata")}"' not in browse


def test_the_viewer_handles_touch_pinch_and_drag():
    js = _read('web/static/manuscript_viewer.js')
    for handler in ('onTouchStart', 'onTouchMove', 'onTouchEnd'):
        assert f"{handler}: function" in js, handler
        assert f"this.{handler}.bind(this)" in js, f'{handler} is not attached'
    assert "{ passive: false }" in js, 'touch listeners must be able to preventDefault'
    assert 'this.state.scale > 1.01' in js, 'one finger must leave an unzoomed image to the page'


def test_the_search_toolbar_density_is_remembered_and_toggled_live():
    search = _read('web/pages/search.py')
    assert "_safe_get('search_density', 'compact')" in search
    assert "_safe_set('search_density', value)" in search
    assert "lst.classes(remove='density-compact density-comfortable', add=f'density-{value}')" in search
    results = _read('web/pages/search_results.py')
    assert "_safe_get('search_density', 'compact')" in results
    assert 'refs.results_list = _results_list' in results


SHARED_HOOKS = {
    'web/pages/search.py': ['search-shell', 'search-main-row', 'search-row-btn', 'search-adv-panel',
                            'search-filters-panel', 'results-header', 'results-count-group',
                            'results-export-group', 'results-more-btn', 'tb-hidden', 'density-toggle'],
    'web/pages/search_results.py': ['pagination-placeholder', 'results-list', 'result-card', 'result-snippet',
                                    'result-shelfmark', 'result-actions', 'result-head', 'result-badges',
                                    'result-title', 'result-index', 'expand-row', 'expand-thumb', 'expand-text',
                                    'expand-actions', 'adv-info-bar', 'adv-content', 'adv-tabs', 'adv-text-col',
                                    'adv-image-col', 'adv-info', 'adv-image-adjust', 'adv-actions-card', 'adv-scroll'],
    'web/auth_state.py': ['phone-icon-only', 'auth-username'],
}


def test_common_css_names_every_class_the_pages_set():
    css = _read('web/static/common.css')
    for rel, classes in SHARED_HOOKS.items():
        src = _read(rel)
        for cls in classes:
            assert cls in src, f'{rel} no longer sets {cls}'
            assert f'.{cls}' in css, f'common.css has no rule for .{cls}'


def test_catalog_browse_stacks_its_sidebar_under_768px():
    rules = _phone_rules(_read('web/static/common.css'), max_width=768)
    src = _read('web/pages/catalog_browse.py')
    for cls in ('catalog-layout', 'catalog-sidebar', 'catalog-filters-toggle'):
        assert cls in src, cls
    assert '.catalog-layout { flex-wrap: wrap !important; }' in rules
    assert '.catalog-sidebar.is-open { display: flex; }' in rules
    assert "classes(toggle='is-open')" in src


BROWSE_HOOKS = ['browse-page-title', 'browse-lookup-card', 'browse-lookup-icon', 'browse-header-center',
                'browse-header-title', 'browse-controls-card', 'browse-image-head', 'browse-image-title',
                'browse-image-tools', 'browse-rotate-slider', 'browse-image-adjust']


def test_browse_rules_name_every_class_the_page_sets_and_reach_the_inline_image_height():
    browse = _read('web/pages/browse.py')
    rules = _phone_rules(browse)
    css_start = browse.index('<style>')
    css_end = browse.index('</style>', css_start)
    page_css, python_side = browse[css_start:css_end], browse[:css_start] + browse[css_end:]
    for cls in BROWSE_HOOKS:
        assert f'.{cls}' in page_css, f'browse.py has no rule for .{cls}'
        assert cls in python_side, f'browse.py no longer sets {cls}'
    # The box carries an inline height; only an !important rule reaches it.
    assert "height: calc(60vh - 100px)" in browse
    assert '.image-container { height: auto !important;' in rules
    # The transcription pane is un-scrolled the way the print rules do it.
    assert 'contain: none !important;' in rules
