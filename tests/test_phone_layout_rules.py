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
    """The fold starts behind the guard, and runs beside the search, not before it.

    Before `search_state.is_running = True`, a double-click or a repeated Enter
    suspended two calls at the same await with the guard still open, and both
    searches ran over one result state (Codex on #434). Awaited, the fold first
    waited for the page to connect: a search opened from its address started
    only once the browser had loaded the page (ux_timing, 2026-10-28).
    """
    search = _read('web/pages/search.py')
    body = search[search.index('    async def execute_search('):]
    ready = body.index('if not state.is_ready():')
    running = body.index('search_state.is_running = True')
    fold = body.index('asyncio.ensure_future(_after_delay(0, _collapse_panel_on_phone))')
    feedback = body.index("search_btn.style('display: none;')")
    assert ready < running < fold < feedback, 'the fold must start after the running guard is set and before the search shows'
    assert 'await _collapse_panel_on_phone()' not in search, 'awaited, the fold holds the search until the page has connected'
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
    # init() runs more than once per image: the handlers are bound once and
    # moved off the previous element, never stacked (Codex on #434).
    assert 'if (!this._touch) {' in js
    assert "this.el.addEventListener('touchstart', this._touch.start, { passive: false })" in js
    assert "this._touchEl.removeEventListener('touchstart', this._touch.start)" in js
    assert "addEventListener('touchstart', this.onTouchStart.bind(this)" not in js, 'a fresh bind per init stacks handlers'
    # A pinch that drops to one finger hands over to a pan (Codex on #434).
    end = js[js.index('onTouchEnd: function'):js.index('applyTransform: function')]
    assert 'e.touches.length === 1 && this.state.scale > 1.01' in end
    assert 'this.state.isDragging = true' in end


def test_the_filter_badge_counts_every_pre_search_filter_the_predicate_knows():
    """`_active_filter_count` (the badge) must name every filter `has_active_filters`
    names, or a search can be filtered with a grey, badge-less button (Codex on
    #434: measured materials were in the chips and not in the count)."""
    panel = _read('web/components/filter_panel.py')
    predicate = panel[panel.index('def has_active_filters('):panel.index('def persist_value(')]
    search = _read('web/pages/search.py')
    counter = search[search.index('def _active_filter_count('):search.index('def _domain_display_name(')]
    names = sorted(set(re.findall(r"filter_[a-z_]+", predicate)))
    assert len(names) >= 20, names
    missing = [n for n in names if n not in counter]
    assert not missing, f'the badge does not count {missing}'
    # ...and the one constraint the chip bar shows that the predicate does not hold.
    assert "(text_position_select.value or 'anywhere') != 'anywhere'" in counter


def test_every_fold_closes_the_filters_panel_and_the_panel_can_close_itself():
    """The "Search only in..." expansion is a sibling of the form with a hidden
    header, so a fold that leaves it open leaves it with no close control
    (Codex on #434). The server-side folds close it; the panel has its own
    close button for the fold the browser performs on scroll."""
    search = _read('web/pages/search.py')
    start = search.index('def _close_filters_panel(')
    helper = search[start:search.index('def ', start + 1)]
    assert 'panel.value = False' in helper and '_update_filters_toggle_btn()' in helper
    chevron_fold = search[search.index('def collapse_panel('):search.index('async def _collapse_panel_on_phone(')]
    assert '_close_filters_panel()' in chevron_fold
    escape_fold = search[search.index('def toggle_search_panel('):search.index('def handle_keyboard_shortcut(')]
    assert '_close_filters_panel()' in escape_fold
    assert "ui.button(icon='close', on_click=lambda: _toggle_filters_panel())" in search


def test_the_search_toolbar_density_is_remembered_and_toggled_live():
    search = _read('web/pages/search.py')
    assert "_safe_get('search_density', 'compact')" in search
    assert "_safe_set('search_density', value)" in search
    assert "lst.classes(remove='density-compact density-comfortable', add=f'density-{value}')" in search
    results = _read('web/pages/search_results.py')
    assert "_safe_get('search_density', 'compact')" in results
    assert 'refs.results_list = _results_list' in results


def test_joins_lab_stacks_its_anchor_pane_under_1024px():
    """The anchor pane is a 380px sticky column by inline style, so only an
    !important rule reaches it; under 1024px the stylesheet stacks it above
    the work column (owner's screenshots, 2026-10-22: at 390px the builder
    had 10px and its controls lay across the pane). The Compare window's two
    panes stack on a phone the same way."""
    src = _read('web/pages/joins_lab.py')
    assert "'width: 380px; flex-shrink: 0; position: sticky; top: 64px;'" in src
    assert 'anchor_chip_label' not in src, 'the never-built narrow-screen strip is gone'
    rules = _phone_rules(_read('web/static/common.css'))
    assert '.joins-lab-layout { flex-direction: column !important; align-items: stretch !important; }' in rules
    anchor = rules[rules.index('.joins-lab-anchor {'):]
    anchor = anchor[:anchor.index('}')]
    for decl in ('position: static !important;', 'width: 100% !important;',
                 'max-height: none !important;', 'overflow: visible !important;'):
        assert decl in anchor, decl
    assert '.joins-lab-work { flex: none !important;' in rules
    assert '.joins-lab-anchor .anchor-transcription-panel { max-height: 45vh;' in rules
    assert '.anchor-controls-bar .zoom-level-label { display: none !important; }' in rules
    # The bar wraps when it must: at 320px its groups do not fit one row, and
    # a no-wrap bar had its trailing buttons clipped (Codex on #439).
    bar = rules[rules.index('.anchor-controls-bar {'):]
    bar = bar[:bar.index('}')]
    assert 'nowrap' not in bar, bar
    assert '.compare-panes > .nicegui-column { flex: 1 1 100% !important;' in rules


def test_lists_stacks_its_sidebar_under_769px_and_scrolls_to_the_chosen_list():
    """The My Lists tree is the splitter's 25% panel (97px on a 390px phone).
    Under 769px the splitter stacks and the page scrolls; a tap on a list
    then brings the list, rendered below the tree, into view."""
    src = _read('web/pages/lists.py')
    assert "ui.splitter(value=25).classes('w-full flex-grow lists-splitter')" in src
    rules = _phone_rules(_read('web/static/common.css'), max_width=768)
    assert '.lists-shell { height: auto !important;' in rules
    assert '.lists-splitter.q-splitter--vertical { flex-direction: column !important; }' in rules
    assert '.lists-splitter > .q-splitter__separator { display: none !important; }' in rules
    panel = rules[rules.index('.lists-splitter > .q-splitter__panel {'):]
    panel = panel[:panel.index('}')]
    for decl in ('width: 100% !important;', 'height: auto !important;', 'flex: none !important;'):
        assert decl in panel, decl
    handler = src[src.index('def select_list('):src.index('def render_list_content(')]
    render = handler.index('render_list_content(lists_mgr=lists_mgr, data=data, counts=counts)')
    scroll = handler.index('scrollIntoView')
    assert render < scroll, 'the list is rendered before it is scrolled to'
    assert 'window.innerWidth <= 768' in handler
    assert 'scroll-margin-top' in rules, 'the sticky header must not cover the list'
    assert '.lists-item-actions { flex-direction: row !important;' in rules


def test_dialog_cards_drop_their_minimum_width_on_a_small_phone():
    """lists.py and project_tree.py give their dialogs min-w-[400px] and
    min-w-[500px]; the 480px rule that makes a dialog full-screen must clear
    that minimum too, or the card is wider than the screen."""
    rules = _phone_rules(_read('web/static/common.css'), max_width=480)
    card = rules[rules.index('.q-dialog .q-card {'):]
    card = card[:card.index('}')]
    assert 'min-width: 0 !important;' in card


SHARED_HOOKS = {
    'web/pages/joins_lab.py': ['joins-lab-layout', 'joins-lab-anchor', 'joins-lab-work',
                               'joins-lab-anchor-strip', 'joins-lab-anchor-strip-thumb'],
    'web/components/compare_modal.py': ['compare-panes', 'compare-panes-rtl', 'compare-header', 'compare-header-title',
                                        'compare-pane-head',
                                        'compare-pane-title', 'compare-pane-details', 'compare-pane-info',
                                        'compare-text-toggle', 'compare-text-shown'],
    'web/pages/lists.py': ['lists-shell', 'lists-splitter', 'lists-sidebar', 'lists-content',
                           'lists-item-card', 'lists-item-actions'],
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
    # From 1024px the green card is one line only when it fits: beside the
    # 280px drawer a 1024px screen leaves 744px (Codex on #434), so the row
    # may wrap and the title's box has a zero basis with a floor.
    laptop = page_css[page_css.index('@media (min-width: 1024px) {'):]
    laptop = laptop[:laptop.index('\n    }')]
    assert '.browse-header-center {' in laptop and 'flex-wrap: nowrap' not in laptop
    assert '.browse-header-center > *:has(> .browse-header-title) { flex: 1 1 0%; min-width: 160px; }' in laptop


def _media_block(css: str, prefix: str) -> str:
    """The text of the one ``@media`` block that opens with ``prefix`` exactly."""
    assert css.count(prefix) == 1, prefix
    start = css.index(prefix) + len(prefix)
    depth, i = 1, start
    while depth and i < len(css):
        depth += {'{': 1, '}': -1}.get(css[i], 0)
        i += 1
    return css[start:i]


def test_compare_shows_the_two_images_alone_on_a_phone():
    """Stacked, each Compare pane was a head, a 40vh image and the whole
    transcription, so the candidate's image sat several screens below the
    anchor's (owner, 2026-10-23). Below 641px, and on a phone on its side, the
    panes show the two images alone with each head one row, until the header's
    "Show text" button toggles compare-text-shown on the panes row."""
    src = _read('web/components/compare_modal.py')
    for cls in ('compare-pane-head', 'compare-pane-title', 'compare-pane-details', 'compare-pane-info'):
        assert src.count(cls) == 2, f'{cls} must be set on both pane heads'
    assert 'panes_row.classes(toggle="compare-text-shown")' in src
    assert '.classes("text-white compare-text-toggle")' in src
    assert src.count('.classes("phone-icon-only")') == 2, 'Add as Join and Add to Puzzle go icon-only'
    assert '" compare-panes-rtl" if is_rtl() else ""' in src
    css = _read('web/static/common.css')
    assert '.compare-text-toggle.q-btn { display: none; }' in css
    compact = _media_block(
        css, '@media (max-width: 640px), (max-height: 520px) and (orientation: landscape) {')
    assert '.compare-text-toggle { display: inline-flex !important; }' in compact
    hidden = compact[compact.index('.compare-panes:not(.compare-text-shown) .anchor-transcription-panel'):]
    hidden = hidden[:hidden.index('}')]
    for cls in ('.compare-pane-title', '.compare-pane-details', '.compare-pane-info'):
        assert f'.compare-panes:not(.compare-text-shown) {cls}' in hidden, cls
    assert 'display: none !important;' in hidden
    assert '.compare-panes:not(.compare-text-shown) .compare-pane-head {' in compact
    assert ('.compare-panes-rtl:not(.compare-text-shown) .compare-pane-head '
            '{ flex-direction: row-reverse; }') in compact
    # Upright: two images of at most 32vh fit an 844px phone with the chrome
    # around them; the viewer's fullscreen rule is left alone.
    rules = _phone_rules(css)
    assert ('.compare-panes:not(.compare-text-shown) .anchor-image-pane:not(:fullscreen) .image-container {\n'
            '        max-height: 32vh !important; max-height: 31dvh !important;') in rules
    # On its side: the panes are side by side above 640px already; the image
    # takes the height the header, head, control bar and verdict bar leave.
    landscape = _media_block(css, '@media (max-height: 520px) and (orientation: landscape) {')
    assert ('max-height: calc(100vh - 200px) !important; '
            'max-height: calc(100dvh - 200px) !important;') in landscape
    # A 640x360 or 568x320 phone on its side matches the phone block too, which
    # stacks the panes; this block puts them back beside each other (Codex on
    # #444), and must come after the phone rule to win at equal specificity.
    assert '.compare-panes > .nicegui-column { flex: 1 1 0% !important; min-width: 0; }' in landscape
    assert css.index('.compare-panes > .nicegui-column { flex: 1 1 100% !important;') < css.index(
        '.compare-panes > .nicegui-column { flex: 1 1 0% !important;')
    # The header stays one row on a 390px phone: the title goes, the gaps and
    # side padding shrink, and the icon-only buttons go dense.
    assert 'px-4 py-2 compare-header"' in src
    assert '.classes("text-lg font-semibold compare-header-title")' in src
    assert '.compare-header { gap: 8px !important; padding-inline: 8px !important; }' in rules
    assert '.compare-header-title { display: none !important; }' in rules
    assert '.compare-header .phone-icon-only { padding: 4px 8px !important; min-width: 0 !important; }' in rules


def test_joins_lab_keeps_the_anchor_in_a_strip_once_the_pane_scrolls_off():
    """Under 1024px the anchor pane stacks above the candidates and scrolls away
    with them. A fixed strip under the header (thumbnail, shelfmark, a tap
    scrolls back up) shows while the pane is off the screen: its script sets
    is-visible from an IntersectionObserver on the pane, copies the pane's
    image, and follows the header's reveal (q-header--hidden)."""
    src = _read('web/pages/joins_lab.py')
    assert "items-center gap-2 joins-lab-anchor-strip'" in src
    assert "ui.element('img').classes('joins-lab-anchor-strip-thumb')" in src
    assert 'ui.add_head_html(_ANCHOR_STRIP_HEAD)' in src
    assert '<script src="/static/js/joins_lab_anchor_strip.js" defer></script>' in src
    assert 'anchor_strip_shelfmark.set_text(shelfmark or sys_id)' in src
    assert 'anchor_strip.set_visibility(True)' in src
    assert src.count('anchor_strip.set_visibility(False)') == 2, 'hidden at build and on Change anchor'
    assert 'window.scrollTo({top: 0, behavior: "smooth"})' in src
    js = _read('web/static/js/joins_lab_anchor_strip.js')
    for needle in ("querySelector('.joins-lab-anchor-strip')", "querySelector('.joins-lab-anchor')",
                   'new IntersectionObserver(', "classList.toggle('is-visible', away)",
                   "classList.contains('q-header--hidden')",
                   "classList.toggle('under-hidden-header', hidden)",
                   "pane.addEventListener('load', syncThumb, true)"):
        assert needle in js, needle
    css = _read('web/static/common.css')
    assert '.joins-lab-anchor-strip { display: none; }' in css
    rules = _phone_rules(css, max_width=1023)
    strip = rules[rules.index('.joins-lab-anchor-strip.is-visible:not(.hidden) {'):]
    strip = strip[:strip.index('}')]
    for decl in ('display: flex;', 'position: fixed;', 'top: 64px;', 'inset-inline: 0;', 'z-index: 900;'):
        assert decl in strip, decl
    assert '.joins-lab-anchor-strip.is-visible.under-hidden-header { top: 0; }' in rules
    assert '.joins-lab-anchor-strip-thumb {' in rules
