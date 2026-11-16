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


def test_sign_in_is_one_named_icon_button_on_a_phone():
    """One button (owner, 2026-11-05): its dialog has the Register tab."""
    auth = _read('web/auth_state.py')
    pattern = (r"ui\.button\(tr\('Sign in'\), icon='login'.*?aria-label=\"\{tr\(\"Sign in or register\"\)\}\".*?"
               r"\.classes\('text-sm phone-icon-only'\)")
    assert re.search(pattern, auth, re.S)
    header = auth.split('def create_auth_buttons', 1)[1].split('\nclass ', 1)[0]
    assert "tr('Register')" not in header and "tr('Login')" not in header
    rules = _phone_rules(_read('web/static/common.css'))
    assert '.phone-icon-only .q-btn__content > .block { font-size: 0;' in rules


def test_the_sidebar_hides_its_labels_only_in_the_icon_rail():
    """Quasar's `q-mini-drawer-hide` is also hidden in the phone's overlay drawer, so the
    labels vanished there (2026-11-05); the sidebar uses its own class, and the overlay
    drawer has no rail button."""
    main = _read('web/main.py')
    css = _read('web/static/common.css')
    assert 'q-mini-drawer-hide' not in main
    assert main.count('nav-mini-hide') == 3
    assert '.q-drawer--mini .nav-mini-hide { display: none !important; }' in css
    assert '.q-drawer--mobile .nav-rail-row { display: none; }' in css
    # The rail button at the sidebar's right edge in Hebrew too (the sidebar is at the left).
    assert '[dir="rtl"] .q-drawer:not(.q-drawer--mini) .nav-rail-row { justify-content: flex-start; }' in css


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
    # domains, authors, works, material_exclude, text x3, criteria (the date, script
    # and measurement fields folded into filter_criteria: the catalogue criteria).
    assert len(names) >= 8 and 'filter_criteria' in names, names
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
                            'results-export-group', 'results-more-btn', 'tb-hidden', 'density-toggle',
                            'results-chip-break', 'results-count', 'has-selection'],
    'web/pages/search_results.py': ['pagination-placeholder', 'results-list', 'result-card', 'result-snippet',
                                    'result-shelfmark', 'result-actions', 'result-head', 'result-badges',
                                    'result-title', 'result-index', 'expand-row', 'expand-thumb', 'expand-text',
                                    'expand-actions', 'adv-info-bar', 'adv-content', 'adv-tabs', 'adv-text-col',
                                    'adv-image-col', 'adv-info', 'adv-image-adjust', 'adv-actions-card', 'adv-scroll'],
    'web/auth_state.py': ['phone-icon-only', 'auth-account-btn', 'auth-menu-name'],
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


def _media_rules(css: str, query: str, at: str = '@media') -> str:
    """The text of every ``<at> <query> {`` block (``@media`` or ``@container``)."""
    blocks = []
    for m in re.finditer(r'%s %s \{' % (re.escape(at), re.escape(query)), css):
        depth, i = 1, m.end()
        while depth and i < len(css):
            depth += {'{': 1, '}': -1}.get(css[i], 0)
            i += 1
        blocks.append(css[m.end():i])
    assert blocks, f'no {at} {query} block'
    return '\n'.join(blocks)


def _top_level(css: str) -> str:
    """The CSS outside every @-rule block: the rules that apply at every width."""
    kept, last = [], 0
    for m in re.finditer(r'@[a-z-]+[^{;]*\{', css):
        if m.start() < last:
            continue  # inside a block already cut
        depth, i = 1, m.end()
        while depth and i < len(css):
            depth += {'{': 1, '}': -1}.get(css[i], 0)
            i += 1
        kept.append(css[last:m.start()])
        last = i
    kept.append(css[last:])
    return ''.join(kept)


def test_the_header_search_box_narrows_then_folds_behind_a_button():
    """The header's go-to box (2026-11-04): a field in the bar, or a search button that opens
    the field across the bar. It gave way at a fixed 1023px, and on a phone always, with room to
    spare (owner, 2026-11-11); now the search group takes the bar's free space as a size
    container, and the field shows while that is at least 150px, narrowing with it; with less,
    the button. The logo text gives its room to the box below 826px. Open, the GROUP covers the
    bar and the field fills it: a placed field would open inside the narrow group in browsers that
    gave size containers layout containment (Codex, round 3). Measured, signed out, Hebrew and
    English, 320-1920px: the field from 440px, at today's widths from 1024px, the button below
    440px; open, the field covers the whole bar; "/" opens or focuses it."""
    main = _read('web/main.py')
    for cls in ("classes('items-center header-search')", "classes('items-center header-search-field')",
                "classes('header-search-toggle')", "classes('header-search-close')"):
        assert cls in main, cls
    css = _read('web/static/common.css')
    top = _top_level(css)
    assert '.q-header {' in top and '@media' not in top and '@container' not in top
    # The button is the default, at every width: no room for the field.
    assert '\n.header-search-field { display: none !important; }\n' in top
    # Open: the group covers the bar, the field fills it, the close button replaces the toggle.
    for rule in ('.header-search.is-open {\n    position: absolute; inset: 0; z-index: 10; padding: 0 8px; '
                 'background: var(--bg-header);\n}',
                 '.header-search.is-open .header-search-field { display: flex !important; flex: 1 1 auto; gap: 4px; }',
                 '.header-search.is-open .header-search-input { flex: 1 1 auto; width: auto; }',
                 '.header-search.is-open .header-search-close { display: inline-flex !important; }',
                 '.header-search.is-open .header-search-toggle { display: none !important; }',
                 '.header-search-input input { color: #fff !important; text-overflow: ellipsis; }'):
        assert rule in top, rule
    # No placed field: that is the containing-block trap.
    assert not re.search(r'\.header-search-field \{[^}]*position: absolute', css)
    # No fixed 1023/1024px switch any more.
    assert not re.search(r'@media \((max-width: 1023|min-width: 1024)px\) \{\s*\.header-search', css)
    # The group at every width; on a phone its button is 36px, and the phone rule that lets the
    # groups shrink leaves it alone.
    assert ('\n.app-header > .header-search {\n    flex: 1 1 0; min-width: 42px; justify-content: center; '
            'container: header-search / inline-size;\n}\n') in top
    assert '.app-header .header-search-toggle { min-width: 36px !important; padding: 4px !important; }' in _phone_rules(css)
    assert '.app-header > .header-search { min-width: 36px; }' in _phone_rules(css)
    assert '.app-header > .nicegui-row:not(.header-search) { min-width: 0; }' in _phone_rules(css)
    assert '.app-header > .nicegui-row { min-width: 0; }' not in css
    # The logo text gives way to the box below 826px.
    assert '.header-logo-text { display: none !important; }' in _media_rules(css, '(max-width: 825px)')
    # The Sign in label goes on a phone, its icon above: exact complements, no 640.5px gap.
    assert '.phone-icon-only .q-btn__content > .q-icon { display: none; }' in _media_rules(
        css, 'not all and (max-width: 640px)')
    field = _media_rules(css, 'header-search (min-width: 150px)', at='@container')
    for rule in ('.header-search-toggle { display: none !important; }',
                 '.header-search .header-search-field { display: flex !important; }',
                 '.header-search-input { width: min(clamp(220px, 24vw, 320px), 100cqi); }'):
        assert rule in field, rule
    assert 'is-open' not in field  # open is the same at every room: the group covers the bar
    phone = _phone_rules(css)
    assert '.app-header > .nicegui-row { flex-wrap: nowrap !important; gap: 4px !important; }' in phone


def test_the_header_is_one_row_above_640px():
    """Owner's screenshot, 2026-11-11: from 641px until the groups fit (about 700px; a browser
    at 200% on a 1360px window is 680px wide) the three header groups were wrap rows, each
    wrapped inside itself, and the second lines hung over the page under the 64px bar.

    The button groups keep one line; the logo text gives way (the next test: hidden below 826px).
    Above that, should the bar be short of room, the logo LINK (web/client_nav.py's `<a>`) wraps
    its text onto a second line that its 28px cap -- the text's own height -- hides. The clip is
    the link's own: an element's overflow never cuts its own focus ring, and nothing around the
    link clips (a `display: contents` logo had no box to draw one). The block is the phone
    block's exact complement, so no fractional width under zoom falls between them (Codex,
    round 1). Measured after the change, 320-1920px wide and 350-1000px high, Hebrew and
    English: one row, nothing outside the bar, spacing unchanged where the text shows."""
    main = _read('web/main.py')
    for cls in ("with ui.row().classes('items-center gap-4 header-left') as section:",
                "with make_link(ui.element('a').classes('row no-wrap items-center gap-3 cursor-pointer "
                "header-logo-link'), '/'):",
                "with ui.column().classes('header-logo-text justify-center').style('gap: 2px;'):",
                "ui.label('Dicta Genizah Search').classes('font-bold text-white tracking-wide')"
                ".style('font-size: 15px; line-height: 1;')",
                "ui.label('אתר הגניזה מבית דיקטה').classes('text-white/60').style('font-size: 11px; line-height: 1;')",
                "with ui.row().classes('items-center gap-2 sm:gap-4 header-right') as section:"):
        assert cls in main, cls
    css = _read('web/static/common.css')
    assert '.header-logo-text {\n    flex-shrink: 0 !important;\n    white-space: nowrap !important;\n}' in css
    # No header rule in the old 641px blocks, which miss fractional widths (Codex, round 2).
    assert '.app-header' not in _media_rules(css, '(min-width: 641px)')
    rules = _media_rules(css, 'not all and (max-width: 640px)')
    assert '.app-header > .header-right { flex: none; flex-wrap: nowrap !important; }' in rules
    assert '.app-header > .header-left { flex: 0 1 auto; min-width: 0; flex-wrap: nowrap !important; }' in rules
    link = rules[rules.index('.app-header .header-logo-link {'):]
    link = link[:link.index('}')]
    # 28px = 15px + 11px at line-height 1 + the 2px gap (the labels above).
    for decl in ('flex: 0 1 auto;', 'min-width: 0;', 'flex-wrap: wrap !important;', 'max-height: 28px;',
                 'overflow: hidden;', 'align-content: flex-start;'):
        assert decl in link, decl
    # Nothing around the link clips, and the link keeps its box.
    assert 'display: contents' not in rules
    left = rules[rules.index('.app-header > .header-left {'):]
    assert 'overflow' not in left[:left.index('}')]


def test_the_search_box_fits_the_card_on_a_phone():
    """The query column's 320px minimum (``min-w-80``) was wider than the card's
    inside on a phone, so the box and the Hebrew keys under it ran off the card
    (2026-11-15); below 640px it takes its own line at the card's width."""
    assert "ui.column().classes('flex-grow min-w-80 gap-1') as query_column" in _read('web/pages/search.py')
    rules = _phone_rules(_read('web/static/common.css'))
    assert '.search-main-row > .min-w-80 { min-width: 0 !important; flex-basis: 100%; }' in rules


def test_the_result_filters_are_never_off_screen_and_start_their_own_line_on_a_phone():
    """Owner, 2026-11-16: a reader must see that the list is filtered. Under 1024px the count,
    Reset filters and the chips wrap rather than scroll sideways; on a phone the chips start a
    line of their own (the break shows there only); a hidden chip takes no space anywhere."""
    css = _read('web/static/common.css')
    narrow = _media_rules(css, '(max-width: 1023px)')
    group = narrow[narrow.index('.results-count-group {'):]
    group = group[:group.index('}')]
    assert 'flex-wrap: wrap' in group and 'overflow-x' not in group, group
    # The count wraps rather than push the page wider than the screen.
    assert '.results-count { flex: 1 1 auto; min-width: 0; }' in narrow
    assert '.results-count-group > * { flex-shrink: 0; }' not in narrow
    phone = _media_rules(css, '(max-width: 640px)')
    assert '.results-chip-break { display: block !important; flex-basis: 100%;' in phone
    top = _top_level(css)
    assert '.results-chip-break { display: none; }' in top
    assert '.tb-hidden { display: none !important; }' in top
    search = _read('web/pages/search.py')
    # The count has the first line; Reset filters and the chips start the next.
    assert search.index("classes('results-chip-break')") > search.index("results-count')")
    assert search.index("classes('results-chip-break')") < search.index('reset_filters_btn = ui.button(')


def test_a_selection_does_not_crowd_the_count_on_a_phone():
    """Selecting results shows "N selected" and the two bulk actions; on a phone the bulk
    actions join the export icons in the corner, so the count leaves them room, and
    "N selected" starts the chips' line instead of following the count (Codex on #26)."""
    css = _read('web/static/common.css')
    phone = _media_rules(css, '(max-width: 640px)')
    assert '.results-count { max-width: calc(100% - 150px); }' in phone
    assert '.results-header.has-selection .results-count { max-width: calc(100% - 214px); }' in phone
    search = _read('web/pages/search.py')
    counter = search.index('selection_counter = ui.label(')
    assert search.index("classes('results-chip-break')") < counter < search.index(
        'reset_filters_btn = ui.button('), 'the selection counter belongs after the chip break'
    assert search.index("ui.row().classes('gap-2 results-bulk-actions tb-hidden')") > search.index(
        "classes('gap-2 results-export-group')")


def test_the_filter_chips_are_compact_with_room_before_the_label():
    """Owner, 2026-11-16: compact chips, the label not crowding the edge it starts from. Logical
    sides, so the room is on the right in Hebrew and on the left in English; the rule is more
    specific than the narrow-screen 34px button height."""
    css = _read('web/static/common.css')
    top = _top_level(css)
    rule = top[top.index('.results-header .q-btn.filter-chip {'):]
    rule = rule[:rule.index('}')]
    assert 'min-height: 28px !important' in rule and 'padding-inline: 12px 6px !important' in rule, rule
    assert 'margin-inline-start: 2px !important' in top
    assert "btn.classes(add='filter-chip')" in _read('web/pages/search.py')
