# -*- coding: utf-8 -*-
"""
Settings Page - Dicta Genizah Search

General settings for search behavior, display preferences, and Lab Mode configuration.
"""

import logging
from nicegui import ui
from web.state import state
from web.translations import tr
from web.components.typography import h1, h3

logger = logging.getLogger(__name__)


def create_settings_page():
    """Create the Settings page."""

    # 2026-05-12 Codex 3rd-pass HIGH: route page-render storage reads through
    # safe_user_get so a prune_user_storage race doesn't 500 /settings.
    # Phase 87 FOUND-02: writes also routed through safe_user_set.
    from web.safe_storage import safe_user_get as _safe_get, safe_user_set as _safe_set
    from web import variant_preferences

    with ui.column().classes('w-full max-w-4xl mx-auto gap-2 fade-in p-4'):

        # === Page Header ===
        with ui.row().classes('items-center gap-2 mb-2'):
            ui.icon('settings').classes('text-2xl').style('color: var(--primary-600);')
            h1(tr('Settings'), classes='text-xl font-bold', style='color: var(--text-primary);')

        # === Tabs for Settings Categories ===
        with ui.tabs().classes('w-full') as tabs:
            tab_general = ui.tab('general', label=tr('General'), icon='tune')
            tab_variants = ui.tab('variants', label=tr('Variants'), icon='spellcheck')
            tab_lab = ui.tab('lab', label=tr('Lab Mode'), icon='science')
            tab_status = ui.tab('status', label=tr('Status'), icon='info')

        with ui.tab_panels(tabs, value='general').classes('w-full'):

            # === General Settings Tab ===
            with ui.tab_panel('general'):
                with ui.column().classes('w-full gap-4'):

                    # Display Settings Row
                    with ui.row().classes('w-full gap-6 flex-wrap items-start'):
                        # Theme
                        with ui.column().classes('gap-1'):
                            ui.label(tr('Theme')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                            current_theme = _safe_get('theme', 'light')
                            theme_select = ui.select(
                                {
                                    'light': tr('Light'),
                                    'parchment': tr('Parchment'),
                                    'dark': tr('Dark'),
                                },
                                value=current_theme
                            ).classes('w-40').props('outlined dense')

                            def change_theme():
                                theme = theme_select.value
                                _safe_set('theme', theme)
                                ui.run_javascript(f'document.body.setAttribute("data-theme", "{theme}")')

                            theme_select.on('update:model-value', change_theme)

                        # Results per page
                        with ui.column().classes('gap-1'):
                            ui.label(tr('Results per page')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                            results_per_page = _safe_get('results_per_page', 50)
                            rpp_select = ui.select(
                                {25: '25', 50: '50', 100: '100', 200: '200'},
                                value=results_per_page
                            ).classes('w-28').props('outlined dense')

                            def change_rpp():
                                _safe_set('results_per_page', rpp_select.value)

                            rpp_select.on('update:model-value', change_rpp)

                        # Default search mode
                        with ui.column().classes('gap-1'):
                            ui.label(tr('Default search mode')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                            default_mode = _safe_get('default_search_mode', 'exact')
                            mode_select = ui.select(
                                {
                                    'exact': tr('Exact'),
                                    'variants': tr('Variants'),
                                    'fuzzy': tr('Fuzzy'),
                                },
                                value=default_mode
                            ).classes('w-36').props('outlined dense')

                            def change_mode():
                                _safe_set('default_search_mode', mode_select.value)

                            mode_select.on('update:model-value', change_mode)

                        # Default gap
                        with ui.column().classes('gap-1'):
                            ui.label(tr('Default word gap')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                            default_gap = _safe_get('default_gap', 0)
                            gap_input = ui.number(
                                value=default_gap,
                                min=0,
                                max=10
                            ).classes('w-20').props('outlined dense')

                            def change_gap():
                                _safe_set('default_gap', int(gap_input.value) if gap_input.value else 0)

                            gap_input.on('update:model-value', change_gap)

                    # Lab Mode default toggle
                    ui.separator().classes('my-2')
                    lab_default = _safe_get('lab_mode_default', False)
                    lab_switch = ui.switch(tr('Enable Lab Mode by default'), value=lab_default)

                    def toggle_lab():
                        _safe_set('lab_mode_default', lab_switch.value)

                    lab_switch.on('update:model-value', toggle_lab)

                    # Session Persistence Settings
                    ui.separator().classes('my-2')
                    h3(tr('Session Persistence'), classes='text-base font-semibold', style='color: var(--text-primary);')
                    ui.label(tr('Control how search state is saved between sessions')).classes('text-xs').style('color: var(--text-muted);')

                    # Enable/disable toggle
                    persist_enabled = _safe_get('session_persistence_enabled', True)
                    persist_switch = ui.switch(tr('Save search state between sessions'), value=persist_enabled)
                    ui.label(tr('When enabled, your search results, exclusions, and filters are preserved when you return')).classes('text-xs mr-10').style('color: var(--text-muted);')

                    def toggle_persistence():
                        _safe_set('session_persistence_enabled', persist_switch.value)

                    persist_switch.on('update:model-value', toggle_persistence)

                    # History limit
                    with ui.row().classes('items-center gap-2 mt-2'):
                        ui.label(tr('Search history entries')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                        history_limit = _safe_get('search_history_limit', 20)
                        history_limit_input = ui.number(
                            value=history_limit,
                            min=5,
                            max=100
                        ).classes('w-20').props('outlined dense')

                        def change_history_limit():
                            _safe_set('search_history_limit', int(history_limit_input.value) if history_limit_input.value else 20)

                        history_limit_input.on('update:model-value', change_history_limit)

                    ui.label(tr('Maximum number of past searches to remember per search type')).classes('text-xs mr-10').style('color: var(--text-muted);')

            # === Variant Settings Tab ===
            # Kept per visitor (web.variant_preferences) and sent with that
            # visitor's searches; the server's settings object is shared by every
            # visitor, so nothing here writes it or its file.
            with ui.tab_panel('variants'):
                with ui.column().classes('w-full gap-4'):
                    # Options row
                    with ui.row().classes('w-full gap-6 flex-wrap'):
                        # Min word length
                        with ui.column().classes('gap-1'):
                            ui.label(tr('Limit Short Words (≤N chars)')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                            variant_min_len = ui.number(
                                value=variant_preferences.get('variant_min_word_len'), min=1, max=5
                            ).props('outlined dense').classes('w-20')

                            def apply_min_len():
                                if variant_min_len.value is not None:
                                    variant_preferences.set('variant_min_word_len', int(variant_min_len.value))
                            variant_min_len.on('update:model-value', apply_min_len)

                        # Max changes per level: the same preference as the search bar's Num Changes.
                        with ui.column().classes('gap-1'):
                            ui.label(tr('Max Changes per Word')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                            with ui.row().classes('items-center gap-2'):
                                for _level, _label in (('basic', tr('Basic')), ('extended', tr('Extended')),
                                                       ('maximum', tr('Maximum'))):
                                    ui.label(_label).classes('text-xs').style('color: var(--text-muted);')
                                    _changes = ui.number(
                                        value=variant_preferences.max_changes(_level), min=1, max=3
                                    ).props('outlined dense').classes('w-16').mark(f'max-changes-{_level}')

                                    def apply_max_changes(_e=None, _lvl=_level, _input=_changes):
                                        if _input.value is not None:
                                            variant_preferences.set_max_changes(_lvl, int(_input.value))
                                    _changes.on('update:model-value', apply_max_changes)

                    # Toggles
                    ui.separator().classes('my-2')

                    variant_aggressive = ui.switch(tr('Aggressive Mode (ignore word length limits)'),
                                                   value=variant_preferences.get('variant_aggressive'))
                    ui.label(tr('Apply max changes to all words regardless of length')).classes('text-xs mr-10').style('color: var(--text-muted);')

                    def apply_aggressive():
                        variant_preferences.set('variant_aggressive', bool(variant_aggressive.value))
                    variant_aggressive.on('update:model-value', apply_aggressive)

                    variant_use_slider = ui.switch(tr('Use slider instead of preset buttons (Basic, Extended, Maximum)'),
                                                   value=variant_preferences.get('variant_use_slider')).classes('mt-2')
                    ui.label(tr('When enabled, shows a slider in the search bar instead of preset buttons')).classes('text-xs mr-10').style('color: var(--text-muted);')

                    def apply_use_slider():
                        variant_preferences.set('variant_use_slider', bool(variant_use_slider.value))
                        ui.notify(tr('Refresh page to see changes'), type='info')
                    variant_use_slider.on('update:model-value', apply_use_slider)

                    # Custom Variants
                    ui.separator().classes('my-2')
                    with ui.expansion(tr('Custom Variant Pairs'), icon='edit').classes('w-full'):
                        ui.label(tr('Add character pairs that should be treated as interchangeable (one per line: ק=א)')).classes('text-xs mb-2').style('color: var(--text-muted);')
                        custom_variants = variant_preferences.get('custom_variants')
                        existing_text = '\n'.join(custom_variants.keys()) if custom_variants else ''
                        custom_textarea = ui.textarea(
                            placeholder='ק=א\nכו=מ\nב=פ',
                            value=existing_text
                        ).classes('w-full').props('outlined rows=4')

                        def apply_custom_variants():
                            try:
                                text = (custom_textarea.value or '').strip()
                                custom = {}
                                if text:
                                    for line in text.split('\n'):
                                        line = line.strip()
                                        if '=' in line:
                                            custom[line] = True
                                variant_preferences.set('custom_variants', custom)
                            except Exception as e:
                                logger.error("Custom variants error: %s", e)
                        custom_textarea.on('blur', apply_custom_variants)

            # === Lab Mode Tab ===
            # Only the minimum score is read by the website's Lab searches; it is
            # kept per visitor like the variant preferences.
            with ui.tab_panel('lab'):
                with ui.column().classes('w-full gap-4'):
                    ui.label(tr('Parameters for composition/parallel search using the Shmidman-Koppel-Porat algorithm.')).classes('text-xs').style('color: var(--text-muted);')

                    with ui.row().classes('w-full gap-6 flex-wrap'):
                        # Min Score
                        with ui.column().classes('gap-1'):
                            ui.label(tr('Min Score')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                            min_score = ui.number(
                                value=variant_preferences.get('comp_min_score'), min=10, max=100
                            ).props('outlined dense').classes('w-20')

                            def apply_min_score():
                                if min_score.value is not None:
                                    variant_preferences.set('comp_min_score', int(min_score.value))
                            min_score.on('update:model-value', apply_min_score)

            # === Status Tab ===
            with ui.tab_panel('status'):
                with ui.column().classes('w-full gap-4'):
                    with ui.row().classes('gap-6 flex-wrap'):
                        # Main Index Status
                        index_active = state.searcher and state.searcher.index
                        with ui.row().classes('items-center gap-2'):
                            ui.label(tr('Search Index')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                            ui.badge(tr('Active') if index_active else tr('Not loaded'),
                                     color='green' if index_active else 'red')

                        # Lab Index Status
                        lab_active = state.lab_engine and getattr(state.lab_engine, 'lab_index', None)
                        with ui.row().classes('items-center gap-2'):
                            ui.label(tr('Lab Index')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                            ui.badge(tr('Active') if lab_active else tr('Not loaded'),
                                     color='green' if lab_active else 'gray')

                        # Document count
                        if state.searcher:
                            try:
                                searcher = getattr(state.searcher, 'searcher', None)
                                if searcher:
                                    doc_count = searcher.num_docs
                                    with ui.row().classes('items-center gap-2'):
                                        ui.label(tr('Documents')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                                        ui.label(f'{doc_count:,}').style('color: var(--text-primary);')
                            except Exception:
                                pass  # Doc count display failed; settings page still usable

                    ui.separator().classes('my-2')

                    # Visual Similarity Database section
                    with ui.row().classes('items-center gap-4'):
                        ui.label(tr('Visual Similarity Database')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                        try:
                            from shared.visual_similarity_service import get_vs_service
                            vs_svc = get_vs_service(thread_safe=True)
                            if vs_svc.is_available():
                                vs_meta = vs_svc.get_db_version()
                                pair_count = vs_meta.get('pair_count', '?')
                                ms_count = vs_meta.get('manuscript_count', '?')
                                ui.badge(f'{pair_count} pairs / {ms_count} manuscripts', color='green')
                            else:
                                ui.badge(tr('Not loaded'), color='gray')
                        except Exception:
                            ui.badge(tr('Not loaded'), color='gray')  # Visual similarity lookup failed; continue
                        # The server route /api/visual_similarity_db was removed on 2026-09-25; any future download needs a new, rate-limited delivery path (SEED-005), so do not re-enable this as is.
                        # VS DB download deferred — nginx proxy_max_temp_file_size blocks 1.3GB response
                        # ui.button(
                        #     tr('Download full visual similarity database'), icon='download',
                        #     on_click=lambda: ui.download('/api/visual_similarity_db', 'visual_similarity.db'),
                        # ).props('flat dense size=sm no-caps').classes('text-xs')

                    ui.separator().classes('my-2')

                    ui.markdown('''
                    **Dicta Genizah Search** · *Data: MiDRASH Project (Friedberg Genizah Project)*
                    ''').classes('text-xs').style('color: var(--text-muted);')
