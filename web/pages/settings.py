# -*- coding: utf-8 -*-
"""
Settings Page - Dicta Genizah Search

General settings for search behavior, display preferences, and Lab Mode configuration.
"""

import logging
from nicegui import ui, app
from web.state import state
from web.translations import tr
from web.components.typography import h1, h3

logger = logging.getLogger(__name__)


def create_settings_page():
    """Create the Settings page."""

    # 2026-05-12 Codex 3rd-pass HIGH: route page-render storage reads through
    # safe_user_get so a prune_user_storage race doesn't 500 /settings.
    from web.safe_storage import safe_user_get as _safe_get

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
                                app.storage.user['theme'] = theme
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
                                app.storage.user['results_per_page'] = rpp_select.value

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
                                app.storage.user['default_search_mode'] = mode_select.value

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
                                app.storage.user['default_gap'] = int(gap_input.value) if gap_input.value else 0

                            gap_input.on('update:model-value', change_gap)

                    # Lab Mode default toggle
                    ui.separator().classes('my-2')
                    lab_default = _safe_get('lab_mode_default', False)
                    lab_switch = ui.switch(tr('Enable Lab Mode by default'), value=lab_default)

                    def toggle_lab():
                        app.storage.user['lab_mode_default'] = lab_switch.value

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
                        app.storage.user['session_persistence_enabled'] = persist_switch.value

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
                            app.storage.user['search_history_limit'] = int(history_limit_input.value) if history_limit_input.value else 20

                        history_limit_input.on('update:model-value', change_history_limit)

                    ui.label(tr('Maximum number of past searches to remember per search type')).classes('text-xs mr-10').style('color: var(--text-muted);')

            # === Variant Settings Tab ===
            with ui.tab_panel('variants'):
                # Get lab settings
                lab_settings = None
                if state.lab_engine:
                    try:
                        lab_settings = state.lab_engine.settings
                    except Exception:
                        pass  # Browser storage operation failed; preference not persisted

                if lab_settings:
                    with ui.column().classes('w-full gap-4'):
                        # Options row
                        with ui.row().classes('w-full gap-6 flex-wrap'):
                            # Min word length
                            with ui.column().classes('gap-1'):
                                ui.label(tr('Limit Short Words (≤N chars)')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                                min_len_val = getattr(lab_settings, 'variant_min_word_len', 2)
                                variant_min_len = ui.number(
                                    value=min_len_val, min=1, max=5
                                ).props('outlined dense').classes('w-20')

                                def apply_min_len():
                                    lab_settings.variant_min_word_len = int(variant_min_len.value)
                                    if hasattr(lab_settings, 'save'):
                                        lab_settings.save()
                                variant_min_len.on('update:model-value', apply_min_len)

                            # Max changes
                            with ui.column().classes('gap-1'):
                                ui.label(tr('Max Changes per Word')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                                max_changes_val = getattr(lab_settings, 'variant_max_changes', 2)
                                variant_max_changes = ui.number(
                                    value=max_changes_val, min=1, max=3
                                ).props('outlined dense').classes('w-20')

                                def apply_max_changes():
                                    lab_settings.variant_max_changes = int(variant_max_changes.value)
                                    if hasattr(lab_settings, 'save'):
                                        lab_settings.save()
                                variant_max_changes.on('update:model-value', apply_max_changes)

                        # Toggles
                        ui.separator().classes('my-2')

                        aggressive_val = getattr(lab_settings, 'variant_aggressive', False)
                        variant_aggressive = ui.switch(tr('Aggressive Mode (ignore word length limits)'), value=aggressive_val)
                        ui.label(tr('Apply max changes to all words regardless of length')).classes('text-xs mr-10').style('color: var(--text-muted);')

                        def apply_aggressive():
                            lab_settings.variant_aggressive = variant_aggressive.value
                            if hasattr(lab_settings, 'save'):
                                lab_settings.save()
                        variant_aggressive.on('update:model-value', apply_aggressive)

                        use_slider_val = getattr(lab_settings, 'variant_use_slider', False)
                        variant_use_slider = ui.switch(tr('Use slider instead of preset buttons (Basic, Extended, Maximum)'), value=use_slider_val).classes('mt-2')
                        ui.label(tr('When enabled, shows a slider in the search bar instead of preset buttons')).classes('text-xs mr-10').style('color: var(--text-muted);')

                        def apply_use_slider():
                            lab_settings.variant_use_slider = variant_use_slider.value
                            if hasattr(lab_settings, 'save'):
                                lab_settings.save()
                            ui.notify(tr('Refresh page to see changes'), type='info')
                        variant_use_slider.on('update:model-value', apply_use_slider)

                        # Custom Variants
                        ui.separator().classes('my-2')
                        with ui.expansion(tr('Custom Variant Pairs'), icon='edit').classes('w-full'):
                            ui.label(tr('Add character pairs that should be treated as interchangeable (one per line: ק=א)')).classes('text-xs mb-2').style('color: var(--text-muted);')
                            custom_variants = getattr(lab_settings, 'custom_variants', {})
                            existing_text = '\n'.join(custom_variants.keys()) if custom_variants else ''
                            custom_textarea = ui.textarea(
                                placeholder='ק=א\nכו=מ\nב=פ',
                                value=existing_text
                            ).classes('w-full').props('outlined rows=4')

                            def apply_custom_variants():
                                try:
                                    text = custom_textarea.value.strip()
                                    custom = {}
                                    if text:
                                        for line in text.split('\n'):
                                            line = line.strip()
                                            if '=' in line:
                                                custom[line] = True
                                    lab_settings.custom_variants = custom
                                    if hasattr(lab_settings, 'save'):
                                        lab_settings.save()
                                except Exception as e:
                                    logger.error("Custom variants error: %s", e)
                            custom_textarea.on('blur', apply_custom_variants)

                else:
                    with ui.column().classes('items-center py-6'):
                        ui.icon('spellcheck').classes('text-4xl').style('color: var(--text-muted);')
                        ui.label(tr('Lab Engine not initialized')).classes('mt-2 text-sm').style('color: var(--text-muted);')

            # === Lab Mode Tab ===
            with ui.tab_panel('lab'):
                settings = None
                if state.lab_engine:
                    try:
                        settings = state.lab_engine.settings
                    except Exception:
                        pass  # Filter operation failed; continue with defaults

                if settings:
                    with ui.column().classes('w-full gap-4'):
                        ui.label(tr('Parameters for composition/parallel search using the Shmidman-Koppel-Porat algorithm.')).classes('text-xs').style('color: var(--text-muted);')

                        with ui.row().classes('w-full gap-6 flex-wrap'):
                            # Candidate Limit
                            with ui.column().classes('gap-1'):
                                ui.label(tr('Candidate Limit')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                                candidate_val = getattr(settings, 'candidate_limit', 5000)
                                candidate_limit = ui.number(
                                    value=candidate_val, min=100, max=50000, step=100
                                ).props('outlined dense').classes('w-28')

                                def apply_candidate_limit():
                                    if hasattr(settings, 'candidate_limit'):
                                        settings.candidate_limit = int(candidate_limit.value)
                                        if hasattr(settings, 'save'):
                                            settings.save()
                                candidate_limit.on('update:model-value', apply_candidate_limit)

                            # Display Limit
                            with ui.column().classes('gap-1'):
                                ui.label(tr('Display Limit')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                                display_val = getattr(settings, 'lab_display_limit', getattr(settings, 'display_limit', 500))
                                display_limit = ui.number(
                                    value=display_val, min=50, max=2000, step=50
                                ).props('outlined dense').classes('w-28')

                                def apply_display_limit():
                                    if hasattr(settings, 'lab_display_limit'):
                                        settings.lab_display_limit = int(display_limit.value)
                                    elif hasattr(settings, 'display_limit'):
                                        settings.display_limit = int(display_limit.value)
                                    if hasattr(settings, 'save'):
                                        settings.save()
                                display_limit.on('update:model-value', apply_display_limit)

                            # Chunk Size
                            with ui.column().classes('gap-1'):
                                ui.label(tr('Default Chunk Size')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                                chunk_val = getattr(settings, 'comp_chunk_size', getattr(settings, 'chunk_size', 5))
                                chunk_size = ui.number(
                                    value=chunk_val, min=2, max=15
                                ).props('outlined dense').classes('w-20')

                                def apply_chunk_size():
                                    if hasattr(settings, 'comp_chunk_size'):
                                        settings.comp_chunk_size = int(chunk_size.value)
                                    elif hasattr(settings, 'chunk_size'):
                                        settings.chunk_size = int(chunk_size.value)
                                    if hasattr(settings, 'save'):
                                        settings.save()
                                chunk_size.on('update:model-value', apply_chunk_size)

                            # Min Score
                            with ui.column().classes('gap-1'):
                                ui.label(tr('Min Score')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                                score_val = getattr(settings, 'comp_min_score', getattr(settings, 'min_score', 30))
                                min_score = ui.number(
                                    value=score_val, min=10, max=100
                                ).props('outlined dense').classes('w-20')

                                def apply_min_score():
                                    if hasattr(settings, 'comp_min_score'):
                                        settings.comp_min_score = int(min_score.value)
                                    if hasattr(settings, 'save'):
                                        settings.save()
                                min_score.on('update:model-value', apply_min_score)

                else:
                    with ui.column().classes('items-center py-6'):
                        ui.icon('science').classes('text-4xl').style('color: var(--text-muted);')
                        ui.label(tr('Lab Engine not initialized')).classes('mt-2 text-sm').style('color: var(--text-muted);')

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
                        # VS DB download deferred — nginx proxy_max_temp_file_size blocks 1.3GB response
                        # ui.button(
                        #     tr('Download full visual similarity database'), icon='download',
                        #     on_click=lambda: ui.download('/api/visual_similarity_db', 'visual_similarity.db'),
                        # ).props('flat dense size=sm no-caps').classes('text-xs')

                    ui.separator().classes('my-2')

                    ui.markdown('''
                    **Dicta Genizah Search** · *Data: MiDRASH Project (Friedberg Genizah Project)*
                    ''').classes('text-xs').style('color: var(--text-muted);')
