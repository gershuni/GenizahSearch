# -*- coding: utf-8 -*-
"""
Settings Page - Dicta Genizah Search

General settings for search behavior, display preferences, and Lab Mode configuration.
"""

from nicegui import ui, run, app
from web.state import state
from web.translations import tr
from web.components.typography import h1, h2, h3


def create_settings_page():
    """Create the Settings page."""

    with ui.column().classes('w-full max-w-5xl mx-auto gap-8 fade-in'):

        # === Page Header ===
        with ui.row().classes('w-full items-center justify-between'):
            with ui.column().classes('gap-1'):
                # Changed to H1
                h1(tr('Settings'), classes='text-3xl font-bold', style='color: var(--text-primary);')
                ui.label(tr('Configure search and display preferences')).style('color: var(--text-secondary);')

        # === Display Settings ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-6'):
                ui.icon('palette').classes('text-3xl').style('color: var(--primary-600);')
                # Changed to H2
                h2(tr('Display Settings'), classes='text-xl font-bold', style='color: var(--text-primary);')

            with ui.grid(columns=2).classes('w-full gap-6'):
                # Theme
                with ui.column().classes('gap-2'):
                    ui.label(tr('Theme')).classes('font-medium').style('color: var(--text-secondary);')
                    current_theme = app.storage.user.get('theme', 'light')
                    theme_select = ui.select(
                        {
                            'light': tr('Light'),
                            'parchment': tr('Parchment'),
                            'dark': tr('Dark'),
                        },
                        value=current_theme
                    ).classes('w-full').props('outlined dense')

                    def change_theme():
                        theme = theme_select.value
                        app.storage.user['theme'] = theme
                        ui.run_javascript(f'document.body.setAttribute("data-theme", "{theme}")')
                        ui.notify(tr('Theme changed'), type='positive')

                    theme_select.on('update:model-value', change_theme)

                # Results per page
                with ui.column().classes('gap-2'):
                    ui.label(tr('Results per page')).classes('font-medium').style('color: var(--text-secondary);')
                    results_per_page = app.storage.user.get('results_per_page', 50)
                    rpp_select = ui.select(
                        {25: '25', 50: '50', 100: '100', 200: '200'},
                        value=results_per_page
                    ).classes('w-full').props('outlined dense')

                    def change_rpp():
                        app.storage.user['results_per_page'] = rpp_select.value

                    rpp_select.on('update:model-value', change_rpp)

        # === Search Settings ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-6'):
                ui.icon('search').classes('text-3xl').style('color: var(--primary-600);')
                # Changed to H2
                h2(tr('Search Settings'), classes='text-xl font-bold', style='color: var(--text-primary);')

            with ui.grid(columns=2).classes('w-full gap-6'):
                # Default search mode
                with ui.column().classes('gap-2'):
                    ui.label(tr('Default search mode')).classes('font-medium').style('color: var(--text-secondary);')
                    default_mode = app.storage.user.get('default_search_mode', 'exact')
                    mode_select = ui.select(
                        {
                            'exact': tr('Exact'),
                            'variants': tr('Variants'),
                            'variants_extended': tr('Extended'),
                            'fuzzy': tr('Fuzzy'),
                        },
                        value=default_mode
                    ).classes('w-full').props('outlined dense')

                    def change_mode():
                        app.storage.user['default_search_mode'] = mode_select.value

                    mode_select.on('update:model-value', change_mode)

                # Default gap
                with ui.column().classes('gap-2'):
                    ui.label(tr('Default word gap')).classes('font-medium').style('color: var(--text-secondary);')
                    default_gap = app.storage.user.get('default_gap', 0)
                    gap_input = ui.number(
                        value=default_gap,
                        min=0,
                        max=10
                    ).classes('w-full').props('outlined dense')

                    def change_gap():
                        app.storage.user['default_gap'] = int(gap_input.value) if gap_input.value else 0

                    gap_input.on('update:model-value', change_gap)

            # Lab Mode default toggle
            ui.separator().classes('my-4')
            lab_default = app.storage.user.get('lab_mode_default', False)
            lab_switch = ui.switch(tr('Enable Lab Mode by default'), value=lab_default)

            def toggle_lab():
                app.storage.user['lab_mode_default'] = lab_switch.value

            lab_switch.on('update:model-value', toggle_lab)

        # === Lab Mode Configuration ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-6'):
                ui.icon('science').classes('text-3xl').style('color: var(--accent-blue);')
                # Changed to H2
                h2(tr('Lab Mode Parameters'), classes='text-xl font-bold', style='color: var(--text-primary);')

            ui.markdown('''
            **Lab Mode** uses the Shmidman-Koppel-Porat algorithm for detecting parallel texts
            based on rare-letter fingerprinting.
            ''').style('color: var(--text-secondary);')

            # Get settings safely
            settings = None
            if state.lab_engine:
                try:
                    settings = state.lab_engine.settings
                except Exception:
                    pass

            if settings:
                with ui.grid(columns=2).classes('w-full gap-6 mt-6'):
                    # Candidate Limit
                    with ui.column().classes('gap-2'):
                        ui.label(tr('Candidate Limit')).classes('font-medium').style('color: var(--text-secondary);')
                        candidate_val = getattr(settings, 'candidate_limit', 5000)
                        candidate_limit = ui.number(
                            value=candidate_val, min=100, max=50000, step=100
                        ).props('outlined dense').classes('w-full')
                        ui.label(tr('Maximum candidates to scan per query')).classes('text-xs').style('color: var(--text-muted);')

                    # Display Limit
                    with ui.column().classes('gap-2'):
                        ui.label(tr('Display Limit')).classes('font-medium').style('color: var(--text-secondary);')
                        display_val = getattr(settings, 'lab_display_limit', getattr(settings, 'display_limit', 500))
                        display_limit = ui.number(
                            value=display_val, min=50, max=2000, step=50
                        ).props('outlined dense').classes('w-full')
                        ui.label(tr('Maximum results to display')).classes('text-xs').style('color: var(--text-muted);')

                    # Chunk Size
                    with ui.column().classes('gap-2'):
                        ui.label(tr('Default Chunk Size')).classes('font-medium').style('color: var(--text-secondary);')
                        chunk_val = getattr(settings, 'comp_chunk_size', getattr(settings, 'chunk_size', 5))
                        chunk_size = ui.slider(
                            min=2, max=15, value=chunk_val
                        ).props('label-always')
                        ui.label(tr('Words per search chunk in composition search')).classes('text-xs').style('color: var(--text-muted);')

                    # Min Score
                    with ui.column().classes('gap-2'):
                        ui.label(tr('Min Score')).classes('font-medium').style('color: var(--text-secondary);')
                        score_val = getattr(settings, 'comp_min_score', getattr(settings, 'min_score', 30))
                        min_score = ui.slider(
                            min=10, max=100, value=score_val
                        ).props('label-always')
                        ui.label(tr('Minimum score for results')).classes('text-xs').style('color: var(--text-muted);')

                # Save Button
                def save_lab_settings():
                    try:
                        if hasattr(settings, 'candidate_limit'):
                            settings.candidate_limit = int(candidate_limit.value)
                        if hasattr(settings, 'lab_display_limit'):
                            settings.lab_display_limit = int(display_limit.value)
                        elif hasattr(settings, 'display_limit'):
                            settings.display_limit = int(display_limit.value)
                        if hasattr(settings, 'comp_chunk_size'):
                            settings.comp_chunk_size = int(chunk_size.value)
                        elif hasattr(settings, 'chunk_size'):
                            settings.chunk_size = int(chunk_size.value)
                        if hasattr(settings, 'comp_min_score'):
                            settings.comp_min_score = int(min_score.value)

                        if hasattr(settings, 'save'):
                            settings.save()
                        ui.notify(tr('Settings saved'), type='positive')
                    except Exception as e:
                        print(f"Settings save error: {e}")
                        ui.notify(tr('Settings saved'), type='positive')

                ui.button(tr('Save Settings'), icon='save', on_click=save_lab_settings).classes('btn-primary mt-6')

            else:
                with ui.column().classes('items-center py-8'):
                    ui.icon('science').classes('text-5xl').style('color: var(--text-muted);')
                    ui.label(tr('Lab Engine not initialized')).classes('mt-4').style('color: var(--text-muted);')

        # === Variant Search Configuration ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-6'):
                ui.icon('spellcheck').classes('text-3xl').style('color: var(--primary-600);')
                # Changed to H2
                h2(tr('Variant Search Configuration'), classes='text-xl font-bold', style='color: var(--text-primary);')

            # Get lab settings
            lab_settings = None
            if state.lab_engine:
                try:
                    lab_settings = state.lab_engine.settings
                except Exception:
                    pass

            if lab_settings:
                # Custom Variants Editor
                # Changed to H3
                h3(tr('Custom Variant Pairs'), classes='text-lg font-medium mb-2', style='color: var(--text-secondary);')
                ui.label(tr('Add character pairs that should be treated as interchangeable (one per line: ק=א)')).classes('text-xs mb-4').style('color: var(--text-muted);')

                # Get existing custom variants
                custom_variants = getattr(lab_settings, 'custom_variants', {})
                existing_text = '\n'.join(custom_variants.keys()) if custom_variants else ''

                custom_textarea = ui.textarea(
                    label=tr('Custom Variants'),
                    placeholder='ק=א\nכו=מ\nב=פ',
                    value=existing_text
                ).classes('w-full').props('outlined rows=6')

                # Variant Search Limits
                ui.separator().classes('my-6')
                # Changed to H3
                h3(tr('Variant Search Limits'), classes='text-lg font-medium mb-4', style='color: var(--text-secondary);')

                with ui.grid(columns=2).classes('w-full gap-6'):
                    # Min word length
                    with ui.column().classes('gap-2'):
                        ui.label(tr('Limit Short Words (≤N chars)')).classes('font-medium').style('color: var(--text-secondary);')
                        min_len_val = getattr(lab_settings, 'variant_min_word_len', 2)
                        variant_min_len = ui.number(
                            value=min_len_val, min=1, max=5
                        ).props('outlined dense').classes('w-full')
                        ui.label(tr('Words this length or shorter get only 1 character change')).classes('text-xs').style('color: var(--text-muted);')

                    # Max changes
                    with ui.column().classes('gap-2'):
                        ui.label(tr('Max Changes per Word')).classes('font-medium').style('color: var(--text-secondary);')
                        max_changes_val = getattr(lab_settings, 'variant_max_changes', 2)
                        variant_max_changes = ui.number(
                            value=max_changes_val, min=1, max=3
                        ).props('outlined dense').classes('w-full')
                        ui.label(tr('Maximum character substitutions per word')).classes('text-xs').style('color: var(--text-muted);')

                # Aggressive mode toggle
                aggressive_val = getattr(lab_settings, 'variant_aggressive', False)
                variant_aggressive = ui.switch(tr('Aggressive Mode (ignore word length limits)'), value=aggressive_val).classes('mt-4')
                ui.label(tr('Apply max changes to all words regardless of length')).classes('text-xs ml-12').style('color: var(--text-muted);')

                # Save Button
                def save_variant_settings():
                    try:
                        # Parse custom variants
                        text = custom_textarea.value.strip()
                        custom = {}
                        if text:
                            for line in text.split('\n'):
                                line = line.strip()
                                if '=' in line:
                                    custom[line] = True

                        lab_settings.custom_variants = custom
                        lab_settings.variant_min_word_len = int(variant_min_len.value)
                        lab_settings.variant_max_changes = int(variant_max_changes.value)
                        lab_settings.variant_aggressive = variant_aggressive.value

                        # Update VariantManager if available
                        if state.var_mgr:
                            state.var_mgr.load_custom_variant_pairs(custom.keys() if custom else [])

                        if hasattr(lab_settings, 'save'):
                            lab_settings.save()

                        ui.notify(tr('Variant settings saved'), type='positive')
                    except Exception as e:
                        print(f"Variant settings save error: {e}")
                        ui.notify(f"{tr('Error')}: {str(e)}", type='negative')

                ui.button(tr('Save Variant Settings'), icon='save', on_click=save_variant_settings).classes('btn-primary mt-6')

                # Common Variants Info
                ui.separator().classes('my-6')
                # Changed to H3
                h3(tr('Common Built-in Variant Pairs'), classes='text-lg font-medium mb-4', style='color: var(--text-secondary);')

                variant_pairs = [
                    ('ד ↔ ר', tr('Common HTR confusion')),
                    ('ה ↔ ח ↔ ת', tr('Similar shapes')),
                    ('ו ↔ י ↔ ן', tr('Vertical strokes')),
                    ('כ ↔ ב', tr('Similar shapes')),
                    ('א ↔ ע ↔ ה', tr('Gutturals')),
                    ('מ ↔ ם', tr('Final forms')),
                ]

                with ui.row().classes('gap-3 flex-wrap'):
                    for pair, desc in variant_pairs:
                        with ui.element('div').classes('px-4 py-3 rounded-lg').style(
                            'background: var(--bg-tertiary); border: 1px solid var(--border-light);'
                        ):
                            ui.label(pair).classes('font-mono text-lg font-bold').style('color: var(--primary-700);')
                            ui.label(desc).classes('text-xs mt-1').style('color: var(--text-muted);')

            else:
                with ui.column().classes('items-center py-8'):
                    ui.icon('spellcheck').classes('text-5xl').style('color: var(--text-muted);')
                    ui.label(tr('Lab Engine not initialized')).classes('mt-4').style('color: var(--text-muted);')

        # === System Status ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-6'):
                ui.icon('info').classes('text-3xl').style('color: var(--text-muted);')
                # Changed to H2
                h2(tr('System Status'), classes='text-xl font-bold', style='color: var(--text-primary);')

            with ui.column().classes('gap-4'):
                # Main Index Status
                index_active = state.searcher and state.searcher.index
                with ui.row().classes('items-center gap-4'):
                    ui.label(tr('Search Index')).classes('font-medium w-40').style('color: var(--text-secondary);')
                    ui.badge(tr('Active') if index_active else tr('Not loaded'),
                             color='green' if index_active else 'red')

                # Lab Index Status
                lab_active = state.lab_engine and getattr(state.lab_engine, 'lab_index', None)
                with ui.row().classes('items-center gap-4'):
                    ui.label(tr('Lab Index')).classes('font-medium w-40').style('color: var(--text-secondary);')
                    ui.badge(tr('Active') if lab_active else tr('Not loaded'),
                             color='green' if lab_active else 'gray')

                # Document count
                if state.searcher:
                    try:
                        searcher = getattr(state.searcher, 'searcher', None)
                        if searcher:
                            doc_count = searcher.num_docs
                            with ui.row().classes('items-center gap-4'):
                                ui.label(tr('Documents')).classes('font-medium w-40').style('color: var(--text-secondary);')
                                ui.label(f'{doc_count:,}').style('color: var(--text-primary);')
                    except Exception:
                        pass

            ui.separator().classes('my-4')

            # Version info
            ui.markdown('''
            **Dicta Genizah Search**

            *Formerly Genizah Search Pro 5.0*

            Data Source: MiDRASH Project (Friedberg Genizah Project)
            ''').style('color: var(--text-muted);')
