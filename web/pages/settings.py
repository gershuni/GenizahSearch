# -*- coding: utf-8 -*-
"""
Lab Settings Page - Genizah Search Pro

Configure advanced search parameters and Lab Mode settings:
- Variant search tuning
- Lab Mode algorithm parameters
- Index management
- AI configuration
"""

from nicegui import ui, run, app
from web.state import state
from web.translations import tr


def create_settings_page():
    """Create the Lab Settings page."""

    with ui.column().classes('w-full max-w-5xl mx-auto gap-8 fade-in'):

        # === Page Header ===
        with ui.row().classes('w-full items-center justify-between'):
            with ui.column().classes('gap-1'):
                ui.label(tr('Lab Settings')).classes('text-3xl font-bold').style('color: var(--text-primary);')
                ui.label(tr('Configure advanced search parameters and algorithms')).style('color: var(--text-secondary);')

        # === Lab Mode Configuration ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-6'):
                ui.icon('science').classes('text-3xl').style('color: var(--primary-600);')
                ui.label(tr('Lab Mode Parameters')).classes('text-xl font-bold').style('color: var(--text-primary);')

            ui.markdown('''
            **Lab Mode** uses the Shmidman-Koppel-Porat algorithm for detecting parallel texts
            based on rare-letter fingerprinting. Adjust these parameters to fine-tune search behavior.
            ''').style('color: var(--text-secondary);')

            if state.lab_engine and hasattr(state.lab_engine, 'settings'):
                settings = state.lab_engine.settings

                with ui.grid(columns=2).classes('w-full gap-6 mt-6'):

                    # Min Should Match
                    with ui.column().classes('gap-2'):
                        ui.label(tr('Min Should Match (%)')).classes('font-medium').style('color: var(--text-secondary);')
                        min_match = ui.slider(
                            min=50, max=100, value=settings.min_should_match
                        ).props('label-always')
                        ui.label(tr('Percentage of query terms that must match')).classes('text-xs').style('color: var(--text-muted);')

                    # Gap Penalty
                    with ui.column().classes('gap-2'):
                        ui.label(tr('Gap Penalty')).classes('font-medium').style('color: var(--text-secondary);')
                        gap_penalty = ui.slider(
                            min=0, max=5, step=0.5, value=settings.gap_penalty
                        ).props('label-always')
                        ui.label(tr('Penalty applied for gaps between matched terms')).classes('text-xs').style('color: var(--text-muted);')

                    # Candidate Limit
                    with ui.column().classes('gap-2'):
                        ui.label(tr('Candidate Limit')).classes('font-medium').style('color: var(--text-secondary);')
                        candidate_limit = ui.number(
                            value=settings.candidate_limit, min=100, max=50000, step=100
                        ).props('outlined dense')
                        ui.label(tr('Maximum candidates to scan per query')).classes('text-xs').style('color: var(--text-muted);')

                    # Display Limit
                    with ui.column().classes('gap-2'):
                        ui.label(tr('Display Limit')).classes('font-medium').style('color: var(--text-secondary);')
                        display_limit = ui.number(
                            value=settings.lab_display_limit, min=50, max=2000, step=50
                        ).props('outlined dense')
                        ui.label(tr('Maximum results to display')).classes('text-xs').style('color: var(--text-muted);')

                ui.separator().classes('my-6')

                # Composition Search Settings
                ui.label(tr('Composition Search')).classes('font-bold text-lg mb-4').style('color: var(--text-primary);')

                with ui.grid(columns=2).classes('w-full gap-6'):

                    # Chunk Size
                    with ui.column().classes('gap-2'):
                        ui.label(tr('Default Chunk Size')).classes('font-medium').style('color: var(--text-secondary);')
                        chunk_size = ui.slider(
                            min=2, max=20, value=settings.comp_chunk_size
                        ).props('label-always')
                        ui.label(tr('Words per search chunk (recommended: 4-7)')).classes('text-xs').style('color: var(--text-muted);')

                    # Min Score
                    with ui.column().classes('gap-2'):
                        ui.label(tr('Min Score')).classes('font-medium').style('color: var(--text-secondary);')
                        min_score = ui.slider(
                            min=10, max=100, value=settings.comp_min_score
                        ).props('label-always')
                        ui.label(tr('Minimum score for results')).classes('text-xs').style('color: var(--text-muted);')

                # Save Button
                def save_lab_settings():
                    settings.min_should_match = int(min_match.value)
                    settings.gap_penalty = float(gap_penalty.value)
                    settings.candidate_limit = int(candidate_limit.value)
                    settings.lab_display_limit = int(display_limit.value)
                    settings.comp_chunk_size = int(chunk_size.value)
                    settings.comp_min_score = int(min_score.value)
                    settings.save()
                    ui.notify(tr('Settings saved'), type='positive')

                ui.button(tr('Save Settings'), icon='save', on_click=save_lab_settings).classes('btn-primary mt-6')

            else:
                ui.label(tr('Lab Engine not initialized')).style('color: var(--text-muted);')

        # === Variant Search Settings ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-6'):
                ui.icon('spellcheck').classes('text-3xl').style('color: var(--accent-blue);')
                ui.label(tr('Variant Search')).classes('text-xl font-bold').style('color: var(--text-primary);')

            ui.markdown('''
            Variant search handles OCR errors and spelling variations common in Genizah manuscripts.
            The system automatically generates variant spellings based on known confusion patterns.
            ''').style('color: var(--text-secondary);')

            with ui.column().classes('mt-4 gap-4'):
                # Variant Pairs Display
                ui.label(tr('Common Variant Pairs')).classes('font-medium').style('color: var(--text-primary);')

                variant_pairs = [
                    ('ד', 'ר', tr('Common HTR confusion')),
                    ('ה', 'ח', tr('Similar shapes')),
                    ('ו', 'י', tr('Vertical strokes')),
                    ('כ', 'ב', tr('Similar shapes')),
                    ('א', 'ע', tr('Gutturals')),
                ]

                with ui.row().classes('gap-3 flex-wrap'):
                    for char1, char2, desc in variant_pairs:
                        with ui.card().classes('p-3').style('background: var(--bg-tertiary);'):
                            with ui.row().classes('items-center gap-2'):
                                ui.label(f'{char1} ↔ {char2}').classes('font-mono text-lg font-bold').style('color: var(--primary-700);')
                                ui.label(desc).classes('text-xs').style('color: var(--text-muted);')

        # === Index Management ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-6'):
                ui.icon('storage').classes('text-3xl').style('color: var(--accent-amber);')
                ui.label(tr('Index Management')).classes('text-xl font-bold').style('color: var(--text-primary);')

            # Index Status
            with ui.row().classes('w-full gap-6 items-center'):
                def get_index_status():
                    if state.searcher and state.searcher.index:
                        return tr('Active'), 'green'
                    return tr('Not loaded'), 'red'

                status_text, status_color = get_index_status()
                with ui.row().classes('items-center gap-2'):
                    ui.icon('circle').classes(f'text-{status_color}-500 text-xs')
                    ui.label(f"{tr('Index Status')}: {status_text}").style('color: var(--text-secondary);')

                def get_doc_count():
                    if state.searcher and state.searcher.searcher:
                        return f"{state.searcher.searcher.num_docs:,}"
                    return "0"

                ui.label(f"{tr('Documents')}: {get_doc_count()}").style('color: var(--text-secondary);')

            ui.separator().classes('my-4')

            # Lab Index Status
            with ui.row().classes('w-full gap-6 items-center'):
                def get_lab_status():
                    if state.lab_engine:
                        if state.lab_engine.lab_index_needs_rebuild:
                            return tr('Rebuild Needed'), 'amber'
                        return tr('Ready'), 'green'
                    return tr('Not available'), 'gray'

                lab_status, lab_color = get_lab_status()
                with ui.row().classes('items-center gap-2'):
                    ui.icon('circle').classes(f'text-{lab_color}-500 text-xs')
                    ui.label(f"{tr('Lab Index')}: {lab_status}").style('color: var(--text-secondary);')

            # Rebuild Button
            with ui.row().classes('gap-4 mt-6'):
                async def rebuild_lab_index():
                    if state.lab_engine:
                        ui.notify(tr('Rebuilding Lab Index...'), type='info')

                        def do_rebuild():
                            state.lab_engine.rebuild_lab_index()
                            return True

                        await run.io_bound(do_rebuild)
                        ui.notify(tr('Lab Index rebuilt'), type='positive')

                ui.button(
                    tr('Rebuild Lab Index'),
                    icon='refresh',
                    on_click=rebuild_lab_index
                ).props('outline').style('color: var(--primary-700);')

        # === About Section ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-4'):
                ui.icon('info').classes('text-2xl').style('color: var(--text-muted);')
                ui.label(tr('About')).classes('text-xl font-bold').style('color: var(--text-primary);')

            ui.markdown('''
            **Genizah Search Pro v5.0**

            A comprehensive research platform for the Cairo Genizah corpus.

            **Data Source:** MiDRASH Project (Friedberg Genizah Project)

            **Algorithm:** Shmidman, A., Koppel, M., & Porat, E. (2016).
            *Identification of Parallel Passages Across a Large Hebrew/Aramaic Corpus.*

            **License:** CC BY 4.0
            ''').style('color: var(--text-secondary);')
