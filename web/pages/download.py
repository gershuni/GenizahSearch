# -*- coding: utf-8 -*-
"""
Download Page - Dicta Genizah Search

Desktop application download page with installation instructions.
"""

from nicegui import ui
from web.translations import tr, get_language
from web.components.typography import h1, h2, h3


def create_download_page():
    """Create the Download page."""

    lang = get_language()
    is_hebrew = lang == 'he'

    with ui.column().classes('w-full max-w-4xl mx-auto gap-6 fade-in p-4'):

        # === Page Header ===
        with ui.column().classes('gap-2 mb-2'):
            h1(tr('Get Dicta Genizah Search'), classes='text-3xl font-bold', style='color: var(--text-primary);')
            ui.label(tr('Desktop Application')).style('color: var(--text-secondary);')

        # === Download Card ===
        with ui.card().classes('w-full p-8'):
            with ui.column().classes('w-full items-center gap-6'):
                # Icon
                with ui.element('div').classes('p-6 rounded-full').style('background: linear-gradient(135deg, #6366f1, #8b5cf6);'):
                    ui.icon('download').classes('text-5xl text-white')

                # Download button
                ui.button(
                    tr('Download for Windows'),
                    icon='windows',
                    on_click=lambda: ui.navigate.to('https://github.com/gershuni/GenizahSearch/releases', new_tab=True)
                ).props('size=lg color=primary unelevated').classes('px-8 py-2')

                ui.label('Windows 10 / 11').classes('text-sm').style('color: var(--text-muted);')

        # === Why Desktop App ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-4'):
                ui.icon('star').classes('text-2xl text-primary')
                h2(tr('Why use the desktop app?'), classes='text-xl font-bold', style='color: var(--text-primary);')

            features = [
                ('wifi_off', tr('No internet required after setup')),
                ('speed', tr('Fast local search')),
                ('tune', tr('More search options and filters')),
                ('sync', tr('Community updates synced regularly')),
                ('lock', tr('Private - data stays on your computer')),
                ('update', tr('Regular updates via built-in updater')),
            ]

            with ui.grid(columns=2).classes('w-full gap-4'):
                for icon, text in features:
                    with ui.row().classes('items-center gap-3 p-3 rounded').style('background: var(--surface-secondary);'):
                        ui.icon(icon).classes('text-xl text-primary')
                        ui.label(text).style('color: var(--text-secondary);')

        # === System Requirements ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-4'):
                ui.icon('computer').classes('text-2xl text-primary')
                h2(tr('System Requirements'), classes='text-xl font-bold', style='color: var(--text-primary);')

            requirements = [
                ('desktop_windows', tr('Windows 10 or Windows 11')),
                ('memory', tr('8 GB RAM minimum')),
                ('storage', tr('2 GB free disk space')),
            ]

            with ui.column().classes('gap-3'):
                for icon, text in requirements:
                    with ui.row().classes('items-center gap-3'):
                        ui.icon(icon).classes('text-lg').style('color: var(--text-muted);')
                        ui.label(text).style('color: var(--text-secondary);')

        # === Installation Steps ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-4'):
                ui.icon('checklist').classes('text-2xl text-primary')
                h2(tr('Installation Steps'), classes='text-xl font-bold', style='color: var(--text-primary);')

            steps = [
                (tr('Download from GitHub Releases'), 'cloud_download'),
                (tr('Follow the installation prompts'), 'install_desktop'),
                (tr('On first launch, download the transcription data from Zenodo'), 'dataset'),
            ]

            with ui.column().classes('gap-4'):
                for i, (text, icon) in enumerate(steps, 1):
                    with ui.row().classes('items-start gap-4'):
                        # Step number
                        with ui.element('div').classes('flex-shrink-0 w-8 h-8 rounded-full flex items-center justify-center').style(
                            'background: var(--primary-600); color: white; font-weight: bold;'
                        ):
                            ui.label(str(i))
                        # Step content
                        with ui.column().classes('gap-1 pt-1'):
                            ui.label(text).style('color: var(--text-secondary);')
