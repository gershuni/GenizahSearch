#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GenizahSearch Web Application

Professional NiceGUI-based web interface for the Cairo Genizah search engine.
Run with: python -m web.main (from project root)
"""

import os
import sys
import threading
import time

# Ensure we can import from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nicegui import ui, app, run
from web.state import state
from web.api import init_api_routes
from web.translations import tr, is_rtl, get_dir, set_language, get_language
from genizah_core import MetadataManager, VariantManager, SearchEngine, LabEngine, Indexer, AIManager, ListsManager, Config

# App configuration
APP_TITLE = "Genizah Search | חיפוש גניזה"
APP_PORT = int(os.environ.get('GENIZAH_PORT', 8081))

# Initialize API routes (Image Proxy, Export)
init_api_routes()

# ============================================================================
# Theme & Styles
# ============================================================================

COMMON_STYLES = '''
<style>
    /* Default Theme (Light/Green) */
    :root {
        --primary-color: #2e7d32;
        --secondary-color: #f5f0e1; /* Parchment */
        --bg-body: #f8f9fa;
        --bg-header: #2e7d32;
        --bg-sidebar: #ffffff;
        --text-primary: #263238;
        --text-secondary: #546e7a;
    }

    /* Parchment Theme */
    [data-theme="parchment"] {
        --primary-color: #8d6e63;
        --secondary-color: #fff8e1;
        --bg-body: #fdfbf7;
        --bg-header: #5d4037;
        --bg-sidebar: #fff8e1;
        --text-primary: #3e2723;
        --text-secondary: #5d4037;
    }

    /* Dark Mode */
    [data-theme="dark"] {
        --primary-color: #90caf9;
        --secondary-color: #424242;
        --bg-body: #121212;
        --bg-header: #1f1f1f;
        --bg-sidebar: #1e1e1e;
        --text-primary: #e0e0e0;
        --text-secondary: #b0bec5;
    }

    body {
        background-color: var(--bg-body);
        color: var(--text-primary);
        font-family: "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    }

    .q-header {
        background-color: var(--bg-header) !important;
    }

    .q-drawer {
        background-color: var(--bg-sidebar) !important;
    }

    /* Dense Drawer Item */
    .q-item--dense {
        min-height: 32px;
        padding: 8px 16px;
    }

    /* Navigation Active State */
    .nav-active {
        background: var(--secondary-color);
        color: var(--primary-color);
        border-right: 3px solid var(--primary-color);
        font-weight: 600;
    }

    .logo-text {
        font-weight: 700;
        letter-spacing: 0.5px;
        color: white;
    }
</style>
'''

# ============================================================================
# Layout Components
# ============================================================================

def create_layout():
    """Create the main application layout with Header and Sidebar."""

    # Header
    with ui.header().classes('shadow-md q-py-xs').style('height: 50px;'):
        with ui.row().classes('w-full items-center no-wrap'):
            ui.button(icon='menu', on_click=lambda: left_drawer.toggle()).props('flat round dense text-color=white')
            ui.icon('auto_stories').classes('text-2xl q-mr-sm')
            ui.label(APP_TITLE).classes('text-lg logo-text')

            ui.space()

            # Status Indicator
            with ui.row().classes('items-center gap-2 q-mr-md'):
                status_icon = ui.icon('circle').classes('text-xs')
                status_label = ui.label('Initializing...').classes('text-xs text-gray-200')

                # Bind status to state
                def update_status():
                    if state.is_ready():
                        status_icon.classes('text-green-300', remove='text-red-300 text-yellow-300')
                        status_label.text = 'Ready'
                    else:
                        status_icon.classes('text-yellow-300', remove='text-green-300')
                        status_label.text = 'Loading...'

                ui.timer(2.0, update_status)

    # Left Sidebar (Drawer)
    left_drawer = ui.left_drawer(value=True).classes('shadow-lg').props('width=240 bordered')
    with left_drawer:
        with ui.column().classes('w-full q-py-md'):

            def nav_item(label, icon, target):
                # Simple navigation helper
                def go():
                    ui.navigate.to(target)

                # Check active state
                is_active = app.storage.user.get('current_page') == target
                classes = 'w-full justify-start hover:opacity-80 rounded-r-full mb-1'
                if is_active:
                    classes += ' nav-active'
                else:
                    # Default text color for inactive items depends on theme context, handled by CSS var inheritance usually
                    # But nicegui overrides might need specifics. We use CSS class.
                    pass

                ui.button(label, icon=icon, on_click=go).props('flat dense align=left').classes(classes)

            ui.label('MENU').classes('text-xs font-bold opacity-60 q-px-md q-mb-sm')

            nav_item(tr('Dashboard'), 'dashboard', '/')
            nav_item(tr('Search'), 'search', '/search')
            nav_item(tr('Find Parallels'), 'compare_arrows', '/parallels')
            nav_item(tr('Browse'), 'menu_book', '/browse')
            nav_item(tr('Personal Lists'), 'star', '/lists')
            nav_item(tr('Settings'), 'settings', '/settings')

            ui.separator().classes('my-2')

            # Language Toggle
            def toggle_lang():
                current = get_language()
                new_lang = 'en' if current == 'he' else 'he'
                set_language(new_lang)
                ui.navigate.reload()

            lang_label = "עברית" if get_language() == 'en' else "English"
            ui.button(lang_label, icon='language', on_click=toggle_lang).props('flat dense align=left').classes('w-full justify-start opacity-80')

            # Theme Toggle
            def set_theme(theme_name):
                app.storage.user['theme'] = theme_name
                ui.run_javascript(f'document.body.setAttribute("data-theme", "{theme_name}")')

            with ui.row().classes('q-px-md gap-2 mt-4'):
                ui.button(icon='light_mode', on_click=lambda: set_theme('light')).props('round flat size=sm').tooltip('Light')
                ui.button(icon='history_edu', on_click=lambda: set_theme('parchment')).props('round flat size=sm').tooltip('Parchment')
                ui.button(icon='dark_mode', on_click=lambda: set_theme('dark')).props('round flat size=sm').tooltip('Dark')

    # Content Area (Slot for pages)
    return ui.column().classes('w-full p-4 items-stretch flex-grow')


# ============================================================================
# Pages
# ============================================================================

@ui.page('/')
def dashboard_page():
    app.storage.user['current_page'] = '/'
    ui.add_head_html(COMMON_STYLES)
    # Apply theme on load
    current_theme = app.storage.user.get('theme', 'light')
    ui.run_javascript(f'document.body.setAttribute("data-theme", "{current_theme}")')

    content = create_layout()
    with content:
        from web.pages import home
        if hasattr(home, 'create_page'):
            home.create_page()
        else:
            ui.label("Dashboard Placeholder").classes('text-2xl opacity-50')

@ui.page('/search')
def search_page_route():
    app.storage.user['current_page'] = '/search'
    ui.add_head_html(COMMON_STYLES)
    current_theme = app.storage.user.get('theme', 'light')
    ui.run_javascript(f'document.body.setAttribute("data-theme", "{current_theme}")')

    content = create_layout()
    with content:
        from web.pages.search import create_search_page
        create_search_page()

@ui.page('/parallels')
def parallels_page_route():
    app.storage.user['current_page'] = '/parallels'
    ui.add_head_html(COMMON_STYLES)
    current_theme = app.storage.user.get('theme', 'light')
    ui.run_javascript(f'document.body.setAttribute("data-theme", "{current_theme}")')
    content = create_layout()
    with content:
        from web.pages.parallels import create_parallels_page
        create_parallels_page()

@ui.page('/browse')
def browse_page_route(sys_id: str = None, highlight: str = None):
    app.storage.user['current_page'] = '/browse'
    ui.add_head_html(COMMON_STYLES)
    current_theme = app.storage.user.get('theme', 'light')
    ui.run_javascript(f'document.body.setAttribute("data-theme", "{current_theme}")')

    content = create_layout()
    with content:
        from web.pages.browse import create_browse_page
        create_browse_page(initial_sys_id=sys_id, highlight=highlight)

@ui.page('/lists')
def lists_page_route():
    app.storage.user['current_page'] = '/lists'
    ui.add_head_html(COMMON_STYLES)
    current_theme = app.storage.user.get('theme', 'light')
    ui.run_javascript(f'document.body.setAttribute("data-theme", "{current_theme}")')
    content = create_layout()
    with content:
        from web.pages.lists import create_lists_page
        create_lists_page()

@ui.page('/settings')
def settings_page_route():
    app.storage.user['current_page'] = '/settings'
    ui.add_head_html(COMMON_STYLES)
    current_theme = app.storage.user.get('theme', 'light')
    ui.run_javascript(f'document.body.setAttribute("data-theme", "{current_theme}")')
    content = create_layout()
    with content:
        ui.label("Settings").classes('text-2xl font-bold opacity-80')

# ============================================================================
# Startup Logic (Background)
# ============================================================================

async def initialize_engine():
    """Heavy initialization running in a separate thread via run.io_bound."""
    print("Starting background initialization...")

    # We define a sync function to run in the thread executor
    def _init_sync():
        try:
            # 1. Metadata
            state.meta_mgr = MetadataManager()
            state.lists_mgr = ListsManager(state.meta_mgr)

            # 2. Lab Settings & Engine (Lightweight)
            state.lab_engine = LabEngine(state.meta_mgr, None) # Variant mgr later

            # 3. Variants (depends on Lab Settings)
            state.var_mgr = VariantManager(settings=state.lab_engine.settings)

            # 4. Search Engine & Indexer
            state.searcher = SearchEngine(state.meta_mgr, state.var_mgr)
            state.indexer = Indexer(state.meta_mgr)

            # 5. AI
            state.ai_mgr = AIManager()

            # 6. Start heavy background loading
            state.meta_mgr.start_background_loading()

            print("Engine initialization complete.")
            return True
        except Exception as e:
            print(f"Engine init failed: {e}")
            return False

    # Await the execution in thread pool
    await run.io_bound(_init_sync)

app.on_startup(initialize_engine)

if __name__ in {'__main__', '__mp_main__'}:
    print(f"\n🚀 Starting Genizah Search Web on port {APP_PORT}...")
    ui.run(
        title=APP_TITLE,
        port=APP_PORT,
        reload=True,
        show=True,
        favicon='📜',
        storage_secret='genizah-secret',
    )
