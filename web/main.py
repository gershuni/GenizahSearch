#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GenizahSearch Web Application

NiceGUI-based web interface for the Cairo Genizah search engine.
Run with: python -m web.main (from project root)
"""

import os
import sys

# Ensure we can import from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nicegui import ui, app

from web.services import get_service, init_service
from web.translations import tr, is_rtl, get_dir, set_language, get_language


# App configuration
APP_TITLE = "Genizah Search"
APP_PORT = int(os.environ.get('GENIZAH_PORT', 8081))


# ============================================================================
# Common Styles
# ============================================================================

COMMON_STYLES = '''
<style>
    .rtl-text { direction: rtl; text-align: right; }
    .ltr-text { direction: ltr; text-align: left; }
    .hebrew-text { font-family: "David", "Frank Ruehl", "Noto Sans Hebrew", "SBL Hebrew", serif; }

    .search-result {
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        padding: 16px;
        margin-bottom: 12px;
        transition: all 0.2s;
    }
    .search-result:hover {
        background-color: #f5f5f5;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
    }

    .snippet {
        white-space: pre-wrap;
        line-height: 1.8;
    }

    .manuscript-text {
        white-space: pre-wrap;
        line-height: 2.0;
        font-size: 1.15rem;
        background-color: #fffef5;
        padding: 24px;
        border-radius: 8px;
        border: 1px solid #e8e4d4;
    }

    .home-card {
        transition: all 0.3s ease;
        cursor: pointer;
    }
    .home-card:hover {
        transform: translateY(-4px);
        box-shadow: 0 8px 25px rgba(0,0,0,0.15);
    }

    .card-icon {
        font-size: 3rem;
        margin-bottom: 1rem;
    }
</style>
'''


# ============================================================================
# Header & Footer
# ============================================================================

def create_header():
    """Create the application header with navigation."""
    with ui.header().classes('items-center justify-between bg-blue-800 text-white px-6 py-3'):
        # Logo/Title
        with ui.row().classes('items-center gap-4'):
            ui.link(tr('Genizah Search'), '/').classes(
                'text-xl font-bold no-underline text-white hover:text-blue-200'
            )

        # Navigation
        with ui.row().classes('items-center gap-6'):
            ui.link(tr('Text Search'), '/search').classes(
                'text-white hover:text-blue-200 no-underline'
            )
            ui.link(tr('Find Parallels'), '/parallels').classes(
                'text-white hover:text-blue-200 no-underline'
            )
            ui.link(tr('Browse Manuscripts'), '/browse').classes(
                'text-white hover:text-blue-200 no-underline'
            )

            # Settings button
            with ui.button(icon='settings').props('flat round').classes('text-white'):
                with ui.menu().classes('p-2'):
                    ui.label(tr('Settings')).classes('font-bold px-2 py-1')
                    ui.separator()
                    with ui.row().classes('items-center gap-2 px-2'):
                        ui.label(tr('Language'))

                        def on_lang_change(e):
                            set_language(e.value)
                            ui.navigate.reload()

                        ui.select(
                            {'he': 'עברית', 'en': 'English'},
                            value=get_language(),
                            on_change=on_lang_change
                        ).classes('w-24')


def create_footer():
    """Create the application footer."""
    with ui.footer().classes('bg-gray-100 text-gray-600 text-center py-3 text-sm'):
        ui.label(tr('Cairo Genizah Search Engine'))


def add_page_head():
    """Add common head elements to pages."""
    ui.add_head_html(f'<html dir="{get_dir()}" lang="{get_language()}">')
    ui.add_head_html(COMMON_STYLES)


# ============================================================================
# Home Page
# ============================================================================

@ui.page('/')
def home_page():
    """Home page with three main feature cards."""
    add_page_head()
    create_header()

    with ui.column().classes('w-full max-w-6xl mx-auto p-8'):
        # Title section
        with ui.column().classes('text-center mb-12'):
            ui.label(tr('Genizah Search')).classes(
                'text-4xl font-bold text-blue-800 mb-2'
            )
            ui.label(tr('Cairo Genizah Search Engine')).classes(
                'text-xl text-gray-600'
            )

        # Cards grid
        with ui.row().classes('w-full justify-center gap-8 flex-wrap'):

            # Search Card
            with ui.card().classes('home-card w-80 p-6 text-center').on(
                'click', lambda: ui.navigate.to('/search')
            ):
                ui.icon('search', size='4rem').classes('text-blue-600 mb-4')
                ui.label(tr('Text Search')).classes(
                    'text-2xl font-bold text-blue-800 mb-2'
                )
                ui.label(tr('Search for words and phrases in the Genizah corpus')).classes(
                    'text-gray-600 rtl-text hebrew-text'
                )

            # Parallels Card
            with ui.card().classes('home-card w-80 p-6 text-center').on(
                'click', lambda: ui.navigate.to('/parallels')
            ):
                ui.icon('compare_arrows', size='4rem').classes('text-green-600 mb-4')
                ui.label(tr('Find Parallels')).classes(
                    'text-2xl font-bold text-green-800 mb-2'
                )
                ui.label(tr('Enter a long text and find parallel texts in the Genizah')).classes(
                    'text-gray-600 rtl-text hebrew-text'
                )

            # Browse Card
            with ui.card().classes('home-card w-80 p-6 text-center').on(
                'click', lambda: ui.navigate.to('/browse')
            ):
                ui.icon('menu_book', size='4rem').classes('text-amber-600 mb-4')
                ui.label(tr('Browse Manuscripts')).classes(
                    'text-2xl font-bold text-amber-800 mb-2'
                )
                ui.label(tr('Enter a shelfmark to browse the manuscript')).classes(
                    'text-gray-600 rtl-text hebrew-text'
                )

        # Status indicator
        service = get_service()
        if not service.is_ready:
            with ui.card().classes('w-full max-w-md mx-auto mt-8 p-4 bg-yellow-50'):
                with ui.row().classes('items-center gap-2'):
                    ui.icon('warning', color='orange')
                    ui.label(tr('Service not available')).classes('text-yellow-800')
        elif not service.index_exists:
            with ui.card().classes('w-full max-w-md mx-auto mt-8 p-4 bg-yellow-50'):
                with ui.row().classes('items-center gap-2'):
                    ui.icon('warning', color='orange')
                    ui.label(tr('Index not found')).classes('text-yellow-800')

    create_footer()


# ============================================================================
# Search Page
# ============================================================================

@ui.page('/search')
def search_page():
    """Text search page."""
    add_page_head()
    create_header()

    from web.pages.search import create_search_page
    create_search_page()

    create_footer()


# ============================================================================
# Parallels (Composition Search) Page
# ============================================================================

@ui.page('/parallels')
def parallels_page():
    """Composition search page for finding parallel texts."""
    add_page_head()
    create_header()

    from web.pages.parallels import create_parallels_page
    create_parallels_page()

    create_footer()


# ============================================================================
# Browse Page
# ============================================================================

@ui.page('/browse')
def browse_page_route():
    """Browse manuscripts by shelfmark."""
    add_page_head()
    create_header()

    from web.pages.browse import create_browse_page
    create_browse_page()

    create_footer()


@ui.page('/browse/{sys_id}')
def browse_manuscript_page(sys_id: str):
    """Browse a specific manuscript."""
    add_page_head()
    create_header()

    from web.pages.browse import create_browse_page
    create_browse_page(sys_id)

    create_footer()


# ============================================================================
# Document Viewer Page
# ============================================================================

@ui.page('/document/{uid}')
def document_page(uid: str):
    """Document viewer page."""
    add_page_head()
    create_header()

    from web.pages.document import create_document_page
    create_document_page(uid)

    create_footer()


# ============================================================================
# About Page
# ============================================================================

@ui.page('/about')
def about_page():
    """About page."""
    add_page_head()
    create_header()

    with ui.column().classes('w-full max-w-4xl mx-auto p-8'):
        ui.label(tr('About')).classes('text-2xl font-bold mb-4')

        with ui.card().classes('w-full p-6'):
            ui.markdown('''
## Genizah Search / חיפוש גניזה

A powerful search engine for the Cairo Genizah manuscript collection.

**Features:**
- Full-text search with Hebrew variant support
- OCR error tolerance through intelligent variant matching
- Composition search for finding parallel texts
- Manuscript browsing with image viewing
- Cross-page search capabilities

**Technology:**
- Tantivy search engine (Rust-based)
- NiceGUI web framework
- Python backend
            ''')

    create_footer()


# ============================================================================
# Startup
# ============================================================================

def startup():
    """Application startup handler."""
    print("Initializing Genizah Search service...")

    if not init_service():
        print(f"Warning: Service initialization failed: {get_service().init_error}")
    else:
        service = get_service()
        if service.index_exists:
            print("Service initialized successfully. Index found.")
            if service.has_lab_engine:
                print("Lab engine available for composition search.")
            else:
                print("Lab engine not available - composition search disabled.")
        else:
            print("Service initialized but index not found. Search will not work.")


# Register startup handler
app.on_startup(startup)


if __name__ in {'__main__', '__mp_main__'}:
    print(f"Starting Genizah Search Web on port {APP_PORT}...")
    ui.run(
        title=APP_TITLE,
        port=APP_PORT,
        reload=True,
        show=True,
    )
