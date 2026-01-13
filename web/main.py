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
from web.translations import tr, is_rtl, get_dir
from web.pages.search import create_search_page
from web.pages.document import create_document_page


# App configuration
APP_TITLE = "Genizah Search"
APP_PORT = int(os.environ.get('GENIZAH_PORT', 8081))


def create_header():
    """Create the application header."""
    with ui.header().classes('items-center justify-between bg-blue-800 text-white px-4 py-2'):
        with ui.row().classes('items-center gap-4'):
            ui.link(tr('Genizah Search'), '/').classes(
                'text-xl font-bold no-underline text-white hover:text-blue-200'
            )
        with ui.row().classes('items-center gap-2'):
            ui.link(tr('Search'), '/').classes('text-white hover:text-blue-200 no-underline')
            ui.link(tr('About'), '/about').classes('text-white hover:text-blue-200 no-underline')


def create_footer():
    """Create the application footer."""
    with ui.footer().classes('bg-gray-100 text-gray-600 text-center py-2 text-sm'):
        ui.label(tr('Cairo Genizah Search Engine'))


@ui.page('/')
def index_page():
    """Main search page."""
    ui.add_head_html(f'<html dir="{get_dir()}">')
    ui.add_head_html('''
        <style>
            .rtl-text { direction: rtl; text-align: right; }
            .hebrew-text { font-family: "David", "Frank Ruehl", "Noto Sans Hebrew", sans-serif; }
            .search-result { border: 1px solid #e0e0e0; border-radius: 8px; padding: 16px; margin-bottom: 12px; }
            .search-result:hover { background-color: #f5f5f5; }
            .snippet { white-space: pre-wrap; line-height: 1.6; }
        </style>
    ''')

    create_header()
    create_search_page()
    create_footer()


@ui.page('/document/{uid}')
def document_page(uid: str):
    """Document viewer page."""
    ui.add_head_html(f'<html dir="{get_dir()}">')
    ui.add_head_html('''
        <style>
            .rtl-text { direction: rtl; text-align: right; }
            .hebrew-text { font-family: "David", "Frank Ruehl", "Noto Sans Hebrew", sans-serif; }
            .manuscript-text {
                white-space: pre-wrap;
                line-height: 1.8;
                font-size: 1.1rem;
                background-color: #fffef5;
                padding: 20px;
                border-radius: 8px;
            }
        </style>
    ''')

    create_header()
    create_document_page(uid)
    create_footer()


@ui.page('/about')
def about_page():
    """About page."""
    ui.add_head_html(f'<html dir="{get_dir()}">')

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
- Cross-page search capabilities
- Document browsing and viewing

**Technology:**
- Tantivy search engine
- NiceGUI web framework
- Python backend
            ''')

    create_footer()


def startup():
    """Application startup handler."""
    print("Initializing Genizah Search service...")

    if not init_service():
        print(f"Warning: Service initialization failed: {get_service().init_error}")
    else:
        if get_service().index_exists:
            print("Service initialized successfully. Index found.")
        else:
            print("Service initialized but index not found. Search will not work.")


# Register startup handler
app.on_startup(startup)


if __name__ == '__main__':
    print(f"Starting Genizah Search Web on port {APP_PORT}...")
    ui.run(
        title=APP_TITLE,
        port=APP_PORT,
        reload=False,  # Set to True for development
        show=False,    # Don't open browser automatically
    )
