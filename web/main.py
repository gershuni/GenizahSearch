#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GenizahSearch Web Application

Professional NiceGUI-based web interface for the Cairo Genizah search engine.
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
APP_TITLE = "Genizah Search | חיפוש גניזה"
APP_PORT = int(os.environ.get('GENIZAH_PORT', 8081))


# ============================================================================
# Professional Styles - Green Theme
# ============================================================================

COMMON_STYLES = '''
<style>
    :root {
        --primary-green: #2e7d32;
        --primary-green-light: #4caf50;
        --primary-green-dark: #1b5e20;
        --accent-gold: #f9a825;
        --bg-parchment: #fffef5;
        --bg-parchment-dark: #f5f0e1;
        --text-dark: #263238;
        --text-light: #546e7a;
    }

    /* RTL & Hebrew Support */
    .rtl-text {
        direction: rtl;
        text-align: right;
    }
    .ltr-text {
        direction: ltr;
        text-align: left;
    }
    .hebrew-text {
        font-family: "David", "Frank Ruehl", "Noto Sans Hebrew", "SBL Hebrew", serif;
        line-height: 1.8;
    }

    /* Main Layout */
    body {
        font-family: "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    }

    /* Search Results Card */
    .search-result {
        border: 1px solid #e0e0e0;
        border-radius: 12px;
        padding: 20px;
        margin-bottom: 16px;
        transition: all 0.25s ease;
        background: white;
    }
    .search-result:hover {
        border-color: var(--primary-green);
        box-shadow: 0 4px 20px rgba(46, 125, 50, 0.15);
        transform: translateY(-2px);
    }

    /* Snippet Styles */
    .snippet {
        white-space: pre-wrap;
        line-height: 1.9;
        background: linear-gradient(to right, var(--bg-parchment), white);
        padding: 16px;
        border-radius: 8px;
        border-left: 4px solid var(--primary-green);
    }

    /* Manuscript Text Display */
    .manuscript-text {
        white-space: pre-wrap;
        line-height: 2.2;
        font-size: 1.2rem;
        background: var(--bg-parchment);
        padding: 28px;
        border-radius: 12px;
        border: 1px solid #e8e4d4;
        box-shadow: inset 0 2px 8px rgba(0,0,0,0.03);
    }

    /* Home Page Cards */
    .home-card {
        transition: all 0.35s cubic-bezier(0.4, 0, 0.2, 1);
        cursor: pointer;
        border-radius: 16px;
        background: linear-gradient(145deg, #ffffff, #f8f9fa);
        border: 2px solid transparent;
        position: relative;
        overflow: hidden;
    }
    .home-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 4px;
        background: linear-gradient(90deg, transparent, var(--primary-green), transparent);
        opacity: 0;
        transition: opacity 0.3s ease;
    }
    .home-card:hover {
        transform: translateY(-8px) scale(1.02);
        box-shadow: 0 12px 40px rgba(0,0,0,0.18);
        border-color: var(--primary-green-light);
    }
    .home-card:hover::before {
        opacity: 1;
    }

    /* Feature Icons */
    .feature-icon {
        width: 80px;
        height: 80px;
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        margin: 0 auto 1.5rem;
        transition: all 0.3s ease;
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
    }
    .home-card:hover .feature-icon {
        transform: scale(1.1) rotate(5deg);
        box-shadow: 0 6px 20px rgba(0,0,0,0.15);
    }
    .feature-icon-search {
        background: linear-gradient(135deg, #e3f2fd, #bbdefb);
        color: #1565c0;
    }
    .feature-icon-parallels {
        background: linear-gradient(135deg, #e8f5e9, #c8e6c9);
        color: var(--primary-green);
    }
    .feature-icon-browse {
        background: linear-gradient(135deg, #fff8e1, #ffecb3);
        color: #f57f17;
    }

    /* Highlight for search matches */
    mark, .highlight {
        background: linear-gradient(180deg, transparent 60%, #ffeb3b 60%);
        padding: 0 2px;
        border-radius: 2px;
    }
    .highlight-strong {
        background: #ffeb3b;
        padding: 2px 4px;
        border-radius: 3px;
        font-weight: 600;
    }

    /* Badges */
    .source-badge {
        font-size: 0.75rem;
        padding: 2px 8px;
        border-radius: 12px;
        font-weight: 600;
    }
    .source-v08 {
        background: #e8f5e9;
        color: var(--primary-green-dark);
    }
    .source-v07 {
        background: #fff3e0;
        color: #e65100;
    }

    /* Header Navigation */
    .nav-link {
        position: relative;
        padding: 8px 16px;
        border-radius: 8px;
        transition: all 0.2s ease;
    }
    .nav-link:hover {
        background: rgba(255,255,255,0.15);
    }
    .nav-link::after {
        content: '';
        position: absolute;
        bottom: 0;
        left: 50%;
        width: 0;
        height: 2px;
        background: white;
        transition: all 0.3s ease;
        transform: translateX(-50%);
    }
    .nav-link:hover::after {
        width: 80%;
    }

    /* Image Viewer */
    .image-panel {
        background: #1a1a1a;
        min-height: 500px;
        position: relative;
        overflow: hidden;
    }
    .image-panel img {
        max-width: 100%;
        max-height: 100%;
        object-fit: contain;
    }

    /* Transcription Panel */
    .transcription-panel {
        background: var(--bg-parchment);
        min-height: 500px;
        padding: 24px;
        overflow-y: auto;
    }

    /* Status Cards */
    .status-warning {
        background: linear-gradient(135deg, #fff8e1, #ffecb3);
        border-left: 4px solid var(--accent-gold);
    }
    .status-success {
        background: linear-gradient(135deg, #e8f5e9, #c8e6c9);
        border-left: 4px solid var(--primary-green);
    }
    .status-error {
        background: linear-gradient(135deg, #ffebee, #ffcdd2);
        border-left: 4px solid #c62828;
    }

    /* Responsive */
    @media (max-width: 768px) {
        .home-card {
            width: 100% !important;
        }
    }

    /* Loading Animation */
    .loading-pulse {
        animation: pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite;
    }
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.5; }
    }

    /* Green Theme Buttons */
    .btn-primary {
        background: var(--primary-green) !important;
        color: white !important;
    }
    .btn-primary:hover {
        background: var(--primary-green-dark) !important;
    }
</style>
'''


# ============================================================================
# Header & Navigation
# ============================================================================

def create_header():
    """Create the professional application header with navigation."""
    with ui.header().classes('items-center justify-between px-6 py-3').style(
        'background: linear-gradient(135deg, #1b5e20, #2e7d32); box-shadow: 0 2px 10px rgba(0,0,0,0.2);'
    ):
        # Logo/Title
        with ui.row().classes('items-center gap-3'):
            ui.icon('auto_stories', size='2rem').classes('text-white')
            ui.link(tr('Genizah Search'), '/').classes(
                'text-xl font-bold no-underline text-white hover:text-green-200'
            ).style('letter-spacing: 0.5px;')

        # Navigation
        with ui.row().classes('items-center gap-2'):
            ui.link(tr('Search'), '/search').classes(
                'nav-link text-white hover:text-green-100 no-underline'
            )
            ui.link(tr('Find Parallels'), '/parallels').classes(
                'nav-link text-white hover:text-green-100 no-underline'
            )
            ui.link(tr('Browse'), '/browse').classes(
                'nav-link text-white hover:text-green-100 no-underline'
            )

            # Settings dropdown
            with ui.button(icon='settings').props('flat round').classes('text-white ml-4'):
                with ui.menu().classes('p-3').style('min-width: 200px;'):
                    ui.label(tr('Settings')).classes('font-bold px-2 py-1 text-green-800')
                    ui.separator().classes('my-2')

                    with ui.row().classes('items-center gap-3 px-2 py-1'):
                        ui.icon('language', size='1.2rem').classes('text-gray-600')
                        ui.label(tr('Language')).classes('text-gray-700')

                    def on_lang_change(e):
                        set_language(e.value)
                        ui.navigate.reload()

                    ui.select(
                        {'he': 'עברית', 'en': 'English'},
                        value=get_language(),
                        on_change=on_lang_change
                    ).classes('w-full mt-1').props('outlined dense')


def create_footer():
    """Create the application footer."""
    with ui.footer().classes('text-center py-6').style(
        'background: linear-gradient(135deg, #f5f5f5, #e8e8e8); border-top: 2px solid #c8e6c9;'
    ):
        with ui.column().classes('items-center gap-3'):
            with ui.row().classes('items-center justify-center gap-2'):
                ui.icon('auto_stories', size='1.2rem').classes('text-green-700')
                ui.label(tr('Cairo Genizah Search Engine')).classes('text-gray-700 font-semibold')

            with ui.row().classes('items-center justify-center gap-4 text-sm'):
                ui.link(tr('About'), '/about').classes('text-green-700 no-underline hover:underline')
                ui.label('•').classes('text-gray-400')
                ui.label('v0.8').classes('text-gray-500')
                ui.label('•').classes('text-gray-400')
                ui.label('450K+ ' + tr('Pages')).classes('text-gray-500')


def add_page_head():
    """Add common head elements to pages."""
    # Set RTL direction on the document
    direction = get_dir()
    lang = get_language()

    ui.add_head_html(f'''
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <link href="https://fonts.googleapis.com/css2?family=Noto+Sans+Hebrew:wght@400;500;700&display=swap" rel="stylesheet">
    ''')
    ui.add_head_html(COMMON_STYLES)

    # Add RTL body styles for Hebrew
    if direction == 'rtl':
        ui.add_head_html('''
        <style>
            body, html { direction: rtl; }
            .q-field__native, .q-field__input, textarea, input { direction: rtl; text-align: right; }
            .q-card, .q-expansion-item { direction: rtl; }
        </style>
        ''')


# ============================================================================
# Home Page
# ============================================================================

@ui.page('/')
def home_page():
    """Home page with three main feature cards."""
    add_page_head()
    create_header()

    with ui.column().classes('w-full max-w-6xl mx-auto p-8'):
        # Hero section
        with ui.column().classes('text-center mb-12'):
            with ui.row().classes('items-center justify-center gap-4 mb-4'):
                ui.icon('auto_stories', size='3.5rem').classes('text-green-700')
                ui.label(tr('Genizah Search')).classes(
                    'text-5xl font-bold'
                ).style('color: #1b5e20; letter-spacing: 1px;')

            ui.label(tr('Cairo Genizah Search Engine')).classes(
                'text-xl text-gray-600 rtl-text hebrew-text'
            )

            # Stats row
            service = get_service()
            if service.is_ready and service.index_exists:
                with ui.row().classes('items-center justify-center gap-8 mt-6'):
                    with ui.column().classes('items-center'):
                        ui.label('450K+').classes('text-2xl font-bold text-green-700')
                        ui.label(tr('Pages')).classes('text-sm text-gray-500')
                    with ui.column().classes('items-center'):
                        ui.label('V0.8').classes('text-2xl font-bold text-green-700')
                        ui.label(tr('Latest Data')).classes('text-sm text-gray-500')

        # Cards grid
        with ui.row().classes('w-full justify-center gap-8 flex-wrap'):

            # Search Card
            with ui.card().classes('home-card w-80 p-8 text-center').on(
                'click', lambda: ui.navigate.to('/search')
            ):
                with ui.element('div').classes('feature-icon feature-icon-search'):
                    ui.icon('search', size='2.5rem')
                ui.label(tr('Text Search')).classes(
                    'text-2xl font-bold mb-3'
                ).style('color: #1565c0;')
                ui.label(tr('Search for words and phrases in the Genizah corpus')).classes(
                    'text-gray-600 rtl-text hebrew-text text-sm'
                )
                with ui.row().classes('justify-center gap-2 mt-4'):
                    ui.badge(tr('Variants'), color='blue').props('outline')
                    ui.badge(tr('Fuzzy'), color='blue').props('outline')
                    ui.badge('Regex', color='blue').props('outline')

            # Parallels Card
            with ui.card().classes('home-card w-80 p-8 text-center').on(
                'click', lambda: ui.navigate.to('/parallels')
            ):
                with ui.element('div').classes('feature-icon feature-icon-parallels'):
                    ui.icon('compare_arrows', size='2.5rem')
                ui.label(tr('Find Parallels')).classes(
                    'text-2xl font-bold mb-3'
                ).style('color: #2e7d32;')
                ui.label(tr('Enter a long text and find parallel texts in the Genizah')).classes(
                    'text-gray-600 rtl-text hebrew-text text-sm'
                )
                with ui.row().classes('justify-center gap-2 mt-4'):
                    ui.badge(tr('Chunk Analysis'), color='green').props('outline')
                    ui.badge(tr('Scoring'), color='green').props('outline')

            # Browse Card
            with ui.card().classes('home-card w-80 p-8 text-center').on(
                'click', lambda: ui.navigate.to('/browse')
            ):
                with ui.element('div').classes('feature-icon feature-icon-browse'):
                    ui.icon('menu_book', size='2.5rem')
                ui.label(tr('Browse Manuscripts')).classes(
                    'text-2xl font-bold mb-3'
                ).style('color: #f57f17;')
                ui.label(tr('Enter a shelfmark to browse the manuscript')).classes(
                    'text-gray-600 rtl-text hebrew-text text-sm'
                )
                with ui.row().classes('justify-center gap-2 mt-4'):
                    ui.badge(tr('Images'), color='amber').props('outline')
                    ui.badge(tr('Transcriptions'), color='amber').props('outline')

        # Status indicator
        if not service.is_ready:
            with ui.card().classes('w-full max-w-md mx-auto mt-8 p-4 status-warning'):
                with ui.row().classes('items-center gap-3'):
                    ui.icon('warning', color='orange', size='1.5rem')
                    with ui.column():
                        ui.label(tr('Service not available')).classes('font-medium text-yellow-900')
                        if service.init_error:
                            ui.label(str(service.init_error)).classes('text-sm text-yellow-800')
        elif not service.index_exists:
            with ui.card().classes('w-full max-w-md mx-auto mt-8 p-4 status-warning'):
                with ui.row().classes('items-center gap-3'):
                    ui.icon('info', color='orange', size='1.5rem')
                    ui.label(tr('Index not found')).classes('font-medium text-yellow-900')

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
        ui.label(tr('About')).classes('text-3xl font-bold mb-6 text-green-800')

        with ui.card().classes('w-full p-8').style('background: var(--bg-parchment);'):
            ui.markdown('''
## חיפוש גניזה | Genizah Search

מנוע חיפוש מתקדם לאוסף כתבי היד של גניזת קהיר.

### תכונות עיקריות:
- **חיפוש טקסט מלא** עם תמיכה בוריאנטים של עברית
- **סבילות לשגיאות OCR** דרך התאמת וריאנטים חכמה
- **חיפוש מקבילות** לאיתור טקסטים מקבילים
- **דפדוף בכתבי יד** עם צפייה בתמונות
- **חיפוש חוצה עמודים** למסמכים רציפים

### טכנולוגיה:
- מנוע חיפוש Tantivy (מבוסס Rust)
- ממשק NiceGUI
- Backend ב-Python
- תמונות דרך IIIF מהספרייה הלאומית

---

*פותח עבור חוקרי גניזה ואוהבי כתבי יד עבריים*
            ''').classes('rtl-text hebrew-text')

    create_footer()


# ============================================================================
# Startup
# ============================================================================

def startup():
    """Application startup handler."""
    print("=" * 60)
    print("  Genizah Search Web Interface")
    print("=" * 60)
    print("\nInitializing service...")

    if not init_service():
        print(f"⚠ Warning: Service initialization failed")
        service = get_service()
        if service.init_error:
            print(f"  Error: {service.init_error}")
    else:
        service = get_service()
        if service.index_exists:
            print("✓ Service initialized successfully")
            print("✓ Search index found and ready")
        else:
            print("⚠ Service initialized but index not found")
            print("  Search functionality will not work")

    print("\n" + "=" * 60)


# Register startup handler
app.on_startup(startup)


if __name__ in {'__main__', '__mp_main__'}:
    print(f"\n🚀 Starting Genizah Search Web on port {APP_PORT}...")
    print(f"   Open http://localhost:{APP_PORT} in your browser\n")

    ui.run(
        title=APP_TITLE,
        port=APP_PORT,
        reload=True,
        show=True,
        favicon='📜',
    )
