#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GenizahSearch Web Application - Professional Research Interface

A modern, high-quality web interface for Cairo Genizah manuscript research.
Designed with academic researchers in mind, providing powerful search tools
with an intuitive, accessible interface.

Run with: python -m web.main (from project root)
"""

import os
import sys
import asyncio

# Load environment variables first (for Supabase configuration)
from dotenv import load_dotenv
load_dotenv()

# Ensure we can import from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nicegui import ui, app, run
from web.state import state
from web.api import init_api_routes
from web.translations import tr, is_rtl, get_dir, set_language, get_language
from genizah_core import MetadataManager, VariantManager, SearchEngine, LabEngine, Indexer, ListsManager, Config

# App configuration
APP_TITLE = "Dicta Genizah Search | חיפוש גניזת קהיר"
APP_VERSION = "5.8"
WHATS_NEW_VERSION = "5.8"  # Bump when adding new "What's New" content
APP_PORT = int(os.environ.get('GENIZAH_PORT', 8081))

# Initialize API routes (Image Proxy, Export)
init_api_routes()

# Serve static files for SEO images
STATIC_DIR = os.path.join(os.path.dirname(__file__), 'static')
app.add_static_files('/static', STATIC_DIR)

# ============================================================================
# Website Metadata - SEO & Social Sharing
# ============================================================================

META_TAGS = '''
<!-- Meta Tags -->
<meta name="description" content="Dicta Genizah Search - חיפוש גניזת קהיר. Advanced research platform with full-text search across 500,000+ Cairo Genizah manuscript fragments.">
<meta name="keywords" content="Dicta Genizah Search, חיפוש גניזה, גניזת קהיר, כתבי יד, גניזה קהירית, מחקר גניזה, Cairo Genizah, Genizah search, manuscripts, Jewish manuscripts">
<meta name="author" content="Dicta Genizah Search">
<meta name="theme-color" content="#059669">

<!-- Open Graph / Facebook / WhatsApp / Slack -->
<meta property="og:type" content="website">
<meta property="og:url" content="https://GenizahSearch.com/">
<meta property="og:title" content="Dicta Genizah Search | Cairo Genizah Manuscript Research Platform">
<meta property="og:description" content="Dicta Genizah Search - חיפוש גניזת קהיר. Advanced research platform with full-text search across 500,000+ Cairo Genizah manuscript fragments.">
<meta property="og:image" content="https://GenizahSearch.com/static/og-image.png">
<meta property="og:locale" content="he_IL">
<meta property="og:site_name" content="Dicta Genizah Search">

<!-- Twitter Card -->
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:url" content="https://GenizahSearch.com/">
<meta name="twitter:title" content="Dicta Genizah Search | Cairo Genizah Manuscript Research Platform">
<meta name="twitter:description" content="Dicta Genizah Search - חיפוש גניזת קהיר. Advanced research platform with full-text search across 500,000+ Cairo Genizah manuscript fragments.">
<meta name="twitter:image" content="https://GenizahSearch.com/static/og-image.png">

<!-- Canonical URL -->
<link rel="canonical" href="https://GenizahSearch.com/">
'''

# Google Analytics
ANALYTICS_SCRIPT = '''
<!-- Google tag (gtag.js) -->
<script async src="https://www.googletagmanager.com/gtag/js?id=G-LXT1PTKG3E"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){dataLayer.push(arguments);}
  gtag('js', new Date());
  gtag('config', 'G-LXT1PTKG3E');
</script>
'''

# PostHog Analytics (real-user monitoring + session recordings)
_posthog_key = os.environ.get('POSTHOG_API_KEY', '')
POSTHOG_SCRIPT = f'''
<!-- PostHog Analytics -->
<script>
    !function(t,e){{var o,n,p,r;e.__SV||(window.posthog=e,e._i=[],e.init=function(i,s,a){{function g(t,e){{var o=e.split(".");2==o.length&&(t=t[o[0]],e=o[1]),t[e]=function(){{t.push([e].concat(Array.prototype.slice.call(arguments,0)))}}}}(p=t.createElement("script")).type="text/javascript",p.crossOrigin="anonymous",p.async=!0,p.src=s.api_host.replace(".i.posthog.com","-assets.i.posthog.com")+"/static/array.js",(r=t.getElementsByTagName("script")[0]).parentNode.insertBefore(p,r);var u=e;for(void 0!==a?u=e[a]=[]:a="posthog",u.people=u.people||[],u.toString=function(t){{var e="posthog";return"posthog"!==a&&(e+="."+a),t||(e+=" (stub)"),e}},u.people.toString=function(){{return u.toString(1)+".people (stub)"}},o="init capture register register_once register_for_session unregister unregister_for_session getFeatureFlag getFeatureFlagPayload isFeatureEnabled reloadFeatureFlags updateEarlyAccessFeatureEnrollment getEarlyAccessFeatures on onFeatureFlags onSessionId getSurveys getActiveMatchingSurveys renderSurvey canRenderSurvey getNextSurveyStep identify setPersonProperties group resetGroups setPersonPropertiesForFlags resetPersonPropertiesForFlags setGroupPropertiesForFlags resetGroupPropertiesForFlags reset get_distinct_id getGroups get_session_id get_session_replay_url lib get_property getSessionProperty sessionRecording startSessionRecording stopSessionRecording sessionRecordingStarted captureException loadToolbar get_config __request_queue".split(" "),n=0;n<o.length;n++)g(u,o[n]);e._i.push([i,s,a])}},e.__SV=1)}}(document,window.posthog||[]);
    posthog.init('{_posthog_key}', {{
        api_host: 'https://us.i.posthog.com',
        person_profiles: 'identified_only',
        autocapture: true,
        capture_pageview: true,
        capture_pageleave: true,
        session_recording: {{
            maskAllInputs: true,
            maskTextSelector: 'input, textarea'
        }}
    }})
</script>
''' if _posthog_key else ''

# ============================================================================
# Modern Theme System - Professional Research UI
# ============================================================================

COMMON_STYLES = '<link rel="stylesheet" href="/static/common.css">'


# ============================================================================
# Layout Components
# ============================================================================

def create_layout():
    """Create the main application layout with modern Header and Sidebar."""

    # Restore persisted language preference (survives ui.navigate.reload)
    try:
        saved_lang = app.storage.user.get('ui_language')
        if saved_lang in ('he', 'en'):
            set_language(saved_lang)
    except Exception:
        pass

    current_page = app.storage.user.get('current_page', '/')
    rtl_mode = is_rtl()

    # Page loading progress bar element (CSS in COMMON_STYLES)
    ui.html('<div class="page-loading-bar" id="pageLoadingBar"></div>', sanitize=False)
    ui.add_head_html('''<script>
(function() {
    function showLoadingBar() {
        var bar = document.getElementById('pageLoadingBar');
        if (bar) {
            bar.classList.remove('complete');
            bar.classList.add('active');
        }
    }
    function hideLoadingBar() {
        var bar = document.getElementById('pageLoadingBar');
        if (bar) {
            bar.classList.remove('active');
            bar.classList.add('complete');
        }
    }

    // Expose globally so Python can call via ui.run_javascript
    window.__showLoadingBar = showLoadingBar;
    window.__hideLoadingBar = hideLoadingBar;

    // 1. Trigger on <a href> clicks (original behavior)
    document.addEventListener('click', function(e) {
        var link = e.target.closest('a[href]');
        if (!link) return;
        var href = link.getAttribute('href');
        if (href && href.startsWith('/') && !href.startsWith('//') && !link.target) {
            showLoadingBar();
        }
    });

    // 2. Trigger on Enter key in text inputs (search/shelfmark navigation)
    document.addEventListener('keydown', function(e) {
        if (e.key === 'Enter' && e.target.tagName === 'INPUT') {
            var skipTypes = ['submit', 'button', 'checkbox', 'radio', 'file'];
            if (skipTypes.indexOf(e.target.type) === -1) {
                showLoadingBar();
            }
        }
    });

    // 3. KEY FIX: Trigger on beforeunload - catches ALL navigation methods
    // This fires when ui.navigate.to() sets window.location, when <a> links navigate,
    // when the user uses back/forward, etc. It's the universal navigation event.
    window.addEventListener('beforeunload', function() {
        showLoadingBar();
    });

    // 4. Hide on page load (new page finished rendering)
    window.addEventListener('load', hideLoadingBar);

    // 5. Fallback: hide after 15 seconds if page didn't navigate
    // (for Enter key searches that update in-page instead of navigating)
    document.addEventListener('keydown', function(e) {
        if (e.key === 'Enter') {
            setTimeout(hideLoadingBar, 15000);
        }
    });
})();
</script>''')

    # Reference dictionary to store UI elements accessible across function scopes
    refs = {}

    # === Modular Header Rendering Functions ===
    # These allow us to reverse DOM order for RTL without using CSS order or row-reverse

    def render_header_left():
        """Render left section: Menu button + Logo"""
        with ui.row().classes('items-center gap-4') as section:
            refs['menu_btn'] = ui.button(icon='menu').props(f'flat round text-color=white aria-label="{tr("Open navigation menu")}"')

            # Logo - compact 2-line layout
            with ui.row().classes('items-center gap-3 cursor-pointer').on('click', lambda: ui.navigate.to('/')):
                ui.icon('auto_stories').classes('text-2xl text-white opacity-90')
                # Hide text on xs screens to prevent crowding; show on sm+
                with ui.column().classes('header-logo-text justify-center').style('gap: 2px;'):
                    ui.label('Dicta Genizah Search').classes('font-bold text-white tracking-wide').style('font-size: 15px; line-height: 1;')
                    ui.label('אתר הגניזה מבית דיקטה').classes('text-white/60').style('font-size: 11px; line-height: 1;')
        return section

    def render_header_center():
        """Render center section: Quick search"""
        with ui.row().classes('hidden md:flex items-center') as section:
            quick_search = ui.input(placeholder=tr('Quick search...')).classes('w-80').props('dark dense outlined rounded')
            quick_search.on('keydown.enter', lambda: ui.navigate.to(f'/search?q={quick_search.value}'))
        return section

    def render_header_right():
        """Render right section: Status + Auth + Help"""
        with ui.row().classes('items-center gap-2 sm:gap-4') as section:
            # Status Indicator with continuous heartbeat monitoring
            with ui.row().classes('items-center gap-2 bg-white/15 px-2 sm:px-4 py-1 sm:py-2 rounded-full status-indicator'):
                status_dot = ui.element('div').classes('w-2 h-2 rounded-full bg-yellow-400')
                status_text = ui.label(tr('Loading...')).classes('text-xs text-white/90 status-text hidden sm:block')

                # Track connection state for reconnection detection
                connection_state = {'was_connected': False, 'check_count': 0, 'timer': None}

                async def update_status():
                    """Heartbeat function that monitors both server readiness and WebSocket connection."""
                    try:
                        # Check if elements still exist (user might have navigated away)
                        if not status_dot.is_deleted and not status_text.is_deleted:
                            connection_state['check_count'] += 1

                            # Check if server-side state is ready
                            server_ready = state.is_ready()

                            # Perform a lightweight JavaScript ping to verify WebSocket connection
                            # This also tests the round-trip to catch connection issues
                            try:
                                ping_result = await ui.run_javascript('Date.now()', timeout=5.0)
                                ws_connected = ping_result is not None
                            except Exception:
                                ws_connected = False

                            if server_ready and ws_connected:
                                # All good - show green, steady (remove pulse animation)
                                status_dot.classes('bg-green-400', remove='bg-yellow-400 animate-pulse')
                                status_text.text = tr('Ready')
                                connection_state['was_connected'] = True
                            else:
                                # Loading or reconnecting - yellow with subtle pulse animation
                                # Don't show alarming text, just visual indicator
                                status_dot.classes('bg-yellow-400 animate-pulse', remove='bg-green-400')
                                # Keep showing "Ready" after initial connection to avoid alarming users
                                # The yellow pulsing dot is sufficient visual feedback
                                if not connection_state['was_connected']:
                                    status_text.text = tr('Loading...')
                                # else: keep current text (Ready) - don't change to alarming message
                        else:
                            # Elements deleted, deactivate timer
                            if connection_state['timer']:
                                connection_state['timer'].deactivate()
                    except Exception:
                        # If update itself fails, deactivate timer to prevent further errors
                        if connection_state['timer']:
                            connection_state['timer'].deactivate()

                # Run heartbeat every 10 seconds to monitor connection health
                # First check after 2 seconds, then continuous
                ui.timer(2.0, update_status, once=True)
                connection_state['timer'] = ui.timer(10.0, update_status)

            # Auth Buttons (Login/Register or User Menu)
            with ui.row().classes('auth-buttons'):
                from web.auth_state import create_auth_buttons
                create_auth_buttons()

            # Help Button (hidden on mobile via CSS)
            ui.button(icon='help_outline', on_click=lambda: ui.navigate.to('/help')).props('flat round text-color=white').tooltip(tr('Help')).classes('help-btn-header')
        return section

    # === Build Header with correct DOM order ===
    # reveal: hide header on scroll down, show on scroll up (mobile-friendly)
    with ui.header().classes('q-py-none header-reveal-mobile').props('reveal').style('height: 64px;'):
        with ui.row().classes('w-full h-full items-center justify-between px-6 app-header'):
            if rtl_mode:
                # RTL: Render Right -> Center -> Left for correct tab order
                render_header_right()
                render_header_center()
                render_header_left()
            else:
                # LTR: Normal order Left -> Center -> Right
                render_header_left()
                render_header_center()
                render_header_right()

    # Sidebar (Drawer)
    # Use stored state, default to True (open) on desktop
    # On mobile (< 768px), we will close it after page load via JavaScript
    drawer_open = app.storage.user.get('drawer_open', True)

    # Set drawer side based on RTL mode - Quasar will handle page padding correctly
    drawer_side = 'right' if rtl_mode else 'left'
    main_drawer = ui.drawer(side=drawer_side, value=drawer_open, bordered=True).classes('shadow-xl').props('width=280 breakpoint=768')

    # Close drawer on mobile by default (screen width < 768px)
    # This runs once on page load to ensure mobile users don't see the drawer overlay
    async def close_drawer_on_mobile():
        """Close drawer if screen width indicates mobile device."""
        try:
            screen_width = await ui.run_javascript('window.innerWidth')
            if screen_width and screen_width < 768:
                main_drawer.set_value(False)
        except Exception:
            pass  # Silently ignore if JavaScript fails

    # Use a short timer to run after page is fully loaded
    ui.timer(0.5, close_drawer_on_mobile, once=True)

    # Content Area
    content_col = ui.column().classes('main-content w-full items-stretch flex-grow')
    # Add ID for skip link target
    content_col.props('id=main-content')

    # === "What's New" Banner (dismissible, shown once per version) ===
    if app.storage.user.get('whats_new_dismissed') != WHATS_NEW_VERSION:
        banner_dir = 'rtl' if rtl_mode else 'ltr'
        with content_col:
            with ui.card().classes('w-full mx-auto max-w-5xl p-4 border-l-4 mt-4').style(
                f'background: var(--bg-tertiary); border-left-color: #10b981; direction: {banner_dir};'
            ) as whats_new_banner:
                with ui.row().classes('w-full items-start gap-4'):
                    ui.icon('new_releases').classes('text-2xl mt-1').style('color: #10b981;')
                    with ui.column().classes('flex-1 gap-2'):
                        ui.label(tr("New Features!")).classes('text-base font-bold').style('color: var(--text-primary);')
                        with ui.column().classes('gap-1'):
                            with ui.row().classes('items-start gap-2'):
                                ui.label('•').style('color: var(--text-secondary);')
                                ui.label(tr('Responsa-Project style Search: advanced syntax parsing with grammatical expansion, Judeo-Arabic support, and tabular query builder')).classes('text-sm').style('color: var(--text-secondary);')
                            with ui.row().classes('items-start gap-2'):
                                ui.label('•').style('color: var(--text-secondary);')
                                ui.label(tr('PGP Integration: 35,000 documents from the Princeton Geniza Project with editions, translations, and more information')).classes('text-sm').style('color: var(--text-secondary);')
                            with ui.row().classes('items-start gap-2'):
                                ui.label('•').style('color: var(--text-secondary);')
                                ui.label(tr('FJMS scholarly metadata: 390K domain classifications, 48K scientific joins, and 500K catalog records from the Friedberg Genizah Project')).classes('text-sm').style('color: var(--text-secondary);')
                    def dismiss_whats_new():
                        app.storage.user['whats_new_dismissed'] = WHATS_NEW_VERSION
                        whats_new_banner.delete()
                    ui.button(tr('Got it!'), icon='check', on_click=dismiss_whats_new).props('flat dense')

    def toggle_drawer():
        """Toggle drawer and save state."""
        main_drawer.toggle()
        app.storage.user['drawer_open'] = not app.storage.user.get('drawer_open', True)

    def nav_to(path):
        """Navigate to path. Drawer auto-hides on mobile via breakpoint=768."""
        ui.navigate.to(path)

    # Connect menu button to toggle function (using refs dictionary)
    if 'menu_btn' in refs:
        refs['menu_btn'].on('click', toggle_drawer)

    with main_drawer:
        with ui.column().classes('h-full'):
            # Navigation Section
            with ui.column().classes('flex-grow py-4'):
                ui.label(tr('NAVIGATION')).classes('nav-section-label')

                nav_items = [
                    ('/', 'home', tr('Home'), None),
                    ('/about', 'info', tr('About the Genizah'), None),
                    ('/search', 'search', tr('Search'), None),
                    ('/parallels', 'compare_arrows', tr('Find Parallels'), None),
                    ('/browse', 'menu_book', tr('Browse'), None),
                    ('/discoveries', 'lightbulb', tr('Community'), None),
                    ('/lists', 'star', tr('My Lists'), None),
                ]

                for path, icon, label, badge in nav_items:
                    is_active = current_page == path

                    with ui.row().classes(f'nav-item {"active" if is_active else ""}').on('click', lambda p=path: nav_to(p)):
                        ui.icon(icon).classes('nav-item-icon')
                        ui.label(label)
                        if badge:
                            ui.label(badge).classes('nav-item-badge')

                ui.separator().classes('my-4 mx-6')

                ui.label(tr('TOOLS')).classes('nav-section-label')

                tool_items = [
                    ('/download', 'download', tr('Download App'), None),
                    ('/settings', 'settings', tr('Settings'), None),
                    ('/help', 'help_center', tr('Help Center'), None),
                ]

                for path, icon, label, badge in tool_items:
                    is_active = current_page == path
                    with ui.row().classes(f'nav-item {"active" if is_active else ""}').on('click', lambda p=path: nav_to(p)):
                        ui.icon(icon).classes('nav-item-icon')
                        ui.label(label)

            # Footer Section
            with ui.column().classes('sidebar-footer gap-4'):
                # Language Toggle
                def toggle_lang():
                    current = get_language()
                    new_lang = 'en' if current == 'he' else 'he'
                    try:
                        app.storage.user['ui_language'] = new_lang
                    except Exception:
                        pass
                    set_language(new_lang)
                    ui.navigate.reload()

                lang_btn_text = "English" if get_language() == 'he' else "עברית"
                with ui.row().classes('w-full items-center justify-center gap-2 cursor-pointer opacity-80 hover:opacity-100').props(
                    'role=button tabindex=0'
                ).on('click', toggle_lang).on('keydown.enter', toggle_lang).on('keydown.space', toggle_lang):
                    ui.icon('translate').classes('text-lg')
                    ui.label(lang_btn_text).classes('text-sm font-medium')

                # Theme Switcher
                with ui.row().classes('theme-switcher w-full'):
                    def set_theme(theme_name):
                        app.storage.user['theme'] = theme_name
                        ui.run_javascript(f'''
                            document.documentElement.setAttribute("data-theme", "{theme_name}");
                            document.body.setAttribute("data-theme", "{theme_name}");
                            document.querySelectorAll(".theme-btn").forEach(btn => btn.classList.remove("active"));
                            document.querySelector(".theme-btn-{theme_name}").classList.add("active");
                        ''')

                    current_theme = app.storage.user.get('theme', 'light')

                    with ui.button(icon='light_mode', on_click=lambda: set_theme('light')).props('flat round size=sm').classes(f'theme-btn theme-btn-light {"active" if current_theme == "light" else ""}'): pass
                    with ui.button(icon='history_edu', on_click=lambda: set_theme('parchment')).props('flat round size=sm').classes(f'theme-btn theme-btn-parchment {"active" if current_theme == "parchment" else ""}'): pass
                    with ui.button(icon='dark_mode', on_click=lambda: set_theme('dark')).props('flat round size=sm').classes(f'theme-btn theme-btn-dark {"active" if current_theme == "dark" else ""}'): pass

                # Version Info (hidden - using "formerly" in settings instead)
                # ui.label(f'v{APP_VERSION}').classes('text-xs text-center opacity-50 mt-2')

                # Accessibility Link
                with ui.row().classes('w-full justify-center mt-2'):
                    ui.link(tr('Accessibility Statement'), '/accessibility').classes('text-xs opacity-70 hover:opacity-100').style('text-decoration: none; color: inherit;')

                # Creator Credit
                ui.label(tr('Created by Hillel Gershuni')).classes('text-xs text-center opacity-50 mt-1')

    # Global Footer with Citation Note (dismissible)
    full_citation = 'Stoekl Ben Ezra et al. (2025). MiDRASH Automatic Transcriptions of the Cairo Geniza Fragments. https://doi.org/10.5281/zenodo.17734473'
    footer = ui.footer().classes('citation-footer')
    with footer:
        with ui.row().classes('w-full items-center justify-center gap-2 py-2 px-4 flex-wrap'):
            # Copy button
            ui.button(icon='content_copy', on_click=lambda: ui.run_javascript(f'navigator.clipboard.writeText("{full_citation}"); alert("{tr("Citation copied!")}")')).props('flat dense size=xs').classes('opacity-70 hover:opacity-100').tooltip(tr('Copy citation'))
            # Hebrew label (hidden on mobile) - comes before citation for correct RTL order
            ui.label(tr('When publishing material from this site, please cite:')).classes('text-xs opacity-80 citation-hebrew-label')
            # Citation link (English, LTR)
            ui.link(full_citation, 'https://doi.org/10.5281/zenodo.17734473', new_tab=True).classes('text-xs font-medium citation-link').style('direction: ltr; text-decoration: none;')
            # Close button
            ui.button(icon='close', on_click=lambda: ui.run_javascript('localStorage.setItem("citation_footer_dismissed", "true"); document.querySelector(".citation-footer").style.display = "none";')).props('flat dense size=xs').classes('opacity-50 hover:opacity-100').tooltip(tr('Dismiss'))

    # Check if footer was dismissed and hide it
    ui.run_javascript('if(localStorage.getItem("citation_footer_dismissed") === "true") { document.querySelector(".citation-footer").style.display = "none"; }')

    return content_col


# ============================================================================
# Page Routes
# ============================================================================

def apply_theme_immediately():
    """Add script to apply theme before page renders to prevent flash."""
    current_theme = app.storage.user.get('theme', 'light')
    current_lang = get_language()
    bg_color = "#0f172a" if current_theme == "dark" else "#fffbf5" if current_theme == "parchment" else "#f8fafc"

    # Use proper direction based on language (RTL for Hebrew, LTR for English)
    dir_attr = get_dir()

    # Add Hebrew-specific class if needed
    body_class_script = 'document.body.classList.add("hebrew-mode");' if current_lang == 'he' else 'document.body.classList.remove("hebrew-mode");'

    # Use immediate inline script that runs before any rendering
    return f'''<style>
        /* Pre-set theme to prevent flash - must be first */
        html, body {{
            background-color: {bg_color} !important;
        }}
        html[data-theme="dark"], body[data-theme="dark"] {{
            background-color: #0f172a !important;
        }}
        html[data-theme="parchment"], body[data-theme="parchment"] {{
            background-color: #fffbf5 !important;
        }}
        html[data-theme="light"], body[data-theme="light"] {{
            background-color: #f8fafc !important;
        }}
    </style>
    <script>
        (function() {{
            var theme = "{current_theme}";
            var lang = "{current_lang}";
            var dir = "{dir_attr}";
            var isRtl = (dir === "rtl");

            // Apply to html immediately (before DOM ready)
            document.documentElement.setAttribute("data-theme", theme);
            document.documentElement.lang = lang;
            document.documentElement.dir = dir;

            // Apply theme function
            var applyTheme = function() {{
                document.documentElement.setAttribute("data-theme", theme);
                document.documentElement.lang = lang;
                document.documentElement.dir = dir;
                if (document.body) {{
                    document.body.setAttribute("data-theme", theme);
                    if (lang === 'he') {{
                        document.body.classList.add("hebrew-mode");
                    }} else {{
                        document.body.classList.remove("hebrew-mode");
                    }}
                }}
            }};

            // Activate Quasar RTL mode when ready
            var activateQuasarRtl = function() {{
                if (typeof Quasar !== 'undefined' && isRtl) {{
                    // Try to use Hebrew language pack first, fallback to generic RTL
                    if (Quasar.lang && Quasar.lang.he) {{
                        Quasar.lang.set(Quasar.lang.he);
                    }} else if (Quasar.lang && Quasar.lang.set) {{
                        // Fallback: Set RTL mode without full language pack
                        Quasar.lang.set({{ rtl: true }});
                    }}
                }}
            }};

            // Execute immediately
            applyTheme();

            // Backup for body element when it exists
            if (document.readyState === 'loading') {{
                document.addEventListener('DOMContentLoaded', function() {{
                    applyTheme();
                    activateQuasarRtl();
                }});
            }} else {{
                activateQuasarRtl();
            }}
        }})();
    </script>'''

def set_current_page(page_path: str):
    """Safely set the current page in user storage."""
    try:
        app.storage.user['current_page'] = page_path
    except (AssertionError, KeyError, Exception):
        pass  # Storage not ready yet, ignore

@ui.page('/')
def dashboard_page():
    set_current_page('/')
    current_theme = app.storage.user.get('theme', 'light') if hasattr(app.storage, 'user') else 'light'
    ui.add_head_html(META_TAGS)
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages import home
        if hasattr(home, 'create_page'):
            home.create_page()

@ui.page('/search')
def search_page_route(
    q: str = None, tag: str = None,
    mode: str = None, variants: int = None,
    ja: int = None, flex_spaces: int = None,
    bidirectional: int = None, domain: str = None
):
    set_current_page('/search')
    ui.add_head_html(META_TAGS)
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.search import create_search_page
        create_search_page(
            initial_query=q, initial_tag=tag,
            initial_mode=mode, initial_variants=variants,
            initial_ja=ja, initial_flex_spaces=flex_spaces,
            initial_bidirectional=bidirectional, initial_domain=domain
        )

@ui.page('/parallels')
def parallels_page_route(text: str = None):
    set_current_page('/parallels')
    ui.add_head_html(META_TAGS)
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.parallels import create_parallels_page
        create_parallels_page(initial_text=text)

@ui.page('/browse')
def browse_page_route(sys_id: str = None, highlight: str = None, fl_id: str = None, page: int = None):
    set_current_page('/browse')
    ui.add_head_html(META_TAGS)
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.browse import create_browse_page
        create_browse_page(initial_sys_id=sys_id, highlight=highlight, initial_fl_id=fl_id, initial_page=page)

@ui.page('/lists')
def lists_page_route():
    set_current_page('/lists')
    ui.add_head_html(META_TAGS)
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.lists import create_lists_page
        create_lists_page()

@ui.page('/reset-hints')
def reset_hints_route():
    """Hidden utility route to reset all feature discovery hints."""
    for key in ('whats_new_dismissed', 'hint_responsa_seen', 'hint_tabular_seen'):
        app.storage.user.pop(key, None)
    ui.navigate.to('/')

@ui.page('/settings')
def settings_page_route():
    set_current_page('/settings')
    ui.add_head_html(META_TAGS)
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.settings import create_settings_page
        create_settings_page()

@ui.page('/help')
def help_page_route():
    set_current_page('/help')
    ui.add_head_html(META_TAGS)
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.help import create_help_page
        create_help_page()

@ui.page('/corrections')
async def corrections_page_route():
    set_current_page('/corrections')
    ui.add_head_html(META_TAGS)
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.corrections import create_corrections_page
        await create_corrections_page()

@ui.page('/discoveries')
def discoveries_page_route():
    set_current_page('/discoveries')
    ui.add_head_html(META_TAGS)
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.discoveries import create_discoveries_page
        create_discoveries_page()

@ui.page('/admin')
async def admin_page_route():
    set_current_page('/admin')
    ui.add_head_html(META_TAGS)
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.admin import create_admin_page
        await create_admin_page()

@ui.page('/profile')
async def profile_page_route():
    set_current_page('/profile')
    ui.add_head_html(META_TAGS)
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.profile import create_profile_page
        await create_profile_page()

@ui.page('/accessibility')
def accessibility_page_route():
    set_current_page('/accessibility')
    ui.add_head_html(META_TAGS)
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.accessibility import create_accessibility_page
        create_accessibility_page()


@ui.page('/about', title='What is the Cairo Genizah? | מהי גניזת קהיר? — Dicta Genizah Search')
def about_page_route():
    set_current_page('/about')
    # Page-specific meta tags override the site-wide defaults
    ui.add_head_html('''
    <!-- About page SEO overrides -->
    <meta name="description" content="The Cairo Genizah: over 350,000 medieval manuscript fragments from the Ben Ezra Synagogue in Cairo, spanning 1,000 years of Jewish life. Search the transcriptions for the first time.">
    <meta property="og:title" content="What is the Cairo Genizah? — Dicta Genizah Search">
    <meta property="og:description" content="Over 350,000 medieval manuscript fragments from a Cairo synagogue attic, now searchable for the first time. Explore letters, contracts, poetry, and Torah from 1,000 years of Jewish life.">
    <meta property="og:url" content="https://GenizahSearch.com/about">
    <meta property="og:type" content="article">
    <meta name="twitter:title" content="What is the Cairo Genizah? — Dicta Genizah Search">
    <meta name="twitter:description" content="Over 350,000 medieval manuscript fragments from a Cairo synagogue attic, now searchable for the first time.">
    <link rel="canonical" href="https://GenizahSearch.com/about">
    ''')
    ui.add_head_html(META_TAGS)
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.about import create_about_page
        create_about_page()


@ui.page('/download')
def download_page_route():
    set_current_page('/download')
    ui.add_head_html(META_TAGS)
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.download import create_download_page
        create_download_page()


@ui.page('/auth/callback')
async def auth_callback_route(code: str = None):
    """
    OAuth callback handler.
    Supabase redirects here after Google login with either:
    - ?code= parameter (PKCE flow) - needs code exchange (fallback)
    - #access_token= hash (implicit flow) - direct tokens (preferred)
    """
    from web.supabase_client import set_session_from_url, get_profile, exchange_code_for_session
    from web.auth_state import GlobalAuthState
    import json

    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    with ui.column().classes('w-full h-screen items-center justify-center'):
        spinner = ui.spinner(size='xl')
        status_label = ui.label('Completing login...').classes('text-lg mt-4')
        error_label = ui.label('').classes('text-red-500 mt-4 hidden')
        home_btn = ui.button('Return to Home', on_click=lambda: ui.navigate.to('/')).classes('mt-2 hidden')

    async def complete_login(user, profile, session=None):
        """Store user in session and redirect."""
        app.storage.user[GlobalAuthState.USER_KEY] = user
        if profile:
            app.storage.user[GlobalAuthState.PROFILE_KEY] = profile
        # Store session tokens for per-user Supabase client
        if session:
            app.storage.user['auth_session'] = {
                'access_token': session.get('access_token'),
                'refresh_token': session.get('refresh_token'),
            }
        status_label.text = 'Login successful! Redirecting...'
        await asyncio.sleep(0.5)
        ui.navigate.to('/')

    def show_error(message):
        """Display error and show home button."""
        spinner.set_visibility(False)
        status_label.set_visibility(False)
        error_label.text = message
        error_label.classes(remove='hidden')
        home_btn.classes(remove='hidden')

    try:
        # Method 1: PKCE flow - code in query parameter (fallback if implicit not available)
        if code:
            print(f"OAuth callback: exchanging code {code[:20]}...")
            result = exchange_code_for_session(code)
            print(f"Code exchange result: {result}")

            if 'error' in result:
                show_error(result['error'])
                return

            user = result.get('user')
            if user:
                profile = get_profile(user['id'])
                await complete_login(user, profile, session=result.get('session'))
            else:
                show_error('Login failed - no user returned')
            return

        # Method 2: Implicit flow - tokens in URL hash
        await asyncio.sleep(0.5)
        tokens_json = await ui.run_javascript('''
            (function() {
                const hash = window.location.hash.substring(1);
                console.log("Hash:", hash);
                if (hash) {
                    const params = new URLSearchParams(hash);
                    return JSON.stringify({
                        access_token: params.get('access_token'),
                        refresh_token: params.get('refresh_token'),
                        error: params.get('error_description') || params.get('error')
                    });
                }
                // Also check query params for error
                const urlParams = new URLSearchParams(window.location.search);
                const error = urlParams.get('error_description') || urlParams.get('error');
                if (error) {
                    return JSON.stringify({error: error});
                }
                return JSON.stringify({no_tokens: true});
            })();
        ''')

        print(f"OAuth callback received: {tokens_json}")
        tokens = json.loads(tokens_json) if tokens_json else {}

        if tokens.get('error'):
            show_error(tokens['error'])
            return

        if tokens.get('no_tokens'):
            # No tokens found - redirect to home
            ui.navigate.to('/')
            return

        access_token = tokens.get('access_token')
        refresh_token = tokens.get('refresh_token')

        if not access_token or not refresh_token:
            ui.navigate.to('/')
            return

        result = set_session_from_url(access_token, refresh_token)
        print(f"set_session_from_url result: {result}")

        if 'error' in result:
            show_error(result['error'])
            return

        user = result.get('user')
        if user:
            profile = get_profile(user['id'])
            await complete_login(user, profile, session=result.get('session'))
        else:
            show_error('Login failed - no user returned')

    except Exception as e:
        print(f"OAuth callback error: {e}")
        import traceback
        traceback.print_exc()
        spinner.set_visibility(False)
        status_label.set_visibility(False)
        error_label.text = f'Error: {str(e)}'
        error_label.classes(remove='hidden')
        home_btn.classes(remove='hidden')


# ============================================================================
# Startup Logic
# ============================================================================

async def initialize_engine():
    """Heavy initialization running in a separate thread via run.io_bound."""
    print("Starting background initialization...")

    def _init_sync():
        try:
            # 1. Metadata
            state.meta_mgr = MetadataManager()
            state.lists_mgr = ListsManager(state.meta_mgr)

            # Initialize user lists manager (auth-aware wrapper)
            state.init_user_lists_mgr()

            # 2. Lab Settings & Engine
            state.lab_engine = LabEngine(state.meta_mgr, None)

            # 3. Variants (depends on Lab Settings)
            state.var_mgr = VariantManager(settings=state.lab_engine.settings)

            # 4. Search Engine & Indexer
            state.searcher = SearchEngine(state.meta_mgr, state.var_mgr)
            state.indexer = Indexer(state.meta_mgr)

            # 5. Start background loading
            state.meta_mgr.start_background_loading()

            print("Engine initialization complete.")
            return True
        except Exception as e:
            print(f"Engine init failed: {e}")
            import traceback
            traceback.print_exc()
            return False

    await run.io_bound(_init_sync)

app.on_startup(initialize_engine)

if __name__ in {'__main__', '__mp_main__'}:
    print(f"\n{'='*60}")
    print(f"  Dicta Genizah Search v{APP_VERSION}")
    print(f"  Starting on port {APP_PORT}...")
    print(f"{'='*60}\n")

    # Production settings via environment variables
    reload_enabled = os.environ.get('NICEGUI_RELOAD', 'true').lower() == 'true'
    show_browser = os.environ.get('NICEGUI_SHOW', 'true').lower() == 'true'

    favicon_path = os.path.join(os.path.dirname(__file__), 'static', 'favicon.ico')

    # Reconnect timeout (seconds) - how long client waits before giving up reconnection
    # Higher value = more patient reconnection attempts under load
    reconnect_timeout = int(os.environ.get('NICEGUI_RECONNECT_TIMEOUT', '30'))

    ui.run(
        title=APP_TITLE,
        port=APP_PORT,
        reload=reload_enabled,
        show=show_browser,
        storage_secret='genizah-secret-v5',
        favicon=favicon_path,
        reconnect_timeout=reconnect_timeout,
    )
