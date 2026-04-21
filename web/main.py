#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GenizahSearch Web Application - Professional Research Interface

A modern, high-quality web interface for Cairo Genizah manuscript research.
Designed with academic researchers in mind, providing powerful search tools
with an intuitive, accessible interface.

Run with: python -m web.main (from project root)
"""

import asyncio
import logging
import os
import sys

# Load environment variables first (for Supabase configuration)
from dotenv import load_dotenv
load_dotenv()

# Ensure we can import from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nicegui import ui, app, run
from web.framework_patches import apply_all_patches
apply_all_patches()


# ---------------------------------------------------------------------------
# Middleware: inject font-display: swap into NiceGUI's bundled fonts.css
# ---------------------------------------------------------------------------
from starlette.responses import Response as _StarletteResponse

@app.middleware('http')
async def _inject_font_display_swap(request, call_next):
    """Add ``font-display: swap`` to NiceGUI's fonts.css for faster text paint.

    Without this, text is invisible during font load (~1200 ms on slow connections).
    The middleware only processes the single fonts.css request; all other requests
    pass through with zero overhead beyond a string comparison on the path.
    """
    response = await call_next(request)
    if '/static/fonts.css' not in str(request.url.path):
        return response
    # Only rewrite 200 responses with a body. For 304/206/4xx/5xx we must pass
    # the original response through untouched: a 304 has no body to mutate, and
    # returning 200 with our CSS would break HTTP cache semantics. (Reported by
    # Codex code review, 2026-04-13.)
    if response.status_code != 200:
        return response
    body = b''
    async for chunk in response.body_iterator:
        body += chunk if isinstance(chunk, bytes) else chunk.encode()
    text = body.decode('utf-8')
    # NiceGUI 3.6+ ships fonts.css with `font-display: block` on a handful
    # of @font-face blocks, and **no** font-display at all on the rest
    # (~40 icon fonts). Block keeps text invisible up to 3 s; missing
    # declarations also default to block in most browsers. Fix both:
    # 1) Replace every explicit `block` with `swap`.
    # 2) Inject `font-display: swap;` into any @font-face block that
    #    still lacks one after step 1. Uses a regex with a per-block
    #    check so we don't double-insert.
    text = text.replace('font-display: block', 'font-display: swap')
    def _ensure_swap(match: '_re.Match[str]') -> str:
        block = match.group(0)
        return block if 'font-display' in block else (
            block.replace('@font-face {', '@font-face {\n  font-display: swap;', 1)
        )
    text = _re.sub(r'@font-face\s*\{[^}]*\}', _ensure_swap, text)
    # Drop cache validators that describe the pre-mutation payload — keeping
    # them would let clients revalidate and then apply our rewritten CSS under
    # the upstream ETag/Last-Modified, poisoning conditional GETs. Also drop
    # content-length (stale after rewrite) and content-encoding (body is now
    # plaintext).
    stale = {'content-length', 'etag', 'last-modified', 'content-encoding'}
    headers = {
        k: v for k, v in response.headers.items()
        if k.lower() not in stale
    }
    return _StarletteResponse(
        content=text,
        status_code=200,
        media_type='text/css',
        headers=headers,
    )


@app.middleware('http')
async def _mark_framework_assets_noindex(request, call_next):
    """Mark internal NiceGUI framework assets as non-indexable.

    These ``/_nicegui/...`` URLs are implementation-detail JavaScript assets,
    not user-facing documents. Search Console can still surface them when
    crawlers request internal ESM paths, so return an explicit X-Robots-Tag
    while keeping the assets crawlable for rendering.
    """
    response = await call_next(request)
    if str(request.url.path).startswith('/_nicegui/'):
        response.headers.setdefault('X-Robots-Tag', 'noindex')
    return response


# NOTE: Tried to defer quasar.unimportant.prod.css via a media-print preload
# rewrite, but NiceGUI 3.6 emits it as an `@import` inside an inline <style>
# with CSS layers (layer(quasar)) rather than a <link rel="stylesheet"> tag,
# so a simple link rewrite was a no-op and moving it out of the layer would
# disturb cascade order. Leaving the font-display middleware as the sole
# CSS-level perf fix for now.
import re as _re


# ---------------------------------------------------------------------------
# Debug endpoint: /_internal/memstat — diagnostic RssAnon/VmRSS probe
# Added 2026-04-19 for leak-investigation baseline tracking post-v7.9.0 deploy.
# Gated by MEMSTAT_SECRET header; returns 503 if the env var is unset.
# ---------------------------------------------------------------------------
from fastapi import Header, HTTPException
from fastapi.responses import JSONResponse
import time as _memstat_time

_MEMSTAT_SECRET = os.environ.get('MEMSTAT_SECRET', '').strip()


@app.get('/_internal/memstat')
def _memstat_endpoint(x_memstat_secret: str = Header(default='')):
    if not _MEMSTAT_SECRET:
        raise HTTPException(status_code=503, detail='memstat disabled (MEMSTAT_SECRET unset)')
    if x_memstat_secret != _MEMSTAT_SECRET:
        raise HTTPException(status_code=401, detail='unauthorized')
    stats: dict = {}
    try:
        with open('/proc/self/status', 'r') as f:
            for line in f:
                for key in ('VmRSS', 'VmData', 'VmSize', 'RssAnon', 'RssFile'):
                    if line.startswith(key + ':'):
                        stats[key + '_kb'] = int(line.split(':', 1)[1].strip().split()[0])
                        break
    except FileNotFoundError:
        raise HTTPException(status_code=501, detail='/proc not available (non-Linux)')
    stats['pid'] = os.getpid()
    stats['timestamp_utc'] = _memstat_time.strftime('%Y-%m-%dT%H:%M:%SZ', _memstat_time.gmtime())
    stats['version'] = None  # filled below
    try:
        from version import APP_VERSION as _v
        stats['version'] = _v
    except Exception:
        pass
    return JSONResponse(stats)


logger = logging.getLogger(__name__)
from web.state import state
from web.api import init_api_routes
from web.translations import tr, set_language, get_language
from web.feature_flags import WEB_PUZZLE_ENABLED
from genizah_core import MetadataManager, VariantManager, SearchEngine, LabEngine, Indexer, ListsManager

# App configuration
APP_TITLE = "Dicta Genizah Search | חיפוש גניזת קהיר"
from version import APP_VERSION
WHATS_NEW_VERSION = APP_VERSION  # Bump when adding new "What's New" content
APP_PORT = int(os.environ.get('GENIZAH_PORT', 8081))

# Initialize API routes (Image Proxy, Export)
init_api_routes()

# Serve static files for SEO images
STATIC_DIR = os.path.join(os.path.dirname(__file__), 'static')
app.add_static_files('/static', STATIC_DIR)

# ============================================================================
# Website Metadata - SEO & Social Sharing
# ============================================================================

_SITE_URL = 'https://genizahsearch.com'
_DEFAULT_TITLE = 'Dicta Genizah Search | חיפוש בגניזה הקהירית — אתר הגניזה של דיקטה'
_DEFAULT_DESCRIPTION = 'חיפוש מלא בכתבי יד מהגניזה הקהירית — טקסטים, תמונות, קטלוג ומטא-דאטה מ-255,000 קטעי גניזת קהיר. אתר הגניזה של דיקטה.'
_DEFAULT_KEYWORDS = 'חיפוש בגניזה הקהירית, חיפוש גניזה, כתבי יד גניזת קהיר, גניזה קהירית, מחקר גניזה, Cairo Genizah search, Genizah manuscripts, Jewish manuscripts, Dicta Genizah Search'


def page_meta(
    path: str = '/',
    title: str = _DEFAULT_TITLE,
    description: str = _DEFAULT_DESCRIPTION,
    og_type: str = 'website',
    noindex: bool = False,
    needs_iiif: bool = False,
) -> str:
    """Build per-page meta tags with correct canonical, OG, and Twitter metadata."""
    import html as _html
    _t = _html.escape(title)
    _d = _html.escape(description)
    url = f'{_SITE_URL}{path}'
    robots = '<meta name="robots" content="noindex, follow">\n' if noindex else ''
    return f'''
<!-- Meta Tags -->
{robots}<meta name="description" content="{_d}">
<meta name="keywords" content="{_DEFAULT_KEYWORDS}">
<meta name="author" content="Dicta Genizah Search">
<meta name="theme-color" content="#059669">
<!-- Preconnect hints -->
{'<link rel="preconnect" href="https://iiif.nli.org.il">' if needs_iiif else ''}
<link rel="dns-prefetch" href="https://cudl.lib.cam.ac.uk">
<link rel="dns-prefetch" href="https://eu.i.posthog.com">
<link rel="dns-prefetch" href="https://www.googletagmanager.com">
<!-- Open Graph -->
<meta property="og:type" content="{og_type}">
<meta property="og:url" content="{url}">
<meta property="og:title" content="{_t}">
<meta property="og:description" content="{_d}">
<meta property="og:image" content="{_SITE_URL}/static/og-image.png">
<meta property="og:locale" content="he_IL">
<meta property="og:site_name" content="Dicta Genizah Search">
<!-- Twitter Card -->
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:url" content="{url}">
<meta name="twitter:title" content="{_t}">
<meta name="twitter:description" content="{_d}">
<meta name="twitter:image" content="{_SITE_URL}/static/og-image.png">
<!-- Canonical URL -->
<link rel="canonical" href="{url}">
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
    // Defer PostHog init past first paint (events are queued by the stub loader above)
    function _phInit() {{
        posthog.init('{_posthog_key}', {{
            api_host: 'https://eu.i.posthog.com',
            person_profiles: 'identified_only',
            autocapture: true,
            capture_pageview: true,
            capture_pageleave: true,
            session_recording: {{
                maskAllInputs: true,
                maskTextSelector: 'input, textarea'
            }},
            // Filter out localhost / dev traffic
            opt_out_capturing_by_default: ['localhost', '127.0.0.1'].includes(location.hostname),
        }});
    }}
    if (window.requestIdleCallback) {{ requestIdleCallback(_phInit); }}
    else {{ setTimeout(_phInit, 2000); }}
</script>
''' if _posthog_key else ''


# posthog_capture moved to web/analytics.py to avoid circular imports
from web.analytics import posthog_capture  # noqa: F401 — re-export


# ============================================================================
# Modern Theme System - Professional Research UI
# ============================================================================

COMMON_STYLES = '<link rel="stylesheet" href="/static/common.css">'


def _resolve_ui_language() -> str:
    """Return the persisted UI language so layout and bootstrap agree on first render."""
    try:
        saved_lang = app.storage.user.get('ui_language')
    except Exception:
        saved_lang = None  # Language pref load failed; will use default

    if saved_lang in ('he', 'en'):
        return saved_lang

    current_lang = get_language()
    return current_lang if current_lang in ('he', 'en') else 'he'


# ============================================================================
# Layout Components
# ============================================================================

def create_layout():
    """Create the main application layout with modern Header and Sidebar."""

    resolved_lang = _resolve_ui_language()
    set_language(resolved_lang)

    current_page = _safe_user_storage_get('current_page', '/')
    rtl_mode = resolved_lang == 'he'

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
            quick_search.on('keydown.enter', lambda: ui.navigate.to(f'/search?q={quick_search.value or ""}'))
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
                    """Heartbeat: check server readiness and update dot color."""
                    try:
                        if status_dot.is_deleted or status_text.is_deleted:
                            return
                        if state.is_ready():
                            status_dot.classes('bg-green-400', remove='bg-yellow-400 animate-pulse')
                            status_text.text = tr('Ready')
                            connection_state['was_connected'] = True
                        elif not connection_state['was_connected']:
                            status_dot.classes('bg-yellow-400 animate-pulse', remove='bg-green-400')
                            status_text.text = tr('Loading...')
                    except RuntimeError:
                        pass  # Element/client deleted

                # Run heartbeat every 10 seconds to monitor connection health
                # Use asyncio to avoid parent_slot RuntimeError on navigation
                async def _connection_heartbeat_loop():
                    await asyncio.sleep(2.0)
                    while True:
                        try:
                            await update_status()
                        except RuntimeError:
                            break  # Element deleted — stop loop
                        except Exception:
                            pass  # Transient failure — keep retrying
                        await asyncio.sleep(10.0)
                connection_state['timer'] = asyncio.ensure_future(_connection_heartbeat_loop())

            # Auth Buttons (Login/Register or User Menu)
            with ui.row().classes('auth-buttons'):
                from web.auth_state import create_auth_buttons
                create_auth_buttons()

            # Language Toggle
            def toggle_lang():
                current = get_language()
                new_lang = 'en' if current == 'he' else 'he'
                try:
                    app.storage.user['ui_language'] = new_lang
                except Exception:
                    pass  # Browser storage operation failed; preference not persisted
                set_language(new_lang)
                ui.navigate.reload()

            lang_label = "EN" if get_language() == 'he' else "\u05E2\u05D1"
            ui.button(lang_label, on_click=toggle_lang).props('flat round text-color=white').tooltip(tr('Switch language')).classes('lang-btn-header')

            # Help Button (hidden on mobile via CSS)
            ui.button(icon='help_outline', on_click=lambda: ui.navigate.to('/help')).props(f'flat round text-color=white aria-label="{tr("Help")}"').tooltip(tr('Help')).classes('help-btn-header')
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
    drawer_open = _safe_user_storage_get('drawer_open', True)

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

    # Run after page is fully loaded (asyncio to avoid parent_slot error)
    async def _deferred_close_drawer():
        await asyncio.sleep(0.5)
        try:
            await close_drawer_on_mobile()
        except Exception:
            pass  # Drawer close failed; non-fatal UI glitch
    asyncio.ensure_future(_deferred_close_drawer())

    # Content Area
    content_col = ui.column().classes('main-content w-full items-stretch flex-grow')
    # Add ID for skip link target
    content_col.props('id=main-content')

    # === "What's New" Banner (dismissible, compact single-line) ===
    if _safe_user_storage_get('whats_new_dismissed') != WHATS_NEW_VERSION:
        banner_dir = 'rtl' if rtl_mode else 'ltr'
        with content_col:
            with ui.element('div').classes('w-full mx-auto max-w-5xl px-4 py-2 flex items-center gap-3 mt-2').style(
                f'background: var(--bg-tertiary); border-bottom: 1px solid var(--border-light); border-radius: 6px; direction: {banner_dir};'
            ) as whats_new_banner:
                ui.icon('new_releases').classes('text-base').style('color: #10b981;')
                ui.label(tr("New Features!")).classes('text-xs font-bold').style('color: var(--text-primary);')
                ui.label(tr('New: Browse different volumes in multi-scan manuscripts — fixes image-to-text mismatch, and Visual Similarity suggestions for discovering related manuscripts.')).classes('text-xs flex-1 truncate').style('color: var(--text-secondary);')
                def dismiss_whats_new():
                    app.storage.user['whats_new_dismissed'] = WHATS_NEW_VERSION
                    whats_new_banner.delete()
                ui.button(icon='close', on_click=dismiss_whats_new).props(f'flat dense round size=xs aria-label="{tr("Dismiss")}"')
                ui.timer(10.0, dismiss_whats_new, once=True)

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
                    ('/browse', 'menu_book', tr('Browse by Shelfmark'), None),
                    ('/catalog-browse', 'category', tr('Browse by Identification'), None),
                    ('/discoveries', 'lightbulb', tr('Community'), None),
                    ('/lists', 'star', tr('My Lists'), None),
                ]
                if WEB_PUZZLE_ENABLED:
                    nav_items.append(('/puzzle', 'extension', tr('Fragment Puzzle'), None))

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
                # Translation Toggle (show/hide machine translations)
                show_translations = False
                try:
                    show_translations = app.storage.user.get('show_translations', False)
                except Exception:
                    pass  # Translation lookup failed; continue without translation

                def toggle_translations():
                    try:
                        current = app.storage.user.get('show_translations', False)
                        app.storage.user['show_translations'] = not current
                    except Exception:
                        pass  # Translation lookup failed; continue without translation
                    ui.navigate.reload()

                trans_icon = 'g_translate' if show_translations else 'translate'
                trans_label = tr('Translations ON') if show_translations else tr('Translations OFF')
                trans_opacity = 'opacity-100' if show_translations else 'opacity-60'
                with ui.row().classes(f'w-full items-center justify-center gap-2 cursor-pointer {trans_opacity} hover:opacity-100').props(
                    'role=button tabindex=0'
                ).on('click', toggle_translations).on('keydown.enter', toggle_translations).on('keydown.space', toggle_translations):
                    ui.icon(trans_icon).classes('text-lg').style(
                        'color: var(--primary-600);' if show_translations else ''
                    )
                    ui.label(trans_label).classes('text-xs font-medium')
                    if show_translations:
                        ui.icon('check_circle').classes('text-xs').style('color: var(--primary-600);')
                if show_translations:
                    ui.label(
                        tr('Translations are machine-generated scholarly aids and may contain errors. Always verify against the original text.')
                    ).classes('text-xs px-2').style(
                        'color: var(--text-tertiary); font-style: italic; line-height: 1.3; opacity: 0.8;'
                    )

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

                    current_theme = _safe_user_storage_get('theme', 'light')

                    with ui.button(icon='light_mode', on_click=lambda: set_theme('light')).props(f'flat round size=sm aria-label="{tr("Light theme")}"').classes(f'theme-btn theme-btn-light {"active" if current_theme == "light" else ""}'): pass
                    with ui.button(icon='history_edu', on_click=lambda: set_theme('parchment')).props(f'flat round size=sm aria-label="{tr("Parchment theme")}"').classes(f'theme-btn theme-btn-parchment {"active" if current_theme == "parchment" else ""}'): pass
                    with ui.button(icon='dark_mode', on_click=lambda: set_theme('dark')).props(f'flat round size=sm aria-label="{tr("Dark theme")}"').classes(f'theme-btn theme-btn-dark {"active" if current_theme == "dark" else ""}'): pass

                # Version Info (hidden - using "formerly" in settings instead)
                # ui.label(f'v{APP_VERSION}').classes('text-xs text-center opacity-50 mt-2')

                # Accessibility Link
                with ui.row().classes('w-full justify-center mt-2'):
                    ui.link(tr('Accessibility Statement'), '/accessibility').classes('text-xs opacity-70 hover:opacity-100').style('text-decoration: none; color: inherit;')

                # Creator Credit
                ui.label(tr('Created by Hillel Gershuni')).classes('text-xs text-center opacity-50 mt-1')

    # Global Footer with Citation Note (dismissible, auto-collapse to compact line)
    full_citation = 'Stoekl Ben Ezra, D., Bambaci, L., Kiessling, B., Lapin, H., Ezer, N., Lolli, E., Rustow, M., Dershowitz, N., Kurar Barakat, B., Gogawale, S., Shmidman, A., Lavee, M., Siew, T., Raziel Kretzmer, V., Vasyutinsky Shapira, D., Olszowy-Schlanger, J., & Gila, Y. (2025). MiDRASH Automatic Transcriptions. Zenodo. https://doi.org/10.5281/zenodo.17734473'
    footer = ui.footer().classes('citation-footer')
    with footer:
        # Full citation (shown initially)
        with ui.row().classes('w-full items-center justify-center gap-2 py-2 px-4 flex-wrap citation-full'):
            ui.button(icon='content_copy', on_click=lambda: ui.run_javascript(f'navigator.clipboard.writeText("{full_citation}"); alert("{tr("Citation copied!")}")')).props(f'flat dense size=xs aria-label="{tr("Copy citation")}"').classes('opacity-70 hover:opacity-100').tooltip(tr('Copy citation'))
            ui.label(tr('When publishing material from this site, please cite:')).classes('text-xs opacity-80 citation-hebrew-label')
            ui.link(full_citation, 'https://doi.org/10.5281/zenodo.17734473', new_tab=True).classes('text-xs font-medium citation-link').style('direction: ltr; text-decoration: none;')
            ui.button(icon='close', on_click=lambda: ui.run_javascript('localStorage.setItem("citation_footer_dismissed", "true"); document.querySelector(".citation-footer").style.display = "none";')).props(f'flat dense size=xs aria-label="{tr("Dismiss")}"').classes('opacity-50 hover:opacity-100').tooltip(tr('Dismiss'))

        # Compact citation (shown after auto-collapse — full text, truncated with ellipsis on narrow screens)
        with ui.row().classes('w-full items-center justify-center gap-2 py-1 px-4 citation-compact').style('flex-wrap: nowrap;'):
            ui.label(full_citation).classes('text-xs opacity-80').style(
                'direction: ltr; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; min-width: 0; flex: 1;'
            )
            ui.button(icon='content_copy', on_click=lambda: ui.run_javascript(f'navigator.clipboard.writeText("{full_citation}"); alert("{tr("Citation copied!")}")')).props(f'flat dense size=xs aria-label="{tr("Copy citation")}"').classes('opacity-70 hover:opacity-100').tooltip(tr('Copy full citation'))
            ui.button(icon='close', on_click=lambda: ui.run_javascript('localStorage.setItem("citation_footer_dismissed", "true"); document.querySelector(".citation-footer").style.display = "none";')).props(f'flat dense size=xs aria-label="{tr("Dismiss")}"').classes('opacity-50 hover:opacity-100').tooltip(tr('Dismiss'))

    # Dismiss check + auto-collapse after 10 seconds
    ui.run_javascript('''
        (function() {
            if (localStorage.getItem("citation_footer_dismissed") === "true") {
                document.querySelector(".citation-footer").style.display = "none";
                return;
            }
            setTimeout(function() {
                var full = document.querySelector(".citation-full");
                var compact = document.querySelector(".citation-compact");
                if (full && compact) {
                    full.style.opacity = "0";
                    setTimeout(function() {
                        full.style.display = "none";
                        compact.style.display = "flex";
                        compact.style.opacity = "0";
                        setTimeout(function() { compact.style.opacity = "1"; }, 50);
                    }, 300);
                }
            }, 10000);
        })();
    ''')

    # One-time citation reminder dialog (per machine, via localStorage)
    _show_citation_reminder(get_language())

    return content_col


def _show_citation_reminder(lang: str):
    """Show a one-time citation reminder dialog if not previously dismissed."""
    dialog = ui.dialog().props('persistent')
    with dialog, ui.card().classes('max-w-lg'):
        if lang == 'he':
            ui.label('בקשה חשובה: ציטוט מדרש').classes('text-lg font-bold').style('direction: rtl;')
            with ui.column().classes('gap-2').style('direction: rtl; text-align: right;'):
                ui.label(
                    'אתר זה מבוסס על תמלולים אוטומטיים שנוצרו על ידי צוות פרויקט מדרש. '
                    'על פי חוק זכויות יוצרים, יש לצטט את המקור בעת פרסום חומר מאתר זה.'
                ).classes('text-sm')
                ui.label(
                    'מעבר לדרישה החוקית \u2014 ככל שיהיו יותר ציטוטים, כך יוכל צוות מדרש '
                    'להשתמש בהם כדי לקבל מענקים נוספים, לשפר את התמלולים ולהרחיב את העבודה '
                    'לכתבי יד עבריים נוספים. הציטוט המלא מופיע בתחתית המסך.'
                ).classes('text-sm')
                ui.label('תודה על שיתוף הפעולה!').classes('text-sm font-medium')
        else:
            ui.label('Important: Please cite MiDRASH').classes('text-lg font-bold')
            with ui.column().classes('gap-2'):
                ui.label(
                    'This website is built on automatic transcriptions produced by the MiDRASH Project. '
                    'Copyright law requires citing the source when publishing material from this site.'
                ).classes('text-sm')
                ui.label(
                    'Beyond the legal requirement \u2014 the more citations the project receives, the more '
                    'the MiDRASH team can use them to secure grants and funding to improve the Genizah '
                    'transcriptions and expand their work to other Hebrew manuscripts. '
                    'The full citation appears at the bottom of the screen.'
                ).classes('text-sm')
                ui.label('Thank you for your cooperation!').classes('text-sm font-medium')
        with ui.row().classes('w-full justify-end mt-2'):
            ui.button(
                tr('Got it'),
                on_click=lambda: (
                    ui.run_javascript('localStorage.setItem("citation_reminder_seen", "true")'),
                    dialog.close(),
                ),
            ).props('color=primary')
    # Only open if not previously seen — pure JS check avoids slot context issues
    _trigger = ui.button('', on_click=dialog.open).props('flat dense').style('display:none;')
    _trigger_id = f'citation-trigger-{_trigger.id}'
    _trigger.props(f'id="{_trigger_id}"')
    ui.run_javascript(f'''
        if (localStorage.getItem("citation_reminder_seen") !== "true") {{
            document.getElementById("{_trigger_id}")?.click();
        }}
    ''')


# ============================================================================
# Page Routes
# ============================================================================

def apply_theme_immediately():
    """Add script to apply theme before page renders to prevent flash."""
    try:
        current_theme = app.storage.user.get('theme', 'light')
    except Exception:
        current_theme = 'light'  # Theme read failed; default to light
    current_lang = _resolve_ui_language()
    bg_color = "#0f172a" if current_theme == "dark" else "#fffbf5" if current_theme == "parchment" else "#f8fafc"

    # Use proper direction based on language (RTL for Hebrew, LTR for English)
    dir_attr = 'rtl' if current_lang == 'he' else 'ltr'

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

            // Activate Quasar layout direction as soon as Quasar is available.
            // On first cold load Quasar may appear after DOMContentLoaded, so
            // a single attempt can miss and leave the drawer on the wrong side.
            var activateQuasarLayout = function() {{
                if (typeof Quasar === 'undefined' || !Quasar.lang || !Quasar.lang.set) {{
                    return false;
                }}

                if (isRtl) {{
                    // Try to use Hebrew language pack first, fallback to generic RTL
                    if (Quasar.lang.he) {{
                        Quasar.lang.set(Quasar.lang.he);
                    }} else {{
                        Quasar.lang.set({{ isoName: 'he', rtl: true }});
                    }}
                }} else {{
                    // Use full lang pack so isoName is set (otherwise html lang becomes "undefined")
                    if (Quasar.lang['en-US']) {{
                        Quasar.lang.set(Quasar.lang['en-US']);
                    }} else {{
                        Quasar.lang.set({{ isoName: 'en', rtl: false }});
                    }}
                }}
                // Belt-and-braces: ensure html lang is never the literal string "undefined"
                if (document.documentElement.lang === 'undefined' || !document.documentElement.lang) {{
                    document.documentElement.lang = lang || 'en';
                }}

                document.documentElement.setAttribute("data-quasar-rtl-ready", "true");
                if (document.body) {{
                    document.body.setAttribute("data-quasar-rtl-ready", "true");
                }}
                return true;
            }};

            var scheduleQuasarActivation = function() {{
                if (activateQuasarLayout()) {{
                    return;
                }}

                var attempts = 0;
                var maxAttempts = 40;
                var retryDelayMs = 50;

                var retry = function() {{
                    attempts += 1;
                    if (!activateQuasarLayout() && attempts < maxAttempts) {{
                        window.setTimeout(retry, retryDelayMs);
                    }}
                }};

                window.setTimeout(retry, retryDelayMs);
            }};

            // Execute immediately
            applyTheme();

            // Backup for body element when it exists
            if (document.readyState === 'loading') {{
                document.addEventListener('DOMContentLoaded', function() {{
                    applyTheme();
                    scheduleQuasarActivation();
                }});
            }} else {{
                scheduleQuasarActivation();
            }}

            window.addEventListener('load', function() {{
                applyTheme();
                scheduleQuasarActivation();
            }}, {{ once: true }});
        }})();
    </script>'''

def _safe_user_storage_get(key: str, default=None):
    """Safely read from app.storage.user, returning default if session not ready."""
    try:
        return app.storage.user.get(key, default)
    except (AssertionError, KeyError, Exception):
        return default


def set_current_page(page_path: str):
    """Safely set the current page in user storage."""
    try:
        app.storage.user['current_page'] = page_path
    except (AssertionError, KeyError, Exception):
        pass  # Storage not ready yet, ignore

@ui.page('/', title='Dicta Genizah Search | חיפוש בגניזה הקהירית — אתר הגניזה של דיקטה')
def dashboard_page():
    set_current_page('/')
    try:
        current_theme = app.storage.user.get('theme', 'light')
    except (AssertionError, KeyError, Exception):
        current_theme = 'light'
    ui.add_head_html(page_meta('/'))
    # Structured data: WebSite schema with SearchAction for homepage
    ui.add_head_html('''
    <script type="application/ld+json">
    {
        "@context": "https://schema.org",
        "@type": "WebSite",
        "name": "Dicta Genizah Search",
        "alternateName": "חיפוש בגניזה הקהירית",
        "url": "https://genizahsearch.com",
        "description": "חיפוש מלא בכתבי יד מהגניזה הקהירית — טקסטים, תמונות, קטלוג ומטא-דאטה מ-255,000 קטעי גניזת קהיר.",
        "inLanguage": ["he", "en"],
        "potentialAction": {
            "@type": "SearchAction",
            "target": {
                "@type": "EntryPoint",
                "urlTemplate": "https://genizahsearch.com/search?q={search_term_string}"
            },
            "query-input": "required name=search_term_string"
        },
        "publisher": {
            "@type": "Organization",
            "name": "Dicta — The Israel Center for Text Analysis",
            "url": "https://dicta.org.il"
        }
    }
    </script>
    ''')
    # Structured data: Organization schema
    ui.add_head_html('''
    <script type="application/ld+json">
    {
        "@context": "https://schema.org",
        "@type": "Organization",
        "name": "Dicta Genizah Search",
        "alternateName": "חיפוש בגניזה הקהירית — דיקטה",
        "url": "https://genizahsearch.com",
        "logo": "https://genizahsearch.com/static/og-image.png",
        "parentOrganization": {
            "@type": "Organization",
            "name": "Dicta — The Israel Center for Text Analysis",
            "url": "https://dicta.org.il"
        }
    }
    </script>
    ''')
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages import home
        if hasattr(home, 'create_page'):
            home.create_page()

@ui.page('/search', title='Full-Text Search | חיפוש טקסט מלא — Dicta Genizah Search')
def search_page_route(
    q: str = None, tag: str = None,
    mode: str = None, variants: int = None,
    ja: int = None, flex_spaces: int = None,
    bidirectional: int = None, domain: str = None,
    from_browse: int = None,
    vs_src: str = None, vs_mode: str = None, vs_browse: int = None,
):
    set_current_page('/search')
    ui.add_head_html(page_meta(
        '/search',
        title='Full-Text Search | חיפוש טקסט מלא — Dicta Genizah Search',
        description='Search across 500,000+ Cairo Genizah manuscript fragments with variant-aware full-text search, Responsa syntax, and domain filters.',
        noindex=True,  # search result pages should not be indexed
        needs_iiif=True,
    ))
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
            initial_bidirectional=bidirectional, initial_domain=domain,
            from_browse=from_browse,
            vs_src=vs_src, vs_mode=vs_mode, vs_browse=vs_browse,
        )

@ui.page('/parallels', title='Textual Parallels | Dicta Genizah Search')
def parallels_page_route(text: str = None):
    set_current_page('/parallels')
    ui.add_head_html(page_meta(
        '/parallels',
        title='Textual Parallels | Dicta Genizah Search',
        description='Find textual parallels and intertextual connections across Cairo Genizah manuscripts. Paste text to discover matching fragments.',
        noindex=True,
    ))
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.parallels import create_parallels_page
        create_parallels_page(initial_text=text)

@ui.page('/browse', title='Manuscript Browser | עיון בכתבי יד — Dicta Genizah Search')
def browse_page_route(sys_id: str = None, highlight: str = None, fl_id: str = None, page: int = None, shelfmark: str = None, volume_ie: str = None):
    set_current_page('/browse')
    # Dynamic metadata for manuscript pages when sys_id is provided
    if sys_id:
        # Resolve the real shelfmark from csv_bank when the URL only has sys_id
        _shelfmark_display = shelfmark
        if not _shelfmark_display:
            try:
                _row = state.meta_mgr.csv_bank.get(sys_id) if state.meta_mgr else None
                _shelfmark_display = _row.get('shelfmark', sys_id) if _row else sys_id
            except Exception:
                _shelfmark_display = sys_id  # Shelfmark lookup failed; use raw identifier
        _browse_title = f'{_shelfmark_display} | Dicta Genizah Search'
        ui.page_title(_browse_title)
        ui.add_head_html(page_meta(
            f'/browse?sys_id={sys_id}',
            title=f'{_shelfmark_display} | Dicta Genizah Search',
            description=f'View Cairo Genizah manuscript {_shelfmark_display} — images, transcription, catalog, bibliography, and scholarly metadata. צפייה בכתב יד {_shelfmark_display} מגניזת קהיר.',
            needs_iiif=True,
        ))
        # Structured data: BreadcrumbList for manuscript pages
        _safe_shelfmark = _shelfmark_display.replace('"', '&quot;').replace('<', '&lt;')
        ui.add_head_html(f'''
        <script type="application/ld+json">
        {{
            "@context": "https://schema.org",
            "@type": "BreadcrumbList",
            "itemListElement": [
                {{
                    "@type": "ListItem",
                    "position": 1,
                    "name": "Home",
                    "item": "https://genizahsearch.com/"
                }},
                {{
                    "@type": "ListItem",
                    "position": 2,
                    "name": "Browse",
                    "item": "https://genizahsearch.com/browse"
                }},
                {{
                    "@type": "ListItem",
                    "position": 3,
                    "name": "{_safe_shelfmark}",
                    "item": "https://genizahsearch.com/browse?sys_id={sys_id}"
                }}
            ]
        }}
        </script>
        ''')
    else:
        ui.add_head_html(page_meta(
            '/browse',
            title='Manuscript Browser | עיון בכתבי יד — Dicta Genizah Search',
            description='Browse Cairo Genizah manuscripts — high-resolution images, transcriptions, scholarly metadata, and catalog data from FJMS and PGP. עיון בכתבי יד מגניזת קהיר.',
            needs_iiif=True,
        ))
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    # If shelfmark param looks like a sys_id (starts with 99, all digits), use it directly
    if not sys_id and shelfmark and shelfmark.strip().isdigit() and shelfmark.strip().startswith('99'):
        sys_id = shelfmark.strip()

    content = create_layout()
    with content:
        from web.pages.browse import create_browse_page
        create_browse_page(initial_sys_id=sys_id, highlight=highlight, initial_fl_id=fl_id, initial_page=page, initial_shelfmark=shelfmark if sys_id is None or sys_id != (shelfmark or '').strip() else None, initial_volume_ie=volume_ie)

@ui.page('/catalog-browse', title='Catalog Browse | עיון בקטלוג — Dicta Genizah Search')
def catalog_browse_page_route(
    domain: str = None, author: str = None, work: str = None, page: int = None,
    text_all: str = None, text_any: str = None, text_not: str = None,
):
    set_current_page('/catalog-browse')
    _catalog_parts = [p for p in [domain, author, work] if p]
    if _catalog_parts:
        _catalog_label = ' / '.join(_catalog_parts)
        _catalog_title = f'{_catalog_label} — Catalog | Dicta Genizah Search'
        ui.page_title(_catalog_title)
        # Build canonical with active filters so metadata matches the indexed URL
        from urllib.parse import urlencode
        _filter_params = {k: v for k, v in [('domain', domain), ('author', author), ('work', work)] if v}
        _catalog_qs = '?' + urlencode(_filter_params) if _filter_params else ''
        ui.add_head_html(page_meta(
            f'/catalog-browse{_catalog_qs}',
            title=_catalog_title,
            description=f'Browse Cairo Genizah manuscripts classified as {_catalog_label}. Faceted catalog with domain, author, and work filters.',
        ))
    else:
        ui.add_head_html(page_meta(
            '/catalog-browse',
            title='Catalog Browse | עיון בקטלוג — Dicta Genizah Search',
            description='Browse 255,000+ Cairo Genizah manuscripts by domain, author, and work. Catalog based on Friedberg Genizah data. עיון בקטלוג הגניזה.',
        ))
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.catalog_browse import create_catalog_browse_page
        create_catalog_browse_page(
            initial_domain=domain,
            initial_author=author,
            initial_work=work,
            initial_page=page,
            initial_text_all=text_all,
            initial_text_any=text_any,
            initial_text_not=text_not,
        )

@ui.page('/lists', title='My Lists | הרשימות שלי — Dicta Genizah Search')
def lists_page_route():
    set_current_page('/lists')
    ui.add_head_html(page_meta('/lists', noindex=True))
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.lists import create_lists_page
        create_lists_page()

@ui.page('/puzzle', title='Fragment Puzzle | Dicta Genizah Search')
def puzzle_page_route(add: str = None, doc: str = None):
    set_current_page('/puzzle')
    ui.add_head_html(page_meta(
        '/puzzle',
        title='Fragment Puzzle | Dicta Genizah Search',
        description='Visually arrange and join Cairo Genizah manuscript fragments. Background removal, zoom, rotate, and composite export for scholarly joins.',
        needs_iiif=True,
    ))
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        if not WEB_PUZZLE_ENABLED:
            is_hebrew = get_language() == 'he'
            with ui.column().classes('w-full max-w-3xl mx-auto p-6'):
                with ui.card().classes('w-full p-8'):
                    ui.icon('construction').classes('text-4xl text-amber-600 mb-3')
                    ui.label('פאזל הקטעים אינו זמין כרגע' if is_hebrew else 'Fragment Puzzle is temporarily unavailable').classes(
                        'text-2xl font-bold mb-2'
                    )
                    ui.label(
                        'הפיצ׳ר הוסתר זמנית עד לייצוב תשתית התמונות והעיבוד.'
                        if is_hebrew else
                        'The feature is temporarily hidden while the image-processing path is being stabilized.'
                    ).classes('text-base').style('color: var(--text-secondary);')
                    with ui.row().classes('gap-2 mt-4'):
                        ui.button(
                            'חזרה לעיון בכתב יד' if is_hebrew else 'Go to Browse',
                            on_click=lambda: ui.navigate.to('/browse'),
                        ).props('color=primary')
                        ui.button(
                            'עמוד הבית' if is_hebrew else 'Home',
                            on_click=lambda: ui.navigate.to('/'),
                        ).props('flat')
            return
        from web.pages.puzzle import create_puzzle_page
        create_puzzle_page(initial_add=add, initial_doc=doc)

@ui.page('/privacy-extension', title='GenizahSearch Image Helper — Privacy Policy')
def privacy_extension_route():
    """Privacy policy for the GenizahSearch Image Helper browser extension."""
    ui.add_head_html(page_meta(
        '/privacy-extension',
        title='GenizahSearch Image Helper — Privacy Policy',
        description='Privacy policy for the GenizahSearch Image Helper browser extension. No personal data is collected or transmitted.',
    ))
    with ui.column().classes('w-full max-w-3xl mx-auto p-8'):
        ui.label('GenizahSearch Image Helper — Privacy Policy').classes('text-2xl font-bold mb-4')
        ui.label('Last updated: March 18, 2026').classes('text-sm text-gray-500 mb-6')
        for title, text in [
            ('What this extension does',
             'The GenizahSearch Image Helper fetches manuscript fragment images from the National Library of Israel (NLI) '
             'IIIF image service (iiif.nli.org.il) through your browser. These images are sent to genizahsearch.com for '
             'background removal processing, enabling the Fragment Puzzle feature.'),
            ('Data collection',
             'This extension does NOT collect, store, or transmit any personal data. It does not track browsing history, '
             'keystrokes, location, or any other user information.'),
            ('What data is transmitted',
             'Only manuscript image data fetched from iiif.nli.org.il is transmitted to genizahsearch.com for processing. '
             'No personal information is included in these requests.'),
            ('Third parties',
             'No data is sold, shared, or transferred to any third party. Image data is sent only to genizahsearch.com '
             '(operated by Dicta, the Israel Center for Text Analysis) for processing.'),
            ('Permissions',
             'The extension requires access to iiif.nli.org.il to fetch manuscript images, and to genizahsearch.com '
             'to communicate with the web application. No other sites are accessed.'),
            ('Contact',
             'For questions about this privacy policy, contact us at genizahsearch.com.'),
        ]:
            ui.label(title).classes('text-lg font-semibold mt-4 mb-1')
            ui.label(text).classes('text-sm leading-relaxed')

@ui.page('/reset-hints')
def reset_hints_route():
    """Hidden utility route to reset all feature discovery hints."""
    for key in ('whats_new_dismissed', 'hint_responsa_seen', 'hint_tabular_seen'):
        app.storage.user.pop(key, None)
    ui.navigate.to('/')

@ui.page('/settings', title='Settings | הגדרות — Dicta Genizah Search')
def settings_page_route():
    set_current_page('/settings')
    ui.add_head_html(page_meta('/settings', noindex=True))
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.settings import create_settings_page
        create_settings_page()

@ui.page('/help', title='Help | Dicta Genizah Search')
def help_page_route():
    set_current_page('/help')
    ui.add_head_html(page_meta(
        '/help',
        title='Help | Dicta Genizah Search',
        description='User guide for Dicta Genizah Search: full-text search, Responsa syntax, catalog browsing, manuscript viewer, fragment puzzle, and research tools.',
    ))
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.help import create_help_page
        create_help_page()

@ui.page('/corrections', title='Corrections | תיקונים — Dicta Genizah Search')
async def corrections_page_route():
    set_current_page('/corrections')
    ui.add_head_html(page_meta('/corrections', noindex=True))
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.corrections import create_corrections_page
        await create_corrections_page()

@ui.page('/discoveries', title='Discoveries Center | Dicta Genizah Search')
def discoveries_page_route():
    set_current_page('/discoveries')
    ui.add_head_html(page_meta(
        '/discoveries',
        title='Discoveries Center | Dicta Genizah Search',
        description='Community-published Cairo Genizah discoveries: fragment joins, new identifications, and scholarly findings shared by researchers.',
    ))
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.discoveries import create_discoveries_page
        create_discoveries_page()

@ui.page('/admin', title='Admin — Dicta Genizah Search')
async def admin_page_route():
    set_current_page('/admin')
    ui.add_head_html(page_meta('/admin', noindex=True))
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.admin import create_admin_page
        await create_admin_page()

@ui.page('/profile', title='Profile | פרופיל — Dicta Genizah Search')
async def profile_page_route():
    set_current_page('/profile')
    ui.add_head_html(page_meta('/profile', noindex=True))
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.profile import create_profile_page
        await create_profile_page()

@ui.page('/accessibility', title='Accessibility | Dicta Genizah Search')
def accessibility_page_route():
    set_current_page('/accessibility')
    ui.add_head_html(page_meta(
        '/accessibility',
        title='Accessibility | Dicta Genizah Search',
        description='Accessibility statement for Dicta Genizah Search. Keyboard navigation, screen reader support, and accommodations for researchers.',
    ))
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.accessibility import create_accessibility_page
        create_accessibility_page()


@ui.page('/about', title='About the Cairo Genizah | על גניזת קהיר — Dicta Genizah Search')
def about_page_route():
    set_current_page('/about')
    ui.add_head_html(page_meta(
        '/about',
        title='About the Cairo Genizah | על גניזת קהיר — Dicta Genizah Search',
        description='The Cairo Genizah: over 350,000 medieval manuscript fragments from the Ben Ezra Synagogue in Cairo, spanning 1,000 years of Jewish life. Search the transcriptions for the first time.',
        og_type='article',
    ))
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.about import create_about_page
        create_about_page()


@ui.page('/download', title='Download Desktop App | Dicta Genizah Search')
def download_page_route():
    set_current_page('/download')
    ui.add_head_html(page_meta(
        '/download',
        title='Download Desktop App | Dicta Genizah Search',
        description='Download the Dicta Genizah Search desktop application for Windows. Full offline search across Cairo Genizah manuscripts.',
    ))
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(POSTHOG_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.download import create_download_page
        create_download_page()


@ui.page('/auth/callback')
async def auth_callback_route(code: str = None, error: str = None, error_description: str = None):
    """
    OAuth callback handler.
    Supabase redirects here after Google login with ?code= parameter (PKCE flow).
    Also handles ?error= / ?error_description= from cancelled or failed OAuth attempts.
    """
    from web.supabase_client import get_profile, exchange_code_for_session
    from web.auth_state import GlobalAuthState

    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())
    ui.add_head_html(POSTHOG_SCRIPT)

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
        posthog_capture('login_success', {'method': 'google_oauth'})
        status_label.text = 'Login successful! Redirecting...'
        await asyncio.sleep(0.5)
        ui.navigate.to('/')

    def show_error(message):
        """Display error and show home button."""
        posthog_capture('login_failed', {'reason': str(message)[:100], 'method': 'google_oauth'})
        spinner.set_visibility(False)
        status_label.set_visibility(False)
        error_label.text = message
        error_label.classes(remove='hidden')
        home_btn.classes(remove='hidden')

    try:
        # Handle OAuth error params (e.g., user cancelled consent, provider error)
        if error or error_description:
            error_msg = error_description or error
            logger.warning(f"OAuth callback error from provider: {error_msg}")
            show_error(f'Authentication failed: {error_msg}')
            return

        # PKCE flow - code in query parameter
        if code:
            logger.info(f"OAuth callback: exchanging code {code[:20]}...")
            result = exchange_code_for_session(code)
            logger.info(f"Code exchange result: {result}")

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

        # No code or error parameter -- redirect home
        logger.info("OAuth callback: no code or error params, redirecting home")
        ui.navigate.to('/')

    except Exception as e:
        logger.exception(f"OAuth callback error: {e}")
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
    print("[init] Starting background initialization...")

    def _init_sync():
        try:
            # 1. Metadata
            print("[init] 1/6 MetadataManager...", flush=True)
            state.meta_mgr = MetadataManager()
            print("[init] 2/6 ListsManager...", flush=True)
            state.lists_mgr = ListsManager(state.meta_mgr)

            # Initialize user lists manager (auth-aware wrapper)
            state.init_user_lists_mgr()

            # 2. Lab Settings & Engine
            print("[init] 3/6 LabEngine...", flush=True)
            state.lab_engine = LabEngine(state.meta_mgr, None)

            # 3. Variants (depends on Lab Settings)
            state.var_mgr = VariantManager(settings=state.lab_engine.settings)

            # 4. Search Engine & Indexer
            print("[init] 4/6 SearchEngine...", flush=True)
            state.searcher = SearchEngine(state.meta_mgr, state.var_mgr)
            print("[init] 5/6 Indexer...", flush=True)
            state.indexer = Indexer(state.meta_mgr)

            # 5. Start background loading
            state.meta_mgr.start_background_loading()

            print("[init] Engine initialization complete (searcher ready).", flush=True)

            return True
        except Exception as e:
            print(f"[init] Engine init FAILED: {e}", flush=True)
            import traceback
            traceback.print_exc()
            return False

    init_ok = await run.io_bound(_init_sync)

    async def _prewarm_fjms_background():
        """Run FJMS browse cache pre-warm after readiness, off the startup critical path."""
        def _prewarm_sync():
            try:
                from shared.fjms_service import get_fjms_service
                fjms = get_fjms_service(thread_safe=True)
                if fjms.is_available():
                    fjms.pre_warm_caches()
                    print("[init] FJMS caches pre-warmed (background).", flush=True)
            except Exception as e:
                print(f"[init] FJMS cache pre-warm failed (non-fatal): {e}", flush=True)

        await run.io_bound(_prewarm_sync)

    if init_ok:
        asyncio.create_task(_prewarm_fjms_background())

app.on_startup(initialize_engine)

def _find_free_port(start_port: int, max_attempts: int = 10) -> int:
    """Find a free port starting from start_port. Returns the first available port."""
    import socket
    for offset in range(max_attempts):
        port = start_port + offset
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('0.0.0.0', port))
                return port
        except OSError:
            if offset == 0:
                print(f"  Port {port} is busy, trying next...")
    return start_port  # Fall through — let uvicorn report the error


if __name__ in {'__main__', '__mp_main__'}:
    # Production settings via environment variables
    reload_enabled = os.environ.get('NICEGUI_RELOAD', 'true').lower() == 'true'
    show_browser = os.environ.get('NICEGUI_SHOW', 'true').lower() == 'true'

    # In dev mode, auto-find a free port if the default is busy.
    # In production (reload=false), use the configured port strictly.
    run_port = _find_free_port(APP_PORT) if reload_enabled else APP_PORT

    print(f"\n{'='*60}")
    print(f"  Dicta Genizah Search v{APP_VERSION}")
    print(f"  Starting on port {run_port}...")
    print(f"{'='*60}\n")

    favicon_path = os.path.join(os.path.dirname(__file__), 'static', 'favicon.ico')

    # Reconnect timeout (seconds) - how long client waits before giving up reconnection
    # Higher value = more patient reconnection attempts under load
    reconnect_timeout = int(os.environ.get('NICEGUI_RECONNECT_TIMEOUT', '30'))

    ui.run(
        title=APP_TITLE,
        port=run_port,
        reload=reload_enabled,
        show=show_browser,
        storage_secret='genizah-secret-v5',
        favicon=favicon_path,
        reconnect_timeout=reconnect_timeout,
    )
