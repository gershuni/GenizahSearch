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
from genizah_core import MetadataManager, VariantManager, SearchEngine, LabEngine, Indexer, AIManager, ListsManager, Config

# App configuration
APP_TITLE = "Dicta Genizah Search | חיפוש גניזת קהיר"
APP_VERSION = "5.1"
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

# ============================================================================
# Modern Theme System - Professional Research UI
# ============================================================================

COMMON_STYLES = '''
<style>
    /* ========================================================================
       CSS Custom Properties - Theme System
       ======================================================================== */

    :root {
        /* Primary Colors - Deep Academic Green */
        --primary-50: #ecfdf5;
        --primary-100: #d1fae5;
        --primary-200: #a7f3d0;
        --primary-300: #6ee7b7;
        --primary-400: #34d399;
        --primary-500: #10b981;
        --primary-600: #059669;
        --primary-700: #047857;
        --primary-800: #065f46;
        --primary-900: #064e3b;

        /* Neutral Colors */
        --neutral-50: #fafafa;
        --neutral-100: #f5f5f5;
        --neutral-200: #e5e5e5;
        --neutral-300: #d4d4d4;
        --neutral-400: #a3a3a3;
        --neutral-500: #737373;
        --neutral-600: #525252;
        --neutral-700: #404040;
        --neutral-800: #262626;
        --neutral-900: #171717;

        /* Accent Colors */
        --accent-gold: #d4a574;
        --accent-amber: #f59e0b;
        --accent-blue: #3b82f6;
        --accent-purple: #8b5cf6;
        --accent-rose: #f43f5e;

        /* Semantic Colors */
        --success: #10b981;
        --warning: #f59e0b;
        --error: #ef4444;
        --info: #3b82f6;

        /* Light Theme (Default) */
        --bg-primary: #ffffff;
        --bg-secondary: #f8fafc;
        --bg-tertiary: #f1f5f9;
        --bg-header: linear-gradient(135deg, #065f46 0%, #047857 50%, #059669 100%);
        --bg-sidebar: #ffffff;
        --bg-card: #ffffff;
        --bg-hover: #f1f5f9;
        --bg-active: #ecfdf5;

        --text-primary: #1e293b;
        --text-secondary: #475569;
        --text-tertiary: #64748b;
        --text-muted: #94a3b8;
        --text-inverse: #ffffff;

        --border-light: #e2e8f0;
        --border-medium: #cbd5e1;
        --border-focus: #059669;

        --shadow-sm: 0 1px 2px 0 rgb(0 0 0 / 0.05);
        --shadow-md: 0 4px 6px -1px rgb(0 0 0 / 0.1), 0 2px 4px -2px rgb(0 0 0 / 0.1);
        --shadow-lg: 0 10px 15px -3px rgb(0 0 0 / 0.1), 0 4px 6px -4px rgb(0 0 0 / 0.1);
        --shadow-xl: 0 20px 25px -5px rgb(0 0 0 / 0.1), 0 8px 10px -6px rgb(0 0 0 / 0.1);
    }

    /* ========================================================================
       Accessibility Focus Styles (P0-C)
       ======================================================================== */

    :focus-visible {
        outline: 2px solid var(--primary-600) !important;
        outline-offset: 2px !important;
    }

    /* Ensure visible focus on buttons and links */
    button:focus-visible, a:focus-visible, [role="button"]:focus-visible, [tabindex="0"]:focus-visible {
        outline: 2px solid var(--primary-600) !important;
        outline-offset: 2px !important;
    }

    /* Adjust for dark theme */
    [data-theme="dark"] :focus-visible {
        outline-color: var(--primary-400) !important;
    }

    /* Parchment Theme - Academic & Warm */
    [data-theme="parchment"] {
        --bg-primary: #fffbf5;
        --bg-secondary: #fef7ed;
        --bg-tertiary: #fef3e2;
        --bg-header: linear-gradient(135deg, #78350f 0%, #92400e 50%, #a16207 100%);
        --bg-sidebar: #fffbf5;
        --bg-card: #fffef9;
        --bg-hover: #fef7ed;
        --bg-active: #fef3c7;

        --text-primary: #422006;
        --text-secondary: #78350f;
        --text-tertiary: #92400e;
        --text-muted: #a16207;

        --border-light: #fde68a;
        --border-medium: #fcd34d;
        --border-focus: #a16207;

        --primary-600: #a16207;
        --primary-700: #92400e;
    }

    /* Dark Theme - Modern & Elegant */
    [data-theme="dark"] {
        --bg-primary: #0f172a;
        --bg-secondary: #1e293b;
        --bg-tertiary: #334155;
        --bg-header: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #334155 100%);
        --bg-sidebar: #1e293b;
        --bg-card: #1e293b;
        --bg-hover: #334155;
        --bg-active: #064e3b;

        --text-primary: #f1f5f9;
        --text-secondary: #cbd5e1;
        --text-tertiary: #94a3b8;
        --text-muted: #64748b;
        --text-inverse: #0f172a;

        --border-light: #334155;
        --border-medium: #475569;
        --border-focus: #10b981;

        --shadow-sm: 0 1px 2px 0 rgb(0 0 0 / 0.3);
        --shadow-md: 0 4px 6px -1px rgb(0 0 0 / 0.4);
        --shadow-lg: 0 10px 15px -3px rgb(0 0 0 / 0.5);
    }

    /* Dark theme input fixes */
    [data-theme="dark"] .q-field__native,
    [data-theme="dark"] .q-field__input,
    [data-theme="dark"] input,
    [data-theme="dark"] textarea {
        color: var(--text-primary) !important;
    }

    [data-theme="dark"] .q-field--outlined .q-field__control {
        background: var(--bg-tertiary) !important;
    }

    [data-theme="dark"] .q-field__label {
        color: var(--text-tertiary) !important;
    }

    [data-theme="dark"] .result-snippet,
    [data-theme="dark"] .transcription-panel,
    [data-theme="dark"] .transcription-content {
        background: var(--bg-tertiary) !important;
        color: var(--text-primary) !important;
    }

    [data-theme="dark"] .q-card {
        background: var(--bg-card) !important;
        color: var(--text-primary) !important;
    }

    /* Dark Theme Menu Fixes */
    [data-theme="dark"] .q-menu {
        background: var(--bg-card) !important;
        color: var(--text-primary) !important;
        border: 1px solid var(--border-light) !important;
    }

    [data-theme="dark"] .q-item {
        color: var(--text-primary) !important;
    }

    [data-theme="dark"] .q-item:hover {
        background: var(--bg-hover) !important;
    }

    [data-theme="dark"] .highlight-match {
        background: linear-gradient(120deg, #854d0e 0%, #a16207 100%) !important;
        color: white !important;
    }

    /* Dark Theme Tabs Fixes */
    [data-theme="dark"] .q-tabs,
    [data-theme="dark"] .q-tab-panels {
        background: var(--bg-card) !important;
        color: var(--text-primary) !important;
    }

    [data-theme="dark"] .q-tab {
        color: var(--text-secondary) !important;
    }

    [data-theme="dark"] .q-tab--active {
        color: var(--primary-400) !important;
    }

    /* Dark Theme Dialog Fixes */
    [data-theme="dark"] .q-dialog__inner {
        background: transparent !important;
    }

    [data-theme="dark"] .q-dialog .q-card {
        background: var(--bg-card) !important;
        color: var(--text-primary) !important;
    }

    /* Dark Theme Expansion Panel Fixes */
    [data-theme="dark"] .q-expansion-item {
        background: var(--bg-card) !important;
        color: var(--text-primary) !important;
    }

    [data-theme="dark"] .q-expansion-item__content {
        background: var(--bg-secondary) !important;
    }

    /* Dark Theme Badge and Chip Fixes */
    [data-theme="dark"] .q-badge {
        color: white !important;
    }

    /* Dark Theme Select/Dropdown Fixes */
    [data-theme="dark"] .q-select .q-field__native,
    [data-theme="dark"] .q-select .q-field__input {
        color: var(--text-primary) !important;
    }

    [data-theme="dark"] .q-select__dropdown-icon {
        color: var(--text-secondary) !important;
    }

    [data-theme="dark"] .q-select .q-chip {
        background: var(--bg-tertiary) !important;
        color: var(--text-primary) !important;
    }

    /* Dark theme option/item text in select dropdown */
    [data-theme="dark"] .q-item__label {
        color: var(--text-primary) !important;
    }

    [data-theme="dark"] .q-item__label--caption {
        color: var(--text-tertiary) !important;
    }

    /* Fix select popup/virtual scroll in dark mode */
    [data-theme="dark"] .q-virtual-scroll__content {
        background: var(--bg-card) !important;
    }

    [data-theme="dark"] .q-select__dialog {
        background: var(--bg-card) !important;
    }

    /* Parchment theme input fixes */
    [data-theme="parchment"] .q-field__native,
    [data-theme="parchment"] .q-field__input,
    [data-theme="parchment"] input,
    [data-theme="parchment"] textarea {
        color: var(--text-primary) !important;
    }

    /* ========================================================================
       Base Styles
       ======================================================================== */

    * {
        box-sizing: border-box;
    }

    body {
        background-color: var(--bg-secondary);
        color: var(--text-primary);
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
        line-height: 1.6;
        margin: 0;
        padding: 0;
    }

    /* Hebrew text styling */
    .hebrew-text, [dir="rtl"] {
        font-family: 'David', 'Frank Ruehl', 'Noto Sans Hebrew', 'SBL Hebrew', serif;
    }

    /* ========================================================================
       Header Styles
       ======================================================================== */

    .q-header {
        background: var(--bg-header) !important;
        box-shadow: var(--shadow-lg) !important;
    }

    .app-header {
        height: 64px;
        display: flex;
        align-items: center;
        padding: 0 24px;
    }

    .logo-container {
        display: flex;
        align-items: center;
        gap: 12px;
    }

    .logo-icon {
        font-size: 2rem;
        color: white;
        opacity: 0.95;
    }

    .logo-text {
        font-size: 1.25rem;
        font-weight: 700;
        color: white;
        letter-spacing: 0.02em;
    }

    .logo-version {
        font-size: 0.7rem;
        background: rgba(255,255,255,0.2);
        padding: 2px 8px;
        border-radius: 12px;
        margin-inline-start: 8px;
    }

    /* Status indicator */
    .status-indicator {
        display: flex;
        align-items: center;
        gap: 8px;
        background: rgba(255,255,255,0.15);
        padding: 6px 16px;
        border-radius: 20px;
        backdrop-filter: blur(8px);
    }

    .status-dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        animation: pulse 2s ease-in-out infinite;
    }

    .status-dot.ready { background: #34d399; }
    .status-dot.loading { background: #fbbf24; }
    .status-dot.error { background: #f87171; }

    @keyframes pulse {
        0%, 100% { opacity: 1; transform: scale(1); }
        50% { opacity: 0.7; transform: scale(0.95); }
    }

    /* ========================================================================
       Sidebar Styles
       ======================================================================== */

    .q-drawer {
        background-color: var(--bg-sidebar) !important;
    }

    /* Use logical properties for drawer borders - works for both LTR and RTL */
    .q-drawer--left {
        border-right: 1px solid var(--border-light) !important;
    }

    .q-drawer--right {
        border-left: 1px solid var(--border-light) !important;
    }

    .sidebar-nav {
        padding: 16px 0;
    }

    .nav-section-label {
        font-size: 0.7rem;
        font-weight: 700;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        color: var(--text-muted);
        padding: 16px 24px 8px;
    }

    .nav-item {
        display: flex;
        align-items: center;
        gap: 12px;
        padding: 12px 24px;
        margin: 2px 12px;
        border-radius: 10px;
        cursor: pointer;
        transition: all 0.2s ease;
        color: var(--text-secondary);
        text-decoration: none;
        font-weight: 500;
    }

    .nav-item:hover {
        background: var(--bg-hover);
        color: var(--text-primary);
    }

    .nav-item.active {
        background: var(--bg-active);
        color: var(--primary-700);
        font-weight: 600;
        border-inline-start: 3px solid var(--primary-600);
    }

    [data-theme="dark"] .nav-item.active {
        color: var(--primary-400);
        border-inline-start-color: var(--primary-400);
    }

    .nav-item-icon {
        font-size: 1.25rem;
        opacity: 0.85;
    }

    .nav-item-badge {
        margin-inline-start: auto;
        background: var(--primary-100);
        color: var(--primary-700);
        padding: 2px 8px;
        border-radius: 12px;
        font-size: 0.7rem;
        font-weight: 600;
    }

    [data-theme="dark"] .nav-item-badge {
        background: var(--primary-900);
        color: var(--primary-300);
    }

    /* Sidebar Footer */
    .sidebar-footer {
        padding: 16px 24px;
        border-top: 1px solid var(--border-light);
        margin-top: auto;
    }

    /* Global Citation Footer */
    .citation-footer {
        background: var(--primary-50, #eff6ff) !important;
        border-top: 1px solid var(--primary-200, #bfdbfe) !important;
        color: var(--primary-800, #1e40af) !important;
        min-height: auto !important;
        padding: 0 !important;
    }
    .citation-footer a {
        color: var(--primary-700, #1d4ed8) !important;
    }
    .citation-footer a:hover {
        text-decoration: underline !important;
    }
    [data-theme="dark"] .citation-footer {
        background: var(--primary-900, #1e3a5f) !important;
        border-top-color: var(--primary-700, #1e40af) !important;
        color: var(--primary-200, #bfdbfe) !important;
    }
    [data-theme="dark"] .citation-footer a {
        color: var(--primary-300, #93c5fd) !important;
    }

    /* Hide Hebrew label on mobile */
    @media (max-width: 768px) {
        .citation-hebrew-label {
            display: none !important;
        }
    }

    .theme-switcher {
        display: flex;
        gap: 8px;
        justify-content: center;
        padding: 8px;
        background: var(--bg-tertiary);
        border-radius: 10px;
    }

    .theme-btn {
        padding: 8px;
        border-radius: 8px;
        cursor: pointer;
        transition: all 0.2s;
        opacity: 0.7;
    }

    .theme-btn:hover, .theme-btn.active {
        opacity: 1;
        background: var(--bg-primary);
        box-shadow: var(--shadow-sm);
    }

    /* ========================================================================
       Card Styles
       ======================================================================== */

    .q-card {
        background: var(--bg-card) !important;
        border: 1px solid var(--border-light) !important;
        border-radius: 12px !important;
        box-shadow: var(--shadow-sm) !important;
        transition: all 0.2s ease !important;
    }

    .q-card:hover {
        box-shadow: var(--shadow-md) !important;
    }

    .card-elevated {
        box-shadow: var(--shadow-lg) !important;
    }

    /* ========================================================================
       Button Styles
       ======================================================================== */

    .btn-primary {
        background: linear-gradient(135deg, var(--primary-600), var(--primary-700)) !important;
        color: white !important;
        font-weight: 600 !important;
        border-radius: 8px !important;
        padding: 10px 20px !important;
        transition: all 0.2s !important;
        box-shadow: 0 2px 4px rgba(5, 150, 105, 0.3) !important;
    }

    .btn-primary:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 8px rgba(5, 150, 105, 0.4) !important;
    }

    .btn-secondary {
        background: var(--bg-tertiary) !important;
        color: var(--text-primary) !important;
        border: 1px solid var(--border-medium) !important;
    }

    .btn-ghost {
        background: transparent !important;
        color: var(--text-secondary) !important;
    }

    .btn-ghost:hover {
        background: var(--bg-hover) !important;
    }

    /* ========================================================================
       Input Styles
       ======================================================================== */

    .q-field--outlined .q-field__control {
        border-color: var(--border-medium) !important;
        border-radius: 10px !important;
        background: var(--bg-primary) !important;
    }

    .q-field--outlined.q-field--focused .q-field__control {
        border-color: var(--border-focus) !important;
        box-shadow: 0 0 0 3px rgba(5, 150, 105, 0.15) !important;
    }

    .q-field__label {
        color: var(--text-secondary) !important;
    }

    /* ========================================================================
       Content Area
       ======================================================================== */

    .main-content {
        background: var(--bg-secondary);
        min-height: calc(100vh - 64px);
        padding: 24px;
    }

    .content-container {
        max-width: 1400px;
        margin: 0 auto;
    }

    /* Page Header */
    .page-header {
        margin-bottom: 24px;
    }

    .page-title {
        font-size: 2rem;
        font-weight: 700;
        color: var(--text-primary);
        margin: 0 0 8px 0;
    }

    .page-subtitle {
        font-size: 1rem;
        color: var(--text-tertiary);
        margin: 0;
    }

    /* ========================================================================
       Research-Specific Styles
       ======================================================================== */

    /* Result Cards */
    .result-card {
        background: var(--bg-card);
        border: 1px solid var(--border-light);
        border-radius: 12px;
        padding: 16px;
        transition: all 0.2s;
        cursor: pointer;
    }

    .result-card:hover {
        border-color: var(--primary-300);
        box-shadow: var(--shadow-md);
        transform: translateY(-2px);
    }

    .result-shelfmark {
        font-size: 1.1rem;
        font-weight: 700;
        color: var(--primary-700);
    }

    .result-title {
        font-size: 0.9rem;
        color: var(--text-secondary);
        margin-top: 4px;
    }

    .result-snippet {
        font-size: 0.95rem;
        line-height: 1.8;
        margin-top: 12px;
        padding: 12px;
        background: var(--bg-tertiary);
        border-radius: 8px;
        direction: rtl;
        text-align: right;
    }

    .highlight-match {
        background: linear-gradient(120deg, #fef08a 0%, #fde047 100%);
        padding: 2px 4px;
        border-radius: 3px;
        font-weight: 600;
    }

    /* Stats Cards */
    .stat-card {
        background: var(--bg-card);
        border: 1px solid var(--border-light);
        border-radius: 16px;
        padding: 24px;
        text-align: center;
        transition: all 0.3s;
    }

    .stat-card:hover {
        transform: translateY(-4px);
        box-shadow: var(--shadow-lg);
    }

    .stat-value {
        font-size: 2.5rem;
        font-weight: 800;
        background: linear-gradient(135deg, var(--primary-600), var(--primary-800));
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }

    .stat-label {
        font-size: 0.9rem;
        color: var(--text-tertiary);
        margin-top: 8px;
        font-weight: 500;
    }

    /* Advanced Options Panel */
    .options-panel {
        background: var(--bg-tertiary);
        border: 1px solid var(--border-light);
        border-radius: 12px;
        padding: 20px;
    }

    .options-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
        gap: 16px;
    }

    /* ========================================================================
       Scrollbar Styles
       ======================================================================== */

    ::-webkit-scrollbar {
        width: 8px;
        height: 8px;
    }

    ::-webkit-scrollbar-track {
        background: var(--bg-tertiary);
        border-radius: 4px;
    }

    ::-webkit-scrollbar-thumb {
        background: var(--neutral-400);
        border-radius: 4px;
    }

    ::-webkit-scrollbar-thumb:hover {
        background: var(--neutral-500);
    }

    /* ========================================================================
       Responsive Styles
       ======================================================================== */

    @media (max-width: 768px) {
        .app-header {
            padding: 0 16px;
        }

        .main-content {
            padding: 16px;
        }

        .page-title {
            font-size: 1.5rem;
        }

        .stat-card {
            padding: 16px;
        }

        .stat-value {
            font-size: 1.75rem;
        }
    }

    /* ========================================================================
       Animation Classes
       ======================================================================== */

    .fade-in {
        animation: fadeIn 0.3s ease-out;
    }

    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(10px); }
        to { opacity: 1; transform: translateY(0); }
    }

    .slide-in-left {
        animation: slideInLeft 0.3s ease-out;
    }

    @keyframes slideInLeft {
        from { opacity: 0; transform: translateX(-20px); }
        to { opacity: 1; transform: translateX(0); }
    }

    /* ========================================================================
       Utility Classes
       ======================================================================== */

    .text-gradient {
        background: linear-gradient(135deg, var(--primary-600), var(--accent-gold));
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }

    .border-gradient {
        border-image: linear-gradient(135deg, var(--primary-400), var(--accent-gold)) 1;
    }

    .glass {
        background: rgba(255, 255, 255, 0.1);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255, 255, 255, 0.2);
    }

    /* ========================================================================
       Mobile Responsive - Minimal Breakpoints
       ======================================================================== */

    /* Mobile select dialog dark mode fixes */
    [data-theme="dark"] .q-dialog--menu .q-card,
    [data-theme="dark"] .q-select__dialog .q-card {
        background: var(--bg-card) !important;
        color: var(--text-primary) !important;
    }

    [data-theme="dark"] .q-dialog--menu .q-item,
    [data-theme="dark"] .q-select__dialog .q-item {
        color: var(--text-primary) !important;
    }

    [data-theme="dark"] .q-dialog--menu .q-item:hover,
    [data-theme="dark"] .q-select__dialog .q-item:hover,
    [data-theme="dark"] .q-dialog--menu .q-item--active,
    [data-theme="dark"] .q-select__dialog .q-item--active {
        background: var(--bg-hover) !important;
    }

    /* Parchment theme select/dropdown fixes */
    [data-theme="parchment"] .q-menu {
        background: var(--bg-card) !important;
        border: 1px solid var(--border-light) !important;
    }

    [data-theme="parchment"] .q-item {
        color: var(--text-primary) !important;
    }

    [data-theme="parchment"] .q-item:hover {
        background: var(--bg-hover) !important;
    }

    [data-theme="parchment"] .q-select .q-field__native {
        color: var(--text-primary) !important;
    }

    /* Tablet and below */
    @media (max-width: 768px) {
        /* Prevent iOS zoom on input focus */
        input, select, textarea { font-size: 16px !important; }

        /* Touch-friendly targets */
        button, .clickable, [role="button"] { min-height: 44px; }

        /* Prevent horizontal scroll */
        body { overflow-x: hidden; }
    }

    /* Mobile */
    @media (max-width: 640px) {
        .app-header { padding: 0 12px; }
        .main-content { padding: 12px; }
        .page-title { font-size: 1.5rem; }
    }

    /* Small mobile */
    @media (max-width: 480px) {
        .main-content { padding: 8px; }
        .page-title { font-size: 1.25rem; }

        /* Dialogs fullscreen */
        .q-dialog__inner { padding: 0 !important; }
        .q-dialog .q-card {
            width: 100vw !important;
            max-width: 100vw !important;
            border-radius: 0 !important;
        }
    }

    /* Utility classes */
    @media (max-width: 640px) {
        .hide-mobile { display: none !important; }
    }
    @media (min-width: 641px) {
        .show-mobile-only { display: none !important; }
    }

    /* === Page-specific mobile styles === */

    /* Home page */
    @media (max-width: 640px) {
        .tool-card { min-width: 100% !important; }
        .mini-stat-card { padding: 12px !important; }
    }

    /* Search page */
    @media (max-width: 768px) {
        /* Hide viewer panel on mobile - results expand inline instead */
        .search-splitter .q-splitter__after { display: none !important; }
        .search-splitter .q-splitter__separator { display: none !important; }
        .search-splitter .q-splitter__before { width: 100% !important; flex: 1 !important; }
        .filters-grid { grid-template-columns: 1fr !important; }
        /* Show mobile expansion in result cards */
        .result-mobile-expand { display: block !important; }
    }
    @media (min-width: 769px) {
        /* Hide mobile expansion on desktop */
        .result-mobile-expand { display: none !important; }
    }

    /* Collapsible Search Panel */
    .search-panel-container {
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    }

    .search-panel-expanded,
    .search-panel-collapsed {
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        overflow: hidden;
    }

    .search-panel-collapsed {
        animation: slideDown 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    }

    .search-panel-expanded {
        animation: slideDown 0.3s cubic-bezier(0.4, 0, 0.2, 1);
    }

    @keyframes slideDown {
        from {
            opacity: 0;
            transform: translateY(-10px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }

    /* Collapsed panel hover effect */
    .search-panel-collapsed:hover {
        background: var(--bg-hover) !important;
    }

    /* Ensure smooth transition for panel content */
    .search-panel-container .q-card {
        will-change: transform, opacity;
    }

    /* RTL support for collapsed panel */
    [dir="rtl"] .search-panel-collapsed .q-row,
    .hebrew-mode .search-panel-collapsed .q-row {
        flex-direction: row-reverse;
    }

    /* Collapsed panel query text RTL */
    .search-panel-collapsed .truncate {
        direction: rtl;
        text-align: right;
    }

    /* Mobile adjustments for collapsible search panel */
    @media (max-width: 768px) {
        .search-panel-expanded {
            padding: 12px !important;
        }

        .search-panel-collapsed {
            padding: 8px 12px !important;
        }

        /* Stack search elements vertically on very small screens */
        .search-panel-expanded .flex-wrap {
            gap: 12px !important;
        }

        /* Collapse toggle button more visible on mobile */
        .search-panel-container [class*="expand_less"],
        .search-panel-container [class*="expand_more"] {
            font-size: 1.5rem !important;
        }
    }

    @media (max-width: 480px) {
        /* Very small screens: hide some labels in collapsed view */
        .search-panel-collapsed .q-badge {
            display: none !important;
        }

        /* Full width search button in collapsed mode */
        .search-panel-collapsed .btn-primary {
            padding: 4px 12px !important;
            font-size: 0.75rem !important;
        }
    }

    /* Browse page */
    @media (max-width: 768px) {
        .browse-container { flex-direction: column !important; }
        .image-panel, .transcription-panel { width: 100% !important; max-height: 50vh !important; }
    }

    /* Lists page */
    @media (max-width: 768px) {
        .lists-layout { flex-direction: column !important; }
        .lists-sidebar { width: 100% !important; max-height: 200px !important; }
    }

    /* Header mobile adjustments */
    @media (max-width: 640px) {
        .app-header { padding: 0 8px !important; }
        .app-header .status-indicator {
            padding: 4px 8px !important;
            font-size: 0.7rem !important;
        }
        .app-header .status-indicator .status-text { display: none; }
        .app-header .auth-buttons { gap: 4px !important; }
        .app-header .auth-buttons .q-btn {
            min-width: 32px !important;
            min-height: 32px !important;
            padding: 4px !important;
        }
        /* Hide help button on mobile (keyboard shortcuts not relevant for touch) */
        .help-btn-header { display: none !important; }
        /* Hide logo text on mobile - only show icon */
        .header-logo-text { display: none !important; }
    }

    /* Ensure logo text doesn't shrink */
    .header-logo-text {
        flex-shrink: 0 !important;
        white-space: nowrap !important;
    }

    /* Header reveal only on mobile - disable on desktop */
    @media (min-width: 769px) {
        .q-header {
            transform: translateY(0) !important;
        }
    }

    /* Mobile drawer - CSS only, hide on phones/tablets */
    /* Handles both LTR (left drawer) and RTL (right drawer) modes */
    @media (max-width: 768px) {
        .q-drawer--left:not(.q-drawer--on-top) {
            transform: translateX(-100%) !important;
        }
        .q-drawer--right:not(.q-drawer--on-top) {
            transform: translateX(100%) !important;
        }
    }

    /* Hebrew mode text direction enhancements */
    /* Layout RTL is handled by Quasar natively; these rules enhance text content */

    /* Ensure Inputs in Hebrew mode are RTL */
    .hebrew-mode input,
    .hebrew-mode textarea,
    .hebrew-mode .q-field__native,
    .hebrew-mode .q-field__input {
        direction: rtl !important;
        text-align: right !important;
    }

    /* Content Text (Markdown, Paragraphs, Headings) should be RTL in Hebrew */
    .hebrew-mode .q-markdown,
    .hebrew-mode p,
    .hebrew-mode .text-body1,
    .hebrew-mode .text-body2,
    .hebrew-mode h1,
    .hebrew-mode h2,
    .hebrew-mode h3,
    .hebrew-mode h4,
    .hebrew-mode h5,
    .hebrew-mode h6,
    .hebrew-mode .q-item__label,
    .hebrew-mode .q-field__label {
        direction: rtl !important;
        text-align: right !important;
        width: 100%; /* Ensure headings take full width to allow right alignment */
    }

    /* Ensure main content container aligns text children to right by default in Hebrew */
    /* This catches loose text and inline-block elements */
    .hebrew-mode .main-content {
        text-align: right;
    }

    /* Specific overrides for content that must be RTL */
    .hebrew-mode .result-snippet,
    .hebrew-mode .hebrew-text {
        direction: rtl;
        text-align: right;
    }

    /* ========================================================================
       Diff Highlighting for Corrections
       ======================================================================== */

    .diff-deleted {
        background: #fecaca;
        text-decoration: line-through;
        padding: 2px 4px;
        border-radius: 3px;
        color: #991b1b;
    }

    .diff-inserted {
        background: #bbf7d0;
        padding: 2px 4px;
        border-radius: 3px;
        color: #166534;
    }

    [data-theme="dark"] .diff-deleted {
        background: #7f1d1d;
        color: #fecaca;
    }

    [data-theme="dark"] .diff-inserted {
        background: #14532d;
        color: #bbf7d0;
    }

    /* ========================================================================
       Fullscreen Edit Mode Overlay
       ======================================================================== */

    .fullscreen-edit-overlay {
        position: fixed;
        top: 0;
        left: 0;
        width: 100vw;
        height: 100vh;
        z-index: 9999;
        background: var(--bg-primary);
        display: flex;
        flex-direction: column;
    }

    .fullscreen-edit-toolbar {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 8px 16px;
        background: var(--bg-secondary);
        border-bottom: 1px solid var(--border-light);
        flex-shrink: 0;
    }

    .fullscreen-edit-content {
        display: flex;
        flex: 1;
        overflow: hidden;
    }

    .fullscreen-edit-image-wrapper {
        flex: 1;
        display: flex;
        flex-direction: column;
        min-width: 200px;
        overflow: hidden;
    }

    .fullscreen-image-toolbar {
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 4px;
        padding: 6px 10px;
        background: #111;
        border-bottom: 1px solid #333;
        flex-shrink: 0;
    }

    .fullscreen-image-toolbar .q-btn {
        color: #aaa !important;
    }

    .fullscreen-image-toolbar .q-btn:hover {
        color: #fff !important;
    }

    .fullscreen-edit-image {
        flex: 1;
        background: #1a1a1a;
        overflow: hidden;
        position: relative;
        display: flex;
        align-items: center;
        justify-content: center;
    }

    .fullscreen-edit-text {
        flex: 1;
        background: var(--bg-primary);
        overflow: hidden;
        padding: 10px;
    }

    .fullscreen-edit-text .q-textarea {
        height: 100%;
    }

    .fullscreen-edit-text textarea {
        resize: none !important;
        overflow-y: auto !important;
    }

    /* Draggable splitter between image and text */
    .fullscreen-edit-splitter {
        width: 6px;
        background: var(--border-light);
        cursor: col-resize;
        flex-shrink: 0;
        transition: background 0.2s;
    }

    .fullscreen-edit-splitter:hover,
    .fullscreen-edit-splitter.dragging {
        background: var(--q-primary);
    }

    /* Fullscreen image should fill and allow pan/zoom */
    .fullscreen-edit-image .zoomable-image {
        max-width: none;
        max-height: none;
        transform-origin: center center;
    }

    @media (max-width: 768px) {
        .fullscreen-edit-content {
            flex-direction: column;
        }
        .fullscreen-edit-image,
        .fullscreen-edit-text {
            flex: 1;
            max-height: 50vh;
        }
        .fullscreen-edit-text {
            border-left: none;
            border-top: 1px solid var(--border-light);
        }
        .fullscreen-edit-splitter {
            width: 100%;
            height: 6px;
            cursor: row-resize;
        }
    }

    /* ========================================================================
       Page Loading Progress Bar - GitHub/YouTube style
       ======================================================================== */

    .page-loading-bar {
        position: fixed;
        top: 0;
        left: 0;
        width: 100%;
        height: 3px;
        z-index: 9999;
        pointer-events: none;
        background: linear-gradient(90deg,
            var(--primary-400) 0%,
            var(--primary-500) 50%,
            var(--primary-400) 100%);
        background-size: 200% 100%;
        transform: translateX(-100%);
        opacity: 0;
        transition: opacity 0.2s ease;
    }

    .page-loading-bar.active {
        opacity: 1;
        animation: loading-progress 1.5s ease-in-out infinite,
                   loading-shimmer 1s linear infinite;
    }

    @keyframes loading-progress {
        0% { transform: translateX(-100%); }
        50% { transform: translateX(-30%); }
        100% { transform: translateX(-10%); }
    }

    @keyframes loading-shimmer {
        0% { background-position: 200% 0; }
        100% { background-position: -200% 0; }
    }

    .page-loading-bar.complete {
        animation: loading-complete 0.3s ease-out forwards;
    }

    @keyframes loading-complete {
        0% { transform: translateX(-10%); opacity: 1; }
        100% { transform: translateX(0%); opacity: 0; }
    }
</style>
'''

# ============================================================================
# Layout Components
# ============================================================================

def create_layout():
    """Create the main application layout with modern Header and Sidebar."""

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
                    ('/reading-desk', 'auto_stories', tr('Reading Desk'), None),
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
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages import home
        if hasattr(home, 'create_page'):
            home.create_page()

@ui.page('/search')
def search_page_route(q: str = None, tag: str = None):
    set_current_page('/search')
    ui.add_head_html(META_TAGS)
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.search import create_search_page
        create_search_page(initial_query=q, initial_tag=tag)

@ui.page('/parallels')
def parallels_page_route(text: str = None):
    set_current_page('/parallels')
    ui.add_head_html(META_TAGS)
    ui.add_head_html(ANALYTICS_SCRIPT)
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
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.browse import create_browse_page
        create_browse_page(initial_sys_id=sys_id, highlight=highlight, initial_fl_id=fl_id, initial_page=page)

@ui.page('/reading-desk')
def reading_desk_page_route(pgpid: int = None, sys_ids: str = None, list_id: str = None):
    set_current_page('/reading-desk')
    ui.add_head_html(META_TAGS)
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.reading_desk import create_reading_desk_page
        create_reading_desk_page(
            initial_pgpid=pgpid,
            initial_sys_ids=sys_ids.split(',') if sys_ids else None,
            initial_list_id=list_id
        )

@ui.page('/lists')
def lists_page_route():
    set_current_page('/lists')
    ui.add_head_html(META_TAGS)
    ui.add_head_html(ANALYTICS_SCRIPT)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.lists import create_lists_page
        create_lists_page()

@ui.page('/settings')
def settings_page_route():
    set_current_page('/settings')
    ui.add_head_html(META_TAGS)
    ui.add_head_html(ANALYTICS_SCRIPT)
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

    async def complete_login(user, profile):
        """Store user in session and redirect."""
        app.storage.user[GlobalAuthState.USER_KEY] = user
        if profile:
            app.storage.user[GlobalAuthState.PROFILE_KEY] = profile
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
                await complete_login(user, profile)
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
            await complete_login(user, profile)
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

            # 5. AI
            state.ai_mgr = AIManager()

            # 6. Start background loading
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
