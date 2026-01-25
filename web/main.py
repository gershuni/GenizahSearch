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

# Ensure we can import from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nicegui import ui, app, run
from web.state import state
from web.api import init_api_routes
from web.translations import tr, is_rtl, get_dir, set_language, get_language
from genizah_core import MetadataManager, VariantManager, SearchEngine, LabEngine, Indexer, AIManager, ListsManager, Config

# App configuration
APP_TITLE = "Genizah Search Pro | חיפוש גניזת קהיר"
APP_VERSION = "5.0"
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
<meta name="description" content="Genizah Search Pro - חיפוש גניזת קהיר. Advanced research platform with full-text search across 500,000+ Cairo Genizah manuscript fragments.">
<meta name="keywords" content="Genizah Search Pro, חיפוש גניזה, גניזת קהיר, כתבי יד, גניזה קהירית, מחקר גניזה, Cairo Genizah, Genizah search, manuscripts, Jewish manuscripts">
<meta name="author" content="Genizah Search Pro">
<meta name="theme-color" content="#059669">

<!-- Open Graph / Facebook / WhatsApp / Slack -->
<meta property="og:type" content="website">
<meta property="og:url" content="https://GenizahSearch.com/">
<meta property="og:title" content="Genizah Search Pro | Cairo Genizah Manuscript Research Platform">
<meta property="og:description" content="Genizah Search Pro - חיפוש גניזת קהיר. Advanced research platform with full-text search across 500,000+ Cairo Genizah manuscript fragments.">
<meta property="og:image" content="https://GenizahSearch.com/static/og-image.png">
<meta property="og:locale" content="he_IL">
<meta property="og:site_name" content="Genizah Search Pro">

<!-- Twitter Card -->
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:url" content="https://GenizahSearch.com/">
<meta name="twitter:title" content="Genizah Search Pro | Cairo Genizah Manuscript Research Platform">
<meta name="twitter:description" content="Genizah Search Pro - חיפוש גניזת קהיר. Advanced research platform with full-text search across 500,000+ Cairo Genizah manuscript fragments.">
<meta name="twitter:image" content="https://GenizahSearch.com/static/og-image.png">

<!-- Canonical URL -->
<link rel="canonical" href="https://GenizahSearch.com/">
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
        margin-left: 8px;
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
        border-right: 1px solid var(--border-light) !important;
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
        border-left: 3px solid var(--primary-600);
    }

    [data-theme="dark"] .nav-item.active {
        color: var(--primary-400);
        border-left-color: var(--primary-400);
    }

    .nav-item-icon {
        font-size: 1.25rem;
        opacity: 0.85;
    }

    .nav-item-badge {
        margin-left: auto;
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
    }

    /* Mobile drawer - CSS only, hide on phones/tablets */
    @media (max-width: 768px) {
        .q-drawer--left:not(.q-drawer--on-top) {
            transform: translateX(-100%) !important;
        }
    }

    /* Force RTL text direction for Hebrew Input/Text only - KEEP LAYOUT LTR */

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
</style>
'''

# ============================================================================
# Layout Components
# ============================================================================

def create_layout():
    """Create the main application layout with modern Header and Sidebar."""

    current_page = app.storage.user.get('current_page', '/')

    # Global LTR Layout (User Request: "Exact English Copy")
    # We do NOT use RTL layout mode. All alignment is LTR.
    # We only inject RTL direction into text content.

    with ui.header().classes('q-py-none').style('height: 64px;'):
        with ui.row().classes('w-full h-full items-center justify-between px-6 app-header'):
            # Left: Menu + Logo
            with ui.row().classes('items-center gap-4'):
                menu_btn = ui.button(icon='menu').props(f'flat round text-color=white aria-label="{tr("Open navigation menu")}"')

                # Logo
                with ui.row().classes('items-center gap-3 cursor-pointer').on('click', lambda: ui.navigate.to('/')):
                    ui.icon('auto_stories').classes('text-3xl text-white opacity-90')
                    with ui.column().classes('gap-0 hidden sm:flex'):
                        ui.label('Genizah Search').classes('text-lg font-bold text-white tracking-wide')
                        ui.label('Pro').classes('text-xs text-white/60')

            # Center: Search Quick Access (optional)
            with ui.row().classes('hidden md:flex items-center'):
                quick_search = ui.input(placeholder=tr('Quick search...')).classes('w-80').props('dark dense outlined rounded')
                quick_search.on('keydown.enter', lambda: ui.navigate.to(f'/search?q={quick_search.value}'))

            # Right: Status + Actions + Auth
            with ui.row().classes('items-center gap-2 sm:gap-4'):
                # Status Indicator
                with ui.row().classes('items-center gap-2 bg-white/15 px-2 sm:px-4 py-1 sm:py-2 rounded-full status-indicator'):
                    status_dot = ui.element('div').classes('w-2 h-2 rounded-full bg-yellow-400')
                    status_text = ui.label(tr('Loading...')).classes('text-xs text-white/90 status-text hidden sm:block')

                    def update_status():
                        if state.is_ready():
                            status_dot.classes('bg-green-400', remove='bg-yellow-400 bg-red-400')
                            status_text.text = tr('Ready')
                        else:
                            status_dot.classes('bg-yellow-400', remove='bg-green-400')
                            status_text.text = tr('Loading...')

                    ui.timer(3.0, update_status, once=True)

                # Auth Buttons (Login/Register or User Menu)
                with ui.row().classes('auth-buttons'):
                    from web.auth_state import create_auth_buttons
                    create_auth_buttons()

                # Help Button (hidden on mobile via CSS)
                ui.button(icon='help_outline', on_click=lambda: ui.navigate.to('/help')).props('flat round text-color=white').tooltip(tr('Help')).classes('help-btn-header')

    # Sidebar (Drawer)
    # Use stored state, default to True (open) on desktop
    drawer_open = app.storage.user.get('drawer_open', True)

    # Standard Left Drawer (LTR Style)
    main_drawer = ui.drawer(side='left', value=drawer_open, bordered=True).classes('shadow-xl').props('width=280 breakpoint=768')

    # Content Area
    content_col = ui.column().classes('main-content w-full items-stretch flex-grow')
    # Add ID for skip link target
    content_col.props('id=main-content')

    def toggle_drawer():
        """Toggle drawer and save state."""
        main_drawer.toggle()
        app.storage.user['drawer_open'] = not app.storage.user.get('drawer_open', True)

    async def nav_to(path):
        """Navigate and close drawer on mobile only."""
        # Check screen width - only close drawer on mobile (<768px)
        width = await ui.run_javascript('window.innerWidth')
        if width < 768:
            app.storage.user['drawer_open'] = False
            main_drawer.hide()
        ui.navigate.to(path)

    # Connect menu button to toggle function
    menu_btn.on('click', toggle_drawer)

    with main_drawer:
        with ui.column().classes('h-full'):
            # Navigation Section
            with ui.column().classes('flex-grow py-4'):
                ui.label(tr('NAVIGATION')).classes('nav-section-label')

                nav_items = [
                    ('/', 'home', tr('Home'), None),
                    ('/search', 'search', tr('Search'), None),
                    ('/parallels', 'compare_arrows', tr('Find Parallels'), None),
                    ('/browse', 'menu_book', tr('Browse'), None),
                    ('/discoveries', 'lightbulb', tr('Discoveries'), None),
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
                        ui.run_javascript(f'document.body.setAttribute("data-theme", "{theme_name}")')

                    current_theme = app.storage.user.get('theme', 'light')

                    with ui.button(icon='light_mode', on_click=lambda: set_theme('light')).props('flat round size=sm').classes(f'theme-btn {"active" if current_theme == "light" else ""}'): pass
                    with ui.button(icon='history_edu', on_click=lambda: set_theme('parchment')).props('flat round size=sm').classes(f'theme-btn {"active" if current_theme == "parchment" else ""}'): pass
                    with ui.button(icon='dark_mode', on_click=lambda: set_theme('dark')).props('flat round size=sm').classes(f'theme-btn {"active" if current_theme == "dark" else ""}'): pass

                # Version Info
                ui.label(f'v{APP_VERSION}').classes('text-xs text-center opacity-50 mt-2')

                # Accessibility Link
                with ui.row().classes('w-full justify-center mt-2'):
                    ui.link(tr('Accessibility Statement'), '/accessibility').classes('text-xs opacity-70 hover:opacity-100').style('text-decoration: none; color: inherit;')

                # Creator Credit
                ui.label(tr('Created by Hillel Gershuni')).classes('text-xs text-center opacity-50 mt-1')

    return content_col


# ============================================================================
# Page Routes
# ============================================================================

def apply_theme_immediately():
    """Add script to apply theme before page renders to prevent flash."""
    current_theme = app.storage.user.get('theme', 'light')
    current_lang = get_language()
    bg_color = "#0f172a" if current_theme == "dark" else "#fffbf5" if current_theme == "parchment" else "#f8fafc"

    # Force LTR Layout Globally (as per user request: "Interface exactly as English")
    # We only set dir="ltr" globally.
    dir_attr = "ltr"

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
            // Execute immediately
            applyTheme();
            // Backup for body element when it exists
            if (document.readyState === 'loading') {{
                document.addEventListener('DOMContentLoaded', applyTheme);
            }}
        }})();
    </script>'''

@ui.page('/')
def dashboard_page():
    app.storage.user['current_page'] = '/'
    current_theme = app.storage.user.get('theme', 'light')
    ui.add_head_html(META_TAGS)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages import home
        if hasattr(home, 'create_page'):
            home.create_page()

@ui.page('/search')
def search_page_route(q: str = None):
    app.storage.user['current_page'] = '/search'
    ui.add_head_html(META_TAGS)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.search import create_search_page
        create_search_page(initial_query=q)

@ui.page('/parallels')
def parallels_page_route(text: str = None):
    app.storage.user['current_page'] = '/parallels'
    ui.add_head_html(META_TAGS)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.parallels import create_parallels_page
        create_parallels_page(initial_text=text)

@ui.page('/browse')
def browse_page_route(sys_id: str = None, highlight: str = None, fl_id: str = None, page: int = None):
    app.storage.user['current_page'] = '/browse'
    ui.add_head_html(META_TAGS)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.browse import create_browse_page
        create_browse_page(initial_sys_id=sys_id, highlight=highlight, initial_fl_id=fl_id, initial_page=page)

@ui.page('/lists')
def lists_page_route():
    app.storage.user['current_page'] = '/lists'
    ui.add_head_html(META_TAGS)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.lists import create_lists_page
        create_lists_page()

@ui.page('/settings')
def settings_page_route():
    app.storage.user['current_page'] = '/settings'
    ui.add_head_html(META_TAGS)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.settings import create_settings_page
        create_settings_page()

@ui.page('/help')
def help_page_route():
    app.storage.user['current_page'] = '/help'
    ui.add_head_html(META_TAGS)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.help import create_help_page
        create_help_page()

@ui.page('/corrections')
async def corrections_page_route():
    app.storage.user['current_page'] = '/corrections'
    ui.add_head_html(META_TAGS)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.corrections import create_corrections_page
        await create_corrections_page()

@ui.page('/discoveries')
async def discoveries_page_route():
    app.storage.user['current_page'] = '/discoveries'
    ui.add_head_html(META_TAGS)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.discoveries import create_discoveries_page
        await create_discoveries_page()

@ui.page('/admin')
async def admin_page_route():
    app.storage.user['current_page'] = '/admin'
    ui.add_head_html(META_TAGS)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.admin import create_admin_page
        await create_admin_page()

@ui.page('/profile')
async def profile_page_route():
    app.storage.user['current_page'] = '/profile'
    ui.add_head_html(META_TAGS)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.profile import create_profile_page
        await create_profile_page()

@ui.page('/accessibility')
def accessibility_page_route():
    app.storage.user['current_page'] = '/accessibility'
    ui.add_head_html(META_TAGS)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.accessibility import create_accessibility_page
        create_accessibility_page()

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
    print(f"  Genizah Search Pro v{APP_VERSION}")
    print(f"  Starting on port {APP_PORT}...")
    print(f"{'='*60}\n")

    # Production settings via environment variables
    reload_enabled = os.environ.get('NICEGUI_RELOAD', 'true').lower() == 'true'
    show_browser = os.environ.get('NICEGUI_SHOW', 'true').lower() == 'true'

    ui.run(
        title=APP_TITLE,
        port=APP_PORT,
        reload=reload_enabled,
        show=show_browser,
        favicon='/static/favicon.ico',
        storage_secret='genizah-secret-v5',
    )
