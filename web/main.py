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
APP_TITLE = "Genizah Search Pro | חיפוש גניזה"
APP_VERSION = "5.0"
APP_PORT = int(os.environ.get('GENIZAH_PORT', 8081))

# Initialize API routes (Image Proxy, Export)
init_api_routes()

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

    [data-theme="dark"] .highlight-match {
        background: linear-gradient(120deg, #854d0e 0%, #a16207 100%) !important;
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
       Mobile-First CSS Variables
       ======================================================================== */

    :root {
        /* Touch targets (Apple HIG: 44px min, Material: 48dp recommended) */
        --touch-target-min: 44px;
        --touch-target-comfortable: 48px;

        /* Mobile-specific spacing */
        --spacing-mobile-xs: 4px;
        --spacing-mobile-sm: 8px;
        --spacing-mobile-md: 12px;
        --spacing-mobile-lg: 16px;

        /* Mobile font sizes */
        --font-size-mobile-xs: 0.75rem;
        --font-size-mobile-sm: 0.875rem;
        --font-size-mobile-base: 1rem;
        --font-size-mobile-lg: 1.125rem;
        --font-size-mobile-xl: 1.25rem;

        /* Mobile sidebar */
        --sidebar-width-mobile: 85vw;
        --sidebar-max-width: 280px;

        /* Header heights */
        --header-height-desktop: 64px;
        --header-height-mobile: 56px;
    }

    /* ========================================================================
       Responsive Styles - Mobile First Approach
       ======================================================================== */

    /* ===== Extra Small Devices (320px and below) ===== */
    @media (max-width: 320px) {
        .app-header {
            padding: 0 8px !important;
            height: var(--header-height-mobile) !important;
        }

        .logo-text {
            font-size: 0.95rem !important;
        }

        .logo-version {
            display: none !important;
        }

        .main-content {
            padding: 8px !important;
        }

        .page-title {
            font-size: 1.15rem !important;
        }

        .page-subtitle {
            font-size: 0.8rem !important;
        }

        .stat-card {
            padding: 10px !important;
        }

        .stat-value {
            font-size: 1.25rem !important;
        }

        .stat-label {
            font-size: 0.75rem !important;
        }

        .result-card {
            padding: 10px !important;
        }

        .nav-item {
            padding: 10px 16px !important;
            font-size: 0.9rem !important;
        }
    }

    /* ===== Small Mobile (375px) ===== */
    @media (max-width: 375px) {
        .app-header {
            padding: 0 10px !important;
        }

        .main-content {
            padding: 10px !important;
        }

        .page-title {
            font-size: 1.2rem !important;
        }

        .stat-value {
            font-size: 1.35rem !important;
        }
    }

    /* ===== Standard Mobile (480px) ===== */
    @media (max-width: 480px) {
        .app-header {
            padding: 0 12px !important;
            height: var(--header-height-mobile) !important;
        }

        .header-logo-text {
            display: none !important;
        }

        .main-content {
            padding: 12px !important;
        }

        .page-title {
            font-size: 1.25rem !important;
        }

        .page-subtitle {
            font-size: 0.85rem !important;
        }

        .stat-card {
            padding: 12px !important;
        }

        .stat-value {
            font-size: 1.5rem !important;
        }

        .stat-label {
            font-size: 0.8rem !important;
        }

        .result-card {
            padding: 12px !important;
        }

        .result-shelfmark {
            font-size: 1rem !important;
        }

        .result-snippet {
            font-size: 0.9rem !important;
            padding: 10px !important;
        }

        /* Sidebar adjustments */
        .q-drawer {
            width: var(--sidebar-width-mobile) !important;
            max-width: var(--sidebar-max-width) !important;
        }

        .nav-item {
            padding: 14px 20px !important;
            min-height: var(--touch-target-comfortable) !important;
            font-size: 1rem !important;
        }

        .nav-item-icon {
            font-size: 1.25rem !important;
        }

        .sidebar-footer {
            padding: 12px 16px !important;
        }

        .theme-switcher {
            gap: 4px !important;
        }

        .theme-btn {
            min-width: 40px !important;
            min-height: 40px !important;
        }

        /* Cards in single column */
        .options-grid {
            grid-template-columns: 1fr !important;
        }

        /* Touch-friendly buttons */
        .btn-primary, .btn-secondary {
            min-height: var(--touch-target-comfortable) !important;
            padding: 12px 20px !important;
        }

        /* Dialogs fullscreen on mobile */
        .q-dialog__inner {
            padding: 0 !important;
        }

        .q-dialog .q-card {
            border-radius: 0 !important;
            max-height: 100vh !important;
            width: 100vw !important;
            max-width: 100vw !important;
            margin: 0 !important;
        }

        .q-dialog .q-card.max-w-lg,
        .q-dialog .q-card.w-96 {
            width: 100vw !important;
            max-width: 100vw !important;
        }

        /* Dialog content scrollable */
        .q-dialog .q-card > .q-card__section {
            max-height: calc(100vh - 120px);
            overflow-y: auto;
            -webkit-overflow-scrolling: touch;
        }

        /* Dialog buttons stacked on small screens */
        .q-dialog .justify-end.gap-2 {
            flex-direction: column !important;
            gap: 8px !important;
        }

        .q-dialog .justify-end .q-btn {
            width: 100% !important;
        }

        /* Dialog titles */
        .q-dialog .text-xl,
        .q-dialog .text-lg {
            font-size: 1.125rem !important;
        }

        /* Expansion panels in dialogs */
        .q-dialog .q-expansion-item {
            margin: 0 -16px !important;
            padding: 0 16px !important;
        }
    }

    /* ===== Large Mobile / Phablet (640px) ===== */
    @media (max-width: 640px) {
        .app-header {
            padding: 0 14px;
        }

        .main-content {
            padding: 14px;
        }

        .page-title {
            font-size: 1.35rem;
        }

        .stat-value {
            font-size: 1.6rem;
        }

        /* Quick search hidden - show toggle button instead */
        .quick-search-desktop {
            display: none !important;
        }

        .quick-search-toggle {
            display: flex !important;
        }

        /* Mobile search overlay */
        .mobile-search-overlay {
            position: fixed;
            top: var(--header-height-mobile);
            left: 0;
            right: 0;
            padding: 12px 16px;
            background: var(--bg-header);
            z-index: 100;
            box-shadow: var(--shadow-lg);
            display: none;
        }

        .mobile-search-overlay.open {
            display: block;
        }
    }

    /* ===== Tablet (768px) ===== */
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

        /* Prevent iOS zoom on input focus */
        input, select, textarea {
            font-size: 16px !important;
        }

        /* Larger touch targets */
        button, .clickable, [role="button"] {
            min-height: var(--touch-target-min);
        }

        /* Dialog responsive styles at tablet */
        .q-dialog__inner {
            padding: 16px !important;
        }

        .q-dialog .q-card {
            max-width: calc(100vw - 32px) !important;
            max-height: calc(100vh - 100px) !important;
        }

        .q-dialog .q-card.max-w-lg {
            max-width: calc(100vw - 32px) !important;
        }

        .q-dialog .q-card .q-input,
        .q-dialog .q-card .q-select,
        .q-dialog .q-card .q-textarea {
            font-size: 16px !important;
        }

        .q-dialog .q-card .q-btn {
            min-height: var(--touch-target-min) !important;
            font-size: 0.9rem !important;
        }

        /* Prevent horizontal scroll */
        body {
            overflow-x: hidden;
        }

        /* Better touch scrolling */
        .scroll-area, .q-scrollarea {
            -webkit-overflow-scrolling: touch;
        }

        /* Status indicator smaller on tablet */
        .status-indicator {
            padding: 4px 12px !important;
        }

        .status-text {
            font-size: 0.7rem !important;
        }

        /* Notification toasts */
        .q-notification {
            max-width: calc(100vw - 32px) !important;
            margin: 8px !important;
            font-size: 0.9rem !important;
        }

        .q-notification__message {
            font-size: 0.875rem !important;
        }
    }

    /* ===== Small Desktop (1024px) ===== */
    @media (max-width: 1024px) {
        .main-content {
            padding: 20px;
        }

        .content-container {
            max-width: 100%;
        }
    }

    /* ========================================================================
       Accessibility - Reduced Motion
       ======================================================================== */

    @media (prefers-reduced-motion: reduce) {
        *, *::before, *::after {
            animation-duration: 0.01ms !important;
            animation-iteration-count: 1 !important;
            transition-duration: 0.01ms !important;
            scroll-behavior: auto !important;
        }

        .fade-in, .slide-in-left {
            animation: none !important;
        }

        .stat-card:hover,
        .result-card:hover,
        .q-card:hover {
            transform: none !important;
        }
    }

    /* ========================================================================
       Focus Visible States (Accessibility)
       ======================================================================== */

    *:focus-visible {
        outline: 2px solid var(--border-focus) !important;
        outline-offset: 2px !important;
    }

    button:focus-visible,
    .nav-item:focus-visible,
    a:focus-visible {
        outline: 2px solid var(--primary-500) !important;
        outline-offset: 2px !important;
    }

    /* ========================================================================
       Mobile Overlay for Sidebar
       ======================================================================== */

    .drawer-overlay {
        position: fixed;
        inset: 0;
        background: rgba(0, 0, 0, 0.5);
        z-index: 1999;
        display: none;
    }

    .drawer-overlay.visible {
        display: block;
    }

    /* ========================================================================
       Mobile-specific utility classes
       ======================================================================== */

    @media (max-width: 640px) {
        .hide-mobile {
            display: none !important;
        }
    }

    @media (min-width: 641px) {
        .show-mobile-only {
            display: none !important;
        }
    }

    @media (max-width: 768px) {
        .hide-tablet {
            display: none !important;
        }
    }

    @media (min-width: 769px) {
        .show-tablet-only {
            display: none !important;
        }
    }

    /* Full-width buttons on mobile */
    @media (max-width: 480px) {
        .mobile-full-width {
            width: 100% !important;
        }
    }

    /* Grid to single column on mobile */
    @media (max-width: 640px) {
        .mobile-single-col {
            grid-template-columns: 1fr !important;
        }
    }

    /* ========================================================================
       Bottom Action Bar (for mobile)
       ======================================================================== */

    .mobile-bottom-bar {
        display: none;
    }

    @media (max-width: 480px) {
        .mobile-bottom-bar {
            display: flex;
            position: fixed;
            bottom: 0;
            left: 0;
            right: 0;
            background: var(--bg-card);
            border-top: 1px solid var(--border-light);
            padding: 12px 16px;
            gap: 8px;
            z-index: 100;
            box-shadow: 0 -4px 6px -1px rgb(0 0 0 / 0.1);
        }

        .mobile-bottom-bar button {
            flex: 1;
            min-height: var(--touch-target-comfortable);
        }

        /* Add padding to content so it's not hidden behind bottom bar */
        .has-bottom-bar {
            padding-bottom: 80px !important;
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
       Skeleton Loading States
       ======================================================================== */

    .skeleton {
        background: var(--surface-secondary);
        border-radius: 4px;
    }

    .skeleton.animated {
        background: linear-gradient(90deg,
            var(--surface-secondary) 0%,
            var(--surface-hover) 50%,
            var(--surface-secondary) 100%);
        background-size: 200% 100%;
        animation: skeleton-pulse 1.5s ease-in-out infinite;
    }

    @keyframes skeleton-pulse {
        0% { background-position: 200% 0; }
        100% { background-position: -200% 0; }
    }

    .skeleton-text {
        height: 1em;
        margin-bottom: 0.5em;
    }

    .skeleton-text.short { width: 40%; }
    .skeleton-text.medium { width: 70%; }
    .skeleton-text.long { width: 100%; }

    .skeleton-circle {
        border-radius: 50%;
    }

    .skeleton-card {
        padding: 16px;
        border-radius: 8px;
    }

    /* Loading spinner for mobile */
    .loading-overlay {
        position: fixed;
        inset: 0;
        background: rgba(255, 255, 255, 0.9);
        display: flex;
        align-items: center;
        justify-content: center;
        z-index: 9999;
    }

    .loading-spinner {
        width: 48px;
        height: 48px;
        border: 3px solid var(--surface-secondary);
        border-top-color: var(--primary-600);
        border-radius: 50%;
    }

    .loading-spinner.active {
        animation: spin 0.8s linear infinite;
    }

    @keyframes spin {
        to { transform: rotate(360deg); }
    }

    /* Mobile loading optimizations */
    @media (max-width: 768px) {
        .skeleton-card {
            padding: 12px;
        }

        .loading-spinner {
            width: 40px;
            height: 40px;
        }

        /* Reduce motion for performance */
        @media (prefers-reduced-motion: reduce) {
            .skeleton {
                animation: none;
                background: var(--surface-secondary);
            }

            .loading-spinner {
                animation-duration: 1.2s;
            }
        }
    }

    /* Lazy loading placeholder */
    .lazy-image {
        background-color: var(--surface-secondary);
        min-height: 200px;
    }

    .lazy-image.loaded {
        min-height: auto;
        background-color: transparent;
    }

    /* ========================================================================
       Page-Specific Mobile Styles (consolidated for performance)
       ======================================================================== */

    /* === Home Page === */
    @media (max-width: 640px) {
        .hero-section { padding: 16px !important; }
        .hero-title { font-size: 1.5rem !important; }
        .hero-subtitle { font-size: 0.95rem !important; }
        .hero-content { flex-direction: column !important; }
        .mini-stats-row { width: 100% !important; justify-content: center !important; }
        .mini-stat-card { min-width: 100px !important; padding: 12px !important; }
        .tool-cards-grid { gap: 16px !important; }
        .tool-card { min-width: unset !important; width: 100% !important; flex: none !important; }
        .tool-card-header { padding: 16px !important; }
        .tool-card-content { padding: 16px !important; }
        .tool-card-icon { font-size: 2rem !important; }
        .tool-card-title { font-size: 1.125rem !important; }
        .secondary-card { min-width: unset !important; width: 100% !important; flex: none !important; }
        .recent-items-grid { gap: 12px !important; }
        .recent-item-card { min-width: 140px !important; flex: 1 1 140px !important; }
        .status-grid { gap: 16px !important; }
        .status-item { min-width: unset !important; width: 45% !important; }
    }
    @media (max-width: 480px) {
        .mini-stats-row { gap: 8px !important; }
        .mini-stat-card { min-width: 80px !important; padding: 10px !important; }
        .mini-stat-value { font-size: 1.25rem !important; }
        .status-item { width: 100% !important; }
    }

    /* === Search Page === */
    @media (max-width: 768px) {
        .search-controls { flex-direction: column !important; gap: 12px !important; }
        .search-input-wrapper { width: 100% !important; }
        .search-input-wrapper .q-input { font-size: 16px !important; }
        .search-buttons { width: 100% !important; justify-content: stretch !important; }
        .search-buttons .q-btn { flex: 1 !important; min-height: 44px !important; }
        .search-splitter { flex-direction: column !important; }
        .search-splitter > div { width: 100% !important; max-width: 100% !important; }
        .search-filters-grid { grid-template-columns: 1fr !important; gap: 12px !important; }
        .filter-select { width: 100% !important; }
        .results-header { flex-direction: column !important; align-items: flex-start !important; gap: 8px !important; }
        .result-card { padding: 12px !important; }
        .result-card-header { flex-wrap: wrap !important; gap: 8px !important; }
        .result-card-actions { width: 100% !important; justify-content: flex-end !important; }
        .result-card-actions .q-btn { min-width: 40px !important; min-height: 40px !important; }
    }
    @media (max-width: 480px) {
        .search-buttons { flex-direction: column !important; }
        .result-card { padding: 10px !important; }
        .result-snippet { font-size: 0.9rem !important; }
    }

    /* === Browse Page === */
    @media (max-width: 768px) {
        .browse-container { flex-direction: column !important; }
        .image-panel, .transcription-panel { width: 100% !important; max-height: 50vh !important; }
        .browse-toolbar { flex-wrap: wrap !important; gap: 8px !important; justify-content: center !important; }
        .browse-toolbar .q-btn { min-width: 40px !important; min-height: 40px !important; }
        .zoom-controls { position: fixed !important; bottom: 16px !important; right: 16px !important; z-index: 100 !important; }
    }
    @media (max-width: 480px) {
        .image-panel, .transcription-panel { max-height: 45vh !important; }
        .browse-nav-btn { min-width: 44px !important; min-height: 44px !important; padding: 8px !important; }
    }

    /* === Parallels Page === */
    @media (max-width: 768px) {
        .parallels-container { flex-direction: column !important; }
        .parallels-input-section { width: 100% !important; }
        .parallels-results-section { width: 100% !important; }
        .parallels-textarea { min-height: 150px !important; }
        .parallels-controls { flex-direction: column !important; gap: 12px !important; }
        .parallels-controls .q-btn { width: 100% !important; min-height: 44px !important; }
    }

    /* === Lists Page === */
    @media (max-width: 768px) {
        .lists-container { flex-direction: column !important; }
        .lists-sidebar { width: 100% !important; max-height: 200px !important; overflow-y: auto !important; }
        .lists-content { width: 100% !important; }
        .list-item { min-height: 44px !important; }
        .list-actions .q-btn { min-width: 40px !important; min-height: 40px !important; }
    }

    /* === Settings Page === */
    @media (max-width: 768px) {
        .settings-grid { grid-template-columns: 1fr !important; gap: 16px !important; }
        .settings-card { padding: 16px !important; }
        .settings-header { font-size: 1.25rem !important; }
    }
    @media (max-width: 480px) {
        .settings-card { padding: 12px !important; }
    }

    /* === Help Page === */
    @media (max-width: 768px) {
        .help-header-title { font-size: 1.75rem !important; }
        .help-card { padding: 16px !important; }
        .help-section-title { font-size: 1.125rem !important; }
        .help-mode-card { padding: 12px !important; }
    }
    @media (max-width: 480px) {
        .help-header-title { font-size: 1.5rem !important; }
        .help-card { padding: 12px !important; }
        .help-section-title { font-size: 1rem !important; }
        .help-mode-card { padding: 10px !important; }
        .help-content { font-size: 0.9rem !important; }
    }

    /* === Discoveries Page === */
    @media (max-width: 768px) {
        .discoveries-header-title { font-size: 1.75rem !important; }
        .discoveries-stats-row { flex-wrap: wrap !important; gap: 8px !important; }
        .discoveries-stats-row .q-card { min-width: calc(50% - 8px) !important; flex: 1 1 calc(50% - 8px) !important; padding: 12px !important; }
        .discoveries-stats-row .q-icon { font-size: 1.5rem !important; }
        .discoveries-stats-row .text-2xl { font-size: 1.25rem !important; }
        .discoveries-filter-bar { flex-direction: column !important; align-items: stretch !important; gap: 12px !important; }
        .discoveries-filter-bar .q-select { min-width: 100% !important; }
        .discoveries-filter-bar .q-btn { width: 100% !important; min-height: 44px !important; }
        .feed-item-card { padding: 12px !important; }
        .feed-item-header { flex-wrap: wrap !important; gap: 8px !important; }
        .feed-item-actions .q-btn { min-width: 40px !important; min-height: 40px !important; }
    }
    @media (max-width: 480px) {
        .discoveries-header-title { font-size: 1.5rem !important; }
        .discoveries-stats-row .q-card { min-width: 100% !important; flex: 1 1 100% !important; padding: 10px !important; }
        .discoveries-stats-row .text-2xl { font-size: 1.125rem !important; }
        .feed-item-card { padding: 10px !important; }
        .feed-item-content { gap: 8px !important; }
        .feed-item-title { font-size: 1rem !important; }
        .correction-diff-row { flex-direction: column !important; }
        .correction-diff-row > div { width: 100% !important; }
    }
</style>
'''

# ============================================================================
# Layout Components
# ============================================================================

def create_layout():
    """Create the main application layout with modern Header and Sidebar."""

    current_page = app.storage.user.get('current_page', '/')

    # Mobile search state
    mobile_search_visible = {'value': False}

    # Header
    with ui.header().classes('q-py-none').style('height: 64px;'):
        with ui.row().classes('w-full h-full items-center justify-between app-header'):
            # Left: Menu + Logo
            with ui.row().classes('items-center gap-2 md:gap-4'):
                # Mobile menu button
                menu_btn = ui.button(icon='menu', on_click=lambda: left_drawer.toggle())
                menu_btn.props('flat round text-color=white')
                menu_btn.classes('lg:hidden')
                menu_btn.style('min-width: 44px; min-height: 44px;')

                # Logo
                with ui.row().classes('items-center gap-2 md:gap-3 cursor-pointer').on('click', lambda: ui.navigate.to('/')):
                    ui.icon('auto_stories').classes('text-2xl md:text-3xl text-white opacity-90')
                    with ui.column().classes('gap-0 hide-mobile'):
                        ui.label('Genizah Search').classes('text-base md:text-lg font-bold text-white tracking-wide header-logo-text')
                        ui.label('Pro').classes('text-xs text-white/60 logo-version')

            # Center: Search Quick Access (hidden on mobile, shown on md+)
            with ui.row().classes('hidden md:flex items-center quick-search-desktop'):
                quick_search = ui.input(placeholder=tr('Quick search...')).classes('w-64 lg:w-80').props('dark dense outlined rounded')
                quick_search.on('keydown.enter', lambda: ui.navigate.to(f'/search?q={quick_search.value}'))

            # Right: Actions
            with ui.row().classes('items-center gap-1 md:gap-4'):
                # Mobile search toggle button (visible only on mobile)
                def toggle_mobile_search():
                    mobile_search_visible['value'] = not mobile_search_visible['value']
                    if mobile_search_visible['value']:
                        mobile_search_container.classes(add='open')
                    else:
                        mobile_search_container.classes(remove='open')

                mobile_search_btn = ui.button(icon='search', on_click=toggle_mobile_search)
                mobile_search_btn.props('flat round text-color=white')
                mobile_search_btn.classes('md:hidden quick-search-toggle')
                mobile_search_btn.style('min-width: 44px; min-height: 44px;')

                # Status Indicator (hidden on very small screens)
                with ui.row().classes('items-center gap-2 bg-white/15 px-2 md:px-4 py-1 md:py-2 rounded-full hide-mobile'):
                    status_dot = ui.element('div').classes('w-2 h-2 rounded-full bg-yellow-400')
                    status_text = ui.label(tr('Loading...')).classes('text-xs text-white/90 status-text hidden sm:block')

                    def update_status():
                        if state.is_ready():
                            status_dot.classes('bg-green-400', remove='bg-yellow-400 bg-red-400')
                            status_text.text = tr('Ready')
                        else:
                            status_dot.classes('bg-yellow-400', remove='bg-green-400')
                            status_text.text = tr('Loading...')

                    # Update once after 3 seconds (enough time for state to be ready)
                    ui.timer(3.0, update_status, once=True)

                # Auth Buttons (Login/Register or User Menu)
                from web.auth_state import create_auth_buttons
                create_auth_buttons()

                # Help Button
                help_btn = ui.button(icon='help_outline', on_click=lambda: show_help_dialog())
                help_btn.props('flat round text-color=white')
                help_btn.tooltip(tr('Help'))
                help_btn.style('min-width: 44px; min-height: 44px;')

    # Mobile Search Overlay (below header)
    mobile_search_container = ui.element('div').classes('mobile-search-overlay')
    with mobile_search_container:
        with ui.row().classes('w-full items-center gap-2'):
            mobile_quick_search = ui.input(placeholder=tr('Search manuscripts...')).classes('flex-grow').props('dark dense outlined rounded autofocus')
            mobile_quick_search.on('keydown.enter', lambda: (ui.navigate.to(f'/search?q={mobile_quick_search.value}'), toggle_mobile_search()))
            close_search_btn = ui.button(icon='close', on_click=toggle_mobile_search).props('flat round text-color=white')
            close_search_btn.style('min-width: 44px; min-height: 44px;')

    # Left Sidebar (Drawer) with mobile overlay
    drawer_overlay = ui.element('div').classes('drawer-overlay')

    def close_drawer():
        left_drawer.hide()
        drawer_overlay.classes(remove='visible')

    def toggle_drawer():
        if left_drawer.value:
            close_drawer()
        else:
            left_drawer.show()
            # Only show overlay on mobile
            ui.run_javascript('''
                if (window.innerWidth <= 1024) {
                    document.querySelector('.drawer-overlay').classList.add('visible');
                }
            ''')

    drawer_overlay.on('click', close_drawer)

    # Update menu button to use toggle function
    left_drawer = ui.left_drawer(value=True, bordered=True).classes('shadow-xl').props('width=280 breakpoint=1024')

    with left_drawer:
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

                    def nav_click(p=path):
                        ui.navigate.to(p)
                        # Close drawer on mobile after navigation
                        ui.run_javascript('''
                            if (window.innerWidth <= 1024) {
                                document.querySelector('.q-drawer').classList.remove('q-drawer--opened');
                                document.querySelector('.drawer-overlay').classList.remove('visible');
                            }
                        ''')

                    nav_item = ui.row().classes(f'nav-item {"active" if is_active else ""}')
                    nav_item.on('click', nav_click)
                    nav_item.style('min-height: var(--touch-target-comfortable);')
                    with nav_item:
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

                    def tool_click(p=path):
                        ui.navigate.to(p)
                        ui.run_javascript('''
                            if (window.innerWidth <= 1024) {
                                document.querySelector('.q-drawer').classList.remove('q-drawer--opened');
                                document.querySelector('.drawer-overlay').classList.remove('visible');
                            }
                        ''')

                    tool_item = ui.row().classes(f'nav-item {"active" if is_active else ""}')
                    tool_item.on('click', tool_click)
                    tool_item.style('min-height: var(--touch-target-comfortable);')
                    with tool_item:
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
                lang_row = ui.row().classes('w-full items-center justify-center gap-2 cursor-pointer opacity-80 hover:opacity-100')
                lang_row.on('click', toggle_lang)
                lang_row.style('min-height: var(--touch-target-min);')
                with lang_row:
                    ui.icon('translate').classes('text-lg')
                    ui.label(lang_btn_text).classes('text-sm font-medium')

                # Theme Switcher
                with ui.row().classes('theme-switcher w-full justify-center'):
                    def set_theme(theme_name):
                        app.storage.user['theme'] = theme_name
                        ui.run_javascript(f'document.body.setAttribute("data-theme", "{theme_name}")')

                    current_theme = app.storage.user.get('theme', 'light')

                    light_btn = ui.button(icon='light_mode', on_click=lambda: set_theme('light'))
                    light_btn.props('flat round').classes(f'theme-btn {"active" if current_theme == "light" else ""}')
                    light_btn.style('min-width: 40px; min-height: 40px;')

                    parchment_btn = ui.button(icon='history_edu', on_click=lambda: set_theme('parchment'))
                    parchment_btn.props('flat round').classes(f'theme-btn {"active" if current_theme == "parchment" else ""}')
                    parchment_btn.style('min-width: 40px; min-height: 40px;')

                    dark_btn = ui.button(icon='dark_mode', on_click=lambda: set_theme('dark'))
                    dark_btn.props('flat round').classes(f'theme-btn {"active" if current_theme == "dark" else ""}')
                    dark_btn.style('min-width: 40px; min-height: 40px;')

                # Version Info
                ui.label(f'v{APP_VERSION}').classes('text-xs text-center opacity-50 mt-2')

    # Content Area
    return ui.column().classes('main-content w-full items-stretch flex-grow')


def show_help_dialog():
    """Show help dialog with keyboard shortcuts and tips."""
    with ui.dialog() as dialog, ui.card().classes('p-6 max-w-lg'):
        ui.label(tr('Keyboard Shortcuts')).classes('text-xl font-bold mb-4')

        shortcuts = [
            ('/', tr('Focus search')),
            ('Ctrl+Enter', tr('Execute search')),
            ('Esc', tr('Close dialogs')),
            ('Arrow keys', tr('Navigate results')),
            ('Enter', tr('Open selected result')),
        ]

        with ui.column().classes('gap-2'):
            for key, desc in shortcuts:
                with ui.row().classes('items-center gap-4'):
                    ui.label(key).classes('font-mono bg-gray-100 px-2 py-1 rounded text-sm')
                    ui.label(desc).classes('text-gray-600')

        ui.button(tr('Close'), on_click=dialog.close).classes('mt-4 btn-primary')

    dialog.open()


# ============================================================================
# Page Routes
# ============================================================================

def apply_theme_immediately():
    """Add script to apply theme before page renders to prevent flash."""
    current_theme = app.storage.user.get('theme', 'light')
    bg_color = "#0f172a" if current_theme == "dark" else "#fffbf5" if current_theme == "parchment" else "#f8fafc"

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
            // Apply to html immediately (before DOM ready)
            document.documentElement.setAttribute("data-theme", theme);
            // Apply theme function
            var applyTheme = function() {{
                document.documentElement.setAttribute("data-theme", theme);
                if (document.body) {{
                    document.body.setAttribute("data-theme", theme);
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
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.search import create_search_page
        create_search_page(initial_query=q)

@ui.page('/parallels')
def parallels_page_route(text: str = None):
    app.storage.user['current_page'] = '/parallels'
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.parallels import create_parallels_page
        create_parallels_page(initial_text=text)

@ui.page('/browse')
def browse_page_route(sys_id: str = None, highlight: str = None, fl_id: str = None, page: int = None):
    app.storage.user['current_page'] = '/browse'
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.browse import create_browse_page
        create_browse_page(initial_sys_id=sys_id, highlight=highlight, initial_fl_id=fl_id, initial_page=page)

@ui.page('/lists')
def lists_page_route():
    app.storage.user['current_page'] = '/lists'
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.lists import create_lists_page
        create_lists_page()

@ui.page('/settings')
def settings_page_route():
    app.storage.user['current_page'] = '/settings'
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.settings import create_settings_page
        create_settings_page()

@ui.page('/help')
def help_page_route():
    app.storage.user['current_page'] = '/help'
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.help import create_help_page
        create_help_page()

@ui.page('/corrections')
async def corrections_page_route():
    app.storage.user['current_page'] = '/corrections'
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.corrections import create_corrections_page
        await create_corrections_page()

@ui.page('/discoveries')
async def discoveries_page_route():
    app.storage.user['current_page'] = '/discoveries'
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.discoveries import create_discoveries_page
        await create_discoveries_page()

@ui.page('/admin')
async def admin_page_route():
    app.storage.user['current_page'] = '/admin'
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.admin import create_admin_page
        await create_admin_page()

@ui.page('/profile')
async def profile_page_route():
    app.storage.user['current_page'] = '/profile'
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.profile import create_profile_page
        await create_profile_page()

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

    ui.run(
        title=APP_TITLE,
        port=APP_PORT,
        reload=True,
        show=True,
        favicon='📜',
        storage_secret='genizah-secret-v5',
    )
