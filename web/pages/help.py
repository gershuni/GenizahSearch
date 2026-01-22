# -*- coding: utf-8 -*-
"""
Help Center - Genizah Search Pro

Comprehensive documentation and tutorials for researchers.
"""

from nicegui import ui
from web.translations import tr, get_language
from web.components.typography import h1, h2, h3


def create_help_page():
    """Create the Help Center page."""

    lang = get_language()
    is_hebrew = lang == 'he'

    with ui.column().classes('w-full max-w-4xl mx-auto gap-6 fade-in p-4'):

        # === Page Header ===
        with ui.column().classes('gap-2 mb-2'):
            h1(tr('Help Center'), classes='text-3xl font-bold', style='color: var(--text-primary);')
            ui.label(tr('Learn how to use Genizah Search effectively')).style('color: var(--text-secondary);')

        # === Quick Start ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-4'):
                ui.icon('rocket_launch').classes('text-2xl text-primary')
                h2(tr('Quick Start'), classes='text-xl font-bold', style='color: var(--text-primary);')

            if is_hebrew:
                ui.markdown('''
                1. **חיפוש טקסט**: הזן מילים בתיבת החיפוש. השתמש ב"וריאנטים" לתוצאות גמישות.
                2. **דפדוף**: צפה בכתבי יד לפי מספר מדף (למשל T-S 10J5.1).
                3. **מקבילות**: הדבק טקסט ארוך כדי למצוא קטעים דומים בגניזה.
                4. **רשימות**: שמור כתבי יד חשובים ברשימות אישיות ע"י לחיצה על הכוכב.
                ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')
            else:
                ui.markdown('''
                1. **Search**: Enter terms in the search box. Use "Variants" mode for flexible matching.
                2. **Browse**: View manuscripts by shelfmark (e.g., T-S 10J5.1).
                3. **Parallels**: Paste long text to find similar passages in the corpus.
                4. **Lists**: Save important manuscripts to personal lists by clicking the star icon.
                ''').style('color: var(--text-secondary);')

        # === Search Modes ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-4'):
                ui.icon('search').classes('text-2xl text-primary')
                h2(tr('Search Modes'), classes='text-xl font-bold', style='color: var(--text-primary);')

            if is_hebrew:
                modes = [
                    ('וריאנטים', 'מומלץ. מוצא מילים גם עם שינויי כתיב קלים (מלך/מלכ).'),
                    ('מורחב', 'גמיש יותר. השתמש אם לא מצאת תוצאות בחיפוש הרגיל.'),
                    ('מדויק', 'מוצא רק את המחרוזת המדויקת שהזנת.'),
                    ('מספר מדף', 'חיפוש לפי מספר קריאה (T-S...).'),
                ]
            else:
                modes = [
                    ('Variants', 'Recommended. Finds words with spelling variations.'),
                    ('Extended', 'More flexible. Use if standard search returns too few results.'),
                    ('Exact', 'Finds only the exact string you entered.'),
                    ('Shelfmark', 'Search by call number (T-S...).'),
                ]

            with ui.grid(columns=2).classes('w-full gap-4'):
                for title, desc in modes:
                    with ui.column().classes('p-3 rounded bg-gray-50 dark:bg-gray-800'):
                        h3(title, classes='font-bold text-base text-primary')
                        ui.label(desc).classes('text-sm text-gray-600 dark:text-gray-300')

        # === Browse Manuscripts ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-4'):
                ui.icon('menu_book').classes('text-2xl text-primary')
                h2(tr('Browse Manuscripts'), classes='text-xl font-bold', style='color: var(--text-primary);')

            if is_hebrew:
                ui.markdown('''
                - **תמונות**: לחץ על סמל התמונה כדי לראות את כתב היד המקורי.
                - **ניווט**: השתמש בחצים או במספרי העמודים.
                - **כתב יד מלא**: צפה בכל דפי כתב היד ברצף אחד.
                ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')
            else:
                ui.markdown('''
                - **Images**: Click the image icon to view the original manuscript.
                - **Navigation**: Use arrows or page numbers to move through pages.
                - **Full View**: View all pages of the manuscript in a single scrollable view.
                ''').style('color: var(--text-secondary);')

        # === Contact ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-4'):
                ui.icon('mail').classes('text-2xl text-primary')
                h2(tr('Feedback'), classes='text-xl font-bold', style='color: var(--text-primary);')

            ui.label('gershuni@gmail.com').classes('text-lg font-mono')
