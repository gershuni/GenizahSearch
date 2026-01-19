# -*- coding: utf-8 -*-
"""
Help Center - Genizah Search Pro

Comprehensive documentation and tutorials for researchers.
"""

from nicegui import ui
from web.translations import tr, get_language


def create_help_page():
    """Create the Help Center page."""

    # Add mobile-responsive styles
    ui.add_head_html('''
    <style>
        /* Help page mobile styles */
        @media (max-width: 768px) {
            .help-header-title {
                font-size: 1.75rem !important;
            }
            .help-card {
                padding: 16px !important;
            }
            .help-section-title {
                font-size: 1.125rem !important;
            }
            .help-mode-card {
                padding: 12px !important;
            }
        }
        @media (max-width: 480px) {
            .help-header-title {
                font-size: 1.5rem !important;
            }
            .help-card {
                padding: 12px !important;
            }
            .help-section-title {
                font-size: 1rem !important;
            }
            .help-mode-card {
                padding: 10px !important;
            }
            .help-content {
                font-size: 0.9rem !important;
            }
        }
        /* Improve markdown readability */
        .help-page p {
            line-height: 1.7;
        }
        .help-page li {
            margin-bottom: 8px;
        }
    </style>
    ''')

    lang = get_language()
    is_hebrew = lang == 'he'

    with ui.column().classes('w-full max-w-4xl mx-auto gap-6 md:gap-8 fade-in p-2 md:p-4 help-page'):

        # === Page Header ===
        with ui.column().classes('gap-2 mb-2 md:mb-4'):
            ui.label(tr('Help Center')).classes('text-2xl md:text-3xl font-bold help-header-title').style('color: var(--text-primary);')
            ui.label(tr('Learn how to use Genizah Search effectively')).classes('text-sm md:text-base').style('color: var(--text-secondary);')

        # === Quick Start ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-4'):
                ui.icon('rocket_launch').classes('text-2xl').style('color: var(--primary-600);')
                ui.label(tr('Quick Start')).classes('text-xl font-bold').style('color: var(--text-primary);')

            if is_hebrew:
                ui.markdown('''
                ### התחלה מהירה

                **Genizah Search Pro** הוא כלי מחקר מתקדם לחיפוש בגניזת קהיר עם יכולות OCR וזיהוי מקבילות.

                #### צעדים ראשונים

                1. **חיפוש טקסט**: הזן טקסט עברי בתיבת החיפוש והקש Enter
                2. **דפדוף בכתבי יד**: השתמש בעמוד "דפדוף" כדי לצפות בכתבי יד לפי מספר מדף
                3. **מציאת מקבילות**: הדבק טקסט ארוך כדי למצוא קטעים דומים בגניזה
                4. **רשימות אישיות**: צור רשימות לארגון ושמירת כתבי יד למחקר שלך

                המערכת תומכת במספר מצבי חיפוש - השתמש בתפריט הנפתח כדי לבחור את המתאים ביותר לצרכים שלך.
                ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')
            else:
                ui.markdown('''
                ### Getting Started

                **Genizah Search Pro** is an advanced research tool for searching the Cairo Genizah with OCR capabilities and parallel text detection.

                #### First Steps

                1. **Text Search**: Enter Hebrew text in the search box and press Enter
                2. **Browse Manuscripts**: Use the Browse page to view manuscripts by shelfmark
                3. **Find Parallels**: Paste a long text to find similar passages in the Genizah
                4. **Personal Lists**: Create lists to organize and save manuscripts for your research

                The system supports multiple search modes - use the dropdown to select the best one for your needs.
                ''').style('color: var(--text-secondary);')

        # === Search Modes ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-4'):
                ui.icon('search').classes('text-2xl').style('color: var(--primary-600);')
                ui.label(tr('Search Modes')).classes('text-xl font-bold').style('color: var(--text-primary);')

            if is_hebrew:
                modes = [
                    ('וריאנטים', 'חיפוש סטנדרטי עם תיקון שגיאות OCR בסיסי. מומלץ לרוב החיפושים.', 'מלך → מלך, מלכ'),
                    ('מורחב', 'יצירת וריאנטים אגרסיבית יותר. השתמש כאשר החיפוש הסטנדרטי מחזיר מעט מדי תוצאות.', 'מלכות → מלכות, מלכית, מלכת'),
                    ('מקסימלי', 'סובלנות מקסימלית לוריאנטים. השתמש לטקסטים פגומים מאוד או יוצאי דופן.', 'התאמה רחבה מאוד'),
                    ('מדויק', 'התאמת מחרוזת מילולית ללא וריאנטים. השתמש כאשר אתה יודע את האיות המדויק.', 'מחפש רק את המילה המדויקת'),
                    ('מטושטש', 'התאמה מבוססת מרחק Levenshtein. טוב לטעויות הקלדה ווריאציות קלות.', 'מוצא מילים דומות'),
                    ('ביטוי רגולרי', 'תבניות ביטויים רגולריים לשאילתות מתקדמות.', '/מל[כך]+ מוצא מלך, מלכים וכו\''),
                    ('מספר מדף', 'חיפוש לפי מספר מדף/מספר קריאה של כתב היד.', 'T-S 8J6.1'),
                    ('כותרת', 'חיפוש בכותרות של כתבי היד.', 'סידור'),
                ]
            else:
                modes = [
                    ('Variants', 'Standard search with basic OCR error correction. Best for most searches.', 'מלך → מלך, מלכ'),
                    ('Extended', 'More aggressive variant generation. Use when standard search returns too few results.', 'מלכות → מלכות, מלכית, מלכת'),
                    ('Maximum', 'Maximum variant tolerance. Use for heavily damaged or unusual texts.', 'Very broad matching'),
                    ('Exact', 'Literal string matching with no variants. Use when you know the exact spelling.', 'Matches only exact text'),
                    ('Fuzzy', 'Levenshtein distance-based matching. Good for typos and minor variations.', 'Finds similar words'),
                    ('Regex', 'Regular expression patterns for advanced queries.', '/מל[כך]+ matches מלך, מלכים, etc.'),
                    ('Shelfmark', 'Search by manuscript shelfmark/call number.', 'T-S 8J6.1'),
                    ('Title', 'Search within manuscript titles.', 'prayer book'),
                ]

            with ui.column().classes('gap-4'):
                for title, desc, example in modes:
                    with ui.card().classes('p-4').style('background: var(--bg-tertiary);'):
                        ui.label(title).classes('font-bold').style('color: var(--primary-700);')
                        label_text = desc
                        if is_hebrew:
                            ui.label(label_text).style('color: var(--text-secondary); direction: rtl; text-align: right;')
                        else:
                            ui.label(label_text).style('color: var(--text-secondary);')
                        with ui.row().classes('items-center gap-2 mt-2'):
                            ui.icon('code').classes('text-sm').style('color: var(--text-muted);')
                            if is_hebrew:
                                ui.label(example).classes('font-mono text-sm').style('color: var(--text-muted); direction: rtl;')
                            else:
                                ui.label(example).classes('font-mono text-sm').style('color: var(--text-muted);')

        # === Browse Manuscripts ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-4'):
                ui.icon('menu_book').classes('text-2xl').style('color: var(--primary-600);')
                ui.label(tr('Browse Manuscripts')).classes('text-xl font-bold').style('color: var(--text-primary);')

            if is_hebrew:
                ui.markdown('''
                ### דפדוף בכתבי יד

                צפה בכתבי יד בהקשר מלא עם תמונות ותעתיקים.

                #### תכונות

                - **ניווט בדפים**: עבור בין דפי כתב היד עם חצים או מספרי דף
                - **הצגת תמונות**: לחץ על סמל התמונה כדי לראות את סריקת הכתב היד המקורי
                - **מטא-דאטא**: לחץ על "הצג מטא-דאטא" לפרטים מלאים וקישורים חיצוניים
                - **כתב יד מלא**: לחץ על "הצג כתב יד מלא" לראות את כל הדפים במסמך אחד
                - **חיפוש מקבילות**: לחץ על "חפש מקבילות" כדי למצוא טקסטים דומים בגניזה
                - **ייצוא**: ייצא את הטקסט ל-Word לניתוח נוסף
                - **הוסף לרשימות**: השתמש בכוכב כדי להוסיף כתבי יד או דפים ספציפיים לרשימות שלך

                #### טיפים

                - השתמש בקיצורי המקלדת: חצים ← → לניווט, +/- לזום
                - התחל עם V0.8 (ברירת המחדל) לתעתיקים מדויקים יותר
                - תמיד בדוק מול תמונות הכתב היד כאשר אפשרי
                ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')
            else:
                ui.markdown('''
                ### Browse Manuscripts

                View manuscripts in full context with images and transcriptions.

                #### Features

                - **Page Navigation**: Move between manuscript pages with arrows or page numbers
                - **Image Display**: Click the image icon to see the original manuscript scan
                - **Metadata**: Click "Show Metadata" for full details and external links
                - **Full Manuscript**: Click "Show Full Manuscript" to view all pages in one document
                - **Search Parallels**: Click "Search for Parallels" to find similar texts in the Genizah
                - **Export**: Export text to Word for further analysis
                - **Add to Lists**: Use the star to add manuscripts or specific pages to your lists

                #### Tips

                - Use keyboard shortcuts: Arrow keys for navigation, +/- for zoom
                - Start with V0.8 (default) for more accurate transcriptions
                - Always verify readings against manuscript images when possible
                ''').style('color: var(--text-secondary);')

        # === Parallels Search ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-4'):
                ui.icon('compare_arrows').classes('text-2xl').style('color: var(--accent-amber);')
                ui.label(tr('Parallels Search')).classes('text-xl font-bold').style('color: var(--text-primary);')

            if is_hebrew:
                ui.markdown('''
                ### מציאת טקסטים מקבילים

                חיפוש המקבילות (חיפוש חיבורים) עוזר לך למצוא טקסטים בגניזה הדומים לטקסט מקור שאתה מספק.

                #### איך להשתמש

                1. **הדבק את טקסט המקור שלך** (מינימום 3 מילים)
                2. **התאם גודל קטע** - כמה מילים לחפש בבת אחת:
                   - קטעים קטנים יותר (3-4) = מדויק יותר אך איטי יותר
                   - קטעים גדולים יותר (6-8) = מהיר יותר אך עלול להחמיץ מקבילות קצרות
                3. **בחר מצב** - וריאנטים בדרך כלל הטוב ביותר
                4. **לחץ על "מצא מקבילות"**

                #### הבנת תוצאות

                התוצאות מקובצות לפי כתב יד ומציגות:
                - **ציון**: עד כמה הטקסט תואם
                - **הקשר מקור**: הטקסט המקורי שלך שהתאים
                - **התאמה בכתב היד**: הטקסט המתאים בגניזה

                #### טיפים

                - התחל עם גודל קטע 4-5 לרוב הטקסטים
                - השתמש באפשרות הסינון כדי להחריג מקורות ידועים
                - ציונים גבוהים (>80) בדרך כלל מצביעים על מקבילות ישירות
                - השתמש ב"סריקה מעמיקה" לתוצאות מקיפות יותר (איטי יותר)
                - ייצא תוצאות ל-Word או Excel לניתוח נוסף
                ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')
            else:
                ui.markdown('''
                ### Finding Parallel Texts

                The Parallels (Composition) Search helps you find texts in the Genizah that are similar to a source text you provide.

                #### How to Use

                1. **Paste your source text** (minimum 3 words)
                2. **Adjust chunk size** - how many words to search at once:
                   - Smaller chunks (3-4) = more precise but slower
                   - Larger chunks (6-8) = faster but may miss short parallels
                3. **Choose mode** - variants is usually best
                4. **Click "Find Parallels"**

                #### Understanding Results

                Results are grouped by manuscript and show:
                - **Score**: How well the text matches
                - **Source Context**: Your original text that matched
                - **Manuscript Match**: The corresponding text in the Genizah

                #### Tips

                - Start with chunk size 4-5 for most texts
                - Use the filter option to exclude known sources
                - High scores (>80) usually indicate direct parallels
                - Enable "Deep Scan" for more comprehensive results (slower)
                - Export results to Word or Excel for further analysis
                ''').style('color: var(--text-secondary);')

        # === Personal Lists ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-4'):
                ui.icon('star').classes('text-2xl').style('color: var(--accent-amber);')
                ui.label(tr('Personal Lists')).classes('text-xl font-bold').style('color: var(--text-primary);')

            if is_hebrew:
                ui.markdown('''
                ### ארגון המחקר שלך

                רשימות אישיות עוזרות לך לשמור ולארגן כתבי יד עבור פרויקטי המחקר שלך.

                #### תכונות

                - **צור רשימות** עם שמות וצבעים מותאמים אישית
                - **הוסף כתבי יד** מתוצאות חיפוש או מעמוד הדפדוף
                - **הוסף דפים ספציפיים** לשמירת מיקום מדויק
                - **הוסף הערות** לכל פריט עבור ההערות שלך
                - **ייצא רשימות** ל-Excel לניתוח נוסף
                - **צבעי רשימות** לארגון חזותי

                #### טיפים

                - השתמש ברשימות כדי לקבץ כתבי יד לפי נושא מחקר
                - סמל הכוכב בתוצאות החיפוש מוסיף פריטים במהירות למועדפים שלך
                - פריטים שנצפו לאחרונה נעקבים אוטומטית ברשימה "נצפו לאחרונה"
                - השתמש בהערות כדי לתעד ממצאים חשובים לכל כתב יד
                ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')
            else:
                ui.markdown('''
                ### Organizing Your Research

                Personal Lists help you save and organize manuscripts for your research projects.

                #### Features

                - **Create lists** with custom names and colors
                - **Add manuscripts** from search results or the browse page
                - **Add specific pages** to save exact locations
                - **Add notes** to each item for your own annotations
                - **Export lists** to Excel for further analysis
                - **List colors** for visual organization

                #### Tips

                - Use lists to group manuscripts by research topic
                - The star icon in search results quickly adds items to your favorites
                - Recent items are automatically tracked in the "Recently Viewed" list
                - Use notes to document important findings for each manuscript
                ''').style('color: var(--text-secondary);')

        # === Export Functions ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-4'):
                ui.icon('download').classes('text-2xl').style('color: var(--primary-600);')
                ui.label(tr('Export')).classes('text-xl font-bold').style('color: var(--text-primary);')

            if is_hebrew:
                ui.markdown('''
                ### ייצוא נתונים

                ייצא את תוצאות המחקר שלך לפורמטים שונים לניתוח נוסף.

                #### אפשרויות ייצוא

                - **חיפוש טקסט**: ייצא תוצאות חיפוש ל-Word או Excel
                - **מקבילות**: ייצא תוצאות מקבילות עם ציונים והקשר
                - **דפדוף**: ייצא דף בודד או כתב יד מלא ל-Word
                - **רשימות**: ייצא את הרשימות שלך ל-Excel

                #### טיפים

                - קובצי Excel כוללים מטא-דאטא מלא וקל לסינון
                - קובצי Word שומרים על עיצוב טקסט עבור קריאה
                - השתמש בייצוא רשימות כדי לשתף מחקר עם עמיתים
                ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')
            else:
                ui.markdown('''
                ### Export Data

                Export your research results to various formats for further analysis.

                #### Export Options

                - **Text Search**: Export search results to Word or Excel
                - **Parallels**: Export parallel results with scores and context
                - **Browse**: Export single page or full manuscript to Word
                - **Lists**: Export your lists to Excel

                #### Tips

                - Excel files include full metadata and are easy to filter
                - Word files preserve text formatting for reading
                - Use list exports to share research with colleagues
                ''').style('color: var(--text-secondary);')

        # === Data Sources ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-4'):
                ui.icon('source').classes('text-2xl').style('color: var(--text-muted);')
                ui.label(tr('Data Sources')).classes('text-xl font-bold').style('color: var(--text-primary);')

            if is_hebrew:
                ui.markdown('''
                ### אודות הנתונים

                **תעתיקים**: תעתיקי OCR שנוצרו במחשב מפרויקט MiDRASH (פרויקט הגניזה של פרידברג). גרסאות זמינות:
                - **V0.8** - OCR האחרון עם דיוק משופר
                - **V0.7** - תעתיקים מדור קודם

                **תמונות כתבי יד**: תמונות IIIF מ:
                - הספרייה הלאומית של ישראל (NLI)
                - ספריית אוניברסיטת קיימברידג'
                - ספריית בודליאן באוקספורד

                **מטא-דאטא**: קטלוגי ספריות ונתונים קודיקולוגיים

                #### הערות חשובות

                - תעתיקי OCR עשויים להכיל שגיאות, במיוחד עבור כתבי יד פגומים
                - השתמש במצבי חיפוש וריאנטים כדי להתמודד עם שגיאות OCR
                - תמיד אמת קריאות מול תמונות כתב היד כאשר אפשרי
                - תעתיקים נוצרו אוטומטית ועשויים לדרוש אימות ידני
                ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')
            else:
                ui.markdown('''
                ### About the Data

                **Transcriptions**: Machine-generated OCR transcriptions from the MiDRASH Project (Friedberg Genizah Project). Available versions:
                - **V0.8** - Latest OCR with improved accuracy
                - **V0.7** - Legacy transcriptions

                **Manuscript Images**: IIIF images from:
                - National Library of Israel (NLI)
                - Cambridge University Library
                - Oxford Bodleian Library

                **Metadata**: Library catalogs and codicological data

                #### Important Notes

                - OCR transcriptions may contain errors, especially for damaged manuscripts
                - Use variant search modes to account for OCR errors
                - Always verify readings against manuscript images when possible
                - Transcriptions are automatically generated and may require manual verification
                ''').style('color: var(--text-secondary);')

        # === Keyboard Shortcuts ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-4'):
                ui.icon('keyboard').classes('text-2xl').style('color: var(--text-muted);')
                ui.label(tr('Keyboard Shortcuts')).classes('text-xl font-bold').style('color: var(--text-primary);')

            if is_hebrew:
                shortcuts = [
                    ('/', 'מיקוד על שדה החיפוש'),
                    ('Ctrl + Enter', 'הפעל חיפוש'),
                    ('Escape', 'סגור דיאלוגים'),
                    ('חץ שמאל/ימין', 'נווט בין דפים (בדפדוף)'),
                    ('+/-', 'הגדל/הקטן זום (בדפדוף)'),
                    ('F', 'מסך מלא (בדפדוף)'),
                ]
            else:
                shortcuts = [
                    ('/', 'Focus search input'),
                    ('Ctrl + Enter', 'Execute search'),
                    ('Escape', 'Close dialogs'),
                    ('Arrow Left/Right', 'Navigate pages (in Browse)'),
                    ('+/-', 'Zoom in/out (in Browse)'),
                    ('F', 'Toggle fullscreen (in Browse)'),
                ]

            with ui.column().classes('gap-3'):
                for key, action in shortcuts:
                    with ui.row().classes('items-center gap-4'):
                        ui.label(key).classes('font-mono px-3 py-1 rounded').style(
                            'background: var(--bg-tertiary); color: var(--text-primary); min-width: 150px;'
                        )
                        if is_hebrew:
                            ui.label(action).style('color: var(--text-secondary); direction: rtl;')
                        else:
                            ui.label(action).style('color: var(--text-secondary);')

        # === Contact ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('items-center gap-3 mb-4'):
                ui.icon('feedback').classes('text-2xl').style('color: var(--primary-600);')
                ui.label(tr('Feedback')).classes('text-xl font-bold').style('color: var(--text-primary);')

            if is_hebrew:
                ui.markdown('''
                יש לך שאלות או הצעות? נשמח לשמוע ממך!

                - **דווח על בעיות**: דווח על באגים או בעיות טכניות
                - **תיעוד**: בדוק את מרכז העזרה הזה למדריכים מפורטים

                המשוב שלך עוזר לנו לשפר את Genizah Search עבור קהילת המחקר.
                ''', extras=['rtl']).style('color: var(--text-secondary); direction: rtl; text-align: right;')
            else:
                ui.markdown('''
                Have questions or suggestions? We'd love to hear from you!

                - **Report issues**: Report bugs or technical problems
                - **Documentation**: Check this Help Center for detailed guides

                Your feedback helps us improve Genizah Search for the research community.
                ''').style('color: var(--text-secondary);')
