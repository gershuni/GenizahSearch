# -*- coding: utf-8 -*-
"""
Web application translations.

Uses a simple key-value approach where English is the key
and translations are the values. Supports RTL languages.
"""

from typing import Optional

# Current language state
_current_lang = 'he'  # Default to Hebrew


# Translation dictionary: English -> Hebrew
TRANSLATIONS = {
    # =========================================================================
    # General UI
    # =========================================================================
    "Search": "חיפוש",
    "Search...": "חיפוש...",
    "Results": "תוצאות",
    "No results found": "לא נמצאו תוצאות",
    "Loading...": "טוען...",
    "Error": "שגיאה",
    "Close": "סגור",
    "Back": "חזרה",
    "Next": "הבא",
    "Previous": "הקודם",
    "Page": "עמוד",
    "of": "מתוך",
    "Settings": "הגדרות",
    "Options": "אפשרויות",
    "Language": "שפה",
    "Hebrew": "עברית",
    "English": "אנגלית",

    # =========================================================================
    # Home Page Cards
    # =========================================================================
    "Genizah Search": "חיפוש גניזה",
    "Cairo Genizah Search Engine": "מנוע חיפוש לגניזת קהיר",
    "Pages": "עמודים",
    "Latest Data": "נתונים עדכניים",
    "Chunk Analysis": "ניתוח קטעים",
    "Scoring": "ניקוד",
    "Images": "תמונות",
    "Transcriptions": "תעתיקים",
    "Browse": "דפדוף",

    # Search Card
    "Text Search": "חיפוש טקסט",
    "Search in manuscripts": "חיפוש בכתבי יד",
    "Search for words and phrases in the Genizah corpus": "חפש מילים וביטויים בגניזה",
    "Enter search terms and find matching manuscripts": "הזן מילות חיפוש ומצא כתבי יד תואמים",

    # Parallels Card
    "Find Parallels": "מציאת מקבילות",
    "Composition Search": "חיפוש חיבורים",
    "Find similar texts": "מצא טקסטים דומים",
    "Enter a long text and find parallel texts in the Genizah": "הזן טקסט ארוך ומצא טקסטים מקבילים בגניזה",
    "Paste your text to discover parallels": "הדבק טקסט כדי לגלות מקבילות",

    # Browse Card
    "Browse Manuscripts": "דפדוף בכתבי יד",
    "Browse by shelfmark": "דפדוף לפי מספר מדף",
    "Enter a shelfmark to browse the manuscript": "הזן מספר מדף כדי לדפדף בכתב היד",
    "Navigate through manuscript pages": "נווט בין דפי כתב היד",

    # =========================================================================
    # Search Modes
    # =========================================================================
    "Exact": "מדויק",
    "Variants": "וריאנטים",
    "Extended": "מורחב",
    "Maximum": "מקסימלי",
    "Fuzzy": "מטושטש",
    "Regex": "ביטוי רגולרי",

    # =========================================================================
    # Search UI
    # =========================================================================
    "Enter search terms": "הזן מילות חיפוש",
    "Enter Hebrew text to search": "הזן טקסט עברי לחיפוש",
    "Search mode": "מצב חיפוש",
    "results found": "תוצאות נמצאו",
    "Searching...": "מחפש...",
    "Advanced options": "אפשרויות מתקדמות",
    "Word gap": "מרווח מילים",
    "Gap description": "מספר המילים המותר בין מילות החיפוש (0 = רק מילים צמודות)",
    "Start search": "התחל חיפוש",
    "Results per page": "תוצאות בעמוד",
    "Search tips": "טיפים לחיפוש",
    "Try different search mode": "נסה מצב חיפוש אחר",
    "Check spelling": "בדוק את האיות",
    "Use fewer words": "השתמש בפחות מילים",
    "Text copied": "הטקסט הועתק",
    "Copy text": "העתק טקסט",
    "Search functionality is currently unavailable": "פונקציית החיפוש אינה זמינה כרגע",

    # =========================================================================
    # Composition Search UI
    # =========================================================================
    "Enter text to search for parallels": "הזן טקסט לחיפוש מקבילות",
    "Paste text here...": "הדבק טקסט כאן...",
    "Source text": "טקסט מקור",
    "Filter text": "טקסט סינון",
    "Optional: exclude matches from this text": "אופציונלי: סנן התאמות מטקסט זה",
    "Chunk size": "גודל קטע",
    "Max frequency": "תדירות מקסימלית",
    "Find parallels": "מצא מקבילות",
    "Score": "ציון",
    "Match": "התאמה",
    "Source": "מקור",
    "Manuscript text": "טקסט כתב היד",
    "Your text": "הטקסט שלך",

    # Parallels Page - Enhanced UI
    "Discover parallel texts in the Genizah corpus": "גלה טקסטים מקבילים בגניזה",
    "Words": "מילים",
    "Characters": "תווים",
    "Paste your Hebrew text here (minimum 10 words)...": "הדבק טקסט עברי כאן (מינימום 10 מילים)...",
    "Filter text (exclude known sources)": "טקסט סינון (הסרת מקורות ידועים)",
    "Matches containing text from this field will be filtered out": "התאמות המכילות טקסט משדה זה יסוננו",
    "Paste text to exclude from results...": "הדבק טקסט להסרה מהתוצאות...",
    "Words per search chunk": "מילים לכל קטע חיפוש",
    "Skip common phrases": "דלג על ביטויים נפוצים",
    "Overlap": "חפיפה",
    "Maximum (step=1)": "מקסימלית (צעד=1)",
    "Medium (step=2)": "בינונית (צעד=2)",
    "Minimal (step=4)": "מינימלית (צעד=4)",
    "Find Parallels": "מצא מקבילות",
    "Searching for parallels...": "מחפש מקבילות...",
    "Cancel": "בטל",
    "Cancelling...": "מבטל...",
    "This may take a while for long texts...": "פעולה זו עשויה לקחת זמן לטקסטים ארוכים...",
    "Initializing search...": "מאתחל חיפוש...",
    "Search cancelled": "החיפוש בוטל",
    "No parallels found": "לא נמצאו מקבילות",
    "Try adjusting your search parameters": "נסה לשנות את הגדרות החיפוש",
    "parallels found": "מקבילות נמצאו",
    "in": "ב",
    "manuscripts": "כתבי יד",
    "Top score": "ציון גבוה ביותר",
    "Sort by": "מיין לפי",
    "Sort by score": "מיון לפי ציון",
    "Sort by shelfmark": "מיון לפי מספר מדף",
    "Sort by matches": "מיון לפי מספר התאמות",
    "Min score": "ציון מינימלי",
    "matches": "התאמות",
    "and": "ו",
    "more matches": "התאמות נוספות",
    "View manuscript": "צפה בכתב היד",
    "No context available": "אין הקשר זמין",
    "Please ensure the search index is loaded": "אנא ודא שאינדקס החיפוש נטען",

    # =========================================================================
    # Browse UI
    # =========================================================================
    "Enter shelfmark": "הזן מספר מדף",
    "e.g. T-S 8J6.1": "לדוגמה: T-S 8J6.1",
    "Go": "עבור",
    "First page": "עמוד ראשון",
    "Last page": "עמוד אחרון",
    "Jump to page": "קפוץ לעמוד",
    "Image": "תמונה",
    "Show image": "הצג תמונה",
    "Hide image": "הסתר תמונה",
    "Open in Ktiv": "פתח בכתיב",
    "External link": "קישור חיצוני",
    "pages": "עמודים",
    "Examples": "דוגמאות",
    "Folio": "דף",

    # =========================================================================
    # Image Viewer Controls
    # =========================================================================
    "Zoom": "זום",
    "Zoom in": "הגדל",
    "Zoom out": "הקטן",
    "Reset zoom": "אפס זום",
    "Fit to width": "התאם לרוחב",
    "Fit to height": "התאם לגובה",
    "Fullscreen": "מסך מלא",
    "Exit fullscreen": "צא ממסך מלא",
    "Image not available": "תמונה לא זמינה",
    "Navigate": "ניווט",

    # =========================================================================
    # Document Viewer
    # =========================================================================
    "View Document": "צפה במסמך",
    "Document": "מסמך",
    "Manuscript": "כתב יד",
    "Full Text": "טקסט מלא",
    "Metadata": "מטא-דאטא",
    "Shelfmark": "מספר מדף",
    "Title": "כותרת",
    "Library": "ספרייה",
    "No text available": "אין טקסט זמין",
    "View": "צפה",
    "Cross-page match": "התאמה חוצת עמודים",

    # =========================================================================
    # Navigation
    # =========================================================================
    "Home": "בית",
    "About": "אודות",
    "Help": "עזרה",

    # =========================================================================
    # Status Messages
    # =========================================================================
    "Ready": "מוכן",
    "Initializing...": "מאתחל...",
    "Service not available": "השירות אינו זמין",
    "Index not found": "האינדקס לא נמצא",
    "Composition search not available": "חיפוש חיבורים אינו זמין",
    "No manuscript found": "לא נמצא כתב יד",
    "Enter at least 10 words": "הזן לפחות 10 מילים",

    # =========================================================================
    # Settings
    # =========================================================================
    "Display settings": "הגדרות תצוגה",
    "Search settings": "הגדרות חיפוש",
    "Default search mode": "מצב חיפוש ברירת מחדל",
    "Results per page": "תוצאות לעמוד",
    "Theme": "ערכת נושא",
    "Light": "בהיר",
    "Dark": "כהה",

    # =========================================================================
    # Personal Lists
    # =========================================================================
    "Personal Lists": "רשימות אישיות",
    "My Lists": "הרשימות שלי",
    "Create new list": "צור רשימה חדשה",
    "Create List": "צור רשימה",
    "Create New List": "צור רשימה חדשה",
    "List Name": "שם הרשימה",
    "Please enter a list name": "אנא הזן שם לרשימה",
    "List created": "הרשימה נוצרה",
    "Delete List?": "למחוק את הרשימה?",
    "Are you sure you want to delete": "האם אתה בטוח שברצונך למחוק",
    "All items in this list will be removed.": "כל הפריטים ברשימה זו יוסרו.",
    "List deleted": "הרשימה נמחקה",
    "Delete list": "מחק רשימה",
    "Delete": "מחק",
    "Lists manager not available": "מנהל הרשימות אינו זמין",
    "No lists yet. Create your first list!": "אין רשימות עדיין. צור את הרשימה הראשונה שלך!",
    "Select a list to view its contents": "בחר רשימה כדי לצפות בתוכן שלה",
    "List not found": "הרשימה לא נמצאה",
    "System List": "רשימת מערכת",
    "This list is empty": "הרשימה הזו ריקה",
    "Add items from search results": "הוסף פריטים מתוצאות החיפוש",
    "items": "פריטים",
    "Organize and save manuscripts for easy access": "ארגן ושמור כתבי יד לגישה נוחה",
    "Color": "צבע",
    "Export": "ייצא",
    "Export functionality coming soon": "פונקציית הייצוא תגיע בקרוב",
    "Export failed": "הייצוא נכשל",

    # List Items
    "Add to List": "הוסף לרשימה",
    "Add to list": "הוסף לרשימה",
    "Item": "פריט",
    "Select List": "בחר רשימה",
    "Note (optional)": "הערה (אופציונלי)",
    "Add": "הוסף",
    "Added to list": "נוסף לרשימה",
    "Already in list": "כבר ברשימה",
    "Cannot add: missing system ID": "לא ניתן להוסיף: חסר מזהה מערכת",
    "No lists available. Create a list first.": "אין רשימות זמינות. צור רשימה תחילה.",
    "Go to Lists": "עבור לרשימות",
    "Edit Item": "ערוך פריט",
    "Edit": "ערוך",
    "Notes": "הערות",
    "Tags (comma-separated)": "תגיות (מופרדות בפסיקים)",
    "Item updated": "הפריט עודכן",
    "Remove": "הסר",
    "Item removed from list": "הפריט הוסר מהרשימה",
    "Browse": "דפדוף",

    # Search Page
    "Search Query": "מילות חיפוש",
    "Mode": "מצב",
    "Gap": "מרווח",
    "Enable Lab Mode algorithms": "הפעל אלגוריתמים של מצב מעבדה",
    "Export Word": "ייצא Word",
    "Export Excel": "ייצא Excel",
    "Advanced Filters": "סינונים מתקדמים",
    "Boolean Operators": "אופרטורים בוליאניים",
    "Shortcuts": "קיצורי דרך",
    "Engine not ready.": "המנוע אינו מוכן.",
    "Starting...": "מתחיל...",
    "Ready to search.": "מוכן לחיפוש.",
    "Select a result to view": "בחר תוצאה לצפייה",
    "Showing first 100 results. Refine search.": "מציג 100 תוצאות ראשונות. דייק את החיפוש.",
    "Browse Full Manuscript": "עיין בכתב יד מלא",
    "Dashboard": "לוח בקרה",

    # Dashboard/Home
    "Done. Found {} results.": "סיים. נמצאו {} תוצאות.",

    # Viewer
    "Showing match context only": "מציג הקשר התאמה בלבד",
    "Click \"Browse Full Manuscript\" to view the complete page with navigation": "לחץ על \"עיין בכתב יד מלא\" לצפייה בעמוד המלא עם ניווט",
    "View Complete Page": "צפה בעמוד המלא",

    # Parallels
    "Composition Search": "חיפוש חיבורים",
    "Find parallel texts in the Genizah corpus": "מצא טקסטים מקבילים בגניזה",
    "Manuscript Match": "התאמה בכתב יד",
    "Source Context": "הקשר מקור",
    "View Result": "צפה בתוצאה",
    "Quick View": "צפייה מהירה",
    "Add to Favorites": "הוסף למועדפים",
    "How does it work?": "איך זה עובד?",
    "found": "נמצאו",
}


def set_language(lang: str) -> None:
    """Set the current language ('he' for Hebrew, 'en' for English)."""
    global _current_lang
    _current_lang = lang


def get_language() -> str:
    """Get the current language code."""
    return _current_lang


def is_rtl() -> bool:
    """Check if current language is RTL."""
    return _current_lang == 'he'


def tr(text: str) -> str:
    """
    Translate text to current language.

    Args:
        text: English text to translate

    Returns:
        Translated text if available, otherwise original text
    """
    if _current_lang == 'en':
        return text

    return TRANSLATIONS.get(text, text)


def get_dir() -> str:
    """Get text direction for current language."""
    return 'rtl' if is_rtl() else 'ltr'
