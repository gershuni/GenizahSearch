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
    "Search mode": "מצב חיפוש",
    "results found": "תוצאות נמצאו",
    "Searching...": "מחפש...",
    "Advanced options": "אפשרויות מתקדמות",
    "Word gap": "מרווח מילים",
    "Start search": "התחל חיפוש",

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
