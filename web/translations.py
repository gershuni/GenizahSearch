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
    # General UI
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

    # Search modes
    "Exact": "מדויק",
    "Variants": "וריאנטים",
    "Extended": "מורחב",
    "Maximum": "מקסימלי",
    "Fuzzy": "מטושטש",
    "Regex": "ביטוי רגולרי",

    # Search UI
    "Enter search terms": "הזן מילות חיפוש",
    "Search mode": "מצב חיפוש",
    "results found": "תוצאות נמצאו",
    "Searching...": "מחפש...",

    # Document viewer
    "View Document": "צפה במסמך",
    "Document": "מסמך",
    "Manuscript": "כתב יד",
    "Full Text": "טקסט מלא",
    "Metadata": "מטא-דאטא",
    "Shelfmark": "מספר מדף",
    "Title": "כותרת",
    "Library": "ספרייה",
    "No text available": "אין טקסט זמין",

    # Navigation
    "Home": "בית",
    "About": "אודות",
    "Help": "עזרה",

    # Status
    "Ready": "מוכן",
    "Initializing...": "מאתחל...",
    "Service not available": "השירות אינו זמין",
    "Index not found": "האינדקס לא נמצא",

    # Page header/footer
    "Genizah Search": "חיפוש גניזה",
    "Cairo Genizah Search Engine": "מנוע חיפוש לגניזת קהיר",

    # Search result card
    "View": "צפה",
    "Cross-page match": "התאמה חוצת עמודים",
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
