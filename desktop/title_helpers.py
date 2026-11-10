"""Title resolution and translation helpers for desktop UI."""

import re

from genizah_core import CURRENT_LANG, load_app_config


_title_svc_singleton = None


def _get_title_svc():
    """Get or create a cached TranslationService for title lookups."""
    global _title_svc_singleton
    if _title_svc_singleton is None:
        try:
            from shared.translation_service import TranslationService
            _title_svc_singleton = TranslationService()
        except Exception:
            return None  # Operation failed; use fallback value
    return _title_svc_singleton

def _truncate_title(text, max_chars=100):
    """Truncate long title text with ellipsis. Returns (truncated_text, tooltip_or_None)."""
    if not text:
        return text, None
    if len(text) <= max_chars:
        return text, None
    return text[:max_chars].rstrip() + "...", text

def _is_hebrew_text(text):
    """Check if text is purely/nearly Hebrew with negligible English.

    Returns True only when Latin content is negligible (<20 chars).
    Mixed text (e.g. scholarly descriptions with Hebrew quotations)
    returns False so it still gets offered for translation.
    """
    if not text:
        return False
    hebrew_count = sum(1 for c in text if '\u0590' <= c <= '\u05FF' or '\uFB1D' <= c <= '\uFB4F')
    latin_count = sum(1 for c in text if 'A' <= c <= 'Z' or 'a' <= c <= 'z')
    return hebrew_count > 0 and latin_count < 20

def _translate_hebrew_date(text):
    """Translate Hebrew-numeral dates like 'מאה ט״ו' → '15th century'.

    Returns translated string or None if the pattern is not recognized.
    Handles: מאה X, מאות X-Y, מאה X-Y, and common suffixes.
    """
    _GEMATRIA = {
        'א': 1, 'ב': 2, 'ג': 3, 'ד': 4, 'ה': 5, 'ו': 6, 'ז': 7, 'ח': 8, 'ט': 9,
        'י': 10, 'כ': 20, 'ך': 20, 'ל': 30, 'מ': 40, 'ם': 40, 'נ': 50, 'ן': 50,
        'ס': 60, 'ע': 70, 'פ': 80, 'ף': 80, 'צ': 90, 'ץ': 90,
        'ק': 100, 'ר': 200, 'ש': 300, 'ת': 400,
    }

    def _parse_heb_numeral(s):
        """Parse a Hebrew numeral string to int. E.g. ט״ו→15, י״ד→14, י→10."""
        s = s.strip().replace('״', '').replace('"', '').replace("'", '').replace('׳', '')
        total = 0
        for c in s:
            total += _GEMATRIA.get(c, 0)
        return total if total > 0 else None

    def _ordinal(n):
        if 11 <= n % 100 <= 13:
            return f"{n}th"
        return f"{n}{['th','st','nd','rd'][n % 10] if n % 10 < 4 else 'th'}"

    t = text.strip()
    # Normalize quotes
    t = t.replace('״', '"').replace('׳', "'")

    # Pattern: מאה/מאות X-Y (range of centuries)
    m = re.match(r'^מא(?:ה|ות)\s+(.+?)\s*[-–]\s*(.+?)(\s*\(.*\))?$', t)
    if m:
        a, b = _parse_heb_numeral(m.group(1)), _parse_heb_numeral(m.group(2))
        if a and b:
            suffix = ''
            if m.group(3):
                suffix = ' ' + m.group(3).strip()
            return f"{_ordinal(a)}-{_ordinal(b)} century{suffix}"

    # Pattern: מאה X (single century), possibly with suffix
    m = re.match(r'^מאה\s+(.+?)(\s*\(.*\))?$', t)
    if m:
        n = _parse_heb_numeral(m.group(1))
        if n:
            suffix = ''
            if m.group(2):
                suffix = ' ' + m.group(2).strip()
            return f"{_ordinal(n)} century{suffix}"

    return None

def _resolve_display_title(sys_id, raw_title, eng_title_marc='', show_translations=None, compact=False):
    """Resolve the display title using libraries_translations.db if available.

    compact=False (ResultDialog):
        show_translations OFF → both Hebrew and English ("he  |  en")
        show_translations ON  → language-aware: English UI → English, Hebrew UI → Hebrew
    compact=True  (search table):
        show_translations OFF → Hebrew only
        show_translations ON  → language-aware: English UI → English, Hebrew UI → Hebrew
    """
    if show_translations is None:
        show_translations = load_app_config().get('show_translations', False)
    try:
        svc = _get_title_svc()
        if svc and svc.titles_available() and sys_id:
            tt = svc.get_title_translation(sys_id)
            if tt:
                he = tt.get('hebrew_title') or ''
                en = tt.get('english_title') or ''
                en_he = tt.get('english_title_he') or ''  # EN→HE translated subtitle
                if show_translations:
                    # Language-aware: show title in UI language
                    if CURRENT_LANG == 'en':
                        if en.strip():
                            return en
                        return he or raw_title or ''
                    else:  # Hebrew UI
                        if he.strip():
                            # If Hebrew is short and EN→HE subtitle exists, append it
                            if en_he.strip() and len(he) < 15:
                                return f"{he}  |  {en_he}"
                            return he
                        if en_he.strip():
                            return en_he
                        return en or raw_title or ''
                if compact:
                    # Compact: prefer Hebrew, with EN→HE subtitle for short Hebrew
                    if he.strip():
                        if en_he.strip() and len(he) < 15:
                            return f"{he} — {en_he}"
                        return he
                    return en_he or en or raw_title or ''
                # Non-compact, translations OFF: show both
                if he.strip():
                    if en_he.strip() and len(he) < 15:
                        # Hebrew is short, show EN→HE as subtitle alongside English
                        if en.strip():
                            return f"{he} — {en_he}  |  {en}"
                        return f"{he} — {en_he}"
                    if en.strip():
                        return f"{he}  |  {en}"
                    return he
                elif en.strip():
                    return en
    except Exception:
        pass  # Translation lookup failed; continue without translation
    # Fallback: original behavior
    if (not raw_title or not raw_title.strip()) and eng_title_marc:
        return eng_title_marc
    if compact:
        if show_translations and eng_title_marc and eng_title_marc.strip():
            return eng_title_marc
        return raw_title or ''
    if show_translations and CURRENT_LANG == 'en' and eng_title_marc and eng_title_marc.strip():
        return eng_title_marc
    if eng_title_marc and eng_title_marc.strip():
        return f"{raw_title}  |  {eng_title_marc}"
    return raw_title or ''

def _set_label_with_tooltip(label, text, max_chars=100):
    """Set label text with truncation and tooltip for full text."""
    truncated, full = _truncate_title(text, max_chars)
    label.setText(truncated or '')
    label.setToolTip(full or '')
