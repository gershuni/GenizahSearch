"""
Hebrew translations for PGP (Princeton Geniza Project) tags.

Used in both web and desktop apps to display tags in Hebrew alongside
the original English tag (which is used for search).

The CATEGORIZED_TAGS structure defines both the category grouping and
the display order for tag dropdowns. Categories have English and Hebrew names.

*** TO EDIT CATEGORY ASSIGNMENTS: ***
Edit the CATEGORIZED_TAGS list below. Each entry is:
    ("English Category Name", "Hebrew Category Name", [
        ("english_tag", "hebrew_translation"),
        ...
    ])
- To move a tag: cut it from one category's list and paste into another.
- To add a new tag: add a ("english_tag", "hebrew_translation") tuple.
- Tags can appear in multiple categories (will show under each).
- Tags not in any category auto-appear under "Other" at the bottom.
- The PGP_TAG_TRANSLATIONS dict is auto-built from this structure.
"""

from typing import List, Tuple, Optional


# Each category: (english_name, hebrew_name, [(english_tag, hebrew_translation), ...])
CATEGORIZED_TAGS: List[Tuple[str, str, List[Tuple[str, str]]]] = [
    # --- סוגי מסמכים / Document Types ---
    ("Document Types", "סוגי מסמכים", [
        ("account", "חשבון"),
        ("addendum", "נספח"),
        ("Appeal", "פנייה/בקשת סיוע"),
        ("bill of sale", "שטר מכר"),
        ("commercial receipt", "קבלה מסחרית"),
        ("court record", "מעשה בית דין"),
        ("Decree", "צו שלטוני"),
        ("draft", "טיוטה"),
        ("Fatimid decree", "צו פטימי"),
        ("Formulary", "נוסחאי"),
        ("idbara", "אדבארה (צרור מכתבים)"),
        ("informal note", "פתקה"),
        ("internal memorandum", "תזכיר פנימי"),
        ("ledger", "פנקס"),
        ("legal", "משפטי"),
        ("legal query", "שאלה משפטית"),
        ("list", "רשימה"),
        ("LOR", "מכתב המלצה"),
        ("memorandum", "תזכיר"),
        ("order", "הזמנה"),
        ("Petition", "עתירה"),
        ("register", "פנקס רישום"),
        ("Report", "דיווח/דו\"ח"),
        ("rescript", "תשובה שלטונית"),
        ("Responsum", "תשובה הלכתית"),
        ("responsa", "שו\"ת"),
        ("tax receipt", "קבלת מס"),
        ("testimony", "עדות"),
    ]),

    # --- הלכה וחברה / Law & Society ---
    ("Law & Society", "הלכה וחברה", [
        ("betrothal", "אירוסין"),
        ("captives", "שבויים"),
        ("charity", "צדקה"),
        ("circumcision", "מילה"),
        ("conversion", "גיור"),
        ("customs", "מכס"),
        ("debt", "חוב"),
        ("disability", "מוגבלות"),
        ("disability: vision", "לקות ראייה"),
        ("dispute", "סכסוך"),
        ("divorce", "גירושין"),
        ("excommunication", "נידוי וחרם"),
        ("fiscal", "פיסקלי"),
        ("get shihrur", "גט שחרור"),
        ("heqdesh", "הקדש"),
        ("hikr", "חִכְּר (חכירה)"),
        ("Jariya", "ג'אריה (שפחה)"),
        ("Karaite-Rabbanite marriage", "נישואי קראים-רבנים"),
        ("ketubba", "כתובה"),
        ("kosher", "כשרות"),
        ("Levirate marriage", "ייבום"),
        ("maks", "מכס (בלתי חוקי)"),
        ("manumission", "שחרור עבד"),
        ("marital", "ענייני אישות"),
        ("marital dispute", "סכסוך אישות"),
        ("Marriage", "נישואין"),
        ("Muslim marriage contract", "חוזה נישואין מוסלמי"),
        ("poverty", "עוני"),
        ("qodesh", "קודש"),
        ("real estate", "מקרקעין"),
        ("Shehita", "שחיטה"),
        ("slave", "עבד/שפחה"),
        ("taqawi", "תקאווי"),
        ("tax", "מס"),
        ("waqf", "וקף (הקדש)"),
    ]),

    # --- רפואה ומחלה / Medicine & Illness ---
    ("Medicine & Illness", "רפואה ומחלה", [
        ("disability", "מוגבלות"),
        ("disability: vision", "לקות ראייה"),
        ("epidemic", "מגפה"),
        ("epidemic: waba'", "דבר (ובאא)"),
        ("Illness", "מחלה"),
        ("illness letter 969-1517", "מכתב מחלה (969–1517)"),
        ("illness: appeal", "פנייה בענייני מחלה"),
        ("illness: death", "מחלה ומוות"),
        ("illness: du'f", "חולשה (דעף)"),
        ("illness: excuse", "התנצלות עקב מחלה"),
        ("illness: eye", "מחלות עיניים"),
        ("illness: gastrointestinal", "מחלות דרכי העיכול"),
        ("illness: joints", "מחלות מפרקים"),
        ("illness: letter norms", "מוסכמות כתיבה על מחלות"),
        ("illness: marad", "מחלה (מרד)"),
        ("illness: mental effects", "השפעות נפשיות של מחלה"),
        ("illness: non-naturals", "הלא-טבעיים (מושג רפואי)"),
        ("illness: pediatric", "רפואת ילדים"),
        ("illness: poverty", "מחלה ועוני"),
        ("illness: ramad", "דלקת עיניים (רמד)"),
        ("illness: rich description", "תיאור מחלה מפורט"),
        ("Illness: women's", "מחלות נשים"),
        ("Materia medica", "מטריה מדיקה"),
        ("medical", "רפואי"),
        ("medical charity", "צדקה לצרכים רפואיים"),
        ("medicine", "רפואה"),
        ("physician", "רופא"),
        ("physicians", "רופאים"),
        ("prescription", "מרשם"),
        ("tadhkira", "תד'כירה"),
    ]),

    # --- מסחר ונסיעות / Trade & Travel ---
    ("Trade & Travel", "מסחר ונסיעות", [
        ("book trade", "סחר ספרים"),
        ("Byzantine merchants", "סוחרים ביזנטים"),
        ("daman", "דמאן (חכירת מסים)"),
        ("damin", "דאמין (ערב מס)"),
        ("funduq", "פונדק/מחסן (Funduq)"),
        ("movement of ships", "תנועת אוניות"),
        ("Nile voyage", "הפלגה בנילוס"),
        ("suftaja", "סופתג'ה (שטר חליפין)"),
        ("trade", "מסחר"),
        ("travel", "מסע/נסיעה"),
    ]),

    # --- ספר הודו / India Book ---
    ("India Book", "ספר הודו", [
        ("halfon-addenda", "תוספות לחלפון"),
        ("IB1", "ספר הודו א"),
        ("IB2", "ספר הודו ב"),
        ("IB3", "ספר הודו ג"),
        ("IB4", "ספר הודו ד"),
        ("IB5", "ספר הודו ה"),
        ("IB6", "ספר הודו ו"),
        ("IB7", "ספר הודו ז"),
        ("ib-addenda", "תוספות לספר הודו"),
        ("ib-partial translation-eng", "ספר הודו - תרגום חלקי לאנגלית"),
        ("ib-untranslated-eng", "ספר הודו - ללא תרגום לאנגלית"),
        ("India Book unedited", "ספר הודו (טיוטה לא ערוכה)"),
    ]),

    # --- שלטון ופוליטיקה / State & Politics ---
    ("State & Politics", "שלטון ופוליטיקה", [
        ("al-Ustul", "אלאסטול (הצי)"),
        ("Amir", "אמיר"),
        ("Arabic crusades", "מסעי הצלב (מקורות ערביים)"),
        ("arabic military report", "דיווח צבאי בערבית"),
        ("Byzantines", "ביזנטים"),
        ("Crusades", "מסעי הצלב"),
        ("DIMME", "ד'ימה (בני חסות)"),
        ("Diwan al-Abwab", "דיוואן אל-אבואב"),
        ("diwan al-amwal", "דיוואן אל-אמוואל"),
        ("isfahsalar", "אספהסלאר (מפקד צבאי)"),
        ("jihad", "ג'יהאד"),
        ("State", "המדינה/הממשל"),
        ("titulature", "תארים"),
        ("war", "מלחמה"),
    ]),

    # --- קהילה ומוסדות / Community ---
    ("Community", "קהילה ומוסדות", [
        ("clandestine", "חשאי"),
        ("communal", "קהילתי"),
        ("donors", "תורמים"),
        ("Jewish community", "הקהילה היהודית"),
        ("personal", "אישי"),
        ("petitioning", "הגשת עתירות"),
        ("place of prayer", "מקום תפילה"),
        ("recipients", "נמענים/נתמכים"),
        ("scribes", "סופרים/מעתיקים"),
    ]),

    # --- חיי יום-יום / Daily Life ---
    ("Daily Life", "חיי יום-יום", [
        ("agriculture", "חקלאות"),
        ("bread", "לחם"),
        ("canal", "תעלה"),
        ("clothing", "לבוש וטקסטיל"),
        ("food", "מזון"),
        ("gratitude", "הכרת תודה"),
        ("humor", "הומור"),
        ("love", "אהבה"),
        ("mint", "מטבעה"),
        ("oil", "שמן"),
        ("porcelain", "חרסינה"),
        ("sini", "סיני"),
        ("syrup", "סירופ/שראב"),
        ("thanks", "תודה"),
        ("vessels", "כלים"),
    ]),

    # --- ספרות ושירה / Literature ---
    ("Literature", "ספרות ושירה", [
        ("Aleppo codex", "כתר ארם צובא"),
        ("arabic bible", "תרגומי מקרא לערבית"),
        ("Arabic poetry", "שירה ערבית"),
        ("judaeo-arabic poetry", "שירה בערבית-יהודית"),
        ("literary with documentary value", "טקסט ספרותי בעל ערך תיעודי"),
        ("poem", "שיר/פיוט"),
        ("popular literature", "ספרות עממית"),
        ("Quran", "קוראן"),
    ]),

    # --- מדע ותורת הנסתר / Science & Occult ---
    ("Science & Occult", "מדע ותורת הנסתר", [
        ("alchemy", "אלכימיה"),
        ("Astronomical", "אסטרונומי"),
        ("astronomy", "אסטרונומיה"),
        ("calendar", "לוח שנה"),
        ("divination", "ניחוש"),
        ("magic", "מאגיה וכישוף"),
        ("mantiq", "מנטיק (לוגיקה)"),
        ("numismatic dating", "תיארוך נומיסמטי"),
        ("occult", "תורת הנסתר"),
        ("prognostication", "חיזוי"),
        ("qiyas", "קיאס (היקש)"),
        ("tonguetying", "קשירת לשון"),
    ]),

    # --- שפות וכתב / Languages & Script ---
    ("Languages & Script", "שפות וכתב", [
        ("Arabic", "ערבית"),
        ("Arabic reused", "שימוש משני בערבית"),
        ("Arabic script", "כתב ערבי"),
        ("bialphabetic", "דו-אלפביתי"),
        ("Judaeo-Persian", "פרסית-יהודית"),
        ("Judaeo-Persian literary", "ספרות פרסית-יהודית"),
        ("Ladino", "לדינו"),
        ("Late JA", "ערבית-יהודית מאוחרת"),
        ("Persian", "פרסית"),
    ]),

    # --- חומרים וקודיקולוגיה / Materials & Codicology ---
    ("Materials & Codicology", "חומרים וקודיקולוגיה", [
        ("holes", "חורים"),
        ("mastara", "מסטרה"),
        ("micrography", "מיקרוגרפיה"),
        ("paper", "נייר"),
        ("parchment", "קלף"),
        ("pinholes", "חורי סיכה"),
        ("printed", "דפוס"),
        ("red and black border", "מסגרת אדומה ושחורה"),
        ("ruling board", "לוח שרטוט"),
        ("Trial of the pen", "ניסוי קולמוס"),
        ("water lily", "נימפאה"),
        ("writing materials", "חומרי כתיבה"),
    ]),

    # --- תקופות / Periods ---
    ("Periods", "תקופות", [
        ("10th or 11th c", "המאה ה-10 או ה-11"),
        ("11th c", "המאה ה-11"),
        ("11th or 12th c", "המאה ה-11 או ה-12"),
        ("12th c", "המאה ה-12"),
        ("13th c", "המאה ה-13"),
        ("16th c or later", "המאה ה-16 ואילך"),
        ("16th or 17th c", "המאה ה-16 או ה-17"),
        ("early modern", "ראשית העת החדשה"),
        ("Fatimid", "פטימי"),
        ("late Fatimid", "פטימי מאוחר"),
        ("Mamluk", "ממלוכי"),
        ("ottoman era", "התקופה העות'מאנית"),
        ("Qajar", "קאג'ארי"),
    ]),

    # --- מקומות / Places ---
    ("Places", "מקומות", [
        ("Akhmim", "אחמים"),
        ("Alexandria", "אלכסנדריה"),
        ("Damascus", "דמשק"),
        ("India", "הודו"),
        ("Istanbul", "איסטנבול"),
        ("Jerusalem", "ירושלים"),
        ("khaybar", "ח'יבר"),
        ("Maldives", "האיים המלדיביים"),
        ("Qus", "קוץ (Qus)"),
        ("Sicily", "סיציליה"),
        ("Spain", "ספרד"),
        ("susa", "סוסה"),
        ("Tyre", "צור"),
    ]),

    # --- אישים / People ---
    ("People", "אישים", [
        ("Abu l-Hasan b. Wahb", "אבו אל-חסן בן והב"),
        ("Abu Sahl Levi", "אבו סהל לוי"),
        ("Abun b. Sedaqa", "עבון בן צדקה"),
        ("al-Razi", "אל-ראזי"),
        ("Arus b. Yosef", "ערוס בן יוסף"),
        ("baldwin", "בלדווין"),
        ("David b. Daniel", "דוד בן דניאל"),
        ("Efrayim b. Shemarya", "אפרים בן שמריה"),
        ("Eli b. Ezekiel", "עלי בן יחזקאל"),
        ("Eli b. Hayyim", "עלי בן חיים"),
        ("Eliyyahu b. Zekharia", "אליהו בן זכריה"),
        ("Halfon b. Menashshe", "חלפון בן מנשה"),
        ("Ibn al-Taffal", "אבן אל-טפאל"),
        ("Ibn Yiju", "אבן יג'ו"),
        ("Judge Eliyyahu", "אליהו הדיין"),
        ("Maimonides", "הרמב\"ם"),
        ("Maimonides autograph", "אוטוגרף של הרמב\"ם"),
        ("Marduk b. Musa", "מרדוך בן מוסא"),
        ("Mevorakh b. Natan", "מבורך בן נתן"),
        ("Moshe b. Levi", "משה בן לוי"),
        ("Moshe b. Levi (signed)", "משה בן לוי (חתום)"),
        ("Moshe b. Levi ha-Levi", "משה בן לוי הלוי"),
        ("Moshe b. Levi literary", "משה בן לוי (ספרותי)"),
        ("Nahray B. Nissim", "נהראי בן נסים"),
        ("Saladin", "צלאח אל-דין"),
        ("Sar Shalom", "שר שלום"),
        ("Shelomo b. Eliyyahu", "שלמה בן אליהו"),
        ("Shemarya b. Elhanan", "שמריה בן אלחנן"),
        ("Yedutun ha-Levi", "ידותון הלוי"),
    ]),

    # --- תגים פנימיים / Editorial ---
    ("Editorial", "תגים פנימיים", [
        ("cmp", "cmp"),
        ("to edit", "לעריכה"),
        ("to examine", "לבדיקה"),
        ("unedited 11th c", "המאה ה-11 (טרם נערך)"),
    ]),
]


# Build flat lookup dict from categorized structure
PGP_TAG_TRANSLATIONS = {}
for _cat_en, _cat_he, _tags in CATEGORIZED_TAGS:
    for _en, _he in _tags:
        if _en not in PGP_TAG_TRANSLATIONS:  # first category wins for duplicates
            PGP_TAG_TRANSLATIONS[_en] = _he


def translate_tag(tag: str) -> str:
    """Return Hebrew translation for a PGP tag, or the original tag if no translation exists."""
    return PGP_TAG_TRANSLATIONS.get(tag, tag)


def translate_tag_display(tag: str, lang: str = 'he') -> str:
    """Return display text for a tag based on language.

    - Hebrew ('he'): 'עברית (English)'
    - English ('en'): just the English tag
    """
    if lang != 'he':
        return tag
    he = PGP_TAG_TRANSLATIONS.get(tag)
    if he and he != tag:
        return f"{he} ({tag})"
    return tag


def get_categorized_tags_for_display(available_tags: List[str], lang: str = 'he') -> List[Tuple[Optional[str], str, str]]:
    """Return tags grouped by category for display in dropdowns.

    Only includes tags that exist in available_tags (from the database).

    Args:
        available_tags: List of English tags available in the database.
        lang: 'he' for Hebrew display, 'en' for English only.

    Returns:
        List of (category_header_or_None, display_text, english_tag) tuples.
        category_header is a string for the first item of each group, None for subsequent items.
        The header items have english_tag = '' (not selectable).
    """
    available_set = set(available_tags)
    result = []

    for cat_en, cat_he, tags in CATEGORIZED_TAGS:
        cat_tags = [(en, he) for en, he in tags if en in available_set]
        if not cat_tags:
            continue

        # Add category header
        header = cat_he if lang == 'he' else cat_en
        result.append((header, f"── {header} ──", ""))

        # Add tags in this category
        for en, he in cat_tags:
            display = translate_tag_display(en, lang)
            result.append((None, display, en))

    # Check for uncategorized tags (in DB but not in any category)
    categorized = set()
    for _, _, tags in CATEGORIZED_TAGS:
        for en, _ in tags:
            categorized.add(en)

    uncategorized = sorted(t for t in available_tags if t not in categorized)
    if uncategorized:
        header = "שונות" if lang == 'he' else "Other"
        result.append((header, f"── {header} ──", ""))
        for tag in uncategorized:
            display = translate_tag_display(tag, lang)
            result.append((None, display, tag))

    return result
