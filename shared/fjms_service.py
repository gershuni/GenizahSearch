# -*- coding: utf-8 -*-
"""
FJMS Enrichment Service for accessing FIST/FJMS data from the SQLite sidecar.

This module provides the FjmsService class for querying domain classifications,
scholar join groups, and catalog metadata from the fjms_enrichment.db sidecar
database. Used by both the web app and desktop app.

All methods handle errors gracefully, returning empty results rather than
raising exceptions. When the sidecar database is missing, the service
degrades gracefully (is_available() returns False, all queries return empty).

Thread-safe mode (check_same_thread=False) is available for the NiceGUI
web app which serves concurrent requests from multiple threads.
"""

import json
import logging
import os
import re
import sqlite3
import threading
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Default sidecar filename
_SIDECAR_FILENAME = "fjms_enrichment.db"
_SIDECAR_DIR = "fist_data"

# Printed badge display constants
PRINTED_BADGE_COLORS = ('#fee2e2', '#dc2626')  # Red-tinted attention color (bg, text)
PRINTED_LABEL_EN = 'Printed'
PRINTED_LABEL_HE = '\u05d3\u05e4\u05d5\u05e1'

# Team name data: maps EngDesc from dbo_CodeSource to display names.
# Each entry: (en_header, en_full, he_header, he_full)
# - en_header / he_header: column header without leader (e.g., "FGP Linguistics team")
# - en_full / he_full: full name with leader (e.g., "FGP Linguistics team, Aharon Maman (head)")
# - None means "fall back to raw source_name"
# Leader names sourced from FJMS website (not present in FIST.db).
_TEAM_NAMES = {
    # fmt: off
    "Aggadic Midrashim": (
        "FGP Aggadic Midrashim team",
        "FGP Aggadic Midrashim team, Chaim Milikowsky (head)",
        "צוות FGP למדרשי אגדה",
        "צוות FGP למדרשי אגדה, חיים מיליקובסקי (ראש צוות)",
    ),
    "Bibliography B": (
        "FGP Bibliography B team",
        "FGP Bibliography B team, Zvi Stampfer, Yitzchack Gila (head)",
        "צוות FGP לביבליוגרפיה ב",
        "צוות FGP לביבליוגרפיה ב, צבי שטמפפר, יצחק גילה (ראש צוות)",
    ),
    "Bibliography C": (
        "FGP Bibliography C team",
        "FGP Bibliography C team, Emanuel Friedberg (head)",
        "צוות FGP לביבליוגרפיה ג",
        "צוות FGP לביבליוגרפיה ג, עמנואל פרידברג (ראש צוות)",
    ),
    "Bibliography Cambridge": (
        "FGP Bibliography Cambridge team",
        "FGP Bibliography Cambridge team",
        "צוות FGP לביבליוגרפיה קיימברידג",
        "צוות FGP לביבליוגרפיה קיימברידג",
    ),
    "Firkovitch Collections": (
        "FGP Firkovitch Collections team",
        "FGP Firkovitch Collections team, David Sklare (head)",
        "צוות FGP לאוספי פירקוביץ'",
        "צוות FGP לאוספי פירקוביץ', דוד סקליר (ראש צוות)",
    ),
    "Halakhic Midrashim": (
        "FGP Halakhic Midrashim team",
        "FGP Halakhic Midrashim team, Menahem Kahana (head)",
        "צוות FGP למדרשי הלכה",
        "צוות FGP למדרשי הלכה, מנחם כהנא (ראש צוות)",
    ),
    "Judeo-Arabic Biblical Exegesis": (
        "FGP Judeo-Arabic Biblical Exegesis team",
        "FGP Judeo-Arabic Biblical Exegesis team, Ephraim Ben-Porat (head)",
        "צוות FGP לפרשנות המקרא בערבית-יהודית",
        "צוות FGP לפרשנות המקרא בערבית-יהודית, אפרים בן-פורת (ראש צוות)",
    ),
    "Judeo-Arabic Halakhic Literature": (
        "FGP Judeo-Arabic Halakhic Literature team",
        "FGP Judeo-Arabic Halakhic Literature team, David Sklare (head)",
        "צוות FGP לספרות ההלכה בערבית-יהודית",
        "צוות FGP לספרות ההלכה בערבית-יהודית, דוד סקליר (ראש צוות)",
    ),
    "Judeo-Persian": (
        "FGP Judeo-Persian team",
        "FGP Judeo-Persian team, Shaul Shaked (head)",
        "צוות FGP לפרסית-יהודית",
        "צוות FGP לפרסית-יהודית, שאול שקד (ראש צוות)",
    ),
    "Ladino": (
        "FGP Ladino team",
        "FGP Ladino team, Aldina Quintana (head)",
        "צוות FGP ללאדינו",
        "צוות FGP ללאדינו, אלדינה קינטנה (ראש צוות)",
    ),
    "Late Documentary Material (Hebrew)": (
        "FGP Late Documentary Material (Hebrew) team",
        "FGP Late Documentary Material (Hebrew) team, Avraham David (head)",
        "צוות FGP לחומר תיעודי מאוחר (עברית)",
        "צוות FGP לחומר תיעודי מאוחר (עברית), אברהם דוד (ראש צוות)",
    ),
    "Linguistics": (
        "FGP Linguistics team",
        "FGP Linguistics team, Aharon Maman (head)",
        "צוות FGP לחכמת הלשון",
        "צוות FGP לחכמת הלשון, אהרן ממן (ראש צוות)",
    ),
    "Liturgy": (
        "FGP Liturgy team",
        "FGP Liturgy team, Uri Erlich (head)",
        "צוות FGP לתפילה",
        "צוות FGP לתפילה, אורי ארליך (ראש צוות)",
    ),
    "Magic": (
        "FGP Magic team",
        "FGP Magic team, Gideon Bohak (head)",
        "צוות FGP למאגיה",
        "צוות FGP למאגיה, גדעון בוהק (ראש צוות)",
    ),
    "Midrash Eikha Rabba": (
        "FGP Midrash Eikha Rabba team",
        "FGP Midrash Eikha Rabba team, Paul Mandel (head)",
        "צוות FGP למדרש איכה רבא",
        "צוות FGP למדרש איכה רבא, פנחס מנדל (ראש צוות)",
    ),
    "Philosophy, Theology and Polemics": (
        "FGP Philosophy, Theology and Polemics team",
        "FGP Philosophy, Theology and Polemics team, Sarah Stroumsa (head)",
        "צוות FGP לפילוסופיה, תיאולוגיה ופולמוס",
        "צוות FGP לפילוסופיה, תיאולוגיה ופולמוס, שרה סטרומזה (ראש צוות)",
    ),
    "Princeton Documentary Material (Goitein)": (
        "FGP Princeton Documentary Material (Goitein) team",
        "FGP Princeton Documentary Material (Goitein) team, Mark Cohen (head)",
        "צוות FGP פרינסטון לחומר תיעודי (גויטין)",
        "צוות FGP פרינסטון לחומר תיעודי (גויטין), מרק כהן (ראש צוות)",
    ),
    "Rabbinic Material": (
        "FGP Rabbinic Material team",
        "FGP Rabbinic Material team, Ezra Chwat (head)",
        "צוות FGP לחומר רבני",
        "צוות FGP לחומר רבני, עזרא שבט (ראש צוות)",
    ),
    "Responsa": (
        "FGP Responsa team",
        "FGP Responsa team, Mordechai A. Friedman (head)",
        "צוות FGP לשו\"ת",
        "צוות FGP לשו\"ת, מרדכי עקיבא פרידמן (ראש צוות)",
    ),
    "Seride Teshuvot Team: Shocken Institute": (
        "FGP Seride Teshuvot Team: Shocken Institute",
        "FGP Seride Teshuvot Team: Shocken Institute, Shmuel Glick (head)",
        "צוות FGP לשרידי תשובות \u2013 מכון שוקן",
        "צוות FGP לשרידי תשובות \u2013 מכון שוקן, שמואל גליק (ראש צוות)",
    ),
    "T-S Cataloging": (
        "FGP T-S Cataloging team",
        "FGP T-S Cataloging team, Yaacov Sussmann (head)",
        "צוות FGP לT-S NS",
        "צוות FGP לT-S NS, יעקב זוסמן (ראש צוות)",
    ),
    "Talmud Commentaries and Halakhic Literature (Hebrew)": (
        "FGP Talmud Commentaries and Halakhic Literature (Hebrew) team",
        "FGP Talmud Commentaries and Halakhic Literature (Hebrew) team, Simcha Emanuel (head)",
        "צוות FGP לפרשנות התלמוד והספרות ההלכתית (עברית)",
        "צוות FGP לפרשנות התלמוד והספרות ההלכתית (עברית), שמחה עמנואל (ראש צוות)",
    ),
    "Talmudic Literature": (
        "FGP Talmudic Literature team",
        "FGP Talmudic Literature team, Yaacov Sussmann (head)",
        "צוות FGP לספרות תלמודית",
        "צוות FGP לספרות תלמודית, יעקב זוסמן (ראש צוות)",
    ),
    "Transcriptions and Information by various authors": (
        "FGP Transcriptions and Information by various authors team",
        "FGP Transcriptions and Information by various authors team, Emanuel Friedberg (head)",
        "צוות FGP להעתקות ומידע ממחברים שונים",
        "צוות FGP להעתקות ומידע ממחברים שונים, עמנואל פרידברג (ראש צוות)",
    ),
    "Transcriptions and Information from various publications": (
        "FGP Transcriptions and Information from various publications team",
        "FGP Transcriptions and Information from various publications team, Emanuel Friedberg (head)",
        "צוות FGP להעתקות ומידע מכתבי עת ופרסומים שונים",
        "צוות FGP להעתקות ומידע מכתבי עת ופרסומים שונים, עמנואל פרידברג (ראש צוות)",
    ),
    "Transcriptions Team-Genuzos": (
        "FGP Transcriptions Team-Genuzos",
        "FGP Transcriptions Team-Genuzos",
        "צוות FGP להעתקות -גנוזות",
        "צוות FGP להעתקות -גנוזות",
    ),
    "Yerushalmi": (
        "FGP Yerushalmi team",
        "FGP Yerushalmi team, Yaacov Sussmann (head)",
        "צוות FGP לתלמוד ירושלמי",
        "צוות FGP לתלמוד ירושלמי, יעקב זוסמן (ראש צוות)",
    ),
    # Teams without FJMS Hebrew data (header only, no leader)
    "Bible": ("FGP Bible team", "FGP Bible team", None, None),
    "Documentary Material (Goitein)": ("FGP Documentary Material (Goitein) team", "FGP Documentary Material (Goitein) team", None, None),
    "Karaite Literature": ("FGP Karaite Literature team", "FGP Karaite Literature team", None, None),
    "Piyyut": ("FGP Piyyut team", "FGP Piyyut team", None, None),
    "Samaritan": ("FGP Samaritan team", "FGP Samaritan team", None, None),
    "Science": ("FGP Science team", "FGP Science team", None, None),
    "Scientific Joins": ("FGP Scientific Joins team", "FGP Scientific Joins team", None, None),
    "Yemenite": ("FGP Yemenite team", "FGP Yemenite team", None, None),
    # Non-team entries
    "FGP": ("FGP", "FGP", None, None),
    "PGPID": ("PGPID", "PGPID", None, None),
    # fmt: on
}

# Case-insensitive lookup (EngDesc in FIST.db may be uppercase)
_TEAM_NAMES_LOWER = {k.lower(): v for k, v in _TEAM_NAMES.items()}

# Backward-compatible flat dict (en_full only) — used by existing callers
TEAM_DISPLAY_NAMES = {k: v[1] for k, v in _TEAM_NAMES.items() if v[1]}

# Generic source names that don't represent scholarly teams — filtered from
# button counts and dialog team columns for consistency.
GENERIC_SOURCE_NAMES = frozenset({
    'Inventory', 'Nuscha', 'Institution', 'Instatution', 'Collection', 'Other',
})

# Domains that appear as children of multiple parent categories.
# These need qualification with parent name to be distinguishable in filters.
# Data: SELECT Domain FROM domains WHERE ParentDomain IS NOT NULL AND ParentDomain != Domain
#        GROUP BY Domain HAVING COUNT(DISTINCT ParentDomain) > 1
AMBIGUOUS_CHILD_DOMAINS = frozenset({'Other'})


def _lookup_team(source_name: str):
    """Case-insensitive lookup in _TEAM_NAMES. Returns tuple or None."""
    if not source_name:
        return None
    return _TEAM_NAMES.get(source_name) or _TEAM_NAMES_LOWER.get(source_name.lower())


def is_team_source(source_name: str) -> bool:
    """Check if a source_name maps to an FGP team (not catalog/other)."""
    return _lookup_team(source_name) is not None


def get_team_display_name(source_name: str, is_heb: bool = False) -> str:
    """Map a catalog SourceName to its full FJMS display name with team leader.

    Case-insensitive lookup (EngDesc in FIST.db may be uppercase).
    Falls back to the original source_name if no mapping exists.

    Args:
        source_name: EngDesc value from dbo_CodeSource (e.g., "Linguistics" or "MAGIC").
        is_heb: If True, return Hebrew name; otherwise English.

    Returns:
        Full display name (e.g., "FGP Linguistics team, Aharon Maman (head)").
    """
    if not source_name:
        return source_name or ''
    data = _lookup_team(source_name)
    if not data:
        return source_name
    result = data[3] if is_heb else data[1]
    return result or source_name


def get_team_header_name(source_name: str, is_heb: bool = False) -> str:
    """Map a catalog SourceName to its column header name (without leader).

    Case-insensitive lookup. Used for table column headers in the catalog dialog.
    Falls back to the original source_name if no mapping exists.

    Args:
        source_name: EngDesc value from dbo_CodeSource (e.g., "Linguistics" or "MAGIC").
        is_heb: If True, return Hebrew header; otherwise English.

    Returns:
        Header name (e.g., "FGP Linguistics team" or "צוות FGP לחכמת הלשון").
    """
    if not source_name:
        return source_name or ''
    data = _lookup_team(source_name)
    if not data:
        return source_name
    result = data[2] if is_heb else data[0]
    return result or source_name


def qualify_domain_name(domain: str, parent_domain: str = None) -> str:
    """Qualify ambiguous child domain names with their parent for uniqueness.

    Domains like "Other" appear under multiple parent categories. Without
    qualification, filtering by "Other" in one parent would affect all parents.
    This function returns "Other (Liturgy and Brakhot)" for ambiguous domains
    and the bare domain name for unambiguous ones.

    Args:
        domain: The domain name (e.g., "Other", "Piyyut").
        parent_domain: The parent domain name (e.g., "Liturgy and Brakhot").

    Returns:
        Qualified name if ambiguous, otherwise the bare domain name.
    """
    if domain in AMBIGUOUS_CHILD_DOMAINS and parent_domain and parent_domain != domain:
        return f"{domain} ({parent_domain})"
    return domain


def unqualify_domain_name(qualified: str) -> tuple[str, str]:
    """Extract bare domain and parent from a qualified domain name.

    Returns (domain, parent_domain) tuple. For unqualified names,
    parent_domain is empty string.

    Args:
        qualified: e.g., "Other (Liturgy and Brakhot)" or "Piyyut".

    Returns:
        Tuple of (domain, parent_domain).
    """
    if ' (' in qualified and qualified.endswith(')'):
        idx = qualified.index(' (')
        domain = qualified[:idx]
        parent = qualified[idx + 2:-1]
        if domain in AMBIGUOUS_CHILD_DOMAINS:
            return (domain, parent)
    return (qualified, '')


def _find_project_root() -> Optional[Path]:
    """Find the project root by looking for libraries.csv up from this file."""
    current = Path(__file__).resolve().parent
    for _ in range(5):  # Up to 5 levels
        if (current / "libraries.csv").exists():
            return current
        current = current.parent
    return None


def _is_int(value) -> bool:
    """Check if a value can be interpreted as an integer (for person_id/title_id)."""
    if isinstance(value, int):
        return True
    if isinstance(value, str):
        try:
            int(value)
            return True
        except ValueError:
            return False
    return False


# Canonical FJMS domain ordering (matches Friedberg classification system, not by count)
_FJMS_PARENT_ORDER = [
    # 'Unknown',  # Not present in enrichment DB
    'Bible: Texts and Translations',
    'Biblical Exegesis',
    'Rabbinic Literature',
    'Halakhic Literature and Talmudic Commentaries',
    'Derashot and Later Midrashim',
    'Philosophy, Theology, Ethical literature',
    'Kabbalah',
    'Polemics',
    'Historiography and geographical descriptions',
    'Occult Sciences',
    'Sciences',
    'Liturgy and Brakhot',
    'Piyut and its Interpretation',
    'Secular Poetry',
    'Stories and Belles Lettres',
    'Philology',
    'Documentary',
    'Ritual Objects',
    'Other Religions',
    'Teaching Aids,Pen Trials,Writing Exercises,Scribblings,Jotting',
    'Ancillaries to the Main Work',
    'Unspecified (Nature of text unclear after initial inspection)',
    'Unspecified Domain',
]
_FJMS_PARENT_IDX = {name: i for i, name in enumerate(_FJMS_PARENT_ORDER)}

# Canonical FJMS sub-domain ordering per parent (sub-domains only, not sub-sub-domains)
_FJMS_CHILD_ORDER = {
    'Bible: Texts and Translations': [
        'Bible: Texts', 'Aramaic Targumim', 'Arabic Tafsir',
        'Translations into other Languages', 'Apocryphal Literature',
        'Massorah', 'Lists of Parshiyyot and Haftarot', 'Haftarot',
    ],
    'Biblical Exegesis': [
        'Biblical Exegesis- Rabbanite', 'Biblical Exegesis- Karaite',
    ],
    'Rabbinic Literature': [
        'Mishnah: Texts and Translations', 'Tosefta',
        'Talmud Bavli: Texts and Anthologies', 'Minor Tractates',
        'Talmud Yerushalmi', 'Midrash',
    ],
    'Halakhic Literature and Talmudic Commentaries': [
        'Mishnaic Commentaries', 'Talmud Bavli Commentaries',
        'Talmud Yerushalmi Commentaries', 'Talmudic Commentaries',
        'Halakhic', 'Sifrei Mitzvot (Rabbinical)',
        'Responsa and Halakhic Decisions', 'Minhagim',
        'Talmud \u2013 Introductions and Rules',
    ],
    'Derashot and Later Midrashim': [
        'Derashot', 'Eulogies', 'Later Midrashim',
    ],
    'Philosophy, Theology, Ethical literature': [
        'Kalam', 'Philosophy', 'Logic', 'Ethical Literature',
        'Mystical Literature (not Kabbalah)', 'Sufi Literature',
        'Hermetic Literature', 'Wisdom Literature',
        'Apocalyptic Literature', 'Theology',
    ],
    'Kabbalah': [
        'Heikhalot', 'Zohar literature',
        'Spanish and Provencal Kabbalah', 'Lurianic Kabbalah',
    ],
    'Polemics': [
        'Polemics Karaite-Rabbanite', 'Polemics Jewish-Christian',
        'Polemics Jewish-Muslim', 'Polemics Rabbinical',
    ],
    'Occult Sciences': [
        'Theoretical Works', 'Astrology', 'Alchemy', 'Magic Recipes',
        'Amulets', 'Shimmush Tehillim', 'Predicting the Future',
        'Revealing Treasures',
    ],
    'Sciences': [
        'Astronomy', 'Mathematics', 'Medicine', 'Meteorology', 'Physics',
    ],
    'Liturgy and Brakhot': [
        'Common Prayers', 'Brakhot', 'Prayer Commentaries',
        'Karaite Prayers', 'Occasional prayer', 'Liturgical additions',
        'Baqqashot and Personal Prayers', 'Passover Haggadah',
    ],
    'Piyut and its Interpretation': [
        'Piyyut', 'Liturgical commentary', 'Piyyut Commentaries',
    ],
    'Secular Poetry': ['Dirges'],
    'Philology': [
        'Grammar', 'Dictionaries', 'Glossaries', 'Cantillation notes',
    ],
    'Documentary': [
        'Letters', 'Personal Status Documents and Legal documents',
        'Business Documents', 'Lists', 'Communal Documents',
        'Court Documents', 'Governmental Documents', 'Notes/Records', 'Accounts',
    ],
    'Ritual Objects': [
        'Mezuzot', 'Tefillin', 'Torah scroll', 'Esther Scroll',
    ],
    'Other Religions': ['Christian', 'Muslim'],
    'Ancillaries to the Main Work': [
        'Colophons', 'Title Pages', 'Table of contents', 'Indices',
        'Calendars', 'Teaching Aids',
    ],
    'Unspecified Domain': [
        'Blank', 'Illegible', 'Missing',
        'Cannot be determined from the catalogue', 'Unidentified',
    ],
}
_FJMS_CHILD_IDX = {
    parent: {name: i for i, name in enumerate(children)}
    for parent, children in _FJMS_CHILD_ORDER.items()
}

# Canonical FJMS sub-sub-domain ordering (third level)
_FJMS_SUBCHILD_ORDER = {
    'Massorah': [
        'Masorah that follows the text order', 'Cumulative or Comparative Masorah',
        'Masorah in Arabic', 'Masorah Variants',
        "Diqduqe ha-Te'amim and Qunterese ha-Masorah", 'Lists and Counts',
    ],
    'Mishnah: Texts and Translations': [
        'Mishnah: Texts', 'Mishnah: Translations',
    ],
    'Talmud Bavli: Texts and Anthologies': [
        'Talmud Bavli', 'Talmud Bavli: Anthologies',
    ],
    'Midrash': ['Halakhic Midrashim', 'Aggadic Midrashim'],
    'Halakhic': [
        'Halakhic - Saadia Gaon', 'Halakhic - Shmuel ben Hofni Gaon',
        'Halakhot ha-Rif and its Commentaries', 'Halakhic- Gaonim',
        'Mishneh Torah and its Commentaries',
        'Halakhic- Rishonim and Aharonim', 'Halakhic- Karaite',
    ],
    'Responsa and Halakhic Decisions': [
        'Responsa- Gaonim', 'Responsa- Rishonim and Aharonim', 'Responsa- Karaite',
    ],
    'Kalam': ['Jewish Kalam', 'Muslim Kalam'],
    'Philosophy': ['Aristotelian Philosophy', 'Neoplatonic Philosophy'],
    'Theology': ['Legal theory'],
    'Predicting the Future': [
        'Dream interpretation', 'Goralot (Lots)', 'Goralot (Lots) in Sand',
        'Predictions by Thunder', 'Palmistry', 'Predictions by Ticks',
        'Physiognomy', 'Hemorology/Horology',
    ],
    'Astronomy': ['Calendar'],
    'Medicine': ['Medical Works', 'Medical Prescriptions', 'Pharmacology'],
    'Glossaries': [
        'Biblical Glossary', 'Mishnaic Glossary',
        'Talmudic Glossary', 'Glossary for Piyyut',
    ],
    'Personal Status Documents and Legal documents': [
        'Get Halitzah', 'Ketubbot', 'Legal documents',
    ],
    'Business Documents': ['Monetary Issues', 'Contracts'],
    'Lists': [
        'Book lists', 'Shopping lists', 'Charity Lists',
        'Genealogical Records', 'Property Lists', 'Lists of People',
        'Lists of Debts', 'Responsa lists',
    ],
    'Communal Documents': [
        'Communal Registers', 'Writs of Appointment', 'Bans and Excommunications',
    ],
    'Court Documents': ['Court Records', 'Court Registers'],
}
_FJMS_SUBCHILD_IDX = {
    parent: {name: i for i, name in enumerate(children)}
    for parent, children in _FJMS_SUBCHILD_ORDER.items()
}


class FjmsService:
    """Service for accessing FJMS enrichment data from the SQLite sidecar."""

    def __init__(self, db_path: str = None, thread_safe: bool = True):
        """
        Initialize FjmsService.

        Args:
            db_path: Path to fjms_enrichment.db. If None, auto-detect from project root.
            thread_safe: If True, use check_same_thread=False. Default True because
                        both the web app (concurrent requests) and desktop app
                        (QThread workers for catalog browse) need cross-thread access.
                        Safe since the connection is read-only (?mode=ro).
        """
        self._conn: Optional[sqlite3.Connection] = None
        self._db_path: Optional[str] = None
        self._hierarchy_cache: Optional[dict] = None
        self._hierarchy_lock = threading.Lock()
        self._authors_cache: Optional[list] = None
        self._authors_lock = threading.Lock()
        self._works_cache: Optional[list] = None
        self._works_lock = threading.Lock()
        self._unclassified_cache: Optional[int] = None
        self._unclassified_lock = threading.Lock()
        self._has_persons_titles: bool = False  # Set True if v5+ tables exist

        # Resolve db_path
        if db_path is None:
            # Check user-updated sidecar location first (LOCALAPPDATA)
            import os
            user_path = os.path.join(
                os.environ.get('LOCALAPPDATA', ''),
                'GenizahSearchPro', 'data', _SIDECAR_DIR, _SIDECAR_FILENAME
            )
            if os.path.isfile(user_path):
                db_path = user_path
            else:
                root = _find_project_root()
                if root:
                    db_path = str(root / _SIDECAR_DIR / _SIDECAR_FILENAME)

        if db_path is None:
            logger.warning("FjmsService: No db_path provided and project root not found")
            return

        self._db_path = db_path
        db_file = Path(db_path)

        if not db_file.exists():
            logger.warning(f"FjmsService: Sidecar database not found at {db_path}")
            return

        try:
            # Open read-only connection using URI mode
            uri = f"file:{db_path}?mode=ro"
            self._conn = sqlite3.connect(
                uri,
                uri=True,
                check_same_thread=not thread_safe,
                timeout=10.0,
            )
            self._conn.row_factory = sqlite3.Row
            logger.info(f"FjmsService: Connected to {db_path}")

            # Note: performance indexes are created lazily in pre_warm_caches()
            # to avoid blocking the main thread during app startup.

            # Detect v5+ lookup tables (genizah_persons, genizah_titles)
            try:
                cnt = self._conn.execute(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' "
                    "AND name IN ('genizah_persons', 'genizah_titles')"
                ).fetchone()[0]
                self._has_persons_titles = cnt == 2
                if self._has_persons_titles:
                    logger.info("FjmsService: v5+ lookup tables detected (genizah_persons, genizah_titles)")
            except Exception:
                self._has_persons_titles = False
        except Exception as e:
            logger.error(f"FjmsService: Failed to connect to {db_path}: {e}")
            self._conn = None

    def is_available(self) -> bool:
        """Returns True if the sidecar database connection is active."""
        return self._conn is not None

    def get_version(self) -> Optional[str]:
        """
        Get the sidecar database version.

        Returns:
            Version string (e.g., '1.0.0') or None if unavailable.
        """
        if self._conn is None:
            return None
        try:
            cursor = self._conn.execute(
                "SELECT value FROM meta WHERE key = 'version'"
            )
            row = cursor.fetchone()
            return row["value"] if row else None
        except Exception as e:
            logger.error(f"FjmsService.get_version error: {e}")
            return None

    def get_domains(self, sys_id: str) -> list[dict]:
        """
        Get domain classifications for a manuscript.

        Args:
            sys_id: The Alma/system ID for the manuscript.

        Returns:
            List of dicts with keys: domain, domain_heb, parent_domain, parent_domain_heb.
            Returns [] if conn is None or sys_id not found.
        """
        if self._conn is None:
            return []
        try:
            cursor = self._conn.execute(
                "SELECT Domain, DomainHeb, ParentDomain, ParentDomainHeb "
                "FROM domains WHERE AlmaId = ?",
                (sys_id,),
            )
            return [
                {
                    "domain": row["Domain"],
                    "domain_heb": row["DomainHeb"],
                    "parent_domain": row["ParentDomain"],
                    "parent_domain_heb": row["ParentDomainHeb"],
                }
                for row in cursor
            ]
        except Exception as e:
            logger.error(f"FjmsService.get_domains error for {sys_id}: {e}")
            return []

    def get_manuscripts_by_domain(self, domain: str) -> set[str]:
        """
        Get all manuscript IDs classified under a domain.

        Matches both direct domain assignments and parent domain references,
        enabling domain-based search filtering via set intersection.

        Args:
            domain: Domain name in English (e.g., 'Piyyut', 'Letters').

        Returns:
            Set of AlmaId strings. Returns set() if conn is None.
        """
        if self._conn is None:
            return set()
        try:
            cursor = self._conn.execute(
                "SELECT DISTINCT AlmaId FROM domains "
                "WHERE Domain = ? OR ParentDomain = ?",
                (domain, domain),
            )
            return {row["AlmaId"] for row in cursor}
        except Exception as e:
            logger.error(f"FjmsService.get_manuscripts_by_domain error for {domain}: {e}")
            return set()

    def get_domains_for_sys_ids(self, sys_ids: list[str]) -> dict:
        """
        Get domain classifications for multiple manuscripts in batch.

        More efficient than calling get_domains() per sys_id when processing
        search results. Uses batched IN queries to stay within SQLite limits.

        Args:
            sys_ids: List of Alma/system IDs.

        Returns:
            Dict mapping sys_id -> list of domain dicts.
            Each domain dict has keys: domain, domain_heb, parent_domain, parent_domain_heb.
        """
        if not self._conn or not sys_ids:
            return {}
        try:
            result = {}
            # Batch to stay under SQLite variable limit (999)
            batch_size = 500
            for i in range(0, len(sys_ids), batch_size):
                batch = sys_ids[i:i + batch_size]
                placeholders = ','.join('?' * len(batch))
                cursor = self._conn.execute(
                    f"SELECT AlmaId, Domain, DomainHeb, ParentDomain, ParentDomainHeb "
                    f"FROM domains WHERE AlmaId IN ({placeholders})",
                    batch,
                )
                for row in cursor:
                    sid = row["AlmaId"]
                    if sid not in result:
                        result[sid] = []
                    result[sid].append({
                        "domain": row["Domain"],
                        "domain_heb": row["DomainHeb"],
                        "parent_domain": row["ParentDomain"],
                        "parent_domain_heb": row["ParentDomainHeb"],
                    })
            return result
        except Exception as e:
            logger.error(f"FjmsService.get_domains_for_sys_ids error: {e}")
            return {}

    def get_printed_sys_ids(self, sys_ids: list[str]) -> set:
        """Batch lookup which sys_ids have FragmentMaterial=Printed.

        Returns: set of sys_ids that are printed materials.
        Uses the catalog_fields table: FieldCategory='FragmentMaterial' AND FieldValue='Printed'.
        Covers ~12,421 AlmaIds in the Genizah corpus.
        """
        if not self._conn or not sys_ids:
            return set()
        try:
            result = set()
            batch_size = 500
            for i in range(0, len(sys_ids), batch_size):
                batch = sys_ids[i:i + batch_size]
                placeholders = ','.join('?' * len(batch))
                cursor = self._conn.execute(
                    f"SELECT DISTINCT AlmaId FROM catalog_fields "
                    f"WHERE FieldCategory = 'FragmentMaterial' "
                    f"AND FieldValue = 'Printed' "
                    f"AND AlmaId IN ({placeholders})",
                    batch,
                )
                for row in cursor:
                    result.add(row["AlmaId"])
            return result
        except Exception as e:
            logger.error(f"FjmsService.get_printed_sys_ids error: {e}")
            return set()

    def get_filter_sys_ids(
        self,
        domain: str = None,
        author: str = None,
        work: str = None,
        date_from: int = None,
        date_to: int = None,
        include_undated: bool = False,
        material_include: list[str] = None,
        material_exclude: list[str] = None,
    ) -> Optional[set]:
        """Return the set of sys_ids matching all provided filter criteria (intersection).

        Used for pre-search filtering: callers pass the result as restrict_sys_ids
        to execute_search / search_composition_logic so that non-matching manuscripts
        are skipped before expensive regex verification or chunk processing.

        Args:
            domain: Domain or parent-domain name (matches Domain OR ParentDomain).
            author: Person ID (int string, v5+) or AuthorText string (legacy).
            work: GenizahTitleId (int string, v5+) or Title string (legacy).
            date_from: Minimum year inclusive.
            date_to: Maximum year inclusive.
            include_undated: If True AND date filter active, include records with no date.
            material_include: List of FragmentMaterial values to INCLUDE (e.g. ["Printed"]).
            material_exclude: List of FragmentMaterial values to EXCLUDE (e.g. ["Printed"]).

        Returns:
            None when ALL filter params are None/empty (meaning "no restriction").
            set of matching AlmaId strings when any filter is active.
            Empty set when filters are active but match nothing.
        """
        # Fast path: no filters active -> None means "no restriction"
        has_any = (
            domain is not None
            or author is not None
            or work is not None
            or date_from is not None
            or date_to is not None
            or material_include
            or material_exclude
        )
        if not has_any:
            return None

        if self._conn is None:
            return None

        try:
            conditions = []
            params = []

            # Domain filter (same logic as get_browse_results)
            if domain is not None:
                conditions.append(
                    "c.AlmaId IN ("
                    "SELECT AlmaId FROM domains WHERE Domain = ? "
                    "UNION SELECT AlmaId FROM domains WHERE ParentDomain = ?)"
                )
                params.extend([domain, domain])

            # Author filter
            if author is not None:
                if self._has_persons_titles and _is_int(author):
                    person_id = int(author)
                    conditions.append(
                        "(c.GenizahTitleId IN ("
                        "  SELECT gt.GenizahTitleId FROM genizah_titles gt "
                        "  WHERE gt.AuthorId = ?"
                        ") OR c.Author = ?)"
                    )
                    params.extend([person_id, person_id])
                else:
                    conditions.append("c.AuthorText = ?")
                    params.append(author)

            # Work filter
            if work is not None:
                if self._has_persons_titles and _is_int(work):
                    conditions.append("c.GenizahTitleId = ?")
                    params.append(int(work))
                else:
                    conditions.append("c.Title = ?")
                    params.append(work)

            # Date range filter (same logic as get_browse_results)
            has_date_filter = date_from is not None or date_to is not None
            if has_date_filter:
                _no_date = "(c.CopyDate IS NULL OR c.CopyDate = '' OR c.CopyDate = '0' OR c.CopyDate = '-99')"
                date_parts = []
                if date_from is not None and date_to is not None:
                    date_parts.append("CAST(c.CopyDate AS INTEGER) BETWEEN ? AND ?")
                    params.extend([date_from, date_to])
                elif date_from is not None:
                    date_parts.append("CAST(c.CopyDate AS INTEGER) >= ?")
                    params.append(date_from)
                else:
                    date_parts.append("CAST(c.CopyDate AS INTEGER) <= ?")
                    params.append(date_to)
                dated_cond = f"(NOT {_no_date} AND {date_parts[0]})"
                if include_undated:
                    conditions.append(f"({dated_cond} OR {_no_date})")
                else:
                    conditions.append(dated_cond)

            # Material include filter
            if material_include:
                placeholders = ','.join('?' * len(material_include))
                conditions.append(
                    f"c.AlmaId IN ("
                    f"SELECT AlmaId FROM catalog_fields "
                    f"WHERE FieldCategory = 'FragmentMaterial' "
                    f"AND FieldValue IN ({placeholders}))"
                )
                params.extend(material_include)

            # Material exclude filter
            if material_exclude:
                placeholders = ','.join('?' * len(material_exclude))
                conditions.append(
                    f"c.AlmaId NOT IN ("
                    f"SELECT AlmaId FROM catalog_fields "
                    f"WHERE FieldCategory = 'FragmentMaterial' "
                    f"AND FieldValue IN ({placeholders}))"
                )
                params.extend(material_exclude)

            where = " WHERE " + " AND ".join(conditions) if conditions else ""
            sql = f"SELECT DISTINCT c.AlmaId FROM catalog c{where}"
            cursor = self._conn.execute(sql, params)
            return {row["AlmaId"] for row in cursor}

        except Exception as e:
            logger.error(f"FjmsService.get_filter_sys_ids error: {e}")
            return set()

    def get_all_domains(self) -> list[dict]:
        """
        Get all unique domain names with manuscript counts.

        Useful for populating domain filter dropdowns in the UI.

        Returns:
            List of dicts with keys: domain, domain_heb, count.
            Sorted by count descending. Returns [] if conn is None.
        """
        if self._conn is None:
            return []
        try:
            cursor = self._conn.execute(
                "SELECT Domain, DomainHeb, COUNT(DISTINCT AlmaId) as count "
                "FROM domains GROUP BY Domain ORDER BY count DESC"
            )
            return [
                {
                    "domain": row["Domain"],
                    "domain_heb": row["DomainHeb"],
                    "count": row["count"],
                }
                for row in cursor
            ]
        except Exception as e:
            logger.error(f"FjmsService.get_all_domains error: {e}")
            return []

    def get_domain_hierarchy(self) -> dict:
        """
        Get domain hierarchy with counts, grouped by parent domain.

        Results are cached in memory after first computation (hierarchy is static).
        Thread-safe via double-checked locking.

        Returns:
            Dict mapping parent_domain -> {
                'parent_domain_heb': str,
                'count': int,  # total manuscripts under this parent (including children)
                'children': [{'domain': str, 'domain_heb': str, 'count': int}, ...]
            }
            Sorted by parent count descending, children by count descending within each parent.
            Returns {} if conn is None.
        """
        # Fast path: return cached result
        if self._hierarchy_cache is not None:
            return self._hierarchy_cache

        if self._conn is None:
            return {}

        # Slow path: compute and cache
        with self._hierarchy_lock:
            # Double-check after acquiring lock
            if self._hierarchy_cache is not None:
                return self._hierarchy_cache

            try:
                # COUNT(*) is correct per (Domain, ParentDomain) group — no
                # duplicate AlmaIds within a group. Cross-group overlap (e.g.,
                # Piyyut with NULL parent AND "Piyut..." parent) is handled
                # by the dedup step below which queries true DISTINCT counts.
                cursor = self._conn.execute(
                    "SELECT Domain, DomainHeb, ParentDomain, ParentDomainHeb, "
                    "COUNT(*) as count "
                    "FROM domains GROUP BY Domain, ParentDomain ORDER BY count DESC"
                )
                rows = cursor.fetchall()

                # Build hierarchy: map parent -> {parent_domain_heb, count, children[]}
                hierarchy = {}
                parent_counts = {}  # Track total counts per parent

                for row in rows:
                    domain = row["Domain"]
                    domain_heb = row["DomainHeb"]
                    parent = row["ParentDomain"]
                    parent_heb = row["ParentDomainHeb"]
                    count = row["count"]

                    # If this domain HAS a parent (not a root domain)
                    if parent and parent != domain:
                        if parent not in hierarchy:
                            hierarchy[parent] = {
                                'parent_domain_heb': parent_heb,
                                'count': 0,
                                'children': []
                            }
                        hierarchy[parent]['children'].append({
                            'domain': domain,
                            'domain_heb': domain_heb,
                            'count': count
                        })
                        hierarchy[parent]['count'] += count
                        parent_counts[parent] = parent_counts.get(parent, 0) + count
                    else:
                        # Root-level domain (Domain == ParentDomain or no parent)
                        if domain not in hierarchy:
                            hierarchy[domain] = {
                                'parent_domain_heb': domain_heb,
                                'count': count,
                                'children': []
                            }
                        else:
                            # Already exists as parent, just update count
                            hierarchy[domain]['count'] += count
                        parent_counts[domain] = parent_counts.get(domain, 0) + count

                # Deduplicate & nest: if a domain appears as both a child and a
                # standalone root (e.g., "Piyyut" with ParentDomain=NULL AND
                # ParentDomain="Piyut and its Interpretation"), fix the child's
                # count and nest any sub-sub-domains under it (3-level hierarchy).
                child_domains = set()
                for info in hierarchy.values():
                    for child in info.get('children', []):
                        child_domains.add(child['domain'])
                for child_name in child_domains:
                    if child_name in hierarchy:
                        # Query the true distinct count for this domain
                        try:
                            dc = self._conn.execute(
                                "SELECT COUNT(DISTINCT AlmaId) FROM domains WHERE Domain = ?",
                                (child_name,)
                            ).fetchone()[0]
                        except Exception:
                            dc = hierarchy[child_name].get('count', 0)
                        # Set the child entry's count and nest sub-sub-domains
                        orphan_children = hierarchy[child_name].get('children', [])
                        for info in hierarchy.values():
                            for child in info.get('children', []):
                                if child['domain'] == child_name:
                                    child['count'] = dc
                                    if orphan_children:
                                        child['children'] = orphan_children
                                    break
                        del hierarchy[child_name]

                # Merge duplicate children (e.g., two "Other" entries at same level)
                for parent_name, info in hierarchy.items():
                    seen = {}
                    merged = []
                    for child in info.get('children', []):
                        key = child['domain']
                        if key in seen:
                            seen[key]['count'] += child['count']
                        else:
                            seen[key] = child
                            merged.append(child)
                    info['children'] = merged

                # Sort children and sub-children using canonical FJMS ordering
                fallback_pos = 9999
                for parent_name, info in hierarchy.items():
                    child_idx = _FJMS_CHILD_IDX.get(parent_name, {})
                    info['children'].sort(
                        key=lambda x: (
                            child_idx.get(x['domain'], fallback_pos),
                            -x['count'],
                        )
                    )
                    # Sort sub-sub-domains (third level)
                    for child in info['children']:
                        subchildren = child.get('children', [])
                        if subchildren:
                            sc_idx = _FJMS_SUBCHILD_IDX.get(child['domain'], {})
                            subchildren.sort(
                                key=lambda x: (
                                    sc_idx.get(x['domain'], fallback_pos),
                                    -x['count'],
                                )
                            )

                # Recalculate parent counts: use sum of children's distinct counts
                # (fast approximation — avoids 25 slow DISTINCT queries at ~100ms each)
                for parent_name, info in hierarchy.items():
                    child_total = sum(c['count'] for c in info.get('children', []))
                    own_count = info.get('count', 0)
                    info['count'] = max(own_count, child_total)
                    parent_counts[parent_name] = info['count']

                # Sort parents using canonical FJMS ordering (fallback: count desc)
                result = dict(sorted(
                    hierarchy.items(),
                    key=lambda x: (
                        _FJMS_PARENT_IDX.get(x[0], fallback_pos),
                        -parent_counts.get(x[0], 0),
                    )
                ))
                self._hierarchy_cache = result
                return result
            except Exception as e:
                logger.error(f"FjmsService.get_domain_hierarchy error: {e}")
                return {}

    # ── Catalog Browse Methods (Phase 41: BROWSE-01..05) ──────────

    def get_browse_authors(self, domain: str = None) -> list[dict]:
        """
        Get unique authors from the catalog with manuscript counts.

        With v5+ sidecar (genizah_persons/genizah_titles tables), uses the
        structured FK path: catalog.GenizahTitleId -> genizah_titles.AuthorId
        -> genizah_persons, UNION catalog.Author -> genizah_persons.

        Falls back to sparse AuthorText for pre-v5 sidecars.

        Args:
            domain: Optional domain filter. If provided, only include manuscripts
                    that have this domain (matches Domain OR ParentDomain).

        Returns:
            List of dicts with keys: person_id (int), eng_desc (str),
            heb_desc (str), count (int). For legacy fallback: person_id=None,
            eng_desc=AuthorText, heb_desc=''.
            Sorted by count descending.
            Returns [] if conn is None.
        """
        if self._conn is None:
            return []

        # Return cached result for unfiltered query
        if domain is None and self._authors_cache is not None:
            return self._authors_cache

        try:
            result = self._query_browse_authors(domain)

            if domain is None:
                with self._authors_lock:
                    if self._authors_cache is not None:
                        return self._authors_cache
                    self._authors_cache = result
            return result
        except Exception as e:
            logger.error(f"FjmsService.get_browse_authors error: {e}")
            return []

    def _query_browse_authors(self, domain: str = None) -> list[dict]:
        """Execute the browse authors query."""
        if self._has_persons_titles:
            return self._query_browse_authors_v5(domain)
        return self._query_browse_authors_legacy(domain)

    def _query_browse_authors_v5(self, domain: str = None) -> list[dict]:
        """v5+ query: structured FK path through genizah_persons.

        Optimization: pre-dedup catalog rows in a CTE so COUNT(*) replaces
        the expensive COUNT(DISTINCT AlmaId). Domain filter uses IN+UNION
        subquery for proper index utilization (30s -> 0.3s).
        """
        params = []
        if domain is not None:
            domain_filter = (
                "AlmaId IN ("
                "SELECT AlmaId FROM domains WHERE Domain = ? "
                "UNION SELECT AlmaId FROM domains WHERE ParentDomain = ?)"
            )
            params = [domain, domain]
            cte = f"WITH dc AS (SELECT DISTINCT AlmaId, GenizahTitleId, Author FROM catalog WHERE {domain_filter})"
        else:
            cte = "WITH dc AS (SELECT DISTINCT AlmaId, GenizahTitleId, Author FROM catalog)"

        # Path 1: catalog -> genizah_titles -> genizah_persons (via GenizahTitleId)
        # Path 2: catalog.Author -> genizah_persons (direct FK, for records without GenizahTitleId)
        sql = f"""
            {cte}
            SELECT person_id, eng_desc, heb_desc, SUM(cnt) as count FROM (
                SELECT gp.GenizahPersonId as person_id, gp.EngDesc as eng_desc,
                       gp.HebDesc as heb_desc, COUNT(*) as cnt
                FROM dc
                INNER JOIN genizah_titles gt ON dc.GenizahTitleId = gt.GenizahTitleId
                INNER JOIN genizah_persons gp ON gt.AuthorId = gp.GenizahPersonId
                WHERE gp.GenizahPersonId > 0
                GROUP BY gp.GenizahPersonId, gp.EngDesc, gp.HebDesc
                UNION ALL
                SELECT gp.GenizahPersonId as person_id, gp.EngDesc as eng_desc,
                       gp.HebDesc as heb_desc, COUNT(*) as cnt
                FROM dc
                INNER JOIN genizah_persons gp ON dc.Author = gp.GenizahPersonId
                WHERE dc.GenizahTitleId IS NULL AND dc.Author IS NOT NULL AND dc.Author > 0
                GROUP BY gp.GenizahPersonId, gp.EngDesc, gp.HebDesc
            ) grouped
            GROUP BY person_id, eng_desc, heb_desc
            ORDER BY count DESC
        """
        cursor = self._conn.execute(sql, params)
        return [
            {
                "person_id": row["person_id"],
                "eng_desc": row["eng_desc"] or "",
                "heb_desc": row["heb_desc"] or "",
                "count": row["count"],
            }
            for row in cursor
        ]

    def _query_browse_authors_legacy(self, domain: str = None) -> list[dict]:
        """Legacy query: sparse AuthorText column."""
        if domain is None:
            cursor = self._conn.execute(
                "SELECT AuthorText, COUNT(DISTINCT AlmaId) as count "
                "FROM catalog "
                "WHERE AuthorText IS NOT NULL AND AuthorText != '' "
                "GROUP BY AuthorText ORDER BY count DESC"
            )
        else:
            cursor = self._conn.execute(
                "SELECT AuthorText, COUNT(DISTINCT AlmaId) as count "
                "FROM catalog "
                "WHERE AuthorText IS NOT NULL AND AuthorText != '' "
                "  AND AlmaId IN ("
                "    SELECT AlmaId FROM domains WHERE Domain = ? "
                "    UNION SELECT AlmaId FROM domains WHERE ParentDomain = ?) "
                "GROUP BY AuthorText ORDER BY count DESC",
                (domain, domain),
            )
        return [
            {
                "person_id": None,
                "eng_desc": row["AuthorText"],
                "heb_desc": "",
                "count": row["count"],
            }
            for row in cursor
        ]

    def get_browse_works(self, domain: str = None, author: str = None) -> list[dict]:
        """
        Get unique works from the catalog with manuscript counts.

        With v5+ sidecar, uses genizah_titles for structured title lookup.
        Falls back to sparse Title/TitleHeb for pre-v5 sidecars.

        Args:
            domain: Optional domain filter. If provided, only include manuscripts
                    that have this domain (matches Domain OR ParentDomain).
            author: Optional author filter. For v5+: person_id (int or str digit).
                    For legacy: AuthorText string.

        Returns:
            List of dicts with keys: title_id (int), org_title (str),
            eng_title (str), count (int). For legacy fallback: title_id=None,
            org_title=Title, eng_title=TitleHeb.
            Sorted by count descending.
            Returns [] if conn is None.
        """
        if self._conn is None:
            return []

        # Return cached result for unfiltered query
        if domain is None and author is None and self._works_cache is not None:
            return self._works_cache

        try:
            result = self._query_browse_works(domain, author)

            if domain is None and author is None:
                with self._works_lock:
                    if self._works_cache is not None:
                        return self._works_cache
                    self._works_cache = result
            return result
        except Exception as e:
            logger.error(f"FjmsService.get_browse_works error: {e}")
            return []

    def _query_browse_works(self, domain: str = None, author=None) -> list[dict]:
        """Execute the browse works query."""
        if self._has_persons_titles:
            return self._query_browse_works_v5(domain, author)
        return self._query_browse_works_legacy(domain, author)

    def _query_browse_works_v5(self, domain: str = None, author=None) -> list[dict]:
        """v5+ query: structured genizah_titles lookup.

        Optimization: pre-dedup catalog in CTE, use COUNT(*), IN+UNION for domain.
        """
        cte_conditions = []
        cte_params = []

        if domain is not None:
            cte_conditions.append(
                "AlmaId IN ("
                "SELECT AlmaId FROM domains WHERE Domain = ? "
                "UNION SELECT AlmaId FROM domains WHERE ParentDomain = ?)"
            )
            cte_params.extend([domain, domain])

        if author is not None:
            try:
                person_id = int(author)
                cte_conditions.append(
                    "(GenizahTitleId IN ("
                    "  SELECT gt.GenizahTitleId FROM genizah_titles gt WHERE gt.AuthorId = ?"
                    ") OR Author = ?)"
                )
                cte_params.extend([person_id, person_id])
            except (ValueError, TypeError):
                cte_conditions.append("AuthorText = ?")
                cte_params.append(author)

        cte_where = (" WHERE " + " AND ".join(cte_conditions)) if cte_conditions else ""
        sql = f"""
            WITH dc AS (
                SELECT DISTINCT AlmaId, GenizahTitleId FROM catalog{cte_where}
            )
            SELECT gt.GenizahTitleId as title_id, gt.OrgTitle as org_title,
                   gt.EngTitle as eng_title, COUNT(*) as count
            FROM dc
            INNER JOIN genizah_titles gt ON dc.GenizahTitleId = gt.GenizahTitleId
            WHERE gt.GenizahTitleId > 0
            GROUP BY gt.GenizahTitleId, gt.OrgTitle, gt.EngTitle
            ORDER BY count DESC
        """
        cursor = self._conn.execute(sql, cte_params)
        return [
            {
                "title_id": row["title_id"],
                "org_title": row["org_title"] or "",
                "eng_title": row["eng_title"] or "",
                "count": row["count"],
            }
            for row in cursor
        ]

    def _query_browse_works_legacy(self, domain: str = None, author=None) -> list[dict]:
        """Legacy query: sparse Title/TitleHeb columns."""
        conditions = [
            "(Title IS NOT NULL AND Title != '' "
            "OR TitleHeb IS NOT NULL AND TitleHeb != '')"
        ]
        params = []

        if domain is not None:
            conditions.append(
                "AlmaId IN ("
                "SELECT AlmaId FROM domains WHERE Domain = ? "
                "UNION SELECT AlmaId FROM domains WHERE ParentDomain = ?)"
            )
            params.extend([domain, domain])

        if author is not None:
            conditions.append("AuthorText = ?")
            params.append(author)

        where = " AND ".join(conditions)
        sql = (
            f"SELECT Title, TitleHeb, COUNT(DISTINCT AlmaId) as count "
            f"FROM catalog "
            f"WHERE {where} "
            f"GROUP BY Title, TitleHeb ORDER BY count DESC"
        )
        cursor = self._conn.execute(sql, params)
        return [
            {
                "title_id": None,
                "org_title": row["Title"] or "",
                "eng_title": row["TitleHeb"] or "",
                "count": row["count"],
            }
            for row in cursor
        ]

    def get_browse_results(
        self,
        domain: str = None,
        author: str = None,
        work: str = None,
        offset: int = 0,
        limit: int = 50,
        date_from: int = None,
        date_to: int = None,
        include_undated: bool = False,
        text_all: list[str] = None,
        text_any: list[str] = None,
        text_not: list[str] = None,
    ) -> dict:
        """
        Get paginated browse results matching all provided filters (intersection).

        Args:
            domain: Optional domain filter (matches Domain OR ParentDomain).
            author: Optional author filter (AuthorText = author).
            work: Optional work/title filter (Title = work).
            offset: Pagination offset.
            limit: Maximum results to return per page.
            date_from: Optional minimum year (inclusive). Records with year >= date_from.
            date_to: Optional maximum year (inclusive). Records with year <= date_to.
            include_undated: If True AND date filter is active, also include records
                with no date (CopyDate is NULL, empty, '0', or '-99').
            text_all: Terms that must ALL appear (AND). Matched across all text fields.
            text_any: Terms where ANY must appear (OR). Matched across all text fields.
            text_not: Terms that must NOT appear. Excluded across all text fields.

        Returns:
            Dict with keys:
                - results: list of dicts with keys: sys_id, title, title_heb,
                  author, copy_date, textual_frame_heb, textual_frame_eng,
                  domains, domains_heb
                - total: int (total matching count before pagination)
            Returns {"results": [], "total": 0} if conn is None.
        """
        empty = {"results": [], "total": 0}
        if self._conn is None:
            return empty

        try:
            conditions = []
            params = []

            if domain is not None:
                conditions.append(
                    "c.AlmaId IN ("
                    "SELECT AlmaId FROM domains WHERE Domain = ? "
                    "UNION SELECT AlmaId FROM domains WHERE ParentDomain = ?)"
                )
                params.extend([domain, domain])

            if author is not None:
                if self._has_persons_titles and _is_int(author):
                    # v5+: author is person_id -- match via title FK or direct Author FK
                    person_id = int(author)
                    conditions.append(
                        "(c.GenizahTitleId IN ("
                        "  SELECT gt.GenizahTitleId FROM genizah_titles gt "
                        "  WHERE gt.AuthorId = ?"
                        ") OR c.Author = ?)"
                    )
                    params.extend([person_id, person_id])
                else:
                    # Legacy: author is AuthorText string
                    conditions.append("c.AuthorText = ?")
                    params.append(author)

            if work is not None:
                if self._has_persons_titles and _is_int(work):
                    # v5+: work is GenizahTitleId
                    conditions.append("c.GenizahTitleId = ?")
                    params.append(int(work))
                else:
                    # Legacy: work is Title string
                    conditions.append("c.Title = ?")
                    params.append(work)

            # Date range filter
            has_date_filter = date_from is not None or date_to is not None
            if has_date_filter:
                _no_date = "(c.CopyDate IS NULL OR c.CopyDate = '' OR c.CopyDate = '0' OR c.CopyDate = '-99')"
                date_parts = []
                if date_from is not None and date_to is not None:
                    date_parts.append("CAST(c.CopyDate AS INTEGER) BETWEEN ? AND ?")
                    params.extend([date_from, date_to])
                elif date_from is not None:
                    date_parts.append("CAST(c.CopyDate AS INTEGER) >= ?")
                    params.append(date_from)
                else:
                    date_parts.append("CAST(c.CopyDate AS INTEGER) <= ?")
                    params.append(date_to)
                # Exclude sentinel values from the numeric range check
                dated_cond = f"(NOT {_no_date} AND {date_parts[0]})"
                if include_undated:
                    conditions.append(f"({dated_cond} OR {_no_date})")
                else:
                    conditions.append(dated_cond)

            # Free text filters: hybrid FTS5 (catalog fields) + domain name LIKE
            # FTS5 covers Title, TitleHeb, TextualFrame*, RunningTitle, FreeDescription, FullText, DetailedFrames
            # Domain LIKE covers Domain/DomainHeb names which are NOT in the FTS5 index
            def _fts_escape(term: str) -> str:
                """Escape FTS5 special characters and wrap for substring match."""
                escaped = term.replace('"', '""')
                return f'"{escaped}"'

            _TEXT_MATCH = (
                "c.rowid IN ("
                "SELECT rowid FROM catalog_fts WHERE catalog_fts MATCH ? "
                "UNION "
                "SELECT c2.rowid FROM catalog c2 "
                "INNER JOIN domains dtf ON c2.AlmaId = dtf.AlmaId "
                "WHERE dtf.Domain LIKE ? OR dtf.DomainHeb LIKE ?"
                ")"
            )
            _TEXT_NOT_MATCH = (
                "c.rowid NOT IN ("
                "SELECT rowid FROM catalog_fts WHERE catalog_fts MATCH ? "
                "UNION "
                "SELECT c2.rowid FROM catalog c2 "
                "INNER JOIN domains dtf ON c2.AlmaId = dtf.AlmaId "
                "WHERE dtf.Domain LIKE ? OR dtf.DomainHeb LIKE ?"
                ")"
            )

            if text_all:
                for term in text_all:
                    conditions.append(_TEXT_MATCH)
                    like_pat = f"%{term}%"
                    params.extend([_fts_escape(term), like_pat, like_pat])

            if text_any:
                fts_expr = " OR ".join(_fts_escape(t) for t in text_any)
                like_parts = []
                like_params = []
                for t in text_any:
                    like_parts.append("dtf.Domain LIKE ? OR dtf.DomainHeb LIKE ?")
                    like_params.extend([f"%{t}%", f"%{t}%"])
                conditions.append(
                    "c.rowid IN ("
                    "SELECT rowid FROM catalog_fts WHERE catalog_fts MATCH ? "
                    "UNION "
                    "SELECT c2.rowid FROM catalog c2 "
                    "INNER JOIN domains dtf ON c2.AlmaId = dtf.AlmaId "
                    f"WHERE {' OR '.join(like_parts)}"
                    ")"
                )
                params.append(fts_expr)
                params.extend(like_params)

            if text_not:
                for term in text_not:
                    conditions.append(_TEXT_NOT_MATCH)
                    like_pat = f"%{term}%"
                    params.extend([_fts_escape(term), like_pat, like_pat])

            where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

            # Count query
            count_sql = (
                f"SELECT COUNT(DISTINCT c.AlmaId) as total "
                f"FROM catalog c{where}"
            )
            total = self._conn.execute(count_sql, params).fetchone()["total"]

            if total == 0:
                return empty

            # Results query -- pick first non-empty value per grouped AlmaId
            # Use GROUP BY + aggregation to deduplicate
            results_sql = (
                f"SELECT c.AlmaId, "
                f"  MAX(CASE WHEN c.Title IS NOT NULL AND c.Title != '' THEN c.Title END) as Title, "
                f"  MAX(CASE WHEN c.TitleHeb IS NOT NULL AND c.TitleHeb != '' THEN c.TitleHeb END) as TitleHeb, "
                f"  MAX(CASE WHEN c.AuthorText IS NOT NULL AND c.AuthorText != '' THEN c.AuthorText END) as AuthorText, "
                f"  MAX(CASE WHEN c.CopyDate IS NOT NULL AND c.CopyDate != '' THEN c.CopyDate END) as CopyDate, "
                f"  MAX(CASE WHEN c.TextualFrameHeb IS NOT NULL AND c.TextualFrameHeb != '' THEN c.TextualFrameHeb END) as TextualFrameHeb, "
                f"  MAX(CASE WHEN c.TextualFrameEng IS NOT NULL AND c.TextualFrameEng != '' THEN c.TextualFrameEng END) as TextualFrameEng "
                f"FROM catalog c{where} "
                f"GROUP BY c.AlmaId "
                f"ORDER BY c.AlmaId "
                f"LIMIT ? OFFSET ?"
            )
            result_params = list(params) + [limit, offset]
            cursor = self._conn.execute(results_sql, result_params)
            rows = cursor.fetchall()

            # Batch-fetch domains for result sys_ids
            sys_ids = [row["AlmaId"] for row in rows]
            domains_map = self._batch_domains(sys_ids)

            results = []
            for row in rows:
                sid = row["AlmaId"]
                dom_info = domains_map.get(sid, [])
                results.append({
                    "sys_id": sid,
                    "title": row["Title"] or "",
                    "title_heb": row["TitleHeb"] or "",
                    "author": row["AuthorText"] or "",
                    "copy_date": row["CopyDate"] or "",
                    "textual_frame_heb": row["TextualFrameHeb"] or "",
                    "textual_frame_eng": row["TextualFrameEng"] or "",
                    "domains": list({d["domain"] for d in dom_info}),
                    "domains_heb": list({d["domain_heb"] for d in dom_info if d.get("domain_heb")}),
                })

            return {"results": results, "total": total}
        except Exception as e:
            logger.error(f"FjmsService.get_browse_results error: {e}")
            return empty

    def _batch_domains(self, sys_ids: list[str]) -> dict:
        """Fetch domains for a batch of sys_ids efficiently.

        Returns dict mapping sys_id -> list of {"domain": str, "domain_heb": str}.
        """
        if not self._conn or not sys_ids:
            return {}
        try:
            result = {}
            batch_size = 500
            for i in range(0, len(sys_ids), batch_size):
                batch = sys_ids[i:i + batch_size]
                placeholders = ','.join('?' * len(batch))
                cursor = self._conn.execute(
                    f"SELECT AlmaId, Domain, DomainHeb FROM domains "
                    f"WHERE AlmaId IN ({placeholders})",
                    batch,
                )
                for row in cursor:
                    sid = row["AlmaId"]
                    if sid not in result:
                        result[sid] = []
                    result[sid].append({
                        "domain": row["Domain"],
                        "domain_heb": row["DomainHeb"],
                    })
            return result
        except Exception as e:
            logger.error(f"FjmsService._batch_domains error: {e}")
            return {}

    def get_unclassified_count(self) -> int:
        """
        Get count of catalog AlmaIds that have no corresponding entry in the domains table.

        Used for showing "Unclassified" bucket in the browse UI.
        Cached after first computation (static data).

        Returns:
            Count of unclassified manuscript IDs. Returns 0 if conn is None.
        """
        if self._unclassified_cache is not None:
            return self._unclassified_cache

        if self._conn is None:
            return 0

        with self._unclassified_lock:
            if self._unclassified_cache is not None:
                return self._unclassified_cache
            try:
                cursor = self._conn.execute(
                    "SELECT COUNT(DISTINCT c.AlmaId) as count FROM catalog c "
                    "WHERE NOT EXISTS (SELECT 1 FROM domains d WHERE d.AlmaId = c.AlmaId)"
                )
                self._unclassified_cache = cursor.fetchone()["count"]
                return self._unclassified_cache
            except Exception as e:
                logger.error(f"FjmsService.get_unclassified_count error: {e}")
                return 0

    def pre_warm_caches(self):
        """Pre-compute and cache domain hierarchy, unclassified count, authors, and works.

        Uses a JSON disk cache alongside the sidecar db so only the first-ever
        launch pays the ~5s SQL cost. Subsequent launches load from disk (<50ms).
        Safe to call multiple times (no-op if already cached in memory).
        """
        if self._conn is None:
            return
        if self._load_browse_cache():
            return
        # Create performance indexes (heavy on first run, runs in background thread)
        try:
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_catalog_author ON catalog (AuthorText)")
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_catalog_title ON catalog (Title)")
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_catalog_alma ON catalog (AlmaId)")
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_domains_domain ON domains (Domain)")
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_domains_parent ON domains (ParentDomain)")
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_domains_group "
                "ON domains(Domain, ParentDomain, DomainHeb, ParentDomainHeb, AlmaId)"
            )
            # Composite indexes for domain-filtered browse queries (JOIN + WHERE)
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_domains_domain_alma ON domains (Domain, AlmaId)")
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_domains_parent_alma ON domains (ParentDomain, AlmaId)")
        except Exception:
            pass
        self.get_domain_hierarchy()
        self.get_unclassified_count()
        self.get_browse_authors()
        self.get_browse_works()
        self._save_browse_cache()

    def _browse_cache_path(self) -> Optional[str]:
        if self._db_path:
            return self._db_path + ".browse_cache.json"
        return None

    _BROWSE_CACHE_VERSION = 2  # Bump when hierarchy format changes (v2: 3-level nesting + canonical ordering)

    def _load_browse_cache(self) -> bool:
        """Load cached browse data from disk. Returns True if loaded successfully."""
        cache_path = self._browse_cache_path()
        if not cache_path or not os.path.exists(cache_path):
            return False
        try:
            db_mtime = os.path.getmtime(self._db_path)
            cache_mtime = os.path.getmtime(cache_path)
            if db_mtime > cache_mtime:
                return False  # DB newer than cache — recompute
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("version") != self._BROWSE_CACHE_VERSION:
                return False  # Format changed — recompute
            self._hierarchy_cache = data["hierarchy"]
            self._unclassified_cache = data["unclassified"]
            self._authors_cache = data["authors"]
            self._works_cache = data["works"]
            logger.info("FjmsService: loaded browse cache from disk (v%s)", self._BROWSE_CACHE_VERSION)
            return True
        except Exception as e:
            logger.debug(f"FjmsService: disk cache load failed: {e}")
            return False

    def _save_browse_cache(self):
        """Persist browse data to JSON file for fast subsequent startups."""
        cache_path = self._browse_cache_path()
        if not cache_path:
            return
        try:
            data = {
                "version": self._BROWSE_CACHE_VERSION,
                "hierarchy": self._hierarchy_cache,
                "unclassified": self._unclassified_cache,
                "authors": self._authors_cache,
                "works": self._works_cache,
            }
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
            logger.info("FjmsService: saved browse cache to disk (v%s)", self._BROWSE_CACHE_VERSION)
        except Exception as e:
            logger.debug(f"FjmsService: disk cache save failed: {e}")

    @staticmethod
    def _split_concat(val):
        """Split GROUP_CONCAT result into list of non-empty strings."""
        if not val:
            return []
        return [v.strip() for v in val.split(',') if v.strip()]

    def get_join_group(self, sys_id: str) -> list[dict]:
        """
        Get other manuscripts in the same join group(s) as the given manuscript.

        A manuscript may belong to multiple join groups. If the same partner
        appears in multiple groups, it is returned once with all distinct
        scholar names and join types aggregated into lists.

        Args:
            sys_id: The Alma/system ID for the manuscript.

        Returns:
            List of dicts with keys:
                - alma_id (str): Partner manuscript ID
                - join_group_ids (list[int]): All join group IDs containing this partner
                - scholar_names (list[str]): All distinct scholars who identified this join
                - join_types (list[str]): All distinct non-NULL join types across groups
                - comment (str or None): Comments joined with '; ' if multiple
            Returns [] if conn is None or no joins found.
        """
        if self._conn is None:
            return []
        try:
            cursor = self._conn.execute(
                "SELECT AlmaId, "
                "       GROUP_CONCAT(DISTINCT JoinGroupId) as JoinGroupIds, "
                "       GROUP_CONCAT(DISTINCT ScholarName) as ScholarNames, "
                "       GROUP_CONCAT(DISTINCT Comment) as Comments, "
                "       GROUP_CONCAT(DISTINCT JoinType) as JoinTypes "
                "FROM joins "
                "WHERE JoinGroupId IN (SELECT JoinGroupId FROM joins WHERE AlmaId = ?) "
                "  AND AlmaId != ? "
                "GROUP BY AlmaId",
                (sys_id, sys_id),
            )
            results = []
            for row in cursor:
                group_ids = self._split_concat(row["JoinGroupIds"])
                comments = self._split_concat(row["Comments"])
                results.append({
                    "alma_id": row["AlmaId"],
                    "join_group_ids": [int(g) for g in group_ids],
                    "scholar_names": self._split_concat(row["ScholarNames"]),
                    "join_types": self._split_concat(row["JoinTypes"]),
                    "comment": '; '.join(comments) if comments else None,
                })
            return results
        except Exception as e:
            logger.error(f"FjmsService.get_join_group error for {sys_id}: {e}")
            return []

    def get_catalog(self, sys_id: str) -> Optional[dict]:
        """
        Get catalog metadata for a manuscript (first record only).

        Args:
            sys_id: The Alma/system ID for the manuscript.

        Returns:
            Dict with keys: title, title_heb, author_text, copy_date, copy_place,
            textual_frame_heb, textual_frame_eng, unit_catalog_rec_id,
            num_folio, num_column, num_row, genizah_title_org, genizah_title_eng.
            Returns None if conn is None or not found.
        """
        if self._conn is None:
            return None
        try:
            cursor = self._conn.execute(
                "SELECT * FROM catalog WHERE AlmaId = ?",
                (sys_id,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            col_names = row.keys()
            return {
                "title": row["Title"],
                "title_heb": row["TitleHeb"],
                "author_text": row["AuthorText"],
                "copy_date": row["CopyDate"],
                "copy_place": row["CopyPlace"],
                "textual_frame_heb": row["TextualFrameHeb"],
                "textual_frame_eng": row["TextualFrameEng"],
                "unit_catalog_rec_id": row["UnitCatalogRecId"] if "UnitCatalogRecId" in col_names else None,
                "num_folio": row["NumFolio"] if "NumFolio" in col_names else None,
                "num_column": row["NumColumn"] if "NumColumn" in col_names else None,
                "num_row": row["NumRow"] if "NumRow" in col_names else None,
                "genizah_title_org": row["GenizahTitleOrgTitle"] if "GenizahTitleOrgTitle" in col_names else None,
                "genizah_title_eng": row["GenizahTitleEngTitle"] if "GenizahTitleEngTitle" in col_names else None,
            }
        except Exception as e:
            logger.error(f"FjmsService.get_catalog error for {sys_id}: {e}")
            return None

    # Sentinel CopyDate values that should be treated as None
    _SENTINEL_DATES = frozenset(('0', '-99', '-1', '0.0', '-99.0', '-1.0', ''))

    def get_catalog_records(self, sys_id: str) -> list[dict]:
        """Get all non-empty catalog records for a manuscript.

        Returns list of dicts. Filters out completely empty records and
        deduplicates by (textual_frame_eng, copy_date, title) tuple.
        Sentinel CopyDate values (0, -99, -1) are normalized to None.
        """
        if self._conn is None:
            return []
        try:
            cursor = self._conn.execute(
                "SELECT * FROM catalog WHERE AlmaId = ?",
                (sys_id,),
            )
            results = []
            seen = set()
            col_names = None

            for row in cursor:
                if col_names is None:
                    col_names = row.keys()

                # Normalize CopyDate sentinel values
                copy_date = row["CopyDate"]
                if copy_date is not None and str(copy_date).strip() in self._SENTINEL_DATES:
                    copy_date = None

                # Handle SourceName columns gracefully (may not exist in old sidecars)
                has_source = "SourceName" in col_names
                source_name = row["SourceName"] if has_source else None
                source_name_heb = row["SourceNameHeb"] if has_source else None

                # Handle new v3.0.0 columns gracefully (may not exist in old sidecars)
                has_rec_id = "UnitCatalogRecId" in col_names
                has_num_folio = "NumFolio" in col_names
                has_num_bifolio = "NumBifolio" in col_names
                has_num_column = "NumColumn" in col_names
                has_num_row = "NumRow" in col_names
                has_genizah_org = "GenizahTitleOrgTitle" in col_names
                has_genizah_eng = "GenizahTitleEngTitle" in col_names

                record = {
                    "title": row["Title"],
                    "title_heb": row["TitleHeb"],
                    "author_text": row["AuthorText"],
                    "copy_date": copy_date,
                    "copy_place": row["CopyPlace"],
                    "textual_frame_heb": row["TextualFrameHeb"],
                    "textual_frame_eng": row["TextualFrameEng"],
                    "source_name": source_name,
                    "source_name_heb": source_name_heb,
                    "unit_catalog_rec_id": row["UnitCatalogRecId"] if has_rec_id else None,
                    "num_folio": row["NumFolio"] if has_num_folio else None,
                    "num_bifolio": row["NumBifolio"] if has_num_bifolio else None,
                    "num_column": row["NumColumn"] if has_num_column else None,
                    "num_row": row["NumRow"] if has_num_row else None,
                    "genizah_title_org": row["GenizahTitleOrgTitle"] if has_genizah_org else None,
                    "genizah_title_eng": row["GenizahTitleEngTitle"] if has_genizah_eng else None,
                }

                # Filter completely empty records (source fields don't count)
                content_fields = (
                    record["title"], record["title_heb"], record["author_text"],
                    record["copy_date"], record["copy_place"],
                    record["textual_frame_heb"], record["textual_frame_eng"],
                    record["num_folio"], record["num_bifolio"],
                    record["num_column"], record["num_row"],
                    record["genizah_title_org"], record["genizah_title_eng"],
                )
                if not any(v and str(v).strip() for v in content_fields):
                    continue

                # Deduplicate by key tuple (include source + UCRID for multi-team records)
                key = (
                    record["textual_frame_eng"] or '',
                    record["copy_date"] or '',
                    record["title"] or '',
                    record["source_name"] or '',
                    record["unit_catalog_rec_id"] or '',
                )
                if key in seen:
                    continue
                seen.add(key)

                results.append(record)

            return results
        except Exception as e:
            logger.error(f"FjmsService.get_catalog_records error for {sys_id}: {e}")
            return []

    # ── Bibliography & Catalog Refs (Phase 33: META-03) ─────────────

    # Reference module-level constant for backward compatibility
    _GENERIC_SOURCE_NAMES = GENERIC_SOURCE_NAMES

    def get_bibliography(self, sys_id: str) -> list[dict]:
        """
        Get bibliography entries for a manuscript.

        Returns denormalized bibliography rows with resolved author/title/mention
        information, ordered with Discussion entries first, then Mentioned, then others.

        Args:
            sys_id: The Alma/system ID for the manuscript.

        Returns:
            List of dicts with keys: running_title, title_year, title_acronym,
            mention_page, from_page, to_page, volume, mention_type,
            transcription_type, translation_type, article_name,
            article_author_eng, article_author_heb, catalog_acronym.
            Returns [] if conn is None, sys_id not found, or table missing.
        """
        if self._conn is None:
            return []
        try:
            cursor = self._conn.execute(
                "SELECT * FROM bibliography WHERE AlmaId = ? "
                "ORDER BY CASE MentionType "
                "WHEN 'Discussion' THEN 0 "
                "WHEN 'Mentioned' THEN 1 "
                "ELSE 2 END, RunningTitle",
                (sys_id,),
            )
            return [
                {
                    "running_title": row["RunningTitle"],
                    "title_year": row["TitleYear"],
                    "title_acronym": row["TitleAcronym"],
                    "mention_page": row["MentionPage"],
                    "from_page": row["FromPage"],
                    "to_page": row["ToPage"],
                    "volume": row["Volume"],
                    "mention_type": row["MentionType"],
                    "transcription_type": row["TranscriptionType"],
                    "translation_type": row["TranslationType"],
                    "article_name": row["ArticleName"],
                    "article_author_eng": row["ArticleAuthorEng"],
                    "article_author_heb": row["ArticleAuthorHeb"],
                    "catalog_acronym": row["CatalogAcronym"],
                }
                for row in cursor
            ]
        except Exception as e:
            logger.error(f"FjmsService.get_bibliography error for {sys_id}: {e}")
            return []

    def get_catalog_refs(self, sys_id: str) -> list[dict]:
        """
        Get catalog cross-references for a manuscript.

        Returns entries linking the manuscript to scholarly catalogs
        (e.g., Goitein Med Soc, Gil Palestine).

        Args:
            sys_id: The Alma/system ID for the manuscript.

        Returns:
            List of dicts with keys: cat_acronym, catalog_author,
            catalog_title, catalog_entry, is_source.
            Returns [] if conn is None, sys_id not found, or table missing.
        """
        if self._conn is None:
            return []
        try:
            cursor = self._conn.execute(
                "SELECT * FROM catalog_refs WHERE AlmaId = ? "
                "ORDER BY CatAcronym, CatalogEntry",
                (sys_id,),
            )
            return [
                {
                    "cat_acronym": row["CatAcronym"],
                    "catalog_author": row["CatalogAuthor"],
                    "catalog_title": row["CatalogTitle"],
                    "catalog_entry": row["CatalogEntry"],
                    "is_source": row["IsSource"],
                }
                for row in cursor
            ]
        except Exception as e:
            logger.error(f"FjmsService.get_catalog_refs error for {sys_id}: {e}")
            return []

    def get_source_names(self, sys_id: str) -> list[str]:
        """
        Get distinct scholarly source names for a manuscript.

        Queries the catalog table for SourceName values, filtering out
        generic labels like 'Catalogs', 'Institution', 'Collection', 'Other'.

        Args:
            sys_id: The Alma/system ID for the manuscript.

        Returns:
            List of non-generic SourceName strings.
            Returns [] if conn is None, sys_id not found, or table missing.
        """
        if self._conn is None:
            return []
        try:
            cursor = self._conn.execute(
                "SELECT DISTINCT SourceName FROM catalog "
                "WHERE AlmaId = ? AND SourceName IS NOT NULL AND SourceName != ''",
                (sys_id,),
            )
            return [
                row["SourceName"]
                for row in cursor
                if row["SourceName"] not in self._GENERIC_SOURCE_NAMES
            ]
        except Exception as e:
            logger.error(f"FjmsService.get_source_names error for {sys_id}: {e}")
            return []

    def get_catalog_source_counts(self, sys_ids: list[str]) -> dict[str, int]:
        """
        Get distinct catalog source counts for multiple manuscripts in batch.

        Used for search card button labels: "Catalog Records (N)".
        Excludes generic source names (Inventory, Nuscha, Institution, Collection, Other).

        Args:
            sys_ids: List of Alma/system IDs.

        Returns:
            Dict mapping sys_id -> count of distinct non-generic SourceName values.
            IDs with no catalog data are omitted (not present in result).
        """
        if not self._conn or not sys_ids:
            return {}
        try:
            result = {}
            batch_size = 500
            for i in range(0, len(sys_ids), batch_size):
                batch = sys_ids[i:i + batch_size]
                placeholders = ','.join('?' * len(batch))
                cursor = self._conn.execute(
                    f"SELECT AlmaId, COUNT(DISTINCT SourceName) as cnt FROM catalog "
                    f"WHERE AlmaId IN ({placeholders}) "
                    f"AND SourceName IS NOT NULL AND SourceName != '' "
                    f"AND SourceName NOT IN ('Inventory','Nuscha','Institution','Instatution','Collection','Other') "
                    f"GROUP BY AlmaId",
                    batch,
                )
                for row in cursor:
                    result[row["AlmaId"]] = row["cnt"]
            return result
        except Exception as e:
            logger.error(f"FjmsService.get_catalog_source_counts error: {e}")
            return {}

    def get_catalog_detail(self, sys_id: str) -> dict:
        """
        Get structured catalog detail for the dialog display.

        Returns all catalog data for a manuscript grouped by child table:
        records, running titles, sizes, fields, free descriptions,
        full texts, textual frames, and mentions.

        Args:
            sys_id: The Alma/system ID for the manuscript.

        Returns:
            Dict with keys:
                - records: list of catalog record dicts (from get_catalog_records)
                - running_titles: dict mapping UnitCatalogRecId -> list of
                    {"running_title": str, "comment": str}
                - sizes: dict mapping UnitCatalogRecId -> list of
                    {"size_x": float, "size_y": float, "inner_size_x": float, "inner_size_y": float}
                - fields: dict mapping UnitCatalogRecId -> {FieldCategory: [{"value": str, "value_heb": str}]}
                - free_descriptions: list of {"text": str, "signature_id": int}
                - full_texts: list of {"text": str, "signature_id": int}
                - textual_frames: dict mapping UnitCatalogRecId -> list of
                    {"heb": str, "eng": str}
                - mentions: dict mapping UnitCatalogRecId -> list of
                    {"mention_type": str, "mention": str, "mention_desc": str}
        """
        empty = {
            "records": [],
            "running_titles": {},
            "sizes": {},
            "fields": {},
            "free_descriptions": [],
            "full_texts": [],
            "textual_frames": {},
            "mentions": {},
        }
        if self._conn is None:
            return empty

        # 1. Catalog records
        records = self.get_catalog_records(sys_id)

        # 2. Running titles
        running_titles = {}
        try:
            cursor = self._conn.execute(
                "SELECT UnitCatalogRecId, RunningTitle, Comment "
                "FROM catalog_running_titles WHERE AlmaId = ?",
                (sys_id,),
            )
            for row in cursor:
                rec_id = row["UnitCatalogRecId"]
                if rec_id not in running_titles:
                    running_titles[rec_id] = []
                running_titles[rec_id].append({
                    "running_title": row["RunningTitle"],
                    "comment": row["Comment"],
                })
        except Exception as e:
            logger.debug(f"FjmsService.get_catalog_detail running_titles error for {sys_id}: {e}")

        # 3. Sizes
        sizes = {}
        try:
            cursor = self._conn.execute(
                "SELECT UnitCatalogRecId, SizeX, SizeY, InnerSizeX, InnerSizeY "
                "FROM catalog_sizes WHERE AlmaId = ?",
                (sys_id,),
            )
            for row in cursor:
                rec_id = row["UnitCatalogRecId"]
                if rec_id not in sizes:
                    sizes[rec_id] = []
                sizes[rec_id].append({
                    "size_x": row["SizeX"],
                    "size_y": row["SizeY"],
                    "inner_size_x": row["InnerSizeX"],
                    "inner_size_y": row["InnerSizeY"],
                })
        except Exception as e:
            logger.debug(f"FjmsService.get_catalog_detail sizes error for {sys_id}: {e}")

        # 4. Fields (grouped by UnitCatalogRecId then FieldCategory)
        fields = {}
        try:
            cursor = self._conn.execute(
                "SELECT UnitCatalogRecId, FieldCategory, FieldValue, FieldValueHeb "
                "FROM catalog_fields WHERE AlmaId = ?",
                (sys_id,),
            )
            for row in cursor:
                rec_id = row["UnitCatalogRecId"]
                category = row["FieldCategory"]
                if rec_id not in fields:
                    fields[rec_id] = {}
                if category not in fields[rec_id]:
                    fields[rec_id][category] = []
                fields[rec_id][category].append({
                    "value": row["FieldValue"],
                    "value_heb": row["FieldValueHeb"],
                })
        except Exception as e:
            logger.debug(f"FjmsService.get_catalog_detail fields error for {sys_id}: {e}")

        # 5. Free descriptions
        free_descriptions = []
        try:
            # Try with SourceName columns first (v4.1.0+ sidecar)
            cursor = self._conn.execute(
                "SELECT SignatureId, FreeDesc, SourceName, SourceNameHeb "
                "FROM catalog_free_desc WHERE AlmaId = ?",
                (sys_id,),
            )
            for row in cursor:
                free_descriptions.append({
                    "text": row["FreeDesc"],
                    "signature_id": row["SignatureId"],
                    "source_name": row["SourceName"],
                    "source_name_heb": row["SourceNameHeb"],
                })
        except Exception:
            # Fallback: old sidecar without SourceName columns
            free_descriptions = []
            try:
                cursor = self._conn.execute(
                    "SELECT SignatureId, FreeDesc "
                    "FROM catalog_free_desc WHERE AlmaId = ?",
                    (sys_id,),
                )
                for row in cursor:
                    free_descriptions.append({
                        "text": row["FreeDesc"],
                        "signature_id": row["SignatureId"],
                        "source_name": None,
                        "source_name_heb": None,
                    })
            except Exception as e:
                logger.debug(f"FjmsService.get_catalog_detail free_desc error for {sys_id}: {e}")

        # 6. Full texts (v4.0.0+, may not exist in older sidecars)
        full_texts = []
        try:
            cursor = self._conn.execute(
                "SELECT SignatureId, FullText "
                "FROM catalog_full_texts WHERE AlmaId = ?",
                (sys_id,),
            )
            for row in cursor:
                text = row["FullText"]
                if text and str(text).strip():
                    full_texts.append({
                        "text": row["FullText"],
                        "signature_id": row["SignatureId"],
                    })
        except Exception as e:
            logger.debug(f"FjmsService.get_catalog_detail full_texts error for {sys_id}: {e}")

        # 7. Detailed textual frames (v4.0.0+, may not exist in older sidecars)
        textual_frames = {}
        try:
            cursor = self._conn.execute(
                "SELECT UnitCatalogRecId, TextualFrameHeb, TextualFrameEng "
                "FROM catalog_textual_frames WHERE AlmaId = ?",
                (sys_id,),
            )
            for row in cursor:
                rec_id = row["UnitCatalogRecId"]
                if rec_id not in textual_frames:
                    textual_frames[rec_id] = []
                textual_frames[rec_id].append({
                    "heb": row["TextualFrameHeb"],
                    "eng": row["TextualFrameEng"],
                })
        except Exception as e:
            logger.debug(f"FjmsService.get_catalog_detail textual_frames error for {sys_id}: {e}")

        # 8. Mentions (v4.0.0+, may not exist in older sidecars)
        mentions = {}
        try:
            cursor = self._conn.execute(
                "SELECT UnitCatalogRecId, MentionType, Mention, MentionDesc "
                "FROM catalog_mentions WHERE AlmaId = ?",
                (sys_id,),
            )
            for row in cursor:
                rec_id = row["UnitCatalogRecId"]
                if rec_id not in mentions:
                    mentions[rec_id] = []
                mentions[rec_id].append({
                    "mention_type": row["MentionType"],
                    "mention": row["Mention"],
                    "mention_desc": row["MentionDesc"],
                })
        except Exception as e:
            logger.debug(f"FjmsService.get_catalog_detail mentions error for {sys_id}: {e}")

        return {
            "records": records,
            "running_titles": running_titles,
            "sizes": sizes,
            "fields": fields,
            "free_descriptions": free_descriptions,
            "full_texts": full_texts,
            "textual_frames": textual_frames,
            "mentions": mentions,
        }

    def close(self):
        """Close the database connection if open."""
        if self._conn is not None:
            try:
                self._conn.close()
                logger.info("FjmsService: Connection closed")
            except Exception as e:
                logger.error(f"FjmsService.close error: {e}")
            finally:
                self._conn = None


def format_page_ref(entry: dict) -> str:
    """Format page reference from FJMS bibliography entry fields.

    Handles mention_page, from_page/to_page ranges, and volume prefixes.

    Args:
        entry: Dict with keys mention_page, from_page, to_page, volume.

    Returns:
        Formatted page reference string (e.g., 'vol. 2, pp. 15-20') or ''.
    """
    parts = []
    vol = entry.get('volume', '')
    if vol and str(vol).strip():
        parts.append(f'vol. {vol}')
    mention_page = entry.get('mention_page', '')
    from_page = entry.get('from_page', '')
    to_page = entry.get('to_page', '')
    if mention_page and str(mention_page).strip():
        parts.append(f'p. {mention_page}')
    elif from_page and str(from_page).strip():
        if to_page and str(to_page).strip() and str(to_page) != str(from_page):
            parts.append(f'pp. {from_page}-{to_page}')
        else:
            parts.append(f'p. {from_page}')
    return ', '.join(parts)


def _parse_marc_annotations(marc_str: str) -> dict:
    """Parse Hebrew annotations from end of NLI MARC 581 string.

    NLI MARC strings end with parenthetical Hebrew annotations derived from
    FJMS data, e.g.: '(דיון, יש תמונה, יש העתקה (מלא), יש תרגום (מלא)).'

    Args:
        marc_str: Raw MARC bibliography string.

    Returns:
        Dict with keys: mention_type, has_image, transcription, translation.
    """
    result = {'mention_type': '', 'has_image': False, 'transcription': '', 'translation': ''}
    if not marc_str:
        return result

    # Find the last parenthetical block containing Hebrew annotations
    # Pattern: content in parens that contains Hebrew chars, possibly nested parens
    match = re.search(r'\(([^()]*(?:\([^()]*\)[^()]*)*)\)\s*\.?\s*$', marc_str)
    if not match:
        return result

    annotation = match.group(1)

    # Mention type
    if 'דיון' in annotation:
        result['mention_type'] = 'Discussion'
    elif 'איזכור' in annotation:
        result['mention_type'] = 'Mentioned'
    elif 'מפתח' in annotation:
        result['mention_type'] = 'Index'

    # Has image
    if 'יש תמונה' in annotation or 'תמונה' in annotation:
        result['has_image'] = True

    # Transcription
    if 'יש העתקה' in annotation:
        if 'העתקה (מלא)' in annotation:
            result['transcription'] = 'Full'
        elif 'העתקה (חלקי)' in annotation:
            result['transcription'] = 'Partial'
        else:
            result['transcription'] = 'Exists'

    # Translation
    if 'יש תרגום' in annotation:
        if 'תרגום (מלא)' in annotation:
            result['translation'] = 'Full'
        elif 'תרגום (חלקי)' in annotation:
            result['translation'] = 'Partial'
        else:
            result['translation'] = 'Exists'

    return result


def strip_marc_annotation_suffix(marc_str: str) -> str:
    """Strip trailing Hebrew annotation parenthetical from MARC 581 string.

    NLI MARC strings end with '(דיון, יש תמונה, ...).' — this returns
    the clean reference text for display in the NLI bibliography table.

    Args:
        marc_str: Raw MARC bibliography string.

    Returns:
        Reference text with trailing annotation removed, or original string.
    """
    if not marc_str:
        return ''
    s = marc_str.strip()
    # Remove trailing period
    if s.endswith('.'):
        s = s[:-1].rstrip()
    # Remove last parenthetical block that contains Hebrew chars
    cleaned = re.sub(r'\s*\(([^()]*(?:\([^()]*\)[^()]*)*)\)\s*$', '', s)
    # Only strip if the removed part actually contained Hebrew
    if cleaned != s:
        removed = s[len(cleaned):]
        if re.search(r'[\u0590-\u05FF]', removed):
            return cleaned.rstrip(' .,;-')
    return s


def _ts_symbol(value) -> str:
    """Map transcription/translation value to FJMS-style symbol.

    Full → '✓+', Partial → '✓−', truthy/Exists → '✓', None/empty → ''.
    """
    if not value or str(value).strip() in ('', 'None', 'Unknown'):
        return ''
    v = str(value).strip()
    if v == 'Full':
        return '\u2713+'
    if v == 'Partial':
        return '\u2713\u2212'
    return '\u2713'


def _parse_marc_bib_string(marc_str: str) -> dict:
    """Parse an NLI MARC 581 bibliography string into structured fields.

    Extracts author (text before first comma), 4-digit year, page numbers,
    title, and Hebrew annotations from the raw MARC string.

    Args:
        marc_str: Raw bibliography string from MARC tag 581.

    Returns:
        Dict with keys: author, year, pages, title, plus annotation fields.
    """
    result = {'author': '', 'year': '', 'pages': '', 'title': ''}
    if not marc_str or not marc_str.strip():
        return result

    s = marc_str.strip()

    # Extract author: text before first comma (if not too long)
    comma_idx = s.find(',')
    if 0 < comma_idx <= 60:
        result['author'] = s[:comma_idx].strip()

    # Extract title: text between author section and year/page section
    # Pattern: "Author, Article Title. Book/Journal Title, Year, ..."
    # Try to find text after author+article that looks like a title
    title_match = re.search(r'(?:,\s+[^,]+)?\.\s+([^,.]+?)(?:\.\s|\,\s*\d{4}|\,\s*\d+\s*עמ)', s)
    if title_match:
        candidate = title_match.group(1).strip()
        # Only use if it's not too short and not just a year
        if len(candidate) > 3 and not re.match(r'^\d{4}$', candidate):
            result['title'] = candidate

    # Extract 4-digit year
    year_match = re.search(r'\b(1[4-9]\d{2}|20[0-2]\d)\b', s)
    if year_match:
        result['year'] = year_match.group(1)

    # Extract page references - Hebrew patterns first (more common in MARC)
    heb_match = re.search(r"עמ(?:וד|['\u2019])\s*([\w\d,/ -–]+?)(?:\s*\(|$)", s)
    if heb_match:
        result['pages'] = heb_match.group(1).strip().rstrip('.')
    else:
        # English patterns
        page_match = re.search(r'(?:pp?\.\s*|pages?\s+)(\d+(?:\s*[-–]\s*\d+)?)', s)
        if page_match:
            result['pages'] = page_match.group(1).strip()

    # Parse Hebrew annotations
    annotations = _parse_marc_annotations(s)
    result.update(annotations)

    return result


def merge_catalog_records(records: list[dict]) -> dict:
    """Merge multiple catalog records into a display-ready structure.

    Metadata fields (title, author, date, place) are merged by taking
    the first non-empty value. TextualFrame entries are collected as
    a list of distinct values with their source attribution.

    Args:
        records: List of catalog record dicts from get_catalog_records().

    Returns:
        Dict with keys: title, title_heb, author_text, copy_date, copy_place,
        textual_frames (list of dicts), record_count (int).
    """
    if not records:
        return {
            "title": None, "title_heb": None, "author_text": None,
            "copy_date": None, "copy_place": None,
            "textual_frames": [], "record_count": 0,
        }

    result = {
        "title": None,
        "title_heb": None,
        "author_text": None,
        "copy_date": None,
        "copy_place": None,
    }

    # Take first non-empty value for each metadata field
    for rec in records:
        for key in ("title", "title_heb", "author_text", "copy_date", "copy_place"):
            if result[key] is None and rec.get(key) and str(rec[key]).strip():
                result[key] = rec[key]

    # Collect distinct TextualFrame entries with source attribution
    # TextualFrame fields can contain multiple entries separated by '; @[$'
    frames = []
    seen_frames = set()
    for rec in records:
        eng_text = rec.get("textual_frame_eng") or ''
        heb_text = rec.get("textual_frame_heb") or ''
        if not eng_text.strip() and not heb_text.strip():
            continue
        eng_parts = split_textual_frames(eng_text)
        heb_parts = split_textual_frames(heb_text)
        max_len = max(len(eng_parts), len(heb_parts), 1)
        # If no parts from split (plain text without [$...$]), use original
        if not eng_parts and not heb_parts:
            eng_parts = [eng_text.strip()] if eng_text.strip() else []
            heb_parts = [heb_text.strip()] if heb_text.strip() else []
            max_len = max(len(eng_parts), len(heb_parts))
        for i in range(max_len):
            eng = eng_parts[i].strip() if i < len(eng_parts) else None
            heb = heb_parts[i].strip() if i < len(heb_parts) else None
            if not (eng or heb):
                continue
            frame_key = (eng or '', heb or '')
            if frame_key in seen_frames:
                continue
            seen_frames.add(frame_key)
            frames.append({
                "eng": eng if eng else None,
                "heb": heb if heb else None,
                "source_name": rec.get("source_name"),
                "source_name_heb": rec.get("source_name_heb"),
            })

    result["textual_frames"] = frames
    result["record_count"] = len(records)

    return result


def split_textual_frames(text: str) -> list[str]:
    """Split a compound TextualFrame string into individual entries.

    FJMS TextualFrame fields can contain multiple entries separated by '; '
    where each entry starts with @[$Category$] or [$Category$] notation.
    E.g.: '@[$Piyyut$]: "poem1"; @[$Piyyut$] (Yotzer): "poem2"'
    """
    if not text or not text.strip():
        return []
    # Split on '; ' followed by optional @ then [$
    parts = re.split(r';\s*(?=@?\[\$)', text.strip())
    return [p.strip() for p in parts if p.strip()]


def parse_textual_frame(text: str) -> tuple[str, str]:
    """Parse '[$Category$]: Content' notation into (category, content).

    Strips optional @ prefix. Captures parenthetical sub-type as part of
    category (e.g., '[$Piyyut$] (Yotzer)' -> category='Piyyut (Yotzer)').
    Returns ('', full_text) if no pattern match.

    Args:
        text: A single textual frame entry (use split_textual_frames first
              to split compound strings).

    Returns:
        Tuple of (category, content). Category is '' if no pattern match.
    """
    if not text:
        return ('', '')
    text = text.strip().lstrip('@')
    match = re.match(r'\[\$(.+?)\$\]\s*(\([^)]+\))?\s*:?\s*(.*)', text, re.DOTALL)
    if match:
        category = match.group(1).strip()
        sub_type = match.group(2)
        content = match.group(3).strip()
        if sub_type:
            category = f"{category} {sub_type}"
        return (category, content)
    return ('', text)


# Module-level singleton pattern
_default_service: Optional[FjmsService] = None


def get_fjms_service(thread_safe: bool = True) -> FjmsService:
    """Get or create the default FjmsService singleton."""
    global _default_service
    if _default_service is None:
        _default_service = FjmsService(thread_safe=thread_safe)
    return _default_service


def reset_fjms_service():
    """Reset the singleton FjmsService instance.

    Call this after replacing the fjms_enrichment.db sidecar file to force
    re-initialization on next access. Closes the existing connection
    before clearing the singleton.
    """
    global _default_service
    if _default_service is not None:
        _default_service.close()
        _default_service = None
