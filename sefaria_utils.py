# -*- coding: utf-8 -*-
"""
Sefaria utilities for text filtering.

This module contains shared utilities for working with Sefaria texts
that do NOT require PyQt6 (safe for web server deployment).
"""

import os
import json
import time
import re
import html as html_module

import requests


def get_cache_dir():
    """Get or create the cache directory for Sefaria texts."""
    cache_dir = os.path.join(os.path.expanduser("~"), ".genizah_search", "sefaria_cache")
    os.makedirs(cache_dir, exist_ok=True)
    return cache_dir


def clean_hebrew_text(text):
    """Remove nikud, taamim and non-alphabetic characters from Hebrew text.

    Keeps only Hebrew letters (א-ת) and basic whitespace.
    """
    # First decode HTML entities like &nbsp; {ס} etc.
    text = html_module.unescape(text)
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    # Remove content in curly braces like {ס} {פ}
    text = re.sub(r'\{[^}]*\}', '', text)
    # Replace maqaf (Hebrew hyphen ־) and regular hyphen with space
    text = re.sub(r'[\u05BE\-]', ' ', text)
    # Keep only Hebrew letters (א-ת) and spaces - this removes nikud, taamim, and everything else
    # Hebrew letter range: \u05D0-\u05EA (א-ת)
    text = re.sub(r'[^\u05D0-\u05EA\s]', '', text)
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


class SefariaLibraryManager:
    """Manages the Sefaria library table of contents with local caching."""

    TOC_URL = "https://www.sefaria.org/api/index/"
    CACHE_TTL_DAYS = 7

    def __init__(self):
        self.toc = None
        self._cache_file = os.path.join(get_cache_dir(), "sefaria_toc.json")

    def get_toc(self):
        """Get the full table of contents, loading from cache or API as needed."""
        if self.toc is not None:
            return self.toc

        # Try loading from cache
        if self._cache_is_valid():
            cached = self._load_from_cache()
            if cached:
                self.toc = cached
                return self.toc

        # Fetch from API
        self.toc = self._fetch_from_api()
        if self.toc:
            self._save_to_cache()

        return self.toc

    def _cache_is_valid(self):
        """Check if cache exists and is not expired."""
        if not os.path.exists(self._cache_file):
            return False
        try:
            mtime = os.path.getmtime(self._cache_file)
            age_days = (time.time() - mtime) / (60 * 60 * 24)
            return age_days < self.CACHE_TTL_DAYS
        except Exception:
            return False

    def _load_from_cache(self):
        """Load TOC from local cache file."""
        try:
            with open(self._cache_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading Sefaria TOC cache: {e}")
            return None

    def _save_to_cache(self):
        """Save TOC to local cache file."""
        try:
            os.makedirs(os.path.dirname(self._cache_file), exist_ok=True)
            with open(self._cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.toc, f, ensure_ascii=False)
        except Exception as e:
            print(f"Error saving Sefaria TOC cache: {e}")

    def _fetch_from_api(self):
        """Fetch the full TOC from Sefaria API."""
        try:
            resp = requests.get(self.TOC_URL, timeout=30)
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            print(f"Error fetching Sefaria TOC: {e}")
        return None

    def get_categories(self):
        """Get top-level categories."""
        toc = self.get_toc()
        if not toc:
            return []
        return toc

    def get_texts_recursive(self, node, max_depth=10):
        """Recursively get all texts from a category node."""
        texts = []
        if max_depth <= 0:
            return texts

        if isinstance(node, list):
            for item in node:
                texts.extend(self.get_texts_recursive(item, max_depth - 1))
        elif isinstance(node, dict):
            # If it has 'title', it's a text
            if 'title' in node and 'contents' not in node:
                texts.append({
                    'title': node.get('title', ''),
                    'heTitle': node.get('heTitle', node.get('title', '')),
                    'categories': node.get('categories', [])
                })
            # If it has 'contents', recurse
            if 'contents' in node:
                texts.extend(self.get_texts_recursive(node['contents'], max_depth - 1))

        return texts


# Singleton instance
_sefaria_library = None


def get_sefaria_library():
    """Get the singleton SefariaLibraryManager instance."""
    global _sefaria_library
    if _sefaria_library is None:
        _sefaria_library = SefariaLibraryManager()
    return _sefaria_library


# Legacy SEFARIA_SOURCES dict for backwards compatibility with quick presets
# Sefaria text indices organized by category
SEFARIA_SOURCES = {
    "tanakh": {
        "name": "תנ\"ך",
        "books": {
            "torah": {
                "name": "תורה",
                "refs": ["Genesis", "Exodus", "Leviticus", "Numbers", "Deuteronomy"],
                "he_names": ["בראשית", "שמות", "ויקרא", "במדבר", "דברים"]
            },
            "neviim": {
                "name": "נביאים",
                "refs": ["Joshua", "Judges", "I Samuel", "II Samuel", "I Kings", "II Kings",
                        "Isaiah", "Jeremiah", "Ezekiel", "Hosea", "Joel", "Amos", "Obadiah",
                        "Jonah", "Micah", "Nahum", "Habakkuk", "Zephaniah", "Haggai",
                        "Zechariah", "Malachi"],
                "he_names": ["יהושע", "שופטים", "שמואל א", "שמואל ב", "מלכים א", "מלכים ב",
                            "ישעיהו", "ירמיהו", "יחזקאל", "הושע", "יואל", "עמוס", "עובדיה",
                            "יונה", "מיכה", "נחום", "חבקוק", "צפניה", "חגי", "זכריה", "מלאכי"]
            },
            "ketuvim": {
                "name": "כתובים",
                "refs": ["Psalms", "Proverbs", "Job", "Song of Songs", "Ruth", "Lamentations",
                        "Ecclesiastes", "Esther", "Daniel", "Ezra", "Nehemiah",
                        "I Chronicles", "II Chronicles"],
                "he_names": ["תהלים", "משלי", "איוב", "שיר השירים", "רות", "איכה",
                            "קהלת", "אסתר", "דניאל", "עזרא", "נחמיה", "דברי הימים א", "דברי הימים ב"]
            }
        }
    },
    "mishnah": {
        "name": "משנה",
        "books": {
            "zeraim": {
                "name": "זרעים",
                "refs": ["Mishnah Berakhot", "Mishnah Peah", "Mishnah Demai", "Mishnah Kilayim",
                        "Mishnah Sheviit", "Mishnah Terumot", "Mishnah Maasrot", "Mishnah Maaser Sheni",
                        "Mishnah Challah", "Mishnah Orlah", "Mishnah Bikkurim"],
                "he_names": ["ברכות", "פאה", "דמאי", "כלאים", "שביעית", "תרומות",
                            "מעשרות", "מעשר שני", "חלה", "ערלה", "ביכורים"]
            },
            "moed": {
                "name": "מועד",
                "refs": ["Mishnah Shabbat", "Mishnah Eruvin", "Mishnah Pesachim", "Mishnah Shekalim",
                        "Mishnah Yoma", "Mishnah Sukkah", "Mishnah Beitzah", "Mishnah Rosh Hashanah",
                        "Mishnah Taanit", "Mishnah Megillah", "Mishnah Moed Katan", "Mishnah Chagigah"],
                "he_names": ["שבת", "עירובין", "פסחים", "שקלים", "יומא", "סוכה",
                            "ביצה", "ראש השנה", "תענית", "מגילה", "מועד קטן", "חגיגה"]
            },
            "nashim": {
                "name": "נשים",
                "refs": ["Mishnah Yevamot", "Mishnah Ketubot", "Mishnah Nedarim", "Mishnah Nazir",
                        "Mishnah Sotah", "Mishnah Gittin", "Mishnah Kiddushin"],
                "he_names": ["יבמות", "כתובות", "נדרים", "נזיר", "סוטה", "גיטין", "קידושין"]
            },
            "nezikin": {
                "name": "נזיקין",
                "refs": ["Mishnah Bava Kamma", "Mishnah Bava Metzia", "Mishnah Bava Batra",
                        "Mishnah Sanhedrin", "Mishnah Makkot", "Mishnah Shevuot", "Mishnah Eduyot",
                        "Mishnah Avodah Zarah", "Pirkei Avot", "Mishnah Horayot"],
                "he_names": ["בבא קמא", "בבא מציעא", "בבא בתרא", "סנהדרין", "מכות",
                            "שבועות", "עדיות", "עבודה זרה", "אבות", "הוריות"]
            },
            "kodashim": {
                "name": "קדשים",
                "refs": ["Mishnah Zevachim", "Mishnah Menachot", "Mishnah Chullin", "Mishnah Bekhorot",
                        "Mishnah Arakhin", "Mishnah Temurah", "Mishnah Keritot", "Mishnah Meilah",
                        "Mishnah Tamid", "Mishnah Middot", "Mishnah Kinnim"],
                "he_names": ["זבחים", "מנחות", "חולין", "בכורות", "ערכין",
                            "תמורה", "כריתות", "מעילה", "תמיד", "מידות", "קינים"]
            },
            "tahorot": {
                "name": "טהרות",
                "refs": ["Mishnah Kelim", "Mishnah Oholot", "Mishnah Negaim", "Mishnah Parah",
                        "Mishnah Tahorot", "Mishnah Mikvaot", "Mishnah Niddah", "Mishnah Makhshirin",
                        "Mishnah Zavim", "Mishnah Tevul Yom", "Mishnah Yadayim", "Mishnah Oktzin"],
                "he_names": ["כלים", "אהלות", "נגעים", "פרה", "טהרות", "מקוואות",
                            "נידה", "מכשירין", "זבים", "טבול יום", "ידיים", "עוקצין"]
            }
        }
    },
    "talmud": {
        "name": "תלמוד בבלי",
        "books": {
            "zeraim": {
                "name": "זרעים",
                "refs": ["Berakhot"],
                "he_names": ["ברכות"]
            },
            "moed": {
                "name": "מועד",
                "refs": ["Shabbat", "Eruvin", "Pesachim", "Yoma", "Sukkah", "Beitzah",
                        "Rosh Hashanah", "Taanit", "Megillah", "Moed Katan", "Chagigah"],
                "he_names": ["שבת", "עירובין", "פסחים", "יומא", "סוכה", "ביצה",
                            "ראש השנה", "תענית", "מגילה", "מועד קטן", "חגיגה"]
            },
            "nashim": {
                "name": "נשים",
                "refs": ["Yevamot", "Ketubot", "Nedarim", "Nazir", "Sotah", "Gittin", "Kiddushin"],
                "he_names": ["יבמות", "כתובות", "נדרים", "נזיר", "סוטה", "גיטין", "קידושין"]
            },
            "nezikin": {
                "name": "נזיקין",
                "refs": ["Bava Kamma", "Bava Metzia", "Bava Batra", "Sanhedrin", "Makkot",
                        "Shevuot", "Avodah Zarah", "Horayot"],
                "he_names": ["בבא קמא", "בבא מציעא", "בבא בתרא", "סנהדרין", "מכות",
                            "שבועות", "עבודה זרה", "הוריות"]
            },
            "kodashim": {
                "name": "קדשים",
                "refs": ["Zevachim", "Menachot", "Chullin", "Bekhorot", "Arakhin",
                        "Temurah", "Keritot", "Meilah", "Tamid"],
                "he_names": ["זבחים", "מנחות", "חולין", "בכורות", "ערכין",
                            "תמורה", "כריתות", "מעילה", "תמיד"]
            },
            "tahorot": {
                "name": "טהרות",
                "refs": ["Niddah"],
                "he_names": ["נידה"]
            }
        }
    },
    "tosefta": {
        "name": "תוספתא",
        "books": {
            "zeraim": {
                "name": "זרעים",
                "refs": ["Tosefta Berakhot", "Tosefta Peah", "Tosefta Demai", "Tosefta Kilayim",
                        "Tosefta Sheviit", "Tosefta Terumot", "Tosefta Maasrot", "Tosefta Maaser Sheni",
                        "Tosefta Challah", "Tosefta Orlah", "Tosefta Bikkurim"],
                "he_names": ["ברכות", "פאה", "דמאי", "כלאים", "שביעית", "תרומות",
                            "מעשרות", "מעשר שני", "חלה", "ערלה", "ביכורים"]
            },
            "moed": {
                "name": "מועד",
                "refs": ["Tosefta Shabbat", "Tosefta Eruvin", "Tosefta Pesachim", "Tosefta Shekalim",
                        "Tosefta Yoma", "Tosefta Sukkah", "Tosefta Beitzah", "Tosefta Rosh Hashanah",
                        "Tosefta Taanit", "Tosefta Megillah", "Tosefta Moed Katan", "Tosefta Chagigah"],
                "he_names": ["שבת", "עירובין", "פסחים", "שקלים", "יומא", "סוכה",
                            "ביצה", "ראש השנה", "תענית", "מגילה", "מועד קטן", "חגיגה"]
            },
            "nashim": {
                "name": "נשים",
                "refs": ["Tosefta Yevamot", "Tosefta Ketubot", "Tosefta Nedarim", "Tosefta Nazir",
                        "Tosefta Sotah", "Tosefta Gittin", "Tosefta Kiddushin"],
                "he_names": ["יבמות", "כתובות", "נדרים", "נזיר", "סוטה", "גיטין", "קידושין"]
            },
            "nezikin": {
                "name": "נזיקין",
                "refs": ["Tosefta Bava Kamma", "Tosefta Bava Metzia", "Tosefta Bava Batra",
                        "Tosefta Sanhedrin", "Tosefta Makkot", "Tosefta Shevuot", "Tosefta Eduyot",
                        "Tosefta Avodah Zarah", "Tosefta Horayot"],
                "he_names": ["בבא קמא", "בבא מציעא", "בבא בתרא", "סנהדרין", "מכות",
                            "שבועות", "עדיות", "עבודה זרה", "הוריות"]
            },
            "kodashim": {
                "name": "קדשים",
                "refs": ["Tosefta Zevachim", "Tosefta Menachot", "Tosefta Chullin", "Tosefta Bekhorot",
                        "Tosefta Arakhin", "Tosefta Temurah", "Tosefta Keritot", "Tosefta Meilah"],
                "he_names": ["זבחים", "מנחות", "חולין", "בכורות", "ערכין",
                            "תמורה", "כריתות", "מעילה"]
            },
            "tahorot": {
                "name": "טהרות",
                "refs": ["Tosefta Kelim Bava Kamma", "Tosefta Kelim Bava Metzia", "Tosefta Kelim Bava Batra",
                        "Tosefta Oholot", "Tosefta Negaim", "Tosefta Parah", "Tosefta Tahorot",
                        "Tosefta Mikvaot", "Tosefta Niddah", "Tosefta Makhshirin", "Tosefta Zavim",
                        "Tosefta Tevul Yom", "Tosefta Yadayim", "Tosefta Oktzin"],
                "he_names": ["כלים בבא קמא", "כלים בבא מציעא", "כלים בבא בתרא",
                            "אהלות", "נגעים", "פרה", "טהרות", "מקוואות",
                            "נידה", "מכשירין", "זבים", "טבול יום", "ידיים", "עוקצין"]
            }
        }
    },
    "yerushalmi": {
        "name": "תלמוד ירושלמי",
        "books": {
            "zeraim": {
                "name": "זרעים",
                "refs": ["Jerusalem Talmud Berakhot", "Jerusalem Talmud Peah", "Jerusalem Talmud Demai",
                        "Jerusalem Talmud Kilayim", "Jerusalem Talmud Sheviit", "Jerusalem Talmud Terumot",
                        "Jerusalem Talmud Maasrot", "Jerusalem Talmud Maaser Sheni", "Jerusalem Talmud Challah",
                        "Jerusalem Talmud Orlah", "Jerusalem Talmud Bikkurim"],
                "he_names": ["ברכות", "פאה", "דמאי", "כלאים", "שביעית", "תרומות",
                            "מעשרות", "מעשר שני", "חלה", "ערלה", "ביכורים"]
            },
            "moed": {
                "name": "מועד",
                "refs": ["Jerusalem Talmud Shabbat", "Jerusalem Talmud Eruvin", "Jerusalem Talmud Pesachim",
                        "Jerusalem Talmud Shekalim", "Jerusalem Talmud Yoma", "Jerusalem Talmud Sukkah",
                        "Jerusalem Talmud Beitzah", "Jerusalem Talmud Rosh Hashanah", "Jerusalem Talmud Taanit",
                        "Jerusalem Talmud Megillah", "Jerusalem Talmud Moed Katan", "Jerusalem Talmud Chagigah"],
                "he_names": ["שבת", "עירובין", "פסחים", "שקלים", "יומא", "סוכה",
                            "ביצה", "ראש השנה", "תענית", "מגילה", "מועד קטן", "חגיגה"]
            },
            "nashim": {
                "name": "נשים",
                "refs": ["Jerusalem Talmud Yevamot", "Jerusalem Talmud Ketubot", "Jerusalem Talmud Nedarim",
                        "Jerusalem Talmud Nazir", "Jerusalem Talmud Sotah", "Jerusalem Talmud Gittin",
                        "Jerusalem Talmud Kiddushin"],
                "he_names": ["יבמות", "כתובות", "נדרים", "נזיר", "סוטה", "גיטין", "קידושין"]
            },
            "nezikin": {
                "name": "נזיקין",
                "refs": ["Jerusalem Talmud Bava Kamma", "Jerusalem Talmud Bava Metzia", "Jerusalem Talmud Bava Batra",
                        "Jerusalem Talmud Sanhedrin", "Jerusalem Talmud Makkot", "Jerusalem Talmud Shevuot",
                        "Jerusalem Talmud Avodah Zarah", "Jerusalem Talmud Horayot"],
                "he_names": ["בבא קמא", "בבא מציעא", "בבא בתרא", "סנהדרין", "מכות",
                            "שבועות", "עבודה זרה", "הוריות"]
            },
            "tahorot": {
                "name": "טהרות",
                "refs": ["Jerusalem Talmud Niddah"],
                "he_names": ["נידה"]
            }
        }
    },
    "midrash_rabbah": {
        "name": "מדרש רבה",
        "books": {
            "torah": {
                "name": "תורה",
                "refs": ["Bereishit Rabbah", "Shemot Rabbah", "Vayikra Rabbah", "Bamidbar Rabbah", "Devarim Rabbah"],
                "he_names": ["בראשית רבה", "שמות רבה", "ויקרא רבה", "במדבר רבה", "דברים רבה"]
            },
            "megillot": {
                "name": "מגילות",
                "refs": ["Shir HaShirim Rabbah", "Ruth Rabbah", "Eichah Rabbah", "Kohelet Rabbah", "Esther Rabbah"],
                "he_names": ["שיר השירים רבה", "רות רבה", "איכה רבה", "קהלת רבה", "אסתר רבה"]
            }
        }
    },
    "midrash_tanchuma": {
        "name": "מדרש תנחומא",
        "books": {
            "torah": {
                "name": "תורה",
                "refs": ["Midrash Tanchuma, Bereshit", "Midrash Tanchuma, Noach", "Midrash Tanchuma, Lech Lecha",
                        "Midrash Tanchuma, Vayera", "Midrash Tanchuma, Chayei Sara", "Midrash Tanchuma, Toldot",
                        "Midrash Tanchuma, Vayetzei", "Midrash Tanchuma, Vayishlach", "Midrash Tanchuma, Vayeshev",
                        "Midrash Tanchuma, Miketz", "Midrash Tanchuma, Vayigash", "Midrash Tanchuma, Vayechi",
                        "Midrash Tanchuma, Shemot", "Midrash Tanchuma, Vaera", "Midrash Tanchuma, Bo",
                        "Midrash Tanchuma, Beshalach", "Midrash Tanchuma, Yitro", "Midrash Tanchuma, Mishpatim",
                        "Midrash Tanchuma, Terumah", "Midrash Tanchuma, Tetzaveh", "Midrash Tanchuma, Ki Tisa",
                        "Midrash Tanchuma, Vayakhel", "Midrash Tanchuma, Pekudei"],
                "he_names": ["בראשית", "נח", "לך לך", "וירא", "חיי שרה", "תולדות",
                            "ויצא", "וישלח", "וישב", "מקץ", "ויגש", "ויחי",
                            "שמות", "וארא", "בא", "בשלח", "יתרו", "משפטים",
                            "תרומה", "תצוה", "כי תשא", "ויקהל", "פקודי"]
            }
        }
    },
    "sifra": {
        "name": "ספרא",
        "books": {
            "all": {
                "name": "ספרא",
                "refs": ["Sifra"],
                "he_names": ["ספרא"]
            }
        }
    },
    "sifrei": {
        "name": "ספרי",
        "books": {
            "all": {
                "name": "ספרי",
                "refs": ["Sifrei Bamidbar", "Sifrei Devarim"],
                "he_names": ["ספרי במדבר", "ספרי דברים"]
            }
        }
    },
    "mekhilta": {
        "name": "מכילתא",
        "books": {
            "all": {
                "name": "מכילתא",
                "refs": ["Mekhilta d'Rabbi Yishmael"],
                "he_names": ["מכילתא דרבי ישמעאל"]
            }
        }
    }
}
