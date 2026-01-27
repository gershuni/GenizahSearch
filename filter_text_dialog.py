from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QLabel, QPlainTextEdit, QHBoxLayout, QPushButton,
    QFileDialog, QGroupBox, QProgressBar, QMessageBox, QComboBox, QCheckBox,
    QListWidget, QListWidgetItem, QAbstractItemView, QTreeWidget, QTreeWidgetItem,
    QSplitter, QLineEdit, QWidget
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from genizah_core import tr
import os
import json
import requests
import time


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
            print("[DEBUG] Fetching Sefaria TOC from API...")
            resp = requests.get(self.TOC_URL, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                print(f"[DEBUG] Fetched Sefaria TOC: {len(data)} top-level categories")
                return data
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

# Cache directory for downloaded texts
def get_cache_dir():
    """Get or create the cache directory for Sefaria texts."""
    cache_dir = os.path.join(os.path.expanduser("~"), ".genizah_search", "sefaria_cache")
    os.makedirs(cache_dir, exist_ok=True)
    return cache_dir


def clean_hebrew_text(text):
    """Remove nikud, taamim and non-alphabetic characters from Hebrew text.

    Keeps only Hebrew letters (א-ת) and basic whitespace.
    """
    import re
    import html as html_module
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


class SefariaFetchThread(QThread):
    """Background thread for fetching texts from Sefaria API."""
    progress = pyqtSignal(int, int, str)  # current, total, current_item
    finished = pyqtSignal(dict)  # dict of {ref: cleaned_text}
    error = pyqtSignal(str)  # error message

    def __init__(self, refs, use_cache=True):
        super().__init__()
        self.refs = refs
        self.use_cache = use_cache
        self._cancelled = False

    def cancel(self):
        self._cancelled = True

    def run(self):
        results = {}  # {ref: cleaned_text}
        cache_dir = get_cache_dir()

        for i, ref in enumerate(self.refs):
            if self._cancelled:
                break

            self.progress.emit(i, len(self.refs), ref)

            # Check cache first (cleaned version)
            cache_file = os.path.join(cache_dir, f"{ref.replace(' ', '_').replace('/', '_')}_clean.txt")

            if self.use_cache and os.path.exists(cache_file):
                try:
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        text = f.read()
                        if text:
                            results[ref] = text
                            continue
                except Exception:
                    pass

            # Fetch from Sefaria
            try:
                url = f"https://www.sefaria.org/api/texts/{ref.replace(' ', '%20')}?context=0&pad=0"
                resp = requests.get(url, timeout=30)
                if resp.status_code == 200:
                    data = resp.json()
                    raw_text = self._extract_hebrew_text(data)
                    if raw_text:
                        # Clean the text (remove nikud, taamim, non-Hebrew)
                        cleaned = clean_hebrew_text(raw_text)
                        if cleaned:
                            results[ref] = cleaned
                            # Cache the cleaned result
                            try:
                                with open(cache_file, 'w', encoding='utf-8') as f:
                                    f.write(cleaned)
                            except Exception:
                                pass
            except Exception as e:
                self.error.emit(f"Error fetching {ref}: {str(e)}")

        self.progress.emit(len(self.refs), len(self.refs), "Done")
        self.finished.emit(results)

    def _extract_hebrew_text(self, data):
        """Extract Hebrew text from Sefaria API response."""
        he_text = data.get('he', [])
        if isinstance(he_text, str):
            return he_text
        return self._flatten_text(he_text)

    def _flatten_text(self, text_data):
        """Recursively flatten nested text arrays."""
        if isinstance(text_data, str):
            # Remove HTML tags
            import re
            return re.sub(r'<[^>]+>', '', text_data)
        elif isinstance(text_data, list):
            parts = []
            for item in text_data:
                flattened = self._flatten_text(item)
                if flattened:
                    parts.append(flattened)
            return " ".join(parts)
        return ""


class SourceSelectionDialog(QDialog):
    """Dialog for selecting specific books/tractates to load."""

    def __init__(self, parent, source_type, source_data):
        super().__init__(parent)
        self.source_type = source_type
        self.source_data = source_data
        self.selected_refs = []

        self.setWindowTitle(tr("Select Books"))
        self.resize(400, 500)
        self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)

        layout = QVBoxLayout()

        # Category selector
        cat_row = QHBoxLayout()
        cat_row.addWidget(QLabel(tr("Category:")))
        self.cat_combo = QComboBox()

        # Add "All" option
        self.cat_combo.addItem(tr("All"), "all")
        for key, book_data in source_data["books"].items():
            self.cat_combo.addItem(book_data["name"], key)
        self.cat_combo.currentIndexChanged.connect(self._on_category_changed)
        cat_row.addWidget(self.cat_combo)
        cat_row.addStretch()
        layout.addLayout(cat_row)

        # Select all checkbox
        self.chk_select_all = QCheckBox(tr("Select All"))
        self.chk_select_all.toggled.connect(self._on_select_all_toggled)
        layout.addWidget(self.chk_select_all)

        # Book list
        self.book_list = QListWidget()
        self.book_list.setSelectionMode(QAbstractItemView.SelectionMode.MultiSelection)
        layout.addWidget(self.book_list)

        # Info label
        self.info_label = QLabel("")
        self.info_label.setStyleSheet("color: #666; font-size: 11px;")
        layout.addWidget(self.info_label)

        # Buttons
        btn_row = QHBoxLayout()
        btn_row.addStretch()
        btn_cancel = QPushButton(tr("Cancel"))
        btn_cancel.clicked.connect(self.reject)
        btn_ok = QPushButton(tr("Load Selected"))
        btn_ok.clicked.connect(self._on_ok)
        btn_row.addWidget(btn_cancel)
        btn_row.addWidget(btn_ok)
        layout.addLayout(btn_row)

        self.setLayout(layout)
        self._populate_list()

    def _on_category_changed(self, index):
        self._populate_list()

    def _populate_list(self):
        self.book_list.clear()
        cat_key = self.cat_combo.currentData()

        if cat_key == "all":
            # Show all books from all categories
            for book_key, book_data in self.source_data["books"].items():
                for i, (ref, he_name) in enumerate(zip(book_data["refs"], book_data["he_names"])):
                    item = QListWidgetItem(f"{book_data['name']} - {he_name}")
                    item.setData(Qt.ItemDataRole.UserRole, ref)
                    self.book_list.addItem(item)
        else:
            book_data = self.source_data["books"].get(cat_key, {})
            for ref, he_name in zip(book_data.get("refs", []), book_data.get("he_names", [])):
                item = QListWidgetItem(he_name)
                item.setData(Qt.ItemDataRole.UserRole, ref)
                self.book_list.addItem(item)

        self._update_info()

    def _on_select_all_toggled(self, checked):
        for i in range(self.book_list.count()):
            self.book_list.item(i).setSelected(checked)
        self._update_info()

    def _update_info(self):
        count = len(self.book_list.selectedItems())
        total = self.book_list.count()
        self.info_label.setText(tr("Selected: {} / {}").format(count, total))

    def _on_ok(self):
        self.selected_refs = []
        for item in self.book_list.selectedItems():
            ref = item.data(Qt.ItemDataRole.UserRole)
            if ref:
                self.selected_refs.append(ref)
        if not self.selected_refs:
            QMessageBox.warning(self, tr("Warning"), tr("Please select at least one book."))
            return
        self.accept()


class AllSourcesDialog(QDialog):
    """Dialog for browsing all Sefaria sources in a hierarchical tree."""

    def __init__(self, parent):
        super().__init__(parent)
        self.selected_refs = []
        self.library = get_sefaria_library()

        self.setWindowTitle(tr("Sefaria Library"))
        self.resize(700, 600)
        self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)

        layout = QVBoxLayout()

        # Search box
        search_row = QHBoxLayout()
        search_row.addWidget(QLabel(tr("Search:")))
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText(tr("Search texts..."))
        self.search_input.textChanged.connect(self._on_search_changed)
        search_row.addWidget(self.search_input)
        layout.addLayout(search_row)

        # Main splitter with tree and text list
        splitter = QSplitter(Qt.Orientation.Horizontal)

        # Category tree on the left
        self.category_tree = QTreeWidget()
        self.category_tree.setHeaderLabel(tr("Categories"))
        self.category_tree.itemClicked.connect(self._on_category_selected)
        self.category_tree.itemExpanded.connect(self._on_category_expanded)
        splitter.addWidget(self.category_tree)

        # Text list on the right
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)
        right_layout.setContentsMargins(0, 0, 0, 0)

        self.text_list = QListWidget()
        self.text_list.setSelectionMode(QAbstractItemView.SelectionMode.MultiSelection)
        self.text_list.itemSelectionChanged.connect(self._update_info)
        right_layout.addWidget(self.text_list)

        # Select all in category
        self.chk_select_all = QCheckBox(tr("Select All in Category"))
        self.chk_select_all.toggled.connect(self._on_select_all_toggled)
        right_layout.addWidget(self.chk_select_all)

        splitter.addWidget(right_widget)
        splitter.setSizes([250, 450])

        layout.addWidget(splitter)

        # Info label
        self.info_label = QLabel("")
        self.info_label.setStyleSheet("color: #666; font-size: 11px;")
        layout.addWidget(self.info_label)

        # Status label for loading
        self.status_label = QLabel("")
        self.status_label.setStyleSheet("color: #888; font-style: italic;")
        layout.addWidget(self.status_label)

        # Buttons
        btn_row = QHBoxLayout()
        btn_row.addStretch()
        btn_cancel = QPushButton(tr("Cancel"))
        btn_cancel.clicked.connect(self.reject)
        btn_ok = QPushButton(tr("Load Selected"))
        btn_ok.clicked.connect(self._on_ok)
        btn_row.addWidget(btn_cancel)
        btn_row.addWidget(btn_ok)
        layout.addLayout(btn_row)

        self.setLayout(layout)

        # Load the library
        self._populate_tree()

    def _populate_tree(self):
        """Populate the category tree from Sefaria TOC."""
        self.category_tree.clear()
        self.status_label.setText(tr("Loading library..."))

        toc = self.library.get_toc()
        if not toc:
            self.status_label.setText(tr("Failed to load library. Check internet connection."))
            return

        self.status_label.setText("")

        for category in toc:
            if isinstance(category, dict) and 'category' in category:
                item = QTreeWidgetItem([category.get('heCategory', category.get('category', ''))])
                item.setData(0, Qt.ItemDataRole.UserRole, category)
                # Add a dummy child to show expand arrow
                if 'contents' in category and category['contents']:
                    dummy = QTreeWidgetItem([tr("Loading...")])
                    item.addChild(dummy)
                self.category_tree.addTopLevelItem(item)

    def _on_category_expanded(self, item):
        """Lazy load subcategories when expanded."""
        # Check if this has a dummy child
        if item.childCount() == 1 and item.child(0).text(0) == tr("Loading..."):
            item.takeChildren()  # Remove dummy
            category_data = item.data(0, Qt.ItemDataRole.UserRole)
            if category_data and 'contents' in category_data:
                self._add_children(item, category_data['contents'])

    def _add_children(self, parent_item, contents):
        """Add child items to a tree node."""
        for child in contents:
            if isinstance(child, dict):
                if 'category' in child:
                    # It's a subcategory
                    child_item = QTreeWidgetItem([child.get('heCategory', child.get('category', ''))])
                    child_item.setData(0, Qt.ItemDataRole.UserRole, child)
                    if 'contents' in child and child['contents']:
                        dummy = QTreeWidgetItem([tr("Loading...")])
                        child_item.addChild(dummy)
                    parent_item.addChild(child_item)
                elif 'title' in child:
                    # It's a text - add to parent's data for later
                    pass

    def _on_category_selected(self, item, column):
        """When a category is selected, show its texts in the list."""
        self.text_list.clear()
        self.chk_select_all.setChecked(False)

        category_data = item.data(0, Qt.ItemDataRole.UserRole)
        if not category_data:
            return

        # Get all texts in this category recursively
        texts = self.library.get_texts_recursive(category_data)

        for text in texts:
            display_name = text.get('heTitle', text.get('title', ''))
            list_item = QListWidgetItem(display_name)
            list_item.setData(Qt.ItemDataRole.UserRole, text.get('title', ''))
            self.text_list.addItem(list_item)

        self._update_info()

    def _on_search_changed(self, text):
        """Filter the text list by search query."""
        search_text = text.strip().lower()
        if not search_text:
            # Show all items
            for i in range(self.text_list.count()):
                self.text_list.item(i).setHidden(False)
            return

        # Filter items
        for i in range(self.text_list.count()):
            item = self.text_list.item(i)
            item_text = item.text().lower()
            ref = (item.data(Qt.ItemDataRole.UserRole) or '').lower()
            visible = search_text in item_text or search_text in ref
            item.setHidden(not visible)

    def _on_select_all_toggled(self, checked):
        """Select or deselect all visible items."""
        for i in range(self.text_list.count()):
            item = self.text_list.item(i)
            if not item.isHidden():
                item.setSelected(checked)
        self._update_info()

    def _update_info(self):
        """Update the info label with selection count."""
        selected = len(self.text_list.selectedItems())
        total = sum(1 for i in range(self.text_list.count()) if not self.text_list.item(i).isHidden())
        self.info_label.setText(tr("Selected: {} / {}").format(selected, total))

    def _on_ok(self):
        """Accept the dialog with selected refs."""
        self.selected_refs = []
        for item in self.text_list.selectedItems():
            ref = item.data(Qt.ItemDataRole.UserRole)
            if ref:
                self.selected_refs.append(ref)
        if not self.selected_refs:
            QMessageBox.warning(self, tr("Warning"), tr("Please select at least one book."))
            return
        self.accept()


class SefariaSearchDialog(QDialog):
    """Dialog to search Sefaria by reference."""

    def __init__(self, parent):
        super().__init__(parent)
        self.ref = None

        self.setWindowTitle(tr("Search Sefaria"))
        self.resize(450, 250)
        self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)

        layout = QVBoxLayout()

        layout.addWidget(QLabel(tr("Enter a Sefaria reference (e.g., \"Genesis 1\", \"Berakhot 2a\", \"Rashi on Genesis 1\"):")))

        from PyQt6.QtWidgets import QLineEdit
        self.ref_input = QLineEdit()
        self.ref_input.setPlaceholderText("Genesis 1")
        layout.addWidget(self.ref_input)

        # Examples
        layout.addWidget(QLabel(tr("Examples:")))
        examples_row = QHBoxLayout()
        for example in ['Genesis 1', 'Exodus', 'Psalms', 'Berakhot', 'Rashi on Genesis']:
            btn = QPushButton(example)
            btn.clicked.connect(lambda checked, e=example: self.ref_input.setText(e))
            examples_row.addWidget(btn)
        examples_row.addStretch()
        layout.addLayout(examples_row)

        # Status
        self.status_label = QLabel("")
        self.status_label.setStyleSheet("color: #666;")
        layout.addWidget(self.status_label)

        layout.addStretch()

        # Buttons
        btn_row = QHBoxLayout()
        btn_row.addStretch()
        btn_cancel = QPushButton(tr("Cancel"))
        btn_cancel.clicked.connect(self.reject)
        btn_load = QPushButton(tr("Load"))
        btn_load.clicked.connect(self._on_load)
        btn_row.addWidget(btn_cancel)
        btn_row.addWidget(btn_load)
        layout.addLayout(btn_row)

        self.setLayout(layout)

    def _on_load(self):
        ref = self.ref_input.text().strip()
        if not ref:
            QMessageBox.warning(self, tr("Warning"), tr("Please enter a Sefaria reference"))
            return

        self.ref = ref
        self.accept()


class AddCustomTextDialog(QDialog):
    """Dialog to add custom text as a filter source."""

    # Class-level counter for unique IDs
    _custom_count = 0

    def __init__(self, parent):
        super().__init__(parent)
        self.custom_ref = None
        self.custom_text = None
        self.custom_name = None

        self.setWindowTitle(tr("Add Custom Text"))
        self.resize(500, 400)
        self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)

        layout = QVBoxLayout()

        layout.addWidget(QLabel(tr("Enter a name for this source:")))

        from PyQt6.QtWidgets import QLineEdit
        self.name_input = QLineEdit()
        self.name_input.setPlaceholderText(tr("e.g., My Commentary"))
        layout.addWidget(self.name_input)

        layout.addWidget(QLabel(tr("Paste your text (will be cleaned automatically):")))

        self.text_area = QPlainTextEdit()
        self.text_area.setPlaceholderText(tr("Paste Hebrew text here..."))
        self.text_area.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        layout.addWidget(self.text_area)

        # Info
        self.info_label = QLabel("")
        self.info_label.setStyleSheet("color: #666; font-size: 11px;")
        layout.addWidget(self.info_label)

        # Buttons
        btn_row = QHBoxLayout()
        btn_row.addStretch()
        btn_cancel = QPushButton(tr("Cancel"))
        btn_cancel.clicked.connect(self.reject)
        btn_add = QPushButton(tr("Add"))
        btn_add.clicked.connect(self._on_add)
        btn_row.addWidget(btn_cancel)
        btn_row.addWidget(btn_add)
        layout.addLayout(btn_row)

        self.setLayout(layout)

    def _on_add(self):
        name = self.name_input.text().strip()
        text = self.text_area.toPlainText().strip()

        if not name:
            QMessageBox.warning(self, tr("Warning"), tr("Please enter a name for the source"))
            return

        if not text or len(text) < 10:
            QMessageBox.warning(self, tr("Warning"), tr("Please enter at least 10 characters of text"))
            return

        # Clean the text
        cleaned = clean_hebrew_text(text)
        if not cleaned or len(cleaned) < 10:
            QMessageBox.warning(self, tr("Warning"), tr("No valid Hebrew text found"))
            return

        # Generate unique ref
        AddCustomTextDialog._custom_count += 1
        self.custom_ref = f"custom:{AddCustomTextDialog._custom_count}:{name}"
        self.custom_text = cleaned
        self.custom_name = name

        self.accept()


class FilterTextDialog(QDialog):
    """Dialog to manage text sources for filtering composition results.

    Sources are loaded from Sefaria and stored internally. Only checked sources
    are included in the final filter text.
    """

    def __init__(self, parent, current_sources=None):
        """
        Args:
            parent: Parent widget
            current_sources: dict of {ref: text} for previously loaded sources
        """
        super().__init__(parent)
        self.setWindowTitle(tr("Filter Text"))
        self.resize(600, 500)
        self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)

        # Store loaded sources: {ref: cleaned_text}
        self.loaded_sources = current_sources.copy() if current_sources else {}
        # Track which sources are enabled
        self.enabled_sources = set(self.loaded_sources.keys())
        self.fetch_thread = None

        layout = QVBoxLayout()

        layout.addWidget(QLabel(tr("Select sources to filter results (matches found in checked sources will be moved to a separate list):")))

        # Sefaria sources section
        sources_group = QGroupBox(tr("Load from Sefaria"))
        sources_layout = QVBoxLayout()

        sources_btn_row = QHBoxLayout()

        btn_tanakh = QPushButton(tr("Tanakh"))
        btn_tanakh.setToolTip(tr("Load Bible text from Sefaria"))
        btn_tanakh.clicked.connect(lambda: self._open_source_dialog("tanakh"))
        sources_btn_row.addWidget(btn_tanakh)

        btn_mishnah = QPushButton(tr("Mishnah"))
        btn_mishnah.setToolTip(tr("Load Mishnah text from Sefaria"))
        btn_mishnah.clicked.connect(lambda: self._open_source_dialog("mishnah"))
        sources_btn_row.addWidget(btn_mishnah)

        btn_talmud = QPushButton(tr("Talmud"))
        btn_talmud.setToolTip(tr("Load Talmud Bavli text from Sefaria"))
        btn_talmud.clicked.connect(lambda: self._open_source_dialog("talmud"))
        sources_btn_row.addWidget(btn_talmud)

        btn_more = QPushButton(tr("More Sources..."))
        btn_more.setToolTip(tr("Browse all available Sefaria sources"))
        btn_more.clicked.connect(self._open_all_sources_dialog)
        sources_btn_row.addWidget(btn_more)

        btn_search = QPushButton(tr("Search Sefaria"))
        btn_search.setToolTip(tr("Search for any Sefaria text by reference"))
        btn_search.clicked.connect(self._open_sefaria_search_dialog)
        sources_btn_row.addWidget(btn_search)

        sources_btn_row.addStretch()
        sources_layout.addLayout(sources_btn_row)

        # Custom source row
        custom_row = QHBoxLayout()
        custom_row.addWidget(QLabel(tr("Custom source") + ":"))
        btn_custom = QPushButton(tr("Add Custom Text"))
        btn_custom.setToolTip(tr("Add your own text as a filter source"))
        btn_custom.clicked.connect(self._open_add_custom_dialog)
        custom_row.addWidget(btn_custom)
        custom_row.addStretch()
        sources_layout.addLayout(custom_row)

        # Progress bar for loading
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        sources_layout.addWidget(self.progress_bar)

        self.progress_label = QLabel("")
        self.progress_label.setVisible(False)
        sources_layout.addWidget(self.progress_label)

        sources_group.setLayout(sources_layout)
        layout.addWidget(sources_group)

        # Loaded sources list (checkboxes)
        loaded_group = QGroupBox(tr("Loaded Sources"))
        loaded_layout = QVBoxLayout()

        # Select all / deselect all buttons
        select_row = QHBoxLayout()
        btn_select_all = QPushButton(tr("Select All"))
        btn_select_all.clicked.connect(self._select_all)
        btn_deselect_all = QPushButton(tr("Deselect All"))
        btn_deselect_all.clicked.connect(self._deselect_all)
        btn_remove_selected = QPushButton(tr("Remove Unchecked"))
        btn_remove_selected.clicked.connect(self._remove_unchecked)
        select_row.addWidget(btn_select_all)
        select_row.addWidget(btn_deselect_all)
        select_row.addWidget(btn_remove_selected)
        select_row.addStretch()
        loaded_layout.addLayout(select_row)

        # Scrollable list of loaded sources
        self.sources_list = QListWidget()
        self.sources_list.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        loaded_layout.addWidget(self.sources_list)

        # Info label
        self.info_label = QLabel("")
        self.info_label.setStyleSheet("color: #666; font-size: 11px;")
        loaded_layout.addWidget(self.info_label)

        loaded_group.setLayout(loaded_layout)
        layout.addWidget(loaded_group)

        # Bottom buttons
        btn_row = QHBoxLayout()
        btn_row.addStretch()

        btn_ok = QPushButton(tr("OK"))
        btn_ok.clicked.connect(self.accept)
        btn_cancel = QPushButton(tr("Cancel"))
        btn_cancel.clicked.connect(self.reject)

        btn_row.addWidget(btn_cancel)
        btn_row.addWidget(btn_ok)
        layout.addLayout(btn_row)

        self.setLayout(layout)

        # Populate the list with any existing sources
        self._refresh_sources_list()

    def _get_source_display_name(self, ref):
        """Get a display name for a source reference."""
        # Handle custom sources
        if ref.startswith('custom:'):
            parts = ref.split(':', 2)
            if len(parts) >= 3:
                return f"📝 {parts[2]}"
            return "📝 Custom Text"

        # Try to find the Hebrew name from SEFARIA_SOURCES
        for source_type, source_data in SEFARIA_SOURCES.items():
            for book_key, book_data in source_data.get("books", {}).items():
                if ref in book_data.get("refs", []):
                    idx = book_data["refs"].index(ref)
                    return f"{source_data['name']} - {book_data['he_names'][idx]}"
        return ref

    def _refresh_sources_list(self):
        """Refresh the list of loaded sources with checkboxes."""
        self.sources_list.clear()

        for ref in sorted(self.loaded_sources.keys()):
            item = QListWidgetItem()
            item.setData(Qt.ItemDataRole.UserRole, ref)

            checkbox = QCheckBox(self._get_source_display_name(ref))
            checkbox.setChecked(ref in self.enabled_sources)
            checkbox.toggled.connect(lambda checked, r=ref: self._on_source_toggled(r, checked))

            item.setSizeHint(checkbox.sizeHint())
            self.sources_list.addItem(item)
            self.sources_list.setItemWidget(item, checkbox)

        self._update_info()

    def _on_source_toggled(self, ref, checked):
        """Handle source checkbox toggle."""
        if checked:
            self.enabled_sources.add(ref)
        else:
            self.enabled_sources.discard(ref)
        self._update_info()

    def _select_all(self):
        """Select all loaded sources."""
        self.enabled_sources = set(self.loaded_sources.keys())
        self._refresh_sources_list()

    def _deselect_all(self):
        """Deselect all loaded sources."""
        self.enabled_sources.clear()
        self._refresh_sources_list()

    def _remove_unchecked(self):
        """Remove sources that are not checked."""
        to_remove = [ref for ref in self.loaded_sources.keys() if ref not in self.enabled_sources]
        for ref in to_remove:
            del self.loaded_sources[ref]
        self._refresh_sources_list()

    def _update_info(self):
        """Update the info label with source count."""
        enabled = len(self.enabled_sources)
        total = len(self.loaded_sources)
        self.info_label.setText(tr("Active: {} / {}").format(enabled, total))

    def _open_source_dialog(self, source_type):
        """Open dialog to select specific books from a source."""
        source_data = SEFARIA_SOURCES.get(source_type)
        if not source_data:
            return

        dlg = SourceSelectionDialog(self, source_type, source_data)
        if dlg.exec() == QDialog.DialogCode.Accepted and dlg.selected_refs:
            self._start_fetch(dlg.selected_refs)

    def _open_all_sources_dialog(self):
        """Open dialog to browse all available Sefaria sources."""
        dlg = AllSourcesDialog(self)
        if dlg.exec() == QDialog.DialogCode.Accepted and dlg.selected_refs:
            self._start_fetch(dlg.selected_refs)

    def _open_sefaria_search_dialog(self):
        """Open dialog to search Sefaria by reference."""
        dlg = SefariaSearchDialog(self)
        if dlg.exec() == QDialog.DialogCode.Accepted and dlg.ref:
            self._start_fetch([dlg.ref])

    def _open_add_custom_dialog(self):
        """Open dialog to add custom text source."""
        dlg = AddCustomTextDialog(self)
        if dlg.exec() == QDialog.DialogCode.Accepted and dlg.custom_ref and dlg.custom_text:
            # Add directly to loaded sources
            self.loaded_sources[dlg.custom_ref] = dlg.custom_text
            self.enabled_sources.add(dlg.custom_ref)
            self._refresh_sources_list()
            QMessageBox.information(
                self, tr("Info"),
                f'{tr("Added")} "{dlg.custom_name}" ({len(dlg.custom_text)} {tr("characters")})'
            )

    def _start_fetch(self, refs):
        """Start fetching texts from Sefaria."""
        if self.fetch_thread and self.fetch_thread.isRunning():
            self.fetch_thread.cancel()
            self.fetch_thread.wait()

        # Filter out already loaded refs
        new_refs = [r for r in refs if r not in self.loaded_sources]
        if not new_refs:
            QMessageBox.information(self, tr("Info"), tr("All selected sources are already loaded."))
            return

        self.progress_bar.setVisible(True)
        self.progress_bar.setMaximum(len(new_refs))
        self.progress_bar.setValue(0)
        self.progress_label.setVisible(True)

        self.fetch_thread = SefariaFetchThread(new_refs)
        self.fetch_thread.progress.connect(self._on_fetch_progress)
        self.fetch_thread.finished.connect(self._on_fetch_finished)
        self.fetch_thread.error.connect(self._on_fetch_error)
        self.fetch_thread.start()

    def _on_fetch_progress(self, current, total, item):
        self.progress_bar.setValue(current)
        self.progress_label.setText(tr("Loading: {}").format(item))

    def _on_fetch_finished(self, results):
        """Handle fetch completion. results is a dict of {ref: cleaned_text}."""
        self.progress_bar.setVisible(False)
        self.progress_label.setVisible(False)

        if results:
            # Add new sources
            self.loaded_sources.update(results)
            # Enable all new sources
            self.enabled_sources.update(results.keys())
            self._refresh_sources_list()

    def _on_fetch_error(self, error_msg):
        self.progress_label.setText(tr("Error: {}").format(error_msg))

    def get_text(self):
        """Get combined text from all enabled sources."""
        texts = [self.loaded_sources[ref] for ref in self.enabled_sources if ref in self.loaded_sources]
        return " ".join(texts)

    def get_sources(self):
        """Get the dict of loaded sources."""
        return self.loaded_sources.copy()

    def get_enabled_sources(self):
        """Get the set of enabled source refs."""
        return self.enabled_sources.copy()

    def closeEvent(self, event):
        if self.fetch_thread and self.fetch_thread.isRunning():
            self.fetch_thread.cancel()
            self.fetch_thread.wait()
        super().closeEvent(event)
