from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QLabel, QPlainTextEdit, QHBoxLayout, QPushButton,
    QFileDialog, QGroupBox, QProgressBar, QMessageBox, QComboBox, QCheckBox,
    QListWidget, QListWidgetItem, QAbstractItemView
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from genizah_core import tr
import os
import json
import requests

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
    }
}

# Cache directory for downloaded texts
def get_cache_dir():
    """Get or create the cache directory for Sefaria texts."""
    cache_dir = os.path.join(os.path.expanduser("~"), ".genizah_search", "sefaria_cache")
    os.makedirs(cache_dir, exist_ok=True)
    return cache_dir


class SefariaFetchThread(QThread):
    """Background thread for fetching texts from Sefaria API."""
    progress = pyqtSignal(int, int, str)  # current, total, current_item
    finished = pyqtSignal(str)  # final text
    error = pyqtSignal(str)  # error message

    def __init__(self, refs, use_cache=True):
        super().__init__()
        self.refs = refs
        self.use_cache = use_cache
        self._cancelled = False

    def cancel(self):
        self._cancelled = True

    def run(self):
        all_text = []
        cache_dir = get_cache_dir()

        for i, ref in enumerate(self.refs):
            if self._cancelled:
                break

            self.progress.emit(i, len(self.refs), ref)

            # Check cache first
            cache_file = os.path.join(cache_dir, f"{ref.replace(' ', '_').replace('/', '_')}.txt")

            if self.use_cache and os.path.exists(cache_file):
                try:
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        text = f.read()
                        if text:
                            all_text.append(text)
                            continue
                except Exception:
                    pass

            # Fetch from Sefaria
            try:
                url = f"https://www.sefaria.org/api/texts/{ref.replace(' ', '%20')}?context=0&pad=0"
                resp = requests.get(url, timeout=30)
                if resp.status_code == 200:
                    data = resp.json()
                    text = self._extract_hebrew_text(data)
                    if text:
                        all_text.append(text)
                        # Cache the result
                        try:
                            with open(cache_file, 'w', encoding='utf-8') as f:
                                f.write(text)
                        except Exception:
                            pass
            except Exception as e:
                self.error.emit(f"Error fetching {ref}: {str(e)}")

        self.progress.emit(len(self.refs), len(self.refs), "Done")
        self.finished.emit("\n".join(all_text))

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


class FilterTextDialog(QDialog):
    """Dialog to input or load text for filtering composition results."""

    def __init__(self, parent, current_text=""):
        super().__init__(parent)
        self.setWindowTitle(tr("Filter Text"))
        self.resize(600, 500)
        self.result_text = current_text
        self.fetch_thread = None

        layout = QVBoxLayout()

        layout.addWidget(QLabel(tr("Enter text to filter results (results found in this text will be moved to a separate list):")))

        self.text_area = QPlainTextEdit()
        self.text_area.setPlaceholderText(tr("Paste text here..."))
        self.text_area.setPlainText(current_text)
        layout.addWidget(self.text_area)

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

        sources_btn_row.addStretch()
        sources_layout.addLayout(sources_btn_row)

        # Progress bar for loading
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        sources_layout.addWidget(self.progress_bar)

        self.progress_label = QLabel("")
        self.progress_label.setVisible(False)
        sources_layout.addWidget(self.progress_label)

        sources_group.setLayout(sources_layout)
        layout.addWidget(sources_group)

        # Bottom buttons
        btn_row = QHBoxLayout()
        btn_load = QPushButton(tr("Load from File"))
        btn_load.clicked.connect(self.load_file)
        btn_row.addWidget(btn_load)

        btn_clear = QPushButton(tr("Clear"))
        btn_clear.clicked.connect(lambda: self.text_area.clear())
        btn_row.addWidget(btn_clear)

        btn_row.addStretch()

        btn_ok = QPushButton(tr("OK"))
        btn_ok.clicked.connect(self.accept)
        btn_cancel = QPushButton(tr("Cancel"))
        btn_cancel.clicked.connect(self.reject)

        btn_row.addWidget(btn_cancel)
        btn_row.addWidget(btn_ok)
        layout.addLayout(btn_row)

        self.setLayout(layout)

    def _open_source_dialog(self, source_type):
        """Open dialog to select specific books from a source."""
        source_data = SEFARIA_SOURCES.get(source_type)
        if not source_data:
            return

        dlg = SourceSelectionDialog(self, source_type, source_data)
        if dlg.exec() == QDialog.DialogCode.Accepted and dlg.selected_refs:
            self._start_fetch(dlg.selected_refs)

    def _start_fetch(self, refs):
        """Start fetching texts from Sefaria."""
        if self.fetch_thread and self.fetch_thread.isRunning():
            self.fetch_thread.cancel()
            self.fetch_thread.wait()

        self.progress_bar.setVisible(True)
        self.progress_bar.setMaximum(len(refs))
        self.progress_bar.setValue(0)
        self.progress_label.setVisible(True)

        self.fetch_thread = SefariaFetchThread(refs)
        self.fetch_thread.progress.connect(self._on_fetch_progress)
        self.fetch_thread.finished.connect(self._on_fetch_finished)
        self.fetch_thread.error.connect(self._on_fetch_error)
        self.fetch_thread.start()

    def _on_fetch_progress(self, current, total, item):
        self.progress_bar.setValue(current)
        self.progress_label.setText(tr("Loading: {}").format(item))

    def _on_fetch_finished(self, text):
        self.progress_bar.setVisible(False)
        self.progress_label.setVisible(False)

        if text:
            # Append to existing text
            current = self.text_area.toPlainText()
            if current:
                self.text_area.setPlainText(current + "\n\n" + text)
            else:
                self.text_area.setPlainText(text)

    def _on_fetch_error(self, error_msg):
        self.progress_label.setText(tr("Error: {}").format(error_msg))

    def load_file(self):
        path, _ = QFileDialog.getOpenFileName(self, tr("Load Text"), "", "Text Files (*.txt);;All Files (*)")
        if path:
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    self.text_area.setPlainText(f.read())
            except Exception as e:
                # In a real app we might show an error message, but simplicity for now
                pass

    def get_text(self):
        return self.text_area.toPlainText()

    def closeEvent(self, event):
        if self.fetch_thread and self.fetch_thread.isRunning():
            self.fetch_thread.cancel()
            self.fetch_thread.wait()
        super().closeEvent(event)
