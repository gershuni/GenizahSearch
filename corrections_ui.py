"""
Corrections UI Components for PyQt6 Desktop App
Integrates with the Genizah Corrections API
"""
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime

from PyQt6.QtWidgets import (
    QDialog, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
    QLabel, QLineEdit, QPushButton, QTextEdit, QComboBox,
    QTableWidget, QTableWidgetItem, QHeaderView, QTabWidget,
    QGroupBox, QFrame, QMessageBox, QProgressDialog,
    QSpinBox, QDoubleSpinBox, QCheckBox, QScrollArea,
    QSplitter, QMenu, QStatusBar, QListWidget, QListWidgetItem,
    QCompleter, QInputDialog
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer
from PyQt6.QtGui import QFont, QColor, QAction, QPalette, QStandardItem, QStandardItemModel

from genizah_core import normalize_shelfmark

try:
    from genizah_core import tr, CURRENT_LANG
except ImportError:
    def tr(text): return text
    CURRENT_LANG = 'en'

from corrections_client import (
    CorrectionsClient, get_corrections_client,
    User, Correction, Comment, Discovery, DiscoveryResponse, FeedItem,
    FragmentJoin, ConnectedFragments
)


def safe_date_str(date_value, default: str = "") -> str:
    """Safely extract date string (YYYY-MM-DD) from various formats."""
    if not date_value:
        return default
    if isinstance(date_value, datetime):
        return date_value.strftime("%Y-%m-%d")
    if isinstance(date_value, str) and len(date_value) >= 10:
        return date_value[:10]
    return str(date_value) if date_value else default

logger = logging.getLogger(__name__)


class LoginDialog(QDialog):
    """Dialog for user login"""
    login_success = pyqtSignal(object)  # Emits User object

    def __init__(self, parent=None, client: CorrectionsClient = None):
        super().__init__(parent)
        self.client = client or get_corrections_client()
        self.setWindowTitle(tr("Login to Corrections System"))
        self.resize(400, 280)
        self.init_ui()
        self._load_saved_credentials()

    def _load_saved_credentials(self):
        """Load saved credentials and pre-fill the form."""
        try:
            if hasattr(self.client, 'get_saved_login_credentials'):
                email, password = self.client.get_saved_login_credentials()
                if email:
                    self.email_input.setText(email)
                    self.remember_checkbox.setChecked(True)
                    if password:
                        self.password_input.setText(password)
        except Exception as e:
            logger.debug(f"Could not load saved credentials: {e}")

    def init_ui(self):
        layout = QVBoxLayout(self)

        # Header
        header = QLabel(tr("Login to submit corrections and comments"))
        header.setStyleSheet("font-weight: bold; font-size: 14px;")
        layout.addWidget(header)

        # Form
        form = QGridLayout()

        form.addWidget(QLabel(tr("Email:")), 0, 0)
        self.email_input = QLineEdit()
        self.email_input.setPlaceholderText("your@email.com")
        form.addWidget(self.email_input, 0, 1)

        form.addWidget(QLabel(tr("Password:")), 1, 0)
        self.password_input = QLineEdit()
        self.password_input.setEchoMode(QLineEdit.EchoMode.Password)
        form.addWidget(self.password_input, 1, 1)

        layout.addLayout(form)

        # Remember me checkbox
        self.remember_checkbox = QCheckBox(tr("Remember me"))
        self.remember_checkbox.setToolTip(tr("Save login credentials for next time"))
        layout.addWidget(self.remember_checkbox)

        # Forgot password link
        forgot_link = QLabel(f'<a href="#">{tr("Forgot password?")}</a>')
        forgot_link.setOpenExternalLinks(False)
        forgot_link.linkActivated.connect(self.open_forgot_password)
        forgot_link.setAlignment(Qt.AlignmentFlag.AlignRight)
        layout.addWidget(forgot_link)

        # Buttons
        btn_layout = QHBoxLayout()

        self.btn_register = QPushButton(tr("Register"))
        self.btn_register.clicked.connect(self.open_register)
        btn_layout.addWidget(self.btn_register)

        btn_layout.addStretch()

        self.btn_cancel = QPushButton(tr("Cancel"))
        self.btn_cancel.clicked.connect(self.reject)
        btn_layout.addWidget(self.btn_cancel)

        self.btn_login = QPushButton(tr("Login"))
        self.btn_login.setStyleSheet("background-color: #27ae60; color: white;")
        self.btn_login.clicked.connect(self.do_login)
        self.btn_login.setDefault(True)
        btn_layout.addWidget(self.btn_login)

        layout.addLayout(btn_layout)

        # Enter key triggers login
        self.password_input.returnPressed.connect(self.do_login)

    def do_login(self):
        email = self.email_input.text().strip()
        password = self.password_input.text()

        if not email or not password:
            QMessageBox.warning(self, tr("Error"), tr("Please enter email and password"))
            return

        self.btn_login.setEnabled(False)
        self.btn_login.setText(tr("Logging in..."))

        success, message = self.client.login(email, password)

        if success:
            # Save or clear credentials based on "Remember me" checkbox
            if hasattr(self.client, 'save_login_credentials'):
                if self.remember_checkbox.isChecked():
                    self.client.save_login_credentials(email, password)
                else:
                    # Clear saved credentials if user unchecked "Remember me"
                    if hasattr(self.client, 'clear_saved_login_credentials'):
                        self.client.clear_saved_login_credentials()
            self.login_success.emit(self.client.current_user)
            self.accept()
        else:
            QMessageBox.warning(self, tr("Login Failed"), message)
            self.btn_login.setEnabled(True)
            self.btn_login.setText(tr("Login"))

    def open_forgot_password(self):
        """Open forgot password dialog to send reset email."""
        email = self.email_input.text().strip()
        if not email:
            email, ok = QInputDialog.getText(
                self, tr("Forgot Password"),
                tr("Enter your email address:"),
                QLineEdit.EchoMode.Normal
            )
            if not ok or not email:
                return

        # Send password reset email via Supabase
        try:
            result = self.client.request_password_reset(email)
            if result.get('success'):
                QMessageBox.information(
                    self, tr("Email Sent"),
                    tr("A password reset link has been sent to your email.\n\nIf you signed up with Google, this will let you set a password for desktop login.")
                )
            else:
                QMessageBox.warning(self, tr("Error"), result.get('error', tr("Failed to send reset email")))
        except Exception as e:
            QMessageBox.warning(self, tr("Error"), str(e))

    def open_register(self):
        dialog = RegisterDialog(self, self.client)
        if dialog.exec():
            # Pre-fill email after successful registration
            self.email_input.setText(dialog.email_input.text())


class RegisterDialog(QDialog):
    """Dialog for user registration"""

    def __init__(self, parent=None, client: CorrectionsClient = None):
        super().__init__(parent)
        self.client = client or get_corrections_client()
        self.setWindowTitle(tr("Create Account"))
        self.resize(450, 400)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # Header
        header = QLabel(tr("Create a new account to contribute corrections"))
        header.setStyleSheet("font-weight: bold; font-size: 14px;")
        layout.addWidget(header)

        # Form
        form = QGridLayout()

        form.addWidget(QLabel(tr("Email*:")), 0, 0)
        self.email_input = QLineEdit()
        self.email_input.setPlaceholderText("your@email.com")
        form.addWidget(self.email_input, 0, 1)

        form.addWidget(QLabel(tr("Username*:")), 1, 0)
        self.username_input = QLineEdit()
        self.username_input.setPlaceholderText(tr("Choose a username"))
        form.addWidget(self.username_input, 1, 1)

        form.addWidget(QLabel(tr("Password*:")), 2, 0)
        self.password_input = QLineEdit()
        self.password_input.setEchoMode(QLineEdit.EchoMode.Password)
        form.addWidget(self.password_input, 2, 1)

        form.addWidget(QLabel(tr("Confirm Password*:")), 3, 0)
        self.confirm_input = QLineEdit()
        self.confirm_input.setEchoMode(QLineEdit.EchoMode.Password)
        form.addWidget(self.confirm_input, 3, 1)

        form.addWidget(QLabel(tr("Full Name:")), 4, 0)
        self.name_input = QLineEdit()
        form.addWidget(self.name_input, 4, 1)

        form.addWidget(QLabel(tr("Affiliation:")), 5, 0)
        self.affiliation_input = QLineEdit()
        self.affiliation_input.setPlaceholderText(tr("University, institute, etc."))
        form.addWidget(self.affiliation_input, 5, 1)

        layout.addLayout(form)

        # Password requirements
        req_label = QLabel(tr("Password: 8+ characters, uppercase letter, number"))
        req_label.setStyleSheet("color: gray; font-size: 10px;")
        layout.addWidget(req_label)

        # Buttons
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()

        self.btn_cancel = QPushButton(tr("Cancel"))
        self.btn_cancel.clicked.connect(self.reject)
        btn_layout.addWidget(self.btn_cancel)

        self.btn_register = QPushButton(tr("Register"))
        self.btn_register.setStyleSheet("background-color: #3498db; color: white;")
        self.btn_register.clicked.connect(self.do_register)
        btn_layout.addWidget(self.btn_register)

        layout.addLayout(btn_layout)

    def do_register(self):
        email = self.email_input.text().strip()
        username = self.username_input.text().strip()
        password = self.password_input.text()
        confirm = self.confirm_input.text()

        if not email or not username or not password:
            QMessageBox.warning(self, tr("Error"), tr("Please fill all required fields"))
            return

        if password != confirm:
            QMessageBox.warning(self, tr("Error"), tr("Passwords do not match"))
            return

        self.btn_register.setEnabled(False)
        self.btn_register.setText(tr("Registering..."))

        success, message = self.client.register(
            email=email,
            username=username,
            password=password,
            full_name=self.name_input.text().strip() or None,
            affiliation=self.affiliation_input.text().strip() or None
        )

        if success:
            QMessageBox.information(self, tr("Success"), tr("Account created! You can now login."))
            self.accept()
        else:
            QMessageBox.warning(self, tr("Registration Failed"), message)
            self.btn_register.setEnabled(True)
            self.btn_register.setText(tr("Register"))


class CorrectionSubmitDialog(QDialog):
    """Dialog for submitting a new correction"""
    correction_submitted = pyqtSignal(object)  # Emits Correction object

    def __init__(
        self,
        parent=None,
        client: CorrectionsClient = None,
        document_id: str = None,
        original_text: str = None,
        shelfmark: str = None,
        system_id: str = None,
        page_number: int = None,
        context_before: str = None,
        context_after: str = None
    ):
        super().__init__(parent)
        self.client = client or get_corrections_client()
        self.document_id = document_id
        self.original_text = original_text or ""
        self.shelfmark = shelfmark
        self.system_id = system_id
        self.page_number = page_number
        self.context_before = context_before
        self.context_after = context_after

        self.setWindowTitle(tr("Submit Correction"))
        self.resize(600, 550)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # Header
        header = QLabel(tr("Submit a Transcription Correction"))
        header.setStyleSheet("font-weight: bold; font-size: 14px;")
        layout.addWidget(header)

        # Document info
        if self.document_id:
            info = QLabel(f"{tr('Document')}: {self.document_id}")
            if self.shelfmark:
                info.setText(f"{tr('Document')}: {self.shelfmark} ({self.document_id})")
            info.setStyleSheet("color: gray;")
            layout.addWidget(info)

        # Original text (read-only)
        layout.addWidget(QLabel(tr("Original Text:")))
        self.original_edit = QTextEdit()
        self.original_edit.setPlainText(self.original_text)
        self.original_edit.setReadOnly(True)
        self.original_edit.setMaximumHeight(80)
        self.original_edit.setStyleSheet("background-color: #ffebee;")
        layout.addWidget(self.original_edit)

        # Corrected text
        layout.addWidget(QLabel(tr("Corrected Text:")))
        self.corrected_edit = QTextEdit()
        self.corrected_edit.setPlainText(self.original_text)
        self.corrected_edit.setMaximumHeight(80)
        self.corrected_edit.setStyleSheet("background-color: #e8f5e9;")
        layout.addWidget(self.corrected_edit)

        # Options row
        options = QHBoxLayout()

        options.addWidget(QLabel(tr("Type:")))
        self.type_combo = QComboBox()
        self.type_combo.addItems([
            tr("Text Correction"),
            tr("Text Addition"),
            tr("Text Deletion"),
            tr("Reading Suggestion"),
            tr("Paleographic Note"),
            tr("Uncertain Reading")
        ])
        self.type_values = [
            "text_correction", "text_addition", "text_deletion",
            "reading_suggestion", "paleographic", "uncertain"
        ]
        options.addWidget(self.type_combo)

        options.addWidget(QLabel(tr("Confidence:")))
        self.confidence_spin = QDoubleSpinBox()
        self.confidence_spin.setRange(0.1, 1.0)
        self.confidence_spin.setValue(0.8)
        self.confidence_spin.setSingleStep(0.1)
        self.confidence_spin.setSuffix("")
        options.addWidget(self.confidence_spin)

        if self.page_number:
            options.addWidget(QLabel(f"{tr('Image')}: {self.page_number}"))

        options.addStretch()
        layout.addLayout(options)

        # Source reference
        layout.addWidget(QLabel(tr("Source/Reference (optional):")))
        self.source_edit = QLineEdit()
        self.source_edit.setPlaceholderText(tr("Academic source, manuscript comparison, etc."))
        layout.addWidget(self.source_edit)

        # Notes
        layout.addWidget(QLabel(tr("Notes (optional):")))
        self.notes_edit = QTextEdit()
        self.notes_edit.setMaximumHeight(60)
        self.notes_edit.setPlaceholderText(tr("Explain the reasoning for this correction..."))
        layout.addWidget(self.notes_edit)

        # Buttons
        btn_layout = QHBoxLayout()

        self.btn_save_draft = QPushButton(tr("Save as Draft"))
        self.btn_save_draft.clicked.connect(self.save_draft)
        btn_layout.addWidget(self.btn_save_draft)

        btn_layout.addStretch()

        self.btn_cancel = QPushButton(tr("Cancel"))
        self.btn_cancel.clicked.connect(self.reject)
        btn_layout.addWidget(self.btn_cancel)

        self.btn_submit = QPushButton(tr("Submit for Review"))
        self.btn_submit.setStyleSheet("background-color: #27ae60; color: white;")
        self.btn_submit.clicked.connect(self.submit_correction)
        btn_layout.addWidget(self.btn_submit)

        layout.addLayout(btn_layout)

    def save_draft(self):
        """Save as draft without submitting"""
        correction = self._create_correction()
        if correction:
            QMessageBox.information(
                self,
                tr("Draft Saved"),
                tr("Your correction has been saved as a draft. You can submit it later.")
            )
            self.correction_submitted.emit(correction)
            self.accept()

    def submit_correction(self):
        """Create and submit for review"""
        correction = self._create_correction()
        if correction:
            # Submit for review
            success, message = self.client.submit_correction(correction.id)
            if success:
                QMessageBox.information(
                    self,
                    tr("Submitted"),
                    tr("Your correction has been submitted for review. Thank you!")
                )
                self.correction_submitted.emit(correction)
                self.accept()
            else:
                QMessageBox.warning(self, tr("Error"), message)

    def _create_correction(self) -> Optional[Correction]:
        """Create the correction object"""
        original = self.original_edit.toPlainText().strip()
        corrected = self.corrected_edit.toPlainText().strip()

        if not corrected:
            QMessageBox.warning(self, tr("Error"), tr("Please enter the corrected text"))
            return None

        if original == corrected:
            QMessageBox.warning(self, tr("Error"), tr("The corrected text must be different from the original"))
            return None

        correction, error = self.client.create_correction(
            document_id=self.document_id or "unknown",
            original_text=original,
            corrected_text=corrected,
            correction_type=self.type_values[self.type_combo.currentIndex()],
            page_number=self.page_number,
            confidence_score=self.confidence_spin.value(),
            source_reference=self.source_edit.text().strip() or None,
            notes=self.notes_edit.toPlainText().strip() or None,
            shelfmark=self.shelfmark,
            system_id=self.system_id,
            context_before=self.context_before,
            context_after=self.context_after
        )

        if error and not correction:
            QMessageBox.warning(self, tr("Error"), error)
            return None

        return correction


class CorrectionsViewerDialog(QDialog):
    """Dialog for viewing corrections on a document"""

    def __init__(
        self,
        parent=None,
        client: CorrectionsClient = None,
        document_id: str = None,
        shelfmark: str = None,
        on_view_result=None,  # Callback for eye icon (view in result dialog)
        on_browse=None        # Callback for book icon (browse in browse tab)
    ):
        super().__init__(parent)
        self.client = client or get_corrections_client()
        self.document_id = document_id
        self.shelfmark = shelfmark
        self.on_view_result = on_view_result
        self.on_browse = on_browse

        self.setWindowTitle(tr("Document Corrections"))
        self.resize(900, 600)
        self.init_ui()
        self.load_corrections()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # Header with action buttons for current document
        header_layout = QHBoxLayout()
        header_text = tr("Corrections for {}").format(self.shelfmark or self.document_id)
        header = QLabel(header_text)
        header.setStyleSheet("font-weight: bold; font-size: 14px;")
        header_layout.addWidget(header)
        header_layout.addStretch()

        # Eye icon - view in result dialog
        if self.on_view_result:
            btn_view = QPushButton("👁️")
            btn_view.setToolTip(tr("View in Result Dialog"))
            btn_view.setFixedSize(32, 32)
            btn_view.clicked.connect(lambda: self._do_view_result(self.shelfmark))
            header_layout.addWidget(btn_view)

        # Book icon - browse
        if self.on_browse:
            btn_browse = QPushButton("📖")
            btn_browse.setToolTip(tr("Browse Document"))
            btn_browse.setFixedSize(32, 32)
            btn_browse.clicked.connect(lambda: self._do_browse(self.shelfmark))
            header_layout.addWidget(btn_browse)

        layout.addLayout(header_layout)

        # Table
        self.table = QTableWidget()
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels([
            tr("Status"), tr("Original"), tr("Corrected"),
            tr("Author"), tr("Score"), tr("Date")
        ])
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.doubleClicked.connect(self.on_row_double_click)
        layout.addWidget(self.table)

        # Buttons
        btn_layout = QHBoxLayout()

        self.btn_refresh = QPushButton(tr("Refresh"))
        self.btn_refresh.clicked.connect(self.load_corrections)
        btn_layout.addWidget(self.btn_refresh)

        btn_layout.addStretch()

        self.btn_close = QPushButton(tr("Close"))
        self.btn_close.clicked.connect(self.accept)
        btn_layout.addWidget(self.btn_close)

        layout.addLayout(btn_layout)

    def load_corrections(self):
        """Load corrections for the document"""
        if not self.document_id:
            return

        self.table.setRowCount(0)

        # Quick server availability check (500ms timeout) to prevent UI freeze
        if not self.client.is_server_available():
            return

        corrections = self.client.get_corrections_for_document(
            self.document_id,
            include_drafts=self.client.is_logged_in()
        )

        for correction in corrections:
            row = self.table.rowCount()
            self.table.insertRow(row)

            # Status with color
            status_item = QTableWidgetItem(correction.status)
            status_colors = {
                'approved': QColor('#27ae60'),
                'pending': QColor('#f39c12'),
                'rejected': QColor('#e74c3c'),
                'draft': QColor('#95a5a6')
            }
            if correction.status in status_colors:
                status_item.setForeground(status_colors[correction.status])
            self.table.setItem(row, 0, status_item)

            self.table.setItem(row, 1, QTableWidgetItem(correction.original_text[:50] + "..."))
            self.table.setItem(row, 2, QTableWidgetItem(correction.corrected_text[:50] + "..."))
            self.table.setItem(row, 3, QTableWidgetItem(correction.author_username or ""))

            score = correction.upvotes - correction.downvotes
            score_item = QTableWidgetItem(str(score))
            if score > 0:
                score_item.setForeground(QColor('#27ae60'))
            elif score < 0:
                score_item.setForeground(QColor('#e74c3c'))
            self.table.setItem(row, 4, score_item)

            date_str = safe_date_str(correction.created_at)
            self.table.setItem(row, 5, QTableWidgetItem(date_str))

            # Store correction ID
            self.table.item(row, 0).setData(Qt.ItemDataRole.UserRole, correction.id)

        self.table.resizeColumnsToContents()

    def _do_view_result(self, shelfmark):
        """Call view result callback"""
        if self.on_view_result and shelfmark:
            self.on_view_result(shelfmark)

    def _do_browse(self, shelfmark):
        """Call browse callback"""
        if self.on_browse and shelfmark:
            self.on_browse(shelfmark)

    def on_row_double_click(self, index):
        """Open correction detail on double-click"""
        correction_id = self.table.item(index.row(), 0).data(Qt.ItemDataRole.UserRole)
        if correction_id:
            correction = self.client.get_correction(correction_id)
            if correction:
                dialog = CorrectionDetailDialog(self, self.client, correction)
                dialog.exec()


class CorrectionDetailDialog(QDialog):
    """Dialog showing correction details with voting"""

    def __init__(
        self,
        parent=None,
        client: CorrectionsClient = None,
        correction: Correction = None,
        original_v08_text: str = None
    ):
        super().__init__(parent)
        self.client = client or get_corrections_client()
        self.correction = correction
        self.original_v08_text = original_v08_text

        self.setWindowTitle(tr("Correction Details"))
        self.resize(550, 450)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        if not self.correction:
            layout.addWidget(QLabel(tr("No correction data")))
            return

        c = self.correction

        # Status
        status_box = QGroupBox(tr("Status"))
        status_layout = QHBoxLayout(status_box)
        status_label = QLabel(c.status.upper())
        status_colors = {
            'approved': '#27ae60',
            'pending': '#f39c12',
            'rejected': '#e74c3c',
            'draft': '#95a5a6'
        }
        status_label.setStyleSheet(f"font-weight: bold; color: {status_colors.get(c.status, 'black')};")
        status_layout.addWidget(status_label)
        status_layout.addStretch()
        layout.addWidget(status_box)

        # Diff
        diff_box = QGroupBox(tr("Correction"))
        diff_layout = QVBoxLayout(diff_box)

        # Use V0.8 text if provided, otherwise fall back to stored original_text
        if self.original_v08_text:
            original_text = self.original_v08_text
            version_label = "V0.8"
        else:
            original_text = c.original_text
            version_label = tr("Original")

        diff_layout.addWidget(QLabel(f"{version_label}:"))
        orig = QTextEdit()
        orig.setPlainText(original_text)
        orig.setReadOnly(True)
        orig.setMaximumHeight(60)
        orig.setStyleSheet("background-color: #4a2020; color: #ffcccc; border: 1px solid #6a3030;")
        diff_layout.addWidget(orig)

        diff_layout.addWidget(QLabel(tr("Corrected:")))
        corr = QTextEdit()
        corr.setPlainText(c.corrected_text)
        corr.setReadOnly(True)
        corr.setMaximumHeight(60)
        corr.setStyleSheet("background-color: #1a3a1a; color: #ccffcc; border: 1px solid #2a5a2a;")
        diff_layout.addWidget(corr)

        layout.addWidget(diff_box)

        # Metadata
        meta_box = QGroupBox(tr("Details"))
        meta_layout = QGridLayout(meta_box)
        meta_layout.addWidget(QLabel(tr("Type:")), 0, 0)
        meta_layout.addWidget(QLabel(c.correction_type), 0, 1)
        meta_layout.addWidget(QLabel(tr("Author:")), 1, 0)
        meta_layout.addWidget(QLabel(c.author_username or "-"), 1, 1)
        meta_layout.addWidget(QLabel(tr("Confidence:")), 2, 0)
        meta_layout.addWidget(QLabel(f"{c.confidence_score:.0%}"), 2, 1)
        if c.source_reference:
            meta_layout.addWidget(QLabel(tr("Source:")), 3, 0)
            meta_layout.addWidget(QLabel(c.source_reference[:100]), 3, 1)
        layout.addWidget(meta_box)

        # Notes
        if c.notes:
            notes_box = QGroupBox(tr("Notes"))
            notes_layout = QVBoxLayout(notes_box)
            notes_text = QTextEdit()
            notes_text.setPlainText(c.notes)
            notes_text.setReadOnly(True)
            notes_text.setMaximumHeight(60)
            notes_text.setStyleSheet("background-color: #2a2a3a; color: #ccccff; border: 1px solid #3a3a5a;")
            notes_layout.addWidget(notes_text)
            layout.addWidget(notes_box)

        # Voting
        vote_box = QGroupBox(tr("Community Rating"))
        vote_layout = QHBoxLayout(vote_box)

        self.btn_upvote = QPushButton("👍")
        self.btn_upvote.setToolTip(tr("Upvote"))
        self.btn_upvote.clicked.connect(lambda: self.vote(1))
        vote_layout.addWidget(self.btn_upvote)

        score = c.upvotes - c.downvotes
        self.score_label = QLabel(str(score))
        self.score_label.setStyleSheet("font-size: 18px; font-weight: bold;")
        vote_layout.addWidget(self.score_label)

        self.btn_downvote = QPushButton("👎")
        self.btn_downvote.setToolTip(tr("Downvote"))
        self.btn_downvote.clicked.connect(lambda: self.vote(-1))
        vote_layout.addWidget(self.btn_downvote)

        vote_layout.addStretch()
        layout.addWidget(vote_box)

        # Close button
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        self.btn_close = QPushButton(tr("Close"))
        self.btn_close.clicked.connect(self.accept)
        btn_layout.addWidget(self.btn_close)
        layout.addLayout(btn_layout)

    def vote(self, value: int):
        """Vote on the correction"""
        if not self.client.is_logged_in():
            QMessageBox.warning(self, tr("Login Required"), tr("Please login to vote"))
            return

        success, message = self.client.vote_correction(self.correction.id, value)
        if success:
            # Refresh correction data
            self.correction = self.client.get_correction(self.correction.id)
            if self.correction:
                score = self.correction.upvotes - self.correction.downvotes
                self.score_label.setText(str(score))
        else:
            QMessageBox.warning(self, tr("Error"), message)


class CorrectionsStatusWidget(QWidget):
    """Widget showing corrections status in the status bar"""

    def __init__(self, parent=None, client: CorrectionsClient = None):
        super().__init__(parent)
        self.client = client or get_corrections_client()
        self.init_ui()

    def init_ui(self):
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        self.user_label = QLabel()
        self.user_label.setStyleSheet("color: gray;")
        layout.addWidget(self.user_label)

        self.login_btn = QPushButton(tr("Login"))
        self.login_btn.setFlat(True)
        self.login_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.login_btn.clicked.connect(self.show_login)
        layout.addWidget(self.login_btn)

        self.update_status()

    def update_status(self):
        """Update the status display"""
        if self.client.is_logged_in() and self.client.current_user:
            user = self.client.current_user
            self.user_label.setText(f"⭐ {user.reputation_score} | {user.username}")
            self.login_btn.setText(tr("Logout"))
            self.login_btn.clicked.disconnect()
            self.login_btn.clicked.connect(self.do_logout)
        else:
            self.user_label.setText("")
            self.login_btn.setText(tr("Login"))
            try:
                self.login_btn.clicked.disconnect()
            except (TypeError, RuntimeError):
                # Signal was not connected or already disconnected
                pass
            self.login_btn.clicked.connect(self.show_login)

    def show_login(self):
        """Show login dialog"""
        dialog = LoginDialog(self, self.client)
        dialog.login_success.connect(lambda _: self.update_status())
        dialog.exec()

    def do_logout(self):
        """Logout user"""
        self.client.logout()
        self.update_status()


class MyCorrectionsDialog(QDialog):
    """Dialog showing user's own corrections"""

    def __init__(self, parent=None, client: CorrectionsClient = None):
        super().__init__(parent)
        self.client = client or get_corrections_client()
        self.setWindowTitle(tr("My Corrections"))
        self.resize(900, 600)
        self.init_ui()
        self.load_corrections()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # Filter row
        filter_layout = QHBoxLayout()
        filter_layout.addWidget(QLabel(tr("Status:")))

        self.status_combo = QComboBox()
        self.status_combo.addItems([
            tr("All"), tr("Draft"), tr("Pending"),
            tr("Approved"), tr("Rejected"), tr("Needs Revision")
        ])
        self.status_values = [None, "draft", "pending", "approved", "rejected", "needs_revision"]
        self.status_combo.currentIndexChanged.connect(self.load_corrections)
        filter_layout.addWidget(self.status_combo)

        filter_layout.addStretch()
        layout.addLayout(filter_layout)

        # Table
        self.table = QTableWidget()
        self.table.setColumnCount(7)
        self.table.setHorizontalHeaderLabels([
            tr("Status"), tr("Document"), tr("Original"), tr("Corrected"),
            tr("Score"), tr("Created"), tr("Actions")
        ])
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        layout.addWidget(self.table)

        # Buttons
        btn_layout = QHBoxLayout()
        self.btn_refresh = QPushButton(tr("Refresh"))
        self.btn_refresh.clicked.connect(self.load_corrections)
        btn_layout.addWidget(self.btn_refresh)
        btn_layout.addStretch()
        self.btn_close = QPushButton(tr("Close"))
        self.btn_close.clicked.connect(self.accept)
        btn_layout.addWidget(self.btn_close)
        layout.addLayout(btn_layout)

    def load_corrections(self):
        """Load user's corrections"""
        self.table.setRowCount(0)

        status = self.status_values[self.status_combo.currentIndex()]
        corrections, total = self.client.get_my_corrections(status=status)

        for correction in corrections:
            row = self.table.rowCount()
            self.table.insertRow(row)

            # Status
            status_item = QTableWidgetItem(correction.status)
            self.table.setItem(row, 0, status_item)

            # Document
            doc = correction.shelfmark or correction.document_id
            self.table.setItem(row, 1, QTableWidgetItem(doc[:30]))

            # Original/Corrected
            self.table.setItem(row, 2, QTableWidgetItem(correction.original_text[:40] + "..."))
            self.table.setItem(row, 3, QTableWidgetItem(correction.corrected_text[:40] + "..."))

            # Score
            score = correction.upvotes - correction.downvotes
            self.table.setItem(row, 4, QTableWidgetItem(str(score)))

            # Date
            date_str = safe_date_str(correction.created_at)
            self.table.setItem(row, 5, QTableWidgetItem(date_str))

            # Actions - placeholder
            self.table.setItem(row, 6, QTableWidgetItem(""))

            # Store correction data
            self.table.item(row, 0).setData(Qt.ItemDataRole.UserRole, correction)

        self.table.resizeColumnsToContents()


class AllCorrectionsDialog(QDialog):
    """Dialog showing all corrections from all users"""

    def __init__(self, parent=None, client: CorrectionsClient = None, document_id: str = None):
        super().__init__(parent)
        self.client = client or get_corrections_client()
        self.document_id = document_id
        self.setWindowTitle(tr("All Corrections"))
        self.resize(1000, 700)
        self.init_ui()
        self.load_corrections()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # Header
        header = QLabel(tr("Browse All User Corrections"))
        header.setStyleSheet("font-weight: bold; font-size: 14px;")
        layout.addWidget(header)

        # Filter row
        filter_layout = QHBoxLayout()

        filter_layout.addWidget(QLabel(tr("Status:")))
        self.status_combo = QComboBox()
        self.status_combo.addItems([
            tr("All"), tr("Approved"), tr("Pending"), tr("Rejected")
        ])
        self.status_values = [None, "approved", "pending", "rejected"]
        self.status_combo.currentIndexChanged.connect(self.load_corrections)
        filter_layout.addWidget(self.status_combo)

        filter_layout.addWidget(QLabel(tr("Search:")))
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText(tr("Search corrections..."))
        self.search_input.returnPressed.connect(self.load_corrections)
        filter_layout.addWidget(self.search_input)

        self.btn_search = QPushButton(tr("Search"))
        self.btn_search.clicked.connect(self.load_corrections)
        filter_layout.addWidget(self.btn_search)

        filter_layout.addStretch()
        layout.addLayout(filter_layout)

        # Table
        self.table = QTableWidget()
        self.table.setColumnCount(8)
        self.table.setHorizontalHeaderLabels([
            tr("Status"), tr("Author"), tr("Document"), tr("Original"),
            tr("Corrected"), tr("Type"), tr("Score"), tr("Date")
        ])
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.doubleClicked.connect(self.on_row_double_click)
        layout.addWidget(self.table)

        # Pagination
        page_layout = QHBoxLayout()
        self.btn_prev = QPushButton(tr("Previous"))
        self.btn_prev.clicked.connect(self.prev_page)
        page_layout.addWidget(self.btn_prev)

        self.page_label = QLabel("1")
        self.page_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        page_layout.addWidget(self.page_label)

        self.btn_next = QPushButton(tr("Next"))
        self.btn_next.clicked.connect(self.next_page)
        page_layout.addWidget(self.btn_next)

        page_layout.addStretch()
        layout.addLayout(page_layout)

        # Buttons
        btn_layout = QHBoxLayout()
        self.btn_refresh = QPushButton(tr("Refresh"))
        self.btn_refresh.clicked.connect(self.load_corrections)
        btn_layout.addWidget(self.btn_refresh)
        btn_layout.addStretch()
        self.btn_close = QPushButton(tr("Close"))
        self.btn_close.clicked.connect(self.accept)
        btn_layout.addWidget(self.btn_close)
        layout.addLayout(btn_layout)

        self.current_page = 1
        self.total_pages = 1

    def load_corrections(self):
        """Load corrections from all users"""
        self.table.setRowCount(0)

        status = self.status_values[self.status_combo.currentIndex()]
        search = self.search_input.text().strip() or None

        corrections, total = self.client.get_all_corrections(
            status=status,
            document_id=self.document_id,
            search_text=search,
            page=self.current_page,
            page_size=20
        )

        self.total_pages = max(1, (total + 19) // 20)
        self.page_label.setText(f"{self.current_page} / {self.total_pages}")
        self.btn_prev.setEnabled(self.current_page > 1)
        self.btn_next.setEnabled(self.current_page < self.total_pages)

        for correction in corrections:
            row = self.table.rowCount()
            self.table.insertRow(row)

            # Status with color
            status_item = QTableWidgetItem(correction.status)
            status_colors = {
                'approved': QColor('#27ae60'),
                'pending': QColor('#f39c12'),
                'rejected': QColor('#e74c3c'),
                'draft': QColor('#95a5a6')
            }
            if correction.status in status_colors:
                status_item.setForeground(status_colors[correction.status])
            self.table.setItem(row, 0, status_item)

            # Author
            self.table.setItem(row, 1, QTableWidgetItem(correction.author_username or "-"))

            # Document
            doc = correction.shelfmark or correction.document_id or "-"
            self.table.setItem(row, 2, QTableWidgetItem(doc[:25]))

            # Original/Corrected
            orig = correction.original_text[:35] + "..." if len(correction.original_text) > 35 else correction.original_text
            corr = correction.corrected_text[:35] + "..." if len(correction.corrected_text) > 35 else correction.corrected_text
            self.table.setItem(row, 3, QTableWidgetItem(orig))
            self.table.setItem(row, 4, QTableWidgetItem(corr))

            # Type
            self.table.setItem(row, 5, QTableWidgetItem(correction.correction_type))

            # Score
            score = correction.upvotes - correction.downvotes
            score_item = QTableWidgetItem(str(score))
            if score > 0:
                score_item.setForeground(QColor('#27ae60'))
            elif score < 0:
                score_item.setForeground(QColor('#e74c3c'))
            self.table.setItem(row, 6, score_item)

            # Date
            date_str = safe_date_str(correction.created_at)
            self.table.setItem(row, 7, QTableWidgetItem(date_str))

            # Store data
            self.table.item(row, 0).setData(Qt.ItemDataRole.UserRole, correction)

        self.table.resizeColumnsToContents()

    def prev_page(self):
        if self.current_page > 1:
            self.current_page -= 1
            self.load_corrections()

    def next_page(self):
        if self.current_page < self.total_pages:
            self.current_page += 1
            self.load_corrections()

    def on_row_double_click(self, index):
        """Open correction detail"""
        correction = self.table.item(index.row(), 0).data(Qt.ItemDataRole.UserRole)
        if correction:
            dialog = CorrectionDetailDialog(self, self.client, correction)
            dialog.exec()


class DiscoveriesDialog(QDialog):
    """Dialog for viewing and creating discoveries"""

    def __init__(self, parent=None, client: CorrectionsClient = None,
                 on_view_result=None, on_browse=None):
        super().__init__(parent)
        self.client = client or get_corrections_client()
        self.on_view_result = on_view_result
        self.on_browse = on_browse
        self.setWindowTitle(tr("Discoveries Center"))
        self.resize(1000, 700)
        self.init_ui()
        self.load_stats()
        self.load_feed()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # Header
        header_layout = QHBoxLayout()
        header = QLabel(tr("Discoveries Center"))
        header.setStyleSheet("font-weight: bold; font-size: 16px;")
        header_layout.addWidget(header)

        header_layout.addStretch()

        # Create discovery button
        self.btn_create = QPushButton(tr("Share Discovery"))
        self.btn_create.setStyleSheet("background-color: #27ae60; color: white;")
        self.btn_create.clicked.connect(self.open_create_dialog)
        header_layout.addWidget(self.btn_create)

        layout.addLayout(header_layout)

        # Stats row
        self.stats_frame = QFrame()
        self.stats_frame.setStyleSheet("border-radius: 5px; border: 1px solid palette(mid);")
        stats_layout = QHBoxLayout(self.stats_frame)

        self.stat_labels = {}
        for stat_name in ['words_corrected', 'documents_edited', 'total_discoveries', 'open_questions', 'active_contributors', 'user_joins']:
            stat_widget = QFrame()
            stat_widget.setStyleSheet("border-radius: 3px; padding: 5px; border: 1px solid palette(mid);")
            stat_v = QVBoxLayout(stat_widget)
            value_label = QLabel("0")
            value_label.setStyleSheet("font-size: 18px; font-weight: bold;")
            value_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            stat_v.addWidget(value_label)

            name_labels = {
                'words_corrected': tr('Words Corrected'),
                'documents_edited': tr('Documents Edited'),
                'total_discoveries': tr('Discoveries'),
                'open_questions': tr('Open Questions'),
                'active_contributors': tr('Contributors'),
                'user_joins': tr('User Joins')
            }
            name_label = QLabel(name_labels.get(stat_name, stat_name))
            name_label.setStyleSheet("font-size: 10px; color: gray;")
            name_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            stat_v.addWidget(name_label)

            self.stat_labels[stat_name] = value_label
            stats_layout.addWidget(stat_widget)

        layout.addWidget(self.stats_frame)

        # Filter row
        filter_layout = QHBoxLayout()

        filter_layout.addWidget(QLabel(tr("Type:")))
        self.type_combo = QComboBox()
        self.type_combo.addItems([tr("All"), tr("Discoveries"), tr("Questions"), tr("Corrections"), tr("Comments"), tr("Joins")])
        self.type_values = ['all', 'discovery', 'question', 'correction', 'comment', 'join']
        self.type_combo.currentIndexChanged.connect(self.load_feed)
        filter_layout.addWidget(self.type_combo)

        filter_layout.addWidget(QLabel(tr("Period:")))
        self.period_combo = QComboBox()
        self.period_combo.addItems([tr("All Time"), tr("Today"), tr("This Week"), tr("This Month")])
        self.period_values = ['all', 'day', 'week', 'month']
        self.period_combo.currentIndexChanged.connect(self.load_feed)
        filter_layout.addWidget(self.period_combo)

        filter_layout.addStretch()
        layout.addLayout(filter_layout)

        # Feed list
        self.feed_scroll = QScrollArea()
        self.feed_scroll.setWidgetResizable(True)
        self.feed_widget = QWidget()
        self.feed_layout = QVBoxLayout(self.feed_widget)
        self.feed_layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        self.feed_scroll.setWidget(self.feed_widget)
        layout.addWidget(self.feed_scroll)

        # Buttons
        btn_layout = QHBoxLayout()
        self.btn_refresh = QPushButton(tr("Refresh"))
        self.btn_refresh.clicked.connect(self.refresh_all)
        btn_layout.addWidget(self.btn_refresh)
        btn_layout.addStretch()
        self.btn_close = QPushButton(tr("Close"))
        self.btn_close.clicked.connect(self.accept)
        btn_layout.addWidget(self.btn_close)
        layout.addLayout(btn_layout)

    def load_stats(self):
        """Load statistics"""
        stats = self.client.get_discovery_stats()
        for key, label in self.stat_labels.items():
            label.setText(str(stats.get(key, 0)))

    def load_feed(self):
        """Load activity feed"""
        # Clear existing items
        while self.feed_layout.count():
            item = self.feed_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

        item_type = self.type_values[self.type_combo.currentIndex()]
        period = self.period_values[self.period_combo.currentIndex()]

        items, total = self.client.get_feed(
            item_type=item_type if item_type != 'all' else None,
            period=period if period != 'all' else None,
            page_size=50
        )

        if not items:
            empty_label = QLabel(tr("No items found"))
            empty_label.setStyleSheet("color: gray; font-size: 14px;")
            empty_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self.feed_layout.addWidget(empty_label)
            return

        for item in items:
            item_widget = self.create_feed_item_widget(item)
            self.feed_layout.addWidget(item_widget)

        # Add stretch at end
        self.feed_layout.addStretch()

    def create_feed_item_widget(self, item: FeedItem) -> QFrame:
        """Create a widget for a feed item"""
        frame = QFrame()
        frame.setStyleSheet("""
            QFrame {
                border: 1px solid palette(mid);
                border-radius: 5px;
                padding: 10px;
                margin: 2px;
            }
            QFrame:hover {
                border-color: #27ae60;
            }
        """)

        layout = QVBoxLayout(frame)

        # Header row
        header_layout = QHBoxLayout()

        # Type badge
        type_colors = {
            'discovery': '#f39c12',
            'question': '#9b59b6',
            'correction': '#3498db',
            'comment': '#1abc9c',
            'join': '#27ae60'
        }
        type_labels = {
            'discovery': tr('Discovery'),
            'question': tr('Question'),
            'correction': tr('Correction'),
            'comment': tr('Comment'),
            'join': tr('Join (noun)')
        }
        type_badge = QLabel(type_labels.get(item.item_type, item.item_type))
        type_badge.setStyleSheet(f"""
            background: {type_colors.get(item.item_type, '#95a5a6')};
            color: white;
            padding: 2px 8px;
            border-radius: 3px;
            font-size: 10px;
        """)
        header_layout.addWidget(type_badge)

        # Pinned indicator
        if item.is_pinned:
            pin_label = QLabel("📌")
            header_layout.addWidget(pin_label)

        # Answered indicator
        if item.is_answered:
            answered_label = QLabel("✓ " + tr("Answered"))
            answered_label.setStyleSheet("color: #27ae60; font-size: 10px;")
            header_layout.addWidget(answered_label)

        # Shelfmark
        if item.shelfmark:
            shelfmark_label = QLabel(item.shelfmark)
            shelfmark_label.setStyleSheet("color: #3498db; font-family: monospace; font-size: 11px;")
            header_layout.addWidget(shelfmark_label)

            # Eye icon - view in result dialog
            if self.on_view_result:
                btn_view = QPushButton("👁️")
                btn_view.setToolTip(tr("View in Result Dialog"))
                btn_view.setFixedSize(24, 24)
                btn_view.setStyleSheet("border: none; background: transparent;")
                shelfmark = item.shelfmark
                btn_view.clicked.connect(lambda checked, s=shelfmark: self.on_view_result(s))
                header_layout.addWidget(btn_view)

            # Book icon - browse
            if self.on_browse:
                btn_browse = QPushButton("📖")
                btn_browse.setToolTip(tr("Browse Document"))
                btn_browse.setFixedSize(24, 24)
                btn_browse.setStyleSheet("border: none; background: transparent;")
                shelfmark = item.shelfmark
                btn_browse.clicked.connect(lambda checked, s=shelfmark: self.on_browse(s))
                header_layout.addWidget(btn_browse)

        header_layout.addStretch()

        # Date
        if item.created_at:
            date_label = QLabel(safe_date_str(item.created_at))
            date_label.setStyleSheet("color: gray; font-size: 10px;")
            header_layout.addWidget(date_label)

        layout.addLayout(header_layout)

        # Title
        title_label = QLabel(item.title)
        title_label.setStyleSheet("font-weight: bold; font-size: 13px;")
        title_label.setWordWrap(True)
        layout.addWidget(title_label)

        # Content preview
        if item.content_preview:
            preview = item.content_preview[:200] + "..." if len(item.content_preview) > 200 else item.content_preview
            content_label = QLabel(preview)
            content_label.setStyleSheet("font-size: 11px;")
            content_label.setWordWrap(True)
            layout.addWidget(content_label)

        # For corrections: show diff
        if item.item_type == 'correction' and item.original_text and item.corrected_text:
            diff_frame = QFrame()
            diff_frame.setStyleSheet("padding: 5px; border-radius: 3px; border: 1px solid palette(mid);")
            diff_layout = QHBoxLayout(diff_frame)

            orig_label = QLabel(f"{tr('Original')}: {item.original_text[:50]}...")
            orig_label.setStyleSheet("color: #e74c3c; font-size: 10px;")
            diff_layout.addWidget(orig_label)

            arrow_label = QLabel("→")
            diff_layout.addWidget(arrow_label)

            corr_label = QLabel(f"{tr('Corrected')}: {item.corrected_text[:50]}...")
            corr_label.setStyleSheet("color: #27ae60; font-size: 10px;")
            diff_layout.addWidget(corr_label)

            diff_layout.addStretch()
            layout.addWidget(diff_frame)

        # For joins: show both fragments with relationship
        if item.item_type == 'join':
            join_frame = QFrame()
            join_frame.setStyleSheet("padding: 5px; border-radius: 3px; border: 1px solid palette(mid); background-color: rgba(39, 174, 96, 0.1);")
            join_layout = QHBoxLayout(join_frame)

            frag_a = getattr(item, 'fragment_a', None) or ''
            frag_b = getattr(item, 'fragment_b', None) or ''
            rel_type = getattr(item, 'relationship_type', None) or ''

            rel_labels = {
                'physical_join': tr('Physical join'),
                'same_composition': tr('Same composition')
            }

            frag_a_label = QLabel(frag_a)
            frag_a_label.setStyleSheet("color: #3498db; font-family: monospace; font-size: 11px; font-weight: bold;")
            join_layout.addWidget(frag_a_label)

            arrow = QLabel("↔")
            arrow.setStyleSheet("font-size: 14px;")
            join_layout.addWidget(arrow)

            frag_b_label = QLabel(frag_b)
            frag_b_label.setStyleSheet("color: #3498db; font-family: monospace; font-size: 11px; font-weight: bold;")
            join_layout.addWidget(frag_b_label)

            if rel_type:
                rel_label = QLabel(f"({rel_labels.get(rel_type, rel_type)})")
                rel_label.setStyleSheet("color: #27ae60; font-size: 10px; margin-left: 10px;")
                join_layout.addWidget(rel_label)

            join_layout.addStretch()
            layout.addWidget(join_frame)

        # Footer row
        footer_layout = QHBoxLayout()

        # Author
        author_name = tr("Anonymous") if item.is_anonymous else (item.author_full_name or item.author_username or "-")
        author_label = QLabel(f"👤 {author_name}")
        author_label.setStyleSheet("color: gray; font-size: 10px;")
        footer_layout.addWidget(author_label)

        footer_layout.addStretch()

        # Engagement
        if item.response_count > 0:
            resp_label = QLabel(f"💬 {item.response_count}")
            resp_label.setStyleSheet("color: gray; font-size: 10px;")
            footer_layout.addWidget(resp_label)

        # Votes
        vote_score = item.upvotes - item.downvotes
        vote_label = QLabel(f"👍 {vote_score}")
        vote_color = "#27ae60" if vote_score > 0 else "#e74c3c" if vote_score < 0 else "gray"
        vote_label.setStyleSheet(f"color: {vote_color}; font-size: 10px;")
        footer_layout.addWidget(vote_label)

        layout.addLayout(footer_layout)

        # Make clickable
        frame.setCursor(Qt.CursorShape.PointingHandCursor)
        frame.mousePressEvent = lambda e, i=item: self.open_item_detail(i)

        return frame

    def open_item_detail(self, item: FeedItem):
        """Open detail dialog for a feed item"""
        if item.item_type in ('discovery', 'question'):
            # Extract numeric ID
            numeric_id = int(item.id.split('_')[-1])
            dialog = DiscoveryDetailDialog(self, self.client, numeric_id)
            dialog.exec()
            self.load_feed()  # Refresh after closing
        elif item.item_type == 'correction':
            numeric_id = int(item.id.split('_')[-1])
            correction = self.client.get_correction(numeric_id)
            if correction:
                dialog = CorrectionDetailDialog(self, self.client, correction)
                dialog.exec()
        elif item.item_type == 'join':
            # For joins, navigate to fragment A if on_browse is available
            doc_id_a = getattr(item, 'document_id_a', None)
            frag_a = getattr(item, 'fragment_a', None)
            if self.on_browse and (frag_a or doc_id_a):
                self.on_browse(frag_a or doc_id_a)

    def open_create_dialog(self):
        """Open dialog to create new discovery"""
        if not self.client.is_logged_in():
            QMessageBox.warning(self, tr("Login Required"), tr("Please login to share a discovery"))
            return

        dialog = CreateDiscoveryDialog(self, self.client)
        if dialog.exec():
            self.refresh_all()

    def refresh_all(self):
        """Refresh stats and feed"""
        self.load_stats()
        self.load_feed()


class DiscoveryDetailDialog(QDialog):
    """Dialog showing discovery details with responses"""

    def __init__(self, parent=None, client: CorrectionsClient = None, discovery_id: int = None):
        super().__init__(parent)
        self.client = client or get_corrections_client()
        self.discovery_id = discovery_id
        self.discovery = None
        self.setWindowTitle(tr("Discovery Details"))
        self.resize(700, 600)
        self.load_discovery()
        self.init_ui()

    def load_discovery(self):
        """Load discovery data"""
        if self.discovery_id:
            self.discovery = self.client.get_discovery(self.discovery_id)

    def init_ui(self):
        layout = QVBoxLayout(self)

        if not self.discovery:
            layout.addWidget(QLabel(tr("Discovery not found")))
            return

        d = self.discovery

        # Header
        header_layout = QHBoxLayout()

        # Type badge
        type_labels = {
            'discovery': tr('Discovery'),
            'question': tr('Question'),
            'identification': tr('Identification'),
            'note': tr('Note')
        }
        type_badge = QLabel(type_labels.get(d.discovery_type, d.discovery_type))
        type_badge.setStyleSheet("""
            background: #f39c12;
            color: white;
            padding: 5px 15px;
            border-radius: 3px;
            font-weight: bold;
        """)
        header_layout.addWidget(type_badge)

        if d.is_answered:
            answered_badge = QLabel(tr("Answered"))
            answered_badge.setStyleSheet("background: #27ae60; color: white; padding: 5px 10px; border-radius: 3px;")
            header_layout.addWidget(answered_badge)

        header_layout.addStretch()

        # Edit/Delete buttons (for author or admin only)
        if self.client.is_logged_in():
            current_user = self.client.current_user
            is_author = current_user and current_user.id == d.author_id
            is_admin = current_user and current_user.role == 'admin'

            if is_author or is_admin:
                self.btn_edit = QPushButton("✏️ " + tr("Edit"))
                self.btn_edit.setStyleSheet("background: #3498db; color: white; padding: 5px 10px;")
                self.btn_edit.clicked.connect(self.edit_discovery)
                header_layout.addWidget(self.btn_edit)

                self.btn_delete = QPushButton("🗑️ " + tr("Delete"))
                self.btn_delete.setStyleSheet("background: #e74c3c; color: white; padding: 5px 10px;")
                self.btn_delete.clicked.connect(self.confirm_delete_discovery)
                header_layout.addWidget(self.btn_delete)

            # Admin-only actions: Pin and Hide
            if is_admin:
                # Pin/Unpin button
                pin_text = "📌 " + (tr("Unpin") if d.is_pinned else tr("Pin"))
                self.btn_pin = QPushButton(pin_text)
                self.btn_pin.setStyleSheet("background: #9b59b6; color: white; padding: 5px 10px;")
                self.btn_pin.clicked.connect(self.toggle_pin)
                header_layout.addWidget(self.btn_pin)

                # Hide/Unhide button
                hide_text = "👁 " + (tr("Unhide") if d.is_hidden else tr("Hide"))
                self.btn_hide = QPushButton(hide_text)
                self.btn_hide.setStyleSheet("background: #95a5a6; color: white; padding: 5px 10px;")
                self.btn_hide.clicked.connect(self.toggle_hide)
                header_layout.addWidget(self.btn_hide)

        layout.addLayout(header_layout)

        # Title
        title_label = QLabel(d.title)
        title_label.setStyleSheet("font-size: 18px; font-weight: bold;")
        title_label.setWordWrap(True)
        layout.addWidget(title_label)

        # Content
        content_box = QGroupBox(tr("Description"))
        content_layout = QVBoxLayout(content_box)
        content_text = QTextEdit()
        content_text.setPlainText(d.content)
        content_text.setReadOnly(True)
        content_text.setMaximumHeight(150)
        content_layout.addWidget(content_text)
        layout.addWidget(content_box)

        # Metadata
        meta_layout = QHBoxLayout()

        # Author
        author_name = tr("Anonymous") if d.is_anonymous else (d.author_full_name or d.author_username or "-")
        meta_layout.addWidget(QLabel(f"{tr('Author')}: {author_name}"))

        if d.created_at:
            meta_layout.addWidget(QLabel(f"{tr('Date')}: {safe_date_str(d.created_at)}"))

        meta_layout.addStretch()
        layout.addLayout(meta_layout)

        # Manuscripts section (supports multiple)
        self._manuscripts = []  # List of (shelfmark, page_number)
        if d.shelfmark:
            self._manuscripts.append((d.shelfmark, d.page_number))
        if d.additional_shelfmarks:
            for ms in d.additional_shelfmarks:
                shelf = ms.get('shelfmark') or ms.get('document_id', '')
                page = ms.get('page_number')
                if shelf:
                    self._manuscripts.append((shelf, page))

        if self._manuscripts:
            if len(self._manuscripts) == 1:
                # Single manuscript - inline display
                shelf, page = self._manuscripts[0]
                self._shelfmark = shelf
                self._page_number = page
                ms_layout = QHBoxLayout()
                ms_layout.addWidget(QLabel(f"{tr('Shelfmark')}: {shelf}"))
                btn_view = QPushButton("👁")
                btn_view.setFixedWidth(30)
                btn_view.setToolTip(tr("View in Result Dialog"))
                btn_view.clicked.connect(self._view_document)
                ms_layout.addWidget(btn_view)
                btn_browse = QPushButton("📖")
                btn_browse.setFixedWidth(30)
                btn_browse.setToolTip(tr("Browse Document"))
                btn_browse.clicked.connect(self._browse_document)
                ms_layout.addWidget(btn_browse)
                ms_layout.addStretch()
                layout.addLayout(ms_layout)
            else:
                # Multiple manuscripts - show in a list
                ms_box = QGroupBox(f"{tr('Manuscripts')} ({len(self._manuscripts)})")
                ms_box_layout = QVBoxLayout(ms_box)
                self.manuscripts_list = QListWidget()
                self.manuscripts_list.setMaximumHeight(80)
                for shelf, page in self._manuscripts:
                    display = shelf
                    if page:
                        display += f" (p.{page})"
                    item = QListWidgetItem(display)
                    item.setData(Qt.ItemDataRole.UserRole, {'shelfmark': shelf, 'page': page})
                    self.manuscripts_list.addItem(item)
                ms_box_layout.addWidget(self.manuscripts_list)

                # View/Browse buttons for selected manuscript
                ms_btns = QHBoxLayout()
                btn_view_ms = QPushButton("👁 " + tr("View"))
                btn_view_ms.clicked.connect(self._view_selected_manuscript)
                ms_btns.addWidget(btn_view_ms)
                btn_browse_ms = QPushButton("📖 " + tr("Browse"))
                btn_browse_ms.clicked.connect(self._browse_selected_manuscript)
                ms_btns.addWidget(btn_browse_ms)
                ms_btns.addStretch()
                ms_box_layout.addLayout(ms_btns)
                layout.addWidget(ms_box)

        # Vote section
        vote_box = QGroupBox(tr("Voting"))
        vote_layout = QHBoxLayout(vote_box)

        self.btn_upvote = QPushButton("👍 " + tr("Upvote"))
        self.btn_upvote.clicked.connect(lambda: self.vote('up'))
        vote_layout.addWidget(self.btn_upvote)

        self.vote_label = QLabel(str(d.upvotes - d.downvotes))
        self.vote_label.setStyleSheet("font-size: 18px; font-weight: bold;")
        vote_layout.addWidget(self.vote_label)

        self.btn_downvote = QPushButton("👎 " + tr("Downvote"))
        self.btn_downvote.clicked.connect(lambda: self.vote('down'))
        vote_layout.addWidget(self.btn_downvote)

        vote_layout.addStretch()

        # Mark as answered (for questions, by author)
        if d.discovery_type == 'question' and self.client.is_logged_in():
            current_user = self.client.current_user
            if current_user and (current_user.id == d.author_id or current_user.role == 'admin'):
                self.btn_answered = QPushButton(tr("Mark as Unanswered") if d.is_answered else tr("Mark as Answered"))
                self.btn_answered.clicked.connect(self.toggle_answered)
                vote_layout.addWidget(self.btn_answered)

        layout.addWidget(vote_box)

        # Responses
        responses_box = QGroupBox(f"{tr('Responses')} ({d.response_count})")
        responses_layout = QVBoxLayout(responses_box)

        self.responses_scroll = QScrollArea()
        self.responses_scroll.setWidgetResizable(True)
        self.responses_widget = QWidget()
        self.responses_inner_layout = QVBoxLayout(self.responses_widget)
        self.responses_inner_layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        self.responses_scroll.setWidget(self.responses_widget)
        responses_layout.addWidget(self.responses_scroll)

        # Load responses
        self.load_responses()

        # Add response form
        if self.client.is_logged_in():
            self.response_input = QTextEdit()
            self.response_input.setPlaceholderText(tr("Write a response..."))
            self.response_input.setMaximumHeight(80)
            responses_layout.addWidget(self.response_input)

            response_btn_layout = QHBoxLayout()
            self.anonymous_check = QCheckBox(tr("Post anonymously"))
            response_btn_layout.addWidget(self.anonymous_check)
            response_btn_layout.addStretch()
            self.btn_submit_response = QPushButton(tr("Submit Response"))
            self.btn_submit_response.setStyleSheet("background-color: #27ae60; color: white;")
            self.btn_submit_response.clicked.connect(self.submit_response)
            response_btn_layout.addWidget(self.btn_submit_response)
            responses_layout.addLayout(response_btn_layout)
        else:
            login_label = QLabel(tr("Login to respond"))
            login_label.setStyleSheet("color: gray;")
            responses_layout.addWidget(login_label)

        layout.addWidget(responses_box)

        # Close button
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        self.btn_close = QPushButton(tr("Close"))
        self.btn_close.clicked.connect(self.accept)
        btn_layout.addWidget(self.btn_close)
        layout.addLayout(btn_layout)

    def load_responses(self):
        """Load responses for the discovery"""
        # Clear existing
        while self.responses_inner_layout.count():
            item = self.responses_inner_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

        responses = self.client.get_discovery_responses(self.discovery_id)

        if not responses:
            empty_label = QLabel(tr("No responses yet"))
            empty_label.setStyleSheet("color: gray;")
            self.responses_inner_layout.addWidget(empty_label)
            return

        for resp in responses:
            resp_frame = QFrame()
            resp_frame.setStyleSheet("background: #f5f5f5; border-radius: 3px; padding: 5px;")
            resp_layout = QVBoxLayout(resp_frame)

            # Header
            header = QHBoxLayout()
            author_name = tr("Anonymous") if resp.is_anonymous else (resp.author_username or "-")
            author_label = QLabel(f"👤 {author_name}")
            author_label.setStyleSheet("font-weight: bold; font-size: 11px;")
            header.addWidget(author_label)

            if resp.created_at:
                date_label = QLabel(safe_date_str(resp.created_at))
                date_label.setStyleSheet("color: gray; font-size: 10px;")
                header.addWidget(date_label)

            header.addStretch()
            resp_layout.addLayout(header)

            # Content
            content_label = QLabel(resp.content)
            content_label.setWordWrap(True)
            content_label.setStyleSheet("font-size: 12px;")
            resp_layout.addWidget(content_label)

            self.responses_inner_layout.addWidget(resp_frame)

    def vote(self, vote_type: str):
        """Vote on the discovery"""
        if not self.client.is_logged_in():
            QMessageBox.warning(self, tr("Login Required"), tr("Please login to vote"))
            return

        success, message = self.client.vote_discovery(self.discovery_id, vote_type)
        if success:
            # Refresh
            self.load_discovery()
            self.vote_label.setText(str(self.discovery.upvotes - self.discovery.downvotes))
        else:
            QMessageBox.warning(self, tr("Error"), message)

    def toggle_answered(self):
        """Toggle answered status"""
        success, message = self.client.mark_discovery_answered(
            self.discovery_id,
            answered=not self.discovery.is_answered
        )
        if success:
            QMessageBox.information(self, tr("Success"), message)
            self.accept()
        else:
            QMessageBox.warning(self, tr("Error"), message)

    def submit_response(self):
        """Submit a response"""
        content = self.response_input.toPlainText().strip()
        if not content:
            QMessageBox.warning(self, tr("Error"), tr("Please enter a response"))
            return

        response, error = self.client.add_discovery_response(
            self.discovery_id,
            content,
            is_anonymous=self.anonymous_check.isChecked()
        )

        if response:
            QMessageBox.information(self, tr("Success"), tr("Response submitted"))
            self.response_input.clear()
            self.load_responses()
            # Update count
            self.discovery.response_count += 1
        else:
            QMessageBox.warning(self, tr("Error"), error)

    def _view_document(self):
        """Open document in Result Dialog"""
        if not hasattr(self, '_shelfmark') or not self._shelfmark:
            return
        parent = self.parent()
        if hasattr(parent, '_open_document_result_dialog'):
            parent._open_document_result_dialog(shelfmark=self._shelfmark)

    def _browse_document(self):
        """Open document in Browse tab"""
        if not hasattr(self, '_shelfmark') or not self._shelfmark:
            return
        parent = self.parent()
        if hasattr(parent, 'meta_mgr') and parent.meta_mgr:
            # Resolve shelfmark to sys_id
            result = parent.meta_mgr.resolve_shelfmark(self._shelfmark)
            sys_id = result.get('sys_id')
            if sys_id:
                page = getattr(self, '_page_number', 1) or 1
                if hasattr(parent, 'navigate_to_browse'):
                    parent.navigate_to_browse(sys_id, page)
                elif hasattr(parent, 'tabs') and hasattr(parent, 'show_in_browse'):
                    parent.tabs.setCurrentWidget(parent.browse_tab)
                    parent.show_in_browse(sys_id, page)

    def _view_selected_manuscript(self):
        """View selected manuscript from list in Result Dialog"""
        if not hasattr(self, 'manuscripts_list'):
            return
        current = self.manuscripts_list.currentItem()
        if not current:
            # Select first if none selected
            if self.manuscripts_list.count() > 0:
                current = self.manuscripts_list.item(0)
            else:
                return
        data = current.data(Qt.ItemDataRole.UserRole)
        if data:
            self._shelfmark = data.get('shelfmark')
            self._page_number = data.get('page')
            self._view_document()

    def _browse_selected_manuscript(self):
        """Browse selected manuscript from list"""
        if not hasattr(self, 'manuscripts_list'):
            return
        current = self.manuscripts_list.currentItem()
        if not current:
            # Select first if none selected
            if self.manuscripts_list.count() > 0:
                current = self.manuscripts_list.item(0)
            else:
                return
        data = current.data(Qt.ItemDataRole.UserRole)
        if data:
            self._shelfmark = data.get('shelfmark')
            self._page_number = data.get('page')
            self._browse_document()

    def edit_discovery(self):
        """Open edit dialog for the discovery"""
        if not self.discovery:
            return

        d = self.discovery

        # Create edit dialog
        edit_dialog = QDialog(self)
        edit_dialog.setWindowTitle(tr("Edit Discovery"))
        edit_dialog.resize(500, 400)

        layout = QVBoxLayout(edit_dialog)

        # Title
        layout.addWidget(QLabel(tr("Title")))
        title_edit = QLineEdit(d.title)
        layout.addWidget(title_edit)

        # Content
        layout.addWidget(QLabel(tr("Description")))
        content_edit = QTextEdit()
        content_edit.setPlainText(d.content)
        layout.addWidget(content_edit)

        # Anonymous checkbox
        anonymous_check = QCheckBox(tr("Post anonymously"))
        anonymous_check.setChecked(d.is_anonymous)
        layout.addWidget(anonymous_check)

        # Buttons
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()

        btn_cancel = QPushButton(tr("Cancel"))
        btn_cancel.clicked.connect(edit_dialog.reject)
        btn_layout.addWidget(btn_cancel)

        btn_save = QPushButton(tr("Save"))
        btn_save.setStyleSheet("background-color: #27ae60; color: white;")

        def save_changes():
            new_title = title_edit.text().strip()
            new_content = content_edit.toPlainText().strip()

            if not new_title or not new_content:
                QMessageBox.warning(edit_dialog, tr("Error"), tr("Title and description are required"))
                return

            success, message = self.client.update_discovery(
                self.discovery_id,
                title=new_title,
                content=new_content,
                is_anonymous=anonymous_check.isChecked()
            )

            if success:
                QMessageBox.information(edit_dialog, tr("Success"), tr("Discovery updated"))
                edit_dialog.accept()
                # Update local display
                self.discovery.title = new_title
                self.discovery.content = new_content
                self.discovery.is_anonymous = anonymous_check.isChecked()
                # Refresh parent if available
                parent = self.parent()
                if hasattr(parent, '_refresh_discoveries_panel'):
                    parent._refresh_discoveries_panel(use_cache_first=False)
                # Close and signal to reload
                self.accept()
            else:
                QMessageBox.warning(edit_dialog, tr("Error"), message)

        btn_save.clicked.connect(save_changes)
        btn_layout.addWidget(btn_save)

        layout.addLayout(btn_layout)
        edit_dialog.exec()

    def confirm_delete_discovery(self):
        """Confirm and delete the discovery"""
        reply = QMessageBox.question(
            self,
            tr("Delete Discovery"),
            tr("Are you sure you want to delete this discovery? This cannot be undone."),
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )

        if reply == QMessageBox.StandardButton.Yes:
            success, message = self.client.delete_discovery(self.discovery_id)
            if success:
                QMessageBox.information(self, tr("Success"), tr("Discovery deleted"))
                # Refresh parent if available
                parent = self.parent()
                if hasattr(parent, '_refresh_discoveries_panel'):
                    parent._refresh_discoveries_panel(use_cache_first=False)
                self.accept()
            else:
                QMessageBox.warning(self, tr("Error"), message)

    def toggle_pin(self):
        """Toggle pin status of the discovery (admin only)"""
        new_pinned = not self.discovery.is_pinned
        success, message = self.client.pin_discovery(self.discovery_id, new_pinned)
        if success:
            self.discovery.is_pinned = new_pinned
            # Update button text
            self.btn_pin.setText("📌 " + (tr("Unpin") if new_pinned else tr("Pin")))
            QMessageBox.information(self, tr("Success"), tr("Discovery pinned") if new_pinned else tr("Discovery unpinned"))
            # Refresh parent
            parent = self.parent()
            if hasattr(parent, '_refresh_discoveries_panel'):
                parent._refresh_discoveries_panel(use_cache_first=False)
        else:
            QMessageBox.warning(self, tr("Error"), message)

    def toggle_hide(self):
        """Toggle hide status of the discovery (admin only)"""
        if self.discovery.is_hidden:
            success, message = self.client.unhide_discovery(self.discovery_id)
        else:
            success, message = self.client.hide_discovery(self.discovery_id)

        if success:
            self.discovery.is_hidden = not self.discovery.is_hidden
            # Update button text
            self.btn_hide.setText("👁 " + (tr("Unhide") if self.discovery.is_hidden else tr("Hide")))
            msg = tr("Discovery unhidden") if not self.discovery.is_hidden else tr("Discovery hidden")
            QMessageBox.information(self, tr("Success"), msg)
            # Refresh parent
            parent = self.parent()
            if hasattr(parent, '_refresh_discoveries_panel'):
                parent._refresh_discoveries_panel(use_cache_first=False)
        else:
            QMessageBox.warning(self, tr("Error"), message)


class CreateDiscoveryDialog(QDialog):
    """Dialog for creating a new discovery"""

    def __init__(self, parent=None, client: CorrectionsClient = None,
                 document_id: str = None, shelfmark: str = None, page_number: int = None,
                 lists_mgr=None, shelf_completer=None, meta_mgr=None):
        super().__init__(parent)
        self.client = client or get_corrections_client()
        self.document_id = document_id
        self.initial_shelfmark = shelfmark
        self.initial_page = page_number
        self.lists_mgr = lists_mgr
        self.shelf_completer = shelf_completer
        self.meta_mgr = meta_mgr
        self.shelfmarks_list = []  # List of (shelfmark, page, sys_id) tuples
        self.setWindowTitle(tr("Share a Discovery"))
        self.resize(650, 600)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # Header
        header = QLabel(tr("Share a Discovery or Ask a Question"))
        header.setStyleSheet("font-weight: bold; font-size: 14px;")
        layout.addWidget(header)

        # Type selection
        type_layout = QHBoxLayout()
        type_layout.addWidget(QLabel(tr("Type:")))
        self.type_combo = QComboBox()
        self.type_combo.addItems([
            tr("Discovery - Found something interesting"),
            tr("Question - Need help"),
            tr("Identification - Identified a text"),
            tr("Note - General observation")
        ])
        self.type_values = ['discovery', 'question', 'identification', 'note']
        type_layout.addWidget(self.type_combo)
        type_layout.addStretch()
        layout.addLayout(type_layout)

        # Title
        layout.addWidget(QLabel(tr("Title:")))
        self.title_input = QLineEdit()
        self.title_input.setPlaceholderText(tr("Brief description of your discovery"))
        layout.addWidget(self.title_input)

        # Content
        layout.addWidget(QLabel(tr("Description:")))
        self.content_input = QTextEdit()
        self.content_input.setPlaceholderText(tr("Describe your discovery in detail..."))
        layout.addWidget(self.content_input)

        # Document reference section
        doc_group = QGroupBox(tr("Document References (optional)"))
        doc_group_layout = QVBoxLayout(doc_group)

        # From Personal List selection
        if self.lists_mgr:
            from_list_layout = QHBoxLayout()
            from_list_layout.addWidget(QLabel(tr("From List:")))
            self.list_combo = QComboBox()
            self.list_combo.addItem(tr("-- Select from list --"), None)
            # Populate lists
            lists = self.lists_mgr.get_all_lists(include_recent=True)
            for lst in lists:
                display_name = lst.get('name', tr("Unnamed"))
                if lst.get('is_recent'):
                    display_name = tr("Recent")
                self.list_combo.addItem(display_name, lst.get('id'))
            self.list_combo.currentIndexChanged.connect(self._on_list_selected)
            from_list_layout.addWidget(self.list_combo, 1)
            doc_group_layout.addLayout(from_list_layout)

            # List items combo
            self.list_items_layout = QHBoxLayout()
            self.list_items_layout.addWidget(QLabel(tr("Item:")))
            self.list_items_combo = QComboBox()
            self.list_items_combo.addItem(tr("-- Select item --"), None)
            self.list_items_combo.currentIndexChanged.connect(self._on_list_item_selected)
            self.list_items_layout.addWidget(self.list_items_combo, 1)
            doc_group_layout.addLayout(self.list_items_layout)

        # Shelfmark input with autocomplete
        input_row = QHBoxLayout()
        input_row.addWidget(QLabel(tr("Shelfmark:")))
        self.shelfmark_input = QLineEdit()
        self.shelfmark_input.setPlaceholderText(tr("Type or select, e.g., T-S 13J1.1"))
        # Attach autocomplete if available
        if self.shelf_completer:
            self.shelfmark_input.setCompleter(self.shelf_completer)
        input_row.addWidget(self.shelfmark_input, 1)

        input_row.addWidget(QLabel(tr("Page:")))
        self.page_input = QSpinBox()
        self.page_input.setMinimum(0)
        self.page_input.setSpecialValueText("-")
        input_row.addWidget(self.page_input)

        # Add button
        self.btn_add_shelfmark = QPushButton("+")
        self.btn_add_shelfmark.setFixedWidth(30)
        self.btn_add_shelfmark.setToolTip(tr("Add shelfmark to list"))
        self.btn_add_shelfmark.clicked.connect(self._add_shelfmark_to_list)
        input_row.addWidget(self.btn_add_shelfmark)

        doc_group_layout.addLayout(input_row)

        # Shelfmarks list widget (for multiple shelfmarks)
        self.shelfmarks_widget = QListWidget()
        self.shelfmarks_widget.setMaximumHeight(100)
        self.shelfmarks_widget.setSelectionMode(QListWidget.SelectionMode.SingleSelection)
        doc_group_layout.addWidget(self.shelfmarks_widget)

        # Buttons row for shelfmarks list
        shelf_btns = QHBoxLayout()
        self.btn_view_doc = QPushButton("👁 " + tr("View"))
        self.btn_view_doc.setToolTip(tr("View in Result Dialog"))
        self.btn_view_doc.clicked.connect(self._view_selected_doc)
        self.btn_view_doc.setEnabled(False)
        shelf_btns.addWidget(self.btn_view_doc)

        self.btn_browse_doc = QPushButton("📖 " + tr("Browse"))
        self.btn_browse_doc.setToolTip(tr("Browse Document"))
        self.btn_browse_doc.clicked.connect(self._browse_selected_doc)
        self.btn_browse_doc.setEnabled(False)
        shelf_btns.addWidget(self.btn_browse_doc)

        shelf_btns.addStretch()

        self.btn_remove_shelfmark = QPushButton("−")
        self.btn_remove_shelfmark.setFixedWidth(30)
        self.btn_remove_shelfmark.setToolTip(tr("Remove selected shelfmark"))
        self.btn_remove_shelfmark.clicked.connect(self._remove_selected_shelfmark)
        self.btn_remove_shelfmark.setEnabled(False)
        shelf_btns.addWidget(self.btn_remove_shelfmark)

        doc_group_layout.addLayout(shelf_btns)

        # Connect selection change
        self.shelfmarks_widget.itemSelectionChanged.connect(self._on_shelfmark_selection_changed)

        layout.addWidget(doc_group)

        # Pre-populate if initial shelfmark provided
        if self.initial_shelfmark:
            self._add_shelfmark_entry(self.initial_shelfmark, self.initial_page or 0, self.document_id)

        # Anonymous option
        self.anonymous_check = QCheckBox(tr("Post anonymously"))
        layout.addWidget(self.anonymous_check)

        # Buttons
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()

        self.btn_cancel = QPushButton(tr("Cancel"))
        self.btn_cancel.clicked.connect(self.reject)
        btn_layout.addWidget(self.btn_cancel)

        self.btn_submit = QPushButton(tr("Share"))
        self.btn_submit.setStyleSheet("background-color: #27ae60; color: white;")
        self.btn_submit.clicked.connect(self.submit)
        btn_layout.addWidget(self.btn_submit)

        layout.addLayout(btn_layout)

    def submit(self):
        """Submit the discovery"""
        title = self.title_input.text().strip()
        content = self.content_input.toPlainText().strip()

        if not title or not content:
            QMessageBox.warning(self, tr("Error"), tr("Please fill in title and description"))
            return

        discovery_type = self.type_values[self.type_combo.currentIndex()]

        # Use shelfmarks list if populated, otherwise use input field
        shelfmark = None
        page_number = None
        additional_shelfmarks = None

        if self.shelfmarks_list:
            # First shelfmark is the main one
            shelfmark = self.shelfmarks_list[0][0]
            page_number = self.shelfmarks_list[0][1] if self.shelfmarks_list[0][1] and self.shelfmarks_list[0][1] > 0 else None

            # Additional shelfmarks (if more than one)
            if len(self.shelfmarks_list) > 1:
                additional_shelfmarks = []
                for shelf, page, sys_id in self.shelfmarks_list[1:]:
                    additional_shelfmarks.append({
                        'shelfmark': shelf,
                        'page_number': page if page and page > 0 else None,
                        'document_id': sys_id
                    })
        else:
            shelfmark = self.shelfmark_input.text().strip() or None
            page_number = self.page_input.value() if self.page_input.value() > 0 else None

        discovery, error = self.client.create_discovery(
            title=title,
            content=content,
            discovery_type=discovery_type,
            shelfmark=shelfmark,
            page_number=page_number,
            is_anonymous=self.anonymous_check.isChecked(),
            additional_shelfmarks=additional_shelfmarks
        )

        if discovery:
            QMessageBox.information(self, tr("Success"), tr("Discovery shared successfully!"))
            self.accept()
        else:
            QMessageBox.warning(self, tr("Error"), error)

    def _on_list_selected(self, index):
        """Handle list selection - populate items combo with shelfmark+title"""
        if not hasattr(self, 'list_items_combo') or not self.lists_mgr:
            return
        self.list_items_combo.clear()
        self.list_items_combo.addItem(tr("-- Select item --"), None)

        list_id = self.list_combo.currentData()
        if not list_id:
            return

        # Get items from the selected list
        items = self.lists_mgr.get_items_in_list(list_id)
        for item in items:
            sys_id = item.get('sys_id', '')
            shelfmark = item.get('shelfmark', '')
            title = item.get('title', '')

            # Try to get metadata if not available
            if (not shelfmark or shelfmark == 'Unknown') and self.meta_mgr and sys_id:
                shelfmark, title = self.meta_mgr.get_meta_for_id(sys_id)

            # Build display string: shelfmark + first words of title
            if shelfmark and shelfmark != 'Unknown':
                display = shelfmark
                if title:
                    # Take first 30 chars of title
                    title_preview = title[:30] + "..." if len(title) > 30 else title
                    display += f" - {title_preview}"
            else:
                display = sys_id

            item['_display_shelfmark'] = shelfmark
            item['_display_title'] = title
            self.list_items_combo.addItem(display, item)

    def _on_list_item_selected(self, index):
        """Handle list item selection - add to shelfmarks list"""
        if not hasattr(self, 'list_items_combo'):
            return
        item = self.list_items_combo.currentData()
        if not item:
            return

        sys_id = item.get('sys_id', '')
        shelfmark = item.get('_display_shelfmark') or item.get('shelfmark', '') or item.get('title', '')
        page = item.get('img') or item.get('page_number') or item.get('page', 0)
        if isinstance(page, str):
            try:
                page = int(page)
            except:
                page = 0

        # Add to list
        self._add_shelfmark_entry(shelfmark, page, sys_id)

    def _add_shelfmark_to_list(self):
        """Add current input to shelfmarks list"""
        shelfmark = self.shelfmark_input.text().strip()
        if not shelfmark:
            return

        page = self.page_input.value()

        # Try to resolve sys_id if meta_mgr available
        sys_id = None
        if self.meta_mgr:
            try:
                result = self.meta_mgr.resolve_system_by_shelfmark(shelfmark)
                if result:
                    sys_id = result.get('sys_id')
            except Exception:
                pass  # If resolution fails, just use None

        self._add_shelfmark_entry(shelfmark, page, sys_id)

        # Clear inputs
        self.shelfmark_input.clear()
        self.page_input.setValue(0)

    def _add_shelfmark_entry(self, shelfmark, page, sys_id):
        """Add an entry to the shelfmarks list widget"""
        if not shelfmark:
            return

        # Check for duplicates
        for existing in self.shelfmarks_list:
            if existing[0] == shelfmark and existing[1] == page:
                return  # Already in list

        self.shelfmarks_list.append((shelfmark, page, sys_id))

        # Display string
        display = shelfmark
        if page > 0:
            display += f" (p.{page})"

        item = QListWidgetItem(display)
        item.setData(Qt.ItemDataRole.UserRole, {'shelfmark': shelfmark, 'page': page, 'sys_id': sys_id})
        self.shelfmarks_widget.addItem(item)

    def _remove_selected_shelfmark(self):
        """Remove selected shelfmark from list"""
        current = self.shelfmarks_widget.currentItem()
        if not current:
            return

        data = current.data(Qt.ItemDataRole.UserRole)
        if data:
            # Remove from internal list
            entry = (data['shelfmark'], data['page'], data['sys_id'])
            if entry in self.shelfmarks_list:
                self.shelfmarks_list.remove(entry)

        row = self.shelfmarks_widget.row(current)
        self.shelfmarks_widget.takeItem(row)

    def _on_shelfmark_selection_changed(self):
        """Update button states based on selection"""
        has_selection = self.shelfmarks_widget.currentItem() is not None
        self.btn_remove_shelfmark.setEnabled(has_selection)
        self.btn_view_doc.setEnabled(has_selection)
        self.btn_browse_doc.setEnabled(has_selection)

    def _view_selected_doc(self):
        """Open selected document in ResultDialog"""
        current = self.shelfmarks_widget.currentItem()
        if not current:
            return
        data = current.data(Qt.ItemDataRole.UserRole)
        if not data:
            return

        sys_id = data.get('sys_id')
        shelfmark = data.get('shelfmark')

        # Find parent GUI and call its method
        parent = self.parent()
        if hasattr(parent, '_open_document_result_dialog'):
            parent._open_document_result_dialog(shelfmark=shelfmark, sys_id=sys_id)

    def _browse_selected_doc(self):
        """Open selected document in Browse tab"""
        current = self.shelfmarks_widget.currentItem()
        if not current:
            return
        data = current.data(Qt.ItemDataRole.UserRole)
        if not data:
            return

        sys_id = data.get('sys_id')
        page = data.get('page', 1) or 1

        # Find parent GUI and navigate
        parent = self.parent()
        if hasattr(parent, 'navigate_to_browse') and sys_id:
            parent.navigate_to_browse(sys_id, page)
        elif hasattr(parent, 'tabs') and hasattr(parent, 'show_in_browse'):
            parent.tabs.setCurrentWidget(parent.browse_tab)
            if sys_id:
                parent.show_in_browse(sys_id, page)


class CommentDialog(QDialog):
    """Dialog for adding a comment to a document or correction"""
    comment_submitted = pyqtSignal(object)

    def __init__(
        self,
        parent=None,
        client: CorrectionsClient = None,
        document_id: str = None,
        correction_id: int = None,
        shelfmark: str = None,
        page_number: int = None
    ):
        super().__init__(parent)
        self.client = client or get_corrections_client()
        self.document_id = document_id
        self.correction_id = correction_id
        self.shelfmark = shelfmark
        self.page_number = page_number
        self.setWindowTitle(tr("Add Comment"))
        self.resize(500, 400)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # Header
        header = QLabel(tr("Add a Comment"))
        header.setStyleSheet("font-weight: bold; font-size: 14px;")
        layout.addWidget(header)

        # Context info
        if self.shelfmark or self.document_id:
            context_text = f"{tr('Document')}: {self.shelfmark or self.document_id}"
            if self.page_number:
                context_text += f" • {tr('Page')} {self.page_number}"
            context_label = QLabel(context_text)
            context_label.setStyleSheet("color: gray;")
            layout.addWidget(context_label)

        # Comment type
        type_layout = QHBoxLayout()
        type_layout.addWidget(QLabel(tr("Type:")))
        self.type_combo = QComboBox()
        self.type_combo.addItems([
            tr("General Comment"),
            tr("Question"),
            tr("Scholarly Note"),
            tr("Suggestion"),
            tr("Issue Report")
        ])
        self.type_values = ['general', 'question', 'scholarly_note', 'suggestion', 'issue']
        type_layout.addWidget(self.type_combo)
        type_layout.addStretch()
        layout.addLayout(type_layout)

        # Comment content
        layout.addWidget(QLabel(tr("Your comment:")))
        self.content_input = QTextEdit()
        self.content_input.setPlaceholderText(tr("Write your comment here..."))
        layout.addWidget(self.content_input)

        # Options
        options_layout = QHBoxLayout()
        self.public_check = QCheckBox(tr("Public comment"))
        self.public_check.setChecked(True)
        self.public_check.setToolTip(tr("Uncheck to make this comment private (only visible to you)"))
        options_layout.addWidget(self.public_check)

        self.anonymous_check = QCheckBox(tr("Post anonymously"))
        options_layout.addWidget(self.anonymous_check)

        options_layout.addStretch()
        layout.addLayout(options_layout)

        # Buttons
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()

        self.btn_cancel = QPushButton(tr("Cancel"))
        self.btn_cancel.clicked.connect(self.reject)
        btn_layout.addWidget(self.btn_cancel)

        self.btn_submit = QPushButton(tr("Submit Comment"))
        self.btn_submit.setStyleSheet("background-color: #27ae60; color: white;")
        self.btn_submit.clicked.connect(self.submit_comment)
        btn_layout.addWidget(self.btn_submit)

        layout.addLayout(btn_layout)

    def submit_comment(self):
        """Submit the comment"""
        content = self.content_input.toPlainText().strip()
        if not content:
            QMessageBox.warning(self, tr("Error"), tr("Please enter a comment"))
            return

        comment_type = self.type_values[self.type_combo.currentIndex()]

        comment, error = self.client.create_comment(
            content=content,
            document_id=self.document_id,
            correction_id=self.correction_id,
            comment_type=comment_type,
            page_number=self.page_number,
            is_public=self.public_check.isChecked(),
            is_anonymous=self.anonymous_check.isChecked()
        )

        if comment:
            QMessageBox.information(self, tr("Success"), tr("Comment submitted successfully"))
            self.comment_submitted.emit(comment)
            self.accept()
        else:
            QMessageBox.warning(self, tr("Error"), error or tr("Failed to submit comment"))


class CommentsViewerDialog(QDialog):
    """Dialog for viewing comments on a document"""

    def __init__(
        self,
        parent=None,
        client: CorrectionsClient = None,
        document_id: str = None,
        shelfmark: str = None
    ):
        super().__init__(parent)
        self.client = client or get_corrections_client()
        self.document_id = document_id
        self.shelfmark = shelfmark
        self.setWindowTitle(tr("Document Comments"))
        self.resize(700, 500)
        self.init_ui()
        self.load_comments()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # Header
        header_text = f"{tr('Comments for')} {self.shelfmark or self.document_id}"
        header = QLabel(header_text)
        header.setStyleSheet("font-weight: bold; font-size: 14px;")
        layout.addWidget(header)

        # Add comment button
        add_btn_layout = QHBoxLayout()
        add_btn_layout.addStretch()
        self.btn_add_comment = QPushButton(tr("Add Comment"))
        self.btn_add_comment.setStyleSheet("background-color: #3498db; color: white;")
        self.btn_add_comment.clicked.connect(self.open_add_comment)
        add_btn_layout.addWidget(self.btn_add_comment)
        layout.addLayout(add_btn_layout)

        # Comments list
        self.comments_scroll = QScrollArea()
        self.comments_scroll.setWidgetResizable(True)
        self.comments_widget = QWidget()
        self.comments_layout = QVBoxLayout(self.comments_widget)
        self.comments_layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        self.comments_scroll.setWidget(self.comments_widget)
        layout.addWidget(self.comments_scroll)

        # Buttons
        btn_layout = QHBoxLayout()
        self.btn_refresh = QPushButton(tr("Refresh"))
        self.btn_refresh.clicked.connect(self.load_comments)
        btn_layout.addWidget(self.btn_refresh)
        btn_layout.addStretch()
        self.btn_close = QPushButton(tr("Close"))
        self.btn_close.clicked.connect(self.accept)
        btn_layout.addWidget(self.btn_close)
        layout.addLayout(btn_layout)

    def load_comments(self):
        """Load comments for the document"""
        # Clear existing
        while self.comments_layout.count():
            item = self.comments_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

        # Quick server availability check (500ms timeout) to prevent UI freeze
        if not self.client.is_server_available():
            unavailable_label = QLabel(tr("Server unavailable"))
            secondary_color = self.palette().color(QPalette.ColorRole.PlaceholderText).name()
            unavailable_label.setStyleSheet(f"color: {secondary_color}; font-size: 14px;")
            unavailable_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self.comments_layout.addWidget(unavailable_label)
            return

        comments = self.client.get_comments_for_document(self.document_id)

        if not comments:
            empty_label = QLabel(tr("No comments yet"))
            secondary_color = self.palette().color(QPalette.ColorRole.PlaceholderText).name()
            empty_label.setStyleSheet(f"color: {secondary_color}; font-size: 14px;")
            empty_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self.comments_layout.addWidget(empty_label)
            return

        for comment in comments:
            comment_widget = self.create_comment_widget(comment)
            self.comments_layout.addWidget(comment_widget)

    def create_comment_widget(self, comment: Comment) -> QFrame:
        """Create a widget for a comment"""
        frame = QFrame()
        # Use palette colors for dark mode support
        palette = self.palette()
        bg_color = palette.color(QPalette.ColorRole.Base).name()
        border_color = palette.color(QPalette.ColorRole.Mid).name()
        frame.setStyleSheet(f"""
            QFrame {{
                background: {bg_color};
                border: 1px solid {border_color};
                border-radius: 5px;
                padding: 10px;
                margin: 2px;
            }}
        """)

        layout = QVBoxLayout(frame)

        # Header
        header_layout = QHBoxLayout()

        # Type badge
        type_labels = {
            'general': tr('Comment'),
            'question': tr('Question'),
            'scholarly_note': tr('Scholarly Note'),
            'suggestion': tr('Suggestion'),
            'issue': tr('Issue')
        }
        type_label = QLabel(type_labels.get(comment.comment_type, comment.comment_type))
        type_label.setStyleSheet("background: #3498db; color: white; padding: 2px 8px; border-radius: 3px; font-size: 10px;")
        header_layout.addWidget(type_label)

        # Private indicator
        if not comment.is_public:
            private_label = QLabel("🔒 " + tr("Private"))
            private_label.setStyleSheet("color: #e67e22; font-size: 10px;")
            header_layout.addWidget(private_label)

        # Pinned indicator
        if comment.is_pinned:
            pin_label = QLabel("📌")
            header_layout.addWidget(pin_label)

        # Resolved indicator
        if comment.is_resolved:
            resolved_label = QLabel("✓ " + tr("Resolved"))
            resolved_label.setStyleSheet("color: #27ae60; font-size: 10px;")
            header_layout.addWidget(resolved_label)

        header_layout.addStretch()

        # Date
        if comment.created_at:
            date_label = QLabel(safe_date_str(comment.created_at))
            secondary_color = palette.color(QPalette.ColorRole.PlaceholderText).name()
            date_label.setStyleSheet(f"color: {secondary_color}; font-size: 10px;")
            header_layout.addWidget(date_label)

        layout.addLayout(header_layout)

        # Author
        author_name = tr("Anonymous") if comment.is_anonymous else (comment.author_username or "-")
        author_label = QLabel(f"👤 {author_name}")
        author_label.setStyleSheet("font-weight: bold; font-size: 11px;")
        layout.addWidget(author_label)

        # Content
        content_label = QLabel(comment.content)
        content_label.setWordWrap(True)
        content_label.setStyleSheet("font-size: 12px; margin-top: 5px;")
        layout.addWidget(content_label)

        # Reply count
        if comment.reply_count > 0:
            reply_label = QLabel(f"💬 {comment.reply_count} {tr('replies')}")
            secondary_color = palette.color(QPalette.ColorRole.PlaceholderText).name()
            reply_label.setStyleSheet(f"color: {secondary_color}; font-size: 10px; margin-top: 5px;")
            layout.addWidget(reply_label)

        return frame

    def open_add_comment(self):
        """Open dialog to add a comment"""
        if not self.client.is_logged_in():
            QMessageBox.warning(self, tr("Login Required"), tr("Please login to add a comment"))
            return

        dialog = CommentDialog(
            self,
            self.client,
            document_id=self.document_id,
            shelfmark=self.shelfmark
        )
        dialog.comment_submitted.connect(lambda _: self.load_comments())
        dialog.exec()


class MyCommentsDialog(QDialog):
    """Dialog showing user's own comments"""

    def __init__(self, parent=None, client: CorrectionsClient = None,
                 on_view_result=None, on_browse=None):
        super().__init__(parent)
        self.client = client or get_corrections_client()
        self.on_view_result = on_view_result
        self.on_browse = on_browse
        self.setWindowTitle(tr("My Comments"))
        self.resize(800, 600)
        self.init_ui()
        self.load_comments()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # Header
        header = QLabel(tr("My Comments"))
        header.setStyleSheet("font-weight: bold; font-size: 14px;")
        layout.addWidget(header)

        # Comments list
        self.comments_scroll = QScrollArea()
        self.comments_scroll.setWidgetResizable(True)
        self.comments_widget = QWidget()
        self.comments_layout = QVBoxLayout(self.comments_widget)
        self.comments_layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        self.comments_scroll.setWidget(self.comments_widget)
        layout.addWidget(self.comments_scroll)

        # Pagination
        page_layout = QHBoxLayout()
        self.btn_prev = QPushButton(tr("Previous"))
        self.btn_prev.clicked.connect(self.prev_page)
        page_layout.addWidget(self.btn_prev)

        self.page_label = QLabel("1")
        self.page_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        page_layout.addWidget(self.page_label)

        self.btn_next = QPushButton(tr("Next"))
        self.btn_next.clicked.connect(self.next_page)
        page_layout.addWidget(self.btn_next)

        page_layout.addStretch()
        layout.addLayout(page_layout)

        # Buttons
        btn_layout = QHBoxLayout()
        self.btn_refresh = QPushButton(tr("Refresh"))
        self.btn_refresh.clicked.connect(self.load_comments)
        btn_layout.addWidget(self.btn_refresh)
        btn_layout.addStretch()
        self.btn_close = QPushButton(tr("Close"))
        self.btn_close.clicked.connect(self.accept)
        btn_layout.addWidget(self.btn_close)
        layout.addLayout(btn_layout)

        self.current_page = 1
        self.total_pages = 1

    def load_comments(self):
        """Load user's comments"""
        # Clear existing
        while self.comments_layout.count():
            item = self.comments_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

        comments, total = self.client.get_my_comments(page=self.current_page)

        self.total_pages = max(1, (total + 19) // 20)
        self.page_label.setText(f"{self.current_page} / {self.total_pages}")
        self.btn_prev.setEnabled(self.current_page > 1)
        self.btn_next.setEnabled(self.current_page < self.total_pages)

        if not comments:
            empty_label = QLabel(tr("No comments yet"))
            secondary_color = self.palette().color(QPalette.ColorRole.PlaceholderText).name()
            empty_label.setStyleSheet(f"color: {secondary_color}; font-size: 14px;")
            empty_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            self.comments_layout.addWidget(empty_label)
            return

        for comment in comments:
            comment_frame = QFrame()
            # Use palette colors for dark mode support
            bg_color = self.palette().color(QPalette.ColorRole.Base).name()
            border_color = self.palette().color(QPalette.ColorRole.Mid).name()
            comment_frame.setStyleSheet(f"""
                QFrame {{
                    background: {bg_color};
                    border: 1px solid {border_color};
                    border-radius: 5px;
                    padding: 10px;
                    margin: 2px;
                }}
            """)

            frame_layout = QVBoxLayout(comment_frame)

            # Header with document info
            header_layout = QHBoxLayout()

            if comment.document_id:
                doc_label = QLabel(f"📄 {comment.document_id}")
                doc_label.setStyleSheet("color: #3498db; font-family: monospace;")
                header_layout.addWidget(doc_label)

                # Eye icon - view in result dialog
                if self.on_view_result:
                    btn_view = QPushButton("👁️")
                    btn_view.setToolTip(tr("View in Result Dialog"))
                    btn_view.setFixedSize(24, 24)
                    btn_view.setStyleSheet("border: none; background: transparent;")
                    doc_id = comment.document_id
                    btn_view.clicked.connect(lambda checked, d=doc_id: self.on_view_result(d))
                    header_layout.addWidget(btn_view)

                # Book icon - browse
                if self.on_browse:
                    btn_browse = QPushButton("📖")
                    btn_browse.setToolTip(tr("Browse Document"))
                    btn_browse.setFixedSize(24, 24)
                    btn_browse.setStyleSheet("border: none; background: transparent;")
                    doc_id = comment.document_id
                    btn_browse.clicked.connect(lambda checked, d=doc_id: self.on_browse(d))
                    header_layout.addWidget(btn_browse)

            # Type
            type_labels = {
                'general': tr('Comment'),
                'question': tr('Question'),
                'scholarly_note': tr('Scholarly Note'),
                'suggestion': tr('Suggestion'),
                'issue': tr('Issue')
            }
            type_label = QLabel(type_labels.get(comment.comment_type, comment.comment_type))
            type_label.setStyleSheet("background: #95a5a6; color: white; padding: 2px 6px; border-radius: 3px; font-size: 10px;")
            header_layout.addWidget(type_label)

            # Private indicator
            if not comment.is_public:
                private_label = QLabel("🔒")
                header_layout.addWidget(private_label)

            header_layout.addStretch()

            # Date
            if comment.created_at:
                date_label = QLabel(safe_date_str(comment.created_at))
                secondary_color = self.palette().color(QPalette.ColorRole.PlaceholderText).name()
                date_label.setStyleSheet(f"color: {secondary_color}; font-size: 10px;")
                header_layout.addWidget(date_label)

            frame_layout.addLayout(header_layout)

            # Content
            content_label = QLabel(comment.content[:200] + "..." if len(comment.content) > 200 else comment.content)
            content_label.setWordWrap(True)
            content_label.setStyleSheet("font-size: 12px;")
            frame_layout.addWidget(content_label)

            self.comments_layout.addWidget(comment_frame)

    def prev_page(self):
        if self.current_page > 1:
            self.current_page -= 1
            self.load_comments()

    def next_page(self):
        if self.current_page < self.total_pages:
            self.current_page += 1
            self.load_comments()


class TextEditorDialog(QDialog):
    """Full-featured text editor for editing transcriptions"""
    text_saved = pyqtSignal(str)  # Emits the edited text

    def __init__(
        self,
        parent=None,
        client: CorrectionsClient = None,
        document_id: str = None,
        page_number: int = None,
        shelfmark: str = None,
        original_text: str = "",
        system_id: str = None
    ):
        super().__init__(parent)
        self.client = client or get_corrections_client()
        self.document_id = document_id
        self.page_number = page_number
        self.shelfmark = shelfmark
        self.original_text = original_text
        self.system_id = system_id

        self.setWindowTitle(tr("Edit Transcription"))
        self.resize(800, 600)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # Header
        header_layout = QHBoxLayout()
        header = QLabel(tr("Edit Transcription"))
        header.setStyleSheet("font-weight: bold; font-size: 16px;")
        header_layout.addWidget(header)

        # Document info
        if self.shelfmark or self.document_id:
            doc_info = self.shelfmark or self.document_id
            if self.page_number:
                doc_info += f" • {tr('Page')} {self.page_number}"
            doc_label = QLabel(doc_info)
            doc_label.setStyleSheet("color: #3498db; font-family: monospace;")
            header_layout.addWidget(doc_label)

        header_layout.addStretch()
        layout.addLayout(header_layout)

        # Splitter for original and edited text
        splitter = QSplitter(Qt.Orientation.Horizontal)

        # Original text (read-only)
        original_group = QGroupBox(tr("Original Text"))
        original_layout = QVBoxLayout(original_group)
        self.original_edit = QTextEdit()
        self.original_edit.setPlainText(self.original_text)
        self.original_edit.setReadOnly(True)
        self.original_edit.setStyleSheet("background-color: #f5f5f5;")
        # RTL for Hebrew
        self.original_edit.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        original_layout.addWidget(self.original_edit)

        # Copy from original button
        copy_btn = QPushButton(tr("Copy to Editor"))
        copy_btn.clicked.connect(self.copy_to_editor)
        original_layout.addWidget(copy_btn)

        splitter.addWidget(original_group)

        # Edited text
        edited_group = QGroupBox(tr("Your Correction"))
        edited_layout = QVBoxLayout(edited_group)
        self.edited_edit = QTextEdit()
        self.edited_edit.setPlainText(self.original_text)
        self.edited_edit.setStyleSheet("background-color: #e8f5e9;")
        # RTL for Hebrew
        self.edited_edit.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
        self.edited_edit.textChanged.connect(self.on_text_changed)
        edited_layout.addWidget(self.edited_edit)

        # Word count
        self.word_count_label = QLabel()
        self.word_count_label.setStyleSheet("color: gray; font-size: 10px;")
        edited_layout.addWidget(self.word_count_label)

        splitter.addWidget(edited_group)

        # Equal sizes
        splitter.setSizes([400, 400])
        layout.addWidget(splitter)

        # Status bar
        status_layout = QHBoxLayout()
        self.status_label = QLabel(tr("Ready to edit"))
        self.status_label.setStyleSheet("color: gray;")
        status_layout.addWidget(self.status_label)
        status_layout.addStretch()
        layout.addLayout(status_layout)

        # Notes
        notes_group = QGroupBox(tr("Notes (explain your correction)"))
        notes_layout = QVBoxLayout(notes_group)
        self.notes_input = QTextEdit()
        self.notes_input.setMaximumHeight(80)
        self.notes_input.setPlaceholderText(tr("Explain what you changed and why..."))
        notes_layout.addWidget(notes_group)
        layout.addWidget(notes_group)

        # Buttons
        btn_layout = QHBoxLayout()

        self.btn_reset = QPushButton(tr("Reset to Original"))
        self.btn_reset.clicked.connect(self.reset_text)
        btn_layout.addWidget(self.btn_reset)

        btn_layout.addStretch()

        self.btn_cancel = QPushButton(tr("Cancel"))
        self.btn_cancel.clicked.connect(self.reject)
        btn_layout.addWidget(self.btn_cancel)

        self.btn_save_draft = QPushButton(tr("Save Draft"))
        self.btn_save_draft.clicked.connect(self.save_draft)
        btn_layout.addWidget(self.btn_save_draft)

        self.btn_submit = QPushButton(tr("Submit Correction"))
        self.btn_submit.setStyleSheet("background-color: #27ae60; color: white;")
        self.btn_submit.clicked.connect(self.submit_correction)
        btn_layout.addWidget(self.btn_submit)

        layout.addLayout(btn_layout)

        # Update word count
        self.on_text_changed()

    def on_text_changed(self):
        """Update word count and status"""
        edited_text = self.edited_edit.toPlainText()
        word_count = len(edited_text.split())
        char_count = len(edited_text)

        # Count changes
        original_words = set(self.original_text.split())
        edited_words = set(edited_text.split())
        changed_words = len(original_words.symmetric_difference(edited_words))

        self.word_count_label.setText(f"{word_count} {tr('words')} • {char_count} {tr('characters')} • {changed_words} {tr('words changed')}")

        # Update status
        if edited_text != self.original_text:
            self.status_label.setText(tr("Unsaved changes"))
            self.status_label.setStyleSheet("color: #e67e22;")
        else:
            self.status_label.setText(tr("No changes"))
            self.status_label.setStyleSheet("color: gray;")

    def copy_to_editor(self):
        """Copy original text to editor"""
        self.edited_edit.setPlainText(self.original_text)

    def reset_text(self):
        """Reset to original text"""
        reply = QMessageBox.question(
            self,
            tr("Reset"),
            tr("Are you sure you want to reset to the original text?"),
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        if reply == QMessageBox.StandardButton.Yes:
            self.edited_edit.setPlainText(self.original_text)

    def save_draft(self):
        """Save as local draft"""
        edited_text = self.edited_edit.toPlainText()
        self.text_saved.emit(edited_text)
        QMessageBox.information(self, tr("Draft Saved"), tr("Your draft has been saved locally"))

    def submit_correction(self):
        """Submit the correction to the server"""
        edited_text = self.edited_edit.toPlainText().strip()

        if edited_text == self.original_text:
            QMessageBox.warning(self, tr("No Changes"), tr("No changes to submit"))
            return

        if not self.client.is_logged_in():
            QMessageBox.warning(self, tr("Login Required"), tr("Please login to submit corrections"))
            return

        # Create correction
        correction, error = self.client.create_correction(
            document_id=self.document_id or self.system_id or "unknown",
            original_text=self.original_text,
            corrected_text=edited_text,
            correction_type="text_correction",
            page_number=self.page_number,
            shelfmark=self.shelfmark,
            system_id=self.system_id,
            notes=self.notes_input.toPlainText().strip() or None
        )

        if error and not correction:
            QMessageBox.warning(self, tr("Error"), error)
            return

        # Auto-submit
        success, message = self.client.submit_correction(correction.id)
        if success:
            QMessageBox.information(
                self,
                tr("Submitted"),
                tr("Your correction has been submitted for review")
            )
            self.text_saved.emit(edited_text)
            self.accept()
        else:
            QMessageBox.warning(self, tr("Error"), message)


class CommunityHubWidget(QWidget):
    """
    Main widget integrating all community features:
    - Discoveries
    - All Corrections
    - Comments
    - Activity Feed
    """

    def __init__(self, parent=None, client: CorrectionsClient = None):
        super().__init__(parent)
        self.client = client or get_corrections_client()
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # Header with login status
        header_layout = QHBoxLayout()
        header = QLabel(tr("Community Hub"))
        header.setStyleSheet("font-weight: bold; font-size: 16px;")
        header_layout.addWidget(header)

        header_layout.addStretch()

        self.status_widget = CorrectionsStatusWidget(self, self.client)
        header_layout.addWidget(self.status_widget)

        layout.addLayout(header_layout)

        # Tabs
        self.tabs = QTabWidget()

        # Discoveries tab
        self.discoveries_btn = QPushButton(tr("Open Discoveries Center"))
        self.discoveries_btn.clicked.connect(self.open_discoveries)
        discoveries_widget = QWidget()
        discoveries_layout = QVBoxLayout(discoveries_widget)
        discoveries_layout.addWidget(QLabel(tr("View community discoveries, questions, and share your own findings.")))
        discoveries_layout.addWidget(self.discoveries_btn)
        discoveries_layout.addStretch()
        self.tabs.addTab(discoveries_widget, tr("Discoveries"))

        # All Corrections tab
        self.corrections_btn = QPushButton(tr("Browse All Corrections"))
        self.corrections_btn.clicked.connect(self.open_all_corrections)
        corrections_widget = QWidget()
        corrections_layout = QVBoxLayout(corrections_widget)
        corrections_layout.addWidget(QLabel(tr("View corrections from all contributors.")))
        corrections_layout.addWidget(self.corrections_btn)
        corrections_layout.addStretch()
        self.tabs.addTab(corrections_widget, tr("All Corrections"))

        # My Corrections tab
        self.my_corrections_btn = QPushButton(tr("View My Corrections"))
        self.my_corrections_btn.clicked.connect(self.open_my_corrections)
        my_corrections_widget = QWidget()
        my_layout = QVBoxLayout(my_corrections_widget)
        my_layout.addWidget(QLabel(tr("View and manage your submitted corrections.")))
        my_layout.addWidget(self.my_corrections_btn)
        my_layout.addStretch()
        self.tabs.addTab(my_corrections_widget, tr("My Corrections"))

        # My Comments tab
        self.my_comments_btn = QPushButton(tr("View My Comments"))
        self.my_comments_btn.clicked.connect(self.open_my_comments)
        my_comments_widget = QWidget()
        comments_layout = QVBoxLayout(my_comments_widget)
        comments_layout.addWidget(QLabel(tr("View and manage your comments.")))
        comments_layout.addWidget(self.my_comments_btn)
        comments_layout.addStretch()
        self.tabs.addTab(my_comments_widget, tr("My Comments"))

        # Joins tab - צירופים
        self.joins_btn = QPushButton(tr("View Fragment Joins"))
        self.joins_btn.clicked.connect(self.open_joins_feed)
        joins_widget = QWidget()
        joins_layout = QVBoxLayout(joins_widget)
        joins_layout.addWidget(QLabel(tr("View user-created fragment joins and connections.")))
        joins_layout.addWidget(self.joins_btn)
        joins_layout.addStretch()
        self.tabs.addTab(joins_widget, tr("Joins"))

        layout.addWidget(self.tabs)

    def open_discoveries(self):
        """Open discoveries dialog"""
        dialog = DiscoveriesDialog(self, self.client)
        dialog.exec()

    def open_all_corrections(self):
        """Open all corrections dialog"""
        dialog = AllCorrectionsDialog(self, self.client)
        dialog.exec()

    def open_my_corrections(self):
        """Open my corrections dialog"""
        if not self.client.is_logged_in():
            QMessageBox.warning(self, tr("Login Required"), tr("Please login to view your corrections"))
            return
        dialog = MyCorrectionsDialog(self, self.client)
        dialog.exec()

    def open_my_comments(self):
        """Open my comments dialog"""
        if not self.client.is_logged_in():
            QMessageBox.warning(self, tr("Login Required"), tr("Please login to view your comments"))
            return
        dialog = MyCommentsDialog(self, self.client)
        dialog.exec()

    def open_joins_feed(self):
        """Open joins feed dialog"""
        # Get on_browse callback from parent if available
        on_browse = None
        parent = self.parent()
        if parent and hasattr(parent, 'browse_shelf_input'):
            def browse_shelfmark(shelfmark):
                parent.browse_shelf_input.setText(shelfmark)
                parent._set_last_browse_field("shelf")
                parent.browse_load()
            on_browse = browse_shelfmark

        dialog = JoinsFeedDialog(self, self.client, on_browse=on_browse)
        dialog.exec()


# =============================================================================
# Fragment Joins Dialog
# =============================================================================

class JoinsDialog(QDialog):
    """Dialog for viewing and managing fragment joins with autocomplete support"""

    def __init__(
        self,
        parent=None,
        client: CorrectionsClient = None,
        document_id: str = None,
        shelfmark: str = None,
        on_browse=None,  # Callback to browse a shelfmark
        shelf_model=None,  # QStandardItemModel for shelfmark autocomplete
        joins_mgr=None,  # JoinsManager for offline-first data
        shelf_completer=None,  # ShelfmarkCompleter instance for autocomplete
        lists_mgr=None,  # ListsManager for picking from personal lists
        meta_mgr=None  # MetadataManager for getting titles
    ):
        super().__init__(parent)
        self.client = client or get_corrections_client()
        self.document_id = document_id
        self.shelfmark = shelfmark
        self.on_browse = on_browse
        self.shelf_model = shelf_model
        self.joins_mgr = joins_mgr
        self.shelf_completer = shelf_completer
        self.lists_mgr = lists_mgr
        self.meta_mgr = meta_mgr
        self.connected_data = None

        self.setWindowTitle(tr("Fragment Joins"))
        self.resize(800, 550)
        self.init_ui()
        self.load_joins()

    def _normalize_shelfmark(self, text: str) -> str:
        """Normalize shelfmark using the canonical function from genizah_core."""
        return normalize_shelfmark(text)

    def init_ui(self):
        layout = QVBoxLayout(self)

        # Header - get canonical shelfmark from document_id via meta_mgr
        header_layout = QHBoxLayout()
        header_shelfmark = None
        if self.document_id and self.meta_mgr:
            try:
                header_shelfmark, _ = self.meta_mgr.get_meta_for_id(self.document_id)
            except:
                pass
        if not header_shelfmark or header_shelfmark == "Unknown":
            header_shelfmark = self.shelfmark
            if header_shelfmark and ' | ' in header_shelfmark:
                header_shelfmark = header_shelfmark.split(' | ')[-1]
        header_text = tr("Connected Fragments for {}").format(header_shelfmark or self.document_id or tr("Unknown"))
        self.header = QLabel(header_text)
        self.header.setStyleSheet("font-weight: bold; font-size: 14px;")
        header_layout.addWidget(self.header)
        header_layout.addStretch()

        # Info label showing cluster size
        self.cluster_info = QLabel("")
        self.cluster_info.setStyleSheet("color: #666;")
        header_layout.addWidget(self.cluster_info)

        layout.addLayout(header_layout)

        # Main content splitter
        splitter = QSplitter(Qt.Orientation.Vertical)

        # ---- Connected Fragments List ----
        fragments_group = QGroupBox(tr("Connected Fragments"))
        fragments_layout = QVBoxLayout(fragments_group)

        self.fragments_list = QListWidget()
        self.fragments_list.setMinimumHeight(120)
        self.fragments_list.itemDoubleClicked.connect(self.on_fragment_double_click)
        fragments_layout.addWidget(self.fragments_list)

        # Navigate button
        frag_btn_layout = QHBoxLayout()
        self.btn_navigate = QPushButton(tr("Navigate to Selected"))
        self.btn_navigate.setEnabled(False)
        self.btn_navigate.clicked.connect(self.navigate_to_selected)
        frag_btn_layout.addWidget(self.btn_navigate)
        frag_btn_layout.addStretch()
        fragments_layout.addLayout(frag_btn_layout)

        splitter.addWidget(fragments_group)

        # ---- Joins Table ----
        joins_group = QGroupBox(tr("Join Details"))
        joins_layout = QVBoxLayout(joins_group)

        self.table = QTableWidget()
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels([
            tr("Fragment A"), tr("Fragment B"), tr("Relationship"),
            tr("Source"), tr("Created By"), tr("Date")
        ])
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setMinimumHeight(150)
        joins_layout.addWidget(self.table)

        splitter.addWidget(joins_group)

        layout.addWidget(splitter)

        # ---- Create Join Section ----
        create_group = QGroupBox(tr("Create New Join"))
        create_layout = QGridLayout(create_group)

        # Fragment A - get canonical shelfmark from document_id via meta_mgr
        # This is the current document, we have its document_id
        canonical_shelfmark_a = None
        if self.document_id and self.meta_mgr:
            try:
                canonical_shelfmark_a, _ = self.meta_mgr.get_meta_for_id(self.document_id)
            except:
                pass
        # Fallback to extracting plain shelfmark if meta_mgr lookup failed
        if not canonical_shelfmark_a or canonical_shelfmark_a == "Unknown":
            canonical_shelfmark_a = self.shelfmark
            if canonical_shelfmark_a and ' | ' in canonical_shelfmark_a:
                canonical_shelfmark_a = canonical_shelfmark_a.split(' | ')[-1]

        create_layout.addWidget(QLabel(tr("Fragment A:")), 0, 0)
        self.frag_a_input = QLineEdit()
        self.frag_a_input.setText(canonical_shelfmark_a or self.document_id or "")
        self.frag_a_input.setReadOnly(True)  # Read-only since we use document_id
        # Use palette-aware color for dark mode support
        palette = self.palette()
        is_dark = palette.color(QPalette.ColorRole.Window).lightness() < 128
        readonly_bg = "#3a3a3a" if is_dark else "#f0f0f0"
        readonly_fg = "#cccccc" if is_dark else "#333333"
        self.frag_a_input.setStyleSheet(f"background-color: {readonly_bg}; color: {readonly_fg};")
        create_layout.addWidget(self.frag_a_input, 0, 1)

        # Fragment B (with autocomplete)
        create_layout.addWidget(QLabel(tr("Fragment B:")), 0, 2)

        # Fragment B input with "From List" button
        frag_b_layout = QHBoxLayout()
        self.frag_b_input = QLineEdit()
        self.frag_b_input.setPlaceholderText(tr("Start typing shelfmark..."))
        self._setup_completer(self.frag_b_input)
        self._selected_doc_id_b = None  # Store document_id when picked from list
        # Clear stored doc_id when user types (need to resolve from shelfmark instead)
        self.frag_b_input.textChanged.connect(lambda: setattr(self, '_selected_doc_id_b', None))
        frag_b_layout.addWidget(self.frag_b_input, 1)

        # "From List" button
        if self.lists_mgr:
            self.btn_from_list = QPushButton("📋")
            self.btn_from_list.setFixedWidth(30)
            self.btn_from_list.setToolTip(tr("Pick from personal list"))
            self.btn_from_list.clicked.connect(self._show_list_picker)
            frag_b_layout.addWidget(self.btn_from_list)

        frag_b_widget = QWidget()
        frag_b_widget.setLayout(frag_b_layout)
        frag_b_layout.setContentsMargins(0, 0, 0, 0)
        create_layout.addWidget(frag_b_widget, 0, 3)

        # Relationship type
        create_layout.addWidget(QLabel(tr("Relationship:")), 1, 0)
        self.type_combo = QComboBox()
        self.type_combo.addItem(tr("Not sure / Related"), "")
        self.type_combo.addItem(tr("Physical join"), "physical_join")
        self.type_combo.addItem(tr("Same composition"), "same_composition")
        create_layout.addWidget(self.type_combo, 1, 1)

        # Notes
        create_layout.addWidget(QLabel(tr("Notes:")), 1, 2)
        self.notes_input = QLineEdit()
        self.notes_input.setPlaceholderText(tr("Optional notes about this join"))
        create_layout.addWidget(self.notes_input, 1, 3)

        # Create button
        self.btn_create = QPushButton(tr("Create Join"))
        self.btn_create.setStyleSheet("background-color: #27ae60; color: white;")
        self.btn_create.clicked.connect(self.create_new_join)
        create_layout.addWidget(self.btn_create, 0, 4, 2, 1)

        # Login message
        if not self.client.is_logged_in():
            self.btn_create.setEnabled(False)
            login_msg = QLabel(tr("Login to create joins"))
            login_msg.setStyleSheet("color: #e74c3c; font-size: 11px;")
            create_layout.addWidget(login_msg, 2, 0, 1, 5)

        layout.addWidget(create_group)

        # ---- Bottom Buttons ----
        btn_layout = QHBoxLayout()

        self.btn_refresh = QPushButton(tr("Refresh"))
        self.btn_refresh.clicked.connect(lambda: self.load_joins(force_fresh=True))
        btn_layout.addWidget(self.btn_refresh)

        self.btn_delete = QPushButton(tr("Delete Selected Join"))
        self.btn_delete.setEnabled(False)
        self.btn_delete.clicked.connect(self.delete_selected_join)
        # Only show delete button for admins
        is_admin = (self.client.is_logged_in() and
                    self.client.current_user and
                    self.client.current_user.role == 'admin')
        self.btn_delete.setVisible(is_admin)
        self.btn_delete.setToolTip(tr("Delete selected join (admin only)"))
        btn_layout.addWidget(self.btn_delete)

        btn_layout.addStretch()

        self.btn_close = QPushButton(tr("Close"))
        self.btn_close.clicked.connect(self.accept)
        btn_layout.addWidget(self.btn_close)

        layout.addLayout(btn_layout)

        # Connect selection changes
        self.fragments_list.itemSelectionChanged.connect(self.on_fragment_selection_changed)
        self.table.itemSelectionChanged.connect(self.on_table_selection_changed)

    def _setup_completer(self, line_edit: QLineEdit):
        """Setup autocomplete on a line edit using the parent's completer/model."""
        import re

        # Try to get model from parent's shelf_completer or shelf_model
        model = None
        if self.shelf_completer and hasattr(self.shelf_completer, 'model'):
            model = self.shelf_completer.model()
        elif self.shelf_model:
            model = self.shelf_model

        if not model:
            return

        # Create a custom completer that normalizes input like ShelfmarkCompleter
        class NormalizingCompleter(QCompleter):
            @staticmethod
            def normalize(text):
                t = re.sub(r'^\s*m[\.\s]*s[\.\s]*\.?\s*', '', text, flags=re.IGNORECASE)
                return re.sub(r"[^\w\./]", "", t).lower()

            def splitPath(self, path):
                return [self.normalize(path)]

            def pathFromIndex(self, index):
                return index.data(Qt.ItemDataRole.DisplayRole)

        completer = NormalizingCompleter(model, self)
        completer.setCompletionRole(Qt.ItemDataRole.UserRole)
        completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        completer.setCompletionMode(QCompleter.CompletionMode.PopupCompletion)
        completer.setFilterMode(Qt.MatchFlag.MatchStartsWith)
        line_edit.setCompleter(completer)

    def _get_fjms_joins(self):
        """Get FJMS scholarly joins for the current document.

        Returns:
            tuple: (fjms_fragment_shelfmarks, fjms_joins, fjms_fragment_details)
        """
        try:
            from shared.fjms_service import get_fjms_service
            fjms_svc = get_fjms_service()
            if not fjms_svc.is_available() or not self.document_id:
                return [], [], []

            fjms_members = fjms_svc.get_join_group(self.document_id)
            if not fjms_members:
                return [], [], []

            plain_shelfmark = self.shelfmark
            if plain_shelfmark and ' | ' in plain_shelfmark:
                plain_shelfmark = plain_shelfmark.split(' | ')[-1]
            current_upper = plain_shelfmark.upper() if plain_shelfmark else ''

            fjms_shelfmarks = []
            fjms_joins = []
            fjms_details = []

            for member in fjms_members:
                alma_id = member.get('alma_id', '')
                # Resolve shelfmark from metadata manager
                shelf = None
                if self.meta_mgr and alma_id:
                    try:
                        shelf, _ = self.meta_mgr.get_meta_for_id(alma_id)
                    except Exception:
                        pass
                if not shelf or shelf == 'Unknown':
                    shelf = alma_id  # Fallback to raw alma_id

                fjms_shelfmarks.append(shelf)
                fjms_details.append({'shelfmark': shelf, 'document_id': alma_id})

                # Skip self-join
                if shelf.upper() == current_upper:
                    continue

                fjms_joins.append({
                    'id': None,
                    'fragment_a': plain_shelfmark or '',
                    'fragment_b': shelf,
                    'relationship_type': ', '.join(member.get('join_types', [])) if member.get('join_types') else '',
                    'source': 'FJMS',
                    'created_by_username': ', '.join(member.get('scholar_names', [])) if member.get('scholar_names') else '',
                    'created_at': '',
                    'notes': '',
                })

            return fjms_shelfmarks, fjms_joins, fjms_details
        except Exception as e:
            print(f"Error getting FJMS joins: {e}")
            return [], [], []

    def _merge_fjms_joins_into_display(self, existing_fragments_upper):
        """Merge FJMS scholarly joins into an already-populated display.

        Args:
            existing_fragments_upper: Set of uppercased shelfmark strings already in fragments list

        Returns the count of FJMS joins added.
        """
        fjms_frags, fjms_joins, fjms_details = self._get_fjms_joins()
        if not fjms_joins:
            return 0

        # Extract plain shelfmark
        plain_shelfmark = self.shelfmark
        if plain_shelfmark and ' | ' in plain_shelfmark:
            plain_shelfmark = plain_shelfmark.split(' | ')[-1]

        # Add new FJMS fragments not already displayed (deduplicate)
        for frag in fjms_frags:
            if frag.upper() in existing_fragments_upper:
                continue
            existing_fragments_upper.add(frag.upper())

            title = ""
            # Try to get title from details
            doc_id = None
            for fd in fjms_details:
                if fd.get('shelfmark', '').upper() == frag.upper():
                    doc_id = fd.get('document_id')
                    break
            if doc_id and self.meta_mgr:
                try:
                    _, title = self.meta_mgr.get_meta_for_id(doc_id)
                    if title and len(title) > 35:
                        title = title[:35] + "..."
                except Exception:
                    pass

            display_text = f"{frag} - {title}" if title else frag
            item = QListWidgetItem(display_text)
            item.setData(Qt.ItemDataRole.UserRole, frag)
            if plain_shelfmark and frag.upper() == plain_shelfmark.upper():
                item.setForeground(QColor('#27ae60'))
                item.setText(f"{display_text} ({tr('current')})")
            self.fragments_list.addItem(item)

        # Build lookup: pair -> row index for existing joins in the table
        existing_pair_rows = {}  # (fa_upper, fb_upper) -> row_index
        for r in range(self.table.rowCount()):
            fa = self.table.item(r, 0).text().upper() if self.table.item(r, 0) else ''
            fb = self.table.item(r, 1).text().upper() if self.table.item(r, 1) else ''
            if fa and fb:
                existing_pair_rows[(fa, fb)] = r
                existing_pair_rows[(fb, fa)] = r

        deduped_fjms_joins = []
        for fj in fjms_joins:
            pair = (fj.get('fragment_a', '').upper(), fj.get('fragment_b', '').upper())
            if pair in existing_pair_rows:
                # Merge FJMS source into existing row instead of dropping
                existing_row = existing_pair_rows[pair]

                # Update source column (col 3) to show dual source
                source_item = self.table.item(existing_row, 3)
                if source_item:
                    current_source = source_item.text()
                    if 'FJMS' not in current_source:
                        new_source = f"{current_source}, FJMS" if current_source else "FJMS"
                        source_item.setText(new_source)
                        source_item.setForeground(QColor('#555555'))  # Neutral color for dual-source

                # Merge scholar name (col 4) if existing is empty
                fjms_scholar = fj.get('created_by_username', '')
                existing_scholar = self.table.item(existing_row, 4)
                if fjms_scholar and existing_scholar and not existing_scholar.text():
                    existing_scholar.setText(fjms_scholar)

                # Merge relationship type (col 2) if existing is empty or Unknown
                fjms_rel = fj.get('relationship_type', '')
                if fjms_rel:
                    existing_rel = self.table.item(existing_row, 2)
                    if existing_rel:
                        existing_rel_text = existing_rel.text()
                        unknown_label = tr('Unknown')
                        if not existing_rel_text or existing_rel_text == unknown_label:
                            rel_display = {
                                'physical_join': tr('Physical join'),
                                'same_composition': tr('Same composition')
                            }.get(fjms_rel, fjms_rel)
                            existing_rel.setText(rel_display)
                        elif fjms_rel not in existing_rel_text:
                            fjms_rel_display = {
                                'physical_join': tr('Physical join'),
                                'same_composition': tr('Same composition')
                            }.get(fjms_rel, fjms_rel)
                            if fjms_rel_display not in existing_rel_text:
                                existing_rel.setText(f"{existing_rel_text}, {fjms_rel_display}")
            else:
                deduped_fjms_joins.append(fj)
                existing_pair_rows[pair] = None  # Mark as seen (no row to update)
                existing_pair_rows[(pair[1], pair[0])] = None

        # Add truly new FJMS join rows (reuse _add_pgp_join_rows pattern with FJMS styling)
        for join in deduped_fjms_joins:
            row = self.table.rowCount()
            self.table.insertRow(row)

            frag_a = join.get('fragment_a', '')
            frag_b = join.get('fragment_b', '')

            self.table.setItem(row, 0, QTableWidgetItem(frag_a))
            self.table.setItem(row, 1, QTableWidgetItem(frag_b))

            rel_type = join.get('relationship_type')
            rel_display = {
                'physical_join': tr('Physical join'),
                'same_composition': tr('Same composition')
            }.get(rel_type, rel_type or tr('Unknown'))
            self.table.setItem(row, 2, QTableWidgetItem(rel_display))

            # FJMS source label with purple color
            source_item = QTableWidgetItem('FJMS')
            source_item.setForeground(QColor('#7e22ce'))
            self.table.setItem(row, 3, source_item)

            self.table.setItem(row, 4, QTableWidgetItem(join.get('created_by_username', '')))
            self.table.setItem(row, 5, QTableWidgetItem(''))

            # Store None as join ID (prevents deletion)
            self.table.item(row, 0).setData(Qt.ItemDataRole.UserRole, None)

        return len(deduped_fjms_joins)

    def _get_pgp_joins(self):
        """Get PGP multi-fragment joins for the current document, if any.

        Returns:
            tuple: (pgp_fragment_shelfmarks, pgp_joins, pgp_fragment_details) where
                   pgp_fragment_shelfmarks is a list of shelfmark strings,
                   pgp_joins is a list of join dicts, and
                   pgp_fragment_details is a list of {shelfmark, document_id} dicts.
                   Returns ([], [], []) if no PGP joins exist.
        """
        try:
            from shared.document_service import get_document_for_fragment, get_fragments_for_document

            # Look up PGP document for this fragment
            pgp_doc = get_document_for_fragment(self.document_id)
            if not pgp_doc:
                return [], [], []

            pgpid = pgp_doc.get('pgpid')
            if not pgpid:
                return [], [], []

            # Get all fragments for this PGP document
            pgp_fragments = get_fragments_for_document(pgpid)

            # Only include if there are MORE THAN 1 unique sys_ids
            # (filters out single-fragment PGP documents - no false "Related Fragments")
            unique_sys_ids = set()
            for pf in pgp_fragments:
                sid = pf.get('sys_id')
                if sid:
                    unique_sys_ids.add(sid)

            if len(unique_sys_ids) <= 1:
                return [], [], []

            # Extract plain shelfmark for comparison
            plain_shelfmark = self.shelfmark
            if plain_shelfmark and ' | ' in plain_shelfmark:
                plain_shelfmark = plain_shelfmark.split(' | ')[-1]
            current_shelfmark_upper = plain_shelfmark.upper() if plain_shelfmark else ''

            pgp_fragment_shelfmarks = []
            pgp_joins = []
            pgp_fragment_details = []

            for pf in pgp_fragments:
                pf_shelfmark = pf.get('shelfmark', '')
                pf_sys_id = pf.get('sys_id', '')

                if not pf_shelfmark:
                    continue

                pgp_fragment_shelfmarks.append(pf_shelfmark)
                pgp_fragment_details.append({
                    'shelfmark': pf_shelfmark,
                    'document_id': pf_sys_id
                })

                # Skip current shelfmark for join entries (don't create self-join)
                if pf_shelfmark.upper() == current_shelfmark_upper:
                    continue

                pgp_joins.append({
                    'id': None,  # Not user-created, prevents delete
                    'fragment_a': plain_shelfmark or '',
                    'fragment_b': pf_shelfmark,
                    'relationship_type': 'same_composition',
                    'source': 'PGP',
                    'created_by_username': '',
                    'created_at': '',
                    'notes': f'PGP Document #{pgpid}',
                    'document_id_b': pf_sys_id  # For navigation
                })

            return pgp_fragment_shelfmarks, pgp_joins, pgp_fragment_details
        except Exception as e:
            print(f"Error getting PGP joins: {e}")
            return [], [], []

    def load_joins(self, force_fresh: bool = False):
        """Load connected fragments - from cache if available, else API

        Args:
            force_fresh: If True, try to refresh from API first (but fall back to cache if unavailable)
        """
        if not self.shelfmark and not self.document_id:
            self.cluster_info.setText(tr("No shelfmark available"))
            return

        self.fragments_list.clear()
        self.table.setRowCount(0)

        # Extract plain shelfmark (without library prefix like "Cambridge | ")
        plain_shelfmark = self.shelfmark
        if plain_shelfmark and ' | ' in plain_shelfmark:
            plain_shelfmark = plain_shelfmark.split(' | ')[-1]

        # Helper to get local cache data
        def get_local_cache():
            if not self.joins_mgr:
                return None
            cached = None
            # First try document_id lookup (most reliable)
            if self.document_id:
                cached = self.joins_mgr.get_connected_fragments_by_id(self.document_id)
            # Fall back to plain shelfmark lookup
            if (not cached or cached.get('total_fragments', 0) <= 1) and plain_shelfmark:
                cached = self.joins_mgr.get_connected_fragments(plain_shelfmark)
            return cached

        # Check server availability first (500ms timeout)
        server_available = self.client.is_server_available()

        # If server unavailable, always use local cache
        if not server_available:
            cached = get_local_cache()
            if cached and (cached.get('total_joins', 0) > 0 or cached.get('total_fragments', 0) > 1):
                self._display_cached_joins(cached)
            else:
                # No user joins in cache - still check PGP and FJMS joins
                pgp_frags, pgp_joins, pgp_details = self._get_pgp_joins()
                if pgp_joins:
                    self._display_pgp_only_joins(pgp_frags, pgp_joins, pgp_details)
                else:
                    # Try FJMS joins as last resort
                    fjms_frags, fjms_joins, fjms_details = self._get_fjms_joins()
                    if fjms_joins:
                        self._display_pgp_only_joins(fjms_frags, fjms_joins, fjms_details)
                    else:
                        self.cluster_info.setText(tr("No joins found (offline)"))
            return

        # Server is available
        # If not force_fresh, try local cache first
        if not force_fresh:
            cached = get_local_cache()
            if cached and (cached.get('total_joins', 0) > 0 or cached.get('total_fragments', 0) > 1):
                self._display_cached_joins(cached)
                return

        # Load from API - prefer document_id lookup
        if self.document_id:
            self.connected_data = self.client.get_connected_fragments_by_id(self.document_id)
        else:
            self.connected_data = self.client.get_connected_fragments(plain_shelfmark or self.shelfmark)

        if not self.connected_data:
            # No user joins from API - still check PGP and FJMS joins
            pgp_frags, pgp_joins, pgp_details = self._get_pgp_joins()
            if pgp_joins:
                self._display_pgp_only_joins(pgp_frags, pgp_joins, pgp_details)
            else:
                # Try FJMS joins as last resort
                fjms_frags, fjms_joins, fjms_details = self._get_fjms_joins()
                if fjms_joins:
                    self._display_pgp_only_joins(fjms_frags, fjms_joins, fjms_details)
                else:
                    self.cluster_info.setText(tr("No joins found"))
            return

        # Update local cache with fetched data
        if self.joins_mgr and self.connected_data:
            self._update_local_cache(self.connected_data)

        self._display_connected_data(self.connected_data)

    def _update_local_cache(self, connected_data):
        """Update the local JoinsManager cache with data fetched from API."""
        if not self.joins_mgr:
            return

        # Get joins from the response
        joins = []
        if hasattr(connected_data, 'joins'):
            joins = connected_data.joins
        elif isinstance(connected_data, dict):
            joins = connected_data.get('joins', [])

        # Get fragment_details for document_id mapping
        fragment_details = []
        if hasattr(connected_data, 'fragment_details'):
            fragment_details = connected_data.fragment_details
        elif isinstance(connected_data, dict):
            fragment_details = connected_data.get('fragment_details', [])

        # Build document_id mapping from fragment_details
        doc_id_map = {}
        for fd in fragment_details:
            if hasattr(fd, 'shelfmark') and hasattr(fd, 'document_id'):
                if fd.document_id:
                    doc_id_map[fd.shelfmark] = fd.document_id
            elif isinstance(fd, dict):
                shelf = fd.get('shelfmark', '')
                doc_id = fd.get('document_id')
                if shelf and doc_id:
                    doc_id_map[shelf] = doc_id

        # Add each join to the local cache
        for join in joins:
            join_dict = {}
            if hasattr(join, 'id'):
                # It's a Join object
                join_dict = {
                    'id': join.id,
                    'fragment_a': join.fragment_a,
                    'fragment_b': join.fragment_b,
                    'document_id_a': getattr(join, 'document_id_a', None) or doc_id_map.get(join.fragment_a),
                    'document_id_b': getattr(join, 'document_id_b', None) or doc_id_map.get(join.fragment_b),
                    'relationship_type': join.relationship_type,
                    'notes': join.notes,
                    'source': join.source,
                    'created_by_username': getattr(join, 'created_by_username', None),
                    'created_at': getattr(join, 'created_at', None)
                }
            elif isinstance(join, dict):
                join_dict = {
                    'id': join.get('id'),
                    'fragment_a': join.get('fragment_a'),
                    'fragment_b': join.get('fragment_b'),
                    'document_id_a': join.get('document_id_a') or doc_id_map.get(join.get('fragment_a', '')),
                    'document_id_b': join.get('document_id_b') or doc_id_map.get(join.get('fragment_b', '')),
                    'relationship_type': join.get('relationship_type'),
                    'notes': join.get('notes'),
                    'source': join.get('source'),
                    'created_by_username': join.get('created_by_username'),
                    'created_at': join.get('created_at')
                }

            if join_dict.get('id'):
                # Add to local cache
                self.joins_mgr.data['joins'][join_dict['id']] = join_dict
                self.joins_mgr._index_join(join_dict)

        # Save the updated cache
        self.joins_mgr.save()

    def _display_pgp_only_joins(self, pgp_frags, pgp_joins, pgp_details):
        """Display PGP-only joins when no user joins exist."""
        # Extract plain shelfmark for comparison
        plain_shelfmark = self.shelfmark
        if plain_shelfmark and ' | ' in plain_shelfmark:
            plain_shelfmark = plain_shelfmark.split(' | ')[-1]

        total_frags = len(set(pgp_frags))
        total_joins = len(pgp_joins)
        self.cluster_info.setText(
            tr("{} fragments in cluster, {} joins (PGP)").format(total_frags, total_joins)
        )

        # Build shelfmark -> document_id map from PGP details
        shelfmark_to_docid = {}
        for pd in pgp_details:
            shelf = pd.get('shelfmark', '')
            doc_id = pd.get('document_id')
            if shelf and doc_id:
                shelfmark_to_docid[shelf.upper()] = doc_id

        # Build fallback map from csv_bank
        shelf_to_sys = {}
        if self.meta_mgr and hasattr(self.meta_mgr, 'csv_bank'):
            for sys_id, meta in self.meta_mgr.csv_bank.items():
                shelf = meta.get('shelfmark', '')
                if shelf:
                    shelf_to_sys[self._normalize_shelfmark(shelf)] = sys_id

        # Populate fragments list (deduplicated)
        seen_upper = set()
        for frag in pgp_frags:
            if frag.upper() in seen_upper:
                continue
            seen_upper.add(frag.upper())

            title = ""
            doc_id = shelfmark_to_docid.get(frag.upper())
            if not doc_id and self.meta_mgr:
                norm_frag = self._normalize_shelfmark(frag)
                doc_id = shelf_to_sys.get(norm_frag)
            if doc_id and self.meta_mgr:
                try:
                    _, title = self.meta_mgr.get_meta_for_id(doc_id)
                    if title and len(title) > 35:
                        title = title[:35] + "..."
                except:
                    pass

            display_text = f"{frag} - {title}" if title else frag
            item = QListWidgetItem(display_text)
            item.setData(Qt.ItemDataRole.UserRole, frag)
            if plain_shelfmark and frag.upper() == plain_shelfmark.upper():
                item.setForeground(QColor('#27ae60'))
                item.setText(f"{display_text} ({tr('current')})")
            self.fragments_list.addItem(item)

        # Populate joins table with PGP joins
        self._add_pgp_join_rows(pgp_joins, plain_shelfmark)

        # Also merge FJMS joins
        existing_frags_upper = set(f.upper() for f in pgp_frags)
        fjms_count = self._merge_fjms_joins_into_display(existing_frags_upper)
        if fjms_count > 0:
            new_total_joins = total_joins + fjms_count
            new_total_frags = self.fragments_list.count()
            self.cluster_info.setText(
                tr("{} fragments in cluster, {} joins").format(new_total_frags, new_total_joins)
            )

        self.table.resizeColumnsToContents()

    def _add_pgp_join_rows(self, pgp_joins, plain_shelfmark):
        """Add PGP join rows to the joins table with green PGP source styling."""
        for join in pgp_joins:
            row = self.table.rowCount()
            self.table.insertRow(row)

            frag_a = join.get('fragment_a', '')
            frag_b = join.get('fragment_b', '')

            self.table.setItem(row, 0, QTableWidgetItem(frag_a))
            self.table.setItem(row, 1, QTableWidgetItem(frag_b))

            rel_type = join.get('relationship_type')
            rel_display = {
                'physical_join': tr('Physical join'),
                'same_composition': tr('Same composition')
            }.get(rel_type, rel_type or tr('Unknown'))
            self.table.setItem(row, 2, QTableWidgetItem(rel_display))

            # PGP source label with green color
            source_item = QTableWidgetItem(join.get('source', 'PGP'))
            source_item.setForeground(QColor('#27ae60'))
            self.table.setItem(row, 3, source_item)

            self.table.setItem(row, 4, QTableWidgetItem(join.get('created_by_username', '')))

            date_str = safe_date_str(join.get('created_at'))
            self.table.setItem(row, 5, QTableWidgetItem(date_str))

            # Store None as join ID (prevents deletion)
            self.table.item(row, 0).setData(Qt.ItemDataRole.UserRole, None)

            # Highlight direct joins (PGP joins always involve current shelfmark)
            is_direct = (frag_a.upper() == plain_shelfmark.upper() or
                         frag_b.upper() == plain_shelfmark.upper()) if plain_shelfmark else False
            if is_direct:
                palette = self.palette()
                is_dark = palette.color(QPalette.ColorRole.Window).lightness() < 128
                highlight_color = QColor('#1b4332') if is_dark else QColor('#e8f5e9')
                for col in range(6):
                    item = self.table.item(row, col)
                    if item:
                        item.setBackground(highlight_color)

    def _merge_pgp_joins_into_display(self, existing_fragments_upper):
        """Merge PGP joins into an already-populated display.

        Args:
            existing_fragments_upper: Set of uppercased shelfmark strings already in fragments list

        Modifies the fragments list and joins table in-place, adding PGP joins.
        Returns the count of PGP joins added.
        """
        pgp_frags, pgp_joins, pgp_details = self._get_pgp_joins()
        if not pgp_joins:
            return 0

        # Extract plain shelfmark
        plain_shelfmark = self.shelfmark
        if plain_shelfmark and ' | ' in plain_shelfmark:
            plain_shelfmark = plain_shelfmark.split(' | ')[-1]

        # Build shelfmark -> document_id map
        shelfmark_to_docid = {}
        for pd in pgp_details:
            shelf = pd.get('shelfmark', '')
            doc_id = pd.get('document_id')
            if shelf and doc_id:
                shelfmark_to_docid[shelf.upper()] = doc_id

        # Build fallback map from csv_bank
        shelf_to_sys = {}
        if self.meta_mgr and hasattr(self.meta_mgr, 'csv_bank'):
            for sys_id, meta in self.meta_mgr.csv_bank.items():
                shelf = meta.get('shelfmark', '')
                if shelf:
                    shelf_to_sys[self._normalize_shelfmark(shelf)] = sys_id

        # Add new PGP fragments not already displayed (deduplicate)
        for frag in pgp_frags:
            if frag.upper() in existing_fragments_upper:
                continue
            existing_fragments_upper.add(frag.upper())

            title = ""
            doc_id = shelfmark_to_docid.get(frag.upper())
            if not doc_id and self.meta_mgr:
                norm_frag = self._normalize_shelfmark(frag)
                doc_id = shelf_to_sys.get(norm_frag)
            if doc_id and self.meta_mgr:
                try:
                    _, title = self.meta_mgr.get_meta_for_id(doc_id)
                    if title and len(title) > 35:
                        title = title[:35] + "..."
                except:
                    pass

            display_text = f"{frag} - {title}" if title else frag
            item = QListWidgetItem(display_text)
            item.setData(Qt.ItemDataRole.UserRole, frag)
            if plain_shelfmark and frag.upper() == plain_shelfmark.upper():
                item.setForeground(QColor('#27ae60'))
                item.setText(f"{display_text} ({tr('current')})")
            self.fragments_list.addItem(item)

        # Deduplicate PGP joins against existing user joins in the table
        existing_pairs = set()
        for r in range(self.table.rowCount()):
            fa = self.table.item(r, 0).text().upper() if self.table.item(r, 0) else ''
            fb = self.table.item(r, 1).text().upper() if self.table.item(r, 1) else ''
            if fa and fb:
                existing_pairs.add((fa, fb))
                existing_pairs.add((fb, fa))

        deduped_pgp_joins = []
        for pj in pgp_joins:
            pair = (pj.get('fragment_a', '').upper(), pj.get('fragment_b', '').upper())
            if pair not in existing_pairs:
                deduped_pgp_joins.append(pj)
                existing_pairs.add(pair)
                existing_pairs.add((pair[1], pair[0]))

        # Add PGP join rows
        self._add_pgp_join_rows(deduped_pgp_joins, plain_shelfmark)
        return len(deduped_pgp_joins)

    def _display_cached_joins(self, cached: dict):
        """Display joins from the local cache."""
        total_frags = cached.get('total_fragments', 0)
        total_joins = cached.get('total_joins', 0)

        if total_frags <= 1:
            self.cluster_info.setText(tr("No joins found"))
        else:
            self.cluster_info.setText(tr("{} fragments in cluster, {} joins").format(total_frags, total_joins))

        # Extract plain shelfmark for comparison (without library prefix)
        plain_shelfmark = self.shelfmark
        if plain_shelfmark and ' | ' in plain_shelfmark:
            plain_shelfmark = plain_shelfmark.split(' | ')[-1]

        # Populate fragments list - use fragment_details if available for document_id lookup
        fragment_details = cached.get('fragment_details', [])
        fragments = cached.get('fragments', [])

        # Build a map of shelfmark -> document_id from fragment_details
        shelfmark_to_docid = {}
        for fd in fragment_details:
            shelf = fd.get('shelfmark', '')
            doc_id = fd.get('document_id')
            if shelf and doc_id:
                shelfmark_to_docid[shelf.upper()] = doc_id

        # Build fallback map from csv_bank for shelfmarks without document_id
        shelf_to_sys = {}
        if self.meta_mgr and hasattr(self.meta_mgr, 'csv_bank'):
            for sys_id, meta in self.meta_mgr.csv_bank.items():
                shelf = meta.get('shelfmark', '')
                if shelf:
                    shelf_to_sys[self._normalize_shelfmark(shelf)] = sys_id

        for frag in fragments:
            # Try to get title using document_id
            title = ""
            doc_id = shelfmark_to_docid.get(frag.upper())

            # Fallback: look up sys_id from csv_bank using normalized shelfmark
            if not doc_id and self.meta_mgr:
                norm_frag = self._normalize_shelfmark(frag)
                doc_id = shelf_to_sys.get(norm_frag)

            if doc_id and self.meta_mgr:
                try:
                    _, title = self.meta_mgr.get_meta_for_id(doc_id)
                    if title and len(title) > 35:
                        title = title[:35] + "..."
                except:
                    pass

            display_text = f"{frag} - {title}" if title else frag
            item = QListWidgetItem(display_text)
            item.setData(Qt.ItemDataRole.UserRole, frag)  # Store plain shelfmark for navigation

            # Compare with plain shelfmark (joins store plain shelfmarks)
            if plain_shelfmark and frag.upper() == plain_shelfmark.upper():
                item.setForeground(QColor('#27ae60'))
                item.setText(f"{display_text} ({tr('current')})")
            self.fragments_list.addItem(item)

        # Populate joins table
        for join in cached.get('joins', []):
            row = self.table.rowCount()
            self.table.insertRow(row)

            frag_a = join.get('fragment_a', '')
            frag_b = join.get('fragment_b', '')

            self.table.setItem(row, 0, QTableWidgetItem(frag_a))
            self.table.setItem(row, 1, QTableWidgetItem(frag_b))

            rel_type = join.get('relationship_type')
            rel_display = {
                'physical_join': tr('Physical join'),
                'same_composition': tr('Same composition')
            }.get(rel_type, rel_type or tr('Unknown'))
            self.table.setItem(row, 2, QTableWidgetItem(rel_display))

            self.table.setItem(row, 3, QTableWidgetItem(join.get('source', 'user')))
            self.table.setItem(row, 4, QTableWidgetItem(join.get('created_by_username', '')))

            date_str = safe_date_str(join.get('created_at'))
            self.table.setItem(row, 5, QTableWidgetItem(date_str))

            self.table.item(row, 0).setData(Qt.ItemDataRole.UserRole, join.get('id'))

            # Highlight direct joins (joins that involve the current shelfmark)
            is_direct = (frag_a.upper() == plain_shelfmark.upper() or
                         frag_b.upper() == plain_shelfmark.upper()) if plain_shelfmark else False
            if is_direct:
                # Use palette-aware color for dark mode support
                palette = self.palette()
                is_dark = palette.color(QPalette.ColorRole.Window).lightness() < 128
                highlight_color = QColor('#1b4332') if is_dark else QColor('#e8f5e9')
                for col in range(6):
                    item = self.table.item(row, col)
                    if item:
                        item.setBackground(highlight_color)

        # Merge PGP joins into display
        existing_frags_upper = set(f.upper() for f in fragments)
        pgp_count = self._merge_pgp_joins_into_display(existing_frags_upper)

        # Merge FJMS scholarly joins into display
        fjms_count = self._merge_fjms_joins_into_display(existing_frags_upper)

        extra_count = pgp_count + fjms_count
        if extra_count > 0:
            new_total_joins = total_joins + extra_count
            new_total_frags = self.fragments_list.count()
            self.cluster_info.setText(
                tr("{} fragments in cluster, {} joins").format(new_total_frags, new_total_joins)
            )

        self.table.resizeColumnsToContents()

    def _display_connected_data(self, data):
        """Display joins from API response."""
        total_frags = data.total_fragments
        total_joins = data.total_joins

        if total_frags <= 1:
            self.cluster_info.setText(tr("No joins found"))
        else:
            self.cluster_info.setText(tr("{} fragments in cluster, {} joins").format(total_frags, total_joins))

        # Extract plain shelfmark for comparison (without library prefix)
        plain_shelfmark = self.shelfmark
        if plain_shelfmark and ' | ' in plain_shelfmark:
            plain_shelfmark = plain_shelfmark.split(' | ')[-1]

        # Build a map of shelfmark -> document_id from fragment_details
        shelfmark_to_docid = {}
        for fd in data.fragment_details:
            shelf = fd.shelfmark
            doc_id = fd.document_id
            if shelf and doc_id:
                shelfmark_to_docid[shelf.upper()] = doc_id

        # Build fallback map from csv_bank for shelfmarks without document_id
        shelf_to_sys = {}
        if self.meta_mgr and hasattr(self.meta_mgr, 'csv_bank'):
            for sys_id, meta in self.meta_mgr.csv_bank.items():
                shelf = meta.get('shelfmark', '')
                if shelf:
                    shelf_to_sys[self._normalize_shelfmark(shelf)] = sys_id

        # Populate fragments list
        for frag in data.fragments:
            # Try to get title using document_id
            title = ""
            doc_id = shelfmark_to_docid.get(frag.upper())

            # Fallback: look up sys_id from csv_bank using normalized shelfmark
            if not doc_id and self.meta_mgr:
                norm_frag = self._normalize_shelfmark(frag)
                doc_id = shelf_to_sys.get(norm_frag)

            if doc_id and self.meta_mgr:
                try:
                    _, title = self.meta_mgr.get_meta_for_id(doc_id)
                    if title and len(title) > 35:
                        title = title[:35] + "..."
                except:
                    pass

            display_text = f"{frag} - {title}" if title else frag
            item = QListWidgetItem(display_text)
            item.setData(Qt.ItemDataRole.UserRole, frag)  # Store plain shelfmark for navigation

            # Compare with plain shelfmark (joins store plain shelfmarks)
            if plain_shelfmark and frag.upper() == plain_shelfmark.upper():
                item.setForeground(QColor('#27ae60'))
                item.setText(f"{display_text} ({tr('current')})")
            self.fragments_list.addItem(item)

        # Populate joins table
        for join in data.joins:
            row = self.table.rowCount()
            self.table.insertRow(row)

            frag_a = join.fragment_a
            frag_b = join.fragment_b

            self.table.setItem(row, 0, QTableWidgetItem(frag_a))
            self.table.setItem(row, 1, QTableWidgetItem(frag_b))

            rel_display = {
                'physical_join': tr('Physical join'),
                'same_composition': tr('Same composition')
            }.get(join.relationship_type, join.relationship_type or tr('Unknown'))
            self.table.setItem(row, 2, QTableWidgetItem(rel_display))

            self.table.setItem(row, 3, QTableWidgetItem(join.source or 'user'))
            self.table.setItem(row, 4, QTableWidgetItem(join.created_by_username or ""))

            date_str = safe_date_str(join.created_at)
            self.table.setItem(row, 5, QTableWidgetItem(date_str))

            self.table.item(row, 0).setData(Qt.ItemDataRole.UserRole, join.id)

            # Highlight direct joins (joins that involve the current shelfmark)
            is_direct = (frag_a.upper() == plain_shelfmark.upper() or
                         frag_b.upper() == plain_shelfmark.upper()) if plain_shelfmark else False
            if is_direct:
                # Use palette-aware color for dark mode support
                palette = self.palette()
                is_dark = palette.color(QPalette.ColorRole.Window).lightness() < 128
                highlight_color = QColor('#1b4332') if is_dark else QColor('#e8f5e9')
                for col in range(6):
                    item = self.table.item(row, col)
                    if item:
                        item.setBackground(highlight_color)

        # Merge PGP joins into display
        existing_frags_upper = set(f.upper() for f in data.fragments)
        pgp_count = self._merge_pgp_joins_into_display(existing_frags_upper)

        # Merge FJMS scholarly joins into display
        fjms_count = self._merge_fjms_joins_into_display(existing_frags_upper)

        extra_count = pgp_count + fjms_count
        if extra_count > 0:
            new_total_joins = total_joins + extra_count
            new_total_frags = self.fragments_list.count()
            self.cluster_info.setText(
                tr("{} fragments in cluster, {} joins").format(new_total_frags, new_total_joins)
            )

        self.table.resizeColumnsToContents()

    def create_new_join(self):
        """Create a new join between fragments"""
        frag_a = self.frag_a_input.text().strip()
        frag_b = self.frag_b_input.text().strip()

        if not frag_a or not frag_b:
            QMessageBox.warning(self, tr("Error"), tr("Please enter both fragment shelfmarks"))
            return

        if frag_a.upper() == frag_b.upper():
            QMessageBox.warning(self, tr("Error"), tr("Cannot join a fragment to itself"))
            return

        rel_type = self.type_combo.currentData() or None
        notes = self.notes_input.text().strip() or None

        # Always use document_id for fragment A - that's the primary identifier
        doc_id_a = self.document_id

        # Get document_id for fragment B
        doc_id_b = getattr(self, '_selected_doc_id_b', None)

        # If not picked from list, try to resolve from shelfmark using csv_bank
        if not doc_id_b and self.meta_mgr and hasattr(self.meta_mgr, 'csv_bank'):
            # Build lookup map if not already done
            norm_frag_b = self._normalize_shelfmark(frag_b)
            norm_frag_b_no_dots = norm_frag_b.replace('.', '')
            for sys_id, meta in self.meta_mgr.csv_bank.items():
                shelf = meta.get('shelfmark', '')
                if shelf:
                    norm_shelf = self._normalize_shelfmark(shelf)
                    # Exact match or dot-agnostic match
                    if norm_shelf == norm_frag_b or norm_shelf.replace('.', '') == norm_frag_b_no_dots:
                        doc_id_b = sys_id
                        break

        # Try to create via API first
        join, msg = self.client.create_join(frag_a, frag_b, rel_type, notes, document_id_a=doc_id_a, document_id_b=doc_id_b)
        if join:
            QMessageBox.information(self, tr("Success"), tr("Join created successfully"))
            self.frag_b_input.clear()
            self.notes_input.clear()
            # Immediately add to local cache (faster than full sync)
            if self.joins_mgr and join:
                join_data = {
                    'id': join.id,
                    'fragment_a': join.fragment_a,
                    'fragment_b': join.fragment_b,
                    'document_id_a': getattr(join, 'document_id_a', None) or doc_id_a,
                    'document_id_b': getattr(join, 'document_id_b', None) or doc_id_b,
                    'relationship_type': join.relationship_type,
                    'notes': join.notes,
                    'source': join.source,
                    'created_by_username': getattr(join, 'created_by_username', None),
                    'created_at': getattr(join, 'created_at', None)
                }
                self.joins_mgr.data['joins'][join.id] = join_data
                self.joins_mgr._index_join(join_data)
                self.joins_mgr.save()
            # Reload to show the new join
            self.load_joins()
        else:
            # If online create failed but we have JoinsManager, queue for later
            if self.joins_mgr and "connection" in msg.lower():
                self.joins_mgr.create_join_local(frag_a, frag_b, rel_type, notes, document_id_a=doc_id_a, document_id_b=doc_id_b)
                QMessageBox.information(
                    self, tr("Saved Offline"),
                    tr("Join saved locally. Will sync when connection is restored.")
                )
                self.frag_b_input.clear()
                self.notes_input.clear()
                self.load_joins()
            else:
                QMessageBox.critical(self, tr("Error"), tr("Failed to create join: {}").format(msg))

    def delete_selected_join(self):
        """Delete the selected join"""
        selected = self.table.selectedItems()
        if not selected:
            return

        row = selected[0].row()
        join_id = self.table.item(row, 0).data(Qt.ItemDataRole.UserRole)

        if not join_id:
            return

        reply = QMessageBox.question(
            self, tr("Confirm Delete"),
            tr("Are you sure you want to delete this join?"),
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )

        if reply == QMessageBox.StandardButton.Yes:
            success, msg = self.client.delete_join(join_id)
            if success:
                QMessageBox.information(self, tr("Success"), tr("Join deleted"))
                # Immediately remove from local cache
                if self.joins_mgr:
                    self.joins_mgr.delete_join_local(join_id)
                # Reload to show updated state
                self.load_joins()
            else:
                # If online delete failed, try local delete
                if self.joins_mgr:
                    self.joins_mgr.delete_join_local(join_id)
                    QMessageBox.information(
                        self, tr("Deleted Locally"),
                        tr("Join deleted locally. Will sync when connection is restored.")
                    )
                    self.load_joins()
                else:
                    QMessageBox.critical(self, tr("Error"), tr("Failed to delete: {}").format(msg))

    def on_fragment_selection_changed(self):
        """Enable/disable navigate button based on selection"""
        selected = self.fragments_list.selectedItems()
        self.btn_navigate.setEnabled(bool(selected))

    def on_table_selection_changed(self):
        """Enable/disable delete button based on selection - admin only, direct joins only"""
        selected = self.table.selectedItems()
        can_delete = False

        # Only allow delete if logged in as admin
        is_admin = (self.client.is_logged_in() and
                    self.client.current_user and
                    self.client.current_user.role == 'admin')

        if is_admin and selected:
            # Get selected row
            row = selected[0].row()
            join_id = self.table.item(row, 0).data(Qt.ItemDataRole.UserRole) if self.table.item(row, 0) else None

            # PGP joins have None as join ID - cannot be deleted
            if join_id is None:
                can_delete = False
            else:
                frag_a = self.table.item(row, 0).text() if self.table.item(row, 0) else ''
                frag_b = self.table.item(row, 1).text() if self.table.item(row, 1) else ''

                # Extract plain shelfmark for comparison
                plain_shelfmark = self.shelfmark
                if plain_shelfmark and ' | ' in plain_shelfmark:
                    plain_shelfmark = plain_shelfmark.split(' | ')[-1]

                # Only allow delete if this join DIRECTLY involves the current shelfmark
                if plain_shelfmark:
                    is_direct = (frag_a.upper() == plain_shelfmark.upper() or
                                 frag_b.upper() == plain_shelfmark.upper())
                    can_delete = is_direct

        self.btn_delete.setEnabled(can_delete)

    def _show_list_picker(self):
        """Show dialog to pick a fragment from personal lists."""
        if not self.lists_mgr:
            return

        dialog = QDialog(self)
        dialog.setWindowTitle(tr("Pick from List"))
        dialog.resize(400, 300)

        layout = QVBoxLayout(dialog)

        # List selection
        list_layout = QHBoxLayout()
        list_layout.addWidget(QLabel(tr("List:")))
        list_combo = QComboBox()
        list_combo.addItem(tr("-- Select list --"), None)
        lists = self.lists_mgr.get_all_lists(include_recent=True)
        for lst in lists:
            display_name = lst.get('name', tr("Unnamed"))
            if lst.get('is_recent'):
                display_name = tr("Recent")
            list_combo.addItem(display_name, lst.get('id'))
        list_layout.addWidget(list_combo, 1)
        layout.addLayout(list_layout)

        # Items list
        items_list = QListWidget()
        layout.addWidget(items_list, 1)

        def load_list_items():
            items_list.clear()
            list_id = list_combo.currentData()
            if not list_id:
                return

            items = self.lists_mgr.get_items_in_list(list_id)
            for item in items:
                sys_id = item.get('sys_id', '')
                shelfmark = sys_id  # Default to sys_id
                title = ""

                # Try to get shelfmark and title from meta_mgr
                if self.meta_mgr and sys_id:
                    try:
                        shelf, _ = self.meta_mgr.get_meta_for_id(sys_id)
                        if shelf:
                            shelfmark = shelf
                        # Try to get title
                        cached = self.meta_mgr.nli_cache.get(sys_id, {})
                        title = cached.get('title', '')
                        if title and len(title) > 40:
                            title = title[:40] + "..."
                    except:
                        pass

                display = shelfmark
                if title:
                    display = f"{shelfmark} - {title}"

                list_item = QListWidgetItem(display)
                list_item.setData(Qt.ItemDataRole.UserRole, {'sys_id': sys_id, 'shelfmark': shelfmark})
                items_list.addItem(list_item)

        list_combo.currentIndexChanged.connect(load_list_items)

        # Buttons
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        btn_cancel = QPushButton(tr("Cancel"))
        btn_cancel.clicked.connect(dialog.reject)
        btn_layout.addWidget(btn_cancel)
        btn_select = QPushButton(tr("Select"))
        btn_select.setEnabled(False)
        btn_layout.addWidget(btn_select)
        layout.addLayout(btn_layout)

        def on_item_selected():
            btn_select.setEnabled(bool(items_list.selectedItems()))

        items_list.itemSelectionChanged.connect(on_item_selected)
        items_list.itemDoubleClicked.connect(lambda: btn_select.click())

        def do_select():
            selected = items_list.selectedItems()
            if selected:
                data = selected[0].data(Qt.ItemDataRole.UserRole)
                # Store document_id for use when creating join
                self._selected_doc_id_b = data.get('sys_id')
                self.frag_b_input.setText(data.get('shelfmark', ''))
                dialog.accept()

        btn_select.clicked.connect(do_select)

        dialog.exec()

    def on_fragment_double_click(self, item):
        """Navigate to fragment on double-click"""
        # Get shelfmark from UserRole if available, otherwise parse from text
        shelfmark = item.data(Qt.ItemDataRole.UserRole)
        if not shelfmark:
            shelfmark = item.text()
        self.navigate_to_fragment(shelfmark)

    def navigate_to_selected(self):
        """Navigate to the selected fragment"""
        selected = self.fragments_list.selectedItems()
        if selected:
            # Get shelfmark from UserRole if available, otherwise parse from text
            shelfmark = selected[0].data(Qt.ItemDataRole.UserRole)
            if not shelfmark:
                shelfmark = selected[0].text()
            self.navigate_to_fragment(shelfmark)

    def navigate_to_fragment(self, fragment_text):
        """Navigate to a fragment"""
        # Remove " (current)" suffix if present - handle various formats
        shelfmark = fragment_text
        current_suffix = f" ({tr('current')})"
        if shelfmark.endswith(current_suffix):
            shelfmark = shelfmark[:-len(current_suffix)]
        shelfmark = shelfmark.strip()

        if self.on_browse and shelfmark:
            # Store callback and shelfmark, close dialog, then call callback
            # Using QTimer to ensure dialog is fully closed before callback
            callback = self.on_browse
            self.accept()  # Close dialog
            try:
                QTimer.singleShot(50, lambda: self._safe_navigate(callback, shelfmark))
            except Exception as e:
                print(f"[ERROR] Navigation failed: {e}", flush=True)
        else:
            QMessageBox.information(
                self, tr("Navigate"),
                tr("To view {}, search for it in the Browse tab").format(shelfmark)
            )

    def _safe_navigate(self, callback, shelfmark):
        """Safely call navigation callback with error handling."""
        try:
            callback(shelfmark)
        except Exception as e:
            print(f"[ERROR] Navigation callback failed: {e}", flush=True)
            import traceback
            traceback.print_exc()


# =============================================================================
# Joins Feed Dialog - Browse all joins or user's joins
# =============================================================================

class JoinsFeedDialog(QDialog):
    """
    Dialog for browsing fragment joins with two tabs:
    - My Joins: User's own created joins
    - All Joins: All user-created joins from community
    """

    def __init__(self, parent=None, client: CorrectionsClient = None, on_browse=None):
        super().__init__(parent)
        self.client = client or get_corrections_client()
        self.on_browse = on_browse  # Callback to navigate to a shelfmark
        self.setWindowTitle(tr("Fragment Joins"))
        self.resize(1000, 700)
        self.current_page = 1
        self.total_pages = 1
        self.page_size = 30
        self.init_ui()
        # Load data in background to avoid freezing
        QTimer.singleShot(100, self.load_joins_safe)

    def init_ui(self):
        layout = QVBoxLayout(self)

        # Header
        header = QLabel(tr("Fragment Joins"))
        header.setStyleSheet("font-weight: bold; font-size: 16px;")
        layout.addWidget(header)

        # Tabs for My Joins / All Joins
        self.tabs = QTabWidget()

        # My Joins tab
        self.my_joins_widget = self._create_joins_tab(is_my_joins=True)
        self.tabs.addTab(self.my_joins_widget, tr("My Joins"))

        # All Joins tab
        self.all_joins_widget = self._create_joins_tab(is_my_joins=False)
        self.tabs.addTab(self.all_joins_widget, tr("All Joins"))

        self.tabs.currentChanged.connect(self.on_tab_changed)
        layout.addWidget(self.tabs)

        # Bottom buttons
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        self.btn_close = QPushButton(tr("Close"))
        self.btn_close.clicked.connect(self.accept)
        btn_layout.addWidget(self.btn_close)
        layout.addLayout(btn_layout)

    def _create_joins_tab(self, is_my_joins: bool):
        """Create a tab widget for joins"""
        widget = QWidget()
        layout = QVBoxLayout(widget)

        # Filter row
        filter_layout = QHBoxLayout()

        filter_layout.addWidget(QLabel(tr("Search:")))
        search_input = QLineEdit()
        search_input.setPlaceholderText(tr("Search shelfmarks..."))
        search_input.setObjectName("my_search" if is_my_joins else "all_search")
        filter_layout.addWidget(search_input, 1)

        filter_layout.addWidget(QLabel(tr("Type:")))
        type_combo = QComboBox()
        type_combo.addItems([tr("All"), tr("Physical join"), tr("Same composition")])
        type_combo.setObjectName("my_type" if is_my_joins else "all_type")
        filter_layout.addWidget(type_combo)

        btn_search = QPushButton(tr("Search"))
        btn_search.setObjectName("my_btn_search" if is_my_joins else "all_btn_search")
        filter_layout.addWidget(btn_search)

        btn_refresh = QPushButton(tr("Refresh"))
        btn_refresh.setObjectName("my_btn_refresh" if is_my_joins else "all_btn_refresh")
        filter_layout.addWidget(btn_refresh)

        layout.addLayout(filter_layout)

        # Status label (for loading/error states)
        status_label = QLabel("")
        status_label.setObjectName("my_status" if is_my_joins else "all_status")
        status_label.setStyleSheet("color: #666;")
        layout.addWidget(status_label)

        # Table
        table = QTableWidget()
        table.setObjectName("my_table" if is_my_joins else "all_table")
        table.setColumnCount(6)
        table.setHorizontalHeaderLabels([
            tr("Fragment A"), tr("Fragment B"), tr("Type"),
            tr("Author"), tr("Date"), tr("Notes")
        ])
        table.horizontalHeader().setStretchLastSection(True)
        table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        table.customContextMenuRequested.connect(
            lambda pos: self._show_context_menu(pos, table, is_my_joins)
        )
        table.doubleClicked.connect(lambda: self._on_double_click(table))
        layout.addWidget(table)

        # Pagination
        page_layout = QHBoxLayout()
        btn_prev = QPushButton(tr("Previous"))
        btn_prev.setObjectName("my_prev" if is_my_joins else "all_prev")
        page_layout.addWidget(btn_prev)

        page_label = QLabel("1 / 1")
        page_label.setObjectName("my_page" if is_my_joins else "all_page")
        page_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        page_layout.addWidget(page_label)

        btn_next = QPushButton(tr("Next"))
        btn_next.setObjectName("my_next" if is_my_joins else "all_next")
        page_layout.addWidget(btn_next)

        page_layout.addStretch()

        total_label = QLabel("")
        total_label.setObjectName("my_total" if is_my_joins else "all_total")
        page_layout.addWidget(total_label)

        layout.addLayout(page_layout)

        # Connect signals
        search_input.returnPressed.connect(lambda: self.load_joins(is_my_joins))
        btn_search.clicked.connect(lambda: self.load_joins(is_my_joins))
        btn_refresh.clicked.connect(lambda: self.load_joins(is_my_joins))
        type_combo.currentIndexChanged.connect(lambda: self.load_joins(is_my_joins))
        btn_prev.clicked.connect(lambda: self._prev_page(is_my_joins))
        btn_next.clicked.connect(lambda: self._next_page(is_my_joins))

        return widget

    def _get_widget(self, name: str, is_my_joins: bool):
        """Get a widget by name from the appropriate tab"""
        parent = self.my_joins_widget if is_my_joins else self.all_joins_widget
        return parent.findChild(QWidget, name)

    def load_joins_safe(self):
        """Load joins with error handling to prevent freezing"""
        # Check server availability first with short timeout
        try:
            is_my_tab = self.tabs.currentIndex() == 0
            self.load_joins(is_my_tab)
        except Exception as e:
            print(f"[ERROR] Failed to load joins: {e}", flush=True)
            # Show error in status
            status = self._get_widget("my_status" if self.tabs.currentIndex() == 0 else "all_status",
                                      self.tabs.currentIndex() == 0)
            if status:
                status.setText(tr("Failed to load joins. Server may be unavailable."))

    def on_tab_changed(self, index):
        """Load data when tab changes"""
        is_my_joins = (index == 0)
        # Load in background
        QTimer.singleShot(50, lambda: self.load_joins(is_my_joins))

    def load_joins(self, is_my_joins: bool):
        """Load joins from server"""
        prefix = "my_" if is_my_joins else "all_"
        parent = self.my_joins_widget if is_my_joins else self.all_joins_widget

        table = parent.findChild(QTableWidget, f"{prefix}table")
        search_input = parent.findChild(QLineEdit, f"{prefix}search")
        type_combo = parent.findChild(QComboBox, f"{prefix}type")
        page_label = parent.findChild(QLabel, f"{prefix}page")
        total_label = parent.findChild(QLabel, f"{prefix}total")
        status_label = parent.findChild(QLabel, f"{prefix}status")
        btn_prev = parent.findChild(QPushButton, f"{prefix}prev")
        btn_next = parent.findChild(QPushButton, f"{prefix}next")

        if not table:
            return

        table.setRowCount(0)
        if status_label:
            status_label.setText(tr("Loading..."))

        # Get filter values
        query = search_input.text().strip() if search_input else None
        type_idx = type_combo.currentIndex() if type_combo else 0
        type_values = [None, "physical_join", "same_composition"]
        rel_type = type_values[type_idx] if type_idx < len(type_values) else None

        # Check server availability with short timeout
        if not self.client.is_server_available():
            if status_label:
                status_label.setText(tr("Server unavailable - showing cached data"))
            # Could show cached data here if available
            return

        try:
            offset = (self.current_page - 1) * self.page_size

            if is_my_joins:
                if not self.client.is_logged_in():
                    if status_label:
                        status_label.setText(tr("Login required to view your joins"))
                    return
                joins, total = self.client.get_my_joins(
                    query=query,
                    relationship_type=rel_type,
                    limit=self.page_size,
                    offset=offset
                )
            else:
                joins, total = self.client.search_joins(
                    query=query,
                    source='user',  # Only show user-created joins
                    relationship_type=rel_type,
                    limit=self.page_size,
                    offset=offset
                )

            if status_label:
                status_label.setText("")

            self.total_pages = max(1, (total + self.page_size - 1) // self.page_size)
            if page_label:
                page_label.setText(f"{self.current_page} / {self.total_pages}")
            if total_label:
                total_label.setText(tr("{} joins total").format(total))
            if btn_prev:
                btn_prev.setEnabled(self.current_page > 1)
            if btn_next:
                btn_next.setEnabled(self.current_page < self.total_pages)

            # Populate table
            for join in joins:
                row = table.rowCount()
                table.insertRow(row)

                # Fragment A
                item_a = QTableWidgetItem(join.fragment_a or "")
                item_a.setData(Qt.ItemDataRole.UserRole, {
                    'join_id': join.id,
                    'fragment_a': join.fragment_a,
                    'fragment_b': join.fragment_b,
                    'document_id_a': join.document_id_a,
                    'document_id_b': join.document_id_b
                })
                table.setItem(row, 0, item_a)

                # Fragment B
                table.setItem(row, 1, QTableWidgetItem(join.fragment_b or ""))

                # Relationship type
                rel_labels = {
                    'physical_join': tr('Physical join'),
                    'same_composition': tr('Same composition')
                }
                rel_display = rel_labels.get(join.relationship_type, join.relationship_type or "")
                table.setItem(row, 2, QTableWidgetItem(rel_display))

                # Author
                table.setItem(row, 3, QTableWidgetItem(join.created_by_username or ""))

                # Date
                date_str = safe_date_str(join.created_at)
                table.setItem(row, 4, QTableWidgetItem(date_str))

                # Notes
                notes = join.notes or ""
                if len(notes) > 50:
                    notes = notes[:50] + "..."
                table.setItem(row, 5, QTableWidgetItem(notes))

            table.resizeColumnsToContents()

        except Exception as e:
            print(f"[ERROR] Failed to load joins: {e}", flush=True)
            if status_label:
                status_label.setText(tr("Failed to load joins"))

    def _prev_page(self, is_my_joins: bool):
        """Go to previous page"""
        if self.current_page > 1:
            self.current_page -= 1
            self.load_joins(is_my_joins)

    def _next_page(self, is_my_joins: bool):
        """Go to next page"""
        if self.current_page < self.total_pages:
            self.current_page += 1
            self.load_joins(is_my_joins)

    def _show_context_menu(self, pos, table: QTableWidget, is_my_joins: bool):
        """Show context menu for join row"""
        item = table.itemAt(pos)
        if not item:
            return

        row = item.row()
        data = table.item(row, 0).data(Qt.ItemDataRole.UserRole)
        if not data:
            return

        menu = QMenu(self)

        # Navigate to Fragment A
        if data.get('document_id_a') or data.get('fragment_a'):
            action_nav_a = menu.addAction(tr("Open Fragment A: {}").format(data.get('fragment_a', '')))
            action_nav_a.triggered.connect(
                lambda: self._navigate_to_fragment(data.get('document_id_a'), data.get('fragment_a'))
            )

        # Navigate to Fragment B
        if data.get('document_id_b') or data.get('fragment_b'):
            action_nav_b = menu.addAction(tr("Open Fragment B: {}").format(data.get('fragment_b', '')))
            action_nav_b.triggered.connect(
                lambda: self._navigate_to_fragment(data.get('document_id_b'), data.get('fragment_b'))
            )

        menu.addSeparator()

        # Copy shelfmarks
        action_copy = menu.addAction(tr("Copy shelfmarks"))
        action_copy.triggered.connect(
            lambda: self._copy_shelfmarks(data.get('fragment_a'), data.get('fragment_b'))
        )

        # Delete (for own joins or admin)
        is_admin = (self.client.is_logged_in() and
                    self.client.current_user and
                    self.client.current_user.role == 'admin')

        if is_my_joins or is_admin:
            menu.addSeparator()
            action_delete = menu.addAction(tr("Delete join"))
            action_delete.triggered.connect(
                lambda: self._delete_join(data.get('join_id'), is_my_joins)
            )

        menu.exec(table.viewport().mapToGlobal(pos))

    def _on_double_click(self, table: QTableWidget):
        """Handle double click on a row - navigate to fragment A"""
        selected = table.selectedItems()
        if not selected:
            return

        row = selected[0].row()
        data = table.item(row, 0).data(Qt.ItemDataRole.UserRole)
        if data:
            self._navigate_to_fragment(data.get('document_id_a'), data.get('fragment_a'))

    def _navigate_to_fragment(self, doc_id: str, shelfmark: str):
        """Navigate to a fragment"""
        if self.on_browse:
            self.accept()  # Close dialog first
            # Use shelfmark for navigation
            if shelfmark:
                QTimer.singleShot(50, lambda: self.on_browse(shelfmark))
        elif doc_id:
            QMessageBox.information(
                self, tr("Navigate"),
                tr("Document ID: {}").format(doc_id)
            )

    def _copy_shelfmarks(self, frag_a: str, frag_b: str):
        """Copy shelfmarks to clipboard"""
        from PyQt6.QtWidgets import QApplication
        clipboard = QApplication.clipboard()
        clipboard.setText(f"{frag_a} ↔ {frag_b}")

    def _delete_join(self, join_id: int, is_my_joins: bool):
        """Delete a join"""
        if not join_id:
            return

        reply = QMessageBox.question(
            self, tr("Confirm Delete"),
            tr("Are you sure you want to delete this join?"),
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )

        if reply == QMessageBox.StandardButton.Yes:
            success, msg = self.client.delete_join(join_id)
            if success:
                QMessageBox.information(self, tr("Success"), tr("Join deleted"))
                self.load_joins(is_my_joins)
            else:
                QMessageBox.critical(self, tr("Error"), tr("Failed to delete: {}").format(msg))
