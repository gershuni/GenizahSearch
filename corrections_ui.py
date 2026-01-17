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
    QSplitter, QMenu, QStatusBar
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer
from PyQt6.QtGui import QFont, QColor, QAction

try:
    from genizah_core import tr, CURRENT_LANG
except ImportError:
    def tr(text): return text
    CURRENT_LANG = 'en'

from corrections_client import (
    CorrectionsClient, get_corrections_client,
    User, Correction, Comment, Discovery, DiscoveryResponse, FeedItem
)

logger = logging.getLogger(__name__)


class LoginDialog(QDialog):
    """Dialog for user login"""
    login_success = pyqtSignal(object)  # Emits User object

    def __init__(self, parent=None, client: CorrectionsClient = None):
        super().__init__(parent)
        self.client = client or get_corrections_client()
        self.setWindowTitle(tr("Login to Corrections System"))
        self.resize(400, 250)
        self.init_ui()

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
            self.login_success.emit(self.client.current_user)
            self.accept()
        else:
            QMessageBox.warning(self, tr("Login Failed"), message)
            self.btn_login.setEnabled(True)
            self.btn_login.setText(tr("Login"))

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
        line_number: int = None,
        context_before: str = None,
        context_after: str = None
    ):
        super().__init__(parent)
        self.client = client or get_corrections_client()
        self.document_id = document_id
        self.original_text = original_text or ""
        self.shelfmark = shelfmark
        self.system_id = system_id
        self.line_number = line_number
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

        if self.line_number:
            options.addWidget(QLabel(f"{tr('Line')}: {self.line_number}"))

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
            line_number=self.line_number,
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
        shelfmark: str = None
    ):
        super().__init__(parent)
        self.client = client or get_corrections_client()
        self.document_id = document_id
        self.shelfmark = shelfmark

        self.setWindowTitle(tr("Document Corrections"))
        self.resize(800, 600)
        self.init_ui()
        self.load_corrections()

    def init_ui(self):
        layout = QVBoxLayout(self)

        # Header
        header_text = tr("Corrections for {}").format(self.shelfmark or self.document_id)
        header = QLabel(header_text)
        header.setStyleSheet("font-weight: bold; font-size: 14px;")
        layout.addWidget(header)

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

            date_str = correction.created_at[:10] if correction.created_at else ""
            self.table.setItem(row, 5, QTableWidgetItem(date_str))

            # Store correction ID
            self.table.item(row, 0).setData(Qt.ItemDataRole.UserRole, correction.id)

        self.table.resizeColumnsToContents()

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
        correction: Correction = None
    ):
        super().__init__(parent)
        self.client = client or get_corrections_client()
        self.correction = correction

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

        diff_layout.addWidget(QLabel(tr("Original:")))
        orig = QTextEdit()
        orig.setPlainText(c.original_text)
        orig.setReadOnly(True)
        orig.setMaximumHeight(60)
        orig.setStyleSheet("background-color: #ffebee;")
        diff_layout.addWidget(orig)

        diff_layout.addWidget(QLabel(tr("Corrected:")))
        corr = QTextEdit()
        corr.setPlainText(c.corrected_text)
        corr.setReadOnly(True)
        corr.setMaximumHeight(60)
        corr.setStyleSheet("background-color: #e8f5e9;")
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
            except:
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
            date_str = correction.created_at[:10] if correction.created_at else ""
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
            date_str = correction.created_at[:10] if correction.created_at else ""
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

    def __init__(self, parent=None, client: CorrectionsClient = None):
        super().__init__(parent)
        self.client = client or get_corrections_client()
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
        self.stats_frame.setStyleSheet("background: #f5f5f5; border-radius: 5px;")
        stats_layout = QHBoxLayout(self.stats_frame)

        self.stat_labels = {}
        for stat_name in ['words_corrected', 'documents_edited', 'total_discoveries', 'open_questions', 'active_contributors']:
            stat_widget = QFrame()
            stat_widget.setStyleSheet("background: white; border-radius: 3px; padding: 5px;")
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
                'active_contributors': tr('Contributors')
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
        self.type_combo.addItems([tr("All"), tr("Discoveries"), tr("Questions"), tr("Corrections"), tr("Comments")])
        self.type_values = ['all', 'discovery', 'question', 'correction', 'comment']
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
                background: white;
                border: 1px solid #ddd;
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
            'comment': '#1abc9c'
        }
        type_labels = {
            'discovery': tr('Discovery'),
            'question': tr('Question'),
            'correction': tr('Correction'),
            'comment': tr('Comment')
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

        header_layout.addStretch()

        # Date
        if item.created_at:
            date_label = QLabel(item.created_at[:10])
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
            content_label.setStyleSheet("color: #555; font-size: 11px;")
            content_label.setWordWrap(True)
            layout.addWidget(content_label)

        # For corrections: show diff
        if item.item_type == 'correction' and item.original_text and item.corrected_text:
            diff_frame = QFrame()
            diff_frame.setStyleSheet("background: #f9f9f9; padding: 5px; border-radius: 3px;")
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

        if d.shelfmark:
            meta_layout.addWidget(QLabel(f"{tr('Shelfmark')}: {d.shelfmark}"))

        if d.created_at:
            meta_layout.addWidget(QLabel(f"{tr('Date')}: {d.created_at[:10]}"))

        meta_layout.addStretch()
        layout.addLayout(meta_layout)

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
                date_label = QLabel(resp.created_at[:10])
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


class CreateDiscoveryDialog(QDialog):
    """Dialog for creating a new discovery"""

    def __init__(self, parent=None, client: CorrectionsClient = None):
        super().__init__(parent)
        self.client = client or get_corrections_client()
        self.setWindowTitle(tr("Share a Discovery"))
        self.resize(600, 500)
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

        # Document reference
        doc_layout = QHBoxLayout()
        doc_layout.addWidget(QLabel(tr("Document (optional):")))
        self.shelfmark_input = QLineEdit()
        self.shelfmark_input.setPlaceholderText(tr("Shelfmark, e.g., T-S 13J1.1"))
        doc_layout.addWidget(self.shelfmark_input)

        doc_layout.addWidget(QLabel(tr("Page:")))
        self.page_input = QSpinBox()
        self.page_input.setMinimum(0)
        self.page_input.setSpecialValueText("-")
        doc_layout.addWidget(self.page_input)

        layout.addLayout(doc_layout)

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
        shelfmark = self.shelfmark_input.text().strip() or None
        page_number = self.page_input.value() if self.page_input.value() > 0 else None

        discovery, error = self.client.create_discovery(
            title=title,
            content=content,
            discovery_type=discovery_type,
            shelfmark=shelfmark,
            page_number=page_number,
            is_anonymous=self.anonymous_check.isChecked()
        )

        if discovery:
            QMessageBox.information(self, tr("Success"), tr("Discovery shared successfully!"))
            self.accept()
        else:
            QMessageBox.warning(self, tr("Error"), error)


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
