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
    User, Correction, Comment
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
