# -*- coding: utf-8 -*-
"""Update-notification and progress UI classes (extracted from genizah_app.py, Phase 127).

Provides four PyQt6 widget / dialog subclasses moved verbatim out of the
28K-line ``genizah_app.py`` god file:

  - UpdateNotificationBar(QFrame)   — narrow top-of-screen update banner
  - WhatsNewBar(QFrame)             — "what's new" feature announcement bar
  - WhatsNewDialog(QDialog)         — detailed "What's New" scrollable dialog
  - UpdateProgressDialog(QDialog)   — download progress + installer launch dialog

ZERO behavior change vs. the originals. ``genizah_app.py`` re-exports these
via a shim (MOVE-and-shim, mirroring genizah_core 122-125). The shim carries
NO ``# noqa: F401`` because all four classes are instantiated by GenizahGUI.

GUARD-01: NO module-level ``import genizah_app`` — symbols come from
``genizah_core`` (tr / CURRENT_LANG), ``gui_threads`` (lazy), PyQt6.
"""
from __future__ import annotations

from PyQt6.QtWidgets import (
    QFrame,
    QDialog,
    QHBoxLayout,
    QVBoxLayout,
    QLabel,
    QPushButton,
    QProgressBar,
    QScrollArea,
    QMessageBox,
)
from PyQt6.QtCore import Qt, QTimer, pyqtSignal, QUrl
from PyQt6.QtGui import QDesktopServices

from genizah_core import tr, CURRENT_LANG


class UpdateNotificationBar(QFrame):
    """A narrow notification bar at the top of the screen."""

    dismissed = pyqtSignal(str) # Emits version string on dismiss
    update_requested = pyqtSignal(str, str, str)  # version, html_url, installer_url

    def __init__(self, parent=None):
        super().__init__(parent)
        self.html_url = ""
        self.installer_url = ""
        self.version_tag = ""

        self.setFrameShape(QFrame.Shape.StyledPanel)
        self.setStyleSheet("background-color: #d1ecf1; color: #0c5460; border-bottom: 1px solid #bee5eb;")
        self.setFixedHeight(40)

        layout = QHBoxLayout(self)
        layout.setContentsMargins(10, 0, 10, 0)

        self.lbl_msg = QLabel()
        self.lbl_msg.setStyleSheet("font-weight: bold; font-size: 13px; border: none; background: transparent;")

        self.btn_download = QPushButton(tr("Update Now"))
        self.btn_download.setStyleSheet("background-color: #17a2b8; color: white; font-weight: bold; border-radius: 4px; padding: 4px 8px;")
        self.btn_download.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_download.clicked.connect(self.on_download)

        self.btn_dismiss = QPushButton("✕")
        self.btn_dismiss.setToolTip(tr("Dismiss until next version"))
        self.btn_dismiss.setStyleSheet("""
            QPushButton { background: transparent; color: #0c5460; font-weight: bold; border: none; font-size: 16px; }
            QPushButton:hover { color: #dc3545; }
        """)
        self.btn_dismiss.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_dismiss.clicked.connect(self.on_dismiss)

        layout.addWidget(self.lbl_msg)
        layout.addStretch()
        layout.addWidget(self.btn_download)
        layout.addSpacing(10)
        layout.addWidget(self.btn_dismiss)

        self.hide()

    def show_update(self, version: str, html_url: str, installer_url: str = ""):
        self.version_tag = version
        self.html_url = html_url
        self.installer_url = installer_url
        self.lbl_msg.setText(tr("New version available: {}").format(version))
        self.show()

    def on_download(self):
        self.update_requested.emit(self.version_tag, self.html_url, self.installer_url)

    def on_dismiss(self):
        self.hide()
        self.dismissed.emit(self.version_tag)


class WhatsNewBar(QFrame):
    """A notification bar showing new features after a version update."""

    dismissed = pyqtSignal()
    learn_more = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFrameShape(QFrame.Shape.StyledPanel)
        self.setStyleSheet("background-color: #d1fae5; color: #065f46; border-bottom: 1px solid #a7f3d0;")
        self.setFixedHeight(40)

        layout = QHBoxLayout(self)
        layout.setContentsMargins(10, 0, 10, 0)

        self.lbl_msg = QLabel()
        self.lbl_msg.setStyleSheet("font-weight: bold; font-size: 13px; border: none; background: transparent;")

        self.btn_learn_more = QPushButton(tr("Learn More"))
        self.btn_learn_more.setStyleSheet("background-color: #10b981; color: white; font-weight: bold; border-radius: 4px; padding: 4px 8px;")
        self.btn_learn_more.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_learn_more.clicked.connect(self.on_learn_more)

        self.btn_dismiss = QPushButton("\u2715")
        self.btn_dismiss.setToolTip(tr("Dismiss"))
        self.btn_dismiss.setStyleSheet("""
            QPushButton { background: transparent; color: #065f46; font-weight: bold; border: none; font-size: 16px; }
            QPushButton:hover { color: #dc3545; }
        """)
        self.btn_dismiss.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_dismiss.clicked.connect(self.on_dismiss)

        layout.addWidget(self.lbl_msg)
        layout.addStretch()
        layout.addWidget(self.btn_learn_more)
        layout.addSpacing(10)
        layout.addWidget(self.btn_dismiss)

        self.hide()

    def show_whats_new(self, version: str):
        self.lbl_msg.setText(tr("Fixed: the Printed filter in Composition Search had no effect — it now filters correctly."))
        self.show()

    def on_learn_more(self):
        self.learn_more.emit()

    def on_dismiss(self):
        self.hide()
        self.dismissed.emit()


class TelemetryConsentBar(QFrame):
    """Non-modal re-invite bar for telemetry consent (SEED-031).

    Mirrors WhatsNewBar (QFrame, 40px, tr() strings, pyqtSignal slots,
    self.hide() in __init__) rather than adding a new blocking modal. Shown at
    most ~3 times, throttled by desktop.telemetry.should_reask_consent — this
    class is purely the surface; the gate + bookkeeping live in telemetry.

    Signals:
      enable_requested    — user clicked "Enable" (the ONLY implicit-opt-in path;
                            the handler calls set_consent(True)).
      learn_more          — user clicked "Learn more" (opens PrivacyDialog).
      never_ask_requested — user clicked "Don't ask again" (hard opt-out).
      dismissed           — user clicked "✕" (ignored this time; ask already counted).

    The copy is an honest invite with no pressure and no default action.
    GUARD-01: NO module-level ``import genizah_app`` — tr()/CURRENT_LANG come
    from genizah_core.
    """

    enable_requested = pyqtSignal()
    learn_more = pyqtSignal()
    never_ask_requested = pyqtSignal()
    dismissed = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFrameShape(QFrame.Shape.StyledPanel)
        self.setStyleSheet("background-color: #e0e7ff; color: #3730a3; border-bottom: 1px solid #c7d2fe;")
        self.setFixedHeight(40)

        layout = QHBoxLayout(self)
        layout.setContentsMargins(10, 0, 10, 0)

        self.lbl_msg = QLabel()
        self.lbl_msg.setStyleSheet("font-weight: bold; font-size: 13px; border: none; background: transparent;")

        self.btn_enable = QPushButton(tr("Enable"))
        self.btn_enable.setStyleSheet("background-color: #10b981; color: white; font-weight: bold; border-radius: 4px; padding: 4px 8px;")
        self.btn_enable.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_enable.clicked.connect(self.on_enable)

        self.btn_learn_more = QPushButton(tr("Learn more"))
        self.btn_learn_more.setFlat(True)
        self.btn_learn_more.setStyleSheet("color: #3730a3; text-decoration: underline; border: none; background: transparent;")
        self.btn_learn_more.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_learn_more.clicked.connect(self.on_learn_more)

        self.btn_never = QPushButton(tr("Don't ask again"))
        self.btn_never.setFlat(True)
        self.btn_never.setStyleSheet("color: #6b7280; border: none; background: transparent;")
        self.btn_never.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_never.clicked.connect(self.on_never_ask)

        self.btn_dismiss = QPushButton("✕")
        self.btn_dismiss.setToolTip(tr("Dismiss"))
        self.btn_dismiss.setStyleSheet("""
            QPushButton { background: transparent; color: #3730a3; font-weight: bold; border: none; font-size: 16px; }
            QPushButton:hover { color: #dc3545; }
        """)
        self.btn_dismiss.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_dismiss.clicked.connect(self.on_dismiss)

        layout.addWidget(self.lbl_msg)
        layout.addStretch()
        layout.addWidget(self.btn_enable)
        layout.addSpacing(6)
        layout.addWidget(self.btn_learn_more)
        layout.addSpacing(6)
        layout.addWidget(self.btn_never)
        layout.addSpacing(10)
        layout.addWidget(self.btn_dismiss)

        self.hide()

    def show_reask(self):
        self.lbl_msg.setText(tr(
            "Help improve Dicta Genizah Search Pro? You can share anonymous "
            "usage data — never your searches or your library."
        ))
        self.show()

    def on_enable(self):
        self.enable_requested.emit()

    def on_learn_more(self):
        self.learn_more.emit()

    def on_never_ask(self):
        self.hide()
        self.never_ask_requested.emit()

    def on_dismiss(self):
        self.hide()
        self.dismissed.emit()


class WhatsNewDialog(QDialog):
    """Dialog showing detailed What's New information."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle(tr("New Features!"))
        self.setModal(True)
        # Resizable + scrollable body (set below) so the four What's New bullets,
        # some of them long, never clip on smaller displays.
        self.setMinimumSize(520, 480)
        self.resize(520, 600)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowType.WindowContextHelpButtonHint)
        if CURRENT_LANG == 'he':
            self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(24, 24, 24, 24)
        layout.setSpacing(16)

        title = QLabel(tr("New Features!"))
        title.setStyleSheet("font-size: 18px; font-weight: bold; color: #065f46;")
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)

        is_heb = CURRENT_LANG == 'he'
        items = [
            tr("Composition Search: the Printed filter had no effect \u2014 \u201cOnly printed\u201d hid everything and \u201cExclude printed\u201d hid nothing. Filtering by Library, Shelfmark and Title now works too, including on the fragment rows under a manuscript."),
            tr("Focus Search: you can no longer confirm before the manuscript count finishes \u2014 that could quietly run a search over the whole corpus instead of the scope you chose."),
        ]
        bullet = "\u200f\u2022 " if is_heb else "\u2022 "
        features_text = "\n\n".join(f"{bullet}{item}" for item in items)

        features_label = QLabel()
        features_label.setTextFormat(Qt.TextFormat.PlainText)
        features_label.setWordWrap(True)
        features_label.setStyleSheet("font-size: 14px; font-weight: bold;")
        if is_heb:
            features_label.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
            features_label.setAlignment(
                Qt.AlignmentFlag.AlignAbsolute
                | Qt.AlignmentFlag.AlignRight
                | Qt.AlignmentFlag.AlignTop
            )
        else:
            features_label.setAlignment(
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop
            )
        features_label.setText(features_text)
        # Wrap in a scroll area so the four (sometimes long) bullets never clip on
        # smaller displays; the dialog is resizable rather than fixed-size.
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        scroll.setWidget(features_label)
        layout.addWidget(scroll, 1)

        btn_ok = QPushButton(tr("Got it!"))
        btn_ok.setStyleSheet("background-color: #10b981; color: white; font-weight: bold; border-radius: 4px; padding: 8px 24px; font-size: 14px;")
        btn_ok.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_ok.clicked.connect(self.accept)
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        btn_layout.addWidget(btn_ok)
        btn_layout.addStretch()
        layout.addLayout(btn_layout)


class UpdateProgressDialog(QDialog):
    """Shows download progress and handles update installation."""

    def __init__(self, parent, version: str, installer_url: str, html_url: str):
        super().__init__(parent)
        self.version = version
        self.installer_url = installer_url
        self.html_url = html_url
        self.download_thread = None
        self.downloaded_path = None

        self.setWindowTitle(tr("Updating Dicta Genizah Search Pro"))
        self.setModal(True)
        self.setFixedSize(420, 180)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowType.WindowContextHelpButtonHint)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(12)

        # Title
        title_label = QLabel(tr("Updating to version {}").format(version))
        title_label.setStyleSheet("font-size: 14px; font-weight: bold;")
        layout.addWidget(title_label)

        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setMinimum(0)
        self.progress_bar.setMaximum(100)
        self.progress_bar.setValue(0)
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setStyleSheet("""
            QProgressBar {
                border: 1px solid #ccc;
                border-radius: 4px;
                text-align: center;
                height: 24px;
            }
            QProgressBar::chunk {
                background-color: #17a2b8;
                border-radius: 3px;
            }
        """)
        layout.addWidget(self.progress_bar)

        # Status label
        self.status_label = QLabel(tr("Preparing download..."))
        self.status_label.setStyleSheet("color: #666;")
        layout.addWidget(self.status_label)

        layout.addStretch()

        # Buttons
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()

        self.btn_cancel = QPushButton(tr("Cancel"))
        self.btn_cancel.clicked.connect(self.on_cancel)
        btn_layout.addWidget(self.btn_cancel)

        layout.addLayout(btn_layout)

    def start_download(self):
        """Start the download process."""
        import tempfile
        import os

        if not self.installer_url:
            QMessageBox.warning(
                self, tr("Download Error"),
                tr("No direct download available. Opening browser instead...")
            )
            QDesktopServices.openUrl(QUrl(self.html_url))
            self.reject()
            return

        # Determine download path
        temp_dir = tempfile.gettempdir()
        safe_version = self.version.replace('/', '_').replace('\\', '_')
        target_path = os.path.join(temp_dir, f"GenizahSearchPro_{safe_version}_Setup.exe")

        # Import and start download thread
        from gui_threads import UpdateDownloaderThread
        self.download_thread = UpdateDownloaderThread(self.installer_url, target_path)
        self.download_thread.progress_signal.connect(self.on_progress)
        self.download_thread.finished_signal.connect(self.on_download_finished)
        self.download_thread.start()

        self.status_label.setText(tr("Downloading..."))

    def on_progress(self, downloaded: int, total: int):
        """Update progress bar with download progress."""
        if total > 0:
            percent = int((downloaded / total) * 100)
            self.progress_bar.setValue(percent)

            # Format size display
            downloaded_mb = downloaded / (1024 * 1024)
            total_mb = total / (1024 * 1024)
            self.status_label.setText(
                tr("Downloading: {:.1f} MB / {:.1f} MB").format(downloaded_mb, total_mb)
            )
        else:
            # Unknown total size - show indeterminate
            self.progress_bar.setMaximum(0)
            downloaded_mb = downloaded / (1024 * 1024)
            self.status_label.setText(tr("Downloaded: {:.1f} MB").format(downloaded_mb))

    def on_download_finished(self, success: bool, result: str):
        """Handle download completion."""
        if success:
            self.downloaded_path = result
            self.progress_bar.setValue(100)
            self.status_label.setText(tr("Download complete. Installing update..."))
            self.btn_cancel.setEnabled(False)

            # Execute the update
            QTimer.singleShot(500, self.execute_update)
        else:
            # Download failed
            self.progress_bar.setValue(0)
            self.status_label.setText(tr("Download failed"))

            reply = QMessageBox.question(
                self, tr("Download Failed"),
                tr("Failed to download update: {}").format(result) + "\n\n" +
                tr("Would you like to download manually from the website?"),
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            if reply == QMessageBox.StandardButton.Yes:
                QDesktopServices.openUrl(QUrl(self.html_url))

            self.reject()

    def execute_update(self):
        """Run the installer in silent mode (Windows only)."""
        import subprocess
        import sys

        # Check platform
        if sys.platform != 'win32':
            QMessageBox.information(
                self, tr("Update Ready"),
                tr("The update has been downloaded to:") + f"\n{self.downloaded_path}\n\n" +
                tr("Please run the installer manually.")
            )
            self.accept()
            return

        # Check if running as compiled executable
        if not getattr(sys, 'frozen', False):
            # Running from Python (development mode)
            QMessageBox.information(
                self, tr("Development Mode"),
                tr("Auto-update is not available in development mode.") + "\n" +
                tr("The installer has been downloaded to:") + f"\n{self.downloaded_path}"
            )
            self.accept()
            return

        # Update status
        self.status_label.setText(tr("Launching installer..."))

        # Close all sidecar DB connections before the installer tries to
        # overwrite the bundled .db files — otherwise Windows file locks
        # prevent the installer from replacing pgp.db / fjms_enrichment.db
        try:
            from shared.document_service import reset_pgp_service
            from shared.fjms_service import reset_fjms_service
            from shared.nli_crossref_service import reset_nli_crossref_service
            reset_pgp_service()
            reset_fjms_service()
            reset_nli_crossref_service()
        except Exception:
            pass  # Best-effort — installer's CloseApplications=force is fallback

        # Run the installer with silent mode
        # The installer will:
        # 1. Close this running app (CloseApplications=force)
        # 2. Install the update
        # 3. Restart the app (RestartApplications=yes)
        try:
            subprocess.Popen(
                [self.downloaded_path, '/VERYSILENT', '/RESTARTAPPLICATIONS'],
                creationflags=subprocess.DETACHED_PROCESS
            )
        except Exception as e:
            QMessageBox.critical(
                self, tr("Update Error"),
                tr("Could not start installer: {}").format(str(e))
            )
            self.reject()
            return

        # Don't call QApplication.quit() — let the installer's CloseApplications=force
        # handle shutdown. This allows Restart Manager to track the process and relaunch
        # the app after installation. If we quit first, Restart Manager has nothing to restart.
        self.accept()

    def on_cancel(self):
        """Handle cancel button click."""
        if self.download_thread and self.download_thread.isRunning():
            reply = QMessageBox.question(
                self, tr("Cancel Download"),
                tr("Are you sure you want to cancel the update?"),
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            if reply == QMessageBox.StandardButton.Yes:
                self.download_thread.cancel()
                self.status_label.setText(tr("Cancelling..."))
                self.btn_cancel.setEnabled(False)
        else:
            self.reject()

    def closeEvent(self, event):
        """Handle dialog close event."""
        if self.download_thread and self.download_thread.isRunning():
            reply = QMessageBox.question(
                self, tr("Cancel Download"),
                tr("Download is in progress. Cancel and close?"),
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            if reply == QMessageBox.StandardButton.Yes:
                self.download_thread.cancel()
                self.download_thread.wait(3000)  # Wait up to 3 seconds
                event.accept()
            else:
                event.ignore()
        else:
            event.accept()
