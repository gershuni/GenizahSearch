# -*- coding: utf-8 -*-
"""What's New in every version, on the desktop (owner, 2026-11-05).

Two uses of one dialog:

- **History**: the ☰ / side menu's "What's New in every version" lists every
  release this copy knows (``shared/whats_new_history.py``, bundled), newest first.
- **Update**: when a newer version is out, the update bar's "What's new?" and the
  manual check show what changed between this copy's version and the new one.
  An old copy cannot know newer entries, so they come from the website
  (``GET /api/whats-new?since=&upto=&app=desktop``) in ``ReleaseNotesThread``;
  if that fails the dialog says so and links the GitHub release page.

GUARD-01: NO module-level ``import genizah_app`` -- tr / CURRENT_LANG come from
``genizah_core``.
"""
from __future__ import annotations

import html
import logging
from typing import Optional

from PyQt6.QtCore import QSize, QThread, Qt, QUrl, pyqtSignal
from PyQt6.QtGui import QDesktopServices
from PyQt6.QtWidgets import (
    QApplication,
    QDialog,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTextBrowser,
    QVBoxLayout,
)

import genizah_core
from genizah_core import tr
from shared.whats_new_history import CATEGORY_LABELS, app_note, from_public, localize


def _lang() -> str:
    """The interface language now (read at call time, as tr() does)."""
    return genizah_core.CURRENT_LANG

logger = logging.getLogger(__name__)

#: Where an installed copy reads the entries it does not have.
WHATS_NEW_API_URL = 'https://genizahsearch.com/api/whats-new'
_FETCH_TIMEOUT = 8


def render_html(releases: list[dict], lang: str) -> str:
    """The releases as the dialog's HTML: a heading per version, then its categories."""
    rtl = lang == 'he'
    align = 'right' if rtl else 'left'
    parts = [f'<div dir="{"rtl" if rtl else "ltr"}" style="text-align:{align};">']
    for release in releases:
        view = localize(release, lang)
        heading = html.escape(tr('Version {version}').format(version=view['version']))
        parts.append(
            f'<h3 style="margin:14px 0 2px 0;">{heading} '
            f'<span style="color:#6b7280;font-weight:normal;font-size:12px;">'
            f'<span dir="ltr">{html.escape(view["date"])}</span></span></h3>')
        parts.append(f'<div style="font-weight:bold;margin-bottom:4px;">{html.escape(view["title"])}</div>')
        for category, items in view['sections']:
            parts.append(f'<div style="color:#17634d;font-weight:bold;margin-top:6px;">'
                         f'{html.escape(tr(CATEGORY_LABELS[category]))}</div>')
            parts.append('<ul style="margin-top:2px;">')
            for item in items:
                note = app_note(item['apps'])
                tag = (f' <span style="color:#6b7280;font-size:11px;">({html.escape(tr(note))})</span>'
                       if note else '')
                parts.append(f'<li>{html.escape(item["text"])}{tag}</li>')
            parts.append('</ul>')
    parts.append('</div>')
    return ''.join(parts)


class ReleaseNotesThread(QThread):
    """Fetches the entries between two versions from the website, off the UI thread.

    ``finished_signal`` carries the releases (a list) on success, or None on any
    failure (network, status, a malformed answer)."""

    finished_signal = pyqtSignal(object)

    def __init__(self, since: str, upto: str, parent=None):
        super().__init__(parent)
        self.since = since
        self.upto = upto

    def run(self):
        try:
            import requests
            from version import APP_VERSION
            resp = requests.get(
                WHATS_NEW_API_URL,
                params={'since': self.since, 'upto': self.upto, 'app': 'desktop'},
                headers={'User-Agent': f'GenizahSearchPro/{APP_VERSION}', 'Accept': 'application/json'},
                timeout=_FETCH_TIMEOUT,
            )
            if resp.status_code != 200:
                raise ValueError(f'status {resp.status_code}')
            self.finished_signal.emit(from_public(resp.json()))
        except Exception as exc:  # noqa: BLE001 - any failure falls back to the release page
            logger.warning('ReleaseNotesThread: the list of changes could not be loaded: %s', exc)
            self.finished_signal.emit(None)


def _strip_v(version: str) -> str:
    return str(version or '').strip().lstrip('vV')


#: The size the dialog asks for, and the least it keeps: the list scrolls, the buttons stay.
PREFERRED_SIZE = QSize(640, 640)
SMALLEST_SIZE = QSize(300, 220)
#: Room left for the window frame and title bar inside the screen's available area.
FRAME_ALLOWANCE = QSize(40, 70)


def fit_size(available: QSize) -> tuple[QSize, QSize]:
    """``(minimum, size)`` for a screen whose available area is ``available``: the preferred
    size shrunk to what the screen holds with the frame, the minimum never above that.
    1920x1080 at 300% leaves about 640x360 logical pixels; a fixed 520x480 minimum put
    Update Now / Later below the screen's edge (Codex, PR #482)."""
    room_w = max(available.width() - FRAME_ALLOWANCE.width(), 1)
    room_h = max(available.height() - FRAME_ALLOWANCE.height(), 1)
    minimum = QSize(min(SMALLEST_SIZE.width(), room_w), min(SMALLEST_SIZE.height(), room_h))
    size = QSize(max(min(PREFERRED_SIZE.width(), room_w), minimum.width()),
                 max(min(PREFERRED_SIZE.height(), room_h), minimum.height()))
    return minimum, size


class VersionHistoryDialog(QDialog):
    """The history (``releases`` given) or, with ``update_to``, the changes since this
    copy's version, loaded from the website, with Update Now / Later."""

    def __init__(self, parent=None, *, releases: Optional[list] = None,
                 current_version: str = '', update_to: str = '', html_url: str = ''):
        super().__init__(parent)
        self._html_url = html_url
        self._thread: Optional[ReleaseNotesThread] = None
        self.update_mode = bool(update_to)
        self.setModal(True)
        screen = (parent.screen() if parent is not None else None) or QApplication.primaryScreen()
        minimum, size = fit_size(screen.availableGeometry().size() if screen is not None
                                 else PREFERRED_SIZE + FRAME_ALLOWANCE)
        self.setMinimumSize(minimum)
        self.resize(size)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowType.WindowContextHelpButtonHint)
        if _lang() == 'he':
            self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(10)

        if self.update_mode:
            title = tr("What's new since your version ({version})").format(version=_strip_v(current_version))
        else:
            title = tr("What's New in every version")
        self.setWindowTitle(title)
        self.title_label = QLabel(title)
        self.title_label.setStyleSheet('font-size: 16px; font-weight: bold; color: #065f46;')
        self.title_label.setWordWrap(True)
        layout.addWidget(self.title_label)
        if self.update_mode:
            self.subtitle_label = QLabel(tr("A new version is available: {}").format(update_to))
            self.subtitle_label.setStyleSheet('font-size: 13px;')
            layout.addWidget(self.subtitle_label)

        self.browser = QTextBrowser()
        self.browser.setOpenExternalLinks(True)
        layout.addWidget(self.browser, 1)

        self.link_btn = QPushButton(tr('Release notes on GitHub'))
        self.link_btn.setFlat(True)
        self.link_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.link_btn.setStyleSheet('color: #0c5460; text-decoration: underline; border: none;')
        self.link_btn.clicked.connect(self._open_release_page)
        self.link_btn.setVisible(False)
        layout.addWidget(self.link_btn, 0, Qt.AlignmentFlag.AlignLeft if _lang() != 'he'
                         else Qt.AlignmentFlag.AlignRight)

        buttons = QHBoxLayout()
        buttons.addStretch()
        if self.update_mode:
            self.update_btn = QPushButton(tr('Update Now'))
            self.update_btn.setStyleSheet('background-color: #17a2b8; color: white; font-weight: bold; '
                                          'border-radius: 4px; padding: 6px 16px;')
            self.update_btn.clicked.connect(self.accept)
            later = QPushButton(tr('Later'))
            later.clicked.connect(self.reject)
            buttons.addWidget(self.update_btn)
            buttons.addWidget(later)
        else:
            close = QPushButton(tr('Close'))
            close.clicked.connect(self.accept)
            buttons.addWidget(close)
        layout.addLayout(buttons)

        if self.update_mode:
            self.set_loading()
            self._thread = ReleaseNotesThread(_strip_v(current_version), _strip_v(update_to))
            self._thread.finished_signal.connect(self._on_notes)
            self._thread.start()
        else:
            self.set_releases(releases or [])

    def done(self, result):  # noqa: N802 - Qt API
        """Closing while the fetch still runs hands the thread to the keeper (a dropped
        running QThread aborts the process, 0xC0000409)."""
        thread = self._thread
        if thread is not None and thread.isRunning():
            try:
                thread.finished_signal.disconnect(self._on_notes)
            except (TypeError, RuntimeError):
                pass
            from desktop.gui_threads import _keep_until_finished
            _keep_until_finished(thread)
        super().done(result)

    def set_releases(self, releases: list) -> None:
        self.browser.setHtml(render_html(releases, _lang()))
        self.link_btn.setVisible(False)

    def set_loading(self) -> None:
        self.browser.setPlainText(tr('Loading the list of changes...'))

    def set_failed(self) -> None:
        self.browser.setPlainText(tr('The list of changes could not be loaded.'))
        self.link_btn.setVisible(bool(self._html_url))

    def _on_notes(self, releases) -> None:
        # None: the fetch failed; []: the website lists nothing for the desktop between
        # the two versions. Either way the release page says what changed.
        if releases:
            self.set_releases(releases)
        else:
            self.set_failed()

    def _open_release_page(self) -> None:
        if self._html_url:
            QDesktopServices.openUrl(QUrl(self._html_url))
