# -*- coding: utf-8 -*-
"""Phase 112 Plan 01 — Bilingual consent + privacy-disclosure dialogs.

Two dialog classes:
  ConsentDialog  — first-run opt-in prompt (single done() finalizer).
  PrivacyDialog  — full bilingual disclosure (D-10 points, EN+HE stacked).

Design invariants:
  - NEVER imports the shared PostHog transport module (PRIV-03 AST guard).
    All consent writes go through desktop.telemetry.set_consent().
  - save_app_config is called via MODULE-ATTRIBUTE access
    (`genizah_core.save_app_config(...)`) so the test fixture's monkeypatch
    of `genizah_core.save_app_config` intercepts dialog writes correctly.
    Do NOT change to a from-import of save_app_config.
  - The SINGLE done(result) finalizer is the SOLE exit path that writes
    FIRST_RUN_SHOWN_KEY and calls set_consent — accept/decline/Escape/X
    all route through done() (REVIEWS HIGH-1).
  - Both buttons have setDefault(False) + setAutoDefault(False) (SC#1).
  - keyPressEvent routes Return/Enter to _on_decline so Enter can never
    silently opt in even when a button is focused (REVIEWS HIGH-2, D-05).
"""

from __future__ import annotations

import genizah_core
from desktop import telemetry
from desktop.telemetry import FIRST_RUN_SHOWN_KEY
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QPalette
from PyQt6.QtWidgets import (
    QApplication,
    QDialog,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTextBrowser,
    QVBoxLayout,
)


# ---------------------------------------------------------------------------
# ConsentDialog
# ---------------------------------------------------------------------------

class ConsentDialog(QDialog):
    """First-run bilingual consent dialog.

    Features:
    - Personal first-person appeal from the developer (D-11).
    - Both EN and HE text always visible regardless of CURRENT_LANG (D-01).
    - Two equal-weight buttons, neither default (T-112-EnterOptIn SC#1).
    - keyPressEvent intercepts Return/Enter → implicit decline (D-05).
    - Single done() finalizer writes FIRST_RUN_SHOWN_KEY + calls set_consent
      on ALL exit paths: Enable, Not now, Escape, X-close (REVIEWS HIGH-1).
    - "Learn more" button opens PrivacyDialog (D-09).
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Telemetry Consent / הסכמה לטלמטריה")
        self.setModal(True)
        self.setFixedSize(540, 480)
        self.setWindowFlags(
            self.windowFlags() & ~Qt.WindowType.WindowContextHelpButtonHint
        )
        # Do NOT call setLayoutDirection — both languages always visible (D-01)

        # Internal flag: True ONLY when user explicitly clicks Enable.
        # Every other exit (decline, Escape, X) leaves this False.
        self._accepted_telemetry: bool = False

        layout = QVBoxLayout(self)
        layout.setContentsMargins(24, 24, 24, 20)
        layout.setSpacing(12)

        # --- Message body (bilingual, stacked EN then HE) ---
        msg = QTextBrowser()
        msg.setOpenExternalLinks(False)
        msg.setReadOnly(True)
        msg.setFrameStyle(0)  # no border
        msg.setHtml(self._build_message_html())
        layout.addWidget(msg)

        # --- "Learn more" flat link button ---
        btn_learn = QPushButton("Learn more / מידע נוסף")
        btn_learn.setFlat(True)
        btn_learn.setCursor(Qt.CursorShape.PointingHandCursor)
        btn_learn.setStyleSheet("color: #2563eb; text-decoration: underline; border: none;")
        btn_learn.clicked.connect(self._on_learn_more)
        learn_row = QHBoxLayout()
        learn_row.addWidget(btn_learn)
        learn_row.addStretch()
        layout.addLayout(learn_row)

        layout.addStretch()

        # --- Action buttons (equal-weight, NO default on either) ---
        self.btn_enable = QPushButton("Enable / הפעל")
        self.btn_enable.setDefault(False)
        self.btn_enable.setAutoDefault(False)
        self.btn_enable.setStyleSheet(
            "background-color: #10b981; color: white; font-weight: bold; "
            "border-radius: 4px; padding: 8px 20px;"
        )
        self.btn_enable.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_enable.clicked.connect(self._on_enable)

        self.btn_decline = QPushButton("Not now / לא עכשיו")
        self.btn_decline.setDefault(False)
        self.btn_decline.setAutoDefault(False)
        self.btn_decline.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_decline.clicked.connect(self._on_decline)

        btn_row = QHBoxLayout()
        btn_row.addStretch()
        btn_row.addWidget(self.btn_enable)
        btn_row.addSpacing(12)
        btn_row.addWidget(self.btn_decline)
        btn_row.addStretch()
        layout.addLayout(btn_row)

    # ------------------------------------------------------------------
    # Message HTML (bilingual, personal first-person appeal — D-11)
    # ------------------------------------------------------------------

    @staticmethod
    def _build_message_html() -> str:
        en_copy = (
            "Hi. My name is Hillel Gershuni, and under the gracious auspices of Dicta "
            "I'm constantly building and improving Dicta Genizah Search Pro. I'm glad to "
            "receive your feedback — suggestions, complaints, and bugs — and in order to "
            "improve the software I'd now like to collect data about how it is used.<br><br>"
            "If you agree, the software will send <b>general data</b> (such as which features "
            "you use, your software version and operating system, performance metrics, and "
            "the like — <b>not</b> search content or your personal library). You can withdraw "
            "your consent at any time in Settings.<br><br>"
            "Oh, and regardless — you're always welcome to email me and tell me how it's "
            "going with the software.<br><br>"
            "<i>Hillel Gershuni, Dicta, gershuni@gmail.com</i>"
        )
        he_copy = (
            "היי. שמי הוא הלל גרשוני, ובחסותה האדיבה של דיקטה אני בונה ומשפר כל הזמן את "
            "Dicta Genizah Search Pro. אני שמח לקבל מכם משוב – הצעות, תלונות ובאגים – וכדי "
            "לשפר את התוכנה אני רוצה כעת לאסוף נתונים על השימוש בה.<br><br>"
            "אם תסכימו, התוכנה תשלח <b>נתונים כלליים</b> (כגון באילו תכונות אתם משתמשים, מהי "
            "גרסת התוכנה ומערכת ההפעלה שלכם, מדדי ביצועים וכיוצא בזה — לא תוכני חיפוש או "
            "ספרייה אישית). אפשר לבטל את ההסכמה בכל רגע בהגדרות.<br><br>"
            "אה, ובלי קשר אתם מוזמנים תמיד לשלוח לי מייל ולספר איך הולך עם התוכנה.<br><br>"
            "<i>הלל גרשוני, דיקטה, gershuni@gmail.com</i>"
        )
        return (
            "<html><body style='font-size: 13px;'>"
            "<div dir='ltr' style='font-size: 11px; color: #888; margin-bottom: 6px;'>(English below)</div>"
            f"<div dir='rtl' style='margin-bottom: 16px;'>{he_copy}</div>"
            "<hr style='border: 0; border-top: 1px solid #ddd; margin: 8px 0;'>"
            f"<div dir='ltr' style='margin-top: 8px;'>{en_copy}</div>"
            "</body></html>"
        )

    # ------------------------------------------------------------------
    # Button slots
    # ------------------------------------------------------------------

    def _on_enable(self) -> None:
        """User explicitly clicked Enable — set the opt-in flag and accept."""
        self._accepted_telemetry = True
        self.accept()  # routes through done(Accepted)

    def _on_decline(self) -> None:
        """User clicked 'Not now' (or Enter was pressed) — implicit decline."""
        # _accepted_telemetry stays False
        self.reject()  # routes through done(Rejected)

    def _on_learn_more(self) -> None:
        """Open the full bilingual privacy disclosure (startup flow — bilingual)."""
        dlg = PrivacyDialog(self, bilingual=True)
        dlg.exec()

    # ------------------------------------------------------------------
    # Key event override (REVIEWS HIGH-2, D-05, SC#1)
    # Return/Enter routes to _on_decline — Enter can NEVER silently opt in.
    # This is belt-and-braces on top of no-default buttons: a focused
    # QPushButton could otherwise consume Return before keyPressEvent fires.
    # ------------------------------------------------------------------

    def keyPressEvent(self, event) -> None:
        if event.key() in (Qt.Key.Key_Return, Qt.Key.Key_Enter):
            self._on_decline()
            return
        super().keyPressEvent(event)

    # ------------------------------------------------------------------
    # SINGLE done() finalizer (REVIEWS HIGH-1)
    # This is the SOLE path that writes FIRST_RUN_SHOWN_KEY and calls
    # set_consent — it fires on accept(), reject(), close(), and any
    # programmatic QDialog termination (Escape and X both route through
    # reject() → done(); accept() → done()).
    # DO NOT add set_consent / flag-write calls to closeEvent, reject, or accept.
    # ------------------------------------------------------------------

    def done(self, result: int) -> None:  # type: ignore[override]
        # Write the shown-flag UNCONDITIONALLY on every exit path (D-05).
        # genizah_core.save_app_config is called via MODULE-ATTRIBUTE access
        # so the test fixture's monkeypatch of genizah_core.save_app_config
        # intercepts this call correctly.
        genizah_core.save_app_config({FIRST_RUN_SHOWN_KEY: True})
        # Opt-in ONLY when the user explicitly clicked Enable.
        telemetry.set_consent(True if self._accepted_telemetry else False)
        super().done(result)


# ---------------------------------------------------------------------------
# PrivacyDialog
# ---------------------------------------------------------------------------

class PrivacyDialog(QDialog):
    """Full bilingual privacy disclosure dialog.

    Covers D-10 points bilingually (EN+HE stacked, both always visible — D-02):
    - what IS collected (privacy-preserving usage/feature counts, OS/version,
      perf buckets, crash signals — counts only)
    - what is NOT collected (no search/query content, no My Library file paths
      or filenames, no email/name beyond bare Supabase user.id when logged in)
    - who processes data (PostHog EU + Dicta)
    - how to opt out (Settings → General → Preferences)
    - pseudonymous install id

    Tone: neutral, factual reference text (NOT a personal appeal — that is
    ConsentDialog's role per D-11).

    NOTE: Hebrew copy is best-effort and flagged for translation-workflow review
    (RESEARCH Open Question 2).  English copy is authoritative.
    """

    def __init__(self, parent=None, bilingual=False):
        super().__init__(parent)
        # bilingual=True ONLY from the one-time startup consent flow (ConsentDialog
        # "Learn more"). Everywhere else (Settings → Privacy details, About tab) the
        # dialog follows the UI language — single-language per CURRENT_LANG.
        self._bilingual = bilingual
        self._he = genizah_core.CURRENT_LANG == 'he'
        if bilingual:
            self.setWindowTitle("Privacy / פרטיות")
        else:
            self.setWindowTitle("פרטיות" if self._he else "Privacy")
        self.setModal(True)
        self.resize(620, 540)
        self.setWindowFlags(
            self.windowFlags() & ~Qt.WindowType.WindowContextHelpButtonHint
        )

        # Palette-aware colours (SettingsDialog pattern, genizah_app.py:2167-2176)
        pal = QApplication.palette()
        self._is_dark = pal.color(QPalette.ColorRole.Window).lightness() < 128
        self._text = pal.color(QPalette.ColorRole.Text).name()
        self._base = pal.color(QPalette.ColorRole.Base).name()

        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 16)
        layout.setSpacing(12)

        if bilingual:
            _title_text = "Privacy / פרטיות"
        else:
            _title_text = "פרטיות" if self._he else "Privacy"
        title_lbl = QLabel(_title_text)
        title_lbl.setStyleSheet("font-size: 15px; font-weight: bold;")
        layout.addWidget(title_lbl)

        browser = QTextBrowser()
        browser.setOpenExternalLinks(True)
        browser.setHtml(self._build_html())
        layout.addWidget(browser)

        if bilingual:
            _close_text = "Close / סגור"
        else:
            _close_text = "סגור" if self._he else "Close"
        btn_close = QPushButton(_close_text)
        btn_close.clicked.connect(self.accept)
        close_row = QHBoxLayout()
        close_row.addStretch()
        close_row.addWidget(btn_close)
        layout.addLayout(close_row)

    def _build_html(self) -> str:  # noqa: PLR0912 (complexity allowed for HTML builder)
        bg = self._base
        fg = self._text

        en_html = """
<p>To help improve Dicta Genizah Search Pro, you can allow it to send
<b>privacy-preserving usage data</b> — data that does not compromise your privacy.
Nothing is sent unless you enable it in Settings.</p>
<h3 style='margin-top:0;'>What is sent</h3>
<ul>
  <li>Which features are used (no usage content)</li>
  <li>App version and operating system</li>
  <li>Performance timing buckets (aggregated, not per-search)</li>
  <li>Crash signals (exception type only — <b>never</b> the exception message text)</li>
</ul>
<h3>What is NOT sent</h3>
<ul>
  <li>Your search queries or text content</li>
  <li>My Library file paths, filenames, or document content</li>
  <li>Your name or email address</li>
  <li>When you are signed in: the only identity attached is your Supabase
      account identifier (a <code>UUID</code>) — a pseudonymous identifier, the
      same one the website already uses. No additional personal data is ever added.</li>
</ul>
<h3>Who processes the data</h3>
<p>Usage data is processed by
<a href='https://posthog.com/privacy'>PostHog</a> (EU region) and Dicta.
PostHog is an open-source analytics platform; data is stored in the EU
and governed by their <a href='https://posthog.com/privacy'>privacy policy</a>.</p>
<h3>The install id</h3>
<p>A random pseudonymous install identifier (UUID) is generated when you first opt in.
It is retained locally even if you later opt out, so a future re-opt-in preserves continuity.
It is never linked to your name, email, or any personal account.</p>
<h3>How to opt out</h3>
<p>Open <b>Settings → General → Preferences</b> and uncheck
<i>Help improve the app</i>. Opting out takes effect immediately.</p>
"""

        he_html = """
<p>לשם שיפור Dicta Genizah Search Pro, ניתן לאפשר לה לשלוח נתוני שימוש שאינם
פוגעים בפרטיות המשתמש. שום דבר לא נשלח אלא אם כן תפעילו זאת בהגדרות.</p>
<h3 style='margin-top:0;'>מה נשלח</h3>
<ul>
  <li>אילו תכונות בשימוש (ללא תוכן השימוש)</li>
  <li>גרסת האפליקציה ומערכת ההפעלה</li>
  <li>קטגוריות זמן תגובה (מצטברות, לא לפי חיפוש)</li>
  <li>סימני קריסה (סוג השגיאה בלבד, ללא טקסט הודעת השגיאה)</li>
</ul>
<h3>מה לא נשלח</h3>
<ul>
  <li>שאילתות החיפוש שלכם או תכני הטקסט</li>
  <li>נתיבי קבצים, שמות קבצים או תוכן מסמכים מ&#x2018;הספרייה שלי&#x2019;</li>
  <li>שמכם או כתובת הדוא&#x05F4;ל שלכם</li>
  <li>כשאתם מחוברים: המזהה היחיד המצורף הוא מזהה החשבון שלכם ב-Supabase (מסוג <code>UUID</code>)
      — מזהה פסאודו-אנונימי, אותו מזהה שהאתר כבר משתמש בו. לא מתווסף שום מידע אישי נוסף.</li>
</ul>
<h3>מי מעבד את הנתונים</h3>
<p>נתוני השימוש מעובדים על ידי
<a href='https://posthog.com/privacy'>PostHog</a> (אזור האיחוד האירופי) ודיקטה.
PostHog הוא פלטפורמת אנליטיקה קוד-פתוח; הנתונים מאוחסנים באיחוד האירופי
ומנוהלים לפי <a href='https://posthog.com/privacy'>מדיניות הפרטיות שלהם</a>.</p>
<h3>מזהה ההתקנה</h3>
<p>מזהה התקנה פסאודו-אנונימי אקראי (UUID) נוצר בעת הסכמה ראשונה.
הוא נשמר מקומית גם לאחר ביטול ההסכמה, כך שחידוש ההסכמה בעתיד שומר על רציפות.
הוא לעולם אינו מקושר לשם, לדוא&#x05F4;ל או לחשבון אישי כלשהו.</p>
<h3>כיצד לבטל את ההסכמה</h3>
<p>פתחו את <b>הגדרות → כללי → העדפות</b> ובטלו את הסימון של
<i>עזרו לשפר את האפליקציה</i>. הביטול נכנס לתוקף מיידית.</p>
"""

        en_block = f"<div dir='ltr' style='margin-bottom:20px;'>{en_html}</div>"
        he_block = f"<div dir='rtl' style='margin-top:12px;'>{he_html}</div>"
        # bilingual=True (startup) shows both stacked, Hebrew first; otherwise
        # follow UI language.
        if getattr(self, '_bilingual', False):
            body = (
                "<div dir='ltr' style='font-size:11px; color:#888; margin-bottom:6px;'>(English below)</div>"
                + he_block
                + "<hr style='border:0; border-top:1px solid #aaa; margin:12px 0;'>"
                + en_block
            )
        elif getattr(self, '_he', False):
            body = he_block
        else:
            body = en_block

        return (
            f"<html><body style='background:{bg}; color:{fg}; font-size:13px;'>"
            f"{body}"
            "</body></html>"
        )
