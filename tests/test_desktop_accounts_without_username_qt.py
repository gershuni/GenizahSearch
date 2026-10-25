# -*- coding: utf-8 -*-
"""The call sites of tests/test_desktop_accounts_without_username.py.

Both correction lists -- ResultDialog's version selector and the Browse tab's
(GenizahGUI._check_document_community_status) -- get corrections parsed by the
real Supabase client, and must show a signed-in author their own unpublished
corrections whether or not the account has a username, keep showing a
reviewer everyone's, and never show anyone else's unpublished ones. The
account name on screen, and the Register / Login dialogs under email
confirmation, are driven here too.
"""
import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtWidgets import QApplication, QComboBox

import genizah_core
from desktop import corrections_ui
from desktop import supabase_corrections_client as scc
from desktop.result_dialog import ResultDialog
from genizah_app import GenizahGUI
from shared.genizah_translations import TRANSLATIONS

pytestmark = pytest.mark.gui  # imports PyQt6: gui bucket only.

_APP = QApplication.instance() or QApplication([])

ME = "6f1c1e2a-0000-4000-8000-000000000001"
OTHER = "6f1c1e2a-0000-4000-8000-000000000002"
THIRD = "6f1c1e2a-0000-4000-8000-000000000003"
SYS_ID = "990000000000000001"


@pytest.fixture(autouse=True)
def _english(monkeypatch):
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", "en")


def _corr(cid, author_id, username, status, created_at="2026-10-21T09:00:00+00:00"):
    return scc.SupabaseCorrectionsClient._parse_correction(None, {
        "id": cid, "sys_id": SYS_ID, "page_number": 1, "original_text": "a",
        "corrected_text": f"text {cid}", "status": status, "author_id": author_id,
        "created_at": created_at,
        "profiles": {"id": author_id, "username": username, "full_name": None}})


def _user(username=None, role="user", uuid=ME, full_name=None):
    return scc.User(id=hash(uuid) % (10**9), email="reader@example.org", username=username,
                    full_name=full_name, role=role, _uuid=uuid)


class _Client:
    def __init__(self, user, corrections):
        self.current_user = user
        self._corrections = corrections

    def is_server_available(self, force_check=False):
        return True

    def get_page_versions(self, document_id, page_num=1):
        return {"all_versions": [], "current_default": None, "total": 0}

    def get_corrections_for_document(self, document_id, include_drafts=False, ie_id=None):
        return list(self._corrections)

    def get_comments_for_document(self, *a, **k):
        return []


# ---------------------------------------------------------------------------
# The two correction lists
# ---------------------------------------------------------------------------

class _RD:
    _rd_refresh_versions = ResultDialog._rd_refresh_versions

    def __init__(self, client):
        self._app = SimpleNamespace(corrections_client=client)
        self.current_sys_id = SYS_ID
        self.current_p_num = 1
        self.current_volume_ie = None
        self.rd_version_combo = QComboBox()

    def _rd_load_version_content(self, data):
        pass

    def shown(self):
        combo = self.rd_version_combo
        return [combo.itemData(i).get("correction_id") for i in range(combo.count())
                if (combo.itemData(i) or {}).get("source") == "correction"]

    def selected(self):
        return (self.rd_version_combo.currentData() or {}).get("correction_id")


class _Button:
    def setVisible(self, b):
        pass

    def setEnabled(self, b):
        pass


class _Browse:
    _check_document_community_status = GenizahGUI._check_document_community_status

    def __init__(self, client):
        self.corrections_client = client
        self.current_browse_sid = SYS_ID
        self.current_browse_p = 1
        self.current_browse_volume_ie = None
        self.browse_text = SimpleNamespace(toPlainText=lambda: "page text")
        self.browse_version_combo = QComboBox()
        self.btn_b_view_comments = _Button()

    def _update_browse_pgp_button(self, doc):
        pass

    def _browse_load_version(self, data):
        pass

    def shown(self):
        combo = self.browse_version_combo
        return [combo.itemData(i).get("correction_id") for i in range(combo.count())
                if (combo.itemData(i) or {}).get("source") == "correction"]


def _rd(user, corrections):
    rd = _RD(_Client(user, corrections))
    rd._rd_refresh_versions(select_latest=False)
    return rd


def _browse(user, corrections):
    host = _Browse(_Client(user, corrections))
    host._check_document_community_status()
    return host


LISTS = {"result-dialog": _rd, "browse": _browse}


@pytest.fixture(params=sorted(LISTS))
def show(request):
    return lambda user, corrections: LISTS[request.param](user, corrections).shown()


def test_own_draft_is_shown_with_no_username(show):
    assert show(_user(username=None), [_corr(1, ME, None, "draft")]) == [1]


def test_own_pending_is_shown_with_a_username(show):
    assert show(_user(username="hillel"), [_corr(1, ME, "hillel", "pending")]) == [1]


def test_own_correction_from_before_2026_10_21_is_shown_to_a_cached_sign_in(show):
    """A sign-in cached before the change keeps the email address as its
    username; the profile now has none, or the typed username back."""
    cached = _user(username="reader@example.org")
    old = "2026-09-30T12:00:00+00:00"
    assert show(cached, [_corr(1, ME, None, "rejected", created_at=old)]) == [1]
    assert show(cached, [_corr(2, ME, "reader", "draft", created_at=old)]) == [2]


def test_own_draft_survives_a_later_correction_by_another_author_without_a_username(show):
    shown = show(_user(username=None), [
        _corr(1, ME, None, "draft", created_at="2026-10-21T09:00:00+00:00"),
        _corr(2, OTHER, None, "pending", created_at="2026-10-21T10:00:00+00:00"),
        _corr(3, THIRD, None, "approved", created_at="2026-10-21T11:00:00+00:00"),
    ])
    assert sorted(shown) == [1, 3]


def test_a_reviewer_still_sees_everyones(show):
    shown = show(_user(username=None, role="reviewer"), [
        _corr(1, OTHER, None, "draft"),
        _corr(2, THIRD, None, "rejected"),
        _corr(3, ME, None, "approved"),
        _corr(4, "6f1c1e2a-0000-4000-8000-000000000004", "alice", "pending"),
    ])
    assert sorted(shown) == [1, 2, 3, 4]


def test_no_one_else_sees_another_authors_unpublished_corrections(show):
    """Neither account has a username: the shared 'User' label must not make
    them one author."""
    shown = show(_user(username=None), [
        _corr(1, OTHER, None, "draft"),
        _corr(2, THIRD, None, "pending"),
        _corr(4, "6f1c1e2a-0000-4000-8000-000000000004", None, "rejected"),
        _corr(5, "6f1c1e2a-0000-4000-8000-000000000005", None, "approved"),
    ])
    assert shown == [5]


def test_result_dialog_selects_the_users_own_correction_with_no_username():
    rd = _rd(_user(username=None), [
        _corr(1, OTHER, "alice", "approved", created_at="2026-10-21T08:00:00+00:00"),
        _corr(2, ME, None, "draft", created_at="2026-10-20T08:00:00+00:00"),
        _corr(3, THIRD, "bob", "approved", created_at="2026-10-21T09:00:00+00:00"),
    ])
    assert rd.selected() == 2


# ---------------------------------------------------------------------------
# The account name on screen
# ---------------------------------------------------------------------------

class _LoggedIn:
    def __init__(self, user):
        self.current_user = user

    def is_logged_in(self):
        return True


@pytest.mark.parametrize("user, expected", [
    (_user(username=None, full_name="Hillel G"), "Hillel G"),
    (_user(username=None), "User"),
    (_user(username="hillel"), "hillel"),
])
def test_status_widget_names_the_account(user, expected):
    widget = corrections_ui.CorrectionsStatusWidget(client=_LoggedIn(user))
    try:
        assert widget.user_label.text() == f"⭐ 0 | {expected}"
    finally:
        widget.deleteLater()


class _Label:
    def __init__(self):
        self.text = None

    def setText(self, text):
        self.text = text

    def hide(self):
        pass

    def show(self):
        pass


class _Host:
    _update_corner_login_state = GenizahGUI._update_corner_login_state
    _update_community_header = GenizahGUI._update_community_header

    def __init__(self, user):
        self.corrections_client = _LoggedIn(user)
        self.corner_login_btn = _Label()
        self.community_user_label = _Label()
        self.community_register_btn = _Label()
        self._logout_pending = False

    def _set_lists_sync_now_enabled(self, enabled):
        pass


def test_corner_button_and_community_header_never_show_none():
    host = _Host(_user(username=None))
    host._update_corner_login_state()
    host._update_community_header()
    # Logged in, the button names the account and opens the account menu
    # (Sync lists now, Logout): with no name to show, "Account".
    assert host.corner_login_btn.text == "Account \u25be"
    assert "None" not in host.community_user_label.text
    assert "<b>User</b>" in host.community_user_label.text

    host = _Host(_user(username=None, full_name="Hillel G"))
    host._update_corner_login_state()
    host._update_community_header()
    assert host.corner_login_btn.text == "Hillel G \u25be"
    assert "<b>Hillel G</b>" in host.community_user_label.text


# ---------------------------------------------------------------------------
# Register / Login dialogs under email confirmation
# ---------------------------------------------------------------------------

class _Auth:
    def __init__(self, session=None, error=None):
        self._session, self._error = session, error

    def sign_up(self, credentials):
        return SimpleNamespace(user=SimpleNamespace(id=ME), session=self._session)

    def sign_in_with_password(self, credentials):
        raise self._error


class _Supabase:
    def __init__(self, **kw):
        self.auth = _Auth(**kw)
        self.tables = []

    def table(self, name):
        self.tables.append(name)
        raise AssertionError(f"no table write expected, got {name}")


@pytest.fixture
def boxes(monkeypatch):
    shown = []
    for kind in ("information", "warning"):
        monkeypatch.setattr(corrections_ui.QMessageBox, kind,
                            lambda parent, title, text, _k=kind: shown.append((_k, title, text)))
    return shown


def _supabase_client(tmp_path, **kw):
    client = scc.SupabaseCorrectionsClient(config_path=tmp_path / "corrections")
    client._client = _Supabase(**kw)
    return client


def _fill_and_register(client):
    dialog = corrections_ui.RegisterDialog(client=client)
    dialog.email_input.setText("new@example.org")
    dialog.username_input.setText("newreader")
    dialog.password_input.setText("Secret123")
    dialog.confirm_input.setText("Secret123")
    dialog.do_register()
    return dialog


@pytest.mark.parametrize("lang", ["en", "he"])
def test_register_without_a_session_says_check_your_inbox(tmp_path, boxes, monkeypatch, lang):
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", lang)
    client = _supabase_client(tmp_path, session=None)
    dialog = _fill_and_register(client)
    try:
        template = TRANSLATIONS[scc.CHECK_EMAIL_MESSAGE] if lang == "he" else scc.CHECK_EMAIL_MESSAGE
        assert boxes == [("information", genizah_core.tr("Create Account"),
                          template.format(email="new@example.org"))]
        assert client._client.tables == []
        assert dialog.result() == dialog.DialogCode.Accepted
    finally:
        dialog.deleteLater()


def test_register_with_a_session_still_says_account_created(boxes):
    class _Registered:
        def register(self, **kw):
            return True, "Registration successful"

    dialog = _fill_and_register(_Registered())
    try:
        assert boxes == [("information", "Success", "Account created! You can now login.")]
    finally:
        dialog.deleteLater()


@pytest.mark.parametrize("lang", ["en", "he"])
def test_login_before_confirming_shows_the_approved_message(tmp_path, boxes, monkeypatch, lang):
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", lang)
    client = _supabase_client(
        tmp_path, error=scc.AuthApiError("Email not confirmed", 400, "email_not_confirmed"))
    dialog = corrections_ui.LoginDialog(client=client)
    try:
        dialog.email_input.setText("new@example.org")
        dialog.password_input.setText("Secret123")
        dialog.do_login()
        expected = (TRANSLATIONS[scc.EMAIL_NOT_CONFIRMED_MESSAGE] if lang == "he"
                    else scc.EMAIL_NOT_CONFIRMED_MESSAGE)
        assert boxes == [("warning", genizah_core.tr("Login Failed"), expected)]
    finally:
        dialog.deleteLater()
