# -*- coding: utf-8 -*-
"""Desktop accounts with no username, and sign-up under email confirmation.

Since 2026-10-21 `profiles.username` is what the person typed at an email
sign-up, else empty (every Google sign-up, and the accounts whose username was
their email address), and production requires email confirmation. Two desktop
defects followed (docs/archive/OPEN_ISSUES_ARCHIVE.md, 2026-10-21):

1. A correction's author was recognised by username, so an author with no
   username never saw their own draft / pending / rejected corrections. The
   Supabase client also never filled `Correction.author_id`, so every author
   without a username shared one group and only the newest of them survived.
2. Registration upserted `profiles` with no session (RLS refuses it) and then
   said "You can now login", though the first sign-in can only fail until the
   confirmation link is opened.

Qt-free: the client and the helpers. The dialogs and the two correction lists
(ResultDialog and the Browse tab) are driven in
tests/test_desktop_accounts_without_username_qt.py.
"""
from types import SimpleNamespace

import pytest

from desktop import supabase_corrections_client as scc
from desktop.corrections_client import (
    User as RestUser, Correction as RestCorrection,
    account_display_name, correction_author_key, is_own_correction,
)
from shared.genizah_translations import TRANSLATIONS

ME = "6f1c1e2a-0000-4000-8000-000000000001"
OTHER = "6f1c1e2a-0000-4000-8000-000000000002"


def _row(cid, author_id, username, status="draft", created_at="2026-10-21T09:00:00+00:00"):
    """A `corrections` row as get_corrections_for_document hands it to
    _parse_correction: the author's profile attached under 'profiles'."""
    return {"id": cid, "sys_id": "990000000000000001", "page_number": 1,
            "original_text": "a", "corrected_text": "b", "status": status,
            "author_id": author_id, "created_at": created_at,
            "profiles": {"id": author_id, "username": username, "full_name": None}}


def _parse(row):
    return scc.SupabaseCorrectionsClient._parse_correction(None, row)


def _user(username=None, uuid=ME, full_name=None, role="user"):
    return scc.User(id=hash(uuid) % (10**9), email="reader@example.org", username=username,
                    full_name=full_name, role=role, _uuid=uuid)


# ---------------------------------------------------------------------------
# Recognising the author
# ---------------------------------------------------------------------------

def test_a_parsed_correction_carries_its_author_id():
    assert _parse(_row(1, ME, None)).author_id == ME


def test_own_correction_is_recognised_with_no_username():
    assert is_own_correction(_parse(_row(1, ME, None)), _user(username=None))


def test_own_correction_is_recognised_with_a_username():
    assert is_own_correction(_parse(_row(1, ME, "hillel")), _user(username="hillel"))


def test_a_sign_in_cached_before_2026_10_21_still_recognises_its_corrections():
    """The desktop keeps the signed-in user in supabase_credentials.json; one
    saved before the change still holds the email address as its username,
    while the profile now has none (or the typed one back)."""
    cached = _user(username="reader@example.org")
    old = "2026-09-30T12:00:00+00:00"
    assert is_own_correction(_parse(_row(1, ME, None, created_at=old)), cached)
    assert is_own_correction(_parse(_row(2, ME, "reader", created_at=old)), cached)


def test_someone_elses_correction_is_never_own_when_neither_has_a_username():
    assert not is_own_correction(_parse(_row(1, OTHER, None)), _user(username=None))


def test_no_signed_in_user_or_no_ids_is_never_own():
    corr = _parse(_row(1, ME, None))
    assert not is_own_correction(corr, None)
    assert not is_own_correction(_parse(_row(1, None, None)), _user())
    assert not is_own_correction(corr, _user(uuid=None))


def test_the_rest_clients_integer_ids_still_match():
    user = RestUser(id=7, email="r@example.org", username="")
    assert is_own_correction(RestCorrection(id=1, document_id="d", original_text="a", corrected_text="b",
                                            status="draft", author_id=7), user)
    assert not is_own_correction(RestCorrection(id=2, document_id="d", original_text="a", corrected_text="b",
                                                status="draft", author_id=8), user)


def test_authors_without_a_username_do_not_share_a_group():
    mine, theirs = _parse(_row(1, ME, None)), _parse(_row(2, OTHER, None))
    assert correction_author_key(mine) != correction_author_key(theirs)
    assert correction_author_key(mine) == correction_author_key(_parse(_row(3, ME, None)))


def test_account_display_name_never_shows_none():
    assert account_display_name(_user(username="hillel", full_name="Hillel G")) == "hillel"
    assert account_display_name(_user(username=None, full_name="Hillel G")) == "Hillel G"
    assert account_display_name(_user(username=None, full_name=None)) == ""
    assert account_display_name(None) == ""


# ---------------------------------------------------------------------------
# Registration and sign-in under email confirmation
# ---------------------------------------------------------------------------

class _Table:
    def __init__(self, calls, name):
        self._calls, self._name = calls, name

    def upsert(self, data):
        self._calls.append((self._name, "upsert", data))
        return self

    def execute(self):
        return SimpleNamespace(data=[])


class _Auth:
    def __init__(self, session, error=None):
        self._session, self._error = session, error
        self.sign_ups = []

    def sign_up(self, credentials):
        self.sign_ups.append(credentials)
        return SimpleNamespace(user=SimpleNamespace(id=ME), session=self._session)

    def sign_in_with_password(self, credentials):
        raise self._error


class _Supabase:
    def __init__(self, session=None, error=None):
        self.auth = _Auth(session, error)
        self.tables = []

    def table(self, name):
        return _Table(self.tables, name)


@pytest.fixture
def account(tmp_path):
    def make(**kw):
        client = scc.SupabaseCorrectionsClient(config_path=tmp_path / "corrections")
        client._client = _Supabase(**kw)
        return client
    return make


def _register(client):
    return client.register(email="new@example.org", username="newreader", password="Secret123",
                           full_name="New Reader", affiliation="Somewhere")


def test_registration_without_a_session_writes_no_profile_and_says_check_email(account):
    client = account(session=None)
    assert _register(client) == (True, scc.CHECK_EMAIL_MESSAGE)
    assert client._client.tables == []


def test_registration_still_sends_what_was_typed_for_the_new_user_trigger(account):
    client = account(session=None)
    _register(client)
    (sent,) = client._client.auth.sign_ups
    assert sent["options"]["data"] == {"username": "newreader", "full_name": "New Reader",
                                       "affiliation": "Somewhere"}


def test_registration_with_a_session_keeps_writing_the_profile(account):
    client = account(session=SimpleNamespace(access_token="a", refresh_token="r"))
    assert _register(client) == (True, "Registration successful")
    assert [(t, op) for t, op, _ in client._client.tables] == [("profiles", "upsert")]


@pytest.mark.parametrize("error", [
    scc.AuthApiError("Email not confirmed", 400, "email_not_confirmed"),
    scc.AuthApiError("Email not confirmed", 400, None),
], ids=["by-code", "by-message"])
def test_an_unconfirmed_sign_in_gets_the_approved_message(account, error):
    assert account(error=error).login("new@example.org", "Secret123") == (
        False, scc.EMAIL_NOT_CONFIRMED_MESSAGE)


def test_the_messages_are_the_websites_approved_keys():
    """The dialogs translate these; a key that drifted from the table would
    show English to a Hebrew reader."""
    assert scc.CHECK_EMAIL_MESSAGE in TRANSLATIONS
    assert scc.EMAIL_NOT_CONFIRMED_MESSAGE in TRANSLATIONS
    assert "{email}" in TRANSLATIONS[scc.CHECK_EMAIL_MESSAGE]
