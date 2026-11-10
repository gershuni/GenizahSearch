# -*- coding: utf-8 -*-
"""Desktop Add Comment offers only what is stored; the comment badges say where it applies.

`comments` (supabase_setup.sql) has no category and no is_anonymous column, and
SupabaseCorrectionsClient.create_comment sends neither. The dialog still offered a
five-way Type combo and a "Post anonymously" checkbox: a user who ticked it got
"Comment submitted successfully", and genizahsearch.com then showed their name on
the comment. The comment lists put the stored scope in the Type badge raw ("page",
"manuscript"), in English in the Hebrew UI too.

Runs the real dialogs: CommentDialog.submit_comment through the real Supabase client
(SDK mocked), and the two list dialogs' own load path over rows parsed by the
client's own `_parse_comment`.
"""
from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.gui  # imports PyQt6: gui bucket only

from PyQt6.QtWidgets import QApplication, QCheckBox, QComboBox, QLabel, QMessageBox  # noqa: E402

import genizah_core  # noqa: E402
from desktop.corrections_ui import (  # noqa: E402
    CommentDialog, CommentsViewerDialog, MyCommentsDialog,
)
from desktop.supabase_corrections_client import SupabaseCorrectionsClient  # noqa: E402
from shared.config import Config  # noqa: E402
from shared.genizah_translations import TRANSLATIONS  # noqa: E402
from shared.lists_manager import ListsManager  # noqa: E402

_APP = QApplication.instance() or QApplication([])


@pytest.fixture(autouse=True)
def _personal_state_in_tmp(tmp_path, monkeypatch):
    """Nothing here should write personal state; if it ever does, it lands in tmp_path."""
    monkeypatch.setattr(Config, "SESSION_FILE", str(tmp_path / "session.json"))
    monkeypatch.setattr(Config, "CONFIG_FILE", str(tmp_path / "config.pkl"))
    monkeypatch.setattr(Config, "LANGUAGE_FILE", str(tmp_path / "lang.pkl"))
    monkeypatch.setattr(ListsManager, "LISTS_FILE", str(tmp_path / "lists.pkl"))

_SYS_ID = "990000000000000001"
# Exactly the columns create_comment writes; nothing the dialog offers may be missing.
_STORED = {'author_id', 'sys_id', 'content', 'scope', 'page_number', 'is_public', 'parent_id'}
# The five categories the old Type combo offered. None of them is stored.
_CATEGORIES = ["General Comment", "Question", "Scholarly Note", "Suggestion", "Issue Report"]


def _in(lang, text):
    return TRANSLATIONS.get(text, text) if lang == 'he' else text


def _supabase_client():
    cli = SupabaseCorrectionsClient.__new__(SupabaseCorrectionsClient)
    cli.current_user = MagicMock(_uuid="00000000-0000-0000-0000-000000000001")
    captured = {}

    def _insert(payload):
        captured['payload'] = payload
        row = dict(payload, id=1, created_at="2026-09-25T00:00:00Z")
        return MagicMock(execute=MagicMock(return_value=MagicMock(data=[row])))

    sb = MagicMock()
    sb.table.return_value.insert.side_effect = _insert
    cli._get_client = lambda: sb
    return cli, captured


@pytest.mark.parametrize("lang", ["en", "he"])
def test_add_comment_offers_no_type_or_anonymous_choice(monkeypatch, lang):
    monkeypatch.setattr(genizah_core, 'CURRENT_LANG', lang)
    cli, _ = _supabase_client()
    dlg = CommentDialog(None, cli, document_id=_SYS_ID, shelfmark="T-S 12.123", page_number=2)
    try:
        offered = {combo.itemText(i) for combo in dlg.findChildren(QComboBox)
                   for i in range(combo.count())}
        categories = {_in(lang, c) for c in _CATEGORIES}
        assert not offered & categories, f"a comment category is offered: {offered & categories}"
        assert _in(lang, "Type:") not in {lbl.text() for lbl in dlg.findChildren(QLabel)}
        boxes = [c.text() for c in dlg.findChildren(QCheckBox)]
        assert _in(lang, "Post anonymously") not in boxes, "anonymity is offered but never stored"
        assert _in(lang, "Public comment") in boxes, "the stored is_public choice went too"
    finally:
        dlg.deleteLater()


@pytest.mark.parametrize("public", [True, False], ids=["public", "private"])
@pytest.mark.parametrize("page_number, scope", [(2, 'page'), (None, 'manuscript')])
def test_add_comment_still_posts_and_claims_nothing_unstored(monkeypatch, page_number, scope,
                                                             public):
    """Browse / Manuscript Viewer pass a page; the context menu and Comments viewer do not.

    "Public comment" is the one choice left, and it is stored: unticking it must post
    a private comment, not a public one.
    """
    monkeypatch.setattr(genizah_core, 'CURRENT_LANG', 'en')
    cli, captured = _supabase_client()
    real_create = cli.create_comment
    cli.create_comment = MagicMock(side_effect=real_create)
    dlg = CommentDialog(None, cli, document_id=_SYS_ID, shelfmark="T-S 12.123",
                        page_number=page_number)
    try:
        dlg.content_input.setPlainText("a comment")
        dlg.public_check.setChecked(public)
        with patch.object(QMessageBox, 'information') as ok, \
                patch.object(QMessageBox, 'warning') as err:
            dlg.submit_comment()
        assert ok.called and not err.called, err.call_args
        kwargs = cli.create_comment.call_args.kwargs
        assert 'is_anonymous' not in kwargs and 'comment_type' not in kwargs, kwargs
        payload = captured['payload']
        assert set(payload) == _STORED
        assert payload['scope'] == scope
        assert payload['page_number'] == page_number, "filed under a different page"
        assert payload['sys_id'] == _SYS_ID
        assert payload['content'] == "a comment"
        assert payload['is_public'] is public, "the Public comment box was not honoured"
    finally:
        dlg.deleteLater()


# What the table holds: 'page' / 'manuscript' from the web and from the desktop since
# 2026-05-12; 'general' with or without a page from the desktop before that; and a
# NULL or missing scope. The page number decides, as it does on the website.
_ROWS = [
    {'scope': 'page', 'page_number': 3},
    {'scope': 'manuscript', 'page_number': None},
    {'scope': 'general', 'page_number': 5},
    {'scope': 'general', 'page_number': None},
    {'scope': None, 'page_number': None},
    {'page_number': 7},
]
_BADGES = {
    'en': ["Page 3", "Entire manuscript", "Page 5", "Entire manuscript",
           "Entire manuscript", "Page 7"],
    'he': ["עמוד 3", "כל כתב היד", "עמוד 5", "כל כתב היד",
           "כל כתב היד", "עמוד 7"],
}


def _comments():
    cli = SupabaseCorrectionsClient.__new__(SupabaseCorrectionsClient)
    return [cli._parse_comment(dict(row, id=i, content="c", sys_id=_SYS_ID,
                                    author_id="u", created_at="2026-09-25"))
            for i, row in enumerate(_ROWS, 1)]


@pytest.mark.parametrize("lang", ["en", "he"])
@pytest.mark.parametrize("which", ["document comments", "my comments"])
def test_comment_badge_says_where_the_comment_applies(monkeypatch, lang, which):
    monkeypatch.setattr(genizah_core, 'CURRENT_LANG', lang)
    client = MagicMock()
    client.is_server_available.return_value = True
    client.get_comments_for_document.return_value = _comments()
    client.get_my_comments.return_value = (_comments(), len(_ROWS))
    if which == "document comments":
        dlg = CommentsViewerDialog(None, client, document_id=_SYS_ID, shelfmark="T-S 12.123")
    else:
        dlg = MyCommentsDialog(None, client)
    try:
        texts = {lbl.text() for lbl in dlg.findChildren(QLabel)}
        assert not texts & {'page', 'manuscript', 'general'}, "raw scope value shown"
        badges = [lbl.text() for lbl in dlg.findChildren(QLabel, "comment_scope_badge")]
        assert badges == _BADGES[lang]
    finally:
        dlg.deleteLater()
