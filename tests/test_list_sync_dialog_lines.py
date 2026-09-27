# -*- coding: utf-8 -*-
"""The list-sync dialog tells the user about notes, whatever the result.

A sync can leave a note different from the account (the upload no longer overwrites a
cloud note it cannot prove is older), keep both versions of a note (a Download or Merge),
combine an entry's tags (a Download or Merge), or be unable to update a note too long for
the request (the gateway refuses it). Each count -- of entries, not of the lists they are
in -- gets one line under the dialog's message -- on success AND on failure, for all
three buttons: an upload in which one item failed and one note was too long must still
say so. A result without these counts shows exactly today's message.

D1 hands GenizahGUI._on_lists_sync_done -- where the dialog's buttons' jobs end, on the
UI thread -- the results the list-sync runner gives it, with the message box faked out
(tests/test_personal_state_atomic_writes.py drives the same buttons through the runner).
D2 checks the five new strings have Hebrew that starts
with a Hebrew letter, and that the Hebrew of the kept-both line says what the English
says. D3 checks Help.html no longer claims that list sync is "disabled
entirely if every item in a list is local" and says what is true instead.
"""
import re
import types
from pathlib import Path

import pytest

import genizah_core
from shared import lists_sync
from shared.genizah_translations import TRANSLATIONS

ROOT = Path(__file__).resolve().parents[1]

FROM_THE_CLOUD = "from the cloud"
DIFFERING = ("Notes that differ between this computer and your account: {}. "
             "They were left as they are; Merge Both keeps both versions.")
MERGED = ("Notes that differed between this computer and your account: {}. "
          "Both versions were kept; the account's text is under the line \"--- {} ---\".")
TOO_LONG = ("Notes too long to update safely in your account: {}. "
            "They were not changed there and are kept on this computer.")
TAGS = "Tags that differed between this computer and your account: {}. The tags from both were kept."
# The list sync off the UI thread: the Skip tooltip, progress, cancel and status lines,
# the preview dialog's rows, the stop message, and the too-long line while saves fail.
TOO_LONG_NOT_SAVED = "Notes too long to update safely in your account: {}. They were not changed there."
SYNC_KEYS = (
    "Don't download from your account now. Uploads continue: until you sign out or close the "
    "program, your lists are uploaded to your account after each change.",
    "Checking the lists in your account...",
    "Waiting for the list sync that is already running...",
    "Downloading list {} of {}...",
    "Uploading list {} of {}...",
    "List sync cancelled. Nothing was changed on this computer.",
    "Upload stopped. The rest of your changes are saved on this computer but have not reached "
    "your account yet.",
    "The cloud lists were downloaded, but the upload was stopped. The rest of your changes are "
    "saved on this computer but have not reached your account yet.",
    "Some notes differ from your account and were not uploaded. To keep both versions, use Sync "
    "lists now, then Merge Both.",
    "Sync stopped",
    "{} ({} items)",
    "... and {} more",
    TOO_LONG_NOT_SAVED,
    # Sign-in at startup, sign-out
    "Could not check the lists in your account: {}. List sync stays on; Sync lists now tries again.",
    "Signing out...",
    "These lists were not uploaded before you signed out: {}. Your changes to them are saved on "
    "this computer and are uploaded after you next sign in.",
    "Your latest list changes were not uploaded before you signed out. They are saved on this "
    "computer and are uploaded after you next sign in.",
    "List sync was not on in this session, so your list changes were not uploaded. They are saved "
    "on this computer and are uploaded after you next sign in.",
    "Some notes differ from your account and were not uploaded. They are kept on this computer; "
    "after you next sign in, use Sync lists now, then Merge Both, to keep both versions.",
    "Your lists cannot be saved on this computer at the moment ({} cannot be written). List changes "
    "that did not reach your account before you signed out are lost when you close the program.",
    # Sync lists now, and the website-removal prompt
    "Sync lists now",
    "Download, upload or merge your lists with your account on genizahsearch.com",
    "Sign in to sync your lists with your account",
    "Entries removed on the website",
    "These entries were removed from your lists on genizahsearch.com. They are still on this "
    "computer, and until you choose they stay here and are not uploaded again.",
    "Decide later",
    "Remove from this computer too",
    "Keep it (and add it back on the website)",
    "Remove all from this computer too",
    "Keep all (and add them back on the website)",
    "Choice",
    "Removed from this computer: {}. To be added back on the website: {}.",
)


def test_the_button_names_in_the_notes_lines_are_the_buttons_labels():
    """"Sync lists now" and "Merge Both" in the hints are the two buttons' own labels,
    in both languages."""
    for key in SYNC_KEYS:
        if "Sync lists now" in key and key != "Sync lists now":
            assert "'" + TRANSLATIONS["Sync lists now"] + "'" in TRANSLATIONS[key], key
        if "Merge Both" in key:
            assert "'" + TRANSLATIONS["Merge Both"] + "'" in TRANSLATIONS[key], key
NEW_KEYS = (FROM_THE_CLOUD, DIFFERING, MERGED, TOO_LONG, TAGS) + SYNC_KEYS

DOWNLOADED = "Downloaded {lists} lists and {items} items from cloud."
UPLOADED = "Uploaded {lists} lists and {items} items to cloud."
MERGED_OK = "Lists merged successfully! Downloaded {dl} lists, uploaded {ul} lists."
UPLOAD_AFTER_DOWNLOAD_FAILED = "The cloud lists were downloaded, but the upload failed: {}"


def _is_hebrew(ch):
    return "֐" <= ch <= "׿"


def _first_letter(text):
    """The first letter of `text`, skipping {} placeholders, digits and punctuation."""
    for ch in re.sub(r"\{[^}]*\}", "", text):
        if ch.isalpha():
            return ch
    return ""


@pytest.fixture
def genizah_app():
    import genizah_app
    return genizah_app


@pytest.fixture(params=["en", "he"])
def lang(request, monkeypatch):
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", request.param)
    return request.param


def _tr(text):
    return genizah_core.tr(text)


class _Progress:
    def __init__(self):
        self.canceled = types.SimpleNamespace(connect=lambda slot: None, disconnect=lambda *a: None)
        self.closed = False

    def close(self):
        self.closed = True

    def __getattr__(self, name):
        return lambda *args: None


def _run(genizah_app, monkeypatch, action, download=None, upload=None):
    """What the dialog shows when one of its buttons' jobs ends.

    The runner hands GenizahGUI._on_lists_sync_done each half's result: a Merge's
    upload half only after a download that succeeded. Returns the message boxes shown
    and the halves the job ran."""
    shown = []
    monkeypatch.setattr(genizah_app, "_show_ok_notice",
                        lambda parent, kind, title, text: shown.append((kind, text)))
    host = genizah_app.GenizahGUI.__new__(genizah_app.GenizahGUI)
    host.lists_mgr = types.SimpleNamespace(saves_failing=lambda: False, LISTS_FILE="lists.pkl")
    host._app_shutting_down = False
    host.lists_tree = None
    host.lists_refresh_all = lambda: None
    host._offer_web_removals = lambda manual=False: None
    outcome, calls = {"cancelled": False}, []
    if action in ("download", "merge"):
        calls.append("download")
        outcome["download"] = dict(download)
    if action == "upload" or (action == "merge" and download.get("success")):
        calls.append("upload")
        outcome["upload"] = dict(upload)
    progress = _Progress()
    genizah_app.GenizahGUI._on_lists_sync_done(host, action, outcome, progress)
    assert progress.closed
    return shown, calls


def _merged_line(n):
    return _tr(MERGED).format(n, _tr(FROM_THE_CLOUD))


def _differing_line(n):
    return _tr(DIFFERING).format(n)


def _too_long_line(n):
    return _tr(TOO_LONG).format(n)


def _tags_line(n):
    return _tr(TAGS).format(n)


def _message(first, *lines):
    return "\n\n".join([first, *lines])


def _check_language(text, lang):
    """In Hebrew every paragraph of the message starts with a Hebrew letter."""
    if lang == "he":
        for paragraph in text.split("\n\n"):
            assert _is_hebrew(_first_letter(paragraph)), f"not in Hebrew: {paragraph!r}"
        assert FROM_THE_CLOUD not in text, "the marker's label is in English"


PARTIAL = {"success": False, "error": lists_sync.UPLOAD_PARTLY_FAILED.format(3, 1),
           "items_pushed": 3, "items_failed": 1, "notes_too_long": 1, "notes_differing": 1}


# ---------------------------------------------------------------------------
# D1 -- the lines under every result message
# ---------------------------------------------------------------------------

def test_download_success_reports_the_notes_it_kept_both_of(genizah_app, monkeypatch, lang):
    shown, _ = _run(genizah_app, monkeypatch, "download",
                    download={"success": True, "lists_added": 1, "items_added": 4, "notes_merged": 2})
    assert shown == [("information", _message(
        _tr(DOWNLOADED).format(lists=1, items=4), _merged_line(2)))]
    assert "2" in shown[0][1].split("\n\n")[1]
    assert f"--- {_tr(FROM_THE_CLOUD)} ---" in shown[0][1]
    _check_language(shown[0][1], lang)


def test_a_download_that_combined_tags_says_so_on_a_line_of_its_own(genizah_app, monkeypatch, lang):
    # Combined tags leave no marker line in any note, so they are not counted as kept notes.
    shown, _ = _run(genizah_app, monkeypatch, "download",
                    download={"success": True, "lists_added": 0, "items_added": 0, "tags_merged": 3})
    assert shown == [("information", _message(_tr(DOWNLOADED).format(lists=0, items=0), _tags_line(3)))]
    assert f"--- {_tr(FROM_THE_CLOUD)} ---" not in shown[0][1]
    _check_language(shown[0][1], lang)

    shown, _ = _run(genizah_app, monkeypatch, "download",
                    download={"success": True, "lists_added": 0, "items_added": 0,
                              "notes_merged": 1, "tags_merged": 2})
    assert shown == [("information", _message(
        _tr(DOWNLOADED).format(lists=0, items=0), _merged_line(1), _tags_line(2)))]


def test_download_failure_shows_the_error_and_any_note_line(genizah_app, monkeypatch, lang):
    shown, _ = _run(genizah_app, monkeypatch, "download",
                    download={"success": False, "error": "Sync not available"})
    assert shown == [("warning", _tr("Sync not available"))]

    shown, _ = _run(genizah_app, monkeypatch, "download",
                    download={"success": False, "error": "Sync not available", "notes_merged": 1})
    assert shown == [("warning", _message(_tr("Sync not available"), _merged_line(1)))]
    _check_language(shown[0][1], lang)


def test_upload_success_reports_differing_and_too_long_notes(genizah_app, monkeypatch, lang):
    shown, _ = _run(genizah_app, monkeypatch, "upload",
                    upload={"success": True, "lists_pushed": 2, "items_pushed": 5,
                            "notes_differing": 3, "notes_too_long": 1})
    assert shown == [("information", _message(
        _tr(UPLOADED).format(lists=2, items=5), _differing_line(3), _too_long_line(1)))]
    _check_language(shown[0][1], lang)


def test_a_partly_failed_upload_still_reports_its_notes(genizah_app, monkeypatch, lang):
    shown, _ = _run(genizah_app, monkeypatch, "upload", upload=PARTIAL)
    assert shown == [("warning", _message(
        _tr(lists_sync.UPLOAD_PARTLY_FAILED).format(3, 1), _differing_line(1), _too_long_line(1)))]
    _check_language(shown[0][1], lang)


def test_merge_success_reports_both_halves(genizah_app, monkeypatch, lang):
    shown, calls = _run(genizah_app, monkeypatch, "merge",
                        download={"success": True, "lists_added": 1, "notes_merged": 2, "tags_merged": 1},
                        upload={"success": True, "lists_pushed": 3,
                                "notes_differing": 1, "notes_too_long": 1})
    assert calls == ["download", "upload"]
    assert shown == [("information", _message(
        _tr(MERGED_OK).format(dl=1, ul=3), _merged_line(2), _tags_line(1), _differing_line(1),
        _too_long_line(1)))]
    _check_language(shown[0][1], lang)


def test_a_merge_whose_upload_fails_reports_both_halves(genizah_app, monkeypatch, lang):
    shown, calls = _run(genizah_app, monkeypatch, "merge",
                        download={"success": True, "lists_added": 0, "notes_merged": 2},
                        upload=PARTIAL)
    assert calls == ["download", "upload"]
    first = _tr(UPLOAD_AFTER_DOWNLOAD_FAILED).format(_tr(lists_sync.UPLOAD_PARTLY_FAILED).format(3, 1))
    assert shown == [("warning", _message(
        first, _merged_line(2), _differing_line(1), _too_long_line(1)))]
    _check_language(shown[0][1], lang)


def test_a_merge_whose_download_fails_never_uploads(
        genizah_app, monkeypatch, lang):
    shown, calls = _run(genizah_app, monkeypatch, "merge",
                        download={"success": False, "error": "Sync already in progress"},
                        upload={"success": True, "notes_differing": 5})
    assert calls == ["download"]
    assert shown == [("warning", _tr("Sync already in progress"))]

    # A failed download reports no counts today; if one ever does, its line is shown too.
    shown, calls = _run(genizah_app, monkeypatch, "merge",
                        download={"success": False, "error": "Sync not available", "notes_merged": 1},
                        upload={"success": True, "notes_differing": 5})
    assert calls == ["download"]
    assert shown == [("warning", _message(_tr("Sync not available"), _merged_line(1)))]
    _check_language(shown[0][1], lang)


@pytest.mark.parametrize("action,download,upload,expected", [
    ("download", {"success": True, "lists_added": 1, "items_added": 2},
     None, ("information", (DOWNLOADED, {"lists": 1, "items": 2}))),
    ("upload", None, {"success": True, "lists_pushed": 1, "items_pushed": 2},
     ("information", (UPLOADED, {"lists": 1, "items": 2}))),
    ("merge", {"success": True, "lists_added": 1}, {"success": True, "lists_pushed": 2},
     ("information", (MERGED_OK, {"dl": 1, "ul": 2}))),
    ("upload", None, {"success": True, "lists_pushed": 1, "items_pushed": 2,
                      "notes_differing": 0, "notes_too_long": 0},
     ("information", (UPLOADED, {"lists": 1, "items": 2}))),
    ("download", {"success": True, "lists_added": 1, "items_added": 2, "notes_merged": 0},
     None, ("information", (DOWNLOADED, {"lists": 1, "items": 2}))),
], ids=["download-no-keys", "upload-no-keys", "merge-no-keys", "upload-zero-counts",
        "download-zero-count"])
def test_a_result_without_note_counts_shows_todays_message(
        genizah_app, monkeypatch, lang, action, download, upload, expected):
    shown, _ = _run(genizah_app, monkeypatch, action, download=download, upload=upload)
    kind, (template, values) = expected
    assert shown == [(kind, _tr(template).format(**values))]


def test_the_note_lines_come_from_one_helper_that_ignores_missing_keys(genizah_app, lang):
    lines = genizah_app.GenizahGUI._sync_note_lines
    assert lines() == []
    assert lines(download={}, upload={}) == []
    assert lines(download={"notes_merged": None, "tags_merged": None}, upload={"notes_differing": None}) == []
    # A download result's upload-side keys are not read, and the reverse.
    assert lines(download={"notes_differing": 1, "notes_too_long": 1}) == []
    assert lines(upload={"notes_merged": 1, "tags_merged": 1}) == []
    assert lines(download={"notes_merged": 1, "tags_merged": 4},
                 upload={"notes_differing": 2, "notes_too_long": 3}) == [
        _merged_line(1), _tags_line(4), _differing_line(2), _too_long_line(3)]


# ---------------------------------------------------------------------------
# D2 -- the new strings
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("key", NEW_KEYS)
def test_every_new_sync_string_has_hebrew_starting_with_a_hebrew_letter(key):
    hebrew = TRANSLATIONS.get(key)
    assert hebrew and hebrew != key, f"no Hebrew for {key!r}"
    assert _is_hebrew(_first_letter(hebrew)), f"the Hebrew does not start with a Hebrew letter: {hebrew!r}"
    assert hebrew.count("{}") == key.count("{}"), "placeholder count differs"
    hebrew.format(*range(key.count("{}")))  # formats without error


def test_the_hebrew_of_the_kept_both_line_says_the_notes_differed():
    # The English says the notes differed between this computer and the account (whichever
    # side changed); the Hebrew once said they were changed on both sides.
    hebrew = TRANSLATIONS[MERGED]
    assert hebrew.startswith("הערות שהיו שונות בין המחשב הזה לחשבונך: {}."), hebrew
    assert "גם במחשב הזה וגם בחשבונך" not in hebrew
    assert "שתי הגרסאות נשמרו" in hebrew and '"--- {} ---"' in hebrew


def test_the_first_letter_check_can_fail():
    assert not _is_hebrew(_first_letter("{}: Notes"))
    assert _is_hebrew(_first_letter("{}: הערות"))
    assert _is_hebrew(_first_letter("--- מהענן ---"))


# ---------------------------------------------------------------------------
# D3 -- Help.html
# ---------------------------------------------------------------------------

HELP_EN = ("Lists cloud sync never uploads local items: an entry for one of your own "
           "documents stays on this computer.")
HELP_HE = "סנכרון הרשימות לענן לעולם אינו מעלה פריטים מקומיים: פריט של מסמך משלכם נשאר במחשב הזה."


def _help_items():
    html = (ROOT / "Help.html").read_text(encoding="utf-8")
    return [re.sub(r"\s+", " ", li).strip() for li in re.findall(r"<li>(.*?)</li>", html, re.S)]


@pytest.mark.parametrize("text,old", [
    (HELP_EN, "disabled entirely if every item in a list is local"),
    (HELP_HE, "מבוטל לחלוטין אם כל פריט ברשימה הוא מקומי"),
], ids=["en", "he"])
def test_help_says_local_entries_are_never_uploaded(text, old):
    items = _help_items()
    assert items.count(text) == 1, "the Help page does not say it (once, as a list item)"
    assert not any(old in item for item in items), "the Help page still makes the old claim"
