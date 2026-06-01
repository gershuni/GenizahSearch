"""Tests for desktop/file_actions.py (v7.16 LOCAL file actions).

Pure unit tests — no Qt — so they run on CI (the desktop QThread tests are
collect-ignored on CI per conftest, but these are not). The key regression these
pin: the open-file extension gate must accept the FULL supported set
(.docx/.pdf/.txt/.html/.xlsx/.csv), not the stale {.docx,.pdf,.txt} literal that
shipped in v7.15 and silently refused .html/.xlsx/.csv LOCAL hits.
"""
import os

import pytest

from desktop import file_actions
from shared.local_indexer import _SUPPORTED_EXTENSIONS


def _make(tmp_path, name):
    p = tmp_path / name
    p.write_text("x", encoding="utf-8")
    return str(p)


@pytest.mark.parametrize("ext", sorted(_SUPPORTED_EXTENSIONS))
def test_is_openable_accepts_every_supported_extension(tmp_path, ext):
    """Regression: the gate must accept the full supported set, incl. html/xlsx/csv."""
    fp = _make(tmp_path, f"doc{ext}")
    assert file_actions.is_openable_local_file(fp) is True


@pytest.mark.parametrize("ext", [".html", ".xlsx", ".csv"])
def test_is_openable_accepts_v715_newly_indexed_types(tmp_path, ext):
    """These three were silently refused by the v7.15 stale gate — the bug."""
    assert file_actions.is_openable_local_file(_make(tmp_path, f"doc{ext}")) is True


def test_is_openable_rejects_disallowed_extension(tmp_path):
    assert file_actions.is_openable_local_file(_make(tmp_path, "evil.exe")) is False


def test_is_openable_rejects_directory(tmp_path):
    """The open gate uses isfile() — a directory is never 'openable'."""
    d = tmp_path / "folder.pdf"  # a directory that happens to look like a pdf name
    d.mkdir()
    assert file_actions.is_openable_local_file(str(d)) is False


def test_is_openable_rejects_missing_and_empty(tmp_path):
    assert file_actions.is_openable_local_file(None) is False
    assert file_actions.is_openable_local_file("") is False
    assert file_actions.is_openable_local_file(str(tmp_path / "nope.pdf")) is False


def test_open_local_file_launches_supported(tmp_path, monkeypatch):
    fp = _make(tmp_path, "ok.html")
    calls = []
    monkeypatch.setattr(os, "startfile", lambda p: calls.append(p), raising=False)
    assert file_actions.open_local_file(fp) is True
    assert calls == [fp]


def test_open_local_file_refuses_disallowed(tmp_path, monkeypatch):
    fp = _make(tmp_path, "bad.exe")
    calls = []
    monkeypatch.setattr(os, "startfile", lambda p: calls.append(p), raising=False)
    assert file_actions.open_local_file(fp) is False
    assert calls == []  # os.startfile never called for a refused extension


def test_reveal_local_file_issues_explorer_select(tmp_path, monkeypatch):
    fp = _make(tmp_path, "doc.pdf")
    captured = {}
    monkeypatch.setattr(
        file_actions.subprocess, "Popen", lambda args: captured.setdefault("args", args)
    )
    assert file_actions.reveal_local_file(fp) is True
    # `/select,` and the path MUST be separate argv entries (the combined
    # `/select,<path>` form breaks for paths with spaces — see reveal_local_file).
    assert captured["args"][0] == "explorer"
    assert captured["args"][1] == "/select,"
    assert captured["args"][2] == os.path.normpath(fp)


def test_reveal_local_file_path_with_spaces_kept_separate(tmp_path, monkeypatch):
    """Regression (v7.16): a path with spaces must stay a SEPARATE argv entry.

    The combined `/select,<path>` form caused subprocess to quote the whole
    token, which Explorer parsed as no-selection and opened 'My Documents'.
    """
    sub = tmp_path / "My Folder With Spaces"
    sub.mkdir()
    fp = str(sub / "the file.pdf")
    (sub / "the file.pdf").write_text("x", encoding="utf-8")
    captured = {}
    monkeypatch.setattr(
        file_actions.subprocess, "Popen", lambda args: captured.setdefault("args", args)
    )
    assert file_actions.reveal_local_file(fp) is True
    assert captured["args"][1] == "/select,"
    assert captured["args"][2] == os.path.normpath(fp)  # full path, NOT combined into args[1]
    assert " " in captured["args"][2]  # the spaces survive as one separate arg


def test_reveal_local_file_not_gated_on_extension(tmp_path, monkeypatch):
    """reveal only opens a folder, so it works even for a non-openable extension."""
    fp = _make(tmp_path, "thing.exe")
    captured = {}
    monkeypatch.setattr(
        file_actions.subprocess, "Popen", lambda args: captured.setdefault("args", args)
    )
    assert file_actions.reveal_local_file(fp) is True
    assert captured["args"][2] == os.path.normpath(fp)


def test_reveal_local_file_missing_returns_false(tmp_path, monkeypatch):
    monkeypatch.setattr(
        file_actions.subprocess, "Popen", lambda args: pytest.fail("must not Popen")
    )
    assert file_actions.reveal_local_file(str(tmp_path / "ghost.pdf")) is False
    assert file_actions.reveal_local_file(None) is False


def test_copy_file_location(tmp_path):
    fp = _make(tmp_path, "doc.pdf")

    class FakeClipboard:
        def __init__(self):
            self.text = None

        def setText(self, t):  # noqa: N802 — mirrors Qt API
            self.text = t

    cb = FakeClipboard()
    assert file_actions.copy_file_location(fp, cb) is True
    assert cb.text == os.path.normpath(fp)


def test_copy_file_location_empty_returns_false():
    class FakeClipboard:
        def setText(self, t):  # noqa: N802
            raise AssertionError("must not copy when there is nothing to copy")

    assert file_actions.copy_file_location(None, FakeClipboard()) is False
    assert file_actions.copy_file_location("", FakeClipboard()) is False
