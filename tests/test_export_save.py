# -*- coding: utf-8 -*-
"""`desktop.export_save.write_export`: saving an export over a file that
Word or Excel holds open (owner, 2026-10-17). Qt-free: the question and the
Save As dialog are passed in."""
from __future__ import annotations

import os

import pytest

from desktop.export_save import (CANCEL, RENAME, RETRY, free_name,
                                 write_export, write_text)


class _Writer:
    """Writes `path`; refuses the first `locked_for` attempts on `locked`."""

    def __init__(self, locked, locked_for=1):
        self.locked, self.left = locked, locked_for
        self.calls = []

    def __call__(self, path):
        self.calls.append(path)
        if path == self.locked and self.left:
            self.left -= 1
            raise PermissionError(13, "Permission denied", path)
        write_text(path, "export")


def _never(*_a):
    pytest.fail("asked although the write succeeded")


def test_a_free_file_is_written_without_asking(tmp_path):
    out = str(tmp_path / "r.docx")
    assert write_export(out, _Writer(None), _never, _never) == out
    assert os.path.exists(out)


def test_try_again_writes_the_same_file_once_it_is_closed(tmp_path):
    out = str(tmp_path / "r.docx")
    w = _Writer(out)
    asked = []
    assert write_export(out, w, lambda p: asked.append(p) or RETRY, _never) == out
    assert asked == [out] and w.calls == [out, out]
    assert os.path.exists(out)


def test_save_under_another_name_writes_there(tmp_path):
    out, other = str(tmp_path / "r.docx"), str(tmp_path / "r (2).docx")
    w = _Writer(out, locked_for=99)
    assert write_export(out, w, lambda p: RENAME, lambda p: other) == other
    assert os.path.exists(other) and not os.path.exists(out)


def test_cancel_writes_nothing_and_says_so(tmp_path):
    out = str(tmp_path / "r.docx")
    w = _Writer(out, locked_for=99)
    assert write_export(out, w, lambda p: CANCEL, _never) is None
    assert w.calls == [out] and not os.path.exists(out)


def test_closing_the_save_as_dialog_cancels(tmp_path):
    out = str(tmp_path / "r.docx")
    w = _Writer(out, locked_for=99)
    assert write_export(out, w, lambda p: RENAME, lambda p: None) is None
    assert w.calls == [out]


def test_a_new_name_that_is_also_locked_asks_again(tmp_path):
    out, other = str(tmp_path / "a.docx"), str(tmp_path / "b.docx")
    answers = iter([RENAME, RETRY])
    calls = []

    def write(path):
        calls.append(path)
        if len(calls) < 3:
            raise PermissionError(13, "Permission denied", path)
        write_text(path, "x")

    assert write_export(out, write, lambda p: next(answers), lambda p: other) == other
    assert calls == [out, other, other]


def test_any_other_error_still_reaches_the_caller(tmp_path):
    def write(path):
        raise OSError(28, "No space left on device")

    with pytest.raises(OSError) as e:
        write_export(str(tmp_path / "r.docx"), write, _never, _never)
    assert not isinstance(e.value, PermissionError)


def test_the_suggested_name_is_the_first_one_not_taken(tmp_path):
    out = tmp_path / "עמידה.docx"
    assert free_name(str(out)) == str(tmp_path / "עמידה (2).docx")
    (tmp_path / "עמידה (2).docx").write_text("x")
    assert free_name(str(out)) == str(tmp_path / "עמידה (3).docx")
