# -*- coding: utf-8 -*-
"""The one folder-removal function of scripts/run_local_tests.py and its pytest plugin.

Both remove test files: the runner whole run and chunk folders, the plugin each passing
test's tmp_path. Plain `shutil.rmtree(..., ignore_errors=True)` leaves two things behind
on Windows, both measured in a run's folder 2026-11-11: a path over 260 characters (a test
of long Hebrew paths makes one) and a read-only file (a test's git objects). In the runner
that left run folders no later run could remove; in the plugin it left a passing test's
folder inside a kept failing chunk (Codex on PR #7). So both use this module."""
from __future__ import annotations

import os
import shutil
import stat
import sys


def rmtree(path):
    """shutil.rmtree, best effort, that also removes what Windows otherwise keeps.

    The read-only retry is Windows only: there a read-only file cannot be deleted, and
    chmod only clears that flag. On POSIX deleting needs the folder's permission, not the
    file's, and chmod follows a symlink: it would strip read access from whatever file a
    test's symlink points at."""
    path = os.fspath(path)
    if os.name == "nt":
        path = extended_length(os.path.abspath(path))

    def make_writable_and_retry(func, failed, _exc):
        if os.name != "nt":
            return                                  # best effort, like ignore_errors
        try:
            os.chmod(failed, stat.S_IWRITE)
            func(failed)
        except OSError:
            pass

    if sys.version_info >= (3, 12):
        shutil.rmtree(path, onexc=make_writable_and_retry)
    else:
        shutil.rmtree(path, onerror=make_writable_and_retry)


def extended_length(path):
    """Windows' extended-length form of an absolute *path*, which has no 260-character limit.

    `C:\\x` -> `\\\\?\\C:\\x`; a UNC path `\\\\server\\share\\x` -> `\\\\?\\UNC\\server\\share\\x`
    (prefixing a UNC path as it is names no file at all); a path already in that form, or
    a device path (`\\\\.\\`), is returned as it is."""
    if path.startswith(("\\\\?\\", "\\\\.\\")):
        return path
    if path.startswith("\\\\"):
        return "\\\\?\\UNC\\" + path[2:]
    return "\\\\?\\" + path
