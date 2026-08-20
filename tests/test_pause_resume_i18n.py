"""Bilingual guard for the Pause/Resume strings.

Scoped to this feature's keys, following the precedent in
test_join_workbench_i18n.py: genizah_app.py carries hundreds of pre-existing
tr() strings, so a full-file scan is not the contract here.

PHASE_107_HOST_KEYS is deliberately NOT extended — its name scopes it to that
phase's additions.
"""

import ast
import pathlib

import pytest

from genizah_translations import TRANSLATIONS

HOST = pathlib.Path(__file__).parent.parent / "genizah_app.py"

PAUSE_KEYS = [
    "Pause",
    "Resume",
    "Pausing...",
    "Paused",
    "Searching My Library...",
    "Pause — the search stops at the next checkpoint and keeps what it found",
    "Resume the search from where it paused",
    "Waiting for the search to reach a checkpoint...",
    "Still stopping the previous search — try again in a moment.",
]


def _extract_tr_keys(source: str) -> list:
    """[(key, lineno), ...] for every tr("...") literal call in source."""
    tree = ast.parse(source)
    keys = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "tr"
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            keys.append((node.args[0].value, node.lineno))
    return keys


@pytest.mark.parametrize("key", PAUSE_KEYS)
def test_key_has_a_hebrew_translation(key):
    assert key in TRANSLATIONS, "missing Hebrew for %r" % key


@pytest.mark.parametrize("key", PAUSE_KEYS)
def test_translation_is_not_an_untranslated_stub(key):
    """tr() falls back to the English key on a miss, so a copy-pasted or empty
    value fails silently at runtime. This is the check that catches it."""
    value = TRANSLATIONS[key]
    assert value, "empty translation for %r" % key
    assert value != key, "translation for %r is still the English string" % key
    assert any('֐' <= ch <= '׿' for ch in value), (
        "translation for %r contains no Hebrew characters: %r" % (key, value))


@pytest.mark.parametrize("key", PAUSE_KEYS)
def test_key_is_actually_used_in_the_app(key):
    """Guards the other direction: a key nothing calls is dead weight, and a
    renamed string would leave the old entry behind."""
    used = {k for k, _ in _extract_tr_keys(HOST.read_text(encoding="utf-8"))}
    assert key in used, "%r is in TRANSLATIONS but no tr() call uses it" % key


def test_pause_labels_are_swapped_through_tr_at_call_time():
    """Language switching is restart-based, so a label captured when the button
    was constructed would be stale. _apply_pause_state must call tr() itself."""
    import inspect

    import genizah_app
    src = inspect.getsource(genizah_app.GenizahGUI._apply_pause_state)
    for key in ("Pause", "Resume", "Pausing..."):
        assert 'tr("%s")' % key in src, key


def test_ellipses_match_the_existing_stop_family():
    """The desktop app uses ASCII "..." for these ("Stopping...", "Cancelling..."),
    not U+2026. Mixing them makes the strings look inconsistent in the UI."""
    for key in PAUSE_KEYS:
        assert '…' not in key, "%r uses U+2026 instead of ASCII ..." % key
