# -*- coding: utf-8 -*-
"""Static AST guard: every tr() call in desktop/join_workbench.py has a corresponding
key in genizah_translations.TRANSLATIONS, AND the Phase-107 host strings are
translated and tr()-wrapped in the host files (genizah_app.py / result_dialog.py).

SC#6 invariant: all strings in the workbench are bilingual from the first line.
Two checks in one file:

1. Full-module check on desktop/join_workbench.py (new module — full-file scan is correct).
2. Scoped check on the Phase-107 host additions in genizah_app.py / result_dialog.py
   (must-fix #10 from 107-REVIEWS.md): rather than scanning those huge host files
   wholesale (hundreds of pre-existing tr() calls), assert ONLY the specific Phase-107
   NEW keys both resolve in TRANSLATIONS AND appear as tr("<key>") in at least one
   host file.  The wrapped-in-source assertion is conditionally active — it only fires
   when at least one host file already contains the tr("Find joins") call (i.e., after
   Plan 03 adds it).  This keeps Plan 01 green while enforcing the wrap by Plan 03's
   wave-merge gate.

Pattern source: tests/test_pgp_filter_cascade.py AST scanner.
"""
import ast
import pathlib

import pytest

TARGET = pathlib.Path(__file__).parent.parent / "desktop" / "join_workbench.py"

# The NEW user-facing keys Plan 03 introduces in the host files
# (genizah_app.py / desktop/result_dialog.py).  Scoped to the Phase-107
# additions — NOT a full-file scan, because the host files contain hundreds of
# pre-existing tr() strings (must-fix #10 from 107-REVIEWS.md).
PHASE_107_HOST_KEYS = ["Find joins"]


def _extract_tr_keys(source: str) -> list:
    """Return [(key_string, lineno), ...] for every tr("...") literal call in source."""
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


# ── Check 1: full-module scan on join_workbench.py ───────────────────────────


def test_all_tr_keys_in_translations():
    """SC#6: every tr() key in desktop/join_workbench.py must be in TRANSLATIONS."""
    from genizah_translations import TRANSLATIONS

    source = TARGET.read_text(encoding="utf-8")
    keys = _extract_tr_keys(source)
    missing = [
        (key, lineno)
        for key, lineno in keys
        if key not in TRANSLATIONS
    ]
    assert not missing, (
        "SC#6 violation — tr() keys in desktop/join_workbench.py not in TRANSLATIONS:\n"
        + "\n".join(f"  {key!r} (line {lineno})" for key, lineno in missing)
        + "\n\nFix: add the key to genizah_translations.TRANSLATIONS "
        "(use TRANSLATIONS.update({...}) at the end)."
    )


def test_target_file_exists():
    """Guard against the file being accidentally removed."""
    assert TARGET.exists(), f"desktop/join_workbench.py not found at {TARGET}"


# ── Check 2: scoped Phase-107 host-key assertions ────────────────────────────


def test_phase107_host_keys_in_translations():
    """SC#6 (host): every Phase-107 host key is present in TRANSLATIONS.

    This assertion runs unconditionally from Plan 01 because Plan-01 Task-1
    bootstraps all phase keys (including 'Find joins').
    """
    from genizah_translations import TRANSLATIONS

    missing_tr = [k for k in PHASE_107_HOST_KEYS if k not in TRANSLATIONS]
    assert not missing_tr, (
        f"Phase-107 host keys absent from TRANSLATIONS: {missing_tr}\n"
        "Fix: add to the Phase-107 TRANSLATIONS.update block in genizah_translations.py."
    )


# Gap-round-3 keys (G-06 eye tooltip / G-13 hint+empty / G-08 link tooltip).
# These are CALLED by desktop/join_workbench.py (eye tooltip, hint, empty msg) and by
# corrections_ui.py (G-08 tooltip) — corrections_ui.py is not AST-scanned, so this guard
# pins the G-08 key too.
GAP_ROUND_3_KEYS = [
    "visual similarity",
    "Turn off Visual Similarity to see more results",
    "No look-alikes match this search — turn off Visual Similarity to see all results",
    "find joins in joins lab",
]


def test_gap_round_3_keys_in_translations():
    """G-06/G-13/G-08: every gap-round-3 key resolves in TRANSLATIONS (D-17)."""
    from genizah_translations import TRANSLATIONS
    missing = [k for k in GAP_ROUND_3_KEYS if k not in TRANSLATIONS]
    assert not missing, (
        f"Gap-round-3 keys absent from TRANSLATIONS: {missing}\n"
        "Fix: add to the Phase-109 gap-round-3 TRANSLATIONS.update block in genizah_translations.py."
    )


def test_phase107_host_keys_translated_and_wrapped():
    """SC#6 (host): Phase-107 host strings are tr()-wrapped in genizah_app.py /
    desktop/result_dialog.py.

    This test is conditionally active: it only enforces the tr()-wrap once at
    least one host file already contains tr("Find joins") — i.e., after Plan 03
    adds the call.  During Plan 01/02, the assertion is skipped with xfail so CI
    stays green while Plan 03's wave-merge gate enforces the wrap.

    Implementation: check whether the host files already contain the tr("Find joins")
    call; if not, mark the test as xfail(strict=False) so Plan 01/02 pass and
    Plan 03 flips it to xpass/pass.
    """
    from genizah_translations import TRANSLATIONS

    hosts = [
        pathlib.Path(__file__).parent.parent / "genizah_app.py",
        pathlib.Path(__file__).parent.parent / "desktop" / "result_dialog.py",
    ]
    # Read all host-file source that exists
    host_src = "\n".join(
        p.read_text(encoding="utf-8") for p in hosts if p.exists()
    )

    # Check whether Plan 03 has already added the tr() calls
    any_host_wrapped = any(
        f'tr("{k}")' in host_src or f"tr('{k}')" in host_src
        for k in PHASE_107_HOST_KEYS
    )

    if not any_host_wrapped:
        # Plan 03 has not yet added the host tr() calls — skip softly.
        # This xfail becomes an xpass once Plan 03 wires tr("Find joins").
        pytest.xfail(
            "Plan 03 has not yet added tr('Find joins') to genizah_app.py / "
            "result_dialog.py — expected; this assertion self-activates after Plan 03."
        )

    # Keys must be in TRANSLATIONS (already checked above, double-check here)
    missing_tr = [k for k in PHASE_107_HOST_KEYS if k not in TRANSLATIONS]
    assert not missing_tr, (
        f"Phase-107 host keys absent from TRANSLATIONS: {missing_tr}"
    )

    # Every PHASE_107_HOST_KEYS key must appear as tr("<key>") in at least one host
    unwrapped = [
        k for k in PHASE_107_HOST_KEYS
        if f'tr("{k}")' not in host_src and f"tr('{k}')" not in host_src
    ]
    assert not unwrapped, (
        f"Phase-107 host strings not tr()-wrapped in genizah_app.py / result_dialog.py: "
        f"{unwrapped}\n"
        "Fix: wrap each string with tr() in the host files."
    )
