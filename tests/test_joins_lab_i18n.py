# -*- coding: utf-8 -*-
"""Permanent CI guard: Joins Lab i18n invariants (FND-07, SC#1, SC#3).

Four checks in one file:

1. test_no_raw_hebrew_literals — SC#3: no raw Hebrew literal outside tr() in the 8
   full-scan files (with the D-04 allowlist for joins_builder.py operator tuples).

2. test_all_tr_keys_covered — SC#1: every tr("literal") key in the 8 full-scan files
   resolves in TRANSLATIONS (catches future drift or additions without HE entries).

3. test_badge_strings_covered — SC#1: the 3 badge_and_tooltip() strings (returned as
   plain strings by shared/joins_lab.py and wrapped via tr(variable) at call sites —
   NOT catchable by the AST literal scanner) each resolve in TRANSLATIONS.

4. test_entry_point_keys — SC#1 consistency: the scoped entry-point keys both
   resolve in TRANSLATIONS AND appear as tr("key") in their host files.

5. test_target_files_exist — guard: all 8 full-scan files exist.

Pattern source: tests/test_join_workbench_i18n.py (desktop template).
"""
import ast
import pathlib
import re

import pytest

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

REPO_ROOT = pathlib.Path(__file__).parent.parent

# The 8 dedicated files for full-scan (D-05): only these are audited wholesale.
# web/joins_executor.py is EXCLUDED: 0 tr() calls, no user-facing strings.
FULL_SCAN_FILES = [
    'web/pages/joins_lab.py',
    'web/components/candidate_grid.py',
    'web/components/compare_modal.py',
    'web/components/anchor_viewer.py',
    'web/components/joins_panel.py',
    'web/components/joins_builder.py',
    'web/components/known_joins_group.py',
    'web/joins_lab_storage.py',
]

# Badge strings returned by shared/joins_lab.py::badge_and_tooltip() and passed
# to tr() as VARIABLE arguments at candidate_grid.py:760 and compare_modal.py:470-471.
# The AST literal scanner CANNOT detect these (tr(variable) not tr("literal")).
# Analogous to GAP_ROUND_3_KEYS in tests/test_join_workbench_i18n.py (Q2 / REVIEWS #5).
BADGE_STRINGS = [
    'Anchor fragment',
    'Found via other side',
    'Visually similar',
]

# Scoped entry-point keys (Q5): map each to its host file.
# These are keys that appear in the 5 entry-point files (FND-04/05/D-19/ACT-02);
# the host files are NOT in FULL_SCAN_FILES (they are large shared files).
# The guard asserts: (a) key in TRANSLATIONS, (b) tr("key") appears in host file source.
ENTRY_POINT_KEYS = {
    'Find Joins in the Joins Lab': 'web/components/joins_panel.py',
    'Joined Fragments': 'web/components/joins_panel.py',
    'Go to Joins Lab to find more joins': 'web/components/joins_panel.py',
    'Open in Joins Lab': 'web/pages/lists.py',
}

# D-04 allowlist: the exact literal values from joins_builder.py:344-351.
# These are syntax-legend operator tuples (Hebrew מילה = "word") — intentionally
# bilingual-safe examples, NOT user-facing strings that need tr().
# The allowlist must contain the EXACT string values (byte-for-byte match — Pitfall 2).
HEBREW_LITERAL_ALLOWLIST = {
    '#מילה',
    'מילה#',
    '%מילה',
    '*מילה / מילה*',
    '(א/ב)',
    '-מילה',
    '|מילה',
    'מילה|',
}

# Standard Hebrew Unicode block (U+0590–U+05FF): aleph-bet, vowels, cantillation.
HEB = re.compile(r"[֐-׿]")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_tr_keys(source: str) -> list:
    """Return [(key_string, lineno), ...] for every tr("...") literal call in source.

    Copied verbatim from tests/test_join_workbench_i18n.py (desktop template).
    Catches tr("literal") only — NOT tr(variable).
    """
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


def _is_docstring_node(node: ast.Constant, tree: ast.AST) -> bool:
    """Return True if the ast.Constant node is a module/class/function docstring.

    A string is a docstring if its parent is an ast.Expr that is the FIRST
    statement in the body of a Module, FunctionDef, AsyncFunctionDef, or ClassDef.
    This structurally excludes docstrings from the raw-Hebrew-literal check (Pitfall 3).
    """
    par = getattr(node, '_parent', None)
    if not isinstance(par, ast.Expr):
        return False
    grandpar = getattr(par, '_parent', None)
    if grandpar is None:
        return False
    body = getattr(grandpar, 'body', None)
    if body and par is body[0]:
        return isinstance(grandpar, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    return False


def _extract_raw_hebrew_literals(source: str) -> list:
    """Return [(value, lineno), ...] for every raw Hebrew literal OUTSIDE tr().

    Exclusions (per D-02 / D-04 rules):
    (a) The node is inside a tr() call (already wrapped — OK).
    (b) The node is a docstring (structurally excluded — Pitfall 3).
    (c) The value is in HEBREW_LITERAL_ALLOWLIST (D-04 intentional examples).

    The parent-link tagging approach follows _tmp/find_missing_tr2.py:65-80.
    """
    tree = ast.parse(source)

    # Tag parent nodes so each child can find its parent (Pitfall 3 / Q3 approach).
    for p in ast.walk(tree):
        for c in ast.iter_child_nodes(p):
            c._parent = p  # type: ignore[attr-defined]

    leaks = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Constant):
            continue
        if not isinstance(node.value, str):
            continue
        if not HEB.search(node.value):
            continue

        # (a) Skip if immediately inside a tr() call.
        par = getattr(node, '_parent', None)
        if (
            isinstance(par, ast.Call)
            and isinstance(par.func, ast.Name)
            and par.func.id == 'tr'
        ):
            continue

        # (b) Skip docstrings (structurally, not by position heuristic).
        if _is_docstring_node(node, tree):
            continue

        # (c) Skip D-04 allowlist (intentional bilingual-safe operator examples).
        if node.value in HEBREW_LITERAL_ALLOWLIST:
            continue

        leaks.append((node.value, node.lineno))

    return leaks


# ---------------------------------------------------------------------------
# Test 0: guard — all 8 full-scan files exist
# ---------------------------------------------------------------------------

def test_target_files_exist():
    """Guard: all 8 full-scan files exist at their expected paths."""
    missing = [
        f for f in FULL_SCAN_FILES
        if not (REPO_ROOT / f).exists()
    ]
    assert not missing, (
        "Full-scan file(s) missing from the repository:\n"
        + "\n".join(f"  {f}" for f in missing)
        + "\n\nIf a file was renamed, update FULL_SCAN_FILES in tests/test_joins_lab_i18n.py."
    )


# ---------------------------------------------------------------------------
# Test 1: SC#3 — no raw Hebrew literals outside tr() in the 8 full-scan files
# ---------------------------------------------------------------------------

def test_no_raw_hebrew_literals():
    """SC#3: zero raw Hebrew literals outside tr() in the 8 full-scan files.

    Allows the D-04 operator-tuple examples in joins_builder.py:344-351
    and structurally excludes docstrings (Pitfall 2, Pitfall 3).

    To prove the guard bites: temporarily insert a raw Hebrew literal (e.g.
    x = "שלום") outside tr() in any full-scan file — this test will FAIL.
    Removing it restores green (SUMMARY sanity injection proof).
    """
    all_leaks = []
    for rel_path in FULL_SCAN_FILES:
        path = REPO_ROOT / rel_path
        if not path.exists():
            continue  # test_target_files_exist catches missing files
        source = path.read_text(encoding="utf-8")
        leaks = _extract_raw_hebrew_literals(source)
        for value, lineno in leaks:
            all_leaks.append((rel_path, lineno, value))

    assert not all_leaks, (
        "SC#3 violation — raw Hebrew literals outside tr() detected:\n"
        + "\n".join(
            f"  {f}:{lineno}: {value!r}"
            for f, lineno, value in all_leaks
        )
        + "\n\nFix: wrap each literal with tr() in the source file, or add it to "
        "HEBREW_LITERAL_ALLOWLIST in tests/test_joins_lab_i18n.py if it is an "
        "intentional bilingual-safe example (like the D-04 operator tuples)."
    )


# ---------------------------------------------------------------------------
# Test 2: SC#1 — every tr("literal") key in the 8 files resolves in TRANSLATIONS
# ---------------------------------------------------------------------------

def test_all_tr_keys_covered():
    """SC#1: every tr("literal") key in the 8 full-scan files must be in TRANSLATIONS.

    Catches future additions of tr("new key") without a corresponding HE entry.
    BADGE_STRINGS (tr(variable) call sites) are covered by test_badge_strings_covered.
    """
    from genizah_translations import TRANSLATIONS

    all_missing = []
    for rel_path in FULL_SCAN_FILES:
        path = REPO_ROOT / rel_path
        if not path.exists():
            continue
        source = path.read_text(encoding="utf-8")
        keys = _extract_tr_keys(source)
        for key, lineno in keys:
            if key not in TRANSLATIONS:
                all_missing.append((rel_path, lineno, key))

    assert not all_missing, (
        "SC#1 violation — tr() keys in Joins Lab files not in TRANSLATIONS:\n"
        + "\n".join(
            f"  {f}:{lineno}: {key!r}"
            for f, lineno, key in all_missing
        )
        + "\n\nFix: add each missing key to genizah_translations.TRANSLATIONS "
        "(use TRANSLATIONS.update({...}) at the end of genizah_translations.py)."
    )


# ---------------------------------------------------------------------------
# Test 3: SC#1 — badge strings from badge_and_tooltip() resolve in TRANSLATIONS
# ---------------------------------------------------------------------------

def test_badge_strings_covered():
    """SC#1: every BADGE_STRINGS key resolves in TRANSLATIONS.

    These 3 strings are returned by shared/joins_lab.py::badge_and_tooltip() and
    wrapped at web call sites via tr(tooltip_text) — a VARIABLE argument, so the
    AST literal scanner in test_all_tr_keys_covered cannot catch them. This test
    pins them as an explicit static list (analogous to GAP_ROUND_3_KEYS in the
    desktop guard test_gap_round_3_keys_in_translations).

    Both web call sites are covered:
    - candidate_grid.py:760: ui.icon(icon_name).tooltip(tr(tooltip_text))
    - compare_modal.py:470-471: same pattern (REVIEWS #5)
    """
    from genizah_translations import TRANSLATIONS

    missing = [k for k in BADGE_STRINGS if k not in TRANSLATIONS]
    assert not missing, (
        f"SC#1 badge-string violation — badge_and_tooltip() keys absent from TRANSLATIONS: "
        f"{missing}\n"
        "Fix: add each key to genizah_translations.TRANSLATIONS "
        "(they are returned by shared/joins_lab.py::badge_and_tooltip() and wrapped "
        "via tr(variable) at candidate_grid.py:760 and compare_modal.py:470-471)."
    )


# ---------------------------------------------------------------------------
# Test 4: SC#1 consistency — scoped entry-point keys resolve + are tr()-wrapped
# ---------------------------------------------------------------------------

def test_entry_point_keys():
    """SC#1 consistency: scoped entry-point keys resolve in TRANSLATIONS AND
    appear as tr("key") (or tr('key')) in their host files.

    The host files (joins_panel.py, lists.py) are large shared files with
    hundreds of pre-existing tr() calls — we do NOT full-scan them.  Instead,
    this check asserts ONLY the specific Phase-120/121 entry-point keys are
    (a) present in TRANSLATIONS (translations exist), and
    (b) wrapped as tr("key") in the mapped host file (code uses the key).

    For 'Open in Joins Lab' specifically, this pins the Pitfall-5 invariant:
    the key resolves AND tr('Open in Joins Lab') still appears in lists.py.
    """
    from genizah_translations import TRANSLATIONS

    failures = []
    for key, rel_host in ENTRY_POINT_KEYS.items():
        # (a) Key must be in TRANSLATIONS.
        if key not in TRANSLATIONS:
            failures.append(
                f"  Key {key!r} is ABSENT from TRANSLATIONS "
                f"(host: {rel_host})"
            )
            continue  # no point checking (b) if (a) fails

        # (b) tr("key") or tr('key') must appear in the host file source.
        host_path = REPO_ROOT / rel_host
        if not host_path.exists():
            failures.append(
                f"  Host file {rel_host} does not exist "
                f"(cannot verify tr() wrap for key {key!r})"
            )
            continue
        host_src = host_path.read_text(encoding="utf-8")
        wrapped = (f'tr("{key}")' in host_src) or (f"tr('{key}')" in host_src)
        if not wrapped:
            failures.append(
                f"  Key {key!r} is in TRANSLATIONS but NOT tr()-wrapped "
                f"in {rel_host} "
                f"(searched for tr(\"{key}\") and tr('{key}'))"
            )

    assert not failures, (
        "SC#1 entry-point key consistency violations:\n"
        + "\n".join(failures)
        + "\n\nFix: ensure each key (a) has a HE entry in TRANSLATIONS and "
        "(b) is used as tr(\"key\") in the mapped host file."
    )
