# -*- coding: utf-8 -*-
"""Static AST guard: no web/*.py calls .stop_propagation() in Python code.

Origin: prod AttributeError 2026-06-12 — `web/components/visual_similarity_dialog.py:602`
registered `.on('click', lambda e: e.stop_propagation())`. NiceGUI's
GenericEventArguments has no `stop_propagation` method, and it never could work:
by the time the server hears about a DOM event it has ALREADY propagated in the
browser. Propagation must be stopped client-side, via either:

  - the Vue event modifier:  .on('click.stop', handler)
  - a JS handler:            .on('click', js_handler='(e) => e.stopPropagation()')

This guard scans every Python file under web/ for an attribute call named
`stop_propagation` (any receiver) and fails CI with the offending locations.
JavaScript `stopPropagation()` inside string literals is invisible to the AST
scanner and remains allowed (that is the correct place for it).

Mirrors the AST scanner convention of tests/test_web_library_options_no_local.py.
"""
import ast
import pathlib

WEB_DIR = pathlib.Path(__file__).parent.parent / "web"

# No exemptions today. Add (relative_path, lineno-independent function name)
# entries only if a future object legitimately exposes a Python-side
# stop_propagation() method. Document the reason alongside the exemption.
EXEMPT_FILES: set[str] = set()


def _find_stop_propagation_calls(tree):
    """Yield lineno of every `<anything>.stop_propagation(...)` call."""
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "stop_propagation"
        ):
            yield node.lineno


def test_no_python_side_stop_propagation_in_web():
    """No web/ Python code may call .stop_propagation() — it does not exist
    server-side and silently breaks the shielding intent (the click DOES
    propagate to the parent handler while raising AttributeError per click).
    """
    offenders = []
    for py_file in WEB_DIR.rglob("*.py"):
        rel = str(py_file.relative_to(WEB_DIR.parent)).replace("\\", "/")
        if rel in EXEMPT_FILES:
            continue
        try:
            source = py_file.read_text(encoding="utf-8")
            tree = ast.parse(source)
        except (SyntaxError, OSError):
            continue
        for lineno in _find_stop_propagation_calls(tree):
            offenders.append(f"  {rel}:{lineno}")
    assert not offenders, (
        "Python-side .stop_propagation() calls found in web/ — "
        "GenericEventArguments has no such method; stop propagation "
        "client-side with .on('click.stop', ...) or "
        ".on('click', js_handler='(e) => e.stopPropagation()'):\n"
        + "\n".join(offenders)
    )


def test_fixed_sites_use_client_side_stop():
    """Positive assertion: the four sites fixed on 2026-06-12 stop propagation
    client-side via js_handler (visual_similarity_dialog x2, version_selector x2
    — the PGP menu_item link and the edition menu_item link, the latter flagged
    by Codex review: it bubbled into select_edition without any shield).
    """
    vs_src = (WEB_DIR / "components" / "visual_similarity_dialog.py").read_text(
        encoding="utf-8"
    )
    sel_src = (WEB_DIR / "components" / "version_selector.py").read_text(
        encoding="utf-8"
    )
    assert vs_src.count("js_handler='(e) => e.stopPropagation()'") >= 2, (
        "visual_similarity_dialog.py lost its client-side stop-propagation "
        "shields (shelfmark link + open_in_new link inside the clickable row)"
    )
    assert sel_src.count("js_handler='(e) => e.stopPropagation()'") >= 2, (
        "version_selector.py lost a client-side stop-propagation shield: both "
        "'View on PGP' external links (inside the select_pgp menu_item AND the "
        "select_edition menu_item) must not propagate clicks to their parent"
    )


def test_web_dir_exists():
    """Sanity check: web/ directory must exist for the scanner to work."""
    assert WEB_DIR.is_dir(), (
        f"web/ directory not found at {WEB_DIR}. The AST scanner has nothing to scan."
    )
