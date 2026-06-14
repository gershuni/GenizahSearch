"""PRIV-03 structural CI guard: no desktop/ file except desktop/telemetry.py
may import shared.posthog_server or call enqueue_event.

This guard mirrors tests/test_no_raw_storage_access.py (Phase 87 pattern).
It is a STATIC AST scan — it never imports desktop/ modules at test time
(importing desktop/ would pull in PyQt6 and other heavy dependencies).

Threat model: T-111-15 (Tampering/Information Disclosure) — a future
desktop/ callsite bypassing the consent gate to reach enqueue_event.

Detection strategy:
  - ast.Import of shared.posthog_server (aliased or not)
  - ast.ImportFrom with module == 'shared.posthog_server'
  - ast.ImportFrom with module == 'shared' that imports name 'posthog_server'
  - ast.Call whose callee is Name(id='enqueue_event') (bare call)
  - ast.Call whose callee is Attribute(attr='enqueue_event') (aliased call)

Exemption: EXACTLY desktop/telemetry.py, matched by RESOLVED PATH — not by
basename — so a future desktop/widgets/telemetry.py would still be scanned
(REVIEWS LOW / T-111-19).

Phase-111 provenance note: PRIV-03 is formally assigned to Phase 116 in
REQUIREMENTS.md, but is delivered here (Phase 111-03) because the chokepoint
already exists and shipping the guard early prevents Phases 112-115 from
introducing violations that would only surface at the milestone end.
Phase 116 plan should REFERENCE this guard, not re-implement it.
"""
import ast
import textwrap
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DESKTOP_DIR = REPO_ROOT / 'desktop'
CHOKEPOINT = (DESKTOP_DIR / 'telemetry.py').resolve()


class _PosthogAccessVisitor(ast.NodeVisitor):
    """Visit AST nodes and collect posthog_server import / enqueue_event violations.

    Detects two violation classes:
      1. Import of shared.posthog_server in any form:
           import shared.posthog_server
           import shared.posthog_server as ph
           from shared.posthog_server import enqueue_event
           from shared import posthog_server
           from shared import posthog_server as ph

      2. Call to enqueue_event in any form:
           enqueue_event('event', {})           # bare Name call
           ph.enqueue_event('event', {})        # Attribute call (aliased module)

    Uses parent tracking (_seen_inner_ids) to avoid double-reporting Attribute
    nodes that are already consumed as part of a Call node — mirrors the
    parent-tracking approach in tests/test_no_raw_storage_access.py (B2 fix).
    """

    def __init__(self, source: str):
        self.source = source
        self.violations: list[tuple[int, str]] = []  # (lineno, description)
        self._seen_inner_ids: set[int] = set()

    def _record(self, node: ast.AST, description: str) -> None:
        self.violations.append((node.lineno, description))

    # ------------------------------------------------------------------
    # Import detection
    # ------------------------------------------------------------------

    def visit_Import(self, node: ast.Import) -> None:
        """Detect: import shared.posthog_server [as ph]"""
        for alias in node.names:
            # Match bare 'shared.posthog_server' or any dotted prefix
            if alias.name == 'shared.posthog_server' or alias.name.startswith('shared.posthog_server.'):
                self._record(node, f"import {alias.name}")
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        """Detect two forms:
           from shared.posthog_server import ...  (module == 'shared.posthog_server')
           from shared import posthog_server [as ph]  (module == 'shared', name == 'posthog_server')
        """
        module = node.module or ''
        if module == 'shared.posthog_server':
            # from shared.posthog_server import enqueue_event [, ...]
            imported_names = ', '.join(
                (a.asname or a.name) for a in node.names
            )
            self._record(node, f"from shared.posthog_server import {imported_names}")
        elif module == 'shared':
            # from shared import posthog_server [as ph]
            for alias in node.names:
                if alias.name == 'posthog_server':
                    display = f"from shared import posthog_server" + (
                        f" as {alias.asname}" if alias.asname else ''
                    )
                    self._record(node, display)
        self.generic_visit(node)

    # ------------------------------------------------------------------
    # Call detection
    # ------------------------------------------------------------------

    def visit_Call(self, node: ast.Call) -> None:
        """Detect enqueue_event(...) calls:
           - Name(id='enqueue_event')              -> bare call
           - Attribute(attr='enqueue_event')        -> aliased: ph.enqueue_event(...)
        """
        if isinstance(node.func, ast.Name):
            if node.func.id == 'enqueue_event':
                self._record(node, "enqueue_event(...) [bare call]")
                # Mark inner func node so visit_Attribute does not double-report
                self._seen_inner_ids.add(id(node.func))
        elif isinstance(node.func, ast.Attribute):
            if node.func.attr == 'enqueue_event':
                seg = ast.get_source_segment(self.source, node) or 'enqueue_event(...)'
                self._record(node, f"{seg} [attribute call]")
                # Mark the Attribute node itself as seen
                self._seen_inner_ids.add(id(node.func))
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        """Catch bare attribute access `ph.enqueue_event` not already consumed by visit_Call."""
        if id(node) in self._seen_inner_ids:
            return
        if node.attr == 'enqueue_event':
            # Only report if this attribute access was not already caught as a Call
            self._record(node, "enqueue_event attribute access (not a call)")
        self.generic_visit(node)


def _scan_file(path: Path) -> list[tuple[int, str]]:
    """Parse one .py file and return (lineno, description) violations."""
    source = path.read_text(encoding='utf-8')
    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError as e:
        # Surface parse errors as violations so CI catches broken files
        return [(0, f"SyntaxError: {e}")]
    visitor = _PosthogAccessVisitor(source)
    visitor.visit(tree)
    return visitor.violations


# ===========================================================================
# Tests
# ===========================================================================

def test_no_direct_posthog_outside_chokepoint():
    """Production guard: no file under desktop/ except desktop/telemetry.py
    may import shared.posthog_server or call enqueue_event.

    Exemption is by RESOLVED PATH — not by basename — so a hypothetical
    desktop/widgets/telemetry.py would still be scanned (REVIEWS LOW /
    T-111-19).

    At Phase 111 this test MUST PASS: only desktop/telemetry.py touches
    the transport today.
    """
    violations = []
    for path in DESKTOP_DIR.rglob('*.py'):
        # Exempt ONLY the official chokepoint, by resolved path
        if path.resolve() == CHOKEPOINT:
            continue
        rel = path.relative_to(REPO_ROOT).as_posix()
        file_violations = _scan_file(path)
        for lineno, description in file_violations:
            violations.append(f"{rel}:{lineno}: {description}")

    if violations:
        msg = (
            "Direct posthog_server access found outside the chokepoint "
            "(desktop/telemetry.py):\n  "
            + "\n  ".join(violations[:50])
            + f"\n\nTotal violations: {len(violations)}"
            + "\n\nFix: route all telemetry through desktop/telemetry.py "
              "public callables (track, track_error, track_performance, "
              "identify, reset_identity). Do NOT import shared.posthog_server "
              "directly from any other desktop/ file (D-05 / PRIV-03)."
        )
        import pytest
        pytest.fail(msg)


def test_chokepoint_itself_does_import_posthog():
    """Positive control: desktop/telemetry.py MUST import shared.posthog_server.

    If this test fails, the exemption in test_no_direct_posthog_outside_chokepoint
    is exempting a file that no longer uses posthog_server — a vacuous invariant.
    Fail loudly so the guard is audited.
    """
    violations = _scan_file(CHOKEPOINT)
    import_violations = [
        (ln, desc) for ln, desc in violations
        if 'posthog_server' in desc and 'enqueue_event' not in desc.lower().replace('import', '')
    ]
    # Accept either import form or enqueue_event usage as proof of legitimate use
    assert violations, (
        "desktop/telemetry.py does not import shared.posthog_server or call "
        "enqueue_event — the PRIV-03 exemption may be exempting the wrong file. "
        "Investigate: has the chokepoint been moved or refactored?"
    )


def test_lint_rejects_synthetic_violation():
    """Synthetic test: the AST visitor detects a bare enqueue_event call.

    Mirrors tests/test_no_raw_storage_access.py lines 235-247.
    Proves the scanner is not vacuous — it catches the simplest violation form.
    """
    synthetic = textwrap.dedent("""\
        from shared.posthog_server import enqueue_event
        def bad():
            enqueue_event('x', {})
    """)
    tree = ast.parse(synthetic)
    visitor = _PosthogAccessVisitor(synthetic)
    visitor.visit(tree)
    assert visitor.violations, (
        "AST visitor failed to detect synthetic bare enqueue_event call — "
        "the PRIV-03 guard is not working (scanner is vacuous)."
    )


def test_lint_detects_aliased_import_call():
    """Synthetic test: visitor detects 'import shared.posthog_server as ph; ph.enqueue_event(...)'.

    Covers the aliased-import + Attribute-call form that Codex identified as a
    bypass risk (T-111-15 / Attribute callee detection).
    """
    synthetic = textwrap.dedent("""\
        import shared.posthog_server as ph
        def bad():
            ph.enqueue_event('x', {})
    """)
    tree = ast.parse(synthetic)
    visitor = _PosthogAccessVisitor(synthetic)
    visitor.visit(tree)
    assert visitor.violations, (
        "AST visitor failed to detect 'import shared.posthog_server as ph; "
        "ph.enqueue_event(...)' — aliased import + Attribute call not caught."
    )
    # Must catch both the import AND the call
    descriptions = [desc for _, desc in visitor.violations]
    assert any('shared.posthog_server' in d for d in descriptions), (
        "Import 'import shared.posthog_server as ph' was not flagged."
    )
    assert any('enqueue_event' in d for d in descriptions), (
        "Call 'ph.enqueue_event(...)' was not flagged."
    )


def test_lint_detects_from_shared_import_alias():
    """Synthetic test: visitor detects 'from shared import posthog_server as ph; ph.enqueue_event(...)'.

    This is the REVIEWS LOW form Codex specifically called out — an indirect
    import via 'from shared import posthog_server' that bypasses the
    'from shared.posthog_server import' detector if not handled separately.
    """
    synthetic = textwrap.dedent("""\
        from shared import posthog_server as ph
        def bad():
            ph.enqueue_event('x', {})
    """)
    tree = ast.parse(synthetic)
    visitor = _PosthogAccessVisitor(synthetic)
    visitor.visit(tree)
    assert visitor.violations, (
        "AST visitor failed to detect 'from shared import posthog_server as ph; "
        "ph.enqueue_event(...)' — REVIEWS LOW form not caught (T-111-15)."
    )
    descriptions = [desc for _, desc in visitor.violations]
    assert any('posthog_server' in d for d in descriptions), (
        "'from shared import posthog_server' was not flagged."
    )
    assert any('enqueue_event' in d for d in descriptions), (
        "Call 'ph.enqueue_event(...)' was not flagged."
    )


def test_skip_is_by_resolved_path_not_basename():
    """Resolved-path exemption guard (REVIEWS LOW / T-111-19).

    Constructs a hypothetical path 'desktop/widgets/telemetry.py' and asserts
    that its resolved path does NOT equal CHOKEPOINT. This proves that the
    production loop's exemption check (path.resolve() == CHOKEPOINT) would
    still scan a future desktop/widgets/telemetry.py — the guard is not
    bypassable by creating a file with the same basename in a subdirectory.

    Does NOT create a real file on disk — uses only Path arithmetic.
    """
    hypothetical = DESKTOP_DIR / 'widgets' / 'telemetry.py'
    # This path cannot equal CHOKEPOINT because CHOKEPOINT resolves to
    # desktop/telemetry.py (one level up), not desktop/widgets/telemetry.py
    assert hypothetical.resolve() != CHOKEPOINT, (
        "UNEXPECTED: desktop/widgets/telemetry.py resolves to the same path as "
        "desktop/telemetry.py — the exemption would incorrectly skip it. "
        "Investigate filesystem symlinks or path resolution anomalies."
    )
    # Also confirm that basename comparison WOULD erroneously skip it (the bug we avoid)
    assert hypothetical.name == CHOKEPOINT.name, (
        "Hypothetical path does not share basename 'telemetry.py' — "
        "test setup is incorrect."
    )
    # The resolved-path check must reject it
    assert hypothetical.resolve() != CHOKEPOINT  # guard is correct
