"""D-17 producer-layer AST guard: telemetry call arguments must not source values
from forbidden UI accessors (REVIEWS HIGH-3 re-scope + LOW-10 identity check).

Phase 114 REVIEWS HIGH-3: the original whole-FunctionDef design would false-positive on
functions that legitimately call forbidden accessors for NON-telemetry work:
  - on_search_finished: calls query_input.text() / gap_input.text()
  - export_results: calls mode_combo.currentText()
  - export_comp_report: calls comp_text_area.toPlainText() / comp_mode_combo.currentText()

RE-SCOPE FIX: inspect ONLY the argument/keyword-value expressions passed to telemetry
calls, NOT whole FunctionDefs.  Co-located accessor calls for non-telemetry work are FINE.

REVIEWS LOW-10: identity callsite check — identify() arguments must be _uuid sources,
never user.id / getattr(user, "id") / hash(...) / bare numerics.

Mirrors tests/test_no_raw_storage_access.py (Phase 87) and
tests/test_telemetry_no_direct_posthog.py (Phase 111) structurally.
"""
import ast
import textwrap
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
TARGET_FILES = [
    REPO_ROOT / 'genizah_app.py',
    REPO_ROOT / 'gui_threads.py',
    REPO_ROOT / 'desktop' / 'result_dialog.py',
]

# Forbidden method names that must never appear as ARGUMENTS to telemetry calls (D-04).
# Safe for non-telemetry use — the guard only inspects telemetry-call argument subtrees.
FORBIDDEN_ACCESSORS = frozenset({
    'currentText',   # QComboBox — translated EN/HE (D-05)
    'tabText',       # QTabWidget — translated EN/HE (D-01)
    'windowTitle',   # may embed shelfmark (D-04, genizah_app.py VS dialog note)
    'text',          # QLabel/QLineEdit — may carry user content
    'selectedFiles', # QFileDialog — file paths (D-04)
    'toPlainText',   # QTextEdit — query content
})


def _is_telemetry_call(node: ast.Call) -> bool:
    """Return True if this Call node is a telemetry emission call.

    Detects:
      telemetry.track(...)          — Attribute: value=Name('telemetry'), attr='track'
      _emit_search_telemetry(...)   — Name or Attribute with those emit helper names
      _emit_comp_search_telemetry(...)
      _emit_pgp_tag_search_telemetry(...)
      _emit_feature_opened(...)
    """
    EMIT_HELPERS = frozenset({
        '_emit_search_telemetry',
        '_emit_comp_search_telemetry',
        '_emit_pgp_tag_search_telemetry',
        '_emit_feature_opened',
        'track',
    })
    func = node.func
    if isinstance(func, ast.Attribute):
        # telemetry.track(...)
        if func.attr in EMIT_HELPERS:
            return True
    elif isinstance(func, ast.Name):
        if func.id in EMIT_HELPERS:
            return True
    return False


def _is_identify_call(node: ast.Call) -> bool:
    """Return True if this Call node is a telemetry.identify() call."""
    func = node.func
    if isinstance(func, ast.Attribute):
        return func.attr == 'identify'
    elif isinstance(func, ast.Name):
        return func.id == 'identify'
    return False


def _is_allowed_identity_source(arg_node: ast.expr) -> bool:
    """Return True if this AST expression is an allowed identity source.

    Allowed: attribute ending in '_uuid' (e.g. user._uuid, current_user._uuid)
    or a Name/Attribute literally containing 'uuid' (case-insensitive).
    Not allowed: user.id, getattr(user, "id"), hash(...), numeric literals.
    """
    if isinstance(arg_node, ast.Attribute):
        attr = arg_node.attr or ''
        if attr.endswith('_uuid') or 'uuid' in attr.lower():
            return True
        # Forbidden: .id suffix that doesn't contain 'uuid'
        if attr == 'id':
            return False
        return True  # other attribute accesses OK (defensive)
    elif isinstance(arg_node, ast.Name):
        name = arg_node.id or ''
        if 'uuid' in name.lower():
            return True
        return True  # local variable names are OK unless they look like .id
    elif isinstance(arg_node, ast.Call):
        # hash(...) is forbidden, getattr(user, "id") is forbidden
        func = arg_node.func
        if isinstance(func, ast.Name) and func.id in ('hash', 'getattr'):
            # getattr(user, "id") check: look for string literal "id" as second arg
            if func.id == 'getattr' and len(arg_node.args) >= 2:
                second = arg_node.args[1]
                if isinstance(second, ast.Constant) and second.value == 'id':
                    return False
            if func.id == 'hash':
                return False
        return True  # other calls OK (e.g. uuid4().hex)
    elif isinstance(arg_node, ast.Constant):
        # String literals (hardcoded UUIDs) are OK; numeric literals are not
        if isinstance(arg_node.value, int):
            return False
        return True
    return True  # conservative: unknown node types pass


class _ForbiddenAccessorInTelemetryArgsVisitor(ast.NodeVisitor):
    """REVIEWS HIGH-3 re-scoped visitor: inspect ONLY telemetry call argument subtrees.

    For each telemetry call, walks node.args and each node.keywords[i].value subtree
    via ast.walk.  If any descendant is a Call whose func.attr is in FORBIDDEN_ACCESSORS,
    records a violation.

    Does NOT scan the rest of the enclosing function — that is the load-bearing fix that
    prevents on_search_finished / export_results / export_comp_report from being
    false-flagged (HIGH-3).

    Also detects identify() callsites that pass non-_uuid identity sources (LOW-10).
    """

    def __init__(self):
        self.violations: list[tuple[int, str]] = []

    def visit_Call(self, node: ast.Call) -> None:
        if _is_telemetry_call(node):
            # Walk ONLY the argument/keyword-value subtrees
            arg_subtrees = list(node.args)
            arg_subtrees += [kw.value for kw in node.keywords]
            for subtree in arg_subtrees:
                for child in ast.walk(subtree):
                    if isinstance(child, ast.Call):
                        func = child.func
                        if isinstance(func, ast.Attribute):
                            if func.attr in FORBIDDEN_ACCESSORS:
                                accessor = func.attr
                                self.violations.append((
                                    child.lineno,
                                    f"{accessor}() inside telemetry call argument",
                                ))

        elif _is_identify_call(node):
            # LOW-10: check identify() first positional argument is a _uuid source
            if node.args:
                first_arg = node.args[0]
                if not _is_allowed_identity_source(first_arg):
                    self.violations.append((
                        node.lineno,
                        "identify() arg is not a _uuid identity source",
                    ))

        self.generic_visit(node)


def _scan_file(path: Path) -> list[tuple[int, str]]:
    """Parse one .py file and return (lineno, description) violations."""
    try:
        source = path.read_text(encoding='utf-8')
    except Exception as e:
        return [(0, f"IOError reading {path}: {e}")]
    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError as e:
        return [(0, f"SyntaxError: {e}")]
    visitor = _ForbiddenAccessorInTelemetryArgsVisitor()
    visitor.visit(tree)
    return visitor.violations


# ===========================================================================
# Tests
# ===========================================================================

def test_lint_rejects_synthetic_arg_violation():
    """Synthetic test: visitor catches a forbidden accessor INSIDE a telemetry call argument.

    telemetry.track('event', x=w.currentText()) MUST be flagged.
    """
    synthetic = textwrap.dedent("""\
        def bad(w, telemetry):
            telemetry.track('desktop_event', x=w.currentText())
    """)
    tree = ast.parse(synthetic)
    visitor = _ForbiddenAccessorInTelemetryArgsVisitor()
    visitor.visit(tree)
    assert visitor.violations, (
        "AST visitor failed to detect currentText() inside telemetry call argument — "
        "guard is vacuous (REVIEWS HIGH-3)."
    )
    assert any('currentText' in desc for _, desc in visitor.violations)


def test_lint_accepts_forbidden_accessor_outside_telemetry_args():
    """REVIEWS HIGH-3 re-scope: accessor outside telemetry args must NOT be flagged.

    This is the load-bearing test proving on_search_finished / export_results /
    export_comp_report style functions are NOT false-flagged.

    A function that calls w.currentText() in a NON-telemetry statement AND separately
    calls telemetry.track(...) with literal args must pass.
    """
    synthetic = textwrap.dedent("""\
        def on_search_finished(self, w, telemetry):
            # Non-telemetry use of forbidden accessor — must NOT be flagged
            mode_label = w.currentText()
            query = w.toPlainText()
            # Telemetry call uses only hardcoded literals — must pass
            telemetry.track('desktop_search_executed', search_mode='keyword', action='completed')
    """)
    tree = ast.parse(synthetic)
    visitor = _ForbiddenAccessorInTelemetryArgsVisitor()
    visitor.visit(tree)
    assert not visitor.violations, (
        f"Guard incorrectly flagged non-telemetry use of forbidden accessor (REVIEWS HIGH-3): "
        f"{visitor.violations}"
    )


def test_lint_accepts_clean_producer():
    """Clean producer (literals + dict lookup) must pass without violations."""
    synthetic = textwrap.dedent("""\
        _FMT_MAP = {'xlsx': 'export_xlsx', 'csv': 'export_csv'}
        def export_results(self, fmt, telemetry, session_id):
            telemetry.track('desktop_feature_opened', dialog_name='export', session_id=session_id)
            telemetry.track('desktop_feature_opened', action=_FMT_MAP.get(fmt))
    """)
    tree = ast.parse(synthetic)
    visitor = _ForbiddenAccessorInTelemetryArgsVisitor()
    visitor.visit(tree)
    assert not visitor.violations, (
        f"Clean producer with literals/dict-lookup flagged incorrectly: {visitor.violations}"
    )


def test_lint_rejects_identify_non_uuid():
    """LOW-10: telemetry.identify(user.id) must be flagged; identify(user._uuid) must pass."""
    # BAD: user.id
    bad_synthetic = textwrap.dedent("""\
        def bad_identify(user, telemetry):
            telemetry.identify(user.id)
    """)
    tree = ast.parse(bad_synthetic)
    visitor = _ForbiddenAccessorInTelemetryArgsVisitor()
    visitor.visit(tree)
    assert visitor.violations, (
        "identify(user.id) must be flagged as non-_uuid identity source (REVIEWS LOW-10)"
    )
    assert any('identify' in desc for _, desc in visitor.violations)

    # GOOD: user._uuid
    good_synthetic = textwrap.dedent("""\
        def good_identify(user, telemetry):
            telemetry.identify(user._uuid)
    """)
    tree2 = ast.parse(good_synthetic)
    visitor2 = _ForbiddenAccessorInTelemetryArgsVisitor()
    visitor2.visit(tree2)
    assert not visitor2.violations, (
        f"identify(user._uuid) must NOT be flagged (LOW-10): {visitor2.violations}"
    )


def test_no_dynamic_telemetry_strings_in_producers():
    """Production guard: scan TARGET_FILES for forbidden-accessor-in-telemetry-arg violations.

    All Phase 114 producers (genizah_app.py, gui_threads.py, desktop/result_dialog.py) must
    pass because they use only hardcoded string constants / the static _EXPORT_ACTION_BY_FMT
    map / user._uuid in telemetry/identify call arguments.

    on_search_finished / export_results / export_comp_report are NOT false-flagged because
    the guard is telemetry-argument-scoped (REVIEWS HIGH-3).
    """
    violations = []
    for path in TARGET_FILES:
        if not path.exists():
            continue
        rel = path.relative_to(REPO_ROOT).as_posix()
        file_violations = _scan_file(path)
        for lineno, description in file_violations:
            violations.append(f"{rel}:{lineno}: {description}")

    if violations:
        msg = (
            "Producer-layer D-17 guard: forbidden accessor found inside telemetry "
            "call argument (or identify() uses non-_uuid source) in:\n  "
            + "\n  ".join(violations[:50])
            + f"\n\nTotal violations: {len(violations)}"
            + "\n\nFix: ensure all telemetry/identify call arguments use only hardcoded "
              "string constants, the static fmt→action map, or user._uuid — never "
              "currentText()/windowTitle()/selectedFiles()/toPlainText()/text()/user.id. "
              "(D-04 / REVIEWS HIGH-3 + LOW-10)"
        )
        pytest.fail(msg)
