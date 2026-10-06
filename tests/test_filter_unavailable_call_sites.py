# -*- coding: utf-8 -*-
"""Source guards (a backstop) for the filter-unavailable call sites (#17).

The behaviour is tested elsewhere: the service, the API endpoints, the web
count, the desktop worker and dialog, the main-window methods, and the web
submit handlers (render smoke). These guards only stop a NEW or rewritten
call site from skipping the handling:

1. Every FilterCountWorker built in genizah_app.py or desktop/*.py has its
   failure handled in the same function: either ``.failed.connect(`` or the
   main window's ``_connect_filter_worker(`` (which connects both signals
   with a generation check).
2. The web /search and /parallels submit handlers catch FilterUnavailable
   around the pre-search lookup, and their lookup closure has no
   ``is_available()`` short-cut that turns a missing sidecar into "no
   restriction".
3. /parallels passes an ``on_state`` renderer to recompute_filter_count, so
   a failed count is visible there too.
4. The history restores copy the saved filter dict instead of aliasing it.
5. The session restore passes BOTH saved filter dicts (pre-search and
   post-search measurement) through drop_unavailable_measurement_filters,
   so a bound the open catalog is known to lack is dropped with a notice
   instead of failing (pre) or hiding every row (post).
"""
import ast
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
DESKTOP_SOURCES = [REPO / "genizah_app.py", *sorted((REPO / "desktop").glob("*.py"))]


def _parse(path):
    src = path.read_text(encoding="utf-8")
    return src, ast.parse(src)


def _enclosing_function(tree, target):
    best = None
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.lineno <= target.lineno <= (node.end_lineno or node.lineno):
                if best is None or node.lineno >= best.lineno:
                    best = node
    return best


def test_every_filter_count_worker_handles_failure():
    missing, found = [], 0
    for path in DESKTOP_SOURCES:
        src, tree = _parse(path)
        for node in ast.walk(tree):
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                    and node.func.id == "FilterCountWorker"):
                found += 1
                fn = _enclosing_function(tree, node)
                body = ast.get_source_segment(src, fn) or ""
                if ".failed.connect(" not in body and "_connect_filter_worker(" not in body:
                    missing.append(f"{path.name}:{node.lineno} in {fn.name}")
    assert found >= 5, f"expected at least 5 FilterCountWorker sites, found {found}; update this guard"
    assert not missing, "FilterCountWorker without a failure handler: " + ", ".join(missing)


def _handlers_around(tree, node):
    out = []
    for t in ast.walk(tree):
        if isinstance(t, ast.Try):
            for stmt in t.body:
                if stmt.lineno <= node.lineno <= (stmt.end_lineno or stmt.lineno):
                    out.extend(t.handlers)
    return out


def test_web_submit_handlers_catch_filter_unavailable():
    problems = []
    for rel in ("web/pages/search.py", "web/pages/parallels.py"):
        src, tree = _parse(REPO / rel)
        sites = [
            n for n in ast.walk(tree)
            if isinstance(n, ast.Await) and isinstance(n.value, ast.Call)
            and isinstance(n.value.func, ast.Attribute) and n.value.func.attr == "io_bound"
            and n.value.args and isinstance(n.value.args[0], ast.Name)
            and n.value.args[0].id == "_compute_restrict"
        ]
        assert sites, f"{rel}: pre-search lookup call not found; update this guard"
        for site in sites:
            ok = [h for h in _handlers_around(tree, site)
                  if h.type is not None and "FilterUnavailable" in ast.unparse(h.type)]
            if not ok:
                problems.append(f"{rel}:{site.lineno} not inside try/except FilterUnavailable")
        for fn in ast.walk(tree):
            if isinstance(fn, ast.FunctionDef) and fn.name == "_compute_restrict":
                if "is_available" in ast.unparse(fn):
                    problems.append(f"{rel}:{fn.lineno} _compute_restrict short-cuts on is_available()")
    assert not problems, "\n".join(problems)


def test_parallels_count_shows_its_error_state():
    src, tree = _parse(REPO / "web/pages/parallels.py")
    calls = [n for n in ast.walk(tree)
             if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
             and n.func.id == "recompute_filter_count"]
    assert calls, "recompute_filter_count call not found; update this guard"
    for c in calls:
        assert any(k.arg == "on_state" for k in c.keywords) or len(c.args) >= 3, (
            f"parallels.py:{c.lineno} recompute_filter_count has no on_state; "
            "a failed count is invisible on /parallels")


def _assigns_to(node, name):
    targets = []
    for t in node.targets:
        if isinstance(t, ast.Name):
            targets.append(t.id)
        elif isinstance(t, ast.Tuple):
            targets.extend(e.id for e in t.elts if isinstance(e, ast.Name))
    return name in targets


def test_history_restores_copy_the_saved_filters():
    src, tree = _parse(REPO / "genizah_app.py")
    problems, seen = [], set()
    for fn in ast.walk(tree):
        if isinstance(fn, ast.FunctionDef) and fn.name in (
                "_restore_regular_search_from_state", "_restore_comp_search_from_state"):
            for node in ast.walk(fn):
                if isinstance(node, ast.Assign) and _assigns_to(node, "psf"):
                    seen.add(fn.name)
                    text = ast.unparse(node.value)
                    if not (text.startswith("dict(")
                            or "drop_unavailable_measurement_filters" in text):
                        problems.append(f"genizah_app.py:{node.lineno} {fn.name}: "
                                        "psf aliases the history entry")
    assert seen == {"_restore_regular_search_from_state", "_restore_comp_search_from_state"}, (
        f"history restore assignments not found ({seen}); update this guard")
    assert not problems, "\n".join(problems)


def test_session_restore_drops_known_unsupported_saved_filters():
    src, tree = _parse(REPO / "genizah_app.py")
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.FunctionDef) and n.name == "_restore_session")
    wrapped = set()
    for node in ast.walk(fn):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == "drop_unavailable_measurement_filters"):
            arg = ast.unparse(node.args[0]) if node.args else ""
            for key in ("pre_search_filters", "post_measurement_filters"):
                if f"'{key}'" in arg:
                    wrapped.add(key)
    assert wrapped == {"pre_search_filters", "post_measurement_filters"}, (
        f"session restore reads saved filters without dropping known-unsupported keys: {wrapped}")
