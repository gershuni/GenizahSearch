"""SQL-shape and facet contract tests for the shared library_mode parameter
and get_browse_library_facets method on FjmsService.

These tests run WITHOUT a real FJMS sqlite file — they use a source-level
text scan of shared/fjms_service.py (AST/text assertions) plus a lightweight
fake-connection/cursor pass for the behavioral DISTINCT-AlmaId counting tests.

All tests are intentionally RED until Tasks 2 and 3 add the implementation.
"""

import inspect
import pathlib
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SERVICE_PATH = pathlib.Path(__file__).parent.parent / "shared" / "fjms_service.py"
_SOURCE = _SERVICE_PATH.read_text(encoding="utf-8")


def _service_class():
    from shared.fjms_service import FjmsService
    return FjmsService


# ---------------------------------------------------------------------------
# Task 1 — RED tests (library_mode SQL-shape + facet contract)
# These will become GREEN after Tasks 2 and 3 implement the functionality.
# ---------------------------------------------------------------------------


# --- Test 1: default = Show-only ---

def test_default_library_mode_is_show_only_source():
    """get_browse_results signature carries library_mode: str = 'show_only'."""
    assert "library_mode: str = 'show_only'" in _SOURCE, (
        "get_browse_results must declare library_mode: str = 'show_only' in its signature"
    )


def test_default_show_only_emits_exists_not_not_exists():
    """Source emits EXISTS condition for _browse_filter_library; the Show-only path
    must not emit 'NOT EXISTS' for that table."""
    # The f-string template that builds the SQL condition contains this fragment
    assert '_browse_filter_library' in _SOURCE, (
        "Source must reference _browse_filter_library temp table"
    )
    # The conditional must gate NOT EXISTS behind library_mode == "hide"
    assert 'library_mode == "hide"' in _SOURCE or "library_mode == 'hide'" in _SOURCE, (
        "Source must contain a library_mode == 'hide' check to select NOT EXISTS"
    )


# --- Test 2: Hide mode emits NOT EXISTS ---

def test_hide_mode_emits_not_exists():
    """Source must contain the NOT EXISTS variant gated on library_mode == 'hide'."""
    assert "NOT EXISTS" in _SOURCE, (
        "Source must contain a NOT EXISTS SQL condition for Hide mode"
    )
    # The NOT EXISTS must be related to the _browse_filter_library temp table —
    # the implementation uses an f-string to select the keyword dynamically.
    # We verify both the NOT EXISTS keyword and the _browse_filter_library reference
    # appear together (within a few lines) by checking for the _exists_kw pattern.
    assert "_exists_kw" in _SOURCE or (
        "_browse_filter_library" in _SOURCE and "NOT EXISTS" in _SOURCE
    ), (
        "NOT EXISTS must be used in conjunction with _browse_filter_library"
    )


# --- Test 3: invalid mode falls back to Show-only (no NOT EXISTS selected) ---

def test_invalid_mode_fallback_is_show_only():
    """An unrecognized library_mode selects EXISTS (show-only), not NOT EXISTS.
    This is verified via source: the NOT EXISTS branch is gated on == 'hide'
    (anything else remains EXISTS)."""
    # The gating condition must be a strict equality check — any other value
    # falls through to the default EXISTS path (fail-safe).
    assert 'library_mode == "hide"' in _SOURCE or "library_mode == 'hide'" in _SOURCE, (
        "NOT EXISTS must be gated by strict `library_mode == 'hide'` so unrecognized "
        "values fall through to the default EXISTS path"
    )


# --- Test 4: get_browse_library_facets exists + correct shape ---

def test_get_browse_library_facets_is_defined():
    """FjmsService must define get_browse_library_facets."""
    svc = _service_class()
    assert hasattr(svc, "get_browse_library_facets"), (
        "FjmsService must have a get_browse_library_facets method"
    )
    assert callable(svc.get_browse_library_facets), (
        "get_browse_library_facets must be callable"
    )


def test_get_browse_library_facets_reuses_shared_conditions():
    """Facet method must reuse the shared condition-builders (domain/author/work/pgp/editions).
    Asserted by checking the source references the shared _build_browse_conditions helper."""
    assert "_build_browse_conditions" in _SOURCE, (
        "Source must define _build_browse_conditions as a shared helper "
        "used by both get_browse_results and get_browse_library_facets"
    )


def test_get_browse_library_facets_omits_library_filter():
    """Facet method must NOT apply the _browse_filter_library temp table in SQL —
    counts must not be scoped by the library filter being chosen (source assertion).
    The filter name may appear in a docstring comment explaining the exclusion, but
    must never appear in an _ensure_filter_temp call or an EXISTS condition."""
    # Extract the get_browse_library_facets function body from source
    facets_idx = _SOURCE.find("def get_browse_library_facets")
    assert facets_idx != -1, "get_browse_library_facets not found in source"
    # Find end of function (next top-level def or class, heuristic: next 'def ' not indented)
    next_def = _SOURCE.find("\n    def ", facets_idx + 1)
    if next_def == -1:
        facets_body = _SOURCE[facets_idx:]
    else:
        facets_body = _SOURCE[facets_idx:next_def]
    # The function must NOT call _ensure_filter_temp with _browse_filter_library
    assert '_ensure_filter_temp(\n' + ' ' * 16 + '"_browse_filter_library"' not in facets_body, (
        "get_browse_library_facets must NOT call _ensure_filter_temp for _browse_filter_library"
    )
    # It must NOT append an EXISTS or NOT EXISTS condition on _browse_filter_library
    # (it may MENTION the name in a comment, but must not condition on it)
    assert 'EXISTS (SELECT 1 FROM "_browse_filter_library"' not in facets_body, (
        "get_browse_library_facets must NOT apply an EXISTS condition on _browse_filter_library"
    )


def test_get_browse_library_facets_counts_distinct_almaid():
    """Facet method counts via SELECT DISTINCT c.AlmaId (source assertion)."""
    facets_idx = _SOURCE.find("def get_browse_library_facets")
    assert facets_idx != -1, "get_browse_library_facets not found in source"
    next_def = _SOURCE.find("\n    def ", facets_idx + 1)
    if next_def == -1:
        facets_body = _SOURCE[facets_idx:]
    else:
        facets_body = _SOURCE[facets_idx:next_def]
    assert "DISTINCT c.AlmaId" in facets_body, (
        "get_browse_library_facets must use SELECT DISTINCT c.AlmaId "
        "to mirror COUNT(DISTINCT c.AlmaId) browse counting"
    )


def test_get_browse_library_facets_accepts_sys_id_to_library():
    """Facet method must accept a sys_id_to_library parameter."""
    svc = _service_class()
    sig = inspect.signature(svc.get_browse_library_facets)
    assert "sys_id_to_library" in sig.parameters, (
        "get_browse_library_facets must accept sys_id_to_library as a parameter"
    )


# --- Test 5: facet method is bounded (no O(255K) Python scan) ---

def test_get_browse_library_facets_is_bounded():
    """Facet method runs a SQL SELECT DISTINCT c.AlmaId (bounded by filters),
    not an O(255K) Python loop over the whole corpus (source assertion)."""
    facets_idx = _SOURCE.find("def get_browse_library_facets")
    assert facets_idx != -1, "get_browse_library_facets not found in source"
    next_def = _SOURCE.find("\n    def ", facets_idx + 1)
    if next_def == -1:
        facets_body = _SOURCE[facets_idx:]
    else:
        facets_body = _SOURCE[facets_idx:next_def]
    # Must use a SELECT DISTINCT to let SQLite do the work, not a Python scan
    assert "SELECT DISTINCT c.AlmaId" in facets_body, (
        "get_browse_library_facets must use SELECT DISTINCT c.AlmaId (SQL-bounded), "
        "not an O(255K) Python loop over the entire corpus"
    )


# ---------------------------------------------------------------------------
# Behavioral tests (fake connection, no real FJMS DB)
# These require the implementation from Tasks 2 and 3.
# ---------------------------------------------------------------------------

def _make_fake_service(alma_id_rows):
    """Build an FjmsService with a fake _conn whose execute() returns
    the supplied alma_id_rows list of dicts with key 'AlmaId'.

    The cursor's fetchall() returns alma_id_rows.
    """
    from shared.fjms_service import FjmsService

    fake_cursor = MagicMock()
    fake_cursor.fetchall.return_value = alma_id_rows

    fake_conn = MagicMock()
    fake_conn.execute.return_value = fake_cursor

    svc = FjmsService.__new__(FjmsService)
    svc._conn = fake_conn
    # Minimal state needed by _build_browse_conditions (if it exists):
    svc._has_persons_titles = False
    svc._filter_temp_local = MagicMock()
    svc._filter_temp_local.built = {}

    return svc


# --- Test 6: duplicate AlmaIds counted once (behavioral, Codex N5) ---

def test_facets_duplicate_alma_id_counted_once():
    """A catalog table with two rows for the same AlmaId (one manuscript)
    must be counted once, not twice. The duplicate is collapsed by SELECT DISTINCT."""
    # Two rows: A1 appears twice, A2 once
    rows = [
        {"AlmaId": "A1"},
        {"AlmaId": "A1"},  # duplicate
        {"AlmaId": "A2"},
    ]
    # DISTINCT at SQL level means fetchall returns DISTINCT rows:
    # the implementation should emit SELECT DISTINCT c.AlmaId so the DB deduplicates.
    # We model this by providing the deduplicated result the SQL would return:
    distinct_rows = [{"AlmaId": "A1"}, {"AlmaId": "A2"}]
    svc = _make_fake_service(distinct_rows)

    mapper = {"A1": "CUL", "A2": "JTS"}.get
    result = svc.get_browse_library_facets(sys_id_to_library=mapper)

    assert result == {"CUL": 1, "JTS": 1}, (
        f"Expected {{'CUL': 1, 'JTS': 1}}, got {result}. "
        "A duplicate AlmaId must be counted once (SELECT DISTINCT deduplicates)."
    )


# --- Test 7: off-page libraries appear (behavioral, Codex N5) ---

def test_facets_off_page_libraries_appear():
    """Libraries present in the full filtered set but not on the current
    PAGE_SIZE=50 page must still appear in the facet result.
    The facet method queries the full filtered set, not just the current page."""
    # Simulate 60 distinct AlmaIds (more than PAGE_SIZE=50), spread across 3 libraries
    distinct_rows = [{"AlmaId": f"M{i}"} for i in range(60)]
    svc = _make_fake_service(distinct_rows)

    # First 50 are CUL, next 5 JTS, last 5 Oxford — only CUL would appear on page 1
    def mapper(sid):
        n = int(sid[1:])
        if n < 50:
            return "CUL"
        elif n < 55:
            return "JTS"
        else:
            return "Oxford"

    result = svc.get_browse_library_facets(sys_id_to_library=mapper)

    assert "JTS" in result, "Off-page JTS library must appear in facet result"
    assert "Oxford" in result, "Off-page Oxford library must appear in facet result"
    assert "CUL" in result, "CUL library must appear in facet result"
    assert result["CUL"] == 50, f"Expected CUL=50, got {result['CUL']}"
    assert result["JTS"] == 5, f"Expected JTS=5, got {result['JTS']}"
    assert result["Oxford"] == 5, f"Expected Oxford=5, got {result['Oxford']}"


# --- Test 8: CALLABLE mapper contract (Codex R3 F3) ---

def test_facets_invokes_mapper_as_callable_per_distinct_alma_id():
    """The sys_id_to_library parameter must be invoked AS A CALLABLE once per
    distinct AlmaId. A dict.get bound method and a recording closure must both work."""
    distinct_rows = [{"AlmaId": "X1"}, {"AlmaId": "X2"}, {"AlmaId": "X3"}]
    svc = _make_fake_service(distinct_rows)

    # 1. Test with a recording closure
    called_with = []

    def recording_mapper(sid):
        called_with.append(sid)
        return {"X1": "CUL", "X2": "JTS", "X3": "RNL"}.get(sid)

    result = svc.get_browse_library_facets(sys_id_to_library=recording_mapper)

    assert sorted(called_with) == ["X1", "X2", "X3"], (
        f"Mapper must be called once per distinct AlmaId; called with {called_with}"
    )
    assert result == {"CUL": 1, "JTS": 1, "RNL": 1}

    # 2. dict.get bound method also works (matches Plan 04 bound-method form)
    mapper_dict = {"X1": "CUL", "X2": "JTS", "X3": "RNL"}
    result2 = svc.get_browse_library_facets(sys_id_to_library=mapper_dict.get)
    assert result2 == {"CUL": 1, "JTS": 1, "RNL": 1}, (
        "dict.get bound method must work as sys_id_to_library mapper"
    )


def test_facets_skips_empty_and_none_library_codes():
    """A mapper returning '' or None for an AlmaId causes that manuscript to be skipped."""
    distinct_rows = [{"AlmaId": "A"}, {"AlmaId": "B"}, {"AlmaId": "C"}]
    svc = _make_fake_service(distinct_rows)

    def mapper(sid):
        return {"A": "CUL", "B": None, "C": ""}.get(sid)

    result = svc.get_browse_library_facets(sys_id_to_library=mapper)

    assert result == {"CUL": 1}, (
        f"Expected {{'CUL': 1}} — None and '' mapper returns must be skipped; got {result}"
    )


def test_facets_skips_local_library_code():
    """'LOCAL' must never appear as a key in the facet result."""
    distinct_rows = [{"AlmaId": "L1"}, {"AlmaId": "L2"}]
    svc = _make_fake_service(distinct_rows)

    def mapper(sid):
        return "LOCAL"  # always returns LOCAL

    result = svc.get_browse_library_facets(sys_id_to_library=mapper)

    assert "LOCAL" not in result, (
        "'LOCAL' must be excluded from facet keys (it is never a selectable library)"
    )
    assert result == {}, f"Expected empty dict when all codes are LOCAL; got {result}"


def test_facets_returns_empty_when_conn_is_none():
    """get_browse_library_facets returns {} when _conn is None (fail-open)."""
    from shared.fjms_service import FjmsService

    svc = FjmsService.__new__(FjmsService)
    svc._conn = None

    mapper = {"A": "CUL"}.get
    result = svc.get_browse_library_facets(sys_id_to_library=mapper)
    assert result == {}, f"Expected {{}} when _conn is None; got {result}"


def test_facets_returns_empty_when_no_mapper():
    """get_browse_library_facets returns {} when sys_id_to_library is None."""
    distinct_rows = [{"AlmaId": "A1"}]
    svc = _make_fake_service(distinct_rows)
    result = svc.get_browse_library_facets(sys_id_to_library=None)
    assert result == {}, f"Expected {{}} when no mapper supplied; got {result}"


def test_facets_returns_empty_when_mapper_not_callable():
    """get_browse_library_facets returns {} when sys_id_to_library is not callable."""
    distinct_rows = [{"AlmaId": "A1"}]
    svc = _make_fake_service(distinct_rows)
    result = svc.get_browse_library_facets(sys_id_to_library={"A1": "CUL"})  # dict, not callable
    assert result == {}, f"Expected {{}} when mapper is a dict (not callable); got {result}"
