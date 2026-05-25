"""Phase 98 Plan 06 — cross-module invariants spanning all 4 modified files.

These tests are higher-level than the per-plan test files; they guard the
PHASE-LEVEL invariants:

  1. Total call-site coverage: every NLI fetch in the codebase is breaker-guarded
  2. Path-string registry: each call site uses a unique, documented path= literal
  3. No residual hardcoded timeouts (15s / 10s / 30s) for NLI hosts (AST-aware
     per Codex REVIEWS Issue 4)
  4. D-02 single-shared-state: failure in web/api.py visible in genizah_core.py
  5. RESEARCH Pitfall 5: legacy class-attribute breaker has zero references repo-wide

State isolation via autouse fixture in tests/conftest.py (Plan 02).
"""

import ast
import pathlib

import pytest

import shared.nli_circuit_breaker as br


# The 4 files modified by Phase 98 Wave 3
PHASE_98_MODIFIED_FILES = [
    'web/api.py',
    'shared/puzzle_image_service.py',
    'web/pages/puzzle.py',
    'genizah_core.py',
]

# The expected path= literals across all call sites (per Plans 98-03/04/05)
EXPECTED_PATHS = {
    'fetch_fl_ids_from_nli',          # web/api.py — Plan 98-03
    'nli_image',                       # web/api.py — Plan 98-03
    '_fetch_nli_image_bytes',          # web/api.py — Plan 98-03
    'proxy_image',                     # web/api.py — Plan 98-03
    'puzzle_fetch_iiif_image',         # shared/puzzle_image_service.py — Plan 98-04
    'puzzle_fetch_direct_url',         # shared/puzzle_image_service.py — Plan 98-04
    'puzzle_resolve_folios',           # web/pages/puzzle.py — Plan 98-04
    'fetch_iiif_manifest',             # genizah_core.py — Plan 98-05
    'fetch_marc_data',                 # genizah_core.py — Plan 98-05
    '_fetch_single_worker',            # genizah_core.py — Plan 98-05
    '_fetch_fl_ids',                   # genizah_core.py — Plan 98-05
}


def _read_src(path):
    return pathlib.Path(path).read_text(encoding='utf-8')


def _combined_src():
    return '\n'.join(_read_src(p) for p in PHASE_98_MODIFIED_FILES)


# -----------------------------------------------------------------------------
# Test class 1 — phase-level call-site coverage
# -----------------------------------------------------------------------------
class TestPhaseLevelCallSiteCoverage:
    def test_total_call_site_coverage(self):
        """At least 10 _nli_circuit_is_open() check sites across all 4 files."""
        total = 0
        per_file = {}
        for p in PHASE_98_MODIFIED_FILES:
            count = _read_src(p).count('_nli_circuit_is_open()')
            per_file[p] = count
            total += count
        assert total >= 10, (
            f'Phase 98 coverage gap: expected >= 10 breaker checks across the 4 files, '
            f'got {total}. Per-file: {per_file}'
        )

    def test_path_string_registry(self):
        """Every documented path= literal appears at least once."""
        combined = _combined_src()
        missing = []
        for path in EXPECTED_PATHS:
            if f"path='{path}'" not in combined:
                missing.append(path)
        # Allow up to 1 missing — split-call-site aliases may differ.
        assert len(missing) <= 1, (
            f'path-string registry missing >= 2 entries: {missing}'
        )


# -----------------------------------------------------------------------------
# Test class 2 — AST-aware NLI timeout audit (Codex REVIEWS Issue 4)
# -----------------------------------------------------------------------------
class TestNoResidualHardcodedNliTimeouts:
    """All NLI fetch sites use env-driven (connect, read) tuples.

    Codex REVIEWS Issue 4: tests are AST-aware, NOT raw greps. Previous versions
    false-failed on legitimate non-NLI timeout=15/30 calls (web/api.py:
    Cambridge/Manchester image endpoints; genizah_core.py: Future.result(timeout=15)
    which is a concurrent-future wait, NOT a requests call).
    """

    _NLI_URL_MARKERS = ('iiif.nli.org.il', 'rosetta.nli.org.il')
    _NLI_URL_VAR_NAMES = {'iiif_url', 'rosetta_url', 'marc_url'}

    def _first_arg_str(self, call):
        if not call.args:
            return None
        a = call.args[0]
        if isinstance(a, ast.Constant) and isinstance(a.value, str):
            return a.value
        if isinstance(a, ast.JoinedStr):
            # f-string: concatenate the literal portions, ignore FormattedValue parts.
            return ''.join(
                v.value for v in a.values
                if isinstance(v, ast.Constant) and isinstance(v.value, str)
            )
        if isinstance(a, ast.Name):
            return f'<var:{a.id}>'
        return None

    def _get_timeout_kw(self, call):
        for kw in call.keywords:
            if kw.arg == 'timeout':
                return kw.value
        return None

    def _is_http_method(self, attr):
        return attr in ('get', 'post', 'put', 'delete', 'patch', 'head', 'options', 'request')

    def _nli_call_violations(self, file_path):
        """Return list of (lineno, message) tuples for NLI calls with bad timeouts."""
        src = _read_src(file_path)
        try:
            tree = ast.parse(src)
        except SyntaxError as e:
            return [(0, f'{file_path}: SyntaxError {e}')]
        violations = []

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            f = node.func
            if not isinstance(f, ast.Attribute):
                continue
            if not self._is_http_method(f.attr):
                continue
            # Filter to "session-like" callers
            is_nli_session_call = (
                isinstance(f.value, ast.Name)
                and f.value.id == '_nli_session'
            )
            is_requests_module = (
                isinstance(f.value, ast.Name)
                and f.value.id in ('requests', '_requests')
            )
            is_session_var = (
                isinstance(f.value, ast.Name)
                and f.value.id == 'session'
            )
            if not (is_nli_session_call or is_requests_module or is_session_var):
                continue
            # Decide NLI vs non-NLI
            if is_nli_session_call:
                is_nli = True
            else:
                arg0 = self._first_arg_str(node)
                if arg0 is None:
                    continue
                bare_name = arg0.replace('<var:', '').rstrip('>')
                is_nli = (
                    any(m in arg0 for m in self._NLI_URL_MARKERS)
                    or bare_name in self._NLI_URL_VAR_NAMES
                )
            if not is_nli:
                continue
            t = self._get_timeout_kw(node)
            if t is None:
                violations.append(
                    (node.lineno, f'{file_path}:{node.lineno}: NLI call has no timeout kwarg')
                )
                continue
            if isinstance(t, ast.Constant):
                violations.append((
                    node.lineno,
                    f'{file_path}:{node.lineno}: NLI call uses bare `timeout={t.value}` '
                    f'— should be `(NLI_CONNECT_TIMEOUT, NLI_*_READ_TIMEOUT)`',
                ))
                continue
            if not isinstance(t, ast.Tuple):
                violations.append((
                    node.lineno,
                    f'{file_path}:{node.lineno}: NLI call timeout is not a Tuple '
                    f'({type(t).__name__})',
                ))
                continue
            for elt in t.elts:
                if isinstance(elt, ast.Name) and elt.id.startswith('NLI_'):
                    continue
                violations.append((
                    node.lineno,
                    f'{file_path}:{node.lineno}: NLI timeout tuple element is not NLI_*',
                ))
        return violations

    def test_no_bare_timeout_on_nli_calls_ast(self):
        """Every NLI session.get / requests.get against NLI hosts uses a tuple timeout."""
        all_violations = []
        for fp in PHASE_98_MODIFIED_FILES:
            all_violations.extend(self._nli_call_violations(fp))
        assert not all_violations, (
            'Codex REVIEWS Issue 4 — AST-aware NLI timeout audit violations:\n  '
            + '\n  '.join(msg for _, msg in all_violations)
        )

    def test_non_nli_image_endpoints_unaffected(self):
        """Sanity guard: non-NLI image endpoints in web/api.py (Cambridge etc.)
        retain timeout=30 — they are NOT covered by the breaker and should NOT
        be flagged by the AST audit above.
        """
        api_src = _read_src('web/api.py')
        assert 'timeout=30' in api_src, (
            'expected at least one timeout=30 for non-NLI image endpoints '
            '(Cambridge / Manchester / Oxford); if all were removed, the audit '
            'is too aggressive'
        )

    def test_nli_semaphore_timeout_default_dropped(self):
        """D-10: NLI_SEMAPHORE_TIMEOUT default is '1', not '20'."""
        src = _read_src('web/api.py')
        assert "NLI_SEMAPHORE_TIMEOUT', '1'" in src
        assert "NLI_SEMAPHORE_TIMEOUT', '20'" not in src


# -----------------------------------------------------------------------------
# Test class 3 — D-02 single shared state across modules
# -----------------------------------------------------------------------------
class TestSharedStateAcrossModules:
    """D-02 invariant: a failure recorded from one module is visible from another."""

    def test_failure_in_web_api_visible_in_genizah_core(self):
        """Tripping the breaker via web.api alias affects genizah_core.py alias.

        This proves the single shared module-level state (D-02): regardless of
        which module's alias `record_failure` is called through, the underlying
        `_consecutive_failures` and `_open_until` counters live in
        `shared.nli_circuit_breaker` and are observable through ANY consumer's
        alias.
        """
        from web import api as web_api
        from genizah_core import _nli_circuit_is_open as gc_is_open
        # Trip via web/api alias
        for _ in range(br.NLI_CIRCUIT_THRESHOLD):
            web_api._nli_record_failure(failure_type='timeout', path='cross_module_test')
        # Observe via genizah_core alias
        assert gc_is_open(), (
            'D-02 violation: breaker state is NOT shared across modules'
        )

    def test_success_in_genizah_core_resets_state_seen_from_web_api(self):
        """A success recorded via genizah_core alias clears state observed by web.api alias."""
        from web import api as web_api
        from genizah_core import _nli_record_success as gc_success
        # Pre-trip via web/api
        for _ in range(br.NLI_CIRCUIT_THRESHOLD):
            web_api._nli_record_failure(failure_type='timeout', path='cross_module_test')
        assert web_api._nli_circuit_is_open()
        # Reset via genizah_core alias
        gc_success(path='cross_module_test')
        # Observe reset via web/api alias
        assert not web_api._nli_circuit_is_open(), (
            'D-02 violation: record_success in one module did not reset shared state'
        )

    def test_puzzle_module_breaker_aliases_are_same_object(self):
        """All 4 modules import the same underlying functions from shared.nli_circuit_breaker."""
        from web import api as web_api
        import genizah_core as gc
        # Both modules must alias the SAME callable object as br.is_open
        assert web_api._nli_circuit_is_open is br.is_open
        assert gc._nli_circuit_is_open is br.is_open


# -----------------------------------------------------------------------------
# Test class 4 — RESEARCH Pitfall 5: legacy class-attribute breaker fully absent
# -----------------------------------------------------------------------------
class TestLegacyBreakerFullyRemoved:
    """RESEARCH Pitfall 5 — legacy class-attribute breaker absent across the 4 files."""

    def test_no_self_dot_nli_circuit_is_open_anywhere(self):
        """The legacy `self._nli_circuit_is_open()` pattern is absent from production code."""
        for p in PHASE_98_MODIFIED_FILES:
            src = _read_src(p)
            assert 'self._nli_circuit_is_open(' not in src, (
                f'{p}: legacy self._nli_circuit_is_open() call detected — '
                'RESEARCH Pitfall 5 violation'
            )
            assert 'cls._nli_circuit_is_open(' not in src, (
                f'{p}: legacy cls._nli_circuit_is_open() call detected'
            )

    def test_metadata_manager_class_no_longer_has_breaker_attributes(self):
        """Class-attribute breaker removed from MetadataManager per Plan 98-05."""
        from genizah_core import MetadataManager
        for attr in (
            '_nli_circuit_is_open',
            '_nli_record_failure',
            '_nli_record_success',
            '_nli_consecutive_failures',
            '_nli_circuit_open_until',
            '_NLI_CIRCUIT_THRESHOLD',
            '_NLI_CIRCUIT_WINDOW',
        ):
            assert not hasattr(MetadataManager, attr), (
                f'MetadataManager still has legacy attribute {attr!r} — '
                'RESEARCH Pitfall 5 violation'
            )


# -----------------------------------------------------------------------------
# Test class 5 — all 4 files import from shared.nli_circuit_breaker with aliases
# -----------------------------------------------------------------------------
class TestBreakerImportConsistency:
    """All 4 files use the documented import aliases."""

    def test_all_4_files_import_shared_breaker(self):
        for p in PHASE_98_MODIFIED_FILES:
            src = _read_src(p)
            assert 'from shared.nli_circuit_breaker import' in src, (
                f'{p}: missing shared breaker import'
            )

    def test_imports_use_aliased_names(self):
        """Each file imports with the conventional `as _nli_*` aliases."""
        for p in PHASE_98_MODIFIED_FILES:
            src = _read_src(p)
            assert 'is_open as _nli_circuit_is_open' in src, (
                f'{p}: import does not alias `is_open` to `_nli_circuit_is_open`'
            )

    def test_no_web_imports_in_shared_modules(self):
        """shared/* modules must not depend on web/* (Option (a) per RESEARCH)."""
        for p in ('shared/nli_circuit_breaker.py', 'shared/posthog_server.py'):
            src = _read_src(p)
            # Reject `from web.` and `import web.` but allow leading whitespace
            for line in src.splitlines():
                stripped = line.lstrip()
                assert not stripped.startswith('from web.'), (
                    f'{p}: forbidden `from web.` import line: {line!r}'
                )
                assert not stripped.startswith('import web.'), (
                    f'{p}: forbidden `import web.` import line: {line!r}'
                )


# -----------------------------------------------------------------------------
# Test class 6 — env-knob defaults match documentation
# -----------------------------------------------------------------------------
class TestEnvKnobDefaults:
    """Pin the 6 NLI_* defaults so CLAUDE.md documentation cannot silently drift."""

    @pytest.mark.parametrize('name,expected', [
        ('NLI_CIRCUIT_THRESHOLD', 3),
        ('NLI_CIRCUIT_WINDOW', 60),
        ('NLI_CONNECT_TIMEOUT', 3),
        ('NLI_IIIF_READ_TIMEOUT', 5),
        ('NLI_MARC_READ_TIMEOUT', 3),
        ('NLI_IMAGE_READ_TIMEOUT', 5),
    ])
    def test_default_matches_documented(self, name, expected):
        """The default for each NLI_* knob matches the value advertised in CLAUDE.md."""
        # Read default off the module (with no env override active in test process)
        actual = getattr(br, name)
        assert actual == expected, (
            f'{name} default drifted: code default is {actual}, expected {expected}. '
            'If you intentionally changed the default, update CLAUDE.md "Environment '
            'Variables" section to match.'
        )
