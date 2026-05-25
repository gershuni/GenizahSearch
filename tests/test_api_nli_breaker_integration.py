"""Phase 98 Plan 03 — integration tests for web/api.py NLI breaker wiring.

Covers D-10, D-11, D-12, D-14..D-18 at the call-site level. The unit-level
breaker behavior is covered by tests/test_nli_circuit_breaker.py (Plan 02);
this file verifies the CALL-SITE WIRING in web/api.py is correct.

State isolation: the autouse fixture in tests/conftest.py
(_reset_nli_breaker_state from Plan 02) automatically resets the breaker
before each test. NO additional fixtures needed here.
"""

import ast
import pathlib
import time
from unittest.mock import patch

import pytest
import requests as _requests
from fastapi import FastAPI

import shared.nli_circuit_breaker as br


# Helpers — mock response objects
class _MockResponse:
    def __init__(self, status_code=200, content=b'', headers=None, text='', json_data=None):
        self.status_code = status_code
        self.content = content
        self.headers = headers or {}
        self.text = text
        self._json_data = json_data or {}

    def json(self):
        return self._json_data


@pytest.fixture(scope='module', autouse=True)
def _initialize_api_routes():
    """Phase 98 Plan 03: init_api_routes() populates web.api._api_test_seam
    with the closure-encapsulated NLI helpers (fetch_fl_ids_from_nli /
    _fetch_nli_image_bytes). Tests reach the breaker-guarded paths via
    that seam. Mounted on a throwaway FastAPI app so the NiceGUI singleton
    is not polluted across the test session.

    Persistence isolation: patch _save_nli_persistent_cache to a no-op so the
    on-disk cache file (Config.INDEX_DIR/nli_fl_ids_cache.json) is NOT polluted
    with test sys_ids. Otherwise a 200 success path persists 'sysid_recovery_*'
    to disk and breaks subsequent test runs.
    """
    from web.api import init_api_routes
    bare = FastAPI()
    init_api_routes(app_override=bare)
    # Replace the persistence helper at module scope so test sys_ids never hit disk.
    with patch('web.api._save_nli_persistent_cache', lambda *a, **kw: None):
        yield


@pytest.fixture(autouse=True)
def _clear_nli_in_memory_cache():
    """Per-test: clear the in-memory NLI cache so 200-success entries from a
    previous test do not satisfy lookups in the current test.

    The cache is captured in the closure of fetch_fl_ids_from_nli; we reach
    it through __closure__/co_freevars rather than refactoring the production
    code shape (which would expand Plan 03's blast radius).
    """
    from web.api import _api_test_seam
    fn = _api_test_seam.get('fetch_fl_ids_from_nli')
    if fn is not None and fn.__closure__:
        closure_vars = dict(zip(fn.__code__.co_freevars, fn.__closure__))
        nli_cache = closure_vars.get('_nli_cache')
        nli_cache_time = closure_vars.get('_nli_cache_time')
        nli_cache_lock = closure_vars.get('_nli_cache_lock')
        if nli_cache is not None and nli_cache_time is not None and nli_cache_lock is not None:
            with nli_cache_lock.cell_contents:
                nli_cache.cell_contents.clear()
                nli_cache_time.cell_contents.clear()
    yield


def _api_fetch_fl_ids_from_nli(*args, **kwargs):
    """Call the closure-encapsulated fetch_fl_ids_from_nli via the test seam."""
    from web.api import _api_test_seam
    return _api_test_seam['fetch_fl_ids_from_nli'](*args, **kwargs)


class TestCircuitGuardsInFetchFlIdsFromNli:
    """D-11, D-12: circuit check BEFORE and AFTER semaphore acquisition."""

    def test_circuit_check_before_semaphore(self):
        """D-11: when breaker is open, do not even call _nli_session.get."""
        from web import api as api_mod

        # Trip the breaker via the shared module
        for _ in range(br.NLI_CIRCUIT_THRESHOLD):
            br.record_failure(failure_type='timeout', path='preload')
        assert br.is_open()

        # Now call into web.api — should short-circuit WITHOUT touching the session
        get_calls = {'n': 0}

        def fake_get(*args, **kwargs):
            get_calls['n'] += 1
            raise AssertionError('breaker was open — _nli_session.get must NOT be called')

        with patch.object(api_mod._nli_session, 'get', side_effect=fake_get):
            start = time.monotonic()
            result = _api_fetch_fl_ids_from_nli('test_sysid_d11')
            elapsed = time.monotonic() - start

        assert result == []
        assert get_calls['n'] == 0, 'D-11 violation: session.get was called while breaker open'
        assert elapsed < 0.5, f'D-11 short-circuit was slow: {elapsed:.2f}s'

    def test_circuit_check_after_semaphore_releases_slot(self):
        """D-12 + RESEARCH Pitfall 3: breaker flips during semaphore wait; slot released."""
        from web import api as api_mod

        # Track is_open call count to flip mid-call
        is_open_calls = {'n': 0}

        def flipping_is_open():
            is_open_calls['n'] += 1
            # Return False on FIRST call (pre-acquire passes), True on subsequent
            return is_open_calls['n'] > 1

        # Capture semaphore baseline
        semaphore = api_mod._nli_semaphore

        with patch('web.api._nli_circuit_is_open', side_effect=flipping_is_open):
            result = _api_fetch_fl_ids_from_nli('test_sysid_d12')

        assert result == []
        assert is_open_calls['n'] >= 2, 'D-12 violation: post-acquire re-check not invoked'

        # Critical invariant: semaphore slot was released even on short-circuit path.
        # We probe by trying to acquire NLI_MAX_CONCURRENT_FETCHES slots in a row
        # without timeout — should succeed (proving none are leaked).
        slots_acquired = 0
        for _ in range(api_mod.NLI_MAX_CONCURRENT_FETCHES):
            if semaphore.acquire(blocking=False):
                slots_acquired += 1
        # Release everything we grabbed
        for _ in range(slots_acquired):
            semaphore.release()

        assert slots_acquired == api_mod.NLI_MAX_CONCURRENT_FETCHES, (
            f'semaphore leaked slots: acquired {slots_acquired} / '
            f'{api_mod.NLI_MAX_CONCURRENT_FETCHES} expected — Pitfall 3 violation'
        )


class TestFailureTypingViaApi:
    """D-06: 5xx, 429, Timeout, ConnectionError trip the breaker. 404 does NOT (D-07)."""

    def test_5xx_response_trips_breaker(self):
        from web import api as api_mod

        def fake_get(*args, **kwargs):
            return _MockResponse(status_code=502)

        # Disable per-sys_id negative cache so the same sys_id can fail repeatedly
        with patch.object(api_mod._nli_session, 'get', side_effect=fake_get), \
             patch.object(api_mod, 'NLI_FAIL_CACHE_TTL', 0):
            for i in range(br.NLI_CIRCUIT_THRESHOLD):
                _api_fetch_fl_ids_from_nli(f'sysid_5xx_{i}')

        assert br.is_open(), '502 responses should trip the breaker'

    def test_429_response_trips_breaker(self):
        from web import api as api_mod

        def fake_get(*args, **kwargs):
            return _MockResponse(status_code=429)

        with patch.object(api_mod._nli_session, 'get', side_effect=fake_get), \
             patch.object(api_mod, 'NLI_FAIL_CACHE_TTL', 0):
            for i in range(br.NLI_CIRCUIT_THRESHOLD):
                _api_fetch_fl_ids_from_nli(f'sysid_429_{i}')

        assert br.is_open(), '429 responses should trip the breaker (rate-limited)'

    def test_404_does_not_trip_breaker(self):
        """D-07: 404 → per-sys_id negative cache only, NO breaker increment."""
        from web import api as api_mod

        def fake_get(*args, **kwargs):
            return _MockResponse(status_code=404)

        # Call many times, each with a different sys_id (negative cache is per-key)
        with patch.object(api_mod._nli_session, 'get', side_effect=fake_get):
            for i in range(5):
                _api_fetch_fl_ids_from_nli(f'sysid_404_{i}')

        snap = br._state_snapshot()
        assert snap['consecutive_failures'] == 0, (
            f'D-07 violation: 404 incremented breaker counter to '
            f'{snap["consecutive_failures"]}'
        )
        assert not br.is_open(), 'D-07 violation: 404 should NOT trip breaker'

    def test_timeout_exception_trips_breaker(self):
        from web import api as api_mod

        def fake_get(*args, **kwargs):
            raise _requests.exceptions.ReadTimeout('simulated NLI timeout')

        with patch.object(api_mod._nli_session, 'get', side_effect=fake_get), \
             patch.object(api_mod, 'NLI_FAIL_CACHE_TTL', 0):
            for i in range(br.NLI_CIRCUIT_THRESHOLD):
                _api_fetch_fl_ids_from_nli(f'sysid_timeout_{i}')

        assert br.is_open()

    def test_connection_error_trips_breaker(self):
        from web import api as api_mod

        def fake_get(*args, **kwargs):
            raise _requests.exceptions.ConnectionError('simulated connection refused')

        with patch.object(api_mod._nli_session, 'get', side_effect=fake_get), \
             patch.object(api_mod, 'NLI_FAIL_CACHE_TTL', 0):
            for i in range(br.NLI_CIRCUIT_THRESHOLD):
                _api_fetch_fl_ids_from_nli(f'sysid_conn_{i}')

        assert br.is_open()

    def test_successful_200_resets_counter(self):
        """D-08: a successful 200 IIIF response resets consecutive_failures to 0.

        Note: each fetch_fl_ids_from_nli call may hit BOTH the IIIF manifest
        and the MARC fallback, so a single failing call typically increments
        the counter by 2 (IIIF timeout + MARC timeout). We don't assert the
        exact count after failures — we only assert that 200 resets it to 0.
        """
        from web import api as api_mod

        call_n = {'n': 0}

        def fake_get(url, *args, **kwargs):
            call_n['n'] += 1
            # First 2 GET calls fail (1 fetch_fl_ids call = IIIF + MARC fallback)
            if call_n['n'] <= 2:
                raise _requests.exceptions.ReadTimeout('initial failure')
            # 3rd GET returns a valid IIIF manifest with 1 FL id
            return _MockResponse(
                status_code=200,
                json_data={
                    'sequences': [{
                        'canvases': [{
                            'images': [{
                                'resource': {'service': {'@id': 'https://x/FL12345/...'}}
                            }]
                        }]
                    }],
                },
            )

        with patch.object(api_mod._nli_session, 'get', side_effect=fake_get), \
             patch.object(api_mod, 'NLI_FAIL_CACHE_TTL', 0):
            _api_fetch_fl_ids_from_nli('sysid_recovery_1')
            failures_after_first = br._state_snapshot()['consecutive_failures']
            assert failures_after_first >= 1, (
                'expected at least 1 failure recorded after timeout call; '
                f'got {failures_after_first}'
            )
            # Second call should succeed (IIIF 200) and reset counter to 0.
            result = _api_fetch_fl_ids_from_nli('sysid_recovery_2')
            assert result == ['12345']
            assert br._state_snapshot()['consecutive_failures'] == 0, (
                '200 success should reset counter (D-08)'
            )


class TestStaticInvariantsInWebApi:
    """Source-level audits — cheap regression guards."""

    def _read_api_source(self):
        return pathlib.Path('web/api.py').read_text(encoding='utf-8')

    def test_nli_semaphore_timeout_default_is_one(self):
        """D-10: default '20' → '1'."""
        src = self._read_api_source()
        assert "NLI_SEMAPHORE_TIMEOUT', '1'" in src, (
            'D-10 violation: NLI_SEMAPHORE_TIMEOUT default not dropped to 1'
        )
        assert "NLI_SEMAPHORE_TIMEOUT', '20'" not in src, (
            'D-10 violation: old NLI_SEMAPHORE_TIMEOUT default 20 still present'
        )

    def test_breaker_import_present(self):
        src = self._read_api_source()
        assert 'from shared.nli_circuit_breaker import' in src

    def test_circuit_check_count_at_least_8(self):
        """D-13 + D-14..D-18 + Codex REVIEW Issue 3 fallback rechecks: every call
        site checks is_open() AND each fallback branch (MARC, Rosetta, per-_try_fl)
        rechecks. Expected count: 5 entry checks + 3 fallback rechecks = 8.
        """
        src = self._read_api_source()
        count = src.count('_nli_circuit_is_open()')
        assert count >= 8, (
            f'expected >= 8 circuit checks in web/api.py: 5 entry checks per call '
            f'site (D-11/D-12/D-16/D-17/D-18) + 3 fallback rechecks per Codex '
            f'REVIEW Issue 3 (MARC fallback, Rosetta fallback, per-_try_fl iteration). '
            f'Got {count}.'
        )

    def test_no_hardcoded_timeout_in_nli_paths_ast(self):
        """D-14..D-18 — Codex REVIEW Issue 4: AST-aware audit.

        Previous version used raw `src.count('timeout=15')` which false-flagged
        legitimate non-NLI image endpoints at web/api.py:1037, :1105, :1162, :1280
        (all use timeout=30 for Cambridge/Manchester/Oxford fetches — NOT NLI).

        New version: parse web/api.py with `ast`, find every `Call` whose function
        is `requests.get` / `requests.post` / `_nli_session.get` / `_nli_session.post`,
        and inspect each call's `url` arg + `timeout` kwarg. A call is "NLI" iff:
          - It uses `_nli_session.*` (dedicated session — always NLI), OR
          - The first positional arg is a string literal / f-string containing
            `iiif.nli.org.il` or `rosetta.nli.org.il`, OR
          - It uses a local variable named `iiif_url` / `rosetta_url` /
            `marc_url` (project convention for NLI URLs).

        Every NLI call MUST use a `timeout=(connect, read)` tuple kwarg with names
        starting `NLI_` — not a bare `timeout=15` / `timeout=10` / `timeout=30`.

        Non-NLI calls are exempt — Cambridge/Manchester/Oxford may use timeout=30.
        """
        src = self._read_api_source()
        tree = ast.parse(src)
        violations = []

        # NLI-indicator helpers
        NLI_URL_MARKERS = ('iiif.nli.org.il', 'rosetta.nli.org.il')
        NLI_URL_VAR_NAMES = {'iiif_url', 'rosetta_url', 'marc_url'}

        def _is_nli_session_call(call: ast.Call) -> bool:
            """True iff `call.func` is `_nli_session.get/post/...` (network call,
            NOT a config call like `_nli_session.mount(...)`)."""
            f = call.func
            return (
                isinstance(f, ast.Attribute)
                and isinstance(f.value, ast.Name)
                and f.value.id == '_nli_session'
                and f.attr in ('get', 'post', 'put', 'delete', 'patch', 'head', 'options', 'request')
            )

        def _is_requests_get_or_post(call: ast.Call) -> bool:
            f = call.func
            return (
                isinstance(f, ast.Attribute)
                and isinstance(f.value, ast.Name)
                and f.value.id == 'requests'
                and f.attr in ('get', 'post')
            )

        def _first_arg_str(call: ast.Call):
            """Return the first positional arg as a string if it's a Constant str or
            JoinedStr (f-string) we can inspect, else return a `<var:NAME>` marker
            if it's a Name reference."""
            if not call.args:
                return None
            arg = call.args[0]
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                return arg.value
            if isinstance(arg, ast.JoinedStr):
                # Concatenate the Constant parts (Constants only — we just need
                # to spot the host)
                parts = []
                for v in arg.values:
                    if isinstance(v, ast.Constant) and isinstance(v.value, str):
                        parts.append(v.value)
                return ''.join(parts)
            if isinstance(arg, ast.Name):
                return f'<var:{arg.id}>'
            return None

        def _get_timeout_kwarg(call: ast.Call):
            for kw in call.keywords:
                if kw.arg == 'timeout':
                    return kw.value
            return None

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            is_nli_session = _is_nli_session_call(node)
            is_requests = _is_requests_get_or_post(node)
            if not (is_nli_session or is_requests):
                continue
            # Determine NLI vs non-NLI for `requests.get` calls
            if is_nli_session:
                is_nli = True
            else:
                arg0 = _first_arg_str(node)
                if arg0 is None:
                    continue  # can't tell — skip
                # NLI iff URL string contains an NLI marker OR the arg is a var
                # named iiif_url / rosetta_url / marc_url
                stripped_var = arg0.replace('<var:', '').rstrip('>') if arg0.startswith('<var:') else None
                is_nli = (
                    any(m in arg0 for m in NLI_URL_MARKERS)
                    or (stripped_var in NLI_URL_VAR_NAMES)
                )
            if not is_nli:
                continue
            # NLI call: timeout kwarg MUST be a Tuple of two values whose names
            # start with NLI_ (e.g., NLI_CONNECT_TIMEOUT, NLI_IIIF_READ_TIMEOUT).
            timeout = _get_timeout_kwarg(node)
            if timeout is None:
                violations.append(f'line {node.lineno}: NLI call has no timeout kwarg')
                continue
            if isinstance(timeout, ast.Constant):
                violations.append(
                    f'line {node.lineno}: NLI call uses bare `timeout={timeout.value}` '
                    f'— should be `timeout=(NLI_CONNECT_TIMEOUT, NLI_*_READ_TIMEOUT)`'
                )
                continue
            # IfExp is allowed if BOTH branches resolve to NLI_* tuples or to a
            # legitimate non-NLI fallback. For the proxy_image case the timeout
            # is computed via `timeout = (NLI_CONNECT_TIMEOUT, NLI_IMAGE_READ_TIMEOUT) if is_nli_host else 15`
            # — but that's an assignment, not a kwarg expression. We only see Name
            # references in the kwarg.
            if isinstance(timeout, ast.Name):
                # A variable reference (e.g., `timeout=timeout`). We trust it.
                continue
            if not isinstance(timeout, ast.Tuple):
                violations.append(
                    f'line {node.lineno}: NLI call timeout kwarg is not a Tuple '
                    f'({type(timeout).__name__})'
                )
                continue
            # Ensure tuple elements are NLI_* Name nodes
            for elt in timeout.elts:
                if isinstance(elt, ast.Name) and elt.id.startswith('NLI_'):
                    continue
                violations.append(
                    f'line {node.lineno}: NLI call timeout tuple has non-NLI_ element '
                    f'({ast.dump(elt)})'
                )

        assert not violations, (
            'NLI timeout audit failures (Codex REVIEW Issue 4):\n  '
            + '\n  '.join(violations)
        )

    def test_bounded_timeout_tuples_present(self):
        """Verify the env-driven tuple timeouts are used."""
        src = self._read_api_source()
        assert 'timeout=(NLI_CONNECT_TIMEOUT, NLI_IIIF_READ_TIMEOUT)' in src
        assert 'timeout=(NLI_CONNECT_TIMEOUT, NLI_MARC_READ_TIMEOUT)' in src
        assert 'timeout=(NLI_CONNECT_TIMEOUT, NLI_IMAGE_READ_TIMEOUT)' in src

    def test_record_failure_typed_correctly(self):
        """D-06: failure_type literal values appear in source."""
        src = self._read_api_source()
        assert "failure_type='5xx'" in src
        assert "failure_type='429'" in src
        assert "failure_type='timeout'" in src
        assert "failure_type='connection_error'" in src

    def test_specific_exception_handlers_replace_broad_except(self):
        """RESEARCH Pitfall 7: bare `except Exception` replaced with typed exceptions."""
        src = self._read_api_source()
        # The breaker call sites use specific handlers
        assert (
            'except (requests.exceptions.Timeout, requests.exceptions.ConnectionError)' in src
            or 'except requests.exceptions.Timeout' in src
        ), 'Pitfall 7 violation: NLI fetch sites should use specific exception classes'
