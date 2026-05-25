"""Phase 98 Plan 04 — integration tests for puzzle NLI breaker wiring.

Covers D-19 (shared/puzzle_image_service.py::_fetch_iiif_image),
D-20 (shared/puzzle_image_service.py::_fetch_direct_url host-conditional),
D-21 (web/pages/puzzle.py::_resolve_folios NLI manifest fetch).

The autouse fixture from tests/conftest.py (Plan 02 Wave 2) resets breaker state
before AND after each test, so module-level state in shared.nli_circuit_breaker
does not bleed between tests.
"""

import pathlib
import re
from unittest.mock import patch

import requests as _requests

import shared.nli_circuit_breaker as br


class _MockResponse:
    def __init__(self, status_code=200, content=b'', headers=None, text='', json_data=None):
        self.status_code = status_code
        self.content = content
        self.headers = headers or {}
        self.text = text
        self._json_data = json_data or {}

    def json(self):
        return self._json_data


def _make_puzzle_service(tmp_path):
    """Construct a minimal PuzzleImageService with a tmp cache dir."""
    from shared.puzzle_image_service import PuzzleImageService
    return PuzzleImageService(cache_dir=tmp_path)


# ---------------------------------------------------------------------------
# D-19: _fetch_iiif_image — unconditional NLI guard
# ---------------------------------------------------------------------------
class TestFetchIiifImageBreakerGuard:
    """D-19: _fetch_iiif_image always targets NLI; unconditional breaker guard."""

    def test_short_circuits_when_breaker_open(self, tmp_path):
        svc = _make_puzzle_service(tmp_path)
        # Trip the breaker
        for _ in range(br.NLI_CIRCUIT_THRESHOLD):
            br.record_failure('timeout', 'preload')
        assert br.is_open()

        get_calls = {'n': 0}

        def fake_get(*args, **kwargs):
            get_calls['n'] += 1
            raise AssertionError('breaker was open — requests.get must NOT be called')

        with patch('shared.puzzle_image_service.requests.get', side_effect=fake_get):
            result = svc._fetch_iiif_image('12345', 600)

        assert result is None
        assert get_calls['n'] == 0, 'D-19 violation: requests.get called while breaker open'

    def test_success_resets_breaker_counter(self, tmp_path):
        svc = _make_puzzle_service(tmp_path)
        # Pre-load 2 failures (sub-threshold)
        br.record_failure('timeout', 'preload')
        br.record_failure('timeout', 'preload')

        def fake_get(*args, **kwargs):
            return _MockResponse(status_code=200, content=b'x' * 500)

        with patch('shared.puzzle_image_service.requests.get', side_effect=fake_get):
            result = svc._fetch_iiif_image('12345', 600)

        assert result == b'x' * 500
        snap = br._state_snapshot()
        assert snap['consecutive_failures'] == 0, 'D-08: success should reset counter'

    def test_timeout_records_typed_failure(self, tmp_path):
        svc = _make_puzzle_service(tmp_path)

        def fake_get(*args, **kwargs):
            raise _requests.exceptions.ReadTimeout('simulated NLI timeout')

        with patch('shared.puzzle_image_service.requests.get', side_effect=fake_get):
            result = svc._fetch_iiif_image('12345', 600)

        assert result is None
        snap = br._state_snapshot()
        assert snap['consecutive_failures'] == 1, 'timeout should increment breaker counter'

    def test_connection_error_records_typed_failure(self, tmp_path):
        svc = _make_puzzle_service(tmp_path)

        def fake_get(*args, **kwargs):
            raise _requests.exceptions.ConnectionError('simulated NLI conn refused')

        with patch('shared.puzzle_image_service.requests.get', side_effect=fake_get):
            result = svc._fetch_iiif_image('12345', 600)

        assert result is None
        snap = br._state_snapshot()
        assert snap['consecutive_failures'] == 1

    def test_5xx_records_typed_failure(self, tmp_path):
        svc = _make_puzzle_service(tmp_path)

        def fake_get(*args, **kwargs):
            return _MockResponse(status_code=503)

        with patch('shared.puzzle_image_service.requests.get', side_effect=fake_get):
            result = svc._fetch_iiif_image('12345', 600)

        assert result is None
        snap = br._state_snapshot()
        assert snap['consecutive_failures'] == 1

    def test_429_records_typed_failure(self, tmp_path):
        svc = _make_puzzle_service(tmp_path)

        def fake_get(*args, **kwargs):
            return _MockResponse(status_code=429)

        with patch('shared.puzzle_image_service.requests.get', side_effect=fake_get):
            result = svc._fetch_iiif_image('12345', 600)

        assert result is None
        snap = br._state_snapshot()
        assert snap['consecutive_failures'] == 1

    def test_uses_bounded_timeout_tuple(self, tmp_path):
        """Pin that the call uses (NLI_CONNECT_TIMEOUT, NLI_IMAGE_READ_TIMEOUT)."""
        svc = _make_puzzle_service(tmp_path)
        captured = {'kwargs': None}

        def fake_get(url, **kwargs):
            captured['kwargs'] = kwargs
            return _MockResponse(status_code=200, content=b'x' * 500)

        with patch('shared.puzzle_image_service.requests.get', side_effect=fake_get):
            svc._fetch_iiif_image('12345', 600)

        timeout = captured['kwargs'].get('timeout')
        assert timeout == (br.NLI_CONNECT_TIMEOUT, br.NLI_IMAGE_READ_TIMEOUT), (
            f'D-19 violation: timeout is {timeout!r}, expected the env-driven tuple'
        )


# ---------------------------------------------------------------------------
# D-20: _fetch_direct_url — host-conditional breaker scoping
# ---------------------------------------------------------------------------
class TestFetchDirectUrlHostConditional:
    """D-20: _fetch_direct_url breaker scoping by host. The critical invariant:
    Cambridge/Manchester/Oxford failures must NOT trip the NLI breaker.
    """

    def test_nli_host_short_circuits_when_breaker_open(self, tmp_path):
        svc = _make_puzzle_service(tmp_path)
        for _ in range(br.NLI_CIRCUIT_THRESHOLD):
            br.record_failure('timeout', 'preload')

        get_calls = {'n': 0}

        def fake_get(*args, **kwargs):
            get_calls['n'] += 1
            raise AssertionError('breaker open — NLI host should NOT hit network')

        with patch('shared.puzzle_image_service.requests.get', side_effect=fake_get):
            result = svc._fetch_direct_url('https://iiif.nli.org.il/IIIFv21/FL12345', 600)

        assert result is None
        assert get_calls['n'] == 0

    def test_rosetta_host_short_circuits_when_breaker_open(self, tmp_path):
        """Both iiif.nli.org.il AND rosetta.nli.org.il should be guarded."""
        svc = _make_puzzle_service(tmp_path)
        for _ in range(br.NLI_CIRCUIT_THRESHOLD):
            br.record_failure('timeout', 'preload')

        get_calls = {'n': 0}

        def fake_get(*args, **kwargs):
            get_calls['n'] += 1
            raise AssertionError('breaker open — Rosetta host should NOT hit network')

        with patch('shared.puzzle_image_service.requests.get', side_effect=fake_get):
            result = svc._fetch_direct_url('https://rosetta.nli.org.il/foo/bar', 600)

        assert result is None
        assert get_calls['n'] == 0

    def test_cambridge_host_unaffected_by_breaker(self, tmp_path):
        """Non-NLI hosts proceed even when the NLI breaker is open.

        This is the CORE D-20 invariant: a Cambridge failure must NOT cause the
        NLI breaker to short-circuit a subsequent Cambridge request.
        """
        svc = _make_puzzle_service(tmp_path)
        # Trip the breaker
        for _ in range(br.NLI_CIRCUIT_THRESHOLD):
            br.record_failure('timeout', 'preload')
        assert br.is_open()

        get_call_args = []

        def fake_get(url, **kwargs):
            get_call_args.append({'url': url, 'kwargs': kwargs})
            return _MockResponse(status_code=200, content=b'x' * 500)

        with patch('shared.puzzle_image_service.requests.get', side_effect=fake_get):
            result = svc._fetch_direct_url('https://cudl.lib.cam.ac.uk/manuscripts/foo', 600)

        # The Cambridge fetch happened despite the NLI breaker being open
        assert len(get_call_args) == 1, 'Cambridge host should fetch even when NLI breaker open'
        assert result == b'x' * 500
        # Cambridge uses the existing 30s timeout, not the NLI env knob
        assert get_call_args[0]['kwargs'].get('timeout') == 30, (
            'Cambridge should use timeout=30, not the NLI tuple'
        )

    def test_manchester_host_unaffected_by_breaker(self, tmp_path):
        """Manchester is another non-NLI host; same invariant as Cambridge."""
        svc = _make_puzzle_service(tmp_path)
        for _ in range(br.NLI_CIRCUIT_THRESHOLD):
            br.record_failure('timeout', 'preload')

        get_call_args = []

        def fake_get(url, **kwargs):
            get_call_args.append({'url': url, 'kwargs': kwargs})
            return _MockResponse(status_code=200, content=b'x' * 500)

        with patch('shared.puzzle_image_service.requests.get', side_effect=fake_get):
            result = svc._fetch_direct_url(
                'https://luna.manchester.ac.uk/iiif/m4-foo/full/600,/0/default.jpg', 600,
            )

        assert len(get_call_args) == 1
        assert result == b'x' * 500
        assert get_call_args[0]['kwargs'].get('timeout') == 30

    def test_oxford_host_unaffected_by_breaker(self, tmp_path):
        """Oxford IIIF is non-NLI; must be unaffected by NLI breaker state."""
        svc = _make_puzzle_service(tmp_path)
        for _ in range(br.NLI_CIRCUIT_THRESHOLD):
            br.record_failure('timeout', 'preload')

        get_call_args = []

        def fake_get(url, **kwargs):
            get_call_args.append({'url': url, 'kwargs': kwargs})
            return _MockResponse(status_code=200, content=b'x' * 500)

        with patch('shared.puzzle_image_service.requests.get', side_effect=fake_get):
            result = svc._fetch_direct_url(
                'https://iiif.bodleian.ox.ac.uk/iiif/image/abc/full/600,/0/default.jpg', 600,
            )

        assert len(get_call_args) == 1
        assert result == b'x' * 500
        assert get_call_args[0]['kwargs'].get('timeout') == 30

    def test_nli_host_records_failure_on_timeout(self, tmp_path):
        svc = _make_puzzle_service(tmp_path)

        def fake_get(*args, **kwargs):
            raise _requests.exceptions.ReadTimeout('simulated NLI timeout')

        with patch('shared.puzzle_image_service.requests.get', side_effect=fake_get):
            svc._fetch_direct_url('https://iiif.nli.org.il/IIIFv21/FL12345', 600)

        snap = br._state_snapshot()
        assert snap['consecutive_failures'] == 1, 'NLI host timeout should trip breaker'

    def test_cambridge_host_does_NOT_record_failure_on_timeout(self, tmp_path):
        """CRITICAL D-20 invariant: Cambridge failures must NOT touch the NLI breaker.

        This is the test that pins the host-conditional scope of D-20. If this test
        fails, a Cambridge outage would incorrectly degrade the user experience for
        NLI manuscripts (and vice-versa).
        """
        svc = _make_puzzle_service(tmp_path)

        def fake_get(*args, **kwargs):
            raise _requests.exceptions.ReadTimeout('simulated Cambridge timeout')

        with patch('shared.puzzle_image_service.requests.get', side_effect=fake_get):
            svc._fetch_direct_url('https://cudl.lib.cam.ac.uk/manuscripts/foo', 600)

        snap = br._state_snapshot()
        assert snap['consecutive_failures'] == 0, (
            'Cambridge timeout MUST NOT increment NLI breaker counter'
        )

    def test_manchester_host_does_NOT_record_failure_on_5xx(self, tmp_path):
        """Manchester 5xx is unrelated to NLI; must not trip the breaker."""
        svc = _make_puzzle_service(tmp_path)

        def fake_get(*args, **kwargs):
            return _MockResponse(status_code=503)

        with patch('shared.puzzle_image_service.requests.get', side_effect=fake_get):
            svc._fetch_direct_url(
                'https://luna.manchester.ac.uk/iiif/m4-foo/full/600,/0/default.jpg', 600,
            )

        snap = br._state_snapshot()
        assert snap['consecutive_failures'] == 0, (
            'Manchester 5xx MUST NOT increment NLI breaker counter'
        )

    def test_oxford_host_does_NOT_record_failure_on_connection_error(self, tmp_path):
        svc = _make_puzzle_service(tmp_path)

        def fake_get(*args, **kwargs):
            raise _requests.exceptions.ConnectionError('simulated Oxford conn refused')

        with patch('shared.puzzle_image_service.requests.get', side_effect=fake_get):
            svc._fetch_direct_url(
                'https://iiif.bodleian.ox.ac.uk/iiif/image/abc/full/600,/0/default.jpg', 600,
            )

        snap = br._state_snapshot()
        assert snap['consecutive_failures'] == 0, (
            'Oxford ConnectionError MUST NOT increment NLI breaker counter'
        )

    def test_nli_host_uses_bounded_timeout_tuple(self, tmp_path):
        svc = _make_puzzle_service(tmp_path)
        captured = {'kwargs': None}

        def fake_get(url, **kwargs):
            captured['kwargs'] = kwargs
            return _MockResponse(status_code=200, content=b'x' * 500)

        with patch('shared.puzzle_image_service.requests.get', side_effect=fake_get):
            svc._fetch_direct_url('https://iiif.nli.org.il/IIIFv21/FL12345', 600)

        timeout = captured['kwargs'].get('timeout')
        assert timeout == (br.NLI_CONNECT_TIMEOUT, br.NLI_IMAGE_READ_TIMEOUT), (
            f'D-20 NLI host violation: timeout is {timeout!r}, expected env tuple'
        )

    def test_malformed_url_does_NOT_trip_breaker(self, tmp_path):
        """T-98-04-05 mitigation: urlparse on malformed URL yields empty netloc,
        which is NOT in the NLI hosts tuple, so the breaker is skipped."""
        svc = _make_puzzle_service(tmp_path)
        captured = {'kwargs': None}

        def fake_get(url, **kwargs):
            captured['kwargs'] = kwargs
            raise _requests.exceptions.ReadTimeout('simulated timeout on malformed URL')

        with patch('shared.puzzle_image_service.requests.get', side_effect=fake_get):
            # Non-URL string — urlparse yields empty netloc; treated as non-NLI host
            svc._fetch_direct_url('not-a-url/full/foo', 600)

        snap = br._state_snapshot()
        assert snap['consecutive_failures'] == 0, (
            'Malformed URL with empty netloc must NOT increment NLI breaker'
        )


# ---------------------------------------------------------------------------
# Static source audits — pin the D-19, D-20, D-21 wiring at the source level.
# ---------------------------------------------------------------------------
class TestStaticSourceAudits:
    """Source-level invariants for D-19, D-20, D-21."""

    def _read(self, path):
        return pathlib.Path(path).read_text(encoding='utf-8')

    def test_puzzle_service_imports_breaker(self):
        src = self._read('shared/puzzle_image_service.py')
        assert 'from shared.nli_circuit_breaker import' in src

    def test_puzzle_service_no_timeout_30_in_nli_method(self):
        """D-19: _fetch_iiif_image must NOT use hardcoded timeout=30 IN CODE.

        Substring 'timeout=30' may appear in DOCSTRING / COMMENT text (e.g. the
        D-19 docstring itself says 'replaces hard-coded 30s'). The invariant is
        about the actual call-site code form: `timeout=30` followed by `)`, `,`,
        or whitespace (NOT followed by `.0` or a digit, which would be a
        different timeout value entirely). Regex catches the precise form.
        """
        src = self._read('shared/puzzle_image_service.py')
        marker = 'def _fetch_iiif_image'
        idx = src.find(marker)
        assert idx >= 0, '_fetch_iiif_image not found in shared/puzzle_image_service.py'
        slice_end = src.find('\n    def ', idx + len(marker))
        body = src[idx:slice_end if slice_end > 0 else idx + 3000]
        # Match `timeout=30` only when NOT followed by another digit or a dot.
        # This catches the call-site form but ignores prose mentions.
        bad = re.compile(r'timeout=30(?![\d.])')
        matches = bad.findall(body)
        assert not matches, (
            f'D-19 violation: _fetch_iiif_image still has hardcoded timeout=30 in code; '
            f'matches: {matches}'
        )
        assert 'timeout=(NLI_CONNECT_TIMEOUT, NLI_IMAGE_READ_TIMEOUT)' in body, (
            'D-19 violation: _fetch_iiif_image missing env-driven timeout tuple'
        )

    def test_puzzle_service_timeout_30_only_for_non_nli(self):
        """D-20: timeout=30 (in code form) may remain ONLY once — the non-NLI
        branch of _fetch_direct_url's ternary fallback. Substring mentions in
        docstrings or comments do not count.
        """
        src = self._read('shared/puzzle_image_service.py')
        # Match call-site code form (followed by close-paren / comma / whitespace)
        code_form = re.compile(r'timeout=30(?![\d.])')
        matches = code_form.findall(src)
        assert len(matches) <= 1, (
            'timeout=30 (code form) should appear at most once '
            f'(non-NLI branch of _fetch_direct_url); found {len(matches)}'
        )

    def test_puzzle_service_is_nli_host_present(self):
        """D-20: _fetch_direct_url uses is_nli_host computation."""
        src = self._read('shared/puzzle_image_service.py')
        assert 'is_nli_host' in src, 'D-20: is_nli_host scoping variable missing'

    def test_puzzle_service_nli_host_tuple_listed(self):
        """D-20: both iiif.nli.org.il and rosetta.nli.org.il are in the host tuple."""
        src = self._read('shared/puzzle_image_service.py')
        assert 'iiif.nli.org.il' in src
        assert 'rosetta.nli.org.il' in src

    def test_puzzle_service_typed_exceptions(self):
        """Pitfall 7: NLI-touching paths use specific exception classes, not bare Exception."""
        src = self._read('shared/puzzle_image_service.py')
        assert src.count('except requests.exceptions.Timeout') >= 2, (
            'Expected >= 2 typed Timeout handlers (one per method)'
        )
        assert src.count('except requests.exceptions.ConnectionError') >= 2

    def test_puzzle_page_imports_breaker(self):
        src = self._read('web/pages/puzzle.py')
        assert 'from shared.nli_circuit_breaker import' in src

    def test_puzzle_page_no_int_timeout_15_for_http(self):
        """D-21: web/pages/puzzle.py NLI manifest fetch no longer uses timeout=15.

        NOTE: There are unrelated `timeout=15.0` calls in `ui.run_javascript(...)`
        for client-side JS execution (NiceGUI-internal, NOT HTTP). The D-21 target
        was the bare-int `timeout=15` on the NLI manifest HTTP request. We assert
        on the precise terminator forms that occurred at the original call site:
        `timeout=15)` or `timeout=15,` or `timeout=15\\n` — i.e. int-15 followed
        by close-paren, comma, or whitespace, NOT by a decimal point.

        See SUMMARY.md `Deviations` for the Rule 1 deviation note explaining why
        the plan's literal substring test was tightened here.
        """
        src = self._read('web/pages/puzzle.py')
        # Substring 'timeout=15' is followed by '.' in 'timeout=15.0' (JS call,
        # NOT our target). The HTTP forms end in ')', ',', or whitespace.
        bad_patterns = [
            re.compile(r'timeout=15\)'),
            re.compile(r'timeout=15,'),
            re.compile(r'timeout=15\s'),
        ]
        for pat in bad_patterns:
            matches = pat.findall(src)
            assert not matches, (
                f'D-21 violation: pattern {pat.pattern!r} still present in web/pages/puzzle.py'
            )

    def test_puzzle_page_uses_iiif_read_timeout(self):
        """D-21: web/pages/puzzle.py manifest fetch uses NLI_IIIF_READ_TIMEOUT (JSON, not image)."""
        src = self._read('web/pages/puzzle.py')
        assert 'NLI_IIIF_READ_TIMEOUT' in src
        # Coarse heuristic: the timeout tuple form
        assert 'timeout=(NLI_CONNECT_TIMEOUT, NLI_IIIF_READ_TIMEOUT)' in src

    def test_puzzle_page_resolve_folios_records_success(self):
        """D-21: the NLI manifest success path records via the breaker."""
        src = self._read('web/pages/puzzle.py')
        assert "_nli_record_success(path='puzzle_resolve_folios')" in src

    def test_puzzle_page_resolve_folios_records_failures(self):
        """D-21: typed failures (timeout/connection_error/5xx/429) all wired."""
        src = self._read('web/pages/puzzle.py')
        for failure_type in ('timeout', 'connection_error', '5xx', '429'):
            needle = f"_nli_record_failure(failure_type='{failure_type}', path='puzzle_resolve_folios')"
            assert needle in src, f'D-21 missing failure-type wiring: {failure_type}'

    def test_three_puzzle_call_sites_have_breaker_check(self):
        """D-13: each of the 3 puzzle call sites checks is_open()."""
        svc_src = self._read('shared/puzzle_image_service.py')
        page_src = self._read('web/pages/puzzle.py')
        assert svc_src.count('_nli_circuit_is_open()') >= 2, (
            f'expected >= 2 breaker checks in shared/puzzle_image_service.py, '
            f'got {svc_src.count("_nli_circuit_is_open()")}'
        )
        assert page_src.count('_nli_circuit_is_open()') >= 1, (
            f'expected >= 1 breaker check in web/pages/puzzle.py, '
            f'got {page_src.count("_nli_circuit_is_open()")}'
        )
