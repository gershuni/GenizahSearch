"""Phase 98 Plan 05 — verify genizah_core.py breaker migration.

Covers:
- Removal of legacy class-attribute breaker on MetadataManager (RESEARCH Pitfall 5)
- All 6 call sites wired to shared.nli_circuit_breaker
- Per-sys_id negative caches preserved (D-07)
- Failure typing correct (D-06) and shared with web/api.py (Issue 1/2)
- Behavioral integration: breaker open => fetch_iiif_manifest / fetch_marc_data
  short-circuit; 5xx trips; 404 does not trip; per-attempt recheck inside the
  _fetch_single_worker retry loop (Codex REVIEW Issue 3) short-circuits the
  second retry when the first attempt opens the breaker.

State isolation via autouse fixture in tests/conftest.py (Plan 02 Wave 2).
"""

import pathlib
from collections import OrderedDict
from unittest.mock import patch

import requests as _requests

import shared.nli_circuit_breaker as br


class _MockResponse:
    """Minimal stand-in for requests.Response."""

    def __init__(self, status_code=200, content=b'', text=''):
        self.status_code = status_code
        self.content = content
        self.text = text


# ---------------------------------------------------------------------------
# Source audit: RESEARCH Pitfall 5 ("class-attribute breaker not fully removed")
# ---------------------------------------------------------------------------
class TestLegacyBreakerRemoved:
    """RESEARCH Pitfall 5 — class-attribute breaker fully removed."""

    def _read_source(self):
        return pathlib.Path('genizah_core.py').read_text(encoding='utf-8')

    def test_no_self_dot_nli_circuit_is_open_calls(self):
        src = self._read_source()
        assert 'self._nli_circuit_is_open' not in src, (
            'RESEARCH Pitfall 5 violation: self._nli_circuit_is_open() call still present'
        )

    def test_no_self_dot_nli_record_failure_calls(self):
        src = self._read_source()
        assert 'self._nli_record_failure' not in src
        assert 'cls._nli_record_failure' not in src

    def test_no_self_dot_nli_record_success_calls(self):
        src = self._read_source()
        assert 'self._nli_record_success' not in src
        assert 'cls._nli_record_success' not in src

    def test_no_class_attribute_state(self):
        src = self._read_source()
        # The 4 class-attribute state variables are gone
        assert 'cls._nli_consecutive_failures' not in src
        assert 'cls._nli_circuit_open_until' not in src
        # The class-level threshold/window definitions are also gone
        # (they now live in shared/nli_circuit_breaker.py).
        # `_NLI_CIRCUIT_THRESHOLD = 3` must NOT appear as a class attribute.
        import re as _re
        assert not _re.search(r'_NLI_CIRCUIT_THRESHOLD\s*=\s*3', src), (
            'Legacy class-attribute _NLI_CIRCUIT_THRESHOLD still present'
        )
        assert not _re.search(r'_NLI_CIRCUIT_WINDOW\s*=\s*60', src), (
            'Legacy class-attribute _NLI_CIRCUIT_WINDOW still present'
        )

    def test_no_residual_class_attribute_breaker(self):
        """Plan-level invariant: the class no longer exposes ANY legacy breaker
        state. Static attribute audit on the loaded class object."""
        from genizah_core import MetadataManager
        assert hasattr(MetadataManager, '_nli_consecutive_failures') is False, (
            'MetadataManager still has _nli_consecutive_failures class attribute'
        )
        assert hasattr(MetadataManager, '_nli_circuit_open_until') is False, (
            'MetadataManager still has _nli_circuit_open_until class attribute'
        )
        assert hasattr(MetadataManager, '_NLI_CIRCUIT_THRESHOLD') is False, (
            'MetadataManager still has _NLI_CIRCUIT_THRESHOLD class attribute'
        )
        assert hasattr(MetadataManager, '_NLI_CIRCUIT_WINDOW') is False, (
            'MetadataManager still has _NLI_CIRCUIT_WINDOW class attribute'
        )

    def test_legacy_classmethods_not_on_class(self):
        """The class no longer exposes the legacy breaker methods as attributes.

        (The MODULE imports the new functions under aliases like _nli_circuit_is_open,
        but those are module-level, not class attributes.)
        """
        from genizah_core import MetadataManager
        assert not hasattr(MetadataManager, '_nli_circuit_is_open'), (
            'MetadataManager still has _nli_circuit_is_open as a class attribute'
        )
        assert not hasattr(MetadataManager, '_nli_record_failure')
        assert not hasattr(MetadataManager, '_nli_record_success')


# ---------------------------------------------------------------------------
# Per-sys_id negative caches preserved (D-07)
# ---------------------------------------------------------------------------
class TestPerSysIdNegativeCachesPreserved:
    """D-07 semantics — per-sys_id negative caches are NOT removed."""

    def test_iiif_manifest_fail_cache_still_class_attribute(self):
        from genizah_core import MetadataManager
        assert hasattr(MetadataManager, '_iiif_manifest_fail_cache')
        assert isinstance(MetadataManager._iiif_manifest_fail_cache, OrderedDict)

    def test_marc_fail_cache_still_class_attribute(self):
        from genizah_core import MetadataManager
        assert hasattr(MetadataManager, '_marc_fail_cache')
        assert isinstance(MetadataManager._marc_fail_cache, OrderedDict)

    def test_nli_fail_ttl_constant_preserved(self):
        from genizah_core import MetadataManager
        assert MetadataManager._NLI_FAIL_TTL == 60


# ---------------------------------------------------------------------------
# Source audit: all 6 call sites wired correctly with typed failure/success
# ---------------------------------------------------------------------------
class TestSharedBreakerWiring:
    """The migration wired all expected call sites + failure types."""

    def _read_source(self):
        return pathlib.Path('genizah_core.py').read_text(encoding='utf-8')

    def test_shared_breaker_import_present(self):
        src = self._read_source()
        assert 'from shared.nli_circuit_breaker import' in src

    def test_shared_breaker_state_visible_from_genizah_core(self):
        """Single source of truth: state set via the shared module is visible
        from genizah_core's call sites and vice versa."""
        from genizah_core import _nli_circuit_is_open as genizah_is_open
        from shared.nli_circuit_breaker import (
            is_open as shared_is_open,
            record_failure,
        )
        # Both names point to the SAME callable (the genizah_core import is
        # just an alias rename, not a wrapper).
        assert genizah_is_open is shared_is_open
        # Tripping via the shared module is observed via the genizah_core alias.
        for _ in range(br.NLI_CIRCUIT_THRESHOLD):
            record_failure('timeout', 'test_shared_state')
        assert genizah_is_open() is True
        assert shared_is_open() is True

    def test_all_4_call_sites_check_breaker(self):
        """D-13: each of 4 migrated/wired functions checks is_open() first."""
        src = self._read_source()
        # The zero-arg guard form
        guard_count = src.count('if _nli_circuit_is_open()')
        assert guard_count >= 4, (
            f'expected >= 4 breaker guards in genizah_core.py (fetch_iiif_manifest, '
            f'fetch_marc_data, _fetch_single_worker, _fetch_fl_ids), got {guard_count}'
        )

    def test_path_strings_for_all_call_sites(self):
        src = self._read_source()
        for expected_path in (
            'fetch_iiif_manifest',
            'fetch_marc_data',
            '_fetch_single_worker',
            '_fetch_fl_ids',
        ):
            assert f"path='{expected_path}'" in src, (
                f"missing path kwarg for {expected_path!r} call site"
            )

    def test_failure_typing_covers_all_4_types(self):
        src = self._read_source()
        for ft in ('timeout', 'connection_error', '5xx', '429'):
            assert f"failure_type='{ft}'" in src, (
                f"missing failure_type={ft!r} branch in genizah_core.py"
            )

    def test_env_driven_timeouts_used(self):
        src = self._read_source()
        # The migrated paths use env-driven tuples
        assert 'timeout=(NLI_CONNECT_TIMEOUT, NLI_IIIF_READ_TIMEOUT)' in src
        assert 'timeout=(NLI_CONNECT_TIMEOUT, NLI_MARC_READ_TIMEOUT)' in src


# ---------------------------------------------------------------------------
# Behavioral integration: breaker open => fetch sites short-circuit; failure
# typing actually trips / does not trip as documented.
# ---------------------------------------------------------------------------
class TestBreakerBehaviorIntegration:
    """End-to-end: tripping the breaker affects fetch_iiif_manifest / fetch_marc_data."""

    def _make_metadata_manager(self):
        """Construct a minimal manager for the breaker integration test.

        MetadataManager.__new__(MetadataManager) skips __init__ side effects
        (no DB / index needed for breaker-only tests).
        """
        from genizah_core import MetadataManager
        mm = MetadataManager.__new__(MetadataManager)
        return mm

    def test_breaker_open_blocks_fetch_iiif_manifest(self):
        mm = self._make_metadata_manager()
        for _ in range(br.NLI_CIRCUIT_THRESHOLD):
            br.record_failure('timeout', 'preload')
        assert br.is_open()

        session_get_called = {'n': 0}

        def fake_make_session(self):
            class FakeSession:
                def get(self, *args, **kwargs):
                    session_get_called['n'] += 1
                    raise AssertionError(
                        'breaker open — session.get must NOT be called'
                    )
            return FakeSession()

        from genizah_core import MetadataManager
        MetadataManager._iiif_manifest_cache.clear()
        MetadataManager._iiif_manifest_fail_cache.clear()
        with patch.object(MetadataManager, '_make_session', new=fake_make_session):
            result = mm.fetch_iiif_manifest('some_sysid_d22_iiif')

        assert result == {'physical_desc': '', 'canvas_map': {}, 'attribution': ''}
        assert session_get_called['n'] == 0, (
            'D-22 violation: fetch_iiif_manifest hit network with open breaker'
        )

    def test_breaker_open_blocks_fetch_marc_data(self):
        mm = self._make_metadata_manager()
        for _ in range(br.NLI_CIRCUIT_THRESHOLD):
            br.record_failure('timeout', 'preload')
        assert br.is_open()

        session_get_called = {'n': 0}

        def fake_make_session(self):
            class FakeSession:
                def get(self, *args, **kwargs):
                    session_get_called['n'] += 1
                    raise AssertionError(
                        'breaker open — session.get must NOT be called'
                    )
            return FakeSession()

        from genizah_core import MetadataManager
        MetadataManager._marc_fail_cache.clear()
        with patch.object(MetadataManager, '_make_session', new=fake_make_session):
            result = mm.fetch_marc_data('some_sysid_marc_d22')

        # fetch_marc_data returns a default dict structure
        assert isinstance(result, dict)
        assert session_get_called['n'] == 0

    def test_5xx_response_trips_breaker_via_fetch_marc(self):
        mm = self._make_metadata_manager()

        def fake_make_session(self):
            class FakeSession:
                def get(self, *args, **kwargs):
                    return _MockResponse(status_code=503, text='', content=b'')
            return FakeSession()

        # Disable per-sys_id negative cache so repeated failures all reach
        # the network path. Each call also uses a unique sys_id for safety.
        from genizah_core import MetadataManager
        original_ttl = MetadataManager._NLI_FAIL_TTL
        MetadataManager._NLI_FAIL_TTL = 0
        try:
            with patch.object(MetadataManager, '_make_session', new=fake_make_session):
                for i in range(br.NLI_CIRCUIT_THRESHOLD):
                    MetadataManager._marc_fail_cache.clear()
                    mm.fetch_marc_data(f'sysid_5xx_{i}')
        finally:
            MetadataManager._NLI_FAIL_TTL = original_ttl

        assert br.is_open(), (
            '503 responses should trip the breaker via fetch_marc_data'
        )

    def test_404_does_not_trip_breaker_via_fetch_marc(self):
        """D-07: 404 -> per-sys_id negative cache only, no breaker increment."""
        mm = self._make_metadata_manager()

        def fake_make_session(self):
            class FakeSession:
                def get(self, *args, **kwargs):
                    return _MockResponse(status_code=404, text='', content=b'')
            return FakeSession()

        from genizah_core import MetadataManager
        MetadataManager._marc_fail_cache.clear()
        with patch.object(MetadataManager, '_make_session', new=fake_make_session):
            for i in range(5):
                mm.fetch_marc_data(f'sysid_404_{i}')

        snap = br._state_snapshot()
        assert snap['consecutive_failures'] == 0, (
            'D-07 violation: 404 incremented breaker counter via fetch_marc_data'
        )
        assert not br.is_open()

    def test_timeout_via_fetch_iiif_manifest_increments_breaker(self):
        mm = self._make_metadata_manager()

        def fake_make_session(self):
            class FakeSession:
                def get(self, *args, **kwargs):
                    raise _requests.exceptions.ReadTimeout('simulated NLI timeout')
            return FakeSession()

        from genizah_core import MetadataManager
        original_ttl = MetadataManager._NLI_FAIL_TTL
        MetadataManager._NLI_FAIL_TTL = 0
        try:
            MetadataManager._iiif_manifest_cache.clear()
            MetadataManager._iiif_manifest_fail_cache.clear()
            with patch.object(MetadataManager, '_make_session', new=fake_make_session):
                mm.fetch_iiif_manifest('sysid_iiif_timeout_1')
        finally:
            MetadataManager._NLI_FAIL_TTL = original_ttl

        snap = br._state_snapshot()
        assert snap['consecutive_failures'] == 1, (
            'timeout via fetch_iiif_manifest should increment breaker counter by 1'
        )

    def test_retry_loop_short_circuits_when_breaker_opens(self):
        """Codex REVIEW Issue 3: if the first attempt of _fetch_single_worker
        opens the breaker (e.g. threshold == 1), the second retry must NOT
        burn another timeout. We do this by setting THRESHOLD effectively
        low via direct state manipulation: pre-load THRESHOLD-1 failures so
        a single failure inside the retry loop opens the breaker, then assert
        session.get was called exactly ONCE across the 2-attempt loop.
        """
        mm = self._make_metadata_manager()

        # Pre-load THRESHOLD - 1 failures so the FIRST timeout inside the
        # retry loop opens the breaker. The per-iteration recheck must then
        # short-circuit the SECOND retry before it hits the network.
        for _ in range(br.NLI_CIRCUIT_THRESHOLD - 1):
            br.record_failure('timeout', 'preload')
        assert not br.is_open(), 'preload should not yet open the breaker'

        session_get_calls = {'n': 0}

        def fake_make_session(self):
            class FakeSession:
                def get(self, *args, **kwargs):
                    session_get_calls['n'] += 1
                    raise _requests.exceptions.ReadTimeout('simulated NLI timeout')
            return FakeSession()

        from genizah_core import MetadataManager
        with patch.object(MetadataManager, '_make_session', new=fake_make_session):
            sys_id, meta = mm._fetch_single_worker('sysid_retry_short_circuit')

        # The first attempt fires (and trips the breaker via timeout). The
        # per-iteration recheck at the top of the second attempt must catch
        # the now-open breaker and break BEFORE calling session.get again.
        assert session_get_calls['n'] == 1, (
            f'Codex REVIEW Issue 3 violation: retry loop made '
            f'{session_get_calls["n"]} network calls, expected 1 (first '
            f'attempt fires, second attempt short-circuits on per-iteration '
            f'breaker recheck).'
        )
        assert br.is_open(), 'breaker should be open after the first timeout'
        # Returns the default empty meta
        assert sys_id == 'sysid_retry_short_circuit'
        assert meta.get('shelfmark') == 'Unknown'

    def test_d22_fetch_single_worker_breaker_pre_loop_guard(self):
        """D-22: a pre-opened breaker prevents _fetch_single_worker from
        ever entering the retry loop."""
        mm = self._make_metadata_manager()
        for _ in range(br.NLI_CIRCUIT_THRESHOLD):
            br.record_failure('timeout', 'preload')
        assert br.is_open()

        session_get_calls = {'n': 0}

        def fake_make_session(self):
            class FakeSession:
                def get(self, *args, **kwargs):
                    session_get_calls['n'] += 1
                    raise AssertionError('breaker open — must not call get')
            return FakeSession()

        from genizah_core import MetadataManager
        with patch.object(MetadataManager, '_make_session', new=fake_make_session):
            sys_id, meta = mm._fetch_single_worker('sysid_d22_preopen')

        assert session_get_calls['n'] == 0
        assert sys_id == 'sysid_d22_preopen'
        assert meta.get('shelfmark') == 'Unknown'

    def test_d23_fetch_fl_ids_breaker_guard(self):
        """D-23: a pre-opened breaker short-circuits _fetch_fl_ids to []."""
        mm = self._make_metadata_manager()
        for _ in range(br.NLI_CIRCUIT_THRESHOLD):
            br.record_failure('timeout', 'preload')
        assert br.is_open()

        session_get_calls = {'n': 0}

        def fake_make_session(self):
            class FakeSession:
                def get(self, *args, **kwargs):
                    session_get_calls['n'] += 1
                    raise AssertionError('breaker open — must not call get')
            return FakeSession()

        from genizah_core import MetadataManager
        with patch.object(MetadataManager, '_make_session', new=fake_make_session):
            result = mm._fetch_fl_ids('sysid_d23_preopen')

        assert result == []
        assert session_get_calls['n'] == 0
