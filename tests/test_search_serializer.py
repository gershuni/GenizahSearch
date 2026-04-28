"""
Phase 77 — Serializer & JSON Export tests.

Wave 0: these tests are RED until Plan 03 creates shared/search_serializer.py.
Plan 02 lands first to extend lab_composition_search with chunk_hits (D-13 Path A).

Test contract derived from:
  - .planning/REQUIREMENTS.md §JSON Export (EXPORT-01..04)
  - .planning/phases/77-serializer-json-export/77-CONTEXT.md (D-01..D-14)
  - .planning/phases/77-serializer-json-export/77-VALIDATION.md (per-task map)
  - .planning/phases/77-serializer-json-export/77-RESEARCH.md (Pitfalls 1-7)

Plan 01 deviations from CONTEXT.md (locked in 77-01-PLAN.md <plan_locked_decisions>):
  - D-01: emit `domains: list[str]` instead of singular `domain` (forward-compatible)
  - D-08: image URL is server-relative `/api/nli_image_by_sysid/{sys_id}?page={p_num-1}`
  - D-12: chunk_index is 0-based
  - D-13: Path A — core extension to track chunk_hits per uid
  - EXPORT-04: filename uniqueness uses millisecond resolution; test runs without sleep.
"""

import pytest
from unittest.mock import MagicMock


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------

@pytest.fixture
def mock_meta_mgr():
    """MetadataManager mock — copies tests/test_export_service.py:240-245 pattern.

    parse_full_id_components stub returns a deterministic locator dict so we can
    assert exact `{sys_id, ie_id, p_num}` values in tests.
    """
    mgr = MagicMock()
    mgr.get_meta_for_id.return_value = ("T-S 12.345", "Test Title")

    def _parse(header):
        # Synthetic parse: 'header_99...IE..._P7' or similar
        if not header or not isinstance(header, str):
            return {'sys_id': None, 'ie_id': None, 'p_num': None, 'fl_id': None}
        # Naive extraction good enough for fixtures
        sys_id = None
        ie_id = None
        p_num = None
        if '99' in header:
            import re
            m = re.search(r'(99\d{8,})', header)
            if m:
                sys_id = m.group(1)
        if 'IE' in header:
            import re
            m = re.search(r'(IE\d+)', header)
            if m:
                ie_id = m.group(1)
        if '_P' in header or 'P0' in header or 'P1' in header:
            import re
            m = re.search(r'_?(P\d+)', header)
            if m:
                p_num = str(int(m.group(1)[1:]))
        return {'sys_id': sys_id, 'ie_id': ie_id, 'p_num': p_num, 'fl_id': None}

    mgr.parse_full_id_components.side_effect = _parse
    mgr.get_library_for_id.return_value = 'CUL'
    return mgr


@pytest.fixture
def sample_search_results():
    """Synthetic search results covering the four locator paths.

    Row 0: standard hit with full display + raw_header → uid path A1.
    Row 1: hit with snippet *foo* *bar* *foo* → match_terms dedupe.
    Row 2: hit with NO display.id → uid empty + locator from raw_header.
    Row 3: metadata-only hit (no raw_header, no full_text) → uid='', locator nulls.
    """
    return [
        {
            'uid': 'uid_001',
            'display': {
                'shelfmark': 'T-S 12.345',
                'title': 'כתב יד עברי',
                'id': '9912345678901234',
                'library_code': 'CUL',
                'img': '7',  # NOTE: page number, not URL (RESEARCH §Pitfall 1)
            },
            'raw_header': 'header_9912345678901234_IE99_P7',
            'snippet': 'This is a *highlighted* snippet with *foo*',
            'full_text': 'Full text content here. ' * 30,  # >500 chars
            'sort_score': 0.873112948,  # Tests rounding to 4 decimals
        },
        {
            'uid': 'uid_002',
            'display': {
                'shelfmark': 'T-S 12.346',
                'title': 'מסמך נוסף',
                'id': '9912345678901235',
                'library_code': 'CUL',
                'img': '23',
            },
            'raw_header': 'header_9912345678901235_IE100_P23',
            'snippet': 'Match *foo* and *bar* and *foo* again — *baz*',
            'full_text': 'Short text',
            'sort_score': 0.7,
        },
        {
            'uid': '',  # No uid (Title/Shelfmark mode hit)
            'display': {
                'shelfmark': 'T-S 999.1',
                'title': 'No-id case',
                'library_code': 'JTS',
                # NO 'id' key on purpose
            },
            'raw_header': 'header_9912345678909999_IE5_P1',
            'snippet': '',
            'full_text': '',
            'sort_score': 0.5,
        },
        {
            'uid': '',
            'display': {
                'shelfmark': 'T-S META',
                'title': 'metadata-only hit',
                'id': '9912345678907777',
                'library_code': 'BL',
            },
            'raw_header': '',  # No raw_header → locator p_num/ie_id null
            'snippet': '',
            'full_text': '',
            'metadata_only': True,
            'sort_score': 0.2,
        },
    ]


@pytest.fixture
def oxford_only_hit():
    """HIGH-07 fixture: Oxford-only hit with sys_id + p_num but library_code='Oxford'.

    Plan 03 _build_image_url() must emit `null` here even though both sys_id and
    p_num are populated, because the manuscript provider is non-NLI.
    """
    return {
        'uid': 'uid_oxford',
        'display': {
            'shelfmark': 'MS Heb c 57',
            'title': 'Oxford-only manuscript',
            'id': '9988887777666655',
            'library_code': 'Oxford',
            'img': '3',
        },
        'raw_header': 'header_9988887777666655_IE42_P3',
        'snippet': '',
        'full_text': '',
        'sort_score': 0.4,
    }


@pytest.fixture
def sample_parallels_results():
    """Two manuscripts × multiple chunk hits → exercises D-13 grouping.

    Path A assumption: each item carries `chunk_hits: [(i, chunk_text, score, ms_snip)]`.
    sys_id 9911111111111111 has 3 chunk hits across 2 uids → 1 result, matches len 3.
    sys_id 9922222222222222 has 1 chunk hit → 1 result, matches len 1.
    """
    return [
        {
            'uid': 'uid_a',
            'raw_header': 'header_9911111111111111_IE1_P3',
            'score': 50,
            'source_ctx': 'first chunk source text',
            'text': 'manuscript match for first chunk',
            'chunk_hits': [
                (0, 'first chunk source text', 30, 'manuscript snippet for chunk 0'),
                (2, 'third chunk source text', 20, 'manuscript snippet for chunk 2'),
            ],
        },
        {
            'uid': 'uid_b',
            'raw_header': 'header_9911111111111111_IE1_P5',  # SAME sys_id as uid_a
            'score': 15,
            'source_ctx': 'fifth chunk source text',
            'text': 'second manuscript match',
            'chunk_hits': [
                (4, 'fifth chunk source text', 15, 'manuscript snippet for chunk 4'),
            ],
        },
        {
            'uid': 'uid_c',
            'raw_header': 'header_9922222222222222_IE1_P1',
            'score': 25,
            'source_ctx': 'lone match text',
            'text': 'lone manuscript text',
            'chunk_hits': [
                (1, 'lone match text', 25, 'manuscript snippet for chunk 1'),
            ],
        },
    ]


@pytest.fixture
def sample_parallels_filtered():
    """Single filtered/high-frequency hit — must end up in `filtered: []` not `results: []` per D-11."""
    return [
        {
            'uid': 'uid_filt',
            'raw_header': 'header_9933333333333333_IE1_P1',
            'score': 5,
            'source_ctx': 'filtered chunk',
            'text': 'filtered text',
            'chunk_hits': [(0, 'filtered chunk', 5, 'filtered snippet')],
        },
    ]


@pytest.fixture
def metadata_only_hit():
    """Single-result fixture for D-04 metadata-only locator nulls test."""
    return {
        'uid': '',
        'display': {
            'shelfmark': 'T-S META',
            'title': 'metadata-only',
            'id': '9912345678907777',
            'library_code': 'BL',
        },
        'raw_header': '',
        'snippet': '',
        'full_text': '',
        'metadata_only': True,
        'sort_score': 0.1,
    }


@pytest.fixture(autouse=True)
def _disable_fjms(monkeypatch):
    """All tests run with fjms.is_available() returning False.

    The serializer's domain/catalog batch lookup will return {} cleanly,
    so domains == [] and dating == None for all items. Production behavior
    is exercised in the manual smoke check; unit tests assert the no-fjms path.

    HIGH-05 note: do NOT assert .close() is called. The serializer must NOT close
    the FJMS singleton — close is reserved for reset_fjms_service() (sidecar swap).
    """
    try:
        from shared import fjms_service as fjms_mod
        fake = MagicMock()
        fake.is_available.return_value = False
        fake.get_domains_for_sys_ids.return_value = {}
        fake.get_catalog.return_value = None
        # Note: fake.close exists but the serializer must not call it on the singleton.
        # If a future regression starts calling close(), this fixture still passes — the
        # behavioral guarantee is enforced by test_serializer_does_not_close_fjms below.
        monkeypatch.setattr(fjms_mod, 'get_fjms_service', lambda thread_safe=True: fake)
    except ImportError:
        # shared.fjms_service may not be importable at test-collection time on some
        # CI workers; serializer's own import-time guard will handle it. The autouse
        # fixture is best-effort.
        pass


# -----------------------------------------------------------------------------
# Tests — RED until Plan 03 (search_serializer module) lands
# -----------------------------------------------------------------------------

class TestSchemaConstant:
    def test_schema_version_constant(self):
        """D-10: SCHEMA_VERSION = 1 is exported."""
        from shared.search_serializer import SCHEMA_VERSION
        assert SCHEMA_VERSION == 1


class TestSearchEnvelope:
    def test_search_envelope_shape(self, mock_meta_mgr, sample_search_results):
        """EXPORT-01 / D-05: envelope has all required keys."""
        from shared.search_serializer import serialize_search_payload
        payload = serialize_search_payload(
            sample_search_results, meta_mgr=mock_meta_mgr,
            query="foo", mode="text", gap=2,
            filters={'domains': ['Letters']}, warnings=None, total=None,
        )
        required = {
            'schema_version', 'source', 'query', 'mode', 'gap', 'filters',
            'count', 'total', 'warnings', 'generated_at', 'results',
        }
        assert required.issubset(payload.keys()), \
            f"Missing keys: {required - payload.keys()}"

    def test_warnings_always_present(self, mock_meta_mgr, sample_search_results):
        """D-07: warnings is always a list, even on clean queries."""
        from shared.search_serializer import serialize_search_payload
        payload = serialize_search_payload(
            sample_search_results, meta_mgr=mock_meta_mgr,
            query="foo", mode="text",
        )
        assert isinstance(payload['warnings'], list)
        assert payload['warnings'] == []

    def test_source_field_tags_search(self, mock_meta_mgr, sample_search_results):
        """D-09: source = 'search' for serialize_search_payload."""
        from shared.search_serializer import serialize_search_payload
        payload = serialize_search_payload(
            sample_search_results, meta_mgr=mock_meta_mgr, query="x", mode="text",
        )
        assert payload['source'] == 'search'

    def test_empty_results_envelope(self, mock_meta_mgr):
        """D-05: empty results produce a well-formed envelope."""
        from shared.search_serializer import serialize_search_payload
        payload = serialize_search_payload(
            [], meta_mgr=mock_meta_mgr, query="", mode="text",
        )
        assert payload['count'] == 0
        assert payload['total'] == 0
        assert payload['results'] == []
        assert payload['warnings'] == []


class TestPerItemShape:
    def test_locator_always_both_present(self, mock_meta_mgr, sample_search_results):
        """D-04: every item has BOTH `uid` (string) and `locator` (dict)."""
        from shared.search_serializer import serialize_search_payload
        payload = serialize_search_payload(
            sample_search_results, meta_mgr=mock_meta_mgr, query="x", mode="text",
        )
        for item in payload['results']:
            assert 'uid' in item, "uid is always a string (may be empty)"
            assert isinstance(item['uid'], str), "uid is always a string (may be empty)"
            assert 'locator' in item, "locator key must always exist"
            assert isinstance(item['locator'], dict), "locator is always a dict"

    def test_locator_phase79_shape(self, mock_meta_mgr, sample_search_results):
        """Locator dict has exactly the keys Phase 79 /api/browse will accept."""
        from shared.search_serializer import serialize_search_payload
        payload = serialize_search_payload(
            sample_search_results, meta_mgr=mock_meta_mgr, query="x", mode="text",
        )
        for item in payload['results']:
            loc = item['locator']
            assert set(loc.keys()) == {'sys_id', 'volume_ie', 'p_num'}
            for v in loc.values():
                assert v is None or isinstance(v, str)

    def test_metadata_only_hit_shape(self, mock_meta_mgr, metadata_only_hit):
        """D-04: metadata-only hits have uid='', sys_id populated, volume_ie/p_num null."""
        from shared.search_serializer import serialize_search_payload
        payload = serialize_search_payload(
            [metadata_only_hit], meta_mgr=mock_meta_mgr, query="x", mode="Title",
        )
        item = payload['results'][0]
        assert item['uid'] == ''
        # sys_id comes from display.id when raw_header is missing
        assert item['locator']['sys_id'] == '9912345678907777'
        assert item['locator']['volume_ie'] is None
        assert item['locator']['p_num'] is None

    def test_snippet_stripped_match_terms_extracted(self, mock_meta_mgr, sample_search_results):
        """D-03: snippet has *term* stripped; match_terms exposes the extracted set deduped in order."""
        from shared.search_serializer import serialize_search_payload
        payload = serialize_search_payload(
            sample_search_results, meta_mgr=mock_meta_mgr, query="foo", mode="text",
        )
        # Row 1 fixture has '*foo* *bar* *foo* *baz*'
        item = payload['results'][1]
        assert '*' not in item['snippet']
        assert item['match_terms'] == ['foo', 'bar', 'baz']

    def test_no_full_text_field_in_item(self, mock_meta_mgr, sample_search_results):
        """D-02: no full_text field; excerpt is full_text[:500]."""
        from shared.search_serializer import serialize_search_payload
        payload = serialize_search_payload(
            sample_search_results, meta_mgr=mock_meta_mgr, query="x", mode="text",
        )
        item = payload['results'][0]
        assert 'full_text' not in item
        assert 'excerpt' in item
        assert len(item['excerpt']) <= 500

    def test_score_rounded_to_4_decimals(self, mock_meta_mgr, sample_search_results):
        """Discretion: round(sort_score, 4)."""
        from shared.search_serializer import serialize_search_payload
        payload = serialize_search_payload(
            sample_search_results, meta_mgr=mock_meta_mgr, query="x", mode="text",
        )
        # Row 0 fixture sort_score = 0.873112948
        item = payload['results'][0]
        assert item['score'] == 0.8731

    def test_image_url_is_relative_or_null(self, mock_meta_mgr, sample_search_results):
        """Plan 01 lock: image URL is server-relative `/api/nli_image_by_sysid/...?page=N` or null."""
        from shared.search_serializer import serialize_search_payload
        payload = serialize_search_payload(
            sample_search_results, meta_mgr=mock_meta_mgr, query="x", mode="text",
        )
        for item in payload['results']:
            url = item['image_url']
            if url is None:
                continue  # metadata-only / no p_num — null is allowed
            assert url.startswith('/api/nli_image_by_sysid/'), f"Got {url}"
            assert '?page=' in url

    def test_image_url_null_for_oxford_only(self, mock_meta_mgr, oxford_only_hit):
        """HIGH-07: Oxford-only hits get image_url=null even when sys_id+p_num populated.

        CONTEXT.md locked decision: emit null for non-NLI providers. Plan 03 must
        consult the library_code (or equivalent provider-check) before emitting.
        """
        from shared.search_serializer import serialize_search_payload
        payload = serialize_search_payload(
            [oxford_only_hit], meta_mgr=mock_meta_mgr, query="x", mode="text",
        )
        item = payload['results'][0]
        assert item['image_url'] is None, \
            f"Oxford-only hit must have image_url=null; got {item['image_url']}"

    def test_domains_is_a_list(self, mock_meta_mgr, sample_search_results):
        """Plan 01 lock (deviation from D-01): emit `domains: list[str]`, not singular `domain`."""
        from shared.search_serializer import serialize_search_payload
        payload = serialize_search_payload(
            sample_search_results, meta_mgr=mock_meta_mgr, query="x", mode="text",
        )
        for item in payload['results']:
            assert 'domains' in item
            assert isinstance(item['domains'], list)
            # With FJMS disabled (autouse fixture), domains == []
            assert item['domains'] == []


class TestParallelsEnvelope:
    def test_parallels_envelope_shape(self, mock_meta_mgr, sample_parallels_results):
        """EXPORT-02 / D-09: parallels envelope tagged with source='parallels'."""
        from shared.search_serializer import serialize_parallels_payload
        payload = serialize_parallels_payload(
            sample_parallels_results, [], meta_mgr=mock_meta_mgr,
            source_text="hello world", chunk_size=5, mode="exact",
        )
        assert payload['source'] == 'parallels'
        assert payload['source_text'] == 'hello world'
        assert payload['chunk_size'] == 5
        assert payload['mode'] == 'exact'
        assert 'filtered' in payload  # D-11

    def test_parallels_filtered_separation(
        self, mock_meta_mgr, sample_parallels_results, sample_parallels_filtered
    ):
        """D-11: results and filtered are separate top-level arrays."""
        from shared.search_serializer import serialize_parallels_payload
        payload = serialize_parallels_payload(
            sample_parallels_results, sample_parallels_filtered,
            meta_mgr=mock_meta_mgr, source_text="x", chunk_size=5, mode="exact",
        )
        # Main results: 2 manuscripts (sys_id 9911... and 9922...)
        assert len(payload['results']) == 2
        # Filtered: 1 manuscript (sys_id 9933...)
        assert len(payload['filtered']) == 1
        # No overlap
        main_ids = {r['locator']['sys_id'] for r in payload['results']}
        filt_ids = {r['locator']['sys_id'] for r in payload['filtered']}
        assert main_ids.isdisjoint(filt_ids)

    def test_parallels_groups_by_manuscript(self, mock_meta_mgr, sample_parallels_results):
        """D-13: one result per manuscript, with matches[] array."""
        from shared.search_serializer import serialize_parallels_payload
        payload = serialize_parallels_payload(
            sample_parallels_results, [], meta_mgr=mock_meta_mgr,
            source_text="x", chunk_size=5, mode="exact",
        )
        # Find the result for sys_id 9911... — should have 3 matches (across 2 uids)
        result_99_11 = next(
            r for r in payload['results'] if r['locator']['sys_id'] == '9911111111111111'
        )
        assert len(result_99_11['matches']) == 3
        # Each match has the D-13 shape
        for m in result_99_11['matches']:
            assert set(m.keys()) >= {
                'chunk_index', 'source_chunk_text', 'manuscript_snippet', 'score'
            }
        # chunk_indices are 0-based per Plan 01 lock
        chunk_idxs = sorted(m['chunk_index'] for m in result_99_11['matches'])
        assert chunk_idxs == [0, 2, 4]

    def test_parallels_populates_snippet_excerpt_match_terms_from_text(self, mock_meta_mgr):
        """Plan 02 fixup: parallels items use 'text' (with *term* markers), not
        'snippet'. _to_parallels_envelope_item now maps text -> synth['snippet']
        before _serialize_item runs, so snippet (stripped) + match_terms
        (extracted) + excerpt (from full_text or text) populate properly.
        """
        from shared.search_serializer import serialize_parallels_payload
        # Standard-mode parallels rep: 'text' carries snippet WITH markers; no 'snippet' key
        results = [{
            'uid': 'parallels_uid_1',
            'raw_header': 'CUL_T-S_NS_001_1r',
            'sort_score': 1.0,
            'score': 1.0,
            'source_ctx': 'source ctx',
            'text': 'foo *bar* baz *qux* quux',  # parallels rep field
            # No 'snippet', no 'full_text' (standard mode shape)
            'chunk_hits': [(0, 'foo bar', 1.0, 'foo *bar* baz')],
            'display': {
                'id': '9911111111111111',
                'shelfmark': 'T-S NS 001',
                'title': 'Test',
                'library_code': 'CUL',
            },
        }]
        payload = serialize_parallels_payload(
            results, [], meta_mgr=mock_meta_mgr,
            source_text='x', chunk_size=5, mode='exact',
        )
        result = payload['results'][0]
        # snippet stripped of markers (from rep['text'])
        assert result['snippet'] == 'foo bar baz qux quux'
        # match_terms extracted from the markers in text
        assert result['match_terms'] == ['bar', 'qux']
        # excerpt non-empty (falls back to text when full_text is missing)
        assert result['excerpt']
        assert 'bar' in result['excerpt']  # markers preserved in raw text source

    def test_parallels_chunk_hits_int_falls_back_to_path_b(self, mock_meta_mgr):
        """Regression: forward-compat guard. If a future caller passes a parallels
        item with chunk_hits as an int (older standard-mode shape, before the
        Phase 77 chunk_count rename in search_composition_logic), the serializer
        must not iterate the int — degrade gracefully to Path B single-match.
        """
        from shared.search_serializer import serialize_parallels_payload
        # Mirror the standard-mode shape: chunk_hits is an int counter, not a list
        results = [{
            'uid': 'std_mode_uid',
            'raw_header': 'CUL_T-S_NS_001_1r',
            'sort_score': 2.5,
            'score': 2.5,
            'src_lbl': 'src',
            'source_ctx': 'source chunk text',
            'text': 'manuscript snippet',
            'full_text': 'full ms text',
            'chunk_hits': 3,  # int counter, NOT a list
            'display': {
                'id': '9911111111111111',
                'shelfmark': 'T-S NS 001',
                'title': 'Test',
                'library_code': 'CUL',
            },
        }]
        # Must not raise 'int' object is not iterable
        payload = serialize_parallels_payload(
            results, [], meta_mgr=mock_meta_mgr,
            source_text='x', chunk_size=5, mode='exact',
        )
        assert payload['count'] == 1
        result = payload['results'][0]
        # Path B fallback: single degenerate match with chunk_index=None
        assert len(result['matches']) == 1
        assert result['matches'][0]['chunk_index'] is None
        assert result['matches'][0]['source_chunk_text'] == 'source chunk text'
        assert result['matches'][0]['manuscript_snippet'] == 'manuscript snippet'

    def test_parallels_score_aggregate_is_sum(self, mock_meta_mgr, sample_parallels_results):
        """Plan 01 lock: manuscript-level score is SUM (matches existing UI per-card score)."""
        from shared.search_serializer import serialize_parallels_payload
        payload = serialize_parallels_payload(
            sample_parallels_results, [], meta_mgr=mock_meta_mgr,
            source_text="x", chunk_size=5, mode="exact",
        )
        # sys_id 9911... has uid_a (score 50) + uid_b (score 15) → SUM = 65
        result_99_11 = next(
            r for r in payload['results'] if r['locator']['sys_id'] == '9911111111111111'
        )
        assert result_99_11['score'] == 65.0


class TestSingleSourceOfTruth:
    def test_serializers_share_serialize_item(self):
        """EXPORT-03 structural: only one _serialize_item exists; no shadow helpers."""
        from shared import search_serializer as ss
        names = [n for n in dir(ss) if n.startswith('_serialize')]
        assert '_serialize_item' in names
        assert '_serialize_search_item' not in names, \
            "EXPORT-03 violation: separate search-specific serializer found"
        assert '_serialize_parallels_item' not in names, \
            "EXPORT-03 violation: separate parallels-specific serializer found"

    def test_search_and_parallels_share_item_shape(
        self, mock_meta_mgr, sample_search_results, sample_parallels_results
    ):
        """EXPORT-03 behavioral: all keys emitted by search items appear on parallels items
        (parallels items add `matches` on top)."""
        from shared.search_serializer import (
            serialize_search_payload, serialize_parallels_payload,
        )
        s_pl = serialize_search_payload(
            sample_search_results, meta_mgr=mock_meta_mgr, query="x", mode="text",
        )
        p_pl = serialize_parallels_payload(
            sample_parallels_results, [], meta_mgr=mock_meta_mgr,
            source_text="x", chunk_size=5, mode="exact",
        )
        s_keys = set(s_pl['results'][0].keys())
        p_keys = set(p_pl['results'][0].keys())
        # parallels has `matches` extra; everything else identical
        assert s_keys.issubset(p_keys), f"Search-only keys: {s_keys - p_keys}"
        assert p_keys - s_keys == {'matches'}, f"Unexpected parallels-only keys: {p_keys - s_keys - {'matches'}}"


class TestFilenameUniqueness:
    def test_filename_uniqueness_consecutive(self):
        """EXPORT-04 / HIGH-06: two consecutive calls (no sleep) produce DISTINCT filenames.

        Plan 03 implements millisecond resolution OR a monotonic counter OR a random
        suffix — the unit test asserts the property without sleeping. The prior
        revision used a one-second wait to dodge a second-resolution collision; that
        was hiding the bug. See 77-REVIEWS.md HIGH-06.
        """
        from shared.search_serializer import build_search_filename
        f1 = build_search_filename()
        f2 = build_search_filename()
        assert f1 != f2, f"EXPORT-04 violation: {f1} == {f2}"

    def test_filename_format(self):
        """EXPORT-04: filename starts with `genizah-search-` and ends with `.json`."""
        from shared.search_serializer import build_search_filename
        f = build_search_filename()
        assert f.startswith('genizah-search-')
        assert f.endswith('.json')
        # ISO-ish timestamp present
        import re
        assert re.search(r'\d{4}-\d{2}-\d{2}T\d{6}', f), f"Got {f}"
