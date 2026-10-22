"""The local gates of Phase 3's second part in ``scripts/measure_responsa_wildcards.py``, run
here on corpora built by the real Indexer:

- ``missing-pages --via-engine``: the brute-force comparison of ``*נים *לים`` (plan §3.6.4)
  against the engine's own page rows, on format 2 (the reversed-words field) and format 1
  (the scan); an engine that looks a leading star up by Phase 1's prefix list is caught;
- ``crossing-diff``: the rows decision 10 changes, counted by kind, and the engine with its
  seams checked against the seams' simulation; an engine that kept today's route is caught.
"""
import json
import os
import sys
from unittest.mock import patch

import pytest

import shared.responsa_match as rm
from shared.config import Config
from shared.search_engine import SearchEngine
from tests.responsa_index_fixture import FakeMeta, build

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, 'scripts'))
import measure_responsa_wildcards as measure  # noqa: E402


def sid(n):
    return f"99000000{n:03d}0205171"


PAGES = {
    1: "ראו לבנים כחולים כאן",          # yes
    2: "אבנים 12לים",                  # no: a digit
    3: "בנים_ לים",                    # no: _ continues the word
    4: "שם הכהנים, הגדולים",            # yes: punctuation between the words
    5: "נתקנים[ כחולים",                # yes: a bracket glued on (§3.6.4's seven)
    6: "סוף בנים",                     # across the break into 7: no page of its own
    7: "לים שם",
    8: "אמר כהנים גדולים",              # yes
}
V08 = [(sid(n), 4000 + n, 1, text) for n, text in PAGES.items() if n != 7] + [(sid(6), 4006, 2, PAGES[7])]
WANT = 4                               # pages 1, 4, 5, 8


class _Meta(FakeMeta):
    def _load_heavy_caches_bg(self):
        pass


@pytest.fixture(scope='module')
def indexes(tmp_path_factory):
    base = tmp_path_factory.mktemp('missing_pages')
    return {'2': build(base / 'format2', V08), '1': build(base / 'format1', V08, seams=False,
                                                           reversed_field=False)}


def _run(root, out):
    with patch('shared.metadata_manager.MetadataManager', _Meta), \
            patch.object(SearchEngine, 'start_fl_id_index_build'), \
            patch.object(Config, 'BROWSE_MAP', os.path.join(root, 'browse_map.pkl')):
        SearchEngine._shared_browse_map = None
        rc = measure.main(['missing-pages', '--via-engine', '--index-dir', os.path.join(root, 'tantivy_db'),
                           '--corpus-file', os.path.join(root, 'v8.txt'), '--out-dir', str(out)])
        SearchEngine._shared_browse_map = None
    with open(out / 'missing-pages-engine.json', encoding='utf-8') as f:
        return rc, json.load(f)


@pytest.mark.parametrize('fmt', ['2', '1'])
def test_the_engine_finds_every_page_the_brute_force_finds(indexes, tmp_path, fmt):
    rc, data = _run(indexes[fmt], tmp_path)
    assert rc == measure.EXIT_OK
    assert data['route'] == 'engine (execute_search)'
    assert data['brute_force']['pages'] == WANT
    assert data['brute_only'] == 0 and data['index_only'] == 0


def test_a_leading_star_looked_up_by_the_prefix_list_is_caught(indexes, tmp_path):
    # Phase 1's meaning: *נים is נים with the 24 grammatical prefixes. The brute force
    # finds what that list misses (לבנים, הכהנים, נתקנים[, כהנים).
    real = rm.star_queries

    def prefix_list(star_forms, schema, field, tolerance, reversed_field=None):
        import tantivy
        from shared.responsa import expand_grammatical_prefixes
        out = real([sf for sf in star_forms if not sf.leading], schema, field, tolerance, reversed_field)
        terms = [f for sf in star_forms if sf.leading for stem in sf.runs[-1]
                 for f in expand_grammatical_prefixes(stem)]
        return out + ([tantivy.Query.term_set_query(schema, field, terms)] if terms else [])
    with patch.object(rm, 'star_queries', prefix_list):
        _rc, data = _run(indexes['2'], tmp_path)
    assert data['brute_only'] == WANT


# --- crossing-diff: decision 10 -----------------------------------------------------------

CROSSING_V08 = [(sid(301), 5301, 1, "אחד כהנים"), (sid(301), 5301, 2, "גדולים שני"),   # a true junction
                (sid(302), 5302, 1, "סוף כהנים")]                                      # V0.8's last page
CROSSING_V07 = [(sid(302), 5302, 1, "גדולים ישן")]       # the V0.7 copy: a false junction


@pytest.fixture
def config_restored():
    """crossing-diff relocates Config's index paths, as a real run must; put them back."""
    saved = {k: v for k, v in vars(Config).items() if isinstance(v, str)}
    yield
    for k, v in saved.items():
        setattr(Config, k, v)
    SearchEngine._shared_browse_map = None


def _crossing_diff(tmp_path, monkeypatch):
    root = build(tmp_path / 'crossings', CROSSING_V08, CROSSING_V07)
    monkeypatch.setattr(measure, 'CROSSING_QUERIES', [(mode, 'כהנים גדולים', {})
                                                      for mode in ('literal', 'variants', 'fuzzy', 'responsa')])
    with patch('shared.metadata_manager.MetadataManager', _Meta), \
            patch.object(SearchEngine, 'start_fl_id_index_build'):
        SearchEngine._shared_browse_map = None
        rc = measure.main(['crossing-diff', '--index-dir', os.path.join(root, 'tantivy_db'),
                           '--out-dir', str(tmp_path / 'out')])
    with open(tmp_path / 'out' / 'crossing-diff.json', encoding='utf-8') as f:
        return rc, json.load(f)['queries']


def test_crossing_diff_counts_what_decision_10_changes_and_the_engine_does_it(tmp_path, monkeypatch,
                                                                              config_restored):
    rc, rows = _crossing_diff(tmp_path, monkeypatch)
    assert rc == measure.EXIT_OK and len(rows) == 4
    for row in rows:
        # Before: both junctions, read through the whole manuscripts (302's V0.7 page has its
        # V0.8 page's uid, so its row counts as inside one page). After: 301's from its seam;
        # 302's V0.8 -> V0.7 restart is no junction a seam holds.
        assert row['today_rows'] == 2 and row['today_aggregate_rows'] == 2, row
        assert row['removed'] == 1 and row['added'] == 0 and row['after_rows'] == 1, row
        assert row['engine_after_rows'] == 1, row
        assert row['engine_vs_simulation']['engine_only'] == 0, row
        assert row['engine_vs_simulation']['simulation_only'] == 0, row


def test_crossing_diff_catches_an_engine_that_kept_todays_route(tmp_path, monkeypatch, config_restored):
    monkeypatch.setattr(SearchEngine, '_seams_ready', lambda self: False)
    _rc, rows = _crossing_diff(tmp_path, monkeypatch)
    assert all(row['engine_vs_simulation']['engine_only'] == 1 for row in rows)
