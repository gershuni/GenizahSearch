# -*- coding: utf-8 -*-
"""x1-x3 ("Num Changes") is the real per-word limit of single-letter changes.

Owner ruling 2026-09-28 (option A): in every variant level x1, x2 and x3 each
mean what they say, kept per level (defaults x1 Basic, x2 Extended and Maximum);
the single value saved before levels had their own seeds Extended and Maximum
only. Raising the limit never pushes out a spelling a smaller one finds (the
two-letters-for-one ones especially), and a list cut at its budget says so.

Uses the real VariantManager and the real pair table (shared/unified_variants.py).
"""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from shared.config import Config
from shared.variants import (
    DEFAULT_MAX_CHANGES_BY_PRESET, VariantManager, max_changes_by_preset, variant_preset_of,
)

LONG = 'והמשפטים'          # 8 letters, most of them confusable
COMMON = 'ישראל'


def _settings(pairs=30, changes=2, **extra):
    return SimpleNamespace(variant_pairs_count=pairs, variant_max_changes=changes,
                           variant_min_word_len=2, variant_aggressive=False,
                           custom_variants={}, **extra)


def _same_length_distance(term, variants):
    return max(sum(a != b for a, b in zip(term, v)) for v in variants if len(v) == len(term))


@pytest.mark.parametrize('tier', ['variants', 'variants_extended', 'variants_maximum'])
@pytest.mark.parametrize('changes', [1, 2, 3])
def test_the_limit_is_the_setting_in_every_tier(tier, changes):
    """x3 did nothing anywhere and x2 nothing in Basic: the tier capped the setting."""
    variants = VariantManager(_settings(changes=changes)).get_variants(
        LONG, tier, limit=Config.REGEX_VARIANTS_LIMIT)
    assert _same_length_distance(LONG, variants) == changes


def test_short_words_keep_one_change():
    variants = VariantManager(_settings(changes=3)).get_variants('אב', 'variants_maximum', limit=8000)
    assert _same_length_distance('אב', variants) == 1


@pytest.mark.parametrize('word', [COMMON, LONG])
@pytest.mark.parametrize('pairs', [70, 150])
def test_raising_the_limit_keeps_every_spelling_the_lower_one_finds(word, pairs):
    """Measured before: ישראל at 150 pairs kept 1 of 2,717 two-letters-for-one
    spellings at x3 -- the third change filled the 8,000 budget first."""
    two = set(VariantManager(_settings(pairs=pairs, changes=2)).get_variants(word, 'variants', limit=8000))
    three = set(VariantManager(_settings(pairs=pairs, changes=3)).get_variants(word, 'variants', limit=8000))
    assert two <= three
    assert len(three) > len(two) or len(two) >= 8000


def test_a_cut_list_says_so():
    mgr = VariantManager(_settings(pairs=150, changes=3))
    assert mgr.variants_overflowed(LONG, 'variants', limit=50) is True
    assert VariantManager(_settings(pairs=30, changes=1)).variants_overflowed(COMMON, 'variants') is False


def test_the_cache_follows_the_change_settings():
    """The desktop changes the limit between searches without clearing the cache."""
    settings = _settings(changes=1)
    mgr = VariantManager(settings)
    one = mgr.get_variants(LONG, 'variants', limit=8000)
    settings.variant_max_changes = 3
    three = mgr.get_variants(LONG, 'variants', limit=8000)
    assert _same_length_distance(LONG, one) == 1 and _same_length_distance(LONG, three) == 3


def test_the_engine_says_plus_when_a_word_overflowed():
    from shared.search_engine import SearchEngine, consume_last_search_cutoff
    engine = SearchEngine.__new__(SearchEngine)
    engine.var_mgr = SimpleNamespace(get_variants=lambda *a, **k: ['x'],
                                     variants_overflowed=lambda term, mode, limit=None: term == 'big')
    consume_last_search_cutoff()
    engine._get_or_compute_variants(['small'], 'variants')
    assert consume_last_search_cutoff()['capped'] is False
    engine._get_or_compute_variants(['small', 'big'], 'variants')
    assert consume_last_search_cutoff()['capped'] is True


@pytest.mark.parametrize('pairs, level', [(10, 'basic'), (30, 'basic'), (69, 'basic'), (70, 'extended'),
                                          (149, 'extended'), (150, 'maximum'), (300, 'maximum')])
def test_levels(pairs, level):
    assert variant_preset_of(pairs) == level


def test_defaults_and_the_old_single_value():
    assert DEFAULT_MAX_CHANGES_BY_PRESET == {'basic': 1, 'extended': 2, 'maximum': 2}
    assert max_changes_by_preset() == {'basic': 1, 'extended': 2, 'maximum': 2}
    # The old global value seeds Extended and Maximum only: Basic stays x1.
    assert max_changes_by_preset(legacy=3) == {'basic': 1, 'extended': 3, 'maximum': 3}
    assert max_changes_by_preset({'basic': 9, 'extended': 'x'}) == {'basic': 3, 'extended': 2, 'maximum': 2}


def test_an_upgraded_desktop_profile_keeps_todays_basic(tmp_path, monkeypatch):
    """A lab_config.json from before (one value, 2 for everyone) gives Basic x1 --
    what Basic always ran -- and Extended/Maximum x2."""
    from shared import lab_settings
    path = tmp_path / 'lab_config.json'
    path.write_text(json.dumps({'variant_max_changes': 2}), encoding='utf-8')
    monkeypatch.setattr(lab_settings.Config, 'LAB_CONFIG_FILE', str(path))
    monkeypatch.setattr(lab_settings.Config, 'LAB_DIR', str(tmp_path))
    settings = lab_settings.LabSettings()
    assert settings.variant_max_changes_by_preset == {'basic': 1, 'extended': 2, 'maximum': 2}
    assert (settings.max_changes_for(30), settings.max_changes_for(70), settings.max_changes_for(150)) == (1, 2, 2)
    settings.variant_max_changes_by_preset = {'basic': 2, 'extended': 3, 'maximum': 1}
    settings.save()
    assert lab_settings.LabSettings().variant_max_changes_by_preset == {'basic': 2, 'extended': 3, 'maximum': 1}


# --- One canonical spelling order per word (external review, 2026-10-06) ---------------
# get_variants(limit=L) is the first L of one list per (word, mode, pairs, change
# settings), whatever was asked before; x1's list starts x2's and x2's starts x3's; the
# order does not depend on PYTHONHASHSEED; and variants_overflowed says exactly when
# spellings were left out. Every site in the engine that cuts a word's list marks "N+".

LIMITS = (1, 17, 100, 200, 316, 1000, 5000, 7999, 8000)


@pytest.mark.parametrize('word', [COMMON, LONG])
@pytest.mark.parametrize('changes', [1, 2, 3])
def test_every_limit_is_a_cut_of_one_list_whatever_was_asked_before(word, changes):
    """Slicing a list cached at a bigger budget gave another subset than generating at
    the smaller one: ישראל at 150 pairs, x2 at 8,000 then 200, against x3 cold at 200 --
    106 of the 200 differed. Both call orders happen in the engine."""
    def mgr():
        return VariantManager(_settings(pairs=150, changes=changes))
    full = mgr().get_variants(word, 'variants', limit=8000)
    assert full[0] == word and len(full) == len(set(full))
    for limit in LIMITS:                                    # cold, one limit each
        assert mgr().get_variants(word, 'variants', limit=limit) == full[:limit], limit
    big_first = mgr()
    big_first.get_variants(word, 'variants', limit=8000)
    for limit in LIMITS:
        assert big_first.get_variants(word, 'variants', limit=limit) == full[:limit], limit
    small_first = mgr()
    for limit in LIMITS:
        assert small_first.get_variants(word, 'variants', limit=limit) == full[:limit], limit
    assert small_first.get_variants(word, 'variants', limit=8000) == full


def test_the_reviewed_call_orders_agree():
    warm = VariantManager(_settings(pairs=150, changes=2))
    warm.get_variants(COMMON, 'variants', limit=8000)          # verifier / precompute first
    two = warm.get_variants(COMMON, 'variants', limit=200)     # then the index query
    three = VariantManager(_settings(pairs=150, changes=3)).get_variants(COMMON, 'variants', limit=200)
    assert two == three     # x2 has 4,871 spellings: its first 200 are x3's first 200


@pytest.mark.parametrize('word', [COMMON, LONG])
@pytest.mark.parametrize('pairs', [30, 70, 150])
@pytest.mark.parametrize('limit', [50, 200, 8000])
def test_a_higher_num_changes_starts_with_the_lower_ones_list(word, pairs, limit):
    """At the same limit x1's list is the start of x2's and x2's of x3's, so raising
    Num Changes never drops a spelling -- the multi-letter spellings' own changes
    included (והמשפטים at 150 pairs: x2 lost 104 of x1's 316, e.g. ארימשפטים)."""
    lists = [VariantManager(_settings(pairs=pairs, changes=c)).get_variants(word, 'variants', limit=limit)
             for c in (1, 2, 3)]
    for lower, higher in zip(lists, lists[1:]):
        assert higher[:len(lower)] == lower


def test_the_multi_letter_spellings_changes_survive_x2():
    one = VariantManager(_settings(pairs=150, changes=1)).get_variants(LONG, 'variants', limit=8000)
    two = VariantManager(_settings(pairs=150, changes=2)).get_variants(LONG, 'variants', limit=8000)
    assert len(one) == 316 and len(two) == 8000
    assert 'ארימשפטים' in one and 'ארימשפטים' in two
    assert not set(one) - set(two)


def test_closest_first():
    """The word, then its multi-letter spellings, then by the number of changes."""
    mgr = VariantManager(_settings(pairs=70, changes=2))      # 2,057 spellings: all of them
    full = mgr.get_variants(LONG, 'variants', limit=8000)
    assert not mgr.variants_overflowed(LONG, 'variants', limit=8000)
    multi, _cut = mgr._multichar_spellings(LONG, 'variants')
    assert multi and full[:1 + len(multi)] == [LONG, *multi]
    distances = [sum(a != b for a, b in zip(LONG, v)) for v in full[1:] if len(v) == len(LONG)]
    assert distances == sorted(distances) and set(distances) == {1, 2}


_SEED_SCRIPT = r'''
import hashlib, json, sys
from types import SimpleNamespace
sys.path.insert(0, {root!r})
from shared.variants import VariantManager
out = {{}}
for word, pairs, changes, limit, custom in json.loads({cases!r}):
    s = SimpleNamespace(variant_pairs_count=pairs, variant_max_changes=changes, variant_min_word_len=2,
                        variant_aggressive=False, custom_variants=dict.fromkeys(custom, True))
    got = VariantManager(s).get_variants(word, 'variants', limit=limit)
    key = '%d/%d/%d/%d/%s' % (len(out), pairs, changes, limit, len(got))
    out[key] = hashlib.sha256('\n'.join(got).encode('utf-8')).hexdigest()
print(json.dumps(out, sort_keys=True))
'''


def test_the_order_does_not_depend_on_the_hash_seed():
    """The mapping holds sets: which spellings a cut list kept followed PYTHONHASHSEED."""
    import os
    import subprocess
    import sys
    from pathlib import Path
    root = str(Path(__file__).resolve().parents[1])
    cases = json.dumps([[COMMON, 150, 2, 8000, []], [COMMON, 150, 3, 200, []], [LONG, 150, 2, 8000, []],
                        [LONG, 70, 3, 1000, []], [LONG, 30, 2, 200, []],
                        ['צא', 30, 1, 8000, [f'א={c}{c}' for c in 'בגדהוזחטי']]])
    script = _SEED_SCRIPT.format(root=root, cases=cases)
    runs = []
    for seed in ('0', '1', '12345'):
        env = dict(os.environ, PYTHONHASHSEED=seed)
        done = subprocess.run([sys.executable, '-c', script], env=env, capture_output=True, text=True,
                              timeout=300, cwd=root)
        assert done.returncode == 0, done.stderr[-2000:]
        runs.append(json.loads(done.stdout.strip().splitlines()[-1]))
    assert runs[0] == runs[1] == runs[2]


def test_a_list_that_holds_every_spelling_exactly_is_not_cut():
    """ישראל at 30 pairs x1 has 17 spellings: asking for 17 said "cut" (>= limit)."""
    full = VariantManager(_settings(pairs=30, changes=1)).get_variants(COMMON, 'variants', limit=8000)
    n = len(full)
    assert n == 17
    assert VariantManager(_settings(pairs=30, changes=1)).variants_overflowed(COMMON, 'variants', limit=n) is False
    assert VariantManager(_settings(pairs=30, changes=1)).variants_overflowed(COMMON, 'variants', limit=n - 1) is True
    warm = VariantManager(_settings(pairs=30, changes=1))
    warm.get_variants(COMMON, 'variants', limit=8000)
    assert warm.variants_overflowed(COMMON, 'variants', limit=n) is False
    assert warm.variants_overflowed(COMMON, 'variants', limit=n - 1) is True
    assert warm.variants_overflowed(COMMON, 'variants', limit=8000) is False


def test_a_list_stopped_at_the_generation_budget_is_cut():
    mgr = VariantManager(_settings(pairs=150, changes=2))
    assert len(mgr.get_variants(LONG, 'variants', limit=8000)) == 8000
    assert mgr.variants_overflowed(LONG, 'variants', limit=8000) is True


def _with_custom_pairs(n):
    """Basic x1 plus n custom multi-letter pairs א=בב, א=גג, ... (Basic's 30 real
    pairs hold no multi-letter one, so these are all the word gets)."""
    settings = _settings(pairs=30, changes=1)
    settings.custom_variants = {f'א={c}{c}': True for c in 'בגדהוזחטי'[:n]}
    return VariantManager(settings)


def test_the_multi_letter_cap_is_reported():
    """Nine custom pairs א=בב .. א=יי give צא nine multi-letter spellings; the cap
    (MAX_MULTICHAR_VARIANTS = 8) keeps eight and left the ninth (ציי) out unreported."""
    assert VariantManager.MAX_MULTICHAR_VARIANTS == 8
    mgr = _with_custom_pairs(9)
    got = mgr.get_variants('צא', 'variants', limit=8000)
    assert len(got) < 8000 and 'ציי' not in got and 'צבב' in got
    assert mgr.variants_overflowed('צא', 'variants', limit=8000) is True
    eight = _with_custom_pairs(8)
    assert 'צטט' in eight.get_variants('צא', 'variants', limit=8000)
    assert eight.variants_overflowed('צא', 'variants', limit=8000) is False   # all eight fit


def test_the_cache_holds_a_bounded_number_of_spellings():
    """Every cached list is up to 8,000 long now (a Responsa query asks for many
    prefixed forms): the cache drops its older half past _cache_max_spellings."""
    mgr = VariantManager(_settings(pairs=150, changes=2))
    mgr._cache_max_spellings = 10_000
    for word in (COMMON, LONG, 'שלום', 'אבג'):
        full = mgr.get_variants(word, 'variants', limit=8000)
        assert mgr.get_variants(word, 'variants', limit=8000) == full
        assert mgr._cached_spellings == sum(map(len, mgr._cache.values())) <= 10_000
        assert set(mgr._cache) == set(mgr._overflow)


@pytest.mark.parametrize('change', ['set_variant_level', 'set_settings', 'clear_cache'])
def test_a_reset_forgets_the_overflow_flags_with_the_lists(change):
    settings = _settings(pairs=150, changes=2)
    mgr = VariantManager(settings)
    assert mgr.variants_overflowed(LONG, 'variants', limit=8000) is True
    assert mgr._cache and mgr._overflow
    if change == 'set_variant_level':
        mgr.set_variant_level(30)
    elif change == 'set_settings':
        mgr.set_settings(_settings(pairs=30, changes=2))
    else:
        mgr.clear_cache()
    assert mgr._cache == {} and mgr._overflow == {}
    if change != 'clear_cache':
        assert mgr.variants_overflowed(LONG, 'variants', limit=8000) is False   # 249 at Basic x2


# --- Every engine site that cuts a word's list marks the search cut off -----------------
# Only the precompute route said "N+"; Responsa (also LOCAL and line-break), the
# position / composition index query, the verifier forms and the regex cut silently.
# A Basic/x2 Responsa expansion of והמשפטים (249 spellings) returned 200, capped=False.

def _engine(pairs, changes):
    from shared.search_engine import SearchEngine
    engine = SearchEngine.__new__(SearchEngine)
    engine.var_mgr = VariantManager(_settings(pairs=pairs, changes=changes))
    return engine


def _responsa_component(engine, word):
    from shared.responsa import ResponsaComponent
    return engine._expand_responsa_component(
        ResponsaComponent(words=[word]), {'variants': True, 'variant_mode': 'variants'})


def _local_responsa(engine, word):
    return engine._build_local_responsa_query_and_regex(
        word, 'literal', 0, {'responsa_mode': True, 'variants': True, 'variant_mode': 'variants'})


SITES_AT_200 = {   # Basic x2: והמשפטים has 249 spellings, ישראל 116
    'responsa component (incl. line-break)': _responsa_component,
    'LOCAL responsa': _local_responsa,
    'index query (position, composition)': lambda e, w: e.build_tantivy_query([w], 'variants'),
}
SITES_AT_8000 = {  # Maximum x2: והמשפטים has more than 8,000, ישראל 4,871
    'precompute': lambda e, w: e._get_or_compute_variants([w], 'variants'),
    'verifier forms': lambda e, w: e._verifier_forms(w, 'variants'),
    'regex': lambda e, w: e.build_regex_pattern([w], 'variants', 0),
}


def _capped_after(site, engine, word):
    from shared.search_engine import consume_last_search_cutoff
    consume_last_search_cutoff()
    site(engine, word)
    return consume_last_search_cutoff()['capped']


@pytest.mark.parametrize('name', sorted(SITES_AT_200))
def test_a_site_that_cuts_at_200_marks_the_search(name):
    site = SITES_AT_200[name]
    assert _capped_after(site, _engine(30, 2), LONG) is True
    assert _capped_after(site, _engine(30, 2), COMMON) is False
    assert _capped_after(site, _engine(30, 1), LONG) is False      # 25 spellings: all in


@pytest.mark.parametrize('name', sorted(SITES_AT_8000))
def test_a_site_that_cuts_at_8000_marks_the_search(name, monkeypatch):
    import shared.search_engine as se
    # The signal is under test, not the regex: compiling 8,000 mark-tolerant
    # alternatives costs seconds a word.
    monkeypatch.setattr(se, 'compile_search_regex', lambda pattern, flags=0: pattern)
    site = SITES_AT_8000[name]
    assert _capped_after(site, _engine(150, 2), LONG) is True
    assert _capped_after(site, _engine(150, 2), COMMON) is False


def test_the_whole_word_query_string_is_not_the_retrieval():
    """build_variant_query retrieves every verifier form; the 200 in its query string
    are not a cut (a false "N+" on every Variants word with 201..8,000 spellings)."""
    engine = _engine(30, 2)
    assert _capped_after(lambda e, w: e.build_tantivy_query([w], 'variants', note_cutoff=False),
                         engine, LONG) is False


# End to end on a tiny index: the main Variants search of a word with 249 spellings is
# complete (every form is retrieved as a whole token); a position search and a Responsa
# search over the same word use its first 200, and say so.

POS_PAGES = {'w1': f'{LONG} אבג', 'w2': f'דהו {LONG}\nאבג', 'c1': f'{COMMON} אבג'}


@pytest.fixture(scope='module')
def small_engine(tmp_path_factory):
    tantivy = pytest.importorskip('tantivy')
    import gc
    import os
    from unittest.mock import patch
    from shared.indexer import Indexer
    from shared.search_engine import SearchEngine
    from shared.search_tokenizer import register_search_tokenizers
    from shared.text_normalize import strip_search_diacritics

    b = tantivy.SchemaBuilder()
    b.add_text_field('unique_id', stored=True)
    b.add_text_field('content', stored=True, tokenizer_name='hebword')
    for f in ('content_head', 'content_tail', 'line_starts', 'line_ends'):
        b.add_text_field(f, stored=False, tokenizer_name='whitespace')
    b.add_text_field('content_search', stored=False, tokenizer_name='hebword')
    for f in ('source', 'full_header', 'shelfmark', 'scope', 'boundaries'):
        b.add_text_field(f, stored=True)
    root = tmp_path_factory.mktemp('variant_cut')
    db = os.path.join(str(root), 'tantivy_db')
    os.makedirs(db)
    idx = tantivy.Index(b.build(), path=db)
    register_search_tokenizers(idx)
    writer = idx.writer(heap_size=50_000_000, num_threads=1)
    for uid, text in POS_PAGES.items():
        writer.add_document(tantivy.Document(
            unique_id=uid, content=text, content_search=strip_search_diacritics(text), source='V0.8',
            full_header=uid, shelfmark=uid, scope='page', boundaries='',
            **Indexer._extract_position_fields(text)))
    writer.commit()
    writer.wait_merging_threads()
    del writer, idx

    class _Meta:
        def get_display_data(self, header, source):
            return {'shelfmark': header, 'title': '', 'img': '1', 'source': source,
                    'id': header, 'library_code': ''}

        def parse_full_id_components(self, header):
            return {'sys_id': None, 'ie_id': None, 'p_num': None, 'fl_id': None}

    with patch.object(Config, 'INDEX_DIR', str(root)):
        engine = SearchEngine(_Meta(), VariantManager(_settings(pairs=30, changes=2)),
                              worker_mode=True, open_local=False)
    yield engine
    del engine
    gc.collect()


def _search(engine, query, **kw):
    from shared.search_engine import consume_last_search_cutoff
    rows = engine.execute_search(query, kw.pop('mode', 'variants'), 0, corpus_scope='genizah', **kw)
    return {r['uid'] for r in rows}, consume_last_search_cutoff()['capped']


RESPONSA = {'responsa_mode': True, 'variants': True, 'variant_mode': 'variants'}


def test_end_to_end_main_variants_search_is_complete(small_engine):
    assert _search(small_engine, LONG) == ({'w1', 'w2'}, False)
    assert _search(small_engine, COMMON) == ({'c1'}, False)


def test_end_to_end_position_search_says_plus(small_engine):
    assert _search(small_engine, LONG, text_position='start') == ({'w1'}, True)
    assert _search(small_engine, COMMON, text_position='start') == ({'c1'}, False)


def test_end_to_end_responsa_searches_say_plus(small_engine):
    uids, capped = _search(small_engine, LONG, mode='literal', responsa_options=dict(RESPONSA))
    assert {'w1', 'w2'} <= uids and capped is True
    uids, capped = _search(small_engine, f'{LONG} | אבג', mode='literal', responsa_options=dict(RESPONSA))
    assert uids == {'w2'} and capped is True                     # the line-break search
    uids, capped = _search(small_engine, COMMON, mode='literal', responsa_options=dict(RESPONSA))
    assert uids == {'c1'} and capped is False
