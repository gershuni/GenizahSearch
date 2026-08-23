# -*- coding: utf-8 -*-
"""EXECUTED tests for the parallels search-identity fingerprint.

The workflow review of PR #325 found the whole cross-tab identity guard was
pinned only by source text -- no test ever CALLED it, so no test could prove
that changing an input changes the hash, or that two orderings of the same
multi-select agree. These tests call the real function.
"""
import pytest

from web.export_state import compute_parallels_search_fingerprint as fp

BASE = dict(
    text='ברוך אתה',
    engine='passage',
    width='widest-40',
    chunk_size=5,
    mode='exact',
    max_freq=50,
    filter_text='',
    deep_scan=False,
    boundary_mode='full',
    boundary_delimiter='\n',
    boundary_boost=1.5,
    min_boundary_matches=0,
    min_delimiter_distance=3,
    variant_level=None,
    variant_max_changes=None,
    library_mode='hide',
    library_filter=['CUL', 'JTS'],
    restrict={'99001', '99002'},
    excluded={'99009'},
    filters={'domains': ['liturgy', 'piyyut'], 'include_mode': True},
)

#: One DIFFERENT value per input. Every one must move the hash.
CHANGED = {
    'text': 'ברוך אתה ה',
    'engine': 'chunk',
    'width': 'max-40',
    'chunk_size': 6,
    'mode': 'variants',
    'max_freq': 40,
    'filter_text': 'גניזה',
    'deep_scan': True,
    'boundary_mode': 'combined',
    'boundary_delimiter': '.',
    'boundary_boost': 2.0,
    'min_boundary_matches': 2,
    'min_delimiter_distance': 4,
    'variant_level': 30,
    'variant_max_changes': 2,
    'library_mode': 'show_only',
    'library_filter': ['CUL'],
    'restrict': {'99001'},
    'excluded': set(),
    'filters': {'domains': ['liturgy'], 'include_mode': True},
}


def test_the_same_search_hashes_the_same():
    assert fp(**BASE) == fp(**dict(BASE))


def test_the_fingerprint_is_short_and_hex():
    value = fp(**BASE)
    assert len(value) == 16
    assert all(c in '0123456789abcdef' for c in value)


@pytest.mark.parametrize('key', sorted(CHANGED))
def test_every_input_moves_the_hash(key):
    """If an input can change the returned buckets but not the fingerprint,
    two different searches share an identity and a reload can recover the
    wrong tab's rows. This is the gate that source-text pins could not be."""
    other = dict(BASE)
    other[key] = CHANGED[key]
    assert fp(**other) != fp(**BASE), (
        f'changing {key!r} left the fingerprint unchanged')


@pytest.mark.parametrize('key', ['library_filter', 'restrict', 'excluded'])
def test_set_like_inputs_are_order_insensitive(key):
    """The order a user clicks multi-select chips in is not part of the
    search. An identity that splits on it makes recovery silently never
    fire -- the quiet half of the failure."""
    base = dict(BASE)
    base[key] = ['b', 'a', 'c']
    reordered = dict(BASE)
    reordered[key] = ['c', 'b', 'a']
    assert fp(**base) == fp(**reordered)


def test_a_python_set_hashes_stably():
    """Sets have no deterministic iteration order across values; hashing one
    raw (via json default=str) would give a search a different identity on
    different runs."""
    a = dict(BASE)
    a['restrict'] = {'99005', '99001', '99003'}
    b = dict(BASE)
    b['restrict'] = {'99003', '99005', '99001'}
    assert fp(**a) == fp(**b)


def test_filter_lists_are_order_insensitive_inside_the_dict():
    a = dict(BASE)
    a['filters'] = {'domains': ['piyyut', 'liturgy'], 'authors': ['x', 'y']}
    b = dict(BASE)
    b['filters'] = {'domains': ['liturgy', 'piyyut'], 'authors': ['y', 'x']}
    assert fp(**a) == fp(**b)


def test_none_filters_and_empty_filters_are_distinguishable_from_a_real_one():
    none_fp = fp(**dict(BASE, filters=None))
    real_fp = fp(**BASE)
    assert none_fp != real_fp


def test_text_none_and_empty_agree():
    """A missing text and an empty text are the same non-search."""
    assert fp(**dict(BASE, text=None)) == fp(**dict(BASE, text=''))


def test_positional_arguments_are_rejected():
    """Keyword-only by design: a positional call could silently shift values
    between inputs and produce a plausible-looking wrong identity."""
    with pytest.raises(TypeError):
        fp('some text', 'passage')


def test_the_defaults_cover_a_partial_caller():
    """The history-restore path knows only a subset of the inputs; it must be
    able to call the helper without inventing values for the rest."""
    value = fp(text='abc', engine='history', chunk_size=5, mode='exact')
    assert len(value) == 16
    assert value != fp(text='abc', engine='history', chunk_size=6, mode='exact')
