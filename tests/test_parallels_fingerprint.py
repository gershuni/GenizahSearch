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


# =========================================================================
# _same_parallels_search: the identity DECISION the fingerprint feeds
# (round 6 -- Codex P2: a mixed fingerprint/legacy pair must fail closed).
# =========================================================================
from web.export_state import _same_parallels_search


def test_both_fingerprints_present_the_fingerprints_decide():
    stamped_x = {'search_fingerprint': 'aaaa', 'source_text': 'x'}
    assert _same_parallels_search(
        stamped_x, {'search_fingerprint': 'aaaa', 'source_text': 'y'})
    assert not _same_parallels_search(
        stamped_x, {'search_fingerprint': 'bbbb', 'source_text': 'x'})


def test_a_mixed_pair_fails_closed_in_both_directions():
    """One side stamped, one not: the payloads straddle the fingerprint
    deploy and cannot be verified as the same search. Same source_text must
    NOT recover the rows -- the other tab may have searched the same text
    with a different width, mode or filter set."""
    legacy = {'source_text': 'x'}
    stamped = {'search_fingerprint': 'aaaa', 'source_text': 'x'}
    assert not _same_parallels_search(legacy, stamped)
    assert not _same_parallels_search(stamped, legacy)


def test_a_legacy_pair_still_falls_back_to_source_text():
    assert _same_parallels_search({'source_text': 'x'}, {'source_text': 'x'})
    assert not _same_parallels_search({'source_text': 'x'},
                                      {'source_text': 'y'})


# ---------------------------------------------------------------------------
# The witness set is part of a search's identity.
# ---------------------------------------------------------------------------

W_PASTED_A = {'kind': 'pasted', 'text': 'aleph bet gimel', 'label': 'A'}
W_PASTED_B = {'kind': 'pasted', 'text': 'dalet he vav', 'label': 'B'}
W_PROMOTED = {'kind': 'manuscript', 'sys_id': '9912345678901234', 'label': 'C'}


def test_no_witnesses_hashes_exactly_as_before_the_parameter_existed():
    """Every fingerprint recorded before witnesses existed has to keep
    matching, or a stored composition-history entry stops recognising its own
    results."""
    assert fp(**BASE) == fp(**BASE, witnesses=None)
    assert fp(**BASE) == fp(**BASE, witnesses=[])


def test_adding_a_witness_moves_the_hash():
    """The same seed searched with three witnesses and with seventeen
    produces different results; recovering one set's rows for the other would
    be silently wrong."""
    assert fp(**BASE, witnesses=[W_PASTED_A]) != fp(**BASE)
    assert fp(**BASE, witnesses=[W_PASTED_A, W_PASTED_B]) != \
        fp(**BASE, witnesses=[W_PASTED_A])


def test_the_witness_list_is_order_insensitive():
    """The set searched is what shapes the results; the order they were typed
    in is not."""
    assert fp(**BASE, witnesses=[W_PASTED_A, W_PASTED_B]) == \
        fp(**BASE, witnesses=[W_PASTED_B, W_PASTED_A])


def test_a_relabelled_witness_keeps_its_identity():
    """Labels are user-editable. Hashing them would make a rename look like a
    different search."""
    renamed = dict(W_PASTED_A, label='renamed entirely')
    assert fp(**BASE, witnesses=[renamed]) == fp(**BASE, witnesses=[W_PASTED_A])


def test_two_witnesses_sharing_a_label_are_still_distinguished():
    """The inverse, and the reason a label-only canonicalisation is not
    enough: a metadata-only entry does not identify the text that was
    searched, so two different pastes under one label would collide."""
    same_label = dict(W_PASTED_B, label='A')
    assert fp(**BASE, witnesses=[same_label]) != fp(**BASE, witnesses=[W_PASTED_A])


def test_editing_a_pasted_witness_moves_the_hash():
    edited = dict(W_PASTED_A, text=W_PASTED_A['text'] + ' zayin')
    assert fp(**BASE, witnesses=[edited]) != fp(**BASE, witnesses=[W_PASTED_A])


def test_a_promoted_witness_is_identified_by_its_sys_id():
    """It carries no text (re-fetchable from the corpus), so the sys_id has
    to be what identifies it."""
    no_text = {'kind': 'manuscript', 'sys_id': '9912345678901234', 'label': 'x'}
    assert fp(**BASE, witnesses=[no_text]) == fp(**BASE, witnesses=[W_PROMOTED])
    other = dict(W_PROMOTED, sys_id='9999999999999999')
    assert fp(**BASE, witnesses=[other]) != fp(**BASE, witnesses=[W_PROMOTED])


def test_a_pasted_and_a_promoted_witness_never_collide():
    collide = {'kind': 'pasted', 'text': '9912345678901234'}
    assert fp(**BASE, witnesses=[collide]) != fp(**BASE, witnesses=[W_PROMOTED])


def test_the_witness_payload_carries_a_digest_not_the_text():
    """A 25 x 20,000-character payload would be hashed on every search; the
    digest keeps it bounded. Asserted by behaviour: two texts sharing a long
    prefix must still differ."""
    a = {'kind': 'pasted', 'text': 'x' * 5000 + 'A'}
    b = {'kind': 'pasted', 'text': 'x' * 5000 + 'B'}
    assert fp(**BASE, witnesses=[a]) != fp(**BASE, witnesses=[b])
