# -*- coding: utf-8 -*-
"""Tests for tests/doc_response_shapes.py -- the helper that binds
docs/SEARCH_API.md's response examples to real response bodies.

Why a gate needs its own tests: the first version of this helper selected "the
response example" as `blocks[-1]`, the last fenced json block in a subsection.
That is a positional guess, not a check. It happened to be right for all three
Quick Start sections, so every test passed and the gate looked sound -- while a
Codex reviewer pointed out that a second example, or a trailing request body,
would silently redirect the comparison or defeat it entirely. A gate that can
quietly stop checking is worse than no gate, because the green build is then
evidence for a claim nobody is testing.

So these tests pin the two properties that matter:

  1. The selector FAILS LOUD when the document is reorganized -- no anchor and
     two anchors are both errors, never a silent skip.
  2. The coverage assertion accounts for EVERY fenced json block in the real
     document, so adding an unchecked example reddens the build.

The real document is never mutated here: the helper's path is monkeypatched to
a temp file. (An earlier round of this work rewrote docs/SEARCH_API.md through
Path.read_text/write_text and silently converted 1,264 CRLF lines to LF.)
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests import doc_response_shapes as drs


# ---------------------------------------------------------------------------
# The real document
# ---------------------------------------------------------------------------

def test_every_json_block_is_checked_or_named_exempt():
    """Adding a response example nobody checks must redden the build.

    This is the assertion that would have caught the 2026-09-08 round-2
    defects: five response examples existed, three were checked, and the two
    (later four) unchecked ones documented shapes the serializers have never
    emitted.
    """
    drs.assert_all_examples_are_covered()


@pytest.mark.parametrize('endpoint,which', drs.EXPECTED_EXAMPLES)
def test_every_expected_example_is_locatable_and_parses(endpoint, which):
    """Each (endpoint, which) pair resolves to a parseable JSON object.

    Separate from the per-endpoint response comparisons: those need a live app
    fixture, so a doc edit that breaks the ANCHOR would surface there as an
    unrelated-looking error inside an endpoint test. Here it points straight at
    the document.
    """
    example = drs.documented_example(endpoint, which)
    assert isinstance(example, dict) and example, (
        '%s/%s resolved to an empty or non-object example' % (endpoint, which)
    )
    assert drs.key_paths(example), '%s/%s pins no key paths' % (endpoint, which)


def test_expected_examples_covers_all_four_endpoints():
    """Every endpoint has at least its reference example checked."""
    covered = {e for e, _which in drs.EXPECTED_EXAMPLES}
    assert covered == set(drs.ENDPOINTS), (
        'endpoints with no documented-example gate: %s'
        % sorted(set(drs.ENDPOINTS) - covered)
    )


# ---------------------------------------------------------------------------
# Fail-loud selection (the F3 defect)
# ---------------------------------------------------------------------------

_MINIMAL_DOC = """# Search API

## Stability

text

## Quick Start

intro

### Search for manuscripts

```bash
curl -s ...
```

Response shape (truncated):

```json
{"schema_version": 1, "source": "search", "results": [{"uid": "U1"}]}
```

### Drill down to a manuscript page

Response shape (truncated):

```json
{"schema_version": 1, "source": "browse", "locator": {"p_num": 1}}
```

### Find composition parallels

Response shape (truncated):

```json
{"schema_version": 1, "source": "parallels", "count": 1, "total": 1}
```

### Error responses

```json
{"error": {"code": "rate_limited"}}
```

## Endpoint: POST /api/search

### Response example

```json
{"schema_version": 1, "source": "search", "count": 1}
```

## Endpoint: GET /api/browse

### Response example

```json
{"schema_version": 1, "source": "browse", "text_source": "pgp_transcription"}
```

## Endpoint: POST /api/parallels

### Response example

```json
{"schema_version": 1, "source": "parallels", "filtered": []}
```

## Endpoint: GET /api/capabilities

### Response example

```json
{"schema_version": 1, "request": {}, "endpoints": []}
```
"""


@pytest.fixture
def fake_doc(tmp_path, monkeypatch):
    """Point the helper at a synthetic document, byte-for-byte under our
    control, so these tests never touch docs/SEARCH_API.md."""
    def _write(text):
        p = tmp_path / 'SEARCH_API.md'
        p.write_bytes(text.encode('utf-8'))
        monkeypatch.setattr(drs, 'SEARCH_API_MD', p)
        return p
    return _write


def test_selector_reads_the_block_the_anchor_names(fake_doc):
    fake_doc(_MINIMAL_DOC)
    qs = drs.documented_example('search', 'quick_start')
    ref = drs.documented_example('search', 'reference')
    assert 'results' in qs and 'count' not in qs
    assert 'count' in ref and 'results' not in ref, (
        'the reference selector picked up the Quick Start block'
    )


def test_missing_anchor_raises_instead_of_checking_nothing(fake_doc):
    """A reorganization that drops the label must fail, not silently pass.

    The dangerous outcome is a gate that finds no example, compares nothing,
    and reports success.
    """
    fake_doc(_MINIMAL_DOC.replace('Response shape (truncated):', 'Here it is:'))
    with pytest.raises(AssertionError, match='no .*label found'):
        drs.documented_example('search', 'quick_start')


def test_two_anchors_in_one_section_raise_as_ambiguous(fake_doc):
    """Two candidate response examples must be disambiguated by a human.

    Under the old `blocks[-1]` selector this case silently checked the LAST
    one and ignored an error in the first.
    """
    fake_doc(_MINIMAL_DOC.replace(
        'Response shape (truncated):\n\n```json\n'
        '{"schema_version": 1, "source": "search", "results": [{"uid": "U1"}]}\n```',
        'Response shape (truncated):\n\n```json\n{"a": 1}\n```\n\n'
        'Response shape (truncated):\n\n```json\n{"b": 2}\n```',
    ))
    with pytest.raises(AssertionError, match='ambiguous'):
        drs.documented_example('search', 'quick_start')


def test_anchor_without_a_json_block_raises(fake_doc):
    fake_doc(_MINIMAL_DOC.replace(
        '```json\n{"schema_version": 1, "source": "search", "results": [{"uid": "U1"}]}\n```',
        '(example removed)',
    ))
    with pytest.raises(AssertionError, match='no ```json block follows'):
        drs.documented_example('search', 'quick_start')


def test_renamed_section_names_the_map_to_update(fake_doc):
    fake_doc(_MINIMAL_DOC.replace('### Search for manuscripts', '### Searching'))
    with pytest.raises(AssertionError, match='_QS_SECTIONS'):
        drs.documented_example('search', 'quick_start')


def test_unknown_endpoint_and_kind_raise():
    with pytest.raises(KeyError):
        drs.documented_example('nope', 'reference')
    with pytest.raises(KeyError):
        drs.documented_example('search', 'sideways')
    with pytest.raises(KeyError):
        # capabilities has no Quick Start entry; asking for one is a bug in the
        # caller, not a silently-empty result.
        drs.documented_example('capabilities', 'quick_start')


def test_coverage_flags_an_unchecked_response_example(fake_doc):
    """The point of the coverage assertion: a NEW example nobody checks fails.

    Simulates exactly what happened for real -- an extra response example
    added to the file with no test reaching it.
    """
    fake_doc(_MINIMAL_DOC + '\n## Endpoint: POST /api/whatever\n\n'
             '### Response example\n\n```json\n{"schema_version": 1}\n```\n')
    with pytest.raises(AssertionError, match='match neither'):
        drs.assert_all_examples_are_covered()


# ---------------------------------------------------------------------------
# Path extraction and the null-parent exemption (the F4 defect)
# ---------------------------------------------------------------------------

def test_key_paths_descends_lists_and_drops_the_index():
    paths = drs.key_paths({'results': [{'uid': 'U1', 'locator': {'p_num': '3'}}]})
    assert paths == {'results', 'results.uid', 'results.locator',
                     'results.locator.p_num'}


def test_missing_from_reports_a_documented_key_the_body_lacks(fake_doc):
    fake_doc(_MINIMAL_DOC)
    missing = drs.missing_from({'schema_version': 1, 'source': 'search'},
                               'search', 'reference')
    assert missing == {'count'}


def test_missing_from_allows_a_truncated_view(fake_doc):
    """A real body may carry MORE than the example shows.

    The check is deliberately one-directional: adding the reverse would redden
    the build every time a serializer gains a field, which trains people to
    delete the assertion rather than fix the doc.
    """
    fake_doc(_MINIMAL_DOC)
    body = {'schema_version': 1, 'source': 'search', 'count': 1,
            'undocumented_extra': True, 'warnings': []}
    assert drs.missing_from(body, 'search', 'reference') == set()


@pytest.mark.parametrize('empty', [None, [], {}, ''])
def test_documented_sub_shape_under_an_empty_parent_is_exempt(fake_doc, empty):
    """`metadata.pgp` is legitimately null when PGP has nothing for a page, and
    the document still has to say what it looks like when populated.

    Without this exemption, documenting any nullable sub-object would fail
    against a fixture that happens to return it empty -- and the natural
    "fix" would be to stop documenting the sub-shape at all.
    """
    fake_doc(_MINIMAL_DOC.replace(
        '{"schema_version": 1, "source": "search", "count": 1}',
        json.dumps({'schema_version': 1, 'source': 'search', 'count': 1,
                    'metadata': {'pgp': {'pgpid': 1, 'tags': ['x']}}}),
    ))
    body = {'schema_version': 1, 'source': 'search', 'count': 1,
            'metadata': {'pgp': empty}}
    assert drs.missing_from(body, 'search', 'reference') == set()


def test_an_absent_parent_is_not_exempt(fake_doc):
    """The exemption covers present-but-empty, NOT missing.

    A body that omits `metadata` entirely is a broken promise and must fail --
    otherwise the exemption would swallow the very defect class this gate
    exists for.
    """
    fake_doc(_MINIMAL_DOC.replace(
        '{"schema_version": 1, "source": "search", "count": 1}',
        json.dumps({'schema_version': 1, 'source': 'search', 'count': 1,
                    'metadata': {'pgp': {'pgpid': 1}}}),
    ))
    missing = drs.missing_from(
        {'schema_version': 1, 'source': 'search', 'count': 1}, 'search', 'reference')
    assert missing == {'metadata', 'metadata.pgp', 'metadata.pgp.pgpid'}


def test_real_doc_is_crlf_and_this_test_run_did_not_change_it():
    """Guard against the round-trip that once flattened this file.

    `Path.read_text()` + `write_text()` on docs/SEARCH_API.md silently rewrote
    1,264 CRLF lines as LF, and `grep -c '\\r'` reported every line as
    CR-terminated on the damaged file -- so only a byte-level count catches it.
    The helper only ever READS the document, and this pins that.
    """
    raw = Path(drs.SEARCH_API_MD).read_bytes()
    crlf = raw.count(b'\r\n')
    bare_lf = raw.count(b'\n') - crlf
    assert bare_lf == 0, (
        'docs/SEARCH_API.md has %d bare-LF lines (expected pure CRLF); '
        'something round-tripped it through read_text/write_text' % bare_lf
    )
    assert crlf > 1000, 'unexpectedly short document: %d lines' % crlf


def test_no_documented_key_contains_a_dot():
    """A literal key with a dot in it would collide with a nested path.

    `key_paths` joins nesting with '.', so a hypothetical literal key
    `"a.b": 1` would render as the path `a.b` and be indistinguishable from
    `{"a": {"b": 1}}` -- one could satisfy the gate in place of the other, in
    either direction. Codex raised this against the helper; it is latent rather
    than live (no example has such a key today, and none of these serializers
    produces one), so the honest fix is this assertion rather than machinery to
    disambiguate a case that does not exist. If a serializer ever does emit a
    dotted key, this fails and the path encoding has to change.
    """
    offenders = {}
    for endpoint, which in drs.EXPECTED_EXAMPLES:
        offenders.update(_dotted_keys(drs.documented_example(endpoint, which),
                                      '%s/%s' % (endpoint, which)))
    assert offenders == {}, (
        'documented examples contain literal dotted keys, which collide with '
        'nested paths in key_paths(): %s' % offenders
    )


def _dotted_keys(obj, where):
    """Literal keys containing a '.', keyed by where they were found."""
    found = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            if '.' in k:
                found[where] = found.get(where, []) + [k]
            found.update(_dotted_keys(v, where))
    elif isinstance(obj, list) and obj:
        found.update(_dotted_keys(obj[0], where))
    return found
