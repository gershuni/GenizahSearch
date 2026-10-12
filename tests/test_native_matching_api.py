"""The word-gap limit at the web interfaces (docs/plans/DESKTOP_NATIVE_MATCHING_PLAN.md 3.6, gate 10).

The engine rejects a word gap over MAX_WORD_GAP in Exact, Variants and Fuzzy searches and
leaves Responsa and Regex unlimited (shared/search_engine.py::_validate_word_gap). Three
interfaces state the same rule at the door:

- the API request model (web/search_api.py::SearchRequest), run over HTTP in tests/test_search_api_v2.py;
- the MCP search tool (web/mcp_server.py), run over the tool stack in tests/test_mcp_tools.py;
- the GPT Actions schema builder (integrations/chatgpt/build_schema.py).

This file holds what spans them: the numbers agree, the interfaces refuse exactly what the
engine refuses, and neither schema carries an unconditional `maximum` on gap (a bound there
would reject Responsa too). Regex is not an API or MCP search_mode (D-09), so there is no
Regex row; Title and Shelfmark take gap 0 only and are covered by their own coupling check.
"""
import importlib.util
import json
from pathlib import Path

import pytest

import web.mcp_server as mcp_server
import web.search_api as search_api
from shared.api_errors import APIError
from shared.responsa import _MAX_GAP_TOKEN
from shared.search_engine import MAX_WORD_GAP, _validate_word_gap
from web.search_api import SearchRequest, _SEARCH_MODE_TO_INTERNAL

CHATGPT_DIR = Path(__file__).resolve().parents[1] / 'integrations' / 'chatgpt'

# The text modes: those the gap limit speaks to (title/shelfmark are gap-0 only).
TEXT_MODES = ['exact', 'variants', 'variants_extended', 'fuzzy', 'responsa']
# ... up to re's repeat-count ceiling, which every mode meets (Codex, #395 round 2).
GAPS = [0, 1, MAX_WORD_GAP - 1, MAX_WORD_GAP, MAX_WORD_GAP + 1, 50, 1000,
        _MAX_GAP_TOKEN, _MAX_GAP_TOKEN + 1]


def load_build_schema():
    spec = importlib.util.spec_from_file_location('chatgpt_build_schema', CHATGPT_DIR / 'build_schema.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def api_refuses(mode, gap):
    try:
        SearchRequest(query='x', search_mode=mode, gap=gap)
    except APIError as exc:
        # The policy limit is a cross-field rule (mode x gap); re's ceiling is a plain
        # out-of-range value, whatever the mode.
        expected = 'invalid_request' if gap > _MAX_GAP_TOKEN else 'invalid_combination'
        assert exc.code == expected and exc.http_status == 400
        return True
    return False


def engine_refuses(mode, gap):
    """What execute_search does with the arguments the API handler gives it."""
    options = {'responsa_mode': True} if mode == 'responsa' else None
    try:
        _validate_word_gap(_SEARCH_MODE_TO_INTERNAL[mode], gap, options)
    except ValueError:
        return True
    return False


def test_the_limit_is_ten_and_every_interface_uses_the_engines():
    assert MAX_WORD_GAP == 10   # owner ruling 2026-10-12 (plan D3): changing it is a decision
    assert search_api.MAX_WORD_GAP is MAX_WORD_GAP and mcp_server.MAX_WORD_GAP is MAX_WORD_GAP
    # The Actions builder imports neither the website nor the corpus, so it carries its own copy.
    assert load_build_schema().MAX_WORD_GAP == MAX_WORD_GAP


@pytest.mark.parametrize('mode', TEXT_MODES)
@pytest.mark.parametrize('gap', GAPS)
def test_the_api_model_refuses_exactly_what_the_engine_refuses(mode, gap):
    """A request the API accepts must not fail inside the engine, and one the engine would
    reject must not be accepted and queued."""
    assert api_refuses(mode, gap) == engine_refuses(mode, gap)


@pytest.mark.parametrize('mode', TEXT_MODES)
@pytest.mark.parametrize('gap', GAPS)
def test_the_mcp_tool_refuses_exactly_what_the_api_model_refuses(mode, gap):
    assert (mcp_server._gap_limit_message(mode, gap) is not None) == api_refuses(mode, gap)


def test_the_mcp_message_names_the_limit_the_mode_and_the_unlimited_mode():
    message = mcp_server._gap_limit_message('variants', 50)
    assert str(MAX_WORD_GAP) in message and 'variants' in message and '50' in message
    assert "'responsa' has no limit" in message


def test_the_mcp_check_is_silent_without_a_gap_and_for_the_gap_zero_modes():
    assert mcp_server._gap_limit_message('exact', None) is None
    # title/shelfmark: the facade's own coupling check says their gap must be 0 --
    # and it answers first for a gap at re's ceiling too, as the API's validator
    # order does (Codex, #395 round 4), so the precheck stays out of it.
    assert mcp_server._gap_limit_message('title', 50) is None
    assert mcp_server._gap_limit_message('shelfmark', 50) is None
    assert mcp_server._gap_limit_message('title', _MAX_GAP_TOKEN + 1) is None
    assert mcp_server._gap_limit_message('shelfmark', _MAX_GAP_TOKEN + 1) is None
    # The API side of the same precedence, pinned so the two cannot drift apart.
    with pytest.raises(APIError) as exc:
        SearchRequest(query='x', search_mode='title', gap=_MAX_GAP_TOKEN + 1)
    assert exc.value.code == 'invalid_combination'


def test_the_gpt_schema_has_no_maximum_on_gap_names_the_limit_and_keeps_it_optional():
    gap = load_build_schema().build()['components']['schemas']['SearchRequest']['properties']['gap']
    assert (gap['type'], gap['minimum'], gap['default']) == ('integer', 0, 0)
    assert not {'maximum', 'exclusiveMaximum'} & set(gap), gap
    assert str(MAX_WORD_GAP) in gap['description'] and 'responsa' in gap['description']
    request = load_build_schema().build()['components']['schemas']['SearchRequest']
    assert 'gap' not in request['required']


def test_the_shipped_gpt_schema_has_no_maximum_on_gap():
    """The served file (integrations/chatgpt/openapi.json), which the builder rewrites."""
    spec = json.loads((CHATGPT_DIR / 'openapi.json').read_text(encoding='utf-8'))
    gap = spec['components']['schemas']['SearchRequest']['properties']['gap']
    assert gap['minimum'] == 0 and not {'maximum', 'exclusiveMaximum'} & set(gap), gap
    assert 'gap' not in spec['components']['schemas']['SearchRequest']['required']
