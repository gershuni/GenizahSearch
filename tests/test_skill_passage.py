"""Transport compatibility and failure semantics for portable passage searches."""
from unittest.mock import Mock

import pytest

from skills.cairo_genizah_research.scripts import parallels


@pytest.fixture
def post(monkeypatch):
    monkeypatch.setattr(parallels.throttle, 'acquire', lambda _: None)
    response = Mock(headers={})
    response.json.return_value = {'results': [], 'warnings': []}
    post = Mock(return_value=response)
    monkeypatch.setattr(parallels.requests, 'post', post)
    return post


def test_passage_sends_letter_method_without_chunk_knobs(post):
    parallels.call_parallels(text='המלך המשפט', method='passage')
    assert post.call_args.kwargs['json'] == {'method': 'passage', 'text': 'המלך המשפט'}


def test_legacy_request_unchanged(post):
    parallels.call_parallels(text='test')
    assert post.call_args.kwargs['json'] == {'text': 'test', 'chunk_size': 5, 'mode': 'exact'}


@pytest.mark.parametrize('option', [{'mode': 'variants'}, {'chunk_size': 6},
                                   {'max_freq': 10}, {'boundary_mode': 'combined'}])
def test_reject_unsupported_passage_tuning_without_request(post, option):
    result = parallels.call_parallels(text='test', method='passage', **option)
    assert result['error']['code'] == 'passage_option_unsupported'
    post.assert_not_called()


def test_witnesses_replace_text(post):
    witnesses = [{'label': 'A', 'text': 'one'}, {'label': 'B', 'text': 'two'}]
    parallels.call_parallels(text='', method='passage', witnesses=witnesses, sort='fused')
    assert post.call_args.kwargs['json'] == {
        'method': 'passage', 'witnesses': witnesses, 'sort': 'fused'}


def test_unavailable_is_not_retried_or_downgraded(post):
    post.return_value.json.return_value = {'error': {'code': 'passage_unavailable'}}
    result = parallels.call_parallels(text='test', method='passage')
    assert result['error']['code'] == 'passage_unavailable'
    assert post.call_count == 1


def test_cli_witness_file(tmp_path, post):
    path = tmp_path / 'witnesses.json'
    path.write_text('[{"text":"first"},{"text":"second"}]', encoding='utf-8')
    parallels._main(['--method', 'passage', '--witnesses-file', str(path)])
    assert 'text' not in post.call_args.kwargs['json']
    assert len(post.call_args.kwargs['json']['witnesses']) == 2
