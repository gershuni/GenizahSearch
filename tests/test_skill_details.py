"""Portable details transport preserves source pages and HTTP failures."""
from unittest.mock import Mock

import pytest

from skills.cairo_genizah_research.scripts import details


@pytest.fixture
def transport(monkeypatch):
    monkeypatch.delenv('GENIZAH_API_BASE', raising=False)
    monkeypatch.setattr(details.throttle, 'acquire', Mock())
    response = Mock(status_code=200, headers={})
    response.json.return_value = {'records': [], 'next_offset': None, 'snapshot': 'saved'}
    get = Mock(return_value=response)
    monkeypatch.setattr(details.requests, 'get', get)
    return get, response


def test_pages_forward_snapshot_and_do_not_autofetch(transport):
    get, response = transport
    body = details.call_details(sys_id='99001', section='pgp_sources', offset=10,
                                snapshot='saved', base_url='https://example.org')
    assert body == response.json.return_value
    assert get.call_count == 1
    assert get.call_args.args[0] == 'https://example.org/api/chatgpt/manuscript-details'
    assert get.call_args.kwargs['params']['snapshot'] == 'saved'
    assert get.call_args.kwargs['params']['offset'] == 10
    assert details.throttle.acquire.call_args.args == ('details',)
    assert details.throttle.acquire.call_args.kwargs['rpm'] <= 24


def test_env_base_wins(transport, monkeypatch):
    monkeypatch.setenv('GENIZAH_API_BASE', 'https://configured.example')
    details.call_details(sys_id='99001', section='fjms_catalog', base_url='https://ignored.example')
    assert transport[0].call_args.args[0].startswith('https://configured.example/')


@pytest.mark.parametrize('status,body', [(409, {'error': {'code': 'details_expired'}}),
                                      (503, {'error': {'code': 'details_pending'}}),
                                      (429, None), (404, None)])
def test_errors_preserve_http_and_retry_information(transport, status, body):
    get, response = transport
    response.status_code = status
    response.headers = {'Retry-After': '5'}
    response.json.return_value = body
    result = details.call_details(sys_id='99001', section='nli_bibliography')
    assert result['error']['http_status'] == status
    assert result['error']['retry_after'] == '5'
    assert get.call_count == 1


def test_invalid_continuation_never_calls_server(transport):
    result = details.call_details(sys_id='99001', section='fjms_bibliography', offset=10)
    assert result['error']['code'] == 'invalid_request'
    transport[0].assert_not_called()


def test_cli_prints_page_and_exit_status(transport, capsys):
    assert details._main(['--sys-id', '99001', '--section', 'fgp_sources']) == 0
    assert 'snapshot' in capsys.readouterr().out
    assert details._main(['--sys-id', '99001', '--section', 'fgp_sources', '--offset', '10']) == 1
