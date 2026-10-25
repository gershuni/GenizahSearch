"""scripts/posthog_report.py: the PostHog read-only report, with PostHog faked.

No test here touches the network: the client is handed a fake session.
"""

from __future__ import annotations

import json

import pytest

from scripts import posthog_report as pr


class FakeResponse:
    def __init__(self, status_code=200, payload=None, headers=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {'columns': ['n'], 'results': [[1]]}
        self.headers = headers or {}
        self.text = json.dumps(self._payload)

    def json(self):
        return self._payload


class FakeSession:
    """Answers each POST from ``responder(name, sql)`` and records the call."""

    def __init__(self, responder=None):
        self.calls = []
        self.responder = responder or (lambda name, sql: FakeResponse())

    def post(self, url, headers=None, json=None, timeout=None):  # noqa: A002 - mirrors requests
        self.calls.append({'url': url, 'headers': headers, 'body': json, 'timeout': timeout})
        return self.responder(json['name'].removeprefix('genizah_report_'), json['query']['query'])


@pytest.fixture
def key(monkeypatch):
    monkeypatch.setenv(pr.KEY_ENV, 'phx_test_key')


def test_missing_key_exits_2_without_network(monkeypatch, tmp_path):
    monkeypatch.delenv(pr.KEY_ENV, raising=False)
    session = FakeSession()
    assert pr.main(['--out', str(tmp_path / 'r.md')], session=session) == 2
    assert session.calls == []
    assert not (tmp_path / 'r.md').exists()


def test_ingestion_key_is_refused(monkeypatch, tmp_path):
    monkeypatch.setenv(pr.KEY_ENV, 'phc_CGTsV72FpOzKDrosXvnSD2t3giKNkjirI2migZCe0j7')
    session = FakeSession()
    assert pr.main(['--out', str(tmp_path / 'r.md')], session=session) == 2
    assert session.calls == []


def test_request_shape_and_report_written(key, tmp_path):
    session = FakeSession()
    out = tmp_path / 'report.md'
    assert pr.main(['--days', '7', '--out', str(out)], session=session) == 0

    first = session.calls[0]
    assert first['url'] == 'https://eu.posthog.com/api/projects/134161/query/'
    assert first['headers']['Authorization'] == 'Bearer phx_test_key'
    assert first['body']['query']['kind'] == 'HogQLQuery'
    # headline now + headline previous + every table
    assert len(session.calls) == 2 + len(pr.QUERIES)
    for call in session.calls:
        sql = call['body']['query']['query']
        assert sql.lstrip().upper().startswith('SELECT')
        assert '{' not in sql

    text = out.read_text(encoding='utf-8')
    assert text.startswith('# GenizahSearch PostHog report, last 7 days')
    assert '## Headline numbers' in text
    raw = json.loads(out.with_suffix('.json').read_text(encoding='utf-8'))
    assert raw['days'] == 7
    assert set(raw['results']) == {'headline', *(q.id for q in pr.QUERIES)}


def test_refused_key_on_first_query_writes_nothing(key, tmp_path):
    session = FakeSession(lambda name, sql: FakeResponse(401, {'detail': 'Invalid token'}))
    out = tmp_path / 'r.md'
    assert pr.main(['--out', str(out)], session=session) == 2
    assert len(session.calls) == 1
    assert not out.exists()


def test_one_failing_query_does_not_stop_the_report(key, tmp_path):
    def responder(name, sql):
        if name == 'web_exceptions':
            return FakeResponse(400, {'detail': 'Unable to resolve field: $exception_types'})
        return FakeResponse(200, {'columns': ['a', 'b'], 'results': [['x', 2]]})

    out = tmp_path / 'r.md'
    assert pr.main(['--out', str(out)], session=FakeSession(responder)) == 0
    text = out.read_text(encoding='utf-8')
    assert '**Query failed:** HTTP 400: Unable to resolve field: $exception_types' in text
    assert text.count('| x | 2 |') == len(pr.QUERIES) - 1


def test_every_query_failing_exits_1(key, tmp_path):
    session = FakeSession(lambda name, sql: FakeResponse(500, {'detail': 'boom'}))
    assert pr.main(['--out', str(tmp_path / 'r.md')], session=session) == 1


def test_rate_limit_is_retried(key, monkeypatch):
    monkeypatch.setattr(pr.time, 'sleep', lambda s: None)
    answers = iter([FakeResponse(429, {}, {'Retry-After': '2'}), FakeResponse()])
    client = pr.PostHogClient(pr.DEFAULT_HOST, pr.DEFAULT_PROJECT, 'phx_k',
                              session=FakeSession(lambda name, sql: next(answers)))
    assert client.query('SELECT 1', 'x') == (['n'], [[1]])


def test_only_selects_groups_and_ids_and_skips_headline(key, tmp_path):
    session = FakeSession()
    out = tmp_path / 'r.md'
    assert pr.main(['--only', 'errors,top_users', '--out', str(out)], session=session) == 0
    names = [c['body']['name'].removeprefix('genizah_report_') for c in session.calls]
    expected = [q.id for q in pr.QUERIES if q.group == 'errors' or q.id == 'top_users']
    assert names == expected


def test_unknown_only_value_is_an_error():
    with pytest.raises(SystemExit):
        pr.select_queries('nonsense')


def test_list_needs_no_key(monkeypatch, capsys):
    monkeypatch.delenv(pr.KEY_ENV, raising=False)
    assert pr.main(['--list']) == 0
    assert 'desktop_crashes' in capsys.readouterr().out


def test_queries_are_well_formed():
    ids = [q.id for q in pr.QUERIES]
    assert len(ids) == len(set(ids))
    for q in pr.QUERIES:
        assert q.group in pr.GROUPS
        sql = pr.render_sql(q.sql, 30)
        assert sql.upper().startswith('SELECT')
        assert 'LIMIT' in sql.upper()
        # read-only: nothing that writes
        assert not any(w in sql.upper().split() for w in ('INSERT', 'DELETE', 'UPDATE', 'DROP', 'ALTER'))


def test_previous_window_is_the_one_before():
    assert pr.window_condition(7) == 'timestamp >= now() - INTERVAL 7 DAY'
    assert pr.window_condition(7, previous=True) == (
        'timestamp >= now() - INTERVAL 14 DAY AND timestamp < now() - INTERVAL 7 DAY')


def test_unknown_token_is_refused():
    with pytest.raises(ValueError):
        pr.render_sql('SELECT 1 WHERE {NOPE}', 7)


@pytest.mark.parametrize('value, expected', [
    (None, ''),
    (3.0, '3'),
    (2.345, '2.35'),
    (True, 'yes'),
    ('a|b\nc', 'a\\|b c'),
    ('2026-10-24T12:34:56.123000Z', '2026-10-24 12:34'),
    (['x', 1], '["x",1]'),
    ('x' * 200, 'x' * (pr.MAX_CELL - 1) + '…'),
])
def test_format_cell(value, expected):
    assert pr.format_cell(value) == expected


def test_headline_change_column():
    table = pr.headline_table(['a', 'b', 'c'], [10, 5, 0], [5, 0, 0], 7)
    assert '| a | 10 | 5 | +100% |' in table
    assert '| b | 5 | 0 | new |' in table
    assert '| c | 0 | 0 | 0 |' in table


def test_default_output_is_git_ignored_scratch():
    assert pr.DEFAULT_OUT_DIR.name == '_tmp'
