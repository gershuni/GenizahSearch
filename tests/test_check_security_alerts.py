# -*- coding: utf-8 -*-
"""scripts/check_security_alerts.py: what blocks, what a publish closes, and that a
check that could not run is never a pass; and the places that run it."""
from __future__ import annotations

import json
import os
import subprocess

import pytest
import yaml

from scripts import check_security_alerts as csa

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _read(*parts):
    with open(os.path.join(REPO, *parts), encoding='utf-8') as fh:
        return fh.read()


@pytest.fixture
def checkout(tmp_path):
    (tmp_path / 'requirements-lock.txt').write_text(
        'pillow==13.0.0\naiohttp==3.14.3\nlxml==6.1.3\n', encoding='utf-8')
    (tmp_path / 'requirements.txt').write_text(
        'Pillow==13.0.0  # case differs\nlxml>=6.1.0,<7.0   # a range\n', encoding='utf-8')
    return str(tmp_path)


def _dep(package='aiohttp', manifest='requirements-lock.txt', rng='<= 3.14.3', severity='high', number=1):
    return {'number': number, 'severity': severity, 'package': package, 'manifest': manifest,
            'range': rng, 'fixed': '3.14.4', 'summary': 's'}


def _code(path, severity='high', number=1):
    return {'number': number, 'severity': severity, 'rule': 'py/x', 'path': path, 'line': 1}


def _numbers(entries):
    return [alert['number'] for _kind, alert in entries]


@pytest.mark.parametrize('alert, where', [
    (_dep(), 'blocking'),
    (_dep(severity='critical'), 'blocking'),
    (_dep(severity='medium'), 'rest'),
    (_dep('Pillow', rng='>= 8.1.0, < 13.0.0'), 'publish'),
    (_dep('Pillow', 'requirements.txt', '< 13.0.0'), 'publish'),
    (_dep('Pillow', rng='>= 8.1.0, < 13.0.1'), 'blocking'),
    # A range is resolved by the lock.
    (_dep('lxml', 'requirements.txt', '< 6.1.0'), 'publish'),
    (_dep('lxml', 'requirements.txt', '< 6.2.0'), 'blocking'),
    # Removed from the manifest and from the lock.
    (_dep('gone', rng='< 9'), 'publish'),
    # Still in the lock, so still installed.
    (_dep('aiohttp', 'requirements.txt'), 'blocking'),
    # A manifest deleted here: fixed only if the lock no longer has the package.
    (_dep('gone', manifest='no-such-file.txt'), 'publish'),
    (_dep(manifest='no-such-file.txt'), 'blocking'),
    # What cannot be worked out is not fixed.
    (_dep('Pillow', rng='whatever'), 'blocking'),
    (_dep('Pillow', rng=None), 'blocking'),
])
def test_dependabot(checkout, alert, where):
    blocking, publish, rest = csa.sort_out({'dependabot': [alert]}, checkout)
    got = {'blocking': blocking, 'publish': publish, 'rest': rest}
    assert {name for name, entries in got.items() if entries} == {where}


def test_without_a_lock_nothing_counts_as_removed(checkout):
    os.remove(os.path.join(checkout, 'requirements-lock.txt'))
    for alert in (_dep('gone', manifest='no-such-file.txt'), _dep('gone', 'requirements.txt')):
        blocking, publish, _rest = csa.sort_out({'dependabot': [alert]}, checkout)
        assert (len(blocking), len(publish)) == (1, 0)


@pytest.mark.parametrize('alert, blocks', [
    (_code('web/api.py'), True),
    (_code('web/api.py', 'critical'), True),
    (_code('web/api.py', 'medium'), False),
    (_code('web/api.py', None), False),
    (_code('tests/test_x.py'), False),
    (_code('web/tests/x.py'), True),
])
def test_code_scanning(checkout, alert, blocks):
    blocking, publish, rest = csa.sort_out({'code-scanning': [alert]}, checkout)
    assert bool(blocking) is blocks and bool(rest) is not blocks and not publish


def test_every_secret_blocks(checkout):
    blocking, _publish, _rest = csa.sort_out({'secret-scanning': [{'number': 7, 'type': 'T'}]}, checkout)
    assert _numbers(blocking) == [7]


def test_the_secret_itself_is_never_asked_for():
    assert 'secret:' not in csa.QUERIES['secret-scanning'] and '.secret ' not in csa.QUERIES['secret-scanning']
    assert csa.QUERIES['secret-scanning'] == '.[] | {number, type: .secret_type_display_name} | @json'


def test_the_report_fails_only_while_something_blocks(checkout):
    lines = []
    assert csa.report({'dependabot': [_dep()]}, root=checkout, out=lines.append) == 1
    assert any('aiohttp (1 high, fixed in 3.14.4)' in line for line in lines)
    lines.clear()
    quiet = {'dependabot': [_dep('Pillow', rng='< 13.0.0')], 'code-scanning': [_code('tests/a.py')]}
    assert csa.report(quiet, root=checkout, out=lines.append) == 0
    assert '0 need a decision, 1 fixed here' in lines[0]


def test_all_lists_each_alert_with_its_page(checkout):
    lines = []
    csa.report({'code-scanning': [_code('web/api.py', number=82)]}, show_all=True, root=checkout,
               out=lines.append)
    assert any('https://github.com/gershuni/GenizahSearch/security/code-scanning/82' in x for x in lines)


class _Gh:
    """Stands in for subprocess.run: one answer for every `gh api` call."""

    def __init__(self, stdout='', returncode=0, stderr='', raises=None):
        self.answer = subprocess.CompletedProcess([], returncode, stdout, stderr)
        self.raises = raises
        self.commands = []

    def __call__(self, cmd, **kwargs):
        self.commands.append(cmd)
        if self.raises:
            raise self.raises
        return self.answer


@pytest.mark.parametrize('gh', [
    _Gh(raises=FileNotFoundError()),
    _Gh(raises=subprocess.TimeoutExpired('gh', 1)),
    _Gh(returncode=1, stderr='gh: Not Found (HTTP 404)'),
    _Gh(returncode=4),
    _Gh(stdout='not json\n'),
], ids=['no gh', 'timeout', 'http error', 'silent error', 'unreadable'])
def test_a_check_that_could_not_run_is_not_a_pass(monkeypatch, capsys, gh):
    monkeypatch.setattr(csa.subprocess, 'run', gh)
    assert csa.main([]) == 2
    assert 'NOT CHECKED' in capsys.readouterr().out


def test_one_kind_that_fails_fails_the_whole_check(monkeypatch):
    def run(cmd, **kwargs):
        failed = any('secret-scanning' in part for part in cmd)
        return subprocess.CompletedProcess(cmd, 1 if failed else 0, '', 'HTTP 403' if failed else '')

    monkeypatch.setattr(csa.subprocess, 'run', run)
    assert csa.main([]) == 2


def test_nothing_open_passes(monkeypatch):
    gh = _Gh()
    monkeypatch.setattr(csa.subprocess, 'run', gh)
    assert csa.main([]) == 0
    asked = ' '.join(part for cmd in gh.commands for part in cmd)
    assert all(f'/{kind}/alerts?state=open' in asked for kind in csa.QUERIES)
    assert all('--paginate' in cmd for cmd in gh.commands)


def test_answers_are_read_line_by_line(monkeypatch):
    gh = _Gh(stdout='\n'.join(json.dumps(_dep(number=n)) for n in (1, 2)) + '\n')
    monkeypatch.setattr(csa.subprocess, 'run', gh)
    assert [a['number'] for a in csa.fetch('dependabot')] == [1, 2]


def test_session_start_and_the_release_run_it():
    assert 'python scripts\\check_security_alerts.py' in _read('scripts', 'init.ps1')
    assert 'python scripts/check_security_alerts.py' in _read('.claude', 'skills', 'release', 'SKILL.md')


def test_the_audit_workflow():
    workflow = yaml.safe_load(_read('.github', 'workflows', 'dependency-audit.yml'))
    on = workflow.get('on', workflow.get(True))  # PyYAML reads the bare key as True
    assert 'requirements*.txt' in on['pull_request']['paths']
    assert on['schedule']
    job = workflow['jobs']['dependency-audit']
    # Scheduled only where the minutes are free; never skipped for a PR.
    assert job['if'] == "github.event_name != 'schedule' || github.repository == 'gershuni/GenizahSearch'"
    audit = job['steps'][-1]['run']
    # Every pin is audited, whatever platform it is for: markers are cut first.
    for name, copy in (('requirements-lock.txt', 'lock.txt'), ('requirements-atlas-bake.txt', 'atlas.txt')):
        assert "sed 's/;.*$//' " + name + ' > "$RUNNER_TEMP/' + copy + '"' in audit
        assert f'-r "$RUNNER_TEMP/{copy}"' in audit
    assert 'pip-audit --no-deps --disable-pip' in audit
    assert workflow['permissions'] == {'contents': 'read'}
    ci_jobs = yaml.safe_load(_read('.github', 'workflows', 'ci.yml'))['jobs']
    assert 'dependency-audit' not in ci_jobs
