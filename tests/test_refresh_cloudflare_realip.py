"""scripts/refresh_cloudflare_realip.py: refuses bad lists, tests before reload, rolls back."""

from __future__ import annotations

import http.client
import io
import subprocess
import urllib.error
import urllib.request

import pytest

from scripts import refresh_cloudflare_realip as r

V4 = '\n'.join([
    '173.245.48.0/20', '103.21.244.0/22', '103.22.200.0/22', '103.31.4.0/22',
    '141.101.64.0/18', '108.162.192.0/18', '190.93.240.0/20', '188.114.96.0/20',
    '197.234.240.0/22', '198.41.128.0/17', '162.158.0.0/15', '104.16.0.0/13',
    '104.24.0.0/14', '172.64.0.0/13', '131.0.72.0/22'])   # no trailing newline, as served
V6 = '\n'.join([
    '2400:cb00::/32', '2606:4700::/32', '2803:f800::/32', '2405:b500::/32',
    '2405:8100::/32', '2a06:98c0::/29', '2c0f:f248::/32'])
LM = 'Wed, 07 Oct 2026 13:35:46 GMT'


def _fetcher(v4=V4, v6=V6):
    def fetch(url):
        return (v4 if url == r.IPV4_URL else v6), LM
    return fetch


class _Runner:
    def __init__(self, test_rc=0, reload_rc=0):
        self.calls = []
        self.test_rc, self.reload_rc = test_rc, reload_rc

    def __call__(self, cmd, **_kw):
        self.calls.append(list(cmd))
        rc = self.test_rc if cmd[-1] == '-t' else self.reload_rc
        return subprocess.CompletedProcess(cmd, rc, stdout='', stderr='boom' if rc else '')


def _kinds(run):
    return ['test' if c[-1] == '-t' else 'reload' for c in run.calls]


def test_parse_accepts_the_real_lists():
    assert len(r.parse_ranges(V4, 4)) == 15
    assert len(r.parse_ranges(V6, 6)) == 7


@pytest.mark.parametrize('text,version', [
    ('', 4), ('   \n\n', 6),                                   # empty
    ('<html><body>Oops</body></html>', 4),                    # captive portal / error page
    (V4.replace('104.16.0.0/13', '104.16.0.0/33'), 4),        # malformed prefix
    (V4.replace('104.16.0.0/13', '104.16.0.1/13'), 4),        # host bits set
    (V4 + '\n0.0.0.0/0', 4),                                  # would trust everyone
    (V4 + '\n10.0.0.0/8', 4),                                 # private
    (V4 + '\n127.0.0.0/8', 4),                                # loopback
    (V4 + '\n173.245.48.0/20', 4),                            # duplicate
    (V4 + '\n2400:cb00::/32', 4),                             # wrong family
    (V6 + '\n::/0', 6),
    (V6.replace('2400:cb00::/32', '2400:CB00::/32'), 6),      # not canonical
    ('173.245.48.0/20\n103.21.244.0/22', 4),                  # truncated: too few
])
def test_parse_refuses_unsafe_lists(text, version):
    with pytest.raises(r.RangeListError):
        r.parse_ranges(text, version)


def test_first_install_writes_and_tests_without_reload(tmp_path):
    dest = tmp_path / 'snippet.conf'
    run = _Runner()
    rc = r.main(['--dest', str(dest), '--no-reload'], run=run, fetcher=_fetcher(), today='2026-10-07')
    assert rc == r.EXIT_OK
    text = dest.read_text(encoding='ascii')
    assert 'set_real_ip_from 172.64.0.0/13;' in text
    assert r.directives(text)[-1] == 'real_ip_header CF-Connecting-IP;'
    assert _kinds(run) == ['test']


def test_unchanged_lists_do_not_write_or_reload(tmp_path):
    dest = tmp_path / 'snippet.conf'
    r.main(['--dest', str(dest), '--no-reload'], run=_Runner(), fetcher=_fetcher(), today='2026-10-07')
    before = dest.read_bytes()
    run = _Runner()
    rc = r.main(['--dest', str(dest)], run=run, fetcher=_fetcher(), today='2026-12-01')
    assert rc == r.EXIT_OK
    assert run.calls == []
    assert dest.read_bytes() == before
    assert not (tmp_path / 'snippet.conf.prev').exists()


def test_changed_lists_write_test_then_reload(tmp_path):
    dest = tmp_path / 'snippet.conf'
    r.main(['--dest', str(dest), '--no-reload'], run=_Runner(), fetcher=_fetcher(), today='2026-10-07')
    run = _Runner()
    rc = r.main(['--dest', str(dest)], run=run, fetcher=_fetcher(v4=V4 + '\n131.0.76.0/22'))
    assert rc == r.EXIT_OK
    assert _kinds(run) == ['test', 'reload']
    assert run.calls[1] == ['systemctl', 'reload', 'nginx']
    assert 'set_real_ip_from 131.0.76.0/22;' in dest.read_text(encoding='ascii')
    assert (tmp_path / 'snippet.conf.prev').exists()


def test_failed_nginx_test_restores_previous_and_does_not_reload(tmp_path):
    dest = tmp_path / 'snippet.conf'
    r.main(['--dest', str(dest), '--no-reload'], run=_Runner(), fetcher=_fetcher(), today='2026-10-07')
    before = dest.read_bytes()
    run = _Runner(test_rc=1)
    rc = r.main(['--dest', str(dest)], run=run, fetcher=_fetcher(v4=V4 + '\n131.0.76.0/22'))
    assert rc == r.EXIT_NGINX_TEST_FAILED
    assert dest.read_bytes() == before
    assert _kinds(run) == ['test']


def test_failed_nginx_test_on_first_install_removes_the_new_file(tmp_path):
    dest = tmp_path / 'snippet.conf'
    rc = r.main(['--dest', str(dest)], run=_Runner(test_rc=1), fetcher=_fetcher())
    assert rc == r.EXIT_NGINX_TEST_FAILED
    assert not dest.exists()


def test_bad_list_writes_nothing(tmp_path):
    dest = tmp_path / 'snippet.conf'
    run = _Runner()
    rc = r.main(['--dest', str(dest)], run=run, fetcher=_fetcher(v6=''))
    assert rc == r.EXIT_REFUSED
    assert not dest.exists()
    assert run.calls == []


def test_fetch_failure_writes_nothing(tmp_path):
    def broken(url):
        raise r.RangeListError(f'could not fetch {url}: timed out')
    dest = tmp_path / 'snippet.conf'
    assert r.main(['--dest', str(dest)], run=_Runner(), fetcher=broken) == r.EXIT_REFUSED
    assert not dest.exists()


def test_large_drop_is_refused_unless_allowed(tmp_path):
    dest = tmp_path / 'snippet.conf'
    r.main(['--dest', str(dest), '--no-reload'], run=_Runner(), fetcher=_fetcher(), today='2026-10-07')
    before = dest.read_bytes()
    small_v4 = '\n'.join(V4.split('\n')[:3])          # keeps 3 + 7 of 22 networks (45%)
    rc = r.main(['--dest', str(dest)], run=_Runner(), fetcher=_fetcher(v4=small_v4))
    assert rc == r.EXIT_REFUSED
    assert dest.read_bytes() == before
    rc = r.main(['--dest', str(dest), '--allow-large-change', '--no-reload'],
                run=_Runner(), fetcher=_fetcher(v4=small_v4))
    assert rc == r.EXIT_OK
    assert 'set_real_ip_from 172.64.0.0/13;' not in dest.read_text(encoding='ascii')


def test_dry_run_writes_nothing(tmp_path, capsys):
    dest = tmp_path / 'snippet.conf'
    run = _Runner()
    assert r.main(['--dest', str(dest), '--dry-run'], run=run, fetcher=_fetcher()) == r.EXIT_OK
    assert not dest.exists()
    assert run.calls == []
    assert 'real_ip_header CF-Connecting-IP;' in capsys.readouterr().out


def test_reload_failure_is_reported(tmp_path):
    dest = tmp_path / 'snippet.conf'
    rc = r.main(['--dest', str(dest)], run=_Runner(reload_rc=1), fetcher=_fetcher())
    assert rc == r.EXIT_RELOAD_FAILED
    assert dest.exists()
    assert (tmp_path / 'snippet.conf.reload-pending').exists()


def test_missing_nginx_binary_counts_as_a_failed_test(tmp_path):
    """Under cron, PATH may lack /usr/sbin: `nginx -t` that cannot run must not leave an
    untested snippet in place."""
    dest = tmp_path / 'snippet.conf'
    r.main(['--dest', str(dest), '--no-reload'], run=_Runner(), fetcher=_fetcher(), today='2026-10-07')
    before = dest.read_bytes()

    def no_nginx(cmd, **_kw):
        raise FileNotFoundError(2, 'No such file or directory', cmd[0])
    rc = r.main(['--dest', str(dest)], run=no_nginx, fetcher=_fetcher(v4=V4 + '\n131.0.76.0/22'))
    assert rc == r.EXIT_NGINX_TEST_FAILED
    assert dest.read_bytes() == before


# ---------------------------------------------------------------------------
# fetch(): a canned server behind urllib's real opener (no network).
# ---------------------------------------------------------------------------

class _Sock:
    def __init__(self, raw):
        self._raw = raw

    def makefile(self, _mode):
        return io.BytesIO(self._raw)


class _CannedServer(urllib.request.HTTPHandler, urllib.request.HTTPSHandler):
    """Answers http(s) requests from canned raw HTTP responses, handing back a real
    http.client.HTTPResponse the way AbstractHTTPHandler.do_open does. Subclassing BOTH
    default handlers makes build_opener drop them, so no request can reach the network."""

    def __init__(self, answers, final_url=None):
        super().__init__()
        self.answers, self.final_url, self.requested = answers, final_url or {}, []

    def https_open(self, req):
        url = req.full_url
        self.requested.append(url)
        if url not in self.answers:
            raise urllib.error.URLError(f'unexpected request to {url}')
        resp = http.client.HTTPResponse(_Sock(self.answers[url]), method=req.get_method())
        resp.begin()
        resp.url = self.final_url.get(url, url)
        resp.msg = resp.reason
        return resp

    http_open = https_open


def _ok(body, *, length=None):
    n = len(body) if length is None else length
    return (b'HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nLast-Modified: ' + LM.encode()
            + b'\r\nContent-Length: ' + str(n).encode() + b'\r\n\r\n' + body)


def _redirect(location):
    return (b'HTTP/1.1 302 Found\r\nLocation: ' + location.encode()
            + b'\r\nContent-Length: 0\r\n\r\n')


def _opener(server):
    return r.build_opener(server, urllib.request.ProxyHandler({}))


def test_fetch_accepts_the_cloudflare_https_answer():
    server = _CannedServer({r.IPV4_URL: _ok(V4.encode())})
    body, last_modified = r.fetch(r.IPV4_URL, opener=_opener(server))
    assert body == V4
    assert last_modified == LM


def test_fetch_refuses_a_body_shorter_than_its_content_length():
    """The connection closes after 2 of the 7 IPv6 networks. http.client's read(n) returns
    those bytes without complaint, and on their own they parse as a valid list."""
    cut = '\n'.join(V6.split('\n')[:2]).encode()
    assert r.parse_ranges(cut.decode(), 6)                 # the fragment alone looks valid
    server = _CannedServer({r.IPV6_URL: _ok(cut, length=len(V6))})
    with pytest.raises(r.RangeListError, match='incomplete'):
        r.fetch(r.IPV6_URL, opener=_opener(server))


def test_fetch_refuses_a_broken_chunked_body():
    cut = '\n'.join(V6.split('\n')[:2]).encode()
    raw = (b'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n'
           + f'{len(cut):x}'.encode() + b'\r\n' + cut + b'\r\n')    # no terminating 0-chunk
    server = _CannedServer({r.IPV6_URL: raw})
    with pytest.raises(r.RangeListError):
        r.fetch(r.IPV6_URL, opener=_opener(server))


@pytest.mark.parametrize('location', ['http://www.cloudflare.com/ips-v4',
                                      'https://evil.example/ips-v4',
                                      'https://www.cloudflare.com.evil.example/ips-v4'])
def test_a_redirect_away_from_cloudflare_https_is_refused_before_it_is_followed(location):
    """Checking only the final URL meant the disallowed hop had already been made."""
    server = _CannedServer({r.IPV4_URL: _redirect(location), location: _ok(V4.encode())})
    with pytest.raises(r.RangeListError, match='redirected'):
        r.fetch(r.IPV4_URL, opener=_opener(server))
    assert server.requested == [r.IPV4_URL]


def test_a_redirect_within_cloudflare_https_is_followed():
    target = 'https://www.cloudflare.com/ips-v4/'
    server = _CannedServer({r.IPV4_URL: _redirect('/ips-v4/'), target: _ok(V4.encode())})
    body, _ = r.fetch(r.IPV4_URL, opener=_opener(server))
    assert body == V4
    assert server.requested == [r.IPV4_URL, target]


def test_fetch_still_checks_the_final_url():
    server = _CannedServer({r.IPV4_URL: _ok(V4.encode())},
                           final_url={r.IPV4_URL: 'http://www.cloudflare.com/ips-v4'})
    with pytest.raises(r.RangeListError, match='redirected'):
        r.fetch(r.IPV4_URL, opener=_opener(server))


# ---------------------------------------------------------------------------
# Large drops are judged per family, in --dry-run too.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('v4,v6', [
    (V4, '\n'.join(V6.split('\n')[:2])),                 # 15 + 2 of 22 kept: 77% combined
    ('\n'.join(V4.split('\n')[:4]), V6),                 # 4 + 7 of 22 kept: exactly 50% combined
])
def test_a_list_that_loses_most_of_one_family_is_refused(tmp_path, v4, v6):
    dest = tmp_path / 'snippet.conf'
    r.main(['--dest', str(dest), '--no-reload'], run=_Runner(), fetcher=_fetcher(), today='2026-10-07')
    before = dest.read_bytes()
    run = _Runner()
    rc = r.main(['--dest', str(dest)], run=run, fetcher=_fetcher(v4=v4, v6=v6))
    assert rc == r.EXIT_REFUSED
    assert dest.read_bytes() == before
    assert run.calls == []


def test_dry_run_applies_the_large_change_check(tmp_path, capsys):
    dest = tmp_path / 'snippet.conf'
    r.main(['--dest', str(dest), '--no-reload'], run=_Runner(), fetcher=_fetcher(), today='2026-10-07')
    capsys.readouterr()
    small_v6 = '\n'.join(V6.split('\n')[:2])
    rc = r.main(['--dest', str(dest), '--dry-run'], run=_Runner(), fetcher=_fetcher(v6=small_v6))
    assert rc == r.EXIT_REFUSED
    out, err = capsys.readouterr()
    assert out == ''
    assert 'IPv6' in err
    rc = r.main(['--dest', str(dest), '--dry-run', '--allow-large-change'],
                run=_Runner(), fetcher=_fetcher(v6=small_v6))
    assert rc == r.EXIT_OK


# ---------------------------------------------------------------------------
# A failed reload is retried.
# ---------------------------------------------------------------------------

def test_a_failed_reload_is_retried_by_the_next_run(tmp_path):
    dest = tmp_path / 'snippet.conf'
    pending = tmp_path / 'snippet.conf.reload-pending'
    r.main(['--dest', str(dest), '--no-reload'], run=_Runner(), fetcher=_fetcher(), today='2026-10-07')
    changed = _fetcher(v4=V4 + '\n131.0.76.0/22')
    assert r.main(['--dest', str(dest)], run=_Runner(reload_rc=1), fetcher=changed) == r.EXIT_RELOAD_FAILED
    assert pending.exists()
    written = dest.read_bytes()

    run = _Runner()                                      # same lists: nothing to write, a reload owed
    assert r.main(['--dest', str(dest)], run=run, fetcher=changed) == r.EXIT_OK
    assert _kinds(run) == ['test', 'reload']
    assert dest.read_bytes() == written
    assert not pending.exists()

    run = _Runner()                                      # once done, a quiet no-op again
    assert r.main(['--dest', str(dest)], run=run, fetcher=changed) == r.EXIT_OK
    assert run.calls == []


def test_a_failed_reload_on_first_install_is_retried(tmp_path):
    dest = tmp_path / 'snippet.conf'
    assert r.main(['--dest', str(dest)], run=_Runner(reload_rc=1), fetcher=_fetcher()) == r.EXIT_RELOAD_FAILED
    run = _Runner()
    assert r.main(['--dest', str(dest)], run=run, fetcher=_fetcher()) == r.EXIT_OK
    assert _kinds(run) == ['test', 'reload']


def test_a_retried_reload_whose_test_fails_stays_pending(tmp_path):
    dest = tmp_path / 'snippet.conf'
    pending = tmp_path / 'snippet.conf.reload-pending'
    r.main(['--dest', str(dest)], run=_Runner(reload_rc=1), fetcher=_fetcher())
    written = dest.read_bytes()
    run = _Runner(test_rc=1)
    assert r.main(['--dest', str(dest)], run=run, fetcher=_fetcher()) == r.EXIT_NGINX_TEST_FAILED
    assert _kinds(run) == ['test']
    assert dest.read_bytes() == written
    assert pending.exists()


# ---------------------------------------------------------------------------
# One run at a time.
# ---------------------------------------------------------------------------

def test_a_second_run_is_refused_while_the_first_holds_the_lock(tmp_path):
    """Run B starts while run A is inside `nginx -t`. Unlocked, B took A's new snippet for
    "the previous one" and overwrote the .prev copy with it, so the original was gone."""
    dest = tmp_path / 'snippet.conf'
    r.main(['--dest', str(dest), '--no-reload'], run=_Runner(), fetcher=_fetcher(), today='2026-10-07')
    original = dest.read_bytes()
    run_b = _Runner(test_rc=1)
    seen = {}

    def run_a(cmd, **_kw):
        if cmd[-1] == '-t':
            seen['b'] = r.main(['--dest', str(dest)], run=run_b,
                               fetcher=_fetcher(v4=V4 + '\n131.0.80.0/22'))
            return subprocess.CompletedProcess(cmd, 1, stdout='', stderr='boom')
        raise AssertionError('run A must not reload after a failed test')

    rc = r.main(['--dest', str(dest)], run=run_a, fetcher=_fetcher(v4=V4 + '\n131.0.76.0/22'))
    assert rc == r.EXIT_NGINX_TEST_FAILED
    assert seen['b'] == r.EXIT_REFUSED
    assert run_b.calls == []
    assert dest.read_bytes() == original
    assert (tmp_path / 'snippet.conf.prev').read_bytes() == original


def test_the_lock_is_exclusive_and_released_after_use(tmp_path):
    lock = str(tmp_path / 'snippet.conf.lock')
    with r.exclusive_lock(lock):
        with pytest.raises(r.LockBusyError):
            with r.exclusive_lock(lock):
                pass
    with r.exclusive_lock(lock):
        pass


class _Killed(BaseException):
    """Stands in for the process being terminated (SIGKILL, power loss)."""


@pytest.mark.parametrize('kill_at', ['-t', 'reload'])
def test_a_run_killed_before_its_reload_finishes_is_finished_by_the_next(tmp_path, kill_at):
    """Codex round 2: a run terminated after writing the new snippet but before the reload
    finished must not leave the next identical run saying "unchanged" with nginx still on
    the previous trust list."""
    dest = tmp_path / 'snippet.conf'
    pending = tmp_path / 'snippet.conf.reload-pending'
    r.main(['--dest', str(dest), '--no-reload'], run=_Runner(), fetcher=_fetcher(), today='2026-10-07')
    changed = _fetcher(v4=V4 + '\n131.0.76.0/22')

    def killed(cmd, **_kw):
        if kill_at in cmd:          # nginx -t, or systemctl reload nginx
            raise _Killed()
        return subprocess.CompletedProcess(cmd, 0, stdout='', stderr='')
    with pytest.raises(_Killed):
        r.main(['--dest', str(dest)], run=killed, fetcher=changed)
    assert pending.exists()

    run = _Runner()
    assert r.main(['--dest', str(dest)], run=run, fetcher=changed) == r.EXIT_OK
    assert _kinds(run) == ['test', 'reload']
    assert not pending.exists()


def test_a_failed_test_leaves_no_pending_marker_behind(tmp_path):
    dest = tmp_path / 'snippet.conf'
    pending = tmp_path / 'snippet.conf.reload-pending'
    r.main(['--dest', str(dest), '--no-reload'], run=_Runner(), fetcher=_fetcher(), today='2026-10-07')
    changed = _fetcher(v4=V4 + '\n131.0.76.0/22')
    assert r.main(['--dest', str(dest)], run=_Runner(test_rc=1), fetcher=changed) == r.EXIT_NGINX_TEST_FAILED
    assert not pending.exists()


def test_a_run_killed_right_after_marking_is_finished_by_the_next(tmp_path, monkeypatch):
    """Killed after the pending marker is written but before the snippet is: the next identical
    run writes the snippet, tests and reloads."""
    dest = tmp_path / 'snippet.conf'
    pending = tmp_path / 'snippet.conf.reload-pending'
    r.main(['--dest', str(dest), '--no-reload'], run=_Runner(), fetcher=_fetcher(), today='2026-10-07')
    changed = _fetcher(v4=V4 + '\n131.0.76.0/22')
    real_write = r._write_atomic

    def write(path, data):
        if path == str(dest):
            raise _Killed()
        return real_write(path, data)
    monkeypatch.setattr(r, '_write_atomic', write)
    with pytest.raises(_Killed):
        r.main(['--dest', str(dest)], run=_Runner(), fetcher=changed)
    monkeypatch.setattr(r, '_write_atomic', real_write)
    assert pending.exists()

    run = _Runner()
    assert r.main(['--dest', str(dest)], run=run, fetcher=changed) == r.EXIT_OK
    assert _kinds(run) == ['test', 'reload']
    assert '131.0.76.0/22' in dest.read_text()
    assert not pending.exists()


def test_an_earlier_pending_marker_survives_a_failed_test_of_a_new_candidate(tmp_path):
    """The marker belongs to the reload still owed from before: a later candidate whose test
    fails must not clear it."""
    dest = tmp_path / 'snippet.conf'
    pending = tmp_path / 'snippet.conf.reload-pending'
    r.main(['--dest', str(dest), '--no-reload'], run=_Runner(), fetcher=_fetcher(), today='2026-10-07')
    first = _fetcher(v4=V4 + '\n131.0.76.0/22')
    assert r.main(['--dest', str(dest)], run=_Runner(reload_rc=1), fetcher=first) == r.EXIT_RELOAD_FAILED
    assert pending.exists()
    second = _fetcher(v4=V4 + '\n131.0.76.0/22\n131.0.80.0/22')
    assert r.main(['--dest', str(dest)], run=_Runner(test_rc=1), fetcher=second) == r.EXIT_NGINX_TEST_FAILED
    assert pending.exists()
