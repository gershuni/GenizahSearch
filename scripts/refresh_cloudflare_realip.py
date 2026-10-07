#!/usr/bin/env python3
"""Regenerate the nginx snippet that makes $remote_addr the real visitor behind Cloudflare.

Why: every request reaches the origin from a Cloudflare edge, so without the realip
module nginx logs (and forwards to the app as the right-most X-Forwarded-For entry) the
EDGE address -- 2026-10-07: an overloading client showed up only as 172.70.153.x in
/var/log/nginx/access.log and had to be found in the Cloudflare dashboard. The snippet
tells nginx to trust the CF-Connecting-IP header ONLY from Cloudflare's published ranges.

The snippet is only as good as the range list, so this script is deliberately strict:

* both lists are fetched over HTTPS from www.cloudflare.com, and a redirect anywhere else
  is refused BEFORE it is followed; a body shorter than its Content-Length (or a broken
  chunked body) is refused; every line must parse as a canonical, globally routable
  network of the right family -- an empty, HTML or otherwise malformed list is refused
  and nothing is written;
* a list that keeps fewer than half of the installed networks of EITHER family is refused
  unless --allow-large-change is given (a cut-off response must not silently stop
  trusting most of Cloudflare); --dry-run applies the same check;
* one run at a time: while a run holds <dest>.lock, a second run is refused;
* the previous snippet is kept as <dest>.prev, `nginx -t` runs before any reload, and a
  failing test puts the previous snippet back (atomically);
* a failed reload leaves <dest>.reload-pending, and the next run retries `nginx -t` and
  the reload even when the ranges are unchanged;
* when the ranges are unchanged and no reload is pending, nothing is written and nginx is
  not reloaded.

What it cannot catch: a body cut short with no Content-Length and no chunked framing that
still holds enough valid networks, when no snippet is installed yet to compare with. That
is why the first install compares --dry-run with the reviewed copy in the repo.

Standard library only; runs with the system python3 on the server (as root, because it
writes under /etc/nginx and reloads nginx). Read the owner steps in
docs/guides/DEPLOYMENT_TECHNICAL.md ("Real visitor addresses behind Cloudflare") first.

    sudo python3 refresh_cloudflare_realip.py --dry-run     # fetch, validate, print; no write
    sudo python3 refresh_cloudflare_realip.py --no-reload   # first install: write + nginx -t
    sudo python3 refresh_cloudflare_realip.py               # write, nginx -t, reload

Exit codes: 0 written and reloaded / unchanged / dry run; 1 fetch or validation refused,
or another run holds the lock (nothing written); 2 `nginx -t` failed (nothing reloaded;
a new snippet was put back to the previous one); 3 reload failed (the tested snippet is in
place and the next run retries the reload).
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as _dt
import ipaddress
import os
import shutil
import subprocess
import sys
import tempfile
import urllib.request
from typing import Callable, Iterator, Optional, Sequence

IPV4_URL = 'https://www.cloudflare.com/ips-v4'
IPV6_URL = 'https://www.cloudflare.com/ips-v6'
ALLOWED_URL_PREFIX = 'https://www.cloudflare.com/'
DEFAULT_DEST = '/etc/nginx/snippets/genizah_cloudflare_realip.conf'
REAL_IP_HEADER = 'CF-Connecting-IP'
MAX_BODY = 65536

# Bounds that a genuine Cloudflare list sits well inside (2026-10-07: 15 IPv4, 7 IPv6).
MIN_COUNT = {4: 3, 6: 2}
MAX_COUNT = {4: 64, 6: 64}
# The broadest prefix we will ever trust. Cloudflare's broadest are /13 (v4) and /29 (v6);
# a /0 here would let ANY peer choose its own address through CF-Connecting-IP.
MIN_PREFIX = {4: 8, 6: 16}
# Refuse a list that keeps fewer than this share of the networks of one family already installed.
MIN_RETAINED_SHARE = 0.5

EXIT_OK = 0
EXIT_REFUSED = 1
EXIT_NGINX_TEST_FAILED = 2
EXIT_RELOAD_FAILED = 3


class RangeListError(ValueError):
    """A fetched range list is not safe to install."""


class LockBusyError(RuntimeError):
    """Another run holds the lock."""


def parse_ranges(text: str, version: int) -> list[str]:
    """Return the networks in `text` (one per line), or raise RangeListError.

    Every non-blank line must be a canonical network of the given IP version
    (exactly how ipaddress prints it, host bits clear), globally routable, no broader
    than MIN_PREFIX, and not repeated. The count must lie within MIN_COUNT..MAX_COUNT.
    """
    if version not in (4, 6):
        raise ValueError(f'version must be 4 or 6, got {version!r}')
    if text is None or not text.strip():
        raise RangeListError(f'IPv{version} list is empty')
    networks: list[str] = []
    seen: set = set()
    for lineno, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue
        try:
            net = ipaddress.ip_network(line, strict=True)
        except ValueError as exc:
            raise RangeListError(f'IPv{version} line {lineno} is not a network: {line[:80]!r} ({exc})') from None
        if net.version != version:
            raise RangeListError(f'IPv{version} line {lineno} is IPv{net.version}: {line!r}')
        if str(net) != line:
            raise RangeListError(f'IPv{version} line {lineno} is not in canonical form: {line!r} (expected {net})')
        if net.prefixlen < MIN_PREFIX[version]:
            raise RangeListError(
                f'IPv{version} line {lineno} is broader than /{MIN_PREFIX[version]}: {line!r}')
        if not net.is_global:
            raise RangeListError(f'IPv{version} line {lineno} is not a public range: {line!r}')
        if net in seen:
            raise RangeListError(f'IPv{version} line {lineno} repeats {line!r}')
        seen.add(net)
        networks.append(line)
    if not MIN_COUNT[version] <= len(networks) <= MAX_COUNT[version]:
        raise RangeListError(
            f'IPv{version} list has {len(networks)} networks; expected '
            f'{MIN_COUNT[version]}..{MAX_COUNT[version]}')
    return networks


def render(v4: Sequence[str], v6: Sequence[str], *, fetched_on: str,
           v4_last_modified: str = '', v6_last_modified: str = '') -> str:
    """Render the snippet text (LF line endings) for the given, already validated, lists."""
    lines = [
        '# Cloudflare edge ranges -> real visitor address (nginx realip module).',
        '#',
        '# GENERATED by scripts/refresh_cloudflare_realip.py -- do not edit by hand; rerun it.',
        f'# Source: {IPV4_URL} and {IPV6_URL}',
        f'# Fetched: {fetched_on}',
    ]
    if v4_last_modified:
        lines.append(f'# ips-v4 Last-Modified: {v4_last_modified}')
    if v6_last_modified:
        lines.append(f'# ips-v6 Last-Modified: {v6_last_modified}')
    lines += [
        '#',
        '# Included inside each server { } block of /etc/nginx/sites-available/genizah.',
        '# For a connection FROM one of these ranges, nginx replaces $remote_addr (and so the',
        '# access log, X-Real-IP and the entry $proxy_add_x_forwarded_for appends) with the',
        f'# {REAL_IP_HEADER} header. From any other peer the header is ignored, so a request',
        '# that reaches the origin directly cannot choose the address it is logged under.',
        '',
        '# IPv4',
    ]
    lines += [f'set_real_ip_from {n};' for n in v4]
    lines += ['', '# IPv6']
    lines += [f'set_real_ip_from {n};' for n in v6]
    lines += ['', f'real_ip_header {REAL_IP_HEADER};', '']
    return '\n'.join(lines)


def directives(text: str) -> list[str]:
    """The non-comment, non-blank lines of a snippet: what nginx actually reads."""
    out = []
    for raw in text.splitlines():
        line = raw.strip()
        if line and not line.startswith('#'):
            out.append(line)
    return out


def installed_networks(text: str) -> set:
    nets = set()
    for line in directives(text):
        if line.startswith('set_real_ip_from ') and line.endswith(';'):
            nets.add(line[len('set_real_ip_from '):-1].strip())
    return nets


def large_change(old_text: str, v4: Sequence[str], v6: Sequence[str]) -> Optional[str]:
    """Why the new lists drop too much of what is installed, or None.

    Judged per family: a cut-off IPv6 list must not hide behind a complete IPv4 list.
    """
    old = installed_networks(old_text)
    new = {4: set(v4), 6: set(v6)}
    for version in (4, 6):
        old_family = {n for n in old if (6 if ':' in n else 4) == version}
        if not old_family:
            continue
        kept = len(old_family & new[version]) / len(old_family)
        if kept < MIN_RETAINED_SHARE:
            return (f'the new IPv{version} list keeps only {kept:.0%} of the '
                    f'{len(old_family)} installed IPv{version} networks')
    return None


def _allowed_url(url: str) -> bool:
    return url.startswith(ALLOWED_URL_PREFIX)


class _CloudflareOnlyRedirects(urllib.request.HTTPRedirectHandler):
    """Refuse a redirect to anything but https://www.cloudflare.com/ BEFORE following it."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if not _allowed_url(newurl):
            raise RangeListError(f'{req.full_url} redirected to {newurl}; refusing')
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def build_opener(*extra_handlers) -> urllib.request.OpenerDirector:
    """urllib's opener with redirects restricted to https://www.cloudflare.com/."""
    return urllib.request.build_opener(_CloudflareOnlyRedirects, *extra_handlers)


def fetch(url: str, timeout: float = 30.0, *,
          opener: Optional[urllib.request.OpenerDirector] = None) -> tuple[str, str]:
    """GET `url`; return (body, Last-Modified).

    Raises RangeListError unless the answer is a complete HTTP 200 from
    https://www.cloudflare.com/ of at most MAX_BODY ASCII bytes. http.client's read(n)
    returns a short body without complaint when the connection closes early, so the
    length is checked against Content-Length here (a broken chunked body already raises).
    """
    req = urllib.request.Request(url, headers={'User-Agent': 'genizahsearch-realip-refresh/1'})
    opener = opener or build_opener()
    try:
        with opener.open(req, timeout=timeout) as resp:
            final = resp.geturl() if hasattr(resp, 'geturl') else url
            if not _allowed_url(final):
                raise RangeListError(f'{url} redirected to {final}; refusing')
            status = getattr(resp, 'status', 200)
            if status != 200:
                raise RangeListError(f'{url} answered HTTP {status}')
            body = resp.read(MAX_BODY + 1)
            if len(body) > MAX_BODY:
                raise RangeListError(f'{url} answered more than {MAX_BODY // 1024} KiB; refusing')
            declared = resp.headers.get('Content-Length')
            if declared is not None:
                try:
                    complete = int(declared.strip()) == len(body)
                except ValueError:
                    complete = False
                if not complete:
                    raise RangeListError(f'{url} sent {len(body)} bytes but declared '
                                         f'Content-Length {declared.strip()!r}; refusing an incomplete answer')
            return body.decode('ascii'), resp.headers.get('Last-Modified', '') or ''
    except RangeListError:
        raise
    except UnicodeDecodeError:
        raise RangeListError(f'{url} answered non-ASCII content') from None
    except Exception as exc:  # URLError, timeout, TLS failure, IncompleteRead ...
        raise RangeListError(f'could not fetch {url}: {exc}') from None


@contextlib.contextmanager
def exclusive_lock(path: str) -> Iterator[None]:
    """Hold an exclusive lock on `path` for the whole run, or raise LockBusyError at once.

    fcntl.flock on the server (Linux); msvcrt's byte-range lock where fcntl does not exist
    (Windows, where the tests also run). Either is released when the descriptor closes, so
    a killed run never leaves a stale lock behind. The lock file itself is left in place.
    """
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        try:
            import fcntl
        except ImportError:
            fcntl = None
        if fcntl is not None:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise LockBusyError(path) from None
            yield
        else:
            import msvcrt
            try:
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            except OSError:
                raise LockBusyError(path) from None
            try:
                yield
            finally:
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
    finally:
        os.close(fd)


def _write_atomic(path: str, data) -> None:
    """Replace `path` with `data` (str is written as ASCII) in one rename."""
    if isinstance(data, str):
        data = data.encode('ascii')
    directory = os.path.dirname(os.path.abspath(path)) or '.'
    fd, tmp = tempfile.mkstemp(prefix='.cf_realip.', dir=directory)
    try:
        with os.fdopen(fd, 'wb') as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.chmod(tmp, 0o644)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _read_bytes(path: str) -> Optional[bytes]:
    try:
        with open(path, 'rb') as fh:
            return fh.read()
    except FileNotFoundError:
        return None


def _pending_path(dest: str) -> str:
    return dest + '.reload-pending'


Runner = Callable[..., subprocess.CompletedProcess]
Fetcher = Callable[[str], tuple]


def _fetch_and_render(fetcher: Fetcher, today: Optional[str]):
    """Fetch and validate both lists; return (v4, v6, snippet text). Raises RangeListError."""
    v4_text, v4_lm = fetcher(IPV4_URL)
    v6_text, v6_lm = fetcher(IPV6_URL)
    v4 = parse_ranges(v4_text, 4)
    v6 = parse_ranges(v6_text, 6)
    fetched_on = today or _dt.datetime.now(_dt.timezone.utc).strftime('%Y-%m-%d')
    return v4, v6, render(v4, v6, fetched_on=fetched_on,
                          v4_last_modified=v4_lm, v6_last_modified=v6_lm)


def _refuse_large_change(old_text: Optional[str], v4, v6, allow: bool) -> bool:
    """Print the refusal and return True if the change is too large to install unasked."""
    if old_text is None or allow:
        return False
    why = large_change(old_text, v4, v6)
    if why is None:
        return False
    print(f'REFUSED: {why}. Check https://www.cloudflare.com/ips/ and rerun with '
          '--allow-large-change if this is real. Nothing was written.', file=sys.stderr)
    return True


def _nginx_test(run: Runner, nginx_bin: str) -> subprocess.CompletedProcess:
    try:
        return run([nginx_bin, '-t'], capture_output=True, text=True)
    except OSError as exc:   # nginx not found / not executable: an untested snippet must not stay
        return subprocess.CompletedProcess([nginx_bin, '-t'], 127, stdout='', stderr=str(exc))


def _reload(run: Runner, dest: str) -> int:
    """Reload nginx; on failure record that the next run must retry it."""
    cmd = ['systemctl', 'reload', 'nginx']
    try:
        result = run(cmd, capture_output=True, text=True)
    except OSError as exc:
        result = subprocess.CompletedProcess(cmd, 127, stdout='', stderr=str(exc))
    pending = _pending_path(dest)
    if result.returncode != 0:
        _write_atomic(pending, 'nginx -t passed but `systemctl reload nginx` failed; '
                               'the next run retries the reload.\n')
        print(f'nginx -t passed but the reload FAILED; the tested snippet is in place and '
              f'{pending} makes the next run retry the reload.\n{result.stderr or result.stdout}',
              file=sys.stderr)
        return EXIT_RELOAD_FAILED
    try:
        os.unlink(pending)
    except FileNotFoundError:
        pass
    print('nginx -t passed; nginx reloaded')
    return EXIT_OK


def _dry_run(args, fetcher: Fetcher, today: Optional[str]) -> int:
    try:
        v4, v6, new_text = _fetch_and_render(fetcher, today)
    except RangeListError as exc:
        print(f'REFUSED: {exc}. Nothing was written.', file=sys.stderr)
        return EXIT_REFUSED
    old = _read_bytes(args.dest)
    old_text = None if old is None else old.decode('ascii', errors='replace')
    if _refuse_large_change(old_text, v4, v6, args.allow_large_change):
        return EXIT_REFUSED
    sys.stdout.write(new_text)
    if old_text is not None:
        same = directives(old_text) == directives(new_text)
        print(f'# dry run: installed snippet at {args.dest} is '
              f'{"UNCHANGED" if same else "DIFFERENT"}', file=sys.stderr)
    return EXIT_OK


def _install(args, run: Runner, fetcher: Fetcher, today: Optional[str]) -> int:
    """Everything that reads or writes the snippet; the caller holds the lock."""
    try:
        v4, v6, new_text = _fetch_and_render(fetcher, today)
    except RangeListError as exc:
        print(f'REFUSED: {exc}. Nothing was written.', file=sys.stderr)
        return EXIT_REFUSED

    old = _read_bytes(args.dest)
    old_text = None if old is None else old.decode('ascii', errors='replace')
    pending = os.path.exists(_pending_path(args.dest))

    if old_text is not None and directives(old_text) == directives(new_text):
        if not pending:
            print(f'unchanged: {len(v4)} IPv4 + {len(v6)} IPv6 ranges already in {args.dest}; no reload')
            return EXIT_OK
        if args.no_reload:
            print(f'unchanged; an earlier reload is still pending ({_pending_path(args.dest)}); '
                  'not reloading (--no-reload)')
            return EXIT_OK
        print(f'unchanged, but an earlier reload is pending ({_pending_path(args.dest)}); '
              'retrying nginx -t and the reload')
        test = _nginx_test(run, args.nginx_bin)
        if test.returncode != 0:
            print(f'nginx -t FAILED; nothing was written; nginx NOT reloaded; the reload stays pending.\n'
                  f'{test.stderr or test.stdout}', file=sys.stderr)
            return EXIT_NGINX_TEST_FAILED
        return _reload(run, args.dest)

    if _refuse_large_change(old_text, v4, v6, args.allow_large_change):
        return EXIT_REFUSED

    backup = args.dest + '.prev'
    if old is not None:
        _write_atomic(backup, old)
    # Mark the reload owed BEFORE the snippet changes: if this run is killed between the write
    # and a finished reload, the next run sees "unchanged but pending" and reloads, instead of
    # "unchanged" while nginx still runs the previous trust list. Only a finished reload clears
    # it; a failed test puts the marker back as it was.
    marked = not args.no_reload and not pending
    if marked:
        _write_atomic(_pending_path(args.dest), 'a new snippet was written; the next run reloads nginx '
                                                'unless this run finished the reload.\n')
    _write_atomic(args.dest, new_text)
    print(f'wrote {args.dest}: {len(v4)} IPv4 + {len(v6)} IPv6 ranges'
          + (f' (previous kept at {backup})' if old is not None else ''))

    test = _nginx_test(run, args.nginx_bin)
    if test.returncode != 0:
        if old is not None:
            _write_atomic(args.dest, old)
            restored = 'put the previous snippet back'
        else:
            os.unlink(args.dest)
            restored = f'removed the new {args.dest}'
        if marked:
            os.unlink(_pending_path(args.dest))   # nginx still runs what is on disk again
        print(f'nginx -t FAILED; {restored}; nginx NOT reloaded.\n{test.stderr or test.stdout}',
              file=sys.stderr)
        return EXIT_NGINX_TEST_FAILED

    if args.no_reload:
        print('nginx -t passed; not reloading (--no-reload)')
        return EXIT_OK
    return _reload(run, args.dest)


def main(argv: Optional[Sequence[str]] = None, *, run: Runner = subprocess.run,
         fetcher: Fetcher = fetch, today: Optional[str] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--dest', default=DEFAULT_DEST, help=f'snippet path (default {DEFAULT_DEST})')
    ap.add_argument('--dry-run', action='store_true',
                    help='fetch, validate (including the large-change check) and print; write nothing')
    ap.add_argument('--no-reload', action='store_true',
                    help='write and run nginx -t, but do not reload (reloading is then yours)')
    ap.add_argument('--allow-large-change', action='store_true',
                    help='install even if a new list drops more than half of the installed '
                         'networks of its family')
    ap.add_argument('--nginx-bin', default=shutil.which('nginx') or '/usr/sbin/nginx',
                    help='nginx binary used for `nginx -t` (cron PATH often lacks /usr/sbin)')
    args = ap.parse_args(argv)

    if args.dry_run:
        return _dry_run(args, fetcher, today)
    try:
        with exclusive_lock(args.dest + '.lock'):
            return _install(args, run, fetcher, today)
    except LockBusyError:
        print(f'REFUSED: another run holds {args.dest}.lock. Nothing was written.', file=sys.stderr)
        return EXIT_REFUSED


if __name__ == '__main__':
    sys.exit(main())
