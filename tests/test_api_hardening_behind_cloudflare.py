"""What address the app keys on behind Cloudflare -> nginx -> uvicorn (2026-10-07).

Production chain: visitor -> Cloudflare edge -> nginx (127.0.0.1:8081 upstream) -> uvicorn
(NiceGUI's `ui.run`) -> the app. Two things decide the "client IP" the app sees:

1. nginx sends `X-Forwarded-For: $proxy_add_x_forwarded_for`, i.e. whatever arrived plus
   `$remote_addr` APPENDED. Without the realip snippet (scripts/genizah_cloudflare_realip.conf)
   `$remote_addr` is the Cloudflare edge; with it, it is the CF-Connecting-IP value -- but only
   for peers in Cloudflare's ranges.
2. uvicorn's ProxyHeadersMiddleware (on by default; trusts only FORWARDED_ALLOW_IPS, default
   127.0.0.1) rewrites `request.client` to the right-most X-Forwarded-For entry that is not
   127.0.0.1 -- the one nginx appended. `web/api_hardening._resolve_rate_limit_key` then sees a
   non-loopback `request.client.host` and returns it; the puzzle limiter in web/api.py reads
   `request.client.host` directly.

These tests run the REAL uvicorn middleware as NiceGUI configures it and feed it the exact
header values nginx produces in each case. The header values are written out by hand from
nginx's documented behaviour (they are not a model of nginx). They pin:

* before the snippet the key is the Cloudflare edge (sweep item L8; why the source of the
  2026-10-07 overload was invisible on the server);
* after it the key is the visitor, and a visitor cannot choose it by sending its own
  X-Forwarded-For or CF-Connecting-IP, through Cloudflare or straight to the origin;
* the app's correctness rests on uvicorn's 127.0.0.1-only default and on no module in web/
  reading CF-Connecting-IP / X-Real-IP itself -- both are guarded here.
"""

from __future__ import annotations

import ast
import asyncio
import re
from pathlib import Path

from starlette.requests import Request

from web.api_hardening import _is_loopback_request, _resolve_rate_limit_key

REPO = Path(__file__).resolve().parents[1]

VISITOR = '198.51.100.7'        # the real visitor
VISITOR6 = '2001:db8:1234::7'   # an IPv6 visitor
EDGE = '172.70.153.10'          # a Cloudflare edge (172.64.0.0/13), as seen 2026-10-07
FORGED = '203.0.113.66'         # an address someone wants to be keyed as
ATTACKER = '192.0.2.50'         # a client connecting to the origin without Cloudflare


def _server_config(monkeypatch):
    """The uvicorn config exactly as `ui.run` builds it (no proxy kwargs in web/main.py)."""
    from nicegui.server import CustomServerConfig
    monkeypatch.delenv('FORWARDED_ALLOW_IPS', raising=False)
    seen: dict = {}

    async def app(scope, receive, send):
        request = Request(scope)
        seen['rate_limit_key'] = _resolve_rate_limit_key(request)          # API limiters
        seen['client_host'] = request.client.host if request.client else None  # puzzle limiter
        seen['is_loopback'] = _is_loopback_request(request)
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    config = CustomServerConfig(app, log_config=None)
    return config, seen


def _through_nginx(monkeypatch, headers: dict, *, env_forwarded_allow_ips=None) -> dict:
    config, seen = _server_config(monkeypatch)
    if env_forwarded_allow_ips is not None:
        # Re-read the env the way uvicorn does at Config() time.
        monkeypatch.setenv('FORWARDED_ALLOW_IPS', env_forwarded_allow_ips)
        from nicegui.server import CustomServerConfig
        config = CustomServerConfig(config.app, log_config=None)
    config.load()
    scope = {
        'type': 'http', 'asgi': {'version': '3.0'}, 'http_version': '1.1',
        'method': 'GET', 'scheme': 'http', 'path': '/api/search', 'raw_path': b'/api/search',
        'query_string': b'', 'root_path': '',
        'server': ('127.0.0.1', 8081),
        'client': ('127.0.0.1', 51234),   # nginx always connects from loopback
        'headers': [(k.lower().encode('latin1'), v.encode('latin1')) for k, v in headers.items()],
    }

    async def receive():
        return {'type': 'http.request', 'body': b'', 'more_body': False}

    async def send(_message):
        return None

    asyncio.run(config.loaded_app(scope, receive, send))
    return seen


def test_uvicorn_proxy_headers_trust_only_loopback_by_default(monkeypatch):
    """The whole chain rests on this: uvicorn rewrites request.client from X-Forwarded-For,
    trusting only 127.0.0.1. If a uvicorn/NiceGUI upgrade changes the default, re-check."""
    config, _ = _server_config(monkeypatch)
    assert config.proxy_headers is True
    assert config.forwarded_allow_ips == '127.0.0.1'


def test_before_realip_snippet_the_key_is_the_cloudflare_edge(monkeypatch):
    """Today's production: Cloudflare sends XFF=visitor, nginx appends $remote_addr=edge."""
    seen = _through_nginx(monkeypatch, {
        'x-forwarded-for': f'{VISITOR}, {EDGE}',
        'x-real-ip': EDGE,
        'cf-connecting-ip': VISITOR,
    })
    assert seen['rate_limit_key'] == EDGE
    assert seen['client_host'] == EDGE


def test_after_realip_snippet_the_key_is_the_visitor(monkeypatch):
    """With the snippet, $remote_addr = CF-Connecting-IP, so nginx appends the visitor."""
    seen = _through_nginx(monkeypatch, {
        'x-forwarded-for': f'{VISITOR}, {VISITOR}',
        'x-real-ip': VISITOR,
        'cf-connecting-ip': VISITOR,
    })
    assert seen['rate_limit_key'] == VISITOR
    assert seen['client_host'] == VISITOR
    assert seen['is_loopback'] is False


def test_after_realip_snippet_ipv6_visitor(monkeypatch):
    seen = _through_nginx(monkeypatch, {
        'x-forwarded-for': f'{VISITOR6}, {VISITOR6}',
        'x-real-ip': VISITOR6,
        'cf-connecting-ip': VISITOR6,
    })
    assert seen['rate_limit_key'] == VISITOR6
    assert seen['client_host'] == VISITOR6


def test_visitor_forging_headers_through_cloudflare_is_still_keyed_as_itself(monkeypatch):
    """A visitor sends XFF and CF-Connecting-IP of its own. Cloudflare APPENDS the visitor to
    XFF and sets CF-Connecting-IP to the connecting client; nginx appends $remote_addr."""
    seen = _through_nginx(monkeypatch, {
        'x-forwarded-for': f'{FORGED}, 127.0.0.1, {VISITOR}, {VISITOR}',
        'x-real-ip': VISITOR,
        'cf-connecting-ip': VISITOR,
    })
    assert seen['rate_limit_key'] == VISITOR
    assert seen['client_host'] == VISITOR
    assert seen['is_loopback'] is False


def test_direct_to_origin_cannot_choose_or_frame_an_address(monkeypatch):
    """A client reaching nginx without Cloudflare is not in set_real_ip_from, so its
    CF-Connecting-IP is ignored by nginx ($remote_addr stays the attacker) -- but the header
    still reaches the app untouched. The app must therefore never read it."""
    seen = _through_nginx(monkeypatch, {
        'x-forwarded-for': f'{VISITOR}, 127.0.0.1, {ATTACKER}',
        'x-real-ip': ATTACKER,
        'cf-connecting-ip': VISITOR,   # forged, to frame the visitor
    })
    assert seen['rate_limit_key'] == ATTACKER
    assert seen['client_host'] == ATTACKER
    assert seen['is_loopback'] is False


def test_trusting_every_proxy_would_let_a_visitor_choose_its_key(monkeypatch):
    """Why FORWARDED_ALLOW_IPS must stay unset in production: with '*' uvicorn takes the
    LEFT-most entry, which the visitor wrote. This is the failure the default prevents."""
    seen = _through_nginx(monkeypatch, {
        'x-forwarded-for': f'{FORGED}, {VISITOR}, {VISITOR}',
        'cf-connecting-ip': VISITOR,
    }, env_forwarded_allow_ips='*')
    assert seen['rate_limit_key'] == FORGED


def test_an_empty_forwarded_allow_ips_is_not_the_default(monkeypatch):
    """`FORWARDED_ALLOW_IPS=` (empty -- e.g. copied into .env, which web/main.py loads) is not
    "unset": uvicorn then trusts no proxy, so request.client stays nginx's 127.0.0.1 and the
    puzzle limiter keys every visitor on that one address."""
    seen = _through_nginx(monkeypatch, {
        'x-forwarded-for': f'{VISITOR}, {VISITOR}',
        'cf-connecting-ip': VISITOR,
    }, env_forwarded_allow_ips='')
    assert seen['client_host'] == '127.0.0.1'


def test_an_extra_trusted_proxy_lets_that_address_choose_its_key(monkeypatch):
    """Why API_TRUSTED_PROXIES must stay empty in this deployment: once uvicorn has resolved
    the visitor V, a V listed there is skipped again, and the walk reaches the entry V wrote."""
    import web.api_hardening as hardening
    monkeypatch.setattr(hardening, '_TRUSTED_PROXIES', hardening.LOOPBACK_IPS | {VISITOR})
    seen = _through_nginx(monkeypatch, {
        'x-forwarded-for': f'{FORGED}, {VISITOR}, {VISITOR}',
        'cf-connecting-ip': VISITOR,
    })
    assert seen['client_host'] == VISITOR
    assert seen['rate_limit_key'] == FORGED


# ---------------------------------------------------------------------------
# Source guards.
# ---------------------------------------------------------------------------

_PROXY_KWARGS = {'proxy_headers', 'forwarded_allow_ips'}


def _ui_run_proxy_kwargs(source: str) -> set:
    found = set()
    for node in ast.walk(ast.parse(source)):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == 'run' and isinstance(node.func.value, ast.Name)
                and node.func.value.id == 'ui'):
            found |= {kw.arg for kw in node.keywords if kw.arg in _PROXY_KWARGS}
            found |= {'**kwargs' for kw in node.keywords if kw.arg is None}
    return found


def test_ui_run_keeps_uvicorns_proxy_defaults():
    source = (REPO / 'web' / 'main.py').read_text(encoding='utf-8')
    assert 'ui.run(' in source
    assert _ui_run_proxy_kwargs(source) == set()


def test_ui_run_guard_can_fail():
    assert _ui_run_proxy_kwargs("ui.run(port=1, forwarded_allow_ips='*')") == {'forwarded_allow_ips'}
    assert _ui_run_proxy_kwargs('ui.run(**opts)') == {'**kwargs'}


def _names_forwarded_allow_ips(source: str) -> bool:
    """True if the code uses the env var's name as a string (not merely in prose)."""
    return any(isinstance(node, ast.Constant) and node.value == 'FORWARDED_ALLOW_IPS'
               for node in ast.walk(ast.parse(source)))


def test_no_web_module_sets_forwarded_allow_ips():
    """The tests above clear the variable themselves; this stops web/ from setting it before
    ui.run reads it. The server's own environment is checked by hand (DEPLOYMENT_TECHNICAL.md,
    "Real visitor addresses behind Cloudflare", step 0)."""
    offenders = [str(path.relative_to(REPO)) for path in sorted((REPO / 'web').rglob('*.py'))
                 if _names_forwarded_allow_ips(path.read_text(encoding='utf-8', errors='replace'))]
    assert offenders == []


def test_forwarded_allow_ips_guard_can_fail():
    assert _names_forwarded_allow_ips("import os\nos.environ['FORWARDED_ALLOW_IPS'] = '*'")
    assert _names_forwarded_allow_ips("import os\nos.environ.setdefault('FORWARDED_ALLOW_IPS', '*')")
    assert not _names_forwarded_allow_ips('"""uvicorn trusts FORWARDED_ALLOW_IPS=127.0.0.1."""')


# A header any client can send straight to the origin; only nginx's appended XFF entry,
# as resolved by uvicorn + web/api_hardening.py, is trustworthy.
_CLIENT_IP_HEADER_RE = re.compile(
    r"""['"](?:cf-connecting-ip|x-real-ip|true-client-ip|x-forwarded-for)['"]""", re.IGNORECASE)
_ALLOWED_READERS = {'api_hardening.py'}


def _offending_lines(text: str) -> list[str]:
    return [line.strip() for line in text.splitlines() if _CLIENT_IP_HEADER_RE.search(line)]


def test_no_web_module_reads_client_ip_headers_itself():
    offenders = []
    for path in sorted((REPO / 'web').rglob('*.py')):
        if path.name in _ALLOWED_READERS:
            continue
        for line in _offending_lines(path.read_text(encoding='utf-8', errors='replace')):
            offenders.append(f'{path.relative_to(REPO)}: {line}')
    assert offenders == [], (
        'Read the client address through web.api_hardening._resolve_rate_limit_key '
        '(or request.client.host), never from a client-settable header:\n' + '\n'.join(offenders))


def test_client_ip_header_guard_can_fail():
    assert _offending_lines("ip = request.headers.get('CF-Connecting-IP')")
    assert _offending_lines('ip = request.headers["x-real-ip"]')
    assert not _offending_lines('ip = request.client.host')


def test_committed_snippet_is_what_the_refresh_script_renders():
    """scripts/genizah_cloudflare_realip.conf must be byte-for-byte the refresh script's own
    rendering of the networks it lists (so the reviewed file and the tool cannot drift),
    and must cover the edge seen on 2026-10-07."""
    import ipaddress
    from scripts import refresh_cloudflare_realip as r

    text = (REPO / 'scripts' / 'genizah_cloudflare_realip.conf').read_text(encoding='ascii')
    text = text.replace('\r\n', '\n')
    nets = [line[len('set_real_ip_from '):-1] for line in r.directives(text)
            if line.startswith('set_real_ip_from ')]
    v4 = r.parse_ranges('\n'.join(n for n in nets if ':' not in n), 4)
    v6 = r.parse_ranges('\n'.join(n for n in nets if ':' in n), 6)
    meta = dict(re.findall(r'^# (Fetched|ips-v4 Last-Modified|ips-v6 Last-Modified): (.+)$',
                           text, re.MULTILINE))
    assert r.render(v4, v6, fetched_on=meta['Fetched'],
                    v4_last_modified=meta.get('ips-v4 Last-Modified', ''),
                    v6_last_modified=meta.get('ips-v6 Last-Modified', '')) == text
    assert r.directives(text)[-1] == 'real_ip_header CF-Connecting-IP;'
    assert any(ipaddress.ip_address(EDGE) in ipaddress.ip_network(n) for n in v4)
