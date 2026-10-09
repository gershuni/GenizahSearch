"""Public MCP server at /mcp: a thin adapter over the /api/chatgpt facade.

docs/plans/MCP_SERVER_PLAN.md. Tools call the facade's own routes in-process
(httpx.ASGITransport into the /api sub-app) with the caller's resolved IP as the
ASGI client, so the facade's mode gate, rate limits, validation and job admission
see the real visitor. No corpus logic lives here.

Transport (D3): stateless Streamable HTTP with JSON responses, subscriptions off,
served from an exact POST-only route -- GET (a legacy standalone SSE stream) and
DELETE (no sessions to end) answer 405 from the route itself. The SDK's session
manager runs inside the host app's lifespan; a mounted sub-app's lifespan would not.
``/mcp/`` has its own route that checks Host/Origin before redirecting, because
Starlette's automatic slash redirect would otherwise answer first, to any host.
"""
from __future__ import annotations

import asyncio
import json
import re
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Literal
from urllib.parse import quote, urlencode

import httpx
from pydantic import BaseModel, ConfigDict, Field
from mcp.server.mcpserver import Context, MCPServer
from mcp.server.streamable_http_manager import StreamableHTTPASGIApp
from mcp.server.transport_security import TransportSecurityMiddleware, TransportSecuritySettings
from mcp_types import CallToolResult, TextContent, ToolAnnotations
from mcp_types.version import HANDSHAKE_PROTOCOL_VERSIONS
from starlette.requests import Request
from starlette.responses import RedirectResponse

from shared.export_utils import GENIZAHSEARCH_URL
from version import APP_VERSION
from web.api_hardening import API_CHANNEL_SCOPE_KEY, _resolve_rate_limit_key, capture_mcp_tool_event
from web.chatgpt_pagination import MCP_RESPONSE_BYTES
from web.feature_flags import MCP_ENABLED

MCP_PATH = '/mcp'
FACADE_BASE_URL = 'http://facade.internal'
SITE_URL = GENIZAHSEARCH_URL

# Host-header allow-list (DNS-rebinding protection). The SDK's default accepts only
# localhost, which would answer 421 to every request through Cloudflare. The SDK
# matches each entry exactly, or as a prefix when it ends in ':*'; it does not
# normalize authorities, so bare and default-port spellings are listed explicitly.
ALLOWED_HOSTS = [
    'genizahsearch.com', 'genizahsearch.com:443',
    'www.genizahsearch.com', 'www.genizahsearch.com:443',
    'localhost', 'localhost:*', '127.0.0.1', '127.0.0.1:*', '[::1]', '[::1]:*',
]
# Server-side clients (ChatGPT, Claude) send no Origin; a browser-originated call must
# come from the site itself or a local page.
ALLOWED_ORIGINS = [
    'https://genizahsearch.com', 'https://www.genizahsearch.com',
    'http://localhost', 'http://localhost:*', 'http://127.0.0.1', 'http://127.0.0.1:*',
    'http://[::1]', 'http://[::1]:*',
]
SECURITY_SETTINGS = TransportSecuritySettings(
    enable_dns_rebinding_protection=True,
    allowed_hosts=ALLOWED_HOSTS,
    allowed_origins=ALLOWED_ORIGINS,
)

# The research workflow ported from the GPT's instructions. ChatGPT weighs the first
# 512 characters most, so the load-bearing rules come first there.
INSTRUCTIONS = (Path(__file__).resolve().parents[1] / 'integrations' / 'mcp'
                / 'server_instructions.md').read_text(encoding='utf-8').strip()

# Annotations follow OpenAI's plugin guidelines: readOnlyHint=false for "starting
# stateful jobs or workflows, queuing work" (MCP_SERVER_PLAN.md section 4).
READ_ONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False,
                            idempotentHint=True, openWorldHint=False)
STARTS_A_JOB = ToolAnnotations(readOnlyHint=False, destructiveHint=False,
                               idempotentHint=False, openWorldHint=False)
BUILDS_A_SNAPSHOT = ToolAnnotations(readOnlyHint=False, destructiveHint=False,
                                    idempotentHint=True, openWorldHint=False)

# From the implementation (web/search_api.py, docs/SEARCH_API.md), not the API schema,
# whose "variants (morphological)" is misleading. The spike showed a model guessing these.
SEARCH_MODES = (
    'Search modes: '
    'exact = the words as typed, with no spelling variation. '
    'variants = also the common letter-confusion spellings (the 30 most frequent letter-confusion '
    "pairs, the website's default variant settings). "
    'fuzzy = near spellings: every whole word within one edit of a query word (two edits from five '
    'letters; an edit adds, drops or changes a letter, or swaps two neighbours), prefixed forms '
    'included; slow, for damaged or uncertain readings. '
    'responsa = the Responsa-Project query syntax: word* and *word wildcards, #word for Hebrew '
    'prefixes and word# for suffixes, %word for plene/defective spelling, (a/b) alternatives, -word '
    'to exclude, word1 [3] word2 for up to three words between; responsa_options adds letter '
    'variants, Judeo-Arabic transliteration, flexible spacing and either word order. It is a query '
    'syntax, NOT a filter for responsa literature. '
    'title = catalogue work titles (FJMS metadata), not manuscript text. '
    'shelfmark = a call number such as T-S 12.123 or ENA 1628.38.'
)

JOB_ID_PATTERN = r'^[A-Za-z0-9_-]{16,128}$'
# Three facade polls of up to ten seconds each: well inside Cloudflare's 125 s.
WAIT_POLLS = 3
DETAIL_SECTIONS = ('fgp_sources', 'fjms_bibliography', 'fjms_catalog', 'fjms_catalog_refs',
                   'nli_bibliography', 'nli_catalog', 'pgp_sources')


class ResponsaOptions(BaseModel):
    """Only with search_mode=responsa."""
    model_config = ConfigDict(extra='forbid')
    variants: bool | None = Field(None, description='Also letter-confusion spellings.')
    ja: bool | None = Field(None, description='Judeo-Arabic transliteration variants.')
    flex_spacing: bool | None = Field(None, description='Tolerate variable spacing within a phrase.')
    bidirectional: bool | None = Field(None, description='Match the phrase in either word order.')


class SearchFilters(BaseModel):
    """Documented values only; never guess catalogue values."""
    model_config = ConfigDict(extra='forbid')
    domains: list[str] | None = None
    authors: list[str] | None = None
    works: list[str] | None = None
    materials: list[str] | None = Field(None, description='Physical support (paper, parchment), not genre.')
    library: list[str] | None = Field(None, description='Known library codes, e.g. CUL, JTS, Oxford.')
    library_filter_mode: Literal['include', 'exclude'] | None = None
    date_from: int | None = None
    date_to: int | None = None


class Witness(BaseModel):
    """One witness of the work: text, or a raw_header returned by a tool -- never both."""
    model_config = ConfigDict(extra='forbid')
    label: str | None = Field(None, max_length=200)
    text: str | None = Field(None, min_length=1, max_length=5000)
    raw_header: str | None = Field(None, description='Exact header returned by a tool; never construct one.')


def _with_mcp_channel(facade_app):
    """The facade as only this adapter reaches it: every request labelled 'mcp'.

    A scope key, unlike a header or a tool argument, cannot be set by a caller, so
    the shared handlers' ``channel`` property cannot be spoofed (web/api_hardening.py).
    """
    async def channel_marked(scope, receive, send):
        if scope.get('type') == 'http':
            scope = {**scope, API_CHANNEL_SCOPE_KEY: 'mcp'}
        await facade_app(scope, receive, send)
    return channel_marked


async def _facade(target, client_ip: str, method: str, path: str, **kwargs) -> httpx.Response:
    """Call a facade route in-process as the visitor ``client_ip``."""
    transport = httpx.ASGITransport(app=target, client=(client_ip, 0))
    async with httpx.AsyncClient(transport=transport, base_url=FACADE_BASE_URL) as client:
        return await client.request(method, path, **kwargs)


# Request limits checked before the SDK sees a request. The SDK echoes a request's
# id in every reply and an unknown tool's name in its error, so an oversized id or
# name would carry an oversized response past the final guard (Codex, build review).
MAX_REQUEST_BYTES = 64 * 1024     # the GPT facade's own request cap
MAX_ID_CHARS = 128
MAX_NAME_CHARS = 64


def _refusal(body: bytes) -> str | None:
    """The reason to refuse a JSON-RPC request before dispatch, or None."""
    try:
        message = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return None  # the SDK answers a parse error without echoing the body
    for item in message if isinstance(message, list) else [message]:
        if not isinstance(item, dict):
            continue
        if isinstance(item.get('id'), str) and len(item['id']) > MAX_ID_CHARS:
            return 'request_id_too_long'
        if isinstance(item.get('method'), str) and len(item['method']) > MAX_NAME_CHARS:
            return 'method_too_long'
        params = item.get('params')
        if isinstance(params, dict) and isinstance(params.get('name'), str) and len(params['name']) > MAX_NAME_CHARS:
            return 'tool_name_too_long'
    return None


class _RequestGuard:
    """ASGI wrapper for POST /mcp: body size, id and name limits, with a small
    JSON-RPC error that echoes nothing. A class, because Starlette treats a plain
    function endpoint as a request handler rather than an ASGI app."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope.get('type') != 'http':
            await self.app(scope, receive, send)
            return
        body = bytearray()
        while True:
            message = await receive()
            if message['type'] == 'http.disconnect':
                return
            body.extend(message.get('body', b''))
            if len(body) > MAX_REQUEST_BYTES:
                await self._refuse(scope, send, 413, 'request_too_large')
                return
            if not message.get('more_body', False):
                break
        reason = _refusal(bytes(body))
        if reason:
            await self._refuse(scope, send, 400, reason)
            return
        delivered = False

        async def replay():
            nonlocal delivered
            if delivered:
                return await receive()
            delivered = True
            return {'type': 'http.request', 'body': bytes(body), 'more_body': False}

        await self.app(scope, replay, send)

    @staticmethod
    async def _refuse(scope, send, status: int, reason: str):
        capture_mcp_tool_event(
            tool='(refused before dispatch)', status_code=status, error_code=reason, latency_seconds=0.0,
            protocol_version=None, client_label=None, client_ip=_resolve_rate_limit_key(Request(scope)))
        payload = json.dumps({'jsonrpc': '2.0', 'id': None,
                              'error': {'code': -32600, 'message': f'Request refused: {reason}.'}}).encode()
        await send({'type': 'http.response.start', 'status': status,
                    'headers': [(b'content-type', b'application/json'),
                                (b'content-length', str(len(payload)).encode())]})
        await send({'type': 'http.response.body', 'body': payload})


ADAPTER_FAILURE = {'code': 'adapter_exception',
                   'message': ('The tool failed inside the server; no result is available. '
                               'This is an error, not an empty result.'),
                   'http_status': 500}


def _tool_error(response: httpx.Response) -> CallToolResult:
    """A facade failure as one JSON text block: its code and message, the original
    HTTP status, and Retry-After only when the facade sent one. Never an empty result."""
    try:
        error = response.json()['error']
        if not isinstance(error, dict) or 'code' not in error:
            raise TypeError
    except (ValueError, KeyError, TypeError):
        error = {'code': 'upstream_error', 'message': response.text[:2000]}
    error = {**error, 'http_status': response.status_code}
    retry_after = response.headers.get('retry-after')
    if retry_after is not None:
        error['retry_after_seconds'] = int(retry_after) if retry_after.isdigit() else retry_after
    text = json.dumps({'error': error}, ensure_ascii=False)
    return CallToolResult(content=[TextContent(type='text', text=text)], is_error=True)


# What the SDK adds around a tool result beyond the JSON-RPC envelope measured
# below; tests/test_mcp_budget.py checks real HTTP bodies against the bound.
META_RESERVE = 1_000


def _wire_size(ctx: Context, result: CallToolResult) -> int:
    """The response's size as the negotiated protocol serializes it: UTF-8 for the
    2025 handshake generation, json.dumps' default ensure_ascii for 2026-07-28 on."""
    modern = ctx.protocol_version not in HANDSHAKE_PROTOCOL_VERSIONS
    body = {'jsonrpc': '2.0', 'id': ctx.request_id,
            'result': result.model_dump(mode='json', by_alias=True, exclude_none=True)}
    return len(json.dumps(body, separators=(',', ':'), ensure_ascii=modern).encode('utf-8'))


def _bounded(ctx: Context, result: CallToolResult):
    """``(result, None)``, or an explicit error when it would exceed the bound. Never a truncation."""
    size = _wire_size(ctx, result) + META_RESERVE
    if size <= MCP_RESPONSE_BYTES:
        return result, None
    error = {'code': 'mcp_response_too_large',
             'message': ('This result is too large for this client. Narrow the request, '
                         'follow pagination, or open it on genizahsearch.com.'),
             'response_bytes': size, 'limit_bytes': MCP_RESPONSE_BYTES}
    text = json.dumps({'error': error}, ensure_ascii=False)
    return CallToolResult(content=[TextContent(type='text', text=text)], is_error=True), error['code']


_LABEL_UNSAFE = re.compile(r'[^A-Za-z0-9 ._-]')


def _client_label(ctx: Context):
    """The caller-supplied MCP clientInfo name, if any: a diagnostic label, never an identity."""
    try:
        name = ctx.request_context.session.client_params.client_info.name
    except AttributeError:
        return None
    return _LABEL_UNSAFE.sub('', str(name))[:64] or None


def _page_url(locator: dict):
    """The durable /browse address of a page: the same sys_id/page/volume_ie the
    website's own citation uses (web/pages/browse.py::_citation_page_url)."""
    if not locator.get('sys_id'):
        return None
    params = {'sys_id': locator['sys_id']}
    p_num = locator.get('p_num')
    if p_num is not None and int(p_num) > 0:
        params['page'] = int(p_num)
    if locator.get('volume_ie'):
        params['volume_ie'] = locator['volume_ie']
    return f'{SITE_URL}/browse?{urlencode(params)}'


def how_to_cite(page: dict, retrieved_on: str) -> dict:
    """The page citation for a browse result, from shared/transcription_credits.py.

    Whose text is credited follows ``text_source`` -- what was actually served --
    never the presence of metadata: a PGP page transcription credits PGP; a
    snippet is the indexed automatic transcription and credits MiDRASH even when
    the page also carries PGP metadata; ``none`` credits no transcription.
    """
    from shared.transcription_credits import page_citation
    source = page.get('text_source')
    pgp = (page.get('metadata') or {}).get('pgp') or {}
    if source == 'pgp_transcription':
        version_info, shown = {'source': 'pgp', 'attribution': pgp.get('transcription_source'),
                               'pgp_url': pgp.get('pgp_url')}, True
    elif source == 'none':
        version_info, shown = None, False
    else:  # 'snippet', and anything unrecognised: the automatic transcription
        version_info, shown = None, True
    locator = page.get('locator') or {}
    shelfmark = page.get('shelfmark') or (f"ID: {locator['sys_id']}" if locator.get('sys_id') else None)
    citations = {lang: page_citation(version_info, lang=lang,
                                     library=(page.get('library') or {}).get('name') or None,
                                     shelfmark=shelfmark, page_url=_page_url(locator),
                                     retrieved_on=retrieved_on, transcription_shown=shown)
                 for lang in ('en', 'he')}
    return {'en': citations['en'].text, 'he': citations['he'].text,
            'credits_midrash': citations['en'].credits_midrash, 'kind': citations['en'].kind}


def _with_how_to_cite(response: httpx.Response) -> str:
    from web.citation_chip import retrieved_today
    page = response.json()
    page['how_to_cite'] = how_to_cite(page, retrieved_today())
    return json.dumps(page, ensure_ascii=False, separators=(',', ':'))


async def _run_tool(ctx: Context, tool: str, operation, *, transform=None) -> CallToolResult:
    """Run ``operation(client_ip)`` -- one or more facade calls ending in the response
    the tool reports -- and record exactly one ``mcp_tool_call`` event.

    ``transform`` turns a successful facade response into the tool's text. Any
    ordinary exception becomes the coded ``adapter_exception`` error; every result,
    errors included, passes the final size guard.
    """
    request = ctx.request_context.request
    client_ip = _resolve_rate_limit_key(request) if request is not None else 'unknown'
    started = time.monotonic()
    status_code, error_code = 500, 'adapter_exception'
    try:
        try:
            response = await operation(client_ip)
            status_code = response.status_code
            if status_code >= 400:
                result = _tool_error(response)
                error_code = json.loads(result.content[0].text)['error']['code']
            else:
                text = transform(response) if transform else response.text
                result = CallToolResult(content=[TextContent(type='text', text=text)], is_error=False)
                error_code = None
        except asyncio.CancelledError:
            status_code, error_code = 499, 'cancelled'
            raise
        except Exception:
            status_code, error_code = 500, 'adapter_exception'
            result = CallToolResult(content=[TextContent(type='text', text=json.dumps({'error': ADAPTER_FAILURE}))],
                                    is_error=True)
        result, oversize = _bounded(ctx, result)
        error_code = oversize or error_code
        return result
    finally:
        capture_mcp_tool_event(
            tool=tool, status_code=status_code, error_code=error_code,
            latency_seconds=time.monotonic() - started,
            protocol_version=ctx.protocol_version, client_label=_client_label(ctx),
            client_ip=client_ip,
        )


async def _call_tool(target, ctx: Context, tool: str, method: str, path: str, *, transform=None, **kwargs):
    """One facade call for ``tool`` (see ``_run_tool``)."""
    async def operation(client_ip):
        return await _facade(target, client_ip, method, path, **kwargs)
    return await _run_tool(ctx, tool, operation, transform=transform)


def _job_path(job_id: str) -> str:
    # Escaped as one path segment: an id like '../capabilities' must not become
    # another facade route. The tool schema already restricts it to [A-Za-z0-9_-].
    return '/chatgpt/jobs/' + quote(job_id, safe='')


def _job_text(response: httpx.Response) -> str:
    """A job's page as is; a pending job names this server's polling tool."""
    if response.status_code != 202:
        return response.text
    pending = response.json()
    pending['next_action'] = 'get_research_job'
    return json.dumps(pending, ensure_ascii=False, separators=(',', ':'))


async def _start_and_wait(target, client_ip: str, path: str, body: dict) -> httpx.Response:
    """Submit a background job, then wait for it inside the tool call.

    Returns page 0 when the job finishes within WAIT_POLLS facade polls, else the
    pending reply with its job_id, so the model continues with get_research_job
    instead of resubmitting. A refused poll (429) also hands back the pending reply.
    """
    submitted = await _facade(target, client_ip, 'POST', path, json=body)
    if submitted.status_code != 202:
        return submitted
    job_path = _job_path(submitted.json()['job_id'])
    response = submitted
    for _ in range(WAIT_POLLS):
        response = await _facade(target, client_ip, 'GET', job_path)
        if response.status_code == 429:
            return submitted
        if response.status_code != 202:
            break
    return response


def build_server(facade_app) -> MCPServer:
    """An MCP server whose tools call ``facade_app`` (one server per facade)."""
    target = _with_mcp_channel(facade_app)
    server = MCPServer(
        name='GenizahSearch',
        title='GenizahSearch',
        instructions=INSTRUCTIONS,
        website_url='https://genizahsearch.com',
        version=APP_VERSION,
        subscriptions=False,
    )

    @server.tool(
        name='get_capabilities',
        title='GenizahSearch capabilities',
        description=('What this GenizahSearch deployment supports: search modes, passage '
                     'parallels and multi-witness features, and limits. Call once at the '
                     'start of research; a runtime error overrides what it advertises.'),
        annotations=READ_ONLY,
        structured_output=False,
    )
    async def get_capabilities(ctx: Context) -> CallToolResult:
        return await _call_tool(target, ctx, 'get_capabilities', 'GET', '/chatgpt/capabilities')

    @server.tool(
        name='browse_manuscript_page',
        title='Read a manuscript page',
        description=(
            'Read one manuscript page returned by a search: its text, where the text comes from, '
            'catalogue metadata, an image link, and how_to_cite. Read a page before quoting it '
            'or judging a match. Copy sys_id, p_num and volume_ie exactly from a result locator; '
            'never invent them. text_source says what the text is: pgp_transcription (a Princeton '
            'Geniza Project edition), snippet (part of the automatic transcription only; say so '
            'and infer nothing unseen), or none. An image link is not an inspection of the image. '
            'Cite with how_to_cite (en/he), which credits whoever made the text shown. '
            'Website link: https://genizahsearch.com/browse?sys_id=SID&page=P_NUM&volume_ie=IE.'),
        annotations=READ_ONLY,
        structured_output=False,
    )
    async def browse_manuscript_page(
        ctx: Context,
        sys_id: Annotated[str, Field(description='locator.sys_id from a result, copied exactly.')],
        p_num: Annotated[int, Field(ge=1, description='The one-based locator.p_num from a result.')],
        volume_ie: Annotated[str | None, Field(description='locator.volume_ie when present.')] = None,
        text_cap: Annotated[int, Field(ge=100, le=10000, description='Characters of text: start with 4000, '
                                                                      'raise to 10000 only if needed.')] = 4000,
    ) -> CallToolResult:
        params = {'sys_id': sys_id, 'p_num': p_num, 'text_cap': text_cap}
        if volume_ie:
            params['volume_ie'] = volume_ie
        return await _call_tool(target, ctx, 'browse_manuscript_page', 'GET', '/chatgpt/browse',
                                params=params, transform=_with_how_to_cite)

    @server.tool(
        name='search_manuscripts',
        title='Search Genizah manuscripts',
        description=(
            'Search the Cairo Genizah corpus for a Hebrew or Judeo-Arabic word or phrase, or look up a '
            'shelfmark. Starts a private background search that has no lasting effect and expires ten '
            'minutes after it finishes, then waits briefly: you get the first page of results, or a '
            'job_id to pass to get_research_job. Never resubmit a pending search; a retry after a lost '
            'reply starts a second search. Start with exact; for a composition search two to four '
            'distinctive phrases separately. ' + SEARCH_MODES +
            ' limit is how many candidates to save (default 100, fuzzy 500), not the page size.'),
        annotations=STARTS_A_JOB,
        structured_output=False,
    )
    async def search_manuscripts(
        ctx: Context,
        query: Annotated[str, Field(min_length=1, max_length=1000, description='The words to search for, or a shelfmark.')],
        search_mode: Annotated[Literal['exact', 'variants', 'fuzzy', 'responsa', 'title', 'shelfmark'],
                               Field(description='See the search modes in the tool description.')],
        limit: Annotated[int | None, Field(ge=1, le=500, description='Candidates to save: at most 100, '
                                                                    'or 500 for fuzzy.')] = None,
        gap: Annotated[int | None, Field(ge=0, description='Words allowed between terms; 0 for title and '
                                                           'shelfmark.')] = None,
        responsa_options: ResponsaOptions | None = None,
        filters: SearchFilters | None = None,
    ) -> CallToolResult:
        body = {'query': query, 'search_mode': search_mode}
        if limit is not None:
            body['limit'] = limit
        if gap is not None:
            body['gap'] = gap
        if responsa_options is not None:
            body['responsa_options'] = responsa_options.model_dump(exclude_none=True)
        if filters is not None:
            body['filters'] = filters.model_dump(exclude_none=True)

        async def operation(client_ip):
            return await _start_and_wait(target, client_ip, '/chatgpt/search/jobs', body)
        return await _run_tool(ctx, 'search_manuscripts', operation, transform=_job_text)

    @server.tool(
        name='find_passage_parallels',
        title='Find parallels to a passage',
        description=(
            'Find manuscripts that share a passage, allowing spelling differences (letter-level '
            'passage matching). Give one text of at most 5,000 characters, OR up to three witnesses of '
            'ONE work (each with text or a returned raw_header, never both; omit text then). Label and '
            'search longer input in sections and say what was covered. Starts a private background '
            'job like search_manuscripts and waits briefly: you get the first page or a job_id for '
            'get_research_job. Scores count matching letters; they are not probabilities. sort only '
            'with witnesses (fused unless asked otherwise).'),
        annotations=STARTS_A_JOB,
        structured_output=False,
    )
    async def find_passage_parallels(
        ctx: Context,
        text: Annotated[str | None, Field(min_length=1, max_length=5000, description='One passage.')] = None,
        witnesses: Annotated[list[Witness] | None, Field(min_length=1, max_length=3,
                                                          description='Witnesses of one work.')] = None,
        sort: Literal['fused', 'best_match', 'witness_count'] | None = None,
        filters: SearchFilters | None = None,
    ) -> CallToolResult:
        body = {'method': 'passage'}
        if text is not None:
            body['text'] = text
        if witnesses is not None:
            body['witnesses'] = [witness.model_dump(exclude_none=True) for witness in witnesses]
        if sort is not None:
            body['sort'] = sort
        if filters is not None:
            body['filters'] = filters.model_dump(exclude_none=True)

        async def operation(client_ip):
            return await _start_and_wait(target, client_ip, '/chatgpt/parallels/jobs', body)
        return await _run_tool(ctx, 'find_passage_parallels', operation, transform=_job_text)

    @server.tool(
        name='get_research_job',
        title='Get search results',
        description=(
            'Poll or page through one search started by search_manuscripts or find_passage_parallels. '
            'Copy job_id exactly; a pending state means the search is still running, not that it found '
            'nothing. Pages hold up to ten candidates: continue with the SAME job_id and '
            'pagination.next_page until it is null; never rerun the search. collection=filtered holds '
            'demoted candidates (e.g. duplicate photographs) with its own pages. job_not_found means the '
            'job expired or the server restarted, not an empty result.'),
        annotations=READ_ONLY,
        structured_output=False,
    )
    async def get_research_job(
        ctx: Context,
        job_id: Annotated[str, Field(pattern=JOB_ID_PATTERN, description='The job_id returned by the search.')],
        page: Annotated[int, Field(ge=0, description='0, then pagination.next_page.')] = 0,
        collection: Literal['results', 'filtered'] = 'results',
    ) -> CallToolResult:
        return await _call_tool(target, ctx, 'get_research_job', 'GET', _job_path(job_id),
                                params={'page': page, 'collection': collection}, transform=_job_text)

    @server.tool(
        name='get_manuscript_details',
        title='Read catalogue records and bibliography',
        description=(
            'Catalogue records, bibliography, or edition and translation credits for a returned sys_id, '
            'one source section at a time (FJMS catalogue, references or bibliography; NLI catalogue or '
            'MARC bibliography; PGP or FGP source credits, without text). Builds a ten-minute snapshot: '
            'page with next_offset and the SAME snapshot until next_offset is null; on details_expired '
            'restart the section; on details_pending retry after retry_after_seconds. Local records '
            'only: missing records do not prove absence. A bibliography entry is a lead, not proof '
            'that this passage was published.'),
        annotations=BUILDS_A_SNAPSHOT,
        structured_output=False,
    )
    async def get_manuscript_details(
        ctx: Context,
        sys_id: Annotated[str, Field(pattern=r'^[A-Za-z0-9_-]{1,100}$',
                                     description='A sys_id from a search or browse result; never guessed.')],
        section: Literal[DETAIL_SECTIONS],
        offset: Annotated[int, Field(ge=0, description='0, then next_offset.')] = 0,
        snapshot: Annotated[str | None, Field(max_length=200, description='Omit at first; then copy it.')] = None,
    ) -> CallToolResult:
        params = {'sys_id': sys_id, 'section': section, 'offset': offset}
        if snapshot:
            params['snapshot'] = snapshot
        return await _call_tool(target, ctx, 'get_manuscript_details', 'GET', '/chatgpt/manuscript-details',
                                params=params)

    return server


def _slash_redirect(security: TransportSecurityMiddleware):
    """``/mcp/`` -> 307 ``/mcp``, but only for a request ``/mcp`` itself would accept."""
    async def endpoint(request: Request):
        rejection = await security.validate_request(request, is_post=False)
        if rejection is not None:
            return rejection
        return RedirectResponse(str(request.url.replace(path=MCP_PATH)), status_code=307)
    return endpoint


def register_mcp_server(app, facade_app) -> bool:
    """Add POST /mcp to ``app`` when the flag is on and the facade is registered.

    Call right after the /api mount and before the site's page routes. Returns
    whether the routes were added; when they were not, /mcp is an ordinary 404.
    """
    if not MCP_ENABLED or getattr(facade_app.state, 'chatgpt_jobs', None) is None:
        return False
    server = build_server(facade_app)
    # Builds and attaches the session manager; the returned Starlette app is unused
    # because we serve the manager from our own exact route.
    server.streamable_http_app(json_response=True, stateless_http=True,
                               transport_security=SECURITY_SETTINGS)
    manager = server.session_manager
    app.router.add_route(MCP_PATH, _RequestGuard(StreamableHTTPASGIApp(manager)), methods=['POST'],
                         include_in_schema=False)
    # A function route defaults to GET only; list every method Starlette's own slash
    # redirect would have answered, so the redirect behaves the same apart from the check.
    app.router.add_route(MCP_PATH + '/', _slash_redirect(TransportSecurityMiddleware(SECURITY_SETTINGS)),
                         methods=['GET', 'POST', 'DELETE'], include_in_schema=False)

    previous_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def lifespan(application):
        async with previous_lifespan(application) as state:
            async with manager.run():
                yield state

    app.router.lifespan_context = lifespan
    return True
