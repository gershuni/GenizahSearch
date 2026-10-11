"""Smoke test for a deployed GenizahSearch MCP server.

    python integrations/mcp/smoke_test.py https://genizahsearch.com
    python integrations/mcp/smoke_test.py http://localhost:8084

Connects with the MCP SDK under both protocol generations and runs a short research path
on public example data: list the tools, read capabilities, look up a shelfmark, wait for
the job, read the first page with its citation, and read one details section. Prints one
line per call with status, latency and size; exits 1 on any failure. Proves the HTTP path,
not a client: run integrations/mcp/acceptance-tests.md in real clients too.
"""
from __future__ import annotations

import asyncio
import json
import sys
import time

from mcp.client import Client

EXPECTED_TOOLS = {'get_capabilities', 'search_manuscripts', 'find_passage_parallels', 'find_chunk_parallels',
                  'get_research_job', 'browse_manuscript_page', 'get_manuscript_details'}
SHELFMARK = 'ENA 1628.38'
CHUNK_TEXT = 'ברוך אתה ה אלהינו מלך העולם'   # short: one chunk job holds a shared research worker
CHUNK_BUSY_TRIES = 5
JOB_DEADLINE = 600      # the facade's job allowance (web/chatgpt_jobs.py MAX_JOB_SECONDS)
DETAILS_DEADLINE = 60   # details_pending is temporary; give the local lookup a minute


async def call(client, failures, name, arguments, *, retry_codes=()):
    """One tool call; an error whose code is in ``retry_codes`` is returned, not failed."""
    started = time.monotonic()
    result = await client.call_tool(name, arguments)
    text = result.content[0].text if result.content else ''
    print(f'  {name:24} {"ERROR" if result.is_error else "ok":5} {time.monotonic() - started:6.2f}s '
          f'{len(text.encode("utf-8")):7} bytes')
    if result.is_error:
        try:
            error = json.loads(text)['error']
        except (ValueError, KeyError, TypeError):
            error = {}
        if error.get('code') in retry_codes:
            return {'error': error}
        failures.append(f'{name}: {text[:300]}')
        return None
    return json.loads(text)


async def wait_for_job(client, failures, found):
    """Poll a pending job until page 0, an error, or the facade's own deadline."""
    deadline = time.monotonic() + JOB_DEADLINE
    while found and 'job_id' in found and found.get('results') is None:
        if time.monotonic() > deadline:
            failures.append(f'job {found["job_id"][:8]}... still {found.get("state")} after {JOB_DEADLINE} s: '
                            'timed out, which is not the same as no results')
            return None
        await asyncio.sleep(found.get('poll_after_seconds', 2))
        found = await call(client, failures, 'get_research_job', {'job_id': found['job_id']})
    return found


async def run(base_url, mode, failures):
    print(f'{mode}:')
    async with Client(f'{base_url.rstrip("/")}/mcp', mode=mode) as client:
        tools = {tool.name for tool in (await client.list_tools()).tools}
        if tools != EXPECTED_TOOLS:
            failures.append(f'{mode}: tools {sorted(tools)}')
        await call(client, failures, 'get_capabilities', {})
        found = await call(client, failures, 'search_manuscripts', {'query': SHELFMARK, 'search_mode': 'shelfmark'})
        found = await wait_for_job(client, failures, found)
        if found is None:
            return
        results = found.get('results') or []
        if not results:
            failures.append(f'{mode}: the search completed with no result for {SHELFMARK}')
            return
        locator = results[0].get('locator') or {}
        p_num = str(locator.get('p_num') or '')
        if not locator.get('sys_id') or not p_num.isdigit() or int(p_num) < 1:
            failures.append(f'{mode}: the first result has no usable locator: {locator}')
            return
        page = await call(client, failures, 'browse_manuscript_page', {
            'sys_id': locator['sys_id'], 'p_num': int(p_num),
            **({'volume_ie': locator['volume_ie']} if locator.get('volume_ie') else {})})
        if page is not None and not (page.get('how_to_cite') or {}).get('en'):
            failures.append(f'{mode}: browse without how_to_cite')
        deadline = time.monotonic() + DETAILS_DEADLINE
        while True:
            details = await call(client, failures, 'get_manuscript_details',
                                 {'sys_id': locator['sys_id'], 'section': 'fjms_catalog'},
                                 retry_codes=('details_pending',))
            if not details or 'error' not in details:
                break
            if time.monotonic() > deadline:
                failures.append(f'{mode}: details still pending after {DETAILS_DEADLINE} s')
                break
            await asyncio.sleep(details['error'].get('retry_after_seconds', 5))
        await check_chunk_search(client, failures, mode)


async def check_chunk_search(client, failures, mode):
    """A chunk search with spelling variants completes and says which search it ran."""
    arguments = {'text': CHUNK_TEXT, 'chunk_size': 3, 'mode': 'variants'}
    for _ in range(CHUNK_BUSY_TRIES):  # one chunk search runs at a time for all users
        chunk = await call(client, failures, 'find_chunk_parallels', arguments, retry_codes=('research_jobs_busy',))
        if not chunk or 'error' not in chunk:
            break
        await asyncio.sleep(chunk['error'].get('retry_after_seconds', 30))
    else:
        failures.append(f'{mode}: chunk search still busy after {CHUNK_BUSY_TRIES} tries')
        return
    chunk = await wait_for_job(client, failures, chunk)
    if chunk is None:
        return
    echo = chunk.get('request') or {}
    if (echo.get('method'), echo.get('mode'), echo.get('chunk_size')) != ('chunk', 'variants', 3):
        failures.append(f'{mode}: the chunk search ran as {echo}')
    if not isinstance(chunk.get('results'), list):
        failures.append(f'{mode}: the chunk search returned no results list')


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(2)
    failures = []
    for mode in ('legacy', '2026-07-28'):
        try:
            asyncio.run(run(sys.argv[1], mode, failures))
        except Exception as exc:  # report and continue with the other generation
            failures.append(f'{mode}: {type(exc).__name__}: {exc}')
    print('FAILED:\n  ' + '\n  '.join(failures) if failures else 'all checks passed')
    sys.exit(1 if failures else 0)


if __name__ == '__main__':
    main()
