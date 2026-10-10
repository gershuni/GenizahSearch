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

EXPECTED_TOOLS = {'get_capabilities', 'search_manuscripts', 'find_passage_parallels', 'get_research_job',
                  'browse_manuscript_page', 'get_manuscript_details'}
SHELFMARK = 'ENA 1628.38'


async def call(client, failures, name, arguments):
    started = time.monotonic()
    result = await client.call_tool(name, arguments)
    text = result.content[0].text if result.content else ''
    print(f'  {name:24} {"ERROR" if result.is_error else "ok":5} {time.monotonic() - started:6.2f}s '
          f'{len(text.encode("utf-8")):7} bytes')
    if result.is_error:
        failures.append(f'{name}: {text[:300]}')
        return None
    return json.loads(text)


async def run(base_url, mode, failures):
    print(f'{mode}:')
    async with Client(f'{base_url.rstrip("/")}/mcp', mode=mode) as client:
        tools = {tool.name for tool in (await client.list_tools()).tools}
        if tools != EXPECTED_TOOLS:
            failures.append(f'{mode}: tools {sorted(tools)}')
        await call(client, failures, 'get_capabilities', {})
        found = await call(client, failures, 'search_manuscripts', {'query': SHELFMARK, 'search_mode': 'shelfmark'})
        for _ in range(20):
            if not found or 'job_id' not in found or found.get('results') is not None:
                break
            found = await call(client, failures, 'get_research_job', {'job_id': found['job_id']})
        results = (found or {}).get('results') or []
        if not results:
            failures.append(f'{mode}: no result for {SHELFMARK}')
            return
        locator = results[0].get('locator') or {}
        page = await call(client, failures, 'browse_manuscript_page', {
            'sys_id': locator['sys_id'], 'p_num': int(locator.get('p_num') or 1),
            **({'volume_ie': locator['volume_ie']} if locator.get('volume_ie') else {})})
        if page is not None and not (page.get('how_to_cite') or {}).get('en'):
            failures.append(f'{mode}: browse without how_to_cite')
        await call(client, failures, 'get_manuscript_details', {'sys_id': locator['sys_id'],
                                                                'section': 'fjms_catalog'})


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
