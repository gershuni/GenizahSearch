"""Focused tests for the process-wide public-search load budgets."""
import asyncio
from pathlib import Path

from web.search_load_control import (
    UI_SEARCH_CONCURRENCY,
    enrichment_batch_slot,
    try_acquire_ui_search_slot,
)


def test_ui_search_slots_fail_fast_at_configured_capacity():
    async def exercise():
        releases = [await try_acquire_ui_search_slot() for _ in range(UI_SEARCH_CONCURRENCY)]
        assert all(releases)
        assert await try_acquire_ui_search_slot() is None
        releases.pop()()
        release = await try_acquire_ui_search_slot()
        assert release is not None
        release()
        for release in releases:
            release()

    asyncio.run(exercise())


def test_enrichment_batch_slot_releases_after_use():
    async def exercise():
        async with enrichment_batch_slot():
            pass
        async with enrichment_batch_slot():
            pass

    asyncio.run(exercise())


def test_cancelled_ui_awaiter_signals_its_isolated_job():
    """Cancellation reaches the waiter even without engine progress events."""
    from threading import Event
    from web.research_jobs import run_research_call, _cancel_context
    started, finish = Event(), Event()

    def worker():
        started.set()
        assert _cancel_context.get().wait(3), 'cancellation did not reach worker'
        finish.set()
        return []

    async def exercise():
        task = asyncio.create_task(run_research_call(worker))
        assert await asyncio.to_thread(started.wait, 1)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        assert await asyncio.to_thread(finish.wait, 1)
    asyncio.run(exercise())


def test_search_ui_uses_bounded_background_enrichment_and_core_permit():
    source = Path('web/pages/search.py').read_text(encoding='utf-8')
    assert 'await run_research_call(run_core_search)' in source
    assert 'async with enrichment_batch_slot()' in source
    assert 'asyncio.ensure_future(_run_remaining_enrichment())' in source


def test_result_cards_defer_optional_joins_and_hidden_thumbnail_fetches():
    source = Path('web/pages/search_results.py').read_text(encoding='utf-8')
    assert 'async def _open_joins_for_card' in source
    assert 'loading="lazy" decoding="async"' in source
    assert '_load_card_joins_count' not in source
    assert '_schedule_card_joins_count' not in source


def test_core_search_emits_query_free_timing_metrics():
    source = Path('shared/search_engine.py').read_text(encoding='utf-8')
    assert '"search_perf mode=%s scope=%s candidates=%d regex_kept=%d final=%d "' in source
    assert 'tantivy_ms=%.0f materialize_ms=%.0f local_merge_ms=%.0f total_ms=%.0f' in source
