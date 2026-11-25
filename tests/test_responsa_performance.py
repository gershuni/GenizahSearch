"""
Search speed on the real Tantivy index: what a website user and an API caller wait for.

These tests require the real Tantivy index, looked for where the apps look for it
(tests/real_index.py: Config.INDEX_DIR). Where there is none (CI), ALL tests in this
file are skipped gracefully.

test_exact_search_shows_first_results_within_3s runs a plain Exact search the way the
website does (web/pages/search.py: engine mode `literal`, the Genizah corpus, a
preview of the first rows), through production's IsolatedEngine and a warm research
worker, and asserts that the first results are handed on within 3 seconds: the 3 s
target of the plain-search benchmark it replaces. That benchmark timed engine mode
`exact`, which neither the website nor the API runs for Exact any more.

TestPerformanceBenchmarks times four Responsa searches over a common word (shalom; with
prefix expansion; with variants + JA; `#shalom [3] olam`) on the two paths that were slow
(docs/open_issues.d/2026-11-24-responsa-common-word-searches-slow.md, fixed 2026-11-25).
Every one of tens of thousands of rows used to be built before anything was shown or
returned. Measured 2026-11-25 through a research worker on this index: the website showed
nothing for 13-33 s (matching as production's Linux workers do, with stdlib re) or 27 s to
2 minutes (as Windows workers do, with the regex module), and the API answered #shalom with
variants + JA in 22 s or with 504 at its 30 s limit (re), and all three with 504 (regex).
Now:
- the website shows a Responsa search's first rows while it runs, as it does Exact's:
  within RESPONSA_FIRST_RESULTS_SECONDS, on production's route (a fresh research worker,
  which compiles the query's patterns and spellings first): 0.9-1.5 s for shalom, 2.1-2.8 s
  for the gap query (its first match comes later in score order);
- the API builds only the rows it returns (execute_search's max_rows) and counts the
  rest: within API_SECONDS, two thirds of the API's 30 s limit. Timed in this process as
  production's Linux research workers match, with stdlib re (shared/search_regex.py):
  Windows workers and the desktop match with the regex module, whose cost per page on these
  patterns is several times higher (the last three searches here: 5.8 s / 6.9 s / 8.5 s
  with re, 9.9 s / 31 s / 70 s with regex). The gap query's time is checking its
  candidates, which counting needs as much as building did.
The 2026-02 benchmarks they replace timed the whole list against 10 s.
"""

import threading
import time
import pytest

FIRST_RESULTS_SECONDS = 3.0
RESPONSA_FIRST_RESULTS_SECONDS = 5.0
API_SECONDS = 20.0
API_LIMIT = 50                 # /api/search's default `limit`


@pytest.fixture(scope="module")
def real_engine(real_web_state):
    """The real SearchEngine over the real catalogue (tests/real_index.py), loaded
    once for the module. Skips where there is no index."""
    return real_web_state.searcher


@pytest.fixture(scope="module")
def real_engine_with_editions(real_web_state):
    """An engine over the same index and catalogue with the scholarly editions attached, as
    production serves them (the API's default is then sources 'all'). Skips where there is no
    coherent editions generation (<INDEX_DIR>/Editions)."""
    from shared.search_engine import SearchEngine
    engine = SearchEngine(real_web_state.meta_mgr, real_web_state.var_mgr, build_fl_index=False)
    try:
        if not engine.attach_editions():
            pytest.skip("no coherent editions generation under Config.INDEX_DIR/Editions")
        yield engine
    finally:
        engine.close_index()


def _responsa(variants, ja):
    """The options the website and the API pass for a Responsa search."""
    return {'responsa_mode': True, 'variants': variants, 'ja': ja, 'flex_spacing': False,
            'bidirectional': False, 'variant_mode': 'variants' if variants else 'exact'}


SHALOM = "שלום"   # shalom
OLAM = "עולם"     # olam
RESPONSA_SEARCHES = {
    # name: (query, the Responsa options)
    'no_expansions': (SHALOM, _responsa(False, False)),
    'prefix_expansion': ("#" + SHALOM, _responsa(False, False)),
    'variants_and_ja': ("#" + SHALOM, _responsa(True, True)),
    'two_words_with_gap': ("#" + SHALOM + " [3] " + OLAM, _responsa(True, False)),
}


@pytest.fixture(scope="class")
def warm_research_queue():
    """The site at rest as it ships: one slot, its worker started ahead of each search
    (an environment with GENIZAH_RESEARCH_PRESTART=0 would start none). A fresh queue
    for the class, which reads both settings when it is created; closed after it."""
    from web.research_jobs import shutdown_research
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv('GENIZAH_RESEARCH_PRESTART', '1')
        mp.setenv('GENIZAH_RESEARCH_WORKERS', '1')
        shutdown_research()
        try:
            yield
        finally:
            shutdown_research()


class TestPerformanceBenchmarks:
    """Responsa over a common word: the website's first rows, and the API's answer."""

    @pytest.mark.parametrize('name', list(RESPONSA_SEARCHES))
    def test_the_website_shows_the_first_results_within_5s(self, real_engine, warm_research_queue, name):
        """Run as web/pages/search.py::_run_core_search runs a Responsa search (engine mode
        `responsa`, the Genizah corpus, early rows asked for), through production's
        IsolatedEngine and a warm research worker. The time runs from the call to the
        wrapper handing on the first rows; the search is then stopped (Stop), since the
        rest of it is not what this measures."""
        from web import research_jobs
        from web.research_jobs import IsolatedEngine

        query, options = RESPONSA_SEARCHES[name]
        first = {}
        stop = threading.Event()
        _wait_warm(research_jobs.get_queue())
        engine = IsolatedEngine(real_engine, 'search').controlled(cancel=stop)
        start = time.perf_counter()

        def preview(rows):
            first.setdefault('seconds', time.perf_counter() - start)
            first.setdefault('rows', len(rows))
            stop.set()

        try:
            engine.execute_search(query, mode='responsa', gap=0, exclude_words=[],
                                  responsa_options=options, restrict_sys_ids=None,
                                  text_position=None, corpus_scope='genizah',
                                  preview_callback=preview)
        except InterruptedError:
            pass                  # stopped once the first rows were handed on
        elapsed = time.perf_counter() - start
        assert 'seconds' in first, f"{name}: no early results ({elapsed:.1f}s)"
        assert first['rows'] >= 1
        assert first['seconds'] < RESPONSA_FIRST_RESULTS_SECONDS, (
            f"{name}: first results after {first['seconds']:.2f}s, over the "
            f"{RESPONSA_FIRST_RESULTS_SECONDS:.0f}s ceiling")

    @pytest.mark.parametrize('name', list(RESPONSA_SEARCHES))
    def test_the_api_answers_well_within_its_time_limit(self, real_engine, name):
        """The engine call web/search_api.py makes for a Responsa search with the default
        limit -- max_rows=50, the corpus the API names (none: all) -- matched as a Linux
        research worker matches (isolated_matching(native=True): stdlib re)."""
        from shared.search_regex import isolated_matching

        query, options = RESPONSA_SEARCHES[name]
        start = time.perf_counter()
        with isolated_matching(native=True):
            rows = real_engine.execute_search(query, 'responsa', 0, responsa_options=options,
                                              max_rows=API_LIMIT)
        elapsed = time.perf_counter() - start
        assert rows, f"{name}: the search found nothing in the real corpus"
        total = rows[0].get('result_total')
        assert isinstance(total, int) and len(rows) == min(API_LIMIT, total), (name, len(rows), total)
        assert elapsed < API_SECONDS, (
            f"{name}: {elapsed:.1f}s for {len(rows)} rows of {total}, over the {API_SECONDS:.0f}s "
            f"ceiling (the API's limit is 30 s)")

    @pytest.mark.parametrize('name', ['prefix_expansion', 'variants_and_ja', 'two_words_with_gap'])
    def test_the_api_with_the_editions_answers_well_within_its_time_limit(self, real_engine_with_editions,
                                                                         name):
        """Production's default: the editions served, so the API reads them too (sources
        'all', one worker): the editions leg first, then only the main rows that can be among
        the first results (SearchEngine._first_grouped_rows). Measured 2026-11-25 through the
        API in a research worker with stdlib re: 12-16 s, 20 s, 16-22 s, against 20-30 s, 23 s
        or 504, 14-18 s before (the gap query's time is the check, and the editions leg now runs
        before the main leg instead of beside it)."""
        from shared.search_regex import isolated_matching

        query, options = RESPONSA_SEARCHES[name]
        start = time.perf_counter()
        with isolated_matching(native=True):
            rows = real_engine_with_editions.execute_search(
                query, 'responsa', 0, responsa_options=options, sources='all', max_rows=API_LIMIT)
        elapsed = time.perf_counter() - start
        assert rows, f"{name}: the search found nothing in the real corpus"
        total = rows[0].get('result_total')
        assert isinstance(total, int) and len(rows) == min(API_LIMIT, total), (name, len(rows), total)
        # One result per page with the editions (a My Library row, where there is one, is not).
        assert any('members' in r for r in rows), "not grouped: the editions were not read"
        assert elapsed < API_SECONDS, (
            f"{name}: {elapsed:.1f}s for {len(rows)} results of {total}, over the "
            f"{API_SECONDS:.0f}s ceiling (the API's limit is 30 s)")


def _wait_warm(queue, timeout=180.0):
    """Until every slot of *queue* has a worker that has loaded the catalogue, as on a site
    at rest (web/research_jobs.py starts each slot's next worker ahead of its search; the
    same wait as scripts/verify_editions_search.py)."""
    deadline = time.monotonic() + timeout
    while True:
        with queue.condition:
            spares = dict(queue.spares)
        assert not any((w.root / "warm.failed").exists() for w in spares.values()), (
            "a research worker could not load the catalogue ahead of its search")
        if len(spares) == queue.capacity and all((w.root / "warm.ready").exists()
                                                 for w in spares.values()):
            return
        assert time.monotonic() < deadline, "research workers did not finish loading"
        time.sleep(0.2)


def test_exact_search_shows_first_results_within_3s(real_engine, monkeypatch):
    """A plain Exact search, run as the website runs it, has its first results ready
    within 3 s: the website shows them while the rest of the search runs.

    The route is production's: web/main.py wraps the engine in IsolatedEngine, so the
    search runs in a research worker (a fresh subprocess, started and loaded ahead of
    the search), which writes its early rows to preview.pkl for the wrapper to hand on.
    The call is web/pages/search.py::_run_core_search's for a plain search: engine
    mode engine_mode('exact') (`literal`), the Genizah corpus, and a preview_callback
    (asked for when there are no NOT-words, no Responsa and only the automatic
    transcriptions). The time runs from the call to the wrapper handing on the first
    rows: queueing, the worker's index opening and search, and the hand-over. Measured
    2026-11-24 for 'shalom' in-process: first rows 0.5-1.6 s; the whole search 3-6 s,
    24,095 rows.
    """
    from web import research_jobs
    from web.pages.search_state import engine_mode
    from web.research_jobs import IsolatedEngine, shutdown_research

    query = "שלום"  # shalom
    first = {}
    # The site at rest as it ships: one slot, its worker started ahead of the search. An
    # environment with GENIZAH_RESEARCH_PRESTART=0 would start none, and the wait below
    # would time out over a setting, not a slow search.
    monkeypatch.setenv('GENIZAH_RESEARCH_PRESTART', '1')
    monkeypatch.setenv('GENIZAH_RESEARCH_WORKERS', '1')
    shutdown_research()  # a fresh queue (it reads both at creation), whose slot starts its worker now
    try:
        _wait_warm(research_jobs.get_queue())
        engine = IsolatedEngine(real_engine, 'search')
        start = time.perf_counter()

        def preview(rows):
            first.setdefault("seconds", time.perf_counter() - start)
            first.setdefault("rows", len(rows))

        results = engine.execute_search(
            query, mode=engine_mode('exact'), gap=0, responsa_options=None,
            corpus_scope='genizah', preview_callback=preview)
        total = time.perf_counter() - start
    finally:
        shutdown_research()

    assert results, "the search found nothing in the real corpus"
    assert "seconds" in first, (
        f"no early results: the preview never fired ({len(results)} rows, {total:.1f}s)")
    assert first["rows"] >= 1
    assert first["seconds"] < FIRST_RESULTS_SECONDS, (
        f"first results after {first['seconds']:.2f}s, over the {FIRST_RESULTS_SECONDS:.0f}s "
        f"target (whole search {total:.1f}s, {len(results)} rows)")
