import pytest

from web.search_bootstrap import resolve_search_bootstrap


def test_restores_saved_state_without_route_context():
    state = resolve_search_bootstrap(
        initial_query=None,
        initial_tag=None,
        initial_mode=None,
        initial_domain=None,
        from_browse=None,
        saved_mode='Title',
        saved_query='ישן',
        saved_results_count=0,
        use_slider=False,
    )

    assert state == {
        'mode': 'Title',
        'query': 'ישן',
        'restore_saved_results': True,
        'restore_saved_filters': True,
        'restore_run_filters': False,
        'restore_saved_exclusions': True,
        'rejoin_run': False,
    }


def test_query_route_uses_clean_default_state():
    state = resolve_search_bootstrap(
        initial_query='בלי ירח',
        initial_tag=None,
        initial_mode=None,
        initial_domain=None,
        from_browse=None,
        saved_mode='Title',
        saved_query='ישן',
        saved_results_count=0,
        use_slider=False,
    )

    assert state == {
        'mode': 'exact',
        'query': 'בלי ירח',
        'restore_saved_results': False,
        'restore_saved_filters': False,
        'restore_run_filters': False,
        'restore_saved_exclusions': False,
        'rejoin_run': False,
    }


def test_explicit_route_mode_wins_and_slider_maps_extended_variants():
    state = resolve_search_bootstrap(
        initial_query='שלום',
        initial_tag=None,
        initial_mode='variants_extended',
        initial_domain=None,
        from_browse=None,
        saved_mode='Title',
        saved_query='ישן',
        saved_results_count=0,
        use_slider=True,
    )

    assert state['mode'] == 'variants'
    assert state['query'] == 'שלום'
    assert state['restore_saved_results'] is False


def test_tag_route_forces_tag_mode_and_clears_saved_query():
    state = resolve_search_bootstrap(
        initial_query=None,
        initial_tag='Merchant',
        initial_mode=None,
        initial_domain=None,
        from_browse=None,
        saved_mode='Regex',
        saved_query='ישן',
        saved_results_count=0,
        use_slider=False,
    )

    assert state == {
        'mode': 'pgp_tags',
        'query': '',
        'restore_saved_results': False,
        'restore_saved_filters': False,
        'restore_run_filters': False,
        'restore_saved_exclusions': False,
        'rejoin_run': False,
    }


def test_browse_navigation_disables_saved_state_restore():
    state = resolve_search_bootstrap(
        initial_query=None,
        initial_tag=None,
        initial_mode=None,
        initial_domain='Letters',
        from_browse=1,
        saved_mode='Regex',
        saved_query='ישן',
        saved_results_count=0,
        use_slider=False,
    )

    assert state == {
        'mode': 'exact',
        'query': '',
        'restore_saved_results': False,
        'restore_saved_filters': False,
        'restore_run_filters': False,
        'restore_saved_exclusions': False,
        'rejoin_run': False,
    }


def test_back_navigation_from_browse_restores_saved_results():
    """Browser Back from /browse to /search?q=... must restore the saved
    snapshot when URL query matches saved_query and snapshot has results.

    What IS restored on back-nav:
      - cached results (restore_saved_results=True)
      - saved mode (restore_saved_results path uses saved_mode)
      - refinement chain (via restore_saved_results path in search.py)
      - exclusions (restore_saved_exclusions=True)

    What is NOT restored on back-nav:
      - pre-search filters (material/domain/printed sliders) — restore_saved_filters=False.
        Rationale: history.replaceState only stamps q/tag/mode/variants into the URL,
        never filters. There is no authoritative round-trip signal that filter state
        belongs to the URL. 829cd7cf (2026-03-27) intent preserved.

    Regression test for 75-UAT.md surface 1 blocker (back-navigation
    state loss). See 75-03-PLAN.md.
    """
    state = resolve_search_bootstrap(
        initial_query='שלום',
        initial_tag=None,
        initial_mode=None,
        initial_domain=None,
        from_browse=None,
        saved_mode='exact',
        saved_query='שלום',
        saved_results_count=12,
        use_slider=False,
    )

    assert state['restore_saved_results'] is True, (
        "Back-navigation must restore cached results"
    )
    assert state['restore_saved_exclusions'] is True, (
        "Back-navigation must restore exclusions (they were part of the snapshot)"
    )
    assert state['restore_saved_filters'] is False, (
        "Back-navigation must NOT restore filters "
        "(history.replaceState did not stamp them; 829cd7cf intent preserved)"
    )
    assert state['query'] == 'שלום'
    assert state['mode'] == 'exact'


def test_fresh_query_route_with_different_saved_query_still_uses_clean_state():
    """Genuinely-fresh /search?q=X (different query than saved) must still
    use clean deterministic state — do not inherit stale snapshot.

    Preserves commit 829cd7cf (2026-03-27) intent.
    """
    state = resolve_search_bootstrap(
        initial_query='שלום',
        initial_tag=None,
        initial_mode=None,
        initial_domain=None,
        from_browse=None,
        saved_mode='exact',
        saved_query='לילה',  # different query than URL
        saved_results_count=12,
        use_slider=False,
    )

    assert state['restore_saved_results'] is False
    assert state['restore_saved_filters'] is False
    assert state['restore_saved_exclusions'] is False
    assert state['query'] == 'שלום'
    assert state['mode'] == 'exact'


def test_query_route_with_matching_saved_but_empty_snapshot_does_not_falsely_restore():
    """URL query matches saved_query but snapshot has zero results —
    nothing to restore, fall through to clean state.
    """
    state = resolve_search_bootstrap(
        initial_query='שלום',
        initial_tag=None,
        initial_mode=None,
        initial_domain=None,
        from_browse=None,
        saved_mode='exact',
        saved_query='שלום',
        saved_results_count=0,  # empty snapshot
        use_slider=False,
    )

    assert state['restore_saved_results'] is False
    assert state['restore_saved_filters'] is False
    assert state['restore_saved_exclusions'] is False


def test_back_navigation_restores_saved_mode_when_saved_mode_is_title():
    """When user was searching in 'Title' mode and hits Back, restore 'Title'
    mode — not reset to 'exact'. Verifies Edit 1(d)'s is_back_navigation
    branch honors saved_mode beyond the default.

    Added per checker warning 4: the mode-restoration branch was untested
    when saved_mode != 'exact'.
    """
    state = resolve_search_bootstrap(
        initial_query='שלום',
        initial_tag=None,
        initial_mode=None,
        initial_domain=None,
        from_browse=None,
        saved_mode='Title',
        saved_query='שלום',
        saved_results_count=42,
        use_slider=False,
    )

    assert state['restore_saved_results'] is True
    assert state['mode'] == 'Title'


# --- Back by run code, in every mode (2026-10-22, PR #436) ---------------------
# Every search stamps its address when it starts: q, the page's mode, the Responsa
# options and a random run=<code> kept in its run (last_run['url_stamp']). Back is
# that code with the same query, mode and options. Measured before this: Back
# re-ran a Responsa search (its URL carried a mode the old check refused), cut a
# finished list to 250 rows in every mode (the per-tab copy cannot be read while the
# page is built), and ran a search again when it was left before it finished.


_RESPONSA_QUERY = '(מאמתי/מאימתי) (קורין/קוריין)'
_CODE = 'k3J9xQ_a'
_RUN_FILTERS = {'filter_domains': ['Halakha'], 'filter_include_mode': True, 'filter_date_from': 1000}
_ABSENT = object()


def _run(query=_RESPONSA_QUERY, page_mode='responsa', url_stamp=_CODE,
         pre_search_filters=_ABSENT, **options):
    run = {
        'query': query, 'mode': page_mode, 'page_mode': page_mode, 'gap': 0, 'exclude_words': [],
        'text_position': None,
        'responsa_options': {
            'responsa_mode': True,
            'variants': options.get('variants', True),
            'ja': options.get('ja', False),
            'flex_spacing': options.get('flex_spacing', True),
            'bidirectional': options.get('bidirectional', False),
            'variant_mode': 'variants' if options.get('variants', True) else 'exact',
        } if page_mode == 'responsa' else None,
        'corpus_scope': 'genizah', 'variant_settings': {},
        'pre_search_filters': dict(_RUN_FILTERS) if pre_search_filters is _ABSENT else pre_search_filters,
    }
    if url_stamp is not None:
        run['url_stamp'] = url_stamp
    return run


def _back(*, initial_query=_RESPONSA_QUERY, initial_mode='responsa', initial_responsa=None,
          initial_run=_CODE, saved_run=_ABSENT, live_run=None, saved_results_count=81,
          saved_mode='exact', saved_query=_RESPONSA_QUERY, from_browse=None):
    # The URL search.py stamps for Responsa with variants + flexible spacing:
    # &mode=responsa&variants=1&flex_spaces=1&run=<code> (the route's ints, by the
    # engine's option names).
    if initial_responsa is None and initial_mode == 'responsa':
        initial_responsa = {'variants': 1, 'ja': None, 'flex_spacing': 1, 'bidirectional': None}
    return resolve_search_bootstrap(
        initial_query=initial_query,
        initial_tag=None,
        initial_mode=initial_mode,
        initial_domain=None,
        from_browse=from_browse,
        saved_mode=saved_mode,
        saved_query=saved_query,
        saved_results_count=saved_results_count,
        use_slider=False,
        initial_responsa=initial_responsa,
        initial_run=initial_run,
        saved_run=_run() if saved_run is _ABSENT else saved_run,
        live_run=live_run,
    )


def test_back_to_a_responsa_search_restores_its_results_and_its_runs_filters():
    state = _back()

    assert state == {
        'mode': 'responsa',
        'query': _RESPONSA_QUERY,
        'restore_saved_results': True,
        'restore_saved_filters': False,
        'restore_run_filters': True,
        'restore_saved_exclusions': True,
        'rejoin_run': False,
    }


@pytest.mark.parametrize('page_mode', ['exact', 'variants', 'variants_extended', 'fuzzy', 'Regex', 'Title'])
def test_back_by_run_code_works_in_every_mode(page_mode):
    state = _back(initial_query='שלום', initial_mode=page_mode,
                  saved_run=_run(query='שלום', page_mode=page_mode))
    assert state['restore_saved_results'] is True
    assert state['restore_run_filters'] is True
    assert state['mode'] == page_mode


def test_a_stamped_run_with_no_rows_is_restored_not_searched_again():
    """Codex round 5 (P2): a completed search that found nothing ran; Back shows it."""
    state = _back(saved_results_count=0)
    assert state['restore_saved_results'] is True
    assert state['restore_run_filters'] is True


def test_back_does_not_depend_on_the_saved_mode_preference():
    """A search started from a /search?mode= link sets the mode in code, which writes
    no `search_mode`: the run decides, not that preference."""
    assert _back(saved_mode='Title', saved_query='')['restore_saved_results'] is True


def test_a_link_without_the_code_searches_again():
    """A typed or pasted link with the same query and mode is not Back: the run may
    have used a gap, NOT-words, filters or a refinement the URL lacks."""
    for initial_run in (None, ''):
        state = _back(initial_run=initial_run)
        assert state['restore_saved_results'] is False, initial_run
        assert state['restore_saved_filters'] is False, initial_run
        assert state['restore_run_filters'] is False, initial_run
        assert state['restore_saved_exclusions'] is False, initial_run
        assert state['rejoin_run'] is False, initial_run


def test_another_runs_code_searches_again():
    assert _back(initial_run='Zz9_other')['restore_saved_results'] is False


def test_a_saved_run_without_a_code_matches_nothing():
    for code in (None, ''):
        assert _back(saved_run=_run(url_stamp=code), initial_run=code)['restore_saved_results'] is False


def test_a_saved_run_without_its_filters_searches_again():
    """Its rows could only show under the wrong chips: no Back, a fresh search."""
    for missing in (None, [], 'x'):
        state = _back(saved_run=_run(pre_search_filters=missing))
        assert state['restore_saved_results'] is False, missing
        assert state['restore_run_filters'] is False, missing
    run = _run()
    del run['pre_search_filters']
    assert _back(saved_run=run)['restore_saved_results'] is False


def test_another_mode_in_the_url_than_the_runs_searches_again():
    state = _back(initial_query='שלום', initial_mode='variants', saved_run=_run(query='שלום', page_mode='exact'))
    assert state['restore_saved_results'] is False
    assert state['mode'] == 'variants'


def test_responsa_options_other_than_the_runs_search_again():
    for name in ('variants', 'ja', 'flex_spacing', 'bidirectional'):
        url = {'variants': 1, 'ja': None, 'flex_spacing': 1, 'bidirectional': None}
        url[name] = None if url[name] else 1
        state = _back(initial_responsa=url)
        assert state['restore_saved_results'] is False, name
        assert state['mode'] == 'responsa'


def test_another_query_searches_again():
    state = _back(initial_query='מאימתי קורין')
    assert state['restore_saved_results'] is False
    assert state['query'] == 'מאימתי קורין'


def test_a_responsa_run_without_its_options_matches_nothing():
    run = _run()
    run['responsa_options'] = None
    assert _back(saved_run=run)['restore_saved_results'] is False


def test_a_url_from_browse_is_never_back():
    state = _back(from_browse=1)
    assert state['restore_saved_results'] is False
    assert state['restore_saved_filters'] is False
    assert state['restore_run_filters'] is False


def test_back_to_a_search_still_running_rejoins_it():
    """Left before it finished: the page waits for that run, with its filters, and
    restores nothing else (its rows come when it ends)."""
    state = _back(saved_run=None, live_run=_run(), saved_results_count=0)
    assert state == {
        'mode': 'responsa',
        'query': _RESPONSA_QUERY,
        'restore_saved_results': False,
        'restore_saved_filters': False,
        'restore_run_filters': True,
        'restore_saved_exclusions': False,
        'rejoin_run': True,
    }


def test_a_running_search_is_rejoined_only_at_its_own_address():
    assert _back(saved_run=None, live_run=_run(url_stamp='other'))['rejoin_run'] is False
    assert _back(saved_run=None, live_run=_run(), initial_run=None)['rejoin_run'] is False
    assert _back(saved_run=None, live_run=_run(), initial_query='אחר')['rejoin_run'] is False
    assert _back(saved_run=None, live_run=_run(), from_browse=1)['rejoin_run'] is False


def test_a_finished_run_wins_over_rejoining():
    state = _back(live_run=_run())
    assert state['restore_saved_results'] is True and state['rejoin_run'] is False


def test_an_address_without_a_mode_keeps_the_query_only_back():
    """Addresses stamped before runs had codes: Back by query, no filters (829cd7cf)."""
    state = _back(initial_mode=None, initial_query='שלום', saved_query='שלום', initial_run=None,
                  saved_run=None)
    assert state['restore_saved_results'] is True
    assert state['restore_saved_filters'] is False
    assert state['restore_run_filters'] is False


# --- What runs keep, and the page's call sites ---------------------------------

def _search_page_tree():
    import ast
    from pathlib import Path
    source = (Path(__file__).resolve().parents[1] / 'web' / 'pages' / 'search.py').read_text(encoding='utf-8')
    return ast.parse(source)


def _module_tree(relative):
    import ast
    from pathlib import Path
    return ast.parse((Path(__file__).resolve().parents[1] / relative).read_text(encoding='utf-8'))


def _function(tree, name):
    import ast
    found = [node for node in ast.walk(tree)
             if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name]
    assert len(found) == 1, name
    return found[0]


def test_search_page_passes_the_url_and_the_runs_to_the_bootstrap():
    """The four URL flags reach the bootstrap under the names the run uses, with the
    code, the run found and a live run. A renamed key would quietly turn Back into a
    fresh search again."""
    import ast
    from web.search_bootstrap import RESPONSA_URL_OPTIONS

    calls = [node for node in ast.walk(_search_page_tree())
             if isinstance(node, ast.Call) and getattr(node.func, 'id', None) == 'resolve_search_bootstrap']
    assert len(calls) == 1, 'expected one bootstrap call in search.py; update this test'
    keywords = {kw.arg: kw.value for kw in calls[0].keywords}
    assert {'saved_run', 'live_run'} <= set(keywords)
    assert getattr(keywords.get('initial_run'), 'id', None) == 'initial_run'
    passed = keywords['initial_responsa']
    assert isinstance(passed, ast.Dict)
    mapping = {key.value: value.id for key, value in zip(passed.keys, passed.values)}
    assert mapping == {
        'variants': 'initial_variants', 'ja': 'initial_ja',
        'flex_spacing': 'initial_flex_spaces', 'bidirectional': 'initial_bidirectional',
    }
    assert set(mapping) == set(RESPONSA_URL_OPTIONS)


def test_the_run_names_every_stamped_responsa_option():
    """The run's options come from _responsa_options_from_controls; each option the
    URL carries must be one of its keys, or nothing ever matches."""
    import ast
    from web.search_bootstrap import RESPONSA_URL_OPTIONS

    func = _function(_search_page_tree(), '_responsa_options_from_controls')
    returned = [node.value for node in ast.walk(func)
                if isinstance(node, ast.Return) and isinstance(node.value, ast.Dict)]
    assert len(returned) == 1
    assert set(RESPONSA_URL_OPTIONS) <= {key.value for key in returned[0].keys}


def test_a_search_records_its_mode_and_filters_and_stamps_its_address_before_it_runs():
    """execute_search: the run records the page's mode and filters; a new code goes
    into last_run, the run is registered and the address shown with that same code,
    all before the job is awaited -- so a Back while it runs finds it."""
    import ast
    func = _function(_search_page_tree(), 'execute_search')
    recorded = [node for node in ast.walk(func)
                if isinstance(node, ast.Assign) and isinstance(node.value, ast.Dict)
                and any(ast.unparse(t) == 'search_state.last_run' for t in node.targets)
                and any(isinstance(k, ast.Constant) and k.value == 'query' for k in node.value.keys)]
    assert len(recorded) == 1
    values = {k.value: ast.unparse(v) for k, v in zip(recorded[0].value.keys, recorded[0].value.values)
              if isinstance(k, ast.Constant)}
    assert values['page_mode'] == 'mode'
    assert values['pre_search_filters'] == 'run_filter_state(search_state)'

    code = [node for node in ast.walk(func)
            if isinstance(node, ast.Assign) and ast.unparse(node.value) == 'search_runs.new_stamp()']
    assert len(code) == 1
    name = code[0].targets[0].id
    lines = {ast.unparse(n): n.lineno for n in ast.walk(func) if isinstance(n, (ast.Assign, ast.Expr))}
    stamped = lines[f"search_state.last_run = {{**search_state.last_run, 'url_stamp': {name}}}"]
    started = lines[f'search_run = start_search_run(search_state, {name})']
    shown = lines[f'_show_search_url(clean_query, mode, {name})']
    awaited = [n.lineno for n in ast.walk(func) if isinstance(n, ast.Await)
               and ast.unparse(n.value) == 'asyncio.shield(search_run.job)']
    assert len(awaited) == 1
    assert code[0].lineno < stamped < started < awaited[0] and started < shown < awaited[0]


def test_search_page_never_binds_the_name_run():
    """`run` is NiceGUI's run module (run.io_bound) throughout search.py; a local named
    `run` in execute_search broke every search's enrichment (found in the browser,
    2026-10-22)."""
    import ast
    bound = [(f.name, n.lineno) for f in ast.walk(_search_page_tree())
             if isinstance(f, (ast.FunctionDef, ast.AsyncFunctionDef))
             for n in ast.walk(f) if isinstance(n, ast.Name) and n.id == 'run' and isinstance(n.ctx, ast.Store)]
    assert bound == []


def test_the_url_carries_the_mode_and_the_code():
    import ast
    func = _function(_search_page_tree(), '_search_url')
    source = ast.unparse(func)
    assert "params += f'&mode={quote(mode)}'" in source
    assert "params += f'&run={stamp}'" in source


def test_the_page_follows_a_live_run_and_restores_an_empty_stamped_one():
    import ast
    func = _function(_search_page_tree(), 'create_search_page')
    source = ast.unparse(func)
    assert 'asyncio.ensure_future(_after_delay(0, execute_search, _live_run))' in source
    assert "elif restore_saved_results and bootstrap_state['restore_run_filters']:" in source
    assert 'restore_search_snapshot(search_state, initial_run)' in source
    assert 'get_search_active_snapshot(initial_run)' in source


def test_stop_on_a_page_following_a_run_stops_the_run():
    import ast
    func = _function(_search_page_tree(), 'cancel_search')
    assert "_page_run['run'].cancelled = True" in ast.unparse(func)
    progress = _function(_search_page_tree(), 'progress_cb')
    assert 'followed.cancelled' in ast.unparse(progress)


def test_only_the_runs_owner_saves_it():
    """A left page still finishes a run a Back took over; it must not save it twice."""
    import ast
    source = ast.unparse(_function(_search_page_tree(), 'execute_search'))
    assert 'search_run.owner = search_state' in source
    assert '_owns_run = search_run.owner is search_state' in source
    assert 'if _owns_run and (not search_state.refinement_chain):' in source


def test_restoring_a_history_entrys_rows_removes_the_code_from_the_address():
    """Codex round 3: this path shows rows without running a search, so the address
    must lose the code of the search shown before (Back to it would restore that)."""
    import ast
    func = _function(_search_page_tree(), '_on_history_item_clicked')
    guarded = [node for node in ast.walk(func)
               if isinstance(node, ast.If) and ast.unparse(node.test) == "state_snapshot.get('results')"
               and any(isinstance(s, ast.Assign) and ast.unparse(s).strip() == 'search_state.last_run = None'
                       for s in node.body)]
    assert len(guarded) == 1
    assert "u.searchParams.delete('run')" in ast.unparse(guarded[0])


def _display_filter_saves(tree):
    """(saved, followed): persist_value calls for the shared display-filter keys a
    change after the search writes, and how many are followed in their own block by
    save_post_filters_with_run(search_state)."""
    import ast
    keys = {'search_pgp_filter', 'search_library_filter', 'word_search_excluded_ids',
            'search_printed_filter', 'search_exclusion_sources'}
    saved = followed = 0
    for node in ast.walk(tree):
        body = getattr(node, 'body', None)
        if not isinstance(body, list):
            continue
        for i, stmt in enumerate(body):
            call = stmt.value if isinstance(stmt, ast.Expr) else None
            if (isinstance(call, ast.Call) and getattr(call.func, 'id', None) == 'persist_value'
                    and call.args and isinstance(call.args[0], ast.Constant) and call.args[0].value in keys):
                saved += 1
                if any(ast.unparse(later).strip() == 'save_post_filters_with_run(search_state)'
                       for later in body[i + 1:]):
                    followed += 1
    return saved, followed


def test_every_display_filter_save_also_updates_the_run():
    # search.py: PGP, library, printed, and five manuscript-exclusion changes.
    for module, expected in (('web/pages/search.py', 8), ('web/pages/search_results.py', 2)):
        saved, followed = _display_filter_saves(_module_tree(module))
        assert saved == expected, f'{module}: {saved} saves; update this test'
        assert followed == saved, module


def test_the_display_filter_check_can_fail():
    import ast
    tree = ast.parse("def f():\n    persist_value('search_pgp_filter', 1)\n    other()\n")
    assert _display_filter_saves(tree) == (1, 0)


def test_the_route_hands_run_to_the_search_page():
    """`run` must reach create_search_page as initial_run, or no Back ever matches."""
    import ast
    func = _function(_module_tree('web/main.py'), 'search_page_route')
    assert 'run' in [a.arg for a in func.args.args + func.args.kwonlyargs]
    calls = [node for node in ast.walk(func)
             if isinstance(node, ast.Call) and getattr(node.func, 'id', None) == 'create_search_page']
    assert len(calls) == 1
    keywords = {kw.arg: kw.value for kw in calls[0].keywords}
    assert getattr(keywords.get('initial_run'), 'id', None) == 'run'


# --- The run's filters and snapshot (web/pages/search_state.py) -------------------

@pytest.fixture
def runs(monkeypatch):
    """The run registry, empty, for one fake browser."""
    from web import search_runs
    import web.pages.search_state as ss
    search_runs.clear()
    monkeypatch.setattr(ss, '_browser_id', lambda: 'browser-1')
    yield ss
    search_runs.clear()


def test_a_runs_filters_round_trip_as_copies():
    from web.pages.search_state import (
        RUN_FILTER_FIELDS, SearchUIState, apply_run_filter_state, run_filter_state)

    page = SearchUIState()
    for name in RUN_FILTER_FIELDS:
        assert hasattr(page, name), name  # every saved field is a real page field
    page.filter_domains = ['Halakha']
    page.filter_include_mode = False
    page.filter_date_from = 1000
    page.filter_width_min = 12.5
    saved = run_filter_state(page)
    page.filter_domains.append('Liturgy')  # the page changes after the search
    assert saved['filter_domains'] == ['Halakha']

    restored = SearchUIState()
    assert apply_run_filter_state(restored, saved) is True
    assert {name: getattr(restored, name) for name in RUN_FILTER_FIELDS} == saved
    restored.filter_domains.append('Bible')
    assert saved['filter_domains'] == ['Halakha']


def test_applying_a_runs_filters_resets_the_fields_it_lacks():
    from web.pages.search_state import SearchUIState, apply_run_filter_state

    page = SearchUIState()
    page.filter_authors = ['other tab']
    page.filter_include_mode = False
    page.filter_height_max = 30
    assert apply_run_filter_state(page, {'filter_domains': ['Halakha']}) is True
    assert page.filter_domains == ['Halakha']
    assert page.filter_authors == []
    assert page.filter_include_mode is True
    assert page.filter_height_max is None

    page.filter_authors = ['kept']
    assert apply_run_filter_state(page, None) is False
    assert page.filter_authors == ['kept']


def _page_with_a_run(ss, rows=1000 + 5):
    page = ss.SearchUIState()
    page.results = [{'display': {'id': f'99{i:07d}'}} for i in range(rows)]
    page.last_run = _run(query='שבת', page_mode='exact')
    page.pgp_filter = 'only_pgp'
    page.library_mode, page.library_filter = 'show_only', ['CUL', 'JTS']
    page.word_search_excluded_ids = {'990002'}
    page.printed_filter = 'hide_printed'
    page.exclusion_sources = ['a source']
    return page


def test_a_run_keeps_a_thousand_rows_and_its_display_filters(runs):
    """Back after a 25,320-row search showed 250 (the shared copy); a run keeps 1,000."""
    ss = runs
    page = _page_with_a_run(ss)
    ss.persist_search_active_snapshot(page)

    back = ss.SearchUIState()
    back.pgp_filter = 'hide_pgp'                       # another tab's, from shared storage
    back.library_mode, back.library_filter = 'hide', ['BL']
    assert ss.restore_search_active_snapshot(back, _CODE) is True
    assert len(back.results) == 1000 and back.result_count_capped is True
    assert back.pgp_filter == 'only_pgp'
    assert (back.library_mode, back.library_filter) == ('show_only', ['CUL', 'JTS'])
    assert back.word_search_excluded_ids == {'990002'}
    assert back.printed_filter == 'hide_printed'
    assert back.exclusion_sources == ['a source']
    assert back.last_run['url_stamp'] == _CODE


def test_a_run_is_restored_only_for_its_browser_and_code(runs, monkeypatch):
    ss = runs
    ss.persist_search_active_snapshot(_page_with_a_run(ss, rows=3))
    assert ss.get_search_active_snapshot('other-code') == {}
    assert ss.get_search_active_snapshot(None) == {}
    monkeypatch.setattr(ss, '_browser_id', lambda: 'another-browser')
    assert ss.get_search_active_snapshot(_CODE) == {}


def test_a_page_without_a_run_code_saves_no_run(runs):
    ss = runs
    page = _page_with_a_run(ss, rows=3)
    page.last_run = None                      # e.g. rows taken from a history entry
    ss.persist_search_active_snapshot(page)
    from web import search_runs
    assert search_runs._runs == {}


def test_a_display_filter_change_reaches_the_run(runs):
    ss = runs
    page = _page_with_a_run(ss, rows=3)
    ss.save_post_filters_with_run(page)
    assert ss.get_search_active_snapshot(_CODE) == {}   # not run yet: nothing is created

    ss.persist_search_active_snapshot(page)
    page.pgp_filter = 'all'
    page.printed_filter = 'only_printed'
    page.exclusion_sources = []
    ss.save_post_filters_with_run(page)
    saved = ss.get_search_active_snapshot(_CODE)
    assert saved['pgp_filter'] == 'all'
    assert saved['printed_filter'] == 'only_printed'
    assert saved['search_exclusion_sources'] == []
    assert len(saved['results']) == 3                    # the rest is untouched


def test_restored_display_filters_are_checked_like_the_shared_ones(runs):
    ss = runs
    from web import search_runs
    search_runs.finish_run('browser-1', _CODE, {
        'version': ss._SEARCH_RUN_SNAPSHOT_VERSION, 'results': [],
        'pgp_filter': 'everything', 'library_filter': {'mode': 'show_only', 'codes': ['LOCAL', 3]},
        'word_search_excluded_ids': 'not a list',
    })
    page = ss.SearchUIState()
    page.pgp_filter, page.word_search_excluded_ids = 'only_pgp', {'990004'}
    ss.restore_search_active_snapshot(page, _CODE)
    assert page.pgp_filter == 'only_pgp'  # an unknown value leaves the loaded one
    assert (page.library_mode, page.library_filter) == ('hide', [])  # LOCAL dropped; show-only nothing = show all
    assert page.word_search_excluded_ids == {'990004'}


def test_a_live_run_is_found_until_a_page_saves_it(runs, monkeypatch):
    ss = runs
    monkeypatch.setattr(ss, 'safe_user_get', lambda key, default=None: default)
    page = _page_with_a_run(ss, rows=0)
    run = ss.start_search_run(page, _CODE)
    assert ss.live_search_run(_CODE) is run              # preparing: followed too

    class _Job:
        finished = False

        def done(self):
            return self.finished

        def cancelled(self):
            return False

        def exception(self):
            return None
    run.job = _Job()
    assert ss.live_search_run(_CODE) is run
    assert ss.live_search_run('other') is None
    run.job.finished = True
    assert ss.live_search_run(_CODE) is run              # ended, not saved: still followed
    ss.persist_search_active_snapshot(page)              # an incidental save: not the run's
    assert ss.live_search_run(_CODE) is run and not run.done
    ss.save_search_run(page)                             # the search's own save: restored now
    assert ss.live_search_run(_CODE) is None
    assert ss.get_search_active_snapshot(_CODE) != {}


def test_a_rejoining_page_takes_the_runs_starting_state(runs):
    ss = runs
    page = _page_with_a_run(ss, rows=0)
    run = ss.start_search_run(page, _CODE)
    back = ss.SearchUIState()
    ss.apply_run_start_snapshot(back, run.snapshot)
    assert back.results == []
    assert back.pgp_filter == 'only_pgp' and back.printed_filter == 'hide_printed'
    assert back.exclusion_sources == ['a source']
    assert back.last_run['url_stamp'] == _CODE and back.last_run['page_mode'] == 'exact'


def test_a_finished_run_no_page_saved_is_followed_not_searched_again(runs):
    """Codex round 7 (P1): the job ended, the page that started it has not saved it
    (or died on its deleted elements): Back takes its result instead of searching."""
    ss = runs
    run = ss.start_search_run(_page_with_a_run(ss, rows=0), _CODE)

    class _Ended:
        def done(self):
            return True

        def cancelled(self):
            return False

        def exception(self):
            return None
    run.job = _Ended()
    assert ss.live_search_run(_CODE) is run
    run.cancelled = True                     # stopped: its rows are not the list
    assert ss.live_search_run(_CODE) is None


def test_a_downgraded_run_with_no_rows_keeps_its_warning(runs):
    """Codex round 7 (P2): with no rows, no first row carries the Responsa downgrade."""
    ss = runs
    page = _page_with_a_run(ss, rows=0)
    page.responsa_warning = 'Spelling variants disabled'
    ss.persist_search_active_snapshot(page)
    back = ss.SearchUIState()
    assert ss.restore_search_active_snapshot(back, _CODE)
    assert back.results == [] and back.responsa_warning == 'Spelling variants disabled'


def test_a_run_without_a_saved_warning_takes_its_first_rows():
    from web.pages.search_state import SearchUIState, restore_search_active_snapshot
    from web import search_runs
    import web.pages.search_state as ss
    search_runs.clear()
    rows = [{'display': {'id': 'r'}, 'responsa_warning': 'from the row'}]
    search_runs.finish_run('browser-1', _CODE, {'version': ss._SEARCH_RUN_SNAPSHOT_VERSION, 'results': rows})
    try:
        from unittest.mock import patch
        with patch.object(ss, '_browser_id', lambda: 'browser-1'):
            page = SearchUIState()
            assert restore_search_active_snapshot(page, _CODE)
        assert page.responsa_warning == 'from the row'
    finally:
        search_runs.clear()


def test_a_page_back_on_a_run_shows_that_runs_controls():
    """Codex round 7 (P2): gap, NOT-words, position and variant level are not in the
    address; restored or followed, the page shows the run's, not the shared ones."""
    import ast
    func = _function(_search_page_tree(), 'create_search_page')
    guarded = [node for node in func.body
               if isinstance(node, ast.If) and ast.unparse(node.test) == "bootstrap_state['restore_run_filters']"
               and any(ast.unparse(s).strip() == '_show_run_controls(search_state.last_run)' for s in node.body)]
    assert len(guarded) == 1
    shown = ast.unparse(_function(_search_page_tree(), '_show_run_controls'))
    for control in ('gap_input.value', 'not_filter.value', 'text_position_select.value',
                    'variant_slider.value', 'max_changes_select.value', "current_preset['value']"):
        assert control in shown, control
    source = ast.unparse(func)
    assert ("results_count.text = f\"0 {tr('Results')}{_downgrade_suffix()}\"" in source
            or "results_count.text = f'0 {tr('Results')}{_downgrade_suffix()}'" in source)


def test_the_page_restore_keeps_a_zero_row_runs_warning(runs):
    """Through restore_search_snapshot (what the page calls), not only the helper."""
    from unittest.mock import patch
    ss = runs
    page = _page_with_a_run(ss, rows=0)
    page.responsa_warning = 'Spelling variants disabled'
    ss.persist_search_active_snapshot(page)
    with patch.object(ss, 'safe_user_get', lambda key, default=None: {'search_snapshot_schema_version': 1}.get(key, default)), \
            patch.object(ss, 'safe_user_set', lambda key, value: True):
        back = ss.SearchUIState()
        ss.restore_search_snapshot(back, _CODE)
    assert back.results == [] and back.responsa_warning == 'Spelling variants disabled'


def test_a_save_while_the_search_runs_does_not_finish_its_run(runs):
    """A filter applied mid-search saves the page: that must not mark the run done
    with no rows (it would also let go of its owner, and the end would not save)."""
    ss = runs
    page = _page_with_a_run(ss, rows=0)
    run = ss.start_search_run(page, _CODE)

    class _Running:
        def done(self):
            return False
    run.job = _Running()
    ss.persist_search_active_snapshot(page)
    assert not run.done and ss.live_search_run(_CODE) is run


def test_a_run_records_lab_mode_and_the_page_shows_it():
    """Codex round 8 (P2): Lab Mode and Deep Scan are not in the address."""
    import ast
    func = _function(_search_page_tree(), 'execute_search')
    recorded = [node for node in ast.walk(func)
                if isinstance(node, ast.Assign) and isinstance(node.value, ast.Dict)
                and any(ast.unparse(t) == 'search_state.last_run' for t in node.targets)
                and any(isinstance(k, ast.Constant) and k.value == 'query' for k in node.value.keys)]
    values = {k.value: ast.unparse(v) for k, v in zip(recorded[0].value.keys, recorded[0].value.values)
              if isinstance(k, ast.Constant)}
    assert values['lab_mode'] == 'bool(lab_mode.value)'
    assert values['deep_scan'] == 'bool(deep_scan.value)'
    shown = ast.unparse(_function(_search_page_tree(), '_show_run_controls'))
    assert "lab_mode.value = bool(run.get('lab_mode'))" in shown
    assert "deep_scan.value = bool(run.get('deep_scan'))" in shown


def test_an_incidental_save_never_finishes_an_unfinished_run(runs, monkeypatch):
    """Codex round 9 (P2): a filter dialog applied after the job ended but before the
    search put its rows on the page would save the old rows as the run's, and the
    search's own save would then be skipped (the owner is let go of on save)."""
    ss = runs
    monkeypatch.setattr(ss, 'safe_user_get', lambda key, default=None: default)
    page = _page_with_a_run(ss, rows=0)
    run = ss.start_search_run(page, _CODE)

    class _Ended:
        def done(self):
            return True

        def cancelled(self):
            return False

        def exception(self):
            return None
    run.job, run.owner = _Ended(), page
    ss.persist_search_active_snapshot(page)
    assert not run.done and run.owner is page
    page.results = [{'display': {'id': '990001'}}]
    ss.save_search_run(page)
    assert run.done and ss.get_search_active_snapshot(_CODE)['results'] == [{'display': {'id': '990001'}}]


def test_the_search_own_save_respects_session_persistence(runs, monkeypatch):
    """Persistence off: nothing kept -- and the run is forgotten, not left unfinished
    holding its job's rows and its page for 30 minutes (Codex, PR #436, round 10)."""
    from web import search_runs
    ss = runs
    monkeypatch.setattr(ss, 'safe_user_get', lambda key, default=None: False
                        if key == 'session_persistence_enabled' else default)
    page = _page_with_a_run(ss, rows=1)
    ss.start_search_run(page, _CODE)
    ss.save_search_run(page)
    assert ss.get_search_active_snapshot(_CODE) == {}
    assert search_runs.get_run('browser-1', _CODE) is None


def test_the_run_is_registered_before_the_search_first_awaits():
    """Codex round 9 (P1): leaving while the search prepares (the phone fold, the
    filter lookup) must leave an address Back can follow."""
    import ast
    func = _function(_search_page_tree(), 'execute_search')
    first_await = min(n.lineno for n in ast.walk(func) if isinstance(n, ast.Await))
    started = [n.lineno for n in ast.walk(func) if isinstance(n, ast.Assign)
               and ast.unparse(n).startswith('search_run = start_search_run(search_state, ')]
    shown = [n.lineno for n in ast.walk(func) if isinstance(n, ast.Expr)
             and ast.unparse(n).startswith('_show_search_url(clean_query, mode, ')]
    assert len(started) == 1 and started[0] < first_await
    assert shown and min(shown) < first_await


def test_the_search_end_paths_save_its_run():
    import ast
    source = ast.unparse(_function(_search_page_tree(), 'execute_search'))
    assert source.count('save_search_run(search_state)') == 2   # completed, and stopped


def test_search_within_and_visual_similarity_runs_are_scoped(runs):
    """Codex round 10 (P1 x2): neither scope is in the address, so such a run is never
    followed (a Back page would search without it)."""
    ss = runs
    within = _page_with_a_run(ss, rows=0)
    within._refine_mode = True
    assert ss.start_search_run(within, 'w').scoped is True
    similar = _page_with_a_run(ss, rows=0)
    similar.vs_restrict_sys_ids = {'990001'}
    assert ss.start_search_run(similar, 'v').scoped is True
    plain = _page_with_a_run(ss, rows=0)
    run = ss.start_search_run(plain, _CODE)
    assert run.scoped is False and ss.live_search_run(_CODE) is run
    assert ss.live_search_run('w') is None and ss.live_search_run('v') is None


def test_a_scoped_run_stamps_its_address_only_when_saved_and_early_ends_forget_runs():
    import ast
    func = _function(_search_page_tree(), 'execute_search')
    guarded = [node for node in ast.walk(func)
               if isinstance(node, ast.If) and ast.unparse(node.test) == 'not search_run.scoped']
    assert len(guarded) == 2   # at the start, and the end's re-stamp
    assert all(any(ast.unparse(s).startswith('_show_search_url(clean_query, mode, ') for s in g.body)
               for g in guarded)
    # Codex round 11: a scoped run's address is stamped only once its save succeeded,
    # completed or stopped -- never for a run that is not kept (persistence off).
    kept = [node for node in ast.walk(func)
            if isinstance(node, ast.If)
            and ast.unparse(node.test) == 'save_search_run(search_state) and search_run.scoped']
    assert len(kept) == 2
    assert all(any(ast.unparse(s).startswith('_show_search_url(clean_query, mode, url_stamp_of(search_state))')
                   for s in k.body) for k in kept)
    source = ast.unparse(func)
    assert source.count('discard_search_run(search_state)') == 3   # stopped before running, no match, error
    stop = ast.unparse(_function(_search_page_tree(), '_stop_before_running'))
    assert 'discard_search_run(search_state)' in stop


def test_save_search_run_says_whether_it_kept_the_run(runs, monkeypatch):
    ss = runs
    page = _page_with_a_run(ss, rows=1)
    monkeypatch.setattr(ss, 'safe_user_get', lambda key, default=None: default)
    assert ss.save_search_run(page) is True
    monkeypatch.setattr(ss, 'safe_user_get', lambda key, default=None: False
                        if key == 'session_persistence_enabled' else default)
    assert ss.save_search_run(page) is False
    page.last_run = None
    monkeypatch.setattr(ss, 'safe_user_get', lambda key, default=None: default)
    assert ss.save_search_run(page) is False


def test_a_page_following_a_run_keeps_its_early_rows():
    """Codex round 13: the early rows lived only in the starting page's call; a Stop
    or the time limit on a page that followed the run after Back showed none."""
    import ast
    func = _function(_search_page_tree(), 'execute_search')
    preview = ast.unparse(_function(_search_page_tree(), 'preview_cb'))
    assert 'search_run.preview_rows = rows' in preview
    followed = [node for node in ast.walk(func)
                if isinstance(node, ast.If) and ast.unparse(node.test) == 'attach is not None'
                and any("_run_cutoff.update(search_run.cutoff or {})" in ast.unparse(s) for s in node.body)]
    assert len(followed) == 1
    assert "_preview_box['rows'] = list(search_run.preview_rows)" in ast.unparse(followed[0])


def test_a_stopped_run_is_stopped_on_every_page_following_it():
    """Codex round 14: Stop in one tab stops the shared job; another tab owning the run
    must take the partial-results path too (its own flag is not set)."""
    import ast
    func = _function(_search_page_tree(), 'execute_search')
    stopped = [node for node in ast.walk(func)
               if isinstance(node, ast.If) and ast.unparse(node.test) == 'search_run.cancelled']
    assert len(stopped) == 1
    body = ast.unparse(stopped[0])
    assert 'search_state.is_cancelled = True' in body
    assert "_preview_box['rows'] = list(search_run.preview_rows)" in body
    awaited = [n.lineno for n in ast.walk(func) if isinstance(n, ast.Await)
               and ast.unparse(n.value) == 'asyncio.shield(search_run.job)']
    partial = [n.lineno for n in ast.walk(func) if isinstance(n, ast.If)
               and ast.unparse(n.test) == 'search_state.is_cancelled or _forced_stop']
    assert awaited[0] < stopped[0].lineno < partial[0]


def test_new_search_forgets_the_run_and_its_address():
    """Codex round 15 (P2): after New Search, a refresh or a Back with the old run code
    restored the cleared results for up to 30 minutes."""
    import ast
    source = ast.unparse(_function(_search_page_tree(), '_reset_search'))
    discard = source.index('discard_search_run(search_state)')
    cleared = source.index('search_state.last_run = None')
    unstamped = source.index("ui.run_javascript(\"history.replaceState(null, '', '/search')\")")
    assert discard < cleared < unstamped < source.index('clear_search_snapshot()')


def test_a_stopped_search_saves_its_query_with_its_rows():
    """The stopped path saves its rows to the shared copy; the query-only Back pairs
    them with `search_query`, so the query goes with them (found in the browser:
    a stopped search's rows came back under an older query)."""
    import ast
    func = _function(_search_page_tree(), 'execute_search')
    stopped = [node for node in ast.walk(func)
               if isinstance(node, ast.If) and ast.unparse(node.test) == 'search_run.owner is search_state']
    assert len(stopped) == 1
    body = ast.unparse(stopped[0])
    assert body.index('persist_search_snapshot(search_state)') < body.index("_safe_set('search_query', clean_query)")
