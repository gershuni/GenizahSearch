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


# --- Back to a Responsa search (2026-10-22) ---------------------------------
# A finished Responsa search stamps q=..&mode=responsa plus its option flags into
# the URL, so its Back carries a mode. It was treated as a fresh link and the whole
# search ran again (production 2026-10-22: one 45 s search ran three times). It now
# also stamps a random run=<stamp>, kept in the saved run, and Back is that stamp
# with the same query and options (Codex, PR #436: equal values alone would let a
# typed link or another tab's snapshot pass).

_RESPONSA_QUERY = '(מאמתי/מאימתי) (קורין/קוריין)'
_STAMP = 'k3J9xQ_a'
_RUN_FILTERS = {'filter_domains': ['Halakha'], 'filter_include_mode': True, 'filter_date_from': 1000}
_ABSENT = object()


def _responsa_run(query=_RESPONSA_QUERY, mode='responsa', url_stamp=_STAMP,
                  pre_search_filters=_ABSENT, **options):
    run = {
        'query': query, 'mode': mode, 'gap': 0, 'exclude_words': [], 'text_position': None,
        'responsa_options': {
            'responsa_mode': True,
            'variants': options.get('variants', True),
            'ja': options.get('ja', False),
            'flex_spacing': options.get('flex_spacing', True),
            'bidirectional': options.get('bidirectional', False),
            'variant_mode': 'variants' if options.get('variants', True) else 'exact',
        },
        'corpus_scope': 'genizah', 'variant_settings': {},
        'pre_search_filters': dict(_RUN_FILTERS) if pre_search_filters is _ABSENT else pre_search_filters,
    }
    if url_stamp is not None:
        run['url_stamp'] = url_stamp
    return run


def _responsa_back(*, initial_query=_RESPONSA_QUERY, initial_responsa=None, initial_run=_STAMP,
                   saved_run=None, saved_results_count=81, saved_mode='exact',
                   saved_query=_RESPONSA_QUERY, from_browse=None):
    # The URL search.py stamps for variants + flexible spacing:
    # &variants=1&flex_spaces=1&run=<stamp> (the route's ints, by the engine's option names).
    if initial_responsa is None:
        initial_responsa = {'variants': 1, 'ja': None, 'flex_spacing': 1, 'bidirectional': None}
    return resolve_search_bootstrap(
        initial_query=initial_query,
        initial_tag=None,
        initial_mode='responsa',
        initial_domain=None,
        from_browse=from_browse,
        saved_mode=saved_mode,
        saved_query=saved_query,
        saved_results_count=saved_results_count,
        use_slider=False,
        initial_responsa=initial_responsa,
        initial_run=initial_run,
        saved_run=_responsa_run() if saved_run is None else saved_run,
    )


def test_back_to_a_responsa_search_restores_its_results_and_its_runs_filters():
    """The stamp ties the snapshot to this address, so the filters saved WITH its run
    come back with its rows (Codex, PR #436, rounds 1-2): no restricted result under
    empty filter chips, nor under the shared current ones another tab may have set."""
    state = _responsa_back()

    assert state == {
        'mode': 'responsa',
        'query': _RESPONSA_QUERY,
        'restore_saved_results': True,
        'restore_saved_filters': False,
        'restore_run_filters': True,
        'restore_saved_exclusions': True,
    }


def test_responsa_back_without_the_runs_filters_searches_again():
    """A saved run without its filters (stamped before they were saved with it) could
    only show its rows under the wrong chips: no Back, a fresh search."""
    for missing in (None, [], 'x'):
        state = _responsa_back(saved_run=_responsa_run(pre_search_filters=missing))
        assert state['restore_saved_results'] is False, missing
        assert state['restore_run_filters'] is False, missing
    run = _responsa_run()
    del run['pre_search_filters']
    assert _responsa_back(saved_run=run)['restore_saved_results'] is False


def test_responsa_link_without_the_stamp_searches_again():
    """A typed or pasted link with the same query and options is not Back: the saved
    run may have used a gap, NOT-words, filters or a refinement the URL lacks."""
    for initial_run in (None, ''):
        state = _responsa_back(initial_run=initial_run)
        assert state['restore_saved_results'] is False, initial_run
        assert state['restore_saved_filters'] is False, initial_run
        assert state['restore_run_filters'] is False, initial_run
        assert state['restore_saved_exclusions'] is False, initial_run


def test_responsa_link_with_another_runs_stamp_searches_again():
    """Another tab's (or an older) run in the snapshot: its stamp is not this URL's."""
    assert _responsa_back(initial_run='Zz9_other')['restore_saved_results'] is False


def test_responsa_link_when_the_saved_run_has_no_stamp_searches_again():
    """A run saved before stamps (or a run that never finished) matches no URL."""
    for stamp in (None, ''):
        state = _responsa_back(saved_run=_responsa_run(url_stamp=stamp), initial_run=stamp)
        assert state['restore_saved_results'] is False, stamp


def test_responsa_back_does_not_depend_on_the_saved_mode_preference():
    """A Responsa search started from a /search?mode=responsa link sets the mode in
    code, which writes no `search_mode`: the saved run decides, not that preference."""
    assert _responsa_back(saved_mode='Title', saved_query='')['restore_saved_results'] is True


def test_responsa_link_with_other_options_than_the_saved_run_searches_again():
    for name in ('variants', 'ja', 'flex_spacing', 'bidirectional'):
        url = {'variants': 1, 'ja': None, 'flex_spacing': 1, 'bidirectional': None}
        url[name] = None if url[name] else 1
        state = _responsa_back(initial_responsa=url)
        assert state['restore_saved_results'] is False, name
        assert state['restore_saved_exclusions'] is False, name
        assert state['mode'] == 'responsa'


def test_responsa_link_with_another_query_searches_again():
    state = _responsa_back(initial_query='מאימתי קורין')

    assert state['restore_saved_results'] is False
    assert state['query'] == 'מאימתי קורין'


def test_responsa_link_when_the_saved_run_was_not_responsa_searches_again():
    state = _responsa_back(saved_run=_responsa_run(mode='literal'))

    assert state['restore_saved_results'] is False


def test_responsa_link_without_a_saved_run_searches_again():
    """A snapshot saved before searches were saved with their run has nothing to
    match the options against: a fresh search, as before."""
    for saved_run in ({}, {'query': _RESPONSA_QUERY, 'mode': 'responsa', 'url_stamp': _STAMP,
                           'pre_search_filters': {}},
                      {'query': _RESPONSA_QUERY, 'mode': 'responsa', 'url_stamp': _STAMP,
                       'pre_search_filters': {}, 'responsa_options': None}):
        assert _responsa_back(saved_run=saved_run)['restore_saved_results'] is False, saved_run


def test_responsa_link_with_no_saved_results_searches_again():
    assert _responsa_back(saved_results_count=0)['restore_saved_results'] is False


def test_responsa_link_from_browse_is_never_back():
    state = _responsa_back(from_browse=1)

    assert state['restore_saved_results'] is False
    assert state['restore_saved_filters'] is False
    assert state['restore_run_filters'] is False


def test_other_explicit_modes_in_the_url_still_search_again():
    """Only Responsa stamps a mode; any other mode in the URL came from a link."""
    state = resolve_search_bootstrap(
        initial_query='שלום',
        initial_tag=None,
        initial_mode='variants',
        initial_domain=None,
        from_browse=None,
        saved_mode='variants',
        saved_query='שלום',
        saved_results_count=12,
        use_slider=False,
        saved_run={'query': 'שלום', 'mode': 'variants', 'responsa_options': None},
    )

    assert state['restore_saved_results'] is False
    assert state['mode'] == 'variants'


def _search_page_tree():
    import ast
    from pathlib import Path
    source = (Path(__file__).resolve().parents[1] / 'web' / 'pages' / 'search.py').read_text(encoding='utf-8')
    return ast.parse(source)


def test_search_page_passes_the_stamped_responsa_options_and_the_saved_run():
    """The call site: the four URL flags reach the bootstrap under the names the
    saved run uses, and the saved run is passed. A renamed key would quietly turn
    Back to Responsa into a fresh search again."""
    import ast
    from web.search_bootstrap import RESPONSA_URL_OPTIONS

    calls = [node for node in ast.walk(_search_page_tree())
             if isinstance(node, ast.Call) and getattr(node.func, 'id', None) == 'resolve_search_bootstrap']
    assert len(calls) == 1, 'expected one bootstrap call in search.py; update this test'
    keywords = {kw.arg: kw.value for kw in calls[0].keywords}
    assert 'saved_run' in keywords
    assert getattr(keywords.get('initial_run'), 'id', None) == 'initial_run'
    passed = keywords['initial_responsa']
    assert isinstance(passed, ast.Dict)
    mapping = {key.value: value.id for key, value in zip(passed.keys, passed.values)}
    assert mapping == {
        'variants': 'initial_variants', 'ja': 'initial_ja',
        'flex_spacing': 'initial_flex_spaces', 'bidirectional': 'initial_bidirectional',
    }
    assert set(mapping) == set(RESPONSA_URL_OPTIONS)


def test_the_saved_run_names_every_stamped_responsa_option():
    """The saved run's options come from _responsa_options_from_controls; each
    option the URL carries must be one of its keys, or nothing ever matches."""
    import ast
    from web.search_bootstrap import RESPONSA_URL_OPTIONS

    funcs = [node for node in ast.walk(_search_page_tree())
             if isinstance(node, ast.FunctionDef) and node.name == '_responsa_options_from_controls']
    assert len(funcs) == 1
    returned = [node.value for node in ast.walk(funcs[0])
                if isinstance(node, ast.Return) and isinstance(node.value, ast.Dict)]
    assert len(returned) == 1
    keys = {key.value for key in returned[0].keys}
    assert set(RESPONSA_URL_OPTIONS) <= keys


def _stamp_names(tree):
    """Names X with `search_state.last_run = {..., 'url_stamp': X}` and an f-string
    `params += f'..&run={X}'` in the same function: the URL and the saved run get
    the same stamp."""
    import ast
    found = set()
    for func in ast.walk(tree):
        if not isinstance(func, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        saved, stamped = set(), set()
        for node in ast.walk(func):
            if (isinstance(node, ast.Assign) and isinstance(node.value, ast.Dict)
                    and any(ast.unparse(t) == 'search_state.last_run' for t in node.targets)):
                saved |= {v.id for k, v in zip(node.value.keys, node.value.values)
                          if isinstance(k, ast.Constant) and k.value == 'url_stamp'
                          and isinstance(v, ast.Name)}
            if (isinstance(node, ast.AugAssign) and getattr(node.target, 'id', None) == 'params'
                    and isinstance(node.value, ast.JoinedStr)):
                parts = node.value.values
                for i, part in enumerate(parts[:-1]):
                    nxt = parts[i + 1]
                    if (isinstance(part, ast.Constant) and str(part.value).endswith('&run=')
                            and isinstance(nxt, ast.FormattedValue) and isinstance(nxt.value, ast.Name)):
                        stamped.add(nxt.value.id)
        found |= saved & stamped
    return found


def test_the_url_and_the_saved_run_get_the_same_stamp():
    assert _stamp_names(_search_page_tree()), 'no run stamp found in search.py; update this test'


def test_the_stamp_check_can_fail():
    import ast
    one = ast.parse("def f():\n    s = 1\n    search_state.last_run = {'url_stamp': s}\n    params += f'&run={s}'\n")
    two = ast.parse("def f():\n    s = 1; t = 2\n    search_state.last_run = {'url_stamp': s}\n    params += f'&run={t}'\n")
    assert _stamp_names(one) == {'s'}
    assert _stamp_names(two) == set()


def test_the_route_hands_run_to_the_search_page():
    """`run` must reach create_search_page as initial_run, or no Back ever matches."""
    import ast
    from pathlib import Path
    tree = ast.parse((Path(__file__).resolve().parents[1] / 'web' / 'main.py').read_text(encoding='utf-8'))
    routes = [node for node in ast.walk(tree)
              if isinstance(node, ast.FunctionDef) and node.name == 'search_page_route']
    assert len(routes) == 1
    assert 'run' in [a.arg for a in routes[0].args.args + routes[0].args.kwonlyargs]
    calls = [node for node in ast.walk(routes[0])
             if isinstance(node, ast.Call) and getattr(node.func, 'id', None) == 'create_search_page']
    assert len(calls) == 1
    keywords = {kw.arg: kw.value for kw in calls[0].keywords}
    assert getattr(keywords.get('initial_run'), 'id', None) == 'run'


# --- The run's own filters, and the stamp dropped at a new search (Codex round 2) ---

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
    """Another tab's filters on this page are replaced field by field, absent ones
    with their defaults; a non-dict changes nothing."""
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


def test_a_new_search_drops_the_saved_stamp(monkeypatch):
    """Only a completed search stamps again: a stopped or failed one must leave no
    saved run that the previous address still matches (Codex, PR #436, round 2)."""
    import web.pages.search_state as ss

    run = {'query': 'q', 'mode': 'responsa', 'url_stamp': _STAMP, 'pre_search_filters': {}}
    tab = {ss._SEARCH_ACTIVE_TAB_KEY: {'version': ss._SEARCH_ACTIVE_TAB_VERSION, 'results': [1], 'last_run': run}}
    user = {'search_last_run': dict(run)}
    monkeypatch.setattr(ss, '_get_tab_storage', lambda: tab)
    monkeypatch.setattr(ss, 'safe_user_get', lambda key, default=None: user.get(key, default))
    monkeypatch.setattr(ss, 'safe_user_set', lambda key, value: user.__setitem__(key, value))

    ss.drop_saved_url_stamp()

    assert 'url_stamp' not in tab[ss._SEARCH_ACTIVE_TAB_KEY]['last_run']
    assert 'url_stamp' not in user['search_last_run']
    assert tab[ss._SEARCH_ACTIVE_TAB_KEY]['results'] == [1]  # nothing else touched
    assert user['search_last_run']['query'] == 'q'
    assert run['url_stamp'] == _STAMP  # copied, not edited in place
    # The saved run no longer matches the address it stamped.
    assert _responsa_back(saved_run=tab[ss._SEARCH_ACTIVE_TAB_KEY]['last_run'])['restore_saved_results'] is False


def _function(tree, name):
    import ast
    found = [node for node in ast.walk(tree)
             if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name]
    assert len(found) == 1, name
    return found[0]


def test_a_search_saves_its_filters_and_drops_the_old_stamp_before_its_run():
    """Call sites in execute_search: the stamp is dropped before the new run is
    recorded, and the run records the page's filters."""
    import ast
    func = _function(_search_page_tree(), 'execute_search')
    drops = [node.lineno for node in ast.walk(func)
             if isinstance(node, ast.Call) and getattr(node.func, 'id', None) == 'drop_saved_url_stamp']
    runs = [node for node in ast.walk(func)  # the run as recorded (not the stamp's update)
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Dict)
            and any(ast.unparse(t) == 'search_state.last_run' for t in node.targets)
            and any(isinstance(k, ast.Constant) and k.value == 'query' for k in node.value.keys)]
    assert len(drops) == 1 and len(runs) == 1
    assert drops[0] < runs[0].lineno
    values = {k.value: v for k, v in zip(runs[0].value.keys, runs[0].value.values) if isinstance(k, ast.Constant)}
    assert ast.unparse(values['pre_search_filters']) == 'run_filter_state(search_state)'


def test_the_page_applies_the_runs_filters_on_a_stamped_back():
    import ast
    func = _function(_search_page_tree(), 'create_search_page')
    guarded = [node for node in ast.walk(func)
               if isinstance(node, ast.If) and ast.unparse(node.test) == "bootstrap_state['restore_run_filters']"]
    assert len(guarded) == 1
    calls = [ast.unparse(node) for node in ast.walk(guarded[0])
             if isinstance(node, ast.Call) and getattr(node.func, 'id', None) == 'apply_run_filter_state']
    assert calls == ["apply_run_filter_state(search_state, _saved_run.get('pre_search_filters'))"]
