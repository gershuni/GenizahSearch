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
