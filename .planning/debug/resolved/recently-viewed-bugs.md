---
slug: recently-viewed-bugs
status: resolved
trigger: "Fix the Recently Viewed: 1. In web it says it in English in Heb UI also, and it's empty (both in main lists and in Join-labs picker) - though it's not empty in add-to-joins picker 'recent activity' tab. 2. In desktop it exists but the order is by libraries and not by recently-viewed order, and also it's duplicated when the same item was viewed multiple times (makes sense when one of them is specific image, but it's also real duplicates)."
created: 2026-06-23
updated: 2026-06-23
---

# Debug Session: recently-viewed-bugs

## Symptoms

### WEB (genizahsearch.com)
- **W1 — Untranslated label:** The "Recently Viewed" system list shows the English string
  "Recently Viewed" even when the UI language is Hebrew. (subtitle "רשימת מערכת"/"מערכת" IS
  translated, so only the list NAME leaks English). Appears in both the lists page header and
  the sidebar list entry.
- **W2 — Empty list:** The "Recently Viewed" list renders empty ("הרשימה הזו ריקה") in BOTH the
  main Lists page AND the Join-Labs candidate picker — even though items were recently viewed.
- **W2 contrast clue:** The SAME recent items DO appear, non-empty, in the "Add to Joins" picker's
  "פעילות אחרונה" / "Recent activity" tab (shows Ms. Add. 3207, T-S Ar.42.192, ENA 3065.8, ...).
  So the underlying recently-viewed data EXISTS; the Recently-Viewed *list* surface is what's empty.
  → strong signal: two different data sources / code paths for "recent" on web. One works, the
    system-list one does not (and is mislabeled).

### DESKTOP (Dicta Genizah Search Pro / PyQt6)
- **D1 — Wrong order:** The "נצפו לאחרונה" (Recently Viewed, system list, 50 items) is ordered
  by LIBRARY (alphabetical/grouped: JTS, HUC, CUL, Manchester, RNL, Toronto, Geneva...) instead of
  by recency (most-recently-viewed first).
- **D2 — Duplicates:** The same manuscript appears multiple times. Some duplication is legitimate
  (different specific images/folios of the same shelfmark), but there are ALSO true duplicates
  (identical rows, e.g. Ms. Add. 3207 ×4, Ms. B 2705 ×4, Ms. FR 9-002.2 ×4 — same title, same
  shelfmark, same library). Real dupes should be collapsed (keep most-recent); per-image variants
  may be intentional but ordering should still be by recency.

### ROUND 2 — follow-up defects from live human verification (2026-06-23)
- **W3 — Count badge shows (0):** On web, the count badge next to "Recently Viewed" reads **(0)**
  even though the list is full of items. The round-1 W2 fix made the list CONTENTS load via
  get_recent_items, but the sidebar/header COUNT badge still reads the stale source (likely the
  cached user_lists.item_count, which counts the empty list_items table = 0). The count must also
  route through the recent_items source (mirror the _is_recent_list routing into the count path,
  and/or the cached list data's item_count for the recent system list).
- **W4 — "General" list name untranslated:** On web under Hebrew UI, the **"General"** system list
  still displays in English; it should read **"כללי"** as the desktop already does. Same class as
  W1 (system-list name not wrapped in tr(), OR the 'General'->'כללי' key missing from the shared
  TRANSLATIONS dict). Verify the key exists and that ALL system-list name render sites (sidebar,
  lists-page header/inline-edit label, joins_lab picker) go through tr() — W1's fix may have only
  covered 'Recently Viewed' or missed a render site that General also passes through.

## Notes / leads to investigate
- Web: find the "Recently Viewed" system list implementation — how it is populated/named, vs the
  "Recent activity" tab in the add-to-joins picker (which works). Why is the system list empty +
  mislabeled? Likely a missing tr() key for the list name + a population/read path that differs
  from the add-to-joins "recent activity" source.
- Desktop: find where the Recently Viewed system list is built — the ORDER BY / sort and any
  de-dup. Likely ordered by a library/shelfmark sort key rather than a viewed_at timestamp, and
  no de-dup on (sys_id) ignoring image/folio.

## Current Focus

hypothesis: ROUND 2 — W3 + W4 fixed and self-verified (16/16 tests pass, ruff clean, imports OK).
test: targeted unit tests + read-back verification against the original symptom code paths
expecting: recent badge count == len(recent_items) at every render site; system AND default list
  names localized at every render site
next_action: AWAITING HUMAN VERIFY — live, authenticated /lists sidebar+header, Join-Labs picker, and
  add-to-list dialog under a Hebrew UI (cannot be exercised headless). On confirmation: archive session.

reasoning_checkpoint_round2:
  hypothesis: |
    W3: the sidebar count badge takes the WARM batched path. _resolve_list_item_count
        (web/components/project_tree.py:47-56) only routes through the recent-aware
        _get_list_item_count when counts IS None; when a batched counts dict is present it returns
        counts.get(int(list_id), 0). The batched RPC get_list_item_counts_for_user counts list_items
        rows; the recent list has 0 list_items rows (its data lives in recent_items) so it is ABSENT
        from the dict -> counts.get(...,0) == 0 -> "(0)". Same stale-counts read at the joins_lab
        picker (joins_lab.py:2937 pdata['counts'].get(list_id,0) and :2006 counts.get(list_id,0)).
    W4: 'General' is the DEFAULT list (is_default=true, is_system=FALSE — supabase_setup.sql:380-382
        + _get_default_data line 108), NOT a system list. Every web render site wraps tr() ONLY when
        is_system (project_tree.py:358, joins_lab.py:2934, the add-candidates picker :2005 wraps
        nothing). So 'General' never gets tr()'d. Desktop is correct: _get_list_display_name
        (genizah_app.py:13165) translates when is_system OR is_default.
  confirming_evidence:
    - "web/components/project_tree.py:47-56 — counts dict path returns counts.get(int(list_id),0), bypassing _get_list_item_count's recent routing."
    - "web/supabase_client.py:952-961 — RPC counts list_items rows; recent list has none -> absent from dict -> 0."
    - "supabase_setup.sql:381-382 — General created is_default=true; is_system defaults FALSE (:54)."
    - "web/components/project_tree.py:358 + joins_lab.py:2934 — tr() gated on is_system only; General (is_default) skips it. joins_lab.py:2005 add-candidates picker has no tr() at all."
    - "genizah_app.py:13165 — desktop translates on is_system OR is_default (the generic-correct rule)."
  falsification_test: |
    W3: if the count routed through recent_items it would show the real count, not (0) — it shows (0).
    W4: if General were a system list, the existing is_system tr() would catch it — it does not, so it must be is_default.
  fix_rationale: |
    W3: in _resolve_list_item_count, route the recent list through lists_mgr._get_list_item_count
        REGARDLESS of whether a batched counts dict is present (the batched RPC can never count
        recent_items). At joins_lab sites, override item_count with len(get_recent_items(user_id)) for
        the recent list. Addresses root cause (wrong source) not symptom.
    W4: introduce a shared display-name helper (is_system OR is_default -> tr(name)) and use it at
        EVERY web render site — generic for all app-managed list names, not hardcoded strings.
  blind_spots: |
    - Whether any user-created list is named exactly 'General'/'Recently Viewed' (would be mistranslated)
      — mitigated: only is_system/is_default rows are translated; user lists are is_default=False/is_system=False.
    - joins_lab add-candidates picker is a DESTINATION picker (counts are advisory); still fixed for consistency.

reasoning_checkpoint:
  hypothesis: |
    W1: web sidebar renders the recent system-list NAME (DB value 'Recently Viewed') raw, no tr().
    W2: web reads recent-list items via the numeric Supabase list id, so get_items_in_list_sync(numeric_id)
        -> get_list_items(numeric_id) on the (empty) list_items table; the working 'recent' literal branch
        (-> get_recent_items / recent_items table) is never reached because the sidebar/lists page key the
        system list by its numeric DB id, not the string 'recent'.
    D1: desktop lists tab (lists_refresh_items) always calls get_items_sorted(list_id, sort_by='shelfmark'),
        which re-sorts the recent list by shelfmark/library and destroys recency order.
    D2: recent_items can contain multiple item_ids for the same sys_id when add_to_recent was called with
        differing fl_id/img (or None vs a value); when rendered without a distinguishing image they appear
        as true duplicate rows.
  confirming_evidence:
    - "web/components/project_tree.py:385 ui.label(list_name) — raw, no tr(); :149 matches name=='Recently Viewed'."
    - "web/user_lists.py:557-563 — get_items_in_list_sync hits the 'recent' branch ONLY for literal 'recent'; otherwise int(list_id)->get_list_items."
    - "web/user_lists.py:153 — list keys are str(lst['id']) (numeric DB id). project_tree.py:350 list_id=list_data.get('id'); handle_click->on_select(numeric_id)."
    - "Working contrast: joins_panel.py:1022 + comment_dialog.py:95 call get_items_in_list_sync('recent') literal -> works."
    - "genizah_app.py:13459-13462 — lists_refresh_items uses get_items_sorted(sort_by='shelfmark') for ALL lists incl. recent; prior quick-fix only fixed the BROWSE tab (browse_on_list_selected:8232)."
    - "genizah_core.py:11949 _build_item_id distinguishes sys_id / sys_id::fl::X / sys_id::img::Y -> same sys_id can have multiple recent entries."
  falsification_test: |
    W1: if name were already tr()'d, Hebrew UI would show 'נצפו לאחרונה' — it shows English, confirming raw.
    W2: get_items_in_list_sync('recent') works (joins_panel) but selecting via sidebar (numeric id) is empty — confirms id-keying.
    D1: if recency were preserved, order would not be alphabetical-by-library — it is, confirming shelfmark sort.
    D2: if dupes were per-image, the Image column would differ — report says identical rows, confirming same sys_id collapse needed.
  fix_rationale: |
    W1: wrap system-list display name in tr() (key already exists in genizah_translations.TRANSLATIONS).
    W2: in UserListsManager.get_items_in_list[_sync], treat a list whose data has is_system + name 'Recently Viewed'
        as the recent source; cleanest: detect the recent system list and route to get_recent_items. Implement by
        having the lists surfaces resolve the recent system-list id to the 'recent' source, AND make
        get_items_in_list[_sync] recognise the recent system list id by consulting cached data.
    D1: in lists_refresh_items, special-case the recent list to call get_items_in_list('recent') (preserve order),
        mirroring the existing browse-tab fix.
    D2: collapse true duplicates (same sys_id with no distinguishing img) keeping the most-recent, in the recent read path.
  blind_spots: |
    - Web: whether the recent system list always has name exactly 'Recently Viewed' for all users (older rows / renamed).
    - Whether any Supabase user_lists row for recent has a stable sentinel (is_system) we can rely on instead of name.
    - D2: per-folio (fl_id) variants — user says some duplication is legitimate; only collapse when NO img distinguishes.

## Evidence

- timestamp: 2026-06-23
  checked: web/user_lists.py (UserListsManager), web/supabase_client.py get_user_lists/get_recent_items
  found: |
    Two distinct data sources for "recent" on web. (1) get_user_lists(user_id) reads the user_lists table;
    the recent system list is a row there (is_system=True, name='Recently Viewed'), keyed in _get_cached_data
    by str(lst['id']) = numeric DB id. (2) get_recent_items(user_id) reads the recent_items table (ordered
    viewed_at desc). get_items_in_list_sync routes to get_recent_items ONLY when list_id == literal 'recent'.
  implication: |
    W2 root cause. The sidebar/lists page select the recent system list by its numeric id, so the read goes to
    get_list_items(numeric_id) (empty list_items rows) instead of get_recent_items. The add-to-joins "Recent
    activity" tab passes the literal 'recent' so it works. Reconciliation: route the recent system list's reads
    to the recent_items source.

- timestamp: 2026-06-23
  checked: web/components/project_tree.py (sidebar render), web/translations.py, genizah_translations.py
  found: |
    project_tree.py:385 renders ui.label(list_name) with no tr(); list_name is the DB-stored 'Recently Viewed'.
    web tr() uses the shared TRANSLATIONS dict which DOES contain 'Recently Viewed'->'נצפו לאחרונה' (genizah_translations.py:823).
    Desktop already wraps system names: genizah_app.py:13166 _get_list_display_name returns tr(name).
  implication: W1 is web-only and fixable by wrapping the system-list name in tr().

- timestamp: 2026-06-23
  checked: genizah_app.py lists_refresh_items (13431-13546), browse_on_list_selected (8223-8271), core get_items_sorted/get_items_in_list/_build_item_id
  found: |
    Main lists tab (lists_refresh_items:13462) calls get_items_sorted(list_id, sort_by='shelfmark') for ALL lists
    incl. recent -> re-sorts by shelfmark/library (D1). The 2026-03-25 quick-fix only special-cased the BROWSE tab
    (browse_on_list_selected:8232 uses get_items_in_list('recent')), never the lists tab. recent_items holds
    item_ids; _build_item_id makes sys_id / sys_id::fl::X / sys_id::img::Y distinct, so one sys_id can occupy
    multiple recent rows (D2) that look identical when no img distinguishes them.
  implication: D1 fix = preserve recency order for recent in lists_refresh_items. D2 fix = collapse same-sys_id rows lacking a distinguishing image, keep most-recent.

### ROUND 2

- timestamp: 2026-06-23
  checked: web/components/project_tree.py _resolve_list_item_count (35-56), web/supabase_client.py get_list_item_counts (928-971)
  found: |
    The sidebar count badge takes the WARM batched path. _resolve_list_item_count routed through the
    recent-aware lists_mgr._get_list_item_count ONLY when counts is None; when a batched counts dict was
    present it returned counts.get(int(list_id), 0). The batched RPC get_list_item_counts_for_user counts
    list_items rows; the recent list has ZERO list_items rows (its data lives in recent_items) so the
    recent list is ABSENT from the dict -> counts.get(...,0) == 0 -> a stale "(0)" badge on a full list.
  implication: |
    W3 root cause. The Round-1 _get_list_item_count fix was correct but UNREACHED on the warm path. Fix:
    in _resolve_list_item_count, route the recent list through _get_list_item_count REGARDLESS of whether
    a batched counts dict is present (the batched RPC can never count recent_items). Same stale read at the
    two joins_lab pickers (pdata['counts'].get(list_id,0) and counts.get(list_id,0)) — overlay the real
    recent count from len(get_recent_items(user_id)) there.

- timestamp: 2026-06-23
  checked: supabase_setup.sql (380-382), web/user_lists.py _get_default_data (108), web render sites' tr() gating, genizah_app.py _get_list_display_name (13161-13167)
  found: |
    The 'General' list is the DEFAULT list, created with is_default=true and is_system defaulting to FALSE
    (supabase_setup.sql:381-382; user_lists _get_default_data line 108 mirrors is_system:False/is_default:True).
    Every web render site wrapped tr() ONLY when is_system (project_tree.py:358, joins_lab.py:2934,
    lists.py create_inline_edit_label:99 is_system-only, add-candidates picker joins_lab.py:2005/2006 wrapped
    NOTHING, add_to_list_dialog.py:147 raw lst['name']). So 'General' (is_default, not is_system) never got
    tr()'d and leaked English under a Hebrew UI. Desktop is correct: _get_list_display_name translates when
    is_system OR is_default. The 'General'->'כללי' and 'Recently Viewed'->'נצפו לאחרונה' keys both exist in
    the shared TRANSLATIONS dict (genizah_translations.py:822-823).
  implication: |
    W4 root cause = render sites translated is_system only, missing the is_default 'General'. Fix GENERICALLY
    with a shared web.user_lists.localize_list_name(list_data) (is_system OR is_default -> tr(name); else raw),
    mirroring the desktop rule, and use it at EVERY web render site — not hardcoded per string.

## Eliminated

- hypothesis: W2 is the Phase-120 off-loop-auth bug (run.io_bound losing request context -> anon client -> RLS 0 rows)
  evidence: |
    SEED-009's own differential refutes it — joins_panel reads the SAME recent data on the event loop and works.
    Root cause is the id-vs-'recent' source mismatch, not auth context. get_recent_items succeeds (joins_panel/comment_dialog).
  timestamp: 2026-06-23

## Resolution

root_cause: |
  W1 (web): system-list display name rendered raw (no tr()) in project_tree.py and lists.py header.
  W2 (web): recent system list keyed by numeric Supabase user_lists id; reads route to get_list_items(id) on the
    empty list_items table instead of the recent_items table. The literal-'recent' branch in
    UserListsManager.get_items_in_list[_sync] is never reached from the sidebar/lists-page selection.
  D1 (desktop): lists_refresh_items sorts the recent list by shelfmark via get_items_sorted, destroying recency.
  D2 (desktop): multiple item_ids per sys_id (from differing fl_id/img) surface as true-duplicate rows.
  W3 (web, ROUND 2): the count badge takes the warm batched path; _resolve_list_item_count returned
    counts.get(int(list_id),0) when a batched counts dict was present, bypassing the recent-aware
    _get_list_item_count. The batched RPC counts list_items rows; the recent list has none -> absent from
    the dict -> stale "(0)". Same stale read at both joins_lab pickers.
  W4 (web, ROUND 2): 'General' is the DEFAULT list (is_default=true, is_system=FALSE), but every web render
    site wrapped tr() only when is_system. So 'General' was never localized and leaked English under a
    Hebrew UI (Recently Viewed was fixed in W1 because it IS is_system).
fix: |
  W1 (web): translate the system-list display NAME at render time (the key 'Recently Viewed'
    already exists in the shared TRANSLATIONS dict that web tr() consumes).
    - web/components/project_tree.py: display_name = tr(list_name) when is_system; label uses display_name.
    - web/pages/lists.py: create_inline_edit_label system branch shows tr_func(current_name).
    - web/pages/joins_lab.py _render_level1: system list names rendered via tr().
  W2 (web): route the recent system list's reads to the recent_items source even when it's
    addressed by its numeric Supabase user_lists id (not just the literal 'recent').
    - web/user_lists.py: new _is_recent_list(list_id) (literal 'recent' OR a cached list row that
      is_system + name(_en) == 'Recently Viewed'); get_items_in_list[_sync] and _get_list_item_count
      route through it to get_recent_items.
    - web/pages/joins_lab.py: _is_recent_system_list() helper; _load_level2 reads get_recent_items
      for the recent list instead of get_list_items.
  D1 (desktop): lists_refresh_items special-cases list_id == 'recent' to preserve view-time order
    (no shelfmark/library re-sort), mirroring the earlier browse-tab fix.
  D2 (desktop): new GenizahGUI._get_recent_items_deduped collapses true duplicates by (sys_id, img)
    keeping the most-recent occurrence; distinct non-empty images stay as separate rows.
  W3 (web, ROUND 2): route the recent list's count through the recent-aware source at every render site.
    - web/components/project_tree.py _resolve_list_item_count: if lists_mgr._is_recent_list(list_id),
      return lists_mgr._get_list_item_count(list_id) BEFORE consulting the batched counts dict (batched RPC
      can never count recent_items). Non-recent lists keep using the batched dict (no extra fanout).
    - web/pages/lists.py header expected_count: skip the batched override for the recent list (trust the
      already-correctly-loaded items_data).
    - web/pages/joins_lab.py both pickers: overlay counts[recent_id] = len(get_recent_items(user_id)); the
      drill-down picker shows the recent badge even when the best-effort RPC was denied (recent_count is
      authoritative).
  W4 (web, ROUND 2): new shared web/user_lists.py::localize_list_name(list_data) (is_system OR is_default ->
    tr(name); else raw name), mirroring desktop _get_list_display_name. Wired at EVERY web render site:
    - web/components/project_tree.py _render_list_item: display_name = localize_list_name(list_data).
    - web/pages/lists.py create_inline_edit_label: new is_default param; the renameable default list's
      DISPLAY (label + edit value) is localized while still editable.
    - web/pages/joins_lab.py both pickers: list_name = localize_list_name(lst).
    - web/components/add_to_list_dialog.py: list_options localized via localize_list_name.
verification: |
  tests/test_recently_viewed_bugs.py now 16 tests, all pass:
    - W2 numeric-id routing -> get_recent_items (not get_list_items); literal 'recent' still works;
      regular list unaffected; count badge uses recent_items.
    - W1 translation key present; web tr() localizes under he/en; joins_lab recent detector.
    - D1 recency order preserved (not shelfmark-sorted).
    - D2 true dupes (same sys_id, no img, differing fl_id) collapse to one keeping most-recent;
      distinct images preserved.
    - W3 (ROUND 2): _resolve_list_item_count routes the recent list through _get_list_item_count even
      when a batched counts dict is present (ignores the stale 0); non-recent lists still use the batched
      dict; legacy (counts=None) path still recent-routes.
    - W4 (ROUND 2): 'General'->'כללי' key present; localize_list_name translates default AND system lists
      under he, passes raw under en, never translates a user-created list literally named 'General',
      falls back to name_en, handles empty dict.
  Import-check passes for all modified web modules (web.user_lists, project_tree, lists, joins_lab,
  add_to_list_dialog); ruff clean on all changed files. No circular import (user_lists imports tr lazily).
  STILL NEEDS HUMAN VERIFY (live, authenticated): the W3 badge count and W4 'General'->'כללי' localization
  on the real /lists sidebar+header, the Join-Labs picker, and the add-to-list dialog require a logged-in
  Supabase user with a real recent_items row + the default 'General' user_lists row under a Hebrew UI;
  cannot be exercised headless.
  Pre-existing unrelated failure: tests/test_joins_lab_new_search_reset.py fails ONLY when run after
  the joins_lab render-smoke test (NiceGUI "parent slot deleted" cross-test pollution); passes in
  isolation on both clean tree and with these changes — not caused by this fix.
  STILL NEEDS HUMAN VERIFY (live, authenticated): the web surfaces require a logged-in Supabase user
  with a real recent_items row + a real 'Recently Viewed' user_lists row; cannot be exercised headless.
files_changed:
  - web/user_lists.py
  - web/components/project_tree.py
  - web/pages/lists.py
  - web/pages/joins_lab.py
  - web/components/add_to_list_dialog.py   # ROUND 2 (W4)
  - genizah_app.py
  - tests/test_recently_viewed_bugs.py
