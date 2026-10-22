## [Unreleased] -- Back to a search shows what the page showed, in every mode (web, branch `claude/responsa-back-restore`)

Measured before (local server, built-in browser, open a result in the same tab,
press Back): a Responsa search ran again from scratch (in production on
2026-10-22 one 45 s search ran three times for one visitor); a finished
25,320-row Exact search came back as 250 rows ("250+"); a search left before it
finished started again, while the abandoned one ran on and the new one waited
behind it. Causes: the Back check refused any address with a `mode`; the page
reads its saved state while it is built, before the browser's socket connects,
so NiceGUI's per-tab storage was never readable there and every restore fell
back to the 250-row copy every tab shares; and the address was stamped only when
a search completed.

Now every search writes its address when it starts: the query, the page's mode,
the Responsa options and a random run code (`run=`). `web/search_runs.py` keeps,
per browser and code, the running search and then the page's snapshot (up to
1,000 rows and the filters shown with them), in memory and bounded (30 minutes
after last use, 5 runs per browser, 100 in all). Back to the address:

* a finished run is restored from it -- all its rows up to 1,000, its own
  pre-search filters (saved with the run) and display filters (PGP, library,
  printed, exclusions; updated as they change), never another tab's; a run that
  found nothing shows "0 results" instead of searching again;
* a run still running is followed: the page shows its progress and clock and
  its rows when it ends, and Stop stops it -- no second search;
* a stopped run comes back as the partial rows the page showed;
* with the run not kept (a restart), the shared copy is used when it is that
  run's, else the address searches again, in its mode.

`web/search_bootstrap.py::matches_saved_run` is the match (code, query, mode,
Responsa options, and the run's filters); a typed or pasted link without the code
searches again, as a link should. Only the page that owns a run saves it (a page
left mid-search still finishes it, and must not add a second history entry).
Addresses without a mode, stamped before this, keep the old query-only Back.
NiceGUI's tab storage is no longer used by the search page.

Tests: `tests/test_search_runs.py` (the registry: per browser, bounds, running /
done / failed), `tests/test_search_bootstrap.py` (the match in every mode, rejoin,
an empty run, each way it must not match, the run's snapshot and filters, and the
page's call sites: the address stamped before the job is awaited, Stop on a page
following a run, the owner-only save, no local named `run` -- that shadowed
NiceGUI's `run` module and broke every search's enrichment until the browser
check caught it). Checked in the browser on a local server: the five cases above.
