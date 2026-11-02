## [Unreleased] -- The Manuscript features counts are ready when the site starts (web, branch `claude/criteria-warmup`)

- The value lists and "recorded for N" counts of the **Manuscript features** window are computed once at
  server start, in the background pre-warm (`web/components/criteria_panel.py::prewarm_criteria`, called from
  `web/main.py`): about 20-30 seconds over all 255,723 manuscripts. The first visitor after a restart used to
  wait for them section by section, 1-8 seconds each (Measured 7.9 s on the owner's machine); afterwards they
  came from the cache, as they do now from the start. `GET /api/criteria` reads the same cache.
