## [Unreleased] -- The Manuscript features counts are ready when the site starts (web, branch `claude/criteria-warmup`)

- The value lists and "recorded for N" counts of the **Manuscript features** window are computed once at
  server start, in the background pre-warm (`web/components/criteria_panel.py::prewarm_criteria`, called from
  `web/main.py`): about 20-30 seconds over all 255,723 manuscripts. The first visitor after a restart used to
  wait for them section by section, 1-8 seconds each (Measured 7.9 s on the owner's machine); afterwards they
  came from the cache, as they do now from the start. `GET /api/criteria` reads the same cache.
- The note under the total ("These criteria cover all N manuscripts...") and the "of M" beside every count
  are cached too (`FjmsService.criteria_corpus_note`): each section opened ran a corpus-wide width query
  for them, every time. A visitor who opens the window while the start is still computing waits for that
  computation instead of running the same query beside it, while other features are still computed side by
  side (`FjmsService._build_once`, one lock per feature; GitHub Codex on PR 461).
