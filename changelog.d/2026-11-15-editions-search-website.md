## [Unreleased] -- Scholarly transcriptions on the website's search and reading pages (web, branch `claude/editions-web-pages`)

Behind `EDITIONS_SEARCH_ENABLED` (off by default; `editors` shows it to signed-in editors and
admins only): nothing changes until it is turned on and the editions index is installed.

- **Search:** beside the mode, **Transcriptions** chooses what is searched -- **All** (the
  automatic and scholarly transcriptions together, the default), **Scholarly** or **Automatic**
  -- remembered per visitor and kept by Search Within. Each page is one result; its
  card names every text that matched ("Matched in: Automatic transcription, PGP edition
  (Ben-Sasson)"), each edition a link to it, and a hit whose page could not be identified says
  so. The count line adds how many results matched in a scholarly transcription, and says when
  they could not be searched this time. With the scholarly transcriptions there are no early rows
  while the search runs.
- **Reading a hit:** `/browse?version=fgp:<id>&span=a-b` opens that edition on its page, before
  the page and coverage rules could leave it out or prefer another, and marks the match on the
  edition's own characters (points and signs included), never a phrase guessed again. An edition
  changed since the search opens the manuscript as usual and says so. Quick View, the expanded
  text, Copy and the exports read the edition, not the page's automatic transcription; the
  exports' Source column names every text that matched.
- **Deploy:** `scripts/deploy_editions_index.ps1` builds the editions index on the server and puts
  it in place (first install, or after a main-index rebuild). The FGP and PGP sidecar deploys
  rebuild it with the sidecar wherever the server has one: built and gated while the site serves,
  then the service stopped, both swapped, the service started, both put back on any failure. The
  PGP deploy now uploads under a temporary name and keeps the served file as `.prev-<stamp>`, as
  the FGP one does. `build_editions_index.py` gains `--promote-only`, `--restore-previous` and
  `--check-live`; the admin page says when the index was built from other inputs than those
  installed.
- Help describes the choice where it is served.
