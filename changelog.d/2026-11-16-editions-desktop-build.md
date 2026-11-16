## [Unreleased] -- The desktop builds and opens the scholarly transcriptions' index (desktop, branch `claude/editions-desktop-build`)

Nothing new is visible yet: the source choice, the grouped results and their `Src` column, the
viewer, lists, exports and Help come in the next change.

- **The desktop builds the editions index itself** (`desktop/editions_index.py`) from the FGP and
  PGP sidecars it ships, the NLI crossref and the main index's browse map, as the editions plan
  decided (D3: built locally, not added to the installer). A few seconds after each start, a
  background thread compares `<INDEX_DIR>/Editions` with the installed inputs; when it is missing
  or stale it builds `Editions.new` beside it (88 s and 276 MB on the full data, 2026-11-16),
  checks it, and the window puts it in place once no search reads the index -- the way a
  rebuilt main index is swapped in -- then opens it. A finished build that was never put in place
  is put in place without building again; an unfinished one is replaced. A promotion whose
  renames fail never reopens the old index: the new build is kept and tried again later.
- **A main-index rebuild stops a running editions build** (its new browse map makes the build
  stale), and the editions are checked and rebuilt when it ends. A sidecar update stops it
  too (a running build holds the files open), its downloads start once the build has ended, and
  the editions are checked when they have ended. After either, the index built from the old files
  is closed once no search reads it, until the rebuilt one is installed (the main index's reopen
  no longer reopens it on the UI thread meanwhile). A build records its inputs as they were
  before it read them (`build_generation`), and one whose inputs were replaced while it ran is
  dropped and built again. The exit stops a running build at its next document
  (`build_generation(should_stop=)`, `BuildCancelled`).
- **The engine searches the editions with `sources`** (`all` / `editions` / `automatic`) once the
  index is open, the per-request copies that carry a run's variant settings included
  (`SearchEngine.install_editions` installs a leaf opened in the background). A search asked to
  read them before then runs on the automatic transcriptions and its result line says so once
  ("The scholarly transcriptions could not be searched this time"). No window passes a source
  choice yet, so every search is today's.
