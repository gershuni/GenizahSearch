## [Unreleased] -- The scholarly transcriptions in the desktop's search (desktop, branch `claude/editions-desktop-ui`)

The visible half of the desktop's editions search; the index it reads is built in the background
since the previous change.

- **A Transcriptions choice** beside the corpus choice on the Search tab: **All** (the automatic
  and the scholarly transcriptions; the default), **Scholarly**, **Automatic** -- the website's
  words and Hebrew (תעתיקים: הכל / מדעיים / אוטומטיים). It is shown where the transcriptions are
  read (the Genizah in a text mode; not My Library alone, Title, Shelfmark or Lab Mode), kept in
  the session and in each history entry, and replayed by every search-within step.
- **One result per page.** The `Src` column names every text that matched -- `V0.8`, `FGP`,
  `PGP` -- with each named in full, with its credit, when you point at it ("Matched in: FGP
  edition (credit), Automatic transcription"); the count line adds "N in scholarly
  transcriptions"; a hit whose page could not be identified says so on its page cell.
- **Opening a hit in a scholarly transcription** -- in the Manuscript Viewer and in Browse --
  chooses that edition on the folio it arrived for (kept even where the page's filter would
  leave it out) and marks the match itself on the edition's own characters, mapped from the
  span the search read (`shared/edition_target.py`, shared with the website's /browse;
  `desktop/edition_view.py`). A stale hit opens the manuscript as usual and says the edition has
  changed; an unplaced one says its page is not known. The viewer no longer replaces such a
  row's identity with the automatic page's.
- **Lists** add a located hit's page and an unplaced one's manuscript; **exports** name every
  text that matched in their Source column (Excel, CSV, Word), and their search information
  says which transcriptions were read. The session keeps the members of each result light.
- **Help**, in both languages: "Scholarly transcriptions in the search". The Hebrew Help now
  writes תעתיק / תעתיקים throughout.
