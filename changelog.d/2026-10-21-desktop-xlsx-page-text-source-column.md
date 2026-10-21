## [Unreleased] -- Desktop downloads write the page, and the Source column shows V0.7 (desktop, branch `claude/desktop-xlsx-fulltext-source`)

Two desktop-only faults found by comparing the same Responsa search,
`*כהנים *גדולים`, on the desktop and on the website. Each fix has a
regression test that fails on the old code.

### Fixed
- **Excel "Full Text"** is the text of the page the row shows, as on the
  website. A row found through a whole manuscript held that manuscript's whole
  text, cut at 32,000 characters: in the owner's download, 132 of 405 cells did
  not contain the match, and the file was 4.3 MB against the website's 0.65 MB.
  The **Word and text** downloads had the same fault: the passage around a hit
  could come from another page of the manuscript.
- **Excel line breaks** no longer glue the words on either side together. The
  Snippet marks a line break with ` ‖ ` and Full Text replaces it with a space,
  as the website does.
- **The Source (מקור) column** shows whenever a result is a V0.7 transcription
  (or a My Library hit). It was shown only when `AllGenizah_OLD.txt` sat beside
  the program, which the installed app's folder does not have, although its
  index holds V0.7 pages: 44 of the 405 results above had no visible marker.
  V0.8-only results now hide it.
