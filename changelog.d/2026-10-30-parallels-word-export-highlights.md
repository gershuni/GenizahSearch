## [Unreleased] -- Web: the Parallels Word export marks the matched words (web, branch `ccr-476f870c-bfg97t`)

### Fixes
- **The Parallels Word export now marks the matched words, as its Excel export does (GS-21).** Since
  2026-08-23 the Excel export has shown each match in red and bold in both Source Context and Manuscript
  Match, but the Word export removed the marks and printed both texts plain. For a short source text the two
  read almost the same, and the two downloads of one search disagreed about what matched. The Word export
  now shows the marked words in bold with a yellow highlight, as the web search's Word export already does
  (`web/export_service.py::export_parallels_word`).
