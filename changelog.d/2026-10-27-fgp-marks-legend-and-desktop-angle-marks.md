## [Unreleased] -- A legend for FGP's transcription marks; the desktop keeps `<…>` marks (web + desktop, branch `master-main`)

### New Features
- **A "?" beside a Genuzot transcription opens the team's legend of marks** (GS-274, option A), on the website
  (the version selector) and on the desktop (Browse and the Manuscript Viewer). The Genuzot team types its
  marks into the text (`?ב?` a doubtful letter, `@…@` a piyyut, `<…>` a line filler, …); the legend gives their
  meanings from the team's 2024 list, the three meanings its 2014 list gives differently, and the marks neither
  list explains. One table serves both apps: `shared/fgp_marks_legend.py`. The text itself is unchanged; drawing
  the marks (option B) waits on FGP.

### Bug Fixes
- **Desktop: `<…>` marks in PGP and FGP editions are shown again** (GS-275). The text went to Qt's `setHtml`
  unescaped, so every `<…>` was read as a tag and dropped with its contents: FGP's line fillers (`<רא>`, 9,729 of
  the FGP rows) and a PGP editor's added letters (`מיב<מ>נא` read `מיבנא`, 315 PGP transcriptions). All
  text-to-HTML sites in Browse, View All and the viewer now escape first (`desktop.widgets.text_lines_to_html`),
  and a test fails any new site that does not.
