## [Unreleased] -- A legend for FGP's transcription marks; the desktop keeps `<…>` marks (web + desktop, branch `master-main`)

### New Features
- **A "?" beside a Genuzot transcription opens the team's legend of marks** (GS-274, option A), on the website
  (the version selector) and on the desktop (Browse and the Manuscript Viewer). The Genuzot team types its
  marks into the text (`?ב?` a doubtful letter, `@…@` a piyyut, `<…>` a line filler, …); the legend shows the
  team's two lists, for prayer and piyyut fragments (2024) and for Talmud fragments transcribed for Hachi Garsinan
  (2014). One source serves both apps: `shared/fgp_marks_legend.py`. The text itself is unchanged; drawing the marks (option B) waits on FGP.

### Bug Fixes
- **Desktop: `<…>` marks in PGP and FGP editions are shown again** (GS-275). The text went to Qt's `setHtml`
  unescaped, so every `<…>` was read as a tag and dropped with its contents: FGP's line fillers (`<רא>`, 9,729 of
  the FGP rows) and a PGP editor's added letters (`מיב<מ>נא` read `מיבנא`, 315 PGP transcriptions). All
  text-to-HTML sites in Browse, View All and the viewer now escape first (`desktop.widgets.text_lines_to_html`),
  and a test fails any new site that does not; the visual-similarity preview shows its text as plain text.
- **FGP's stored HTML entities are decoded once, for every surface** (GS-60). The sidecar keeps `&deg;` where the
  Genuzot team typed its margin sign `°`; the website's Quick View decoded it, its Browse showed `&deg;`, and the
  desktop decoded it only through the bug above. `shared.fgp_service` now decodes well-formed entities when it
  reads a row (`decode_stored_entities`), so all show `°`.
