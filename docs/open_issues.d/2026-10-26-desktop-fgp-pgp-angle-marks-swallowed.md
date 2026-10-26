## P2 desktop: FGP and PGP editions lose every `<…>` mark on the desktop
Where: `genizah_app.py::_browse_display_pgp_text`, `desktop/result_dialog.py::_rd_display_pgp_text`
Opened: 2026-10-26

Both functions show a PGP edition/translation or an FGP transcription by replacing `\n` with `<br>` and passing the
text to `setHtml` (through `apply_line_numbered_text`) without escaping it, so Qt's rich-text parser takes every
`<…>` in the text for a tag and drops it with its contents. Measured 2026-10-26 with the same calls in an offscreen
probe: `דמנח אפתורא <ד'>` shows as `דמנח אפתורא`, `<ב>`, `<(ה)>` and `<~>` vanish, and `<<רא>>` leaves a stray `>`.
In FGP's transcriptions `<…>` is the Genuzot team's line-filler / catchword mark (`<רא>`, `<ראובן>`): 9,729 of
today's FGP rows and 2,942 of the source-folders candidate's new rows carry one. In PGP, 315 of 36,642 document
transcriptions (and 382 source / 408 footnote texts) use `<…>` for an editor's added letters (`מיב<מ>נא`,
`ו<י>טול`), which the desktop silently removes from the word. Entities decode as a side effect (`&deg;` shows as
`°`, 4 FGP rows). The website escapes (`web/components/typography.py::render_line_numbered_html`) and shows them.
Fix sketch: `html.escape` the text before the `\n` → `<br>` step in both functions (the search-hit `*…*` markers
and `markers_to_bold_html` are unaffected by escaping), first checking that no PGP text relies on real markup
reaching Qt; a test that renders `<ד'>` and `מיב<מ>נא` through both paths and reads them back from the widget.
Found while assessing how to show FGP's transcription marks (GS-274).
