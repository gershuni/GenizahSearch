## [Unreleased] -- Browse: the version chooser above the text, and A- / A+ (web, branch `claude/web-version-text-size`)

- The version chooser (with the line numbers and the dictionary switch) heads Browse's text pane. It sat under the text: below the fold at 1280x800 (at 973 px), and on a phone after the whole text.
- The "V0.8" badge beside the page number is gone: it named the automatic transcription's model even while a PGP or FGP edition was on screen. The chooser names the text shown.
- The version chooser is the name of the version shown, with a drop-down arrow: a click on it opens the list of versions. The separate "Version History" button beside it is gone (the owner: misleading). Browse and Quick View.
- A- / A+ beside the text, as in the desktop app: Browse's page and Full Manuscript View, and the Reading Desk's texts (by fragment, and a document's own text). One size per visitor, kept for the next page, in the desktop app's steps (9pt to 40pt by 2pt of its 16: from 9/16 to 2.5 times the size, by 0.125). `web/components/text_size.py` (the `--gs-text-scale` CSS variable the texts' font sizes are multiplied by).
- Tests `tests/render_smoke/test_browse_text_head_render_smoke.py`.
- The text starts a step smaller for a visitor who has not chosen a size (one press of A−: 14pt of the desktop's 16; owner, 2026-11-23), everywhere; a size chosen is kept.
- The line numbers' button no longer makes the page jump up: the text was drawn again from nothing, the pane empty for a moment (845 px of the page's height at 375 px), and the browser pulled a short page's scroll up. The text's content now changes in place, for a new version too.
