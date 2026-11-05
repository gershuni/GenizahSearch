## [Unreleased] -- What's New in every version, and the update notice shows what changed (web + desktop, branch `claude/whats-new-history`)

Owner, 2026-11-05: a What's New for every version from 9.0.0, on the website and in the desktop app,
and the desktop update notice showing the changes between the reader's version and the new one.

- **One history, four surfaces.** `shared/whats_new_history.py` reads and checks the entries in
  `shared/whats_new_data.py`: per release its date, the apps it shipped to, a title and its items,
  each a new feature, an improvement or a fix, in English and Hebrew, with the app a reader sees it
  in. The entries for 9.0.0-9.13.1 were written from each release's approved What's New texts, its
  GitHub notes and its CHANGELOG section. `tests/test_whats_new_history.py` fails while
  `version.APP_VERSION` has no entry, so each release adds its own. An entry newer than the running
  app's version is not shown anywhere (it is written with the release texts, before the bump).
- **Website:** `/whats-new` (`web/pages/whats_new.py`), linked from the header's What's New panel
  ("What's New in every version"; the panel is not shown on the page itself), and in the static
  sitemap. `GET /api/whats-new?since=&upto=&app=web|desktop` serves the same entries in both
  languages, cacheable for an hour; a malformed version or app answers 400 `invalid_request`.
- **Desktop:** "What's New in every version" in the side menu and the ☰ menu, and "All versions" in
  the What's New dialog, open `desktop/version_history.py::VersionHistoryDialog` on the bundled
  history. The update bar gains "What's new?", and a manual check that finds a newer version now
  opens the same dialog in update mode: the changes since this copy's version, read from the website
  in `ReleaseNotesThread` (an installed copy cannot know newer entries), with Update Now / Later; if
  the website cannot answer, it says so and links the GitHub release page.
- **The header's search box sits inside the header.** `q-py-none` lost to the header's own 16px
  padding, so the 64px row sat 16px low and the 40px go-to box hung over the page below the header;
  the header now sets no vertical padding inline (`tests/render_smoke/test_header_search_render_smoke.py`).

- **The dictionary panel's disclaimer** now says Blau's dictionary records *mainly* (בעיקר) meanings
  particular to Judeo-Arabic, not *only* (רק) (owner, 2026-11-05, with the Help text).

Tests: `tests/test_whats_new_history.py` (entries, readers, the API, the dialog's HTML),
`tests/test_version_history_qt.py` (the dialog, the update notes with the fetch stubbed, the bar,
the manual-check call site).
