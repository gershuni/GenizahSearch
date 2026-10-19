# Joins Lab and Visual Similarity improvements — decisions and PR order

**Status:** PR 1 implemented on `feat/joins-lab-links-compare` (2026-10-19); PRs 2-6 not started.
**Source:** the owner's list of six ideas and a WhatsApp exchange with a user (Adiel Breuer),
both 2026-10-19. Every decision below was made by the owner on 2026-10-19, one question at a time.

## The ideas, as the owner wrote them

1. Sort by domain and by library (as the website's Visual Similarity window does).
2. More information and links in the Compare window.
3. The Browse icon in the website's Compare window can be seen only in the dark theme.
4. Icons on the buttons of the Browse page.
5. Some flexibility in the Visual Similarity image sizes: larger thumbnails, and a larger
   original that takes more room with fewer thumbnails per row.
6. To check: keeping thumbnails of Genizah fragments on the website's own server.

From the user (Joins Lab, website):

- Ctrl+click on a candidate's Browse icon, to keep the Lab and read the fragment later, replaced
  the Lab's own tab instead.
- Clicking a candidate's image should open Compare, not Browse (see "Open question" below).
- A link that reopens the current Joins Lab screen: the same fragment, candidates, grid page, and
  the same pair in Compare. The URL today names only the Lab.

## Decisions (owner, 2026-10-19)

| Item | Decision |
|---|---|
| Candidate-card Browse icon (web) | A real link; a plain click opens a new tab; Ctrl/middle-click and "Open in new tab" work |
| Known Joins Browse button (web) | The same |
| Compare Browse icons (3) | Visible in the light and warm themes |
| Compare window (2), web and desktop | Each side: an info line (library · domain · date · size · material; the anchor's title on the web), external links (Ktiv, the library's viewer, PGP), a genizahsearch.com link on the desktop, a copy-shelfmark button. **Must not take much of the screen.** The two bugs found while reading (below) fixed with it |
| Share link (PR 2) | Screen state in the URL; the address bar follows as you work; a "Copy link" button with an "include my marks" option, off by default. The link carries the anchor and page, search text and options, Visual Similarity on/off, filters, view, grid page, and the Compare candidate. Opening it re-runs the search. Shared marks are shown for that visit only, never saved, and the recipient's own marks are untouched. This reverses the earlier rule "Builder/candidate/triage state is NEVER in the URL" (`web/main.py::joins_lab_page_route`) for the shared parts |
| Sort and filters (1, PR 4) | Web and desktop Joins Lab: Sort (Rank / Library / Domain) and Library / Domain filters with Hebrew names and counts, built as PR #404 builds them; the desktop cards also show library and domain |
| Browse icons (4, PR 5) | Website only; an icon next to the text, never instead of it |
| Image sizes (5, PR 3) | Visual Similarity window: S/M/L for the thumbnails (about 170/230/300 px; the thumbnails are already fetched 300 px wide) and a separate "enlarge original" button (260 -> about 450 px, image up to half the screen height), both remembered per visitor like the List/Thumbnails choice. S/M/L also on both Joins Labs (web 160 px images; desktop 220x130 cards, whose loader fetches 320 px) |
| Thumbnails on the site (6, PR 6) | Step (a) now: the image proxy routes stop setting the session cookie so Cloudflare and browsers keep thumbnails (check each route does not use the session). Step (b), a size-capped disk store filled as images are viewed, waits for `df -h /` from the server and the owner's reading of NLI's terms of use |
| Way of working | Small PRs in this order, one branch per PR from `master-main`: 1 Joins Lab links + Compare window; 2 share link; 3 image sizes (on top of PR #404, or after it merges); 4 sort and filters; 5 Browse icons; 6 thumbnail caching (a) |

## PR 1 (this branch): Joins Lab links and the Compare window

- `web/components/candidate_grid.py`: the card's Browse icon (`menu_book`) is a `ui.link(new_tab=True)`.
  It was a button that set `window.location.href`, so a plain click and a Ctrl+click both
  replaced the Lab.
- `web/components/known_joins_group.py`: the member row's Browse icon is a new-tab link
  (`member_browse_url`); `on_open_browse` and the page's `_on_open_browse` are gone.
- `web/components/compare_modal.py`:
  - The two Browse icons are coloured like the shelfmark. They were `text-white` on the light pane.
  - Each pane gets a copy button, one info line and one links row, through `_render_pane_details`.
    The anchor gets the catalogue title when the Lab passed none.
  - Bug fixed: the FJMS/PGP button row was filled only once, on open, so after Prev/Next it still
    opened the first candidate's data. It is now emptied on every flip and refilled for the shown
    candidate (`_load_candidate_meta`, latest-wins on `_cand_load_gen`).
- `shared/fragment_details.py` (new): the one rule for both windows. It provides `info_line`,
  `domain_labels`, `ktiv_url`, and `fetch_fragment_details`, which uses local reads only (FJMS,
  NLI crossref, pgp.db, and Oxford parts through `meta_mgr`).
- `desktop/join_workbench.py` `CompareDialog`:
  - A 📋 button by each shelfmark.
  - A title line, an info line, and a links row (Ktiv, the library's viewer, PGP, then
    genizahsearch.com, which follows the folio).
  - Details are fetched on `_PaneDetailsWorker`, retained, reaped and waited on with the page-text
    workers. A result for a fragment the pane no longer shows is dropped.
  - Bug fixed: the anchor's line read library and title from the top level of the result dict
    (`meta_brief`), but they live under `display`, so the line came out empty.

Not checked in a browser or against real sidecars. The web tests drive the real dialog in
NiceGUI's simulated client. The desktop pane was rendered offscreen with stub data.

## Open question

The user said that clicking a candidate's **image** replaced the Lab with Browse. In `master-main`
the image has opened Compare since 2026-06-19 (`candidate_grid.py`, "G4"). The only things in the
Lab that left the tab were the Browse icon and the Known Joins Browse button, both fixed in PR 1.
Ask the user what exactly he clicked. If it really was the image, the live site differs from the
code and needs a look.
