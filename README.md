# Dicta Genizah Search Pro 8.0.0

**Collaborative Research Platform for the Cairo Genizah**

A comprehensive research environment for the Cairo Genizah, featuring a **Web Platform** ([Dicta Genizah Search](https://genizahsearch.com)), **Community Features**, and full **WCAG 2.0 Accessibility**.

> **Web Access:** [genizahsearch.com](https://genizahsearch.com) (אתר הגניזה של דיקטה) - Search, browse, and collaborate from any browser

---

## What's New in Version 8.0.0?

### v8.0.0: Dicta Genizah Search Pro — Joins Lab & enhanced Local Library

The desktop app is now **Dicta Genizah Search Pro**. This release adds a dedicated
workspace for **physical joins** and brings your own document library into
Composition Search.

- **Joins Lab (desktop)** — pin a fragment as the **anchor**, build a line-by-line
  query for the joining fragment, and triage candidates **Yes / Maybe / No**.
  Surface look-alikes with built-in **Visual Similarity**, flag size mismatches, and
  send confirmed joins straight to the Fragment Puzzle. The window is modeless and
  remembers its state across restarts.
- **Enhanced Local Library (desktop)** — **Composition Search** now runs over your
  own indexed documents (choose **Genizah / Local / ALL**), and My Library now
  searches more file types — **xlsx, CSV, HTML**. Composition runs that include
  local hits export correctly to xlsx / CSV / TXT / DOCX.
- **Hebrew interface coverage** — ~250 more interface strings now appear in Hebrew
  for Hebrew users across both apps.

### v7.16.0: Hebrew PDF Text Quality

A major overhaul of how **My Library** reads Hebrew PDFs. Typeset Hebrew
scholarly books that previously came out unsearchable — shattered into single
letters, or fused into giant run-on words — now extract and index correctly.

- **Word boundaries detected from real letter spacing** — `אוצר הגאונים`
  one-letter tokens dropped from ~74% to ~5%; tight-set books' word-fusion from
  16% to ~0%, so `פירוש המשנה` is searchable again
- **Vowels & punctuation by Unicode category** — the maqaf (`־`) and sof-pasuq
  are kept, not stripped, so `דו־שיח` and `סב־סג` stay intact
- **Numbers no longer reversed** — `1977` stays `1977`; ranges like `194-256`
  stay intact
- **Garbled text layers flagged** in the My Library tree (would need OCR)
- **Open file location** for LOCAL hits (ResultDialog, Browse panel, and the
  right-click menu) — opens the containing folder with the file selected; the
  right-click menu for LOCAL files is now file-aware (Open file / Open file
  location / Copy file location / Copy filename)
- **Fixes** opening `.html` / `.xlsx` / `.csv` LOCAL files, and a launch
  freeze after an interrupted re-index
- **Run "Re-index All"** (אנדקס מחדש הכל) once to apply the text-quality fixes
  to an existing library

### v7.15.0: PDF Page Image in My Library

The headline: when you open a LOCAL PDF in **My Library**, you now see
the actual PDF page next to its extracted text — in both the
ResultDialog and the Browse panel. Prev/next navigation keeps the image
and text in sync.

- **PDF page image alongside extracted text** in My Library
  (ResultDialog + Browse panel)
- **Indexes large folders without UI freezes** — folder enumeration is
  workerized; UI stays responsive
- **Resume interrupted indexing** — on next launch, choose Resume,
  Restart, or Skip when the previous scan was killed
- **Reset My Library** — two-step typed-confirm destructive button for
  when a clean start is faster than recovery
- **Re-index All** button to apply extractor improvements without
  losing your library or opt-out preferences
- **Hebrew PDF text quality** — word-order RTL fix + intra-paragraph
  reflow collapse
- **Graceful PDF render failures** — corrupt/missing/encrypted PDFs
  show a placeholder instead of hanging

### v7.14.0: My Library — Local Document Search

The desktop app adds **My Library**, a new 7th tab that indexes
folders of your own `.docx`, `.pdf`, and `.txt` documents into a
separate side-index and surfaces them inline in Search, Composition
Search, and Parallels alongside the Cairo Genizah corpus. Personal
corpora stay on your machine — three regression tests pin the
cloud-write boundaries.

**Scoping a query is now explicit:** a pre-search dropdown next to
the Search button picks `Genizah` / `Local` / `ALL` (Hebrew
`גניזה / מקומי / הכול`, default `Genizah`); a post-search 3-state
button (`Filter Local → Only Local → No Local`) mirrors the existing
PGP and Printed filters on Search, Composition Search, and
Parallels. LOCAL hits carry a blue `LOCAL` badge; double-click
opens the file text in ResultDialog with a blue **Open file**
button that launches the document with your OS default app.

**Technical notes:** PyMuPDF (`fitz`) is now a desktop dependency
for PDF extraction (~25 MB installer growth). LOCAL hits merge via
reciprocal rank fusion (k=60) *after* deduplication. All 28 new UI
strings translate to Hebrew. Inspired by Yehuda Seewald's
`GenizahLocal` prototype.

### v7.13.0: Research-Grade Export & Polish

This release turns the xlsx download from a flat results table into a
**four-sheet citation-ready workbook** designed for academic use, adds
a **PGP filter** to web search, adds new bilingual About + FAQ sections
to the homepage (new to the site), and fixes a list-merge bug on
desktop.

**Citation-ready Excel downloads (web + desktop):** every xlsx export
now produces four sheets — **Search Results**, **Manuscripts**,
**Bibliography**, and **Credits and Info** — with bilingual column
headers matching your UI language. Every Search Results row gets a
clickable **Image URL** pointing to the proxied folio image. Every
Manuscripts row gets clickable PGP, Library Viewer, and GenizahSearch
URLs. The web JSON export gains `has_pgp`, `is_printed`, and `domains`
keys per item.

**PGP filter (web):** a new 3-state toggle in the search results
toolbar — `Filter PGP` → `Has PGP` → `No PGP` — appears once your
current results include at least one PGP-tagged manuscript.

**Homepage + Help page polish:** new bilingual About + FAQ sections
on the homepage, more links to source projects (MiDRASH, FGP, PGP,
NLI, CUDL, the Responsa Project), a faster homepage (favicon shrunk
44×), and a new "Public API & AI Tools" Help page section linking to
the Search API endpoints and the `cairo-genizah-research` Claude skill.

**For desktop users — bug fix:** the cloud-merge prompt no longer
duplicates items when reconciling lists that exist on both web and
desktop. Existing duplicates from past syncs clean up automatically
on the next merge.

### v7.12.0: Multitenant Safety and Line Numbering

A milestone release closing two big threads at once:

**For all users — new feature:** Every transcription text view now
shows a line-number gutter on the right-hand (RTL leading-edge)
side, with numbers matching the Responsa `L<N>:word` search syntax
and restarting at 1 for each folio. Copy-paste never picks up the
numbers. Toggle on/off via the line-number button in the
transcription header. Available in web Browse, web Quick View, web
Full Manuscript View, desktop Browse tab, and desktop ResultDialog.

**For web users — small parity win:** Search result cards now show
the page/image number inline after the shelfmark (the same field
the desktop app's `COL_IMG` column shows), so you can tell which
folio a hit came from without opening Quick View.

**Behind the scenes:** The web app now has multitenant safety as a
structural property rather than a per-bug-fix property. The
cross-user xlsx export filename leak fixed in v7.11.1 was one
instance of a class of bugs (search results, lists, exports, auth
state could all bleed between concurrent users on the same server);
v7.12 replaces the entire pattern with intentional primitives:
per-session UUID, single chokepoint adapter for all per-user state,
request-scoped auth that doesn't call `set_session` mid-flight,
real server-side `sign_out` revocation. Architecture documented in
`docs/guides/MULTITENANT.md`. Web-only refactor; desktop unaffected.

### v7.11.2: Composition Search Bug Fixes

Desktop-only patch addressing two user-reported bugs in composition
search (find Genizah manuscripts matching a long source text like a
prayer collection, responsum, or letter).

- **`Min chunks` filter no longer inflated by repeating phrases** — if
  the source text contained the same words multiple times (e.g.
  "ברוך אתה יי" recurring through benedictions and prayers), the
  system counted each repetition as a separate chunk match. A
  manuscript with the phrase only once could wrongly pass
  `Min chunks = 2`. The filter now counts unique chunk *contents*, so
  it matches user expectations.
- **Expanded result view scrolls to the highlight** — opening a
  composition result by double-click now anchors both the source text
  pane and the manuscript text pane to the first match. No more manual
  scrolling through a 70-page source to find the highlighted segment.
  Page navigation in the manuscript pane also re-anchors.

Internal: bundles v7.12 Path B refactor foundations (Phases 87-89) with
zero user-visible change.

### v7.11.1: Desktop Catch-up Release

This release brings the desktop installer up to par with what shipped
to web in v7.11.0 (CUDL Coverage) plus six post-release hotfixes.
Most user-visible benefit for desktop users:

- **108 CUDL-only manuscripts now visible on desktop** — Cambridge
  manuscripts that have no NLI Alma record (including T-S NS 329.96
  and ~100 Mosseri/CUL entries) now appear in search results and the
  catalog browser. Images and metadata only; no transcription text.
- **Cambridge shelfmark search recovery** — alternative shelfmark forms
  (Moss. III,27O ↔ mosseriiii27o, Or. numeric variants, leading zeros)
  now resolve to the same record. Thousands of "missing" Cambridge
  shelfmarks recovered.
- **"View on CUDL" link** — fixed for previously-orphan classmarks.
- **Browse pagination for image-only manuscripts** — Next/Prev now
  works for CUDL manuscripts that have images but no transcription.
- **Desktop comments save** — fixed a bug where Question, Scholarly
  Note, Suggestion, and Issue silently failed to save (only General
  worked).
- **Web fixes also bundled** — /help 500, cross-user export filename
  leak, /browse 500 race after stopped search, browse expanded panel
  silently missing enrichment, lists "Sync Now" UX clarification.

### v7.11.0: CUDL Coverage & Synthetic Inventories

A 3-phase milestone (Phases 84, 85, 86) closing the gap between Cambridge CUDL's ~141K classmark catalogue and GenizahSearch's libraries.csv. Triggered by a user-reported case (`T-S NS 329.96` — present in CUDL with 2 image canvases, missing entirely from the app) that turned out to be representative of thousands of orphan classmarks.

- **CUDL shelfmark normalization** — Mosseri label form (`Moss. III,27O` ↔ `mosseriiii27o`), Cambridge Or. numeric collapse, leading-zero collision audit, slash/comma/dot bug fixes. Recovers thousands of CUDL classmarks already represented in libraries.csv under different forms (96.23% of CUDL coverage now resolves via direct Phase 84 normalization)
- **108 image-bearing synthetic manuscripts** — including T-S NS 329.96. These are FJMS-only inventories that have CUDL canvas images but no NLI Alma record at all. They now appear in search results, browse, the API, and image viewers exactly like real manuscripts, with proper image source attribution
- **New "View on CUDL" link** — works correctly for previously-orphan Mosseri and CUL CUDL shelfmarks that previously fell through to a slug-fallback that 404'd
- **Browse pagination fix** — synthetic manuscripts with CUDL canvases but no transcription text can now navigate Next/Prev through their image pages (both web and desktop). Page combo populates with correct image count
- **Search Text Position dropdown reset** — selecting New Search now properly resets the Anywhere/Start/End/Line-starts/Line-ends dropdown; an active-state chip appears when the dropdown is set to anything other than the default
- **Duplicate "Exclude manuscripts" button removed** — the filter panel's button is now the single source

Synthetic manuscripts are currently read-only (no transcription/joins/comments support) — that's tracked as a Phase 87 follow-up.

### v7.10.0: Search API Public Release

GenizahSearch now exposes a public HTTP/JSON research-automation API over the Genizah corpus. Three documented endpoints — `POST /api/search`, `GET /api/browse`, `POST /api/parallels` — let researchers and AI tools execute keyword/Responsa search, drill down to a single manuscript page with PGP/FJMS/NLI enrichment, and run composition-parallels detection over arbitrary input text.

- **Three documented endpoints** — search (keyword, variants, Responsa, title, shelfmark), browse (stateless manuscript drill-down), parallels (sliding-window chunk-match composition detection)
- **OpenAPI spec at `/api/openapi.json`** — auto-generated from Pydantic models with full request/response schemas
- **Interactive Swagger UI at `/api/docs`** — try-it-now endpoint explorer
- **Stability commitment** — additive changes any time; breaking changes only on major-version releases announced in `CHANGELOG.md`
- **Reference Claude skill** — `skills/cairo-genizah-research/` demonstrates search → browse → rank with throttling and citation guidance
- **Per-IP rate limiting** — 30 req/min per endpoint by default (configurable via `SEARCH_API_RATE_LIMIT`)

See [docs/SEARCH_API.md](docs/SEARCH_API.md) for the full reference, curl examples, error codes, env vars, attribution policy, and citation guidance.

### v7.9.3: Visual Similarity Dialog Fixes

A small web-only patch fixing three usability bugs in the Visual Similarity dialog, all from the same user report.

- **Firefox `Show more` button** — past the first 20 results, the pagination control is now reachable in Firefox (was broken only in Firefox; Chrome was unaffected)
- **Open-in-new-tab works** — Ctrl/Cmd-click and middle-click on a suggestion now open the manuscript in a second tab without losing the list you're browsing
- **Copyable shelfmarks** — manually selecting the suggestion list now includes the shelfmark column (previously excluded because the shelfmark rendered as a button)

### v7.9.2: PGP Data Refresh

Our bundled Princeton Geniza Project data is now current through April 2026. Hundreds of new documents, transcriptions, and scholarly references added.

- **PGP data refreshed** — +147 documents, +159 transcriptions and translations, +211 scholarly footnotes
- **Web browse source buttons** — Oxford/NLI source-toggle restored
- **Desktop Cambridge nav** — crash fix

### v7.9.1: Catalog Attribution & Reading Desk Polish

A data-quality release with targeted bug fixes across FJMS catalog attribution, JTS and Cambridge image alignment, and Reading Desk UX polish on the desktop app.

- **Catalog source attribution** — ~30,000 manuscripts previously showed empty `Catalog Information` dialogs because their source was labeled `Instatution` (a generic placeholder). They now render proper institutional attributions: GRU – Cambridge, Schocken-Zulay Poetry Catalog, The Fleischer Piyut Project, Yad Harav Herzog, Uri Ehrlich, and more (both apps)
- **JTS manuscript images** — ENA manuscripts like `ENA 1052.1` now correctly toggle to their Princeton DPUL catalog images, and navigation between JTS manuscripts is ~5x faster thanks to NLI request timeouts and a circuit breaker (both apps)
- **Cambridge image alignment** — bifolio manuscripts (e.g., T-S NS 158.112) and CUL manuscripts where CUDL canvases are misordered now display the correct leaf for each transcription page, falling back to NLI images automatically when CUDL is unreliable (both apps)
- **Reading Desk polish (desktop)** — Add to View respects typed shelfmark/sys_id in the top bar instead of silently re-adding the current manuscript; the green toolbar is slimmer; fragments added from any source now load their images whether or not the manuscript was browsed earlier in the session; What's New dialog Hebrew alignment is right-edge clean
- **Security & stability** — hardened PostgREST filter sanitization in the desktop Supabase client; unified Supabase config via shared provider; eliminated recurring `parent_slot has been deleted` log spam on web

### v7.9.0: Structural Foundation + Decomposition

Internal refactor — CI pipeline (Ubuntu + Windows matrix), dependency pinning (`requirements.txt` + lock), Supabase auth migration (`gotrue` → `supabase_auth`), `desktop/` package extraction, web page decomposition. Plus back-navigation state-loss bugfix and CUL paired-leaf folio-label fix. Zero user-visible behavior changes except the two bugfixes.

### v7.7.2: PageSpeed Quick Wins (A11y + Perf)

Targeted fixes against Lighthouse findings on the homepage — accessibility 85 → 96, performance 90 → 98.

- **Valid `html lang`** — fixed `<html lang="undefined">` by passing the full Quasar lang pack, plus a JS guard and NiceGUI template patch at startup
- **Aria-labels** — descriptive labels on 10 icon-only buttons (help, dismiss, theme, citation copy/close, hero search)
- **Color contrast (WCAG AA)** — light-theme `--text-muted` and global link color now meet AA; dark-theme overrides for muted text and Quasar primary/secondary/accent tokens
- **`font-display: swap`** — Starlette middleware injects it into NiceGUI's `fonts.css`, preventing ~1200ms of invisible text on slow connections
- **Conditional IIIF preconnect** — only emitted on routes that actually load manuscript images (`/search`, `/browse`, `/puzzle`)

### v7.7.1: SEO Round 2

Bilingual meta tags so the site can rank for Hebrew queries like "חיפוש בגניזה הקהירית" while preserving English brand identity.

- **Bilingual titles/descriptions** — English brand + Hebrew search phrase + Hebrew brand across default and per-page metadata
- **Homepage h1** — now contains target Hebrew search phrases in visible content for crawlers
- **Structured data** — Organization + BreadcrumbList JSON-LD; legacy SearchAction markup retained (Google deprecated Sitelinks Search Box in Nov 2024)
- **Performance** — PostHog deferred past first paint via `requestIdleCallback`; dns-prefetch for analytics CDNs

### v7.7.0: Volume-Aware Browse

Manuscripts with multiple microfilm scans (IEs) — 3,193 items — previously showed images from the wrong volume alongside the transcription text. A new **volume selector** fixes this by letting users switch between scans, each with its own correctly matched images and text.

- **Volume selector dropdown** — switch between scans on multi-IE manuscripts. Single-IE manuscripts are unaffected
- **Volume-correct images** — Manchester, Oxford, Cambridge, and JTS images properly offset per volume
- **Auto-default to available images** — when NLI IIIF is unavailable, automatically loads from Manchester, Cambridge, or JTS
- **Volume-aware community data** — corrections and comments are tagged per volume
- **Both apps** — full feature parity across web and desktop

### v7.6.0: Visual Similarity Suggestions

Researchers can now **discover visually similar manuscripts** using FJMS SVM image analysis data (~15.5M pairs). A new "Visual Similarity" button in the browse page opens a side-by-side workbench showing ranked partners with thumbnails, domains, and action buttons.

- **Browse visual partners** — ranked suggestions with score, domain, and library metadata. Click to browse, add to puzzle, or create a join
- **Search within suggestions** — restrict a text search to the partner pool, with union/intersection modes for multi-manuscript selection
- **Both apps** — full feature parity across web and desktop

### Previous Releases

- **v7.5.0: Exclude Known Manuscripts** — hide already-reviewed manuscripts from search results using saved lists, imported files, or pasted shelfmarks
- **v7.4.0: Search Within Results** — progressively narrow search results by searching within your current result set
- **v7.2.0: Image Adjustment Controls** — brightness / contrast / gamma / invert on all image viewers
- **v7.1.0: FIST Gap Fill** — 255,615 manuscript records (+18%), 7 new library codes
- **v7.0.0: Fragment Puzzle** — visual canvas for arranging fragments, community publishing via Discoveries Center

### v6.5.0: Search UX & Filtered Search

* **Focused Search:** Filter manuscripts by domain, author, work, date range, and material type before searching — across all search modes (both apps)
* **Dicta Translation:** ~924K machine translations for catalog data, titles, and scholarly descriptions (Hebrew↔English) via Dicta Translation API, with translation QA and user reporting
* **Translation Toggle:** Show/hide translated text in search results, browse views, and catalog dialogs with clickable Translated/Original badges
* **Line-Boundary Search:** Find words at start/end of lines or text — useful for detecting joins between fragments. Position dropdown + per-word constraints in Responsa mode
* **Browse-to-Search:** Domain and author labels on Browse page link directly to a focused search
* **Citation Reminder:** One-time popup reminding users to cite MiDRASH when publishing

### Previous Features (v5.0–v6.2)

* **Power-User UX (v6.2):** Composition search UX (timer, ETA, cancel with partial results), session persistence, search history, desktop notifications, Hebrew library names, copy context menu
* **Line-Break Search (v6.2.3):** Consecutive-line search in Responsa mode with `|` syntax and line gap notation

* **Catalog Browse (v6.1):** Browse by domain, author, and work with free-text filtering, cross-links, and FIST v5.0 enrichment
* **Local Data Architecture (v6.0):** PGP data in local SQLite sidecar, offline browsing, paginated search, desktop crash fixes, performance optimizations

* **Multi-Source Images (v5.9):** NLI, Cambridge, Manchester LUNA, JTS/Princeton Figgy with folio navigation, bibliography (542K), catalog cross-references (64K)
* **FJMS Integration (v5.8):** Domain classifications, scientific joins, and catalog enrichment from FIST.db via SQLite sidecar
* **Responsa Search (v5.7):** Advanced search with Responsa-Project style syntax, grammatical expansion, Judeo-Arabic support, and tabular query builder
* **Princeton Geniza Project (PGP):** 35,839 curated documents with transcriptions, translations, and metadata
* **Virtual Reading Desk:** Multi-manuscript synchronized viewer for related fragments
* **PGP Tag Search:** 251 tags in 16 categories for thematic browsing
* **Web Platform:** [genizahsearch.com](https://genizahsearch.com) — full-featured web access from any browser
* **Community Features:** Discoveries, comments, corrections — collaborate with researchers worldwide
* **Cross-Paragraph Search:** Find text spanning paragraph boundaries, filtering out common citations
* **Cloud Sync:** Automatic list sync across devices
* **In-App Updates:** Desktop self-updates from GitHub Releases

---

## Core Features

### Integrated Visual Analysis (IIIF)

* **In-App Viewer:** High-resolution images from NLI, Cambridge, Manchester LUNA, and JTS/Princeton Figgy
* **Multi-Source Toggle:** Switch between image sources with colored source chips
* **Folio Navigation:** Page-level navigation with scholarly recto/verso notation
* **Image Tools:** Zoom and rotation controls

### Oxford Bodleian Integration

* Full support with **Neubauer catalog** integration
* Part-based and folio-based navigation
* Rich metadata display

### Lab Mode (Experimental)

Parallel detection based on **Shmidman, Koppel, and Porat (2016)**.

* Rare letter encoding for spelling variation tolerance
* Deep scan for complex queries

### Personal Lists

* Create and organize manuscript collections
* Browse and filter by custom lists

---

## Additional Capabilities

* **Cross-Page Search:** Results span page boundaries
* **Enhanced Export:** Excel, CSV, DOCX with selection support
* **Find in Text:** Quick search with highlighting
* **Composition Search:** Detect parallels using chunk analysis

---

## API

GenizahSearch exposes a public HTTP/JSON API for research automation: keyword/Responsa search (`POST /api/search`), single-manuscript drill-down (`GET /api/browse`), and composition-parallels detection (`POST /api/parallels`). All endpoints are anonymous and rate-limited (30 req/min per endpoint per IP in the default public deployment) and return JSON in a uniform envelope.

Full reference, curl examples, and interactive Swagger UI: [docs/SEARCH_API.md](docs/SEARCH_API.md) · [genizahsearch.com/api/docs](https://genizahsearch.com/api/docs).

---

## Getting Started

### Web (Recommended)

Visit [genizahsearch.com](https://genizahsearch.com) to start using Dicta Genizah Search immediately.

### Desktop Installation

1. **Download:** Get `GenizahSearchPro_V8.0.0_Setup.exe` from the **Assets** section
2. **Install:** Run the installer and follow instructions
3. **Data Setup:** The software requires the **MiDRASH** dataset (`Transcriptions.txt`)

> **Antivirus Note:** Some antivirus software (Avast, AVG, Windows Defender) may flag the installer as suspicious. These are **false positives** caused by PyInstaller packaging. See [ANTIVIRUS_INFO.txt](ANTIVIRUS_INFO.txt) for details and solutions.

---

## Documentation

For detailed documentation, see the [docs/](docs/DOCUMENTATION_INDEX.md) directory:

* **[Documentation Index](docs/DOCUMENTATION_INDEX.md)** - Overview of all documentation
* **[Admin Guide](docs/guides/WEBSITE_ADMIN_GUIDE.md)** - Website management for administrators
* **[Code Index](docs/CODE_INDEX.md)** - Code structure and architecture
* **[Plans](docs/plans/)** - Implementation plans and roadmaps

---

## Credits & Data

* **Hosted & Supported by [Dicta — The Israel Center for Text Analysis](https://dicta.org.il/)**
* **Development:** Hillel Gershuni
* **Data Sources:**
  - Stoekl Ben Ezra, D., Bambaci, L., Kiessling, B., Lapin, H., Ezer, N., Lolli, E., Rustow, M., Dershowitz, N., Kurar Barakat, B., Gogawale, S., Shmidman, A., Lavee, M., Siew, T., Raziel Kretzmer, V., Vasyutinsky Shapira, D., Olszowy-Schlanger, J., & Gila, Y. (2025). *MiDRASH Automatic Transcriptions of the Cairo Geniza Fragments* [Data set]. Zenodo. [doi.org/10.5281/zenodo.17734473](https://doi.org/10.5281/zenodo.17734473)
  - [Princeton Geniza Project (PGP)](https://geniza.princeton.edu/) — Curated transcriptions, translations, and metadata for Cairo Genizah documents
  - Fragment of the Jewish Manuscript Studies (FJMS) — Domain classifications, scientific joins, bibliography, catalog records, joins and visual joins suggestions, and much more.
* **Lab Mode Algorithm:** Based on [Shmidman, Koppel, and Porat (2016)](https://arxiv.org/abs/1602.08715)

**Acknowledgments:**
Assisted by **Claude**, **Gemini**, and **GPT**.
Special thanks to Avi Shmidman, Elisha Rosenzweig, Efraim Meiri, Elazar Gershuni, Itai Kagan, Elnatan Chen, and Adiel Breuer.

