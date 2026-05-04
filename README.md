# Genizah Search Pro 7.9.4

**Collaborative Research Platform for the Cairo Genizah**

A comprehensive research environment for the Cairo Genizah, featuring a **Web Platform** ([Dicta Genizah Search](https://genizahsearch.com)), **Community Features**, and full **WCAG 2.0 Accessibility**.

> **Web Access:** [genizahsearch.com](https://genizahsearch.com) (אתר הגניזה של דיקטה) - Search, browse, and collaborate from any browser

---

## What's New in Version 7.9.3?

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

## Getting Started

### Web (Recommended)

Visit [genizahsearch.com](https://genizahsearch.com) to start using Genizah Search Pro immediately.

### Desktop Installation

1. **Download:** Get `GenizahSearchPro_V7.9.4_Setup.exe` from the **Assets** section
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

