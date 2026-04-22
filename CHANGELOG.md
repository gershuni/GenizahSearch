# Changelog

All notable changes to Genizah Search Pro will be documented in this file.

---

## [7.9.2] - PGP Data Refresh - 2026-04-22

Refreshes our bundled Princeton Geniza Project metadata (last imported February). Plus two small post-7.9.1 fixes.

### Data

- **PGP metadata refresh**: re-imported from [princetongenizalab/pgp-metadata](https://github.com/princetongenizalab/pgp-metadata). +147 documents, +159 source editions/translations, +211 footnotes, +345 fragment links. `pgp.db` rebuilt (148.6 MB).

### Bug Fixes

- **Web browse source buttons**: Oxford and NLI source-toggle buttons on `/browse` restored (regressed post-7.9.1). (web)
- **Desktop Cambridge nav**: fixed undefined `page_idx` in the Cambridge nav helper (ruff F821). (desktop)

---

## [7.9.1] - Catalog Attribution & Reading Desk Polish - 2026-04-22

Data-quality fixes across FJMS source attribution, JTS and Cambridge image alignment, plus Reading Desk UX polish on the desktop. Also ships Phase 64/65 code-review follow-ups, security hardening, and web log hygiene.

### Bug Fixes

- **FJMS empty catalog dialogs**: ~30K manuscripts that previously rendered empty `Catalog Information` dialogs (source label was `Instatution` — an export typo suppressed by 5 of 6 `GENERIC_SOURCE_NAMES` consumers) now show real institutional attributions. Local `CODE_Institution` join rewrote 267,104 `catalog` rows and 47,800 `catalog_free_desc` rows across 8 SubIds — top by volume: GRU – Cambridge (161K), Schocken-Zulay (51K), Fleischer Piyut Project (30K), Yad Harav Herzog (15K), Uri Ehrlich (8K). 100% resolution; 4 new regression tests. (both apps)
- **JTS browse source-switch button**: ENA manuscripts like `ENA 1052.1` (sys_id 990053572370205171) couldn't toggle to Princeton DPUL images. Stack of 4 bugs: (1) MARC 942$z parser now prefers first non-numeric value, avoiding FGP photo-ID clobber of the real shelfmark; (2) new `_jts_shelfmark_variants` helper tolerates `Ms.`/`MS.` prefix mismatch; (3) new `get_jts_urls_for_sys_id(sys_id)` resolves via `nli_images.Shelfmark ↔ jts_dpul.shelfmark` JOIN in one query, replacing up to 16 variant lookups; (4) NLI IIIF/MARC/Figgy timeouts tightened 10s→5s + per-sys_id negative cache + class-level circuit breaker (3 consecutive NLI failures → 60s skip), cutting JTS navigation lag ~25s → ~5s per hop. (both apps)
- **CUL CUDL/NLI image alignment**: Bifolio, binding-canvas, and count-matched-but-order-mismatched CUDL manifests (e.g., T-S NS 158.112 with 14 transcription pages vs 12 CUDL canvases; Or.2245 with same-count but index-2 divergence) could display a wrong-leaf image. New `classify_cambridge_alignment()` helper returns `aligned | misaligned | unknown` + reason; `enrich_metadata` computes the verdict once and 5 previously-duplicated decision sites consume it, defaulting the whole manuscript to NLI when misaligned. CUL-only scope preserves non-CUL Cambridge (Mosseri, Gaster, private CUDL). 7 new regression tests. (both apps)
- **CUL paired-leaf folio labels**: `_FOLIO_PATTERN` now accepts `L{first}_{second}F...S{side}` bifolio notation — bifolio CUL manuscripts now show folio labels (1r, 1v, 2r, 2v) in the picker instead of flat page numbers. (both apps)
- **Desktop past-CUDL auto-fallback**: Viewer auto-flips to NLI images when navigating past the last CUDL page, auto-restores CUDL on return. Also disambiguates `folio_num` vs 1-indexed `page_num` across 6 image-index call sites. 18 regression tests. (desktop)
- **Reading Desk "No images available"**: Fragments added from a list, the top shelfmark field, or the green-bar input that hadn't been browsed earlier in the session showed no images, because the viewer reads from `meta_mgr.nli_cache[sid]` which is only populated by metadata enrichment. New `_browse_rd_enrich_entry(sys_id, volume_ie=None)` helper launches enrichment on add and re-renders on completion. Threads deduped in a sys_id-keyed dict with `wait(1500)`-then-terminate on exit and window close. Volume-aware: when the added fragment matches the currently-browsed manuscript, its `volume_ie` is carried through so multi-IE fragments backfill the correct volume. (desktop)
- **Reading Desk green toolbar too tall**: Green bar used to stretch vertically because its `QWidget` used the default size policy. Now `QSizePolicy(Preferred, Fixed)` pins it to its inner scroll-row height; margins/padding also slimmed. (desktop)
- **Reading Desk "Add to View" ignored typed input**: Clicking Add to View with a new shelfmark typed in the top bar silently re-added the currently-loaded manuscript. Now resolves typed shelfmark/sys_id via `resolve_system_by_shelfmark` (with multi-match dialog + not-found warning) and adds that target. (desktop)
- **Reading Desk field empty on entry**: Green toolbar's shelfmark input was empty when the desk opened. Now pre-populates with the last-added entry so the user can tweak and press Enter to add a variant. (desktop)
- **What's New dialog RTL alignment**: Hebrew bullet paragraphs were left-edge aligned. Now the wrapping `<div>` carries `dir='rtl' align='right'` and the `QLabel` is set to `AlignRight | AlignTop` in Hebrew. (desktop)
- **Banner auto-dismiss ordering**: Site-wide "What's New" banner and `/home` OCR disclaimer previously persisted the dismissed flag before deleting the banner, so navigating away within 10s permanently hid the banner on next reload without user interaction. Now the flag is persisted only on successful `.delete()`. (web)

### Improvements

- **FJMS catalog within-source dedup**: Duplicate rows from the same SourceName no longer appear as separate entries. (desktop)
- **Web `/_nicegui/` framework assets marked noindex**: `X-Robots-Tag: noindex` header so internal ESM/module URLs don't surface as Search Console "pages". Still crawlable for rendering. (web)
- **Journalctl log hygiene**: 3 `ui.timer()` callsites in ephemeral containers (What's New banner, OCR disclaimer, `/home` carousel) converted to `asyncio.call_later` / `ensure_future`, eliminating recurring `RuntimeError: The parent slot of the element has been deleted` spam. (web)

### Security

- **PostgREST `.or_()` ilike sanitization**: Added `_sanitize_ilike_pattern(value)` helper that strips PostgREST filter separators (`,` `(` `)` `*`), LIKE wildcards (`%` `_`), backslashes, and newlines. Applied at 4 `supabase_corrections_client.py` callsites: `list_corrections`, `get_connected_fragments`, `list_all_fragment_joins`, `list_user_fragment_joins`. Closes Phase 64 CR-02. (desktop)
- **Supabase config unified via shared provider**: `supabase_corrections_client.py` now imports `SUPABASE_URL` / `SUPABASE_ANON_KEY` from `shared/supabase_provider.py`. Provider opportunistically calls `load_dotenv()` at import (non-fatal if missing), so desktop entry points pick up `.env` without their own call. Closes Phase 64 CR-01. (desktop)

### Data

- **`fjms_enrichment.db` upload** (1.6 GB, 2026-04-21 build): bundles the Instatution migration above plus Shivtiel transliteration fix (ת→ט across 14,608 rows), Sussmann Supplement translation (111 rows), and 36 empty "צוות 500" placeholder records removed.

### Internal

- **`/_internal/memstat`** diagnostic endpoint (gated by `MEMSTAT_SECRET` header) added for leak investigation post-v7.9.0. (web)
- **Phase 65 code-review cleanup**: WR-01 `import re as _re` moved to top of `web/main.py`; WR-02 misleading exception comment in `puzzle_tokens.py:verify_upload_token` corrected; IN-01 redundant `except (AssertionError, Exception)` simplified in `auth_state.py`; IN-02 NiceGUI upgrade threshold extracted to `_PATCH_AUDIT_THRESHOLD` constant with module-load WARNING on exceed.

---

## [7.9.0] - Structural Foundation + Decomposition - 2026-04-19

Bundles the two internal GSD milestones `v7.8` (Structural Foundation, 2026-04-15) and `v7.9` (Decomposition, 2026-04-17) into a shippable release, plus a back-navigation bugfix caught during Phase 75 verification and a CUL paired-leaf folio-label fix. Zero user-visible behavior changes except the two bug fixes below.

### Bug Fixes
- **Back-navigation state loss**: browser Back from `/browse` to `/search` now restores the saved snapshot; regression introduced 2026-03-27 (commit `829cd7cf`) and fixed 2026-04-17 (commit `8f9c5ef3`). Caught during Phase 75 non-regression verification.
- **CUL paired-leaf folio labels**: `parse_folio_label` now handles paired-leaf bifolio `ImageName` patterns (e.g., T-S NS 158.112), so image-text alignment is correct for these manuscripts.

### Internal: v7.9 Decomposition (milestone complete 2026-04-17)
10 phases, 23 plans. Zero user-visible behavior changes.
- **Desktop split**: `ResultDialog`, filter/scholarly dialogs, image viewers (`ManuscriptViewerWidget`, `FullscreenImageWindow`), puzzle canvas, VS cache, and shared widgets extracted into a new `desktop/` package — `genizah_app.py` slimmer (though remaining core still ~22.5K lines per external review)
- **Web split**: `web/pages/search.py` decomposed into `search_state.py` + `search_results.py`; `web/pages/browse.py` decomposed into `browse_state.py` + `browse_enrichment.py`
- **Page-scoped state refactor**: reduced `app.storage.user` sprawl and detached `asyncio.ensure_future` usage in search and browse pages
- **Documentation**: `docs/CODE_INDEX.md` v7.9 section regenerated via new `scripts/gen_code_index_section.py` AST generator; `check_docs` green

### Internal: v7.8 Structural Foundation (milestone complete 2026-04-15)
4 phases, 9 plans, 64 commits, 173 files changed (+6,269/-828 lines). Zero user-visible behavior changes. 12/12 requirements satisfied.
- **CI safety net**: GitHub Actions with Ubuntu + Windows matrix running `ruff` + `check_docs` + `pytest` (`.github/workflows/ci.yml`)
- **Dependency pinning**: `requirements.txt` (14 direct) + `requirements-lock.txt` (115 transitive) for reproducible builds
- **Supabase auth migration**: deprecated `gotrue` error surface replaced with `supabase_auth`; PKCE-only OAuth flow
- **Silent-exception audit**: 205+ `except: pass` handlers reviewed across 76 first-party files
- **Framework-patch isolation**: NiceGUI monkey-patches moved into `web/framework_patches.py` with version guards and logging
- **Repo hygiene**: `.gitignore` 50 → 126 lines; untracked root files 67 → 1; CODE_INDEX / OPEN_ISSUES / DEVELOPER_GUIDE docs refreshed

### Build / Release Tooling
- **`scripts/bump_version.py`**: added README installer-filename regex so `GenizahSearchPro_V<X.Y.Z>_Setup.exe` stays in sync on future bumps. Closes long-standing drift (filename had been stale since v6.1.1, eight releases).

---

## [7.7.2] - PageSpeed Quick Wins (A11y + Perf) - 2026-04-13

### Accessibility
- **Valid html lang attribute**: Fixed Lighthouse "Document does not have a valid `lang` attribute" by passing full Quasar lang pack (with `isoName`) to `Quasar.lang.set()` instead of partial `{rtl: false}` object, which caused `<html lang="undefined">`. Added JS belt-and-braces guard + NiceGUI template patch at startup (web)
- **Aria-labels**: Added descriptive `aria-label` to 10 icon-only buttons (help, dismiss, theme toggles, citation copy/close, OCR banner dismiss, hero search) (web)
- **Color contrast (WCAG AA)**: Light-theme `--text-muted` `#94a3b8` → `#64748b` (2.34:1 → 4.63:1); global link color `--primary-700` replaces Quasar default `#5898d4` (3.06:1 → 5.44:1); dark-theme overrides for `--text-muted`, `--primary-600/700`, and Quasar primary/secondary/accent tokens so inline links and badges meet AA on dark backgrounds (web)
- **Heading hierarchy**: Homepage "What is the Cairo Genizah?" promoted from `h3` to `h2` (web)

### Performance
- **font-display: swap**: Starlette middleware injects `font-display: swap` into NiceGUI's `fonts.css` response, preventing ~1200ms of invisible text on slow connections (web)
- **Conditional iiif preconnect**: `<link rel="preconnect" href="https://iiif.nli.org.il">` now only emitted on routes that actually load manuscript images (`/search`, `/browse`, `/puzzle`) instead of every page (web)

### Results (Lighthouse desktop, homepage)
- Accessibility: 85 → 96 (target ≥95)
- Performance: 90 → 98 (target ≥93)
- SEO: 100 (unchanged)
- Remaining: 13 parchment-theme color-contrast warnings (same root cause as dark-theme fix, deferred — a11y still above target)

---

## [7.7.1] - SEO Round 2 - 2026-04-13

### Improvements
- **Bilingual meta tags**: Default title now bilingual (English brand + Hebrew search phrase + Hebrew brand) so the site can rank for Hebrew queries like "חיפוש בגניזה הקהירית" while preserving English brand identity. Description, keywords, and JSON-LD alternateName also include both languages (web)
- **Per-page titles**: Indexable pages (`/browse`, `/catalog-browse`, `/about`) use English-leading bilingual format; manuscript pages stay shelfmark-first; low-intent pages (`/help`, `/download`, `/accessibility`) restored to original concise English (web)
- **Homepage h1**: Updated to "אתר הגניזה של דיקטה — חיפוש בגניזה הקהירית" (in Hebrew UI mode) so target search phrases appear in visible above-the-fold content for crawlers (web)
- **Structured data**: Added Organization JSON-LD on homepage and BreadcrumbList JSON-LD on browse manuscript pages. Also added Sitelinks Search Box (SearchAction) markup — note: Google deprecated this surface in November 2024, but markup is harmless and may be used elsewhere (web)
- **Performance**: PostHog analytics deferred past first paint via requestIdleCallback; dns-prefetch hints added for analytics CDNs (web)
- **Title consistency fix**: Client-side intra-app navigation on `/browse` no longer overrides server-rendered title to a different format (web)

### Known Limitations
- Real performance measurement (Lighthouse / PageSpeed Insights / Search Console) was NOT performed in this release — placeholder analysis only. Schedule a follow-up with real Search Console URL Inspection and PSI runs after deploy.
- Site is single-URL bilingual (no `/he/` vs `/en/` routes, no hreflang). Google sees Hebrew as the default rendered language because UI defaults to Hebrew. Future phase: per-language URLs with hreflang.
- Browse and catalog page bodies are mostly empty until WebSocket hydration — limits crawlable text content. Tracked in `docs/OPEN_ISSUES.md`.

---

## [7.7.0] - Volume-Aware Browse - 2026-04-01

### New Features
- **Volume-aware browsing**: 3,193 multi-IE manuscripts now show a volume selector, allowing users to switch between different microfilm scans (IEs) with correct text and images per volume (both apps)
- **Volume-specific community data**: Corrections and comments are tagged with the active IE, so notes on Volume 2 only appear when browsing Volume 2 (legacy data with no IE tag still shows everywhere)
- **Auto-default to external image sources**: Manchester LUNA, Cambridge IIIF, and JTS/Princeton DPUL images now auto-load when available, instead of waiting for manual source switching (web)

### Improvements
- **Volume-correct external images**: Manchester and Oxford images properly offset by volume — Volume 2 shows the correct folio, not Volume 1's images (both apps)
- **Desktop external image filtering**: Manchester/Cambridge/JTS canvases filtered to active volume's pages instead of showing all volumes' images (desktop)
- **Volume page counts**: Volume selector shows actual transcription page count per volume instead of total IIIF manifest pages (both apps)
- **Thread-safe browse_map loading**: `threading.Lock` replaces boolean flag to prevent concurrent pickle read/write race conditions
- **Browse_map IE repair**: Automatically restores pages from non-primary IEs that were lost by pre-v7.7 deduplication, with UID format correction for Tantivy index compatibility

### Bug Fixes
- **Multi-IE image/text mismatch**: Browse page showed images from Volume 1 regardless of which volume's text was displayed — now each volume shows its own matching images
- **Browse_map pickle corruption**: Multiple SearchEngine instances loading simultaneously no longer causes "pickle data was truncated" errors
- **Notes/comments ie_id gap**: `create_notes_panel` and `create_notes_button` now pass volume IE to comment queries

---

## [7.6.0] - Visual Similarity Suggestions - 2026-03-31

### New Features
- **Visual Similarity browse dialog**: New "Visual Similarity" button in the browse page opens a side-by-side workbench showing ranked image-similarity partners from FJMS SVM analysis (~15.5M pairs). Each suggestion shows a thumbnail, shelfmark, domain, library, and score — with Browse, Puzzle, and "Add as Join" action buttons (both apps)
- **Search in visual suggestions**: From the VS dialog, restrict a text search to the suggestion partner pool. Supports union (any manuscript's partners) and intersection (shared partners only) modes for multi-manuscript selection (both apps)

### Improvements
- **VS text snippet preloading**: First 20 suggestions preload text snippets in the background for instant preview
- **VS performance**: 4 regressions fixed from Codex audit — batch enrichment, lazy loading, query optimization
- **Exclusion dialog "Active exclusions" section**: Clicking the "Exclude Manuscripts" button now shows currently active sources with per-source remove buttons and "Clear all"
- **New Search clears exclusions**: The reset button now properly clears exclusion sources and their persisted storage

### Bug Fixes
- **Exclusion chip persisted after New Search**: "Exclude Manuscripts (N)" button remained visible after clicking "New Search" — now properly cleared
- **VS dialog scrolling**: Fixed web VS dialog right pane not scrolling to show all suggestions
- **VS dark mode**: Fixed background colors and search URL parameters in dark mode
- **Desktop VS puzzle integration**: Fixed add_to_puzzle method call from VS dialog

---

## [7.5.0] - Exclude Known Manuscripts - 2026-03-29

### New Features
- **Exclude known manuscripts**: Hide already-reviewed manuscripts from search results using saved lists, imported shelfmark files (TXT/CSV), or pasted shelfmarks. Multi-source tracking with per-source clear, collapsible excluded section showing what was hidden and why, resolution report table with per-row status (found/not found/duplicate). Available in both web and desktop apps
- **Web exclusion dialog**: Tabbed picker with "Paste Shelfmarks" (default), "From List" (expandable with per-manuscript checkboxes), and "From File" (upload with resolution preview). Three entry points: results header button, post-search filter panel, and "Search only in..." pre-search panel
- **Desktop exclusion dialog**: Enhanced ExcludeDialog with QTabWidget — "From File / Manual" tab with resolution report table and "From List" tab with "Load to Editor" workflow for review before applying

### Improvements
- **Desktop exclusion by row hiding**: Uses QTableWidget `setRowHidden()` to instantly toggle visibility without re-rendering — preserves enrichment state (domain badges, printed indicators, scroll position)
- **Export respects exclusions**: Both desktop and web exports (Excel/CSV/Word) now only include visible (non-excluded) manuscripts (WYSIWYG export)
- **Session persistence**: Exclusion sources survive page navigation and session restore, with backward compatibility for legacy exclusion state
- **38 Hebrew translations**: Full bilingual coverage for all exclusion UI strings

### Bug Fixes
- **Bracket-aware search**: Searching `נשתנה` now finds `]נשתנה` (bracket-transparent matching)

---

## [7.4.0] - Search Within Results - 2026-03-29

### New Features
- **Search within results**: Progressive refinement — run a second query restricted to manuscripts from your current result set. Breadcrumb chip chain shows refinement history with per-chip removal, cross-mode support (text, Responsa, Title, Shelfmark), "Only results with all terms" page-level filter, and chain-aware snippet highlighting
- **Lightweight browse first-render**: Browse page first paint now uses only Tantivy + csv_bank (zero SQLite calls). Crossref, Oxford, Cambridge, and attribution data load asynchronously in Phase B enrichment — faster first content for users and 255K sitemap URLs for crawlers

### Improvements
- **Thread-safe SQLite services**: All shared services (FJMS, NLI, PGP, Translation) now use per-thread connections for safer concurrent access
- **NiceGUI ESM handler hardening**: Patched ESM static file handler to reject directory path traversal

### Bug Fixes
- **Bracket-aware search**: Searching `נשתנה` now finds `]נשתנה` (bracket-transparent matching). Searching `]נשתנה` still matches only the bracketed form (literal match). Two-layer fix: Tantivy OR-expansion with bracket variants for candidate recall, plus conditional bracket stripping in regex phase. Correctly handles Responsa `[N]`/`[|N]` gap operators, position filters (start/end/line), and composition search
- **csv_bank race condition**: Fixed `dictionary changed size during iteration` error in metadata search under concurrent access
- **Browse stale enrichment guard**: Generation counter re-checked after deferred Oxford translation fetch to prevent stale state on rapid navigation
- **Metadata-only page state**: Records without transcription text now correctly show page 1 with folio label after enrichment (was stuck at page 0)

---

## [7.3.1] - SEO Foundation & Shareable Browse URLs - 2026-03-27

### New Features
- **Shareable browse URLs**: Browser URL bar now updates to reflect the current manuscript and page (e.g., `/browse?sys_id=...&page=2`) as you navigate, so links can be copied and shared. Preserves highlight terms within the same manuscript, clears them on manuscript change
- **Share button**: New share icon in the manuscript header toolbar copies the current URL to clipboard with visual toast feedback

### Improvements
- **Per-page metadata**: Every web route now has a unique title, description, canonical URL, and OG/Twitter tags (replaced global META_TAGS that pointed every page's canonical at the homepage)
- **Manuscript-specific metadata**: Browse pages with sys_id resolve the real shelfmark for title/description/canonical (e.g., "T-S 12.123 — Manuscript | Dicta Genizah Search")
- **Manuscript sitemap**: Sitemap index with ~255K manuscript URLs in 40K-per-file chunks, replacing the previous 11-URL static sitemap
- **Indexability policy**: Search, parallels, lists, settings, corrections, admin, and profile pages marked `noindex, follow`; robots.txt aligned
- **Homepage structured data**: WebSite JSON-LD with publisher, bilingual name, and languages
- **Performance hints**: Preconnect for NLI IIIF server, DNS prefetch for Cambridge CUDL

---

## [7.3.0] - Manuscript Measurements, Bibliography Cleanup & Desktop Stability - 2026-03-26

### New Features
- **Manuscript Measurements dialog**: New "Measurements" button in browse (web + desktop) opens a detailed dialog showing physical dimensions, margins, line counts, text density, material, and DPI quality — per-image data from 434K computed measurements and 179K catalog size records across 231K manuscripts

### Improvements
- **Bibliography dedup**: Removed ~401K duplicate bibliography entries (828K → 427K, 48.4% reduction) via exact and near-dupe merge passes
- **55K new Hebrew translations**: English free descriptions (FreeDesc) now available in Hebrew via Dicta Translation, with hallucination filtering
- **Persistent NLI FL-ID cache**: Cache survives service restarts, reducing repeated IIIF manifest lookups
- **NLI concurrent fetches**: Default bumped from 4 to 8 for faster image loading

### Bug Fixes
- **Desktop browse tab crash**: Fixed crash when rapidly navigating between manuscripts — added 150ms navigation debounce, generation guard, and proper QThread lifecycle for image loaders
- **Desktop image thread lifecycle**: Threads now properly terminate on teardown instead of being dropped, preventing orphaned workers
- **ResultDialog image threads**: Wait-or-terminate pattern prevents thread leaks when closing dialogs
- **Profiles FK join**: Reverted broken foreign key join in community comments/responses, using batch lookup instead

---

## [7.2.4] - JTS Image Upgrade + Shelfmark Search Fixes - 2026-03-25

### New Features
- **Princeton DPUL as primary JTS image source**: JTS manuscripts now auto-default to Princeton Digital PUL images (36,283 items via full DPUL catalog import v2), replacing unreliable manifest-based loading
- **Desktop "Printed" badge**: ResultDialog info row and browse tab now show "Printed"/"דפוס" badge for printed materials, with per-session FJMS cache
- **Blue mat auto-detection**: Background removal now auto-detects blue conservation mats across all libraries instead of hardcoding CUL only

### Improvements
- **Enhanced shelfmark lookup**: Strips full library names ("Cambridge University Library", "British Library") not just codes; ENA-MS/ENA MS normalized to ENA for JTS variant matching
- **FJMS bibliography enrichment**: 8 new FIST fields (JournalVolumeTxt, Hebrew title support), fixed volume source attribution
- **Deduplicated catalog descriptions**: FJMS catalog free descriptions no longer show duplicates in get_catalog_detail()
- **Search performance**: Removed duplicate search enrichment pass and redundant background FJMS prewarm
- **Shared JS extraction**: Filter panel and manuscript viewer JavaScript extracted into reusable modules (~1,050 lines deduplicated)

### Bug Fixes
- **Browse shelfmark search**: Fixed slot context, stale content, and async caller issues
- **PostHog UX & auth**: Rageclick prevention (immediate button disable), OAuth implicit flow fix, login tracking enrichment, login dialog for anonymous write actions
- **JTS external link**: Now points to DPUL catalog page instead of raw manifest URL
- **Puzzle from ResultDialog**: Fixed add-to-puzzle using correct viewer image list, proper shelfmark/fl_id, and auto-close dialog
- **Puzzle external fragments**: Fixed session restore, metadata persistence, NLI timeout skip for non-NLI libraries, NiceGUI context loss on add-from-browse
- **Puzzle extension banner**: Fixed English text showing in Hebrew UI
- **Image viewer**: Guarded viewer.init() call in handleImageError for viewers without init

### Tests
- Fixed puzzle model tests (join_type default changed to 'physical')
- Fixed responsa explosion guard tests (Hebrew tr() warning assertions)

---

## [7.2.3] - Chrome Extension Live + Puzzle Enabled - 2026-03-20

### New Features
- **Chrome Web Store install link**: Puzzle extension install banner now includes a clickable "Install Extension" / "התקינו את התוסף" button linking to the live Chrome Web Store listing
- **Puzzle enabled by default**: `WEB_PUZZLE_ENABLED` now defaults to `True` — the Fragment Puzzle page is available on all deployments without setting an environment variable

### Improvements
- **Banner text escaping**: Switched from manual `.replace()` to `json.dumps()` for safer JS string injection in puzzle banner texts

---

## [7.2.2] - Desktop Browse Tab Polish - 2026-03-20

### Improvements
- **Browse button icons**: All browse tab buttons now have emoji icons matching ResultDialog style (Puzzle, Parallels, List, Info, View on Ktiv, View Corrections, Add to View)
- **Reorganized action row**: Action buttons (Puzzle, Parallels, List) moved from crowded top bar to dedicated ext_info_row alongside Info, Bibliography, and Catalog buttons
- **External library links**: New button shows Cambridge/Oxford/Manchester/Princeton link dynamically based on manuscript source, with globe icon
- **Compact translations toggle**: Translations ON/OFF replaced with a colored icon button (green when ON, grey when OFF) with tooltip
- **Cross-shelfmark page navigation**: Prev/Next page buttons no longer disable at manuscript boundaries — navigating past last page wraps to first page of next shelfmark, and vice versa (matching ResultDialog behavior)
- **Extended info state preserved**: Extended info panel open/close state remembered when navigating between shelfmarks
- **ResultDialog image toggle**: Hide/show image state preserved when navigating between search results
- **Fullscreen image viewer**: New fullscreen mode for manuscript images (both Browse and ResultDialog) — zoom/pan, rotation, brightness/contrast/gamma/invert, page navigation with arrow keys or buttons, Escape to close

### Bug Fixes
- **Enrichment race condition**: Fixed stale Oxford metadata appearing on RNL manuscripts — `browse_load()` did not clear `current_browse_part_id` when loading non-Part manuscripts, causing Oxford Part context to leak into subsequent enrichment callbacks
- **Centralized enrichment launch**: All 4 browse enrichment paths consolidated into `_start_browse_enrichment()` with generation counter to reject stale queued cross-thread signals
- **Lambda disconnect fix**: Stored lambda slot reference for proper `disconnect()` instead of trying to disconnect bare method (no-op)
- **Ext-info restore flag lifecycle**: Flag survives from enrichment to PGP callback for PGP-only manuscripts; cleared unconditionally at final async step to prevent forward leak

---

## [7.2.1] - Search UX Overhaul - 2026-03-19

### New Features
- **Hero search bar**: Prominent search input on home page below "What is the Cairo Genizah?" card, with Hebrew translation ("חיפוש בכתבי יד...")
- **Inline accordion results**: Replaced the 35/65 splitter layout with full-width results. Clicking a result expands an inline accordion showing manuscript thumbnail image + highlighted full text with original line breaks
- **Result card action buttons**: Browse, Quick View (renamed from Advanced View / מבט מהיר), Add to List, and Catalog Records buttons directly on each result card
- **Citation footer auto-collapse**: Full citation shows for 10 seconds, then collapses to a compact single line with copy button
- **Thumbnail images**: New `width` parameter on `/api/nli_image_by_sysid/` endpoint — accordion requests 300px thumbnails instead of 2000px full images
- **Lazy text loading**: On session restore, full text loads on first accordion expand via `get_browse_page()`
- **Clickable thumbnails**: Clicking the accordion image opens Quick View dialog
- **Progressive image loading**: All web image viewers (browse standard/fullscreen, Quick View normal/fullscreen) now show a spinner → 400px thumbnail → 2000px full resolution. CSS `.img-loading-container` spinner + JS `progressiveLoad()` upgrade chain. Desktop already had this pattern.

### Bug Fixes
- **Homepage search stuck**: `_after_delay()` deferred tasks ran in a separate asyncio task without NiceGUI slot context, causing silent `RuntimeError`. Fixed by capturing `ui.context.client` at page creation
- **Status duplication**: Merged "Search completed in X — N Results" into results header ("588 Results · 0:18"), eliminated duplicate timer-based status messages
- **Browse rotation slider**: Removed JS string handler passed to `.on('update:model-value')` where NiceGUI expects a Python callable — Python `on_change` already handles rotation

### Technical
- Web: `progressiveLoad()` JS function in VIEWER_STYLES — loads `?width=400` thumbnail, preloads full via hidden `Image()`, swaps `src` on completion
- CSS: `.img-loading-container` animated spinner, `.img-loaded` hides it
- NiceGUI: `_page_client = ui.context.client` captured at page creation, entered with `with _page_client:` in deferred async tasks
- API: `/api/nli_image_by_sysid/` accepts `width` param (100-2000, default 2000), IIIF requests use `full/{width},/0/default.jpg`

---

## [7.2.0] - Image Adjustment Controls - 2026-03-19

### New Features
- **Image adjustment controls**: Brightness, contrast, gamma sliders and invert toggle on all image viewers — web browse (standard, fullscreen, reading desk), web search advanced view, and desktop ManuscriptViewerWidget
- **Desktop export with adjustments**: Right-click Copy Image and Save Image As export the adjusted (filtered) image, not the raw original
- **Icon-based toolbar**: Compact icon labels (sun, half-circle, curve, ±, reset arrow) with translated tooltips replacing text labels

### Bug Fixes
- **Browse page crash**: Fixed `RuntimeError: slot stack empty` — async `load_page()` called `ui.run_javascript()` outside NiceGUI slot context, killing the entire page render
- **Desktop image race condition**: Stale debounce timer from previous image could fire after new image loaded, reverting the display. Fixed by cancelling timer in `set_image()`
- **Desktop thumbnail race**: Stale thumbnail callback overwrote new image when switching manuscripts with same page index. Fixed with `_load_generation` counter

### Technical
- Web: CSS `filter` property for brightness/contrast/invert (native) + SVG `feComponentTransfer` for gamma. Per-viewer SVG filter IDs for reading desk multi-image isolation
- Desktop: QImage LUT-based pixel processing with 80ms QTimer debounce. 256-entry lookup table applied via `bits()` direct byte access
- Hebrew translations: בהירות, ניגודיות, גמא, הפוך צבעים, איפוס תמונה

---

## [7.1.0] - FIST Gap Fill & Expanded Catalog - 2026-03-19

### New Features
- **38,673 new manuscript records**: FIST.db gap records merged into libraries.csv (216,942 → 255,615 records, +17.8%). Manuscripts from 52 libraries now browsable, searchable by shelfmark/title, and enrichable via FJMS catalog data and NLI images
- **7 new library codes**: Solomon Halberstam, Reinach, Vatican, Central Archives, JC Mainz, Corwin, Mehlman — with Hebrew translations
- **Metadata-only search**: Title and shelfmark search now returns records without transcription text. Results carry a `metadata_only` flag; page navigation hidden for text-less records
- **Metadata-only browse**: Browse page shows metadata, NLI images, FJMS enrichment, and external links for records without Tantivy text (instead of "No text available" error)
- **Shelfmark normalization**: Yevr→EVR (Russian National Library) and Halper→Genizah (CAJS) aliases with Halpern guard

### Bug Fixes
- **Mosseri CUDL images**: Mosseri collection images now load via Cambridge Digital Library fallback path (both apps)
- **Puzzle auto-fit**: Canvas auto-fits view only on document load, not when adding individual fragments
- **Server heartbeat**: Status heartbeat survives transient failures; removed JS ping in favor of server-state-only checks
- **Server init ordering**: Engine marked ready before FJMS pre-warm to prevent premature "loading" state
- **JWT auto-retry**: Browse guards against deleted Supabase client; automatic retry on JWT expiry

### Data
- Generation script: `scripts/generate_fist_gap_csv.py` (repeatable with `--dry-run` and `--validate-only`)
- Stats report: `docs/FIST_GAP_FILL_STATS.md`
- Gap manifest: `fist_gap_manifest.txt` (38,673 AlmaIds for validation)

---

## [7.0.1] - Web Puzzle Browser Extension - 2026-03-18

### New Features
- **GenizahSearch Image Helper extension**: Chrome/Firefox extension fetches NLI manuscript images via user's browser, bypassing datacenter IP blocks. Submitted to Chrome Web Store and Firefox AMO
- **Server derivative cache**: Processed images cached on server disk; once cached, available to all users without extension
- **Unified image loader**: Single `_loadImageWithFallbacks()` function replaces 4 separate fallback chains (add/reload/folio/restore)
- **HMAC upload tokens**: Secure cache writes — server issues signed tokens on cache miss, uploads require valid token
- **Extension install banner**: Bilingual dismissible banner when extension not detected; green "Extension active" indicator when present
- **Cache key versioning**: `PROCESSING_VERSION` in cache keys for automatic invalidation when bg removal algorithm changes
- **Privacy policy page**: `/privacy-extension` route for Chrome Web Store listing requirement

### Security
- `POST /api/puzzle_process` hardened with token verification, 10MB size limit, content-type validation, rate limiting (60/min/IP)
- New `POST /api/puzzle_upload_derivative` endpoint with same protections
- Extension validates URL origin (only `iiif.nli.org.il`) and message origin (only `genizahsearch.com`)

### Bug Fixes
- **Manchester LUNA recto/verso**: Both recto and verso showed the same (recto) image because each Manchester page has its own luna_id but only the first was fetched. New `get_manchester_canvases()` resolves ALL crossref images directly to individual IIIF canvas entries, bypassing the single-manifest approach (both apps)
- **Library attribution credit lines**: All non-Oxford manuscripts showed NLI default attribution. Now each library gets proper credit: Manchester (CC BY-NC-SA 4.0), Oxford (CC BY-NC 4.0), Cambridge/JTS (from IIIF manifest), and NLI-digitized collections (BL, RNL, AIU, Gaster, Mosseri, etc.) acknowledge both holding institution and NLI digitization. Web credit footer links to correct library website (both apps)

### Infrastructure
- Nginx: removed stale `location /api/` block that proxied to dead port 8000 (old FastAPI)
- `WEB_PUZZLE_ENABLED=true` set on production via `.env` for staged rollout

---

## [7.0.0] - Fragment Puzzle & Community Publishing - 2026-03-17

### New Features — Fragment Puzzle (Phases 47-51)

- **Fragment Puzzle canvas**: Visual workspace for arranging manuscript fragments side-by-side to reconstruct physical joins. HSV-based automatic background removal, zoom/rotate/crop controls, folio navigation (prev/next page), multiple background modes (dark gray/black/white/checkerboard/light table/grid) (both apps)
- **Save/Load puzzle arrangements**: Persistent "join documents" in local `joins.db` SQLite sidecar. Documents include title, notes, and all fragment positions/transforms. Auto-save after canvas changes (both apps)
- **Composite PNG export**: Full-resolution RGBA PNG with transparent background and metadata banner. Desktop offers draft (1000px) / standard (2000px) / full (3000px) resolution choices with progress dialog. Web downloads directly
- **Recto/Verso**: Automatic verso view generation from recto arrangement with correct verso images (both apps)
- **Bring Forward / Send Backward**: Layer ordering controls for overlapping fragments — toolbar buttons and desktop context menu (both apps)
- **Fragment selector combobox**: Dropdown showing all fragments on canvas, syncs with canvas selection. Browse button opens selected fragment in browse view (both apps)
- **Add from Lists / Known Joins**: Quick-add fragments from personal lists or from known FJMS/PGP joins for the current manuscript (both apps)
- **Saved Joins panel** (desktop): Side panel with thumbnails, details editing, delete, rename
- **Saved Joins dialog** (web): Modal dialog with load/delete/metadata editing

### New Features — Community Publishing (Phase 52)

- **Publish puzzle joins**: Share fragment arrangements with the research community. Publish button turns green when published, share dialog with copyable deep link (`/puzzle?doc={id}`) (both apps)
- **Discoveries Center integration**: Published puzzle joins appear in the community feed with composite thumbnails, author names, and shelfmark badges. "Published Puzzles" stat card in stats row
- **Fork & Open**: "Open in Puzzle" creates a local copy of any published join and opens it in the puzzle canvas (both apps)
- **Community Puzzle Joins panel**: When browsing a manuscript, see all published puzzle joins containing that fragment (both apps)
- **All Puzzles / My Puzzles tabs**: Browse and manage published puzzle joins in the community Joins section (both apps)
- **Clickable shelfmark badges**: Shelfmark badges on published joins navigate to the browse page for that manuscript
- **Admin soft-delete**: Admins can hide published puzzle joins from the community feed
- **Auto-unpublish on delete**: Deleting a local join automatically removes it from the community

### Improvements

- **Desktop toolbar compacted**: Text buttons replaced with emoji icon buttons (28px) with translated tooltips
- **Save dialog with notes**: Title and notes fields (was title-only)
- **Theme-aware web dialogs**: CSS variables for light/dark mode compatibility
- **Stats cards layout**: 7 stat cards fit in one responsive row on Discoveries page
- **Saved joins list dedup**: Hides shelfmarks line when it duplicates the title (handles reversed order, fork prefixes)
- **Help Center updated**: New Fragment Puzzle and Community Publishing sections in both web and desktop help (bilingual)
- **Hebrew translations**: 50+ new strings for all puzzle and community features

### Bug Fixes

- **Web puzzle page crash**: `ui.left_drawer` replaced with `ui.dialog` modal
- **Export position accuracy**: Fixed per-fragment `coord_scale` drift — uses single global scale factor
- **Export bg-removal fidelity**: Reuses same 800px processed image shown on canvas
- **Desktop export UI freeze**: Export moved to background thread with progress dialog
- **CUL blue conservation mat**: Two-pass background detection for colored conservation mats
- **Web publish button invisible**: Removed `flat` prop when published so green background shows
- **Fork button RuntimeWarning**: Async fork coroutine was not being awaited
- **Desktop discovery stats all zeros**: `get_discovery_stats()` now queries all relevant tables
- **BrowseState.meta_mgr AttributeError**: Guarded with `getattr` in joined view Oxford detection
- **Reading Desk from joined view**: Added `meta_mgr.resolve_system_by_shelfmark()` as fallback for fragment resolution
- **Desktop corrections showing anonymous**: Added profile batch-fetch to `get_my_corrections` and `get_all_corrections`
- **Index rebuild fails on Windows (WinError 5)**: Tantivy memory-mapped files were locked by the live searcher during rebuild. Now releases index before `shutil.rmtree` and reopens after
- **Publish broken joins**: `publish_join()` now fails fast if composite image generation returns None, with storage rollback on partial upload failure
- **Stale fragment selector after folio nav**: Fragment combobox now refreshes on folio prev/next and meta updates (both apps)
- **Web puzzle reload loses document identity**: `current_doc_id` now persisted to session storage and restored on page reload

---

## [6.5.4] - 2026-03-16

### Performance

- **Staged search enrichment**: Search results now appear immediately after core search completes, before metadata (domains, transcription badges, catalog counts, printed flags, translations) finishes loading. Enrichment runs in three progressive stages: title translations first (~1ms), then visible page (50 IDs), then remaining results in background chunks (200 IDs each)
- **FJMS build-time indexes**: 6 database indexes previously attempted at runtime (silently failing on read-only connection) are now pre-built in `fjms_enrichment.db` during export. Requires sidecar rebuild
- **Search generation guard**: New searches immediately cancel stale background enrichment from previous searches, preventing data overwrites
- **Performance instrumentation**: Timing spans added to search logger (`first_render_ms`, `visible_enrichment_ms`, `background_enrichment_ms`) for regression tracking

---

## [6.5.3] - 2026-03-15

### Improvements

- **Desktop image viewer — right-click menu**: Added context menu to the manuscript image viewer (both ResultDialog and Browse by Shelfmark) with "Copy Image" and "Save Image As..." options. Supports PNG, JPEG, and BMP export. Rotation is preserved in both copy and save

---

## [6.5.2] - 2026-03-15

### Improvements

- **Desktop ResultDialog — icon+text buttons**: Converted cluttered text-only buttons to compact icon+short text format across action row (📖 Browse, 🔍 Parallels, ⭐ List, ℹ️ Info, 📚 Bib, 📋 Catalog, 🌐 Trans), community row (📝 Corrections), and image toolbar (↩️ Reset, 🔗 External/Ktiv). All buttons include full-text tooltips
- **Web language toggle**: Moved language switch button from sidebar footer to header bar for better visibility

---

## [6.5.1] - 2026-03-14

### Improvements

- **Desktop session persistence — browse tabs**: Browse by Shelfmark restores the last viewed manuscript (text + images) on restart. Browse by Identification restores domain tree selection, date range, text filter chips, and undated checkbox
- **Desktop session persistence — composition search**: Composition search restores results (flat view), summary bar, sort mode, and appendix threshold
- **Desktop session persistence — active tab**: The last active tab is restored on restart (previously always returned to Search tab)

### Bug Fixes

- **Desktop composition search — ResultDialog navigation**: Fixed missing next/prev navigation when opening filtered (high-frequency) results. The tree traversal now recursively descends through all levels including filtered reason sub-groups and lazy-loaded appendix groups
- **Desktop composition search — lazy appendix ordering**: Lazy appendix groups are now sorted before being added to the ResultDialog navigation list, matching the order shown when groups are expanded in the tree
- **Web parallels page — parent_slot crash**: Fixed "The parent slot of the element has been deleted" RuntimeError by replacing all `ui.timer()` calls with `asyncio` patterns that don't attach to NiceGUI parent slots. The repeating progress timer and one-shot init timers no longer crash when users navigate away from the page
- **Web Hebrew UI — first-load drawer bootstrap**: Fixed the cold-start race where the drawer could paint on the wrong side on the first load and only settle after navigation/reload. The web bootstrap now resolves the persisted UI language before layout creation and retries Quasar RTL activation until the framework is ready

---

## [6.5.0] - 2026-03-13

### Milestone: Search UX & Filtered Search

Focused search by manuscript properties, ~924K catalog & metadata translations, line-boundary search for join detection, and cumulative improvements from 6.2.1–6.2.4.

#### Focused Search — Pre-Search Filtering (Phase 45)
- **Focused search panel**: Filter manuscripts by domain, author, work, date range, and material type before searching — narrows the corpus to a specific subset (both apps)
- **Removable chip bar**: Active filters shown as color-coded removable chips above results (purple=domain, blue=author, teal=work, orange=date, red=material)
- **Real-time manuscript count**: Filter panel updates matching manuscript count as filters are selected
- **All search modes**: Filters apply across Exact, Variants, Responsa, and Parallels search modes
- **Per-result word search exclusion**: Individually exclude manuscripts from word search results
- **Parallels filter parity**: Full filter panel on Parallels page with auto-exclude source manuscript, per-manuscript exclude buttons, and import exclusions from word search
- **Browse-to-search navigation**: Domain and author labels on Browse page link directly to a focused search (both apps)
- **Filter-aware search history**: Filters saved and restored with search history entries
- **Session persistence**: Filter state preserved across restarts

#### Dicta Translation — Multilingual Catalog Data (Phase 46)
- **~924K machine translations**: All catalog data, titles, and scholarly descriptions translated Hebrew↔English via Dicta Translation API with scholarly few-shot templates across 3 rounds
  - Libraries: 184,514 title translations (bilingual extraction + Dicta HE→EN)
  - PGP: 34,954 document description translations (EN→HE)
  - FJMS catalog fields: 3,830 translations across 6 categories (titles, authors, persons, genizah_titles)
  - FJMS free descriptions: 254,835 scholarly description translations (HE→EN)
  - FJMS running titles: ~134K translations (EN→HE)
  - FJMS full texts: ~71K scholarly description translations (EN→HE)
  - FJMS textual frames: ~84K translations (HE→EN)
  - Round 3 gap-closing: 206K additional translations for previously untranslated fields
- **Translation toggle**: Show Translations sidebar toggle enables translated text in search results, browse views, and catalog dialogs (both apps)
- **Translated/Original badge**: Clickable badge on each translated text to toggle between translated and original
- **Subtitle display**: When Hebrew title is short (<15 chars), English subtitle shown alongside (desktop)
- **Per-record RunningTitle translation**: Web catalog dialog uses per-record lookup matching desktop behavior
- **Translation QA**: Heuristic quality checks (length ratio, script mismatch, number drift, truncation), stratified audit sampling, user-facing "Report translation issue" dialog
- **Data quality fixes**: 12,827 translation rows fixed (stuttering, hallucinations, collapsed text), 34 gibberish rows deleted
- **Extraction fix**: MARC semicolon split improved — 87K records fixed, 58K Hebrew values improved
- **Dicta-powered translate buttons**: Individual translate buttons now use Dicta API instead of MyMemory

#### Source Attribution (Phase 46)
- **FJMS site user attribution**: 6,655 catalog records attributed to 168 named users via FJMS API bridge
- **Source name cleanup**: "Site User" → "FJMS Site User", Hebrew source labels, Fleischer Piyut Project (1,716 rows)
- **Handlist source fix**: 43,233 NULL SourceName records fixed with proper handlist/team labels

#### Citation Reminder
- **One-time citation popup**: Reminds users to cite MiDRASH when publishing material from the site (web + desktop, bilingual)

#### Line-Boundary Search (6.2.3)
- **Text position dropdown**: Search for words at Start of text, End of text, Line starts, or Line ends — useful for join detection between fragments
- **Per-word line constraints**: In Responsa mode, `|word` (start of line) and `word|` (end of line) with tabular builder checkboxes
- **Line-break syntax**: `word1 | word2` for cross-line search, `[|N]` for line gap notation
- **Snippet indicator**: `‖` (U+2016) shows line breaks in search snippets (both apps)

#### Cumulative Fixes (6.2.1–6.2.4)
- Search progress bar fixes (desktop): stuck "Restoring", elapsed timer, processing phase
- Pre-search domain filter parity: language-conditional display, "Other" disambiguation, sub-sub-domains
- Parallels search critical fix: stale branch + min-chunks filter bug
- Small-screen layout fix: browse button visibility, result card browse buttons
- Data quality: 1,144 shelfmark-SysID mismatches fixed in libraries.csv
- 30+ new Hebrew translation keys

---

## [6.2.4] - 2026-03-10

### Data Quality Fix: Shelfmark-SysID Mismatches
- **Fixed 1,144 records** in libraries.csv where a single NLI system number was incorrectly mapped to multiple different shelfmarks from the same series
- Primarily affects RNL (1,012), CUL (91), JTS (18), Oxford (10), BL (6)
- Added 36 new records for orphaned shelfmarks with their own correct sys_ids
- Added `scripts/fix_shelfmark_sysid_mismatch.py` for reproducible correction using NLI crossref as authoritative source
- Reported by Gregor Schwarb

---

## [6.2.3] - 2026-03-06

### Line-Break Search & Search Progress UX

#### Line-Break Search (| syntax)
- **Consecutive-line search** in Responsa mode: `|word` (line starts with), `word|` (line ends with), `word1 | word2` (cross-line)
- **Line gap notation** `[|N]` for skipping N lines between groups
- **Tabular builder** "Lines" scope with start/end-of-line modifier checkboxes
- **Multiline regex** matching via `_build_line_break_regex()` for accurate filtering and highlighting

#### Snippet Line-Break Indicator
- **`‖` (U+2016)** replaces invisible newline flattening in all search snippets (desktop + web)
- Styled gray/bold — visually distinct from query `|` and parallels segment breaks
- Dark theme support in web CSS

#### Search Progress Bar Fixes (Desktop)
- **Stuck "Restoring" message** — progress bar format now resets when a new search starts
- **Elapsed timer** updates every 1 second via independent QTimer (no longer freezes between progress callbacks)
- **"Processing" phase** — after Tantivy loop completes, progress bar shows `מעבד תוצאות...` with running clock during result rendering
- **Accurate "Search completed in"** — total time now includes post-processing and row rendering

#### Multiline Regex Re-highlighting
- ResultDialog and web expanded view now add `re.MULTILINE` flag when the highlight pattern contains `^` or `\n` anchors
- Fixes broken highlighting when opening line-break search results in detail view

#### Desktop Snippet Column
- Column is now **resizable** (Interactive mode, 600px default) — was previously locked to Stretch

#### Hebrew Translations
- Position dropdown: Text Position, Anywhere, Start/End of text, Line starts/ends
- Processing indicator, constraint tooltip, Position label (10 new keys)

---

## [6.2.2] - 2026-03-05

### Bug Fixes: Parallels Search & Small-Screen Layout

#### Parallels Search Fix
- **Critical: parallels search returning "No results"** — Two bugs combined to break all parallels searches:
  1. Server was on stale branch missing `restrict_sys_ids` parameter, causing silent `TypeError` on every search
  2. "Min. chunk matches" filter was incorrectly filtering on paragraph boundary crossings (always 0 for most input text), discarding all results even when matches existed
- **Missing `web/analytics.py`** — PostHog analytics module was never committed, causing server startup failure after deployment

#### Small-Screen Layout Fix
- **Browse button visibility** — ~40% of users (viewport height <700px) couldn't see the "Browse Full Manuscript" button, which was buried below tabs at the bottom of the right panel
- **Result card browse button** — Added green browse (📖) button directly on each search result card for one-click manuscript access
- **Right panel header actions** — Moved Browse, Find Parallels, and Advanced View buttons to the header row (always visible), removed old bottom action section
- **PGP tag viewer** — Same fix applied to PGP tag result viewer

---

## [6.2.1] - 2026-03-03

### Bug Fix: Pre-Search Domain Filter Parity

- **Language-conditional display**: Pre-search domain dropdown now shows only the current UI language (Hebrew or English), matching post-search filter behavior (web + desktop)
- **"Other" disambiguation**: Ambiguous child domains like "Other" now display with parent prefix (e.g., "Other (Bible)" / "אחר (מקרא)") in both dropdown and chip bar
- **Sub-sub-domain support**: 3rd-level domains now appear in pre-search filter tree/dropdown (previously only 2 levels shown)
- **Recursive checkbox propagation**: Desktop domain tree now propagates check/uncheck to all descendants (grandchildren), matching post-search filter
- **Chip bar display fix**: Web chip bar strips only trailing count `(N,NNN)` instead of all parenthesized text, preserving qualified domain names
- **Chip bar refresh**: Web chip bar re-renders after deferred filter init completes, showing proper display names
- **Qualified-name SQL filtering**: `get_filter_sys_ids()` now handles qualified domain names like "Other (Bible)" correctly, generating parent-scoped SQL queries

---

## [6.2.0] - 2026-03-02

### Milestone: Power-User UX — Search Workflow, Session & Notifications

Major UX overhaul driven by power-user feedback: composition search workflow improvements, session persistence, search history, desktop notifications, and Hebrew library names.

#### Composition Search UX (Phase 42)
- **Elapsed timer**: Real-time search duration display (both apps)
- **Chunk count display**: Shows number of chunks processed during composition search
- **Summary line**: Persistent search stats after completion (duration, matches, exclusions)
- **Min-chunks filter**: Filter regular search results by minimum chunk match count
- **Cancel with partial results**: Cancelling mid-search preserves results found so far, displayed in a collapsible "excluded" section with reason sub-headers
- **Printed badge**: Manuscripts identified as printed editions marked with badge in search results, composition tree, and catalog browse (both apps)
- **3-state printed filter**: All / Manuscripts only / Printed only toggle in both desktop and web, including composition tree
- **Responsive cancel**: Progress callback checked every chunk for immediate cancel response
- **Excluded items clickable**: Click excluded items in web to navigate to manuscript detail
- **Full Hebrew translations**: All Phase 42 UI strings translated

#### Session Persistence & Search History (Phase 43)
- **Session persistence service**: New `shared/session_persistence.py` module saves and restores full search state (query, mode, results, exclusions) across app restarts
- **Desktop session restore**: Automatic save on exit with restore prompt on startup; configurable via settings (Ask / Always / Never)
- **Web session persistence**: Search and parallels state preserved in browser sessionStorage
- **Search history dropdowns**: Dropdown arrow (▼) inside search bar with last 20 searches, showing mode indicator and result count (both apps, both search and composition)
- **Keyboard navigation**: Down arrow opens history from search bar, arrow keys navigate, Enter selects, Delete removes entries

#### Notification, Copy & Hebrew Names (Phase 44)
- **Desktop search notifications**: Taskbar flash when search completes while app is in background
- **Sleep prevention**: Prevents Windows sleep during long-running searches
- **Copy context menu**: Right-click to copy cell text from desktop search results table
- **Hebrew library names**: Full Hebrew names for all 81 library codes displayed when UI language is Hebrew (both apps)

#### Web & Performance
- **Home page redesign**: Compact notices section, hero banner, and 5 action cards for quick navigation
- **Sidebar RTL fix**: Correct positioning in RTL mode, improved Core Web Vitals
- **Lazy imports**: Faster desktop startup via deferred module loading
- **PostHog EU endpoint**: Fixed analytics endpoint to match account region; logged-in user identification
- **Language toggle fix**: Resolved bug when switching UI language

---

## [6.1.1] - 2026-03-01

### Performance: Catalog Browse & Domain Queries

Massive performance optimization for catalog browse domain filtering and async desktop UI.

#### Query Optimization (35s -> 0.8s)
- **100x faster domain-filtered queries**: Replaced JOIN+OR domain filter with IN(UNION) subquery for proper SQLite index utilization
- **Pre-dedup CTE pattern**: Deduplicate catalog rows in CTE then COUNT(*) instead of expensive COUNT(DISTINCT) on 685K-row table with 3x duplicates
- **Benchmarks** (Halakhic Literature, 20,951 manuscripts): Authors 30s->0.27s, Works 4.4s->0.29s

#### Async Desktop Catalog Browse
- **Non-blocking UI**: All catalog browse operations (domain/author/work select, pagination, text/date filters) now run in background QThread
- **Module-level QThread class**: Fixes PyQt6 signal delivery for locally-defined thread classes
- **Thread-safe FjmsService**: Default `thread_safe=True` for read-only sidecar connections

#### Domain Hierarchy Enhancements
- **3-level domain nesting**: Sub-sub-domains shown in web search filter and catalog browse (both apps)
- **Canonical FJMS ordering**: Domain tree sorted by Friedberg classification system order
- **Browse cache v2**: Versioned disk cache invalidates stale pre-nesting caches

---

## [6.1.0] - 2026-02-27

### Milestone: Catalog Browse & Navigation (Phase 41)

Added faceted catalog browsing by domain, author, and work in both apps, with free-text filtering, FIST v5.0.0 enrichment, and cross-links between browse pages.

#### Catalog Browse Pages (Plans 41-01 through 41-04)
- **Web catalog browse page**: New `/catalog-browse` page with domain hierarchy tree, author/work search dropdowns, combined filtering, pagination, and deep linking via URL params
- **Desktop catalog browse tab**: New "Browse by Identification" tab with matching domain tree, author/work filtering, and result navigation
- **Cross-links**: Domain and author labels on manuscript browse pages are clickable links to catalog browse filtered by that value (both apps)
- **Sidebar/tab navigation**: "Browse by Shelfmark" and "Browse by Identification" entries in both apps

#### Free Text Filter
- **FTS5-based catalog search**: ALL/ANY/NOT modes for filtering catalog browse results by text across titles, descriptions, and identifications
- **Hybrid FTS5 + domain LIKE**: Domain name searches (e.g., "פילוסופיה") return results via UNION query combining FTS5 catalog fields with domain table LIKE search
- **Filter chips**: Color-coded removable chips (blue=ALL, green=ANY, red=NOT) with button-style removal to avoid NiceGUI slot issues
- **sessionStorage persistence**: Text filter state preserved across page navigation

#### FIST v5.0.0 Enrichment
- **3 new tables**: genizah_persons (2,286 historical people), genizah_titles (775 works), code_values (3,440 decoded field values)
- **20 new catalog columns**: GenizahTitleId, Author, CopyToDate, CreationTypeCode, Comment, Colophon, CopyName, and more
- **Structured author/work browsing**: 801 authors (was 204) and 663 works via FK path through genizah_persons/genizah_titles
- **Graceful fallback**: `_has_persons_titles` flag enables v4 sidecar compatibility

#### Translations & Tests
- **15 new Hebrew translations** for catalog browse UI strings
- **Test updates**: Browse author/work tests updated for new dict key format; 72 FJMS tests passing

---

## [6.0.0] - 2026-02-22

### Milestone: Local Data Architecture

Migrated all PGP reference data from Supabase to a local SQLite sidecar, added FJMS catalog descriptions as a scholarly resource, stabilized the app with crash fixes and pagination, and optimized performance across both apps.

#### PGP Sidecar Migration (Phase 35-36)
- **pgp.db sidecar**: All PGP data (35,839 documents, 9,364 sources, 22,757 footnotes, 36,155 fragments) exported to local SQLite (147MB)
- **PgpService rewrite**: `document_service.py` reads from SQLite instead of Supabase -- sub-millisecond local queries replace 50-200ms API calls
- **JSON preservation**: Tags and sections stored as TEXT JSON, queried with `json_each()` for full parity with Supabase GIN queries
- **Both apps updated**: Web shim and desktop imports all point to local pgp.db
- **Zero Supabase dependency**: All PGP reference data served locally; Supabase retained only for community features (auth, corrections, lists)

#### FJMS Catalog Descriptions (Phase 37)
- **Enriched export**: fjms_enrichment.db extended to v3.0.0 with 4 new tables (running_titles, size_field, catalog_free_desc, genizah_titles) adding ~1.7M rows
- **Catalog dialog**: Dedicated 5-section scholarly layout (content identification, physical metadata, running titles, free descriptions, genizah titles) in both apps
- **Source attribution**: Each catalog entry shows which scholarly catalog or scholar produced the description
- **Batch catalog counts**: Search results show catalog source count on button labels for quick reference

#### Distribution & Offline (Phase 38)
- **Desktop bundling**: pgp.db included in installer via `build_app.bat` -- no separate download needed
- **LOCALAPPDATA resolution**: User-updated sidecars stored in AppData, separate from bundled install directory
- **Sidecar update mechanism**: SidecarUpdateThread checks for newer sidecar versions at startup with sequential download queue
- **About screen**: Data Sources section showing versions of all 3 sidecars (pgp.db, fjms_enrichment.db, nli_crossref.db)
- **Offline verification**: 12 tests confirming zero network dependency for all 3 sidecar services
- **Desktop offline PGP browsing**: Full metadata, transcriptions, footnotes, and fragment navigation without internet (images excluded)

#### Bug Fixing & Cleanup (Phase 39)
- **Desktop crash fixes**: `sip.isdeleted()` guards on all Qt lifecycle crash sites (set_status_message, update_text_pos) -- eliminates all known crash-on-navigate bugs
- **Paginated search results**: PAGE_SIZE=50 replaces the 200-result hard cap; prev/next navigation with scroll-to-top; storage persistence cap raised to 1000
- **PostHog analytics**: Integrated alongside Google Analytics (env-var gated via `POSTHOG_API_KEY`); maskAllInputs + identified_only for privacy
- **Domain filter performance**: Cached domain hierarchy eliminates ~5s lag when opening domain filter dialog (double-checked locking for thread safety)
- **E2E test infrastructure**: Custom NiceGUI Screen fixture, app-level E2E via runpy, selenium as dev dependency with skip logic for CI
- **CSS extraction**: Inline styles moved to static CSS file for maintainability
- **Lazy login dialog**: Login dialog created on-demand instead of at page load, improving navigation speed
- **Parallel page queries**: asyncio.gather + run.io_bound for search enrichment, batch FJMS for browse, async discoveries

#### Performance Optimization (Phase 40)
- **Parallel NLI fetch**: ThreadPoolExecutor for concurrent MARC + IIIF manifest calls, halving browse metadata load time
- **Desktop async domain enrichment**: DomainEnrichmentWorker thread loads domain data after results display (~200ms); catalog detail fetched lazily on click
- **Browse crossref parallelization**: 3 independent crossref queries via ThreadPoolExecutor (catalog entry, collection/storage, physical metadata)
- **FL ID O(1) index**: Dictionary-based lookup for browse-by-FL-ID replacing linear scan (with fallback during startup window)
- **Variant cache unification**: Pre-compute variants at REGEX_VARIANTS_LIMIT (8000) before per-term loops; Tantivy slices from superset cache

#### Pre-Ship Cleanup
- **IsNotGenizah badge removed**: Orange "Not Genizah" badge removed from both apps' browse pages and Reading Desk (data preserved in nli_crossref.db)

---

## [5.9.0] - 2026-02-16

### Milestone: Multi-Source Image & Metadata Integration

Import of NLI crossreference data (815K image-level records) and Cambridge IIIF manifests (141K URLs) into a second SQLite sidecar, plus Manchester LUNA and JTS/Princeton Figgy integration, enabling direct image access across 75+ libraries, physical metadata, scholarly bibliography, and library-specific viewer links in both apps.

#### Data Infrastructure (Phase 29)
- **NLI crossref sidecar** (`nli_crossref.db`): 815K image-level records from NLI crossreference CSV with 253K distinct AlmaIds, plus 141K Cambridge IIIF manifest URLs
- **Shared NliCrossrefService**: 16 query methods (images, folio labels, physical metadata, relationships, library URLs, Manchester/JTS lookups)
- **Thread-safe SQLite**: Read-only URI mode with thread safety for NiceGUI concurrent requests
- **Graceful degradation**: All methods return empty results when sidecar is missing

#### Direct Image Access (Phase 30)
- **Cambridge local resolution**: Cambridge manuscripts load images via pre-stored CUDL IIIF manifest URLs, bypassing NLI entirely (141K records)
- **Fallback chain preserved**: Memory cache -> sidecar -> network for all image resolution

#### Image Navigation & Indicators (Phase 31)
- **Folio navigation**: Page-level navigation using scholarly notation (1r, 1v, 2r, etc.) in both apps
- **Source availability indicators**: Colored chips showing which digital image sources exist (NLI, Cambridge, Manchester, JTS)
- **Source switching**: Toggle between NLI and external image sources in the browse viewer
- **Cambridge IIIF proxy**: Server-side proxy endpoint for Cambridge image serving

#### Metadata Display (Phase 32)
- **Physical metadata**: Material type (paper/parchment) and folio count on browse page (both apps)
- **NLI catalog link (KTIV)**: Clickable link to NLI KTIV viewer for manuscripts
- **Library collection links**: Clickable links to holding library digital collections (CUDL, Manchester LUNA, JTS DPUL, BL, Oxford)
- **Hebrew translations**: Material types and metadata labels translated for Hebrew UI

#### Metadata Enrichment (Phase 33)
- **FIST bibliography**: 542K denormalized bibliography references with scholar attribution, mention type badges, and transcription/translation availability (both apps)
- **Catalog cross-references**: 64K entries across 80 scholarly catalogs displayed as structured references (both apps)
- **Neubauer-Cowley catalog numbers**: 27K Oxford entries displayed alongside shelfmark
- **IsNotGenizah badge**: Orange visual badge for 304K flagged items in corpus
- **Collection & storage**: NLI collection names and physical storage references (box/volume/folio)
- **Scholarly source names**: FJMS source attributions with generic name filtering
- **FJMS sidecar extended**: fjms_enrichment.db upgraded to v2.0.0 with bibliography, catalog_refs, and reference tables

#### Library IIIF Integration (Phase 34)
- **Manchester LUNA**: 27,940 LUNA IDs pre-imported via API pagination; detail page links (not search); IIIF manifests as image source with pink source chip
- **JTS/Princeton Figgy**: 453 validated ARK IDs + Figgy manifest URLs via DPUL catalog search; catalog page links; IIIF manifests as image source with orange source chip
- **BL deferred**: British Library links use searcharchives.bl.uk (BL IIIF API still down from cyber attack)

---

## [5.8.0] - 2026-02-15

### Milestone: FJMS Integration

Integration of scholarly metadata from the Fragment of the Jewish Manuscript Studies (FJMS) database into both web and desktop apps via a SQLite sidecar database. Adds subject-based filtering, scientific join groups, and catalog enrichment for manuscripts.

#### Data Infrastructure (Phase 25)
- **SQLite sidecar database** (`fjms_enrichment.db`): 762K rows exported from 13GB FIST.db with domains, joins, catalog tables, and FTS5 full-text index
- **Shared FjmsService**: 8 query methods accessible from both web and desktop apps
- **Thread-safe SQLite**: Read-only URI mode with thread safety for NiceGUI concurrent requests
- **Graceful degradation**: All methods return empty results when sidecar is missing

#### Scientific Joins (Phase 26)
- **FJMS join groups** in Related Fragments panel: scholarly join identification with scholar name and join type (Physical Join, Codex Join, etc.)
- **Three-source merge**: FJMS joins merged as third source after user and PGP joins with full deduplication
- **Purple badge** for FJMS source visual distinction (user=none, PGP=blue, FJMS=purple)
- **Navigation**: Click join group members to navigate to that fragment in both apps

#### Domain Classifications (Phase 27)
- **Domain badges** on browse page: clickable subject classification links (e.g., Piyyut, Bible, Letters)
- **Domain search filtering**: hierarchical multi-select with type-ahead, OR logic for multi-domain queries
- **Standalone domain browsing**: browse manuscripts by domain without text query (capped at 500)
- **Post-search dynamic filtering**: Domains button with checkbox tree dialog for excluding domains from results
- **Domain indicators** on result cards: primary domain + "+N more" pattern with tooltip

#### Catalog Enrichment (Phase 28)
- **FJMS catalog titles**: Hebrew and English titles with language-aware display
- **Author information**: Scholar/author attribution from FJMS catalog records
- **Copy date and place**: Manuscript dating and origin information with sentinel value filtering
- **Content identifications**: Parsed TextualFrame entries with category and source attribution
- **FJMS description alongside PGP**: Separate sections, not replacing existing PGP metadata
- **Cross-app parity**: All catalog fields display in both web and desktop (Browse tab + ResultDialog)

---

## [5.7.2] - 2026-02-11

### Cleanup & Polish

- Removed deprecated AI Search feature code (AIManager, AIDialog, AIWorkerThread, Settings panel, button, help references)
- Removed `google-genai` dependency

### Search Normalization

- Combining diacritical marks (U+0300-U+036F) stripped from search queries at query time
- Hebrew geresh (U+05F3) and gershayim (U+05F4) stripped from search queries
- ASCII apostrophe and curly quote variants normalized in search
- Mark-tolerant search highlighting (matches through interleaved combining marks in source text)
- All existing search modes unaffected (normalization globally safe)
- Regex mode exempt from normalization (users control their own patterns)

### Test Suite

- Fixed 17 pre-existing test failures (export filenames, boundary search, responsa integration, shelfmark normalization)
- Deleted 3 obsolete backend test files (test_api_flow.py, test_corrections_api.py, test_corrections_integration.py)
- Full green suite: 447 tests passing, 0 failures

### PGP Transcription Sections

- Structural HTML section parser for PGP transcriptions from pgp-text repository
- Canvas-based parsing (h3 inside data-canvas divs) replaces fragile regex-only approach
- New `sections` JSONB column on document_sources with `source_language` and `source_direction`
- Recto/verso/margin sections correctly display alongside manuscript images in both apps
- Language-based translation ordering (Hebrew first, English second) consistent across both apps
- Import script: clones pgp-text repo, parses HTML, populates structured section data

## [5.7.0] - 2026-02-10

### Milestone: Responsa Search

Advanced search capabilities inspired by the Responsa Project, available in both web and desktop apps. Researchers can now use Responsa-Project style syntax, grammatical expansion, Judeo-Arabic support, and a visual query builder to search the Genizah corpus with fine-grained control.

#### Responsa Core Engine (Phase 14)
- **Responsa syntax**: `#word` (prefix expansion), `word#` (suffix expansion), `#word#` (both), `*word`/`word*` (wildcards), `%word` (plene/defective variants), `(a/b)` (OR alternatives), `[N]` (gap notation)
- **Hebrew grammatical expansion**: 24 prefix forms (single + compound: ו,ה,ב,כ,ל,מ,ש + combinations) and 25 suffix forms per word
- **Judeo-Arabic article expansion**: 8 forms per word using simplified al- model (no sun letter assimilation)
- **Plene/defective variants**: Bidirectional ו/י insertion/removal for spelling variations
- **Sofit letter conversion**: Final forms (ם,ן,ץ,ף,ך) normalized before suffix expansion
- **Combinatorial explosion guard**: MAX_EXPANDED_TERMS=500 with 6-step downgrade cascade (variants basic -> off -> JA off -> plene off -> suffixes off -> prefixes off -> error)
- All Responsa logic in shared `genizah_core.py` -- no search logic in UI code

#### Search UI (Phase 15)
- **Responsa as dropdown mode**: "Responsa (R)" appears as a first-class option in the Mode dropdown/combo in both apps
- **Sub-option checkboxes**: Variants, Judeo-Arabic, Flexible Spacing, Bidirectional Gap -- visible only when Responsa mode is selected
- **Syntax legend**: Quick reference for Responsa operators shown below the search field
- **Keyboard shortcut**: Type `R ` (R+Space) to switch to Responsa mode
- **URL state persistence**: Web URLs include `?mode=responsa&variants=1&ja=1&flex_spaces=1&bidirectional=1`
- **PGP Tags interaction**: Responsa sub-options hidden when PGP Tags mode is active
- **Desktop defaults**: Checkboxes reset to defaults on each app startup

#### Tabular Query Builder (Phase 16)
- **Visual query construction**: Dialog with 2-4 component columns for building complex Responsa queries without memorizing syntax
- **Per-word modifiers**: Checkboxes for prefix (#), suffix (#), wildcard (*), plene (%), and negation per word
- **Distance control**: Per-pair gap spinners with [N] notation between components
- **Live preview**: Real-time syntax preview updates as you modify the query
- **One-way sync**: "Apply" inserts generated syntax into the search field and triggers search
- **Web**: Dialog opened via "Query Builder" button in Responsa sub-row
- **Desktop**: QDialog opened via "Query Builder" button with full RTL layout

#### Integration Testing & Polish (Phase 17)
- **221 automated Responsa tests**: 68 core engine + 31 parity + 20 edge cases + 30 regression + 5 performance + 36 additional
- **Cross-app parity**: All 16 checkbox combinations verified to produce identical results
- **Non-Responsa regression**: 30 tests confirming all existing search modes (Exact, Variants, Fuzzy, Regex, Shelfmark, Title, PGP Tags) work unchanged
- **Bug fixes**: R+Space shortcut sub-options visibility, WebSocket crash on large results (200 cap), sofit-aware wildcard regex, explosion guard cascade expanded from 3 to 6 steps, ValueError surfaced to user via toast notification, desktop tabular builder unconditional RTL

---

## [5.6.1] - 2026-02-10

### Bug Fixes — User Authentication & Corrections

#### Web: Singleton Supabase Client Fix
- **Critical fix**: Web app used a shared singleton Supabase client for all users. When multiple users were logged in, the auth session belonged to whoever signed in last — causing RLS policy failures for all other users' write operations (corrections, comments, discoveries, lists, etc.)
- Added `get_user_client()` — creates a per-user Supabase client from session tokens stored in NiceGUI's per-user storage
- All 28+ write functions now use the per-user client; read-only functions remain on the efficient singleton
- Session tokens are stored during email login and Google OAuth, and refreshed automatically when expired

#### Web: Admin Panel Corrections
- Fixed admin panel not showing pending corrections — the PostgREST join between `corrections` and `profiles` failed silently because there is no direct FK between the tables (both reference `auth.users` independently). Replaced with separate queries.
- Fixed admin unable to approve/reject corrections — added RLS policies allowing admins to update/delete corrections, comments, discoveries, and fragment joins from any user
- Admin write operations now use per-user client instead of singleton

#### Web: Correction Submission UX
- Fixed "parent element deleted" error after submitting a correction — the async handler's UI slot was destroyed by `update_content()` during the submit flow. Removed all `update_content()` calls from the async handler; all feedback now uses slot-independent `ui.notify()`
- Added success notification when correction is submitted
- RLS errors (42501) now show "Session expired — please log out and log back in" instead of raw Supabase error

#### Web: Profile Password Change
- Fixed password change using singleton client — could silently fail or change wrong user's password. Now uses per-user client.

#### Desktop: Login Error Messages
- Improved error messages for common login failures:
  - "Invalid email or password" for wrong credentials
  - "Email not confirmed" for unverified accounts
  - "No account found" for non-existent emails
  - Network-specific errors for connection issues

---

## [5.6.0] - 2026-02-09

### Milestone: Desktop Parity & PGP Integration

Full integration of Princeton Geniza Project (PGP) data across both web and desktop apps.

#### PGP Data (Phases 8-9)
- Imported 35,839 PGP documents with full metadata, 9,364 sources, 22,757 footnotes, 36,155 fragment links
- Shared document_service.py for Supabase access from both apps

#### Desktop PGP Core (Phase 10)
- PGP transcriptions and metadata in desktop Browse and Result dialogs
- Per-source directionality (editions RTL, English translations LTR)

#### Virtual Reading Desk (Phase 11)
- Multi-manuscript synchronized viewer in both web and desktop apps
- Stacked images + stacked texts with fragment-level sync scrolling
- Per-fragment version selector, zoom/rotate controls, lazy loading

#### Desktop PGP Discovery (Phase 12)
- PGP badges and tag display in search results
- PGP column sorting (click to show PGP-linked manuscripts first)
- PGP joins visible in desktop JoinsDialog
- Tag-based search as a search mode in both apps

#### PGP Tag Search UX
- "PGP Tags" as a search mode in the Mode dropdown (both apps)
- Desktop: hides query row, shows tag combo in Mode row
- Web: tag select replaces query input when PGP Tags mode selected
- Tag click navigation from result dialogs and browse pages
- 251 PGP tags with curated Hebrew translations and category grouping
- 16 categories: Document Types, Law & Society, Medicine, Trade, India Book, etc.
- Language-aware display: Hebrew UI shows "עברית (English)", English UI shows English only
- Category headers as visual separators in tag dropdowns

#### Phase 13 Deferred
- Transcription Search (full-text search in PGP transcriptions) was implemented but reverted
- Reason: Tantivy index build too slow for desktop distribution
- Will revisit with server-side index architecture in a future milestone
- Full documentation preserved in docs/archive/PHASE_13_TRANSCRIPTION_SEARCH_DEFERRED.md

---

## [5.5.0] - 2026-02-04

### New Feature: In-App Software Updates

The desktop application can now download and install updates without leaving the app.

#### How It Works
1. When a new version is available, a notification bar appears at the top
2. Click "Update Now" to start the update process
3. A progress dialog shows download progress
4. After download, the installer runs automatically in silent mode
5. The app restarts with the new version

#### Technical Details
- Downloads the official installer from GitHub Releases
- Uses Inno Setup's silent mode (`/VERYSILENT /RESTARTAPPLICATIONS`)
- Installer automatically closes the running app, updates files, and restarts
- UAC prompt will appear (same as manual install) since app is in Program Files
- Falls back to opening browser if installer not found in release

#### Files Changed
- `gui_threads.py` - New `UpdateDownloaderThread` class for downloading with progress
- `genizah_app.py` - New `UpdateProgressDialog` for update UI
- `CompileScriptGenizah.iss` - Added `CloseApplications` and `RestartApplications` settings

---

## [5.4.1] - 2026-02-03

### Enhancement: "Remember Me" Login Feature

Both the desktop and web applications now support saving login credentials.

#### Desktop Application
- **"Remember me" checkbox**: New checkbox in the login dialog to opt-in to credential saving
- **Secure storage**: Password stored in Windows Credential Manager (via `keyring` library) - not in plain text files
- **Persistent across updates**: Credentials survive software updates since they're stored in user profile, not application folder
- **Easy to disable**: Uncheck "Remember me" to clear saved credentials

#### Web Application
- **"Remember me" checkbox**: New checkbox in the login dialog
- **Email remembered**: Email address saved in browser localStorage for convenience
- **Session persistence**: Login session already persists via Supabase cookies

---

## [5.4.0] - 2026-02-03

### New Feature: Library/Holding Institution Display

Every manuscript record now shows which library or collection holds the original document.

- **Coverage:** 99.99% of ~217,000 records have library codes assigned (only 14 records with missing source data)
- **Libraries identified:** 70+ institutions including Cambridge (CUL), JTS, National Library of Russia, Bodleian (Oxford), Manchester, British Library, Alliance Israélite, Library of Geneva, Senckenberg (Frankfurt), Schocken Institute, and many more

#### Web Application
- Library badge with code (e.g., "CUL") displayed in search results with full name tooltip
- Library field in Advanced View metadata cards
- Library field in browse page metadata panel
- Library column in all Excel exports (Search, Lists, Parallels)

#### Desktop Application
- New "Library" column in search results table
- Filterable/sortable like other columns
- Library column in Excel/Word exports

#### Technical Details
- New `library_code` column in `libraries.csv`
- New functions: `LIBRARY_CODES` constant, `get_library_display()`, `get_library_for_id()`
- Backward compatible with old CSV files (gracefully handles missing column)

### Enhancement: Nikud (Vowel Mark) Removal in Parallels Search

Parallels search now automatically strips Hebrew vowel marks (nikud) and cantillation marks from text before matching. This ensures consistent results whether the input text contains nikud or not.

- Affects both Lab Mode and Standard parallels search
- Also strips nikud from filter/exclude text for consistent filtering
- New function: `strip_nikud()` in `genizah_core.py`

### Enhancement: Advanced View Dialog Improvements

The Advanced View dialog (opened from search results) has been significantly enhanced:

#### Navigation & Viewing
- **Fixed navigation bug**: Results now navigate in-place without closing/reopening the dialog
- **Page navigation**: Browse pages within a manuscript using prev/next buttons
- **IIIF image viewer**: Side-by-side image panel with zoom, rotate, and pan controls
- **Fullscreen mode**: Distraction-free view with compact navigation bar
- **Image toggle**: Show/hide image panel as needed

#### Inline Editing
- Edit text directly in the Advanced View (same as Browse page)
- Save drafts, submit for review, or publish immediately (for editors/admins)
- Visual feedback: orange border for unsaved changes, green for saved
- Notes field for correction comments

#### Bug Fixes
- Fixed "Unknown" author display in version selector (now joins profiles table)
- Fixed script tag error in edit dialog (NiceGUI compatibility)

### Files Changed
- `genizah_core.py` - Core library functions, CSV loading, nikud removal
- `genizah_app.py` - Desktop table columns
- `web/services.py` - Data classes and page retrieval
- `web/pages/search.py` - Library badge display, Advanced View dialog enhancements
- `web/pages/browse.py` - Metadata panel
- `web/components/text_editor.py` - Fixed script tag in HTML
- `web/supabase_client.py` - Added profiles join to get_corrections()
- `web/export_service.py` - Export functions
- `libraries.csv` - Added library_code column

---

## [5.3.1] - 2026-02-03

### Bug Fixes

- **RTL navigation arrows:** Fixed all directional icons (arrows, chevrons, skip buttons) that were reversed in Hebrew UI mode. Icons now correctly flip direction based on language setting.
- **Removed directional icons from action buttons:** Removed `send` arrow icons from Submit/Share/Reply buttons and `arrow_forward` from Go button, as these looked incorrect in RTL mode.
- **Missing title metadata in search results:** Fixed bug where title and other metadata wasn't displayed in search results. The `get_display_data()` method now uses proper fallback logic (CSV bank → NLI cache) matching the browse page behavior.
- **Search panel auto-collapse:** Fixed scroll-based auto-collapse that wasn't working. Added proper class targeting for the results scroll area and improved JavaScript detection.
- **Search panel collapse/expand visibility:** Fixed panels not showing/hiding properly by using explicit styles with `!important` flags.
- **Advanced Options inside search panel:** Moved the Advanced Options expansion inside the collapsible search panel so it hides when the search bar collapses.
- **Search results layout overflow:** Fixed text getting cut off when zooming or resizing window. Removed `max-width` restrictions and added proper flex wrapping and word-wrap styles.
- **Removed Edit/Comment buttons from result cards:** Cleaned up search result cards by removing Edit and Send Comment buttons (still available in the detailed viewer).

### Enhancements

- **Full Text pane highlighting:** Added search term highlighting to the Full Text tab in search results, matching the highlighting in the Match pane.

### Files Changed

- `genizah_core.py` - Fixed `get_display_data()` metadata fallback
- `web/pages/search.py` - Search panel collapse, Advanced Options placement, result card layout, Full Text highlighting
- `browse.py` - Page/shelfmark navigation, Go button, Back buttons, Submit buttons
- `document.py` - Back button, page navigation
- `home.py` - Start Search, Find Parallels, Browse, View All buttons
- `discoveries.py` - Back buttons, Reply/Share buttons
- `comment_dialog.py` - Back button, Submit button
- `joins_panel.py` - Navigation indicator, Back button
- `text_editor.py` - Submit Correction button

---

## [5.3.0] - 2026-02-02

### New Feature: Cross-Paragraph Search

A new parallel search mode that finds manuscripts with text spanning paragraph boundaries, now available on **both Web and Desktop**.

- **Why it's useful:** Text within paragraphs often contains citations (Mishnah, Talmud, known phrases). Text that crosses paragraph boundaries is unlikely to be a citation, effectively filtering out noise.

- **Three search modes:**
  - **Full search** - All results (default)
  - **Cross-paragraph only** - Only matches that span paragraph breaks
  - **Combined** - All results, with boundary-crossing matches boosted

- **Customizable delimiters:** Line break, blank line (paragraph), period, colon

- **Visual indicators:**
  - Web: Amber "Cross-paragraph" badge; red `|` at boundary points in matched text
  - Desktop: 🔗 emoji prefix on scores; tooltips showing match count

- **Advanced settings:** Configurable boost factor (1.0-3.0), minimum boundary matches filter, minimum delimiter distance

- **Real-time feedback:** Desktop shows boundary count and crossing chunks before search

### Bug Fixes

- **Duplicate results fix:** Fixed bug where same manuscript appeared multiple times in Standard search when found by overlapping chunks routed to different filter maps
- **Boundary detection:** Improved to require words on BOTH sides of the boundary (not just touching)
- **Desktop boundary stats:** Fixed silent exception handling, now logs errors properly
- **Desktop translation:** Fixed fragmented translation string for cross-paragraph tooltips
- **Anonymous display bug:** Fixed discoveries showing as "Anonymous" even when user didn't check anonymous - now fetches profile data properly
- **Dialog Esc key:** Fixed Share Discovery dialog flickering when pressing Esc (removed 'persistent' prop)
- **Simplified Share Discovery:** Removed superfluous "Related manuscripts" section from dialog
- **Database constraint:** Updated discoveries type constraint to include 'identification' and 'note' types

### Technical Changes

- `CompositionThread` and `LabCompositionThread` now accept boundary parameters
- `LabSettings` stores boundary preferences (mode, delimiter, boost, min matches, min distance)
- Added temporary storage fallback for settings when `lab_engine` not initialized

### Documentation

- Updated help page with cross-paragraph search documentation (English and Hebrew)
- Updated BOUNDARY_SEARCH_SPEC.md with completed desktop implementation details

---

## [5.2.0] - 2026-02-01

### Documentation

- **Help Center rewrite:** Comprehensive bilingual help page covering Search, Parallels, Browse, Lists, and Export features
- **File index:** New `docs/FILE_INDEX.md` with comprehensive listing of all project files

### Codebase Cleanup

- **Root directory cleanup:** Removed unused directories (`backend/`, `backend_legacy/`, `frontend_web/`, `build/`, `Reports/`, `Results/`)
- **Scripts organization:** Moved utility scripts to `scripts/` folder (cleanup, verify, debug scripts)
- **Branch cleanup:** Deleted 25 stale/merged git branches

### UX Improvements

- **Search spinners:** More prominent animated spinners (bars instead of dots, larger size, pulsing text)
- **Parallels search feedback:** Spinner and status now visible in control panel without scrolling
- **Stop button:** Added to regular search (swaps with search button during search), shows partial results when stopped
- **Filter sources badge:** Shows count of enabled filter sources on the expansion header
- **Filter tooltip:** Explains filter feature in both English and Hebrew

### Header Branding

- **Dicta branding:** Header now shows "Dicta Genizah Search" with Hebrew subtitle "אתר הגניזה מבית דיקטה"
- **Mobile optimization:** Header hides on scroll down, reveals on scroll up (mobile only)
- **Responsive logo:** Text hidden on small screens, only icon shows

### Backend Migration: Supabase

- **Complete Supabase migration:** Replaced FastAPI backend with direct Supabase integration
- All authentication now handled by Supabase Auth
- User lists, corrections, and comments stored in Supabase
- Built-in rate limiting and security features

### Authentication Fixes

- **OAuth flow:** Fixed Google OAuth to use Supabase's `sign_in_with_oauth` method with proper state parameter
- **Session handling:** Implicit flow tokens properly extracted from URL hash on callback
- **Forgot password (desktop):** Added password reset link to desktop app login dialog for OAuth users
- **OAuth user guidance:** Web Google signup now shows note about setting password for desktop app login

### Row Level Security (RLS) Fixes

- **RLS policies:** Fixed all INSERT/UPDATE/DELETE policies to use `authenticated` role instead of `public`
- **Column naming:** Updated queries to use correct column names (`author_id` for comments/corrections, `user_id` for others)
- **Profile joins removed:** Removed `profiles` table joins from queries that failed without FK relationships
- **SQL script:** Added `scripts/fix_rls_policies.sql` for bulk RLS policy updates

### Community Feed & Comments

- **Feed loading:** Fixed `get_feed_items` to properly load discoveries, corrections, comments, and joins
- **Comments display:** Fixed comments to appear on browse pages (removed failing profiles join)
- **Profile page:** Fixed to load data from profile storage instead of auth user

### Lists & Projects Management

- **Management mode toggle:** New "Manage lists" button reveals edit controls
- **Icon-based actions:** Replaced dropdown menus with direct action buttons (rename, move to project, delete)
- **Improved UI:** Cleaner interface with actions hidden by default
- **Auto-sync:** Lists automatically sync between devices for logged-in users
- **Soft delete support:** Lists can be recovered after deletion

### Bug Fixes

- **Register button:** Fixed bug where clicking "Register" opened login dialog instead of register
- **Dependencies:** Added missing `gotrue` and `python-dotenv` to requirements.txt

### Documentation

- **English translation:** PRE_LAUNCH_CHECKLIST.md translated from Hebrew to English
- **Documentation reorganization:** New `docs/` structure with guides, plans, and specs

---

## [5.1.0] - 2026-01-27

### Web Platform: Dicta Genizah Search (אתר הגניזה של דיקטה)

The web platform has been rebranded to **"Dicta Genizah Search"** (אתר הגניזה של דיקטה), reflecting our partnership with DICTA. The desktop application remains "Genizah Search Pro".

### Accessibility Compliance (WCAG 2.0 / IS 5568)

- Full compliance with Israeli Standard 5568 and WCAG 2.0 AA accessibility guidelines
- Improved Hebrew RTL layout and text alignment
- Semantic headings with proper sizing
- Enhanced keyboard navigation support

### New Features

- **Automatic Text Source Filtering:** Intelligent filtering based on Sefaria text database
- **Enhanced Variant Search:** Improved letter variation handling (multi-character, 2-to-1 letters)
- **Fullscreen Edit Mode:** Image controls with splitter for side-by-side viewing
- **Fragment Joins System:** Connect related fragments via Discovery Center
- **Exclude Words UI:** Exclude specific words from search results
- **Citation Footer:** Dismissible footer with publishing guidelines

### Browse & Viewer Improvements

- Side-by-side layout for browse page
- Image drag, rotate, and wheel zoom in manuscript viewer
- Image credits/attribution for NLI and Oxford sources
- Title truncation with tooltips for long titles

### UI/UX Improvements

- Desktop app download page with website integration
- SEO metadata for social sharing
- Dark mode fixes across multiple pages
- Dismissible transcription disclaimer banner
- Creator credit in sidebar footer

### Technical Improvements

- Migrated to google-genai SDK (gemini-3-flash-preview)
- SSL certificate verification for all HTTPS requests
- Build optimizations for faster packaging
- Antivirus false positive documentation
- Server-side index building support

### Bug Fixes

- Fixed NLI and Oxford image loading issues
- Fixed version comparison for different component lengths
- Fixed RTL layout overlap and alignment issues
- Fixed theme toggle functionality
- Fixed fullscreen edit image loading

---

## [5.0.0] - 2026-01-19

### Major Release: Web Platform & Community Features

Version 5.0 marks the launch of the **Genizah Search Pro Web Platform** and introduces comprehensive **Community Features**, transforming the software into a collaborative research environment.

---

### Web Platform

- **Public Web Application:** Full-featured web interface accessible from any browser at [genizahsearch.com](https://genizahsearch.com)
- **Mobile Responsive Design:** Optimized experience for tablets and phones with adaptive layouts
- **User Authentication:** Registration, login, and profile management
- **Offline Mode:** Community features work offline and sync when reconnected

### Community Features

- **Discovery Center:** Share and explore research discoveries with the community
  - Voting system for discoveries
  - Pin important discoveries
  - Mark discoveries as answered/resolved
  - Multiple shelfmark references per discovery
  - Document lookup from discovery dialog
- **Comments System:** Add comments to manuscripts with page-specific references
  - Public and private comments
  - Draft support for work-in-progress notes
- **Corrections & Contributions:** Submit corrections to transcriptions
  - Review workflow for submitted corrections
  - Track your contributions in "My Edits & Comments" page

### Admin Features

- User management panel with role assignment
- Corrections review system for approving/rejecting submissions
- Profile editing capabilities for users

### Desktop App Integration

- New dialogs for comments and text editing
- Improved synchronization between desktop and web data
- Consistent page number handling across platforms
- Full offline mode for community tab

### Stability & Performance

- Fixed infinite timer issues causing connection problems
- Improved CSS performance for faster page loads
- Better error handling for offline scenarios
- Disabled reload mode for improved stability

---

## [4.1.1] - 2026-01-12

### Fixes
- Corrected star icon alignment in search results
- Fixed list preview image loading

---

## [4.1.0] - 2026-01

### Personal Lists Management
- New tab for creating and organizing personal manuscript lists
- Browse by list: side panel in Browse tab for navigating custom lists
- List filtering: filter search results based on personal lists

### Interface Refinements
- Compact context view at the bottom of the interface
- Reports saved to user's Documents directory
- Resolved duplicate search results issue

---

## [4.0.0] - 2025

### Major Update: From Search Engine to Research Suite

### Integrated Visual Analysis (IIIF)
- In-app viewer for high-resolution manuscript images
- Direct integration with National Library of Israel and Cambridge University Library
- Sequential page and manuscript navigation
- Built-in zoom and rotation controls

### Oxford Bodleian Integration
- Full support for Oxford Bodleian Library manuscripts
- Neubauer catalog integration
- Part-based and folio-based navigation

### Lab Mode (Experimental)
- Parallel detection algorithm based on Shmidman, Koppel, and Porat (2016)
- Rare letter encoding for spelling variation tolerance

### Additional Features
- Cross-page search
- Enhanced export (Excel, CSV, DOCX)
- Find in text with highlighting
- Composition search for parallel detection

---

## [3.6.0] and earlier

See previous release notes for historical changes.
