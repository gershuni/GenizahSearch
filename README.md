# Genizah Search Pro 7.1.0

**Collaborative Research Platform for the Cairo Genizah**

A comprehensive research environment for the Cairo Genizah, featuring a **Web Platform** ([Dicta Genizah Search](https://genizahsearch.com)), **Community Features**, and full **WCAG 2.0 Accessibility**.

> **Web Access:** [genizahsearch.com](https://genizahsearch.com) (אתר הגניזה של דיקטה) - Search, browse, and collaborate from any browser

---

## What's New in Version 7.1.0?

### v7.1.0: FIST Gap Fill & Expanded Catalog

GenizahSearch now covers **255,615 manuscript records** — 38,673 new records added from the Friedberg Genizah Project database (FIST.db), an 18% expansion across 52 libraries including 7 newly registered collections.

* **38,673 new manuscripts:** Records from JTS (13,520), Cambridge (12,641), Mosseri (4,862), British Library (2,982), Manchester (1,741), and 47 more libraries — all browsable, searchable by title/shelfmark, with NLI images and FJMS catalog enrichment
* **Metadata-only search:** Title and shelfmark search now returns records even without transcription text. Browse page shows metadata, images, and scholarly data instead of an error
* **7 new library codes:** Solomon Halberstam, Reinach, Vatican, Central Archives, JC Mainz, Corwin, Mehlman — with Hebrew translations
* **Shelfmark normalization:** Yevr→EVR and Halper→Genizah aliases for cross-collection search compatibility

### Previous Releases

### v7.0.0: Fragment Puzzle & Community Publishing

* **Fragment Puzzle:** Visual canvas for arranging manuscript fragments side-by-side. Background removal, zoom/rotate/crop, folio navigation, export as composite PNG (both apps)
* **Community Publishing:** Share puzzle joins with the research community via Discoveries Center (both apps)

### v6.5.3: Image Viewer — Copy & Save
* **Right-click image menu:** Copy or save manuscript images directly from the viewer with rotation preserved (desktop)

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

1. **Download:** Get `GenizahSearchPro_V6.1.1_Setup.exe` from the **Assets** section
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

* **Development:** Hillel Gershuni
* **Data Sources:**
  - Stoekl Ben Ezra et al. (2025). *MiDRASH Automatic Transcriptions of the Cairo Geniza Fragments*. ([doi.org/10.5281/zenodo.17734473](https://doi.org/10.5281/zenodo.17734473))
  - [Princeton Geniza Project (PGP)](https://geniza.princeton.edu/) — Curated transcriptions, translations, and metadata for Cairo Genizah documents
  - Fragment of the Jewish Manuscript Studies (FJMS) — Domain classifications, scientific joins, and catalog records
* **Lab Mode Algorithm:** Based on [Shmidman, Koppel, and Porat (2016)](https://arxiv.org/abs/1602.08715)

**Acknowledgments:**
Developed with the support of **DICTA**.
Assisted by **Claude**, **Gemini**, and **GPT**.
Special thanks to Avi Shmidman, Elisha Rosenzweig, Efraim Meiri, Elazar Gershuni, Itai Kagan, Elnatan Chen, and Adiel Breuer.

