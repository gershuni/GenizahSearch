# Dicta Genizah Search Pro 9.2.1

**Collaborative Research Platform for the Cairo Genizah**

A comprehensive research environment for the Cairo Genizah, featuring a **Web Platform** ([Dicta Genizah Search](https://genizahsearch.com)), **Community Features**, and full **WCAG 2.0 Accessibility**.

> **Web Access:** [genizahsearch.com](https://genizahsearch.com) (אתר הגניזה של דיקטה) - Search, browse, and collaborate from any browser

---

## What's New in Version 9.2.1?

**On the web** (deployed 2026-09-17):

- **Ask the Genizah in ChatGPT.** The GenizahSearch GPT searches the corpus, reads pages, finds
  letter-level parallels across several witnesses and returns manuscript details with FJMS
  bibliography, catalog records and source credits, through a bounded API facade with
  background jobs and paginated results. Start from the new **AI tools** page,
  [genizahsearch.com/ai](https://genizahsearch.com/ai), which also links the research skill for
  other AI assistants (with a portable installation guide and a `details` command) and the API guide.
- **Long research searches** run in isolated, resource-limited workers: interactive searches and
  background API jobs (submit, poll, fetch, cancel) no longer hit a time cutoff; the synchronous
  API keeps its deadlines.
- **Faster, steadier search**: one heavy query can no longer stall the whole site (bounded regex
  work, a clear error instead of a hang, API `504 core_timeout`); pattern compilation is about
  7x faster; matching in supervised workers is accelerated.
- Responsa tabular search: **Within Document** now matches its components anywhere in the transcription.

**On the desktop** (installer 9.2.1):

- **Manuscript Viewer**: a window of its own, with its own taskbar button; minimize it, keep the
  search panel in view, and it closes with the app
- **Local-scope search with no results**: one-click "Search the Genizah corpus instead"
- **Composition Search** opens on Letter-level search when its index is built (v9.1.0 promised
  this; a stored default was being read as a choice, so it never happened)
- **Composition Search**: search type and frequency work again after Letter-level -> Chunk
- **Oxford images**: translated notice, a Bodleian link in every failure state, and the
  placeholder icon is no longer shown as a manuscript

Earlier releases: [CHANGELOG.md](CHANGELOG.md). The README's previous What's New sections
(8.1.0 to 9.2.0) are kept in [docs/archive/README_WHATS_NEW_ARCHIVE.md](docs/archive/README_WHATS_NEW_ARCHIVE.md).

---

## Features

### Search

- **Keyword, phrase and pattern search** over the MiDRASH automatic transcriptions of the whole
  corpus, with fuzzy and spelling-variant matching for Hebrew and Judeo-Arabic, a
  **Responsa mode** and a **tabular query builder**; results span page boundaries
- **Letter-level parallels**: paste a passage, or several witnesses of one work, and find the
  manuscripts that carry it (desktop; also via the API); **Composition Search** by chunks
- **Lab Mode**: parallel detection after Shmidman, Koppel and Porat (2016), with rare-letter
  encoding for spelling tolerance
- **Library filter** (show only / hide), exclusion lists per tab, and export to Excel, CSV and DOCX

### Reading

- **IIIF viewer** with high-resolution images from the NLI, Cambridge, Manchester (LUNA), JTS and
  Princeton (Figgy); folio navigation with recto/verso notation; zoom and rotation; **print /
  save as PDF**
- **Oxford Bodleian** support with Neubauer catalog integration, and an honest notice with a
  Bodleian link when no image can be fetched
- **Editions side by side**: MiDRASH transcription, Princeton Geniza Project and Friedberg
  editions, translations and community corrections, each credited to whoever made it, with a
  **Cite this page** button and prominent PGP links
- **Scholarly metadata**: FJMS catalog records, bibliography, joins and measurements; NLI
  catalog data; Hebrew translations of library metadata

### Discovery

- **Computed Identifications** (public beta): manuscripts matched to known works, with the
  evidence shown, on [/computed-identifications](https://genizahsearch.com/computed-identifications)
  and in a connections panel while browsing
- **Visual Genizah Atlas** (preview) and a guided **Start Here** launchpad

### Community and personal work

- Accounts, corrections, comments and discoveries (Supabase-backed)
- **Personal lists**, recently viewed, and cloud sync between web and desktop
- **Joins Lab** and the **Fragment Puzzle** for assembling fragments visually
- **My Library** (desktop): a private side-index of your own PDF, DOCX and TXT files, searched
  alongside or instead of the corpus

### AI and automation

- The **GenizahSearch GPT** in ChatGPT, a **research skill** for other AI assistants, and a
  **public HTTP/JSON API**; see [genizahsearch.com/ai](https://genizahsearch.com/ai)

---

## API

GenizahSearch exposes a public HTTP/JSON API for research automation: keyword/Responsa search (`POST /api/search`), single-manuscript drill-down (`GET /api/browse`), and composition-parallels detection (`POST /api/parallels`). All endpoints are anonymous and rate-limited (120 req/min per endpoint per IP in the default public deployment) and return JSON in a uniform envelope.

Full reference, curl examples, and interactive Swagger UI: [docs/SEARCH_API.md](docs/SEARCH_API.md) · [genizahsearch.com/api/docs](https://genizahsearch.com/api/docs).

---

---

## Getting Started

### Web (Recommended)

Visit [genizahsearch.com](https://genizahsearch.com) to start using Dicta Genizah Search immediately.

### With an AI assistant

Open the **GenizahSearch GPT** in ChatGPT from [genizahsearch.com/ai](https://genizahsearch.com/ai) and ask a question, a name or a passage; no installation. Other assistants can use the [research skill](skills/cairo-genizah-research/README.md) or the [public API](docs/SEARCH_API.md).

### Desktop Installation

1. **Download:** the installer (`GenizahSearchPro_V9.2.1_Setup.exe`, about 512 MB) from the [latest GitHub release](https://github.com/gershuni/GenizahSearch/releases/latest)
2. **Install:** Run the installer and follow instructions
3. **Data Setup:** The software requires the **MiDRASH** dataset (`Transcriptions.txt`)

> **Antivirus Note:** Some antivirus software (Avast, AVG, Windows Defender) may flag the installer as suspicious. These are **false positives** caused by PyInstaller packaging. See [ANTIVIRUS_INFO.txt](ANTIVIRUS_INFO.txt) for details and solutions.

---

---

## Documentation

For detailed documentation, see the [docs/](docs/DOCUMENTATION_INDEX.md) directory:

* **[Documentation Index](docs/DOCUMENTATION_INDEX.md)** - Overview of all documentation
* **[Admin Guide](docs/guides/WEBSITE_ADMIN_GUIDE.md)** - Website management for administrators
* **[Code Index](docs/CODE_INDEX.md)** - Code structure and architecture
* **[Plans](docs/plans/)** - Implementation plans and roadmaps

---

---

## Development

```bash
python -m web.main            # web app on port 8080/8081
python genizah_app.py         # desktop app
python scripts/run_local_tests.py   # the test suite, in bounded lanes (never one pytest tests/ process)
```

Python 3.10+, NiceGUI (web), PyQt6 (desktop). Read [CLAUDE.md](CLAUDE.md) for how the repo is
organised and [docs/guides/DEVELOPER_GUIDE.md](docs/guides/DEVELOPER_GUIDE.md) before changing
environment variables or data paths. Release process: `python scripts/bump_version.py X.Y.Z`,
then the steps it prints.

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
