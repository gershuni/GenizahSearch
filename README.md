# Dicta Genizah Search Pro 11.0.0

**Collaborative Research Platform for the Cairo Genizah**

A comprehensive research environment for the Cairo Genizah, featuring a **Web Platform** ([Dicta Genizah Search](https://genizahsearch.com)), **Community Features**, and full **WCAG 2.0 Accessibility**.

> **Web Access:** [genizahsearch.com](https://genizahsearch.com) (אתר הגניזה של דיקטה) - Search, browse, and collaborate from any browser

---

## What's New in Version 11.0.0?

**Pick from the shelf**

On the website and in the desktop app, **Pick from the shelf** finds a manuscript where it stands
on its library's shelf: library, collection, volume and shelfmark, with counts and a filter in every
column. A click on a shelfmark shows its details beside the shelf before you open it: its image,
its title and the first pages of its text, its catalogue and bibliography entries, scholarly
transcriptions, joins and links to the library's own viewer. Signs on the shelfmarks show which
manuscripts the Princeton Geniza Project knows, which have a scholarly transcription and which have
joins. Tick several shelfmarks, or a whole volume, to open them together in the Reading Desk, add
them to a list or the Fragment Puzzle, join several fragments at once, or exclude them from a
search. Browse shows where each manuscript stands on the shelf, and Previous / Next manuscript
follow the shelf. The shelfmarks can be seen as a list, a table with their details, or thumbnails.
In the desktop app, Ctrl+W opens the shelf from anywhere. On the website, manuscript images appear
sooner on Browse. Also new: images from
the Katz Center and the Bodleian's links to each leaf, part and volume. The desktop Reading Desk
takes the website's new shape: one fragment at a time beside its text, all the images, or the join's
puzzles; on the website it also moves fragments up or down and gives every image its links.

Earlier releases: [CHANGELOG.md](CHANGELOG.md).

---

## Features

### Search

- **Keyword, phrase and pattern search** over the MiDRASH automatic transcriptions of the whole
  corpus and the FGP and PGP scholarly transcriptions, with fuzzy and spelling-variant matching for Hebrew and Judeo-Arabic, a
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
- **Pick from the shelf**: find a fragment by library, collection, volume and shelfmark, as on the
  FGP site, with counts and a filter in every column; Browse shows the manuscript's place on the
  shelf, and previous / next follow the shelf
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

- **GenizahSearch in ChatGPT** (an app you add from [genizahsearch.com/ai](https://genizahsearch.com/ai);
  the GenizahSearch GPT remains until 2026-12-11), a **research skill** for other AI assistants, and a
  **public HTTP/JSON API**

---

## API

GenizahSearch exposes a public HTTP/JSON API for research automation: keyword/Responsa search (`POST /api/search`), single-manuscript drill-down (`GET /api/browse`), and composition-parallels detection (`POST /api/parallels`). All endpoints are anonymous and rate-limited (120 req/min per endpoint per IP in the default public deployment) and return JSON in a uniform envelope.

Full reference, curl examples, and interactive Swagger UI: [docs/SEARCH_API.md](docs/SEARCH_API.md) · [genizahsearch.com/api/docs](https://genizahsearch.com/api/docs).

---

---

## Getting Started

### Web 

Visit [genizahsearch.com](https://genizahsearch.com) to start using Dicta Genizah Search immediately.

### With an AI assistant

Add **GenizahSearch to ChatGPT** from [genizahsearch.com/ai](https://genizahsearch.com/ai) and ask a question, a name or a passage; no installation (the GenizahSearch GPT remains until 2026-12-11). Other assistants can use the [research skill](skills/cairo-genizah-research/README.md) or the [public API](docs/SEARCH_API.md).

### Desktop Installation

1. **Download:** the installer (`GenizahSearchPro_V11.0.0_Setup.exe`, about 667 MB) from the [latest GitHub release](https://github.com/gershuni/GenizahSearch/releases/latest).
2. **Install:** Run the installer and follow instructions
3. **Data Setup:** The software requires the **MiDRASH** dataset (`Transcriptions.txt`)

---

---

## Documentation

For detailed documentation, see the [docs/](docs/DOCUMENTATION_INDEX.md) directory:

* **[Documentation Index](docs/DOCUMENTATION_INDEX.md)** - Overview of all documentation
* **[Code Index](docs/CODE_INDEX.md)** - Code structure and architecture

---

---

## Development

```bash
python -m web.main            # web app on port 8081 (GENIZAH_PORT)
python genizah_app.py         # desktop app
python scripts/run_local_tests.py   # the test suite, in bounded lanes (never one pytest tests/ process)
```

Python 3.11, NiceGUI (web), PyQt6 (desktop). Read [docs/architecture/OVERVIEW.md](docs/architecture/OVERVIEW.md)
for how the repo is organised, [CLAUDE.md](CLAUDE.md) for project context, and
[docs/guides/DEVELOPER_GUIDE.md](docs/guides/DEVELOPER_GUIDE.md) before changing
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
Special thanks to Avi Shmidman, Elisha Rosenzweig, Ephraim Meiri, Elazar Gershuni, Itai Kagan, Elnatan Chen, and Adiel Breuer.
