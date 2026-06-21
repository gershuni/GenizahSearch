# FGP Transcriptions — Integration Plan (next phases)

> Status: **Data prepared (2026-06-18). Not yet integrated.** This document is the
> handoff for the phases that incorporate the FGP transcription corpus into the
> app(s). The data asset is built and validated; integration (search + chooser)
> is the remaining work.

## TL;DR

We imported the Friedberg Genizah Project (FGP) transcription archive into a
local, **gitignored** sidecar `fgp_data/`, extracted searchable text, and resolved
the GenizahSearch join key. What's ready:

- **45,034 transcription/translation rows** in `fgp_data/fgp_transcriptions.db`
  (41,692 transcriptions + 2,725 Hebrew + 617 English translations), schema
  **mirroring the PGP `document_sources` model**.
- **`sys_id` resolved for 45,006 / 45,034 (99.94%)** — equals the corpus
  `system_number` in `libraries.csv` (100% of resolved ids validated against it).
- **recto/verso (`page_info`) for 18,222 rows** via the FGP C-number.
- Plus author/title/domain enrichment (non-CUL), copied raw source metadata, and
  6 re-runnable build scripts.

Two phases remain: **(1) Search/Indexing** — make FGP text searchable; **(2)
Transcription chooser** — surface FGP as a selectable source alongside PGP / user
corrections / V0.8. Both are described below with concrete recipes.

The data store itself is fully documented in **`fgp_data/README.md`** (gitignored,
lives next to the data). This file is the *integration* doc and is tracked in git.

---

## 1. The data asset (what exists now)

Location: `fgp_data/` at repo root — **entirely gitignored** (`.gitignore`:
`fgp_data/`). ~2 GB total.

```
fgp_data/
├── transcriptions/            # 48,317 source files, tree preserved (PDFs + XML)
├── fgp_transcriptions.db      # ~330 MB — the searchable store (USE THIS)
├── metadata/                  # copied source metadata (JSON + info.txt)
├── manifest.csv / manifest_by_shelfmark.json / manifest_summary.json
├── README.md                  # full data-store reference
└── fgp_copy.py … fgp_tail.py  # 6 build scripts (provenance / regeneration)
```

Source drive: `D:\fgp_transcriptions` (NLI FJMS export, April 2026) — an external
drive that disconnects; **the `fgp_data/` copy is self-sufficient** for everything
except re-copying/re-enriching from source.

### `fgp_transcriptions.db` — primary table `fgp_transcriptions`

One row per transcription/translation PDF. Mirrors PGP `document_sources` so the
chooser integration is a near-copy of the PGP path.

| Column | Meaning |
|---|---|
| `id` | PK |
| `collection` | CUL, JTS, Oxford, BL, … (24 institutions) |
| `shelfmark` | FGP folder shelfmark (e.g. `T-S 8H16.20`) |
| `c_number` | FGP per-image catalog id `C#####` (24,184 rows) — **the key to recto/verso** |
| `image_id` | FGP photo id (filename numeric prefix) |
| `sys_id` / `mms_id` | **GenizahSearch join key** = `libraries.csv` system_number = NLI Alma id. Resolved for 45,006 rows. |
| `sysid_method` | how it was resolved (provenance) — see §6 |
| `page_info` | `'recto'` / `'verso'` (== PGP `document_fragments.page_info`); 18,222 rows |
| `folio_num`, `image_side` | leaf number + raw `1r`/`1v` side |
| `doc_relation` | `'Digital Edition'` (transcription) / `'Digital Translation'` |
| `language` | translation language (`Hebrew`/`English`); `NULL` for editions |
| `content` | **full plain-text transcription** (UTF-8 Hebrew, reading order) |
| `content_length`, `n_pages`, `heb_ratio` | size + quality signals |
| `sections` | JSON `[{"page_num":1,"text":"…"}]` per PDF page (analog of PGP canvas sections) |
| `source_scholar` | `'FGP'` (source has no per-transcriber field) |
| `author_en/he`, `title_en/he`, `domain`, `language_meta`, `script`, `creation_type` | manuscript metadata (non-CUL only — see §7) |
| `inventory_id`, `full_shelfmark` | from `C*_info.txt` (non-CUL) |
| `rel_path`, `filename`, `size_bytes` | pointer to the PDF under `transcriptions/` |

Companion tables: `fgp_cnumber_info` (76,075 rows — per-fragment `C# → AlmaId /
shelfmark / inventory` bridge), `fgp_shelfmark_meta` (45,006 raw metadata records),
`fgp_meta` (build provenance key/value). Indexes on `shelfmark`, `collection`,
`c_number`, `doc_relation`, `sys_id`.

---

## 2. How transcriptions work today (the model to mirror)

A manuscript fragment is keyed by **`sys_id`**. The transcription "chooser" lets the
user pick among versions for the current fragment + page.

- **UI:** `web/components/version_selector.py::create_version_selector(document_id,
  page_number, original_text, on_version_change=…, pgp_transcription=…,
  all_sources=…)`. `document_id` is the `sys_id`; `page_number` is 1=recto/2=verso.
  It consumes a list of **source dicts** (`all_sources`).
- **Data:** `shared/document_service.py::get_all_sources_for_fragment(sys_id)`
  returns the PGP source dicts (joined via `document_fragments.sys_id →
  documents.pgpid → document_sources`), each carrying `page_info`,
  `doc_relation`, `source_scholar`/attribution, `language`, `content`, `sections`.
  Page text is extracted with `get_section_for_page(content, page_num, sections,
  page_info)`.
- **Source-dict shape the chooser expects** (per type): PGP edition
  `{source:'pgp', attribution, content, pgpid, source_id, is_pgp, …}`; PGP
  translation `{source:'translation', language, content, …}`; user correction
  `{source:'user', author, content, correction_id}`; V0.8 `{source:'V0.8',
  content, is_original}`. Default selection priority: PGP edition → approved user
  correction → V0.8.
- **Wiring sites:**
  - Web: `web/pages/browse.py` (~lines 1020, 1068, 1100, 2744 build `sources`;
    `create_version_selector(... all_sources=...)` at ~4214).
  - Desktop: `gui_threads.py` (~704–710, 838–844 call the same service fns);
    grouping mirrored at `genizah_app.py:4889`.
  - `web/document_service.py` is a thin re-export shim of `shared/document_service.py`.

**Both apps must be wired** (project invariant — web NiceGUI + desktop PyQt6 share
the service layer; the chooser exists in both).

---

## 3. Phase 1 — Search / Indexing (make FGP text searchable)

Goal: FGP transcription text participates in search the way PGP/V0.8/LOCAL text
does. The corpus search is two-phase (Tantivy candidates → regex filter/highlight);
LOCAL "My Library" already shows the pattern of a **separate Tantivy side-index
merged via RRF (k=60) after dedup** (see `genizah_core` + the v7.14 LOCAL work).

Open design choices for this phase:
- **Index target:** add FGP text to the existing transcription search field vs. a
  separate FGP side-index merged by `sys_id`. Mirror the LOCAL RRF merge if keeping
  it separate.
- **Granularity:** index per row (per folio side) keyed by `sys_id` (+ `page_info`),
  so hits map back to a fragment/side.
- **Dedup vs PGP/V0.8:** a `sys_id` may already have PGP/V0.8 transcription text;
  decide whether FGP is additive (another searchable witness) or deduped.
- **Quality gate:** consider `heb_ratio` / `content_length` to drop near-empty rows.
- **Scope:** index `Digital Edition` rows; translations optional.

No code is committed for this yet — it's a clean phase.

---

## 4. Phase 2 — Transcription chooser integration

Make FGP a selectable source in the version selector, mirroring PGP. Minimal,
low-risk because the data already matches the `document_sources` shape.

**4a. New service (mirror `PgpService`).** Add `FgpService` (or functions) in
`shared/` that opens `fgp_data/fgp_transcriptions.db` using the **same path
resolution as `PgpService`**: `_find_project_root()` (walks up for `libraries.csv`)
→ `<root>/fgp_data/fgp_transcriptions.db`, with a `%LOCALAPPDATA%/GenizahSearchPro/
data/fgp_data/fgp_transcriptions.db` override for desktop, and per-thread
connections (`thread_safe=True`) for the web app.

```python
def get_fgp_sources_for_fragment(sys_id: str) -> list[dict]:
    # SELECT * FROM fgp_transcriptions WHERE sys_id = ? ORDER BY doc_relation, folio_num
    # map each row -> chooser source dict:
    #   {'source':'fgp', 'attribution':'FGP (Friedberg Genizah Project)',
    #    'doc_relation': row['doc_relation'], 'language': row['language'],
    #    'content': row['content'], 'sections': json.loads(row['sections']),
    #    'page_info': row['page_info'], 'fgp_c_number': row['c_number'],
    #    'is_fgp': True}
```

**4b. Page filtering.** Prefer `page_info` (recto/verso) to select the row for the
current `page_number`; fall back to `sections`/`page_num` when `page_info` is NULL
(the no-C-number rows). Reuse `get_section_for_page` semantics.

**4c. UI.** Extend `create_version_selector` with an `fgp_sources` param (or merge
into `all_sources` with `source='fgp'`) and render an FGP group with its own badge
(distinct from PGP). Decide default-selection priority relative to PGP/user (see §8).

**4d. Wire both apps** at the sites in §2 (web `browse.py`; desktop `gui_threads.py`
+ `genizah_app.py`).

---

## 5. Deployment & packaging

- **The DB is gitignored** and `deploy.sh` only does `git pull` + restart — it does
  **not** move sidecars. So for the **web** app, `fgp_transcriptions.db` (~330 MB)
  must be **scp'd to the server** (`/home/ubuntu/GenizahSearch/fgp_data/`) out of
  band, before/with the code deploy (same discipline as the other sidecar DBs —
  "scp DBs first, then push code"). The 1.4 GB of PDFs are **not** needed on the
  server if only the extracted text is served.
- **Desktop:** ship `fgp_transcriptions.db` as a sidecar via the installer
  (`GenizahSearchPro.spec` / `CompileScriptGenizah.iss`), resolved from the
  LOCALAPPDATA path like the other DBs. 330 MB adds to installer size — confirm
  acceptable, or gate behind a feature flag / optional download.
- Add a `WEB_FGP_ENABLED` / `FGP_TRANSCRIPTIONS_ENABLED` env flag so the feature
  degrades gracefully when the DB is absent (service returns `[]`).

---

## 6. `sysid_method` provenance values

How each row's `sys_id` was resolved (for auditing / trust):

| method | meaning | confidence |
|---|---|---|
| `fjms_cnumber` | FGP C-number → `fjms_enrichment.db extra_info.FGP → AlmaId` | highest (per-image) |
| `fjms_shelfmark` | exact shelfmark → `extra_info.Shelfmark → AlmaId` (unique) | high |
| `fjms_shelfmark_norm` | space-normalized shelfmark match | high |
| `infotxt_prior` | non-CUL `C*_info.txt` `MMS` (kept where fjms had no hit) | high |
| `libraries_csv` | exact normalized shelfmark in `libraries.csv` | high |
| `lib_shelfmark_token` | exact shelfmark token inside a range/multi-shelfmark cell | medium (audited) |
| `NULL` | unresolved (28 rows) | — |

**Key fact (do not re-derive):** the FGP **C-number** is the per-image key, and
`fist_data/fjms_enrichment.db` `extra_info` is the complete FGP image catalog
(742,853 rows) mapping `FGP → AlmaId` and `Shelfmark → AlmaId`, where
**`AlmaId == sys_id`**. ⚠ Do **not** use `nli_crossref.db` `FGPNumber` for this —
it's a different/derived numbering that disagrees with the FGP catalog (the
Phase-30 "FGP photo number ≠ FL id" trap).

---

## 7. Known gaps, caveats & decisions for the next phase

- **28 unresolved `sys_id`** (0.06%) — left `NULL` on purpose (no confident match
  beats a wrong id): `ENA 1493.4`, `ENA 2072.2` (absent from corpus), `Or.1080
  4.52` (FGP variant of corpus `Or.1080 14.52`), and 8 Oxford `MS heb. *_NN`
  fragments whose FGP part-numbering isn't in `libraries.csv`. Needs human
  adjudication or corpus additions.
- **recto/verso coverage = 18,222 rows.** Rows without a C-number (≈20,850, mostly
  CUL's direct-`Trans/` layout) have no side from the C-number method; deciding
  side for those needs a content/PDF-order heuristic (separate work).
- **CUL has no source metadata** (no `MetadataOnShelfmark.JSON`/`info.txt`), so
  `author`/`title`/`domain` are blank for the ~21.6K CUL rows. Now that `sys_id` is
  resolved, these are **joinable from the fjms `catalog` table** by AlmaId if the
  chooser wants to show them.
- **Attribution is generic `'FGP'`** — the source carries no per-scholar/editor
  field. Display should credit FGP / Friedberg Genizah Project / NLI (licensing &
  outreach requirement).
- **Overlap with PGP (design decision):** a `sys_id` may have BOTH a PGP and an FGP
  transcription. Decide ordering, labeling, and whether to dedup near-identical
  text. Recommend showing both as distinct witnesses with clear source badges.
- **Multiple FGP rows per `sys_id`** (one per image/side) — the chooser must group
  by `page_info`/`folio_num` and present the right row for the current page.
- **Translations & XML:** Hebrew/English translation rows are included
  (`doc_relation='Digital Translation'`). The 3,283 structured XML files were copied
  but **not parsed** — parse only if PDF text proves insufficient somewhere.

---

## 8. Provenance / how to regenerate

Pipeline scripts live in `fgp_data/` (run in order; see `fgp_data/README.md` for
details). Steps 1 & 3 need `D:\fgp_transcriptions` connected; steps 2,4,5 use only
the local copy + the two sidecar DBs + `libraries.csv`.

1. `fgp_copy.py` — copy transcription PDFs/XML + build manifests.
2. `fgp_extract.py` — PyMuPDF text extraction → `fgp_transcriptions.db` (mirrors PGP).
3. `fgp_enrich.py` — copy source metadata, add author/title/domain + companion tables.
4. `fgp_resolve_sysid.py` — fjms-authoritative `sys_id` resolution.
5. `fgp_pageinfo.py` then `fgp_tail.py` — recto/verso + tail `sys_id` fallbacks.

---

## 9. Pointers

- Data-store reference: `fgp_data/README.md`
- Transcription UI: `web/components/version_selector.py`
- Transcription service (template): `shared/document_service.py` (`PgpService`,
  `get_all_sources_for_fragment`, `get_section_for_page`)
- Web wiring: `web/pages/browse.py`; desktop wiring: `gui_threads.py`, `genizah_app.py`
- Crossref key source: `fist_data/fjms_enrichment.db` `extra_info` / `computed_measurements`
- Memory: `project_fgp_transcriptions_sidecar`
