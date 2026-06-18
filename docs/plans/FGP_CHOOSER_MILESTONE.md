# FGP Transcriptions in the Version Chooser — Milestone Readiness Doc

> **Status: GROUNDWORK / NOT STARTED (prepared 2026-06-18).** This is a non-destructive
> readiness doc for a *future* milestone. It is intentionally **not** a GSD milestone yet —
> a separate milestone (**v8.2.0 "Web Joins Lab"**, phases 117–121) is being executed in
> parallel, and starting this one now (`gsd-new-milestone`) would replace `REQUIREMENTS.md`
> and reset `STATE.md`, clobbering live v8.2.0 state. Instantiate this only **after v8.2.0
> ships** (see [Runbook](#runbook--instantiate-after-v820-ships)).
>
> Companion artifacts: the broader data/integration reference is
> `docs/plans/FGP_TRANSCRIPTIONS_INTEGRATION_PLAN.md`; the data-store reference is
> `fgp_data/README.md`; memory `project_fgp_transcriptions_sidecar`. Seed: `.planning/seeds/
> SEED-004-fgp-chooser-integration.md`.

---

## 1. Goal & scope

Surface the prepared FGP (Friedberg Genizah Project) transcription corpus as a **distinct,
selectable transcription source alongside PGP** in the version chooser, in **both** apps.

The data asset already exists (built 2026-06-18, gitignored): `fgp_data/fgp_transcriptions.db`
— 387 MB, 45,034 rows, schema mirroring PGP `document_sources`, `sys_id` resolved for 99.94%,
recto/verso (`page_info`) for 18,222 rows.

**IN scope**
- FGP's **extracted transcription text** — a faithful PDF→text conversion already committed to
  the DB — offered as a selectable source in the transcription chooser, clearly distinct from PGP.

**OUT / deferred**
- (a) Making FGP text **searchable** + any dedup-vs-PGP/V0.8 logic — a separate later milestone.
- (b) **Displaying the original FGP source PDFs.** The source files were mostly PDFs, converted
  truthfully to text in the DB; rendering the PDFs themselves is a possible **later stage**, not
  this milestone. Operational note for that stage: the ~1.4 GB FGP PDF tree
  (`fgp_data/transcriptions/`) is gitignored and **not** deployed (only the 387 MB text DB ships),
  so PDF display would require shipping/serving those files; it could likely reuse the v7.15
  "PDF Page Image" renderer (the `shared/` PDF page-render path used by My Library).

**Project invariant:** both apps maintained (NiceGUI web + PyQt6 desktop, shared service layer).

This doc was Codex-reviewed (gpt-5.x); HIGH/MEDIUM findings are folded in, and the two
"missing surface" findings were re-verified against the code.

---

## 2. Draft requirements (FGP-01 … FGP-12)

| ID | Requirement |
|----|-------------|
| **FGP-01** | New `shared/fgp_service.py` mirroring `PgpService` sidecar resolution; `get_fgp_sources_for_fragment(sys_id)` returns chooser-shaped source dicts. |
| **FGP-02** | FGP `sections` (keyed `page_num`) normalized so `get_section_for_page()` (matches `canvas_num`) splits recto/verso correctly; no-`page_info` rows must not show full text on both sides. |
| **FGP-03** | A shared **source-kind discriminator** (`source='fgp'`) + a normalized source-kind helper used at every classifier; **namespaced IDs** (`pgp:`/`fgp:`) to prevent integer `source_id` collisions in a merged `all_sources`. |
| **FGP-04** | Shared env flag `FGP_TRANSCRIPTIONS_ENABLED` (read in `shared/`); degrades to `[]` when off or DB absent. Optional `WEB_FGP_ENABLED` web override. `shared/` must not import `web/`. |
| **FGP-05** | Web: FGP merged + classified at **all** chooser surfaces — `browse_enrichment.py`, reading-desk sites in `browse.py`, and `search_results.py`. |
| **FGP-06** | Desktop: FGP merged + classified at **all** chooser surfaces — `genizah_app.py` reading desk + `_populate_pgp_combo`, and `desktop/result_dialog.py`. |
| **FGP-07** | FGP renders as its **own group/badge** in `version_selector.py` (not folded into the green "PGP" group) + desktop combo parity; default-selection rule defined (recommend PGP-first, FGP additive). |
| **FGP-08** | i18n: Hebrew strings for the FGP group label / badge / attribution, shipped *with* the UI. |
| **FGP-09** | Packaging + deploy: desktop bundling (`GenizahSearchPro.spec`, `build_app.bat`, `scripts/checkpoint_sidecars.py`, sidecar updater as needed); web scp + atomic-rename deploy runbook. |
| **FGP-10** | Attribution/licensing sign-off — exact FGP / Friedberg / NLI credit text + any usage constraints confirmed before ship (release criterion, not just a label). |
| **FGP-11** | Tests: `tests/test_fgp_service.py` + behavior tests (classification/default order; PGP+FGP overlap; FGP-only edition; translation-only; no-`page_info` split; flag-off; DB-absent; desktop combo source kinds). |
| **FGP-12** | Searchability-deferral guardrail: do **not** touch Tantivy indexing, `get_sys_ids_with_transcriptions`, `has_pgp`, PGP filters/badges, API/export metadata, or PGP tag search. |

---

## 3. Open decisions (resolve at discuss-phase)

- **Desktop packaging of the 387 MB DB:** bundle into the installer vs **on-demand download**.
  *Recommend: ship as a DOWNLOAD via the existing `SidecarUpdateThread`/`SidecarDownloadThread`
  infra (add FGP to `service_map` + the GitHub `data-latest` `sidecar-versions.json` manifest) —
  cheap, no installer bloat, services already degrade gracefully when the DB is absent.* This is a
  small extension, NOT the full thin-installer milestone (see `.planning/seeds/
  SEED-005-thin-installer-data-manager.md`). Bundling (+387 MB on a base that already carries
  ~3.4 GB of DBs) remains the fallback if the `data-latest` release isn't usable.
- **Flag default after desktop ship:** a packaged desktop that bundles the DB but defaults the
  flag OFF would *not actually surface FGP*. *Recommend: desktop defaults ON once shipped; web
  stays env-gated.*
- **Default-selection priority** when a fragment has both PGP and FGP editions (code auto-picks
  `editions[0]`). *Recommend: keep PGP-first default, FGP additive below.*
- **PGP + FGP overlap on the same `sys_id`:** show both as distinct witnesses (don't dedup in
  this milestone — dedup is part of the deferred searchability work).
- **Attribution/licensing** text + constraints (FGP-10).

---

## 4. Code-grounded integration recipe

### 4.1 New service — `shared/fgp_service.py` (mirror `PgpService`)
- Copy the sidecar pattern from `shared/document_service.py:43-134`: constants
  `_SIDECAR_DIR="fgp_data"`, `_SIDECAR_FILENAME="fgp_transcriptions.db"`, `_find_project_root()`,
  LOCALAPPDATA override, `thread_safe` (web `ThreadLocalConnection` / desktop `sqlite3`), read-only
  URI, `is_available()`, `get_fgp_service()` singleton + `reset_fgp_service()`.
- `get_fgp_sources_for_fragment(sys_id) -> list[dict]` → chooser-shaped dicts with discriminator:
  `{'source':'fgp', 'is_fgp':True, 'doc_relation', 'language', 'content', 'sections',
  'page_info', 'source_scholar':'FGP', 'attribution':'FGP (Friedberg Genizah Project)',
  'fgp_c_number'}`.
- **Section normalization (FGP-02):** FGP `sections` are keyed `page_num`; `get_section_for_page()`
  Path 1 matches `canvas_num`. Normalize FGP sections to `canvas_num` in the service (or extend
  the shared helper) — else no-`page_info` rows show full text on BOTH sides.

### 4.2 Source-kind contract (FGP-03)
- Classifiers today key on `'Edition'`/`'Translation'` substring in `doc_relation`
  (`version_selector.py:120-130`; desktop `_populate_pgp_combo`). Add a shared normalized
  source-kind helper used at *every* surface.
- Namespace IDs/cache keys (`pgp:123`/`fgp:123`) — both PGP and FGP carry integer `id`/`source_id`
  that would collide when merged into one `all_sources` list.

### 4.3 Feature flag (FGP-04)
- Shared env flag `FGP_TRANSCRIPTIONS_ENABLED` read in `shared/fgp_service.py` (degrade to `[]`
  when off / DB absent). Optional `WEB_FGP_ENABLED` web override via
  `web/feature_flags.py::_env_enabled()`. Do **not** import `web/` from `shared/`.

### 4.4 Merge + classify at ALL chooser surfaces (re-verified — original scan missed two)
**Web — fetch + merge FGP into `all_sources`:**
- `browse_enrichment.py::_pgp_sync` (`:75-78`); reading-desk sites `browse.py` (`~1008/1068/2744`).
- `search_results.py:1208` (Advanced Search result detail) — *was missing in first pass*.

**Web — render `create_version_selector`:** `browse.py:4214`, `browse_enrichment.py:462`,
`search_results.py:2016` (*was missing*).

**Desktop:**
- Reading desk: `genizah_app.py` loader (`~10210`) + renderer (`~10674`); translation grouping
  `~4887`; the parent `_populate_pgp_combo` classifier.
- `desktop/result_dialog.py` (*was missing*): `PGPSourceWorker` (import `:18`, start `:2443`),
  `rd_version_combo` via `parent._populate_pgp_combo(...)` (`:1439`), `_rd_load_version_content`
  (`:1303`). On the desktop parity checklist.
- Centralize the page-filter (`browse_enrichment.py:288-300`) rather than copying it per site.

> **NOTE — line numbers will stale** while v8.2.0 phases 119–121 land. Before planning, re-grep
> (see Runbook step 3).

### 4.5 Packaging / deploy (FGP-09)
- **Desktop (preferred — download, no bloat):** add FGP to the existing **`SidecarUpdateThread`
  `service_map`** + the GitHub `data-latest` `sidecar-versions.json` manifest (with **`sha256` +
  exact byte size**, not just version) so it downloads to
  `%LOCALAPPDATA%\GenizahSearchPro\data\fgp_data\` (the path the service checks first); add
  `reset_fgp_service()` to the post-download service-reset. Two Codex guardrails: (1) **show
  install status for FGP in the chooser** — empty FGP because the DB isn't downloaded must NOT look
  like a data-quality bug (offer "Install FGP data"); (2) build a **small reusable
  artifact-status/install widget**, not a one-off FGP download UX the future Data Manager
  (SEED-005) will throw away. See SEED-005.
- **Desktop (fallback — bundle):** add the DB to `GenizahSearchPro.spec` datas **and**
  `build_app.bat` add-data lines **and** `scripts/checkpoint_sidecars.py`. Inno `.iss` recursive
  copy only works once the dist tree already contains the DB. (+387 MB on a ~3.4 GB base.)
- **Web:** `mkdir -p /home/ubuntu/GenizahSearch/fgp_data` → upload to a temp filename → **atomic
  rename** → verify permissions → set env → **restart the process** (flags read at
  import/startup). "scp DBs first."

### 4.6 Tests (FGP-11)
- `tests/test_fgp_service.py` mirroring `tests/test_document_service.py` (source-dict shape, page
  filtering, section normalization).
- Behavior tests: PGP+FGP overlap, FGP-only edition, translation-only row, no-`page_info` split,
  flag-off, DB-absent, desktop combo source kinds. (Note: existing `test_version_selector_pending.py`
  is mostly *static* source checks — add real behavior assertions.)

### 4.7 Searchability-deferral guardrails (FGP-12)
Chooser-only. Do **not** touch: Tantivy indexing, `get_sys_ids_with_transcriptions`, `has_pgp`,
PGP filters/badges, API/export metadata, PGP tag search — those are search/discovery semantics
belonging to the deferred milestone.

---

## 5. Proposed condensed phase shape (phases ~122–124)

Thin de-risk spine first, then theme-merged.

- **Phase A — Data + contract spine:** `shared/fgp_service.py` (+ section normalization), the
  shared source-kind helper + namespaced IDs, `FGP_TRANSCRIPTIONS_ENABLED`, and
  `test_fgp_service.py` + flag-off/DB-absent tests. **No UI, no packaging.** → FGP-01..04, 11, 12
- **Phase B — Chooser integration (both apps) + i18n:** merge + classify at all surfaces (web:
  browse, browse_enrichment, search_results; desktop: reading desk + ResultDialog +
  `_populate_pgp_combo`); FGP group/badge in `version_selector.py`; desktop combo parity;
  default-selection rule; Hebrew strings with the UI; grouping/behavior tests. → FGP-05..08, 11
- **Phase C — Packaging + deploy + UAT (release-gated):** add the 387 MB DB to
  `.spec`/`build_app.bat`/checkpoint + updater; web scp + atomic-rename runbook; bilingual
  attribution sign-off; UAT; flip the flag in prod after the DB is in place. → FGP-09, 10

---

## 6. Runbook — instantiate AFTER v8.2.0 ships

1. `/gsd-complete-milestone v8.2.0` (archives ROADMAP/REQUIREMENTS, resets STATE).
2. `/gsd-new-milestone` for the FGP chooser, feeding this doc / the seed → phases start at **122**.
3. **Before `/gsd-plan-phase 122`, re-grep** (line refs stale while 119–121 land): `rg` for
   `get_all_sources_for_fragment`, `create_version_selector`, `_populate_pgp_combo`,
   `PGPSourceWorker`, `get_section_for_page`.
4. `/gsd-discuss-phase 122` → resolve §3 open decisions → `/gsd-plan-phase` → execute.

---

## 7. Verification (of the integration, when built later)

- Unit: `pytest tests/test_fgp_service.py tests/test_document_service.py` + chooser grouping tests.
- Manual web: with the DB present + flag on, open `/browse` and a `/search` result detail on a
  fragment with a known FGP `sys_id`; confirm a distinct "FGP — Friedberg" entry appears in the
  chooser (not folded into PGP), selects correctly, shows the right recto/verso text.
- Manual desktop: ResultDialog version combo + reading desk show the FGP source distinctly.
- Flag off / DB absent → chooser behaves exactly as today (FGP simply absent).
