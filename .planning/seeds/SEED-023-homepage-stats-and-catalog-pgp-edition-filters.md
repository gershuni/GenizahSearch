---
id: SEED-023
status: built
built: 2026-06-24 — Part A (homepage stats) merged via PR #306/#307; Part B (catalog PGP/Editions filters) on branch audit/seed-023b-catalog-filters. Built per the Codex corrections below.
planted: 2026-06-24
planted_during: User feature request (Hillel, 2026-06-24) during SEED-022 work — homepage corpus stats band + PGP/Editions filters on the catalog ("Browse by identification") page.
trigger_when: After SEED-022 desktop side merges (this reuses the SEED-022 PGP-text/FGP helpers and adds a sibling editions helper). Codex-review this seed before coding (project seed-review gate).
scope: medium (one cached stats service + homepage band; two 3-state filters on catalog_browse with service-level intersection)
---

# SEED-023: Homepage corpus stats + catalog PGP/Editions filters

> User intent (Hillel, 2026-06-24): (1) a homepage stats band advertising the scale of the corpus,
> and (2) PGP + Editions filters on the catalog "Browse by identification" page.

Two related features (share the PGP/editions presence helpers). Build together.

## Locked decisions (Hillel, 2026-06-24)
- **"Manuscripts" stat = ALL catalog records = 255,726** (`libraries.csv` row count), not images-gated.
- **"Images" stat = ALL providers summed ≈ 1,019,886** (NLI 814,954 + Cambridge 141,368 + Manchester
  27,940 + JTS 35,624), not NLI-only.
- One seed, Codex-reviewed, built after SEED-022 desktop lands.

---

## Part A — Homepage stats band

Five metrics, rendered as a responsive, bilingual (EN/HE, RTL-aware) band on the homepage
(`web/pages/home.py`).

| Metric | Source | Count (2026-06-24) |
|---|---|---|
| Manuscripts | `libraries.csv` row count | 255,726 |
| Catalog entries | `fjms_enrichment.db` `catalog` rows | 731,354 |
| Images | `nli_data/nli_crossref.db`: `nli_images` + `cambridge_manifests` + `manchester_luna` + `jts_dpul` | ~1,019,886 |
| Automatic transcriptions (MiDRASH) | distinct manuscripts in the GENIZAH transcription corpus | **compute once** (see below) |
| Scholarly editions | distinct manuscripts with a PGP edition ∪ FGP Digital Edition | ~7,114 + FGP (see Part B helper) |

### Design
- **NEW `web/stats_service.py`** — a memoized stats provider. These are huge tables; **compute each
  count ONCE and cache** (module-level cache, computed on first access / at startup), NEVER per
  request. Expose `get_corpus_stats() -> dict` returning the 5 ints (+ a cached timestamp). Re-reads
  on process restart (so a data refresh + redeploy updates the numbers); no live recompute per page.
- **"Automatic transcriptions" count** is the one number with no ready source. Options (decide in
  review): (a) count `DISTINCT sys_id` in the GENIZAH Tantivy index at startup (authoritative, one
  scan, cached); (b) precompute offline and store in an index `.meta.json` / a tiny stat file read at
  startup. PREFER (a) if the index exposes a distinct-sys_id count cheaply; else (b). Do NOT scan the
  1.47 GB `Transcriptions.txt` at runtime.
- **Render** in `home.py` as a stat-card row (icon + big number + bilingual label), responsive grid,
  `max-width:100%`, RTL mirrors order. Numbers formatted with thousands separators per locale. Add the
  5 labels as EN+HE keys in `genizah_translations.py`.
- **No new network/DB on the request path** — `get_corpus_stats()` returns cached ints synchronously.

---

## Part B — Catalog "Browse by identification" PGP + Editions filters

`web/pages/catalog_browse.py` browses the FJMS `catalog` by domain/author/work with removable chips.
Add two **3-state** filters mirroring the search-page `pgp_filter` pattern:
- **PGP:** `all` / `has_pgp` / `no_pgp` — keyed on the PGP link-presence set
  (`get_sys_ids_with_transcriptions`, the same set the PGP badge uses).
- **Editions:** `all` / `has_edition` / `no_edition` — keyed on SCHOLARLY editions only.

### NEW helper (sibling to SEED-022)
`document_service.get_sys_ids_with_editions(sys_ids)` = `document_fragments JOIN document_sources`
where `doc_relation LIKE '%Edition%'` (PGP scholarly editions) **∪** FGP `doc_relation='Digital Edition'`.
NOTE this is EDITIONS-ONLY — distinct from SEED-022's `get_sys_ids_with_manual_transcriptions` (which
also includes translations). Put the FGP edition-only count behind a small fgp_service helper if one
doesn't already filter by `doc_relation`. (~7,114 PGP docs have an edition; FGP adds Digital Editions.)

### Design crux — PAGINATION (the real decision)
`catalog_browse` is **server-side paginated** (`data.get('results')` + `data.get('total')`). It ALREADY
fetches `sys_id` per result row (`catalog_browse.py:216`) and ALREADY post-computes `printed_ids` for
the visible page (`:223`). But a *filter* (unlike the printed badge) must apply to the **FULL** result
set so pagination + the total count stay correct — post-filtering only the visible page would show
< page_size rows and a wrong total.

**Required approach:** push the PGP/editions filter DOWN into the catalog browse query/service so the
intersection happens before pagination and `total` reflects the filtered count. The candidate sets are
sys_id sets (`get_sys_ids_with_transcriptions` / `get_sys_ids_with_editions`); the catalog key is
`AlmaId == sys_id` (226,555 distinct AlmaIds; samples `9900…205171`). The browse service already maps
catalog→sys_id, so intersect there. Do NOT post-filter the rendered page.

- **Persist** both filter states via the `web/safe_storage.py` chokepoint (Phase 87 invariant), like
  `pgp_filter`. Removable chips consistent with the existing domain/author/work chips.
- **3-state button** UI cloned from the search `pgp_filter` button (label cycles all → has → no).

## Reuse / invariants
- Reuses SEED-022's PGP-text/FGP presence plumbing; the **PGP badge + `pgp_filter` on /search stay
  untouched**. Part B's PGP filter uses the SAME link-presence set as the existing PGP badge (NOT the
  SEED-022 text set) — "has PGP info", per the user's wording.
- Phase 87 `safe_storage` chokepoint for all per-user filter state (CI guard: allowlist `[]`).

## Tests required
- `stats_service`: counts memoized (computed once; second call hits cache, no re-query); each metric
  returns a positive int from its source; graceful 0 when a sidecar is absent.
- Homepage: stats band renders 5 cards; EN+HE labels (no English leak under Hebrew); thousands-separator.
- `get_sys_ids_with_editions`: PGP-edition ∪ FGP-Digital-Edition; FGP-absent degrades to PGP set;
  EDITIONS-only (a translation-only mss is NOT in the set); List[str] signature + None/empty safe.
- Catalog filter: `has_pgp` / `no_pgp` and `has_edition` / `no_edition` change `total` correctly (full
  set, not page subset); `all` is a no-op; state persists via safe_storage.

## Done when
Homepage shows a cached 5-metric stats band (bilingual, RTL-aware, no per-request big-table COUNT);
catalog page has working 3-state PGP + Editions filters that paginate + count correctly over the full
filtered set; new editions helper + tests green; ruff clean; PGP badge/`pgp_filter`/SEED-022 untouched;
Codex-reviewed before code.

## NOT in scope
Stats on any other page; live/auto-refreshing counters; editions filter on /search (separate);
touching the PGP badge or SEED-022; an API stats endpoint (could be a later add).

---

## Codex review corrections (2026-06-24) — BUILD PER THESE (override the above where they differ)
Codex verdict: **GO-WITH-CHANGES**. Viable; tighten the metric definitions and put the catalog filter
INSIDE the FJMS browse query (not the NiceGUI page layer).

### BLOCKERS
- **B1 — metric DEFINITIONS + corrected numbers** (count distinct *manuscripts*, not raw docs):
  | Metric | Correct definition | Count (2026-06-24) |
  |---|---|---|
  | Manuscripts | `libraries.csv` LOADABLE rows (exclude header + the two `#` marker rows) — use the CSV loader's own count, not `wc -l` | **255,723** (not 255,726) |
  | Catalog entries | fjms `catalog` rows | 731,354 |
  | Images | raw provider-row SUM — **label "image/manifest records"** (heterogeneous, not distinct images) | 1,019,886 |
  | Automatic transcriptions | DISTINCT sys_ids in the deduped GENIZAH `browse_map` | **232,450** |
  | Scholarly editions | DISTINCT **manuscripts/sys_ids** with PGP `%Edition%` ∪ FGP `Digital Edition` | **27,424** (27,264 in FJMS catalog) — FGP dominates (24,668) |
- **B2 — automatic-transcriptions count is NOT cheap at runtime.** Tantivy schema has no `sys_id` field;
  `searcher.num_docs` counts page/system/part docs, not manuscripts. → **Write a tiny `corpus_stats.json`
  at INDEX BUILD time** (after `browse_map` is deduped) and have `stats_service` read it at startup.
  Do NOT compute this live. (Web index is rebuilt on deploy, so the file refreshes then.)
- **B3 — catalog filter lives in the FJMS query, not the page.** Thread the two 3-state filters into
  `shared/fjms_service.py::get_browse_results` (it ALREADY has a total/limit/offset path,
  ~`fjms_service.py:1984`); intersect on `c.AlmaId == sys_id` and apply the conditions BEFORE
  `COUNT(DISTINCT c.AlmaId)` (~`:2146`) and before `LIMIT/OFFSET`. Page-level post-filtering corrupts
  totals + pagination — `catalog_browse.py:216` only post-computes visible-page flags today.

### SHOULD-FIX
- Don't pass a 30k-element `IN (...)` list — materialize the PGP/edition sys_id set into a
  per-connection **temp table** keyed by `AlmaId` and `JOIN`/`EXISTS` (~`fjms_service.py:2156`).
- **PGP filter = LINK presence** (`get_sys_ids_with_transcriptions`, document_fragments) — NOT the
  SEED-022 text helper `get_sys_ids_with_pgp_text`. ("has PGP info", per user.)
- **Editions = NEW helpers, editions-only:** `document_service.get_sys_ids_with_editions` (PGP
  `%Edition%`) + `fgp_service.get_sys_ids_with_fgp_editions` (`doc_relation = 'Digital Edition'` ONLY —
  do NOT reuse `get_sys_ids_with_fgp_sources`, which includes translations). Keep editions-only DISTINCT
  from SEED-022's translation-inclusive union.
- **stats_service paths:** read the REAL sidecar dirs via existing service constants (`fist_data/`,
  `nli_data/`, `pgp_data/`, `fgp_data/`) — the root `nli_crossref.db` is an empty placeholder. Make the
  cache **lazy + lock-protected**, optional background warm, and stamp `computed_at` (+ optional sidecar
  mtimes) for diagnosability. Never query these DBs per request.
- **safe_storage:** `catalog_browse` currently persists filter state via browser `sessionStorage`
  (`:108`) — route the new filters (preferably ALL catalog filters) through `web/safe_storage.py`
  (Phase 87 chokepoint, CI allowlist `[]`).

### NICE-TO-HAVE
- i18n EN+HE keys for the 5 stat labels + 2 filter buttons (no English leak under Hebrew).
- Reserve stable height for the stats band (CLS); home.py already guards async height.
- Keep the homepage SEO/JSON-LD rounded corpus prose (`web/main.py:1578`) consistent with the exact
  visible stats (update both together).
- Catalog PGP enum: search uses `only_pgp`/`hide_pgp`; either use a catalog-local `has_pgp`/`no_pgp`
  enum intentionally or add an explicit mapping (don't silently diverge).
