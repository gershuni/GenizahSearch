# Architecture Research

**Domain:** v9.0.0 Discovery — same-work identification + connection atlas, folded into the existing GenizahSearch NiceGUI web app (web-only)
**Researched:** 2026-07-19
**Confidence:** HIGH (grounded in the actual research DB schema + the real house sidecar/enrichment/community-write code; MEDIUM only on final distilled row counts, which depend on band-selection decisions still open)

> This is an **integration** architecture, not a greenfield one. The house patterns
> (read-only SQLite sidecar → `shared/*_service.py` → `run.io_bound` off the event loop
> → NiceGUI page/enrichment; Supabase only for community writes via `get_user_client()`)
> are fixed and load-bearing. The discovery module must *conform* to them, not invent
> new plumbing. Every recommendation below points at a real file/function.

---

## Standard Architecture

### System Overview — new components (▓) overlaid on the existing web app

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         WEB UI (NiceGUI @ui.page)                          │
│  ┌─────────┐ ┌─────────┐ ┌──────────┐ ▓▓▓▓▓▓▓▓▓▓ ▓▓▓▓▓▓▓ ▓▓▓▓▓▓▓          │
│  │ /search │ │ /browse │ │  /home   │ ▓/work/{}▓ ▓/atlas▓ ▓/leads▓          │
│  └────┬────┘ └────┬────┘ └────┬─────┘ ▓▓▓▓┬▓▓▓▓▓ ▓▓▓┬▓▓▓ ▓▓▓┬▓▓▓           │
│       │           │           │           │         │       │              │
│       │      ▓ MS connections │      (homepage flagship cards → routes)     │
│       │      ▓ panel (new     │                                            │
│       │      ▓ enrichment     │                                            │
│       │      ▓ section)       │                                            │
├───────┴───────────┴───────────┴───────────┴─────────┴───────┴──────────────┤
│                     SERVICE / ADAPTER LAYER (shared/, web/)                  │
│  ┌───────────────────┐ ┌──────────────────┐ ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓    │
│  │ shared/fjms_      │ │ shared/document_ │ ▓ shared/discovery_service.py ▓  │
│  │ service.py        │ │ service.py (PGP) │ ▓ DiscoveryService (read-only)▓  │
│  │ FjmsService (RO)  │ │                  │ ▓ get_discovery_service()     ▓  │
│  └─────────┬─────────┘ └────────┬─────────┘ ▓▓▓▓▓▓▓▓┬▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓    │
│            │                    │                    │                       │
│  ┌─────────┴────────────────────┴────────────────────┴──────────────────┐   │
│  │  ThreadLocalConnection (shared/thread_local_db.py) — per-thread RO    │   │
│  │  sqlite; every heavy call wrapped in `await run.io_bound(...)`         │   │
│  └────────────────────────────────────────────────────────────────────────┘ │
├──────────────────────────────────────────────────────────────────────────────┤
│                              DATA STORES                                       │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐ ▓▓▓▓▓▓▓▓▓▓▓▓▓▓ ┌────────────┐  │
│  │ Tantivy    │ │ fjms_      │ │ pgp.db     │ ▓ discovery.db▓ │  Supabase  │  │
│  │ (HTR text) │ │ enrich .db │ │ nli_x.db   │ ▓ (NEW sidecar)▓│ (community │  │
│  │            │ │            │ │            │ ▓▓▓▓▓▓▓▓▓▓▓▓▓▓ │  writes)   │  │
│  └────────────┘ └────────────┘ └────────────┘                └─────┬──────┘  │
└─────────────────────────────────────────────────────────────────────┼────────┘
                                                    ▓ work_witness_judgments (NEW table,
                                                    ▓ corrections-style RLS + GRANTs)
        ┌───────────────────────────────────────────────────────────────────┐
        │  OFFLINE (dev box, not shipped): distillation pipeline              │
        │  fullcorpus_v2.db (2.9 GB) ──distill+MASK──▶ discovery.db (~150 MB) │
        └───────────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

| Component | Responsibility | New / Modified | House pattern it mirrors |
|-----------|----------------|----------------|--------------------------|
| **Distillation script** (`scripts/build_discovery_sidecar.py`) | Read `same_work_spike/probe/data/fullcorpus_v2.db`, select tier-A ids + bands + MS-MS edges + work metadata, **apply Maagarim masking**, emit `discovery.db`. Offline only — never shipped, never on the event loop. | NEW | `scripts/*` build scripts; the "scp DB first" deploy posture |
| **`discovery.db`** sidecar | Read-only product data: `work`, `identification`, `connection_edge`, `meta`. No reference text, no Maagarim strings. | NEW | `fjms_enrichment.db`, `pgp.db`, `nli_crossref.db` |
| **`shared/discovery_service.py`** | The ONLY reader of `discovery.db`. `DiscoveryService` class + `get_discovery_service(thread_safe=True)` singleton; graceful degrade when file absent. | NEW | `shared/fjms_service.py::FjmsService` / `get_fjms_service()` verbatim |
| **MS connections panel** | Browse-page enrichment section: "identified as ⟨work⟩" + related manuscripts with band labels + "show uncertified leads" toggle. | MODIFIED (`web/pages/browse_enrichment.py`, `browse_state.py`, `browse.py`) | the existing PGP/FJMS `fetch_*` enrichment coroutines |
| **`/work/{work_id}`** witness page | Per-work witness map: all carrier MSS, filter by band/library. | NEW (`web/pages/work.py`) | `web/pages/catalog_browse.py` |
| **`/atlas`** graph explorer | Corpus connection-graph; server-side neighborhood queries, capped. | NEW (`web/pages/atlas.py`) | `web/pages/browse.py` (heavy async render) |
| **`/leads`** queue page | R-B / R-CANON screening leads, labeled not-certified. | NEW (`web/pages/leads.py`) | `web/pages/discoveries.py` (list + filter) |
| **Homepage promotion** | Atlas + Discovery flagship `ui.card()`s. | MODIFIED (`web/pages/home.py`) | existing module cards (lines ~314-464) |
| **`work_witness_judgments`** Supabase table | Logged-in confirm/reject/annotate on a work-witness claim. | NEW (migration SQL) | `corrections` table + RLS + **GRANTs** |
| **Judgment write/read** | `create_work_judgment()` / read helpers. | NEW (`web/supabase_client.py` + `shared/discovery_judgments_service.py`) | `create_correction()` (`web/supabase_client.py:1358`) + `shared/corrections_service.py` |
| **Public API** (optional) | `GET /api/work/{id}`, `GET /api/connections/{sys_id}`. | NEW (`web/discovery_api.py` or extend `web/search_api.py`) | `init_search_api()` sub-app pattern (`web/main.py:717-736`) |

---

## Recommended Project Structure

New (▓) and modified (~) files, keyed to the real tree:

```
scripts/
▓ build_discovery_sidecar.py     # offline distillation + MASKING (dev box only)
▓ discovery_work_titles.csv       # curated neutral work_id → title map (masking source-of-truth)

discovery_data/                   # NEW sidecar dir (gitignored, scp'd to prod like fist_data/)
▓ discovery.db                    # the distilled product sidecar (~150 MB)

shared/
▓ discovery_service.py            # DiscoveryService + get_discovery_service()  [mirrors fjms_service.py]
▓ discovery_judgments_service.py  # read own/aggregate judgments (client-param) [mirrors corrections_service.py]

web/
~ supabase_client.py              # + create_work_judgment(), get_work_judgments()
~ main.py                         # + @ui.page('/work/{work_id}') / '/atlas' / '/leads'
▓ discovery_api.py                # optional: init_discovery_api() FastAPI routes
web/pages/
~ home.py                         # + Atlas + Discovery flagship cards
~ browse.py                       # + wire the connections enrichment section
~ browse_enrichment.py            # + fetch_discovery() coroutine + render section
~ browse_state.py                 # + discovery result fields on BrowseState
▓ work.py                         # create_work_page(work_id)
▓ atlas.py                        # create_atlas_page()
▓ leads.py                        # create_leads_page()

migrations/
▓ create_work_witness_judgments.sql   # table + RLS + GRANT (2026-05-30 rule)

tests/
▓ test_discovery_service.py       # graceful-degrade, band filtering, off-thread
▓ test_discovery_masking.py       # HARD GATE: no "מאגרים"/Maagarim/reference-text leakage in shipped DB
```

### Structure Rationale

- **`shared/discovery_service.py` (not `web/`):** the sidecar-reader belongs in `shared/` exactly like `fjms_service.py` / `document_service.py`. Even though v9.0.0 is web-only, the house rule is "read-only reference data → `shared/` service." A future desktop parity milestone then costs nothing. No `shared/`→`web/` back-edge (the SEED-016 / GUARD-01-style AST layering guards would catch it).
- **`discovery_data/` as its own sidecar dir:** matches `fist_data/`, `pgp_data/`, `nli_data/`, `fgp_data/`. Keeps the deploy recipe uniform ("scp DBs FIRST, then push code").
- **Distillation lives in `scripts/` and is never imported by the app:** the 2.9 GB research DB and its `pages.text` (which contains reference-corpus-derived data) must never be reachable at runtime — masking is enforced at build time, so runtime code *cannot* leak it.
- **Judgments split write (`web/supabase_client.py`) vs read (`shared/discovery_judgments_service.py`):** exactly the corrections split — writes use `get_user_client()` (web-only, request-scoped auth per Phase 90 D-12), reads take a `client` param so the logic is app-agnostic (see `shared/corrections_service.py::get_pending_corrections_for_page(client, ...)`).

---

## Architectural Patterns

### Pattern 1: Read-only sidecar service (the house pattern — copy `FjmsService`)

**What:** A `DiscoveryService` class opening `discovery.db` read-only through a per-thread connection, plus a module-level singleton factory. Every query guards `if self._conn is None`.
**When to use:** All discovery reads.
**Trade-offs:** Per-thread connections cost a few file handles but make concurrent `run.io_bound()` calls safe; read-only URI (`?mode=ro`) prevents accidental writes and lets SQLite share mmap'd pages.

**Example (mirrors `shared/fjms_service.py:693-760` init + `:3735` factory):**
```python
# shared/discovery_service.py
from shared.thread_local_db import ThreadLocalConnection

class DiscoveryService:
    def __init__(self, db_path: str | None = None, thread_safe: bool = True):
        self._conn = None
        if db_path is None:
            db_path = _resolve("discovery_data", "discovery.db")   # LOCALAPPDATA→root, like fjms
        if not db_path or not Path(db_path).exists():
            logger.warning("DiscoveryService: sidecar not found at %s", db_path)
            return
        uri = f"file:{db_path}?mode=ro"
        self._conn = ThreadLocalConnection(uri, row_factory=sqlite3.Row, timeout=10.0)

    def is_available(self) -> bool:
        return self._conn is not None

    def get_identification(self, sys_id: str) -> dict | None:
        if self._conn is None:
            return None
        row = self._conn.execute(
            "SELECT work_id, title, genre, band, band_precision, matched_letters "
            "FROM identification WHERE sys_id=? "
            "ORDER BY (band='R-A') DESC, matched_letters DESC LIMIT 1", (sys_id,)
        ).fetchone()
        return dict(row) if row else None

_default = None
def get_discovery_service(thread_safe: bool = True) -> "DiscoveryService":
    global _default
    if _default is None:
        _default = DiscoveryService(thread_safe=thread_safe)
    return _default
```

### Pattern 2: Browse enrichment hook (add a `fetch_discovery()` sibling)

**What:** The connections panel is a **new enrichment section**, not a new page. `web/pages/browse_enrichment.py::load_enrichment()` already runs Phase-B enrichment as parallel coroutines (`fetch_pgp()`, `fetch_fjms()` at lines 76-120), each wrapping a sync body in `await run.io_bound(...)`. Add `fetch_discovery()` the same way; stash results on `BrowseState` (`web/pages/browse_state.py`); render in `update_enrichment_sections()`. Wiring point in `web/pages/browse.py:918-925` (`_load_enrichment` / `_update_enrichment_sections` delegates).
**When to use:** The MS connections panel on `/browse`.
**Trade-offs:** Reuses the generation-token staleness guard (`refs.load_generation`) and batched-fetch discipline for free; the cost is touching three coupled files — all already wired to each other.

**Example (mirrors `web/pages/browse_enrichment.py:106-120`):**
```python
async def fetch_discovery():
    _sys = page.sys_id
    def _sync():
        from shared.discovery_service import get_discovery_service
        svc = get_discovery_service(thread_safe=True)
        if not svc.is_available():
            return None
        return {
            'identification': svc.get_identification(_sys),
            'related':        svc.get_related_ms(_sys, high_bands_only=True, limit=25),
        }
    try:
        return await run.io_bound(_sync)   # OFF the event loop — house invariant
    except Exception as e:
        logger.error("discovery fetch failed for %s: %s", _sys, e)
        return None
```

### Pattern 3: Community write via `get_user_client()` (the corrections model)

**What:** Judgment writes go to Supabase through a fresh, request-scoped, RLS-authenticated client (`web/supabase_client.py::get_user_client` :357 — NO caching, Phase 90 D-12 invariant). The write helper returns `{'success': True, ...}` or `{'error': str}` — never raises. Reads take a `client` param and rely on server-side RLS.
**When to use:** Confirm / reject / annotate a work-witness claim.
**Trade-offs:** Fresh client per call costs a little per write but is the only leak-safe path. Anonymous users cannot write (RLS `WITH CHECK auth.uid() = user_id`) — the UI login-gates the verdict buttons like Add-as-Join does.

**Example (mirrors `web/supabase_client.py::create_correction` :1358-1381):**
```python
def create_work_judgment(user_id, sys_id, work_id, verdict, band, note=''):
    try:
        client = get_user_client()
        data = {'user_id': user_id, 'sys_id': sys_id, 'work_id': work_id,
                'verdict': verdict, 'band': band, 'note': note}
        resp = client.table('work_witness_judgments').upsert(
            data, on_conflict='user_id,sys_id,work_id').execute()
        return {'success': True, 'judgment': resp.data[0]} if resp.data else {'error': 'insert failed'}
    except Exception as e:
        return {'error': str(e)}
```

### Pattern 4: Distill-time masking (the hard constraint enforced in the build, not the UI)

**What:** The Maagarim provenance ban is impossible to violate at runtime if the sidecar never contains it. The distillation step (a) replaces every displayed work title with a **neutral canonical title** from a curated `discovery_work_titles.csv` — measured: 3,468 of 4,093 non-shadowed works have `cat='Maagarim'` and 224 `cat='Sefaria'`, so nearly every title needs the de-provenancing pass; (b) drops the `cat`/reference-source columns or rewrites them to neutral genre buckets; (c) **never copies `pages.text` or reference spans** — only character offsets into OUR HTR text, which the display layer already renders from Tantivy/PGP/FGP sources.
**When to use:** Always, in `build_discovery_sidecar.py`; verified by a CI test (`tests/test_discovery_masking.py`) that scans the shipped DB for banned strings (`מאגרים`, "Maagarim", raw `REF2:`-style ids in display columns, Maagarim-convention title patterns).
**Trade-offs:** Requires a one-time human-curated title map (~4k works — bulk-derivable from existing titles, then reviewed), but converts a "remember to hide it on every surface" UI discipline into a single structural gate.

---

## Data Flow

### End-to-end: research DB → distillation → sidecar → service → UI/API

```
[fullcorpus_v2.db 2.9 GB]  (same_work_spike/probe/data/ — dev box, gitignored)
   track1_matches(381,341) · track1_candidates(1.9M) · accepted_pairs_canonmask(1.33M)
   e1_band_frame.jsonl(88,963: band2 R-A/R-B/R-CANON + band_precision) · pages(667,411 — HAS text)
        │
        ▼  scripts/build_discovery_sidecar.py   (OFFLINE, snapshot ship + documented rebuild recipe)
   • SELECT tier-A: track1_matches WHERE shadowed_by IS NULL          → 275,894 page ids / 52,497 MSS / 4,093 works
   • JOIN band labels + precision from the E1 band frame (R-A 0.889 / R-B 0.859 / R-CANON 0.647)
   • aggregate accepted_pairs_canonmask → DISTINCT (sys_a, sys_b)     → 442,696 MS-MS edges (keep flank_class)
   • MASK: work title → neutral (discovery_work_titles.csv); DROP cat/Maagarim/Sefaria provenance; DROP pages.text
        │
        ▼
[discovery.db  ~130-160 MB]  (discovery_data/ — scp'd to prod FIRST, then code — deploy posture)
   work(4,093) · identification(275,894) · connection_edge(442,696) · meta
        │
        ▼  shared/discovery_service.py  (read-only, ThreadLocalConnection, get_discovery_service())
        │        ▲ every heavy query invoked via `await run.io_bound(sync_fn)` — event-loop safety
        ├──────────────────────────┬─────────────────────┬───────────────────────┐
        ▼                          ▼                     ▼                       ▼
[/browse connections panel]  [/work/{work_id}]     [/atlas]                [GET /api/...]
 browse_enrichment.           create_work_page      create_atlas_page       discovery_api
 fetch_discovery() →          (witness list,        (capped neighborhood    (optional, mirrors
 BrowseState → section        band/library filter)  queries, never full     init_search_api)
                                                    graph to browser)
        │
        ▼  user clicks Confirm / Reject / Annotate  (logged-in only)
[web/supabase_client.create_work_judgment()] → get_user_client() → Supabase work_witness_judgments (RLS + GRANTs)
        │
        ▼  reads (own judgment + aggregate counts)
[shared/discovery_judgments_service.get_judgments_for(client, sys_id, work_id)]
        → panel badges today · export feeds future certification rounds (schema anticipates it)
```

### Key Data Flows

1. **Identification (browse):** `sys_id` → `DiscoveryService.get_identification()` + `get_related_ms()` (high bands by default; "show uncertified leads" toggle widens to R-B/R-CANON) → rendered as an inline browse enrichment section, off the event loop.
2. **Witness map (`/work/{id}`):** `work_id` → all carrier MSS + band + library (denormalize `library_code` into `identification` at distill time for zero-join filtering) → filterable table; each row links to `/browse?sys_id=…`.
3. **Atlas neighborhood:** a focus `sys_id`/work → `connection_edge` neighborhood **capped server-side** → compact JSON to the client graph renderer. Never ship all 442k edges.
4. **Judgment loop:** UI verdict → Supabase upsert (one live judgment per user per claim) → aggregate counts read back into the panel → periodic export feeds future E1 certification rounds (out of v9.0.0 scope, schema-ready).

---

## Sidecar schema sketch (`discovery.db`)

```sql
-- Work metadata (MASKED titles; NO cat/Maagarim/Sefaria provenance)
CREATE TABLE work (
    work_id      TEXT PRIMARY KEY,      -- opaque internal id (kept for joins; never displayed raw)
    title        TEXT NOT NULL,         -- NEUTRAL canonical title (from discovery_work_titles.csv)
    title_en     TEXT,                  -- optional EN display title (bilingual UI)
    genre        TEXT,                  -- neutral bucket: Bible / Talmud / Mishnah / Liturgy / JA / ...
    author       TEXT,                  -- if safely displayable (from track1 author col, reviewed)
    n_witnesses  INTEGER,               -- distinct sys_id count (denormalized for /work + /atlas speed)
    n_pages      INTEGER
);

-- Tier-A page-level identifications (275,894 rows = track1_matches WHERE shadowed_by IS NULL)
CREATE TABLE identification (
    page_id          TEXT NOT NULL,     -- {sys_id}_{IE}_{P######}_{FL...}
    sys_id           TEXT NOT NULL,
    work_id          TEXT NOT NULL REFERENCES work(work_id),
    band             TEXT NOT NULL,     -- 'R-A' | 'R-B' | 'R-CANON'  (from e1 band2)
    band_precision   REAL,              -- 0.889 / 0.859 / 0.647 (measured per band)
    certified        INTEGER DEFAULT 0, -- flips to 1 only when R-A independent audit passes
    matched_letters  INTEGER,
    density          REAL,
    n_spans          INTEGER,
    span_offsets     TEXT,              -- compact JSON [[p0,p1],...] into OUR HTR text ONLY
    library_code     TEXT               -- denormalized for zero-join band/library filters
);
CREATE INDEX idx_ident_sys  ON identification(sys_id);
CREATE INDEX idx_ident_work ON identification(work_id);
CREATE INDEX idx_ident_band ON identification(band);

-- MS↔MS connection edges (442,696 rows, aggregated from accepted_pairs_canonmask)
CREATE TABLE connection_edge (
    sys_a        TEXT NOT NULL,         -- canonicalized sys_a < sys_b (dedup)
    sys_b        TEXT NOT NULL,
    n_pages      INTEGER,               -- number of accepted page-pairs behind this edge
    aligned_len  INTEGER,               -- total aligned chars (edge weight for atlas)
    density      REAL,
    flank_class  TEXT,                  -- 'continuation' | 'island' | 'edge' | 'ambig'
    work_id      TEXT                   -- shared work when both ends carry the same id (nullable)
);
CREATE INDEX idx_edge_a ON connection_edge(sys_a);
CREATE INDEX idx_edge_b ON connection_edge(sys_b);

-- Build provenance / kill-switch metadata
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
-- rows: schema_version, source_db_sha256, built_at, ra_audit_status='pending',
--       band_precisions_json, masking_gate='passed'
```

**Size estimate (measured from the actual research DB, not guessed):**

| Table | Rows | Raw payload (measured) | With SQLite overhead + indexes |
|-------|------|------------------------|-------------------------------|
| `identification` | 275,894 | ~41.5 MB | ~70-90 MB |
| `connection_edge` (distinct MS-MS) | 442,696 | ~34 MB | ~55-70 MB |
| `work` | 4,093 | <1 MB | <1 MB |
| **Total** | | | **~130-160 MB** |

Comfortably deployable — an order of magnitude smaller than `fjms_enrichment.db` (1.5 GB), similar league to `libraries_translations.db` (76 MB). **Do NOT ship** the full per-page-pair edge table (1.33M rows → +~100 MB with little UI value at MS granularity) or `pages.text` (hundreds of MB and a masking hazard). If per-page pair detail is ever needed for an atlas drill-down, add it later as an optional table — not in the v9.0.0 default.

**Leads-band note:** the R-B / R-CANON screening rows for `/leads` come from the same E1 band frame (7,498 R-B rows measured in `e1_rb_screening.jsonl`; R-CANON similar order). They fit inside `identification` with `band` values — no separate table needed; `/leads` is just `WHERE band IN ('R-B','R-CANON')`.

---

## Supabase judgment-table sketch (`work_witness_judgments`)

Follows the `corrections` model (`supabase_setup.sql:90-107` + policies :309-316) **plus** the mandatory GRANTs — `supabase_setup.sql` predates the 2026-05-30 Supabase Data API rule and contains **zero** GRANT statements, so this table must add them explicitly (CLAUDE.md convention #6).

```sql
CREATE TABLE public.work_witness_judgments (
    id           SERIAL PRIMARY KEY,
    user_id      UUID REFERENCES auth.users(id) ON DELETE SET NULL,
    sys_id       TEXT NOT NULL,            -- the witness manuscript
    work_id      TEXT NOT NULL,            -- the claimed work (opaque id from discovery.db)
    band         TEXT,                     -- band shown at judgment time (audit trail vs future rebuilds)
    verdict      TEXT NOT NULL CHECK (verdict IN ('confirm', 'reject', 'uncertain')),
    note         TEXT,
    sidecar_version TEXT,                  -- meta.schema_version at judgment time (rebuild provenance)
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    updated_at   TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (user_id, sys_id, work_id)       -- one live judgment per user per claim (upsert target)
);

CREATE INDEX idx_wwj_sys  ON work_witness_judgments(sys_id);
CREATE INDEX idx_wwj_work ON work_witness_judgments(work_id);
CREATE INDEX idx_wwj_user ON work_witness_judgments(user_id);

ALTER TABLE work_witness_judgments ENABLE ROW LEVEL SECURITY;

-- Aggregate counts + rows publicly readable (like fragment_joins); writes only by author.
CREATE POLICY "Anyone can view judgments"      ON work_witness_judgments FOR SELECT USING (true);
CREATE POLICY "Users can create own judgments" ON work_witness_judgments FOR INSERT WITH CHECK (auth.uid() = user_id);
CREATE POLICY "Users can update own judgments" ON work_witness_judgments FOR UPDATE USING (auth.uid() = user_id);
CREATE POLICY "Users can delete own judgments" ON work_witness_judgments FOR DELETE USING (auth.uid() = user_id);

-- MANDATORY (2026-05-30 Data API rule) — RLS alone is NOT enough for PostgREST/supabase-js:
GRANT SELECT                          ON public.work_witness_judgments TO anon;
GRANT SELECT, INSERT, UPDATE, DELETE  ON public.work_witness_judgments TO authenticated;
GRANT USAGE, SELECT ON SEQUENCE public.work_witness_judgments_id_seq TO authenticated;

CREATE TRIGGER update_wwj_updated_at BEFORE UPDATE ON work_witness_judgments
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();   -- reuse existing fn (supabase_setup.sql:397)
```

---

## Suggested Build Order (dependency-ordered)

De-risk the data spine first (a wrong sidecar shape invalidates every surface), then the lowest-risk UI (reuses existing enrichment machinery), then progressively heavier surfaces.

1. **Sidecar schema + distillation script + masking gate** *(spine — everything depends on it)*
   `scripts/build_discovery_sidecar.py` + `discovery_work_titles.csv` + `tests/test_discovery_masking.py`. Ship a snapshot `discovery.db`; document the rebuild recipe in the script header. **Gate:** masking audit finds zero Maagarim/reference-text strings in the shipped file.
2. **`shared/discovery_service.py`** *(depends on 1)* — copy `FjmsService`: singleton factory, thread-local RO conn, `is_available()`, graceful degrade. Unit tests incl. absent-file no-op + band filtering.
3. **MS connections panel on `/browse`** *(depends on 2; lowest UI risk, highest reach)* — `fetch_discovery()` in `browse_enrichment.py`, fields on `BrowseState`, render section + "show uncertified leads" toggle. First user-visible surface; reuses the proven enrichment path. Requires a live render-smoke test (NiceGUI headless-pytest gap — house lesson).
4. **`/work/{work_id}` witness page** *(depends on 2)* — new `@ui.page` route in `web/main.py` + `web/pages/work.py`; band/library filters; rows link to `/browse`.
5. **`/atlas` graph explorer** *(depends on 2; heaviest, riskiest)* — server-side capped neighborhood queries; new route + `web/pages/atlas.py` + a client-side graph renderer. Do the payload-cap and event-loop work carefully here; pre-aggregate/exclude the giant liturgical hub.
6. **Supabase judgments** *(independent of 4-5; parallelizable after 3)* — migration (table + RLS + GRANTs) → `create_work_judgment()` / read service → wire verdict controls into the panel (3) and work page (4). Login-gated writes.
7. **`/leads` queue page** *(depends on 2)* — R-B/R-CANON list from `identification WHERE band IN (...)`, explicitly labeled not-certified; `web/pages/leads.py`, mirrors `discoveries.py` list UX.
8. **Homepage promotion** *(depends on 4 + 5 existing as routes)* — Atlas + Discovery flagship cards in `home.py` (~lines 314-464 card pattern).
9. **Public API endpoints** *(optional; depends on 2)* — `init_discovery_api()` following `init_search_api(app_override, path_prefix)`; `GET /api/work/{id}`, `GET /api/connections/{sys_id}`; document in `docs/SEARCH_API.md`.

Note the **tier-A stratified precision certificate** (milestone target) is a measurement activity on the research side; architecturally it only touches the sidecar via `band_precision`/`certified` values and the `meta.ra_audit_status` row — build-order slot: alongside 1, before launch copy is finalized ("expert-verified" vs "certified" labels).

---

## Scaling Considerations

| Scale | Architecture Adjustments |
|-------|--------------------------|
| Sidecar reads (all traffic) | None — `discovery.db` ~150 MB, mmap'd, sub-ms indexed lookups on `sys_id`/`work_id`. Same regime as existing sidecars. Adds ~150 MB to the web box's file-cache footprint — negligible next to the 5-6 GB Tantivy baseline, but note the 15.4 GB box runs near its MemoryHigh cap. |
| Atlas graph (52,497 nodes / 442,696 edges) | **Never render the full graph.** Server-side neighborhood queries with a hard node/edge budget (top-N by `aligned_len` around a focus), mirroring the search render cap `[:200]` + WebSocket-safety house rule. The SEED-029 rehearsal's "giant liturgical component" (~16k MSS) must be excluded/aggregated behind a filter or it dominates every view. |
| Concurrent web requests | Already handled by `ThreadLocalConnection` + `run.io_bound`; no per-request state on the singleton service. |
| Judgment volume | Trivial for Supabase; the UNIQUE upsert keeps the table at O(users × judged claims). |

### Scaling Priorities

1. **First bottleneck — atlas payload size.** A naive "return all edges" kills the browser tab and the NiceGUI WebSocket. Fix: capped neighborhood queries computed off-thread; paginate expansion.
2. **Second bottleneck — the connections panel on hub manuscripts.** A Bible/liturgy fragment can connect to thousands of MSS. Fix: `LIMIT` + high-band-first + `aligned_len DESC` ordering in `get_related_ms()`, "show more" pagination, default `high_bands_only=True`.

---

## Anti-Patterns

### Anti-Pattern 1: Displaying (or shipping) Maagarim-derived text/provenance
**What people do:** Copy `pages.text`/reference spans into the sidecar "so the UI can show the matched passage," or carry the `cat`/`work_id` provenance strings into display columns.
**Why it's wrong:** Violates the v9.0.0 hard masking constraint — 3,468/4,093 works trace to Maagarim (plus 224 Sefaria); provenance and reference text must never surface. Also an HTR-license consideration (model is CC-BY-NC-SA).
**Do this instead:** Ship only offsets into OUR HTR text; render passages from the Tantivy/PGP/FGP surfaces the browse page already uses; neutralize every title at distill time; CI-scan the shipped DB.

### Anti-Pattern 2: Querying the sidecar on the event loop
**What people do:** Call `DiscoveryService` methods directly inside a NiceGUI handler or page builder.
**Why it's wrong:** SQLite scans block the single asyncio event loop → whole-app stalls for every connected user (the exact failure mode `run.io_bound` exists to prevent; see `fetch_pgp`/`fetch_fjms` in `browse_enrichment.py`).
**Do this instead:** Wrap every read in `await run.io_bound(sync_fn)`; capture `sys_id`/args into the closure BEFORE the thread hop (as the existing enrichment code does with `_page_sys_id`).

### Anti-Pattern 3: A standalone connections page instead of an enrichment section
**What people do:** Build a separate page and link out from browse.
**Why it's wrong:** Fragments the reading flow and re-implements the generation-token/staleness/batched-fetch machinery `browse_enrichment.py` already provides.
**Do this instead:** Add `fetch_discovery()` alongside `fetch_pgp`/`fetch_fjms`; render inline via `update_enrichment_sections()`; deep-link to `/work/{id}` and `/atlas` FROM the section.

### Anti-Pattern 4: Creating the Supabase table without GRANTs
**What people do:** Copy the `corrections` DDL (RLS + policies only) from `supabase_setup.sql`.
**Why it's wrong:** That file predates the 2026-05-30 rule; PostgREST/supabase-js needs explicit `GRANT`s in addition to RLS, or every request 401/403s on the new table.
**Do this instead:** Add `GRANT ... TO authenticated` (+ the sequence grant) and `GRANT SELECT TO anon` as in the sketch above.

### Anti-Pattern 5: Caching auth clients or bypassing `safe_storage` for panel state
**What people do:** Reuse a stored `get_user_client()` handle across requests, or stash the "show uncertified leads" toggle in raw `app.storage.user`.
**Why it's wrong:** Both break the Phase 87/90 multitenant invariants (cross-user leak); CI-guarded by `tests/test_no_raw_storage_access.py` (allowlist `[]`).
**Do this instead:** `get_user_client()` fresh per write (like `create_correction`); persist any per-user UI toggle via the `web/safe_storage.py` chokepoint.

### Anti-Pattern 6: Overstating certification in UI copy
**What people do:** Label R-A rows "certified" while the independent audit is still pending.
**Why it's wrong:** The E1 registry (`same_work_spike/probe/data/e1_certification_registry.json`) records `independent_audit: pending` for all bands; the milestone explicitly requires "expert-verified" phrasing until it passes, plus in-UI recall honesty ("no identification shown ≠ none exists").
**Do this instead:** Drive labels from `meta.ra_audit_status` + `identification.certified` so a sidecar rebuild flips the copy without a code change.

---

## Integration Points

### External Services

| Service | Integration Pattern | Notes |
|---------|---------------------|-------|
| Supabase | Community writes only, via `get_user_client()` (request-scoped, RLS). Reads via client-param service functions. | New `work_witness_judgments` table needs explicit GRANTs (2026-05-30 rule). |
| Distillation source | `scripts/build_discovery_sidecar.py` reads `same_work_spike/probe/data/fullcorpus_v2.db` on the dev box; never in prod. | Output `discovery.db` scp'd to prod FIRST, then code (deploy posture, memory `feedback_deploy_db_sync`). |

### Internal Boundaries

| Boundary | Communication | Notes |
|----------|---------------|-------|
| `web/pages/*` ↔ `shared/discovery_service.py` | Direct import + `run.io_bound` | Service in `shared/`; no `shared/`→`web/` back-edge (AST layering guards). |
| `browse.py` ↔ `browse_enrichment.py` ↔ `browse_state.py` | Explicit `state`+`refs` params (no closures — Phase 73 design) | Add discovery fields to `BrowseState`; wire at `browse.py:918-925`. |
| Judgment write (`web/supabase_client.py`) ↔ read (`shared/discovery_judgments_service.py`) | Split like corrections | Write uses `get_user_client()`; read helper takes a `client` param. |
| `discovery_api` ↔ NiceGUI FastAPI app | `init_*_api(app_override, path_prefix)` mounted at `/api` | Follow `init_search_api()` + the `/api` sub-app mount (`web/main.py:717-736`); rate-limit + error envelope via existing `web/api_hardening.py` machinery. |
| Homepage ↔ new routes | Plain `ui.card()` nav cards | `home.py` flagship-card pattern (~:314-464). |

## Sources

- `shared/fjms_service.py` (`FjmsService.__init__` :693-760, `get_fjms_service` :3735) — sidecar-service house pattern — HIGH (read directly)
- `shared/thread_local_db.py` (`ThreadLocalConnection`) — per-thread RO connection — HIGH
- `web/pages/browse_enrichment.py` (`load_enrichment`, `fetch_pgp`/`fetch_fjms` :76-120, `run.io_bound` usage, `BrowsePageRefs`) — enrichment hook — HIGH
- `web/pages/browse.py:87-90, 918-925` — enrichment wiring points — HIGH
- `shared/corrections_service.py` (client-param read pattern) + `web/supabase_client.py::create_correction` :1358 / `get_user_client` :357 (request-scoped auth, no caching) — community-write pattern — HIGH
- `supabase_setup.sql` (corrections/discoveries tables + RLS :90-146, :309-334; confirmed NO GRANT statements → 2026-05-30 rule applies to new tables) — HIGH
- `web/main.py` (`@ui.page` routes :1473-2082; `init_api_routes` :698; `init_search_api` sub-app mount :717-736) — page/API registration — HIGH
- `web/pages/home.py` (flagship `ui.card()`s :314-464) — homepage promotion pattern — HIGH
- `same_work_spike/probe/data/fullcorpus_v2.db` — schema + counts measured directly via sqlite3: tier-A 275,894 (shadowed_by IS NULL) / 4,093 works / 52,497 MSS; accepted_pairs 1,332,099 page-pairs → 442,696 distinct MS-MS; works-per-cat (Maagarim 3,468, Sefaria 224); flank_class distribution; byte-size estimates — HIGH
- `same_work_spike/probe/data/e1_band_frame.jsonl` / `e1_ra_confirmed.jsonl` (band2 R-A, band_precision 0.889) / `e1_rb_screening.jsonl` (7,498 rows) / `e1_certification_registry.json` (independent_audit: pending) — band structure — HIGH
- `.planning/PROJECT.md` "Current Milestone: v9.0.0" + `.planning/seeds/SEED-029-fragment-textual-similarity-same-work-detection.md` — milestone scope + research provenance — HIGH

---
*Architecture research for: v9.0.0 Discovery module integration into the GenizahSearch web app*
*Researched: 2026-07-19*
