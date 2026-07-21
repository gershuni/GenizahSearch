# Phase 134: Discovery Data Spine - Research

**Researched:** 2026-07-21
**Domain:** Offline data distillation → masked versioned SQLite sidecar + async read-only service (event-loop safety, provenance masking, fail-open). No UI, no LLM/model calls.
**Confidence:** HIGH (all findings verified against the actual probe DB, the shipped Phase 133 code, and the repo service layer; only forward-looking budget numbers are ASSUMED and flagged)

<user_constraints>
## User Constraints (from 134-CONTEXT.md)

### Locked Decisions
- **D-01:** Launch distillation matches on **three** reference sources: Sefaria + JA + M-source. R-source **deferred** (not ingested for launch).
- **D-02:** R-source is a newly-acquired 4th corpus (off-repo/gitignored; ~6 GB, ~1,679 Hebrew files). Masked **exactly like M-source** — codename "R-source" only, never real name/path/provenance. Parallel-track ingest; MUST NOT block 134–139.
- **D-03:** Sidecar built **source-extensible** so gen-2 (adding R-source + deferred M-source genres) is a **versioned REBUILD, not a migration**: (a) masked `source_corpus` field on works/claims — internal only, never displayed; (b) cross-corpus canonical `work_id` (opaque, work-level dedup); (c) R-source tokens pre-registered in the DATA-05 pattern set now; (d) frozen-frame + certificate scoped to "the reference sources present at THIS distillation" — gen-2 = a new versioned frame, never a retrofit.
- **D-04:** R-source handling policy is for the PARALLEL track, NOT the 134 build (composition-date-aware anachronism policy + cross-corpus work-dedup + span-shadowing). Captured only so the planner understands why R-source is out of the launch spine.
- **D-05:** Launch DISPLAYED work-set = **Sefaria + JA (all works)** + **M-source large literary works that resemble the open corpora**. EXCLUDE M-source piyyut + documentary at launch.
- **D-06:** "Large literary works" is a **curation policy, not a mechanical filter.** Researcher proposes candidate set (primary signal = exclude-by-genre: drop piyyut + documentary); **owner is final gate** via the D-08 review artifact.
- **D-07:** Every shown work carries a **human-reviewed neutral title** (+ reviewed author/genre). **NO fallback to research titles** — **fail-closed**: unreviewed = EXCLUDED.
- **D-08:** Curation via a **generated review artifact**: opaque `work_id` + candidate neutral title + author + genre, **source MASKED**. Owner approves; only approved rows distill. **Auto-adopt open-corpus (Sefaria/JA) canonical titles** with a light spot-check; concentrate FULL owner review on the M-source literary subset. Model on `scripts/export_translation_audit_sample.py` + `web/components/translation_report.py`.
- **D-09:** **[SUPERSEDED — see 134-CONTEXT.md CONTRACT CORRECTION C-4/C-5/F8: bands are per-`evidence_source`, a claim carries MULTIPLE evidence rows/bands, and "one band per claim key" is DROPPED.]** **All four bands** populate the sidecar — `expert_verified` (R-A) > `tier_a` > `screening_rb` (R-B) > `screening_canon` (R-CANON); exactly one band per claim key post-precedence.
- **D-10:** Canon lane (`screening_canon`) ships but is separately caveated (Targum-confusion class) per LEADS-01.
- **D-11:** Trim to fit **≤300 MB** (DATA-08). Planner sets per-band inclusion caps against `discovery-frames.md` + `discovery-budgets.md`; frozen-frame records per-band deduped counts BEFORE any certificate cards.
- **D-12:** `claim_type` semantic set is **FROZEN** (direct witness / quotes-this-work / textual parallel / direct text overlap), stored as a stable code. Bilingual EN/HE wording DEFERRED to 135/136.

### Claude's Discretion
- Exact per-query timeouts, bounded-concurrency limits, LRU sizing, pagination page sizes (DATA-06) — planner sets against PERF-01 caps.
- User-facing overload copy ("temporarily unavailable") + fail-open / sidecar-absent messaging.
- `discovery.db` internal table/index layout, the deterministic `claim_id`/`unit_id` hashing implementation (algorithm frozen in DATA-01/DATA-10; implementation is the planner's), the schema-versioned filename scheme.
- Whether the DATA-05 guard extension **reuses `scripts/check_atlas_masking.py` wholesale or factors a shared scanner core.**

### Deferred Ideas (OUT OF SCOPE)
- R-source ingest → parallel research track → gen-2 sidecar refresh (FUT-04).
- M-source piyyut + documentary works → fast-follow / gen-2.
- Relation-vocabulary bilingual EN/HE wording → Phases 135/136.
- All downstream surfaces (135–139): band contract, methods page, tier-A cert, panel, work pages, community judgments/Supabase, leads queue, atlas drill-down, homepage band.
- FUT-01..08.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| DATA-01 | Canonical claim model: two families (work–witness + MS–MS relation w/ child alignment rows queryable by page), deterministic SHA-256 `claim_id`, frozen `claim_type` vocab, one band per key | §"Claim Model" — verified `accepted_pairs_canonmask` (MS-MS page pairs, `flank_class`→claim_type) + `track1_matches`/`work_query_hits_fullv2` (work–witness). DDL sketch + hashing recipe provided. |
| DATA-02 | Frozen band model (`expert_verified`>`tier_a`>`screening_rb`>`screening_canon`), exactly-one-band per key, opaque work IDs, frozen-frame artifact `discovery-frames.md` | §"Bands & the Frame" — band sources located (E1 track / R-A 0.889 / R-B 0.859 / R-CANON 0.647); opaque-work-id re-mint requirement (raw `work_id` embeds `M:`/`J:` source prefixes AND M-source filenames — a hard leak). |
| DATA-03 | Structural masking: no reference text/sigla/provenance columns; evidence = offsets into OUR HTR text + snapshot hash; fail-closed on drift | §"Masking" + §"Evidence spans" — verified `pages.text` is OUR text (`provenance∈{htr,fgp,pgp}`); `a0/a1/b0/b1` are char offsets into it. Store offsets + text-layer + snapshot hash; never copy text. |
| DATA-04 | Curated shown-work subset; every shown work has a human-reviewed neutral title (no research-title fallback); all displayable fields reviewed | §"Neutral-title review artifact" + §"Genre signal" — genre taxonomy per work located; open-corpus auto-adopt vs M-source manual review split. |
| DATA-05 | Permanent CI leak-vector guard over sidecar (schema + every cell) + every product surface + committed repo; M-source codename only; R-source tokens pre-registered | §"Masking guard extension" — `scripts/check_atlas_masking.py` analyzed; recommend a `--scan-sqlite` mode iterating schema + all cells; extend pattern set. |
| DATA-06 | One async DiscoveryService chokepoint: per-query timeouts, bounded concurrency, indexed bounded queries, LRU on browse-enrichment, server-side pagination, overload→"temporarily unavailable" never hang | §"Service pattern" — `web/search_api.py` off-loop precedent (`run_in_executor`+`asyncio.wait`+semaphore→503) + `shared/fjms_service.py` sidecar pattern. Method signatures + defaults proposed. |
| DATA-07 | Feature flag gates all discovery surfaces; off / sidecar absent → hide cleanly, zero errors | §"Flag + fail-open" — `web/feature_flags.py::_env_enabled` + Phase 133 `atlas_preview_available()` (flag AND readiness). New `DISCOVERY_ENABLED` + `discovery_available()`. |
| DATA-08 | Release contract (schema version, source-DB hash, build date, data-as-of, expected row counts, `PRAGMA integrity_check`, ≤300 MB), schema-versioned filename, reject incompatible at startup, temp-upload→verify→atomic-rename→code deploy, rollback + rebuild recipe | §"Versioned loader + release contract" — `web/atlas_assets.py` fail-closed startup-load model + versioning §9 of atlas schema spec. |
| DATA-10 | Codicological witness unit = union of sys_ids via (a) catalogued parts (Oxford part id) + (b) physical join groups (PGP/FJMS/user; NOT "same scribe"); deterministic `unit_id` = hash over sorted members; unit×work displays highest band; same-unit members suppressed from "other MSS" | §"Witness units" — `libraries.csv` col1 oxford_part_id + `fjms_enrichment.db` `joins` table (`JoinType` taxonomy: exclude `'Scribe join'`). |
| PERF-01 | `discovery-budgets.md` exit artifact w/ numeric caps (browse-enrichment p95≤150ms/timeout 2s; work/leads ≤200 rows/≤500KB/p95≤1.5s/timeout 5s; atlas caps; discovery ≤250MB added RSS) | §"Budgets & exit artifacts" — caps sourced from PERF-01; measurement strategy in Validation Architecture. |
</phase_requirements>

## Summary

Phase 134 is a **data-engineering + service-plumbing** phase with two genuinely hard parts, both about *safety*, not features: (1) a **deterministic, masked, offline distillation** that turns a 3.1 GB research DB (`fullcorpus_v2.db`) into a ≤300 MB shipped `discovery.db` carrying two claim families, four bands, witness-units and human-reviewed neutral titles, with **zero reference text / sigla / provenance**; and (2) **one async `DiscoveryService`** that reads that sidecar off the event loop, with timeouts, bounded concurrency, LRU and pagination, and that (with the service and the whole app) fails open when the flag is off or the sidecar is absent/corrupt/incompatible. The runtime stack is entirely already-present (stdlib `sqlite3` + `hashlib`; the exact off-loop + fail-closed-loader + feature-flag + masking-scan patterns all shipped in Phase 133 and the existing 13 `shared/*_service.py` sidecars). Nothing new is installed on the web box.

The **single highest risk is a provenance leak**, and I found concrete leak vectors that MUST drive the plan: the research `work_id` is **not opaque** — it is source-prefixed (`M:` = M-source, `J:` = JA, `REF` = canon) and for M-source it embeds the actual M-source **filename/title** (`M:` + filename stem). The reference-source `cat` column contains the literal M-source name as a value. The M-source work **catalogue** (author/title/date/genre) that feeds neutral titles lives in an off-repo XLSX. Therefore the distillation MUST (a) mint fresh opaque product `work_id`s, (b) never copy the raw `work_id`, `cat`, `provenance`, `title`/`author`/`genre` verbatim without review, and (c) route every M-source-derived title/author/genre through the D-08 owner review (fail-closed) before it can ship. The masking CI guard must scan the built `.db` cell-by-cell, not just as a byte blob.

**Primary recommendation:** Structure the phase as: (Wave 0) freeze the schema doc + budgets doc + extend the masking guard to SQLite; (Wave 1) build the offline distillation as a **new committed script** (`scripts/build_discovery_sidecar.py`) that forks the *shape* of `same_work_spike/probe/scripts/build_atlas_draft.py`'s DB pipeline but re-mints opaque IDs, emits the review artifact, and runs behind the masking gate; (Wave 2) the review→re-distill loop producing `discovery.db` + `discovery-frames.md`; (Wave 3) `shared/discovery_service.py` (async, off-loop, fail-open) + `web/feature_flags.py` flag + a `web/discovery_assets.py` versioned loader modeled 1:1 on `web/atlas_assets.py`. No UI.

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Distillation research-DB → `discovery.db` | Offline / dev-box build | CI (masking gate + golden fixture) | Never at request time; heavy clustering/hashing; runs on the dev box like the atlas bake (METHOD §9: "never on the production web server"). |
| Deterministic claim/unit id hashing | Offline build | — | Frozen algorithm applied at build; reproducibility is a build property. |
| Neutral-title curation | Offline build + human (owner) | — | Owner is the fail-closed gate; artifact generated by the build, approved out-of-band, re-consumed by the build. |
| Sidecar integrity + version gate | Frontend server (startup) | — | Startup fail-closed load (mirrors `atlas_assets.load_atlas_state`), never per-request `os.path.exists`. |
| Banded claim reads | API / Backend (`shared/discovery_service.py`) | Database (SQLite sidecar) | All reads through one async chokepoint, off the event loop, indexed + bounded. |
| Feature-flag + readiness gate | Frontend server | — | `discovery_available()` = flag AND loaded (mirrors `atlas_preview_available()`). |
| Masking leak-vector guard | CI / Static | — | Permanent gate over `.db` + surfaces + committed repo; fail-closed. |
| Provenance masking | Offline build (structural) | CI (verification) | Masking is enforced at the build boundary (no leaky columns exist), CI proves it. |

## Standard Stack

### Core (all already present — nothing to install for the runtime path)
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| Python `sqlite3` | stdlib (SQLite 3.45.1) `[VERIFIED: python -c import sqlite3]` | Sidecar read/write; `PRAGMA integrity_check`; indexed bounded queries | Every one of the 13 `shared/*_service.py` sidecars uses it; the project's entire read-only reference-data layer is SQLite. |
| Python `hashlib` | stdlib | SHA-256 for `claim_id` (DATA-01), `unit_id` (DATA-10), source-DB hash + content-hash (DATA-08), frame content hash (DATA-02) | DATA-01 freezes SHA-256; matches `atlas_assets` content-hash idiom. |
| `shared.thread_local_db.ThreadLocalConnection` | in-repo | Per-thread SQLite connections so concurrent `run_in_executor`/`run.io_bound` calls each get their own connection | The established sidecar-thread-safety pattern (`fjms_service` line 26, 753). |
| `brotli` | 1.2.0 (already a runtime dep since Phase 133) | Not required for the sidecar (SQLite is not Brotli-served); listed only because the loader model reuses it optionally | Already vetted in Phase 133 legitimacy audit. |

### Supporting (OFFLINE distillation only — dev box, never shipped/imported by web)
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `networkx` | 3.6.1 `[VERIFIED]` | Connected-components / graph ops if the distillation clusters MS-MS pairs into works for the discovery lane | Only in `scripts/build_discovery_sidecar.py`, run offline. Prefer `scipy.sparse.csgraph.connected_components` for scale (build_atlas_draft.py already uses it). |
| `python-louvain` (`community`) | present `[VERIFIED]` | Community detection (the atlas bake uses it) — likely NOT needed for the spine (works come from `track1_matches.work_id` + page-chains, not Louvain) | Offline only; evaluate whether the spine needs any clustering at all (see Pitfall 8). |
| `numpy` / `scipy` | 2.4.3 / 1.17.1 `[VERIFIED]` | Vectorized set ops / sparse CC at 1.33M-pair scale | Offline only. |

**Do NOT add any new runtime dependency.** The shipped web path is stdlib-only.

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| SQLite sidecar | Supabase/Postgres | Rejected — Supabase is reserved for community features (Phase 137). Read-only reference data is always a local SQLite sidecar (CLAUDE.md architecture). |
| One `discovery.db` | Multiple sidecars (works/claims/units split) | One file is simpler for the versioned-filename + atomic-rename + integrity-check contract (DATA-08). Use one DB, multiple tables. |
| Forking `build_atlas_draft.py` in place | New committed `scripts/build_discovery_sidecar.py` | Must fork — the probe script lives in a gitignored tree and reads/writes raw (leaky) work_ids and an HTML draft; the product script is committed, masked, and emits `.db`+artifacts. Reuse the *DB-read shape*, not the file. |

**Installation:** None. Runtime is stdlib. Offline distillation deps (`networkx`, `python-louvain`, `numpy`, `scipy`) are already installed in the research environment.

**Version verification (runtime):**
```bash
python -c "import sqlite3; print(sqlite3.sqlite_version)"   # 3.45.1 [VERIFIED 2026-07-21]
```

## Package Legitimacy Audit

> This phase installs **no external packages**. The runtime path is Python stdlib (`sqlite3`, `hashlib`) plus in-repo modules. The offline distillation reuses packages already vetted/installed in the research env (`networkx`, `python-louvain`, `numpy`, `scipy`) and imported only by a dev-box script never shipped to or imported by the web process.

| Package | Registry | Age | Downloads | Source Repo | slopcheck | Disposition |
|---------|----------|-----|-----------|-------------|-----------|-------------|
| (none — no install step) | — | — | — | — | n/a | No audit required |

**Packages removed due to slopcheck [SLOP] verdict:** none.
**Packages flagged as suspicious [SUS]:** none.

*If a later plan proposes any new package (it should not), run the Package Legitimacy Gate before adding it.*

## Architecture Patterns

### System Architecture Diagram

```
   OFFLINE (dev box, never at request time) ─────────────────────────────
   fullcorpus_v2.db (3.1 GB, gitignored research DB)
     ├─ accepted_pairs_canonmask (1.33M MS-MS page pairs; flank_class)
     ├─ track1_matches / work_query_hits_fullv2 (work↔page identifications)
     ├─ page_chains_* (MS-MS chains), passage_units_* (unit clustering)
     └─ pages (OUR text; provenance htr/fgp/pgp; offsets index into this)
            │
            ▼
   scripts/build_discovery_sidecar.py  (NEW, committed, masked)
     1. select shown-work set  ── genre filter (D-05/06) ──► TITLE REVIEW ARTIFACT
     2. mint OPAQUE work_id (drop M:/J:/REF prefixes + filenames)   │ (owner edits,
     3. assign band per claim key (precedence) from E1 frame        │  fail-closed)
     4. build both claim families + child alignment rows            ◄─┘ approved rows only
     5. build witness-units (Oxford parts ∪ physical joins)
     6. store evidence as OFFSETS + text-layer + HTR snapshot hash (NO text)
     7. compute deterministic claim_id / unit_id (SHA-256)
            │                                   │
            ▼                                   ▼
   discovery-v1-<hash>.db (≤300 MB)      discovery-frames.md (frozen frame:
     + meta/release-contract table         per-band dedup counts, dedup formula,
                                            overlap-resolution counts, frame hash)
            │
   ══ MASKING CI GATE (scripts/check_atlas_masking.py --scan-sqlite + --scan-repo) ══
            │  (fail-closed; scans schema + EVERY cell; M-source/R-source patterns)
            ▼
   DEPLOY: temp-upload → PRAGMA integrity_check + row-count/hash verify → atomic rename → code
            │
   RUNTIME (web box) ────────────────────────────────────────────────────
   web/main.py startup ─► web/discovery_assets.py::load_discovery_state()
       (fail-closed: open, PRAGMA integrity_check, schema-version match,
        release-contract row-count check) ─► discovery_available()=flag AND ready
            │
   shared/discovery_service.py  (async chokepoint)
       run_in_executor(sync SQLite query) wrapped in asyncio.wait(timeout)
       + bounded-concurrency semaphore (heavy) + LRU (browse-enrichment)
       + server-side LIMIT/OFFSET pagination
            │  overload/timeout ─► "temporarily unavailable" (never a hang)
            ▼
   (downstream consumers — Phases 136+; NOT this phase)
```

### Recommended Project Structure (new/changed files)
```
scripts/
├── build_discovery_sidecar.py   # NEW offline distillation (committed, masked)
├── check_atlas_masking.py       # EXTEND: add --scan-sqlite; register R-source patterns
shared/
├── discovery_service.py         # NEW async read-only chokepoint (fail-open)
web/
├── discovery_assets.py          # NEW versioned fail-closed startup loader (mirror atlas_assets.py)
├── feature_flags.py             # EXTEND: DISCOVERY_ENABLED
├── main.py                      # EXTEND: call load_discovery_state() at startup
docs/specs/
├── discovery-sidecar-schema-v1.md   # NEW frozen schema + claim_id/unit_id recipe (like atlas-asset-schema-v1.md)
├── discovery-frames.md              # NEW exit artifact (frozen frame)
├── discovery-budgets.md             # NEW exit artifact (PERF-01 caps)
discovery_data/                  # NEW sidecar dir (gitignored payload; deploy asset-first)
└── discovery-v1-<content_hash>.db
```

### Pattern 1: Fail-closed versioned startup loader (DATA-08)
**What:** Load + validate the sidecar ONCE at startup; any failure leaves `ready=False` with no traceback; the app stays fully up. **Directly mirror `web/atlas_assets.py`** (verified, shipped Phase 133).
**When to use:** Always — this is the DATA-07/DATA-08 fail-open + reject-incompatible mechanism.
**Example (adapted skeleton — model on the real `web/atlas_assets.py::load_atlas_state`):**
```python
# web/discovery_assets.py  (NEW)  — structure mirrors web/atlas_assets.py
_EXPECTED_SCHEMA_VERSION = 1

def load_discovery_state() -> bool:
    new_state = _DiscoveryState(ready=False)
    try:
        db_path = _resolve_versioned_db(DISCOVERY_DATA_DIR)   # discovery-v1-<hash>.db
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        # release contract in a meta table (fjms_service uses SELECT value FROM meta WHERE key=...)
        meta = _read_meta(conn)
        if int(meta["schema_version"]) != _EXPECTED_SCHEMA_VERSION:
            raise ValueError("incompatible schema_version")     # reject incompatible snapshot
        if conn.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ValueError("integrity_check failed")
        _verify_expected_row_counts(conn, meta)                 # release-contract cross-check
        new_state = _DiscoveryState(ready=True, path=db_path, meta=meta)
    except Exception as exc:                                    # fail-closed
        logger.info("Discovery sidecar not loaded (fail-closed): %s", exc)
        new_state = _DiscoveryState(ready=False)
    with _lock:
        globals()["_state"] = new_state
    return new_state.ready

def discovery_available() -> bool:
    return bool(DISCOVERY_ENABLED and _state.ready)   # flag AND readiness (Phase 133 D-13)
```
*Source: `web/atlas_assets.py` lines 164–294 (verified this session).*

### Pattern 2: Off-event-loop query with timeout → overload (DATA-06)
**What:** Run the blocking SQLite query in a threadpool via `run_in_executor`, wrap in `asyncio.wait(..., timeout=…)` (NOT bare `wait_for`, which cannot cancel a running thread), and gate heavy queries behind a non-blocking semaphore that raises a 503-style "temporarily unavailable" when full.
**When to use:** Every `DiscoveryService` method the web calls.
**Example (adapted from `web/search_api.py` lines 400–458, 1121–1159):**
```python
# shared/discovery_service.py  (NEW)
async def get_work_witnesses(self, work_id, *, page, page_size, timeout_s):
    loop = asyncio.get_event_loop()
    fut = loop.run_in_executor(None, self._sync_work_witnesses, work_id, page, page_size)
    done, _pending = await asyncio.wait({fut}, timeout=timeout_s)
    if not done:                       # run_in_executor cannot cancel the thread; do not await it
        raise DiscoveryUnavailable("temporarily unavailable")   # -> caller renders fail-open copy
    return fut.result()
```
*Note: `web/search_api.py` deliberately uses `asyncio.wait` over `wait_for` because `run_in_executor` threads are not cancellable — the comment at lines 1131–1140 is the load-bearing rationale. Reuse it.*

### Pattern 3: Read-only sidecar service with graceful-absent (DATA-06/07)
**What:** Module-level sidecar open; every method returns empty/`None` on any error and when `self._conn is None`; `is_available()` predicate; `get_version()` from a `meta` table.
**When to use:** The synchronous core of `DiscoveryService`.
**Example (verified `shared/fjms_service.py` lines 789–811):**
```python
def is_available(self) -> bool:
    return self._conn is not None
def get_version(self):
    if self._conn is None: return None
    try:
        row = self._conn.execute("SELECT value FROM meta WHERE key='version'").fetchone()
        return row["value"] if row else None
    except Exception as e:
        logger.error("...: %s", e); return None
```

### Pattern 4: Deterministic content-key hashing (DATA-01/DATA-10)
**What:** `claim_id` = SHA-256 over a frozen canonical UTF-8 serialization; `unit_id` = SHA-256 over sorted member sys_ids.
**Example (recipe frozen by DATA-01):**
```python
import hashlib
def claim_id_work_witness(sys_id, work_id, claim_type):
    key = f"work_witness|{sys_id}|{work_id}|{claim_type}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()
def claim_id_ms_ms(sys_id_a, sys_id_b, claim_type):        # canonical order a<b enforced UPSTREAM
    assert sys_id_a < sys_id_b
    key = f"ms_ms|{sys_id_a}|{sys_id_b}|{claim_type}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()
def unit_id(member_sys_ids):
    key = "unit|" + "|".join(sorted(member_sys_ids))        # sort as STRINGS, freeze in schema doc
    return hashlib.sha256(key.encode("utf-8")).hexdigest()
```
*Freeze in `docs/specs/discovery-sidecar-schema-v1.md`: the exact field order, the `|` delimiter, that sys_ids are the 18-digit numeric strings, and the sort collation (byte/lexicographic). A uniqueness constraint on the key fields backs the determinism.*

### Recommended `discovery.db` schema (DDL sketch — planner owns final layout, D-11 discretion)
```sql
-- release contract (DATA-08); read at startup, verified against actuals
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
-- keys: schema_version, sidecar_version, source_db_sha256, build_date, data_as_of,
--       htr_snapshot_hash, expected_rows_work_claims, expected_rows_ms_claims,
--       expected_rows_alignments, expected_rows_works, expected_rows_units, frame_content_hash

-- works: OPAQUE product ids only; source_corpus is masked codename, NEVER displayed (D-03a)
CREATE TABLE works (
  work_id TEXT PRIMARY KEY,            -- opaque product id (minted; NOT raw M:/J:/REF)
  neutral_title TEXT NOT NULL,         -- human-reviewed (D-07); fail-closed exclude if unreviewed
  author TEXT, genre TEXT,             -- reviewed displayable fields
  source_corpus TEXT NOT NULL          -- masked codename ('sefaria'|'ja'|'msource'); internal-only
);

-- SUPERSEDED: this 6-table sketch (work_witness_claims / work_witness_pages / ms_ms_claims / ms_ms_alignments)
-- is HISTORICAL research output. The FROZEN model is the two-table split in 134-CONTEXT.md CONTRACT CORRECTION
-- (discovery_claim PK (page_id, work_id) + discovery_evidence with an evidence_kind discriminator) — see docs/specs/discovery-sidecar-schema-v1.md.
-- work–witness claims (DATA-01 family a)
CREATE TABLE work_witness_claims (
  claim_id TEXT PRIMARY KEY,           -- sha256(work_witness|sys_id|work_id|claim_type)
  sys_id TEXT NOT NULL,
  work_id TEXT NOT NULL REFERENCES works(work_id),
  claim_type TEXT NOT NULL,            -- frozen code (direct_witness|quotes_this_work|textual_parallel|direct_text_overlap)
  band TEXT NOT NULL,                  -- exactly one post-precedence
  sidecar_version TEXT NOT NULL,
  UNIQUE (sys_id, work_id, claim_type)
);
CREATE INDEX idx_ww_work ON work_witness_claims(work_id, band);
CREATE INDEX idx_ww_sys  ON work_witness_claims(sys_id);

-- supporting pages per work–witness claim (page→witness aggregation)
CREATE TABLE work_witness_pages (
  claim_id TEXT NOT NULL REFERENCES work_witness_claims(claim_id),
  page_id TEXT NOT NULL,               -- {sys_id}_IE..._P..._FL... (renderable folio key)
  text_layer TEXT NOT NULL,            -- htr|fgp|pgp (which OUR-text layer offsets index)
  span_start INT NOT NULL, span_end INT NOT NULL,   -- offsets into OUR text (NO text stored)
  matched_letters INT, density REAL
);
CREATE INDEX idx_wwp_claim ON work_witness_pages(claim_id);

-- MS–MS relation claims (DATA-01 family b): parent = stable voting target
CREATE TABLE ms_ms_claims (
  claim_id TEXT PRIMARY KEY,           -- sha256(ms_ms|sys_id_a|sys_id_b|claim_type), a<b
  sys_id_a TEXT NOT NULL, sys_id_b TEXT NOT NULL,   -- sys_id_a < sys_id_b
  claim_type TEXT NOT NULL, band TEXT NOT NULL, sidecar_version TEXT NOT NULL,
  UNIQUE (sys_id_a, sys_id_b, claim_type), CHECK (sys_id_a < sys_id_b)
);
-- CHILD alignment rows: queryable BY PAGE (PANEL-02 "pages related to this page")
CREATE TABLE ms_ms_alignments (
  claim_id TEXT NOT NULL REFERENCES ms_ms_claims(claim_id),
  page_id_a TEXT NOT NULL, page_id_b TEXT NOT NULL,
  a_start INT, a_end INT, b_start INT, b_end INT,    -- each side's own span into OUR text
  aligned_len INT, density REAL, flank_class TEXT
);
CREATE INDEX idx_align_page_a ON ms_ms_alignments(page_id_a);   -- serve by-page directly
CREATE INDEX idx_align_page_b ON ms_ms_alignments(page_id_b);
CREATE INDEX idx_align_claim  ON ms_ms_alignments(claim_id);

-- witness units (DATA-10): claims stay per-sys_id; membership recorded here
CREATE TABLE witness_units (unit_id TEXT PRIMARY KEY);          -- sha256 over sorted members
CREATE TABLE witness_unit_members (
  unit_id TEXT NOT NULL REFERENCES witness_units(unit_id),
  sys_id TEXT NOT NULL,
  merge_basis TEXT NOT NULL,           -- 'oxford_part' | 'physical_join' (NEVER 'scribe')
  UNIQUE (sys_id)                      -- a sys_id belongs to at most one unit per snapshot
);
```
**Size sanity (against ≤300 MB, D-11):** `ms_ms_alignments` is the dominant table (child rows ≈ the 1.33M accepted pairs before per-band trimming). At ~60–90 bytes/row plus indexes, the *untrimmed* alignment set is roughly ~150–250 MB — **the per-band inclusion caps the planner sets are what keep it under budget** (e.g. cap `screening_canon`/`screening_rb` child rows, keep all `expert_verified`/`tier_a`). Record final counts in `discovery-frames.md`. `[ASSUMED]` byte-per-row — measure during the build.

### Anti-Patterns to Avoid
- **Shipping the raw `work_id`.** It embeds `M:`/`J:`/`REF` and (for M-source) the source filename — a direct provenance leak. Mint opaque ids; keep a build-time-only mapping in the gitignored research tree, never in `discovery.db`.
- **Copying `pages.text` (or `cat`, raw `provenance`, raw `title`/`author`/`genre`) into the sidecar.** DATA-03: store offsets + snapshot hash only. Titles/authors/genres ship ONLY post-review.
- **Per-request `os.path.exists` / opening the DB per request.** Startup-load once (atlas_assets model); readiness is a cached predicate.
- **`asyncio.wait_for(run_in_executor(...))`.** A timed-out `wait_for` cannot cancel the thread and will strand it; use `asyncio.wait` and abandon the future (search_api precedent).
- **Blindly `sqlite3.connect(path)` on a network/shared file at request time.** Read-only `mode=ro` URI; per-thread connections.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Fail-closed versioned asset loader | A bespoke loader | Copy `web/atlas_assets.py` structure (`load_*_state`, `_lock`, `*_available()`) | Phase 133 already hardened it across 4 Codex review rounds (header/bounds/hash/fail-closed). |
| Off-loop query + overload | New threadpool mgmt | `web/search_api.py` `run_in_executor`+`asyncio.wait`+semaphore | The cancellation subtlety (threads aren't cancellable) is already solved there. |
| Masking scanner | A new scanner | Extend `scripts/check_atlas_masking.py` (add a `--scan-sqlite` mode) | It already does literal+NFC/NFD+casefold+UTF-8/16/32+URL/HTML/JS, fail-closed, repo+asset surfaces (9 HIGH + 1 MED hardened). Do NOT re-implement matching. |
| Review artifact | New tooling | Model on `scripts/export_translation_audit_sample.py` + `web/components/translation_report.py` (CONTEXT D-08) | Existing audit-sample→edit→re-consume loop. |
| Per-thread SQLite | Manual connection pools | `shared.thread_local_db.ThreadLocalConnection` | Existing sidecar-thread-safety primitive. |
| Graph connected-components at 1.33M pairs | Python dict BFS | `scipy.sparse.csgraph.connected_components` (offline) | `build_atlas_draft.py` already uses it; Python BFS OOMs at pilot scale (METHOD §9). |

**Key insight:** Phase 133 shipped, and Codex-hardened, *exactly* the loader + flag + masking-scan primitives this phase needs. The spine's novelty is the **distillation correctness + masking discipline**, not the plumbing — spend the review budget there.

## Runtime State Inventory

> This phase is greenfield (a new sidecar + new service), not a rename/refactor. But the DATA-05 one-time-cleanup verification and the masking posture make a leak-vector inventory load-bearing, so it is included in that spirit.

| Category | Items Found | Action Required |
|----------|-------------|------------------|
| Stored data (leak vectors in the source DB) | `fullcorpus_v2.db`: `pages.text`/`pages.provenance` (OUR text; `provenance∈{htr,fgp,pgp}` — NOT corpus, safe), `pages.fgp_id`; **`accepted_pairs_canonmask.bucket_*`** (values `all`/`tier1t`/`bh` — internal, do not ship raw); **`track1_matches.cat`** (contains the literal M-source name as a VALUE — MUST map to masked codename), **`track1_matches.work_id`/`title`/`author`/`genre`/`mesirah`** (M-source-derived; leak vectors) | Distillation: mint opaque ids; map `cat`→masked `source_corpus`; route titles/authors/genres through D-08 review; store offsets not text. |
| Live service config | None — no external service holds discovery state; Supabase is Phase 137, not this phase. | None. |
| OS-registered state | None. | None. |
| Secrets/env vars | `MASKING_SCAN_PATTERNS_FILE` (existing, gitignored) — MUST gain R-source tokens (D-03c); new `DISCOVERY_ENABLED` flag (no secret). The off-repo M-source catalogue XLSX + R-source corpus are gitignored — never referenced by real path in committed files. | Add R-source patterns to the gitignored pattern file; document `DISCOVERY_ENABLED` in CLAUDE.md. |
| Build artifacts | The offline probe tree (`same_work_spike/probe/**`) is gitignored — verified it must stay untracked. The built `discovery-v1-<hash>.db` goes in a gitignored `discovery_data/` (deploy asset-first, like `atlas_data/`). | Add `discovery_data/` to `.gitignore`; the `.db` is never committed. |

**Nothing found in "Live service config" / "OS-registered state":** confirmed — this phase adds no live external state and no OS registrations.

## Common Pitfalls / Landmines

### Landmine 1: `work_id` is NOT opaque — it embeds source + M-source filename
**What goes wrong:** Shipping `work_id` verbatim leaks provenance. Verified: prefixes are `M:` (M-source, 278,636 rows), `J:` (JA), `REF` (canon); and `build_works_witnesses_table.py` shows M-source `work_id = "M:" + filename_stem`, i.e. the actual M-source file/work name is inside the id.
**How to avoid:** Mint fresh opaque `work_id`s (e.g. `w:` + zero-padded counter, or a salted hash — but a hash of the raw id is still a stable *opaque* value only if the raw id never ships; a counter is safest). Keep the raw→opaque map ONLY in the gitignored research tree. DATA-02 explicitly requires "opaque product IDs (no raw research IDs)".
**Warning sign:** any `M:`, `J:`, `REF`, or a filename-shaped token in a `work_id` cell → masking gate must fail.

### Landmine 2: `cat` column value = the literal M-source name
**What goes wrong:** `track1_matches.cat` holds the real M-source name as a data value alongside `Sefaria`/`JA`/`Bible`/etc. Copying `cat` into the sidecar leaks it.
**How to avoid:** Map `cat` → a masked `source_corpus` code (`sefaria`/`ja`/`msource`/`canon`) at distillation. Never store the raw `cat`.

### Landmine 3: Titles/authors/genres are M-source metadata → fail-closed review
**What goes wrong:** Auto-shipping M-source `title`/`author`/`genre` (a) can leak M-source naming and (b) violates D-07 (no research-title fallback).
**How to avoid:** D-08 review artifact (source masked); unreviewed → EXCLUDED. Auto-adopt only open-corpus (Sefaria/JA) canonical titles with a light spot-check. The build must literally have no code path that ships an unreviewed M-source title.

### Landmine 4: SQLite masking scan must be cell-level, not byte-level
**What goes wrong:** Scanning `discovery.db` as a raw byte blob can miss values split across pages/overflow, and gives no cell provenance for triage. It's also brittle to SQLite internals.
**How to avoid:** Add a `--scan-sqlite` mode to `check_atlas_masking.py`: connect read-only, scan `sqlite_master.sql` (schema/identifiers) + iterate every table, every row, every cell value through the existing matcher. Keep the raw-byte `--scan-asset` pass too (defense-in-depth, catches freelist/deleted-page residue).

### Landmine 5: `page_id` embeds `sys_id` (fine) — but don't over-mask
**What goes wrong:** `page_id` = `{sys_id}_IE{ie}_P{p}_FL{fl}` (verified). This is OUR production folio key (masking-safe — it's the same identifier the browse/IIIF layer already uses). Over-zealous "mask everything numeric" would break click-through (PANEL-02) and the browse-map join.
**How to avoid:** `sys_id`/`page_id`/`fl_id`/shelfmarks/library_code are all our own catalogue identifiers and are explicitly masking-safe (the atlas schema §Scope confirms `sys_id`+shelfmark are intended masking-safe fields). Mask corpus provenance, not our own IDs.

### Landmine 6: Determinism traps in the hash
**What goes wrong:** Non-deterministic `claim_id`/`unit_id` across rebuilds (dict iteration order, int-vs-str sys_id, locale sort) breaks DATA-01's "stable across rebuilds" + Phase 137 judgment targeting.
**How to avoid:** Freeze: sys_ids as their 18-digit numeric strings; `sorted()` byte/lexicographic; explicit field order + `|` delimiter; SHA-256 over UTF-8. Add a golden-fixture test that re-running the id functions on frozen inputs yields committed hashes (mirror the atlas golden-fixture approach).

### Landmine 7: Event-loop blocking via the "small" query
**What goes wrong:** A query that's usually fast (browse-enrichment) occasionally scans and blocks the loop for seconds under load.
**How to avoid:** ALL DiscoveryService reads go off-loop via `run_in_executor` (even the cheap ones), each with a timeout; every query is indexed + `LIMIT`-bounded; pagination is server-side `LIMIT/OFFSET` (or keyset). Prove it with the overload test (Validation Architecture).

### Landmine 8: Deriving "works" for the discovery lane vs reusing track1 works
**What goes wrong:** Over-engineering — running Louvain/CC clustering to invent works when `track1_matches.work_id` + `work_query_hits_fullv2` already give work↔page identifications for the reference-work lane.
**How to avoid:** For the launch shown-set (Sefaria+JA+M-source literary), works come from the reference catalogues (track1 / work_query_hits), NOT from unsupervised clustering. MS-MS relation claims come from `accepted_pairs_canonmask` + `page_chains_*`. Clustering (Louvain) is an atlas-layout concern, not a spine concern — confirm with the planner before adding any clustering to the spine.

### Landmine 9: `shadowed_by IS NULL` filter must be applied
**What goes wrong:** `track1_matches` carries a `shadowed_by` column; NULL = live row. Consumers that forget the filter double-count nested/quoted overlaps and blur "quotes-this-work" into "direct witness".
**How to avoid:** Every read of `track1_matches` in the distillation must include `WHERE shadowed_by IS NULL` (this is a documented, repeatedly-bitten rule in the probe tree: SYNTHESIS-AND-PLAN.md "all consumers must filter WHERE shadowed_by IS NULL").

### Landmine 10: Band precedence must be within-key only
**What goes wrong:** Applying band precedence across different works on the same manuscript collapses legitimately-distinct claims (Talmud+Rashi+Tosafot; Mikra+Targum+Tafsir).
**How to avoid:** Dedup/precedence operate ONLY within a single `claim_id` key (DATA-01/02). A manuscript can carry multiple work–witness claims; each keeps its own band.

## Code Examples

### Claim-type mapping from engine `flank_class` (frozen semantics — DATA-01)
`accepted_pairs_canonmask.flank_class` verified values + counts: `island` 582,599 / `continuation` 387,333 / `edge` 300,237 / `ambig` 61,930. METHOD §8.1 gives the frozen semantics:
```text
continuation  -> flanks also align -> running witnesses of the same text
                 => "textual parallel" (MS-MS same-work) / "direct witness" (work-witness, Track 1)
island        -> flanks dissimilar -> quotation / shared formula, NOT a common work
                 island ∧ canonical  => "quotes-this-work" (canonical quotation, Track 1)
                 island ∧ non-canon  => indirect textual witness / "direct text overlap"
edge          -> boundary/partial continuation (planner: fold to continuation or ambig per E1 rule)
ambig         -> ambiguous (screening only)
```
*The exact `flank_class`→`claim_type` code table is FROZEN by DATA-01 before distillation. Confirm the edge/ambig routing against the E1 frame with the owner; bilingual wording is deferred (D-12).*

### Genre signal for the M-source literary/piyyut/documentary split (D-05/D-06)
Verified work-level genre distribution WITHIN the masked M-source `cat` (English glosses of the Hebrew genre labels; counts are `DISTINCT work_id`):
```text
PIYYUT (exclude):        ~2,208 works  (piyyut & prayer 1,617; Spanish poetry 590; +1)
DOCUMENTARY (exclude):     ~446 works  (letters 426; deeds 8; Judean-desert docs 4+2; epigraphy 6)
LITERARY (candidate keep): ~841 works  (Geonic 447; Talmud&Midrash 237; Karaite 58; rabbinic 45;
                                        belles-lettres 24; science 14; philology 13; Arabic-transl. 3)
MODERN/other (owner call):  ~32 works  (study/journalism/publicism — likely exclude as post-Genizah)
```
M-source total matched works ≈ 3,527. Open-corpus works (ship all): Sefaria 229 + JA 106 + canonical strata (Bible/Bavli/Mishnah/Tosefta/Yerushalmi/Targum/Liturgy) ≈ 336 → ~671. So the launch shown-set candidate ≈ **~671 open + ~841 M-source literary ≈ ~1,500 works before owner hand-pick** (D-06: owner is the final gate). `[VERIFIED: fullcorpus_v2.db this session]`

### Witness-unit sources (DATA-10)
- **Oxford codicological parts:** `libraries.csv` column index 1 (`oxford_part_id`, per CLAUDE.md) groups several sys_ids into one MS. ~11,160 of the first 200k rows carry a non-empty part id `[VERIFIED]`.
- **Physical joins:** `fjms_enrichment.db` `joins(AlmaId, JoinGroupId, ScholarName, Comment, JoinType)`. Merge when `JoinType` ∈ {`Physical Join` (7,183), `Codex join` (5,202), `Partial Physical Join` (109), `Unspecified join` (129)}; **EXCLUDE `Scribe join` (31)** per DATA-10 ("same scribe does NOT merge"). NULL/`''`/`Insufficient information`/`Partially Physical and not Join` → planner decides (recommend: do NOT merge on ambiguous basis — conservative, keeps units tight). `[VERIFIED: fjms_enrichment.db this session]`
- Existing readers to reuse: `shared/fjms_service.py::get_join_group` (lines 2651–2700) already resolves join partners; `shared/browse_map_utils.py` handles multi-IE/part maps.

### Masking guard SQLite extension (DATA-05)
```python
# scripts/check_atlas_masking.py  (EXTEND) — new scan surface, reuses build_matcher()
def scan_sqlite(db_path, patterns) -> list[Issue]:
    matcher = build_matcher(patterns)
    issues = []
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    for (sql,) in conn.execute("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL"):
        issues += matcher.scan(_encode_text(sql), f"{db_path}::schema")     # identifiers/DDL
    for (tbl,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table'"):
        cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{tbl}")')]
        for row in conn.execute(f'SELECT * FROM "{tbl}"'):
            for col, val in zip(cols, row):
                if isinstance(val, str):
                    issues += matcher.scan(_encode_text(val), f"{db_path}::{tbl}.{col}")
    return issues  # fail-closed on any connect/read error (raise ScanError) — matches the module posture
```
*Keep the existing `--scan-asset <discovery.db>` raw-byte pass as well (freelist/residue). Register R-source tokens in the gitignored `MASKING_SCAN_PATTERNS_FILE` now (D-03c).*

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Standalone backend process + DB | Read-only SQLite sidecars + Supabase for community only | Jan 2026 | Discovery reads are a sidecar; no new service process. |
| Ad-hoc feature checks | Flag AND readiness predicate (`atlas_preview_available()`) | Phase 133 (2026-07) | `discovery_available()` must AND the flag with startup-load readiness. |
| Byte-blob asset masking | Multi-surface, encoding-aware, fail-closed scanner | Phase 133 (2026-07) | Extend, don't reinvent; add cell-level SQLite mode. |

**Deprecated/outdated:**
- The gitignored probe HTML draft (`build_atlas_draft.py` → `review/atlas_draft.html`) is a research artifact; the product path emits `.db` + committed docs, not HTML.
- Raw research IDs (`M:`/`J:`/`REF` work_ids) — replaced by minted opaque product ids.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Per-row byte estimate for `ms_ms_alignments` (~60–90 B) putting the untrimmed set ~150–250 MB | Schema DDL / size sanity | If larger, per-band caps must be tighter; measure during the build before freezing `discovery-frames.md`. |
| A2 | The launch shown-set is ~1,500 candidate works pre-owner-pick (~671 open + ~841 M-source literary) | Genre signal | Owner hand-pick (D-06) is authoritative; this is only a sizing estimate for the review artifact. |
| A3 | Works for the launch lane come from reference catalogues (track1/work_query_hits), not unsupervised clustering | Pitfall 8 | If the owner wants discovery-only (non-reference) works in the launch spine, add a clustering step — confirm before planning. |
| A4 | `edge`/`ambig` flank classes route to screening/ambiguous rather than a high band | Claim-type mapping | The exact routing is set by the E1 frame + owner; confirm before freezing the claim_type table. |
| A5 | The band values R-A 0.889 / R-B 0.859 / R-CANON 0.647 and the per-claim band assignment come from the E1 frame (`e1_r3_frame.jsonl` / `e1_band_frame.py`) rather than being recomputable from `accepted_pairs_canonmask` alone | Bands & the Frame | If the band assignment lives in a different artifact, the distillation's band-join source changes; verify the exact E1 frame file the distillation should read with the researcher. |
| A6 | NULL/ambiguous `JoinType` rows should NOT merge into a witness unit (conservative) | Witness units | If the owner wants ambiguous joins to merge, unit membership widens; low risk (claims stay per-sys_id regardless). |
| A7 | Subsecond/latency PERF-01 caps are achievable with straightforward indexing at this row scale | Budgets | Must be MEASURED before release (PERF-01 says so); the budgets doc is tunable only by versioning. |

**If this table looks long:** it reflects honest scoping — the distillation's *semantic* choices (band source, claim-type routing, work provenance) are owned by the researcher/owner and the E1 track (Phase 135), and the planner should schedule a short confirmation with the researcher (A3/A4/A5) before Wave 1 freezes the schema doc.

## Open Questions (RESOLVED — see CONTEXT.md CONTRACT CORRECTION + discovery-sidecar-schema-v1.md OQ2/OQ3)

1. **Which E1 artifact is the authoritative band source for the distillation?**
   - What we know: bands + the frozen frame live in the E1 track (`e1_band_frame.py`, `e1_r3_frame.jsonl`, `e1_r3_freeze.json`, `e1_certification_registry.json`); R-A/R-B/R-CANON precision values are recorded there; `accepted_pairs_canonmask` carries the raw signals (`flank_class`, `density`, `aligned_len`) but not the final band label.
   - What's unclear: the exact file + join key the distillation should read to stamp each claim's band.
   - Recommendation: Wave 0 task — researcher confirms the single authoritative E1 frame file + its claim key; freeze it in `docs/specs/discovery-sidecar-schema-v1.md`. (Note: CERT-01 in Phase 135 freezes AGAINST the Phase 134 frame, so the frame-source decision is load-bearing downstream.)

2. **Does the launch spine need any unsupervised clustering, or only reference-catalogue works?** (A3)
   - Recommendation: default to reference-catalogue works only for the shown-set; treat discovery-only clusters as a gen-2 concern unless the owner says otherwise.

3. **HTR snapshot hash scope (DATA-03).**
   - What we know: offsets index into `pages.text` per `text_layer` (htr/fgp/pgp). Evidence must fail closed on text-version drift (render-time offset validation, Phase 136).
   - What's unclear: is the snapshot hash a single corpus-wide hash, or per-page? Per-page is more precise for drift detection but larger.
   - Recommendation: store one corpus-level `htr_snapshot_hash` in `meta` for the release contract PLUS a per-page hash (or `htr_n_chars`, already in `pages`) on `work_witness_pages`/alignments so render-time validation is page-scoped. Confirm the cheapest sufficient granularity with the Phase 136 planner.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Python `sqlite3` (SQLite) | sidecar read/write, integrity_check | ✓ | 3.45.1 | — |
| Python `hashlib` | deterministic ids / hashes | ✓ | stdlib | — |
| `fullcorpus_v2.db` (source research DB) | distillation input | ✓ | v2 (3.1 GB, gitignored) | — (blocks the build if absent; dev-box only) |
| `libraries.csv` | Oxford-part witness units | ✓ | ~255K rows | — |
| `fist_data/fjms_enrichment.db` | physical-join witness units + domains | ✓ | 1.5 GB | — |
| `networkx` / `python-louvain` / `numpy` / `scipy` | OFFLINE distillation (if clustering used) | ✓ | 3.6.1 / present / 2.4.3 / 1.17.1 | scipy CC if networkx too slow |
| `brotli` | loader model (optional; sidecar not Brotli-served) | ✓ | 1.2.0 | — |
| M-source catalogue XLSX (neutral titles/genre) | D-08 review artifact seed (M-source titles) | ✓ (off-repo, gitignored) | — | none — required for M-source title candidates; open-corpus titles from Sefaria/JA |
| `MASKING_SCAN_PATTERNS_FILE` (M-source + R-source tokens) | DATA-05 gate | ✓ (must add R-source tokens) | gitignored | none — gate fails closed (exit 1) if unset, by design |

**Missing dependencies with no fallback:** none on the dev box. (`fullcorpus_v2.db` and the M-source XLSX are dev-box-only; the web box needs only the built `discovery.db`.)
**Missing dependencies with fallback:** networkx→scipy CC (offline scale).

## Validation Architecture

> Nyquist validation is ENABLED (`workflow.nyquist_validation: true`). This phase has no test-runner config yet for discovery; the project uses `pytest tests/` (see Wave 0 gaps). Every deliverable below has an observable signal and an automated command the planner can turn into VALIDATION.md.

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest (existing; `tests/`), Node golden-fixture pattern precedent in `tests/atlas_bake/` |
| Config file | none discovery-specific — see Wave 0 |
| Quick run command | `pytest tests/test_discovery_*.py -x` |
| Full suite command | `pytest tests/` (Windows: use the marker-based GUI split per `feedback_full_suite_testing_windows`; discovery tests are non-GUI) |

### Phase Requirements → Test Map
| Req | Observable signal to PROVE | Test type | Automated command | File exists? |
|-----|----------------------------|-----------|-------------------|-------------|
| DATA-03/05 | Zero provenance leak in the shipped `.db` (schema + every cell) + committed repo | integration (CI gate) | `MASKING_SCAN_PATTERNS_FILE=... python scripts/check_atlas_masking.py --scan-sqlite discovery_data/discovery-v1-*.db --scan-repo --strict` (exit 0) | ❌ Wave 0 (extend scanner + test) |
| DATA-03 | No `text`/`cat`/raw-`work_id`/`title`/`author`/`genre`/`provenance` columns carry reference content; only offsets + snapshot hash | unit | `pytest tests/test_discovery_schema.py::test_no_reference_columns` (assert column allowlist; assert no `M:`/`J:`/`REF` in any `work_id`) | ❌ Wave 0 |
| DATA-01/02 | Deterministic `claim_id`/`unit_id` stable across rebuilds | unit (golden) | `pytest tests/test_discovery_ids.py::test_claim_id_golden` (frozen inputs → committed hashes) | ❌ Wave 0 |
| DATA-02 | ~~Exactly one band per claim key~~ **SUPERSEDED (F8 — DROPPED; see 134-VALIDATION.md)**: enforce VALID (evidence_kind × evidence_source × confidence_band) combinations instead | unit | `pytest tests/test_discovery_bands.py::test_valid_evidence_combinations` (multi-band claims allowed; multi-work-per-MS preserved) | ❌ Wave 0 |
| DATA-02 | Frozen-frame reproducibility: rebuild → identical frame content hash | integration | `pytest tests/test_discovery_frame.py::test_frame_hash_reproducible` (two builds from fixture → equal hash; matches `discovery-frames.md`) | ❌ Wave 0 |
| DATA-06 | Service never blocks the loop under overload; timeout → "temporarily unavailable" not hang | async unit | `pytest tests/test_discovery_service.py::test_overload_returns_unavailable` (monkeypatch a slow sync query; assert `DiscoveryUnavailable` within timeout + loop still responsive) | ❌ Wave 0 |
| DATA-06 | Every list query is bounded (`LIMIT`) + paginated server-side | unit | `pytest tests/test_discovery_service.py::test_pagination_bounds` | ❌ Wave 0 |
| DATA-07 | Flag OFF → `discovery_available()` False, all reads no-op | unit | `pytest tests/test_discovery_flag.py::test_flag_off_hides` | ❌ Wave 0 |
| DATA-07/08 | Sidecar absent / corrupt / incompatible-schema → `ready=False`, app stays up | unit | `pytest tests/test_discovery_loader.py::{test_absent,test_corrupt_integrity,test_incompatible_version}` (mirror `tests` for `atlas_assets`) | ❌ Wave 0 |
| DATA-08 | `PRAGMA integrity_check == ok`; release-contract row counts match actuals; source-DB hash recorded | integration | `pytest tests/test_discovery_release_contract.py` | ❌ Wave 0 |
| DATA-10 | Witness units merge Oxford parts + physical joins but NOT `Scribe join`; `unit_id` deterministic; ≤1 unit per sys_id | unit | `pytest tests/test_discovery_units.py::{test_scribe_not_merged,test_unit_id_deterministic}` | ❌ Wave 0 |
| PERF-01 | Query latencies within caps; discovery adds ≤250 MB RSS | measurement (documented in `discovery-budgets.md`) | a benchmark script over the real `.db` (p95 timings) + an RSS probe; recorded, not a hard unit gate | ❌ Wave 0 (bench script) |

### Sampling Rate
- **Per task commit:** `pytest tests/test_discovery_*.py -x` + the masking gate (`--scan-sqlite` on any freshly built fixture).
- **Per wave merge:** full discovery test set + `check_atlas_masking.py --scan-repo` (committed-content leak check).
- **Phase gate:** full masking gate over the real built `.db` + `--scan-repo` (exit 0); `discovery-frames.md` + `discovery-budgets.md` committed; loader fail-open tests green; overload test green.

### Wave 0 Gaps
- [ ] Extend `scripts/check_atlas_masking.py` with a `--scan-sqlite` mode + tests (`tests/test_masking_sqlite.py`).
- [ ] `tests/test_discovery_schema.py`, `test_discovery_ids.py`, `test_discovery_bands.py`, `test_discovery_frame.py`, `test_discovery_units.py` — build-output invariants over a small deterministic fixture DB.
- [ ] `tests/test_discovery_service.py`, `test_discovery_flag.py`, `test_discovery_loader.py`, `test_discovery_release_contract.py` — service + loader (model the loader tests on the existing atlas_assets tests).
- [ ] A benchmark/RSS script feeding `discovery-budgets.md` (PERF-01 numbers measured, not asserted).
- [ ] A tiny committed fixture `discovery.db` builder (deterministic, masking-safe synthetic data) so CI never needs the 3.1 GB research DB.

## Security Domain

> `security_enforcement` is not explicitly disabled; this is a data/infrastructure phase with no auth/session/UI. The dominant "security" property IS the provenance-masking gate (DATA-05), covered above.

### Applicable ASVS Categories
| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V5 Input Validation | yes | Read-only sidecar; `sqlite3` parameterized queries only (never string-format sys_id/work_id into SQL); open DB `mode=ro`. |
| V6 Cryptography | partial | SHA-256 used only as a content key / integrity hash (not a secret) — `hashlib`, never hand-rolled. |
| V12/V14 Files & Config | yes | Versioned filename + startup integrity/schema gate; sidecar payload gitignored; deploy temp→verify→atomic-rename (DATA-08). |
| V2/V3/V4 Auth/Session/Access | no | No auth in this phase (Supabase is Phase 137). |

### Known Threat Patterns for this stack
| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Provenance leak (M-source/R-source string in sidecar/repo/surface) | Information Disclosure | Permanent DATA-05 CI gate (`--scan-sqlite` + `--scan-repo`, fail-closed); structural masking (no leaky columns exist); opaque work_ids. |
| Corrupt/incompatible sidecar served | Tampering / DoS | Startup `PRAGMA integrity_check` + schema-version reject; fail-closed loader; app stays up. |
| Event-loop starvation under load | Denial of Service | Off-loop `run_in_executor` + timeout + bounded-concurrency semaphore → "temporarily unavailable". |
| SQL injection via sys_id/work_id | Tampering | Parameterized queries; ids are our own validated identifiers. |

## Sources

### Primary (HIGH confidence — verified this session)
- `same_work_spike/probe/data/fullcorpus_v2.db` — schema + row counts + `flank_class`/`cat`/`genre`/`work_id`/`page_id`/`provenance` shapes (gitignored; structure reported, values masked).
- `web/atlas_assets.py` (lines 1–321) — the fail-closed versioned startup-load model (DATA-08).
- `web/search_api.py` (lines 379–458, 1121–1159) — off-loop `run_in_executor` + `asyncio.wait` + semaphore→503 (DATA-06).
- `web/feature_flags.py` — `_env_enabled` + `ATLAS_PREVIEW_ENABLED` (DATA-07).
- `shared/fjms_service.py` (lines 1–80, 789–811, 2651–2700) — sidecar service pattern + `meta` table + `joins` schema/reader (DATA-06/10).
- `fist_data/fjms_enrichment.db` — `joins.JoinType` taxonomy (DATA-10).
- `scripts/check_atlas_masking.py` (lines 160–205, 1047–1116, 1207–1260) — masking scanner (DATA-05).
- `same_work_spike/probe/METHOD.md` §8.1–8.3 — flank/claim semantics + unit-level "same text" policy.
- `same_work_spike/probe/scripts/build_atlas_draft.py` / `build_works_witnesses_table.py` — DB-read pipeline shape + the `M:`+filename work_id derivation (leak vector).
- `docs/specs/atlas-asset-schema-v1.md` §9 — versioning/cache-invalidation model.
- `.planning/{ROADMAP,REQUIREMENTS,STATE}.md` + `134-CONTEXT.md` — requirements + decisions.

### Secondary (MEDIUM confidence)
- `same_work_spike/probe/{SYNTHESIS-AND-PLAN,PROBE-RESULTS}.md` + `e1_certification_registry.json` / `e1_r3_freeze.json` — band/frame lineage (the exact E1 band-source file needs researcher confirmation — Open Question 1).

### Tertiary (LOW confidence)
- Byte-per-row / latency estimates — training-based; flagged in Assumptions Log; MUST be measured during the build.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — verified present; runtime is stdlib.
- Architecture (loader/service/flag/masking patterns): HIGH — read the actual shipped Phase 133 + service code.
- Schema / claim model: HIGH on structure (verified DB), MEDIUM on band-source join (Open Question 1).
- Pitfalls / masking leak vectors: HIGH — the `work_id`/`cat`/title leak vectors were observed directly in the DB.
- Budgets/perf: LOW — must be measured (PERF-01 mandates measurement).

**Research date:** 2026-07-21
**Valid until:** ~2026-08-20 (30 days; stable in-repo patterns). Re-check if the E1 frame or the probe schema changes.

## RESEARCH COMPLETE

**Phase:** 134 - Discovery Data Spine
**Confidence:** HIGH

**Key findings:**
- The runtime path is stdlib-only; the loader, off-loop service, feature-flag, and masking-scanner primitives ALL shipped and were Codex-hardened in Phase 133 — reuse them, don't reinvent.
- Concrete provenance-leak vectors verified in the source DB: raw `work_id` embeds `M:`/`J:`/`REF` prefixes AND the M-source filename; the `cat` column value is the literal M-source name; M-source titles/authors/genres are leak-sensitive → the distillation MUST mint opaque work_ids, map to a masked `source_corpus`, and fail-closed on unreviewed M-source titles.
- The claim model maps cleanly onto verified tables: `accepted_pairs_canonmask` (1.33M MS-MS page pairs, `flank_class`→claim_type, `a0/a1/b0/b1` offsets into OUR text) + `track1_matches`/`work_query_hits_fullv2` (work–witness); evidence stores offsets + text-layer + snapshot hash, never text.
- Witness units (DATA-10): Oxford `oxford_part_id` (libraries.csv col 1) ∪ physical joins (`fjms_enrichment.db joins`, exclude `Scribe join`); deterministic `unit_id`.
- The masking CI gate needs a new cell-level `--scan-sqlite` mode plus R-source token pre-registration.

**Open questions for the planner to resolve with the researcher (Wave 0):** the authoritative E1 band-source file (OQ1), whether the launch spine needs any clustering (OQ2/A3), and the HTR-snapshot-hash granularity (OQ3). None block planning; all should be confirmed before the schema doc is frozen.

**Ready for planning.**
