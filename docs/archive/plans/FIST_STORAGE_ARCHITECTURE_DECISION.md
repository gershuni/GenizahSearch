# FIST Data Storage Architecture Decision

> **Status:** Completed — SQLite sidecar approach selected and shipped
> **Date:** 2026-02-12 (updated 2026-03-13)
> **Context:** Deciding how GenizahSearch should store and access FIST enrichment data
> **Stakeholders:** GenizahSearch developer, NLI data team

---

## 1. About GenizahSearch

### What It Is

GenizahSearch is a research platform for the Cairo Genizah — a collection of ~400,000 manuscript fragments (10th-19th century) discovered in the Ben Ezra Synagogue in Cairo, now scattered across 70+ libraries worldwide. The platform helps scholars find and study these manuscripts by combining:

- **Manuscript image browsing** — view high-resolution images from Cambridge, Manchester, JTS, British Library, and other institutions via IIIF
- **OCR transcription search** — search across ~217,000 auto-transcribed manuscript pages (MiDRASH V0.8/V0.7 OCR)
- **Scholarly data from Princeton Geniza Project (PGP)** — 35,839 document records with human-curated transcriptions, translations, metadata, and tags
- **Responsa-style advanced search** — grammatical prefix/suffix expansion, Judeo-Arabic forms, wildcards, gap notation, plene/defective alternation
- **Community corrections** — users can submit corrections to OCR transcriptions with an approval workflow

### Who Uses It

Genizah researchers, academic scholars, and students working with medieval Jewish manuscripts. Primarily Hebrew/Judeo-Arabic text, right-to-left.

**URL:** https://genizahsearch.com (web app)

### Two Applications

GenizahSearch ships as **two apps** that must stay in sync:

| App | Technology | Deployment | Users |
|-----|-----------|------------|-------|
| **Web app** | NiceGUI (Python) | Cloud server, port 8080 | Primary, most users |
| **Desktop app** | PyQt6 (Python) | Windows executable | Power users, offline work |

Both apps share core logic via `genizah_core.py` (~8,200 lines) and access the same data sources. Any new feature must work in both.

### Current Data Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    GenizahSearch Apps                         │
│                                                              │
│   Web (NiceGUI)              Desktop (PyQt6)                │
│       │                          │                           │
│       └──────────┬───────────────┘                           │
│                  │                                           │
│          genizah_core.py (shared search engine)              │
│                  │                                           │
│    ┌─────────────┼─────────────────┐                        │
│    │             │                 │                          │
│    ▼             ▼                 ▼                          │
│                                                              │
│  libraries.csv   Tantivy Index    Supabase (PostgreSQL)     │
│  (217K records)  (290K pages)     (cloud)                    │
│                                                              │
│  Loaded at       Local full-text   - 35,839 PGP documents   │
│  startup into    search index.     - 9,364 transcriptions    │
│  Python dict     OCR pages         - 36,155 fragment links   │
│  (~48 MB RAM)    indexed.          - 22,757 footnotes        │
│                                    - User auth, corrections  │
│  Access: O(1)    Access: ~10ms     Access: ~50-200ms         │
│  dict lookup     per search        (network round-trip)      │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

**How search works (two-phase):**
1. **Tantivy** retrieves candidate pages matching the query (fast, broad)
2. **Regex** filters and highlights exact matches (precise, pattern-based)
3. Results enriched with metadata from `libraries.csv` dict and Supabase PGP data

**How browse works:**
- User navigates to a manuscript by shelfmark
- Metadata loaded from `libraries.csv` (instant) + Supabase PGP data (network call)
- Images fetched from library IIIF endpoints (Cambridge, NLI, etc.)

### Key Data File: libraries.csv

The backbone of GenizahSearch. Contains one row per manuscript with:

| Column | Description | Example |
|--------|-------------|---------|
| `system_number` | NLI Alma ID (unique key) | `990051334280205171` |
| `call_numbers` | Pipe-separated shelfmark variants | `T-S 12.147 \| Ms. T-S 12.147` |
| `library_code` | Holding institution | `CUL`, `JTS`, `Oxford` |
| `titles_non_placeholder` | Hebrew title | Hebrew text |

Loaded at startup into `MetadataManager.csv_bank` — a Python dict keyed by `system_number`. This dict is referenced throughout the codebase (search results, browse pages, display formatting). Deeply integrated, would be costly to refactor.

### Technology Stack

| Layer | Technology |
|-------|-----------|
| Web UI | NiceGUI (Python wrapper around Vue.js/Quasar) |
| Desktop UI | PyQt6 |
| Search engine | Tantivy (Rust-based, via tantivy-py) |
| Cloud database | Supabase (PostgreSQL + auth + REST API) |
| Local metadata | CSV → Python dict |
| Language | Python 3.10+ |
| Hosting | Cloud server (web), local Windows (desktop) |

---

## 2. What We're Integrating

The FIST database (Friedberg Image and Study Tool) from NLI provides scholarly metadata for ~253,000 manuscripts. We want to integrate three data types:

| Data Type | Records | Size (est.) | Use Case |
|-----------|---------|-------------|----------|
| **Domain classifications** | 386,711 (for 203K manuscripts) | ~15 MB | Subject filtering: Piyyut, Bible, Letters, Talmud, etc. |
| **Scientific joins** | 14,926 join groups (35,254 memberships) | ~3 MB | Fragment relationships with scholar attribution |
| **Catalog records** | 411,022 | ~30 MB | Hebrew titles, authors, dates, descriptions |

**Join key:** `libraries.csv.system_number` = `FIST.AlmaId` (18-digit NLI Alma IDs). Direct 1:1 mapping.

**Access pattern:** Read-only reference data. Updated infrequently (when NLI provides new FIST exports).

### About the FIST Database

FIST (Friedberg Image and Study Tool) is an internal NLI database originally running on MS SQL Server. We have a local backup converted to SQLite (`FIST_DB_BACKUP/FIST.db`, ~13 GB). It contains 176 tables, but we only need data from a few: domain classifications, scientific joins, and catalog records.

The FIST database uses the same `AlmaId` (NLI Alma system ID) as GenizahSearch's `libraries.csv.system_number`, so the join is direct — no shelfmark normalization needed.

**Coverage:** 253,316 AlmaIds in FIST, of which 214,586 overlap with GenizahSearch's 216,907 libraries.csv records (~99% coverage). 2,321 GenizahSearch records have no FIST match.

### How FIST Data Would Be Used

1. **Domain filtering in search:** User searches for "כתובה" and filters to "Letters" domain → only results from manuscripts classified as Letters are shown
2. **Domain display in browse:** User views a manuscript → sees "Piyyut, Liturgy" classification badges
3. **Join groups in browse:** User views a fragment → sees "Part of join group with T-S 12.148 (identified by Goitein, Physical Join)" with links to related fragments
4. **Catalog enrichment:** User views a manuscript → sees FIST catalog title, author, date, description alongside existing PGP metadata

---

## 3. Storage Options Analyzed

### Option A: Export to CSVs, Load into In-Memory Dict at Startup

**How it works:** Run export scripts against FIST.db → produce 3 CSV files → load into Python dicts at app startup, same pattern as libraries.csv.

```python
# Startup loading (same as MetadataManager for libraries.csv)
class FistEnrichment:
    def __init__(self):
        self.domains_by_sysid = {}   # sys_id → ['Piyyut', 'Liturgy']
        self.catalog_by_sysid = {}   # sys_id → {title, author, date, ...}
        self.joins_by_sysid = {}     # sys_id → [{group_id, scholar, type}]
```

| Dimension | Assessment |
|-----------|------------|
| **Lookup speed** | O(1) dict lookup — fastest possible |
| **Memory** | ~50 MB additional RAM at startup (domains 15 + catalog 30 + joins 3) |
| **Startup time** | +2-5 seconds for CSV parsing |
| **Query flexibility** | Forward lookup only (sys_id → data). Reverse lookup (domain → sys_ids) requires building reverse index or iterating all entries |
| **Dependencies** | None (built-in CSV module) |
| **Update process** | Re-run export scripts, replace CSV files |
| **Both apps** | Both load same CSV files — simple |
| **Offline** | Fully offline |
| **Complexity** | Low — proven pattern already used for libraries.csv |

**Weakness for domain filtering:** To answer "which manuscripts are in domain Piyyut?", you'd need either:
- A reverse index (`domain → set(sys_ids)`) built at startup, or
- Iterating 203K entries to filter — slow for UI responsiveness

### Option B: Small Purpose-Built SQLite Sidecar Database

**How it works:** Export FIST data into a small `fist_enrichment.db` (~50 MB) with three indexed tables. Query at runtime using Python's built-in `sqlite3` module.

```python
# Runtime query
conn = sqlite3.connect('fist_enrichment.db')
domains = conn.execute(
    "SELECT Domain, DomainHeb FROM domains WHERE AlmaId = ?", (sys_id,)
).fetchall()

# Reverse lookup (domain filtering) — natural
piyyut_ids = conn.execute(
    "SELECT DISTINCT AlmaId FROM domains WHERE Domain = 'Piyyut'"
).fetchall()
```

**Schema:**
```sql
CREATE TABLE domains (
    AlmaId TEXT NOT NULL,
    Domain TEXT NOT NULL,
    DomainHeb TEXT,
    ParentDomain TEXT
);
CREATE INDEX idx_domains_alma ON domains(AlmaId);
CREATE INDEX idx_domains_domain ON domains(Domain);

CREATE TABLE joins (
    AlmaId TEXT NOT NULL,
    JoinGroupId INTEGER NOT NULL,
    ScholarName TEXT,
    Comment TEXT,
    JoinType TEXT
);
CREATE INDEX idx_joins_alma ON joins(AlmaId);
CREATE INDEX idx_joins_group ON joins(JoinGroupId);

CREATE TABLE catalog (
    AlmaId TEXT PRIMARY KEY,
    Title TEXT,
    TitleHeb TEXT,
    AuthorText TEXT,
    CopyDate TEXT,
    CopyPlace TEXT,
    Description TEXT
);
```

| Dimension | Assessment |
|-----------|------------|
| **Lookup speed** | ~0.1ms per indexed query — imperceptible to users |
| **Memory** | Near zero (data stays on disk, SQLite caches hot pages) |
| **Startup time** | None (connect on first use) |
| **Query flexibility** | Full SQL — reverse lookups, GROUP BY, JOINs, aggregations |
| **Dependencies** | Python built-in `sqlite3` — no extra packages |
| **Update process** | Regenerate the .db file from FIST.db with one script |
| **Both apps** | Both open same .db file — simple |
| **Offline** | Fully offline |
| **Complexity** | Medium — need connection management, but standard Python patterns |

**Strengths for this use case:**
- Domain filtering is a natural SQL query (indexed, fast)
- Join group queries (find all members of a group) are relational by nature
- No memory overhead for 50MB of reference data
- Can evolve queries without code changes to data structures

### Option C: Import into Supabase (Cloud PostgreSQL)

**How it works:** Create new Supabase tables for FIST data. Both apps query via their existing Supabase clients.

```sql
-- New Supabase tables
CREATE TABLE fist_domains (
    alma_id TEXT NOT NULL,
    domain TEXT NOT NULL,
    domain_heb TEXT,
    parent_domain TEXT
);

CREATE TABLE fist_joins (
    alma_id TEXT NOT NULL,
    join_group_id INTEGER NOT NULL,
    scholar_name TEXT,
    join_type TEXT
);

CREATE TABLE fist_catalog (
    alma_id TEXT PRIMARY KEY,
    title TEXT,
    title_heb TEXT,
    author TEXT,
    copy_date TEXT,
    description TEXT
);
```

| Dimension | Assessment |
|-----------|------------|
| **Lookup speed** | ~50-200ms per query (network round-trip) |
| **Memory** | None locally |
| **Startup time** | None |
| **Query flexibility** | Full SQL + can JOIN with existing PGP tables |
| **Dependencies** | Supabase connection (already exists) |
| **Update process** | SQL migration scripts |
| **Both apps** | Both already have Supabase clients |
| **Offline** | Not available offline |
| **Complexity** | Medium — migration scripts, RLS policies, API calls |

**Weakness:** Every enrichment lookup requires a network round-trip. For search results (showing domains for 50+ results), this means either:
- N+1 queries (slow), or
- Batch query with IN clause (better but still network-bound), or
- Pre-fetching (complex)

### Option D: Add to Tantivy Index

**How it works:** Add FIST domain/catalog fields to each Tantivy document. Domain filtering happens at the search engine level.

| Dimension | Assessment |
|-----------|------------|
| **Lookup speed** | Integrated with search — fastest for filtering |
| **Memory** | Increases index size |
| **Query flexibility** | Only for search filtering, not for browse/display |
| **Update process** | Full index rebuild required |
| **Complexity** | Medium — schema migration, index rebuild pipeline |

**Critical limitation:** Tantivy indexes pages, FIST data is per-manuscript. The granularity mismatch means:
- Every page of a manuscript gets the same domain tags (redundant)
- Only useful for search filtering, not for browse page enrichment
- Still need another option (A, B, or C) for display

**Verdict:** Could be a future optimization layer, but cannot be the primary storage.

### Option E: Extend libraries.csv

**How it works:** Add columns to libraries.csv for domains and catalog info.

| Dimension | Assessment |
|-----------|------------|
| **Lookup speed** | O(1) (already in csv_bank dict) |
| **Memory** | Increases existing dict size |
| **Complexity** | **High risk** — domains are multi-valued (one manuscript → multiple domains), doesn't fit CSV columns cleanly. Would need denormalized format (e.g., pipe-separated domains). |

**Verdict:** Poor fit for multi-valued data like domains. libraries.csv is already complex enough.

### Option F: Hybrid (SQLite for domains/joins, dict for catalog)

**How it works:** Use SQLite for relational data (domains, joins) and in-memory dict for simple key→value data (catalog).

| Dimension | Assessment |
|-----------|------------|
| **Complexity** | Higher — two different access patterns |
| **Benefit** | Marginal — SQLite handles all three well enough |

**Verdict:** Added complexity for minimal benefit. Pick one approach.

---

## 4. Comparison Matrix

| Criteria | A: CSV+Dict | B: SQLite | C: Supabase | D: Tantivy | E: Extend CSV |
|----------|-------------|-----------|-------------|------------|---------------|
| Forward lookup (sys_id → data) | O(1) | ~0.1ms | ~100ms | N/A | O(1) |
| Reverse lookup (domain → sys_ids) | Awkward | Natural | Natural | Natural | Awkward |
| Memory overhead | ~50 MB | ~0 | 0 | Index grows | Increases existing |
| Offline support | Yes | Yes | No | Yes | Yes |
| Both apps | Yes | Yes | Yes | Yes | Yes |
| Update process | Replace CSVs | Regenerate .db | SQL migration | Rebuild index | Risky |
| New dependencies | None | None (built-in) | None (exists) | None (exists) | None |
| Complexity | Low | Medium | Medium | Medium | High |
| Domain filtering UX | Build reverse index | SQL WHERE clause | SQL WHERE clause | Query field | Denormalize |
| Browse enrichment | Direct | Direct | Network call | Not applicable | Direct |

---

## 5. Recommendation

### Primary: Option B — SQLite Sidecar

**`fist_enrichment.db`** (~50 MB), containing three indexed tables (domains, joins, catalog).

**Rationale:**
1. **Domain filtering is a core feature** — users will want to search within Piyyut, or filter browse results by Letters. SQLite handles this naturally with indexed queries. A dict requires building and maintaining a reverse index.
2. **Joins are relational** — "find all fragments in join group X" is a natural SQL query. In a dict, you'd need to maintain group-level data structures.
3. **No memory overhead** — 50MB stays on disk. The app already uses ~48MB for libraries.csv; doubling that is unnecessary when SQLite handles it well.
4. **Python `sqlite3` is built-in** — no new dependencies.
5. **Both apps** open the same .db file with identical code.
6. **Update story is simple** — regenerate the .db from FIST.db with one script.

**Risk:** Slightly slower individual lookups than in-memory dict (~0.1ms vs ~0.001ms). In practice, this is imperceptible — the user will never notice a 0.1ms lookup when the rest of the UI takes 50-500ms to render.

### What stays as-is

| Data | Current Storage | Change? |
|------|----------------|---------|
| libraries.csv (217K manuscripts) | In-memory dict (MetadataManager) | **No change** — too deeply integrated, high refactoring risk |
| PGP data (documents, sources, fragments) | Supabase cloud | **No change** — collaborative data, multi-user |
| Tantivy index (290K pages) | Local index files | **No change** — search engine |
| FIST enrichment (domains, joins, catalog) | *Not yet integrated* | **New: SQLite sidecar** |

### Future consideration

If the SQLite sidecar works well, migrating libraries.csv to it in a future milestone could:
- Save ~48MB of startup memory
- Provide query flexibility for libraries too
- Unify the local data access pattern

But that's a separate, larger effort with its own risks.

---

## 6. Implementation Sketch

### File structure
```
C:\GenizahSearch\
├── fist_data\
│   └── fist_enrichment.db          # ~50 MB SQLite sidecar
├── scripts\
│   └── export_fist_enrichment.py   # Generates fist_enrichment.db from FIST.db
├── shared\
│   └── fist_service.py             # Shared service for both apps
└── FIST_DB_BACKUP\
    └── FIST.db                     # Source database (13 GB, reference only)
```

### Access pattern
```python
# shared/fist_service.py
class FistService:
    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)

    def get_domains(self, sys_id: str) -> list[dict]:
        """Get domain classifications for a manuscript."""
        return self.conn.execute(
            "SELECT Domain, DomainHeb, ParentDomain FROM domains WHERE AlmaId = ?",
            (sys_id,)
        ).fetchall()

    def get_manuscripts_by_domain(self, domain: str) -> set[str]:
        """Get all sys_ids classified under a domain (for filtering)."""
        rows = self.conn.execute(
            "SELECT DISTINCT AlmaId FROM domains WHERE Domain = ? OR ParentDomain = ?",
            (domain, domain)
        ).fetchall()
        return {r[0] for r in rows}

    def get_join_group(self, sys_id: str) -> list[dict]:
        """Get join group members for a manuscript."""
        ...

    def get_catalog(self, sys_id: str) -> dict | None:
        """Get catalog enrichment (title, author, date)."""
        ...
```

### Export script
```python
# scripts/export_fist_enrichment.py
"""Generate fist_enrichment.db from FIST.db"""
import sqlite3

source = sqlite3.connect('FIST_DB_BACKUP/FIST.db')
target = sqlite3.connect('fist_data/fist_enrichment.db')

# Export domains (~386K rows)
target.execute("CREATE TABLE domains (...)")
target.executemany("INSERT INTO domains ...",
    source.execute("SELECT ... FROM FIST domain tables"))

# Export joins (~35K rows)
# Export catalog (~411K rows)
# Create indexes

target.commit()
```

---

## 7. Questions for Expert Review

1. **Is SQLite sidecar the right choice for this access pattern?** (Read-only reference data, ~450K records, forward + reverse lookups, two client apps)

2. **Should we consider a unified local data layer?** (e.g., migrating libraries.csv into the same SQLite database, or should each data source keep its own access pattern?)

3. **Memory vs. disk tradeoff:** The app already loads 48MB for libraries.csv. Is adding 50MB for FIST dicts (Option A) acceptable, or is keeping data on disk (Option B) meaningfully better?

4. **Connection management:** Should the SQLite connection be opened once at startup and kept open, or opened per-request? (Single long-lived connection is simpler but may have threading concerns in the web app.)

5. **Any architectural concerns with having three different data access patterns in one app?** (dict for libraries.csv, Supabase for PGP, SQLite for FIST)

---

*Document created: 2026-02-12*
*Purpose: Architecture decision review before milestone v5.8.0 planning*
