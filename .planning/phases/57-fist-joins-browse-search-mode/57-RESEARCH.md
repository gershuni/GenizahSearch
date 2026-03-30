# Phase 57: FIST Joins Browse & Search Mode - Research

**Researched:** 2026-03-29
**Domain:** SQLite data import, on-demand service layer, browse enrichment dialogs, search restriction
**Confidence:** HIGH

## Summary

Phase 57 adds a "Visual Similarity" feature to the browse experience, powered by the `Image_BestMarkForJoin` table in FIST.db (35.9M rows of SVM-scored visual similarity pairs from FJMS's image analysis pipeline). This is distinct from existing scholarly joins (48K rows already in the app). The implementation follows established patterns: a shared service layer (like FjmsService), a browse enrichment dialog (like Measurements), and search restriction (Phase 55's `restrict_sys_ids`).

The primary technical challenge is the **import pipeline**: mapping FIST.db `DocumentID` to our `AlmaId` (sys_id) through a 3-table join chain (`Image_ImageDocument` -> `dbo_ImgDigitalImage` -> `dbo_InventoryAlma`), deduplicating pairs where multiple DocumentIDs map to the same AlmaId, and producing a server-side sidecar that can serve ~100 suggestions per manuscript on demand. The full dataset after AlmaId mapping is approximately 8-15M unique pairs across ~196K manuscripts, yielding a sidecar of 500-900 MB.

**Primary recommendation:** Store data as a new `visual_suggestions` table in a separate `visual_similarity.db` sidecar on the server (not in `fjms_enrichment.db`, which is already 941 MB). Use INTEGER AlmaIds for compactness. Desktop fetches per-manuscript via a new `/api/visual_suggestions/{sys_id}` endpoint and caches results in a local SQLite file.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** Server-only storage. The full visual suggestions dataset lives on the web server only. Desktop does NOT ship a local sidecar.
- **D-02:** On-demand per-manuscript fetch. When a user browses a manuscript, the app fetches that manuscript's visual suggestions from the server and caches them to local disk.
- **D-03:** Optional full DB download. A settings option allows power users to download the entire visual suggestions database for offline use.
- **D-04:** Import all scored pairs (MarkCode=NULL and 10318, deduplicated) from `Image_BestMarkForJoin`. Map DocumentID -> FGPImageNumberId -> InventoryId -> AlmaId via the chain: `Image_ImageDocument` -> `dbo_ImgDigitalImage` -> `dbo_InventoryAlma`.
- **D-05:** Future phases may use this data for line-based join filtering. Design the storage to support this.
- **D-06:** Dedicated dialog (like Measurements/Bibliography). A "Visual Similarity" button in the browse toolbar opens a sortable dialog showing ranked suggestions with thumbnails, partner metadata (domain, library), and action buttons (Browse, Open in Puzzle).
- **D-07:** Top 20 suggestions shown by default, no score floor. Ranked by SVM score internally.
- **D-08:** No raw SVM score displayed to users. Suggestions shown as a ranked list (#1, #2, #3...) without any score indicator.
- **D-09:** Dialog includes sorting and filtering controls (sort by rank is default; filter by library, domain).
- **D-10:** Cross-cutting "Search in visual suggestions" action from multiple contexts: Browse, ResultDialog, Advanced View, List items, Search results.
- **D-11:** Two combination modes: Union (combine all suggestion partners) or Intersection (only partners suggested for ALL selected manuscripts).
- **D-12:** Two entry modes: Browse suggestions (directly show pool) and Search within suggestions (restrict text search to pool's sys_ids via Phase 55 mechanism).
- **D-13:** Label as "Visual Similarity" throughout the UI.
- **D-14:** Clear visual distinction from scholarly joins.

### Claude's Discretion
- Fetch mechanism for desktop (HTTP API endpoint vs SQLite download)
- Local disk cache format and eviction policy
- Exact dialog layout and component choices
- Button placement in browse toolbar
- How "Search in visual suggestions" is triggered from each context
- Server-side DB format (separate SQLite sidecar vs table in fjms_enrichment.db)
- Import script design

### Deferred Ideas (OUT OF SCOPE)
- Line-based join search that cross-references with visual similarity data (future phase)
- Thumbnail preview comparison view in suggestions dialog
- Visual similarity score calibration or quality tiers
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| JOIN-01 | User can see FIST join group suggestions in browse enrichment alongside existing FJMS scientific joins | Visual Similarity dialog in browse toolbar (D-06), data served from visual_similarity.db sidecar, enrichment pattern from measurements_dialog.py |
| JOIN-02 | User can search within FIST join groups as a dedicated search mode | "Search in Visual Suggestions" action (D-10, D-12), reuses Phase 55 restrict_sys_ids mechanism from shared/refinement.py |
| JOIN-03 | Search results show join partners for matched fragments with visual distinction | Per UI-SPEC: implemented as cross-cutting "Search in Visual Suggestions" action, NOT automatic search result enrichment |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| sqlite3 | stdlib | Visual similarity sidecar DB | Same as all other sidecars (fjms_enrichment.db, pgp.db, nli_crossref.db) |
| shared/thread_local_db.py | project | ThreadLocalConnection for thread-safe access | Required for NiceGUI run.io_bound() concurrent calls |
| NiceGUI | existing | Web UI dialogs, buttons, tables | Project standard |
| PyQt6 | existing | Desktop UI dialogs, tables | Project standard |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| shared/refinement.py | project | restrict_sys_ids, compute_effective_restrict | D-12: restricting search to visual suggestion pool |
| shared/fjms_service.py | project | Domain enrichment for suggestion partners | D-06: showing domain info in dialog |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Separate visual_similarity.db | Table in fjms_enrichment.db | fjms_enrichment.db is already 941 MB; adding 500+ MB would double it. Separate sidecar is cleaner for D-01 (server-only) and D-03 (optional download). |
| SQLite for desktop cache | JSON files | SQLite allows indexed queries and is consistent with project patterns; JSON files would be simpler but harder to query for union/intersection |
| TEXT AlmaIds | INTEGER AlmaIds | INTEGER saves ~30% space (45 vs 65 bytes/row). Since AlmaIds are large integers (e.g., 990001746800205171), INTEGER is correct and more compact. Service methods convert to/from string at the boundary. |

## Architecture Patterns

### Recommended Project Structure
```
shared/
    visual_similarity_service.py  # New service (pattern: FjmsService)
    thread_local_db.py            # Existing ThreadLocalConnection
web/
    components/
        visual_similarity_dialog.py  # New dialog (pattern: measurements_dialog.py)
    api.py                           # New endpoints: /api/visual_suggestions/{sys_id}
    pages/
        browse.py                    # Add Visual Similarity chip button
        search.py                    # Add "Search in visual suggestions" action
scripts/
    import_visual_similarity.py      # One-time import script (pattern: import_measurements.py)
fist_data/
    visual_similarity.db             # Server-only sidecar (NOT shipped with desktop)
tests/
    test_visual_similarity_service.py  # Service unit tests
    test_visual_similarity_import.py   # Import script tests
```

### Pattern 1: Shared Service Layer
**What:** `VisualSimilarityService` class with thread-safe SQLite, singleton accessor
**When to use:** All visual similarity data access from web and desktop
**Example:**
```python
# Pattern from shared/fjms_service.py
class VisualSimilarityService:
    def __init__(self, db_path=None, thread_safe=True):
        self._conn = None  # ThreadLocalConnection or sqlite3.Connection
        if db_path and os.path.exists(db_path):
            uri = f"file:{db_path}?mode=ro"
            if thread_safe:
                self._conn = ThreadLocalConnection(uri, row_factory=sqlite3.Row)
            else:
                self._conn = sqlite3.connect(uri, uri=True)
                self._conn.row_factory = sqlite3.Row

    def get_suggestions(self, sys_id: str, limit: int = 200) -> list[dict]:
        """Get visual similarity suggestions for a manuscript, ranked by SVM score."""
        # Returns: [{alma_id, rank, domain, library_code}, ...]

    def has_suggestions(self, sys_id: str) -> bool:
        """Quick check for browse chip visibility."""

    def get_suggestion_partners(self, sys_ids: list[str], mode: str = 'union') -> set[str]:
        """Get union or intersection of suggestion partners for multiple manuscripts."""
```

### Pattern 2: Browse Enrichment Dialog
**What:** Dedicated dialog opened from browse toolbar chip, async data fetch
**When to use:** Displaying visual similarity suggestions for the current manuscript
**Example:**
```python
# Pattern from web/components/measurements_dialog.py
async def show_visual_similarity_dialog(sys_id, shelfmark, vs_service=None):
    if vs_service is None:
        from shared.visual_similarity_service import get_vs_service
        vs_service = get_vs_service(thread_safe=True)
    data = await run.io_bound(vs_service.get_suggestions, sys_id)
    # ... build dialog with ui.dialog, ui.table, sort/filter controls
```

### Pattern 3: Server API Endpoint for Desktop
**What:** FastAPI endpoint on NiceGUI's app for desktop to fetch per-manuscript suggestions
**When to use:** Desktop on-demand fetch (D-02)
**Example:**
```python
# Pattern from web/api.py (existing endpoints like /api/fl_ids/{sys_id})
@app.get('/api/visual_suggestions/{sys_id}')
def visual_suggestions(sys_id: str, limit: int = 200):
    """Return ranked visual similarity suggestions for a manuscript."""
    svc = get_vs_service(thread_safe=True)
    return svc.get_suggestions(sys_id, limit=limit)
```

### Pattern 4: Desktop Local Cache
**What:** SQLite cache file storing previously fetched suggestions
**When to use:** Desktop app caches server responses for offline access
**Example:**
```python
# In desktop, VisualSimilarityService checks local cache first, then fetches from server
class DesktopVSCache:
    def __init__(self, cache_dir):
        self._db_path = os.path.join(cache_dir, 'vs_cache.db')
        # CREATE TABLE IF NOT EXISTS cached_suggestions (...)

    def get_cached(self, sys_id: str) -> list[dict] | None:
        """Return cached suggestions or None if not cached."""

    def store(self, sys_id: str, suggestions: list[dict]):
        """Cache server response locally."""
```

### Pattern 5: Search Restriction Integration
**What:** Gathering visual suggestion partner sys_ids and feeding into restrict_sys_ids
**When to use:** D-12 "Search within suggestions" mode
**Example:**
```python
# Gather partner IDs from visual suggestions
partner_ids = vs_service.get_suggestion_partners(
    selected_sys_ids, mode='union'  # or 'intersection'
)
# Feed into existing refinement mechanism
effective = compute_effective_restrict(filter_restrict, partner_ids)
results = searcher.execute_search(query, mode, gap, restrict_sys_ids=effective)
```

### Anti-Patterns to Avoid
- **Storing in fjms_enrichment.db:** Already 941 MB. Adding 500+ MB visual similarity data would make it unwieldy and violate D-01 (server-only, desktop should not ship this).
- **Loading all suggestions into memory:** With 15M+ pairs, memory usage would be extreme. Use indexed SQLite queries.
- **Fetching all partners eagerly on browse:** Only fetch when user clicks the Visual Similarity button (lazy/on-demand).
- **Showing SVM scores to users:** D-08 explicitly prohibits this. Use rank numbers only.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Thread-safe SQLite | Custom locking | ThreadLocalConnection | Per-thread connections, proven pattern in project |
| Search restriction | Custom result filtering | shared/refinement.py restrict_sys_ids | Already handles None vs empty-set semantics, tested |
| Dialog layout | Custom HTML/CSS | Measurements dialog pattern | Consistent UX, dark mode, RTL support baked in |
| Shelfmark display | Raw sys_id | csv_bank / meta_mgr | Maps sys_id to human-readable shelfmark |

**Key insight:** This phase reuses 4 established patterns (service layer, enrichment dialog, API endpoint, search restriction). The novelty is the import pipeline and the union/intersection logic for multi-manuscript selection.

## Common Pitfalls

### Pitfall 1: AlmaId Type Mismatch
**What goes wrong:** FIST.db stores AlmaId as INTEGER, but the rest of the codebase uses AlmaId as TEXT (string sys_id). Queries fail silently or return no results.
**Why it happens:** Phase 53 discovered this: "FIST AlmaId is integer, CSV system_number is string -- CAST(AlmaId AS TEXT) required for matching."
**How to avoid:** Store AlmaId as INTEGER in the sidecar for compactness, but CAST to TEXT at service boundary. All service methods accept string sys_id and convert internally.
**Warning signs:** `has_suggestions()` returns False for manuscripts that should have data; empty dialog despite data existing.

### Pitfall 2: Duplicate Pairs from Multiple DocumentIDs per AlmaId
**What goes wrong:** Same (AlmaId_A, AlmaId_B) pair appears multiple times with different scores because each AlmaId maps to 1.16 DocumentIDs on average (10,052 AlmaIds have >1 DocumentID).
**Why it happens:** Different images of the same manuscript get separate visual similarity scores.
**How to avoid:** During import, GROUP BY (alma_id_a, alma_id_b) and take MAX(svm_score). This gives the "best case" similarity.
**Warning signs:** More rows than expected in sidecar; duplicate entries in dialog.

### Pitfall 3: Self-Pairs in Suggestions
**What goes wrong:** A manuscript appears as its own visual similarity suggestion (#1 ranked).
**Why it happens:** Same AlmaId reached via different DocumentIDs (recto vs verso images may share AlmaId).
**How to avoid:** Exclude pairs where alma_id_a = alma_id_b during import.
**Warning signs:** First suggestion in dialog is the same manuscript being viewed.

### Pitfall 4: Large Sidecar Size for Full Download (D-03)
**What goes wrong:** Full download option offers a 500-900 MB file, users abandon mid-download.
**Why it happens:** 15M+ pairs with 18-digit INTEGER IDs and REAL scores.
**How to avoid:** Compress the sidecar (SQLite + gzip). A 600 MB SQLite file compresses to ~150-200 MB. Show download progress with percentage and estimated size upfront.
**Warning signs:** Users complaining about download size or duration.

### Pitfall 5: Stale Enrichment References in Browse
**What goes wrong:** Visual Similarity button references the wrong manuscript after navigation.
**Why it happens:** Established pattern issue: enrichment_refs must be cleared on manuscript change.
**How to avoid:** Follow existing pattern: clear enrichment_refs in browse navigation handlers (already done for PGP, version selector, measurements).
**Warning signs:** Dialog shows suggestions for previously viewed manuscript.

### Pitfall 6: Union/Intersection Performance
**What goes wrong:** Intersection mode on 5+ manuscripts becomes slow (multiple DB queries + set intersection).
**Why it happens:** Each manuscript query returns ~100-200 IDs, intersection requires querying all.
**How to avoid:** For union, batch queries with IN clause. For intersection, query each manuscript's partners then Python set intersection (fast for sets of ~100-200 elements).
**Warning signs:** Multi-second delay when computing intersection of many manuscripts.

## Code Examples

### Import Script Core Logic
```python
# Pattern: scripts/import_measurements.py
# Source: FIST.db verified 2026-03-29

def import_visual_similarity(fist_db_path, output_db_path):
    """Import visual similarity pairs from FIST.db into visual_similarity.db sidecar."""
    src = sqlite3.connect(fist_db_path)
    dst = sqlite3.connect(output_db_path)

    dst.execute('''CREATE TABLE IF NOT EXISTS visual_suggestions (
        alma_id_a INTEGER NOT NULL,
        alma_id_b INTEGER NOT NULL,
        svm_score REAL NOT NULL,
        PRIMARY KEY (alma_id_a, alma_id_b)
    )''')
    dst.execute('CREATE INDEX IF NOT EXISTS idx_vs_a ON visual_suggestions(alma_id_a)')
    dst.execute('CREATE INDEX IF NOT EXISTS idx_vs_b ON visual_suggestions(alma_id_b)')

    # Build DocumentID -> AlmaId mapping
    doc_to_alma = {}
    for row in src.execute('''
        SELECT d.DocumentId, ia.AlmaId
        FROM Image_ImageDocument d
        JOIN dbo_ImgDigitalImage img ON img.FGPImageNumberId = d.FGPImageNumberIdRecto
        JOIN dbo_InventoryAlma ia ON ia.InventoryId = img.InventoryId
    '''):
        doc_to_alma[row[0]] = row[1]

    # Process pairs in chunks, dedup by (alma_a, alma_b), take max score
    # Filter: MarkCode IS NULL only (10318 is duplicate data)
    # Exclude self-pairs (alma_a = alma_b)
    batch = []
    for row in src.execute('''
        SELECT DocumentID_A, DocumentID_B, SVMMark
        FROM Image_BestMarkForJoin
        WHERE MarkCode IS NULL
        ORDER BY DocumentID_A, SVMMark DESC
    '''):
        alma_a = doc_to_alma.get(row[0])
        alma_b = doc_to_alma.get(row[1])
        if alma_a and alma_b and alma_a != alma_b:
            batch.append((alma_a, alma_b, row[2]))
            if len(batch) >= 100000:
                dst.executemany(
                    'INSERT OR REPLACE INTO visual_suggestions VALUES (?, ?, ?)',
                    batch
                )
                batch.clear()
    if batch:
        dst.executemany('INSERT OR REPLACE INTO visual_suggestions VALUES (?, ?, ?)', batch)

    dst.commit()
    # VACUUM for compactness
    dst.execute('VACUUM')
```

### Service Method: Get Suggestions
```python
def get_suggestions(self, sys_id: str, limit: int = 200) -> list[dict]:
    """Get visual similarity suggestions ranked by SVM score (descending)."""
    if self._conn is None:
        return []
    try:
        alma_id = int(sys_id)
    except (ValueError, TypeError):
        return []
    rows = self._conn.execute(
        'SELECT alma_id_b, svm_score FROM visual_suggestions '
        'WHERE alma_id_a = ? ORDER BY svm_score DESC LIMIT ?',
        (alma_id, limit)
    ).fetchall()
    return [{'alma_id': str(r[0]), 'svm_score': r[1], 'rank': i + 1}
            for i, r in enumerate(rows)]
```

### Web API Endpoint
```python
@app.get('/api/visual_suggestions/{sys_id}')
def visual_suggestions_api(sys_id: str, limit: int = 200):
    """Return ranked visual similarity suggestions for desktop on-demand fetch."""
    svc = get_vs_service(thread_safe=True)
    suggestions = svc.get_suggestions(sys_id, limit=limit)
    # Enrich with shelfmark, domain, library_code from csv_bank / fjms
    # ... (lightweight enrichment)
    return suggestions
```

### Browse Chip Integration
```python
# In browse.py, inside the enrichment button row (after Measurements chip)
has_vs = fjms_data.get('has_visual_suggestions', False)
if has_vs:
    from web.components.visual_similarity_dialog import show_visual_similarity_dialog
    vs_chip_style = 'border: 1.5px solid #ef6c00; border-radius: 12px; min-height: 22px; color: #ef6c00;'
    vs_count = fjms_data.get('visual_suggestion_count', 0)
    ui.button(
        f'{tr("Visual Similarity")} ({vs_count})',
        on_click=lambda s=page.sys_id, sm=page.shelfmark or '': show_visual_similarity_dialog(s, sm),
    ).props('flat dense size=sm no-caps').classes('text-xs px-2 py-0').style(vs_chip_style)
```

## Data Architecture Details

### FIST.db Source Tables (verified 2026-03-29)

| Table | Rows | Key Columns | Purpose |
|-------|------|-------------|---------|
| Image_BestMarkForJoin | 35,900,809 | BestMarkID (PK), DocumentID_A, DocumentID_B, SVMMark, MarkCode | Raw visual similarity pairs |
| Image_ImageDocument | 228K | DocumentId (PK), FGPImageNumberIdRecto, FGPImageNumberIdVerso | Maps DocumentID to FGP image IDs |
| dbo_ImgDigitalImage | 742K | FGPImageNumberId (PK), InventoryId | Maps FGP to inventory |
| dbo_InventoryAlma | 254K | InventoryId, AlmaId, SiteId | Maps inventory to AlmaId (our sys_id) |

### Join Chain
```
DocumentID (in BestMarkForJoin)
    -> Image_ImageDocument.DocumentId -> FGPImageNumberIdRecto
    -> dbo_ImgDigitalImage.FGPImageNumberId -> InventoryId
    -> dbo_InventoryAlma.InventoryId -> AlmaId (= sys_id as integer)
```

### MarkCode Distribution
| MarkCode | Count | Action |
|----------|-------|--------|
| NULL | 15,494,116 | Import (primary scored pairs) |
| 10318 | 15,494,116 | Skip (exact duplicate of NULL rows) |
| 32318 | 4,637,246 | Skip (score=0, no useful signal) |
| 33318 | 275,331 | Skip (score=0, no useful signal) |

### Data Volume Estimates
| Metric | Value |
|--------|-------|
| Unique AlmaIds with suggestions | ~196,050 |
| Average partners per manuscript | ~100 |
| Estimated unique AlmaId pairs after dedup | ~8-12M |
| Sidecar size (INTEGER keys, no compression) | ~500-700 MB |
| Sidecar size (gzipped for download) | ~150-200 MB |
| Per-manuscript JSON response | ~2-5 KB |
| Desktop cache per manuscript | ~12 KB |

### Sidecar Schema
```sql
-- visual_similarity.db (server-only sidecar)
CREATE TABLE visual_suggestions (
    alma_id_a INTEGER NOT NULL,  -- source manuscript
    alma_id_b INTEGER NOT NULL,  -- suggested partner
    svm_score REAL NOT NULL,     -- higher = more visually similar
    PRIMARY KEY (alma_id_a, alma_id_b)
);
CREATE INDEX idx_vs_a ON visual_suggestions(alma_id_a);
CREATE INDEX idx_vs_b ON visual_suggestions(alma_id_b);
-- Index on alma_id_b supports reverse lookup: "who considers me a match?"
-- Also needed for bidirectional union/intersection queries

-- Metadata table for versioning
CREATE TABLE vs_metadata (
    key TEXT PRIMARY KEY,
    value TEXT
);
-- INSERT INTO vs_metadata VALUES ('version', '1.0.0');
-- INSERT INTO vs_metadata VALUES ('import_date', '2026-03-29');
-- INSERT INTO vs_metadata VALUES ('source', 'FIST.db Image_BestMarkForJoin');
-- INSERT INTO vs_metadata VALUES ('pair_count', '...');
```

### Desktop Cache Schema
```sql
-- vs_cache.db (local desktop cache)
CREATE TABLE cached_suggestions (
    sys_id TEXT NOT NULL,
    partner_sys_id TEXT NOT NULL,
    svm_score REAL NOT NULL,
    rank INTEGER NOT NULL,
    fetched_at TEXT NOT NULL,  -- ISO timestamp
    PRIMARY KEY (sys_id, partner_sys_id)
);
CREATE INDEX idx_cache_sys ON cached_suggestions(sys_id);

-- Track which manuscripts have been fully cached
CREATE TABLE cache_manifest (
    sys_id TEXT PRIMARY KEY,
    fetched_at TEXT NOT NULL,
    partner_count INTEGER NOT NULL
);
```

## Bidirectional Pair Consideration

**Key question:** If manuscript A lists B as a suggestion, does B also list A?

The BestMarkForJoin table stores directional pairs: (DocumentID_A, DocumentID_B) means "A's image analysis found B similar." This is NOT necessarily symmetric. The import should store pairs as-is (directional). To support "who considers me a match?" queries (reverse lookup), we index on `alma_id_b` as well.

For the browse dialog, we query only `WHERE alma_id_a = ?` (what are MY suggestions). For union/intersection in "Search in visual suggestions," we also query only forward direction. Reverse lookup is a potential future enhancement.

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| No visual similarity | FIST BestMarkForJoin | This phase | Researchers can discover algorithmically suggested fragment pairs |
| Scholarly joins only (48K) | Scholarly + visual suggestions (15.5M) | This phase | 300x more discovery candidates |

## Open Questions

1. **Import duration estimate**
   - What we know: 15.5M rows to process through a 3-table join. DocumentID->AlmaId mapping is ~228K entries (fits in memory as dict).
   - What's unclear: Whether processing 15.5M rows with Python dict lookup takes minutes or hours.
   - Recommendation: Build the doc_to_alma dict first (fast), then stream through BestMarkForJoin in chunks. Expect 5-15 minutes on a modern machine.

2. **Bidirectional storage vs query-time flip**
   - What we know: Pairs are directional. If A->B exists with score X, B->A may exist with different score or not at all.
   - What's unclear: Whether users expect symmetric results ("if I'm similar to you, you're similar to me").
   - Recommendation: Store as-is (directional). For D-05 future extensibility, the bidirectional index supports reverse queries if needed later.

3. **Download size for D-03 full DB download**
   - What we know: Uncompressed sidecar will be 500-700 MB. Gzipped ~150-200 MB.
   - What's unclear: Whether server upload bandwidth supports frequent full downloads.
   - Recommendation: Implement with gzip transfer encoding. Show size estimate before download. Consider providing the download as a background operation with resume support.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest 7.x |
| Config file | tests/ directory (auto-discovered) |
| Quick run command | `python -m pytest tests/test_visual_similarity_service.py -x -q` |
| Full suite command | `python -m pytest tests/ -x -q` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| JOIN-01 | get_suggestions returns ranked list for manuscript with data | unit | `pytest tests/test_visual_similarity_service.py::test_get_suggestions -x` | Wave 0 |
| JOIN-01 | has_suggestions returns True/False correctly | unit | `pytest tests/test_visual_similarity_service.py::test_has_suggestions -x` | Wave 0 |
| JOIN-01 | Empty result for manuscript without suggestions | unit | `pytest tests/test_visual_similarity_service.py::test_no_suggestions -x` | Wave 0 |
| JOIN-02 | get_suggestion_partners union mode | unit | `pytest tests/test_visual_similarity_service.py::test_union_mode -x` | Wave 0 |
| JOIN-02 | get_suggestion_partners intersection mode | unit | `pytest tests/test_visual_similarity_service.py::test_intersection_mode -x` | Wave 0 |
| JOIN-02 | restrict_sys_ids integration with suggestion partners | unit | `pytest tests/test_visual_similarity_service.py::test_restrict_integration -x` | Wave 0 |
| JOIN-03 | API endpoint returns JSON suggestions | unit | `pytest tests/test_visual_similarity_service.py::test_api_endpoint -x` | Wave 0 |
| JOIN-01 | Import script produces correct sidecar from FIST.db | integration | `pytest tests/test_visual_similarity_import.py -x` | Wave 0 |

### Sampling Rate
- **Per task commit:** `python -m pytest tests/test_visual_similarity_service.py -x -q`
- **Per wave merge:** `python -m pytest tests/ -x -q`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `tests/test_visual_similarity_service.py` -- covers JOIN-01, JOIN-02, JOIN-03
- [ ] `tests/test_visual_similarity_import.py` -- covers import pipeline
- [ ] No framework install needed (pytest already available, 1062 tests collected)

## Sources

### Primary (HIGH confidence)
- FIST.db direct inspection (2026-03-29): table schemas, row counts, join chain verification, MarkCode distribution, score ranges
- Project codebase: shared/fjms_service.py (service pattern), web/components/measurements_dialog.py (dialog pattern), shared/refinement.py (restrict_sys_ids), web/api.py (endpoint pattern)
- 57-CONTEXT.md: user decisions D-01 through D-14
- 57-UI-SPEC.md: dialog layout, colors, copywriting, interaction contracts

### Secondary (MEDIUM confidence)
- Size estimates: based on SQLite per-row measurement with representative data (65 bytes/row TEXT, 45 bytes/row INTEGER)
- Import duration: estimated from data volume, not measured

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - all libraries are existing project dependencies
- Architecture: HIGH - all 5 patterns are established in the codebase with working examples
- Data pipeline: HIGH - join chain verified against live FIST.db data
- Pitfalls: HIGH - AlmaId type mismatch, dedup issues verified from data analysis
- Size estimates: MEDIUM - based on sample measurements, actual may vary 20%

**Research date:** 2026-03-29
**Valid until:** 2026-04-28 (stable - FIST.db data is static export)
