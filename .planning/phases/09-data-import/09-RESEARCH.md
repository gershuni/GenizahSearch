# Phase 9: Data Import - Research

**Researched:** 2026-02-08
**Domain:** Supabase bulk data import (Python), CSV data pipeline, schema migration
**Confidence:** HIGH

## Summary

This phase imports the full PGP dataset (~36K documents, ~36K fragments, ~24K footnotes) into Supabase, expanding from the existing 7,090 documents to the complete corpus. Research focused on: (1) actual data file structure and volumes, (2) existing schema gaps, (3) the relationship between data files, (4) the proven v1 import infrastructure, and (5) Supabase capacity constraints.

Key findings: documents.csv contains 35,839 records (not 41,193 -- the file has multiline fields inflating the line count). The existing v1 import scripts (`import_pgp_documents.py`, `import_document_sources.py`) provide a solid foundation with dry-run/execute pattern, batch upsert, shelfmark normalization, and issue reporting. Fragment shelfmark matching achieves 94.0% rate (33,994/36,162) using existing `normalize_shelfmark()`. The full import (~77MB with indexes) fits comfortably within the 500MB Supabase free tier. The `footnotes.csv` is a bibliography/sources table where only ~9,745 of 24,388 records have transcription content -- the rest are scholarly references (Edition, Discussion, Translation citations without digital text).

**Primary recommendation:** Build a single comprehensive import script (replacing the two v1 scripts) that handles all four data files in a multi-pass pipeline: (1) schema migration, (2) documents upsert, (3) document_sources upsert from footnotes, (4) fragment links creation. Reuse the proven dry-run/execute pattern and batch upsert infrastructure from v1.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Import ALL 41K documents from documents.csv (actually 35,839 records) -- not just those with sys_id matches
- Import everything available: all metadata columns (type, tags, description, dates, languages, scholarship_records, shelfmarks_historic, etc.)
- Fetch and import any available transcription/translation text for the ~34K new documents -- not just metadata
- Import full fragment metadata from fragments.csv (collection, library, provenance, material) -- not just the pgpid-to-sys_id linkage
- Import footnotes.csv scholarship records into a new table
- Full upsert of all documents -- existing 7,090 get updated if PGP data changed, new ~28.7K get inserted
- Full upsert of document_sources alongside documents
- PGP data and user corrections are separate layers -- upsert freely overwrites PGP source data
- Two-pass FK-safe pattern: documents first, then fragment links and sources
- Documents without sys_id matches: import the document record, just don't create fragment links
- Success threshold: 99%+ of documents must load successfully
- Full verification report required: before/after counts, new/updated/failed records

### Claude's Discretion
- Dry-run vs direct execute approach (safety pattern)
- Batch sizes for Supabase operations
- Footnotes table schema design
- Fragment metadata storage (extend document_fragments vs new table)
- Script location and structure (reuse v1 import script or build fresh)
- Error recovery and resume-on-failure approach
- Progress reporting format

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope
</user_constraints>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| supabase-py | 2.x | Database client for upserts | Already in use for v1 imports |
| csv (stdlib) | - | CSV parsing | Standard for this project's data files |
| tqdm | any | Progress bars for batch operations | Already used in v1 scripts |
| python-dotenv | any | Environment variable loading | Already used in v1 scripts |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| argparse (stdlib) | - | CLI flags (--dry-run, --execute) | Script entry point |
| pathlib (stdlib) | - | Cross-platform file paths | All file references |

### No Additional Libraries Needed
The v1 import infrastructure already has all dependencies installed. No new packages required.

**Installation:** None needed -- all dependencies already in the project.

## Architecture Patterns

### Recommended Script Structure
```
scripts/
  import_pgp_full.py           # New comprehensive import script
  import_pgp_documents.py      # Keep as reference (v1)
  import_document_sources.py   # Keep as reference (v1)
  pgp_transcriptions_export.py # Reuse normalize_shelfmark()
migrations/
  add_full_pgp_columns.sql     # Schema additions for new columns
  create_footnotes_table.sql   # New footnotes/scholarship table
```

### Pattern 1: Multi-Pass Import Pipeline
**What:** Import data in FK-safe order with full error tracking
**When to use:** Always -- this is the core import pattern
**Passes:**
1. Documents (no FK dependencies -- pgpid is the PK)
2. Document sources from footnotes.csv (FK to documents.pgpid)
3. Fragment links via fragments.csv + shelfmark matching (FK to documents.pgpid)

### Pattern 2: Dry-Run / Execute Pattern (proven in v1)
**What:** Default to --dry-run mode that validates data and reports statistics without writing to database. --execute flag triggers actual import.
**Why:** Safety net for destructive operations. Already proven in v1.
```python
parser = argparse.ArgumentParser()
group = parser.add_mutually_exclusive_group()
group.add_argument('--dry-run', action='store_true', default=True)
group.add_argument('--execute', action='store_true')
dry_run = not args.execute
```

### Pattern 3: Batch Upsert with Progress
**What:** Split records into batches of N, upsert each with tqdm progress
**Why:** Supabase REST API has practical payload limits; batching prevents timeouts
```python
BATCH_SIZE = 500  # Proven in v1

def upsert_in_batches(client, table_name, records, on_conflict, dry_run=True):
    for i in tqdm(range(0, len(records), BATCH_SIZE), desc=f"Importing {table_name}"):
        batch = records[i:i + BATCH_SIZE]
        if not dry_run:
            client.table(table_name).upsert(batch, on_conflict=on_conflict).execute()
```

### Pattern 4: Snapshot-Compare Verification
**What:** Capture before-counts for all tables, run import, capture after-counts, compute deltas
**Why:** User requires full verification report with before/after counts

### Anti-Patterns to Avoid
- **Loading all records into memory at once for huge files:** footnotes.csv is 29MB but only 24K records -- manageable in memory. No streaming needed.
- **Single-row inserts:** Always batch. 36K individual API calls would take ~30 minutes; batches of 500 take ~2 minutes.
- **Importing without deduplication:** The `UNIQUE(document_id, sys_id)` constraint on document_fragments requires deduplication before upsert (v1 already handles this).
- **Using anon key for imports:** Must use SUPABASE_SERVICE_KEY to bypass RLS.

## Data Analysis (Critical Findings)

### Actual Data Volumes (Verified)

| File | Line Count | Actual Records | Notes |
|------|------------|----------------|-------|
| documents.csv | 41,194 | 35,839 | Multiline fields inflate line count |
| fragments.csv | 36,163 | 36,162 | 1 row per physical fragment (unique by shelfmark) |
| footnotes.csv | 420,124 | 24,388 | Multiline content fields |
| transcriptions_linked.csv | 388,241 | 9,364 | Already imported in v1 |

**Confidence:** HIGH -- verified by DictReader counting

### Document Overlap with Existing Data

| Category | Count |
|----------|-------|
| In documents.csv | 35,839 |
| Already imported (in transcriptions_linked) | 7,090 |
| New documents to insert | 28,749 |
| Documents with has_transcription=Y | 7,302 |
| Documents with has_translation=Y | 1,721 |

### Fragment Matching Rate

Using existing `normalize_shelfmark()` + FIST supplement lookup:
- **Matched:** 33,994 (94.0%)
- **Unmatched:** 2,168 (6.0%)
- Top unmatched libraries: JRL (916), ENL (260), MTA (172), CUL (133)

**Confidence:** HIGH -- tested with actual data

### Footnotes Structure (Critical)

| doc_relation | Count | Has Content | Purpose |
|--------------|-------|-------------|---------|
| Digital Edition | 7,968 | 7,945 (99.7%) | Transcriptions -- mostly already in document_sources |
| Digital Translation | 1,792 | 1,791 (99.9%) | Translations -- mostly already in document_sources |
| Discussion | 6,805 | 0 | Scholarly bibliography references |
| Edition | 6,448 | 2 | Published edition citations |
| Translation | 264 | 0 | Published translation citations |
| Edition ; Translation | 538 | 1 | Combined citations |
| Other composite | 396 | 6 | Various composite types |
| (empty) | 177 | 5 | Miscellaneous |

**Key insight:** Only ~9,745 of 24,388 footnotes have actual content (transcription/translation text). The remaining ~14,643 are purely bibliographic references (source, location, URL). Both types should go into the footnotes table.

**Overlap with existing document_sources:** 8,130 of 8,462 Digital footnotes overlap with transcriptions_linked. Only 332 Digital footnotes are NOT already imported. Of the new ~28.7K documents, only 290 have Digital content in footnotes.

### New Columns Needed in documents Table

| Column | Population Rate | Type | Notes |
|--------|----------------|------|-------|
| scholarship_records | 28.3% (10,157) | TEXT | HTML-formatted bibliography (avg 133 chars, max 976) |
| shelfmarks_historic | 55.7% (19,947) | TEXT | Historical shelfmark variants |
| language_note | 0.7% (256) | TEXT | Sparse but valuable |
| doc_date_calendar | 11.8% (4,224) | TEXT | Calendar system (Seleucid, etc.) |
| inferred_date_notes | 2.7% (956) | TEXT | Additional date reasoning |
| has_transcription | 99.9% (35,839) | BOOLEAN | Flag from PGP |
| has_translation | varies | BOOLEAN | Flag from PGP |
| input_by | 99.8% (35,751) | TEXT | PGP contributors list |

### Fragment Metadata from fragments.csv

| Column | Population | Type | Notes |
|--------|-----------|------|-------|
| collection | 100% | TEXT | Always populated |
| library | 100% | TEXT | Full library name |
| library_abbrev | 100% | TEXT | Short code (CUL, JTS, etc.) |
| url | 34.2% | TEXT | Fragment URL |
| iiif_url | 54.3% | TEXT | IIIF image URL |
| provenance | 0.1% (35) | TEXT | Very sparse |
| material_support | 0.2% (68) | TEXT | Very sparse |

### Data Size Estimates

| Table | Records | Est. Size | Notes |
|-------|---------|-----------|-------|
| documents (expanded) | 35,839 | ~22 MB | With all new columns |
| footnotes (new) | 24,388 | ~28 MB | Includes content text |
| document_fragments (link table) | ~34K+ | ~9 MB | With fragment metadata |
| Indexes overhead | - | ~18 MB | ~30% of data |
| **Total** | | **~77 MB** | Well within 500MB free tier |

**Confidence:** HIGH -- calculated from actual file data

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Shelfmark normalization | Custom regex | `pgp_transcriptions_export.normalize_shelfmark()` | Already handles 20+ edge cases, tested at 96.5% match rate |
| GS shelfmark lookup | Custom loader | `pgp_transcriptions_export.load_genizahsearch_shelfmarks()` | Loads libraries.csv + FIST supplement, produces 635K-entry lookup dict |
| Batch upsert with progress | Custom loop | Reuse v1 `upsert_in_batches()` pattern | Proven with 9K+ records |
| CSV BOM handling | Manual detection | `encoding='utf-8-sig'` + BOM key fallback | All 4 CSV files have BOM |
| Multi-fragment shelfmark parsing | New parser | Reuse v1 `parse_multi_fragment_shelfmark()` | Handles " + " delimiter and side info |

## Common Pitfalls

### Pitfall 1: Incorrect Record Count (41K vs 36K)
**What goes wrong:** documents.csv has 41,194 lines but only 35,839 records due to multiline description/scholarship_records fields
**Why it happens:** Using `wc -l` instead of CSV DictReader to count records
**How to avoid:** Always use `csv.DictReader` for counting; the script should report actual record count
**Warning signs:** If import reports 41K records, something is wrong with CSV parsing

### Pitfall 2: Upsert vs Insert for Existing Records
**What goes wrong:** Using INSERT instead of UPSERT causes constraint violations for the 7,090 existing documents
**Why it happens:** Forgetting that documents table already has data
**How to avoid:** Always use `.upsert(batch, on_conflict='pgpid')` for documents table
**Warning signs:** IntegrityError on pgpid constraint

### Pitfall 3: Foreign Key Ordering
**What goes wrong:** Inserting fragment links before their parent documents exist
**Why it happens:** Not following two-pass pattern
**How to avoid:** Pass 1 = documents (no FK deps), Pass 2+ = everything that references documents.pgpid
**Warning signs:** FK violation errors in document_fragments, document_sources, or footnotes

### Pitfall 4: Fragment-Document Deduplication
**What goes wrong:** The same (document_id, sys_id) pair appears multiple times in prepared fragment records
**Why it happens:** Multi-fragment documents parsed from documents.csv shelfmarks can produce duplicate entries when combined with fragments.csv data
**How to avoid:** Deduplicate by composite key before upsert (v1 already does this with `seen_keys` set)
**Warning signs:** Unique constraint violation on `(document_id, sys_id)`

### Pitfall 5: Service Key Not Set
**What goes wrong:** Import silently fails or gets permission denied
**Why it happens:** Using anon key instead of service_role key; RLS blocks writes
**How to avoid:** Check `SUPABASE_SERVICE_KEY` env var at startup, fail fast with clear message
**Warning signs:** "permission denied" or "new row violates row-level security" errors

### Pitfall 6: Footnotes Unique Constraint Design
**What goes wrong:** Duplicate footnote records if unique constraint is too narrow or too wide
**Why it happens:** Some documents have multiple footnotes from the same source with different doc_relations
**How to avoid:** Use `(document_id, source_slug, doc_relation)` as unique constraint -- source_slug is more reliable than source text for deduplication
**Warning signs:** Duplicate rows or constraint violations during upsert

### Pitfall 7: Hebrew/UTF-8 Encoding
**What goes wrong:** Hebrew text in content/description fields gets mangled
**Why it happens:** Not using utf-8-sig encoding, or console encoding issues on Windows
**How to avoid:** Always use `encoding='utf-8-sig'` for CSV reading; the Supabase Python client handles UTF-8 correctly
**Warning signs:** Mojibake characters in database fields

## Schema Design Recommendations

### New Columns for documents Table (Migration)

```sql
-- Migration: add_full_pgp_columns.sql
ALTER TABLE documents ADD COLUMN IF NOT EXISTS scholarship_records TEXT;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS shelfmarks_historic TEXT;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS language_note TEXT;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS doc_date_calendar TEXT;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS inferred_date_notes TEXT;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS has_transcription BOOLEAN DEFAULT FALSE;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS has_translation BOOLEAN DEFAULT FALSE;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS input_by TEXT;
```

### Footnotes Table Schema (Recommendation)

```sql
CREATE TABLE public.document_footnotes (
    id BIGSERIAL PRIMARY KEY,
    pgpid INTEGER NOT NULL REFERENCES documents(pgpid) ON DELETE CASCADE,
    source TEXT NOT NULL,              -- "Moshe Gil, Palestine During..."
    source_slug TEXT,                  -- "gil-moshe-palestine-1983" (for dedup)
    doc_relation TEXT NOT NULL,        -- "Digital Edition", "Edition", "Discussion", etc.
    location TEXT,                     -- Page/section reference (e.g., "233")
    url TEXT,                          -- External URL to source
    notes TEXT,                        -- Emendations or annotations
    content TEXT,                      -- Transcription/translation text (NULL for bibliography-only)
    content_length INTEGER,            -- Character count of content
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(pgpid, source_slug, doc_relation)
);

-- Indexes
CREATE INDEX idx_document_footnotes_pgpid ON document_footnotes(pgpid);
CREATE INDEX idx_document_footnotes_relation ON document_footnotes(pgpid, doc_relation);

-- RLS
ALTER TABLE document_footnotes ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Footnotes are publicly viewable" ON document_footnotes
    FOR SELECT TO public USING (true);
```

**Why "document_footnotes" instead of reusing document_sources:**
- document_sources stores curated Digital Edition/Translation records with language detection and sequence ordering
- footnotes.csv includes bibliographic references (Edition, Discussion, Translation) that are structurally different -- no content, just citations
- Separate tables avoid mixing curated transcription sources with bibliography
- The document_sources unique constraint `(pgpid, source_scholar, doc_relation)` doesn't match footnotes' `source_slug` field
- Overlapping Digital records can be cross-referenced but stored independently

**Alternative:** Extend document_sources to hold all footnotes. Tradeoff: simpler schema but mixes different data semantics.

### Fragment Metadata: Extend document_fragments (Recommendation)

Add fragment metadata columns directly to `document_fragments` rather than creating a separate table:

```sql
-- Add fragment metadata columns to document_fragments
ALTER TABLE document_fragments ADD COLUMN IF NOT EXISTS collection TEXT;
ALTER TABLE document_fragments ADD COLUMN IF NOT EXISTS library TEXT;
ALTER TABLE document_fragments ADD COLUMN IF NOT EXISTS library_abbrev TEXT;
ALTER TABLE document_fragments ADD COLUMN IF NOT EXISTS fragment_url TEXT;
ALTER TABLE document_fragments ADD COLUMN IF NOT EXISTS iiif_url TEXT;
```

**Why extend rather than new table:**
- document_fragments already has the shelfmark that joins to fragments.csv
- The fragment metadata is denormalized data (just like the existing `shelfmark` column)
- Avoids an extra JOIN for common queries
- Provenance (0.1%) and material_support (0.2%) are too sparse to justify a separate table -- can be added as columns if needed later

**How to populate:** During fragment link creation, look up each fragment's shelfmark in a pre-loaded fragments.csv lookup dict, and include the metadata in the upsert record.

## Code Examples

### Loading documents.csv with All Columns (Verified Pattern)

```python
def load_documents_full(documents_path: str) -> Dict[int, Dict]:
    """Load ALL columns from documents.csv."""
    documents = {}
    with open(documents_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            pgpid_str = row.get('\ufeffpgpid') or row.get('pgpid', '')
            if not pgpid_str:
                continue
            try:
                pgpid = int(pgpid_str)
            except ValueError:
                continue
            documents[pgpid] = {
                'pgpid': pgpid,
                'shelfmark_combined': row.get('shelfmark', ''),
                'document_type': row.get('type', ''),
                'tags': parse_tags(row.get('tags', '')),  # -> JSONB array
                'description': row.get('description', '') or None,
                'doc_date_original': row.get('doc_date_original', '') or None,
                'doc_date_standard': row.get('doc_date_standard', '') or None,
                'doc_date_calendar': row.get('doc_date_calendar', '') or None,
                'inferred_date_display': row.get('inferred_date_display', '') or None,
                'inferred_date_standard': row.get('inferred_date_standard', '') or None,
                'inferred_date_rationale': row.get('inferred_date_rationale', '') or None,
                'inferred_date_notes': row.get('inferred_date_notes', '') or None,
                'languages_primary': row.get('languages_primary', '') or None,
                'languages_secondary': row.get('languages_secondary', '') or None,
                'language_note': row.get('language_note', '') or None,
                'scholarship_records': row.get('scholarship_records', '') or None,
                'shelfmarks_historic': row.get('shelfmarks_historic', '') or None,
                'has_transcription': row.get('has_transcription', '') == 'Y',
                'has_translation': row.get('has_translation', '') == 'Y',
                'input_by': row.get('input_by', '') or None,
            }
    return documents
```

### Loading fragments.csv as Lookup (Verified Pattern)

```python
def load_fragment_metadata(fragments_path: str) -> Dict[str, Dict]:
    """Load fragments.csv into shelfmark -> metadata lookup."""
    fragments = {}
    with open(fragments_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            shelfmark = list(row.values())[0]  # Handle BOM in first column
            fragments[shelfmark] = {
                'pgpids': row.get('pgpids', ''),
                'collection': row.get('collection', '') or None,
                'library': row.get('library', '') or None,
                'library_abbrev': row.get('library_abbrev', '') or None,
                'url': row.get('url', '') or None,
                'iiif_url': row.get('iiif_url', '') or None,
            }
    return fragments
```

### Preparing Fragment Links with Metadata (New Pattern)

```python
def prepare_fragment_records_from_csv(
    fragment_metadata: Dict[str, Dict],
    gs_lookup: Dict[str, str]
) -> Tuple[List[Dict], List[Dict]]:
    """
    Build document_fragments records from fragments.csv.
    Each fragment row has pgpids (semicolon-separated) and metadata.
    """
    valid_records = []
    issues = []
    seen_keys = set()

    for shelfmark, meta in fragment_metadata.items():
        pgpids_str = meta['pgpids']
        normalized = normalize_shelfmark(shelfmark)
        sys_id = gs_lookup.get(normalized)

        if not sys_id:
            issues.append({
                'shelfmark': shelfmark,
                'issue_type': 'unmatched_fragment',
                'details': f'Normalized "{normalized}" not in libraries.csv'
            })
            continue

        for pgpid_str in pgpids_str.split(';'):
            pgpid_str = pgpid_str.strip()
            if not pgpid_str:
                continue
            try:
                pgpid = int(pgpid_str)
            except ValueError:
                continue

            key = (pgpid, sys_id)
            if key in seen_keys:
                continue
            seen_keys.add(key)

            valid_records.append({
                'document_id': pgpid,
                'sys_id': sys_id,
                'shelfmark': shelfmark,
                'sequence_order': 1,  # Will be computed per document
                'collection': meta.get('collection'),
                'library': meta.get('library'),
                'library_abbrev': meta.get('library_abbrev'),
                'fragment_url': meta.get('url'),
                'iiif_url': meta.get('iiif_url'),
            })

    return valid_records, issues
```

### Before/After Verification Report (New Pattern)

```python
def capture_table_counts(client) -> Dict[str, int]:
    """Capture current row counts for all PGP tables."""
    counts = {}
    for table in ['documents', 'document_fragments', 'document_sources', 'document_footnotes']:
        try:
            response = client.table(table).select('*', count='exact', head=True).execute()
            counts[table] = response.count or 0
        except Exception:
            counts[table] = 0
    return counts

def write_verification_report(before, after, issues, report_path):
    """Write before/after comparison report."""
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("=== Import Verification Report ===\n\n")
        for table in before:
            delta = after.get(table, 0) - before.get(table, 0)
            f.write(f"{table}: {before[table]} -> {after[table]} (delta: {delta:+d})\n")
        f.write(f"\nTotal issues: {len(issues)}\n")
        # ... issue details
```

## State of the Art

| Old Approach (v1) | New Approach (v2 full import) | Impact |
|-------------------|-------------------------------|--------|
| Import only docs with transcriptions (7,090) | Import ALL 35,839 docs | 5x more documents |
| Transcription text on documents.transcription column | Transcription text in document_sources table | Already migrated in v1 |
| Fragment links from shelfmark parsing in documents.csv | Fragment links from fragments.csv (authoritative source) | More accurate, includes metadata |
| No footnotes/bibliography | Full footnotes table (24,388 records) | Scholarly references now searchable |
| Manual column selection | All available metadata columns imported | No data loss |

## Open Questions

1. **Transcription content for existing 7,090 documents**
   - What we know: v1 stored transcription text in documents.transcription column AND document_sources.content
   - What's unclear: Should the upsert overwrite documents.transcription with NULL for docs that have content in document_sources?
   - Recommendation: Keep the transcription column populated for backward compatibility; the document_sources table is the authoritative source for multi-source access

2. **Side/page_info for new fragment links**
   - What we know: documents.csv has a `side` column with values like "recto", "verso", "recto ; verso"
   - What's unclear: How to assign page_info when building fragments from fragments.csv (which doesn't have side info)
   - Recommendation: Use documents.csv `side` column to set page_info when creating fragment links for documents with known side information

3. **Sequence order for multi-fragment documents**
   - What we know: documents.csv shelfmark uses " + " to separate fragments in order
   - What's unclear: Does fragments.csv preserve this ordering via pgpids field?
   - Recommendation: Parse sequence from documents.csv shelfmark order (already proven in v1)

## Sources

### Primary (HIGH confidence)
- `scripts/import_pgp_documents.py` -- v1 import script (in codebase)
- `scripts/import_document_sources.py` -- v1 sources import (in codebase)
- `scripts/pgp_transcriptions_export.py` -- shelfmark normalization (in codebase)
- `migrations/add_pgp_documents_tables.sql` -- current schema (in codebase)
- `migrations/create_document_sources_table.sql` -- sources schema (in codebase)
- All 4 CSV data files analyzed directly via Python

### Secondary (MEDIUM confidence)
- [Supabase Python upsert docs](https://supabase.com/docs/reference/python/upsert) -- API parameters
- [Supabase batch insert discussion](https://github.com/orgs/supabase/discussions/11349) -- batch size recommendations

### Tertiary (LOW confidence)
- Supabase free tier limit information from web search -- should verify in dashboard

## Metadata

**Confidence breakdown:**
- Data volumes & structure: HIGH -- verified by direct analysis of all CSV files
- Schema design: HIGH -- based on existing schema + data analysis
- Import patterns: HIGH -- based on proven v1 code
- Fragment matching rate: HIGH -- tested with actual data (94.0%)
- Supabase capacity: MEDIUM -- free tier limit from web search, should verify
- Footnotes table design: MEDIUM -- new design based on data analysis, not yet validated

**Research date:** 2026-02-08
**Valid until:** 2026-03-08 (data files are static exports; schema is stable)
