# Phase 2: PGP Data Import - Research

**Researched:** 2026-02-05
**Domain:** Data import, CSV parsing, Supabase batch operations
**Confidence:** HIGH

## Summary

This research analyzed the PGP data files to understand column mappings, delimiter patterns, page/folio notation, and Oxford codicological parts handling. The existing `transcriptions_linked.csv` provides 9,364 pre-matched records with 96.5% match rate, while `documents.csv` contains 35,839 documents with full metadata.

Key findings:
- The primary delimiter for multi-fragment shelfmarks is ` + ` (1,617 occurrences)
- The `side` column contains page/folio info that can be parsed per-fragment
- Oxford parts each have unique sys_ids in libraries.csv - no special handling needed
- Supabase batch inserts perform best at 500 records per batch
- The schema needs a `page_info` column added to `document_fragments`

**Primary recommendation:** Import documents first with upsert-on-pgpid, then parse multi-fragment shelfmarks to create document_fragments entries with page_info from the `side` column.

## Standard Stack

The established libraries/tools for this domain:

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| supabase-py | latest | Database operations | Project already uses it |
| csv (stdlib) | 3.11+ | CSV parsing | Standard library, UTF-8 BOM handling |
| tqdm | latest | Progress bars | Simple, well-supported progress display |
| argparse (stdlib) | 3.11+ | CLI arguments | Standard for --dry-run, --execute flags |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| re (stdlib) | 3.11+ | Regex parsing | Shelfmark/delimiter parsing |
| json (stdlib) | 3.11+ | JSONB handling | Tags array serialization |
| logging (stdlib) | 3.11+ | Structured logging | Error reporting |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| tqdm | rich | tqdm is simpler, rich adds complexity |
| csv | pandas | csv is lighter, pandas overkill for this |

**Installation:**
```bash
pip install supabase tqdm
```

## Architecture Patterns

### Recommended Project Structure
```
scripts/
    import_pgp_documents.py    # Main import script
pgp_data/
    documents.csv              # Input: PGP metadata
    transcriptions_linked.csv  # Input: matched transcriptions
    import_report.csv          # Output: detailed issue log
    import_summary.txt         # Output: console summary
```

### Pattern 1: Two-Pass Import
**What:** Import documents first, then create fragment links in a second pass
**When to use:** When foreign key relationships exist between tables
**Example:**
```python
# Pass 1: Import documents (no FK dependencies)
documents_data = parse_documents_csv(documents_path, transcriptions_path)
upsert_documents(documents_data, batch_size=500)

# Pass 2: Create fragment links (depends on documents.pgpid)
fragments_data = parse_multi_fragment_shelfmarks(documents_data)
upsert_fragments(fragments_data, batch_size=500)
```

### Pattern 2: Batch Upsert with Progress
**What:** Process records in chunks with visual progress
**When to use:** Large datasets (>1000 records)
**Example:**
```python
from tqdm import tqdm

def upsert_in_batches(table_name, records, batch_size=500, dry_run=False):
    for i in tqdm(range(0, len(records), batch_size), desc=f"Importing {table_name}"):
        batch = records[i:i + batch_size]
        if not dry_run:
            supabase.table(table_name).upsert(batch).execute()
```

### Pattern 3: Dry-Run with Report
**What:** Validate all data before committing, generate detailed report
**When to use:** Any destructive/bulk operation
**Example:**
```python
def import_documents(dry_run=True):
    issues = []
    valid_records = []

    for record in parse_records():
        validation = validate_record(record)
        if validation.errors:
            issues.append(validation)
        else:
            valid_records.append(record)

    # Write report regardless of dry_run
    write_report(issues)

    if dry_run:
        print(f"DRY RUN: Would import {len(valid_records)} records")
        print(f"Issues found: {len(issues)}")
    else:
        upsert_in_batches('documents', valid_records)
```

### Anti-Patterns to Avoid
- **Single-record inserts:** Never insert one record at a time - use batches of 500
- **Silent failures:** Always log errors to both console and CSV report
- **Hard-coded paths:** Use relative paths from script location or CLI arguments

## Don't Hand-Roll

Problems that look simple but have existing solutions:

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Progress display | Custom print statements | tqdm | Handles terminal width, ETA, speed |
| CSV parsing | Manual string splitting | csv.DictReader | Handles quoting, escapes, BOM |
| Shelfmark normalization | New normalization code | Existing normalize_shelfmark() | Already handles 40+ edge cases |
| Batch chunking | Custom slice logic | range(0, len(data), batch_size) | Standard Python idiom |

**Key insight:** The existing `pgp_transcriptions_export.py` already has comprehensive shelfmark normalization - reuse or import it rather than rewriting.

## Common Pitfalls

### Pitfall 1: UTF-8 BOM in CSV Files
**What goes wrong:** CSV files from Excel/Windows have BOM that breaks first column
**Why it happens:** Windows encodes UTF-8 with BOM marker at start
**How to avoid:** Use `encoding='utf-8-sig'` when reading CSV files
**Warning signs:** First column name has weird prefix character

### Pitfall 2: Supabase Timeout on Large Batches
**What goes wrong:** Batch insert times out or fails silently
**Why it happens:** REST API has limits, large payloads take too long
**How to avoid:** Use batch size of 500 records maximum
**Warning signs:** Hanging progress bar, partial imports

### Pitfall 3: Missing Primary Key in Upsert
**What goes wrong:** Upsert fails or creates duplicates
**Why it happens:** Supabase upsert requires primary key in data
**How to avoid:** Always include `pgpid` in documents data, use `on_conflict='pgpid'`
**Warning signs:** "Primary keys must be included" error

### Pitfall 4: Fragment Parsing Order Matters
**What goes wrong:** Fragments linked in wrong order
**Why it happens:** Set/dict operations lose insertion order
**How to avoid:** Use enumerate() to capture position, store as sequence_order
**Warning signs:** Fragment display order doesn't match shelfmark string

### Pitfall 5: Empty Transcriptions
**What goes wrong:** Import appears successful but no searchable content
**Why it happens:** Some documents have has_transcription=Y but empty footnotes
**How to avoid:** Join documents.csv with transcriptions_linked.csv on pgpid
**Warning signs:** document count != transcription count

## Data Analysis Findings

### Delimiter Patterns
| Delimiter | Count | Example |
|-----------|-------|---------|
| ` + ` | 1,617 | `T-S 13J35.3 + AIU VII.A.23` |
| ` and ` | 19 | Excavation images only (not real joins) |
| `; ` | 1 | Single exception (not a real join) |

**Recommendation:** Parse only ` + ` as the fragment delimiter. The ` and ` cases are excavation image references, not multi-fragment manuscripts.

### Page/Folio Info (side column)
The `side` column contains recto/verso info that can be split per-fragment using `;`:

| Side Value | Meaning |
|------------|---------|
| `recto` | Single fragment, recto only |
| `verso` | Single fragment, verso only |
| `recto and verso` | Single fragment, both sides |
| `recto ; verso` | Two fragments: first=recto, second=verso |
| `recto ; recto ; recto` | Three fragments, all recto |

**183 multi-fragment documents** have side values with matching fragment counts.

### Multi-Fragment Statistics
| Metric | Count |
|--------|-------|
| Total documents | 35,839 |
| With transcription | 7,302 |
| Multi-fragment (` + `) | 1,617 |
| Max fragments per doc | 5+ |

### Oxford Codicological Parts
**Key finding:** Oxford parts are already individual sys_ids in libraries.csv with format like:
- `MS heb. f.21/21` has sys_id `990053464220205171`
- `MS heb. f.7/98` has sys_id `990053461700205171`

**12,298 Oxford MS heb parts** exist as separate records. No parent-child relationship to handle.

**Matching strategy:** The existing shelfmark normalization handles Oxford format:
- PGP: `Bodl. MS heb. a 2/22` normalizes to `ms heb. a.2.22`
- GS: `MS heb. a.2/22` also normalizes to `ms heb. a.2.22`

## Column Mappings

### documents.csv to documents table
| CSV Column | DB Column | Transform |
|------------|-----------|-----------|
| pgpid | pgpid | int() |
| shelfmark | shelfmark_combined | direct |
| type | document_type | direct |
| tags | tags | split on `,` to JSONB array |
| doc_date_original | doc_date_original | direct |
| doc_date_standard | doc_date_standard | direct |
| inferred_date_display | inferred_date_display | direct |
| description | description | direct |
| (from transcriptions_linked.csv) | transcription | content column |
| (from transcriptions_linked.csv) | transcription_source | source_scholar column |

### Derived data for document_fragments
| Source | DB Column | Transform |
|--------|-----------|-----------|
| shelfmark split on ` + ` | shelfmark | each part |
| position in split | sequence_order | 1-indexed |
| transcriptions_linked.sys_id | sys_id | match on pgpid |
| side column split on ` ; ` | page_info | corresponding position |
| pgpid | document_id | FK reference |

## Schema Changes Needed

The `document_fragments` table needs a `page_info` column:

```sql
-- Add page_info column to store recto/verso/folio information
ALTER TABLE document_fragments
ADD COLUMN page_info TEXT;

COMMENT ON COLUMN document_fragments.page_info IS
'Page/folio info (recto, verso, recto and verso) for this fragment within the document';
```

**Optional:** Add `updated_at` for tracking re-imports:

```sql
-- Optional: Add updated_at for tracking
ALTER TABLE documents ADD COLUMN updated_at TIMESTAMPTZ;
ALTER TABLE document_fragments ADD COLUMN updated_at TIMESTAMPTZ;
```

## Recommended Batch Size

Based on Supabase community best practices:

| Batch Size | Performance | Notes |
|------------|-------------|-------|
| 100 | Safe but slow | Use for testing |
| **500** | Optimal | Recommended for production |
| 1000 | Risky | May timeout on slow connections |

**Recommendation:** Use 500 records per batch with tqdm progress bar.

## Script Location

Following project conventions (existing scripts in `scripts/`):

**Location:** `scripts/import_pgp_documents.py`

**Rationale:**
- Matches existing pattern (`scripts/pgp_transcriptions_export.py`)
- Keeps data processing scripts separate from web/app code
- Easy to run from command line

## Code Examples

### Shelfmark Parsing
```python
def parse_multi_fragment_shelfmark(shelfmark: str, side: str = None) -> list:
    """
    Parse a multi-fragment shelfmark into individual fragments.

    Args:
        shelfmark: Combined shelfmark like "T-S 13J35.3 + AIU VII.A.23"
        side: Optional side info like "recto ; verso"

    Returns:
        List of dicts with shelfmark, sequence_order, page_info
    """
    fragments = []
    parts = [p.strip() for p in shelfmark.split(' + ')]
    side_parts = [s.strip() for s in side.split(' ; ')] if side else []

    for i, part in enumerate(parts):
        fragment = {
            'shelfmark': part,
            'sequence_order': i + 1,
            'page_info': side_parts[i] if i < len(side_parts) else None
        }
        fragments.append(fragment)

    return fragments
```

### Supabase Upsert
```python
def upsert_documents(records: list, batch_size: int = 500, dry_run: bool = True):
    """
    Upsert documents in batches.

    Source: https://supabase.com/docs/reference/python/upsert
    """
    from supabase import create_client
    from tqdm import tqdm

    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    for i in tqdm(range(0, len(records), batch_size), desc="Documents"):
        batch = records[i:i + batch_size]
        if not dry_run:
            client.table('documents').upsert(
                batch,
                on_conflict='pgpid'  # Update if exists
            ).execute()
```

### Tags Parsing
```python
def parse_tags(tags_str: str) -> list:
    """
    Parse comma-separated tags into JSONB array.

    Example: "communal, marriage, trade" -> ["communal", "marriage", "trade"]
    """
    if not tags_str or tags_str.strip() == '':
        return []
    return [t.strip() for t in tags_str.split(',') if t.strip()]
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Individual inserts | Batch upsert 500 | Supabase 2.x | 10-100x faster |
| Service role in client | Service key env var | Security best practice | Prevents key exposure |
| Manual progress | tqdm progress bars | Industry standard | Better UX |

**Deprecated/outdated:**
- Single-record inserts: Use batch operations
- Unvalidated imports: Always use dry-run first

## Open Questions

Things that couldn't be fully resolved:

1. **Service Role Key Access**
   - What we know: Need service role key for upsert (bypasses RLS)
   - What's unclear: How to securely provide it to the script
   - Recommendation: Use SUPABASE_SERVICE_KEY environment variable

2. **Handling documents.csv without transcription**
   - What we know: 35,839 documents but only 7,302 with transcriptions
   - What's unclear: Should non-transcribed documents be imported?
   - Recommendation: Import only documents that have transcriptions (inner join)

## Sources

### Primary (HIGH confidence)
- `pgp_data/documents.csv` - Analyzed column structure, delimiter patterns
- `pgp_data/transcriptions_linked.csv` - Analyzed match data
- `migrations/add_pgp_documents_tables.sql` - Current schema
- `scripts/pgp_transcriptions_export.py` - Existing normalization logic
- [Supabase Python Upsert Docs](https://supabase.com/docs/reference/python/upsert) - Official API

### Secondary (MEDIUM confidence)
- [Supabase Batch Insert Discussion](https://github.com/orgs/supabase/discussions/11349) - Community 500-batch recommendation
- `web/supabase_client.py` - Project Supabase patterns

### Tertiary (LOW confidence)
- None

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - Using project's existing Supabase patterns
- Architecture: HIGH - Two-pass import is standard for FK dependencies
- Data analysis: HIGH - Direct analysis of CSV files
- Pitfalls: HIGH - Based on documented Supabase behavior

**Research date:** 2026-02-05
**Valid until:** 2026-03-05 (30 days - stable domain)
