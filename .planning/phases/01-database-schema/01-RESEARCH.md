# Phase 1: Database Schema - Research

**Researched:** 2026-02-05
**Domain:** Supabase/PostgreSQL schema design for PGP document integration
**Confidence:** HIGH

## Summary

This research examines the existing GenizahSearch Supabase schema patterns, PGP data structures, and PostgreSQL/Supabase best practices to inform the database schema design for multi-fragment PGP documents.

The existing schema uses consistent patterns: SERIAL primary keys, UUID foreign keys for user references, TIMESTAMPTZ for timestamps, JSONB for flexible data (tags), and TEXT for content. Row-Level Security (RLS) policies follow a clear pattern with `authenticated` role for write operations and selective public read access.

**Primary recommendation:** Follow existing schema conventions exactly. Use `pgpid` (INTEGER) as the natural primary key for documents table, JSONB for tags array, and sys_id (TEXT) for fragment linkage.

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| PostgreSQL | 15+ | Database | Supabase default, excellent JSONB support |
| Supabase | Latest | Backend-as-a-Service | Already in use, RLS built-in |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| pg_jsonschema | 0.2+ | JSON validation | Optional - validate tags structure |
| GIN indexes | Native | JSONB search | Required for tag filtering |

**Installation:** No additional installation required - using existing Supabase infrastructure.

## Architecture Patterns

### Existing Schema Conventions (from supabase_setup.sql)

```sql
-- Primary keys
id SERIAL PRIMARY KEY           -- For auto-increment tables
id UUID PRIMARY KEY             -- For auth-linked tables (profiles)

-- Foreign keys to auth.users
user_id UUID REFERENCES auth.users(id) ON DELETE CASCADE
author_id UUID REFERENCES auth.users(id) ON DELETE SET NULL

-- Timestamps
created_at TIMESTAMPTZ DEFAULT NOW()
updated_at TIMESTAMPTZ DEFAULT NOW()

-- JSONB for arrays
tags JSONB DEFAULT '[]'

-- Text fields
content TEXT NOT NULL
notes TEXT

-- Status enums via CHECK
status TEXT DEFAULT 'draft' CHECK (status IN ('draft', 'pending', 'approved'))
```

### Recommended documents Table Structure

```sql
CREATE TABLE public.documents (
    pgpid INTEGER PRIMARY KEY,  -- Natural key from PGP
    shelfmark_combined TEXT,    -- "T-S 13J35.3 + AIU VII.A.23"
    document_type TEXT,         -- "Letter", "Legal document", etc.
    tags JSONB DEFAULT '[]',    -- ["communal", "marriage", "trade"]
    doc_date_original TEXT,     -- "1337 Seleucid"
    doc_date_standard TEXT,     -- "1025-08-28/1026-09-14"
    inferred_date_display TEXT, -- "1025-1026 CE"
    description TEXT,           -- English scholarly description
    transcription TEXT,         -- Full transcription content
    transcription_source TEXT,  -- "Amir Ashur, PGP"
    pgp_url TEXT GENERATED ALWAYS AS
        ('https://geniza.princeton.edu/documents/' || pgpid || '/') STORED,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

### Recommended document_fragments Table Structure

```sql
CREATE TABLE public.document_fragments (
    id SERIAL PRIMARY KEY,
    document_id INTEGER REFERENCES documents(pgpid) ON DELETE CASCADE,
    sys_id TEXT NOT NULL,           -- GenizahSearch system_number
    shelfmark TEXT,                 -- Denormalized for display
    sequence_order INTEGER DEFAULT 1,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(document_id, sys_id)     -- Prevent duplicate fragment links
);
```

### Index Strategy (Based on Usage Patterns)

```sql
-- Primary lookup: find document for a fragment
CREATE INDEX idx_document_fragments_sys_id ON document_fragments(sys_id);

-- Document lookup
CREATE INDEX idx_document_fragments_document_id ON document_fragments(document_id);

-- Tag filtering
CREATE INDEX idx_documents_tags ON documents USING GIN (tags);

-- Type filtering
CREATE INDEX idx_documents_document_type ON documents(document_type);
```

### Anti-Patterns to Avoid

- **Separate tags table:** JSONB arrays are simpler and performant for tags with GIN index. Existing schema uses JSONB for tags in list_items.
- **user_id in documents:** These are PGP-sourced scholarly records, not user-generated content. No ownership column needed.
- **Storing transcription in corrections table:** Keep PGP transcriptions separate - they are authoritative source data, not user corrections.

## Don't Hand-Roll

Problems that look simple but have existing solutions:

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| URL generation | String concatenation | PostgreSQL GENERATED column | Computed once, always consistent |
| Tag validation | Application-level checks | pg_jsonschema or CHECK constraint | Database-enforced integrity |
| Timestamp management | Application code | DEFAULT NOW() | Automatic, consistent |
| Cascade deletes | Application code | ON DELETE CASCADE | Database handles integrity |

**Key insight:** PostgreSQL handles data integrity better than application code. Use database constraints.

## Common Pitfalls

### Pitfall 1: Inconsistent User Reference Columns
**What goes wrong:** Existing schema has both `user_id` and `author_id` columns for the same purpose.
**Why it happens:** Historical inconsistency in naming.
**How to avoid:** New tables for PGP documents don't need user references - they're system data.
**Warning signs:** If you're adding a user reference column, reconsider - PGP data is not user-owned.

### Pitfall 2: Wrong RLS Role
**What goes wrong:** RLS policies using `public` role instead of `authenticated`.
**Why it happens:** Copy-paste from examples that don't match Supabase's auth model.
**How to avoid:** Use `TO authenticated` for INSERT/UPDATE/DELETE operations. Use `TO public` only for SELECT on public data.
**Warning signs:** `auth.uid()` returning NULL in write operations.

### Pitfall 3: Missing Unique Constraints
**What goes wrong:** Duplicate fragment-document associations.
**Why it happens:** Forgetting to add composite unique constraint.
**How to avoid:** Add `UNIQUE(document_id, sys_id)` on document_fragments.
**Warning signs:** Multiple rows returned when querying a fragment's document.

### Pitfall 4: JSONB vs TEXT for Tags
**What goes wrong:** Storing tags as comma-separated TEXT.
**Why it happens:** Simpler initial implementation.
**How to avoid:** Use JSONB array - existing schema uses this pattern.
**Warning signs:** Complex string parsing in queries.

## Code Examples

### Table Creation Pattern (from supabase_setup.sql)

```sql
-- Source: C:\GenizahSearch\supabase_setup.sql
CREATE TABLE public.fragment_joins (
    id SERIAL PRIMARY KEY,
    user_id UUID REFERENCES auth.users(id) ON DELETE SET NULL,
    fragment_a_sys_id TEXT NOT NULL,
    fragment_a_shelfmark TEXT,
    fragment_b_sys_id TEXT NOT NULL,
    fragment_b_shelfmark TEXT,
    join_type TEXT DEFAULT 'uncertain' CHECK (join_type IN ('physical', 'content', 'uncertain')),
    confidence TEXT DEFAULT 'possible' CHECK (confidence IN ('certain', 'probable', 'possible')),
    notes TEXT,
    evidence TEXT,
    status TEXT DEFAULT 'proposed' CHECK (status IN ('proposed', 'confirmed', 'rejected')),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    confirmed_by UUID REFERENCES auth.users(id),
    confirmed_at TIMESTAMPTZ
);
```

### RLS Policy Pattern (from fix_rls_policies.sql)

```sql
-- Source: C:\GenizahSearch\scripts\fix_rls_policies.sql

-- Public read access
CREATE POLICY "Documents are publicly viewable" ON documents
FOR SELECT TO public
USING (true);

-- No INSERT/UPDATE/DELETE policies needed - data is imported, not user-created
```

### Index Creation Pattern

```sql
-- Source: C:\GenizahSearch\supabase_setup.sql
CREATE INDEX idx_fragment_joins_fragment_a ON fragment_joins(fragment_a_sys_id);
CREATE INDEX idx_fragment_joins_fragment_b ON fragment_joins(fragment_b_sys_id);
```

### JSONB Tag Query Pattern

```sql
-- Query documents by tag
SELECT * FROM documents WHERE tags @> '["communal"]';

-- Query documents by multiple tags (AND)
SELECT * FROM documents WHERE tags @> '["communal", "marriage"]';

-- Query documents by any tag (OR)
SELECT * FROM documents WHERE tags ?| ARRAY['communal', 'marriage'];
```

## PGP Data Structure Analysis

### documents.csv Columns (HIGH confidence - verified from file)

| Column | PGP Type | Supabase Type | Notes |
|--------|----------|---------------|-------|
| pgpid | int | INTEGER | Primary key |
| shelfmark | text | TEXT | Combined shelfmarks, e.g., "T-S 13J35.3 + AIU VII.A.23" |
| type | text | TEXT | "Letter", "Legal document", "List or table", etc. |
| tags | CSV | JSONB | "communal, marriage, trade" -> ["communal", "marriage", "trade"] |
| description | text | TEXT | Long scholarly description |
| doc_date_original | text | TEXT | "1337 Seleucid", "19 Adar 1427" |
| doc_date_standard | text | TEXT | "1025-08-28/1026-09-14" |
| inferred_date_display | text | TEXT | "1025-1026 CE" |
| languages_primary | text | TEXT | "Judaeo-Arabic", "Hebrew" |
| initial_entry | timestamp | (not imported) | PGP metadata |
| last_modified | timestamp | (not imported) | PGP metadata |
| has_transcription | Y/N | (derived) | Will be computed from transcriptions_linked.csv |

### transcriptions_linked.csv Schema (HIGH confidence - verified from file)

| Column | Description | Supabase Mapping |
|--------|-------------|------------------|
| sys_id | GenizahSearch system_number | document_fragments.sys_id |
| pgpid | PGP document ID | documents.pgpid |
| shelfmark | Original shelfmark | document_fragments.shelfmark |
| doc_type | Document type | documents.document_type |
| source_scholar | Attribution | documents.transcription_source |
| content | Full transcription | documents.transcription |

### Multi-Fragment Document Examples

```csv
# pgpid 444: "T-S 13J35.3 + AIU VII.A.23"
# pgpid 448: "Moss. IV,14.2 + AIU VII.E.119"
# pgpid 461: "BL OR 5549.3 + BL OR 5549.4"
# pgpid 472: "T-S 20.169 + T-S 10J8.9 + BL OR 5542.6"
# pgpid 491: "T-S 10J16.8 + BL OR 5566D.24"
```

Multi-fragment documents use " + " as delimiter. Import logic should:
1. Split on " + "
2. Match each part to sys_id via normalization
3. Create document_fragments rows with sequence_order

## Transcription Storage Decision

**Decision: Store transcription in documents table, NOT in corrections table.**

Rationale:
1. PGP transcriptions are authoritative scholarly source data
2. corrections table has `author_id`, `status`, `votes` - designed for user submissions
3. Transcriptions are read-only imports, not subject to review workflow
4. Simpler queries: `SELECT transcription FROM documents WHERE pgpid = X`

The attribution format "Amir Ashur, PGP" goes in `transcription_source` column.

## Import Metadata Decision

**Decision: Add minimal import metadata for debugging.**

```sql
-- Add to documents table
imported_at TIMESTAMPTZ DEFAULT NOW(),
```

Rationale:
- `created_at` serves import timestamp purpose
- Source file hash not needed - we can re-import from PGP anytime
- PGP's `last_modified` not needed - we track our own import time

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| JSON column | JSONB column | PostgreSQL 9.4+ | Binary storage, faster queries |
| TEXT arrays | JSONB arrays | Supabase standard | GIN indexing, cleaner queries |
| Application URL building | GENERATED columns | PostgreSQL 12+ | Computed automatically |

**Current practice:**
- JSONB for semi-structured data (tags, settings)
- TEXT for free-form content (descriptions, transcriptions)
- GENERATED columns for computed fields (URLs)

## RLS Policy Recommendations

### documents table (PUBLIC READ)

```sql
-- Enable RLS
ALTER TABLE documents ENABLE ROW LEVEL SECURITY;

-- Public read access (no authentication required)
CREATE POLICY "Documents are publicly viewable" ON documents
FOR SELECT TO public
USING (true);

-- No write policies - data imported via service role
```

### document_fragments table (PUBLIC READ)

```sql
-- Enable RLS
ALTER TABLE document_fragments ENABLE ROW LEVEL SECURITY;

-- Public read access
CREATE POLICY "Document fragments are publicly viewable" ON document_fragments
FOR SELECT TO public
USING (true);

-- No write policies - data imported via service role
```

**Note:** Import operations use Supabase service role key, which bypasses RLS.

## Migration File Conventions

Based on existing files in the project:

### Pattern 1: Full Setup Script (supabase_setup.sql)
- All tables, indexes, RLS policies in one file
- Good for initial setup
- Run in Supabase SQL Editor

### Pattern 2: Incremental Migration (migrations/add_soft_delete.sql)
- Single focused change
- Date-prefixed comment
- Includes index and comment

### Recommended Approach for Phase 1

1. Create `migrations/add_pgp_documents_tables.sql`
2. Include:
   - CREATE TABLE statements
   - Indexes
   - RLS policies
   - Comments on columns
3. Update `supabase_setup.sql` to include new tables (for fresh deployments)
4. Update `docs/guides/SUPABASE_GUIDE.md` with new table documentation

## Open Questions

Things that couldn't be fully resolved:

1. **Multi-fragment sequence_order**
   - What we know: PGP lists fragments in order "T-S X + T-S Y"
   - What's unclear: Is this scholarly sequence order or arbitrary?
   - Recommendation: Use order from PGP shelfmark string (left=1, next=2, etc.)

2. **Tag normalization**
   - What we know: PGP tags are comma-separated lowercase strings
   - What's unclear: Should we normalize casing/formatting?
   - Recommendation: Store as-is from PGP, normalize in import script

3. **Transcription versioning**
   - What we know: PGP updates daily
   - What's unclear: How to handle transcription updates
   - Recommendation: For Phase 1, full replace on re-import. Versioning deferred.

## Sources

### Primary (HIGH confidence)
- `C:\GenizahSearch\supabase_setup.sql` - Full existing schema
- `C:\GenizahSearch\scripts\fix_rls_policies.sql` - RLS patterns
- `C:\GenizahSearch\migrations\add_soft_delete.sql` - Migration conventions
- `C:\GenizahSearch\docs\guides\SUPABASE_GUIDE.md` - Schema documentation
- `C:\GenizahSearch\pgp_data\documents.csv` - PGP document structure
- `C:\GenizahSearch\pgp_data\transcriptions_linked.csv` - Linked transcriptions
- `C:\GenizahSearch\pgp_data\MATCHING_SUMMARY.md` - Data matching analysis

### Secondary (MEDIUM confidence)
- [Supabase JSONB Documentation](https://supabase.com/docs/guides/database/json) - JSONB best practices
- [Supabase pg_jsonschema](https://supabase.com/docs/guides/database/extensions/pg_jsonschema) - JSON validation

### Tertiary (LOW confidence)
- General PostgreSQL schema design patterns from web search

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - verified from existing project files
- Architecture patterns: HIGH - derived from existing supabase_setup.sql
- PGP data structure: HIGH - verified from actual CSV files
- Pitfalls: HIGH - documented in SUPABASE_GUIDE.md
- RLS patterns: HIGH - verified from fix_rls_policies.sql

**Research date:** 2026-02-05
**Valid until:** 60 days (schema patterns stable)
