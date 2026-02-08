-- Migration: Create document_footnotes table
-- Run this in Supabase SQL Editor
-- Date: 2026-02-08
-- Purpose: Store PGP footnotes/scholarship records including bibliographic references,
--          editions, discussions, and digital transcription/translation content.
--          24,388 records: ~9,745 with content text, ~14,643 bibliography-only.
--
-- Prerequisites: add_pgp_documents_tables.sql (documents table must exist)
-- Safe to re-run: Uses DO block with IF NOT EXISTS pattern

-- ============================================
-- DOCUMENT FOOTNOTES TABLE
-- ============================================

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'document_footnotes'
    ) THEN
        CREATE TABLE public.document_footnotes (
            id BIGSERIAL PRIMARY KEY,
            pgpid INTEGER NOT NULL REFERENCES documents(pgpid) ON DELETE CASCADE,
            source TEXT NOT NULL,                -- Full source citation, e.g., "Moshe Gil, Palestine During..."
            source_slug TEXT,                    -- Slug for deduplication, e.g., "gil-moshe-palestine-1983"
            doc_relation TEXT NOT NULL,          -- "Digital Edition", "Edition", "Discussion", "Translation", etc.
            location TEXT,                       -- Page/section reference, e.g., "233"
            url TEXT,                            -- External URL to source
            notes TEXT,                          -- Annotations or emendations
            content TEXT,                        -- Transcription/translation text (NULL for bibliography-only)
            content_length INTEGER,              -- Character count of content
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        RAISE NOTICE 'Created document_footnotes table';
    ELSE
        RAISE NOTICE 'document_footnotes table already exists, skipping creation';
    END IF;
END $$;

-- ============================================
-- INDEXES
-- ============================================

-- Primary lookup: find all footnotes for a document
CREATE INDEX IF NOT EXISTS idx_document_footnotes_pgpid
    ON document_footnotes(pgpid);

-- Filtered lookup: find footnotes by document and relation type
CREATE INDEX IF NOT EXISTS idx_document_footnotes_relation
    ON document_footnotes(pgpid, doc_relation);

-- ============================================
-- UNIQUE CONSTRAINT
-- ============================================
-- Prevent duplicate imports: same source_slug, same relation type for same document
-- Note: Using DO block because ADD CONSTRAINT IF NOT EXISTS is not supported

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'document_footnotes_unique_source'
    ) THEN
        ALTER TABLE document_footnotes
            ADD CONSTRAINT document_footnotes_unique_source
            UNIQUE(pgpid, source_slug, doc_relation);
        RAISE NOTICE 'Added unique constraint document_footnotes_unique_source';
    ELSE
        RAISE NOTICE 'Unique constraint document_footnotes_unique_source already exists';
    END IF;
END $$;

-- ============================================
-- ROW LEVEL SECURITY
-- ============================================
-- This table is PUBLIC READ ONLY
-- Writes happen via service role during data import

ALTER TABLE document_footnotes ENABLE ROW LEVEL SECURITY;

-- Public can read all footnotes (no authentication required)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies
        WHERE tablename = 'document_footnotes' AND policyname = 'Footnotes are publicly viewable'
    ) THEN
        CREATE POLICY "Footnotes are publicly viewable" ON document_footnotes
            FOR SELECT TO public USING (true);
        RAISE NOTICE 'Created RLS policy "Footnotes are publicly viewable"';
    ELSE
        RAISE NOTICE 'RLS policy "Footnotes are publicly viewable" already exists';
    END IF;
END $$;

-- ============================================
-- COLUMN COMMENTS
-- ============================================

COMMENT ON TABLE public.document_footnotes IS
    'PGP footnotes and scholarship records. Includes bibliographic references (Edition, Discussion) and digital content (Digital Edition, Digital Translation). System data, not user-generated.';
COMMENT ON COLUMN document_footnotes.id IS
    'Auto-incrementing primary key';
COMMENT ON COLUMN document_footnotes.pgpid IS
    'Foreign key to documents.pgpid - the PGP document this footnote belongs to';
COMMENT ON COLUMN document_footnotes.source IS
    'Full source citation, e.g., "Moshe Gil, Palestine During the First Muslim Period (634-1099)"';
COMMENT ON COLUMN document_footnotes.source_slug IS
    'Slug for deduplication, e.g., "gil-moshe-palestine-1983"';
COMMENT ON COLUMN document_footnotes.doc_relation IS
    'Relation type: "Digital Edition", "Edition", "Discussion", "Translation", "Digital Translation", etc.';
COMMENT ON COLUMN document_footnotes.location IS
    'Page/section reference within the source, e.g., "233", "vol. 2, pp. 118-120"';
COMMENT ON COLUMN document_footnotes.url IS
    'External URL to the source or digital resource';
COMMENT ON COLUMN document_footnotes.notes IS
    'Annotations, emendations, or scholarly notes';
COMMENT ON COLUMN document_footnotes.content IS
    'Transcription or translation text content (NULL for bibliography-only footnotes)';
COMMENT ON COLUMN document_footnotes.content_length IS
    'Character count of content field, for display/sorting purposes';
COMMENT ON COLUMN document_footnotes.created_at IS
    'Timestamp when record was imported';
