-- Migration: Create document_sources table
-- Run this in Supabase SQL Editor
-- Date: 2026-02-06
-- Purpose: Store multiple transcriptions (from different scholars) and translations
--          (Hebrew/English) for each PGP document, enabling multi-source UI display
--
-- This table supports:
-- - Multiple transcriptions from different scholars for the same document
-- - Multiple translations in different languages
-- - Source attribution and linking to original PGP records
-- - Notes/emendations from PGP
--
-- Replaces the single transcription/transcription_source columns in documents table
-- for documents that have multiple sources.

-- ============================================
-- DOCUMENT SOURCES TABLE
-- ============================================
-- Use DO block with IF NOT EXISTS pattern for safe re-runs

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'document_sources'
    ) THEN
        CREATE TABLE public.document_sources (
            id BIGSERIAL PRIMARY KEY,
            pgpid INTEGER NOT NULL REFERENCES documents(pgpid) ON DELETE CASCADE,
            source_scholar TEXT NOT NULL,           -- Attribution: "S.D. Goitein", "Amir Ashur", etc.
            doc_relation TEXT NOT NULL,             -- "Digital Edition", "Digital Translation", "Edition", etc.
            language TEXT,                          -- Detected language: "Hebrew", "English", "Judaeo-Arabic", etc.
            content TEXT NOT NULL,                  -- Transcription or translation text
            content_length INTEGER,                 -- Character count for display/sorting
            source_url TEXT,                        -- Optional URL to original source
            notes TEXT,                             -- Emendations/notes from PGP
            sequence_order INTEGER DEFAULT 1,       -- Order within same doc_relation type
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        RAISE NOTICE 'Created document_sources table';
    ELSE
        RAISE NOTICE 'document_sources table already exists, skipping creation';
    END IF;
END $$;

-- ============================================
-- INDEXES
-- ============================================

-- Primary lookup: find all sources for a document
CREATE INDEX IF NOT EXISTS idx_document_sources_pgpid
    ON document_sources(pgpid);

-- Filtered lookup: find sources by document and relation type
-- Useful for queries like "get all transcriptions for document X"
CREATE INDEX IF NOT EXISTS idx_document_sources_relation
    ON document_sources(pgpid, doc_relation);

-- ============================================
-- UNIQUE CONSTRAINT
-- ============================================
-- Prevent duplicate imports: same scholar, same relation type for same document
-- Note: Using DO block because ADD CONSTRAINT IF NOT EXISTS is not supported

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'document_sources_unique_source'
    ) THEN
        ALTER TABLE document_sources
            ADD CONSTRAINT document_sources_unique_source
            UNIQUE(pgpid, source_scholar, doc_relation);
        RAISE NOTICE 'Added unique constraint document_sources_unique_source';
    ELSE
        RAISE NOTICE 'Unique constraint document_sources_unique_source already exists';
    END IF;
END $$;

-- ============================================
-- ROW LEVEL SECURITY
-- ============================================
-- This table is PUBLIC READ ONLY
-- Writes happen via service role during data import

ALTER TABLE document_sources ENABLE ROW LEVEL SECURITY;

-- Public can read all document sources (no authentication required)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies
        WHERE tablename = 'document_sources' AND policyname = 'Public read access'
    ) THEN
        CREATE POLICY "Public read access" ON document_sources
            FOR SELECT TO public USING (true);
        RAISE NOTICE 'Created RLS policy "Public read access"';
    ELSE
        RAISE NOTICE 'RLS policy "Public read access" already exists';
    END IF;
END $$;

-- ============================================
-- COLUMN COMMENTS
-- ============================================

COMMENT ON TABLE public.document_sources IS
    'Multiple transcriptions and translations per PGP document. System data, not user-generated.';
COMMENT ON COLUMN document_sources.id IS
    'Auto-incrementing primary key';
COMMENT ON COLUMN document_sources.pgpid IS
    'Foreign key to documents.pgpid - the PGP document this source belongs to';
COMMENT ON COLUMN document_sources.source_scholar IS
    'Attribution for this source, e.g., "S.D. Goitein", "Amir Ashur, PGP"';
COMMENT ON COLUMN document_sources.doc_relation IS
    'Source type: "Digital Edition" (transcription), "Digital Translation", "Edition", etc.';
COMMENT ON COLUMN document_sources.language IS
    'Language of content: "Hebrew", "English", "Judaeo-Arabic", or null if unknown';
COMMENT ON COLUMN document_sources.content IS
    'Full text content of transcription or translation';
COMMENT ON COLUMN document_sources.content_length IS
    'Character count of content, useful for sorting by length or display purposes';
COMMENT ON COLUMN document_sources.source_url IS
    'Optional URL to original source (external link)';
COMMENT ON COLUMN document_sources.notes IS
    'Emendations or notes from PGP, typically scholarly annotations';
COMMENT ON COLUMN document_sources.sequence_order IS
    'Order within same doc_relation type (1-based), for documents with multiple sources of same type';
COMMENT ON COLUMN document_sources.created_at IS
    'Timestamp when record was imported';
