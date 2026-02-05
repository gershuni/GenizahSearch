-- Migration: Add PGP documents tables
-- Run this in Supabase SQL Editor
-- Date: 2026-02-05
-- Purpose: Create tables to store PGP (Princeton Geniza Project) document metadata,
--          transcriptions, and fragment-to-document linkages for multi-fragment manuscripts

-- ============================================
-- DOCUMENTS TABLE
-- ============================================
-- Stores PGP document metadata and transcriptions
-- Note: This is system data, NOT user-generated content
-- Single-fragment manuscripts do NOT have entries here (DOC-02 requirement)

CREATE TABLE public.documents (
    pgpid INTEGER PRIMARY KEY,               -- Natural key from PGP (Princeton Geniza Project ID)
    shelfmark_combined TEXT,                 -- "T-S 13J35.3 + AIU VII.A.23"
    document_type TEXT,                      -- "Letter", "Legal document", etc.
    tags JSONB DEFAULT '[]',                 -- ["communal", "marriage", "trade"]
    doc_date_original TEXT,                  -- "1337 Seleucid"
    doc_date_standard TEXT,                  -- "1025-08-28/1026-09-14"
    inferred_date_display TEXT,              -- "1025-1026 CE"
    description TEXT,                        -- English scholarly description
    transcription TEXT,                      -- Full transcription content
    transcription_source TEXT,               -- "Amir Ashur, PGP"
    pgp_url TEXT GENERATED ALWAYS AS
        ('https://geniza.princeton.edu/documents/' || pgpid || '/') STORED,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================
-- DOCUMENT FRAGMENTS TABLE
-- ============================================
-- Links PGP documents to GenizahSearch fragments via sys_id
-- This enables looking up which document(s) a manuscript fragment belongs to

CREATE TABLE public.document_fragments (
    id SERIAL PRIMARY KEY,
    document_id INTEGER REFERENCES documents(pgpid) ON DELETE CASCADE,
    sys_id TEXT NOT NULL,                    -- GenizahSearch system_number from libraries.csv
    shelfmark TEXT,                          -- Denormalized for display efficiency
    sequence_order INTEGER DEFAULT 1,        -- Order within multi-fragment document
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(document_id, sys_id)              -- Prevent duplicate fragment links
);

-- ============================================
-- INDEXES
-- ============================================

-- Primary lookup: find document for a fragment (most common query)
CREATE INDEX idx_document_fragments_sys_id ON document_fragments(sys_id);

-- Document lookup: find all fragments for a document
CREATE INDEX idx_document_fragments_document_id ON document_fragments(document_id);

-- Tag filtering with GIN for efficient JSONB array queries
CREATE INDEX idx_documents_tags ON documents USING GIN (tags);

-- Type filtering for browsing by document type
CREATE INDEX idx_documents_document_type ON documents(document_type);

-- ============================================
-- ROW LEVEL SECURITY
-- ============================================
-- These tables are PUBLIC READ ONLY
-- Writes happen via service role during data import

ALTER TABLE documents ENABLE ROW LEVEL SECURITY;
ALTER TABLE document_fragments ENABLE ROW LEVEL SECURITY;

-- Public can read all documents (no authentication required)
CREATE POLICY "Documents are publicly viewable" ON documents
FOR SELECT TO public USING (true);

-- Public can read all fragment linkages (no authentication required)
CREATE POLICY "Document fragments are publicly viewable" ON document_fragments
FOR SELECT TO public USING (true);

-- ============================================
-- COLUMN COMMENTS
-- ============================================

-- documents table columns
COMMENT ON TABLE public.documents IS 'PGP document metadata and transcriptions. System data, not user-generated.';
COMMENT ON COLUMN documents.pgpid IS 'Princeton Geniza Project document ID - natural primary key';
COMMENT ON COLUMN documents.shelfmark_combined IS 'Combined shelfmark from PGP, e.g., "T-S 13J35.3 + AIU VII.A.23"';
COMMENT ON COLUMN documents.document_type IS 'Document type classification: Letter, Legal document, List, etc.';
COMMENT ON COLUMN documents.tags IS 'Subject tags array as JSONB, e.g., ["communal", "marriage", "trade"]';
COMMENT ON COLUMN documents.doc_date_original IS 'Original date notation from source, e.g., "1337 Seleucid"';
COMMENT ON COLUMN documents.doc_date_standard IS 'Standardized date range in ISO format, e.g., "1025-08-28/1026-09-14"';
COMMENT ON COLUMN documents.inferred_date_display IS 'Human-readable inferred date for display, e.g., "1025-1026 CE"';
COMMENT ON COLUMN documents.description IS 'English scholarly description of the document content';
COMMENT ON COLUMN documents.transcription IS 'Full transcription text content';
COMMENT ON COLUMN documents.transcription_source IS 'Attribution for transcription, e.g., "Amir Ashur, PGP"';
COMMENT ON COLUMN documents.pgp_url IS 'Generated URL to PGP website (computed column)';
COMMENT ON COLUMN documents.created_at IS 'Timestamp when record was imported';

-- document_fragments table columns
COMMENT ON TABLE public.document_fragments IS 'Links PGP documents to GenizahSearch fragments via sys_id';
COMMENT ON COLUMN document_fragments.id IS 'Auto-incrementing primary key';
COMMENT ON COLUMN document_fragments.document_id IS 'Foreign key to documents.pgpid';
COMMENT ON COLUMN document_fragments.sys_id IS 'GenizahSearch system_number from libraries.csv';
COMMENT ON COLUMN document_fragments.shelfmark IS 'Denormalized shelfmark for display efficiency';
COMMENT ON COLUMN document_fragments.sequence_order IS 'Order of fragment within multi-fragment document (1-based)';
COMMENT ON COLUMN document_fragments.created_at IS 'Timestamp when record was imported';
