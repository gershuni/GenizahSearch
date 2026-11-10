-- Migration: Add full PGP metadata columns to documents and document_fragments
-- Run this in Supabase SQL Editor
-- Date: 2026-02-08
-- Purpose: Extend documents table with 8 new columns for full PGP metadata import,
--          and document_fragments table with 5 new columns for fragment metadata
--          from fragments.csv (collection, library, URLs)
--
-- Prerequisites: add_pgp_documents_tables.sql and add_pgp_metadata_columns.sql
-- Safe to re-run: Uses ADD COLUMN IF NOT EXISTS

-- ============================================
-- DOCUMENTS TABLE: NEW COLUMNS
-- ============================================

-- Scholarship/bibliography HTML (28.3% populated)
ALTER TABLE documents ADD COLUMN IF NOT EXISTS scholarship_records TEXT;

-- Historical shelfmark variants (55.7% populated)
ALTER TABLE documents ADD COLUMN IF NOT EXISTS shelfmarks_historic TEXT;

-- Language notes, sparse but valuable (0.7% populated)
ALTER TABLE documents ADD COLUMN IF NOT EXISTS language_note TEXT;

-- Calendar system used for dating (11.8% populated)
ALTER TABLE documents ADD COLUMN IF NOT EXISTS doc_date_calendar TEXT;

-- Additional date reasoning notes (2.7% populated)
ALTER TABLE documents ADD COLUMN IF NOT EXISTS inferred_date_notes TEXT;

-- PGP flags for transcription/translation availability (nearly 100% populated)
ALTER TABLE documents ADD COLUMN IF NOT EXISTS has_transcription BOOLEAN DEFAULT FALSE;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS has_translation BOOLEAN DEFAULT FALSE;

-- PGP contributors who entered/edited the record (99.8% populated)
ALTER TABLE documents ADD COLUMN IF NOT EXISTS input_by TEXT;

-- ============================================
-- DOCUMENT_FRAGMENTS TABLE: NEW COLUMNS
-- ============================================
-- Fragment metadata from fragments.csv, stored directly on the link table
-- to avoid an extra JOIN for common queries

-- Collection name (always populated in fragments.csv)
ALTER TABLE document_fragments ADD COLUMN IF NOT EXISTS collection TEXT;

-- Full library name (always populated)
ALTER TABLE document_fragments ADD COLUMN IF NOT EXISTS library TEXT;

-- Short library code: CUL, JTS, Oxford, etc. (always populated)
ALTER TABLE document_fragments ADD COLUMN IF NOT EXISTS library_abbrev TEXT;

-- Fragment URL from PGP (34.2% populated)
ALTER TABLE document_fragments ADD COLUMN IF NOT EXISTS fragment_url TEXT;

-- IIIF image URL (54.3% populated)
ALTER TABLE document_fragments ADD COLUMN IF NOT EXISTS iiif_url TEXT;

-- ============================================
-- COLUMN COMMENTS: DOCUMENTS
-- ============================================

COMMENT ON COLUMN documents.scholarship_records IS
    'HTML-formatted bibliography/scholarship records from PGP (28.3% populated)';
COMMENT ON COLUMN documents.shelfmarks_historic IS
    'Historical shelfmark variants from PGP (55.7% populated)';
COMMENT ON COLUMN documents.language_note IS
    'Language notes, sparse but valuable for edge cases (0.7% populated)';
COMMENT ON COLUMN documents.doc_date_calendar IS
    'Calendar system used for dating, e.g., "Seleucid" (11.8% populated)';
COMMENT ON COLUMN documents.inferred_date_notes IS
    'Additional reasoning/notes for inferred dates (2.7% populated)';
COMMENT ON COLUMN documents.has_transcription IS
    'PGP flag: document has at least one digital transcription available';
COMMENT ON COLUMN documents.has_translation IS
    'PGP flag: document has at least one digital translation available';
COMMENT ON COLUMN documents.input_by IS
    'PGP contributors who entered/edited this record (99.8% populated)';

-- ============================================
-- COLUMN COMMENTS: DOCUMENT_FRAGMENTS
-- ============================================

COMMENT ON COLUMN document_fragments.collection IS
    'Collection name from PGP fragments.csv, e.g., "Taylor-Schechter Collection"';
COMMENT ON COLUMN document_fragments.library IS
    'Full library name, e.g., "Cambridge University Library"';
COMMENT ON COLUMN document_fragments.library_abbrev IS
    'Short library code: CUL, JTS, Oxford, Manchester, etc.';
COMMENT ON COLUMN document_fragments.fragment_url IS
    'URL to fragment on holding library website (34.2% populated)';
COMMENT ON COLUMN document_fragments.iiif_url IS
    'IIIF image manifest URL for fragment (54.3% populated)';
