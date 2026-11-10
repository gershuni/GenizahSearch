-- Migration: Add page_info column to document_fragments
-- Run this in Supabase SQL Editor AFTER add_pgp_documents_tables.sql
-- Date: 2026-02-05
-- Purpose: Store recto/verso/folio information for each fragment within a document

-- Add page_info column (safe to re-run - uses IF NOT EXISTS pattern via DO block)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'document_fragments' AND column_name = 'page_info'
    ) THEN
        ALTER TABLE document_fragments ADD COLUMN page_info TEXT;
    END IF;
END $$;

-- Add column comment
COMMENT ON COLUMN document_fragments.page_info IS
'Page/folio info (recto, verso, recto and verso) for this fragment within the document';
