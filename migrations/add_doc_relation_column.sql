-- Migration: Add doc_relation column to documents table
-- Purpose: Distinguish Digital Editions (transcriptions) from Digital Translations
-- Date: 2026-02-05
--
-- Background:
-- PGP records include both Digital Editions (original Hebrew/Aramaic transcriptions)
-- and Digital Translations (English translations). Users expect "PGP Transcription"
-- to show transcriptions only, not translations.
--
-- Distribution from import data:
--   Digital Edition: 7,664 records (transcriptions - primary use case)
--   Digital Translation: 1,696 records (English translations - filter out)
--   Other: 4 records (Edition, Edition ; Translation, etc.)

-- Add the column
ALTER TABLE public.documents ADD COLUMN IF NOT EXISTS doc_relation TEXT;

-- Add comment explaining the column
COMMENT ON COLUMN documents.doc_relation IS 'Document relation type from PGP: "Digital Edition" (transcription) or "Digital Translation"';

-- Index for filtering (helps queries that filter by doc_relation)
CREATE INDEX IF NOT EXISTS idx_documents_doc_relation ON documents(doc_relation);
