-- Migration: Add structured sections column to document_sources
-- Purpose: Store per-canvas section data parsed from pgp-text HTML files
-- Format: [{"canvas_url": "...", "canvas_num": 1, "label": null, "text": "..."}]
--
-- Run in: Supabase Dashboard -> SQL Editor
-- Date: 2026-02-11

-- Add structured sections column (JSONB for per-canvas section data)
ALTER TABLE document_sources ADD COLUMN IF NOT EXISTS sections JSONB;

-- Add language and direction metadata from HTML section element
ALTER TABLE document_sources ADD COLUMN IF NOT EXISTS source_language TEXT;
ALTER TABLE document_sources ADD COLUMN IF NOT EXISTS source_direction TEXT;

COMMENT ON COLUMN document_sources.sections IS
    'Structured section data from pgp-text HTML: [{canvas_url, canvas_num, label, text, subsections}]';

COMMENT ON COLUMN document_sources.source_language IS
    'Language code from pgp-text HTML section element (e.g., jrb, he, en, ar)';

COMMENT ON COLUMN document_sources.source_direction IS
    'Text direction from pgp-text HTML section element (rtl or ltr)';
