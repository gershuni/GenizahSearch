-- Migration: Add missing PGP metadata columns to documents table
-- Run this in Supabase SQL Editor
-- Date: 2026-02-06
-- Purpose: Add languages and inferred date columns that exist in pgp_data/documents.csv
--          but were not included in the original table creation

-- ============================================
-- ADD MISSING COLUMNS
-- ============================================

ALTER TABLE documents ADD COLUMN IF NOT EXISTS languages_primary TEXT;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS languages_secondary TEXT;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS inferred_date_standard TEXT;
ALTER TABLE documents ADD COLUMN IF NOT EXISTS inferred_date_rationale TEXT;

-- ============================================
-- COLUMN COMMENTS
-- ============================================

COMMENT ON COLUMN documents.languages_primary IS 'Primary language(s), e.g., "Judaeo-Arabic", "Hebrew, Aramaic"';
COMMENT ON COLUMN documents.languages_secondary IS 'Secondary language(s), if any';
COMMENT ON COLUMN documents.inferred_date_standard IS 'Standardized inferred date, e.g., "1160/1166"';
COMMENT ON COLUMN documents.inferred_date_rationale IS 'Rationale for inferred date, e.g., "Person mentioned"';
