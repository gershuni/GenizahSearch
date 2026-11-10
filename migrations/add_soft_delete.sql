-- Migration: Add soft delete support to user_lists
-- Run this in Supabase SQL Editor
-- Date: 2026-01-31

-- Add deleted_at column for soft delete
ALTER TABLE user_lists
ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ DEFAULT NULL;

-- Add index for efficient filtering of non-deleted lists
CREATE INDEX IF NOT EXISTS idx_user_lists_deleted_at
ON user_lists(user_id, deleted_at)
WHERE deleted_at IS NULL;

-- Update RLS policies to handle deleted lists
-- Users can see their own deleted lists (for trash view)
-- But normal queries should filter them out in application code

-- Optional: Add a scheduled job to permanently delete old trash items
-- This would need to be set up in Supabase Dashboard > Database > Extensions > pg_cron
-- Example (uncomment and adjust retention period as needed):
-- SELECT cron.schedule(
--   'cleanup-old-deleted-lists',
--   '0 0 * * *',  -- Run daily at midnight
--   $$DELETE FROM user_lists WHERE deleted_at < NOW() - INTERVAL '30 days'$$
-- );

COMMENT ON COLUMN user_lists.deleted_at IS 'Soft delete timestamp. NULL means active, set means in trash.';
