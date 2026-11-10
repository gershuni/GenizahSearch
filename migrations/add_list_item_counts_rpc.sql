-- Migration: Add zero-trust get_list_item_counts_for_user RPC
-- Phase: 92.2 lists-performance-investigation
-- Purpose: Batched item-count lookup for the /lists hot path.
--
-- Filename convention reconciliation (Phase 92.2 PATTERNS.md §1 drift #1):
--   CONTEXT.md D-FANOUT-02 proposed `supabase/migrations/2026-MM-DD-list-item-counts-rpc.sql`.
--   Repo convention is `migrations/<verb>_<descriptive>.sql` (undated, flat dir).
--   Reconciled to current filename to match the 9 existing migrations.
--
-- Reviews MUST-FIX 2 (2026-05-17 cross-AI review):
--   - Drops `p_user_id uuid` argument (anti-pattern under SECURITY INVOKER:
--     caller could pass another user's UUID; RLS would block but the design
--     is a false-positive trigger for security audits and a zero-trust
--     violation). Identity now derived exclusively from `auth.uid()` inside
--     the function body.
--   - Explicit `REVOKE ALL ... FROM PUBLIC, anon;` before `GRANT EXECUTE`
--     to override PostgreSQL's default `PUBLIC` execute grant.
--   - `GRANT EXECUTE` to BOTH `authenticated` AND `service_role` (per
--     Codex; Gemini agrees -- background workers / admin scripts may need it).
--   - Adds `STABLE` + `SET search_path = public` for safety.
--   - Adds `CREATE INDEX IF NOT EXISTS idx_list_items_list_id` (verifies
--     the aggregation index exists).
--   - Adds `auth.uid() IS NOT NULL` guard AND `ul.deleted_at IS NULL`
--     filter (soft-delete safety; `user_lists.deleted_at` exists per
--     migrations/add_soft_delete.sql).
--   - Adds trailing `-- ROLLBACK` comment (Gemini-LOW breadcrumb).
--
-- Note: The DROP below drops the OLD signature (with p_user_id uuid arg) if
-- a half-applied migration from the prior planning iteration left it behind.

DROP FUNCTION IF EXISTS public.get_list_item_counts_for_user(uuid);

CREATE INDEX IF NOT EXISTS idx_list_items_list_id
ON public.list_items(list_id);

CREATE OR REPLACE FUNCTION public.get_list_item_counts_for_user()
RETURNS TABLE(list_id bigint, item_count bigint)
LANGUAGE sql
SECURITY INVOKER
STABLE
SET search_path = public
AS $$
    SELECT li.list_id, count(*) AS item_count
    FROM public.list_items AS li
    WHERE auth.uid() IS NOT NULL
      AND EXISTS (
          SELECT 1
          FROM public.user_lists AS ul
          WHERE ul.id = li.list_id
            AND ul.user_id = auth.uid()
            AND ul.deleted_at IS NULL
      )
    GROUP BY li.list_id;
$$;

REVOKE ALL ON FUNCTION public.get_list_item_counts_for_user() FROM PUBLIC, anon;
GRANT EXECUTE ON FUNCTION public.get_list_item_counts_for_user() TO authenticated, service_role;

-- ROLLBACK: DROP FUNCTION IF EXISTS public.get_list_item_counts_for_user();
