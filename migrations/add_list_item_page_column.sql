-- Migration: list_items.page -- which page of a manuscript a list entry is
-- Run this in the Supabase SQL Editor (the complete file; it is one transaction).
-- Apply it BEFORE releasing the desktop version that writes the column.
-- Date: 2026-09-27
--
-- The desktop saves a list entry per page ({sys_id}::img::{page}) or per folio
-- ({sys_id}::fl::{fl_id}). list_items had sys_id + fl_id only, so a page without an
-- FL id was stored as the whole manuscript, and two such pages of one manuscript could
-- not be told apart. The desktop writes page when this column exists and keeps working
-- without it (it checks on every sync). The web reads it through select('*'); web
-- writes never set it (inserts leave it NULL, note and tag edits are partial updates).
--
-- page is TEXT: the desktop's page values are digit strings, and occasionally 'Unknown'.
--
-- Safe to run more than once: the column is added only if missing, and the comment
-- and grants are simply restated.
--
-- RLS: unchanged. list_items keeps RLS enabled and its existing row policies; a new
-- column needs none. Grants: table privileges already cover a new column; they are
-- restated here per the project convention (CLAUDE.md, convention 6). anon is left as it is.
--
-- Rollback: alter table public.list_items drop column if exists page;
--           notify pgrst, 'reload schema';

begin;

alter table public.list_items add column if not exists page text;

comment on column public.list_items.page is
  'Page (image number) of this entry within the manuscript, as the desktop records it; NULL = no page (a folio via fl_id, or the whole manuscript).';

grant select, insert, update, delete on table public.list_items to authenticated;
grant all on table public.list_items to service_role;

do $$
declare
  seq text := pg_get_serial_sequence('public.list_items', 'id');
begin
  if seq is not null then
    execute format('grant usage, select on sequence %s to authenticated, service_role', seq);
  end if;
end $$;

commit;

-- Make the API see the new column now instead of at its next schema reload.
notify pgrst, 'reload schema';

-- Verify (run both after the migration). The first must list two rows,
--   page | text
--   tags | jsonb
-- The desktop's tag checks are written for a jsonb tags column; if tags is not
-- jsonb, do not release the desktop version until that is resolved.
--
--   select column_name, data_type from information_schema.columns
--    where table_schema = 'public' and table_name = 'list_items'
--      and column_name in ('page', 'tags') order by 1;
--
-- The second must show SELECT, INSERT, UPDATE and DELETE for authenticated:
--
--   select grantee, privilege_type from information_schema.role_table_grants
--    where table_schema = 'public' and table_name = 'list_items' order by 1, 2;
