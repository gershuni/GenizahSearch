-- scripts/supabase_live_check.sql -- READ-ONLY live check of the Supabase RLS, grants,
-- functions and views (written for the 2026-10-20 reliability review; the repo's SQL and the
-- live database have drifted before, so the live catalog is the authority). Not run against
-- production and not machine-checked against a Postgres parser: fix a typo if the editor
-- reports one. Referenced from docs/OPEN_ISSUES.md (the `profiles` P1 row and its neighbours).
-- ============================================================================
-- GenizahSearch -- READ-ONLY live check of RLS, grants, functions and views.
-- Paste into the Supabase SQL editor and run ONE NUMBERED QUERY AT A TIME
-- (the editor shows only the last result set of a multi-statement run).
-- Every statement is a SELECT against catalogs; nothing is written. Query 12
-- reads auth.users for COUNTS only (no addresses are returned).
-- Written 2026-10-20 against repo HEAD 4cfaed48.
-- ============================================================================

-- Q1. Every table/view in public: RLS on? forced? and what anon/authenticated may do.
--     RED FLAG: kind = 'r', rls_enabled = false, and anon_* or auth_* true.
select c.relname                                   as name,
       case c.relkind when 'r' then 'table' when 'v' then 'view'
                      when 'm' then 'matview' when 'p' then 'partitioned' else c.relkind::text end as kind,
       c.relrowsecurity                            as rls_enabled,
       c.relforcerowsecurity                       as rls_forced,
       pg_get_userbyid(c.relowner)                 as owner,
       has_table_privilege('anon', c.oid, 'SELECT')          as anon_select,
       has_table_privilege('anon', c.oid, 'INSERT')          as anon_insert,
       has_table_privilege('anon', c.oid, 'UPDATE')          as anon_update,
       has_table_privilege('anon', c.oid, 'DELETE')          as anon_delete,
       has_table_privilege('authenticated', c.oid, 'SELECT') as auth_select,
       has_table_privilege('authenticated', c.oid, 'INSERT') as auth_insert,
       has_table_privilege('authenticated', c.oid, 'UPDATE') as auth_update,
       has_table_privilege('authenticated', c.oid, 'DELETE') as auth_delete,
       (select count(*) from pg_policy p where p.polrelid = c.oid) as n_policies,
       c.reloptions                                as reloptions
from pg_class c
join pg_namespace n on n.oid = c.relnamespace
where n.nspname = 'public' and c.relkind in ('r', 'v', 'm', 'p')
order by (c.relkind = 'r' and not c.relrowsecurity) desc, c.relname;

-- Q2. The tables the app code reaches (web/desktop/shared), and whether each exists.
--     Expect 20 rows with exists = true. Three of them have NO DDL in the repo:
--     published_joins, published_join_fragments, discovery_responses.
select t.name,
       to_regclass('public.' || t.name) is not null as exists,
       coalesce((select c.relrowsecurity from pg_class c where c.oid = to_regclass('public.' || t.name)), false) as rls_enabled
from (values ('profiles'), ('projects'), ('user_lists'), ('list_items'), ('recent_items'),
             ('corrections'), ('comments'), ('discoveries'), ('fragment_joins'),
             ('correction_votes'), ('discovery_votes'), ('documents'), ('document_fragments'),
             ('document_sources'), ('document_footnotes'), ('published_joins'),
             ('published_join_fragments'), ('discovery_responses'), ('discovery_suppressed'),
             ('identification_reviews')) as t(name)
order by 1;

-- Q3. Every policy in public (and storage), in full.
select schemaname, tablename, policyname, permissive, roles, cmd,
       qual       as using_expr,
       with_check as with_check_expr
from pg_policies
where schemaname in ('public', 'storage')
order by schemaname, tablename, cmd, policyname;

-- Q4. Policy red flags.
--   a) a WRITE policy (INSERT/UPDATE/DELETE/ALL) that applies to anon or public
--      and whose expressions never mention auth.uid() / auth.jwt() / auth.role();
--   b) any permissive UPDATE/ALL policy whose WITH CHECK is literally true (permissive
--      policies' checks combine with OR, so it widens every other UPDATE policy on the table);
--   c) SELECT policies USING (true) -- fine for public data, NOT for per-user tables.
select tablename, policyname, cmd, roles, qual, with_check,
       case
         when cmd in ('INSERT', 'UPDATE', 'DELETE', 'ALL')
              and (roles && array['public', 'anon']::name[])
              and coalesce(qual, '') || coalesce(with_check, '') !~ 'auth\.(uid|jwt|role)\(' then 'a) unauthenticated write'
         when cmd in ('UPDATE', 'ALL') and permissive = 'PERMISSIVE'
              and btrim(coalesce(with_check, '')) = 'true' then 'b) WITH CHECK (true) on UPDATE'
         when cmd in ('SELECT', 'ALL') and btrim(coalesce(qual, '')) = 'true' then 'c) public read of every row'
       end as flag
from pg_policies
where schemaname = 'public'
  and (
        (cmd in ('INSERT', 'UPDATE', 'DELETE', 'ALL')
         and (roles && array['public', 'anon']::name[])
         and coalesce(qual, '') || coalesce(with_check, '') !~ 'auth\.(uid|jwt|role)\(')
     or (cmd in ('UPDATE', 'ALL') and permissive = 'PERMISSIVE' and btrim(coalesce(with_check, '')) = 'true')
     or (cmd in ('SELECT', 'ALL') and btrim(coalesce(qual, '')) = 'true')
  )
order by flag, tablename;

-- Q5. Per-user tables must have an owner predicate on SELECT (no USING (true)).
--     Expect zero rows.
select tablename, policyname, cmd, roles, qual
from pg_policies
where schemaname = 'public'
  and tablename in ('projects', 'user_lists', 'list_items', 'recent_items')
  and cmd in ('SELECT', 'ALL')
  and coalesce(qual, '') !~ 'auth\.uid\(';

-- Q6. Grants to the Data API roles, per table (information_schema view).
select table_name, grantee, string_agg(privilege_type, ', ' order by privilege_type) as privileges
from information_schema.role_table_grants
where table_schema = 'public' and grantee in ('anon', 'authenticated', 'service_role')
group by table_name, grantee
order by table_name, grantee;

-- Q7. Which COLUMNS of profiles exist and can anon read them? (email, settings, last_login?)
select a.attname as column_name, format_type(a.atttypid, a.atttypmod) as type,
       has_column_privilege('anon', 'public.profiles'::regclass, a.attname, 'SELECT') as anon_can_select,
       has_column_privilege('authenticated', 'public.profiles'::regclass, a.attname, 'UPDATE') as auth_can_update
from pg_attribute a
where a.attrelid = 'public.profiles'::regclass and a.attnum > 0 and not a.attisdropped
order by a.attnum;

-- Q8. Functions in public: SECURITY DEFINER? search_path pinned? who may EXECUTE?
--     RED FLAG: prosecdef = true AND (search_path_pinned = false OR (anon_exec and body has no auth.uid())).
select p.proname,
       pg_get_function_identity_arguments(p.oid)          as args,
       pg_get_function_result(p.oid)                      as returns,
       p.prosecdef                                        as security_definer,
       p.proconfig                                        as config,
       coalesce(array_to_string(p.proconfig, ',') ~ 'search_path=', false) as search_path_pinned,
       has_function_privilege('anon', p.oid, 'EXECUTE')          as anon_exec,
       has_function_privilege('authenticated', p.oid, 'EXECUTE') as auth_exec,
       pg_get_functiondef(p.oid) ~ 'auth\.uid\('                as body_uses_auth_uid,
       pg_get_userbyid(p.proowner)                        as owner
from pg_proc p
join pg_namespace n on n.oid = p.pronamespace
where n.nspname = 'public' and p.prokind = 'f'
order by p.prosecdef desc, p.proname;

-- Q9. The live new-user trigger: does it still copy the email into profiles.username?
select pg_get_functiondef('public.handle_new_user'::regproc);

-- Q10. Triggers on public tables (the 2026-09-25 guards must be on profiles, corrections,
--      discoveries, fragment_joins).
select event_object_table as table_name, trigger_name, action_timing, event_manipulation, action_statement
from information_schema.triggers
where trigger_schema = 'public'
order by 1, 2, 4;

-- Q11. Views in public: do they run as the caller (security_invoker) or as their owner?
--      RED FLAG: a view over an RLS table without security_invoker=true that anon/authenticated can read.
select c.relname as view_name, c.relkind, c.reloptions,
       coalesce(c.reloptions::text ~ 'security_invoker=(true|on|1)', false) as security_invoker,
       pg_get_userbyid(c.relowner) as owner,
       has_table_privilege('anon', c.oid, 'SELECT') as anon_select,
       has_table_privilege('authenticated', c.oid, 'SELECT') as auth_select
from pg_class c join pg_namespace n on n.oid = c.relnamespace
where n.nspname = 'public' and c.relkind in ('v', 'm')
order by 1;

-- Q12. COUNTS ONLY: how many profiles expose an email address through public columns.
select count(*)                                                        as profiles,
       count(*) filter (where p.username ilike '%@%')                  as username_looks_like_email,
       count(*) filter (where lower(p.username) = lower(u.email))      as username_equals_auth_email,
       count(*) filter (where p.full_name is null or btrim(p.full_name) = '') as no_full_name
from public.profiles p
left join auth.users u on u.id = p.id;
-- If Q7 lists an `email` column, also run:
-- select count(*) filter (where email is not null) as profiles_with_email_column from public.profiles;

-- Q13. Anonymous content: rows marked anonymous that still carry a user_id.
select 'discoveries' as tbl, count(*) filter (where is_anonymous) as anonymous_rows,
       count(*) filter (where is_anonymous and user_id is not null) as anonymous_rows_with_user_id
from public.discoveries;
-- and, if Q2 shows discovery_responses exists:
-- select count(*) filter (where is_anonymous) as anonymous_rows,
--        count(*) filter (where is_anonymous and user_id is not null) as with_user_id
-- from public.discovery_responses;

-- Q14. Default privileges in public (shows whether the 2026-10-30 Data API change has been
--      applied: after it, new tables get NO grants for anon/authenticated/service_role).
select pg_get_userbyid(d.defaclrole) as for_role, n.nspname as schema,
       case d.defaclobjtype when 'r' then 'tables' when 'S' then 'sequences'
                            when 'f' then 'functions' else d.defaclobjtype::text end as object_type,
       d.defaclacl
from pg_default_acl d left join pg_namespace n on n.oid = d.defaclnamespace
where n.nspname = 'public' or d.defaclnamespace = 0
order by 1, 3;

-- Q15. Sequence privileges for authenticated (INSERTs through the API need USAGE).
select c.relname as sequence_name,
       has_sequence_privilege('authenticated', c.oid, 'USAGE') as auth_usage,
       has_sequence_privilege('anon', c.oid, 'USAGE') as anon_usage
from pg_class c join pg_namespace n on n.oid = c.relnamespace
where n.nspname = 'public' and c.relkind = 'S'
order by 1;

-- Q16. What an ANONYMOUS caller can actually read, row counts per table (runs the
--      real RLS as role anon inside a transaction that is rolled back).
--      Run these four lines together:
-- begin;
-- set local role anon;
-- select 'profiles' t, count(*) from public.profiles union all
-- select 'user_lists', count(*) from public.user_lists union all
-- select 'list_items', count(*) from public.list_items union all
-- select 'recent_items', count(*) from public.recent_items union all
-- select 'projects', count(*) from public.projects union all
-- select 'comments_private', count(*) from public.comments where is_public = false union all
-- select 'corrections_draft', count(*) from public.corrections where status = 'draft' union all
-- select 'discoveries_hidden', count(*) from public.discoveries where is_hidden = true union all
-- select 'published_joins_unpublished', count(*) from public.published_joins where is_published = false;
-- rollback;
--   Expected: profiles = all rows (by design today), every other count 0.
--   A non-zero private count is a live cross-user exposure.
--   "permission denied for table X" means anon has no SELECT grant on X (safe);
--   drop that line and re-run. The rollback undoes the role switch either way.
