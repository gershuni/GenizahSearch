-- ============================================================================
-- profiles: a public username is never an email address (2026-10-21).
--
-- handle_new_user copied NEW.email into profiles.username for every new
-- account, and profiles is readable by the Data API's anon role, so the
-- username doubled as the address (live 2026-10-21: 187 of 191 profiles).
--
--   1) handle_new_user: for an email/password sign-up, the username, full name
--      and affiliation the person typed into the sign-up form -- the website
--      sends them under raw_user_meta_data.signup_profile, the desktop app and
--      older websites at the top level -- else NULL. Never an address, never a
--      username someone already has (profiles.username is UNIQUE: a clash
--      would abort the sign-up). A Google (or other provider) sign-up gets
--      NULL: its metadata is the provider's claims, not something the person
--      chose to publish here.
--   2) Existing profiles whose username contains '@': the username typed at
--      sign-up where the metadata has one and nobody else has it (oldest
--      account first on a tie), else NULL. Every change is recorded in
--      private.profiles_username_fix_20261021 (id, old, new) first, so it can
--      be undone (see the end of this file).
--
-- DRY RUN BY DEFAULT. As written, the file does everything inside one
-- transaction, then reports and undoes it: the result is an error titled
-- "DRY RUN -- nothing was kept". To apply it, change 'on' to 'off' on the
-- dry_run line below and run the whole file again; the result is then a
-- one-row summary.
--
-- Before and after: scripts/verify_profiles_no_email_username.sql (rolled
-- back). Expected before: the sign-up checks FAIL (the old trigger), after:
-- all PASS.
--
-- Usernames only. Full names stay as they are: an account's metadata
-- full_name may be Google's (a Google sign-in merges its claims into the
-- metadata), and publishing that is a separate decision.
-- ============================================================================

begin;

select set_config('genizah.dry_run', 'on', true);  -- 'off' applies the change

-- 0) Pre-check: refuse to install anything if a column this file writes is
--    missing live (the live schema has drifted from the repo before).
do $$
declare
  missing text;
begin
  select string_agg(format('%s.%s', r.t, r.c), ', ') into missing
  from (values ('profiles', 'id'), ('profiles', 'username'), ('profiles', 'full_name'),
               ('profiles', 'affiliation'), ('user_lists', 'user_id'), ('user_lists', 'name'),
               ('user_lists', 'name_en'), ('user_lists', 'is_default')) as r(t, c)
  where not exists (select 1 from information_schema.columns ic
                    where ic.table_schema = 'public' and ic.table_name = r.t and ic.column_name = r.c);
  if missing is not null then
    raise exception 'aborted -- columns not found live: %', missing;
  end if;
  -- The new trigger leaves the username empty for most sign-ups (every Google
  -- one): with a NOT NULL username live, every such sign-up would fail.
  if exists (select 1 from information_schema.columns
             where table_schema = 'public' and table_name = 'profiles'
               and column_name = 'username' and is_nullable = 'NO') then
    raise exception 'aborted -- profiles.username is NOT NULL live';
  end if;
end $$;

-- Sign-ups, sign-ins and profile edits wait while this file runs (about a
-- second). Without it a sign-up that started under the old trigger could
-- commit an email username after the scan below, and a username edit could
-- take a name this file is about to give back. auth.users first: the order a
-- sign-up takes them. Gives up after 15 s rather than queue behind something
-- long; then simply run the file again.
set local lock_timeout = '15s';
lock table auth.users, public.profiles in share row exclusive mode;


-- ============================================================================
-- 1) The new-user trigger
-- ============================================================================
create or replace function public.handle_new_user()
returns trigger
language plpgsql
security definer
set search_path = ''
as $$
declare
  meta jsonb := coalesce(new.raw_user_meta_data, '{}'::jsonb);
  typed jsonb := case when jsonb_typeof(meta -> 'signup_profile') = 'object'
                      then meta -> 'signup_profile' else '{}'::jsonb end;
  v_username text;
  v_full_name text;
  v_affiliation text;
begin
  if coalesce(new.raw_app_meta_data ->> 'provider', '') = 'email' then
    v_username    := left(nullif(btrim(coalesce(typed ->> 'username',    meta ->> 'username')),    ''), 100);
    v_full_name   := left(nullif(btrim(coalesce(typed ->> 'full_name',   meta ->> 'full_name')),   ''), 200);
    v_affiliation := left(nullif(btrim(coalesce(typed ->> 'affiliation', meta ->> 'affiliation')), ''), 200);
  end if;

  if v_username is not null
     and (position('@' in v_username) > 0
          or exists (select 1 from public.profiles p where lower(p.username) = lower(v_username))) then
    v_username := null;
  end if;

  begin
    insert into public.profiles (id, username, full_name, affiliation)
    values (new.id, v_username, v_full_name, v_affiliation);
  exception when unique_violation then
    -- The username was taken between the check and the insert (two sign-ups
    -- at once). Any other clash (the id) still aborts, as before.
    if v_username is null then
      raise;
    end if;
    insert into public.profiles (id, username, full_name, affiliation)
    values (new.id, null, v_full_name, v_affiliation);
  end;

  insert into public.user_lists (user_id, name, name_en, is_default)
  values (new.id, 'General', 'General', true);

  return new;
end;
$$;


-- ============================================================================
-- 2) Existing usernames that contain '@'
-- ============================================================================
create schema if not exists private;
revoke all on schema private from public, anon, authenticated;

create table if not exists private.profiles_username_fix_20261021 (
  id uuid primary key,
  old_username text not null,
  new_username text,
  changed_at timestamptz not null default now()
);
alter table private.profiles_username_fix_20261021 enable row level security;
revoke all on private.profiles_username_fix_20261021 from public, anon, authenticated;

with candidates as (
  select p.id, p.username as old_username, u.created_at,
         left(nullif(btrim(coalesce(
           case when jsonb_typeof(u.raw_user_meta_data -> 'signup_profile') = 'object'
                then u.raw_user_meta_data -> 'signup_profile' ->> 'username' end,
           u.raw_user_meta_data ->> 'username')), ''), 100) as typed
  from public.profiles p
  join auth.users u on u.id = p.id
  where position('@' in p.username) > 0
),
checked as (
  select c.*,
         case when c.typed is null or position('@' in c.typed) > 0 then null
              when exists (select 1 from public.profiles o
                           where o.id <> c.id and lower(o.username) = lower(c.typed)) then null
              else c.typed end as usable
  from candidates c
),
ranked as (
  select ch.*,
         row_number() over (partition by lower(ch.usable) order by ch.created_at, ch.id) as rn
  from checked ch
)
insert into private.profiles_username_fix_20261021 (id, old_username, new_username)
select id, old_username, case when usable is not null and rn = 1 then usable end
from ranked
on conflict (id) do nothing;

update public.profiles p
   set username = f.new_username
  from private.profiles_username_fix_20261021 f
 where f.id = p.id
   and p.username = f.old_username;


-- ============================================================================
-- 3) Checks and report. Any failed check aborts the whole file.
-- ============================================================================
do $$
declare
  n_left integer;
  n_rows integer;
  n_named integer;
  n_cleared integer;
  n_typed_unused integer;
  fn_config text[];
begin
  select count(*) into n_left from public.profiles where position('@' in username) > 0;
  select count(*), count(new_username), count(*) - count(new_username)
    into n_rows, n_named, n_cleared
    from private.profiles_username_fix_20261021;
  select count(*) into n_typed_unused
    from private.profiles_username_fix_20261021 f
    join auth.users u on u.id = f.id
   where f.new_username is null
     and nullif(btrim(coalesce(
           case when jsonb_typeof(u.raw_user_meta_data -> 'signup_profile') = 'object'
                then u.raw_user_meta_data -> 'signup_profile' ->> 'username' end,
           u.raw_user_meta_data ->> 'username')), '') is not null;
  select p.proconfig into fn_config from pg_proc p
   where p.oid = 'public.handle_new_user'::regproc;

  if n_left <> 0 then
    raise exception 'aborted -- % usernames still contain ''@''', n_left;
  end if;
  if not coalesce(array_to_string(fn_config, ',') ~ 'search_path=', false) then
    raise exception 'aborted -- handle_new_user has no pinned search_path';
  end if;

  if current_setting('genizah.dry_run') = 'on' then
    raise exception E'DRY RUN -- nothing was kept. Would change % usernames: % to the username typed at sign-up, % to empty (% of those had a typed username that was taken or contained ''@''). Usernames with ''@'' left: %. To apply: set dry_run to ''off'' and run again.',
      n_rows, n_named, n_cleared, n_typed_unused, n_left;
  end if;
end $$;

commit;

select count(*)                                   as usernames_changed,
       count(new_username)                        as set_to_typed_username,
       count(*) - count(new_username)             as set_to_empty,
       (select count(*) from public.profiles
         where position('@' in username) > 0)    as usernames_with_at_left
from private.profiles_username_fix_20261021;

-- ----------------------------------------------------------------------------
-- Undo (only if needed):
--   update public.profiles p set username = f.old_username
--     from private.profiles_username_fix_20261021 f
--    where f.id = p.id and p.username is not distinct from f.new_username;
-- and re-create the previous handle_new_user (supabase_setup.sql before
-- 2026-10-21: INSERT INTO public.profiles (id, username) VALUES (NEW.id, NEW.email)).
-- When no longer needed: drop table private.profiles_username_fix_20261021;
-- ----------------------------------------------------------------------------
