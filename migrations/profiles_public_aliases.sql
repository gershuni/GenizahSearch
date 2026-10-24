-- ============================================================================
-- profiles: everyone has a public name (2026-10-24).
--
-- migrations/profiles_no_email_username.sql (2026-10-21) took the email
-- address out of profiles.username. 164 of 193 accounts then had neither a
-- username nor a full name, and every page that credits a contribution --
-- corrections, comments, joins, the citation of a corrected text -- showed
-- them as "Unknown". This file gives each of them an alias, and the website
-- then asks each person once how their name should appear.
--
--   1) profiles.public_name_confirmed (boolean, default false): true once the
--      person has chosen how their name appears -- a username or full name of
--      their own, or an answer to the website's prompt. The website asks only
--      while it is false and the full name is empty.
--   2) public.new_public_alias(): a name like "Curious Scribe" -- one of 40
--      adjectives and one of 40 Genizah-world trades and callings; on a clash
--      "of <city>" is added (24 cities of the Genizah world), then a number. Never
--      a username someone already has, in any letter case. Not callable
--      through the Data API.
--   3) handle_new_user: as 2026-10-21, plus an alias for every sign-up that
--      brings no usable username of its own, and public_name_confirmed set
--      when the person typed a username or a full name.
--   4) Existing profiles with an empty username get an alias. Each one is
--      recorded in private.profiles_alias_fix_20261024 (id, alias) first, so
--      it can be undone (see the end of this file). Full names are untouched.
--
-- The aliases are English and stored once, so the Hebrew site shows them in
-- English too. Every surface that names a contributor -- the website, every
-- installed desktop version -- already falls back from the full name to the
-- username, so they all show the alias with no code change.
--
-- DRY RUN BY DEFAULT. As written, the file does everything inside one
-- transaction, then reports and undoes it: the result is an error titled
-- "DRY RUN -- nothing was kept", with a sample of the aliases it would give.
-- To apply it, change 'on' to 'off' on the dry_run line below and run the
-- whole file again; the result is then a one-row summary.
--
-- Before and after: scripts/verify_profiles_public_aliases.sql (rolled back).
-- Expected before: the sign-up checks FAIL, after: all PASS.
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
               ('profiles', 'affiliation'), ('profiles', 'created_at'), ('user_lists', 'user_id'),
               ('user_lists', 'name'), ('user_lists', 'name_en'), ('user_lists', 'is_default')) as r(t, c)
  where not exists (select 1 from information_schema.columns ic
                    where ic.table_schema = 'public' and ic.table_name = r.t and ic.column_name = r.c);
  if missing is not null then
    raise exception 'aborted -- columns not found live: %', missing;
  end if;
end $$;

-- Sign-ups, sign-ins and profile edits wait while this file runs (about a
-- second): a sign-up under the old trigger after the scan below would leave a
-- profile without a name. auth.users first: the order a sign-up takes them.
-- Gives up after 15 s rather than queue behind something long; then simply
-- run the file again.
set local lock_timeout = '15s';
lock table auth.users, public.profiles in share row exclusive mode;


-- ============================================================================
-- 1) The column
-- ============================================================================
alter table public.profiles
  add column if not exists public_name_confirmed boolean not null default false;


-- ============================================================================
-- 2) The alias generator
-- ============================================================================
create or replace function public.new_public_alias()
returns text
language plpgsql
volatile
set search_path = ''
as $$
declare
  adjectives text[] := array[
    'Attentive', 'Bold', 'Bright', 'Candid', 'Careful', 'Cheerful', 'Clever', 'Curious',
    'Devoted', 'Diligent', 'Discerning', 'Eager', 'Earnest', 'Generous', 'Gentle', 'Hopeful',
    'Inquisitive', 'Intrepid', 'Keen', 'Kindly', 'Learned', 'Lively', 'Meticulous', 'Modest',
    'Nimble', 'Observant', 'Patient', 'Persistent', 'Quiet', 'Resourceful', 'Serene', 'Sharp-eyed',
    'Steady', 'Studious', 'Thoughtful', 'Tireless', 'Unhurried', 'Wandering', 'Watchful', 'Wise'];
  nouns text[] := array[
    'Apothecary', 'Archivist', 'Astronomer', 'Bookbinder', 'Bookseller', 'Calligrapher',
    'Cartographer', 'Cataloguer', 'Chronicler', 'Collector', 'Commentator', 'Copyist',
    'Correspondent', 'Decipherer', 'Glassmaker', 'Grammarian', 'Illuminator', 'Lexicographer',
    'Librarian', 'Linguist', 'Mathematician', 'Merchant', 'Navigator', 'Papermaker',
    'Perfumer', 'Philosopher', 'Physician', 'Poet', 'Puzzler', 'Reader', 'Scholar', 'Scribe',
    'Seeker', 'Silversmith', 'Storyteller', 'Student', 'Teacher', 'Translator', 'Traveler', 'Weaver'];
  places text[] := array[
    'Acre', 'Aden', 'Aleppo', 'Alexandria', 'Almeria', 'Baghdad', 'Cairo', 'Constantinople',
    'Cordoba', 'Damascus', 'Fez', 'Fustat', 'Jerusalem', 'Mangalore', 'Narbonne', 'Palermo',
    'Qayrawan', 'Ramla', 'Seville', 'Sijilmasa', 'Tiberias', 'Toledo', 'Tripoli', 'Tyre'];
  candidate text;
begin
  for attempt in 1..60 loop
    candidate := adjectives[1 + floor(random() * cardinality(adjectives))::int]
                 || ' ' || nouns[1 + floor(random() * cardinality(nouns))::int];
    if attempt > 20 then
      candidate := candidate || ' of ' || places[1 + floor(random() * cardinality(places))::int];
    end if;
    if attempt > 40 then
      candidate := candidate || ' ' || (2 + floor(random() * 998))::int;
    end if;
    if not exists (select 1 from public.profiles p where lower(p.username) = lower(candidate)) then
      return candidate;
    end if;
  end loop;
  -- Not reached in practice (1,600 names, then 38,400, then millions). A clash
  -- here is caught by the UNIQUE constraint and handle_new_user tries again.
  return candidate || ' ' || (100000 + floor(random() * 900000))::int;
end;
$$;

-- Only the new-user trigger (and this file) need it; nothing in the Data API.
revoke all on function public.new_public_alias() from public, anon, authenticated;


-- ============================================================================
-- 3) The new-user trigger
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
  v_confirmed boolean;
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

  -- A username or full name the person typed is their public name; otherwise
  -- they get an alias and the website asks them once.
  v_confirmed := v_username is not null or v_full_name is not null;
  v_username := coalesce(v_username, public.new_public_alias());

  begin
    insert into public.profiles (id, username, full_name, affiliation, public_name_confirmed)
    values (new.id, v_username, v_full_name, v_affiliation, v_confirmed);
  exception when unique_violation then
    -- The username was taken between the check and the insert (two sign-ups
    -- at once): a fresh alias. Any other clash (the id) raises again here and
    -- aborts, as before.
    insert into public.profiles (id, username, full_name, affiliation, public_name_confirmed)
    values (new.id, public.new_public_alias(), v_full_name, v_affiliation, v_full_name is not null);
  end;

  insert into public.user_lists (user_id, name, name_en, is_default)
  values (new.id, 'General', 'General', true);

  return new;
end;
$$;


-- ============================================================================
-- 4) Existing profiles
-- ============================================================================
create schema if not exists private;
revoke all on schema private from public, anon, authenticated;

create table if not exists private.profiles_alias_fix_20261024 (
  id uuid primary key,
  alias text not null,
  assigned_at timestamptz not null default now()
);
alter table private.profiles_alias_fix_20261024 enable row level security;
revoke all on private.profiles_alias_fix_20261024 from public, anon, authenticated;

-- A name of their own counts as chosen. Before the aliases, and never for an
-- alias this file gave (a second run of the file).
update public.profiles p
   set public_name_confirmed = true
 where not p.public_name_confirmed
   and (nullif(btrim(p.full_name), '') is not null or nullif(btrim(p.username), '') is not null)
   and not exists (select 1 from private.profiles_alias_fix_20261024 f where f.id = p.id);

-- One at a time, so each alias sees the ones given before it.
do $$
declare
  r record;
  v_alias text;
begin
  for r in select p.id from public.profiles p
           where nullif(btrim(p.username), '') is null
           order by p.created_at, p.id loop
    v_alias := public.new_public_alias();
    insert into private.profiles_alias_fix_20261024 (id, alias) values (r.id, v_alias)
    on conflict (id) do update set alias = excluded.alias, assigned_at = now();
    update public.profiles set username = v_alias where id = r.id;
  end loop;
end $$;


-- ============================================================================
-- 5) Checks and report. Any failed check aborts the whole file.
-- ============================================================================
do $$
declare
  n_empty integer;
  n_clash integer;
  n_aliases integer;
  n_ask integer;
  sample text;
  trigger_owner text;
begin
  select count(*) into n_empty from public.profiles where nullif(btrim(username), '') is null;
  select count(*) into n_clash
    from private.profiles_alias_fix_20261024 f
    join public.profiles p on lower(p.username) = lower(f.alias) and p.id <> f.id;
  select count(*) into n_aliases from private.profiles_alias_fix_20261024;
  select count(*) into n_ask from public.profiles
   where not public_name_confirmed and nullif(btrim(full_name), '') is null;
  select string_agg(alias, ', ' order by assigned_at, id) into sample
    from (select alias, assigned_at, id from private.profiles_alias_fix_20261024
          order by assigned_at, id limit 12) s;
  select pg_get_userbyid(proowner) into trigger_owner
    from pg_proc where oid = 'public.handle_new_user'::regproc;

  if n_empty <> 0 then
    raise exception 'aborted -- % profiles still have no username', n_empty;
  end if;
  if n_clash <> 0 then
    raise exception 'aborted -- % aliases clash with another username', n_clash;
  end if;
  if not exists (select 1 from pg_proc where oid = 'public.handle_new_user'::regproc
                 and array_to_string(proconfig, ',') ~ 'search_path=')
     or not exists (select 1 from pg_proc where oid = 'public.new_public_alias'::regproc
                    and array_to_string(proconfig, ',') ~ 'search_path=') then
    raise exception 'aborted -- a function has no pinned search_path';
  end if;
  -- handle_new_user runs as its owner: without this every sign-up would fail.
  if not has_function_privilege(trigger_owner, 'public.new_public_alias()', 'execute') then
    raise exception 'aborted -- %, the owner of handle_new_user, cannot run new_public_alias', trigger_owner;
  end if;
  if has_function_privilege('anon', 'public.new_public_alias()', 'execute')
     or has_function_privilege('authenticated', 'public.new_public_alias()', 'execute') then
    raise exception 'aborted -- new_public_alias is reachable through the Data API';
  end if;

  if current_setting('genizah.dry_run') = 'on' then
    raise exception E'DRY RUN -- nothing was kept. Would give % aliases (profiles left without a username: %); % accounts would be asked for a public name on the website. For example: %. To apply: set dry_run to ''off'' and run again.',
      n_aliases, n_empty, n_ask, coalesce(sample, '(none)');
  end if;
end $$;

commit;

select count(*)                                                as aliases_given,
       (select count(*) from public.profiles
         where nullif(btrim(username), '') is null)           as profiles_without_username,
       (select count(*) from public.profiles
         where not public_name_confirmed
           and nullif(btrim(full_name), '') is null)          as to_be_asked_on_the_website
from private.profiles_alias_fix_20261024;

-- ----------------------------------------------------------------------------
-- Undo (only if needed):
--   update public.profiles p set username = null
--     from private.profiles_alias_fix_20261024 f
--    where f.id = p.id and p.username = f.alias;
-- re-create handle_new_user from migrations/profiles_no_email_username.sql,
-- then: drop function public.new_public_alias();
-- The column can stay (the website reads it only when it is there); to drop
-- it: alter table public.profiles drop column public_name_confirmed;
-- When no longer needed: drop table private.profiles_alias_fix_20261024;
-- ----------------------------------------------------------------------------
