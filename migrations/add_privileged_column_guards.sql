-- Migration: Hardening -- server-side guards on privilege and moderation columns
-- Run this in Supabase SQL Editor (the complete file; it is one transaction).
-- Date: 2026-09-25 (applied to production the same day)
-- Verify with: scripts/verify_privileged_column_guards.sql
--
-- BEFORE INSERT OR UPDATE triggers on profiles, corrections, discoveries and
-- fragment_joins make the database enforce who may set which values, instead
-- of relying on the apps. They reject only an actual change, so a client that
-- resends an unchanged column is unaffected.
--
-- Exempt: callers whose current_user is not anon/authenticated (the SQL
-- editor, service_role, SECURITY DEFINER functions such as handle_new_user),
-- and callers whose own profile has role = 'admin'. A new SECURITY DEFINER
-- function that writes these tables must do its own role check.
--
-- Rules for roles user / reviewer / editor:
--   profiles        role and reputation never change; an INSERT must be role
--                   'user', reputation 0 (NULL reputation becomes 0).
--   corrections     INSERT: status draft or pending, or approved by an editor;
--                   never NULL; no review stamps. UPDATE: approved/merged/
--                   rejected rows are frozen (vote counters and updated_at
--                   excepted); an author may only submit a draft (draft ->
--                   pending, or draft -> approved as an editor); reviewing
--                   someone else's row is pending/under_review -> approved/
--                   rejected by a reviewer or editor and stamps reviewed_by /
--                   reviewed_at. DELETE: drafts only (section 5).
--   discoveries     INSERT: not hidden, not pinned, status active (NULLs filled).
--                   UPDATE: no pin changes; the author may hide their own row
--                   (the desktop soft delete) but not un-hide it; status only
--                   between active and answered. Editing the content of a
--                   pinned or featured discovery removes the pin / the
--                   featured status.
--   fragment_joins  INSERT: status proposed (NULL filled), no confirmation
--                   fields. UPDATE: status / confirmed_by / confirmed_at fixed;
--                   changing the fragments, type, confidence or evidence of a
--                   confirmed or rejected join returns it to proposed.
--
-- Rollback (keep guard_profile_privileges in place):
--   drop trigger if exists guard_correction_privileges on public.corrections;
--   drop trigger if exists guard_discovery_privileges on public.discoveries;
--   drop trigger if exists guard_fragment_join_privileges on public.fragment_joins;
--   drop function if exists public.guard_correction_privileges();
--   drop function if exists public.guard_discovery_privileges();
--   drop function if exists public.guard_fragment_join_privileges();

begin;

-- 0) Pre-check. The live schema has drifted from the repo before
--    (discoveries.is_answered exists live but in no tracked CREATE TABLE). A
--    trigger that names a column the live table lacks errors on EVERY write
--    to that table, so refuse to install anything unless every column the
--    guards read exists.
do $$
declare
  missing text;
begin
  select string_agg(format('%s.%s', r.t, r.c), ', ') into missing
  from (values
    ('profiles', 'role'), ('profiles', 'reputation'),
    ('corrections', 'status'), ('corrections', 'reviewed_by'), ('corrections', 'reviewed_at'),
    ('corrections', 'author_id'),
    ('discoveries', 'is_hidden'), ('discoveries', 'is_pinned'), ('discoveries', 'status'),
    ('discoveries', 'user_id'),
    ('fragment_joins', 'status'), ('fragment_joins', 'confirmed_by'), ('fragment_joins', 'confirmed_at')
  ) as r(t, c)
  where not exists (
    select 1 from information_schema.columns ic
    where ic.table_schema = 'public' and ic.table_name = r.t and ic.column_name = r.c
  );
  if missing is not null then
    raise exception 'guard migration aborted -- columns not found live: %', missing;
  end if;
end $$;


-- ============================================================
-- 1) profiles.role / profiles.reputation -> admin only
-- ============================================================
create or replace function public.guard_profile_privileges()
returns trigger
language plpgsql
security invoker
set search_path = public, pg_temp
as $$
begin
  if current_user not in ('anon', 'authenticated') then
    return new;
  end if;

  if exists (select 1 from public.profiles p where p.id = auth.uid() and p.role = 'admin') then
    return new;
  end if;

  if tg_op = 'INSERT' then
    -- NULL is never "allowed by default": a missing reputation becomes 0, a
    -- missing or other role is refused.
    new.reputation := coalesce(new.reputation, 0);
    if new.role is distinct from 'user' or new.reputation <> 0 then
      raise exception 'not authorized to set this value' using errcode = '42501';
    end if;
    return new;
  end if;

  if new.role is distinct from old.role or new.reputation is distinct from old.reputation then
    raise exception 'not authorized to set this value' using errcode = '42501';
  end if;

  return new;
end;
$$;

revoke all on function public.guard_profile_privileges() from public;

drop trigger if exists guard_profile_privileges on public.profiles;
create trigger guard_profile_privileges
  before insert or update on public.profiles
  for each row execute function public.guard_profile_privileges();


-- ============================================================
-- 2) corrections.status / reviewed_by / reviewed_at
-- ============================================================
create or replace function public.guard_correction_privileges()
returns trigger
language plpgsql
security invoker
set search_path = public, pg_temp
as $$
declare
  caller_role text;
  is_owner boolean;
begin
  if current_user not in ('anon', 'authenticated') then
    return new;
  end if;

  select p.role into caller_role from public.profiles p where p.id = auth.uid();
  caller_role := coalesce(caller_role, 'user');  -- no profile row = no privileges

  if caller_role = 'admin' then
    return new;
  end if;

  if tg_op = 'INSERT' then
    -- Submitters create drafts or pending rows; only an editor may publish at
    -- creation (web/pages/search_results.py:1363, browse.py:1629). NULL refused.
    if new.status is null
       or not (new.status in ('draft', 'pending') or (new.status = 'approved' and caller_role = 'editor')) then
      raise exception 'not authorized to set this value' using errcode = '42501';
    end if;
    if new.reviewed_by is not null or new.reviewed_at is not null then
      raise exception 'not authorized to set this value' using errcode = '42501';
    end if;
    return new;
  end if;

  -- UPDATE.
  -- (1) A reviewed row is frozen for every non-admin, reviewers and editors
  --     included, so approved text is always the text that was reviewed. The
  --     vote counters (desktop votes write them on this row) and updated_at
  --     (its own trigger fires after this one) are the only exceptions.
  if old.status in ('approved', 'merged', 'rejected')
     and (to_jsonb(new) - array['updated_at', 'upvotes', 'downvotes'])
         is distinct from (to_jsonb(old) - array['updated_at', 'upvotes', 'downvotes']) then
    raise exception 'not authorized to set this value' using errcode = '42501';
  end if;

  is_owner := coalesce(old.author_id = auth.uid(), false);

  -- (2) Status transitions. An author may only submit a draft: draft -> pending
  --     for everyone (web browse.py:1634 / search_results.py:1371, desktop
  --     supabase_corrections_client.py:1856), draft -> approved for an editor.
  --     Nothing moves a submitted row back to draft (the DELETE policy allows
  --     drafts only). Reviewing someone else's row is
  --     pending/under_review -> approved/rejected, reviewer or editor only --
  --     reachable only once an RLS policy lets them update it.
  if new.status is distinct from old.status then
    if new.status is null then
      raise exception 'not authorized to set this value' using errcode = '42501';
    end if;
    if is_owner then
      if not coalesce(old.status = 'draft'
                      and (new.status = 'pending' or (new.status = 'approved' and caller_role = 'editor')), false) then
        raise exception 'not authorized to set this value' using errcode = '42501';
      end if;
    else
      if not coalesce(caller_role in ('reviewer', 'editor')
                      and old.status in ('pending', 'under_review')
                      and new.status in ('approved', 'rejected'), false) then
        raise exception 'not authorized to set this value' using errcode = '42501';
      end if;
      -- An authorised review always records who and when (the Review tab sends
      -- neither, web/pages/corrections.py:821,836).
      new.reviewed_by := auth.uid();
      new.reviewed_at := now();
    end if;
  end if;

  -- (3) Review stamps: only with a review decision on someone else's row, only
  --     naming the caller, and the time is always the server's.
  if new.reviewed_by is distinct from old.reviewed_by or new.reviewed_at is distinct from old.reviewed_at then
    if is_owner
       or caller_role not in ('reviewer', 'editor')
       or new.status is not distinct from old.status
       or new.reviewed_by is distinct from auth.uid() then
      raise exception 'not authorized to set this value' using errcode = '42501';
    end if;
    new.reviewed_at := now();
  end if;

  return new;
end;
$$;

revoke all on function public.guard_correction_privileges() from public;

drop trigger if exists guard_correction_privileges on public.corrections;
create trigger guard_correction_privileges
  before insert or update on public.corrections
  for each row execute function public.guard_correction_privileges();


-- ============================================================
-- 3) discoveries.is_hidden / is_pinned / status
-- ============================================================
create or replace function public.guard_discovery_privileges()
returns trigger
language plpgsql
security invoker
set search_path = public, pg_temp
as $$
declare
  caller_role text;
begin
  if current_user not in ('anon', 'authenticated') then
    return new;
  end if;

  select p.role into caller_role from public.profiles p where p.id = auth.uid();

  if caller_role = 'admin' then
    return new;
  end if;

  if tg_op = 'INSERT' then
    -- Fill NULLs with the schema defaults (so a missing live default can never
    -- break a legitimate create), then refuse anything else.
    new.is_hidden := coalesce(new.is_hidden, false);
    new.is_pinned := coalesce(new.is_pinned, false);
    new.status := coalesce(new.status, 'active');
    if new.is_hidden or new.is_pinned or new.status <> 'active' then
      raise exception 'not authorized to set this value' using errcode = '42501';
    end if;
    return new;
  end if;

  if new.is_pinned is distinct from old.is_pinned then
    raise exception 'not authorized to set this value' using errcode = '42501';
  end if;

  if new.is_hidden is distinct from old.is_hidden then
    -- The author may hide their own row (the desktop "Delete" is a soft delete,
    -- desktop/supabase_corrections_client.py:1915-1924). Un-hiding, or touching
    -- anyone else's row, is admin-only. A NULL anywhere means "not allowed".
    if not coalesce(old.user_id = auth.uid() and old.is_hidden is not true and new.is_hidden is true, false) then
      raise exception 'not authorized to set this value' using errcode = '42501';
    end if;
  end if;

  -- The author may toggle answered/active (desktop mark_discovery_answered);
  -- closed/featured are set, and undone, by admins only. A legacy NULL status
  -- counts as active.
  if new.status is distinct from old.status then
    if not coalesce(coalesce(old.status, 'active') in ('active', 'answered')
                    and new.status in ('active', 'answered'), false) then
      raise exception 'not authorized to set this value' using errcode = '42501';
    end if;
  end if;

  -- A pin or a featured status is an admin's endorsement of the CONTENT. If the
  -- author changes the content (anything but the counters, the flags guarded
  -- above and the answered state), the endorsement comes off; an admin can put
  -- it back after looking.
  if (coalesce(old.is_pinned, false) or coalesce(old.status, '') = 'featured')
     and (to_jsonb(new) - array['updated_at', 'upvotes', 'downvotes', 'view_count', 'is_answered',
                                'status', 'is_hidden', 'is_pinned'])
         is distinct from
         (to_jsonb(old) - array['updated_at', 'upvotes', 'downvotes', 'view_count', 'is_answered',
                                'status', 'is_hidden', 'is_pinned']) then
    new.is_pinned := false;
    if coalesce(old.status, '') = 'featured' then
      new.status := 'active';
    end if;
  end if;

  return new;
end;
$$;

revoke all on function public.guard_discovery_privileges() from public;

drop trigger if exists guard_discovery_privileges on public.discoveries;
create trigger guard_discovery_privileges
  before insert or update on public.discoveries
  for each row execute function public.guard_discovery_privileges();


-- ============================================================
-- 4) fragment_joins.status / confirmed_by / confirmed_at
-- ============================================================
create or replace function public.guard_fragment_join_privileges()
returns trigger
language plpgsql
security invoker
set search_path = public, pg_temp
as $$
declare
  caller_role text;
begin
  if current_user not in ('anon', 'authenticated') then
    return new;
  end if;

  select p.role into caller_role from public.profiles p where p.id = auth.uid();

  if caller_role = 'admin' then
    return new;
  end if;

  if tg_op = 'INSERT' then
    new.status := coalesce(new.status, 'proposed');
    if new.status <> 'proposed' or new.confirmed_by is not null or new.confirmed_at is not null then
      raise exception 'not authorized to set this value' using errcode = '42501';
    end if;
    return new;
  end if;

  if new.status is distinct from old.status
     or new.confirmed_by is distinct from old.confirmed_by
     or new.confirmed_at is distinct from old.confirmed_at then
    raise exception 'not authorized to set this value' using errcode = '42501';
  end if;

  -- A confirmation (or rejection) is a decision about THIS pair of fragments.
  -- If the owner changes what the join asserts, it goes back to proposed.
  -- Notes are commentary and do not reset it.
  if old.status in ('confirmed', 'rejected')
     and (select jsonb_object_agg(k, to_jsonb(new) -> k)
            from unnest(array['fragment_a_sys_id', 'fragment_a_shelfmark', 'fragment_b_sys_id',
                              'fragment_b_shelfmark', 'join_type', 'confidence', 'evidence']) as k)
         is distinct from
         (select jsonb_object_agg(k, to_jsonb(old) -> k)
            from unnest(array['fragment_a_sys_id', 'fragment_a_shelfmark', 'fragment_b_sys_id',
                              'fragment_b_shelfmark', 'join_type', 'confidence', 'evidence']) as k) then
    new.status := 'proposed';
    new.confirmed_by := null;
    new.confirmed_at := null;
  end if;

  return new;
end;
$$;

revoke all on function public.guard_fragment_join_privileges() from public;

drop trigger if exists guard_fragment_join_privileges on public.fragment_joins;
create trigger guard_fragment_join_privileges
  before insert or update on public.fragment_joins
  for each row execute function public.guard_fragment_join_privileges();


-- ============================================================
-- 5) corrections DELETE: drafts only. An older policy allowed deleting an own
--    correction in any status; the draft-only policy ("Users can delete own
--    draft corrections", supabase_setup.sql) stays. The app offers Delete only
--    for drafts, or to admins (web/pages/corrections.py).
-- ============================================================
drop policy if exists "Users can delete own corrections" on public.corrections;

commit;
