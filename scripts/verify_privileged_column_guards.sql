-- ============================================================================
-- Verify migrations/add_privileged_column_guards.sql on a live Supabase project.
-- Paste the COMPLETE file into the SQL editor and run it; if the editor warns
-- that the query creates tables without RLS, choose "Run without RLS" (the
-- three tables are session-only temp tables).
--
-- It picks four real profiles (an admin and three others, which it makes a
-- plain user, a reviewer and an editor inside its own transaction), creates
-- fixture rows with known states, and runs 68 checks as PostgREST would
-- (SET ROLE authenticated / anon plus request.jwt.claims). Each check runs in
-- its own sub-transaction that is always undone. Outcomes: BLOCKED (SQLSTATE
-- 42501 from a guard or an RLS WITH CHECK), NO ROWS (RLS filtered the row),
-- ALLOWED (n rows), WRONG VALUE (the write went through but stored the wrong
-- value), ERROR <sqlstate> for anything else, SKIPPED if the project has too
-- few profiles.
--
-- The report arrives as ONE final error titled "VERIFY RESULTS"; that is the
-- normal outcome, and the error is what undoes every table change (serial id
-- counters still advance). A pass: 0 skipped and every check matching.
--
-- Assumes the SELECT policies of 2026-09-25: corrections readable when
-- status <> 'draft' or own; discoveries when is_hidden = false or own.
-- ============================================================================

begin;

create temp table results (
  seq serial primary key,
  check_name text,
  expected text,
  got text
);
create temp table who (label text primary key, id uuid);
create temp table fx (name text primary key, id bigint);

-- ---------------------------------------------------------------- people
insert into who
select 'admin', (select id from public.profiles where role = 'admin' order by created_at limit 1);
insert into who
select v.label, (select id from public.profiles
                 where role is distinct from 'admin'
                 order by created_at, id offset v.n limit 1)
from (values ('plain', 0), ('reviewer', 1), ('editor', 2)) as v(label, n);

-- Temporary role changes (rolled back with everything else). Run as the
-- editor's own role, so the guards step aside.
update public.profiles set role = 'user'     where id = (select id from who where label = 'plain');
update public.profiles set role = 'reviewer' where id = (select id from who where label = 'reviewer');
update public.profiles set role = 'editor'   where id = (select id from who where label = 'editor');
-- A known starting reputation, so every reputation check really changes the value.
update public.profiles set reputation = 0
 where id in (select id from who where label in ('plain', 'reviewer', 'editor'));

-- ---------------------------------------------------------------- fixtures
create function pg_temp.fx_correction(p_name text, p_owner text, p_status text) returns void
language plpgsql as $$
declare v bigint;
begin
  insert into public.corrections (author_id, sys_id, corrected_text, status)
  values ((select id from who where label = p_owner), 'RLS-VERIFY-TEST', 'fixture ' || p_name, p_status)
  returning id into v;
  insert into fx values (p_name, v);
end $$;

create function pg_temp.fx_discovery(p_name text, p_owner text, p_hidden boolean, p_status text) returns void
language plpgsql as $$
declare v bigint;
begin
  insert into public.discoveries (user_id, title, content, is_hidden, is_pinned, status)
  values ((select id from who where label = p_owner), 'RLS-VERIFY-TEST', 'fixture ' || p_name, p_hidden, false, p_status)
  returning id into v;
  insert into fx values (p_name, v);
end $$;

do $$
begin
  if exists (select 1 from who where id is null) then
    return;  -- the checks will report SKIPPED
  end if;
  perform pg_temp.fx_correction('c_draft_plain',       'plain',    'draft');
  perform pg_temp.fx_correction('c_pending_plain',     'plain',    'pending');
  perform pg_temp.fx_correction('c_approved_plain',    'plain',    'approved');
  perform pg_temp.fx_correction('c_draft_reviewer',    'reviewer', 'draft');
  perform pg_temp.fx_correction('c_pending_reviewer',  'reviewer', 'pending');
  perform pg_temp.fx_correction('c_draft_editor',      'editor',   'draft');
  perform pg_temp.fx_correction('c_approved_editor',   'editor',   'approved');
  perform pg_temp.fx_correction('c_approved_reviewer', 'reviewer', 'approved');
  perform pg_temp.fx_correction('c_rejected_plain',    'plain',    'rejected');
  -- an approved row that carries review stamps, as the admin page leaves them
  update public.corrections
     set reviewed_by = (select id from who where label = 'admin'), reviewed_at = now()
   where id = (select id from fx where name = 'c_approved_plain');
  perform pg_temp.fx_discovery('d_visible_plain', 'plain',  false, 'active');
  perform pg_temp.fx_discovery('d_hidden_plain',  'plain',  true,  'active');
  perform pg_temp.fx_discovery('d_closed_plain',  'plain',  false, 'closed');
  perform pg_temp.fx_discovery('d_visible_other', 'editor', false, 'active');
  insert into public.fragment_joins (user_id, fragment_a_sys_id, fragment_b_sys_id, status)
  values ((select id from who where label = 'plain'), 'RLS-VERIFY-A', 'RLS-VERIFY-B', 'proposed');
  insert into fx select 'j_plain', max(id) from public.fragment_joins
   where fragment_a_sys_id = 'RLS-VERIFY-A' and user_id = (select id from who where label = 'plain');
end $$;

-- ---------------------------------------------------------------- the runner
-- In p_sql: {me} = the acting user's id, {u:<label>} = a person's id, {<name>} = a fixture id.
-- The SQL runs as authenticated, so it must not read the temp tables itself.
-- p_verify (optional) is a SELECT returning one boolean, run as the same user
-- right after p_sql: false or NULL reads WRONG VALUE, so an ALLOWED write is also
-- checked for what it stored. p_actor 'anon' runs with no signed-in user.
create function pg_temp.check_as(p_check text, p_actor text, p_expected text, p_sql text,
                                 p_verify text default null)
returns void language plpgsql as $$
declare
  v_uid uuid;
  v_sql text := p_sql;
  v_verify text := p_verify;
  v_ok boolean;
  v_n integer;
  v_got text;
  f record;
begin
  select id into v_uid from who where label = p_actor;
  if (v_uid is null and p_actor <> 'anon') or exists (select 1 from who where id is null) then
    insert into results (check_name, expected, got)
      values (p_check, p_expected, 'SKIPPED (not enough profiles in this project)');
    return;
  end if;
  for f in select name, id from fx loop
    v_sql := replace(v_sql, '{' || f.name || '}', f.id::text);
    v_verify := replace(v_verify, '{' || f.name || '}', f.id::text);
  end loop;
  v_sql := replace(v_sql, '{me}', coalesce(quote_literal(v_uid::text) || '::uuid', 'null::uuid'));
  v_verify := replace(v_verify, '{me}', coalesce(quote_literal(v_uid::text) || '::uuid', 'null::uuid'));
  for f in select label as name, id from who loop
    v_sql := replace(v_sql, '{u:' || f.name || '}', quote_literal(f.id::text) || '::uuid');
    v_verify := replace(v_verify, '{u:' || f.name || '}', quote_literal(f.id::text) || '::uuid');
  end loop;

  begin
    if p_actor = 'anon' then
      perform set_config('request.jwt.claims', json_build_object('role', 'anon')::text, true);
      set local role anon;
    else
      perform set_config('request.jwt.claims', json_build_object('sub', v_uid::text, 'role', 'authenticated')::text, true);
      set local role authenticated;
    end if;
    execute v_sql;
    get diagnostics v_n = row_count;
    if v_verify is not null then
      execute v_verify into v_ok;
      if v_ok is not true then
        raise exception using errcode = 'P0V02', message = v_n::text;
      end if;
    end if;
    -- Undo this check's own change: raise a private sentinel and catch it below.
    raise exception using errcode = 'P0V01', message = v_n::text;
  exception
    when sqlstate 'P0V01' then
      v_got := case when sqlerrm = '0' then 'NO ROWS' else 'ALLOWED (' || sqlerrm || ' row(s))' end;
    when sqlstate 'P0V02' then
      v_got := 'WRONG VALUE (the write went through, ' || sqlerrm || ' row(s), but stored the wrong value)';
    when insufficient_privilege then
      -- Only the guards' own message or an RLS WITH CHECK counts as a block; any
      -- other permission error (a missing grant, say) is reported as an error.
      v_got := case when sqlerrm in ('not authorized to set this value', 'not allowed')
                      or sqlerrm like 'new row violates row-level security policy%'
                    then 'BLOCKED (' || sqlerrm || ')'
                    else 'ERROR 42501: ' || sqlerrm end;
    when others then
      v_got := 'ERROR ' || sqlstate || ': ' || sqlerrm;
  end;
  -- The sub-transaction rollback also undid SET LOCAL ROLE and the claims.
  insert into results (check_name, expected, got) values (p_check, p_expected, v_got);
end $$;

-- ---------------------------------------------------------------- checks
-- profiles
select pg_temp.check_as('P1 user sets own role = admin', 'plain', 'BLOCKED',
  $q$update public.profiles set role = 'admin' where id = {me}$q$);
select pg_temp.check_as('P2 user sets own reputation', 'plain', 'BLOCKED',
  $q$update public.profiles set reputation = coalesce(reputation, 0) + 1 where id = {me}$q$);
select pg_temp.check_as('P3 user upserts own profile with role = admin', 'plain', 'BLOCKED',
  $q$insert into public.profiles (id, role) values ({me}, 'admin') on conflict (id) do update set role = excluded.role$q$);
select pg_temp.check_as('P4 user upserts own profile with role = user (desktop signup path)', 'plain', 'ALLOWED',
  $q$insert into public.profiles (id, role) values ({me}, 'user') on conflict (id) do update set role = excluded.role$q$,
  $v$select role = 'user' from public.profiles where id = {me}$v$);
select pg_temp.check_as('P5 user edits own full_name', 'plain', 'ALLOWED',
  $q$update public.profiles set full_name = coalesce(full_name, '') || ' (verify)' where id = {me}$q$,
  $v$select full_name like '% (verify)' from public.profiles where id = {me}$v$);
select pg_temp.check_as('P6 editor sets own role = admin', 'editor', 'BLOCKED',
  $q$update public.profiles set role = 'admin' where id = {me}$q$);
select pg_temp.check_as('P7 admin changes another user''s role', 'admin', 'ALLOWED',
  $q$update public.profiles set role = 'reviewer' where id = {u:plain}$q$);
select pg_temp.check_as('P8 admin bumps another user''s reputation', 'admin', 'ALLOWED',
  $q$update public.profiles set reputation = coalesce(reputation, 0) + 1 where id = {u:plain}$q$);

-- corrections: INSERT
select pg_temp.check_as('C1 user inserts pending', 'plain', 'ALLOWED',
  $q$insert into public.corrections (author_id, sys_id, corrected_text, status) values ({me}, 'RLS-VERIFY-TEST', 't', 'pending')$q$);
select pg_temp.check_as('C2 user inserts approved', 'plain', 'BLOCKED',
  $q$insert into public.corrections (author_id, sys_id, corrected_text, status) values ({me}, 'RLS-VERIFY-TEST', 't', 'approved')$q$);
select pg_temp.check_as('C3 user inserts status NULL', 'plain', 'BLOCKED',
  $q$insert into public.corrections (author_id, sys_id, corrected_text, status) values ({me}, 'RLS-VERIFY-TEST', 't', null)$q$);
select pg_temp.check_as('C4 user inserts with reviewed_by set', 'plain', 'BLOCKED',
  $q$insert into public.corrections (author_id, sys_id, corrected_text, status, reviewed_by) values ({me}, 'RLS-VERIFY-TEST', 't', 'pending', {me})$q$);
select pg_temp.check_as('C5 reviewer inserts approved', 'reviewer', 'BLOCKED',
  $q$insert into public.corrections (author_id, sys_id, corrected_text, status) values ({me}, 'RLS-VERIFY-TEST', 't', 'approved')$q$);
select pg_temp.check_as('C6 editor inserts approved (app self-publish)', 'editor', 'ALLOWED',
  $q$insert into public.corrections (author_id, sys_id, corrected_text, status) values ({me}, 'RLS-VERIFY-TEST', 't', 'approved')$q$);

-- corrections: author's own rows
select pg_temp.check_as('C7 user edits own draft text', 'plain', 'ALLOWED',
  $q$update public.corrections set corrected_text = 'edited', notes = 'n' where id = {c_draft_plain}$q$);
select pg_temp.check_as('C8 user submits own draft (draft -> pending)', 'plain', 'ALLOWED',
  $q$update public.corrections set status = 'pending' where id = {c_draft_plain}$q$);
select pg_temp.check_as('C9 reviewer submits own draft (draft -> pending)', 'reviewer', 'ALLOWED',
  $q$update public.corrections set status = 'pending' where id = {c_draft_reviewer}$q$);
select pg_temp.check_as('C10 editor publishes own draft (draft -> approved)', 'editor', 'ALLOWED',
  $q$update public.corrections set status = 'approved' where id = {c_draft_editor}$q$);
select pg_temp.check_as('C11 user approves own pending', 'plain', 'BLOCKED',
  $q$update public.corrections set status = 'approved' where id = {c_pending_plain}$q$);
select pg_temp.check_as('C12 reviewer approves own pending', 'reviewer', 'BLOCKED',
  $q$update public.corrections set status = 'approved' where id = {c_pending_reviewer}$q$);
select pg_temp.check_as('C13 user returns own pending to draft', 'plain', 'BLOCKED',
  $q$update public.corrections set status = 'draft' where id = {c_pending_plain}$q$);
select pg_temp.check_as('C14 user moves own approved back to pending', 'plain', 'BLOCKED',
  $q$update public.corrections set status = 'pending' where id = {c_approved_plain}$q$);
select pg_temp.check_as('C15 user rewrites own approved text', 'plain', 'BLOCKED',
  $q$update public.corrections set corrected_text = 'rewritten' where id = {c_approved_plain}$q$);
select pg_temp.check_as('C16 editor rewrites own approved text', 'editor', 'BLOCKED',
  $q$update public.corrections set corrected_text = 'rewritten' where id = {c_approved_editor}$q$);
select pg_temp.check_as('C17 user bumps vote counters on own approved (desktop votes)', 'plain', 'ALLOWED',
  $q$update public.corrections set upvotes = coalesce(upvotes, 0) + 1 where id = {c_approved_plain}$q$);
select pg_temp.check_as('C18 user sets reviewed_by on own pending', 'plain', 'BLOCKED',
  $q$update public.corrections set reviewed_by = {me} where id = {c_pending_plain}$q$);
select pg_temp.check_as('C19 user sets reviewed_at on own pending', 'plain', 'BLOCKED',
  $q$update public.corrections set reviewed_at = now() - interval '1 year' where id = {c_pending_plain}$q$);
select pg_temp.check_as('C20 user sets own pending status to NULL', 'plain', 'BLOCKED',
  $q$update public.corrections set status = null where id = {c_pending_plain}$q$);

select pg_temp.check_as('C26 editor submits own draft (draft -> pending)', 'editor', 'ALLOWED',
  $q$update public.corrections set status = 'pending' where id = {c_draft_editor}$q$);
select pg_temp.check_as('C27 reviewer rewrites own approved text', 'reviewer', 'BLOCKED',
  $q$update public.corrections set corrected_text = 'rewritten' where id = {c_approved_reviewer}$q$);
select pg_temp.check_as('C28 user resubmits own rejected (rejected -> pending)', 'plain', 'BLOCKED',
  $q$update public.corrections set status = 'pending' where id = {c_rejected_plain}$q$);
select pg_temp.check_as('C29 user rewrites own rejected text', 'plain', 'BLOCKED',
  $q$update public.corrections set corrected_text = 'rewritten' where id = {c_rejected_plain}$q$);
select pg_temp.check_as('C30 user clears the review stamp on own approved', 'plain', 'BLOCKED',
  $q$update public.corrections set reviewed_by = null where id = {c_approved_plain}$q$);
select pg_temp.check_as('C31 user hands own pending to another author (RLS)', 'plain', 'BLOCKED',
  $q$update public.corrections set author_id = {u:admin} where id = {c_pending_plain}$q$);
select pg_temp.check_as('C32 user inserts with reviewed_at set', 'plain', 'BLOCKED',
  $q$insert into public.corrections (author_id, sys_id, corrected_text, status, reviewed_at) values ({me}, 'RLS-VERIFY-TEST', 't', 'pending', now())$q$);
select pg_temp.check_as('C33 anonymous caller approves a pending correction', 'anon', 'NO ROWS',
  $q$update public.corrections set status = 'approved' where id = {c_pending_plain}$q$);

-- corrections: DELETE and other people's rows
select pg_temp.check_as('C21 user deletes own draft', 'plain', 'ALLOWED',
  $q$delete from public.corrections where id = {c_draft_plain}$q$);
select pg_temp.check_as('C22 user deletes own pending', 'plain', 'NO ROWS',
  $q$delete from public.corrections where id = {c_pending_plain}$q$);
select pg_temp.check_as('C23 user deletes own approved', 'plain', 'NO ROWS',
  $q$delete from public.corrections where id = {c_approved_plain}$q$);
select pg_temp.check_as('C24 reviewer approves someone else''s pending (RLS; Review tab gap)', 'reviewer', 'NO ROWS',
  $q$update public.corrections set status = 'approved' where id = {c_pending_plain}$q$);
select pg_temp.check_as('C25 admin approves a user''s pending', 'admin', 'ALLOWED',
  $q$update public.corrections set status = 'approved', reviewed_by = {me}, reviewed_at = now() where id = {c_pending_plain}$q$);

-- discoveries
select pg_temp.check_as('D1 user creates a discovery', 'plain', 'ALLOWED',
  $q$insert into public.discoveries (user_id, title, content) values ({me}, 'RLS-VERIFY-TEST', 't')$q$);
select pg_temp.check_as('D2 user creates a pinned discovery', 'plain', 'BLOCKED',
  $q$insert into public.discoveries (user_id, title, content, is_pinned) values ({me}, 'RLS-VERIFY-TEST', 't', true)$q$);
select pg_temp.check_as('D3 user creates a featured discovery', 'plain', 'BLOCKED',
  $q$insert into public.discoveries (user_id, title, content, status) values ({me}, 'RLS-VERIFY-TEST', 't', 'featured')$q$);
select pg_temp.check_as('D1b user creates a discovery with NULL flags and status', 'plain', 'ALLOWED',
  $q$insert into public.discoveries (user_id, title, content, is_hidden, is_pinned, status) values ({me}, 'RLS-VERIFY-NULLS', 't', null, null, null)$q$,
  $v$select is_hidden = false and is_pinned = false and status = 'active' from public.discoveries where user_id = {me} and title = 'RLS-VERIFY-NULLS' order by id desc limit 1$v$);
select pg_temp.check_as('D3b user creates a hidden discovery', 'plain', 'BLOCKED',
  $q$insert into public.discoveries (user_id, title, content, is_hidden) values ({me}, 'RLS-VERIFY-TEST', 't', true)$q$);
select pg_temp.check_as('D3c user creates a closed discovery', 'plain', 'BLOCKED',
  $q$insert into public.discoveries (user_id, title, content, status) values ({me}, 'RLS-VERIFY-TEST', 't', 'closed')$q$);
select pg_temp.check_as('D7b user pins own discovery through an upsert', 'plain', 'BLOCKED',
  $q$insert into public.discoveries (id, user_id, title, content) values ({d_visible_plain}, {me}, 'x', 'y') on conflict (id) do update set is_pinned = true$q$);
select pg_temp.check_as('D4 user hides own discovery (desktop delete)', 'plain', 'ALLOWED',
  $q$update public.discoveries set is_hidden = true where id = {d_visible_plain}$q$);
select pg_temp.check_as('D5 user un-hides own hidden discovery', 'plain', 'BLOCKED',
  $q$update public.discoveries set is_hidden = false where id = {d_hidden_plain}$q$);
select pg_temp.check_as('D6 user sets own hidden discovery to is_hidden NULL', 'plain', 'BLOCKED',
  $q$update public.discoveries set is_hidden = null where id = {d_hidden_plain}$q$);
select pg_temp.check_as('D7 user pins own discovery', 'plain', 'BLOCKED',
  $q$update public.discoveries set is_pinned = true where id = {d_visible_plain}$q$);
select pg_temp.check_as('D8 user features own discovery', 'plain', 'BLOCKED',
  $q$update public.discoveries set status = 'featured' where id = {d_visible_plain}$q$);
select pg_temp.check_as('D9 user marks own discovery answered (desktop)', 'plain', 'ALLOWED',
  $q$update public.discoveries set status = 'answered' where id = {d_visible_plain}$q$);
select pg_temp.check_as('D10 user reopens own closed discovery', 'plain', 'BLOCKED',
  $q$update public.discoveries set status = 'active' where id = {d_closed_plain}$q$);
select pg_temp.check_as('D11 user edits own discovery text', 'plain', 'ALLOWED',
  $q$update public.discoveries set content = 'edited' where id = {d_visible_plain}$q$);
select pg_temp.check_as('D12 user hides someone else''s discovery', 'plain', 'NO ROWS',
  $q$update public.discoveries set is_hidden = true where id = {d_visible_other}$q$);
select pg_temp.check_as('D13 admin pins a user''s discovery', 'admin', 'ALLOWED',
  $q$update public.discoveries set is_pinned = true where id = {d_visible_plain}$q$);

-- fragment_joins
select pg_temp.check_as('F1 user creates a join', 'plain', 'ALLOWED',
  $q$insert into public.fragment_joins (user_id, fragment_a_sys_id, fragment_b_sys_id) values ({me}, 'RLS-VERIFY-A', 'RLS-VERIFY-C')$q$);
select pg_temp.check_as('F2 user creates a confirmed join', 'plain', 'BLOCKED',
  $q$insert into public.fragment_joins (user_id, fragment_a_sys_id, fragment_b_sys_id, status) values ({me}, 'RLS-VERIFY-A', 'RLS-VERIFY-C', 'confirmed')$q$);
select pg_temp.check_as('F1b user creates a join with status NULL', 'plain', 'ALLOWED',
  $q$insert into public.fragment_joins (user_id, fragment_a_sys_id, fragment_b_sys_id, status) values ({me}, 'RLS-VERIFY-NULL', 'RLS-VERIFY-C', null)$q$,
  $v$select status = 'proposed' from public.fragment_joins where user_id = {me} and fragment_a_sys_id = 'RLS-VERIFY-NULL' order by id desc limit 1$v$);
select pg_temp.check_as('F2b user creates a join with confirmed_at set', 'plain', 'BLOCKED',
  $q$insert into public.fragment_joins (user_id, fragment_a_sys_id, fragment_b_sys_id, confirmed_at) values ({me}, 'RLS-VERIFY-A', 'RLS-VERIFY-C', now())$q$);
select pg_temp.check_as('F3b user confirms own join through an upsert', 'plain', 'BLOCKED',
  $q$insert into public.fragment_joins (id, user_id, fragment_a_sys_id, fragment_b_sys_id) values ({j_plain}, {me}, 'RLS-VERIFY-A', 'RLS-VERIFY-B') on conflict (id) do update set status = 'confirmed'$q$);
select pg_temp.check_as('F3 user confirms own join', 'plain', 'BLOCKED',
  $q$update public.fragment_joins set status = 'confirmed' where id = {j_plain}$q$);
select pg_temp.check_as('F4 user sets confirmed_by on own join', 'plain', 'BLOCKED',
  $q$update public.fragment_joins set confirmed_by = {me} where id = {j_plain}$q$);
select pg_temp.check_as('F4b user sets confirmed_at on own join', 'plain', 'BLOCKED',
  $q$update public.fragment_joins set confirmed_at = now() where id = {j_plain}$q$);
select pg_temp.check_as('F5 user edits own join notes', 'plain', 'ALLOWED',
  $q$update public.fragment_joins set notes = 'edited' where id = {j_plain}$q$);
select pg_temp.check_as('F6 admin confirms a user''s join', 'admin', 'ALLOWED',
  $q$update public.fragment_joins set status = 'confirmed', confirmed_by = {me}, confirmed_at = now() where id = {j_plain}$q$);

-- ---------------------------------------------------------------- report
-- Delivered as an ERROR: the SQL editor shows only the last statement's
-- outcome, and the error is what guarantees the rollback.
do $$
declare
  report text;
  n_all integer;
  n_skip integer;
  n_ok integer;
begin
  select count(*),
         count(*) filter (where got like 'SKIPPED%'),
         count(*) filter (where got not like 'SKIPPED%' and split_part(got, ' ', 1) = split_part(expected, ' ', 1))
    into n_all, n_skip, n_ok
    from results;
  select string_agg(format('%s | %s | expected: %s | got: %s', seq, check_name, expected, got), E'\n' order by seq)
    into report from results;
  raise exception E'VERIFY RESULTS -- % checks: % skipped, % as expected. A pass is 0 skipped and all % as expected. No table change was kept; serial id counters advanced.\n%',
    n_all, n_skip, n_ok, n_all, report;
end $$;

rollback;
