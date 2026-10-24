-- ============================================================================
-- Verify migrations/profiles_no_email_username.sql on the live project.
--
-- SUPERSEDED once migrations/profiles_public_aliases.sql (2026-10-24) has
-- run: a sign-up below that expects an empty username now gets an alias, so
-- those checks FAIL by design. Use scripts/verify_profiles_public_aliases.sql.
--
-- Paste the COMPLETE file into the SQL editor and run it. Run it BEFORE the
-- migration (the sign-up checks then FAIL: the old trigger copies the email)
-- and AFTER it (a pass: every check PASS, none SKIPPED).
--
-- It signs up ten test accounts by inserting into auth.users, as Supabase
-- Auth does, so the installed handle_new_user runs; it reads the profile and
-- default list each one got, then checks the existing data. The report
-- arrives as ONE final error titled "VERIFY RESULTS"; that is the normal
-- outcome, and the error is what undoes every change: no test account,
-- profile or list is kept.
--
-- What it cannot check: a real sign-up through Supabase Auth (that it puts
-- raw_app_meta_data.provider in the row it inserts, as these test rows do),
-- and the case of two sign-ups taking one username at the same moment. After
-- the migration, register one account on the website and look at its
-- profiles row.
-- ============================================================================

begin;

create temp table results (seq serial primary key, check_name text, expected text, got text);

create function pg_temp.sign_up_as(p_label text, p_provider text, p_meta jsonb)
returns text
language plpgsql as $$
declare
  v_id uuid := gen_random_uuid();
  v_profile record;
  v_lists integer;
begin
  begin
    insert into auth.users (id, instance_id, aud, role, email,
                            raw_app_meta_data, raw_user_meta_data, created_at, updated_at)
    values (v_id, '00000000-0000-0000-0000-000000000000', 'authenticated', 'authenticated',
            'verify-' || p_label || '@genizahsearch.invalid',
            jsonb_build_object('provider', p_provider, 'providers', jsonb_build_array(p_provider)),
            p_meta, now(), now());
  exception when others then
    return 'ERROR ' || sqlstate || ' ' || sqlerrm;
  end;
  select username, full_name, affiliation into v_profile from public.profiles where id = v_id;
  if not found then
    return 'NO PROFILE';
  end if;
  select count(*) into v_lists from public.user_lists where user_id = v_id and is_default;
  return format('username=%s full_name=%s affiliation=%s default_lists=%s',
                coalesce(v_profile.username, '(empty)'), coalesce(v_profile.full_name, '(empty)'),
                coalesce(v_profile.affiliation, '(empty)'), v_lists);
end $$;

create function pg_temp.expect_sign_up(p_name text, p_label text, p_provider text, p_meta jsonb,
                                       p_username text, p_full_name text, p_affiliation text)
returns void
language plpgsql as $$
begin
  insert into results (check_name, expected, got)
  values (p_name,
          format('username=%s full_name=%s affiliation=%s default_lists=1',
                 coalesce(p_username, '(empty)'), coalesce(p_full_name, '(empty)'),
                 coalesce(p_affiliation, '(empty)')),
          pg_temp.sign_up_as(p_label, p_provider, p_meta));
end $$;

-- ---------------------------------------------------------------- sign-ups
select pg_temp.expect_sign_up('S1 website sign-up (signup_profile)', 's1', 'email',
  '{"full_name": "Verify One", "affiliation": "Verify U", "signup_profile": {"username": "zzverify_one", "full_name": "Verify One", "affiliation": "Verify U"}}',
  'zzverify_one', 'Verify One', 'Verify U');
select pg_temp.expect_sign_up('S2 desktop sign-up (top-level fields)', 's2', 'email',
  '{"username": "zzverify_two", "full_name": "Verify Two", "affiliation": "Verify U2"}',
  'zzverify_two', 'Verify Two', 'Verify U2');
select pg_temp.expect_sign_up('S3 website before 2026-10-21 (name, no username)', 's3', 'email',
  '{"full_name": "Verify Three", "affiliation": "Verify U3"}',
  null, 'Verify Three', 'Verify U3');
select pg_temp.expect_sign_up('S4 Google sign-up: its claims are not published', 's4', 'google',
  '{"full_name": "Verify Google", "name": "Verify Google", "email": "verify-s4@genizahsearch.invalid", "avatar_url": "https://example.invalid/a.png"}',
  null, null, null);
select pg_temp.expect_sign_up('S5 a typed username that is an address', 's5', 'email',
  '{"signup_profile": {"username": "someone@example.invalid"}}',
  null, null, null);
select pg_temp.expect_sign_up('S6 a taken username (other case): sign-up still succeeds', 's6', 'email',
  '{"signup_profile": {"username": "ZZVERIFY_ONE", "full_name": "Verify Six"}}',
  null, 'Verify Six', null);
select pg_temp.expect_sign_up('S7 no metadata', 's7', 'email', '{}', null, null, null);
select pg_temp.expect_sign_up('S8 blank username, malformed signup_profile', 's8', 'email',
  '{"signup_profile": "not an object", "username": "   "}', null, null, null);
select pg_temp.expect_sign_up('S9 signup_profile wins over the top level', 's9', 'email',
  '{"username": "zzverify_nine_top", "signup_profile": {"username": "zzverify_nine"}}',
  'zzverify_nine', null, null);
select pg_temp.expect_sign_up('S10 surrounding spaces are trimmed', 's10', 'email',
  '{"signup_profile": {"username": "  zzverify_ten  ", "full_name": "  Verify Ten "}}',
  'zzverify_ten', 'Verify Ten', null);

-- ---------------------------------------------------------------- data and function
insert into results (check_name, expected, got)
select 'D1 existing usernames containing ''@''', '0', count(*)::text
from public.profiles
where position('@' in username) > 0
  and id not in (select id from auth.users where email like 'verify-%@genizahsearch.invalid');

do $$
declare
  v_got text;
begin
  if to_regclass('private.profiles_username_fix_20261021') is null then
    v_got := 'SKIPPED (no fix table: the migration has not run)';
  else
    execute $q$
      select count(*)::text from private.profiles_username_fix_20261021 f
      join public.profiles p on p.id = f.id
      where p.username is distinct from f.new_username
    $q$ into v_got;
  end if;
  insert into results (check_name, expected, got)
  values ('D2 every changed username is what the fix recorded', '0', v_got);
end $$;

insert into results (check_name, expected, got)
select 'F1 handle_new_user pins its search_path',
       'true',
       coalesce(array_to_string(proconfig, ',') ~ 'search_path=', false)::text
from pg_proc where oid = 'public.handle_new_user'::regproc;

-- ---------------------------------------------------------------- report
do $$
declare
  report text;
  n_all integer;
  n_pass integer;
  n_skip integer;
begin
  select count(*), count(*) filter (where got = expected), count(*) filter (where got like 'SKIPPED%')
    into n_all, n_pass, n_skip from results;
  select string_agg(format('%s | %s | %s | expected: %s | got: %s', seq,
                           case when got = expected then 'PASS'
                                when got like 'SKIPPED%' then 'SKIP' else 'FAIL' end,
                           check_name, expected, got), E'\n' order by seq)
    into report from results;
  raise exception E'VERIFY RESULTS -- % checks: % PASS, % SKIPPED. A pass after the migration is all % PASS. Nothing was kept.\n%',
    n_all, n_pass, n_skip, n_all, report;
end $$;

rollback;
