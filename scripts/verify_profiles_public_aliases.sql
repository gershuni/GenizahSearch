-- ============================================================================
-- Verify migrations/profiles_public_aliases.sql on the live project.
-- Paste the COMPLETE file into the SQL editor and run it. Run it BEFORE the
-- migration (the sign-up checks then FAIL: no alias, no public_name_confirmed)
-- and AFTER it (a pass: every check PASS, none SKIPPED).
--
-- It signs up eleven test accounts by inserting into auth.users, as Supabase
-- Auth does, so the installed handle_new_user runs; it reads the profile and
-- default list each one got, then checks the existing data and the alias
-- function. The report arrives as ONE final error titled "VERIFY RESULTS";
-- that is the normal outcome, and the error is what undoes every change: no
-- test account, profile or list is kept.
--
-- It replaces scripts/verify_profiles_no_email_username.sql, whose sign-ups
-- expect an empty username where there is now an alias.
--
-- What it cannot check: a real sign-up through Supabase Auth, and two
-- sign-ups taking one name at the same moment. After the migration, register
-- one account on the website and look at its profiles row.
-- ============================================================================

begin;

create temp table results (seq serial primary key, check_name text, expected text, got text);

-- What new_public_alias() makes: "Adjective Noun", then " of City", then " 123"
-- (and, past every retry, one more number).
create function pg_temp.alias_pattern()
returns text
language sql as $$
  select '^[A-Z][a-z]+(-[a-z]+)? [A-Z][a-z]+( of [A-Z][a-z]+)?( [0-9]+){0,2}$'
$$;

create function pg_temp.sign_up_as(p_label text, p_provider text, p_meta jsonb)
returns text
language plpgsql as $$
declare
  v_id uuid := gen_random_uuid();
  v_profile jsonb;
  v_lists integer;
  v_username text;
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
  -- As jsonb, so a missing public_name_confirmed reads as "(missing)" before
  -- the migration instead of stopping the script.
  select to_jsonb(p) into v_profile from public.profiles p where p.id = v_id;
  if v_profile is null then
    return 'NO PROFILE';
  end if;
  select count(*) into v_lists from public.user_lists where user_id = v_id and is_default;
  v_username := v_profile ->> 'username';
  return format('username=%s full_name=%s affiliation=%s confirmed=%s default_lists=%s',
                case when v_username ~ pg_temp.alias_pattern() then '(alias)'
                     else coalesce(v_username, '(empty)') end,
                coalesce(v_profile ->> 'full_name', '(empty)'),
                coalesce(v_profile ->> 'affiliation', '(empty)'),
                coalesce(v_profile ->> 'public_name_confirmed', '(missing)'),
                v_lists);
end $$;

create function pg_temp.expect_sign_up(p_name text, p_label text, p_provider text, p_meta jsonb,
                                       p_username text, p_full_name text, p_affiliation text,
                                       p_confirmed boolean)
returns void
language plpgsql as $$
begin
  insert into results (check_name, expected, got)
  values (p_name,
          format('username=%s full_name=%s affiliation=%s confirmed=%s default_lists=1',
                 coalesce(p_username, '(empty)'), coalesce(p_full_name, '(empty)'),
                 coalesce(p_affiliation, '(empty)'), p_confirmed),
          pg_temp.sign_up_as(p_label, p_provider, p_meta));
end $$;

-- ---------------------------------------------------------------- sign-ups
select pg_temp.expect_sign_up('S1 website sign-up (signup_profile)', 's1', 'email',
  '{"full_name": "Verify One", "affiliation": "Verify U", "signup_profile": {"username": "zzverify_one", "full_name": "Verify One", "affiliation": "Verify U"}}',
  'zzverify_one', 'Verify One', 'Verify U', true);
select pg_temp.expect_sign_up('S2 desktop sign-up (top-level fields)', 's2', 'email',
  '{"username": "zzverify_two", "full_name": "Verify Two", "affiliation": "Verify U2"}',
  'zzverify_two', 'Verify Two', 'Verify U2', true);
select pg_temp.expect_sign_up('S3 a name and no username: an alias, nothing to ask', 's3', 'email',
  '{"full_name": "Verify Three", "affiliation": "Verify U3"}',
  '(alias)', 'Verify Three', 'Verify U3', true);
select pg_temp.expect_sign_up('S4 Google sign-up: an alias, its claims not published, asked', 's4', 'google',
  '{"full_name": "Verify Google", "name": "Verify Google", "email": "verify-s4@genizahsearch.invalid", "avatar_url": "https://example.invalid/a.png"}',
  '(alias)', null, null, false);
select pg_temp.expect_sign_up('S5 a typed username that is an address', 's5', 'email',
  '{"signup_profile": {"username": "someone@example.invalid"}}',
  '(alias)', null, null, false);
select pg_temp.expect_sign_up('S6 a taken username (other case): an alias, the name kept', 's6', 'email',
  '{"signup_profile": {"username": "ZZVERIFY_ONE", "full_name": "Verify Six"}}',
  '(alias)', 'Verify Six', null, true);
select pg_temp.expect_sign_up('S7 no metadata', 's7', 'email', '{}', '(alias)', null, null, false);
select pg_temp.expect_sign_up('S8 blank username, malformed signup_profile', 's8', 'email',
  '{"signup_profile": "not an object", "username": "   "}', '(alias)', null, null, false);
select pg_temp.expect_sign_up('S9 signup_profile wins over the top level', 's9', 'email',
  '{"username": "zzverify_nine_top", "signup_profile": {"username": "zzverify_nine"}}',
  'zzverify_nine', null, null, true);
select pg_temp.expect_sign_up('S10 surrounding spaces are trimmed', 's10', 'email',
  '{"signup_profile": {"username": "  zzverify_ten  ", "full_name": "  Verify Ten "}}',
  'zzverify_ten', 'Verify Ten', null, true);

-- S11: a typed username equal (in another case) to an alias someone already has.
do $$
declare
  v_alias text;
begin
  select username into v_alias from public.profiles where username ~ pg_temp.alias_pattern() limit 1;
  if v_alias is null then
    insert into results (check_name, expected, got)
    values ('S11 a typed username that is someone''s alias', 'PASS', 'SKIPPED (no alias yet: the migration has not run)');
  else
    perform pg_temp.expect_sign_up('S11 a typed username that is someone''s alias', 's11', 'email',
      jsonb_build_object('signup_profile', jsonb_build_object('username', lower(v_alias))),
      '(alias)', null, null, false);
  end if;
end $$;

-- ---------------------------------------------------------------- data and functions
insert into results (check_name, expected, got)
select 'D1 profiles without a username', '0', count(*)::text
from public.profiles where nullif(btrim(username), '') is null;

insert into results (check_name, expected, got)
select 'D2 usernames containing ''@''', '0', count(*)::text
from public.profiles where position('@' in username) > 0;

insert into results (check_name, expected, got)
select 'D3 usernames shared by two accounts (any letter case)', '0', count(*)::text
from (select lower(username) from public.profiles where username is not null
      group by lower(username) having count(*) > 1) d;

do $$
declare
  v_got text;
begin
  if to_regclass('private.profiles_alias_fix_20261024') is null then
    v_got := 'SKIPPED (no fix table: the migration has not run)';
  else
    execute $q$
      select count(*)::text from private.profiles_alias_fix_20261024 f
      join public.profiles p on p.id = f.id
      where p.username is distinct from f.alias
    $q$ into v_got;
  end if;
  insert into results (check_name, expected, got)
  values ('D4 every alias given is still that account''s username', '0', v_got);
end $$;

insert into results (check_name, expected, got)
select 'F1 handle_new_user pins its search_path', 'true',
       coalesce(array_to_string(proconfig, ',') ~ 'search_path=', false)::text
from pg_proc where oid = 'public.handle_new_user'::regproc;

do $$
declare
  fn regprocedure := to_regprocedure('public.new_public_alias()');
  v_owner text;
  v_bad integer := 0;
  v_alias text;
begin
  if fn is null then
    insert into results (check_name, expected, got) values
      ('F2 new_public_alias pins its search_path', 'true', 'SKIPPED (no function: the migration has not run)'),
      ('F3 anon and authenticated cannot run new_public_alias', 'false', 'SKIPPED (no function)'),
      ('F4 the owner of handle_new_user can run new_public_alias', 'true', 'SKIPPED (no function)'),
      ('F5 200 aliases: none malformed or taken', '0', 'SKIPPED (no function)');
    return;
  end if;
  insert into results (check_name, expected, got)
  select 'F2 new_public_alias pins its search_path', 'true',
         coalesce(array_to_string(proconfig, ',') ~ 'search_path=', false)::text
  from pg_proc where oid = fn;
  insert into results (check_name, expected, got)
  values ('F3 anon and authenticated cannot run new_public_alias', 'false',
          (has_function_privilege('anon', fn, 'execute')
           or has_function_privilege('authenticated', fn, 'execute'))::text);
  select pg_get_userbyid(proowner) into v_owner from pg_proc where oid = 'public.handle_new_user'::regproc;
  insert into results (check_name, expected, got)
  values ('F4 the owner of handle_new_user can run new_public_alias', 'true',
          has_function_privilege(v_owner, fn, 'execute')::text);
  for i in 1..200 loop
    v_alias := public.new_public_alias();
    if v_alias !~ pg_temp.alias_pattern()
       or exists (select 1 from public.profiles p where lower(p.username) = lower(v_alias)) then
      v_bad := v_bad + 1;
    end if;
  end loop;
  insert into results (check_name, expected, got)
  values ('F5 200 aliases: none malformed or taken', '0', v_bad::text);
end $$;

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
