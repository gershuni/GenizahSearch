"""migrations/profiles_public_aliases.sql (2026-10-24) lives in SQL the app
never runs, so nothing else would notice if it drifted. These checks pin what a
later edit is most likely to break: every alias the generator can make is one
the verification script recognises, the new-user trigger still never copies the
email and now always leaves a name, supabase_setup.sql installs the same two
functions and the column, the migration is a dry run unless switched, sign-ups
wait for it, a chosen name is marked before any alias is given, every alias is
recorded before it is written, the generator is out of the Data API's reach,
and the verification script never commits.
"""
import itertools
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MIGRATION = ROOT / "migrations" / "profiles_public_aliases.sql"
VERIFY = ROOT / "scripts" / "verify_profiles_public_aliases.sql"
SETUP = ROOT / "supabase_setup.sql"


def _sql(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _without_comments(sql: str) -> str:
    return re.sub(r"--[^\n]*", "", sql)


def _function(sql: str, name: str) -> str:
    m = re.search(rf"create or replace function public\.{name}\(\).*?\$\$;",
                  _without_comments(sql), re.S | re.I)
    assert m, f"{name} is not defined"
    return m.group(0)


def _normalized(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip().lower()


def _words(name: str) -> list:
    m = re.search(rf"{name} text\[\] := array\[(.*?)\];", _function(_sql(MIGRATION), "new_public_alias"), re.S)
    assert m, f"no {name} list"
    return re.findall(r"'([^']*)'", m.group(1))


def test_the_word_lists_are_clean():
    adjectives, nouns, places = _words("adjectives"), _words("nouns"), _words("places")
    assert (len(adjectives), len(nouns), len(places)) == (40, 40, 24)
    for words in (adjectives, nouns, places):
        assert len(set(w.lower() for w in words)) == len(words), "a word is listed twice"
        assert all(re.fullmatch(r"[A-Z][a-z]+(-[a-z]+)?", w) for w in words), words


def test_every_alias_the_generator_can_make_is_recognised_by_the_verification():
    pattern = re.search(r"select '(\^.*?\$)'", _sql(VERIFY)).group(1)
    adjectives, nouns, places = _words("adjectives"), _words("nouns"), _words("places")
    for (a, n), p in zip(itertools.product(adjectives, nouns), itertools.cycle(places)):
        # Each tier of new_public_alias: the pair, " of City", " 123", the last resort's number.
        for alias in (f"{a} {n}", f"{a} {n} of {p}", f"{a} {n} of {p} 999", f"{a} {n} of {p} 999 123456"):
            assert re.fullmatch(pattern, alias), alias
    # And a typed username of the verification's own sign-ups is not mistaken for one.
    assert not re.fullmatch(pattern, "zzverify_one")


def test_the_trigger_never_copies_the_email_and_always_leaves_a_name():
    fn = _function(_sql(MIGRATION), "handle_new_user").lower()
    assert "new.email" not in fn, "the address must never become a profile field"
    assert re.search(r"security definer\s+set search_path = ''", fn)
    assert "v_username := coalesce(v_username, public.new_public_alias());" in fn
    loop = fn.split("for attempt in 1..5 loop", 1)[1].split("end loop;", 1)[0]
    insert, handler = loop.split("exception when unique_violation then", 1)
    assert "insert into public.profiles" in insert and "exit;" in insert
    assert "v_username := public.new_public_alias();" in handler, "a lost race must take a fresh alias"
    # Codex (PR #447): an insert inside the handler escapes it on a second clash
    # and aborts the sign-up; the retry must be the loop around the insert.
    assert "insert into" not in handler
    assert "if attempt = 5 or exists (select 1 from public.profiles p where p.id = new.id) then" in handler
    assert fn.count("public_name_confirmed") == 1


def test_the_alias_generator_is_out_of_the_data_apis_reach():
    sql = _without_comments(_sql(MIGRATION)).lower()
    fn = _function(_sql(MIGRATION), "new_public_alias").lower()
    assert "security definer" not in fn
    assert "set search_path = ''" in fn
    assert "revoke all on function public.new_public_alias() from public, anon, authenticated;" in sql


def test_supabase_setup_installs_the_same_functions_and_the_column():
    for name in ("handle_new_user", "new_public_alias"):
        assert _normalized(_function(_sql(SETUP), name)) == _normalized(_function(_sql(MIGRATION), name)), name
    setup = _without_comments(_sql(SETUP)).lower()
    table = setup.split("create table public.profiles (", 1)[1].split(");", 1)[0]
    assert "public_name_confirmed boolean not null default false" in table
    assert "revoke all on function public.new_public_alias() from public, anon, authenticated;" in setup


def test_the_migration_is_a_dry_run_unless_switched():
    sql = _without_comments(_sql(MIGRATION))
    statements = [s.strip().lower() for s in sql.split(";") if s.strip()]
    assert statements[0] == "begin"
    assert "select set_config('genizah.dry_run', 'on', true)" in statements
    assert "current_setting('genizah.dry_run') = 'on'" in sql


def test_sign_ups_wait_and_names_are_marked_before_aliases_are_given():
    sql = _without_comments(_sql(MIGRATION)).lower()
    lock = sql.index("lock table auth.users, public.profiles in share row exclusive mode")
    assert "set local lock_timeout" in sql[:lock]
    assert lock < sql.index("alter table public.profiles")
    assert lock < sql.index("create or replace function public.handle_new_user")
    marked = sql.index("set public_name_confirmed = true")
    aliases = sql.index("v_alias := public.new_public_alias()")
    assert lock < marked < aliases, "an alias must never count as a name the person chose"
    # ...nor on a second run of the file (Codex, PR #447): by then the trigger has
    # given aliases with no record here, so the marking runs only on the run
    # that adds the column, decided before the column is added.
    first_run = sql.index("select set_config('genizah.first_run'")
    assert first_run < sql.index("alter table public.profiles")
    update = sql[marked:aliases].split(";", 1)[0]
    assert "where current_setting('genizah.first_run')::boolean" in update


def test_every_alias_is_recorded_before_it_is_written():
    sql = _without_comments(_sql(MIGRATION)).lower()
    record = sql.index("insert into private.profiles_alias_fix_20261024 (id, alias)")
    change = sql.index("update public.profiles set username = v_alias")
    assert record < change
    assert "revoke all on private.profiles_alias_fix_20261024 from public, anon, authenticated" in sql
    assert "revoke all on schema private from public, anon, authenticated" in sql


def test_the_migration_refuses_a_trigger_owner_that_cannot_make_aliases():
    sql = _without_comments(_sql(MIGRATION)).lower()
    assert "has_function_privilege(trigger_owner, 'public.new_public_alias()', 'execute')" in sql


def test_the_verification_script_never_commits():
    sql = _without_comments(_sql(VERIFY))
    statements = [s.strip().lower() for s in sql.split(";") if s.strip()]
    assert statements[0] == "begin" and statements[-1] == "rollback"
    assert not any(s == "commit" for s in statements)
    assert "raise exception e'verify results" in sql.lower()
