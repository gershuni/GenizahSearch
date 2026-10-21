"""migrations/profiles_no_email_username.sql (2026-10-21) lives in SQL the app
never runs, so nothing else would notice if it drifted. These checks pin what
a later edit is most likely to break: the new-user trigger never copies the
email address and pins its search_path, supabase_setup.sql installs the same
function, the migration is a dry run unless someone switches it, every
username it changes is recorded before it is changed, and the verification
script never commits.
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MIGRATION = ROOT / "migrations" / "profiles_no_email_username.sql"
VERIFY = ROOT / "scripts" / "verify_profiles_no_email_username.sql"
SETUP = ROOT / "supabase_setup.sql"


def _sql(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _without_comments(sql: str) -> str:
    return re.sub(r"--[^\n]*", "", sql)


def _handle_new_user(sql: str) -> str:
    m = re.search(r"create or replace function public\.handle_new_user\(\).*?\$\$;",
                  _without_comments(sql), re.S | re.I)
    assert m, "handle_new_user is not defined"
    return m.group(0)


def _normalized(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip().lower()


def test_the_trigger_never_copies_the_email_and_pins_its_search_path():
    fn = _handle_new_user(_sql(MIGRATION))
    assert "new.email" not in fn.lower(), "the address must never become a profile field"
    assert re.search(r"security definer\s+set search_path = ''", fn, re.I)
    assert "'signup_profile'" in fn and "'provider'" in fn


def test_supabase_setup_installs_the_same_trigger():
    assert _normalized(_handle_new_user(_sql(SETUP))) == _normalized(_handle_new_user(_sql(MIGRATION)))


def test_the_migration_is_a_dry_run_unless_switched():
    sql = _without_comments(_sql(MIGRATION))
    statements = [s.strip().lower() for s in sql.split(";") if s.strip()]
    assert statements[0] == "begin"
    assert "select set_config('genizah.dry_run', 'on', true)" in statements
    assert "current_setting('genizah.dry_run') = 'on'" in sql


def test_sign_ups_wait_until_the_new_trigger_and_the_scan_are_committed():
    """Codex round 3 (P1): a sign-up running the old trigger during the file
    could commit an email username after the scan. The lock comes before the
    function is replaced and before the scan, and lasts to the commit."""
    sql = _without_comments(_sql(MIGRATION)).lower()
    lock = sql.index("lock table auth.users, public.profiles in share row exclusive mode")
    assert lock < sql.index("create or replace function public.handle_new_user")
    assert lock < sql.index("with candidates as")
    assert "set local lock_timeout" in sql[:lock]


def test_every_username_change_is_recorded_before_it_is_made():
    sql = _without_comments(_sql(MIGRATION)).lower()
    record = sql.index("insert into private.profiles_username_fix_20261021")
    change = sql.index("update public.profiles p")
    assert record < change
    assert "revoke all on private.profiles_username_fix_20261021 from public, anon, authenticated" in sql
    assert "revoke all on schema private from public, anon, authenticated" in sql


def test_the_verification_script_never_commits():
    sql = _without_comments(_sql(VERIFY))
    statements = [s.strip().lower() for s in sql.split(";") if s.strip()]
    assert statements[0] == "begin" and statements[-1] == "rollback"
    assert not any(s == "commit" for s in statements)
    assert "raise exception e'verify results" in sql.lower()
