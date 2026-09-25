"""The database hardening of 2026-09-25 lives in SQL the app never runs, so nothing else
would notice if it drifted. These checks pin the parts a later edit is most likely to break:
the migration installs all four guards in one transaction, the provisioning scripts do not
recreate the any-status correction DELETE policy, and the verification script still runs
every check inside a transaction it never commits.
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MIGRATION = ROOT / "migrations" / "add_privileged_column_guards.sql"
VERIFY = ROOT / "scripts" / "verify_privileged_column_guards.sql"
FIX_RLS = ROOT / "scripts" / "fix_rls_policies.sql"
SETUP = ROOT / "supabase_setup.sql"

GUARDS = {
    "guard_profile_privileges": "profiles",
    "guard_correction_privileges": "corrections",
    "guard_discovery_privileges": "discoveries",
    "guard_fragment_join_privileges": "fragment_joins",
}


def _sql(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _without_comments(sql: str) -> str:
    return re.sub(r"--[^\n]*", "", sql)


def _function_parts(sql: str, name: str) -> tuple:
    """(declaration up to AS $$, body) of one guard's CREATE FUNCTION, comments excluded."""
    m = re.search(rf"create or replace function public\.{name}\(\)(.*?)\bas\s+\$\$(.*?)\$\$;",
                  _without_comments(sql), re.S | re.I)
    assert m, f"{name} is not defined"
    return m.group(1), m.group(2)


def wide_correction_delete_policies(sql: str) -> list:
    """Names of corrections DELETE policies whose USING neither limits the status nor checks admin."""
    wide = []
    pattern = r'create policy "([^"]+)" on (?:public\.)?corrections\s+for delete\b(.*?);'
    for name, rest in re.findall(pattern, _without_comments(sql), re.S | re.I):
        using = re.search(r"using\s*\((.*)\)", rest, re.S | re.I)
        clause = using.group(1) if using else ""
        if "status" not in clause and "admin" not in clause:
            wide.append(name)
    return wide


def test_migration_installs_all_four_guards_in_one_transaction():
    sql = _without_comments(_sql(MIGRATION))
    statements = [s.strip().lower() for s in sql.split(";") if s.strip()]
    assert statements[0] == "begin" and statements[-1] == "commit"
    for name, table in GUARDS.items():
        assert re.search(
            rf"create trigger {name}\s+before insert or update on public\.{table}\s+for each row execute function public\.{name}\(\)",
            sql, re.I), name


def test_every_guard_exempts_only_server_side_callers_and_raises_42501():
    sql = _sql(MIGRATION)
    for name in GUARDS:
        declaration, body = _function_parts(sql, name)
        assert "current_user not in ('anon', 'authenticated')" in body, name
        assert "errcode = '42501'" in body, name
        # As a definer the function would run as its owner, and current_user would never be
        # anon/authenticated -- every caller would be exempt.
        assert re.search(r"\bsecurity\s+invoker\b", declaration, re.I), name
        assert not re.search(r"\bsecurity\s+definer\b", declaration, re.I), name


def test_migration_drops_the_any_status_correction_delete_policy():
    sql = _without_comments(_sql(MIGRATION))
    assert 'drop policy if exists "Users can delete own corrections" on public.corrections' in sql
    assert wide_correction_delete_policies(sql) == []


def test_provisioning_scripts_do_not_recreate_a_wide_correction_delete_policy():
    for path in (FIX_RLS, SETUP):
        assert wide_correction_delete_policies(_sql(path)) == [], path.name


def test_the_detector_finds_the_old_wide_policy():
    old = ('CREATE POLICY "Users can delete own corrections" ON corrections\n'
           'FOR DELETE TO authenticated\nUSING (auth.uid() = author_id);\n')
    assert wide_correction_delete_policies(old) == ["Users can delete own corrections"]


def test_setup_points_to_the_migration():
    assert "migrations/add_privileged_column_guards.sql" in _sql(SETUP)


def test_verification_never_commits_and_keeps_all_its_checks():
    sql = _sql(VERIFY)
    top = _without_comments(re.sub(r"\$(\w*)\$.*?\$\1\$", "", sql, flags=re.S))
    statements = [s.strip().lower() for s in top.split(";") if s.strip()]
    assert statements[0] == "begin" and statements[-1] == "rollback"
    assert "commit" not in statements and "end" not in statements
    names = re.findall(r"^select pg_temp\.check_as\('([A-Z]\d+[a-z]?) ", sql, re.M)
    assert len(names) == 68 and len(set(names)) == 68
