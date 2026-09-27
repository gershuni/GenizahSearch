# -*- coding: utf-8 -*-
"""The list_items.page migration is SQL the app never runs, so nothing else would notice
if it drifted. It must add one nullable TEXT column in one transaction, be safe to run
again, restate the table grants (CLAUDE.md, convention 6) without touching RLS or its
policies, and carry the verify query the owner runs afterwards -- which must name `tags`,
because the desktop's tag filters are written for a jsonb column. A fresh project built
from supabase_setup.sql must get the same column, and the same grants on every list table
it creates.
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MIGRATION = ROOT / "migrations" / "add_list_item_page_column.sql"
SETUP = ROOT / "supabase_setup.sql"


def _sql(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _without_comments(sql: str) -> str:
    return re.sub(r"--[^\n]*", "", sql)


def _statements(sql: str) -> list:
    """Top-level statements, comments stripped; a $$ ... $$ body stays in one piece."""
    body = _without_comments(sql)
    parts, depth_open, current = [], False, []
    for token in re.split(r"(\$\$|;)", body):
        if token == "$$":
            depth_open = not depth_open
            current.append(token)
        elif token == ";" and not depth_open:
            text = "".join(current).strip()
            if text:
                parts.append(re.sub(r"\s+", " ", text).lower())
            current = []
        else:
            current.append(token)
    tail = "".join(current).strip()
    if tail:
        parts.append(re.sub(r"\s+", " ", tail).lower())
    return parts


def _comment_lines(sql: str) -> str:
    return "\n".join(line for line in sql.splitlines() if line.lstrip().startswith("--"))


def test_the_migration_file_exists():
    assert MIGRATION.is_file(), MIGRATION


def test_the_column_is_added_inside_one_transaction():
    statements = _statements(_sql(MIGRATION))
    assert statements.count("begin") == 1 and statements.count("commit") == 1, statements
    begin, commit = statements.index("begin"), statements.index("commit")
    assert begin == 0, "something runs before the transaction starts"
    inside = statements[begin + 1:commit]
    assert "alter table public.list_items add column if not exists page text" in inside
    # After the commit only the schema-cache reload may run: DDL there would not roll back.
    after = statements[commit + 1:]
    assert after == ["notify pgrst, 'reload schema'"], after


def test_the_column_is_nullable_text_with_no_default():
    sql = _without_comments(_sql(MIGRATION)).lower()
    alters = re.findall(r"alter table[^;]*;", sql)
    assert alters == ["alter table public.list_items add column if not exists page text;"], alters


def test_the_table_grants_are_restated_for_both_api_roles():
    statements = _statements(_sql(MIGRATION))
    assert "grant select, insert, update, delete on table public.list_items to authenticated" in statements
    assert "grant all on table public.list_items to service_role" in statements
    sequence_block = next(s for s in statements if s.startswith("do $$"))
    assert "pg_get_serial_sequence('public.list_items', 'id')" in sequence_block
    assert "grant usage, select on sequence %s to authenticated, service_role" in sequence_block
    # anon is left as it is: nothing here grants to it or revokes from anyone.
    assert not any(re.search(r"\banon\b", s) for s in statements), statements
    assert not any(s.startswith("revoke") for s in statements), statements


def test_rls_and_its_policies_are_left_alone():
    sql = _without_comments(_sql(MIGRATION)).lower()
    for forbidden in ("create policy", "drop policy", "alter policy",
                      "disable row level security", "no force row level security"):
        assert forbidden not in sql, forbidden


def test_the_migration_is_safe_to_run_again():
    statements = _statements(_sql(MIGRATION))
    for s in statements:
        if s.startswith("alter table"):
            assert "if not exists" in s, s
        assert not s.startswith("create table"), s
        assert not s.startswith("drop "), s


def test_the_verify_query_names_page_and_tags_and_expects_jsonb():
    comments = _comment_lines(_sql(MIGRATION))
    verify = comments[comments.lower().index("-- verify"):]
    assert re.search(r"column_name in \('page', 'tags'\)", verify), verify
    assert re.search(r"page\s*\|\s*text", verify), verify
    assert re.search(r"tags\s*\|\s*jsonb", verify), verify
    assert "information_schema.role_table_grants" in verify, verify


def test_a_fresh_setup_creates_list_items_with_a_page_column():
    sql = _without_comments(_sql(SETUP))
    table = re.search(r"create table public\.list_items\s*\((.*?)\);", sql, re.S | re.I)
    assert table, "supabase_setup.sql has no list_items table"
    columns = {m.group(1).lower(): m.group(2).strip().rstrip(",").lower()
               for m in re.finditer(r"^\s*(\w+)\s+([^\n]+)$", table.group(1), re.M)}
    assert columns.get("page") == "text", columns
    assert columns.get("tags", "").startswith("jsonb"), columns


LIST_TABLES = ('projects', 'user_lists', 'list_items', 'recent_items')


def test_a_fresh_setup_grants_every_list_table_to_both_api_roles():
    # CLAUDE.md, convention 6: a public table the Data API reaches needs explicit grants
    # as well as RLS; the setup file creates these four and must grant them as the
    # migration does, sequences included, without touching anon.
    statements = _statements(_sql(SETUP))
    for table in LIST_TABLES:
        created = next(n for n, s in enumerate(statements) if s.startswith(f"create table public.{table} "))
        for grant in (f"grant select, insert, update, delete on table public.{table} to authenticated",
                      f"grant all on table public.{table} to service_role"):
            assert grant in statements, grant
            assert statements.index(grant) > created, f"{grant} runs before the table exists"
    blocks = [s for s in statements if s.startswith("do $$") and "pg_get_serial_sequence" in s]
    assert len(blocks) == 1, blocks
    for table in LIST_TABLES:
        assert f"'public.{table}'" in blocks[0], table
    assert "grant usage, select on sequence %s to authenticated, service_role" in blocks[0]
    grants = [s for s in statements if s.startswith("grant") or "grant " in s and s.startswith("do $$")]
    assert not any(re.search(r"\banon\b", s) for s in grants), grants


def test_the_setup_grants_leave_rls_and_its_policies_as_they_were():
    # The grants are additive: the policies still decide which rows each user reaches.
    statements = _statements(_sql(SETUP))
    assert not any(s.startswith("revoke") for s in statements), "the setup file revokes something"
    for table in LIST_TABLES:
        assert f"alter table {table} enable row level security" in statements, table


def test_the_statement_splitter_can_fail():
    """The checks above read _statements(); make sure it would see a stray DDL after commit."""
    bad = "begin;\nalter table public.list_items add column if not exists page text;\ncommit;\n" \
          "alter table public.list_items add column page2 text;\n"
    statements = _statements(bad)
    assert statements[statements.index("commit") + 1:] == [
        "alter table public.list_items add column page2 text"]
