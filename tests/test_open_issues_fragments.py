# -*- coding: utf-8 -*-
"""docs/open_issues.d/: every fragment is well formed and names sections the tracker
has, and scripts/collect_open_issues.py folds them into docs/OPEN_ISSUES.md correctly.

Each PR writes its new tracker rows as a file of its own rather than in the tracker,
where any two PRs that found something conflicted on the counts, the count notes and
the "Last Updated" line (see docs/open_issues.d/README.md).
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent


def _load():
    spec = importlib.util.spec_from_file_location(
        "collect_open_issues", REPO_ROOT / "scripts" / "collect_open_issues.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


co = _load()

ROW_P2 = "| **New P2 bug** | `a.py` | ❌ Open (2026-10-21) | notes |"
ROW_P3 = "| **New P3 bug** | `b.py` | ⏳ Pending (2026-10-21) | notes |"
ROW_P3B = "| **Older P3 bug** | `c.py` | ❌ Open (2026-10-20) | notes |"
TRACKER = "\n".join([
    "# GenizahSearch - Open Issues Tracker",
    "",
    "> **Last Updated:** 2026-10-20 -- 3 open.",
    "",
    "## Quick Summary",
    "",
    "| Category | Open |",
    "|----------|------|",
    "| P1 Critical Bugs | 0 |",
    "| P2 Medium Bugs | 1 |",
    "| P3 Low Priority | 2 |",
    "| **Total open** | **3** |",
    "",
    "> 2026-10-19 (something): P2 +1 -- 3.",
    "> **Checks the cloud cannot run**: unrelated block.",
    "",
    "## 1. Outstanding Bugs",
    "",
    "### P1 - Critical",
    "",
    "| Issue | File | Status | Notes |",
    "|-------|------|--------|-------|",
    "",
    "### P2 - Medium",
    "",
    "| Issue | File | Status | Notes |",
    "|-------|------|--------|-------|",
    "| **Old P2** | `x.py` | ❌ Open | n |",
    "",
    "### P3 - Low Priority",
    "",
    "| Issue | File | Status | Notes |",
    "|-------|------|--------|-------|",
    "| **Old P3 a** | `y.py` | ❌ Open | n |",
    "| **Old P3 b** | `z.py` | ❌ Open | n |",
    "",
])


@pytest.mark.parametrize("path", co.fragment_paths(co.FRAGMENT_DIR), ids=lambda p: p.name)
def test_every_fragment_in_the_tree_is_well_formed(path):
    assert co.problems(path) == [], (path.name, co.problems(path))


@pytest.mark.parametrize("path", co.fragment_paths(co.FRAGMENT_DIR), ids=lambda p: p.name)
def test_every_fragment_in_the_tree_folds_into_the_real_tracker(path):
    blocks = co.parse(path.read_text(encoding="utf-8"))[0]
    co.fold(co.TRACKER.read_text(encoding="utf-8"), [(path.name, blocks)], "2026-01-01")


def test_every_section_the_script_accepts_exists_in_the_real_tracker():
    lines = co.TRACKER.read_text(encoding="utf-8").splitlines()
    for heading, label in co.SECTIONS.items():
        assert heading in lines
        assert any(ln.startswith("| %s | " % label) for ln in lines)


def _tree(tmp_path, fragments):
    d = tmp_path / "open_issues.d"
    d.mkdir()
    (d / "README.md").write_text("instructions, never folded in\n", encoding="utf-8")
    for name, text in fragments.items():
        (d / name).write_text(text, encoding="utf-8")
    t = tmp_path / "OPEN_ISSUES.md"
    t.write_text(TRACKER, encoding="utf-8")
    return d, t


def _run(d, t, *extra):
    return co.main([*extra, "--dir", str(d), "--tracker", str(t), "--today", "2026-10-22"])


def test_apply_adds_rows_counts_and_one_note_and_deletes_the_fragments(tmp_path):
    d, t = _tree(tmp_path, {
        "2026-10-20-older.md": "### P3 - Low Priority\n" + ROW_P3B + "\n",
        "2026-10-21-newer.md": "### P2 - Medium\n%s\n\n### P3 - Low Priority\n%s\n" % (ROW_P2, ROW_P3),
    })
    assert _run(d, t, "--apply") == 0
    lines = t.read_text(encoding="utf-8").split("\n")
    p2 = lines.index("### P2 - Medium")
    assert lines[p2 + 4] == ROW_P2 and lines[p2 + 5].startswith("| **Old P2**")
    p3 = lines.index("### P3 - Low Priority")
    # Newest fragment first, each fragment's rows in its own order, above the old rows.
    assert lines[p3 + 4:p3 + 7] == [ROW_P3, ROW_P3B, "| **Old P3 a** | `y.py` | ❌ Open | n |"]
    assert "| P2 Medium Bugs | 2 |" in lines and "| P3 Low Priority | 4 |" in lines
    assert "| **Total open** | **6** |" in lines and "| P1 Critical Bugs | 0 |" in lines
    note = lines.index("> 2026-10-19 (something): P2 +1 -- 3.") + 1
    assert lines[note] == ("> 2026-10-22 (folded from `docs/open_issues.d/`: "
                           "2026-10-21-newer.md, 2026-10-20-older.md): P2 +1, P3 +2 -- 6.")
    assert lines[note + 1].startswith("> **Checks the cloud")
    assert sorted(p.name for p in d.iterdir()) == ["README.md"]


def test_without_apply_nothing_changes(tmp_path):
    d, t = _tree(tmp_path, {"2026-10-21-a.md": "### P2 - Medium\n" + ROW_P2 + "\n"})
    assert _run(d, t) == 0
    assert t.read_text(encoding="utf-8") == TRACKER
    assert (d / "2026-10-21-a.md").exists()


def test_no_fragments_is_a_no_op(tmp_path):
    d, t = _tree(tmp_path, {})
    assert _run(d, t, "--apply") == 0
    assert t.read_text(encoding="utf-8") == TRACKER


@pytest.mark.parametrize("name,text", [
    ("2026-10-21-b.md", ""),
    ("2026-10-21-b.md", ROW_P2 + "\n"),                                       # no heading
    ("2026-10-21-b.md", "### P4 - Someday\n" + ROW_P2 + "\n"),                # unknown section
    ("2026-10-21-b.md", "## 2. Documentation Issues\n" + ROW_P2 + "\n"),      # uncounted section
    ("2026-10-21-b.md", "### P2 - Medium\n"),                                 # heading, no rows
    ("2026-10-21-b.md", "### P2 - Medium\n| **a** | b | ❌ Open |\n"),       # three cells
    ("2026-10-21-b.md", "### P2 - Medium\n| **a** | b | ✅ Fixed (2026-10-21) | n |\n"),
    ("2026-10-21-b.md", "### P2 - Medium\nplain prose, not a row\n"),
    ("my-bug.md", "### P2 - Medium\n" + ROW_P2 + "\n"),
    ("2026-10-21-Upper.md", "### P2 - Medium\n" + ROW_P2 + "\n"),
])
def test_a_malformed_fragment_stops_the_fold_and_writes_nothing(tmp_path, name, text):
    d, t = _tree(tmp_path, {"2026-10-20-ok.md": "### P3 - Low Priority\n" + ROW_P3B + "\n",
                            name: text})
    assert _run(d, t, "--apply") == 1
    assert t.read_text(encoding="utf-8") == TRACKER
    assert (d / "2026-10-20-ok.md").exists() and (d / name).exists()


@pytest.mark.parametrize("broken", [
    ("### P2 - Medium", "### P2 - Renamed"),                 # section heading gone
    ("| P2 Medium Bugs | 1 |", "| P2 Bugs | 1 |"),           # its count row gone
    ("| **Total open** | **3** |", "| Total | 3 |"),         # total row gone
])
def test_a_tracker_missing_what_the_fold_needs_writes_nothing(tmp_path, broken):
    d, t = _tree(tmp_path, {"2026-10-21-a.md": "### P2 - Medium\n" + ROW_P2 + "\n"})
    changed = TRACKER.replace(*broken)
    t.write_text(changed, encoding="utf-8")
    assert _run(d, t, "--apply") == 1
    assert t.read_text(encoding="utf-8") == changed
    assert (d / "2026-10-21-a.md").exists()


def test_an_escaped_pipe_inside_a_cell_is_not_a_cell_boundary():
    row = r"| **a \| b** | `x.py` | ❌ Open | n |"
    assert co.parse("### P2 - Medium\n" + row + "\n") == ({"### P2 - Medium": [row]}, [])
