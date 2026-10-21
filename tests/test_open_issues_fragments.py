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
    "## Protocol",
    "",
    "```markdown",
    "### P2 - Medium",                          # an example, as the real tracker has
    "| **Issue description** | `file.py` | ❌ Open (2026-10-21) | Details |",
    "```",
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
        assert co._heading_index(lines, heading) is not None
        assert any(ln.startswith("| %s | " % label) for ln in lines)


@pytest.mark.parametrize("heading", list(co.SECTIONS), ids=lambda h: h[4:])
def test_a_row_for_every_section_lands_in_the_real_tracker_table(heading):
    """Folded into the real tracker, a row lands under the real section heading, not
    under the fenced example in its maintenance protocol (Codex review of PR #428)."""
    row = "| **Probe row** | `probe.py` | ❌ Open (2026-01-01) | probe |"
    text = co.fold(co.TRACKER.read_text(encoding="utf-8"),
                   [("2026-01-01-probe.md", {heading: [row]})], "2026-01-01")
    lines = text.split("\n")
    i = lines.index(row)
    above = [ln for ln in lines[:i] if ln.startswith("#")]
    assert above[-1] == heading and co._heading_index(lines, heading) < i


def test_a_heading_inside_a_code_fence_is_not_the_section():
    lines = ["```", "### P2 - Medium", "```", "### P2 - Medium"]
    assert co._heading_index(lines, "### P2 - Medium") == 3


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
    p2 = co._heading_index(lines, "### P2 - Medium")      # the real one, not the fenced example
    assert lines[p2 + 4] == ROW_P2 and lines[p2 + 5].startswith("| **Old P2**")
    p3 = co._heading_index(lines, "### P3 - Low Priority")
    # Newest fragment first, each fragment's rows in its own order, above the old rows.
    assert lines[p3 + 4:p3 + 7] == [ROW_P3, ROW_P3B, "| **Old P3 a** | `y.py` | ❌ Open | n |"]
    assert "| P2 Medium Bugs | 2 |" in lines and "| P3 Low Priority | 4 |" in lines
    assert "| **Total open** | **6** |" in lines and "| P1 Critical Bugs | 0 |" in lines
    note = lines.index("> 2026-10-19 (something): P2 +1 -- 3.") + 1
    assert lines[note] == ("> 2026-10-22 (folded from `docs/open_issues.d/`: "
                           "2026-10-21-newer.md, 2026-10-20-older.md): P2 +1, P3 +2 -- 6.")
    assert lines[note + 1].startswith("> **Checks the cloud")
    # The header no longer announces the old total (Codex review of PR #428).
    assert lines[2] == ("> **Last Updated:** 2026-10-22 (folded 2 fragment(s) from "
                        "`docs/open_issues.d/`: 2026-10-21-newer.md, 2026-10-20-older.md) "
                        "-- P2 +1, P3 +2. 6 open.")
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
    ("2026-10-21-b.md", "### P2 - Medium\n| **a** | b | ✅ Fixed (was ❌ Open) | n |\n"),
    ("2026-10-21-b.md", "### P2 - Medium\n| **a** | b | Closed, see ⏳ note | n |\n"),
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
    ("> **Last Updated:**", "> Updated:"),                    # header line gone
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


# The tracker is written before the fragments are deleted, so a run that could not
# delete one (a locked file on Windows, an interrupted run) leaves its rows in both
# places; the next run must not fold them twice (Codex review of PR #428).
FOLDED_P2 = TRACKER.replace("|-------|------|--------|-------|\n| **Old P2**",
                            "|-------|------|--------|-------|\n" + ROW_P2 + "\n| **Old P2**")


def test_a_fragment_whose_rows_the_tracker_already_holds_adds_nothing_and_is_deleted(tmp_path):
    d, t = _tree(tmp_path, {"2026-10-21-a.md": "### P2 - Medium\n" + ROW_P2 + "\n"})
    t.write_text(FOLDED_P2, encoding="utf-8")
    assert _run(d, t, "--apply") == 0
    assert t.read_text(encoding="utf-8") == FOLDED_P2
    assert sorted(p.name for p in d.iterdir()) == ["README.md"]


def test_a_failed_delete_is_finished_by_the_next_run_without_duplicating(tmp_path, monkeypatch):
    d, t = _tree(tmp_path, {"2026-10-21-a.md": "### P2 - Medium\n" + ROW_P2 + "\n",
                            "2026-10-20-b.md": "### P3 - Low Priority\n" + ROW_P3B + "\n"})
    real_unlink = Path.unlink

    def locked(self, *args, **kwargs):
        if self.name == "2026-10-21-a.md":
            raise PermissionError(13, "Permission denied")
        return real_unlink(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", locked)
    assert _run(d, t, "--apply") == 1
    first = t.read_text(encoding="utf-8")
    assert first.count(ROW_P2) == 1 and first.count(ROW_P3B) == 1
    assert "| P2 Medium Bugs | 2 |" in first and "| **Total open** | **5** |" in first
    assert sorted(p.name for p in d.iterdir()) == ["2026-10-21-a.md", "README.md"]
    monkeypatch.setattr(Path, "unlink", real_unlink)
    assert _run(d, t, "--apply") == 0
    assert t.read_text(encoding="utf-8") == first      # same rows, same counts
    assert sorted(p.name for p in d.iterdir()) == ["README.md"]


def test_the_count_note_names_only_the_fragments_that_added_rows(tmp_path):
    d, t = _tree(tmp_path, {"2026-10-21-a.md": "### P2 - Medium\n" + ROW_P2 + "\n",
                            "2026-10-20-b.md": "### P3 - Low Priority\n" + ROW_P3B + "\n"})
    t.write_text(FOLDED_P2, encoding="utf-8")
    assert _run(d, t, "--apply") == 0
    text = t.read_text(encoding="utf-8")
    assert text.count(ROW_P2) == 1 and text.count(ROW_P3B) == 1
    assert "| P2 Medium Bugs | 1 |" in text and "| P3 Low Priority | 3 |" in text
    assert "| **Total open** | **4** |" in text
    assert "> 2026-10-22 (folded from `docs/open_issues.d/`: 2026-10-20-b.md): P3 +1 -- 4." in text
    assert ("> **Last Updated:** 2026-10-22 (folded 1 fragment(s) from `docs/open_issues.d/`: "
            "2026-10-20-b.md) -- P3 +1. 4 open.") in text
    assert sorted(p.name for p in d.iterdir()) == ["README.md"]
