# -*- coding: utf-8 -*-
"""docs/OPEN_ISSUES.md, docs/BACKLOG.md and docs/ISSUE_DETAILS.md agree with each other
and are in canonical form; the docs/open_issues.d/ fragments are well formed; and
scripts/collect_open_issues.py and scripts/archive_closed_issues.py move issues in and
out correctly.

The tracker was reconstructed on 2026-10-25 (scripts/open_issues_model.py): its counts
are generated, so the check that the real files are canonical is what stops them
drifting again (the old hand-kept Quick Summary said P2 48 while 45 rows were open).
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
# The scripts import their shared model as a sibling, as they do when run. Appended,
# not inserted: nothing already importable can be shadowed by a script's name.
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.append(str(REPO_ROOT / "scripts"))

import archive_closed_issues as ac  # noqa: E402
import collect_open_issues as co  # noqa: E402
import open_issues_model as model  # noqa: E402

PREAMBLE = ["# Tracker", "", "> **Last Updated:** 2026-10-20 (something). 3 open: P1 0, P2 1, P3 2.", ""]
OLD = [model.Issue(4, "P2", "search", "Old P2", "`x.py`", "2026-10-01", "❌ Open"),
       model.Issue(2, "P3", "web", "Old P3 a", "`y.py`", "2026-09-01", "❌ Open"),
       model.Issue(3, "P3", "web", "Old P3 b", "`z.py`", "", "⏳ Needs a check")]
TRACKER = model.render(model.Doc(list(PREAMBLE), [model.Issue(**vars(i)) for i in OLD],
                                 model.PRIORITIES))
BACKLOG = model.render(model.Doc(["# Backlog", "", "> **Last Updated:** 2026-10-20 (x). 1 items.", ""],
                                 [model.Issue(5, "debt", "build-ci", "Old debt", "`d.py`", "", "❌ Open")],
                                 model.KINDS))
DETAILS = model.render_details(["# Issue details", ""],
                               {2: "a", 3: "b", 4: "p2", 5: "debt"})
ARCHIVE = "# Archive\n\nGS-7 (OPEN_ISSUES.md) was closed long ago.\n"

FRAG_P2 = ("## P2 search: New P2 bug\nWhere: `a.py`\nOpened: 2026-10-21\n\n"
           "What happens, and where.\n")
FRAG_P3 = ("## P3 desktop: New P3 bug\nWhere: `b.py`\nStatus: ⏳ Needs a check\n\n"
           "Details of the P3.\n")
LEGACY = "### P3 - Low Priority\n| **Legacy bug** | `c.py` | ❌ Open (2026-10-19) | notes |\n"


def test_the_canonical_form_is_pinned():
    assert TRACKER == "\n".join(PREAMBLE + [
        "## Summary",
        "",
        "| Area | P1 | P2 | P3 | Total |",
        "|------|------|------|------|------|",
        "| Search | 0 | 1 | 0 | 1 |",
        "| Website | 0 | 0 | 2 | 2 |",
        "| **Total** | **0** | **1** | **2** | **3** |",
        "",
        "## P1 - Critical",
        "",
        "_None open._",
        "",
        "## P2 - Medium",
        "",
        "### Search",
        "",
        "| ID | Issue | Where | Opened | Status |",
        "|----|-------|-------|--------|--------|",
        "| [GS-4](ISSUE_DETAILS.md#gs-4) | Old P2 | `x.py` | 2026-10-01 | ❌ Open |",
        "",
        "## P3 - Low Priority",
        "",
        "### Website",
        "",
        "| ID | Issue | Where | Opened | Status |",
        "|----|-------|-------|--------|--------|",
        "| [GS-2](ISSUE_DETAILS.md#gs-2) | Old P3 a | `y.py` | 2026-09-01 | ❌ Open |",
        "| [GS-3](ISSUE_DETAILS.md#gs-3) | Old P3 b | `z.py` | — | ⏳ Needs a check |",
        "",
    ])
    assert model.render(model.parse(TRACKER, model.PRIORITIES)) == TRACKER
    assert model.problems(TRACKER, BACKLOG, DETAILS) == []


# ------------------------------------------------------------ the real files ----

def test_the_real_tracker_backlog_and_details_are_consistent_and_canonical():
    assert co.check(model.TRACKER, model.BACKLOG, model.DETAILS) == []


def test_no_open_issue_reuses_an_id_the_archive_holds():
    archive = model.ARCHIVE.read_text(encoding="utf-8")
    open_ids = {i.id for i in model.parse(model.TRACKER.read_text(encoding="utf-8"),
                                          model.PRIORITIES).issues}
    open_ids |= {i.id for i in model.parse(model.BACKLOG.read_text(encoding="utf-8"),
                                           model.KINDS).issues}
    assert not open_ids & model.used_ids(archive)


@pytest.mark.parametrize("path", co.fragment_paths(co.FRAGMENT_DIR), ids=lambda p: p.name)
def test_every_fragment_in_the_tree_is_well_formed(path):
    assert co.problems(path) == [], (path.name, co.problems(path))


@pytest.mark.parametrize("path", co.fragment_paths(co.FRAGMENT_DIR), ids=lambda p: p.name)
def test_every_fragment_in_the_tree_folds_into_the_real_tracker(path):
    issues = co.parse(path.read_text(encoding="utf-8"), "2026-01-01")[0]
    co.fold(model.TRACKER.read_text(encoding="utf-8"), model.DETAILS.read_text(encoding="utf-8"),
            [(path.name, issues)], set(), "2026-01-01")


# ------------------------------------------------------------------ the check ----

@pytest.mark.parametrize("change,expect", [
    (("| Search | 0 | 1 | 0 | 1 |", "| Search | 0 | 2 | 0 | 2 |"), "not in canonical form"),
    (("| [GS-3](ISSUE_DETAILS.md#gs-3)", "| [GS-4](ISSUE_DETAILS.md#gs-4)"), "GS-4 is in"),
    (("❌ Open |\n\n## P3", "✅ Fixed (2026-10-22) |\n\n## P3"), "archive_closed_issues.py"),
    (("| ⏳ Needs a check |", "| Open |"), "does not start with"),
    (("### Website", "### Web"), "is not one of the areas"),
    (("| `y.py` |", "| `y.py` | extra |"), "not 5"),
    # The count closing "Last Updated" is generated too (Codex review of PR #449).
    ((". 3 open: P1 0, P2 1, P3 2.", ". 7 open: P1 0, P2 1, P3 2."), "not in canonical form"),
    ((" 3 open: P1 0, P2 1, P3 2.", " three open."), "the generated count"),
])
def test_the_check_reports_a_hand_edit_that_breaks_the_tracker(change, expect):
    found = model.problems(TRACKER.replace(*change), BACKLOG, DETAILS)
    assert any(expect in p for p in found), found


def test_a_count_shaped_phrase_in_hand_written_text_is_never_rewritten(tmp_path):
    """Only the count after '(...). ' is generated; 'reviewed 3 items.' in narrative is the
    owner's text: it is reported, not silently turned into today's count (Codex, #449)."""
    narrative = TRACKER.replace("(something). 3 open: P1 0, P2 1, P3 2.",
                                "(something); reviewed 3 items.")
    doc = model.parse(narrative, model.PRIORITIES)
    assert model.render(doc) == narrative
    assert any("the generated count" in p for p in model.problems(narrative, BACKLOG, DETAILS))
    d, f = _tree(tmp_path, {}, tracker=narrative)
    assert _run(d, f, "--apply") == 0
    assert "reviewed 3 items." in f["tracker"].read_text(encoding="utf-8")


def test_the_check_reports_an_emptied_details_section():
    """The section is the row's only full text: a heading left with no body is a loss."""
    found = model.problems(TRACKER, BACKLOG, model.render_details(
        ["# d", ""], {2: "", 3: "b", 4: "p2", 5: "debt"}))
    assert any("GS-2 (OPEN_ISSUES.md): its '## GS-2' section in ISSUE_DETAILS.md is empty" in p
               for p in found), found


def test_a_stale_last_updated_count_is_repaired_by_apply(tmp_path):
    d, f = _tree(tmp_path, {}, tracker=TRACKER.replace(". 3 open: P1 0, P2 1, P3 2.",
                                                       ". 7 open: P1 0, P2 1, P3 2."))
    assert _run(d, f, "--apply") == 0
    assert f["tracker"].read_text(encoding="utf-8") == TRACKER


def test_the_check_reports_details_without_a_row_and_a_row_without_details():
    found = model.problems(TRACKER, BACKLOG, model.render_details(
        ["# d", ""], {2: "a", 3: "b", 5: "debt", 9: "orphan"}))
    assert any("GS-4 (OPEN_ISSUES.md) has no '## GS-4'" in p for p in found)
    assert any("GS-9 section but no row" in p for p in found)


# ------------------------------------------------------------------- the fold ----

def _tree(tmp_path, fragments, tracker=TRACKER, details=DETAILS):
    d = tmp_path / "open_issues.d"
    d.mkdir()
    (d / "README.md").write_text("instructions, never folded in\n", encoding="utf-8")
    for name, text in fragments.items():
        (d / name).write_text(text, encoding="utf-8")
    files = {}
    for key, text in (("tracker", tracker), ("backlog", BACKLOG), ("details", details),
                      ("archive", ARCHIVE)):
        files[key] = tmp_path / ("%s.md" % key)
        files[key].write_text(text, encoding="utf-8")
    return d, files


def _run(d, files, *extra):
    argv = list(extra) + ["--dir", str(d), "--today", "2026-10-22"]
    for key, path in files.items():
        argv += ["--" + key, str(path)]
    return co.main(argv)


def test_apply_adds_issues_with_new_ids_details_and_counts_and_deletes_the_fragments(tmp_path):
    d, f = _tree(tmp_path, {"2026-10-20-older.md": FRAG_P3, "2026-10-21-newer.md": FRAG_P2})
    assert _run(d, f, "--apply") == 0
    tracker = f["tracker"].read_text(encoding="utf-8")
    doc = model.parse(tracker, model.PRIORITIES)
    new = {i.headline: i for i in doc.issues}
    # Oldest fragment first; IDs above every ID in use, the archive's GS-7 included.
    assert new["New P3 bug"].id == 8 and new["New P2 bug"].id == 9
    assert new["New P3 bug"].opened == "2026-10-20"          # the fragment's date
    assert new["New P3 bug"].status == "⏳ Needs a check"
    assert "| Search | 0 | 2 | 0 | 2 |" in tracker and "| Desktop app | 0 | 0 | 1 | 1 |" in tracker
    assert ("> **Last Updated:** 2026-10-22 (folded 2 fragment(s) from `docs/open_issues.d/`: "
            "2026-10-20-older.md, 2026-10-21-newer.md). 5 open: P1 0, P2 2, P3 3.") in tracker
    _, sections = model.parse_details(f["details"].read_text(encoding="utf-8"))
    assert sections[9].startswith("**New P2 bug** · P2 · Search · ❌ Open · opened 2026-10-21")
    assert sections[9].endswith("What happens, and where.")
    assert co.check(f["tracker"], f["backlog"], f["details"]) == []
    assert sorted(p.name for p in d.iterdir()) == ["README.md"]


def test_a_new_p2_lands_above_an_older_one_in_its_area(tmp_path):
    d, f = _tree(tmp_path, {"2026-10-21-a.md": FRAG_P2})
    assert _run(d, f, "--apply") == 0
    lines = f["tracker"].read_text(encoding="utf-8").split("\n")
    i = next(n for n, ln in enumerate(lines) if "| New P2 bug |" in ln)
    assert "| Old P2 |" in lines[i + 1]


def test_a_legacy_fragment_lands_under_unsorted(tmp_path):
    d, f = _tree(tmp_path, {"2026-10-19-legacy.md": LEGACY})
    assert _run(d, f, "--apply") == 0
    doc = model.parse(f["tracker"].read_text(encoding="utf-8"), model.PRIORITIES)
    legacy = next(i for i in doc.issues if i.headline == "Legacy bug")
    assert (legacy.group, legacy.area, legacy.opened) == ("P3", "unsorted", "2026-10-19")
    assert "### Unsorted" in f["tracker"].read_text(encoding="utf-8")


def test_a_pipe_in_a_fragment_stays_inside_its_cell_and_is_not_folded_twice(tmp_path):
    """A "|" in a headline or Where (a regex, an alternative) is escaped in the row; the
    next parse reads five cells, and a re-run recognises the issue (Codex review of #449)."""
    frag = "## P2 search: Regex a|b finds nothing\nWhere: `x.py` (a|b)\n\nd\n"
    d, f = _tree(tmp_path, {"2026-10-21-a.md": frag})
    assert _run(d, f, "--apply") == 0
    tracker = f["tracker"].read_text(encoding="utf-8")
    assert "| Regex a\\|b finds nothing | `x.py` (a\\|b) | 2026-10-21 | ❌ Open |" in tracker
    assert co.check(f["tracker"], f["backlog"], f["details"]) == []
    (d / "2026-10-22-again.md").write_text(frag, encoding="utf-8")
    assert _run(d, f, "--apply") == 0
    assert f["tracker"].read_text(encoding="utf-8") == tracker


def test_an_old_style_heading_inside_new_style_details_is_just_text():
    """The first heading decides the form (Codex review of PR #449)."""
    text = "## P2 search: New bug\nWhere: `a.py`\n\nIntro.\n\n### P2 - Medium\nnotes under it\n"
    issues, found = co.parse(text, "2026-10-21")
    assert found == [] and len(issues) == 1
    assert issues[0][1].endswith("### P2 - Medium\nnotes under it")


def test_without_apply_nothing_changes(tmp_path):
    d, f = _tree(tmp_path, {"2026-10-21-a.md": FRAG_P2})
    assert _run(d, f) == 0
    assert f["tracker"].read_text(encoding="utf-8") == TRACKER
    assert (d / "2026-10-21-a.md").exists()


def test_apply_without_fragments_repairs_a_hand_edited_count(tmp_path):
    d, f = _tree(tmp_path, {}, tracker=TRACKER.replace("| Search | 0 | 1 | 0 | 1 |",
                                                       "| Search | 0 | 7 | 0 | 7 |"))
    assert _run(d, f, "--apply") == 0
    assert f["tracker"].read_text(encoding="utf-8") == TRACKER


@pytest.mark.parametrize("name,text", [
    ("2026-10-21-b.md", ""),
    ("2026-10-21-b.md", "Where: `a.py`\n\nno heading\n"),
    ("2026-10-21-b.md", "## P4 search: Someday\nWhere: `a.py`\n\nd\n"),
    ("2026-10-21-b.md", "## P2 nowhere: Bad area\nWhere: `a.py`\n\nd\n"),
    ("2026-10-21-b.md", "## P2 unsorted: Not for new fragments\nWhere: `a.py`\n\nd\n"),
    ("2026-10-21-b.md", "## P2 search: No where\nOpened: 2026-10-21\n\nd\n"),
    ("2026-10-21-b.md", "## P2 search: No details\nWhere: `a.py`\n"),
    ("2026-10-21-b.md", "## P2 search: Closed\nWhere: `a.py`\nStatus: ✅ Fixed\n\nd\n"),
    ("2026-10-21-b.md", "## P2 search: Bad date\nWhere: `a.py`\nOpened: Oct 21\n\nd\n"),
    ("2026-10-21-b.md", "## P2 search: Bad key\nWhere: `a.py`\nOwner: me\n\nd\n"),
    ("2026-10-21-b.md", "### P2 - Medium\n| **a** | b | ❌ Open |\n"),             # legacy, 3 cells
    ("2026-10-21-b.md", "### P2 - Medium\n| **a** | b | ✅ Fixed (was ❌ Open) | n |\n"),
    ("my-bug.md", FRAG_P2),
    ("2026-10-21-Upper.md", FRAG_P2),
])
def test_a_malformed_fragment_stops_the_fold_and_writes_nothing(tmp_path, name, text):
    d, f = _tree(tmp_path, {"2026-10-20-ok.md": FRAG_P3, name: text})
    assert _run(d, f, "--apply") == 1
    assert f["tracker"].read_text(encoding="utf-8") == TRACKER
    assert f["details"].read_text(encoding="utf-8") == DETAILS
    assert (d / "2026-10-20-ok.md").exists() and (d / name).exists()


def test_an_issue_the_tracker_already_holds_adds_nothing_and_its_fragment_is_deleted(tmp_path):
    d, f = _tree(tmp_path, {"2026-10-21-a.md": "## P2 search: Old P2\nWhere: `x.py`\n\nsame\n"})
    assert _run(d, f, "--apply") == 0
    assert f["tracker"].read_text(encoding="utf-8") == TRACKER
    assert sorted(p.name for p in d.iterdir()) == ["README.md"]


def test_a_failed_delete_is_finished_by_the_next_run_without_duplicating(tmp_path, monkeypatch):
    d, f = _tree(tmp_path, {"2026-10-21-a.md": FRAG_P2, "2026-10-20-b.md": FRAG_P3})
    real_unlink = Path.unlink

    def locked(self, *args, **kwargs):
        if self.name == "2026-10-21-a.md":
            raise PermissionError(13, "Permission denied")
        return real_unlink(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", locked)
    assert _run(d, f, "--apply") == 1
    first = f["tracker"].read_text(encoding="utf-8"), f["details"].read_text(encoding="utf-8")
    assert first[0].count("| New P2 bug |") == 1
    monkeypatch.setattr(Path, "unlink", real_unlink)
    assert _run(d, f, "--apply") == 0
    assert (f["tracker"].read_text(encoding="utf-8"),
            f["details"].read_text(encoding="utf-8")) == first
    assert sorted(p.name for p in d.iterdir()) == ["README.md"]


def test_a_run_that_stopped_before_the_details_is_finished_by_the_next(tmp_path):
    d, f = _tree(tmp_path, {"2026-10-21-a.md": FRAG_P2})
    assert _run(d, f, "--apply") == 0
    folded = f["tracker"].read_text(encoding="utf-8")
    # As if the run had written the tracker and stopped: the details lack GS-8.
    (tmp_path / "again").mkdir()
    d2, f2 = _tree(tmp_path / "again", {"2026-10-21-a.md": FRAG_P2}, tracker=folded)
    assert co.check(f2["tracker"], f2["backlog"], f2["details"])        # GS-8 has no details
    assert _run(d2, f2, "--apply") == 0
    assert f2["tracker"].read_text(encoding="utf-8") == folded
    assert co.check(f2["tracker"], f2["backlog"], f2["details"]) == []


def test_a_details_heading_left_empty_is_refilled_by_the_next_run(tmp_path):
    """A heading with no body is as incomplete as no heading (Codex review of PR #449)."""
    d, f = _tree(tmp_path, {"2026-10-21-a.md": FRAG_P2})
    assert _run(d, f, "--apply") == 0
    folded = f["tracker"].read_text(encoding="utf-8")
    head, sections = model.parse_details(f["details"].read_text(encoding="utf-8"))
    sections[8] = ""
    (tmp_path / "again").mkdir()
    d2, f2 = _tree(tmp_path / "again", {"2026-10-21-a.md": FRAG_P2}, tracker=folded,
                   details=model.render_details(head, sections))
    assert any("is empty" in p for p in co.check(f2["tracker"], f2["backlog"], f2["details"]))
    assert _run(d2, f2, "--apply") == 0
    assert co.check(f2["tracker"], f2["backlog"], f2["details"]) == []
    assert model.parse_details(f2["details"].read_text(encoding="utf-8"))[1][8].endswith(
        "What happens, and where.")
    assert sorted(p.name for p in d2.iterdir()) == ["README.md"]


def test_the_same_issue_in_two_fragments_is_folded_once(tmp_path):
    d, f = _tree(tmp_path, {"2026-10-21-a.md": FRAG_P2, "2026-10-20-b.md": FRAG_P2})
    assert _run(d, f, "--apply") == 0
    text = f["tracker"].read_text(encoding="utf-8")
    assert text.count("| New P2 bug |") == 1
    assert "(folded 1 fragment(s) from `docs/open_issues.d/`: 2026-10-20-b.md)" in text


# ---------------------------------------------------------------- the archive ----

def test_a_closed_row_moves_to_the_archive_with_its_details_and_its_id_stays_taken(tmp_path):
    d, f = _tree(tmp_path, {}, tracker=TRACKER.replace(
        "| `y.py` | 2026-09-01 | ❌ Open |", "| `y.py` | 2026-09-01 | ✅ Fixed (2026-10-22, abc1234) |"))
    argv = ["--apply", "--date", "2026-10-22"]
    for key, path in f.items():
        argv += ["--" + key, str(path)]
    assert ac.main(argv) == 0
    archive = f["archive"].read_text(encoding="utf-8")
    assert archive.startswith(ARCHIVE)                                   # appended, not rewritten
    assert ("| GS-2 (OPEN_ISSUES.md) | Old P3 a | `y.py` | 2026-09-01 | "
            "✅ Fixed (2026-10-22, abc1234) |") in archive
    assert "### GS-2\n\na\n" in archive
    tracker = f["tracker"].read_text(encoding="utf-8")
    assert "Old P3 a" not in tracker and "| Website | 0 | 0 | 1 | 1 |" in tracker
    assert "(archived GS-2). 2 open: P1 0, P2 1, P3 1." in tracker
    assert 2 not in model.parse_details(f["details"].read_text(encoding="utf-8"))[1]
    assert co.check(f["tracker"], f["backlog"], f["details"]) == []


def test_the_highest_id_closed_is_never_given_out_again(tmp_path):
    d, f = _tree(tmp_path, {}, tracker=TRACKER)
    f["archive"].write_text("# Archive\n", encoding="utf-8")
    f["backlog"].write_text(BACKLOG.replace("| ❌ Open |", "| ✅ Done (2026-10-22) |"),
                            encoding="utf-8")
    argv = ["--apply", "--date", "2026-10-22"]
    for key, path in f.items():
        argv += ["--" + key, str(path)]
    assert ac.main(argv) == 0                                    # GS-5, the highest, archived
    assert "[GS-5](" not in f["backlog"].read_text(encoding="utf-8")
    (d / "2026-10-23-x.md").write_text(FRAG_P2, encoding="utf-8")
    assert _run(d, f, "--apply") == 0
    new = next(i for i in model.parse(f["tracker"].read_text(encoding="utf-8"),
                                      model.PRIORITIES).issues if i.headline == "New P2 bug")
    assert new.id == 6


@pytest.mark.parametrize("sections", [{3: "b", 4: "p2", 5: "debt"},             # no section
                                      {2: "", 3: "b", 4: "p2", 5: "debt"}])    # an empty one
def test_a_closed_row_without_its_details_is_not_archived(tmp_path, sections):
    """Its details are its full text: archiving without them loses it (Codex review of #449)."""
    closed = TRACKER.replace("| `y.py` | 2026-09-01 | ❌ Open |",
                             "| `y.py` | 2026-09-01 | ✅ Fixed (2026-10-22, abc1234) |")
    d, f = _tree(tmp_path, {}, tracker=closed,
                 details=model.render_details(["# Issue details", ""], sections))
    argv = ["--apply", "--date", "2026-10-22"]
    for key, path in f.items():
        argv += ["--" + key, str(path)]
    assert ac.main(argv) == 1
    assert f["archive"].read_text(encoding="utf-8") == ARCHIVE
    assert f["tracker"].read_text(encoding="utf-8") == closed


def test_the_archive_without_closed_rows_is_left_alone(tmp_path):
    d, f = _tree(tmp_path, {})
    argv = ["--apply"]
    for key, path in f.items():
        argv += ["--" + key, str(path)]
    assert ac.main(argv) == 0
    assert f["archive"].read_text(encoding="utf-8") == ARCHIVE
    assert f["tracker"].read_text(encoding="utf-8") == TRACKER
