# -*- coding: utf-8 -*-
"""changelog.d/: every fragment is a well-formed entry, and
scripts/collect_changelog.py folds them into CHANGELOG.md correctly.

Each PR writes its changelog entry as a file of its own in changelog.d/ rather
than at the top of CHANGELOG.md, where every PR's entry conflicted with every
other open PR's after each merge (see changelog.d/README.md).
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent


def _load():
    spec = importlib.util.spec_from_file_location(
        "collect_changelog", REPO_ROOT / "scripts" / "collect_changelog.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


cc = _load()

ENTRY_A = "## [Unreleased] -- Older change (web, branch `a`)\n\n- one\n"
ENTRY_B = "## [Unreleased] -- Newer change (desktop, branch `b`)\n\n### Fixes\n- two\n"
CHANGELOG = (
    "# Changelog\n\nAll notable changes.\n\n---\n\n"
    "## [Unreleased] -- Already here (web, branch `c`)\n\n- three\n\n"
    "## [9.7.0] - 2026-10-19 -- A release\n\n- four\n"
)


@pytest.mark.parametrize("path", cc.fragment_paths(cc.FRAGMENT_DIR),
                         ids=lambda p: p.name)
def test_every_fragment_in_the_tree_is_well_formed(path):
    assert cc.problems(path) == [], (path.name, cc.problems(path))


def _tree(tmp_path, fragments):
    d = tmp_path / "changelog.d"
    d.mkdir()
    (d / "README.md").write_text("instructions, never folded in\n", encoding="utf-8")
    for name, text in fragments.items():
        (d / name).write_text(text, encoding="utf-8")
    log = tmp_path / "CHANGELOG.md"
    log.write_text(CHANGELOG, encoding="utf-8")
    return d, log


def test_apply_puts_the_entries_newest_first_above_the_existing_ones(tmp_path):
    d, log = _tree(tmp_path, {"2026-10-18-a.md": ENTRY_A, "2026-10-19-b.md": ENTRY_B})
    assert cc.main(["--apply", "--dir", str(d), "--changelog", str(log)]) == 0
    text = log.read_text(encoding="utf-8")
    order = [text.index(s) for s in ("# Changelog", "---", "Newer change",
                                     "Older change", "Already here", "[9.7.0]")]
    assert order == sorted(order)
    assert "### Fixes" in text and "instructions" not in text
    assert sorted(p.name for p in d.iterdir()) == ["README.md"]   # fragments deleted


def test_without_apply_nothing_changes(tmp_path):
    d, log = _tree(tmp_path, {"2026-10-19-b.md": ENTRY_B})
    assert cc.main(["--dir", str(d), "--changelog", str(log)]) == 0
    assert log.read_text(encoding="utf-8") == CHANGELOG
    assert (d / "2026-10-19-b.md").exists()


def test_no_fragments_is_a_no_op(tmp_path):
    d, log = _tree(tmp_path, {})
    assert cc.main(["--apply", "--dir", str(d), "--changelog", str(log)]) == 0
    assert log.read_text(encoding="utf-8") == CHANGELOG


@pytest.mark.parametrize("heading", [
    "## [Unreleased] -- A change (web, branch `a`)",
    "## [Unreleased] -- A change (desktop, branch `fix/x`)",
    "## [Unreleased] -- A change (web + desktop, branch `feat/x-y`)",
    "## [Unreleased] -- A change (ci, branch `ci/x`)",
])
def test_every_area_the_readme_names_is_accepted(heading):
    assert cc.HEADING_RE.match(heading)


@pytest.mark.parametrize("name,text", [
    ("2026-10-19-b.md", "## [Unreleased] -- No branch (web)\n\n- x\n"),
    ("2026-10-19-b.md", "## [Unreleased] -- Bad area (CI, branch `x`)\n\n- x\n"),
    ("2026-10-19-b.md", "## [Unreleased] -- Bad area (both, branch `x`)\n\n- x\n"),
    ("2026-10-19-b.md", "## [Unreleased] -- No metadata at all\n\n- x\n"),
    ("2026-10-19-b.md", "## [Unreleased] -- Unquoted (web, branch x)\n\n- x\n"),
    ("2026-10-19-b.md", "## [9.8.0] -- not an Unreleased entry\n\n- x\n"),
    ("2026-10-19-b.md", "## [Unreleased] -- heading only\n"),
    ("2026-10-19-b.md", ENTRY_B + "\n## [Unreleased] -- a second entry\n\n- y\n"),
    ("2026-10-19-b.md", ""),
    ("my-change.md", ENTRY_B),
    ("2026-10-19-Upper.md", ENTRY_B),
])
def test_a_malformed_fragment_stops_the_fold_and_writes_nothing(tmp_path, name, text):
    d, log = _tree(tmp_path, {"2026-10-18-a.md": ENTRY_A, name: text})
    assert cc.main(["--apply", "--dir", str(d), "--changelog", str(log)]) == 1
    assert log.read_text(encoding="utf-8") == CHANGELOG
    assert (d / "2026-10-18-a.md").exists() and (d / name).exists()


def test_a_changelog_with_no_entries_yet_gets_them_at_the_end():
    assert cc.fold("# Changelog\n\n---\n", [ENTRY_A]).endswith(ENTRY_A.strip() + "\n\n")
