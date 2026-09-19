# -*- coding: utf-8 -*-
"""The instruction files agree with each other and with the repo.

Seven files tell a person or an agent how to work here: ``AGENTS.md`` (the canonical commands and
layout rules), ``CLAUDE.md``, ``CONTRIBUTING.md``, ``.cursorrules``, ``README.md``,
``tests/README.md`` and the release skill. Until 2026-09-18 they disagreed on the Python version
(3.10+ vs 3.11), on how to run the tests (``pytest tests/`` as one process, which the suite cannot
survive -- see tests/README.md), and on whether FastAPI was removed (it was not; the standalone
backend process was). Each of those was a real sentence someone could act on.

Three rules, each proven able to fail before it was committed:

1. **Python version.** Every ``Python 3.x`` mention says 3.11 -- what CI runs and what
   ``ruff.toml`` targets. A file with no mention is fine.
2. **Executable test commands.** Inside a fenced code block, a line that invokes pytest must name
   a path narrower than ``tests/`` (a file, a subdirectory, a node id) or be the bounded runner.
   A path-less pytest, or ``pytest tests/``, fails -- with or without ``-m``; a marker narrows the
   selection but not the process lifetime, which is the thing that failed. Prose and inline code
   are exempt, so a heading like "Why not ``pytest tests/``" stays legal.
3. **FastAPI.** A sentence that contains "FastAPI" together with "removed" or "outdated" must
   also contain "standalone" or "process", so that nobody reads it as "the framework is gone".

The rules are deliberately about executable lines and sentences, not about whether the runner is
"mentioned somewhere": the first draft of this policy was satisfied by files that mentioned the
runner once and then showed the forbidden command in the next code block.
"""
from __future__ import annotations

import pathlib
import re
import shlex

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

INSTRUCTION_FILES = (
    "AGENTS.md",
    "CLAUDE.md",
    "CONTRIBUTING.md",
    ".cursorrules",
    "README.md",
    "tests/README.md",
    ".claude/skills/release/SKILL.md",
)

REQUIRED_PYTHON = "11"
BOUNDED_RUNNERS = ("scripts/run_local_tests.py", "scripts/run_gui_tests.py")

_PY_VERSION = re.compile(r"python\s*3\.(\d+)", re.IGNORECASE)
_FENCE = re.compile(r"^\s*(```|~~~)")
_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+|\n")


def _read(rel: str) -> str:
    return (REPO_ROOT / rel).read_text(encoding="utf-8-sig")


def fenced_lines(text: str) -> list[tuple[int, str]]:
    """(line number, line) for every line inside a fenced code block."""
    out: list[tuple[int, str]] = []
    inside = False
    for n, line in enumerate(text.splitlines(), start=1):
        if _FENCE.match(line):
            inside = not inside
            continue
        if inside:
            out.append((n, line))
    return out


def pytest_invocation(line: str) -> list[str] | None:
    """The argv of a pytest invocation on this shell line, or None.

    Recognises ``pytest ...`` and ``python -m pytest ...`` (optionally behind ``VAR=value``
    prefixes and a ``$`` prompt); ignores everything after a ``#``. ``pip install pytest`` is
    not an invocation -- pytest is an argument there, not the command.
    """
    code = line.split("#", 1)[0].strip()
    if code.startswith("$"):
        code = code[1:].strip()
    if not code:
        return None
    try:
        tokens = shlex.split(code, posix=True)
    except ValueError:
        tokens = code.split()
    # drop leading VAR=value environment assignments
    while tokens and re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", tokens[0]):
        tokens = tokens[1:]
    if not tokens:
        return None
    if pathlib.PurePosixPath(tokens[0]).name in ("pytest", "pytest.exe", "py.test"):
        return tokens[1:]
    if (
        len(tokens) >= 3
        and pathlib.PurePosixPath(tokens[0]).name.startswith("python")
        and tokens[1] == "-m"
        and tokens[2] == "pytest"
    ):
        return tokens[3:]
    return None


def names_a_path_narrower_than_tests(args: list[str]) -> bool:
    for a in args:
        if a.startswith("-"):
            continue
        p = a.replace("\\", "/")
        if p.startswith("tests/") and len(p.rstrip("/")) > len("tests"):
            return True
        if "::" in p:
            return True
    return False


_CHAIN_SPLIT = re.compile(r"\s*(?:&&|\|\||;|\|)\s*")


def offending_pytest_lines(text: str) -> list[tuple[int, str]]:
    """Every fenced line whose executed command(s) include a broad pytest invocation.

    The comment is stripped and chained commands are split BEFORE anything is judged, so
    ``pytest tests/  # use scripts/run_local_tests.py instead`` and
    ``python scripts/run_local_tests.py && pytest tests/`` are both flagged: a runner named in a
    comment or in a sibling command does not excuse the broad invocation (Codex, PR #347).
    The runner itself is never a pytest invocation, so it needs no exemption.
    """
    bad: list[tuple[int, str]] = []
    for n, line in fenced_lines(text):
        code = line.split("#", 1)[0]
        for segment in _CHAIN_SPLIT.split(code):
            args = pytest_invocation(segment)
            if args is None:
                continue
            if not names_a_path_narrower_than_tests(args):
                bad.append((n, line.strip()))
                break
    return bad


def fastapi_sentences_missing_scope(text: str) -> list[str]:
    """Sentences that say FastAPI was removed/outdated without scoping it to the process.

    Single line breaks are Markdown wrapping, not sentence boundaries, so they are folded into
    spaces first; otherwise ``FastAPI backend was\\nremoved`` would split into two harmless
    fragments and pass (Codex, PR #347). Blank lines still separate paragraphs.
    """
    bad: list[str] = []
    text = re.sub(r"[ \t]*\n(?!\n)[ \t]*", " ", text)
    for sentence in _SENTENCE_SPLIT.split(text):
        s = sentence.lower()
        if "fastapi" in s and ("removed" in s or "outdated" in s):
            if "standalone" not in s and "process" not in s:
                bad.append(sentence.strip())
    return bad


@pytest.mark.parametrize("rel", INSTRUCTION_FILES)
def test_instruction_file_exists(rel):
    assert (REPO_ROOT / rel).is_file(), f"{rel} is one of the seven instruction files and must exist"


@pytest.mark.parametrize("rel", INSTRUCTION_FILES)
def test_python_version_is_3_11_everywhere(rel):
    text = _read(rel)
    wrong = sorted({m.group(0) for m in _PY_VERSION.finditer(text) if m.group(1) != REQUIRED_PYTHON})
    assert not wrong, (
        f"{rel} mentions {wrong}; the project states Python 3.{REQUIRED_PYTHON} everywhere "
        "(CI runs 3.11 only; ruff.toml target-version = py311)."
    )


@pytest.mark.parametrize("rel", INSTRUCTION_FILES)
def test_no_full_suite_pytest_in_code_blocks(rel):
    bad = offending_pytest_lines(_read(rel))
    assert not bad, (
        f"{rel} shows a pytest command that runs a selection broader than one file or one "
        f"subdirectory of tests/: {bad}. Broad selections go through "
        "python scripts/run_local_tests.py (see AGENTS.md, 'Test-command policy')."
    )


@pytest.mark.parametrize("rel", INSTRUCTION_FILES)
def test_fastapi_removed_is_always_scoped_to_the_process(rel):
    bad = fastapi_sentences_missing_scope(_read(rel))
    assert not bad, (
        f"{rel} says FastAPI was removed/outdated without scoping it to the standalone backend "
        f"process: {bad}. FastAPI serves /api/* inside the NiceGUI app; only the separate "
        "backend process was removed (Jan 2026)."
    )


# ---------------------------------------------------------------------------
# The rules themselves must be able to fail (self-tests on synthetic text).
# ---------------------------------------------------------------------------

def test_rule_2_flags_the_forbidden_shapes():
    text = (
        "```bash\n"
        "pytest tests/\n"
        "pytest\n"
        "python -m pytest -m slow\n"
        "pytest -m slow tests/\n"
        "PYTHONUTF8=1 python -m pytest tests/ -x -q\n"
        "pytest tests/  # use scripts/run_local_tests.py instead\n"
        "python scripts/run_local_tests.py && pytest tests/\n"
        "```\n"
    )
    assert len(offending_pytest_lines(text)) == 7


def test_rule_2_allows_the_permitted_shapes():
    text = (
        "```bash\n"
        "pytest tests/test_x.py\n"
        "pytest tests/atlas_bake -m atlas_bake\n"
        "python -m pytest tests/test_x.py::test_y -v\n"
        "python scripts/run_local_tests.py -m slow\n"
        "pip install ruff==0.15.10 pytest\n"
        "# pytest tests/ would be wrong here\n"
        "```\n"
        "Prose may say `pytest tests/` when explaining why not.\n"
    )
    assert offending_pytest_lines(text) == []


def test_rule_3_flags_the_bare_claim_and_accepts_the_scoped_one():
    assert fastapi_sentences_missing_scope("FastAPI backend was REMOVED in January 2026.")
    assert fastapi_sentences_missing_scope("Avoid outdated terms (FastAPI, genizah-backend).")
    # a Markdown line wrap inside the sentence must not hide it
    assert fastapi_sentences_missing_scope("FastAPI backend was\nremoved in January 2026.")
    assert not fastapi_sentences_missing_scope(
        "The standalone backend process was removed in January 2026; FastAPI itself is still live."
    )
    # ...and a wrap between the qualifier and the claim must not reject valid wording
    assert not fastapi_sentences_missing_scope(
        "The standalone backend process was removed in\nJanuary 2026; FastAPI itself is still live."
    )
