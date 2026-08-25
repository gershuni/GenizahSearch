# -*- coding: utf-8 -*-
"""The policy-comparison instrument must be able to express every policy axis.

`scripts/compare_passage_policies.py` is the committed instrument the depth
measurements were taken with -- `shared/passage_policy.py`'s own comments cite
those numbers as the reason `deep` and `deepest` exist. But `--baseline` and
`--candidate` name a WIDTH preset only, and the depth profiles reach a policy
solely through `compose()`, which the script never called. So the tool committed
to make a finding reproducible could not reproduce it: `--candidate-depth deep`
did not exist and `--candidate deep` was rejected by argparse (Codex review of
PR #328, confirmed by running it).

These tests drive the real argparse (via `--help` and an invalid choice, both of
which exit before the index is opened) and the real `compose()`. They do not run
`main()` to completion, which needs a multi-GB index on disk. What they pin is
the part that broke: which arguments are accepted, and which policy they
actually produce.
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.passage_policy import (  # noqa: E402
    DEFAULT_DEPTH, DEFAULT_LENGTH, DEPTH_PROFILES, LENGTH_PROFILES, compose,
)

SCRIPTS = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'scripts')
if SCRIPTS not in sys.path:
    sys.path.insert(0, SCRIPTS)


def _cli_source() -> str:
    path = os.path.join(SCRIPTS, 'compare_passage_policies.py')
    with open(path, encoding='utf-8') as fh:
        return fh.read()


# ---------------------------------------------------------------------------
# The axes exist on the command line at all.
# ---------------------------------------------------------------------------

def _run_cli(*args):
    """Invoke the real script. Only used with arguments that make argparse
    exit before the index is touched."""
    import subprocess
    return subprocess.run(
        [sys.executable, os.path.join(SCRIPTS, 'compare_passage_policies.py'),
         *args],
        capture_output=True, text=True, cwd=os.path.dirname(SCRIPTS))


@pytest.mark.parametrize('flag', [
    '--baseline-depth', '--candidate-depth',
    '--baseline-length', '--candidate-length',
])
def test_every_policy_axis_is_reachable_from_the_command_line(flag):
    """Per SIDE, not once: the depth measurements compare depths against each
    other at a fixed width, which is a baseline-vs-candidate run.

    Asked of the real parser. The flags are built in a loop from an f-string,
    so no literal `--baseline-depth` appears in the source -- a substring
    assertion would have to match the template instead, which is the shape
    that has produced a vacuous test repeatedly on this branch.
    """
    out = _run_cli('--help')
    assert out.returncode == 0, out.stderr
    assert flag in out.stdout, f'{flag} is not accepted by the parser'


def test_the_depth_flag_actually_takes_the_profile_names():
    """The original defect, as a test: `deep` was rejected outright."""
    rejected = _run_cli('--index', 'x', '--query-file', 'y',
                        '--candidate-depth', 'deeep')
    assert rejected.returncode != 0
    assert 'invalid choice' in rejected.stderr
    # ...and a real profile name gets PAST argparse (it then fails on the
    # bogus index path, which is the next check and not this one's business).
    accepted = _run_cli('--index', 'x', '--query-file', 'y',
                        '--candidate-depth', 'deep')
    assert 'invalid choice' not in accepted.stderr, accepted.stderr


def test_the_depth_choices_are_the_shared_profiles():
    """Hard-coding the names here would let the CLI and the engine drift --
    a `--candidate-depth deepest` that silently means something else."""
    src = _cli_source()
    assert 'choices=sorted(DEPTH_PROFILES)' in src
    assert 'choices=sorted(LENGTH_PROFILES)' in src


def test_the_script_composes_rather_than_hand_rolling_a_replace():
    """`compose()` derives the composed NAME and, because policy_id is a
    content hash, its own id -- so a probe run stays traceable to exactly the
    settings that produced it. A fourth hand-rolled `replace()` would not."""
    src = _cli_source()
    assert 'compose(args.baseline' in src
    assert 'compose(args.candidate' in src
    assert 'get_preset(args.baseline)' not in src, (
        'the width-only path is back; depth and length would be ignored'
    )


# ---------------------------------------------------------------------------
# ...and they produce a genuinely different policy.
# ---------------------------------------------------------------------------

def test_depth_changes_the_three_coupled_budgets_together():
    """The three are NOT independent sliders. Raising the posting budget
    without the verify cap verifies the same 3,000 candidates and shows almost
    none of what the budget admitted -- which is why depth is one named axis
    and not three knobs."""
    normal = compose('widest-40', DEFAULT_LENGTH, 'normal')
    deepest = compose('widest-40', DEFAULT_LENGTH, 'deepest')

    assert (normal.posting_budget, normal.verify_cap, normal.candidate_cap) \
        == DEPTH_PROFILES['normal']
    assert (deepest.posting_budget, deepest.verify_cap, deepest.candidate_cap) \
        == DEPTH_PROFILES['deepest']


def test_a_composed_policy_carries_its_own_identity():
    """Two runs that differ only in depth must not report the same policy_id;
    a comparison table keyed on it would silently merge them."""
    a = compose('widest-40', DEFAULT_LENGTH, 'normal')
    b = compose('widest-40', DEFAULT_LENGTH, 'deepest')
    assert a.policy_id != b.policy_id
    assert b.name != a.name and 'deepest' in b.name


def test_the_default_depth_leaves_every_previously_measured_policy_alone():
    """`normal` must stay byte-identical to the bare preset, or every recall
    figure measured before the depth axis existed becomes unreproducible."""
    from shared.passage_policy import get_preset
    for width in ('widest-40', 'max-40', 'short-28'):
        bare = get_preset(width)
        composed = compose(width, DEFAULT_LENGTH, DEFAULT_DEPTH)
        assert composed.policy_id == bare.policy_id
        assert composed.name == bare.name


def test_length_and_depth_compose_together():
    """The flagship measurement is `max-40+short` at a chosen depth -- both
    axes at once, not either/or."""
    p = compose('max-40', 'short', 'deep')
    assert p.min_span, p.verify_margin == LENGTH_PROFILES['short']
    assert (p.posting_budget, p.verify_cap, p.candidate_cap) \
        == DEPTH_PROFILES['deep']
    assert 'short' in p.name and 'deep' in p.name


def test_an_unknown_profile_is_refused_by_name():
    """`compose` lists what it knows rather than failing opaquely -- the CLI
    relies on that for anything argparse does not catch first."""
    with pytest.raises(ValueError, match='deepset'):
        compose('widest-40', DEFAULT_LENGTH, 'deepset')
    with pytest.raises(ValueError, match='known:'):
        compose('widest-40', 'shrt', DEFAULT_DEPTH)


# ---------------------------------------------------------------------------
# The archived CSV must say which policies produced it.
# ---------------------------------------------------------------------------

def _csv_row_source() -> str:
    """Just the `w.writerow([sid, ...])` call that writes a data row.

    Scoped deliberately. Asked of the whole file, `'base_p.name' in src` stays
    true because a console `print` also mentions it -- so a mutation that put
    the raw width argument back into the CSV left the test green. That is the
    fourth time on this branch that asserting a NAME appears somewhere in a
    file has produced a test which could not fail.
    """
    src = _cli_source()
    start = src.index('w.writerow([sid, sm')
    return src[start:src.index('])', start)]


def test_the_csv_records_the_composed_policy_not_the_width_argument():
    """It wrote `args.baseline` / `args.candidate`, the raw WIDTH names. That
    was unambiguous until width, length and depth became three axes: two runs
    at different depths now write identical provenance and their archives
    cannot be told apart (Codex review of PR #328)."""
    row = _csv_row_source()
    assert 'base_p.name' in row and 'cand_p.name' in row
    assert 'args.baseline' not in row and 'args.candidate' not in row, (
        'the CSV row is back to recording the width argument only'
    )


def test_the_csv_records_the_content_hash_of_each_policy():
    """`policy_id` is a content hash, so it pins the settings even for a
    composition nobody has named -- including probe overrides, which were
    only ever printed to a transient console."""
    assert 'base_p.policy_id' in _csv_row_source()
    assert 'cand_p.policy_id' in _csv_row_source()
    src = _cli_source()
    assert "'baseline_policy_id'" in src and "'candidate_policy_id'" in src


def test_the_csv_header_and_row_stay_the_same_width():
    """A header and a row that disagree silently shift every column -- and a
    CSV nobody reads until months later is the worst place to find out."""
    src = _cli_source()
    start = src.index("w.writerow(['sys_id'")
    header = src[start:src.index('])', start)]
    n_header = header.count("'") // 2
    row_start = src.index('w.writerow([sid, sm', start)
    row = src[row_start:src.index('])', row_start)]
    n_row = row.count(',') + 1
    assert n_header == n_row, (
        f'header has {n_header} columns, the row writes {n_row}'
    )
