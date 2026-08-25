# -*- coding: utf-8 -*-
"""A delta CSV holds TWO runs; scoring it whole inflates the candidate.

`compare_passage_policies.py --csv` writes one table containing both policies'
results, distinguished only by a `presence` column (`both`, `candidate_only`,
`baseline_only`). `score_antiochus_deck.py`'s own usage block documents
`--run delta.csv` as a supported workflow, and its loader read every row's
shelfmark without ever consulting `presence` -- so it scored the UNION as though
it were the candidate. A candidate that LOST manuscripts still had their recall
counted as its own, which makes a regression read as an improvement.

That is the one output an evaluation instrument must never produce: a number
indistinguishable from a real result. (Codex review of PR #328.)

Checked before fixing: every archived run in eval/antiochus/runs/ is JSON with
no `presence` column, so no published figure came through this path.

These tests use REAL deck shelfmarks -- a fixture of invented ones would score
0 either way and could not tell the union from the candidate.
"""
from __future__ import annotations

import csv
import os
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPT = os.path.join(ROOT, 'scripts', 'score_antiochus_deck.py')
sys.path.insert(0, os.path.join(ROOT, 'scripts'))

# Real WITNESS entries from eval/antiochus/deck.json. Split so the two sides
# are DISTINGUISHABLE: whichever side the scorer reads changes the count.
CANDIDATE_POSITIVES = ['ENA 1629.10', 'ENA NS I.89c']
BASELINE_POSITIVES = ['L-G Ar.II.151', 'L-G Ar.II.152', 'MS heb. d.37/69']
SHARED_POSITIVE = 'MS heb. d.37/70'


def _delta_csv(tmp_path):
    """A delta CSV shaped exactly like compare_passage_policies.py writes."""
    path = tmp_path / 'delta.csv'
    with open(path, 'w', encoding='utf-8-sig', newline='') as fh:
        w = csv.writer(fh)
        w.writerow(['sys_id', 'shelfmark', 'shelfmark_key', 'library',
                    'title', 'presence', 'score', 'score_unit', 'record_id',
                    'baseline_policy', 'baseline_policy_id',
                    'candidate_policy', 'candidate_policy_id'])
        rows = ([(sm, 'candidate_only') for sm in CANDIDATE_POSITIVES]
                + [(sm, 'baseline_only') for sm in BASELINE_POSITIVES]
                + [(SHARED_POSITIVE, 'both')])
        for i, (sm, presence) in enumerate(rows):
            w.writerow([f'99000000000{i}', sm, sm, 'lib', 'title', presence,
                        100, 'matched_letters', f'rec{i}',
                        'widest-40', 'aaaa', 'widest-40+deepest', 'bbbb'])
    return str(path)


def _run(*args):
    return subprocess.run([sys.executable, SCRIPT, *args],
                          capture_output=True, text=True, cwd=ROOT)


def _manuscripts(stdout: str) -> int:
    """The manuscript count the scorer reports."""
    import re
    m = re.search(r'manuscripts\s*[:=]?\s*(\d+)', stdout)
    if m:
        return int(m.group(1))
    raise AssertionError(f'no manuscript count in:\n{stdout}')


def test_a_delta_csv_is_refused_without_a_side(tmp_path):
    """THE BUG. Refusing rather than defaulting to 'candidate' is deliberate:
    silently picking a side would change what an already-saved
    `--run delta.csv` command means, trading a loud wrong answer for a quiet
    one."""
    out = _run('--run', _delta_csv(tmp_path))
    assert out.returncode != 0, 'the two-run file was scored as if it were one'
    combined = out.stdout + out.stderr
    assert 'presence' in combined
    assert '--side' in combined, 'the refusal must name the way out'
    # It says what it found, so the user can tell which side they want.
    assert 'baseline_only' in combined and 'candidate_only' in combined


@pytest.mark.parametrize('side,expected', [
    ('candidate', len(CANDIDATE_POSITIVES) + 1),
    ('baseline', len(BASELINE_POSITIVES) + 1),
    ('union', len(CANDIDATE_POSITIVES) + len(BASELINE_POSITIVES) + 1),
])
def test_each_side_scores_only_its_own_manuscripts(tmp_path, side, expected):
    """The candidate must not be credited with what only the baseline found."""
    out = _run('--run', _delta_csv(tmp_path), '--side', side)
    assert out.returncode == 0, out.stderr
    assert _manuscripts(out.stdout) == expected, out.stdout


def test_the_candidate_is_not_credited_with_the_baselines_finds(tmp_path):
    """Stated as the defect rather than as an arithmetic identity: scoring the
    whole file gives the candidate a strictly better recall than it earned."""
    path = _delta_csv(tmp_path)
    cand = _manuscripts(_run('--run', path, '--side', 'candidate').stdout)
    union = _manuscripts(_run('--run', path, '--side', 'union').stdout)
    assert cand < union, (
        'the fixture cannot distinguish the two sides, so these tests would '
        'pass with the filter removed'
    )


def test_an_ordinary_single_run_export_still_needs_no_side(tmp_path):
    """Only a file that SAYS it holds two runs is refused. A GUI export has no
    `presence` column and must keep working untouched."""
    path = tmp_path / 'plain.csv'
    with open(path, 'w', encoding='utf-8-sig', newline='') as fh:
        w = csv.writer(fh)
        w.writerow(['shelfmark', 'score'])
        for sm in CANDIDATE_POSITIVES:
            w.writerow([sm, 100])
    out = _run('--run', str(path))
    assert out.returncode == 0, out.stderr
    assert _manuscripts(out.stdout) == len(CANDIDATE_POSITIVES)


def test_the_archived_table_is_unchanged(tmp_path):
    """The five committed runs are JSON with no `presence` column. If this
    moves, the published figures moved with it."""
    out = _run('--all')
    assert out.returncode == 0, out.stderr
    for name in ('letters-widest-40', 'letters-max-40', 'chunks-3',
                 'chunks-2-filtered', 'chunks-linebreaks'):
        assert name in out.stdout
    # The two letter-level rows, as recorded in the spec.
    assert '100%' in out.stdout and '1727' in out.stdout


# ---------------------------------------------------------------------------
# A shared manuscript has TWO scores, and --side baseline must read its own.
# ---------------------------------------------------------------------------

def _two_score_csv(tmp_path, cand_score, base_score):
    """A delta CSV where the shared manuscript scored DIFFERENTLY under each
    policy -- which is the normal case once the length and depth axes let the
    two accept different spans or pick a different best page."""
    path = tmp_path / 'twoscore.csv'
    with open(path, 'w', encoding='utf-8-sig', newline='') as fh:
        w = csv.writer(fh)
        w.writerow(['sys_id', 'shelfmark', 'shelfmark_key', 'library',
                    'title', 'presence', 'score', 'score_unit', 'record_id',
                    'baseline_score', 'baseline_record_id',
                    'baseline_policy', 'baseline_policy_id',
                    'candidate_policy', 'candidate_policy_id'])
        w.writerow(['990000000000', SHARED_POSITIVE, SHARED_POSITIVE, 'lib',
                    'title', 'both', cand_score, 'matched_letters', 'recC',
                    base_score, 'recB',
                    'widest-40', 'aaaa', 'widest-40+deepest', 'bbbb'])
    return str(path)


def test_the_baseline_side_reads_the_baselines_own_score():
    """`score` and `record_id` are the CANDIDATE's by contract. Reading them
    for `--side baseline` reports the candidate's number under the baseline's
    name -- the same substitution `--side` exists to stop, one column over.

    Asked of `_select_side` directly, not through the CLI. The scorer carries
    the score into its buckets but none of the figures it REPORTS uses it, so
    the wrong column is invisible in the CLI output and a mutation removing
    this remap left the whole suite green. The behaviour is real and worth
    pinning; the observation point had to move to where it is observable.
    """
    from score_antiochus_deck import _select_side
    row = {'shelfmark': SHARED_POSITIVE, 'presence': 'both',
           'score': '900', 'record_id': 'recC',
           'baseline_score': '100', 'baseline_record_id': 'recB'}

    base = _select_side([dict(row)], 'baseline')
    assert base[0]['score'] == '100', "read the candidate's score"
    assert base[0]['record_id'] == 'recB'

    cand = _select_side([dict(row)], 'candidate')
    assert cand[0]['score'] == '900', 'the candidate keeps its own'
    assert cand[0]['record_id'] == 'recC'


def test_a_delta_csv_without_baseline_columns_still_works():
    """An older CSV written before the two-score columns existed carries only
    `score`. The remap must not invent a column or blank the row."""
    from score_antiochus_deck import _select_side
    rows = [{'shelfmark': SHARED_POSITIVE, 'presence': 'both', 'score': '900'}]
    out = _select_side(rows, 'baseline')
    assert len(out) == 1
    assert out[0]['score'] == '900'


def test_the_two_sides_are_distinguishable_at_all(tmp_path):
    """Guard on the fixture: if the writer ever collapses back to one score
    column, the test above would pass while reading the wrong number."""
    path = _two_score_csv(tmp_path, cand_score=900, base_score=100)
    with open(path, encoding='utf-8-sig', newline='') as fh:
        row = next(csv.DictReader(fh))
    assert row['score'] != row['baseline_score']
    assert row['record_id'] != row['baseline_record_id']


def test_the_writer_emits_both_sides_columns():
    """The producer half of the same contract, read from the source: a delta
    CSV that keeps only one side's score cannot reproduce the baseline's
    ranking even with `--side baseline`."""
    path = os.path.join(ROOT, 'scripts', 'compare_passage_policies.py')
    with open(path, encoding='utf-8') as fh:
        src = fh.read()
    start = src.index("w.writerow(['sys_id'")
    header = src[start:src.index('])', start)]
    for col in ('baseline_score', 'baseline_record_id'):
        assert f"'{col}'" in header, f'{col} is not written'
    row_start = src.index('w.writerow([sid, sm', start)
    row = src[row_start:src.index('])', row_start)]
    # The ASSIGNMENT, not the names: a mutation writing
    # `b_hit = c_hit = cand.get(sid) or base[sid]` keeps both names in the row
    # and left this green. Fifth time on this branch that asserting a name
    # appears somewhere has produced a test which could not fail.
    assert 'b_hit, c_hit = base.get(sid), cand.get(sid)' in src, (
        'the two hits are no longer looked up independently'
    )
    assert 'b_hit' in row and 'c_hit' in row, (
        'the row still picks one hit for both sides'
    )
    # Scoped to the ROW, not the file: the comment above the writer quotes
    # the old expression to explain why it went, so `not in src` fails on the
    # explanation. Fourth time this shape has bitten on this branch.
    assert 'cand.get(sid) or base[sid]' not in row, (
        'the candidate-wins-a-tie selection is back'
    )
