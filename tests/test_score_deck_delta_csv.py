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
