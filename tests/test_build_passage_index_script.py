# -*- coding: utf-8 -*-
"""`scripts/build_passage_index.py` must not report a failed build as a success.

This script runs unattended on the web server, usually under `nohup`, and the
only thing anybody reads afterwards is its exit code and its last few lines.
So the property that matters is not that it builds -- the lifecycle module is
tested for that -- but that every way of NOT building is reported as one.

`run_build_and_swap` RETURNS a failed build as a status rather than raising it.
The desktop worker got this wrong once and announced a failed build as a
completed one (Codex review round 3, PR #331); a `try/except` around the call
does not catch it, because nothing was thrown. These tests pin the same trap
shut here, one status at a time.
"""
from __future__ import annotations

import importlib.util
import os
import sys

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)


@pytest.fixture(scope='module')
def script():
    path = os.path.join(REPO_ROOT, 'scripts', 'build_passage_index.py')
    spec = importlib.util.spec_from_file_location('build_passage_index', path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _Result:
    def __init__(self, status, error='', live_dir='', quarantine_dir=''):
        self.status = status
        self.error = error
        self.live_dir = live_dir
        self.quarantine_dir = quarantine_dir
        self.index = None
        self.stats = None


def _run(script, monkeypatch, tmp_path, result=None, raises=None):
    corpus = tmp_path / 'Transcriptions.txt'
    corpus.write_text('nothing in particular', encoding='utf-8')
    root = tmp_path / 'passage_index'

    def fake(*a, **k):
        if raises is not None:
            raise raises
        return result

    monkeypatch.setattr(script.passage_lifecycle, 'run_build_and_swap', fake)
    # The real preflight would refuse this tmp dir on size grounds long before
    # reaching the part under test.
    monkeypatch.setattr(script, 'check_free_space', lambda *a, **k: None)
    monkeypatch.setattr(script.passage_lifecycle, 'estimate_build_bytes',
                        lambda *a, **k: 1024)
    monkeypatch.setattr(sys, 'argv',
                        ['build_passage_index.py', '--root', str(root),
                         '--corpus', str(corpus)])
    return script.main()


def test_a_returned_error_reports_the_failure_it_was_given(script, monkeypatch,
                                                           tmp_path, capsys):
    """The trap. `status='error'` is RETURNED, so nothing raises.

    Asserting only a non-zero exit here would be VACUOUS, and was: deleting the
    error branch outright left this green, because the `!= 'installed'`
    catch-all below it also returns 1. Proven by mutation, which is why this
    test now asserts something else.

    What the branch uniquely owns is the DIAGNOSTIC. The catch-all reports
    "the previous index is still live and working; nothing was lost" -- true of
    a refused swap, potentially quite false of a build that died partway -- and
    it never names the quarantine directory, the one path an operator needs in
    order to go and look at the wreckage.
    """
    rc = _run(script, monkeypatch, tmp_path,
              _Result('error', error='disk went away',
                      quarantine_dir='/srv/idx/.failed-7'))
    err = capsys.readouterr().err
    assert rc != 0, 'a failed build reported success to the shell'
    assert 'disk went away' in err, (
        'the failure was reported without the reason the builder gave')
    assert '/srv/idx/.failed-7' in err, (
        'the quarantined tree was never named, so nobody can go and look')
    assert 'nothing was lost' not in err, (
        'a failed build was described with the reassurance meant for a '
        'refused swap')


def test_a_swap_that_did_not_install_is_a_nonzero_exit(script, monkeypatch,
                                                       tmp_path):
    """`readers_active` leaves the PREVIOUS index live and working. That is not
    a disaster, but it is also not the new index -- and an operator who reads
    "done" will flip the flag believing the corpus changed under it."""
    rc = _run(script, monkeypatch, tmp_path, _Result('readers_active'))
    assert rc != 0


def test_a_cancelled_build_is_a_nonzero_exit(script, monkeypatch, tmp_path):
    rc = _run(script, monkeypatch, tmp_path, _Result('cancelled'))
    assert rc != 0


def test_a_raised_cancellation_is_a_nonzero_exit(script, monkeypatch, tmp_path):
    rc = _run(script, monkeypatch, tmp_path,
              raises=script.passage_lifecycle.BuildCancelled())
    assert rc != 0


def test_an_installed_build_is_a_zero_exit(script, monkeypatch, tmp_path):
    """And the happy path still succeeds -- otherwise the tests above pass by
    a script that can only ever fail."""
    live = tmp_path / 'passage_index' / 'current'
    live.mkdir(parents=True)
    rc = _run(script, monkeypatch, tmp_path,
              _Result('installed', live_dir=str(live)))
    assert rc == 0


def test_a_missing_corpus_is_refused_before_anything_else(script, monkeypatch,
                                                          tmp_path):
    monkeypatch.setattr(sys, 'argv',
                        ['build_passage_index.py',
                         '--root', str(tmp_path / 'idx'),
                         '--corpus', str(tmp_path / 'nope.txt')])
    assert script.main() != 0


def test_check_mode_builds_nothing(script, monkeypatch, tmp_path):
    """`--check` is what an operator runs first on a live box. It must be
    incapable of starting a ten-minute job by accident."""
    corpus = tmp_path / 'Transcriptions.txt'
    corpus.write_text('x', encoding='utf-8')
    called = []
    monkeypatch.setattr(script.passage_lifecycle, 'run_build_and_swap',
                        lambda *a, **k: called.append(1))
    monkeypatch.setattr(script, 'check_free_space', lambda *a, **k: None)
    monkeypatch.setattr(script.passage_lifecycle, 'estimate_build_bytes',
                        lambda *a, **k: 1024)
    monkeypatch.setattr(sys, 'argv',
                        ['build_passage_index.py', '--check',
                         '--root', str(tmp_path / 'idx'),
                         '--corpus', str(corpus)])
    assert script.main() == 0
    assert not called, '--check started a build'


def test_the_release_seam_accepts_the_generation_positionally(script):
    """The seam contract: it is handed the generation and must accept it. A
    seam that has not been taught it fails on the first build rather than
    closing the wrong generation later, so the no-op must not quietly take
    zero arguments."""
    assert script._no_live_state(7) is True
    with pytest.raises(TypeError):
        script._no_live_state()
