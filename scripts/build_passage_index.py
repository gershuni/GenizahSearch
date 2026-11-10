# -*- coding: utf-8 -*-
"""Build the passage index where the web app will read it. Server-safe.

`scripts/bench_passage_build.py` is a MEASUREMENT harness -- it builds
repeatedly, compares constructions, and says so in its own docstring: "Dev-box
/ owner-machine only. Never run it on the web server." That left no way to
build the index on the box that serves it, which is the only place the corpus
and the index are guaranteed to be the same corpus. This is that way.

It is a thin wrapper on purpose. `desktop/passage_lifecycle.py` already owns
the whole build -> validate -> atomic-swap -> rollback -> recovery path, it is
covered by the lifecycle test suite, and despite living under `desktop/` it
imports no Qt whatsoever -- only stdlib and `shared/`. Reimplementing any of
that here would mean a second, less-tested copy of the one part of this
feature that can destroy a working index.

WHERE IT WRITES
    `--root` defaults to the repo-root `passage_index/` directory, so the
    build lands in `passage_index/current/` -- exactly the path
    `web/passage_assets.py` reads by default. Point `--root` elsewhere only if
    `GENIZAH_PASSAGE_DATA_DIR` points the web app elsewhere too; the two are
    one decision and disagreeing on it is a silent no-op launch, where the
    feature stays hidden because the loader is fail-closed.

A RUNNING WEB APP IS NOT DISTURBED
    `load_passage_state()` runs once at startup and the index is mmapped for
    the process's life. On Linux a rename over an open mmap is legal -- the
    server keeps serving the OLD index from its now-unlinked inode and notices
    nothing. So this is safe to run against a live site, and the new index
    does NOT take effect until the service restarts. That is the whole
    deployment story: build, verify, then restart.

    (This is exactly why the desktop needs an elaborate handle-release
    protocol and this script does not: Windows refuses the rename, Linux does
    not care. `--release-timeout` is irrelevant here for the same reason --
    there is no in-process index to release, so the seam is a no-op.)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from desktop import passage_lifecycle                            # noqa: E402
from shared.passage_builder import check_free_space              # noqa: E402
from shared.passage_corpus import iter_records                   # noqa: E402

# 16, not the shared default of 8. Measured peak RSS at P=8 is 3.5 GB and
# halving the partition size halves it; a box that is also serving the site
# should not surrender 3.5 GB for ten minutes when 1.75 GB buys the same
# artifact. See docs/specs/passage-index-build-measurements.md.
SERVER_PARTITIONS = 16

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_ROOT = os.path.join(REPO_ROOT, 'passage_index')


def _gb(n):
    return '%.1f GB' % (n / (1024.0 ** 3))


def _default_corpus():
    """The corpus the site itself serves, not a copy of it."""
    env = os.environ.get('GENIZAH_TRANSCRIPTIONS')
    if env:
        return env
    try:
        from shared.config import Config
        return Config.FILE_V8
    except Exception:                                        # noqa: BLE001
        return os.path.join(REPO_ROOT, 'Transcriptions.txt')


class _Reporter:
    """One line per phase change, plus a heartbeat. Deliberately terse: this
    runs over ssh, often under nohup, and a progress bar redrawn thousands of
    times makes a log nobody can read."""

    def __init__(self, every_seconds=15.0):
        self._every = every_seconds
        self._last = 0.0
        self._phase = None
        self._t0 = time.time()

    def __call__(self, phase, done=0, total=0, elapsed=0.0):
        now = time.time()
        new_phase = phase != self._phase
        if new_phase:
            self._phase = phase
            self._last = 0.0
        if not new_phase and (now - self._last) < self._every:
            return
        self._last = now
        stamp = '%6.1fs' % (now - self._t0)
        if phase == 'pass1':
            # `total` is records INDEXED, not a denominator -- pass 1 streams
            # a file of unknown record count. Printing "done/total" here would
            # invent a percentage that cannot exist.
            print('[%s] pass 1: %s records seen, %s indexed'
                  % (stamp, format(done, ','), format(total, ',')), flush=True)
        elif phase == 'pass2':
            print('[%s] pass 2: partition %d of %d'
                  % (stamp, done, total), flush=True)
        else:
            print('[%s] %s: %s/%s' % (stamp, phase, done, total), flush=True)

    def records(self, path):
        for rec in iter_records(path):
            yield rec


def _no_live_state(expect_generation):
    """The release seam.

    In the desktop this hops to the UI thread and closes a live index before
    the rename. Here there is no in-process index to close: the web app is a
    different process entirely, and on Linux its open mmap does not block the
    rename. The generation is accepted positionally because the seam contract
    requires it -- a seam that has not been taught the generation fails loudly
    on the first build rather than closing the wrong one later.
    """
    del expect_generation
    return True


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('--root', default=DEFAULT_ROOT,
                    help='directory CONTAINING current/ (default: %(default)s)')
    ap.add_argument('--corpus', default=None,
                    help='transcriptions file (default: the app\'s own)')
    ap.add_argument('--partitions', type=int, default=SERVER_PARTITIONS)
    ap.add_argument('--label', default='',
                    help='corpus label recorded in the manifest')
    ap.add_argument('--progress-seconds', type=float, default=15.0)
    ap.add_argument('--check', action='store_true',
                    help='preflight only: report paths, sizes and free space, '
                         'then exit without building')
    args = ap.parse_args()

    corpus = args.corpus or _default_corpus()
    if not os.path.exists(corpus):
        print('corpus not found: %s' % corpus, file=sys.stderr)
        return 2

    root = os.path.abspath(args.root)
    live = os.path.join(root, 'current')
    needed = passage_lifecycle.estimate_build_bytes(corpus)

    print('corpus     : %s (%s)' % (corpus, _gb(os.path.getsize(corpus))))
    print('index root : %s' % root)
    print('live dir   : %s%s'
          % (live, '' if os.path.isdir(live) else '   (none yet)'))
    print('partitions : %d' % args.partitions)
    print('needs      : %s free while building; the finished index is about '
          '3.5 GB' % _gb(needed))

    os.makedirs(root, exist_ok=True)
    try:
        # Deliberately BEFORE the build rather than inside it: being refused
        # after ten minutes of work is a worse answer than being refused now.
        check_free_space(root, needed)
    except Exception as exc:                                 # noqa: BLE001
        print('\nnot enough free space: %s' % exc, file=sys.stderr)
        return 2
    print('free space : OK')

    if args.check:
        print('\n--check given; nothing built.')
        return 0

    reporter = _Reporter(args.progress_seconds)
    print('\nbuilding. The running site keeps serving its current index '
          'until you restart it.\n', flush=True)
    t0 = time.time()
    try:
        result = passage_lifecycle.run_build_and_swap(
            root, reporter.records(corpus), [corpus], corpus,
            partitions=args.partitions,
            corpus_label=args.label,
            progress=reporter,
            release_live_state=_no_live_state)
    except passage_lifecycle.BuildCancelled:
        print('cancelled; the existing index was not touched.', file=sys.stderr)
        return 1
    except Exception as exc:                                 # noqa: BLE001
        print('build failed: %s' % exc, file=sys.stderr)
        return 1

    took = time.time() - t0

    # `run_build_and_swap` RETURNS a failed build as a status rather than
    # raising it, so a bare try/except would report this one as a success.
    if result.status == 'error':
        print('\nbuild failed: %s' % (result.error or '(no detail)'),
              file=sys.stderr)
        if result.quarantine_dir:
            print('quarantined at: %s' % result.quarantine_dir, file=sys.stderr)
        return 1
    if result.status == 'cancelled':
        print('\ncancelled; the existing index was not touched.',
              file=sys.stderr)
        return 1
    if result.status != 'installed':
        # readers_active and friends: a working index is still in place, but
        # it is the OLD one, and saying "done" here would be a lie.
        print('\nnot installed (%s). The previous index is still live and '
              'working; nothing was lost. %s'
              % (result.status, result.error or ''), file=sys.stderr)
        return 1

    print('\ninstalled in %.1f s at %s' % (took, result.live_dir or live))

    manifest = os.path.join(result.live_dir or live, 'manifest.json')
    try:
        with open(manifest, encoding='utf-8') as fh:
            m = json.load(fh)
        counts = m.get('counts', {})
        excluded = (m.get('build') or {}).get('excluded', {})
        print('records    : %s indexed' % format(counts.get('n_records', 0), ','))
        print('letters    : %s' % format(counts.get('n_letters', 0), ','))
        print('postings   : %s' % format(counts.get('n_postings', 0), ','))
        if excluded:
            print('excluded   : %s'
                  % ', '.join('%s %s' % (k, format(v, ','))
                              for k, v in sorted(excluded.items())))
    except Exception as exc:                                 # noqa: BLE001
        # The build is installed and validated either way -- open_index has
        # already accepted it. A manifest we cannot pretty-print is a
        # reporting problem, not a reason to report failure.
        print('(could not summarise the manifest: %s)' % exc)

    print('\nThe site is still serving its previous index. Restart the '
          'service to pick this one up.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
