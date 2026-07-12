# -*- coding: utf-8 -*-
"""MAPV2 overnight chain driver (FRAG2-PLAN step 3, launched 2026-07-10).

Sequential, RAM-safe, resumable: each step's completion is recorded in
data/mapv2_chain_state.json, so a crash/restart re-runs only the steps that
have not completed (long steps — the Track-1 v2 runner, work_query,
motif_query — additionally checkpoint INTERNALLY per batch and resume
mid-step). Stdout/stderr per step goes to results/overnight/mapv2-<step>.log;
progress + verdicts accumulate in results/MAPV2-RUN-LOG.md.

Chain (all against fullcorpus_v2.db — pages.text IS the search text):
  0. preflight     inputs exist + page-count parity with v1
  1. final-cal1    cal1_calibration.py --tag final  -> p_calibration_final
  2. track1-v2     mapv2_track1_run.py              -> tier A + tier B tables
  3. shadow        track1_shadow.py (tier A only — track1_matches)
  4. testimonies   track1_testimonies.py (+ 4-channel known-witness gate)
  5. review        build_track1_review.py            (non-critical)
  6. track2        rehearsal_run.py fullv2 maskcanon (~35 min)
  7. map           rehearsal_map.py                  (non-critical)
  7b. atlas        build_rehearsal_atlas.py 120      (non-critical, HTML)
  8. graph         build_reuse_graph.py              (non-critical)
  9. chains        chain_pages.py                    (non-critical)
 10. units         passage_units.py
 11. motifs        motif_pilot.py
 12. motif-query   motif_query.py                    (checkpointed)
 13. work-query    work_query.py --census-db v2 --ref v2 --masks v2
                   (checkpointed)

Launch (detached, BelowNormal — PC-crash lessons codified):
  powershell -Command "Start-Process -WindowStyle Hidden `
    -FilePath python -ArgumentList '-X','utf8','-u','mapv2_overnight.py' `
    -WorkingDirectory 'C:\\Genizahsearch\\same_work_spike\\probe\\scripts'"
  (the driver drops its own priority class; children inherit)
"""
import datetime
import json
import os
import sqlite3
import subprocess
import sys
import time

SCRIPTS = os.path.dirname(os.path.abspath(__file__))
PROBE = os.path.dirname(SCRIPTS)
RESULTS = os.path.join(PROBE, 'results')
LOGDIR = os.path.join(RESULTS, 'overnight')
RUNLOG = os.path.join(RESULTS, 'MAPV2-RUN-LOG.md')
STATE = os.path.join(PROBE, 'data', 'mapv2_chain_state.json')

DB_V1 = os.path.join(PROBE, 'data', 'fullcorpus.db')
DB_V2 = os.path.join(PROBE, 'data', 'fullcorpus_v2.db')
REF_V2 = os.path.join(PROBE, 'data', 'ref_corpus_v2.pkl')
MASKS_V2 = os.path.join(PROBE, 'data', 'ref_canon_masks_v2.json')
P_FINAL = os.path.join(PROBE, 'data', 'p_calibration_final.json')
KNOWN = os.path.join(PROBE, 'data', 'known_witnesses_all.json')
PY = sys.executable

os.makedirs(LOGDIR, exist_ok=True)


def log(msg):
    stamp = datetime.datetime.now().strftime('%H:%M:%S')
    line = f"- {stamp} {msg}"
    print(line, flush=True)
    with open(RUNLOG, 'a', encoding='utf-8') as f:
        f.write(line + "\n")


def load_state():
    if os.path.exists(STATE):
        try:
            return json.load(open(STATE, encoding='utf-8'))
        except Exception:
            pass
    return {'done': []}


def mark_done(state, name):
    state['done'].append(name)
    tmp = STATE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=1)
    os.replace(tmp, STATE)


def run_step(state, name, args, critical=True):
    if name in state['done']:
        log(f"SKIP  {name} (already completed)")
        return True
    logf = os.path.join(LOGDIR, f"mapv2-{name}.log")
    log(f"START {name}: {' '.join(args)}")
    t0 = time.time()
    with open(logf, 'a', encoding='utf-8') as f:
        f.write(f"\n===== {datetime.datetime.now():%Y-%m-%d %H:%M:%S} "
                f"{' '.join(args)} =====\n")
        f.flush()
        p = subprocess.run([PY, '-X', 'utf8', '-u'] + args, cwd=SCRIPTS,
                           stdout=f, stderr=subprocess.STDOUT)
    dur = (time.time() - t0) / 60
    if p.returncode == 0:
        log(f"OK    {name} ({dur:.0f} min)")
        mark_done(state, name)
        return True
    log(f"FAIL  {name} exit={p.returncode} ({dur:.0f} min) — see {logf}")
    if critical:
        log("ABORT — critical step failed; chain stopped (re-launch resumes "
            "from this step; long steps resume mid-step via their own "
            "checkpoints)")
        sys.exit(1)
    return False


def preflight():
    problems = []
    for path, what in [(DB_V1, 'v1 corpus'), (DB_V2, 'v2 corpus (stage-0)'),
                       (REF_V2, 'v2 reference pkl'),
                       (MASKS_V2, 'v2 canonical masks'),
                       (KNOWN, 'known-witness table')]:
        if not os.path.exists(path):
            problems.append(f"missing {what}: {path}")
    if problems:
        return problems
    c1 = sqlite3.connect(f"file:{DB_V1}?mode=ro", uri=True)
    c2 = sqlite3.connect(f"file:{DB_V2}?mode=ro", uri=True)
    n1 = c1.execute("SELECT COUNT(*) FROM pages").fetchone()[0]
    n2 = c2.execute("SELECT COUNT(*) FROM pages").fetchone()[0]
    if n1 != n2:
        problems.append(f"page-count mismatch v1={n1:,} v2={n2:,}")
    cols = {r[1] for r in c2.execute("PRAGMA table_info(pages)")}
    for col in ('provenance', 'text', 'sys_id'):
        if col not in cols:
            problems.append(f"v2 pages missing column {col}")
    n_fgp = c2.execute("SELECT COUNT(*) FROM pages WHERE provenance='fgp'"
                       ).fetchone()[0]
    log(f"preflight: {n2:,} v2 pages, {n_fgp:,} FGP-substituted; "
        f"masks {os.path.getsize(MASKS_V2):,} B; ref "
        f"{os.path.getsize(REF_V2) // 1048576} MB")
    c1.close()
    c2.close()
    return problems


def main():
    # drop our own priority; child processes inherit
    try:
        import ctypes
        BELOW_NORMAL = 0x4000
        ctypes.windll.kernel32.SetPriorityClass(
            ctypes.windll.kernel32.GetCurrentProcess(), BELOW_NORMAL)
    except Exception as e:                                    # noqa: BLE001
        print(f"priority drop failed (continuing): {e!r}", flush=True)

    with open(RUNLOG, 'a', encoding='utf-8') as f:
        f.write(f"\n## MAPV2 overnight chain — "
                f"{datetime.datetime.now():%Y-%m-%d %H:%M}\n")
    state = load_state()
    if state['done']:
        log(f"resume: steps already done: {state['done']}")

    problems = preflight()
    if problems:
        for p in problems:
            log(f"PREFLIGHT FAIL: {p}")
        sys.exit(1)

    run_step(state, '1-final-cal1',
             ['cal1_calibration.py', '--tag', 'final'])
    run_step(state, '2-track1-v2',
             ['mapv2_track1_run.py', DB_V2, 'v2', P_FINAL])
    run_step(state, '3-shadow', ['track1_shadow.py', DB_V2, 'fullv2'])
    run_step(state, '4-testimonies',
             ['track1_testimonies.py', DB_V2, 'fullv2'])
    run_step(state, '5-review',
             ['build_track1_review.py', DB_V2, 'fullv2'], critical=False)
    run_step(state, '6-track2',
             ['rehearsal_run.py', DB_V2, 'fullv2', 'maskcanon'])
    run_step(state, '7-map',
             ['rehearsal_map.py', DB_V2, 'fullv2',
              'accepted_pairs_canonmask'], critical=False)
    run_step(state, '7b-atlas',
             ['build_rehearsal_atlas.py', DB_V2, 'fullv2', '120',
              'accepted_pairs_canonmask'], critical=False)
    run_step(state, '8-graph',
             ['build_reuse_graph.py', DB_V2, 'fullv2',
              'accepted_pairs_canonmask'], critical=False)
    run_step(state, '9-chains',
             ['chain_pages.py', DB_V2, 'fullv2',
              'accepted_pairs_canonmask'], critical=False)
    run_step(state, '10-units',
             ['passage_units.py', DB_V2, 'fullv2',
              'accepted_pairs_canonmask'])
    run_step(state, '11-motifs',
             ['motif_pilot.py', DB_V2, 'accepted_pairs_canonmask', 'pilot'])
    run_step(state, '12-motif-query', ['motif_query.py', DB_V2, '3', '100'])
    run_step(state, '13-work-query',
             ['work_query.py', DB_V2, 'fullv2', '--census-db', DB_V2,
              '--ref', REF_V2, '--masks', MASKS_V2])
    log("MAPV2 CHAIN COMPLETE — tier A census + tier B candidates + Track-2 "
        "map rebuilt on the v2 state; products (delta report, discovery "
        "deck, blinded deck) run next, interactively")


if __name__ == '__main__':
    main()
