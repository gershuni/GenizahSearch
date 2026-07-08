# -*- coding: utf-8 -*-
"""Unattended full-corpus pipeline driver (launched 2026-07-07 night).

Waits for the running Track-1 full pass (track1_match.py fullcorpus full)
to finish, then chains every remaining SEED-029 full-corpus step
sequentially (RAM-safe: nothing heavy runs concurrently). Each step's
stdout/stderr goes to results/overnight/<step>.log; progress + verdicts
accumulate in results/OVERNIGHT-RUN-LOG.md. Failures of non-critical
steps (atlas/graph) are logged and skipped; failures of critical steps
abort the chain with a clear log entry.

Steps:
  0. wait for results/track1_full_stats.json (Track-1 completion);
     abort if the track1 process is gone and no stats appeared
  1. parity_spill.py ram      (needs the RAM Track-1 was holding)
  2. parity_spill.py compare  -> byte-level spill-engine verdict
  3. track1_testimonies.py fullcorpus full   (census + bib demotion)
  4. build_track1_review.py fullcorpus full  (400-card review page)
  5. rehearsal_run.py fullcorpus full maskcanon  (THE map run, spill
     engine, ~4-6h)
  6. rehearsal_map.py fullcorpus full accepted_pairs_canonmask
  7. build_rehearsal_atlas.py fullcorpus full accepted_pairs_canonmask
  8. build_reuse_graph.py fullcorpus full accepted_pairs_canonmask
"""
import datetime
import os
import subprocess
import sys
import time

SCRIPTS = os.path.dirname(os.path.abspath(__file__))
ROOT = r"C:\Genizahsearch"
RESULTS = ROOT + r"\same_work_spike\probe\results"
LOGDIR = RESULTS + r"\overnight"
RUNLOG = RESULTS + r"\OVERNIGHT-RUN-LOG.md"
DB = ROOT + r"\same_work_spike\probe\data\fullcorpus.db"
T1_STATS = RESULTS + r"\track1_full_stats.json"
PY = sys.executable

os.makedirs(LOGDIR, exist_ok=True)


def log(msg):
    stamp = datetime.datetime.now().strftime('%H:%M:%S')
    line = f"- {stamp} {msg}"
    print(line, flush=True)
    with open(RUNLOG, 'a', encoding='utf-8') as f:
        f.write(line + "\n")


def track1_alive():
    try:
        out = subprocess.run(
            ['powershell', '-NoProfile', '-Command',
             "(Get-CimInstance Win32_Process -Filter \"Name like 'python%'\""
             " | Where-Object {$_.CommandLine -match 'track1_match'} |"
             " Measure-Object).Count"],
            capture_output=True, text=True, timeout=60)
        return int(out.stdout.strip() or 0) > 0
    except Exception:
        return True   # can't tell -> keep waiting


def run_step(name, args, critical=True):
    logf = os.path.join(LOGDIR, f"{name}.log")
    log(f"START {name}: {' '.join(args)}")
    t0 = time.time()
    with open(logf, 'w', encoding='utf-8') as f:
        p = subprocess.run([PY, '-X', 'utf8'] + args, cwd=SCRIPTS,
                           stdout=f, stderr=subprocess.STDOUT)
    dur = (time.time() - t0) / 60
    if p.returncode == 0:
        log(f"OK    {name} ({dur:.0f} min)")
        return True
    log(f"FAIL  {name} exit={p.returncode} ({dur:.0f} min) — see {logf}")
    if critical:
        log("ABORT — critical step failed; chain stopped")
        sys.exit(1)
    return False


def main():
    with open(RUNLOG, 'a', encoding='utf-8') as f:
        f.write(f"\n## Overnight full-corpus run — "
                f"{datetime.datetime.now():%Y-%m-%d %H:%M}\n")
    # ---- step 0: wait for Track-1 ----
    log("waiting for Track-1 full pass (track1_full_stats.json)…")
    waited = 0
    while not os.path.exists(T1_STATS):
        if not track1_alive():
            time.sleep(120)   # grace: stats write may lag process listing
            if os.path.exists(T1_STATS):
                break
            log("ABORT — track1_match process gone but no stats file; "
                "Track-1 crashed. Re-run: python track1_match.py "
                f"{DB} full")
            sys.exit(1)
        time.sleep(300)
        waited += 5
        if waited % 30 == 0:
            log(f"  still waiting ({waited} min)")
        if waited > 14 * 60:
            log("ABORT — 14h wait exceeded")
            sys.exit(1)
    log(f"Track-1 done (waited {waited} min)")

    run_step('1-parity-ram', ['parity_spill.py', 'ram'])
    run_step('2-parity-compare', ['parity_spill.py', 'compare'])
    run_step('3-testimonies', ['track1_testimonies.py', DB, 'full'])
    run_step('4-review', ['build_track1_review.py', DB, 'full'],
             critical=False)
    run_step('5-track2-canonmask',
             ['rehearsal_run.py', DB, 'full', 'maskcanon'])
    run_step('6-map', ['rehearsal_map.py', DB, 'full',
                       'accepted_pairs_canonmask'])
    run_step('7-atlas', ['build_rehearsal_atlas.py', DB, 'full', '120',
                         'accepted_pairs_canonmask'], critical=False)
    run_step('8-graph', ['build_reuse_graph.py', DB, 'full',
                         'accepted_pairs_canonmask'], critical=False)
    log("CHAIN COMPLETE — all artifacts under results/ + review/ "
        "(tags *_full*)")


if __name__ == '__main__':
    main()
