# -*- coding: utf-8 -*-
"""Morning product chain: waits for the MAPV2 overnight chain to finish
(state json contains '13-work-query'), then runs the product/test steps that
need exclusive read access to fullcorpus_v2.db:

  M1. mapv2_delta_report.py           tier-A v1<->v2 delta + invariant checks
  M2. mapv2_deck.py (full corpus)     discovery deck + blinded deck + report

Detached-safe (own RUNLOG section, BelowNormal), resumable: completed morning
steps are recorded in data/mapv2_morning_state.json. Gives up with a loud log
line if the overnight chain hasn't finished within MAX_WAIT_H hours (crash
watchdog — a human/agent should check MAPV2-RUN-LOG.md).

Usage: python -X utf8 -u mapv2_morning.py
"""
import ctypes
import json
import os
import subprocess
import sys
import time

PROBE = r"C:\Genizahsearch\same_work_spike\probe"
SCRIPTS = os.path.join(PROBE, 'scripts')
CHAIN_STATE = os.path.join(PROBE, 'data', 'mapv2_chain_state.json')
STATE = os.path.join(PROBE, 'data', 'mapv2_morning_state.json')
RUNLOG = os.path.join(PROBE, 'results', 'MAPV2-RUN-LOG.md')
LOGDIR = os.path.join(PROBE, 'results', 'overnight')
FINAL_STEP = '13-work-query'
MAX_WAIT_H = 16
POLL_S = 180

STEPS = [
    ('M1-delta', ['mapv2_delta_report.py']),
    ('M2-deck', ['mapv2_deck.py',
                 '--outdir', os.path.join(PROBE, 'review', 'full_deck'),
                 '--label', 'הקורפוס המלא (667 אלף עמודים)']),
]


def log(msg):
    line = f"- {time.strftime('%H:%M:%S')} {msg}"
    print(line, flush=True)
    with open(RUNLOG, 'a', encoding='utf-8') as f:
        f.write(line + "\n")


def main():
    try:
        ctypes.windll.kernel32.SetPriorityClass(
            ctypes.windll.kernel32.GetCurrentProcess(), 0x4000)  # BelowNormal
    except Exception:
        pass
    with open(RUNLOG, 'a', encoding='utf-8') as f:
        f.write(f"\n## MAPV2 morning chain — {time.strftime('%Y-%m-%d %H:%M')}\n")

    t0 = time.time()
    while True:
        try:
            done = json.load(open(CHAIN_STATE, encoding='utf-8'))['done']
        except Exception:
            done = []
        if FINAL_STEP in done:
            log(f"overnight chain complete ({len(done)} steps) — starting "
                f"morning products")
            break
        if time.time() - t0 > MAX_WAIT_H * 3600:
            log(f"GAVE UP: overnight chain not complete after {MAX_WAIT_H}h "
                f"(done: {done}) — check for a crash and resume it")
            sys.exit(1)
        time.sleep(POLL_S)

    mstate = {'done': []}
    if os.path.exists(STATE):
        mstate = json.load(open(STATE, encoding='utf-8'))
    for name, cmd in STEPS:
        if name in mstate['done']:
            log(f"SKIP  {name} (already done)")
            continue
        log(f"START {name}: {' '.join(cmd)}")
        lp = os.path.join(LOGDIR, f'morning-{name}.log')
        with open(lp, 'a', encoding='utf-8') as lf:
            rc = subprocess.call(
                [sys.executable, '-X', 'utf8', '-u'] + cmd,
                cwd=SCRIPTS, stdout=lf, stderr=subprocess.STDOUT)
        if rc != 0:
            log(f"FAIL  {name} (exit {rc}) — chain stopped; see {lp}")
            sys.exit(rc)
        mstate['done'].append(name)
        json.dump(mstate, open(STATE, 'w', encoding='utf-8'))
        log(f"OK    {name}")
    log("morning chain COMPLETE")


if __name__ == '__main__':
    main()
