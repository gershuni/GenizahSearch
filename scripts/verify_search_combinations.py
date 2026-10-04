# -*- coding: utf-8 -*-
"""Real-index check of search combinations (D8, 2026-10-04). FAILS on missing data.

WHAT IT CHECKS
--------------
The desktop shows a search cut off at its 50,000-candidate limit ("N+"), and a
combination built on such a step must not miss what the cut left out:

1. Restriction. Search-within puts the manuscripts in the query at any size
   (until 2026-10-04 it searched the whole corpus above 500 manuscripts, kept the
   first 50,000 hits and filtered them: 69-81% of the true pages lost). For
   Exact and Variants, משה within the manuscripts of ישראל must be exactly the
   pages of an unrestricted משה search whose manuscript is among them. A page id
   catalogued under two manuscripts can be found through the copy inside the set
   while the unrestricted search kept the copy outside it; such an extra counts
   only if that copy holds a whole-word match.
2. Completion. ישראל at the display limit (cut off) and משה within it (cut off
   too) are completed by shared.refinement.complete_chain: the first must hold
   every manuscript of an uncapped ישראל search, the second every page of the
   brute force above. A Stop part-way must come back interrupted and leave the
   chain as it was.

Every expected value is computed in the same run from an uncapped search, so a
rebuilt index needs no new numbers. The check is also refused when it has no
teeth: if ישראל no longer reaches the limit, or the cut-off step already equals
the complete one, the run FAILS ("vacuous") rather than passing a check that
could not have failed.

WHY THIS IS NOT A TEST
----------------------
It needs the real index and takes minutes. A pytest test that skipped without
the data would report success for a check it never ran, so the contract is
inverted, as in scripts/verify_review_artifact.py: missing data is a failure.
Run it nightly on the machine that holds the index
(scripts/schedule_nightly_search_gate.ps1), and by hand before merging a change
to search-within, the cut-off signal or the ids-only mode.

Exit codes: 0 verified · 1 verification failed · 2 required data missing.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
import threading
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

EXIT_OK, EXIT_FAILED, EXIT_MISSING_DATA = 0, 1, 2
PARENT, CHILD = "ישראל", "משה"
ORACLE_LIMIT = 5_000_000          # far above any query's candidate count


def _git(*args):
    try:
        out = subprocess.run(("git",) + args, cwd=REPO_ROOT, capture_output=True,
                             text=True, timeout=30)
        return out.stdout.strip()
    except Exception:  # noqa: BLE001 -- the log records what it can
        return ""


def missing_data(index_dir: str, libraries_csv: str) -> list:
    """What this run needs and cannot find (empty when everything is there)."""
    need = [
        ("search index", os.path.join(index_dir, "tantivy_db", "meta.json")),
        ("browse map", os.path.join(index_dir, "browse_map.pkl")),
        ("libraries.csv", libraries_csv),
    ]
    return [f"{what}: {path}" for what, path in need if not os.path.isfile(path)]


class _Report:
    def __init__(self):
        self.checks = []

    def check(self, name, ok, detail=""):
        self.checks.append({"name": name, "ok": bool(ok), "detail": detail})
        print(f"{'PASS' if ok else 'FAIL'}  {name}  {detail}", flush=True)

    @property
    def failed(self):
        return [c for c in self.checks if not c["ok"]]


def _uids(rows):
    return {r["uid"] for r in rows}


def _sys_ids(rows):
    return {r["display"]["id"] for r in rows}


def check_restriction(eng, meta, se, mode, parent_ids, report):
    """Restriction in the query: the restricted search equals the brute force."""
    from unittest.mock import patch
    from shared.config import Config
    with patch.object(Config, "SEARCH_LIMIT", ORACLE_LIMIT):
        full = eng.execute_search(CHILD, mode, 0, corpus_scope="genizah")
        got = _uids(eng.execute_search(CHILD, mode, 0, restrict_sys_ids=parent_ids,
                                       corpus_scope="genizah"))
    brute = {r["uid"] for r in full if r["display"]["id"] in parent_ids}
    missing, extra = brute - got, got - brute
    rx = eng.build_regex_pattern([CHILD], mode, 0)
    unexplained = set()
    for uid in extra:
        if not _a_copy_inside_matches(eng, meta, se, rx, uid, parent_ids):
            unexplained.add(uid)
    report.check(f"restriction ({mode})", brute and not missing and not unexplained,
                 f"{len(got):,} pages = brute force {len(brute):,}; missing {len(missing)}, "
                 f"extra {len(extra)} ({len(extra) - len(unexplained)} verified copies)")


def _a_copy_inside_matches(eng, meta, se, rx, uid, parent_ids):
    """Whether a copy of page *uid* filed under a manuscript in *parent_ids* holds a
    whole-word match (the one way a restricted search may find a page the
    unrestricted one kept outside the set)."""
    q = eng.index.parse_query(f'unique_id:"{uid}"', ["unique_id"])
    for _score, addr in eng.searcher.search(q, 20).hits:
        doc = eng.searcher.doc(addr)
        if meta.get_display_data(doc.get_first("full_header"), doc.get_first("source"))["id"] not in parent_ids:
            continue
        text = se._strip_brackets(doc.get_first("content") or "")
        m = rx.search(text)
        if m is not None and se._first_accepted_match(
                rx, text, m, lambda x, _t=text: se._whole_word_span(_t, x.start(), x.end())):
            return True
    return False


def check_completion(eng, se, parent_ids, report):
    """A cut-off chain is completed exactly, and Stop leaves it as it was."""
    from unittest.mock import patch
    from shared.config import Config
    from shared.refinement import RefinementStep, complete_chain

    rows0 = eng.execute_search(PARENT, "literal", 0, corpus_scope="genizah")
    cut0 = se.consume_last_search_cutoff()
    s0 = RefinementStep(PARENT, "literal", corpus_scope="genizah", result_count=len(rows0),
                        result_count_capped=bool(cut0["capped"]))
    s0._result_sys_ids, s0._result_uids = _sys_ids(rows0), _uids(rows0)
    rows1 = eng.execute_search(CHILD, "literal", 0, restrict_sys_ids=s0._result_sys_ids,
                               corpus_scope="genizah")
    s1 = RefinementStep(CHILD, "literal", corpus_scope="genizah", result_count=len(rows1),
                        result_count_capped=True)
    s1._result_sys_ids, s1._result_uids = _sys_ids(rows1), _uids(rows1)
    report.check("the parent reaches the display limit (else this run is vacuous)",
                 s0.result_count_capped and s0._result_sys_ids != parent_ids,
                 f"{len(s0._result_sys_ids):,} of {len(parent_ids):,} manuscripts shown")
    with patch.object(Config, "SEARCH_LIMIT", ORACLE_LIMIT):
        brute = {r["uid"] for r in eng.execute_search(CHILD, "literal", 0, corpus_scope="genizah")
                 if r["display"]["id"] in parent_ids}

    _check_stop([s0, s1], eng, report)

    started = time.perf_counter()
    result = complete_chain([s0, s1], eng, None)
    took = time.perf_counter() - started
    report.check("completion: the parent holds every manuscript", s0._result_sys_ids == parent_ids,
                 f"{len(s0._result_sys_ids):,} (oracle {len(parent_ids):,})")
    missing, extra = brute - s1._result_uids, s1._result_uids - brute
    report.check("completion: the child holds every page within them", not missing and not extra,
                 f"{len(s1._result_uids):,} (oracle {len(brute):,}; missing {len(missing)}, "
                 f"extra {len(extra)}); {took:.0f}s")
    report.check("completion: hands on the child's manuscripts, nothing left cut off",
                 result == {"restrict": s1._result_sys_ids, "interrupted": False}
                 and not s0.result_count_capped and not s1.result_count_capped)
    return took


def _check_stop(chain, eng, report):
    from PyQt6.QtCore import QCoreApplication
    from desktop.gui_threads import ChainCompletionThread
    qapp = QCoreApplication.instance() or QCoreApplication([])
    before = [(set(s._result_sys_ids), s.result_count, s.result_count_capped) for s in chain]
    thread = ChainCompletionThread(chain, eng, None)
    got = []
    thread.finished_signal.connect(got.append)   # queued here: delivered by processEvents
    runner = threading.Thread(target=thread.run)
    runner.start()
    time.sleep(1)
    started = time.perf_counter()
    thread.request_cancel()
    runner.join(60)
    stop_s = time.perf_counter() - started
    qapp.processEvents()
    after = [(set(s._result_sys_ids), s.result_count, s.result_count_capped) for s in chain]
    report.check("Stop: returns promptly, interrupted, chain unchanged",
                 not runner.is_alive() and stop_s < 5
                 and got == [{"restrict": None, "interrupted": True}] and after == before,
                 f"{stop_s:.2f}s, {got}")


def main(argv=None) -> int:
    from shared.config import Config
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--index-dir", default=Config.INDEX_DIR)
    ap.add_argument("--libraries-csv", default=Config.LIBRARIES_CSV)
    ap.add_argument("--json-out", default=None,
                    help="append one JSON line per run (commit, counts, verdict)")
    args = ap.parse_args(argv)

    started = time.time()
    record = {"started": time.strftime("%Y-%m-%dT%H:%M:%S"), "commit": _git("rev-parse", "HEAD"),
              "dirty": bool(_git("status", "--porcelain")), "host": platform.node(),
              "index_dir": args.index_dir}
    missing = missing_data(args.index_dir, args.libraries_csv)
    if missing:
        for m in missing:
            print(f"MISSING  {m}", flush=True)
        return _finish(record, args.json_out, EXIT_MISSING_DATA, started, missing=missing)

    from unittest.mock import patch
    from shared.metadata_manager import MetadataManager
    from shared.variants import VariantManager
    import shared.search_engine as se

    report = _Report()
    with patch.object(Config, "INDEX_DIR", args.index_dir), \
            patch.object(Config, "LIBRARIES_CSV", args.libraries_csv):
        meta = MetadataManager()
        meta._load_heavy_caches_bg()
        eng = se.SearchEngine(meta, VariantManager(), worker_mode=True, open_local=False)
        with patch.object(Config, "SEARCH_LIMIT", ORACLE_LIMIT):
            parent_ids = _sys_ids(eng.execute_search(PARENT, "literal", 0, corpus_scope="genizah"))
        print(f"{PARENT}: {len(parent_ids):,} manuscripts (uncapped)", flush=True)
        for mode in ("literal", "variants"):
            check_restriction(eng, meta, se, mode, parent_ids, report)
        record["completion_s"] = round(check_completion(eng, se, parent_ids, report), 1)
    record["checks"] = report.checks
    code = EXIT_FAILED if report.failed else EXIT_OK
    print("VERIFIED" if code == EXIT_OK else f"FAILED ({len(report.failed)} check(s))", flush=True)
    return _finish(record, args.json_out, code, started)


def _finish(record, json_out, code, started, **extra):
    record.update(extra, exit_code=code, seconds=round(time.time() - started, 1),
                  verdict={EXIT_OK: "verified", EXIT_FAILED: "failed",
                           EXIT_MISSING_DATA: "missing-data"}[code])
    if json_out:
        os.makedirs(os.path.dirname(os.path.abspath(json_out)), exist_ok=True)
        with open(json_out, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    return code


if __name__ == "__main__":
    sys.exit(main())
