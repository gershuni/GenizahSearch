"""Record today's shelfmark resolver answers as a baseline fixture (catalogue-shelfmark plan, gate 2).

Run on UNCHANGED resolver code (it was first run on master-main ed05058f, before
shared/catalog_shelfmark.py existed). ``tests/test_shelfmark_resolve_baseline.py``
then holds the changed resolver to it:

* every answer that was exact (a single record or exact options) must come back
  identical, order included;
* every other answer must be identical, or now resolve to its source record, or
  be a list that still contains its source record.

Inputs (fixed seeds): 1,000 stored call-number variants, 200 simulated NLI
catalogue forms (150 ``<institution>, Ms. <rest>`` and 50 zero-padded Cambridge
ones), and a few explicit cases, including ``limit`` 0 and 1 on exact options.

Read-only: libraries.csv is read, the CUDL collision report goes to a temporary
directory, and the per-user NLI cache is not used.

    python scripts/build_shelfmark_resolve_baseline.py            # writes the fixture
    python scripts/build_shelfmark_resolve_baseline.py --check    # prints a summary only
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import random
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
FIXTURE = REPO / "tests" / "fixtures" / "shelfmark_resolve_baseline.json"
SEED = 20261018
N_STORED = 1000
N_COMMA = 150
N_CUL_PADDED = 50
HEAD_RE = re.compile(r'^(?P<head>.+?)[,\s]+(?:Ms\.?|MS\.?)\s+(?P<rest>\S.*)$')
EXPLICIT = [
    ("T-S 12.123", 100), ("GASTER 1201.17", 100), ("Halper 381", 100), ("T-S AS 31.1", 100),
    ("T-S AS 3.11", 100), ("Or. 10578R.98", 100), ("T-S F 8.2", 100), ("T-S F 8/002", 100),
    ("F 01", 100), ("T-S K 8.026", 100), ("Cambridge University Library, Ms. T-S K 8.26", 100),
    ("Cambridge University Library, Ms. T-S K 08.026", 100), ("Mosseri, Jacques, Ms. VIII.402", 100),
    ("6", 100), ("6", 1), ("6", 0), ("Ms. 37", 1), ("Ms. 37", 0), ("T-S 12.123", 0),
]


def _load_corpus_helper():
    spec = importlib.util.spec_from_file_location("shelfmark_corpus", REPO / "tests" / "shelfmark_corpus.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _commit():
    try:
        return subprocess.check_output(["git", "-C", str(REPO), "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"


def build_inputs(mm):
    rng = random.Random(SEED)
    index = mm._get_shelfmark_index()
    stored = rng.sample(sorted({(e[0], e[1]) for e in index}), N_STORED)
    inputs = [{"kind": "stored", "query": shelf, "source": sid, "limit": 100} for sid, shelf in stored]

    heads, cul = [], []
    for sid in sorted(mm.csv_bank):
        d = mm.csv_bank[sid]
        for v in d.get("call_numbers_raw") or []:
            m = HEAD_RE.match(v)
            if not m or (" Ms" not in v and " MS" not in v):
                continue
            head, rest = m.group("head").rstrip(","), m.group("rest")
            heads.append((sid, f"{head}, Ms. {rest}"))
            if d.get("library_code") == "CUL":
                nums = list(re.finditer(r"\d+", rest))
                if nums:
                    out, last = [], 0
                    for i, n in enumerate(nums[:2]):
                        out += [rest[last:n.start()], n.group().zfill(2 + i)]
                        last = n.end()
                    padded = "".join(out) + rest[last:]
                    if padded != rest:
                        cul.append((sid, f"{head}, Ms. {padded}"))
            break
    for kind, pool, n in (("nli_comma", heads, N_COMMA), ("nli_cul_padded", cul, N_CUL_PADDED)):
        for sid, q in rng.sample(pool, n):
            inputs.append({"kind": kind, "query": q, "source": sid, "limit": 100})
    for q, limit in EXPLICIT:
        inputs.append({"kind": "explicit", "query": q, "source": None, "limit": limit})
    return inputs


def t1_exact(mm, query, norm_set, nodot_set):
    """Whether today's resolver answers `query` from an exact match (incl. dotless and the bridge)."""
    from shared.browse_map_utils import _strip_library_prefix, normalize_shelfmark
    from shared.shelfmark_bridge import lookup_cudl
    sq = _strip_library_prefix(query.strip())
    nq = normalize_shelfmark(sq)
    if not nq:
        return False
    if nq in norm_set or ('.' not in nq and nq in nodot_set):
        return True
    hit = lookup_cudl(sq)
    return bool(hit and hit.get("sys_id"))


def response(mm, query, limit):
    r = mm.resolve_system_by_shelfmark(query, limit=limit)
    return {"sys_id": r.get("sys_id"), "options": [o["sys_id"] for o in r.get("options") or []],
            "selected_shelfmark": r.get("selected_shelfmark")}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true", help="print a summary, do not write")
    args = ap.parse_args()
    helper = _load_corpus_helper()
    with tempfile.TemporaryDirectory() as tmp:
        mm = helper.load_real_manager(tmp)
    if len(mm.csv_bank) < helper.MIN_ROWS:
        sys.exit(f"libraries.csv has only {len(mm.csv_bank)} rows; refusing to write a baseline")
    index = mm._get_shelfmark_index()
    norm_set = {e[3] for e in index}
    nodot_set = {e[4] for e in index}
    t0 = time.perf_counter()
    entries = []
    for item in build_inputs(mm):
        item["exact"] = t1_exact(mm, item["query"], norm_set, nodot_set)
        item["response"] = response(mm, item["query"], item["limit"])
        entries.append(item)
    data = {
        "header": {"source_commit": _commit(), "libraries_csv_sha256": _sha256(REPO / "libraries.csv"),
                   "seed": SEED, "default_limit": 100, "n": len(entries)},
        "entries": entries,
    }
    n_exact = sum(e["exact"] for e in entries)
    print(f"{len(entries)} inputs, {n_exact} exact today, {time.perf_counter() - t0:.0f}s")
    if not args.check:
        FIXTURE.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
        print(f"wrote {FIXTURE.relative_to(REPO)} ({FIXTURE.stat().st_size // 1024} KiB)")


if __name__ == "__main__":
    main()
