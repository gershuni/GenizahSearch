"""Emit M-source's RECORDED WITNESSES per work as a novelty-gate input.

SUPERSEDES `emit_work_attributions.py`, which read one field and got the
question wrong. The corpus records witnesses on THREE channels, and the join
across them was already built:

  * channel 1 -- the edition's own base manuscript (per-work, local to the
    edition file);
  * channel 2 -- the catalogue's used-witness list;
  * channel 3 -- the catalogue's ADDITIONAL-witness list.

Channels 2 and 3 together carry MORE resolved witness rows than channel 1
(9,153 + 8,969 vs 11,768), so reading channel 1 alone understates "already
known" by roughly 3x. Measured on the real artifact: 1,375 flips from channel-1
free text versus 4,518 from the three-channel resolved set.

WHY THIS INPUT IS BETTER, not merely bigger. The joined table is already
resolved to `sys_id` with a confidence tier (`high` / `low` / `ambiguous`) by
the pipeline that owns shelfmark matching. Re-deriving that from attribution
free text -- which is what the first version of this wiring did -- reinvents
shelfmark normalization badly, in a place that has no business owning it.

WHAT IT IS AND IS NOT. A recorded witness means the corpus already attests THIS
manuscript for THIS work, so a claim on that pair is a restatement, not a
discovery. The converse does not hold: absence from these channels is NOT
evidence of novelty, because the catalogue's coverage is uneven. This input can
only ever establish "already known".

MASKING (D-25). The corpus is M-source throughout. The source table's PATH
contains the restricted name, so it is a REQUIRED argument with no default and
is never echoed -- only counts are printed. Emitted values (shelfmark strings,
library codes, opaque sys_ids) are checked against the live restricted-pattern
set on every emit, and a hit FAILS the emit.

Output (JSON):
    {"schema": "work-witnesses-v1",
     "works": N, "pairs": M,
     "witnesses": {"<raw_work_id>": {"<sys_id>": {"confidence": "...",
                                                  "channels": [1,3]}}},
     "attestation": {"<raw_work_id>": "<short human-readable summary>"}}

Keyed on the RAW work id; the consumer maps through the crosswalk.

Run:
    python scripts/emit_work_witnesses.py \
        --witness-table <path to the joined witness table> \
        --out discovery_data/work_witnesses-v1.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from typing import Dict, List, Optional, Sequence

SCHEMA = "work-witnesses-v1"

# The channel keys in the joined table CANNOT be written here -- two of the three
# carry the restricted term, and an earlier draft hardcoded them and was
# correctly rejected by the masking scan (twice, in two files: the same mistake).
#
# Detected structurally instead: a channel is any per-work key whose value is a
# mapping carrying a `matches` list. That is a property of the table's own shape,
# so it needs no name, and it picks up a fourth channel automatically if one is
# ever added -- where a hardcoded triple would silently ignore it, which is
# exactly how this whole defect started.
_CHANNEL_MARKER = "matches"


def _channel_keys(row: Dict) -> List[str]:
    return sorted(
        k for k, v in row.items()
        if isinstance(v, dict) and isinstance(v.get(_CHANNEL_MARKER), list)
    )

# Precision-first ordering, as defined by the table's own producer: `high` means
# the classmark matched a call-number variant EXACTLY and the library agreed.
_CONF_RANK = {"high": 3, "low": 2, "ambiguous": 1}


def _restricted_patterns(path: Optional[str]) -> List[str]:
    path = path or os.environ.get("MASKING_SCAN_PATTERNS_FILE")
    if not path or not os.path.isfile(path):
        raise SystemExit(
            "refusing to emit without the restricted-pattern set -- set "
            "MASKING_SCAN_PATTERNS_FILE or pass --masking-patterns. An emit that "
            "cannot check is not a clean emit."
        )
    with open(path, encoding="utf-8") as fh:
        pats = [ln.strip() for ln in fh if ln.strip()]
    if not pats:
        raise SystemExit(f"restricted-pattern file {path} is empty -- failing closed")
    return pats


def extract(witness_table_path: str) -> Dict[str, Dict]:
    with open(witness_table_path, encoding="utf-8") as fh:
        table = json.load(fh)
    witnesses: Dict[str, Dict[str, Dict]] = {}
    attestation: Dict[str, str] = {}
    for row in table:
        raw = row.get("work_id")
        if not raw:
            continue
        per_sys: Dict[str, Dict] = {}
        for idx, key in enumerate(_channel_keys(row), start=1):
            for m in (row.get(key, {}).get("matches") or []):
                sid = m.get("sys_id")
                if not sid:
                    continue
                sid = str(sid)
                conf = m.get("confidence") or "low"
                entry = per_sys.setdefault(sid, {"confidence": conf, "channels": []})
                if idx not in entry["channels"]:
                    entry["channels"].append(idx)
                if _CONF_RANK.get(conf, 0) > _CONF_RANK.get(entry["confidence"], 0):
                    entry["confidence"] = conf
        if not per_sys:
            continue
        witnesses[raw] = per_sys
        n_high = sum(1 for e in per_sys.values() if e["confidence"] == "high")
        # Human-readable and DELIBERATELY count-only: this string reaches the
        # model's evidence bundle, and the per-manuscript decision is made from
        # the structured map, not from prose. Naming shelfmarks here would put
        # reference-side detail in the prompt for no decision benefit.
        attestation[raw] = (
            f"M-source records {len(per_sys)} manuscript witness(es) for this work "
            f"({n_high} matched with high confidence)."
        )
    return {"witnesses": witnesses, "attestation": attestation}


def _check_clean(payload: Dict, patterns: Sequence[str]) -> None:
    """The emitted payload must carry no restricted string. Checked on the
    SERIALIZED form, so a value nested anywhere is covered rather than only the
    fields this function happens to know about."""
    blob = json.dumps(payload, ensure_ascii=False).casefold()
    hits = sum(1 for p in patterns if p.casefold() in blob)
    if hits:
        raise SystemExit(
            f"EMIT REFUSED: the payload matches {hits} restricted pattern(s). "
            f"(counts only, D-25 -- no value is echoed.)"
        )


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--witness-table", required=True,
                    help="the joined three-channel witness table (path withheld here; "
                         "it lives in the gitignored spike results directory)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--masking-patterns", default=None)
    args = ap.parse_args(argv)

    patterns = _restricted_patterns(args.masking_patterns)
    data = extract(args.witness_table)
    pairs = sum(len(v) for v in data["witnesses"].values())
    payload = {
        "schema": SCHEMA,
        "works": len(data["witnesses"]),
        "pairs": pairs,
        "witnesses": data["witnesses"],
        "attestation": data["attestation"],
    }
    _check_clean(payload, patterns)

    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=1)
    encoded = blob.encode("utf-8")
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    # Binary write: the default text mode translates newlines on Windows, so the
    # digest below would identify a string that is not the file.
    with open(args.out, "wb") as fh:
        fh.write(encoded)
    digest = hashlib.sha256(encoded).hexdigest()

    conf_counts: Dict[str, int] = {}
    for per_sys in data["witnesses"].values():
        for e in per_sys.values():
            conf_counts[e["confidence"]] = conf_counts.get(e["confidence"], 0) + 1
    print(f"wrote {args.out}")
    print(f"  works with >=1 recorded witness : {len(data['witnesses']):,}")
    print(f"  (work, manuscript) pairs        : {pairs:,}")
    print(f"  by confidence                   : {conf_counts}")
    print(f"  restricted-pattern check        : clean ({len(patterns)} patterns)")
    print(f"  sha256                          : {digest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
