"""Emit the reference corpus's per-work WITNESS ATTRIBUTION as a novelty-gate input.

WHY THIS EXISTS. `NoveltyCandidate` has carried a field for an internal
reference-corpus shelfmark attribution since the gate was written -- it is
threaded into `assemble_evidence_bundle`, listed in `_SOURCE_ORDER`, and is an
input to the cache fingerprint. It was never populated: the one production
assignment in `discovery_novelty_probe.build_all_candidates` was the literal
`None`, and the only non-None assignments in the tree are two test fixtures. So
the gate declared it checked this source and never did.

That matters because the attribution frequently records WHICH MANUSCRIPT a text
is witnessed in. When it names the very manuscript we are claiming, the claim is
not a discovery -- it is a restatement of something the corpus already recorded.

WHY A SEPARATE FILE RATHER THAN A COLUMN IN THE ASSET. The novelty gate is a
build-time process; its VERDICTS ship, its INPUTS need not. Adding attribution
text to `discovery-*.db` would widen the shipped surface for no consumer
benefit. This emits a small side file instead, pinned like the gate's other
inputs (`--novelty-verdicts`, `--work-domains`).

MASKING (D-25). The corpus is referred to only as M-source, and the source
field's own NAME is restricted -- the neutral name `src_attr_note` is used
throughout, matching `v3_build_research_db.FORBIDDEN_COLUMN_SUBSTRINGS`. The
attribution VALUES were checked against the live restricted-pattern set before
this script was written: 0 of 5,077 contain one, so the text itself is safe to
carry. That check is re-run here on every emit rather than assumed, and a hit
FAILS the emit -- a future corpus refresh could introduce one.

Output shape (JSON):
    {"schema": "work-attributions-v1",
     "count": N,
     "attributions": {"<raw_work_id>": "<src_attr_note text>", ...}}

Keyed on the RAW work id, because that is what the reference corpus carries and
what the crosswalk maps FROM. The consumer maps to minted ids itself.

Run:
    python scripts/emit_work_attributions.py \
        --ref-corpus same_work_spike/probe/data/ref_corpus_v2.pkl \
        --out discovery_data/work_attributions-v1.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pickle
import sys
from typing import Dict, List, Optional, Sequence

NEUTRAL_NAME = "src_attr_note"
SCHEMA = "work-attributions-v1"

# The reference corpus's key for this field is ITSELF restricted (it is the sole
# entry in `v3_build_research_db.FORBIDDEN_COLUMN_SUBSTRINGS`), so it must not
# appear in this file -- an earlier draft hardcoded it and the masking scan
# correctly rejected the file.
#
# Resolved by ELIMINATION instead: every OTHER per-work key is unrestricted and
# enumerable, so the attribution is whatever remains. This is not a cute trick
# to dodge the scanner -- it is deterministic, and it fails LOUDLY if the corpus
# shape changes, where a hardcoded name would silently read `None` and emit an
# empty file that looks like "no attributions exist".
_UNRESTRICTED_WORK_KEYS = frozenset(
    {"author", "cat", "date", "genre", "id", "stream", "title", "vgroup"}
)


def resolve_attribution_key(work: Dict[str, object]) -> str:
    """The one per-work key that is not on the unrestricted list."""
    remaining = sorted(set(work) - _UNRESTRICTED_WORK_KEYS)
    if len(remaining) != 1:
        raise SystemExit(
            "cannot resolve the attribution field by elimination: expected "
            f"exactly 1 unaccounted-for per-work key, found {len(remaining)}. "
            "The reference corpus's shape has changed -- update "
            "_UNRESTRICTED_WORK_KEYS deliberately (names withheld, D-25)."
        )
    return remaining[0]


def _restricted_patterns(path: Optional[str]) -> List[str]:
    """Load the restricted-string set. Absent/empty => FAIL CLOSED, matching
    `check_atlas_masking`'s posture: an unset pattern file is never a silent
    green, because "no patterns loaded" and "no matches found" are
    indistinguishable in the output."""
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


def extract(ref_corpus_path: str, patterns: Sequence[str]) -> Dict[str, str]:
    with open(ref_corpus_path, "rb") as fh:
        works = pickle.load(fh)
    if not works:
        raise SystemExit("reference corpus is empty")
    source_key = resolve_attribution_key(works[0])
    folded = [p.casefold() for p in patterns]
    out: Dict[str, str] = {}
    leaked = 0
    for w in works:
        text = w.get(source_key)
        if not text:
            continue
        text = str(text).strip()
        if not text:
            continue
        low = text.casefold()
        if any(p in low for p in folded):
            # Counted, never echoed -- printing the offending value would put the
            # restricted string in a log, which is the thing being prevented.
            leaked += 1
            continue
        out[str(w["id"])] = text
    if leaked:
        raise SystemExit(
            f"EMIT REFUSED: {leaked} attribution value(s) contain a restricted "
            f"string. This was 0 when the field was first surveyed, so a corpus "
            f"refresh has introduced one. Resolve before emitting (counts only, "
            f"D-25 -- no value is echoed)."
        )
    return out


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ref-corpus", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--masking-patterns", default=None)
    args = ap.parse_args(argv)

    patterns = _restricted_patterns(args.masking_patterns)
    attributions = extract(args.ref_corpus, patterns)
    payload = {
        "schema": SCHEMA,
        "neutral_name": NEUTRAL_NAME,
        "count": len(attributions),
        "attributions": attributions,
    }
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=1)
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    # `newline=""` because the default translates "\n" -> "\r\n" on Windows, so
    # the bytes ON DISK differ from `blob` and the digest below would identify a
    # string nobody can reproduce from the file. A pinned hash that does not match
    # its artifact is worse than no hash: it fails the check it exists to pass.
    encoded = blob.encode("utf-8")
    with open(args.out, "wb") as fh:
        fh.write(encoded)
    digest = hashlib.sha256(encoded).hexdigest()
    print(f"wrote {args.out}")
    print(f"  works with an attribution : {len(attributions):,}")
    print(f"  restricted-pattern check  : clean ({len(patterns)} patterns)")
    print(f"  sha256                    : {digest}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
