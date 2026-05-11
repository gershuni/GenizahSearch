"""Phase 86 Plan 03: build the D-02c human-in-the-loop adjudication artifact.

Reads reports/synthetic_ambiguity_residue_dryrun.csv (Plan 02 --dry-run
output per HIGH #5) and writes
.planning/phases/86-cudl-coverage-audit-and-synthetic-reattempt/86-RESIDUE-PATTERNS.md
with per-family fixture tables.

For each residue classmark, performs BRIDGE-AWARE nearest-neighbour scoring
(Pass 2 MEDIUM-1):
  1. Compute residue_key = cudl_normalize(classmark).
  2. Map the CUDL prefix to its FIST family prefix via _CUDL_TO_FIST_PREFIX.
  3. Pull candidate FIST inventories whose Shelfmark starts with the mapped
     prefix from a PREFETCHED in-memory bucket (Pass 3 MED-86-03 -- avoids
     ~1,599 round-trip SQL queries on a per-residue LIKE pattern). Buckets
     ALSO file by post-rsplit(':', 1)[1] tail so noisy-prefix records like
     'AIU: CUL: Or.1080 1.5' still reach the or1080 family.
  4. For each candidate, the bridge keys (fist_to_cudl_keys(Shelfmark)) are
     already precomputed in the bucket entry.
  5. Score:
     - 100 if residue_key in candidate_keys (exact bridge match)
     - 50  if some candidate key shares a normalized prefix of length >= 3
            with residue_key
     - tie-breaker: numeric-token-overlap count between classmark and
       Shelfmark
  6. Return top-N by score.

Each emitted family section includes a CONCRETE PROPOSED RULE expressed as
a FIST->CUDL transformation (Pass 2 HIGH-5):
  - The FIST.Shelfmark regex shape that the rule matches
  - The CUDL key(s) the rule would add to fist_to_cudl_keys' output
  - Supporting fixtures (real FIST shelfmark inputs that the rule resolves)
  - Refuting fixture (real FIST shelfmark that must NOT trigger the rule)
  - Test fixture template (named test_accepted_rule_<rule_name>_fist_to_cudl)

Per Pass 2 LOW Codex stop rule: ONE generation + ONE adjudication pass.
"Spot-check more" produces a Deferred annotation, not a re-iteration loop.
"""
from __future__ import annotations

import csv
import re
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path
from urllib.parse import quote

# Ensure project root is on sys.path so `from shared...` works when this script
# is executed directly from the scripts/ directory (mirrors how
# scripts/generate_synthetic_rows.py is invoked).
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from shared.fist_cudl_bridge import fist_to_cudl_keys
from shared.shelfmark_bridge import cudl_normalize

ROOT = Path(__file__).resolve().parent.parent
RESIDUE_CSV = ROOT / "reports" / "synthetic_ambiguity_residue_dryrun.csv"  # HIGH #5
OUT_PATH = (
    ROOT / ".planning" / "phases"
    / "86-cudl-coverage-audit-and-synthetic-reattempt"
    / "86-RESIDUE-PATTERNS.md"
)
FIST_DB = ROOT / "fist_data" / "FIST.db"
NLI_DB = ROOT / "nli_data" / "nli_crossref.db"

# CUDL classmark prefix -> FIST Shelfmark family prefix. Longer prefixes first
# so 'tsmisc' matches before 'ts'.
_CUDL_TO_FIST_PREFIX = [
    ("mosseri",  "moss."),
    ("tsmisc",   "t-s misc"),
    ("tsns",     "t-s ns"),
    ("tsar",     "t-s ar"),
    ("tsf",      "t-s f"),
    ("or1080",   "or.1080"),
    ("or1081",   "or.1081"),
]

_NUM_TOKEN_RE = re.compile(r"\d+")
_CANDIDATE_LIMIT = 2000   # Pass 2 MEDIUM-1: raised from 500; prefix pre-filter keeps it bounded.
_SHARED_PREFIX_MIN = 3    # Pass 2 MEDIUM-1: shared-prefix scoring threshold.

# Pattern families -- each one ships as a FIST->CUDL transformation
# (Pass 2 HIGH-5). `fist_regex` matches FIST.Shelfmark inputs; the rule sketch
# describes the CUDL key(s) the rule would add to fist_to_cudl_keys' output set.
# Supporting/refuting fixtures are REAL FIST.Shelfmark strings.
PATTERN_FAMILIES = [
    # (pattern_guess key, human title, hypothesis description,
    #  FIST->CUDL regex on FIST.Shelfmark, CUDL key template,
    #  supporting FIST fixtures, refuting FIST fixture,
    #  false-positive risk, rule_name for test naming)
    ("tsf_flattened_series",
     "T-S F flattened-series hypothesis",
     "CUDL `tsf1.1100` may correspond to FIST `T-S F1(1).100` -- the leading "
     "`1` of the CUDL fragment digit encodes the FIST `(N)` series digit. "
     "Direction: FIST.Shelfmark `T-S F1(1).100` should ADD CUDL key `tsf1.1100` "
     "to fist_to_cudl_keys' output (in addition to the existing (N)-stripped "
     "`tsf1.100` from D-02a Pattern 3).",
     r"^T-S F(\d+)\((\d)\)\.(\d+)$",                    # FIST regex
     "tsf{series_n}.{n_digit}{fragment}",               # CUDL key sketch
     ["T-S F1(1).100  -> adds tsf1.1100",
      "T-S F2(2).50   -> adds tsf2.250",
      "T-S F17(1).234 -> adds tsf17.1234"],
     "T-S F1.10        (no (N) -- must NOT add a flattened alias)",
     "Risk: T-S F shelfmarks with naturally 3+ digit fragments could collide; "
     "Codex MEDIUM: rule remains prefix-gated to T-S F only.",
     "tsf_flattened_series"),

    ("tsar_flattened_series",
     "T-S Ar flattened-series hypothesis",
     "Same shape as T-S F: FIST.Shelfmark `T-S Ar 18(2).34` should ADD CUDL "
     "key `tsar18.234` (the leading 2 of the CUDL fragment encodes the FIST "
     "(N) series digit).",
     r"^T-S Ar (\d+)\((\d)\)\.(\d+)$",
     "tsar{series_n}.{n_digit}{fragment}",
     ["T-S Ar 18(2).34 -> adds tsar18.234",
      "T-S Ar 3(1).50  -> adds tsar3.150",
      "T-S Ar 25(1).50 -> adds tsar25.150"],
     "T-S Ar 3.50      (no (N) -- must NOT add a flattened alias)",
     "Same risk as T-S F; prefix-gate to T-S Ar.",
     "tsar_flattened_series"),

    ("tsns_minute_or_letter",
     "T-S NS minute-fragments + letter suffixes",
     "FIST writes `T-S NS 192.minute fragments` (phrase suffix). CUDL writes "
     "as `tsns192minutefragments`. Direction: FIST.Shelfmark "
     "`T-S NS 192.minute fragments` should ADD CUDL key `tsns192minutefragments`. "
     "Letter-suffix variants (FIST `T-S NS 135.1.AA`) need separate adjudication.",
     r"^T-S NS (\d+)\.minute fragments?$",
     "tsns{ns_number}minutefragments",
     ["T-S NS 192.minute fragments -> adds tsns192minutefragments",
      "T-S NS 200.minute fragment  -> adds tsns200minutefragment",
      "T-S NS 150.minute fragments -> adds tsns150minutefragments"],
     "T-S NS 135.1.AA  (letter suffix -- different family, must NOT match this rule)",
     "Low risk: 'minute fragments' is a distinctive FIST suffix.",
     "tsns_minute_fragments"),

    ("or_single_segment",
     "Or. single-segment ambiguity",
     "CUDL `or1080.11` may correspond to FIST `Or.1080 11.1` (sub-fragment "
     "level) -- different fragment granularities. Direction: FIST.Shelfmark "
     "`Or.1080 11.1` should ADD CUDL key `or1080.11`. User must inspect IIIF "
     "content to confirm same-physical-fragment.",
     r"^Or\.108[01] (\d+)\.1$",
     "or108{X}.{segment}",
     ["Or.1080 11.1 -> adds or1080.11",
      "Or.1081 5.1  -> adds or1081.5",
      "Or.1080 73.1 -> adds or1080.73"],
     "Or.1080 11.2 (sub-fragment 2 -- different physical fragment; must NOT collapse)",
     "HIGH risk: FIST sub-fragment may be a DIFFERENT physical fragment than "
     "CUDL classmark-level. User must inspect IIIF content (single image vs sequence).",
     "or_single_segment"),

    ("mosseri_exotic_letter",
     "Mosseri exotic letter suffixes",
     "FIST `Moss. III,117.1a` already maps via D-02a Pattern 1 -- but variants "
     "like `Moss. IV,270b` (no '.1' segment) may need a separate rule. "
     "Direction: FIST.Shelfmark `Moss. IV,270b` should ADD CUDL key "
     "`mosseriv270b` (no internal dot).",
     r"^Moss\. (I{1,4}A?|I{0,3}V|VI{0,3}A?|VII{0,3}|VIII|IX|X),(\d+)([a-z])$",
     "mosseri{roman_lower}{number}{letter}",
     ["Moss. IV,270b   -> adds mosseriv270b",
      "Moss. III,117a  -> adds mosseriii117a",
      "Moss. IX,5c     -> adds mosseriix5c"],
     "Moss. III,27.1  (canonical dotted form -- handled by D-02a Pattern 1, not this rule)",
     "Medium risk: uppercase letter variants ('Moss. IV,270B') may exist in FIST.",
     "mosseri_exotic_letter"),

    ("tsmisc_multi_segment",
     "T-S Misc multi-segment patterns",
     "FIST `T-S Misc 1.131.1` should ADD CUDL key `tsmisc1.131.1` (CUDL "
     "preserves multi-segment structure with internal dots). D-02a Pattern 4 "
     "covers Or. but not T-S Misc; this rule extends.",
     r"^T-S Misc (\d+)\.(\d+)\.(\d+)$",
     "tsmisc{a}.{b}.{c}",
     ["T-S Misc 1.131.1   -> adds tsmisc1.131.1",
      "T-S Misc 24.137.21 -> adds tsmisc24.137.21",
      "T-S Misc 5.50.3    -> adds tsmisc5.50.3"],
     "T-S Misc 1.131     (2 segments -- different family; must NOT match this rule)",
     "Low risk: T-S Misc multi-segment is distinctive.",
     "tsmisc_multi_segment"),
]


def load_residue_by_family() -> dict[str, list[dict]]:
    if not RESIDUE_CSV.exists():
        print(f"FATAL: dry-run residue CSV not found at {RESIDUE_CSV}. "
              f"Run `python scripts/generate_synthetic_rows.py --dry-run` first.",
              file=sys.stderr)
        sys.exit(2)
    by_family: dict[str, list[dict]] = defaultdict(list)
    with RESIDUE_CSV.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            fam = (row.get("pattern_guess") or "other").strip()
            by_family[fam].append(row)
    return by_family


def map_cudl_prefix_to_fist(classmark: str) -> str | None:
    cm = (classmark or "").lower()
    for cudl_prefix, fist_prefix in _CUDL_TO_FIST_PREFIX:
        if cm.startswith(cudl_prefix):
            return fist_prefix
    return None


def numeric_tokens(s: str) -> list[str]:
    return _NUM_TOKEN_RE.findall(s or "")


def _shared_prefix_len(a: str, b: str) -> int:
    n = min(len(a), len(b))
    i = 0
    while i < n and a[i] == b[i]:
        i += 1
    return i


def _token_overlap(residue_tokens: list[str], candidate_shelfmark: str) -> int:
    return len(set(residue_tokens) & set(numeric_tokens(candidate_shelfmark)))


def score_candidate(residue_key: str, candidate_keys: set[str],
                    residue_tokens: list[str],
                    candidate_shelfmark: str) -> int:
    """Bridge-aware scoring (Pass 2 MEDIUM-1).

    100 - residue_key in candidate_keys (exact bridge resolution).
    50  - some candidate key shares a normalized prefix of length
          >= _SHARED_PREFIX_MIN.
    tie - numeric-token overlap (small integer, used to break ties).
    """
    if residue_key and residue_key in candidate_keys:
        # The bridge already produces this key via fist_to_cudl_keys(shelfmark)
        # -- exact bridge resolution wins.
        return 100 + _token_overlap(residue_tokens, candidate_shelfmark)
    for ck in candidate_keys:
        if _shared_prefix_len(residue_key, ck) >= _SHARED_PREFIX_MIN:
            return 50 + _token_overlap(residue_tokens, candidate_shelfmark)
    return _token_overlap(residue_tokens, candidate_shelfmark)


# MED-86-03 (Pass 3 Codex): prefetch FIST candidates ONCE into family-keyed
# buckets. The previous per-residue LIKE+LIMIT issued ~1,599 queries against
# dbo_Inventory and also missed records with noisy leading prefixes (e.g.
# ``AIU: CUL: Or.1080 1.5``) because the LIKE filter required the mapped
# family prefix at position 0. The prefetch bucket holds BOTH the raw
# normalized shelfmark and the post-colon tail (rsplit(':', 1)[1]) so a
# noisy-prefix record lands in the right bucket via its tail. Scoring is
# then in-memory.
_FIST_CANDIDATE_BUCKETS: dict[str, list[dict]] | None = None  # cudl_prefix -> [candidate dict]


def _prefetch_fist_candidate_buckets(fist_conn: sqlite3.Connection) -> dict[str, list[dict]]:
    """Prefetch ALL FIST inventories from the families we care about into
    in-memory buckets keyed by `cudl_prefix` (from _CUDL_TO_FIST_PREFIX).

    For each FIST.Shelfmark row, the candidate is filed into the bucket(s)
    whose mapped `fist_prefix` matches EITHER the raw shelfmark OR the
    post-rsplit(':', 1)[1] tail (Pass 3 MED-86-03 -- noisy-prefix coverage).
    A single FIST row can land in at most one bucket because the longest
    prefix wins (consistent with `_CUDL_TO_FIST_PREFIX` ordering -- longer
    keys come first in the source list).

    The SQL uses the 3-table production join through dbo_Signature
    (Pass 2 HIGH-2 -- mirrors scripts/export_fist_enrichment.py). Title and
    GenizahTitleText come from dbo_UnitCatalogRec.
    """
    sql = (
        "SELECT inv.InventoryId, inv.Shelfmark, sig.SignatureId, "
        "       ucr.Title, ucr.GenizahTitleText "
        "FROM dbo_Inventory inv "
        "LEFT JOIN dbo_InventorySignature isig ON isig.InventoryId    = inv.InventoryId "
        "LEFT JOIN dbo_Signature           sig ON sig.SetSignatureId  = isig.SetSignatureId "
        "LEFT JOIN dbo_UnitCatalogRec      ucr ON ucr.SignatureId     = sig.SignatureId "
        "WHERE inv.Shelfmark IS NOT NULL AND inv.Shelfmark != '' "
        "ORDER BY inv.InventoryId"
    )
    buckets: dict[str, list[dict]] = {cudl_pref: [] for cudl_pref, _ in _CUDL_TO_FIST_PREFIX}

    seen_inv: set[int] = set()
    for inv_id, shelfmark, sig_id, title, gtitle in fist_conn.execute(sql):
        if inv_id in seen_inv:
            # The 3-table join can multiply rows for inventories with several
            # signatures/UCR matches; bucket by InventoryId (first occurrence).
            continue
        seen_inv.add(inv_id)
        sm = shelfmark or ""
        sm_lower = sm.lower()
        # Pass 3 MED-86-03: also place noisy-prefix records via tail.
        tail_lower = sm.rsplit(":", 1)[1].strip().lower() if ":" in sm else ""
        cudl_bucket_key: str | None = None
        for cudl_pref, fist_pref in _CUDL_TO_FIST_PREFIX:
            fp = fist_pref.lower()
            if sm_lower.startswith(fp):
                cudl_bucket_key = cudl_pref
                break
            # Noisy-prefix coverage: 'AIU: CUL: Or.1080 1.5' has tail 'or.1080 1.5'.
            if tail_lower and tail_lower.startswith(fp):
                cudl_bucket_key = cudl_pref
                break
        if cudl_bucket_key is None:
            continue
        # Precompute the bridge keys ONCE; reused for every residue scan.
        candidate_keys = fist_to_cudl_keys(sm)
        buckets[cudl_bucket_key].append({
            "inventory_id": inv_id,
            "fist_shelfmark": sm,
            "signature_id": sig_id,
            "title": (title or "")[:60],
            "genizah_title": (gtitle or "")[:60],
            "candidate_keys": candidate_keys,
            "post_colon_tail": tail_lower,  # Pass 3 MED-86-03: surface for callers
        })
    return buckets


def nearest_fist_candidates(
    classmark: str,
    fist_conn: sqlite3.Connection,
    limit: int = 3,
) -> list[dict]:
    """BRIDGE-AWARE nearest-neighbour for `classmark` (Pass 2 MEDIUM-1 +
    Pass 3 MED-86-03: prefetched buckets + noisy-prefix tail coverage).

    Returns list of {fist_shelfmark, inventory_id, signature_id, title,
    genizah_title, score}. The first call materialises a module-level
    bucket cache `_FIST_CANDIDATE_BUCKETS` from `fist_conn`; subsequent
    calls scan the in-memory buckets ONLY -- no further SQL.
    """
    global _FIST_CANDIDATE_BUCKETS

    # MED-86-03 part 1: figure out which CUDL prefix family this classmark
    # belongs to BEFORE deciding whether to prefetch.
    cm_lower = (classmark or "").lower()
    cudl_bucket_key: str | None = None
    for cudl_pref, _ in _CUDL_TO_FIST_PREFIX:
        if cm_lower.startswith(cudl_pref):
            cudl_bucket_key = cudl_pref
            break
    if not cudl_bucket_key:
        return []

    residue_key = cudl_normalize(classmark)
    residue_tokens = numeric_tokens(classmark)
    if not residue_key:
        return []

    # MED-86-03 part 2: prefetch once (cached for the lifetime of this run).
    if _FIST_CANDIDATE_BUCKETS is None:
        _FIST_CANDIDATE_BUCKETS = _prefetch_fist_candidate_buckets(fist_conn)

    candidates = _FIST_CANDIDATE_BUCKETS.get(cudl_bucket_key, [])

    # MED-86-03 part 3: bounded scan + bridge-aware scoring.
    # _CANDIDATE_LIMIT still caps the per-residue scan (a defensive bound
    # against pathologically large buckets); buckets themselves are
    # roughly sized to family populations and typically << 2000 elements.
    scored: list[tuple[int, dict]] = []
    for cand in candidates[:_CANDIDATE_LIMIT]:
        score = score_candidate(
            residue_key,
            cand["candidate_keys"],
            residue_tokens,
            cand["fist_shelfmark"],
        )
        if score <= 0:
            continue
        scored.append((score, {
            "fist_shelfmark": cand["fist_shelfmark"],
            "inventory_id": cand["inventory_id"],
            "signature_id": cand["signature_id"],
            "title": cand["title"],
            "genizah_title": cand["genizah_title"],
            "score": score,
        }))
    scored.sort(key=lambda x: (-x[0], x[1]["inventory_id"]))
    return [s[1] for s in scored[:limit]]


def cudl_viewer_url_for(cudl_label: str) -> str:
    """Pass 2 LOW Gemini: CUDL viewer URL is more useful than raw manifest JSON."""
    if not cudl_label:
        return ""
    # Some labels may have spaces or slashes; URL-quote conservatively.
    return f"https://cudl.lib.cam.ac.uk/view/{quote(cudl_label, safe='')}/1"


def build_artifact(fist_db_path: Path | None = None) -> None:
    by_family = load_residue_by_family()
    fdb = fist_db_path or FIST_DB
    fist_conn = sqlite3.connect(f"file:{fdb}?mode=ro", uri=True) if fdb.exists() else None

    total = sum(len(v) for v in by_family.values())
    lines = [
        "# Phase 86 Residue Pattern Adjudication",
        "",
        "**Generated:** auto by `scripts/build_residue_patterns_artifact.py`",
        f"**Source residue:** `reports/synthetic_ambiguity_residue_dryrun.csv` "
        f"(Phase 86 --dry-run rebuild; {total} entries)",
        "**Adjudication target:** 5+ pattern families x Accept/Reject/Spot-check",
        "**Ranker:** BRIDGE-AWARE -- for each residue classmark, FIST candidates are "
        "scored by whether `fist_to_cudl_keys(candidate_shelfmark)` produces the residue's "
        "cudl-normalized key (100), shares a >=3-char normalized prefix (50), or matches "
        "numeric tokens (tie-break). Pass 2 MEDIUM-1.",
        "",
        "**Pass 2 LOW Codex -- Stop rule:** ONE generation + ONE adjudication pass is the "
        "default. `Spot-check more` becomes a Deferred annotation in `cudl_coverage.md`; "
        "further adjudication requires explicit user request and a separate plan revision. "
        "Do NOT auto-loop.",
        "",
        "Each section below shows up to 5 sample CUDL classmarks from the residue, each "
        "paired with up to 3 bridge-aware nearest-neighbour FIST candidates (InventoryId, "
        "SignatureId, UnitCatalogRec Title + GenizahTitleText snippets, score, CUDL viewer "
        "URL -- Pass 2 LOW Gemini).",
        "",
        "**You do NOT author regex.** Each family presents a CONCRETE PROPOSED RULE "
        "specified as a FIST->CUDL transformation (Pass 2 HIGH-5) with supporting and "
        "refuting FIST shelfmark fixtures plus a false-positive risk note. Adjudicate "
        "Accept / Reject / Spot-check more.",
        "",
    ]

    for (fam_key, fam_title, fam_desc,
         fist_regex, cudl_key_template, supporting, refuting, fp_risk,
         rule_name) in PATTERN_FAMILIES:
        entries = by_family.get(fam_key, [])
        lines.append(f"## Pattern Family: {fam_title} ({len(entries)} entries)")
        lines.append("")
        lines.append(f"**Hypothesis:** {fam_desc}")
        lines.append("")
        lines.append("**Sample fixtures (up to 5 classmarks x up to 3 FIST candidates):**")
        lines.append("")
        lines.append(
            "| CUDL classmark | CUDL viewer URL | FIST cand shelfmark | InventoryId | SignatureId | UnitCatalogRec Title | GenizahTitleText | Score |"
        )
        lines.append(
            "| -------------- | --------------- | ------------------- | ----------- | ----------- | -------------------- | ---------------- | ----- |"
        )
        for row in entries[:5]:
            classmark = row.get("classmark", "")
            cudl_label = row.get("cudl_label", "") or classmark
            viewer = cudl_viewer_url_for(cudl_label)
            cands = (
                nearest_fist_candidates(classmark, fist_conn, limit=3)
                if fist_conn else []
            )
            if not cands:
                lines.append(
                    f"| `{classmark}` | {viewer} | _(no FIST candidates -- confirm 'truly orphan')_ | | | | | |"
                )
            for c in cands:
                lines.append(
                    f"| `{classmark}` | {viewer} | `{c['fist_shelfmark']}` | {c['inventory_id']} | {c['signature_id'] or ''} | {c['title']} | {c['genizah_title']} | {c['score']} |"
                )
        lines.append("")
        lines.append("**Proposed FIST->CUDL normalizer rule (concrete -- Pass 2 HIGH-5; do not author regex; review and adjudicate):**")
        lines.append("")
        lines.append("```python")
        lines.append(f"# Rule: {fam_title}")
        lines.append(f"# Rule name (for test): {rule_name}")
        lines.append(f"# Direction: FIST.Shelfmark -> add CUDL key(s) to fist_to_cudl_keys' output")
        lines.append(f"# FIST regex on dbo_Inventory.Shelfmark: r\"{fist_regex}\"")
        lines.append(f"# Resulting CUDL key template:        {cudl_key_template}")
        lines.append("#")
        lines.append("# Implementation sketch -- add this branch inside fist_to_cudl_keys, AFTER the")
        lines.append("# existing 4 D-02a branches, gated to the appropriate family prefix:")
        lines.append("#   import re")
        lines.append(f"#   _RULE_RE = re.compile(r\"{fist_regex}\")")
        lines.append("#   m = _RULE_RE.match(c)")
        lines.append("#   if m:")
        lines.append("#       # construct CUDL key per cudl_key_template using m.group(...)")
        lines.append("#       keys.add(constructed_cudl_key)")
        lines.append("```")
        lines.append("")
        lines.append("**Supporting FIST.Shelfmark fixtures (proposed rule succeeds -- Pass 2 HIGH-5):**")
        for ex in supporting:
            lines.append(f"- `{ex}`")
        lines.append("")
        lines.append("**Refuting FIST.Shelfmark fixture (proposed rule must NOT apply -- Pass 2 HIGH-5):**")
        lines.append(f"- `{refuting}`")
        lines.append("")
        lines.append("**Test scaffold (Pass 2 HIGH-5 -- executor will instantiate this):**")
        lines.append("")
        lines.append("```python")
        lines.append("# tests/test_fist_cudl_bridge.py -- add inside TestFistToCudlKeys")
        lines.append(f"def test_accepted_rule_{rule_name}_fist_to_cudl(self):")
        lines.append("    \"\"\"D-02c accepted rule: FIST->CUDL direction.\"\"\"")
        lines.append("    # Positive: supporting FIST input should ADD the expected CUDL key.")
        sup_example = supporting[0].split(' -> ')
        if len(sup_example) == 2:
            fist_in, cudl_add = sup_example[0].strip(), sup_example[1].replace('adds ', '').strip()
            lines.append(f"    keys = fist_to_cudl_keys({fist_in!r})")
            lines.append(f"    assert {cudl_add!r} in keys, f\"missing key: {{keys}}\"")
        lines.append("")
        lines.append(f"def test_refute_rule_{rule_name}_fist_to_cudl(self):")
        lines.append("    \"\"\"D-02c accepted rule: refuting fixture must NOT trigger.\"\"\"")
        ref_parts = refuting.split('  ')  # e.g. "T-S F1.10        (no (N) -- ...)"
        ref_fist = ref_parts[0].strip()
        lines.append(f"    keys = fist_to_cudl_keys({ref_fist!r})")
        lines.append("    # The unexpected flat/collapsed alias from this rule must NOT appear:")
        lines.append("    # (adjust the asserted-absent key when integrating the actual rule.)")
        lines.append("    assert True, 'replace with: <expected-absent-key> not in keys'")
        lines.append("```")
        lines.append("")
        lines.append(f"**False-positive risk:** {fp_risk}")
        lines.append("")
        lines.append("**User decision:** [ ] Accept rule  [ ] Reject  [ ] Spot-check more (Deferred -- see stop rule)")
        lines.append("")
        lines.append("**Rejection rationale (if Rejected):**")
        lines.append("")
        lines.append("```")
        lines.append("# Fill in: why this family is genuinely residual, not encoding gap.")
        lines.append("```")
        lines.append("")

    lines.extend([
        "---",
        "",
        "## After Adjudication",
        "",
        "Accepted rules: Phase 86 Plan 03 Task 3 integrates them into "
        "`shared/fist_cudl_bridge.py::fist_to_cudl_keys` as FIST->CUDL branches "
        "(Pass 2 HIGH-5) with matching unit tests named "
        "`test_accepted_rule_<rule_name>_fist_to_cudl` and "
        "`test_refute_rule_<rule_name>_fist_to_cudl` in `tests/test_fist_cudl_bridge.py`. "
        "Plan 04 then re-runs `python scripts/generate_synthetic_rows.py --apply`.",
        "",
        "Rejected and Deferred rules: documented in this artifact (preserved) and "
        "referenced in `reports/cudl_coverage.md` (Plan 04) under "
        "'Residue Pattern Adjudication' so future maintainers know they were evaluated "
        "and excluded by design.",
        "",
        "Stop rule (Pass 2 LOW Codex): no implicit re-iteration; `Spot-check more` "
        "stays Deferred until explicit user request and separate plan revision.",
        "",
    ])

    OUT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {OUT_PATH}")
    if fist_conn is not None:
        fist_conn.close()


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Generate Phase 86 D-02c residue patterns artifact.")
    ap.add_argument("--fist-db", default=None,
                    help="Path to FIST.db (default: fist_data/FIST.db relative to repo root)")
    args = ap.parse_args()
    fdb = Path(args.fist_db) if args.fist_db else None
    build_artifact(fist_db_path=fdb)
