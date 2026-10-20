#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Append-preservation gate: a new discovery artifact may not silently lose live identifications.

WHY THIS EXISTS (OPS-02, v9.0.0 audit finding C3)
-------------------------------------------------
Track-1 matching is not monotonic in the size of the reference corpus:
``REF_DF_CAP`` drops every posting of an over-cap gram, so ADDING reference works
removes match seeds from old, unrelated works. The V4 -> V4.2 append shipped with
131 live main-pool identifications deleted, and that loss was invisible until it was
measured by hand. The owner accepted it (2026-08-19) on one condition: every future
append is diffed against live before it ships, and the diff is a gate, not a report.

The hand measurement also missed half of the damage. Run on the same two artifacts,
this gate finds the 131 deletions AND 523 more live main-pool identifications that
were demoted below the main-pool bar, 406 of them with lower coverage than live.

WHAT IT CHECKS
--------------
A *loss* is a live main-pool identification (a ``discovery_identification`` row with
``main_pool = 1``, keyed by ``(sys_id, canonical_work_id)``) that is not in the
candidate's main pool: either gone, or still there below the bar. Each loss gets
exactly one cause from ``CAUSES``, computed from the two public artifacts. The cause
explains a loss; it never excuses one. Any loss fails the gate unless an acceptance
file names exactly that set of losses, for exactly these two files (by SHA-256, the
same ``content_hash`` the manifest and the loader use), and carries a ruling.

Live identifications outside the main pool that disappear are counted in the summary
but do not block: the owner's condition is about the main pool.

USAGE
-----
    # dev box, before upload: LIVE_DB must be a byte copy of what production serves
    python scripts/verify_append_preservation.py LIVE_DB CANDIDATE_DB \\
        --report-html _tmp/append-delta.html --draft-acceptance _tmp/append-acceptance.json

    # web box, pre-swap: resolve both files from their manifests, exactly as the loader does
    python scripts/verify_append_preservation.py \\
        --live-manifest discovery_data/manifest.json \\
        --candidate-manifest discovery_data/manifest.json.candidate \\
        --acceptance scripts/discovery_append_acceptance_<live12>_to_<candidate12>.json

A draft acceptance is written with an empty ruling and fails until the owner fills in
``ruled_by``, ``ruled_on`` and ``ruling``. A ruled one is committed next to the other
owner-approved discovery inputs in ``scripts/`` (``data/*`` is gitignored), named by
the first 12 hex of each hash. Rollback (repointing the manifest to the prior asset)
does not run this gate.

Exit codes: 0 no unaccepted loss, 1 loss or any check failure (fail-closed),
2 usage error.
"""
from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import re
import sqlite3
import sys
from collections import Counter
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Set, Tuple

ACCEPTANCE_SCHEMA_VERSION = "discovery-append-acceptance-v1"

# The closed list of causes. Every loss gets exactly one; the order is the order of
# the checks in ``classify_losses``.
CAUSE_DEMOTED = "demoted"  # still identified, below the main-pool bar
CAUSE_PAGE_REATTRIBUTED = "deleted_page_reattributed"  # its page now ships another work
CAUSE_EVIDENCE_KEPT = "deleted_evidence_kept"  # evidence for the pair remains, no identification
CAUSE_NO_PAIR_EVIDENCE = "deleted_no_pair_evidence"  # nothing for the pair is left
CAUSES = (CAUSE_DEMOTED, CAUSE_PAGE_REATTRIBUTED, CAUSE_EVIDENCE_KEPT, CAUSE_NO_PAIR_EVIDENCE)

# Only the columns this gate reads. A missing one fails closed.
REQUIRED_COLUMNS: Dict[str, Tuple[str, ...]] = {
    "meta": ("key", "value"),
    "works": ("work_id", "canonical_work_id", "neutral_title"),
    "discovery_claim": ("claim_id", "work_id"),
    "discovery_evidence": ("claim_id", "sys_id", "a_page_id", "routing_status"),
    "discovery_identification": (
        "sys_id", "canonical_work_id", "display_work_id", "main_pool", "main_pool_reason",
        "page_count", "max_coverage_ppm", "novelty_status",
    ),
    "manuscript_display": ("sys_id", "library_code", "shelfmark_display"),
}

_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
_IN_CHUNK = 500  # stays under SQLite's bound-parameter limit

Key = Tuple[str, str]  # (sys_id, canonical_work_id)


class GateError(Exception):
    """A check failed. The message is written from this file's own constants and
    counts, never from a cell value (the discovery masking discipline)."""


@dataclass
class Loss:
    sys_id: str
    canonical_work_id: str
    cause: str
    detail: str = ""  # the candidate's main_pool_reason, for a demotion
    live_coverage_ppm: Optional[int] = None
    candidate_coverage_ppm: Optional[int] = None
    live_page_count: Optional[int] = None
    novelty_status: Optional[str] = None
    display_work_id: Optional[str] = None


@dataclass
class Delta:
    live_sha256: str
    candidate_sha256: str
    live_main_pool: int
    candidate_main_pool: int
    losses: List[Loss] = field(default_factory=list)
    other_pool_deleted: int = 0
    main_pool_gained: int = 0

    def lost_keys(self) -> List[Key]:
        return sorted((x.sys_id, x.canonical_work_id) for x in self.losses)


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------

def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def resolve_from_manifest(manifest_path: str, label: str) -> Tuple[str, str]:
    """``(db_path, content_hash)`` for the EXACT file a manifest names, by the
    loader's own rule (``web/discovery_assets.py::_resolve_versioned_db``)."""
    try:
        with open(manifest_path, "rb") as fh:
            manifest = json.loads(fh.read().decode("utf-8"))
    except (OSError, ValueError) as exc:
        raise GateError(f"{label} manifest unreadable ({type(exc).__name__})") from None
    basename = manifest.get("asset_basename") if isinstance(manifest, dict) else None
    if not isinstance(basename, str) or not basename:
        raise GateError(f"{label} manifest has no string asset_basename")
    if "/" in basename or "\\" in basename or os.path.isabs(basename) or basename in (".", ".."):
        raise GateError(f"{label} manifest asset_basename is not a bare filename stem")
    content_hash = manifest.get("content_hash")
    if not isinstance(content_hash, str) or not _SHA256_RE.fullmatch(content_hash):
        raise GateError(f"{label} manifest has no 64-hex content_hash")
    db_path = os.path.join(os.path.dirname(os.path.abspath(manifest_path)), basename + ".db")
    return db_path, content_hash


def open_artifact(path: str, label: str) -> sqlite3.Connection:
    # mode=ro: a bare connect on a missing path would create a 0-byte stub.
    if not os.path.isfile(path):
        raise GateError(f"{label} artifact not found")
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        for table, cols in REQUIRED_COLUMNS.items():
            have = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
            if not have:
                raise GateError(f"{label} artifact has no table {table}")
            missing = [c for c in cols if c not in have]
            if missing:
                raise GateError(f"{label} artifact table {table} lacks column(s) {', '.join(missing)}")
        row = conn.execute("SELECT value FROM meta WHERE key = 'audience'").fetchone()
        if not row or row[0] != "public":
            raise GateError(f"{label} artifact is not a public projection (meta.audience)")
        dup = conn.execute(
            "SELECT COUNT(*) FROM (SELECT 1 FROM discovery_identification "
            "GROUP BY sys_id, canonical_work_id HAVING COUNT(*) > 1)").fetchone()[0]
        if dup:
            raise GateError(f"{label} artifact has {dup} duplicate (sys_id, canonical_work_id) key(s)")
    except sqlite3.Error as exc:
        raise GateError(f"{label} artifact unreadable ({type(exc).__name__})") from None
    return conn


# ---------------------------------------------------------------------------
# The comparison
# ---------------------------------------------------------------------------

def _identifications(conn: sqlite3.Connection) -> Dict[Key, tuple]:
    return {
        (r[0], r[1]): r[2:]
        for r in conn.execute(
            "SELECT sys_id, canonical_work_id, main_pool, main_pool_reason, max_coverage_ppm, "
            "page_count, novelty_status, display_work_id FROM discovery_identification")
    }


def _evidence_by_sys_id(conn: sqlite3.Connection, sys_ids: Sequence[str]) -> Dict[str, list]:
    """``sys_id -> [(a_page_id, routing_status, canonical_work_id), ...]``."""
    out: Dict[str, list] = {}
    ids = sorted(set(sys_ids))
    for i in range(0, len(ids), _IN_CHUNK):
        chunk = ids[i:i + _IN_CHUNK]
        rows = conn.execute(
            "SELECT de.sys_id, de.a_page_id, de.routing_status, w.canonical_work_id "
            "FROM discovery_evidence de "
            "JOIN discovery_claim dc ON dc.claim_id = de.claim_id "
            "JOIN works w ON w.work_id = dc.work_id "
            f"WHERE de.sys_id IN ({','.join('?' * len(chunk))})", chunk)
        for sys_id, page, status, work in rows:
            out.setdefault(sys_id, []).append((page, status, work))
    return out


def classify_losses(live: sqlite3.Connection, candidate: sqlite3.Connection,
                    live_sha256: str, candidate_sha256: str) -> Delta:
    live_ids = _identifications(live)
    cand_ids = _identifications(candidate)
    delta = Delta(
        live_sha256=live_sha256,
        candidate_sha256=candidate_sha256,
        live_main_pool=sum(1 for v in live_ids.values() if v[0] == 1),
        candidate_main_pool=sum(1 for v in cand_ids.values() if v[0] == 1),
    )
    deleted: List[Key] = []
    for key, (main_pool, _reason, coverage, pages, novelty, display) in sorted(live_ids.items()):
        cand = cand_ids.get(key)
        if main_pool != 1:
            if cand is None:
                delta.other_pool_deleted += 1
            continue
        if cand is not None and cand[0] == 1:
            continue
        loss = Loss(key[0], key[1], cause="", live_coverage_ppm=coverage,
                    live_page_count=pages, novelty_status=novelty, display_work_id=display)
        if cand is not None:
            loss.cause = CAUSE_DEMOTED
            loss.detail = cand[1] or ""
            loss.candidate_coverage_ppm = cand[2]
        else:
            deleted.append(key)
        delta.losses.append(loss)
    delta.main_pool_gained = sum(
        1 for key, v in cand_ids.items()
        if v[0] == 1 and live_ids.get(key, (None,))[0] != 1)

    if deleted:
        sys_ids = [k[0] for k in deleted]
        live_ev = _evidence_by_sys_id(live, sys_ids)
        cand_ev = _evidence_by_sys_id(candidate, sys_ids)
        by_key = {(x.sys_id, x.canonical_work_id): x for x in delta.losses}
        for sys_id, work in deleted:
            pages = {p for p, status, w in live_ev.get(sys_id, ())
                     if w == work and status == "shipped"}
            cand_rows = cand_ev.get(sys_id, ())
            if any(p in pages and status == "shipped" and w != work for p, status, w in cand_rows):
                cause = CAUSE_PAGE_REATTRIBUTED
            elif any(w == work for _p, _s, w in cand_rows):
                cause = CAUSE_EVIDENCE_KEPT
            else:
                cause = CAUSE_NO_PAIR_EVIDENCE
            by_key[(sys_id, work)].cause = cause
    return delta


def lost_digest(keys: Sequence[Key]) -> str:
    h = hashlib.sha256()
    for sys_id, work in sorted(keys):
        h.update(f"{sys_id}\t{work}\n".encode("utf-8"))
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Acceptance
# ---------------------------------------------------------------------------

def draft_acceptance(delta: Delta) -> dict:
    return {
        "schema_version": ACCEPTANCE_SCHEMA_VERSION,
        "live_sha256": delta.live_sha256,
        "candidate_sha256": delta.candidate_sha256,
        "lost_count": len(delta.losses),
        "lost_sha256": lost_digest(delta.lost_keys()),
        "cause_counts": dict(sorted(Counter(x.cause for x in delta.losses).items())),
        "ruled_by": "",
        "ruled_on": "",
        "ruling": "",
        "lost": [[x.sys_id, x.canonical_work_id, x.cause]
                 for x in sorted(delta.losses, key=lambda x: (x.sys_id, x.canonical_work_id))],
    }


def check_acceptance(acceptance: object, delta: Delta) -> List[str]:
    """Every reason this acceptance does not cover this delta; empty means it does."""
    if not isinstance(acceptance, dict):
        return ["acceptance is not a JSON object"]
    problems: List[str] = []
    if acceptance.get("schema_version") != ACCEPTANCE_SCHEMA_VERSION:
        problems.append(f"acceptance schema_version is not {ACCEPTANCE_SCHEMA_VERSION}")
    if acceptance.get("live_sha256") != delta.live_sha256:
        problems.append("acceptance was ruled for a different LIVE artifact (live_sha256)")
    if acceptance.get("candidate_sha256") != delta.candidate_sha256:
        problems.append("acceptance was ruled for a different CANDIDATE artifact (candidate_sha256)")

    listed: Set[Key] = set()
    rows = acceptance.get("lost")
    if not isinstance(rows, list):
        problems.append("acceptance has no 'lost' list")
    else:
        for row in rows:
            if (not isinstance(row, list) or len(row) < 2
                    or not all(isinstance(v, str) for v in row[:2])):
                problems.append("acceptance 'lost' has a malformed row")
                break
            listed.add((row[0], row[1]))
    actual = set(delta.lost_keys())
    if listed - actual:
        problems.append(f"acceptance names {len(listed - actual)} loss(es) this candidate does not have")
    if actual - listed:
        problems.append(f"this candidate has {len(actual - listed)} loss(es) the acceptance does not name")
    if acceptance.get("lost_count") != len(actual):
        problems.append("acceptance lost_count does not match this candidate")
    if acceptance.get("lost_sha256") != lost_digest(sorted(actual)):
        problems.append("acceptance lost_sha256 does not match this candidate's losses")

    for name in ("ruled_by", "ruling"):
        value = acceptance.get(name)
        if not isinstance(value, str) or not value.strip():
            problems.append(f"acceptance has no {name} (a draft is not a ruling)")
    ruled_on = acceptance.get("ruled_on")
    if not isinstance(ruled_on, str) or not _DATE_RE.fullmatch(ruled_on):
        problems.append("acceptance ruled_on is not a YYYY-MM-DD date")
    return problems


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def summary_lines(delta: Delta) -> List[str]:
    causes = Counter(x.cause for x in delta.losses)
    demoted = [x for x in delta.losses if x.cause == CAUSE_DEMOTED]
    coverage_fell = sum(1 for x in demoted
                        if (x.candidate_coverage_ppm or 0) < (x.live_coverage_ppm or 0))
    lines = [
        f"live      {delta.live_sha256}  main pool {delta.live_main_pool:,}",
        f"candidate {delta.candidate_sha256}  main pool {delta.candidate_main_pool:,}",
        f"live main-pool identifications lost: {len(delta.losses):,}",
    ]
    for cause in CAUSES:
        lines.append(f"  {cause:<28} {causes.get(cause, 0):>7,}")
    if demoted:
        for reason, n in Counter(x.detail for x in demoted).most_common():
            lines.append(f"    demoted as {reason or '(no reason)':<22} {n:>7,}")
        lines.append(f"    demoted with coverage lower than live {coverage_fell:>7,}")
    lines.append(f"not blocking: live identifications outside the main pool deleted: "
                 f"{delta.other_pool_deleted:,}")
    lines.append(f"not blocking: main-pool identifications the candidate adds or promotes: "
                 f"{delta.main_pool_gained:,}")
    return lines


def write_report_html(path: str, delta: Delta, live: sqlite3.Connection) -> None:
    keys = [x.sys_id for x in delta.losses]
    display: Dict[str, Tuple[str, str]] = {}
    for i in range(0, len(keys), _IN_CHUNK):
        chunk = sorted(set(keys[i:i + _IN_CHUNK]))
        for sys_id, lib, shelf in live.execute(
                "SELECT sys_id, library_code, shelfmark_display FROM manuscript_display "
                f"WHERE sys_id IN ({','.join('?' * len(chunk))})", chunk):
            display[sys_id] = (lib or "", shelf or "")
    titles = {r[0]: r[1] for r in live.execute("SELECT work_id, neutral_title FROM works")}

    def esc(v) -> str:
        return html.escape(str(v)) if v not in (None, "") else "&mdash;"

    def pct(ppm) -> str:
        return f"{ppm / 10000:.0f}%" if isinstance(ppm, int) else "&mdash;"

    order = {c: i for i, c in enumerate(CAUSES[::-1])}  # deletions first
    rows = []
    for n, x in enumerate(sorted(delta.losses, key=lambda x: (
            order[x.cause], -(x.live_coverage_ppm or 0))), 1):
        lib, shelf = display.get(x.sys_id, ("", ""))
        title = titles.get(x.display_work_id) or titles.get(x.canonical_work_id)
        rows.append(
            f"<tr><td>{n}</td><td class='ltr'>{esc(shelf)}<br><small>{esc(lib)} · "
            f"{esc(x.sys_id)}</small></td><td class='he'>{esc(title)}<br>"
            f"<small class='ltr'>{esc(x.canonical_work_id)}</small></td>"
            f"<td>{esc(x.cause)}</td><td>{esc(x.detail)}</td>"
            f"<td>{pct(x.live_coverage_ppm)} &rarr; {pct(x.candidate_coverage_ppm)}</td>"
            f"<td>{esc(x.live_page_count)}</td><td>{esc(x.novelty_status)}</td></tr>")
    summary = html.escape("\n".join(summary_lines(delta)))
    page = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<title>Append delta</title><style>
body{{font-family:Segoe UI,sans-serif;margin:24px;background:#faf9f6;color:#222}}
table{{border-collapse:collapse;width:100%;font-size:14px}}
th,td{{border:1px solid #ddd;padding:5px 8px;text-align:left;vertical-align:top}}
th{{background:#efece5;position:sticky;top:0}} tr:nth-child(even){{background:#f4f2ec}}
.he{{direction:rtl;text-align:right;font-size:15px}} .ltr{{direction:ltr;white-space:nowrap}}
small{{color:#666}} pre{{background:#fff;border:1px solid #ddd;padding:12px}}
</style></head><body>
<h1>Live main-pool identifications this candidate loses</h1>
<pre>{summary}</pre>
<table><tr><th>#</th><th>Shelfmark</th><th>Work</th><th>Cause</th><th>Candidate reason</th>
<th>Coverage live &rarr; candidate</th><th>Live pages</th><th>Novelty</th></tr>
{''.join(rows)}</table></body></html>"""
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(page)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def run(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("live_db", nargs="?")
    ap.add_argument("candidate_db", nargs="?")
    ap.add_argument("--live-manifest")
    ap.add_argument("--candidate-manifest")
    ap.add_argument("--acceptance", help="owner-ruled acceptance JSON for exactly this delta")
    ap.add_argument("--report-html", help="write the per-loss review page here")
    ap.add_argument("--draft-acceptance", help="write an unruled acceptance draft here")
    args = ap.parse_args(argv)
    if hasattr(sys.stdout, "reconfigure"):  # a Hebrew ruled_by on a cp1255 console
        sys.stdout.reconfigure(errors="replace")

    by_path = args.live_db is not None or args.candidate_db is not None
    by_manifest = args.live_manifest is not None or args.candidate_manifest is not None
    if by_path == by_manifest or (by_path and not (args.live_db and args.candidate_db)) or (
            by_manifest and not (args.live_manifest and args.candidate_manifest)):
        ap.error("give LIVE_DB CANDIDATE_DB, or --live-manifest and --candidate-manifest")
    for flag, path in (("--report-html", args.report_html),
                       ("--draft-acceptance", args.draft_acceptance)):
        if path and os.path.exists(path):
            print(f"usage: {flag} target already exists; choose a new path", file=sys.stderr)
            return 2

    try:
        if by_manifest:
            live_path, live_expected = resolve_from_manifest(args.live_manifest, "live")
            cand_path, cand_expected = resolve_from_manifest(args.candidate_manifest, "candidate")
        else:
            live_path, cand_path = args.live_db, args.candidate_db
            live_expected = cand_expected = None
        live = open_artifact(live_path, "live")
        candidate = open_artifact(cand_path, "candidate")
        live_sha, cand_sha = sha256_file(live_path), sha256_file(cand_path)
        if live_expected and live_sha != live_expected:
            raise GateError("live artifact bytes do not match the live manifest's content_hash")
        if cand_expected and cand_sha != cand_expected:
            raise GateError("candidate artifact bytes do not match the candidate manifest's content_hash")
        delta = classify_losses(live, candidate, live_sha, cand_sha)
        acceptance = None
        if args.acceptance:
            try:
                with open(args.acceptance, "rb") as fh:
                    acceptance = json.loads(fh.read().decode("utf-8"))
            except (OSError, ValueError) as exc:
                raise GateError(f"acceptance unreadable ({type(exc).__name__})") from None
    except GateError as exc:
        print(f"FAIL: {exc}")
        return 1
    except sqlite3.Error as exc:
        print(f"FAIL: artifact query failed ({type(exc).__name__})")
        return 1

    for line in summary_lines(delta):
        print(line)
    if args.report_html:
        write_report_html(args.report_html, delta, live)
        print(f"report: {args.report_html}")
    if args.draft_acceptance:
        with open(args.draft_acceptance, "w", encoding="utf-8") as fh:
            json.dump(draft_acceptance(delta), fh, ensure_ascii=False, indent=1)
            fh.write("\n")
        print(f"draft acceptance (unruled): {args.draft_acceptance}")

    if not delta.losses:
        print("PASS: no live main-pool identification is lost")
        return 0
    if acceptance is None:
        print(f"FAIL: {len(delta.losses):,} live main-pool identification(s) lost and no "
              f"acceptance given")
        return 1
    problems = check_acceptance(acceptance, delta)
    if problems:
        for p in problems:
            print(f"FAIL: {p}")
        return 1
    print(f"PASS: all {len(delta.losses):,} losses accepted by {acceptance['ruled_by']} "
          f"on {acceptance['ruled_on']}")
    return 0


if __name__ == "__main__":
    sys.exit(run())
