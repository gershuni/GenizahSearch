# -*- coding: utf-8 -*-
"""Attach the CARD grain to the v5 private review db: one card per
(page_id, known_work).

Why a second grain. `review_row` is one row per ALIGNMENT: the same page and the
same work can appear several times because a work has several witnesses (an
R-source whole-work file and the base corpus's per-book works, two editions,
two halves of one midrash). A grader asked "is this page this work?" should be
asked ONCE, with every witness's evidence beneath the question. Measured on the
current artifact: 519,382 evidence rows collapse to 433,913 cards over 211,510
pages, and 12.6% of cards hold more than one row.

What a card does NOT do. It never merges evidence: `card_member` keeps every
`evidence_id`, its raw `work_id`, its witness `scope`/`scope_prefix` and the
membership/route basis that put it there, so each row's offsets still point at
its own file. Where a card's rows disagree, the card says 'mixed' rather than
picking a side, and its locus is left NULL rather than guessed.

Routing. A work with exactly one `known_work_member` sends all its rows there.
The anthology containers are the only multi-member works: their rows route by
the LONGEST pinned `scope_prefix` their `locus_label` starts with -- the same
rule `scripts/build_work_registry.py` used to count them. A row that routes
nowhere refuses the build; it never lands in a default bucket.

Run (review server STOPPED -- it holds the db):
    python -X utf8 scripts/attach_review_cards.py
"""
import argparse
import hashlib
import os
import sqlite3
import sys
from collections import Counter, defaultdict

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DB = os.path.join(REPO_ROOT, "discovery_data", "discovery-v5-REVIEW.db")

DOC = ("Card grain: one card per (page_id, known_work) -- the unit of the "
       "question 'is this page this work?'. card_member keeps every evidence "
       "row with its raw work_id, witness scope and route basis; nothing is "
       "merged. A column whose rows disagree reads 'mixed'; a card whose rows "
       "carry different loci has locus_label NULL and locus_variants > 1. "
       "known_work_assertion rows are NEVER evidence and never reach a card.")

# NULL is a value with a meaning ('this row was never scored'), so it is named
# rather than dropped -- an aggregate that silently skips NULLs would report
# agreement where there is none.
UNSET = "unset"
MIXED = "mixed"

# columns summarized onto the card (a single agreed value, else MIXED)
SUMMARY_COLS = ("routing_status", "novelty_status", "confidence_band",
                "router_verdict", "adjudication_status", "claim_type",
                "domain", "locus_status")

DDL = [
    """CREATE TABLE card(
  card_id TEXT PRIMARY KEY,
  page_id TEXT NOT NULL,
  kw_id TEXT NOT NULL REFERENCES known_work(kw_id),
  sys_id TEXT NOT NULL,
  shelfmark TEXT,
  library_code TEXT,
  evidence_rows INTEGER NOT NULL,
  witnesses INTEGER NOT NULL,
  kw_witnesses INTEGER NOT NULL,
  best_matched_letters INTEGER,
  best_coverage_ppm INTEGER,
  locus_label TEXT,
  locus_variants INTEGER NOT NULL,
  locus_status TEXT NOT NULL,
  main_pool TEXT NOT NULL CHECK (main_pool IN ('yes','no','unset','mixed')),
  routing_status TEXT NOT NULL,
  novelty_status TEXT NOT NULL,
  confidence_band TEXT NOT NULL,
  router_verdict TEXT NOT NULL,
  adjudication_status TEXT NOT NULL,
  claim_type TEXT NOT NULL,
  domain TEXT NOT NULL,
  source_corpora TEXT NOT NULL,
  provisional INTEGER NOT NULL,
  UNIQUE(page_id, kw_id)
)""",
    """CREATE TABLE card_member(
  evidence_id TEXT PRIMARY KEY REFERENCES review_row(evidence_id),
  card_id TEXT NOT NULL REFERENCES card(card_id),
  work_id TEXT NOT NULL,
  scope TEXT NOT NULL,
  scope_prefix TEXT,
  member_basis TEXT NOT NULL,
  route_basis TEXT
)""",
    "CREATE INDEX ix_card_page ON card(page_id)",
    "CREATE INDEX ix_card_kw ON card(kw_id)",
    "CREATE INDEX ix_card_sys ON card(sys_id)",
    "CREATE INDEX ix_cm_card ON card_member(card_id)",
    "CREATE INDEX ix_cm_work ON card_member(work_id)",
]


class GateError(SystemExit):
    pass


def card_id_of(page_id, kw_id):
    """Content-derived, like the artifact's other auxiliary keys: a rebuild
    reproduces the same ids, and two independently rendered halves cannot
    collide."""
    return "c" + hashlib.sha256(
        f"{page_id}|{kw_id}".encode("utf-8")).hexdigest()[:16]


def summarize(values):
    """One agreed value, or MIXED. Never a majority vote: a card that mixes
    'confirms' and 'diverges_work' must not read as either."""
    seen = {v if (v is not None and v != "") else UNSET for v in values}
    if not seen:
        return UNSET
    if len(seen) == 1:
        return next(iter(seen))
    return MIXED


def summarize_pool(values):
    seen = {("unset" if v is None else ("yes" if v else "no")) for v in values}
    return next(iter(seen)) if len(seen) == 1 else MIXED


def best(values):
    """The largest measured value, or None when nothing was measured. A real 0
    must not be laundered into 'unknown'."""
    got = [v for v in values if v is not None]
    return max(got) if got else None


def route_rows(con, say=print):
    """(evidence_id -> member) for every review_row, or refuse.

    Returns (routes, members) where routes maps evidence_id to
    (work_id, scope) and members maps (work_id, scope) to its member record.
    """
    members = {}
    by_work = defaultdict(list)
    for wid, scope, prefix, basis, rbasis, rows in con.execute(
            "SELECT work_id, scope, scope_prefix, basis, route_basis, "
            "evidence_rows FROM known_work_member"):
        members[(wid, scope)] = dict(work_id=wid, scope=scope, prefix=prefix,
                                     basis=basis, route_basis=rbasis, rows=rows)
        by_work[wid].append((wid, scope))
    kw_of = dict(con.execute(
        "SELECT work_id || '\x1f' || scope, kw_id FROM known_work_member"))

    # an assertion is an identity claim with NO evidence -- if it ever grew rows
    # the card layer would be inventing evidence for it (Codex round-8)
    for (wid,) in con.execute("SELECT work_id FROM known_work_assertion"):
        n = con.execute("SELECT COUNT(*) FROM review_row WHERE work_id=?",
                        (wid,)).fetchone()[0]
        if n:
            raise GateError(f"asserted work {wid} carries {n} evidence rows -- "
                            "an assertion is never evidence")

    routes = {}
    unrouted = Counter()
    counts = Counter()
    for ev, wid, label in con.execute(
            "SELECT evidence_id, work_id, locus_label FROM review_row"):
        keys = by_work.get(wid)
        if not keys:
            raise GateError(f"work {wid} has no known-work membership "
                            "-- rebuild the registry before the cards")
        if len(keys) == 1:
            key = keys[0]
        else:
            hits = [k for k in keys
                    if members[k]["prefix"]
                    and (label or "").startswith(members[k]["prefix"])]
            if not hits:
                unrouted[(wid, label)] += 1
                continue
            key = max(hits, key=lambda k: len(members[k]["prefix"]))
        routes[ev] = key
        counts[key] += 1
    if unrouted:
        worst = unrouted.most_common(5)
        raise GateError(f"{sum(unrouted.values())} rows route to no witness "
                        f"scope: {worst}")
    # the registry pinned how many rows each witness holds; the card layer must
    # reproduce those counts EXACTLY or the two grains disagree
    drift = [(k, counts.get(k, 0), m["rows"]) for k, m in members.items()
             if counts.get(k, 0) != m["rows"]]
    if drift:
        raise GateError(f"{len(drift)} witnesses whose routed row count differs "
                        f"from known_work_member.evidence_rows: {drift[:5]}")
    say(f"routed {len(routes)} evidence rows to {len(counts)} witnesses")
    return routes, members, kw_of


def build(db_path, say=print):
    con = sqlite3.connect(db_path)
    try:
        return _build(con, say=say)
    finally:
        try:
            con.close()
        except sqlite3.Error:
            pass


def _build(con, say=print):
    con.execute("PRAGMA foreign_keys=ON")
    have = {r[0] for r in con.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    for t in ("review_row", "known_work", "known_work_member",
              "known_work_assertion"):
        if t not in have:
            raise GateError(f"{t} is missing -- run build_work_registry.py first")
    meta = dict(con.execute("SELECT key, value FROM meta"))
    if meta.get("work_registry.pins_root_verified") != "yes":
        raise GateError("the registry was not built against a verified trusted "
                        "root; refusing to project cards from it")
    registry_root = meta.get("work_registry.pins_sha256") or ""

    routes, members, kw_of = route_rows(con, say=say)
    kw_witnesses = Counter(
        k for (k,) in con.execute("SELECT kw_id FROM known_work_member"))
    provisional = dict(con.execute("SELECT kw_id, provisional FROM known_work"))

    # ---- group rows into cards ------------------------------------------------
    cols = ("evidence_id", "page_id", "sys_id", "shelfmark", "library_code",
            "main_pool", "matched_letters", "coverage_ppm", "locus_label",
            "source_corpus") + SUMMARY_COLS
    cards = {}
    card_members = []
    for row in con.execute(f"SELECT {', '.join(cols)} FROM review_row"):
        r = dict(zip(cols, row))
        key = routes[r["evidence_id"]]
        kw = kw_of[key[0] + "\x1f" + key[1]]
        cid = card_id_of(r["page_id"], kw)
        c = cards.get(cid)
        if c is None:
            c = cards[cid] = dict(
                card_id=cid, page_id=r["page_id"], kw_id=kw, sys_id=r["sys_id"],
                shelfmark=r["shelfmark"], library_code=r["library_code"],
                rows=[], witnesses=set(), corpora=set())
        # A shelfmark belongs to the PAGE, so any row of this card that has one
        # speaks for the card. Taking it from whichever row arrived first left
        # 1,584 of 2,021 multi-row cards blank in the header while a sibling row
        # printed the shelfmark a few lines below.
        if not c["shelfmark"] and r["shelfmark"]:
            c["shelfmark"] = r["shelfmark"]
        if not c["library_code"] and r["library_code"]:
            c["library_code"] = r["library_code"]
        c["rows"].append(r)
        c["witnesses"].add(key)
        c["corpora"].add(r["source_corpus"] or UNSET)
        m = members[key]
        card_members.append((r["evidence_id"], cid, key[0], key[1], m["prefix"],
                             m["basis"], m["route_basis"]))

    card_rows = []
    for cid, c in cards.items():
        rows = c["rows"]
        loci = {r["locus_label"] for r in rows if r["locus_label"]}
        card_rows.append((
            cid, c["page_id"], c["kw_id"], c["sys_id"], c["shelfmark"],
            c["library_code"], len(rows), len(c["witnesses"]),
            kw_witnesses[c["kw_id"]],
            best(r["matched_letters"] for r in rows),
            best(r["coverage_ppm"] for r in rows),
            # a single agreed locus, or NONE -- never one row's label standing
            # in for the card
            (next(iter(loci)) if len(loci) == 1 else None), len(loci),
            summarize(r["locus_status"] for r in rows),
            summarize_pool(r["main_pool"] for r in rows),
        ) + tuple(summarize(r[k] for r in rows)
                  for k in SUMMARY_COLS if k != "locus_status") + (
            " · ".join(sorted(c["corpora"])),
            provisional[c["kw_id"]],
        ))

    check_card_gates(card_rows, card_members, cards, members, kw_witnesses)

    # ---- write ---------------------------------------------------------------
    try:
        con.execute("BEGIN")
        for t in ("card_member", "card"):
            con.execute(f"DROP TABLE IF EXISTS {t}")
        for ddl in DDL:
            con.execute(ddl)
        con.executemany(
            "INSERT INTO card VALUES (%s)" % ",".join("?" * 24), card_rows)
        con.executemany(
            "INSERT INTO card_member VALUES (?,?,?,?,?,?,?)", card_members)
        n_pages = len({c["page_id"] for c in cards.values()})
        n_ms = len({c["sys_id"] for c in cards.values()})
        for k, v in (("card_grain.version", "1"),
                     ("card_grain.cards", str(len(card_rows))),
                     ("card_grain.members", str(len(card_members))),
                     ("card_grain.pages", str(n_pages)),
                     ("card_grain.manuscripts", str(n_ms)),
                     ("card_grain.registry_pins_sha256", registry_root),
                     ("doc.card_grain", DOC)):
            con.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (k, v))
        con.execute("COMMIT")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    # THREE NAMED NUMBERS, never one total that hides which grain it counts
    say(f"cards: {len(card_rows)}  evidence rows: {len(card_members)}  "
        f"manuscripts: {n_ms} (pages: {n_pages})")
    multi = sum(1 for r in card_rows if r[6] > 1)
    say(f"cards holding more than one evidence row: {multi} "
        f"({multi / len(card_rows) * 100:.1f}%)")
    return len(card_rows), len(card_members), n_ms


def check_card_gates(card_rows, card_members, cards, members, kw_witnesses):
    """Reconciliation gates over the finished projection (separate so each can
    be exercised directly)."""
    ids = [m[0] for m in card_members]
    if len(set(ids)) != len(ids):
        dup = [e for e, n in Counter(ids).items() if n > 1][:5]
        raise GateError(f"evidence row in more than one card: {dup}")
    cids = [r[0] for r in card_rows]
    if len(set(cids)) != len(cids):
        raise GateError("card_id collision")
    pairs = {(r[1], r[2]) for r in card_rows}
    if len(pairs) != len(card_rows):
        raise GateError("two cards for one (page_id, known_work)")
    per_card = Counter(m[1] for m in card_members)
    for r in card_rows:
        if per_card[r[0]] != r[6]:
            raise GateError(f"card {r[0]}: evidence_rows={r[6]} but "
                            f"{per_card[r[0]]} members")
        if r[7] > r[8]:
            raise GateError(f"card {r[0]}: {r[7]} witnesses aligned but its "
                            f"known work has only {r[8]}")
        # a stated locus must be the ONE label its own evidence carries
        if r[11] is not None and r[12] != 1:
            raise GateError(f"card {r[0]}: locus_label set with "
                            f"{r[12]} distinct labels")
    known = set(members)
    for ev, cid, wid, scope, prefix, basis, rbasis in card_members:
        if (wid, scope) not in known:
            raise GateError(f"card_member {ev}: ({wid}, {scope}) is not a "
                            "known_work_member")
    if sum(r[6] for r in card_rows) != len(card_members):
        raise GateError("card evidence_rows do not sum to the member count")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=DEFAULT_DB)
    args = ap.parse_args(argv)
    build(args.db)
    return 0


if __name__ == "__main__":
    sys.exit(main())
