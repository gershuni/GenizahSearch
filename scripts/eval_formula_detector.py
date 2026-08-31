# -*- coding: utf-8 -*-
"""Evaluate the formula-detector candidates on every owner-graded row.

The owner's acceptance condition (2026-08-30): show, on the gradings we hold,
that the detector catches the real formulas and does not catch real
witnesses. This script is EVALUATION ONLY -- it writes nothing to any
artifact.

Labels: _tmp/formula_eval_labels.json (assembled from the two v5 decks and
the A0C round joined by (sys_id, work)): 'formula' positives vs 'witness'
negatives (owner grades correct/cowitness/partial).

Detectors evaluated, separately and as a union:
  S  section/domain evidence -- fires when the claimed work's domain is fixed
     prayer (Liturgy and Brakhot / Common Prayers) or the row's locus names a
     liturgy/formulary section (curated keyword list; sections whose head is
     'הלכות' are halakha ABOUT prayer and do not fire unless the section also
     names a rite order or formulary).
  R  target-specific residue (the Codex design) -- the span's 20-grams are
     looked up across ALL reference work families; fires when >=REQ_COMMON of
     the grams occur in >=2 families other than the claimed one AND the span
     contains no run of >=UNIQ_RUN consecutive grams unique to the claimed
     family. "No unique residue" is what makes commonness damning.
"""
from __future__ import annotations

import json
import os
import pickle
import re
import sqlite3
import sys
import unicodedata
from collections import defaultdict

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
G = 20
REQ_COMMON = 0.90     # span is ~wholly common material
UNIQ_RUN = 10         # grams (i.e. a ~30-letter unique stretch rescues a span)

FINALS = {"ך": "כ", "ם": "מ", "ן": "נ", "ף": "פ", "ץ": "צ"}
_HEB = re.compile("[א-ת]")

KW = re.compile("סדר תפלות|סדר תפילות|ברכות השחר|סדור|סידור|מחזור|קדיש|פיוט|"
                "נוסח הברכות|נוסח התפלה|נוסח התפילה|שטר|סדר רב עמרם|"
                "סדר ראש השנה|סדר יום הכפורים|סדר תעניות|הגדה של פסח|"
                "עמידה|שמונה עשרה|ברכת המזון")
NEG_HEAD = re.compile(r"^הלכות ")
LITURGY_DOMAINS = ("Liturgy and Brakhot / Common Prayers",
                   "Liturgy and Brakhot / Brakhot")


def fold(t):
    t = unicodedata.normalize("NFC", t or "")
    return "".join(FINALS.get(c, c) for c in _HEB.findall(t))


def detector_s(domain, locus):
    if domain in LITURGY_DOMAINS:
        return True
    if not locus:
        return False
    if NEG_HEAD.search(locus) and not re.search("סדר תפלות|סדר תפילות|נוסח", locus):
        return False
    return bool(KW.search(locus))


def main():
    labels = json.load(open(os.path.join(REPO_ROOT, "_tmp",
                                         "formula_eval_labels.json"),
                            encoding="utf-8"))
    con = sqlite3.connect("file:%s?mode=ro" % os.path.join(
        REPO_ROOT, "discovery_data", "discovery-v5-REVIEW.db"), uri=True)
    con.row_factory = sqlite3.Row
    rows = {}
    for ev in labels:
        r = con.execute(
            "SELECT r.evidence_id, r.work_id, r.domain, r.locus_label, "
            "r.ref_match, rw.raw_id "
            "FROM review_row r "
            "LEFT JOIN reference_witness rw ON rw.witness_id = r.witness_id "
            "WHERE r.evidence_id=?", (ev,)).fetchone()
        if r:
            rows[ev] = dict(r)
    con.close()
    print("rows loaded: %d of %d labels" % (len(rows), len(labels)))

    # ---- family map + streams ---------------------------------------------
    say = print
    say("loading reference streams (v4.2 pkl + G-R combined)...")
    fam_of = {}          # work key -> family id
    streams = []         # (family, stream)
    v42 = pickle.load(open(os.path.join(
        REPO_ROOT, "discovery_builds", "discovery_v4_2", "build",
        "ref_corpus_v6.pkl"), "rb"))
    for x in v42:
        fam = x.get("vgroup") or x["id"]
        fam_of[x["id"]] = fam
        streams.append((fam, x["stream"]))
    gr = pickle.load(open(os.path.join(
        REPO_ROOT, "same_work_spike", "probe", "rsource", "data",
        "ref_gr_combined.pkl"), "rb"))
    gr_iter = gr.items() if isinstance(gr, dict) else (
        (x.get("id"), x.get("stream")) for x in gr)
    n_gr = 0
    for wid, st in gr_iter:
        if isinstance(st, dict):
            st = st.get("stream")
        if not wid or not st or not str(wid).startswith(("RS:",)):
            continue
        fam_of[wid] = wid.rsplit(".", 1)[0]   # RS family = file group
        streams.append((fam_of[wid], st))
        n_gr += 1
    say("  %d v4.2 works + %d RS works" % (len(v42), n_gr))

    # claimed family per labeled row: base work ids match pkl ids; RS rows
    # via their raw_id
    claimed = {}
    for ev, r in rows.items():
        wid = r["raw_id"] if (r["raw_id"] or "").startswith("RS:") else r["work_id"]
        claimed[ev] = fam_of.get(wid, wid)

    # ---- one pass: which families contain each span gram -------------------
    span_grams = {}
    want = set()
    for ev, r in rows.items():
        s = fold(r["ref_match"])
        gs = [s[i:i + G] for i in range(len(s) - G + 1)]
        span_grams[ev] = gs
        want.update(gs)
    say("distinct span grams: %d" % len(want))
    fams = defaultdict(set)
    for fam, st in streams:
        for i in range(len(st) - G + 1):
            g = st[i:i + G]
            if g in want:
                fams[g].add(fam)

    # ---- score --------------------------------------------------------------
    def detector_r(ev):
        gs = span_grams[ev]
        if len(gs) < 3:
            return False                      # too short for a gram verdict
        cf = claimed[ev]
        common = [len(fams.get(g, ()) - {cf}) >= 2 for g in gs]
        frac = sum(common) / len(gs)
        run = best = 0
        for g in gs:
            others = fams.get(g, set()) - {cf}
            if not others and cf in fams.get(g, set()):
                run += 1
                best = max(best, run)
            else:
                run = 0
        return frac >= REQ_COMMON and best < UNIQ_RUN

    table = defaultdict(lambda: defaultdict(int))
    misses, false_hits = [], []
    for ev, (kind, src) in labels.items():
        if ev not in rows:
            continue
        r = rows[ev]
        s_fire = detector_s(r["domain"], r["locus_label"])
        r_fire = detector_r(ev)
        u = s_fire or r_fire
        for det, fire in (("S", s_fire), ("R", r_fire), ("UNION", u)):
            table[det]["%s_%s" % (kind, "hit" if fire else "miss")] += 1
        if kind == "formula" and not u:
            misses.append((ev, r["locus_label"]))
        if kind == "witness" and u:
            false_hits.append((ev, "S" if s_fire else "R",
                               (r["locus_label"] or "")[:60]))

    for det in ("S", "R", "UNION"):
        t = table[det]
        print("%-6s formulas caught %d/%d   witnesses wrongly hit %d/%d"
              % (det, t["formula_hit"], t["formula_hit"] + t["formula_miss"],
                 t["witness_hit"], t["witness_hit"] + t["witness_miss"]))
    for ev, loc in misses:
        print("  FORMULA MISSED:", ev[:10], (loc or "")[:70])
    for ev, det, loc in false_hits:
        print("  WITNESS HIT (%s): %s %s" % (det, ev[:10], loc))
    return 0


if __name__ == "__main__":
    sys.exit(main())
