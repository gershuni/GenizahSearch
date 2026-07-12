# -*- coding: utf-8 -*-
"""F-SQL-1: re-align every Stage-0 substitution against the untouched v1 HTR.

Read-only on BOTH DBs (sqlite URI mode=ro). Single process, BelowNormal
priority. For each of the 18,982 pages whose search text was REPLACED by a
human transcription (pages.provenance != 'htr' in fullcorpus_v2.db) we recover
the ORIGINAL HTR text from fullcorpus.db (same page_id, column pages.text) and
recompute the TRUE content coverage the substitution actually achieved.

Measures per substituted page (streams via normalize.norm_stream, exactly as
the pipeline compares text):
  len_htr, len_human, len_ratio = len_human/len_htr
  case          'A' human>=HTR length  |  'B' human shorter (danger zone)
  score         partial_ratio_alignment score (internal fidelity of the window)
  cov           span coverage of the HTR stream  (Case B: dest span / len_htr)
  faithful      cov * score/100          (audit's TRUE content coverage proxy)
  matched_frac  matched HTR chars / len_htr via Indel.opcodes  (stronger true
                preservation measure: interior gaps + divergence both count)

Risk band (audit F-SQL-1 criteria, Case B): cov<0.85 OR score<75 OR faithful<0.75
"""
import ctypes
import json
import sqlite3
import sys
import time

sys.path.insert(0, r"C:\Genizahsearch\same_work_spike\probe\scripts")
from normalize import norm_stream  # noqa: E402
from rapidfuzz.fuzz import partial_ratio_alignment as pra  # noqa: E402
from rapidfuzz.distance import Indel  # noqa: E402

# --- BelowNormal priority (a large deck build owns the machine) ---
try:
    BELOW_NORMAL_PRIORITY_CLASS = 0x00004000
    ctypes.windll.kernel32.SetPriorityClass(
        ctypes.windll.kernel32.GetCurrentProcess(), BELOW_NORMAL_PRIORITY_CLASS)
except Exception as e:  # noqa: BLE001
    print("priority set skipped:", e)

PROBE = r"C:\Genizahsearch\same_work_spike\probe"
V1 = PROBE + r"\data\fullcorpus.db"
V2 = PROBE + r"\data\fullcorpus_v2.db"
MD_OUT = PROBE + r"\results\mapv2_substitution_risk.md"
JSON_OUT = PROBE + r"\data\substitution_risk_pages.json"

COV_MIN = 0.85
SCORE_MIN = 75.0
FAITHFUL_MIN = 0.75


def matched_fraction(sh, sp):
    """Fraction of the HTR stream (sp) chars matched by the human stream (sh),
    via a global Indel alignment. Interior gaps and internal divergence both
    reduce this. sp is the 'destination' (HTR page) dimension."""
    if not sp:
        return 0.0
    matched = 0
    for tag, i1, i2, j1, j2 in Indel.opcodes(sh, sp):
        if tag == "equal":
            matched += (j2 - j1)  # chars of sp (HTR) preserved
    return matched / len(sp)


def bucketize(vals, edges):
    """Right-open buckets [edges[i], edges[i+1]); last bucket includes top."""
    counts = [0] * (len(edges) - 1)
    for v in vals:
        for i in range(len(edges) - 1):
            lo, hi = edges[i], edges[i + 1]
            if (lo <= v < hi) or (i == len(edges) - 2 and v == hi):
                counts[i] += 1
                break
    return counts


def main():
    t0 = time.time()
    v1 = sqlite3.connect(f"file:{V1}?mode=ro", uri=True)
    v2 = sqlite3.connect(f"file:{V2}?mode=ro", uri=True)

    rows = v2.execute(
        "SELECT page_id, sys_id, provenance, fgp_score, text "
        "FROM pages WHERE provenance!='htr'").fetchall()
    total = len(rows)
    print(f"substituted pages (provenance!='htr'): {total}")

    case_a = case_b = no_htr = empty_htr = 0
    # aggregate collectors
    lenratio_all = []
    b_cov = []
    b_score = []
    b_faithful = []
    b_matched = []
    a_matched = []       # Case A: fraction of HTR preserved (corroboration)
    a_score = []
    risky = []           # Case B risk-band detail records
    # per-condition tallies (Case B)
    fail_cov = fail_score = fail_faithful = 0
    # matched_frac severity tallies (Case B)
    sev_lt50 = sev_50_70 = sev_70_85 = 0
    prov_b = {"fgp": 0, "pgp": 0}
    prov_risky = {"fgp": 0, "pgp": 0}

    for k, (pid, sid, prov, stored_score, human) in enumerate(rows):
        h = v1.execute("SELECT text FROM pages WHERE page_id=?", (pid,)).fetchone()
        if not h or not h[0]:
            no_htr += 1
            continue
        sp, _ = norm_stream(h[0])          # original HTR stream
        sh, _ = norm_stream(human or "")   # stored (substituted) human stream
        if len(sp) == 0:
            empty_htr += 1
            continue
        lr = len(sh) / len(sp)
        lenratio_all.append(lr)

        if len(sh) >= len(sp):
            # Case A: human >= HTR length. HTR expected fully contained.
            case_a += 1
            r = pra(sp, sh)  # HTR as pattern found inside the (longer) human
            a_score.append(r.score if r else 0.0)
            a_matched.append(matched_fraction(sh, sp))
            continue

        # Case B: human shorter than HTR -> partial-draft danger zone.
        case_b += 1
        prov_b[prov] = prov_b.get(prov, 0) + 1
        r = pra(sh, sp)  # shorter human as pattern; dest span in HTR page
        if r is None:
            cov = 0.0
            score = 0.0
        else:
            cov = (r.dest_end - r.dest_start) / len(sp)
            score = r.score
        faithful = cov * (score / 100.0)
        mf = matched_fraction(sh, sp)
        b_cov.append(cov)
        b_score.append(score)
        b_faithful.append(faithful)
        b_matched.append(mf)

        is_risky = (r is None) or (cov < COV_MIN) or (score < SCORE_MIN) or (faithful < FAITHFUL_MIN)
        if is_risky:
            if cov < COV_MIN:
                fail_cov += 1
            if score < SCORE_MIN:
                fail_score += 1
            if faithful < FAITHFUL_MIN:
                fail_faithful += 1
            if mf < 0.50:
                sev_lt50 += 1
            elif mf < 0.70:
                sev_50_70 += 1
            elif mf < 0.85:
                sev_70_85 += 1
            prov_risky[prov] = prov_risky.get(prov, 0) + 1
            risky.append({
                "page_id": pid, "sys_id": sid, "provenance": prov,
                "stored_score": round(stored_score, 1) if stored_score is not None else None,
                "len_htr": len(sp), "len_human": len(sh),
                "len_ratio": round(lr, 3),
                "score": round(score, 1), "cov": round(cov, 3),
                "faithful": round(faithful, 3), "matched_frac": round(mf, 3),
            })

        if (k + 1) % 5000 == 0:
            print(f"  ...{k + 1}/{total} ({time.time() - t0:.0f}s)")

    v1.close()
    v2.close()
    elapsed = time.time() - t0
    print(f"pass complete in {elapsed:.0f}s")
    print(f"case A={case_a}  case B={case_b}  no_htr={no_htr}  empty_htr={empty_htr}")
    print(f"Case-B risk band: {len(risky)}  (fail_cov={fail_cov} fail_score={fail_score} fail_faithful={fail_faithful})")

    # ---- distributions ----
    lr_edges = [0.0, 0.6, 0.7, 0.8, 0.9, 1.0, 1.3, 2.0, 1e9]
    lr_labels = ["<0.6", "0.6-0.7", "0.7-0.8", "0.8-0.9", "0.9-1.0",
                 "1.0-1.3", "1.3-2.0", ">=2.0"]
    lr_counts = bucketize(lenratio_all, lr_edges)

    cov_edges = [0.0, 0.5, 0.6, 0.7, 0.8, 0.85, 0.9, 0.95, 1.0001]
    cov_labels = ["<0.5", "0.5-0.6", "0.6-0.7", "0.7-0.8", "0.8-0.85",
                  "0.85-0.9", "0.9-0.95", "0.95-1.0"]
    cov_counts = bucketize(b_cov, cov_edges)

    sc_edges = [0.0, 60, 70, 75, 80, 85, 90, 95, 100.0001]
    sc_labels = ["<60", "60-70", "70-75", "75-80", "80-85", "85-90",
                 "90-95", "95-100"]
    sc_counts = bucketize(b_score, sc_edges)

    fa_edges = [0.0, 0.5, 0.6, 0.7, 0.75, 0.8, 0.9, 1.0001]
    fa_labels = ["<0.5", "0.5-0.6", "0.6-0.7", "0.7-0.75", "0.75-0.8",
                 "0.8-0.9", "0.9-1.0"]
    fa_counts = bucketize(b_faithful, fa_edges)

    mf_edges = [0.0, 0.5, 0.6, 0.7, 0.8, 0.85, 0.9, 0.95, 1.0001]
    mf_labels = ["<0.5", "0.5-0.6", "0.6-0.7", "0.7-0.8", "0.8-0.85",
                 "0.85-0.9", "0.9-0.95", "0.95-1.0"]
    mf_counts = bucketize(b_matched, mf_edges)

    amf_counts = bucketize(a_matched, mf_edges)

    # worst 40 by faithful ascending (then matched_frac)
    risky_sorted = sorted(risky, key=lambda d: (d["faithful"], d["matched_frac"], d["cov"]))
    worst40 = risky_sorted[:40]

    # ---- markdown ----
    def hist(labels, counts, denom):
        lines = ["| bucket | n | % |", "|---|---:|---:|"]
        for lab, c in zip(labels, counts):
            pct = (100.0 * c / denom) if denom else 0.0
            lines.append(f"| {lab} | {c} | {pct:.1f}% |")
        return "\n".join(lines)

    def median(vals):
        if not vals:
            return 0.0
        s = sorted(vals)
        n = len(s)
        return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2

    b_damaged_mf = sum(1 for m in b_matched if m < 0.85)
    md = []
    md.append("# Stage-0 substitution risk — F-SQL-1 recompute (true HTR coverage)\n")
    md.append(f"- Generated: {time.strftime('%Y-%m-%d %H:%M')} | runtime {elapsed:.0f}s | "
              "read-only both DBs\n")
    md.append("- Method: re-align each stored (substituted) human stream against the "
              "ORIGINAL HTR stream preserved in `fullcorpus.db` (v1), same `page_id`. "
              "Streams via `normalize.norm_stream` (space-free folded Hebrew letters), "
              "`rapidfuzz.fuzz.partial_ratio_alignment` for score+span coverage, "
              "`rapidfuzz.distance.Indel.opcodes` for matched-character coverage.\n")
    n_fgp = sum(1 for r in rows if r[2] == 'fgp')
    n_pgp = sum(1 for r in rows if r[2] == 'pgp')
    md.append("\n## Totals\n")
    md.append(f"- Substituted pages (`provenance!='htr'`): **{total}** "
              f"(fgp={n_fgp}, pgp={n_pgp})\n")
    md.append(f"- **Case A** (human stream >= HTR length): **{case_a}** "
              f"({100.0*case_a/total:.1f}%)\n")
    md.append(f"- **Case B** (human stream SHORTER than HTR = danger zone): **{case_b}** "
              f"({100.0*case_b/total:.1f}%)\n")
    md.append(f"- HTR missing/empty in v1 (excluded): no_htr={no_htr}, empty_htr={empty_htr}\n")

    md.append("\n## Case-B risk bands (audit F-SQL-1 criteria)\n")
    md.append(f"Risk band = `cov<{COV_MIN}` OR `score<{SCORE_MIN}` OR `faithful<{FAITHFUL_MIN}` "
              "(faithful = span-coverage x internal-fidelity).\n")
    md.append(f"- **Risky Case-B substitutions: {len(risky)}** "
              f"({100.0*len(risky)/total:.2f}% of all {total} subs; "
              f"{100.0*len(risky)/case_b:.1f}% of Case-B) — fgp={prov_risky.get('fgp',0)}, "
              f"pgp={prov_risky.get('pgp',0)}\n")
    md.append(f"- fail coverage(<{COV_MIN}): {fail_cov}\n")
    md.append(f"- fail score(<{SCORE_MIN}): {fail_score}\n")
    md.append(f"- fail faithful(<{FAITHFUL_MIN}): {fail_faithful}\n")
    md.append("\n**True damage severity (matched-char coverage of the HTR page, "
              "risky rows only):**\n")
    md.append(f"- matched_frac < 0.50 (>=half of HTR page lost): {sev_lt50}\n")
    md.append(f"- 0.50-0.70 lost a large minority: {sev_50_70}\n")
    md.append(f"- 0.70-0.85 lost a modest slice: {sev_70_85}\n")
    md.append(f"- Case-B pages with matched_frac < 0.85 (ANY real HTR loss, "
              f"incl. not-flagged-by-band): {b_damaged_mf}\n")

    md.append("\n## Distributions\n")
    md.append("\n### length ratio human/HTR (all subs)\n")
    md.append(hist(lr_labels, lr_counts, len(lenratio_all)) + "\n")
    md.append("\n### Case-B span coverage of HTR page\n")
    md.append(hist(cov_labels, cov_counts, len(b_cov)) + "\n")
    md.append("\n### Case-B partial_ratio score\n")
    md.append(hist(sc_labels, sc_counts, len(b_score)) + "\n")
    md.append("\n### Case-B faithful (cov x score/100)\n")
    md.append(hist(fa_labels, fa_counts, len(b_faithful)) + "\n")
    md.append("\n### Case-B matched-char coverage (true HTR preservation)\n")
    md.append(hist(mf_labels, mf_counts, len(b_matched)) + "\n")
    md.append(f"\n- Case-B medians: cov={median(b_cov):.3f}, score={median(b_score):.1f}, "
              f"faithful={median(b_faithful):.3f}, matched_frac={median(b_matched):.3f}\n")
    md.append("\n### Case-A matched-char coverage of HTR (sanity: is HTR preserved when "
              "human is longer?)\n")
    md.append(hist(mf_labels, amf_counts, len(a_matched)) + "\n")
    md.append(f"- Case-A median matched_frac={median(a_matched):.3f}, "
              f"median score={median(a_score):.1f}; "
              f"Case-A with matched_frac<0.85: {sum(1 for m in a_matched if m<0.85)}\n")

    md.append("\n## 40 worst offenders (Case-B, by faithful ascending)\n")
    md.append("| page_id | sys_id | prov | stored_score | len_htr | len_human | "
              "score | cov | faithful | matched_frac |")
    md.append("|---|---|---|---:|---:|---:|---:|---:|---:|---:|")
    for d in worst40:
        md.append(f"| {d['page_id']} | {d['sys_id']} | {d['provenance']} | "
                  f"{d['stored_score']} | {d['len_htr']} | {d['len_human']} | "
                  f"{d['score']} | {d['cov']} | {d['faithful']} | {d['matched_frac']} |")
    md.append("")

    with open(MD_OUT, "w", encoding="utf-8") as f:
        f.write("\n".join(md))
    print("wrote", MD_OUT)

    # ---- json (all risky page_ids + detail records) ----
    payload = {
        "generated": time.strftime("%Y-%m-%d %H:%M"),
        "criteria": {"cov_min": COV_MIN, "score_min": SCORE_MIN,
                     "faithful_min": FAITHFUL_MIN,
                     "note": "Case-B substitutions (human stream shorter than HTR) "
                             "flagged when span-coverage/score/faithful falls below "
                             "threshold; re-derived from v1 HTR."},
        "totals": {"substitutions": total, "case_a": case_a, "case_b": case_b,
                   "risky": len(risky), "no_htr": no_htr, "empty_htr": empty_htr},
        "page_ids": [d["page_id"] for d in risky_sorted],
        "records": risky_sorted,
    }
    with open(JSON_OUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=1)
    print("wrote", JSON_OUT, "-", len(risky), "risky page_ids")


if __name__ == "__main__":
    main()
