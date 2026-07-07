# -*- coding: utf-8 -*-
"""Empirical HTR error profile: align MiDRASH HTR pages against FGP human
transcriptions of the same manuscripts.

Note: we compute LETTER-stream CER (space-free, normalized) — segmentation
errors are invisible by design, because the probe engine is space-free too.
FGP editorial conventions (expansions, bracket restorations) inflate apparent
CER somewhat -> treat results as an UPPER bound on effective letter noise.

Output: results/confusion_matrix.json + results/cer_report.txt
"""
import json
import sqlite3
import sys
from collections import Counter, defaultdict

sys.path.insert(0, r"C:\Genizahsearch\same_work_spike\probe\scripts")
from normalize import norm_stream  # noqa: E402
from rapidfuzz.fuzz import partial_ratio_alignment  # noqa: E402
from rapidfuzz.distance import Levenshtein  # noqa: E402

ROOT = r"C:\Genizahsearch"
PROBE_DB = ROOT + r"\same_work_spike\probe\data\probe.db"
FGP_DB = ROOT + r"\fgp_data\fgp_transcriptions.db"
OUT_JSON = ROOT + r"\same_work_spike\probe\results\confusion_matrix.json"
OUT_TXT = ROOT + r"\same_work_spike\probe\results\cer_report.txt"

MIN_SCORE = 65      # partial-ratio floor to accept an HTR<->FGP page match
MIN_LEN = 300       # min stream length on both sides

# --- load FGP human transcriptions for the fgp-bucket sys_ids ---
pcon = sqlite3.connect(PROBE_DB)
fgp_sys = [r[0] for r in pcon.execute(
    "SELECT DISTINCT sys_id FROM pages WHERE buckets LIKE '%fgp%'")]
htr_pages = defaultdict(list)  # sys_id -> [(page_id, stream)]
for pid, sid, text in pcon.execute(
        "SELECT page_id, sys_id, text FROM pages WHERE buckets LIKE '%fgp%'"):
    s, _ = norm_stream(text)
    if len(s) >= MIN_LEN:
        htr_pages[sid].append((pid, s))

fcon = sqlite3.connect(FGP_DB)
fgp_rows = defaultdict(list)  # sys_id -> [(row_id, stream)]
q = ("SELECT id, sys_id, content FROM fgp_transcriptions "
     f"WHERE sys_id IN ({','.join('?' * len(fgp_sys))}) AND content IS NOT NULL")
for rid, sid, content in fcon.execute(q, fgp_sys):
    s, _ = norm_stream(content)
    if len(s) >= MIN_LEN:
        fgp_rows[str(sid)].append((rid, s))

# --- match each HTR page to its best FGP transcription span ---
sub_counter = Counter()      # (ref_char, hyp_char) -> count
ins_counter = Counter()      # hyp char inserted
del_counter = Counter()      # ref char deleted
page_cers = []
n_matched = n_tried = 0
score_dist = Counter()

for sid, pages in htr_pages.items():
    frows = fgp_rows.get(sid, [])
    if not frows:
        continue
    for pid, htr_s in pages:
        n_tried += 1
        best = None
        for rid, fgp_s in frows:
            # find where the HTR page sits inside the (possibly longer) FGP text
            if len(htr_s) <= len(fgp_s):
                res = partial_ratio_alignment(htr_s, fgp_s)
                if res is None:
                    continue
                span_ref = fgp_s[res.dest_start:res.dest_end]
                span_hyp = htr_s
            else:
                res = partial_ratio_alignment(fgp_s, htr_s)
                if res is None:
                    continue
                span_ref = fgp_s
                span_hyp = htr_s[res.dest_start:res.dest_end]
            if best is None or res.score > best[0]:
                best = (res.score, span_ref, span_hyp)
        if best is None:
            continue
        score, span_ref, span_hyp = best
        score_dist[int(score // 10) * 10] += 1
        if score < MIN_SCORE or min(len(span_ref), len(span_hyp)) < MIN_LEN:
            continue
        n_matched += 1
        ops = Levenshtein.editops(span_ref, span_hyp)
        n_err = len(ops)
        cer = n_err / max(1, len(span_ref))
        page_cers.append((pid, sid, round(cer, 4), len(span_ref), score))
        for op in ops:
            if op.tag == 'replace':
                sub_counter[(span_ref[op.src_pos], span_hyp[op.dest_pos])] += 1
            elif op.tag == 'delete':
                del_counter[span_ref[op.src_pos]] += 1
            elif op.tag == 'insert':
                ins_counter[span_hyp[op.dest_pos]] += 1

# --- aggregate ---
page_cers.sort(key=lambda x: x[2])
cers = [c for _, _, c, _, _ in page_cers]


def pct(p):
    if not cers:
        return None
    return round(cers[min(len(cers) - 1, int(p * len(cers)))], 4)


total_ref = sum(ln for _, _, _, ln, _ in page_cers)
total_err = sum(c * ln for _, _, c, ln, _ in page_cers)
report = {
    'pages_tried': n_tried,
    'pages_matched': n_matched,
    'micro_cer': round(total_err / max(1, total_ref), 4),
    'median_cer': pct(0.5),
    'p25_cer': pct(0.25),
    'p75_cer': pct(0.75),
    'p90_cer': pct(0.90),
    'score_distribution': dict(sorted(score_dist.items())),
    'top_substitutions': [
        {'ref': a, 'hyp': b, 'count': c}
        for (a, b), c in sub_counter.most_common(30)],
    'top_deletions': [{'ref': a, 'count': c} for a, c in del_counter.most_common(10)],
    'top_insertions': [{'hyp': a, 'count': c} for a, c in ins_counter.most_common(10)],
}
json.dump({'report': report,
           'substitutions': [[a, b, c] for (a, b), c in sub_counter.most_common()],
           'page_cers': page_cers},
          open(OUT_JSON, 'w', encoding='utf-8'), ensure_ascii=False, indent=1)

lines = [
    f"HTR pages tried: {n_tried}, matched (score>={MIN_SCORE}, len>={MIN_LEN}): {n_matched}",
    f"micro CER (letters, space-free): {report['micro_cer']}",
    f"CER quartiles: p25={report['p25_cer']} median={report['median_cer']} "
    f"p75={report['p75_cer']} p90={report['p90_cer']}",
    f"match-score distribution (by decade): {report['score_distribution']}",
    "top substitutions (ref->hyp):",
]
for s in report['top_substitutions'][:20]:
    lines.append(f"  {s['ref']} -> {s['hyp']}: {s['count']}")
lines.append("top deletions: " + ", ".join(
    f"{d['ref']}:{d['count']}" for d in report['top_deletions']))
lines.append("top insertions: " + ", ".join(
    f"{d['hyp']}:{d['count']}" for d in report['top_insertions']))
open(OUT_TXT, 'w', encoding='utf-8').write("\n".join(lines))
print(f"matched {n_matched}/{n_tried}; micro-CER={report['micro_cer']} "
      f"median={report['median_cer']}")
print(f"report: {OUT_TXT}")
