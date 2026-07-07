# -*- coding: utf-8 -*-
"""Join Hillel's grades to engine pair metrics -> the calibration report.

Label semantics (Hillel, 2026-07-07):
- junk = microfilm opening-title sheets (same image, NOT part of the ms) ->
  stage-0 target-sheet filter class, removable.
- canonical (8) = quotation embedded in a DIFFERENT work; two Bible MSS of
  the same passage are graded same_text (both ARE witnesses of the work).
- same_text is judged at the UNIT level: a siddur's Birkat Hamazon vs a
  Haggadah's Birkat Hamazon = same_text (shared unit, different containers).
"""
import json
import sys
from collections import Counter, defaultdict

GRADES_FILE = sys.argv[1] if len(sys.argv) > 1 else \
    r"C:\Users\gersh\Downloads\seed029_grades (1).json"
ROOT = r"C:\Genizahsearch"
ENGINE = ROOT + r"\same_work_spike\probe\results\verified_pairs_d50_cap1.json"
REVIEW = ROOT + r"\same_work_spike\probe\review\review_data.json"
OUT = ROOT + r"\same_work_spike\probe\results\grades_analysis.md"

grades = json.load(open(GRADES_FILE, encoding='utf-8'))
engine = json.load(open(ENGINE, encoding='utf-8'))
review = json.load(open(REVIEW, encoding='utf-8'))

eng_by_key = {frozenset((p['a'], p['b'])): p for p in engine}
rev_by_id = {r['id']: r for r in review}

REAL = {'same_text', 'verbatim', 'near_verbatim', 'paraphrase',
        'shared_formula', 'canonical'}
SPURIOUS = {'topical', 'unrelated'}


def norm_grade(g):
    return 'same_text' if g in ('verbatim', 'near_verbatim') else g


rows = []
for g in grades:
    a, b = g['id'].split('|')
    e = eng_by_key.get(frozenset((a, b)))
    r = rev_by_id.get(g['id'], {})
    rows.append({
        'grade': norm_grade(g['grade']),
        'stratum': g['stratum'],
        'eng_density': e['density'] if e else None,
        'eng_len': e['len'] if e else None,
        'dup_flag': int(r.get('dup_lines', 0) >= 0.6 or r.get('dup_shelf', 0)),
    })

lines = [f"# Grades analysis — {len(rows)} graded pairs (2026-07-07)", ""]

# overall
c = Counter(r['grade'] for r in rows)
n = len(rows)
real = sum(v for k, v in c.items() if k in REAL)
spur = sum(v for k, v in c.items() if k in SPURIOUS)
lines += [
    "## Overall",
    f"- grades: {dict(c)}",
    f"- REAL shared text (same/paraphrase/formula/canonical): "
    f"**{real}/{n} = {100*real/n:.1f}%**",
    f"- duplicate photography: {c.get('duplicate_photo',0)} "
    f"({100*c.get('duplicate_photo',0)/n:.1f}%) — removed by stage-0",
    f"- junk (microfilm title sheets): {c.get('junk',0)} — removed by stage-0",
    f"- ACTUALLY SPURIOUS: **{spur}/{n} = {100*spur/n:.1f}%**",
    f"- precision after stage-0 removes dup+junk: "
    f"**{real}/{real+spur} = {100*real/max(1,real+spur):.1f}%**",
    "",
]

# per stratum
lines.append("## Per stratum")
for st in sorted({r['stratum'] for r in rows}):
    sub = [r for r in rows if r['stratum'] == st]
    cc = Counter(r['grade'] for r in sub)
    lines.append(f"- **{st}** (n={len(sub)}): {dict(cc)}")
lines.append("")

# per engine-density band (pairs with engine join)
lines.append("## Per ENGINE density band (excl. dup/junk — post-stage-0 view)")
BANDS = [(0.0, 0.30), (0.30, 0.35), (0.35, 0.40), (0.40, 0.45), (0.45, 0.51)]
for lo, hi in BANDS:
    sub = [r for r in rows
           if r['eng_density'] is not None and lo <= r['eng_density'] < hi
           and r['grade'] not in ('duplicate_photo', 'junk')]
    if not sub:
        continue
    cc = Counter(r['grade'] for r in sub)
    rr = sum(v for k, v in cc.items() if k in REAL)
    lines.append(f"- density [{lo:.2f},{hi:.2f}): n={len(sub)}, "
                 f"real={rr} ({100*rr/len(sub):.0f}%), detail={dict(cc)}")
lines.append("")

# discovery stratum detail
disc = [r for r in rows if r['stratum'] == 'discovery']
cc = Counter(r['grade'] for r in disc)
lines += [
    "## Discovery stratum (the headline capability)",
    f"- n={len(disc)}: {dict(cc)}",
    f"- REAL discoveries (same_text, not dup/junk): "
    f"**{cc.get('same_text',0)}**",
    "",
]

# detector agreement
dup_graded = [r for r in rows if r['grade'] == 'duplicate_photo']
det_hits = sum(1 for r in dup_graded if r['dup_flag'])
flagged = [r for r in rows if r['dup_flag']]
flag_correct = sum(1 for r in flagged if r['grade'] == 'duplicate_photo')
lines += [
    "## Line-agreement detector vs human duplicate grades",
    f"- human graded duplicate_photo: {len(dup_graded)}; "
    f"detector flagged {det_hits} of them "
    f"(recall {100*det_hits/max(1,len(dup_graded)):.0f}%)",
    f"- detector flagged {len(flagged)} graded items; "
    f"{flag_correct} are human-confirmed duplicates "
    f"(precision {100*flag_correct/max(1,len(flagged)):.0f}%)",
    "",
]

# label semantics record
lines += [
    "## Label semantics (Hillel's policy, binding for future annotation)",
    "1. `junk` = microfilm opening-title sheets — identical images, not part",
    "   of the manuscript; stage-0 filter class.",
    "2. `canonical` = quotation embedded in a DIFFERENT work. Two Bible MSS",
    "   of the same passage = `same_text` (both are witnesses of the work).",
    "3. `same_text` is judged at the UNIT level: siddur-BH vs Haggadah-BH =",
    "   same_text — the shared liturgical unit is the atom, not the",
    "   codicological container. (=> same-work clustering must cluster",
    "   UNITS, not manuscripts.)",
]

open(OUT, 'w', encoding='utf-8').write("\n".join(lines))
print("\n".join(lines))
