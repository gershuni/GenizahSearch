# Stage-0 substitution-gate audit — `scripts/mapv2_stage0.py`

- Auditor: agent (static + empirical rapidfuzz 3.14.5 probing; DB never opened)
- Date: 2026-07-10
- Target run: `results/stage0_report.md` (18,982 subs = 14,932 FGP + 4,050 PGP of 667,411 pages; 5,419 windows cropped)
- Directive under test (Hillel): *"we should not skip the HTR unless it's clear the human copy is just a better copy"* — a **partial** human draft must **never** replace **fuller** HTR.

## VERDICT: **GATE-LEAKY**

The gate is structurally on the right track — it *does* split on the asymmetry (Finding 8), rejects tiny drafts (MIN_LEN), and rejects gross non-matches (score + gram prefilter). But two independent legs (span-coverage and partial-ratio score) sit at thresholds that are **not jointly sufficient**: in a realistic scenario a partial/divergent human draft can replace **100% of the HTR while preserving as little as ~57% of the page's true content**, and interior-skip drafts (middle 20% missing) pass. The per-substitution coverage that would let us *see* how many real subs live in that band was **never recorded** (Finding 3), so today we can only bound the floor at the **2,642 substitutions in the 70–79 score decade (13.9%)**. Recommend the F-SQL-1 recompute before trusting `fullcorpus_v2.db` downstream.

---

## Findings

### F1 — Span-coverage × score threshold are not jointly sufficient → partial/divergent draft replaces fuller HTR   **[BLOCKER]**
**Code path:** `gate_candidate`, shorter-transcription branch, lines 276–283:
```python
res = partial_ratio_alignment(tr_stream, page_stream, score_cutoff=MIN_SCORE)  # 70
if res is None or res.score < MIN_SCORE: return "low_score"
coverage = (res.dest_end - res.dest_start) / len(page_stream)   # SPAN, not matched content
if coverage < COVER_MIN: return "partial_coverage"              # 0.80
return (res.score, tr_raw, False)
```
Two problems compound:
1. **`coverage` is the span from first-aligned char to last-aligned char**, not the count of characters that actually matched. An interior gap counts as "covered."
2. **score and coverage are checked independently**, so both can sit at their floors at once.

**Empirical (rapidfuzz 3.14.5, non-periodic Hebrew, this machine):**

| human draft shape | len(tr)/len(page) | score | coverage | verdict | true faithful fraction of page |
|---|---|---|---|---|---|
| transcribe head 40% + tail 40%, **skip middle 20%** | 0.80 | **75.0** | **0.80** | **PASS** | 0.80 (but middle 20% lost + false adjacency, see F5) |
| cover 82% span, **30% of covered chars diverge** | 0.82 | **72.0** | **0.819** | **PASS** | **0.574** |
| cover 82% span, 25% diverge | 0.82 | 76.6 | 0.819 | PASS | 0.616 |

So a substitution can pass at `score≈72, coverage≈0.82` while only ~57% of the page's real content is faithfully represented — and 100% of the HTR is discarded. This is exactly the failure Hillel warned about.

**Fix (any one, ideally all):**
- For the Case-B (shorter-tr) branch, raise the floors and/or **couple them**: require `coverage >= 0.90` **and** `score >= 80`, or require the product `coverage * score/100 >= 0.75`.
- Replace span-coverage with **matched-character coverage**: sum the lengths of the `equal` blocks from `rapidfuzz.distance.Indel.editops(tr_stream, page_stream)` (or `Levenshtein.matching_blocks`) and divide by `len(page_stream)`. This makes interior gaps and internal divergence both count against coverage.
- Reject when `editops` reveal a single interior deletion run longer than ~10% of the page (interior-skip guard).

**Post-hoc:** F-SQL-1 recomputes true coverage per substitution from the untouched v1 HTR.

---

### F2 — COVER_MIN=0.80 discards up to 20% of page content on every Case-B substitution   **[HIGH]**
**Code path:** `COVER_MIN = 0.80` (line 94) used at line 281.
Head-only sweep (this machine): a draft covering the **first 80%** of the page and stopping **passes 40/40**; first 75% fails 0/40. So dropping the last 20% of a page is accepted by design. Combined with the 70–79 score decade = **2,642 subs (13.9% of 18,982)** where internal divergence is highest, this is the largest *quantifiable* slice of policy-tolerated loss. For short pages 20% can be several whole lines ("80% of little is little": pages with HTR stream just over MIN_LEN=200 can lose ~40 letters and still pass).
**Fix:** raise `COVER_MIN` (0.90 suggested) and make it length-scaled — stricter for pages under, say, 600 stream letters. Note MIN_LEN=200 already blocks *sub-200* HTR pages from being substituted at all (they are marked `short`), so the "little" case is bounded to the 200–~600 band; still worth tightening.

---

### F3 — Per-substitution coverage is never recorded → the partial-draft risk band is invisible in the artifacts   **[HIGH]**
**Code path:** `write_report` (lines 376–461) dumps only `score_decades`; `create_schema` (lines 150–161) has columns `fgp_score`, `n_chars`, `htr_n_chars` but **no coverage, no case-A/B flag, no cropped flag**. The one number the scholar explicitly asked to see — how many subs sit near coverage 0.80 — is not in the DB or the report. (I could quantify the *score* band from the report: 70–79 = 2,642; 80–89 = 6,731; 90–99 = 9,598; 100 = 11. I could **not** quantify "within 0.05 of coverage 0.80" from any static artifact — it does not exist.)
**Fix:** add `coverage REAL`, `case CHAR(1)` ('A'/'B'), `cropped INTEGER` to the `pages` table (or a `stage0_sub_audit` side table keyed by page_id), and emit a coverage histogram + a "risk band" line (score∈[70,75) ∩ coverage∈[0.80,0.85)) in the report.

---

### F4 — Case-A (longer-tr) branch has no coverage check: apparatus injection + parallel-witness substitution   **[MEDIUM]**
**Code path:** `gate_candidate` lines 257–273 (`len(page_stream) <= len(tr_stream)`). No coverage leg here — a longer stream is *assumed* fuller/safe.
- If ratio ≤ WINDOW_RATIO (1.3) the **full `tr_raw` is stored** (line 273). A partial draft padded with **Hebrew editorial apparatus / variae lectiones / commentary** (which survives `norm_stream`, since only nikud/punct/Latin are stripped) is longer than the HTR page and injects non-page text as page content — phantom shingles for the downstream reuse detector.
- At score≈70 in this branch, up to ~30% of the HTR page may be unmatched; when the "transcription" is actually a **PGP parallel/copy** (intended per the module docstring line 16), storing its window replaces the page with a related-but-different witness, losing up to ~30% of the page's own content.
**Fix:** apply a matched-coverage check in Case A too (page-content coverage of the stored window), and/or when `ratio ∈ (1.0, 1.3]` prefer the cropped aligned window over the full `tr_raw` to strip apparatus.

---

### F5 — Interior-gap false adjacency in stored text   **[MEDIUM]**
**Code path:** Case-B pass returns `tr_raw` unmodified (line 283); Case-A non-cropped returns full `tr_raw` (line 273). When a passing draft skips an interior chunk (F1 shows middle-20% skips pass), the stored raw is `head‖tail` with **no gap marker**, creating a spurious bigram/shingle spanning the deleted region. This is a shared-passage/n-gram mapping pipeline (FRAG2), so a phantom adjacency can manufacture a false reuse edge.
**Fix:** the editops-based interior-gap guard from F1 also removes this; alternatively insert a sentinel separator at detected gaps so shingles can't cross them.

---

### F6 — Greedy 1:1 by raw score can misattribute a transcription to the wrong folio   **[MEDIUM]**
**Code path:** `substitute_sys` lines 342–349: `passing.sort(key=lambda t: (-t[0], t[1], t[2]))` then first-come 1:1. A short/easy page that scores 95 against candidate C steals it from the correct fuller page that scored 90; the correct page is left un-upgraded (`lost_greedy` = 2,763 pages) and the short page carries a sibling folio's transcription. Every accepted sub still passed the gate for its page, so this is a **mapping-correctness** issue, not raw content loss — but it degrades attribution.
**Fix:** tie-break/penalize by length-ratio closeness (`abs(1 - len(tr)/len(page))`), or replace greedy with a max-weight (Hungarian) assignment over the passing pairs within a sys_id.

---

### F7 — DB alone cannot separate safe (Case A) from dangerous (Case B) subs; multi-column fails safe   **[LOW]**
`htr_n_chars` is a **raw** char count (not stream length) and there is **no per-row cropped/case flag** (F3), so `fullcorpus_v2.db` alone cannot isolate the Case-B subset — the audit must re-derive from v1 (F-SQL-1). Separately: **multi-column** pages where HTR and human differ in column reading order produce a permuted stream → `partial_ratio` scores low → `low_score` → HTR kept. That is fail-safe (a missed upgrade, not a loss), so no leak there; noted for completeness.

---

## Asymmetry question (task #3) — does the gate distinguish FGP-longer from FGP-shorter?
**Yes, structurally.** The branch `if len(page_stream) <= len(tr_stream)` (line 257) is the discriminator: **longer/equal tr → Case A** (assumed fuller, no coverage check); **shorter tr → Case B** (partial-draft danger, coverage gate applied). The field that tells them apart is the **norm-stream length comparison** (`len(tr_stream)` vs `len(page_stream)`); in the output DB the raw proxy is `n_chars` vs `htr_n_chars`. So the gate *does* aim the coverage guard at the dangerous direction. The leaks are: (a) the Case-B guard itself is too weak/independent (F1, F2); (b) Case A is assumed safe but a longer draft can still be partial-with-apparatus or a divergent parallel (F4); (c) neither the length comparison nor coverage is persisted (F3/F7).

Aggregate reassurance is **insufficient**: the report shows substituted pages gained stream letters overall (HTR 12,284,727 → tr 12,676,340, +391,613 ≈ +3.2%), i.e. Case A dominates — but that SUM hides the Case-B subset where the human text is shorter. Only F-SQL-1 can count and grade that subset.

---

## Post-hoc checks to run once the machine/DB is free (do NOT run now — writer owns the DB)

### F-SQL-1 — PRIMARY / definitive: recompute TRUE coverage from untouched v1 HTR
> This is the single most important check. `fullcorpus.db` (v1) still holds the original HTR keyed by `page_id`; `fullcorpus_v2.db` holds the substituted text. Recompute the real content coverage per substitution. Run both connections **read-only**.
```python
# scripts/audit_stage0_coverage.py  (read-only both DBs; run when writer is done)
import sqlite3, sys
sys.path.insert(0, r"C:\Genizahsearch\same_work_spike\probe\scripts")
from normalize import norm_stream
from rapidfuzz.fuzz import partial_ratio_alignment as pra
V1 = r"C:\Genizahsearch\same_work_spike\probe\data\fullcorpus.db"
V2 = r"C:\Genizahsearch\same_work_spike\probe\data\fullcorpus_v2.db"
v1 = sqlite3.connect(f"file:{V1}?mode=ro", uri=True)
v2 = sqlite3.connect(f"file:{V2}?mode=ro", uri=True)
rows = v2.execute("SELECT page_id, provenance, fgp_score, text "
                  "FROM pages WHERE provenance!='htr'").fetchall()
shorter = leak = 0; band = []
for pid, prov, score, human in rows:
    h = v1.execute("SELECT text FROM pages WHERE page_id=?", (pid,)).fetchone()
    if not h:
        continue
    sp, _ = norm_stream(h[0] or "")        # original HTR stream
    sh, _ = norm_stream(human or "")       # stored human stream
    if len(sp) == 0:
        continue
    if len(sh) < len(sp):                  # Case-B (dangerous direction)
        shorter += 1
        r = pra(sh, sp)                     # shorter first: dest = HTR page
        cov = (r.dest_end - r.dest_start) / len(sp) if r else 0.0
        true_faithful = cov * (r.score / 100.0) if r else 0.0
        if (r is None) or cov < 0.85 or r.score < 75 or true_faithful < 0.75:
            leak += 1
            band.append((pid, prov, round(score,1),
                         len(sp), len(sh), round(cov,3), round(true_faithful,3)))
print(f"substitutions total: {len(rows)}")
print(f"human SHORTER than HTR (Case-B): {shorter}")
print(f"risk band (cov<0.85 or score<75 or faithful<0.75): {leak}")
band.sort(key=lambda t: t[6])
print("worst 50 (page_id, src, stored_score, htr_len, human_len, TRUE_cov, faithful):")
for b in band[:50]:
    print(b)
```

### F-SQL-2 — quick length-shrinkage buckets (pure SQL, seconds)
```sql
SELECT
  CASE
    WHEN n_chars <  0.6*htr_n_chars THEN 'a <0.6'
    WHEN n_chars <  0.7*htr_n_chars THEN 'b 0.6-0.7'
    WHEN n_chars <  0.8*htr_n_chars THEN 'c 0.7-0.8'
    WHEN n_chars <  0.9*htr_n_chars THEN 'd 0.8-0.9'
    WHEN n_chars <  1.0*htr_n_chars THEN 'e 0.9-1.0'
    ELSE 'f >=1.0' END                          AS lenbucket,
  COUNT(*)                                       AS n,
  ROUND(AVG(fgp_score),1)                        AS avg_score,
  MIN(fgp_score)                                 AS min_score
FROM pages WHERE provenance!='htr'
GROUP BY lenbucket ORDER BY lenbucket;
-- buckets a-c = human text >20% shorter than HTR = the partial-draft danger set.
```

### F-SQL-3 — near-threshold risk band + the highest-risk rows
```sql
SELECT provenance,
  SUM(fgp_score < 72)                                   AS score_70_72,
  SUM(fgp_score < 75)                                   AS score_lt75,
  SUM(n_chars < 0.8*htr_n_chars)                        AS shrunk_gt20pct,
  SUM(n_chars < 0.8*htr_n_chars AND fgp_score < 80)     AS shrunk_and_lowscore
FROM pages WHERE provenance!='htr' GROUP BY provenance;

-- eyeball worst offenders:
SELECT page_id, sys_id, provenance, fgp_id, fgp_score,
       n_chars, htr_n_chars, ROUND(n_chars*1.0/htr_n_chars,3) AS lenratio
FROM pages
WHERE provenance!='htr' AND n_chars < 0.8*htr_n_chars
ORDER BY fgp_score ASC, lenratio ASC
LIMIT 200;
```

**Single most important check to run tomorrow: F-SQL-1** — it is the only one that recovers the *true* content coverage (span × internal fidelity) each substitution actually achieved, by re-aligning the stored human text against the original HTR in v1. Everything else (score band, length shrinkage) is a proxy.
