# V0.8 cross-manuscript misattribution — corpus-wide scan (2026-08-14)

Follow-up to SEED-033's finding #1, which recorded one case (`Ms. P. Heid. Hebr. 18`
holding `p. Heid. Hebr. 19`'s folio text, appended twice) and asked whether the
concatenate-and-duplicate pattern recurs corpus-wide. It does — **five confirmed
instances, and they share a mechanism.**

Scripts (gitignored, re-runnable): `_tmp/v08_repeat_scan.py`, `_tmp/v08_attribute.py`,
`_tmp/v07_only_export.py`, `_tmp/v07_only_profile.py`. Results: `_tmp/v08_repeats.json`,
`_tmp/v08_attribution.json`, `_tmp/v07_only_sysids.json`, `_tmp/v07_only_profile.json`.

## Why the first design failed, and what replaced it

A Codex pre-flight killed the original plan by measuring it: 119 token-aligned exact
phrase windows of 35–50 characters from the victim's V0.7 page produced **zero** V0.8
matches, and the longest exact token run shared by victim and host is **3 tokens**.

The reason is structural. The text sitting inside Hebr. 18's record is a **V0.8**
transcription of Hebr. 19's folio; Hebr. 19's own surviving record is a **V0.7** OCR pass
of the same leaf. Two transcriptions of one page, so exact matching cannot bridge them.
Base-Hebrew **token-set overlap is 0.475** on the same pair. Seen side by side:

```
host V0.8 block : … או' טמאה ואו' מתלין או' טהור הה[ … שמו יותננת ל על פי הגיר[ …
victim V0.7     : … או טמאלה ואו מלין  או' טהורהה   … שמו יותננת ל על פי הניר   …
```

So detection is exact (repetition inside one record) and attribution is fuzzy
(IDF-weighted token overlap against the victim pool).

## Sweep A — repetition, over the whole V0.8 corpus

`Transcriptions.txt` streamed directly rather than the index: it is what the index is
built from, costs 130 s instead of 941 k stored-document retrievals, and takes no lock.

| | |
|---|---|
| records scanned | **948,549** (index holds 941,026 V0.8 page docs — 7,523 fewer; unexplained, worth a look) |
| flagged: non-overlapping repeat ≥20 base-Hebrew tokens | **160 (0.017%)** |
| of those, **back-to-back** (gap == repeat length) | **13** — the concatenation shape |
| scattered repeats | 147 |

The 0.017% flag rate over the entire population *is* the control false-positive rate,
measured rather than sampled.

**Gate proven able to fail** (`feedback_gates_must_be_proven_able_to_fail`): the known
host fires at exactly 46 tokens / 211 chars with the reported phrase `עצים עליו למודה`
inside the repeated block; at N=47 it goes dark; at N=46 it fires. Its two sibling
records (the 7-token stub, the recto) stay silent.

## Attribution — IDF-weighted, calibrated here

Victim pool = the 15,539 V0.7-only manuscripts (2,983 pages of ≥25 tokens).

Plain `|A∩B| / min(|A|,|B|)` — the `_content_similarity` shape — **degenerated exactly as
the review predicted**: it scored 1.000 for dozens of unrelated records against one
manuscript, `990053386640205171`, which turns out to be **Numbers 7**, the twelve tribal
offerings — a small stock vocabulary repeated twelve times. Shared words are now weighted
by rarity across the victim corpus. Null distribution (block vs random victim page):
**p50 0.0, p95 0.042, p99 0.078**.

### Confirmed pairs — every one names a *neighbouring* system number

| idf | rare shared | cov | host | victim |
|---:|---:|---:|---|---|
| 0.765 | 45 | 0.95 | Strasbourg `Ms. 4038` (`990026373060205171`) | `990026373340205171` |
| 0.764 | 23 | 0.63 | BL `Gaster Ms. 1356` (`990001990000205171`) | `990001990020205171` |
| 0.396 | 9 | 0.93 | Heidelberg `Ms. P. Heid. Hebr. 18` (`990043939960205171`) | **`p. Heid. Hebr. 19`** — the reported case |
| 0.251 | 11 | 0.67 | BL `Halpern, Joseph 41 \| 42 \| 43` (`990026139540205171`) | `990026139630205171` |
| 0.232 | 6 | 0.69 | AIU `Ms. III A 22` (`990001504930205171`) | **`III.A.23`** |

All five sit 3–10× above the null p99. The two pairs where both sides carry a catalogue
row have **consecutive shelfmarks** — Hebr. 18 → 19, III A 22 → III.A.23 — and the AIU
pair was verified by reading both texts (above).

The remaining 6 back-to-back records score ≤0.17 with ≤3 rare shared tokens and point at
unrelated manuscripts; treat them as **unattributed**, not as clean. Their victim most
likely has its own V0.8 record, which puts it outside this victim pool by construction.
2 of the 13 were not scored at all (victim-pool token floor).

## Mechanism

- **7 of the 13 back-to-back hosts are multi-IE sys_ids**, against a base rate of
  3,193 / 232,450 = **1.4%** — roughly a 39× enrichment. Among all 160 flagged records
  only 14 of 108 hosts are multi-IE, so the enrichment is specific to the
  concatenation shape, not to repetition generally.
- Hosts are frequently multi-shelfmark records (`Heid. Arab. 1443 | Hebr. 18`,
  `Halpern 41 | 42 | 43`).
- Every confirmed victim is the neighbouring catalogue record.

Reading: this looks like an **ingest boundary error** — where one system number bundles
several physical items, the next item's folio text is appended to the previous record
instead of starting its own, and duplicated in the process. Not an app bug; not fixable
in application code.

## The 147 scattered flags are not defects

Their top match is overwhelmingly the Numbers 7 page. These are canonical texts that
genuinely repeat themselves; the "match" means *another witness of the same well-known
passage*, not a misattribution. They should not be counted in any defect total.

## This is a lower bound, by construction

Named shapes the scan cannot see:
- the victim **has** its own V0.8 record (outside the V0.7-only victim pool);
- the foreign text was appended **once**, with no repetition (invisible to Sweep A);
- **replacement** rather than concatenation;
- a victim absent from **both** indexed versions.

Report as "five confirmed instances of one shape", never as "the corpus contains five
misattributions".

## Side finding — the V0.7-only population is mostly blank

SEED-033 reported ~6.2% of manuscripts as V0.7-only and therefore "missing from
production search". The count is now exact (15,539 / 6.68%), but **how much is actually
lost is far smaller than the count suggests**:

| V0.7 tokens | manuscripts | share |
|---|---:|---:|
| 0 | 4,402 | 28.3% |
| 1–9 | 6,486 | 41.7% |
| 10–49 | 2,955 | 19.0% |
| 50–199 | 1,264 | 8.1% |
| 200–999 | 418 | 2.7% |
| 1000+ | 14 | 0.1% |

**70% carry under 10 base-Hebrew tokens**; only **1,696 (10.9%) carry ≥50**. The whole
V0.7-only population totals 372,993 tokens. All 15,539 do have a V0.7 record — none is
missing from the file. So the honest framing is *~1,700 manuscripts with substantive text
absent from a V0.8-only index*, not 9,000–20,000 manuscripts of lost content.

By collection the population is concentrated: CUL 7,757 and JTS 5,910 are 88% of it;
Heidelberg contributes 4.

## Still open

- Whether production really is V0.8-only (unverified from a dev box — check the prod box
  for `AllGenizah_OLD.txt`, and for the older name `Genizah_OLD.txt` the deployment guide
  uses, or count `source:"V0.7"` against the production index).
- The 7,523-record gap between the corpus file (948,549) and the index (941,026).
- Whether the 6 unattributed back-to-back records have victims that carry their own V0.8.
