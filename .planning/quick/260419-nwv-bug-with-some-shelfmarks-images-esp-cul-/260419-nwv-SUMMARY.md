---
id: 260419-nwv
type: quick-fix
status: partial-fix
completed: 2026-04-19
files_modified:
  - scripts/debug_ts_ns_158_112_image_alignment.py
  - shared/nli_crossref_service.py
  - tests/test_nli_crossref_service.py
  - docs/OPEN_ISSUES.md
commits:
  - "0825437c: feat(260419-nwv): add diagnostic for paired-leaf CUL image-text mis-alignment"
  - "17b8e9bf: fix(260419-nwv): parse_folio_label handles paired-leaf bifolio ImageNames"
decisions:
  - "Paired-leaf notation L{first}_{second}F... treats the first number as the primary leaf (conservation page-turning order). Matches the plan's recommendation; diagnostic evidence for this specific sys_id matches the assumption because the Transcriptions.txt order and CUDL canvas labels both walk the folio sequence 1r,1v,2r,2v,..."
  - "Did NOT rewrite web/api.py cambridge_image handler — the positional CUDL canvas mismatch is a larger change tracked as a separate follow-up in OPEN_ISSUES.md."
---

# Quick Fix 260419-nwv — Paired-leaf CUL image/text mis-alignment

## One-liner

Fixed the `parse_folio_label()` regex so paired-leaf / bifolio NLI ImageNames
(e.g. `T_S_NS_158_112__L1_12F0B0S1`) produce correct folio labels and sort
by primary leaf number; shipped a diagnostic script that confirmed both the
regex bug (H2) and an additional positional CUDL canvas mismatch (H1) plus
an NLI manifest IE-suffix drift (H3) that are tracked as follow-ups.

## Bug

On T-S NS 158.112 (sys_id 990051537270205171, CUL) — and presumably other
CUL manuscripts with paired-leaf/bifolio scans — the image shown for a
given transcription page did not match the text. The user's original
report was "images don't fit the text on some shelfmarks, especially CUL".

## Diagnostic Run (Task 1)

Script: `scripts/debug_ts_ns_158_112_image_alignment.py` (committed as
`0825437c`). Run output for `sys_id=990051537270205171`:

```
=== Bug 260419-nwv diagnostic for sys_id=990051537270205171 ===

Transcriptions.txt: 14 entries
  P0001 FL167150424
  P0002 FL167150425
  P0003 FL167150426
  P0004 FL167150427
  P0005 FL167150428
  ... (+9 more)

INFO: Cambridge manifest lookup succeeded for normalized_shelfmark='tsns158.112'
nli_crossref.db nli_images: 14 rows
Cambridge manifest URL: http://cudl.lib.cam.ac.uk/iiif/MS-TS-NS-00158-00112

NLI IIIF manifest canvas count: 14
CUDL manifest canvas count: 12

ALIGNMENT TABLE
----------------------------------------
p_num  trans_FL   nli_manifest_FL  cudl_label  nli_images.ImageName         parse_folio_label()
-----  ---------  ---------------  ----------  ---------------------------  -------------------
1      167150424  167150439        1r          T_S_NS_158_112__L1_12F0B0S1  ''
2      167150425  167150440        1v          T_S_NS_158_112__L1_12F0B0S2  ''
3      167150426  167150441        2r          T_S_NS_158_112__L2_11F0B0S1  ''
4      167150427  167150442        2v          T_S_NS_158_112__L2_11F0B0S2  ''
5      167150428  167150443        3r          T_S_NS_158_112__L3_10F0B0S1  ''
6      167150429  167150444        3v          T_S_NS_158_112__L3_10F0B0S2  ''
7      167150430  167150445        4r          T_S_NS_158_112__L4_9F0B0S1   ''
8      167150431  167150446        4v          T_S_NS_158_112__L4_9F0B0S2   ''
9      167150432  167150447        5r          T_S_NS_158_112__L5F0B0S1     '5r'
10     167150433  167150448        5v          T_S_NS_158_112__L5F0B0S2     '5v'
11     167150434  167150449        6r          T_S_NS_158_112__L6_7F0B0S1   ''
12     167150435  167150450        6v          T_S_NS_158_112__L6_7F0B0S2   ''
13     167150436  167150451                    T_S_NS_158_112__L8F0B0S1     '8r'
14     167150437  167150452                    T_S_NS_158_112__L8F0B0S2     '8v'

ALIGNMENT VERDICTS
----------------------------------------
text<->NLI MISALIGNED - 14 FL id mismatches in position order (first: p_num=1 trans=FL167150424 vs nli=FL167150439)
text<->CUDL COUNT MISMATCH - CUDL has 12 canvases but transcription has 14 pages. This means positional /api/cambridge_image/{sys_id}?page={p-1} WILL return the wrong image at least some of the time. Non-folio-like CUDL labels: [] (+0 more)
parse_folio_label BROKEN (H2 CONFIRMED) - 10/14 ImageNames return empty string. Examples: ['T_S_NS_158_112__L1_12F0B0S1', 'T_S_NS_158_112__L1_12F0B0S2', 'T_S_NS_158_112__L2_11F0B0S1']

(diagnostic completed in 7.1s)
```

## Verdict on hypotheses

| Hypothesis | Claim | Verdict | Action taken |
|---|---|---|---|
| **H1** | Positional CUDL canvas indexing serves the wrong image because CUDL canvas count != transcription count / ordering | **CONFIRMED** — CUDL has 12 canvases, transcription has 14 pages; last two pages (p13, p14) have no canvas at the positional index and the off-by-one also tilts wherever CUDL inserts cover/binding canvases before the folio sequence | **Deferred** — tracked in `docs/OPEN_ISSUES.md` as a separate P2 Open entry with the diagnostic evidence. Requires mapping CUDL canvas labels to FL IDs and routing `/api/cambridge_image/{sys_id}` by FL ID instead of positional index. Out of quick-fix scope. |
| **H2** | `parse_folio_label()` regex does not handle paired-leaf / bifolio notation `L{first}_{second}F...` | **CONFIRMED** — 10 of 14 ImageNames returned empty folio label; all 10 rows had sort key `(999999, 0)` so they fell through to alphabetical ImageName order | **FIXED** in commit `17b8e9bf`. `_FOLIO_PATTERN` now accepts optional `_{second_leaf}` between the primary leaf and `F`. 8 new `TestParseFolioLabelPairedLeaf` cases + 1 sort-order regression test, all 77 tests in `tests/test_nli_crossref_service.py` pass. |
| **H3** | NLI IIIF manifest canvas order differs from Transcriptions.txt FL id order for this sys_id | **OBSERVED** (unexpected surprise) — NLI manifest returned FL167150439..452 but Transcriptions.txt references FL167150424..437. Looks like a different IE ordinal / suffix mismatch: this CUL shelfmark has more than one IE and `fetch_fl_ids_from_nli(..., suffix=1)` is resolving the wrong IE. | **Deferred / escalated** — same follow-up OPEN_ISSUES entry as H1. Needs an IE-selection audit for CUL. The primary-IE map at `primary_ie_map.json` (untracked, from v7.7.0 multi-IE work) may be the intended correction path. |

## Fix summary (H2 only)

**Files changed:**

- `shared/nli_crossref_service.py` — `_FOLIO_PATTERN` regex relaxed from
  `r'L(\d+)F\d+B\d+S(\d+)'` to `r'L(\d+)(?:_\d+)?F\d+B\d+S(\d+)'`. Docstring
  updated with a paired-leaf example.
- `tests/test_nli_crossref_service.py` — added `TestParseFolioLabelPairedLeaf`
  (8 cases) + `test_get_folio_images_sorts_paired_leaf_by_leaf_number`
  (regression fixture using a temp SQLite DB).

**User-perceptible change:** folio labels for T-S NS 158.112 (and every
other paired-leaf CUL shelfmark routed through `get_folio_images()`) will
now render as `1r, 1v, 2r, 2v, ...` instead of the sequential fallback
`1, 2, 3, ...`. Sort order also becomes leaf-number-correct instead of
alphabetical by ImageName.

## Follow-up tracking

`docs/OPEN_ISSUES.md` updated:

1. Added P2 entry **"Paired-leaf / bifolio folio labels empty on CUL
   shelfmarks (e.g. T-S NS 158.112)"** with status
   `🟡 Partially Fixed (2026-04-19)` — folio labels fixed, positional
   mismatch separate.
2. Added P2 entry **"CUL positional image mismatch:
   `/api/cambridge_image/{sys_id}?page={p-1}` indexes a CUDL canvas list
   whose length does not match transcription page count"** with status
   `❌ Open` and the full diagnostic evidence, so the larger fix path
   (canvas-label → FL ID mapping, NLI IE-suffix audit) is not lost.
3. Updated "Last Updated" to 2026-04-19 and Quick Summary counts
   (P2 Open 17 → 18, Fixed 59 → 60, Total 28 → 29).

## Commits

| # | Hash | Type | Message |
|---|---|---|---|
| 1 | `0825437c` | feat | add diagnostic for paired-leaf CUL image-text mis-alignment |
| 2 | `17b8e9bf` | fix | parse_folio_label handles paired-leaf bifolio ImageNames |
| 3 | _pending_ | docs | update OPEN_ISSUES.md with 260419-nwv findings |

## Deviations from plan

### Auto-documented issues (Rule 2 — additional finding during diagnosis)

**1. [Rule 2 - Missing tracking] H3 (NLI IE-suffix mismatch) surfaced as a
genuinely new signal, not in the plan's hypotheses list as something to
act on**

- **Found during:** Task 1 diagnostic run
- **Issue:** The plan called H3 ("NLI manifest FL order differs from
  transcription order") "more serious than expected, escalate in
  SUMMARY.md". The diagnostic confirmed this for T-S NS 158.112 and the
  most likely cause is `fetch_fl_ids_from_nli(..., suffix=1)` resolving
  the wrong IE ordinal, which is a known multi-IE concern from v7.7.0.
- **Handling:** Documented in the follow-up OPEN_ISSUES entry alongside
  H1 so the next CUL image investigation starts with both signals on the
  table. No code change in this plan.
- **Files modified:** `docs/OPEN_ISSUES.md` only.

Otherwise plan was executed exactly as written. No Rule 4 (architectural)
decisions needed — the regex fix is trivial and the deferred follow-ups
are already what the plan said to do if H1/H3 were confirmed.

## Self-Check: PASSED

- [x] `scripts/debug_ts_ns_158_112_image_alignment.py` exists and runs to
      completion in ~7s, prints alignment table and three verdict lines.
- [x] `shared/nli_crossref_service.py` `_FOLIO_PATTERN` updated; docstring
      references paired-leaf example.
- [x] `tests/test_nli_crossref_service.py` — 9 new tests pass;
      `pytest tests/test_nli_crossref_service.py` reports 77 passed.
- [x] `docs/OPEN_ISSUES.md` contains two new entries (partial-fix + open
      follow-up) with diagnostic evidence.
- [x] Commits `0825437c` and `17b8e9bf` present in `git log`.
- [x] `python scripts/check_docs.py` (with `PYTHONIOENCODING=utf-8`) reports
      "All checks passed! Documentation is healthy." — no new failures
      introduced.
