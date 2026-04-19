---
name: 260419-cfx CONTEXT
description: Follow-up to 260419-nwv. Fix H1 (CUDL positional canvas mismatch) in both web and desktop; correct the prior H3 verdict.
type: quick-context
---

# Quick Task 260419-cfx — CUL CUDL positional canvas mismatch — Context

**Gathered:** 2026-04-19
**Status:** Ready for planning
**Predecessor:** 260419-nwv (fixed H2 paired-leaf regex; deferred H1/H3)

<domain>
## Task Boundary

Fix the Cambridge image positional mismatch for CUL manuscripts where the
CUDL IIIF manifest canvas count differs from the transcription page count
(H1). The representative case is **T-S NS 158.112 (sys_id 990051537270205171)**
where CUDL has 12 canvases (folios 1r–6v) but transcription has 14 pages
(covers leaf 7/8 as paired-leaf/single).

This plan also **retracts the prior H3 verdict** — the original
260419-nwv SUMMARY.md claimed `fetch_fl_ids_from_nli` returned FL ids from
the wrong IE. Deeper probing shows this is not a bug: `Transcriptions.txt`
references text-layer FLs (e.g. FL167150424–437) which 500 on image GET,
while the NLI IIIF manifest correctly returns image-layer FLs
(FL167150439–452) in the same IE167150422. Both layers are valid, just
serving different content; the manifest is serving what we want.

**In scope:**
- Web: `/api/cambridge_image/{sys_id}?page={N}` — change from positional
  canvas lookup to folio+side based lookup, with NLI fallback when no
  match.
- Web: browse.py `switch_to_cambridge` path / image URL construction must
  continue to work (no UI change required; URL shape stays the same).
- Desktop: mirror the same correction wherever CUDL canvas selection is
  done positionally. Desktop already uses `_get_folio_image_index` for
  folio-based mapping elsewhere, but CUDL short-list cases still need
  verification; extend / reuse that helper so page→folio→canvas is correct
  when CUDL has fewer canvases than transcription pages.
- SUMMARY.md for this task + OPEN_ISSUES.md update correcting the earlier
  H3 misinterpretation and marking H1 fixed.

**Out of scope:**
- Any multi-IE `suffix=` audit or primary_ie_map.json integration. User
  confirmed H3 can be dropped.
- Schema changes to `images_ext` (e.g., adding FL ids to CUDL canvas
  entries) — we use folio+side matching instead.
- New REST endpoints. Keep the URL `/api/cambridge_image/{sys_id}?page={N}`
  compatible; just fix the server-side resolution of N→canvas.

</domain>

<decisions>
## Implementation Decisions

### CUDL canvas mapping strategy
**LOCKED: Folio+side match with NLI fallback.**

- For a given transcription page index N on sys_id S:
  1. Look up the N-th nli_images row for S sorted by
     `parse_folio_label` (now that the paired-leaf regex is fixed, these
     sort leaf-first, side-first: e.g. (1,r), (1,v), (2,r), (2,v), ...,
     (8,r), (8,v) for T-S NS 158.112).
  2. Parse that row's `ImageName` → folio label like `"1r"`, `"5v"`, etc.
  3. Search `cambridge_images` (a.k.a. `images_ext`) for the canvas whose
     label normalizes to the same `{folio_num}{side}`. Labels in
     `images_ext` already carry `folio_num` and original `label`; side
     comes from the trailing `r`/`v` in the original label.
  4. If found, serve that canvas.
  5. If not found (e.g. transcription page 13 has no CUDL canvas), **fall
     back to the NLI image** for that transcription page (see below).

This mirrors the spirit of `desktop/widgets.py::_get_folio_image_index`
but adds explicit side (recto/verso) matching — the existing helper
ignores side in the common case.

### Missing-canvas fallback
**LOCKED: Fall back to NLI image for that transcription page.**

- When no CUDL canvas matches, the Cambridge endpoint should transparently
  serve the NLI image for that transcription page (equivalent to what
  `/api/nli_image_by_sysid/{sys_id}?page={N}&suffix={volume_suffix}` would
  return).
- Add a response header `X-Image-Fallback-Source: nli` so the UI can
  optionally indicate provenance (not required for this quick fix).
- The same behavior applies on desktop — when no CUDL canvas matches, use
  the NLI image for that page.

### H3 scope
**LOCKED: Drop H3. Correct the prior verdict.**

- Do NOT modify `fetch_fl_ids_from_nli` or touch `primary_ie_map.json`
  logic.
- Do update `docs/OPEN_ISSUES.md` to mark the "CUL positional image
  mismatch" P2 entry from 260419-nwv as `✅ Fixed (2026-04-19)` (once this
  plan lands) and add a short NOTE that the H3 claim in the 260419-nwv
  SUMMARY was a misinterpretation (text-layer vs image-layer FL ids), not
  an actual IE-selection bug.

### Desktop scope
**LOCKED: Verify then fix desktop.** User confirmed "the problem was
inspected in desktop" — desktop shows the same symptom on T-S NS 158.112.

- Task must check the desktop code path that selects a CUDL canvas when
  the user is on transcription page N of a manuscript whose CUDL canvas
  list is shorter than the transcription. If the existing
  `_get_folio_image_index` path already routes through folio_num and
  yields the wrong index (because p_num != folio_num for paired-leaf
  shelfmarks), extend it so it uses the same NLI-image-name derived
  (folio, side) lookup as web.
- Desktop's NLI fallback for missing canvases should reuse whatever NLI
  image fetch path is already used when `active_source == 'nli'` —
  do not invent a new one.

### Claude's Discretion
- Exact regex / helper name for "extract side from label" — keep it local
  to the helper that does the mapping; do not bloat shared service APIs.
- Whether to surface a small UI indicator ("showing NLI image — no CUDL
  canvas for this page") is out of scope for this quick fix. Log it
  server-side at INFO only.
- Test fixture scope — minimum: unit test the new mapping helper with the
  exact T-S NS 158.112 inputs (12 CUDL canvases vs 14 nli_images rows) and
  assert expected indices for pages 1..14. Additional E2E HTTP test
  optional.

</decisions>

<specifics>
## Specific Ideas

### Representative fixture — T-S NS 158.112

```
sys_id = 990051537270205171

Transcriptions.txt (14 pages):
  P1..P14 → FL167150424..FL167150437 (text-layer FLs, 500 on image GET)

nli_images rows (14) sorted by parse_folio_label after 260419-nwv fix:
  P1  L1_12F0B0S1 → 1r
  P2  L1_12F0B0S2 → 1v
  P3  L2_11F0B0S1 → 2r
  P4  L2_11F0B0S2 → 2v
  P5  L3_10F0B0S1 → 3r
  P6  L3_10F0B0S2 → 3v
  P7  L4_9F0B0S1  → 4r
  P8  L4_9F0B0S2  → 4v
  P9  L5F0B0S1    → 5r
  P10 L5F0B0S2    → 5v
  P11 L6_7F0B0S1  → 6r
  P12 L6_7F0B0S2  → 6v
  P13 L8F0B0S1    → 8r   ← no CUDL canvas for 8r; fall back to NLI
  P14 L8F0B0S2    → 8v   ← no CUDL canvas for 8v; fall back to NLI

CUDL canvases (12):
  [0]  label='1r'  (MS-TS-NS-00158-00112-000-00001.jp2)
  [1]  label='1v'
  [2]  label='2r'
  [3]  label='2v'
  [4]  label='3r'
  [5]  label='3v'
  [6]  label='4r'
  [7]  label='4v'
  [8]  label='5r'
  [9]  label='5v'
  [10] label='6r'
  [11] label='6v'

Expected resolver output for /api/cambridge_image/{sys_id}?page={N-1}:
  N=1..12 → CUDL canvas[N-1]
  N=13, 14 → NLI image (fallback) for transcription page N
```

### Key code anchors

- `web/api.py:576-634` — `cambridge_image(sys_id, page)` positional
  lookup. This is the main site of the fix.
- `web/api.py:497-568` — `nli_image_by_sysid(sys_id, page, ..., suffix)`
  can be reused (internally, not as an extra HTTP hop) for the NLI
  fallback path.
- `web/pages/browse.py:3440-3446` — caller that sets
  `/api/cambridge_image/{sys_id}?page={page_idx}`. No change.
- `shared/nli_crossref_service.py` — `parse_folio_label` fixed in
  260419-nwv; `get_folio_images` sort is now correct. Use
  `NliCrossrefService.get_folio_images(sys_id)` to retrieve the sorted
  (leaf, side) list for N→folio mapping.
- `desktop/widgets.py:94-152` — `_get_folio_image_index` and
  `_get_initial_image_index`. Extend to also match side, or add a
  sibling helper that does side-aware matching.
- `genizah_core.py:3934-4010` — `fetch_external_iiif_data` already
  parses `folio_num` from CUDL labels. Extend parsing to also capture
  `side` ('r' or 'v') so we have (folio_num, side) tuples on each
  images_ext entry. This change is localized.
- `scripts/debug_ts_ns_158_112_image_alignment.py` — existing diagnostic
  from 260419-nwv. Extend (or add a small companion verifier) to assert
  the expected N→canvas mapping as a post-fix smoke check.

</specifics>

<canonical_refs>
## Canonical References

- Predecessor: `.planning/quick/260419-nwv-bug-with-some-shelfmarks-images-esp-cul-/260419-nwv-SUMMARY.md`
- Diagnostic script: `scripts/debug_ts_ns_158_112_image_alignment.py`
- Open issue: `docs/OPEN_ISSUES.md` — P2 entry added by 260419-nwv
  "CUL positional image mismatch" (currently `❌ Open`; flip to
  `✅ Fixed (2026-04-19)` on completion of this plan).
- Folio-label service: `shared/nli_crossref_service.py::parse_folio_label`
  (fixed in 260419-nwv commit 17b8e9bf).

</canonical_refs>
