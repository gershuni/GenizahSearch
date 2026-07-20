---
status: resolved
trigger: "Desktop: pages/images duplicated. CUL T-S AS 2.3 shows 4 images instead of 2, each image-page duplicated, also in 'view all' (full-text view shows each folio's transcription twice, e.g. תמונה: 2 appears twice with identical content)."
created: 2026-07-20T00:00:00Z
updated: 2026-07-20T02:30:00Z
---

## Current Focus

hypothesis: CONFIRMED (by code analysis + live human UAT). Both symptoms share ONE cause: the browse map for T-S AS 2.3 contains literal duplicate page entries (same IE + p_num + FL). `SearchEngine.get_browse_page` sets `total_pages = len(pages)` and `get_full_manuscript` iterates the same list — so 2 physical folios stored 4× → nav shows "4 pages" (cycling the 2 real images = "4 images, each duplicated") AND view-all renders 4 text blocks (each folio twice, identical fl_id 164987437). `dedupe_browse_map()` only dedups manuscripts registered in `ie_volume_map.json` (the "multi-IE" branch, keyed on (ie_id, p_num)); manuscripts NOT in that map hit the single-IE branch which does NO dedup (comment: "p_nums are unique"). T-S AS 2.3 is not in the map, so its bundled-data dupes survive. Web's rebuilt-on-deploy map is clean → shared dedup is a no-op there.
test: fix = extend single-IE branch to dedup by (ie_id, p_num) preserving first-seen order (mirrors multi-IE "true duplicate" definition); add regression test with a synthetic duplicated single-IE map
expecting: deduped map yields 2 pages for the ms; web unaffected (distinct pages → distinct keys → nothing removed); multi-IE branch untouched
next_action: DONE — human UAT confirmed on the live desktop app (2 pages/images not 4; view-all each folio once; normal multi-folio CUL ms unaffected). Fix committed on fix/desktop-duplicate-pages; session archived.

## Reasoning Checkpoint (pre-fix)

reasoning_checkpoint:
  hypothesis: "T-S AS 2.3 (CUL, single-IE, not in ie_volume_map.json) has duplicate page entries in the desktop browse map; dedupe_browse_map's single-IE branch performs NO dedup, so total_pages doubles → view-all text doubles AND per-page image navigation cycles each real image twice."
  confirming_evidence:
    - "tr('FL')='מס' קובץ' + browse_load_all separator = 'Image: {p_num} (FL: {fl_id})' → user's doubled block 'תמונה: 2 (מס' קובץ: 164987437)' is one page entry rendered twice with IDENTICAL fl_id (same full_header ⇒ literal duplicate, not two IEs)."
    - "get_browse_page: total_pages = len(pages); get_full_manuscript iterates browse_map[sys_id] — both the nav count and view-all consume the same list. One doubled list explains BOTH symptoms."
    - "dedupe_browse_map single-IE branch (sid NOT in ie_volume_map) sets cleaned[sid]=pages with no dedup; multi-IE branch DOES dedup by (ie_id,p_num). A single-IE ms with raw dupes is never cleaned."
    - "Web + desktop share get_full_manuscript/enrich_metadata; web shows 2 → web's rebuilt browse map has no dupes; the divergence is data + the single-IE no-dedup gap, not two code paths."
  falsification_test: "If T-S AS 2.3 were registered in ie_volume_map.json (multi-IE), the (ie_id,p_num) dedup would already remove the dupes and the bug wouldn't reproduce. Also: if the two doubled blocks had DIFFERENT fl_ids, it would be two IEs (not a dedup miss) and this fix would be wrong."
  fix_rationale: "Deduping the single-IE branch by (ie_id,p_num) — the codebase's own 'true duplicate' definition (multi-IE branch) — removes exact page duplicates at load AND index time. A single manuscript can never legitimately have two distinct pages sharing the same IE+page-number, so this only removes true dupes; distinct pages keep distinct keys (web no-op)."
  blind_spots: "Cannot inspect the actual bundled browse_map.pkl/Transcriptions.txt here (gitignored/absent), so the raw duplicate is inferred from the rendered symptom, not directly observed. Also assuming the desktop image navigation is driven by transcription-page count (verified via get_browse_page.total_pages + per-page folio→image lookup), not an independent image-list duplication."

## Symptoms

expected: CUL T-S AS 2.3 shows 2 images (2 folios/pages), each transcription once
actual: Shows 4 images instead of 2; each image-page duplicated; "view all" (full text) repeats each folio's transcription verbatim (user pasted "תמונה: 2 (מס' קובץ: 164987437)" block appearing twice, identical)
errors: None — visual/data duplication, no exception
timeline: Not sure when it started / whether it ever worked
reproduction: Open CUL T-S AS 2.3 in the DESKTOP app; view images and/or "view all" full text

## Scope & Isolation (from user)

- **Platform:** DESKTOP only. The WEB app (genizahsearch.com) shows CUL T-S AS 2.3 CORRECTLY with 2 images. → bug is in desktop-only code, or a shared helper the desktop calls differently than web.
- **Corpus:** Appears CUL-specific. User's own hypothesis: "perhaps because of the dual source of images (NLI/CUDL)". CUL manuscripts can have images from both NLI and Cambridge University Digital Library (CUDL); nli_crossref.db holds 815K NLI images AND 141K Cambridge manifests. v7.11.0 added a FIST↔CUDL shelfmark bridge.
- **Evidence of pairing:** In the pasted "view all", the "file number" (מס' קובץ) for the doubled folio is the SAME on both copies (164987437) → same folio emitted twice, not two different folios. This favors a dedup/merge bug over a data-count bug.

## Evidence

- timestamp: 2026-07-20
  checked: shared/nli_crossref_service.py::get_folio_images + enrich_metadata (shared/metadata_manager.py) + desktop/viewers.py::ManuscriptViewerWidget
  found: image lists (images_nli from NLI IIIF manifest, images_ext from CUDL) are NOT concatenated by the desktop; active_list is one or the other. The viewer shows one image with prev/next. So "4 images" is NOT nli+ext concatenation.
  implication: the user's/original NLI+CUDL-concatenation hypothesis is wrong; look at the per-page/transcription list instead.

- timestamp: 2026-07-20
  checked: shared/search_engine.py::get_full_manuscript (L3563) + get_browse_page (L3632) + genizah_app.py::browse_load_all (L9509)
  found: both the "N pages" navigator (total_pages = len(pages)) and the view-all render iterate browse_map[sys_id] pages. Per-page image is looked up by the page's folio → so N transcription pages drive N image navigations.
  implication: a doubled browse_map page list produces BOTH doubled view-all text AND doubled image navigation. Single shared root cause.

- timestamp: 2026-07-20
  checked: genizah_translations.py (tr('FL')="מס' קובץ") + browse_load_all separator format 'Image: {p_num} (FL: {fl_id})'
  found: user's doubled block "תמונה: 2 (מס' קובץ: 164987437)" = page p_num=2, fl_id=164987437, rendered twice with IDENTICAL fl_id.
  implication: literal duplicate page entry (same full_header) — not two different IEs.

- timestamp: 2026-07-20
  checked: shared/browse_map_utils.py::dedupe_browse_map (L536) + _load_ie_volume_map (L353) + shared/indexer.py (L414-423)
  found: dedupe_browse_map dedups by (ie_id,p_num) ONLY for manuscripts present in ie_volume_map.json (multi-IE branch). Manuscripts absent from that map take the single-IE branch which sets cleaned[sid]=pages with NO dedup ("p_nums are unique"). dedupe runs at both index-build and load time.
  implication: a manuscript not registered in ie_volume_map (single-IE) whose raw browse map has duplicate pages is never cleaned → root cause.

- timestamp: 2026-07-20
  checked: git log fix/desktop-duplicate-pages vs master-main; docs/OPEN_ISSUES.md; build spec for AllGenizah_OLD.txt/FILE_V7
  found: branch has no fix commits yet (empty diff). Prior CUL/CUDL work (260419-cfx/260421-aln) is about image ALIGNMENT, a different bug. FILE_V7 not referenced in build spec.
  implication: this is a fresh fix; the dedup single-IE gap is untracked. Runtime data (browse_map.pkl, full Transcriptions.txt, sidecar DBs) is gitignored/absent on this machine — dupes inferred from symptom, not directly inspected.

## Eliminated

- hypothesis: Desktop concatenates NLI images + Cambridge/CUDL manifests into one image list without dedup (the original trigger hypothesis).
  evidence: desktop/viewers.py ManuscriptViewerWidget keeps images_nli and images_ext as SEPARATE lists; active_list is one OR the other (source combo switches between them), never concatenated. The "4 images" is the per-page image navigator cycling 2 real images across 4 (duplicated) transcription pages, not a merged 4-image list.
  timestamp: 2026-07-20

- hypothesis: _repair_missing_ie_pages injects duplicate pages on desktop.
  evidence: that repair only touches manuscripts registered in ie_volume_map.json (multi-IE) with MISSING expected IEs; T-S AS 2.3 is single-IE (not in the map) so the repair skips it entirely.
  timestamp: 2026-07-20

## Resolution

root_cause: |
  dedupe_browse_map() (shared/browse_map_utils.py) only removed duplicate page
  entries for manuscripts registered in ie_volume_map.json (the "multi-IE"
  branch, keyed on (ie_id, p_num)). Manuscripts absent from that map took a
  single-IE branch that did NO dedup ("p_nums are unique"). CUL T-S AS 2.3 is
  not in the map, and its bundled browse map carries literal duplicate page
  entries (same IE + p_num + FL). Since get_browse_page sets
  total_pages = len(pages) and get_full_manuscript iterates the same list,
  2 physical folios stored 4× produced BOTH a "4 pages" navigator (cycling the
  2 real images → "4 images, each duplicated") AND a view-all that repeated each
  folio verbatim (identical fl_id 164987437). Web's browse map is rebuilt clean
  on deploy, so the shared dedup was already a no-op there — hence web showed 2.
fix: |
  Extended the single-IE branch of dedupe_browse_map() to dedup by
  (ie_id, p_num) — the same "true duplicate" definition the multi-IE branch
  uses — preserving first-seen order and setting changed=True when a dup is
  dropped. Distinct pages have distinct keys (no false removal), un-registered
  manuscripts that carry multiple real IEs keep every IE (different ie_id →
  different key), and the multi-IE branch is untouched. dedupe_browse_map runs
  at BOTH index-build (shared/indexer.py) and load (SearchEngine._load_browse_map),
  so this single change heals the map on the next desktop load and rebuild.
verification: |
  Added tests/test_browse_map_dedup_single_ie.py (5 tests, all green):
  reproduces the exact T-S AS 2.3 shape (2 folios stored 4×) → dedup yields 2;
  distinct single-IE pages preserved (web non-regression); first-seen order +
  object identity; un-registered two-IE ms keeps both IEs; registered multi-IE
  branch unchanged. Regression suites green: browse (87), page-contract +
  folio-nav + composition-dedup (27). ruff clean on changed files.
  Live desktop UAT CONFIRMED (human, 2026-07-20): CUL T-S AS 2.3 now shows
  2 pages/images (not 4), View All renders each folio's transcription once,
  and a normal multi-folio CUL manuscript still shows all its real pages.
files_changed:
  - shared/browse_map_utils.py (dedupe_browse_map single-IE branch)
  - tests/test_browse_map_dedup_single_ie.py (new regression test)
