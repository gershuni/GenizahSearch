---
id: 260419-nwv
type: quick-fix
status: planned
created: 2026-04-19
description: "Bug: images don't fit the text on some shelfmarks, especially CUL (example: T-S NS 158.112, sys_id 990051537270205171)"
files_modified:
  - scripts/debug_ts_ns_158_112_image_alignment.py
  - shared/nli_crossref_service.py
  - tests/test_nli_crossref_service.py
  - docs/OPEN_ISSUES.md
must_haves:
  truths:
    - "For T-S NS 158.112 (sys_id 990051537270205171), the image shown on text page N corresponds to the same leaf/side that the text references (verified by comparing transcription FL IDs to NLI manifest FL IDs)."
    - "parse_folio_label() returns a correct folio label (e.g. '1r', '1v', '12r') for paired-leaf ImageName patterns like 'T_S_NS_158_112__L1_12F0B0S1'."
    - "get_folio_images() sorts paired-leaf images by leaf-number correctly (not all collapsing into the fallback sort key)."
  artifacts:
    - path: "scripts/debug_ts_ns_158_112_image_alignment.py"
      provides: "Reproducer/diagnostic: prints text-page order vs NLI-manifest FL ID order vs transcription FL IDs vs CUDL canvas labels for one sys_id"
    - path: "shared/nli_crossref_service.py"
      provides: "Fixed _FOLIO_PATTERN + parse_folio_label handling paired-leaf variants"
    - path: "tests/test_nli_crossref_service.py"
      provides: "Regression tests for paired-leaf parse_folio_label cases"
  key_links:
    - from: "parse_folio_label()"
      to: "get_folio_images() sort key"
      via: "_FOLIO_PATTERN regex (shared between both)"
      pattern: "_FOLIO_PATTERN\\.search"
---

<objective>
Fix the root cause of mis-aligned images on CUL (and other paired-leaf) shelfmarks.

**Strategy chosen: (E) Diagnose first, then (A) Fix the regex — plus decide about H1 with evidence, not guess.**

Rationale:
- The orchestrator has three ranked hypotheses (H1 positional CUDL canvas mismatch, H2 parse_folio_label regex bug on paired-leaf names, H3 NLI manifest FL order). H2 is code-inspectable and **confirmed** (see root-cause note below). H1 and H3 are runtime/data claims that need to be observed on a real manuscript before we commit to a fix.
- A 40-line diagnostic script that calls `fetch_fl_ids_from_nli`, `fetch_iiif_manifest`, and `fetch_external_iiif_data` and compares their output to `Transcriptions.txt` FL IDs will tell us in one run which hypothesis is actually the root cause for this sys_id — and whether CUL shelfmarks in general are affected via the same pathway.
- If the diagnostic shows text↔NLI are aligned (pages go in the same order the manifest returns them), then the CUL bug is purely H1 positional Cambridge mismatch, and we can fix it separately (and probably larger than a quick task). If diagnostic shows text↔NLI are themselves misaligned, that's a bigger problem that must be escalated.
- Meanwhile H2 (parse_folio_label bug on paired-leaf names) is low-risk to fix in the same plan: it is a clear bug, has a trivial regex change, is covered by new unit tests, and does not affect positional image indexing, so it cannot regress correct behavior. The folio-label display for paired-leaf shelfmarks will actually get better.

Purpose: Stop silently serving the wrong image to the user while reading a transcription. Get observable evidence on which pathway is broken for CUL-class shelfmarks.

Output: A reproducer script (committed), a real regex/parse fix with tests, updated OPEN_ISSUES.md tracking both the H2 fix and the H1 investigation outcome.

**Scope boundary:** This plan does NOT rewrite the Cambridge image URL path to map FL→canvas (strategy C). That is a larger change that depends on what the diagnostic reveals. If the diagnostic confirms H1 is the user-visible root cause, OPEN_ISSUES.md will track it as a follow-up quick task.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@CLAUDE.md
@.planning/STATE.md

# Root cause notes — DO NOT RE-INVESTIGATE (already verified by orchestrator)

- sys_id=990051537270205171 is T-S NS 158.112 (CUL).
- 14 rows in nli_crossref.db `nli_images` with ImageName like `T_S_NS_158_112__L1_12F0B0S1`, `L1_12F0B0S2`, `L2_11F0B0S1` ... `L5F0B0S1`, `L5F0B0S2` (paired-leaf / bifolio notation).
- Cambridge manifest exists: `cudl.lib.cam.ac.uk/iiif/MS-TS-NS-00158-00112`.
- Transcriptions.txt has 14 entries, FL IDs FL167150424..FL167150437 sequential, pattern `{sys_id}_IE167150422_P{page}_FL{fl_id}`.
- Confirmed regex bug: `_FOLIO_PATTERN = re.compile(r'L(\d+)F\d+B\d+S(\d+)')` at `shared/nli_crossref_service.py:32`. For input `T_S_NS_158_112__L1_12F0B0S1` the search locates `L1` then immediately expects `F`, but finds `_`. Regex does not backtrack across `_`, so `search()` returns None → `parse_folio_label()` returns empty string → all 14 images get the sequential fallback label "1","2",... AND all sort with key `(999999, 0)` (alphabetical fallback, unstable).
- Browse image URL for CUL auto-defaults to `active_source='cambridge'` at `web/pages/browse.py:3440-3441`, which calls `/api/cambridge_image/{sys_id}?page={page_idx}` where `page_idx = p_num - 1`. Server-side (`web/api.py:577-613`) does `images_ext[page_idx]` — purely positional, based on whatever order CUDL's IIIF manifest returned canvases. **No correction by folio label or FL ID.**
- Existing `/api/nli_image/{fl_id}` endpoint at `web/api.py:449-492` accepts an FL ID directly; browse does not currently use it.

# Relevant source files (interface contracts)

From `shared/nli_crossref_service.py:29-62`:
```python
_FOLIO_PATTERN = re.compile(r'L(\d+)F\d+B\d+S(\d+)')

def parse_folio_label(image_name: str) -> str:
    """Extract folio notation from an NLI ImageName value."""
    # Returns '1r', '3v', etc., or empty string if not matched.
```

From `shared/nli_crossref_service.py:246-289`:
```python
def get_folio_images(self, sys_id: str) -> list[dict]:
    """Returns images with 'folio_label' set, sorted by (leaf, side)."""
    # Sort key uses _FOLIO_PATTERN.search(); returns (999999, 0) if no match.
```

From `web/api.py:322-441`:
```python
def fetch_fl_ids_from_nli(system_id: str, suffix: int = 1) -> list:
    """Returns list of FL ID digit-strings in canvas order from NLI IIIF manifest."""
```

From `genizah_core.py:3517-3569`:
```python
def fetch_iiif_manifest(self, system_id, suffix=1):
    """Returns {'canvas_map': {fl_digits: label, ...}, ...}"""
```

From `genizah_core.py:3934-4010`:
```python
def fetch_external_iiif_data(self, view_url):
    """Returns {'canvases': [{'label': str, 'url': str, 'folio_num': int|None}], ...}"""
```
</context>

<tasks>

<task type="auto">
  <name>Task 1: Diagnostic reproducer — compare text order vs NLI manifest vs CUDL manifest for T-S NS 158.112</name>
  <files>scripts/debug_ts_ns_158_112_image_alignment.py</files>
  <action>
    Create a standalone diagnostic script (no web server, no NiceGUI). The script should:

    1. Accept an optional `--sys-id` argument (default: `990051537270205171`).
    2. Read the 14 Transcriptions.txt entries for this sys_id (grep the file or use existing loader in `genizah_core.GenizahSearchEngine._load_browse_map` — whichever is simpler; the file is in the repo root). Extract `(p_num, fl_id)` pairs in Transcriptions file order.
    3. Import and call `web.api.fetch_fl_ids_from_nli(sys_id, suffix=1)` — this returns FL digit strings in NLI IIIF manifest canvas order.
    4. Import and call `genizah_core.GenizahSearchEngine().fetch_iiif_manifest(sys_id, suffix=1)` — returns `canvas_map` with FL → label pairs.
    5. Query `nli_crossref.db` directly via sqlite3 (or via `NliCrossrefService`) for:
       - All rows in `nli_images` for this sys_id, showing `ImageName`, `FGPImageNumberId`, `FGPNumber`.
       - The cambridge_manifests row for normalized_shelfmark='tsns158.112'.
    6. Fetch the CUDL manifest JSON directly via `requests.get(manifest_url)` and extract the canvas list in order with labels.
    7. Print a 4-column alignment table:
       ```
       p_num | transcription_fl | nli_manifest_fl (at idx p_num-1) | cudl_canvas_label (at idx p_num-1) | nli_images_folio_label_parsed
       ```
    8. Print an ALIGNMENT VERDICT at the bottom for each pairing:
       - "text↔NLI aligned" if transcription_fl == nli_manifest_fl for all pages
       - "text↔NLI misaligned — NLI canvas order differs from transcription" otherwise (list which p_nums mismatch)
       - "text↔CUDL visually likely-aligned" if CUDL labels are strictly "1r,1v,2r,2v,..." order and transcription FL IDs are strictly sequential
       - "text↔CUDL misaligned" otherwise (e.g., CUDL has "Binding", "Front cover", extra canvases, or labels out of order)
    9. Also print `parse_folio_label(name)` output for each `nli_images.ImageName` — this will make the H2 regex bug visible as empty strings for all 14 rows.

    Keep the script self-contained. Use only sqlite3, requests, re, sys, pathlib — no NiceGUI import. Never start a web server. Run via `python scripts/debug_ts_ns_158_112_image_alignment.py` and pipe output to stdout.

    Include a docstring explaining: "Diagnostic for bug 260419-nwv. Reproduces image-vs-text mis-alignment on paired-leaf CUL shelfmarks. Not a test — a one-shot forensic tool."
  </action>
  <verify>
    <automated>python scripts/debug_ts_ns_158_112_image_alignment.py</automated>
    Script runs to completion and prints:
    - A 4-column alignment table with 14 rows.
    - An ALIGNMENT VERDICT section.
    - A parse_folio_label column showing empty strings for all 14 paired-leaf names (confirms H2).
    Based on the verdict output, amend the plan summary (in SUMMARY.md, written in Task 3) to state which of H1 / H3 are actually in play. If the script fails with network errors (NLI/CUDL 5xx or timeout), cache the last-known-good JSON under `_iiif_samples/` and reference it as a static fallback — tool must still complete within 30 seconds.
  </verify>
  <done>
    scripts/debug_ts_ns_158_112_image_alignment.py exists and, when run, prints verdicts answering: (a) is text↔NLI order aligned? (b) is text↔CUDL order aligned? (c) does parse_folio_label return non-empty for these names? Output is captured into the Task 3 SUMMARY.md.
  </done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Fix parse_folio_label regex for paired-leaf ImageNames + add regression tests</name>
  <files>shared/nli_crossref_service.py, tests/test_nli_crossref_service.py</files>
  <behavior>
    - Test: `parse_folio_label('T_S_NS_158_112__L1_12F0B0S1')` returns `'1r'` (leaf = first number, i.e. 1; NOT 12).
    - Test: `parse_folio_label('T_S_NS_158_112__L1_12F0B0S2')` returns `'1v'`.
    - Test: `parse_folio_label('T_S_NS_158_112__L2_11F0B0S1')` returns `'2r'`.
    - Test: `parse_folio_label('T_S_NS_158_112__L5F0B0S1')` returns `'5r'` (non-paired still works — regression guard).
    - Test: `parse_folio_label('T_S_12_1__L1F0B0S2')` returns `'1v'` (existing case — regression guard).
    - Test: `parse_folio_label('I_C_71__L3F0B0S1')` returns `'3r'` (existing case).
    - Test: `parse_folio_label('')` returns `''`.
    - Test: `parse_folio_label('no_folio_here')` returns `''`.
    - Test: `get_folio_images(sys_id)` on a DB with paired-leaf names sorts images by leaf number ascending, then side (1 before 2), no longer falls back to `(999999, 0)`. Use a temp sqlite DB or monkeypatched service for this one — do NOT require the full nli_crossref.db to be present. Minimum assertion: with inputs `L2_11F0B0S1`, `L1_12F0B0S2`, `L1_12F0B0S1`, sorted order is `L1_12F0B0S1`, `L1_12F0B0S2`, `L2_11F0B0S1`.

    The decision: paired-leaf notation `L{first}_{second}F...` refers to a bifolio consisting of leaves `first` and `second` (conjoint leaves of a folded sheet). For image-ordering and folio-label purposes, use `first` as the primary leaf number — this matches NLI's intent that images display as if you're turning the physical pages in conservation order. (This is a pragmatic choice; if T1 diagnostic shows the second number is actually the one referenced by the transcription text, swap the decision to `second` — note it in SUMMARY.md.)
  </behavior>
  <action>
    1. Write the 8 test cases above in `tests/test_nli_crossref_service.py` (append to the file if it exists, create otherwise). Include a test class `TestParseFolioLabelPairedLeaf`.

    2. Run the tests — they MUST fail (RED) on the current regex.

    3. Update `_FOLIO_PATTERN` in `shared/nli_crossref_service.py:32` to:
       ```python
       # Pattern: L{leaf}(optionally _{second_leaf} for bifolio/paired-leaf)F{folio}B{bifolio}S{side}
       _FOLIO_PATTERN = re.compile(r'L(\d+)(?:_\d+)?F\d+B\d+S(\d+)')
       ```
       Group 1 remains the primary leaf; the optional non-capturing `(?:_\d+)?` absorbs paired-leaf notation. `F\d+B\d+S(\d+)` is unchanged.

    4. Update the docstring of `parse_folio_label` to document paired-leaf support with one example:
       ```
       - 'T_S_NS_158_112__L1_12F0B0S1' -> '1r' (paired-leaf / bifolio; primary leaf = 1)
       ```

    5. Run the tests — they MUST pass (GREEN). Run the FULL test suite (`pytest tests/` or at minimum `pytest tests/test_nli_crossref_service.py`) to confirm no regressions in other folio-label tests.

    6. If the diagnostic from Task 1 revealed that `_sort_key` in `get_folio_images()` still falls back to `(999999, 0)` for some remaining ImageName variant in the production DB (e.g. names without the `L...F...B...S...` suffix at all), leave the fallback intact but log a debug warning listing the problem ImageName once per sys_id. Do NOT rewrite the sort logic broadly — that's out of scope for this quick fix.

    7. NO changes to `web/pages/browse.py` or `web/api.py` in this task. Even though H1 (CUL positional mismatch) may be the user-visible root cause, fixing it requires mapping CUDL canvas labels to FL IDs, which is (a) larger than a quick task and (b) depends on Task 1 diagnostic data. Track it in Task 3 for follow-up.
  </action>
  <verify>
    <automated>pytest tests/test_nli_crossref_service.py -x -v</automated>
    All new tests pass; pre-existing parse_folio_label tests still pass.
  </verify>
  <done>
    - `parse_folio_label` handles paired-leaf names correctly (returns first leaf number as the primary).
    - 8 new test cases in `tests/test_nli_crossref_service.py` all pass.
    - No regressions in the broader test suite.
    - Folio labels in the browse UI for T-S NS 158.112 will now display as "1r, 1v, 2r, ..." instead of "1, 2, 3, ..." (visually verifiable but not mandatory for this task).
  </done>
</task>

<task type="auto">
  <name>Task 3: Update OPEN_ISSUES.md, record diagnostic findings, decide on H1 follow-up</name>
  <files>docs/OPEN_ISSUES.md, .planning/quick/260419-nwv-bug-with-some-shelfmarks-images-esp-cul-/260419-nwv-SUMMARY.md</files>
  <action>
    1. Create the quick-task summary file `.planning/quick/260419-nwv-bug-with-some-shelfmarks-images-esp-cul-/260419-nwv-SUMMARY.md` containing:
       - Short description of the bug.
       - The Task 1 diagnostic output (copy-paste or reference the captured stdout).
       - The alignment verdict: which of H1/H2/H3 were confirmed/refuted.
       - H2 fix summary: regex change + tests added.
       - **Decision on H1 follow-up:**
         - If diagnostic shows text↔CUDL canvas order IS aligned for this manuscript → H1 is not the user's issue; close the bug once user confirms the regex fix resolved their symptom (folio labels were wrong, leading to wrong-page navigation through the folio picker).
         - If diagnostic shows text↔CUDL canvas order is NOT aligned → create follow-up quick task file `docs/OPEN_ISSUES.md` entry "260419-nwv-followup: CUL text↔CUDL positional mismatch" with the diagnostic evidence pasted in, and mark THIS bug (260419-nwv) as "partially fixed — folio labels corrected; positional canvas mismatch tracked separately". Do NOT auto-create the follow-up plan; let the user decide scope.
         - If diagnostic shows text↔NLI misaligned (H3) → this is more serious than expected, escalate in SUMMARY.md, do not self-close.

    2. Update `docs/OPEN_ISSUES.md`:
       - Add an entry to the appropriate section (likely "Image Loading / Display Issues" or equivalent) describing the original bug.
       - Mark status based on diagnostic verdict:
         - `✅ Fixed (2026-04-19)` if diagnostic confirms folio-label was the only user-visible symptom,
         - `🟡 Partially Fixed (2026-04-19) — folio-label parse bug fixed; positional CUDL-canvas alignment tracked as separate follow-up` if H1 also confirmed.
       - Reference: `scripts/debug_ts_ns_158_112_image_alignment.py` as the reproducer, `.planning/quick/260419-nwv-*` for the full writeup.
       - Update the "Last Updated" timestamp at the top to `2026-04-19`.

    3. Do NOT touch CLAUDE.md "Recently Changed" — that's for user-facing release-gated changes and this is a bug-squash; it will be bundled in the next release.
  </action>
  <verify>
    <automated>python scripts/check_docs.py</automated>
    docs/check_docs green (or unchanged error count — don't introduce new failures). SUMMARY.md file exists at the expected path and has a non-empty verdict section.
  </verify>
  <done>
    - docs/OPEN_ISSUES.md reflects the bug and fix status.
    - SUMMARY.md contains the actual diagnostic output (not a placeholder) and a concrete verdict on H1/H2/H3.
    - Follow-up (if any) is tracked in OPEN_ISSUES.md with enough context to act on later — not lost.
  </done>
</task>

</tasks>

<verification>
- Task 1 diagnostic script runs and produces an alignment verdict.
- Task 2 regex fix tests pass; existing tests still pass.
- Task 3 OPEN_ISSUES.md and SUMMARY.md reflect actual findings, not speculation.
- **User-perceptible behavior change:** folio labels for T-S NS 158.112 (and ~N other paired-leaf CUL shelfmarks — diagnostic will count them) will now show as correct leaf notation ("1r, 1v, 2r, ...") in the browse UI folio picker, instead of generic "1, 2, 3, ...". If text↔CUDL positional alignment was also broken (H1), the UI symptom for that pathway is unchanged by this plan and will be tracked as follow-up.
</verification>

<success_criteria>
- [ ] `pytest tests/test_nli_crossref_service.py -x` passes including 8 new paired-leaf test cases.
- [ ] Running `scripts/debug_ts_ns_158_112_image_alignment.py` prints a concrete alignment verdict.
- [ ] `docs/OPEN_ISSUES.md` reflects the correct fix status.
- [ ] `.planning/quick/260419-nwv-.../260419-nwv-SUMMARY.md` contains the verdict and a decision on H1 follow-up.
- [ ] No regression: existing `parse_folio_label` tests still pass; no changes to browse.py or api.py.
</success_criteria>

<output>
After completion, create `.planning/quick/260419-nwv-bug-with-some-shelfmarks-images-esp-cul-/260419-nwv-SUMMARY.md` per Task 3.
</output>
