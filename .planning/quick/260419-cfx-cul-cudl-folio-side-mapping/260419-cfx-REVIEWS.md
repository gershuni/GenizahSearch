---
phase: 260419-cfx
reviewers: [gemini, codex]
reviewed_at: 2026-04-19
plans_reviewed: [260419-cfx-PLAN.md]
---

# Cross-AI Plan Review — Quick Task 260419-cfx

## Gemini Review

# Cross-AI Plan Review: 260419-cfx (CUL CUDL Positional Canvas Mismatch)

## 1. Summary
The implementation plan is **exceptionally thorough and technically robust**. It demonstrates a sophisticated understanding of the GenizahSearch data architecture, particularly the subtle but critical distinction between NLI image-layer FL IDs and Friedberg photo numbers (FGP IDs). By replacing brittle positional indexing with a deterministic **folio+side resolver**, the plan directly solves the reported alignment bugs for CUL paired-leaf manuscripts (like T-S NS 158.112) while providing a transparent fallback to NLI images for missing CUDL canvases. The inclusion of cache versioning and graceful degradation ensures high reliability for both web and desktop users.

---

## 2. Strengths
*   **Data Accuracy (Pitfall Avoidance):** The plan rigorously enforces the rule against using `FGPImageNumberId` for IIIF URLs, correctly sourcing FL IDs from the NLI manifest's `canvas_map`. This prevents a major architectural regression.
*   **Cache Safety:** Introducing `_CAMBRIDGE_CACHE_VERSION = 2` is a critical move. Since the resolution logic changes (e.g., page 13 moves from a 404/wrong-canvas to an NLI fallback), invalidating the old cache is mandatory to prevent stale data.
*   **Surgical Desktop Integration:** The clear separation of `MODIFY` vs `LEAVE ALONE` sites in the ~22.5K line `genizah_app.py` minimizes regression risks in sensitive areas like Oxford-specific logic.
*   **Resilient Fallback Strategy:** The "Degraded Mode" (returning to legacy positional behavior if the sidecar is missing) is a senior-level engineering decision that prioritizes availability over "perfect" correctness in edge-case failures.
*   **Test-Driven Diagnostic:** Leveraging the existing `debug_ts_ns_158_112_image_alignment.py` script as a post-fix verifier provides empirical proof of success.

---

## 3. Concerns
*   **Multi-IE Fallback (LOW):** The plan hardcodes `suffix=1` for NLI fallbacks on web and potentially desktop. While acceptable for a quick-fix, CUL has many multi-IE manuscripts. If a manuscript spans two IEs (e.g., 8 folios in IE1, 8 in IE2), the fallback for pages 9+ might incorrectly attempt to pull from IE1. 
    *   *Mitigation:* The plan already mandates documenting this in `SUMMARY.md`.
*   **CUDL Label Parsing (LOW):** The regex `^\s*(?:f\.?\s*)?(\d+)\s*([rv])?\b` is optimized for CUDL's standard "1r" / "f. 1v" labels. If a rare shelfmark uses "Plate 1" or "Insert", it will fail to match.
    *   *Mitigation:* The "bare numeric = recto" convention covers 95% of non-explicit cases.
*   **NiceGUI Synchronicity (LOW):** The web API uses `def` (sync) endpoints. While `requests.get` is blocking, Starlette/FastAPI runs sync handlers in a thread pool. As long as the NLI/CUDL latency is within bounds (10-15s), this won't block the main event loop.

---

## 4. Suggestions
*   **Fallback Logging:** In `web/api.py`, when an NLI fallback occurs, consider adding the resolved `fl_id` to the `INFO` log. This makes it easier to verify that the fallback is pulling the *intended* page.
*   **Desktop Metadata Mutation:** In `genizah_app.py`, the plan uses `display_meta = dict(display_meta)` before appending the synthetic NLI entry. This is correct to prevent side-effects on the underlying cached metadata, but double-check that `viewer_images.load_images` doesn't hold a reference to the old list if the user navigates back and forth rapidly.
*   **Header Debugging:** The `X-Image-Fallback-Source` header is excellent. You might also add `X-Folio-Matched: 8r` to the response to confirm exactly which folio the resolver matched against.

---

## 5. Risk Assessment
**Overall Risk: LOW**

The plan is surgically scoped and relies on a well-understood fixture (T-S NS 158.112). The logic for folio+side matching is fundamentally more correct than positional indexing for this corpus. The most significant risk—stale cache—is effectively mitigated by the version bump. The H3 retraction shows high forensic integrity, ensuring the codebase doesn't carry unnecessary "fixes" for non-existent bugs.

**Verdict: Approved for autonomous execution.**

---

## Codex Review

## Summary

The plan is strong on the core H1 fix: for the T-S NS 158.112 fixture, the intended mapping is mathematically correct, and folio+side matching is the right correction for the existing web positional bug. It also does a good job fencing off the FGP-vs-FL pitfall and documenting the H3 retraction. The main weakness is that the desktop scope is underspecified in one critical place: the plan treats two load sites as sufficient, but the live desktop navigation path also recomputes image indices after page changes, and that path still uses `_get_folio_image_index`. If left unchanged, the desktop fix is likely partial.

## Strengths

- The resolver logic for the representative fixture is correct: sorted `nli_images` yields `1r,1v,...,6r,6v,8r,8v`, CUDL canvases expose `1r..6v`, so `page 0..11 -> canvas 0..11` and `page 12..13 -> NLI fallback` is the expected mapping.
- Using `(folio_num, side)` instead of positional `images_ext[page]` is the right abstraction for CUDL count mismatches and for cases with prepended `Binding`/`Cover` canvases.
- The plan is very explicit about the FGP-vs-FL rule. It repeats the prohibition in the objective, task steps, helper docstrings, and verification, which materially lowers the chance of executor drift.
- Cache-versioning on the server-side Cambridge cache is the right idea for in-process stale bytes after the positional->resolver cutover.
- The degraded path is thoughtfully separated from “true fallback”: `{'degraded': True}` vs `None` is a good contract.
- The SQLite fixture approach is reasonable and the plan correctly validates the important assumption about `NliCrossrefService.__init__`/`is_available()` before building tests around it.

## Concerns

- **HIGH**: The desktop “LEAVE ALONE” decision for the page-navigation path looks wrong. The real browse navigation code at `genizah_app.py:21010-21016` and `22496-22509` still recalculates indices with `_get_folio_image_index` against `viewer_images`. That is not just passive `set_page`; it actively chooses which image index to show after page changes. If only the two load sites are fixed, navigation from page 12 to 13/14 can still clamp back onto the CUDL list and miss the NLI fallback.
- **HIGH**: The suffix=`1` fallback is documented, but it is still a real latent correctness bug for multi-IE CUL manuscripts. On desktop, `current_browse_volume_ie` already exists, so hardcoding `1` there would be an unnecessary regression. On web, the limitation may be unavoidable for this quick fix, but it should be treated as a known functional gap, not just a documentation note.
- **MEDIUM**: `_CAMBRIDGE_CACHE_VERSION` only invalidates the server’s in-memory cache. It does not invalidate client/browser/CDN caches for the unchanged `/api/cambridge_image/{sys_id}?page={N}` URL, and responses are still `Cache-Control: public, max-age=600`. Users can still see stale wrong bytes for up to 10 minutes after deploy.
- **MEDIUM**: Task 2 is carrying too much: refactor `nli_image_by_sysid`, change Cambridge cache shape, add fallback semantics, add desktop helper, wire desktop load sites, and possibly add a new web test harness. That is a lot of moving parts for one “atomic” task in a quick fix.
- **MEDIUM**: The resolver depends on CUDL labels being parseable by `^\\s*(?:f\\.?\\s*)?(\\d+)\\s*([rv])?\\b`. That covers `1r`, `1v`, `1`, `f.2v`, and `f. 2v`, but not alternative notations like `1 recto`, `fol. 1r`, or `1a/1b`. That may be acceptable for current Cambridge data, but the plan assumes more label regularity than it proves.
- **LOW**: There is a small inconsistency in the test accounting. The plan describes “16 new tests” in one place, but the specified additions are 12 parametrized resolver cases + 4 edge cases + 4 `_parse_cudl_label` tests = 20 pytest cases.
- **LOW**: The synchronous `requests.get` usage is not newly introduced, but the inline NLI fallback adds an extra blocking network path to `cambridge_image`. It is probably acceptable for this quick fix, but it may worsen tail latency on fallback pages.

## Suggestions

- Expand desktop scope to include the browse page-navigation reindex path in `genizah_app.py:21010-21016`. That is the biggest gap.
- On desktop, do not accept suffix=`1` as a blanket limitation. Use `current_browse_volume_ie` when building NLI fallback URLs; the state already exists.
- On web, if suffix cannot be derived safely, consider a narrower fallback rule: only serve NLI fallback for clearly single-volume cases, or derive suffix from per-volume transcription offsets if that metadata is already available.
- Add one explicit resolver test for a canvas list with prepended non-folio labels like `Binding`, `Cover`, `1r`, `1v` to prove the matching remains stable.
- Add one explicit parser test for `"f. 1 r"` if the plan wants to claim that spacing variant is supported.
- Separate Task 2 into “web integration” and “desktop integration” in execution order, even if they remain in one plan document.
- If stale client caching matters operationally, temporarily reduce Cambridge response caching during rollout or add an HTTP validator/version header so deploy behavior is clearer.

## Risk Assessment

**Overall risk: MEDIUM**

The core resolver design is sound, and the plan is unusually disciplined about the FL-id source pitfall. The main risk is not the folio math; it is incomplete desktop coverage and the knowingly incorrect multi-IE fallback behavior. If those are left as written, the web fix will likely work for the target fixture, but the desktop fix may be partial and some CUL multi-volume manuscripts may still show the wrong NLI fallback image.

---

## Consensus Summary

### Agreed Strengths

- **Folio+side mapping is the correct abstraction** (both). Superior to positional indexing; handles CUDL count mismatches and prepended binding/cover canvases.
- **FGP-vs-FL guard is adequately hardened** (both). Pitfall 6 is repeated in objective, helper docstrings, must_haves truths, and verification checks — executor drift risk is low.
- **Cache-versioning with `_CAMBRIDGE_CACHE_VERSION = 2`** is the right approach for server-side cutover (both).
- **H3 retraction is a positive signal** (gemini) — forensic integrity.
- **Degraded vs true-fallback contract (`{'degraded': True}` vs `None`)** is well-separated (codex).

### Agreed Concerns

- **HIGH — Desktop navigation reindex path (codex, partially gemini):** Desktop browse navigation reindexes via `_get_folio_image_index` at `genizah_app.py:21010-21016` and `22496-22509`. Plan's "LEAVE ALONE" treatment of these may leave page 12→13 navigation still clamping to the CUDL list and missing the NLI fallback. **Must re-examine desktop scope before execution.**

- **HIGH — `suffix=1` hardcoding for multi-IE CUL shelfmarks (codex):** On desktop, `current_browse_volume_ie` already exists and should be used to derive the suffix. Documenting it as a 'known limitation' is acceptable for web, but desktop should do better since state is available. CUL has many multi-IE manuscripts — this is a real latent bug.

- **MEDIUM — Client/browser/CDN cache (codex):** `_CAMBRIDGE_CACHE_VERSION` only invalidates the server's in-memory cache. The response still returns `Cache-Control: public, max-age=600` on an unchanged URL, so browsers/CDNs serve stale wrong-image bytes for up to 10 min after deploy. Consider shorter Cache-Control during rollout or add an HTTP validator header.

- **MEDIUM — Task 2 scope (codex):** Too many moving parts in one task: `_fetch_nli_image_bytes` extraction, cache shape change, fallback semantics, desktop helper, desktop wiring. Consider splitting into "web integration" and "desktop integration" sub-tasks.

- **MEDIUM/LOW — CUDL label regex coverage:** Plan's `^\s*(?:f\.?\s*)?(\d+)\s*([rv])?\b` covers `1r`, `f.1v`, `f. 2v`, and bare numeric labels, but not `1 recto`, `fol. 1r`, `1a/1b`. May be acceptable for current CUL data but is an assumption, not a proof.

- **LOW — Test count inconsistency (codex):** Plan describes "16 new tests" in one place; actual additions count to 20 pytest cases (12 parametrized resolver + 4 edge + 4 label parser).

### Divergent Views

- **Overall risk**: Gemini says LOW ("approved for autonomous execution"). Codex says MEDIUM ("incomplete desktop coverage and knowingly incorrect multi-IE fallback"). Codex's deeper inspection of desktop call sites is the more rigorous read — treat MEDIUM as the operative assessment.

- **Cache-Control severity**: Gemini did not flag client-side caching. Codex flagged it as MEDIUM. Given deploy-time user impact, worth a belt-and-suspenders mitigation (e.g. drop max-age during deploy window, or include fix-version in response header).

