---
id: SEED-021
status: shipped
planted: 2026-06-23
planted_during: 2026-06-23 product-quality audit — created during Codex's seed review, which found these CLOUD-AUTO items were orphaned (parked in absent/decision-gated SEED-015/017). Register: .planning/audit-2026-06-23-product-quality/MASTER.md
trigger_when: CLOUD-AUTO. Gives single owners to web/api.py (NLI image observability/lock) + web/components/image_resolution.py (#37/#38) so they don't collide with SEED-013. Desktop polish (#42/#43) shares desktop/join_workbench.py with the LATER decision-gated SEED-017 → run SEED-021 BEFORE 017. NOT in round-1 parallel set (web/api.py is also in keystone SEED-015's scope — but 015 is blocked, so 021 lands its observability first and 015 builds on it).
scope: small (logging/lock + io_bound guardrail + 2 desktop viewer polish items; no behavior change beyond observability/parity)
---

# SEED-021: Image-fetch observability + desktop viewer polish

> Homes the CLOUD-AUTO findings Codex found orphaned in the seed review (`_tmp/codex-seed-review-output.md`).
> Distinct from the keystone SEED-015 (which DECIDES the image-loading unification architecture); this seed
> is only the safe, no-decision observability/guardrail/polish slice. It de-risks 015 by giving web/api.py
> image paths their logging/lock hygiene first.

## web/api.py — NLI image observability & lock hygiene

### #23 — NLI cache snapshot persisted outside the update lock (LOW-MED · EASY)
`web/api.py:741-745` updates `_nli_cache` inside `_nli_cache_lock`, releases, then calls
`_persist_positive_cache_snapshot()` (which re-locks for the copy at `:633-637`). Not an unlocked read, but
the update+snapshot pair is non-atomic.
**Fix:** hold the lock through the copy, or pass a copied snapshot from the update critical section into the
persistence writer.

### #36 — Image fetch non-200/429/5xx unlogged (LOW · EASY)
`web/api.py:851-871` (IIIF) and `:887-906` (Rosetta) handle 200/429/5xx then fall through other statuses
(404 etc.) with no log; final fallback `:914` returns 404.
**Fix:** low-noise debug log or structured counter for non-200/non-429/non-5xx statuses (esp. 404 + unexpected
3xx/4xx) so image-resolution failures are diagnosable.

## web/components/image_resolution.py — single owner (moved here from SEED-013)

### #37 — `resolve_external_images` warning lacks `exc_info` (LOW · 1LINE)
`web/components/image_resolution.py:295-296` → `logger.exception(...)` or add `exc_info=True`.

### #38 — `resolve_external_images` network I/O has no io_bound guardrail (LOW · EASY)
`image_resolution.py:253-258` (docstring requires `run.io_bound`), I/O at `:292-294`. Checked call sites
(`anchor_viewer.py:669-708`, `joins_lab.py:822-839`) currently comply, but the contract is comment-only.
**Fix:** add a runtime warning/assert when called on the event-loop thread, OR expose only an async wrapper
that performs the io_bound hop. (One owner for this file avoids the SEED-013 #37 collision Codex flagged.)

## Desktop viewer polish (shares desktop/join_workbench.py with later SEED-017 → do first)

### #42 — Desktop Compare lacks a zoom-% label (LOW · EASY)
Web shows it (`anchor_viewer.py:613-615`, updated at `:414-415`); desktop Compare adds zoom -/+ but no label
(`desktop/join_workbench.py:3861-3871`). **Fix:** add a small zoom-% label updated on zoom change.

### #43 — Loading affordance: web skeleton vs desktop text (LOW · EASY)
Web animated skeleton (`anchor_viewer.py:124-131`, shown `:685-686`) vs desktop `tr("loading...")` text
(`desktop/join_workbench.py:2079`). **Fix:** lightweight placeholder/spinner on desktop for parity. (Polish —
defer if not worth the churn before SEED-017 lands the bigger viewer parity work.)

## Tests required
- web/api.py: caplog assertions for the new image-status logging (#36); a test that the snapshot reflects a
  consistent view under the lock (#23).
- image_resolution.py: `exc_info` present on the warning (#37); io_bound guardrail fires when called on the
  loop thread (#38).
- Desktop #42/#43 are GUI polish → source/construct tests where feasible; otherwise manual.
- ⚠ Run `tests/test_image_resolution.py` and any web/api NLI-cache tests after edits.

## Done when
web/api.py image paths log unexpected statuses + snapshot is lock-consistent; image_resolution.py has one
owner with exc_info + io_bound guardrail; desktop Compare shows zoom-% + parity loading; tests green, ruff
clean. (Leaves the image-loading *unification* architecture entirely to SEED-015.)
