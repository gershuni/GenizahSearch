---
id: SEED-010
status: dormant
planted: 2026-06-21
planted_during: v8.2.0 / Phase 121 (i18n-polish) HE-mode RTL HUMAN-UAT (Plan 121-03)
trigger_when: A dedicated debug/fix pass on a cloud branch off origin (per memory feedback_seed_midphase_fixes_to_cloud). NOT i18n — do NOT fold into the i18n-polish phase. Needs live testing with NLI both UP and DOWN, so it cannot be validated during the current outage. Candidate for a small standalone phase or /gsd:debug once NLI is reachable again.
scope: medium (cross-cutting — 3 divergent image-resolution paths + a zoom-init coupling; standalone, not an inline fix)
---

> **ROUTING:** Functional image-resolution + zoom bug surfaced during the 2026-06-21 live HE-mode RTL
> UAT of the Web Joins Lab. It is **language-independent** (reproduces in English too), so it is OUT OF
> SCOPE for the Phase 121 i18n-polish phase. Logged + seeded per Plan 121-03's "larger → log/seed"
> policy and memory `feedback_seed_midphase_fixes_to_cloud`. The i18n/RTL SC#2 sign-off is orthogonal
> and can proceed independently of this bug.

# SEED-010: Web Joins Lab — per-provider image resolution diverges across anchor / grid / Compare; zoom dead when image fails to load

> Captured as a seed (NOT fixed inline). Observed while NLI's image API was DOWN, which exposed the
> divergence between three image-resolution paths that each handle providers differently.

## Symptom (observed 2026-06-21, web Joins Lab, NLI image API DOWN)

- **Oxford:** thumbnails ARE shown in the candidate **grid**, but full images are NOT shown in the
  **main anchor** pane nor in **Compare**.
- **CUDL / Cambridge:** images ARE shown in **Compare**, but NOT in the **grid** nor in the **main anchor**.
- **Zoom controls** do NOT work, at least for CUDL images.

## Root cause — three divergent image-resolution paths + a zoom-init coupling

The three surfaces each resolve images differently, with inconsistent provider coverage:

| Surface | Resolver | Provider coverage |
|---------|----------|-------------------|
| **Candidate grid** | `candidate_grid.py::build_thumbnail_url()` (`web/components/candidate_grid.py:397-440`) | Oxford has its own fork (`:424-436`); **ALL other providers (Cambridge/CUDL, Manchester, JTS) are hardcoded to the NLI proxy** `/api/nli_image_by_sysid/...&width=300` (`:440`). So CUDL thumbnails 404 when NLI is down; Oxford survives. |
| **Main anchor** | `anchor_viewer.py::_resolve_off_loop()` → `image_resolution.py::resolve_image_url()` (`web/components/image_resolution.py:47-216`) | Oxford **auto-defaults immediately** (`:162-163`) → works. Cambridge auto-default is **verdict-gated** (`:172-185`): only defaults to `cambridge` when the CUDL alignment verdict is `aligned` (or page count matches). With no/`misaligned` verdict, `active_source` stays `nli` → fails when NLI is down. |
| **Compare modal** | `compare_modal.py::_fill_candidate()` (`web/components/compare_modal.py:394-520`) builds a **fresh `AnchorViewer` per candidate**, which re-runs `resolve_external_images()` + `resolve_image_url()`. | Same resolver as the anchor pane, but Compare re-evaluates per flip, so CUDL shows **when the verdict is aligned**. It does NOT carry forward the grid card's/user's chosen `active_source` (no `source_user_override`), so it can still re-default to NLI. |

**Why zoom is dead for CUDL:** zoom init is coupled to image load. `_build_img_html()`
(`anchor_viewer.py:449-470`) emits `<img ... onload="if(window.manuscriptViewer) window.manuscriptViewer.init()">`.
The pan/zoom/wheel handlers are wired only inside `manuscriptViewer.init()`
(`web/static/manuscript_viewer.js:205-238`). When the image URL 404s (NLI down, or CUDL routed to the
NLI proxy), `onload` never fires → `init()` never runs → the zoom buttons (`anchor_viewer.py:347-384`,
server-side `_zoom` state pushed via `ui.run_javascript`) click but do nothing. So "zoom doesn't work
for CUDL" is **downstream** of the image failing to resolve, not a separate zoom bug — though the
`onload`-only coupling is itself fragile and worth fixing.

## Per-surface × per-provider matrix (what each cell resolves to)

| Surface | NLI | Oxford | CUDL/Cambridge | Manchester | JTS |
|---------|-----|--------|----------------|------------|-----|
| Anchor | `/api/nli_image_by_sysid` | direct Bodleian / `/api/oxford_image` | `/api/cambridge_image` **only if verdict aligned**, else stays NLI | `/api/manchester_image` | `/api/jts_image` |
| Grid thumb | `/api/nli_image_by_sysid&width=300` | direct Bodleian / `/api/oxford_image` | **`/api/nli_image_by_sysid` (WRONG)** | `/api/nli_image_by_sysid` | `/api/nli_image_by_sysid` |
| Compare | via fresh AnchorViewer (= Anchor row) | = Anchor | = Anchor (re-evaluated per flip) | = Anchor | = Anchor |

## Scope of the fix (unify the three paths + decouple zoom-init)

1. **Grid provider-awareness (primary):** `build_thumbnail_url()` (`candidate_grid.py:440`) must route
   Cambridge/Manchester/JTS to their own proxies (`/api/cambridge_image` etc.) instead of the NLI proxy.
   Either thread `external_provider` into card rendering, or call `resolve_external_images()` during card
   creation (heavier; mirrors Compare). Ideally route ALL three surfaces through a single shared resolver.
2. **Cambridge auto-default when NLI is down:** relax the verdict gate in
   `image_resolution.py:156-185` — when the Phase-98 NLI circuit breaker is OPEN
   (`shared/nli_circuit_breaker.py`), auto-default CUDL to `cambridge` even without an `aligned` verdict
   (pass breaker state into `resolve_image_url()`).
3. **Compare provider preservation:** store the resolved `active_source` in Compare state and pass it as
   `source_user_override=True` to the fresh `AnchorViewer` (`compare_modal.py:394-520`) so it doesn't
   re-default to NLI.
4. **Zoom-init robustness:** decouple `manuscriptViewer.init()` from `<img> onload` (or also call it on
   the placeholder/error path) so the controls are live even when an image fails — and/or ensure a working
   provider URL is always chosen (items 1-3 largely subsume this).

## Best fix locations (file:line)

- `web/components/candidate_grid.py:397-440` — `build_thumbnail_url()` (NLI-proxy hardcode at `:440`)
- `web/components/image_resolution.py:47-216` — `resolve_image_url()` router; auto-default gate `:151-185`; provider overrides `:188-210`
- `web/components/anchor_viewer.py:390-443` (`_resolve_off_loop`), `:449-470` (`_build_img_html` onload coupling), `:347-384` (server-side zoom state)
- `web/components/compare_modal.py:394-520` — `_fill_candidate()` fresh-viewer creation
- `web/static/manuscript_viewer.js:205-238` (`init()`), `:281-288` (`applyTransform`)
- `web/api.py` proxies: `/api/nli_image_by_sysid` (`:1012-1035`, breaker check `:1027-1029`), `/api/cambridge_image` (`:1054-1211`, NLI-fallback `:1156-1177`), `/api/manchester_image` (`:1216-1268`), `/api/jts_image` (`:1273-1325`), `/api/oxford_image` (`:1335-1452`)
- Phase-98 breaker: `shared/nli_circuit_breaker.py`

## Notes / pointers

- The deeper design issue is **three resolvers instead of one**. The durable fix is a single shared
  image-URL resolver used by grid, anchor, and Compare, with provider selection that is breaker-aware.
- **Must be tested with NLI both UP and DOWN** — the bug only manifests when NLI is down (the grid's
  NLI-proxy hardcode is masked when NLI is up because the NLI proxy itself falls back to other providers
  server-side; `/api/cambridge_image` has its own NLI fallback at `:1156-1177`). Cannot be validated
  during the current outage.
- Related deferred image UAT items already tracked: `119-HUMAN-UAT.md` R2-3 (Compare image pane layout),
  R2-8 (transcription-start for VS-only). This seed is the resolution-correctness layer beneath those.
