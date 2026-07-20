# Phase 133: Visual Atlas Preview (early quick win) - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-07-20
**Phase:** 133-Visual Atlas Preview (early quick win)
**Areas discussed:** Primary graph object (ATLAS-02), Claim-free content & masking, Interactivity & byte budget, Page framing & beta labeling

---

## Primary graph object (ATLAS-02)

| Option | Description | Selected |
|--------|-------------|----------|
| Manuscripts (stars) in algorithmic regions | Star = one connected sys_id; communities = regions; claim-free; matches ATLAS-01 | ✓ |
| Works (nodes) as primary | Work nodes assert work-witness identification = claim-level; disallowed in preview | |
| Hybrid (manuscripts primary + work-hub regions) | Manuscripts as stars, work identity shapes regions; risks leaking work-witness implications | |

**User's choice:** Manuscripts (stars) in algorithmic regions — the durable primary object; work-lens deferred.

| Option | Description | Selected |
|--------|-------------|----------|
| Domain color + split the giant (prototype) | FJMS domain default + library toggle; recursively split the 43K liturgical giant; regions framed as algorithmic | ✓ |
| Domain color, keep giant intact | Same coloring but show the giant as one region (Codex: illegible blob) | |
| Library color as default | Color by library instead of domain (Codex recommends domain default) | |

**User's choice:** Domain color + split the giant (prototype).

---

## Claim-free content & masking

| Option | Description | Selected |
|--------|-------------|----------|
| Remove discovery overlay entirely | Per-MS discovery highlighting = claim-level; strip overlay/toggle/counts | ✓ |
| Keep as a neutral aggregate only | Drop per-MS gold stars, keep a corpus-level number; still edges toward claim territory | |
| Keep the overlay | Violates ATLAS-01 no-claim constraint | |

**User's choice:** Remove entirely for the preview.

| Option | Description | Selected |
|--------|-------------|----------|
| Domain region labels + safe per-star tooltips | Regions by domain + size; drop catalogue-title-as-region-label; tooltips keep shelfmark/domain/library | |
| Domain labels only, no per-star text | Only region domain labels; no shelfmarks/tooltips | |
| Keep catalogue titles as region labels | Keep the representative catalogue title per region | ✓ |

**User's choice:** Keep catalogue titles as region labels.
**Notes:** Follow-up asked how to reconcile with SC#1 ("work labels only from reviewed neutral titles or omitted"). Owner then chose **"Keep titles as region labels, no special framing"** (over "representative cluster labels + banner" and "titles in tooltip only"). Recorded as a deliberate, informed decision under the ATLAS-PREVIEW exception — catalogue titles are our own data (masking-safe) and SC#1 permits cluster-level visualization.

| Option | Description | Selected |
|--------|-------------|----------|
| Reusable script + scan committed repo too | Forerunner of DATA-05; scans asset + rendered output + committed repo | ✓ |
| Reusable script, asset only | Same asset scan, defer committed-repo scan to Phase 134 | |
| One-time manual check | Manual verify this phase; build the guard in Phase 134 | |

**User's choice:** Reusable script + scan committed repo too.

---

## Interactivity & byte budget

| Option | Description | Selected |
|--------|-------------|----------|
| Full interactive prototype | Zoom/pan, search, color toggle, library filter, focus-constellation, click-through to /browse, intro | ✓ |
| Leaner: zoom/pan + search + labels | Drop focus drill, library filter, color toggle | |
| Static image + cluster cards | Pre-rendered image + cards (kept as the no-WebGL fallback, not the main experience) | |

**User's choice:** Full interactive prototype.

| Option | Description | Selected |
|--------|-------------|----------|
| 62K stars + aggregate flows, target ~4 MB | All stars + baked flows; per-MS edges on deep zoom; ~4 MB | |
| 62K stars + all edges (~13 MB, compressed) | Embed every MS-MS edge; heavy load / CLS risk | |
| Reduced subset (~1.5 MB, fewer stars) | Rehearsal-style smaller set | |
| 62K connected + faint catalogue dust | Add ~193K unconnected as dust; much larger | |
| **(Other)** Explain option 1 — wants per-MS info | User asked for clarification, wanting per-MS info included | ✓ |

**User's choice:** Chose "Other" to ask for clarification (wanted per-MS info retained). Clarified in prose that per-MS **node** info is kept in all options; only the per-MS **edge** hairball is deferred. Refined follow-up below.

| Option | Description | Selected |
|--------|-------------|----------|
| Keep all per-MS edges on zoom, optimize + Brotli (~6 MB cap) | Embed full edge set (drawn on zoom/focus), typed/delta + Brotli, generous ~6 MB beta cap | ✓ |
| Per-MS edges only inside a focused region, ~4 MB | Overview = stars + flows; per-region edge companion files | |
| Embed everything, minimal optimization (~13 MB) | Ship prototype payload with Brotli only | |

**User's choice:** Keep all per-MS edges on zoom, optimize + Brotli (~6 MB cap).

---

## Page framing & beta labeling

| Option | Description | Selected |
|--------|-------------|----------|
| Beta-tagged nav link, no homepage band | Nav link now; homepage waits for ATLAS-03/Phase 139 | |
| Direct-URL only | /atlas reachable but unlinked during beta | |
| Nav link + homepage teaser | Nav link + homepage teaser (homepage band is Phase 139/REL-01-gated) | ✓ |

**User's choice:** Nav link + homepage teaser — flagged as colliding with the ATLAS-03/REL-01 gate; reconciled below.

| Option | Description | Selected |
|--------|-------------|----------|
| Follow site language (EN/HE + RTL) | Page chrome follows the site's active UI language; baked labels bilingual | ✓ |
| Standalone EN/HE toggle on the page | Independent language toggle on the atlas page | |
| HE-primary only for the beta | Ship HE-primary now, EN later | |

**User's choice:** Follow site language (EN/HE + RTL).

| Option | Description | Selected |
|--------|-------------|----------|
| Beta badge + short intro line + honesty banner | Badge + one-line intro + algorithmic-provenance banner | ✓ |
| Honesty banner only, minimal beta mark | Small beta tag + banner | |
| Full disclaimer interstitial before entry | Dismissible disclaimer screen before load | |

**User's choice:** Beta badge + short intro line + honesty banner.

| Option | Description | Selected |
|--------|-------------|----------|
| Defer teaser to Phase 139; ship nav link now | Respect the locked gate, no requirements change | |
| Extend the exception to allow a claim-free teaser now | Revise ATLAS-03/REL-01; claim-free CLS-safe teaser, noindex until REL-01 | ✓ |
| Nav link only, no teaser, no change | Just the beta nav link | |

**User's choice:** Extend the exception to allow a claim-free teaser now.
**Notes:** Deliberate requirements revision (owner-authorized). Captured as locked in CONTEXT.md D-16 with a "Requirements Impact" note; the REQUIREMENTS.md (ATLAS-03 + REL-01) and ROADMAP.md (Phase 133/139) sync is flagged as the top follow-up rather than edited silently during discuss.

---

## Claude's Discretion
- Clustering algorithm (Louvain vs Codex-recommended Leiden), force-layout params, edge encoding format, intro choreography, no-WebGL/asset-absent fallback (prototype is Canvas 2D → broadly compatible). Deterministic seed + algorithm/version metadata recorded with the bake for reproducibility.

## Deferred Ideas
- Faint catalogue "dust" (~193K unconnected) for a literal whole-Genizah view — later add-on.
- Sigma.js v3 + binary multilevel assets for the server-bounded drill-down explorer — Phase 139 (ATLAS-02).
- Work-lens (bipartite manuscripts↔works) secondary view — future.
- Per-region companion edge chunks — optimization if the ~6 MB cap proves tight.
