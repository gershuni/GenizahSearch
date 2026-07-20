# Codex critique — v9.0.0 Discovery milestone REQUIREMENTS draft

**Date:** 2026-07-19 · **Reviewer:** Codex (`codex exec`, brief at
`_tmp/codex-v9-requirements-brief.md`) · **Verdict: REWORK**

> The product direction is sound, but the draft is not yet execution-ready.
> Several release-blocking contracts remain implicit.

1. **[HIGH] The canonical "claim" is undefined.** DATA-01 stores page-level identifications, while WORK and JUDGE operate on manuscript–work claims and PANEL-02 exposes MS–MS edges. Define stable `claim_id`, `claim_type`, `sys_id`, `work_id`, supporting page IDs, sidecar version, and exact page→witness aggregation rules. Otherwise per-witness bands, deduplication, judgments, and rebuild continuity are ambiguous.

2. **[HIGH] DATA-03/04/05 do not fully enforce masking.** A banned-string scan cannot detect arbitrary reference text, unknown sigla, or provenance encoded in raw work IDs. Require an allowlisted sidecar schema, opaque product work IDs, a complete reviewed title map with no fallback to research titles, and validation of every displayable field—including author and genre. Scan schema and all cell values, but also test atlas assets, internal JSON payloads, clipboard output, SEO/JSON-LD, sitemap, exports, Supabase claim payloads, and user-facing errors. Community annotations create another leak vector: either block/moderate forbidden provenance before publication or do not render notes publicly.

3. **[HIGH] PANEL-03 is unsupported by the proposed data spine.** Identification rows have offsets into one manuscript's HTR, but aggregated MS–MS edges carry no paired evidence offsets. Specify whether evidence applies only to a work–witness claim or also to related-MS edges. For the latter, add compact paired evidence records or explicitly derive each manuscript's independent work span. Store the HTR snapshot/hash used for offsets, validate offsets at render time, and fail closed when text versions drift.

4. **[HIGH] The certification requirement is not a sufficient pre-registration.** CERT-01 must define the estimand and unit—page identification or deduplicated manuscript–work claim—the exact frozen eligible-frame hash, mutually exclusive strata and weights, sampling seed, blindness procedure, treatment of existing gold, exclusions/indeterminates, confidence interval, pass/fail gates, and copy for every outcome. Sampling cannot begin against a moving distillation: preparation may run in parallel, but the frame must freeze before cards are drawn.

5. **[HIGH] Tier-A and band membership are underspecified.** The draft does not reconcile 275,894 tier-A rows with the smaller R-A/R-B/R-CANON frames, nor say whether every shipped identification has exactly one band. DATA-01 needs explicit inclusion sets, expected counts, uniqueness invariants, and rules for multiple pages or bands supporting one witness. Precision belongs to a certificate/band snapshot, not as an apparent per-row probability.

6. **[HIGH] DATA-02 omits the actual production-safety controls.** "Off the event loop" is necessary but insufficient. Require one async web adapter/chokepoint, per-query timeouts, bounded concurrency with overload behavior, indexed bounded queries, LRU caching for browse enrichment, and server-side pagination. Add acceptance budgets for browse latency regression, atlas payload size, sidecar RSS, and failure behavior when queries are slow.

7. **[HIGH] JUDGE-01 is not enforceably append-only.** Make immutability a database rule: authenticated users may insert, but not update/delete historical rows; changing a judgment creates a superseding event. Persist claim ID, claim type, sidecar version, and band shown at judgment time. Add role-matrix tests for RLS and GRANTs, rate limits, annotation length/escaping, moderation status and admin hide path, explicit disagreement counts, and exclusion of hidden/spam rows from aggregates. JUDGE-04 must state that community signals never affect band, precision, rank, or certified styling.

8. **[HIGH] ATLAS-01 is compound and internally unclear.** A whole-corpus atlas and a bounded interactive explorer are two deliverables, while "full graph never ships" conflicts with a 52K-node static starfield unless "full graph" means raw edges. Split them: an offline, canon-masked aggregated overview; and a server-bounded drill-down with explicit node, edge, byte, and hop caps. State the graph's primary object—works, manuscripts, or clusters—and prohibit request-time layout and client-side full-edge loading.

9. **[MED] The sidecar release contract is incomplete.** Add schema version compatibility, source DB hash, build timestamp/data-as-of date, expected row counts, uniqueness and referential-integrity checks, `PRAGMA integrity_check`, disk/RSS budgets, and startup rejection of corrupt or incompatible files. Deployment must upload to a temporary filename, verify it, atomically rename it, then deploy code, with rollback documented. "scp-first" alone does not protect against a partial file.

10. **[MED] BAND-01 accidentally reintroduces deferred scope.** Public APIs are explicitly deferred, and no export feature is otherwise required. Replace "UI, copy, export, API" with "every serializer or copy/export path actually shipped in v9." Either define specific work/leads exports with acceptance tests or state that v9 adds none. Do not implement discovery API endpoints merely to satisfy BAND-01.

11. **[MED] The public certificate surface is missing.** CERT-02 says copy uses measured numbers, but no requirement says where users can inspect the methodology. Add a bilingual confidence/methods page containing population, unit, sample size, strata, weighted estimate, interval, date, grader/audit status, and immutable report identifier; every band tooltip should link to it.

12. **[MED] SEO and indexing policy are absent.** Add canonical URLs, EN/HE `hreflang`, titles/descriptions, and sitemap inclusion for approved `/work/{id}` pages and the atlas. Keep `/leads` and screening-toggle states `noindex`. JSON-LD should contain only neutral work metadata; do not serialize algorithmic work–witness associations unless their uncertainty can travel inseparably. These outputs belong in the masking gate.

13. **[MED] Claim semantics are too loose.** "Related," "identified as," and "same-work carriers/edges" can conflate copies, citations/indirect witnesses, textual parallels, and physical joins. Define the allowed bilingual relation vocabulary and mapping from `flank_class`. Never use join language here. LEADS-01 must separately deprioritize/caveat the canon lane and the known Targum confusion, not merely call R-CANON "weakest."

14. **[MED] WORK-01 and LEADS-01 lack scale behavior.** "All carrier MSS" should be "all identified carriers in this dated snapshot." Require server-side pagination, deterministic sorting, counts, filter semantics, maximum response size, and preserved band labels on every row. WORK-02 also needs defined bilingual title/alias normalization and duplicate-title handling.

15. **[MED] I18N-01 is too broad and accessibility is missing.** Split translation completeness, RTL layout, and graph bidi handling into separately testable requirements with HE render-smokes. Add textual/table equivalents for graph information, keyboard navigation, screen-reader labels, contrast checks, reduced-motion behavior, and the rule that color is never the only confidence signal.

16. **[MED] Analytics and operational observability are missing.** Add privacy-allowlisted PostHog events for panel impressions, lead-toggle use, evidence opens, work/atlas navigation, and judgment completion—without titles, manuscript text, shelfmarks, annotations, or raw research IDs. Operational metrics should cover timeouts, truncation, unavailable/incompatible sidecars, atlas payloads, and judgment rate-limit/moderation activity.

17. **[MED] The release ordering needs explicit gates.** Safe order is: finalize claim model and masked schema → build and validate the sidecar/title map → freeze the shipped population and draw certificate cards → panel/work surfaces → Supabase migration and security smoke before judgment UI → leads → bounded atlas → homepage promotion. Homepage/sitemap/SEO must remain disabled until masking, certificate-copy, RTL render, performance, and sidecar deployment gates pass.

18. **[LOW] Trim scope inside the fixed five surfaces.** For v9, make the homepage a CLS-safe static promotion rather than live suggestions or graph work; keep the atlas to an aggregated overview plus one bounded drill-down; and capture annotations without publicly displaying free text until moderation is operational. Public API, new generalized exports, live full-graph interactivity, and multi-hop exploration should remain Future.

**Final verdict: REWORK**

---

*Disposition (orchestrator, 2026-07-19): ALL 18 findings accepted and folded
into the revised requirements draft (REQUIREMENTS.md). #8's "primary graph
object" and #13's bilingual relation vocabulary are flagged as discuss-phase
UX decisions. The prior content of this file (2026-06-14 v8.1.0 telemetry
critique) was superseded; recover from git history if needed.*
