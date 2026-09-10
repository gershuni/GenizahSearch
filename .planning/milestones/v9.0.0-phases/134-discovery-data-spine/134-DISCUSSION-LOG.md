# Phase 134: Discovery Data Spine - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-07-21
**Phase:** 134-Discovery Data Spine
**Areas discussed:** R-source (4th reference corpus, owner-raised deviation), Shown work-set scope, Neutral-title curation workflow, Band selection at launch, Relation-vocab display wording

---

## R-source — a newly-acquired 4th reference corpus (owner-raised mid-discussion)

Owner introduced a 4th reference database ("R-source", masked like M-source; ~6 GB / ~1,679 Hebrew text files), flagging overlaps + "many later sources we don't expect in the Genizah." Analysis confirmed ~86% of R-source is post-Genizah (responsa, Shulchan-Aruch tradition, Rishonim→Acharonim commentators, Hasidut, modern encyclopedias); ~14% is in-horizon literary.

| Option | Description | Selected |
|--------|-------------|----------|
| Defer ingest, prep now | v9.0 spine on Sefaria+JA+M-source; mask R-source + carry a masked source_corpus column so a gen-2 refresh drops in cleanly | ✓ |
| Selective date-gated ingest into v9.0 | Bring in only R-source's classical/early strata; needs a research cycle + frame re-freeze before the certificate | |
| Full ingest into v9.0 | Match all of R-source (advised against — floods spurious same-work claims, endangers the certificate) | |

**User's choice:** Defer, prep now.
**Notes:** Owner will run all R-source research **in parallel** with the milestone so it does not interfere with completion, aiming for a relatively quick gen-2 DB release afterward. → sidecar must be built source-extensible (versioned rebuild, not migration). Owner requested a recommended prompt for the parallel R-source session (delivered separately).

## Shown work-set scope

| Option | Description | Selected |
|--------|-------------|----------|
| Include M-source works (masked) | Show M-source-derived works with reviewed neutral titles + masking | ✓ (refined) |
| Open-corpora only at launch | Show only Sefaria + JA works; defer all M-source works | |

**User's choice:** Include M-source works — **refined**: NOT piyyut and documentary, but the M-source works that more resemble the Sefaria/JA corpus (mostly large literary works).
**Notes:** Clean line — large literary works have obvious neutral titles + low masking sensitivity; piyyut + documentary are a curation nightmare + highest masking risk → both deferred to the fast-follow / gen-2 track with R-source.

## Neutral-title curation workflow

| Option | Description | Selected |
|--------|-------------|----------|
| Auto-adopt open + review restricted | Auto-adopt Sefaria/JA canonical titles + light spot-check; full manual owner review on the M-source literary subset; generated review artifact, source masked, fail-closed | ✓ |
| Full manual review of every work | Owner reviews a neutral title for every shipped work regardless of source | |

**User's choice:** Auto-adopt open + review restricted.
**Notes:** Unreviewed = excluded (no research-title fallback). Artifact modeled on the existing translation-audit-sample pattern.

## Band selection at launch

| Option | Description | Selected |
|--------|-------------|----------|
| All four; canon caveated | expert_verified + tier_a default; screening_rb + screening_canon behind toggle; canon lane separately caveated (Targum-confusion); row counts trimmed to ≤300 MB | ✓ |
| High-confidence only at launch | Ship only expert_verified + tier_a; defer screening lanes + Leads queue | |

**User's choice:** All four; canon caveated.
**Notes:** Matches the roadmap (Phase 138 Leads = R-B/canon).

## Relation-vocab display wording

| Option | Description | Selected |
|--------|-------------|----------|
| Defer to display phase (135/136) | claim_type stored as a stable code; human-facing labels render where shown | ✓ |
| Lock the labels now | Capture preferred EN/HE wording now | |

**User's choice:** Defer to display phase (135/136).
**Notes:** Sidecar never blocks on wording.

---

## Claude's Discretion
- Exact per-query timeouts / bounded-concurrency / LRU sizing / pagination page sizes (DATA-06) vs PERF-01 caps.
- Overload + fail-open user-facing copy.
- `discovery.db` table/index layout; `claim_id`/`unit_id` hashing implementation; schema-versioned filename scheme.
- Whether the DATA-05 guard extension reuses `scripts/check_atlas_masking.py` wholesale or factors a shared core.
- Operationalizing the M-source "large literary works" filter (researcher investigates genre/size metadata; owner hand-picks via the review artifact).

## Deferred Ideas
- R-source ingest → parallel research track → gen-2 sidecar refresh (FUT-04).
- M-source piyyut + documentary works → fast-follow / gen-2.
- Relation-vocab bilingual wording → Phases 135/136.
- All downstream discovery surfaces (135–139).
- FUT-01..08.
</content>
