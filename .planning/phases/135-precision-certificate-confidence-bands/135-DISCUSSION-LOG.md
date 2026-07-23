# Phase 135: Precision Certificate & Confidence Bands - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-07-23
**Phase:** 135-precision-certificate-confidence-bands
**Areas discussed:** Scope & census sequencing, CERT-01 unit + certification posture, CERT-01 gate + strata + harness, Methods page + open wordings

---

## Scope & census sequencing

### Structure under the census blocker
| Option | Description | Selected |
|--------|-------------|----------|
| Split: build the unblocked work now | Track A (display code + methods page + written protocol) now; Track B (v2 bake + cards + deploy) fires when census lands | ✓ |
| Hard-gate: wait for the census | Don't open Phase 135 until the census is delivered | |

### Finish line
| Option | Description | Selected |
|--------|-------------|----------|
| When grading has STARTED | Phase closes once Track A live + protocol pre-registered + v2 baked/deployed + cards drawn + grading begun; grading completes in parallel (matches roadmap) | ✓ |
| When grading is COMPLETE | Phase stays open until grading done + certificate published | |

### Census dependency treatment
| Option | Description | Selected |
|--------|-------------|----------|
| Accept as critical path, note it | Record census as milestone critical path; Track A ships independently; no special mitigation | ✓ |
| Add a fallback plan | Define a minimal partial-census bake if the census slips | |

**Notes:** Census gates v2 AND Phase 136 (read surfaces need corrected v2 data). v2 prod deploy is a human-approved, asset-first, deploy-once checkpoint (133-06 pattern). **The census was then delivered mid-discussion — see the addendum below; the blocker is lifted.**

---

## CERT-01 unit + certification posture

### Estimand unit
| Option | Description | Selected |
|--------|-------------|----------|
| Manuscript × work (witness unit) | One card = does this manuscript witness this work; matches /work display + E1 clustering + the requirement wording | |
| Page × work (routed unit) | One card = does this folio-side witness this work; matches coverage routing + §3.1 validation | |
| Both: grade MS, report page secondary | Primary = MS, secondary = page descriptive | |

**User's choice (free text):** "mss can contain many identifications in several pages (esp. when it comes to citations), so grading pages is more precise (but keep in mind that several identifications can be true even for one page)."
**Notes:** Chose **page × work**, overriding the requirement's "manuscript–work" wording. Multi-register preserved (several true claims per page, each judged on its own). CI still clustered by physical MS (pages not independent).

### Certification posture
| Option | Description | Selected |
|--------|-------------|----------|
| Expert-measured, audit pending | Number + CI + "expert-measured · independent audit pending" (R-A parity); rows stay unreviewed; never "certified" | ✓ |
| Your grading = certified | Label the band "certified" | |

---

## CERT-01 gate + strata + harness

### Pass floor
| Option | Description | Selected |
|--------|-------------|----------|
| Strict — lower bound ≥ 0.85 | High bar matching "high-confidence"; demotion risk if tier_a can't clear it | ✓ |
| Balanced — lower bound ≥ 0.75 | Solidly-better-than-coin; more likely to pass; weaker "high-confidence" claim | |
| Broad — lower bound ≥ 0.60 | Almost certainly passes; too low for the default band | |

### Strata
| Option | Description | Selected |
|--------|-------------|----------|
| Corpus × coverage; category as diagnostic | Strata = corpus × coverage band, weighted; category = descriptive cut only (genre unreliable) | ✓ |
| Also make category a real stratum | Add category as a weighting dimension (rides error-prone genre) | |
| Flat / uniform (no strata) | Simple random sample; wastes power, can't localize failures | |

### Harness
| Option | Description | Selected |
|--------|-------------|----------|
| Reuse the E1/Q2 harness as-is | Point existing deck-builder + analyzer at v2 tier_a; standard blind/gold/bootstrap/OC/freeze-manifest kit | ✓ |
| Build fresh production tooling | New CERT-01-specific tooling | |

---

## Methods page + open wordings

### Methods page placement
| Option | Description | Selected |
|--------|-------------|----------|
| Dedicated /discovery/methods page | Standalone bilingual route with per-band anchors + content-hashed report id | |
| Section inside the existing Help page | Add a Confidence & methods section to the current Help page | ✓ |
| Tie it to the atlas (/atlas/methods) | Nest under atlas beta | |

**Notes:** Help-page section chosen; per-band anchors for tooltip deep-links; flag-gated + noindex until REL-01; content-hashed versioned report id carried forward.

### "Show more" toggle wording
| Option | Description | Selected |
|--------|-------------|----------|
| Show more possible identifications | EN literal from BAND-03 (זיהויים) | |
| Show more possible matches | EN "possible matches" (התאמות אפשריות נוספות) | ✓ |
| Show additional / lower-confidence finds | Makes the confidence gradient explicit | |

### Recall-honesty disclaimer base sentence
| Option | Description | Selected |
|--------|-------------|----------|
| Absence isn't evidence of absence | "No identification shown does not mean none exists — coverage is incomplete and computed" | |
| Computed suggestions, may be incomplete | "These are computed suggestions; a manuscript may have identifications we haven't found" | |
| Short form | "Not exhaustive — more identifications may exist." / "אינו ממצה — ייתכנו זיהויים נוספים." | ✓ |

---

## Census / canonical-merge handoff (delivered mid-discussion)

In response to the "ready for context?" prompt, the owner reported the twin census + canonical-merge decisions were delivered by the parallel SEED-029 session as an **owner-ratified handoff artifact** (`same_work_spike/probe/rsource/data/v2_canonical_merges.json` + `.md`): 16 merges (canonical = Sefaria id; incl. RCh ×12 tractates), 1 ratified `part_of` (Haggadah in MT Sefer Zmanim), 1 contested (Hai / RCh Shabbat), 174 provisional relations, 8 residuals, and a `dropped_by_135` list (the w001239 drop). Supersedes the v2-bake-plan's 7-merge/3-relation draft.

### 174 provisional relations
| Option | Description | Selected |
|--------|-------------|----------|
| Ship only the 1 ratified relation | Load only the owner-ratified part_of; record the 174 as available-but-not-loaded | |
| Load all 174, tagged provisional | Load all 174 flagged provisional, behind the show-more toggle | |
| You ratify a batch now | Owner reviews the 174 and ratifies the good ones before the bake | ✓ |

**Notes:** Three return-questions the parallel session posed back to 135 (output-shape mapping, filter-merges-to-1,270, ref_corpus_v2.pkl stability) are to be resolved in the v2-bake-plan update + its Codex review. RCh-Shabbat three-way auto-resolution (canonical flips to M-source w000452 so the drop doesn't orphan it) captured. Artifact is masking-sensitive (`title_msource`) — consumed by the build, never rendered/committed.

## Claude's Discretion

- Band-label values module = hand-authored + guard test (vs auto-parsing the markdown); numbers/status stay data-driven from the sidecar `band_precision` table.
- The (B) enum rename (`expert_verified` → `high_confidence_algorithmic`) rides the v2 bake in lockstep (7 files); display layer maps both v1 and v2 keys → the same label.

## Deferred Ideas

- Independent audit of tier_a + R-A → FUT-07.
- Lever 2 (direction-aware ref-subspan router; high-coverage quoted-works residual) → v2.1.
- gen2_workid_registry.json fold-in → parallel session's #17 mask rebuild.
- Provisional relations not in the first ratified batch → later re-distill.
- Connections panel + /work/{id} claim surfaces → Phase 136.
