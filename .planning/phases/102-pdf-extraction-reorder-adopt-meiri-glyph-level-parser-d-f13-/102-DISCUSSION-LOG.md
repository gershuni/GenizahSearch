# Phase 102: LOCAL PDF Text-Layer Extraction Rewrite - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-05-29
**Phase:** 102-pdf-extraction-reorder-adopt-meiri-glyph-level-parser-d-f13
**Areas discussed:** Architecture (replace vs gated fallback), De-spacing aggressiveness,
Scope boundaries & edge cases, + Codex pre-planning cross-AI critique

---

## Gray-area selection

| Option | Selected |
|--------|----------|
| Migration of existing libraries | ✗ — DECIDED: rely on existing "Re-index All" button (no new mechanism) |
| Replace vs. gated fallback | ✓ |
| De-spacing aggressiveness | ✓ |
| Scope boundaries & edge cases | ✓ |

---

## Architecture (Replace vs. gated fallback)

| Option | Description | Selected |
|--------|-------------|----------|
| rawdict primary, RTL-gated internally | Single path; de-space+reorder only on RTL lines; LTR passes through | ✓ |
| blocks default + new rawdict trigger | Keep blocks, add triggered rawdict path | |
| rawdict primary, blocks emergency fallback | rawdict primary + blocks on empty | |

**User's choice:** rawdict primary, RTL-gated internally.

### Follow-up: gate granularity
| Option | Selected |
|--------|----------|
| Per-line (rebuilt from glyph y-bands) | ✓ |
| Per-span (Meiri-native) | |
| Per-block | |

### Follow-up: empty-rawdict safety net
| Option | Selected |
|--------|----------|
| Fall back to blocks for that page | ✓ (later STRENGTHENED to an LTR-damage guard per Codex HIGH-1) |
| Trust rawdict, no fallback | |

---

## De-spacing aggressiveness

| Option | Description | Selected |
|--------|-------------|----------|
| Adaptive 1.8× median + space-glyph hints | Proven threshold + embedded space-glyph corrective hints | ✓ |
| Adaptive 1.8× median only | Spike baseline, accept over-merge | |
| Invest in bimodal/Otsu split | Statistical gap clustering | |

**User's choice:** Adaptive 1.8× median + space-glyph hints. (Later refined with hysteresis
/ two-threshold guidance per Codex MED-6.)
**Notes:** Flagged that over-merge hurts Tantivy search recall as much as under-split, so
"join>divide" doesn't apply to word-gap de-spacing here.

---

## Scope boundaries & edge cases (batch of 4)

| Decision | Options | Selected |
|----------|---------|----------|
| F-G detection | codepoint-garbage ratio / cmap inspection / both | **codepoint-garbage ratio** (later made conservative per Codex MED-7) |
| F-G status/UX | new 'corrupt_encoding' status / reuse 'no_text_layer' | **new 'corrupt_encoding' status, shown in tree** |
| Nikud | adopt-attachment+keep / **strip at extraction** / defer | strip at extraction → **REVERSED to keep-nikud-strip-only-for-index** per Codex HIGH-2 |
| Multi-column | **defer + documented limitation + seed** / add detection now | defer (+ later: add a suspected-marker per Codex LOW-10) |

---

## Codex pre-planning cross-AI critique

User requested (via "Other"): "Send to Codex for recommendations, pushback, more insights.
Frame it clearly as a pre-planning stage." Brief written, `codex exec` (gpt-5.5, xhigh) run
against the live codebase. Full critique → `102-CODEX-CRITIQUE.md`. Two findings escalated to
the user for a decision:

### Codex HIGH-2 — Nikud
| Option | Selected |
|--------|----------|
| Keep nikud in text, strip only for the index | ✓ |
| Keep original: strip at extraction | (overturned) |

**User's choice:** Keep nikud in cached_text/display; index a stripped copy. extraction_format_version bump.

### Codex HIGH-1 — Safety net
| Option | Selected |
|--------|----------|
| Add an LTR-damage guard (rawdict-vs-blocks compare on low-RTL pages) | ✓ |
| Empty-only fallback | (superseded) |

**User's choice:** Add the LTR-damage guard.

Findings HIGH-3/4 + all MED/LOW baked into CONTEXT.md decisions (synthetic-space bbox hazard,
corrupt_encoding full status wiring, baseline-based line grouping, gap-threshold hysteresis,
conservative garbage detection, disable-images-in-rawdict perf, multi-column suspected-marker,
bbox/glyph-trace fixtures).

## Claude's Discretion
F-B punctuation spacing, F-C `_fix_visual_brackets` adoption, F-F header reversal,
extraction_format_version bookkeeping, Unicode presentation-form/ligature normalization.

## Deferred Ideas
Multi-column reconstruction (detect-only this phase), OCR (D-F2 / SEED-003), Otsu/bimodal
split (only if hysteresis insufficient).
