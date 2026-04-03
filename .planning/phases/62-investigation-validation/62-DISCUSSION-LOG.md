# Phase 62: Investigation & Validation - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md -- this log preserves the alternatives considered.

**Date:** 2026-04-03
**Phase:** 62-investigation-validation
**Areas discussed:** Rate limit testing approach, Storage sampling method, TOS outreach strategy, Output format & deliverables

---

## Rate Limit Testing Approach

### Test Machine

| Option | Description | Selected |
|--------|-------------|----------|
| Your home PC | Residential IP, already has the codebase and nli_crossref.db. Simplest setup. | ✓ |
| A separate residential machine | Dedicated machine or VPS with residential IP proxy | |
| EC2 via residential proxy | Run from EC2 but route through a residential proxy service | |

**User's choice:** Home PC
**Notes:** None

### Ramp Speed

| Option | Description | Selected |
|--------|-------------|----------|
| Conservative | Start at 1 req/sec, ramp to 2, 4, 8 over 15+ minutes. Stop at first sign of throttling. ~100-200 images. | ✓ |
| Moderate | Start at 2 req/sec, ramp faster. ~300-500 images. Higher block risk. | |
| Minimal probe | Just 50 images at 1 req/sec. | |

**User's choice:** Conservative
**Notes:** None

### Block Detection

| Option | Description | Selected |
|--------|-------------|----------|
| HTTP 429 or 403 | Stop on rate-limit or forbidden. Also stop on 3+ consecutive timeouts (>30s). | ✓ |
| Any non-200 response | More cautious -- treat any error as signal. | |
| Manual monitoring | Watch live and Ctrl+C. | |

**User's choice:** HTTP 429 or 403
**Notes:** None

### Resolution

| Option | Description | Selected |
|--------|-------------|----------|
| Full max resolution | /full/max/0/default.jpg | |
| 800px width | /full/800,/0/default.jpg | |
| Both for comparison | 50 at max + 50 at 800px | |

**User's choice:** Other -- "800 or 1200, needs also to see the size"
**Notes:** User wants to compare 800px and 1200px widths to make an informed resolution decision with real file size data.

---

## Storage Sampling Method

### Sample Selection

| Option | Description | Selected |
|--------|-------------|----------|
| Stratified by library | Proportional sample across libraries | |
| Random from full NLI corpus | Simple random from all 815K | |
| NLI-only subset only | Only manuscripts with no alternative sources | ✓ |

**User's choice:** NLI-only subset only
**Notes:** Priority cache targets are manuscripts without CUL/Oxford/JTS/Manchester alternatives.

### NLI-Only Determination

| Option | Description | Selected |
|--------|-------------|----------|
| Cross-reference nli_crossref.db | Query for sys_ids with NLI but no Cambridge/Oxford/Manchester/JTS entries | ✓ |
| Use FJMS catalog data | Check fjms_enrichment.db library_code | |
| Both sources combined | Cross-reference both DBs | |

**User's choice:** Cross-reference nli_crossref.db
**Notes:** None

### Directory Structure

| Option | Description | Selected |
|--------|-------------|----------|
| 2-level hash on sys_id | e.g., /cache/ab/cd/12345.jpg | |
| By library_code then sys_id | e.g., /cache/CUL/12345/ | |
| You decide | Claude picks based on analysis | ✓ |

**User's choice:** You decide (Claude's discretion)
**Notes:** None

---

## TOS Outreach Strategy

### Contact Method

| Option | Description | Selected |
|--------|-------------|----------|
| Email their digital services team | Formal academic request | |
| Use an existing contact | Leverage known NLI contact | |
| Check TOS first, email only if unclear | Review published terms first | ✓ |

**User's choice:** Check TOS first, email only if unclear
**Notes:** None

### Ambiguous TOS Handling

| Option | Description | Selected |
|--------|-------------|----------|
| Proceed cautiously | If TOS doesn't prohibit, proceed with conservative rate + academic framing | ✓ |
| Treat as no-go | Ambiguity = stop until explicit permission | |
| Proceed but email in parallel | Start fetch while reaching out | |

**User's choice:** Proceed cautiously
**Notes:** None

### INV-04 Gate Level

| Option | Description | Selected |
|--------|-------------|----------|
| TOS review is sufficient | Mark conditional go without waiting for NLI reply | ✓ |
| Must have NLI acknowledgment | Block until NLI responds | |
| TOS + courtesy email sent | Go after review, but send courtesy email | |

**User's choice:** TOS review is sufficient
**Notes:** None

---

## Output Format & Deliverables

### Main Deliverable

| Option | Description | Selected |
|--------|-------------|----------|
| Investigation report + scripts | Markdown report + reusable test scripts | ✓ |
| Report only | Just findings document | |
| Scripts with inline docs | Scripts are the deliverable | |

**User's choice:** Investigation report + scripts
**Notes:** None

### Report Location

| Option | Description | Selected |
|--------|-------------|----------|
| Phase directory | .planning/phases/62-*/62-REPORT.md | |
| docs/ directory | docs/specs/image-cache-investigation.md | |
| Both | Full report in .planning/, summary in docs/ | ✓ |

**User's choice:** Both
**Notes:** None

### Script Location

| Option | Description | Selected |
|--------|-------------|----------|
| scripts/ directory | Consistent with existing project scripts | ✓ |
| Phase directory | Self-contained but non-standard | |
| New cache/ directory | Dedicated cache tooling space | |

**User's choice:** scripts/ directory
**Notes:** None

---

## Claude's Discretion

- EC2 filesystem directory structure
- Sample selection algorithm within NLI-only subset
- Report structure and sections

## Deferred Ideas

None -- discussion stayed within phase scope
