# Phase 54: Dimensions Display & Filtering - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-03-26
**Phase:** 54-dimensions-display-filtering
**Areas discussed:** Display format & placement, Unit normalization, Filter UI & behavior, Coverage gap handling (then rescoped to Schema design, Display in browse, Data quality handling)

---

## Display Format & Placement

| Option | Description | Selected |
|--------|-------------|----------|
| Browse info + search results | Show in browse extended info AND search result cards | |
| Browse info only | Show only in browse extended info | ✓ |
| Everywhere + catalog dialog | Browse, search, AND FJMS catalog dialog | |

**User's choice:** Browse info only
**Notes:** Later revised to "new dedicated dialog button" after scope expansion

| Option | Description | Selected |
|--------|-------------|----------|
| Compact: 15.2 × 22.1 cm | Width × height with unit suffix | ✓ |
| Labeled: W: 15.2 H: 22.1 cm | Explicit labels | |
| Full: outer + inner | Both dimensions when available | |

**User's choice:** Compact format

| Option | Description | Selected |
|--------|-------------|----------|
| First/largest only | Pick one representative | |
| All with source | Show all measurements with cataloger attribution | ✓ |

**User's choice:** All with source attribution

---

## Unit Normalization

Initial analysis showed bimodal distribution (cm + mm mixed without unit column). User directed to FIST_Computed_Measurements.xlsx which contains pre-normalized SizeX_cm/SizeY_cm columns with unit information.

| Option | Description | Selected |
|--------|-------------|----------|
| Replace raw with normalized | Use xlsx SizeX_cm/SizeY_cm, single source of truth | ✓ |
| Add cm columns alongside | Keep raw + add normalized | |
| New table from spreadsheet | Separate table, keep existing | |

**User's choice:** Replace raw with normalized cm values from xlsx

**User also directed:** Full import of all xlsx sheets (Computed_Measurements, Extra_Info, Blank_Images, Catalog_Sizes) — not just catalog sizes. This significantly expanded Phase 54 scope.

---

## Scope Expansion & Phase Split

User chose "Full import of all sheets" + "Import Material + Size_Category". This expanded Phase 54 from catalog dimensions to 1.5M rows of measurement data.

| Option | Description | Selected |
|--------|-------------|----------|
| All in Phase 54 | Import + display + filtering in one phase | |
| Split: 54=import+display, 55=filters | Phase 54 imports and displays, filtering becomes Phase 55 | ✓ |
| Catalog_Sizes only | Just 179K catalog dimensions | |

**User's choice:** Split into Phase 54 (import+display) and Phase 55 (filtering)

---

## Schema Design (after rescope)

| Option | Description | Selected |
|--------|-------------|----------|
| Image-level (per FGP) | Full granularity, 434K rows | |
| Manuscript-level aggregate | One row per AlmaId | |
| Both | Detail + precomputed summary | ✓ |

| Option | Description | Selected |
|--------|-------------|----------|
| New tables in fjms_enrichment.db | Add alongside existing | ✓ |
| Replace + add new | Replace catalog_sizes + new tables | |
| Separate measurements.db | New sidecar | |

---

## Display in Browse (after rescope)

**User's choice:** New "Measurements" button + dedicated dialog (like Catalog/Bibliography pattern). All measurements in dialog, not in browse info panel.

---

## Data Quality

| Option | Description | Selected |
|--------|-------------|----------|
| Show when available, hide when NULL | Only display material/size when data exists | ✓ |
| Show with 'Unknown' fallback | Always show field | |

| Option | Description | Selected |
|--------|-------------|----------|
| Import but mark visually | Warning icon for flagged records | |
| Exclude flagged from display | Don't show bad data | ✓ |
| Import all, flag in summary | Store flags, let filters exclude | |

---

## Deferred Ideas

- Dimension/material filtering → Phase 55
- PuzzleRatio/CentroidDescriptor for join suggestions → future
- DPI data for puzzle canvas calibration → future
