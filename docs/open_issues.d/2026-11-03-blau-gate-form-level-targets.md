## P3 data: The Blau gate does not check form-level cross-reference targets
Where: `scripts/verify_blau_sidecar.py` (the cross-reference checks); `scripts/build_blau_sidecar.py` (`forms.target_ids`)
Opened: 2026-11-03

Codex round 5 on PR #472 (`7fca394c`), recorded rather than fixed because the review budget was spent.
The gate verifies that every `roots.target_id` names a root, but the builder also stores the
targets of the `cross_reference` segments inside forms as `forms.target_ids` (a JSON list), and no
check reads them: a form whose target was corrupted after the build, or a source release that
points a form at an entry it does not ship, passes the gate and leaves a dead link for the phase-2
companion card.

Measured on the real release-1.1 file (2026-11-03): 860 forms carry targets, 862 ids in all; 688
resolve to a root and **174 to a stem** (`blau-st-…`), none to a sub-entry, none unresolved. So the
check must look in `roots` and `stems` (and, to be safe, `sub_entries` and `forms`), not in `roots`
alone, or the real file would fail it.

Fix sketch (two parts, ~15 lines): in the gate, parse every non-NULL `forms.target_ids`, count the
ids absent from `roots ∪ sub_entries ∪ stems ∪ forms`, and fail on any; in the builder, pin
`meta.form_targets` (862) and `meta.form_targets_unresolved` (0) and compare them in the gate like
`cross_references` / `cross_references_unresolved`, so a later release's gaps are reported as
differences rather than hidden. One test on the synthetic fixture: point `blau-f-000004`'s
`target_ids` at `blau-e-999999` and expect FAIL.
