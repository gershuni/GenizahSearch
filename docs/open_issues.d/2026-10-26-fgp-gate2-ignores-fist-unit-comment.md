## P3 data: FIST's RecordStatus 1 / 5 and PermissionCommentCode 5309 have no ruling before FGP new rows ship
Where: `fist_data/FIST.db` `dbo_Signature.RecordStatus`, `PermissionCommentCode`; `scripts/fgp_gate2_rules.py`, `scripts/fgp_gate4_chooser_validation.py`
Opened: 2026-10-26

The 2026-10-26 check of drafts in the FGP delta (`_tmp/fgp_delta_drafts_report.md`, section 4) found three FIST
signals the gates do not read. The first, the unit's free-text `Comment` saying `לא נבדק מול המקור` ("not checked
against the source") or `אין לפרסם עד לקריאה מול מקור` ("do not publish until read against the source"), was fixed
the same day: gate 2 holds those units (`fist_comment_unchecked`, 167 of today's rows, team 130) and gate 4 applies
the same rule to new rows (4). Two remain without a ruling: `dbo_Signature.RecordStatus` is 0 on 40,415 of today's
40,416 units but 1 or 5 on 6,586 of the delta's units, and the FIST export carries no code table for it; and
`PermissionCommentCode` 5309 ("being worked on by a team, data temporarily inaccessible") covers 156 of today's
units (team 135, 180 rows publishable) and 8 of the delta's. Fix sketch: the owner asks FGP what `RecordStatus` 1 /
5 and code 5309 mean; a gate-2 hold for whichever means "not for publication", applied in gate 4's candidate build
before the `plain` new-rows policy ships.
