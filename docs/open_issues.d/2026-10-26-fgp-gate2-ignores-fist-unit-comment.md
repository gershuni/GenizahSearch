## P2 data: Gate 2 publishes FGP units whose FIST comment says "not checked against the source"
Where: `scripts/fgp_gate2_rules.py` (hold reasons), `fist_data/FIST.db` `dbo_UnitTranscription.Comment`, `dbo_Signature.RecordStatus`
Opened: 2026-10-26

The gate-2 publishability rules read FIST's `tmp_DoubleNotToDisplay` and `ViewLevelCode` but not the unit's free-text
`Comment`, so 166 of the 168 rows of today's sidecar whose FIST unit says `לא נבדק מול המקור` ("not checked against the
source", with a writing stage `כתוב חדש` / `הגהה א'` / `הגהה ב'`) or `אין לפרסם עד לקריאה מול מקור` ("do not publish
until read against the source", 15 rows) are marked publishable (151 units, 167 of them team 130). Found by the
2026-10-26 check of drafts in the FGP delta (`_tmp/fgp_delta_drafts_report.md`, section 4), which also found that
`dbo_Signature.RecordStatus` is 0 on 40,415 of today's 40,416 units but 1 or 5 on 6,586 of the delta's units with no
code table to say what the values mean, and that `PermissionCommentCode` 5309 ("being worked on by a team, data
temporarily inaccessible") covers 156 of today's units (team 135, 180 rows publishable). Fix sketch: a gate-2 hold
reason `fist_comment_unchecked` for the two comment families, a ruling from the owner on `RecordStatus` 1 / 5 and on
code 5309 before any new row enters, and the same rule applied in gate 4's candidate build.
