# FGP identity, credit, visibility and refresh rules (gate 2)

**Status:** proposed rules, applied and measured 2026-10-25; the open rulings in §6 are the owner's.
**Scope:** gate 2 of [`FGP_SOURCE_FOLDERS_ASSESSMENT.md`](FGP_SOURCE_FOLDERS_ASSESSMENT.md) §8 — what a public
`fgp:<id>` asserts, where its credit comes from, when a row may be shown, and how a later FIST delivery changes the
rows. The rules are applied by `scripts/fgp_gate2_rules.py` over the gate-1 inventory
(`scripts/fgp_association_inventory.py`) and today's `fgp_data/fgp_transcriptions.db`; the per-row result is
`fgp_data/inventory/gate2.db` (`row_outcomes`, `aliases`, `credit_rulings`, `checks`, `counts`). Declared counts:
`scripts/fgp_gate2_rules.expect.json`; wrapper `scripts/run_fgp_gate2_rules.ps1`.

## 1. What a public id means

`fgp:<id>` is the integer `id` of a `fgp_transcriptions` row, namespaced by `shared.fgp_service.namespaced_source_id`.
It is public in two places: the web chooser saves a reader's selection by it (`reading_desk_selected_sources`, persisted
in the per-user reading-desk state), and the MCP detail section `fgp_sources` lists it. Nothing else cites it.

**Meaning** of an id = (kind, language, `sys_id`, `c_number`) as the row carries them, plus the FIST source record the
row's file belongs to. Today's rows are one per (file, manuscript, image): 44,975 of 45,034 carry a `sys_id` that the
file's FIST inventory chain reaches, so the record is found by **the row's basename, choosing among the file's FIST
units the one whose chain reaches the row's `sys_id`** (never by the 14-digit id, never by image number). The row's
durable key is `<unit_table>:<unit_id>|<sys_id>|<c_number>`; 45,003 distinct keys exist among the live rows, and no two
live rows share one (check `G06`).

## 2. Outcomes of the 45,034 rows

| Outcome | Rows | Rule |
|---|---|---|
| **kept** | 40,893 | the unit's chain reaches the row's `sys_id` (or the row is one of the 7 whose unit has no AlmaId / 19 whose file is absent from this delivery / 1 FIST never saw); identity, text source and credit source unchanged |
| **re-sourced** | 4,091 | identity kept, but the credit (961 rows) or the XML text (3,130 rows) will come from a different source than today — see §3, §4 |
| **retired-with-alias** | 50 | the meaning cannot stand: **17** exact duplicates of another row (same file, manuscript and image, imported from two shelfmark folders; successor = the lowest id, same meaning, content byte-identical in all 23 groups), **28** rows with no `sys_id` (unreachable: the chooser selects by `sys_id`), **5** rows whose `sys_id` the FIST chain contradicts (4 `libraries_csv` resolutions of non-corpus-shaped ids and 1 `fjms_shelfmark` neighbour; no successor — the pipeline creates the correct row under a new id) |

Ids are never reused. A retired id resolves through `aliases(old_id, successor_id, reason, same_meaning)`: with a
successor the chooser substitutes it silently (same meaning); without one a saved selection falls back to the default
and is never redirected to another manuscript. Association basis of the live rows: 44,992 `fist_chain`, 2
`fist_chain_multi_registration` (one file registered twice for one manuscript under two signature sets of the same
source — the lowest unit id is canonical), 7 `unconfirmed` (FIST unit without an AlmaId; 3 `libraries_csv`, 4
`lib_shelfmark_token` on images fjms does not resolve either), 28 `none` (the retired unreachable rows). The file's
multi-association class from gate 1 is carried on every row; a multi-AlmaId file is one row per manuscript, which is
what the June import already produced.

## 3. Credit

The credit of a row is derived from **its unit's FIST `SourceId` / `SubId`**, not from the file-name prefix (gate 1:
1,551 files disagree). Precedence:

1. a verified team-head credit (`TEAM_CREDITS` in `scripts/fgp_fill_credits_bilingual.py`, owner-verified against FJMS
   2026-06-24), keeping the manuscript's DataSource part for *that* team when present (the individual transcriber) —
   7,171 + 4,919 rows;
2. the infrastructure credit for source 200 (Genuzos) / 400 (Herzog) — 24,373 rows;
3. source 500 → `CODE_Catalog` by `SubId` — 3,152 rows; source 600 → `CODE_Title` by `SubId` — 3,341 rows;
4. otherwise **unresolved** (2,078 rows): 1,204 where only the manuscript-level DataSource aggregate exists
   (`datasource_only`), 874 with nothing. These are listed per source in `credit_rulings` (§6).

FIST `HebCopyRight` / `EngCopyRight` (849 rows: the Sussmann / Israel Academy acknowledgment, "Displayed by permission
of Koninklijke Brill NV.") is carried verbatim **beside** the credit as an acknowledgment, never as the credit.
961 rows change credit under this rule, 932 of them book rows whose `SubId` (e.g. 50414 → גנזי קדם) the old
`image_id[3:7]` heuristic could not reach, and 26 rows the heuristic had credited to Genuzos that FIST registers under
team 132 or a book (e.g. row 3154 → Gil, *Eretz Israel in the first Muslim period*).

## 4. Visibility

A row is **publishable (strict)** only when all of these hold — 39,754 rows today:

- kind is transcription or translation (descriptions and unmatched files never enter the chooser);
- outcome is not retired; association basis is `fist_chain` or `fist_chain_multi_registration`;
- the unit's `tmp_DoubleNotToDisplay` is not 1 (23 rows held; its meaning is unconfirmed with FGP);
- the credit is resolved (§3);
- the text is verified to belong to the row's own record. XML rows fail this when today's rebuild keyed the XML by
  image number: 3,099 `xml_unverified` (3,073 of them on Genuzos units whose image folder FIST assigns to *another
  Genuzos unit of the same manuscript* — probably the same edition under a second registration, but unproven until
  gate 3 compares texts; 24 with no FIST owner; 2 same source, other manuscript), 26 `xml_region` (the row's record is
  a `_G` region of the image), 5 `xml_other_edition` (the row's record is the B2 edition, the XML the B1 one);
- the file is in the delivery (19 rows are served from the June export only).

Every row that is not strictly publishable names its hold reasons (`hold_reasons`; check `G07`). A second count,
**41,807**, is what the same rules give if the generic "Friedberg Genizah Project" attribution is accepted as a
resolved credit; the strict flag never uses it. Gate 2 changes nothing in production: today all 45,034 rows are
served; the flags say what the rebuilt sidecar may publish.

## 5. Refresh

On a new FIST delivery the pipeline matches a row by its unit id first, then by `FileName`:

- same unit, same chain, changed file hash → a new revision of the same id (the hash is revision evidence, not
  identity); the reader's saved selection survives;
- same unit, chain no longer reaches the row's `sys_id` → retire with an alias to the row created for the new
  association only when kind, language, `c_number` and manuscript agree, else without a successor;
- unit gone from FIST → retire without a successor; a renamed `FileName` on a live unit → kept;
- a new unit → a new row with a new id; never reuse an id; `aliases` is append-only.

## 6. Open rulings (the owner's)

1. **Unresolved credits (2,078 rows, 18 FIST sources).** Largest: source **100** — 1,297 rows; today's DataSource
   credits name site users ("משתמש באתר – אבי שמידמן; מכון שוקן…"), i.e. contributions through the FGP site; **600**
   with ten `TitleId`s that have no title in `CODE_Title` (3391: 178 rows, 3267: 110, 3377: 74, 8008: 47, 3158,
   1436, 6413, 7458, 3183); **850** — 98 rows; **161** — 60 (today "עמנואל פרידברג…"); **300** — 42 (Ben-Sasson
   personal lists); **160** — 34; **601** — 11; 240, 111, 700 — 1 each. Options: a verified credit per source (as
   was done for the 20 teams), the DataSource text where it names an individual, or the generic attribution.
   The strict flag holds them all until ruled.
2. **`tmp_DoubleNotToDisplay`** (23 served rows, 90 files): confirm its meaning with FGP before anything flagged is
   published or withheld on purpose.
3. **XML text on 3,130 rows** (§4): gate 3 decides by comparison; the hold is provisional.
4. **The 5 retired conflicts and 7 unconfirmed associations**: tiny, listed in `row_outcomes` by id, worth a look by
   hand when the pipeline runs.

## 7. Run record

2026-10-25 — `scripts/run_fgp_gate2_rules.ps1`: **PASS, 7 checks + 29 declared expectations**, 10 s.
`G01` every row has one outcome · `G02` meaning preserved (0 kind disagreements, 0 publishable rows off the chain) ·
`G03` every retired row has a reason and any successor has the same meaning · `G04` no publishable row with a hold ·
`G05` every publishable row has a Hebrew and an English credit · `G06` durable keys unique among live rows ·
`G07` every unpublishable row names its reason. `tests/test_fgp_gate2_rules.py` proves the gate fails when a unit's
kind disagrees with its row's and on a missed expectation.
