# FGP identity, credit, visibility and refresh rules (gate 2)

**Status:** rules applied and measured 2026-10-25. The owner ruled the same day that the generic FGP attribution
is acceptable for rows whose FIST source has no verified credit (the gate's default `--credit-policy generic`); the
per-source refinement from the live FGP site in §6 is pending.
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
| **kept** | 39,291 | the unit's chain reaches the row's `sys_id` (or the row is one of the 7 whose unit has no AlmaId / 19 whose file is absent from this delivery); identity, text source and credit source unchanged |
| **re-sourced** | 5,693 | identity kept, but the credit (2,563 rows) or the XML text (3,130 rows) will come from a different source than today — see §3, §4 |
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
   2026-06-24; team 111, Simcha Emanuel, added 2026-10-25 from the live site), keeping the manuscript's DataSource
   part for *that* team when present (the individual transcriber) — 7,172 + 4,919 rows;
2. the infrastructure credit for source 200 (Genuzos) / 400 (Herzog) — 24,373 rows;
3. source 500 → `CODE_Catalog` by `SubId` — 3,152 rows; source 600 → `CODE_Title` by `SubId` — 3,872 rows (the
   title in Hebrew, else in English: 2,127 FIST titles carry only an English title — Gil's *Documents of the Jewish
   Pious Foundations*, the India Book, Goitein's *Letters of Medieval Jewish Traders*, Mann, Ginzberg's *Geonica* —
   and `build_book_map` read only the Hebrew columns until 2026-10-25, which is why 531 of these rows had no credit);
4. for sources 100, 850, 700 (site users), 161 (Emanuel Friedberg's team) and 300 (Ben-Sasson's lists): **the single
   DataSource part that names the contributor** (`DATASOURCE_PART_TOKENS`) — 1,087 rows (1,003 of them
   "משתמש באתר – אבי שמידמן"). FJMS's DataSource is per manuscript and lists everyone who contributed anything to
   it, so the part is taken only when exactly one matches the source's token (14 manuscripts list several site
   users). The owner confirmed the rule on the live site 2026-10-25 (BL OR 10578S.16 → Shmidman; AIU III.B.109 →
   Friedberg's line);
5. otherwise **unresolved** at the source level (459 rows over 8 FIST sources, §6): 117 where only an aggregate
   without a matching part exists (`datasource_only`), 342 with nothing. **Ruled 2026-10-25:** these are published
   under the generic attribution "Friedberg Genizah Project / פרויקט הגניזה של פרידברג" (`credit_status = generic`,
   453 live rows); the DataSource text stays recorded per row.

FIST `HebCopyRight` / `EngCopyRight` (849 rows: the Sussmann / Israel Academy acknowledgment, "Displayed by permission
of Koninklijke Brill NV.") is carried verbatim **beside** the credit as an acknowledgment, never as the credit.
2,563 rows change credit under these rules: 1,463 book rows whose `SubId` the old `image_id[3:7]` heuristic could
not reach (e.g. 50414 → גנזי קדם, 3391 → Gil, *Pious Foundations*), 1,087 contributor parts that replace an aggregate
or nothing, 26 rows the heuristic had credited to Genuzos that FIST registers under team 132 or a book (e.g. row
3154 → Gil, *Eretz Israel in the first Muslim period*), 3 catalogue rows, 1 team-111 row.

## 4. Visibility

A row is **publishable** when all of these hold — 41,807 rows under the ruled policy (41,371 under the strict
reading, which `publishable_strict` keeps reporting):

- kind is transcription or translation (descriptions and unmatched files never enter the chooser);
- outcome is not retired; association basis is `fist_chain` or `fist_chain_multi_registration`;
- the unit's `tmp_DoubleNotToDisplay` is not 1 (23 rows held; its meaning is unconfirmed with FGP);
- the credit is resolved, or generic under the ruled policy (§3);
- the text is verified to belong to the row's own record. XML rows fail this when today's rebuild keyed the XML by
  image number: 3,099 `xml_unverified` (3,073 of them on Genuzos units whose image folder FIST assigns to *another
  Genuzos unit of the same manuscript* — probably the same edition under a second registration, but unproven until
  gate 3 compares texts; 24 with no FIST owner; 2 same source, other manuscript), 26 `xml_region` (the row's record is
  a `_G` region of the image), 5 `xml_other_edition` (the row's record is the B2 edition, the XML the B1 one);
- the file is in the delivery (19 rows are served from the June export only).

Every row that is not publishable names its hold reasons (`hold_reasons`; check `G07`). Gate 2 changes nothing in
production: today all 45,034 rows are served; the flags say what the rebuilt sidecar may publish.

## 5. Refresh

On a new FIST delivery the pipeline matches a row by its unit id first, then by `FileName`:

- same unit, same chain, changed file hash → a new revision of the same id (the hash is revision evidence, not
  identity); the reader's saved selection survives;
- same unit, chain no longer reaches the row's `sys_id` → retire with an alias to the row created for the new
  association only when kind, language, `c_number` and manuscript agree, else without a successor;
- unit gone from FIST → retire without a successor; a renamed `FileName` on a live unit → kept;
- a new unit → a new row with a new id; never reuse an id; `aliases` is append-only.

## 6. Rulings

1. **Credits without a verified source — ruled 2026-10-25: generic FGP attribution**, refined the same day from
   the live FGP site. What each source got, and what is left generic (`credit_rulings`):

   | FIST source | Rows | Resolved | Still generic | Live-site finding |
   |---|---|---|---|---|
   | 100 | 1,297 | 1,003 named site users (DataSource part) | 294 (no DataSource, or no site-user part) | BL OR 10578S.16 → Avi Shmidman, confirmed |
   | 161 | 60 | 36 "עמנואל פרידברג, ראש צוות FGP להעתקות ומידע ממחברים שונים" | 24 | AIU III.B.109 → that line, confirmed |
   | 300 | 42 | 36 "בן ששון מנחם, רשימות אישיות" | 6 | — |
   | 850 | 98 | 11 named site users | 87 (catalogues and other teams only) | — |
   | 700 | 1 | 1 (Weiss, a site user's own manuscript) | 0 | — |
   | 111 | 1 | **verified team 111**: Simcha Emanuel, Head of FGP Talmud Commentaries and Halakhic Literature (Hebrew) team | 0 | T-S F5.49 → that line, confirmed |
   | 160 | 34 | — | 34 (DataSource lists catalogues and printed editions) | — |
   | 601 | 11 | — | 11 (bibliographic references) | — |
   | 600 / 6413 | 2 | — | 2 | T-S Misc.35.108: FJMS lists two transcriptions, Abramson's *Chapters from the Introduction to the Talmud* (our row 22208, FIST `TitleId` 3413) and *Ginzei Qedem* (old) — which FIST registers as a Hebrew *translation* (`TitleId` 117, our row 22210). The two rows on `TitleId` 6413 (22207 transcription, 22209 translation; no title in `CODE_Title`, "Tarbiz" by the old heuristic) have no counterpart on FJMS. Candidates for a hold, pending gate 3's text comparison |
   | 240 | 1 | — | 1 (Gottstein catalogue + "מרים נוסף, צוות FGP להדגמה") | — |

   A further per-source credit the owner confirms goes into `TEAM_CREDITS` or `DATASOURCE_PART_TOKENS`; the gate then
   reports it as resolved and the expectations move.
2. **`tmp_DoubleNotToDisplay`** (23 served rows, 90 files): confirm its meaning with FGP before anything flagged is
   published or withheld on purpose. Open.
3. **XML text on 3,130 rows** (§4): gate 3 decides by comparison; the hold is provisional.
4. **The 5 retired conflicts and 7 unconfirmed associations**: tiny, listed in `row_outcomes` by id, worth a look by
   hand when the pipeline runs.

## 7. Run record

2026-10-25 — `scripts/run_fgp_gate2_rules.ps1` (policy generic, DataSource-part rule, team 111): **PASS, 7 checks
+ 33 declared expectations**, 10 s. `G01` every row has one outcome · `G02` meaning preserved (0 kind disagreements, 0 publishable rows off the
chain) · `G03` every retired row has a reason and any successor has the same meaning · `G04` no publishable row with
a hold, and no strictly publishable row with a generic credit · `G05` every publishable row has a Hebrew and an
English credit · `G06` durable keys unique among live rows · `G07` every unpublishable row names its reason.
`tests/test_fgp_gate2_rules.py` proves the gate fails when a unit's kind disagrees with its row's and on a missed
expectation, and that `--credit-policy strict` holds the generic rows again.
