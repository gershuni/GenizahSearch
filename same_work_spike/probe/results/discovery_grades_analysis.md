# Discovery grading — Hillel's verdicts (2026-07-09)

Source: `review/grades_discovery_2026-07-09.json`. Hillel graded the
**new-witness deck** (34 of 40 `new_sample` cards) and gave **instructions**
(not grades) for the **residue deck** — see `results/residue_naming.md` for the
residue follow-up that acts on those instructions.

## new_sample (identified-work discovery queue) — 34 graded

| verdict | n | share |
|---|---|---|
| **confirmed-new-witness** | **27** | **79%** |
| correct-work-likely-known | 6 | 18% |
| misidentified | 1 | 3% |

### Two readings of the same 34

- **Identification precision = 33/34 (97%).** Both the 27 new witnesses AND the
  6 "correct-work-likely-known" are the RIGHT work — the engine named the
  composition correctly. Only **1** is a wrong work-attribution. For the
  governing goal (*more same-work, more precision*), the new? tier is 97%
  correct at the work level.
- **Novelty rate = 27/34 (79%).** Of the correctly-identified fragments, ~79%
  are genuinely NEW witnesses; ~18% are correct-work identifications of
  manuscripts that were ALREADY known (recorded as the source witness).

### The 6 "correct-work-likely-known" — a real, nameable leak

Every one is a witness already recorded, mostly in the Maagarim **מסירה**
(manuscript-transmission) field or as a published source:

| card work | note |
|---|---|
| Ytext839000 | מסירה: Cambridge UL, T-S C 2, 107 |
| Ytext689001 | מסירה: JTS, ENA 1501, 1-7 |
| Ytext500017 | מסירה: Philadelphia, Halper 156 |
| Ytext610000 | מסירה: JTS, ENA 1745, 3-10 |
| Ytext503001 | "מוכר בתור מדרש עשרת הדיברות" (known as Midrash Aseret ha-Dibrot) |
| Ytext447001 | "נראה אותו טקסט בדיוק" (looks like exactly the same text) |

**Root cause:** the `new?` demotion caught bibliography-cited witnesses
(`FjmsInfo.bib_signal`) but NOT witnesses recorded only in Maagarim's **מסירה**
shelfmark field. Those 6 were correctly identified, then wrongly flagged "new?"
because we never checked the source-witness list.

**Action (novelty precision lever):** cross-check each `new?` candidate's
shelfmark against the Maagarim מסירה shelfmark(s) of the identified work; demote
matches to `new?known`. This is the discovery-side analog of the bib-demotion
and should lift the true novelty rate above 79% by removing the ~18% known leak.
(It does NOT change work-attribution precision, which is already 97%.)

### The 1 misidentified — the canonical/colophon trap

`Ytext555001`: "זה מקרא. למה להשוות לקולופון? זה מוזר" — a **Scripture** page
matched to a **colophon** reference entry. Same failure class FRAG-1 flagged
(short shared-canonical text sneaking into the census). Reinforces the
**canonical-masking / Scripture guard** on the identification side; a bare
biblical run should not be allowed to identify a work via a colophon fingerprint.

### Two flagship finds (high-value output class)

- **Ytext350093** — "the original Genizah fragment that **Wertheimer published**
  and was considered **unknown location**." We relocated the physical manuscript
  behind a published edition.
- **Ytext514001** — "Another **Wertheimer** re-finding!"

These are a distinct, publishable output type: *relocating the manuscript
behind a known printed edition whose base fragment was lost.* Worth surfacing as
its own report — cross the `new?`/`new?known` queue against editions whose source
shelfmark is recorded as lost/unknown.

## Consequences for FRAG-2

1. **Novelty precision:** add the **מסירה-shelfmark cross-check** to the `new?`
   queue (removes the ~18% already-known leak; work-attribution unaffected).
2. **Identification precision:** canonical/Scripture guard kills the
   Bible→colophon class (1/34 here; same lever as the FRAG-1 ambiguous class).
3. **New output class:** "relocated source-of-edition" report (the 2 Wertheimer
   rediscoveries) — genuinely publishable, cheap to generate.
4. The core discovery engine is **sound**: 97% correct work-attribution, ~79%
   true novelty even before the מסירה cross-check.
