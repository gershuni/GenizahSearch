# REF-1 Stage 1 — Reference-source expansion (Sefaria + Hebrew Wikisource)

Generated 2026-07-09. Script: `same_work_spike/probe/scripts/ref1_fetch_sefaria.py`
(`python -X utf8 -u ref1_fetch_sefaria.py`, run from `scripts/`). Output:
`same_work_spike/probe/refs_staging/` (58 body+versemap file pairs + `manifest.json`,
gitignored — **NOT committed**, per the brief, pending the Stage-2 `reuse_ok` review).
No changes to `ref_corpus.pkl` or `track1_build_ref.py`.

## Headline numbers

| | |
|---|---|
| Works staged | **58** |
| Total characters staged | **3,275,267** |
| License mix | Public Domain 47 · CC-BY 7 · unknown 4 |
| `reuse_ok` | yes 54 · unclear 4 (quarantined) · no 0 |
| Sidecar structure | `verse` (chapter:verse) 46 · `hierarchical` (schema-node path) 12 |
| `ref_kind` | `edition` 45 · `modern_rite_mask_only` 13 |
| API calls made | ~230, throttled to ~1 req/sec, zero errors/bans from Sefaria |

All bodies pass a zero-tolerance post-hoc scan: **every character in every staged
`.txt` is either a Hebrew base letter (א–ת, final forms preserved) or a single
space/newline** — no nikud, no te'amim, no geresh/gershayim/quotes, no digits,
no HTML, no verse/chapter labels leaked into any body.

## 1. Targum Onkelos + Targum (Pseudo-)Jonathan + Targum to Writings

**42 works, 2,434,399 chars, 100% hit rate** (every targeted book of Onkelos,
Targum Jonathan, and the Ketuvim Targumim exists on Sefaria and was staged).

| Group | Books | Chars | License |
|---|---|---|---|
| Targum Onkelos (Torah) | 5/5 | 420,260 | Public Domain (toratemetfreeware.com edition — outranked the also-available CC-BY-NC Metsudah and CC-BY-SA Yemenite-Taj/Wikisource editions) |
| Targum (Pseudo-)Jonathan on Torah | 5/5 | 572,235 (incl. in Jonathan row below) | Public Domain |
| Targum Jonathan on Nevi'im (all 21 books, Joshua–Malachi) | 21/21 | Public Domain | |
| Targum Jonathan total (Torah+Nevi'im) | 26/26 | 1,405,870 | Public Domain |
| Aramaic Targum to Writings (Psalms, Proverbs, Job, Song of Songs, Ruth, Lamentations, Ecclesiastes, Esther/Targum Rishon, I+II Chronicles) | 10/10 | 523,136 | Public Domain |
| Targum Sheni on Esther (bonus, aggadic 2nd Targum) | 1/1 | 85,133 | **unknown → quarantined `unclear`** (only Hebrew version on Sefaria is "Berlin, 1898" with no license metadata) |

This directly closes the finding flagged at the top of this spike ("Targum
Onkelos VERIFIED absent from ref_corpus.pkl") and — cross-referencing the B2
residue list — **directly resolves B2 unit 2141254** ("תרגום אונקלוס;תרגומים
ארמיים... Targum Onqelos: Leviticus 5:17–6:3") and **unit 286716** (same
Onkelos signature), both catalog-confirmed Targum-Onkelos gaps that this
acquisition now covers.

Not sourced (out of the explicit target list, noted as Stage-2 leads):
**Targum Neofiti** and **Targum Jerusalem** (fragmentary Palestinian Targum to
Torah) both exist on Sefaria as single-leaf indices — real texts, just not
requested by the brief's target list (Onkelos + Jonathan + Writings only).

## 2. Statutory liturgy core — `ref_kind: modern_rite_mask_only`

**13 works, 73,595 chars.** All 13 correctly carry `ref_kind:
modern_rite_mask_only` in the manifest — per the brief, Stage 2 must gate
these out of witness/new-testimony census and use them for masking +
candidate-linking only (these are modern printed Ashkenazi rite texts, not
Genizah-era witnesses).

| Work | Chars | License |
|---|---|---|
| Weekday Amidah (Shacharit) | 5,367 | CC-BY (Metsudah Siddur 1981) |
| Weekday Amidah (Maariv) | 5,293 | CC-BY |
| Shabbat Amidah (Maariv) | 3,972 | CC-BY |
| Shabbat Amidah (Shacharit)* | 3,715 | Public Domain |
| Shabbat Amidah (Musaf) | 5,654 | Public Domain |
| Weekday Shacharit Blessings of the Shema | 4,811 | CC-BY |
| Weekday Maariv Blessings of the Shema | 4,066 | CC-BY |
| Shabbat Maariv Blessings of the Shema | 2,990 | CC-BY |
| Kiddush (Friday night) | 508 | CC-BY |
| Kiddush (Shabbat day / Kiddusha Rabba) | 443 | Public Domain |
| Hallel | 3,513 | **unknown → unclear** |
| Birkat Hamazon** | 6,256 | **unknown → unclear** |
| Pesach Haggadah | 27,007 | Public Domain |

\* one leaf ("Holiness of God") had no text in the chosen Hebrew version — the
work is staged from its other 9 leaves; noted per-work in `transformation_notes`.
\*\* 8/10 leaves resolved; "HaRachaman of Brit Milah" and "Sheva Brachot"
(occasional-blessing addenda) had no text in the chosen version.

**Sidecar upgrade (mid-flight requirement from Hillel):** every liturgy work's
`*.versemap.json` now carries `"structure": "hierarchical"` with a `sections`
array, one entry per Sefaria schema leaf, each holding the **full schema-node
path** (e.g. `["Siddur Ashkenaz","Weekday","Shacharit","Amidah","Patriarchs"]`),
its own `[start,end)` span in the body, and a nested `paragraphs` array for
finer-grained offsets. Example (`liturgy_amidah_weekday_shacharit.versemap.json`,
first section):
```json
{
 "schema_path": ["Siddur Ashkenaz","Weekday","Shacharit","Amidah","Patriarchs"],
 "ref": "Siddur Ashkenaz, Weekday, Shacharit, Amidah, Patriarchs",
 "start": 0, "end": 314,
 "paragraphs": [{"seq":1,"start":0,"end":29}, ...]
}
```
Simple verse texts (all 45 Targum works) keep the flat `units` list
(`chapter`/`verse`/`start`/`end`) as originally specified — no complex-node
hierarchy applies there. No target hit the "degenerate structure" case (every
leaf's Sefaria ref resolved to a real, non-trivial schema path), so no
`structure: flat` notes were needed.

**Cleaning note specific to liturgy:** the chosen Metsudah/PD siddur editions
wrap publisher rubrics and kavanah instructions in `<small>` (e.g.
`בלחש:`/`בקול:` = "in a whisper:"/"aloud:", and a paragraph of halachic
instruction before the Amidah) — these are dropped entirely, not just
detagged, since they are not prayer text (spot-checked in
`liturgy_kiddush_friday_night.txt`, which now starts directly at
`ויהי ערב ויהי בקר`, not at the whisper-rubric).

## 3. B2 residue list — hit-rate table (all 90 auto-labeled entries)

Checked every one of the 90 `(a) Auto-labeled by catalog` entries in
`b2_residue_most_copied.md` against Sefaria's full title index (6,600 leaves,
`/api/index/`) by hand plus a token-overlap cross-check script (to catch
anything missed by eye). Verdict counts:

| Verdict | Count | Notes |
|---|---|---|
| **FOUND & staged** | 3 distinct works, covering **8 rows** | Rif (Hilchot HaRif, Shabbat) — 6 rows; Radak on Isaiah — 1 row; Keter Malkhut (Ibn Gabirol) — 1 row |
| **FOUND, already covered elsewhere** | **2 rows** | Targum Onkelos (row 2141254 — covered by §1 above); a generic "Common Prayers: Shaharit Weekday Amidah" catalog hit (row 2422280 — covered by §2's Weekday Amidah) |
| **FOUND but not staged (addressing problem)** | **1 row** | Zohar, Devarim/Ha'azinu (row 478512) — Sefaria's Zohar is one index addressed by traditional daf (e.g. `Zohar 3:279a`), not by parsha; resolving the Vilna-edition page cite (רצב–רצט) to a daf range is a small research task deferred to Stage 2 |
| **N/A — not a sourceable "work"** | **~10 rows** | Personal documents/deeds (`תעודות אישיות ושטרות`, 3 rows); raw biblical-text fragments with Masoretic apparatus (Torah/Haftarot/Ketuvim excerpts, ~6 rows) already covered by Tanakh in `ref_corpus.pkl` via Maagarim; Talmud Bavli Shabbat (row 50224) likewise already in `ref_corpus.pkl` |
| **MISS — genuine gap, not on Sefaria/Wikisource** | **~69 rows** | Overwhelmingly Judeo-Arabic Karaite exegesis/philology (Yefet ben Ali, Salmon ben Yeruham, Sharh al-Alfaz glossaries, Abu al-Faraj Harun, al-Qirqisani-adjacent material), Karaite liturgy/piyyutim, medieval JA Hebrew-grammar treatises, and individual unidentified piyyutim — exactly the class the brief predicted would miss |

**Hit rate headline: ~11 of 90 rows (12%) resolved to a real, sourceable
Sefaria text** (8 newly staged + 2 already covered by items 1–2 above +
1 found-but-deferred); the remaining 88% miss, split between "not a coherent
work" (~11%) and "genuine specialist-text gap Sefaria/Wikisource doesn't
cover" (~77%) — consistent with the brief's own expectation ("expect many
misses — JA works like קצת חנה will NOT be there").

The 3 newly-staged B2 confirms:

| Work | Chars | License | `reuse_ok` |
|---|---|---|---|
| Rif (Isaac Alfasi), Hilchot HaRif on Shabbat | 236,160 | Public Domain | yes |
| Radak on Isaiah | 511,908 | unknown ("Radak on Nach") | **unclear** |
| Keter Malkhut (Solomon ibn Gabirol) | 19,205 | Public Domain | yes |

**Hebrew Wikisource fallback:** no direct Wikisource API calls were made in
this pass. Reasoning: (a) the miss list is dominated by medieval Judeo-Arabic
specialist texts that were never set in accessible print and are therefore
extremely unlikely to be on Wikisource either; (b) Sefaria's own version
metadata already transitively surfaces Wikisource-digitized editions where
they exist (e.g. one of Targum Onkelos's four Hebrew source options was
literally "Targum Onkelos, vocalized according to the Yemenite Taj", sourced
from `he.wikisource.org` — it just ranked below the Public-Domain
toratemetfreeware.com edition in the license-preference order, so it wasn't
the one chosen). If a future pass wants direct Wikisource coverage for the
~69 genuine misses, expect the same near-zero hit rate for the JA/Karaite
material specifically.

## 4. Tafsīr (Saadia Gaon Judeo-Arabic Bible translation) coverage — report only, no sourcing

Checked all 89 JA-category works already in `ref_corpus.pkl` (via
`track1_build_ref.py`'s `JA` category) for Saadia Gaon (`רס"ג`) titles:

**Torah (תפסיר תורה, the literal translation) — 4/5 books present:**

| Book | Present? | ref_corpus id |
|---|---|---|
| בראשית Genesis | Yes | `J:36-רסג-בראשית-תרגום-תפסיר-תורה` |
| שמות Exodus | Yes | `J:37-רסג-שמות-תרגום-תפסיר-תורה` |
| **ויקרא Leviticus** | **NO — confirmed gap** | (id `J:38` is skipped entirely in the source numbering — jumps 37→39; not a mis-categorization, the file is genuinely absent from the JA per_doc source directory) |
| במדבר Numbers | Yes | `J:39-רסג-במדבר-תרגום-תפסיר-תורה` |
| דברים Deuteronomy | Yes | `J:40-רסג-דברים-תרגום-תפסיר-תורה` |

Also present: Saadia's Torah *commentaries* (פירוש, distinct from the literal
תרגום) for Genesis (`J:65`) and Exodus (`J:59`) only.

**Nevi'im — 1 book only:** Isaiah תרגום (`J:78`). No Saadia translation
attested for the rest of Nevi'im in this corpus (consistent with the historical
record — Saadia's Tafsir on Nevi'im beyond Isaiah is thinly attested in general).

**Ketuvim — essentially complete for what's historically attested:** Psalms
(תרגום `J:47` + פירוש `J:48`), Proverbs (`J:04`/`J:05`), Job (`J:02`/`J:03`),
Song of Songs (`J:10`/`J:18`), Ruth (`J:11`), Ecclesiastes (`J:12`), Esther
(`J:13`), Lamentations (`J:14`), Daniel (`J:21`/`J:22`). Missing: Ezra-Nehemiah,
Chronicles — not historically attested as Saadia translations anyway.

**Headline gap: Saadia's Tafsir on ויקרא/Leviticus is the one real hole in an
otherwise near-complete Torah Tafsir set**, and Nevi'im coverage beyond Isaiah
is thin (but likely reflects real transmission gaps, not a corpus-building
oversight). **Not sourced per the brief** — flagged for Stage 2/future work.

**Lead for Stage 2 (found incidentally, not pursued further per the brief's
"do not hunt sources for it yet"):** Sefaria has its own **"Tafsir Rasag"**
index (`Tanakh > Targum > Tafsir Rasag`), a complex schema-node text with an
"Introduction" node and at least a "Genesis" node — i.e. Saadia's Torah Tafsir
in Sefaria's own edition. Worth checking in Stage 2 whether it covers
Leviticus (closing the one confirmed Torah gap) and whether its license is
usable; not verified here.

## 5. Manifest schema (per entry)

Every entry in `refs_staging/manifest.json` (`entries: [...]`) carries:
`key`, `title_he`, `title_en`, `source_ref` (Sefaria ref), `source_url`,
`version_title`, `license`, `license_url` (standard CC URL or `null` for
Public Domain/unknown), `attribution_text` (ready-to-use credit line),
`retrieval_date` (`2026-07-09`), `transformation_notes` (cleaning pipeline +
per-work leaf-resolution caveats), `ref_kind` (`edition` |
`modern_rite_mask_only`), `reuse_ok` (`yes`/`unclear`/`no` — license-rank
derived, NC/unknown licenses quarantined as `unclear`), `char_count`,
`body_file`, `versemap_file`, `structure` (`verse` | `hierarchical`).

**Quarantined (`reuse_ok: unclear`) — 4 works, do not integrate until a human
license review clears them:**

| Key | License | Reason |
|---|---|---|
| `targum_ketuvim_esther_targum_sheni` | unknown | Only Hebrew version on Sefaria ("Berlin, 1898") carries no license metadata |
| `liturgy_hallel` | unknown | Sefaria's Hallel Hebrew version license field is empty/"unknown" |
| `liturgy_birkat_hamazon` | unknown | Same |
| `b2_radak_isaiah` | unknown | Sefaria's "Radak on Nach" Hebrew version license field is empty/"unknown" |

No works landed in `reuse_ok: no` — nothing explicitly copyright-restricted
was staged (Metsudah's CC-BY-NC editions existed for several works but were
always outranked by a Public-Domain or CC-BY alternative from the same work's
other versions, so NC editions were never the chosen source).

## Open issues / Stage 2 notes

1. **Zohar Devarim/Ha'azinu** (B2 row 478512) needs daf-range resolution
   before it can be staged (Vilna page רצב–רצט → Zohar volume/daf).
2. **Targum Neofiti / Targum Jerusalem** exist on Sefaria, not sourced here
   (outside the brief's explicit target list) — cheap Stage-2 add if wanted.
3. **"Tafsir Rasag" on Sefaria** may close the confirmed Leviticus Tafsir gap
   — worth a look in Stage 2, license unverified.
4. **4 quarantined `unclear`-license works** need a human license call before
   Stage-2 integration (table above).
5. Per the brief, Stage 2 must enforce `ref_kind: modern_rite_mask_only`
   texts being excluded from the witness/new-testimony census at the
   classifier level — this report only sets the flag; the consumer-side gate
   is Stage 2's job.
6. The 88% B2 miss rate (~79/90 rows either N/A or genuinely unsourceable)
   confirms the residue is overwhelmingly Judeo-Arabic Karaite specialist
   material outside Sefaria/Wikisource's scope — any further closing of this
   gap needs primary critical editions (Ben-Shammai, Frank, Polliack, Vollandt
   etc.), not general Hebrew text repositories.
