# REF-2 reference-corpus ingestion report (SEED-029, 2026-07-10)

`ref_corpus_v2.pkl` = ref_corpus v1 (untouched) + 58 new Sefaria-sourced reference
works: **Targum** (42, the systemic-gap class), **Liturgy** (13 statutory units,
`ref_kind=modern_rite_mask_only`), **Sefaria** (3 b2 gap works). Built by
`scripts/ref2_build.py`; verified by `scripts/ref2_verify.py`; gap-works lookup by
`scripts/ref2_gap_lookup.py`. v1 (`data/ref_corpus.pkl`) was never modified.

## Counts

| | |
|---|---|
| v1 works | 5,363 (86,819,752 letters) |
| staged files | 58 (`refs_staging/`, manifest 2026-07-09) |
| **ingested** | **58** — Targum 42 (1,974,559 letters) · Liturgy 13 (59,225) · Sefaria 3 (614,143) |
| skipped (<200 letters) | 0 (smallest kept: `liturgy_kiddush_shabbat_day`, 349 letters) |
| dropped true-dups (≥0.98 vs v1) | 0 |
| version groups formed | 4 (10 member works, incl. 1 v1 work) |
| v2 total | **5,421 works / 89,467,679 letters** → `data/ref_corpus_v2.pkl` (171 MB) |

New work ids are `REF2:<staging-key>` (e.g. `REF2:targum_onkelos_genesis`). Schema
matches v1 (`id/cat/author/title/date/genre/mesirah/stream`) plus additive fields:
`title_en`, `provenance='sefaria'`, `source_url`, `license`, `ref_kind`, `vgroup`.
All v1 works gained `vgroup=None` (twins get their group id); nothing else touched.
Per-work records: `data/ref2_manifest.json`. Verse/leaf maps live in the
`refs_staging/*.versemap.json` sidecars for the Map-v2 masking pass (not in the pkl).

## Version groups (5-gram set containment ≥ 0.85, same-kind only)

| vgroup | members | max containment |
|---|---|---|
| 1 | `REF2:liturgy_amidah_weekday_shacharit` · `REF2:liturgy_amidah_weekday_maariv` · `REF2:liturgy_amidah_shabbat_maariv` · **`M:Ytext599016`** (v1 Maagarim: קדושת היום בערבית לשבת) | 0.989 (weekday shacharit~maariv) |
| 2 | `REF2:liturgy_amidah_shabbat_shacharit` · `REF2:liturgy_amidah_shabbat_musaf` | 0.869 |
| 3 | `REF2:liturgy_haggadah` · `REF2:liturgy_hallel` | 0.872 (hallel ⊂ haggadah) |
| 4 | `REF2:liturgy_shema_blessings_weekday_maariv` · `REF2:liturgy_shema_blessings_shabbat_maariv` | 0.985 |

Version groups are an asset (multi-rite recall): consumers should treat same-vgroup
hits as one work identity. All Targum books and the 3 b2 works are vgroup-singletons —
**no Targum twin exists anywhere in v1**, confirming Targum was a genuine reference gap.

## Anomalies & judgment calls

1. **Same-kind gate (Bible quotation).** Five liturgy units showed ≥0.85 containment in
   the single v1 Bible work (`M:Ytext1000` מקרא) because statutory liturgy quotes
   scripture wholesale: hallel 0.967 (= Pss 113–118), kiddush_shabbat_day 0.929,
   shema_blessings_weekday_maariv 0.899, shema_blessings_shabbat_maariv 0.887,
   kiddush_friday_night 0.856. These are canonical **quotation**, not version
   relationships — the spec's "same kind" clause was implemented as `v1.cat != 'Bible'`,
   so they were neither twinned nor dropped (logged in
   `ref2_manifest.json:bible_quotation_pairs`). Without this gate the ≥0.85 rule would
   have wrongly bound Hallel to the Bible as a "version", and a slightly cleaner Hallel
   text could even have been auto-dropped at 0.98.
2. **Two new-new pairs ≥ 0.98, both kept** (spec drops only vs v1):
   weekday amidah shacharit~maariv 0.989 and shema-blessings weekday~shabbat maariv
   0.985. Expected — these rites share nearly all their text; they remain distinct
   works inside one vgroup (their small deltas are exactly the rite-discriminating bits).
3. **תפסיר רס"ג is present in v1 yet its residue cluster stayed unidentified** (see gap
   table): the JA corpus contains Saadia's complete Torah tafsir (5 books) plus
   Isaiah/Psalms/Daniel/megillot units. The unit-1430332-class residue naming it is
   therefore *not* a reference gap — worth a Track-1 matching post-mortem (recension
   distance? interleaving with שרח אלאלפאץ?).
4. Targum Onkelos Genesis stream begins mid-Genesis-8 vocabulary in the 1,000-char
   slice sanity check — fine; the staged bodies are whole-book, order preserved by the
   versemap sidecars.

## License notes (from `refs_staging/manifest.json`)

| license | n | works / notes |
|---|---|---|
| Public Domain | 47 | all 42 targum_* (Sefaria, orig. toratemetfreeware.com) + haggadah, kiddush_shabbat_day, amidah_shabbat_shacharit/musaf (Daat Siddur Ashkenaz), keter_malkhut (Davidson/JPS 1923), rif_hilchot_shabbat (Vilna) |
| CC-BY | 7 | liturgy units from **The Metsudah siddur, 1981** (amidah weekday ×2 + shabbat maariv, shema blessings ×3, kiddush friday night) — attribution required on any published artifact; `attribution_text` strings are in the manifest |
| unknown | 4 | `targum_ketuvim_esther_targum_sheni`, `liturgy_hallel`, `liturgy_birkat_hamazon`, `b2_radak_isaiah` — fine for internal matching; **do not republish text** until license is clarified |

All entries carry `reuse_ok: yes` and per-work `attribution_text`; retrieval 2026-07-09
via the Sefaria API.

## Gap-works lookup (residue_naming.md CLEAR/COMPETING named works)

Closed-universe check: Maagarim filenames (8,233), JA per_doc filenames (92), v1
titles/authors (5,363), REF2 new works (58). **Report only — nothing ingested** (any
match needs human confirmation). Raw matches: `data/ref2_gap_lookup.json`.

| # | work (residue cluster naming) | found in | verdict |
|---|---|---|---|
| 1 | שרח אלמקדמאת | — | **ABSENT** (unrecoverable-by-reference) |
| 2 | תפסיר אלאלפאט' אלצעבה / שרח אלאלפאץ | — | **ABSENT** |
| 3 | כתאב אלאפעאל ד'ואת חרוף אללין (חיוג') | — | **ABSENT** |
| 4 | כתאב אפעאל דואת אלמתלין (חיוג') | — | **ABSENT** |
| 5 | כתאב אלמסתלחק (אבן ג'נאח) | — | **ABSENT** |
| 6 | ספור על אסתר / קצת אסתר | — | **ABSENT** |
| 7 | קצת חנה / ספור על חנה | — | **ABSENT** |
| 8 | תפסיר רס"ג | JA corpus → v1 (`J:36–40-רסג-*-תפסיר-תורה` + Isaiah, Psalms, Daniel, megillot, ~26 units) | **already-present** (see anomaly 3 — cluster unidentified despite presence) |
| 9 | פירוש יפת בן עלי למקרא | v1 has only 2 Yefet **piyyutim** (`M:Ytext672001/672002`), not the commentary | **ABSENT** (the dominant absence: Daniel/Psalms/Torah commentary clusters) |
| 10 | כתאב אלהדאיה | — | **ABSENT** |
| 11 | כתאב אלטריפות | — | **ABSENT** |
| 12 | שרוט אלדבאחה (both authors) | — | **ABSENT** |
| 13 | תחכמוני (אלחריזי) | — | **ABSENT** (surprise: Maagarim has no Tahkemoni) |
| 14 | כתאב אלמשתמל (אבו אלפרג' הארון) | — | **ABSENT** |
| 15 | כתאב אלכאפי (אבו אלפרג' הארון) | — | **ABSENT** |
| 16 | כתר מלכות (אבן גבירול) | **REF2** `b2_keter_malkhut` (this ingest) | **present in v2** |
| 17 | שרח אלאלפאט' אלמתג'אנסה / ספר הענק (מ' אבן עזרא) | only a false-positive Halevi poem | **ABSENT** |
| 18 | כתאב אלאצול / השורשים בערבית (אבן ג'נאח) | only Ben-Ze'ev 1807 hakdama (unrelated) | **ABSENT** |
| 19 | כתאב אלאנואר ואלמראקב (קרקסאני) | — | **ABSENT** |
| 20 | ספר מצוות [לוי בן יפת] | v1 `M:Ytext665000` — medieval **Hebrew translation** | **already-present (partial)**: Hebrew translation only; the Judeo-Arabic original the witnesses carry is ABSENT |
| 21 | ספר מצוות [יפת בן דוד אבן צגיר] | — | **ABSENT** |
| 22 | תלכ'יץ תפסיר אבן נוח | — | **ABSENT** |
| 23 | כתאב אלמרשד [שמואל בן משה המערבי] | — | **ABSENT** |
| 24 | רסאלה אלתנביה | — | **ABSENT** |
| 25 | כתאב אלמועט'ה | — | **ABSENT** |
| 26 | דלאלה אלחאירין / מורה נבוכים | — | **ABSENT** (surprise: no Guide, Hebrew or JA, anywhere in the closed universe) |
| 27 | סדור מנהג קראים | 71 Maagarim "קראים" matches are all works *about* Karaites, no siddur text | **ABSENT** (also a rite-class, not one work — route to liturgy masking, not naming) |

**Summary: 27 distinct named works → 1 present in v2 via this ingest (כתר מלכות),
1 fully already-present (תפסיר רס"ג), 1 partially present (ספר מצוות לוי בן יפת,
Hebrew translation only), 24 ABSENT = unrecoverable from the closed reference
universe.** The absentees are dominated by Karaite Judeo-Arabic exegesis/grammar
(Yefet, Abu al-Faraj Harun, ibn Nuh, Qirqisani, Levi b. Yefet JA original, ibn Janah's
grammars) plus JA popular narrative (קצת אסתר/חנה) — recoverable only by acquiring
editions outside the current universe.

## Verification (ref2_verify.py — ALL CHECKS PASSED)

- no duplicate work ids (5,421 unique)
- all 5,363 v1 ids present; 20 random spot-checks: streams byte-identical, cat/title untouched
- all 58 REF2 streams are pure Hebrew base-letter streams (finals folded), ≥200 letters
- categories: Maagarim 5182 · Bible 1 · Mishnah 1 · Tosefta 1 · Bavli 38 · Yerushalmi 51 · JA 89 · **Targum 42 · Liturgy 13 · Sefaria 3**

Sample 100-char stream slices (offset 1000):

```
REF2:targum_onkelos_genesis (85,890 letters):
ארברביאויתכלנפשאחיתאדרחשאדיארחישומיאלזניהונויתכלעופאדפרחלזנוהיוחזאייאריטבובריכיתהוניילמימרפושווסגוומ

REF2:liturgy_birkat_hamazon (5,035 letters):
חקיכשהודעתנוועלחיימחנוחסדשחוננתנוועלאכילתמזונשאתהזנומפרנסאותנותמידבכליומובכלעתובכלשעהעלהנסימועלהפרקנ

REF2:b2_keter_malkhut (15,537 letters):
לנשמהאתהחיולאכחייאדמלהבלדמהוסופועשורמהאתהחיוהמגיעלסודכימצאתענוגעולמואכלוחילעולמאתהגדולומולגדלתככלגדל
```

## Files

- `same_work_spike/probe/scripts/ref2_build.py` — build (NEW; no pipeline scripts touched)
- `same_work_spike/probe/scripts/ref2_gap_lookup.py` — gap-works lookup (NEW)
- `same_work_spike/probe/scripts/ref2_verify.py` — verification (NEW)
- `same_work_spike/probe/data/ref_corpus_v2.pkl` — the v2 corpus (171 MB, gitignored)
- `same_work_spike/probe/data/ref2_manifest.json` — per-work ingest records + vgroups
- `same_work_spike/probe/data/ref2_gap_lookup.json` — raw gap-lookup matches
- `same_work_spike/probe/results/ref2_extract_report.md` — this report

Reminder (project context): Targum joins the canonical class at Map-v2 (ref-side
masking); `CANON_CATS` and consumers were deliberately NOT modified in this step.
