# Citation-Guard Statutory Liturgy Additions

**Date:** 2026-07-10
**Source:** Sefaria API (`www.sefaria.org/api/v3/texts`, `/api/name`, `/api/shape`, `/api/search-wrapper`)
**Scope:** Add the statutory liturgy units the citation-guard was leaking on. All new entries
carry `"guard_only": true` and are appended to `refs_staging/manifest.json`. **The frozen
`data/ref_corpus_v2.pkl` was NOT touched** and nothing was ingested/compiled.

Manifest went from **58 → 73 entries** (+15 guard-only). All 15 body files pass integrity
checks: Hebrew-only, no HTML/entities/brackets, no nikud/te'amim, no Latin/digits,
`char_count` matches body length, each has a `*.versemap.json` sidecar.

Text transformation matches the existing REF-2 liturgy files: `<small>`/`<sup>`/footnote
blocks dropped, `[קהל: …]` response cues dropped, tags stripped, entities unescaped,
nikud/te'amim removed, punctuation dropped, maqaf→space, final letters preserved.

---

## 1. Already present (REF-2 — NOT duplicated)

REF-2 staged **13** `liturgy_*` units (all `ref_kind: modern_rite_mask_only`):

`liturgy_amidah_weekday_shacharit`, `liturgy_amidah_weekday_maariv`,
`liturgy_amidah_shabbat_maariv`, `liturgy_amidah_shabbat_shacharit`,
`liturgy_amidah_shabbat_musaf`, `liturgy_shema_blessings_weekday_shacharit`,
`liturgy_shema_blessings_weekday_maariv`, `liturgy_shema_blessings_shabbat_maariv`,
`liturgy_kiddush_friday_night`, `liturgy_kiddush_shabbat_day`,
`liturgy_hallel`, `liturgy_birkat_hamazon`, `liturgy_haggadah`.

Relevant to the 7 requested units: **unit 7 (קידוש)** was **partially** covered —
Shabbat evening (`liturgy_kiddush_friday_night`) and Shabbat day
(`liturgy_kiddush_shabbat_day`) already existed, so only the **festival** kiddush was missing.
None of units 1–6 were present.

---

## 2. Units ADDED (15 entries)

| Requested unit | Manifest key(s) | Sefaria source / license |
|---|---|---|
| **1. כל חמירא / ביעור חמץ** | `liturgy_kol_chamira_peninei` (night + morning declarations) | Peninei Halakhah, Pesach 5:1 — CC-BY-NC |
| | `liturgy_kol_chamira_shulchan_arukh` (older nusach) | Shulchan Arukh, OC 434 — Public Domain |
| **2. קדיש (4 recensions, Ashkenaz)** | `liturgy_kaddish_chatzi_ashkenaz` (חצי קדיש) | Siddur Ashkenaz, Kaddish, Half Kaddish — PD (Daat) |
| | `liturgy_kaddish_shalem_ashkenaz` (קדיש שלם/תתקבל) | …, Kaddish Shalem — PD |
| | `liturgy_kaddish_yatom_ashkenaz` (קדיש יתום) | …, Mourner's Kaddish — PD |
| | `liturgy_kaddish_derabbanan_ashkenaz` (קדיש דרבנן) | …, Kaddish d'Rabbanan — PD |
| **3. ברכת המפיל + ק"ש שעל המטה** | `liturgy_hamapil_bedtime_shema_sefard` | Siddur Sefard, Bedtime Shema — CC-BY (Metsudah) |
| **4. שבע ברכות (wedding)** | `liturgy_sheva_brachot` (the 7 blessings) | Birkat Hamazon, Sheva Brachot — unknown |
| | `liturgy_marriage_blessings_erusin` (erusin/kiddushin) | Siddur Sefard, Various Blessings, Marriage Blessings — CC-BY |
| **5. ברכות השחר** | `liturgy_birkot_hashachar_ashkenaz` (18-blessing sequence) | Siddur Ashkenaz, …, Morning Blessings — CC-BY |
| | `liturgy_birkot_hashachar_sefard` (netilat/asher-yatzar/elohai-neshama) | Siddur Sefard, Weekday Shacharit, Morning Blessings — CC-BY |
| **6. הבדלה (2 rites)** | `liturgy_havdalah_ashkenaz` | Siddur Ashkenaz, Shabbat, Havdalah — PD |
| | `liturgy_havdalah_edot_hamizrach` | Siddur Edot HaMizrach, Havdalah — CC0 |
| **7. קידוש (festival)** | `liturgy_kiddush_yomtov_evening_sefard` | Siddur Sefard, Holidays, Yom Tov Eve Kiddush — PD |
| | `liturgy_kiddush_yomtov_day_sefard` | Siddur Sefard, Holidays, Yom Tov Daytime Kiddush — PD |

All 7 requested units are now covered, several with multiple rites/recensions (kaddish ×4,
havdalah ×2 rites, morning-blessings ×2 rites, kol-chamira ×2 recensions, wedding ×2,
festival kiddush ×2), matching the "HTR pages come from all rites" requirement.

---

## 3. Notes, caveats, and what could NOT be fetched cleanly

- **כל חמירא — no clean liturgical node.** Sefaria's `Siddur Sefard, Nissan, Search for
  Hametz` / `Burning Hametz` nodes are indexed for search but the read API returns **empty
  Hebrew** (no served version text), and the standard `Pesach Haggadah` schema starts at
  *Kadesh* (no Bedikat-Chametz node). The declaration formula is therefore sourced from
  halakhic texts that quote it verbatim: both bittul declarations (night bedikah + morning
  biur) were regex-extracted (bounded `כל חמירא … כעפרא דארעא`) from **Peninei Halakhah,
  Pesach 5:1**, and the older short nusach from **Shulchan Arukh, OC 434**. This keeps the
  guard text pure declaration (surrounding prose / Yiddish glosses excluded). Chayei Adam and
  Kitzur Shulchan Arukh were rejected (KSA only abbreviates "כל חמירא וכו'"; Chayei Adam mixes
  in Yiddish that survives Hebrew-letter cleaning).
- **קדיש — Ashkenaz only.** The four common recensions come from the dedicated
  `Siddur Ashkenaz, Kaddish` node. Sefaria has **no clean standalone Sefard/Edot-HaMizrach
  Kaddish node** (kaddish there is embedded inside larger service sections), so a second-rite
  kaddish was not added. The 4 Ashkenaz recensions fully satisfy the "all common recensions"
  ask.
- **ברכת המפיל / bedtime שמע — Adon Olam excluded.** The Sefard Bedtime-Shema section ends
  with the *Adon Olam* piyyut (segments 26–35); these were deliberately dropped so the guard
  does not over-demote a genuine shared piyyut. Kept: HaMapil blessing + Shema + first
  paragraph + Psalm 91 + Hashkiveinu + the standard protective verses (21 lines).
- **Licensing:** `liturgy_kol_chamira_peninei` is **CC-BY-NC** (`reuse_ok: noncommercial_only`);
  all others are Public Domain / CC0 / CC-BY / unknown, consistent with existing entries.
- **⚠ Future `ref2_build.py` re-runs must filter `guard_only`.** `cat_for()` currently keys off
  the `liturgy_` prefix and would categorize these as `cat='Liturgy'` and ingest them into
  `ref_corpus_v2.pkl`. These entries are citation-guard material only and must be excluded from
  any census rebuild (skip entries where `entry.get('guard_only')`).
- Minor cosmetic artifact: `liturgy_kiddush_yomtov_evening_sefard.txt` has one stray mid-token
  space ("ו מועדים"); immaterial because `normalize.norm_stream` drops all spaces.
