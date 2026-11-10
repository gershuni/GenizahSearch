# Few-Shot Prompt Comparison: Dicta Defaults vs Custom Scholarly

**Date:** 2026-03-04
**Model:** dicta-il/dictalm2.0
**Sample size:** 20 (10 HE->EN, 10 EN->HE)
**Temperature:** 0 (deterministic)

## Method

- **Default prompt:** Minimal generic example ("Hello" / "שלום") with no domain-specific context
- **Scholarly prompt:** 5 example pairs of Genizah manuscript descriptions in scholarly register
- **HE->EN samples:** FJMS catalog entries with known English ground truth (Title/TitleHeb pairs)
- **EN->HE samples:** PGP document descriptions (quality assessed by length ratio and completeness)

## HE->EN Results (FJMS Catalog, with Ground Truth)

| # | Source (Hebrew) | Ground Truth (EN) | Default | Scholarly | Winner |
|---|----------------|-------------------|---------|-----------|--------|
| 1 | ביאור ל"על הפירוש" של אריסטו | תפסיר מתי לכתאב בארי ארמיניאס ... | Commentary on Aristotle's "On ... | Commentary on Aristotle's "On ... | tie |
| 2 | אונה מאדרי קומיו אשאדו אה שו פיג'ו | אינדיג'ה (קנה) לתשע באב במבנה ... | I will go to my mother's house... | I am leaving you with my son | default |
| 3 | זמירות ישראל | יה קשה כשאול קנאתי | Songs of Israel | Songs of Israel | tie |
| 4 | אונה מאדרי קומיו אשאדו אה שו פיג'ו | אינדיג'ה (קינה) לתשע באב במבנה... | I will go to my mother's house... | I am leaving you with my son | default |
| 5 | כתאב אלפוז אלאצגר | כתאב אלפוז | The Book of the Most Excellent... | Kitab al-Fawz al-Aghar | scholarly |
| 6 | כתאב אלפוז אלאצגר | כתאב אלפוז | The Book of the Most Excellent... | Kitab al-Fawz al-Aghar | scholarly |
| 7 | קיצור הלכות שחיטה לרס"ג | בשמך רחמנא הלכות שחיטה מכתצרה ... | Abbreviated Halakhot of Shehit... | Abbreviated laws of slaughteri... | default |
| 8 | קיצור הלכות שחיטה לרס"ג | בשמך רחמנא הלכות שחיטה מכתצרה ... | Abbreviated Halakhot of Shehit... | Abbreviated laws of slaughteri... | default |
| 9 | קיצור הלכות שחיטה לרס"ג | בשמך רחמנא הלכות שחיטה מכתצרה ... | Abbreviated Halakhot of Shehit... | Abbreviated laws of slaughteri... | default |
| 10 | קיצור הלכות שחיטה לרס"ג | בשמך רחמנא הלכות שחיטה מכתצרה ... | Abbreviated Halakhot of Shehit... | Abbreviated laws of slaughteri... | default |

**HE->EN Score:** Default=6, Scholarly=2, Tie=2

## EN->HE Results (PGP Descriptions, Quality Assessment)

| # | Source (English) | Default (HE) | Scholarly (HE) | Winner | Notes |
|---|-----------------|--------------|----------------|--------|-------|
| 1 | Fragment of a draft letter in the n... | קטע מטיוטת מכתב בשם קהילת פוסט... | קטע מטיוטת מכתב בשם קהילת פוסט... | tie | D-ratio=0.60, S-ratio=0.63 |
| 2 | Fragment of a legal document in whi... | קטע של מסמך משפטי שבו מופיעה ח... | קטע ממסמך משפטי שבו מופיעה חתי... | tie | D-ratio=0.67, S-ratio=0.64 |
| 3 | A writ of qiddushin (betrothal), Ty... | כתובת קידושין, צור, 1011-1037 ... | כתב קידושין, צור, בערך 1011-10... | tie | D-ratio=0.66, S-ratio=0.62 |
| 4 | Legal document. According to Ashtor... | הסכם בין מוחדהב בן רדיה ושוקרה... | מסמך משפטי. לפי אשתור, "הסכם ב... | scholarly | D-ratio=0.16, S-ratio=0.65 |
| 5 | Letter from Mufaḍḍal, probably in F... | Letter from Mufaḍḍal, probably... | מכתב ממופדל, כנראה בפוסטאט, לא... | scholarly | D-ratio=7.21, S-ratio=0.78 |
| 6 | Legal document for authorization fo... | שטר הרשאה לשחיטת עופות. תאריך:... | מסמך משפטי לאישור שחיטת עופות.... | tie | D-ratio=0.77, S-ratio=0.93 |
| 7 | Letter from Mardūk b. Mūsā to Nahra... | מכתב ממרדוך בן מוסא לנהרי בן נ... | מכתב ממרדוך בן מוסא לנהרי בן נ... | tie | D-ratio=0.62, S-ratio=0.60 |
| 8 | Letter, apparently by Mevorakh b. N... | מכתב, כנראה מאת מבורך בן נתן ב... | מכתב, ככל הנראה מאת מבורך בן נ... | tie | D-ratio=0.76, S-ratio=0.80 |
| 9 | Ketubba fragment (marriage contract... | קטע כתובה. מקום: דמשק. תאריך: ... | קטע מכתובה (חוזה נישואין). מיק... | tie | D-ratio=0.63, S-ratio=0.85 |
| 10 | Marriage contract from Tyre, dated ... | שטר כתובה מטייר, יום שלישי, י"... | שטר כתובה מטיר, יום שלישי, 19 ... | tie | D-ratio=0.64, S-ratio=0.68 |

**EN->HE Score:** Default=0, Scholarly=2, Tie=8

## Overall Summary

| Metric | Default | Scholarly | Tie |
|--------|---------|-----------|-----|
| HE->EN | 6 | 2 | 2 |
| EN->HE | 0 | 2 | 8 |
| **Total** | **6** | **4** | **10** |

## Analysis

The automated scoring shows Default winning HE->EN (6 vs 2), but this metric is misleading:
the FJMS "ground truth" (Title column) is often not a direct translation but a different catalog
entry, making string-based comparison unreliable for scoring.

For EN->HE (the primary PGP batch use case), the scholarly prompts show clear advantages:
- **Sample 4:** Scholarly produced a proper translation ("mismakh mishpati...") while Default
  truncated to just the agreement clause
- **Sample 5:** Scholarly produced correct Hebrew while Default echoed the English input verbatim

Both prompts produce comparable results when the model has sufficient context (most ties).
The scholarly prompts add value specifically for edge cases where domain vocabulary matters.

## Conclusion

**Decision: Use Custom Scholarly Few-Shots for Production**

While the automated scoring slightly favored defaults for HE->EN, the scholarly prompts are
adopted for production because:

1. **EN->HE quality:** Scholarly prompts never failed (0 default wins) and won on edge cases
   where domain terminology matters most
2. **Domain consistency:** Scholarly examples establish proper register for manuscript metadata
   (legal documents, court records, merchant letters) that the default "Hello/Shalom" prompt
   cannot provide
3. **DictaLM 2.0 is already strong:** The model handles most translations well regardless of
   prompt, so few-shot quality mainly matters for challenging edge cases
4. **No downside:** Scholarly prompts match or exceed default quality in all EN->HE cases

The production few-shot templates are:
- `data/few_shot_en2he_scholarly.json` (5 EN->HE pairs: merchant letters, legal docs, court records, literary texts, household lists)
- `data/few_shot_he2en_scholarly.json` (5 HE->EN pairs: Torah portions, Talmud commentary, liturgical poetry, Rambam responsa, deeds of sale)

## Recommendations

1. Use scholarly few-shot templates for batch translation (domain consistency, edge case quality)
2. Keep 5 examples per template (tested with 5, good balance of quality vs prompt length)
3. Temperature 0 for deterministic, reproducible translations
4. Stop sequence `\n\n` for multi-sentence descriptions
5. Max tokens 1024 (sufficient for all observed description lengths)
6. Monitor translations for the ~174 descriptions >2000 chars -- may need reduced few-shot count
