# PGP Hebrew translations — quality finding and remediation plan

**Status: WITHHELD (2026-09-21, owner decision).** The corpus is **not shipped on either
surface.** It was never deployed to the website, and it has been lifted out of
`pgp_data/pgp.db` so the desktop installer stops carrying it -- see
[What was decided](#what-was-decided). Both surfaces now show the English description, which is
what production was already doing. Resuming needs one answer from the owner (see
[Open question](#open-question)).

**Last updated:** 2026-09-21

---

## What this is about

`pgp_data/pgp.db` carries a `pgp_translations` table: one Hebrew rendering per PGP document
description, shown on the desktop PGP info panel
([genizah_app.py](../../genizah_app.py), `_build_pgp_extended_info_html`) and on the web reading
view ([web/pages/browse.py](../../web/pages/browse.py)), gated on the `show_translations`
toggle. The table was lost in the 2026-04-22 sidecar rebuild and regenerated on 2026-09-21
(35,111 rows, zero API failures).

Regeneration succeeded. **The translations themselves are not good enough**, and the checks the
repository already had cannot see why.

## The measurement

A random sample of 180 rows, each graded against its English source by an independent reader,
with every adverse grade re-judged by a second reader instructed to overturn stylistic
nitpicking:

| Grade | Rows | Share |
|---|---|---|
| good | 57 | 31.7% |
| minor (a wart, but nothing false) | 73 | 40.6% |
| **wrong (misleads about a fact)** | 48 | 26.7% |
| **garbage (not a usable translation)** | 2 | 1.1% |

**27.8% wrong or garbage**, 95% CI 21.2–34.3% — roughly **9,700 of 35,005 rows**. The second
reader upheld 50 of 56 adverse grades, so the rate is not an artefact of an over-eager auditor.
Only a third of the corpus is unreservedly good.

Sample and per-row verdicts: regenerate with the audit described in [Reproducing](#reproducing).

## Why the existing QC scored these clean

[`shared/translation_qc.py`](../../shared/translation_qc.py) runs ten checks — script balance,
number drift, bracket and parenthesis mismatch, length ratio, truncation, copied source. All are
**mechanical**. A translation that is fluent, well-formed Hebrew of the right length about the
wrong thing passes every one of them.

Measured against the restored corpus, that QC reports **94.7% clean**. The sampled audit reports
**27.8% wrong**. Both numbers are correct; they measure different things. *94.7% clean has never
been a quality claim and should not be quoted as one.*

A hand-written domain-vocabulary lint does better but not nearly enough — it found 326 rows
(0.9%), roughly a thirtieth of what the audit found, because a lint only catches errors somebody
thought to encode in advance.

## What the failures look like

Each verified directly against the database, not taken on an auditor's word.

| pgpid | English | Hebrew | Why it matters |
|---|---|---|---|
| 12077 | in the hand of Efaryim b. **Shemarya**? | אפרים בן **שמואל** | Names a different person. Efrayim b. Shemarya led the Fustat Palestinian congregation; the attribution of the hand is now wrong. |
| 3762 | sent on the evening **following** the day of Atonement | **בערב** יום כיפור | The eve *before*. The date is reversed, and the next clause — hoping the addressee's fast was accepted — becomes incoherent. |
| 27966 | they have been **renting out** a shop | הם **חכרו** חנות | Renting *from*. Landlords become tenants. Also silently drops "the deceased" and "Karaite". |
| 15178 | **Newly treated** and encapsulated | **חדש** ומעובד | Asserts a medieval fragment is new, and loses the conservation fact. **This boilerplate appears in 2,260 rows and is rendered this way in all 2,260.** |
| 33032 | Few preserved words in **Hebrew script** | כמה מילים **בעברית** בכתב עברי | Adds a claim about language. Script vs language is load-bearing here: Judaeo-Arabic is also written in Hebrew script. |
| 3764 | the addressee's **intimate relations** with the Tustari brothers | **יחסים אינטימיים** | Reads as sexual relations, about named historical figures. |
| 26233 | a few **miles** northwest | כמה **קילומטרים** | A stated distance silently converted. |
| 23202 | **Bifolium** | דף דו-צדדי | "A two-sided leaf" — every leaf is two-sided. A bifolium is a folded sheet of two conjoint leaves. |
| 39934 | the **marriage gifts** amounted to 30 plus 80 dinars | סך ה**נדוניה** | Conflates מוהר with נדוניה, making the entry contradict its own next sentence. |
| 37409 | deed of acknowledgment (**iqrār**) | שטר **איקר** (אקראר) | "איקר" is a meaningless string; the same Arabic word is transliterated two ways in one sentence. |

Recurring classes: domain vocabulary (a *join* between fragments as הצטרפות rather than צירוף; a
*fragment* as שבר; a *beit din* as בית המשפט, in 159 rows; *shelfmark* as ספרייה; "intervene
with" as להתערב עם, which means to place a bet), calques that invert meaning, and silently
dropped glosses.

## The likely root cause

The client calls `dicta-translation.loadbalancer3.dicta.org.il/whatcanthisbe/completions` with
`"model": "dicta-il/dictalm2.0"` ([shared/dicta_client.py](../../shared/dicta_client.py)).
Probing that endpoint: a nonsense model name returns HTTP 404, so the field is validated, not
decorative; an **empty** model name returns 200 and the response reports
`"model": "dicta-il/dictalm2.0"` as the server's own default.

So the corpus was produced by **DictaLM 2.0, a general Hebrew language model, driven by few-shot
completion prompting** — a `/completions` call carrying five scholarly example pairs from
`data/few_shot_en2he_scholarly.json` and a stop sequence. It is a language model continuing a
pattern, not a translation model translating. The owner has confirmed that **Dicta Translate is
a different thing**.

That is consistent with the failure profile: a few-shot general LM drifts on exactly this
material, and has no way to be taught a house glossary.

**Two consequences to note:** the `model_version` column reads `dictalm2.0` for 35,005 rows,
which is accurate about the wire and misleading about the product; and the help page
([web/pages/help.py](../../web/pages/help.py)) tells the public the translations are "powered by
machine translation via Dicta Translation", which on this evidence is not accurate.

## What was decided

The owner ruled on 2026-09-21: **do not ship these translations.** Better no Hebrew than Hebrew
that names the wrong person or reverses a date.

Withholding had two halves, and only the first is obvious. The website never received the restored
sidecar, so nothing had to be undone there. But `GenizahSearchPro.spec` bundles `pgp_data\pgp.db`
into every desktop installer, so the next `build_app.bat` would have shipped the same rows to
desktop users even though the site never saw them. The table was therefore lifted out of the
sidecar entirely:

| | |
|---|---|
| Withheld to | `pgp_data/pgp_translations_withheld_2026-09-21.db` (18.0 MB, gitignored) |
| Verified | 35,111 rows, SHA-256 over every column identical to the source before the drop |
| Effect on the sidecar | 174.8 MB -> 155.8 MB, so the installer shrinks with it |
| Restore | `python scripts/restore_pgp_translations.py` (`--dry-run` first; refuses to overwrite a different corpus without `--force`) |

Nothing was deleted. The 15.1-hour run is kept because it is the baseline any remediation has to
beat: option 2 and option 3 below are only measurable against it.

Behaviour after the withdrawal, verified rather than assumed: `TranslationService` reports
`_pgp_has_translations = False`, `get_pgp_description_he()` and `get_pgp_document_type_he()` return
`None`, and no exception is raised. `is_available()` still returns `True` -- it is satisfied by the
unrelated FJMS sidecar -- so the translations toggle stays visible, correctly, for the surfaces that
do have translations.

One consequence to keep in view: `scripts/export_pgp_sidecar.py` USED TO drop
`pgp_translations` on every rebuild, which happened to work *in favour* of this decision. That was
an accident, not a safeguard. It was fixed on 2026-09-22 -- the table is now carried across a
rebuild -- so the withholding no longer rides on a bug, and is enforced deliberately instead:
`scripts/check_shipping_sidecar.py` refuses to let `build_app.bat` package a sidecar that still
contains the table.

## Options

| # | Approach | Cost | Unknowns |
|---|---|---|---|
| 1 | **Real Dicta Translate**, if it can be called | one overnight run; the worker pool and the stale-checkpoint guard carry over unchanged | needs the endpoint and model id; quality unmeasured |
| 2 | **Better prompting on the current endpoint** — the prompt carries five pairs and no glossary; many failures are vocabulary and calque errors | ~1 hour to measure against the same 180-row sample | may not move enough; a completions endpoint gives limited steering |
| 3 | **Claude translates the corpus** | large; measured 101/106 clean on the hardest rows, against 0/106 for the incumbent | 35,000 rows is a long job |
| 4 | **Targeted fixes only** — the 2,260 "Newly treated" rows plus the 326 lint hits | under an hour | leaves ~9,000 wrong rows untouched. Not sufficient on its own. |

Option 2 is the cheapest thing that produces a number, and it is worth doing before choosing
between 1 and 3 whatever happens.

## Open question

**What is the real Dicta Translate endpoint and model identifier, and should the help page's
claim be corrected in the meantime?** Both are Dicta's own internals; the owner answers them.

## Reproducing

Scripts used for the finding live in the session scratchpad, not the repo. To rebuild:

- Mechanical QC over every row — `run_qc` from
  [shared/translation_qc.py](../../shared/translation_qc.py), or `full_qc_scan_pgp` in
  [scripts/export_translation_audit_sample.py](../../scripts/export_translation_audit_sample.py).
- Domain lint — regex pairs of (English trigger, wrong Hebrew rendering); the terms are listed
  under [What the failures look like](#what-the-failures-look-like).
- Sampled audit — draw N rows at random, grade each against its English on a
  good/minor/wrong/garbage rubric, re-judge every adverse grade with a second reader told to
  overturn stylistic nitpicking. Do not skip the second pass; it overturned 6 of 56 here.

## Related

- Tracker row: `docs/OPEN_ISSUES.md`, "PGP Hebrew translations are materially wrong in ~28% of rows".
- The table's loss and restoration, and why every rebuild drops it:
  [DATA_LIFECYCLE.md](../architecture/DATA_LIFECYCLE.md).
