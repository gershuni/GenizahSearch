# v3 quote-identification review — how to open it

You have been sent two files. This is everything needed to open them; there is
nothing to install beyond Python itself.

| File | Size | What it is |
|---|---|---|
| `discovery-v3-REVIEW.db` | ~1.5 GB | The review set: 254,612 candidate identifications, each with **both sides of the match** — the manuscript text and the reference text it was matched against. |
| `serve_v3_review.py` | ~36 KB | A small local viewer for it. Python standard library only — no `pip install`. |

## Run it

Put both files in the same folder, then:

```
python serve_v3_review.py --db discovery-v3-REVIEW.db
```

Open the address it prints — `http://127.0.0.1:8777`.

Python 3.8 or newer — nothing else. Start is immediate; the filter index is
already inside the DB. Nothing is uploaded anywhere: the server listens on your
own machine only, and the address works only on that machine.

If port 8777 is busy it moves to the next free one and prints which — read the
address off the console rather than assuming 8777.

## What to do in it

Filter down to something you know — a work, an author, a domain — and read the
two panes side by side. Click **? What do these mean** at the top for what each
label on a row means; every column is explained there.

The one thing only a person can do is the **grade** buttons at the bottom of a
card. Where our identification and the catalogue disagree, which is right? The
model was measured at 8 correct out of 28 on that question, so its answer is
worthless and the column ships empty. Yours is the only signal.

Grades are saved to disk the moment you click, into a separate file
(`discovery-v3-REVIEW.db.grades.db`) that is created for you. Nothing is lost if
you close the browser or the server. **Export grades** in the header writes them
out as JSON to send back.

The sidebar mirrors the public findings page — same controls, same grouping — plus the
axes only this file carries: **relation** (the router's witness-vs-quotation verdict),
**span rank**, **match length**, corpus, pool reason, band, review state, and
graded/ungraded. One chip selects the 45,149 rows where the router and the span rank
disagree.

If the folio image does not load for a manuscript, add `--preview frame` to embed the live
browse page instead, or just use the `open in a tab` link on the row.

## Three things that will look wrong and are not

- **The two panes will not read as the same text.** A Genizah fragment against a
  printed edition diverges heavily — different spelling, different abbreviation,
  damage. Roughly 40% apart is normal, not a bug.
- **"Catalogued as:" is NOT what the software read.** It is one field of the library
  record. The novelty label was decided on more — the FJMS catalogue entry, the
  bibliography, PGP and FGP transcriptions. On 83% of `confirms` rows the line you see
  differs from the text that was actually judged, so a label can look absurd beside it and
  still be defensible. One measured example: a manuscript catalogued only as `קובץ`
  ("miscellany") is marked `confirms` because the bibliography records it in two Schäfer
  catalogues *of Hekhalot manuscripts*. Judge the label on the two text panes, not on this
  line.
- **Some short matches rest on shared scripture or liturgy.** The screen that is
  supposed to suppress those is running against a stale list, so a passage both
  texts merely *quote* can still appear as though it identified the work. Short
  matches on famous passages deserve suspicion. This is a known gap, not a
  surprise.

## Please do not forward it

The DB embeds reference text from a corpus we hold under restricted terms. It is
fine within the team; it must not go onto a public share, a public repository, or
anywhere outside it.
