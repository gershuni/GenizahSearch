# v5 review artifact — how to open it, and what is new

You have been sent two files. This is everything needed to open them; there is
nothing to install beyond Python itself.

| File | Size | What it is |
|---|---|---|
| `discovery-v5-REVIEW.db` | ~3.5 GB | The review set: **519,382** candidate identifications — grouped into **433,911 cards**, one per page and work — each with both sides of the match, the manuscript text and the reference text, and **where each side came from**. |
| `serve_v3_review.py` | ~220 KB | A small local viewer for it. Python standard library only — no `pip install`. |

## Run it

Put both files in the same folder, then:

```
python serve_v3_review.py --db discovery-v5-REVIEW.db
```

Open the address it prints — `http://127.0.0.1:8777`. Python 3.8 or newer.
Nothing is uploaded anywhere: the server listens on your own machine only.

---

## What changed since the v3 file

### 1. Every match now says where it came from

The v3 file showed you two passages. This one also tells you, for each of them,
the file they were taken from and the exact character range inside it:

| Column | Meaning |
|---|---|
| `page_char_start` / `page_char_end` | where the matched span sits in this page's own text |
| `file_char_start` / `file_char_end` | where it sits in the whole transcriptions corpus file |
| `w_start` / `w_end` | the reference work's letter-stream coordinates (what the matcher works in) |
| `ref_char_start` / `ref_char_end` | the character range in the reference work's own source file |
| `witness_id` | joins `reference_witness` → `source_file`: **which file** produced this row's reference offsets |
| `locus_label` | the citable address (e.g. a tractate folio). R-source rows get theirs from the innermost section header preceding the match in the source file — work / tractate / chapter grade, resolved on **all 226,679** of them |

Two cautions worth reading once:

* **Offsets are counted in characters of the NFC-normalized text**, not bytes.
  Where a source's NFC form differs from its raw form, the row says so
  (`ms_provenance_status='nfc_shift'`) and withholds the file offsets rather
  than giving you an address that is off by a character or two.
* **18,982 of 667,411 manuscript pages have no address in the corpus file** —
  their text came from a different source (FGP/PGP). Those rows carry
  `ms_provenance_status='offsets_missing'`, not a guess.

Every offset in this file was verified by an independent re-derivation: a
separate checker, written so that it cannot call any of the code that produced
the offsets, re-computed them from the source files and compared. **424,659
reference-side offsets and 336,899 manuscript-side offsets were checked with
zero mismatches.**

### 2. A fourth reference corpus (R-source)

226,679 rows come from a corpus that was not in the v3 file at all
(`source_corpus` shows as **R-source**). Points to keep in mind:

* These come from a **fresh matching run** using a corrected mask, not from the
  older exploratory pass. Its predecessor had a known contamination problem;
  this run does not.
* R-source matches are mostly **parallels, not witnesses**: the coverage router
  labels 95,287 groups `parallel` against 36,373 `same_work`. That is expected —
  much of this corpus is later literature quoting earlier texts, so a match
  usually means "this page is the text being quoted", not "this page is a copy
  of that work".
* **Novelty HAS been run for these rows** (the same pinned gate the site
  uses; heuristic pass first, the residual judged by the model). 195,544 of
  226,679 carry a verdict — `diverges_work` 131,482, `confirms` 31,393,
  `fills_gap` 21,794, smaller shades 10,875. The 31,135 `not_checked` are
  pairs with no shipped evidence, which the gate never receives — an honest
  absence. `diverges_work` dominating is expected here: most R-source matches
  are quotations, where the catalogue names the page's own text and the match
  names the work quoting it.

### 3. Compilations are marked, not hidden

Some works are anthologies or compendia — Yalkut Shimoni, Machzor Vitry, the
Tur, Shibolei HaLeket, Siddur Rashi. A match to one of them usually witnesses
the *source it compiled*, not the compilation, so they were ruled out of the
public site. They are **kept here on purpose** (they still suppress worse
matches) and flagged, so you can see what you are looking at:

* `owner_ruling` — the decision already taken, with `owner_ruling_date` and a
  note giving the reason.
* `compilation_risk` — a computed suspicion for works nobody has ruled on
  (`high`/`medium`/`low`), derived from how much of the work is quotation of
  other texts. It excludes nothing by itself; it is there so these can be
  judged later.

20,057 rows sit on works carrying an owner ruling of some kind (the rulings
also cover works dropped as reference sources or excluded from public
identities, not only compilations); the ruling, its date and its note appear
as a chip on each such row. `compilation_risk` shows as a chip on `high` and
`medium` rows.

### 4. Shared-scripture matches are flagged

Much of what an R-source work shares with a Genizah page is text **both sides
are quoting** — a Bible fragment "matching" Seder Olam at exactly the verse
Seder Olam cites. The pre-matching mask catches most of this; what escapes it
(variant wording, quotes broken by inline citations) is now flagged on the row:
**may rest on shared scripture**, with the chip naming which detector fired —

* at least half the matched text occurs verbatim in the Bible or in
  Mishnah/Tosefta/Talmud/Targum;
* a citation formula (שנאמר, דכתיב, וגו׳, a parenthesized book citation) sits
  at the match boundary **and** the match is short (under 150 letters);
* at least half the span lies inside quotation intervals the mask already
  caught in that work.

80,218 rows (23% of those scored) carry the flag. A sidebar card filters on
it. It is a **review label, never a relation verdict**: flagged rows are not
hidden, demoted, or excluded, and the detectors' thresholds are recorded in
the file's own `meta` table (`scripture_fact.thresholds`). Computed for every
corpus **except works that are themselves canonical scripture** (Bible,
Targum, Mishnah, Talmud, Tosefta, Massorah) — there a verbatim-scripture span
*is* the identification, so those rows read "not computed". The
mask-adjacency detector runs only where a mask artifact exists (R-source);
elsewhere the other two detectors carry the flag.

### 4b. Model adjudication of divergences and new-find candidates

Two LLM passes (gemini-3.7-flash, 2026-08-31) judged every main+unclear pair
at identification grain — one verdict per (manuscript, work), on the pair's
strongest page, reading the catalogue's own prose, bibliography, PGP, the
aligned excerpts and the computed signals:

* **Catalogue vs computed** (23,997 divergent pairs): who is right when the
  catalogue points at a different work? Validated on 117 owner-graded cases —
  "catalogue right" verdicts were 98.8% owner-confirmed. The one verdict to
  distrust is **"model claims the catalogue is wrong"**: it asserts scholars
  erred and went 0-for-4 when contested — read it as *needs human review*.
* **New-find candidates** (11,045 pairs no finding aid records): 7,130 judged
  **credible new identifications** (precision 58/60 on the owner's graded
  cases), each carrying a `doubt` — the one thing an expert should verify.

Both are **labels, never verdicts**: they move nothing between pools. Two
sidebar cards under Advanced filter on them; each row's chip tooltip shows
the model's reason and doubt.

### 5. Work titles were checked by two models

The R-source titles had never been reviewed. Two independent models judged all
351 of them against their catalogue metadata; the 104 they disagreed on were
adjudicated with a recorded reason. `title_provenance` on each work says which
route it took (`both_agreed_correct`, `adjudicated`, `both_agreed_wrong_corrected`,
or `unsure_using_catalogue`). This caught real errors — six unrelated works had
all inherited the title "הלכות גדולות", and fifteen had no title at all.

### 6. One question per page and work: **cards**

A row in this file is one *alignment*. The same page and the same work can
therefore appear several times, because a work has several witnesses — an
R-source whole-work file and the base corpus's per-book works, two editions,
two halves of one midrash. Asked as rows, you would judge the same question
three or sixteen times.

So the viewer opens in **card** grain: one card per (page, known work), with
every witness's evidence beneath it. **519,382 evidence rows become 433,911
cards** over 211,510 pages and 55,383 manuscripts; 54,583 cards (12.6%) hold
more than one row, the largest 22. A `Cards / Evidence rows` toggle in the
result bar switches grain at any time — the row view is unchanged from before.

Nothing is merged and nothing is averaged:

* Each card keeps **every** evidence row, with its raw `work_id`, which witness
  it came from, and the offsets pointing at that witness's own file. The grade
  buttons stay on the rows, so your grades key to evidence exactly as before.
* Where a card's rows **disagree** — one says `confirms`, another
  `diverges_work` — the card reads **mixed** rather than picking a side
  (12,771 cards for novelty, 10,978 for pool, 5,592 for relation).
* Where its rows cite **different addresses**, the card says how many rather
  than choosing one ("13 addresses").
* The header reports **three numbers, never one**: cards, the evidence rows
  inside them, and manuscripts. The counts beside each sidebar control still
  count evidence rows, and the sidebar says so.

The **witness strip** under each card header is the honesty surface. Every
witness of the known work is listed, aligned here or not:

* **aligned here (n)** — this witness produced n of the card's rows.
* **no returned alignment** — the engine ran this witness and returned nothing
  for this page. That is *not* the same as the witness being irrelevant, and the
  strip does not pretend to know which. 75,055 cards have at least one such
  witness.
* **not applicable here** — said **only** where the file can prove it: a
  division-scoped witness of an anthology whose rows on this page belong to a
  *different* division, where the divisions partition that container's rows.

### 7. Works that are one work are now one identity

1,701 corpus works resolve to **1,447 known works**, with 1,736 witness
memberships. This is what lets a card ask its question once. The identity of
each known work comes from exactly one source, recorded in `title_basis`:

| basis | count | what decided it |
|---|---|---|
| `singleton` | 1,329 | nothing to merge |
| `census_canonical` | 63 | the production reference-merge contract |
| `cluster` | 20 | a same-work link between corpora |
| `family` | 17 | a container file and its parts are one work |
| `mint` | 9 | a named division with no work of its own to point at |
| `work_group` | 5 | an owner ruling that two halves are one work |
| `owner_merge` | 4 | an owner ruling this artifact carries on its own |

Two things to read carefully:

* **`work_relation` is not identity.** 16 pairs are recorded as
  `shares_material` — works that share text without being the same work
  (Tanchuma recensions, a reworking of ספר המצוות, geonic responsa shared
  between collections). They were demoted from identity links by explicit
  ruling; the viewer shows them as a chip, never as a merge.
* **One known work is `provisional`** — minted from a routing nobody has ruled
  on yet. It says so on the card.
* `known_work_assertion` holds identity claims with **no evidence rows at all**
  (one row: a census member absent from this artifact). It is never evidence and
  never reaches a card.

### 8. Authors and titles were corrected by hand

The author column was rebuilt around one rule: **one canonical string per
person**, in the form *full name + acronym* with Hebrew gershayim
(`שלמה בן יצחק (רש״י)`). 215 owner rulings are recorded inside the file in
`work_author_ruling` — the old value, the new one, and why — so nothing can
quietly re-derive over a decision.

What changed, and what it means for reading:

* **No author is derived from a work's own title any more.** 37 works had one;
  every single one repeated a name already spelled in its title, so it said
  nothing the title did not. They now carry no author.
* **Attribution notes are not authors.** "מיוחס לר' אליעזר בן הורקנוס (נתחבר
  במאות ה-8-9)" is a note about an attribution plus a dating; the five
  פרקי (ד)רבי אליעזר works now carry no author.
* **"אנונימי" is not an author** either — it is the absence of one, and those
  works are now empty rather than asserting anonymity.
* **862 of 1,701 works carry an author; 839 do not**, and that is mostly
  correct rather than incomplete — the largest are Bible books, which have no
  author. An empty author is never "unknown author" on screen; the line simply
  does not appear.

---

## What to do in it

You are reading **cards** by default — one per (page, known work), each holding
its witnesses' evidence (section 6). Everything below describes the filters and
signals, which work the same in either grain; the `Cards / Evidence rows` toggle
sits in the result bar if you want the old row-at-a-time view.

**The first card sorts everything into four pools** — deliberately named so
they claim nothing:

* **Main pool — witness candidates, nearly the whole page matches**
  (117,611 rows) — the default view. Two bars, both set by blind owner-graded
  decks over THIS artifact: 85% of the page's letters for most corpora
  (graded 40/40 clean; 70–85% graded 82.5%), and 75% for R-source (graded
  37/40 — its letter-exact coverage against printed editions tops out at
  83.5%, so 85% would exclude the corpus wholesale). Works owner-ruled out
  as identification references never enter the pool.
* **Citation relationship** — one text quotes the other (158,709).
* **Only shared quotations** — both sides quote the same third text,
  near-useless for identification (100,341). This now includes matches whose
  section, by the work's own header, is a fixed prayer or a notarial formula
  embedded in a non-liturgy work (Mishneh Torah's prayer appendix, Seder Rav
  Amram's orders, Sefer ha-Shetarot's deeds) — carrier text every siddur or
  deed shares.
* **Unclear / borderline** (142,721) — matches to standalone liturgy units
  (an Amidah, a Haggadah — a generic prayer excerpt identifies no page),
  held-back rows, witnesses between the
  router's validated 29.8% line and the 85% main-pool bar, conflicting
  signals.

The rule is deterministic and recorded in the file (`doc.triage` in `meta`;
also the card's tooltip). Every row's first chip states its pool. The raw
signals — the router's relation, span rank, page coverage, shared scripture,
the public site's own display rule — live under **"Advanced — the raw signals
behind the triage"**, collapsed; open it to see *why* a row landed where it
did, or to filter on a single signal.

R-source works sit in the same **domain tree** as every other corpus (their
own source taxonomy, hand-mapped onto the existing 19 domains — the mapping is
in the file's `meta`). Each row shows its **Locus** — the citable address —
on the card, and a **"Locus — the citable address"** box in the sidebar
filters by substring (a tractate, a chapter, a siman). Choosing a **Work**
adds a **Part of work** From/To control below it — the work's loci in the
order the work runs. Works sharing one title (two different works are both
called רש״י) carry their domain in the dropdown so you can tell them apart.
The manuscript preview pane embeds the **live web viewer** by default
(`--preview image` restores the plain folio image).

A **Page coverage** card filters by the router's own quantity. The 30–40%
band is where a long quotation most easily slips over the 29.8% witness line;
**witness + novelty `diverges_work` + coverage 30–40%** (8,258 rows) is the
population of witness claims most worth suspicion.

Filter to something you know — a work, an author, a domain — and read the two
panes side by side. Click **What these columns mean** at the top: every column
is explained there. Terms you will meet on the way, in one line each:

* **The router** is the automatic rule that labels a page's relation to a work
  — witness or quotation — by how much of the page the match covers.
* **Pool**: "main pool" rows met the site's display rule; "more matches" rows
  did not meet it — which is a statement about the *evidence*, never that the
  identification is wrong.
* **Tiers / bands** grade how strong the matching evidence is; "tier A" is the
  strongest. You do not need them to grade.
* **R-source**, like the other corpus labels, is an internal codename on
  purpose; it is not meant to be decoded.

**When you are done** (or want to send partial progress): send back the file
`discovery-v5-REVIEW.db.grades.db` that appears next to the database — or
click **Export grades** and send the downloaded file. Either carries
everything you graded.

The one thing only a person can do is the **grade** buttons. Where our
identification and the catalogue disagree, which is right? The model scored 8
out of 28 on that question, so its answer is worthless and the column ships
empty. Yours is the only signal. Grades save to a separate file
(`discovery-v5-REVIEW.db.grades.db`) the moment you click.

## Known limitations, stated plainly

* **"What the software read" is not available in this file.** The novelty
  verdicts are real, but the bundle of catalogue/bibliography text the gate
  read when judging was not extracted for this build — the button on each row
  says so. Grading does not depend on it.
* **Manuscript shelfmarks for R-source-only manuscripts come straight from
  the catalogue file** (first listed variant), not from the curated display
  table the other corpora use — 124 manuscripts have no shelfmark anywhere
  and show their system number instead.

* **One row of 519,382 trips the automated name-screen.** It is a Hebrew word in
  a genuine Genizah transcription that happens to coincide with a restricted
  corpus name; the word occurs naturally in 5 of 667,411 pages. It is a
  coincidence, not a leak, and was accepted as a documented exception.
* **19 of 354 R-source works sit in over-merged identity groups** (one group has
  293 members, chained together through shared liturgical and biblical text).
  Their grouping — and therefore their routing — should be treated with
  suspicion. Their character offsets are unaffected: those are keyed to the
  individual file, not the group.
* **The R-source rows were not compared against the live site's own claims.**
  Where the live site already identifies a page, this file will not tell you so
  for the R-source half.
* Reference text and manuscript text will **not** match closely. A Genizah
  fragment against a printed edition runs roughly 0.4 apart per character —
  orthography, abbreviations, real variants. That is what a witness looks like.

Please do not forward this file. It carries reference-corpus text; within the
team is fine, a public share is not.
