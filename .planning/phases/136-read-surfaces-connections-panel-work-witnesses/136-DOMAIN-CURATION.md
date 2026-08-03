# Phase 136 Plan 09 — Work→Domain Curation and the Author Alias Map

**Recorded:** 2026-08-03 · **Plan:** `136-09-PLAN.md` · **Requirements:** NOVEL-01, PANEL-02
· **Updated:** 2026-08-03 (rulings P and Q applied — the halt is resolved)

Two hash-pinned artifacts and the rows the owner ruled on.

> **✅ THE HALT IS RESOLVED — ALL 29 HELD ROWS ARE RULED.**
> `136-GATE1-DECISIONS.md` § D recorded the owner's verdict verbatim: *"THE OWNER WILL
> RULE. The 'ship as Unassigned' default is explicitly DECLINED."* Plan 136-09 was
> therefore required to **produce** the `needs-ruling` work list and halt, not to invent
> a default. It did (§ 4 below: **23 decisions covering 29 of the 1,073 rows, 2.7%**),
> and on **2026-08-03** the owner ruled: **§ Ruling P** settles 5 rows from FJMS's own
> work-level domain, **§ Ruling Q** delegates the remaining 24 (*"Go with your judgements,
> I trust you"*). Every ruled row now carries its `domain_leaf`, its `domain_parent` and
> an `owner_ruling` citation; the artifact was **re-emitted and re-pinned**; and
> **`--validate --release` exits 0**. See § 4.5 for the applied rulings.

---

## 1. What was produced

| Artifact | Path | Content hash |
|---|---|---|
| Work → FJMS domain | `discovery_data/work_domains-v1.json` | `sha256:573937731e2e31f4ad3fccd6f84aadecc7e67210bf4cda82513dfc5c4d94f605` |
| Author alias map | `discovery_data/work_author_aliases-v1.json` | `sha256:acce47f67dcde456eb477fc092294ee42546963f5d977549f53e635da65f8a64` |
| Harness + validator | `scripts/curate_work_domains.py` | committed |
| Tests | `tests/test_work_domains.py` (41) | committed |

Both artifacts live in `discovery_data/`, which is **gitignored** — exactly like
`novelty_hardcase_labels-v1.json`, `v2_canonical_merges.build.json` and every sibling
curated input. The content hash recorded here is therefore the pin; re-emitting from the
same asset reproduces it.

The hash is computed over the payload array **only** (`assignments` / `aliases`), so it
survives any later change to the artifact's own header fields — the same recipe
`novelty_hardcase_labels-v1.json` uses for its `cases` array.

**Source asset:** `discovery-v1-33499c5b89f9e635565cd1cc8831c012f5373811c2870ddbda7d303e60d4c5ff`
(the deployed v2 asset; D-01).

**Validation, run as the last step (2026-08-03, after rulings P and Q were applied):**

```
$ python scripts/curate_work_domains.py --emit-artifact
wrote discovery_data/work_domains-v1.json
content_hash=sha256:57393773…
counts={"total": 1073, "by_confidence": {"high": 1012, "medium": 32,
        "needs-ruling": 29}, "unassigned": 0,
        "needs_ruling_held": 0, "needs_ruling_ruled": 29}

$ python scripts/curate_work_domains.py --validate discovery_data/work_domains-v1.json
VALID: 1073 assignment(s), content_hash=sha256:57393773…                       exit 0

$ python scripts/curate_work_domains.py --validate discovery_data/work_domains-v1.json --release
VALID: 1073 assignment(s), content_hash=sha256:57393773… [RELEASE GATE PASSED]   exit 0

$ python scripts/curate_work_domains.py --validate-aliases discovery_data/work_author_aliases-v1.json
VALID: 96 alias row(s), content_hash=sha256:acce47f6…                          exit 0
```

An artifact that has not been validated is not pinned. Both were.

**The re-pin was produced by the script, never by hand.** The rulings live in
`OWNER_RULINGS` in `scripts/curate_work_domains.py` — a **tracked** input
`--emit-artifact` reads, in the same committed-decisions / gitignored-artifact shape as
`CURATION_RULES` and `MANUAL_ASSIGNMENTS` — so re-emitting reproduces the ruled rows
instead of discarding them and the artifact stays regenerable. The change is provably
confined to the 29 ruled rows: re-running the identical pass with the rulings table
suppressed reproduces the pre-ruling hash `sha256:4cc103ff…` byte-for-byte, and a
row-by-row diff of the two emissions returns exactly the 29 ruled ids and nothing else.

---

## 2. The assignment axis, stated explicitly

**Every row was assigned from the IDENTIFIED WORK's own neutral title and author. No row
was assigned from any manuscript's catalogue metadata.**

This is the whole point of the facet (findings-page reference, and CONTEXT A-6): Moss.
V,374 is catalogued *Court Records* while carrying a verifiably correct Rashi finding, and
338 tier-A findings sit on documentary-catalogued manuscripts. Filtering on the catalogue
axis would hide exactly the findings that disagree with the catalogue.

Mechanically, the curation pass reads only `works.neutral_title`, `works.author` and
`works.canonical_work_id` from the asset. It never opens `domains`, `catalog` or any
`AlmaId`-keyed table.

**Grain.** Assignment is at the **canonical** work level (`works.canonical_work_id`), so a
duplicate work reachable under two source ids is never assigned twice. 1,088 raw source
work ids carry a shipped claim; they collapse to **1,073 canonical works**. `--validate`
rejects a key that is not itself canonical.

---

## 3. Coverage

| | count | share |
|---|---:|---:|
| **Canonical works carrying a shipped claim** | **1,073** | 100% |
| `high` confidence | 1,012 | 94.3% |
| `medium` confidence | 32 | 3.0% |
| `needs-ruling`, **ruled** (§ Ruling P: 5, § Ruling Q: 24) | 29 | 2.7% |
| `needs-ruling`, still **held** | **0** | 0% |
| `Unassigned` | 0 | 0% |

`confidence` deliberately **stays `needs-ruling`** on a ruled row. The row's provenance is
genuinely different from a rule-derived `high`/`medium` one — it was held, put to the
owner, and settled by a citation — and it is the `owner_ruling` **citation**, not the
confidence token, that the release gate reads. Rewriting the confidence would erase the
only marker that these 29 rows were ever contested.

Every row carries a `confidence` and a `provenance`; a test asserts no row is missing
either, over the real artifact.

**Distinct FJMS `(parent, leaf)` pairs used: 61** of the 202-node tree (55 before the
rulings). Ruling Q's governing principle — *use the leaf the vocabulary carries for
exactly this work; a fallback to a broader leaf leaves the specific node empty and
destroys the information the facet exists to expose* — is what added six of them:
`Documentary / Documentary`, `Kalam / Jewish Kalam`, `Medicine / Medical Works`,
`Polemics / Polemics Jewish-Christian`, `Polemics / Polemics Rabbinical` and
`Secular Poetry / Other`, each of which would otherwise have had **zero** occupants.
The ten largest:

| rows | domain |
|---:|---|
| 227 | Responsa and Halakhic Decisions / Responsa- Gaonim |
| 120 | Biblical Exegesis / Biblical Exegesis- Rabbanite |
| 83 | Halakhic Literature and Talmudic Commentaries / Talmud Bavli Commentaries |
| 67 | Midrash / Aggadic Midrashim |
| 64 | Mishnah: Texts and Translations / Mishnah: Texts |
| 43 | Halakhic / Halakhic- Gaonim |
| 40 | Bible: Texts and Translations / Aramaic Targumim |
| 39 | Bible: Texts and Translations / Bible: Texts |
| 35 | Talmud Bavli: Texts and Anthologies / Talmud Bavli |
| 34 | Rabbinic Literature / Tosefta |

Full distribution: `python scripts/curate_work_domains.py --report`.

### How the rows were reached

`provenance` is `rule:<name>` or `manual:<reason>`, and the artifact's own `rules` array
resolves every rule name to a sentence.

- **1,012 rows by the ordered rule table** (57 named rules — "a title of this shape denotes
  a work of this kind"). The rule table is a set of **curation decisions, not a
  vocabulary**: every node it names is checked against the live FJMS tree before a single
  row is emitted, and a rule naming a node the tree does not carry is a build error.
- **32 rows individually curated** where the rules place a work badly or not at all.
- **29 rows held** for the owner (§ 4) and **since ruled** (§ 4.5). Their `provenance`
  now reads `owner-ruling:136-GATE1-DECISIONS.md § Ruling P|Q -- <class>`, so a ruled row
  is distinguishable from a rule-derived one at a glance.

### Independent check against the feasibility sample

The 93-work feasibility sample (`work-domains.sample.json`, produced independently during
the sketch pass) overlaps this worklist on 91 works. The rule table reproduces **88 of 91
(96.7%) exactly** — same parent, same leaf. **The three disagreements are precisely the
three the sample itself flagged as low-confidence**, and all three are in the
`needs-ruling` list below. The rule table was not fitted to the sample; the agreement is a
genuine cross-check.

### `Unassigned` is a real, visible value — and currently empty

`Unassigned` is a legitimate assignment with its own parent, never a null and never a
silent disappearance (the catalogue itself ships an "Unspecified Domain" bucket with
19,709 rows). It is the fallback for a work no rule places, and `--validate` accepts it as
data — a test asserts it validates while a null leaf on a non-held row does not.

The **count is 0** because the one work that reached the fallback (`w000846`, Eldad
ha-Dani's laws of ritual slaughter) is a work the vocabulary can in fact place; it was
curated to `Halakhic / Halakhic- Gaonim` at `medium` rather than left in the bucket. The
bucket remains a real, reachable value in the schema.

---

## 4. ✅ The `needs-ruling` work list — 23 decisions, 29 rows, all ruled

### 4.1 The posture applied

**One sentence, as the plan requires:** the `needs-ruling` rows were **HELD** — each
carrying `domain_leaf: null` plus its candidate leaves and a stated question, none
carrying a guessed leaf, with `--validate --release` failing closed while any remained
unruled — because `136-GATE1-DECISIONS.md` § D records that the owner will rule and that
the "ship as `Unassigned`" default is **explicitly DECLINED**; **29 rows (2.7% of 1,073)
were affected**, grouped into **23 decisions**, and **all 29 were ruled on 2026-08-03**
(§ Ruling P for 5, § Ruling Q for 24), each row now carrying its leaf plus an
`owner_ruling` citation, so the release gate passes.

§§ 4.2–4.4 below are left exactly as they were written **before** the rulings — they are
the record of the question that was actually put to the owner, and § 4.5's guard is that
every ruling picked one of the candidates those sections offered.

`--validate` checks structure only, so a `needs-ruling` row would otherwise ship into the
asset whether or not anyone looked at it (threat T-136-09-06). Two mechanisms close that:

1. a `needs-ruling` row carrying a concrete `domain_leaf` **without** an `owner_ruling`
   citation is a **validation error** — a guessed leaf cannot be smuggled in;
2. `--validate --release` fails while any held row is unruled — the artifact cannot reach
   the build.

A test asserts each, on synthetic rows and on the real artifact.

### 4.2 How to rule

For each decision below, name the chosen `domain_parent / domain_leaf`. Any candidate
listed is valid in the closed vocabulary; a leaf outside it is a build error, not a new
domain. The three classes are the ones the plan named: **(a)** literary genre with a
documentary surface form, **(b)** no plausible leaf in the closed vocabulary at all,
**(c)** between two adjacent leaves on a real scholarly judgement.

Once ruled, each row gains its `domain_leaf` plus an `owner_ruling` citation pointing at
the record that carries the ruling, and `--validate --release` passes.

### 4.3 Groups — one ruling covers several rows

| # | decision | rows | claims |
|---|---|---:|---:|
| G1 | **Yosippon** — `w001152`, `w000853`, `w000855` | 3 | 420 |
| G2 | **Kifayat al-Abidin** (המספיק לעובדי השם) — `w000007`, `w000036`, `w000038` | 3 | 248 |
| G3 | **Seder Olam** — `w000164`, `w001066` | 2 | 205 |
| G4 | **Sefer Yetzirah** — `w000522`, `w000021` | 2 | 181 |

The remaining **19 decisions are single rows**.

### 4.4 The list, heaviest first

#### 1. `w001152` — ספר יוסיפון (ערבי)  ·  **G1**

- **Author (as recorded):** — none — · **shipped claims:** 345 · **class:** (c)
- **The question:** Yosippon (the Judaeo-Arabic Yosippon) is a historical narrative written as a romance. The feasibility sample recorded it as unplaceable; note that the vocabulary DOES carry a historiography node, contrary to that note.
- **Candidate leaves:**
  - `Historiography and geographical descriptions / Historiography and geographical descriptions` — Yosippon as history
  - `Stories and Belles Lettres / Stories and Belles Lettres` — Yosippon as a historical romance

#### 2. `w000001` — העיונים והדיונים

- **Author (as recorded):** משה בן עזרא · **shipped claims:** 321 · **class:** (c)
- **The question:** Moses ibn Ezra's Kitab al-Muhadara is a treatise on Hebrew POETRY and rhetoric, not a philosophical work.
- **Candidate leaves:**
  - `Secular Poetry / Other` — a treatise on poetry
  - `Philology / Grammar` — a treatise on rhetoric and language

#### 3. `w000164` — סדר עולם רבה  ·  **G3**

- **Author (as recorded):** — none — · **shipped claims:** 204 · **class:** (c)
- **The question:** Seder Olam is a rabbinic chronography: a work of the rabbinic corpus, or a work of historiography?
- **Candidate leaves:**
  - `Rabbinic Literature / Other` — a tannaitic composition
  - `Historiography and geographical descriptions / Historiography and geographical descriptions` — a chronography
  - `Derashot and Later Midrashim / Later Midrashim` — transmitted midrashically

#### 4. `w000007` — המספיק לעובדי השם (כרך ב חלק ב)  ·  **G2**

- **Author (as recorded):** אברהם בן הרמב"ם · **shipped claims:** 132 · **class:** (c)
- **The question:** Abraham Maimonides' Kifayat al-Abidin is the classical Jewish-Sufi pietist work: ethical literature, or the Sufi Literature leaf?
- **Candidate leaves:**
  - `Philosophy, Theology, Ethical literature / Ethical Literature` — the work's own genre is pietist ethics
  - `Philosophy, Theology, Ethical literature / Sufi Literature` — the work is the central Jewish-Sufi text

#### 5. `w001149` — תעודות יהודי סיציליה (בן ששון)

- **Author (as recorded):** — none — · **shipped claims:** 128 · **class:** (b)
- **The question:** A scholarly edition of the documents of Sicilian Jewry mixes several documentary kinds; no single documentary leaf covers it.
- **Candidate leaves:**
  - `Documentary / Documentary` — the parent node itself, deliberately coarse, covering the mixture
  - `Documentary / Communal Documents` — if the edition is predominantly communal
  - `Documentary / Letters` — the documentary leaf the surface form suggests

#### 6. `w000021` — רס"ג, ספר יצירה פירוש  ·  **G4**

- **Author (as recorded):** סעדיה גאון (רס"ג) · **shipped claims:** 119 · **class:** (b)
- **The question:** Saadia's commentary on Sefer Yetzirah: the vocabulary has no Sefer Yetzirah leaf, and the work sits between cosmological speculation, philosophy and mysticism.
- **Candidate leaves:**
  - `Kabbalah / Other` — the mystical tradition
  - `Philosophy, Theology, Ethical literature / Philosophy` — the cosmological / philosophical reading
  - `Occult Sciences / Theoretical Works` — the speculative-science reading

#### 7. `w000058` — המעשה בפולמוס הכומר

- **Author (as recorded):** — none — · **shipped claims:** 115 · **class:** (c)
- **The question:** An account of a disputation with a priest: a polemic, or a narrative about one?
- **Candidate leaves:**
  - `Polemics / Polemics Jewish-Christian` — the subject is a Jewish-Christian dispute
  - `Stories and Belles Lettres / Stories and Belles Lettres` — the text is cast as a narrative of an event

#### 8. `w001132` — כתאב אלדרר

- **Author (as recorded):** יהודה אלחריזי · **shipped claims:** 115 · **class:** (c)
- **The question:** al-Harizi's Kitab al-Durar is a Judaeo-Arabic literary anthology.
- **Candidate leaves:**
  - `Stories and Belles Lettres / Stories and Belles Lettres` — literary prose
  - `Secular Poetry / Other` — the anthology carries poetry

#### 9. `w000036` — המספיק לעובדי השם (כרך ט חלק ב)  ·  **G2**

- **Author (as recorded):** אברהם בן הרמב"ם · **shipped claims:** 108 · **class:** (c)
- (same question and candidates as #4)

#### 10. `w001140` — אגרות הרמב״ם (שילת)

- **Author (as recorded):** משה בן מימון (רמב"ם) · **shipped claims:** 100 · **class:** (a)
- **The question:** Maimonides' epistles (Shailat's edition) are literary and halakhic treatises cast as letters. Do they file under the documentary Letters leaf, or with responsa / ethical literature?
- **Candidate leaves:**
  - `Documentary / Letters` — the documentary leaf the surface form suggests
  - `Responsa and Halakhic Decisions / Responsa- Rishonim and Aharonim` — several of the epistles answer halakhic questions
  - `Philosophy, Theology, Ethical literature / Ethical Literature` — the Epistle to Yemen and the Epistle on Martyrdom are read as ethical works

#### 11. `w000057` — עשרים מאמרים

- **Author (as recorded):** דאוד אלמקמץ · **shipped claims:** 76 · **class:** (c)
- **The question:** Dawud al-Muqammis' Twenty Chapters: kalam or theology? (The feasibility sample recorded this as one of its three low-confidence cases.)
- **Candidate leaves:**
  - `Kalam / Jewish Kalam` — the work is the founding Jewish kalam text
  - `Philosophy, Theology, Ethical literature / Theology` — the work is read as systematic theology

#### 12. `w000853` — יוסיפון  ·  **G1**

- **Author (as recorded):** — none — · **shipped claims:** 74 · **class:** (c)
- (same question and candidates as #1, for the Hebrew Yosippon)

#### 13. `w000522` — ספר יצירה  ·  **G4**

- **Author (as recorded):** — none — · **shipped claims:** 62 · **class:** (b)
- (same question and candidates as #6, for Sefer Yetzirah itself)

#### 14. `w000079` — אגרות שמואל בן עלי

- **Author (as recorded):** שמואל בן עלי · **shipped claims:** 61 · **class:** (a)
- **The question:** The letters of Samuel b. Ali are a literary letter collection. Documentary / Letters, or a literary parent? (The feasibility sample recorded this as one of its three low-confidence cases.)
- **Candidate leaves:**
  - `Documentary / Letters` — the documentary leaf the surface form suggests
  - `Responsa and Halakhic Decisions / Responsa- Rishonim and Aharonim` — the Gaon of Baghdad's correspondence is largely responsive
  - `Stories and Belles Lettres / Stories and Belles Lettres` — the collection is transmitted as literature

#### 15. `w000081` — איגרת ההשתקה

- **Author (as recorded):** יוסף בן שמעון · **shipped claims:** 55 · **class:** (a)
- **The question:** The Silencing Epistle is a philosophical treatise addressed as a letter.
- **Candidate leaves:**
  - `Philosophy, Theology, Ethical literature / Philosophy` — the content is a philosophical argument
  - `Documentary / Letters` — the documentary leaf the surface form suggests
  - `Polemics / Other` — the treatise is polemical in purpose

#### 16. `w000820` — משיבת נפש

- **Author (as recorded):** — none — · **shipped claims:** 22 · **class:** (c)
- **The question:** Meshivat Nefesh is the title of Yeshua b. Judah's Karaite Torah commentary and also reads as a title for a devotional or remedial text. Assigning this without a ruling would be a guess; the two readings put it in **different parents**.
- **Candidate leaves:**
  - `Biblical Exegesis / Biblical Exegesis- Karaite` — Yeshua b. Judah's commentary of this name
  - `Occult Sciences / Shimmush Tehillim` — reading the title devotionally
  - `Philosophy, Theology, Ethical literature / Ethical Literature` — reading the title as pietist

#### 17. `w000040` — חטר בן שלמה, שאלות

- **Author (as recorded):** חטר בן שלמה · **shipped claims:** 21 · **class:** (b)
- **The question:** Hoter b. Solomon is a fifteenth-century Yemenite philosopher; his "questions" are philosophical, though the surface form reads as responsa.
- **Candidate leaves:**
  - `Philosophy, Theology, Ethical literature / Philosophy` — the author's known philosophical work
  - `Responsa and Halakhic Decisions / Responsa- Rishonim and Aharonim` — the surface form

#### 18. `w000065` — יהודה ראש הסדר, ספר השנים

- **Author (as recorded):** Judah ha-Kohen · **shipped claims:** 12 · **class:** (b)
- **The question:** Judah Rosh ha-Seder's Sefer ha-Shanim: the title reads as a book of years (a calendar treatise), but the author is known as a Talmudic lexicographer.
- **Candidate leaves:**
  - `Astronomy / Calendar` — reading the title as a calendrical treatise
  - `Philology / Dictionaries` — the author's known lexicographic work
  - `Halakhic Literature and Talmudic Commentaries / Talmud Bavli Commentaries` — the author's known Talmudic work

#### 19. `w000444` — מגילת אביתר

- **Author (as recorded):** אביתר הכהן גאון · **shipped claims:** 11 · **class:** (c)
- **The question:** Megillat Evyatar is a partisan historical account of the Palestinian gaonate, preserved as a communal document.
- **Candidate leaves:**
  - `Historiography and geographical descriptions / Historiography and geographical descriptions` — a historical narrative
  - `Documentary / Communal Documents` — a communal record
  - `Polemics / Polemics Rabbinical` — a partisan attack

#### 20. `w000818` — מרפא לעצם

- **Author (as recorded):** — none — · **shipped claims:** 9 · **class:** (c)
- **The question:** Marpe la-Etzem reads as a medical title and is also transmitted with magical recipe material.
- **Candidate leaves:**
  - `Medicine / Medical Works` — reading the title medically
  - `Occult Sciences / Magic Recipes` — if it is a recipe / praxis text

#### 21. `w000038` — המספיק לעובדי השם (כרך ט חלק א)  ·  **G2**

- **Author (as recorded):** אברהם בן הרמב"ם · **shipped claims:** 8 · **class:** (c)
- (same question and candidates as #4)

#### 22. `w000154` — זיכרונות מימי נעוריי (חלק שני)

- **Author (as recorded):** אברהם דב בר גוטלובר · **shipped claims:** 4 · **class:** (b)
- **The question:** A nineteenth-century Hebrew memoir. The FJMS vocabulary, built for the Genizah corpus, has no leaf for modern autobiography. **⚠ DATA QUALITY:** a nineteenth-century maskilic memoir carrying shipped claims in a Genizah discovery corpus is itself worth checking.
- **Candidate leaves:**
  - `Historiography and geographical descriptions / Historiography and geographical descriptions` — memoir as historical writing
  - `Stories and Belles Lettres / Stories and Belles Lettres` — memoir as literature
  - `Unassigned / Unassigned` — the vocabulary genuinely cannot place it

#### 23. `w000160` — ערוגת הבושם

- **Author (as recorded):** שמואל ארקוולטי · **shipped claims:** 3 · **class:** (c)
- **The question:** **⚠ DATA QUALITY:** Arugat ha-Bosem names two different works — Archivolti's sixteenth-century Hebrew rhetoric (the author the asset records) and Abraham b. Azriel's thirteenth-century piyyut commentary (the better-known work of that name). The ruling settles which work the claims are actually about.
- **Candidate leaves:**
  - `Philology / Grammar` — Archivolti's rhetoric, per the recorded author
  - `Piyut and its Interpretation / Piyyut Commentaries` — Abraham b. Azriel's piyyut commentary

#### 24. `w001055` — ספר הזיכרון

- **Author (as recorded):** סעדיה גאון · **shipped claims:** 3 · **class:** (b)
- **The question:** Saadia's Sefer ha-Zikkaron: the subject of this title is not determinable from the title and author alone.
- **Candidate leaves:**
  - `Astronomy / Calendar` — if it belongs with his calendar works
  - `Halakhic / Halakhic- Gaonim` — if it is a halakhic monograph
  - `Polemics / Other` — if it belongs with his polemical works

#### 25. `w001004` — פרקי ט׳ באב

- **Author (as recorded):** — none — · **shipped claims:** 2 · **class:** (c)
- **The question:** Chapters for the Ninth of Av: a midrashic composition, or an occasional liturgy for the fast?
- **Candidate leaves:**
  - `Derashot and Later Midrashim / Later Midrashim` — a midrashic composition
  - `Liturgy and Brakhot / Occasional prayer` — a liturgy for the fast day
  - `Secular Poetry / Dirges` — if the chapters are dirges

#### 26. `w001058` — תולדות רבנו הקדוש

- **Author (as recorded):** סעדיה גאון · **shipped claims:** 2 · **class:** (c)
- **The question:** The Life of Rabbenu ha-Qadosh is a hagiographic narrative about R. Judah ha-Nasi.
- **Candidate leaves:**
  - `Stories and Belles Lettres / Stories and Belles Lettres` — hagiographic narrative
  - `Historiography and geographical descriptions / Historiography and geographical descriptions` — a biography

#### 27. `w001079` — תולדות בן סירא, נוסח א

- **Author (as recorded):** — none — · **shipped claims:** 2 · **class:** (c)
- **The question:** The Alphabet of Ben Sira is a satirical narrative transmitted in midrashic dress.
- **Candidate leaves:**
  - `Stories and Belles Lettres / Stories and Belles Lettres` — satirical narrative
  - `Derashot and Later Midrashim / Later Midrashim` — midrashic transmission

#### 28. `w000855` — יוסיפון, סיום, נוסח אחר  ·  **G1**

- **Author (as recorded):** — none — · **shipped claims:** 1 · **class:** (c)
- (same question and candidates as #1, for an alternative ending)

#### 29. `w001066` — סדר עולם קצר  ·  **G3**

- **Author (as recorded):** — none — · **shipped claims:** 1 · **class:** (c)
- (same question and candidates as #3)

### 4.5 ✅ The rulings, as applied — 2026-08-03

Recorded verbatim in `136-GATE1-DECISIONS.md` **§ Ruling P** and **§ Ruling Q**, and
applied here through `OWNER_RULINGS` in `scripts/curate_work_domains.py`. Every row below
carries `domain_parent`, `domain_leaf` and the `owner_ruling` citation naming the section
that settles it.

**§ Ruling P — 5 rows, settled from FJMS's OWN work-level domain.** The owner was right
that a work-level domain exists and this pass had missed it:
`fjms_enrichment.db::genizah_titles.DomainId` is populated for 718 of its 775 titles — a
domain attached to a **title**, not to an AlmaId, so it does not breach § 2's
assignment axis. It has no code→string table; the mapping was recovered empirically by
restricting to AlmaIds with exactly one `catalog.GenizahTitleId` **and** exactly one
`domains` row (62 DomainIds at 99.8% mean concentration). "Follow FJMS" is **scoped to
this evidence and is not a blanket override** — FJMS exact-covers only 55 of the 1,073
works, its taxonomy is often coarser than the rule table's, and in the largest
disagreement (הגדה של פסח, 2,180 claims) the rule table is the better answer. Where
comparable, agreement is **79.6%** (39/49) — this pass's first independent validation.
Two candidate rulings were **declined as too thin** and fell through to § Ruling Q:
מגילת אביתר (n=6) and ספר יצירה (n=1).

| rows | canonical ids | ruled `parent / leaf` | FJMS support |
|---|---|---|---|
| Yosippon (G1) | `w001152`, `w000853`, `w000855` | `Historiography and geographical descriptions / Historiography and geographical descriptions` | DomainId 180000, 100%, n=98, exact title match |
| Seder Olam (G3) | `w000164`, `w001066` | `Rabbinic Literature / Other` | DomainId 120000, 100%, n=87 |

**§ Ruling Q — the remaining 24, DELEGATED.** The owner's instruction verbatim: *"Go with
your judgements, I trust you."* These are **delegated judgements, not owner-authored
ones**; delegation does not make a weak call strong, and the ⚠ rows below are the first
things to revisit if the facet ever looks wrong. Governing principle: where the closed
vocabulary carries a leaf for *exactly this work*, use it.

| rows | canonical ids | ruled `parent / leaf` | ⚠ |
|---|---|---|---|
| המספיק לעובדי השם ×3 | `w000007`, `w000036`, `w000038` | `Philosophy, Theology, Ethical literature / Sufi Literature` | |
| העיונים והדיונים | `w000001` | `Secular Poetry / Other` | |
| תעודות יהודי סיציליה | `w001149` | `Documentary / Documentary` | catalogue override (see below) |
| אגרות הרמב״ם (שילת) | `w001140` | `Philosophy, Theology, Ethical literature / Ethical Literature` | |
| ספר יצירה + רס״ג's commentary | `w000522`, `w000021` | `Kabbalah / Other` | ⚠ Philosophy defensible for `w000021` alone |
| המעשה בפולמוס הכומר | `w000058` | `Polemics / Polemics Jewish-Christian` | |
| כתאב אלדרר | `w001132` | `Secular Poetry / Other` | ⚠ Belles Lettres arguable |
| עשרים מאמרים | `w000057` | `Kalam / Jewish Kalam` | |
| משיבת נפש | `w000820` | `Biblical Exegesis / Biblical Exegesis- Karaite` | |
| אגרות שמואל בן עלי | `w000079` | `Documentary / Letters` | |
| איגרת ההשתקה | `w000081` | `Philosophy, Theology, Ethical literature / Philosophy` | |
| חטר בן שלמה, שאלות | `w000040` | `Philosophy, Theology, Ethical literature / Philosophy` | catalogue override (see below) |
| מגילת אביתר | `w000444` | `Polemics / Polemics Rabbinical` | ⚠ n=6; Historiography arguable |
| ערוגת הבושם | `w000160` | `Philology / Grammar` | ⚠ n=12; unresolved title/author collision |
| יהודה ראש הסדר, ספר השנים | `w000065` | `Astronomy / Calendar` | ⚠ |
| מרפא לעצם | `w000818` | `Medicine / Medical Works` | |
| זיכרונות מימי נעוריי | `w000154` | `Historiography and geographical descriptions` | ⚠ `Unassigned` deliberately NOT used |
| תולדות בן סירא | `w001079` | `Stories and Belles Lettres` | |
| ספר הזיכרון | `w001055` | `Halakhic / Halakhic- Gaonim` | ⚠ **lowest-confidence call in the set** — a prior, not evidence |
| פרקי ט׳ באב | `w001004` | `Derashot and Later Midrashim / Later Midrashim` | ⚠ |
| תולדות רבנו הקדוש | `w001058` | `Stories and Belles Lettres` | |

**Two catalogue overrides are deliberate and must not be "corrected" later:** `w000040`
(a philosopher's questions, not responsa — the catalogue reads 51% Responsa, n=37) and
`w001149` (the edition is a mixture even though its individual fragments are mostly
letters — the catalogue's 55% `Letters` describes the fragments, not the edition).

**`w000154` was deliberately NOT sent to `Unassigned`,** though that was on its candidate
list: `Unassigned` would hide a row the artifact itself says may not belong in the corpus
at all. Assigning it keeps it visible. The real question is corpus membership — a
data-quality item (§ 6.5), not a domain question.

**How a ruling is prevented from becoming a new guess.** Rulings live in `OWNER_RULINGS`
in the committed script, and `assert_rulings_are_answerable()` refuses to emit if any of
three things is true: the ruling names a work that was never held; the ruled
`(parent, leaf)` is absent from the **live** FJMS tree; or the ruled leaf was **not one of
the candidates that row itself offered** — a ruling answers the question that was put to
the owner, it does not introduce a fourth option after the fact. All 29 pass all three;
five tests pin the behaviour.

---

## 5. The author alias map (Task 3)

`discovery_data/work_author_aliases-v1.json` ·
`sha256:acce47f67dcde456eb477fc092294ee42546963f5d977549f53e635da65f8a64`

| | count | share |
|---|---:|---:|
| Canonical works | 1,073 | — |
| Works carrying an author | 506 | 47.2% |
| **Distinct author strings** | **96** | — |
| — `exact` match to the catalogue person list | 37 | 38.5% |
| — `containment` match (recorded as containment, never as exact) | 39 | 40.6% |
| — `unmatched` (retained, never forced) | 20 | 20.8% |
| Matched overall | 76 | **79.2%** |
| Author gaps left unfilled | 567 | 52.8% of works |

The findings-page reference quotes **520 of 1,088 works (48%), 96 distinct strings, 81%
matching (38 exact + 40 containment)**. This pass measures **506 of 1,073 (47.2%), 96
distinct, 79.2% (37 + 39)**. The **96 distinct strings match exactly**; the small deltas
are (a) the canonical grain (1,073, not 1,088 raw ids) and (b) this pass's own explicit
normalization (NFC, quote/geresh/gershayim stripped, whitespace collapsed — the same
normalization `shared/discovery_grouping.py` applies to titles). The reference figures are
not restated as this pass's own; these are.

**The target vocabulary** is `fist_data/fjms_enrichment.db :: genizah_persons` — the list
`FjmsService.get_browse_authors()` reads back, which is the accessor the facet cascade
queries.

**Resolution is deterministic and order-independent**, in the same shape as
`shared/discovery_novelty.py::novelty_work_key`'s alias groups: an exact match beats every
containment match; among containment matches the **longest (most specific)** catalogue
name wins; every remaining tie goes to the smallest `person_id`. Four tests pin this
(containment is not recorded as exact; an unmatched author is retained rather than
dropped; resolution is order-independent; the map is keyed deterministically), plus a
fifth added after the defect in § 6.1.

**No author was inferred from a title pattern**, per the plan's explicit rule (that is how
the wrong Bahya gets attributed, which this corpus already demonstrates — see § 6.2).
**Six rows carry an author gap that a title-pattern inference would have filled, and were
deliberately left unfilled**:

| work | title (the title names a person) | person the title names | claims |
|---|---|---|---:|
| `w000323` | שערי שחיטה ובדיקה לרב שמואל גאון בן חפני(؟), תרגום | שמואל גאון | 8 |
| `w000322` | שערי ברכות לרב שמואל גאון בן חפני, תרגום | שמואל גאון | 3 |
| `w000504` | שאלה אל יוסף בן אביתור בעניין סכסוך קרקעות | יוסף בן אביתור | 2 |
| `w001049` | הלכות תפילין לרב האיי גאון, תרגום | האיי גאון | 2 |
| `w000256` | שאלה אל רב שרירא גאון בעניין צוואה | שרירא גאון | 1 |
| `w001076` | רשימת החילופים שבין רב סעדיה גאון ובן מאיר | סעדיה גאון | 1 |

Two of those six (`w000504`, `w000256`) show why the rule matters: the title names the
**addressee** of a question, not its author. A title-pattern fill would have attributed the
work to the person it was sent to.

The other 561 gaps are works that are anonymous or collective by nature — 63 Mishnah
tractates, 47 anonymous Geonic responsa collections, 39 books of the Bible, 35 Bavli
tractates, 34 Tosefta tractates, 32 Yerushalmi tractates, 20 anonymous Geonic Talmud
commentaries, and the rest.

---

## 6. Data-quality findings — recorded, not silently fixed

### 6.1 A containment-resolution defect, found and fixed in this pass

The first alias run resolved `שלמה בן יצחק (רש״י)` — **Rashi, the corpus's second most
frequent author at 39 works** — to FJMS person **147 `שלמה`**, the bare given name, while
person **152 `שלמה בן יצחק`** sat in the same candidate list. Cause: the containment
tie-break was "smallest `person_id`", which is order-independent but blind to specificity.
Fixed to "longest catalogue name first, then smallest id" (still fully deterministic and
order-independent); Rashi now resolves to 152. `tests/test_work_domains.py::
test_alias_containment_prefers_the_most_specific_catalogue_name` pins it. This was a real
bug in code written in this plan, fixed in this plan.

### 6.2 The wrong Bahya — confirmed, and left in place

`w000022` (תורת חובות הלבבות, **981 shipped claims** — the fourth heaviest non-Bible work in
the corpus) records its author as **בחיי בן אשר**. *Duties of the Hearts* is by **Bahya ibn
Paquda**, not Bahya ben Asher. The feasibility sample already flagged this; this pass
confirms it on the live asset. The domain assignment is unaffected (Ethical Literature
either way), so the row is assigned `high` and carries the finding as a `note`. **Not
corrected in this artifact** — an author correction belongs in the asset, not in a domain
map.

### 6.3 `האיי גאון` — 59 works, unmatched on an orthographic variant alone

`האיי גאון` is the single most frequent author string in the corpus (59 of 1,073 works) and
is recorded **`unmatched`**. The catalogue does carry the person: FJMS person **683
`האי גאון`** — the same name with a single yod. Neither the exact nor the containment test
fires across that one-letter difference, and the alias map's three labels
(exact/containment/unmatched) have no room for an orthographic-variant tier. **Recorded,
not applied** — forcing this match would be exactly the "forced rather than unmatched"
outcome the plan forbids, and adding a fourth match label is a vocabulary change, not a
curation decision. Same shape, smaller: `סלמון בן ירוחים` vs FJMS `סלמון בן ירוחם`,
`שמעון קיארא` vs FJMS `שמעון קיירא`.

**This is the largest single lever available on the alias map's coverage**: matching
`האיי גאון` would move 59 works, and the yod-doubling family plausibly covers several more
of the 20 unmatched strings.

### 6.4 The feasibility sample's "no history leaf" note is wrong

The sample recorded `ספר יוסיפון (ערבי)` as unplaceable on the grounds that *"the FJMS
vocabulary has no clear history leaf"* and parked it in `Unspecified Domain`. The
vocabulary **does** carry `Historiography and geographical descriptions` — a top-level node
with no children, and therefore itself a usable leaf. The sample's two-level model
(parent → child) could not see it. This pass's validator explicitly allows a childless
top-level node to be its own parent, and a test pins that. Yosippon is still held for a
ruling — but as a genuine two-way judgement (history vs romance), not as an
unplaceable work.

### 6.5 One work that may not belong in the corpus at all

`w000154` — Gottlober's *Zikhronot mi-Ymei Ne'urai*, a **nineteenth-century maskilic
memoir**, carries 4 shipped claims. `w000158` (שלשלת הקבלה, a sixteenth-century printed
chronicle, 17 claims) and `w000160` (ערוגת הבושם, sixteenth century, 3 claims) are the same
shape. These are recorded here as **provenance questions about the reference corpus**, not
as domain problems; the Genizah corpus is overwhelmingly earlier than any of them.

---

## 7. Threat-model coverage

| Threat | Disposition |
|---|---|
| **T-136-09-01** a domain invented outside the closed vocabulary | The vocabulary is read from `shared/fjms_service.py` at runtime with no snapshot and no fallback; `assert_rules_within_vocabulary()` rejects a rule naming an unknown node before any row is emitted; `--validate` rejects an out-of-tree leaf; three tests assert it, including a behavioural one that swaps the tree and watches validation follow. |
| **T-136-09-02** a restricted-corpus name reaching a committed artifact | `check_atlas_masking.py --scan-asset` run on both artifacts, on this report, and on the script and tests — all exit 0. A **positive control** was run in the same session (a real pattern seeded into a scratch file) and correctly tripped, so the clean results are true negatives, not a misconfigured scanner. Restricted corpora are named only as "M-source"/"R-source" anywhere in this plan's output. |
| **T-136-09-03** a guessed assignment presented with the same weight as a confident one | Every row carries `confidence` and `provenance`; `needs-ruling` rows are listed for the owner rather than resolved. |
| **T-136-09-06** a `needs-ruling` row shipping unreviewed | The posture is read from `136-GATE1-DECISIONS.md` § D and stated in § 4.1; a held row with a concrete leaf and no `owner_ruling` is a validation error; `--validate --release` fails while any held row is unruled; tests assert both on synthetic and real rows. **Since 2026-08-03** all 29 rows carry an `owner_ruling` citation naming § Ruling P or § Ruling Q, and `assert_rulings_are_answerable()` additionally refuses a ruling on a work that was never held, a ruled leaf outside the live tree, or a ruled leaf that was never among that row's own candidates. |
| **T-136-09-04** a work assigned twice through a duplicate id | Keys are canonical work ids; `--validate` rejects duplicates and rejects a non-canonical key. |
| **T-136-09-05** an artifact edited after pinning | Content hash over the payload array, recorded in § 1, re-checked by `--validate` and to be re-checked at ingest in 136-12. |
| **T-136-09-SC** package installs | None in this plan. |

---

## 8. What plan 136-12 must do with this

1. Re-verify **both** content hashes before ingest — they are the pin.
   **`work_domains-v1.json` is now `sha256:57393773…`, not the `sha256:4cc103ff…` this
   report first recorded**; the earlier hash is the pre-ruling artifact and must not be
   accepted.
2. Load `work_domains-v1.json` into `works.genre` **at the canonical grain**, per
   `discovery-sidecar-schema-v1.md` § Amendment 2026-08-02 (C): the column already exists
   and must not be re-added by DDL; a value outside the closed vocabulary is a build error.
3. **Refuse to build while `--validate --release` fails.** As of 2026-08-03 it **passes**
   (exit 0, 0 held rows) — but the gate stays wired: it is what stops a future re-emission
   that adds a held row from shipping silently.
4. Do **not** key on `confidence` to decide whether a row is trustworthy. The 29 ruled
   rows keep `confidence: needs-ruling` deliberately (§ 3); `owner_ruling` is the field
   that says "settled".
