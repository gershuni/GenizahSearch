# MAPV2-A — Discovery-deck annotation brief (per-card)

## Why you exist

The v10 "discovery deck" (88 cards) claims each Genizah page is a NEW witness
of a specific edited work. Hillel (senior Genizah scholar, product owner)
reviewed it and said: many cards are **not really discoveries** — either the
catalogs (NLI / FJMS) already identify the fragment as that work (sometimes
under a DIFFERENT name for the same work), or the overlap is merely a **shared
source** both texts quote. Your job: annotate every card in your chunk with an
honest verdict of what it actually is, using the texts AND the catalog titles,
**controlling for name variants of the same work**.

The single governing question per card:
**"Would a Genizah scholar learn anything from this card that is not already
in the catalogs?"**

## Input (your chunk JSON)

Each card object:
- `card_no`, `section` (P stratum), `p`, `band`, `margin`, `alen` (match
  length in letters), `dens` (edit distance/alen — lower = closer), `n_work_witnesses_tierA`
  (how many strict census witnesses the claimed work already has),
  `n_pages_this_ms` (more matched pages in the same manuscript = stronger).
- `shelfmark`, `library`, `sys_id`, `url`.
- `nli_title` — the NLI catalog title line for the manuscript (may be
  generic, specific, or absent).
- `fjms_catalog_identifications` — list of FJMS catalog units for this
  manuscript: `work` (= GenizahTitleOrgTitle, THE work identification),
  `work_en`, `author`, `genre_frame`, `unit_title`, `identified_by` (which
  catalog/team). Empty list = FJMS has no identification. NOTE: a manuscript
  may hold several works (miscellany) — an identification of a DIFFERENT part
  of the codex does not settle this page.
- `work_id`, `work_name`, `cat` — the claimed work (edition side).
- `page_snippet` — the Genizah page text (HTR or human transcription, see
  `text_provenance`; expect HTR noise). The matched span is marked 【…】.
- `ref_snippet` — the corresponding passage in the claimed work's edition.
- `flank_class` — does the page text CONTINUE the edition around the match
  (`continues` = strong same-work signal) or is the match an island
  (`island` = quotation-like) or at a text edge (`edge` = undecidable).

## Verdict taxonomy (pick exactly one)

1. **KNOWN-SAME** — a catalog (NLI title or an FJMS identification) already
   names the claimed work for this manuscript, INCLUDING name variants:
   Hebrew↔Arabic↔Judeo-Arabic titles (הדאיה אלקארי = הוראת הקורא; תפסיר =
   Saadia's Arabic translation), abbreviations (ר"ח = רבנו חננאל), author
   named instead of work, series names (משנה תורה ספר אהבה vs רמב"ם),
   spelling variants. This is a catalog CONFIRMATION — correct but not new.
2. **KNOWN-DEPENDENCE** — the titles alone reveal a known literary dependence
   that fully explains the overlap: e.g. the manuscript is the ערוך (which
   quotes רבנו חננאל verbatim), a halakhic digest quoting Talmud/geonim,
   הלכות גדולות quoting Bavli, a commentary quoting its base text. The match
   is real but EXPECTED from the identifications — not a discovery.
3. **SHARED-SOURCE** — the highlighted overlap is a quotation both texts take
   from a THIRD common source: a Bible verse, Mishnah, Talmud passage,
   midrash, or a fixed liturgical/legal formula. Leak, teaches nothing.
4. **CITATION** — one side quotes the other work, typically with a citation
   formula near the span head (כדתניא, כדאיתא, ואמרו במדרש, וז"ל, JA: לקו׳,
   קאל). State the DIRECTION: `page_quotes_work` (page merely cites the
   claimed work → leak) or `work_quotes_pages_source` (the edition quotes an
   older source that the page carries directly → REVERSED, this is a
   potential FIND — flag it clearly).
5. **DISCOVERY** — the catalogs do NOT identify this page as the claimed work
   (title generic — "קטע", "מצאי", genre-only — or absent, or a different
   identification that this match corrects/complements), AND the texts read
   as genuinely the same work (long close match, page continues the edition,
   not quotation-framed). NEW knowledge.
6. **PARALLEL** — genuine literary relationship worth scholarly study but not
   same-work witness: parallel midrash traditions, Karaite↔Rabbanite shared
   verse-chains, shared formulary between distinct compositions, two
   translations of the same base text.
7. **NO-RELATION** — noise; the overlap is coincidental (common names,
   generic phrases).

## Known failure families (from Hillel's blind grading — watch for them)

- Shared Bible-verse quotation = the #1 leak (both quote the same פסוק).
- Both quote the same Talmud/Mishnah passage (geonic/halakhic digests).
- Citation formula right before the span start (incl. Judeo-Arabic: לקו׳ =
  לקולה; קאל; and HTR-garbled forms like וגדסי׳ = וגרסינן).
- NLI-title mismatch was a tell in ~16/18 graded leaks — if the NLI title
  names a specific DIFFERENT work, be suspicious of the claim (but check
  KNOWN-DEPENDENCE first: the "different" work may be a known quoter).
- Reversed direction (edition quotes the page's source) looks like a leak
  but is actually interesting — don't bury it, use CITATION + direction.

## Title-relation field (independent of verdict)

`title_relation`: one of
`same_work` (title names the claimed work), `name_variant` (same work,
different name/language — SAY the equation you used), `known_quoter`
(title names a work known to quote / be quoted by the claimed work),
`different_specific` (title names an unrelated specific work),
`generic_or_absent` (no usable identification).

## Output — STRICT

Write a JSON array to the output path given in your task prompt, one object
per card, ALL cards of your chunk, in order:

```json
{
 "card_no": 1,
 "verdict": "KNOWN-DEPENDENCE",
 "confidence": "high",            // high | medium | low
 "title_relation": "known_quoter",
 "direction": null,                // only for CITATION: "page_quotes_work" | "work_quotes_pages_source"
 "novelty": false,                 // would a scholar learn something new?
 "name_equation": "ערוך ↔ מצטט את פירוש ר\"ח",   // if a variant/quoter equation was used, else null
 "reasoning_he": "1-3 משפטים בעברית: מה ראית בכותרות ובטקסטים ולמה פסקת כך.",
 "key_evidence": "short English: the decisive observation"
}
```

Rules:
- Judge from the JSON payload first. If genuinely stuck on a title equation,
  you MAY read the DBs read-only (`C:\Genizahsearch\fist_data\fjms_enrichment.db`,
  `C:\Genizahsearch\libraries.csv`) or the deck HTML — but do NOT modify
  anything, and do NOT open `data\fullcorpus_v2.db`.
- `novelty=true` only when the card adds identification knowledge beyond the
  catalogs (DISCOVERY always; CITATION-reversed usually; a KNOWN-SAME never;
  PARALLEL true only if the parallel itself is non-obvious).
- Be decisive; use `confidence` to express doubt, not fence-sitting verdicts.
- Hebrew reasoning is for Hillel — write it naturally, no jargon.
