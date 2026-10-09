> **DRAFT — not published; owner to review.** This is a starting point for the owner to
> edit, not live text. It has not been linked from the site, committed, or sent anywhere.
>
> **Placeholders left for the owner (2):**
> - `[OWNER: governing law — Israel, or omit?]` — in "Governing law" below.
> - `[OWNER: effective date]` — the date to put at the top once this is actually posted.
>
> **Also check:** §3 invites researchers who need full-corpus access to get in touch. Keep it
> only if you are willing to field those requests.

# Terms of Use — Dicta Genizah Search

*Effective date: [OWNER: effective date]*

## Who we are

Dicta Genizah Search ("GenizahSearch," "the site," "we") is a free research platform for
Cairo Genizah manuscripts at [genizahsearch.com](https://genizahsearch.com). It includes a
web app, a Windows desktop app, a public Search API, and AI integrations — an MCP server at
`https://genizahsearch.com/mcp` that ChatGPT, Claude, and Gemini users can connect to, and
(for as long as it lasts) the GenizahSearch custom GPT in ChatGPT.

GenizahSearch is published by Hillel Gershuni personally, with the support of Dicta — The
Israel Center for Text Analysis.

## Acceptance

By using the website, the desktop app, the Search API, or any AI tool connected to
GenizahSearch (the MCP server or the GPT), you agree to these terms. If you don't agree,
please don't use the service.

## 1. AI answers can be wrong

ChatGPT, Claude, and Gemini (through the MCP server) and the GPT can search the corpus and
describe what they find, but they can also misread, mistranslate, or misattribute a
manuscript, or simply get something wrong. These tools are a research aid, not a source of
truth. Before relying on or publishing a claim an AI tool gave you, check it against the
actual manuscript images, the transcriptions, and the catalogue records on the site.

## 2. Sources keep their rights

GenizahSearch brings together work from many sources, and each one keeps its own rights:

- **MiDRASH** — credited for the automatic transcriptions only, not for any edition,
  translation, or scholarly reading drawn from another source.
- **Princeton Geniza Project (PGP)** — transcriptions, translations, and metadata.
- **Friedberg Genizah Project (FGP)** — transcriptions, editions, and translations.
- **FJMS** and the **National Library of Israel** — catalogue descriptions and bibliography.
- **Holding libraries** (Cambridge, JTS, Oxford, the National Library of Israel, and others)
  — manuscript images and library metadata.

GenizahSearch's own software is licensed under
[CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/) (non-commercial use,
with attribution, shared alike), © Hillel Gershuni / Dicta. That licence covers the
GenizahSearch code only, not the sources above.

When you use material from GenizahSearch, you're bound by that source's own terms and
licence, and you should cite it accordingly — the site tells you, for each page, who made
what you're looking at. GenizahSearch does not grant any right it does not itself hold.

## 3. Fair use — no bulk scraping

GenizahSearch is free to use, and we rate-limit and cap jobs so it stays usable for everyone.
Don't try to download the corpus or its images in bulk, and don't try to work around the
rate limits or job limits (for example, by spreading requests across many addresses or
ignoring a "try again" response). If you need full-corpus access for research, contact us
instead — see below.

## 4. Provided free, as is

GenizahSearch is provided free of charge, as is, with no warranty of any kind. We don't
guarantee the site, the desktop app, the Search API, or the AI integrations (the MCP server
or the GPT) will always be available, accurate, or unchanged — any of them may change or stop
without notice.

## Governing law

[OWNER: governing law — Israel, or omit?]

## Changes to these terms

We may update these terms as the service changes. The current version is always the one
posted at this page, with a revised date at the top.

## Contact

Questions about these terms: email [gershuni@gmail.com](mailto:gershuni@gmail.com), or use
the support page at [genizahsearch.com/support](https://genizahsearch.com/support).

---

### Sources checked

- `integrations/chatgpt/privacy.html` — tone (plain, short sentences), the operator framing
  ("maintained by Hillel Gershuni, creator of Dicta Genizah Search"), and the contact address
  `gershuni@gmail.com`. Used to keep this draft from contradicting what the privacy policy
  already says about the service and its operator; this draft makes no new claims about
  logging or data retention.
- `shared/transcription_credits.py` — confirms MiDRASH is credited only for the automatic
  transcription (never for an FGP/PGP edition, a translation, or a correction), and that the
  MiDRASH dataset is CC-BY-4.0 (the module's own licence reasoning for crediting it even on a
  derivative correction).
- `integrations/chatgpt/instructions.md` — the citation/credit rule for AI output ("For
  MiDRASH automatic text used in quotations, search snippets or analysis, include this full
  citation unchanged") that backs the "cite your sources" point.
- `docs/SEARCH_API.md` — per-IP rate limiting (independent token-bucket per endpoint, `429
  rate_limited` + `Retry-After`), concurrency "busy" errors on heavy/passage search, the
  explicit "Not a bulk-export interface" statement, "there are no API keys today; access is
  anonymous and rate-limited per-IP," and the Attribution & Citation section naming MiDRASH,
  PGP, FJMS/FGP, and NLI.
- `web/api_hardening.py` — confirms the rate limiter and mode gate described in
  `docs/SEARCH_API.md` are real (per-IP sliding window, `Retry-After` header, `SEARCH_API_MODE`
  disable switch), so the fair-use section states only behavior that exists in code.
- `LICENSE` and `CompileScriptGenizah.iss` — the software's own licence, CC BY-NC-SA 4.0,
  © Hillel Gershuni / Dicta (added by Claude in review).
- `web/main.py` — the footer credit ("Created by Hillel Gershuni") and the FAQ answer ("The
  website was created by Hillel Gershuni, with the support of Dicta. Feel free to contact him
  at gershuni@gmail.com"), which the "Who we are" and "Contact" sections match verbatim in
  substance.
