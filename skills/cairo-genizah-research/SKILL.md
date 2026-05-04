---
name: cairo-genizah-research
description: |
  Research Cairo Genizah manuscripts. Drives genizahsearch.com APIs to find candidate
  witnesses for a phrase, piyyut, responsum, letter, or composition with browse-grounded
  justifications and honest reporting of partial evidence. Use when the user asks about
  Cairo Genizah manuscripts, medieval Hebrew or Judeo-Arabic texts, shelfmarks (T-S, ENA,
  JTS, Halper, Mosseri), or wants to find parallels to a piyyut/responsum/document
  fragment. Produces a tiered ranked list (Tier A = matches multiple distinctive phrases,
  B = two phrases, C = one) with browse text, library attribution, and image URL where
  available.
---

# Cairo Genizah Research

You drive a research workflow against the GenizahSearch APIs to find candidate
witnesses for Hebrew / Judeo-Arabic medieval manuscripts. The skill targets Claude
Code (or Claude Desktop with code execution + network) — see "Surface compatibility"
below.

## When to use

Trigger this skill when the user:
- Asks "find Genizah manuscripts containing X" or "where is this piyyut attested"
- Pastes a piyyut, responsum, or letter and wants parallels
- Asks about a specific shelfmark (T-S, ENA, JTS, Halper, Mosseri, etc.) and wants context
- Wants to enumerate witnesses to a known composition

Do NOT use this skill for general Hebrew translation, modern Hebrew text, or
non-Genizah biblical/talmudic queries.

## Surface compatibility (R1 — IMPORTANT)

| Surface | Network | Works? |
|---------|---------|--------|
| Claude Code | full (user's machine) | YES — primary target |
| Claude Desktop (code execution + network enabled) | per user/admin settings | YES |
| claude.ai (web, code execution) | per user/admin settings | YES if network enabled |
| Claude API code-execution containers | NONE | NO — skill cannot reach genizahsearch.com |

The skill REQUIRES outbound HTTPS to `genizahsearch.com`. On the Claude API
surface, code-execution containers have no network access; the skill will fail
on every script invocation. v7.10 acceptance run targets Claude Code.

## Configuration (D-09 — env wins over CLI)

| Variable | Default | Purpose |
|----------|---------|---------|
| `GENIZAH_API_BASE` | `https://genizahsearch.com` | Base URL for all API calls. |
| `GENIZAH_TOP_N` | `10` | Top-N candidates to drill via /api/browse (bounded [1, 25]). |
| `GENIZAH_SKILL_REQ_PER_MIN` | `24` | Throttle ceiling per endpoint bucket. |
| `GENIZAH_SKILL_BURST` | `5` | Token-bucket burst capacity. |

**Precedence (D-09 — INVERSION of typical CLI convention):** if `GENIZAH_API_BASE`
is set, it wins over any `--base-url` CLI flag. Rationale: a developer who set the
env var once for local testing and forgot would otherwise be surprised when CLI
overrides quietly redirect to production. Document loudly when both are set.

## Workflow

1. **Decide entry point based on input shape:**
   - Multi-line text > 200 chars (a piyyut stanza, document body) → composition
     search → use `scripts/parallels.py`.
   - Otherwise → text query → use `scripts/stage.py` (staged phrase discovery).
   - User explicitly asks for a shelfmark resolution → use `scripts/search.py`
     with `--search-mode shelfmark`.

2. **Staged phrase discovery (text queries):**
   - Extract 2–4 distinctive phrases from the user's query. Choose phrases that
     are unusual enough to discriminate (avoid stopwords, very common
     expressions). For Hebrew/Judeo-Arabic, include rare orthography variants
     where applicable.
   - Run `python ${CLAUDE_SKILL_DIR}/scripts/stage.py --phrase "P1" --phrase "P2"
     --phrase "P3" --search-mode exact --limit 50`. The script fans out one
     /api/search call per phrase, merges by uid, assigns Tier A/B/C, and emits
     JSON to stdout.
   - If the user has a Responsa-corpus query, use `--search-mode responsa` and
     optionally pass `--responsa-options-json '{"variants":true,"ja":true}'`.

3. **Top-N drill-down (default top 10):**
   - Take the first `GENIZAH_TOP_N` (default 10) merged candidates.
   - For each, call `python ${CLAUDE_SKILL_DIR}/scripts/browse.py --uid <UID>`.
     The `uid` field on each candidate is preferred per Phase 77 D-13. If the
     candidate lacks `uid`, fall back to `--sys-id <SID> --p-num <N> --volume-ie <IE>`.
   - The browse response carries `text`, `text_source`, `metadata` (PGP/FJMS/NLI),
     and `image` (url, sources). The skill MUST handle errors per "Error
     handling" below — do not crash on a single failed candidate.

4. **Compose justifications GROUNDED IN BROWSE TEXT:**
   - For each successful candidate, write a 1–2 sentence justification that
     cites specific words/phrases from the browse `text` field.
   - **CRITICAL (R9 mitigation):** if `text_source != "pgp_transcription"`, the
     justification MUST be solely about how the matched phrase appears in the
     snippet — never invent surrounding context. The honesty annotation is your
     safety net but is NOT a license to extrapolate.
   - The skill's `format_output.honesty_annotation(browse_response)` returns the
     exact text to append; ALWAYS append its return value.

5. **R2 mapping — text_source values (locked):**
   - The Phase 79 API enum is `pgp_transcription | snippet | none`. There is NO
     `'full'` value, despite REQUIREMENTS.md SKILL-04's prose mentioning `'full'`.
   - Treat `text_source == "pgp_transcription"` as the equivalent-of-full case
     (no annotation).
   - Treat `snippet` and `none` as triggering the annotation `"(full text
     unavailable; based on snippet of N chars)"`.
   - The `_FULL_TEXT_SOURCE` constant in `scripts/format_output.py` enforces this
     mapping; do not duplicate the check in your prose.

6. **Apply known-witness policy if user provided `known_witnesses[]`:**
   - Default policy: `flag` (mark with `known_witness: true`, keep in list).
   - `exclude` policy drops them from output.
   - `scripts/format_output.apply_known_witness_policy(candidates, known_uids,
     policy)` does this. Use Tier-1 `normalize_shelfmark.normalize` to canonicalize
     user-supplied shelfmarks; for shelfmarks that don't match any candidate's
     normalized form, optionally call `/api/search?search_mode=shelfmark&query=<S>`
     (Tier 2 — costs one extra search-bucket token per unresolved witness).

7. **Render output (Markdown by default):**
   - Pass the enriched candidate list to `scripts/format_output.render_markdown`.
   - End with a summary line: `"Processed N candidates: X succeeded, Y rate-limited,
     Z NLI image unavailable."`

## Error handling (D-07 + SKILL-03)

When ANY script invocation returns `{"error": {"code": ..., "message": ...}}`:
- Add a one-line plain-text inline note for that candidate: e.g.
  `"browse failed: rate-limited, retry-after 12s"` or
  `"browse failed: manuscript page not found"`.
- **Do NOT retry** (D-08 — no retry logic in v7.10).
- **Do NOT crash** the conversation. Continue processing remaining candidates.
- Surface 429 `Retry-After` value in the inline note when present.
- Tally success/failure counts for the summary line.

Error code → plain-text mapping:

| Server error code | Inline note |
|-------------------|-------------|
| `rate_limited` | `rate-limited (retry-after Ns)` — N from Retry-After |
| `core_timeout` | `core timeout — search backend hung` |
| `manuscript_page_not_found` | `manuscript page not found for this locator` |
| `locator_conflict` | `locator validation failed (uid + sys_id mismatch)` |
| `invalid_request` | `invalid request body — server rejected` |
| `invalid_combination` | `invalid field combination` (e.g. responsa_options with non-responsa mode) |
| `invalid_filter_value` | `unknown filter value` |
| `filter_vocabulary_unavailable` | `filter vocabulary not loaded — try without filters` |
| `query_required` | `query field empty` |
| `query_too_long` | `query exceeds 1000 chars` |
| `regex_pattern_too_long` | `regex pattern exceeds 256 chars` |
| `composition_required` | `composition text empty` |
| `composition_too_long` | `composition exceeds 20000 chars` |
| other / unknown | `request failed: <code> — <message>` |

## Throttle (SKILL-06)

All scripts share `scripts/throttle.py` with three independent token buckets:
`search`, `browse`, `parallels`. Default 24 rpm per bucket; burst 5. State
persists in `state/throttle.json` under a file lock. You do not call the
throttle directly — every endpoint script acquires its bucket internally.

Workload sizing: a typical scholarly query with 3 phrases + top-10 drill-down
is ~3 search calls + ~10 browse calls = ~13 requests. Comfortably under the
server's 30 rpm per bucket. A heavier query (5 phrases + top-25 + tier-2
shelfmark resolution for 3 known witnesses) is ~5 + 25 + 3 = ~33 requests
spread across two buckets — still safe with throttle pacing.

## Sample invocations

```bash
# Text query
python ${CLAUDE_SKILL_DIR}/scripts/stage.py \
  --phrase "ויאמר משה אל בני ישראל" \
  --phrase "קרא ה' בשם בצלאל" \
  --search-mode exact --limit 50

# Drill-down on a result
python ${CLAUDE_SKILL_DIR}/scripts/browse.py \
  --uid 990001234560205171_001r

# Composition search
cat composition.txt | python ${CLAUDE_SKILL_DIR}/scripts/parallels.py \
  --text-file - --chunk-size 5 --mode exact

# Shelfmark resolution (Tier 2)
python ${CLAUDE_SKILL_DIR}/scripts/search.py \
  --query "T-S 12.123" --search-mode shelfmark --limit 5
```

Note: `${CLAUDE_SKILL_DIR}` is a Claude-Code-only string substitution. On other
surfaces, scripts resolve their own directory via `Path(__file__).parent` so
invocations work without the variable.

## Future extension point — local-data shortcut (D-03, NOT implemented in v7.10)

When the user has the GenizahSearch desktop app installed, the skill could
optionally read `Genizah_Index/` (Tantivy) and/or `transcriptions.txt` directly
to skip /api/search calls. v7.10 ships API-only; v7.11 candidate. Hook lives
here for a future contributor.

## See also

- `references/api_contract.md` — exact envelope shapes for /api/search,
  /api/browse, /api/parallels (load on demand if you need to debug a response
  shape mismatch).
- `README.md` — installation instructions and acceptance-run procedure.
