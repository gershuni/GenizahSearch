---
phase: 81B
plan: 04
type: execute
wave: 2
depends_on: [81B-01]
files_modified:
  - skills/cairo-genizah-research/SKILL.md
  - skills/cairo-genizah-research/README.md
  - skills/cairo-genizah-research/references/api_contract.md
  - .planning/REQUIREMENTS.md
autonomous: true
requirements: [SKILL-01, SKILL-02, SKILL-03, SKILL-04]
tags: [skill, documentation, skill-md, anthropic-skill, wave-2]
must_haves:
  truths:
    - "SKILL.md frontmatter has `name: cairo-genizah-research` (≤64 chars, lowercase+hyphens, no 'anthropic'/'claude') and `description` ≤1024 chars"
    - "SKILL.md body ≤500 lines per Anthropic guidance"
    - "SKILL.md `## Surface compatibility` section explicitly documents R1: works on Claude Code/Desktop with network; does NOT work on Claude API surface (no network in code-execution containers)"
    - "SKILL.md `## Configuration` section documents D-09: env wins over CLI flag (R6 mitigation)"
    - "SKILL.md instructions tell the model to extract 2-4 phrases, run stage.py, drill top-N via browse.py, append honesty annotation when text_source != 'pgp_transcription' (R2 mapping)"
    - "REQUIREMENTS.md SKILL-04 patched to reference the actual Phase 79 D-10 enum (or explicit mapping note added) — closing R2"
    - "Skill discoverable description triggers on Genizah/Hebrew manuscript queries"
  artifacts:
    - path: "skills/cairo-genizah-research/SKILL.md"
      provides: "Anthropic Skill instruction file (Level 2 progressive disclosure); frontmatter + workflow body"
      contains: "name: cairo-genizah-research"
    - path: "skills/cairo-genizah-research/README.md"
      provides: "Human-facing install/usage guide for the skill"
      min_lines: 30
    - path: "skills/cairo-genizah-research/references/api_contract.md"
      provides: "Loaded-on-demand reference of locked envelope shapes (Level 3 progressive disclosure)"
      min_lines: 40
    - path: ".planning/REQUIREMENTS.md"
      provides: "R2 mapping note added or SKILL-04 enum-value patch"
      contains: "pgp_transcription"
  key_links:
    - from: "skills/cairo-genizah-research/SKILL.md"
      to: "skills/cairo-genizah-research/scripts/stage.py"
      via: "instruction references CLI invocation"
      pattern: "stage\\.py"
    - from: "skills/cairo-genizah-research/SKILL.md"
      to: "skills/cairo-genizah-research/scripts/browse.py"
      via: "instruction references CLI invocation"
      pattern: "browse\\.py"
    - from: "skills/cairo-genizah-research/SKILL.md"
      to: "skills/cairo-genizah-research/references/api_contract.md"
      via: "see-also pointer for Level-3 disclosure"
      pattern: "api_contract"
---

<objective>
Author the Anthropic Skill instruction file (`SKILL.md`) plus human-facing README and on-demand API contract reference. Patches REQUIREMENTS.md SKILL-04 to close the R2 enum mismatch (or add an explicit mapping note). Documentation-only — no source code; runs in parallel with Plan 03 (no file overlap).

Purpose: Per CONTEXT D-01, the deliverable is an Anthropic Skill (SKILL.md + scripts), not a bare CLI. The model invokes scripts via the bash tool driven by SKILL.md instructions; without SKILL.md, the scripts are unreachable. SKILL.md must declare R1 (Claude API has no network → skill targets Claude Code / Desktop with network) and R2 (text_source enum mapping) explicitly so the user — and any future contributor — has zero ambiguity about surface compatibility and the contract gap with REQUIREMENTS.md.

Output: 4 files. SKILL.md ≤ 500 lines per Anthropic guidance. README explains installation (copy `skills/cairo-genizah-research/` to `~/.claude/skills/cairo-genizah-research/`). references/api_contract.md is the Level-3 lookup the model loads via bash when debugging envelope shape questions.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/STATE.md
@.planning/REQUIREMENTS.md
@.planning/phases/81B-claude-skill-consumer/81B-CONTEXT.md
@.planning/phases/81B-claude-skill-consumer/81B-RESEARCH.md
@.planning/phases/81A-api-contract-expansion/81A-CONTEXT.md
@.planning/phases/79-api-browse-drill-down/79-CONTEXT.md
@.planning/phases/80-api-parallels/80-CONTEXT.md
@.planning/phases/77-serializer-json-export/77-CONTEXT.md
</context>

<tasks>

<task type="auto">
  <name>Task 1: SKILL.md instruction file (Level-2 progressive disclosure)</name>
  <files>skills/cairo-genizah-research/SKILL.md</files>
  <read_first>
    - .planning/phases/81B-claude-skill-consumer/81B-RESEARCH.md (full file — frontmatter schema, three-level disclosure, runtime constraints per surface, R1/R2/R6/R9 mitigations, Open Q1-Q8 recommendations)
    - .planning/phases/81B-claude-skill-consumer/81B-CONTEXT.md (D-01 through D-12 — all locked decisions)
    - .planning/REQUIREMENTS.md (SKILL-01..06 verbatim text)
    - .claude/skills/release/SKILL.md (existing project skill — frontmatter + body conventions)
  </read_first>
  <action>
    Create `skills/cairo-genizah-research/SKILL.md` with the following exact structure. Body must stay ≤500 lines. Use Markdown headings as anchors so the model can navigate quickly.

    ```markdown
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
    ```

    Constraints:
    - Frontmatter `name`: `cairo-genizah-research` (24 chars, lowercase + hyphens, contains neither "anthropic" nor "claude").
    - Frontmatter `description`: ~600 chars (well under 1024 cap), opens with action verb, names the surface (`genizahsearch.com APIs`), enumerates trigger conditions.
    - Body: Use literal Markdown level-2 headings exactly as listed above (the model uses them as anchors).
    - Total file ≤500 lines.
  </action>
  <verify>
    <automated>python -c "
import re, pathlib
p = pathlib.Path('skills/cairo-genizah-research/SKILL.md')
text = p.read_text(encoding='utf-8')
lines = text.splitlines()
assert len(lines) <= 500, f'SKILL.md exceeds 500 lines ({len(lines)})'
m = re.match(r'^---\s*\n(.*?)\n---\s*\n', text, re.DOTALL)
assert m, 'frontmatter delimiter missing'
fm = m.group(1)
assert re.search(r'^name:\s*cairo-genizah-research\s*$', fm, re.MULTILINE), 'name missing/wrong'
assert 'description:' in fm
desc_match = re.search(r'description:\s*\|?\s*\n((?:  .*\n?)+)', fm)
assert desc_match, 'description missing'
desc = desc_match.group(1)
assert len(desc) <= 1024, f'description exceeds 1024 chars ({len(desc)})'
assert 'anthropic' not in 'cairo-genizah-research'
assert 'claude' not in 'cairo-genizah-research'
print('OK lines=' + str(len(lines)) + ' desc_chars=' + str(len(desc)))
"</automated>
  </verify>
  <acceptance_criteria>
    - File `skills/cairo-genizah-research/SKILL.md` exists.
    - Verify command above prints `OK lines=N desc_chars=M` with N ≤ 500, M ≤ 1024.
    - `grep -E "^name: cairo-genizah-research$" skills/cairo-genizah-research/SKILL.md` returns 1 line.
    - `grep -c "^## " skills/cairo-genizah-research/SKILL.md` returns ≥ 8 (at least 8 H2 sections).
    - `grep "Surface compatibility" skills/cairo-genizah-research/SKILL.md` returns ≥1 line (R1 section present).
    - `grep "Claude API" skills/cairo-genizah-research/SKILL.md` returns ≥1 line (R1 limitation explicit).
    - `grep "no network" skills/cairo-genizah-research/SKILL.md` returns ≥1 line.
    - `grep "GENIZAH_API_BASE" skills/cairo-genizah-research/SKILL.md` returns ≥1 line.
    - `grep "env wins" skills/cairo-genizah-research/SKILL.md` returns ≥1 line (R6 / D-09).
    - `grep "pgp_transcription" skills/cairo-genizah-research/SKILL.md` returns ≥2 lines (R2 mapping documented).
    - `grep "stage.py" skills/cairo-genizah-research/SKILL.md` returns ≥2 lines (workflow references CLI).
    - `grep "browse.py" skills/cairo-genizah-research/SKILL.md` returns ≥2 lines.
    - `grep "parallels.py" skills/cairo-genizah-research/SKILL.md` returns ≥1 line.
    - `grep "Do NOT retry" skills/cairo-genizah-research/SKILL.md` returns 1 line (D-08).
    - `grep "Tier A" skills/cairo-genizah-research/SKILL.md` returns ≥1 line.
    - `grep "honesty" skills/cairo-genizah-research/SKILL.md` returns ≥2 lines.
    - `grep "rate_limited" skills/cairo-genizah-research/SKILL.md` returns ≥1 line.
    - `grep "manuscript_page_not_found" skills/cairo-genizah-research/SKILL.md` returns ≥1 line.
  </acceptance_criteria>
  <done>SKILL.md authored ≤500 lines, frontmatter valid, R1 + R2 + R6 + D-08 + D-09 + ranking schema all explicit. Plan 05 acceptance run loads this file when the skill triggers.</done>
</task>

<task type="auto">
  <name>Task 2: README.md install/usage + references/api_contract.md</name>
  <files>skills/cairo-genizah-research/README.md, skills/cairo-genizah-research/references/api_contract.md</files>
  <read_first>
    - skills/cairo-genizah-research/SKILL.md (just-authored — README cross-references workflow)
    - .planning/phases/77-serializer-json-export/77-CONTEXT.md (envelope shape)
    - .planning/phases/79-api-browse-drill-down/79-CONTEXT.md (browse envelope + text_source enum)
    - .planning/phases/80-api-parallels/80-CONTEXT.md (parallels envelope; mode field name)
    - .planning/phases/81A-api-contract-expansion/81A-CONTEXT.md (search_mode enum + responsa_options)
    - shared/api_errors.py (error code list)
  </read_first>
  <action>
    **`skills/cairo-genizah-research/README.md`** — human-facing doc (≥30 lines):

    ```markdown
    # Cairo Genizah Research — Anthropic Skill

    Drives genizahsearch.com APIs to find candidate Genizah manuscript witnesses for
    a phrase, piyyut, responsum, or composition. v7.10 acceptance harness for
    GenizahSearch's internal API.

    ## Installation

    ### Claude Code (primary target)

    Copy this directory to your Claude Code skills location:

    ```bash
    # Personal (all your projects)
    cp -r skills/cairo-genizah-research ~/.claude/skills/

    # Project-only
    mkdir -p .claude/skills && cp -r skills/cairo-genizah-research .claude/skills/
    ```

    Restart Claude Code (or wait for live change detection). Verify:

    ```bash
    ls ~/.claude/skills/cairo-genizah-research/SKILL.md
    ```

    ### Claude Desktop (Pro/Max/Team/Enterprise)

    1. Zip the skill directory: `cd skills && zip -r cairo-genizah-research.zip cairo-genizah-research/`.
    2. In Claude Desktop: Settings > Features > Custom Skills > Upload.
    3. Code execution + network access must be enabled in admin settings.

    ### Claude API (NOT SUPPORTED in v7.10)

    Code-execution containers on the Claude API surface have no outbound network
    access and cannot reach genizahsearch.com. v7.10 acceptance run targets Claude
    Code only. v7.11 may add an egress-allowlisted path.

    ## Configuration

    Set env vars in your shell or Claude Code config:

    | Variable | Default | Purpose |
    |----------|---------|---------|
    | `GENIZAH_API_BASE` | `https://genizahsearch.com` | Base URL (env wins over `--base-url`). |
    | `GENIZAH_TOP_N` | `10` | Top-N for drill-down (bounded [1, 25]). |
    | `GENIZAH_SKILL_REQ_PER_MIN` | `24` | Throttle ceiling per endpoint. |
    | `GENIZAH_SKILL_BURST` | `5` | Token-bucket burst. |

    For local development against a dev server: `export GENIZAH_API_BASE=http://localhost:8080`.

    ## Smoke test

    ```bash
    python skills/cairo-genizah-research/scripts/search.py --query "ויאמר" --search-mode exact --limit 1
    ```

    Expected: JSON envelope with `schema_version: 1`, `source: "search"`, `results: [...]`.

    ## Acceptance run procedure

    Per ROADMAP.md Phase 81B phase gate, the acceptance run is live and user-observed:

    1. Install skill (above).
    2. Open Claude Code in any directory.
    3. Ask a real scholarly question, e.g. "Find Cairo Genizah witnesses to the piyyut
       'אין אדיר כי-י-י' — list shelfmarks with library and brief evidence."
    4. Confirm Claude invokes `cairo-genizah-research`, runs through `stage.py` →
       `browse.py` chain, returns ranked Tier A/B/C list with shelfmarks, libraries,
       browse URLs, image URLs, justifications, and honesty annotations where
       text_source != "pgp_transcription".
    5. Sign off (or report bugs) on at least one query.

    ## Architecture

    Three-level Anthropic Skill progressive disclosure:

    | Level | Loaded | Content |
    |-------|--------|---------|
    | 1 | Always | Frontmatter `name` + `description` (~100 tokens) |
    | 2 | On trigger | `SKILL.md` body (workflow instructions) |
    | 3 | On demand | `references/api_contract.md`, fixture JSON, scripts via bash |

    Scripts execute via the model's `bash` tool — only stdout/stderr enters context,
    not script source. This keeps token cost flat.

    ## See also

    - `SKILL.md` — instructions loaded by the model on trigger.
    - `references/api_contract.md` — locked envelope shapes for debugging.
    - `scripts/` — Python transport + business-logic helpers.
    ```

    **`skills/cairo-genizah-research/references/api_contract.md`** — Level-3 lookup (≥40 lines):

    ```markdown
    # GenizahSearch API contract (v7.10) — for skill debugging

    Locked by Phases 77, 78, 79, 80, 81A. The skill MUST match these shapes exactly.
    Load this file via `cat references/api_contract.md` only when debugging an
    envelope-shape mismatch.

    ## Envelope (all endpoints)

    Every successful response carries:
    - `schema_version: 1` (integer; bump on breaking changes)
    - `source: "search" | "browse" | "parallels"`
    - `generated_at: ISO-8601 string`
    - `request: {...echoed request fields...}` (Phase 81A D-04)
    - `count: int` (current page) / `total: int` (full)
    - `warnings: [{code, message, ...}]` (top-level — never inside results)
    - `results: [...]` (shape varies per endpoint)

    Error envelope (any endpoint):
    ```json
    {"error": {"code": "<error_code>", "message": "<human readable>"}}
    ```
    HTTP 429 carries `Retry-After: <seconds>` header.

    ## POST /api/search request

    ```json
    {
      "search_mode": "exact" | "variants" | "regex" | "responsa" | "title" | "shelfmark",
      "query": "<1-1000 chars; 256 if regex>",
      "gap": 0,
      "limit": 10,
      "filters": {"library": ["CUL"], "domain": ["Liturgy"], ...},
      "responsa_options": {"variants": false, "ja": false, "flex_spacing": false, "bidirectional": false}
    }
    ```

    Notes:
    - Phase 81A REPLACED the old `mode` field. `mode` is hard-rejected.
    - `responsa_options` only with `search_mode: "responsa"`; otherwise 400 invalid_combination.
    - `limit` ceiling 100 (Phase 81A D-05).

    ## POST /api/search response

    Each `results[i]` item:
    ```json
    {
      "uid": "<UID>",
      "locator": {"sys_id": "...", "volume_ie": "...", "p_num": 1, "fl_id": "..."},
      "score": 0.8731,
      "shelfmark": "T-S 12.123",
      "title": "...",
      "snippet": "...highlighted text...",
      "excerpt": "...short text...",
      "metadata": {"library": "CUL", "library_name": "...", "domains": [...], "dating": "..."},
      "image_url": "/api/nli_image_by_sysid/<sys_id>?page=<p_num-1>" or null
    }
    ```

    ## GET /api/browse

    Query params (one of):
    - `?uid=<UID>` (preferred)
    - `?sys_id=<S>&p_num=<N>&volume_ie=<IE>` (volume_ie optional)
    - `?fl_id=<F>`
    - Optional `?text_cap=<100..10000>`

    Response:
    ```json
    {
      "schema_version": 1,
      "source": "browse",
      "generated_at": "...",
      "locator": {"uid": "...", "sys_id": "...", "volume_ie": "...", "p_num": N, "fl_id": "..."},
      "shelfmark": "...",
      "title": "...",
      "library_code": "CUL",
      "library_name": "...",
      "text": "...",
      "text_source": "pgp_transcription" | "snippet" | "none",
      "text_truncated": false,
      "metadata": {"pgp": {...} | null, "fjms": {...} | null, "nli": {...} | null},
      "image": {"url": "..." | null, "provider": "..." | null, "sources": [...]},
      "warnings": []
    }
    ```

    **CRITICAL — text_source enum (R2 mapping):** REQUIREMENTS.md SKILL-04 says
    `!= 'full'`; the locked enum is `pgp_transcription | snippet | none`. The skill
    treats `pgp_transcription` as the equivalent-of-full case. See SKILL.md
    "R2 mapping" section.

    ## POST /api/parallels request

    ```json
    {
      "text": "<≤20000 chars>",
      "chunk_size": 5,
      "mode": "exact" | "variants" | "fuzzy",
      "max_freq": null,
      "boundary_mode": "full" | "boundary" | "combined" | null,
      "filters": {...}
    }
    ```

    Note: parallels uses `mode` NOT `search_mode` (Phase 81A D-07 kept name as-is).

    Response: same envelope; `results` are groups (per sys_id) with `aggregate_score`,
    `matches: [{chunk_index, score, source_chunk_text, manuscript_snippet}]`. Top-level
    also has `filtered: [...]` (always present, possibly empty per Phase 80 D-04).

    ## Error code catalogue

    See `shared/api_errors.py` (in the GenizahSearch repo) for the canonical list.
    Common codes the skill encounters:
    - `rate_limited` (HTTP 429 + Retry-After header)
    - `core_timeout` (HTTP 504; Tantivy/csv_bank hung)
    - `manuscript_page_not_found` (HTTP 404)
    - `locator_conflict` (HTTP 400)
    - `invalid_request` (HTTP 400; e.g. unknown field, including legacy `mode`)
    - `invalid_combination` (HTTP 400; e.g. responsa_options with non-responsa)
    - `invalid_filter_value` (HTTP 400; unknown filter token)
    - `filter_vocabulary_unavailable` (HTTP 503; FJMS sidecar misloaded)
    - `query_required`, `query_too_long`, `regex_pattern_too_long`
    - `composition_required`, `composition_too_long`

    ## Rate limits

    Per-endpoint independent buckets, server enforces 30 rpm per IP per bucket
    (Phase 78/79/80 HARDEN-01, D-05). Skill self-throttles to 24 rpm per bucket
    (6 rpm headroom; SKILL-06).
    ```
  </action>
  <verify>
    <automated>python -c "
import pathlib
r = pathlib.Path('skills/cairo-genizah-research/README.md').read_text(encoding='utf-8')
assert len(r.splitlines()) >= 30
assert 'Installation' in r
assert 'Claude Code' in r
assert 'GENIZAH_API_BASE' in r
ac = pathlib.Path('skills/cairo-genizah-research/references/api_contract.md').read_text(encoding='utf-8')
assert len(ac.splitlines()) >= 40
assert 'search_mode' in ac
assert 'pgp_transcription' in ac
assert 'rate_limited' in ac
print('OK')
"</automated>
  </verify>
  <acceptance_criteria>
    - File `skills/cairo-genizah-research/README.md` exists (≥30 lines).
    - File `skills/cairo-genizah-research/references/api_contract.md` exists (≥40 lines).
    - Verify command prints `OK`.
    - `grep "Installation" skills/cairo-genizah-research/README.md` returns ≥1 line.
    - `grep "Acceptance run" skills/cairo-genizah-research/README.md` returns ≥1 line.
    - `grep "NOT SUPPORTED" skills/cairo-genizah-research/README.md` returns ≥1 line (R1 explicit).
    - `grep "search_mode" skills/cairo-genizah-research/references/api_contract.md` returns ≥3 lines.
    - `grep "pgp_transcription" skills/cairo-genizah-research/references/api_contract.md` returns ≥2 lines.
    - `grep "R2 mapping" skills/cairo-genizah-research/references/api_contract.md` returns ≥1 line.
    - `grep -c "^- \`" skills/cairo-genizah-research/references/api_contract.md` returns ≥6 (error code catalogue).
  </acceptance_criteria>
  <done>README and api_contract reference authored. Skill is now self-installing (per README) and self-debugging (per api_contract.md).</done>
</task>

<task type="auto">
  <name>Task 3: Patch REQUIREMENTS.md SKILL-04 to close R2 enum mismatch</name>
  <files>.planning/REQUIREMENTS.md</files>
  <read_first>
    - .planning/REQUIREMENTS.md (current SKILL-04 wording at lines 60-62: `text_source != 'full'`)
    - .planning/phases/79-api-browse-drill-down/79-CONTEXT.md (D-10 enum: `pgp_transcription | snippet | none`)
    - .planning/phases/81B-claude-skill-consumer/81B-RESEARCH.md (R2 — recommends Option 1: skill maps `pgp_transcription` → "full")
  </read_first>
  <action>
    Edit `.planning/REQUIREMENTS.md` SKILL-04 line. Find the existing line:

    > `**SKILL-04**: Browse honesty. When `/api/browse` returns `text_source != 'full'`, the candidate's justification appends `"(full text unavailable; based on snippet of N chars)"`. ...`

    Replace with the corrected wording that references the locked Phase 79 enum and explicitly notes the mapping:

    > `**SKILL-04**: Browse honesty. When `/api/browse` returns `text_source != 'pgp_transcription'` (the locked Phase 79 D-10 enum value carrying full transcription; other values are `snippet` or `none`), the candidate's justification appends `"(full text unavailable; based on snippet of N chars)"`. When `image_url` is null or NLI returns 4xx for the image, the output appends `"(no image available)"`. Researchers always know what evidence the justification is grounded in. (R2 mapping note added 2026-05-04 by Phase 81B Plan 04: original draft used `'full'` as a placeholder before Phase 79 locked the enum; the skill maps `pgp_transcription` → "full" via the `_FULL_TEXT_SOURCE` constant in `skills/cairo-genizah-research/scripts/format_output.py`.)`

    Use the Edit tool with `old_string` matching the existing SKILL-04 line exactly and `new_string` providing the patched version. Verify the rest of REQUIREMENTS.md is byte-unchanged outside the targeted line.
  </action>
  <verify>
    <automated>python -c "
import pathlib, re
r = pathlib.Path('.planning/REQUIREMENTS.md').read_text(encoding='utf-8')
# SKILL-04 entry must reference pgp_transcription, not the literal 'full' as the trigger value.
m = re.search(r'\*\*SKILL-04\*\*.*?(?=\n- \[\s\] \*\*SKILL-05\*\*)', r, re.DOTALL)
assert m, 'SKILL-04 entry not found'
entry = m.group(0)
assert 'pgp_transcription' in entry, 'pgp_transcription missing from SKILL-04 patched entry'
assert 'R2 mapping' in entry or 'Phase 79' in entry, 'mapping note missing'
# The literal trigger 'text_source != \\'full\\'' should NOT appear in the patched SKILL-04 entry.
assert \"text_source != 'full'\" not in entry, 'old text_source != full clause still present'
print('OK')
"</automated>
  </verify>
  <acceptance_criteria>
    - `grep "SKILL-04" .planning/REQUIREMENTS.md` returns 1 line (no duplication).
    - Verify command prints `OK`.
    - `grep "pgp_transcription" .planning/REQUIREMENTS.md` returns ≥1 line in the SKILL-04 context.
    - `grep "Phase 81B" .planning/REQUIREMENTS.md` returns ≥1 line (the patch attribution).
    - `git diff .planning/REQUIREMENTS.md` shows ONLY the SKILL-04 line changed (other requirements untouched).
    - SKILL-05 line is byte-unchanged: `grep -A 0 "SKILL-05" .planning/REQUIREMENTS.md` matches the original wording.
  </acceptance_criteria>
  <done>REQUIREMENTS.md SKILL-04 patched. R2 enum mismatch closed: skill code and REQUIREMENTS.md now agree that `pgp_transcription` is the trigger value.</done>
</task>

</tasks>

<threat_model>

## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| SKILL.md description string → Claude's metadata layer | Trigger signal; influences when the skill loads. Wording must be specific enough to fire on Genizah queries and not over-fire on general Hebrew text. |
| References/api_contract.md → model debug context | Trusted reference; matches locked envelope shapes. Risk: shape drift if Phase 79/80 contract changes without skill update. |
| REQUIREMENTS.md edit | Single targeted line; rest of file is byte-unchanged. Risk: accidental wider edit corrupts other phase plans. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-81B-14 | Spoofing | Skill description triggers on unrelated Hebrew queries | mitigate | Description names specific surfaces ("Cairo Genizah", "T-S, ENA, JTS"); explicit "Do NOT use this skill for general Hebrew translation" in `## When to use`. |
| T-81B-15 | Tampering | Errant edit corrupts other REQUIREMENTS entries | mitigate | Acceptance grep confirms `git diff` shows ONLY SKILL-04 line changed. |
| T-81B-16 | Information Disclosure | api_contract.md leaks server-internal error codes | accept | All error codes already public in shared/api_errors.py; skill is consumer documentation. |
| T-81B-17 | Repudiation | R2 mapping decision is unattributed | mitigate | Patch attribution `(R2 mapping note added 2026-05-04 by Phase 81B Plan 04: ...)` in REQUIREMENTS.md and `_FULL_TEXT_SOURCE` constant in format_output.py both reference the mapping origin. |

</threat_model>

<verification>
- All 3 task verify commands print `OK`.
- `grep -E "^- \[ \] \*\*SKILL-0[1-6]\*\*" .planning/REQUIREMENTS.md` returns 6 lines (all 6 requirements still present).
- `git diff --stat .planning/REQUIREMENTS.md` shows 1 file changed with small line delta (≤5).
- SKILL.md frontmatter validates as YAML: `python -c "import yaml, re; t = open('skills/cairo-genizah-research/SKILL.md').read(); fm = re.match(r'^---\\s*\\n(.*?)\\n---', t, re.DOTALL).group(1); yaml.safe_load(fm); print('YAML OK')"`.
- No regression in skill tests: `pytest tests/test_skill_consumer.py tests/test_skill_throttle.py` passes 22/22 (Plan 03's tests still GREEN).
</verification>

<success_criteria>
- SKILL.md authored ≤500 lines with valid frontmatter.
- README install procedure complete (Claude Code + Desktop + Claude API not-supported note).
- references/api_contract.md is the Level-3 reference for envelope debugging.
- REQUIREMENTS.md SKILL-04 patched: R2 enum mismatch CLOSED.
- R1 (Claude API surface incompatibility) explicitly documented in SKILL.md and README.
- D-09 (env wins over CLI) loudly documented (R6 mitigation).
- All ranking schema fields per SC-2 documented in SKILL.md `## Workflow` step 7.
</success_criteria>

<output>
After completion, create `.planning/phases/81B-claude-skill-consumer/81B-04-SUMMARY.md`:
- Files created/modified
- SKILL.md line count, description char count
- Confirmation R1 + R2 + R6 documented
- Confirmation REQUIREMENTS.md byte-diff is scoped to SKILL-04 only
</output>
