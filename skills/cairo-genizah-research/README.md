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
