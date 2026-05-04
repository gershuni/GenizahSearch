# Phase 81B: Claude Skill Consumer — Research

**Researched:** 2026-05-04
**Domain:** Anthropic Agent Skill format + cross-surface portability + GenizahSearch API consumption
**Confidence:** HIGH on Skill format and API contracts; MEDIUM on cross-surface throttle persistence

## Summary

Phase 81B builds a runnable Anthropic Agent Skill (SKILL.md + scripts/) that drives the locked v7.10 API surface (`/api/search` → `/api/browse`, with `/api/parallels` as a sub-mode) end-to-end via staged phrase discovery. The artifact is portable across Claude Code, Claude Desktop, claude.ai, and the Claude API per the [agentskills.io](https://agentskills.io) open standard, but with **runtime constraints that vary sharply by surface** — Claude API skills run in a sandboxed code-execution VM with **no network access**, while Claude Code skills run on the user's machine with full network access. This is the single largest landmine in this phase: the skill must declare its network requirement honestly and the v7.10 acceptance run targets Claude Code (or Claude Desktop with code execution + network).

The Skill format itself is well-specified: a directory containing `SKILL.md` (YAML frontmatter `name` + `description`, plus markdown body), optional `scripts/` and reference files, with progressive disclosure (Level 1 metadata always loaded, Level 2 SKILL.md body loaded on trigger, Level 3 bundled files loaded on demand via bash). Scripts are executed via the model's `bash` tool — output flows back as bash output, **never the script source**, which keeps token cost flat. State persistence across script invocations within a single skill run is filesystem-based (the model invokes `python script.py` multiple times in the same session/working directory).

**Primary recommendation:** Build a Python-3-only skill (`requests` for HTTP — simpler than `httpx` async, sufficient for sequential 25-call workflows; pre-installed in Claude API code-execution sandbox per Anthropic docs). Throttle state lives in a JSON file in the skill's working directory, written by every script invocation. Base URL via `GENIZAH_API_BASE` env var with `--base-url` CLI override (env wins per CONTEXT D-09). Skill targets Claude Code as the primary surface; Claude API support is a documented limitation (no network → won't work without server-side egress allowlisting).

## User Constraints (from CONTEXT.md)

### Locked Decisions

**Format & Location**
- **D-01:** Anthropic Skill (SKILL.md + scripts) — not a slash command, not a bare CLI.
- **D-02:** Skill lives **external to GenizahSearch repo** — its filesystem location is environment-specific (per SKILL-01 / SC-1). Phase 81B deliverable in this repo is planning artifacts only.
- **D-03:** Local-data hook (Tantivy / `transcriptions.txt`) **documented, not implemented**. v7.10 ships API-only; SKILL.md notes the future extension point.

**Endpoint Coverage**
- **D-04:** Skill exercises **all three endpoints** — `/api/search`, `/api/browse`, `/api/parallels`.

**Ranking & Justifications**
- **D-05:** **Hybrid ranking** — API order + Claude-authored justification per candidate, grounded in browse text. No LLM rerank.
- **D-06:** Top N **default 10**, configurable; bounded `[1, 25]`.

**Error-Handling UX**
- **D-07:** Per-candidate inline note + continue. End-of-run summary line.
- **D-08:** **No retry logic** in v7.10. First failure → inline note → move on.

**Configuration**
- **D-09:** Base URL — `GENIZAH_API_BASE` env var; `--base-url` CLI flag overrides; env wins. Defaults to `https://genizahsearch.com`.
- **D-10:** Top-N override via `GENIZAH_TOP_N` env var or `--top-n` CLI flag.

**Result-Shape Handling**
- **D-11:** Justification logic differs by endpoint:
  - `/api/search`: one justification per result item.
  - `/api/parallels`: one justification per group (per `sys_id`); browse fetched once per group using group-level locator.
  - Skill reads `uid` (preferred) and `locator: {sys_id, volume_ie, p_num}` (fallback).

**Acceptance Run**
- **D-12:** Phase gate is a **live user-observed run**. No in-repo CI smoke test for v7.10.

### Claude's Discretion

- Skill name (`genizah-search`, `cairo-genizah-research`, etc.) — planner picks.
- HTTP client (`httpx` async vs `requests` sync) — planner picks.
- Acceptance-query source (runtime vs sample suite) — planner picks; runtime preferred.
- Justification length / format (1–2 sentences plain text).
- Output format (Markdown vs JSON) — Markdown preferred.
- `/api/parallels` invocation surface — separate sub-mode invoked when input looks like a composition (multi-line text > 200 chars), else `/api/search`.
- Hosting repo for the external skill artifact — out of scope.

### Deferred Ideas (OUT OF SCOPE)

- Local-data shortcut (Tantivy / transcriptions.txt) — v7.11.
- Retry-with-backoff transport — v7.11.
- LLM rerank after browse — v7.11.
- In-repo CI smoke test — v7.11.
- Curated sample-query suite — v7.11.
- Justification quality eval — v7.11.
- Multi-language UX — v7.11.

## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| SKILL-01 | Reference consumer skill/harness with configurable base URL (default = production); runnable; filesystem location not pinned to repo path. | Skill format (Level 1 metadata + Level 2 instructions + Level 3 scripts) — see Implementation Approach §1; D-09 config pattern in §2. |
| SKILL-02 | Staged phrase discovery: extract phrases from `query`/`base_text`, multiple `/api/search` calls (using API-EXPAND surface), merge by `uid`/`sys_id`, drill down via `/api/browse` for top-N, return ranked candidates with justifications grounded in browse text. | Search-mode enum from 81A (5 values), uid contract from 77 D-13, browse drill-down from 79 D-06; merge-by-uid pattern in §3. |
| SKILL-03 | Handle 429 / timeouts / partial `/api/browse` data without crashing; surface in plain terms. | Error envelope shape from 78 D-07 + 79 D-16 + 80 D-06; per-candidate inline note pattern in §4. |
| SKILL-04 | Browse honesty. When `text_source != 'full'`, append `(full text unavailable; based on snippet of N chars)`. When `image_url` null or NLI 4xx, append `(no image available)`. | **NAMING GAP:** SKILL-04 says `text_source != 'full'` but Phase 79 D-10 enum is `pgp_transcription | snippet | none` — no `'full'` value. See §4. |
| SKILL-05 | Optional `known_witnesses[]` + `known_witness_policy='flag'\|'exclude'` (default `flag`). Two-tier shelfmark normalization: lightweight local (Tier 1) + `/api/search?search_mode=shelfmark` resolution (Tier 2). Skill does NOT depend on `genizah_core`. | Shelfmark mode locked in 81A D-09 enum; normalization patterns in §5. |
| SKILL-06 | Token-bucket throttle, separate buckets per endpoint, default ≤24 req/min per bucket; burst capacity 5; `GENIZAH_SKILL_REQ_PER_MIN` env override. 15 search + 10 browse calls completes without self-rate-limiting. | Throttle state across script invocations is non-trivial (each bash call = fresh process); filesystem-backed pattern in §6. |

## Domain Overview (Anthropic Skill Format)

### Authoritative Specification

The Skill format is governed by:
1. **Anthropic platform docs** — [Agent Skills Overview](https://platform.claude.com/docs/en/agents-and-tools/agent-skills/overview) (cross-surface canonical reference).
2. **Claude Code docs** — [Extend Claude with skills](https://code.claude.com/docs/en/skills) (Claude Code-specific frontmatter extensions).
3. **agentskills.io** — open standard adopted by 30+ tools (cross-vendor portability surface).
4. **anthropics/skills GitHub** — example skills (skill-creator, document skills).

### Required SKILL.md Schema

```yaml
---
name: cairo-genizah-research            # max 64 chars; lowercase + digits + hyphens; no "anthropic" or "claude"
description: |                          # required; max 1024 chars; primary trigger signal
  Research Cairo Genizah manuscripts. Drives genizahsearch.com APIs to find
  candidate witnesses for a phrase or composition with browse-grounded justifications.
  Use when user asks about Genizah manuscripts, Hebrew/Judeo-Arabic medieval texts,
  shelfmarks (T-S, ENA, JTS), or wants to find parallels to a piyyut/responsum/letter.
---

# Skill body (markdown)
```

**Required frontmatter fields (cross-surface):** `name`, `description`. Everything else is optional. [VERIFIED: platform.claude.com docs]

**Claude-Code-only optional fields** (will not work on claude.ai or via API): `allowed-tools`, `disable-model-invocation`, `user-invocable`, `argument-hint`, `arguments`, `model`, `effort`, `context: fork`, `agent`, `hooks`, `paths`, `shell`. [VERIFIED: code.claude.com/docs/en/skills]

### Three-Level Progressive Disclosure

| Level | When loaded | Token cost | Content |
|-------|-------------|------------|---------|
| 1: Metadata | Always (at startup) | ~100 tokens per skill | `name` + `description` from YAML |
| 2: Instructions | When skill is triggered | ≤ 5k tokens (recommendation: SKILL.md < 500 lines) | SKILL.md body |
| 3: Resources / scripts | As needed | Effectively unlimited | Bundled files, scripts (output only enters context) |

[VERIFIED: platform.claude.com Agent Skills Overview] Crucially: **scripts are executed via bash; the script source code never enters the model's context — only the script's stdout/stderr does**. This makes Python helpers far more efficient than asking the model to construct equivalent logic inline.

### Runtime Execution Model (CRITICAL — varies by surface)

| Surface | Network access | Code execution | Notes |
|---------|----------------|----------------|-------|
| **Claude Code** | Full (user's machine) | Bash, Python via Bash | **Primary target for v7.10.** Skills are filesystem-based at `~/.claude/skills/<name>/` (personal) or `.claude/skills/<name>/` (project) or `<plugin>/skills/<name>/`. Live change detection within session. |
| **Claude Desktop** | Per user/admin settings (full / partial / none) | Code execution sandbox | Custom skills uploaded as ZIP via Settings > Features. Pro/Max/Team/Enterprise only. Not centrally managed. |
| **claude.ai (web)** | Per user/admin settings | Code execution sandbox | Same as Desktop. |
| **Claude API** | **NONE** | Code execution container (`code-execution-2025-08-25` beta) | **Skill cannot reach `genizahsearch.com` from API surface.** Requires three beta headers; pre-installed packages only; no `pip install` at runtime. |

[VERIFIED: platform.claude.com docs §"Runtime environment constraints"]

**Implication for Phase 81B:** the v7.10 acceptance run (D-12) targets Claude Code or Claude Desktop with full network access. Document the Claude API limitation in SKILL.md as "v7.10 not supported on Claude API surface due to no-network constraint; v7.11 may add an egress-allowlisted path."

### Where Skills Live On Disk

[VERIFIED: code.claude.com/docs/en/skills]

| Location | Path | Applies to |
|----------|------|-----------|
| Personal | `~/.claude/skills/<skill-name>/SKILL.md` | All user's projects |
| Project | `.claude/skills/<skill-name>/SKILL.md` | This project only |
| Plugin | `<plugin>/skills/<skill-name>/SKILL.md` | Where plugin is enabled |
| Enterprise | Managed settings | All users in org |

For the v7.10 acceptance run, the skill installs to `~/.claude/skills/cairo-genizah-research/` (personal scope) — runnable from any directory the user invokes Claude Code in. This satisfies SC-1's "filesystem location is environment-specific and not pinned to a specific repo path."

### Cross-Surface Sync (LANDMINE)

Custom Skills do **not** sync across surfaces [VERIFIED: platform.claude.com §"Cross-surface availability"]:
- Skills uploaded to claude.ai must be separately uploaded to API.
- Skills uploaded via API are not available on claude.ai.
- Claude Code skills are filesystem-based and separate from both.

A user who wants the skill on Claude Desktop *and* in Claude Code must install it twice. Document this in SKILL.md installation instructions.

## Validation Architecture

> Phase 81B's deliverable is external to the repo (D-02), so traditional pytest validation does not apply directly. Phase gate is a live user-observed run (D-12). Validation here is two-tiered:

### Test Framework
| Property | Value |
|----------|-------|
| Framework | None in-repo for the skill itself; live acceptance run is the gate. Optional: a thin smoke-test Python script bundled inside the skill (`scripts/smoke_test.py`) that hits `/api/search?search_mode=exact&query=test` and prints status. |
| Config file | None |
| Quick run command | `python scripts/smoke_test.py --base-url https://genizahsearch.com` (from inside the skill directory) |
| Full suite command | Live acceptance run via Claude Code: `claude` then invoke skill with a real scholarly query |
| Phase gate | User-observed live run against production; user signs off ranking quality on at least one scholarly query (D-12). |

### Phase Requirements → Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| SKILL-01 | Skill loads in Claude Code, base URL configurable | manual / smoke | `claude --version && ls ~/.claude/skills/cairo-genizah-research/SKILL.md` | ❌ Wave 0 (skill doesn't exist yet) |
| SKILL-01 | Smoke: production endpoint reachable | smoke (Python) | `python scripts/smoke_test.py` | ❌ Wave 0 |
| SKILL-02 | End-to-end staged discovery against production | manual (live) | Live skill invocation with scholarly query | manual-only |
| SKILL-03 | Graceful 429 / timeout / partial-NLI handling | manual (live) | Trigger with rapid-fire requests OR mock with low rate-limit env | manual-only |
| SKILL-04 | Browse honesty annotations appear when `text_source` != `pgp_transcription` | unit (Python) | `python -m scripts.test_honesty_annotations` (parses fixture browse responses, asserts annotation strings) | ❌ Wave 0 |
| SKILL-05 | Known-witness flag / exclude policy works | unit (Python) | `python -m scripts.test_known_witness_policy` (mock search results, assert filtering/marking) | ❌ Wave 0 |
| SKILL-06 | Throttle stays under ≤24 req/min per bucket; 15 search + 10 browse run completes | integration (Python) | `python -m scripts.test_throttle` — uses fake clock, asserts no self-429 | ❌ Wave 0 |

### Sampling Rate

- **Per task commit:** `python scripts/smoke_test.py` (sub-second; just hits one endpoint)
- **Per wave merge:** All `python -m scripts.test_*` scripts in skill directory
- **Phase gate:** Live user-observed run on production (D-12)

### Wave 0 Gaps

- [ ] `scripts/smoke_test.py` — covers SKILL-01 (production reachable + base-URL config)
- [ ] `scripts/test_honesty_annotations.py` — covers SKILL-04 (text_source / image annotation logic, with fixture browse JSON)
- [ ] `scripts/test_known_witness_policy.py` — covers SKILL-05 (shelfmark normalization + flag/exclude behavior)
- [ ] `scripts/test_throttle.py` — covers SKILL-06 (token-bucket math + filesystem state, fake-clock test)
- [ ] `scripts/fixtures/` — recorded `/api/search`, `/api/browse`, `/api/parallels` responses for offline testing (no network in unit tests)

**Note:** Standard pytest framework not used because (a) skill lives external to repo, (b) tests run inside the skill directory at user install location, (c) the model itself can run them via bash. Plain `python -m scripts.test_X` invocation keeps the skill self-contained.

## Implementation Approach

### §1. Skill Anatomy (SKILL-01)

```
~/.claude/skills/cairo-genizah-research/
├── SKILL.md                       # Level 2: instructions for the model
├── README.md                      # Human-facing install/usage docs
├── scripts/
│   ├── search.py                  # Calls /api/search; emits JSON to stdout
│   ├── browse.py                  # Calls /api/browse; emits JSON to stdout
│   ├── parallels.py               # Calls /api/parallels; emits JSON to stdout
│   ├── stage.py                   # Staged phrase discovery orchestrator (multiple search calls + merge by uid)
│   ├── normalize_shelfmark.py     # Tier-1 lightweight shelfmark normalization (SKILL-05)
│   ├── throttle.py                # Token-bucket helper (SKILL-06); imported by other scripts
│   ├── format_output.py           # Markdown rendering of ranked candidates with justifications
│   ├── smoke_test.py              # SKILL-01 smoke
│   ├── test_honesty_annotations.py
│   ├── test_known_witness_policy.py
│   ├── test_throttle.py
│   └── fixtures/                  # Recorded API responses for offline tests
├── references/
│   └── api_contract.md            # Locked envelope shapes (search/browse/parallels) — model loads on demand
└── state/                         # Throttle state JSON files (gitignored when skill repo lives external)
    └── .gitkeep
```

[ASSUMED: scripts in `scripts/` subdir is convention] — verified against [anthropics/skills repo structure](https://github.com/anthropics/skills) and code.claude.com example pattern. `scripts/` is the standard subdir name; not strictly enforced but matches every public example.

**SKILL.md body structure (≤ 500 lines per [Anthropic guidance, code.claude.com docs]):**

```markdown
# Cairo Genizah Research

You drive a research workflow against the GenizahSearch APIs to find
candidate witnesses for Hebrew / Judeo-Arabic medieval manuscripts.

## When to use
- User asks "find manuscripts containing X"
- User pastes a piyyut, responsum, letter and wants parallels
- User asks about a specific shelfmark and wants context

## Configuration
- Base URL: ${GENIZAH_API_BASE:-https://genizahsearch.com} (override with --base-url)
- Top N: ${GENIZAH_TOP_N:-10} (--top-n; bounded [1,25])
- Throttle: ${GENIZAH_SKILL_REQ_PER_MIN:-24}

## Workflow

1. Decide search mode based on input shape:
   - Multi-line text > 200 chars → composition → use scripts/parallels.py
   - Otherwise → text query → use scripts/stage.py (staged phrase discovery)

2. For text queries: extract 2–4 distinctive phrases from the query.
   Run scripts/stage.py to drive multiple /api/search calls and merge by uid.

3. For each top-N candidate, fetch /api/browse via scripts/browse.py with
   the locator (uid preferred, sys_id+volume_ie+p_num fallback).

4. Compose 1–2 sentence justifications per candidate, grounded in
   browse text. If text_source != 'pgp_transcription', append
   "(full text unavailable; based on snippet of N chars)".
   If image.url is null or sources[] is empty, append "(no image available)".

5. Apply known_witnesses policy (flag or exclude) per ARGUMENTS.

6. Render output (Markdown by default) via scripts/format_output.py.

7. End with summary line: "Processed N candidates: X succeeded, Y rate-limited, Z NLI image unavailable."

## See also
- references/api_contract.md — exact envelope shapes if you need to debug
- scripts/throttle.py — handles per-endpoint pacing automatically
```

### §2. Base-URL Configurability (SKILL-01 / D-09)

Each script reads in this priority:
1. `--base-url` CLI flag
2. `GENIZAH_API_BASE` env var
3. Default `https://genizahsearch.com`

**Per CONTEXT D-09 — env wins over CLI flag.** This is a deliberate inversion of the typical CLI convention; the rationale is "the user sets `GENIZAH_API_BASE=http://localhost:8080` once for development and forgets it; CLI overrides quietly hitting prod would surprise them." Document loudly in SKILL.md and `--help`.

```python
# scripts/_config.py (helper imported by all endpoint scripts)
import os
def resolve_base_url(cli_arg: str | None) -> str:
    env = os.environ.get("GENIZAH_API_BASE")
    if env:
        return env  # env wins per D-09
    if cli_arg:
        return cli_arg
    return "https://genizahsearch.com"
```

### §3. Staged Phrase Discovery + merge-by-uid (SKILL-02)

**Phrase extraction** (Tier 1 — lightweight, no LLM call from the script): the **model** authors phrases in the SKILL.md instruction layer, then the model invokes `scripts/stage.py --phrases "phrase1" "phrase2" ...`. Phrase selection is the model's job; the script is just a parallel HTTP fan-out + merger.

[ASSUMED: this division of labor matches CONTEXT D-05's "Claude composes ... grounded in the fetched browse text" — the model owns natural-language extraction, scripts own deterministic transport.]

**Merge by uid pattern** (the actual technical question):

```python
# scripts/stage.py — pseudocode
def merge_results(per_phrase_results: list[list[dict]]) -> list[dict]:
    """Merge results from N /api/search calls. Dedupe by uid; aggregate match scores."""
    by_uid: dict[str, dict] = {}
    for phrase_results in per_phrase_results:
        for item in phrase_results:
            uid = item["uid"]  # Phase 77 D-13 guarantees uid always populated
            if uid not in by_uid:
                by_uid[uid] = {**item, "_matched_phrases": [], "_phrase_count": 0}
            by_uid[uid]["_matched_phrases"].append(item.get("snippet", ""))
            by_uid[uid]["_phrase_count"] += 1
    # Tier ranking: more phrases matched = higher tier
    candidates = list(by_uid.values())
    for c in candidates:
        n = c["_phrase_count"]
        if n >= 3:
            c["_tier"] = "A"
        elif n == 2:
            c["_tier"] = "B"
        else:
            c["_tier"] = "C"
    candidates.sort(key=lambda c: (-c["_phrase_count"], -c.get("score", 0)))
    return candidates
```

[VERIFIED: `uid` is always populated per Phase 77 D-13; `locator: {sys_id, volume_ie, p_num}` is also always populated per same.]

Output of `stage.py` is JSON to stdout; the model reads it via bash output and decides which top-N to drill down on.

### §4. Browse Honesty Annotations (SKILL-04)

**CRITICAL NAMING GAP** — surfacing for planner discussion:

| Source | Value of `text_source` |
|--------|------------------------|
| **SKILL-04 (REQUIREMENTS.md)** | "When `text_source != 'full'`, append `(full text unavailable; based on snippet of N chars)`" |
| **Phase 79 D-10 (locked API contract)** | `"pgp_transcription" \| "snippet" \| "none"` — there is **no** `'full'` value. |

The skill must reconcile this. Two options:
1. Skill treats `text_source == "pgp_transcription"` as "full" (most natural mapping); appends honesty annotation when value is `"snippet"` or `"none"`.
2. Update REQUIREMENTS.md SKILL-04 to use the actual enum values.

**Recommendation: Option 1.** REQUIREMENTS.md was written before Phase 79 locked the enum; the API contract is canonical. Skill code:

```python
# scripts/format_output.py
def honesty_annotation(browse_response: dict) -> str:
    parts = []
    text_source = browse_response.get("text_source", "none")
    if text_source != "pgp_transcription":
        n = len(browse_response.get("text", ""))
        parts.append(f"(full text unavailable; based on snippet of {n} chars)")
    image = browse_response.get("image", {}) or {}
    if not image.get("url") and not image.get("sources"):
        parts.append("(no image available)")
    return " ".join(parts)
```

**Image-unavailable detection:** Phase 79 D-14 (revised per R-PR-01) means `image.url` is best-effort and the server does NOT probe NLI. Three signals indicate "no image":
1. `image.url is None`
2. `image.sources == []` (R-06 allows empty list when no usable URL exists)
3. (Optional, requires extra HTTP HEAD per candidate; defer to v7.11) `image.url` returns 4xx when fetched.

Per CONTEXT specifics, the v7.10 skill **cites image URLs in citations rather than rendering them**, so signals 1+2 are sufficient. Skip the HEAD probe — it doubles the latency budget.

**Parallels endpoint nuance (D-11):** for `/api/parallels` results, the justification is per-group (per `sys_id`). The browse fetch uses the group's locator (Phase 80 D-08 confirms every parallels result has uid + locator). Honesty annotation logic is identical.

### §5. known_witnesses[] handling (SKILL-05)

**Tier 1: lightweight local normalization** (script-side, no API call):

```python
# scripts/normalize_shelfmark.py
import re
import unicodedata

def normalize(s: str) -> str:
    """Tier-1 normalizer for shelfmark comparison. Best-effort, intentionally simple."""
    s = unicodedata.normalize("NFKC", s.strip())
    # Common library prefixes
    s = re.sub(r"^(MS\.?\s+|Ms\.?\s+|MS_)", "", s, flags=re.IGNORECASE)
    # Spacing variants: "T-S 12.123" == "T-S  12.123" == "T-S12.123"
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\s*([.\-])\s*", r"\1", s)
    return s.upper()
```

[ASSUMED] — covers the common cases (T-S, ENA-MS, MS Heb c). Edge cases (paired-leaf bifolio, library code prefixes like "JTS" before number) drop to Tier 2.

**Tier 2: API resolution** — call `/api/search?search_mode=shelfmark&query=<input>` for any known_witness whose Tier-1 normalized form doesn't match any candidate's normalized shelfmark. The shelfmark search mode was locked into the 81A enum (D-09). Map back to `sys_id`/`uid`.

**Policy application:**

```python
def apply_policy(candidates, known_uids: set[str], policy: str):
    if policy == "exclude":
        return [c for c in candidates if c["uid"] not in known_uids]
    elif policy == "flag":
        for c in candidates:
            c["known_witness"] = c["uid"] in known_uids
        return candidates
    raise ValueError(f"Unknown policy: {policy}")
```

**Important per SKILL-05:** "Skill does NOT depend on `genizah_core`." All shelfmark logic is duplicated in the skill (intentionally — the skill must be portable to systems without the GenizahSearch repo).

### §6. Throttle State Across Script Invocations (SKILL-06)

**The non-trivial problem:** Each invocation of `python scripts/search.py` is a fresh process. Token-bucket state must persist across invocations within a single skill run. Three options:

| Option | Persistence | Pros | Cons |
|--------|------------|------|------|
| **A. JSON file in skill dir** | Filesystem write per call | Simple, no deps, survives subprocess boundary | Race conditions if model parallelizes calls (rare); file locking via `fcntl` (Unix) / `msvcrt` (Windows) needed |
| **B. SQLite in skill dir** | SQLite ACID transactions | Concurrency-safe | Heavier; overkill for a 25-call workflow |
| **C. Pass state via stdin/stdout chain** | Model relays state JSON between calls | No filesystem | Burdens the model; brittle |

**Recommendation: Option A** with file locking. State file: `~/.claude/skills/cairo-genizah-research/state/throttle.json`.

```python
# scripts/throttle.py — pseudocode
import json
import time
import os
from pathlib import Path

STATE_FILE = Path(os.environ.get("CLAUDE_SKILL_DIR", ".")) / "state" / "throttle.json"

def acquire(bucket: str, rpm: int = 24, burst: int = 5) -> float:
    """Block until one token is available for `bucket`. Returns wait_seconds."""
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "a+") as f:
        # platform-aware file lock (fcntl on Unix, msvcrt on Win)
        _lock(f)
        f.seek(0)
        try:
            state = json.loads(f.read() or "{}")
        except json.JSONDecodeError:
            state = {}
        now = time.time()
        b = state.get(bucket, {"tokens": burst, "last_refill": now})
        # Refill: tokens += elapsed * (rpm / 60), capped at burst
        elapsed = now - b["last_refill"]
        b["tokens"] = min(burst, b["tokens"] + elapsed * (rpm / 60.0))
        b["last_refill"] = now
        wait = 0.0
        if b["tokens"] < 1.0:
            wait = (1.0 - b["tokens"]) * 60.0 / rpm
            time.sleep(wait)
            b["tokens"] = 0.0
        else:
            b["tokens"] -= 1.0
        state[bucket] = b
        f.seek(0); f.truncate()
        f.write(json.dumps(state))
        _unlock(f)
        return wait
```

**Bucket keys:** `"search"`, `"browse"`, `"parallels"`. Three buckets, each independently rate-limited at `≤24 req/min`. This matches the server's three independent buckets per Phase 78/79/80 D-05 (which run at 30 req/min each) and gives 6 req/min headroom for jitter.

`${CLAUDE_SKILL_DIR}` is a Claude-Code-only string substitution per [code.claude.com docs] — for cross-surface portability, scripts also fall back to the directory of `__file__`'s parent.

**Verification math:** SKILL-06 requires "15 search + 10 browse calls completes without triggering its own rate limit."
- 15 search calls at 24 rpm = 15 / 24 minutes = 37.5 sec minimum spacing (with burst 5, first 5 are immediate).
- 10 browse calls at 24 rpm with burst 5 = first 5 immediate, next 5 spaced at 60/24 = 2.5 sec each = ~12.5 sec total.
- Sequential total: ~37.5 + 12.5 = ~50 sec. Comfortable margin under any reasonable user timeout. [ASSUMED: model runs calls sequentially, not parallel — which is the typical bash invocation pattern.]

## Dependencies & Integration Points

### Phase 78 (`/api/search`) — Hardening Shell

[VERIFIED via 78-CONTEXT.md]

| Field | Skill use |
|-------|-----------|
| Request: `POST /api/search` body `{search_mode, query, gap?, limit?, filters?, responsa_options?}` | Skill builds requests; **after 81A**: uses `search_mode` enum (NOT old `mode` field — hard-rejected by `extra='forbid'`). |
| Response envelope: `{schema_version: 1, source: 'search', count, total, warnings: [], generated_at, request: {...}, results: [{uid, locator, score, snippet, metadata, ...}]}` | Skill reads `results[*].uid` (preferred) and `results[*].locator` (fallback). `request` echo block (81A D-04) lets skill verify server applied requested mode. |
| Error envelope: `{error: {code, message}}` | Skill maps `code` → plain-text inline note. Codes used: `rate_limited`, `query_required`, `query_too_long`, `invalid_request`, `invalid_combination`, `invalid_filter_value`. |
| Rate limit: 30 req/min per IP | Skill self-throttles to 24 (6 req/min headroom). |
| HTTP 429 with `Retry-After` header | Skill notes "rate-limited, retry-after Ns" and moves on (no retry per D-08). |

### Phase 79 (`/api/browse`) — Drill-Down

[VERIFIED via 79-CONTEXT.md]

| Field | Skill use |
|-------|-----------|
| Request: `GET /api/browse?sys_id=...&uid=...` (or `&p_num=...&volume_ie=...` or `&fl_id=...`) | Skill prefers uid; falls back to `sys_id+volume_ie+p_num` if uid absent (Phase 77 D-04 guarantees both populated). |
| Response: `{text, text_source, text_truncated, metadata: {pgp, fjms, nli}, image: {url, provider, sources: []}, locator: {uid, sys_id, volume_ie, p_num, fl_id}, warnings, ...}` | Skill reads `text` for justification grounding; `text_source` drives honesty annotation; `image.url` + `image.sources` drive image-availability annotation. |
| `text_source` enum: `pgp_transcription \| snippet \| none` | **NAMING GAP** with SKILL-04 (`!= 'full'`) — see §4. |
| Per-source enrichment failure → `metadata.{pgp\|fjms\|nli} = null` + warning entry | Skill tolerates null metadata sub-objects; doesn't crash. |
| 504 `core_timeout` if Tantivy/csv_bank hangs (R-01) | Skill maps to inline note "browse failed: core timeout"; moves on. |
| 404 `manuscript_page_not_found` for bad sys_id/uid pair | Skill maps to inline note "browse failed: manuscript page not found"; moves on. |
| Image is **best-effort, not probed** (D-14 / R-PR-01) | Skill must NOT assume `image.url` is reachable. Treat as candidate URL; cite without rendering. |

### Phase 80 (`/api/parallels`) — Composition Search

[VERIFIED via 80-CONTEXT.md]

| Field | Skill use |
|-------|-----------|
| Request: `POST /api/parallels` body `{text, chunk_size: int = 5, mode: 'exact'\|'variants'\|'fuzzy', max_freq?, boundary_mode: 'full'\|'boundary'\|'combined', filters?}` | Skill invokes when input multi-line text > 200 chars (D-discretion). Composition cap 20000 chars. |
| Response: same envelope shape; `results` = groups (per sys_id) with `matches[]` of chunk hits | Skill iterates groups; one justification per group (D-11). |
| `mode` enum: `exact \| variants \| fuzzy` (NOT `search_mode` — 81A D-07 keeps name as-is for parallels) | Skill must use `mode` here, NOT `search_mode`. **This is a temporary stylistic inconsistency** with `/api/search.search_mode` documented in Phase 82 (DOC-01). |
| `truncated_to_200` warning when group count > 200 | Skill surfaces in summary. |

### Phase 81A — Contract Expansion (just landed 2026-05-04)

[VERIFIED via 81A-CONTEXT.md]

| Change | Skill impact |
|--------|--------------|
| `search_mode` enum: `exact \| variants \| responsa \| title \| shelfmark` (5 values, NO `regex`) | Skill scripts use these values exclusively. `shelfmark` mode powers SKILL-05 Tier-2 normalization. |
| `responsa_options: {variants: bool, ja: bool, flex_spacing: bool, bidirectional: bool}` | Skill exposes only when `search_mode='responsa'`. v7.10 skill workflow probably never sets these (default off); document as advanced. |
| `request` echo block in `/api/search` and `/api/parallels` envelopes | Skill can show "what server applied" — useful for debugging when responsa cascade silently disables an option. Compare `responsa_options` (sent) vs `responsa_options_effective` (server applied). |
| Hard cutover: old `mode` field rejected with 400 `invalid_request` "unknown field 'mode' — use search_mode instead" | Skill code MUST use `search_mode`. No backward compatibility. |
| `limit` ceiling lowered 200 → 100 | Skill default top-N is 10 (D-06); ceiling not relevant. |

### Existing Codebase Dependencies (consumed, not modified)

| File | Skill use |
|------|-----------|
| `shared/search_serializer.py` | **Reference only** — skill reproduces the envelope-parsing logic in Python. Skill does NOT import from this module (D-02: skill is external). |
| `shared/api_errors.py` | **Reference only** — skill hard-codes the error code → plain-text mapping. |
| `web/search_api.py` | **Reference only** — skill targets the live HTTP surface, not the Python module. |

### Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Python 3 | All scripts | ✓ | 3.11.9 (this machine); skill needs ≥ 3.10 per CLAUDE.md | — |
| `requests` (Python) | HTTP transport | ✓ | 2.32.5 (this machine); pre-installed in Claude API code-execution sandbox per Anthropic docs | `urllib.request` (stdlib) |
| `httpx` (Python) | Async HTTP (alt) | ✓ | 0.28.1 | Not needed if `requests` chosen |
| Claude Code CLI | Skill execution surface | ✓ (assumed; D-12 acceptance run requires it) | latest | — |
| Internet access to `genizahsearch.com` | All endpoints | ✓ on Claude Code/Desktop; **✗ on Claude API** | — | None — skill explicitly does not work on Claude API surface in v7.10 |

**Missing dependencies with no fallback:**
- Claude API surface (no network) — documented limitation; do not target.

**Missing dependencies with fallback:**
- None blocking.

## Risks & Landmines

### R1: Cross-surface portability claim is false on Claude API (HIGH)
The CONTEXT D-01 says "Portable across Claude Code, Claude Desktop, and claude.ai." [VERIFIED] — but the Claude API surface has **no network access** for skill code-execution containers. The skill cannot reach `genizahsearch.com` from API surface in v7.10.

**Mitigation:** Document explicitly in SKILL.md and README. v7.10 acceptance run targets Claude Code (or Desktop with code execution + network). v7.11 candidate: Anthropic egress allowlist OR a server-side proxy at a different domain.

### R2: text_source enum mismatch with SKILL-04 (HIGH)
SKILL-04 says `text_source != 'full'`; Phase 79 D-10 enum is `pgp_transcription | snippet | none`. **No `'full'` value exists.**

**Mitigation:** Skill treats `text_source != 'pgp_transcription'` as the "needs honesty annotation" trigger. Planner should either (a) update REQUIREMENTS.md SKILL-04 to use the actual enum, or (b) note the mapping in PLAN.md.

### R3: Throttle state race conditions across parallel script invocations (MEDIUM)
If the model invokes multiple `python scripts/search.py` calls in parallel (rare but possible — Bash tool can run commands in parallel), the JSON state file race-conditions on read-modify-write.

**Mitigation:** File locking via `fcntl.flock` (Unix) / `msvcrt.locking` (Windows). Cross-platform helper in `scripts/_lock.py`. Document the constraint: skill scripts are designed for sequential invocation; parallel use degrades silently to "approximately rate-limited."

### R4: ${CLAUDE_SKILL_DIR} only exists in Claude Code (MEDIUM)
The string-substitution variable `${CLAUDE_SKILL_DIR}` is a Claude-Code-specific extension [VERIFIED: code.claude.com docs] — not part of the open agentskills.io standard.

**Mitigation:** Scripts resolve their own directory via `Path(__file__).parent.parent` instead of relying on `${CLAUDE_SKILL_DIR}`. SKILL.md instructions reference `${CLAUDE_SKILL_DIR}` for Claude Code where it works, but scripts don't depend on it.

### R5: Server-side rate-limit drift (MEDIUM)
Server caps at 30 req/min per bucket (Phase 78/79/80). Skill defaults to 24 — 6 req/min headroom. If server drops to 20 (SRE response to abuse), skill will get 429s.

**Mitigation:** SKILL-06 already has `GENIZAH_SKILL_REQ_PER_MIN` env override. Document: "if you see persistent 429s, lower this." On 429, skill notes inline + moves on (no retry per D-08); summary line counts 429s so user sees pattern.

### R6: Base-URL portability — env wins is non-standard (LOW)
D-09 reverses the typical CLI convention (CLI > env). A user who sets `GENIZAH_API_BASE` once and forgets, then passes `--base-url` thinking it overrides, will be surprised.

**Mitigation:** Loud warning in `--help` output and SKILL.md. Optionally: print "USING GENIZAH_API_BASE=... (CLI flag --base-url ignored)" on stderr when both are set.

### R7: Phase gate is unfakeable — no automated CI (LOW)
D-12 requires user-observed live run. If the user is unavailable, the phase blocks indefinitely.

**Mitigation:** Bundle `scripts/smoke_test.py` so the planner can sanity-check the skill before scheduling the live run. The smoke test is not a substitute for the live run but de-risks it.

### R8: Skill must not import `genizah_core` (per SKILL-05) (LOW)
Easy mistake during development to `from shared.search_serializer import ...`.

**Mitigation:** Skill repo is external (D-02); no import path to `genizah_core` exists. Smoke test asserts the skill works from a clean checkout with only `requests`/`httpx` installed.

### R9: Justification cites browse text — risk of hallucinated grounding (MEDIUM)
The model writes 1–2 sentence justifications "grounded in browse text." If the browse text is empty (`text_source: 'none'`) or just a snippet, the model may extrapolate beyond what's there.

**Mitigation:** SKILL.md explicitly instructs: "if `text_source != 'pgp_transcription'`, justification MUST be solely about the snippet's match — never invent context." Honesty annotation is the safety net; phrasing reminds the human reviewer the evidence is partial.

## Open Questions for Planner

1. **HTTP client choice (planner discretion per D-claude).** `requests` (sync) is simpler and pre-installed in Claude API sandbox. `httpx` allows async fan-out for staged phrase discovery (parallel `/api/search` calls), saving ~3-5 sec on a 4-phrase staged query. **Recommendation:** `requests` for v7.10 — simpler debugging, throttle math is cleaner sequentially, the latency saving doesn't justify the complexity.

2. **Skill name (planner discretion).** Options: `cairo-genizah-research`, `genizah-search`, `genizah`. **Recommendation:** `cairo-genizah-research` — most descriptive in the skill description triggering text; matches "Cairo Genizah" terminology in REQUIREMENTS.md.

3. **Where does the external skill repo live?** D-02 says external; CONTEXT explicitly says "out of GenizahSearch's scope" but the planner needs a working location for development. **Recommendation:** Develop in a private gist or a dedicated `cairo-genizah-skill` repo under the user's GitHub. Symlink `~/.claude/skills/cairo-genizah-research/` to the dev checkout for live-edit testing per Claude Code's live change detection.

4. **REQUIREMENTS.md SKILL-04 text_source mismatch — fix it or accept the mapping?** See Risk R2. Planner should either patch REQUIREMENTS.md or add a one-line note in PLAN.md acknowledging the skill maps `pgp_transcription → "full"`.

5. **Bundle a fixture corpus for offline tests, or skip?** Wave 0 §"Test Map" lists `scripts/test_*.py` with fixtures. Recording 5–10 representative `/api/search`/`/api/browse`/`/api/parallels` responses adds ~1 hour of work but makes the skill testable without network. **Recommendation:** yes, bundle — it's the only way to unit-test SKILL-04/SKILL-05/SKILL-06 logic without a live deployment.

6. **Should `/api/parallels` always run alongside `/api/search`, or only when input shape suggests composition?** D-claude-discretion preferred "separate sub-mode invoked when input looks like a composition (multi-line text > 200 chars)." **Recommendation:** stick with the heuristic. A 200-char threshold is generous (one piyyut stanza ~150 chars; a search query rarely > 200). Document the threshold; let user override via `--mode parallels` flag.

7. **Output format default (D-claude-discretion: Markdown vs JSON).** **Recommendation:** Markdown by default; `--json` flag for programmatic consumers. Markdown matches D-claude preference and renders cleanly in Claude Code's terminal output.

8. **What's the ranking output schema?** SC-2 spec lists: shelfmark, library, catalog title, tier (A/B/C), known-witness flag, matching phrases, justification grounded in browse text, browse URL, image URL or "(no image available)". **Recommendation:** plan a Markdown template now (in `scripts/format_output.py`) so all required fields surface explicitly.

## Sources

### Primary (HIGH confidence)
- [Anthropic platform docs — Agent Skills Overview](https://platform.claude.com/docs/en/agents-and-tools/agent-skills/overview) — cross-surface canonical (frontmatter schema, runtime constraints per surface, network access matrix, sharing scope).
- [Claude Code skills docs](https://code.claude.com/docs/en/skills) — full frontmatter reference (name, description, allowed-tools, disable-model-invocation, etc.), filesystem locations, live change detection, string substitutions including `${CLAUDE_SKILL_DIR}`.
- [agentskills.io — open standard](https://agentskills.io) — cross-vendor portability surface; confirms minimal SKILL.md schema (name + description).
- [anthropics/skills GitHub](https://github.com/anthropics/skills) — example skills directory structure (skills/skill-creator, skills/docx, skills/pdf).

### Secondary (MEDIUM confidence)
- Phase 77 CONTEXT.md (D-04 locator both-fields-always-populated; D-13 matches[] per uid) — VERIFIED via local file.
- Phase 78 CONTEXT.md (rate limit, error envelope, statelessness) — VERIFIED via local file.
- Phase 79 CONTEXT.md (text_source enum, image best-effort, browse drill-down contract) — VERIFIED via local file.
- Phase 80 CONTEXT.md (parallels mode enum, group cap, locator round-trip) — VERIFIED via local file.
- Phase 81A CONTEXT.md (search_mode enum, request echo block, regex dropped) — VERIFIED via local file.

### Tertiary (LOW confidence — flagged for validation)
- [ASSUMED] Throttle JSON state across script invocations: filesystem locking is the right pattern. No public Anthropic guidance on rate-limit-respecting Python skills found in this research session — based on standard token-bucket implementation knowledge.
- [ASSUMED] `scripts/` subdir is the convention for bundled executable code — verified by structural inspection of [anthropics/skills repo](https://github.com/anthropics/skills) but not formally specified in spec docs.
- [ASSUMED] The model invokes scripts sequentially by default in skill execution. Anthropic docs describe "Claude runs them via bash and receives only the output" without specifying parallelism. Risk R3 captures the implication.

## Metadata

**Confidence breakdown:**
- Skill format & SKILL.md schema: HIGH — verified against three authoritative docs (platform.claude.com, code.claude.com, agentskills.io).
- API contract integration: HIGH — Phases 77/78/79/80/81A CONTEXT files all read; locator round-trip and envelope shapes are locked.
- Cross-surface runtime constraints: HIGH — explicitly documented in platform.claude.com.
- Throttle state pattern: MEDIUM — standard token-bucket math is straightforward, but cross-surface persistence + cross-process locking is [ASSUMED] from general systems knowledge, not validated against an Anthropic example.
- Test/validation strategy: MEDIUM — no precedent for "skill external to repo with smoke tests" in this codebase; pattern is reasonable but the planner may refine.

**Research date:** 2026-05-04
**Valid until:** 2026-06-03 (30 days — Skill format spec is stable but Anthropic occasionally adds frontmatter fields)

## RESEARCH COMPLETE
