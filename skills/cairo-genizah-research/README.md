# Cairo Genizah Research

Research Cairo Genizah manuscripts with an AI assistant: search phrases, read
pages, and find letter-level parallels through the public GenizahSearch API.
No desktop search application or local corpus is required.

## Requirements

Python 3.10+, the `requests` package (`python -m pip install requests`), and an
assistant that can execute scripts and access `https://genizahsearch.com`.
The public API currently requires no key; your AI provider's usage limits apply.
Network availability depends on the runtime and organization.

## Claude web or Desktop

1. Obtain a ZIP containing the complete `cairo-genizah-research` folder, including
   SKILL.md, scripts, and references. Uploading SKILL.md alone is insufficient.
2. Enable **Code execution and file creation** in **Settings → Capabilities**.
3. Open **Customize → Skills → + → Create skill → Upload a skill**, upload the
   ZIP, and enable the skill.
4. Ask Claude to run the smoke test below. Installation alone does not establish
   that the runtime can contact `genizahsearch.com`.

Organization settings may control these options. See the current
[Claude instructions](https://support.claude.com/en/articles/12512180-use-skills-in-claude).
The upload route is documented by Claude; test this specific skill in your account.

## Claude Code

Copy the complete folder to `~/.claude/skills/cairo-genizah-research/` for personal
use, or `.claude/skills/cairo-genizah-research/` inside a project. On Windows `~`
means your user profile directory. Ask the assistant to read SKILL.md and run a test.

## Codex and other agents

Give the agent the [skill directory](https://github.com/gershuni/GenizahSearch/tree/master-main/skills/cairo-genizah-research)
and ask its skill installer to install that subdirectory. Alternatively, give it
the extracted folder and explicitly ask it to read SKILL.md and run its scripts.
The scripts do not depend on Claude-specific tools or environment variables.
See [Codex skill documentation](https://developers.openai.com/codex/skills/).

## First questions

> השתמש בסקיל cairo-genizah-research כדי למצוא מקורות בגניזה העוסקים בתוספות לתפילה בעשרת ימי תשובה. הצג סימני מדף וקישורים והסבר על מה מבוסס הזיהוי.

> מצא מקבילות לקטע המצורף בעזרת חיפוש האותיות. הבחן בין עד נוסף לאותו חיבור לבין נוסח תפילה משותף.

> בדוק את הקטע בתצלום והסבר אילו קריאות שינית לעומת התעתיק האוטומטי.

## Smoke test

From the installed skill directory:

```bash
python scripts/search.py --query "זכרינו לחיים" --search-mode exact --limit 1
python scripts/parallels.py --method passage --text-file passage.txt
```

Supply your own UTF-8 text in passage.txt. Success returns `results` and `warnings`;
a structured `error` means the search did not complete. Passage search never
silently falls back to word chunks. The legacy method is available explicitly
with `--method chunk` (also the script's backward-compatible default).

For several witnesses of one work, create a JSON array:

```json
[
  {"label": "Witness A", "text": "First witness text"},
  {"label": "Witness B", "text": "Second witness text"}
]
```

```bash
python scripts/parallels.py --method passage --witnesses-file witnesses.json --sort fused
```

## Sharing a ZIP

Run the bundled packager; it excludes caches and local throttle state:

```bash
python scripts/package_skill.py --output cairo-genizah-research.zip
```

Distribute the ZIP with these instructions. Creating it does not publish it online.

## Configuration and research limits

`GENIZAH_API_BASE` overrides the API URL, including `--base-url`.
`CAIRO_GENIZAH_STATE_DIR` can place throttle state in a writable directory if the
installed folder is read-only. Other options are documented in SKILL.md.

Automatic text can contain errors. Check significant readings against photographs.
Catalog descriptions do not prove authorship or publication. Truncation warnings
mean results are not exhaustive. See [API contract](references/api_contract.md)
and [public API docs](https://genizahsearch.com/api/docs).
