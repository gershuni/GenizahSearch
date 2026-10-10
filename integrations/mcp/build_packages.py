"""Build and check the GenizahSearch MCP distribution packages.

    python integrations/mcp/build_packages.py --check          # what tests run
    python integrations/mcp/build_packages.py --write          # regenerate the skill files
    python integrations/mcp/build_packages.py --zip dist/genizahsearch-openai-plugin.zip

One source of truth: integrations/mcp/server_instructions.md, which the MCP server itself
sends. The Claude plugin's SKILL.md and the Gemini extension's GEMINI.md are generated
from it (and committed, because both are published straight from a repository); the
OpenAI plugin's skill is generated into its ZIP. --check fails when any generated file
is stale or any manifest breaks a directory's documented limit (formats checked
2026-10-10 against developers.openai.com/plugins, claude.com/docs/plugins and
geminicli.com/docs/extensions).
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HERE = ROOT / 'integrations' / 'mcp'
PLUGIN = HERE / 'plugin'
INSTRUCTIONS = HERE / 'server_instructions.md'
ICON = ROOT / 'web' / 'static' / 'genizahsearch-icon-512.png'
MCP_URL = 'https://genizahsearch.com/mcp'
SKILL_NAME = 'genizahsearch-research'
SKILL_DESCRIPTION = (
    'Research Cairo Genizah manuscripts with the GenizahSearch tools: search a Hebrew or Judeo-Arabic '
    'phrase or a shelfmark, find parallels to a passage, read manuscript pages with citations, and '
    'read catalogue records and bibliography. Use when the user asks about Cairo Genizah manuscripts, '
    'shelfmarks (T-S, ENA, JTS, Mosseri, Halper), or parallels to a piyyut, responsum, letter or '
    'composition.')
CLAUDE_SKILL = PLUGIN / 'claude' / 'skills' / SKILL_NAME / 'SKILL.md'
GEMINI_CONTEXT = PLUGIN / 'gemini' / 'GEMINI.md'


def skill_text() -> str:
    body = INSTRUCTIONS.read_text(encoding='utf-8').strip()
    # Quoted: the description contains ': ', which a plain YAML scalar cannot, and a
    # directory refuses unparseable frontmatter. A JSON string is a valid YAML string.
    return f'---\nname: {SKILL_NAME}\ndescription: {json.dumps(SKILL_DESCRIPTION)}\n---\n\n{body}\n'


def frontmatter(text: str) -> dict:
    import yaml
    head = re.match(r'---\n(.*?)\n---\n', text, re.S)
    return yaml.safe_load(head.group(1)) if head else {}


def gemini_text() -> str:
    return '# GenizahSearch\n\n' + INSTRUCTIONS.read_text(encoding='utf-8').strip() + '\n'


GENERATED = {CLAUDE_SKILL: skill_text, GEMINI_CONTEXT: gemini_text}


def _json(path: Path) -> dict:
    return json.loads(path.read_text(encoding='utf-8'))


def problems() -> list[str]:
    found = []
    for path, build in GENERATED.items():
        if not path.exists() or path.read_text(encoding='utf-8') != build():
            found.append(f'{path.relative_to(ROOT)} is stale: run --write')
    try:
        meta = frontmatter(skill_text())
    except Exception as exc:  # any YAML error is a refused submission
        meta = {'error': str(exc)}
    if meta.get('name') != SKILL_NAME or meta.get('description') != SKILL_DESCRIPTION:
        found.append(f'SKILL.md frontmatter does not parse to name and description: {meta}')

    openai = _json(PLUGIN / 'openai' / 'plugin.json')
    interface = openai['extensions']['com.openai']['interface']
    if not re.fullmatch(r'[a-z0-9]+(-[a-z0-9]+)*', openai['name']) or len(openai['name']) > 64:
        found.append('openai plugin.json: name must be kebab-case, at most 64 characters')
    for key, limit in (('displayName', 30), ('shortDescription', 30), ('longDescription', 4000)):
        if not interface.get(key) or len(interface[key]) > limit:
            found.append(f'openai plugin.json: {key} missing or over {limit} characters')
    for key in ('websiteURL', 'supportURL', 'privacyPolicyURL', 'termsOfServiceURL'):
        value = interface.get(key, '')
        if not value.startswith('https://') or len(value) > 1024:
            found.append(f'openai plugin.json: {key} must be an https URL of at most 1024 characters')
    servers = _json(PLUGIN / 'openai' / 'mcp.json')['mcpServers']
    if len(servers) != 1 or [s.get('url') for s in servers.values()] != [MCP_URL] or \
            any(s.get('headers') for s in servers.values()):
        found.append('openai mcp.json: exactly one server, at the MCP URL, without headers')

    claude = _json(PLUGIN / 'claude' / '.claude-plugin' / 'plugin.json')
    for key in ('name', 'description', 'license', 'supportUrl', 'privacyPolicyUrl', 'termsOfServiceUrl'):
        if not claude.get(key):
            found.append(f'claude plugin.json: {key} missing')
    claude_servers = _json(PLUGIN / 'claude' / '.mcp.json')['mcpServers']
    if [(s.get('type'), s.get('url')) for s in claude_servers.values()] != [('http', MCP_URL)]:
        found.append('claude .mcp.json: one http server at the MCP URL')
    readme = re.sub(r'```.*?```', '', (PLUGIN / 'claude' / 'README.md').read_text(encoding='utf-8'), flags=re.S)
    if len(readme.split()) < 40:
        found.append('claude README.md: at least 40 words outside code blocks')

    gemini = _json(PLUGIN / 'gemini' / 'gemini-extension.json')
    if [s.get('httpUrl') for s in gemini['mcpServers'].values()] != [MCP_URL] or \
            gemini.get('contextFileName') != 'GEMINI.md':
        found.append('gemini-extension.json: httpUrl at the MCP URL, contextFileName GEMINI.md')

    from PIL import Image
    with Image.open(ICON) as icon:
        if icon.format not in ('PNG', 'JPEG', 'WEBP') or icon.width != icon.height or icon.width < 48 \
                or ICON.stat().st_size > 5 * 1024 * 1024:
            found.append('icon: a square PNG/JPEG/WebP of at least 48 px and at most 5 MiB')
    return found


def write() -> None:
    for path, build in GENERATED.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(build(), encoding='utf-8', newline='\n')


def build_zip(target: Path) -> list[str]:
    target.parent.mkdir(parents=True, exist_ok=True)
    entries = {
        'plugin.json': (PLUGIN / 'openai' / 'plugin.json').read_bytes(),
        'mcp.json': (PLUGIN / 'openai' / 'mcp.json').read_bytes(),
        f'skills/{SKILL_NAME}/SKILL.md': skill_text().encode('utf-8'),
        'assets/icon.png': ICON.read_bytes(),
    }
    with zipfile.ZipFile(target, 'w', zipfile.ZIP_DEFLATED) as archive:
        for name, data in entries.items():
            archive.writestr(name, data)
    return sorted(entries)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--write', action='store_true')
    parser.add_argument('--zip', type=Path)
    args = parser.parse_args()
    if args.write:
        write()
    found = problems()
    for problem in found:
        print('PROBLEM:', problem)
    if args.zip and not found:
        print('built', args.zip, build_zip(args.zip))
    if not found:
        print('packages OK')
    return 1 if found else 0


if __name__ == '__main__':
    sys.exit(main())
