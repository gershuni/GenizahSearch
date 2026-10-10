"""Build and check the GenizahSearch MCP distribution packages.

    python integrations/mcp/build_packages.py --check          # what tests run
    python integrations/mcp/build_packages.py --write          # regenerate the skill files
    python integrations/mcp/build_packages.py --zip dist/genizahsearch-openai-plugin.zip
    python integrations/mcp/build_packages.py --export dist/genizahsearch-ai-plugins

One source of truth: integrations/mcp/server_instructions.md, which the MCP server itself
sends. The Claude plugin's SKILL.md and the Gemini extension's GEMINI.md are generated
from it and committed; the OpenAI plugin's skill is generated into its ZIP; --export writes
the separate small repository the Gemini gallery and the Claude directory need.

--check enforces what is written in problems(), no more: stale generated files; the
OpenAI manifests against the transcribed Agent Plugins 1.0.0 schemas plus the submission
page's field limits; the Claude bundle's fields, remote server shape, README length and
per-file limits; the Gemini manifest's name, version, server and context file; the icon.
Formats checked 2026-10-10 against developers.openai.com/plugins, claude.com/docs/plugins
and geminicli.com/docs/extensions; the directories' own validators remain the authority.
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

# The Agent Plugins 1.0.0 schemas OpenAI's portable format validates against, transcribed
# 2026-10-10 from https://agent-plugins.org/schemas/1.0.0/{plugin,mcp}.schema.json (the
# constraints this package touches; both roots are closed). Codex caught a package that
# broke them -- no $schema, and a root `skills` key the closed root rejects.
PLUGIN_SCHEMA = {
    'type': 'object', 'required': ['$schema', 'name'], 'additionalProperties': False,
    'properties': {
        '$schema': {'const': 'https://agent-plugins.org/schemas/1.0.0/plugin.schema.json'},
        'name': {'type': 'string', 'minLength': 1, 'maxLength': 64,
                 'pattern': r'^(?!.*(?:--|\.\.))[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$'},
        'version': {'type': 'string'}, 'description': {'type': 'string'},
        'author': {'type': 'object', 'additionalProperties': False,
                   'properties': {'name': {'type': 'string'}, 'email': {'type': 'string'},
                                  'url': {'type': 'string'}}},
        'homepage': {'type': 'string'}, 'repository': {'type': 'string'}, 'license': {'type': 'string'},
        'keywords': {'type': 'array', 'items': {'type': 'string'}},
        'extensions': {'type': 'object', 'additionalProperties': {'type': 'object'}},
    },
}
_REMOTE_SERVER = {
    'type': 'object', 'required': ['type', 'url'], 'additionalProperties': False,
    'properties': {'type': {'enum': ['streamable-http', 'sse']}, 'url': {'type': 'string', 'minLength': 1},
                   'headers': {'type': 'object', 'additionalProperties': {'type': 'string'}}},
}
MCP_SCHEMA = {
    'type': 'object', 'required': ['$schema', 'mcpServers'], 'additionalProperties': False,
    'properties': {'$schema': {'const': 'https://agent-plugins.org/schemas/1.0.0/mcp.schema.json'},
                   'mcpServers': {'type': 'object', 'additionalProperties': _REMOTE_SERVER}},
}


def _schema_problems(document: dict, schema: dict, label: str) -> list[str]:
    import jsonschema
    validator = jsonschema.Draft202012Validator(schema)
    return [f'{label}: {error.json_path}: {error.message}' for error in validator.iter_errors(document)]


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
    found += _schema_problems(openai, PLUGIN_SCHEMA, 'openai plugin.json')
    found += _schema_problems(_json(PLUGIN / 'openai' / 'mcp.json'), MCP_SCHEMA, 'openai mcp.json')
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
    if not re.fullmatch(r'[a-z0-9]+(-[a-z0-9]+)*', gemini.get('name', '')) or not gemini.get('version'):
        found.append('gemini-extension.json: a lowercase dashed name and a version are required')

    # The Claude directory validates the WHOLE repository (under 50 MiB archived, 256 MiB
    # unpacked, fewer than 10,000 files), so the bundle is published from its own small
    # repository (export_distribution); here, the per-file limits it holds for a reviewer.
    for file in (PLUGIN / 'claude').rglob('*'):
        if file.is_file() and (file.stat().st_size > 256 * 1024 or file.suffix not in ('.json', '.md')):
            found.append(f'claude bundle: {file.relative_to(PLUGIN)} must be a text file under 256 KiB')

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


DISTRIBUTION_README = """# GenizahSearch AI plugins

Connect AI assistants to Dicta Genizah Search, a free research platform for the Cairo Genizah
manuscripts at https://genizahsearch.com, through its public MCP server at
https://genizahsearch.com/mcp (no account needed).

- The repository root is a Gemini CLI extension (`gemini-extension.json`, `GEMINI.md`).
- `claude-plugin/` is the Claude plugin: the connector plus a research skill.

Generated from https://github.com/gershuni/GenizahSearch (`integrations/mcp/build_packages.py --export`);
change the files there, not here. Support: https://genizahsearch.com/support. Privacy:
https://genizahsearch.com/ai/privacy. Terms: https://genizahsearch.com/terms.
"""


def export_distribution(target: Path) -> list[str]:
    """Write the small public repository the Gemini gallery and the Claude directory read.

    Both need a repository of their own: the Gemini gallery reads gemini-extension.json
    only at a repository ROOT, and the Claude directory validates the whole repository
    (under 50 MiB archived, 256 MiB unpacked), which GenizahSearch far exceeds.
    """
    files = {
        'README.md': DISTRIBUTION_README.encode('utf-8'),
        'LICENSE': (ROOT / 'LICENSE').read_bytes(),
        'gemini-extension.json': (PLUGIN / 'gemini' / 'gemini-extension.json').read_bytes(),
        'GEMINI.md': gemini_text().encode('utf-8'),
    }
    for file in (PLUGIN / 'claude').rglob('*'):
        if file.is_file():
            files['claude-plugin/' + file.relative_to(PLUGIN / 'claude').as_posix()] = file.read_bytes()
    for name, data in files.items():
        path = target / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    return sorted(files)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--write', action='store_true')
    parser.add_argument('--zip', type=Path)
    parser.add_argument('--export', type=Path, help='write the separate public distribution repository here')
    args = parser.parse_args()
    if args.write:
        write()
    found = problems()
    for problem in found:
        print('PROBLEM:', problem)
    if args.zip and not found:
        print('built', args.zip, build_zip(args.zip))
    if args.export and not found:
        print('exported', args.export, export_distribution(args.export))
    if not found:
        print('packages OK')
    return 1 if found else 0


if __name__ == '__main__':
    sys.exit(main())
