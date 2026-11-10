"""Distribution packages for the MCP server (build commit 8).

integrations/mcp/build_packages.py generates the Claude skill and the Gemini context from
server_instructions.md, checks every manifest against the directories' documented limits,
and builds the OpenAI plugin ZIP.
"""
from __future__ import annotations

import importlib.util
import json
import zipfile
from pathlib import Path

import pytest

import web.mcp_server as mcp_server

ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location('build_packages', ROOT / 'integrations' / 'mcp' / 'build_packages.py')
packages = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(packages)


def test_the_committed_packages_pass_every_check():
    assert packages.problems() == []


def test_every_package_points_at_the_server_this_app_serves():
    assert packages.MCP_URL == mcp_server.SITE_URL + mcp_server.MCP_PATH


def test_a_changed_instruction_makes_the_generated_files_stale(tmp_path, monkeypatch):
    changed = tmp_path / 'server_instructions.md'
    changed.write_text(packages.INSTRUCTIONS.read_text(encoding='utf-8') + '\nOne more rule.\n', encoding='utf-8')
    monkeypatch.setattr(packages, 'INSTRUCTIONS', changed)
    stale = [problem for problem in packages.problems() if 'is stale' in problem]
    assert len(stale) == 2


def test_the_committed_skill_frontmatter_parses():
    meta = packages.frontmatter(packages.CLAUDE_SKILL.read_text(encoding='utf-8'))
    assert meta == {'name': packages.SKILL_NAME, 'description': packages.SKILL_DESCRIPTION}


def test_the_openai_zip_holds_the_manifest_server_skill_and_icon(tmp_path):
    target = tmp_path / 'plugin.zip'
    assert packages.build_zip(target) == ['assets/icon.png', 'mcp.json', 'plugin.json',
                                          f'skills/{packages.SKILL_NAME}/SKILL.md']
    with zipfile.ZipFile(target) as archive:
        manifest = json.loads(archive.read('plugin.json'))
        interface = manifest['extensions']['com.openai']['interface']
        for path in (interface['composerIcon'], interface['logo']):
            assert path.lstrip('./') in archive.namelist()
        assert archive.read(f'skills/{packages.SKILL_NAME}/SKILL.md').decode('utf-8') == packages.skill_text()
        assert archive.read('assets/icon.png') == packages.ICON.read_bytes()


def copied_plugins(tmp_path, monkeypatch):
    plugin = tmp_path / 'plugin'
    for part in ('openai', 'claude', 'gemini'):
        source = packages.PLUGIN / part
        for file in source.rglob('*'):
            if file.is_file():
                target = plugin / part / file.relative_to(source)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(file.read_bytes())
    monkeypatch.setattr(packages, 'PLUGIN', plugin)
    monkeypatch.setattr(packages, 'GENERATED', {})
    return plugin


def edit_json(path, change):
    document = json.loads(path.read_text(encoding='utf-8'))
    change(document)
    path.write_text(json.dumps(document), encoding='utf-8')


@pytest.mark.parametrize('field,value', [('displayName', 'G' * 31), ('shortDescription', 'S' * 31),
                                         ('supportURL', 'http://genizahsearch.com/support'),
                                         ('category', 'Research')])  # refused by OpenAI's dashboard
def test_a_manifest_over_a_documented_limit_is_reported(tmp_path, monkeypatch, field, value):
    plugin = copied_plugins(tmp_path, monkeypatch)
    edit_json(plugin / 'openai' / 'plugin.json',
              lambda m: m['extensions']['com.openai']['interface'].__setitem__(field, value))
    assert any(field in problem for problem in packages.problems())


@pytest.mark.parametrize('file,change,expected', [
    # Codex (build 7-8 review): the package shipped without $schema and with a root `skills`.
    ('plugin.json', lambda m: m.pop('$schema'), "'$schema' is a required property"),
    ('plugin.json', lambda m: m.__setitem__('skills', './skills/'), "'skills' was unexpected"),
    ('plugin.json', lambda m: m['author'].__setitem__('phone', '1'), "'phone' was unexpected"),
    ('mcp.json', lambda m: m.pop('$schema'), "'$schema' is a required property"),
    ('mcp.json', lambda m: m['mcpServers']['genizahsearch'].__setitem__('type', 'http'), "'http' is not one of"),
], ids=['no-schema', 'root-skills', 'author-extra', 'mcp-no-schema', 'mcp-wrong-type'])
def test_the_openai_manifests_are_validated_against_the_agent_plugins_schemas(tmp_path, monkeypatch,
                                                                              file, change, expected):
    plugin = copied_plugins(tmp_path, monkeypatch)
    edit_json(plugin / 'openai' / file, change)
    assert any(expected in problem for problem in packages.problems()), packages.problems()


@pytest.mark.parametrize('key', ['name', 'version'])
def test_the_gemini_manifest_needs_a_name_and_version(tmp_path, monkeypatch, key):
    plugin = copied_plugins(tmp_path, monkeypatch)
    edit_json(plugin / 'gemini' / 'gemini-extension.json', lambda m: m.pop(key))
    assert any('name and a version are required' in problem for problem in packages.problems())


@pytest.mark.parametrize('name,data', [('logo.ico', b'\x00\x01'),
                                       ('payload.md', b'\x89PNG\r\n\x1a\n\x00\x00')])  # Codex: a binary named .md
def test_a_binary_file_in_the_claude_bundle_is_reported(tmp_path, monkeypatch, name, data):
    plugin = copied_plugins(tmp_path, monkeypatch)
    (plugin / 'claude' / name).write_bytes(data)
    assert any(name in problem for problem in packages.problems())


def test_the_distribution_repository_fits_both_directories(tmp_path):
    target = tmp_path / 'dist-repo'
    names = packages.export_distribution(target)
    assert {'gemini-extension.json', 'GEMINI.md', 'README.md', 'LICENSE',
            'claude-plugin/.claude-plugin/plugin.json', 'claude-plugin/.mcp.json', 'claude-plugin/README.md',
            f'claude-plugin/skills/{packages.SKILL_NAME}/SKILL.md'} == set(names)
    files = [path for path in target.rglob('*') if path.is_file()]
    # Claude: the whole repository under 256 MiB unpacked and 10,000 files (50 MiB archived
    # is implied); every plugin-folder file under 256 KiB. Gemini: the manifest at the root.
    assert sum(path.stat().st_size for path in files) < 256 * 1024 * 1024 and len(files) < 10_000
    assert all(path.stat().st_size < 256 * 1024 for path in (target / 'claude-plugin').rglob('*') if path.is_file())
    assert json.loads((target / 'gemini-extension.json').read_text(encoding='utf-8'))['contextFileName'] == 'GEMINI.md'
    assert (target / 'GEMINI.md').read_text(encoding='utf-8') == packages.gemini_text()
