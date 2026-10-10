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


@pytest.mark.parametrize('field,value', [('displayName', 'G' * 31), ('shortDescription', 'S' * 31),
                                         ('supportURL', 'http://genizahsearch.com/support')])
def test_a_manifest_over_a_documented_limit_is_reported(tmp_path, monkeypatch, field, value):
    plugin = tmp_path / 'plugin'
    for part in ('openai', 'claude', 'gemini'):
        source = packages.PLUGIN / part
        for file in source.rglob('*'):
            if file.is_file():
                target = plugin / part / file.relative_to(source)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(file.read_bytes())
    manifest_path = plugin / 'openai' / 'plugin.json'
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    manifest['extensions']['com.openai']['interface'][field] = value
    manifest_path.write_text(json.dumps(manifest), encoding='utf-8')
    monkeypatch.setattr(packages, 'PLUGIN', plugin)
    monkeypatch.setattr(packages, 'GENERATED', {})
    assert any(field in problem for problem in packages.problems())
