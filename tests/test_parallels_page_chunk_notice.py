# -*- coding: utf-8 -*-
"""The /parallels page must show the engine's composition notices.

execute_parallels is a closure inside create_parallels_page and needs a live
NiceGUI client, so this pins the call site structurally: the loop that
notifies each notice must sit directly in the `if result_data:` block (so it
runs for chunk AND Lab results, including empty ones), not under the
letter-level branch or behind a non-empty-results check, and the export
metadata must carry the same notices.
"""
from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PAGE = REPO_ROOT / 'web' / 'pages' / 'parallels.py'


def _execute_parallels():
    tree = ast.parse(PAGE.read_text(encoding='utf-8'))
    return next(n for n in ast.walk(tree)
                if isinstance(n, ast.AsyncFunctionDef) and n.name == 'execute_parallels')


def _result_data_block(fn):
    blocks = [n for n in ast.walk(fn) if isinstance(n, ast.If)
              and isinstance(n.test, ast.Name) and n.test.id == 'result_data']
    assert len(blocks) == 1, f'expected one `if result_data:` block, found {len(blocks)}'
    return blocks[0]


def _mentions(node, text):
    return text in ast.unparse(node)


def test_notices_are_notified_for_every_chunk_and_lab_result():
    block = _result_data_block(_execute_parallels())
    loops = [s for s in block.body if isinstance(s, ast.For)
             and _mentions(s.iter, "'composition_notices'")]
    assert loops, ('execute_parallels does not loop over composition_notices '
                   'directly inside `if result_data:`')
    loop = loops[0]
    assert any(isinstance(n, ast.Call) and _mentions(n.func, 'chunk_notice_message')
               for n in ast.walk(loop))
    assert any(isinstance(n, ast.Call) and _mentions(n.func, 'ui.notify')
               for n in ast.walk(loop))
    # Nothing before it in the block may return.
    before = block.body[:block.body.index(loop)]
    assert not any(isinstance(n, ast.Return) for s in before for n in ast.walk(s))


def test_export_metadata_carries_the_notices():
    block = _result_data_block(_execute_parallels())
    dicts = [n for n in ast.walk(block) if isinstance(n, ast.Dict)
             and any(isinstance(k, ast.Constant) and k.value == 'warnings' for k in n.keys)
             and any(isinstance(k, ast.Constant) and k.value == 'chunk_size' for k in n.keys)]
    assert dicts, 'export metadata dict not found'
    value = dicts[0].values[[getattr(k, 'value', None) for k in dicts[0].keys].index('warnings')]
    assert _mentions(value, 'composition_notices'), ast.unparse(value)
