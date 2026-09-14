"""Memory failures and container limits must not silently degrade research."""
from unittest.mock import Mock

import pytest

from shared.research_limits import _cgroup_available_memory


@pytest.mark.parametrize('version', [1, 2])
@pytest.mark.parametrize('parent_limited', [False, True])
def test_nested_cgroup_limits_include_controller_mount_and_ancestors(tmp_path, version, parent_limited):
    root = tmp_path / 'cgroup'
    mount = root / 'memory' if version == 1 else root
    container = mount / 'docker' / 'test-container'
    container.mkdir(parents=True)
    limit, used = ('memory.limit_in_bytes', 'memory.usage_in_bytes') if version == 1 else ('memory.max', 'memory.current')
    (container / limit).write_text('768')
    (container / used).write_text('256')
    if parent_limited:
        (container.parent / limit).write_text('512')
        (container.parent / used).write_text('384')
    membership = tmp_path / 'membership'
    membership.write_text(('5:memory:' if version == 1 else '0::') + '/docker/test-container\n')
    assert _cgroup_available_memory(10000, root, membership) == (128 if parent_limited else 512)
    assert _cgroup_available_memory(64, root, membership) == 64


@pytest.mark.parametrize('cache_name', ['CACHE_NLI', 'CACHE_META'])
def test_metadata_cache_memory_failure_propagates(tmp_path, monkeypatch, cache_name):
    from shared import metadata_manager as module
    manager = module.MetadataManager.__new__(module.MetadataManager)
    for key in ['CACHE_NLI', 'CACHE_META']:
        monkeypatch.setattr(module.Config, key, str(tmp_path / key))
    (tmp_path / cache_name).write_bytes(b'cache')
    monkeypatch.setattr(module.pickle, 'load', Mock(side_effect=MemoryError('allocation failed')))
    with pytest.raises(MemoryError):
        manager._load_small_caches()


@pytest.mark.parametrize('failure_site', ['csv', 'alias', 'manifest'])
def test_heavy_metadata_memory_failure_propagates(tmp_path, monkeypatch, failure_site):
    import csv
    from shared import metadata_manager as module
    from shared import shelfmark_bridge
    manager = module.MetadataManager.__new__(module.MetadataManager)
    manager.csv_bank = {}
    manager.codico_mgr = Mock()
    source = tmp_path / 'libraries.csv'
    source.write_text('id,part,shelf\n123,,T-S 1\n', encoding='utf-8')
    monkeypatch.setattr(module.Config, 'LIBRARIES_CSV', str(source))
    monkeypatch.setattr(shelfmark_bridge, 'build_alias_index', Mock())
    if failure_site == 'csv':
        def rows(*args, **kwargs):
            yield ['id', 'part', 'shelf']
            yield ['123', '', 'T-S 1']
            raise MemoryError('CSV allocation failed')
        monkeypatch.setattr(csv, 'reader', rows)
    elif failure_site == 'alias':
        monkeypatch.setattr(shelfmark_bridge, 'build_alias_index', Mock(side_effect=MemoryError('alias allocation failed')))
    else:
        manifest = tmp_path / 'fist_data' / 'vs_manifest.txt'
        manifest.parent.mkdir()
        manifest.write_text('123\n')
        import builtins
        real_open = builtins.open

        def open_file(path, *args, **kwargs):
            if str(path) == str(manifest):
                raise MemoryError('manifest allocation failed')
            return real_open(path, *args, **kwargs)
        monkeypatch.setattr(builtins, 'open', open_file)
    with pytest.raises(MemoryError):
        manager._load_heavy_caches_bg()
    manager.codico_mgr.load.assert_not_called()


def test_codicological_memory_failure_propagates(tmp_path, monkeypatch):
    from shared import codicological
    source = tmp_path / 'oxford.json'
    source.write_text('{}')
    monkeypatch.setattr(codicological.Config, 'OXFORD_DB', str(source))
    monkeypatch.setattr(codicological.json, 'load', Mock(side_effect=MemoryError('allocation failed')))
    with pytest.raises(MemoryError):
        codicological.CodicologicalManager().load()
