"""Regression for the Codex finding in round 14 on PR #333 (2026-09-02).

P2 `web/pages/browse_enrichment.py` — the attribution block reads
`attribution_nli` from `nli_cache` BEFORE `resolve_external_images()` triggers
`enrich_metadata()`, which is what populates that field. On a first, uncached
browse load the value was therefore empty, so the Oxford→NLI fallback showed the
generic credit instead of the manifest's own. The value is now re-read after
enrichment, next to the `external_url` re-read that solves the same ordering
problem.
"""
from __future__ import annotations

import ast


SRC = "web/pages/browse_enrichment.py"


def _read(path):
    return open(path, encoding="utf-8").read()


def _sync_body():
    src = _read(SRC)
    fn = next(n for n in ast.walk(ast.parse(src))
              if isinstance(n, ast.FunctionDef) and n.name == "_browse_enrich_sync")
    return ast.get_source_segment(src, fn)


class TestNliCreditIsRereadAfterEnrichment:
    def test_the_reread_exists(self):
        body = _sync_body()
        assert "result['attribution_nli'] = _cached_attr['attribution_nli']" in body

    def test_it_runs_after_the_cache_is_populated(self):
        body = _sync_body()
        first_read = body.index("nli_attribution = cached_meta.get('attribution_nli'")
        enrich = body.index("resolve_external_images(")
        reread = body.index("result['attribution_nli'] = _cached_attr")
        assert first_read < enrich < reread, (
            "the re-read must come after the call that populates the cache"
        )

    def test_it_does_not_overwrite_a_value_already_found(self):
        body = _sync_body()
        i = body.index("if not result.get('attribution_nli')")
        assert i > 0, "the re-read must be conditional on the field still being empty"

    def test_it_sits_with_the_external_url_reread(self):
        # Same ordering problem, same place -- so the next reader finds both.
        body = _sync_body()
        url_reread = body.index("result['external_url'] = cached_url")
        attr_reread = body.index("result['attribution_nli'] = _cached_attr")
        assert 0 < attr_reread - url_reread < 1200

    def test_the_page_still_receives_the_field(self):
        src = _read(SRC)
        assert "pg.attribution_nli = browse_enrich['attribution_nli']" in src
