"""Regression: SearchEngine._normalize_text must not depend on a
`self.lab_index_normalize` attribute.

`lab_index_normalize` is a @staticmethod on LabEngine, NOT on SearchEngine.
`SearchEngine.rebuild_local_lab_index` wires `normalize_text_fn=self._normalize_text`,
and `build_lab_side_index` runs that callback in a pre-flight probe. The old
`return self.lab_index_normalize(content)` raised AttributeError on every
SearchEngine instance, aborting every LOCAL LAB rebuild before the writer
opened — so `.meta.json` was never written, the index stayed perpetually
"stale", and a doomed rebuild was re-attempted on every startup/refresh
(console churn + wasted reloads, part of the v7.16 startup churn).

The existing build_lab_side_index tests pass a LOCAL stub normalize_fn
(tests/test_local_lab_invalidation.py), so they never exercised this real
wiring — which is exactly why the bug shipped. This closes that gap.
"""
from genizah_core import SearchEngine, LabEngine


class _NoLabNormalize:
    """Stand-in with NO `lab_index_normalize` attribute — i.e. exactly what a
    SearchEngine instance is. If _normalize_text reverts to
    `self.lab_index_normalize`, this raises AttributeError and the test fails."""


def test_normalize_text_does_not_require_self_lab_index_normalize():
    text = "Foo_Bar שלום! 123 ,;:"
    # Mirrors build_lab_side_index's pre-flight probe: the callback must run
    # without an AttributeError and produce LabEngine's normalization.
    result = SearchEngine._normalize_text(_NoLabNormalize(), text)
    assert result == LabEngine.lab_index_normalize(text)
    # Sanity: normalization actually does something (lowercase, strip punct).
    assert result == "foo bar שלום 123 "
