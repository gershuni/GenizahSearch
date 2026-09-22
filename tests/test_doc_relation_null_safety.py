# -*- coding: utf-8 -*-
"""``documents.doc_relation`` is NULL for most documents, and ``.get(k, '')`` will not save you.

Until 2026-09-22 ``scripts/export_pgp_sidecar.py`` silently dropped this column, so the
key was ABSENT from every PGP document dict and ``pgp_doc.get('doc_relation', '')``
returned ``''``. Carrying the column fixed a real misclassification -- and turned that
same expression into a crash, because the default fires only on a MISSING KEY, never on a
present ``None``::

    >>> 'Edition' in {'doc_relation': None}.get('doc_relation', '')
    TypeError: argument of type 'NoneType' is not iterable

In the live corpus 28,896 of 35,986 documents (80%) have SQL NULL there, so this would
have taken out Browse enrichment for the large majority of PGP documents. Found by an
independent review, not by the suite -- hence this file.

The distinction that matters: ``document_sources.doc_relation`` is ``TEXT NOT NULL``, so
the ``.get(k, '')`` form is safe on SOURCE rows (``web/pages/browse.py``,
``genizah_app.py``). It is only the DOCUMENT rows that can be NULL.
"""
from __future__ import annotations

import pathlib

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

# Sites that read doc_relation off a PGP *document* dict, where NULL is the common case.
DOCUMENT_SIDE_READERS = (
    "web/pages/browse_enrichment.py",
    "web/pages/search_results.py",
)

UNSAFE = "get('doc_relation', '')"
SAFE = "get('doc_relation') or ''"


def test_the_expression_itself_is_the_trap():
    """Pin the language behaviour the bug turned on, so the reason is never re-litigated."""
    present_but_null = {"doc_relation": None}
    with pytest.raises(TypeError):
        "Edition" in present_but_null.get("doc_relation", "")

    # The form the code must use:
    assert ("Edition" in (present_but_null.get("doc_relation") or "")) is False

    # ...and it must still behave for a genuinely missing key and a real value.
    assert ("Edition" in ({}.get("doc_relation") or "")) is False
    assert ("Edition" in ({"doc_relation": "Digital Edition"}.get("doc_relation") or "")) is True


@pytest.mark.parametrize("relative", DOCUMENT_SIDE_READERS)
def test_document_side_readers_tolerate_null(relative):
    path = REPO_ROOT / relative
    source = path.read_text(encoding="utf-8", errors="replace")

    assert UNSAFE not in source, (
        "%s reads documents.doc_relation with a default that only fires on a missing "
        "key. That column is NULL for ~80%% of documents, so this raises TypeError. "
        "Use %r." % (relative, SAFE)
    )
    assert SAFE in source, (
        "%s should still read doc_relation defensively" % relative
    )
