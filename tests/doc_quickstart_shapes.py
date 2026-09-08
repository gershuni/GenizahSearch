# -*- coding: utf-8 -*-
"""SUPERSEDED by tests/doc_response_shapes.py -- kept only so a stale import
keeps working. Nothing in the tree imports this module any more.

Why it was replaced rather than extended: this helper only ever read the
`## Quick Start` section, and its selector took `blocks[-1]` -- the last fenced
json block in a subsection -- as "the response example". Both limits caused
real defects on 2026-09-08:

  * Quick-Start-only coverage meant three examples were checked and five were
    not. The unchecked endpoint REFERENCE examples went on promising a
    nonexistent `metadata` object on search items, `aggregate_score` on
    parallels items, flat `library_code`/`library_name` on browse, an `fl_id`
    key in the search/parallels locator, and `metadata.pgp`/`fjms`/`nli`
    sub-objects whose every key was wrong -- in the same file, a few hundred
    lines below the examples that were green.
  * `blocks[-1]` is a positional guess, not a check. It cannot fail loud: a
    second example or a trailing request body would silently redirect the
    comparison or stop it checking anything at all.

The replacement locates each example by an explicit ANCHOR, covers every
response example in the document for all four endpoints, and asserts that no
fenced json block is left unaccounted for.

`missing_from` here forwards to the new module (whose signature added a third
`which` argument, defaulted to 'quick_start' for exactly this reason), so any
caller written against the old name still gets a correct answer.

This file can be deleted; it is retained pending the owner's word rather than
removed unilaterally.
"""
from __future__ import annotations

from tests.doc_response_shapes import (  # noqa: F401
    documented_example,
    key_paths,
    missing_from,
)

__all__ = ['documented_example', 'key_paths', 'missing_from']
