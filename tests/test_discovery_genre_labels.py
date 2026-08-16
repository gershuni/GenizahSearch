# -*- coding: utf-8 -*-
"""Hebrew display labels for the findings page's domain facet (2026-08-04).

THE DEFECT THIS SUITE PINS
--------------------------
``works.genre`` in the discovery sidecar is 100% ENGLISH, and it is the
findings page's main facet -- so a Hebrew reader met a Hebrew page with an
English filter list. FJMS already holds the bilingual vocabulary
(``domains.DomainHeb`` / ``domains.ParentDomainHeb``), so the fix is a
DISPLAY-time lookup: nothing is invented, nothing is re-baked, and the stored
value the service filters on never changes.

Three properties are load-bearing and each has a test that fails without it:

1. the LABEL is translated and the VALUE is not (a translated value would
   silently stop matching the service's filter, and would not persist);
2. an unmapped name falls back to its ENGLISH form -- never to a blank, which
   would delete a filter from the page rather than leave it readable;
3. the FJMS read happens OFF the event loop, exactly once per process.

Plus a LIVE coverage test, skipped when the sidecar is absent rather than
validated against a snapshot: every distinct ``works.genre`` value in the
shipped artifacts must translate in FULL. That test is the one that caught the
first implementation, which reused ``get_domain_hierarchy`` and could translate
only 126 of the vocabulary's 188 names.
"""

from __future__ import annotations

import asyncio
import pathlib
import sqlite3

import pytest

import web.discovery_genre_labels as gl

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
FJMS_DB = REPO_ROOT / "fist_data" / "fjms_enrichment.db"
DISCOVERY_DIR = REPO_ROOT / "discovery_data"

#: A real compound genre string from the shipped public artifact, with the real
#: FJMS Hebrew names for both halves.
REAL_GENRE = "Bible: Texts and Translations / Aramaic Targumim"
REAL_PARENT_HE = "מקרא ותרגומים"
REAL_LEAF_HE = "תרגומים ארמיים"


@pytest.fixture(autouse=True)
def _clean_cache():
    """Every test starts with an unbuilt cache and leaves one behind."""
    gl.reset_for_tests()
    yield
    gl.reset_for_tests()


@pytest.fixture
def stub_map(monkeypatch):
    def _apply(mapping):
        monkeypatch.setitem(gl._STATE, "map", dict(mapping))
        return mapping

    return _apply


# ---------------------------------------------------------------------------
# 1. The label is translated; the VALUE never is.
# ---------------------------------------------------------------------------

def test_hebrew_translates_both_halves_of_a_compound_genre(stub_map):
    stub_map({"Bible: Texts and Translations": REAL_PARENT_HE,
              "Aramaic Targumim": REAL_LEAF_HE})
    assert gl.genre_display_label(REAL_GENRE, "he") == (
        REAL_PARENT_HE + gl.GENRE_PART_SEPARATOR + REAL_LEAF_HE)


def test_english_passes_the_stored_string_through_untouched(stub_map):
    stub_map({"Bible: Texts and Translations": REAL_PARENT_HE,
              "Aramaic Targumim": REAL_LEAF_HE})
    assert gl.genre_display_label(REAL_GENRE, "en") == REAL_GENRE


def test_the_stored_value_is_never_rewritten(stub_map):
    """The whole point of a display-time lookup: a Hebrew reader's selection
    still travels to the service as the English key it filters on."""
    stub_map({"Bible: Texts and Translations": REAL_PARENT_HE,
              "Aramaic Targumim": REAL_LEAF_HE})
    item = {"level": "domain", "value": REAL_GENRE, "label": REAL_GENRE}
    before = dict(item)
    gl.genre_display_label(item["label"], "he")
    assert item == before, "translating a label mutated the facet row"


# ---------------------------------------------------------------------------
# 2. Fallback -- English, never blank.
# ---------------------------------------------------------------------------

def test_an_unmapped_part_keeps_its_english_name(stub_map):
    stub_map({"Bible: Texts and Translations": REAL_PARENT_HE})
    assert gl.genre_display_label(REAL_GENRE, "he") == (
        REAL_PARENT_HE + gl.GENRE_PART_SEPARATOR + "Aramaic Targumim")


def test_a_wholly_unmapped_genre_renders_english_rather_than_blank(stub_map):
    stub_map({"Something Else": "משהו אחר"})
    assert gl.genre_display_label("Kalam / Jewish Kalam", "he") == "Kalam / Jewish Kalam"


def test_an_unprimed_cache_renders_english_rather_than_blank():
    assert not gl.is_primed()
    assert gl.genre_display_label(REAL_GENRE, "he") == REAL_GENRE
    assert gl.domain_translations() == {}


def test_a_null_genre_renders_empty_rather_than_the_word_none(stub_map):
    stub_map({})
    assert gl.genre_display_label(None, "he") == ""
    assert gl.genre_display_label(None, "en") == ""


def test_fjms_being_absent_fails_open_to_english(monkeypatch):
    """A missing sidecar must degrade to English labels, never raise into a
    render."""
    class _Boom:
        def get_domain_translations(self):
            raise RuntimeError("no sidecar")

    monkeypatch.setattr("shared.fjms_service.get_fjms_service", lambda **_kw: _Boom())
    assert gl.build_domain_translations() == {}
    assert gl.is_primed(), "a failed build must still be terminal, not retried per render"
    assert gl.genre_display_label(REAL_GENRE, "he") == REAL_GENRE


def test_the_unassigned_bucket_is_named_in_hebrew_and_keeps_its_english_value():
    """The service substitutes a sentinel for a null `works.genre`, so FJMS has
    no name for that bucket and it was the one English item left on a Hebrew
    page. The Hebrew word is `/catalog-browse`'s own; the ENGLISH label and the
    stored value are untouched."""
    from shared.discovery_service import DOMAIN_UNASSIGNED
    from web.translations import set_language

    set_language("he")
    try:
        hebrew = gl.genre_display_label(DOMAIN_UNASSIGNED, "he")
    finally:
        set_language("he")
    assert hebrew != DOMAIN_UNASSIGNED, (
        "the unassigned bucket still reads in English on a Hebrew page")
    assert not hebrew.isascii(), f"expected a Hebrew name, got {hebrew!r}"
    assert gl.genre_display_label(DOMAIN_UNASSIGNED, "en") == DOMAIN_UNASSIGNED


def test_the_unassigned_bucket_falls_back_to_english_with_no_translation(monkeypatch):
    """A missing translation entry must degrade to the English sentinel, never
    to the literal lookup key."""
    monkeypatch.setattr("web.translations.tr", lambda key, *a, **k: key)
    from shared.discovery_service import DOMAIN_UNASSIGNED

    assert gl.genre_display_label(DOMAIN_UNASSIGNED, "he") == DOMAIN_UNASSIGNED


# ---------------------------------------------------------------------------
# 3. Off the event loop, exactly once.
# ---------------------------------------------------------------------------

def _spy_executor(loop):
    calls = []
    original = loop.run_in_executor

    def spy(executor, fn, *args):
        calls.append(getattr(fn, "__name__", repr(fn)))
        return original(executor, fn, *args)

    loop.run_in_executor = spy
    return calls, original


def test_priming_dispatches_once_and_then_never_again(monkeypatch):
    """ONE executor dispatch for the first prime, ZERO for every later one.

    Zero would mean a blocking FJMS read on the single uvicorn worker's event
    loop (invisible in load average, stalls every concurrent request); two would
    mean a nested offload; and a dispatch on every page load would put an
    80ms-measured query on the request path forever.
    """
    built = {"n": 0}

    def _build():
        built["n"] += 1
        gl._STATE["map"] = {"X": "איקס"}
        return gl._STATE["map"]

    monkeypatch.setattr(gl, "build_domain_translations", _build)

    async def _run():
        loop = asyncio.get_running_loop()
        calls, original = _spy_executor(loop)
        try:
            first = await gl.prime_domain_translations()
            after_first = len(calls)
            second = await gl.prime_domain_translations()
            after_second = len(calls)
        finally:
            loop.run_in_executor = original
        return first, second, after_first, after_second

    first, second, after_first, after_second = asyncio.run(_run())
    assert first is True and second is False
    assert after_first == 1, f"the first prime produced {after_first} dispatches"
    assert after_second == 1, "a second prime dispatched again"
    assert built["n"] == 1


def test_the_pure_label_function_never_touches_the_database(monkeypatch):
    """A renderer calls this on the event loop, so it must not be able to open
    a database even when the cache is cold."""
    def _explode(**_kw):  # pragma: no cover -- must never run
        raise AssertionError("genre_display_label reached FJMS")

    monkeypatch.setattr("shared.fjms_service.get_fjms_service", _explode)
    assert gl.genre_display_label(REAL_GENRE, "he") == REAL_GENRE
    assert gl.domain_translations() == {}


# ---------------------------------------------------------------------------
# LIVE data -- skipped, never faked.
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not FJMS_DB.is_file(),
    reason="the FJMS sidecar is absent, so the bilingual domain vocabulary "
           "cannot be read LIVE; recorded as a skip rather than validated "
           "against a stale snapshot")
def test_the_live_fjms_vocabulary_covers_every_shipped_genre_string():
    """The test that caught the first implementation.

    Reusing `get_domain_hierarchy` (which nests and de-duplicates for the browse
    tree) yielded 126 of 188 names and left 10 of the public artifact's 38 genre
    strings untranslatable -- a quarter of the facet silently English on a
    Hebrew page. `get_domain_translations` keeps every name; this asserts the
    coverage rather than trusting it.
    """
    from shared.discovery_service import DOMAIN_UNASSIGNED
    from shared.fjms_service import FjmsService

    service = FjmsService(db_path=str(FJMS_DB), thread_safe=True)
    mapping = service.get_domain_translations()
    assert mapping, "the live FJMS domain vocabulary read back empty"

    sidecars = sorted(DISCOVERY_DIR.glob("*.db")) if DISCOVERY_DIR.is_dir() else []
    checked = 0
    for path in sidecars:
        try:
            conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            genres = [
                row[0] for row in conn.execute(
                    "SELECT DISTINCT genre FROM works "
                    "WHERE genre IS NOT NULL AND genre <> ''")
            ]
            conn.close()
        except sqlite3.Error:
            continue
        if not genres:
            continue
        checked += 1
        # Only ENGLISH parts need a Hebrew name. One superseded development
        # sidecar in this directory (`discovery-v1-real-smoke-r2rework.db`)
        # stores its genres in Hebrew already; those pass through the display
        # function unchanged, which is the correct behaviour rather than a gap.
        def _needs_translation(part: str) -> bool:
            return any("a" <= ch.lower() <= "z" for ch in part)

        unmapped = [
            genre for genre in genres
            # The unassigned SENTINEL is not a domain name and is deliberately
            # absent from the FJMS vocabulary: `genre_display_label` routes it
            # through its own `tr("Unclassified")` branch before the vocabulary
            # is ever consulted. Reported as a coverage gap when the V4 artifact
            # (the first shipped build to carry it) was staged locally on
            # 2026-08-16 -- the product was right and this check was too broad.
            # The exemption is PROVED below rather than trusted, so deleting
            # that branch still fails a test.
            if genre != DOMAIN_UNASSIGNED
            and any(_needs_translation(part.strip()) and part.strip() not in mapping
                    for part in genre.split(gl.GENRE_PART_SEPARATOR))
        ]
        assert not unmapped, (
            f"{path.name}: {len(unmapped)} English genre string(s) have no Hebrew "
            f"name in the live FJMS vocabulary: {unmapped[:5]}")

        if DOMAIN_UNASSIGNED in genres:
            rendered = gl.genre_display_label(DOMAIN_UNASSIGNED, "he")
            assert rendered and rendered != DOMAIN_UNASSIGNED, (
                f"{path.name} ships the unassigned sentinel and it rendered as "
                f"{rendered!r} under Hebrew -- the vocabulary exemption above is "
                "only sound while the dedicated branch supplies a Hebrew name")
            assert any("֐" <= ch <= "ת" for ch in rendered), (
                f"the unassigned sentinel rendered {rendered!r}, which carries no "
                "Hebrew, so a Hebrew reader sees an English facet value")

    if not checked:
        pytest.skip("no discovery sidecar in discovery_data/ carries genre values")


@pytest.mark.skipif(not FJMS_DB.is_file(), reason="the FJMS sidecar is absent")
def test_every_live_hebrew_domain_name_passes_the_surface_honesty_gate():
    """These strings become RENDERED TEXT on a discovery surface, so they are
    subject to the same six-detector gate as every other word on the page.

    The gate's Hebrew rate lexicon includes ordinary-looking words (`שיעור`,
    `נתח`, `דיוק`), so a future FJMS vocabulary entry could trip it; this fails
    on the offending NAME rather than on some unrelated render three plans
    later."""
    import html as _html

    from shared.fjms_service import FjmsService
    from tests.render_smoke.discovery_honesty_gate import assert_surface_honesty

    mapping = FjmsService(db_path=str(FJMS_DB), thread_safe=True).get_domain_translations()
    assert mapping
    for english, hebrew in sorted(mapping.items()):
        for lang, text in (("en", english), ("he", hebrew)):
            fragment = f'<div class="probe">{_html.escape(text)}</div>'
            assert_surface_honesty(fragment, scope_selector="probe", lang=lang)


@pytest.mark.skipif(not FJMS_DB.is_file(), reason="the FJMS sidecar is absent")
def test_the_live_vocabulary_carries_both_parents_and_leaves():
    """A map built from leaves alone would translate half of every compound
    label and leave the other half English."""
    from shared.fjms_service import FjmsService

    mapping = FjmsService(db_path=str(FJMS_DB), thread_safe=True).get_domain_translations()
    parent, leaf = REAL_GENRE.split(gl.GENRE_PART_SEPARATOR)
    assert mapping.get(parent) == REAL_PARENT_HE
    assert mapping.get(leaf) == REAL_LEAF_HE


@pytest.mark.skipif(not FJMS_DB.is_file(), reason="the FJMS sidecar is absent")
def test_the_live_lookup_is_cached_after_the_first_read():
    """One query per process. The second call must not re-scan ~390K rows."""
    from shared.fjms_service import FjmsService

    service = FjmsService(db_path=str(FJMS_DB), thread_safe=True)
    first = service.get_domain_translations()
    calls = {"n": 0}
    real_execute = service._conn.execute

    def _counting(*args, **kwargs):
        calls["n"] += 1
        return real_execute(*args, **kwargs)

    service._conn.execute = _counting
    second = service.get_domain_translations()
    assert second is first
    assert calls["n"] == 0, "the cached lookup queried the database again"
