"""Feature flags for the web application."""

from __future__ import annotations

import os


def _env_enabled(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


WEB_PUZZLE_ENABLED = _env_enabled("WEB_PUZZLE_ENABLED", True)

# Phase 133 (ATLAS-01) — Visual Atlas Preview. Default OFF: the flag is the
# safety mechanism that keeps the not-yet-released beta surface (the /atlas
# page, its data routes, and the nav link) hidden in production until the
# REL-01 launch gate. Enable in the beta environment with ATLAS_PREVIEW_ENABLED=1.
# NOTE: this flag is necessary but NOT sufficient — the single authoritative
# predicate web/atlas_assets.py::atlas_preview_available() ANDs it with the
# baked-asset readiness state, so a flag-ON/asset-missing window still hides.
ATLAS_PREVIEW_ENABLED = _env_enabled("ATLAS_PREVIEW_ENABLED", False)

# Phase 134 (DATA-07) — Discovery Data Spine. Default OFF: the flag gates the
# not-yet-released discovery surfaces (no UI ships in Phase 134 itself; this
# flag is plumbing for Phase 135+). Enable with DISCOVERY_ENABLED=1.
# NOTE: this flag is necessary but NOT sufficient — the single authoritative
# predicate web/discovery_assets.py::discovery_available() ANDs it with the
# discovery.db sidecar's startup-loaded readiness state, so a flag-ON/sidecar-
# missing (or incompatible/corrupt) window still hides cleanly.
DISCOVERY_ENABLED = _env_enabled("DISCOVERY_ENABLED", False)

# Phase 135 (BAND-05) — the Phase-139 REL-01 public-release gate. Default OFF
# through Phase 138: while OFF, the (flag-gated) discovery surfaces that DO ship
# pre-release — currently only the /help "Confidence Bands & Methods" methods
# section — are noindexed (via web/discovery.py::discovery_methods_noindex(),
# which ANDs discovery_available() with NOT this flag) so search engines never
# index the pre-release methods copy. Flip to 1/true at the REL-01 launch gate
# (Phase 139) to make those surfaces indexable — a dedicated flag so the
# noindex is a bounded pre-release window, never a forever de-index.
DISCOVERY_PUBLIC_RELEASED = _env_enabled("DISCOVERY_PUBLIC_RELEASED", False)

# Phase 145 (passage-matching parallels search, web surface). Default OFF:
# the flag gates the not-yet-released `method='passage'` option on
# POST /api/parallels and the method selector on web/pages/parallels.py.
# Enable with PASSAGE_PARALLELS_ENABLED=1. NOTE: this flag is necessary but
# NOT sufficient -- the single authoritative predicate
# web/passage_assets.py::passage_available() ANDs it with the passage
# index's startup-loaded readiness state (shared/passage_index.py::open_index
# is itself fail-closed: manifest, layout version, normalizer version, bit
# budgets, byte order, CSR sanity, declared-vs-actual file sizes), so a
# flag-ON/index-missing-or-corrupt window still hides cleanly.
PASSAGE_PARALLELS_ENABLED = _env_enabled("PASSAGE_PARALLELS_ENABLED", False)


def web_fgp_enabled() -> bool:
    """Whether FGP transcriptions surface in the WEB version chooser.

    Layers an optional web-only override on top of the shared
    ``FGP_TRANSCRIPTIONS_ENABLED`` gate (the live one, re-read per call in
    ``shared/fgp_service.py``). When ``WEB_FGP_ENABLED`` is unset it defaults to
    the shared flag; when set it wins for the web app (e.g. disable on web while
    the shared/desktop default is on). Read per call so an env flip + restart
    takes effect without code changes. ``shared/`` must not import this.

    Default: ON (2026-06-22, go-live) — mirrors the shared
    ``FGP_TRANSCRIPTIONS_ENABLED`` default. Graceful no-op when the sidecar DB is
    absent. Disable on web only with ``WEB_FGP_ENABLED=0``.
    """
    shared_default = _env_enabled("FGP_TRANSCRIPTIONS_ENABLED", True)
    return _env_enabled("WEB_FGP_ENABLED", shared_default)
