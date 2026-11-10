"""The unused /api/visual_similarity_db download route is not registered.

No client calls it (the web button and the desktop download UI are both
commented out, and the desktop installer bundles the file), so the web app
does not serve it. The checks use Starlette route matching on a bare app built
exactly as production registers the API routes; they send no HTTP request.
"""
from fastapi import FastAPI
from starlette.routing import Match

from web.api import init_api_routes
from web.search_api import init_search_api

REMOVED_PATH = '/api/visual_similarity_db'
KEPT_PATHS = (
    '/api/visual_suggestions/version',
    '/api/visual_suggestions/batch_check',
    '/api/visual_suggestions/{sys_id}',
)


def _bare_app() -> FastAPI:
    bare = FastAPI()
    init_api_routes(app_override=bare)
    init_search_api(app_override=bare)
    return bare


def _matching_routes(app: FastAPI, path: str, method: str) -> list[str]:
    scope = {'type': 'http', 'path': path, 'method': method, 'root_path': ''}
    return [
        getattr(route, 'path', repr(route))
        for route in app.routes
        if hasattr(route, 'matches') and route.matches(scope)[0] is not Match.NONE
    ]


def test_visual_similarity_db_route_is_not_registered():
    app = _bare_app()
    for method in ('GET', 'HEAD'):
        # Any match, FULL or PARTIAL (a method mismatch), means a route claims the path.
        assert _matching_routes(app, REMOVED_PATH, method) == [], method


def test_visual_suggestions_routes_still_registered():
    """Regression guard, green before and after: the three kept routes stay, in order.

    /version must stay registered before /{sys_id}, or the wildcard captures it.
    """
    paths = [getattr(route, 'path', None) for route in _bare_app().routes]
    for kept in KEPT_PATHS:
        assert kept in paths, kept
    positions = [paths.index(kept) for kept in KEPT_PATHS]
    assert positions == sorted(positions), positions
