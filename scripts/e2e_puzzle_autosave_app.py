"""Run the web app for scripts/e2e_puzzle_autosave.py (a real-browser check).

Not a deployment entry point. It starts ``web.main`` with three test seams so
the Fragment Puzzle runs offline: saved joins go to the SQLite file named by
``E2E_JOINS_DB``, folio lookups return two made-up folios per manuscript
(``FL<sys_id>`` and ``FL<sys_id>v``) instead of asking NLI, and join thumbnails
are skipped. Fragment images are stubbed by the browser side of the check.
"""
import os
import runpy
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import shared.puzzle_export as puzzle_export  # noqa: E402
import shared.puzzle_service as puzzle_service  # noqa: E402

_service = puzzle_service.PuzzleService(db_path=os.environ['E2E_JOINS_DB'], thread_safe=True)
puzzle_service.get_puzzle_service = lambda thread_safe=False: _service
puzzle_export.generate_thumbnail = lambda fragments, image_service, thumb_size=150: ''

import web.pages.puzzle as puzzle_page  # noqa: E402

puzzle_page.STATE_SAVE_INTERVAL_S = float(os.environ.get('E2E_STATE_SAVE_INTERVAL_S', '2'))
puzzle_page._resolve_folios = lambda sys_id: [
    {'fl_id': f'FL{sys_id}', 'label': '1r'},
    {'fl_id': f'FL{sys_id}v', 'label': '1v'},
]

runpy.run_module('web.main', run_name='__main__')
