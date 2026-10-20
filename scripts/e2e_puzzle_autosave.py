"""Real-browser check of the web Fragment Puzzle's saving (auto-save, restore, crop).

The canvas is Fabric.js, which the NiceGUI User harness cannot run, so the
harness tests in tests/test_puzzle_autosave.py fake the browser's replies.
This script is the other half: it starts the app (scripts/e2e_puzzle_autosave_app.py),
drives /puzzle in headless Chromium with Playwright, and reads the saved joins
straight from their SQLite file. Fragment images are stubbed (a flat PNG; one
manuscript's image always fails), so it runs offline except for the Fabric.js
CDN. It is a manual check, not part of CI:

    python scripts/e2e_puzzle_autosave.py            # exit 0 when every check passes

Needs ``pip install playwright`` and a Chromium (``PLAYWRIGHT_BROWSERS_PATH``
or ``playwright install chromium``; ``E2E_CHROMIUM`` names another executable),
and GENIZAH_STORAGE_SECRET set.
"""
from __future__ import annotations

import io
import json
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from urllib.parse import parse_qs, urlparse

from PIL import Image
from playwright.sync_api import sync_playwright

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
S1 = '990053835020205171'   # two real corpus sys_ids (shelfmarks come from libraries.csv)
S2 = '990053835750205171'
BAD = '990053836920205171'  # its image never loads
K1, K2, KBAD = f'{S1},1r', f'{S2},1r', f'{BAD},1r'

results: list[tuple[str, bool, str]] = []


def check(name, ok, detail=''):
    results.append((name, bool(ok), detail))
    print(('PASS ' if ok else 'FAIL ') + name + (f' -- {detail}' if detail and not ok else ''), flush=True)


def _png():
    buf = io.BytesIO()
    Image.new('RGBA', (400, 300), (120, 80, 40, 255)).save(buf, format='PNG')
    return buf.getvalue()


PNG = _png()


def _free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def _route(route):
    url = urlparse(route.request.url)
    if url.hostname in ('127.0.0.1', 'localhost'):
        if url.path in ('/api/puzzle_image', '/api/puzzle_ext_image'):
            fl_id = parse_qs(url.query).get('fl_id', [''])[0]
            if BAD in fl_id or url.path == '/api/puzzle_ext_image':
                return route.fulfill(status=404, body='no image')
            return route.fulfill(status=200, body=PNG, content_type='image/png')
        return route.continue_()
    if url.hostname == 'cdn.jsdelivr.net':
        return route.continue_()
    return route.abort()


class Db:
    def __init__(self, path):
        self.path = path

    def row(self, doc_id):
        con = sqlite3.connect(self.path)
        con.row_factory = sqlite3.Row
        try:
            r = con.execute('SELECT * FROM join_documents WHERE id = ?', (doc_id,)).fetchone()
            return dict(r) if r else None
        finally:
            con.close()

    def ids(self):
        con = sqlite3.connect(self.path)
        try:
            return {r[0] for r in con.execute('SELECT id FROM join_documents')}
        finally:
            con.close()

    def frags(self, doc_id):
        row = self.row(doc_id)
        return {f"{f['sys_id']},{f['folio_label']}": f for f in json.loads(row['fragments_json'])} if row else {}

    def put(self, doc_id, owner, fragments):
        con = sqlite3.connect(self.path)
        try:
            con.execute(
                "INSERT INTO join_documents (id, title, notes, join_type, fragments_json, owner_key, "
                "created_at, updated_at) VALUES (?, 'seeded', '', 'uncertain', ?, ?, "
                "'2026-01-01T00:00:00', '2026-01-01T00:00:00')",
                (doc_id, json.dumps(fragments), owner))
            con.commit()
        finally:
            con.close()


def keys(page):
    return set(page.evaluate('window.puzzleCanvas && window.puzzleCanvas.canvas ? '
                             'Object.keys(window.puzzleCanvas.fragments) : []'))


def wait_keys(page, expected, timeout=20.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if keys(page) == expected:
            return True
        time.sleep(0.2)
    return False


def canvas_after(page, expected, timeout=25.0):
    """The canvas's keys once it shows ``expected`` (or the timeout passes),
    read again a little later so a fragment that lands late is seen too."""
    wait_keys(page, expected, timeout)
    time.sleep(2.5)
    return keys(page)


def settle(seconds):
    time.sleep(seconds)


def open_page(page, base, path):
    page.goto(base + path)
    page.wait_for_function('window.puzzleCanvas && window.puzzleCanvas.canvas', timeout=30000)


def click_icon(page, icon):
    page.locator(f'button:has(i:text-is("{icon}"))').first.click()


def save_new_join(page, db, timeout=20.0):
    """Save the scratch canvas through the Save dialog; the new join's id."""
    before = db.ids()
    click_icon(page, 'save')
    page.get_by_role('button', name='Save', exact=True).click()
    deadline = time.time() + timeout
    while time.time() < deadline:
        new = db.ids() - before
        if new:
            return new.pop()
        time.sleep(0.2)
    raise AssertionError('the Save dialog saved nothing')


def screen_center(page, key):
    return page.evaluate('''(key) => {
        const pc = window.puzzleCanvas, o = pc.fragments[key], p = o.getCenterPoint();
        const v = pc.canvas.viewportTransform, r = pc.canvas.upperCanvasEl.getBoundingClientRect();
        return {x: r.left + p.x * v[0] + v[4], y: r.top + p.y * v[3] + v[5]};
    }''', key)


def drag(page, key, dx, dy):
    c = screen_center(page, key)
    page.mouse.move(c['x'], c['y'])
    page.mouse.down()
    page.mouse.move(c['x'] + dx, c['y'] + dy, steps=8)
    page.mouse.up()


def run(base, db):
    with sync_playwright() as p:
        browser = p.chromium.launch(executable_path=os.environ.get('E2E_CHROMIUM') or None)
        context = browser.new_context(viewport={'width': 1400, 'height': 900})
        context.route('**/*', _route)
        page = context.new_page()

        # A saved draft D1 holding S1.
        open_page(page, base, f'/puzzle?add={S1}')
        assert wait_keys(page, {K1}), keys(page)
        d1 = save_new_join(page, db)
        assert K1 in db.frags(d1), 'setup: the first draft was not saved'
        owner = db.row(d1)['owner_key']

        # 1. Open the draft through ?doc= while the tab holds an older canvas.
        click_icon(page, 'note_add')                       # New: a scratch canvas
        settle(1.0)
        open_page(page, base, f'/puzzle?add={S2}')
        assert wait_keys(page, {K2}), keys(page)
        settle(3.0)                                        # the tab keeps S2's canvas
        open_page(page, base, f'/puzzle?doc={d1}')
        shown = canvas_after(page, {K1})
        check('?doc= shows only the draft', shown == {K1}, f'canvas {sorted(shown)}')
        settle(2.5)                                        # a periodic tab save
        open_page(page, base, '/puzzle')                   # reload, no ?doc=
        shown = canvas_after(page, {K1})
        check('a reload after ?doc= restores the draft, not the older canvas',
              shown == {K1}, f'canvas {sorted(shown)}')

        # 2. Loading writes nothing; a drag does.
        before = db.row(d1)['updated_at']
        open_page(page, base, f'/puzzle?doc={d1}')
        assert canvas_after(page, {K1}) == {K1}, keys(page)
        settle(2.0)
        check('opening a draft does not save it', db.row(d1)['updated_at'] == before,
              f"{before} -> {db.row(d1)['updated_at']}")
        x_before = db.frags(d1)[K1]['x']
        drag(page, K1, 150, 60)
        settle(4.0)
        dragged = page.evaluate(f'window.puzzleCanvas.fragments["{K1}"].getCenterPoint().x')
        x_after = db.frags(d1)[K1]['x']
        check('dragging a fragment of a saved draft auto-saves it', abs(x_after - x_before) > 50,
              f'x {x_before} -> {x_after}')
        page.evaluate('document.activeElement && document.activeElement.blur()')
        for _ in range(5):
            page.keyboard.press('ArrowRight')              # nudge the selected fragment 5 px
        settle(4.0)
        check('a keyboard nudge auto-saves', abs(db.frags(d1)[K1]['x'] - x_after - 5) < 0.01,
              f"x {x_after} -> {db.frags(d1)[K1]['x']}")
        dragged = page.evaluate(f'window.puzzleCanvas.fragments["{K1}"].getCenterPoint().x')

        # Title and notes: leaving the field saves only a changed text.
        click_icon(page, 'folder_open')
        notes = page.get_by_label('Notes')
        stamp = db.row(d1)['updated_at']
        notes.click()
        page.keyboard.press('Tab')
        settle(3.5)
        check('leaving the notes unchanged does not save', db.row(d1)['updated_at'] == stamp)
        notes.click()
        page.keyboard.type('e2e note')
        page.keyboard.press('Tab')
        settle(3.5)
        check('a changed note is saved', db.row(d1)['notes'] == 'e2e note', repr(db.row(d1)['notes']))
        page.keyboard.press('Escape')                      # close the dialog
        settle(0.5)

        open_page(page, base, '/puzzle')
        canvas_after(page, {K1})
        drawn = page.evaluate(f'window.puzzleCanvas.fragments["{K1}"] ? '
                              f'window.puzzleCanvas.fragments["{K1}"].getCenterPoint().x : null')
        check('a reload shows the dragged position', drawn is not None and abs(drawn - dragged) < 2,
              f'dragged to x {dragged}, drawn at {drawn}')

        # 3. A fragment whose image fails keeps its saved place.
        d2 = 'e2e-failed-image'
        db.put(d2, owner, [
            {'sys_id': S1, 'folio_label': '1r', 'fl_id': f'FL{S1}', 'shelfmark': 'S1',
             'x': 10, 'y': 20, 'rotation': 0, 'scale': 1.0},
            {'sys_id': BAD, 'folio_label': '1r', 'fl_id': f'FL{BAD}', 'shelfmark': 'BAD',
             'x': 500, 'y': 600, 'rotation': 45, 'scale': 0.5},
        ])
        open_page(page, base, f'/puzzle?doc={d2}')
        assert canvas_after(page, {K1}) == {K1}, keys(page)
        drag(page, K1, 120, 0)
        settle(4.0)
        bad = db.frags(d2).get(KBAD)
        check('an edit saves the draft', db.row(d2)['updated_at'] != '2026-01-01T00:00:00')
        check('an undrawn fragment keeps its position and rotation',
              bad and bad['x'] == 500 and bad['y'] == 600 and bad['rotation'] == 45 and bad['scale'] == 0.5,
              f'{bad}')

        # 4. Crop: saved with the draft and kept by a reload of the tab.
        click_icon(page, 'note_add')
        settle(1.0)
        open_page(page, base, f'/puzzle?add={S2}')
        assert wait_keys(page, {K2}), keys(page)
        settle(1.0)
        page.get_by_role('button', name='Crop').click()
        settle(0.5)
        page.evaluate('document.activeElement && document.activeElement.blur()')
        for _ in range(3):
            page.keyboard.press('ArrowLeft')               # crop mode: arrows crop an edge
        click_icon(page, 'check')                          # confirm the crop
        settle(0.5)
        crop_x = page.evaluate(f'window.puzzleCanvas.fragments["{K2}"].cropX')
        assert crop_x and crop_x > 0, f'setup: the crop did not apply ({crop_x})'
        settle(3.0)                                        # periodic tab save
        open_page(page, base, '/puzzle')                   # reload the scratch canvas
        assert wait_keys(page, {K2}), keys(page)
        settle(2.0)
        crop_after = page.evaluate(f'window.puzzleCanvas.fragments["{K2}"].cropX')
        check('a reload keeps a crop', crop_after == crop_x, f'cropX {crop_x} -> {crop_after}')
        d3 = save_new_join(page, db)
        saved = db.frags(d3).get(K2) or {}
        check('Save writes a confirmed crop', saved.get('crop_left', 0) == crop_x,
              f"crop_left {saved.get('crop_left')} (canvas cropX {crop_x})")

        # 5. Cropping a saved draft auto-saves it, and a reload shows the crop.
        open_page(page, base, f'/puzzle?doc={d1}')
        assert canvas_after(page, {K1}) == {K1}, keys(page)
        page.evaluate(f'window.puzzleCanvas.canvas.setActiveObject(window.puzzleCanvas.fragments["{K1}"])')
        page.get_by_role('button', name='Crop').click()
        settle(0.5)
        page.evaluate('document.activeElement && document.activeElement.blur()')
        for _ in range(2):
            page.keyboard.press('ArrowUp')                 # crop the top edge
        click_icon(page, 'check')
        settle(4.0)
        top = page.evaluate(f'window.puzzleCanvas.fragments["{K1}"].cropY')
        check('cropping a saved draft auto-saves the crop', db.frags(d1)[K1].get('crop_top') == top and top > 0,
              f"crop_top {db.frags(d1)[K1].get('crop_top')} (canvas cropY {top})")
        open_page(page, base, '/puzzle')
        canvas_after(page, {K1})
        settle(1.0)
        check('a reload of the draft shows its crop',
              page.evaluate(f'window.puzzleCanvas.fragments["{K1}"].cropY') == top)
        browser.close()


def main():
    tmp = tempfile.mkdtemp(prefix='e2e_puzzle_')
    db_path = os.path.join(tmp, 'joins.db')
    port = _free_port()
    env = dict(os.environ, E2E_JOINS_DB=db_path, GENIZAH_PORT=str(port), QT_QPA_PLATFORM='offscreen')
    log = open(os.path.join(tmp, 'app.log'), 'w')
    app = subprocess.Popen([sys.executable, os.path.join(ROOT, 'scripts', 'e2e_puzzle_autosave_app.py')],
                           cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
    try:
        import urllib.request
        base = f'http://127.0.0.1:{port}'
        for _ in range(120):
            try:
                urllib.request.urlopen(base + '/puzzle', timeout=2)
                break
            except Exception:
                time.sleep(1)
        else:
            raise SystemExit(f'the app did not start; see {log.name}')
        run(base, Db(db_path))
    finally:
        app.terminate()
        app.wait(timeout=20)
    failed = [name for name, ok, _ in results if not ok]
    print(f'\n{len(results) - len(failed)}/{len(results)} passed; app log {log.name}')
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
