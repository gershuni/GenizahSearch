# -*- coding: utf-8 -*-
"""The web shelf picker's keyboard (``_KEYS_JS``, owner 2026-11-25) in a real browser: a page laid out as
the picker draws it (columns of rows, the shelfmarks as thumbnails, the filters, the back arrow, the pane's
zoom and page buttons), driven by Node's Playwright. Skipped where Node or its Playwright is missing.

* each list is one Tab stop; the arrows, Home / End move in it, in the thumbnails by rows and columns;
* in a column the arrow forward opens the row and goes into the next list once it is drawn, the arrow
  back goes to the list before, and from the first list one level up; mirrored in Hebrew;
* from a list Ctrl + an arrow presses the pane's previous / next image (as the arrows on screen),
  + / - press its zoom buttons; Ctrl+F goes to the list's filter, Esc back to the list without closing
  anything; Alt+D presses the details button.
"""
from __future__ import annotations

import json
import shutil
import subprocess

import pytest

from web.components.shelfmark_tree_picker import _KEYS_JS

_PAGE = """<!doctype html><html><head><meta charset="utf-8"><style>
.stp-card { display: flex; gap: 10px; }
.stp-list { display: flex; flex-direction: column; width: 160px; }
.stp-list.stp-gallery { display: grid; grid-template-columns: repeat(3, 50px); width: 170px; }
.stp-row { height: 20px; }
</style></head><body>
<div class="stp-card" dir="DIR" data-testid="shelf-picker">
  <button class="stp-back" data-testid="back">back</button>
  <div class="stp-col"><div class="stp-filter"><input id="f1"></div>
    <div class="stp-list" id="l1">ROWS1</div></div>
  <div class="stp-col"><div class="stp-filter"><input id="f2"></div>
    <div class="stp-list" id="l2">ROWS2</div></div>
  <div class="stp-col stp-leaves"><div class="stp-filter"><input id="f3"></div>
    <div class="stp-list stp-gallery" id="l3">TILES</div></div>
  <div data-testid="shelf-details">
    <button data-testid="shelf-details-toggle">details</button>
    <div class="stp-d-zoom" id="c77"><img></div>
    <button data-testid="shelf-page-prev">prev</button><button data-testid="shelf-page-next">next</button>
    <button data-testid="shelf-zoom-out">-</button><button data-testid="shelf-zoom-in">+</button>
  </div>
</div>
<script>
window.log = [];
document.addEventListener('click', (e) => {
  const t = e.target.closest('[data-testid], [data-node]');
  if (!t) return;
  if (t.dataset.node) {
    window.log.push('open ' + t.dataset.node);
    // the server draws the lists again a moment later: this row open, its children in the next list
    setTimeout(() => {
      const list = t.parentElement;
      list.querySelectorAll('[data-node]').forEach((r) => r.classList.toggle('stp-on', r === t));
      if (list.id === 'l1') {
        document.getElementById('l2').innerHTML = [1, 2, 3].map(
          (n) => `<div class="stp-row" data-node="${t.dataset.node}.${n}" tabindex="-1">${n}</div>`).join('');
      }
    }, 80);
  } else {
    window.log.push(t.dataset.testid);
  }
});
window.__stpZoom = {reset: (id) => window.log.push('reset ' + id)};
</script>
<script>KEYS</script>
<script>setTimeout(() => window.__stpKeys.fix(), 10);</script>
</body></html>"""


def _rows(prefix, n, on=None):
    return ''.join(f'<div class="stp-row{" stp-on" if i == on else ""}" data-node="{prefix}{i}" tabindex="-1">'
                   f'{prefix}{i}</div>' for i in range(n))


def _tiles(n):
    return ''.join(f'<div class="stp-tile" data-leaf="t{i}" tabindex="-1" style="height:40px">{i}</div>'
                   for i in range(n))


_SCRIPT = r"""
const { chromium } = require('playwright');
(async () => {
  const pages = JSON.parse(process.argv[2]);
  const browser = await chromium.launch();
  const out = {};
  for (const [dir, html] of Object.entries(pages)) {
    const page = await browser.newPage();
    await page.setContent(html);
    await page.waitForTimeout(60);
    const steps = [];
    const at = async () => page.evaluate(() => {
      const a = document.activeElement;
      return a ? (a.dataset.node || a.dataset.leaf || a.id || a.dataset.testid || a.tagName) : null;
    });
    const tabs = async () => page.evaluate(() => Array.from(document.querySelectorAll('[data-node], [data-leaf]'))
      .filter((e) => e.tabIndex === 0).map((e) => e.dataset.node || e.dataset.leaf));
    const press = async (key) => { await page.keyboard.press(key); await page.waitForTimeout(30); steps.push([key, await at()]); };
    steps.push(['tabbable', await tabs()]);
    await page.focus('#f1');
    await press('Tab');                 // from a filter into its list, on its open row
    await press('Tab');                 // the next list, its filter passed by
    await press('Tab');
    await press('Shift+Tab');
    await press('Shift+Tab');
    const fwd = dir === 'rtl' ? 'ArrowLeft' : 'ArrowRight', back = dir === 'rtl' ? 'ArrowRight' : 'ArrowLeft';
    await press('ArrowDown');
    await press('End');
    await press('Home');
    await press(fwd);                   // a row not open: it opens, then the next list takes the keyboard
    await page.waitForTimeout(250);
    steps.push(['after open', await at()]);
    await press(back);                  // the list before
    await press(back);                  // from the first list: one level up (the back arrow)
    await page.focus('#f3');
    await press('Tab');                 // the thumbnails
    await press(fwd);
    await press('ArrowDown');           // a row of three down
    await press('Control+' + fwd);
    await press('Control+' + back);
    await press('+');
    await press('-');
    await press('0');
    await press('Control+KeyF');        // the thumbnails' filter
    await press('Escape');              // back to the thumbnails
    await press('Alt+KeyD');
    out[dir] = {steps, log: await page.evaluate(() => window.log)};
    await page.close();
  }
  await browser.close();
  console.log(JSON.stringify(out));
})().catch((e) => { console.error(e); process.exit(1); });
"""


def _node_with_playwright():
    node = shutil.which('node')
    if node is None:
        return None
    check = subprocess.run([node, '-e', "require('playwright')"], capture_output=True, timeout=60)
    return node if check.returncode == 0 else None


def test_the_keyboard_in_a_browser(tmp_path):
    node = _node_with_playwright()
    if node is None:
        pytest.skip("node with playwright is not installed")
    pages = {}
    for direction in ('ltr', 'rtl'):
        pages[direction] = (_PAGE.replace('DIR', direction).replace('ROWS1', _rows('a', 3, on=0))
                            .replace('ROWS2', _rows('a0.', 4)).replace('TILES', _tiles(7))
                            .replace('KEYS', _KEYS_JS))
    script = tmp_path / 'keys.js'
    script.write_text(_SCRIPT, encoding='utf-8')
    run = subprocess.run([node, str(script), json.dumps(pages)], capture_output=True, text=True, timeout=180)
    assert run.returncode == 0, run.stderr
    out = json.loads(run.stdout)
    for direction in ('ltr', 'rtl'):
        steps, log = out[direction]['steps'], out[direction]['log']
        seen = [s[1] for s in steps]
        # one Tab stop a list: the open row, else the first
        assert seen[0] == ['a0', 'a0.0', 't0'], direction
        assert seen[1:6] == ['a0', 'a0.0', 't0', 'a0.0', 'a0'], direction
        assert seen[6:9] == ['a1', 'a2', 'a0'], direction
        # the arrow forward on the open row goes into its list ... (a0 is open: no new drawing)
        assert seen[9] == 'a0.0' and seen[10] == 'a0.0', direction
        assert 'open a0' not in log
        # back: the list before, then one level up
        assert seen[11] == 'a0', direction
        assert 'back' in log, direction
        # the thumbnails, three a row
        assert seen[13:15] == ['t0', 't1'] and seen[15] == 't4', direction
        # the pane's keys pressed its buttons, the filter and back, the details button
        assert log[log.index('back') + 1:] == ['shelf-page-next', 'shelf-page-prev', 'shelf-zoom-in',
                                                'shelf-zoom-out', 'reset 77', 'shelf-details-toggle'], direction
        assert seen[-3] == 'f3' and seen[-2] == 't4', direction


def test_a_closed_row_opens_and_its_list_takes_the_keyboard(tmp_path):
    node = _node_with_playwright()
    if node is None:
        pytest.skip("node with playwright is not installed")
    html = (_PAGE.replace('DIR', 'ltr').replace('ROWS1', _rows('a', 3, on=0)).replace('ROWS2', _rows('a0.', 4))
            .replace('TILES', _tiles(3)).replace('KEYS', _KEYS_JS))
    script = tmp_path / 'open.js'
    script.write_text(r"""
const { chromium } = require('playwright');
(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage();
  await page.setContent(JSON.parse(process.argv[2]));
  await page.waitForTimeout(60);
  await page.focus('[data-node="a1"]');
  await page.keyboard.press('ArrowRight');
  await page.waitForTimeout(300);
  const at = await page.evaluate(() => document.activeElement.dataset.node);
  console.log(JSON.stringify({at, log: await page.evaluate(() => window.log)}));
  await browser.close();
})().catch((e) => { console.error(e); process.exit(1); });
""", encoding='utf-8')
    run = subprocess.run([node, str(script), json.dumps(html)], capture_output=True, text=True, timeout=120)
    assert run.returncode == 0, run.stderr
    out = json.loads(run.stdout)
    assert out['log'] == ['open a1']
    assert out['at'] == 'a1.1'     # the redrawn list's first row: a1's children, the keyboard in them


_VIEWS_PAGE = """<!doctype html><html><head><meta charset="utf-8"><style>
.stp-list { display: grid; grid-template-columns: repeat(3, 50px); }
</style></head><body>
<div class="stp-card" dir="ltr">
  <div class="stp-col stp-leaves">
    <button data-view="list" aria-pressed="false">list</button>
    <button data-view="table" aria-pressed="false">table</button>
    <button data-view="gallery" aria-pressed="true">thumbnails</button>
    <div data-testid="shelf-thumb-size"><button>S</button><button>M</button><button>L</button><button>XL</button></div>
    <div class="stp-filter"><input id="f"></div>
    <div class="stp-list stp-gallery stp-size-m" id="g">TILES</div>
  </div>
</div>
<script>
window.log = [];
const redraw = () => setTimeout(() => {
  // the server draws the shelfmarks again: a new list element
  const old = document.getElementById('g'), list = old.cloneNode(true);
  list.querySelectorAll('[data-leaf]').forEach((t) => { t.tabIndex = -1; });
  old.replaceWith(list);
}, 120);
document.addEventListener('click', (e) => {
  const b = e.target.closest('button');
  if (!b) return;
  window.log.push(b.dataset.view ? 'view ' + b.dataset.view : 'size ' + b.textContent);
  redraw();
});
</script>
<script>KEYS</script>
<script>setTimeout(() => window.__stpKeys.fix(), 10);</script>
</body></html>"""


def test_the_view_and_size_keys(tmp_path):
    node = _node_with_playwright()
    if node is None:
        pytest.skip("node with playwright is not installed")
    html = _VIEWS_PAGE.replace('TILES', _tiles(6)).replace('KEYS', _KEYS_JS)
    script = tmp_path / 'views.js'
    script.write_text(r"""
const { chromium } = require('playwright');
(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage();
  await page.setContent(JSON.parse(process.argv[2]));
  await page.waitForTimeout(60);
  const at = async () => page.evaluate(() => {
    const a = document.activeElement;
    return a && a.dataset.leaf ? a.dataset.leaf + (a.parentElement.isConnected ? '' : ' (gone)') : (a ? a.tagName : null);
  });
  const out = [];
  await page.focus('[data-leaf="t1"]');
  for (const key of ['BracketRight', 'BracketLeft', 'Digit1', 'Digit3']) {
    await page.keyboard.press(key);
    await page.waitForTimeout(300);
    out.push([key, await at()]);
  }
  console.log(JSON.stringify({out, log: await page.evaluate(() => window.log)}));
  await browser.close();
})().catch((e) => { console.error(e); process.exit(1); });
""", encoding='utf-8')
    run = subprocess.run([node, str(script), json.dumps(html)], capture_output=True, text=True, timeout=120)
    assert run.returncode == 0, run.stderr
    out = json.loads(run.stdout)
    # medium now: ] is large, [ is small (the page does not change its class here: medium each time),
    # 1 the list; 3 is the view on show already: nothing pressed
    assert out['log'] == ['size L', 'size S', 'view list']
    # the keyboard back in the shelfmarks once they are drawn again, on the row it was on
    assert [where for _key, where in out['out']] == ['t1', 't1', 't1', 't1']


def test_tab_past_the_last_list_goes_on_and_f6_leaves_the_lists_and_comes_back(tmp_path):
    node = _node_with_playwright()
    if node is None:
        pytest.skip("node with playwright is not installed")
    html = (_PAGE.replace('DIR', 'ltr').replace('ROWS1', _rows('a', 3, on=0)).replace('ROWS2', _rows('a0.', 4))
            .replace('TILES', _tiles(3)).replace('KEYS', _KEYS_JS))
    script = tmp_path / 'f6.js'
    script.write_text(r"""
const { chromium } = require('playwright');
(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage();
  await page.setContent(JSON.parse(process.argv[2]));
  await page.waitForTimeout(60);
  const at = async () => page.evaluate(() => {
    const a = document.activeElement;
    return a ? (a.dataset.node || a.dataset.leaf || a.id || a.dataset.testid || a.tagName) : null;
  });
  const out = [];
  const press = async (key) => { await page.keyboard.press(key); await page.waitForTimeout(30); out.push(await at()); };
  await page.focus('[data-node="a0.0"]');
  await press('F6');          // out: the first field after the lists
  await press('F6');          // back into the list last in use
  await press('Shift+F6');    // out the other way: the field before the lists (the filter passed by)
  await press('F6');
  await page.focus('[data-leaf="t0"]');
  await press('Tab');         // past the last list: the page's own order goes on
  await page.focus('[data-node="a0"]');
  await press('Shift+Tab');   // before the first list: the page's own order (its filter)
  console.log(JSON.stringify(out));
  await browser.close();
})().catch((e) => { console.error(e); process.exit(1); });
""", encoding='utf-8')
    run = subprocess.run([node, str(script), json.dumps(html)], capture_output=True, text=True, timeout=120)
    assert run.returncode == 0, run.stderr
    assert json.loads(run.stdout) == ['shelf-details-toggle', 'a0.0', 'back', 'a0.0', 'shelf-details-toggle', 'f1']


_OPTIONS_PAGE = """<!doctype html><html><head><meta charset="utf-8"></head><body>
<div class="q-dialog"><div class="stp-card" dir="ltr">
  <div class="stp-col"><div class="stp-filter"><input id="f1"></div>
    <div class="stp-list" id="l1">ROWS</div></div>
  <div class="stp-col stp-leaves"><div class="stp-filter"><input id="f2"></div>
    <div class="stp-list" id="l2">OPTIONS</div></div>
</div></div>
<script>
window.log = [];
document.addEventListener('click', (e) => {
  const o = e.target.closest('[data-leaf]');
  if (o) {
    window.log.push('select ' + o.dataset.leaf);
    o.parentElement.querySelectorAll('[data-leaf]').forEach((x) => x.setAttribute('aria-selected', x === o ? 'true' : 'false'));
    return;
  }
  const r = e.target.closest('[data-node]');
  if (r) {
    window.log.push('open ' + r.dataset.node);
    setTimeout(() => {          // the server draws the lists again: the row open, new shelfmarks
      r.parentElement.querySelectorAll('[data-node]').forEach((x) => x.classList.toggle('stp-on', x === r));
      document.getElementById('l2').innerHTML = [0, 1, 2].map((n) =>
        `<div class="stp-row" data-leaf="${r.dataset.node}-${n}" role="option" aria-selected="false" tabindex="-1">${n}</div>`).join('');
    }, 80);
  }
});
</script>
<script>KEYS</script>
<script>setTimeout(() => window.__stpKeys.fix(), 10);</script>
</body></html>"""


def test_the_keyboard_starts_in_a_list_follows_an_opened_row_and_arrows_select(tmp_path):
    node = _node_with_playwright()
    if node is None:
        pytest.skip("node with playwright is not installed")
    options = ''.join(f'<div class="stp-row" data-leaf="o{i}" role="option" aria-selected="false" tabindex="-1">{i}</div>'
                      for i in range(4))
    html = (_OPTIONS_PAGE.replace('ROWS', _rows('a', 3)).replace('OPTIONS', options).replace('KEYS', _KEYS_JS))
    script = tmp_path / 'start.js'
    script.write_text(r"""
const { chromium } = require('playwright');
(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage();
  await page.setContent(JSON.parse(process.argv[2]));
  await page.waitForTimeout(100);
  const at = async () => page.evaluate(() => {
    const a = document.activeElement;
    return a ? (a.dataset.node || a.dataset.leaf || a.id || a.tagName) : null;
  });
  const out = {};
  out.opened = await at();                               // the opening: the list of the next choice
  await page.evaluate(() => document.activeElement.blur());
  await page.keyboard.press('ArrowDown');
  await page.waitForTimeout(30);
  out.fromNowhere = await at();                         // an arrow from nowhere: into that list
  await page.click('[data-node="a1"]');                 // a row opened with the mouse ...
  await page.waitForTimeout(300);
  out.afterClick = await at();                          // ... the keyboard in the list it opened
  await page.focus('[data-leaf="a1-0"]');
  await page.keyboard.press('ArrowDown');
  await page.keyboard.press('ArrowDown');               // two quick steps: the one it rests on is selected
  await page.waitForTimeout(400);
  out.log = await page.evaluate(() => window.log);
  console.log(JSON.stringify(out));
  await browser.close();
})().catch((e) => { console.error(e); process.exit(1); });
""", encoding='utf-8')
    run = subprocess.run([node, str(script), json.dumps(html)], capture_output=True, text=True, timeout=120)
    assert run.returncode == 0, run.stderr
    out = json.loads(run.stdout)
    assert out['opened'] == 'a0'               # no row open in the column: it is the next choice
    assert out['fromNowhere'] == 'a0'
    assert out['afterClick'] == 'a1-0'         # a1 open: its shelfmarks are the next choice
    assert out['log'] == ['open a1', 'select a1-2']


def test_s_rotate_and_w_in_a_browser(tmp_path):
    from pathlib import Path

    from web.components.shelfmark_tree_picker import _ZOOM_JS
    node = _node_with_playwright()
    if node is None:
        pytest.skip("node with playwright is not installed")
    shortcuts = (Path(__file__).resolve().parent.parent / 'web' / 'static' / 'js' / 'shortcuts.js').read_text('utf-8')
    picker_page = (_PAGE.replace('DIR', 'ltr').replace('ROWS1', _rows('a', 2, on=0)).replace('ROWS2', _rows('a0.', 2))
                   .replace('TILES', _tiles(3))
                   .replace('<button data-testid="shelf-details-toggle">',
                            '<button data-testid="shelf-picker-switch">switch</button>'
                            '<button data-testid="shelf-details-toggle">')
                   .replace('<div class="stp-d-zoom" id="c77"><img></div>',
                            '<div class="stp-d-zoom" id="c77"><img style="width:200px;height:100px;display:block"></div>')
                   .replace('window.__stpZoom = {reset: (id) => window.log.push(\'reset \' + id)};', '')
                   .replace('<script>KEYS</script>', '<script>' + _ZOOM_JS + '</script><script>KEYS</script>')
                   .replace('KEYS', _KEYS_JS))
    page_with_w = """<!doctype html><html><body>
<input id="box"><button data-testid="header-shelf-pick" onclick="window.log.push('header')">shelf</button>
<script>window.log = [];</script><script>SHORTCUTS</script></body></html>""".replace('SHORTCUTS', shortcuts)
    script = tmp_path / 'more.js'
    script.write_text(r"""
const { chromium } = require('playwright');
(async () => {
  const [picker, w] = JSON.parse(process.argv[2]);
  const browser = await chromium.launch();
  const page = await browser.newPage();
  await page.setContent(picker);
  await page.waitForTimeout(60);
  const out = {};
  await page.focus('[data-leaf="t1"]');
  await page.keyboard.press('KeyS');
  await page.waitForTimeout(30);
  out.afterS = await page.evaluate(() => window.log.slice());
  await page.evaluate(() => window.__stpZoom.rotate(77, 90));
  out.turned = await page.evaluate(() => document.querySelector('#c77 img').style.transform);
  await page.evaluate(() => window.__stpZoom.rotate(77, -90));
  out.upright = await page.evaluate(() => document.querySelector('#c77 img').style.transform);
  const page2 = await browser.newPage();
  await page2.setContent(w);
  await page2.focus('#box');
  await page2.keyboard.press('KeyW');                   // typing: a W in the box, nothing opened
  await page2.evaluate(() => document.activeElement.blur());
  await page2.keyboard.press('KeyW');                   // elsewhere: the picker's button
  out.w = await page2.evaluate(() => [window.log.slice(), document.querySelector('#box').value]);
  console.log(JSON.stringify(out));
  await browser.close();
})().catch((e) => { console.error(e); process.exit(1); });
""", encoding='utf-8')
    run = subprocess.run([node, str(script), json.dumps([picker_page, page_with_w])], capture_output=True, text=True,
                         timeout=120)
    assert run.returncode == 0, run.stderr
    out = json.loads(run.stdout)
    assert out['afterS'] == ['shelf-picker-switch']
    assert 'rotate(90deg)' in out['turned'] and 'scale(0.5)' in out['turned']   # on its side, fitted to the box
    assert out['upright'] == ''
    assert out['w'] == [['header'], 'w']


def test_the_keys_answer_the_picker_on_screen_not_a_closed_one_still_in_the_page(tmp_path):
    # Codex on #129: a closed picker's card may still be in the page; the first card found was its
    node = _node_with_playwright()
    if node is None:
        pytest.skip("node with playwright is not installed")
    html = (_PAGE.replace('<div class="stp-card"', '<div class="stp-card" style="display:none"><div class="stp-list">'
                          '<div data-node="old" tabindex="-1">old</div></div></div>\n<div class="stp-card"', 1)
            .replace('DIR', 'ltr').replace('ROWS1', _rows('a', 3, on=0)).replace('ROWS2', _rows('a0.', 4))
            .replace('TILES', _tiles(3)).replace('KEYS', _KEYS_JS))
    script = tmp_path / 'second.js'
    script.write_text(r"""
const { chromium } = require('playwright');
(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage();
  await page.setContent(JSON.parse(process.argv[2]));
  await page.waitForTimeout(60);
  await page.focus('[data-node="a0"]');
  await page.keyboard.press('ArrowDown');
  await page.waitForTimeout(30);
  console.log(JSON.stringify(await page.evaluate(() => document.activeElement.dataset.node)));
  await browser.close();
})().catch((e) => { console.error(e); process.exit(1); });
""", encoding='utf-8')
    run = subprocess.run([node, str(script), json.dumps(html)], capture_output=True, text=True, timeout=120)
    assert run.returncode == 0, run.stderr
    assert json.loads(run.stdout) == 'a1'


_LOOK_PAGE = """<!doctype html><html><head><meta charset="utf-8"><style>
body { margin: 0; }
.stp-look { width: 1000px; height: 760px; display: flex; flex-direction: column; }
.stp-look-stage { width: 1000px; height: 700px; display: flex; align-items: center; justify-content: center; }
.stp-d-zoom { width: fit-content; overflow: hidden; }
.stp-d-zoom > img { display: block; max-width: 1000px; max-height: 700px; transform-origin: 0 0; }
</style></head><body>
<div class="stp-look" dir="DIR">
  <div class="stp-look-bar">BUTTONS</div>
  <div class="stp-look-stage"><div class="stp-d-zoom stp-look-zoom" id="c88"><img></div></div>
</div>
<script>
window.log = [];
document.addEventListener('click', (e) => {
  const t = e.target.closest('[data-testid]');
  if (t) window.log.push(t.dataset.testid);
});
</script>
<script>ZOOM</script><script>LOOK</script>
</body></html>"""

_LOOK_IDS = ('shelf-look-prev', 'shelf-look-next', 'shelf-look-page-prev', 'shelf-look-page-next',
             'shelf-look-rotate-left', 'shelf-look-rotate-right', 'shelf-look-zoom-out', 'shelf-look-zoom-in',
             'shelf-look-choose', 'shelf-look-open', 'shelf-look-first', 'shelf-look-last', 'shelf-look-close')


def test_the_quick_look_asks_for_the_window_s_width_and_its_keys_press_its_buttons(tmp_path):
    from web.components.shelfmark_tree_picker import _LOOK_JS, _ZOOM_JS
    node = _node_with_playwright()
    if node is None:
        pytest.skip("node with playwright is not installed")
    buttons = ''.join(f'<button data-testid="{t}">{t}</button>' for t in _LOOK_IDS)
    pages = {d: _LOOK_PAGE.replace('DIR', d).replace('BUTTONS', buttons).replace('ZOOM', _ZOOM_JS)
             .replace('LOOK', _LOOK_JS) for d in ('ltr', 'rtl')}
    picker = (_PAGE.replace('DIR', 'ltr').replace('ROWS1', _rows('a', 2, on=0)).replace('ROWS2', _rows('a0.', 2))
              .replace('TILES', _tiles(3))
              .replace('<button data-testid="shelf-zoom-out">',
                       '<button data-testid="shelf-rotate-left">L</button><button data-testid="shelf-rotate-right">R'
                       '</button><button data-testid="shelf-zoom-out">')
              .replace('KEYS', _KEYS_JS))
    script = tmp_path / 'look.js'
    script.write_text(r"""
const { chromium } = require('playwright');
(async () => {
  const [pages, picker] = JSON.parse(process.argv[2]);
  const browser = await chromium.launch();
  const out = {};
  for (const [dir, html] of Object.entries(pages)) {
    const page = await browser.newPage({viewport: {width: 1000, height: 760}});
    const asked = [];
    await page.route('http://img.test/**', (route) => {
      asked.push(route.request().url().slice('http://img.test/'.length));
      route.fulfill({contentType: 'image/svg+xml',
                     body: '<svg xmlns="http://www.w3.org/2000/svg" width="300" height="400"></svg>'});
    });
    await page.setContent(html);
    await page.evaluate(() => window.__stpLook.show(88, 'http://img.test/low',
      'http://img.test/p1?width=__W__', ['http://img.test/p2?width=__W__', 'http://img.test/n1?width=__W__'],
      {fresh: true}));
    await page.waitForFunction(() => (document.querySelector('#c88 img').getAttribute('src') || '').includes('p1'));
    const src = await page.evaluate(() => document.querySelector('#c88 img').getAttribute('src'));
    for (const key of ['ArrowRight', 'ArrowLeft', 'Control+ArrowRight', 'Shift+ArrowLeft', 'ArrowDown',
                       'PageUp', 'Home', 'End', '+', '-', 'KeyR', 'Shift+KeyR', 'Space', 'Enter', 'KeyQ']) {
      await page.keyboard.press(key);
    }
    out[dir] = {asked, src, log: await page.evaluate(() => window.log)};
    await page.close();
  }
  const page = await browser.newPage();
  await page.setContent(picker);
  await page.waitForTimeout(60);
  await page.evaluate(() => document.querySelector('.stp-card').addEventListener('stp-look',
    (e) => window.log.push('look ' + e.detail)));
  await page.focus('[data-leaf="t1"]');
  await page.keyboard.press('KeyQ');
  await page.keyboard.press('KeyR');
  await page.keyboard.press('Shift+KeyR');
  await page.keyboard.press('Shift+ArrowRight');
  out.picker = await page.evaluate(() => window.log);
  console.log(JSON.stringify(out));
  await browser.close();
})().catch((e) => { console.error(e); process.exit(1); });
""", encoding='utf-8')
    run = subprocess.run([node, str(script), json.dumps([pages, picker])], capture_output=True, text=True,
                         timeout=180)
    assert run.returncode == 0, run.stderr
    out = json.loads(run.stdout)
    for direction in ('ltr', 'rtl'):
        seen = out[direction]
        # 300 x 400 at hand, a box of 1000 x 700: fitted by its height, 525 wide, asked in a step of 200
        assert seen['asked'][0] == 'low', direction
        assert set(seen['asked'][1:]) == {'p1?width=600', 'p2?width=600', 'n1?width=600'}, direction
        assert seen['src'] == 'http://img.test/p1?width=600', direction
        nxt, prev = ('shelf-look-next', 'shelf-look-prev') if direction == 'ltr' else ('shelf-look-prev', 'shelf-look-next')
        pnext, pprev = (('shelf-look-page-next', 'shelf-look-page-prev') if direction == 'ltr'
                        else ('shelf-look-page-prev', 'shelf-look-page-next'))
        assert seen['log'] == [nxt, prev, pnext, pprev, 'shelf-look-page-next', 'shelf-look-page-prev',
                               'shelf-look-first', 'shelf-look-last', 'shelf-look-zoom-in', 'shelf-look-zoom-out',
                               'shelf-look-rotate-right', 'shelf-look-rotate-left', 'shelf-look-choose',
                               'shelf-look-open', 'shelf-look-close'], direction
    assert out['picker'] == ['look t1', 'shelf-rotate-right', 'shelf-rotate-left', 'shelf-page-next']


def test_the_quick_look_script_parses():
    from web.components.shelfmark_tree_picker import _LOOK_JS
    node = shutil.which('node')
    if node is None:
        pytest.skip('node is not installed')
    out = subprocess.run([node, '--check', '-'], input=_LOOK_JS, text=True, capture_output=True, timeout=60)
    assert out.returncode == 0, out.stderr


def test_the_quick_look_waits_with_a_spinner_and_never_shows_a_broken_picture(tmp_path):
    # owner, 2026-11-25: a broken image showed for 3 seconds; the picture at hand blurred, a spinner, instead
    from web.components.shelfmark_tree_picker import _CSS, _LOOK_JS, _ZOOM_JS
    node = _node_with_playwright()
    if node is None:
        pytest.skip("node with playwright is not installed")
    html = (_LOOK_PAGE.replace('</style>', '</style><style>' + _CSS + '</style>', 1)
            .replace('DIR', 'ltr').replace('BUTTONS', '').replace('ZOOM', _ZOOM_JS).replace('LOOK', _LOOK_JS)
            .replace('<div class="stp-look-stage">', '<div class="stp-look-stage" id="stage">'))
    script = tmp_path / 'wait.js'
    script.write_text(r"""
const { chromium } = require('playwright');
(async () => {
  const [html] = JSON.parse(process.argv[2]);
  const browser = await chromium.launch();
  const page = await browser.newPage({viewport: {width: 1000, height: 760}});
  let release;
  const held = new Promise((r) => { release = r; });
  await page.route('http://img.test/**', async (route) => {
    const url = route.request().url();
    if (url.includes('broken')) { route.fulfill({status: 404, body: ''}); return; }
    if (url.includes('big')) await held;
    route.fulfill({contentType: 'image/svg+xml',
                   body: '<svg xmlns="http://www.w3.org/2000/svg" width="300" height="400"></svg>'});
  });
  await page.setContent(html);
  const state = () => page.evaluate(() => ({
    wait: document.getElementById('stage').classList.contains('stp-look-wait'),
    src: document.querySelector('#c88 img').getAttribute('src'),
    hidden: getComputedStyle(document.querySelector('#c88')).visibility === 'hidden'}));
  const out = {};
  await page.evaluate(() => window.__stpLook.show(88, '', '', [], {fresh: true}));     // the list not known yet
  await page.waitForTimeout(50);
  out.opening = await state();
  await page.evaluate(() => window.__stpLook.show(88, 'http://img.test/broken', 'http://img.test/big?width=__W__', [],
                                                  {fresh: true}));
  await page.waitForTimeout(150);
  out.broken = await state();
  release();
  await page.waitForFunction(() => !document.getElementById('stage').classList.contains('stp-look-wait'));
  out.done = await state();
  await page.evaluate(() => window.__stpLook.show(88, '', '', [], {fresh: true, none: true}));
  await page.waitForTimeout(50);
  out.none = await state();
  console.log(JSON.stringify(out));
  await browser.close();
})().catch((e) => { console.error(e); process.exit(1); });
""", encoding='utf-8')
    run = subprocess.run([node, str(script), json.dumps([html])], capture_output=True, text=True, timeout=120)
    assert run.returncode == 0, run.stderr
    out = json.loads(run.stdout)
    assert out['opening'] == {'wait': True, 'src': None, 'hidden': True}
    assert out['broken'] == {'wait': True, 'src': None, 'hidden': True}          # no broken picture, the spinner
    assert out['done']['wait'] is False and 'big?width=' in out['done']['src'] and not out['done']['hidden']
    assert out['none'] == {'wait': False, 'src': None, 'hidden': True}
