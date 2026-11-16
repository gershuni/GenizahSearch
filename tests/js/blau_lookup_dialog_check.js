'use strict';
// The click script inside a dialog (the search page's Quick View) and on a page (/browse), with a
// small DOM and event model: which overlay stands the script down, where the phone chip lives, and
// that an Esc closing the panel in a dialog goes no further (neither Quasar's dialog nor the search
// page's own Esc handler sees it), while an Esc with nothing to close passes on.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../../web/static/js/blau_lookup.js'), 'utf8');

function matches(n, sel) {
  if (sel.startsWith('.')) return n.classList.contains(sel.slice(1));
  if (sel.startsWith('[')) return n.attrs && sel.slice(1, -1) in n.attrs;
  return n.tagName === sel.toUpperCase();
}
class Element {
  constructor(tag, cls = '', attrs = {}) {
    this.nodeType = 1; this.tagName = tag.toUpperCase(); this.className = cls; this.attrs = attrs;
    this.childNodes = []; this.parentNode = null; this.style = { display: '' }; this.events = [];
    const self = this;
    this.classList = {
      contains: c => self.className.split(' ').includes(c),
      toggle: (c, on) => { const all = self.className.split(' ').filter(x => x && x !== c); if (on) all.push(c); self.className = all.join(' '); },
    };
  }
  get parentElement() { return this.parentNode; }
  get offsetWidth() { return 80; }
  get offsetHeight() { return 30; }
  appendChild(n) { if (n.parentNode) n.parentNode.removeChild(n); this.childNodes.push(n); n.parentNode = this; return n; }
  removeChild(n) { this.childNodes.splice(this.childNodes.indexOf(n), 1); n.parentNode = null; }
  contains(n) { for (let x = n; x; x = x.parentNode) if (x === this) return true; return false; }
  closest(sel) { for (let x = this; x && x.nodeType === 1; x = x.parentNode) if (matches(x, sel)) return x; return null; }
  getAttribute(a) { return a in this.attrs ? this.attrs[a] : null; }
  setAttribute(a, v) { this.attrs[a] = v; }
  addEventListener() {}
  dispatchEvent(e) { this.events.push(e.type); }
}
function all(n) { return [n, ...n.childNodes.flatMap(all)]; }
const html = new Element('html');
const body = html.appendChild(new Element('body'));
function listen(target) {
  const list = { capture: [], bubble: [] };
  target.addEventListener = (name, fn, capture) => list[capture ? 'capture' : 'bubble'].push({ name, fn });
  return list;
}
const document = {
  body, documentElement: html,
  querySelector: sel => all(html).find(n => matches(n, sel)) || null,
  querySelectorAll: sel => all(html).filter(n => matches(n, sel)),
  createElement: tag => new Element(tag),
};
html.style.setProperty = () => {};
let selection = null;
const window = { matchMedia: () => ({ matches: false }), getSelection: () => selection, innerWidth: 1200, innerHeight: 800 };
const onDocument = listen(document), onWindow = listen(window);
class CustomEvent { constructor(type, init) { this.type = type; this.detail = init && init.detail; } }
const context = vm.createContext({ document, window, CustomEvent });
vm.runInContext(source.replace(/\}\)\(\);\s*$/,
  'globalThis.check = {overlayOpen, dialogOf, chipHost, showChip};})();'), context);
const api = context.check;
assert.ok(api, 'load the shipped IIFE');

// What the search page and Quasar listen with, registered after the script (as on the page).
const seen = [];
onDocument.bubble.push({ name: 'keydown', fn: e => { if (e.key === 'Escape') seen.push('ui.keyboard'); } });
onWindow.bubble.push({ name: 'keydown', fn: e => { if (e.key === 'Escape') seen.push('quasar-down'); } });
onWindow.bubble.push({ name: 'keyup', fn: e => { if (e.key === 'Escape') seen.push('quasar-up'); } });

// window capture, document capture, document bubble, window bubble; stopImmediatePropagation ends it.
function key(type, k, target) {
  let stopped = false;
  const event = {
    type, key: k, target: target || body, code: '', repeat: false, prevented: false,
    preventDefault() { this.prevented = true; }, stopImmediatePropagation() { stopped = true; },
    stopPropagation() { stopped = true; },
  };
  for (const phase of [onWindow.capture, onDocument.capture, onDocument.bubble, onWindow.bubble]) {
    for (const l of phase.filter(x => x.name === type)) { l.fn(event); if (stopped) return event; }
  }
  return event;
}
function esc() { seen.length = 0; const down = key('keydown', 'Escape'); key('keyup', 'Escape'); return down; }
function reset() { body.childNodes.slice().forEach(n => body.removeChild(n)); }
let checks = 0;

// /browse: the popover in the page.
reset();
let pop = body.appendChild(new Element('div', 'gs-blau-popover'));
assert.equal(api.dialogOf(pop), null);
assert.equal(api.overlayOpen(pop), false, 'nothing open');
assert.equal(api.chipHost(pop), body, 'the chip lives in the body on a page');
let dialog = body.appendChild(new Element('div', 'q-dialog'));
assert.equal(api.overlayOpen(pop), true, 'a dialog over the page stands it down');
body.removeChild(dialog);
let menu = body.appendChild(new Element('div', 'q-menu'));
assert.equal(api.overlayOpen(pop), true, 'a menu stands it down');
body.removeChild(menu);
pop.style.display = 'flex';
let e = esc();
assert.deepEqual(pop.events, ['blau-close'], 'Esc closes the panel on a page');
assert.ok(e.prevented);
assert.deepEqual(seen, ['ui.keyboard', 'quasar-down', 'quasar-up'], 'and passes on there, as before');
checks++;

// Quick View: the popover inside the dialog's card.
reset();
dialog = body.appendChild(new Element('div', 'q-dialog'));
const card = dialog.appendChild(new Element('div', 'q-card'));
pop = card.appendChild(new Element('div', 'gs-blau-popover'));
pop.style.display = 'none';
assert.equal(api.dialogOf(pop), dialog);
assert.equal(api.overlayOpen(pop), false, 'the dialog the panel is in does not stand it down');
assert.equal(api.chipHost(pop), card, "the chip lives beside the panel, inside the dialog");
menu = body.appendChild(new Element('div', 'q-menu'));
assert.equal(api.overlayOpen(pop), true, 'a menu over Quick View stands it down');
body.removeChild(menu);
const second = body.appendChild(new Element('div', 'q-dialog'));
assert.equal(api.overlayOpen(pop), true, 'a second dialog over Quick View stands it down');
assert.equal(api.overlayOpen(null), true, 'no panel: any dialog stands it down');
body.removeChild(second);
checks++;

// Esc with nothing to close: passes on, Quick View closes as today.
e = esc();
assert.deepEqual(pop.events, []);
assert.deepEqual(seen, ['ui.keyboard', 'quasar-down', 'quasar-up'], 'Esc with no panel reaches the dialog');
// Esc with the panel shown: closes the panel only.
pop.style.display = 'flex';
e = esc();
assert.deepEqual(pop.events, ['blau-close'], 'Esc closes the panel');
assert.ok(e.prevented);
assert.deepEqual(seen, [], 'neither the search page nor Quasar sees that Esc, down or up');
// A menu above the panel (the stem list, the version chooser): its Esc is its own.
pop.events.length = 0;
menu = body.appendChild(new Element('div', 'q-menu'));
esc();
assert.deepEqual(pop.events, [], 'the menu keeps its Esc');
assert.deepEqual(seen, ['ui.keyboard', 'quasar-down', 'quasar-up']);
body.removeChild(menu);
// The next Esc after the panel closed reaches the dialog again.
pop.style.display = 'none';
esc();
assert.deepEqual(seen, ['ui.keyboard', 'quasar-down', 'quasar-up'], 'an Esc used once is not used again');
checks++;

// Esc from the panel's own box in Quick View: closed there too, and no further.
pop.style.display = 'flex';
pop.events.length = 0;
const box = pop.appendChild(new Element('input'));
seen.length = 0;
key('keydown', 'Escape', box);
key('keyup', 'Escape', box);
assert.deepEqual(pop.events, ['blau-close'], 'Esc from the panel box closes the panel');
assert.deepEqual(seen, [], 'and goes no further');
// The window lost the focus between an Esc's keydown and its keyup: the next keyup is not used up.
pop.style.display = 'flex';
seen.length = 0;
key('keydown', 'Escape');
for (const l of onWindow.bubble.filter(x => x.name === 'blur')) l.fn({});
key('keyup', 'Escape');
assert.deepEqual(seen, ['quasar-up'], 'after a blur, a keyup is no longer swallowed');
pop.style.display = 'none';
checks++;

// The phone chip, drawn by the script for words selected in the text: inside the dialog, beside the
// panel (in the body it sits under Quick View).
function selectIn(paneEl, text) {
  const node = paneEl.appendChild(new Element('span'));
  selection = { isCollapsed: false, rangeCount: 1, toString: () => text,
    getRangeAt: () => ({ commonAncestorContainer: node, getBoundingClientRect: () => ({ left: 10, right: 60, top: 100, bottom: 120, width: 50, height: 20 }) }) };
}
const pane = card.appendChild(new Element('div', '', { 'data-blau-pane': '', 'data-blau-chip': 'Look up' }));
selectIn(pane, 'אלכתאב');
api.showChip();
const chip = all(html).find(n => n.className === 'blau-chip');
assert.ok(chip, 'the chip is drawn');
assert.equal(chip.parentNode, card, 'the chip is drawn inside the dialog, beside the panel');
assert.equal(chip.style.display, 'block');
checks++;

// Closed and opened again: a new dialog and card; the chip moves along. On a page it is in the body.
reset();
const dialog2 = body.appendChild(new Element('div', 'q-dialog'));
const card2 = dialog2.appendChild(new Element('div', 'q-card'));
pop = card2.appendChild(new Element('div', 'gs-blau-popover'));
assert.equal(api.chipHost(pop), card2);
const pane2 = card2.appendChild(new Element('div', '', { 'data-blau-pane': '' }));
selectIn(pane2, 'כתאב');
api.showChip();
assert.equal(chip.parentNode, card2, 'the chip followed the panel to the dialog opened again');
reset();
pop = body.appendChild(new Element('div', 'gs-blau-popover'));
const pane3 = body.appendChild(new Element('div', '', { 'data-blau-pane': '' }));
selectIn(pane3, 'כתאב');
api.showChip();
assert.equal(chip.parentNode, body, 'on a page the chip is in the body, as before');
checks++;

console.log(`dictionary dialog checks passed: ${checks}`);
