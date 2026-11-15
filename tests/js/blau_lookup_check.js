'use strict';
// Exercise the real extractor with a small DOM model; no browser or dictionary data required.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../../web/static/js/blau_lookup.js'), 'utf8');
class Text {
  constructor(data) { this.nodeType = 3; this.data = data; this.parentNode = null; }
  get textContent() { return this.data; }
  get parentElement() { return this.parentNode; }
}
class Element {
  constructor(tag, children = []) {
    this.nodeType = 1; this.tagName = tag.toUpperCase(); this.childNodes = [];
    this.className = ''; this.parentNode = null;
    this.classList = { contains: cls => this.className.split(' ').includes(cls) };
    for (const child of children) this.appendChild(typeof child === 'string' ? new Text(child) : child);
  }
  get firstChild() { return this.childNodes[0] || null; }
  get textContent() { return this.childNodes.map(n => n.textContent).join(''); }
  get parentElement() { return this.parentNode; }
  appendChild(n) { if (n.parentNode) n.parentNode.removeChild(n); this.childNodes.push(n); n.parentNode = this; }
  insertBefore(n, ref) {
    if (n.parentNode) n.parentNode.removeChild(n);
    this.childNodes.splice(this.childNodes.indexOf(ref), 0, n); n.parentNode = this;
  }
  removeChild(n) { this.childNodes.splice(this.childNodes.indexOf(n), 1); n.parentNode = null; }
  normalize() {
    for (let i = 1; i < this.childNodes.length; i++) {
      const a = this.childNodes[i - 1], b = this.childNodes[i];
      if (a.nodeType === 3 && b.nodeType === 3) { a.data += b.data; this.removeChild(b); i--; }
    }
  }
  closest() { return this.pane || (this.parentNode && this.parentNode.closest()); }
}
const el = (tag, ...children) => new Element(tag, children);
let pane, selection;
const rect = { left: 10, right: 100, top: 10, bottom: 30, width: 90, height: 20 };
function all(n) { return [n, ...(n.childNodes || []).flatMap(all)]; }
const listeners = {};
const document = {
  addEventListener(name, callback) { listeners[name] = callback; }, createElement: tag => el(tag),
  querySelectorAll: () => all(pane).filter(n => n.className === 'blau-word-hit'),
  createRange() {
    return {
      setStart(n, offset) { this.start = n; this.from = offset; },
      setEnd(n, offset) { this.end = n; this.to = offset; },
      getBoundingClientRect() { return rect; },
      surroundContents(span) {
        assert.equal(this.start, this.end, 'highlight one text node at a time');
        const n = this.start, parent = n.parentNode;
        parent.insertBefore(new Text(n.data.slice(0, this.from)), n);
        span.appendChild(new Text(n.data.slice(this.from, this.to)));
        parent.insertBefore(span, n);
        parent.insertBefore(new Text(n.data.slice(this.to)), n);
        parent.removeChild(n);
      },
    };
  },
};
const context = vm.createContext({ document, window: {
  matchMedia: () => ({ matches: false }), addEventListener() {}, getSelection: () => selection,
} });
vm.runInContext(source.replace(/\}\)\(\);\s*$/, 'globalThis.check = {wordAt, wordsBeside, cleanWord, selectedWords, markWord, clearHit};})();'), context);
const api = context.check;
assert.ok(api, 'load the shipped IIFE');
const plain = value => JSON.parse(JSON.stringify(value));
function root(...children) { pane = el('div', ...children); pane.pane = pane; return pane; }
function picked(node, offset) { return api.wordAt(pane, node, offset); }
let checks = 0;
for (const spelling of ['וסלאמת[ך', 'סלמא[תך', 'סלאמ[תך]', '[סלאמתך]', ']הנתשנ', 'חיט(ה̈)', 'ג̇דّ']) {
  root('לפני ', spelling, ' אחרי');
  const node = pane.childNodes[1];
  for (let offset = 0; offset <= spelling.length; offset++) {
    assert.equal(picked(node, offset).text, spelling, spelling + ' at ' + offset);
  }
  checks++;
}
// Formatting can split the brackets or letters into several nodes. Preserve every element.
const emphasis = el('em', '[ת');
const ending = el('b', 'ך]');
root('לפני סלאמ', emphasis, ending, ' אחרי');
const original = pane.textContent;
let word = picked(emphasis.firstChild, 1);
assert.equal(word.text, 'סלאמ[תך]');
assert.deepEqual(plain(api.wordsBeside(word)), { before: ['לפני'], after: ['אחרי'] });
assert.deepEqual(plain(api.markWord(word)), rect);
assert.equal(pane.textContent, original, 'highlight preserves the text');
assert.ok(pane.childNodes.includes(emphasis) && pane.childNodes.includes(ending), 'keep formatting nodes');
assert.equal(document.querySelectorAll().length, 3, 'one mark per text slice');
api.clearHit();
assert.equal(pane.textContent, original, 'clearing highlights preserves the text');
assert.equal(document.querySelectorAll().length, 0);
assert.equal(picked(ending.firstChild, 0).text, 'סלאמ[תך]', 'repeat click after clearing');
checks++;
// Spaces, lacunae, punctuation and block boundaries are never transparent.
for (const separator of [' ', '[ ]', '[]', '[...]', '…', '־', '.', ':']) {
  root('אחד' + separator + 'שני');
  const node = pane.firstChild;
  assert.ok(!picked(node, 1).text.includes('שני'), separator);
  assert.ok(!picked(node, node.data.length - 1).text.includes('אחד'), separator);
  checks++;
}
for (const tag of ['br', 'div', 'p', 'li', 'td']) {
  root('אחד', el(tag, tag === 'br' ? '' : 'שני'), 'שלישי');
  assert.equal(picked(pane.firstChild, 1).text, 'אחד', tag + ' start');
  assert.equal(picked(pane.childNodes[2], 1).text, 'שלישי', tag + ' end');
  checks++;
}
const gutter = el('span', '12'); gutter.className = 'line-number-gutter';
root('אחד', gutter, 'שני');
assert.equal(picked(gutter.firstChild, 1), null, 'line number not searchable');
assert.equal(picked(pane.firstChild, 1).text, 'אחד');
assert.equal(picked(pane.childNodes[2], 1).text, 'שני');
checks++;
root('אטאל סלאמ[תך] ', el('em', 'אללה'), ' וברכ[תך] דאימא');
word = picked(pane.childNodes[1].firstChild, 2);
assert.deepEqual(plain(api.wordsBeside(word)), { before: ['אטאל', 'סלאמ[תך]'], after: ['וברכ[תך]', 'דאימא'] });
checks++;
for (const gap of ['[]', '[ ]', '[...]', '…']) {
  root('לפני ' + gap + ' מילה ' + gap + ' אחרי');
  word = picked(pane.firstChild, pane.firstChild.data.indexOf('מילה') + 1);
  assert.deepEqual(plain(api.wordsBeside(word)), { before: [], after: [] }, gap);
  checks++;
}
root('לפני אחד[]שני אחרי');
assert.deepEqual(plain(api.wordsBeside(picked(pane.firstChild, 12))), { before: [], after: ['אחרי'] });
assert.deepEqual(plain(api.wordsBeside(picked(pane.firstChild, 7))), { before: ['לפני'], after: [] });
checks++;
for (const spelling of ['וסלאמת[ך', '[סלאמתך]', 'סלמא[תך', 'סלאמ[תך] אללה']) {
  root(spelling);
  selection = { isCollapsed: false, rangeCount: 1, toString: () => spelling,
    getRangeAt: () => ({ commonAncestorContainer: pane, getBoundingClientRect: () => rect }) };
  assert.equal(api.selectedWords(false).text, spelling);
  checks++;
}
root('אחד[]שני');
selection = { isCollapsed: false, rangeCount: 1, toString: () => 'אחד[]שני',
  getRangeAt: () => ({ commonAncestorContainer: pane, getBoundingClientRect: () => rect }) };
assert.equal(api.selectedWords(false), null, 'selection must not concatenate missing letters');
checks++;
const longSelection = 'אחד שני שלישי רביעי חמישי שישי שביעי שמיני תשיעי עשירי אחדעשר שניםעשר שלושהעשר';
root(longSelection);
selection = { isCollapsed: false, rangeCount: 1, toString: () => longSelection,
  getRangeAt: () => ({ commonAncestorContainer: pane, getBoundingClientRect: () => rect }) };
assert.equal(api.selectedWords(false).text, longSelection, 'send the full selection; server explains its limit');
checks++;
root('ואנה אדא והב אללה לה אלעאפיה וכרג');
word = picked(pane.firstChild, pane.textContent.indexOf('אלעאפיה') + 1);
assert.deepEqual(plain(api.wordsBeside(word)), { before: ['ואנה', 'אדא', 'והב', 'אללה', 'לה'], after: ['וכרג'] });
checks++;
// Portalled gap options are part of the panel. A close dispatch would throw here.
document.querySelector = () => ({ contains: () => false, dispatchEvent: () => { throw Error('closed'); } });
listeners.click({target: {nodeType: 1, closest: selector => selector === '.dict-gap-menu' ? {} : null}});
checks++;
console.log(checks + ' dictionary reading checks passed');
