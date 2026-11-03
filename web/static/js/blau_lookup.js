/* The Blau dictionary companion on /browse: the click side (plan §6.2).
 *
 * On a page where the dictionary is served, the browse page creates one
 * element `.gs-blau-popover` and marks the transcription pane with
 * `data-blau-lookup`. This script, on any click:
 *   - inside the popover: nothing (its links and buttons are its own);
 *   - on a word of the marked pane: unwraps the previous mark, wraps the word
 *     in `span.blau-word-hit`, places the popover beside the word (CSS puts
 *     it at the foot of a phone instead) and dispatches `blau-lookup` on the
 *     popover with `detail` = JSON {word};
 *   - anywhere else, or on no word: unwraps the mark and dispatches
 *     `blau-close` (the page hides the popover);
 *   - on the panel's own close control (`.blau-close`): unwraps the mark and
 *     leaves the hiding to the page, which owns that button.
 * The word is what sits between two non-word characters in the text node
 * under the pointer: Hebrew letters, combining marks, the apostrophe and
 * geresh family the transcriptions write the Judeo-Arabic letters with.
 * A click that lands beside a word (padding, a line number) is no word.
 * Nothing here changes what the page says: the page asks the dictionary.
 */
(function () {
  'use strict';

  var POPOVER = '.gs-blau-popover';
  var PANE = '[data-blau-lookup]';
  var HIT = 'blau-word-hit';
  var CLOSE = 'blau-close';   // the panel's own close control (the page hides the popover)
  // Hebrew letters; the Hebrew combining marks (U+0591-05BD, 05BF, 05C1-05C2,
  // 05C4-05C5, 05C7 -- NOT the punctuation between them: maqaf 05BE, paseq
  // 05C0, sof pasuq 05C3, nun hafukha 05C6, which end a word as they do for
  // shared/blau_normalize.py); the combining diacriticals; the apostrophe and
  // geresh family the transcriptions write the Judeo-Arabic letters with.
  var WORD_CHAR = /[א-ת֑-ׇֽֿׁׂׅׄ̀-ͯ'"׳״‘’]/;
  var WIDTH = 440;   // keep in step with .gs-blau-popover in common.css
  var HEIGHT = 380;
  var GAP = 8;
  var MARGIN = 16;

  function caretAt(x, y) {
    if (document.caretPositionFromPoint) {
      var p = document.caretPositionFromPoint(x, y);
      return p ? { node: p.offsetNode, offset: p.offset } : null;
    }
    if (document.caretRangeFromPoint) {
      var r = document.caretRangeFromPoint(x, y);
      return r ? { node: r.startContainer, offset: r.startOffset } : null;
    }
    return null;
  }

  function wordAt(node, offset) {
    if (!node || node.nodeType !== 3) return null;
    var text = node.data;
    var start = offset, end = offset;
    while (start > 0 && WORD_CHAR.test(text.charAt(start - 1))) start--;
    while (end < text.length && WORD_CHAR.test(text.charAt(end))) end++;
    if (start === end) return null;
    var word = text.slice(start, end);
    if (!/[א-ת]/.test(word)) return null;  // marks alone are no word
    return { start: start, end: end, text: word };
  }

  function clearHit() {
    var marks = document.querySelectorAll('.' + HIT);
    for (var i = 0; i < marks.length; i++) {
      var mark = marks[i];
      var parent = mark.parentNode;
      if (!parent) continue;
      while (mark.firstChild) parent.insertBefore(mark.firstChild, mark);
      parent.removeChild(mark);
      parent.normalize();
    }
  }

  function markWord(node, word) {
    try {
      var range = document.createRange();
      range.setStart(node, word.start);
      range.setEnd(node, word.end);
      var span = document.createElement('span');
      span.className = HIT;
      range.surroundContents(span);
      return span.getBoundingClientRect();
    } catch (e) {
      return null;
    }
  }

  function place(pop, rect) {
    var width = Math.min(WIDTH, window.innerWidth - 2 * MARGIN);
    var left = Math.round(rect.left + rect.width / 2 - width / 2);
    left = Math.max(MARGIN, Math.min(left, window.innerWidth - width - MARGIN));
    var below = rect.bottom + GAP + HEIGHT <= window.innerHeight || rect.top - GAP - HEIGHT < 0;
    pop.style.setProperty('--blau-left', left + 'px');
    pop.style.setProperty('--blau-top', Math.round(rect.bottom + GAP) + 'px');
    pop.style.setProperty('--blau-bottom', Math.round(window.innerHeight - rect.top + GAP) + 'px');
    pop.setAttribute('data-blau-place', below ? 'below' : 'above');
  }

  function dispatch(pop, name, detail) {
    pop.dispatchEvent(new CustomEvent(name, { detail: JSON.stringify(detail || {}), bubbles: false }));
  }

  document.addEventListener('click', function (event) {
    var pop = document.querySelector(POPOVER);
    if (!pop) return;                       // no dictionary on this page
    if (pop.contains(event.target)) {       // the panel's own controls
      var closing = event.target.nodeType === 1 ? event.target.closest('.' + CLOSE) : null;
      if (closing) clearHit();              // the page hides the popover; the word's mark goes with it
      return;
    }
    var target = event.target && event.target.nodeType === 1 ? event.target : event.target && event.target.parentElement;
    var pane = target ? target.closest(PANE) : null;
    // Unwrap the previous mark BEFORE reading the caret: unwrapping merges the
    // text nodes it had split, and a caret taken first would point into a node
    // that no longer exists (a second click on the same line found no word).
    clearHit();
    var caret = pane ? caretAt(event.clientX, event.clientY) : null;
    var word = caret && pane && pane.contains(caret.node) ? wordAt(caret.node, caret.offset) : null;
    if (!word) {
      dispatch(pop, 'blau-close');
      return;
    }
    var rect = markWord(caret.node, word) ||
      { left: event.clientX, right: event.clientX, top: event.clientY, bottom: event.clientY, width: 0 };
    place(pop, rect);
    dispatch(pop, 'blau-lookup', { word: word.text });
  });
})();
