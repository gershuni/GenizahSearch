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
 *     leaves the hiding to the page, which owns that button;
 *   - on the "type a word" button (`.blau-open`, beside the hint): unwraps the
 *     mark, places the popover beside the button and dispatches `blau-open`
 *     (the page draws the panel with an empty box). The page gives that button
 *     no click handler of its own: a server click and this script's close
 *     would race;
 *   - the close is dispatched only when there is something to close (the
 *     popover shown, or a word marked): with the lookup switched off the pane
 *     is not marked, and a click on the page asks the server nothing.
 * The reader's switch (blau_panel.PREF_KEY) is the pane's attribute: the page
 * adds or removes it, and this script reads it on every click.
 * The look-up key (owner, 2026-11-05), the switch on or off, in a pane the page
 * marks `data-blau-pane` wherever a dictionary is served:
 *   - D or M tapped arms the next click: it looks up the word under the pointer
 *     (`how: 'key'`); held with a mouse, every click while it is down. Alt + a
 *     click does the same (`how: 'alt'`): a laptop touchpad ignores input while
 *     a letter key is held, but not while Alt is;
 *   - D or M pressed while words of the pane are selected, or a selection made
 *     with the key armed or Alt held, looks up the selection (`how:
 *     'selection'`): its first words that hold Hebrew letters, without what
 *     stands around them (line numbers, punctuation);
 *   - on a phone (no hover), selected words show a "Look up" chip under them
 *     (`how: 'chip'`), its label from the pane's `data-blau-chip`;
 *   - Esc closes the panel and disarms the key.
 * D and M are the keys by name, the Hebrew letter on a Hebrew layout (`מ`), or
 * the physical keys on any layout; never while a box has the focus, while a
 * dialog or menu is open, or with Ctrl, Alt or Cmd (those are the browser's).
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
  var KEY_PANE = '[data-blau-pane]';   // where the look-up key works, the switch on or off
  var SELECTION_WORDS = 6;            // a selection is looked up by its first words
  var HIT = 'blau-word-hit';
  var CLOSE = 'blau-close';   // the panel's own close control (the page hides the popover)
  var OPEN = 'blau-open';     // the "type a word" button (this script opens the panel)
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
    // On <html>, NOT on the popover: the page shows the popover by setting its
    // style ('display: flex'), and that re-render replaces the popover's whole
    // inline style -- the position written there was wiped before the panel
    // appeared, and it always opened at the fallback corner. Custom properties
    // inherit, so the stylesheet's var(--blau-left) reads these.
    var root = document.documentElement.style;
    root.setProperty('--blau-left', left + 'px');
    root.setProperty('--blau-top', Math.round(rect.bottom + GAP) + 'px');
    root.setProperty('--blau-bottom', Math.round(window.innerHeight - rect.top + GAP) + 'px');
    pop.setAttribute('data-blau-place', below ? 'below' : 'above');
  }

  function dispatch(pop, name, detail) {
    pop.dispatchEvent(new CustomEvent(name, { detail: JSON.stringify(detail || {}), bubbles: false }));
  }

  function shown(pop) {
    return pop.style.display !== 'none';
  }

  // The look-up key (owner, 2026-11-05): D or M. Tapped, it arms the NEXT click: that click
  // looks up the word under it, and is used up wherever it lands. Held (with a mouse), every
  // click while it is down looks up, and letting it go disarms. Alt + a click does the same
  // without a letter: on a laptop Windows ignores the touchpad while a letter key is held
  // (the pointer stops, taps are dropped -- the owner's report on M), but not for Alt.
  // Held or tapped is told by the key's own down and up, not by auto-repeat: a click can come
  // before the first repeat (Codex, PR #484).
  var armed = false;
  var down = false;                     // D or M is down now: its keydown seen, its keyup not yet
  var held = false;                     // ...and it auto-repeated, or a click came while it was down
  var ARMED = 'blau-armed';             // on <html> while armed: the pointer says so over the text

  function isLookupKey(event) {
    return event.code === 'KeyD' || event.code === 'KeyM' || event.key === 'd' || event.key === 'D' ||
      event.key === 'm' || event.key === 'M' || event.key === 'מ';
  }

  function arm(on) {
    armed = on;
    document.documentElement.classList.toggle(ARMED, on);
  }

  // A dialog or menu above the page (the shortcuts card, a confirmation, the account menu):
  // its keys are its own (Codex, PR #484). The dictionary panel is neither.
  function overlayOpen() {
    return document.querySelector('.q-dialog, .q-menu') !== null;
  }

  function inABox(el) {
    if (!el || el.nodeType !== 1) return false;
    var tag = el.tagName;
    return tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT' || el.isContentEditable;
  }

  function paneOf(node, viaKey) {
    var el = node && node.nodeType === 1 ? node : node && node.parentElement;
    if (!el) return null;
    return el.closest(PANE) || (viaKey ? el.closest(KEY_PANE) : null);
  }

  // The selected words of a pane: {text, rect}, or null when nothing of a pane
  // is selected or the selection holds no Hebrew letters.
  function selectedWords(viaKey) {
    var sel = window.getSelection ? window.getSelection() : null;
    if (!sel || sel.isCollapsed || !sel.rangeCount) return null;
    var range = sel.getRangeAt(0);
    if (!paneOf(range.commonAncestorContainer, viaKey)) return null;
    var words = [];
    var parts = sel.toString().split(/\s+/);
    for (var i = 0; i < parts.length && words.length < SELECTION_WORDS; i++) {
      var part = parts[i];
      var start = 0, end = part.length;
      while (start < end && !WORD_CHAR.test(part.charAt(start))) start++;
      while (end > start && !WORD_CHAR.test(part.charAt(end - 1))) end--;
      part = part.slice(start, end);
      if (/[א-ת]/.test(part)) words.push(part);
    }
    if (!words.length) return null;
    return { text: words.join(' '), rect: range.getBoundingClientRect() };
  }

  function lookUp(pop, picked, how) {
    clearHit();
    hideChip();
    chipDone = picked.text;
    place(pop, picked.rect);
    dispatch(pop, 'blau-lookup', { word: picked.text, how: how });
  }

  function lookUpSelection(pop, viaKey) {
    var picked = selectedWords(viaKey);
    if (!picked) return false;
    lookUp(pop, picked, 'selection');
    return true;
  }

  // Phones (owner, 2026-11-05): no key, so words selected in the text (a long press) show a
  // "Look up" chip under them, the switch on or off (`how: 'chip'`). Its label is the
  // page's, translated there (`data-blau-chip` on the pane).
  var CHIP = 'blau-chip';
  var chip = null;
  var chipPick = null;
  var chipDone = null;    // the words the panel shows from the chip: no chip again for them

  function hideChip() {
    if (chip) chip.style.display = 'none';
    chipPick = null;
  }

  function showChip() {
    var pop = document.querySelector(POPOVER);
    var picked = pop && !overlayOpen() ? selectedWords(true) : null;
    // Nothing selected, or the very words the open panel already shows.
    if (!picked || (picked.text === chipDone && shown(pop))) { hideChip(); return; }
    var pane = document.querySelector(KEY_PANE);
    if (!chip) {
      chip = document.createElement('button');
      chip.type = 'button';
      chip.className = CHIP;
      // Pressing the chip must not clear the selection it looks up.
      chip.addEventListener('pointerdown', function (e) { e.preventDefault(); });
      chip.addEventListener('mousedown', function (e) { e.preventDefault(); });
      chip.addEventListener('click', function (e) {
        e.stopPropagation();
        var p = document.querySelector(POPOVER);
        if (p && chipPick) lookUp(p, chipPick, 'chip');
      });
      document.body.appendChild(chip);
    }
    chip.textContent = (pane && pane.getAttribute('data-blau-chip')) || 'Look up';
    chipPick = picked;
    chip.style.display = 'block';
    var r = picked.rect;
    var w = chip.offsetWidth, h = chip.offsetHeight;
    var left = Math.max(MARGIN, Math.min(r.left + r.width / 2 - w / 2, window.innerWidth - w - MARGIN));
    var top = r.bottom + 2 * GAP;   // under the selection: the phone's own menu sits above it
    if (top + h > window.innerHeight - MARGIN) top = Math.max(MARGIN, r.top - h - 2 * GAP);
    chip.style.left = Math.round(left) + 'px';
    chip.style.top = Math.round(top) + 'px';
  }

  if (window.matchMedia && window.matchMedia('(hover: none)').matches) {
    var chipTimer = null;
    document.addEventListener('selectionchange', function () {
      clearTimeout(chipTimer);
      chipTimer = setTimeout(showChip, 250);   // after the selection handles settle
    });
    window.addEventListener('scroll', hideChip, true);
  }

  document.addEventListener('keydown', function (event) {
    var pop = document.querySelector(POPOVER);
    if (!pop) return;                       // no dictionary on this page
    if (event.key === 'Escape') {
      // Esc closes the panel (owner, 2026-11-05), from its own box too, and disarms the key.
      if (overlayOpen()) return;
      var had = shown(pop) || document.querySelector('.' + HIT) !== null;
      var wasArmed = armed;
      arm(false);
      hideChip();
      clearHit();
      if (had) dispatch(pop, 'blau-close');
      if (had || wasArmed) event.preventDefault();
      return;
    }
    if (event.ctrlKey || event.altKey || event.metaKey || !isLookupKey(event)) return;
    if (inABox(event.target) || inABox(document.activeElement)) return;
    if (overlayOpen()) return;
    down = true;
    if (event.repeat) {                     // held: every click while it is down
      held = true;
      arm(true);
      return;
    }
    held = false;
    if (lookUpSelection(pop, true)) { arm(false); return; }   // selected words, then the key
    arm(true);
  });
  document.addEventListener('keyup', function (event) {
    if (!isLookupKey(event) || !down) return;
    down = false;
    if (held) arm(false);                   // held and let go; a tap stays armed for its click
  });
  // Back from another window: a held key's release was never seen here.
  window.addEventListener('blur', function () {
    if (down) { down = false; arm(false); }
  });

  document.addEventListener('click', function (event) {
    var pop = document.querySelector(POPOVER);
    if (!pop) return;                       // no dictionary on this page
    var viaAlt = event.altKey && !event.ctrlKey && !event.metaKey;
    var viaKey = viaAlt || (armed && !event.ctrlKey && !event.metaKey);
    // A tapped key arms one click, wherever it lands: before the panel's and the switch's own
    // clicks return, which use it up too (Codex, PR #484). A key still down is held: every click
    // while it is down looks up, and letting it go disarms.
    if (armed) { if (down) held = true; else arm(false); }
    if (pop.contains(event.target)) {       // the panel's own controls
      var closing = event.target.nodeType === 1 ? event.target.closest('.' + CLOSE) : null;
      if (closing) clearHit();              // the page hides the popover; the word's mark goes with it
      return;
    }
    var target = event.target && event.target.nodeType === 1 ? event.target : event.target && event.target.parentElement;
    var opener = target ? target.closest('.' + OPEN) : null;
    if (opener) {
      clearHit();
      place(pop, opener.getBoundingClientRect());
      dispatch(pop, 'blau-open');
      return;
    }
    // Words selected with the key armed (or Alt held): the selection, not the word under the pointer.
    if (viaKey && target && paneOf(target, true) && lookUpSelection(pop, true)) return;
    var pane = target ? paneOf(target, viaKey) : null;
    // Something to close: the popover is up, or a word is marked (its lookup
    // may still be running; the close makes it stale).
    var open = shown(pop) || document.querySelector('.' + HIT) !== null;
    // Unwrap the previous mark BEFORE reading the caret: unwrapping merges the
    // text nodes it had split, and a caret taken first would point into a node
    // that no longer exists (a second click on the same line found no word).
    clearHit();
    var caret = pane ? caretAt(event.clientX, event.clientY) : null;
    var word = caret && pane && pane.contains(caret.node) ? wordAt(caret.node, caret.offset) : null;
    if (!word) {
      if (open) dispatch(pop, 'blau-close');
      return;
    }
    var rect = markWord(caret.node, word) ||
      { left: event.clientX, right: event.clientX, top: event.clientY, bottom: event.clientY, width: 0 };
    place(pop, rect);
    hideChip();
    dispatch(pop, 'blau-lookup', { word: word.text, how: viaAlt ? 'alt' : viaKey ? 'key' : 'word' });
  });
})();
