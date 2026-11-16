/* The Blau dictionary companion on /browse and in the search page's Quick View: the
 * click side (plan §6.2).
 *
 * On a page where the dictionary is served, the page creates one element
 * `.gs-blau-popover` (web/components/dictionary_lookup.py; in Quick View inside
 * the dialog) and marks the transcription pane with `data-blau-lookup`. This
 * script, on any click:
 *   - inside the popover: nothing (its links and buttons are its own);
 *   - at the end of a drag over words of the marked pane: looks up the selection,
 *     as the key does below (`how: 'selection'`);
 *   - on a word of the marked pane: unwraps the previous mark, wraps the word
 *     in `span.blau-word-hit`, places the popover beside the word (CSS puts
 *     it at the foot of a phone instead) and dispatches `blau-lookup` on the
 *     popover with `detail` = JSON {word, how, before, after}: `before` and
 *     `after` the words beside it in the text (`wordsBeside`);
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
 * menu or another dialog than the panel's own is open, or with Ctrl, Alt or Cmd
 * (those are the browser's).
 * A word can span inline text nodes and contain reading brackets. Its letters,
 * marks and brackets are sent unchanged; whitespace, punctuation and an empty
 * lacuna separate words. Highlighting preserves the original inline elements.
 * A click that lands beside a word (padding, a line number) is no word.
 * Nothing here changes what the page says: the page asks the dictionary.
 */
(function () {
  'use strict';

  var POPOVER = '.gs-blau-popover';
  var PANE = '[data-blau-lookup]';
  var KEY_PANE = '[data-blau-pane]';   // where the look-up key works, the switch on or off
  var HIT = 'blau-word-hit';
  var CLOSE = 'blau-close';   // the panel's own close control (the page hides the popover)
  var OPEN = 'blau-open';     // the "type a word" button (this script opens the panel)
  var STEM_MENU = 'blau-stem-menu';   // the verb-stem field's list (blau_panel.STEM_MENU_CLASS)
  // Hebrew letters; the Hebrew combining marks (U+0591-05BD, 05BF, 05C1-05C2,
  // 05C4-05C5, 05C7 -- NOT the punctuation between them: maqaf 05BE, paseq
  // 05C0, sof pasuq 05C3, nun hafukha 05C6, which end a word as they do for
  // shared/blau_normalize.py); the combining diacriticals; the apostrophe and
  // geresh family the transcriptions write the Judeo-Arabic letters with.
  var WORD_CHAR = /[א-ת֑-ׇֽֿׁׂׅׄ̀-ͯ'"׳״‘’]/;
  var COMBINING_MARK = /\p{M}/u;       // including Arabic vocalization on Hebrew letters
  var BRACKET_CHAR = /[\[\]()]/;
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

  // Reading brackets are transparent within a word, but [] is a missing stretch,
  // not supplied letters. Do not join the legible fragments on its two sides.
  function wordPart(text, at) {
    var ch = text.charAt(at);
    if (WORD_CHAR.test(ch) || COMBINING_MARK.test(ch)) return true;
    if (!BRACKET_CHAR.test(ch)) return false;
    return !(ch === '[' && text.charAt(at + 1) === ']' ||
             ch === ']' && text.charAt(at - 1) === '[');
  }

  function wordAt(pane, node, offset) {
    if (!node || node.nodeType !== 3) return null;
    var reading = paneText(pane);
    var anchor = reading.parts.find(function (part) { return part.node === node; });
    if (!anchor) return null;
    var text = reading.text;
    var start = anchor.start + offset, end = start;
    while (start > 0 && wordPart(text, start - 1)) start--;
    while (end < text.length && wordPart(text, end)) end++;
    if (start === end) return null;
    var word = text.slice(start, end);
    if (!/[א-ת]/.test(word)) return null;  // marks alone are no word
    var parts = reading.parts.filter(function (part) { return part.start < end && part.end > start; });
    return { start: start, end: end, text: word, reading: reading, parts: parts };
  }

  // The words beside a clicked word, sent with it ("אטאל אללה בקאך" is one
  // blessing, and אללה alone reached other entries): at most CONTEXT_WORDS on each side
  // (shared/ja_expression.py MAX_CONTEXT_WORDS), the nearest last before it and first after it,
  // not across a sentence's end.
  var CONTEXT_WORDS = 8;
  var CONTEXT_CHARS = 200;                 // of the text on each side, plenty for two words
  var SENTENCE_END = /[.:׃!?…]$/;
  var BLOCK_TAGS = { DIV: 1, P: 1, BR: 1, LI: 1, UL: 1, OL: 1, PRE: 1, TD: 1, TH: 1, TR: 1 };
  var GUTTER = 'line-number-gutter';       // web/components/typography.py: the line numbers

  // One reading stream, with offsets back into its text nodes. Inline formatting
  // does not split a word; both ends of a block do. Line numbers never enter it.
  function paneText(pane) {
    var reading = { text: '', parts: [] };
    function visit(n) {
      if (n.nodeType === 3) {
        var start = reading.text.length;
        reading.text += n.data;
        reading.parts.push({ node: n, start: start, end: reading.text.length });
      } else if (n.nodeType === 1) {
        if (n.classList.contains(GUTTER)) { reading.text += ' '; return; }
        if (BLOCK_TAGS[n.tagName]) reading.text += ' ';
        for (var i = 0; i < n.childNodes.length; i++) visit(n.childNodes[i]);
        if (BLOCK_TAGS[n.tagName]) reading.text += ' ';
      }
    }
    visit(pane);
    return reading;
  }

  // A word without what stands around it; '' for none (a line number, a mark); null for a word
  // broken inside ("מ].נ": a gap in the manuscript), past which the words are not beside it.
  function cleanWord(part) {
    if (/\[\s*\]|\.\.\.|…/.test(part) || (!/[א-ת]/.test(part) && /[\[\]]/.test(part))) return null;
    var start = 0, end = part.length;
    while (start < end && !wordPart(part, start)) start++;
    while (end > start && !wordPart(part, end - 1)) end--;
    part = part.slice(start, end);
    for (var k = 0; k < part.length; k++) if (!wordPart(part, k)) return null;
    return /[א-ת]/.test(part) ? part : '';
  }

  function wordsBeside(word) {
    var text = { before: word.reading.text.slice(Math.max(0, word.start - CONTEXT_CHARS), word.start),
                 after: word.reading.text.slice(word.end, word.end + CONTEXT_CHARS) };
    var before = [], after = [];
    // What touches the word with no space is its own punctuation ("[", "]."), not a word.
    var parts = text.before.split(/\s+/);
    if (!/\s$/.test(text.before) && /[.:׃!?…\[\]]/.test(parts.pop() || '')) parts = [];
    for (var i = parts.length - 1; i >= 0 && before.length < CONTEXT_WORDS; i--) {
      if (SENTENCE_END.test(parts[i])) break;
      var w = cleanWord(parts[i]);
      if (w === null) break;
      if (w) before.unshift(w);
    }
    parts = text.after.split(/\s+/);
    if (!/^\s/.test(text.after) && /[.:׃!?…\[\]]/.test(parts.shift() || '')) parts = [];
    for (var j = 0; j < parts.length && after.length < CONTEXT_WORDS; j++) {
      var a = cleanWord(parts[j]);
      if (a === null) break;
      if (a) after.push(a);
      if (SENTENCE_END.test(parts[j])) break;
    }
    return { before: before, after: after };
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

  function markWord(word) {
    try {
      var first = word.parts[0], last = word.parts[word.parts.length - 1];
      var range = document.createRange();
      range.setStart(first.node, word.start - first.start);
      range.setEnd(last.node, word.end - last.start);
      var rect = range.getBoundingClientRect();
      // Wrap only text slices, not partially selected inline elements. Reversing
      // the slices also keeps their original offsets stable while nodes split.
      for (var i = word.parts.length - 1; i >= 0; i--) {
        var part = word.parts[i];
        var mark = document.createRange();
        mark.setStart(part.node, Math.max(0, word.start - part.start));
        mark.setEnd(part.node, Math.min(part.node.data.length, word.end - part.start));
        var span = document.createElement('span');
        span.className = HIT;
        mark.surroundContents(span);
      }
      return rect;
    } catch (e) {
      clearHit();
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
  // its keys are its own (Codex, PR #484). The dictionary panel is neither, and neither is the
  // dialog the panel is in (the search page's Quick View): a menu always is, a dialog unless it
  // holds the panel. A second dialog over Quick View holds no panel, so it stands the script down.
  function overlayOpen(pop) {
    if (document.querySelector('.q-menu') !== null) return true;
    var dialogs = document.querySelectorAll('.q-dialog');
    for (var i = 0; i < dialogs.length; i++) {
      if (!pop || !dialogs[i].contains(pop)) return true;
    }
    return false;
  }

  // The dialog the panel is in (Quick View), or null on a page (/browse).
  function dialogOf(pop) {
    return pop && pop.closest ? pop.closest('.q-dialog') : null;
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
    for (var i = 0; i < parts.length; i++) {
      var part = cleanWord(parts[i]);
      if (part === null) return null;
      if (part) words.push(part);
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

  // Where the chip lives: the page's body, or, when the panel is in a dialog, the panel's own
  // parent there. A chip in the body sits under the dialog (z-index 2400 against Quasar's 6000),
  // and a dialog closed and opened again is a new parent: the chip moves along.
  function chipHost(pop) {
    return dialogOf(pop) ? pop.parentNode : document.body;
  }

  function showChip() {
    var pop = document.querySelector(POPOVER);
    var picked = pop && !overlayOpen(pop) ? selectedWords(true) : null;
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
    }
    var host = chipHost(pop);
    if (chip.parentNode !== host) host.appendChild(chip);
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

  // Esc closes the panel (owner, 2026-11-05), from its own box too, and disarms the key.
  // True when there was a panel or a marked word to close.
  function escape(pop, event) {
    var had = shown(pop) || document.querySelector('.' + HIT) !== null;
    var wasArmed = armed;
    arm(false);
    hideChip();
    clearHit();
    if (had) dispatch(pop, 'blau-close');
    if (had || wasArmed) event.preventDefault();
    return had;
  }

  // In a dialog (Quick View), the Esc that closes the panel closes only the panel: the dialog
  // closes on Esc (Quasar: keydown, then keyup, both on window) and the search page toggles its
  // form on it (ui.keyboard, on document). So it is handled first, on window as it comes down,
  // and goes no further; its keyup too. Esc with nothing to close passes on and closes the dialog.
  var escUsed = false;
  window.addEventListener('keydown', function (event) {
    if (event.key !== 'Escape') return;
    var pop = document.querySelector(POPOVER);
    if (!dialogOf(pop) || overlayOpen(pop)) return;
    if (!shown(pop) && document.querySelector('.' + HIT) === null) return;
    escape(pop, event);
    escUsed = true;
    event.stopImmediatePropagation();
  }, true);
  window.addEventListener('keyup', function (event) {
    if (event.key !== 'Escape' || !escUsed) return;
    escUsed = false;
    event.stopImmediatePropagation();
  }, true);

  document.addEventListener('keydown', function (event) {
    var pop = document.querySelector(POPOVER);
    if (!pop) return;                       // no dictionary on this page
    if (event.key === 'Escape') {
      if (overlayOpen(pop)) return;
      escape(pop, event);
      return;
    }
    if (event.ctrlKey || event.altKey || event.metaKey || !isLookupKey(event)) return;
    if (inABox(event.target) || inABox(document.activeElement)) return;
    if (overlayOpen(pop)) return;
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
    escUsed = false;                        // nor an Esc's
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
    // The verb-stem field's list is drawn outside the popover, at the end of <body>: a choice
    // there is the panel's own control, not a click elsewhere (it closed the panel, 2026-11-11).
    if (event.target && event.target.nodeType === 1 && (event.target.closest('.' + STEM_MENU) || event.target.closest('.dict-gap-menu'))) return;
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
    // With the switch on, words dragged over end in this click too: the selection, as the key looks
    // it up (only the word under the pointer was looked up). A double or triple
    // click (`detail` 2, 3) selects a word or a line by itself: that stays the word clicked.
    if (!viaKey && event.detail <= 1 && target && paneOf(target, false) && lookUpSelection(pop, false)) return;
    var pane = target ? paneOf(target, viaKey) : null;
    // Something to close: the popover is up, or a word is marked (its lookup
    // may still be running; the close makes it stale).
    var open = shown(pop) || document.querySelector('.' + HIT) !== null;
    // Unwrap the previous mark BEFORE reading the caret: unwrapping merges the
    // text nodes it had split, and a caret taken first would point into a node
    // that no longer exists (a second click on the same line found no word).
    clearHit();
    var caret = pane ? caretAt(event.clientX, event.clientY) : null;
    var word = caret && pane && pane.contains(caret.node) ? wordAt(pane, caret.node, caret.offset) : null;
    if (!word) {
      if (open) dispatch(pop, 'blau-close');
      return;
    }
    // Before the mark: wrapping the word splits its text node.
    var beside = wordsBeside(word);
    var rect = markWord(word) ||
      { left: event.clientX, right: event.clientX, top: event.clientY, bottom: event.clientY, width: 0 };
    place(pop, rect);
    hideChip();
    dispatch(pop, 'blau-lookup', { word: word.text, how: viaAlt ? 'alt' : viaKey ? 'key' : 'word',
                                   before: beside.before, after: beside.after });
  });
})();
