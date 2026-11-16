/*
 * The on-screen Hebrew keyboard (web/hebrew_keyboard.py).
 *
 * A key inside a panel marked `data-gs-hebrew-keyboard` puts its character into
 * the box marked `data-gs-hebrew-keyboard-target` at the cursor (replacing a
 * selection); the backspace key deletes the selection or the character before
 * the cursor with its marks. The box hears an ordinary `input` event, so Vue's
 * model, the page's mode-prefix detection and Enter see a key as a typed one.
 *
 * Focus: a mouse click on a key leaves the focus (and the cursor) in the box. A
 * tap or Tab moves the focus to the key, so a phone's own keyboard closes and the
 * panel stays in view; the box keeps its cursor position while it has no focus.
 *
 * At a search attempt's first key it sends `hebrew_keyboard_used` to PostHog and
 * dispatches `gs-hebrew-keyboard-used` on the panel; the page calls reset() when
 * a search starts. No characters are sent anywhere.
 */
(function () {
  'use strict';
  var PANEL = '[data-gs-hebrew-keyboard]';
  var TARGET = '[data-gs-hebrew-keyboard-target]';
  var USED_EVENT = 'gs-hebrew-keyboard-used';

  var usedThisAttempt = false;
  var touchedBox = new WeakSet();  // boxes the reader has put a cursor in

  function targetInput() {
    var el = document.querySelector(TARGET);
    if (!el) return null;
    if (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA') return el;
    return el.querySelector('input, textarea');
  }

  function cursor(input) {
    var len = input.value.length;
    var s = input.selectionStart, e = input.selectionEnd;
    // A box the reader never clicked into: its cursor may sit at 0 after the
    // page restored the last query; the end is where they mean to type.
    if (!touchedBox.has(input) || s === null || e === null) return [len, len];
    return [Math.min(s, len), Math.min(e, len)];
  }

  function commit(input, value, caret) {
    input.value = value;
    try { input.setSelectionRange(caret, caret); } catch (_) { /* not focusable now */ }
    input.dispatchEvent(new Event('input', { bubbles: true }));
  }

  function insert(input, ch) {
    var c = cursor(input), v = input.value;
    commit(input, v.slice(0, c[0]) + ch + v.slice(c[1]), c[0] + ch.length);
  }

  function backspace(input) {
    var c = cursor(input), v = input.value;
    if (c[0] !== c[1]) {
      // A selection that ends between a letter and its marks takes the marks
      // too, or they would land on the letter before it (Codex, PR #23).
      var tail = v.slice(c[1]).match(/^\p{M}*/u);
      var end = c[1] + (tail ? tail[0].length : 0);
      commit(input, v.slice(0, c[0]) + v.slice(end), c[0]);
      return;
    }
    if (c[0] === 0) return;
    var before = v.slice(0, c[0]);
    // The last character with every combining mark on it (a vowel point, the
    // dot of a dotted letter). \p{M} is Unicode's own list: maqaf, paseq and sof
    // pasuq sit among the Hebrew points but are characters of their own (Codex,
    // PR #3). With the u flag a surrogate pair is one character.
    var m = before.match(/\P{M}?\p{M}*$/u);
    var cut = (m && m[0].length) ? m[0].length : 1;
    commit(input, before.slice(0, before.length - cut) + v.slice(c[1]), c[0] - cut);
  }

  function noteUse(panel) {
    if (usedThisAttempt) return;
    usedThisAttempt = true;
    try {
      if (window.posthog) {
        window.posthog.capture('hebrew_keyboard_used', {
          field: panel.getAttribute('data-hk-field') || 'main_search',
          layout: panel.getAttribute('data-hk-layout') || 'alphabetical'
        });
      }
    } catch (_) { /* analytics never blocks a key */ }
    panel.dispatchEvent(new CustomEvent(USED_EVENT, { detail: JSON.stringify({ used: true }) }));
  }

  // A mouse click on a key keeps the focus in the box; a tap does not (above).
  document.addEventListener('pointerdown', function (ev) {
    var key = ev.target.closest && ev.target.closest(PANEL + ' .gs-hk-key');
    if (key && ev.pointerType === 'mouse') ev.preventDefault();
  }, true);

  document.addEventListener('focusin', function (ev) {
    var input = targetInput();
    if (input && ev.target === input) touchedBox.add(input);
  }, true);

  document.addEventListener('click', function (ev) {
    var key = ev.target.closest && ev.target.closest(PANEL + ' .gs-hk-key');
    if (!key) return;
    var panel = key.closest(PANEL);
    var input = targetInput();
    if (!input || input.disabled || input.readOnly) return;
    ev.preventDefault();
    if (key.getAttribute('data-hk-action') === 'backspace') {
      backspace(input);
    } else {
      var ch = key.getAttribute('data-hk-char');
      if (!ch) return;
      insert(input, ch);
      touchedBox.add(input);
    }
    noteUse(panel);
  });

  window.gsHebrewKeyboard = {
    reset: function () { usedThisAttempt = false; },
    // For tests.
    _insert: insert,
    _backspace: backspace
  };
})();
