/* Keyboard shortcuts on every page of the website (web/keyboard_shortcuts.py).
 *
 *   ?  opens the card that lists the shortcuts: the script dispatches
 *      `gs-shortcuts` on the page's hidden `.gs-shortcuts-trigger`, and the
 *      page draws the card (one round trip; the card is translated there).
 *   /  moves the focus to the page's own search box, the one marked
 *      `data-gs-primary-search` (/search, the homepage), else to the header's
 *      go-to box, opening it first where it sits behind its button (below
 *      1024px; common.css `.header-search.is-open`).
 *
 * Neither key does anything while a box has the focus, while a dialog or menu is open,
 * or with Ctrl, Alt or Cmd (those are the browser's). Both answer the key's
 * character and its physical key, so they work on a Hebrew layout too: there
 * the "/" key types ".", and "/" is on the Q key.
 */
(function () {
  'use strict';

  var TRIGGER = '.gs-shortcuts-trigger';
  var EVENT = 'gs-shortcuts';
  var PRIMARY = '[data-gs-primary-search]';

  function inABox(el) {
    if (!el || el.nodeType !== 1) return false;
    var tag = el.tagName;
    return tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT' || el.isContentEditable;
  }

  function visible(el) {
    return !!el && el.getClientRects().length > 0;
  }

  // Quasar puts the attribute on the <input> itself; a wrapper may carry it too. One box can
  // stand in for another (/search: the query box, or the PGP tag selector in its mode), so the
  // first VISIBLE marked box wins (Codex, PR #484: the first one was the hidden query box).
  function primaryField() {
    var marked = document.querySelectorAll(PRIMARY);
    for (var i = 0; i < marked.length; i++) {
      var el = marked[i];
      var field = (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA') ? el : el.querySelector('input, textarea');
      if (visible(field)) return field;
    }
    return null;
  }

  function focusSearch() {
    var field = primaryField();
    if (field) {
      field.focus();
      if (field.select) field.select();
      return true;
    }
    var box = document.querySelector('.header-search');
    field = box ? box.querySelector('input') : null;
    if (!field) return false;
    if (!visible(field)) box.classList.add('is-open');
    setTimeout(function () {
      field.focus();
      if (field.select) field.select();
    }, 0);
    return true;
  }

  document.addEventListener('keydown', function (event) {
    if (event.ctrlKey || event.altKey || event.metaKey || event.repeat || event.defaultPrevented) return;
    if (inABox(event.target) || inABox(document.activeElement)) return;
    if (document.querySelector('.q-dialog, .q-menu')) return;
    if (event.key === '?' || (event.code === 'Slash' && event.shiftKey)) {
      var trigger = document.querySelector(TRIGGER);
      if (!trigger) return;
      event.preventDefault();
      trigger.dispatchEvent(new CustomEvent(EVENT));
      return;
    }
    if (!event.shiftKey && (event.key === '/' || event.code === 'Slash')) {
      if (focusSearch()) event.preventDefault();
    }
  });
})();
