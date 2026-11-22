// The Reading Desk keeps the reader's place on the page when it is redrawn.
//
// 1. A control that redraws the desk (class "rd-redraws": the image and text views, a fragment's
//    up, down and remove) is destroyed by the redraw it asks for. A mouse click leaves the focus
//    in it, and when the focused element leaves the page the browser takes the page to its top
//    (2026-11-22: "switching between One at a time and the other options jumps the page to
//    top"). The focus is let go as soon as the control is clicked, before the server answers.
//
// 2. The two panes of "All images" scroll in step (rdSyncPanes). They used scrollIntoView, which
//    also scrolls the page, and the observers report where the panes already are as soon as they
//    are set up: every redraw moved the page to the top of the text side. Each pane is now
//    scrolled on its own, and the first report is not followed.
//
// Loaded by web/pages/browse.py; checked by tests/js/reading_desk_place_check.js.
(function () {
    'use strict';
    if (window.__rdPlaceInstalled) return;
    window.__rdPlaceInstalled = true;

    document.addEventListener('click', function (event) {
        var target = event.target;
        var control = target && target.closest ? target.closest('.rd-redraws') : null;
        if (!control) return;
        setTimeout(function () {
            var active = document.activeElement;
            if (active && active !== document.body && control.contains(active) && active.blur) active.blur();
        }, 0);
    }, true);

    // The pane's own scroll that brings ``target`` to its top (never the page's).
    function scrollPaneTo(pane, target) {
        var top = pane.scrollTop + target.getBoundingClientRect().top - pane.getBoundingClientRect().top;
        pane.scrollTo({ top: Math.max(0, top), behavior: 'smooth' });
    }

    function follow(fromPrefix, toPrefix, toPane, state) {
        var first = true;
        return function (entries) {
            if (first) { first = false; return; }  // where the panes already are: nothing to follow
            if (state.syncing) return;
            for (var i = 0; i < entries.length; i++) {
                var entry = entries[i];
                if (!entry.isIntersecting) continue;
                var idx = entry.target.id.replace(fromPrefix, '');
                var other = document.getElementById(toPrefix + idx);
                if (other) {
                    state.syncing = true;
                    scrollPaneTo(toPane, other);
                    setTimeout(function () { state.syncing = false; }, 600);
                }
                break;
            }
        };
    }

    // One pair of observers at a time: each drawing of the desk replaces the last one's (which
    // watched panes now gone), and a setup still waiting is not made (GitHub Codex, #97).
    var current = { observers: [], timer: null };

    function stopSync() {
        if (current.timer !== null) { clearTimeout(current.timer); current.timer = null; }
        current.observers.forEach(function (o) { o.disconnect(); });
        current.observers = [];
    }

    // The image pane and the text pane of a desk of ``fragCount`` fragments scroll in step, by
    // the fragments' headers (rd-img-frag-<n>, rd-text-frag-<n>). Called after each drawing of
    // the desk; the panes are looked for once they are on the page.
    window.rdSyncPanes = function (fragCount, delay) {
        stopSync();
        if (!(fragCount > 1)) return;
        current.timer = setTimeout(function () {
            current.timer = null;
            var imgPane = document.querySelector('.rd-image-pane .q-scrollarea__container');
            var textPane = document.querySelector('.rd-text-pane .q-scrollarea__container');
            if (!imgPane || !textPane) return;
            var state = { syncing: false };
            var imgObserver = new IntersectionObserver(
                follow('rd-img-frag-', 'rd-text-frag-', textPane, state), { root: imgPane, threshold: 0.3 });
            var textObserver = new IntersectionObserver(
                follow('rd-text-frag-', 'rd-img-frag-', imgPane, state), { root: textPane, threshold: 0.3 });
            current.observers = [imgObserver, textObserver];
            document.querySelectorAll('[id^="rd-img-frag-"]').forEach(function (h) { imgObserver.observe(h); });
            document.querySelectorAll('[id^="rd-text-frag-"]').forEach(function (h) { textObserver.observe(h); });
        }, delay === undefined ? 500 : delay);
    };
})();
