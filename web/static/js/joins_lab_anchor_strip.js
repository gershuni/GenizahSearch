/* Joins Lab anchor strip (owner, 2026-10-23).
 *
 * Under 1024px the Lab's anchor pane stacks above the work column and scrolls
 * away with the candidates (web/static/common.css, "Joins Lab"). This keeps
 * the anchor in view once the pane has left the screen: the page builds a
 * .joins-lab-anchor-strip (web/pages/joins_lab.py) that the stylesheet shows
 * only with is-visible, which an IntersectionObserver on the pane sets while
 * the pane is above the header. The strip's thumbnail is a copy of the image
 * the pane shows (so a folio flip or a new anchor is followed), and
 * under-hidden-header moves the strip to the top while the header's reveal
 * has hidden it (phones hide the header on scroll down; Quasar marks that
 * with q-header--hidden).
 *
 * Loaded from the page's <head> at build time; the elements arrive later, so
 * init waits for them.
 */
(function () {
    'use strict';
    var HEADER_PX = 64;

    function wire(strip, pane) {
        var thumb = strip.querySelector('.joins-lab-anchor-strip-thumb');
        var header = document.querySelector('.q-header');

        function syncThumb() {
            var img = pane.querySelector('.image-container img');
            if (thumb && img && img.src && thumb.src !== img.src) {
                thumb.src = img.src;
            }
        }
        function syncHeader() {
            var hidden = !!(header && header.classList.contains('q-header--hidden'));
            strip.classList.toggle('under-hidden-header', hidden);
        }

        new IntersectionObserver(function (entries) {
            var e = entries[entries.length - 1];
            var away = !e.isIntersecting && e.boundingClientRect.bottom < HEADER_PX;
            if (away) { syncThumb(); }
            strip.classList.toggle('is-visible', away);
        }, { rootMargin: '-' + HEADER_PX + 'px 0px 0px 0px', threshold: 0 }).observe(pane);

        // An image loading in the pane (first load, folio flip, a new anchor)
        // refreshes the thumbnail: load does not bubble, so listen in capture.
        pane.addEventListener('load', syncThumb, true);

        if (header && window.MutationObserver) {
            new MutationObserver(syncHeader).observe(header, {
                attributes: true, attributeFilter: ['class']
            });
            syncHeader();
        }
    }

    function init() {
        var strip = document.querySelector('.joins-lab-anchor-strip');
        var pane = document.querySelector('.joins-lab-anchor');
        if (!strip || !pane || !window.IntersectionObserver) { return false; }
        wire(strip, pane);
        return true;
    }

    if (!init()) {
        var waiting = new MutationObserver(function () {
            if (init()) { waiting.disconnect(); }
        });
        waiting.observe(document.documentElement, { childList: true, subtree: true });
    }
})();
