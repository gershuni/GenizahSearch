/**
 * GenizahSearch Image Helper - Content Script
 *
 * Bridges the GenizahSearch page and the background service worker.
 * Injects a detection meta tag so the page knows the extension is installed.
 * Relays image fetch requests from the page to the background worker and
 * returns blob URLs back to the page.
 *
 * Only responds to messages from the same window (event.source === window)
 * and only posts results back to the page's own origin.
 */

const api = typeof browser !== 'undefined' ? browser : chrome;

// Inject detection meta tag so the page can check for extension presence
(function () {
    var meta = document.createElement('meta');
    meta.name = 'genizah-extension';
    meta.content = '1';
    (document.head || document.documentElement).appendChild(meta);
})();

// Allowed origins for message validation
var ALLOWED_ORIGINS = ['https://genizahsearch.com', 'http://localhost', 'http://127.0.0.1'];

function isAllowedOrigin(origin) {
    for (var i = 0; i < ALLOWED_ORIGINS.length; i++) {
        if (origin === ALLOWED_ORIGINS[i] || origin.indexOf(ALLOWED_ORIGINS[i] + ':') === 0) return true;
    }
    return false;
}

// Listen for fetch requests from the page
window.addEventListener('message', function (event) {
    if (!event.data || event.data.type !== 'genizah-fetch-image') return;
    if (event.source !== window) return;
    if (!isAllowedOrigin(event.origin)) return;

    var requestId = event.data.requestId;
    var url = event.data.url;

    if (!url || !requestId) return;

    api.runtime.sendMessage({ type: 'fetch-image', url: url }, function (response) {
        if (api.runtime.lastError) {
            window.postMessage({
                type: 'genizah-image-result',
                requestId: requestId,
                error: api.runtime.lastError.message || 'Extension error'
            }, window.location.origin);
            return;
        }

        if (!response || !response.ok || !response.data) {
            window.postMessage({
                type: 'genizah-image-result',
                requestId: requestId,
                error: (response && response.error) || 'No data'
            }, window.location.origin);
            return;
        }

        // Create blob from the byte array returned by the background worker
        try {
            var blob = new Blob([new Uint8Array(response.data)], { type: 'image/jpeg' });
            var blobUrl = URL.createObjectURL(blob);
            window.postMessage({
                type: 'genizah-image-result',
                requestId: requestId,
                blobUrl: blobUrl
            }, window.location.origin);

            // Revoke blob URL after 5 minutes to prevent memory leaks
            // (generous timeout for slow server processing)
            setTimeout(function () { URL.revokeObjectURL(blobUrl); }, 300000);
        } catch (e) {
            window.postMessage({
                type: 'genizah-image-result',
                requestId: requestId,
                error: e.message || 'Blob creation failed'
            }, window.location.origin);
        }
    });
});
