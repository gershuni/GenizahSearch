/**
 * GenizahSearch Image Helper - Content Script
 *
 * Bridges the page and the background service worker.
 * Injects a detection meta tag so the page knows the extension is installed.
 * Relays fetch requests from the page to the background worker and returns
 * blob URLs back to the page.
 */

const api = typeof browser !== 'undefined' ? browser : chrome;

// Inject detection meta tag so the page can check for extension presence
(function () {
    var meta = document.createElement('meta');
    meta.name = 'genizah-extension';
    meta.content = '1';
    (document.head || document.documentElement).appendChild(meta);
})();

// Listen for fetch requests from the page
window.addEventListener('message', function (event) {
    if (!event.data || event.data.type !== 'genizah-fetch-image') return;
    if (event.source !== window) return;

    var requestId = event.data.requestId;
    var url = event.data.url;

    if (!url || !requestId) return;

    api.runtime.sendMessage({ type: 'fetch-image', url: url }, function (response) {
        if (api.runtime.lastError) {
            window.postMessage({
                type: 'genizah-image-result',
                requestId: requestId,
                error: api.runtime.lastError.message || 'Extension error'
            }, '*');
            return;
        }

        if (!response || !response.ok || !response.data) {
            window.postMessage({
                type: 'genizah-image-result',
                requestId: requestId,
                error: (response && response.error) || 'No data'
            }, '*');
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
            }, '*');
        } catch (e) {
            window.postMessage({
                type: 'genizah-image-result',
                requestId: requestId,
                error: e.message || 'Blob creation failed'
            }, '*');
        }
    });
});
