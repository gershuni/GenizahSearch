/**
 * GenizahSearch Image Helper - Background Service Worker
 *
 * Fetches NLI IIIF images on behalf of the page via the user's own IP.
 * Datacenter IPs are blocked by NLI, but residential/university IPs work fine.
 */

const api = typeof browser !== 'undefined' ? browser : chrome;

api.runtime.onMessage.addListener(function (msg, sender, sendResponse) {
    if (msg.type !== 'fetch-image' || !msg.url) {
        sendResponse({ ok: false, error: 'Invalid message' });
        return false;
    }

    fetch(msg.url, {
        headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://www.nli.org.il/'
        }
    })
        .then(function (response) {
            if (!response.ok) {
                throw new Error('HTTP ' + response.status);
            }
            return response.arrayBuffer();
        })
        .then(function (ab) {
            // ArrayBuffer cannot be cloned in message passing, send as plain array
            sendResponse({ ok: true, data: Array.from(new Uint8Array(ab)) });
        })
        .catch(function (e) {
            sendResponse({ ok: false, error: e.message || 'Fetch failed' });
        });

    // Return true to indicate async response
    return true;
});
