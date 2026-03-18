/**
 * GenizahSearch Image Helper - Background Service Worker
 *
 * Fetches NLI IIIF images on behalf of the page via the user's own IP.
 * Production servers cannot fetch these images because NLI blocks
 * datacenter IP ranges. This extension uses the user's residential or
 * institutional IP, which NLI accepts.
 *
 * Security: Only fetches from iiif.nli.org.il — all other URLs are rejected.
 */

const api = typeof browser !== 'undefined' ? browser : chrome;

const ALLOWED_HOST = 'iiif.nli.org.il';

api.runtime.onMessage.addListener(function (msg, sender, sendResponse) {
    if (msg.type !== 'fetch-image' || !msg.url) {
        sendResponse({ ok: false, error: 'Invalid message' });
        return false;
    }

    // Validate URL — only allow NLI IIIF requests
    try {
        var parsed = new URL(msg.url);
        if (parsed.hostname !== ALLOWED_HOST) {
            sendResponse({ ok: false, error: 'URL not allowed: ' + parsed.hostname });
            return false;
        }
    } catch (e) {
        sendResponse({ ok: false, error: 'Invalid URL' });
        return false;
    }

    fetch(msg.url)
        .then(function (response) {
            if (!response.ok) {
                throw new Error('HTTP ' + response.status);
            }
            return response.arrayBuffer();
        })
        .then(function (ab) {
            // MV3 structured clone supports Uint8Array directly — no Array conversion needed
            sendResponse({ ok: true, data: new Uint8Array(ab) });
        })
        .catch(function (e) {
            sendResponse({ ok: false, error: e.message || 'Fetch failed' });
        });

    // Return true to indicate async response
    return true;
});
