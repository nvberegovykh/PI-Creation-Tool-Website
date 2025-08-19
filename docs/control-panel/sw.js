/* Service Worker for LIBER/APPS
 * - Shows notifications when the page posts a message to the SW
 * - Handles generic Push events if configured by a push service (FCM or other)
 */

self.addEventListener('install', (event) => {
	self.skipWaiting();
});

self.addEventListener('activate', (event) => {
	clients.claim();
});

async function showNotification(data){
	const title = data.title || 'LIBER/APPS';
	const options = {
		body: data.body || '',
		icon: data.icon || '/images/LIBER LOGO.png',
		badge: data.badge || '/images/LIBER LOGO.png',
		data: data.data || {},
		silent: !!data.silent,
	};
	try { await self.registration.showNotification(title, options); } catch (_) {}
}

self.addEventListener('message', (event) => {
	const msg = event.data || {};
	if (msg && msg.type === 'notify') {
		showNotification(msg.payload || {});
	}
});

self.addEventListener('push', (event) => {
	let payload = {};
	try { payload = event.data ? event.data.json() : {}; } catch(_) {}
	event.waitUntil(showNotification(payload));
});


