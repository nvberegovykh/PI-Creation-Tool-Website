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


// Focus an existing client or open a new tab when the user clicks a notification
self.addEventListener('notificationclick', (event) => {
	event.notification.close();
	const targetUrl = (event.notification && event.notification.data && event.notification.data.url) || '/control-panel/index.html';
	event.waitUntil((async () => {
		const allClients = await clients.matchAll({ type: 'window', includeUncontrolled: true });
		for (const client of allClients) {
			// If our app is already open, navigate/focus it
			try {
				await client.navigate(targetUrl);
				return client.focus();
			} catch(_) {
				// Fallback to focus only
				return client.focus();
			}
		}
		// Otherwise open a new window
		if (clients.openWindow) {
			return clients.openWindow(targetUrl);
		}
	})());
});

// Optional: accept a message to clear caches and help force-reload
self.addEventListener('message', (event) => {
	const msg = event.data || {};
	if (msg && msg.type === 'force-reload') {
		event.waitUntil((async () => {
			try {
				const names = await caches.keys();
				await Promise.all(names.map((n) => caches.delete(n)));
			} catch (_) {}
			const list = await clients.matchAll({ type: 'window', includeUncontrolled: true });
			list.forEach((c) => c.postMessage && c.postMessage({ type: 'force-reload-done' }));
		})());
	}
});
