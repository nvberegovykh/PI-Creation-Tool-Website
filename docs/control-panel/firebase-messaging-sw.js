/* Firebase Messaging SW shim.
 * Some SDK paths try loading /firebase-messaging-sw.js by default.
 * Keep this file present to avoid 404 noise and delegate to main SW behavior.
 */

self.addEventListener('install', (event) => {
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  clients.claim();
});
