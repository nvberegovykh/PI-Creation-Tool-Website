(function () {
  const DOWNLOAD_COLLECTION = 'downloads';
  const PLATFORM_ORDER = ['windows', 'macos', 'android', 'ios', 'linux', 'web', 'other'];

  function initOne(container) {
    if (!container) return;
    const desktopMenu = container.querySelector('.navbar-interactive-desktop-menu');
    const mobileNav = container.querySelector('.navbar-interactive-nav');

    if (desktopMenu && !desktopMenu.querySelector('.gc-nav-submenu-zone')) {
      const zone = document.createElement('div');
      zone.className = 'gc-nav-submenu-zone';
      zone.innerHTML =
        '<div class="gc-nav-dot" aria-label="Show menu"></div>' +
        '<div class="gc-nav-submenu">' +
        '<button type="button" class="gc-nav-submenu-btn" data-gc-nav="request">Request</button>' +
        '<button type="button" class="gc-nav-submenu-btn" data-gc-nav="services">Services</button>' +
        '<button type="button" class="gc-nav-submenu-btn gc-nav-downloads-btn" data-gc-nav="downloads">Downloads</button>' +
        '<a href="liber-apps/index.html" target="_blank" rel="noopener" class="gc-nav-submenu-btn">Liber Apps</a>' +
        '</div>';
      desktopMenu.appendChild(zone);

      zone.querySelector('[data-gc-nav="services"]').addEventListener('click', openPricingPopup);
      zone.querySelector('[data-gc-nav="downloads"]').addEventListener('click', openDownloadsPopup);
      zone.querySelector('[data-gc-nav="request"]').addEventListener('click', () => {
        if (typeof window.openRequestQuiz === 'function') window.openRequestQuiz();
      });

      const dot = zone.querySelector('.gc-nav-dot');
      const submenu = zone.querySelector('.gc-nav-submenu');
      dot.addEventListener('mouseenter', () => container.classList.add('gc-submenu-visible'));
      submenu.addEventListener('mouseenter', () => container.classList.add('gc-submenu-visible'));
      const header = container.querySelector('header');
      (header || container).addEventListener('mouseleave', () => container.classList.remove('gc-submenu-visible'));
    }

    if (mobileNav && !mobileNav.querySelector('.gc-nav-mobile-dot-row')) {
      const dotRow = document.createElement('div');
      dotRow.className = 'gc-nav-mobile-dot-row';
      dotRow.innerHTML =
        '<div class="gc-nav-mobile-dot" aria-label="Show menu"></div>' +
        '<div class="gc-nav-mobile-submenu">' +
        '<button type="button" class="gc-nav-submenu-btn" data-gc-nav="request">Request</button>' +
        '<button type="button" class="gc-nav-submenu-btn" data-gc-nav="services">Services</button>' +
        '<button type="button" class="gc-nav-submenu-btn gc-nav-downloads-btn" data-gc-nav="downloads">Downloads</button>' +
        '<a href="liber-apps/index.html" target="_blank" rel="noopener" class="gc-nav-submenu-btn">Liber Apps</a>' +
        '</div>';
      mobileNav.appendChild(dotRow);

      const dot = dotRow.querySelector('.gc-nav-mobile-dot');
      const submenu = dotRow.querySelector('.gc-nav-mobile-submenu');
      dot.addEventListener('click', () => {
        const willOpen = !dotRow.classList.contains('gc-submenu-open');
        dot.classList.toggle('gc-active', willOpen);
        submenu.classList.toggle('gc-open', willOpen);
        dotRow.classList.toggle('gc-submenu-open', willOpen);
      });

      dotRow.querySelector('[data-gc-nav="services"]').addEventListener('click', () => {
        openPricingPopup();
        submenu.classList.remove('gc-open');
        dot.classList.remove('gc-active');
        dotRow.classList.remove('gc-submenu-open');
      });
      dotRow.querySelector('[data-gc-nav="downloads"]').addEventListener('click', () => {
        openDownloadsPopup();
        submenu.classList.remove('gc-open');
        dot.classList.remove('gc-active');
        dotRow.classList.remove('gc-submenu-open');
      });
      dotRow.querySelector('[data-gc-nav="request"]').addEventListener('click', () => {
        if (typeof window.openRequestQuiz === 'function') window.openRequestQuiz();
        submenu.classList.remove('gc-open');
        dot.classList.remove('gc-active');
        dotRow.classList.remove('gc-submenu-open');
      });
    }

    syncDownloadsButtons(container);
  }

  function init() {
    const containers = document.querySelectorAll('.navbar-interactive-container');
    if (!containers.length) {
      setTimeout(init, 100);
      return;
    }
    containers.forEach((c) => initOne(c));
    setTimeout(refreshAllDownloadsButtons, 800);
    setTimeout(refreshAllDownloadsButtons, 2500);
    setTimeout(refreshAllDownloadsButtons, 6000);
    setTimeout(refreshAllDownloadsButtons, 12000);
  }

  function initForNewContent(root) {
    if (!root) return;
    const containers = root.querySelectorAll ? root.querySelectorAll('.navbar-interactive-container') : [];
    const arr = (root.classList && root.classList.contains('navbar-interactive-container')) ? [root] : Array.from(containers);
    arr.forEach((c) => initOne(c));
  }

  window.__navbarSubmenuInitForNewContent = initForNewContent;

  let pricingCache = null;
  let downloadsRowsCache = null;
  let downloadsHasItemsCache = null;
  let downloadsFetchedAt = 0;
  const DOWNLOADS_CACHE_MS = 60 * 1000;

  function wirePricingTabs(container) {
    if (!container) return;
    const tabs = container.querySelectorAll('.gc-pricing-tab');
    const panels = container.querySelectorAll('.gc-pricing-panel');
    tabs.forEach((tab) => {
      tab.addEventListener('click', () => {
        const id = tab.getAttribute('data-tab');
        tabs.forEach((t) => t.classList.toggle('active', t === tab));
        panels.forEach((p) => p.classList.toggle('active', p.getAttribute('data-panel') === id));
      });
    });
  }

  function ensurePricingStyles() {
    if (document.getElementById('gc-pricing-styles')) return;
    const link = document.createElement('link');
    link.id = 'gc-pricing-styles';
    link.rel = 'stylesheet';
    link.href = new URL('contact.css', window.location.href).href;
    document.head.appendChild(link);
  }

  function openPricingPopup() {
    let overlay = document.getElementById('gc-pricing-overlay');
    if (!overlay) {
      overlay = document.createElement('div');
      overlay.id = 'gc-pricing-overlay';
      overlay.className = 'gc-pricing-overlay';
      overlay.innerHTML =
        '<div class="gc-pricing-popup">' +
        '<button type="button" class="gc-pricing-close" aria-label="Close">&times;</button>' +
        '<div id="gc-pricing-content"></div>' +
        '</div>';
      document.body.appendChild(overlay);

      overlay.querySelector('.gc-pricing-close').addEventListener('click', closePricing);
      overlay.addEventListener('click', (e) => {
        if (e.target === overlay) closePricing();
      });
      document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape' && overlay.classList.contains('gc-open')) closePricing();
      });
    }

    function closePricing() {
      overlay.classList.remove('gc-open');
      document.body.classList.remove('gc-pricing-open');
    }

    const content = overlay.querySelector('#gc-pricing-content');
    if (pricingCache) {
      content.innerHTML = pricingCache;
      wirePricingTabs(content);
      overlay.classList.add('gc-open');
      document.body.classList.add('gc-pricing-open');
      return;
    }

    ensurePricingStyles();
    if (typeof window.__getPricingPopupHTML === 'function') {
      content.innerHTML = window.__getPricingPopupHTML();
      pricingCache = content.innerHTML;
      wirePricingTabs(content);
      overlay.classList.add('gc-open');
      document.body.classList.add('gc-pricing-open');
      return;
    }

    content.innerHTML = '<p>Pricing unavailable.</p>';
    overlay.classList.add('gc-open');
    document.body.classList.add('gc-pricing-open');
  }

  function normalizePlatform(v) {
    const p = String(v || '').trim().toLowerCase();
    return PLATFORM_ORDER.includes(p) ? p : 'other';
  }

  function platformLabel(platform) {
    const p = normalizePlatform(platform);
    if (p === 'macos') return 'Mac OS';
    if (p === 'ios') return 'iOS';
    return p.charAt(0).toUpperCase() + p.slice(1);
  }

  function detectPlatform(url, fileName) {
    const hay = `${String(url || '').toLowerCase()} ${String(fileName || '').toLowerCase()}`;
    if (/\bwindows\b|\.exe\b|\.msi\b/.test(hay)) return 'windows';
    if (/\bmac\b|macos|osx|\.dmg\b|\.pkg\b/.test(hay)) return 'macos';
    if (/\bandroid\b|\.apk\b|play\.google\.com/.test(hay)) return 'android';
    if (/\bios\b|iphone|ipad|apps\.apple\.com/.test(hay)) return 'ios';
    if (/\blinux\b|\.deb\b|\.rpm\b|\.appimage\b|\.tar\.gz\b/.test(hay)) return 'linux';
    if (/chrome\.google\.com\/webstore|addons\.mozilla\.org|microsoftedge\.microsoft\.com/.test(hay)) return 'web';
    return 'other';
  }

  function parseDownloadMeta(rawUrl) {
    const url = String(rawUrl || '').trim();
    let host = '';
    let path = '';
    let tag = '';
    let fileName = '';
    let versionNumber = '';
    let versionName = '';

    try {
      const u = new URL(url);
      host = String(u.hostname || '').toLowerCase();
      path = String(u.pathname || '');
      const segments = path.split('/').filter(Boolean);
      fileName = decodeURIComponent(segments[segments.length - 1] || '');

      const relIdx = segments.findIndex((s) => s.toLowerCase() === 'download');
      if (host.includes('github.com') && relIdx >= 1 && segments.length > relIdx + 2) {
        tag = decodeURIComponent(segments[relIdx + 1] || '');
      }
      const match = /(?:^|[^a-z0-9])(v?\d+(?:\.\d+){1,3}(?:[-._][a-z0-9]+)*)/i.exec(`${fileName} ${tag} ${path}`);
      versionNumber = match ? match[1].replace(/_/g, '.') : '';
      if (fileName) versionName = fileName.replace(/\.[a-z0-9]{1,6}$/i, '');
      else if (tag) versionName = tag;
      else if (versionNumber) versionName = `Release ${versionNumber}`;
      else versionName = host || 'Download';
    } catch (_) {
      versionName = 'Download';
    }

    const source = host.includes('github.com')
      ? 'GitHub'
      : host.includes('play.google.com')
      ? 'Google Play'
      : host.includes('apps.apple.com')
      ? 'App Store'
      : host
      ? host.replace(/^www\./, '')
      : 'External';

    return { versionName, versionNumber, tag, fileName, source, platform: detectPlatform(url, fileName) };
  }

  function escapeHtml(s) {
    const d = document.createElement('div');
    d.textContent = String(s || '');
    return d.innerHTML;
  }

  function formatDate(v) {
    const d = v && typeof v.toDate === 'function' ? v.toDate() : new Date(v || Date.now());
    if (Number.isNaN(d.getTime())) return '-';
    return d.toLocaleDateString();
  }

  async function fetchDownloadsRows(force) {
    const now = Date.now();
    if (!force && downloadsRowsCache && now - downloadsFetchedAt < DOWNLOADS_CACHE_MS) return downloadsRowsCache;
    if (!window.firebaseService?.db || !window.firebase?.collection) return [];
    const db = window.firebaseService.db;
    const fb = window.firebase;
    let docs = [];
    try {
      const q = fb.query(
        fb.collection(db, DOWNLOAD_COLLECTION),
        fb.orderBy('updatedAt', 'desc'),
        fb.limit(300)
      );
      const snap = await fb.getDocs(q);
      docs = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
    } catch (_) {
      try {
        const snap = await fb.getDocs(fb.collection(db, DOWNLOAD_COLLECTION));
        docs = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
        docs.sort((a, b) => new Date(b.updatedAt || 0) - new Date(a.updatedAt || 0));
      } catch (err) {
        docs = [];
      }
    }
    downloadsRowsCache = docs.filter((r) => String(r.directUrl || '').trim());
    downloadsHasItemsCache = downloadsRowsCache.length > 0;
    downloadsFetchedAt = Date.now();
    return downloadsRowsCache;
  }

  async function syncDownloadsButtons(container) {
    const buttons = container.querySelectorAll('.gc-nav-downloads-btn');
    if (!buttons.length) return;
    buttons.forEach((btn) => btn.classList.add('gc-hidden'));
    try {
      const rows = await fetchDownloadsRows(false);
      const show = Array.isArray(rows) && rows.length > 0;
      buttons.forEach((btn) => btn.classList.toggle('gc-hidden', !show));
    } catch (_) {
      buttons.forEach((btn) => btn.classList.add('gc-hidden'));
    }
  }

  function refreshAllDownloadsButtons() {
    const containers = document.querySelectorAll('.navbar-interactive-container');
    containers.forEach((c) => syncDownloadsButtons(c));
  }

  function renderDownloadsPopupContent(rows) {
    if (!rows.length) {
      return '<div class="gc-downloads-empty">No downloads are available right now.</div>';
    }
    const enriched = rows.map((r) => {
      const meta = parseDownloadMeta(r.directUrl || '');
      return {
        ...r,
        _platform: normalizePlatform(r.platform || meta.platform),
        _versionName: String(r.versionName || meta.versionName || '-'),
        _version: String(r.version || r.tag || meta.tag || r.versionNumber || meta.versionNumber || '-'),
        _source: String(r.source || meta.source || '-'),
        _fileName: String(r.fileName || meta.fileName || '-')
      };
    });

    const platforms = PLATFORM_ORDER.filter((p) => enriched.some((r) => r._platform === p));
    const first = platforms[0] || 'other';
    const tabs = platforms
      .map((p) => `<button type="button" class="gc-downloads-platform-tab${p === first ? ' active' : ''}" data-gc-platform="${p}">${platformLabel(p)}</button>`)
      .join('');

    const panels = platforms.map((p) => {
      const list = enriched.filter((r) => r._platform === p);
      const rowsHtml = list
        .map((r) => `
          <tr>
            <td>${escapeHtml(r._versionName)}</td>
            <td>${escapeHtml(r._version)}</td>
            <td>${escapeHtml(r._source)}</td>
            <td>${escapeHtml(r._fileName)}</td>
            <td title="${escapeHtml(String(r.description || r.releaseDescription || r.repoDescription || '-'))}">${escapeHtml(String(r.description || r.releaseDescription || r.repoDescription || '-'))}</td>
            <td>${escapeHtml(formatDate(r.updatedAt || r.createdAt))}</td>
            <td><button type="button" class="gc-download-link gc-download-button" data-url="${escapeHtml(r.directUrl || '')}" data-file="${escapeHtml(r._fileName || 'download')}">Download</button></td>
          </tr>
        `)
        .join('');
      return `
        <div class="gc-downloads-panel${p === first ? ' active' : ''}" data-gc-platform-panel="${p}">
          <div class="gc-downloads-table-wrap">
            <table class="gc-downloads-table">
              <thead>
                <tr>
                  <th>Version Name</th>
                  <th>Version</th>
                  <th>Source</th>
                  <th>Filename</th>
                  <th>Description</th>
                  <th>Updated</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody>${rowsHtml}</tbody>
            </table>
          </div>
        </div>
      `;
    }).join('');

    return `
      <h2 class="gc-downloads-title">Downloads</h2>
      <div class="gc-downloads-platform-tabs">${tabs}</div>
      <div class="gc-downloads-panels">${panels}</div>
    `;
  }

  function wireDownloadsTabs(container) {
    if (!container) return;
    const tabs = container.querySelectorAll('.gc-downloads-platform-tab');
    const panels = container.querySelectorAll('.gc-downloads-panel');
    tabs.forEach((tab) => {
      tab.addEventListener('click', () => {
        const id = tab.getAttribute('data-gc-platform');
        tabs.forEach((t) => t.classList.toggle('active', t === tab));
        panels.forEach((p) => p.classList.toggle('active', p.getAttribute('data-gc-platform-panel') === id));
      });
    });
    container.querySelectorAll('.gc-download-button').forEach((btn) => {
      btn.addEventListener('click', () => {
        const url = btn.getAttribute('data-url');
        const file = btn.getAttribute('data-file') || 'download';
        if (!url) return;
        triggerDirectDownload(url, file);
      });
    });
  }

  function triggerDirectDownload(url, fileName) {
    const a = document.createElement('a');
    a.href = String(url || '');
    a.download = String(fileName || 'download').trim() || 'download';
    a.target = '_blank';
    a.rel = 'noopener noreferrer';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
  }

  function closeDownloads() {
    const overlay = document.getElementById('gc-downloads-overlay');
    if (!overlay) return;
    overlay.classList.remove('gc-open');
    document.body.classList.remove('gc-downloads-open');
  }

  async function openDownloadsPopup() {
    let overlay = document.getElementById('gc-downloads-overlay');
    if (!overlay) {
      overlay = document.createElement('div');
      overlay.id = 'gc-downloads-overlay';
      overlay.className = 'gc-downloads-overlay';
      overlay.innerHTML =
        '<div class="gc-downloads-popup">' +
        '<button type="button" class="gc-downloads-close" aria-label="Close">&times;</button>' +
        '<div id="gc-downloads-content"></div>' +
        '</div>';
      document.body.appendChild(overlay);
      overlay.querySelector('.gc-downloads-close').addEventListener('click', closeDownloads);
      overlay.addEventListener('click', (e) => {
        if (e.target === overlay) closeDownloads();
      });
      document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape' && overlay.classList.contains('gc-open')) closeDownloads();
      });
    }

    const content = overlay.querySelector('#gc-downloads-content');
    content.innerHTML = '<div class="gc-downloads-empty">Loading downloads...</div>';
    overlay.classList.add('gc-open');
    document.body.classList.add('gc-downloads-open');

    const rows = await fetchDownloadsRows(true);
    content.innerHTML = renderDownloadsPopupContent(rows);
    wireDownloadsTabs(content);

    // Update all menu buttons in case collection became empty/non-empty since last render.
    document.querySelectorAll('.navbar-interactive-container').forEach((c) => syncDownloadsButtons(c));
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
  window.addEventListener('focus', refreshAllDownloadsButtons);
})();
