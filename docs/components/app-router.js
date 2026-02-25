(function () {
  'use strict';
  const INTRO_LOGO_CYCLE_MS = 600;
  const INTRO_TOTAL_MS = 2200;
  const CONTENT_FADE_MS = 400;

  function ready(fn) {
    if (document.readyState !== 'loading') {
      fn();
    } else {
      document.addEventListener('DOMContentLoaded', fn);
    }
  }

  function showPage(pageId) {
    const pages = document.querySelectorAll('[data-page]');
    pages.forEach((p) => {
      const isActive = p.getAttribute('data-page') === pageId;
      p.classList.toggle('page-active', isActive);
      p.classList.toggle('page-hidden', !isActive);
    });
    document.body.setAttribute('data-current-page', pageId);
    if (typeof history !== 'undefined' && history.replaceState) {
      const hash = pageId === 'home' ? '' : pageId;
      history.replaceState({ page: pageId }, '', hash ? '#' + hash : window.location.pathname);
    }
  }

  function initNav() {
    document.addEventListener('click', (e) => {
      const a = e.target.closest('a');
      if (!a || a.target === '_blank' || (a.href && a.href.startsWith('mailto:'))) return;
      const href = (a.getAttribute('href') || '').trim();
      let path = href.split('#')[0].replace(/^\//, '').toLowerCase();
      try {
        if (path.startsWith('http')) {
          const u = new URL(a.href);
          if (u.origin !== window.location.origin) return;
          path = u.pathname.replace(/^\//, '').toLowerCase();
        }
      } catch (_) {}
      const hash = href.includes('#') ? href.split('#')[1] : '';
      const isHome = path === '' || path === 'index.html' || path === './index.html' || path.endsWith('/') || path === 'index';
      if (isHome) {
        e.preventDefault();
        showPage('home');
      } else if (path === 'gallery.html' || hash === 'gallery') {
        e.preventDefault();
        showPage('gallery');
      } else if (path === 'contact.html' || hash === 'contact') {
        e.preventDefault();
        showPage('contact');
      }
    });
    window.addEventListener('hashchange', () => {
      const hash = (window.location.hash || '').replace(/^#/, '');
      if (hash === 'gallery' || hash === 'contact') showPage(hash);
      else if (!hash) showPage('home');
    });
    const hash = (window.location.hash || '').replace(/^#/, '');
    if (hash === 'gallery' || hash === 'contact') showPage(hash);
  }

  function runIntro() {
    const overlay = document.getElementById('liber-intro-overlay');
    const content = document.getElementById('liber-main-content');
    if (!overlay) return;
    const logo = overlay.querySelector('.liber-intro-logo');
    let cycle = 0;
    const maxCycles = Math.ceil(INTRO_TOTAL_MS / INTRO_LOGO_CYCLE_MS);
    const tick = () => {
      cycle++;
      if (logo) logo.classList.toggle('liber-intro-logo-visible', cycle % 2 === 1);
      if (cycle < maxCycles * 2) {
        setTimeout(tick, INTRO_LOGO_CYCLE_MS / 2);
      } else {
        overlay.classList.add('liber-intro-done');
        setTimeout(() => {
          if (content) content.classList.add('liber-content-visible');
        }, 200);
      }
    };
    if (logo) logo.classList.add('liber-intro-logo-visible');
    setTimeout(tick, INTRO_LOGO_CYCLE_MS / 2);
  }

  function loadExtraPages() {
    const galleryHost = document.getElementById('liber-gallery-host');
    const contactHost = document.getElementById('liber-contact-host');
    const promises = [];
    if (galleryHost && !galleryHost.dataset.loaded) {
      promises.push(
        fetch('gallery.html')
          .then((r) => r.text())
          .then((html) => {
            const parser = new DOMParser();
            const doc = parser.parseFromString(html, 'text/html');
            const main = doc.querySelector('.gallery-container');
            if (main) {
              galleryHost.innerHTML = main.outerHTML;
              galleryHost.dataset.loaded = '1';
              if (typeof window.__gcBoot === 'function') window.__gcBoot();
              if (typeof window.__navbarSubmenuInitForNewContent === 'function') window.__navbarSubmenuInitForNewContent(galleryHost);
            }
          })
          .catch(() => {})
      );
    }
    if (contactHost && !contactHost.dataset.loaded) {
      promises.push(
        fetch('contact.html')
          .then((r) => r.text())
          .then((html) => {
            const parser = new DOMParser();
            const doc = parser.parseFromString(html, 'text/html');
            const main = doc.querySelector('.contact-container');
            if (main) {
              contactHost.innerHTML = main.outerHTML;
              contactHost.dataset.loaded = '1';
              if (typeof window.__gcBoot === 'function') window.__gcBoot();
              if (typeof window.__navbarSubmenuInitForNewContent === 'function') window.__navbarSubmenuInitForNewContent(contactHost);
            }
          })
          .catch(() => {})
      );
    }
    return Promise.all(promises);
  }

  ready(function () {
    const isSpa = document.getElementById('liber-intro-overlay') && document.getElementById('liber-main-content');
    if (!isSpa) return;
    loadExtraPages();
    initNav();
    runIntro();
    const hash = (window.location.hash || '').replace(/^#/, '');
    if (hash === 'gallery' || hash === 'contact') showPage(hash);
  });
})();
