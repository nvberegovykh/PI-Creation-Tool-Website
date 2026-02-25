(function () {
  'use strict';
  const INTRO_FORCE_MS = 5000;

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

  function forceIntroComplete() {
    const overlay = document.getElementById('liber-intro-overlay');
    const content = document.getElementById('liber-main-content');
    if (overlay && !overlay.classList.contains('liber-intro-done')) {
      overlay.classList.add('liber-intro-done');
      overlay.style.pointerEvents = 'none';
      overlay.style.visibility = 'hidden';
    }
    if (content && !content.classList.contains('liber-content-visible')) {
      content.classList.add('liber-content-visible');
      content.style.pointerEvents = 'auto';
    }
  }

  function repairPointerEvents() {
    const overlay = document.getElementById('liber-intro-overlay');
    const content = document.getElementById('liber-main-content');
    const activePage = document.querySelector('[data-page].page-active');
    if (overlay && overlay.classList.contains('liber-intro-done')) {
      overlay.style.pointerEvents = 'none';
    }
    if (content && content.classList.contains('liber-content-visible')) {
      content.style.pointerEvents = 'auto';
    }
    if (activePage) {
      activePage.style.pointerEvents = 'auto';
    }
  }

  function runIntro() {
    const overlay = document.getElementById('liber-intro-overlay');
    const content = document.getElementById('liber-main-content');
    if (!overlay) return;
    const introStart = Date.now();
    const logo = overlay.querySelector('.liber-intro-logo');
    if (logo) requestAnimationFrame(() => logo.classList.add('liber-intro-logo-visible'));

    document.addEventListener('visibilitychange', () => {
      if (document.visibilityState !== 'visible') return;
      if (Date.now() - introStart >= INTRO_FORCE_MS) {
        forceIntroComplete();
      }
      repairPointerEvents();
    });
  }

  function loadExtraPages() {
    const galleryHost = document.getElementById('liber-gallery-host');
    const contactHost = document.getElementById('liber-contact-host');
    const overlay = document.getElementById('liber-intro-overlay');
    const content = document.getElementById('liber-main-content');
    const dotEls = overlay ? Array.from(overlay.querySelectorAll('.liber-intro-dot')) : [];
    const dotOrder = dotEls.map((_, i) => i).sort(() => Math.random() - 0.5);
    let loadedCount = 0;
    const MIN_INTRO_MS = 800;

    function markLoaded() {
      const idx = dotOrder[loadedCount];
      if (dotEls[idx]) dotEls[idx].classList.add('loaded');
      loadedCount++;
    }

    function finishIntro() {
      if (!overlay || overlay.classList.contains('liber-intro-done')) return;
      overlay.classList.add('liber-intro-done');
      setTimeout(() => {
        if (content) content.classList.add('liber-content-visible');
      }, 50);
    }

    const fetchPromises = [];
    if (galleryHost && !galleryHost.dataset.loaded) {
      fetchPromises.push(
        fetch('gallery.html')
          .then((r) => r.text())
          .then((html) => {
            const parser = new DOMParser();
            const doc = parser.parseFromString(html, 'text/html');
            const main = doc.querySelector('.gallery-container');
            if (main) {
              galleryHost.innerHTML = main.outerHTML;
              galleryHost.dataset.loaded = '1';
              if (typeof window.__navbarSubmenuInitForNewContent === 'function') window.__navbarSubmenuInitForNewContent(galleryHost);
            }
            markLoaded();
          })
          .catch(() => { markLoaded(); })
      );
    } else {
      markLoaded();
    }
    if (contactHost && !contactHost.dataset.loaded) {
      fetchPromises.push(
        fetch('contact.html')
          .then((r) => r.text())
          .then((html) => {
            const parser = new DOMParser();
            const doc = parser.parseFromString(html, 'text/html');
            const main = doc.querySelector('.contact-container');
            if (main) {
              contactHost.innerHTML = main.outerHTML;
              contactHost.dataset.loaded = '1';
              if (typeof window.__navbarSubmenuInitForNewContent === 'function') window.__navbarSubmenuInitForNewContent(contactHost);
            }
            markLoaded();
          })
          .catch(() => { markLoaded(); })
      );
    } else {
      markLoaded();
    }

    window.__introStartTime = Date.now();

    function runBootAndFinish() {
      const bootFn = typeof window.__gcBoot === 'function' ? window.__gcBoot : null;
      if (bootFn) {
        Promise.resolve(bootFn({ onGalleryMount: markLoaded })).then(() => {
          const elapsed = Date.now() - window.__introStartTime;
          const remaining = Math.max(0, MIN_INTRO_MS - elapsed);
          setTimeout(finishIntro, remaining);
        }).catch(() => {
          dotEls.forEach((d) => d.classList.add('loaded'));
          setTimeout(finishIntro, MIN_INTRO_MS);
        });
      } else {
        dotEls.forEach((d) => d.classList.add('loaded'));
        setTimeout(finishIntro, MIN_INTRO_MS);
      }
    }

    if (fetchPromises.length === 0) {
      runBootAndFinish();
      return Promise.resolve();
    }

    return Promise.all(fetchPromises).then(runBootAndFinish);
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
