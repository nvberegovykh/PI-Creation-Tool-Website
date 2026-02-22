(function () {
  function init() {
    const container = document.querySelector('.navbar-interactive-container');
    if (!container) {
      setTimeout(init, 100);
      return;
    }

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
        '<a href="liber-apps/index.html" target="_blank" rel="noopener" class="gc-nav-submenu-btn">Liber Apps</a>' +
        '</div>';
      desktopMenu.appendChild(zone);

      zone.querySelector('[data-gc-nav="services"]').addEventListener('click', openPricingPopup);

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
    }
  }

  let pricingCache = null;

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
      overlay.classList.add('gc-open');
      document.body.classList.add('gc-pricing-open');
      return;
    }

    const source = document.getElementById('gc-pricing-source');
    if (source) {
      // Use outerHTML for full structure; strip id so #gc-pricing-source { display:none } doesn't hide the popup copy
      let html = source.outerHTML || source.innerHTML;
      if (html && html.trim()) {
        html = html.replace(/\s*id=["']gc-pricing-source["']/i, '');
        pricingCache = html;
        content.innerHTML = pricingCache;
      } else {
        content.innerHTML = '<p>Pricing unavailable.</p>';
      }
      overlay.classList.add('gc-open');
      document.body.classList.add('gc-pricing-open');
      return;
    }

    ensurePricingStyles();
    const showPopup = () => {
      overlay.classList.add('gc-open');
      document.body.classList.add('gc-pricing-open');
    };
    const contactUrl = new URL('contact.html', window.location.href).href;
    fetch(contactUrl)
      .then((r) => r.text())
      .then((html) => {
        const parser = new DOMParser();
        const doc = parser.parseFromString(html, 'text/html');
        const pricing = doc.querySelector('.pricing10-wrapper');
        if (pricing) {
          let html = pricing.outerHTML;
          html = html.replace(/\s*id=["']gc-pricing-source["']/i, '');
          pricingCache = html;
          content.innerHTML = html;
        } else {
          content.innerHTML = '<p>Pricing unavailable.</p>';
        }
        requestAnimationFrame(() => showPopup());
      })
      .catch(() => {
        content.innerHTML = '<p>Could not load pricing.</p>';
        requestAnimationFrame(() => showPopup());
      });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
