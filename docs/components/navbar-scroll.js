(function () {
  const THRESHOLD = 80;
  const HIDDEN_CLASS = 'gc-navbar-hidden';

  function init() {
    const container = document.querySelector('.navbar-interactive-container');
    if (!container) return;
    let lastY = window.scrollY || 0;
    let ticking = false;

    function update() {
      const y = window.scrollY || 0;
      if (y <= THRESHOLD) {
        container.classList.remove(HIDDEN_CLASS);
      } else if (y > lastY) {
        container.classList.add(HIDDEN_CLASS);
      } else if (y < lastY) {
        container.classList.remove(HIDDEN_CLASS);
      }
      lastY = y;
      ticking = false;
    }

    function onScroll() {
      if (!ticking) {
        requestAnimationFrame(update);
        ticking = true;
      }
    }

    window.addEventListener('scroll', onScroll, { passive: true });
    update();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
