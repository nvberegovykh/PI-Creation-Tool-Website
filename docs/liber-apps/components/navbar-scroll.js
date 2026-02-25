(function () {
  function init() {
    const container = document.querySelector('.navbar-interactive-container');
    if (!container) {
      setTimeout(init, 100);
      return;
    }
    container.classList.remove('gc-navbar-hidden');
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
