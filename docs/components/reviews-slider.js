/**
 * Load and display project reviews in a slider on the landing page.
 * Fetches from projectReviews collection (all projects).
 */
(function () {
  'use strict';

  const MAX_REVIEWS = 30;

  function escapeHtml(s) {
    if (!s) return '';
    const div = document.createElement('div');
    div.textContent = String(s);
    return div.innerHTML;
  }

  function renderReviews(reviews) {
    const container = document.getElementById('home-reviews-slider');
    const track = document.getElementById('home-reviews-track');
    const prevBtn = document.getElementById('home-reviews-prev');
    const nextBtn = document.getElementById('home-reviews-next');
    if (!container || !track) return;

    if (!reviews || reviews.length === 0) {
      container.classList.add('hidden');
      return;
    }

    track.innerHTML = reviews
      .map(function (r) {
        const text = escapeHtml(r.text || '').trim();
        const name = escapeHtml(r.userName || 'User').trim();
        if (!text) return '';
        return (
          '<div class="home-review-card">' +
          '<p class="home-review-text">' + text + '</p>' +
          '<div class="home-review-by">— ' + name + '</div>' +
          '</div>'
        );
      })
      .filter(Boolean)
      .join('');

    if (track.innerHTML === '') {
      container.classList.add('hidden');
      return;
    }

    container.classList.remove('hidden');
    setTimeout(function () {
      container.classList.add('is-visible');
    }, 100);

    if (prevBtn) {
      prevBtn.onclick = function () {
        track.scrollBy({ left: -track.offsetWidth * 0.8, behavior: 'smooth' });
      };
    }
    if (nextBtn) {
      nextBtn.onclick = function () {
        track.scrollBy({ left: track.offsetWidth * 0.8, behavior: 'smooth' });
      };
    }
  }

  function loadReviews() {
    const fs = window.firebaseService;
    const fb = (fs && fs.firebase) || window.firebase;
    if (!fs || !fs.db || !fb || !fb.collection || !fb.getDocs) return;

    try {
      const col = fb.collection(fs.db, 'projectReviews');
      const q = fb.query(col, fb.orderBy('createdAt', 'desc'), fb.limit(MAX_REVIEWS));
      fb.getDocs(q)
        .then(function (snap) {
          const reviews = [];
          snap.forEach(function (doc) {
            const d = doc.data();
            if (d && d.text) reviews.push(d);
          });
          renderReviews(reviews);
        })
        .catch(function (err) {
          console.warn('Reviews load failed:', err);
          document.getElementById('home-reviews-slider')?.classList.add('hidden');
        });
    } catch (e) {
      console.warn('Reviews init failed:', e);
      document.getElementById('home-reviews-slider')?.classList.add('hidden');
    }
  }

  function waitAndLoad() {
    const fs = window.firebaseService;
    if (fs && fs.isInitialized && fs.db) {
      loadReviews();
      return;
    }
    var attempts = 0;
    var t = setInterval(function () {
      attempts++;
      if (window.firebaseService && window.firebaseService.isInitialized && window.firebaseService.db) {
        clearInterval(t);
        loadReviews();
      } else if (attempts >= 50) {
        clearInterval(t);
      }
    }, 200);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', waitAndLoad);
  } else {
    waitAndLoad();
  }
})();
