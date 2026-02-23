(function () {
  const ROTATION_MS = 20000;
  const reducedMotion = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  function shuffle(arr) {
    const out = arr.slice();
    for (let i = out.length - 1; i > 0; i -= 1) {
      const j = Math.floor(Math.random() * (i + 1));
      const tmp = out[i];
      out[i] = out[j];
      out[j] = tmp;
    }
    return out;
  }

  function visualItems(items) {
    return (items || []).filter((item) => item && (item.type === 'image' || item.type === 'video') && item.url);
  }

  function textItems(items) {
    return (items || []).filter((item) => item && item.type === 'text' && item.text);
  }

  async function loadFirebase() {
    if (window.__galleryFirebaseCtx) return window.__galleryFirebaseCtx;
    const [{ initializeApp }, firestoreMod] = await Promise.all([
      import('https://www.gstatic.com/firebasejs/12.1.0/firebase-app.js'),
      import('https://www.gstatic.com/firebasejs/12.1.0/firebase-firestore.js')
    ]);
    const { getFirestore, collection, getDocs, query, where, orderBy } = firestoreMod;
    let cfg = window.LIBER_FIREBASE_CONFIG || null;
    if (!cfg && window.secureKeyManager && typeof window.secureKeyManager.getKeys === 'function') {
      for (let attempt = 0; attempt < 3; attempt++) {
        try {
          const keys = await window.secureKeyManager.getKeys();
          cfg = keys && keys.firebase ? keys.firebase : null;
          if (cfg) break;
        } catch (_) {
          if (attempt === 2) cfg = null;
          else await new Promise((r) => setTimeout(r, 500 * (attempt + 1)));
        }
      }
    }
    if (!cfg || !cfg.apiKey || !cfg.projectId) {
      throw new Error('Gallery config unavailable');
    }
    const app = initializeApp(cfg, 'landing-gallery');
    const db = getFirestore(app);
    window.__galleryFirebaseCtx = { db, collection, getDocs, query, where, orderBy };
    return window.__galleryFirebaseCtx;
  }

  async function loadProjects() {
    const { db, collection, getDocs, query, where, orderBy } = await loadFirebase();
    const projectsCol = collection(db, 'galleryProjects');
    let projectSnap;
    try {
      projectSnap = await getDocs(query(projectsCol, where('isPublished', '==', true), orderBy('updatedAtTS', 'desc')));
    } catch (e) {
      try {
        projectSnap = await getDocs(query(projectsCol, where('isPublished', '==', true)));
      } catch (_) {
        throw e;
      }
    }

    const projects = [];
    for (const doc of projectSnap.docs) {
      const project = { id: doc.id, ...doc.data(), items: [] };
      let itemSnap;
      try {
        itemSnap = await getDocs(query(collection(db, 'galleryProjects', doc.id, 'items'), where('isPublished', '==', true), orderBy('sortOrder', 'asc')));
      } catch (_) {
        itemSnap = await getDocs(query(collection(db, 'galleryProjects', doc.id, 'items'), where('isPublished', '==', true)));
      }
      project.items = itemSnap.docs.map((d) => ({ id: d.id, ...d.data() })).sort((a, b) => Number(a.sortOrder || 0) - Number(b.sortOrder || 0));
      projects.push(project);
    }
    projects.sort((a, b) => {
      const at = (a.updatedAtTS && a.updatedAtTS.toMillis) ? a.updatedAtTS.toMillis() : new Date(a.updatedAt || a.createdAt || 0).getTime();
      const bt = (b.updatedAtTS && b.updatedAtTS.toMillis) ? b.updatedAtTS.toMillis() : new Date(b.updatedAt || b.createdAt || 0).getTime();
      return bt - at;
    });
    const withMedia = projects.filter((p) => {
      const v = visualItems(p.items || []);
      return v.length > 0;
    });
    if (withMedia.length < projects.length && projects.length > 0) {
      console.warn('[Gallery] Some projects have no published items with media:', projects.map((p) => ({ id: p.id, title: p.title, itemCount: (p.items || []).length, withUrl: (p.items || []).filter((i) => i && i.url).length })));
    }
    return withMedia;
  }

  function createMediaElement(item, mutedVideo) {
    if (!item || !item.url) return '';
    if (item.type === 'video') {
      return `<video src="${item.url}" ${mutedVideo ? 'muted autoplay loop playsinline' : 'controls playsinline'}></video>`;
    }
    return `<img src="${item.url}" alt="${item.caption || 'Gallery item'}" loading="lazy" />`;
  }

  function buildCard(project, modeClass) {
    const visuals = visualItems(project.items);
    const first = visuals[0];
    if (!first) return '';
    return (
      `<article class="gc-card ${modeClass}" data-project-id="${project.id}" tabindex="0">` +
      `<div class="gc-media" data-rotation-index="0">${createMediaElement(first, true)}</div>` +
      `<div class="gc-overlay"><div><h3>${project.title || 'Project'}</h3><p>${project.year || ''}</p></div></div>` +
      '</article>'
    );
  }

  const INTRO_MS = 3000;
  function wireCardIntro(container) {
    const cards = Array.from(container.querySelectorAll('.gc-card[data-project-id]:not([data-gc-intro-done])'));
    if (!cards.length || !('IntersectionObserver' in window)) return;
    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (!entry.isIntersecting) return;
          const card = entry.target;
          observer.unobserve(card);
          card.dataset.gcIntroDone = '1';
          card.classList.add('gc-intro');
          window.setTimeout(() => card.classList.remove('gc-intro'), INTRO_MS);
        });
      },
      { threshold: 0.1, rootMargin: '20px' }
    );
    cards.forEach((card) => observer.observe(card));
  }

  function wireRotations(container, projectById) {
    const cards = Array.from(container.querySelectorAll('.gc-card[data-project-id]:not([data-gc-rotation-wired])'));
    cards.forEach((card) => {
      card.dataset.gcRotationWired = '1';
      const id = card.getAttribute('data-project-id');
      const project = projectById.get(id);
      if (!project) return;
      const visuals = visualItems(project.items);
      if (visuals.length < 2 || reducedMotion) return;
      let idx = 0;
      let timer = null;
      const mediaEl = card.querySelector('.gc-media');
      const rotate = () => {
        idx = (idx + 1) % visuals.length;
        card.classList.add('fade-swap');
        window.setTimeout(() => {
          mediaEl.innerHTML = createMediaElement(visuals[idx], true);
          card.classList.remove('fade-swap');
          mediaEl.classList.add('gc-new-media');
          requestAnimationFrame(() => {
            requestAnimationFrame(() => mediaEl.classList.remove('gc-new-media'));
          });
        }, 320);
      };
      const start = () => {
        if (timer) return;
        timer = window.setInterval(rotate, ROTATION_MS);
      };
      const stop = () => {
        if (!timer) return;
        window.clearInterval(timer);
        timer = null;
      };
      card.addEventListener('mouseenter', stop);
      card.addEventListener('mouseleave', start);
      window.setTimeout(start, 400);
    });
  }

  function openPopup(project) {
    let popup = document.getElementById('gc-popup');
    if (!popup) {
      popup = document.createElement('div');
      popup.id = 'gc-popup';
      popup.className = 'gc-popup';
      document.body.appendChild(popup);
    }
    const visuals = visualItems(project.items);
    const texts = textItems(project.items);
    let idx = 0;
    const dotsHtml = visuals.length > 1
      ? `<div class="gc-popup-dots" role="tablist">${visuals.map((_, i) => `<button type="button" class="gc-dot" data-gc-idx="${i}" aria-label="Slide ${i + 1}"></button>`).join('')}</div>`
      : '';
    const bodyHtml = `<div class="gc-popup-body"><h3>${project.title || 'Project'} ${project.year || ''}</h3><p>${project.description || ''}</p><div>${texts.map((t) => `<p>${t.text}</p>`).join('')}</div></div>`;
    const slidesHtml = visuals.map((v) => `<div class="gc-popup-slide">${createMediaElement(v, false)}</div>`).join('');
    popup.innerHTML =
      `<div class="gc-popup-card" role="dialog" aria-modal="true">` +
      `<div class="gc-popup-media">` +
      `<button class="gc-slider-btn gc-popup-close" data-gc-close="1">Close</button>` +
      `<div class="gc-popup-track">${slidesHtml}</div>` +
      dotsHtml +
      `</div>` + bodyHtml + `</div>`;
    const mediaEl = popup.querySelector('.gc-popup-media');
    const track = popup.querySelector('.gc-popup-track');
    const dots = popup.querySelectorAll('.gc-popup-dots .gc-dot');
    const setIdx = (i) => {
      idx = Math.max(0, Math.min(i, visuals.length - 1));
      if (track) track.style.transform = `translateX(-${idx * 100}%)`;
      dots.forEach((d, j) => { d.classList.toggle('active', j === idx); d.setAttribute('aria-selected', j === idx); });
    };
    setIdx(0);
    const goPrev = () => setIdx(idx - 1);
    const goNext = () => setIdx(idx + 1);
    if (mediaEl && track && visuals.length > 1) {
      let startX = 0;
      let startY = 0;
      let startIdx = 0;
      let isHorizontalSwipe = null; /* null=undecided, true=swipe, false=scroll */
      const SLOPE = 0.5; /* vertical wins: |dx| > SLOPE*|dy| => horizontal, else scroll */
      const onStart = (x, y) => { startX = x; startY = y; startIdx = idx; isHorizontalSwipe = null; };
      const onMove = (x) => {
        const w = mediaEl.offsetWidth || 400;
        const delta = (startX - x) / w;
        const raw = startIdx + delta;
        const clamped = Math.max(-0.2, Math.min(visuals.length - 1 + 0.2, raw));
        track.style.transition = 'none';
        track.style.transform = `translateX(-${clamped * 100}%)`;
      };
      const onEnd = (x) => {
        track.style.transition = '';
        const w = mediaEl.offsetWidth || 400;
        const delta = startX - x;
        if (Math.abs(delta) > w * 0.15) {
          if (delta > 0) goNext();
          else goPrev();
        } else setIdx(idx);
      };
      mediaEl.addEventListener('mousedown', (e) => {
        onStart(e.clientX, e.clientY);
        const m = (ev) => onMove(ev.clientX);
        const u = (ev) => { document.removeEventListener('mousemove', m); document.removeEventListener('mouseup', u); onEnd(ev.clientX); };
        document.addEventListener('mousemove', m);
        document.addEventListener('mouseup', u);
      });
      let touching = false;
      mediaEl.addEventListener('touchstart', (e) => {
        touching = true;
        const t = e.touches[0];
        onStart(t ? t.clientX : 0, t ? t.clientY : 0);
      }, { passive: true });
      mediaEl.addEventListener('touchmove', (e) => {
        if (!touching || !e.touches[0]) return;
        const t = e.touches[0];
        const dx = Math.abs(t.clientX - startX);
        const dy = Math.abs(t.clientY - startY);
        if (isHorizontalSwipe === null) {
          isHorizontalSwipe = dx > SLOPE * dy;
        }
        if (isHorizontalSwipe) {
          onMove(t.clientX);
          e.preventDefault();
        }
      }, { passive: false });
      mediaEl.addEventListener('touchend', (e) => {
        if (touching && e.changedTouches && e.changedTouches[0]) {
          if (isHorizontalSwipe) onEnd(e.changedTouches[0].clientX);
          touching = false;
        }
      }, { passive: true });
      mediaEl.addEventListener('touchcancel', () => { touching = false; }, { passive: true });
    }
    dots.forEach((dot, i) => dot.addEventListener('click', (e) => { e.stopPropagation(); setIdx(i); }));
    popup.classList.add('open');
    document.body.classList.add('gc-popup-open');
    const close = () => {
      popup.classList.remove('open');
      document.body.classList.remove('gc-popup-open');
      popup.removeEventListener('click', handler);
    };
    const handler = (ev) => {
      const target = ev.target;
      if (!(target instanceof HTMLElement)) return;
      if (target.dataset.gcClose || target === popup) close();
    };
    popup.addEventListener('click', handler);
  }

  function wirePopupOpener(container, projectById) {
    container.addEventListener('click', (e) => {
      if (window.__gcSuppressNextClick) { window.__gcSuppressNextClick = false; return; }
      const card = e.target.closest('.gc-card[data-project-id]');
      if (!card || !container.contains(card)) return;
      const id = card.getAttribute('data-project-id');
      const project = projectById.get(id);
      if (!project) return;
      e.preventDefault();
      e.stopPropagation();
      openPopup(project);
    });
    container.addEventListener('keydown', (e) => {
      const card = e.target.closest('.gc-card[data-project-id]');
      if (!card || !container.contains(card)) return;
      if (e.key !== 'Enter' && e.key !== ' ') return;
      const id = card.getAttribute('data-project-id');
      const project = projectById.get(id);
      if (!project) return;
      e.preventDefault();
      openPopup(project);
    });
  }

  function pickProjects(source, count) {
    const byId = new Map();
    source.forEach((p) => {
      if (visualItems(p.items || []).length > 0) byId.set(p.id, p);
    });
    if (!byId.size || count <= 0) return [];
    return shuffle([...byId.values()]).slice(0, count);
  }

  function mountTiles(host, projects, projectById) {
    const count = Number(host.dataset.cardCount || 9);
    const selected = pickProjects(projects, count);
    if (!selected.length) {
      host.innerHTML = '<div class="gc-template"><p class="gc-empty">No published gallery projects yet.</p></div>';
      return;
    }
    host.innerHTML = `<div class="gc-template"><div class="gc-grid">${selected.map((p) => buildCard(p, '')).join('')}</div></div>`;
    wireRotations(host, projectById);
    wireCardIntro(host);
    wirePopupOpener(host, projectById);
  }

  const SINGLE_SLIDER_AUTO_MS = 30000;

  function mountSingleSlider(host, projects, projectById) {
    const count = Number(host.dataset.cardCount || 9);
    let selected = pickProjects(projects, count);
    if (!selected.length) {
      host.innerHTML = '<div class="gc-template"><p class="gc-empty">No published gallery projects yet.</p></div>';
      return;
    }
    const renderTrack = (projs) => projs.map((p) => `<div class="gc-slide"><div class="gc-slide-inner">${buildCard(p, 'gc-card--slider')}</div></div>`).join('');
    host.innerHTML =
      `<div class="gc-template gc-single-slider">` +
      `<div class="gc-slider-shell gc-single-shell"><div class="gc-slider-track gc-single-track">${renderTrack(selected)}</div></div>` +
      `</div>`;
    const shell = host.querySelector('.gc-single-shell');
    const track = host.querySelector('.gc-single-track');
    let page = 0;
    let startX = 0;
    let startPage = 0;
    let didDrag = false;
    let autoTimer = null;
    const MAX_SLIDE_WIDTH = 2200;
    const appendQueue = () => {
      const more = pickProjects(projects, count);
      if (!more.length) return;
      selected = selected.concat(more);
      track.insertAdjacentHTML('beforeend', renderTrack(more));
      wireRotations(host, projectById);
      wireCardIntro(host);
    };
    const ensureQueueAhead = () => {
      if (page >= selected.length - 2) appendQueue();
    };
    const update = () => {
      page = Math.max(0, Math.min(page, selected.length - 1));
      const rawWidth = shell && shell.offsetWidth > 0 ? shell.offsetWidth : (typeof window !== 'undefined' ? Math.min(900, window.innerWidth || 900) : 600);
      const slideWidth = Math.min(MAX_SLIDE_WIDTH, rawWidth);
      track.style.width = `${selected.length * slideWidth}px`;
      track.querySelectorAll('.gc-slide').forEach((s) => { s.style.flex = `0 0 ${slideWidth}px`; s.style.minWidth = `${slideWidth}px`; s.style.maxWidth = `${slideWidth}px`; });
      track.style.transform = `translateX(-${page * slideWidth}px)`;
    };
    const onStart = (x) => { didDrag = false; startX = x; startPage = page; stopAutoAdvance(); };
    const onMove = (x) => {
      if (Math.abs(x - startX) > 5) { didDrag = true; window.__gcSuppressNextClick = true; }
      const slideWidth = shell ? shell.offsetWidth : 400;
      const delta = startX - x;
      const pageOffset = delta / slideWidth;
      track.style.transition = 'none';
      track.style.transform = `translateX(-${(startPage + pageOffset) * slideWidth}px)`;
    };
    const startAutoAdvance = () => {
      stopAutoAdvance();
      autoTimer = setInterval(() => {
        ensureQueueAhead();
        page = Math.min(page + 1, selected.length - 1);
        update();
      }, SINGLE_SLIDER_AUTO_MS);
    };
    const stopAutoAdvance = () => {
      if (autoTimer) { clearInterval(autoTimer); autoTimer = null; }
    };
    const onEnd = (x) => {
      track.style.transition = '';
      const slideWidth = shell ? shell.offsetWidth : 400;
      const delta = startX - x;
      if (Math.abs(delta) > slideWidth * 0.2) {
        if (delta > 0) {
          ensureQueueAhead();
          page = Math.min(page + 1, selected.length - 1);
        } else {
          page = Math.max(page - 1, 0);
        }
      }
      update();
      startAutoAdvance();
    };
    let isHorizontalSwipe = null;
    const SLOPE = 0.5; /* vertical scroll wins over horizontal swipe */
    let startY = 0;
    const addDrag = (el) => {
      el.addEventListener('mousedown', (e) => { onStart(e.clientX); const m = (ev) => onMove(ev.clientX); const u = (ev) => { document.removeEventListener('mousemove', m); document.removeEventListener('mouseup', u); onEnd(ev.clientX); }; document.addEventListener('mousemove', m); document.addEventListener('mouseup', u); });
      let touched = false;
      el.addEventListener('touchstart', (e) => {
        touched = true;
        const t = e.touches[0];
        if (t) { startY = t.clientY; onStart(t.clientX); }
        isHorizontalSwipe = null;
      }, { passive: true });
      el.addEventListener('touchmove', (e) => {
        if (!touched || !e.touches[0]) return;
        const t = e.touches[0];
        const dx = Math.abs(t.clientX - startX);
        const dy = Math.abs(t.clientY - startY);
        if (isHorizontalSwipe === null) isHorizontalSwipe = dx > SLOPE * dy;
        if (isHorizontalSwipe) {
          onMove(t.clientX);
          e.preventDefault();
        }
      }, { passive: false });
      el.addEventListener('touchend', (e) => {
        if (touched && e.changedTouches && e.changedTouches[0]) {
          if (isHorizontalSwipe) onEnd(e.changedTouches[0].clientX);
          else startAutoAdvance();
          touched = false;
        }
      }, { passive: true });
      el.addEventListener('touchcancel', () => { touched = false; }, { passive: true });
    };
    addDrag(shell);
    const ro = new ResizeObserver(update);
    if (shell) ro.observe(shell);
    update();
    requestAnimationFrame(() => requestAnimationFrame(update));
    startAutoAdvance();
    wireRotations(host, projectById);
    wireCardIntro(host);
    wirePopupOpener(host, projectById);
  }

  function mountFullWidth(host, projects, projectById) {
    const count = Number(host.dataset.cardCount || 10) || 999;
    const selected = pickProjects(projects, count);
    if (!selected.length) {
      host.innerHTML = '<div class="gc-template"><p class="gc-empty">No published gallery projects yet.</p></div>';
      return;
    }
    const noDuplicates = host.dataset.noDuplicates === 'true' || host.dataset.noDuplicates === '';
    const gap = 18;
    const cardWidth = 420 + gap;
    let displaySet = selected.slice();
    const renderTrack = (numSets) => {
      const base = displaySet.map((p) => buildCard(p, 'gc-card--full'));
      if (numSets <= 1) return base.join('');
      const out = [];
      for (let i = 0; i < numSets; i++) out.push(...base);
      return out.join('');
    };
    const vw = typeof window !== 'undefined' ? window.innerWidth || 1200 : 1200;
    const setW = displaySet.length * cardWidth;
    const needSets = noDuplicates ? 1 : (setW >= vw ? 2 : Math.min(3, Math.ceil(vw / setW) + 1));
    host.innerHTML = `<div class="gc-template gc-fullwrap"><div class="gc-full-track">${renderTrack(needSets)}</div></div>`;
    const wrap = host.querySelector('.gc-fullwrap');
    const track = host.querySelector('.gc-full-track');
    let offset = 0;
    let startX = 0;
    let startOffset = 0;
    let autoScrollId = null;
    const setWidth = () => displaySet.length * cardWidth;
    const cycleReset = () => {
      track.style.transition = 'none';
      if (noDuplicates) {
        offset = 0;
      } else {
        offset %= setWidth();
        if (offset < 0) offset += setWidth();
        displaySet = shuffle(displaySet.slice());
        track.innerHTML = renderTrack(needSets);
        wireRotations(host, projectById);
        wireCardIntro(host);
      }
      requestAnimationFrame(() => { track.style.transition = ''; });
    };
    const getViewW = () => wrap.offsetWidth || (typeof window !== 'undefined' ? window.innerWidth : 1200);
    const update = () => {
      const totalW = track.offsetWidth || setWidth() * (noDuplicates ? 1 : 3);
      const viewW = getViewW();
      const maxO = Math.max(0, totalW - viewW);
      offset = Math.max(0, Math.min(offset, maxO));
      track.style.transform = `translateX(-${offset}px)`;
    };
    const onStart = (x) => {
      startX = x;
      startOffset = offset;
      if (autoScrollId) { cancelAnimationFrame(autoScrollId); autoScrollId = null; }
      track.style.transition = 'none';
    };
    const onMove = (x) => {
      if (Math.abs(x - startX) > 5) window.__gcSuppressNextClick = true;
      offset = startOffset + (startX - x);
      update();
    };
    const onEndRestoreTransition = () => { track.style.transition = ''; };
    const onEnd = () => { onEndRestoreTransition(); if (autoScrollId) return; startAutoScroll(); };
    let startY = 0;
    let isHorizontalSwipe = null;
    const SWIPE_SLOPE = 0.5; /* vertical scroll wins over horizontal swipe */
    const addDrag = (el) => {
      el.addEventListener('mousedown', (e) => { onStart(e.clientX); const m = (ev) => onMove(ev.clientX); const u = () => { document.removeEventListener('mousemove', m); document.removeEventListener('mouseup', u); onEnd(); }; document.addEventListener('mousemove', m); document.addEventListener('mouseup', u); });
      let touching = false;
      el.addEventListener('touchstart', (e) => {
        touching = true;
        const t = e.touches[0];
        if (t) { startY = t.clientY; onStart(t.clientX); }
        isHorizontalSwipe = null;
      }, { passive: true });
      el.addEventListener('touchmove', (e) => {
        if (!touching || !e.touches[0]) return;
        const t = e.touches[0];
        const dx = Math.abs(t.clientX - startX);
        const dy = Math.abs(t.clientY - startY);
        if (isHorizontalSwipe === null) isHorizontalSwipe = dx > SWIPE_SLOPE * dy;
        if (isHorizontalSwipe) {
          onMove(t.clientX);
          e.preventDefault();
        }
      }, { passive: false });
      el.addEventListener('touchend', (e) => {
        if (touching && e.changedTouches && e.changedTouches[0]) {
          if (isHorizontalSwipe) onEnd();
          touching = false;
        }
      }, { passive: true });
      el.addEventListener('touchcancel', () => { touching = false; }, { passive: true });
    };
    addDrag(wrap);
    wrap.addEventListener('click', (e) => { if (e.target.closest('.gc-card')) return; e.preventDefault(); });
    const AUTO_PX_PER_MS = 0.016;
    const startAutoScroll = () => {
      let lastT = 0;
      const tick = (t) => {
        const viewW = getViewW();
        if (viewW > 0) {
          const dt = lastT ? Math.min(t - lastT, 100) : 16;
          lastT = t;
          const oneSet = setWidth();
          offset += AUTO_PX_PER_MS * dt;
          if (offset >= oneSet) cycleReset();
        }
        update();
        autoScrollId = requestAnimationFrame(tick);
      };
      autoScrollId = requestAnimationFrame(tick);
    };
    startAutoScroll();
    wireRotations(host, projectById);
    wireCardIntro(host);
    wirePopupOpener(host, projectById);
    requestAnimationFrame(update);
    const ro = new ResizeObserver(() => update());
    ro.observe(wrap);
  }

  async function boot() {
    const hosts = Array.from(document.querySelectorAll('[data-gallery-template]'));
    if (!hosts.length) return;
    try {
      const projects = await loadProjects();
      if (!projects.length) {
        console.warn('[Gallery] No published projects with media found. In Gallery Control: ensure the project has "Published" checked, each item has media (image/video with URL), and if you published after adding items, re-open the project in Edit mode and click Save to sync visibility to items.');
        hosts.forEach((host) => { host.innerHTML = '<div class="gc-template"><p class="gc-empty">No published gallery projects yet.</p></div>'; });
        return;
      }
      const projectById = new Map(projects.map((p) => [p.id, p]));
      hosts.forEach((host) => {
        const kind = host.getAttribute('data-gallery-template');
        if (kind === 'tile') mountTiles(host, projects, projectById);
        else if (kind === 'single-slider') mountSingleSlider(host, projects, projectById);
        else if (kind === 'full-width-slider') mountFullWidth(host, projects, projectById);
      });
    } catch (err) {
      hosts.forEach((host) => { host.innerHTML = '<div class="gc-template">Gallery is temporarily unavailable.</div>'; });
      console.error(err);
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else {
    boot();
  }
})();
