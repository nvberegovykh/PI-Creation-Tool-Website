(function () {
  const ROTATION_MS = 10000;
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
    const cards = Array.from(container.querySelectorAll('.gc-card[data-project-id]'));
    if (!cards.length || !('IntersectionObserver' in window)) return;
    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (!entry.isIntersecting) return;
          const card = entry.target;
          observer.unobserve(card);
          card.classList.add('gc-intro');
          window.setTimeout(() => card.classList.remove('gc-intro'), INTRO_MS);
        });
      },
      { threshold: 0.1, rootMargin: '20px' }
    );
    cards.forEach((card) => observer.observe(card));
  }

  function wireRotations(container, projectById) {
    const cards = Array.from(container.querySelectorAll('.gc-card[data-project-id]'));
    cards.forEach((card) => {
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
        }, 120);
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
      let startIdx = 0;
      const onStart = (x) => { startX = x; startIdx = idx; };
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
        onStart(e.clientX);
        const m = (ev) => onMove(ev.clientX);
        const u = (ev) => { document.removeEventListener('mousemove', m); document.removeEventListener('mouseup', u); onEnd(ev.clientX); };
        document.addEventListener('mousemove', m);
        document.addEventListener('mouseup', u);
      });
      let touching = false;
      mediaEl.addEventListener('touchstart', (e) => { touching = true; onStart(e.touches[0] ? e.touches[0].clientX : 0); }, { passive: true });
      mediaEl.addEventListener('touchmove', (e) => {
        if (touching && e.touches[0]) { onMove(e.touches[0].clientX); e.preventDefault(); }
      }, { passive: false });
      mediaEl.addEventListener('touchend', (e) => {
        if (touching && e.changedTouches && e.changedTouches[0]) { touching = false; onEnd(e.changedTouches[0].clientX); }
      }, { passive: true });
      mediaEl.addEventListener('touchcancel', () => { touching = false; }, { passive: true });
    }
    dots.forEach((dot, i) => dot.addEventListener('click', (e) => { e.stopPropagation(); setIdx(i); }));
    popup.classList.add('open');
    const handler = (ev) => {
      const target = ev.target;
      if (!(target instanceof HTMLElement)) return;
      if (target.dataset.gcClose || target === popup) {
        popup.classList.remove('open');
        popup.removeEventListener('click', handler);
      }
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

  function mountSingleSlider(host, projects, projectById) {
    const count = Number(host.dataset.cardCount || 9);
    const selected = pickProjects(projects, count);
    if (!selected.length) {
      host.innerHTML = '<div class="gc-template"><p class="gc-empty">No published gallery projects yet.</p></div>';
      return;
    }
    const slides = selected.map((p) => `<div class="gc-slide"><div class="gc-slide-inner">${buildCard(p, 'gc-card--slider')}</div></div>`);
    const dotsHtml = selected.length > 1 ? Array.from({ length: selected.length }, (_, i) => `<button type="button" class="gc-dot" data-gc-page="${i}" aria-label="Slide ${i + 1}"></button>`).join('') : '';
    host.innerHTML =
      `<div class="gc-template gc-single-slider">` +
      `<div class="gc-slider-shell gc-single-shell"><div class="gc-slider-track gc-single-track">${slides.join('')}</div></div>` +
      (dotsHtml ? `<div class="gc-slider-dots" role="tablist">${dotsHtml}</div>` : '') +
      `</div>`;
    const shell = host.querySelector('.gc-single-shell');
    const track = host.querySelector('.gc-single-track');
    const dots = host.querySelectorAll('.gc-dot');
    let page = 0;
    let startX = 0;
    let startPage = 0;
    let didDrag = false;
    const update = () => {
      page = Math.max(0, Math.min(page, selected.length - 1));
      const slideWidth = shell ? shell.offsetWidth : 400;
      track.style.width = `${selected.length * 100}%`;
      track.querySelectorAll('.gc-slide').forEach((s) => { s.style.flex = `0 0 ${100 / selected.length}%`; });
      track.style.transform = `translateX(-${page * slideWidth}px)`;
      dots.forEach((d, i) => { d.classList.toggle('active', i === page); d.setAttribute('aria-selected', i === page); });
    };
    const onStart = (x) => { didDrag = false; startX = x; startPage = page; };
    const onMove = (x) => {
      if (Math.abs(x - startX) > 5) { didDrag = true; window.__gcSuppressNextClick = true; }
      const slideWidth = shell ? shell.offsetWidth : 400;
      const delta = startX - x;
      const pageOffset = delta / slideWidth;
      track.style.transition = 'none';
      track.style.transform = `translateX(-${(startPage + pageOffset) * slideWidth}px)`;
    };
    const onEnd = (x) => {
      track.style.transition = '';
      const slideWidth = shell ? shell.offsetWidth : 400;
      const delta = startX - x;
      if (Math.abs(delta) > slideWidth * 0.2) page = delta > 0 ? Math.min(page + 1, selected.length - 1) : Math.max(page - 1, 0);
      update();
    };
    const addDrag = (el) => {
      el.addEventListener('mousedown', (e) => { onStart(e.clientX); const m = (ev) => onMove(ev.clientX); const u = (ev) => { document.removeEventListener('mousemove', m); document.removeEventListener('mouseup', u); onEnd(ev.clientX); }; document.addEventListener('mousemove', m); document.addEventListener('mouseup', u); });
      let touched = false;
      el.addEventListener('touchstart', (e) => {
        touched = true;
        onStart(e.touches[0] ? e.touches[0].clientX : 0);
      }, { passive: true });
      el.addEventListener('touchmove', (e) => {
        if (touched && e.touches[0]) {
          onMove(e.touches[0].clientX);
          e.preventDefault();
        }
      }, { passive: false });
      el.addEventListener('touchend', (e) => {
        if (touched && e.changedTouches && e.changedTouches[0]) {
          touched = false;
          onEnd(e.changedTouches[0].clientX);
        }
      }, { passive: true });
      el.addEventListener('touchcancel', () => { touched = false; }, { passive: true });
    };
    addDrag(shell);
    dots.forEach((dot) => dot.addEventListener('click', (e) => { e.preventDefault(); page = Number(dot.getAttribute('data-gc-page')) || 0; update(); }));
    const ro = new ResizeObserver(update);
    if (shell) ro.observe(shell);
    update();
    wireRotations(host, projectById);
    wireCardIntro(host);
    wirePopupOpener(host, projectById);
  }

  function mountFullWidth(host, projects, projectById) {
    const count = Number(host.dataset.cardCount || 10);
    const selected = pickProjects(projects, count);
    if (!selected.length) {
      host.innerHTML = '<div class="gc-template"><p class="gc-empty">No published gallery projects yet.</p></div>';
      return;
    }
    const gap = 18;
    const cardWidth = 420 + gap;
    let displaySet = selected.slice();
    /* Repeat so track always scrolls (works with 1 element, prevents desktop stuck) */
    const renderTrack = () => {
      const base = displaySet.map((p) => buildCard(p, 'gc-card--full'));
      const vw = typeof window !== 'undefined' ? window.innerWidth || 1200 : 1200;
      const needSets = Math.max(2, Math.min(8, Math.ceil(vw / (displaySet.length * cardWidth)) + 1));
      const out = [];
      for (let i = 0; i < needSets; i++) out.push(...base);
      return out.join('');
    };
    host.innerHTML = `<div class="gc-template gc-fullwrap"><div class="gc-full-track">${renderTrack()}</div></div>`;
    const wrap = host.querySelector('.gc-fullwrap');
    const track = host.querySelector('.gc-full-track');
    let offset = 0;
    let startX = 0;
    let startOffset = 0;
    let autoScrollId = null;
    const setWidth = () => displaySet.length * cardWidth;
    const cycleReset = () => {
      track.style.transition = 'none';
      offset %= setWidth();
      if (offset < 0) offset += setWidth();
      displaySet = shuffle(displaySet.slice());
      track.innerHTML = renderTrack();
      wireRotations(host, projectById);
      wireCardIntro(host);
      requestAnimationFrame(() => { track.style.transition = ''; });
    };
    const getViewW = () => wrap.offsetWidth || (typeof window !== 'undefined' ? window.innerWidth : 1200);
    const update = () => {
      const totalW = track.offsetWidth || setWidth() * 3;
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
    const addDrag = (el) => {
      el.addEventListener('mousedown', (e) => { onStart(e.clientX); const m = (ev) => onMove(ev.clientX); const u = () => { document.removeEventListener('mousemove', m); document.removeEventListener('mouseup', u); onEnd(); }; document.addEventListener('mousemove', m); document.addEventListener('mouseup', u); });
      let touching = false;
      el.addEventListener('touchstart', (e) => {
        touching = true;
        onStart(e.touches[0] ? e.touches[0].clientX : 0);
      }, { passive: true });
      el.addEventListener('touchmove', (e) => {
        if (touching && e.touches[0]) {
          onMove(e.touches[0].clientX);
          e.preventDefault();
        }
      }, { passive: false });
      el.addEventListener('touchend', (e) => {
        if (touching && e.changedTouches && e.changedTouches[0]) { touching = false; onEnd(); }
      }, { passive: true });
      el.addEventListener('touchcancel', () => { touching = false; }, { passive: true });
    };
    addDrag(wrap);
    wrap.addEventListener('click', (e) => { if (e.target.closest('.gc-card')) return; e.preventDefault(); });
    const AUTO_PX_PER_MS = 0.08;
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
