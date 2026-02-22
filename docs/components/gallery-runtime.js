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
        }, 170);
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
      start();
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
    const render = () => {
      const item = visuals[idx] || visuals[0];
      popup.innerHTML =
        `<div class="gc-popup-card" role="dialog" aria-modal="true">` +
        `<div class="gc-popup-media">` +
        `<button class="gc-slider-btn gc-popup-close" data-gc-close="1">Close</button>` +
        `${item ? createMediaElement(item, false) : ''}` +
        dotsHtml +
        `</div>` +
        `<div class="gc-popup-body"><h3>${project.title || 'Project'} ${project.year || ''}</h3><p>${project.description || ''}</p><div>${texts.map((t) => `<p>${t.text}</p>`).join('')}</div></div>` +
        `</div>`;
      const dots = popup.querySelectorAll('.gc-popup-dots .gc-dot');
      dots.forEach((dot, i) => {
        dot.classList.toggle('active', i === idx);
        dot.setAttribute('aria-selected', i === idx);
      });
    };
    render();
    popup.classList.add('open');
    const handler = (ev) => {
      const target = ev.target;
      if (!(target instanceof HTMLElement)) return;
      if (target.dataset.gcClose || target === popup) {
        popup.classList.remove('open');
        popup.removeEventListener('click', handler);
      } else if (target.classList.contains('gc-dot') && target.dataset.gcIdx != null) {
        idx = Number(target.dataset.gcIdx) || 0;
        render();
      }
    };
    popup.addEventListener('click', handler);
  }

  function wirePopupOpener(container, projectById) {
    container.addEventListener('click', (e) => {
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
    const withVisuals = source.filter((p) => visualItems(p.items || []).length > 0);
    if (!withVisuals.length || count <= 0) return [];
    const pool = shuffle(withVisuals);
    const out = [];
    while (out.length < count) {
      out.push(pool[out.length % pool.length]);
    }
    return out;
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
    wirePopupOpener(host, projectById);
  }

  function mountSingleSlider(host, projects, projectById) {
    const count = Number(host.dataset.cardCount || 9);
    const selected = pickProjects(projects, count);
    if (!selected.length) {
      host.innerHTML = '<div class="gc-template"><p class="gc-empty">No published gallery projects yet.</p></div>';
      return;
    }
    const perPage = window.innerWidth <= 900 ? (window.innerWidth <= 640 ? 1 : 2) : 3;
    const pages = Math.max(1, Math.ceil(selected.length / perPage));
    const slides = [];
    for (let i = 0; i < pages; i++) {
      const chunk = selected.slice(i * perPage, (i + 1) * perPage);
      slides.push(`<div class="gc-slide">${chunk.map((p) => buildCard(p, 'gc-card--slider')).join('')}</div>`);
    }
    const dotsHtml = Array.from({ length: pages }, (_, i) => `<button type="button" class="gc-dot" data-gc-page="${i}" aria-label="Slide ${i + 1}"></button>`).join('');
    host.innerHTML =
      `<div class="gc-template">` +
      `<div class="gc-slider-shell"><div class="gc-slider-track">${slides.join('')}</div></div>` +
      `<div class="gc-slider-dots" role="tablist">${dotsHtml}</div>` +
      `</div>`;
    let page = 0;
    const track = host.querySelector('.gc-slider-track');
    const dots = host.querySelectorAll('.gc-dot');
    const update = () => {
      track.style.transform = `translateX(-${page * 100}%)`;
      dots.forEach((d, i) => { d.classList.toggle('active', i === page); d.setAttribute('aria-selected', i === page); });
    };
    dots.forEach((dot) => {
      dot.addEventListener('click', (e) => {
        e.preventDefault();
        page = Number(dot.getAttribute('data-gc-page')) || 0;
        update();
      });
    });
    update();
    wireRotations(host, projectById);
    wirePopupOpener(host, projectById);
  }

  function mountFullWidth(host, projects, projectById) {
    const count = Number(host.dataset.cardCount || 10);
    const selected = pickProjects(projects, count);
    if (!selected.length) {
      host.innerHTML = '<div class="gc-template"><p class="gc-empty">No published gallery projects yet.</p></div>';
      return;
    }
    const gap = 14;
    const cardWidth = 300 + gap;
    const perPage = Math.max(1, Math.floor((host.offsetWidth || 800) / cardWidth));
    const pages = Math.max(1, Math.ceil(selected.length / perPage));
    const dotsHtml = pages > 1 ? Array.from({ length: pages }, (_, i) => `<button type="button" class="gc-dot" data-gc-full-page="${i}" aria-label="Page ${i + 1}"></button>`).join('') : '';
    host.innerHTML =
      `<div class="gc-template gc-fullwrap"><div class="gc-full-track">${selected.map((p) => buildCard(p, 'gc-card--full')).join('')}</div></div>` +
      (dotsHtml ? `<div class="gc-slider-dots gc-full-dots" role="tablist">${dotsHtml}</div>` : '');
    const wrap = host.querySelector('.gc-fullwrap');
    const track = host.querySelector('.gc-full-track');
    let offset = 0;
    let dragged = false;
    let startX = 0;
    let startOffset = 0;
    const update = () => {
      const maxO = Math.max(0, (selected.length * cardWidth) - (wrap.offsetWidth || 0));
      offset = Math.max(0, Math.min(offset, maxO));
      track.style.transform = `translateX(-${offset}px)`;
      const dots = host.querySelectorAll('.gc-full-dots .gc-dot');
      if (dots.length) {
        const pageIdx = Math.min(pages - 1, Math.round(offset / (perPage * cardWidth)));
        dots.forEach((d, i) => { d.classList.toggle('active', i === pageIdx); d.setAttribute('aria-selected', i === pageIdx); });
      }
    };
    const onStart = (x) => {
      dragged = false;
      startX = x;
      startOffset = offset;
    };
    const onMove = (x) => {
      if (Math.abs(x - startX) > 5) dragged = true;
      offset = startOffset + (startX - x);
      update();
    };
    const onTap = (x) => {
      if (dragged) return;
      const rect = wrap.getBoundingClientRect();
      if (x - rect.left > rect.width / 2) offset += cardWidth;
      else offset -= cardWidth;
      update();
    };
    wrap.addEventListener('mousedown', (e) => {
      if (e.target.closest('.gc-card')) return;
      e.preventDefault();
      onStart(e.clientX);
      const move = (ev) => onMove(ev.clientX);
      const up = (ev) => {
        document.removeEventListener('mousemove', move);
        document.removeEventListener('mouseup', up);
        if (!dragged) onTap(ev.clientX);
      };
      document.addEventListener('mousemove', move);
      document.addEventListener('mouseup', up);
    });
    let touching = false;
    wrap.addEventListener('touchstart', (e) => {
      if (e.target.closest('.gc-card')) return;
      touching = true;
      onStart(e.touches[0].clientX);
    }, { passive: true });
    wrap.addEventListener('touchmove', (e) => {
      if (!touching) return;
      onMove(e.touches[0].clientX);
      e.preventDefault();
    }, { passive: false });
    wrap.addEventListener('touchend', (e) => {
      if (!touching) return;
      touching = false;
      if (e.changedTouches[0] && !dragged) onTap(e.changedTouches[0].clientX);
    }, { passive: true });
    wrap.addEventListener('click', (e) => {
      if (e.target.closest('.gc-card')) return;
      e.preventDefault();
    });
    host.querySelectorAll('.gc-full-dots .gc-dot').forEach((dot) => {
      dot.addEventListener('click', (e) => {
        e.preventDefault();
        const p = Number(dot.getAttribute('data-gc-full-page')) || 0;
        offset = Math.min(p * perPage * cardWidth, Math.max(0, (selected.length * cardWidth) - (wrap.offsetWidth || 0)));
        update();
      });
    });
    wireRotations(host, projectById);
    wirePopupOpener(host, projectById);
    requestAnimationFrame(update);
    const ro = new ResizeObserver(update);
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
