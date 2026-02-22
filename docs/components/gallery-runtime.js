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
      try {
        const keys = await window.secureKeyManager.getKeys();
        cfg = keys && keys.firebase ? keys.firebase : null;
      } catch (_) {
        cfg = null;
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
    } catch (_) {
      projectSnap = await getDocs(query(projectsCol, where('isPublished', '==', true)));
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
    return projects.filter((p) => p.items && p.items.length);
  }

  function createMediaElement(item, mutedVideo) {
    if (item.type === 'video') {
      return `<video src="${item.url}" ${mutedVideo ? 'muted autoplay loop playsinline' : 'controls playsinline'}></video>`;
    }
    return `<img src="${item.url}" alt="${item.caption || 'Gallery item'}" loading="lazy" />`;
  }

  function buildCard(project, modeClass) {
    const visuals = visualItems(project.items);
    const first = visuals[0];
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
    const render = () => {
      const item = visuals[idx] || visuals[0];
      popup.innerHTML =
        `<div class="gc-popup-card" role="dialog" aria-modal="true">` +
        `<div class="gc-popup-media">` +
        `<button class="gc-slider-btn gc-popup-close" data-gc-close="1">Close</button>` +
        `<div class="gc-popup-nav"><button class="gc-slider-btn" data-gc-prev="1">Prev</button><button class="gc-slider-btn" data-gc-next="1">Next</button></div>` +
        `${item ? createMediaElement(item, false) : ''}` +
        `</div>` +
        `<div class="gc-popup-body"><h3>${project.title || 'Project'} ${project.year || ''}</h3><p>${project.description || ''}</p><div>${texts.map((t) => `<p>${t.text}</p>`).join('')}</div></div>` +
        `</div>`;
    };
    render();
    popup.classList.add('open');
    popup.addEventListener('click', (ev) => {
      const target = ev.target;
      if (!(target instanceof HTMLElement)) return;
      if (target.dataset.gcClose || target === popup) {
        popup.classList.remove('open');
      } else if (target.dataset.gcPrev) {
        idx = (idx - 1 + visuals.length) % visuals.length;
        render();
      } else if (target.dataset.gcNext) {
        idx = (idx + 1) % visuals.length;
        render();
      }
    }, { once: true });
  }

  function wirePopupOpener(container, projectById) {
    Array.from(container.querySelectorAll('.gc-card[data-project-id]')).forEach((card) => {
      const id = card.getAttribute('data-project-id');
      const project = projectById.get(id);
      if (!project) return;
      card.addEventListener('click', () => openPopup(project));
      card.addEventListener('keydown', (ev) => {
        if (ev.key === 'Enter' || ev.key === ' ') {
          ev.preventDefault();
          openPopup(project);
        }
      });
    });
  }

  function pickProjects(source, count) {
    if (!source.length || count <= 0) return [];
    const pool = shuffle(source);
    const out = [];
    while (out.length < count) {
      out.push(pool[out.length % pool.length]);
    }
    return out;
  }

  function mountTiles(host, projects, projectById) {
    const count = Number(host.dataset.cardCount || 9);
    const selected = pickProjects(projects, count);
    host.innerHTML = `<div class="gc-template"><div class="gc-grid">${selected.map((p) => buildCard(p, '')).join('')}</div></div>`;
    wireRotations(host, projectById);
    wirePopupOpener(host, projectById);
  }

  function mountSingleSlider(host, projects, projectById) {
    const count = Number(host.dataset.cardCount || 9);
    const selected = pickProjects(projects, count);
    host.innerHTML =
      `<div class="gc-template">` +
      `<div class="gc-slider-actions"><button class="gc-slider-btn" data-gc-step="-1">Prev</button><button class="gc-slider-btn" data-gc-step="1">Next</button></div>` +
      `<div class="gc-slider-shell"><div class="gc-slider-track">${selected.map((p) => buildCard(p, 'gc-card--slider')).join('')}</div></div>` +
      `</div>`;
    let page = 0;
    const perPage = window.innerWidth <= 900 ? (window.innerWidth <= 640 ? 1 : 2) : 3;
    const pages = Math.max(1, Math.ceil(selected.length / perPage));
    const track = host.querySelector('.gc-slider-track');
    const step = () => {
      track.style.transform = `translateX(-${page * 100}%)`;
    };
    Array.from(host.querySelectorAll('[data-gc-step]')).forEach((btn) => {
      btn.addEventListener('click', () => {
        page = (page + Number(btn.getAttribute('data-gc-step')) + pages) % pages;
        step();
      });
    });
    wireRotations(host, projectById);
    wirePopupOpener(host, projectById);
  }

  function mountFullWidth(host, projects, projectById) {
    const count = Number(host.dataset.cardCount || 10);
    const selected = pickProjects(projects, count);
    const duplicated = selected.concat(selected);
    host.innerHTML =
      `<div class="gc-template gc-track-wrap"><div class="gc-track-full">${duplicated.map((p) => buildCard(p, 'gc-card--full')).join('')}</div></div>`;
    wireRotations(host, projectById);
    wirePopupOpener(host, projectById);
  }

  async function boot() {
    const hosts = Array.from(document.querySelectorAll('[data-gallery-template]'));
    if (!hosts.length) return;
    try {
      const projects = await loadProjects();
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
