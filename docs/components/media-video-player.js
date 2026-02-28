(function(){
  if (window.LiberVideoPlayer) return;

  const state = {
    open: false,
    items: [],
    index: 0,
    source: 'app',
    overlay: null,
    shell: null,
    playerHost: null,
    recList: null,
    titleEl: null,
    authorEl: null,
    descEl: null,
    likeBtn: null,
    commentsBtn: null,
    shareBtn: null,
    mobileMeta: null,
    uiHidden: false,
    holdTimer: null,
    verticalStartY: 0,
    verticalStartX: 0,
    verticalTouching: false,
    horizontalSwipe: null
  };

  function clamp(v, min, max){
    return Math.max(min, Math.min(max, v));
  }

  function isMobile(){
    return window.matchMedia && window.matchMedia('(max-width: 767px)').matches;
  }

  function normalizeItem(item){
    if (!item || !item.url) return null;
    return {
      id: String(item.id || item.url),
      type: 'video',
      url: String(item.url),
      title: String(item.title || item.alt || 'Video'),
      author: String(item.author || item.by || item.authorName || ''),
      description: String(item.description || ''),
      poster: String(item.poster || item.cover || item.thumbnailUrl || ''),
      sourceId: String(item.sourceId || item.id || ''),
      sourceType: String(item.sourceType || 'video')
    };
  }

  function createEl(tag, className){
    const el = document.createElement(tag);
    if (className) el.className = className;
    return el;
  }

  function close(){
    if (!state.open) return;
    state.open = false;
    if (state.overlay) state.overlay.classList.remove('open');
    if (state.overlay) state.overlay.setAttribute('aria-hidden', 'true');
    document.body.classList.remove('lvp-open');
  }

  async function fetchRecommendations(current){
    try{
      if (!window.firebaseService || typeof window.firebaseService.getVideoRecommendations !== 'function') return [];
      const list = await window.firebaseService.getVideoRecommendations({
        source: state.source,
        sourceId: current?.sourceId || '',
        limit: 30
      });
      return Array.isArray(list) ? list.map(normalizeItem).filter(Boolean) : [];
    }catch(_){
      return [];
    }
  }

  function buildVideoNode(item){
    const v = document.createElement('video');
    v.className = 'lvp-video';
    v.src = item.url;
    if (item.poster) v.poster = item.poster;
    v.controls = false;
    v.playsInline = true;
    v.autoplay = true;
    v.preload = 'metadata';
    v.addEventListener('click', (e)=>{
      e.stopPropagation();
      if (v.paused) v.play().catch(()=>{});
      else v.pause();
    });
    v.addEventListener('play', ()=> {
      try{
        window.firebaseService?.trackVideoInteraction?.({
          action: 'play',
          source: state.source,
          sourceId: item.sourceId,
          videoId: item.id
        });
      }catch(_){ }
    });
    v.addEventListener('timeupdate', ()=>{
      const pct = v.duration > 0 ? (v.currentTime / v.duration) : 0;
      try{
        window.firebaseService?.trackVideoInteraction?.({
          action: 'progress',
          source: state.source,
          sourceId: item.sourceId,
          videoId: item.id,
          watchRatio: Number(pct.toFixed(4))
        });
      }catch(_){ }
      const p = state.shell ? state.shell.querySelector('.lvp-progress-fill') : null;
      if (p) p.style.width = `${clamp(pct * 100, 0, 100)}%`;
    });
    return v;
  }

  function renderRecommendations(activeId){
    if (!state.recList) return;
    state.recList.innerHTML = '';
    state.items.forEach((item, idx)=>{
      const row = createEl('button', `lvp-rec-item${item.id === activeId ? ' active' : ''}`);
      row.type = 'button';
      row.innerHTML = `<span class="lvp-rec-title">${item.title.replace(/</g,'&lt;')}</span><span class="lvp-rec-author">${item.author.replace(/</g,'&lt;')}</span>`;
      row.addEventListener('click', ()=>{
        setIndex(idx);
      });
      state.recList.appendChild(row);
    });
  }

  function updateMeta(item){
    if (state.titleEl) state.titleEl.textContent = item.title || 'Video';
    if (state.authorEl) state.authorEl.textContent = item.author ? `by ${item.author}` : '';
    if (state.descEl) state.descEl.textContent = item.description || '';
    if (state.mobileMeta) {
      const t = state.mobileMeta.querySelector('.lvp-mobile-title');
      const a = state.mobileMeta.querySelector('.lvp-mobile-author');
      const d = state.mobileMeta.querySelector('.lvp-mobile-desc');
      if (t) t.textContent = item.title || 'Video';
      if (a) a.textContent = item.author ? `@${item.author}` : '';
      if (d) d.textContent = item.description || '';
    }
  }

  async function setIndex(next){
    if (!state.items.length) return;
    state.index = clamp(next, 0, state.items.length - 1);
    const item = state.items[state.index];
    if (!item) return;
    if (state.playerHost){
      state.playerHost.innerHTML = '';
      state.playerHost.appendChild(buildVideoNode(item));
    }
    updateMeta(item);
    renderRecommendations(item.id);
    try{
      const recs = await fetchRecommendations(item);
      if (recs.length){
        const merged = [item].concat(recs.filter((r)=> r.id !== item.id));
        state.items = merged.slice(0, 50);
        renderRecommendations(item.id);
      }
    }catch(_){ }
  }

  function onWheel(e){
    if (!state.open || !isMobile()) return;
    if (Math.abs(e.deltaY) < 14) return;
    e.preventDefault();
    if (e.deltaY > 0) setIndex(state.index + 1);
    else setIndex(state.index - 1);
  }

  function onTouchStart(e){
    if (!state.open || !e.touches || !e.touches[0]) return;
    const t = e.touches[0];
    state.verticalStartY = t.clientY;
    state.verticalStartX = t.clientX;
    state.verticalTouching = true;
    state.horizontalSwipe = null;
    clearTimeout(state.holdTimer);
    state.holdTimer = setTimeout(()=>{
      state.uiHidden = true;
      if (state.shell) state.shell.classList.add('ui-hidden');
    }, 380);
  }

  function onTouchMove(e){
    if (!state.open || !state.verticalTouching || !e.touches || !e.touches[0]) return;
    const t = e.touches[0];
    const dx = Math.abs(t.clientX - state.verticalStartX);
    const dy = Math.abs(t.clientY - state.verticalStartY);
    if (state.horizontalSwipe === null) state.horizontalSwipe = dy > (dx * 1.2);
    if (state.horizontalSwipe) e.preventDefault();
  }

  function onTouchEnd(e){
    clearTimeout(state.holdTimer);
    if (state.uiHidden){
      state.uiHidden = false;
      if (state.shell) state.shell.classList.remove('ui-hidden');
      return;
    }
    if (!state.open || !state.verticalTouching || !e.changedTouches || !e.changedTouches[0]) return;
    const y = e.changedTouches[0].clientY;
    const delta = state.verticalStartY - y;
    if (Math.abs(delta) > 50){
      if (delta > 0) setIndex(state.index + 1);
      else setIndex(state.index - 1);
    }
    state.verticalTouching = false;
  }

  function ensureDom(){
    if (state.overlay) return;
    const overlay = createEl('div', 'lvp-overlay');
    overlay.setAttribute('aria-hidden', 'true');
    overlay.innerHTML = `
      <div class="lvp-shell" role="dialog" aria-modal="true" aria-label="Video player">
        <button class="lvp-close" type="button" aria-label="Close">×</button>
        <div class="lvp-grid">
          <aside class="lvp-recommendations">
            <div class="lvp-rec-head">Recommended</div>
            <div class="lvp-rec-list"></div>
          </aside>
          <section class="lvp-main">
            <div class="lvp-video-host"></div>
            <div class="lvp-progress"><span class="lvp-progress-fill"></span></div>
            <div class="lvp-meta">
              <h3 class="lvp-title"></h3>
              <div class="lvp-author"></div>
              <p class="lvp-desc"></p>
            </div>
            <div class="lvp-actions">
              <button type="button" class="lvp-action lvp-like">Like</button>
              <button type="button" class="lvp-action lvp-comments">Comments</button>
              <button type="button" class="lvp-action lvp-share">Share</button>
            </div>
          </section>
        </div>
        <div class="lvp-mobile-meta">
          <div class="lvp-mobile-author"></div>
          <div class="lvp-mobile-title"></div>
          <div class="lvp-mobile-desc"></div>
        </div>
      </div>
    `;
    document.body.appendChild(overlay);
    state.overlay = overlay;
    state.shell = overlay.querySelector('.lvp-shell');
    state.playerHost = overlay.querySelector('.lvp-video-host');
    state.recList = overlay.querySelector('.lvp-rec-list');
    state.titleEl = overlay.querySelector('.lvp-title');
    state.authorEl = overlay.querySelector('.lvp-author');
    state.descEl = overlay.querySelector('.lvp-desc');
    state.likeBtn = overlay.querySelector('.lvp-like');
    state.commentsBtn = overlay.querySelector('.lvp-comments');
    state.shareBtn = overlay.querySelector('.lvp-share');
    state.mobileMeta = overlay.querySelector('.lvp-mobile-meta');

    const closeBtn = overlay.querySelector('.lvp-close');
    if (closeBtn) closeBtn.addEventListener('click', close);
    overlay.addEventListener('click', (e)=>{
      const target = e.target;
      if (!(target instanceof Element)) return;
      if (target.closest('.lvp-main,.lvp-recommendations,.lvp-mobile-meta,.lvp-close')) return;
      close();
    });
    state.shareBtn?.addEventListener('click', async ()=>{
      const current = state.items[state.index];
      if (!current) return;
      const shareData = { title: current.title || 'Video', text: current.description || '', url: current.url };
      try{
        if (navigator.share) await navigator.share(shareData);
        else await navigator.clipboard?.writeText(current.url);
      }catch(_){ }
    });
    state.commentsBtn?.addEventListener('click', ()=>{
      // Placeholder comment sheet integration point for both app and landing.
      try { window.dispatchEvent(new CustomEvent('liber-video-comments-open', { detail: state.items[state.index] || null })); } catch (_) { }
    });
    state.likeBtn?.addEventListener('click', ()=>{
      const current = state.items[state.index];
      if (!current) return;
      try{
        window.firebaseService?.trackVideoInteraction?.({
          action: 'like_click',
          source: state.source,
          sourceId: current.sourceId,
          videoId: current.id
        });
      }catch(_){ }
    });

    overlay.addEventListener('wheel', onWheel, { passive: false });
    overlay.addEventListener('touchstart', onTouchStart, { passive: true });
    overlay.addEventListener('touchmove', onTouchMove, { passive: false });
    overlay.addEventListener('touchend', onTouchEnd, { passive: true });
    document.addEventListener('keydown', (e)=>{
      if (!state.open) return;
      if (e.key === 'Escape') close();
      if (!isMobile() && e.key === 'ArrowDown') setIndex(state.index + 1);
      if (!isMobile() && e.key === 'ArrowUp') setIndex(state.index - 1);
    });
  }

  async function open(payload){
    ensureDom();
    const items = (Array.isArray(payload?.items) ? payload.items : []).map(normalizeItem).filter(Boolean);
    if (!items.length) return false;
    state.items = items;
    state.index = clamp(Number(payload?.startIndex) || 0, 0, items.length - 1);
    state.source = String(payload?.source || 'app');
    state.overlay.classList.add('open');
    state.overlay.setAttribute('aria-hidden', 'false');
    document.body.classList.add('lvp-open');
    state.open = true;
    await setIndex(state.index);
    return true;
  }

  window.LiberVideoPlayer = {
    open,
    close,
    isOpen: function(){ return !!state.open; }
  };
})();
