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
    authorBtn: null,
    shareBtn: null,
    viewsBtn: null,
    mobileMeta: null,
    theaterBtn: null,
    hideBtn: null,
    restoreBtn: null,
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
      sourceType: String(item.sourceType || 'video'),
      likesCount: Number(item.likesCount || item.likes || 0) || 0,
      commentsCount: Number(item.commentsCount || item.comments || 0) || 0,
      viewsCount: Number(item.viewsCount || item.viewCount || 0) || 0,
      authorId: String(item.authorId || '')
    };
  }

  function createEl(tag, className){
    const el = document.createElement(tag);
    if (className) el.className = className;
    return el;
  }

  function close(){
    if (!state.open && !state.overlay?.classList.contains('minimized')) return;
    try{
      if (document.fullscreenElement || document.webkitFullscreenElement){
        document.exitFullscreen?.().catch?.(()=>{});
      }
    }catch(_){ }
    const v = getCurrentVideoNode();
    if (v){
      try{ v.pause(); }catch(_){ }
      try{ v.currentTime = 0; }catch(_){ }
    }
    if (state.playerHost) state.playerHost.innerHTML = '';
    if (state.restoreBtn) state.restoreBtn.remove();
    state.restoreBtn = null;
    state.open = false;
    if (state.overlay) state.overlay.classList.remove('minimized');
    if (state.overlay) state.overlay.classList.remove('open');
    if (state.overlay) state.overlay.classList.remove('lvp-native-fullscreen');
    if (state.overlay) state.overlay.setAttribute('aria-hidden', 'true');
    document.body.classList.remove('lvp-open');
    syncFullscreenUi();
  }

  function hideToBackground(){
    if (!state.open || !state.overlay) return;
    state.open = false;
    state.overlay.classList.remove('open');
    state.overlay.classList.add('minimized');
    state.overlay.setAttribute('aria-hidden', 'true');
    document.body.classList.remove('lvp-open');
    if (!state.restoreBtn){
      const b = document.createElement('button');
      b.type = 'button';
      b.className = 'lvp-restore-btn';
      b.innerHTML = '<i class="fas fa-play"></i>';
      b.title = 'Show player';
      b.setAttribute('aria-label', 'Show player');
      b.addEventListener('click', ()=>{
        if (!state.overlay) return;
        state.overlay.classList.remove('minimized');
        state.overlay.classList.add('open');
        state.overlay.setAttribute('aria-hidden', 'false');
        document.body.classList.add('lvp-open');
        state.open = true;
      });
      document.body.appendChild(b);
      state.restoreBtn = b;
    }
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

  function getCurrentVideoNode(){
    return state.playerHost ? state.playerHost.querySelector('video.lvp-video') : null;
  }

  function buildVideoNode(item){
    const v = document.createElement('video');
    v.className = 'lvp-video';
    v.src = item.url;
    if (item.poster) v.poster = item.poster;
    // Desktop: keep native controls. Mobile rail handles controls.
    v.controls = !isMobile();
    v.playsInline = true;
    v.autoplay = true;
    v.preload = 'metadata';
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
      const thumb = String(item.poster || '').trim();
      const thumbHtml = thumb ? `<img class="lvp-rec-thumb" src="${thumb.replace(/"/g,'&quot;')}" alt="">` : '<span class="lvp-rec-thumb lvp-rec-thumb-fallback"><i class="fas fa-play"></i></span>';
      row.innerHTML = `${thumbHtml}<span class="lvp-rec-meta"><span class="lvp-rec-title">${item.title.replace(/</g,'&lt;')}</span><span class="lvp-rec-author">${item.author.replace(/</g,'&lt;')}</span></span>`;
      row.addEventListener('click', ()=>{
        if (!state.open) return;
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

  function getCurrentItem(){
    return state.items[state.index] || null;
  }

  async function refreshCurrentEngagementStats(){
    try{
      const current = getCurrentItem();
      if (!current) return;
      const dm = window.dashboardManager || window.top?.dashboardManager || window.parent?.dashboardManager;
      let likes = Number(current.likesCount || 0) || 0;
      let comments = Number(current.commentsCount || 0) || 0;
      if (dm && typeof dm.getAssetAggregatedLikeCount === 'function'){
        try{ likes = Number(await dm.getAssetAggregatedLikeCount('video', current.url)) || likes; }catch(_){ }
      }
      if (dm && typeof dm.resolveAssetPostId === 'function' && window.firebaseService?.getPostStats){
        try{
          const pid = await dm.resolveAssetPostId({ kind: 'video', url: current.url, sourceId: current.sourceId, title: current.title });
          if (pid){
            const st = await window.firebaseService.getPostStats(pid);
            comments = Number(st?.comments || 0) || comments;
          }
        }catch(_){ }
      }
      current.likesCount = likes;
      current.commentsCount = comments;
      if (state.viewsBtn){
        const c = state.viewsBtn.querySelector('.lvp-action-count');
        if (c) c.textContent = String(Math.max(0, Number(current.viewsCount || 0) || 0));
      }
      if (state.likeBtn){
        const c = state.likeBtn.querySelector('.lvp-action-count');
        if (c) c.textContent = String(Math.max(0, Number(likes || 0) || 0));
      }
      if (state.commentsBtn){
        const c = state.commentsBtn.querySelector('.lvp-action-count');
        if (c) c.textContent = String(Math.max(0, Number(comments || 0) || 0));
      }
      if (state.overlay){
        const railLike = state.overlay.querySelector('.lvp-mobile-rail .lvp-like .lvp-rail-count');
        const railComments = state.overlay.querySelector('.lvp-mobile-rail .lvp-comments .lvp-rail-count');
        if (railLike) railLike.textContent = String(Math.max(0, Number(likes || 0) || 0));
        if (railComments) railComments.textContent = String(Math.max(0, Number(comments || 0) || 0));
      }
      try{
        window.dispatchEvent(new CustomEvent('liber-video-engagement-sync', {
          detail: {
            kind: 'video',
            url: String(current.url || ''),
            sourceId: String(current.sourceId || ''),
            likesCount: Number(likes || 0) || 0,
            commentsCount: Number(comments || 0) || 0,
            viewsCount: Number(current.viewsCount || 0) || 0
          }
        }));
      }catch(_){ }
    }catch(_){ }
  }

  async function toggleCurrentLike(){
    try{
      const current = getCurrentItem();
      if (!current || !current.url) return;
      const dm = window.dashboardManager || window.top?.dashboardManager || window.parent?.dashboardManager;
      if (!dm || typeof dm.getAssetLikeKeys !== 'function' || !window.firebaseService?.db){
        return;
      }
      const me = await (window.firebaseService?.getCurrentUser?.() || Promise.resolve(window.firebaseService?.auth?.currentUser || null));
      if (!me || !me.uid) return;
      const keys = dm.getAssetLikeKeys('video', current.url) || [];
      const primary = keys[0];
      const refs = Array.from(new Set([primary, ...keys].filter(Boolean))).map((k)=> firebase.doc(window.firebaseService.db, 'assetLikes', k, 'likes', me.uid));
      if (!refs.length) return;
      let hasLike = false;
      for (const ref of refs){
        try{
          const s = await firebase.getDoc(ref);
          if (s.exists()){ hasLike = true; break; }
        }catch(_){ }
      }
      if (hasLike){
        await Promise.all(refs.map(async (ref)=>{ try{ await firebase.deleteDoc(ref); }catch(_){ } }));
      } else {
        await firebase.setDoc(refs[0], { uid: me.uid, createdAt: new Date().toISOString(), kind: 'video', url: current.url });
      }
      await refreshCurrentEngagementStats();
    }catch(_){ }
  }

  function toggleVideoFullscreen(){
    try{
      const v = getCurrentVideoNode();
      if (!v) return;
      if (document.fullscreenElement){
        document.exitFullscreen?.().catch?.(()=>{});
        return;
      }
      if (v.requestFullscreen){
        v.requestFullscreen().catch(()=>{});
      } else if (v.webkitEnterFullscreen){
        try{ v.webkitEnterFullscreen(); }catch(_){ }
      } else if (state.shell?.requestFullscreen){
        state.shell.requestFullscreen().catch(()=>{});
      }
    }catch(_){ }
  }

  function syncFullscreenUi(){
    try{
      const fsEl = document.fullscreenElement || document.webkitFullscreenElement || null;
      const active = !!fsEl;
      if (state.overlay) state.overlay.classList.toggle('lvp-native-fullscreen', active);
      const rail = state.overlay?.querySelector('.lvp-mobile-rail .lvp-fullscreen');
      if (rail){
        rail.title = active ? 'Exit fullscreen' : 'Fullscreen';
        rail.setAttribute('aria-label', active ? 'Exit fullscreen' : 'Fullscreen');
      }
    }catch(_){ }
  }

  async function setIndex(next){
    if (!state.items.length) return;
    state.index = clamp(next, 0, state.items.length - 1);
    const item = state.items[state.index];
    if (!item) return;
    if (state.playerHost){
      const current = getCurrentVideoNode();
      if (current){
        try{ current.pause(); }catch(_){ }
      }
      const nextVideo = buildVideoNode(item);
      nextVideo.controls = !isMobile();
      nextVideo.style.opacity = '0';
      nextVideo.style.transition = 'opacity .22s ease';
      state.playerHost.innerHTML = '';
      state.playerHost.appendChild(nextVideo);
      requestAnimationFrame(()=>{
        nextVideo.style.opacity = '1';
      });
    }
    try{
      window.firebaseService?.trackVideoInteraction?.({
        action: 'open',
        source: state.source,
        sourceId: item.sourceId,
        videoId: item.id
      });
    }catch(_){ }
    updateMeta(item);
    if (state.likeBtn){
      const c = state.likeBtn.querySelector('.lvp-action-count');
      if (c) c.textContent = String(Math.max(0, Number(item.likesCount || 0) || 0));
    }
    if (state.commentsBtn){
      const c = state.commentsBtn.querySelector('.lvp-action-count');
      if (c) c.textContent = String(Math.max(0, Number(item.commentsCount || 0) || 0));
    }
    if (state.overlay){
      const railLike = state.overlay.querySelector('.lvp-mobile-rail .lvp-like .lvp-rail-count');
      const railComments = state.overlay.querySelector('.lvp-mobile-rail .lvp-comments .lvp-rail-count');
      if (railLike) railLike.textContent = String(Math.max(0, Number(item.likesCount || 0) || 0));
      if (railComments) railComments.textContent = String(Math.max(0, Number(item.commentsCount || 0) || 0));
    }
    if (state.viewsBtn){
      const c = state.viewsBtn.querySelector('.lvp-action-count');
      if (c) c.textContent = String(Math.max(0, Number(item.viewsCount || 0) || 0));
    }
    renderRecommendations(item.id);
    try{
      const recs = await fetchRecommendations(item);
      if (recs.length){
        const merged = [item].concat(recs.filter((r)=> r.id !== item.id));
        state.items = merged.slice(0, 50);
        renderRecommendations(item.id);
      }
    }catch(_){ }
    refreshCurrentEngagementStats();
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
        <button class="lvp-hide" type="button" aria-label="Hide player"><i class="fas fa-chevron-down"></i></button>
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
              <button type="button" class="lvp-action lvp-like has-count" title="Like" aria-label="Like"><i class="fas fa-heart"></i><span class="lvp-action-count">0</span></button>
              <button type="button" class="lvp-action lvp-comments has-count" title="Comments" aria-label="Comments"><i class="fas fa-comment-dots"></i><span class="lvp-action-count">0</span></button>
              <button type="button" class="lvp-action lvp-views has-count" disabled title="Views" aria-label="Views"><i class="fas fa-eye"></i><span class="lvp-action-count">0</span></button>
              <button type="button" class="lvp-action lvp-author" title="Author" aria-label="Author"><i class="fas fa-user-circle"></i></button>
              <button type="button" class="lvp-action lvp-share" title="Share" aria-label="Share"><i class="fas fa-paper-plane"></i></button>
              <button type="button" class="lvp-action lvp-theater" title="Theater mode" aria-label="Theater mode"><i class="fas fa-expand"></i></button>
            </div>
          </section>
        </div>
        <div class="lvp-mobile-rail">
          <button type="button" class="lvp-rail-btn lvp-like"><i class="fas fa-heart"></i><span class="lvp-rail-count"></span></button>
          <button type="button" class="lvp-rail-btn lvp-comments"><i class="fas fa-comment-dots"></i><span class="lvp-rail-count"></span></button>
          <button type="button" class="lvp-rail-btn lvp-author"><i class="fas fa-user-circle"></i><span class="lvp-rail-count"></span></button>
          <button type="button" class="lvp-rail-btn lvp-share"><i class="fas fa-paper-plane"></i><span class="lvp-rail-count"></span></button>
          <button type="button" class="lvp-rail-btn lvp-fullscreen"><i class="fas fa-expand"></i><span class="lvp-rail-count"></span></button>
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
    state.authorBtn = overlay.querySelector('.lvp-author');
    state.shareBtn = overlay.querySelector('.lvp-share');
    state.viewsBtn = overlay.querySelector('.lvp-views');
    state.theaterBtn = overlay.querySelector('.lvp-theater');
    state.hideBtn = overlay.querySelector('.lvp-hide');
    state.mobileMeta = overlay.querySelector('.lvp-mobile-meta');

    const closeBtn = overlay.querySelector('.lvp-close');
    if (closeBtn) closeBtn.addEventListener('click', close);
    if (state.hideBtn) state.hideBtn.addEventListener('click', hideToBackground);
    overlay.addEventListener('click', (e)=>{
      const target = e.target;
      if (!(target instanceof Element)) return;
      if (target.closest('.lvp-main,.lvp-recommendations,.lvp-mobile-meta,.lvp-mobile-rail,.lvp-close,.lvp-hide')) return;
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
    state.authorBtn?.addEventListener('click', ()=>{
      try { window.dispatchEvent(new CustomEvent('liber-video-author-open', { detail: state.items[state.index] || null })); } catch (_) { }
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
      toggleCurrentLike();
    });
    document.addEventListener('fullscreenchange', syncFullscreenUi);
    document.addEventListener('webkitfullscreenchange', syncFullscreenUi);
    state.theaterBtn?.addEventListener('click', ()=>{
      const next = !state.shell.classList.contains('theater');
      state.shell.classList.toggle('theater', next);
      const icon = state.theaterBtn.querySelector('i');
      if (icon) icon.className = `fas ${next ? 'fa-compress' : 'fa-expand'}`;
      state.theaterBtn.title = next ? 'Default mode' : 'Theater mode';
      state.theaterBtn.setAttribute('aria-label', next ? 'Default mode' : 'Theater mode');
    });
    overlay.querySelectorAll('.lvp-mobile-rail .lvp-like').forEach((b)=> b.addEventListener('click', ()=> state.likeBtn?.click()));
    overlay.querySelectorAll('.lvp-mobile-rail .lvp-comments').forEach((b)=> b.addEventListener('click', ()=> state.commentsBtn?.click()));
    overlay.querySelectorAll('.lvp-mobile-rail .lvp-author').forEach((b)=> b.addEventListener('click', ()=> state.authorBtn?.click()));
    overlay.querySelectorAll('.lvp-mobile-rail .lvp-share').forEach((b)=> b.addEventListener('click', ()=> state.shareBtn?.click()));
    overlay.querySelectorAll('.lvp-mobile-rail .lvp-fullscreen').forEach((b)=> b.addEventListener('click', toggleVideoFullscreen));

    const progress = overlay.querySelector('.lvp-progress');
    if (progress){
      const seekTo = (clientX)=>{
        const v = getCurrentVideoNode();
        if (!v || !(v.duration > 0)) return;
        const rect = progress.getBoundingClientRect();
        const ratio = clamp((clientX - rect.left) / Math.max(1, rect.width), 0, 1);
        v.currentTime = ratio * v.duration;
      };
      progress.addEventListener('click', (e)=> seekTo(e.clientX));
      let dragging = false;
      progress.addEventListener('pointerdown', (e)=>{
        dragging = true;
        try{ progress.setPointerCapture(e.pointerId); }catch(_){ }
        seekTo(e.clientX);
      });
      progress.addEventListener('pointermove', (e)=>{ if (dragging) seekTo(e.clientX); });
      progress.addEventListener('pointerup', (e)=>{
        dragging = false;
        try{ progress.releasePointerCapture(e.pointerId); }catch(_){ }
      });
      progress.addEventListener('pointercancel', ()=>{ dragging = false; });
    }

    overlay.addEventListener('wheel', onWheel, { passive: false });
    overlay.addEventListener('touchstart', onTouchStart, { passive: true });
    overlay.addEventListener('touchmove', onTouchMove, { passive: false });
    overlay.addEventListener('touchend', onTouchEnd, { passive: true });
    window.addEventListener('resize', ()=>{
      const v = getCurrentVideoNode();
      if (v) v.controls = !isMobile();
    });
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
    if (state.restoreBtn){
      try{ state.restoreBtn.remove(); }catch(_){ }
      state.restoreBtn = null;
    }
    state.items = items;
    state.index = clamp(Number(payload?.startIndex) || 0, 0, items.length - 1);
    state.source = String(payload?.source || 'app');
    state.overlay.classList.remove('minimized');
    state.overlay.classList.add('open');
    state.overlay.setAttribute('aria-hidden', 'false');
    document.body.classList.add('lvp-open');
    state.open = true;
    syncFullscreenUi();
    await setIndex(state.index);
    return true;
  }

  window.LiberVideoPlayer = {
    open,
    close,
    isOpen: function(){ return !!state.open; }
  };
})();
