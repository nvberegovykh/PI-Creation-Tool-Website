(function(){
  if (window.LiberMediaFullscreenViewer) return;

  const MAX_ZOOM = 4;
  const MIN_ZOOM = 1;
  const SWIPE_SLOPE = 0.5;
  const SWIPE_MOMENTUM_THRESHOLD = 0.42;

  function clamp(v, min, max){
    return Math.max(min, Math.min(max, v));
  }

  function normalizeItem(raw){
    if (!raw || !raw.url) return null;
    const type = String(raw.type || '').toLowerCase() === 'video' ? 'video' : 'image';
    return {
      type,
      url: String(raw.url),
      alt: String(raw.alt || raw.title || (type === 'video' ? 'Video' : 'Image')),
      poster: String(raw.poster || raw.cover || ''),
      title: String(raw.title || '')
    };
  }

  function createEl(tag, className){
    const el = document.createElement(tag);
    if (className) el.className = className;
    return el;
  }

  const state = {
    open: false,
    items: [],
    idx: 0,
    scale: 1,
    panX: 0,
    panY: 0,
    startX: 0,
    startY: 0,
    startTs: 0,
    startIdx: 0,
    lastX: 0,
    lastTs: 0,
    horizontalSwipe: null,
    touchDragging: false,
    panning: false,
    pointerId: null,
    pinchStartDistance: 0,
    pinchStartScale: 1,
    overlay: null,
    stage: null,
    track: null,
    closeBtn: null,
    prevBtn: null,
    nextBtn: null,
    zoomBtn: null,
    zoomRange: null,
    counter: null,
    lastActive: null
  };

  function getCurrentSlide(){
    return state.track ? state.track.children[state.idx] : null;
  }

  function getCurrentImage(){
    const slide = getCurrentSlide();
    if (!slide) return null;
    return slide.querySelector('img.lmfs-img');
  }

  function updatePanBounds(){
    const img = getCurrentImage();
    const stageRect = state.stage ? state.stage.getBoundingClientRect() : null;
    if (!img || !stageRect) return { x: 0, y: 0 };
    const contentW = stageRect.width * state.scale;
    const contentH = stageRect.height * state.scale;
    return {
      x: Math.max(0, (contentW - stageRect.width) / 2),
      y: Math.max(0, (contentH - stageRect.height) / 2)
    };
  }

  function applyZoomTransform(){
    const img = getCurrentImage();
    if (!img) return;
    const bounds = updatePanBounds();
    state.panX = clamp(state.panX, -bounds.x, bounds.x);
    state.panY = clamp(state.panY, -bounds.y, bounds.y);
    img.style.transform = `translate(${state.panX}px, ${state.panY}px) scale(${state.scale})`;
    img.style.cursor = state.scale > 1 ? 'grab' : 'zoom-in';
  }

  function setScale(nextScale, anchorX, anchorY){
    const prev = state.scale;
    const clamped = clamp(nextScale, MIN_ZOOM, MAX_ZOOM);
    if (Math.abs(clamped - prev) < 0.001) return;
    state.scale = clamped;
    if (anchorX != null && anchorY != null && prev > 0){
      const ratio = clamped / prev;
      state.panX = (state.panX - anchorX) * ratio + anchorX;
      state.panY = (state.panY - anchorY) * ratio + anchorY;
    }
    if (state.zoomRange) state.zoomRange.value = String(clamped);
    if (state.zoomBtn) state.zoomBtn.setAttribute('aria-pressed', clamped > 1 ? 'true' : 'false');
    applyZoomTransform();
  }

  function resetZoom(){
    state.scale = 1;
    state.panX = 0;
    state.panY = 0;
    if (state.zoomRange) state.zoomRange.value = '1';
    if (state.zoomBtn) state.zoomBtn.setAttribute('aria-pressed', 'false');
    applyZoomTransform();
  }

  function setIndex(nextIdx, animated){
    if (!state.items.length) return;
    const clamped = clamp(nextIdx, 0, state.items.length - 1);
    state.idx = clamped;
    if (state.track){
      state.track.style.transition = animated ? 'transform 280ms cubic-bezier(0.25, 0.1, 0.25, 1)' : 'none';
      state.track.style.transform = `translateX(-${state.idx * 100}%)`;
    }
    if (state.prevBtn) state.prevBtn.disabled = state.idx <= 0;
    if (state.nextBtn) state.nextBtn.disabled = state.idx >= state.items.length - 1;
    if (state.counter) state.counter.textContent = `${state.idx + 1} / ${state.items.length}`;
    resetZoom();
  }

  function goPrev(){
    if (state.idx > 0) setIndex(state.idx - 1, true);
  }

  function goNext(){
    if (state.idx < state.items.length - 1) setIndex(state.idx + 1, true);
  }

  function closeViewer(){
    if (!state.open) return;
    state.open = false;
    if (state.overlay) state.overlay.classList.remove('open');
    if (state.overlay) state.overlay.setAttribute('aria-hidden', 'true');
    if (state.track){
      state.track.querySelectorAll('video').forEach(function(video){
        try { video.pause(); } catch (_) { }
      });
    }
    document.body.classList.remove('lmfs-open');
    if (state.lastActive && typeof state.lastActive.focus === 'function'){
      try { state.lastActive.focus(); } catch (_) { }
    }
  }

  function handleSwipeEnd(x){
    const width = state.stage ? (state.stage.clientWidth || 400) : 400;
    const dx = state.startX - x;
    const velocity = (x - state.lastX) / Math.max(10, Date.now() - state.lastTs);
    let targetIdx = state.idx;
    if (Math.abs(velocity) > SWIPE_MOMENTUM_THRESHOLD) {
      targetIdx = velocity < 0 ? state.idx + 1 : state.idx - 1;
    } else if (Math.abs(dx) > width * 0.14) {
      targetIdx = dx > 0 ? state.idx + 1 : state.idx - 1;
    }
    setIndex(targetIdx, true);
  }

  function onStageWheel(e){
    if (!state.open) return;
    if (e.ctrlKey || e.deltaY !== 0){
      e.preventDefault();
      const rect = state.stage.getBoundingClientRect();
      const anchorX = e.clientX - rect.left - rect.width / 2;
      const anchorY = e.clientY - rect.top - rect.height / 2;
      const next = state.scale + (e.deltaY < 0 ? 0.16 : -0.16);
      setScale(next, anchorX, anchorY);
    }
  }

  function onPointerDown(e){
    if (!state.open || e.button > 0) return;
    state.pointerId = e.pointerId;
    state.startX = e.clientX;
    state.startY = e.clientY;
    state.startTs = Date.now();
    state.lastX = e.clientX;
    state.lastTs = state.startTs;
    state.startIdx = state.idx;
    state.horizontalSwipe = null;
    state.panning = state.scale > 1;
    if (state.panning) {
      state.stage.setPointerCapture(e.pointerId);
      e.preventDefault();
    }
  }

  function onPointerMove(e){
    if (!state.open || state.pointerId !== e.pointerId) return;
    if (state.panning){
      const dx = e.clientX - state.lastX;
      const dy = e.clientY - state.startY;
      state.panX += dx;
      state.panY += dy;
      state.lastX = e.clientX;
      state.startY = e.clientY;
      applyZoomTransform();
      return;
    }
    const dx = Math.abs(e.clientX - state.startX);
    const dy = Math.abs(e.clientY - state.startY);
    if (state.horizontalSwipe === null) state.horizontalSwipe = dx > SWIPE_SLOPE * dy;
    if (!state.horizontalSwipe) return;
    const width = state.stage ? (state.stage.clientWidth || 400) : 400;
    const delta = (state.startX - e.clientX) / width;
    const raw = state.startIdx + delta;
    const clamped = clamp(raw, -0.18, state.items.length - 1 + 0.18);
    state.track.style.transition = 'none';
    state.track.style.transform = `translateX(-${clamped * 100}%)`;
    state.lastX = e.clientX;
    state.lastTs = Date.now();
    e.preventDefault();
  }

  function onPointerUp(e){
    if (!state.open || state.pointerId !== e.pointerId) return;
    if (state.panning){
      state.panning = false;
      state.pointerId = null;
      return;
    }
    if (state.horizontalSwipe) handleSwipeEnd(e.clientX);
    else setIndex(state.idx, true);
    state.pointerId = null;
  }

  function touchDistance(t0, t1){
    const dx = Number(t1.clientX || 0) - Number(t0.clientX || 0);
    const dy = Number(t1.clientY || 0) - Number(t0.clientY || 0);
    return Math.hypot(dx, dy);
  }

  function onTouchStart(e){
    if (!state.open) return;
    if (e.touches.length === 2){
      state.pinchStartDistance = touchDistance(e.touches[0], e.touches[1]);
      state.pinchStartScale = state.scale;
      state.touchDragging = false;
      return;
    }
    if (e.touches.length === 1 && state.scale <= 1){
      const t = e.touches[0];
      state.startX = t.clientX;
      state.startY = t.clientY;
      state.startTs = Date.now();
      state.lastX = t.clientX;
      state.lastTs = state.startTs;
      state.startIdx = state.idx;
      state.horizontalSwipe = null;
      state.touchDragging = true;
    }
  }

  function onTouchMove(e){
    if (!state.open) return;
    if (e.touches.length === 2){
      const nextDistance = touchDistance(e.touches[0], e.touches[1]);
      if (state.pinchStartDistance > 0){
        const ratio = nextDistance / state.pinchStartDistance;
        setScale(state.pinchStartScale * ratio);
        e.preventDefault();
      }
      return;
    }
    if (!state.touchDragging || !e.touches[0] || state.scale > 1) return;
    const t = e.touches[0];
    const dx = Math.abs(t.clientX - state.startX);
    const dy = Math.abs(t.clientY - state.startY);
    if (state.horizontalSwipe === null) state.horizontalSwipe = dx > SWIPE_SLOPE * dy;
    if (!state.horizontalSwipe) return;
    const width = state.stage ? (state.stage.clientWidth || 400) : 400;
    const delta = (state.startX - t.clientX) / width;
    const raw = state.startIdx + delta;
    const clamped = clamp(raw, -0.18, state.items.length - 1 + 0.18);
    state.track.style.transition = 'none';
    state.track.style.transform = `translateX(-${clamped * 100}%)`;
    state.lastX = t.clientX;
    state.lastTs = Date.now();
    e.preventDefault();
  }

  function onTouchEnd(e){
    if (!state.open) return;
    if (state.scale > 1){
      state.touchDragging = false;
      return;
    }
    if (state.touchDragging && e.changedTouches && e.changedTouches[0]){
      if (state.horizontalSwipe) handleSwipeEnd(e.changedTouches[0].clientX);
      else setIndex(state.idx, true);
    }
    state.touchDragging = false;
    state.pinchStartDistance = 0;
  }

  function onKeyDown(e){
    if (!state.open) return;
    if (e.key === 'Escape'){
      e.preventDefault();
      closeViewer();
      return;
    }
    if (e.key === 'ArrowLeft'){
      e.preventDefault();
      goPrev();
      return;
    }
    if (e.key === 'ArrowRight'){
      e.preventDefault();
      goNext();
    }
  }

  function buildSlide(item){
    const slide = createEl('div', 'lmfs-slide');
    if (item.type === 'video'){
      const video = createEl('video', 'lmfs-video');
      video.src = item.url;
      if (item.poster) video.poster = item.poster;
      video.controls = true;
      video.playsInline = true;
      slide.appendChild(video);
      return slide;
    }
    const img = createEl('img', 'lmfs-img');
    img.src = item.url;
    img.alt = item.alt || 'Image';
    img.loading = 'eager';
    img.draggable = false;
    slide.appendChild(img);
    return slide;
  }

  function ensureDom(){
    if (state.overlay) return;
    const overlay = createEl('div', 'lmfs-overlay');
    overlay.setAttribute('aria-hidden', 'true');
    const closeIcon = '<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path d="M6 6l12 12M18 6L6 18" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>';
    const prevIcon = '<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path d="M15 18l-6-6 6-6" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>';
    const nextIcon = '<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path d="M9 18l6-6-6-6" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>';
    const zoomIcon = '<svg viewBox="0 0 24 24" aria-hidden="true" focusable="false"><circle cx="11" cy="11" r="6" fill="none" stroke="currentColor" stroke-width="2"/><path d="M21 21l-4.2-4.2M11 8v6M8 11h6" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>';
    overlay.innerHTML =
      '<div class="lmfs-shell" role="dialog" aria-modal="true" aria-label="Fullscreen media viewer">' +
        `<button type="button" class="lmfs-close" aria-label="Close viewer">${closeIcon}</button>` +
        `<button type="button" class="lmfs-nav lmfs-prev" aria-label="Previous media">${prevIcon}</button>` +
        `<button type="button" class="lmfs-nav lmfs-next" aria-label="Next media">${nextIcon}</button>` +
        '<div class="lmfs-stage">' +
          '<div class="lmfs-track"></div>' +
        '</div>' +
        '<div class="lmfs-toolbar">' +
          `<button type="button" class="lmfs-zoom-btn" aria-label="Toggle zoom" aria-pressed="false">${zoomIcon}</button>` +
          '<input class="lmfs-zoom-range" type="range" min="1" max="4" step="0.1" value="1" aria-label="Zoom" />' +
          '<span class="lmfs-counter">1 / 1</span>' +
        '</div>' +
      '</div>';
    document.body.appendChild(overlay);
    state.overlay = overlay;
    state.stage = overlay.querySelector('.lmfs-stage');
    state.track = overlay.querySelector('.lmfs-track');
    state.closeBtn = overlay.querySelector('.lmfs-close');
    state.prevBtn = overlay.querySelector('.lmfs-prev');
    state.nextBtn = overlay.querySelector('.lmfs-next');
    state.zoomBtn = overlay.querySelector('.lmfs-zoom-btn');
    state.zoomRange = overlay.querySelector('.lmfs-zoom-range');
    state.counter = overlay.querySelector('.lmfs-counter');

    overlay.addEventListener('click', function(e){
      const t = e.target;
      if (!(t instanceof Element)) return;
      if (t.closest('.lmfs-close,.lmfs-nav,.lmfs-toolbar')) return;
      if (t.closest('.lmfs-img,.lmfs-video')) return;
      if (t.closest('.lmfs-stage,.lmfs-slide,.lmfs-track,.lmfs-shell') || t === overlay) {
        closeViewer();
      }
    });
    state.closeBtn.addEventListener('click', closeViewer);
    state.prevBtn.addEventListener('click', function(e){ e.stopPropagation(); goPrev(); });
    state.nextBtn.addEventListener('click', function(e){ e.stopPropagation(); goNext(); });
    state.zoomBtn.addEventListener('click', function(e){
      e.stopPropagation();
      setScale(state.scale > 1 ? 1 : 2);
    });
    state.zoomRange.addEventListener('input', function(){
      const next = Number(state.zoomRange.value || 1);
      setScale(next);
    });

    state.stage.addEventListener('wheel', onStageWheel, { passive: false });
    state.stage.addEventListener('pointerdown', onPointerDown, { passive: false });
    state.stage.addEventListener('pointermove', onPointerMove, { passive: false });
    state.stage.addEventListener('pointerup', onPointerUp, { passive: true });
    state.stage.addEventListener('pointercancel', onPointerUp, { passive: true });
    state.stage.addEventListener('touchstart', onTouchStart, { passive: true });
    state.stage.addEventListener('touchmove', onTouchMove, { passive: false });
    state.stage.addEventListener('touchend', onTouchEnd, { passive: true });
    state.stage.addEventListener('touchcancel', onTouchEnd, { passive: true });
    document.addEventListener('keydown', onKeyDown);
  }

  function open(payload){
    ensureDom();
    const rawItems = Array.isArray(payload && payload.items) ? payload.items : [];
    const items = rawItems.map(normalizeItem).filter(Boolean);
    if (!items.length) return false;
    state.items = items;
    state.idx = clamp(Number(payload && payload.startIndex) || 0, 0, items.length - 1);
    state.track.innerHTML = '';
    items.forEach(function(item){
      state.track.appendChild(buildSlide(item));
    });
    setIndex(state.idx, false);
    state.open = true;
    state.lastActive = document.activeElement;
    state.overlay.classList.add('open');
    state.overlay.setAttribute('aria-hidden', 'false');
    document.body.classList.add('lmfs-open');
    return true;
  }

  window.LiberMediaFullscreenViewer = {
    open: open,
    close: closeViewer,
    isOpen: function(){ return !!state.open; }
  };
})();
