/* eslint-disable import/no-unresolved */
(() => {

  class SecureChatApp {
    constructor() {
      this.db = null;
      this.storage = null;
      this.currentUser = null;
      this.activeConnection = null;
      this.connections = [];
      this.sharedKeyCache = {}; // connId -> CryptoKey
      this.me = null; // cached profile
      this.usernameCache = new Map(); // uid -> {username, avatarUrl}
      this.userUnsubs = new Map(); // uid -> unsubscribe
      this._activePCs = new Map(); // peerUid -> {pc, unsubs, stream, videoEl}
      this._roomUnsub = null;
      this._peersUnsub = null;
      this._roomState = null;
      this._peersPresence = {};
      this._lastSpeech = new Map(); // uid -> timestamp
      this._inactTimer = null;
      this._videoEnabled = false;
      this._micEnabled = true;
      this._monitoring = false;
      this._monitorStream = null;
      this._inRoom = false;
      this._startingCall = false;
      this._joiningCall = false;
      this._cleanupIdleTimer = null;
      this._lastJoinedCallId = null;
      this._connectingCid = null;
      this._activeCid = null;
      this._joinRetryTimer = null;
      this._forceRelay = false; // Prefer mixed ICE; TURN is still included in iceServers.
      this._pcWatchdogs = new Map(); // key: callId:peerUid -> {t1,t2}
      this._msgLoadSeq = 0;
      this._connLoadSeq = 0;
      this._connRetryTimer = null;
      this._lastRenderSigByConn = new Map();
      this._lastDocIdsByConn = new Map();
      this._lastOldestDocSnapshotByConn = new Map();
      this._loadMoreOlderInFlight = false;
      this._hasMoreOlderByConn = new Map();
      this._lastDayByConn = new Map();
      this._fallbackKeyCandidatesCache = new Map();
      this._voiceWidgets = new Map();
      this._voiceCurrentSrc = '';
      this._voiceCurrentAttachmentKey = '';
      this._voiceCurrentTitle = 'Voice message';
      this._voiceUserIntendedPlay = false;
      this._voiceWaveCache = new Map();
      this._voiceDurationCache = new Map();
      this._voiceWaveCtx = null;
      this._voiceHydrateQueue = [];
      this._voiceHydrateRunning = 0;
      this._voiceHydrateMax = 1;
      this._voiceHydrateSession = 0;
      this._voiceProgressRaf = 0;
      this._senderLookupInFlight = new Set();
      this._typingUnsub = null;
      this._typingTicker = null;
      this._typingByUid = {};
      this._typingLastSent = false;
      this._typingLastSentAt = 0;
      this._recordMode = 'audio';
      this._recFacing = 'user';
      this._setActiveSeq = 0;
      this._attachmentPreviewQueue = [];
      this._attachmentPreviewRunning = 0;
      this._attachmentPreviewMax = 4;
      this._chatAudioPlaylist = [];
      this._peerUidByConn = new Map();
      this._avatarCache = new Map();
      this._attachmentBlobUrlByKey = new Map();
      this._liveSnapshotPrimedByConn = new Map();
      this._suppressLivePatchUntilByConn = new Map();
      this._actionPressArmed = false;
      this._isRecordingByHold = false;
      this._suppressActionClickUntil = 0;
      this._recordingSendInFlight = false;
      this._pendingRecording = null;
      this._pendingAttachments = [];
      this._pendingRemoteShares = [];
      this._pendingReusedAttachments = [];
      this._readMarkTimer = null;
      this._activeConnOpenedAt = 0;
      this._latestPeerMessageMsByConn = new Map();
      this._pendingRequestCount = 0;
      this._pendingRequestUnsub = null;
      this.init();
    }

    computeConnKey(uids){
      try{ return (uids||[]).slice().sort().join('|'); }catch(_){ return ''; }
    }

    getReadMap(){
      try{
        const raw = localStorage.getItem('liber_chat_read_map_v1');
        const data = raw ? JSON.parse(raw) : {};
        return (data && typeof data === 'object') ? data : {};
      }catch(_){ return {}; }
    }

    getReadMarkerForConn(connId){
      try{
        const map = this.getReadMap();
        return Number(map[String(connId || '')] || 0) || 0;
      }catch(_){ return 0; }
    }

    toTimestampMs(value){
      try{
        if (value && typeof value.toMillis === 'function'){
          const ms = Number(value.toMillis() || 0) || 0;
          return ms > 0 ? ms : 0;
        }
        if (value && typeof value.seconds === 'number'){
          const ms = Math.floor(Number(value.seconds || 0) * 1000);
          return ms > 0 ? ms : 0;
        }
        if (typeof value === 'number'){
          return Number.isFinite(value) && value > 0 ? value : 0;
        }
        const ms = Number(new Date(value || 0).getTime() || 0) || 0;
        return ms > 0 ? ms : 0;
      }catch(_){ return 0; }
    }

    getEffectiveReadMarkerForConn(connId, connData = null){
      try{
        const id = String(connId || '').trim();
        if (!id) return 0;
        const localMs = this.getReadMarkerForConn(id);
        const conn = connData || (this.connections || []).find((c)=> c && c.id === id) || null;
        const connReadBy = conn && typeof conn.readBy === 'object' ? conn.readBy : null;
        const activeReadBy = (id === this.activeConnection && this._activeConnReadBy && typeof this._activeConnReadBy === 'object')
          ? this._activeConnReadBy
          : null;
        const serverMs = Math.max(
          this.toTimestampMs(connReadBy ? connReadBy[this.currentUser?.uid] : 0),
          this.toTimestampMs(activeReadBy ? activeReadBy[this.currentUser?.uid] : 0)
        );
        return Math.max(localMs, serverMs);
      }catch(_){ return 0; }
    }

    setReadMarkerForConn(connId, ts = Date.now()){
      try{
        const key = String(connId || '').trim();
        if (!key) return;
        const map = this.getReadMap();
        const next = Number(ts || Date.now()) || Date.now();
        const prev = Number(map[key] || 0) || 0;
        map[key] = Math.max(prev, next);
        localStorage.setItem('liber_chat_read_map_v1', JSON.stringify(map));
      }catch(_){ }
    }

    async updateUnreadBadges(){
      try{
        let unreadChats = 0;
        const listEl = document.getElementById('connections-list');
        if (listEl){
          listEl.querySelectorAll('li[data-id]').forEach((li)=>{
            const id = li.getAttribute('data-id');
            const conn = (this.connections || []).find((c)=> c && c.id === id);
            const updatedMs = Number(new Date(conn?.updatedAt || 0).getTime() || 0) || 0;
            let readMs = this.getEffectiveReadMarkerForConn(id, conn);
            const openedAgo = Date.now() - Number(this._activeConnOpenedAt || 0);
            if (id && id === this.activeConnection && openedAgo >= 4800 && updatedMs > readMs){
              // Auto-clear unread for actively viewed chat after the grace period.
              this.setReadMarkerForConn(id, updatedMs);
              readMs = updatedMs;
            }
            const fromPeer = String(conn?.lastMessageSender || '').trim() && String(conn?.lastMessageSender || '').trim() !== this.currentUser?.uid;
            const isUnread = !!id && fromPeer && updatedMs > readMs;
            let dot = li.querySelector('.chat-unread-dot');
            if (isUnread){
              unreadChats += 1;
              if (!dot){
                dot = document.createElement('span');
                dot.className = 'chat-unread-dot';
                dot.style.cssText = 'margin-left:auto;min-width:8px;height:8px;border-radius:50%;background:#4da3ff;display:inline-block;box-shadow:0 0 0 2px rgba(77,163,255,.2);flex-shrink:0';
                const row = li.querySelector('.chat-conn-row');
                if (row) row.appendChild(dot);
                else li.appendChild(dot);
              }
            } else if (dot){
              dot.remove();
            }
          });
        }
        try{
          if ('setAppBadge' in navigator){
            if (unreadChats > 0) await navigator.setAppBadge(unreadChats);
            else if ('clearAppBadge' in navigator) await navigator.clearAppBadge();
          }
        }catch(_){ }
        const unreadBadge = document.getElementById('connections-unread-badge');
        if (unreadBadge){
          if (unreadChats > 0){
            unreadBadge.textContent = String(unreadChats > 99 ? '99+' : unreadChats);
            unreadBadge.classList.remove('hidden');
            unreadBadge.removeAttribute('aria-hidden');
          }else{
            unreadBadge.classList.add('hidden');
            unreadBadge.setAttribute('aria-hidden', 'true');
          }
        }
        try{
          localStorage.setItem('liber_chat_unread_count', String(unreadChats));
          const target = window.parent !== window ? window.parent : (window.top || window);
          if (target && target.postMessage) target.postMessage({ type: 'liber:chat-unread', count: unreadChats }, '*');
        }catch(_){ }
      }catch(_){ }
    }

    isMobileViewport(){
      return !!window.matchMedia && window.matchMedia('(max-width: 768px)').matches;
    }

    setMobileMenuOpen(open){
      const sidebar = document.querySelector('.sidebar');
      const app = document.getElementById('chat-app');
      if (!sidebar || !app) return;
      sidebar.classList.toggle('open', !!open);
      if (!open) sidebar.classList.remove('searching');
      app.classList.toggle('mobile-menu-open', !!open);
      const tip = document.getElementById('mobile-sidebar-tip');
      if (tip){
        tip.setAttribute('aria-label', open ? 'Collapse chat menu' : 'Expand chat menu');
      }
    }

    updateSidebarSearchState(hasResults){
      const sidebar = document.querySelector('.sidebar');
      if (!sidebar) return;
      sidebar.classList.toggle('searching', !!hasResults);
      if (hasResults) sidebar.classList.add('open');
    }

    formatMessageTime(value, msg = null){
      let d;
      try{
        const ts = msg?.createdAtTS;
        let ms = 0;
        if (ts) {
          if (typeof ts.toMillis === 'function') ms = Number(ts.toMillis());
          else if (typeof ts.seconds === 'number' && ts.seconds > 0) ms = ts.seconds * 1000 + (Number(ts.nanoseconds || 0) / 1e6);
        }
        if (ms > 0) d = new Date(ms);
        else {
          d = new Date(value || msg?.createdAt || 0);
          if (msg?.sender === this.currentUser?.uid && (Number.isNaN(d.getTime()) || d.getTime() <= 0)) d = new Date();
        }
        if (Number.isNaN(d.getTime()) || d.getTime() <= 0) d = new Date(value || msg?.createdAt || Date.now());
      }catch(_){ d = new Date(value || msg?.createdAt || Date.now()); }
      if (Number.isNaN(d.getTime())) return '';
      const s = d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
      return typeof s === 'string' ? s : String(d.getHours()).padStart(2,'0') + ':' + String(d.getMinutes()).padStart(2,'0');
    }

    formatMessageDay(value, msg = null){
      let d;
      try{
        const ms = msg ? this.getMessageTimestampMs(msg) : 0;
        if (ms > 0) d = new Date(ms);
        else {
          d = new Date(value || msg?.createdAt || 0);
          if (msg?.sender === this.currentUser?.uid && (Number.isNaN(d.getTime()) || d.getTime() <= 0)) d = new Date();
        }
        if (Number.isNaN(d.getTime()) || d.getTime() <= 0) d = new Date(value || msg?.createdAt || Date.now());
        const nowMs = Date.now();
        const age = nowMs - d.getTime();
        if (msg?.sender === this.currentUser?.uid && age >= -60000 && age < 24 * 60 * 60 * 1000) return 'Today';
        if (age >= -60000 && age < 24 * 60 * 60 * 1000) {
          const now = new Date();
          const todayStart = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
          const thatStart = new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime();
          if (thatStart >= todayStart - 60000) return 'Today';
        }
        const fallbackMs = msg ? this.getMessageTimestampMs(msg) : 0;
        if (msg?.sender === this.currentUser?.uid && fallbackMs > 0) {
          const now = new Date();
          const todayStart = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
          const thatDate = new Date(fallbackMs);
          const thatStart = new Date(thatDate.getFullYear(), thatDate.getMonth(), thatDate.getDate()).getTime();
          if (thatStart >= todayStart - 60000) return 'Today';
        }
        if (d.getTime() > nowMs + 60000) d = new Date(nowMs);
      }catch(_){ d = new Date(value || msg?.createdAt || Date.now()); }
      if (Number.isNaN(d.getTime())) return '';
      try{
        const now = new Date();
        const todayStart = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
        const thatStart = new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime();
        const dayMs = 24 * 60 * 60 * 1000;
        const deltaDays = Math.round((todayStart - thatStart) / dayMs);
        if (deltaDays === 0) return 'Today';
        if (deltaDays === 1) return 'Yesterday';
      }catch(_){ }
      return d.toLocaleDateString([], { year: 'numeric', month: 'short', day: 'numeric' });
    }

    formatDuration(seconds){
      const raw = Number(seconds);
      const s = Number.isFinite(raw) && raw > 0 ? Math.floor(raw) : 0;
      const m = Math.floor(s / 60);
      const ss = String(s % 60).padStart(2, '0');
      return `${m}:${ss}`;
    }

    openFullscreenImage(src, alt = 'image'){
      try{
        const url = String(src || '').trim();
        if (!url) return;
        const existing = document.getElementById('chat-image-lightbox');
        if (existing) existing.remove();
        const overlay = document.createElement('div');
        overlay.id = 'chat-image-lightbox';
        overlay.style.cssText = 'position:fixed;inset:0;z-index:20000;background:rgba(0,0,0,.92);display:flex;align-items:center;justify-content:center;padding:16px';
        overlay.innerHTML = `<button type="button" aria-label="Close" style="position:fixed;top:12px;right:12px;background:rgba(16,20,28,.92);border:1px solid rgba(255,255,255,.2);color:#fff;border-radius:10px;padding:8px 10px;cursor:pointer;z-index:1"><i class="fas fa-xmark"></i></button><img src="${url.replace(/"/g,'&quot;')}" alt="${String(alt || 'image').replace(/"/g,'&quot;')}" style="max-width:100%;max-height:100%;object-fit:contain;border-radius:10px">`;
        const close = ()=>{ try{ overlay.remove(); }catch(_){ } };
        overlay.addEventListener('click', (e)=>{ if (e.target === overlay) close(); });
        const closeBtn = overlay.querySelector('button');
        if (closeBtn) closeBtn.addEventListener('click', close);
        document.body.appendChild(overlay);
      }catch(_){ }
    }

    setupFullscreenImagePreview(){
      if (this._fullscreenImagePreviewBound) return;
      this._fullscreenImagePreviewBound = true;
      document.addEventListener('click', (e)=>{
        try{
          const target = e.target;
          if (!(target instanceof HTMLElement)) return;
          const img = target.closest('img');
          if (!(img instanceof HTMLImageElement)) return;
          if (img.closest('button,[data-user-preview],.chat-conn-avatar,.avatar,.mini-player')) return;
          const isPreviewImage = img.closest('.msg-text')
            || img.closest('.file-preview')
            || img.closest('#chat-attachments-sheet')
            || img.classList.contains('composer-attachment-thumb')
            || img.getAttribute('data-fullscreen-image') === '1';
          if (!isPreviewImage) return;
          const src = String(img.currentSrc || img.src || '').trim();
          if (!src) return;
          e.preventDefault();
          e.stopPropagation();
          this.openFullscreenImage(src, img.alt || 'image');
        }catch(_){ }
      }, true);
    }

    isEditedMessage(msg){
      try{
        const createdMs = Number(msg?.createdAtTS?.toMillis?.() || 0) || Number(new Date(msg?.createdAt || 0).getTime() || 0) || 0;
        const updatedMs = Number(new Date(msg?.updatedAt || 0).getTime() || 0) || 0;
        return updatedMs > 0 && createdMs > 0 && updatedMs > (createdMs + 1000);
      }catch(_){ return false; }
    }

    getDeliveryLabel(msg){
      try{
        if (!msg || msg.sender !== this.currentUser?.uid) return '';
        const msgTs = this.getMessageTimestampMs(msg);
        if (!msgTs) return 'Sent';
        const res = this._getDeliveryLabelInner(msg, msgTs);
        return typeof res === 'string' ? res : 'Sent';
      }catch(_){ return ''; }
    }
    _getDeliveryLabelInner(msg, msgTs){
      try{
        // "Read" only when recipient actually saw the message – use peer's readBy, not ours.
        const readBy = (this._activeConnReadBy && typeof this._activeConnReadBy === 'object')
          ? this._activeConnReadBy
          : (msg.readBy && typeof msg.readBy === 'object' ? msg.readBy : null);
        if (readBy){
          const peers = Object.entries(readBy).filter(([uid])=> uid && uid !== this.currentUser.uid);
          const peerReadPast = peers.some(([,ts])=> this.toTimestampMs(ts) >= msgTs);
          if (peerReadPast) return 'Read';
        }
        // If peer sent a later message, they had the chat open – treat as seen.
        const latestPeerMs = Number(this._latestPeerMessageMsByConn.get(String(this.activeConnection || '')) || 0) || 0;
        if (latestPeerMs >= msgTs) return 'Read';
        return 'Sent';
      }catch(_){ return ''; }
    }

    getMessageTimestampMs(msg){
      try{
        const ts = msg?.createdAtTS;
        let ms = 0;
        if (ts) {
          if (typeof ts.toMillis === 'function') ms = Number(ts.toMillis());
          else if (typeof ts.seconds === 'number' && ts.seconds > 0) ms = ts.seconds * 1000 + (Number(ts.nanoseconds || 0) / 1e6);
        }
        if (ms <= 0) ms = Number(new Date(msg?.createdAt || 0).getTime() || 0);
        return ms || 0;
      }catch(_){ return Number(new Date(msg?.createdAt || 0).getTime() || 0) || 0; }
    }

    applyNewMessagesSeparator(box){
      try{
        if (!box) return;
        box.querySelectorAll('.new-messages-separator').forEach((el)=> el.remove());
        const messages = Array.from(box.querySelectorAll('.message'));
        if (!messages.length) return;
        let seenUnread = false;
        for (const el of messages){
          const isUnread = el.dataset.unread === '1';
          if (isUnread){ seenUnread = true; continue; }
          if (seenUnread){
            const sep = document.createElement('div');
            sep.className = 'new-messages-separator';
            sep.textContent = 'New messages';
            box.insertBefore(sep, el);
            break;
          }
        }
      }catch(_){ }
    }

    markVisibleMessagesReadInDom(connId, markerMs){
      try{
        const box = document.getElementById('messages');
        if (!box || String(connId || '') !== String(this.activeConnection || '')) return;
        const target = Number(markerMs || 0) || 0;
        if (target <= 0) return;
        box.querySelectorAll('.message').forEach((el)=>{
          if (el.classList.contains('self')) return;
          const ts = Number(el.dataset.msgTs || 0) || 0;
          if (ts > 0 && ts <= target) el.dataset.unread = '0';
        });
        this.applyNewMessagesSeparator(box);
      }catch(_){ }
    }

    async markConnectionReadAfterDelay(connId){
      try{
        const id = String(connId || '').trim();
        if (!id || this.activeConnection !== id) return;
        const box = document.getElementById('messages');
        let maxPeerTs = 0;
        if (box){
          box.querySelectorAll('.message.other').forEach((el)=>{
            const ts = Number(el.dataset.msgTs || 0) || 0;
            if (ts > maxPeerTs) maxPeerTs = ts;
          });
        }
        const conn = (this.connections || []).find((c)=> c && c.id === id);
        const connUpdatedMs = Number(new Date(conn?.updatedAt || 0).getTime() || 0) || 0;
        const markerMs = Math.max(Date.now(), maxPeerTs, connUpdatedMs, this.getEffectiveReadMarkerForConn(id, conn));
        this.setReadMarkerForConn(id, markerMs);
        this.markVisibleMessagesReadInDom(id, markerMs);
        const stampIso = new Date(markerMs).toISOString();
        try{
          firebase.updateDoc(firebase.doc(this.db,'chatConnections', id), {
            [`readBy.${this.currentUser.uid}`]: stampIso
          }).catch(()=>{});
          this._activeConnReadBy = { ...(this._activeConnReadBy || {}), [this.currentUser.uid]: stampIso };
        }catch(_){ }
        this.updateUnreadBadges().catch(()=>{});
      }catch(_){ }
    }

    async dissolveOutRemove(el, ms = 220){
      try{
        if (!el || !el.isConnected) return;
        el.classList.add('liber-dissolve-out');
        await new Promise((r)=> setTimeout(r, Math.max(120, ms)));
        if (el && el.isConnected) el.remove();
      }catch(_){ }
    }

    paintSeedWaveBars(wave, barsCount, seed){
      if (!wave) return;
      wave.innerHTML = '';
      for (let i = 0; i < barsCount; i++){
        const bar = document.createElement('span');
        bar.className = 'bar';
        const seedCode = seed.charCodeAt(i % (seed.length || 1)) || 37;
        const waveBase = (Math.sin((i / barsCount) * Math.PI * 2) + 1) * 0.5;
        const jitter = ((seedCode * (i + 7)) % 9) / 10;
        const h = 5 + Math.round((waveBase * 12) + (jitter * 6));
        bar.style.height = `${h}px`;
        wave.appendChild(bar);
      }
    }

    applyWaveHeights(wave, heights){
      if (!wave || !Array.isArray(heights) || !heights.length) return;
      wave.innerHTML = '';
      heights.forEach((h)=>{
        const bar = document.createElement('span');
        bar.className = 'bar';
        bar.style.height = `${Math.max(4, Math.min(24, Math.round(h)))}px`;
        wave.appendChild(bar);
      });
    }

    async getWaveHeightsForAudio(url, barsCount = 54){
      try{
        const src = String(url || '').trim();
        if (!src) return null;
        const key = `${src}::${barsCount}`;
        if (this._voiceWaveCache.has(key)) return await this._voiceWaveCache.get(key);
        const p = (async ()=>{
          const resp = await fetch(src, { mode: 'cors' });
          if (!resp.ok) return null;
          const buf = await resp.arrayBuffer();
          if (!this._voiceWaveCtx){
            const AC = window.AudioContext || window.webkitAudioContext;
            if (!AC) return null;
            this._voiceWaveCtx = new AC();
          }
          const audioBuf = await this._voiceWaveCtx.decodeAudioData(buf.slice(0));
          const channelCount = Math.max(1, Number(audioBuf.numberOfChannels || 1));
          const channels = [];
          for (let c = 0; c < channelCount; c++){
            channels.push(audioBuf.getChannelData(c));
          }
          const total = channels[0] ? channels[0].length : 0;
          if (!total) return null;
          const step = Math.max(1, Math.floor(total / barsCount));
          const out = [];
          for (let i = 0; i < barsCount; i++){
            const start = i * step;
            const end = Math.min(total, start + step);
            let peak = 0;
            for (let j = start; j < end; j++){
              let sample = 0;
              for (let c = 0; c < channelCount; c++){
                const v = Math.abs((channels[c] && channels[c][j]) || 0);
                if (v > sample) sample = v;
              }
              if (sample > peak) peak = sample;
            }
            out.push(peak);
          }
          // Light smoothing + log shaping so visible bars mirror loudness changes better.
          const smooth = out.map((v, i)=>{
            const a = out[Math.max(0, i - 1)] || v;
            const b = out[i] || v;
            const c = out[Math.min(out.length - 1, i + 1)] || v;
            return (a * 0.25) + (b * 0.5) + (c * 0.25);
          });
          const max = Math.max(...smooth, 0.0001);
          return smooth.map((v)=>{
            const norm = Math.max(0, Math.min(1, v / max));
            const shaped = Math.log10(1 + (9 * norm)); // 0..1
            return 4 + (shaped * 20);
          });
        })();
        this._voiceWaveCache.set(key, p);
        const result = await p;
        if (!result) this._voiceWaveCache.delete(key);
        return result;
      }catch(_){ return null; }
    }

    async getDurationForAudio(url){
      try{
        const src = String(url || '').trim();
        if (!src) return 0;
        if (this._voiceDurationCache.has(src)) return await this._voiceDurationCache.get(src);
        const p = new Promise((resolve)=>{
          const a = document.createElement('audio');
          a.preload = 'metadata';
          a.src = src;
          const done = (v)=> resolve(Number.isFinite(v) && v > 0 ? v : 0);
          a.addEventListener('loadedmetadata', ()=> done(a.duration), { once: true });
          a.addEventListener('error', ()=> done(0), { once: true });
          setTimeout(()=> done(0), 6000);
        });
        this._voiceDurationCache.set(src, p);
        return await p;
      }catch(_){ return 0; }
    }

    hydrateVoiceWidgetMedia(widget, barsCount, seed){
      const mySession = this._voiceHydrateSession || 0;
      this.getDurationForAudio(widget.src).then((duration)=>{
        try{
          if ((this._voiceHydrateSession || 0) !== mySession) return;
          if (!widget || !widget.wave || !widget.wave.isConnected) return;
          if (duration > 0){
            widget.durationGuess = duration;
            this.updateVoiceWidgets();
          }
        }catch(_){ }
      }).catch(()=>{});
      this.enqueueVoiceWaveHydrate(widget, barsCount, seed, { priority: false, session: mySession });
    }

    enqueueVoiceWaveHydrate(widget, barsCount, seed, opts = {}){
      try{
        const session = Number.isFinite(Number(opts.session)) ? Number(opts.session) : (this._voiceHydrateSession || 0);
        const priority = !!opts.priority;
        const run = async ()=>{
          if ((this._voiceHydrateSession || 0) !== session) return;
          if (!widget || !widget.wave || !widget.wave.isConnected) return;
          try{
            const heights = await this.getWaveHeightsForAudio(widget.src, barsCount);
            if ((this._voiceHydrateSession || 0) !== session) return;
            if (!widget.wave || !widget.wave.isConnected) return;
            if (Array.isArray(heights) && heights.length){
              this.applyWaveHeights(widget.wave, heights);
              this.updateVoiceWidgets();
            } else {
              this.paintSeedWaveBars(widget.wave, barsCount, seed);
            }
          }catch(_){ }
        };
        if (priority) this._voiceHydrateQueue.unshift(run);
        else this._voiceHydrateQueue.push(run);
        this.pumpVoiceWaveHydrateQueue();
      }catch(_){ }
    }

    pumpVoiceWaveHydrateQueue(){
      try{
        if ((this._voiceHydrateRunning || 0) >= (this._voiceHydrateMax || 1)) return;
        const next = this._voiceHydrateQueue.shift();
        if (!next) return;
        this._voiceHydrateRunning = (this._voiceHydrateRunning || 0) + 1;
        const done = ()=>{
          this._voiceHydrateRunning = Math.max(0, (this._voiceHydrateRunning || 1) - 1);
          this.pumpVoiceWaveHydrateQueue();
        };
        const invoke = ()=>{
          Promise.resolve().then(next).catch(()=>{}).finally(done);
        };
        if (typeof window !== 'undefined' && typeof window.requestIdleCallback === 'function'){
          window.requestIdleCallback(invoke, { timeout: 700 });
        } else {
          setTimeout(invoke, 0);
        }
      }catch(_){ }
    }

    enqueueAttachmentPreview(task, loadSeq, connId){
      try{
        this._attachmentPreviewQueue.push({ task, loadSeq, connId });
        this.pumpAttachmentPreviewQueue();
      }catch(_){ }
    }

    pumpAttachmentPreviewQueue(){
      try{
        const max = this._attachmentPreviewMax ?? 12;
        while ((this._attachmentPreviewRunning || 0) < max && this._attachmentPreviewQueue.length){
          const item = this._attachmentPreviewQueue.shift();
          this._attachmentPreviewRunning = (this._attachmentPreviewRunning || 0) + 1;
          const run = async ()=>{
            try{
              if (!item || typeof item.task !== 'function') return;
              if (item.loadSeq !== this._msgLoadSeq) return;
              if (item.connId && item.connId !== this.activeConnection) return;
              await item.task();
            }catch(e){}
            finally{
              this._attachmentPreviewRunning = Math.max(0, (this._attachmentPreviewRunning || 1) - 1);
              this.pumpAttachmentPreviewQueue();
            }
          };
          Promise.resolve(run()).catch(()=>{});
        }
      }catch(_){ }
    }

    async yieldToUi(){
      await new Promise((resolve)=>{
        try{
          if (typeof window !== 'undefined' && typeof window.requestAnimationFrame === 'function'){
            window.requestAnimationFrame(()=> resolve());
            return;
          }
        }catch(_){ }
        setTimeout(resolve, 0);
      });
    }

    stripPlaceholderText(text){
      const raw = String(text || '').trim();
      if (!raw) return '';
      if (/^\[file\]/i.test(raw)) return '';
      if (/^\[sticker\]/i.test(raw)) return '';
      if (/^\[(voice|video) message\]/i.test(raw)) return '';
      return raw;
    }

    inferAttachmentFileName(msg, text){
      const fileName = String(msg?.fileName || '').trim();
      if (fileName) return fileName;
      const plain = String(text || '').trim();
      const preview = String(msg?.previewText || '').trim();
      const url = String(msg?.fileUrl || '');
      const previewNameMatch = /\[(?:Attachment|File)\]\s+(.+)$/i.exec(preview);
      const previewName = previewNameMatch ? String(previewNameMatch[1] || '').trim() : '';
      if (previewName && /\.[a-z0-9]{2,6}$/i.test(previewName)) return previewName;
      if (/^\[voice message\]/i.test(plain) || /^\[voice message\]/i.test(preview)) return 'voice.webm';
      if (/^\[video message\]/i.test(plain) || /^\[video message\]/i.test(preview)) return 'video.webm';
      if (/\.(mp4|webm|mov|m4v)(\?|$)/i.test(url)) return 'video.mp4';
      const audioExt = /\.(mp3|m4a|aac|ogg|wav)(\?|$)/i.exec(url);
      if (audioExt) return `audio.${audioExt[1].toLowerCase()}`;
      if (/\.(jpe?g|png|gif|webp)(\?|$)/i.test(url)) return 'image.jpg';
      return '';
    }

    getRecentAttachments(){
      try{
        const raw = localStorage.getItem('liber_recent_attachments');
        const list = raw ? JSON.parse(raw) : [];
        return Array.isArray(list) ? list : [];
      }catch(_){ return []; }
    }

    pushRecentAttachment(item){
      try{
        const list = this.getRecentAttachments().filter(x=> x && x.fileUrl !== item.fileUrl);
        list.unshift(item);
        list.sort((a,b)=> new Date(b.sentAt||0) - new Date(a.sentAt||0));
        localStorage.setItem('liber_recent_attachments', JSON.stringify(list.slice(0, 40)));
      }catch(_){ }
    }

    showAttachmentQuickActions(){
      const existing = document.getElementById('attachment-quick-actions');
      const existingBackdrop = document.getElementById('attachment-quick-actions-backdrop');
      if (existing){ existing.remove(); if (existingBackdrop) existingBackdrop.remove(); return; }
      const backdrop = document.createElement('div');
      backdrop.id = 'attachment-quick-actions-backdrop';
      backdrop.style.cssText = 'position:fixed;inset:0;z-index:96;background:transparent';
      const panel = document.createElement('div');
      panel.id = 'attachment-quick-actions';
      panel.style.cssText = 'position:fixed;left:8px;right:8px;bottom:calc(118px + env(safe-area-inset-bottom));z-index:97;background:#10141c;border:1px solid #2a2f36;border-radius:10px;padding:8px;max-height:58vh;overflow:auto';
      const upload = document.createElement('button');
      upload.className = 'btn secondary';
      upload.textContent = 'Choose files';
      upload.style.marginBottom = '8px';
      upload.onclick = ()=>{
        // Keep selection inside in-app libraries popup (no OS file explorer).
        try{
          panel.scrollTo({ top: 0, behavior: 'smooth' });
        }catch(_){ panel.scrollTop = 0; }
        if (typeof tabVideo.onclick === 'function') tabVideo.onclick();
      };
      panel.appendChild(upload);
      const browseDevice = document.createElement('button');
      browseDevice.className = 'btn secondary';
      browseDevice.textContent = 'Browse device';
      browseDevice.style.marginBottom = '8px';
      browseDevice.onclick = ()=>{
        panel.remove();
        backdrop.remove();
        const fileInput = document.getElementById('file-input');
        if (fileInput) fileInput.click();
      };
      panel.appendChild(browseDevice);
      const mineTitle = document.createElement('div');
      mineTitle.textContent = 'My media';
      mineTitle.style.cssText = 'font-size:12px;opacity:.8;margin:2px 0 8px';
      panel.appendChild(mineTitle);
      const tabs = document.createElement('div');
      tabs.style.cssText = 'display:flex;gap:6px;margin-bottom:8px';
      const tabVideo = document.createElement('button');
      const tabAudio = document.createElement('button');
      const tabPics = document.createElement('button');
      [tabVideo, tabAudio, tabPics].forEach((t)=>{ t.className = 'btn secondary'; t.style.padding = '6px 10px'; });
      tabVideo.textContent = 'Video';
      tabAudio.textContent = 'Audio';
      tabPics.textContent = 'Pictures';
      tabs.appendChild(tabVideo); tabs.appendChild(tabAudio); tabs.appendChild(tabPics);
      panel.appendChild(tabs);
      const listHost = document.createElement('div');
      panel.appendChild(listHost);
      const selectAttachment = (a)=>{
        panel.remove();
        backdrop.remove();
        if (!this.activeConnection) return;
        this.queueReusedAttachment({ fileUrl: a.fileUrl, fileName: a.fileName || 'Media', message: null });
        this.refreshActionButton();
      };
      const makeRow = (a)=>{
        const row = document.createElement('button');
        row.className = 'btn secondary';
        row.style.cssText = 'display:flex;align-items:center;gap:10px;width:100%;margin-bottom:6px;text-align:left;padding:8px';
        const thumb = document.createElement('span');
        thumb.style.cssText = 'width:40px;height:40px;border-radius:6px;overflow:hidden;flex-shrink:0;background:rgba(255,255,255,.06);display:flex;align-items:center;justify-content:center';
        const coverUrl = String(a?.coverUrl || a?.cover || '').trim();
        if (coverUrl){
          const img = document.createElement('img');
          img.src = coverUrl;
          img.alt = '';
          img.style.cssText = 'width:100%;height:100%;object-fit:cover';
          thumb.appendChild(img);
        } else {
          thumb.innerHTML = '<i class="fas fa-music" style="font-size:18px;opacity:.8"></i>';
        }
        row.appendChild(thumb);
        const titleSpan = document.createElement('span');
        titleSpan.textContent = String(a?.fileName || a?.title || 'Audio').trim();
        titleSpan.style.flex = '1';
        titleSpan.style.overflow = 'hidden';
        titleSpan.style.textOverflow = 'ellipsis';
        titleSpan.style.whiteSpace = 'nowrap';
        row.appendChild(titleSpan);
        row.onclick = ()=> selectAttachment(a);
        return row;
      };
      const makeTile = (a, type)=>{
        const tile = document.createElement('button');
        tile.className = 'btn secondary';
        tile.style.cssText = 'display:inline-flex;flex-direction:column;align-items:stretch;width:88px;height:88px;padding:4px;margin:0 6px 8px 0;overflow:hidden;border-radius:10px';
        const mediaWrap = document.createElement('div');
        mediaWrap.style.cssText = 'width:100%;flex:1;min-height:0;border-radius:8px;overflow:hidden;background:#0b0f16;display:flex;align-items:center;justify-content:center';
        if (type === 'video'){
          const video = document.createElement('video');
          video.src = a.fileUrl || '';
          video.muted = true;
          video.playsInline = true;
          video.preload = 'metadata';
          video.style.cssText = 'width:100%;height:100%;object-fit:cover';
          mediaWrap.appendChild(video);
        } else if (type === 'pics') {
          const img = document.createElement('img');
          img.src = a.fileUrl || '';
          img.alt = '';
          img.style.cssText = 'width:100%;height:100%;object-fit:cover';
          mediaWrap.appendChild(img);
        } else {
          const icon = document.createElement('span');
          icon.innerHTML = '<i class="fas fa-music" style="font-size:28px;opacity:.7"></i>';
          mediaWrap.appendChild(icon);
        }
        tile.appendChild(mediaWrap);
        tile.onclick = ()=> selectAttachment(a);
        return tile;
      };
      this.loadMyMediaQuickChoices().then((items)=>{
        const rows = (items || []).slice(0, 60);
        const kindOfQuickItem = (a)=>{
          const explicit = String(a?.mediaKind || '').toLowerCase();
          if (explicit === 'image' || explicit === 'video' || explicit === 'audio') return explicit;
          const n = String(a?.fileName || '');
          if (this.isImageFilename(n)) return 'image';
          if (this.isVideoFilename(n)) return 'video';
          if (this.isAudioFilename(n)) return 'audio';
          return 'file';
        };
        const byKind = {
          video: rows.filter((a)=> kindOfQuickItem(a) === 'video'),
          audio: rows.filter((a)=> kindOfQuickItem(a) === 'audio'),
          pics: rows.filter((a)=> kindOfQuickItem(a) === 'image')
        };
        const activate = (kind)=>{
          [tabVideo, tabAudio, tabPics].forEach((b)=>{ b.style.opacity = '.75'; });
          (kind === 'video' ? tabVideo : kind === 'audio' ? tabAudio : tabPics).style.opacity = '1';
          listHost.innerHTML = '';
          const group = byKind[kind] || [];
          if (!group.length){
            const empty = document.createElement('div');
            empty.style.cssText = 'font-size:12px;opacity:.7;padding:4px 0 8px';
            empty.textContent = 'No items';
            listHost.appendChild(empty);
            return;
          }
          if (kind === 'video' || kind === 'pics'){
            const tileWrap = document.createElement('div');
            tileWrap.style.cssText = 'display:flex;flex-wrap:wrap;align-items:flex-start';
            group.forEach((a)=> tileWrap.appendChild(makeTile(a, kind === 'video' ? 'video' : 'pics')));
            listHost.appendChild(tileWrap);
          } else {
            group.forEach((a)=> listHost.appendChild(makeRow(a)));
          }
        };
        tabVideo.onclick = ()=> activate('video');
        tabAudio.onclick = ()=> activate('audio');
        tabPics.onclick = ()=> activate('pics');
        activate('video');
      }).catch(()=>{});
      const recentTitle = document.createElement('div');
      recentTitle.textContent = 'Recent attachments';
      recentTitle.style.cssText = 'font-size:12px;opacity:.8;margin:10px 0 8px';
      panel.appendChild(recentTitle);
      this.getRecentAttachments()
        .filter((a)=> a && !/^voice\.|^video\./i.test(String(a.fileName || '').toLowerCase()))
        .forEach((a)=> panel.appendChild(makeRow(a)));
      const root = document.body;
      if (root){ root.appendChild(backdrop); root.appendChild(panel); }
      backdrop.addEventListener('click', ()=>{ panel.remove(); backdrop.remove(); });
      panel.addEventListener('click', (e)=> e.stopPropagation());
    }

    async loadMyMediaQuickChoices(){
      const out = [];
      try{
        if (!this.db || !this.currentUser?.uid) return out;
        const me = this.currentUser.uid;
        try{
          const qWave = firebase.query(firebase.collection(this.db,'wave'), firebase.where('ownerId','==', me), firebase.limit(20));
          const sWave = await firebase.getDocs(qWave);
          sWave.forEach((d)=>{
            const w = d.data() || {};
            if (w.url) out.push({ fileUrl: w.url, fileName: `${w.title || 'Audio'}.mp3`, coverUrl: w.coverUrl || w.cover || null, sentAt: w.createdAt || new Date().toISOString(), mediaKind: 'audio' });
          });
        }catch(_){ }
        try{
          const qVid = firebase.query(firebase.collection(this.db,'videos'), firebase.where('owner','==', me), firebase.limit(20));
          const sVid = await firebase.getDocs(qVid);
          sVid.forEach((d)=>{
            const v = d.data() || {};
            if (!v.url) return;
            const mediaType = String(v.mediaType || '').toLowerCase();
            const sourceType = String(v.sourceMediaType || '').toLowerCase();
            const inferredKind = mediaType === 'image'
              ? 'image'
              : (sourceType === 'image' ? 'image' : 'video');
            const ext = inferredKind === 'image' ? '.jpg' : '.mp4';
            out.push({
              fileUrl: v.url,
              fileName: `${v.title || (inferredKind === 'image' ? 'Picture' : 'Video')}${ext}`,
              sentAt: v.createdAt || new Date().toISOString(),
              mediaKind: inferredKind
            });
          });
        }catch(_){ }
        try{
          const qPost = firebase.query(firebase.collection(this.db,'posts'), firebase.where('authorId','==', me), firebase.limit(40));
          const sPost = await firebase.getDocs(qPost);
          sPost.forEach((d)=>{
            const p = d.data() || {};
            const media = Array.isArray(p.media) ? p.media : (p.mediaUrl ? [p.mediaUrl] : []);
            media.forEach((entry, idx)=>{
              const isObject = entry && typeof entry === 'object' && !Array.isArray(entry);
              const url = String(isObject ? (entry.url || '') : (entry || '')).trim();
              if (!url) return;
              const kindRaw = String(isObject ? (entry.kind || entry.type || '') : '').toLowerCase();
              const inferredKind = kindRaw === 'image' || kindRaw === 'video' || kindRaw === 'audio'
                ? kindRaw
                : (this.isImageFilename(url) ? 'image' : (this.isVideoFilename(url) ? 'video' : (this.isAudioFilename(url) ? 'audio' : 'file')));
              const extMatch = String(url).match(/\.(png|jpe?g|gif|webp|mp4|webm|mov|mkv|mp3|wav|m4a|aac|ogg)(\?|$)/i);
              const ext = extMatch ? extMatch[1].toLowerCase() : (inferredKind === 'image' ? 'jpg' : (inferredKind === 'video' ? 'mp4' : (inferredKind === 'audio' ? 'mp3' : 'bin')));
              const baseName = String(isObject ? (entry.name || entry.title || '') : '').trim() || String(p.text || 'Media').trim() || 'Media';
              out.push({
                fileUrl: url,
                fileName: `${baseName}_${idx + 1}.${ext}`,
                sentAt: p.createdAt || new Date().toISOString(),
                mediaKind: inferredKind
              });
            });
          });
        }catch(_){ }
      }catch(_){ }
      out.sort((a,b)=> new Date(b.sentAt||0) - new Date(a.sentAt||0));
      return out.filter((a)=> !/^voice\.|^video\./i.test(String(a.fileName || '').toLowerCase()));
    }

    async loadCurrentChatAttachments(limit = 240){
      const out = [];
      try{
        const connId = String(this.activeConnection || '').trim();
        if (!connId || !this.db) return out;
        let snap;
        try{
          const qTs = firebase.query(
            firebase.collection(this.db,'chatMessages',connId,'messages'),
            firebase.orderBy('createdAtTS','desc'),
            firebase.limit(limit)
          );
          snap = await firebase.getDocs(qTs);
        }catch(_){
          try{
            const qIso = firebase.query(
              firebase.collection(this.db,'chatMessages',connId,'messages'),
              firebase.orderBy('createdAt','desc'),
              firebase.limit(limit)
            );
            snap = await firebase.getDocs(qIso);
          }catch(_){
            const qLoose = firebase.query(
              firebase.collection(this.db,'chatMessages',connId,'messages'),
              firebase.limit(limit)
            );
            snap = await firebase.getDocs(qLoose);
          }
        }
        (snap?.docs || []).forEach((d)=>{
          const m = d.data() || {};
          if (!m.fileUrl) return;
          const inferred = this.inferAttachmentFileName(m, m.text || m.previewText || '');
          const ts = Number(m?.createdAtTS?.toMillis?.() || 0) || Number(new Date(m?.createdAt || 0).getTime() || 0) || 0;
          out.push({
            id: d.id,
            fileUrl: m.fileUrl,
            fileName: inferred || m.fileName || 'Attachment',
            sender: m.sender || '',
            createdAt: m.createdAt || '',
            createdAtTS: m.createdAtTS || null,
            attachmentKeySalt: m.attachmentKeySalt || '',
            message: { ...m, id: d.id }
          });
        });
      }catch(_){ }
      out.sort((a,b)=>{
        const ta = Number(a?.createdAtTS?.toMillis?.() || 0) || Number(new Date(a?.createdAt || 0).getTime() || 0) || 0;
        const tb = Number(b?.createdAtTS?.toMillis?.() || 0) || Number(new Date(b?.createdAt || 0).getTime() || 0) || 0;
        return tb - ta;
      });
      return out;
    }

    scrollMessageIntoViewSafely(messageEl, opts = {}){
      try{
        if (!messageEl || !messageEl.isConnected) return false;
        const smooth = opts.smooth !== false;
        try{
          messageEl.scrollIntoView({ behavior: smooth ? 'smooth' : 'auto', block: 'end', inline: 'nearest' });
        }catch(_){
          messageEl.scrollIntoView(false);
        }
        const prevShadow = messageEl.style.boxShadow;
        const prevTransition = messageEl.style.transition;
        messageEl.style.transition = `${prevTransition ? `${prevTransition}, ` : ''}box-shadow .2s ease`;
        messageEl.style.boxShadow = '0 0 0 2px rgba(104, 180, 255, .88)';
        if (messageEl._jumpFlashTimer) clearTimeout(messageEl._jumpFlashTimer);
        messageEl._jumpFlashTimer = setTimeout(()=>{
          try{
            messageEl.style.boxShadow = prevShadow || '';
            messageEl.style.transition = prevTransition || '';
          }catch(_){ }
        }, 1400);
        return true;
      }catch(_){ return false; }
    }

    async jumpToMessageById(messageId, opts = {}){
      try{
        const id = String(messageId || '').trim();
        if (!id) return false;
        const connId = String(this.activeConnection || '').trim();
        const box = document.getElementById('messages');
        if (!connId || !box) return false;
        const queryId = id.replace(/"/g, '\\"');
        let target = box.querySelector(`[data-msg-id="${queryId}"]`);
        if (!target){
          await this.loadMessages().catch(()=>{});
          await this.yieldToUi();
          target = box.querySelector(`[data-msg-id="${queryId}"]`);
        }
        if (!target) return false;
        return this.scrollMessageIntoViewSafely(target, { smooth: opts.smooth !== false });
      }catch(_){ return false; }
    }

    openCurrentChatAttachmentsSheet(){
      const existing = document.getElementById('chat-attachments-sheet');
      const existingBackdrop = document.getElementById('chat-attachments-backdrop');
      if (existing){ existing.remove(); if (existingBackdrop) existingBackdrop.remove(); return; }
      if (!this.activeConnection){ alert('Open a chat first.'); return; }

      const backdrop = document.createElement('div');
      backdrop.id = 'chat-attachments-backdrop';
      backdrop.style.cssText = 'position:fixed;inset:0;z-index:104;background:rgba(0,0,0,.28)';

      const panel = document.createElement('div');
      panel.id = 'chat-attachments-sheet';
      panel.style.cssText = this.isMobileViewport()
        ? 'position:fixed;left:10px;right:10px;bottom:calc(90px + env(safe-area-inset-bottom));max-height:min(70vh,620px);overflow:auto;background:#10141c;border:1px solid #2a2f36;border-radius:12px;z-index:105;padding:10px'
        : 'position:fixed;left:50%;transform:translateX(-50%);top:76px;width:min(920px,calc(100vw - 30px));max-height:min(76vh,760px);overflow:auto;background:#10141c;border:1px solid #2a2f36;border-radius:12px;z-index:105;padding:10px';

      panel.innerHTML = '<div style="display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:8px"><div style="font-weight:600">Chat attachments</div><button id="chat-attachments-close" class="icon-btn" title="Close"><i class="fas fa-xmark"></i></button></div>';

      const tabs = document.createElement('div');
      tabs.style.cssText = 'display:flex;gap:6px;flex-wrap:wrap;margin-bottom:8px';
      const tabAll = document.createElement('button'); tabAll.className = 'btn secondary'; tabAll.textContent = 'All'; tabAll.dataset.filter = 'all';
      const tabPics = document.createElement('button'); tabPics.className = 'btn secondary'; tabPics.textContent = 'Pictures'; tabPics.dataset.filter = 'pics';
      const tabVideo = document.createElement('button'); tabVideo.className = 'btn secondary'; tabVideo.textContent = 'Video'; tabVideo.dataset.filter = 'video';
      const tabAudio = document.createElement('button'); tabAudio.className = 'btn secondary'; tabAudio.textContent = 'Audio'; tabAudio.dataset.filter = 'audio';
      const tabFiles = document.createElement('button'); tabFiles.className = 'btn secondary'; tabFiles.textContent = 'Files'; tabFiles.dataset.filter = 'files';
      [tabAll, tabPics, tabVideo, tabAudio, tabFiles].forEach((b)=> tabs.appendChild(b));
      panel.appendChild(tabs);

      const grid = document.createElement('div');
      grid.style.cssText = 'display:grid;grid-template-columns:repeat(auto-fill,minmax(230px,1fr));gap:10px';
      panel.appendChild(grid);

      const categoryOf = (name)=>{
        if (this.isImageFilename(name)) return 'pics';
        if (this.isVideoFilename(name)) return 'video';
        if (this.isAudioFilename(name)) return 'audio';
        return 'files';
      };

      const markActive = (kind)=>{
        [tabAll, tabPics, tabVideo, tabAudio, tabFiles].forEach((b)=>{ b.style.opacity = '.75'; });
        ({ all: tabAll, pics: tabPics, video: tabVideo, audio: tabAudio, files: tabFiles }[kind]).style.opacity = '1';
      };

      const connId = this.activeConnection;
      const keyCache = new Map();
      const getConnKey = async ()=>{
        if (keyCache.has(connId)) return keyCache.get(connId);
        let key = null;
        try{ key = await this.getFallbackKeyForConn(connId); }catch(_){ key = null; }
        keyCache.set(connId, key);
        return key;
      };

      const typeOrder = { pics: 0, video: 1, audio: 2, files: 3 };
      const renderList = async (rows, kind = 'all')=>{
        grid.innerHTML = '';
        markActive(kind);
        let filtered = rows.filter((r)=> kind === 'all' ? true : categoryOf(String(r.fileName || '')) === kind);
        filtered = filtered.slice().sort((a, b)=>{
          const catA = categoryOf(String(a.fileName || ''));
          const catB = categoryOf(String(b.fileName || ''));
          const ia = typeOrder[catA] ?? 4;
          const ib = typeOrder[catB] ?? 4;
          if (ia !== ib) return ia - ib;
          const ta = Number(a?.createdAtTS?.toMillis?.() || 0) || Number(new Date(a?.createdAt || 0).getTime() || 0) || 0;
          const tb = Number(b?.createdAtTS?.toMillis?.() || 0) || Number(new Date(b?.createdAt || 0).getTime() || 0) || 0;
          return tb - ta;
        });
        if (!filtered.length){
          const empty = document.createElement('div');
          empty.style.cssText = 'opacity:.75;padding:8px;grid-column:1/-1';
          empty.textContent = kind === 'all' ? 'No attachments in this chat.' : `No ${kind} in this chat.`;
          grid.appendChild(empty);
          return;
        }
        for (const a of filtered){
          const card = document.createElement('div');
          card.style.cssText = 'background:#0f141d;border:1px solid #2a2f36;border-radius:10px;overflow:hidden;padding:8px';
          const preview = document.createElement('div');
          preview.className = 'file-preview chat-attachment-preview';
          preview.style.cssText = 'min-height:100px';
          const addBtn = document.createElement('button');
          addBtn.className = 'btn secondary';
          addBtn.textContent = 'Add';
          addBtn.style.cssText = 'margin-top:8px;width:100%';
          addBtn.onclick = (e)=>{ e.stopPropagation(); this.queueReusedAttachment({ fileUrl: a.fileUrl, fileName: a.fileName, message: a.message, sender: a.sender }); try{ panel.remove(); backdrop.remove(); }catch(_){ } };
          const senderName = this.usernameCache.get(a.sender) || String(a.sender || '').slice(0,8) || 'Unknown';
          preview.dataset.pickerMode = '1';
          card.appendChild(preview);
          card.appendChild(addBtn);
          grid.appendChild(card);
          try{
            const aesKey = await getConnKey();
            await this.renderEncryptedAttachment(preview, a.fileUrl, a.fileName, aesKey, connId, senderName, a.message);
          }catch(_){
            this.renderDirectAttachment(preview, a.fileUrl, a.fileName, a.message, senderName, true);
          }
        }
      };

      const loading = document.createElement('div');
      loading.style.cssText = 'opacity:.8;padding:8px';
      loading.textContent = 'Loading attachments...';
      grid.appendChild(loading);

      let currentRows = [];
      tabs.addEventListener('click', (e)=>{
        const btn = e.target?.closest?.('button');
        if (!btn || !btn.dataset.filter) return;
        renderList(currentRows, btn.dataset.filter);
      });
      this.loadCurrentChatAttachments(320).then(async (rows)=>{
        currentRows = rows;
        await renderList(rows, 'all');
      }).catch(()=>{
        grid.innerHTML = '<div style="opacity:.75;padding:8px">Failed to load attachments.</div>';
      });

      const host = document.body;
      if (host){ host.appendChild(backdrop); host.appendChild(panel); }
      const closeBtn = panel.querySelector('#chat-attachments-close');
      if (closeBtn) closeBtn.addEventListener('click', ()=>{ panel.remove(); backdrop.remove(); });
      backdrop.addEventListener('click', ()=>{ panel.remove(); backdrop.remove(); });
      panel.addEventListener('click', (e)=> e.stopPropagation());
    }

    getConnParticipants(data){
      const parts = Array.isArray(data?.participants)
        ? data.participants
        : (Array.isArray(data?.users) ? data.users : (Array.isArray(data?.memberIds) ? data.memberIds : []));
      if (parts.length) return parts.filter(Boolean);
      // Participants as map {uid: true}
      if (data?.participants && typeof data.participants === 'object' && !Array.isArray(data.participants)) {
        const fromMap = Object.keys(data.participants).filter(Boolean);
        if (fromMap.length) return fromMap;
      }
      if (typeof data?.key === 'string' && data.key.includes('|')) return data.key.split('|').filter(Boolean);
      return [];
    }

    async findConnectionByKey(key){
      try{
        const q = firebase.query(firebase.collection(this.db,'chatConnections'), firebase.where('key','==', key));
        const s = await firebase.getDocs(q);
        const rows = [];
        s.forEach(d=> rows.push({ id:d.id, ...d.data() }));
        if (!rows.length) return null;
        const withMsgTs = [];
        for (const r of rows){
          let ts = 0;
          try{
            let qMsg;
            try{
              qMsg = firebase.query(
                firebase.collection(this.db,'chatMessages',r.id,'messages'),
                firebase.orderBy('createdAtTS','desc'),
                firebase.limit(1)
              );
            }catch(_){
              qMsg = firebase.query(
                firebase.collection(this.db,'chatMessages',r.id,'messages'),
                firebase.orderBy('createdAt','desc'),
                firebase.limit(1)
              );
            }
            const sm = await firebase.getDocs(qMsg);
            const d = sm.docs && sm.docs[0] ? sm.docs[0].data() : null;
            ts = (d?.createdAtTS?.toMillis?.() || new Date(d?.createdAt || 0).getTime() || 0);
          }catch(_){ ts = 0; }
          withMsgTs.push({ row: r, msgTs: ts });
        }
        withMsgTs.sort((a,b)=>{
          const am = a.msgTs || 0; const bm = b.msgTs || 0;
          if (am !== bm) return bm - am;
          const aa = !!a.row.archived; const bb = !!b.row.archived;
          if (aa !== bb) return aa ? 1 : -1;
          return new Date(b.row.updatedAt||0) - new Date(a.row.updatedAt||0);
        });
        return withMsgTs[0]?.row?.id || rows[0]?.id || null;
      }catch(_){ return null; }
    }

    async resolveCanonicalConnectionId(connId){
      try{
        if (!connId) return connId;
        let key = '';
        // If deep link passes a key directly, use it as-is.
        if (String(connId).includes('|')) key = String(connId);
        if (!key){
          const snap = await firebase.getDoc(firebase.doc(this.db,'chatConnections', connId));
          if (snap.exists()){
            const data = snap.data() || {};
            key = String(data.key || this.computeConnKey(this.getConnParticipants(data)) || '');
          }
        }
        if (!key) return connId;
        const canonical = await this.findConnectionByKey(key);
        return canonical || connId;
      }catch(_){
        return connId;
      }
    }

    async getIceServers(){
      try{
        // 1) Try static TURN from keys
        let regionPref = 'europe-west1';
        if (window.secureKeyManager && typeof window.secureKeyManager.getKeys === 'function'){
          const keys = await window.secureKeyManager.getKeys();
          regionPref = (keys && keys.firebase && (keys.firebase.functionsRegion || keys.firebase.region)) || regionPref;
          const turn = keys && keys.turn;
          if (turn && Array.isArray(turn.uris) && turn.username && turn.credential){
            return [
              { urls: ['stun:stun.l.google.com:19302','stun:global.stun.twilio.com:3478'] },
              { urls: turn.uris, username: turn.username, credential: turn.credential }
            ];
          }
        }
        // 2) Else fetch ephemeral TURN via Cloud Function
        if (window.firebaseService && window.firebaseService.auth && window.firebaseService.auth.currentUser){
          const idToken = await window.firebaseService.auth.currentUser.getIdToken(true);
          // Prefer explicit run.app URL if provided in keys or known
          let runAppUrl = null;
          try{ const keys = await window.secureKeyManager.getKeys(); runAppUrl = keys && (keys.turnFunctionUrl || keys.turn?.functionUrl) || null; }catch(_){ runAppUrl = null; }
          const knownRunHost = 'https://getturnconfig-hkhtxasofa-ew.a.run.app';
          const regions = [regionPref, 'europe-west1', 'us-central1'];
          const candidates = [];
          if (runAppUrl) candidates.push(runAppUrl);
          candidates.push(knownRunHost);
          regions.forEach(r=> candidates.push(`https://${r}-liber-apps-cca20.cloudfunctions.net/getTurnConfig`));
          for (const url of candidates){
            try{
              const resp = await fetch(url, { headers: { 'Authorization': `Bearer ${idToken}` }});
              if (resp.ok){
                const json = await resp.json();
                if (Array.isArray(json.iceServers) && json.iceServers.length){
                  // Prefer TCP/TLS relays first for restrictive networks
                  const expanded = [];
                  const normalizeTcp = (u)=>{
                    if (typeof u !== 'string') return u;
                    if (!u.startsWith('turn')) return u;
                    // Replace any existing transport parameter with tcp
                    const hasQ = u.includes('?');
                    const base = hasQ ? u.replace(/([?&])transport=(udp|tcp)/i, '$1transport=tcp') : u + '?transport=tcp';
                    // Ensure only one transport param exists
                    const parts = base.split('?');
                    if (parts.length>1){
                      const q = parts[1]
                        .split('&')
                        .filter(kv => !/^transport=(udp|tcp)$/i.test(kv))
                        .concat(['transport=tcp'])
                        .join('&');
                      return parts[0] + '?' + q;
                    }
                    return base;
                  };
                  json.iceServers.forEach(s => {
                    const urls = Array.isArray(s.urls) ? s.urls : (s.urls ? [s.urls] : []);
                    const tcpUrls = urls.map(normalizeTcp);
                    const dedup = Array.from(new Set([ ...urls, ...tcpUrls ]));
                    expanded.push({ ...s, urls: dedup });
                  });
                  return expanded;
                }
              }
            }catch(_){ /* try next region */ }
          }
        }
      }catch(_){ /* ignore */ }
      return [ { urls: ['stun:stun.l.google.com:19302','stun:global.stun.twilio.com:3478'] } ];
    }

    async init() {
      // Wait for firebase (parent's when in iframe, or our own)
      let attempts = 0; while((!window.firebaseService || !window.firebaseService.isInitialized) && attempts < 150){ await new Promise(r=>setTimeout(r,100)); attempts++; }
      if (!window.firebaseService || !window.firebaseService.isInitialized) return;
      this.db = window.firebaseService.db;
      if (!this.db) {
        console.error('Firestore not initialized (db is null)');
        return;
      }
      this.storage = window.firebaseService.app ? firebase.getStorage(window.firebaseService.app) : null;
      
      // Enhanced auth readiness with token refresh
      attempts = 0;
      while (attempts < 50) {
        try {
          await window.firebaseService.auth.currentUser?.getIdToken(true); // Force refresh token
          this.currentUser = await window.firebaseService.getCurrentUser();
          if (this.currentUser) break;
        } catch (err) {
          console.warn('Auth retry attempt', attempts, err.message);
        }
        await new Promise(r => setTimeout(r, 100));
        attempts++;
      }
      if (!this.currentUser) {
        console.error('Firebase auth not ready after retries');
        return;
      }

      try { this.me = await window.firebaseService.getUserData(this.currentUser.uid); } catch { this.me = null; }
      this.bindUI();
      this.setupFullscreenImagePreview();
      // If a connId is provided via query param, set active after connections load
      try{
        const params = new URLSearchParams(location.search);
        this._deepLinkConnId = params.get('connId') || null;
      }catch(_){ this._deepLinkConnId = null; }
      await this.loadConnections();
      // If deep link provided, activate it after list is ready (supports key or doc id)
      if (this._deepLinkConnId){
        try{
          let id = this._deepLinkConnId;
          // If this appears to be a participant key (contains a pipe), resolve to existing doc id
          if (id.includes('|')){
            const existing = await this.findConnectionByKey(id);
            if (existing) id = existing; else {
              // If not found, fall back to using the key directly if a doc with same id exists
              try{ const test = await firebase.getDoc(firebase.doc(this.db,'chatConnections', id)); if (!test.exists()) id = null; }catch(_){ id = null; }
            }
          }
          if (id){
            await this.setActive(id);
          }
        }catch(_){ /* ignore deep link issues */ }
      } else {
        // Restore last opened chat when reopening chat app.
        try{
          const last = localStorage.getItem('liber_last_chat_conn');
          if (last) await this.setActive(last);
        }catch(_){ }
      }
      if (!this.activeConnection){
        const firstConn = (this.connections && this.connections[0] && this.connections[0].id) || '';
        if (firstConn){
          try{ await this.setActive(firstConn); }catch(_){ }
        }
      }

      // Ensure self is cached
      this.usernameCache.set(this.currentUser.uid, { username: this.me?.username || 'You', avatarUrl: this.me?.avatarUrl || '../../images/default-bird.png' });
      this._avatarCache.set(this.currentUser.uid, this.me?.avatarUrl || '../../images/default-bird.png');
      this.startPendingRequestListener();
    }

    startPendingRequestListener(){
      try{
        if (this._pendingRequestUnsub) this._pendingRequestUnsub();
        if (!this.db || !this.currentUser?.uid) return;
        const peersRef = firebase.collection(this.db, 'connections', this.currentUser.uid, 'peers');
        firebase.getDocs(peersRef).then((snap)=>{
          let count = 0;
          snap.forEach(d=>{
            const data = d.data() || {};
            if (data.status === 'pending' && String(data.requestedTo || '') === this.currentUser?.uid) count++;
          });
          this._pendingRequestCount = count;
          this.updatePendingRequestBadge();
        }).catch(()=>{});
        this._pendingRequestUnsub = firebase.onSnapshot(peersRef, (snap)=>{
          let count = 0;
          snap.forEach(d=>{
            const data = d.data() || {};
            if (data.status === 'pending' && String(data.requestedTo || '') === this.currentUser?.uid) count++;
          });
          this._pendingRequestCount = count;
          this.updatePendingRequestBadge();
        }, ()=>{});
      }catch(_){ }
    }

    updatePendingRequestBadge(){
      const n = this._pendingRequestCount || 0;
      const backBadge = document.getElementById('back-request-badge');
      const connBadge = document.getElementById('connections-request-badge');
      if (backBadge){
        backBadge.textContent = n > 99 ? '99+' : String(n);
        backBadge.classList.toggle('hidden', n === 0);
      }
      if (connBadge){
        connBadge.textContent = n > 99 ? '99+' : String(n);
        connBadge.classList.toggle('hidden', n === 0);
      }
    }

    bindUI(){
      const backBtn = document.getElementById('back-btn');
      if (backBtn){
        backBtn.addEventListener('click', (e)=>{
          try{
            const inShell = new URLSearchParams(location.search).get('inShell') === '1' || window.self !== window.top;
            if (!inShell) return;
            e.preventDefault();
            if (window.parent && window.parent !== window){
              window.parent.postMessage({ type: 'liber:close-app-shell' }, '*');
              if (window.parent.appsManager && typeof window.parent.appsManager.closeAppShell === 'function'){
                window.parent.appsManager.closeAppShell();
              }
            }
          }catch(_){ /* keep default href fallback */ }
        });
      }
      document.getElementById('new-connection-btn').addEventListener('click', ()=> { this.groupBaseParticipants = null; this.promptNewConnection(); });
      const mobileGroupBtn = document.getElementById('mobile-new-connection-btn');
      if (mobileGroupBtn){
        mobileGroupBtn.addEventListener('click', ()=> { this.groupBaseParticipants = null; this.promptNewConnection(); });
      }
      const actionBtn = document.getElementById('action-btn');
      if (actionBtn){
        actionBtn.addEventListener('click', ()=> this.handleActionButton());
        actionBtn.addEventListener('mousedown', (e)=> this.handleActionPressStart(e));
        actionBtn.addEventListener('touchstart', (e)=> this.handleActionPressStart(e));
        ['mouseup','touchend','touchcancel'].forEach(evt=> actionBtn.addEventListener(evt, (e)=> this.handleActionPressEnd(e)));
      }
      if (!this._globalRecReleaseBound){
        this._globalRecReleaseBound = true;
        window.addEventListener('mouseup', (e)=> this.handleActionPressEnd(e), true);
        window.addEventListener('touchend', (e)=> this.handleActionPressEnd(e), true);
        window.addEventListener('touchcancel', (e)=> this.handleActionPressEnd(e), true);
      }
      document.getElementById('attach-btn').addEventListener('click', ()=>{
        this.showAttachmentQuickActions();
      });
      /* Add from WaveConnect removed from composer - available only via attach popup or share button */
      document.getElementById('file-input').addEventListener('change', (e)=>{
        this.queueAttachments(e.target.files);
        try{ e.target.value = ''; }catch(_){ }
      });
      const stickerBtn = document.getElementById('sticker-btn');
      if (stickerBtn){ stickerBtn.addEventListener('click', ()=> this.toggleStickers()); }
      document.getElementById('user-search').addEventListener('input', (e)=> this.searchUsers(e.target.value.trim()));
      // Predictive suggestions: update datalist as user types
      const suggest = document.getElementById('user-suggestions');
      const searchEl = document.getElementById('user-search');
      if (suggest && searchEl){
        let lastTerm = '';
        searchEl.addEventListener('input', async ()=>{
          const term = (searchEl.value||'').trim();
          if (term === lastTerm || term.length === 0){ if (term.length===0) suggest.innerHTML=''; lastTerm = term; return; }
          lastTerm = term;
          try{
            const users = await window.firebaseService.searchUsers(term.toLowerCase());
            // simple dedupe and top-10 ranking by prefix/contains
            const rank = (u)=>{
              const n=(u.username||'').toLowerCase(); const em=(u.email||'').toLowerCase(); const t=term.toLowerCase();
              let s=0; if (n.startsWith(t)||em.startsWith(t)) s+=3; if (n.includes(t)||em.includes(t)) s+=2; return s;
            };
            const opts = (users||[])
              .sort((a,b)=> rank(b)-rank(a))
              .slice(0,10)
              .map(u=> `<option value="${u.username||u.email}"></option>`)
              .join('');
            suggest.innerHTML = opts;
          }catch(_){ /* ignore */ }
        });
      }
      const userSearch = document.getElementById('user-search');
      if (userSearch){
        const openSidebar = ()=> this.setMobileMenuOpen(true);
        userSearch.addEventListener('focus', openSidebar);
        userSearch.addEventListener('click', openSidebar);
        // iOS Safari sometimes needs a short delay to compute layout; force open on input with rAF
        userSearch.addEventListener('input', ()=>{ requestAnimationFrame(()=> openSidebar()); });
        userSearch.addEventListener('keydown', (e)=>{ if(e.key==='Escape'){ this.setMobileMenuOpen(false); }});
        // Prevent header container from swallowing the tap
        const header = document.getElementById('sidebar-header');
        if (header){
          header.style.pointerEvents = 'auto';
          userSearch.style.pointerEvents = 'auto';
        }
      }

      // Register service worker once (best-effort)
      if ('serviceWorker' in navigator){
        navigator.serviceWorker.register('/sw.js').catch(()=>{});
      }
      // Call and recording buttons (placeholders)
      const voiceBtn = document.getElementById('voice-call-btn'); if (voiceBtn) voiceBtn.addEventListener('click', ()=> this.enterRoom(false));
      const videoBtn = document.getElementById('video-call-btn'); if (videoBtn) videoBtn.addEventListener('click', ()=> this.enterRoom(true));
      const attachSheetBtn = document.getElementById('chat-attachments-btn'); if (attachSheetBtn) attachSheetBtn.addEventListener('click', ()=> this.openCurrentChatAttachmentsSheet());
      const groupBtn = document.getElementById('group-menu-btn'); if (groupBtn) groupBtn.addEventListener('click', ()=>{ if (this._isPersonalChat) return; this.toggleGroupPanel(); });
      const mobileVoiceBtn = document.getElementById('mobile-voice-call-btn'); if (mobileVoiceBtn) mobileVoiceBtn.addEventListener('click', ()=> this.enterRoom(false));
      const mobileVideoBtn = document.getElementById('mobile-video-call-btn'); if (mobileVideoBtn) mobileVideoBtn.addEventListener('click', ()=> this.enterRoom(true));
      const mobileAttachSheetBtn = document.getElementById('mobile-chat-attachments-btn'); if (mobileAttachSheetBtn) mobileAttachSheetBtn.addEventListener('click', ()=> this.openCurrentChatAttachmentsSheet());
      const mobileGroupMenuBtn = document.getElementById('mobile-group-menu-btn'); if (mobileGroupMenuBtn) mobileGroupMenuBtn.addEventListener('click', ()=>{ if (this._isPersonalChat) return; this.toggleGroupPanel(); });
      const recAudioBtn = document.getElementById('record-audio-btn'); if (recAudioBtn) recAudioBtn.addEventListener('click', ()=> this.recordVoiceMessage());
      const recVideoBtn = document.getElementById('record-video-btn'); if (recVideoBtn) recVideoBtn.addEventListener('click', ()=> this.recordVideoMessage());
      // Drag & Drop upload within chat app area
      const appEl = document.getElementById('chat-app');
      if (appEl){
        ['dragenter','dragover'].forEach(evt=> appEl.addEventListener(evt, (e)=>{
          e.preventDefault();
          if (e.dataTransfer) e.dataTransfer.dropEffect = 'copy';
        }));
        appEl.addEventListener('drop', (e)=>{
          e.preventDefault();
          const dt = e.dataTransfer;
          if (!dt) return;
          const files = dt.files && dt.files.length ? dt.files : null;
          if (files && files.length) this.queueAttachments(files);
        });
      }
      // Paste-to-upload on message input (supports images/files from clipboard)
      const msgInput = document.getElementById('message-input');
      if (msgInput){
        msgInput.addEventListener('paste', async (e)=>{
          const cd = e.clipboardData;
          if (!cd) return;
          const files = [];
          if (cd.files && cd.files.length){
            for (let i=0;i<cd.files.length;i++) files.push(cd.files[i]);
          } else if (cd.items && cd.items.length){
            for (let i=0;i<cd.items.length;i++){
              const it = cd.items[i];
              if (it && it.kind === 'file'){
                const f = it.getAsFile();
                if (f) files.push(f);
              }
            }
          }
          if (files.length){
            e.preventDefault();
            this.queueAttachments(files);
          }
        });
      }
      const sidebarHeader = document.getElementById('sidebar-header');
      if (sidebarHeader){
        sidebarHeader.addEventListener('click', (ev)=>{
          // Avoid toggling when interacting with controls inside header
          const tgt = ev.target;
          if (tgt && (tgt.tagName === 'A' || tgt.tagName === 'BUTTON' || tgt.closest('button') || tgt.closest('a'))) return;
          const sidebar = document.querySelector('.sidebar');
          if (!sidebar) return;
          this.setMobileMenuOpen(!sidebar.classList.contains('open'));
        });
      }
      const mobileTip = document.getElementById('mobile-sidebar-tip');
      if (mobileTip){
        mobileTip.addEventListener('click', ()=>{
          const sidebar = document.querySelector('.sidebar');
          this.setMobileMenuOpen(!(sidebar && sidebar.classList.contains('open')));
        });
      }
      // Enter to send, Shift+Enter for newline (desktop & mobile)
      const msgInput2 = document.getElementById('message-input');
      if (msgInput2){
        msgInput2.addEventListener('input', ()=>{
          this.refreshActionButton();
          this.syncTypingFromInput();
        });
        msgInput2.addEventListener('focus', ()=> this.syncTypingFromInput());
        msgInput2.addEventListener('blur', ()=> this.publishTypingState(false, { force: true }));
        msgInput2.addEventListener('keydown', (e)=>{
          if (e.key === 'Enter'){
            if (e.shiftKey){
              // allow newline
              return;
            }
            e.preventDefault();
            this.sendCurrent();
          }
        });
      }
      document.addEventListener('keydown', (e)=>{
        if (e.key === 'Escape'){
          this.setMobileMenuOpen(false);
        }
      });
      if (this.isMobileViewport()) this.setMobileMenuOpen(false);
      this.bindVoiceTopStrip();
      this.bindChatTitleProfileOpen();
      this.renderComposerAttachmentQueue();
    }

    bindVoiceTopStrip(){
      const strip = document.getElementById('voice-top-strip');
      const toggle = document.getElementById('voice-top-toggle');
      const close = document.getElementById('voice-top-close');
      if (!strip || !toggle || !close) return;
      toggle.addEventListener('click', ()=>{
        const p = this.ensureChatBgPlayer();
        const playerSrc = this.getChatPlayerSrc(p);
        const m = this._topMediaEl;
        if (playerSrc){
          if (p.paused){ this._voiceUserIntendedPlay = true; p.play().catch(()=>{}); }
          else{ this._voiceUserIntendedPlay = false; p.pause(); }
        } else if (m && m.isConnected){
          if (m.paused){ this._voiceUserIntendedPlay = true; m.play().catch(()=>{}); }
          else{ this._voiceUserIntendedPlay = false; m.pause(); }
        }
        this.updateVoiceWidgets();
      });
      close.addEventListener('click', (e)=>{
        try{ e.preventDefault(); e.stopPropagation(); }catch(_){ }
        const m = this._topMediaEl;
        const p = this.ensureChatBgPlayer();
        this._voiceUserIntendedPlay = false;
        this._forceHideVoiceStripUntil = Date.now() + 260;
        this.pauseOtherInlineMedia(null);
        if (m && m.isConnected){
          try{ m.pause(); }catch(_){ }
          try{ m.currentTime = 0; }catch(_){ }
          this._topMediaEl = null;
          try{ p.pause(); }catch(_){ }
          try{ p.removeAttribute('src'); }catch(_){ }
          p.src = '';
          try{ p.load(); }catch(_){ }
          this._voiceCurrentSrc = '';
          this._voiceCurrentAttachmentKey = '';
          this._voiceCurrentTitle = 'Voice message';
          strip.classList.add('hidden');
          this.updateVoiceWidgets();
          return;
        }
        try{ p.pause(); }catch(_){ }
        try{ p.removeAttribute('src'); }catch(_){ }
        p.src = '';
        try{ p.load(); }catch(_){ }
        this._voiceCurrentSrc = '';
        this._voiceCurrentAttachmentKey = '';
        this._voiceCurrentTitle = 'Voice message';
        this._topMediaEl = null;
        this.stopVoiceProgressLoop();
        strip.classList.add('hidden');
        this.updateVoiceWidgets();
      });
    }

    bindChatTitleProfileOpen(){
      const click = (e)=>{
        const el = e.currentTarget;
        const uid = String(el?.dataset?.userPreview || '').trim();
        if (!uid) return;
        try{ e.preventDefault(); e.stopPropagation(); }catch(_){ }
        this.openUserPreviewFromChat(uid);
      };
      const ids = ['active-connection-name', 'chat-top-title'];
      ids.forEach((id)=>{
        const el = document.getElementById(id);
        if (!el || el._userPreviewBound) return;
        el._userPreviewBound = true;
        el.addEventListener('click', click);
      });
    }

    openUserPreviewFromChat(uid){
      const id = String(uid || '').trim();
      if (!id) return;
      const hosts = [window.top, window.parent, window];
      for (const host of hosts){
        try{
          if (host?.dashboardManager && typeof host.dashboardManager.showUserPreviewModal === 'function'){
            host.dashboardManager.showUserPreviewModal(id);
            return;
          }
        }catch(_){ }
      }
    }

    getChatPlayerSrc(p){
      try{
        return String(p?.currentSrc || p?.src || p?.getAttribute?.('src') || '').trim();
      }catch(_){
        return '';
      }
    }

    normalizeMediaSrc(src){
      try{
        const raw = String(src || '').trim();
        if (!raw) return '';
        if (/^blob:/i.test(raw)) return raw;
        const u = new URL(raw, window.location.href);
        return `${u.origin}${u.pathname}`;
      }catch(_){
        return String(src || '').trim();
      }
    }

    isSameMediaSrc(a, b){
      if (!a || !b) return false;
      if (a === b) return true;
      const na = this.normalizeMediaSrc(a);
      const nb = this.normalizeMediaSrc(b);
      if (!na || !nb) return false;
      if (na === nb) return true;
      if (/^blob:/i.test(na) && /^blob:/i.test(nb)) return na === nb;
      if (/^blob:/i.test(na) || /^blob:/i.test(nb)) return false;
      return na.endsWith(nb) || nb.endsWith(na);
    }

    getStableBlobUrl(cacheKey, blob){
      try{
        const key = String(cacheKey || '').trim();
        if (!key || !blob) return URL.createObjectURL(blob);
        if (this._attachmentBlobUrlByKey.has(key)) return this._attachmentBlobUrlByKey.get(key);
        const nextUrl = URL.createObjectURL(blob);
        this._attachmentBlobUrlByKey.set(key, nextUrl);
        while (this._attachmentBlobUrlByKey.size > 600){
          const firstKey = this._attachmentBlobUrlByKey.keys().next().value;
          const firstUrl = this._attachmentBlobUrlByKey.get(firstKey);
          this._attachmentBlobUrlByKey.delete(firstKey);
          try{ URL.revokeObjectURL(firstUrl); }catch(_){ }
        }
        return nextUrl;
      }catch(_){
        return URL.createObjectURL(blob);
      }
    }

    async fixDuplicateConnections(){
      try{
        const byId = new Map();
        const fields = ['participants', 'users', 'memberIds'];
        for (const field of fields){
          try{
            const q = firebase.query(
              firebase.collection(this.db,'chatConnections'),
              firebase.where(field,'array-contains', this.currentUser.uid)
            );
            const s = await firebase.getDocs(q);
            s.forEach(d=> byId.set(d.id, { id:d.id, ...d.data() }));
          }catch(_){ }
        }
        // Include key-only docs that may not have participant arrays.
        try{
          const qAll = firebase.query(firebase.collection(this.db,'chatConnections'), firebase.orderBy('updatedAt','desc'), firebase.limit(400));
          const sAll = await firebase.getDocs(qAll);
          sAll.forEach(d=> byId.set(d.id, { id:d.id, ...d.data() }));
        }catch(_){
          try{
            const sAll2 = await firebase.getDocs(firebase.collection(this.db,'chatConnections'));
            sAll2.forEach(d=> byId.set(d.id, { id:d.id, ...d.data() }));
          }catch(__){ }
        }
        const all = Array.from(byId.values())
          .map(c=> ({ ...c, participants: this.getConnParticipants(c) }))
          .filter(c=> Array.isArray(c.participants) && c.participants.includes(this.currentUser.uid));
        // Group by stable key of participants
        const groups = new Map();
        for (const c of all){
          const key = c.key || this.computeConnKey(c.participants||[]);
          if (!groups.has(key)) groups.set(key, []);
          groups.get(key).push(c);
        }
        let archived = 0;
        for (const [key, conns] of groups.entries()){
          if (conns.length <= 1) continue;
          // Keep the newest; mark others archived → no message copying to respect rules
          conns.sort((a,b)=> new Date(b.updatedAt||0) - new Date(a.updatedAt||0));
          const keep = conns[0];
          const rest = conns.slice(1);
          for (const r of rest){
            try{
              await firebase.updateDoc(firebase.doc(this.db,'chatConnections', r.id),{
                archived: true,
                mergedInto: keep.id,
                key: key,
                updatedAt: new Date().toISOString()
              });
              archived++;
            }catch(err){ console.warn('Archive duplicate failed', r.id, err); }
          }
          // Normalize keep doc fields and ensure key present
          try{
            await firebase.updateDoc(firebase.doc(this.db,'chatConnections', keep.id),{
              key,
              participants: keep.participants || [],
              updatedAt: new Date().toISOString()
            });
          }catch(_){ }
        }
        alert(archived>0 ? `Archived ${archived} duplicate chats.` : 'No duplicates found.');
        await this.loadConnections();
      }catch(e){ console.error('Fix duplicates failed:', e); /* no alert */ }
    }

    refreshActionButton(){
      const input = document.getElementById('message-input');
      const review = document.getElementById('recording-review');
      const actionBtn = document.getElementById('action-btn');
      const inReview = review && !review.classList.contains('hidden');
      const hasQueuedAttachments = (Array.isArray(this._pendingAttachments) && this._pendingAttachments.length > 0) || (Array.isArray(this._pendingRemoteShares) && this._pendingRemoteShares.length > 0) || (Array.isArray(this._pendingReusedAttachments) && this._pendingReusedAttachments.length > 0);
      const hasContent = !!(input && input.value.trim().length) || hasQueuedAttachments;
      if (hasContent){
        actionBtn.title = 'Send';
        actionBtn.innerHTML = '<i class="fas fa-arrow-up"></i>';
        actionBtn.style.background = '#2563eb';
        actionBtn.style.borderRadius = '12px';
        actionBtn.style.color = '#fff';
      } else if (inReview){
        actionBtn.title = 'Send';
        actionBtn.innerHTML = '<i class="fas fa-arrow-up"></i>';
        actionBtn.style.background = '#2563eb';
        actionBtn.style.borderRadius = '12px';
        actionBtn.style.color = '#fff';
      } else {
        const isVideoMode = this._recordMode === 'video';
        actionBtn.title = isVideoMode ? 'Video message' : 'Voice message';
        actionBtn.innerHTML = `<i class="fas ${isVideoMode ? 'fa-video' : 'fa-microphone'}"></i>`;
        actionBtn.style.background = '#2563eb';
        actionBtn.style.borderRadius = '12px';
        actionBtn.style.color = '#fff';
      }
    }

    ensureComposerAttachmentHost(){
      let host = document.getElementById('composer-attachments');
      if (host) return host;
      const composer = document.querySelector('.composer');
      if (!composer || !composer.parentElement) return null;
      host = document.createElement('div');
      host.id = 'composer-attachments';
      host.className = 'composer-attachments hidden';
      composer.parentElement.insertBefore(host, composer);
      return host;
    }

    clearPendingAttachments(){
      try{
        (this._pendingAttachments || []).forEach((item)=>{
          const url = String(item?.previewUrl || '').trim();
          if (url) {
            try{ URL.revokeObjectURL(url); }catch(_){ }
          }
        });
      }catch(_){ }
      this._pendingAttachments = [];
      this._pendingRemoteShares = [];
      this._pendingReusedAttachments = [];
      this.renderComposerAttachmentQueue();
    }

    queueReusedAttachment({ fileUrl, fileName, message }){
      try{
        if (!fileUrl || !String(fileUrl).trim()) return;
        this._pendingReusedAttachments = this._pendingReusedAttachments || [];
        const sig = `${String(fileUrl||'').trim()}`;
        const existing = new Set((this._pendingReusedAttachments || []).map((x)=> String(x?.fileUrl || '').trim()));
        if (existing.has(sig)) return;
        this._pendingReusedAttachments.push({ fileUrl: String(fileUrl).trim(), fileName: String(fileName || 'Attachment').trim(), message: message || null });
        this.renderComposerAttachmentQueue();
      }catch(_){ }
    }

    renderComposerAttachmentQueue(){
      try{
        const host = this.ensureComposerAttachmentHost();
        if (!host) return;
        const queue = Array.isArray(this._pendingAttachments) ? this._pendingAttachments : [];
        const remote = Array.isArray(this._pendingRemoteShares) ? this._pendingRemoteShares : [];
        const reused = Array.isArray(this._pendingReusedAttachments) ? this._pendingReusedAttachments : [];
        const allQueue = queue.concat(remote).concat(reused.map((r)=>({ reused: r })));
        if (!allQueue.length){
          host.classList.add('hidden');
          host.innerHTML = '';
          this.refreshActionButton();
          return;
        }
        host.classList.remove('hidden');
        host.innerHTML = '';
        const slider = document.createElement('div');
        slider.className = 'composer-attachments-slider';
        allQueue.forEach((item, idx)=>{
          const card = document.createElement('div');
          card.className = 'composer-attachment-card';
          if (item && item.file instanceof File){
            const file = item.file;
            const objectUrl = String(item.previewUrl || '').trim();
            const isImage = !!objectUrl;
            const icon = isImage
              ? `<img src="${objectUrl}" alt="${this.renderText(file.name)}" class="composer-attachment-thumb">`
              : `<span class="composer-attachment-icon"><i class="fas ${String(file.type || '').startsWith('video/') ? 'fa-video' : (String(file.type || '').startsWith('audio/') ? 'fa-music' : 'fa-file')}"></i></span>`;
            card.innerHTML = `${icon}<div class="composer-attachment-meta"><div class="composer-attachment-name">${this.renderText(file.name || 'file')}</div><div class="composer-attachment-size">${this.formatBytes(file.size || 0)}</div></div><button class="composer-attachment-remove" type="button" title="Remove"><i class="fas fa-xmark"></i></button>`;
          } else if (item && item.sharedAsset){
            const a = item.sharedAsset;
            const kind = String(a.kind || '').toLowerCase();
            const icon = (kind === 'image' || kind === 'video') && a.cover
              ? `<img src="${this.renderText(String(a.cover || a.url || ''))}" alt="${this.renderText(String(a.title || 'shared'))}" class="composer-attachment-thumb">`
              : `<span class="composer-attachment-icon"><i class="fas ${kind === 'video' ? 'fa-video' : (kind === 'audio' ? 'fa-music' : (kind === 'image' ? 'fa-image' : 'fa-file'))}"></i></span>`;
            card.innerHTML = `${icon}<div class="composer-attachment-meta"><div class="composer-attachment-name">${this.renderText(String(a.title || `Shared ${kind || 'asset'}`))}</div><div class="composer-attachment-size">Shared card</div></div><button class="composer-attachment-remove" type="button" title="Remove"><i class="fas fa-xmark"></i></button>`;
          } else if (item && item.reused){
            const r = item.reused;
            const fn = String(r.fileName || 'Attachment').trim();
            const isImg = this.isImageFilename(fn);
            const isVid = this.isVideoFilename(fn);
            const isAud = this.isAudioFilename(fn);
            const icon = `<span class="composer-attachment-icon"><i class="fas ${isImg ? 'fa-image' : (isVid ? 'fa-video' : (isAud ? 'fa-music' : 'fa-file'))}"></i></span>`;
            card.innerHTML = `${icon}<div class="composer-attachment-meta"><div class="composer-attachment-name">${this.renderText(fn)}</div><div class="composer-attachment-size">From chat</div></div><button class="composer-attachment-remove" type="button" title="Remove"><i class="fas fa-xmark"></i></button>`;
          } else {
            return;
          }
          const rm = card.querySelector('.composer-attachment-remove');
          if (rm){
            rm.addEventListener('click', ()=>{
              const removed = allQueue[idx];
              const removedUrl = String(removed?.previewUrl || '').trim();
              if (removedUrl){
                try{ URL.revokeObjectURL(removedUrl); }catch(_){ }
              }
              if (idx < queue.length) this._pendingAttachments.splice(idx, 1);
              else if (idx < queue.length + remote.length) this._pendingRemoteShares.splice(idx - queue.length, 1);
              else this._pendingReusedAttachments.splice(idx - queue.length - remote.length, 1);
              this.renderComposerAttachmentQueue();
            });
          }
          slider.appendChild(card);
        });
        host.appendChild(slider);
        this.refreshActionButton();
      }catch(_){ }
    }

    formatBytes(size){
      const n = Number(size || 0);
      if (!Number.isFinite(n) || n <= 0) return '0 B';
      if (n < 1024) return `${Math.round(n)} B`;
      if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
      if (n < 1024 * 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(1)} MB`;
      return `${(n / (1024 * 1024 * 1024)).toFixed(1)} GB`;
    }

    queueAttachments(files){
      try{
        if (!files || !files.length) return;
        const list = Array.from(files).filter((f)=> f instanceof File);
        if (!list.length) return;
        const existing = new Set((this._pendingAttachments || []).map((x)=> `${x.file?.name || ''}|${x.file?.size || 0}|${x.file?.lastModified || 0}`));
        list.forEach((file)=>{
          const sig = `${file.name || ''}|${file.size || 0}|${file.lastModified || 0}`;
          if (existing.has(sig)) return;
          existing.add(sig);
          const previewUrl = String(file.type || '').startsWith('image/') ? URL.createObjectURL(file) : '';
          this._pendingAttachments.push({ file, previewUrl });
        });
        this.renderComposerAttachmentQueue();
      }catch(_){ }
    }

    async openWaveConnectPickerForComposer(){
      try{
        if (!this.db) return;
        const dm = window.dashboardManager || window.top?.dashboardManager || window.parent?.dashboardManager;
        if (dm && typeof dm.openWaveConnectPickerForChat === 'function'){
          dm.openWaveConnectPickerForChat((payload)=>{
            this.queueRemoteSharedAssets([{ sharedAsset: payload }]);
          });
          return;
        }
        const me = this.currentUser || window.firebaseService?.auth?.currentUser;
        const uid = me?.uid;
        if (!uid){ alert('Sign in to add from WaveConnect'); return; }
        const audioRows = []; const videoRows = [];
        try{
          const q = firebase.query(firebase.collection(this.db,'wave'), firebase.where('ownerId','==', uid), firebase.orderBy('createdAt','desc'), firebase.limit(60));
          const s = await firebase.getDocs(q); s.forEach((d)=> audioRows.push(d.data() || {}));
        }catch(_){
          try{
            const q2 = firebase.query(firebase.collection(this.db,'wave'), firebase.where('ownerId','==', uid));
            const s2 = await firebase.getDocs(q2); s2.forEach((d)=> audioRows.push(d.data() || {}));
          }catch(__){ }
        }
        try{
          const qv = firebase.query(firebase.collection(this.db,'videos'), firebase.where('owner','==', uid), firebase.orderBy('createdAtTS','desc'), firebase.limit(60));
          const sv = await firebase.getDocs(qv); sv.forEach((d)=> videoRows.push(d.data() || {}));
        }catch(_){
          try{
            const qv2 = firebase.query(firebase.collection(this.db,'videos'), firebase.where('owner','==', uid));
            const sv2 = await firebase.getDocs(qv2); sv2.forEach((d)=> videoRows.push(d.data() || {}));
          }catch(__){ }
        }
        const rows = [
          ...audioRows.map((w)=> ({ type:'audio', data:w })),
          ...videoRows.map((v)=> {
            const st = String(v?.sourceMediaType || '').toLowerCase();
            const inferred = st === 'image' ? 'image' : (st === 'video' ? 'video' : (String(v?.mediaType||'') === 'image' ? 'image' : 'video'));
            return ({ type: inferred, data: v });
          })
        ];
        rows.sort((a,b)=> new Date(b.data?.createdAt||0) - new Date(a.data?.createdAt||0));
        if (!rows.length){ alert('No WaveConnect media found'); return; }
        const overlay = document.createElement('div');
        overlay.className = 'modal-overlay';
        overlay.style.cssText = 'position:fixed;inset:0;z-index:1300;background:rgba(0,0,0,.58);display:flex;align-items:center;justify-content:center;padding:16px';
        overlay.innerHTML = '<div style="width:min(96vw,560px);max-height:76vh;overflow:auto;background:#0f1724;border:1px solid #2b3445;border-radius:12px;padding:12px"><div style="font-weight:700;margin-bottom:8px">Add from WaveConnect (to composer)</div><div id="chat-wave-picker-list"></div><div style="display:flex;justify-content:flex-end;margin-top:8px"><button id="chat-wave-picker-close" class="btn btn-secondary">Close</button></div></div>';
        const list = overlay.querySelector('#chat-wave-picker-list');
        list.style.cssText = 'display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:10px';
        rows.slice(0,80).forEach((entry)=>{
          const w = entry.data || {};
          const isVideo = entry.type === 'video';
          const isImage = entry.type === 'image';
          const kind = isVideo ? 'video' : (isImage ? 'image' : 'audio');
          const title = String(w.title || (isVideo ? 'Video' : (isImage ? 'Picture' : 'Audio'))).replace(/</g,'&lt;');
          const by = String(w.authorName || '').replace(/</g,'&lt;');
          const url = String(w.url || '').trim();
          const cover = isVideo || isImage ? String(w.thumbnailUrl||w.coverUrl||w.url||'') : String(w.coverUrl||'');
          const card = document.createElement('div');
          card.className = 'shared-asset-card shared-asset-waveconnect wave-picker-card';
          card.style.cssText = 'border:1px solid #2b3240;border-radius:12px;padding:10px;background:#0f1520;cursor:pointer;transition:transform .15s,box-shadow .15s';
          card.onmouseenter = ()=> { card.style.transform='scale(1.02)'; card.style.boxShadow='0 4px 12px rgba(0,0,0,.4)'; };
          card.onmouseleave = ()=> { card.style.transform=''; card.style.boxShadow=''; };
          if (isVideo){
            card.innerHTML = `<div class="shared-asset-head" style="margin-bottom:8px"><div class="shared-asset-title" style="font-weight:600;font-size:13px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${title}</div>${by ? `<div class="shared-asset-byline" style="font-size:11px;opacity:.85">by ${by}</div>` : ''}</div><video src="${this.renderText(url)}" muted playsinline preload="metadata" style="width:100%;max-height:120px;border-radius:8px;object-fit:cover;background:#000"></video>`;
          } else if (isImage){
            card.innerHTML = `<div class="shared-asset-head" style="margin-bottom:8px"><div class="shared-asset-title" style="font-weight:600;font-size:13px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${title}</div>${by ? `<div class="shared-asset-byline" style="font-size:11px;opacity:.85">by ${by}</div>` : ''}</div><img src="${this.renderText(url)}" alt="" style="width:100%;max-height:140px;border-radius:8px;object-fit:cover">`;
          } else {
            const coverHtml = cover ? `<img src="${this.renderText(cover)}" alt="" style="width:48px;height:48px;border-radius:8px;object-fit:cover">` : `<span style="width:48px;height:48px;border-radius:8px;display:flex;align-items:center;justify-content:center;background:rgba(255,255,255,.08)"><i class="fas fa-music"></i></span>`;
            card.innerHTML = `<div class="shared-asset-head" style="display:flex;gap:10px;align-items:flex-start;margin-bottom:8px"><div style="flex-shrink:0">${coverHtml}</div><div style="min-width:0;flex:1;overflow:hidden"><div class="shared-asset-title" style="font-weight:600;font-size:13px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${title}</div>${by ? `<div class="shared-asset-byline" style="font-size:11px;opacity:.85">by ${by}</div>` : ''}</div></div>`;
          }
          card.onclick = ()=>{
            const payload = isVideo
              ? { kind:'video', url, title: String(w.title||'Video'), name: String(w.title||'Video'), by, authorName: by, cover: String(w.thumbnailUrl||w.coverUrl||''), thumbnailUrl: String(w.thumbnailUrl||w.coverUrl||''), sourceId: String(w.id||'') }
              : (isImage
              ? { kind:'image', url, title: String(w.title||'Picture'), name: String(w.title||'Picture'), by, authorName: by, cover: String(w.thumbnailUrl||w.coverUrl||w.url||''), thumbnailUrl: String(w.thumbnailUrl||w.coverUrl||w.url||''), sourceId: String(w.id||'') }
              : { kind:'audio', url, title: String(w.title||'Audio'), name: String(w.title||'Audio'), by, authorName: by, cover: String(w.coverUrl||''), coverUrl: String(w.coverUrl||''), sourceId: String(w.id||'') });
            this.queueRemoteSharedAssets([{ sharedAsset: payload }]);
            overlay.remove();
          };
          list.appendChild(card);
        });
        overlay.querySelector('#chat-wave-picker-close').onclick = ()=> overlay.remove();
        overlay.addEventListener('click', (e)=>{ if (e.target === overlay) overlay.remove(); });
        document.body.appendChild(overlay);
      }catch(_){ alert('Failed to load WaveConnect'); }
    }

    queueRemoteSharedAssets(items){
      try{
        const rows = Array.isArray(items) ? items : [];
        if (!rows.length) return;
        const existing = new Set((this._pendingRemoteShares || []).map((x)=> `${x?.sharedAsset?.kind || ''}|${x?.sharedAsset?.url || ''}|${x?.sharedAsset?.postId || ''}`));
        rows.forEach((it)=>{
          if (!it || typeof it !== 'object') return;
          const raw = it.sharedAsset || it.asset || it;
          const inferredKind = String(raw?.kind || raw?.type || (raw?.post || raw?.postId ? 'post' : '')).toLowerCase();
          const a = (inferredKind === 'post')
            ? { ...raw, kind: 'post', postId: String(raw?.postId || raw?.post?.id || '').trim() || null }
            : { ...raw, kind: inferredKind || String(raw?.kind || raw?.type || '').toLowerCase() };
          const sig = `${String(a.kind || '')}|${String(a.url || '')}|${String(a.postId || '')}`;
          const canQueue = String(a.url || '').trim() || (String(a.kind || '').toLowerCase() === 'post' && String(a.postId || a?.post?.id || '').trim());
          if (!canQueue || existing.has(sig)) return;
          existing.add(sig);
          this._pendingRemoteShares.push({ sharedAsset: a });
        });
        this.renderComposerAttachmentQueue();
      }catch(_){ }
    }

    takePendingSharedAssetsForConn(connId){
      try{
        const cid = String(connId || '').trim();
        if (!cid) return [];
        const key = 'liber_chat_pending_shares_v1';
        const raw = localStorage.getItem(key);
        const arr = raw ? JSON.parse(raw) : [];
        const list = Array.isArray(arr) ? arr : [];
        const keep = [];
        const picked = [];
        list.forEach((row)=>{
          if (String(row?.connId || '').trim() === cid) picked.push(row);
          else keep.push(row);
        });
        localStorage.setItem(key, JSON.stringify(keep.slice(-80)));
        return picked.map((x)=> {
          const payload = x?.payload || {};
          const asset = payload.asset && typeof payload.asset === 'object' ? payload.asset : null;
          const post = payload.post && typeof payload.post === 'object' ? payload.post : (asset?.post && typeof asset.post === 'object' ? asset.post : null);
          const inferredKind = String(asset?.kind || asset?.type || (post || payload?.postId ? 'post' : '')).toLowerCase();
          if (asset){
            if (inferredKind === 'post'){
              return { sharedAsset: { ...asset, kind: 'post', postId: String(asset.postId || post?.id || payload?.postId || '').trim() || null, post: post || asset.post || null } };
            }
            return { sharedAsset: { ...asset, kind: inferredKind || String(asset.kind || '').toLowerCase() } };
          }
          if (post || payload?.postId){
            return { sharedAsset: { kind:'post', postId: String(payload.postId || post?.id || '').trim() || null, title: String(post?.text || 'Post'), by: String(post?.authorName || ''), post: post || null } };
          }
          return { sharedAsset: null };
        }).filter((x)=> x.sharedAsset);
      }catch(_){ return []; }
    }

    handleActionButton(){
      if (Date.now() < (this._suppressActionClickUntil || 0)) return;
      const input = document.getElementById('message-input');
      const review = document.getElementById('recording-review');
      const hasQueuedAttachments = (Array.isArray(this._pendingAttachments) && this._pendingAttachments.length > 0) || (Array.isArray(this._pendingRemoteShares) && this._pendingRemoteShares.length > 0) || (Array.isArray(this._pendingReusedAttachments) && this._pendingReusedAttachments.length > 0);
      if (this._activeRecorder && this._recStop){
        try{ this._recStop(); }catch(_){ }
        return;
      }
      if (review && !review.classList.contains('hidden')){
        const sendBtn = document.getElementById('send-recording-btn');
        if (sendBtn){ sendBtn.click(); return; }
      }
      if ((input && input.value.trim().length) || hasQueuedAttachments){
        this.sendCurrent();
      } else {
        // Toggle stable recording mode (audio <-> video)
        this._recordMode = this._recordMode === 'video' ? 'audio' : 'video';
        this.refreshActionButton();
      }
    }

    handleActionPressStart(e){
      const input = document.getElementById('message-input');
      if ((input && input.value.trim().length) || ((Array.isArray(this._pendingAttachments) && this._pendingAttachments.length) || (Array.isArray(this._pendingRemoteShares) && this._pendingRemoteShares.length) || (Array.isArray(this._pendingReusedAttachments) && this._pendingReusedAttachments.length))) return; // only record when empty
      if (this._activeRecorder) return;
      if (e && e.type === 'mousedown' && (this._lastTouchStartAt || 0) && (Date.now() - (this._lastTouchStartAt || 0)) < 500) return; // ignore synthetic mouse after touch
      this._actionPressArmed = true;
      if (e && e.type === 'touchstart'){
        this._lastTouchStartAt = Date.now();
        this._suppressActionClickUntil = Date.now() + 650;
      }
      if (this._pressTimer){ clearTimeout(this._pressTimer); this._pressTimer = null; }
      const indicator = document.getElementById('recording-indicator');
      if (indicator) indicator.classList.remove('hidden');
      this._pressTimer = setTimeout(async()=>{
        this._isRecordingByHold = true;
        try{
          const useVideo = this._recordMode === 'video';
          if (indicator) { indicator.querySelector('i').className = `fas ${useVideo ? 'fa-video' : 'fa-microphone'}`; }
          if (useVideo) await this.recordVideoMessage();
          else await this.recordVoiceMessage();
        }catch(err){ alert('Recording failed'); }
        finally{
          if (indicator) indicator.classList.add('hidden');
          this._actionPressArmed = false;
        }
      }, 200);
    }

    // Crystal-clear recording: noise suppression, auto gain, stereo, high sample rate.
    getRecordingAudioConstraints(){
      return {
        echoCancellation: true,
        noiseSuppression: true,
        autoGainControl: true,
        sampleRate: { ideal: 48000 },
        channelCount: { ideal: 2 }
      };
    }

    getPreferredMediaRecorderOptions(kind = 'audio'){
      try{
        const MR = window.MediaRecorder;
        if (!MR || typeof MR.isTypeSupported !== 'function') return {};
        const audioBits = 128000; // Crystal-clear voice + ambient
        if (kind === 'video'){
          const videoTypes = [
            'video/webm;codecs=vp9,opus',
            'video/webm;codecs=vp8,opus',
            'video/webm;codecs=h264,opus',
            'video/webm',
            'video/mp4'
          ];
          const chosen = videoTypes.find((t)=> MR.isTypeSupported(t));
          return chosen ? { mimeType: chosen, audioBitsPerSecond: audioBits } : {};
        }
        const audioTypes = ['audio/webm;codecs=opus', 'audio/webm', 'audio/ogg;codecs=opus'];
        const chosen = audioTypes.find((t)=> MR.isTypeSupported(t));
        return chosen ? { mimeType: chosen, audioBitsPerSecond: audioBits } : {};
      }catch(_){ return {}; }
    }

    handleActionPressEnd(e){
      if (!this._actionPressArmed && !this._isRecordingByHold && !this._pressTimer) return;
      if (e && e.type === 'mouseup' && (this._lastTouchStartAt || 0) && (Date.now() - (this._lastTouchStartAt || 0)) < 500){ this._actionPressArmed = false; this._pressTimer && clearTimeout(this._pressTimer); this._pressTimer = null; return; }
      const hadPressTimer = !!this._pressTimer;
      if (this._pressTimer){ clearTimeout(this._pressTimer); this._pressTimer = null; }
      if (this._isRecordingByHold){
        try{ if (this._recStop) this._recStop(); }catch(_){ }
      } else if (hadPressTimer){
        const input = document.getElementById('message-input');
        const review = document.getElementById('recording-review');
        if (!this._activeRecorder && (!input || !input.value.trim().length) && (!review || review.classList.contains('hidden'))){
          this._recordMode = this._recordMode === 'video' ? 'audio' : 'video';
          this.refreshActionButton();
          this._suppressActionClickUntil = Date.now() + 600;
        }
      }
      this._isRecordingByHold = false;
      this._actionPressArmed = false;
      const indicator = document.getElementById('recording-indicator'); if (indicator) indicator.classList.add('hidden');
      this.refreshActionButton();
    }

    async promptNewConnection(){
      // Enter group selection mode
      this.isGroupMode = true;
      this.groupSelection = new Map(); // uid -> {uid, username, email}
      const panel = document.getElementById('group-builder');
      const chips = document.getElementById('group-selected');
      const createBtn = document.getElementById('create-group-btn');
      if (panel) panel.style.display = 'block';
      if (chips) chips.innerHTML = '';
      if (createBtn){
        createBtn.onclick = async ()=>{
          const members = Array.from(this.groupSelection.values());
          if (members.length === 0){ this.isGroupMode = false; if (panel) panel.style.display='none'; return; }
          let baseUids = Array.isArray(this.groupBaseParticipants)? this.groupBaseParticipants.slice() : [this.currentUser.uid];
          const addUids = members.map(m=> m.uid||m.id);
          const participantUids = Array.from(new Set([...baseUids, ...addUids]));
          const myName = (this.me&&this.me.username) || (this.currentUser.email||'me');
          const nameMap = new Map();
          nameMap.set(this.currentUser.uid, myName);
          members.forEach(m=> nameMap.set(m.uid||m.id, m.username||m.email));
          const participantNames = participantUids.map(uid=> nameMap.get(uid) || uid);
          const key = this.computeConnKey(participantUids);
          let connId = await this.findConnectionByKey(key);
          if (!connId){
            try{
              const stableRef = firebase.doc(this.db,'chatConnections', key);
              await firebase.setDoc(stableRef,{
                id: key,
                key,
                participants: participantUids,
                participantUsernames: participantNames,
                admins: [this.currentUser.uid],
                createdAt: new Date().toISOString(),
                updatedAt: new Date().toISOString(),
                lastMessage:''
              }, { merge:true });
              connId = key;
            }catch(errStable){
              // If key already exists or race happened, resolve canonical id by key.
              connId = await this.findConnectionByKey(key);
              if (!connId) throw errStable;
            }
          }
          this.isGroupMode = false;
          if (panel) panel.style.display='none';
          await this.loadConnections();
          this.setActive(connId, participantNames.filter(n=> n !== ((this.me&&this.me.username)||this.currentUser.email)).join(', '));
        };
      }
      // Ensure panel is open on mobile
      const sidebar = document.querySelector('.sidebar');
      if (sidebar && !sidebar.classList.contains('open')) sidebar.classList.add('open');
      const search = document.getElementById('user-search'); if (search) search.focus();
    }

    async loadConnections(){
      const listEl = document.getElementById('connections-list');
      if (!listEl) return;
      const connSeq = (this._connLoadSeq || 0) + 1;
      this._connLoadSeq = connSeq;
      listEl.classList.add('loading');
      listEl.innerHTML = '';
      let permissionDenied = false;
      try{
        const fields = ['participants', 'users', 'memberIds'];
        const byId = new Map();
        for (const field of fields){
          try{
            const q = firebase.query(
              firebase.collection(this.db,'chatConnections'),
              firebase.where(field,'array-contains', this.currentUser.uid),
              firebase.orderBy('updatedAt','desc')
            );
            const s = await firebase.getDocs(q);
            s.forEach(d=> byId.set(d.id, { id: d.id, ...d.data() }));
          }catch(_){
            try{
              const q2 = firebase.query(
                firebase.collection(this.db,'chatConnections'),
                firebase.where(field,'array-contains', this.currentUser.uid)
              );
              const s2 = await firebase.getDocs(q2);
              s2.forEach(d=> byId.set(d.id, { id: d.id, ...d.data() }));
            }catch(e2){
              if (e2 && e2.code === 'permission-denied') permissionDenied = true;
            }
          }
        }
        // Key-only legacy docs fallback (no participants/users/memberIds arrays).
        try{
          let allSnap;
          try{
            const qAll = firebase.query(
              firebase.collection(this.db,'chatConnections'),
              firebase.orderBy('updatedAt','desc'),
              firebase.limit(600)
            );
            allSnap = await firebase.getDocs(qAll);
          }catch(_){
            allSnap = await firebase.getDocs(firebase.collection(this.db,'chatConnections'));
          }
          allSnap.forEach(d=>{
            const row = { id: d.id, ...d.data() };
            const key = String(row.key || '');
            if (!key) return;
            const keyParts = key.split('|').filter(Boolean);
            if (keyParts.includes(this.currentUser.uid)) byId.set(d.id, row);
          });
        }catch(_){ }
        // Last-resort scan (handles transient index/network glitches on reopen).
        if (byId.size === 0 && !permissionDenied){
          try{
            const anySnap = await firebase.getDocs(firebase.collection(this.db,'chatConnections'));
            anySnap.forEach(d=>{
              const row = { id: d.id, ...d.data() };
              const parts = this.getConnParticipants(row || {});
              const key = String(row.key || '');
              if (parts.includes(this.currentUser.uid) || key.split('|').includes(this.currentUser.uid)){
                byId.set(d.id, row);
              }
            });
          }catch(_){ }
        }
        if (connSeq !== this._connLoadSeq) return;
        const temp = Array.from(byId.values());
        temp.forEach((c)=>{
          const fallbackParts = this.getConnParticipants(c);
          if (!Array.isArray(c.participants) && fallbackParts.length) c.participants = fallbackParts;
          if (Array.isArray(c.participants) && c.participants.length === 2){
            const peer = c.participants.find((u)=> u && u !== this.currentUser.uid);
            if (peer) this._peerUidByConn.set(c.id, peer);
          }
        });
        temp.sort((a,b)=> new Date(b.updatedAt||0) - new Date(a.updatedAt||0));
        this.connections = temp;
        if (this.activeConnection){
          const active = temp.find((c)=> c && c.id === this.activeConnection);
          if (active && typeof active.readBy === 'object'){
            this._activeConnReadBy = active.readBy;
          }
        }
      } catch (e) {
        if (e && e.code === 'permission-denied') permissionDenied = true;
        this.connections = [];
      }
      if (connSeq !== this._connLoadSeq) return;
      if (permissionDenied && this.connections.length === 0){
        listEl.classList.remove('loading');
        listEl.innerHTML = '<li style="opacity:.8">No access to chat connections. Please redeploy Firestore rules and reload.</li>';
      }
      if (!permissionDenied && this.connections.length === 0){
        listEl.classList.remove('loading');
        listEl.innerHTML = '<li class="conn-loading-item" style="opacity:.8">Loading chats…</li>';
        if (this._connRetryTimer) clearTimeout(this._connRetryTimer);
        this._connRetryTimer = setTimeout(()=>{
          if (connSeq === this._connLoadSeq) this.loadConnections().catch(()=>{});
        }, 800);
      } else if (this._connRetryTimer){
        clearTimeout(this._connRetryTimer);
        this._connRetryTimer = null;
      }
      const seen = new Set();
      const getCachedName = (uid, fallback = '')=>{
        const cached = this.usernameCache.get(uid);
        if (cached && typeof cached === 'object') return cached.username || fallback;
        return cached || fallback;
      };
      const getCachedAvatar = (uid)=>{
        const direct = this._avatarCache.get(uid);
        if (direct) return direct;
        const cached = this.usernameCache.get(uid);
        if (cached && typeof cached === 'object' && cached.avatarUrl) return cached.avatarUrl;
        return '../../images/default-bird.png';
      };
      // Backfill participantUsernames if missing
      for (const c of this.connections){
        try{
          const parts = Array.isArray(c.participants)? c.participants:[];
          const names = Array.isArray(c.participantUsernames)? c.participantUsernames:[];
          if (parts.length && names.length !== parts.length){
            const enriched = [];
            for (const uid of parts){
              if (uid === this.currentUser.uid){ enriched.push((this.me&&this.me.username)||this.currentUser.email||'me'); continue; }
              enriched.push(getCachedName(uid, names[parts.indexOf(uid)] || ('User ' + String(uid).slice(0,6))));
            }
            c.participantUsernames = enriched;
          }
        }catch(_){ }
      }
      listEl.classList.remove('loading');
      listEl.innerHTML = '';
      this.connections.forEach(c=>{
        if (connSeq !== this._connLoadSeq) return;
        const key = c.key || this.computeConnKey(c.participants||[]);
        if (seen.has(key)) return; seen.add(key);
        const li = document.createElement('li');
        li.setAttribute('data-id', c.id);
        let label = 'Chat';
        const myNameLower = ((this.me && this.me.username) || '').toLowerCase();
        if (Array.isArray(c.participantUsernames) && c.participantUsernames.length){
          const others = c.participantUsernames.filter(n=> String(n ?? '').toLowerCase() !== myNameLower);
          if (String(c.groupName || '').trim()) label = String(c.groupName).trim();
          else if (others.length===1) label = others[0];
          else if (others.length>1){
            label = others.slice(0,2).join(', ');
            if (others.length>2) label += `, +${others.length-2}`;
          } else {
            label = 'Chat';
          }
        } else if (Array.isArray(c.participants) && c.participants.length) {
          const others = c.participants.filter(u => u !== this.currentUser.uid);
          label = String(c.groupName || '').trim() || (others.length === 1 ? `Chat with ${others[0].slice(0,8)}` : `Group Chat (${others.length})`);
        } else {
          label = 'Chat';
        }
        const updatedMs = Number(new Date(c.updatedAt || 0).getTime() || 0);
        const readMs = this.getEffectiveReadMarkerForConn(c.id, c);
        const fromPeer = String(c.lastMessageSender || '').trim() && String(c.lastMessageSender || '').trim() !== this.currentUser?.uid;
        const isUnread = !!c.id && fromPeer && updatedMs > readMs;
        const unreadDotHtml = isUnread ? '<span class="chat-unread-dot" style="margin-left:auto;min-width:8px;height:8px;border-radius:50%;background:#4da3ff;display:inline-block;box-shadow:0 0 0 2px rgba(77,163,255,.2);flex-shrink:0"></span>' : '';
        li.innerHTML = `<span class="chat-conn-row" style="display:flex;align-items:center;gap:8px;min-width:0;width:100%">
          <img class="chat-conn-avatar" src="../../images/default-bird.png" alt="" style="width:18px;height:18px;object-fit:cover;clip-path:polygon(50% 0, 0 100%, 100% 100%);border:1px solid rgba(255,255,255,.24);flex:0 0 auto">
          <span class="chat-label" style="min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;flex:1">${String(label || 'Chat').replace(/</g,'&lt;')}</span>
          ${unreadDotHtml}
        </span>`;
        const partsNow = Array.isArray(c.participants) ? c.participants : [];
        const peerUidNow = partsNow.length === 2 ? partsNow.find((u)=> u && u !== this.currentUser.uid) : '';
        const avatarEl = li.querySelector('.chat-conn-avatar');
        if (avatarEl){
          if (peerUidNow){
            avatarEl.src = getCachedAvatar(peerUidNow);
          }else if (String(c.groupCoverUrl || '').trim()){
            avatarEl.src = String(c.groupCoverUrl).trim();
          } else {
            avatarEl.src = '../../images/default-bird.png';
          }
        }
        // Admin badge in header when active
        li.addEventListener('mouseenter', ()=> li.classList.add('active-hover'));
        li.addEventListener('click', async ()=>{
          const targetId = c.id;
          if (targetId && this.activeConnection === targetId){
            this.setMobileMenuOpen(false);
            return;
          }
          await this.setActive(targetId || c.id);
          this.setMobileMenuOpen(false);
        });
        listEl.appendChild(li);
      });
      // Subscribe to participant username changes for live label updates
      try{
        if (connSeq !== this._connLoadSeq) return;
        const uidSet = new Set();
        this.connections.forEach(c => (Array.isArray(c.participants)?c.participants:[]).forEach(u=>uidSet.add(u)));
        const uids = Array.from(uidSet);
        // Create listeners
        uids.forEach(uid => {
          if (this.userUnsubs && this.userUnsubs.has(uid)) return;
          try{
            const ref = firebase.doc(this.db, 'users', uid);
            const unsub = firebase.onSnapshot(ref, (snap)=>{
              if (!snap.exists()) return;
              const d = snap.data() || {};
              const name = d.username || d.email || uid;
              const avatar = String(d.avatarUrl || '../../images/default-bird.png');
              const prevObj = this.usernameCache.get(uid);
              const prevName = (prevObj && typeof prevObj === 'object') ? prevObj.username : prevObj;
              const prevAvatar = this._avatarCache.get(uid);
              if (prevName !== name || prevAvatar !== avatar){
                this.usernameCache.set(uid, { username: name, avatarUrl: avatar });
                this._avatarCache.set(uid, avatar);
                // Refresh connection list labels and active header
                try{
                  const listEl = document.getElementById('connections-list');
                  if (listEl){
                    const myNameLower = (this.me?.username || '').toLowerCase();
                    listEl.querySelectorAll('li').forEach(li => {
                      const id = li.getAttribute('data-id');
                      const c = this.connections.find(x => x.id === id);
                      if (!c) return;
                      const parts = Array.isArray(c.participants)?c.participants:[];
                      const stored = Array.isArray(c.participantUsernames)?c.participantUsernames:[];
                      const names = parts.map((p,i)=> getCachedName(p, stored[i] || p));
                      const others = names.filter(n => String(n ?? '').toLowerCase() !== myNameLower);
                      const label = String(c.groupName || '').trim() || (others.length===1? others[0] : (others.slice(0,2).join(', ')+(others.length>2?`, +${others.length-2}`:'')));
                      const labelEl = li.querySelector('.chat-label');
                      if (labelEl) labelEl.textContent = label || 'Chat';
                      const avatarEl = li.querySelector('.chat-conn-avatar');
                      if (avatarEl){
                        const peerUid = parts.length === 2 ? parts.find((u)=> u && u !== this.currentUser.uid) : '';
                        if (peerUid) avatarEl.src = getCachedAvatar(peerUid);
                        else avatarEl.src = String(c.groupCoverUrl || '../../images/default-bird.png');
                      }
                      li.setAttribute('data-id', c.id);
                    });
                  }
                  // Keep active chat stable; do not re-open chat on every profile update.
                }catch(_){ }
              }
            });
            if (this.userUnsubs) this.userUnsubs.set(uid, unsub);
          }catch(_){ }
        });
      }catch(_){ }
      // No recursive call
      this.updateUnreadBadges().catch(()=>{});
    }

    async setActive(connId, displayName){
      const setSeq = (this._setActiveSeq || 0) + 1;
      this._setActiveSeq = setSeq;
      this._voiceHydrateSession = (this._voiceHydrateSession || 0) + 1;
      this._voiceHydrateQueue = [];
      // Always resolve to one canonical chat doc for this participant key, so
      // chat search / sidebar / personal-space popup all open the same thread.
      const shouldResolve = String(connId || '').includes('|');
      const resolvedConnId = shouldResolve ? await this.resolveCanonicalConnectionId(connId) : connId;
      if (setSeq !== this._setActiveSeq) return;
      const prevConn = this.activeConnection;
      if (prevConn && prevConn !== (resolvedConnId || connId)){
        this.publishTypingState(false, { force: true, connId: prevConn }).catch(()=>{});
      }
      this.stopTypingListener();
      this.activeConnection = resolvedConnId || connId;
      this.clearPendingAttachments();
      try{
        const pendingShared = this.takePendingSharedAssetsForConn(this.activeConnection);
        if (pendingShared && pendingShared.length) this.queueRemoteSharedAssets(pendingShared);
      }catch(_){ }
      // Drop stale heavy preview tasks from previous chat to keep switching stable.
      this._attachmentPreviewQueue = [];
      try{ localStorage.setItem('liber_last_chat_conn', this.activeConnection || ''); }catch(_){ }
      // Never block switching on metadata fetch; render immediately from cached connection data.
      let activeConnData = (this.connections || []).find((c)=> c && c.id === this.activeConnection) || null;
      this._activeConnReadBy = (activeConnData && typeof activeConnData.readBy === 'object') ? activeConnData.readBy : {};
      if (!displayName){
        displayName = this.getConnectionDisplayName(activeConnData || {}) || 'Chat';
      }
      this.updateChatScopeUI(activeConnData);
      document.getElementById('active-connection-name').textContent = displayName;
      const topTitle = document.getElementById('chat-top-title');
      if (topTitle) topTitle.textContent = displayName;
      try{
        const peerUid = await this.getPeerUidForConn(this.activeConnection);
        const isPersonal = !!peerUid;
        const titleEl = document.getElementById('active-connection-name');
        const topTitleEl = document.getElementById('chat-top-title');
        [titleEl, topTitleEl].forEach((el)=>{
          if (!el) return;
          if (isPersonal){
            el.dataset.userPreview = peerUid;
            el.style.cursor = 'pointer';
            el.title = 'Open profile';
          }else{
            delete el.dataset.userPreview;
            el.style.cursor = '';
            el.title = '';
          }
        });
      }catch(_){ }
      if (this.isMobileViewport()) this.setMobileMenuOpen(false);
      this._activeConnOpenedAt = Date.now();
      if (this._readMarkTimer){ clearTimeout(this._readMarkTimer); this._readMarkTimer = null; }
      this._readMarkTimer = setTimeout(()=>{
        try{
          if (this.activeConnection !== (resolvedConnId || connId)) return;
          this.markConnectionReadAfterDelay(this.activeConnection).catch(()=>{});
        }catch(_){ }
      }, 5000);
      this.startTypingListener(this.activeConnection);
      try{
        const box = document.getElementById('messages');
        if (box){
          box.dataset.renderedConnId = '';
          this._lastDayByConn?.delete(resolvedConnId || connId);
          box.innerHTML = '<div style="opacity:.75;padding:10px 2px">Loading messages…</div>';
        }
      }catch(_){ }
      this.loadMessages().catch(()=>{});
      this.applyAdminBadgeForConn(this.activeConnection);
      Promise.resolve().then(async ()=>{
        try{
          if (setSeq !== this._setActiveSeq || this.activeConnection !== (resolvedConnId || connId)) return;
          const snapMeta = await firebase.getDoc(firebase.doc(this.db,'chatConnections', this.activeConnection));
          if (setSeq !== this._setActiveSeq || this.activeConnection !== (resolvedConnId || connId)) return;
          if (!snapMeta.exists()) return;
          const data = snapMeta.data() || {};
          this._activeConnReadBy = (data && typeof data.readBy === 'object') ? data.readBy : {};
          this.updateChatScopeUI(data);
          this.applyAdminBadgeForConn(this.activeConnection, data);
          if (!displayName || displayName === 'Chat'){
            const parts = this.getConnParticipants(data);
            const stored = Array.isArray(data.participantUsernames) ? data.participantUsernames : [];
            const names = parts.map((uid, i)=> this.usernameCache.get(uid) || stored[i] || uid);
            const myNameLower = (this.me?.username || '').toLowerCase();
            const others = names.filter((n)=> String(n ?? '').toLowerCase() !== myNameLower);
            const resolvedName = others.length === 1 ? others[0] : (others.slice(0,2).join(', ') + (others.length > 2 ? `, +${others.length-2}` : ''));
            const finalName = resolvedName || 'Chat';
            const titleEl = document.getElementById('active-connection-name');
            if (titleEl) titleEl.textContent = finalName;
            const topTitleEl = document.getElementById('chat-top-title');
            if (topTitleEl) topTitleEl.textContent = finalName;
          }
        }catch(_){ }
      });
      if (setSeq !== this._setActiveSeq) return;
      // If current user is not a participant of this connection, show banner to recreate with same users
      Promise.resolve().then(async ()=>{
        try{
          const seqNow = this._setActiveSeq;
          const connNow = this.activeConnection;
          const snap = await firebase.getDoc(firebase.doc(this.db,'chatConnections', connNow));
          if (seqNow !== this._setActiveSeq || connNow !== this.activeConnection) return;
          if (snap.exists()){
            const data = snap.data();
            const parts = Array.isArray(data.participants)
              ? data.participants
              : (Array.isArray(data.users) ? data.users : (Array.isArray(data.memberIds) ? data.memberIds : []));
            const header = document.querySelector('.chat-header');
            const existing = document.getElementById('chat-access-banner');
            if (!parts.includes(this.currentUser.uid)){
              if (!existing && header){
                const bar = document.createElement('div');
                bar.id='chat-access-banner';
                bar.style.cssText='background:#2a2f36;color:#fff;padding:8px 12px;border-bottom:1px solid #3a404a;display:flex;gap:10px;align-items:center';
                const msg = document.createElement('div'); msg.textContent='You are not a participant of this chat. Recreate a new chat with the same users to start messaging.'; bar.appendChild(msg);
                const btn = document.createElement('button'); btn.className='btn btn-secondary'; btn.textContent='Recreate chat';
                btn.onclick = async ()=>{
                  try{
                    const participants = parts.slice(); const names = (data.participantUsernames||[]).slice();
                    if (!participants.includes(this.currentUser.uid)){ participants.push(this.currentUser.uid); names.push((this.me&&this.me.username)||this.currentUser.email||'me'); }
                    const newKey = this.computeConnKey(participants);
                    let newId = await this.findConnectionByKey(newKey);
                    if (!newId){
                      const ref = firebase.doc(this.db,'chatConnections', newKey);
                      await firebase.setDoc(ref,{ id:newKey, key:newKey, participants, participantUsernames:names, admins:[this.currentUser.uid], createdAt:new Date().toISOString(), updatedAt:new Date().toISOString(), lastMessage:'' });
                      newId = newKey;
                    }
                    await this.loadConnections(); this.setActive(newId);
                  }catch(_){ }
                };
                bar.appendChild(btn);
                header.parentNode.insertBefore(bar, header.nextSibling);
              }
            } else if (existing){ existing.remove(); }
          }
        }catch(_){ }
      });
      // refresh group panel if open
      const gp = document.getElementById('group-panel'); if (gp){ await this.renderGroupPanel(); }
    }

    applyAdminBadgeForConn(connId, connData = null){
      try{
        const titleBar = document.getElementById('active-connection-name');
        if (!titleBar) return;
        const badgeId = 'admin-badge';
        let badge = document.getElementById(badgeId);
        const apply = (admins)=>{
          if (!badge){
            badge = document.createElement('span');
            badge.id = badgeId;
            badge.style.cssText = 'margin-left:8px;font-size:12px;opacity:.8';
            if (titleBar.parentElement) titleBar.parentElement.appendChild(badge);
          }
          const isAdmin = Array.isArray(admins) && admins.includes(this.currentUser?.uid);
          badge.textContent = isAdmin ? '(Admin)' : '';
        };
        if (connData && typeof connData === 'object'){
          apply(Array.isArray(connData.admins) ? connData.admins : []);
          return;
        }
        if (!connId || !this.db) return;
        firebase.getDoc(firebase.doc(this.db, 'chatConnections', connId)).then((snap)=>{
          if (snap.exists()){
            const data = snap.data() || {};
            apply(Array.isArray(data.admins) ? data.admins : []);
          }
        }).catch(()=>{});
      }catch(_){ }
    }

    updateChatScopeUI(connData){
      const parts = this.getConnParticipants(connData || {});
      const isPersonal = parts.length <= 2;
      this._isPersonalChat = isPersonal;
      const groupBtn = document.getElementById('group-menu-btn');
      const mobileGroupBtn = document.getElementById('mobile-group-menu-btn');
      if (groupBtn) groupBtn.style.display = isPersonal ? 'none' : '';
      if (mobileGroupBtn) mobileGroupBtn.style.display = isPersonal ? 'none' : '';
    }

    async loadMessages(){
      const box = document.getElementById('messages');
      if (!box) return;
      if (!this.activeConnection) return;
      if (!this.db) {
        box.innerHTML = '<div class="error">Firestore not ready. Please refresh.</div>';
        return;
      }
      const activeConnId = this.activeConnection;
      const visibleLimit = 500;
      const activeConnData = (this.connections || []).find((c)=> c && c.id === activeConnId) || null;
      const readMarkerMs = this.getEffectiveReadMarkerForConn(activeConnId, activeConnData);
      this._msgLoadSeq = (this._msgLoadSeq || 0) + 1;
      const loadSeq = this._msgLoadSeq;
      this._attachmentPreviewQueue = [];
      let loadFinished = false;
      let loadWatchdog = null;
      let hardGuardTimer = null;
      // Ensure a persistent scroll-to-latest affordance exists.
      let toBottomBtn = document.getElementById('chat-scroll-bottom-btn');
      if (!toBottomBtn){
        toBottomBtn = document.createElement('button');
        toBottomBtn.id = 'chat-scroll-bottom-btn';
        toBottomBtn.className = 'btn secondary';
        toBottomBtn.textContent = '↓';
        toBottomBtn.title = 'Scroll to latest';
        toBottomBtn.style.cssText = 'position:fixed;right:16px;bottom:84px;z-index:40;display:none;width:34px;height:34px;border-radius:17px;padding:0;font-size:18px;line-height:34px;text-align:center';
        const main = document.querySelector('.main') || document.body;
        main.appendChild(toBottomBtn);
      }
      const placeToBottomBtn = ()=>{
        const mobile = this.isMobileViewport();
        toBottomBtn.style.bottom = mobile
          ? 'calc(136px + env(safe-area-inset-bottom))'
          : '84px';
      };
      placeToBottomBtn();
      const updateBottomUi = ()=>{
        // column-reverse chat: end-of-chat is near scrollTop = 0.
        const pinned = box.scrollTop <= 36;
        box.dataset.pinnedBottom = pinned ? '1' : '0';
        const canShow = box.childElementCount > 5;
        toBottomBtn.style.display = (!pinned && canShow) ? 'inline-block' : 'none';
      };
      const maybeLoadOlder = async ()=>{
        const connId = this.activeConnection;
        if (this._loadMoreOlderInFlight || !connId) return;
        if (!this._hasMoreOlderByConn.get(connId)) return;
        const lastDoc = this._lastOldestDocSnapshotByConn.get(connId);
        if (!lastDoc) return;
        const nearTop = box.scrollHeight - box.clientHeight - box.scrollTop <= 120;
        if (!nearTop) return;
        if (box.dataset.renderedConnId !== connId) return;
        this._loadMoreOlderInFlight = true;
        try{
          let qOlder;
          try{
            qOlder = firebase.query(
              firebase.collection(this.db,'chatMessages',connId,'messages'),
              firebase.orderBy('createdAtTS','desc'),
              firebase.limit(200),
              firebase.startAfter(lastDoc)
            );
          }catch(_){
            qOlder = firebase.query(
              firebase.collection(this.db,'chatMessages',connId,'messages'),
              firebase.orderBy('createdAt','desc'),
              firebase.limit(200),
              firebase.startAfter(lastDoc)
            );
          }
          const snapOlder = await firebase.getDocs(qOlder);
          const rawOlder = snapOlder.docs || [];
          if (rawOlder.length === 0){
            this._hasMoreOlderByConn.set(connId, false);
            return;
          }
          this._lastOldestDocSnapshotByConn.set(connId, rawOlder[rawOlder.length - 1]);
          this._hasMoreOlderByConn.set(connId, rawOlder.length >= 200);
          const prevScrollTop = box.scrollTop;
          const prevScrollHeight = box.scrollHeight;
          const appendContext = { lastRenderedDay: '' };
          const lastMsgEl = box.lastElementChild?.classList?.contains('message') ? box.lastElementChild : null;
          if (lastMsgEl?.dataset?.msgTs){
            const ts = Number(lastMsgEl.dataset.msgTs || 0);
            if (ts) appendContext.lastRenderedDay = this.formatMessageDay({ createdAt: new Date(ts) }, {});
          }
          const olderAsDocs = rawOlder.map((d)=> ({ id: d.id, data: d.data ? d.data() : {}, sourceConnId: connId }));
          const renderOne = currentRenderOneForLoadMore;
          if (!renderOne) return;
          for (let i = olderAsDocs.length - 1; i >= 0; i--){
            const d = olderAsDocs[i];
            if (loadSeq !== this._msgLoadSeq || this.activeConnection !== connId) break;
            try{ await renderOne(d, connId, { forceAppend: true, appendContext }); }catch(_){}
            if ((olderAsDocs.length - 1 - i) % 3 === 2) await this.yieldToUi();
          }
          const heightAdded = box.scrollHeight - prevScrollHeight;
          box.scrollTop = prevScrollTop + heightAdded;
          this.applyNewMessagesSeparator(box);
        }finally{ this._loadMoreOlderInFlight = false; }
      };
      if (!box._bottomUiBound){
        box._bottomUiBound = true;
        box.addEventListener('scroll', ()=>{
          updateBottomUi();
          if (loadFinished && !this._loadMoreOlderInFlight) maybeLoadOlder();
        }, { passive: true });
        window.addEventListener('resize', placeToBottomBtn, { passive: true });
        toBottomBtn.addEventListener('click', ()=>{ box.scrollTop = 0; updateBottomUi(); });
      }
      try{
        if (this._unsubMessages) { this._unsubMessages(); this._unsubMessages = null; }
        if (this._msgPoll) { clearInterval(this._msgPoll); this._msgPoll = null; }
        if (this._scheduleLiveSnapTimer) { clearTimeout(this._scheduleLiveSnapTimer); this._scheduleLiveSnapTimer = null; }
        // Keep switching stable: avoid expensive merged-thread fanout queries on each live update.
        let relatedConnIds = [activeConnId];
        try{
          const related = await this.getRelatedConnIds(activeConnId);
          if (Array.isArray(related) && related.length) relatedConnIds = related;
        }catch(_){}
        let q;
        try{
          q = firebase.query(
            firebase.collection(this.db,'chatMessages',activeConnId,'messages'),
            firebase.orderBy('createdAtTS','desc'),
            firebase.limit(visibleLimit)
          );
        }catch(_){
          q = firebase.query(
            firebase.collection(this.db,'chatMessages',activeConnId,'messages'),
            firebase.orderBy('createdAt','desc'),
            firebase.limit(visibleLimit)
          );
        }
        const fetchLatestSnap = async ()=>{
          try{
            const qTs = firebase.query(
              firebase.collection(this.db,'chatMessages',activeConnId,'messages'),
              firebase.orderBy('createdAtTS','desc'),
              firebase.limit(visibleLimit)
            );
            return await firebase.getDocs(qTs);
          }catch(_){
            try{
              const qIso = firebase.query(
                firebase.collection(this.db,'chatMessages',activeConnId,'messages'),
                firebase.orderBy('createdAt','desc'),
                firebase.limit(visibleLimit)
              );
              return await firebase.getDocs(qIso);
            }catch(_){
              // Last resort for missing indexes/transient query failures.
              const qLoose = firebase.query(
                firebase.collection(this.db,'chatMessages',activeConnId,'messages'),
                firebase.limit(visibleLimit)
              );
              return await firebase.getDocs(qLoose);
            }
          }
        };
        const fetchLatestSnapWithTimeout = async (timeoutMs = 8000)=>{
          return await Promise.race([
            fetchLatestSnap(),
            new Promise((_, reject)=> setTimeout(()=> reject(new Error('fetch-timeout')), timeoutMs))
          ]);
        };
        // Keep live rendering stable on canonical active connection only.
        const keyByConn = new Map();
        const getKeyForConn = async (cid)=>{
          if (keyByConn.has(cid)) return keyByConn.get(cid);
          let k = null;
          try{ k = await this.getFallbackKeyForConn(cid); }catch(_){ k = null; }
          keyByConn.set(cid, k);
          return k;
        };
        const fetchDocsForConn = async (cid)=>{
          try{
            let q2;
            try{
              q2 = firebase.query(
                firebase.collection(this.db,'chatMessages',cid,'messages'),
                firebase.orderBy('createdAtTS','desc'),
                firebase.limit(visibleLimit)
              );
            }catch(_){
              q2 = firebase.query(
                firebase.collection(this.db,'chatMessages',cid,'messages'),
                firebase.orderBy('createdAt','desc'),
                firebase.limit(visibleLimit)
              );
            }
            const s2 = await firebase.getDocs(q2);
            return (s2.docs || []).map((d)=> ({ id: d.id, data: d.data() || {}, sourceConnId: cid }));
          }catch(e){
            if (e?.code === 'permission-denied') return [];
            return [];
          }
        };
        const normalizeDocTime = (m)=> this.getMessageTimestampMs(m);
        let currentRenderOneForLoadMore = null;
        const handleSnap = async (snap, fromLive = false)=>{
          try{
            if (loadSeq !== this._msgLoadSeq || this.activeConnection !== activeConnId) return;
            const suppressUntil = Number(this._suppressLivePatchUntilByConn.get(activeConnId) || 0);
            if (fromLive && Date.now() < suppressUntil){
              return;
            }
            const renderedConnId = String(box.dataset.renderedConnId || '');
            const docsPrimary = (snap.docs || []).map((d)=> ({ id: d.id, data: d.data() || {}, sourceConnId: activeConnId }));
            const extraIds = (relatedConnIds || []).filter((cid)=> cid && cid !== activeConnId);
            if (renderedConnId === activeConnId && extraIds.length === 0){
              const haveIds = new Set([...box.querySelectorAll('[data-msg-id]')].map(el=> el.getAttribute('data-msg-id')));
              const newDocs = docsPrimary.filter(d=> !haveIds.has(String(d.id)));
              if (newDocs.length === 0){
                loadFinished = true;
                if (loadWatchdog){ clearTimeout(loadWatchdog); loadWatchdog = null; }
                if (hardGuardTimer){ clearTimeout(hardGuardTimer); hardGuardTimer = null; }
                updateBottomUi();
                return;
              }
              let lastRenderedDay = this._lastDayByConn.get(activeConnId) || '';
              const renderOneAppend = async (d, sourceConnId = activeConnId, opts = {})=>{
                if (loadSeq !== this._msgLoadSeq || this.activeConnection !== activeConnId) return;
                const m=(typeof d.data === 'function' ? d.data() : d.data) || {};
                const aesKey = await getKeyForConn(sourceConnId);
                let text='';
                if (typeof m.text === 'string' && !m.cipher){ text = m.text; } else {
                  try{ text = await chatCrypto.decryptWithKey(m.cipher, aesKey); }catch(_){
                    let ok = false;
                    try{ const candidates = await this.getFallbackKeyCandidatesForConn(sourceConnId);
                      for (const k of candidates){ try{ text = await chatCrypto.decryptWithKey(m.cipher, k); ok = true; break; }catch(_){ } }
                    }catch(_){ }
                    if (!ok){ try { const ecdh = await this.getOrCreateSharedAesKey(); text = await chatCrypto.decryptWithKey(m.cipher, ecdh);} catch(_){ text='[unable to decrypt]'; } }
                  }
                }
                const el = document.createElement('div');
                el.className='message '+(m.sender===this.currentUser.uid?'self':'other');
                el.dataset.msgId = String(d.id || m.id || '');
                const msgTs = this.getMessageTimestampMs(m);
                el.dataset.msgTs = String(msgTs || 0);
                if (m.sender !== this.currentUser.uid && msgTs > readMarkerMs) el.dataset.unread = '1';
                if (m.systemType === 'connection_request_intro') el.classList.add('message-system', 'message-connection-request');
                let senderName = m.sender === this.currentUser.uid ? 'You' : this.usernameCache.get(m.sender) || m.sender.slice(0,8);
                if (!this.usernameCache.has(m.sender) && !this._senderLookupInFlight.has(m.sender)) {
                  this._senderLookupInFlight.add(m.sender);
                  Promise.resolve().then(async ()=>{ try { const user = await window.firebaseService.getUserData(m.sender); this.usernameCache.set(m.sender, (user?.username || user?.email || m.sender.slice(0,8))); } catch (_) { this.usernameCache.set(m.sender, senderName || 'Unknown'); } finally { this._senderLookupInFlight.delete(m.sender); } });
                }
                const inferredFileName = this.inferAttachmentFileName(m, text);
                const hasMedia = Array.isArray(m.media) && m.media.length > 0;
                const hasFile = !!m.fileUrl && !hasMedia;
                const previewOnlyFile = this.isAudioFilename(inferredFileName) || this.isVideoFilename(inferredFileName) || this.isImageFilename(inferredFileName);
                const cleanedText = this.stripPlaceholderText(text);
                const isMediaOnlyMessage = (hasFile || hasMedia) && !cleanedText && (hasFile ? previewOnlyFile : true);
                if (isMediaOnlyMessage) el.classList.add('message-media-only');
                let bodyHtml = this.renderText(cleanedText);
                if (m.sharedAsset && typeof m.sharedAsset === 'object') bodyHtml = this.renderSharedAssetCardHtml(m.sharedAsset, d.id || m.id || '');
                const callMatch = /^\[call:(voice|video):([A-Za-z0-9_\-]+)\]$/.exec(text);
                if (callMatch) bodyHtml = `<button class="btn secondary" data-call-id="${callMatch[2]}" data-kind="${callMatch[1]}">${callMatch[1]==='voice'?'Join voice call':'Join video call'}</button>`;
                const gifMatch = /^\[gif\]\s+(https?:\/\/\S+)$/i.exec(text || '');
                if (gifMatch) bodyHtml = `<img src="${gifMatch[1]}" alt="gif" style="max-width:100%;border-radius:8px" />`;
                const stickerDataMatch = /^\[sticker-data\](data:image\/[a-zA-Z0-9.+-]+;base64,[A-Za-z0-9+/=]+)$/i.exec(text || '');
                if (stickerDataMatch) bodyHtml = `<img src="${stickerDataMatch[1]}" alt="sticker" style="max-width:100%;border-radius:8px" />`;
                const canModify = m.sender === this.currentUser.uid;
                const sharePayload = this.buildSharePayload(text, m.fileUrl, inferredFileName, sourceConnId, m.attachmentKeySalt || '', { ...m, id: d.id || m.id }, senderName);
                const dayLabel = this.formatMessageDay(m.createdAt, m);
                if (dayLabel !== lastRenderedDay){ const sep = document.createElement('div'); sep.className = 'message-day-separator'; sep.textContent = dayLabel; box.insertBefore(sep, box.firstElementChild); lastRenderedDay = dayLabel; }
                const systemBadge = m.systemType === 'connection_request_intro' ? '<span class="system-chip">Connection request</span>' : '';
                const editedBadge = this.isEditedMessage(m) ? ' · <span class="msg-edited-badge">edited</span>' : '';
                const isShared = !!(m.isShared || m.sharedFromMessageId || m.sharedOriginalAuthorUid || m.sharedOriginalAuthorName);
                const originalAuthorName = String(m.sharedOriginalAuthorName || '').trim();
                const isWaveConnectShare = isShared && m.sharedAsset && ['audio','video','image','picture','post'].includes(String(m.sharedAsset?.kind||'').toLowerCase());
        const repostBadge = isShared ? (isWaveConnectShare ? ' · <span class="msg-repost-badge">Repost from WaveConnect</span>' : ' · <span class="msg-repost-badge">repost</span>') : '';
                const originalSignature = isShared ? ` · <span class="msg-original-author">by ${(originalAuthorName || 'original author').replace(/</g,'&lt;')}</span>` : '';
                const delivery = this.getDeliveryLabel(m);
                const deliveryTxt = delivery ? ` · <span class="msg-delivery">${String(delivery)}</span>` : '';
                const mediaBlockHtml = hasMedia ? this.getMessageMediaBlockHtml(m) : '';
                const timeStr = String(this.formatMessageTime(m.createdAt, m) || '');
                const isVoiceMsgAppend = hasFile && this.isVoiceRecordingMessage(m, inferredFileName);
                const senderStr = isVoiceMsgAppend ? '' : String(senderName || '');
                const metaSepAppend = senderStr ? `${senderStr} · ` : '';
                el.innerHTML = `${mediaBlockHtml}<div class="msg-text">${bodyHtml}</div>${hasFile?`${previewOnlyFile ? '' : `<div class="file-link">${inferredFileName || 'Attachment'}</div>`}<div class="file-preview"><span class="attachment-loading" style="font-size:11px;opacity:.6">Loading…</span></div>`:''}<div class="meta">${systemBadge}${metaSepAppend}${timeStr}${editedBadge}${repostBadge}${originalSignature}${deliveryTxt}${canModify?` · <span class="msg-actions" data-mid="${d.id || m.id}" style="cursor:pointer"><i class="fas fa-edit" title="Edit"></i> <i class="fas fa-trash" title="Delete"></i> <i class="fas fa-paperclip" title="Replace file"></i></span>`:''} · <span class="msg-share" style="cursor:pointer" title="Share"><i class="fas fa-share-nodes"></i></span></div>`;
                box.insertBefore(el, box.firstElementChild);
                const joinBtn = el.querySelector('button[data-call-id]');
                if (joinBtn) joinBtn.addEventListener('click', ()=> this.joinOrStartCall({ video: joinBtn.dataset.kind === 'video' }));
                if (hasMedia){
                  const attachmentSourceConnId = this.resolveAttachmentSourceConnId(m, sourceConnId);
                  const attachmentAesKey = await getKeyForConn(attachmentSourceConnId);
                  m.media.forEach((mediaItem, idx)=>{
                    const container = el.querySelector(`.msg-media-item[data-media-index="${idx}"] .file-preview`);
                    if (container){
                      const msgProxy = { ...m, fileUrl: mediaItem.fileUrl, fileName: mediaItem.fileName, attachmentKeySalt: mediaItem.attachmentKeySalt, isVideoRecording: mediaItem.isVideoRecording ?? m.isVideoRecording, isVoiceRecording: mediaItem.isVoiceRecording ?? m.isVoiceRecording, text, id: d.id };
                      this.enqueueAttachmentPreview(()=>{
                        const c = box.querySelector(`[data-msg-id="${String(d.id||m.id||'').replace(/"/g,'\\"')}"] .msg-media-item[data-media-index="${idx}"] .file-preview`);
                        if (c?.isConnected) this.renderEncryptedAttachment(c, mediaItem.fileUrl, mediaItem.fileName, attachmentAesKey, attachmentSourceConnId, senderName, msgProxy);
                      }, loadSeq, activeConnId);
                    }
                  });
                  try{ const dm = window.dashboardManager || window.top?.dashboardManager || window.parent?.dashboardManager; if (dm && typeof dm.activatePlayers === 'function') dm.activatePlayers(el); }catch(_){ }
                  this.activateChatPlayers(el);
                }
                if (hasFile && el.querySelector('.file-preview')){
                  const attachmentSourceConnId = this.resolveAttachmentSourceConnId(m, sourceConnId);
                  const attachmentAesKey = await getKeyForConn(attachmentSourceConnId);
                  const preview = el.querySelector('.file-preview');
                  const isRecording = this.isVideoRecordingMessage(m, inferredFileName) || this.isVoiceRecordingMessage(m, inferredFileName);
                  const isPriorityMedia = isRecording || this.isAudioFilename(inferredFileName);
                  if (isPriorityMedia && preview){
                    try{ await this.renderEncryptedAttachment(preview, m.fileUrl, inferredFileName, attachmentAesKey, attachmentSourceConnId, senderName, { ...m, text, id: d.id }); }catch(e){}
                  }else{
                    this.enqueueAttachmentPreview(()=>{
                      const container = box.querySelector(`[data-msg-id="${String(d.id||m.id||'').replace(/"/g,'\\"')}"] .file-preview`);
                      if (container?.isConnected) this.renderEncryptedAttachment(container, m.fileUrl, inferredFileName, attachmentAesKey, attachmentSourceConnId, senderName, { ...m, text });
                    }, loadSeq, activeConnId);
                  }
                }
                if (m.sharedAsset && typeof m.sharedAsset === 'object') this.bindSharedAssetCardInteractions(el, m.sharedAsset);
                if (canModify){ const actions = el.querySelector('.msg-actions'); if (actions){ const mid = actions.getAttribute('data-mid'); const icons = actions.querySelectorAll('i'); icons[0].onclick = async ()=>{ const next = prompt('Edit:', el.querySelector('.msg-text')?.textContent || ''); if (next===null) return; await firebase.updateDoc(firebase.doc(this.db,'chatMessages',activeConnId,'messages', mid),{ cipher: await chatCrypto.encryptWithKey(next, await this.getFallbackKey()), updatedAt: new Date().toISOString() }); }; icons[1].onclick = async ()=>{ if (!confirm('Delete?')) return; await this.dissolveOutRemove(el, 220); await firebase.deleteDoc(firebase.doc(this.db,'chatMessages',activeConnId,'messages', mid)); }; icons[2].onclick = ()=>{ const p = document.createElement('input'); p.type='file'; p.style.display='none'; document.body.appendChild(p); p.onchange = async ()=>{ try{ const f = p.files[0]; if (!f) return; const aesKey2 = await this.getFallbackKey(); const base64 = await new Promise((r,e)=>{ const fr = new FileReader(); fr.onload=()=>r(String(fr.result||'').split(',')[1]); fr.onerror=e; fr.readAsDataURL(f); }); const cipherF = await chatCrypto.encryptWithKey(base64, aesKey2); const blob = new Blob([JSON.stringify(cipherF)], {type:'application/json'}); const sref = firebase.ref(this.storage, `chat/${activeConnId}/${Date.now()}_${f.name.replace(/[^a-zA-Z0-9._-]/g,'_')}.enc.json`); await firebase.uploadBytes(sref, blob, { contentType: 'application/json' }); await firebase.updateDoc(firebase.doc(this.db,'chatMessages',activeConnId,'messages', mid),{ fileUrl: await firebase.getDownloadURL(sref), fileName: f.name, updatedAt: new Date().toISOString() }); }catch(_){ alert('Failed'); } finally{ document.body.removeChild(p); } }; p.click(); }; }; }
                const shareBtn = el.querySelector('.msg-share'); if (shareBtn) shareBtn.onclick = ()=> this.openShareMessageSheet(sharePayload);
              };
              newDocs.sort((a,b)=>{ const ta = normalizeDocTime(a.data); const tb = normalizeDocTime(b.data); return ta !== tb ? ta - tb : String(a.id).localeCompare(String(b.id)); });
              for (const d of newDocs){ try{ await renderOneAppend(d, d.sourceConnId || activeConnId); }catch(_){ } }
              this._lastDocIdsByConn.set(activeConnId, docsPrimary.map(x=> x.id));
              this._lastDayByConn.set(activeConnId, lastRenderedDay);
              updateBottomUi();
              this.applyNewMessagesSeparator(box);
              loadFinished = true;
              if (loadWatchdog){ clearTimeout(loadWatchdog); loadWatchdog = null; }
              if (hardGuardTimer){ clearTimeout(hardGuardTimer); hardGuardTimer = null; }
              return;
            }
            let merged = docsPrimary.slice();
            const extraIdsInit = (relatedConnIds || []).filter((cid)=> cid && cid !== activeConnId);
            if (extraIdsInit.length){
              const extraSets = await Promise.all(extraIdsInit.map((cid)=> fetchDocsForConn(cid)));
              extraSets.forEach((rows)=> merged.push(...rows));
            }
            merged.sort((a,b)=>{
              const ta = normalizeDocTime(a.data);
              const tb = normalizeDocTime(b.data);
              if (ta !== tb) return ta - tb;
              return String(a.id || '').localeCompare(String(b.id || ''));
            });
            const docs = merged;
            let latestPeerMs = 0;
            docs.forEach((row)=>{
              try{
                const m = (typeof row.data === 'function' ? row.data() : row.data) || {};
                if (m.sender && m.sender !== this.currentUser?.uid){
                  const ts = this.getMessageTimestampMs(m);
                  if (ts > latestPeerMs) latestPeerMs = ts;
                }
              }catch(_){ }
            });
            this._latestPeerMessageMsByConn.set(activeConnId, latestPeerMs);
            let lastRenderedDay = this._lastDayByConn.get(activeConnId) || '';
            const prevIds = this._lastDocIdsByConn.get(activeConnId) || [];
            const isFirstPaint = renderedConnId !== activeConnId;
            const sigBase = [...docs].sort((a,b)=> String(a.sourceConnId+':'+a.id).localeCompare(String(b.sourceConnId+':'+b.id))).map(d=> `${d.sourceConnId}:${d.id}`).join('|');
            const sig = `${activeConnId}::${sigBase}`;
            if (this._lastRenderSigByConn.get(activeConnId) === sig && renderedConnId === activeConnId){
              loadFinished = true;
              if (loadWatchdog){ clearTimeout(loadWatchdog); loadWatchdog = null; }
              if (hardGuardTimer){ clearTimeout(hardGuardTimer); hardGuardTimer = null; }
              updateBottomUi();
              return;
            }
            this._lastRenderSigByConn.set(activeConnId, sig);
            const prefixMatch = prevIds.length > 0 && docs.length >= prevIds.length && prevIds.every((id, i)=> docs[i] && docs[i].id === id);
            const suffixMatch = prevIds.length > 0 && docs.length >= prevIds.length && prevIds.every((id, i)=> docs[docs.length - prevIds.length + i] && docs[docs.length - prevIds.length + i].id === id);
            const appendOnly = renderedConnId === activeConnId && extraIdsInit.length === 0 && (prefixMatch || suffixMatch);
            // Do NOT return here when !appendOnly – we must re-render to show new messages (fixes missing messages in admin/merged chats).
            let renderTarget = box;
            if (!appendOnly){
              if (isFirstPaint){
                box.innerHTML='';
                renderTarget = box;
              }else{
                renderTarget = document.createElement('div');
              }
              lastRenderedDay = '';
              this._voiceWidgets.clear();
            }
            const renderOne = async (d, sourceConnId = activeConnId, opts = {})=>{
              const forceInsertBefore = !!opts.forceInsertBefore;
              const forceAppend = !!opts.forceAppend;
              const replaceEl = opts.replaceEl || null;
              if (loadSeq !== this._msgLoadSeq || this.activeConnection !== activeConnId) return;
              const m=(typeof d.data === 'function' ? d.data() : d.data) || {};
              const aesKey = await getKeyForConn(sourceConnId);
              let text='';
              if (typeof m.text === 'string' && !m.cipher){
                text = m.text;
              } else {
                try{
                  text = await chatCrypto.decryptWithKey(m.cipher, aesKey);
                }catch(_){
                  let ok = false;
                  try{
                    const candidates = await this.getFallbackKeyCandidatesForConn(sourceConnId);
                    for (const k of candidates){
                      try{
                        text = await chatCrypto.decryptWithKey(m.cipher, k);
                        ok = true;
                        break;
                      }catch(_){ }
                    }
                  }catch(_){ }
                  if (!ok){
                    try { const ecdh = await this.getOrCreateSharedAesKey(); text = await chatCrypto.decryptWithKey(m.cipher, ecdh);}
                    catch(_){ text='[unable to decrypt]'; }
                  }
                }
              }
              const el = document.createElement('div');
              el.className='message '+(m.sender===this.currentUser.uid?'self':'other');
              el.dataset.msgId = String(d.id || m.id || '');
              const msgTs = this.getMessageTimestampMs(m);
              el.dataset.msgTs = String(msgTs || 0);
              if (m.sender !== this.currentUser.uid && msgTs > readMarkerMs) el.dataset.unread = '1';
              if (m.systemType === 'connection_request_intro'){
                el.classList.add('message-system', 'message-connection-request');
              }
              // Resolve sender name async
              let senderName = m.sender === this.currentUser.uid ? 'You' : this.usernameCache.get(m.sender) || m.sender.slice(0,8);
              if (!this.usernameCache.has(m.sender) && !this._senderLookupInFlight.has(m.sender)) {
                this._senderLookupInFlight.add(m.sender);
                Promise.resolve().then(async ()=>{
                  try {
                    const user = await window.firebaseService.getUserData(m.sender);
                    const resolved = (user?.username || user?.email || m.sender.slice(0,8));
                    this.usernameCache.set(m.sender, resolved);
                  } catch (_err) {
                    this.usernameCache.set(m.sender, senderName || 'Unknown');
                  } finally {
                    this._senderLookupInFlight.delete(m.sender);
                  }
                });
              }
              const inferredFileName = this.inferAttachmentFileName(m, text);
              const hasMedia = Array.isArray(m.media) && m.media.length > 0;
              const hasFile = !!m.fileUrl && !hasMedia;
              const previewOnlyFile = this.isAudioFilename(inferredFileName) || this.isVideoFilename(inferredFileName) || this.isImageFilename(inferredFileName);
            // Render call invites as buttons
              const cleanedText = this.stripPlaceholderText(text);
              const isMediaOnlyMessage = (hasFile || hasMedia) && !cleanedText && (hasFile ? previewOnlyFile : true);
              if (isMediaOnlyMessage) el.classList.add('message-media-only');
              let bodyHtml = this.renderText(cleanedText);
              if (m.sharedAsset && typeof m.sharedAsset === 'object') bodyHtml = this.renderSharedAssetCardHtml(m.sharedAsset, d.id || m.id || '');
              const callMatch = /^\[call:(voice|video):([A-Za-z0-9_\-]+)\]$/.exec(text);
              if (callMatch){
                const kind = callMatch[1]; const callId = callMatch[2];
                const btnLabel = kind==='voice' ? 'Join voice call' : 'Join video call';
                bodyHtml = `<button class=\"btn secondary\" data-call-id=\"${callId}\" data-kind=\"${kind}\">${btnLabel}</button>`;
              }
              const gifMatch = /^\[gif\]\s+(https?:\/\/\S+)$/i.exec(text || '');
              if (gifMatch){
                bodyHtml = `<img src="${gifMatch[1]}" alt="gif" style="max-width:100%;border-radius:8px" />`;
              }
              const stickerDataMatch = /^\[sticker-data\](data:image\/[a-zA-Z0-9.+-]+;base64,[A-Za-z0-9+/=]+)$/i.exec(text || '');
              if (stickerDataMatch){
                bodyHtml = `<img src="${stickerDataMatch[1]}" alt="sticker" style="max-width:100%;border-radius:8px" />`;
              }
              const canModify = m.sender === this.currentUser.uid;
              const sharePayload = this.buildSharePayload(text, m.fileUrl, inferredFileName, sourceConnId, m.attachmentKeySalt || '', { ...m, id: d.id || m.id }, senderName);
              const dayLabel = this.formatMessageDay(m.createdAt, m);
              const effectiveLastDay = (forceAppend || forceInsertBefore) && opts.appendContext ? opts.appendContext.lastRenderedDay : lastRenderedDay;
              if (dayLabel !== effectiveLastDay){
                if (!appendOnly && !forceInsertBefore && effectiveLastDay){
                  const prevSep = document.createElement('div');
                  prevSep.className = 'message-day-separator';
                  prevSep.textContent = effectiveLastDay;
                  renderTarget.appendChild(prevSep);
                }
                if (appendOnly || forceInsertBefore || forceAppend){
                  const sep = document.createElement('div');
                  sep.className = 'message-day-separator';
                  sep.textContent = dayLabel;
                  if (forceAppend) box.insertBefore(sep, box.firstElementChild);
                  else box.insertBefore(sep, box.firstElementChild);
                }
                if ((forceAppend || forceInsertBefore) && opts.appendContext) opts.appendContext.lastRenderedDay = dayLabel;
                else lastRenderedDay = dayLabel;
              }
              const systemBadge = m.systemType === 'connection_request_intro' ? '<span class="system-chip">Connection request</span>' : '';
              const editedBadge = this.isEditedMessage(m) ? ' · <span class="msg-edited-badge">edited</span>' : '';
              const isShared = !!(m.isShared || m.sharedFromMessageId || m.sharedOriginalAuthorUid || m.sharedOriginalAuthorName);
              const originalAuthorName = String(m.sharedOriginalAuthorName || '').trim();
              const isWaveConnectShare = isShared && m.sharedAsset && ['audio','video','image','picture','post'].includes(String(m.sharedAsset?.kind||'').toLowerCase());
        const repostBadge = isShared ? (isWaveConnectShare ? ' · <span class="msg-repost-badge">Repost from WaveConnect</span>' : ' · <span class="msg-repost-badge">repost</span>') : '';
              const originalSignature = isShared ? ` · <span class="msg-original-author">by ${(originalAuthorName || 'original author').replace(/</g,'&lt;')}</span>` : '';
              const delivery = this.getDeliveryLabel(m);
              const deliveryTxt = delivery ? ` · <span class="msg-delivery">${String(delivery)}</span>` : '';
              const mediaBlockHtml2 = hasMedia ? this.getMessageMediaBlockHtml(m) : '';
              const timeStr2 = String(this.formatMessageTime(m.createdAt, m) || '');
              const isVoiceMsg = hasFile && this.isVoiceRecordingMessage(m, inferredFileName);
              const senderStr2 = isVoiceMsg ? '' : String(senderName || '');
              const metaSep = senderStr2 ? `${senderStr2} · ` : '';
              el.innerHTML = `${mediaBlockHtml2}<div class=\"msg-text\">${bodyHtml}</div>${hasFile?`${previewOnlyFile ? '' : `<div class=\"file-link\">${inferredFileName || 'Attachment'}</div>`}<div class=\"file-preview\"><span class="attachment-loading" style="font-size:11px;opacity:.6">Loading…</span></div>`:''}<div class=\"meta\">${systemBadge}${metaSep}${timeStr2}${editedBadge}${repostBadge}${originalSignature}${deliveryTxt}${canModify?` · <span class=\"msg-actions\" data-mid=\"${d.id || m.id}\" style=\"cursor:pointer\"><i class=\"fas fa-edit\" title=\"Edit\"></i> <i class=\"fas fa-trash\" title=\"Delete\"></i> <i class=\"fas fa-paperclip\" title=\"Replace file\"></i></span>`:''} · <span class=\"msg-share\" style=\"cursor:pointer\" title=\"Share to another chat\"><i class=\"fas fa-share-nodes\"></i></span></div>`;
              if (replaceEl){
                box.replaceChild(el, replaceEl);
              } else if (forceAppend){
                box.appendChild(el);
              } else if (appendOnly || forceInsertBefore){
                box.insertBefore(el, box.firstElementChild);
              }else{
                renderTarget.appendChild(el);
              }
              const joinBtn = el.querySelector('button[data-call-id]');
              if (joinBtn){ joinBtn.addEventListener('click', ()=> this.joinOrStartCall({ video: joinBtn.dataset.kind === 'video' })); }
              if (hasMedia){
                const attachmentSourceConnId = this.resolveAttachmentSourceConnId(m, sourceConnId);
                const attachmentAesKey = await getKeyForConn(attachmentSourceConnId);
                m.media.forEach((mediaItem, idx)=>{
                  const container = el.querySelector(`.msg-media-item[data-media-index="${idx}"] .file-preview`);
                  if (container){
                    const msgProxy = { ...m, fileUrl: mediaItem.fileUrl, fileName: mediaItem.fileName, attachmentKeySalt: mediaItem.attachmentKeySalt, isVideoRecording: mediaItem.isVideoRecording ?? m.isVideoRecording, isVoiceRecording: mediaItem.isVoiceRecording ?? m.isVoiceRecording, text, id: d.id };
                    this.enqueueAttachmentPreview(()=>{
                      const c = box.querySelector(`[data-msg-id="${String(d.id||m.id||'').replace(/"/g,'\\"')}"] .msg-media-item[data-media-index="${idx}"] .file-preview`);
                      if (c?.isConnected) this.renderEncryptedAttachment(c, mediaItem.fileUrl, mediaItem.fileName, attachmentAesKey, attachmentSourceConnId, senderName, msgProxy);
                    }, loadSeq, activeConnId);
                  }
                });
                try{ const dm = window.dashboardManager || window.top?.dashboardManager || window.parent?.dashboardManager; if (dm && typeof dm.activatePlayers === 'function') dm.activatePlayers(el); }catch(_){ }
                this.activateChatPlayers(el);
              }
              if (hasFile){
                const preview = el.querySelector('.file-preview');
                if (preview){
                  const attachmentSourceConnId = this.resolveAttachmentSourceConnId(m, sourceConnId);
                  const attachmentAesKey = await getKeyForConn(attachmentSourceConnId);
                  const isRecording = this.isVideoRecordingMessage(m, inferredFileName) || this.isVoiceRecordingMessage(m, inferredFileName);
                  const isPriorityMedia = isRecording || this.isAudioFilename(inferredFileName);
                  if (isPriorityMedia){
                    try{ await this.renderEncryptedAttachment(preview, m.fileUrl, inferredFileName, attachmentAesKey, attachmentSourceConnId, senderName, { ...m, text, id: d.id }); }catch(e){}
                  }else{
                    this.enqueueAttachmentPreview(
                      ()=>{
                        const container = box.querySelector(`[data-msg-id="${String(d.id||m.id||'').replace(/"/g,'\\"')}"] .file-preview`);
                        if (container?.isConnected) this.renderEncryptedAttachment(container, m.fileUrl, inferredFileName, attachmentAesKey, attachmentSourceConnId, senderName, { ...m, text });
                      },
                      loadSeq,
                      activeConnId
                    );
                  }
                }
              }
              if (m.sharedAsset && typeof m.sharedAsset === 'object') this.bindSharedAssetCardInteractions(el, m.sharedAsset);
            // Bind edit/delete/replace for own messages
              if (canModify){
              const actions = el.querySelector('.msg-actions');
              if (actions){
                const mid = actions.getAttribute('data-mid');
                const icons = actions.querySelectorAll('i');
                const editIcon = icons[0];
                const delIcon = icons[1];
                const repIcon = icons[2];
                editIcon.onclick = async ()=>{
                  const current = el.querySelector('.msg-text')?.textContent || '';
                  const next = prompt('Edit message:', current);
                  if (next===null) return;
                  const key = await this.getFallbackKey();
                  const cipher2 = await chatCrypto.encryptWithKey(next, key);
                  await firebase.updateDoc(firebase.doc(this.db,'chatMessages',activeConnId,'messages', mid),{ cipher: cipher2, updatedAt: new Date().toISOString() });
                };
                delIcon.onclick = async ()=>{
                  if (!confirm('Delete this message?')) return;
                  await this.dissolveOutRemove(el, 220);
                  await firebase.deleteDoc(firebase.doc(this.db,'chatMessages',activeConnId,'messages', mid));
                };
                repIcon.onclick = async ()=>{
                  const picker = document.createElement('input'); picker.type='file'; picker.accept='*/*'; picker.style.display='none'; document.body.appendChild(picker);
                  picker.onchange = async ()=>{
                    try{
                      const f = picker.files[0]; if (!f) return;
                      const aesKey2 = await this.getFallbackKey();
                      const base64 = await new Promise((resolve, reject)=>{ const r = new FileReader(); r.onload=()=>{ const s=String(r.result||''); resolve(s.includes(',')?s.split(',')[1]:''); }; r.onerror=reject; r.readAsDataURL(f); });
                      const cipherF = await chatCrypto.encryptWithKey(base64, aesKey2);
                      const blob = new Blob([JSON.stringify(cipherF)], {type:'application/json'});
                      const sref = firebase.ref(this.storage, `chat/${activeConnId}/${Date.now()}_${f.name.replace(/[^a-zA-Z0-9._-]/g,'_')}.enc.json`);
                      await firebase.uploadBytes(sref, blob, { contentType: 'application/json' });
                      const url = await firebase.getDownloadURL(sref);
                      await firebase.updateDoc(firebase.doc(this.db,'chatMessages',activeConnId,'messages', mid),{ fileUrl:url, fileName:f.name, updatedAt: new Date().toISOString() });
                    }catch(_){ alert('Failed to replace file'); }
                    finally{ document.body.removeChild(picker); }
                  };
                  picker.click();
                };
              }
              }
              const shareBtn = el.querySelector('.msg-share');
              if (shareBtn){
                shareBtn.onclick = ()=> this.openShareMessageSheet(sharePayload);
              }
          };
          currentRenderOneForLoadMore = renderOne;
          // docChanges incremental: add/modify/remove only – no full reload. New messages pop in smoothly.
          let changes = [];
          try{ changes = (typeof snap.docChanges === 'function' ? snap.docChanges() : snap.docChanges || []); }catch(_){}
          const allAdded = changes.length > 0 && changes.every((c)=> (String(c.type||'').toLowerCase()) === 'added');
          const alreadyHaveAll = allAdded && prevIds.length > 0 && changes.every((c)=> prevIds.includes(((c.doc||c).id)));
          const suppressUntilVal = Number(this._suppressLivePatchUntilByConn.get(activeConnId) || 0);
          const useDocChanges = fromLive
            && renderedConnId === activeConnId
            && extraIdsInit.length === 0
            && changes.length > 0
            && !alreadyHaveAll
            && Date.now() >= suppressUntilVal;
          if (useDocChanges){
            let didMutate = false;
            let lastDayFromChanges = lastRenderedDay;
            for (const c of changes){
              const type = String(c.type||'').toLowerCase();
              const doc = c.doc || c;
              const id = doc.id;
              const existing = box.querySelector('[data-msg-id="' + id + '"]');
              if (type === 'removed'){
                if (existing){ existing.remove(); didMutate = true; }
              } else if (type === 'added'){
                if (existing) continue;
                const d = { id, data: (typeof doc.data === 'function' ? doc.data() : (doc.data || {})) || {}, sourceConnId: activeConnId };
                const dm = d.data;
                const firstMsg = box.querySelector('.message');
                const ts = firstMsg?.dataset?.msgTs ? Number(firstMsg.dataset.msgTs) : 0;
                const appendCtx = ts > 0 ? { lastRenderedDay: this.formatMessageDay(ts, {}) } : undefined;
                try{
                  await renderOne(d, d.sourceConnId || activeConnId, { forceInsertBefore: true, appendContext: appendCtx });
                  const dayLbl = this.formatMessageDay(dm?.createdAt, dm);
                  if (dayLbl) lastDayFromChanges = dayLbl;
                  didMutate = true;
                }catch(_){ }
              } else if (type === 'modified' && existing){
                const d = { id, data: (typeof doc.data === 'function' ? doc.data() : (doc.data || {})) || {}, sourceConnId: activeConnId };
                try{
                  await renderOne(d, d.sourceConnId || activeConnId, { replaceEl: existing });
                  const dayLbl = this.formatMessageDay(d.data?.createdAt, d.data);
                  if (dayLbl) lastDayFromChanges = dayLbl;
                  didMutate = true;
                }catch(_){ }
              }
            }
            if (didMutate) this._lastDayByConn.set(activeConnId, lastDayFromChanges);
            if (!didMutate){
              loadFinished = true;
              if (loadWatchdog){ clearTimeout(loadWatchdog); loadWatchdog = null; }
              if (hardGuardTimer){ clearTimeout(hardGuardTimer); hardGuardTimer = null; }
              return;
            }
            this._lastDocIdsByConn.set(activeConnId, docs.map((x)=> x.id));
            updateBottomUi();
            this.applyNewMessagesSeparator(box);
            loadFinished = true;
            if (loadWatchdog){ clearTimeout(loadWatchdog); loadWatchdog = null; }
            if (hardGuardTimer){ clearTimeout(hardGuardTimer); hardGuardTimer = null; }
            return;
          }
          const docsToRender = appendOnly
            ? (prefixMatch ? docs.slice(prevIds.length) : docs.filter((d)=> !prevIds.includes(d.id)))
            : docs;
          // column-reverse: first child = bottom. Non-append: iterate newest-first. AppendOnly: prepend oldest-first so newest ends up first.
          const iter = appendOnly ? Array.from({length: docsToRender.length}, (_, j)=> j) : Array.from({length: docsToRender.length}, (_, j)=> docsToRender.length - 1 - j);
          for (let idx = 0; idx < docsToRender.length; idx++) {
            const i = iter[idx];
            const d = docsToRender[i];
            try{ await renderOne(d, d.sourceConnId || activeConnId); }catch(_){ }
            if (idx % 5 === 4) await this.yieldToUi();
          }
          if (!appendOnly && renderTarget !== box && lastRenderedDay){
            const topSep = document.createElement('div');
            topSep.className = 'message-day-separator';
            topSep.textContent = lastRenderedDay;
            renderTarget.appendChild(topSep);
          }
          if (!appendOnly && renderTarget !== box){
            box.innerHTML = '';
            while (renderTarget.firstChild){
              box.appendChild(renderTarget.firstChild);
            }
          }
          while (box.childElementCount > 2000){
            const last = box.lastElementChild;
            if (last) box.removeChild(last);
          }
          this._lastDocIdsByConn.set(activeConnId, docs.map(d=> d.id));
          const rawDocs = snap.docs || [];
          const hasMerged = (relatedConnIds || []).filter((cid)=> cid && cid !== activeConnId).length > 0;
          if (rawDocs.length > 0 && !hasMerged){
            this._lastOldestDocSnapshotByConn.set(activeConnId, rawDocs[rawDocs.length - 1]);
            this._hasMoreOlderByConn.set(activeConnId, rawDocs.length >= visibleLimit);
          } else if (hasMerged){
            this._hasMoreOlderByConn.set(activeConnId, false);
          }
          this._lastDayByConn.set(activeConnId, lastRenderedDay);
          box.dataset.renderedConnId = activeConnId;
          updateBottomUi();
          this.applyNewMessagesSeparator(box);
          loadFinished = true;
          if (loadWatchdog){ clearTimeout(loadWatchdog); loadWatchdog = null; }
          if (hardGuardTimer){ clearTimeout(hardGuardTimer); hardGuardTimer = null; }
          }catch(_){
            loadFinished = true;
          }
        };
        let liveRenderInFlight = false;
        const pendingLiveSnaps = [];
        const processLiveSnap = async ()=>{
          if (liveRenderInFlight) return;
          liveRenderInFlight = true;
          try{
            while (pendingLiveSnaps.length){
              const snapNow = pendingLiveSnaps.shift();
              if (snapNow) await handleSnap(snapNow, true);
            }
          }finally{
            liveRenderInFlight = false;
          }
        };
        const scheduleLiveSnap = (snap)=>{
          if (Date.now() < Number(this._suppressLivePatchUntilByConn?.get(activeConnId) || 0)) return;
          pendingLiveSnaps.length = 0;
          pendingLiveSnaps.push(snap);
          if (this._scheduleLiveSnapTimer) clearTimeout(this._scheduleLiveSnapTimer);
          this._scheduleLiveSnapTimer = setTimeout(()=>{
            this._scheduleLiveSnapTimer = null;
            processLiveSnap().catch(()=>{});
          }, 200);
        };
        // Core invariant: first paint must run inline for active chat (no queued async dependency).
        try{
          const sInit = await Promise.race([
            fetchLatestSnap(),
            new Promise((_, reject)=> setTimeout(()=> reject(new Error('init-fetch-timeout')), 8000))
          ]);
          await handleSnap(sInit, false);
        }catch(_){ }
        const hasMergedForListener = (relatedConnIds || []).filter((cid)=> cid && cid !== activeConnId).length > 0;
        if (firebase.onSnapshot && this.activeConnection === activeConnId && loadSeq === this._msgLoadSeq && !hasMergedForListener){
          this._liveSnapshotPrimedByConn.set(activeConnId, false);
          try{
            this._unsubMessages = firebase.onSnapshot(
              q,
              (snap)=>{
                if (!loadFinished) return;
                this._liveSnapshotPrimedByConn.set(activeConnId, true);
                scheduleLiveSnap(snap);
              },
              async (err)=>{
                if (err?.code === 'permission-denied') return;
                try{
                  const s = await fetchLatestSnap();
                  scheduleLiveSnap(s);
                }catch(_){ }
              }
            );
          }catch(_){ this._unsubMessages = null; }
          // No polling when listener is active - poll causes unwanted reloads and scroll resets.
          // No periodic polling in snapshot mode to avoid constant refresh jitter.
        } else {
          this._msgPoll && clearInterval(this._msgPoll);
          this._msgPoll = setInterval(async()=>{
            try{
              const s = await fetchLatestSnapWithTimeout(4500);
              await handleSnap(s, false);
            }catch(_){ }
          }, 2500);
          const snap = await fetchLatestSnapWithTimeout(4500); await handleSnap(snap, false);
        }
        loadWatchdog = setTimeout(async ()=>{
          try{
            if (loadFinished) return;
            if (loadSeq !== this._msgLoadSeq || this.activeConnection !== activeConnId) return;
            // Skip if messages already rendered – avoids redundant reload when decrypt is slow.
            if (box.querySelector('.message')){ loadFinished = true; if (loadWatchdog){ clearTimeout(loadWatchdog); loadWatchdog = null; } return; }
            const sKick = await fetchLatestSnapWithTimeout(8000);
            await handleSnap(sKick, false);
          }catch(_){ }
        }, 7000);
        // Hard guard: never keep "Loading messages…" forever on rapid switches or stalled listeners.
        hardGuardTimer = setTimeout(async ()=>{
          try{
            if (loadFinished) return;
            if (loadSeq !== this._msgLoadSeq || this.activeConnection !== activeConnId) return;
            if (!/Loading messages/i.test(String(box.textContent || ''))) return;
            const sHard = await fetchLatestSnapWithTimeout(8000);
            await handleSnap(sHard, false);
            const hardDocsCount = Number((sHard && sHard.docs && sHard.docs.length) || 0);
            if (!loadFinished && hardDocsCount === 0 && /Loading messages/i.test(String(box.textContent || ''))){
              box.innerHTML = '<div style="opacity:.75;padding:10px 2px">No messages yet</div>';
              box.dataset.renderedConnId = activeConnId;
              updateBottomUi();
              this.applyNewMessagesSeparator(box);
              loadFinished = true;
            } else if (!loadFinished && /Loading messages/i.test(String(box.textContent || ''))){
              box.innerHTML = '<button id="chat-load-retry-btn" class="btn secondary" style="margin:10px 2px">Still loading... Tap to retry</button>';
              box.dataset.renderedConnId = '';
              const retryBtn = document.getElementById('chat-load-retry-btn');
              if (retryBtn){
                retryBtn.addEventListener('click', ()=> this.loadMessages().catch(()=>{}), { once: true });
              }
              updateBottomUi();
              this.applyNewMessagesSeparator(box);
            }
          }catch(_){ }
        }, 12000);
      }catch(_){
        try{
          const q = firebase.query(
            firebase.collection(this.db,'chatMessages',activeConnId,'messages'),
            firebase.orderBy('createdAt','desc'),
            firebase.limit(500)
          );
          const snap = await firebase.getDocs(q);
          if (loadSeq !== this._msgLoadSeq || this.activeConnection !== activeConnId) return;
          box.innerHTML='';
          let lastRenderedDay2 = '';
          let aesKey = await this.getFallbackKey();
          const fallbackDocs = (snap.docs || []).slice().reverse();
          for (let i = 0; i < fallbackDocs.length; i++){
            const d = fallbackDocs[i];
            if (loadSeq !== this._msgLoadSeq || this.activeConnection !== activeConnId) return;
            const m=d.data();
            let text='';
            if (typeof m.text === 'string' && !m.cipher){
              text = m.text;
            } else {
              try{
                text = await chatCrypto.decryptWithKey(m.cipher, aesKey);
              }catch(_){
                let ok = false;
                try{
                  const candidates = await this.getFallbackKeyCandidatesForConn(activeConnId);
                  for (const k of candidates){
                    try{
                      text = await chatCrypto.decryptWithKey(m.cipher, k);
                      ok = true;
                      break;
                    }catch(_){ }
                  }
                }catch(_){ }
                if (!ok){
                  try { const ecdh = await this.getOrCreateSharedAesKey(); text = await chatCrypto.decryptWithKey(m.cipher, ecdh);}
                  catch(_){ text='[unable to decrypt]'; }
                }
              }
            }
            const el = document.createElement('div');
            el.className='message '+(m.sender===this.currentUser.uid?'self':'other');
            el.dataset.msgId = String(d.id || m.id || '');
            const msgTs = this.getMessageTimestampMs(m);
            el.dataset.msgTs = String(msgTs || 0);
            if (m.sender !== this.currentUser.uid && msgTs > readMarkerMs) el.dataset.unread = '1';
            if (m.systemType === 'connection_request_intro'){
              el.classList.add('message-system', 'message-connection-request');
            }
            // Resolve sender name async
            let senderName = m.sender === this.currentUser.uid ? 'You' : this.usernameCache.get(m.sender) || m.sender.slice(0,8);
            if (!this.usernameCache.has(m.sender) && !this._senderLookupInFlight.has(m.sender)) {
              this._senderLookupInFlight.add(m.sender);
              Promise.resolve().then(async ()=>{
                try {
                  const user = await window.firebaseService.getUserData(m.sender);
                  const resolved = (user?.username || user?.email || m.sender.slice(0,8));
                  this.usernameCache.set(m.sender, resolved);
                } catch (_err) {
                  this.usernameCache.set(m.sender, senderName || 'Unknown');
                } finally {
                  this._senderLookupInFlight.delete(m.sender);
                }
              });
            }
            const inferredFileName = this.inferAttachmentFileName(m, text);
            const hasMedia = Array.isArray(m.media) && m.media.length > 0;
            const hasFile = !!m.fileUrl && !hasMedia;
            const previewOnlyFile = this.isAudioFilename(inferredFileName) || this.isVideoFilename(inferredFileName) || this.isImageFilename(inferredFileName);
            // Render call invites as buttons
            const cleanedText = this.stripPlaceholderText(text);
            const isMediaOnlyMessage = (hasFile || hasMedia) && !cleanedText && (hasFile ? previewOnlyFile : true);
            if (isMediaOnlyMessage) el.classList.add('message-media-only');
            let bodyHtml = this.renderText(cleanedText);
            if (m.sharedAsset && typeof m.sharedAsset === 'object') bodyHtml = this.renderSharedAssetCardHtml(m.sharedAsset, d.id || m.id || '');
            const callMatch = /^\[call:(voice|video):([A-Za-z0-9_\-]+)\]$/.exec(text);
            if (callMatch){
              const kind = callMatch[1]; const callId = callMatch[2];
              const btnLabel = kind==='voice' ? 'Join voice call' : 'Join video call';
              bodyHtml = `<button class=\"btn secondary\" data-call-id=\"${callId}\" data-kind=\"${kind}\">${btnLabel}</button>`;
            }
            const gifMatch = /^\[gif\]\s+(https?:\/\/\S+)$/i.exec(text || '');
            if (gifMatch){
              bodyHtml = `<img src="${gifMatch[1]}" alt="gif" style="max-width:100%;border-radius:8px" />`;
            }
            const stickerDataMatch = /^\[sticker-data\](data:image\/[a-zA-Z0-9.+-]+;base64,[A-Za-z0-9+/=]+)$/i.exec(text || '');
            if (stickerDataMatch){
              bodyHtml = `<img src="${stickerDataMatch[1]}" alt="sticker" style="max-width:100%;border-radius:8px" />`;
            }
            const sharePayload = this.buildSharePayload(text, m.fileUrl, inferredFileName, activeConnId, m.attachmentKeySalt || '', { ...m, id: d.id || m.id }, senderName);
            const dayLabel = this.formatMessageDay(m.createdAt, m);
            if (dayLabel !== lastRenderedDay2){
              const sep = document.createElement('div');
              sep.className = 'message-day-separator';
              sep.textContent = dayLabel;
              box.appendChild(sep);
              lastRenderedDay2 = dayLabel;
            }
            const systemBadge = m.systemType === 'connection_request_intro' ? '<span class="system-chip">Connection request</span>' : '';
            const editedBadge = this.isEditedMessage(m) ? ' · <span class="msg-edited-badge">edited</span>' : '';
            const isShared = !!(m.isShared || m.sharedFromMessageId || m.sharedOriginalAuthorUid || m.sharedOriginalAuthorName);
            const originalAuthorName = String(m.sharedOriginalAuthorName || '').trim();
            const isWaveConnectShare = isShared && m.sharedAsset && ['audio','video','image','picture','post'].includes(String(m.sharedAsset?.kind||'').toLowerCase());
        const repostBadge = isShared ? (isWaveConnectShare ? ' · <span class="msg-repost-badge">Repost from WaveConnect</span>' : ' · <span class="msg-repost-badge">repost</span>') : '';
            const originalSignature = isShared ? ` · <span class="msg-original-author">by ${(originalAuthorName || 'original author').replace(/</g,'&lt;')}</span>` : '';
            const delivery = this.getDeliveryLabel(m);
            const deliveryTxt = delivery ? ` · <span class="msg-delivery">${String(delivery)}</span>` : '';
            const mediaBlockHtml3 = hasMedia ? this.getMessageMediaBlockHtml(m) : '';
            const timeStr3 = String(this.formatMessageTime(m.createdAt, m) || '');
            const isVoiceMsgFb = hasFile && this.isVoiceRecordingMessage(m, inferredFileName);
            const senderStr3 = isVoiceMsgFb ? '' : String(senderName || '');
            const metaSepFb = senderStr3 ? `${senderStr3} · ` : '';
            el.innerHTML = `${mediaBlockHtml3}<div class=\"msg-text\">${bodyHtml}</div>${hasFile?`${previewOnlyFile ? '' : `<div class=\"file-link\">${inferredFileName || 'Attachment'}</div>`}<div class=\"file-preview\"><span class="attachment-loading" style="font-size:11px;opacity:.6">Loading…</span></div>`:''}<div class=\"meta\">${systemBadge}${metaSepFb}${timeStr3}${editedBadge}${repostBadge}${originalSignature}${deliveryTxt} · <span class=\"msg-share\" style=\"cursor:pointer\" title=\"Share to another chat\"><i class=\"fas fa-share-nodes\"></i></span></div>`;
            box.appendChild(el);
            const joinBtn = el.querySelector('button[data-call-id]');
            if (joinBtn){ joinBtn.addEventListener('click', ()=> this.answerCall(joinBtn.dataset.callId, { video: joinBtn.dataset.kind === 'video' })); }
            if (hasMedia){
              const attachmentSourceConnId = this.resolveAttachmentSourceConnId(m, activeConnId);
              const attachmentAesKey = await this.getFallbackKeyForConn(attachmentSourceConnId);
              m.media.forEach((mediaItem, idx)=>{
                const container = el.querySelector(`.msg-media-item[data-media-index="${idx}"] .file-preview`);
                if (container){
                  const msgProxy = { ...m, fileUrl: mediaItem.fileUrl, fileName: mediaItem.fileName, attachmentKeySalt: mediaItem.attachmentKeySalt, isVideoRecording: mediaItem.isVideoRecording ?? m.isVideoRecording, isVoiceRecording: mediaItem.isVoiceRecording ?? m.isVoiceRecording, text, id: d.id };
                  const msgId = String(d.id || m.id || '');
                  this.enqueueAttachmentPreview(()=>{
                    const c = box.querySelector(`[data-msg-id="${msgId.replace(/"/g,'\\"')}"] .msg-media-item[data-media-index="${idx}"] .file-preview`);
                    if (c?.isConnected) this.renderEncryptedAttachment(c, mediaItem.fileUrl, mediaItem.fileName, attachmentAesKey, attachmentSourceConnId, senderName, msgProxy);
                  }, loadSeq, activeConnId);
                }
              });
              try{ const dm = window.dashboardManager || window.top?.dashboardManager || window.parent?.dashboardManager; if (dm && typeof dm.activatePlayers === 'function') dm.activatePlayers(el); }catch(_){ }
              this.activateChatPlayers(el);
            }
            if (hasFile){
              const preview = el.querySelector('.file-preview');
              if (preview){
                const attachmentSourceConnId = this.resolveAttachmentSourceConnId(m, activeConnId);
                const attachmentAesKey = await this.getFallbackKeyForConn(attachmentSourceConnId);
                const isRecording = this.isVideoRecordingMessage(m, inferredFileName) || this.isVoiceRecordingMessage(m, inferredFileName);
                const isPriorityMedia = isRecording || this.isAudioFilename(inferredFileName);
                if (isPriorityMedia){
                  try{ await this.renderEncryptedAttachment(preview, m.fileUrl, inferredFileName, attachmentAesKey, attachmentSourceConnId, senderName, { ...m, text, id: d.id }); }catch(e){}
                }else{
                  const msgId = String(d.id || m.id || '');
                  this.enqueueAttachmentPreview(
                    ()=>{
                      const container = box.querySelector(`[data-msg-id="${msgId.replace(/"/g,'\\"')}"] .file-preview`);
                      if (container?.isConnected) this.renderEncryptedAttachment(container, m.fileUrl, inferredFileName, attachmentAesKey, attachmentSourceConnId, senderName, { ...m, text });
                    },
                    loadSeq,
                    activeConnId
                  );
                }
              }
            }
            if (m.sharedAsset && typeof m.sharedAsset === 'object') this.bindSharedAssetCardInteractions(el, m.sharedAsset);
            const shareBtn = el.querySelector('.msg-share');
            if (shareBtn){
              shareBtn.onclick = ()=> this.openShareMessageSheet(sharePayload);
            }
            if ((i % 3) === 2){
              await this.yieldToUi();
            }
          }
          box.dataset.renderedConnId = activeConnId;
          const rawFallback = snap.docs || [];
          if (rawFallback.length > 0){
            this._lastOldestDocSnapshotByConn.set(activeConnId, rawFallback[rawFallback.length - 1]);
            this._hasMoreOlderByConn.set(activeConnId, rawFallback.length >= 500);
          }
          updateBottomUi();
          this.applyNewMessagesSeparator(box);
        }catch(e){
          console.error('Failed to load messages:', e);
          box.innerHTML = '<div class="error">Failed to load messages. Check console.</div>';
        }
      }
    }
    // Legacy helper kept for compatibility
    async getArchivedConnIds(connId){
      const q = firebase.query(firebase.collection(this.db,'chatConnections'), firebase.where('mergedInto','==', connId));
      const s = await firebase.getDocs(q);
      return s.docs.map(d=> d.id);
    }

    async getRelatedConnIds(connId){
      const out = new Set([connId]);
      try{
        // merged chains into current
        const qM = firebase.query(firebase.collection(this.db,'chatConnections'), firebase.where('mergedInto','==', connId));
        const sM = await firebase.getDocs(qM);
        sM.forEach(d=> out.add(d.id));
      }catch(_){ }
      try{
        // same user set by key (covers duplicate docs not yet archived)
        const cur = await firebase.getDoc(firebase.doc(this.db,'chatConnections', connId));
        const key = cur.exists() ? (cur.data().key || this.computeConnKey(this.getConnParticipants(cur.data()))) : '';
        if (key){
          const qK = firebase.query(firebase.collection(this.db,'chatConnections'), firebase.where('key','==', key));
          const sK = await firebase.getDocs(qK);
          sK.forEach(d=> out.add(d.id));
        }
      }catch(_){ }
      return Array.from(out);
    }

    renderText(t){ return t.replace(/</g,'&lt;'); }

    normalizeMediaUrl(url){
      try{
        const raw = String(url || '').trim();
        if (!raw) return '';
        let decoded = raw;
        try{ decoded = decodeURIComponent(raw); }catch(_){ decoded = raw; }
        let out = decoded.toLowerCase();
        out = out.replace(/^https?:\/\//, '');
        out = out.replace(/^www\./, '');
        out = out.split('?')[0].split('#')[0];
        return out.replace(/\/+$/, '');
      }catch(_){ return String(url || '').trim().toLowerCase(); }
    }

    hashStringShort(input){
      try{
        const str = String(input || '');
        let h1 = 2166136261 >>> 0;
        let h2 = 2166136261 >>> 0;
        for (let i = 0; i < str.length; i++){
          const c = str.charCodeAt(i);
          h1 ^= c;
          h1 = Math.imul(h1, 16777619) >>> 0;
          h2 ^= (c + ((i * 13) & 255));
          h2 = Math.imul(h2, 16777619) >>> 0;
        }
        return `${h1.toString(16).padStart(8, '0')}${h2.toString(16).padStart(8, '0')}`;
      }catch(_){ return '0000000000000000'; }
    }

    urlsLikelySame(a, b){
      const x = this.normalizeMediaUrl(a);
      const y = this.normalizeMediaUrl(b);
      if (!x || !y) return false;
      if (x === y) return true;
      return x.endsWith(y) || y.endsWith(x);
    }

    makeAssetLikeKey(kind, url){
      const normalizedUrl = this.normalizeMediaUrl(url) || String(url || '').trim();
      const digest = this.hashStringShort(normalizedUrl);
      return `ak2_u_${digest}`;
    }

    getAssetLikeKeys(kind, url){
      const keys = [];
      const k1 = this.makeAssetLikeKey(kind, url);
      if (k1) keys.push(k1);
      const rawUrl = String(url || '').trim();
      const normUrl = this.normalizeMediaUrl(rawUrl) || rawUrl;
      const kinds = Array.from(new Set([
        String(kind || 'asset').toLowerCase(),
        'asset', 'audio', 'video', 'image', 'picture', 'file'
      ]));
      for (const k of kinds){
        for (const u of [rawUrl, normUrl]){
          if (!u) continue;
          try{
            const legacyBase = `${k}|${u}`;
            const legacy = `ak_${btoa(unescape(encodeURIComponent(legacyBase))).replace(/=+$/,'').replace(/\+/g,'-').replace(/\//g,'_')}`;
            if (legacy && !keys.includes(legacy)) keys.push(legacy);
          }catch(_){ }
        }
      }
      return keys;
    }

    async getAssetAggregatedLikeCount(kind, url){
      try{
        const href = String(url || '').trim();
        if (!href) return 0;
        let total = 0;
        const assetUsers = new Set();
        const countedPostIds = new Set();
        const keys = this.getAssetLikeKeys(kind, href);
        for (const key of keys){
          try{
            const s = await firebase.getDocs(firebase.collection(this.db,'assetLikes',key,'likes'));
            (s.docs || []).forEach((d)=>{
              const row = d.data() || {};
              const uid = String(row.uid || d.id || '').trim();
              if (uid) assetUsers.add(uid);
            });
          }catch(_){ }
        }
        total += assetUsers.size;
        try{
          const q = firebase.query(firebase.collection(this.db,'posts'), firebase.limit(500));
          const s = await firebase.getDocs(q);
          for (const d of (s.docs || [])){
            const p = d.data() || {};
            const pid = String(d.id || '').trim();
            if (!pid || countedPostIds.has(pid)) continue;
            const media = Array.isArray(p.media) ? p.media : [];
            const mediaUrl = String(p.mediaUrl || '').trim();
            const hasMediaUrl = mediaUrl && this.urlsLikelySame(mediaUrl, href);
            const hasUrl = media.some((m)=> this.urlsLikelySame(String((m && (m.url || m.mediaUrl)) || '').trim(), href));
            if (!hasMediaUrl && !hasUrl) continue;
            try{
              const likes = await firebase.getDocs(firebase.collection(this.db,'posts',pid,'likes'));
              total += Number(likes.size || 0);
              countedPostIds.add(pid);
            }catch(_){ }
          }
        }catch(_){ }
        return total;
      }catch(_){ return 0; }
    }

    getMessageMediaBlockHtml(m){
      if (!Array.isArray(m?.media) || !m.media.length) return '';
      const visual = m.media.filter((it)=> this.isImageFilename(it.fileName) || this.isVideoFilename(it.fileName));
      const rest = m.media.filter((it)=> !this.isImageFilename(it.fileName) && !this.isVideoFilename(it.fileName));
      let html = '<div class="msg-media-block" style="margin-bottom:8px">';
      if (visual.length){
        const slideItems = visual.map((_,i)=> `<div class="msg-media-item post-media-visual-item" data-media-index="${i}" style="flex:0 0 100%;min-width:0;width:100%;scroll-snap-align:start;scroll-snap-stop:always"><div class="file-preview msg-slider-preview" style="min-height:48px;max-height:280px"><span class="attachment-loading" style="font-size:11px;opacity:.6">Loading…</span></div></div>`).join('');
        html += `<div class="post-media-visual-shell msg-media-slider"><div class="post-media-visual-wrap"><div class="post-media-visual-slider">${slideItems}</div></div>${visual.length>1?`<div class="post-media-dots">${visual.map((_,i)=>`<button type="button" class="post-media-dot${i===0?' active':''}" data-slide-index="${i}"></button>`).join('')}</div>`:''}</div>`;
      }
      if (rest.length) html += `<div class="msg-media-files" style="display:flex;flex-direction:column;gap:6px;margin-top:6px">${rest.map((_,i)=> `<div class="msg-media-item" data-media-index="${visual.length+i}" style="min-width:0"><div class="file-preview" style="min-height:40px"><span class="attachment-loading" style="font-size:11px;opacity:.6">Loading…</span></div></div>`).join('')}</div>`;
      html += '</div>';
      return html;
    }

    renderSharedAssetCardHtml(asset, msgId = ''){
      try{
        const a = (asset && typeof asset === 'object') ? asset : {};
        let kind = String(a.kind || a.type || (a.post || a.postId ? 'post' : '')).toLowerCase();
        if (!kind && a.url) kind = String(this.inferMediaKindFromUrl(a.url) || '').toLowerCase();
        if (kind === 'post'){
          const pRaw = (a.post && typeof a.post === 'object') ? a.post : {};
          const p = { ...pRaw };
          if ((!p.media || !Array.isArray(p.media) || !p.media.length) && (a.media || a.attachments)){
            p.media = Array.isArray(a.media) ? a.media : (Array.isArray(a.attachments) ? a.attachments : [a.media || a.attachments]);
          }
          const postId = String(a.postId || p.id || '').trim();
          const author = this.renderText(String(p.authorName || a.by || 'User'));
          const created = this.renderText(String(this.formatMessageTime(p.createdAt || Date.now(), p) || ''));
          let text = String(p.text || a.title || '').trim();
          try{
            const dm = window.dashboardManager || window.top?.dashboardManager || window.parent?.dashboardManager;
            if (dm && typeof dm.getPostDisplayText === 'function'){
              const t = String(dm.getPostDisplayText(p) || '').trim();
              if (t) text = t;
            }
          }catch(_){ }
          if (!text) text = 'Shared post';
          const textHtml = `<div class="shared-post-text post-text" style="margin-top:8px">${this.renderText(text)}</div>`;
          const mediaHtml = this.renderSharedPostMediaHtml(p);
          return `<div class="shared-asset-card post-item" data-shared-kind="post" data-shared-post-id="${this.renderText(postId)}" data-msg-id="${this.renderText(String(msgId || ''))}" style="margin-top:6px;border:1px solid #2b3240;border-radius:12px;padding:10px;background:#0f1520"><div class="byline post-head" style="display:flex;align-items:center;justify-content:space-between;gap:8px;margin:4px 0"><span style="font-size:12px;color:#aaa">${author}</span><span style="font-size:11px;opacity:.74">${created}</span></div><div class="shared-post-media post-media-block">${mediaHtml}</div>${textHtml}<div class="post-actions" style="margin-top:8px;display:flex;flex-wrap:wrap;gap:10px;align-items:center"><button class="shared-like-btn btn secondary" style="padding:4px 8px"><i class="fas fa-heart"></i></button><span class="shared-like-count">0</span><button class="shared-comment-btn btn secondary" style="padding:4px 8px"><i class="fas fa-comment"></i></button><span class="shared-comment-count">0</span><button class="shared-repost-btn btn secondary" style="padding:4px 8px"><i class="fas fa-retweet"></i></button><span class="shared-repost-count">0</span></div></div>`;
        }
        const url = String(a.url || '').trim();
        const title = this.renderText(String(a.title || a.name || `Shared ${kind || 'asset'}`));
        const by = this.renderText(String(a.by || a.authorName || ''));
        const cover = String(a.cover || a.coverUrl || a.thumbnailUrl || '').trim();
        const byline = by ? `<div class="shared-asset-byline" style="font-size:12px;opacity:.85;margin-top:2px">by ${by}</div>` : '';
        const coverImg = cover ? `<img src="${this.renderText(cover)}" alt="" style="width:40px;height:40px;flex-shrink:0;border-radius:8px;object-fit:cover">` : '';
        const header = `<div class="shared-asset-head" style="margin-bottom:8px;display:flex;gap:10px;align-items:flex-start;min-width:0"><div style="flex-shrink:0">${coverImg}</div><div style="min-width:0;flex:1;overflow:hidden"><div class="shared-asset-title" style="font-weight:600;font-size:14px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${title}</div>${byline}</div></div>`;
        const visual = kind === 'video'
          ? `<video src="${this.renderText(url)}" controls playsinline style="width:100%;max-height:280px;border-radius:10px;object-fit:contain;background:#000"></video>`
          : (kind === 'image' || kind === 'picture'
              ? `<img src="${this.renderText(url)}" alt="${title}" style="width:100%;max-height:320px;object-fit:contain;border-radius:10px" data-fullscreen-image="1">`
              : (kind === 'audio'
                  ? `<div class="post-media-files-item shared-audio-waveconnect"><div class="post-media-audio-head">${cover ? `<img src="${this.renderText(cover)}" alt="cover" class="post-media-audio-cover" style="width:56px;height:56px;border-radius:8px;object-fit:cover">` : `<span class="post-media-audio-cover post-media-audio-cover-fallback" style="width:56px;height:56px;border-radius:8px;display:flex;align-items:center;justify-content:center;background:rgba(255,255,255,.08)"><i class="fas fa-music"></i></span>`}<div class="post-media-audio-head-text"><span class="post-media-audio-title">${title}</span>${by ? `<span class="post-media-audio-by">by ${by}</span>` : ''}</div></div><audio class="liber-lib-audio" src="${this.renderText(url)}" style="display:none" data-title="${this.renderText(title)}" data-by="${this.renderText(by)}" data-cover="${this.renderText(cover)}"></audio><div class="wave-item-audio-host" style="margin-top:6px"></div></div>`
                  : `<div style="display:flex;gap:10px;align-items:center">${cover ? `<img src="${this.renderText(cover)}" alt="${title}" style="width:56px;height:56px;border-radius:8px;object-fit:cover">` : ''}<audio src="${this.renderText(url)}" controls style="width:100%"></audio></div>`));
        const titleBy = (kind === 'video' || kind === 'image' || kind === 'picture') ? header : '';
        const actions = kind === 'audio'
          ? `<div class="shared-asset-actions" style="margin-top:10px;display:flex;flex-wrap:wrap;gap:10px;align-items:center"><button class="shared-like-btn btn secondary" style="padding:4px 10px"><i class="fas fa-heart"></i></button><span class="shared-like-count">0</span><button class="shared-asset-add-btn btn secondary" style="padding:4px 10px" title="Add to library"><i class="fas fa-plus"></i></button></div>`
          : `<div class="shared-asset-actions" style="margin-top:10px;display:flex;flex-wrap:wrap;gap:10px;align-items:center"><button class="shared-like-btn btn secondary" style="padding:4px 10px"><i class="fas fa-heart"></i></button><span class="shared-like-count">0</span></div>`;
        return `<div class="shared-asset-card shared-asset-waveconnect" data-shared-kind="${this.renderText(kind)}" data-shared-url="${this.renderText(url)}" data-msg-id="${this.renderText(String(msgId || ''))}" style="margin-top:6px;border:1px solid #2b3240;border-radius:12px;padding:12px;background:#0f1520">${titleBy}${visual}${actions}</div>`;
      }catch(_){ return `<div>${this.renderText('Shared')}</div>`; }
    }

    normalizePostMediaForShared(media, post = {}){
      const raw = Array.isArray(media) ? media : (media ? [media] : []);
      const defaultBy = String(post?.authorName || '').trim();
      const defaultCover = String(post?.coverUrl || post?.thumbnailUrl || '').trim();
      const out = [];
      raw.forEach((entry)=>{
        if (!entry) return;
        if (typeof entry === 'string'){
          const url = String(entry || '').trim();
          if (!url) return;
          const kind = this.inferMediaKindFromUrl(url);
          out.push({ kind, url, name: kind === 'image' ? 'Picture' : (kind === 'video' ? 'Video' : 'Attachment'), by: defaultBy, cover: defaultCover });
          return;
        }
        if (typeof entry === 'object'){
          const kind = String(entry.kind || entry.mediaType || '').trim().toLowerCase();
          const url = String(entry.url || entry.mediaUrl || '').trim();
          const name = String(entry.name || entry.title || '').trim();
          if (kind === 'playlist'){
            out.push({ kind: 'playlist', name: name || 'Playlist', playlistId: String(entry.playlistId || entry.id || '').trim() || null, by: String(entry.by || entry.authorName || '').trim(), cover: String(entry.cover || entry.coverUrl || '').trim(), items: Array.isArray(entry.items) ? entry.items.slice(0, 120) : [] });
            return;
          }
          if (url){
            const resolvedKind = ['image','picture','video','audio','file'].includes(kind) ? kind : this.inferMediaKindFromUrl(url);
            out.push({ kind: resolvedKind, url, name: name || (resolvedKind === 'image' || resolvedKind === 'picture' ? 'Picture' : (resolvedKind === 'video' ? 'Video' : (resolvedKind === 'audio' ? 'Audio' : 'Attachment'))), by: String(entry.by || entry.authorName || defaultBy || '').trim(), cover: String(entry.cover || entry.coverUrl || defaultCover || '').trim() });
          }
        }
      });
      return out;
    }

    renderSharedPostMediaHtml(post){
      try{
        const p = (post && typeof post === 'object') ? post : {};
        const mediaInput = p.media || p.mediaUrl || p.attachments || [];
        try{
          const dm = window.dashboardManager || window.top?.dashboardManager || window.parent?.dashboardManager;
          if (dm && typeof dm.renderPostMedia === 'function'){
            const html = dm.renderPostMedia(mediaInput, {
              defaultBy: p.authorName || '',
              defaultCover: p.coverUrl || p.thumbnailUrl || '',
              authorId: p.authorId || ''
            });
            if (html) return html;
          }
        }catch(_){ }
        const items = this.normalizePostMediaForShared(mediaInput, p);
        if (!items.length) return '';
        const mediaRank = (it)=> (it.kind === 'image' || it.kind === 'picture' || it.kind === 'video') ? 0 : 1;
        const ordered = items.slice(0, 10).sort((a,b)=> mediaRank(a) - mediaRank(b));
        const visual = ordered.filter((it)=> it.kind === 'image' || it.kind === 'picture' || it.kind === 'video');
        const rest = ordered.filter((it)=> it.kind !== 'image' && it.kind !== 'picture' && it.kind !== 'video');
        let visualHtml = '';
        if (visual.length){
          const slideItems = visual.map((it)=>{
            if (it.kind === 'image' || it.kind === 'picture') return `<div class="post-media-visual-item"><img src="${this.renderText(it.url)}" alt="media" class="post-media-image" style="max-height:260px;object-fit:cover" data-fullscreen-image="1"></div>`;
            return `<div class="post-media-visual-item"><div class="player-card"><div class="post-media-video-head">${this.renderText(it.name || 'Video')}</div><video src="${this.renderText(it.url)}" class="player-media post-media-video" controls playsinline style="width:100%;max-height:260px;border-radius:10px;object-fit:cover"></video><div class="player-bar"><button class="btn-icon" data-action="play"><i class="fas fa-play"></i></button><div class="progress"><div class="fill"></div></div><div class="time"></div></div></div>`;
          }).join('');
          visualHtml = `<div class="post-media-visual-shell"><div class="post-media-visual-wrap"><div class="post-media-visual-slider">${slideItems}</div></div>${visual.length > 1 ? `<div class="post-media-dots">${visual.map((_,i)=> `<button type="button" class="post-media-dot${i===0?' active':''}" data-slide-index="${i}"></button>`).join('')}</div>` : ''}</div>`;
        }
        const restHtml = rest.length ? `<div class="post-media-files-list">${rest.map((it)=>{
          if (it.kind === 'audio' && it.url) return `<div class="post-media-files-item"><div class="post-media-audio-head">${it.cover ? `<img src="${this.renderText(it.cover)}" alt="cover" class="post-media-audio-cover">` : `<span class="post-media-audio-cover post-media-audio-cover-fallback"><i class="fas fa-music"></i></span>`}<div class="post-media-audio-head-text"><span class="post-media-audio-title">${this.renderText(it.name || 'Audio')}</span>${it.by ? `<span class="post-media-audio-by">by ${this.renderText(it.by)}</span>` : ''}</div></div><audio src="${this.renderText(it.url)}" controls style="width:100%"></audio></div>`;
          if (it.kind === 'playlist'){
            const items = Array.isArray(it.items) ? it.items : [];
            const encoded = encodeURIComponent(JSON.stringify(items));
            const pid = String(it.playlistId || '').replace(/"/g,'&quot;');
            const cover = it.cover ? `<img src="${this.renderText(it.cover)}" alt="playlist" style="width:40px;height:40px;border-radius:8px;object-fit:cover">` : `<span style="width:40px;height:40px;border-radius:8px;display:flex;align-items:center;justify-content:center;background:#1b2230"><i class="fas fa-list-music"></i></span>`;
            return `<div class="post-media-files-item post-playlist-card" style="border:1px solid #2b3240;border-radius:10px;padding:10px;background:rgba(255,255,255,.02);min-width:0;max-width:100%;overflow:hidden;display:flex;align-items:center;gap:10px"><div style="flex-shrink:0">${cover}</div><div style="min-width:0;flex:1"><div style="font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${this.renderText(it.name || 'Playlist')}</div>${it.by ? `<div style="font-size:12px;opacity:.8;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">by ${this.renderText(it.by)}</div>` : ''}</div><button type="button" class="btn secondary post-playlist-play-btn" data-playlist-items="${encoded}" data-playlist-id="${pid}" title="Play playlist" style="padding:6px 12px"><i class="fas fa-play"></i></button></div>`;
          }
          if (it.url) return `<div class="post-media-files-item"><a href="${this.renderText(it.url)}" target="_blank" rel="noopener noreferrer" class="post-media-file-chip"><i class="fas fa-paperclip"></i> ${this.renderText(it.name || 'Attachment')}</a></div>`;
          return '';
        }).filter(Boolean).join('')}</div>` : '';
        return `<div class="post-media-block">${visualHtml}${restHtml}</div>`;
      }catch(_){ return ''; }
    }

    async hydrateSharedPostCard(root, postId){
      try{
        const card = root && root.classList?.contains('shared-asset-card') ? root : (root?.querySelector?.('.shared-asset-card') || null);
        const pid = String(postId || card?.dataset?.sharedPostId || '').trim();
        if (!card || !pid) return;
        if (!this.db) return;
        const snap = await firebase.getDoc(firebase.doc(this.db, 'posts', pid));
        if (!snap.exists()) return;
        const p = snap.data() || {};
        let text = String(p.text || '').trim();
        try{
          if (window.dashboardManager && typeof window.dashboardManager.getPostDisplayText === 'function'){
            const t = String(window.dashboardManager.getPostDisplayText(p) || '').trim();
            if (t) text = t;
          }
        }catch(_){ }
        const mediaHtml = this.renderSharedPostMediaHtml(p);
        const head = card.querySelector('.post-head');
        if (head){
          const author = this.renderText(String(p.authorName || 'User'));
          const created = this.renderText(String(this.formatMessageTime(p.createdAt || Date.now(), p) || ''));
          head.innerHTML = `<span style="font-size:12px;color:#aaa">${author}</span><span style="font-size:11px;opacity:.74">${created}</span>`;
        }
        const mediaHost = card.querySelector('.shared-post-media');
        if (mediaHost) mediaHost.innerHTML = mediaHtml;
        try{
          const dm = window.dashboardManager || window.top?.dashboardManager || window.parent?.dashboardManager;
          if (dm){
            if (typeof dm.activatePlayers === 'function') dm.activatePlayers(card);
            if (typeof dm.bindUserPreviewTriggers === 'function') dm.bindUserPreviewTriggers(card);
          }
          this.activateChatPlayers(card);
        }catch(_){ this.activateChatPlayers(card); }
        const textHost = card.querySelector('.shared-post-text');
        if (textHost){
          const nextText = String(text || 'Shared post').trim() || 'Shared post';
          textHost.style.display = '';
          textHost.innerHTML = this.renderText(nextText);
        }
      }catch(_){ }
    }

    activateChatPlayers(root){
      try{
        if (!root || !root.querySelectorAll) return;
        root.querySelectorAll('.post-media-visual-shell, .msg-media-slider').forEach((shell)=>{
          if (shell.dataset.sliderBound === '1') return;
          shell.dataset.sliderBound = '1';
          const wrap = shell.querySelector('.post-media-visual-wrap');
          const slider = shell.querySelector('.post-media-visual-slider');
          const dots = shell.querySelectorAll('.post-media-dot');
          if (!wrap || !slider || !dots.length) return;
          dots.forEach((dot, i)=>{
            dot.onclick = ()=>{ const item = slider.children[i]; if (item) item.scrollIntoView({ behavior: 'smooth', block: 'nearest', inline: 'start' }); dots.forEach(d=> d.classList.remove('active')); dot.classList.add('active'); };
          });
          wrap.addEventListener('scroll', ()=>{
            const items = slider.querySelectorAll('.post-media-visual-item, .msg-media-item');
            if (!items.length) return;
            const w = wrap.offsetWidth;
            const idx = Math.round(wrap.scrollLeft / (w || 1));
            dots.forEach((d, j)=> d.classList.toggle('active', j === Math.min(idx, dots.length - 1)));
          });
        });
        const setPlayIcon = (btn, isPlaying)=>{ if (btn) btn.innerHTML = `<i class="fas ${isPlaying ? 'fa-pause' : 'fa-play'}"></i>`; };
        root.querySelectorAll('.player-card').forEach((card)=>{
          if (card.dataset.chatPlayerBound === '1') return;
          card.dataset.chatPlayerBound = '1';
          const media = card.querySelector('.player-media');
          if (!media) return;
          const btn = card.querySelector('.btn-icon');
          const fill = card.querySelector('.progress .fill');
          let knob = card.querySelector('.progress .knob');
          if (!knob){ const k = document.createElement('div'); k.className='knob'; const bar = card.querySelector('.progress'); if (bar){ bar.appendChild(k); knob = k; } }
          const time = card.querySelector('.time');
          const fmt = (s)=>{ const m=Math.floor(s/60); const ss=Math.floor(s%60).toString().padStart(2,'0'); return `${m}:${ss}`; };
          const sync = ()=>{ if (!media.duration) return; const p=(media.currentTime/media.duration)*100; if (fill) fill.style.width = `${p}%`; if (knob) knob.style.left = `${p}%`; if (time) time.textContent = `${fmt(media.currentTime)} / ${fmt(media.duration)}`; };
          if (btn){
            btn.onclick = ()=>{
              if (media.paused){
                this.pauseOtherInlineMedia(media);
                media.play().catch(()=>{});
                setPlayIcon(btn, true);
              } else {
                media.pause();
                setPlayIcon(btn, false);
              }
            };
          }
          media.addEventListener('timeupdate', sync);
          media.addEventListener('loadedmetadata', sync);
          media.addEventListener('play', ()=> setPlayIcon(btn, true));
          media.addEventListener('pause', ()=> setPlayIcon(btn, false));
          media.addEventListener('ended', ()=> setPlayIcon(btn, false));
          const bar = card.querySelector('.progress');
          if (bar){
            const seekTo = (clientX)=>{
              const rect = bar.getBoundingClientRect();
              const ratio = Math.min(1, Math.max(0, (clientX-rect.left)/rect.width));
              if (media.duration){ media.currentTime = ratio * media.duration; }
              if (media.paused){ this.pauseOtherInlineMedia(media); media.play().catch(()=>{}); setPlayIcon(btn, true); }
            };
            bar.addEventListener('click', (e)=> seekTo(e.clientX));
            let dragging = false;
            bar.addEventListener('pointerdown', (e)=>{ dragging = true; bar.setPointerCapture(e.pointerId); seekTo(e.clientX); });
            bar.addEventListener('pointermove', (e)=>{ if (dragging) seekTo(e.clientX); });
            bar.addEventListener('pointerup', (e)=>{ dragging = false; bar.releasePointerCapture(e.pointerId); });
          }
          sync();
        });
        root.querySelectorAll('.post-playlist-play-btn').forEach((btn)=>{
          if (btn.dataset.chatPlaylistBound === '1') return;
          btn.dataset.chatPlaylistBound = '1';
          btn.onclick = async ()=>{
            try{
              const raw = decodeURIComponent(String(btn.dataset.playlistItems || '[]'));
              let items = [];
              try{ items = JSON.parse(raw || '[]'); }catch(_){}
              const pid = String(btn.dataset.playlistId || '').trim();
              if (!items.length && pid && this.db){
                const snap = await firebase.getDoc(firebase.doc(this.db,'playlists',pid));
                if (snap.exists()) items = Array.isArray((snap.data()||{}).items) ? (snap.data()||{}).items : [];
              }
              const first = items[0];
              const src = first && (first.url || first.src) ? String(first.url || first.src || '').trim() : '';
              const title = first && (first.title || first.name) ? String(first.title || first.name || 'Track') : 'Playlist';
              const by = first && (first.by || first.authorName) ? String(first.by || first.authorName || '') : '';
              const cover = first && (first.cover || first.coverUrl) ? String(first.cover || first.coverUrl || '') : '';
              const target = window.parent !== window ? window.parent : (window.top || window);
              if (target && target.postMessage) target.postMessage({ type: 'liber:chat-playlist-play', items, firstTrack: { src, title, by, cover } }, '*');
              if (src) this.notifyParentAudioMetaOnly({ src, title, by, cover });
            }catch(_){}
          };
        });
      }catch(_){ }
    }

    notifyParentAudioPlay(mediaElOrTrack){
        try{
            let src, title, by, cover, currentTime;
            if (mediaElOrTrack && typeof mediaElOrTrack === 'object' && typeof mediaElOrTrack.nodeType === 'number'){
                const el = mediaElOrTrack;
                src = (el?.currentSrc || el?.src || '').trim();
                title = String(el?.dataset?.title || el?.closest?.('.shared-asset-card,.audio-attachment-block,.post-media-files-item')?.querySelector?.('.post-media-audio-title,.shared-asset-title')?.textContent || 'Audio').trim();
                by = String(el?.dataset?.by || el?.closest?.('.shared-asset-card,.audio-attachment-block,.post-media-files-item')?.querySelector?.('.post-media-audio-by,.shared-asset-byline')?.textContent || '').replace(/^by\s+/i,'').trim();
                cover = String(el?.dataset?.cover || '').trim();
                currentTime = el?.currentTime;
            } else if (mediaElOrTrack && typeof mediaElOrTrack === 'object'){
                src = String(mediaElOrTrack.src || mediaElOrTrack.currentSrc || '').trim();
                title = String(mediaElOrTrack.title || 'Audio').trim();
                by = String(mediaElOrTrack.by || '').trim();
                cover = String(mediaElOrTrack.cover || '').trim();
                currentTime = mediaElOrTrack.currentTime;
            }
            if (!src) return;
            const target = window.parent !== window ? window.parent : (window.top || window);
            if (target && target.postMessage) target.postMessage({ type: 'liber:chat-audio-play', src, title: title || 'Audio', by, cover, currentTime }, '*');
        }catch(_){ }
    }

    /** Notify parent only to sync mini player display (metadata). Does NOT trigger addChatAudioToPlayer. */
    notifyParentAudioMetaOnly(mediaElOrTrack){
        try{
            let src, title, by, cover;
            if (mediaElOrTrack && typeof mediaElOrTrack === 'object' && typeof mediaElOrTrack.nodeType === 'number'){
                const el = mediaElOrTrack;
                src = (el?.currentSrc || el?.src || '').trim();
                title = String(el?.dataset?.title || el?.closest?.('.shared-asset-card,.audio-attachment-block,.post-media-files-item')?.querySelector?.('.post-media-audio-title,.shared-asset-title')?.textContent || 'Audio').trim();
                by = String(el?.dataset?.by || el?.closest?.('.shared-asset-card,.audio-attachment-block,.post-media-files-item')?.querySelector?.('.post-media-audio-by,.shared-asset-byline')?.textContent || '').replace(/^by\s+/i,'').trim();
                cover = String(el?.dataset?.cover || '').trim();
            } else if (mediaElOrTrack && typeof mediaElOrTrack === 'object'){
                src = String(mediaElOrTrack.src || mediaElOrTrack.currentSrc || '').trim();
                title = String(mediaElOrTrack.title || 'Audio').trim();
                by = String(mediaElOrTrack.by || '').trim();
                cover = String(mediaElOrTrack.cover || '').trim();
            }
            if (!src) return;
            const target = window.parent !== window ? window.parent : (window.top || window);
            if (target && target.postMessage) target.postMessage({ type: 'liber:chat-audio-meta', src, title: title || 'Audio', by, cover }, '*');
        }catch(_){ }
    }

    bindSharedAssetCardInteractions(el, asset){
      try{
        if (!el || !asset) return;
        const root = el.querySelector('.shared-asset-card');
        if (!root) return;
        const a = (asset && typeof asset === 'object') ? asset : {};
        const kind = String(a.kind || a.type || (a.post || a.postId ? 'post' : '')).toLowerCase();
        if (kind === 'post'){
          const postId = String(a.postId || a?.post?.id || '').trim();
          if (!postId) return;
          this.hydrateSharedPostCard(root, postId).catch(()=>{});
          const likeBtn = root.querySelector('.shared-like-btn');
          const comBtn = root.querySelector('.shared-comment-btn');
          const repBtn = root.querySelector('.shared-repost-btn');
          const likeCnt = root.querySelector('.shared-like-count');
          const comCnt = root.querySelector('.shared-comment-count');
          const repCnt = root.querySelector('.shared-repost-count');
          const refreshPostCounters = async ()=>{
            try{
              const [likes, comments, reposts] = await Promise.all([
                firebase.getDocs(firebase.collection(this.db,'posts',postId,'likes')).catch(()=>({ size: 0 })),
                firebase.getDocs(firebase.collection(this.db,'posts',postId,'comments')).catch(()=>({ size: 0 })),
                firebase.getDocs(firebase.collection(this.db,'posts',postId,'reposts')).catch(()=>({ size: 0 }))
              ]);
              if (likeCnt) likeCnt.textContent = String(likes.size || 0);
              if (comCnt) comCnt.textContent = String(comments.size || 0);
              if (repCnt) repCnt.textContent = String(reposts.size || 0);
            }catch(_){ }
          };
          refreshPostCounters();
          if (firebase && typeof firebase.onSnapshot === 'function'){
            const onErr = ()=>{};
            try{ firebase.onSnapshot(firebase.collection(this.db,'posts',postId,'likes'), ()=> refreshPostCounters(), onErr); }catch(_){ }
            try{ firebase.onSnapshot(firebase.collection(this.db,'posts',postId,'comments'), ()=> refreshPostCounters(), onErr); }catch(_){ }
            try{ firebase.onSnapshot(firebase.collection(this.db,'posts',postId,'reposts'), ()=> refreshPostCounters(), onErr); }catch(_){ }
          }
          if (likeBtn) likeBtn.onclick = async ()=>{ try{ const uid = String(this.currentUser?.uid || window.firebaseService?.auth?.currentUser?.uid || '').trim(); if (!uid) return; const ref = firebase.doc(this.db,'posts',postId,'likes', uid); const s = await firebase.getDoc(ref); if (s.exists()) await firebase.deleteDoc(ref); else await firebase.setDoc(ref,{uid,createdAt:new Date().toISOString()}); const n = await firebase.getDocs(firebase.collection(this.db,'posts',postId,'likes')); if (likeCnt) likeCnt.textContent = String(n.size||0); }catch(_){ } };
          if (repBtn) repBtn.onclick = async ()=>{ try{ const uid = String(this.currentUser?.uid || window.firebaseService?.auth?.currentUser?.uid || '').trim(); if (!uid) return; const ref = firebase.doc(this.db,'posts',postId,'reposts', uid); const s = await firebase.getDoc(ref); if (s.exists()) await firebase.deleteDoc(ref); else await firebase.setDoc(ref,{uid,createdAt:new Date().toISOString()}); const n = await firebase.getDocs(firebase.collection(this.db,'posts',postId,'reposts')); if (repCnt) repCnt.textContent = String(n.size||0); }catch(_){ } };
          if (comBtn) comBtn.onclick = async ()=>{ try{ const uid = String(this.currentUser?.uid || window.firebaseService?.auth?.currentUser?.uid || '').trim(); if (!uid) return; const text = prompt('Comment'); if (text === null) return; const t = String(text || '').trim(); if (!t) return; const ref = firebase.doc(firebase.collection(this.db,'posts',postId,'comments')); await firebase.setDoc(ref,{id:ref.id,uid,text:t,createdAt:new Date().toISOString()}); const n = await firebase.getDocs(firebase.collection(this.db,'posts',postId,'comments')); if (comCnt) comCnt.textContent = String(n.size||0); }catch(_){ } };
          return;
        }
        const url = String(a.url || '').trim();
        if (!url) return;
        if (kind === 'audio'){
          const audioEl = root.querySelector('.liber-lib-audio');
          const hostEl = root.querySelector('.wave-item-audio-host');
          if (audioEl && hostEl){
            hostEl.innerHTML = '';
            this.renderInlineWaveAudio(hostEl, url, String(a.title || a.name || 'Audio'), '');
            audioEl.addEventListener('play', ()=> this.notifyParentAudioMetaOnly(audioEl), { once: false });
            const addBtn = root.querySelector('.shared-asset-add-btn');
            if (addBtn) addBtn.onclick = (e)=>{ try{ e.preventDefault(); e.stopPropagation(); this.addToChatAudioPlaylist({ src: url, title: String(a.title || a.name || 'Audio'), author: String(a.by || a.authorName || ''), sourceKey: '' }); }catch(_){ } };
          }
        }
        root.dataset.assetLikeKind = String(kind || 'asset').toLowerCase();
        root.dataset.assetLikeUrl = this.normalizeMediaUrl(url);
        const likeBtn = root.querySelector('.shared-like-btn');
        const likeCnt = root.querySelector('.shared-like-count');
        const keys = this.getAssetLikeKeys(kind || 'asset', url);
        const primaryKey = keys[0];
        const refreshAssetLikeCount = async ()=>{
          try{
            const n = await this.getAssetAggregatedLikeCount(kind || 'asset', url);
            if (likeCnt) likeCnt.textContent = String(n);
            const norm = this.normalizeMediaUrl(url);
            document.querySelectorAll('.shared-asset-card[data-asset-like-kind][data-asset-like-url]').forEach((host)=>{
              const k = String(host.getAttribute('data-asset-like-kind') || '').toLowerCase();
              const u = String(host.getAttribute('data-asset-like-url') || '').trim();
              if (k !== String(kind || 'asset').toLowerCase()) return;
              if (!this.urlsLikelySame(u, norm)) return;
              const cnt = host.querySelector('.shared-like-count');
              if (cnt) cnt.textContent = String(n);
            });
          }catch(_){ }
        };
        refreshAssetLikeCount();
        try{
          const poll = setInterval(()=>{
            try{
              if (!root || !document.body.contains(root)){ clearInterval(poll); return; }
              refreshAssetLikeCount();
            }catch(_){ clearInterval(poll); }
          }, 6000);
        }catch(_){ }
        if (firebase && typeof firebase.onSnapshot === 'function'){
          const onErr = ()=>{};
          keys.forEach((key)=>{
            try{ firebase.onSnapshot(firebase.collection(this.db,'assetLikes',key,'likes'), ()=> refreshAssetLikeCount(), onErr); }catch(_){ }
          });
        }
        if (likeBtn){
          likeBtn.onclick = async ()=>{
            try{
              const uid = String(this.currentUser?.uid || window.firebaseService?.auth?.currentUser?.uid || '').trim();
              if (!uid) return;
              const refs = [];
              for (const key of keys){
                try{
                  if (!key || String(key).length > 1200) continue;
                  refs.push(firebase.doc(this.db,'assetLikes',key,'likes', uid));
                }catch(_){ }
              }
              if (!refs.length){
                if (primaryKey){
                  try{ refs.push(firebase.doc(this.db,'assetLikes',primaryKey,'likes', uid)); }catch(_){ }
                }
              }
              let hasLike = false;
              for (const ref of refs){
                const s = await firebase.getDoc(ref);
                if (s.exists()){ hasLike = true; break; }
              }
              if (hasLike){
                await Promise.all(refs.map(async (ref)=>{ try{ await firebase.deleteDoc(ref); }catch(_){ } }));
              } else {
                let wrote = false;
                const writeKeys = Array.from(new Set([primaryKey, ...keys].filter(Boolean)));
                for (const key of writeKeys){
                  try{
                    const ref = firebase.doc(this.db,'assetLikes',key,'likes', uid);
                    await firebase.setDoc(ref,{uid,kind,url,createdAt:new Date().toISOString()});
                    wrote = true;
                    break;
                  }catch(_){ }
                }
                if (!wrote) throw new Error('asset-like-write-failed');
              }
              await refreshAssetLikeCount();
            }catch(_){ }
          };
        }
      }catch(_){ }
    }

    async getConnectionStateWithPeer(peerUid){
      if (!peerUid) return { status: 'none' };
      try{
        const ref = firebase.doc(this.db, 'connections', this.currentUser.uid, 'peers', peerUid);
        const snap = await firebase.getDoc(ref);
        if (!snap.exists()) return { status: 'none' };
        const d = snap.data() || {};
        return { status: d.status || 'none', requestedBy: d.requestedBy || '', requestedTo: d.requestedTo || '' };
      }catch(_){ return { status: 'none' }; }
    }

    async canSendToActiveConnection(){
      return this.canSendToConnection(this.activeConnection);
    }

    async canSendToConnection(connId){
      if (!connId) return { ok: false, reason: 'No active chat' };
      let conn;
      try{
        const snap = await firebase.getDoc(firebase.doc(this.db,'chatConnections',connId));
        if (!snap.exists()) return { ok: false, reason: 'Chat not found' };
        conn = snap.data() || {};
      }catch(_){ return { ok: true }; }
      const participants = this.getConnParticipants(conn);
      if (participants.length !== 2) return { ok: true };
      const peerUid = participants.find(u=> u !== this.currentUser.uid);
      if (!peerUid) return { ok: true };
      const state = await this.getConnectionStateWithPeer(peerUid);
      if (state.status === 'connected') return { ok: true };
      let peerName = 'This user';
      try{
        const peerData = await window.firebaseService.getUserData(peerUid);
        peerName = (peerData && (peerData.username || peerData.email)) || peerName;
        if (!peerData || peerData.allowMessagesFromUnconnected !== false) return { ok: true };
      }catch(_){ return { ok: true }; }
      return { ok: false, reason: `${peerName} disallowed messages with unconnected users` };
    }

    async sendCurrent(){
      const input = document.getElementById('message-input');
      const text = input.value.trim();
      const queuedFiles = (this._pendingAttachments || []).map((x)=> x && x.file).filter((f)=> f instanceof File);
      const queuedShared = (this._pendingRemoteShares || []).map((x)=> x && x.sharedAsset).filter((x)=> x && typeof x === 'object');
      const queuedReused = (this._pendingReusedAttachments || []).filter((x)=> x && x.fileUrl);
      if ((!text && !queuedFiles.length && !queuedShared.length && !queuedReused.length) || !this.activeConnection) return;
      const can = await this.canSendToActiveConnection();
      if (!can.ok){
        alert(can.reason || 'Cannot send message');
        return;
      }
      input.value = '';
      this.clearPendingAttachments();
      this.publishTypingState(false, { force: true }).catch(()=>{});
      try{
        const mediaRank = (it)=> (this.isImageFilename(it.fileName) ? 0 : (this.isVideoFilename(it.fileName) ? 1 : (this.isAudioFilename(it.fileName) ? 2 : 3)));
        const shouldCombine = (queuedFiles.length + queuedReused.length) > 1 || ((queuedFiles.length + queuedReused.length) >= 1 && !!text.trim());
        if (shouldCombine && (queuedFiles.length || queuedReused.length)){
          let mediaItems = [];
          if (queuedFiles.length){
            const up = await this.uploadFilesForBatch(queuedFiles);
            mediaItems = mediaItems.concat(up.uploaded || []);
            if (up.failedFiles && up.failedFiles.length) this.queueAttachments(up.failedFiles);
          }
          queuedReused.forEach((r)=>{
            const msg = r.message || {};
            mediaItems.push({
              fileUrl: r.fileUrl,
              fileName: r.fileName,
              attachmentKeySalt: String(msg.attachmentKeySalt || '').trim() || null,
              attachmentSourceConnId: this.activeConnection,
              isVideoRecording: !!msg.isVideoRecording
            });
          });
          mediaItems.sort((a,b)=> mediaRank(a) - mediaRank(b));
          const combinedText = text.trim() || '';
          await this.saveMessage({ text: combinedText, media: mediaItems, attachmentSourceConnId: this.activeConnection });
          mediaItems.forEach((it)=> this.pushRecentAttachment({ fileUrl: it.fileUrl, fileName: it.fileName, sentAt: new Date().toISOString() }));
        } else {
          if (text) await this.saveMessage({ text });
          if (queuedFiles.length){
            const result = await this.sendFiles(queuedFiles, { silent: true });
            if (result.failedFiles && result.failedFiles.length){
              this.queueAttachments(result.failedFiles);
              alert(`Sent ${result.sentCount || 0} attachments, ${result.failedFiles.length} failed. Failed files are still in queue.`);
            }
          }
          for (const r of queuedReused){
            const msg = r.message || {};
            await this.saveMessage({
              text: this.isAudioFilename(r.fileName) ? '[voice message]' : (this.isVideoFilename(r.fileName) ? '[video message]' : `[file] ${r.fileName}`),
              fileUrl: r.fileUrl,
              fileName: r.fileName,
              attachmentSourceConnId: this.activeConnection,
              attachmentKeySalt: String(msg.attachmentKeySalt || '').trim() || null,
              isVideoRecording: !!msg.isVideoRecording
            });
          }
        }
        for (const sharedAsset of queuedShared){
          await this.saveMessage({ text: '[shared]', sharedAsset });
        }
      }catch(e){
        input.value = text;
        if (queuedFiles.length) this.queueAttachments(queuedFiles);
        if (queuedShared.length) this.queueRemoteSharedAssets(queuedShared.map((a)=> ({ sharedAsset: a })));
        if (queuedReused.length) this._pendingReusedAttachments = (this._pendingReusedAttachments || []).concat(queuedReused);
        this.publishTypingState(!!text, { force: true }).catch(()=>{});
        throw e;
      }
    }

    buildSharePayload(rawText, fileUrl, fileName, sourceConnId = this.activeConnection, attachmentKeySalt = '', sourceMessage = null, sourceSenderName = ''){
      const inferredName = String(fileName || '').trim();
      let nextText = String(rawText || '').trim();
      if (fileUrl){
        if (this.isAudioFilename(inferredName)) nextText = '[voice message]';
        else if (this.isVideoFilename(inferredName)) nextText = '[video message]';
        else if (!nextText || /^\[file\]/i.test(nextText)) nextText = '[file]';
      }
      if (!nextText && fileUrl) nextText = '[file]';
      const src = sourceMessage && typeof sourceMessage === 'object' ? sourceMessage : {};
      const derivedSharedAsset = (()=> {
        if (src && typeof src.sharedAsset === 'object') return src.sharedAsset;
        const inferredKind = String(src.kind || src.type || (src.post || src.postId ? 'post' : '')).toLowerCase();
        if (inferredKind === 'post'){
          return {
            kind: 'post',
            postId: String(src.postId || src?.post?.id || '').trim() || null,
            title: String(src?.post?.text || src.title || 'Post'),
            by: String(src?.post?.authorName || src.by || ''),
            post: (src.post && typeof src.post === 'object') ? src.post : null
          };
        }
        return null;
      })();
      const originalAuthorUid = String(src.sharedOriginalAuthorUid || src.sender || '').trim() || null;
      const originalAuthorName = String(src.sharedOriginalAuthorName || sourceSenderName || '').trim() || null;
      const isVideoRecording = !!(sourceMessage?.isVideoRecording === true);
      return {
        text: nextText,
        fileUrl: fileUrl || null,
        fileName: inferredName || null,
        sharedAsset: derivedSharedAsset,
        attachmentSourceConnId: sourceConnId || null,
        attachmentKeySalt: String(attachmentKeySalt || '').trim() || null,
        isVideoRecording,
        isShared: true,
        sharedFromConnId: String(sourceConnId || '').trim() || null,
        sharedFromMessageId: String(src.id || '').trim() || null,
        sharedOriginalAuthorUid: originalAuthorUid,
        sharedOriginalAuthorName: originalAuthorName
      };
    }

    extractConnIdFromAttachmentUrl(fileUrl){
      try{
        const raw = String(fileUrl || '');
        if (!raw) return '';
        let decoded = raw;
        try{ decoded = decodeURIComponent(raw); }catch(_){ decoded = raw; }
        let m = /(?:^|\/)chat\/([^/]+)\//i.exec(decoded);
        if (m && m[1]) return m[1];
        // Firebase Storage: .../o/chat%2FconnId%2F... — path in "o" segment
        const oMatch = /\/o\/([^?#]+)/i.exec(raw);
        if (oMatch && oMatch[1]) {
          try {
            const pathDecoded = decodeURIComponent(oMatch[1]);
            m = /chat\/([^/]+)\//i.exec(pathDecoded);
            if (m && m[1]) return m[1];
          }catch(_){}
        }
        // Encoded path: chat%2FconnId%2F... when full decode fails
        const enc = /chat%2F([^%?#/]+)(?:%2F|\/|$)/i.exec(raw);
        if (enc && enc[1]) return (decodeURIComponent(enc[1]) || enc[1]).replace(/^["'\s]+|["'\s]+$/g, '');
        return '';
      }catch(_){ return ''; }
    }

    resolveAttachmentSourceConnId(message, fallbackConnId = this.activeConnection){
      try{
        const explicit = String(message?.attachmentSourceConnId || '').trim();
        if (explicit) return explicit;
        const fromUrl = this.extractConnIdFromAttachmentUrl(message?.fileUrl || '');
        if (fromUrl) return fromUrl;
      }catch(_){ }
      return fallbackConnId || this.activeConnection;
    }

    getConnectionDisplayName(conn){
      try{
        const parts = this.getConnParticipants(conn || {});
        const names = Array.isArray(conn?.participantUsernames) ? conn.participantUsernames : [];
        const mine = (this.me?.username || this.currentUser?.email || '').toLowerCase();
        const resolved = parts.map((uid, i)=> names[i] || this.usernameCache.get(uid) || uid).filter(Boolean);
        const others = resolved.filter((n)=> String(n || '').toLowerCase() !== mine);
        if (!others.length) return 'Chat';
        if (others.length === 1) return String(others[0]);
        return `${others[0]}, ${others[1]}${others.length > 2 ? ` +${others.length - 2}` : ''}`;
      }catch(_){ return 'Chat'; }
    }

    toPlainObject(val){
      if (val === null || val === undefined) return null;
      if (typeof val !== 'object') return val;
      try { return JSON.parse(JSON.stringify(val)); } catch(_){ return null; }
    }

    async saveMessageToConnection(connId, { text, fileUrl, fileName, sharedAsset, media, attachmentSourceConnId, attachmentKeySalt, isVideoRecording, isShared, sharedFromConnId, sharedFromMessageId, sharedOriginalAuthorUid, sharedOriginalAuthorName }){
      const aesKey = await this.getFallbackKeyForConn(connId);
      const cipher = await chatCrypto.encryptWithKey(text, aesKey);
      const mediaArr = Array.isArray(media) && media.length ? this.toPlainObject(media) : null;
      const firstMedia = mediaArr && mediaArr[0] ? mediaArr[0] : null;
      const legacyFileUrl = fileUrl || (firstMedia && firstMedia.fileUrl) || null;
      const legacyFileName = fileName || (firstMedia && firstMedia.fileName) || null;
      const previewText = this.stripPlaceholderText(text) || (legacyFileName ? `[Attachment] ${legacyFileName}` : (mediaArr ? `[${mediaArr.length} attachments]` : ''));
      const msgRef = firebase.doc(firebase.collection(this.db,'chatMessages',connId,'messages'));
      const doc = {
        id: msgRef.id,
        connId,
        sender: this.currentUser.uid,
        cipher,
        fileUrl: legacyFileUrl,
        fileName: legacyFileName,
        sharedAsset: (sharedAsset && typeof sharedAsset === 'object') ? this.toPlainObject(sharedAsset) : null,
        media: mediaArr,
        attachmentSourceConnId: String(attachmentSourceConnId || connId || '').trim() || null,
        attachmentKeySalt: String(attachmentKeySalt || (firstMedia && firstMedia.attachmentKeySalt) || '').trim() || null,
        isVideoRecording: isVideoRecording === true || (firstMedia && firstMedia.isVideoRecording === true),
        isShared: !!isShared,
        sharedFromConnId: String(sharedFromConnId || '').trim() || null,
        sharedFromMessageId: String(sharedFromMessageId || '').trim() || null,
        sharedOriginalAuthorUid: String(sharedOriginalAuthorUid || '').trim() || null,
        sharedOriginalAuthorName: String(sharedOriginalAuthorName || '').trim() || null,
        previewText: previewText.slice(0, 220),
        createdAt: new Date().toISOString(),
        createdAtTS: firebase.serverTimestamp()
      };
      if (!mediaArr) delete doc.media;
      const ts = doc.createdAtTS;
      delete doc.createdAtTS;
      const plainDoc = JSON.parse(JSON.stringify(doc));
      plainDoc.createdAtTS = ts;
      await firebase.setDoc(msgRef, plainDoc);
      await firebase.updateDoc(firebase.doc(this.db,'chatConnections',connId),{
        lastMessage: String(text || '').slice(0,200),
        updatedAt: new Date().toISOString()
      });
      this.sendPushForMessageForConnection(connId, text);
    }

    async sendPushForMessageForConnection(connId, text){
      Promise.resolve().then(async ()=>{
        try{
          if (!(window.firebaseService && typeof window.firebaseService.callFunction === 'function')) return;
          const connSnap = await firebase.getDoc(firebase.doc(this.db,'chatConnections',connId));
          const data = connSnap.exists() ? (connSnap.data() || {}) : {};
          const participantUids = this.getConnParticipants(data);
          const recipients = participantUids.filter((uid)=> uid && uid !== this.currentUser.uid);
          if (!recipients.length) return;
          const payload = { connId, recipients, preview: String(text || '').slice(0,120) };
          let delayMs = 250;
          for (let i = 0; i < 3; i++){
            try{ await window.firebaseService.callFunction('sendPush', payload); return; }
            catch(_){ if (i === 2) return; await new Promise((r)=> setTimeout(r, delayMs)); delayMs *= 2; }
          }
        }catch(_){ }
      });
    }

    async openShareMessageSheet(payload){
      const existing = document.getElementById('msg-share-sheet');
      const existingBackdrop = document.getElementById('msg-share-backdrop');
      if (existing){ existing.remove(); if (existingBackdrop) existingBackdrop.remove(); }
      const rawTargets = [];
      const seenTargetIds = new Set();
      const pushTarget = (c)=>{
        const id = String(c?.id || '').trim();
        if (!id || id === String(this.activeConnection || '').trim() || seenTargetIds.has(id)) return;
        seenTargetIds.add(id);
        rawTargets.push(c);
      };
      (this.connections || []).forEach(pushTarget);
      try{
        const meUid = String(this.currentUser?.uid || '').trim();
        if (meUid){
          const pull = async (q)=>{
            const s = await firebase.getDocs(q);
            s.forEach((d)=> pushTarget({ id: d.id, ...(d.data() || {}) }));
          };
          await pull(firebase.query(firebase.collection(this.db,'chatConnections'), firebase.where('participants','array-contains', meUid), firebase.limit(220)));
          try{ await pull(firebase.query(firebase.collection(this.db,'chatConnections'), firebase.where('users','array-contains', meUid), firebase.limit(220))); }catch(_){ }
          try{ await pull(firebase.query(firebase.collection(this.db,'chatConnections'), firebase.where('memberIds','array-contains', meUid), firebase.limit(220))); }catch(_){ }
          // Legacy/key-only docs fallback for chats missing participant arrays.
          try{
            let allSnap;
            try{
              allSnap = await firebase.getDocs(firebase.query(
                firebase.collection(this.db,'chatConnections'),
                firebase.orderBy('updatedAt','desc'),
                firebase.limit(700)
              ));
            }catch(_){
              allSnap = await firebase.getDocs(firebase.collection(this.db,'chatConnections'));
            }
            allSnap.forEach((d)=>{
              const row = { id: d.id, ...(d.data() || {}) };
              const keyParts = String(row.key || '').split('|').filter(Boolean);
              if (keyParts.includes(meUid)) pushTarget(row);
            });
          }catch(_){ }
          // Last resort scan to catch transient query/index misses.
          if (!rawTargets.length){
            try{
              const any = await firebase.getDocs(firebase.collection(this.db,'chatConnections'));
              any.forEach((d)=>{
                const row = { id: d.id, ...(d.data() || {}) };
                const parts = this.getConnParticipants(row || {});
                const keyParts = String(row.key || '').split('|').filter(Boolean);
                if (parts.includes(meUid) || keyParts.includes(meUid)) pushTarget(row);
              });
            }catch(_){ }
          }
        }
      }catch(_){ }
      const targetMap = new Map();
      rawTargets.forEach((c)=>{
        if (!c || !c.id) return;
        const parts = this.getConnParticipants(c).filter(Boolean);
        const isGroup = parts.length > 2 || !!String(c.groupName || '').trim() || !!String(c.groupCoverUrl || '').trim();
        const dmKey = (parts.length >= 2) ? parts.slice().sort().join('|') : `id:${String(c.id || '')}`;
        const key = isGroup ? `group:${c.id}` : `dm:${dmKey}`;
        const prev = targetMap.get(key);
        if (!prev){
          targetMap.set(key, c);
          return;
        }
        const prevTs = Number(new Date(prev.updatedAt || 0).getTime() || 0);
        const curTs = Number(new Date(c.updatedAt || 0).getTime() || 0);
        const prevArchived = prev.archived === true || !!String(prev.mergedInto || '').trim();
        const curArchived = c.archived === true || !!String(c.mergedInto || '').trim();
        if (prevArchived && !curArchived){ targetMap.set(key, c); return; }
        if (curArchived && !prevArchived) return;
        if (curTs >= prevTs) targetMap.set(key, c);
      });
      const dedupedTargets = Array.from(targetMap.values())
        .sort((a,b)=> Number(new Date(b.updatedAt || 0).getTime() || 0) - Number(new Date(a.updatedAt || 0).getTime() || 0));
      if (!dedupedTargets.length){ alert('No other chats to share into yet'); return; }

      const targetMetaById = new Map();
      const resolveMeta = async (c)=>{
        try{
          const conn = c || {};
          const parts = this.getConnParticipants(conn).filter(Boolean);
          const isGroup = parts.length > 2 || !!String(conn.groupName || '').trim() || !!String(conn.groupCoverUrl || '').trim();
          if (isGroup){
            const title = String(conn.groupName || this.getConnectionDisplayName(conn) || 'Group chat').trim();
            const cover = String(conn.groupCoverUrl || '../../images/default-bird.png').trim() || '../../images/default-bird.png';
            return { title, subtitle: 'Group chat', cover };
          }
          const peerUid = parts.find((uid)=> uid && uid !== this.currentUser.uid) || '';
          let title = String(this.getConnectionDisplayName(conn) || 'Chat').trim();
          let cover = '../../images/default-bird.png';
          if (peerUid){
            const cachedAvatar = this._avatarCache.get(peerUid);
            if (cachedAvatar) cover = String(cachedAvatar || cover);
            const cached = this.usernameCache.get(peerUid);
            if (cached && typeof cached === 'object'){
              if (cached.username) title = String(cached.username);
              if (cached.avatarUrl) cover = String(cached.avatarUrl);
            }else if (typeof cached === 'string' && cached.trim()){
              title = cached.trim();
            }
            try{
              const u = await window.firebaseService.getUserData(peerUid);
              if (u){
                const uname = String(u.username || u.email || title || 'Chat').trim();
                const avatar = String(u.avatarUrl || cover || '../../images/default-bird.png').trim() || '../../images/default-bird.png';
                title = uname;
                cover = avatar;
                this.usernameCache.set(peerUid, { username: uname, avatarUrl: avatar });
                this._avatarCache.set(peerUid, avatar);
              }
            }catch(_){ }
          }
          return { title, subtitle: 'Direct chat', cover };
        }catch(_){
          return { title: 'Chat', subtitle: '', cover: '../../images/default-bird.png' };
        }
      };
      await Promise.all(dedupedTargets.map(async (c)=>{
        targetMetaById.set(c.id, await resolveMeta(c));
      }));

      const backdrop = document.createElement('div');
      backdrop.id = 'msg-share-backdrop';
      backdrop.style.cssText = 'position:fixed;inset:0;z-index:102;background:rgba(0,0,0,.24)';
      const panel = document.createElement('div');
      panel.id = 'msg-share-sheet';
      panel.style.cssText = 'position:fixed;left:10px;right:10px;bottom:calc(96px + env(safe-area-inset-bottom));max-height:min(62vh,480px);overflow:auto;background:#10141c;border:1px solid #2a2f36;border-radius:12px;z-index:103;padding:10px';
      panel.innerHTML = `<div style="font-weight:600;margin-bottom:8px">Share message to chat</div><input id="share-chat-filter" type="text" placeholder="Search chats..." style="width:100%;padding:8px 10px;border-radius:8px;border:1px solid #2a2f36;background:#0f1116;color:#fff;margin-bottom:8px">`;
      const list = document.createElement('div');
      panel.appendChild(list);

      const render = (term = '')=>{
        const q = String(term || '').trim().toLowerCase();
        list.innerHTML = '';
        dedupedTargets
          .filter((c)=>{
            const m = targetMetaById.get(c.id) || {};
            const hay = `${String(m.title || this.getConnectionDisplayName(c) || '')} ${String(m.subtitle || '')}`.toLowerCase();
            return hay.includes(q);
          })
          .forEach((c)=>{
            const meta = targetMetaById.get(c.id) || { title: this.getConnectionDisplayName(c), subtitle: '', cover: '../../images/default-bird.png' };
            const row = document.createElement('button');
            row.className = 'btn secondary';
            row.style.cssText = 'display:flex;align-items:center;gap:10px;width:100%;text-align:left;margin-bottom:6px;padding:8px 10px;border-radius:10px';
            row.innerHTML = `<img src="${String(meta.cover || '../../images/default-bird.png').replace(/"/g,'&quot;')}" alt="" style="width:28px;height:28px;border-radius:8px;object-fit:cover;flex:0 0 auto"><span style="min-width:0;display:flex;flex-direction:column"><span style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${String(meta.title || 'Chat').replace(/</g,'&lt;')}</span><span style="opacity:.72;font-size:11px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${String(meta.subtitle || '').replace(/</g,'&lt;')}</span></span>`;
            row.onclick = async ()=>{
              const can = await this.canSendToConnection(c.id);
              if (!can.ok){ alert(can.reason || 'Cannot share into this chat'); return; }
              await this.saveMessageToConnection(c.id, payload);
              panel.remove();
              backdrop.remove();
            };
            list.appendChild(row);
          });
      };
      render('');

      const filter = panel.querySelector('#share-chat-filter');
      if (filter) filter.addEventListener('input', ()=> render(filter.value));
      backdrop.addEventListener('click', ()=>{ panel.remove(); backdrop.remove(); });
      panel.addEventListener('click', (e)=> e.stopPropagation());
      document.body.appendChild(backdrop);
      document.body.appendChild(panel);
    }

    formatTypingText(names){
      const list = (names || []).filter(Boolean);
      if (!list.length) return '';
      if (list.length === 1) return `${list[0]} is typing...`;
      if (list.length === 2) return `${list[0]} and ${list[1]} are typing...`;
      return `${list.slice(0, 2).join(', ')} and ${list.length - 2} others are typing...`;
    }

    renderTypingIndicator(){
      const el = document.getElementById('typing-indicator');
      if (!el) return;
      const now = Date.now();
      const names = Object.entries(this._typingByUid || {})
        .filter(([uid, row])=>{
          if (!uid || uid === this.currentUser.uid) return false;
          if (!row || row.active !== true) return false;
          const ts = new Date(row.updatedAt || 0).getTime() || 0;
          return (now - ts) < 9000;
        })
        .map(([uid, row])=> row.username || this.usernameCache.get(uid) || 'Someone');
      if (!names.length){
        el.textContent = '';
        el.classList.remove('show');
        return;
      }
      el.textContent = this.formatTypingText(names);
      el.classList.add('show');
    }

    async publishTypingState(active, options = {}){
      try{
        const connId = options.connId || this.activeConnection;
        if (!connId || !this.currentUser?.uid) return;
        const next = !!active;
        const now = Date.now();
        if (!options.force && this._typingLastSent === next && (now - this._typingLastSentAt) < 700){
          return;
        }
        const uname = this.me?.username || this.currentUser.email || this.currentUser.uid;
        const payload = { active: next, username: uname, updatedAt: new Date().toISOString() };
        await firebase.updateDoc(firebase.doc(this.db,'chatConnections', connId), {
          [`typing.${this.currentUser.uid}`]: payload
        });
        this._typingLastSent = next;
        this._typingLastSentAt = now;
      }catch(_){ }
    }

    syncTypingFromInput(){
      const input = document.getElementById('message-input');
      if (!input) return;
      const hasText = !!String(input.value || '').trim();
      this.publishTypingState(hasText).catch(()=>{});
    }

    startTypingListener(connId){
      if (!connId || !firebase.onSnapshot) return;
      this.stopTypingListener();
      try{
        const ref = firebase.doc(this.db,'chatConnections', connId);
        this._typingUnsub = firebase.onSnapshot(ref, (snap)=>{
          const data = snap.exists() ? (snap.data() || {}) : {};
          this._typingByUid = (data && data.typing && typeof data.typing === 'object') ? data.typing : {};
          this.renderTypingIndicator();
        }, ()=>{});
        this._typingTicker = setInterval(()=> this.renderTypingIndicator(), 1500);
      }catch(_){ }
    }

    stopTypingListener(){
      try{ if (this._typingUnsub){ this._typingUnsub(); this._typingUnsub = null; } }catch(_){ }
      if (this._typingTicker){ clearInterval(this._typingTicker); this._typingTicker = null; }
      this._typingByUid = {};
      const el = document.getElementById('typing-indicator');
      if (el){
        el.textContent = '';
        el.classList.remove('show');
      }
    }

    async uploadFilesForBatch(files){
      const result = { uploaded: [], failedFiles: [] };
      const targetConnId = this.activeConnection;
      if (!files || !files.length || !targetConnId || !this.storage) return result;
      try{
        const salts = await this.getConnSaltForConn(targetConnId);
        const aesKey = await this.getFallbackKeyForConn(targetConnId);
        const salt = String(salts?.stableSalt || targetConnId || '');
        for (const f of files){
          try{
            const base64 = await new Promise((resolve, reject)=>{
              const reader = new FileReader();
              reader.onload = ()=>{ const s = String(reader.result || ''); resolve(s.includes(',') ? s.split(',')[1] : ''); };
              reader.onerror = reject;
              reader.readAsDataURL(f);
            });
            const cipher = await chatCrypto.encryptWithKey(base64, aesKey);
            const blob = new Blob([JSON.stringify(cipher)], { type: 'application/json' });
            const safeName = f.name.replace(/[^a-zA-Z0-9._-]/g, '_');
            const r = firebase.ref(this.storage, `chat/${targetConnId}/${Date.now()}_${safeName}.enc.json`);
            await firebase.uploadBytes(r, blob, { contentType: 'application/json' });
            const url = await firebase.getDownloadURL(r);
            result.uploaded.push({
              fileUrl: url,
              fileName: f.name,
              attachmentKeySalt: salt,
              attachmentSourceConnId: targetConnId,
              isVideoRecording: false
            });
          }catch(_){ result.failedFiles.push(f); }
        }
      }catch(_){ }
      return result;
    }

    async sendFiles(files, opts = {}){
      const silent = !!opts.silent;
      const result = { sentCount: 0, failedFiles: [] };
      const targetConnId = this.activeConnection;
      if (!files || !files.length || !targetConnId) { console.warn('No files or no active connection'); return result; }
      const can = await this.canSendToConnection(targetConnId);
      if (!can.ok){
        if (!silent) alert(can.reason || 'Cannot send attachments');
        return result;
      }
      if (!this.storage) {
        if (!silent) alert('File upload is not available because Firebase Storage is not configured.');
        return result;
      }
      try{
        const cRef = firebase.doc(this.db, 'chatConnections', targetConnId);
        const cSnap = await firebase.getDoc(cRef);
        const participants = cSnap.exists()
          ? (Array.isArray(cSnap.data().participants)
              ? cSnap.data().participants
              : (Array.isArray(cSnap.data().users)
                  ? cSnap.data().users
                  : (Array.isArray(cSnap.data().memberIds) ? cSnap.data().memberIds : [])))
          : [];
        if (!participants.includes(this.currentUser.uid)) {
          if (!silent) alert('You are not a participant of this chat. Please reopen the chat and try again.');
          return result;
        }
      }catch(_){ /* best-effort pre-check */ }
      console.log('Auth state before sendFiles:', !!this.currentUser, firebase.auth().currentUser?.uid);
      try {
        await firebase.auth().currentUser?.getIdToken(true); // Force refresh
        if (!firebase.auth().currentUser) throw new Error('Auth lost - please re-login');
      } catch (err) {
        console.error('Auth refresh failed before sendFiles:', err);
        if (!silent) alert('Auth error - please reload and re-login');
        return result;
      }
      for (const f of files){
        try {
          console.log('Sending file:', f.name);
          const salts = await this.getConnSaltForConn(targetConnId);
          const aesKey = await this.getFallbackKeyForConn(targetConnId);
          // Read file as base64 via FileReader to avoid large argument spreads
          const base64 = await new Promise((resolve, reject)=>{
            try{
              const reader = new FileReader();
              reader.onload = ()=>{
                const result = String(reader.result || '');
                const b64 = result.includes(',') ? result.split(',')[1] : '';
                resolve(b64);
              };
              reader.onerror = reject;
              reader.readAsDataURL(f);
            }catch(e){ reject(e); }
          });
          // Encrypt base64 string
          const cipher = await chatCrypto.encryptWithKey(base64, aesKey);
          // Store encrypted JSON payload with .json extension to aid CORS/content-type and preview
          const blob = new Blob([JSON.stringify(cipher)], {type:'application/json'});
          const safeName = f.name.replace(/[^a-zA-Z0-9._-]/g,'_');
          const r = firebase.ref(this.storage, `chat/${targetConnId}/${Date.now()}_${safeName}.enc.json`);
          console.log('File upload started');
          await firebase.uploadBytes(r, blob, { contentType: 'application/json' });
          const url = await firebase.getDownloadURL(r);
          console.log('File upload completed');
          const isVideo = this.isVideoFilename(f.name);
          const text = isVideo ? '[video message]' : (this.isAudioFilename(f.name) ? '[voice message]' : `[file] ${f.name}`);
          await this.saveMessage({
            text,
            fileUrl:url,
            fileName:f.name,
            connId: targetConnId,
            attachmentSourceConnId: targetConnId,
            attachmentKeySalt: String(salts?.stableSalt || targetConnId || ''),
            isVideoRecording: false
          });
          this.pushRecentAttachment({ fileUrl: url, fileName: f.name, sentAt: new Date().toISOString() });
          result.sentCount += 1;
        } catch (err) {
          console.error('Send file error details:', err.code, err.message, err);
          result.failedFiles.push(f);
          if (!silent) alert('Failed to send file: ' + err.message);
        }
      }
      if (!silent && result.failedFiles.length && result.sentCount > 0){
        alert(`Sent ${result.sentCount} attachments, ${result.failedFiles.length} failed.`);
      }
      return result;
    }

    /* Stickerpacks */
    async toggleStickers(){
      const existing = document.getElementById('sticker-panel');
      const existingBackdrop = document.getElementById('sticker-backdrop');
      if (existing){
        existing.remove();
        if (existingBackdrop) existingBackdrop.remove();
        return;
      }
      if (existingBackdrop) existingBackdrop.remove();
      const backdrop = document.createElement('div');
      backdrop.id = 'sticker-backdrop';
      backdrop.className = 'sticker-backdrop';
      const panel = document.createElement('div'); panel.id='sticker-panel'; panel.className='sticker-panel';
      panel.innerHTML = `
        <div class="panel-header">
          <strong>Library</strong>
        </div>
        <div class="sticker-top-tabs">
          <button class="tab active" data-tab="stickers">Stickers</button>
          <button class="tab" data-tab="emoji">Emoji</button>
          <button class="tab" data-tab="gifs">GIFs</button>
        </div>
        <div class="sticker-pack-manage" id="sticker-pack-manage">
          <button class="icon-btn add" id="add-pack-btn" title="Add pack"><i class="fas fa-plus"></i></button>
          <button class="icon-btn" id="manage-packs-btn" title="Manage packs"><i class="fas fa-gear"></i></button>
        </div>
        <div id="sticker-pack-list" class="sticker-pack-list"></div>
        <div id="sticker-grid" class="sticker-grid"></div>
        <input id="sticker-pack-input" type="file" accept="image/*" multiple style="display:none" />
        <input id="sticker-pack-add-image" type="file" accept="image/*" multiple style="display:none" />
      `;
      const host = document.querySelector('.main') || document.body;
      host.appendChild(backdrop);
      host.appendChild(panel);
      backdrop.addEventListener('click', ()=>{
        panel.remove();
        backdrop.remove();
      });
      panel.addEventListener('click', (e)=> e.stopPropagation());
      document.getElementById('add-pack-btn').onclick = ()=> document.getElementById('sticker-pack-input').click();
      document.getElementById('sticker-pack-input').onchange = (e)=> this.addStickerFiles(e.target.files);
      document.getElementById('manage-packs-btn').onclick = ()=> this.manageStickerpacks();
      panel.querySelectorAll('[data-tab]').forEach(btn=>{
        btn.addEventListener('click', async ()=>{
          panel.querySelectorAll('[data-tab]').forEach(b=> b.classList.remove('active'));
          btn.classList.add('active');
          await this.renderStickerGrid(btn.dataset.tab || 'stickers');
        });
      });
      await this.renderStickerGrid('stickers');
    }

    async getStickerIndex(){
      try{
        const raw = localStorage.getItem('liber_stickerpacks');
        const obj = raw ? JSON.parse(raw) : { packs: [] };
        if (!Array.isArray(obj.packs)) obj.packs = [];
        return obj;
      }catch(_){ return { packs: [] }; }
    }
    async setStickerIndex(idx){ localStorage.setItem('liber_stickerpacks', JSON.stringify(idx)); }

    async addStickerFiles(fileList){
      if (!fileList || !fileList.length) return;
      const idx = await this.getStickerIndex();
      // Current pack is timestamp-based
      const packId = 'pack_'+Date.now();
      const pack = { id: packId, name: 'My pack '+new Date().toLocaleDateString(), items: [] };
      // Store PNGs to Firebase Storage encrypted, keep manifest locally with storage URLs
      for (const f of fileList){
        if (!/^image\//i.test(f.type || '')) continue;
        const safeName = (f.name || 'sticker.png').replace(/[^a-zA-Z0-9._-]/g,'_');
        try{
          const aesKey = await this.getFallbackKey();
          const base64 = await new Promise((resolve,reject)=>{ const r=new FileReader(); r.onload=()=>{ const s=String(r.result||''); resolve(s.includes(',')?s.split(',')[1]:''); }; r.onerror=reject; r.readAsDataURL(f); });
          const cipher = await chatCrypto.encryptWithKey(base64, aesKey);
          const path = `stickers/${this.currentUser.uid}/${Date.now()}_${safeName}.enc.json`;
          const sref = firebase.ref(this.storage, path);
          await firebase.uploadBytes(sref, new Blob([JSON.stringify(cipher)], {type:'application/json'}), { contentType: 'application/json' });
          const url = await firebase.getDownloadURL(sref);
          pack.items.push({ name: safeName, url });
        }catch(_){
          try{
            const localDataUrl = await new Promise((resolve,reject)=>{ const r=new FileReader(); r.onload=()=> resolve(String(r.result||'')); r.onerror=reject; r.readAsDataURL(f); });
            if (localDataUrl) pack.items.push({ name: f.name || safeName, dataUrl: localDataUrl, local: true });
          }catch(__){ /* skip failed file */ }
        }
      }
      if (pack.items.length){ idx.packs.unshift(pack); await this.setStickerIndex(idx); await this.renderStickerGrid(); }
    }

    async renderStickerGrid(tab = 'stickers'){
      const grid = document.getElementById('sticker-grid'); if (!grid) return;
      const packList = document.getElementById('sticker-pack-list');
      const manageBar = document.getElementById('sticker-pack-manage');
      grid.innerHTML='';
      if (packList) packList.innerHTML = '';
      if (manageBar) manageBar.style.display = tab === 'stickers' ? 'flex' : 'none';
      if (packList) packList.style.display = tab === 'stickers' ? 'grid' : 'none';

      if (tab === 'emoji'){
        const emojis = '😀 😃 😄 😁 😆 😅 😂 🤣 😊 😇 🙂 🙃 😉 😌 😍 🥰 😘 😗 😙 😚 😋 😛 😝 😜 🤪 🤨 🧐 🤓 😎 🥸 🤩 🥳 😏 😒 😞 😔 😟 😕 🙁 ☹️ 😣 😖 😫 😩 🥺 😢 😭 😤 😠 😡 🤬 🤯 😳 🥵 🥶 😱 😨 😰 😥 😓 🤗 🤔 🫡 🤭 🫢 🤫 🤥 😶 😐 😑 😬 🫠 🙄 😯 😦 😧 😮 😲 🥱 😴 🤤 😪 😵 😵‍💫 🤐 🥴 🤢 🤮 🤧 😷 🤒 🤕 🤑 🤠 😈 👿 👹 👺 🤡 💩 👻 💀 ☠️ 👽 🤖 🎃 😺 😸 😹 😻 😼 😽 🙀 😿 😾 ❤️ 🩷 🧡 💛 💚 💙 🩵 💜 🤎 🖤 🤍 💯 👍 👎 🙌 👏 🙏 🤝 💪 👀 🚀 ✨ 🔥 🎉 🎵'.split(/\s+/);
        emojis.forEach(ch=>{
          const cell = document.createElement('div');
          cell.className = 'sticker-pack-chip';
          cell.textContent = ch;
          cell.style.fontSize = '56px';
          cell.addEventListener('click', async ()=> this.saveMessage({ text: ch }));
          grid.appendChild(cell);
        });
        return;
      }

      if (tab === 'gifs'){
        try{
          const resp = await fetch('https://tenor.googleapis.com/v2/featured?key=LIVDSRZULELA&limit=18&media_filter=tinygif');
          const json = await resp.json();
          const rows = Array.isArray(json?.results) ? json.results : [];
          if (!rows.length) throw new Error('No gifs');
          rows.forEach((g)=>{
            const media = g?.media_formats?.tinygif || g?.media_formats?.gif;
            const url = media?.url;
            if (!url) return;
            const img = document.createElement('img');
            img.src = url;
            img.alt = g?.content_description || 'gif';
            img.style.width = '70px';
            img.style.height = '70px';
            img.style.objectFit = 'cover';
            img.style.borderRadius = '8px';
            img.style.cursor = 'pointer';
            img.addEventListener('click', async ()=> this.saveMessage({ text: `[gif] ${url}` }));
            grid.appendChild(img);
          });
        }catch(_){
          const fallback = [
            'https://media.tenor.com/3x63SNMKPogAAAAM/hello.gif',
            'https://media.tenor.com/uR6w0kQx5jEAAAAM/happy-dance.gif',
            'https://media.tenor.com/XC6F6o5s0rUAAAAM/thumbs-up.gif',
            'https://media.tenor.com/VgG8o1HqzqgAAAAM/love-heart.gif',
            'https://media.tenor.com/9ptm6v4jYQYAAAAM/party-celebrate.gif',
            'https://media.tenor.com/eYQw9hVh7xMAAAAM/cat-funny.gif'
          ];
          fallback.forEach((url)=>{
            const img = document.createElement('img');
            img.src = url;
            img.alt = 'gif';
            img.style.width = '70px';
            img.style.height = '70px';
            img.style.objectFit = 'cover';
            img.style.borderRadius = '8px';
            img.style.cursor = 'pointer';
            img.addEventListener('click', async ()=> this.saveMessage({ text: `[gif] ${url}` }));
            grid.appendChild(img);
          });
        }
        return;
      }

      const idx = await this.getStickerIndex();
      const selectedPackId = this._selectedStickerPackId || (idx.packs[0] && idx.packs[0].id) || null;
      this._selectedStickerPackId = selectedPackId;
      idx.packs.forEach((p)=>{
        const chip = document.createElement('button');
        chip.className = 'sticker-pack-chip' + (p.id === selectedPackId ? ' active' : '');
        chip.title = p.name;
        if (p.items && p.items[0]){
          const thumb = document.createElement('img');
          thumb.alt = p.name;
          thumb.style.width = '48px';
          thumb.style.height = '48px';
          thumb.style.objectFit = 'cover';
          thumb.style.borderRadius = '6px';
          chip.appendChild(thumb);
          (async()=>{
            try{
              if (p.items[0].local && p.items[0].dataUrl){ thumb.src = p.items[0].dataUrl; return; }
              const res = await fetch(p.items[0].url);
              const payload = await res.json();
              const b64 = await chatCrypto.decryptWithKey(payload, await this.getFallbackKey());
              thumb.src = URL.createObjectURL(this.base64ToBlob(b64, 'image/png'));
            }catch(_){ }
          })();
        } else {
          chip.textContent = p.name.slice(0, 2).toUpperCase();
        }
        chip.addEventListener('click', async ()=>{
          this._selectedStickerPackId = p.id;
          await this.renderStickerGrid('stickers');
        });
        if (packList) packList.appendChild(chip);
      });

      const pack = idx.packs.find(p=> p.id === selectedPackId);
      if (!pack || !Array.isArray(pack.items) || !pack.items.length){
        grid.innerHTML = '<div style="opacity:.8;padding:8px">Add a pack to start using stickers.</div>';
        return;
      }
      const aesKey = await this.getFallbackKey();
      for (const it of pack.items){
        const cell = document.createElement('div');
        const img = document.createElement('img');
        img.alt = it.name;
        (async()=>{
          try{
            if (it.local && it.dataUrl){ img.src = it.dataUrl; return; }
            const res = await fetch(it.url); const payload = await res.json();
            const b64 = await chatCrypto.decryptWithKey(payload, aesKey);
            img.src = URL.createObjectURL(this.base64ToBlob(b64, 'image/png'));
          }catch(_){ img.src='data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR4nGMAAQAABQABdQh3VwAAAABJRU5ErkJggg=='; }
        })();
        cell.appendChild(img);
        cell.addEventListener('click', ()=> this.sendSticker(it));
        grid.appendChild(cell);
      }
    }

    async manageStickerpacks(){
      const idx = await this.getStickerIndex();
      const pack = idx.packs.find(p=> p.id === this._selectedStickerPackId) || idx.packs[0];
      if (!pack) return;
      const grid = document.getElementById('sticker-grid');
      if (!grid) return;
      grid.innerHTML = '';
      const top = document.createElement('div');
      top.style.cssText = 'display:flex;gap:8px;align-items:center;margin-bottom:8px';
      const addBtn = document.createElement('button');
      addBtn.className = 'icon-btn add';
      addBtn.title = 'Add image';
      addBtn.innerHTML = '<i class="fas fa-plus"></i>';
      const delPackBtn = document.createElement('button');
      delPackBtn.className = 'btn secondary';
      delPackBtn.textContent = 'Delete pack';
      top.appendChild(addBtn);
      top.appendChild(delPackBtn);
      grid.appendChild(top);
      addBtn.onclick = ()=>{
        const picker = document.getElementById('sticker-pack-add-image');
        if (!picker) return;
        picker.onchange = async (e)=>{
          const files = e.target.files;
          if (!files || !files.length) return;
          for (const f of files){
            if (!/^image\//i.test(f.type || '')) continue;
            const safeName = (f.name || 'sticker.png').replace(/[^a-zA-Z0-9._-]/g,'_');
            try{
              const aesKey = await this.getFallbackKey();
              const base64 = await new Promise((resolve,reject)=>{ const r=new FileReader(); r.onload=()=>{ const s=String(r.result||''); resolve(s.includes(',')?s.split(',')[1]:''); }; r.onerror=reject; r.readAsDataURL(f); });
              const cipher = await chatCrypto.encryptWithKey(base64, aesKey);
              const path = `stickers/${this.currentUser.uid}/${Date.now()}_${safeName}.enc.json`;
              const sref = firebase.ref(this.storage, path);
              await firebase.uploadBytes(sref, new Blob([JSON.stringify(cipher)], {type:'application/json'}), { contentType: 'application/json' });
              const url = await firebase.getDownloadURL(sref);
              pack.items.push({ name: safeName, url });
            }catch(_){
              try{
                const localDataUrl = await new Promise((resolve,reject)=>{ const r=new FileReader(); r.onload=()=> resolve(String(r.result||'')); r.onerror=reject; r.readAsDataURL(f); });
                if (localDataUrl) pack.items.push({ name: safeName, dataUrl: localDataUrl, local: true });
              }catch(__){ }
            }
          }
          await this.setStickerIndex(idx);
          await this.manageStickerpacks();
        };
        picker.click();
      };
      delPackBtn.onclick = async ()=>{
        idx.packs = idx.packs.filter(p=> p.id !== pack.id);
        await this.setStickerIndex(idx);
        this._selectedStickerPackId = idx.packs[0] ? idx.packs[0].id : null;
        await this.renderStickerGrid('stickers');
      };
      for (const it of (pack.items || [])){
        const row = document.createElement('div');
        row.style.cssText = 'display:flex;align-items:center;gap:8px;margin-bottom:6px';
        const img = document.createElement('img');
        img.style.cssText = 'width:50px;height:50px;object-fit:cover;border-radius:8px;border:1px solid #2a2f36';
        const del = document.createElement('button');
        del.className = 'icon-btn';
        del.style.background = '#c62828';
        del.innerHTML = '<i class="fas fa-minus"></i>';
        (async()=>{
          try{
            if (it.local && it.dataUrl){ img.src = it.dataUrl; return; }
            const res = await fetch(it.url); const payload = await res.json();
            const b64 = await chatCrypto.decryptWithKey(payload, await this.getFallbackKey());
            img.src = URL.createObjectURL(this.base64ToBlob(b64, 'image/png'));
          }catch(_){ }
        })();
        del.onclick = async ()=>{
          pack.items = (pack.items || []).filter(x=> x.url !== it.url);
          await this.setStickerIndex(idx);
          await this.manageStickerpacks();
        };
        row.appendChild(img);
        row.appendChild(del);
        grid.appendChild(row);
      }
    }

    async sendSticker(item){
      if (!this.activeConnection) return;
      try{
        if (item.local && item.dataUrl){
          await this.saveMessage({ text: `[sticker-data]${item.dataUrl}` });
        } else {
          await this.saveMessage({ text: '[sticker]', fileUrl: item.url, fileName: item.name });
        }
        // update recents
        try{
          const itemKey = item.url || item.dataUrl || '';
          const curr = JSON.parse(localStorage.getItem('liber_sticker_recents')||'[]').filter(x=> (x.url||x.dataUrl||'')!==itemKey);
          curr.unshift(item); localStorage.setItem('liber_sticker_recents', JSON.stringify(curr.slice(0,24)));
        }catch(_){ }
        const pnl = document.getElementById('sticker-panel'); if (pnl) pnl.remove();
        const bd = document.getElementById('sticker-backdrop'); if (bd) bd.remove();
      }catch(_){ alert('Failed to send sticker'); }
    }

    async saveMessage({text,fileUrl,fileName, sharedAsset, media, connId, attachmentSourceConnId, attachmentKeySalt, isVideoRecording, isVoiceRecording}){
      const targetConnId = connId || this.activeConnection;
      if (!targetConnId) return;
      const aesKey = await this.getFallbackKeyForConn(targetConnId);
      const cipher = await chatCrypto.encryptWithKey(text, aesKey);
      const mediaArr = Array.isArray(media) && media.length ? this.toPlainObject(media) : null;
      const firstMedia = mediaArr && mediaArr[0] ? mediaArr[0] : null;
      const legacyFileUrl = fileUrl || (firstMedia && firstMedia.fileUrl) || null;
      const legacyFileName = fileName || (firstMedia && firstMedia.fileName) || null;
      const previewText = this.stripPlaceholderText(text) || (legacyFileName ? `[Attachment] ${legacyFileName}` : (mediaArr ? `[${mediaArr.length} attachments]` : ''));
      const msgRef = firebase.doc(firebase.collection(this.db,'chatMessages',targetConnId,'messages'));
      const doc = {
        id: msgRef.id,
        connId: targetConnId,
        sender: this.currentUser.uid,
        cipher,
        fileUrl: legacyFileUrl,
        fileName: legacyFileName,
        sharedAsset: (sharedAsset && typeof sharedAsset === 'object') ? this.toPlainObject(sharedAsset) : null,
        attachmentSourceConnId: String(attachmentSourceConnId || targetConnId || '').trim() || null,
        attachmentKeySalt: String(attachmentKeySalt || (firstMedia && firstMedia.attachmentKeySalt) || '').trim() || null,
        isVideoRecording: isVideoRecording === true || (firstMedia && firstMedia.isVideoRecording === true),
        isVoiceRecording: isVoiceRecording === true,
        previewText: previewText.slice(0, 220),
        createdAt: new Date().toISOString(),
        createdAtTS: firebase.serverTimestamp()
      };
      if (mediaArr) doc.media = mediaArr;
      const ts = doc.createdAtTS;
      delete doc.createdAtTS;
      const plainDoc = JSON.parse(JSON.stringify(doc));
      plainDoc.createdAtTS = ts;
      await firebase.setDoc(msgRef, plainDoc);
      await firebase.updateDoc(firebase.doc(this.db,'chatConnections',targetConnId),{
        lastMessage: text.slice(0,200),
        lastMessageSender: this.currentUser.uid,
        updatedAt: new Date().toISOString()
      });
      if (fileUrl){
        this.pushRecentAttachment({ fileUrl, fileName: fileName || 'file', sentAt: new Date().toISOString() });
      }
      // Push notify every sent message (receiver-only, never sender).
      Promise.resolve().then(()=> this.sendPushForMessage(text));
      // Live listener updates UI; avoid forcing reload here to prevent race/flicker.
    }

    async sendPushForMessage(text){
      try{
        if (!(window.firebaseService && typeof window.firebaseService.callFunction === 'function')) return;
        const connSnap = await firebase.getDoc(firebase.doc(this.db,'chatConnections',this.activeConnection));
        const data = connSnap.exists() ? (connSnap.data() || {}) : {};
        const participantUids = this.getConnParticipants(data);
        const recipients = participantUids.filter((uid)=> uid && uid !== this.currentUser.uid);
        if (!recipients.length) return;
        const payload = { connId: this.activeConnection, recipients, preview: String(text || '').slice(0,120) };
        // Retry a few times to improve reliability without blocking message send UI.
        let delayMs = 250;
        for (let i = 0; i < 3; i++){
          try{
            await window.firebaseService.callFunction('sendPush', payload);
            return;
          }catch(_){
            if (i === 2) return;
            await new Promise((r)=> setTimeout(r, delayMs));
            delayMs *= 2;
          }
        }
      }catch(_){ }
    }

    /* Group admin management */
    async toggleGroupPanel(){
      const existing = document.getElementById('group-panel');
      if (existing){ existing.remove(); return; }
      const panel = document.createElement('div'); panel.id='group-panel'; panel.className='group-panel';
      panel.innerHTML = `<h4>Group</h4><div id="group-summary"></div>
      <div id="group-meta" style="display:grid;gap:8px;margin:8px 0 12px 0">
        <input id="group-name-input" class="input" type="text" maxlength="80" placeholder="Group name">
        <input id="group-cover-input" class="input" type="url" placeholder="Group cover image URL">
        <button class="btn secondary" id="save-group-meta-btn">Save group settings</button>
      </div>
      <ul id="group-list" class="group-list"></ul><div class="group-actions"><button class="btn secondary" id="add-member-btn">Add member</button><button class="btn secondary" id="close-group-btn">Close</button></div>`;
      document.querySelector('.main').appendChild(panel);
      document.getElementById('close-group-btn').onclick = ()=> panel.remove();
      document.getElementById('save-group-meta-btn').onclick = async ()=>{ await this.saveGroupMeta(); };
      document.getElementById('add-member-btn').onclick = async ()=>{
        const s=document.getElementById('user-search');
        try{
          const snap = await firebase.getDoc(firebase.doc(this.db,'chatConnections', this.activeConnection));
          if (snap.exists()) this.groupBaseParticipants = (snap.data().participants||[]);
        }catch(_){ this.groupBaseParticipants = null; }
        if(s){ this.isGroupMode=true; this.groupSelection=this.groupSelection||new Map(); s.focus(); }
      };
      await this.renderGroupPanel();
    }

    async renderGroupPanel(){
      const list = document.getElementById('group-list'); const summary = document.getElementById('group-summary');
      if (!list || !this.activeConnection) return;
      list.innerHTML=''; if (summary) summary.textContent='';
      try{
        const doc = await firebase.getDoc(firebase.doc(this.db,'chatConnections', this.activeConnection));
        if (!doc.exists()) return;
        const conn = doc.data();
        const participants = Array.isArray(conn.participants)? conn.participants:[];
        const usernames = Array.isArray(conn.participantUsernames)? conn.participantUsernames:[];
        const admins = Array.isArray(conn.admins)? conn.admins : [participants[0]].filter(Boolean);
        if (!Array.isArray(conn.admins)){
          await firebase.updateDoc(firebase.doc(this.db,'chatConnections', this.activeConnection),{ admins });
        }
        const amAdmin = admins.includes(this.currentUser.uid);
        if (summary){ summary.innerHTML = `Members: ${participants.length} · Admins: ${admins.length}${amAdmin? ' · You are admin':''}`; }
        const nameInput = document.getElementById('group-name-input');
        const coverInput = document.getElementById('group-cover-input');
        const saveMetaBtn = document.getElementById('save-group-meta-btn');
        if (nameInput) nameInput.value = String(conn.groupName || '');
        if (coverInput) coverInput.value = String(conn.groupCoverUrl || '');
        if (nameInput) nameInput.disabled = !amAdmin;
        if (coverInput) coverInput.disabled = !amAdmin;
        if (saveMetaBtn) saveMetaBtn.disabled = !amAdmin;
        for (let i=0;i<participants.length;i++){
          const uid = participants[i]; const name = usernames[i] || uid;
          const li = document.createElement('li');
          const left = document.createElement('span'); left.textContent = name + (admins.includes(uid)? ' (admin)':'');
          const right = document.createElement('span');
          if (amAdmin && uid !== this.currentUser.uid){
            const rm = document.createElement('button'); rm.className='btn secondary'; rm.textContent='Remove'; rm.onclick = ()=> this.removeMember(uid);
            right.appendChild(rm);
            const isAdmin = admins.includes(uid);
            const toggle = document.createElement('button'); toggle.className='btn secondary'; toggle.textContent = isAdmin? 'Revoke admin':'Make admin'; toggle.onclick = ()=> this.toggleAdmin(uid, !isAdmin);
            right.appendChild(toggle);
          }
          li.appendChild(left); li.appendChild(right); list.appendChild(li);
        }
      }catch(_){ }
    }

    async saveGroupMeta(){
      try{
        if (!this.activeConnection) return;
        const ref = firebase.doc(this.db,'chatConnections', this.activeConnection);
        const snap = await firebase.getDoc(ref);
        if (!snap.exists()) return;
        const conn = snap.data() || {};
        const participants = Array.isArray(conn.participants) ? conn.participants : [];
        const admins = Array.isArray(conn.admins) ? conn.admins : [participants[0]].filter(Boolean);
        if (!admins.includes(this.currentUser.uid)){
          alert('Only admins can edit group settings.');
          return;
        }
        const nameEl = document.getElementById('group-name-input');
        const coverEl = document.getElementById('group-cover-input');
        const groupName = String(nameEl?.value || '').trim();
        const groupCoverUrl = String(coverEl?.value || '').trim();
        await firebase.updateDoc(ref, {
          groupName,
          groupCoverUrl,
          updatedAt: new Date().toISOString()
        });
        await this.loadConnections();
        await this.setActive(this.activeConnection, groupName || undefined);
        await this.renderGroupPanel();
      }catch(_){
        alert('Failed to save group settings');
      }
    }

    async removeMember(uid){
      try{
        const ref = firebase.doc(this.db,'chatConnections', this.activeConnection);
        const doc = await firebase.getDoc(ref); if (!doc.exists()) return;
        const conn = doc.data();
        const parts = (conn.participants||[]).filter(x=> x!==uid);
        const names = (conn.participantUsernames||[]).filter((_,i)=> (conn.participants||[])[i]!==uid);
        let admins = Array.isArray(conn.admins)? conn.admins: [];
        admins = admins.filter(x=> x!==uid);
        await firebase.updateDoc(ref, { participants: parts, participantUsernames: names, admins, updatedAt: new Date().toISOString() });
        // Recompute key and possibly merge with existing connection with same set
        const key = this.computeConnKey(parts);
        await firebase.updateDoc(ref, { key });
        await this.renderGroupPanel(); await this.loadConnections();
      }catch(_){ alert('Failed to remove member'); }
    }

    async toggleAdmin(uid, make){
      try{
        const ref = firebase.doc(this.db,'chatConnections', this.activeConnection);
        const doc = await firebase.getDoc(ref); if (!doc.exists()) return;
        let admins = Array.isArray(doc.data().admins)? doc.data().admins: [];
        if (make){ if (!admins.includes(uid)) admins.push(uid); }
        else { admins = admins.filter(x=> x!==uid); }
        await firebase.updateDoc(ref, { admins, updatedAt: new Date().toISOString() });
        await this.renderGroupPanel();
      }catch(_){ alert('Failed to update admin'); }
    }

    async notifyParticipants(plaintext){
      try{
        // Local device notification
        if ('Notification' in window){
          const chatName = (document.getElementById('active-connection-name')?.textContent || 'Chat').trim();
          const body = (this.stripPlaceholderText(plaintext) || plaintext || 'New message').slice(0, 120);
          if (Notification.permission === 'granted'){
            new Notification(chatName, { body });
          } else if (Notification.permission !== 'denied'){
            // Request once
            Notification.requestPermission().then(p=>{ if(p==='granted'){ new Notification(chatName, { body }); } });
          }
        }
      }catch(_){}

      // Chat: push only, no email. Server onChatMessageWrite sends push only.
    }

    async getOrCreateSharedAesKey(){
      if (!this.activeConnection) throw new Error('No active connection');
      if (this.sharedKeyCache[this.activeConnection]) return this.sharedKeyCache[this.activeConnection];

      // Load my identity keys (ECDH)
      const myUid = this.currentUser.uid;
      const myId = await chatCrypto.loadOrCreateIdentity(myUid);

      // Store my public key in Firestore under users collection for discovery
      const pubRef = firebase.doc(this.db, 'userPublicKeys', myUid);
      await firebase.setDoc(pubRef, { uid: myUid, publicJwk: myId.publicJwk, updatedAt: new Date().toISOString() }, { merge: true });

      // Determine peer uid
      const peerUid = await this.getPeerUid();

      // Fetch peer public key
      const peerSnap = await firebase.getDoc(firebase.doc(this.db, 'userPublicKeys', peerUid));
      if (!peerSnap.exists()) throw new Error('Peer public key not found, ask peer to open chat');
      const peerJwk = peerSnap.data().publicJwk;

      // Import my private key
      const myPriv = await chatCrypto.getPrivateKey(myUid);

      // Derive shared AES key
      const aesKey = await chatCrypto.deriveSharedAesKey(myPriv, peerJwk);
      this.sharedKeyCache[this.activeConnection] = aesKey;
      return aesKey;
    }

    async getPeerUid(){
      const conn = (this.connections||[]).find(c=> c.id === this.activeConnection);
      if (conn && Array.isArray(conn.participants)){
        return conn.participants.find(u=> u !== this.currentUser.uid);
      }
      try {
        const snap = await firebase.getDoc(firebase.doc(this.db,'chatConnections',this.activeConnection));
        if (snap.exists()){
          const data = snap.data();
          if (Array.isArray(data.participants)){
            return data.participants.find(u=> u !== this.currentUser.uid);
          }
        }
      } catch {}
      const parts = (this.activeConnection||'').split('_');
      if (parts.length === 2){
        return parts[0] === this.currentUser.uid ? parts[1] : parts[0];
      }
      return '';
    }

    async getPeerUidForConn(connId){
      try{
        const cached = this._peerUidByConn && this._peerUidByConn.get(connId);
        if (cached) return cached;
      }catch(_){ }
      try{
        const local = (this.connections || []).find((c)=> c && c.id === connId);
        const localParts = this.getConnParticipants(local || {});
        if (localParts.length === 2){
          const peer = localParts.find((u)=> u !== this.currentUser.uid);
          if (peer){
            this._peerUidByConn.set(connId, peer);
            return peer;
          }
        }
      }catch(_){ }
      try{
        const snap = await firebase.getDoc(firebase.doc(this.db,'chatConnections',connId));
        if (snap.exists()){
          const parts = this.getConnParticipants(snap.data() || {});
          if (parts.length === 2){
            const peer = parts.find((u)=> u !== this.currentUser.uid);
            if (peer){
              this._peerUidByConn.set(connId, peer);
              return peer;
            }
          }
        }
      }catch(_){ }
      try{
        const raw = String(connId || '').trim();
        if (raw.includes('|')){
          const parts = raw.split('|').filter(Boolean);
          if (parts.length === 2){
            const peer = parts.find((u)=> u !== this.currentUser.uid);
            if (peer){
              this._peerUidByConn.set(connId, peer);
              return peer;
            }
          }
        }
        if (raw.includes('_')){
          const parts = raw.split('_').filter(Boolean);
          if (parts.length === 2){
            const peer = parts.find((u)=> u !== this.currentUser.uid);
            if (peer){
              this._peerUidByConn.set(connId, peer);
              return peer;
            }
          }
        }
      }catch(_){ }
      return '';
    }

    async getConnSaltForConn(connId){
      try{
        const local = (this.connections || []).find((c)=> c && c.id === connId);
        const localParts = this.getConnParticipants(local || {});
        const localKey = String(local?.key || '').trim();
        if (localKey || localParts.length){
          const stable = localKey || this.computeConnKey(localParts);
          return { parts: localParts, stableSalt: stable || connId, connIdSalt: connId };
        }
      }catch(_){ }
      try{
        const snap = await firebase.getDoc(firebase.doc(this.db,'chatConnections', connId));
        if (snap.exists()){
          const data = snap.data() || {};
          const parts = this.getConnParticipants(data || {});
          const key = String(data?.key || '').trim();
          const stable = key || this.computeConnKey(parts);
          return { parts, stableSalt: stable || connId, connIdSalt: connId };
        }
      }catch(_){ }
      return { parts: [], stableSalt: connId, connIdSalt: connId };
    }

    async getFallbackKeyCandidatesForConn(connId){
      // Recompute per call to avoid stale wrong-key cache poisoning across chat switches.
      const out = [];
      const add = (k)=>{ if (k && !out.includes(k)) out.push(k); };
      const tryAdd = async (factory)=>{
        try{
          const k = await factory();
          add(k);
        }catch(_){ }
      };
      const salts = await this.getConnSaltForConn(connId);
      const parts = Array.isArray(salts.parts) ? salts.parts : [];
      const stableSalt = String(salts.stableSalt || connId || '');
      const connIdSalt = String(salts.connIdSalt || connId || '');
      // Always include conn-stable fallback key for cross-device/cross-session compatibility.
      await tryAdd(()=> window.chatCrypto.deriveChatKey(`${stableSalt}|liber_secure_chat_conn_stable_v1`));
      if (connIdSalt && connIdSalt !== stableSalt){
        await tryAdd(()=> window.chatCrypto.deriveChatKey(`${connIdSalt}|liber_secure_chat_conn_stable_v1`));
      }
      if (parts.length > 2){
        await tryAdd(()=> window.chatCrypto.deriveChatKey(`${parts.slice().sort().join('|')}|${stableSalt}|liber_group_fallback_v2`));
        if (connIdSalt && connIdSalt !== stableSalt){
          await tryAdd(()=> window.chatCrypto.deriveChatKey(`${parts.slice().sort().join('|')}|${connIdSalt}|liber_group_fallback_v2`));
        }
        return out;
      }
      const peerUid = await this.getPeerUidForConn(connId);
      if (!peerUid){
        try{ if (out.length) this._fallbackKeyCandidatesCache.set(connId, out.slice()); }catch(_){ }
        return out;
      }
      if (window.chatCrypto && typeof window.chatCrypto.deriveFallbackSharedAesKey === 'function'){
        await tryAdd(()=> window.chatCrypto.deriveFallbackSharedAesKey(this.currentUser.uid, peerUid, stableSalt));
        if (connIdSalt && connIdSalt !== stableSalt){
          await tryAdd(()=> window.chatCrypto.deriveFallbackSharedAesKey(this.currentUser.uid, peerUid, connIdSalt));
        }
      }
      // Legacy fallback variants
      await tryAdd(()=> window.chatCrypto.deriveChatKey(`${[this.currentUser.uid, peerUid || ''].sort().join('|')}|${stableSalt}|liber_secure_chat_fallback_v1`));
      if (connIdSalt && connIdSalt !== stableSalt){
        await tryAdd(()=> window.chatCrypto.deriveChatKey(`${[this.currentUser.uid, peerUid || ''].sort().join('|')}|${connIdSalt}|liber_secure_chat_fallback_v1`));
      }
      try{
        if (out.length) this._fallbackKeyCandidatesCache.set(connId, out.slice());
      }catch(_){ }
      return out;
    }

    async getFallbackKeyForConn(connId){
      try{
        const keys = await this.getFallbackKeyCandidatesForConn(connId);
        if (keys && keys.length) return keys[0];
      }catch(_){ }
      const peerUid = await this.getPeerUidForConn(connId);
      return window.chatCrypto.deriveChatKey(`${[this.currentUser.uid, peerUid || ''].sort().join('|')}|${connId}|liber_secure_chat_fallback_v1`);
    }

    async getFallbackKey(){
      return this.getFallbackKeyForConn(this.activeConnection);
    }

    async getAllMyConnectionIdsForDecrypt(limit = 1200){
      const out = new Set();
      try{
        (this.connections || []).forEach((c)=>{
          const id = String(c?.id || '').trim();
          if (id) out.add(id);
        });
      }catch(_){ }
      try{
        const uid = String(this.currentUser?.uid || '').trim();
        if (!uid) return Array.from(out);
        const pull = async (q)=>{
          const s = await firebase.getDocs(q);
          s.forEach((d)=>{ const id = String(d.id || '').trim(); if (id) out.add(id); });
        };
        await pull(firebase.query(firebase.collection(this.db,'chatConnections'), firebase.where('participants','array-contains', uid), firebase.limit(limit)));
        try{ await pull(firebase.query(firebase.collection(this.db,'chatConnections'), firebase.where('users','array-contains', uid), firebase.limit(limit))); }catch(_){ }
        try{ await pull(firebase.query(firebase.collection(this.db,'chatConnections'), firebase.where('memberIds','array-contains', uid), firebase.limit(limit))); }catch(_){ }
      }catch(_){ }
      return Array.from(out);
    }

    isImageFilename(name){
      const n = (name||'').toLowerCase();
      return n.endsWith('.png') || n.endsWith('.jpg') || n.endsWith('.jpeg') || n.endsWith('.gif') || n.endsWith('.webp');
    }
    isVideoFilename(name){
      const n = (name||'').toLowerCase();
      if (n.startsWith('voice.') || n.startsWith('audio.')) return false;
      return n.endsWith('.mp4') || n.endsWith('.webm') || n.endsWith('.mov') || n.endsWith('.mkv');
    }
    isAudioFilename(name){
      const n = (name||'').toLowerCase();
      return n.startsWith('voice.') || n.startsWith('audio.') || n.endsWith('.mp3') || n.endsWith('.wav') || n.endsWith('.m4a') || n.endsWith('.aac') || n.endsWith('.ogg') || n.endsWith('.oga') || n.endsWith('.weba');
    }

    inferMediaKindFromUrl(url){
      const href = String(url || '');
      let pathOnly = href;
      try{ pathOnly = new URL(href).pathname; }catch(_){ pathOnly = href.split('?')[0].split('#')[0]; }
      const lower = pathOnly.toLowerCase();
      if (['.png','.jpg','.jpeg','.gif','.webp','.avif'].some((ext)=> lower.endsWith(ext))) return 'image';
      if (['.mp4','.webm','.mov','.mkv'].some((ext)=> lower.endsWith(ext))) return 'video';
      if (['.mp3','.wav','.m4a','.aac','.ogg','.oga','.weba'].some((ext)=> lower.endsWith(ext))) return 'audio';
      return 'file';
    }

    inferVideoMime(name){
      const n = String(name || '').toLowerCase();
      if (n.endsWith('.mp4') || n.endsWith('.m4v')) return 'video/mp4';
      if (n.endsWith('.mov')) return 'video/quicktime';
      if (n.endsWith('.mkv')) return 'video/x-matroska';
      return 'video/webm';
    }

    inferAudioMime(name){
      const n = String(name || '').toLowerCase();
      if (n.endsWith('.mp3')) return 'audio/mpeg';
      if (n.endsWith('.wav')) return 'audio/wav';
      if (n.endsWith('.m4a') || n.endsWith('.aac')) return 'audio/mp4';
      if (n.endsWith('.ogg') || n.endsWith('.oga')) return 'audio/ogg';
      return 'audio/webm';
    }

    pauseOtherInlineMedia(current){
      try{
        document.querySelectorAll('.messages audio, .messages video, .file-preview video, .file-preview audio').forEach((m)=>{
          if (m !== current){
            try{ m.pause(); }catch(_){ }
          }
        });
      }catch(_){ }
    }

    bindInlineVideoPlayback(videoEl, title){
      if (!videoEl || videoEl._inlineToggleBound) return;
      videoEl._inlineToggleBound = true;
      videoEl.style.cursor = 'pointer';
      videoEl.addEventListener('click', (e)=>{
        e.preventDefault();
        e.stopPropagation();
        if (videoEl.paused){
          try{
            const p = this.ensureChatBgPlayer();
            if (p && !p.paused) p.pause();
          }catch(_){ }
          try{
            const hostBg = this.getGlobalBgPlayer();
            if (hostBg && !hostBg.paused) hostBg.pause();
          }catch(_){ }
          this.pauseOtherInlineMedia(videoEl);
          videoEl.play().catch(()=>{});
          this.bindTopStripToMedia(videoEl, title || 'Video message');
        } else {
          videoEl.pause();
        }
        this.updateVoiceWidgets();
      });
      videoEl.addEventListener('play', ()=> this.bindTopStripToMedia(videoEl, title || 'Video message'));
      videoEl.addEventListener('pause', ()=> this.updateVoiceWidgets());
    }

    getGlobalBgPlayer(){
      try{
        const topDoc = window.top && window.top.document ? window.top.document : document;
        return topDoc.getElementById('bg-player') || null;
      }catch(_){ return document.getElementById('bg-player') || null; }
    }

    stopRegularPlayer(){
      try{
        const hostBg = this.getGlobalBgPlayer();
        if (hostBg && !hostBg.paused) hostBg.pause();
      }catch(_){ }
    }

    detectMimeFromBase64(b64, fallback = 'application/octet-stream'){
      try{
        const head = atob(String(b64 || '').slice(0, 96));
        const bytes = new Uint8Array(Math.min(16, head.length));
        for (let i = 0; i < bytes.length; i++) bytes[i] = head.charCodeAt(i);
        // WEBM / Matroska (EBML)
        if (bytes.length >= 4 && bytes[0] === 0x1a && bytes[1] === 0x45 && bytes[2] === 0xdf && bytes[3] === 0xa3){
          if (String(fallback).startsWith('audio/')) return 'audio/webm';
          return 'video/webm';
        }
        // MP4 family ('ftyp' at offset 4)
        if (bytes.length >= 8 && bytes[4] === 0x66 && bytes[5] === 0x74 && bytes[6] === 0x79 && bytes[7] === 0x70){
          if (String(fallback).startsWith('audio/')) return 'audio/mp4';
          return 'video/mp4';
        }
      }catch(_){ }
      return fallback;
    }

    ensureChatBgPlayer(){
      if (this._chatBgPlayer) return this._chatBgPlayer;
      let p = document.getElementById('chat-bg-player');
      if (!p){
        p = document.createElement('audio');
        p.id = 'chat-bg-player';
        // iOS Safari/PWA: keep element rendered (not display:none) for reliable time/progress events.
        p.style.cssText = 'position:fixed;left:-9999px;bottom:0;width:1px;height:1px;opacity:0;pointer-events:none';
        p.preload = 'auto';
        p.playsInline = true;
        p.setAttribute('playsinline', 'true');
        p.setAttribute('webkit-playsinline', 'true');
        document.body.appendChild(p);
      }
      this._chatBgPlayer = p;
      if (!p._voiceBound){
        p._voiceBound = true;
        const sync = ()=> this.updateVoiceWidgets();
        const startTick = ()=>{
          try{
            if (p._voiceTick) return;
            p._voiceTick = setInterval(()=> {
              try{ if (!p.paused) this.updateVoiceWidgets(); }catch(_){ }
            }, 220);
          }catch(_){ }
        };
        const stopTick = ()=>{
          try{
            if (p._voiceTick){
              clearInterval(p._voiceTick);
              p._voiceTick = null;
            }
          }catch(_){ }
        };
        p.addEventListener('timeupdate', sync);
        p.addEventListener('play', ()=>{ startTick(); sync(); });
        p.addEventListener('pause', ()=>{ stopTick(); sync(); });
        p.addEventListener('loadedmetadata', sync);
        p.addEventListener('durationchange', sync);
        p.addEventListener('seeking', sync);
        p.addEventListener('seeked', sync);
        p.addEventListener('ended', ()=>{ this._voiceUserIntendedPlay = false; stopTick(); sync(); });
      }
      // Resume after external interruption (phone call, tab switch, other app).
      if (!p._voiceVisibilityBound){
        p._voiceVisibilityBound = true;
        document.addEventListener('visibilitychange', ()=>{
          if (document.visibilityState !== 'visible') return;
          const player = this.ensureChatBgPlayer();
          const topMedia = (this._topMediaEl && this._topMediaEl.isConnected) ? this._topMediaEl : null;
          if (this._voiceUserIntendedPlay && topMedia && topMedia.paused){
            topMedia.play().catch(()=>{});
            this.updateVoiceWidgets();
            return;
          }
          if (this._voiceUserIntendedPlay && !!this.getChatPlayerSrc(player) && player.paused){
            player.play().catch(()=>{});
            this.updateVoiceWidgets();
          }
        });
      }
      // Keep a single-track experience across host and app shell.
      if (!p._singleTrackBound){
        p._singleTrackBound = true;
        p.addEventListener('play', ()=>{
          try{
            const hostBg = this.getGlobalBgPlayer();
            if (hostBg && hostBg !== p && !hostBg.paused) hostBg.pause();
          }catch(_){ }
        });
      }
      if (!p._playlistBound){
        p._playlistBound = true;
        p.addEventListener('ended', ()=>{
          try{
            const next = (this._chatAudioPlaylist || []).shift();
            if (!next || !next.src) return;
            this._voiceCurrentSrc = next.src;
            this._voiceCurrentAttachmentKey = String(next.sourceKey || '').trim();
            this._voiceCurrentTitle = next.title || 'Audio';
            p.src = next.src;
            this._voiceUserIntendedPlay = true;
            p.play().catch(()=>{});
          }catch(_){ }
        });
      }
      return p;
    }

    startVoiceWidgetTicker(){
      if (this._voiceWidgetTicker) return;
      this._voiceWidgetTicker = setInterval(()=>{
        const p = this.ensureChatBgPlayer();
        const topMedia = (this._topMediaEl && this._topMediaEl.isConnected) ? this._topMediaEl : null;
        const hasActivePlayback = (!!this.getChatPlayerSrc(p) && !p.paused) || (!!topMedia && !topMedia.paused);
        if (this._voiceWidgets.size === 0 && !hasActivePlayback) return;
        this.updateVoiceWidgets();
      }, 80);
    }

    stopVoiceWidgetTicker(){
      if (this._voiceWidgetTicker){ clearInterval(this._voiceWidgetTicker); this._voiceWidgetTicker = null; }
    }

    startVoiceProgressLoop(){
      if (this._voiceProgressRaf) return;
      const tick = ()=>{
        this.updateVoiceWidgets();
        if (this._voiceProgressRaf) this._voiceProgressRaf = window.requestAnimationFrame(tick);
      };
      this._voiceProgressRaf = window.requestAnimationFrame(tick);
    }

    stopVoiceProgressLoop(){
      if (!this._voiceProgressRaf) return;
      try{ window.cancelAnimationFrame(this._voiceProgressRaf); }catch(_){ }
      this._voiceProgressRaf = 0;
    }

    updateVoiceWidgets(){
      if (this._voiceWidgets.size === 0){ this.stopVoiceWidgetTicker(); }
      const p = this.ensureChatBgPlayer();
      const stale = [];
      this._voiceWidgets.forEach((w, k)=>{ if (!w.wave || !w.wave.isConnected) stale.push(k); });
      stale.forEach((k)=> this._voiceWidgets.delete(k));
      const strip = document.getElementById('voice-top-strip');
      const stripTitle = document.getElementById('voice-top-title');
      const stripToggle = document.getElementById('voice-top-toggle');
      const topMedia = (this._topMediaEl && this._topMediaEl.isConnected) ? this._topMediaEl : null;
      const playerSrc = this.getChatPlayerSrc(p);
      const canShowStrip = (Date.now() > Number(this._forceHideVoiceStripUntil || 0)) && (!!topMedia || !!playerSrc);
      if (strip && stripToggle){
        if (canShowStrip){
          strip.classList.remove('hidden');
          const paused = topMedia ? !!topMedia.paused : !!p.paused;
          stripToggle.innerHTML = `<i class="fas ${paused ? 'fa-play' : 'fa-pause'}"></i>`;
        } else {
          strip.classList.add('hidden');
          stripToggle.innerHTML = '<i class="fas fa-play"></i>';
        }
      }
      if (topMedia && stripTitle){
        stripTitle.textContent = this._voiceCurrentTitle || 'Media';
      }
      this._voiceWidgets.forEach((w)=>{
        const active = (!!playerSrc && (w.src === playerSrc || this.isSameMediaSrc(w.src, playerSrc)))
          || (!!this._voiceCurrentSrc && (w.src === this._voiceCurrentSrc || this.isSameMediaSrc(w.src, this._voiceCurrentSrc)))
          || (!!this._voiceCurrentAttachmentKey && !!w.srcKey && w.srcKey === this._voiceCurrentAttachmentKey);
        const durationRaw = Number(p.duration || 0);
        const durationIsFinite = Number.isFinite(durationRaw) && durationRaw > 0;
        const duration = durationIsFinite ? durationRaw : Math.max(0, Number(w.durationGuess || 0));
        const ctRaw = Number(p.currentTime || 0);
        const ct = Number.isFinite(ctRaw) && ctRaw > 0 ? ctRaw : 0;
        if (active && durationIsFinite) w.durationGuess = durationRaw;
        const ratio = active && duration > 0 ? Math.min(1, Math.max(0, ct / duration)) : 0;
        const bars = w.wave.querySelectorAll('.bar');
        const playedBars = Math.round(bars.length * ratio);
        bars.forEach((b, i)=> b.classList.toggle('played', active && i < playedBars));
        w.playBtn.innerHTML = `<i class="fas ${active && !p.paused ? 'fa-pause' : 'fa-play'}"></i>`;
        const currentTxt = this.formatDuration(active ? ct : 0);
        const totalTxt = this.formatDuration(active ? duration : (w.durationGuess || 0));
        w.time.textContent = w.showRemaining
          ? `-${this.formatDuration(Math.max(0, (active ? duration - ct : w.durationGuess || 0)))}`
          : `${currentTxt} / ${totalTxt}`;
        if (active && stripTitle) stripTitle.textContent = w.title || 'Voice message';
      });
      if (stripTitle){
        if (!playerSrc) stripTitle.textContent = 'Voice message';
        else if (!this._voiceWidgets.size) stripTitle.textContent = this._voiceCurrentTitle || 'Voice message';
      }
      const shouldAnimate = (!!playerSrc && !p.paused) || (!!topMedia && !topMedia.paused);
      if (shouldAnimate) this.startVoiceProgressLoop();
      else this.stopVoiceProgressLoop();
    }

    renderWaveAttachment(containerEl, url, fileName, sourceKey = ''){
      const wrapper = document.createElement('div');
      wrapper.className = 'voice-wave-player';
      const playBtn = document.createElement('button');
      playBtn.className = 'play';
      playBtn.innerHTML = '<i class="fas fa-play"></i>';
      const wave = document.createElement('div');
      wave.className = 'wave';
      const time = document.createElement('div');
      time.className = 'time';
      time.textContent = '0:00 / 0:00';
      const keySeed = String(fileName || url || 'voice');
      const barsCount = 54;
      this.paintSeedWaveBars(wave, barsCount, keySeed);
      wrapper.appendChild(playBtn);
      wrapper.appendChild(wave);
      wrapper.appendChild(time);
      containerEl.appendChild(wrapper);
      const widget = {
        id: `vw_${Date.now()}_${Math.random().toString(36).slice(2,7)}`,
        src: url,
        srcKey: String(sourceKey || '').trim(),
        connId: this.activeConnection || '',
        title: fileName || 'Voice message',
        playBtn,
        wave,
        time,
        showRemaining: false,
        durationGuess: 0
      };
      this._voiceWidgets.set(widget.id, widget);
      this.hydrateVoiceWidgetMedia(widget, barsCount, keySeed);
      this.startVoiceWidgetTicker();

      const p = this.ensureChatBgPlayer();
      const startFromRatio = (ratio)=>{
        const clamped = Math.min(1, Math.max(0, ratio));
        if (!this.isSameMediaSrc(this.getChatPlayerSrc(p), url)){
          this._topMediaEl = null;
          this._voiceCurrentSrc = url;
          this._voiceCurrentAttachmentKey = widget.srcKey || '';
          this._voiceCurrentTitle = widget.title;
          this.setupMediaSessionForVoice(widget.title);
          this.pauseOtherInlineMedia(null);
          p.src = url;
          try{ p.load(); }catch(_){ }
          p.currentTime = 0;
          this._voiceUserIntendedPlay = true;
          p.play().catch(()=>{});
          this.notifyParentAudioMetaOnly({ src: url, title: widget.title, by: '', cover: '' });
          const onMeta = ()=>{
            if (Number.isFinite(p.duration) && p.duration > 0){
              p.currentTime = clamped * p.duration;
              widget.durationGuess = p.duration;
              this.updateVoiceWidgets();
            }
            p.removeEventListener('loadedmetadata', onMeta);
          };
          p.addEventListener('loadedmetadata', onMeta);
          return;
        }
        if (Number.isFinite(p.duration) && p.duration > 0){
          p.currentTime = clamped * p.duration;
          widget.durationGuess = p.duration;
        }
        if (p.paused){ this._voiceUserIntendedPlay = true; p.play().catch(()=>{}); }
        this.updateVoiceWidgets();
      };

      playBtn.addEventListener('click', (e)=>{
        try{ e.stopPropagation(); }catch(_){ }
        this.enqueueVoiceWaveHydrate(widget, barsCount, keySeed, { priority: true });
        const isThisPlaying = !!this.getChatPlayerSrc(p) && (this.isSameMediaSrc(this.getChatPlayerSrc(p), url) || (!!widget.srcKey && this._voiceCurrentAttachmentKey === widget.srcKey)) && !p.paused;
        if (!this.isSameMediaSrc(this.getChatPlayerSrc(p), url) && !(!!widget.srcKey && this._voiceCurrentAttachmentKey === widget.srcKey)){
          this._topMediaEl = null;
          this._voiceCurrentSrc = url;
          this._voiceCurrentAttachmentKey = widget.srcKey || '';
          this._voiceCurrentTitle = widget.title;
          this.setupMediaSessionForVoice(widget.title);
          this.pauseOtherInlineMedia(null);
          p.src = url;
          try{ p.load(); }catch(_){ }
          this._voiceUserIntendedPlay = true;
          p.play().catch(()=>{});
          this.notifyParentAudioMetaOnly({ src: url, title: widget.title, by: '', cover: '' });
          this.updateVoiceWidgets();
          return;
        }
        if (Number.isFinite(p.duration) && p.duration > 0){
          widget.durationGuess = p.duration;
        }
        if (p.paused) {
          this._voiceUserIntendedPlay = true;
          p.play().catch(()=>{});
          this.notifyParentAudioMetaOnly({ src: url, title: widget.title, by: '', cover: '' });
        } else {
          this._voiceUserIntendedPlay = false;
          p.pause();
        }
        this.updateVoiceWidgets();
      });

      let dragging = false;
      const seekFromClientX = (clientX)=>{
        this.enqueueVoiceWaveHydrate(widget, barsCount, keySeed, { priority: true });
        const rect = wave.getBoundingClientRect();
        const ratio = (clientX - rect.left) / rect.width;
        startFromRatio(ratio);
      };
      wave.addEventListener('click', (e)=>{ try{ e.stopPropagation(); }catch(_){ } seekFromClientX(e.clientX); });
      wave.addEventListener('pointerdown', (e)=>{ try{ e.stopPropagation(); }catch(_){ } dragging = true; wave.setPointerCapture(e.pointerId); seekFromClientX(e.clientX); });
      wave.addEventListener('pointermove', (e)=>{ if (dragging) seekFromClientX(e.clientX); });
      wave.addEventListener('pointerup', (e)=>{ dragging = false; try{ e.stopPropagation(); }catch(_){ } try{ wave.releasePointerCapture(e.pointerId); }catch(_){ } });
      time.addEventListener('click', (e)=>{
        try{ e.stopPropagation(); }catch(_){ }
        widget.showRemaining = !widget.showRemaining;
        this.updateVoiceWidgets();
      });
      this.updateVoiceWidgets();
    }

    handoffToPersistentVoicePlayer(mediaEl, meta = {}){
      try{
        const global = this.getGlobalBgPlayer();
        if (global && !global.paused){ try{ global.pause(); }catch(_){ } }
        const p = this.ensureChatBgPlayer();
        const src = mediaEl.currentSrc || mediaEl.src || '';
        if (!src) return;
        this._voiceCurrentSrc = src;
        this._voiceCurrentAttachmentKey = String(meta.sourceKey || '').trim();
        this._voiceCurrentTitle = meta.title || 'Voice message';
        if (p.src !== src) p.src = src;
        if (!Number.isNaN(mediaEl.currentTime)) p.currentTime = mediaEl.currentTime;
        this._voiceUserIntendedPlay = true;
        p.play().catch(()=>{});
        mediaEl.pause();
        this.setupMediaSessionForVoice(this._voiceCurrentTitle);
        this.updateVoiceWidgets();
      }catch(_){ }
    }

    bindTopStripToMedia(mediaEl, title = 'Media'){
      try{
        this._topMediaEl = mediaEl;
        if (mediaEl && !mediaEl.paused) this._voiceUserIntendedPlay = true;
        this._voiceCurrentTitle = title || 'Media';
        const sync = ()=> this.updateVoiceWidgets();
        if (!mediaEl._topStripBound){
          mediaEl._topStripBound = true;
          mediaEl.addEventListener('play', sync);
          mediaEl.addEventListener('pause', sync);
          mediaEl.addEventListener('timeupdate', sync);
          mediaEl.addEventListener('ended', ()=>{ this._voiceUserIntendedPlay = false; this._topMediaEl = null; this.updateVoiceWidgets(); });
        }
        this.updateVoiceWidgets();
      }catch(_){ }
    }

    applyRandomTriangleMask(mediaEl){
      try{
        const applyMask = ()=>{
          try{
            const rect = mediaEl.getBoundingClientRect();
            const w = Number(rect.width || mediaEl.clientWidth || mediaEl.videoWidth || mediaEl.naturalWidth || 0);
            const h = Number(rect.height || mediaEl.clientHeight || mediaEl.videoHeight || mediaEl.naturalHeight || 0);
            if (!(w > 0 && h > 0)) return;
            const cx = w / 2;
            const cy = h / 2;
            const r = Math.min(w, h) / 2;
            const tau = Math.PI * 2;
            // Vertices stay on the centered outer circle; random rotation keeps it random.
            // This guarantees the mask crosses center and contains an inner centered circle of radius R/2.
            const start = Math.random() * tau;
            const dir = Math.random() < 0.5 ? 1 : -1;
            const angles = [start, start + dir * (tau / 3), start + dir * ((tau * 2) / 3)];
            const points = angles.map((a)=>{
              const x = cx + (Math.cos(a) * r);
              const y = cy + (Math.sin(a) * r);
              return `${((x / w) * 100).toFixed(2)}% ${((y / h) * 100).toFixed(2)}%`;
            });
            mediaEl.style.clipPath = `polygon(${points.join(',')})`;
            mediaEl.style.webkitClipPath = `polygon(${points.join(',')})`;
          }catch(_){ }
        };
        applyMask();
        if (!mediaEl._triangleMaskBound){
          mediaEl._triangleMaskBound = true;
          mediaEl.addEventListener('loadedmetadata', applyMask);
          mediaEl.addEventListener('load', applyMask);
          if (typeof ResizeObserver !== 'undefined'){
            try{
              const ro = new ResizeObserver(()=> applyMask());
              ro.observe(mediaEl);
              mediaEl._triangleMaskRO = ro;
            }catch(_){ }
          }
        }
      }catch(_){ }
    }

    isVideoRecordingMessage(message, fileName = ''){
      try{
        if (message && message.isVideoRecording === true) return true;
        const n = String(fileName || '').toLowerCase().trim();
        if (/^video\.webm$/i.test(n)) return true;
        const text = String(message?.text || '').trim();
        if (/^\[video message\]/i.test(text)) return true;
        const preview = String(message?.previewText || '').trim();
        if (/^\[video message\]/i.test(preview)) return true;
        return false;
      }catch(_){ return false; }
    }

    isVoiceRecordingMessage(message, fileName = ''){
      try{
        if (!this.isAudioFilename(fileName)) return false;
        if (message && message.isVoiceRecording === true) return true;
        const n = String(fileName || '').toLowerCase().trim();
        if (/^voice\.webm$/i.test(n)) return true;
        const text = String(message?.text || '').trim();
        if (/^\[voice message\]/i.test(text)) return true;
        const preview = String(message?.previewText || '').trim();
        if (/^\[voice message\]/i.test(preview)) return true;
        return false;
      }catch(_){ return false; }
    }

    /** Returns 'voice' | 'uploaded' | 'waveconnect' for audio messages. Used to separate visuals from playback. */
    getChatAudioType(message, fileName = '', hasSharedAsset = false){
      try{
        if (hasSharedAsset && message?.sharedAsset && typeof message.sharedAsset === 'object'){
          const k = String(message.sharedAsset.kind || message.sharedAsset.type || '').toLowerCase();
          if (k === 'audio') return 'waveconnect';
        }
        if (!this.isAudioFilename(fileName)) return null;
        if (this.isVoiceRecordingMessage(message, fileName)) return 'voice';
        return 'uploaded';
      }catch(_){ return null; }
    }

    /** Central playback: all 3 audio types use the same bg player. */
    playChatAudioInBgPlayer(opts = {}){
      try{
        const src = String(opts.src || opts.url || '').trim();
        if (!src) return;
        const p = this.ensureChatBgPlayer();
        const seekTo = Number.isFinite(opts.currentTime) && opts.currentTime >= 0 ? opts.currentTime : 0;
        this._topMediaEl = null;
        this._voiceCurrentSrc = src;
        this._voiceCurrentAttachmentKey = String(opts.sourceKey || '').trim();
        this._voiceCurrentTitle = String(opts.title || 'Audio').trim();
        this.setupMediaSessionForVoice(this._voiceCurrentTitle);
        this.pauseOtherInlineMedia(null);
        p.src = src;
        try{ p.load(); }catch(_){ }
        if (seekTo > 0){
          const onMeta = ()=>{ try{ if (Number.isFinite(p.duration) && p.duration > 0) p.currentTime = Math.min(seekTo, p.duration); }catch(_){ } p.removeEventListener('loadedmetadata', onMeta); };
          p.addEventListener('loadedmetadata', onMeta);
        }
        this._voiceUserIntendedPlay = true;
        p.play().catch((err)=>{ if (err && !String(err?.message||'').includes('aborter')) console.warn('Chat audio play failed:', err); });
        this.notifyParentAudioMetaOnly({ src, title: opts.title || 'Audio', by: opts.by || '', cover: opts.cover || '' });
        this.updateVoiceWidgets();
      }catch(_){ }
    }

    addToChatAudioPlaylist(item){
      try{
        if (!item || !item.src) return;
        this._chatAudioPlaylist.push(item);
        const payload = {
          src: item.src,
          title: item.title || 'Track',
          by: item.author || ''
        };
        let opened = false;
        if (!opened){
          try{
            this.openChatAddToPlaylistPopup(payload);
            opened = true;
          }catch(_){ }
        }
        if (!opened){
          try{
            const candidates = [window.top, window.parent, window];
            for (const host of candidates){
              try{
                const mgr = host?.dashboardManager;
                if (mgr && typeof mgr.openAddToPlaylistPopup === 'function'){
                  mgr.openAddToPlaylistPopup.call(mgr, payload);
                  opened = true;
                  break;
                }
              }catch(_){ }
            }
          }catch(_){ }
        }
        if (!opened){
          // Fallback bridge in case direct host object access is blocked by embedding.
          try{ window.top?.postMessage({ type: 'LIBER_ADD_TO_PLAYLIST', track: payload }, '*'); }catch(_){ }
          try{ window.parent?.postMessage({ type: 'LIBER_ADD_TO_PLAYLIST', track: payload }, '*'); }catch(_){ }
          try{
            this.openChatAddToPlaylistPopup(payload);
            opened = true;
          }catch(_){ }
        }
      }catch(_){ }
    }

    openChatAddToPlaylistPopup(track){
      const uid = this.currentUser?.uid || window.firebaseService?.auth?.currentUser?.uid || 'anon';
      const key = `liber_playlists_${uid}`;
      let playlists = [];
      try{
        const raw = localStorage.getItem(key);
        const parsed = raw ? JSON.parse(raw) : [];
        playlists = Array.isArray(parsed) ? parsed : [];
      }catch(_){ playlists = []; }
      const overlay = document.createElement('div');
      overlay.style.cssText = 'position:fixed;inset:0;z-index:1400;background:rgba(0,0,0,.58);display:flex;align-items:center;justify-content:center;padding:16px';
      const options = playlists.map((p)=> `<option value="${String(p.id || '').replace(/"/g,'&quot;')}">${String(p.name || 'Playlist').replace(/</g,'&lt;')}</option>`).join('');
      overlay.innerHTML = `
        <div style="width:min(96vw,420px);background:#0f1724;border:1px solid #2b3445;border-radius:12px;padding:12px">
          <div style="font-weight:700;margin-bottom:10px">Add to playlist</div>
          <div style="margin-bottom:8px">
            <label style="font-size:12px;opacity:.9;display:block;margin-bottom:4px">Existing playlist</label>
            <select id="pl-select" style="width:100%;padding:8px;border-radius:8px;background:#121a28;color:#e8eefb;border:1px solid #2b3445">
              <option value="">Choose playlist...</option>
              ${options}
            </select>
          </div>
          <div style="margin-bottom:10px">
            <label style="font-size:12px;opacity:.9;display:block;margin-bottom:4px">Or create new</label>
            <input id="pl-new" type="text" placeholder="New playlist name" style="width:100%;padding:8px;border-radius:8px;background:#121a28;color:#e8eefb;border:1px solid #2b3445" />
          </div>
          <div style="display:flex;justify-content:flex-end;gap:8px">
            <button id="pl-cancel" class="btn secondary" type="button">Cancel</button>
            <button id="pl-save" class="btn" type="button">Save</button>
          </div>
        </div>`;
      document.body.appendChild(overlay);
      const remove = ()=>{ try{ overlay.remove(); }catch(_){ } };
      overlay.querySelector('#pl-cancel').onclick = remove;
      overlay.addEventListener('click', (e)=>{ if (e.target === overlay) remove(); });
      overlay.querySelector('#pl-save').onclick = ()=>{
        const selectedId = String(overlay.querySelector('#pl-select').value || '').trim();
        const newName = String(overlay.querySelector('#pl-new').value || '').trim();
        let selected = null;
        if (selectedId) selected = playlists.find((p)=> String(p.id) === selectedId) || null;
        else if (newName){
          selected = { id: `pl_${Date.now()}`, name: newName, items: [] };
          playlists.push(selected);
        }
        if (!selected) return;
        if (!Array.isArray(selected.items)) selected.items = [];
        selected.items.push({
          id: `it_${Date.now()}_${Math.random().toString(36).slice(2,6)}`,
          src: track.src,
          title: track.title || 'Track',
          by: track.by || '',
          cover: track.cover || ''
        });
        try{ localStorage.setItem(key, JSON.stringify(playlists)); }catch(_){ }
        remove();
      };
    }

    renderNamedAudioAttachment(containerEl, src, fileName, authorName = '', sourceKey = ''){
      const safeName = String(fileName || 'Audio');
      const safeAuthor = String(authorName || 'Unknown');
      const wrap = document.createElement('div');
      wrap.className = 'audio-attachment-block post-media-files-item shared-audio-waveconnect';
      const nameEl = document.createElement('div');
      nameEl.className = 'audio-attachment-head';
      nameEl.style.cssText = 'margin-bottom:6px';
      nameEl.innerHTML = `<div class="post-media-audio-head-text"><span class="post-media-audio-title">${this.renderText(safeName)}</span><span class="post-media-audio-by">by ${this.renderText(safeAuthor)}</span></div>`;
      wrap.appendChild(nameEl);
      const audio = document.createElement('audio');
      audio.className = 'liber-lib-audio';
      audio.src = src;
      audio.controls = false;
      audio.style.display = 'none';
      audio.dataset.title = safeName;
      audio.dataset.by = safeAuthor;
      audio.preload = 'auto';
      audio.addEventListener('play', ()=> this.notifyParentAudioMetaOnly(audio), { once: false });
      const wrapper = document.createElement('div');
      wrapper.className = 'voice-wave-player';
      wrapper.style.cssText = 'display:flex;align-items:center;gap:8px';
      const playBtn = document.createElement('button');
      playBtn.className = 'play';
      playBtn.innerHTML = '<i class="fas fa-play"></i>';
      playBtn.title = 'Play';
      const wave = document.createElement('div');
      wave.className = 'wave';
      const time = document.createElement('div');
      time.className = 'time';
      time.textContent = '0:00 / 0:00';
      const addBtn = document.createElement('button');
      addBtn.type = 'button';
      addBtn.className = 'play';
      addBtn.style.cssText = 'width:28px;height:28px;flex-shrink:0';
      addBtn.innerHTML = '<i class="fas fa-plus"></i>';
      addBtn.title = 'Add to library';
      const seed = String(safeName || src || 'audio');
      const barsCount = 54;
      this.paintSeedWaveBars(wave, barsCount, seed);
      this.getWaveHeightsForAudio(src, barsCount).then((heights)=>{
        if (Array.isArray(heights) && heights.length) this.applyWaveHeights(wave, heights);
      }).catch(()=>{});
      const widget = {
        id: `ua_${Date.now()}_${Math.random().toString(36).slice(2,7)}`,
        src, srcKey: String(sourceKey || '').trim(), title: safeName,
        playBtn, wave, time, showRemaining: false, durationGuess: 0
      };
      this._voiceWidgets.set(widget.id, widget);
      this.startVoiceWidgetTicker();
      const sync = ()=>{
        const p = this.ensureChatBgPlayer();
        const playerSrc = this.getChatPlayerSrc(p);
        const active = (!!playerSrc && (src === playerSrc || this.isSameMediaSrc(src, playerSrc)))
          || (!!this._voiceCurrentSrc && (src === this._voiceCurrentSrc || this.isSameMediaSrc(src, this._voiceCurrentSrc)))
          || (!!this._voiceCurrentAttachmentKey && !!sourceKey && this._voiceCurrentAttachmentKey === sourceKey);
        const d = active && Number.isFinite(p.duration) && p.duration > 0 ? Number(p.duration) : Number(audio.duration || 0);
        const c = active ? Number(p.currentTime || 0) : Number(audio.currentTime || 0);
        if (active && d > 0) widget.durationGuess = d;
        const ratio = d > 0 ? Math.min(1, Math.max(0, c / d)) : 0;
        const bars = wave.querySelectorAll('.bar');
        const played = Math.round(bars.length * ratio);
        bars.forEach((b, i)=> b.classList.toggle('played', active && i < played));
        const paused = active ? !!p.paused : !!audio.paused;
        playBtn.innerHTML = `<i class="fas ${paused ? 'fa-play' : 'fa-pause'}"></i>`;
        playBtn.title = paused ? 'Play' : 'Stop';
        time.textContent = `${this.formatDuration(c)} / ${this.formatDuration(d)}`;
      };
      ['play','pause','timeupdate','loadedmetadata','ended'].forEach(ev=> audio.addEventListener(ev, sync));
      audio.addEventListener('error', ()=>{ try{ time.textContent = 'Error loading'; }catch(_){ } });
      playBtn.addEventListener('click', (e)=>{
        try{ e.stopPropagation(); }catch(_){ }
        const p = this.ensureChatBgPlayer();
        const isThisPlaying = (!!this.getChatPlayerSrc(p) && (this.isSameMediaSrc(this.getChatPlayerSrc(p), src) || (!!sourceKey && this._voiceCurrentAttachmentKey === sourceKey))) && !p.paused;
        if (isThisPlaying){
          this._voiceUserIntendedPlay = false;
          p.pause();
          this.updateVoiceWidgets();
          return;
        }
        this.playChatAudioInBgPlayer({ src, title: safeName, by: safeAuthor, cover: '', sourceKey });
        sync();
      });
      const seekTo = (clientX)=>{
        const rect = wave.getBoundingClientRect();
        const ratio = Math.min(1, Math.max(0, (clientX - rect.left) / rect.width));
        const p = this.ensureChatBgPlayer();
        if (this.isSameMediaSrc(this.getChatPlayerSrc(p), src) && Number.isFinite(p.duration) && p.duration > 0){
          p.currentTime = ratio * p.duration;
          if (p.paused){ this._voiceUserIntendedPlay = true; p.play().catch(()=>{}); }
        } else {
          if (Number(audio.duration) > 0) audio.currentTime = ratio * audio.duration;
          this.playChatAudioInBgPlayer({ src, title: safeName, by: safeAuthor, cover: '', sourceKey, currentTime: ratio * (audio.duration || 0) });
        }
        sync();
      };
      wave.addEventListener('click', (e)=>{ try{ e.stopPropagation(); }catch(_){ } seekTo(e.clientX); });
      let dragging = false;
      wave.addEventListener('pointerdown', (e)=>{ dragging = true; wave.setPointerCapture(e.pointerId); seekTo(e.clientX); });
      wave.addEventListener('pointermove', (e)=>{ if (dragging) seekTo(e.clientX); });
      wave.addEventListener('pointerup', (e)=>{ dragging = false; try{ wave.releasePointerCapture(e.pointerId); }catch(_){ } });
      addBtn.addEventListener('click', (e)=>{
        try{ e.preventDefault(); e.stopPropagation(); this.addToChatAudioPlaylist({ src, title: safeName, author: safeAuthor, sourceKey }); }catch(_){ }
      });
      wrapper.appendChild(playBtn);
      wrapper.appendChild(wave);
      wrapper.appendChild(time);
      wrapper.appendChild(addBtn);
      wrap.appendChild(audio);
      wrap.appendChild(wrapper);
      containerEl.appendChild(wrap);
      sync();
    }

    renderInlineWaveAudio(hostEl, url, title = 'Audio', sourceKey = ''){
      try{
        const audio = document.createElement('audio');
        audio.src = url;
        audio.controls = false;
        audio.style.display = 'none';
        audio.preload = 'metadata';
        const seed = String(title || url || 'audio');
        const wrapper = document.createElement('div');
        wrapper.className = 'voice-wave-player';
        const playBtn = document.createElement('button');
        playBtn.className = 'play';
        playBtn.innerHTML = '<i class="fas fa-play"></i>';
        const wave = document.createElement('div');
        wave.className = 'wave';
        const time = document.createElement('div');
        time.className = 'time';
        time.textContent = '0:00 / 0:00';
        const barsCount = 54;
        this.paintSeedWaveBars(wave, barsCount, seed);
        this.getWaveHeightsForAudio(url, barsCount).then((heights)=>{
          if (Array.isArray(heights) && heights.length){ this.applyWaveHeights(wave, heights); sync(); }
        }).catch(()=>{});
        const widget = { id: `wca_${Date.now()}_${Math.random().toString(36).slice(2,7)}`, src: url, srcKey: String(sourceKey || '').trim(), title, playBtn, wave, time, showRemaining: false, durationGuess: 0 };
        this._voiceWidgets.set(widget.id, widget);
        this.startVoiceWidgetTicker();
        const sync = ()=>{
          const p = this.ensureChatBgPlayer();
          const playerSrc = this.getChatPlayerSrc(p);
          const active = (!!playerSrc && (url === playerSrc || this.isSameMediaSrc(url, playerSrc))) || (!!this._voiceCurrentSrc && (url === this._voiceCurrentSrc || this.isSameMediaSrc(url, this._voiceCurrentSrc)));
          const d = active && Number.isFinite(p.duration) && p.duration > 0 ? Number(p.duration) : Number(audio.duration || 0);
          const c = active ? Number(p.currentTime || 0) : Number(audio.currentTime || 0);
          if (active && d > 0) widget.durationGuess = d;
          const ratio = d > 0 ? Math.min(1, Math.max(0, c / d)) : 0;
          const bars = wave.querySelectorAll('.bar');
          const played = Math.round(bars.length * ratio);
          bars.forEach((b, i)=> b.classList.toggle('played', active && i < played));
          playBtn.innerHTML = `<i class="fas ${active ? (p.paused ? 'fa-play' : 'fa-pause') : (audio.paused ? 'fa-play' : 'fa-pause')}"></i>`;
          time.textContent = `${this.formatDuration(c)} / ${this.formatDuration(d)}`;
        };
        ['play','pause','timeupdate','loadedmetadata','ended'].forEach(ev=> audio.addEventListener(ev, sync));
        playBtn.addEventListener('click', (e)=>{
          try{ e.stopPropagation(); }catch(_){ }
          const p = this.ensureChatBgPlayer();
          const isThisPlaying = (!!this.getChatPlayerSrc(p) && (this.isSameMediaSrc(this.getChatPlayerSrc(p), url) || (!!sourceKey && this._voiceCurrentAttachmentKey === sourceKey))) && !p.paused;
          if (isThisPlaying){ this._voiceUserIntendedPlay = false; p.pause(); this.updateVoiceWidgets(); return; }
          this.playChatAudioInBgPlayer({ src: url, title, by: '', cover: '', sourceKey });
          sync();
        });
        const seekTo = (clientX)=>{
          const rect = wave.getBoundingClientRect();
          const ratio = Math.min(1, Math.max(0, (clientX - rect.left) / rect.width));
          const p = this.ensureChatBgPlayer();
          if (this.isSameMediaSrc(this.getChatPlayerSrc(p), url) && Number.isFinite(p.duration) && p.duration > 0){
            p.currentTime = ratio * p.duration;
            if (p.paused){ this._voiceUserIntendedPlay = true; p.play().catch(()=>{}); }
          } else this.playChatAudioInBgPlayer({ src: url, title, by: '', cover: '', sourceKey, currentTime: ratio * (audio.duration || 0) });
          sync();
        };
        wave.addEventListener('click', (e)=>{ try{ e.stopPropagation(); }catch(_){ } seekTo(e.clientX); });
        let dragging = false;
        wave.addEventListener('pointerdown', (e)=>{ dragging = true; wave.setPointerCapture(e.pointerId); seekTo(e.clientX); });
        wave.addEventListener('pointermove', (e)=>{ if (dragging) seekTo(e.clientX); });
        wave.addEventListener('pointerup', (e)=>{ dragging = false; try{ wave.releasePointerCapture(e.pointerId); }catch(_){ } });
        wrapper.appendChild(playBtn);
        wrapper.appendChild(wave);
        wrapper.appendChild(time);
        hostEl.appendChild(audio);
        hostEl.appendChild(wrapper);
        sync();
      }catch(_){ }
    }

    setupMediaSessionForVoice(title = 'Voice message'){
      const p = this.ensureChatBgPlayer();
      if (!('mediaSession' in navigator)) return;
      try{
        navigator.mediaSession.metadata = new MediaMetadata({
          title,
          artist: 'LIBER Chat'
        });
        navigator.mediaSession.setActionHandler('play', ()=>{ this._voiceUserIntendedPlay = true; p.play().catch(()=>{}); });
        navigator.mediaSession.setActionHandler('pause', ()=>{ this._voiceUserIntendedPlay = false; p.pause(); });
        navigator.mediaSession.setActionHandler('seekbackward', ()=>{ p.currentTime = Math.max(0, (p.currentTime||0)-10); });
        navigator.mediaSession.setActionHandler('seekforward', ()=>{ p.currentTime = Math.min((p.duration||0), (p.currentTime||0)+10); });
      }catch(_){ }
    }

    normalizeEncryptedPayload(payload){
      try{
        if (!payload) return null;
        if (typeof payload === 'string'){
          const trimmed = String(payload).trim();
          if (!trimmed) return null;
          if (/^data:[^,]+,/.test(trimmed)) return { _inlineBase64: trimmed.split(',')[1] || '' };
          try{ payload = JSON.parse(trimmed); }catch(_){ return null; }
        }
        if (payload && typeof payload === 'object'){
          if (typeof payload.iv === 'string' && typeof payload.data === 'string') return payload;
          const inner = payload.cipher || payload.encrypted || payload.enc || payload.ciphertext;
          if (inner && typeof inner.iv === 'string' && typeof inner.data === 'string') return inner;
          const inline = this.extractInlineBase64Payload(payload);
          if (inline) return { _inlineBase64: inline };
        }
      }catch(_){ }
      return null;
    }

    async renderEncryptedAttachment(containerEl, fileUrl, fileName, aesKey, sourceConnId = this.activeConnection, senderDisplayName = '', message = null){
      try {
        if (!containerEl?.isConnected) return;
        const cid = message?.connId || sourceConnId || this.activeConnection;
        const msgId = message?.id;
        if (msgId && cid && this.db) {
          try {
            const snap = await firebase.getDoc(firebase.doc(this.db,'chatMessages',cid,'messages',msgId));
            if (snap.exists()) {
              const fresh = snap.data() || {};
              message = { ...message, ...fresh };
            }
          }catch(_){}
        }
        let payload;
        const isRec = this.isVideoRecordingMessage(message, fileName) || this.isVoiceRecordingMessage(message, fileName);
        const storagePathMatch = /\/o\/([^?#]+)/i.exec(String(fileUrl || ''));
        if (!isRec && this.storage && firebase.getBlob && storagePathMatch?.[1]) {
          try {
            const pathDecoded = decodeURIComponent(storagePathMatch[1]);
            const sref = firebase.ref(this.storage, pathDecoded);
            const blob = await firebase.getBlob(sref);
            const raw = await new Promise((res,rej)=>{ const r=new FileReader(); r.onload=()=>res(r.result); r.onerror=rej; r.readAsText(blob); });
            payload = (typeof raw === 'string' && raw) ? (()=>{ try{ return JSON.parse(raw); }catch(_){ return null; } })() : null;
          } catch (_) {}
        }
        if (!payload) {
          const res = await fetch(fileUrl, { mode: 'cors', cache: 'default' });
          if (!res.ok) throw new Error('attachment-fetch-failed');
          const raw = await res.text();
          try{ payload = raw ? JSON.parse(raw) : null; }
          catch(_){}
        }
        const payloadCands = this.extractEncryptedPayloadCandidates(payload || {});
        let found = payload && typeof payload.iv === 'string' && typeof payload.data === 'string' ? payload : null;
        if (!found && payloadCands.length){
          for (const c of payloadCands){
            if (c && typeof c === 'object' && typeof c.iv === 'string' && typeof c.data === 'string'){ found = c; break; }
          }
        }
        if (!found){
          const inline = this.extractInlineBase64Payload(payload || {});
          if (inline){ payload = { _inlineBase64: inline }; }
        }
        let cipher = this.normalizeEncryptedPayload(payload);
        let b64 = null;
        if (cipher?._inlineBase64){
          b64 = this.normalizeBinaryPayloadString(cipher._inlineBase64);
        }
        if (!b64){
          if (cipher && !cipher._inlineBase64){
            payload = cipher;
          } else if (found){
            payload = found;
          } else if (payload && typeof payload === 'object'){
            const candidates = this.extractEncryptedPayloadCandidates(payload);
            for (const c of candidates){
              if (c && typeof c === 'object' && typeof c.iv === 'string' && typeof c.data === 'string'){
                payload = c;
                break;
              }
            }
          }
          if (!payload || typeof payload.iv !== 'string' || typeof payload.data !== 'string'){
            const inline = this.extractInlineBase64Payload(payload || {});
            if (inline) b64 = this.normalizeBinaryPayloadString(inline);
            if (!b64) throw new Error('invalid-payload-structure');
          }
        }
        const isVideoRecording = this.isVideoRecordingMessage(message, fileName);
        const isVoiceRecording = this.isVoiceRecordingMessage(message, fileName);
        const hintSalt = String(message?.attachmentKeySalt || '').trim();
        const urlConnId = this.extractConnIdFromAttachmentUrl(fileUrl);
        const cryptoMod = window.chatCrypto || (typeof chatCrypto !== 'undefined' ? chatCrypto : null);
        if (!b64 && aesKey && payload?.iv && payload?.data) {
          try { b64 = await chatCrypto.decryptWithKey(payload, aesKey); } catch (_) {}
        }
        if (!b64 && hintSalt && cryptoMod) {
          try {
            const hintedKey = await cryptoMod.deriveChatKey(`${hintSalt}|liber_secure_chat_conn_stable_v1`);
            b64 = await chatCrypto.decryptWithKey(payload, hintedKey);
          } catch (_) {}
        }
        if (!b64 && (isVideoRecording || isVoiceRecording) && cryptoMod) {
          const connIdR = urlConnId || sourceConnId || message?.attachmentSourceConnId || message?.connId || this.activeConnection;
          const saltsR = [hintSalt, urlConnId];
          try {
            const s = await this.getConnSaltForConn(connIdR);
            const st = String(s?.stableSalt || '').trim();
            if (st && !saltsR.includes(st)) saltsR.push(st);
          } catch (_) {}
          for (const s of saltsR) {
            if (b64 || !s) continue;
            try {
              const k = await cryptoMod.deriveChatKey(`${s}|liber_secure_chat_conn_stable_v1`);
              b64 = await chatCrypto.decryptWithKey(payload, k);
            } catch (_) {}
          }
          if (!b64 && connIdR) {
            try {
              const keys = await this.getFallbackKeyCandidatesForConn(connIdR);
              for (const k of (keys || [])) {
                if (b64) break;
                try { b64 = await chatCrypto.decryptWithKey(payload, k); } catch (_) {}
              }
            } catch (_) {}
            if (!b64) {
              try {
                const k = await this.getFallbackKeyForConn(connIdR);
                b64 = await chatCrypto.decryptWithKey(payload, k);
              } catch (_) {}
            }
          }
        }
        if (!b64) {
          let decrypted = false;
          if (urlConnId || hintSalt) {
            try {
              const saltsToTry = [];
              if (hintSalt) saltsToTry.push(hintSalt);
              if (urlConnId && !saltsToTry.includes(urlConnId)) saltsToTry.push(urlConnId);
              try {
                const salts = await this.getConnSaltForConn(urlConnId || sourceConnId || this.activeConnection);
                const stableSalt = String(salts?.stableSalt || '').trim();
                if (stableSalt && !saltsToTry.includes(stableSalt)) saltsToTry.push(stableSalt);
              } catch (_) {}
              for (const salt of saltsToTry) {
                if (decrypted || !salt) break;
                try {
                  const key = await window.chatCrypto.deriveChatKey(`${salt}|liber_secure_chat_conn_stable_v1`);
                  b64 = await chatCrypto.decryptWithKey(payload, key);
                  decrypted = true;
                } catch (_) {}
              }
              const connIdForKey = urlConnId || sourceConnId || message?.attachmentSourceConnId || this.activeConnection;
              if (!decrypted && connIdForKey) {
                const allCandidates = await this.getFallbackKeyCandidatesForConn(connIdForKey);
                for (const k of (allCandidates || [])) {
                  if (decrypted) break;
                  try {
                    b64 = await chatCrypto.decryptWithKey(payload, k);
                    decrypted = true;
                  } catch (_) {}
                }
              }
              if (!decrypted && connIdForKey) {
                const fallbackKey = await this.getFallbackKeyForConn(connIdForKey);
                try {
                  b64 = await chatCrypto.decryptWithKey(payload, fallbackKey);
                  decrypted = true;
                } catch (_) {}
              }
            } catch (_) {}
          }
          // Salt-based derivation (attachmentKeySalt, connection key, etc.)
          let saltsToTryFirst = [hintSalt, urlConnId].filter(Boolean);
          const connIdsForKey = [sourceConnId, message?.attachmentSourceConnId, message?.connId, urlConnId, this.activeConnection].filter(Boolean);
          if (connIdsForKey.length) {
            const seenKeys = new Set(saltsToTryFirst);
            for (const cid of connIdsForKey) {
              const row = (this.connections || []).find((c)=> c && c.id === cid);
              let connKey = String(row?.key || '').trim() || (row ? this.computeConnKey(this.getConnParticipants(row)) : '');
              if (!connKey && cid) {
                try {
                  const snap = await firebase.getDoc(firebase.doc(this.db,'chatConnections', cid));
                  if (snap.exists()) {
                    const d = snap.data() || {};
                    connKey = String(d.key || '').trim() || this.computeConnKey(this.getConnParticipants(d));
                  }
                }catch(_){}
              }
              if (connKey && !seenKeys.has(connKey)){ seenKeys.add(connKey); saltsToTryFirst.push(connKey); }
            }
          }
          for (const salt of saltsToTryFirst) {
            if (decrypted) break;
            try {
              const key = await window.chatCrypto.deriveChatKey(`${salt}|liber_secure_chat_conn_stable_v1`);
              b64 = await chatCrypto.decryptWithKey(payload, key);
              decrypted = true;
            } catch (_) {}
            if (!decrypted && window.chatCrypto?.deriveFallbackSharedAesKey) {
              const peerUid = String(message?.sender || '').trim() || await this.getPeerUidForConn(sourceConnId || urlConnId);
              if (peerUid) {
                try {
                  const key = await window.chatCrypto.deriveFallbackSharedAesKey(this.currentUser.uid, peerUid, salt);
                  b64 = await chatCrypto.decryptWithKey(payload, key);
                  decrypted = true;
                } catch (_) {}
              }
            }
          }
          if (!decrypted && hintSalt && !saltsToTryFirst.includes(hintSalt)) {
            try {
              const key = await window.chatCrypto.deriveChatKey(`${hintSalt}|liber_secure_chat_conn_stable_v1`);
              b64 = await chatCrypto.decryptWithKey(payload, key);
              decrypted = true;
            } catch (_) {}
            if (!decrypted && window.chatCrypto?.deriveFallbackSharedAesKey) {
              const peerUid = String(message?.sender || '').trim() || await this.getPeerUidForConn(sourceConnId);
              if (peerUid) {
                try {
                  const key = await window.chatCrypto.deriveFallbackSharedAesKey(this.currentUser.uid, peerUid, hintSalt);
                  b64 = await chatCrypto.decryptWithKey(payload, key);
                  decrypted = true;
                } catch (_) {}
              }
            }
          }
          const connIdsToTry = [urlConnId, sourceConnId, message?.attachmentSourceConnId, message?.connId, this.activeConnection].filter(Boolean);
          const seen = new Set();
          for (const cid of connIdsToTry) {
            if (!cid || seen.has(cid)) continue;
            seen.add(cid);
            if (decrypted) break;
            try {
              const key = await this.getFallbackKeyForConn(cid);
              b64 = await chatCrypto.decryptWithKey(payload, key);
              decrypted = true;
              break;
            } catch (_) {}
          }
          const candidateConnIds = [];
          const pushConn = (cid)=>{
            const id = String(cid || '').trim();
            if (!id) return;
            if (candidateConnIds.includes(id)) return;
            candidateConnIds.push(id);
          };
          pushConn(sourceConnId);
          pushConn(message?.attachmentSourceConnId);
          pushConn(message?.connId);
          pushConn(this.extractConnIdFromAttachmentUrl(fileUrl));
          pushConn(this.activeConnection);
          try{
            const sourceRow = (this.connections || []).find((c)=> c && c.id === sourceConnId) || null;
            const sourceParts = this.getConnParticipants(sourceRow || {});
            const sourceKey = String(sourceRow?.key || '').trim() || this.computeConnKey(sourceParts);
            if (sourceKey){
              for (const c of (this.connections || [])){
                if (!c || !c.id) continue;
                const cParts = this.getConnParticipants(c || {});
                const cKey = String(c.key || '').trim() || this.computeConnKey(cParts);
                if (cKey === sourceKey) pushConn(c.id);
              }
            }
          }catch(_){ }
          for (const cid of candidateConnIds){
            if (decrypted) break;
            try{
              const candidates = await this.getFallbackKeyCandidatesForConn(cid);
              // Include legacy fallback keys too (often placed after newer keys).
              for (const k of (candidates || [])){
                try{
                  b64 = await chatCrypto.decryptWithKey(payload, k);
                  decrypted = true;
                  break;
                }catch(_){ }
              }
            }catch(_){ }
            if (!decrypted){
              try{
                const alt = await this.getFallbackKeyForConn(cid);
                b64 = await chatCrypto.decryptWithKey(payload, alt);
                decrypted = true;
              }catch(_){ }
            }
            if (!decrypted){
              // Compatibility: recover files encrypted when peer resolution was missing at send time.
              try{
                let senderUid = String(message?.sender || '').trim();
                if (!senderUid){
                  const peerFromConn = await this.getPeerUidForConn(cid);
                  senderUid = String(peerFromConn || '').trim();
                }
                if (senderUid){
                  const compat = await window.chatCrypto.deriveChatKey(`${['', senderUid].sort().join('|')}|${cid}|liber_secure_chat_fallback_v1`);
                  b64 = await chatCrypto.decryptWithKey(payload, compat);
                  decrypted = true;
                }
              }catch(_){ }
            }
          }
          if (!decrypted){
            try{
              const hintSalt = String(message?.attachmentKeySalt || '').trim();
              if (hintSalt){
                try{
                  const hinted = await window.chatCrypto.deriveChatKey(`${hintSalt}|liber_secure_chat_conn_stable_v1`);
                  b64 = await chatCrypto.decryptWithKey(payload, hinted);
                  decrypted = true;
                }catch(_){ }
                if (!decrypted){
                  const senderUid = String(message?.sender || '').trim();
                  if (senderUid){
                    const compatHinted = await window.chatCrypto.deriveChatKey(`${[this.currentUser.uid, senderUid].sort().join('|')}|${hintSalt}|liber_secure_chat_fallback_v1`);
                    b64 = await chatCrypto.decryptWithKey(payload, compatHinted);
                    decrypted = true;
                  }
                }
                if (!decrypted && window.chatCrypto && typeof window.chatCrypto.deriveFallbackSharedAesKey === 'function'){
                  try{
                    const senderUid = String(message?.sender || '').trim();
                    if (senderUid){
                      const hintedShared = await window.chatCrypto.deriveFallbackSharedAesKey(this.currentUser.uid, senderUid, hintSalt);
                      b64 = await chatCrypto.decryptWithKey(payload, hintedShared);
                      decrypted = true;
                    }
                  }catch(_){ }
                }
              }
            }catch(_){ }
          }
          if (!decrypted){
            try{
              const salts = new Set();
              const addSalt = (v)=>{ const s = String(v || '').trim(); if (s) salts.add(s); };
              addSalt(message?.attachmentKeySalt);
              addSalt(sourceConnId);
              addSalt(message?.attachmentSourceConnId);
              addSalt(message?.connId);
              addSalt(this.extractConnIdFromAttachmentUrl(fileUrl));
              addSalt(this.activeConnection);
              const peers = new Set();
              const addPeer = (v)=>{ const s = String(v || '').trim(); if (s && s !== this.currentUser.uid) peers.add(s); };
              addPeer(message?.sender);
              for (const cid of candidateConnIds){
                try{ addPeer(await this.getPeerUidForConn(cid)); }catch(_){ }
              }
              for (const salt of salts){
                if (decrypted) break;
                try{
                  const kStable = await window.chatCrypto.deriveChatKey(`${salt}|liber_secure_chat_conn_stable_v1`);
                  b64 = await chatCrypto.decryptWithKey(payload, kStable);
                  decrypted = true;
                  break;
                }catch(_){ }
                for (const peer of peers){
                  if (decrypted) break;
                  try{
                    const kCompat = await window.chatCrypto.deriveChatKey(`${[this.currentUser.uid, peer].sort().join('|')}|${salt}|liber_secure_chat_fallback_v1`);
                    b64 = await chatCrypto.decryptWithKey(payload, kCompat);
                    decrypted = true;
                    break;
                  }catch(_){ }
                  if (!decrypted && window.chatCrypto && typeof window.chatCrypto.deriveFallbackSharedAesKey === 'function'){
                    try{
                      const kShared = await window.chatCrypto.deriveFallbackSharedAesKey(this.currentUser.uid, peer, salt);
                      b64 = await chatCrypto.decryptWithKey(payload, kShared);
                      decrypted = true;
                      break;
                    }catch(_){ }
                  }
                }
              }
            }catch(_){ }
          }
          // Keep chat switching smooth: do not fan out decrypt attempts across all chats.
          if (!decrypted && (isVideoRecording || this.isVideoFilename(fileName))){
            const recentConnIds = (this.connections || [])
              .map((c)=> c && c.id)
              .filter((cid)=> cid && !candidateConnIds.includes(cid))
              .slice(0, 200);
            for (const cid of recentConnIds){
              if (decrypted) break;
              try{
                const candidates = await this.getFallbackKeyCandidatesForConn(cid);
                for (const k of (candidates || [])){
                  try{
                    b64 = await chatCrypto.decryptWithKey(payload, k);
                    decrypted = true;
                    break;
                  }catch(_){ }
                }
              }catch(_){ }
            }
            if (!decrypted){
              try{
                const allConnIds = await this.getAllMyConnectionIdsForDecrypt(1500);
                for (const cid of allConnIds){
                  if (decrypted) break;
                  if (!cid || candidateConnIds.includes(cid)) continue;
                  try{
                    const candidates = await this.getFallbackKeyCandidatesForConn(cid);
                    for (const k of (candidates || [])){
                      try{
                        b64 = await chatCrypto.decryptWithKey(payload, k);
                        decrypted = true;
                        break;
                      }catch(_){ }
                    }
                  }catch(_){ }
                }
              }catch(_){ }
            }
          }
          if (!decrypted){
            // Last-chance compatibility pass for legacy encrypted payload wrappers.
            try{
              const payloadCandidates = this.extractEncryptedPayloadCandidates(payload);
              const keyCandidates = [];
              const keySeen = new Set();
              const pushKey = (k)=>{
                const key = String(k || '');
                if (!key || keySeen.has(key)) return;
                keySeen.add(key);
                keyCandidates.push(k);
              };
              pushKey(aesKey);
              try{
                for (const cid of candidateConnIds){
                  const ks = await this.getFallbackKeyCandidatesForConn(cid);
                  (ks || []).forEach((k)=> pushKey(k));
                }
              }catch(_){ }
              try{
                if (isVideoRecording || this.isVideoFilename(fileName)){
                  const allConnIds = await this.getAllMyConnectionIdsForDecrypt(1500);
                  for (const cid of allConnIds){
                    const ks = await this.getFallbackKeyCandidatesForConn(cid);
                    (ks || []).forEach((k)=> pushKey(k));
                  }
                }
              }catch(_){ }
              try{ pushKey(await this.getFallbackKey()); }catch(_){ }
              for (const cand of payloadCandidates){
                if (decrypted) break;
                for (const key of keyCandidates){
                  try{
                    b64 = await chatCrypto.decryptWithKey(cand, key);
                    decrypted = true;
                    break;
                  }catch(_){ }
                }
              }
            }catch(_){ }
          }
          if (!decrypted && (isVideoRecording || isVoiceRecording)){
            try{
              const ecdh = await this.getOrCreateSharedAesKey();
              if (ecdh) {
                b64 = await chatCrypto.decryptWithKey(payload, ecdh);
                decrypted = true;
              }
            }catch(_){}
          }
          if (!decrypted){
            // Legacy recordings could be encrypted with non-connection key.
            try{
              const legacy = await this.getFallbackKey();
              b64 = await chatCrypto.decryptWithKey(payload, legacy);
              decrypted = true;
            }catch(_legacy){
              const inline = this.extractInlineBase64Payload(payload);
              if (inline){
                b64 = inline;
                decrypted = true;
              }else{
                throw _legacy;
              }
            }
          }
        }
        const resolvedB64 = this.normalizeBinaryPayloadString(b64);
        if (!resolvedB64){
          throw new Error('empty-decrypted-payload');
        }
        if (!containerEl?.isConnected) return;
        this._voiceWidgets.forEach((w, id)=>{
          if (w?.playBtn && containerEl.contains(w.playBtn)) this._voiceWidgets.delete(id);
        });
        try { containerEl.innerHTML = ''; } catch (_) {}
        if (this.isImageFilename(fileName)){
          const mime = fileName.toLowerCase().endsWith('.png') ? 'image/png'
                      : fileName.toLowerCase().endsWith('.webp') ? 'image/webp'
                      : fileName.toLowerCase().endsWith('.gif') ? 'image/gif'
                      : 'image/jpeg';
          const blob = this.base64ToBlob(resolvedB64, mime);
          const cacheKey = `img|${String(message?.id || '')}|${String(fileUrl || '')}|${String(fileName || '')}`;
          const url = this.getStableBlobUrl(cacheKey, blob);
          const img = document.createElement('img');
          img.src = url; img.style.maxWidth = '100%'; img.style.height='auto'; img.style.borderRadius='8px'; img.alt = fileName;
          img.setAttribute('data-fullscreen-image', '1');
          containerEl.appendChild(img);
        } else if (this.isVideoFilename(fileName)){
          const mime = this.detectMimeFromBase64(resolvedB64, this.inferVideoMime(fileName));
          const blob = this.base64ToBlob(resolvedB64, mime);
          const cacheKey = `vid|${String(message?.id || '')}|${String(fileUrl || '')}|${String(fileName || '')}|${mime}`;
          const url = this.getStableBlobUrl(cacheKey, blob);
          const video = document.createElement('video');
          video.src = url;
          video.playsInline = true;
          video.style.maxWidth = '100%';
          video.style.borderRadius = '8px';
          if (isVideoRecording){
            video.controls = false;
            video.classList.add('video-recording-mask');
            this.applyRandomTriangleMask(video);
            this.bindInlineVideoPlayback(video, fileName || 'Video message');
          } else {
            video.controls = true;
          }
          containerEl.appendChild(video);
        } else if (this.isAudioFilename(fileName)){
          const mime = this.detectMimeFromBase64(resolvedB64, this.inferAudioMime(fileName));
          const blob = this.base64ToBlob(resolvedB64, mime);
          const attachmentSourceKey = `aud|${String(message?.id || '')}|${String(fileUrl || '')}|${String(fileName || '')}|${mime}`;
          const cacheKey = `aud|${String(message?.id || '')}|${String(fileUrl || '')}|${String(fileName || '')}|${mime}`;
          const url = this.getStableBlobUrl(cacheKey, blob);
          const audioType = this.getChatAudioType(message, fileName, false);
          if (audioType === 'voice'){
            this.renderWaveAttachment(containerEl, url, senderDisplayName || 'Voice message', attachmentSourceKey);
          } else {
            this.renderNamedAudioAttachment(containerEl, url, fileName || 'Audio', senderDisplayName || 'Unknown', attachmentSourceKey);
          }
        } else if ((fileName||'').toLowerCase().endsWith('.pdf')){
          const blob = this.base64ToBlob(resolvedB64, 'application/pdf');
          const cacheKey = `pdf|${String(message?.id || '')}|${String(fileUrl || '')}|${String(fileName || '')}`;
          const url = this.getStableBlobUrl(cacheKey, blob);
          const row = document.createElement('div');
          row.style.display = 'flex';
          row.style.gap = '8px';
          row.style.alignItems = 'center';
          row.style.flexWrap = 'wrap';
          const info = document.createElement('span');
          info.textContent = fileName || 'document.pdf';
          info.style.opacity = '0.9';
          const btn = document.createElement('a');
          btn.href = url;
          btn.download = fileName || 'document.pdf';
          btn.className = 'btn btn-secondary';
          btn.textContent = 'Download PDF';
          row.appendChild(info);
          row.appendChild(btn);
          containerEl.appendChild(row);
        } else {
          const blob = this.base64ToBlob(resolvedB64, 'application/octet-stream');
          const cacheKey = `bin|${String(message?.id || '')}|${String(fileUrl || '')}|${String(fileName || '')}`;
          const url = this.getStableBlobUrl(cacheKey, blob);
          const row = document.createElement('div');
          row.style.display = 'flex';
          row.style.gap = '8px';
          row.style.alignItems = 'center';
          row.style.flexWrap = 'wrap';
          const info = document.createElement('span');
          info.textContent = fileName || 'attachment.bin';
          info.style.opacity = '0.9';
          const btn = document.createElement('a');
          btn.href = url;
          btn.download = fileName || 'attachment.bin';
          btn.className = 'btn btn-secondary';
          btn.textContent = 'Download decrypted file';
          row.appendChild(info);
          row.appendChild(btn);
          containerEl.appendChild(row);
        }
      } catch (e) {
        if (!containerEl?.isConnected) return;
        try{ containerEl.innerHTML = ''; }catch(_){ }
        const looksEncrypted = /\.enc\.json(?:$|\?)/i.test(String(fileUrl || ''));
        const isFetchFail = String(e?.message || '').includes('fetch-failed') || String(e?.message || '').includes('attachment-fetch');
        const isInvalidPayload = String(e?.message || '').includes('invalid-payload');
        const isVideo = this.isVideoRecordingMessage(message, fileName) || this.isVideoFilename(fileName || '');
        if (looksEncrypted && !isFetchFail){
          const err = document.createElement('div');
          err.className = 'file-link';
          err.textContent = isInvalidPayload ? 'Invalid attachment format' : 'Unable to decrypt attachment';
          containerEl.appendChild(err);
          // Backfill attachmentKeySalt for recordings missing it — next load may decrypt
          const msg = message; const fUrl = fileUrl; const fName = fileName; const srcConn = sourceConnId;
          if (msg?.id && !String(msg?.attachmentKeySalt||'').trim()) {
            const uid = this.extractConnIdFromAttachmentUrl(fUrl);
            const rec = /\.(webm|mp4|mov|mkv)\.enc\.json/i.test(String(fUrl||'')) || (fName||'').toLowerCase().startsWith('video.');
            if (rec && uid) {
              Promise.resolve().then(async ()=>{
                try {
                  const salts = await this.getConnSaltForConn(uid);
                  const salt = String(salts?.stableSalt || uid || '').trim();
                  const cid = msg?.connId || srcConn || uid;
                  if (salt && cid) {
                    await firebase.updateDoc(firebase.doc(this.db,'chatMessages',cid,'messages',msg.id),{ attachmentKeySalt: salt, attachmentSourceConnId: cid, isVideoRecording: true });
                  }
                }catch(_){}
              });
            }
          }
          return;
        }
        if (looksEncrypted && isFetchFail){
          const err = document.createElement('div');
          err.className = 'file-link';
          err.textContent = 'Could not load attachment';
          containerEl.appendChild(err);
          return;
        }
        this.renderDirectAttachment(containerEl, fileUrl, fileName, message, senderDisplayName, !!containerEl?.dataset?.pickerMode);
      }
    }

    renderDirectAttachment(containerEl, fileUrl, fileName, message = null, senderDisplayName = '', pickerMode = false){
      try{
        if (!containerEl?.isConnected) return;
        containerEl.innerHTML = '';
        let name = String(fileName || '');
        const isVideoRecording = this.isVideoRecordingMessage(message, name);
        if (!name && fileUrl){
          try{
            const clean = String(fileUrl).split('?')[0].split('#')[0];
            const tail = clean.split('/').pop() || '';
            name = decodeURIComponent(tail);
          }catch(_){ }
        }
        if (this.isImageFilename(name)){
          const img = document.createElement('img');
          img.src = fileUrl;
          img.style.maxWidth = '100%';
          img.style.height = 'auto';
          img.style.borderRadius = '8px';
          img.setAttribute('data-fullscreen-image', '1');
          containerEl.appendChild(img);
          return;
        }
        if (this.isVideoFilename(name)){
          const v = document.createElement('video');
          v.src = fileUrl;
          v.playsInline = true;
          v.style.maxWidth = '100%';
          v.style.borderRadius = '8px';
          if (isVideoRecording){
            v.controls = false;
            v.classList.add('video-recording-mask');
            this.applyRandomTriangleMask(v);
            this.bindInlineVideoPlayback(v, name || 'Video message');
          } else {
            v.controls = true;
          }
          containerEl.appendChild(v);
          return;
        }
        if (this.isAudioFilename(name)){
          const attachmentSourceKey = `aud|${String(message?.id || '')}|${String(fileUrl || '')}|${String(name || '')}`;
          const audioType = this.getChatAudioType(message, name, false);
          if (audioType === 'voice'){
            this.renderWaveAttachment(containerEl, fileUrl, 'Voice message', attachmentSourceKey);
          } else {
            this.renderNamedAudioAttachment(containerEl, fileUrl, name || 'Audio', senderDisplayName || 'Unknown', attachmentSourceKey);
          }
          return;
        }
        const a = document.createElement('a');
        a.href = fileUrl;
        a.target = '_blank';
        a.rel = 'noopener noreferrer';
        a.textContent = name || 'Open attachment';
        containerEl.appendChild(a);
      }catch(_){ }
    }

    extractInlineBase64Payload(payload){
      try{
        const isLikelyB64 = (s)=> /^[A-Za-z0-9+/_=-]{120,}$/.test(String(s || '').trim());
        if (typeof payload === 'string'){
          const raw = String(payload || '').trim();
          if (/^data:[^,]+,/.test(raw)) return raw.split(',')[1] || '';
          return isLikelyB64(raw) ? raw : '';
        }
        const obj = (payload && typeof payload === 'object') ? payload : null;
        if (!obj) return '';
        const candidates = [
          obj.b64, obj.base64, obj.data, obj.payload, obj.content,
          obj.file, obj.bytes, obj.blob, obj.ciphertext
        ];
        for (const c of candidates){
          const raw = String(c || '').trim();
          if (!raw) continue;
          if (/^data:[^,]+,/.test(raw)) return raw.split(',')[1] || '';
          if (isLikelyB64(raw)) return raw;
        }
      }catch(_){ }
      return '';
    }

    normalizeBinaryPayloadString(value){
      try{
        if (value instanceof Uint8Array){
          let bin = '';
          for (let i = 0; i < value.length; i++) bin += String.fromCharCode(value[i]);
          return btoa(bin);
        }
        if (typeof ArrayBuffer !== 'undefined' && value instanceof ArrayBuffer){
          const u8 = new Uint8Array(value);
          let bin = '';
          for (let i = 0; i < u8.length; i++) bin += String.fromCharCode(u8[i]);
          return btoa(bin);
        }
        if (typeof value === 'string'){
          const raw = String(value || '').trim();
          if (!raw) return '';
          if (/^data:[^,]+,/.test(raw)) return raw.split(',')[1] || '';
          return raw;
        }
        const inline = this.extractInlineBase64Payload(value);
        if (inline) return inline;
        return '';
      }catch(_){ return ''; }
    }

    extractEncryptedPayloadCandidates(payload){
      const out = [];
      const seen = new Set();
      const push = (v)=>{
        if (v === undefined || v === null) return;
        const key = typeof v === 'string' ? `s:${v}` : (()=>{ try{ return `o:${JSON.stringify(v)}`; }catch(_){ return ''; } })();
        if (!key || seen.has(key)) return;
        seen.add(key);
        out.push(v);
      };
      const walk = (v, depth = 0)=>{
        if (depth > 2 || v === undefined || v === null) return;
        push(v);
        if (typeof v === 'string'){
          const raw = String(v || '').trim();
          if (!raw) return;
          try{
            const parsed = JSON.parse(raw);
            if (parsed && typeof parsed === 'object') walk(parsed, depth + 1);
          }catch(_){ }
          return;
        }
        if (typeof v !== 'object') return;
        const keys = ['cipher', 'ciphertext', 'encrypted', 'enc', 'payload', 'data', 'content', 'value', 'body', 'blob', 'file', 'bytes'];
        keys.forEach((k)=>{ try{ walk(v[k], depth + 1); }catch(_){ } });
      };
      try{
        walk(payload, 0);
      }catch(_){ }
      return out;
    }

    base64ToBlob(b64, mime){
      const raw = String(b64 || '').trim();
      const body = /^data:/i.test(raw) && raw.includes(',') ? raw.split(',')[1] : raw;
      const normalized = String(body || '').replace(/\s+/g, '').replace(/-/g, '+').replace(/_/g, '/');
      const padded = normalized + '==='.slice((normalized.length + 3) % 4);
      const byteString = atob(padded);
      const len = byteString.length;
      const bytes = new Uint8Array(len);
      for (let i=0;i<len;i++) bytes[i] = byteString.charCodeAt(i);
      return new Blob([bytes], {type: mime});
    }

    async searchUsers(term){
      const wrapper = document.getElementById('search-results');
      const userGroup = document.getElementById('user-results-group');
      const resultsEl = document.getElementById('user-results');
      const msgGroup = document.getElementById('message-results-group');
      const msgResults = document.getElementById('message-results');
      if (!wrapper || !resultsEl || !msgResults) return;
      resultsEl.innerHTML = '';
      msgResults.innerHTML = '';
      if (!term){
        wrapper.style.display='none';
        if (userGroup) userGroup.style.display='none';
        if (msgGroup) msgGroup.style.display='none';
        this.updateSidebarSearchState(false);
        if (this.isMobileViewport()) this.setMobileMenuOpen(false);
        return;
      }
      try{
        if (window.firebaseService && window.firebaseService.isFirebaseAvailable()){
          const users = await window.firebaseService.searchUsers(term.toLowerCase());
          // Rank by fuzzy score + recency (last messaged) + frequency placeholder
          const filtered = (users||[]).filter(u=> u.uid !== (this.currentUser&&this.currentUser.uid)).map(u=>{
            const name = (u.username||'').toLowerCase(); const mail=(u.email||'').toLowerCase(); const t=term.toLowerCase();
            const contains = (s)=> s.includes(t);
            const prefix = (s)=> s.startsWith(t);
            const subseq = (s)=>{ let i=0; for (const ch of s){ if (ch===t[i]) i++; if (i===t.length) return true; } return t.length===0; };
            let score = 0; if (prefix(name)||prefix(mail)) score+=3; if (contains(name)||contains(mail)) score+=2; if (subseq(name)||subseq(mail)) score+=1;
            // recent conversation boost
            let recentBoost = 0;
            try{
              const existingKey = this.computeConnKey([this.currentUser.uid, u.uid||u.id]);
              const conn = this.connections.find(c=> (c.key||this.computeConnKey(c.participants||[]))===existingKey);
              if (conn && conn.updatedAt){
                const age = Date.now() - new Date(conn.updatedAt).getTime();
                recentBoost = Math.max(0, 5 - Math.log10(1+age/86400000));
              }
            }catch(_){ }
            return {u, score: score + recentBoost};
          })
          .sort((a,b)=> b.score - a.score)
          .map(x=> x.u);
          if (filtered.length > 0){
            if (userGroup) userGroup.style.display='block';
            filtered.slice(0,20).forEach(u=>{
              const li=document.createElement('li');
              li.textContent = `${u.username||u.email}`;
              li.addEventListener('click', async ()=>{
                const search = document.getElementById('user-search');
                if (this.isGroupMode){
                  // Add chip
                  const chips = document.getElementById('group-selected');
                  if (chips && !this.groupSelection.has(u.uid||u.id)){
                    this.groupSelection.set(u.uid||u.id, u);
                    const chip = document.createElement('span');
                    chip.className = 'chip';
                    chip.textContent = u.username||u.email;
                    chip.addEventListener('click', ()=>{
                      this.groupSelection.delete(u.uid||u.id);
                      chip.remove();
                    });
                    chips.appendChild(chip);
                  }
                  // stay in selection mode, keep results open and continue typing
                  if (search) { search.focus(); search.select(); }
                  return;
                }
                wrapper.style.display='none';
                if (search) search.value = '';
                const myName = (this.me && this.me.username) || (this.currentUser.email||'me');
                const uids = [this.currentUser.uid, u.uid||u.id];
                const key = this.computeConnKey(uids);
                try{
                  let connId = await this.findConnectionByKey(key);
                  if (!connId){
                    try{
                      const stableRef = firebase.doc(this.db,'chatConnections', key);
                      await firebase.setDoc(stableRef,{
                        id: key,
                        key,
                        participants: uids,
                        participantUsernames:[myName, u.username||u.email],
                        admins: [this.currentUser.uid],
                        createdAt: new Date().toISOString(),
                        updatedAt: new Date().toISOString(),
                        lastMessage:''
                      }, { merge:true });
                      connId = key;
                    }catch(errSet){
                      connId = await this.findConnectionByKey(key);
                      if (!connId) throw errSet;
                    }
                  }
                  await this.loadConnections();
                  this.setActive(connId, u.username||u.email);
                }catch(err){
                  console.error('Chat creation failed:', err);
                  const wrapper = document.getElementById('search-results');
                  if (wrapper){
                    const warn = document.createElement('div');
                    warn.style.cssText='padding:8px;color:#ff6a6a';
                    warn.textContent = 'Failed to start chat (permissions or network). Check Firestore rules: user must be a participant.';
                    wrapper.appendChild(warn);
                  }
                }
              });
              resultsEl.appendChild(li);
            });
          } else if (userGroup) { userGroup.style.display='none'; }
          // Messages search within current view
          const termLower = term.toLowerCase();
          const allMessages = Array.from(document.querySelectorAll('.messages .message'));
          const matches = allMessages.filter(m => m.textContent.toLowerCase().includes(termLower));
          if (matches.length>0){
            if (msgGroup) msgGroup.style.display='block';
            matches.slice(0,20).forEach(m=>{
              const li=document.createElement('li');
              li.textContent = m.textContent.slice(0,120);
              li.addEventListener('click', ()=>{
                wrapper.style.display='none';
                this.scrollMessageIntoViewSafely(m, { smooth: true });
              });
              msgResults.appendChild(li);
            });
          } else if (msgGroup) { msgGroup.style.display='none'; }
          const hasResults = (filtered.length>0 || matches.length>0);
          wrapper.style.display = hasResults ? 'block':'none';
          this.updateSidebarSearchState(hasResults);
          if (!hasResults && this.isMobileViewport()) this.setMobileMenuOpen(false);
        }
      }catch(e){
        console.warn('Search failed', e);
        if (wrapper) wrapper.style.display='none';
        this.updateSidebarSearchState(false);
        if (this.isMobileViewport()) this.setMobileMenuOpen(false);
      }
    }

    // Placeholders / basic signaling for calls
  async startVoiceCall(){ await this.startCall({ callId: `${this.activeConnection}_latest`, video:false }); }
  async startVideoCall(){ await this.startCall({ callId: `${this.activeConnection}_latest`, video:true }); }

  async ensureRoom(){
    if (!this.activeConnection) return null;
    const roomRef = firebase.doc(this.db,'callRooms', this.activeConnection);
    try{
      const snap = await firebase.getDoc(roomRef);
      if (!snap.exists()){
        await firebase.setDoc(roomRef, {
          id: this.activeConnection,
          status: 'idle',
          activeCallId: null,
          createdAt: new Date().toISOString(),
          lastActiveAt: new Date().toISOString()
        });
      } else {
        const data = snap.data() || {};
        const updates = { lastActiveAt: new Date().toISOString() };
        if (typeof data.status === 'undefined') updates.status = 'idle';
        if (!('activeCallId' in data)) updates.activeCallId = null;
        await firebase.updateDoc(roomRef, updates);
      }
      return roomRef;
    }catch(_){ return roomRef; }
  }

  async enterRoom(video = false){
    try {
      await navigator.mediaDevices.getUserMedia({ audio: true });
      console.log('Mic permission granted');
    } catch (err) {
      console.error('Mic permission denied', err);
      alert('Microphone access is required for voice detection in calls.');
      return;
    }
    this._videoEnabled = !!video;
    await this.ensureRoom();
    const ov = document.getElementById('call-overlay');
    if (ov) ov.classList.remove('hidden');
    const cs = document.getElementById('call-status');
    if (cs) cs.textContent = 'Room open. Click Start Call or wait for other to start.';
    this.initCallControls(video);
    if (this._roomUnsub) this._roomUnsub();
    if (this._peersUnsub) this._peersUnsub();
    const roomRef = firebase.doc(this.db,'callRooms', this.activeConnection);
    const onSnapErr = ()=>{};
    this._roomUnsub = firebase.onSnapshot(roomRef, async snap => {
      this._roomState = snap.data() || { status: 'idle', activeCallId: null };
      const activeCid = this._roomState.activeCallId;
      const startedBy = this._roomState.startedBy;
      const iAmInitiator = !!activeCid && ((startedBy === (this.currentUser?.uid)) || (activeCid === this._connectingCid));
      const alreadyConnected = !!this._activeCid && this._activeCid === activeCid && this._activePCs.size > 0;
      const canRetry = this._lastJoinedCallId === activeCid && this._activePCs.size === 0 && (Date.now() - (this._lastJoinAttemptAt || 0)) > 3000;
      if (activeCid && !iAmInitiator && !alreadyConnected && !this._joiningCall && !this._startingCall && (this._lastJoinedCallId !== activeCid || canRetry)) {
        this._lastJoinAttemptAt = Date.now();
        await this.joinMultiCall(activeCid, video);
      }
      await this.updateRoomUI();
      const status = document.getElementById('call-status');
      if (status) status.textContent = activeCid ? 'In call' : 'Room open. Click Start Call or wait for other to start.';
    }, onSnapErr);
    const peersRef = firebase.collection(this.db,'callRooms', this.activeConnection, 'peers');
    this._peersUnsub = firebase.onSnapshot(peersRef, snap => {
      this._peersPresence = {};
      snap.forEach(d => this._peersPresence[d.id] = d.data());
      this.updateRoomUI();
    }, onSnapErr);
    await this.updatePresence('idle', false);
    // Start silence monitor if no call active
    if (this._roomState.status === 'idle') {
      this._startAutoResumeMonitor(video);
    }
    this._inRoom = true;
  }

  initCallControls(video){
    const ov = document.getElementById('call-overlay');
    const startBtn = document.getElementById('start-call-btn');
    const endBtn = document.getElementById('end-call-btn');
    const micBtn = document.getElementById('toggle-mic-btn');
    const camBtn = document.getElementById('toggle-camera-btn');
    const shareBtn = document.getElementById('share-screen-btn');
    const hideBtn = document.getElementById('hide-call-btn');
    const showBtn = document.getElementById('show-call-btn');
    const exitBtn = document.getElementById('exit-room-btn');
    if (startBtn) startBtn.onclick = async () => {
      await this.attemptStartRoomCall(this._videoEnabled);
      startBtn.style.display = 'none';
    };
    if (endBtn) endBtn.onclick = async () => { 
      await this.cleanupActiveCall(true, 'end_button');
      const status = document.getElementById('call-status');
      if (status) status.textContent = 'Room open. Ready for call.';
      const sb = document.getElementById('start-call-btn');
      if (sb) sb.style.display = '';
    };
    if (exitBtn) exitBtn.onclick = async () => { 
      console.log('Exit clicked'); 
      await this.cleanupActiveCall(false, 'exit_button'); 
      const ov = document.getElementById('call-overlay');
      if (ov) ov.classList.add('hidden'); 
    };
    if (micBtn) micBtn.onclick = () => {
      this._micEnabled = !this._micEnabled;
      this._activePCs.forEach(p => { const s = p.stream; if (s) s.getAudioTracks().forEach(t => { t.enabled = this._micEnabled; }); });
      micBtn.classList.toggle('muted', !this._micEnabled);
    };
    if (camBtn) camBtn.onclick = () => {
      this._videoEnabled = !this._videoEnabled;
      this._activePCs.forEach(p => { const s = p.stream; if (s) s.getVideoTracks().forEach(t => { t.enabled = this._videoEnabled; }); });
      this.updatePresence(this._roomState ? this._roomState.status : 'idle', this._videoEnabled);
      const lv = document.getElementById('localVideo');
      if (lv) lv.style.display = this._videoEnabled ? 'block' : 'none';
      camBtn.classList.toggle('muted', !this._videoEnabled);
    };
    if (shareBtn) shareBtn.style.display = '';
    if (shareBtn) shareBtn.onclick = async () => {
      if (this._screenSharing) { await this._stopScreenShare(); return; }
      try {
        const screenStream = await navigator.mediaDevices.getDisplayMedia({ video: true });
        this._screenStream = screenStream;
        this._screenSharing = true;
        screenStream.getTracks().forEach(t => t.addEventListener('ended', () => this._stopScreenShare()));
        const screenTrack = screenStream.getVideoTracks()[0];
        if (!screenTrack) throw new Error('No video track');
        let replaced = 0;
        this._activePCs.forEach((p) => {
          const senders = p.pc.getSenders?.() || [];
          const videoSender = senders.find(s => s.track?.kind === 'video');
          if (videoSender) {
            videoSender.replaceTrack(screenTrack);
            replaced++;
          }
        });
        const lv = document.getElementById('localVideo');
        if (lv) lv.srcObject = screenStream;
        shareBtn.classList.add('active');
        if (replaced === 0) console.warn('Screen share: no video sender found (voice-only call?)');
      } catch (e) { console.warn('Screen share failed', e); }
    };
    if (hideBtn) hideBtn.onclick = () => { 
      console.log('Hide clicked'); 
      const ov = document.getElementById('call-overlay');
      if (ov) ov.classList.add('hidden'); 
      const showBtn = document.getElementById('show-call-btn');
      if (showBtn) showBtn.style.display = 'block'; 
    };
    if (showBtn) showBtn.onclick = () => { 
      console.log('Show clicked'); 
      const ov = document.getElementById('call-overlay');
      if (ov) ov.classList.remove('hidden'); 
      const showBtn = document.getElementById('show-call-btn');
      if (showBtn) showBtn.style.display = 'none'; 
    };
  }

  async attemptStartRoomCall(video){
    console.log('Attempting to start room call');
    if (this._startingCall || this._joiningCall) { console.warn('Call start suppressed (busy)'); return; }
    if (!this._roomState) {
      try {
        const snap = await firebase.getDoc(firebase.doc(this.db, 'callRooms', this.activeConnection));
        this._roomState = snap.exists() ? (snap.data() || { status: 'idle', activeCallId: null }) : { status: 'idle', activeCallId: null };
      } catch (_) {
        this._roomState = { status: 'idle', activeCallId: null };
      }
      if (!this._roomState) return;
    }
    // If room already active, join instead of trying to set active
    if (this._roomState.activeCallId) {
      this._joiningCall = true;
      try { await this.joinMultiCall(this._roomState.activeCallId, video); }
      finally { this._joiningCall = false; }
      return;
    }
    const roomRef = firebase.doc(this.db,'callRooms', this.activeConnection);
    const cid = `${this.activeConnection}_latest`;
    this._startingCall = true;
    this._connectingCid = cid;
    const success = await this.runStartTransaction(roomRef, cid).catch(err => {
      console.error('Transaction failed:', err);
      if (err.code === 'permission-denied') alert('Permission denied starting call. Check Firestore rules for /callRooms.');
      return false;
    });
    if (success) {
      console.log('Transaction success, starting multi call');
      try { await this.startMultiCall(cid, video); }
      finally { this._startingCall = false; }
      await this.saveMessage({ text: `[call:${video?'video':'voice'}:${cid}]` });
      this._monitoring = false;
    } else {
      console.log('Room already active, joining');
      if (this._roomState && this._roomState.activeCallId && this._roomState.activeCallId !== 'undefined') {
        const joinCid = this._roomState.activeCallId;
        await this.saveMessage({ text: `[call:${video?'video':'voice'}:${joinCid}]` });
        console.log('Joining call ID:', joinCid);
        this._joiningCall = true;
        try { await this.joinMultiCall(joinCid, video); }
        finally { this._joiningCall = false; }
      } else {
        console.warn('No valid active call to join');
      }
      this._startingCall = false;
    }
  }

  async runStartTransaction(roomRef, cid) {
    return firebase.runTransaction(this.db, async (tx) => {
      const snap = await tx.get(roomRef);
      const data = snap.exists() ? (snap.data() || {}) : { status: 'idle', activeCallId: null };
      if (data.status !== 'idle' || data.activeCallId) return false;
      tx.update(roomRef, {
        status: 'connecting',
        activeCallId: cid,
        startedBy: this.currentUser.uid,
        startedAt: new Date().toISOString()
      });
      return true;
    }).catch(err => {
      console.error('Transaction failed:', err);
      return false;
    });
  }

  async startMultiCall(callId, video = false){
    const statusEl = document.getElementById('call-status');
    if (statusEl) statusEl.textContent = 'Starting call...';
    if (this._activePCs && this._activePCs.size > 0) {
      await this.cleanupActiveCall(false, 'start_multi_preclean');
    }
    const connSnap = await firebase.getDoc(firebase.doc(this.db,'chatConnections', this.activeConnection));
    const conn = connSnap.data() || {};
    const participants = (conn.participants||[]).filter(Boolean).filter(uid => uid !== this.currentUser.uid);
    // Temporary: limit to first remote only for stability
    const limited = participants.slice(0,1);
    if (participants.length + 1 > 8) {
      alert('Max 8 users per room');
      return;
    }
    await this.updatePresence('connecting', video);
    this._activePCs = new Map();
    const config = { audio: true, video: !!video };
    let stream;
    try {
      stream = await navigator.mediaDevices.getUserMedia(config);
    } catch (err) {
      console.error('getUserMedia failed:', err);
      const se = document.getElementById('call-status');
      if (se) se.textContent = 'Microphone/camera access denied.';
      return;
    }
    stream.getVideoTracks().forEach(t => t.enabled = !!video);
    let videosCont = document.getElementById('call-videos');
    if (!videosCont) {
      videosCont = document.createElement('div');
      videosCont.id = 'call-videos';
      videosCont.style.cssText = 'display: flex; flex-wrap: wrap; gap: 10px; justify-content: center;';
      const overlay = document.getElementById('call-overlay');
      if (overlay) overlay.appendChild(videosCont);
    }
    let lv = document.getElementById('localVideo');
    if (!lv) {
      lv = document.createElement('video');
      lv.id = 'localVideo';
      lv.autoplay = true;
      lv.playsInline = true;
      lv.muted = true;
      lv.style.display = 'none';
      videosCont.appendChild(lv);
    }
    lv.srcObject = stream;
    lv.style.display = (video && stream.getVideoTracks().some(t=>t.enabled)) ? 'block' : 'none';
    for (const peerUid of limited){
      const pc = new RTCPeerConnection({
        iceServers: await this.getIceServers(),
        iceTransportPolicy: this._forceRelay ? 'relay' : 'all'
      });
      pc.oniceconnectionstatechange = () => {
        console.log('ICE state for ' + peerUid + ':', pc.iceConnectionState);
        const st = pc.iceConnectionState;
        if (st === 'disconnected' || st === 'failed'){
          try { pc.restartIce && pc.restartIce(); } catch(_){ }
        }
      };
      pc.onconnectionstatechange = () => {
        console.log('PC state for ' + peerUid + ':', pc.connectionState);
        if (pc.connectionState === 'connected'){
          const cs = document.getElementById('call-status'); if (cs) cs.textContent = 'In call';
          this._activeCid = callId;
          const key = callId+':'+peerUid; const w=this._pcWatchdogs.get(key); if (w){ clearTimeout(w.t1); clearTimeout(w.t2); this._pcWatchdogs.delete(key); }
        }
      };
      // ICE watchdogs
      const wdKey = callId+':'+peerUid;
      try { const old = this._pcWatchdogs.get(wdKey); if (old){ clearTimeout(old.t1); clearTimeout(old.t2); } } catch(_){ }
      const t1 = setTimeout(()=>{ try{ if (pc.connectionState!=='connected' && pc.connectionState!=='completed'){ console.log('ICE watchdog: restartIce for '+peerUid); pc.restartIce && pc.restartIce(); } }catch(e){ console.warn('watchdog restartIce error', e?.message||e); } }, 15000);
      const t2 = setTimeout(async()=>{ try{ if (pc.connectionState!=='connected' && pc.connectionState!=='completed'){ console.log('ICE watchdog: renegotiate for '+peerUid); const offer = await pc.createOffer({ iceRestart:true }); await pc.setLocalDescription(offer); const offersRef = firebase.collection(this.db,'calls',callId,'offers'); await firebase.setDoc(firebase.doc(offersRef, peerUid), { sdp: offer.sdp, type: offer.type, createdAt: new Date().toISOString(), connId: this.activeConnection, fromUid: this.currentUser.uid, toUid: peerUid }); } }catch(e){ console.warn('watchdog renegotiate error', e?.message||e); } }, 25000);
      this._pcWatchdogs.set(wdKey, { t1, t2 });
      // Prepare transceivers first to lock m-line order
      const txAudio = pc.addTransceiver('audio', { direction: 'sendrecv' });
      const txVideo = pc.addTransceiver('video', { direction: video ? 'sendrecv' : 'recvonly' });
      // Attach local tracks via replaceTrack when possible
      try {
        const a = stream.getAudioTracks()[0];
        if (a && txAudio && txAudio.sender) { try { await txAudio.sender.replaceTrack(a); } catch(_) {} }
      } catch(_) {}
      try {
        const v = stream.getVideoTracks()[0];
        if (v && txVideo && txVideo.sender && video) { try { await txVideo.sender.replaceTrack(v); } catch(_) {} }
      } catch(_) {}
      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      const offersRef = firebase.collection(this.db,'calls',callId,'offers');
      await firebase.setDoc(firebase.doc(offersRef, peerUid), { sdp: offer.sdp, type: offer.type, createdAt: new Date().toISOString(), connId: this.activeConnection, fromUid: this.currentUser.uid, toUid: peerUid });
      // Mark room active once first offer is published
      try { await firebase.updateDoc(firebase.doc(this.db,'callRooms', this.activeConnection), { status: 'connecting', activeCallId: callId, lastActiveAt: new Date().toISOString() }); } catch(_){ }
      let rv = document.getElementById(`remoteVideo-${peerUid}`);
      if (!rv) {
        rv = document.createElement('video');
        rv.id = `remoteVideo-${peerUid}`;
        rv.autoplay = true;
        rv.playsInline = true;
        rv.style.display = 'none';
        videosCont.appendChild(rv);
      }
      pc.ontrack = e => {
        console.log('Received remote track for ' + peerUid, e.track.kind);
        const stream = e.streams[0];
        if (!stream) return;
        const hasVid = stream.getVideoTracks && stream.getVideoTracks().some(t => t.enabled);
        // Always ensure an audio sink exists (hidden) regardless of video
        let audEl = document.getElementById(`remoteAudio-${peerUid}`);
        if (!audEl){ audEl = document.createElement('audio'); audEl.id = `remoteAudio-${peerUid}`; audEl.autoplay = true; audEl.playsInline = true; audEl.style.display = 'none'; document.body.appendChild(audEl); }
        audEl.srcObject = stream; audEl.muted = false; audEl.volume = 1;
        // Video element visible only if video tracks exist
        let mediaEl = rv;
        if (!hasVid){ mediaEl = audEl; }
        mediaEl.srcObject = stream;
        mediaEl.muted = false;
        mediaEl.volume = 1;
        mediaEl.addEventListener('loadedmetadata', () => {
          try { mediaEl.play().catch(err => console.error('Play failed:', err)); }
          catch (err) { console.error('Play error:', err); }
        });
        mediaEl.style.display = hasVid ? 'block' : 'none';
        this._attachSpeakingDetector(stream, `[data-uid="${peerUid}"]`, peerUid);
      };
      const candsRef = firebase.collection(this.db,'calls',callId,'candidates');
      const localCandidateQueue = [];
      const remoteCandidateQueue = [];
      pc.onicecandidate = e => {
        if (e.candidate) {
          if (pc.remoteDescription) {
            firebase.setDoc(firebase.doc(candsRef), { type: 'offer', fromUid: this.currentUser.uid, toUid: peerUid, connId: this.activeConnection, candidate: e.candidate.toJSON() });
          } else {
            localCandidateQueue.push(e.candidate);
          }
        }
      };
      const unsubs = [];
      unsubs.push(firebase.onSnapshot(firebase.doc(this.db,'calls',callId,'answers', peerUid), async doc => {
        if (!doc.exists()) return;
        const data = doc.data();
        // Normalize to RTCSessionDescriptionInit
        const desc = { type: data.type || 'answer', sdp: data.sdp };
        // Only set when we have a local offer
        if (pc.signalingState !== 'have-local-offer') { return; }
        console.log('Setting remote description for ' + peerUid);
        try {
          await pc.setRemoteDescription(desc);
          // Flush queued LOCAL ICE now that remoteDescription is set (send to peer)
          while (localCandidateQueue.length) {
            const cand = localCandidateQueue.shift();
            try { await firebase.setDoc(firebase.doc(candsRef), { type: 'offer', fromUid: this.currentUser.uid, toUid: peerUid, connId: this.activeConnection, candidate: cand.toJSON() }); } catch (_) {}
          }
          try { await firebase.updateDoc(firebase.doc(this.db,'callRooms', this.activeConnection), { status: 'active', lastActiveAt: new Date().toISOString() }); } catch(_){ }
        } catch (err) {
          console.error('setRemote failed for ' + peerUid, err);
        }
      }));
      const seenCands = new Set();
      unsubs.push(firebase.onSnapshot(candsRef, snap => {
        snap.forEach(d => {
          const v = d.data();
          if (v.type !== 'answer' || v.fromUid !== peerUid || v.toUid !== this.currentUser.uid || !v.candidate) return;
          const key = JSON.stringify(v.candidate);
          if (seenCands.has(key)) return;
          seenCands.add(key);
          if (!pc.remoteDescription) { remoteCandidateQueue.push(v.candidate); return; }
          if (pc.signalingState === 'closed') return;
          pc.addIceCandidate(new RTCIceCandidate(v.candidate)).catch(()=>{});
        });
      }));
      this._activePCs.set(peerUid, {pc, unsubs, stream, videoEl: rv});
    }
    await this.updatePresence('connected', video);
    this._setupRoomInactivityMonitor();
    this._attachSpeakingDetector(stream, '[data-uid="' + this.currentUser.uid + '"]', this.currentUser.uid);
    const sb = document.getElementById('start-call-btn');
    if (sb) sb.style.display = 'none';
  }

  async joinMultiCall(callId, video = false){
    if (this._joiningCall) return;
    if (this._lastJoinedCallId === callId && this._activePCs.size > 0) return;
    const statusEl = document.getElementById('call-status');
    if (statusEl) statusEl.textContent = 'Connecting...';
    this._lastJoinedCallId = callId;
    this._joiningCall = true;
    try{
      if (this._activePCs && this._activePCs.size > 0) {
        await this.cleanupActiveCall(false, 'join_multi_preclean');
      }
      await this.updatePresence('connecting', video);
    this._activePCs = new Map();
    // Prepare local media
    const localCfg = { audio: true, video: !!video };
    let localStream;
    try {
      localStream = await navigator.mediaDevices.getUserMedia(localCfg);
    } catch (err) {
      console.error('getUserMedia failed:', err);
      const se = document.getElementById('call-status');
      if (se) se.textContent = 'Microphone/camera access denied.';
      return;
    }
    localStream.getVideoTracks().forEach(t => t.enabled = !!video);
    // UI containers
    let videosCont = document.getElementById('call-videos');
    if (!videosCont) {
      videosCont = document.createElement('div');
      videosCont.id = 'call-videos';
      videosCont.style.cssText = 'display: flex; flex-wrap: wrap; gap: 10px; justify-content: center;';
      const overlay = document.getElementById('call-overlay');
      if (overlay) overlay.appendChild(videosCont);
    }
    let lv = document.getElementById('localVideo');
    if (!lv) {
      lv = document.createElement('video');
      lv.id = 'localVideo';
      lv.autoplay = true;
      lv.playsInline = true;
      lv.muted = true;
      lv.style.display = 'none';
      videosCont.appendChild(lv);
    }
    lv.srcObject = localStream;
    lv.style.display = (video && localStream.getVideoTracks().some(t=>t.enabled)) ? 'block' : 'none';

    // Find the offer addressed to me to identify the initiator
    // Find the newest offer addressed to me; if none, poll briefly waiting for initiator
    let offerDoc = await firebase.getDoc(firebase.doc(this.db,'calls',callId,'offers', this.currentUser.uid));
    if (!offerDoc.exists()){
      for (let i=0;i<6;i++){
        await new Promise(r=>setTimeout(r,250));
        offerDoc = await firebase.getDoc(firebase.doc(this.db,'calls',callId,'offers', this.currentUser.uid));
        if (offerDoc.exists()) break;
      }
      if (!offerDoc.exists()){
        console.warn('No offer for current user found; room may not be active yet');
        const se = document.getElementById('call-status');
        if (se) se.textContent = 'Room open. Waiting for offer...';
        return;
      }
    }
    const offer = offerDoc.data();
    const peerUid = offer.fromUid;

    const pc = new RTCPeerConnection({
      iceServers: await this.getIceServers(),
      iceTransportPolicy: this._forceRelay ? 'relay' : 'all'
    });
    pc.oniceconnectionstatechange = () => {
      console.log('ICE state for ' + peerUid + ':', pc.iceConnectionState);
      const st = pc.iceConnectionState;
      if (st === 'disconnected' || st === 'failed'){
        try { pc.restartIce && pc.restartIce(); } catch(_){ }
      }
    };
    pc.onconnectionstatechange = () => {
      console.log('PC state for ' + peerUid + ':', pc.connectionState);
      if (pc.connectionState === 'connected' || pc.connectionState === 'completed'){
        const key = callId+':'+peerUid; const w=this._pcWatchdogs.get(key); if (w){ clearTimeout(w.t1); clearTimeout(w.t2); this._pcWatchdogs.delete(key); }
        const cs = document.getElementById('call-status'); if (cs) cs.textContent = 'In call';
      }
    };

    // Remote media element
    let rv = document.getElementById(`remoteVideo-${peerUid}`);
    if (!rv) {
      rv = document.createElement('video');
      rv.id = `remoteVideo-${peerUid}`;
      rv.autoplay = true;
      rv.playsInline = true;
      rv.style.display = 'none';
      videosCont.appendChild(rv);
    }
    pc.ontrack = e => {
      console.log('Received remote track for ' + peerUid, e.track.kind);
      const rstream = e.streams[0];
      if (!rstream) return;
      const hasVid = rstream.getVideoTracks && rstream.getVideoTracks().some(t => t.enabled);
      // Always ensure an audio sink exists
      let audEl = document.getElementById(`remoteAudio-${peerUid}`);
      if (!audEl){ audEl = document.createElement('audio'); audEl.id = `remoteAudio-${peerUid}`; audEl.autoplay = true; audEl.playsInline = true; audEl.style.display = 'none'; document.body.appendChild(audEl); }
      audEl.srcObject = rstream; audEl.muted = false; audEl.volume = 1;
      let mediaEl = rv; if (!hasVid) mediaEl = audEl;
      mediaEl.srcObject = rstream;
      mediaEl.muted = false;
      mediaEl.volume = 1;
      mediaEl.addEventListener('loadedmetadata', () => {
        try { mediaEl.play().catch(err => console.error('Play failed:', err)); }
        catch (err) { console.error('Play error:', err); }
      });
      mediaEl.style.display = hasVid ? 'block' : 'none';
      this._attachSpeakingDetector(rstream, `[data-uid="${peerUid}"]`, peerUid);
    };

    const answersRef = firebase.collection(this.db,'calls',callId,'answers');
    const candsRef = firebase.collection(this.db,'calls',callId,'candidates');
    // Ensure we only process candidates for this peer
    const myUid = this.currentUser.uid;
    const remoteCandidateQueue = [];
    const candidateQueue = [];
    pc.onicecandidate = e => {
      if (e.candidate) {
        if (pc.remoteDescription) {
          firebase.setDoc(firebase.doc(candsRef), { type: 'answer', fromUid: this.currentUser.uid, toUid: peerUid, connId: this.activeConnection, candidate: e.candidate.toJSON() });
        } else {
          candidateQueue.push(e.candidate);
        }
      }
    };

    await pc.setRemoteDescription({ type:'offer', sdp: offer.sdp });
    // Align transceivers to the remote offer and attach local tracks without changing m-line order
    const trxs = pc.getTransceivers ? pc.getTransceivers() : [];
    trxs.forEach(tx => {
      if (!tx || !tx.receiver || !tx.receiver.track) return;
      const kind = tx.receiver.track.kind;
      try {
        if (kind === 'audio') tx.direction = 'sendrecv';
        if (kind === 'video') tx.direction = (video ? 'sendrecv' : 'recvonly');
      } catch(_) {}
      const local = localStream.getTracks().find(t => t.kind === kind);
      if (local && tx.sender && typeof tx.sender.replaceTrack === 'function') {
        try { tx.sender.replaceTrack(local); } catch(_) {}
      }
    });
    // Fallback: attach any local track that has no sender yet
    localStream.getTracks().forEach(tr => {
      const hasSender = trxs.some(tx => tx && tx.sender && tx.sender.track && tx.sender.track.kind === tr.kind);
      if (!hasSender) {
        if (tr.kind === 'video' && !video) return;
        try { pc.addTrack(tr, localStream); } catch(_) {}
      }
    });
      const answer = await pc.createAnswer();
    await pc.setLocalDescription(answer);
      await firebase.setDoc(firebase.doc(answersRef, peerUid), { sdp: answer.sdp, type: answer.type, createdAt: new Date().toISOString(), connId: this.activeConnection, fromUid: this.currentUser.uid, toUid: peerUid });
      // Flush any queued ICE now that descriptions are set
      while (candidateQueue.length) { const c = candidateQueue.shift(); try { await pc.addIceCandidate(c); } catch(_){} }
      while (remoteCandidateQueue.length) { const rc = remoteCandidateQueue.shift(); try { await pc.addIceCandidate(new RTCIceCandidate(rc)); } catch(_){} }
      try { await firebase.updateDoc(firebase.doc(this.db,'callRooms', this.activeConnection), { status: 'active', activeCallId: callId, lastActiveAt: new Date().toISOString() }); this._activeCid = callId; } catch(_){ }
      candidateQueue.forEach(async cand => await pc.addIceCandidate(cand));
    candidateQueue.length = 0;
    const unsubs = [];
    const seenCands = new Set();
    unsubs.push(firebase.onSnapshot(candsRef, snap => {
      snap.forEach(d => {
        const v = d.data();
        if (v.type !== 'offer' || v.fromUid !== peerUid || v.toUid !== myUid || !v.candidate) return;
        const key = JSON.stringify(v.candidate);
        if (seenCands.has(key)) return;
        seenCands.add(key);
        if (!pc.remoteDescription) { remoteCandidateQueue.push(v.candidate); return; }
        if (pc.signalingState === 'closed') return;
        pc.addIceCandidate(new RTCIceCandidate(v.candidate)).catch(()=>{});
      });
    }));
    // Listen for future offers (ICE restarts/renegotiations) from initiator
    let lastOfferSdp = offer.sdp;
    const myOfferRef = firebase.doc(this.db,'calls',callId,'offers', this.currentUser.uid);
    const uOffers = firebase.onSnapshot(myOfferRef, async d => {
      try{
        if (!d.exists()) return;
        const data = d.data() || {};
        const sdp = data.sdp || '';
        if (!sdp || sdp === lastOfferSdp) return;
        lastOfferSdp = sdp;
        if (pc.signalingState === 'closed') return;
        // Apply new remote offer and answer back
        await pc.setRemoteDescription({ type:'offer', sdp: sdp });
        const ans2 = await pc.createAnswer();
        await pc.setLocalDescription(ans2);
        await firebase.setDoc(firebase.doc(answersRef, peerUid), { sdp: ans2.sdp, type: ans2.type, createdAt: new Date().toISOString(), connId: this.activeConnection, fromUid: this.currentUser.uid, toUid: peerUid });
      }catch(e){ console.warn('offer update handling failed', e?.message||e); }
    });
    unsubs.push(uOffers);

    this._activePCs.set(peerUid, {pc, unsubs, stream: localStream, videoEl: rv});

    // Add watchdogs on joiner too
    const wdKey = callId+':'+peerUid;
    try { const old = this._pcWatchdogs.get(wdKey); if (old){ clearTimeout(old.t1); clearTimeout(old.t2); } } catch(_){ }
    const t1 = setTimeout(()=>{ try{ if (pc.connectionState!=='connected' && pc.connectionState!=='completed'){ console.log('ICE watchdog (join): restartIce for '+peerUid); pc.restartIce && pc.restartIce(); } }catch(e){ console.warn('watchdog restartIce error', e?.message||e); } }, 15000);
    const t2 = setTimeout(async()=>{ try{ if (pc.connectionState!=='connected' && pc.connectionState!=='completed'){ console.log('ICE watchdog (join): resend answer for '+peerUid); if (pc.signalingState === 'have-remote-offer'){ const ans = await pc.createAnswer({}); await pc.setLocalDescription(ans); await firebase.setDoc(firebase.doc(answersRef, peerUid), { sdp: ans.sdp, type: ans.type, createdAt: new Date().toISOString(), connId: this.activeConnection, fromUid: this.currentUser.uid, toUid: peerUid }); } else { console.log('Skip resend answer, signalingState=', pc.signalingState); } } }catch(e){ console.warn('watchdog resend answer error', e?.message||e); } }, 25000);
    this._pcWatchdogs.set(wdKey, { t1, t2 });

      await this.updatePresence('connected', video);
      this._setupRoomInactivityMonitor();
      this._attachSpeakingDetector(localStream, '[data-uid="' + this.currentUser.uid + '"]', this.currentUser.uid);
      const sb = document.getElementById('start-call-btn');
      if (sb) sb.style.display = 'none';
    } finally {
      this._joiningCall = false;
    }
  }

  async _setupRoomInactivityMonitor(){
    this._lastSpeech.clear();
    if (this._inactTimer) clearInterval(this._inactTimer);
    // Temporarily disable auto-silence end during debug to avoid premature cleanup
    /* this._inactTimer = setInterval(()=>{
      const now = Date.now();
      let maxLast = 0;
      this._lastSpeech.forEach(ts => maxLast = Math.max(maxLast, ts));
      if (now - maxLast > 5 * 60 * 1000){
        clearInterval(this._inactTimer);
        this.cleanupActiveCall(false, 'silence_timer');
        const roomRef = firebase.doc(this.db,'callRooms', this.activeConnection);
        firebase.updateDoc(roomRef, { status: 'idle', activeCallId: null });
        this.saveMessage({ text: '[system] Call ended due to silence' });
        this._startAutoResumeMonitor(false);
      }
    }, 15000); */
  }

  // Add to _attachSpeakingDetector to take uid and update this._lastSpeech.set(uid, now) when avg > 30

  // In camBtn onclick for local: toggle enabled and updatePresence with hasVideo = enabled

  // For remote video toggle: on ontrack check hasVid and toggle display

  // In endBtn onclick: this.cleanupActiveCall(true) to end room for all

  // Limit: in enterRoom check conn.participants.length > 8 and alert/return

  async joinOrStartCall({ video }){
    // If latest call offer exists in this room, join; otherwise create a new one
    const roomId = this.activeConnection; if (!roomId) return;
    try{
      const cid = `${roomId}_latest`;
      const offersRef = firebase.collection(this.db,'calls',cid,'offers');
      const docSnap = await firebase.getDoc(firebase.doc(offersRef,'offer'));
      if (docSnap.exists()){
        await this.answerCall(cid, { video });
      } else {
        await this.startCall({ callId: cid, video });
      }
    }catch(_){ const cid = `${roomId}_latest`; await this.startCall({ callId: cid, video }); }
  }

  async startCall({ callId, video }){
      try{
        // ensure previous listeners/pcs are cleaned up when reconnecting
        try{ this._activeCall && this._activeCall.unsubs && this._activeCall.unsubs.forEach(u=>{ try{u&&u();}catch(_){}}); }catch(_){ }
        try{ this._activeCall && this._activeCall.pc && this._activeCall.pc.close(); }catch(_){ }
        callId = callId || `${this.activeConnection}_${Date.now()}`;
        const config = { audio: true, video: !!video };
        const stream = await navigator.mediaDevices.getUserMedia(config);
        const pc = new RTCPeerConnection({
          iceServers: await this.getIceServers(),
          iceTransportPolicy: this._forceRelay ? 'relay' : 'all'
        });
        window._pc = pc; // debug handle
        pc.oniceconnectionstatechange = ()=>{
          console.log('ICE state:', pc.iceConnectionState);
          if (pc.iceConnectionState === 'failed' || pc.iceConnectionState === 'disconnected'){
            try { pc.restartIce && pc.restartIce(); } catch(_){ }
          }
        };
        pc.onconnectionstatechange = ()=>{
          console.log('PC state:', pc.connectionState);
          if (pc.connectionState === 'failed' && this._forceRelay){
            this._forceRelay = false;
          }
        };
        // Add local tracks safely (avoid duplicate senders)
        try{
          const existing = pc.getSenders ? pc.getSenders() : [];
          stream.getTracks().forEach(tr => {
            const sender = existing && existing.find(s => s.track && s.track.kind === tr.kind);
            if (sender){ try{ sender.replaceTrack(tr); }catch(_){} }
            else { pc.addTrack(tr, stream); }
          });
        }catch(_){ }
        // tracks already added or replaced above
        const lv = document.getElementById('localVideo'); const rv = document.getElementById('remoteVideo'); const ov = document.getElementById('call-overlay');
        if (lv){ lv.srcObject = stream; try{ lv.muted = true; lv.playsInline = true; lv.play().catch(()=>{}); }catch(_){} lv.style.display = (video && stream.getVideoTracks().some(t=>t.enabled))? 'block':'none'; }
        pc.ontrack = (e)=>{
          if (rv){
            rv.srcObject = e.streams[0];
            try{ rv.playsInline = true; rv.muted = false; rv.play().catch(()=>{}); }catch(_){ }
            try{ const hasVid = e.streams[0].getVideoTracks().some(t=> t.enabled); rv.style.display = hasVid? 'block':'none'; }catch(_){ }
          }
          // For audio-only calls ensure an audio sink exists
          if (!video){
            let aud = document.getElementById('remoteAudio');
            if (!aud){ aud = document.createElement('audio'); aud.id='remoteAudio'; aud.autoplay = true; aud.style.display='none'; document.body.appendChild(aud); }
            aud.srcObject = e.streams[0];
          }
          this._attachSpeakingDetector(e.streams[0], '.call-participants .avatar.remote');
        };
        if (ov){ ov.classList.remove('hidden'); const cs = document.getElementById('call-status'); if (cs) cs.textContent = 'Connecting...'; }
        try { await this._renderParticipants(); this._attachSpeakingDetector(stream, '.call-participants .avatar.local'); } catch(_){ }
        const offersRef = firebase.collection(this.db,'calls',callId,'offers');
        const candsRef = firebase.collection(this.db,'calls',callId,'candidates');
        pc.onicecandidate = (e)=>{ if(e.candidate){ firebase.setDoc(firebase.doc(candsRef), { type:'offer', connId: this.activeConnection, candidate:e.candidate.toJSON() }); }};
        // Avoid duplicate starts: if an offer already exists, switch to join flow
        const existingOffer = await firebase.getDoc(firebase.doc(offersRef,'offer'));
        if (existingOffer.exists()){
          try{ stream.getTracks().forEach(t=> t.stop()); }catch(_){ }
          return await this.answerCall(callId, { video });
        }
        const offer = await pc.createOffer(); await pc.setLocalDescription(offer);
        await firebase.setDoc(firebase.doc(offersRef,'offer'), { sdp: offer.sdp, type: offer.type, createdAt: new Date().toISOString(), connId: this.activeConnection });
        // publish one join message for room-wide latest call id
        await this.saveMessage({ text:`[call:${video?'video':'voice'}:${callId}]` });
        // Listen for answer
        if (firebase.onSnapshot){
          const unsubs=[];
          const u1 = firebase.onSnapshot(firebase.doc(this.db,'calls',callId,'answers','answer'), async (doc)=>{
            if (doc.exists()){
              const data = doc.data();
              await pc.setRemoteDescription(new RTCSessionDescription({ type:'answer', sdp:data.sdp }));
            }
          });
          unsubs.push(u1);
          const u2 = firebase.onSnapshot(candsRef, (snap)=>{
            snap.forEach(d=>{
              const v=d.data();
              if(v.type==='answer' && v.candidate){
                try{
                  if (pc.signalingState==='closed') return;
                  if (!pc.remoteDescription) return;
                  pc.addIceCandidate(new RTCIceCandidate(v.candidate)).catch(()=>{});
                }catch(_){ }
              }
            });
          });
          unsubs.push(u2);
          this._activeCall = { pc, unsubs };
        }
        const endBtn = document.getElementById('end-call-btn'); if (endBtn) endBtn.textContent = 'Exit';
        const micBtn = document.getElementById('toggle-mic-btn');
        const camBtn = document.getElementById('toggle-camera-btn');
        const hideBtn = document.getElementById('hide-call-btn');
        const showBtn = document.getElementById('show-call-btn');
        if (endBtn){ endBtn.onclick = ()=>{ try{ this._activeCall && this._activeCall.unsubs && this._activeCall.unsubs.forEach(u=>{ try{u&&u();}catch(_){}}); }catch(_){ } try{ pc.close(); }catch(_){} stream.getTracks().forEach(t=> t.stop()); if (ov) ov.classList.add('hidden'); if (showBtn) showBtn.style.display='none'; }; }
        if (micBtn){ micBtn.onclick = ()=>{ stream.getAudioTracks().forEach(t=> t.enabled = !t.enabled); }; }
        if (camBtn){ camBtn.onclick = ()=>{ const enabled = stream.getVideoTracks().some(t=> t.enabled); stream.getVideoTracks().forEach(t=> t.enabled = !enabled); if (lv) lv.style.display = stream.getVideoTracks().some(t=>t.enabled)? 'block':'none'; }; }
        if (hideBtn){ hideBtn.onclick = ()=>{ if (ov) ov.classList.add('hidden'); if (showBtn) showBtn.style.display='block'; }; }
        if (showBtn){ showBtn.onclick = ()=>{ if (ov) ov.classList.remove('hidden'); showBtn.style.display='none'; }; }
      }catch(e){ console.warn('Call start failed', e); }
    }

    async answerCall(callId, { video }){
      try{
        const config = { audio:true, video: !!video };
        const stream = await navigator.mediaDevices.getUserMedia(config);
        const pc = new RTCPeerConnection({
          iceServers: await this.getIceServers(),
          iceTransportPolicy: this._forceRelay ? 'relay' : 'all'
        });
        window._pc = pc; // debug handle
        pc.oniceconnectionstatechange = ()=>{
          console.log('ICE state:', pc.iceConnectionState);
          if (pc.iceConnectionState === 'failed' || pc.iceConnectionState === 'disconnected'){
            try { pc.restartIce && pc.restartIce(); } catch(_){ }
          }
        };
        pc.onconnectionstatechange = ()=>{
          console.log('PC state:', pc.connectionState);
          if (pc.connectionState === 'failed' && this._forceRelay){
            this._forceRelay = false;
          }
        };
        try{
          const existing = pc.getSenders ? pc.getSenders() : [];
          stream.getTracks().forEach(tr => {
            const sender = existing && existing.find(s => s.track && s.track.kind === tr.kind);
            if (sender){ try{ sender.replaceTrack(tr); }catch(_){} }
            else { pc.addTrack(tr, stream); }
          });
        }catch(_){ }
        // tracks already added or replaced above
        const lv = document.getElementById('localVideo'); const rv = document.getElementById('remoteVideo'); const ov = document.getElementById('call-overlay');
        if (lv){ lv.srcObject = stream; try{ lv.muted = true; lv.playsInline = true; lv.play().catch(()=>{}); }catch(_){} }
        pc.ontrack = (e)=>{
          if (rv){
            rv.srcObject = e.streams[0];
            try{ rv.playsInline = true; rv.muted = false; rv.play().catch(()=>{}); }catch(_){ }
          }
          if (!video){
            let aud = document.getElementById('remoteAudio');
            if (!aud){ aud = document.createElement('audio'); aud.id='remoteAudio'; aud.autoplay = true; aud.style.display='none'; document.body.appendChild(aud); }
            aud.srcObject = e.streams[0];
          }
          this._attachSpeakingDetector(e.streams[0], '.call-participants .avatar.remote');
        };
        if (ov){ ov.classList.remove('hidden'); const cs = document.getElementById('call-status'); if (cs) cs.textContent = 'Connecting...'; }
        try { await this._renderParticipants(); this._attachSpeakingDetector(stream, '.call-participants .avatar.local'); } catch(_){ }
        const answersRef = firebase.collection(this.db,'calls',callId,'answers');
        const candsRef = firebase.collection(this.db,'calls',callId,'candidates');
        pc.onicecandidate = (e)=>{ if(e.candidate){ firebase.setDoc(firebase.doc(candsRef), { type:'answer', connId: this.activeConnection, candidate:e.candidate.toJSON() }); }};
        const offerDoc = await firebase.getDoc(firebase.doc(this.db,'calls',callId,'offers','offer'));
        if (!offerDoc.exists()) return;
        const offer = offerDoc.data();
        await pc.setRemoteDescription(new RTCSessionDescription({ type:'offer', sdp: offer.sdp }));
        pc.addTransceiver('audio', { direction: 'sendrecv' });
        pc.addTransceiver('video', { direction: video ? 'sendrecv' : 'recvonly' });
        const answer = await pc.createAnswer();
        await pc.setLocalDescription(answer);
        await firebase.setDoc(firebase.doc(answersRef,'answer'), { sdp: answer.sdp, type: answer.type, createdAt: new Date().toISOString(), connId: this.activeConnection });
        // Listen for caller ICE candidates (type 'offer') and add to this peer connection
        if (firebase.onSnapshot){
          const added = new Set();
          firebase.onSnapshot(candsRef, (snap)=>{
            snap.forEach(d=>{
              const v=d.data();
              const key=v && v.candidate && (v.candidate.sdpMid+':'+v.candidate.sdpMLineIndex+':'+v.candidate.candidate);
              if (v.type==='offer' && v.candidate && !added.has(key)){
                added.add(key);
                if (pc.signalingState==='closed' || !pc.remoteDescription) return;
                pc.addIceCandidate(new RTCIceCandidate(v.candidate)).catch(()=>{});
              }
            });
          });
        }
        const endBtn = document.getElementById('end-call-btn');
        const micBtn = document.getElementById('toggle-mic-btn');
        const camBtn = document.getElementById('toggle-camera-btn');
        const hideBtn = document.getElementById('hide-call-btn');
        const showBtn = document.getElementById('show-call-btn');
        if (endBtn){ endBtn.onclick = ()=>{ try{ pc.close(); }catch(_){} stream.getTracks().forEach(t=> t.stop()); if (ov) ov.classList.add('hidden'); if (showBtn) showBtn.style.display='none'; }; }
        if (micBtn){ micBtn.onclick = ()=>{ stream.getAudioTracks().forEach(t=> t.enabled = !t.enabled); }; }
        if (camBtn){ camBtn.onclick = ()=>{ stream.getVideoTracks().forEach(t=> t.enabled = !t.enabled); }; }
        if (hideBtn){ hideBtn.onclick = ()=>{ if (ov) ov.classList.add('hidden'); if (showBtn) showBtn.style.display='block'; }; }
        if (showBtn){ showBtn.onclick = ()=>{ if (ov) ov.classList.remove('hidden'); showBtn.style.display='none'; }; }
      }catch(e){ console.warn('Answer call failed', e); }
    }

    async _renderParticipants(){
      try{
        const cont = document.getElementById('call-participants'); if (!cont) return;
        cont.innerHTML='';
        const selfAvatar = (this.me && this.me.avatarUrl) || '../../images/default-bird.png';
        const local = document.createElement('div'); local.className = 'avatar local connected'; local.innerHTML = `<img src="${selfAvatar}" alt="me"/>`;
        cont.appendChild(local);
        // Peer
        const peerUid = await this.getPeerUid();
        let peerAvatar = '../../images/default-bird.png';
        try{ const d = await window.firebaseService.getUserData(peerUid); if (d && d.avatarUrl) peerAvatar = d.avatarUrl; }catch(_){ }
        const remote = document.createElement('div'); remote.className = 'avatar remote connected'; remote.innerHTML = `<img src="${peerAvatar}" alt="peer"/>`;
        cont.appendChild(remote);
      }catch(_){ }
    }

    _attachSpeakingDetector(stream, selector, uid){
      try{
        const el = document.querySelector(selector); if (!el) return;
        const ac = new (window.AudioContext || window.webkitAudioContext)();
        const src = ac.createMediaStreamSource(stream);
        const analyser = ac.createAnalyser(); analyser.fftSize = 512;
        src.connect(analyser);
        const data = new Uint8Array(analyser.frequencyBinCount);
        let rafId = 0;
        const tick = ()=>{
          analyser.getByteFrequencyData(data);
          let sum = 0; for (let i=0;i<data.length;i++) sum += data[i];
          const avg = sum / data.length;
          if (avg > 30){ el.classList.add('speaking'); if (uid) this._lastSpeech.set(uid, Date.now()); } else { el.classList.remove('speaking'); }
          // Update last speech timestamps for inactivity monitor
          if (avg > 30){
            const now = Date.now();
            if (selector && selector.includes('local')){ this._lastLocalSpeechTs = now; }
            if (selector && selector.includes('remote')){ this._lastRemoteSpeechTs = now; }
          }
          rafId = requestAnimationFrame(tick);
        };
        tick();
        // Stop when stream ends
        const stop = ()=>{ try{ cancelAnimationFrame(rafId); }catch(_){} try{ ac.close(); }catch(_){} };
        stream.getTracks().forEach(t=> t.addEventListener('ended', stop));
      }catch(_){ }
    }

    _setupInactivityMonitor(pc, localStream, opts){
      try{
        this._lastLocalSpeechTs = Date.now();
        this._lastRemoteSpeechTs = Date.now();
        // initial detector values are set by _attachSpeakingDetector callbacks
        if (this._inactTimer) clearInterval(this._inactTimer);
        const limitMs = 5 * 60 * 1000; // 5 minutes
        this._inactTimer = setInterval(()=>{
          const last = Math.max(this._lastLocalSpeechTs||0, this._lastRemoteSpeechTs||0);
          if (Date.now() - last > limitMs){
            clearInterval(this._inactTimer); this._inactTimer=null;
            try{ pc.close(); }catch(_){ }
            try{ localStream && localStream.getTracks().forEach(t=> t.stop()); }catch(_){ }
            const ov = document.getElementById('call-overlay'); if (ov) ov.classList.add('hidden');
            this.saveMessage({ text: '[system] Call ended due to 5 minutes of silence' }).catch(()=>{});
            this._startAutoResumeMonitor(opts && opts.video);
          }
        }, 15000);
      }catch(_){ }
    }

    async _startAutoResumeMonitor(video){
      if (this._monitoring) return;
      this._monitoring = true;
      console.log('Starting speech monitor');
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      this._monitorStream = stream;
      const ac = new AudioContext();
      const src = ac.createMediaStreamSource(stream);
      const analyser = ac.createAnalyser();
      analyser.fftSize = 256; // Smaller for faster response
      src.connect(analyser);
      const data = new Uint8Array(analyser.frequencyBinCount);
      let streak = 0;
      const tick = () => {
        if (!this._inRoom || (this._roomState && this._roomState.activeCallId)) return;
        analyser.getByteFrequencyData(data);
        let sum = 0; data.forEach(v => sum += v * v); // Energy
        const rms = Math.sqrt(sum / data.length);
        console.log('RMS:', rms);
        if (rms > 10) streak++; else streak = 0; // Lower threshold
        if (streak > 3) { // Shorter streak
          console.log('Speech detected! Starting call');
          stream.getTracks().forEach(t => t.stop());
          this._monitorStream = null;
          ac.close();
          this.attemptStartRoomCall(video);
          this._monitoring = false;
          return;
        }
        requestAnimationFrame(tick);
      };
      requestAnimationFrame(tick);
    }
    async recordVoiceMessage(){
      try{
        let stream;
        try{
          stream = await navigator.mediaDevices.getUserMedia({ audio: this.getRecordingAudioConstraints() });
        }catch(_){
          stream = await navigator.mediaDevices.getUserMedia({ audio: true });
        }
        await this.captureMedia(stream, this.getPreferredMediaRecorderOptions('audio'), 60_000, 'voice.webm');
      }catch(e){ console.warn('Voice record unavailable', e); }
    }
    async recordVideoMessage(){
      try{
        const audioCfg = this.getRecordingAudioConstraints();
        let stream;
        try{
          stream = await navigator.mediaDevices.getUserMedia({ audio: audioCfg, video: { facingMode: this._recFacing || 'user' } });
        }catch(_){
          try{
            stream = await navigator.mediaDevices.getUserMedia({ audio: audioCfg, video: true });
          }catch(__){
            stream = await navigator.mediaDevices.getUserMedia({ audio: true, video: { facingMode: this._recFacing || 'user' } });
          }
        }
        const indicator = document.getElementById('recording-indicator');
        if (indicator){ indicator.classList.remove('hidden'); indicator.querySelector('i').className = 'fas fa-video'; }
        const opts = this.getPreferredMediaRecorderOptions('video');
        const ext = String(opts?.mimeType || '').includes('mp4') ? 'mp4' : 'webm';
        await this.captureMedia(stream, opts, 30_000, `video.${ext}`);
        if (indicator){ indicator.classList.add('hidden'); }
      }catch(e){ console.warn('Video record unavailable', e); }
    }

    async captureMedia(stream, options, maxMs, filename){
      return new Promise((resolve)=>{
        this.stopRegularPlayer();
        const rec = new MediaRecorder(stream, options);
        const chunks = [];
        let stopped = false;
        const stopAll = ()=>{ if (stopped) return; stopped = true; try{ rec.stop(); }catch(_){} try{ stream.getTracks().forEach(t=> t.stop()); }catch(_){} };
        this._activeRecorder = rec; this._activeStream = stream; this._recStop = stopAll;
        this.showLiveRecordingPreview(stream);
        rec.ondataavailable = (e)=>{ if (e.data && e.data.size) chunks.push(e.data); };
        const hardStopTimer = setTimeout(stopAll, maxMs);
        rec.onstop = async ()=>{
          try{
            clearTimeout(hardStopTimer);
            this.hideLiveRecordingPreview();
            const blob = new Blob(chunks, { type: options.mimeType || 'application/octet-stream' });
            // stage for review
            this._pendingRecording = { blob, type: (options.mimeType||'application/octet-stream'), filename };
            this.showRecordingReview(blob, filename);
          }catch(_){ }
          this._activeRecorder = null; this._activeStream = null; this._recStop = null; this._isRecordingByHold = false;
          resolve();
        };
        rec.start();
      });
    }

    showLiveRecordingPreview(stream){
      try{
        const review = document.getElementById('recording-review');
        const player = document.getElementById('recording-player');
        const sendBtn = document.getElementById('send-recording-btn');
        const switchBtn = document.getElementById('switch-camera-btn');
        const discardBtn = document.getElementById('discard-recording-btn');
        const input = document.getElementById('message-input');
        if (!review || !player) return;
        player.innerHTML = '';
        const hasVideo = !!(stream && stream.getVideoTracks && stream.getVideoTracks().length);
        const mediaEl = document.createElement(hasVideo ? 'video' : 'audio');
        mediaEl.autoplay = true;
        mediaEl.muted = true;
        mediaEl.playsInline = true;
        mediaEl.controls = false;
        mediaEl.srcObject = stream;
        if (hasVideo){
          mediaEl.classList.add('video-recording-mask');
          this.applyRandomTriangleMask(mediaEl);
        }
        player.appendChild(mediaEl);
        review.classList.remove('hidden');
        if (input) input.style.display = 'none';
        if (sendBtn) sendBtn.style.display = 'none';
        if (switchBtn){
          switchBtn.style.display = hasVideo ? 'inline-block' : 'none';
          switchBtn.onclick = ()=>{
            this._recFacing = this._recFacing === 'user' ? 'environment' : 'user';
          };
        }
        if (discardBtn){
          discardBtn.textContent = 'Stop';
          discardBtn.onclick = ()=>{ try{ if (this._recStop) this._recStop(); }catch(_){ } };
        }
      }catch(_){ }
    }

    hideLiveRecordingPreview(){
      try{
        const sendBtn = document.getElementById('send-recording-btn');
        const discardBtn = document.getElementById('discard-recording-btn');
        if (sendBtn) sendBtn.style.display = '';
        if (discardBtn) discardBtn.textContent = 'Discard';
      }catch(_){ }
    }

    async _stopScreenShare(){
      if (!this._screenSharing) return;
      this._screenSharing = false;
      const shareBtn = document.getElementById('share-screen-btn');
      if (shareBtn) shareBtn.classList.remove('active');
      try {
        if (this._screenStream) {
          this._screenStream.getTracks().forEach(t => t.stop());
          this._screenStream = null;
        }
        let localStream = null;
        this._activePCs.forEach((p) => {
          const videoSender = p.pc.getSenders?.().find(s => s.track?.kind === 'video');
          const camTrack = p.stream?.getVideoTracks?.()[0];
          if (videoSender && camTrack) videoSender.replaceTrack(camTrack);
          if (p.stream) localStream = p.stream;
        });
        const lv = document.getElementById('localVideo');
        if (lv && localStream) lv.srcObject = localStream;
      } catch (e) { console.warn('Stop screen share error', e); }
    }

    async cleanupActiveCall(endRoom = false, reason = 'unknown'){
    this._startingCall = false;
    this._joiningCall = false;
    this._lastJoinedCallId = null;
    if (this._screenSharing) await this._stopScreenShare();
      this._activePCs.forEach((p, uid) => {
        try{ const key = (this._activeCid||'')+':'+uid; const w=this._pcWatchdogs.get(key); if (w){ clearTimeout(w.t1); clearTimeout(w.t2); this._pcWatchdogs.delete(key); } }catch(_){ }
        try{ p.unsubs.forEach(u => u()); }catch(_){ }
        try{ p.pc.close(); }catch(_){ }
        try{ p.stream.getTracks().forEach(t => { t.stop(); }); }catch(_){ }
        if (p.videoEl) p.videoEl.remove();
      });
      this._activePCs.clear();
      if (this._inactTimer) clearInterval(this._inactTimer);
      if (endRoom){
        const roomRef = firebase.doc(this.db,'callRooms', this.activeConnection);
        await firebase.updateDoc(roomRef, { status: 'idle', activeCallId: null });
      }
      await this.updatePresence('idle', false);
      if (this._monitorStream) {
        this._monitorStream.getTracks().forEach(t => { t.stop(); });
        this._monitorStream = null;
        this._monitoring = false;
      }
      document.querySelectorAll('#call-videos video').forEach(v => v.remove());
      if (!endRoom) this._inRoom = false;
      try {
        await navigator.mediaDevices.getUserMedia({audio: false});
      } catch (_) {}
      const audioEls = document.querySelectorAll('[id^="remoteAudio-"]');
      audioEls.forEach(el => el.remove());
      try {
        const devices = await navigator.mediaDevices.enumerateDevices();
        devices.forEach(d => {
          if (d.kind === 'audioinput' && typeof d.stop === 'function') d.stop();
        });
      } catch (_) {}
      this.updateRoomUI();
      const sb = document.getElementById('start-call-btn');
      if (sb) sb.style.display = '';
    }

    async updatePresence(state, hasVideo = false){
      const ref = firebase.doc(this.db,'callRooms', this.activeConnection, 'peers', this.currentUser.uid);
      await firebase.setDoc(ref, { uid: this.currentUser.uid, state, hasVideo, updatedAt: new Date().toISOString() }, { merge: true });
    }

    async updateRoomUI(){
      const cont = document.getElementById('call-participants');
      if (!cont) return;
      cont.innerHTML = '';
      try {
        const connSnap = await firebase.getDoc(firebase.doc(this.db,'chatConnections', this.activeConnection));
        if (!connSnap.exists()) return;
        const conn = connSnap.data();
        const inCall = this._activePCs && this._activePCs.size > 0;
        const peerUids = inCall ? Array.from(this._activePCs.keys()) : Array.from(new Set((conn.participants || []).filter(Boolean)));
        const uniq = Array.from(new Set(peerUids.filter(Boolean)));
        const fetches = uniq.map(async uid => {
          if (uid === this.currentUser.uid) return null;
          const p = this._peersPresence[uid] || { state: 'idle', hasVideo: false };
          let cached = this.usernameCache.get(uid);
          if (!cached || !cached.avatarUrl) {
            try {
              const u = await window.firebaseService.getUserData(uid);
              cached = { username: u?.username || uid.slice(0,8), avatarUrl: u?.avatarUrl || '../../images/default-bird.png' };
              this.usernameCache.set(uid, cached);
            } catch (_) { cached = { username: uid.slice(0,8), avatarUrl: '../../images/default-bird.png' }; }
          }
          return { uid, p, cached };
        });
        const remotes = (await Promise.all(fetches)).filter(item => item);
        remotes.forEach(item => {
          try {
            const {uid, p, cached} = item;
            const av = document.createElement('div');
            av.className = `avatar ${p.state}`;
            av.setAttribute('data-uid', uid);
            av.innerHTML = `<img src="${cached.avatarUrl || '../../images/default-bird.png'}" alt="${cached.username}"/>`;
            cont.appendChild(av);
            // Video tile logic...
          } catch (err) {
            console.error('Error rendering remote participant:', err);
          }
        });
        // Self avatar (single)
        let selfCached = this.usernameCache.get(this.currentUser.uid);
        if (!selfCached || !selfCached.avatarUrl) {
          try {
            const u = await window.firebaseService.getUserData(this.currentUser.uid);
            selfCached = { username: u?.username || 'You', avatarUrl: u?.avatarUrl || '../../images/default-bird.png' };
            this.usernameCache.set(this.currentUser.uid, selfCached);
          } catch (_) { selfCached = { username: 'You', avatarUrl: '../../images/default-bird.png' }; }
        }
        const meState = (this._peersPresence[this.currentUser.uid] && this._peersPresence[this.currentUser.uid].state) || 'idle';
        const selfAv = document.createElement('div');
        selfAv.className = `avatar local ${meState}`;
        selfAv.setAttribute('data-uid', this.currentUser.uid);
        selfAv.innerHTML = `<img src="${selfCached.avatarUrl || '../../images/default-bird.png'}" alt="${selfCached.username}"/>`;
        cont.appendChild(selfAv);
      } catch (err) {
        console.error('UI update error:', err);
      }
      const inCall = !!(this._activePCs && this._activePCs.size > 0);
      const statusEl = document.getElementById('call-status');
      if (statusEl) {
        statusEl.textContent = inCall ? 'In call' : ((this._roomState && this._roomState.activeCallId) ? 'Connecting...' : 'Ready for call.');
      }
      // Hide header call buttons when in call to avoid duplicated icons
      [ 'voice-call-btn', 'video-call-btn', 'mobile-voice-call-btn', 'mobile-video-call-btn' ].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.style.display = inCall ? 'none' : '';
      });
    }

    async renegotiateCall(callId, video){
      for (const [peerUid, p] of this._activePCs) {
        const pc = p.pc;
        const videoSender = pc.getSenders?.().find(s => s.track?.kind === 'video');
        const videoTrack = p.stream?.getVideoTracks?.()[0];
        if (videoSender && videoTrack) videoSender.replaceTrack(video ? videoTrack : null);
        const offer = await pc.createOffer();
        await pc.setLocalDescription(offer);
        const offersRef = firebase.collection(this.db,'calls',callId,'offers');
        await firebase.setDoc(firebase.doc(offersRef, peerUid), { sdp: offer.sdp, type: offer.type, createdAt: new Date().toISOString(), connId: this.activeConnection, fromUid: this.currentUser.uid, toUid: peerUid });
      }
    }
  }

  window.secureChatApp = new SecureChatApp();
})();

// Review helpers (attach safely to running instance to avoid global class reference errors)
if (window && window.secureChatApp && typeof window.secureChatApp.showRecordingReview !== 'function'){
window.secureChatApp.showRecordingReview = function(blob, filename){
  try{
    const review = document.getElementById('recording-review');
    const player = document.getElementById('recording-player');
    const sendBtn = document.getElementById('send-recording-btn');
    const discardBtn = document.getElementById('discard-recording-btn');
    const switchBtn = document.getElementById('switch-camera-btn');
    if (!review || !player || !sendBtn || !discardBtn) return;
    const self = window.secureChatApp;
    player.innerHTML = '';
    const url = URL.createObjectURL(blob);
    const isVideo = (blob.type||'').startsWith('video');
    if (isVideo){
      const mediaEl = document.createElement('video');
      mediaEl.controls = true;
      mediaEl.src = url;
      mediaEl.playsInline = true;
      mediaEl.classList.add('video-recording-mask');
      mediaEl.style.maxWidth = '100%';
      mediaEl.addEventListener('loadedmetadata', ()=>{ try{ self.applyRandomTriangleMask(mediaEl); }catch(_){ } });
      mediaEl.addEventListener('play', ()=>{
        try{
          const hostBg = self.getGlobalBgPlayer();
          if (hostBg && !hostBg.paused) hostBg.pause();
        }catch(_){ }
      });
      player.appendChild(mediaEl);
      mediaEl.load();
    } else {
      // Keep type-bar review consistent with in-chat voice UI.
      self.renderWaveAttachment(player, url, 'You');
    }
    review.classList.remove('hidden');
    const input = document.getElementById('message-input'); if (input) input.style.display='none';
    const actionBtn = document.getElementById('action-btn'); if (actionBtn){ actionBtn.innerHTML = '<i class="fas fa-arrow-up"></i>'; actionBtn.title = 'Send'; }
    self._recordingSendInFlight = false;
    if (switchBtn){ switchBtn.style.display = isVideo ? 'inline-block':'none'; }
    if (switchBtn && isVideo){
      switchBtn.onclick = async ()=>{
        try{
          // Toggle between user/environment
          const currFacing = self._recFacing || 'user';
          const next = currFacing === 'user' ? 'environment' : 'user';
          self._recFacing = next;
          const newStream = await navigator.mediaDevices.getUserMedia({ audio: self.getRecordingAudioConstraints(), video: { facingMode: next } });
          await self.captureMedia(newStream, self.getPreferredMediaRecorderOptions('video'), 30_000, 'video.webm');
        }catch(_){ }
      };
    }
    sendBtn.onclick = async ()=>{
      if (self._recordingSendInFlight) return;
      const targetConnId = self.activeConnection;
      if (!targetConnId) return;
      const pending = self._pendingRecording;
      if (!pending || !pending.blob){ review.classList.add('hidden'); player.innerHTML = ''; return; }
      const blob = pending.blob;
      const isVideoSend = (pending.type||'').startsWith('video');
      const filename = pending.filename || (isVideoSend ? 'video.webm' : 'voice.webm');
      self._recordingSendInFlight = true;
      self._pendingRecording = null;
      sendBtn.disabled = true;
      discardBtn.disabled = true;
      sendBtn.style.opacity = '0.65';
      discardBtn.style.opacity = '0.65';
      review.classList.add('hidden');
      player.innerHTML = '';
      if (input) input.style.display='';
      if (actionBtn){ actionBtn.innerHTML = `<i class="fas ${self._recordMode === 'video' ? 'fa-video' : 'fa-microphone'}"></i>`; actionBtn.title = self._recordMode === 'video' ? 'Video message' : 'Voice message'; }
      if (!self.storage) {
        console.error('Storage not available for recording');
        alert('Recording upload not available - storage not configured. Please check Firebase configuration.');
        self._recordingSendInFlight = false;
        sendBtn.disabled = false;
        discardBtn.disabled = false;
        sendBtn.style.opacity = '';
        discardBtn.style.opacity = '';
        return;
      }

      try {
        if (window.firebaseService?.auth?.currentUser?.getIdToken){
          await window.firebaseService.auth.currentUser.getIdToken(true);
        }
        if (!window.firebaseService?.auth?.currentUser) throw new Error('Auth lost - please re-login');
      } catch (err) {
        console.error('Auth refresh failed before recording send:', err);
        alert('Auth error - please reload and re-login');
        self._recordingSendInFlight = false;
        sendBtn.disabled = false;
        discardBtn.disabled = false;
        sendBtn.style.opacity = '';
        discardBtn.style.opacity = '';
        return;
      }
      try {
        const aesKey = await self.getFallbackKeyForConn(targetConnId);
        const salts = await self.getConnSaltForConn(targetConnId);
        let base64;
        if (isVideoSend) {
          const buf = await blob.arrayBuffer();
          const bytes = new Uint8Array(buf);
          const chunkSize = 0x8000;
          let binary = '';
          for (let i = 0; i < bytes.length; i += chunkSize) {
            binary += String.fromCharCode.apply(null, bytes.subarray(i, Math.min(i + chunkSize, bytes.length)));
          }
          base64 = btoa(binary);
        } else {
          base64 = await new Promise((resolve,reject)=>{ const r=new FileReader(); r.onload=()=>{ const s=String(r.result||''); resolve(s.includes(',')?s.split(',')[1]:''); }; r.onerror=reject; r.readAsDataURL(blob); });
        }
        const cipher = await chatCrypto.encryptWithKey(base64, aesKey);
        const safe = `chat/${targetConnId}/${Date.now()}_${filename}`;
        const sref = firebase.ref(self.storage, `${safe}.enc.json`);
        await firebase.uploadBytes(sref, new Blob([JSON.stringify(cipher)], {type:'application/json'}), { contentType: 'application/json' });
        const url2 = await firebase.getDownloadURL(sref);
        await self.saveMessage({
          text: isVideoSend ? '[video message]' : '[voice message]',
          fileUrl: url2,
          fileName: filename,
          connId: targetConnId,
          attachmentSourceConnId: targetConnId,
          attachmentKeySalt: String(salts?.stableSalt ?? targetConnId ?? ''),
          isVideoRecording: isVideoSend,
          isVoiceRecording: !isVideoSend
        });
      } catch (err) {
        console.error('Recording upload error:', err.code, err.message, err);
        if (err.code === 'storage/unauthorized') {
          alert('Recording upload failed: Storage access denied. Please check Firebase Storage rules.');
        } else if (err.code === 'storage/bucket-not-found') {
          alert('Recording upload failed: Storage bucket not found. Please check Firebase configuration.');
        } else {
          alert('Failed to send recording: ' + err.message);
        }
      }
      finally{
        self._recordingSendInFlight = false;
        sendBtn.disabled = false;
        discardBtn.disabled = false;
        sendBtn.style.opacity = '';
        discardBtn.style.opacity = '';
        try{ if (url) URL.revokeObjectURL(url); }catch(_){ }
        review.classList.add('hidden');
        player.innerHTML='';
        if (input) input.style.display='';
        self.refreshActionButton();
      }
    };
    discardBtn.onclick = ()=>{
      if (self._recordingSendInFlight) return;
      self._pendingRecording = null;
      try{ if (url) URL.revokeObjectURL(url); }catch(_){ }
      review.classList.add('hidden');
      player.innerHTML='';
      if (input) input.style.display='';
      self.refreshActionButton();
    };
  }catch(_){ }
};
}

window.addEventListener('beforeunload', () => {
  try{ secureChatApp.publishTypingState(false, { force: true }); }catch(_){ }
  try{ secureChatApp.stopTypingListener(); }catch(_){ }
  if (secureChatApp._inRoom) secureChatApp.cleanupActiveCall(false);
  if (secureChatApp._monitorStream) {
    secureChatApp._monitorStream.getTracks().forEach(t => t.stop());
    secureChatApp._monitorStream = null;
  }
});
