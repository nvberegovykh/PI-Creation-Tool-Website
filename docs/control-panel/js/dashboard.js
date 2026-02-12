/**
 * Dashboard Module for Liber Apps Control Panel
 * Handles navigation, overview, and dashboard functionality
 */

class DashboardManager {
    constructor() {
        this.currentSection = 'apps';
        this._dashboardSuspended = false;
        this._playQueue = [];
        this._playQueueIndex = -1;
        this.init();
    }

    async resolveCurrentUser(){
        try{
            const u = await window.firebaseService.getCurrentUser();
            if (u && u.uid) return u;
        }catch(_){ }
        try{
            const u2 = window.firebaseService?.auth?.currentUser;
            if (u2 && u2.uid) return u2;
        }catch(_){ }
        try{
            const am = window.authManager?.currentUser;
            if (am && am.id) return { uid: am.id, email: am.email || '' };
        }catch(_){ }
        return null;
    }

    async resolveCurrentUserWithRetry(maxWaitMs = 2500){
        const start = Date.now();
        let user = await this.resolveCurrentUser();
        while ((!user || !user.uid) && (Date.now() - start) < maxWaitMs){
            await new Promise((r)=> setTimeout(r, 180));
            user = await this.resolveCurrentUser();
        }
        return user;
    }

    scheduleSpaceRetry(){
        if (this._spaceRetryTimer) return;
        this._spaceRetryTimer = setTimeout(()=>{
            this._spaceRetryTimer = null;
            if (this._dashboardSuspended) return;
            if (this.currentSection === 'space'){
                this.loadSpace();
            }
        }, 320);
    }

    clearPostActionListeners(container){
        try{
            if (!container) return;
            if (this._postActionUnsubsByContainer && this._postActionUnsubsByContainer.get(container)) {
                this._postActionUnsubsByContainer.get(container).forEach((u) => { try { u(); } catch (_) {} });
                this._postActionUnsubsByContainer.set(container, []);
            }
        }catch(_){ }
    }

    suspendDashboardActivity(){
        this._dashboardSuspended = true;
        this.clearPostActionListeners(document.getElementById('global-feed'));
        this.clearPostActionListeners(document.getElementById('space-feed'));
    }

    resumeDashboardActivity(){
        const wasSuspended = this._dashboardSuspended;
        this._dashboardSuspended = false;
        if (!wasSuspended) return;
        if (this.currentSection === 'feed') this.loadGlobalFeed();
        if (this.currentSection === 'space') this.loadSpace();
    }

    getOrCreateDeviceId(){
        try{
            const existing = localStorage.getItem('liber_device_id');
            if (existing) return existing;
            const base = [
                navigator.userAgent || '',
                navigator.platform || '',
                navigator.language || '',
                Intl.DateTimeFormat().resolvedOptions().timeZone || ''
            ].join('|');
            const encoded = btoa(unescape(encodeURIComponent(base))).replace(/[^a-zA-Z0-9]/g, '');
            const id = `dv_${encoded.slice(0, 64)}`;
            localStorage.setItem('liber_device_id', id);
            return id;
        }catch(_){
            const id = `dv_${Date.now().toString(36)}_${Math.random().toString(36).slice(2,10)}`;
            try{ localStorage.setItem('liber_device_id', id); }catch(__){ }
            return id;
        }
    }

    async updateVerificationBanner(){
        try{
            let banner = document.getElementById('verify-warning-banner');
            const authUser = window.firebaseService?.auth?.currentUser || null;
            const shouldShow = !!(authUser && authUser.uid && authUser.email && !authUser.emailVerified);
            if (!shouldShow){
                if (banner) banner.remove();
                return;
            }
            if (!banner){
                banner = document.createElement('div');
                banner.id = 'verify-warning-banner';
                banner.style.cssText = 'position:fixed;left:12px;bottom:12px;z-index:1200;background:#b91c1c;color:#fff;padding:10px 12px;border-radius:10px;display:flex;gap:10px;align-items:center;max-width:min(92vw,560px);box-shadow:0 8px 24px rgba(0,0,0,.35)';
                banner.innerHTML = `<span style="font-size:13px;line-height:1.3">Accounts not verified in 7 days after register are being deleted.</span>
                                    <button id="verify-warning-resend" class="btn btn-secondary" style="white-space:nowrap">Resend verification</button>`;
                document.body.appendChild(banner);
            }
            const resendBtn = banner.querySelector('#verify-warning-resend');
            if (resendBtn && !resendBtn._bound){
                resendBtn._bound = true;
                resendBtn.onclick = async ()=>{
                    try{
                        resendBtn.disabled = true;
                        const ok = await window.firebaseService.sendEmailVerification();
                        if (ok !== false) this.showSuccess('Verification email sent');
                        else this.showError('Failed to send verification email');
                    }catch(_){ this.showError('Failed to send verification email'); }
                    finally{ resendBtn.disabled = false; }
                };
            }
        }catch(_){ }
    }

    renderPostMedia(media){
        const urls = Array.isArray(media) ? media : (media ? [media] : []);
        if (!urls.length) return '';
        const renderOne = (url)=>{
            const href = String(url||'');
            let pathOnly = href;
            try{ pathOnly = new URL(href).pathname; }catch(_){ pathOnly = href.split('?')[0].split('#')[0]; }
            const lower = pathOnly.toLowerCase();
            const isImg = ['.png','.jpg','.jpeg','.gif','.webp','.avif'].some(ext=> lower.endsWith(ext));
            const isVid = ['.mp4','.webm','.mov','.mkv'].some(ext=> lower.endsWith(ext));
            const isAud = ['.mp3','.wav','.m4a','.aac','.ogg','.oga','.weba'].some(ext=> lower.endsWith(ext));
            if (isImg){
                return `<img src="${href}" alt="media" style="max-width:100%;height:auto;border-radius:12px" />`;
            }
            if (isVid){
                return `<div class="player-card"><video src="${href}" class="player-media" controls playsinline style="width:100%;max-height:360px;border-radius:8px;object-fit:contain"></video><div class="player-bar"><button class="btn-icon" data-action="play"><i class="fas fa-play"></i></button><div class="progress"><div class="fill"></div></div><div class="time"></div></div></div>`;
            }
            if (isAud){
                return `<div class="player-card"><audio src="${href}" class="player-media" preload="metadata" ></audio><div class="player-bar"><button class="btn-icon" data-action="play"><i class="fas fa-play"></i></button><div class="progress"><div class="fill"></div></div><div class="time"></div></div></div>`;
            }
            if (lower.endsWith('.pdf')){
                return `<iframe src="${href}" style="width:100%;height:420px;border:none"></iframe>`;
            }
            return `<a href="${href}" target="_blank" rel="noopener noreferrer">Open attachment</a>`;
        };
        if (urls.length === 1){
            return `<div style="margin-top:8px">${renderOne(urls[0])}</div>`;
        }
        // simple gallery
        const items = urls.map(u=> `<div style="flex:0 0 auto;max-width:100%;">${renderOne(u)}</div>`).join('');
        return `<div style="margin-top:8px;overflow:auto;-webkit-overflow-scrolling:touch"><div style="display:flex;gap:8px">${items}</div></div>`;
    }

    pauseAllMediaExcept(current){
        try{
            const bg = document.getElementById('bg-player');
            if (bg && bg !== current){ try{ bg.pause(); }catch(_){ } }
            const localChatBg = document.getElementById('chat-bg-player');
            if (localChatBg && localChatBg !== current){ try{ localChatBg.pause(); }catch(_){ } }
            try{
                const shellFrame = document.getElementById('app-shell-frame');
                const iframeDoc = shellFrame?.contentWindow?.document;
                const iframeChatBg = iframeDoc?.getElementById('chat-bg-player');
                if (iframeChatBg && iframeChatBg !== current){ try{ iframeChatBg.pause(); }catch(_){ } }
            }catch(_){ }
            document.querySelectorAll('audio.player-media, video.player-media, .liber-lib-audio, .liber-lib-video').forEach((m)=>{
                if (m !== current){
                    try{ m.pause(); }catch(_){ }
                    const card = m.closest('.player-card');
                    const btn = card && card.querySelector('.btn-icon');
                    if (btn) this.setPlayIcon(btn, false);
                }
            });
        }catch(_){ }
    }

    setPlayIcon(btn, isPlaying){
        if (!btn) return;
        btn.innerHTML = `<i class="fas ${isPlaying ? 'fa-pause' : 'fa-play'}"></i>`;
    }

    formatDuration(seconds){
        const s = Math.max(0, Math.floor(Number(seconds || 0)));
        const m = Math.floor(s / 60);
        const ss = String(s % 60).padStart(2, '0');
        return `${m}:${ss}`;
    }

    getBgPlayer(){
        let bg = document.getElementById('bg-player');
        if (!bg){
            bg = document.createElement('audio');
            bg.id = 'bg-player';
            bg.style.display='none';
            document.body.appendChild(bg);
        }
        return bg;
    }

    getPlaylistStorageKey(){
        const uid = this.currentUser?.uid || window.firebaseService?.auth?.currentUser?.uid || 'anon';
        return `liber_playlists_${uid}`;
    }

    getPlaylists(){
        try{
            const raw = localStorage.getItem(this.getPlaylistStorageKey());
            const data = raw ? JSON.parse(raw) : [];
            return Array.isArray(data) ? data : [];
        }catch(_){ return []; }
    }

    savePlaylists(playlists){
        try{ localStorage.setItem(this.getPlaylistStorageKey(), JSON.stringify(playlists || [])); }catch(_){ }
    }

    openAddToPlaylistPopup(track){
        const playlists = this.getPlaylists();
        const overlay = document.createElement('div');
        overlay.className = 'modal-overlay';
        overlay.style.cssText = 'position:fixed;inset:0;z-index:1300;background:rgba(0,0,0,.58);display:flex;align-items:center;justify-content:center;padding:16px';
        const options = playlists.map((p)=> `<option value="${p.id}">${(p.name||'Playlist').replace(/</g,'&lt;')}</option>`).join('');
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
              <button id="pl-cancel" class="btn btn-secondary">Cancel</button>
              <button id="pl-save" class="btn btn-primary">Save</button>
            </div>
          </div>`;
        document.body.appendChild(overlay);
        overlay.querySelector('#pl-cancel').onclick = ()=> overlay.remove();
        overlay.addEventListener('click', (e)=>{ if (e.target === overlay) overlay.remove(); });
        overlay.querySelector('#pl-save').onclick = ()=>{
            const selectedId = String(overlay.querySelector('#pl-select').value || '').trim();
            const newName = String(overlay.querySelector('#pl-new').value || '').trim();
            let selected = null;
            if (selectedId){
                selected = playlists.find((p)=> p.id === selectedId) || null;
            } else if (newName){
                selected = { id: `pl_${Date.now()}`, name: newName, items: [] };
                playlists.push(selected);
            }
            if (!selected){ this.showError('Choose or create a playlist'); return; }
            if (!selected.items) selected.items = [];
            selected.items.push({
                id: `it_${Date.now()}_${Math.random().toString(36).slice(2,6)}`,
                src: track.src,
                title: track.title || 'Track',
                by: track.by || '',
                cover: track.cover || ''
            });
            this.savePlaylists(playlists);
            this.renderPlaylists();
            overlay.remove();
            this.showSuccess('Added to playlist');
        };
    }

    renderQueuePanel(){
        const panel = document.getElementById('mini-queue-panel');
        const list = document.getElementById('mini-queue-list');
        if (!panel || !list) return;
        const queue = this._playQueue || [];
        const next = queue.slice(Math.max(0, this._playQueueIndex + 1));
        list.innerHTML = next.length
            ? next.map((q,i)=> `<div class="mini-queue-item" data-idx="${this._playQueueIndex + 1 + i}">${(q.title||'Track').replace(/</g,'&lt;')}</div>`).join('')
            : '<div class="mini-queue-item">Queue is empty</div>';
        list.querySelectorAll('[data-idx]').forEach((el)=>{
            el.onclick = ()=>{
                const idx = Number(el.getAttribute('data-idx'));
                this.playQueueIndex(idx);
                panel.style.display = 'none';
            };
        });
    }

    playQueueIndex(idx){
        const item = this._playQueue[idx];
        if (!item) return;
        this._playQueueIndex = idx;
        const bg = this.getBgPlayer();
        bg.src = item.src;
        bg.currentTime = 0;
        bg.play().catch(()=>{});
        const miniTitle = document.getElementById('mini-title');
        const miniBy = document.getElementById('mini-by');
        const miniCover = document.querySelector('#mini-player .cover');
        if (miniTitle) miniTitle.textContent = item.title || 'Now playing';
        if (miniBy) miniBy.textContent = item.by || '';
        if (miniCover && item.cover) miniCover.src = item.cover;
        this.renderQueuePanel();
    }

    async renderPlaylists(){
        const host = document.getElementById('wave-playlists');
        if (!host) return;
        const playlists = this.getPlaylists();
        host.innerHTML = '';
        if (!playlists.length){
            host.innerHTML = '<div style="opacity:.8">No playlists yet.</div>';
            return;
        }
        playlists.forEach((pl)=>{
            const wrap = document.createElement('div');
            wrap.style.cssText = 'border:1px solid var(--border-color);border-radius:10px;padding:8px;margin-bottom:10px;background:#0f1116';
            const head = document.createElement('div');
            head.style.cssText = 'display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:8px';
            head.innerHTML = `<strong>${(pl.name||'Playlist').replace(/</g,'&lt;')}</strong><button class="btn btn-secondary" data-del="${pl.id}"><i class="fas fa-trash"></i></button>`;
            wrap.appendChild(head);
            const list = document.createElement('div');
            (pl.items || []).forEach((it, idx)=>{
                const row = document.createElement('div');
                row.className = 'playlist-row';
                row.draggable = true;
                row.dataset.idx = String(idx);
                row.style.cssText = 'display:flex;align-items:center;justify-content:space-between;gap:8px;padding:8px;border-radius:8px;border:1px solid #273247;margin-bottom:6px;cursor:grab';
                row.innerHTML = `<span style="min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${(it.title||'Track').replace(/</g,'&lt;')}</span><div style="display:flex;gap:6px"><button class="btn btn-secondary" data-up="${it.id}" title="Move up"><i class="fas fa-arrow-up"></i></button><button class="btn btn-secondary" data-down="${it.id}" title="Move down"><i class="fas fa-arrow-down"></i></button><button class="btn btn-secondary" data-play="${it.id}"><i class="fas fa-play"></i></button><button class="btn btn-secondary" data-remove="${it.id}"><i class="fas fa-xmark"></i></button></div>`;
                list.appendChild(row);
            });
            wrap.appendChild(list);
            host.appendChild(wrap);

            list.querySelectorAll('[data-play]').forEach((btn)=>{
                btn.onclick = ()=>{
                    const tid = btn.getAttribute('data-play');
                    const item = (pl.items || []).find((x)=> x.id === tid);
                    if (!item) return;
                    this._playQueue = (pl.items || []).map((x)=> ({ src: x.src, title: x.title, by: x.by, cover: x.cover }));
                    this._playQueueIndex = Math.max(0, (pl.items || []).findIndex((x)=> x.id === tid));
                    this.playQueueIndex(this._playQueueIndex);
                };
            });
            list.querySelectorAll('[data-remove]').forEach((btn)=>{
                btn.onclick = ()=>{
                    const tid = btn.getAttribute('data-remove');
                    pl.items = (pl.items || []).filter((x)=> x.id !== tid);
                    this.savePlaylists(playlists);
                    this.renderPlaylists();
                };
            });
            list.querySelectorAll('[data-up]').forEach((btn)=>{
                btn.onclick = ()=>{
                    const tid = btn.getAttribute('data-up');
                    const arr = pl.items || [];
                    const i = arr.findIndex((x)=> x.id === tid);
                    if (i <= 0) return;
                    [arr[i-1], arr[i]] = [arr[i], arr[i-1]];
                    pl.items = arr;
                    this.savePlaylists(playlists);
                    this.renderPlaylists();
                };
            });
            list.querySelectorAll('[data-down]').forEach((btn)=>{
                btn.onclick = ()=>{
                    const tid = btn.getAttribute('data-down');
                    const arr = pl.items || [];
                    const i = arr.findIndex((x)=> x.id === tid);
                    if (i < 0 || i >= arr.length - 1) return;
                    [arr[i+1], arr[i]] = [arr[i], arr[i+1]];
                    pl.items = arr;
                    this.savePlaylists(playlists);
                    this.renderPlaylists();
                };
            });
            const delBtn = wrap.querySelector('[data-del]');
            if (delBtn){
                delBtn.onclick = ()=>{
                    const next = playlists.filter((x)=> x.id !== pl.id);
                    this.savePlaylists(next);
                    this.renderPlaylists();
                };
            }

            let dragSrc = -1;
            list.querySelectorAll('.playlist-row').forEach((row)=>{
                row.addEventListener('dragstart', ()=>{ dragSrc = Number(row.dataset.idx); row.style.opacity = '0.5'; });
                row.addEventListener('dragend', ()=>{ row.style.opacity = '1'; });
                row.addEventListener('dragover', (e)=>{ e.preventDefault(); });
                row.addEventListener('drop', (e)=>{
                    e.preventDefault();
                    const target = Number(row.dataset.idx);
                    if (!Number.isFinite(dragSrc) || !Number.isFinite(target) || dragSrc === target) return;
                    const arr = pl.items || [];
                    const [moved] = arr.splice(dragSrc, 1);
                    arr.splice(target, 0, moved);
                    pl.items = arr;
                    this.savePlaylists(playlists);
                    this.renderPlaylists();
                });
            });
        });
    }

    showMiniPlayer(mediaEl, meta={}){
        try{
            if (this._currentPlayer && this._currentPlayer !== mediaEl){ try{ this._currentPlayer.pause(); }catch(_){} }
            this._currentPlayer = mediaEl;
            // Promote playback to a hidden background audio so it persists across sections
            const bg = this.getBgPlayer();
            const mini = document.getElementById('mini-player'); if (!mini) return;
            const mTitle = document.getElementById('mini-title');
            const mBy = document.getElementById('mini-by');
            const mCover = mini.querySelector('.cover');
            const playBtn = document.getElementById('mini-play');
            const addBtn = document.getElementById('mini-add-playlist');
            const queueBtn = document.getElementById('mini-queue');
            const queuePanel = document.getElementById('mini-queue-panel');
            const queueClose = document.getElementById('mini-queue-close');
            const closeBtn = document.getElementById('mini-close');
            const miniProgress = document.getElementById('mini-progress');
            const miniFill = document.getElementById('mini-fill');
            const miniTime = document.getElementById('mini-time');
            if (mTitle) mTitle.textContent = meta.title || 'Now playing';
            if (mBy) mBy.textContent = meta.by || '';
            if (mCover && meta.cover) mCover.src = meta.cover;
            if ('mediaSession' in navigator){
                try{
                    navigator.mediaSession.metadata = new MediaMetadata({
                        title: meta.title || 'Now playing',
                        artist: meta.by || 'LIBER',
                        artwork: meta.cover ? [{ src: meta.cover, sizes: '512x512', type: 'image/png' }] : undefined
                    });
                    navigator.mediaSession.setActionHandler('play', ()=> bg.play().catch(()=>{}));
                    navigator.mediaSession.setActionHandler('pause', ()=> bg.pause());
                    navigator.mediaSession.setActionHandler('seekbackward', ()=>{ bg.currentTime = Math.max(0, (bg.currentTime||0)-10); });
                    navigator.mediaSession.setActionHandler('seekforward', ()=>{ bg.currentTime = Math.min((bg.duration||0), (bg.currentTime||0)+10); });
                }catch(_){ }
            }
            mini.classList.add('show');
            // Hand off current media to bg player
            try{
                this.pauseAllMediaExcept(mediaEl);
                bg.src = mediaEl.currentSrc || mediaEl.src;
                if (!isNaN(mediaEl.currentTime)) bg.currentTime = mediaEl.currentTime;
                bg.play().catch(()=>{});
                mediaEl.pause();
            }catch(_){ }
            if (playBtn){ playBtn.onclick = ()=>{ if (bg.paused){ bg.play(); } else { bg.pause(); } }; }
            const syncMiniBtn = ()=> this.setPlayIcon(playBtn, !bg.paused);
            const syncProgress = ()=>{
                if (!miniFill || !miniTime) return;
                const d = Number(bg.duration || 0);
                const c = Number(bg.currentTime || 0);
                if (d > 0){
                    miniFill.style.width = `${Math.max(0, Math.min(100, (c / d) * 100))}%`;
                    miniTime.textContent = `${this.formatDuration(c)} / ${this.formatDuration(d)}`;
                } else {
                    miniFill.style.width = '0%';
                    miniTime.textContent = '0:00 / 0:00';
                }
            };
            if (miniProgress){
                miniProgress.onclick = (e)=>{
                    const rect = miniProgress.getBoundingClientRect();
                    const ratio = Math.min(1, Math.max(0, (e.clientX - rect.left) / rect.width));
                    if (Number(bg.duration) > 0) bg.currentTime = ratio * bg.duration;
                };
            }
            bg.onplay = ()=>{ syncMiniBtn(); syncProgress(); };
            bg.onpause = ()=>{ syncMiniBtn(); syncProgress(); };
            bg.ontimeupdate = syncProgress;
            bg.onloadedmetadata = syncProgress;
            bg.onended = ()=>{
                syncMiniBtn();
                if ((this._playQueueIndex + 1) < this._playQueue.length) this.playQueueIndex(this._playQueueIndex + 1);
            };
            syncMiniBtn(); syncProgress();
            if (closeBtn){ closeBtn.onclick = ()=>{ mini.classList.remove('show'); try{ bg.pause(); }catch(_){} }; }
            if (queueBtn && queuePanel){
                queueBtn.onclick = ()=>{ this.renderQueuePanel(); queuePanel.style.display = queuePanel.style.display === 'none' ? 'block' : 'none'; };
            }
            if (queueClose && queuePanel){ queueClose.onclick = ()=> queuePanel.style.display = 'none'; }
            if (addBtn){
                addBtn.onclick = ()=> this.openAddToPlaylistPopup({
                    src: bg.currentSrc || bg.src,
                    title: mTitle?.textContent || meta.title || 'Track',
                    by: mBy?.textContent || meta.by || '',
                    cover: (mCover && mCover.src) || meta.cover || ''
                });
            }

            // Build deterministic queue from current context.
            const contextRoot = mediaEl.closest('#wave-library') || mediaEl.closest('#global-feed') || mediaEl.closest('#space-feed') || mediaEl.closest('#video-library') || mediaEl.closest('#wave-results') || document;
            const nodes = Array.from(contextRoot.querySelectorAll('audio.player-media, video.player-media, .liber-lib-audio, .liber-lib-video'));
            const queue = nodes
                .map((n)=>({
                    src: n.currentSrc || n.src || '',
                    title: n.dataset?.title || n.closest('.wave-item,.video-item,.post-item')?.querySelector('.post-text')?.textContent?.trim() || 'Track',
                    by: n.dataset?.by || '',
                    cover: n.dataset?.cover || ''
                }))
                .filter((q)=> !!q.src);
            const currentSrc = bg.currentSrc || bg.src || '';
            this._playQueue = queue;
            this._playQueueIndex = Math.max(0, queue.findIndex((q)=> q.src === currentSrc));
            this.renderQueuePanel();

            // Keep deterministic behavior: do not auto-jump to random next tracks.
            if (this._onBgEnded){ try{ bg.removeEventListener('ended', this._onBgEnded); }catch(_){ } }
            this._onBgEnded = null;
        }catch(_){ }
    }

    // Activate custom players inside a container (audio/video unified controls)
    activatePlayers(root=document){
        try{
            root.querySelectorAll('.player-card').forEach(card=>{
                if (card.dataset.playerBound === '1') return;
                card.dataset.playerBound = '1';
                const media = card.querySelector('.player-media');
                const btn = card.querySelector('.btn-icon');
                const fill = card.querySelector('.progress .fill');
                let knob = card.querySelector('.progress .knob');
                if (!knob){ const k = document.createElement('div'); k.className='knob'; const bar = card.querySelector('.progress'); if (bar){ bar.appendChild(k); knob = k; } }
                const time = card.querySelector('.time');
                const fmt = (s)=>{ const m=Math.floor(s/60); const ss=Math.floor(s%60).toString().padStart(2,'0'); return `${m}:${ss}`; };
                const sync = ()=>{ if (!media.duration) return; const p=(media.currentTime/media.duration)*100; if (fill) fill.style.width = `${p}%`; if (knob){ knob.style.left = `${p}%`; } if (time) time.textContent = `${fmt(media.currentTime)} / ${fmt(media.duration)}`; };
                if (btn){
                    btn.onclick = ()=>{
                        if (media.paused){
                            this.pauseAllMediaExcept(media);
                            media.play().catch(()=>{});
                            this.setPlayIcon(btn, true);
                        } else {
                            media.pause();
                            this.setPlayIcon(btn, false);
                        }
                    };
                }
                media.addEventListener('timeupdate', sync);
                media.addEventListener('loadedmetadata', sync);
                media.addEventListener('play', ()=> this.setPlayIcon(btn, true));
                media.addEventListener('pause', ()=> this.setPlayIcon(btn, false));
                media.addEventListener('ended', ()=> this.setPlayIcon(btn, false));
                const bar = card.querySelector('.progress');
                if (bar){
                    const seekTo = (clientX)=>{
                        const rect = bar.getBoundingClientRect();
                        const ratio = Math.min(1, Math.max(0, (clientX-rect.left)/rect.width));
                        if (media.duration){ media.currentTime = ratio * media.duration; }
                        if (media.paused){ this.pauseAllMediaExcept(media); media.play().catch(()=>{}); this.setPlayIcon(btn, true); }
                    };
                    bar.addEventListener('click', (e)=> seekTo(e.clientX));
                    let dragging = false;
                    bar.addEventListener('pointerdown', (e)=>{ dragging = true; bar.setPointerCapture(e.pointerId); seekTo(e.clientX); });
                    bar.addEventListener('pointermove', (e)=>{ if (dragging) seekTo(e.clientX); });
                    bar.addEventListener('pointerup', (e)=>{ dragging = false; bar.releasePointerCapture(e.pointerId); });
                }
                // keyboard shortcuts when focused
                card.tabIndex = 0;
                card.onkeydown = (e)=>{
                    if (e.code === 'Space'){ e.preventDefault(); btn && btn.click(); }
                    if (e.code === 'ArrowLeft'){ media.currentTime = Math.max(0, media.currentTime - 5); }
                    if (e.code === 'ArrowRight'){ media.currentTime = Math.min(media.duration||0, media.currentTime + 5); }
                };

                media.addEventListener('play', ()=>{
                    const title = media.dataset?.title || card.closest('.post-item')?.querySelector('.post-text')?.textContent?.slice(0, 60) || 'Now playing';
                    const by = media.dataset?.by || card.closest('.post-item')?.querySelector('.byline')?.textContent || '';
                    this.showMiniPlayer(media, { title, by });
                });
            });
        }catch(_){ }
    }

    /**
     * Initialize the dashboard
     */
    init() {
        this.setupEventListeners();
        window.addEventListener('liber:app-shell-open', ()=> this.suspendDashboardActivity());
        window.addEventListener('liber:app-shell-close', ()=> this.resumeDashboardActivity());
        // Prevent browser-autofill from leaking login email into dashboard search fields.
        ['app-search', 'space-search', 'user-search', 'wave-search', 'video-search'].forEach((id) => {
            const el = document.getElementById(id);
            if (el){
                el.value = '';
                el.setAttribute('autocomplete', 'off');
                el.setAttribute('autocorrect', 'off');
                el.setAttribute('autocapitalize', 'off');
                el.setAttribute('spellcheck', 'false');
            }
        });
        // Harden against browser autofill on dynamically rendered fields.
        document.querySelectorAll('#dashboard input, #dashboard textarea').forEach((el)=>{
            try{
                if ((el.type || '').toLowerCase() === 'password') return;
                el.setAttribute('autocomplete', 'off');
                el.setAttribute('autocorrect', 'off');
                el.setAttribute('autocapitalize', 'off');
                el.setAttribute('spellcheck', 'false');
                if (!el.getAttribute('name')) el.setAttribute('name', 'fld-' + Math.random().toString(36).slice(2, 9));
                if (/@/.test((el.value || '').trim())) el.value = '';
            }catch(_){ }
        });
        // Extra scrub: clear browser-restored account values from non-auth inputs.
        const scrubSearchPrefill = ()=>{
            try{
                const raw = localStorage.getItem('liber_accounts');
                const accounts = raw ? JSON.parse(raw) : [];
                const bad = new Set();
                (accounts||[]).forEach(a=>{
                    if (a && typeof a.email === 'string' && a.email) bad.add(a.email.trim().toLowerCase());
                    if (a && typeof a.username === 'string' && a.username) bad.add(a.username.trim().toLowerCase());
                });
                ['app-search','space-search','user-search','wave-search','video-search'].forEach((id)=>{
                    const el = document.getElementById(id);
                    if (!el) return;
                    const v = String(el.value || '').trim();
                    if (!v) return;
                    const low = v.toLowerCase();
                    if (bad.has(low) || /@/.test(v)) el.value = '';
                });
            }catch(_){ }
        };
        scrubSearchPrefill();
        setTimeout(scrubSearchPrefill, 120);
        setTimeout(scrubSearchPrefill, 900);
        window.addEventListener('pageshow', scrubSearchPrefill);
        document.addEventListener('focusin', (e)=>{
            const el = e.target;
            if (!(el instanceof HTMLInputElement || el instanceof HTMLTextAreaElement)) return;
            try{
                if ((el.type || '').toLowerCase() === 'password') return;
                el.setAttribute('autocomplete', 'off');
                el.setAttribute('autocorrect', 'off');
                el.setAttribute('autocapitalize', 'off');
                el.setAttribute('spellcheck', 'false');
                if (/@/.test((el.value || '').trim())) el.value = '';
            }catch(_){ }
        }, true);
        try{
            const fromHash = (window.location.hash||'').replace('#','');
            const stored = localStorage.getItem('liber_last_section') || '';
            const preferred = fromHash || stored || 'apps';
            this.switchSection(preferred);
        }catch(_){ this.switchSection('apps'); }
        this.updateNavigation();
        this.handleWallETransitionToDashboard();
        // Service worker registration (best-effort)
        if ('serviceWorker' in navigator){
            const swPath = (location.pathname && location.pathname.includes('/control-panel/'))
                ? '/control-panel/sw.js'
                : '/sw.js';
            navigator.serviceWorker.register(swPath).catch(()=>{});
        }

        // Cache current user for feed actions
        (async()=>{
            try{ this.currentUser = await this.resolveCurrentUser(); }catch(_){ this.currentUser = null; }
            // Keep it fresh
            try{
                firebase.onAuthStateChanged(window.firebaseService.auth, (u)=>{
                    this.currentUser = u || null;
                    this.updateVerificationBanner();
                    this.updateNavigation();
                });
            }catch(_){ }
            this.updateVerificationBanner();
        })();

        // Global delegated handlers for feed interactions (Feed tab and Personal Space feed)
        if (!window.__LIBER_FEED_DELEGATE_BOUND__){
            window.__LIBER_FEED_DELEGATE_BOUND__ = true;
            const handler = async (e)=>{
                const inFeed = document.getElementById('global-feed')?.contains(e.target) || document.getElementById('space-feed')?.contains(e.target);
                if (!inFeed) return;
                const likeEl = e.target.closest('.like-btn');
                const commentEl = e.target.closest('.comment-btn');
                const repostEl = e.target.closest('.repost-btn');
                const actionEl = likeEl || commentEl || repostEl;
                if (!actionEl) return;
                const delegatedContainer = actionEl.closest('#global-feed, #space-feed');
                // Avoid double handling when container-level delegation is active.
                if (delegatedContainer && delegatedContainer.__postActionsDelegated) return;
                const actionsWrap = actionEl.closest('.post-actions');
                const postItem = actionEl.closest('.post-item');
                const pid = actionsWrap?.dataset.postId || postItem?.dataset.postId;
                if (!pid) return;
                let me = this.currentUser;
                if (!me){ try{ me = await this.resolveCurrentUser(); this.currentUser = me; }catch(_){ return; } }
                if (!me || !me.uid) return;
                if (likeEl){
                    try{ const likeRef = firebase.doc(window.firebaseService.db,'posts',pid,'likes', me.uid); const s=await firebase.getDoc(likeRef); if(s.exists()) await firebase.deleteDoc(likeRef); else await firebase.setDoc(likeRef,{ userId:me.uid, createdAt:new Date().toISOString() }); }catch(_){ }
                    return;
                }
                if (repostEl){
                    try{ const repRef = firebase.doc(window.firebaseService.db,'posts',pid,'reposts', me.uid); const s=await firebase.getDoc(repRef); if(s.exists()) await firebase.deleteDoc(repRef); else await firebase.setDoc(repRef,{ userId:me.uid, createdAt:new Date().toISOString() }); }catch(_){ }
                    return;
                }
                if (commentEl){
                    const tree = postItem?.querySelector('.comment-tree') || document.getElementById(`comments-${pid}`);
                    if (tree) return; // Advanced comments are handled at container/post level.
                    const text = prompt('Add comment:');
                    if (text && text.trim()){ try{ await firebase.addDoc(firebase.collection(window.firebaseService.db,'posts',pid,'comments'), { userId:me.uid, text:text.trim(), createdAt:new Date().toISOString() }); }catch(_){ } }
                    return;
                }
            };
            document.addEventListener('click', handler);
        }
    }

    /**
     * Setup dashboard event listeners
     */
    setupEventListeners() {
        // Desktop navigation buttons
        const navBtns = document.querySelectorAll('.nav-btn');
        navBtns.forEach(btn => {
            // Remove any existing event listeners by cloning
            const newBtn = btn.cloneNode(true);
            btn.parentNode.replaceChild(newBtn, btn);
            
            newBtn.addEventListener('click', () => this.switchSection(newBtn.dataset.section));
        });

        // Mobile navigation buttons
        const mobileNavBtns = document.querySelectorAll('.mobile-nav-btn');
        mobileNavBtns.forEach(btn => {
            // Remove any existing event listeners by cloning
            const newBtn = btn.cloneNode(true);
            btn.parentNode.replaceChild(newBtn, btn);
            
            if (newBtn.id === 'mobile-wall-e-btn') {
                // WALL-E button - toggle the WALL-E widget
                newBtn.addEventListener('click', () => this.toggleWallEWidget());
            } else {
                // Regular navigation buttons
                newBtn.addEventListener('click', () => this.switchSection(newBtn.dataset.section));
            }
        });

        // Logout button
        const logoutBtn = document.getElementById('logout-btn');
        if (logoutBtn && logoutBtn.parentNode) logoutBtn.parentNode.removeChild(logoutBtn);
        const spaceLogoutBtn = document.getElementById('space-logout-btn');
        if (spaceLogoutBtn) {
            const btn = spaceLogoutBtn.cloneNode(true);
            spaceLogoutBtn.parentNode.replaceChild(btn, spaceLogoutBtn);
            btn.addEventListener('click', () => authManager.logout());
        }

        // Settings form handlers
        this.setupSettingsHandlers();
        this.setupMobileSectionSwipe();

        // Prevent browser autofill in dynamic search/comment fields.
        if (!this._autofillGuardBound){
            this._autofillGuardBound = true;
            document.addEventListener('focusin', (e)=>{
                const t = e.target;
                if (!(t instanceof HTMLInputElement || t instanceof HTMLTextAreaElement)) return;
                const isTarget = t.matches('#app-search,#space-search,#user-search,#wave-search,#video-search,.reply-input');
                if (!isTarget) return;
                t.setAttribute('autocomplete', 'off');
                t.setAttribute('autocorrect', 'off');
                t.setAttribute('autocapitalize', 'off');
                t.setAttribute('spellcheck', 'false');
                if (/@/.test((t.value || '').trim())) t.value = '';
            }, true);
        }

        // Simple account switcher UI: fill a dropdown if exists
        const switcher = document.getElementById('account-switcher');
        const addAcct = document.getElementById('add-account-btn');
        if (switcher){
            try{
                const raw = localStorage.getItem('liber_accounts');
                const accounts = raw ? JSON.parse(raw) : [];
                switcher.innerHTML = '<option value="">Switch account…</option>' + accounts.map(a=>`<option value="${a.uid||''}">${a.username||a.email}</option>`).join('');
                // Toggle visibility based on availability
                if (Array.isArray(accounts) && accounts.length > 0) { switcher.style.display = ''; }
                else { switcher.style.display = ''; }
                if (addAcct){ addAcct.style.display = 'inline-block'; addAcct.onclick = ()=>{ try{ window.location.hash = 'login'; if (window.authManager && typeof window.authManager.switchTab==='function'){ window.authManager.switchTab('login'); } }catch(_){ } }; }
                // Open popup on click instead of native select UX
                switcher.addEventListener('mousedown', (e)=>{ e.preventDefault(); this.showAccountSwitcherPopup(); });
            }catch(_){ }
        }

        // Ensure a visible switch-accounts button in header near logout/search
        try{
            const btn = document.getElementById('switch-accounts-btn');
            if (btn && !btn._bound){ btn._bound = true; btn.addEventListener('click', ()=> this.showAccountSwitcherPopup()); }
            const spaceBtn = document.getElementById('space-switch-btn');
            if (spaceBtn && !spaceBtn._bound){ spaceBtn._bound = true; spaceBtn.addEventListener('click', ()=> this.showAccountSwitcherPopup()); }
        }catch(_){ }

        // Account switcher popup UI
        this.showAccountSwitcherPopup = async ()=>{
            try{
                const raw = localStorage.getItem('liber_accounts');
                const accounts = raw ? JSON.parse(raw) : [];
                const me = await window.firebaseService.getCurrentUser();
                const currentUid = me && me.uid;
                // Create / reuse layer
                let layer = document.getElementById('account-switcher-layer');
                if (!layer){
                    layer = document.createElement('div');
                    layer.id = 'account-switcher-layer';
                    layer.style.cssText = 'position:fixed;top:60px;right:20px;background:#0f1116;border:1px solid var(--border-color);border-radius:12px;box-shadow:0 10px 30px rgba(0,0,0,.4);z-index:1000;min-width:260px;max-width:320px;max-height:60vh;overflow:auto;padding:8px';
                    document.body.appendChild(layer);
                }
                const rows = accounts.map(a=>{
                    const checked = (a.uid===currentUid) ? '<span style="color:#22c55e">✔</span>' : '';
                    const avatar = a.avatarUrl || 'images/default-bird.png';
                    const label = a.username || a.email || a.uid;
                    return `<div class="acct-row" data-uid="${a.uid||''}" style="display:flex;gap:10px;align-items:center;padding:8px;border-radius:8px;cursor:pointer">
                              <img src="${avatar}" style="width:28px;height:28px;border-radius:50%;object-fit:cover"> 
                              <div style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis">${label}</div>
                              ${checked}
                            </div>`;
                }).join('');
                layer.innerHTML = `<div style="padding:6px 8px;font-weight:600;border-bottom:1px solid var(--border-color)">Switch account</div>
                                    <div>${rows || '<div style="padding:12px;opacity:.8">No saved accounts</div>'}</div>
                                    <div style="padding:8px;border-top:1px solid var(--border-color)"><button id="acct-add" class="btn btn-secondary" style="width:100%"><i class="fas fa-user-plus"></i> Add account</button></div>`;

                layer.querySelectorAll('.acct-row').forEach(row=>{
                    row.onclick = async ()=>{
                        const uid = row.getAttribute('data-uid'); if (!uid) return;
                        try{
                            const fnErrors = [];
                            const callSwitchFn = async (name, payload)=>{
                                const fs = window.firebaseService;
                                const preferred = (()=>{ try{ return localStorage.getItem('liber_functions_region') || 'europe-west1'; }catch(_){ return 'europe-west1'; } })();
                                const regions = [preferred, 'europe-west1'].filter((v,i,a)=> v && a.indexOf(v)===i);
                                const errs = [];
                                for (const r of regions){
                                    try{
                                        if (window.firebaseModular?.httpsCallable && fs?.app){
                                            const f = (fs.functionsByRegion && fs.functionsByRegion[r])
                                                ? fs.functionsByRegion[r]
                                                : (window.firebaseModular.getFunctions ? window.firebaseModular.getFunctions(fs.app, r) : null);
                                            if (!f) throw new Error('functions unavailable');
                                            const callable = window.firebaseModular.httpsCallable(f, name);
                                            const res = await callable(payload);
                                            return (res && res.data) || null;
                                        }
                                    }catch(e){
                                        const code = e?.code || '';
                                        const msg = e?.message || '';
                                        errs.push(`${name}@${r}[callable]: ${code}${msg ? ` (${msg})` : ''}`.trim());
                                    }
                                }
                                throw new Error(errs.join(' | '));
                            };
                            const callFnMaybe = async (name, payload)=>{
                                try{ return await callSwitchFn(name, payload); }
                                catch(e){
                                    fnErrors.push(`${name}: ${e?.message || e}`);
                                    return null;
                                }
                            };
                            const deviceId = this.getOrCreateDeviceId();
                            // Refresh current account seed token before attempting a switch.
                            try{
                                const ua = navigator.userAgent || '';
                                const seed = await callFnMaybe('saveSwitchToken', { deviceId, ua });
                                const tok = seed && seed.token;
                                if (tok){
                                    const meNow = await window.firebaseService.getCurrentUser();
                                    if (meNow && meNow.uid){
                                        const rawT = localStorage.getItem('liber_switch_tokens');
                                        const mapT = rawT ? JSON.parse(rawT) : {};
                                        mapT[meNow.uid] = tok;
                                        localStorage.setItem('liber_switch_tokens', JSON.stringify(mapT));
                                    }
                                }
                            }catch(_){ }
                            let tokenMap = JSON.parse(localStorage.getItem('liber_switch_tokens')||'{}');
                            let token = tokenMap[uid];
                            // If selecting current account, just close
                            if (uid === currentUid){ try{ layer.remove(); }catch(_){ } return; }
                            let isAdmin = false;
                            try{
                                const meData = await window.firebaseService.getUserData(currentUid);
                                isAdmin = String(meData?.role || 'user') === 'admin';
                            }catch(_){ isAdmin = false; }
                            let customToken = null;
                            // Cache-proof same-device switch (requires both accounts seeded on this device).
                            const byDevice = await callFnMaybe('switchToByDevice', { uid, deviceId });
                            customToken = byDevice?.customToken || null;
                            // Secondary fallback: direct admin-issued custom token.
                            // Server enforces admin role, so it's safe to attempt unconditionally.
                            if (!customToken && isAdmin){
                                const adminSw = await callFnMaybe('adminSwitchToUser', { uid });
                                customToken = adminSw?.customToken || null;
                            }
                            if (!customToken){
                                // Do NOT reload on failed switch. Route to login with one-time prefill.
                                const acc = (accounts||[]).find(a=> (a.uid===uid));
                                const prefill = (acc && (acc.email || acc.username)) || '';
                                try{
                                    if (prefill){
                                        sessionStorage.setItem('liber_switch_prefill_email', prefill);
                                        localStorage.setItem('liber_prefill_email', prefill);
                                    }
                                }catch(_){ }
                                try{ layer.remove(); }catch(_){ }
                                const authScreen = document.getElementById('auth-screen');
                                const dashboard = document.getElementById('dashboard');
                                if (authScreen && dashboard){ dashboard.classList.add('hidden'); authScreen.classList.remove('hidden'); }
                                if (window.authManager && typeof window.authManager.switchTab==='function'){ window.authManager.switchTab('login'); }
                                const emailInput = document.getElementById('loginUsername'); if (emailInput && prefill){ emailInput.value = prefill; emailInput.focus(); }
                                if (fnErrors.length){
                                    console.warn('Instant switch call errors:', fnErrors.join(' | '));
                                }
                                const onlyByDeviceDenied = fnErrors.length > 0 && fnErrors.every((s)=> /switchToByDevice/i.test(s) && /permission-denied/i.test(s));
                                if (onlyByDeviceDenied){
                                    this.showInfo('Target account needs one login on this device first, then instant switch will work.');
                                } else {
                                    this.showError('Instant switch unavailable. Log in once for this account on this device.');
                                }
                                return;
                            }
                            if (firebase.signInWithCustomToken) {
                                await firebase.signInWithCustomToken(window.firebaseService.auth, customToken);
                            } else if (window.firebaseModular && window.firebaseModular.signInWithCustomToken) {
                                await window.firebaseModular.signInWithCustomToken(window.firebaseService.auth, customToken);
                            } else {
                                throw new Error('signInWithCustomToken API not available');
                            }
                            window.location.reload();
                        }catch(e){ console.error('Switch failed', e); this.showError('Switch failed'); }
                    };
                });
                const addBtn = document.getElementById('acct-add');
                if (addBtn){ addBtn.onclick = ()=>{ try{ layer.remove();
                    // Show auth screen and select login tab
                    const authScreen = document.getElementById('auth-screen');
                    const dashboard = document.getElementById('dashboard');
                    if (authScreen && dashboard){ dashboard.classList.add('hidden'); authScreen.classList.remove('hidden'); }
                    if (window.authManager && typeof window.authManager.switchTab==='function'){ window.authManager.switchTab('login'); }
                    // Focus email field
                    const email = document.getElementById('loginUsername'); if (email){ email.focus(); }
                }catch(_){ window.location.hash='login'; } };
                }

                // Close when clicking outside
                const closer = (e)=>{ if (!layer.contains(e.target) && e.target !== switcher){ document.removeEventListener('mousedown', closer); try{ layer.remove(); }catch(_){ } } };
                setTimeout(()=> document.addEventListener('mousedown', closer), 0);
            }catch(_){ }
        };

        // Force reload all (admin only)
        const frAll = document.getElementById('force-reload-all-btn');
        if (frAll){
            frAll.onclick = async ()=>{
                const currentUser = await window.firebaseService.getCurrentUser();
                const me = await window.firebaseService.getUserData(currentUser.uid) || {};
                if ((me.role||'user') !== 'admin'){ return this.showError('Admin only'); }
                const ok = await this.showConfirm('Force sign-out and reload for all users? This will invalidate caches on next load.');
                if (!ok) return;
                try{
                    const ref = firebase.doc(window.firebaseService.db, 'admin', 'broadcast');
                    await firebase.setDoc(ref, { action: 'forceReload', at: new Date().toISOString(), nonce: Math.random().toString(36).slice(2) }, { merge: true });
                    this.showSuccess('Broadcast sent');
                }catch(_){ this.showError('Failed to broadcast'); }
            };
        }
    }

    setupMobileSectionSwipe() {
        const host = document.querySelector('.dashboard-content');
        if (!host || host._swipeBound) return;
        host._swipeBound = true;

        let startX = 0;
        let startY = 0;
        let startTs = 0;

        host.addEventListener('touchstart', (e) => {
            if (!e.touches || !e.touches.length) return;
            startX = e.touches[0].clientX;
            startY = e.touches[0].clientY;
            startTs = Date.now();
        }, { passive: true });

        host.addEventListener('touchend', (e) => {
            if (!e.changedTouches || !e.changedTouches.length) return;
            const endX = e.changedTouches[0].clientX;
            const endY = e.changedTouches[0].clientY;
            const dx = endX - startX;
            const dy = endY - startY;
            const dt = Date.now() - startTs;
            // Horizontal swipe only: fast and clearly stronger than vertical movement.
            if (dt > 600 || Math.abs(dx) < 60 || Math.abs(dx) < Math.abs(dy) * 1.35) return;

            const navBtns = Array.from(document.querySelectorAll('.mobile-nav-btn[data-section]'));
            const sections = navBtns
                .map(btn => btn.dataset.section)
                .filter(Boolean)
                .filter((section, index, arr) => arr.indexOf(section) === index);
            if (!sections.length) return;

            const current = this.currentSection || 'apps';
            const idx = sections.indexOf(current);
            if (idx < 0) return;

            if (dx < 0 && idx < sections.length - 1) this.switchSection(sections[idx + 1]);
            if (dx > 0 && idx > 0) this.switchSection(sections[idx - 1]);
        }, { passive: true });
    }
    /**
     * Load Personal Space
     */
    async loadSpace(){
        if (this._dashboardSuspended) return;
        try{
            if (!(window.firebaseService && window.firebaseService.isFirebaseAvailable())){ this.scheduleSpaceRetry(); return; }
            const user = await this.resolveCurrentUserWithRetry(2600);
            if (!user || !user.uid){ this.scheduleSpaceRetry(); return; }
            if (this._spaceRetryTimer){ clearTimeout(this._spaceRetryTimer); this._spaceRetryTimer = null; }
            const data = await window.firebaseService.getUserData(user.uid) || {};
            const unameEl = document.getElementById('space-username');
            const moodEl = document.getElementById('space-mood');
            const avatarEl = document.getElementById('space-avatar');
            const feedTitle = document.getElementById('space-feed-title');
            if (unameEl) unameEl.value = data.username || user.email || '';
            if (moodEl) moodEl.value = data.mood || '';
            if (avatarEl) avatarEl.src = data.avatarUrl || 'images/default-bird.png';
            if (feedTitle) feedTitle.textContent = 'My Feed';

            const saveBtn = document.getElementById('space-save-profile');
            if (saveBtn){
                saveBtn.onclick = async ()=>{
                    const newMood = (document.getElementById('space-mood').value||'').trim();
                    await window.firebaseService.updateUserProfile(user.uid, { mood: newMood });
                    const avInput = document.getElementById('space-avatar-input');
                    if (avInput && avInput.files && avInput.files[0] && firebase.getStorage){
                        try{
                            const s = firebase.getStorage();
                            const file = avInput.files[0];
                            const ext = (file.name.split('.').pop()||'jpg').toLowerCase();
                            const path = `avatars/${user.uid}/avatar.${ext}`;
                            const r = firebase.ref(s, path);
                            await firebase.uploadBytes(r, file, { contentType: file.type || 'image/jpeg' });
                            const url = await firebase.getDownloadURL(r);
                            await window.firebaseService.updateUserProfile(user.uid, { avatarUrl: url });
                            if (avatarEl) avatarEl.src = url;
                        }catch(e){ /* ignore avatar errors */ }
                    }
                    this.showSuccess('Profile saved');
                };
            }

            const postBtn = document.getElementById('space-post-btn');
            if (postBtn){
                postBtn.onclick = async ()=>{
                    const text = (document.getElementById('space-post-text').value||'').trim();
                    const mediaInput = document.getElementById('space-post-media');
                    const visibility = 'private';
                    let mediaUrl = '';
                    let media = [];
                    if (mediaInput && mediaInput.files && mediaInput.files.length && firebase.getStorage){
                        try{
                            const s = firebase.getStorage();
                            const postIdRef = firebase.doc(firebase.collection(window.firebaseService.db, 'posts'));
                            for (let i=0;i<Math.min(mediaInput.files.length, 5); i++){
                                const file = mediaInput.files[i];
                                const ext = (file.name.split('.').pop()||'jpg').toLowerCase();
                                const path = `posts/${user.uid}/${postIdRef.id}/media_${i}.${ext}`;
                                const r = firebase.ref(s, path);
                                await firebase.uploadBytes(r, file, { contentType: file.type||'application/octet-stream' });
                                const url = await firebase.getDownloadURL(r);
                                media.push(url);
                            }
                            mediaUrl = media[0] || '';
                            const payload = { id: postIdRef.id, authorId: user.uid, text, mediaUrl, media, visibility, createdAt: new Date().toISOString(), createdAtTS: firebase.serverTimestamp() };
                            await firebase.setDoc(postIdRef, payload);
                            document.getElementById('space-post-text').value='';
                            mediaInput.value='';
                            this.showSuccess('Posted');
                            this.loadMyPosts(user.uid);
                            return;
                        }catch(e){ /* fallback to text-only below */ }
                    }
                    if (!text){ this.showError('Add some text or attach media'); return; }
                    const newRef = firebase.doc(firebase.collection(window.firebaseService.db, 'posts'));
                    await firebase.setDoc(newRef, { id: newRef.id, authorId: user.uid, text, visibility, createdAt: new Date().toISOString(), createdAtTS: firebase.serverTimestamp() });
                    document.getElementById('space-post-text').value='';
                    if (mediaInput) mediaInput.value='';
                    this.showSuccess('Posted');
                    this.loadMyPosts(user.uid);
                };
                // media preview chips
                const mediaInput = document.getElementById('space-post-media');
                const previews = document.getElementById('space-media-previews');
                if (mediaInput && previews && !mediaInput._previewsBound){
                    mediaInput._previewsBound = true;
                    mediaInput.addEventListener('change', ()=>{
                        previews.innerHTML='';
                        const files = Array.from(mediaInput.files||[]).slice(0,5);
                        files.forEach((f, idx)=>{
                            const chip = document.createElement('div'); chip.className='chip';
                            const remove = document.createElement('div'); remove.className='remove'; remove.innerHTML='<i class="fas fa-times"></i>';
                            remove.onclick = ()=>{
                                const dt = new DataTransfer();
                                Array.from(mediaInput.files).forEach((ff,i)=>{ if (i!==idx) dt.items.add(ff); });
                                mediaInput.files = dt.files; chip.remove();
                            };
                            chip.appendChild(remove);
                            if (f.type.startsWith('image/')){
                                const img = document.createElement('img'); img.src = URL.createObjectURL(f); chip.appendChild(img);
                            } else if (f.type.startsWith('video/')){
                                const v = document.createElement('video'); v.src = URL.createObjectURL(f); v.muted=true; chip.appendChild(v);
                            } else if (f.type.startsWith('audio/')){
                                const i = document.createElement('i'); i.className='fas fa-music'; chip.appendChild(i);
                                const span = document.createElement('span'); span.textContent = f.name; chip.appendChild(span);
                            } else {
                                const i = document.createElement('i'); i.className='fas fa-paperclip'; chip.appendChild(i);
                                const span = document.createElement('span'); span.textContent = f.name; chip.appendChild(span);
                            }
                            previews.appendChild(chip);
                        });
                    });
                }
            }

            const spaceSearch = document.getElementById('space-search');
            if (spaceSearch){
                const ensureLayer = ()=>{
                    let layer = document.getElementById('space-search-results-layer');
                    if (!layer){
                        layer = document.createElement('div');
                        layer.id = 'space-search-results-layer';
                        layer.style.cssText = 'position:fixed; z-index: 10050; display:none; max-height:320px; overflow:auto; border:1px solid var(--border-color); background: var(--secondary-bg); border-radius:10px; box-shadow:0 8px 24px rgba(0,0,0,.25); left:0; width:auto; max-width:100%;';
                        document.body.appendChild(layer);
                    }
                    return layer;
                };
                const positionLayer = (layer)=>{
                    const r = spaceSearch.getBoundingClientRect();
                    const width = Math.max(280, Math.min(420, r.width));
                    layer.style.left = '0px';
                    layer.style.top = `${Math.round(r.bottom+6)}px`;
                    layer.style.width = 'auto';
                    layer.style.maxWidth = '100%';
                };
                const renderResults = (layer, users)=>{
                    layer.innerHTML = '';
                    const ul = document.createElement('ul');
                    ul.style.listStyle='none'; ul.style.margin='0'; ul.style.padding='8px';
                    (users||[]).slice(0,10).forEach(u=>{
                        const li = document.createElement('li');
                        li.style.cssText='display:flex;flex-wrap:wrap;gap:8px;align-items:center;padding:8px;border-radius:8px;cursor:pointer;';
                        li.onmouseenter = ()=> li.style.background='var(--hover-bg, rgba(255,255,255,.06))';
                        li.onmouseleave = ()=> li.style.background='transparent';
                        li.innerHTML = `<img class="avatar" src="${u.avatarUrl||'images/default-bird.png'}" style="width:32px;height:32px;border-radius:50%;object-fit:cover">`+
                                       `<div style="min-width:0"><div class="uname" style="font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${u.username||''}</div>`+
                                       `<div class="email" style="opacity:.8;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${u.email||''}</div></div>`;
                        li.onclick = (ev)=>{ ev.stopPropagation(); layer.style.display='none'; this.showUserPreviewModal(u); };
                        ul.appendChild(li);
                    });
                    layer.appendChild(ul);
                };
                const hideOnOutside = (e)=>{
                    const layer = document.getElementById('space-search-results-layer');
                    if (!layer) return;
                    if (e.target===spaceSearch || spaceSearch.contains(e.target) || layer.contains(e.target)) return;
                    layer.style.display='none';
                };
                window.addEventListener('scroll', ()=>{ const l=document.getElementById('space-search-results-layer'); if(l&&l.style.display!=='none') positionLayer(l); }, true);
                window.addEventListener('resize', ()=>{ const l=document.getElementById('space-search-results-layer'); if(l&&l.style.display!=='none') positionLayer(l); });
                document.addEventListener('click', hideOnOutside);

                spaceSearch.addEventListener('input', async (e)=>{
                    const term = (e.target.value||'').trim().toLowerCase();
                    const layer = ensureLayer();
                    if (!term){ layer.style.display='none'; layer.innerHTML=''; return; }
                    const users = await window.firebaseService.searchUsers(term);
                    renderResults(layer, users);
                    positionLayer(layer);
                    layer.style.display = (users && users.length) ? 'block':'none';
                });
            }

            // Use the class method implementation of showUserPreviewModal.

            // Follow/unfollow bar
            const feedCard = document.getElementById('space-feed-card');
            if (feedCard && !document.getElementById('follow-bar')){
                const followBar = document.createElement('div');
                followBar.id = 'follow-bar';
                followBar.style.marginBottom = '10px';
                followBar.innerHTML = `<button id="follow-btn" class="btn btn-primary" style="display:none">Follow</button>
                                       <button id="unfollow-btn" class="btn btn-secondary" style="display:none">Unfollow</button>`;
                feedCard.insertBefore(followBar, feedCard.firstChild);
            }

            // Initial feed load: show my posts in Personal Space
            this.loadMyPosts(user.uid);
            this.loadConnectionsForSpace();
        }catch(e){ console.error('loadSpace error', e); }
    }

    async loadMyPosts(uid){
        const feed = document.getElementById('space-feed'); if (!feed) return;
        const feedTitle = document.getElementById('space-feed-title');
        feed.innerHTML='';
        feed.__useAdvancedComments = true;
        try{
            const meUser = await this.resolveCurrentUser();
            const meProfile = (meUser && meUser.uid) ? ((await window.firebaseService.getUserData(meUser.uid)) || {}) : {};
            // Try ordered query for my posts (both public and private)
            let snap;
            try{
                const q = firebase.query(
                    firebase.collection(window.firebaseService.db,'posts'),
                    firebase.where('authorId','==', uid),
                    firebase.orderBy('createdAtTS','desc'),
                    firebase.limit(50)
                );
                snap = await firebase.getDocs(q);
            }catch{
                const q2 = firebase.query(firebase.collection(window.firebaseService.db,'posts'), firebase.where('authorId','==', uid));
                snap = await firebase.getDocs(q2);
                snap = { docs: snap.docs.sort((a,b)=> (b.data()?.createdAtTS?.toMillis?.()||0) - (a.data()?.createdAtTS?.toMillis?.()||0) || new Date((b.data()||{}).createdAt||0) - new Date((a.data()||{}).createdAt||0)), forEach: (cb)=> snap.docs.forEach(cb) };
            }
            const postsById = new Map();
            snap.forEach(d=>{
                const p = d.data();
                if (p && p.id) postsById.set(p.id, p);
            });
            // Legacy admin fallback: some old posts were not keyed by authorId.
            if (postsById.size === 0 && meUser && meUser.uid){
                try{
                    let allSnap;
                    try{
                        const qAll = firebase.query(
                            firebase.collection(window.firebaseService.db,'posts'),
                            firebase.orderBy('createdAtTS','desc'),
                            firebase.limit(300)
                        );
                        allSnap = await firebase.getDocs(qAll);
                    }catch(_){
                        allSnap = await firebase.getDocs(firebase.collection(window.firebaseService.db,'posts'));
                    }
                    const myEmail = String(meUser.email || '').toLowerCase();
                    const myName = String(meProfile.username || meUser.email || '').toLowerCase();
                    allSnap.forEach(d=>{
                        const p = d.data() || {};
                        if (!p.id) return;
                        const authorId = String(p.authorId || '');
                        const authorEmail = String(p.authorEmail || '').toLowerCase();
                        const authorName = String(p.authorName || '').toLowerCase();
                        if (authorId === meUser.uid || (myEmail && authorEmail === myEmail) || (myName && authorName === myName)){
                            postsById.set(p.id, p);
                        }
                    });
                }catch(_){ }
            }
            // Include public posts that current user reposted so they appear in My Feed.
            try{
                let recent;
                try{
                    const qRecent = firebase.query(
                        firebase.collection(window.firebaseService.db,'posts'),
                        firebase.where('visibility','==','public'),
                        firebase.orderBy('createdAtTS','desc'),
                        firebase.limit(80)
                    );
                    recent = await firebase.getDocs(qRecent);
                }catch{
                    const qRecent2 = firebase.query(
                        firebase.collection(window.firebaseService.db,'posts'),
                        firebase.where('visibility','==','public')
                    );
                    recent = await firebase.getDocs(qRecent2);
                }
                for (const d of recent.docs || []){
                    const p = d.data();
                    if (!p || !p.id || postsById.has(p.id)) continue;
                    try{
                        const repRef = firebase.doc(window.firebaseService.db,'posts', p.id, 'reposts', uid);
                        const rep = await firebase.getDoc(repRef);
                        if (rep.exists()){
                            postsById.set(p.id, { ...p, _isRepostInMyFeed: true });
                        }
                    }catch(_){ }
                }
            }catch(_){ }
            const mergedPosts = Array.from(postsById.values()).sort((a,b)=>
                (b.createdAtTS?.toMillis?.()||0) - (a.createdAtTS?.toMillis?.()||0) ||
                new Date(b.createdAt||0) - new Date(a.createdAt||0)
            );
            mergedPosts.forEach((p)=>{
                const div = document.createElement('div');
                div.className = 'post-item';
                div.style.cssText = 'border:1px solid var(--border-color);border-radius:12px;padding:12px;margin:10px 0;background:var(--secondary-bg)';
                const by = p.authorName ? `<div class=\"byline\" style=\"display:flex;align-items:center;gap:8px;margin:4px 0\"><img src=\"${p.coverUrl||p.thumbnailUrl||'images/default-bird.png'}\" alt=\"cover\" style=\"width:20px;height:20px;border-radius:50%;object-fit:cover\"><span style=\"font-size:12px;color:#aaa\">by ${(p.authorName||'').replace(/</g,'&lt;')}</span></div>` : '';
                const media = (p.media || p.mediaUrl) ? this.renderPostMedia(p.media || p.mediaUrl) : '';
                const repostBadge = p._isRepostInMyFeed ? `<div style="font-size:12px;opacity:.8;margin-bottom:4px"><i class="fas fa-retweet"></i> Reposted</div>` : '';
                div.innerHTML = `${repostBadge}<div>${(p.text||'').replace(/</g,'&lt;')}</div>${by}${media}
                                 <div class=\"post-actions\" data-post-id=\"${p.id}\" data-author=\"${p.authorId||''}\" style=\"margin-top:8px;display:flex;flex-wrap:wrap;gap:14px;align-items:center\">\n                                   <i class=\"fas fa-heart like-btn\" title=\"Like\" style=\"cursor:pointer\"></i>\n                                   <span class=\"likes-count\"></span>\n                                   <i class=\"fas fa-comment comment-btn\" title=\"Comments\" style=\"cursor:pointer\"></i>\n                                   <i class=\"fas fa-retweet repost-btn\" title=\"Repost\" style=\"cursor:pointer\"></i>\n                                   <span class=\"reposts-count\"></span>\n                                   <button class=\"btn btn-secondary visibility-btn\">${p.visibility==='public'?'Make Private':'Make Public'}</button>\n                                 </div>\n                                 <div class=\"comment-tree\" id=\"comments-${p.id}\" style=\"display:none\"></div>`;
                feed.appendChild(div);
                // double-tap like on post content
                const contentArea = div.querySelector('.post-text') || div;
                let lastTap = 0;
                contentArea.addEventListener('touchend', async ()=>{
                    const now = Date.now();
                    if (now - lastTap < 350){
                        const likeBtn = div.querySelector('.like-btn');
                        likeBtn && likeBtn.click();
                        const pulse = document.createElement('i'); pulse.className='fas fa-heart dbl-like-pulse'; div.appendChild(pulse); setTimeout(()=> pulse.remove(), 700);
                    }
                    lastTap = now;
                }, { passive: true });
                this.activatePlayers(div);
            });
            // Bind actions for my posts
            if (!meUser || !meUser.uid) return;
            document.querySelectorAll('#space-section .post-actions').forEach(async (pa)=>{
                const postId = pa.getAttribute('data-post-id');
                const likeBtn = pa.querySelector('.like-btn');
                const commentBtn = pa.querySelector('.comment-btn');
                const repostBtn = pa.querySelector('.repost-btn');
                const visBtn = pa.querySelector('.visibility-btn');
                const authorId = pa.getAttribute('data-author') || '';
                const likesCount = pa.querySelector('.likes-count');
                const rCount = pa.querySelector('.reposts-count');
                const s = await window.firebaseService.getPostStats(postId); likesCount.textContent = `${s.likes||0}`; if (rCount) rCount.textContent = `${s.reposts||0}`;
                if (repostBtn){ if (await window.firebaseService.hasReposted(postId, meUser.uid)){ repostBtn.classList.add('active'); } }
                if (await window.firebaseService.hasLiked(postId, meUser.uid)){ likeBtn.classList.add('active'); likeBtn.style.color = '#ff4d4f'; }
                likeBtn.onclick = async ()=>{
                    const liked = await window.firebaseService.hasLiked(postId, meUser.uid);
                    if (liked){ await window.firebaseService.unlikePost(postId, meUser.uid); likeBtn.classList.remove('active'); likeBtn.style.color=''; }
                    else { await window.firebaseService.likePost(postId, meUser.uid); likeBtn.classList.add('active'); likeBtn.style.color='#ff4d4f'; }
                    const s2 = await window.firebaseService.getPostStats(postId); likesCount.textContent = `${s2.likes||0}`; if (rCount) rCount.textContent = `${s2.reposts||0}`;
                };
                if (repostBtn){
                    repostBtn.onclick = async ()=>{
                        try{
                            const me = await this.resolveCurrentUser();
                            if (!me || !me.uid) return;
                            const already = await window.firebaseService.hasReposted(postId, me.uid);
                            if (already){
                                await window.firebaseService.unRepost(postId, me.uid);
                                repostBtn.classList.remove('active');
                            } else {
                                await window.firebaseService.repost(postId, me.uid);
                                repostBtn.classList.add('active');
                            }
                            const s3 = await window.firebaseService.getPostStats(postId); if (rCount) rCount.textContent = `${s3.reposts||0}`;
                            this.loadMyPosts(uid);
                        }catch(_){ }
                    };
                }
                commentBtn.onclick = async ()=>{
                    const tree = pa.closest('.post-item')?.querySelector('.comment-tree') || document.getElementById(`comments-${postId}`);
                    if (!tree) return;
                    const forceOpen = tree.dataset.forceOpen === '1';
                    tree.dataset.forceOpen = '0';
                    if (tree.style.display === 'none' || forceOpen){ tree.style.display='block'; } else { tree.style.display='none'; return; }
                    tree.innerHTML = '';
                    const currentLimit = Number(tree.dataset.limit || 5);
                    const comments = await window.firebaseService.getComments(postId, currentLimit);
                    // Build threaded structure
                    const map = new Map();
                    comments.forEach(c=> map.set(c.id, { ...c, children: [] }));
                    const roots = [];
                    comments.forEach(c=>{ if (c.parentId && map.has(c.parentId)){ map.get(c.parentId).children.push(map.get(c.id)); } else { roots.push(map.get(c.id)); } });
                    const renderNode = (node, container)=>{
                        const item = document.createElement('div');
                        item.className = 'comment-item';
                        item.innerHTML = `<div class="comment-text"><i class=\"fas fa-comment\"></i> ${(node.text||'').replace(/</g,'&lt;')}</div>
                        <div class="comment-actions" data-comment-id="${node.id}" data-author="${node.authorId}" style="display:flex;gap:8px;margin-top:4px">
                          <span class="reply-btn" style="cursor:pointer"><i class=\"fas fa-reply\"></i> Reply</span>
                          <i class="fas fa-edit edit-comment-btn" title="Edit" style="cursor:pointer"></i>
                          <i class="fas fa-trash delete-comment-btn" title="Delete" style="cursor:pointer"></i>
                        </div>`;
                        container.appendChild(item);
                        // Reply input (hidden until click)
                        const replyBox = document.createElement('div');
                        replyBox.style.cssText = 'margin:6px 0 0 0; display:none';
                        replyBox.innerHTML = `<input type="text" class="reply-input" placeholder="Reply..." style="width:100%">`;
                        item.appendChild(replyBox);
                        item.querySelector('.reply-btn').onclick = ()=>{ replyBox.style.display = replyBox.style.display==='none'?'block':'none'; if (replyBox.style.display==='block'){ const inp=replyBox.querySelector('.reply-input'); inp && inp.focus(); } };
                        const inp = replyBox.querySelector('.reply-input');
                        if (inp){ inp.onkeydown = async (e)=>{ if (e.key==='Enter' && inp.value.trim()){ const meU = await this.resolveCurrentUser(); if (!meU || !meU.uid) return; await window.firebaseService.addComment(postId, meU.uid, inp.value.trim(), node.id); inp.value=''; tree.dataset.forceOpen = '1'; await commentBtn.onclick(); } }; }
                        if (node.children && node.children.length){
                            const sub = document.createElement('div'); sub.className='comment-tree'; item.appendChild(sub);
                            node.children.forEach(ch=> renderNode(ch, sub));
                        }
                    };
                    roots.reverse().forEach(n=> renderNode(n, tree));
                    if (comments.length >= currentLimit){
                        const more = document.createElement('button');
                        more.className = 'btn btn-secondary';
                        more.style.marginTop = '8px';
                        more.textContent = 'See more comments';
                        more.onclick = async ()=>{ tree.dataset.limit = String(currentLimit + 10); tree.dataset.forceOpen = '1'; await commentBtn.onclick(); };
                        tree.appendChild(more);
                    }
                    // Inline add comment (top-level)
                    const addWrap = document.createElement('div');
                    addWrap.style.cssText = 'margin-top:8px';
                    addWrap.innerHTML = `<input type="text" class="reply-input" id="add-comment-${postId}" placeholder="Add a comment..." style="width:100%">`;
                    tree.appendChild(addWrap);
                    const addInp = document.getElementById(`add-comment-${postId}`);
                    if (addInp){ addInp.onkeydown = async (e)=>{ if (e.key==='Enter' && addInp.value.trim()){ const meU = await this.resolveCurrentUser(); if (!meU || !meU.uid) return; await window.firebaseService.addComment(postId, meU.uid, addInp.value.trim(), null); addInp.value=''; tree.dataset.forceOpen = '1'; await commentBtn.onclick(); } }; }
                    // bind comment edit/delete
                    tree.querySelectorAll('.comment-actions').forEach(act=>{
                        const cid = act.getAttribute('data-comment-id');
                        const author = act.getAttribute('data-author');
                        const canEdit = author === meUser.uid;
                        const eb = act.querySelector('.edit-comment-btn');
                        const db = act.querySelector('.delete-comment-btn');
                        if (!canEdit){ eb && (eb.style.display='none'); db && (db.style.display='none'); }
                        if (canEdit){
                            if (eb){ eb.onclick = async ()=>{
                                const newText = prompt('Edit comment:');
                                if (newText===null) return;
                                await window.firebaseService.updateComment(postId, cid, newText.trim());
                                this.loadFeed(uid, titleName);
                            }; }
                            if (db){ db.onclick = async ()=>{
                                if (!confirm('Delete this comment?')) return;
                                await window.firebaseService.deleteComment(postId, cid);
                                this.loadFeed(uid, titleName);
                            }; }
                        }
                    });
                };
                if (visBtn){
                    if (authorId !== meUser.uid){ visBtn.style.display = 'none'; }
                    visBtn.onclick = async ()=>{
                        if (authorId !== meUser.uid) return;
                        try{
                            const ref = firebase.doc(window.firebaseService.db, 'posts', postId);
                            const doc = await firebase.getDoc(ref);
                            const p = doc.data()||{};
                            const next = p.visibility==='public' ? 'private' : 'public';
                            await firebase.updateDoc(ref, { visibility: next, updatedAt: new Date().toISOString() });
                            visBtn.textContent = next==='public' ? 'Make Private' : 'Make Public';
                        }catch(_){ }
                    };
                }
            });
            if (feedTitle) feedTitle.textContent = 'My Feed';
        }catch(_){ }
    }

    async loadConnectionsForSpace(){
        try{
            if (!(window.firebaseService && window.firebaseService.isFirebaseAvailable())) return;
            const me = await this.resolveCurrentUser(); if (!me || !me.uid) return;
            const list = document.getElementById('space-connections-list'); if (!list) return;
            list.innerHTML = '';
            let snap;
            try{
                const q = firebase.query(
                    firebase.collection(window.firebaseService.db,'connections',me.uid,'peers'),
                    firebase.orderBy('updatedAt','desc'),
                    firebase.limit(100)
                );
                snap = await firebase.getDocs(q);
            }catch{
                snap = await firebase.getDocs(firebase.collection(window.firebaseService.db,'connections',me.uid,'peers'));
            }
            const rows = [];
            snap.forEach(d=> rows.push({ id:d.id, ...d.data() }));
            rows.sort((a,b)=> new Date(b.updatedAt||b.connectedAt||0) - new Date(a.updatedAt||a.connectedAt||0));
            if (!rows.length){
                list.innerHTML = '<li style="opacity:.8">No connections yet.</li>';
                return;
            }
            rows.forEach((r)=>{
                const li = document.createElement('li');
                li.style.cssText = 'display:flex;gap:10px;align-items:center;padding:8px;border-radius:10px;cursor:pointer;';
                const avatar = r.avatarUrl || 'images/default-bird.png';
                const uname = (r.username || r.email || r.uid || r.id || 'User').toString().replace(/</g,'&lt;');
                const email = (r.email || '').toString().replace(/</g,'&lt;');
                const status = String(r.status || 'connected');
                const requestedBy = String(r.requestedBy || '');
                const requestedTo = String(r.requestedTo || '');
                const incomingPending = status === 'pending' && requestedTo === me.uid;
                const outgoingPending = status === 'pending' && requestedBy === me.uid;
                li.innerHTML = `<img src="${avatar}" style="width:34px;height:34px;border-radius:50%;object-fit:cover"><div style="min-width:0;flex:1"><div style="font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${uname}</div><div style="opacity:.8;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${email}</div></div>`;
                const actions = document.createElement('div');
                actions.style.cssText = 'display:flex;align-items:center;gap:6px;margin-left:auto';
                if (incomingPending){
                    const confirmBtn = document.createElement('button');
                    confirmBtn.className = 'btn btn-primary';
                    confirmBtn.textContent = 'Confirm';
                    confirmBtn.onclick = async (ev)=>{
                        ev.stopPropagation();
                        try{
                            confirmBtn.disabled = true;
                            const peerUid = r.uid || r.id;
                            const now = new Date().toISOString();
                            const myRef = firebase.doc(window.firebaseService.db,'connections',me.uid,'peers',peerUid);
                            const peerRef = firebase.doc(window.firebaseService.db,'connections',peerUid,'peers',me.uid);
                            await firebase.setDoc(myRef, { status:'connected', connectedAt:now, updatedAt:now }, { merge:true });
                            await firebase.setDoc(peerRef, { status:'connected', connectedAt:now, updatedAt:now }, { merge:true });
                            await this.loadConnectionsForSpace();
                        }catch(_){ this.showError('Failed to confirm connection'); }
                        finally{ confirmBtn.disabled = false; }
                    };
                    const denyBtn = document.createElement('button');
                    denyBtn.className = 'btn btn-secondary';
                    denyBtn.textContent = 'Deny';
                    denyBtn.onclick = async (ev)=>{
                        ev.stopPropagation();
                        try{
                            denyBtn.disabled = true;
                            const peerUid = r.uid || r.id;
                            const myRef = firebase.doc(window.firebaseService.db,'connections',me.uid,'peers',peerUid);
                            const peerRef = firebase.doc(window.firebaseService.db,'connections',peerUid,'peers',me.uid);
                            await firebase.deleteDoc(myRef).catch(()=>null);
                            await firebase.deleteDoc(peerRef).catch(()=>null);
                            await this.loadConnectionsForSpace();
                        }catch(_){ this.showError('Failed to deny request'); }
                        finally{ denyBtn.disabled = false; }
                    };
                    actions.appendChild(confirmBtn);
                    actions.appendChild(denyBtn);
                } else {
                    const badge = document.createElement('span');
                    badge.style.cssText = 'font-size:12px;opacity:.85';
                    badge.textContent = outgoingPending ? 'Pending' : 'Connected';
                    actions.appendChild(badge);
                }
                li.appendChild(actions);
                li.onclick = ()=> this.showUserPreviewModal({ uid: r.uid || r.id, username: r.username, email: r.email, avatarUrl: r.avatarUrl });
                list.appendChild(li);
            });
        }catch(_){ }
    }

    async loadGlobalFeed(){
        if (this._dashboardSuspended) return;
        try{
            const feedEl = document.getElementById('global-feed');
            const suggEl = document.getElementById('global-suggestions');
            if (!feedEl) return;
            // Prevent snapshot listener accumulation across re-renders.
            if (this._postActionUnsubsByContainer && this._postActionUnsubsByContainer.get(feedEl)) {
                this._postActionUnsubsByContainer.get(feedEl).forEach((u) => { try { u(); } catch (_) {} });
                this._postActionUnsubsByContainer.set(feedEl, []);
            }
            feedEl.innerHTML = '';
            // Prevent prompt-based fallback comment UI in delegated handlers.
            feedEl.__useAdvancedComments = true;
            // Recent public posts
            const snap = await firebase.getDocs(firebase.query(
                firebase.collection(window.firebaseService.db,'posts'),
                firebase.where('visibility','==','public')
            ));
            const list = [];
            snap.forEach((d) => {
                const row = d.data() || {};
                list.push({ ...row, id: row.id || d.id });
            });
            list.sort((a,b)=> (b.createdAtTS?.toMillis?.()||0) - (a.createdAtTS?.toMillis?.()||0) || new Date(b.createdAt||0) - new Date(a.createdAt||0));
            list.slice(0,20).forEach(p=>{
                const div = document.createElement('div');
                div.className = 'post-item';
                div.dataset.postId = p.id;
                div.style.cssText = 'border:1px solid var(--border-color);border-radius:12px;padding:12px;margin:10px 0;background:var(--secondary-bg)';
                const media = (p.media || p.mediaUrl) ? this.renderPostMedia(p.media || p.mediaUrl) : '';
                div.innerHTML = `<div class="post-text">${(p.text||'').replace(/</g,'&lt;')}</div>${media}
                                 <div class="post-actions" data-post-id="${p.id}" data-author="${p.authorId}" style="margin-top:8px;display:flex;flex-wrap:wrap;gap:14px;align-items:center">
                                   <i class="fas fa-heart like-btn" title="Like" style="cursor:pointer"></i>
                                   <span class="likes-count"></span>
                                   <i class="fas fa-comment comment-btn" title="Comments" style="cursor:pointer"></i>
                                   <span class="comments-count"></span>
                                   <i class="fas fa-retweet repost-btn" title="Repost" style="cursor:pointer"></i>
                                   <span class="reposts-count"></span>
                                   <i class="fas fa-ellipsis-h post-menu" title="More" style="cursor:pointer"></i>
                                   <i class="fas fa-edit edit-post-btn" title="Edit" style="cursor:pointer"></i>
                                   <i class="fas fa-trash delete-post-btn" title="Delete" style="cursor:pointer"></i>
                                 </div>
                                 <div class="comment-tree" id="comments-${p.id}" style="display:none"></div>`;
                feedEl.appendChild(div);
            });
            if (suggEl){
                try{
                    const trending = await window.firebaseService.getTrendingPosts('', 10);
                    suggEl.innerHTML = trending.map(tp=>`<div class="post-item" style="border:1px solid var(--border-color);border-radius:12px;padding:10px;margin:8px 0">${(tp.text||'').replace(/</g,'&lt;')}</div>`).join('');
                }catch(_){
                    suggEl.innerHTML = '';
                }
            }
            this.activatePostActions(feedEl);  // Activate actions after rendering (delegated like/comment/repost)
            // owner-only controls and advanced comments UI parity with personal space
            try{
                const meUser = await this.resolveCurrentUserWithRetry(1200);
                const myUid = meUser?.uid || '';
                feedEl.querySelectorAll('.post-actions').forEach(pa=>{
                    const postId = pa.getAttribute('data-post-id');
                    const postAuthor = pa.getAttribute('data-author');
                    const canEditPost = !!myUid && postAuthor === myUid;
                    const editBtn = pa.querySelector('.edit-post-btn');
                    const delBtn = pa.querySelector('.delete-post-btn');
                    const menuBtn = pa.querySelector('.post-menu');
                    const visBtn = pa.querySelector('.visibility-btn');
                    const commentBtn = pa.querySelector('.comment-btn');
                    if (!canEditPost){ if (editBtn) editBtn.style.display='none'; if (delBtn) delBtn.style.display='none'; if (visBtn) visBtn.style.display='none'; }
                    if (menuBtn){ menuBtn.onclick = async ()=>{ try{ const loc = `${location.origin}${location.pathname}#post-${postId}`; await navigator.clipboard.writeText(loc); this.showSuccess('Post link copied'); }catch(_){ this.showError('Failed to copy'); } }; }
                    if (visBtn && canEditPost){ visBtn.onclick = async ()=>{ try{ const ref = firebase.doc(window.firebaseService.db,'posts', postId); const doc = await firebase.getDoc(ref); const p=doc.data()||{}; const next = p.visibility==='public'?'private':'public'; await firebase.updateDoc(ref, { visibility: next, updatedAt:new Date().toISOString() }); visBtn.textContent = next==='public'?'Make Private':'Make Public'; }catch(_){ } }; }
                    if (editBtn && canEditPost){ editBtn.onclick = async ()=>{ const container = pa.closest('.post-item'); const textDiv = container && container.querySelector('.post-text'); const current = textDiv ? textDiv.textContent : ''; const newText = prompt('Edit post:', current); if (newText===null) return; await window.firebaseService.updatePost(postId, { text: newText.trim() }); this.loadGlobalFeed(); }; }
                    if (delBtn && canEditPost){ delBtn.onclick = async ()=>{ if (!confirm('Delete this post?')) return; await window.firebaseService.deletePost(postId); this.loadGlobalFeed(); }; }
                    if (commentBtn){ commentBtn.onclick = async ()=>{
                        const tree = pa.closest('.post-item')?.querySelector('.comment-tree') || document.getElementById(`comments-${postId}`); if (!tree) return;
                        const forceOpen = tree.dataset.forceOpen === '1';
                        tree.dataset.forceOpen = '0';
                        if (tree.style.display === 'none' || forceOpen){ tree.style.display='block'; } else { tree.style.display='none'; return; }
                        tree.innerHTML = '';
                        const currentLimit = Number(tree.dataset.limit || 5);
                        const comments = await window.firebaseService.getComments(postId, currentLimit);
                        const map = new Map(); comments.forEach(c=> map.set(c.id, { ...c, children: [] }));
                        const roots = []; comments.forEach(c=>{ if (c.parentId && map.has(c.parentId)){ map.get(c.parentId).children.push(map.get(c.id)); } else { roots.push(map.get(c.id)); } });
                        const renderNode = (node, container)=>{
                            const item = document.createElement('div');
                            item.className = 'comment-item';
                            item.innerHTML = `<div class=\"comment-text\"><i class=\"fas fa-comment\"></i> ${(node.text||'').replace(/</g,'&lt;')}</div>
                            <div class=\"comment-actions\" data-comment-id=\"${node.id}\" data-author=\"${node.authorId}\" style=\"display:flex;gap:8px;margin-top:4px\">
                              <span class=\"reply-btn\" style=\"cursor:pointer\"><i class=\"fas fa-reply\"></i> Reply</span>
                              <i class=\"fas fa-edit edit-comment-btn\" title=\"Edit\" style=\"cursor:pointer\"></i>
                              <i class=\"fas fa-trash delete-comment-btn\" title=\"Delete\" style=\"cursor:pointer\"></i>
                            </div>`;
                            container.appendChild(item);
                            const replyBox = document.createElement('div'); replyBox.style.cssText='margin:6px 0 0 0; display:none'; replyBox.innerHTML = `<input type=\"text\" class=\"reply-input\" placeholder=\"Reply...\" style=\"width:100%\">`; item.appendChild(replyBox);
                            item.querySelector('.reply-btn').onclick = ()=>{ replyBox.style.display = replyBox.style.display==='none'?'block':'none'; if (replyBox.style.display==='block'){ const inp=replyBox.querySelector('.reply-input'); inp && inp.focus(); } };
                            const inp = replyBox.querySelector('.reply-input'); if (inp){ inp.onkeydown = async (e)=>{ if (e.key==='Enter' && inp.value.trim()){ const meU = await this.resolveCurrentUser(); if (!meU || !meU.uid) return; await window.firebaseService.addComment(postId, meU.uid, inp.value.trim(), node.id); inp.value=''; tree.dataset.forceOpen = '1'; await commentBtn.onclick(); } }; }
                            if (node.children && node.children.length){ const sub = document.createElement('div'); sub.className='comment-tree'; item.appendChild(sub); node.children.forEach(ch=> renderNode(ch, sub)); }
                        };
                        roots.reverse().forEach(n=> renderNode(n, tree));
                        if (comments.length >= currentLimit){
                            const more = document.createElement('button');
                            more.className = 'btn btn-secondary';
                            more.style.marginTop = '8px';
                            more.textContent = 'See more comments';
                            more.onclick = async ()=>{
                                tree.dataset.limit = String(currentLimit + 10);
                                tree.dataset.forceOpen = '1';
                                await commentBtn.onclick();
                            };
                            tree.appendChild(more);
                        }
                        const addWrap = document.createElement('div'); addWrap.style.cssText='margin-top:8px'; addWrap.innerHTML = `<input type=\"text\" class=\"reply-input\" id=\"add-comment-${postId}\" placeholder=\"Add a comment...\" style=\"width:100%\">`; tree.appendChild(addWrap);
                        const addInp = document.getElementById(`add-comment-${postId}`); if (addInp){ addInp.onkeydown = async (e)=>{ if (e.key==='Enter' && addInp.value.trim()){ const meU = await this.resolveCurrentUser(); if (!meU || !meU.uid) return; await window.firebaseService.addComment(postId, meU.uid, addInp.value.trim(), null); addInp.value=''; tree.dataset.forceOpen = '1'; await commentBtn.onclick(); } }; }
                        tree.querySelectorAll('.comment-actions').forEach(act=>{
                            const cid = act.getAttribute('data-comment-id'); const author = act.getAttribute('data-author'); const canEdit = !!myUid && author === myUid; const eb = act.querySelector('.edit-comment-btn'); const db = act.querySelector('.delete-comment-btn');
                            if (!canEdit){ eb && (eb.style.display='none'); db && (db.style.display='none'); }
                            if (canEdit){ if (eb){ eb.onclick = async ()=>{ const newText = prompt('Edit comment:'); if (newText===null) return; await window.firebaseService.updateComment(postId, cid, newText.trim()); this.loadGlobalFeed(); }; }
                                if (db){ db.onclick = async ()=>{ if (!confirm('Delete this comment?')) return; await window.firebaseService.deleteComment(postId, cid); this.loadGlobalFeed(); }; }
                            }
                        });
                    }; }
                });
            }catch(_){ }
            this.activatePlayers(feedEl);
        }catch(_){ }
    }

    async loadFeed(uid, titleName){
        const feed = document.getElementById('space-feed');
        const feedTitle = document.getElementById('space-feed-title');
        if (!feed) return;
        feed.innerHTML = '';
        try{
            const meUser = await this.resolveCurrentUser();
            const meUid = meUser?.uid || '';
            const following = meUid ? await window.firebaseService.getFollowingIds(meUid) : [];
            const fb = document.getElementById('follow-btn');
            const ub = document.getElementById('unfollow-btn');
            if (fb && ub){
                if (meUid && uid !== meUid){
                    const isFollowing = following.includes(uid);
                    fb.style.display = isFollowing? 'none':'inline-block';
                    ub.style.display = isFollowing? 'inline-block':'none';
                    fb.onclick = async ()=>{ if (!meUid) return; await window.firebaseService.followUser(meUid, uid); this.loadFeed(uid, titleName); };
                    ub.onclick = async ()=>{ if (!meUid) return; await window.firebaseService.unfollowUser(meUid, uid); this.loadFeed(uid, titleName); };
                } else { fb.style.display='none'; ub.style.display='none'; }
            }
            let snap;
            try {
                const q = firebase.query(
                    firebase.collection(window.firebaseService.db,'posts'),
                    firebase.where('authorId','==', uid),
                    firebase.where('visibility','==','public'),
                    firebase.orderBy('createdAtTS','desc'),
                    firebase.limit(50)
                );
                snap = await firebase.getDocs(q);
            } catch {
                const q2 = firebase.query(firebase.collection(window.firebaseService.db,'posts'), firebase.where('authorId','==', uid));
                snap = await firebase.getDocs(q2);
                // Normalize to similar interface
                snap = { docs: snap.docs.sort((a,b)=> (b.data()?.createdAtTS?.toMillis?.()||0) - (a.data()?.createdAtTS?.toMillis?.()||0) || new Date((b.data()||{}).createdAt||0) - new Date((a.data()||{}).createdAt||0)), forEach: (cb)=> snap.docs.forEach(cb) };
            }
            snap.forEach(d=>{
                const p = d.data();
                const div = document.createElement('div');
                div.className = 'post-item';
                div.style.cssText = 'border:1px solid var(--border-color);border-radius:12px;padding:12px;margin:10px 0;background:var(--secondary-bg)';
                const media = (p.media || p.mediaUrl) ? this.renderPostMedia(p.media || p.mediaUrl) : '';
                div.innerHTML = `<div>${(p.text||'').replace(/</g,'&lt;')}</div>${media}
                                 <div class="post-actions" data-post-id="${p.id}" data-author="${p.authorId}" style="margin-top:8px;display:flex;flex-wrap:wrap;gap:14px;align-items:center">
                                   <i class="fas fa-heart like-btn" title="Like" style="cursor:pointer"></i>
                                   <span class="likes-count"></span>
                                   <i class="fas fa-comment comment-btn" title="Comments" style="cursor:pointer"></i>
                                   <i class="fas fa-retweet repost-btn" title="Repost" style="cursor:pointer"></i>
                                   <span class="reposts-count"></span>
                                   <i class="fas fa-ellipsis-h post-menu" title="More" style="cursor:pointer"></i>
                                   <i class="fas fa-edit edit-post-btn" title="Edit" style="cursor:pointer"></i>
                                   <i class="fas fa-trash delete-post-btn" title="Delete" style="cursor:pointer"></i>
                                 </div>
                                 <div class="comment-tree" id="comments-${p.id}" style="display:none"></div>`;
                feed.appendChild(div);
            });
            // Bind like/comment actions
            const me2 = await this.resolveCurrentUser();
            const me2Uid = me2?.uid || '';
            feed.querySelectorAll('.post-actions').forEach(async (pa)=>{
                const postId = pa.getAttribute('data-post-id');
                const likeBtn = pa.querySelector('.like-btn');
                const commentBtn = pa.querySelector('.comment-btn');
                const repostBtn = pa.querySelector('.repost-btn');
                const editBtn = pa.querySelector('.edit-post-btn');
                const delBtn = pa.querySelector('.delete-post-btn');
                const menuBtn = pa.querySelector('.post-menu');
                const likesCount = pa.querySelector('.likes-count');
                const rCount = pa.querySelector('.reposts-count');
                const stats = await window.firebaseService.getPostStats(postId); likesCount.textContent = `${stats.likes||0}`; if (rCount) rCount.textContent = `${stats.reposts||0}`;
                if (me2Uid){
                    if (await window.firebaseService.hasLiked(postId, me2Uid)){ likeBtn.classList.add('active'); likeBtn.style.color = '#ff4d4f'; }
                }
                likeBtn.onclick = async ()=>{
                    if (!me2Uid) return;
                    const liked = await window.firebaseService.hasLiked(postId, me2Uid);
                    if (liked){ await window.firebaseService.unlikePost(postId, me2Uid); likeBtn.classList.remove('active'); likeBtn.style.color=''; }
                    else { await window.firebaseService.likePost(postId, me2Uid); likeBtn.classList.add('active'); likeBtn.style.color='#ff4d4f'; }
                    const s = await window.firebaseService.getPostStats(postId); likesCount.textContent = `${s.likes||0}`; if (rCount) rCount.textContent = `${s.reposts||0}`;
                };
                if (repostBtn){
                    repostBtn.onclick = async ()=>{
                        try{
                            const me = await window.firebaseService.getCurrentUser();
                            const already = await window.firebaseService.hasReposted(postId, me.uid);
                            if (already){ await window.firebaseService.unRepost(postId, me.uid); repostBtn.classList.remove('active'); }
                            else { await window.firebaseService.repost(postId, me.uid); repostBtn.classList.add('active'); }
                            const s3 = await window.firebaseService.getPostStats(postId); if (rCount) rCount.textContent = `${s3.reposts||0}`;
                        }catch(_){ }
                    };
                }
                if (menuBtn){
                    menuBtn.onclick = async ()=>{
                        try{
                            const loc = `${location.origin}${location.pathname}#post-${postId}`;
                            await navigator.clipboard.writeText(loc);
                            this.showSuccess('Post link copied');
                        }catch(_){ this.showError('Failed to copy'); }
                    };
                }
                commentBtn.onclick = async ()=>{
                    const tree = pa.closest('.post-item')?.querySelector('.comment-tree') || document.getElementById(`comments-${postId}`);
                    if (!tree) return;
                    const forceOpen = tree.dataset.forceOpen === '1';
                    tree.dataset.forceOpen = '0';
                    if (tree.style.display === 'none' || forceOpen){ tree.style.display='block'; } else { tree.style.display='none'; return; }
                    tree.innerHTML = '';
                    const currentLimit = Number(tree.dataset.limit || 5);
                    const comments = await window.firebaseService.getComments(postId, currentLimit);
                    // Build threaded structure
                    const map = new Map();
                    comments.forEach(c=> map.set(c.id, { ...c, children: [] }));
                    const roots = [];
                    comments.forEach(c=>{ if (c.parentId && map.has(c.parentId)){ map.get(c.parentId).children.push(map.get(c.id)); } else { roots.push(map.get(c.id)); } });
                    const renderNode = (node, container)=>{
                        const item = document.createElement('div');
                        item.className = 'comment-item';
                        item.innerHTML = `<div class="comment-text"><i class=\"fas fa-comment\"></i> ${(node.text||'').replace(/</g,'&lt;')}</div>
                        <div class="comment-actions" data-comment-id="${node.id}" data-author="${node.authorId}" style="display:flex;gap:8px;margin-top:4px">
                          <span class="reply-btn" style="cursor:pointer"><i class=\"fas fa-reply\"></i> Reply</span>
                          <i class="fas fa-edit edit-comment-btn" title="Edit" style="cursor:pointer"></i>
                          <i class="fas fa-trash delete-comment-btn" title="Delete" style="cursor:pointer"></i>
                        </div>`;
                        container.appendChild(item);
                        // Reply input (hidden until click)
                        const replyBox = document.createElement('div');
                        replyBox.style.cssText = 'margin:6px 0 0 0; display:none';
                        replyBox.innerHTML = `<input type="text" class="reply-input" placeholder="Reply..." style="width:100%">`;
                        item.appendChild(replyBox);
                        item.querySelector('.reply-btn').onclick = ()=>{ replyBox.style.display = replyBox.style.display==='none'?'block':'none'; if (replyBox.style.display==='block'){ const inp=replyBox.querySelector('.reply-input'); inp && inp.focus(); } };
                        const inp = replyBox.querySelector('.reply-input');
                        if (inp){ inp.onkeydown = async (e)=>{ if (e.key==='Enter' && inp.value.trim()){ const meU = await this.resolveCurrentUser(); if (!meU || !meU.uid) return; await window.firebaseService.addComment(postId, meU.uid, inp.value.trim(), node.id); inp.value=''; tree.dataset.forceOpen = '1'; await commentBtn.onclick(); } }; }
                        if (node.children && node.children.length){
                            const sub = document.createElement('div'); sub.className='comment-tree'; item.appendChild(sub);
                            node.children.forEach(ch=> renderNode(ch, sub));
                        }
                    };
                    roots.reverse().forEach(n=> renderNode(n, tree));
                    if (comments.length >= currentLimit){
                        const more = document.createElement('button');
                        more.className = 'btn btn-secondary';
                        more.style.marginTop = '8px';
                        more.textContent = 'See more comments';
                        more.onclick = async ()=>{ tree.dataset.limit = String(currentLimit + 10); tree.dataset.forceOpen = '1'; await commentBtn.onclick(); };
                        tree.appendChild(more);
                    }
                    // bind comment edit/delete
                    tree.querySelectorAll('.comment-actions').forEach(act=>{
                        const cid = act.getAttribute('data-comment-id');
                        const author = act.getAttribute('data-author');
                        const canEdit = author === meUser.uid;
                        const eb = act.querySelector('.edit-comment-btn');
                        const db = act.querySelector('.delete-comment-btn');
                        if (!canEdit){ eb && (eb.style.display='none'); db && (db.style.display='none'); }
                        if (canEdit){
                            if (eb){ eb.onclick = async ()=>{
                                const newText = prompt('Edit comment:');
                                if (newText===null) return;
                                await window.firebaseService.updateComment(postId, cid, newText.trim());
                                this.loadFeed(uid, titleName);
                            }; }
                            if (db){ db.onclick = async ()=>{
                                if (!confirm('Delete this comment?')) return;
                                await window.firebaseService.deleteComment(postId, cid);
                                this.loadFeed(uid, titleName);
                            }; }
                        }
                    });
                };
                // bind post edit/delete (owner only)
                const postAuthor = pa.getAttribute('data-author');
                const canEditPost = postAuthor === meUser.uid;
                if (!canEditPost){ if (editBtn) editBtn.style.display='none'; if (delBtn) delBtn.style.display='none'; }
                if (canEditPost){
                    if (editBtn){ editBtn.onclick = async ()=>{
                        const container = pa.closest('.post-item');
                        const textDiv = container && container.querySelector('.post-text');
                        const current = textDiv ? textDiv.textContent : '';
                        const newText = prompt('Edit post:', current);
                        if (newText===null) return;
                        await window.firebaseService.updatePost(postId, { text: newText.trim() });
                        this.loadFeed(uid, titleName);
                    }; }
                    if (delBtn){ delBtn.onclick = async ()=>{
                        if (!confirm('Delete this post?')) return;
                        await window.firebaseService.deletePost(postId);
                        this.loadFeed(uid, titleName);
                    }; }
                }
            });
            // Removed legacy unreachable duplicate feed renderer block.
            // Suggestions (simple): recent posts by others
            const sugg = document.getElementById('space-suggestions');
            if (sugg){
                try{
                    const qAny = firebase.query(firebase.collection(window.firebaseService.db,'posts'), firebase.orderBy('createdAt','desc'), firebase.limit(10));
                    const s2 = await firebase.getDocs(qAny);
                    const cards = [];
                    s2.forEach(dd=>{ const pp = dd.data(); if (pp.authorId !== uid){ cards.push(`<div class="post-item" style="border:1px solid var(--border-color);border-radius:12px;padding:10px;margin:8px 0">${(pp.text||'').replace(/</g,'&lt;')}</div>`); } });
                    sugg.innerHTML = cards.length ? `<h4>Suggestions</h4>${cards.join('')}` : '';
                }catch(_){ sugg.innerHTML = ''; }
            }
            if (feedTitle) feedTitle.textContent = titleName ? `${titleName}'s Feed` : 'My Feed';
        }catch(e){ /* ignore */ }
    }

    async openUserSpace(uid, displayName){
        try{
            // Show banner with basic info and follow controls
            const banner = document.getElementById('space-view-banner');
            const nameEl = document.getElementById('view-user-name');
            const moodEl = document.getElementById('view-user-mood');
            const avatarEl = document.getElementById('view-user-avatar');
            const fBtn = document.getElementById('view-follow-btn');
            const ufBtn = document.getElementById('view-unfollow-btn');

            if (banner){ banner.style.display='block'; }
            if (nameEl){ nameEl.textContent = displayName || ''; }
            try{
                const profile = await window.firebaseService.getUserData(uid);
                if (moodEl) moodEl.textContent = (profile && profile.mood) ? profile.mood : '';
                if (avatarEl && profile && profile.avatarUrl) avatarEl.src = profile.avatarUrl;
            }catch(_){ }

            try{
                const me = await window.firebaseService.getCurrentUser();
                const following = await window.firebaseService.getFollowingIds(me.uid);
                const isFollowing = (following||[]).includes(uid);
                if (fBtn && ufBtn){
                    fBtn.style.display = isFollowing ? 'none' : 'inline-block';
                    ufBtn.style.display = isFollowing ? 'inline-block' : 'none';
                    fBtn.onclick = async ()=>{ await window.firebaseService.followUser(me.uid, uid); this.openUserSpace(uid, displayName); };
                    ufBtn.onclick = async ()=>{ await window.firebaseService.unfollowUser(me.uid, uid); this.openUserSpace(uid, displayName); };
                }
            }catch(_){ }

            // Load only public posts of selected user
            const feed = document.getElementById('space-feed');
            const feedTitle = document.getElementById('space-feed-title');
            if (feed) feed.innerHTML='';
            if (feedTitle) feedTitle.textContent = `${displayName||'User'}'s Feed`;
            try{
                let snap;
                try{
                    const q = firebase.query(
                        firebase.collection(window.firebaseService.db,'posts'),
                        firebase.where('authorId','==', uid),
                        firebase.where('visibility','==','public'),
                        firebase.orderBy('createdAtTS','desc'),
                        firebase.limit(50)
                    );
                    snap = await firebase.getDocs(q);
                }catch{
                    const q2 = firebase.query(firebase.collection(window.firebaseService.db,'posts'), firebase.where('authorId','==', uid), firebase.where('visibility','==','public'));
                    snap = await firebase.getDocs(q2);
                    snap = { docs: snap.docs.sort((a,b)=> (b.data()?.createdAtTS?.toMillis?.()||0) - (a.data()?.createdAtTS?.toMillis?.()||0) || new Date((b.data()||{}).createdAt||0) - new Date((a.data()||{}).createdAt||0)), forEach: (cb)=> snap.docs.forEach(cb) };
                }
                snap.forEach(d=>{
                    const p = d.data();
                    const div = document.createElement('div');
                    div.className = 'post-item';
                    div.style.cssText = 'border:1px solid var(--border-color);border-radius:12px;padding:12px;margin:10px 0;background:var(--secondary-bg)';
                    const media = (p.media || p.mediaUrl) ? this.renderPostMedia(p.media || p.mediaUrl) : '';
                    div.innerHTML = `<div>${(p.text||'').replace(/</g,'&lt;')}</div>${media}`;
                    if (feed) feed.appendChild(div);
                });
            }catch(_){ }
        }catch(_){ }
    }

    /**
     * Switch between dashboard sections
     */
    switchSection(section) {
        if (this._dashboardSuspended) return;
        this.updateVerificationBanner();
        // Update desktop navigation
        const navBtns = document.querySelectorAll('.nav-btn');
        navBtns.forEach(btn => btn.classList.remove('active'));
        const desktopActiveBtn = document.querySelector(`.nav-btn[data-section="${section}"]`);
        if (desktopActiveBtn) {
            desktopActiveBtn.classList.add('active');
        }

        // Update mobile navigation
        const mobileNavBtns = document.querySelectorAll('.mobile-nav-btn');
        mobileNavBtns.forEach(btn => btn.classList.remove('active'));
        const mobileActiveBtn = document.querySelector(`.mobile-nav-btn[data-section="${section}"]`);
        if (mobileActiveBtn) {
            mobileActiveBtn.classList.add('active');
        }

        // Update content sections
        const contentSections = document.querySelectorAll('.content-section');
        contentSections.forEach(sectionEl => sectionEl.classList.remove('active'));
        const target = document.getElementById(`${section}-section`);
        if (target) target.classList.add('active');

        this.currentSection = section;

        // Persist last section and sync hash
        try{ localStorage.setItem('liber_last_section', section); if (window.location.hash !== `#${section}`) window.location.hash = section; }catch(_){ }

        // Load section-specific content
        switch (section) {
            
            case 'apps':
                if (window.appsManager) {
                    window.appsManager.loadApps();
                }
                break;
            case 'space':
                this.loadSpace();
                break;
            case 'feed':
                this.loadGlobalFeed();
                // Ensure players activate after section switch
                setTimeout(()=> this.activatePlayers(document.getElementById('global-feed')), 50);
                break;
            case 'waveconnect':
                this.loadWaveConnect();
                // Setup tabs
                const tA = document.getElementById('wave-tab-audio');
                const tV = document.getElementById('wave-tab-video');
                const pA = document.getElementById('wave-audio-pane');
                const pV = document.getElementById('wave-video-pane');
                if (tA && tV && pA && pV && !tA._bound){
                    tA._bound = tV._bound = true;
                    const activate = (isVideo)=>{
                        if (isVideo){ tV.classList.add('active'); tA.classList.remove('active'); pV.style.display='block'; pA.style.display='none'; this.loadVideoHost(); }
                        else { tA.classList.add('active'); tV.classList.remove('active'); pA.style.display='block'; pV.style.display='none'; this.loadWaveConnect(); }
                    };
                    tA.onclick = ()=> activate(false);
                    tV.onclick = ()=> activate(true);
                }
                break;
            case 'profile':
                this.loadProfile();
                break;
            case 'users':
                if (window.usersManager) {
                    window.usersManager.loadUsers();
                }
                break;
            case 'settings':
                this.loadSettings();
                break;
        }
    }

    async loadWaveConnect(){
        try{
            const me = await window.firebaseService.getCurrentUser();
            const lib = document.getElementById('wave-library');
            const res = document.getElementById('wave-results');
            const upBtn = document.getElementById('wave-upload-btn');
            if (upBtn && !upBtn._bound){
                upBtn._bound = true;
                upBtn.onclick = async ()=>{
                    try{
                        const file = document.getElementById('wave-file').files[0];
                        const coverFile = document.getElementById('wave-cover')?.files?.[0] || null;
                        if (!file){ return this.showError('Select an audio file'); }
                        // Accept common audio types; cap size to 50 MB
                        const okTypes = ['audio/mpeg','audio/mp3','audio/wav','audio/x-wav','audio/ogg','audio/webm','audio/aac','audio/m4a'];
                        if (!okTypes.includes((file.type||'').toLowerCase())){
                            console.warn('WaveConnect: unexpected type', file.type);
                        }
                        if (file.size > 50*1024*1024){ return this.showError('Max 50 MB'); }
                        const title = (document.getElementById('wave-title').value||file.name).trim();
                        const s = firebase.getStorage();
                        const ref = firebase.ref(s, `wave/${me.uid}/${Date.now()}_${file.name.replace(/[^a-zA-Z0-9._-]/g,'_')}`);
                        const task = firebase.uploadBytesResumable(ref, file, { contentType: (file.type||'audio/mpeg') });
                        await task;
                        const url = await firebase.getDownloadURL(ref);
                        let authorName = (await window.firebaseService.getUserData(me.uid))?.username || me.email || 'Unknown';
                        let coverUrl = (await window.firebaseService.getUserData(me.uid))?.avatarUrl || '';
                        if (coverFile){
                            const cRef = firebase.ref(s, `wave-covers/${me.uid}/${Date.now()}_${coverFile.name.replace(/[^a-zA-Z0-9._-]/g,'_')}`);
                            await firebase.uploadBytes(cRef, coverFile, { contentType: coverFile.type || 'image/jpeg' });
                            coverUrl = await firebase.getDownloadURL(cRef);
                        }
                        const docRef = firebase.doc(firebase.collection(window.firebaseService.db, 'wave'));
                        await firebase.setDoc(docRef, { id: docRef.id, ownerId: me.uid, title, url, createdAt: new Date().toISOString(), authorId: me.uid, authorName, coverUrl });
                        this.showSuccess('Uploaded');
                        this.renderWaveLibrary(me.uid);
                        this.renderPlaylists();
                    }catch(e){ console.error('Wave upload failed', e); this.showError('Upload failed'); }
                };
            }
            const search = document.getElementById('wave-search');
            const newPlaylistBtn = document.getElementById('wave-new-playlist-btn');
            if (search && !search._bound){
                search._bound = true;
                search.oninput = async (e)=>{
                    const qStr = (e.target.value||'').toLowerCase();
                    if (!res) return;
                    res.innerHTML='';
                    if (!qStr) return;
                    const snap = await firebase.getDocs(firebase.collection(window.firebaseService.db,'wave'));
                    snap.forEach(d=>{ const w=d.data(); if ((w.title||'').toLowerCase().includes(qStr)){ res.appendChild(this.renderWaveItem(w)); } });
                };
            }
            if (newPlaylistBtn && !newPlaylistBtn._bound){
                newPlaylistBtn._bound = true;
                newPlaylistBtn.onclick = ()=>{
                    const name = String(prompt('Playlist name:') || '').trim();
                    if (!name) return;
                    const playlists = this.getPlaylists();
                    playlists.push({ id: `pl_${Date.now()}`, name, items: [] });
                    this.savePlaylists(playlists);
                    this.renderPlaylists();
                };
            }
            await this.renderWaveLibrary(me.uid);
            await this.renderPlaylists();
        }catch(_){ }
    }

    async renderWaveLibrary(uid){
        const lib = document.getElementById('wave-library'); if (!lib) return;
        lib.innerHTML = '';
        try{
            const q = firebase.query(firebase.collection(window.firebaseService.db,'wave'), firebase.where('ownerId','==', uid), firebase.orderBy('createdAt','desc'), firebase.limit(50));
            const snap = await firebase.getDocs(q);
            snap.forEach(d=> lib.appendChild(this.renderWaveItem(d.data())));
        }catch{
            const q2 = firebase.query(firebase.collection(window.firebaseService.db,'wave'), firebase.where('ownerId','==', uid));
            const s2 = await firebase.getDocs(q2); s2.forEach(d=> lib.appendChild(this.renderWaveItem(d.data())));
        }
    }

    renderWaveItem(w){
        const div = document.createElement('div');
        div.className = 'wave-item';
        div.style.cssText = 'border:1px solid var(--border-color);border-radius:10px;padding:10px;margin:8px 0;display:column;gap:10px;align-items:center;justify-content:space-between';
        const cover = w.coverUrl || 'images/default-bird.png';
        const byline = w.authorName ? `<div style="font-size:12px;color:#aaa">by ${(w.authorName||'').replace(/</g,'&lt;')}</div>` : '';
        div.innerHTML = `<div style="display:flex;gap:10px;align-items:center"><img src="${cover}" alt="cover" style="width:48px;height:48px;border-radius:8px;object-fit:cover"><div><div>${(w.title||'Untitled').replace(/</g,'&lt;')}</div>${byline}</div></div><audio class="liber-lib-audio" src="${w.url}" controls style="width:100%" data-title="${(w.title||'').replace(/"/g,'&quot;')}" data-by="${(w.authorName||'').replace(/"/g,'&quot;')}" data-cover="${(w.coverUrl||'').replace(/"/g,'&quot;')}"></audio><div style="display:flex;gap:8px"><button class="btn btn-secondary share-btn"><i class="fas fa-share"></i></button><button class="btn btn-secondary repost-btn" title="Repost"><i class="fas fa-retweet"></i></button></div>`;
        div.querySelector('.share-btn').onclick = async ()=>{
            try{
                const me = await window.firebaseService.getCurrentUser();
                const newRef = firebase.doc(firebase.collection(window.firebaseService.db, 'posts'));
                await firebase.setDoc(newRef, { id: newRef.id, authorId: me.uid, text: (w.title||'Audio'), mediaUrl: w.url, mediaType:'audio', visibility:'private', createdAt: new Date().toISOString(), createdAtTS: firebase.serverTimestamp(), authorName: (w.authorName||''), coverUrl: (w.coverUrl||'') });
                this.showSuccess('Shared to your feed (private)');
            }catch(_){ this.showError('Share failed'); }
        };
        const repostBtn = div.querySelector('.repost-btn');
        if (repostBtn){ repostBtn.onclick = async ()=>{
            try{
                const me = await window.firebaseService.getCurrentUser();
                const newRef = firebase.doc(firebase.collection(window.firebaseService.db, 'posts'));
                await firebase.setDoc(newRef, { id: newRef.id, authorId: me.uid, text: (w.title||'Audio'), mediaUrl: w.url, mediaType:'audio', visibility:'public', createdAt: new Date().toISOString(), createdAtTS: firebase.serverTimestamp(), authorName: (w.authorName||''), coverUrl: (w.coverUrl||'') });
                this.showSuccess('Reposted to your feed');
            }catch(_){ this.showError('Repost failed'); }
        }; }
        const a = div.querySelector('.liber-lib-audio');
        if (a){
            a.addEventListener('play', ()=> this.showMiniPlayer(a, { title: w.title, by: w.authorName, cover: w.coverUrl }));
        }
        return div;
    }

    async loadVideoHost(){
        try{
            const me = await window.firebaseService.getCurrentUser();
            const lib = document.getElementById('video-library');
            const sug = document.getElementById('video-suggestions');
            const upBtn = document.getElementById('video-upload-btn');
            const prog = document.getElementById('video-progress');
            if (upBtn && !upBtn._bound){
                upBtn._bound = true;
                upBtn.onclick = async ()=>{
                    try{
                        const f = document.getElementById('video-file').files[0];
                        const coverFile = document.getElementById('video-cover')?.files?.[0] || null;
                        if (!f){ return this.showError('Select a video'); }
                        // 5 hours approximate cap enforced by size (assume <= 4GB for safety depending on codec)
                        if (f.size > 4*1024*1024*1024){ return this.showError('Max 4 GB'); }
                        const title = (document.getElementById('video-title').value||f.name).trim();
                        const s = firebase.getStorage();
                        const r = firebase.ref(s, `videos/${me.uid}/${Date.now()}_${f.name.replace(/[^a-zA-Z0-9._-]/g,'_')}`);
                        const task = firebase.uploadBytesResumable(r, f, { contentType: f.type||'video/mp4' });
                        task.on('state_changed', (snap)=>{ if (prog) prog.value = Math.round((snap.bytesTransferred/snap.totalBytes)*100); });
                        await task;
                        const url = await firebase.getDownloadURL(r);
                        const meProfile = await window.firebaseService.getUserData(me.uid);
                        const authorName = (meProfile && meProfile.username) || me.email || 'Unknown';
                        let thumbnailUrl = (meProfile && meProfile.avatarUrl) || '';
                        if (coverFile){
                            const cRef = firebase.ref(s, `video-covers/${me.uid}/${Date.now()}_${coverFile.name.replace(/[^a-zA-Z0-9._-]/g,'_')}`);
                            await firebase.uploadBytes(cRef, coverFile, { contentType: coverFile.type || 'image/jpeg' });
                            thumbnailUrl = await firebase.getDownloadURL(cRef);
                        }
                        const docRef = firebase.doc(firebase.collection(window.firebaseService.db, 'videos'));
                        await firebase.setDoc(docRef, { id: docRef.id, owner: me.uid, title, url, createdAt: new Date().toISOString(), createdAtTS: firebase.serverTimestamp(), visibility: 'public', authorId: me.uid, authorName, thumbnailUrl });
                        this.showSuccess('Video uploaded');
                        this.renderVideoLibrary(me.uid);
                    }catch(e){ this.showError('Upload failed'); }
                };
            }

            const search = document.getElementById('video-search');
            if (search && !search._bound){
                search._bound = true;
                search.oninput = async (e)=>{
                    const qStr = (e.target.value||'').toLowerCase();
                    const res = document.getElementById('video-suggestions'); if (!res) return;
                    res.innerHTML='';
                    const snap = await firebase.getDocs(firebase.collection(window.firebaseService.db,'videos'));
                    snap.forEach(d=>{ const v=d.data(); if ((v.title||'').toLowerCase().includes(qStr)){ res.appendChild(this.renderVideoItem(v)); } });
                };
            }

            await this.renderVideoLibrary(me.uid);
            await this.renderVideoSuggestions(me.uid);
        }catch(_){ }
    }

    async renderVideoLibrary(uid){
        const lib = document.getElementById('video-library'); if (!lib) return;
        lib.innerHTML = '';
        try{
            let q;
            try{
                q = firebase.query(firebase.collection(window.firebaseService.db,'videos'), firebase.where('owner','==', uid), firebase.orderBy('createdAtTS','desc'), firebase.limit(50));
            }catch(_){
                q = firebase.query(firebase.collection(window.firebaseService.db,'videos'), firebase.where('owner','==', uid));
            }
            const snap = await firebase.getDocs(q);
            snap.forEach(d=> lib.appendChild(this.renderVideoItem(d.data())));
        }catch{
            const q2 = firebase.query(firebase.collection(window.firebaseService.db,'videos'), firebase.where('owner','==', uid));
            const s2 = await firebase.getDocs(q2); s2.forEach(d=> lib.appendChild(this.renderVideoItem(d.data())));
        }
    }

    async renderVideoSuggestions(uid){
        const sug = document.getElementById('video-suggestions'); if (!sug) return;
        sug.innerHTML = '';
        try{
            const snap = await firebase.getDocs(firebase.collection(window.firebaseService.db,'videos'));
            const list = []; snap.forEach(d=>{ const v=d.data(); if (v.owner !== uid) list.push(v); });
            list.slice(0,10).forEach(v=> sug.appendChild(this.renderVideoItem(v)) );
        }catch(_){ }
    }

    renderVideoItem(v){
        const div = document.createElement('div');
        div.className = 'video-item';
        div.style.cssText = 'border:1px solid var(--border-color);border-radius:10px;padding:10px;margin:8px 0;position:relative';
        const thumb = v.thumbnailUrl || 'images/default-bird.png';
        const byline = v.authorName ? `<div style=\"font-size:12px;color:#aaa\">by ${(v.authorName||'').replace(/</g,'&lt;')}</div>` : '';
        div.innerHTML = `<div style="display:flex;gap:10px;align-items:center;margin-bottom:6px"><img src="${thumb}" alt="cover" style="width:48px;height:48px;border-radius:8px;object-fit:cover"><div><div style=\"font-weight:600\">${(v.title||'Untitled').replace(/</g,'&lt;')}</div>${byline}</div></div>
                         <video class="liber-lib-video" src="${v.url}" controls playsinline style="width:100%;max-height:480px;border-radius:8px" data-title="${(v.title||'').replace(/"/g,'&quot;')}" data-by="${(v.authorName||'').replace(/"/g,'&quot;')}" data-cover="${(v.thumbnailUrl||'').replace(/"/g,'&quot;')}"></video>
                         <div style="position:absolute;top:10px;right:10px;display:flex;gap:8px"><button class="btn btn-secondary share-video-btn"><i class="fas fa-share"></i></button><button class="btn btn-secondary repost-video-btn" title="Repost"><i class="fas fa-retweet"></i></button></div>`;
        div.querySelector('.share-video-btn').onclick = async ()=>{
            try{
                const me = await window.firebaseService.getCurrentUser();
                const newRef = firebase.doc(firebase.collection(window.firebaseService.db, 'posts'));
                await firebase.setDoc(newRef, { id: newRef.id, authorId: me.uid, text: (v.title||'Video'), mediaUrl: v.url, mediaType:'video', visibility:'private', createdAt: new Date().toISOString(), createdAtTS: firebase.serverTimestamp(), authorName: (v.authorName||''), thumbnailUrl: (v.thumbnailUrl||'') });
                this.showSuccess('Shared to your feed (private)');
            }catch(_){ this.showError('Share failed'); }
        };
        const rv = div.querySelector('.repost-video-btn');
        if (rv){ rv.onclick = async ()=>{
            try{
                const me = await window.firebaseService.getCurrentUser();
                const newRef = firebase.doc(firebase.collection(window.firebaseService.db, 'posts'));
                await firebase.setDoc(newRef, { id: newRef.id, authorId: me.uid, text: (v.title||'Video'), mediaUrl: v.url, mediaType:'video', visibility:'public', createdAt: new Date().toISOString(), createdAtTS: firebase.serverTimestamp(), authorName: (v.authorName||''), thumbnailUrl: (v.thumbnailUrl||'') });
                this.showSuccess('Reposted to your feed');
            }catch(_){ this.showError('Repost failed'); }
        }; }
        const vEl = div.querySelector('.liber-lib-video');
        if (vEl){ vEl.addEventListener('play', ()=> this.showMiniPlayer(vEl, { title: v.title, by: v.authorName, cover: v.thumbnailUrl })); }
        return div;
    }

    /**
     * Update navigation visibility based on user role
     */
    async updateNavigation() {
        let isAdmin = false;
        try {
            const me = await this.resolveCurrentUserWithRetry(1200);
            if (me && me.uid && window.firebaseService?.getUserData) {
                const data = await window.firebaseService.getUserData(me.uid);
                isAdmin = String(data?.role || '').toLowerCase() === 'admin';
            } else {
                const fallback = authManager.getCurrentUser();
                isAdmin = String(fallback?.role || '').toLowerCase() === 'admin';
            }
        } catch (_) {
            const fallback = authManager.getCurrentUser();
            isAdmin = String(fallback?.role || '').toLowerCase() === 'admin';
        }
        document.querySelectorAll('.admin-only').forEach((el) => {
            el.style.display = isAdmin ? '' : 'none';
        });
    }

    /**
     * Load overview data
     */
    async loadOverview() {
        // Overview removed – no-op to avoid errors
        return;
    }

    /**
     * Get apps count
     */
    async getAppsCount() {
        try {
            if (window.appsManager) {
                const apps = await window.appsManager.getApps();
                return apps.length;
            }
            return 0;
        } catch (error) {
            console.error('Error getting apps count:', error);
            return 0;
        }
    }

    /**
     * Get users count
     */
    async getUsersCount() {
        try {
            if (window.firebaseService && window.firebaseService.isFirebaseAvailable()) {
                const stats = await window.firebaseService.getUserStats();
                return `${stats.total} (${stats.pending} pending)`;
            } else {
                const users = await authManager.getUsers();
                return users.length + 1; // +1 for admin
            }
        } catch (error) {
            console.error('Error getting users count:', error);
            return '0';
        }
    }

    /**
     * Format date for display
     */
    formatDate(date) {
        const now = new Date();
        const diff = now - date;
        const minutes = Math.floor(diff / 60000);
        const hours = Math.floor(diff / 3600000);
        const days = Math.floor(diff / 86400000);

        if (minutes < 1) return 'Just now';
        if (minutes < 60) return `${minutes}m ago`;
        if (hours < 24) return `${hours}h ago`;
        if (days < 7) return `${days}d ago`;

        return date.toLocaleDateString();
    }

    /**
     * Setup settings handlers
     */
    setupSettingsHandlers() {
        // Session timeout
        const sessionTimeout = document.getElementById('session-timeout');
        if (sessionTimeout) {
            sessionTimeout.addEventListener('change', (e) => {
                const value = parseInt(e.target.value);
                if (value >= 5 && value <= 480) {
                    authManager.sessionTimeout = value * 60 * 1000;
                    this.saveSettings();
                }
            });
        }

        // Auto-refresh apps
        const autoRefreshApps = document.getElementById('auto-refresh-apps');
        if (autoRefreshApps) {
            autoRefreshApps.addEventListener('change', () => {
                this.saveSettings();
            });
        }

        // Show app descriptions
        const showAppDescriptions = document.getElementById('show-app-descriptions');
        if (showAppDescriptions) {
            showAppDescriptions.addEventListener('change', () => {
                this.saveSettings();
            });
        }

        // Enable 2FA
        const enable2FA = document.getElementById('enable-2fa');
        if (enable2FA) {
            enable2FA.addEventListener('change', () => {
                this.saveSettings();
            });
        }

        // Secure keys settings
        this.setupSecureKeysHandlers();

        // Profile Force reload handler (user only)
        const profileForceBtn = document.getElementById('profile-force-reload-btn');
        if (profileForceBtn){
            profileForceBtn.onclick = async ()=>{
                const ok = await this.showConfirm(
`You will log out and force reload the LIBER App.
This will clear cached files (including Service Worker caches) to align with the most recent version of the app and reload all pages.
Do you want to proceed?`);
                if (!ok) return;
                await this.forceHardReload();
            };
        }
    }

    /**
     * Load profile info and bind actions
     */
    async loadProfile() {
        try {
            // Wait for firebase
            if (window.firebaseService && window.firebaseService.isFirebaseAvailable()) {
                const user = await window.firebaseService.getCurrentUser();
                const data = await window.firebaseService.getUserData(user.uid) || {};
                const emailEl = document.getElementById('profil-email');
                const unameEl = document.getElementById('profile-username');
                const verifiedEl = document.getElementById('profile-verified');
                const allowUnconnectedEl = document.getElementById('profile-allow-unconnected-msg');
                if (emailEl) emailEl.value = user.email;
                if (unameEl) unameEl.value = data.username || '';
                if (verifiedEl) verifiedEl.textContent = user.emailVerified ? 'Verified' : 'Not verified';
                if (allowUnconnectedEl) allowUnconnectedEl.checked = data.allowMessagesFromUnconnected !== false;

                const saveBtn = document.getElementById('save-username-btn');
                if (saveBtn) {
                    saveBtn.onclick = async () => {
                        try {
                            const newName = document.getElementById('profile-username').value.trim();
                            if (!newName) return this.showError('Username cannot be empty');
                            await window.firebaseService.updateUserProfile(user.uid, { username: newName });
                            this.showSuccess('Username updated');
                        } catch (e) {
                            this.showError('Failed to update username');
                        }
                    };
                }

                const rvBtn = document.getElementById('resend-verify-btn');
                if (rvBtn) {
                    rvBtn.style.display = user.emailVerified ? 'none' : 'inline-flex';
                    rvBtn.onclick = async () => {
                        try { await window.firebaseService.sendEmailVerification(); this.showSuccess('Verification email sent'); }
                        catch { this.showError('Failed to send verification'); }
                    };
                }
                if (allowUnconnectedEl) {
                    allowUnconnectedEl.onchange = async () => {
                        try {
                            await window.firebaseService.updateUserProfile(user.uid, {
                                allowMessagesFromUnconnected: !!allowUnconnectedEl.checked
                            });
                        } catch (_) {
                            this.showError('Failed to update message privacy');
                        }
                    };
                }

                // Add Link Google Account button if missing
                const profileCard = document.querySelector('#profile-section .settings-card');
                if (profileCard && !document.getElementById('link-google-btn')){
                    const wrap = document.createElement('div');
                    wrap.className = 'setting-item';
                    wrap.innerHTML = '<label>Google</label><button id="link-google-btn" class="btn btn-secondary">Link Google Account</button>';
                    profileCard.appendChild(wrap);
                    const linkBtn = document.getElementById('link-google-btn');
                    if (linkBtn){ linkBtn.onclick = ()=>{ if (window.authManager && typeof window.authManager.linkGoogleAccount==='function'){ window.authManager.linkGoogleAccount(); } }; }
                }

                const chBtn = document.getElementById('change-password-btn');
                if (chBtn) {
                    chBtn.onclick = async () => {
                        const curr = (document.getElementById('current-password').value || '').trim();
                        const np = (document.getElementById('new-password').value || '').trim();
                        const cnp = (document.getElementById('confirm-new-password').value || '').trim();
                        if (!curr || !np || !cnp) return this.showError('Please fill all fields');
                        if (np !== cnp) return this.showError('Passwords do not match');
                        try {
                            // Re-authenticate
                            const cred = firebase.EmailAuthProvider.credential(user.email, curr);
                            await firebase.reauthenticateWithCredential(user, cred);
                            await firebase.updatePassword(user, np);
                            this.showSuccess('Password updated');
                            document.getElementById('current-password').value='';
                            document.getElementById('new-password').value='';
                            document.getElementById('confirm-new-password').value='';
                        } catch (e) {
                            this.showError('Failed to update password');
                        }
                    };
                }
            }
        } catch (e) {
            console.error('Profile load error', e);
        }
    }

    /**
     * Setup secure keys handlers
     */
    setupSecureKeysHandlers() {
        // Load saved secure keys URL
        this.loadSecureKeysUrl();

        // Test keys connection
        const testKeysBtn = document.getElementById('test-keys-btn');
        if (testKeysBtn) {
            testKeysBtn.addEventListener('click', async () => {
                await this.testKeysConnection();
            });
        }

        // Clear key cache
        const clearCacheBtn = document.getElementById('clear-key-cache-btn');
        if (clearCacheBtn) {
            clearCacheBtn.addEventListener('click', () => {
                this.clearKeyCache();
            });
        }

        // Clear all encrypted data
        const clearEncryptedDataBtn = document.getElementById('clear-encrypted-data-btn');
        if (clearEncryptedDataBtn) {
            clearEncryptedDataBtn.addEventListener('click', () => {
                this.clearAllEncryptedData();
            });
        }

        // Save Secure Keys URL
        const secureKeysUrlInput = document.getElementById('secure-keys-url');
        if (secureKeysUrlInput) {
            secureKeysUrlInput.addEventListener('change', () => {
                this.saveSecureKeysUrl();
            });
        }

        // Key cache duration
        const keyCacheDuration = document.getElementById('key-cache-duration');
        if (keyCacheDuration) {
            keyCacheDuration.addEventListener('change', () => {
                this.saveKeyCacheDuration();
            });
        }
    }

    /**
     * Load secure keys URL from localStorage
     */
    loadSecureKeysUrl() {
        const urlInput = document.getElementById('secure-keys-url');
        if (urlInput) {
            const savedUrl = localStorage.getItem('liber_keys_url');
            if (savedUrl) {
                urlInput.value = savedUrl;
            }
        }
    }

    /**
     * Test keys connection
     */
    async testKeysConnection() {
        const testBtn = document.getElementById('test-keys-btn');
        const originalText = testBtn.innerHTML;
        
        try {
            testBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Testing...';
            testBtn.disabled = true;

            const result = await window.secureKeyManager.testConnection();
            
            if (result.success) {
                this.showSuccess('✅ Keys connection successful!');
            } else {
                this.showError(`❌ Connection failed: ${result.message}`);
            }
        } catch (error) {
            this.showError(`❌ Test failed: ${error.message}`);
        } finally {
            testBtn.innerHTML = originalText;
            testBtn.disabled = false;
        }
    }

    /**
     * Clear key cache
     */
    clearKeyCache() {
        window.secureKeyManager.clearCache();
        this.showSuccess('🗑️ Key cache cleared successfully!');
    }

    /**
     * Clear all encrypted data
     */
    clearAllEncryptedData() {
        if (confirm('This will clear all user data, sessions, and encrypted information. This action cannot be undone. Continue?')) {
            window.secureKeyManager.clearAllEncryptedData();
            this.showSuccess('🗑️ All encrypted data cleared successfully!');
            
            // Redirect to login after a short delay
            setTimeout(() => {
                window.location.reload();
            }, 2000);
        }
    }

    /**
     * Save Secure Keys URL
     */
    saveSecureKeysUrl() {
        const urlInput = document.getElementById('secure-keys-url');
        const url = urlInput.value.trim();
        
        if (url) {
            window.secureKeyManager.setKeySource(url);
            this.showSuccess('💾 Secure Keys URL saved!');
        }
    }

    /**
     * Save key cache duration
     */
    saveKeyCacheDuration() {
        const durationInput = document.getElementById('key-cache-duration');
        const duration = parseInt(durationInput.value);
        
        if (duration >= 5 && duration <= 120) {
            window.secureKeyManager.keyCacheExpiry = duration * 60 * 1000;
            this.showSuccess('⏱️ Cache duration updated!');
        }
    }

    /**
     * Load settings
     */
    loadSettings() {
        try {
            const settings = this.getSettings();
            
            // Apply settings to form elements
            const sessionTimeout = document.getElementById('session-timeout');
            if (sessionTimeout) {
                sessionTimeout.value = Math.floor(settings.sessionTimeout / 60000);
            }

            const autoRefreshApps = document.getElementById('auto-refresh-apps');
            if (autoRefreshApps) {
                autoRefreshApps.checked = settings.autoRefreshApps;
            }

            const showAppDescriptions = document.getElementById('show-app-descriptions');
            if (showAppDescriptions) {
                showAppDescriptions.checked = settings.showAppDescriptions;
            }

            const enable2FA = document.getElementById('enable-2fa');
            if (enable2FA) {
                enable2FA.checked = settings.enable2FA;
            }

        } catch (error) {
            console.error('Error loading settings:', error);
        }
    }

    /**
     * Save settings
     */
    saveSettings() {
        try {
            const stEl = document.getElementById('session-timeout');
            const autoEl = document.getElementById('auto-refresh-apps');
            const showDescEl = document.getElementById('show-app-descriptions');
            const twoFaEl = document.getElementById('enable-2fa');

            // If settings UI is not rendered on this page, silently skip
            if (!stEl && !autoEl && !showDescEl && !twoFaEl) {
                return;
            }

            const sessionTimeout = stEl ? (parseInt(stEl.value || '30') * 60000) : (authManager?.sessionTimeout || 30*60*1000);
            const settings = {
                sessionTimeout,
                autoRefreshApps: !!(autoEl && autoEl.checked),
                showAppDescriptions: !!(showDescEl && showDescEl.checked),
                enable2FA: !!(twoFaEl && twoFaEl.checked),
                lastUpdated: new Date().toISOString()
            };

            localStorage.setItem('liber_settings', JSON.stringify(settings));
            
            // Apply session timeout change
            if (authManager.currentUser) {
                authManager.startSessionTimer();
            }

            // Only show toast if UI exists
            if (stEl || autoEl || showDescEl || twoFaEl) this.showSuccess('Settings saved successfully');
        } catch (error) {
            // Do not alert on pages without settings UI
            console.warn('Skipped saving settings:', error?.message || error);
        }
    }

    /**
     * Get settings
     */
    getSettings() {
        try {
            const settings = localStorage.getItem('liber_settings');
            if (settings) {
                return JSON.parse(settings);
            }
        } catch (error) {
            console.error('Error getting settings:', error);
        }

        // Default settings
        return {
            sessionTimeout: 30 * 60 * 1000, // 30 minutes
            autoRefreshApps: true,
            showAppDescriptions: true,
            enable2FA: false,
            lastUpdated: new Date().toISOString()
        };
    }

    /**
     * Show success message
     */
    showSuccess(message) {
        this.showNotification(message, 'success');
    }

    /**
     * Show error message
     */
    showError(message) {
        this.showNotification(message, 'error');
    }

    /**
     * Show info message
     */
    showInfo(message) {
        this.showNotification(message, 'info');
    }

    /**
     * Show notification
     */
    showNotification(message, type = 'info') {
        // Create notification element
        const notification = document.createElement('div');
        notification.className = `notification notification-${type}`;
        notification.innerHTML = `
            <div class="notification-content">
                <i class="fas fa-${type === 'success' ? 'check-circle' : type === 'error' ? 'exclamation-circle' : 'info-circle'}"></i>
                <span>${message}</span>
            </div>
            <button class="notification-close">&times;</button>
        `;

        // Add styles
        notification.style.cssText = `
            position: fixed;
            top: 20px;
            right: 20px;
            background: var(--${type === 'success' ? 'success' : type === 'error' ? 'error' : 'accent'}-color);
            color: var(--primary-bg);
            padding: 12px 20px;
            border-radius: var(--border-radius);
            box-shadow: var(--shadow-hover);
            z-index: 10000;
            display: flex;
            align-items: center;
            gap: 12px;
            max-width: 400px;
            animation: slideIn 0.3s ease-out;
        `;

        // Add to page
        document.body.appendChild(notification);

        // Setup close button
        const closeBtn = notification.querySelector('.notification-close');
        closeBtn.addEventListener('click', () => {
            notification.remove();
        });

        // Auto remove after 5 seconds
        setTimeout(() => {
            if (notification.parentNode) {
                notification.remove();
            }
        }, 5000);
    }

    async showConfirm(message){
        return new Promise((resolve)=>{
            // Simple modal
            const modal = document.createElement('div');
            modal.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,.6);display:flex;align-items:center;justify-content:center;z-index:10000';
            modal.innerHTML = `
                <div style="background:#0f1116;border:1px solid #333;border-radius:12px;padding:20px;max-width:420px;width:90%;color:#eaeaea;box-shadow:0 10px 30px rgba(0,0,0,.5)">
                    <div style="margin-bottom:14px;white-space:pre-wrap">${message.replace(/</g,'&lt;')}</div>
                    <div style="display:flex;gap:10px;justify-content:flex-end">
                        <button id="confirm-no" class="btn btn-secondary">No</button>
                        <button id="confirm-yes" class="btn btn-primary">Yes</button>
                    </div>
                </div>`;
            document.body.appendChild(modal);
            modal.querySelector('#confirm-no').onclick = ()=>{ modal.remove(); resolve(false); };
            modal.querySelector('#confirm-yes').onclick = ()=>{ modal.remove(); resolve(true); };
        });
    }

    async forceHardReload(){
        try {
            if (window.authManager) await window.authManager.logout();
        } catch(_) {}

        // Preserve secure keys URL if present
        let keysUrl = null;
        try { keysUrl = localStorage.getItem('liber_keys_url'); } catch(_){ keysUrl = null; }

        // Unregister all service workers
        try{
            if ('serviceWorker' in navigator){
                const regs = await navigator.serviceWorker.getRegistrations();
                for (const r of regs){ try { await r.unregister(); } catch(_){} }
            }
        }catch(_){ }

        // Clear Cache Storage
        try{
            if (window.caches && caches.keys){
                const names = await caches.keys();
                await Promise.all(names.map(n=> caches.delete(n)));
            }
        }catch(_){ }

        // Clear local/session storage (restore keys URL afterwards)
        try { sessionStorage.clear(); } catch(_){}
        try { localStorage.clear(); } catch(_){}
        try { if (keysUrl) localStorage.setItem('liber_keys_url', keysUrl); } catch(_){}

        // Bypass cache on reload
        const href = window.location.pathname + '?reload=' + Date.now();
        window.location.replace(href);
    }

    /**
     * Refresh dashboard data
     */
    async refreshDashboard() {
        await this.loadOverview();
        this.showSuccess('Dashboard refreshed');
    }

    /**
     * Get current section
     */
    getCurrentSection() {
        return this.currentSection;
    }

    /**
     * Check if user has permission for section
     */
    hasPermission(section) {
        const currentUser = authManager.getCurrentUser();
        if (!currentUser) return false;

        // Admin can access all sections
        if (currentUser.role === 'admin') return true;

        // Regular users can only access overview and apps
        return ['overview', 'apps'].includes(section);
    }

    /**
     * Handle keyboard shortcuts
     */
    setupKeyboardShortcuts() {
        document.addEventListener('keydown', (e) => {
            // Only handle shortcuts when not in input fields
            if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') {
                return;
            }

            // Ctrl/Cmd + R to refresh
            if ((e.ctrlKey || e.metaKey) && e.key === 'r') {
                e.preventDefault();
                this.refreshDashboard();
            }

            // Number keys for navigation
            if (e.key >= '1' && e.key <= '4') {
                const sections = ['overview', 'apps', 'users', 'settings'];
                const sectionIndex = parseInt(e.key) - 1;
                if (sections[sectionIndex] && this.hasPermission(sections[sectionIndex])) {
                    this.switchSection(sections[sectionIndex]);
                }
            }
        });
    }

    /**
     * Toggle WALL-E widget
     */
    toggleWallEWidget() {
        console.log('Dashboard WALL-E toggle button clicked!');
        
        // Check if we're on mobile
        if (window.innerWidth <= 768) {
            // Use the exact same logic as login page
            const widget = document.querySelector('.chatgpt-widget');
            const isWidgetActive = widget && widget.classList.contains('mobile-activated');
            
            if (isWidgetActive) {
                // Widget is active, hide it
                console.log('Hiding WALL-E widget on dashboard...');
                widget.classList.remove('mobile-activated');
                
                // Update mobile WALL-E button state
                const mobileWallEBtn = document.getElementById('mobile-wall-e-btn');
                if (mobileWallEBtn) {
                    mobileWallEBtn.classList.remove('active');
                }
            } else {
                // Widget is not active, show it
                console.log('Showing WALL-E widget on dashboard...');
                
                // Wait for WALL-E widget to be initialized (same as login page)
                this.waitForWidgetAndShow();
            }
        } else {
            // On desktop, use the normal toggle
            if (window.wallE && typeof window.wallE.toggleChat === 'function') {
                console.log('Toggling WALL-E widget on desktop...');
                window.wallE.toggleChat();
            } else {
                console.warn('WALL-E widget not available on desktop');
            }
        }
    }

    /**
     * Wait for widget to be available and then show it (same logic as login page)
     */
    async waitForWidgetAndShow() {
        let attempts = 0;
        const maxAttempts = 30;
        
        while ((!window.wallE || !document.querySelector('.chatgpt-widget')) && attempts < maxAttempts) {
            console.log(`Waiting for WALL-E widget... attempt ${attempts + 1}`);
            await new Promise(resolve => setTimeout(resolve, 100));
            attempts++;
        }
        
        if (!window.wallE) {
            console.error('WALL-E widget not available after waiting');
            return;
        }
        
        // Force create widget if it doesn't exist
        let widgetToShow = document.querySelector('.chatgpt-widget');
        if (!widgetToShow && window.wallE.createChatInterface) {
            console.log('Forcing widget creation...');
            window.wallE.createChatInterface();
            widgetToShow = document.querySelector('.chatgpt-widget');
        }
        
        if (widgetToShow) {
            console.log('WALL-E widget found, showing it...');
            
            // Use CSS class instead of inline styles for better compatibility
            widgetToShow.classList.add('mobile-activated');
            
            // Expand the widget
            if (window.wallE && typeof window.wallE.expandChat === 'function') {
                console.log('Expanding WALL-E widget...');
                window.wallE.expandChat();
            }
            
            // Update mobile WALL-E button state
            const mobileWallEBtn = document.getElementById('mobile-wall-e-btn');
            if (mobileWallEBtn) {
                mobileWallEBtn.classList.add('active');
            }
            
            console.log('WALL-E widget successfully activated on dashboard');
        } else {
            console.error('WALL-E widget element not found even after creation attempt');
        }
    }

    /**
     * Show WALL-E modal for mobile
     */
    showWallEModal() {
        // Create modal if it doesn't exist
        let modal = document.getElementById('wall-e-modal');
        if (!modal) {
            modal = document.createElement('div');
            modal.id = 'wall-e-modal';
            modal.className = 'wall-e-modal';
            modal.innerHTML = `
                <div class="wall-e-modal-content">
                                            <div class="wall-e-modal-header">
                            <h3><img src="images/wall_e.svg" alt="WALL-E" class="modal-icon"> WALL-E Assistant</h3>
                            <button class="wall-e-modal-close">&times;</button>
                        </div>
                    <div class="wall-e-modal-body">
                        <div class="wall-e-chat-container">
                            <div class="wall-e-messages" id="wall-e-messages">
                                <div class="wall-e-welcome">
                                    <img src="images/wall_e.svg" alt="WALL-E" class="welcome-icon">
                                    <h4>Wall-eeeee!</h4>
                                    <p>Any help?</p>
                                </div>
                            </div>
                            <div class="wall-e-input-area">
                                <div class="wall-e-file-upload" id="wall-e-file-upload">
                                    <button class="wall-e-upload-btn" id="wall-e-upload-btn">
                                        <i class="fas fa-paperclip"></i>
                                    </button>
                                    <input type="file" id="wall-e-file-input" multiple style="display: none;">
                                </div>
                                <div class="wall-e-input-container">
                                    <textarea id="wall-e-input" placeholder="Type your message..." rows="1"></textarea>
                                    <button class="wall-e-send" id="wall-e-send">
                                        <i class="fas fa-paper-plane"></i>
                                    </button>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            `;
            document.body.appendChild(modal);
            
            // Add event listeners
            const closeBtn = modal.querySelector('.wall-e-modal-close');
            closeBtn.addEventListener('click', () => this.hideWallEModal());
            
            const sendBtn = modal.querySelector('#wall-e-send');
            const input = modal.querySelector('#wall-e-input');
            const uploadBtn = modal.querySelector('#wall-e-upload-btn');
            const fileInput = modal.querySelector('#wall-e-file-input');
            
            sendBtn.addEventListener('click', () => this.sendWallEMessage());
            input.addEventListener('keypress', (e) => {
                if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault();
                    this.sendWallEMessage();
                }
            });
            
            uploadBtn.addEventListener('click', () => fileInput.click());
            fileInput.addEventListener('change', (e) => this.handleWallEFileUpload(e));
            
            // Add modal styles
            this.addWallEModalStyles();
        }
        
        // Show modal
        modal.style.display = 'flex';
        document.body.style.overflow = 'hidden';
        
        // Focus on input
        setTimeout(() => {
            const input = modal.querySelector('#wall-e-input');
            if (input) input.focus();
        }, 100);
    }

    /**
     * Hide WALL-E modal
     */
    hideWallEModal() {
        const modal = document.getElementById('wall-e-modal');
        if (modal) {
            modal.style.display = 'none';
            document.body.style.overflow = '';
        }
        
        // Update mobile WALL-E button state
        const mobileWallEBtn = document.getElementById('mobile-wall-e-btn');
        if (mobileWallEBtn) {
            mobileWallEBtn.classList.remove('active');
        }
    }

    /**
     * Send WALL-E message
     */
    async sendWallEMessage() {
        const input = document.getElementById('wall-e-input');
        const message = input.value.trim();
        
        if (!message || this._wallESending) return;
        this._wallESending = true;
        
        // Add user message to chat
        this.addWallEMessage('user', message);
        input.value = '';
        this.addWallEMessage('assistant', '...');
        
        // Call WALL-E API
        if (window.wallE && typeof window.wallE.callWALLE === 'function') {
            try {
                const response = await window.wallE.callWALLE(message);
                const msgs = document.querySelectorAll('#wall-e-messages .wall-e-message.assistant');
                const last = msgs[msgs.length - 1];
                if (last && last.textContent && last.textContent.includes('...')) last.remove();
                this.addWallEMessage('assistant', response);
            } catch (error) {
                const msgs = document.querySelectorAll('#wall-e-messages .wall-e-message.assistant');
                const last = msgs[msgs.length - 1];
                if (last && last.textContent && last.textContent.includes('...')) last.remove();
                this.addWallEMessage('error', 'Sorry, I encountered an error. Please try again.');
            }
        } else {
            const msgs = document.querySelectorAll('#wall-e-messages .wall-e-message.assistant');
            const last = msgs[msgs.length - 1];
            if (last && last.textContent && last.textContent.includes('...')) last.remove();
            this.addWallEMessage('assistant', 'Sorry, WALL-E is not available at the moment.');
        }
        this._wallESending = false;
    }

    /**
     * Add message to WALL-E chat
     */
    addWallEMessage(type, content) {
        const messagesContainer = document.getElementById('wall-e-messages');
        if (!messagesContainer) return;
        
        const messageDiv = document.createElement('div');
        messageDiv.className = `wall-e-message ${type}`;
        
        const avatar = document.createElement('div');
        avatar.className = 'wall-e-avatar';
                    avatar.innerHTML = type === 'user' ? '<i class="fas fa-user"></i>' : '<img src="images/wall_e.svg" alt="WALL-E" class="avatar-icon">';
        
        const textDiv = document.createElement('div');
        textDiv.className = 'wall-e-text';
        textDiv.textContent = content;
        
        messageDiv.appendChild(avatar);
        messageDiv.appendChild(textDiv);
        messagesContainer.appendChild(messageDiv);
        
        // Scroll to bottom
        messagesContainer.scrollTop = messagesContainer.scrollHeight;
    }

    /**
     * Handle WALL-E file upload
     */
    handleWallEFileUpload(event) {
        const files = Array.from(event.target.files);
        // Handle file upload logic here
        console.log('Files selected:', files);
    }

    /**
     * Add WALL-E modal styles
     */
    addWallEModalStyles() {
        if (document.getElementById('wall-e-modal-styles')) return;
        
        const style = document.createElement('style');
        style.id = 'wall-e-modal-styles';
        style.textContent = `
            .wall-e-modal {
                position: fixed;
                top: 0;
                left: 0;
                right: 0;
                bottom: 0;
                background: rgba(0, 0, 0, 0.8);
                z-index: 10000;
                display: none;
                align-items: center;
                justify-content: center;
            }
            
            .wall-e-modal-content {
                background: #1a1a1a;
                border-radius: 12px;
                width: 90vw;
                max-width: 500px;
                height: 80vh;
                max-height: 600px;
                display: flex;
                flex-direction: column;
                box-shadow: 0 10px 30px rgba(0, 0, 0, 0.5);
            }
            
            .wall-e-modal-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                padding: 16px 20px;
                background: #007bff;
                color: white;
                border-radius: 12px 12px 0 0;
            }
            
            .wall-e-modal-header h3 {
                margin: 0;
                font-size: 16px;
                display: flex;
                align-items: center;
                gap: 8px;
            }
            
            .wall-e-modal-close {
                background: none;
                border: none;
                color: white;
                font-size: 24px;
                cursor: pointer;
                padding: 0;
                width: 30px;
                height: 30px;
                display: flex;
                align-items: center;
                justify-content: center;
            }
            
            .wall-e-modal-body {
                flex: 1;
                display: flex;
                flex-direction: column;
                background: #1a1a1a;
                border-radius: 0 0 12px 12px;
            }
            
            .wall-e-chat-container {
                flex: 1;
                display: flex;
                flex-direction: column;
                height: 100%;
            }
            
            .wall-e-messages {
                flex: 1;
                overflow-y: auto;
                padding: 16px;
                display: flex;
                flex-direction: column;
                gap: 12px;
            }
            
            .wall-e-welcome {
                text-align: center;
                padding: 20px;
                color: #888;
            }
            
            .wall-e-welcome i {
                font-size: 2rem;
                margin-bottom: 12px;
                color: #007bff;
            }
            
            .wall-e-welcome h4 {
                margin: 0 0 8px 0;
                color: #fff;
            }
            
            .wall-e-message {
                display: flex;
                gap: 8px;
                max-width: 100%;
            }
            
            .wall-e-message.user {
                flex-direction: row-reverse;
            }
            
            .wall-e-avatar {
                width: 32px;
                height: 32px;
                border-radius: 50%;
                display: flex;
                align-items: center;
                justify-content: center;
                flex-shrink: 0;
                font-size: 14px;
            }
            
            .wall-e-message.user .wall-e-avatar {
                background: #007bff;
                color: white;
            }
            
            .wall-e-message.assistant .wall-e-avatar {
                background: #333;
                color: #007bff;
            }
            
            .wall-e-text {
                padding: 10px 14px;
                border-radius: 12px;
                font-size: 14px;
                line-height: 1.4;
                max-width: calc(100% - 40px);
            }
            
            .wall-e-message.user .wall-e-text {
                background: #007bff;
                color: white;
                border-bottom-right-radius: 4px;
            }
            
            .wall-e-message.assistant .wall-e-text {
                background: #333;
                color: #fff;
                border-bottom-left-radius: 4px;
            }
            
            .wall-e-input-area {
                border-top: 1px solid #333;
                padding: 16px;
                background: #1a1a1a;
                border-radius: 0 0 12px 12px;
            }
            
            .wall-e-file-upload {
                margin-bottom: 12px;
            }
            
            .wall-e-upload-btn {
                background: #333;
                border: 1px solid #555;
                color: #888;
                padding: 8px 12px;
                border-radius: 6px;
                cursor: pointer;
                font-size: 14px;
            }
            
            .wall-e-input-container {
                display: flex;
                gap: 8px;
                align-items: flex-end;
            }
            
            #wall-e-input {
                flex: 1;
                border: 1px solid #333;
                border-radius: 8px;
                padding: 10px 12px;
                background: #2a2a2a;
                color: #fff;
                font-size: 14px;
                resize: none;
                min-height: 20px;
                max-height: 120px;
                font-family: inherit;
            }
            
            #wall-e-input:focus {
                outline: none;
                border-color: #007bff;
            }
            
            .wall-e-send {
                background: #007bff;
                border: none;
                color: white;
                padding: 10px 12px;
                border-radius: 8px;
                cursor: pointer;
                font-size: 14px;
                min-width: 40px;
                min-height: 40px;
                display: flex;
                align-items: center;
                justify-content: center;
            }
            
            .wall-e-send:hover {
                background: #0056b3;
            }
        `;
        document.head.appendChild(style);
    }

    /**
     * Initialize keyboard shortcuts
     */
    initKeyboardShortcuts() {
        this.setupKeyboardShortcuts();
    }



    /**
     * Handle WALL-E widget positioning when entering dashboard
     */
    handleWallETransitionToDashboard() {
        const widget = document.querySelector('.chatgpt-widget');
        if (widget) {
            if (sessionStorage.getItem('wallE_activated_on_login') === 'true') {
                // Remove mobile-activated class
                widget.classList.remove('mobile-activated');
                
                // Reset widget positioning for dashboard
                widget.style.position = '';
                widget.style.bottom = '';
                widget.style.right = '';
                widget.style.width = '';
                widget.style.maxWidth = '';
                widget.style.zIndex = '';
                
                // Clear the session storage flag
                sessionStorage.removeItem('wallE_activated_on_login');
            }
            
            // Ensure widget is positioned correctly for dashboard on initial load
            if (window.innerWidth <= 768) {
                // On mobile dashboard, ensure proper positioning but keep hidden by default
                widget.style.bottom = '80px';
                widget.style.right = '10px';
                widget.style.width = 'calc(100vw - 20px)';
                widget.style.maxWidth = '350px';
                widget.style.display = 'none'; // Hidden by default on mobile
                widget.classList.remove('hidden');
                widget.classList.remove('mobile-activated'); // Ensure not activated
                
                // Update mobile button state to inactive
                const mobileWallEBtn = document.getElementById('mobile-wall-e-btn');
                if (mobileWallEBtn) {
                    mobileWallEBtn.classList.remove('active');
                }
                
                // Ensure WALL-E widget is collapsed on mobile
                if (window.wallE && typeof window.wallE.collapseChat === 'function') {
                    window.wallE.collapseChat();
                }
                
                console.log('WALL-E widget positioned for mobile dashboard - hidden by default');
            } else {
                // On desktop, use normal positioning and keep visible
                widget.style.display = 'block';
                widget.classList.remove('hidden');
                widget.classList.remove('mobile-activated'); // Ensure not activated
                console.log('WALL-E widget positioned for desktop dashboard on initial load');
            }
        }
    }

    // In searchUsers or loadSpace search handler
    async searchUsers(term) {
      // existing code ...
      filtered.forEach(u => {
        const li = document.createElement('li');
        li.innerHTML = `
          <img src="${u.avatarUrl || 'default-avatar.png'}" class="avatar" alt="${u.username}">
          <div>
            <div class="uname">${u.username}</div>
            <div class="email">${u.email}</div>
          </div>
        `;
        li.dataset.uid = u.uid || u.id || '';
        li.addEventListener('click', () => {
          this.showUserPreviewModal(u.uid || u.id || u);
        });
        resultsEl.appendChild(li);
      });
      // ... 
    }

    // User profile popup from search/connections.
    async showUserPreviewModal(u) {
      const uid = (u && (u.uid || u.id)) || u;
      if (!uid) return;
      const data = (u && u.username) ? u : ((await window.firebaseService.getUserData(uid)) || {});
      const me = await this.resolveCurrentUser();
      if (!me || !me.uid) return;
      const readConnState = async ()=>{
        try{
          const c = await firebase.getDoc(firebase.doc(window.firebaseService.db,'connections',me.uid,'peers',uid));
          if (!c.exists()) return { status: 'none', requestedBy: '', requestedTo: '' };
          const d = c.data() || {};
          return {
            status: d.status || ((d.requestedBy || d.requestedTo) ? 'pending' : 'connected'),
            requestedBy: d.requestedBy || '',
            requestedTo: d.requestedTo || ''
          };
        }catch(_){
          return { status: 'none', requestedBy: '', requestedTo: '' };
        }
      };
      const connectLabelFor = (state)=>{
        const isConnected = state.status === 'connected';
        const isOutgoingPending = state.status === 'pending' && state.requestedBy === me.uid;
        const isIncomingPending = state.status === 'pending' && state.requestedTo === me.uid;
        if (isConnected) return '<i class="fas fa-unlink"></i> Disconnect';
        if (isOutgoingPending) return '<i class="fas fa-hourglass-half"></i> Pending';
        if (isIncomingPending) return '<i class="fas fa-check"></i> Accept request';
        return '<i class="fas fa-link"></i> Connect';
      };
      let isFollowing = false;
      let connState = { status: 'none', requestedBy: '', requestedTo: '' };
      try { const following = await window.firebaseService.getFollowingIds(me.uid); isFollowing = (following || []).includes(uid); } catch(_) {}
      connState = await readConnState();
      const connectLabel = connectLabelFor(connState);

      const overlay = document.createElement('div');
      overlay.className = 'modal-overlay';
      overlay.style.cssText = 'position:fixed;inset:0;z-index:10060;background:rgba(0,0,0,.55);display:flex;align-items:flex-start;justify-content:center;padding:16px;overflow:auto';
      overlay.innerHTML = `
        <div class="modal" style="max-width:860px;width:min(860px,96vw);max-height:calc(100vh - 32px);overflow:auto;margin:0 auto;">
          <div class="modal-header"><h3>${data.username || data.email || 'User'}</h3><button class="modal-close">&times;</button></div>
          <div class="modal-body">
            <div style="display:flex;flex-wrap:wrap;gap:16px;align-items:center;margin-bottom:12px">
              <img src="${data.avatarUrl || 'images/default-bird.png'}" style="width:64px;height:64px;border-radius:12px;object-fit:cover">
              <div style="flex:1">
                <div style="font-weight:700">${data.username || ''}</div>
                <div style="opacity:.8">${data.email || ''}</div>
              </div>
              <button id="follow-toggle" class="btn ${isFollowing ? 'btn-secondary' : 'btn-primary'}">${isFollowing ? 'Unfollow' : 'Follow'}</button>
              <button id="connect-toggle" class="btn btn-secondary">${connectLabel}</button>
              <button id="start-chat" class="btn btn-secondary"><i class="fas fa-comments"></i> Start chat</button>
            </div>
            <div id="preview-feed"></div>
            <div id="preview-audio" style="margin-top:12px"></div>
            <div id="preview-video" style="margin-top:12px"></div>
          </div>
        </div>`;
      document.body.appendChild(overlay);
      overlay.querySelector('.modal-close').onclick = () => overlay.remove();
      overlay.addEventListener('click', (e) => { if (e.target.classList.contains('modal-overlay')) overlay.remove(); });

      const toggle = overlay.querySelector('#follow-toggle');
      toggle.onclick = async () => {
        try {
          toggle.disabled = true;
          if (toggle.textContent === 'Follow') {
            await window.firebaseService.followUser(me.uid, uid);
            toggle.textContent = 'Unfollow';
            toggle.className = 'btn btn-secondary';
          } else {
            await window.firebaseService.unfollowUser(me.uid, uid);
            toggle.textContent = 'Follow';
            toggle.className = 'btn btn-primary';
          }
        } catch(_) {}
        finally { toggle.disabled = false; }
      };

      const connectBtn = overlay.querySelector('#connect-toggle');
      connectBtn.onclick = async ()=>{
        try{
          connectBtn.disabled = true;
          const currentState = await readConnState();
          const status = currentState.status;
          const requestedBy = currentState.requestedBy;
          const requestedTo = currentState.requestedTo;
          const now = new Date().toISOString();
          const meData = (await window.firebaseService.getUserData(me.uid)) || {};
          const mePeer = {
            uid: me.uid,
            username: meData.username || me.email || me.uid,
            email: meData.email || me.email || '',
            avatarUrl: meData.avatarUrl || 'images/default-bird.png'
          };
          const otherPeer = {
            uid,
            username: data.username || data.email || uid,
            email: data.email || '',
            avatarUrl: data.avatarUrl || 'images/default-bird.png'
          };
          const myRef = firebase.doc(window.firebaseService.db,'connections',me.uid,'peers',uid);
          const peerRef = firebase.doc(window.firebaseService.db,'connections',uid,'peers',me.uid);
          let connectionRequestIntro = '';
          if (status === 'connected'){
            await firebase.deleteDoc(myRef).catch(()=>null);
            await firebase.deleteDoc(peerRef).catch(()=>null);
          } else if (status === 'pending' && requestedBy === me.uid){
            await firebase.deleteDoc(myRef).catch(()=>null);
            await firebase.deleteDoc(peerRef).catch(()=>null);
          } else if (status === 'pending' && requestedTo === me.uid){
            await firebase.setDoc(myRef, {
              ...otherPeer, status:'connected', requestedBy, requestedTo, connectedAt:now, updatedAt:now
            }, { merge:true });
            await firebase.setDoc(peerRef, {
              ...mePeer, status:'connected', requestedBy, requestedTo, connectedAt:now, updatedAt:now
            }, { merge:true });
          } else {
            const promptText = window.prompt('Add one intro message with your connection request:', '');
            if (promptText === null){
              return;
            }
            const requesterName = mePeer.username || me.email || 'User';
            connectionRequestIntro = String(promptText || '').trim() || `${requesterName} just sent a connection request`;
            await firebase.setDoc(myRef, {
              ...otherPeer, status:'pending', requestedBy:me.uid, requestedTo:uid, requestedAt:now, updatedAt:now
            }, { merge:true });
            await firebase.setDoc(peerRef, {
              ...mePeer, status:'pending', requestedBy:me.uid, requestedTo:uid, requestedAt:now, updatedAt:now
            }, { merge:true });
            try{
              const key = [me.uid, uid].sort().join('|');
              const connRef = firebase.doc(window.firebaseService.db, 'chatConnections', key);
              await firebase.setDoc(connRef, {
                id: key,
                key,
                participants: [me.uid, uid],
                participantUsernames: [mePeer.username || me.email || me.uid, data.username || data.email || uid],
                admins: [me.uid],
                createdAt: now,
                updatedAt: now,
                lastMessage: connectionRequestIntro.slice(0, 200)
              }, { merge: true });
              const msgRef = firebase.doc(firebase.collection(window.firebaseService.db, 'chatMessages', key, 'messages'));
              await firebase.setDoc(msgRef, {
                id: msgRef.id,
                connId: key,
                sender: me.uid,
                text: connectionRequestIntro,
                previewText: connectionRequestIntro.slice(0, 220),
                createdAt: new Date().toISOString(),
                createdAtTS: firebase.serverTimestamp(),
                systemType: 'connection_request_intro'
              });
            }catch(err){
              console.warn('Failed to send connection intro message', err);
            }
          }
          const nextState = await readConnState();
          connectBtn.innerHTML = connectLabelFor(nextState);
          await this.loadConnectionsForSpace();
        }catch(e){
          console.error('Connection update failed', e);
          this.showError('Connection update failed');
        }
        finally{ connectBtn.disabled = false; }
      };

      const chatBtn = overlay.querySelector('#start-chat');
      if (chatBtn) {
        chatBtn.onclick = async () => {
          try {
            const key = [me.uid, uid].sort().join('|');
            overlay.remove();
            const qs = new URLSearchParams({ connId: key });
            const localPath = `apps/secure-chat/index.html?${qs.toString()}`;
            const full = `${window.location.origin}${window.location.pathname.includes('/control-panel') ? '/control-panel' : ''}/${localPath}`;
            if (window.appsManager && typeof window.appsManager.openAppInShell === 'function') {
              window.appsManager.openAppInShell({ id: 'secure-chat', name: 'Connections' }, full);
            } else {
              window.location.href = localPath;
            }
          } catch(_) {}
        };
      }

      // Public posts with safe fallback (no-index fallback).
      const feed = overlay.querySelector('#preview-feed');
      try{
        let list = [];
        try{
          const q = firebase.query(
            firebase.collection(window.firebaseService.db, 'posts'),
            firebase.where('authorId', '==', uid),
            firebase.where('visibility', '==', 'public'),
            firebase.orderBy('createdAtTS', 'desc'),
            firebase.limit(10)
          );
          const s = await firebase.getDocs(q);
          s.forEach(d => list.push(d.data()));
        }catch{
          const q2 = firebase.query(firebase.collection(window.firebaseService.db, 'posts'), firebase.where('authorId', '==', uid));
          const s2 = await firebase.getDocs(q2);
          s2.forEach(d=>{ const p=d.data(); if ((p.visibility||'public')==='public') list.push(p); });
          list.sort((a,b)=> (b.createdAtTS?.toMillis?.()||0)-(a.createdAtTS?.toMillis?.()||0) || new Date(b.createdAt||0)-new Date(a.createdAt||0));
          list = list.slice(0,10);
        }
        feed.innerHTML = `<h4 style="margin:4px 0 8px">Posts</h4>` + (list.map(p => `<div class="post-item" style="border:1px solid var(--border-color);border-radius:12px;padding:12px;margin:10px 0;background:var(--secondary-bg)">${(p.text || '').replace(/</g, '&lt;')}</div>`).join('') || '<div style="opacity:.8">No public posts yet.</div>');
      }catch(_){ feed.innerHTML = '<div style="opacity:.8">Unable to load posts.</div>'; }

      // Audio preview section.
      const audioEl = overlay.querySelector('#preview-audio');
      try{
        let rows = [];
        try{
          const q = firebase.query(firebase.collection(window.firebaseService.db,'wave'), firebase.where('ownerId','==', uid), firebase.orderBy('createdAt','desc'), firebase.limit(8));
          const s = await firebase.getDocs(q); s.forEach(d=> rows.push(d.data()));
        }catch{
          const q2 = firebase.query(firebase.collection(window.firebaseService.db,'wave'), firebase.where('ownerId','==', uid));
          const s2 = await firebase.getDocs(q2); s2.forEach(d=> rows.push(d.data()));
          rows.sort((a,b)=> new Date(b.createdAt||0) - new Date(a.createdAt||0));
          rows = rows.slice(0,8);
        }
        audioEl.innerHTML = `<h4 style="margin:4px 0 8px">Audio</h4>` + (rows.length ? rows.map(w=> `<div class="post-item" style="border:1px solid var(--border-color);border-radius:12px;padding:10px;margin:8px 0;background:var(--secondary-bg)"><div style="margin-bottom:6px">${(w.title||'Audio').replace(/</g,'&lt;')}</div><audio controls style="width:100%" src="${w.url||''}"></audio></div>`).join('') : '<div style="opacity:.8">No audio uploaded.</div>');
      }catch(_){ audioEl.innerHTML = '<h4 style="margin:4px 0 8px">Audio</h4><div style="opacity:.8">Unable to load audio.</div>'; }

      // Video preview section.
      const videoEl = overlay.querySelector('#preview-video');
      try{
        let rows = [];
        try{
          const q = firebase.query(firebase.collection(window.firebaseService.db,'videos'), firebase.where('owner','==', uid), firebase.orderBy('createdAtTS','desc'), firebase.limit(8));
          const s = await firebase.getDocs(q); s.forEach(d=> rows.push(d.data()));
        }catch{
          const q2 = firebase.query(firebase.collection(window.firebaseService.db,'videos'), firebase.where('owner','==', uid));
          const s2 = await firebase.getDocs(q2); s2.forEach(d=> rows.push(d.data()));
          rows.sort((a,b)=> (b.createdAtTS?.toMillis?.()||0)-(a.createdAtTS?.toMillis?.()||0) || new Date(b.createdAt||0)-new Date(a.createdAt||0));
          rows = rows.slice(0,8);
        }
        videoEl.innerHTML = `<h4 style="margin:4px 0 8px">Videos</h4>` + (rows.length ? rows.map(v=> `<div class="post-item" style="border:1px solid var(--border-color);border-radius:12px;padding:10px;margin:8px 0;background:var(--secondary-bg)"><div style="margin-bottom:6px">${(v.title||'Video').replace(/</g,'&lt;')}</div><video controls playsinline style="width:100%;max-height:260px" src="${v.url||''}"></video></div>`).join('') : '<div style="opacity:.8">No videos uploaded.</div>');
      }catch(_){ videoEl.innerHTML = '<h4 style="margin:4px 0 8px">Videos</h4><div style="opacity:.8">Unable to load videos.</div>'; }
    }

    async openAdvancedCommentsFallback(postId, tree, myUid){
      if (!tree || !postId) return;
      const forceOpen = tree.dataset.forceOpen === '1';
      tree.dataset.forceOpen = '0';
      if (tree.style.display === 'none' || forceOpen) tree.style.display = 'block';
      else { tree.style.display = 'none'; return; }

      const renderTree = async () => {
        tree.innerHTML = '';
        const currentLimit = Number(tree.dataset.limit || 5);
        const comments = await window.firebaseService.getComments(postId, currentLimit);
        const map = new Map();
        comments.forEach(c => map.set(c.id, { ...c, children: [] }));
        const roots = [];
        comments.forEach(c => {
          if (c.parentId && map.has(c.parentId)) map.get(c.parentId).children.push(map.get(c.id));
          else roots.push(map.get(c.id));
        });

        const renderNode = (node, parent) => {
          const item = document.createElement('div');
          item.className = 'comment-item';
          item.innerHTML = `<div class="comment-text"><i class="fas fa-comment"></i> ${(node.text||'').replace(/</g,'&lt;')}</div>
          <div class="comment-actions" data-comment-id="${node.id}" data-author="${node.authorId}" style="display:flex;gap:8px;margin-top:4px">
            <span class="reply-btn" style="cursor:pointer"><i class="fas fa-reply"></i> Reply</span>
            <i class="fas fa-edit edit-comment-btn" title="Edit" style="cursor:pointer"></i>
            <i class="fas fa-trash delete-comment-btn" title="Delete" style="cursor:pointer"></i>
          </div>`;
          parent.appendChild(item);

          const replyBox = document.createElement('div');
          replyBox.style.cssText = 'margin:6px 0 0 0; display:none';
          replyBox.innerHTML = '<input type="text" class="reply-input" placeholder="Reply..." style="width:100%">';
          item.appendChild(replyBox);
          item.querySelector('.reply-btn').onclick = () => {
            replyBox.style.display = replyBox.style.display === 'none' ? 'block' : 'none';
            if (replyBox.style.display === 'block'){
              const i = replyBox.querySelector('.reply-input'); if (i) i.focus();
            }
          };

          const inp = replyBox.querySelector('.reply-input');
          if (inp){
            inp.onkeydown = async (ev) => {
              if (ev.key === 'Enter' && inp.value.trim()){
                await window.firebaseService.addComment(postId, myUid, inp.value.trim(), node.id);
                inp.value = '';
                tree.dataset.forceOpen = '1';
                await this.openAdvancedCommentsFallback(postId, tree, myUid);
              }
            };
          }

          if (node.children && node.children.length){
            const sub = document.createElement('div');
            sub.className = 'comment-tree';
            item.appendChild(sub);
            node.children.forEach(ch => renderNode(ch, sub));
          }
        };
        roots.reverse().forEach(n => renderNode(n, tree));

        if (comments.length >= currentLimit){
          const more = document.createElement('button');
          more.className = 'btn btn-secondary';
          more.style.marginTop = '8px';
          more.textContent = 'See more comments';
          more.onclick = async () => {
            tree.dataset.limit = String(currentLimit + 10);
            tree.dataset.forceOpen = '1';
            await this.openAdvancedCommentsFallback(postId, tree, myUid);
          };
          tree.appendChild(more);
        }

        const addWrap = document.createElement('div');
        addWrap.style.cssText = 'margin-top:8px';
        addWrap.innerHTML = '<input type="text" class="reply-input" placeholder="Add a comment..." style="width:100%">';
        tree.appendChild(addWrap);
        const addInp = addWrap.querySelector('.reply-input');
        if (addInp){
          addInp.onkeydown = async (ev) => {
            if (ev.key === 'Enter' && addInp.value.trim()){
              await window.firebaseService.addComment(postId, myUid, addInp.value.trim(), null);
              addInp.value = '';
              tree.dataset.forceOpen = '1';
              await this.openAdvancedCommentsFallback(postId, tree, myUid);
            }
          };
        }

        tree.querySelectorAll('.comment-actions').forEach((act) => {
          const cid = act.getAttribute('data-comment-id');
          const author = act.getAttribute('data-author');
          const canEdit = !!myUid && author === myUid;
          const eb = act.querySelector('.edit-comment-btn');
          const db = act.querySelector('.delete-comment-btn');
          if (!canEdit){ if (eb) eb.style.display = 'none'; if (db) db.style.display = 'none'; return; }
          if (eb){
            eb.onclick = async () => {
              const newText = prompt('Edit comment:');
              if (newText === null) return;
              await window.firebaseService.updateComment(postId, cid, newText.trim());
              tree.dataset.forceOpen = '1';
              await this.openAdvancedCommentsFallback(postId, tree, myUid);
            };
          }
          if (db){
            db.onclick = async () => {
              if (!confirm('Delete this comment?')) return;
              await window.firebaseService.deleteComment(postId, cid);
              tree.dataset.forceOpen = '1';
              await this.openAdvancedCommentsFallback(postId, tree, myUid);
            };
          }
        });
      };

      try { await renderTree(); } catch (_) { tree.innerHTML = '<div style="opacity:.8">Unable to load comments.</div>'; }
    }

    activatePostActions(container = document) {
      if (!this._postActionUnsubsByContainer) this._postActionUnsubsByContainer = new WeakMap();
      if (!this._postActionUnsubsByContainer.get(container)) this._postActionUnsubsByContainer.set(container, []);
      // Delegate clicks once per container to ensure handlers always work
      if (!container.__postActionsDelegated) {
        container.__postActionsDelegated = true;
        container.addEventListener('click', async (e) => {
          const likeEl = e.target.closest('.like-btn');
          const commentEl = e.target.closest('.comment-btn');
          const repostEl = e.target.closest('.repost-btn');
          const actionEl = likeEl || commentEl || repostEl;
          if (!actionEl || !container.contains(actionEl)) return;

          const actionsWrap = actionEl.closest('.post-actions');
          const postItem = actionEl.closest('.post-item');
          const pid = actionsWrap?.dataset.postId || postItem?.dataset.postId;
          if (!pid) return;

          let me = this.currentUser;
          if (!me) {
            try { me = await this.resolveCurrentUser(); this.currentUser = me; } catch(_) { return; }
          }
          if (!me || !me.uid) return;

          if (likeEl) {
            try {
              const likeRef = firebase.doc(window.firebaseService.db, 'posts', pid, 'likes', me.uid);
              const snap = await firebase.getDoc(likeRef);
              if (snap.exists()) { await firebase.deleteDoc(likeRef); }
              else { await firebase.setDoc(likeRef, { userId: me.uid, createdAt: new Date().toISOString() }); }
            } catch(_) {}
            return;
          }

          if (repostEl) {
            try {
              const repostRef = firebase.doc(window.firebaseService.db, 'posts', pid, 'reposts', me.uid);
              const snap = await firebase.getDoc(repostRef);
              if (snap.exists()) { await firebase.deleteDoc(repostRef); }
              else { await firebase.setDoc(repostRef, { userId: me.uid, createdAt: new Date().toISOString() }); }
            } catch(_) {}
            return;
          }

          if (commentEl) {
            // Advanced comments are normally bound per-post. Keep prompt fallback for basic containers.
            if (!container.__useAdvancedComments){
              const text = prompt('Add comment:');
              if (text && text.trim()) {
                try { await firebase.addDoc(firebase.collection(window.firebaseService.db, 'posts', pid, 'comments'), { userId: me.uid, text: text.trim(), createdAt: new Date().toISOString() }); } catch(_) {}
              }
            } else if (typeof commentEl.onclick !== 'function') {
              const tree = postItem?.querySelector('.comment-tree') || document.getElementById(`comments-${pid}`);
              if (!tree) return;
              try { await this.openAdvancedCommentsFallback(pid, tree, me.uid); } catch (_) {}
            }
            return;
          }
        });
      }

      container.querySelectorAll('.post-item').forEach(item => {
        const pid = item.dataset.postId || item.querySelector('.post-actions')?.dataset.postId;
        if (!pid) return;
        
        // Like
        const likeBtn = item.querySelector('.like-btn');
        const likeSpan = item.querySelector('.likes-count');
        const likeIcon = likeBtn?.querySelector('i');
        if (likeBtn) {
          const unsub = firebase.onSnapshot(
            firebase.collection(window.firebaseService.db, 'posts', pid, 'likes'),
            snap => { if (likeSpan) likeSpan.textContent = snap.size; },
            async ()=>{ try{ const s = await window.firebaseService.getPostStats(pid); if (likeSpan) likeSpan.textContent = `${s.likes||0}`; }catch(_){ } }
          );
          this._postActionUnsubsByContainer.get(container).push(unsub);
          // Clicks handled by delegated listener above
        }
        
        // Comment
        const commentBtn = item.querySelector('.comment-btn');
        const commentSpan = item.querySelector('.comments-count');
        if (commentBtn) {
          const unsub = firebase.onSnapshot(
            firebase.collection(window.firebaseService.db, 'posts', pid, 'comments'),
            snap => { if (commentSpan) commentSpan.textContent = snap.size; },
            ()=>{}
          );
          this._postActionUnsubsByContainer.get(container).push(unsub);
          // Clicks handled by delegated listener above
        }
        
        // Repost
        const repostBtn = item.querySelector('.repost-btn');
        const repostSpan = item.querySelector('.reposts-count');
        const repostIcon = repostBtn?.querySelector('i');
        if (repostBtn) {
          const unsub = firebase.onSnapshot(
            firebase.collection(window.firebaseService.db, 'posts', pid, 'reposts'),
            snap => { if (repostSpan) repostSpan.textContent = snap.size; },
            async ()=>{ try{ const s = await window.firebaseService.getPostStats(pid); if (repostSpan) repostSpan.textContent = `${s.reposts||0}`; }catch(_){ } }
          );
          this._postActionUnsubsByContainer.get(container).push(unsub);
          // Clicks handled by delegated listener above
        }
      });
    }
}

// Create global instance
window.dashboardManager = new DashboardManager();

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = DashboardManager;
}
