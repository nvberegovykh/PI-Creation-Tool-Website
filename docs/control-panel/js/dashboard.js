/**
 * Dashboard Module for Liber Apps Control Panel
 * Handles navigation, overview, and dashboard functionality
 */

class DashboardManager {
    constructor() {
        this.currentSection = 'apps';
        this.init();
    }

    renderPostMedia(media){
        const urls = Array.isArray(media) ? media : (media ? [media] : []);
        if (!urls.length) return '';
        const renderOne = (url)=>{
            const lower = (url||'').toLowerCase();
            const isImg = ['.png','.jpg','.jpeg','.gif','.webp','.avif'].some(ext=> lower.endsWith(ext));
            const isVid = ['.mp4','.webm','.mov','.mkv'].some(ext=> lower.endsWith(ext));
            const isAud = ['.mp3','.wav','.m4a','.aac','.ogg','.oga','.weba'].some(ext=> lower.endsWith(ext));
            if (isImg){
                return `<img src="${url}" alt="media" style="max-width:100%;height:auto;border-radius:12px" />`;
            }
            if (isVid){
                return `<div class="player-card"><video src="${url}" class="player-media" controls playsinline></video><div class="player-bar"><button class="btn-icon" data-action="play"><i class="fas fa-play"></i></button><div class="progress"><div class="fill"></div></div><div class="time"></div></div></div>`;
            }
            if (isAud){
                return `<div class="player-card"><audio src="${url}" class="player-media" preload="metadata" ></audio><div class="player-bar"><button class="btn-icon" data-action="play"><i class="fas fa-play"></i></button><div class="progress"><div class="fill"></div></div><div class="time"></div></div></div>`;
            }
            if (lower.endsWith('.pdf')){
                return `<iframe src="${url}" style="width:100%;height:420px;border:none"></iframe>`;
            }
            return `<a href="${url}" target="_blank" rel="noopener noreferrer">Open attachment</a>`;
        };
        if (urls.length === 1){
            return `<div style="margin-top:8px">${renderOne(urls[0])}</div>`;
        }
        // simple gallery
        const items = urls.map(u=> `<div style="flex:0 0 auto;max-width:100%;">${renderOne(u)}</div>`).join('');
        return `<div style="margin-top:8px;overflow:auto;-webkit-overflow-scrolling:touch"><div style="display:flex;gap:8px">${items}</div></div>`;
    }

    showMiniPlayer(mediaEl, meta={}){
        try{
            if (this._currentPlayer && this._currentPlayer !== mediaEl){ try{ this._currentPlayer.pause(); }catch(_){} }
            this._currentPlayer = mediaEl;
            const mini = document.getElementById('mini-player'); if (!mini) return;
            const mTitle = document.getElementById('mini-title');
            const mBy = document.getElementById('mini-by');
            const mCover = mini.querySelector('.cover');
            const playBtn = document.getElementById('mini-play');
            const closeBtn = document.getElementById('mini-close');
            if (mTitle) mTitle.textContent = meta.title || 'Now playing';
            if (mBy) mBy.textContent = meta.by || '';
            if (mCover && meta.cover) mCover.src = meta.cover;
            mini.classList.add('show');
            if (playBtn){ playBtn.onclick = ()=>{ if (mediaEl.paused){ mediaEl.play(); playBtn.innerHTML='<i class="fas fa-pause"></i>'; } else { mediaEl.pause(); playBtn.innerHTML='<i class="fas fa-play"></i>'; } }; }
            if (closeBtn){ closeBtn.onclick = ()=>{ mini.classList.remove('show'); try{ mediaEl.pause(); }catch(_){} }; }
        }catch(_){ }
    }

    // Activate custom players inside a container (audio/video unified controls)
    activatePlayers(root=document){
        try{
            root.querySelectorAll('.player-card').forEach(card=>{
                const media = card.querySelector('.player-media');
                const btn = card.querySelector('.btn-icon');
                const fill = card.querySelector('.progress .fill');
                let knob = card.querySelector('.progress .knob');
                if (!knob){ const k = document.createElement('div'); k.className='knob'; const bar = card.querySelector('.progress'); if (bar){ bar.appendChild(k); knob = k; } }
                const time = card.querySelector('.time');
                const fmt = (s)=>{ const m=Math.floor(s/60); const ss=Math.floor(s%60).toString().padStart(2,'0'); return `${m}:${ss}`; };
                const sync = ()=>{ if (!media.duration) return; const p=(media.currentTime/media.duration)*100; if (fill) fill.style.width = `${p}%`; if (knob){ knob.style.left = `${p}%`; } if (time) time.textContent = `${fmt(media.currentTime)} / ${fmt(media.duration)}`; };
                if (btn){ btn.onclick = ()=>{ if (media.paused){ media.play(); btn.innerHTML='<i class="fas fa-pause"></i>'; } else { media.pause(); btn.innerHTML='<i class="fas fa-play"></i>'; } }; }
                media.addEventListener('timeupdate', sync);
                media.addEventListener('loadedmetadata', sync);
                const bar = card.querySelector('.progress');
                if (bar){
                    const seekTo = (clientX)=>{
                        const rect = bar.getBoundingClientRect();
                        const ratio = Math.min(1, Math.max(0, (clientX-rect.left)/rect.width));
                        if (media.duration) media.currentTime = ratio * media.duration;
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

                // hook up mini player when playing
                const mini = document.getElementById('mini-player');
                const mTitle = document.getElementById('mini-title');
                const mBy = document.getElementById('mini-by');
                const mCover = mini && mini.querySelector('.cover');
                const playBtn = document.getElementById('mini-play');
                const closeBtn = document.getElementById('mini-close');
                const showMini = ()=>{
                    if (!mini) return;
                    mini.classList.add('show');
                    const parent = card.closest('.post-item');
                    if (parent){
                        const t = parent.querySelector('.post-text');
                        mTitle && (mTitle.textContent = (t && t.textContent) ? t.textContent.slice(0,50) : 'Now playing');
                    }
                    if (mBy){ const by = card.closest('.post-item')?.querySelector('.byline')?.textContent || ''; mBy.textContent = by; }
                };
                media.addEventListener('play', showMini);
                if (playBtn){ playBtn.onclick = ()=>{ if (media.paused){ media.play(); playBtn.innerHTML='<i class="fas fa-pause"></i>'; } else { media.pause(); playBtn.innerHTML='<i class="fas fa-play"></i>'; } }; }
                if (closeBtn){ closeBtn.onclick = ()=>{ if (mini) mini.classList.remove('show'); try{ media.pause(); }catch(_){} }; }
            });
        }catch(_){ }
    }

    /**
     * Initialize the dashboard
     */
    init() {
        this.setupEventListeners();
        this.switchSection('apps');
        this.updateNavigation();
        this.handleWallETransitionToDashboard();
        // Service worker registration (best-effort)
        if ('serviceWorker' in navigator){ navigator.serviceWorker.register('/sw.js').catch(()=>{}); }
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
    /**
     * Load Personal Space
     */
    async loadSpace(){
        try{
            if (!(window.firebaseService && window.firebaseService.isFirebaseAvailable())) return;
            const user = await window.firebaseService.getCurrentUser();
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
                spaceSearch.oninput = async (e)=>{
                    const term = (e.target.value||'').trim().toLowerCase();
                    const resultsEl = document.getElementById('space-search-results');
                    if (!term){ if(resultsEl){ resultsEl.style.display='none'; resultsEl.innerHTML=''; } return; }
                    const users = await window.firebaseService.searchUsers(term);
                    if (resultsEl){
                        resultsEl.innerHTML = '';
                        (users||[]).slice(0,10).forEach(u=>{
                            const li = document.createElement('li');
                            li.style.cursor = 'pointer';
                            li.innerHTML = `<img class="avatar" src="${u.avatarUrl||'images/default-bird.png'}" alt=""><div><div class="uname">${u.username||''}</div><div class="email">${u.email||''}</div></div>`;
                            li.onclick = ()=>{ this.showUserPreviewModal(u); resultsEl.style.display='none'; };
                            resultsEl.appendChild(li);
                        });
                        resultsEl.style.display = (users && users.length) ? 'block':'none';
                    }
                };
            }

            this.showUserPreviewModal = async (u)=>{
                const uid = u.uid||u.id;
                const data = u.username ? u : (await window.firebaseService.getUserData(uid))||{};
                const me = await window.firebaseService.getCurrentUser();
                let isFollowing = false;
                try{ const following = await window.firebaseService.getFollowingIds(me.uid); isFollowing = (following||[]).includes(uid);}catch(_){ }
                const overlay = document.createElement('div');
                overlay.className='modal-overlay';
                overlay.innerHTML = `
                  <div class="modal" style="max-width:720px">
                    <div class="modal-header"><h3>${data.username||data.email||'User'}</h3><button class="modal-close">&times;</button></div>
                    <div class="modal-body">
                      <div style="display:flex;gap:16px;align-items:center;margin-bottom:12px">
                        <img src="${data.avatarUrl||'images/default-bird.png'}" style="width:64px;height:64px;border-radius:12px;object-fit:cover">
                        <div style="flex:1">
                          <div style="font-weight:700">${data.username||''}</div>
                          <div style="opacity:.8">${data.email||''}</div>
                        </div>
                        <button id="follow-toggle" class="btn ${isFollowing?'btn-secondary':'btn-primary'}">${isFollowing?'Unfollow':'Follow'}</button>
                        <button id="start-chat" class="btn btn-secondary"><i class="fas fa-comments"></i> Start chat</button>
                      </div>
                      <div id="preview-feed"></div>
                    </div>
                  </div>`;
                document.body.appendChild(overlay);
                overlay.querySelector('.modal-close').onclick = ()=> overlay.remove();
                overlay.addEventListener('click', (e)=>{ if (e.target.classList.contains('modal-overlay')) overlay.remove(); });
                const toggle = overlay.querySelector('#follow-toggle');
                toggle.onclick = async ()=>{
                  try{
                    if (toggle.textContent==='Follow'){ await window.firebaseService.followUser(me.uid, uid); toggle.textContent='Unfollow'; toggle.className='btn btn-secondary'; }
                    else { await window.firebaseService.unfollowUser(me.uid, uid); toggle.textContent='Follow'; toggle.className='btn btn-primary'; }
                  }catch(_){ }
                };
                const chatBtn = overlay.querySelector('#start-chat');
                if (chatBtn){ chatBtn.onclick = async ()=>{
                  try{
                    const key = [me.uid, uid].sort().join('|');
                    window.location.href = `apps/secure-chat/index.html?connId=${encodeURIComponent(key)}`;
                  }catch(_){ }
                }; }
                // Load recent public posts for preview
                const feed = overlay.querySelector('#preview-feed');
                try{
                  const q = firebase.query(
                    firebase.collection(window.firebaseService.db,'posts'),
                    firebase.where('authorId','==', uid),
                    firebase.where('visibility','==','public'),
                    firebase.orderBy('createdAtTS','desc'),
                    firebase.limit(10)
                  );
                  const s = await firebase.getDocs(q);
                  const list=[]; s.forEach(d=> list.push(d.data()));
                  feed.innerHTML = list.map(p=>`<div class="post-item" style="border:1px solid var(--border-color);border-radius:12px;padding:12px;margin:10px 0;background:var(--secondary-bg)">${(p.text||'').replace(/</g,'&lt;')}</div>`).join('') || '<div style="opacity:.8">No public posts yet.</div>';
                }catch(_){ feed.innerHTML = '<div style="opacity:.8">Unable to load posts.</div>'; }
            };

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
        try{
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
            snap.forEach(d=>{
                const p = d.data();
                const div = document.createElement('div');
                div.className = 'post-item';
                div.style.cssText = 'border:1px solid var(--border-color);border-radius:12px;padding:12px;margin:10px 0;background:var(--secondary-bg)';
                const by = p.authorName ? `<div class=\"byline\" style=\"display:flex;align-items:center;gap:8px;margin:4px 0\"><img src=\"${p.coverUrl||p.thumbnailUrl||'images/default-bird.png'}\" alt=\"cover\" style=\"width:20px;height:20px;border-radius:50%;object-fit:cover\"><span style=\"font-size:12px;color:#aaa\">by ${(p.authorName||'').replace(/</g,'&lt;')}</span></div>` : '';
                const media = (p.media || p.mediaUrl) ? this.renderPostMedia(p.media || p.mediaUrl) : '';
                div.innerHTML = `<div>${(p.text||'').replace(/</g,'&lt;')}</div>${by}${media}
                                 <div class=\"post-actions\" data-post-id=\"${p.id}\" style=\"margin-top:8px;display:flex;gap:14px;align-items:center\">\n                                   <i class=\"fas fa-heart like-btn\" title=\"Like\" style=\"cursor:pointer\"></i>\n                                   <span class=\"likes-count\"></span>\n                                   <i class=\"fas fa-comment comment-btn\" title=\"Comments\" style=\"cursor:pointer\"></i>\n                                   <i class=\"fas fa-retweet repost-btn\" title=\"Repost\" style=\"cursor:pointer\"></i>\n                                   <span class=\"reposts-count\"></span>\n                                   <button class=\"btn btn-secondary visibility-btn\">${p.visibility==='public'?'Make Private':'Make Public'}</button>\n                                 </div>\n                                 <div class=\"comment-tree\" id=\"comments-${p.id}\" style=\"display:none\"></div>`;
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
            const meUser = await window.firebaseService.getCurrentUser();
            document.querySelectorAll('#space-section .post-actions').forEach(async (pa)=>{
                const postId = pa.getAttribute('data-post-id');
                const likeBtn = pa.querySelector('.like-btn');
                const commentBtn = pa.querySelector('.comment-btn');
                const repostBtn = pa.querySelector('.repost-btn');
                const visBtn = pa.querySelector('.visibility-btn');
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
                            const me = await window.firebaseService.getCurrentUser();
                            const stats = await window.firebaseService.getPostStats(postId);
                            // naive toggle: if I already reposted, unRepost; else repost
                            // we don't have hasReposted; attempt delete then add fallback
                            try{ await window.firebaseService.unRepost(postId, me.uid); }catch(_){ await window.firebaseService.repost(postId, me.uid); }
                            const s3 = await window.firebaseService.getPostStats(postId); if (rCount) rCount.textContent = `${s3.reposts||0}`;
                        }catch(_){ }
                    };
                }
                commentBtn.onclick = async ()=>{
                    const tree = document.getElementById(`comments-${postId}`);
                    if (!tree) return;
                    if (tree.style.display === 'none'){ tree.style.display='block'; } else { tree.style.display='none'; return; }
                    tree.innerHTML = '';
                    const comments = await window.firebaseService.getComments(postId, 100);
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
                        if (inp){ inp.onkeydown = async (e)=>{ if (e.key==='Enter' && inp.value.trim()){ const meU = await window.firebaseService.getCurrentUser(); await window.firebaseService.addComment(postId, meU.uid, inp.value.trim(), node.id); inp.value=''; await commentBtn.onclick(); } }; }
                        if (node.children && node.children.length){
                            const sub = document.createElement('div'); sub.className='comment-tree'; item.appendChild(sub);
                            node.children.forEach(ch=> renderNode(ch, sub));
                        }
                    };
                    roots.reverse().forEach(n=> renderNode(n, tree));
                    // Inline add comment (top-level)
                    const addWrap = document.createElement('div');
                    addWrap.style.cssText = 'margin-top:8px';
                    addWrap.innerHTML = `<input type="text" class="reply-input" id="add-comment-${postId}" placeholder="Add a comment..." style="width:100%">`;
                    tree.appendChild(addWrap);
                    const addInp = document.getElementById(`add-comment-${postId}`);
                    if (addInp){ addInp.onkeydown = async (e)=>{ if (e.key==='Enter' && addInp.value.trim()){ const meU = await window.firebaseService.getCurrentUser(); await window.firebaseService.addComment(postId, meU.uid, addInp.value.trim(), null); addInp.value=''; await commentBtn.onclick(); } }; }
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
                    visBtn.onclick = async ()=>{
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
            const me = await window.firebaseService.getCurrentUser(); if (!me) return;
            const list = document.getElementById('space-connections-list'); if (!list) return;
            list.innerHTML = '';
            let snap;
            try{
                const q = firebase.query(firebase.collection(window.firebaseService.db,'chatConnections'), firebase.where('participants','array-contains', me.uid), firebase.orderBy('updatedAt','desc'), firebase.limit(50));
                snap = await firebase.getDocs(q);
            }catch{
                const q2 = firebase.query(firebase.collection(window.firebaseService.db,'chatConnections'), firebase.where('participants','array-contains', me.uid));
                snap = await firebase.getDocs(q2);
            }
            const myData = await window.firebaseService.getUserData(me.uid) || {};
            const myNameLower = (myData.username||'').toLowerCase();
            snap.forEach(d=>{
                const c = d.data();
                const li = document.createElement('li');
                let label = c.id;
                if (Array.isArray(c.participantUsernames) && c.participantUsernames.length){
                    const other = c.participantUsernames.find(n=> (n||'').toLowerCase() !== myNameLower);
                    if (other) label = other;
                }
                li.textContent = label;
                li.style.cursor = 'pointer';
                li.onclick = ()=>{ window.location.href = `apps/secure-chat/index.html?connId=${encodeURIComponent(c.id)}`; };
                list.appendChild(li);
            });
        }catch(_){ }
    }

    async loadGlobalFeed(){
        try{
            const feedEl = document.getElementById('global-feed');
            const suggEl = document.getElementById('global-suggestions');
            if (!feedEl) return;
            feedEl.innerHTML = '';
            // Recent public posts
            const snap = await firebase.getDocs(firebase.query(
                firebase.collection(window.firebaseService.db,'posts'),
                firebase.where('visibility','==','public')
            ));
            const list = []; snap.forEach(d=> list.push(d.data()));
            list.sort((a,b)=> (b.createdAtTS?.toMillis?.()||0) - (a.createdAtTS?.toMillis?.()||0) || new Date(b.createdAt||0) - new Date(a.createdAt||0));
            list.slice(0,20).forEach(p=>{
                const div = document.createElement('div');
                div.className = 'post-item';
                div.style.cssText = 'border:1px solid var(--border-color);border-radius:12px;padding:12px;margin:10px 0;background:var(--secondary-bg)';
                const media = (p.media || p.mediaUrl) ? this.renderPostMedia(p.media || p.mediaUrl) : '';
                div.innerHTML = `<div>${(p.text||'').replace(/</g,'&lt;')}</div>${media}`;
                feedEl.appendChild(div);
            });
            if (suggEl){
                const trending = await window.firebaseService.getTrendingPosts('', 10);
                suggEl.innerHTML = trending.map(tp=>`<div class="post-item" style="border:1px solid var(--border-color);border-radius:12px;padding:10px;margin:8px 0">${(tp.text||'').replace(/</g,'&lt;')}</div>`).join('');
            }
            this.activatePostActions(feedEl);  // Activate actions after rendering
        }catch(_){ }
    }

    async loadFeed(uid, titleName){
        const feed = document.getElementById('space-feed');
        const feedTitle = document.getElementById('space-feed-title');
        if (!feed) return;
        feed.innerHTML = '';
        try{
            const meUser = await window.firebaseService.getCurrentUser();
            const following = await window.firebaseService.getFollowingIds(meUser.uid);
            const fb = document.getElementById('follow-btn');
            const ub = document.getElementById('unfollow-btn');
            if (fb && ub){
                if (uid !== meUser.uid){
                    const isFollowing = following.includes(uid);
                    fb.style.display = isFollowing? 'none':'inline-block';
                    ub.style.display = isFollowing? 'inline-block':'none';
                    fb.onclick = async ()=>{ await window.firebaseService.followUser(meUser.uid, uid); this.loadFeed(uid, titleName); };
                    ub.onclick = async ()=>{ await window.firebaseService.unfollowUser(meUser.uid, uid); this.loadFeed(uid, titleName); };
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
                                 <div class="post-actions" data-post-id="${p.id}" data-author="${p.authorId}" style="margin-top:8px;display:flex;gap:14px;align-items:center">
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
            const me2 = await window.firebaseService.getCurrentUser();
            document.querySelectorAll('.post-actions').forEach(async (pa)=>{
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
                if (await window.firebaseService.hasLiked(postId, me2.uid)){ likeBtn.classList.add('active'); likeBtn.style.color = '#ff4d4f'; }
                likeBtn.onclick = async ()=>{
                    const liked = await window.firebaseService.hasLiked(postId, me2.uid);
                    if (liked){ await window.firebaseService.unlikePost(postId, me2.uid); likeBtn.classList.remove('active'); likeBtn.style.color=''; }
                    else { await window.firebaseService.likePost(postId, me2.uid); likeBtn.classList.add('active'); likeBtn.style.color='#ff4d4f'; }
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
                    const tree = document.getElementById(`comments-${postId}`);
                    if (!tree) return;
                    if (tree.style.display === 'none'){ tree.style.display='block'; } else { tree.style.display='none'; return; }
                    tree.innerHTML = '';
                    const comments = await window.firebaseService.getComments(postId, 100);
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
                        if (inp){ inp.onkeydown = async (e)=>{ if (e.key==='Enter' && inp.value.trim()){ const meU = await window.firebaseService.getCurrentUser(); await window.firebaseService.addComment(postId, meU.uid, inp.value.trim(), node.id); inp.value=''; await commentBtn.onclick(); } }; }
                        if (node.children && node.children.length){
                            const sub = document.createElement('div'); sub.className='comment-tree'; item.appendChild(sub);
                            node.children.forEach(ch=> renderNode(ch, sub));
                        }
                    };
                    roots.reverse().forEach(n=> renderNode(n, tree));
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
            if (uid === meUser.uid){
                try{
                    const posts = await window.firebaseService.getFeedPosts(meUser.uid, following, 10);
                    feed.innerHTML = '';
                    posts.forEach(p=>{
                        const div = document.createElement('div');
                        div.className = 'post-item';
                        div.style.cssText = 'border:1px solid var(--border-color);border-radius:12px;padding:12px;margin:10px 0;background:var(--secondary-bg)';
                        const media = (p.media || p.mediaUrl) ? this.renderPostMedia(p.media || p.mediaUrl) : '';
                        div.innerHTML = `<div>${(p.text||'').replace(/</g,'&lt;')}</div>${media}
                                         <div class="post-actions" data-post-id="${p.id}" data-author="${p.authorId}" style="margin-top:8px;display:flex;gap:10px;align-items:center">
                                           <i class="fas fa-heart like-btn" style="cursor:pointer"></i>
                                           <span class="likes-count"></span>
                                           <i class="fas fa-comment comment-btn" style="cursor:pointer"></i>
                                           <button class="btn btn-secondary visibility-btn">${p.visibility==='public'?'Make Private':'Make Public'}</button>
                                           <i class="fas fa-edit edit-post-btn" title="Edit" style="cursor:pointer"></i>
                                           <i class="fas fa-trash delete-post-btn" title="Delete" style="cursor:pointer"></i>
                                         </div>
                                         <div class="comment-tree" id="comments-${p.id}" style="display:none"></div>`;
                        feed.appendChild(div);
                    });
                    // bind actions
                    document.querySelectorAll('.post-actions').forEach(async (pa)=>{
                        const postId = pa.getAttribute('data-post-id');
                        const likeBtn = pa.querySelector('.like-btn');
                        const commentBtn = pa.querySelector('.comment-btn');
                        const visBtn = pa.querySelector('.visibility-btn');
                        const editBtn = pa.querySelector('.edit-post-btn');
                        const delBtn = pa.querySelector('.delete-post-btn');
                        const likesCount = pa.querySelector('.likes-count');
                        const s = await window.firebaseService.getPostStats(postId); likesCount.textContent = `${s.likes||0} likes`;
                        if (await window.firebaseService.hasLiked(postId, meUser.uid)){ likeBtn.classList.add('active'); likeBtn.style.color = '#ff4d4f'; }
                        likeBtn.onclick = async ()=>{
                            const liked = await window.firebaseService.hasLiked(postId, meUser.uid);
                            if (liked){ await window.firebaseService.unlikePost(postId, meUser.uid); likeBtn.classList.remove('active'); likeBtn.style.color=''; }
                            else { await window.firebaseService.likePost(postId, meUser.uid); likeBtn.classList.add('active'); likeBtn.style.color='#ff4d4f'; }
                            const s2 = await window.firebaseService.getPostStats(postId); likesCount.textContent = `${s2.likes||0} likes`;
                        };
                        commentBtn.onclick = async ()=>{
                            const tree = document.getElementById(`comments-${postId}`);
                            if (!tree) return;
                            if (tree.style.display === 'none'){ tree.style.display='block'; } else { tree.style.display='none'; return; }
                            tree.innerHTML = '';
                            const comments = await window.firebaseService.getComments(postId, 100);
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
                                if (inp){ inp.onkeydown = async (e)=>{ if (e.key==='Enter' && inp.value.trim()){ const meU = await window.firebaseService.getCurrentUser(); await window.firebaseService.addComment(postId, meU.uid, inp.value.trim(), node.id); inp.value=''; await commentBtn.onclick(); } }; }
                                if (node.children && node.children.length){
                                    const sub = document.createElement('div'); sub.className='comment-tree'; item.appendChild(sub);
                                    node.children.forEach(ch=> renderNode(ch, sub));
                                }
                            };
                            roots.reverse().forEach(n=> renderNode(n, tree));
                            const addWrap = document.createElement('div'); addWrap.style.cssText='margin-top:8px';
                            addWrap.innerHTML = `<input type="text" class="reply-input" id="add-comment-${postId}" placeholder="Add a comment..." style="width:100%">`;
                            tree.appendChild(addWrap);
                            const addInp = document.getElementById(`add-comment-${postId}`);
                            if (addInp){ addInp.onkeydown = async (e)=>{ if (e.key==='Enter' && addInp.value.trim()){ const meU = await window.firebaseService.getCurrentUser(); await window.firebaseService.addComment(postId, meU.uid, addInp.value.trim(), null); addInp.value=''; await commentBtn.onclick(); } }; }
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
                            visBtn.onclick = async ()=>{
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
                        // post edit/delete for owner only
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
                }catch(_){ /* ignore */ }
            }
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
                        if (!file){ return this.showError('Select an MP3 file'); }
                        if (file.type !== 'audio/mpeg' || file.size > 25*1024*1024){ return this.showError('MP3 only, up to 25 MB'); }
                        const title = (document.getElementById('wave-title').value||file.name).trim();
                        const s = firebase.getStorage();
                        const ref = firebase.ref(s, `wave/${me.uid}/${Date.now()}_${file.name.replace(/[^a-zA-Z0-9._-]/g,'_')}`);
                        await firebase.uploadBytes(ref, file, { contentType: 'audio/mpeg' });
                        const url = await firebase.getDownloadURL(ref);
                        let authorName = (await window.firebaseService.getUserData(me.uid))?.username || me.email || 'Unknown';
                        let coverUrl = (await window.firebaseService.getUserData(me.uid))?.avatarUrl || '';
                        const docRef = firebase.doc(firebase.collection(window.firebaseService.db, 'wave'));
                        await firebase.setDoc(docRef, { id: docRef.id, owner: me.uid, title, url, createdAt: new Date().toISOString(), authorId: me.uid, authorName, coverUrl });
                        this.showSuccess('Uploaded');
                        this.renderWaveLibrary(me.uid);
                    }catch(e){ this.showError('Upload failed'); }
                };
            }
            const search = document.getElementById('wave-search');
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
            await this.renderWaveLibrary(me.uid);
        }catch(_){ }
    }

    async renderWaveLibrary(uid){
        const lib = document.getElementById('wave-library'); if (!lib) return;
        lib.innerHTML = '';
        try{
            const q = firebase.query(firebase.collection(window.firebaseService.db,'wave'), firebase.where('owner','==', uid), firebase.orderBy('createdAt','desc'), firebase.limit(50));
            const snap = await firebase.getDocs(q);
            snap.forEach(d=> lib.appendChild(this.renderWaveItem(d.data())));
        }catch{
            const q2 = firebase.query(firebase.collection(window.firebaseService.db,'wave'), firebase.where('owner','==', uid));
            const s2 = await firebase.getDocs(q2); s2.forEach(d=> lib.appendChild(this.renderWaveItem(d.data())));
        }
    }

    renderWaveItem(w){
        const div = document.createElement('div');
        div.className = 'wave-item';
        div.style.cssText = 'border:1px solid var(--border-color);border-radius:10px;padding:10px;margin:8px 0;display:flex;gap:10px;align-items:center;justify-content:space-between';
        const cover = w.coverUrl || 'images/default-bird.png';
        const byline = w.authorName ? `<div style="font-size:12px;color:#aaa">by ${(w.authorName||'').replace(/</g,'&lt;')}</div>` : '';
        div.innerHTML = `<div style="display:flex;gap:10px;align-items:center"><img src="${cover}" alt="cover" style="width:48px;height:48px;border-radius:8px;object-fit:cover"><div><div>${(w.title||'Untitled').replace(/</g,'&lt;')}</div>${byline}</div></div><audio class="liber-lib-audio" src="${w.url}" controls style="max-width:260px"></audio><div style="display:flex;gap:8px"><button class="btn btn-secondary share-btn"><i class="fas fa-share"></i></button><button class="btn btn-secondary repost-btn" title="Repost"><i class="fas fa-retweet"></i></button></div>`;
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
                        const thumbnailUrl = (meProfile && meProfile.avatarUrl) || '';
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
                         <video class="liber-lib-video" src="${v.url}" controls playsinline style="width:100%;max-height:480px;border-radius:8px"></video>
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
    updateNavigation() {
        const currentUser = authManager.getCurrentUser();
        if (currentUser) {
            const adminElements = document.querySelectorAll('.admin-only');
            adminElements.forEach(el => {
                el.style.display = currentUser.role === 'admin' ? 'block' : 'none';
            });
        }
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
                if (emailEl) emailEl.value = user.email;
                if (unameEl) unameEl.value = data.username || '';
                if (verifiedEl) verifiedEl.textContent = user.emailVerified ? 'Verified' : 'Not verified';

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
                    rvBtn.onclick = async () => {
                        try { await window.firebaseService.sendEmailVerification(); this.showSuccess('Verification email sent'); }
                        catch { this.showError('Failed to send verification'); }
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
        
        if (!message) return;
        
        // Add user message to chat
        this.addWallEMessage('user', message);
        input.value = '';
        
        // Call WALL-E API
        if (window.wallE && typeof window.wallE.callWALLE === 'function') {
            try {
                const response = await window.wallE.callWALLE(message);
                this.addWallEMessage('assistant', response);
            } catch (error) {
                this.addWallEMessage('error', 'Sorry, I encountered an error. Please try again.');
            }
        } else {
            this.addWallEMessage('assistant', 'Sorry, WALL-E is not available at the moment.');
        }
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
        li.dataset.uid = u.uid;
        li.addEventListener('click', () => {
          this.showUserPreviewModal(u.uid);
        });
        resultsEl.appendChild(li);
      });
      // ... 
    }

    // Enhance showUserPreviewModal
    async showUserPreviewModal(userId) {
      const modal = document.createElement('div');
      modal.className = 'user-preview-modal';
      modal.innerHTML = `
        <div class="modal-content">
          <h3>User Profile</h3>
          <div id="preview-avatar"></div>
          <div id="preview-username"></div>
          <div id="preview-mood"></div>
          <button id="follow-btn">Follow</button>
          <div id="preview-posts"></div>
          <button class="close-btn">Close</button>
        </div>
      `;
      document.body.appendChild(modal);
      
      const userData = await firebaseService.getUserData(userId);
      document.getElementById('preview-username').textContent = userData.username;
      document.getElementById('preview-mood').textContent = userData.mood;
      document.getElementById('preview-avatar').innerHTML = `<img src="${userData.avatarUrl || 'default.png'}">`;
      
      const followBtn = document.getElementById('follow-btn');
      // Check if following and set text
      followBtn.onclick = () => this.toggleFollow(userId);
      
      const postsEl = document.getElementById('preview-posts');
      const q = firebase.query(firebase.collection(this.db, 'posts'), firebase.where('authorId', '==', userId), firebase.where('visibility', '==', 'public'), firebase.orderBy('createdAtTS', 'desc'));
      const snap = await firebase.getDocs(q);
      snap.forEach(doc => {
        const post = doc.data();
        const item = document.createElement('div');
        item.className = 'post-item';
        item.innerHTML = `
          <div class="post-text">${post.text}</div>
          ${this.renderPostMedia(post.media)}
        `;
        postsEl.appendChild(item);
      });
      
      modal.querySelector('.close-btn').onclick = () => modal.remove();
    }

    activatePostActions(container = document) {
      container.querySelectorAll('.post-item').forEach(item => {
        const pid = item.dataset.postId;
        if (!pid) return;
        
        // Like
        const likeBtn = item.querySelector('.like-btn');
        const likeSpan = likeBtn?.querySelector('span');
        const likeIcon = likeBtn?.querySelector('i');
        if (likeBtn) {
          firebase.onSnapshot(firebase.collection(firebase.doc(this.db, 'posts', pid), 'likes'), snap => {
            likeSpan.textContent = snap.size;
          });
          likeBtn.onclick = async () => {
            // Toggle like logic
            const likeRef = firebase.doc(firebase.collection(firebase.doc(this.db, 'posts', pid), 'likes'), this.currentUser.uid);
            const snap = await firebase.getDoc(likeRef);
            if (snap.exists()) {
              await firebase.deleteDoc(likeRef);
              likeIcon.classList.remove('active');
            } else {
              await firebase.setDoc(likeRef, { userId: this.currentUser.uid, createdAt: new Date().toISOString() });
              likeIcon.classList.add('active');
            }
          };
        }
        
        // Comment
        const commentBtn = item.querySelector('.comment-btn');
        const commentSpan = commentBtn?.querySelector('span');
        if (commentBtn) {
          firebase.onSnapshot(firebase.collection(firebase.doc(this.db, 'posts', pid), 'comments'), snap => {
            commentSpan.textContent = snap.size;
          });
          commentBtn.onclick = () => {
            // Simple modal stub - expand as needed
            const comment = prompt('Add comment:');
            if (comment) {
              firebase.addDoc(firebase.collection(firebase.doc(this.db, 'posts', pid), 'comments'), {
                userId: this.currentUser.uid,
                text: comment,
                createdAt: new Date().toISOString()
              });
            }
          };
        }
        
        // Repost
        const repostBtn = item.querySelector('.repost-btn');
        const repostSpan = repostBtn?.querySelector('span');
        const repostIcon = repostBtn?.querySelector('i');
        if (repostBtn) {
          firebase.onSnapshot(firebase.collection(firebase.doc(this.db, 'posts', pid), 'reposts'), snap => {
            repostSpan.textContent = snap.size;
          });
          repostBtn.onclick = async () => {
            const repostRef = firebase.doc(firebase.collection(firebase.doc(this.db, 'posts', pid), 'reposts'), this.currentUser.uid);
            const snap = await firebase.getDoc(repostRef);
            if (snap.exists()) {
              await firebase.deleteDoc(repostRef);
              repostIcon.classList.remove('active');
            } else {
              await firebase.setDoc(repostRef, { userId: this.currentUser.uid, createdAt: new Date().toISOString() });
              repostIcon.classList.add('active');
            }
          };
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
