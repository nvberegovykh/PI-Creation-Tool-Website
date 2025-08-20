/**
 * Dashboard Module for Liber Apps Control Panel
 * Handles navigation, overview, and dashboard functionality
 */

class DashboardManager {
    constructor() {
        this.currentSection = 'apps';
        this.init();
    }

    renderPostMedia(url){
        const lower = (url||'').toLowerCase();
        if (lower.endsWith('.png') || lower.endsWith('.jpg') || lower.endsWith('.jpeg') || lower.endsWith('.gif') || lower.endsWith('.webp')){
            return `<div style="margin-top:8px"><img src="${url}" alt="media" style="max-width:100%;border-radius:8px" /></div>`;
        }
        if (lower.endsWith('.mp4') || lower.endsWith('.webm')){
            return `<div style="margin-top:8px"><video src="${url}" style="max-width:100%;border-radius:8px" controls playsinline></video></div>`;
        }
        if (lower.endsWith('.mp3') || lower.endsWith('.wav') || lower.endsWith('.m4a')){
            return `<div style="margin-top:8px"><audio src="${url}" style="width:100%" controls></audio></div>`;
        }
        return `<div style="margin-top:8px"><a href="${url}" target="_blank" rel="noopener noreferrer">Open attachment</a></div>`;
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
                    let mediaUrl = '';
                    if (mediaInput && mediaInput.files && mediaInput.files[0] && firebase.getStorage){
                        try{
                            const file = mediaInput.files[0];
                            const s = firebase.getStorage();
                            const postIdRef = firebase.doc(firebase.collection(window.firebaseService.db, 'posts'));
                            const ext = (file.name.split('.').pop()||'jpg').toLowerCase();
                            const path = `posts/${user.uid}/${postIdRef.id}/media.${ext}`;
                            const r = firebase.ref(s, path);
                            await firebase.uploadBytes(r, file, { contentType: file.type||'application/octet-stream' });
                            mediaUrl = await firebase.getDownloadURL(r);
                            // We will reuse the generated postIdRef for the post document below
                            const payload = { id: postIdRef.id, authorId: user.uid, text, mediaUrl, createdAt: new Date().toISOString() };
                            await firebase.setDoc(postIdRef, payload);
                            document.getElementById('space-post-text').value='';
                            mediaInput.value='';
                            this.showSuccess('Posted');
                            this.loadFeed(user.uid);
                            return;
                        }catch(e){ /* fallback to text-only below */ }
                    }
                    if (!text){ this.showError('Add some text or attach media'); return; }
                    const newRef = firebase.doc(firebase.collection(window.firebaseService.db, 'posts'));
                    await firebase.setDoc(newRef, { id: newRef.id, authorId: user.uid, text, createdAt: new Date().toISOString() });
                    document.getElementById('space-post-text').value='';
                    if (mediaInput) mediaInput.value='';
                    this.showSuccess('Posted');
                    this.loadFeed(user.uid);
                };
            }

            const spaceSearch = document.getElementById('space-search');
            if (spaceSearch){
                spaceSearch.oninput = async (e)=>{
                    const term = (e.target.value||'').trim().toLowerCase();
                    const resultsEl = document.getElementById('space-search-results');
                    if (!term){ if(resultsEl){ resultsEl.style.display='none'; resultsEl.innerHTML=''; } this.loadFeed(user.uid); return; }
                    const users = await window.firebaseService.searchUsers(term);
                    if (resultsEl){
                        resultsEl.innerHTML = '';
                        (users||[]).slice(0,10).forEach(u=>{
                            const li = document.createElement('li');
                            li.textContent = u.username || u.email;
                            li.style.cursor = 'pointer';
                            li.onclick = ()=>{ this.loadFeed(u.uid||u.id, u.username||u.email); resultsEl.style.display='none'; };
                            resultsEl.appendChild(li);
                        });
                        resultsEl.style.display = (users && users.length) ? 'block':'none';
                    }
                };
            }

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

            // Initial feed load
            this.loadFeed(user.uid);
        }catch(e){ console.error('loadSpace error', e); }
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
                const q = firebase.query(firebase.collection(window.firebaseService.db,'posts'), firebase.where('authorId','==', uid), firebase.orderBy('createdAt','desc'), firebase.limit(50));
                snap = await firebase.getDocs(q);
            } catch {
                const q2 = firebase.query(firebase.collection(window.firebaseService.db,'posts'), firebase.where('authorId','==', uid));
                snap = await firebase.getDocs(q2);
                // Normalize to similar interface
                snap = { docs: snap.docs.sort((a,b)=> new Date((b.data()||{}).createdAt||0) - new Date((a.data()||{}).createdAt||0)), forEach: (cb)=> snap.docs.forEach(cb) };
            }
            snap.forEach(d=>{
                const p = d.data();
                const div = document.createElement('div');
                div.className = 'post-item';
                div.style.cssText = 'border:1px solid var(--border-color);border-radius:12px;padding:12px;margin:10px 0;background:var(--secondary-bg)';
                const media = p.mediaUrl ? this.renderPostMedia(p.mediaUrl) : '';
                div.innerHTML = `<div>${(p.text||'').replace(/</g,'&lt;')}</div>${media}
                                 <div class=\"post-actions\" data-post-id=\"${p.id}\" style=\"margin-top:8px\">
                                   <button class=\"btn btn-secondary like-btn\">Like</button>
                                   <button class=\"btn btn-secondary comment-btn\">Comment</button>
                                   <span class=\"likes-count\"></span>
                                 </div>`;
                feed.appendChild(div);
            });
            // Bind like/comment actions
            const me2 = await window.firebaseService.getCurrentUser();
            document.querySelectorAll('.post-actions').forEach(async (pa)=>{
                const postId = pa.getAttribute('data-post-id');
                const likeBtn = pa.querySelector('.like-btn');
                const commentBtn = pa.querySelector('.comment-btn');
                const likesCount = pa.querySelector('.likes-count');
                const stats = await window.firebaseService.getPostStats(postId); likesCount.textContent = `${stats.likes||0} likes`;
                if (await window.firebaseService.hasLiked(postId, me2.uid)){ likeBtn.textContent = 'Unlike'; }
                likeBtn.onclick = async ()=>{
                    const liked = await window.firebaseService.hasLiked(postId, me2.uid);
                    if (liked){ await window.firebaseService.unlikePost(postId, me2.uid); likeBtn.textContent = 'Like'; }
                    else { await window.firebaseService.likePost(postId, me2.uid); likeBtn.textContent = 'Unlike'; }
                    const s = await window.firebaseService.getPostStats(postId); likesCount.textContent = `${s.likes||0} likes`;
                };
                commentBtn.onclick = async ()=>{
                    const text = prompt('Write a comment'); if (!text) return;
                    await window.firebaseService.addComment(postId, me2.uid, text);
                    alert('Comment added');
                };
            });
            if (uid === meUser.uid){
                try{
                    const posts = await window.firebaseService.getFeedPosts(meUser.uid, following, 10);
                    feed.innerHTML = '';
                    posts.forEach(p=>{
                        const div = document.createElement('div');
                        div.className = 'post-item';
                        div.style.cssText = 'border:1px solid var(--border-color);border-radius:12px;padding:12px;margin:10px 0;background:var(--secondary-bg)';
                        const media = p.mediaUrl ? this.renderPostMedia(p.mediaUrl) : '';
                        div.innerHTML = `<div>${(p.text||'').replace(/</g,'&lt;')}</div>${media}
                                         <div class=\"post-actions\" data-post-id=\"${p.id}\" style=\"margin-top:8px\">
                                           <button class=\"btn btn-secondary like-btn\">Like</button>
                                           <button class=\"btn btn-secondary comment-btn\">Comment</button>
                                           <span class=\"likes-count\"></span>
                                         </div>`;
                        feed.appendChild(div);
                    });
                    // bind actions
                    document.querySelectorAll('.post-actions').forEach(async (pa)=>{
                        const postId = pa.getAttribute('data-post-id');
                        const likeBtn = pa.querySelector('.like-btn');
                        const commentBtn = pa.querySelector('.comment-btn');
                        const likesCount = pa.querySelector('.likes-count');
                        const s = await window.firebaseService.getPostStats(postId); likesCount.textContent = `${s.likes||0} likes`;
                        if (await window.firebaseService.hasLiked(postId, meUser.uid)){ likeBtn.textContent = 'Unlike'; }
                        likeBtn.onclick = async ()=>{
                            const liked = await window.firebaseService.hasLiked(postId, meUser.uid);
                            if (liked){ await window.firebaseService.unlikePost(postId, meUser.uid); likeBtn.textContent = 'Like'; }
                            else { await window.firebaseService.likePost(postId, meUser.uid); likeBtn.textContent = 'Unlike'; }
                            const s2 = await window.firebaseService.getPostStats(postId); likesCount.textContent = `${s2.likes||0} likes`;
                        };
                        commentBtn.onclick = async ()=>{ const t = prompt('Write a comment'); if(!t) return; await window.firebaseService.addComment(postId, meUser.uid, t); alert('Comment added'); };
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

        // Force reload handler
        const forceBtn = document.getElementById('force-reload-btn');
        if (forceBtn){
            forceBtn.onclick = async ()=>{
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
            const settings = {
                sessionTimeout: parseInt(document.getElementById('session-timeout').value) * 60000,
                autoRefreshApps: document.getElementById('auto-refresh-apps').checked,
                showAppDescriptions: document.getElementById('show-app-descriptions').checked,
                enable2FA: document.getElementById('enable-2fa').checked,
                lastUpdated: new Date().toISOString()
            };

            localStorage.setItem('liber_settings', JSON.stringify(settings));
            
            // Apply session timeout change
            if (authManager.currentUser) {
                authManager.startSessionTimer();
            }

            this.showSuccess('Settings saved successfully');
        } catch (error) {
            console.error('Error saving settings:', error);
            this.showError('Failed to save settings');
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
}

// Create global instance
window.dashboardManager = new DashboardManager();

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = DashboardManager;
}
