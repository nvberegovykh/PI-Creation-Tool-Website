/**
 * Firebase Service for Liber Apps Control Panel
 * Handles user authentication and data storage with Firebase
 * Updated for Firebase Modular SDK v12.1.0
 * Full Firebase Database integration
 */

class FirebaseService {
    constructor() {
        this.app = null;
        this.auth = null;
        this.db = null;
        this.isInitialized = false;
        this.init();
    }

    /**
     * Register Firebase Messaging (Web) using VAPID key from secure keys
     */
    async registerMessaging(user){
        try{
            const keys = await window.secureKeyManager.getKeys();
            const vapid = keys && keys.messaging && keys.messaging.vapidPublicKey;
            if (!vapid) return;
            if (!firebase.getStorage || !window.firebaseModular || !window.firebaseModular.getMessaging) return;
            if (window.firebaseModular.isSupported && !(await window.firebaseModular.isSupported())) return;
            const messaging = window.firebaseModular.getMessaging(this.app);
            // Request notification permission
            if ('Notification' in window && Notification.permission !== 'granted'){
                try { await Notification.requestPermission(); } catch(_) {}
            }
            // Requires a service worker at /sw.js
            const token = await window.firebaseModular.getToken(messaging, { vapidKey: vapid, serviceWorkerRegistration: await navigator.serviceWorker.getRegistration() });
            if (!token) return;
            // Store token under user doc for later server-side delivery if needed
            try{
                const userDocRef = firebase.doc(this.db, 'users', user.uid);
                await firebase.updateDoc(userDocRef, { fcmToken: token, fcmUpdatedAt: new Date().toISOString() });
            }catch(_){/* ignore */}
        }catch(_){/* ignore */}
    }

    /**
     * Initialize Firebase
     */
    async init() {
        try {
            console.log('=== Firebase Service Initialization ===');
            
            // Wait for Firebase SDK to be available (modular SDK loads asynchronously)
            await this.waitForFirebaseSDK();
            
            console.log('✅ Firebase SDK is available');
            console.log('Firebase version:', firebase.SDK_VERSION);
            
            // Wait for secure keys to be loaded
            console.log('Waiting for secure keys...');
            await this.waitForSecureKeys();
            console.log('Secure keys loaded');
            
            // Get Firebase configuration from secure keys
            console.log('Getting Firebase config...');
            const firebaseConfig = await this.getFirebaseConfig();
            console.log('Firebase config loaded:', !!firebaseConfig);
            
            if (!firebaseConfig) {
                throw new Error('❌ Firebase configuration is required but not found in secure keys. Please add Firebase configuration to your Gist.');
            }

            // Validate Firebase config - only require essential fields
            const essentialFields = ['apiKey', 'projectId'];
            const missingFields = essentialFields.filter(field => !firebaseConfig[field]);
            if (missingFields.length > 0) {
                console.error('❌ Invalid Firebase config - missing essential fields:', missingFields);
                this.isInitialized = false;
                return;
            }
            
            // Initialize Firebase with modular SDK
            console.log('Initializing Firebase app...');
            this.app = firebase.initializeApp(firebaseConfig);
            console.log('Firebase app created with name:', this.app.name);

            // Initialize services with modular SDK
            console.log('Initializing Firebase services...');
            this.auth = firebase.auth(this.app);
            this.db = firebase.firestore(this.app);
            // Optional Functions (for future FCM/webhooks)
            try {
                // Determine preferred region from secure keys if provided
                let functionsRegion = 'us-central1';
                try {
                    const keys = await window.secureKeyManager.getKeys();
                    functionsRegion = keys.functionsRegion || keys.firebase?.functionsRegion || 'us-central1';
                } catch(_) { /* default */ }
                if (window.firebaseModular && window.firebaseModular.getFunctions) {
                    this.functions = window.firebaseModular.getFunctions(this.app, functionsRegion);
                } else if (firebase.functions) {
                    // Compat fallback if provided by loader
                    this.functions = firebase.functions(this.app);
                } else {
                    this.functions = null;
                }
            } catch(_){ this.functions = null; }
            
            // Set up persistence for better offline support
            try {
                // Prefer IndexedDB persistence if available; fallback to local storage
                // This helps survive reloads, app updates, and iOS webview quirks better
                if (firebase.inMemoryPersistence && firebase.indexedDBLocalPersistence) {
                    try {
                        await firebase.setPersistence(this.auth, firebase.indexedDBLocalPersistence);
                        console.log('✅ Auth persistence set to IndexedDB');
                    } catch (_) {
                        await firebase.setPersistence(this.auth, firebase.browserLocalPersistence);
                        console.log('✅ Auth persistence set to local storage');
                    }
                } else {
                    await firebase.setPersistence(this.auth, firebase.browserLocalPersistence);
                    console.log('✅ Auth persistence set to local storage');
                }
            } catch (error) {
                console.warn('⚠️ Auth persistence setup failed:', error.message);
            }
            
            // Add missing methods to auth object for compatibility
            this.auth.fetchSignInMethodsForEmail = (email) => {
                return firebase.fetchSignInMethodsForEmail(this.auth, email);
            };
            
            // Enable offline persistence for Firestore (modular API)
            try {
                if (firebase.enableMultiTabIndexedDbPersistence) {
                    await firebase.enableMultiTabIndexedDbPersistence(this.db);
                    console.log('✅ Firestore multi-tab offline persistence enabled');
                } else if (firebase.enableIndexedDbPersistence) {
                    await firebase.enableIndexedDbPersistence(this.db);
                    console.log('✅ Firestore offline persistence enabled');
                } else {
                    console.log('ℹ️ Firestore persistence APIs not exposed; skipping');
                }
            } catch (error) {
                console.warn('⚠️ Firestore offline persistence failed:', error.message);
            }
            
            this.isInitialized = true;
            console.log('✅ Firebase initialized successfully');

            // Notify the app that Firebase is ready
            try {
                window.firebaseServiceReady = true;
                window.dispatchEvent(new Event('firebase-ready'));
            } catch (e) {
                console.warn('Failed to dispatch firebase-ready event:', e?.message || e);
            }

            // Set up auth state listener
            firebase.onAuthStateChanged(this.auth, (user) => {
                if (window.__DEBUG_AUTH__) console.log('Auth state changed:', user ? 'User logged in' : 'User logged out');
                if (user) {
                    if (window.__DEBUG_AUTH__) {
                        console.log('Current user:', user.email);
                        console.log('User UID:', user.uid);
                        console.log('Email verified:', user.emailVerified);
                    }
                    
                    // Mirror minimal user info for account switcher
                    try {
                        const acct = {
                            uid: user.uid,
                            email: user.email || '',
                            username: '',
                            lastUsedAt: new Date().toISOString()
                        };
                        (async ()=>{
                            try{ const d = await this.getUserData(user.uid); acct.username = (d&&d.username)||acct.email; }catch(_){}
                            const raw = localStorage.getItem('liber_accounts');
                            const list = raw ? JSON.parse(raw) : [];
                            const dedup = list.filter(a => a.uid !== acct.uid);
                            dedup.unshift(acct);
                            localStorage.setItem('liber_accounts', JSON.stringify(dedup.slice(0, 10)));
                        })();
                    } catch (_) { /* ignore */ }

                    // Update user data in Firestore if needed
                    this.updateUserLastLogin(user.uid);

                    // Register messaging for push/web notifications (best-effort)
                    this.registerMessaging(user).catch(()=>{});
                }
            });

        } catch (error) {
            console.error('❌ Firebase initialization error:', error);
            console.error('Error details:', error.message);
            console.error('Error stack:', error.stack);
            this.isInitialized = false;
        }
    }

    /**
     * Wait for Firebase SDK to be available (modular SDK loads asynchronously)
     */
    async waitForFirebaseSDK() {
        let attempts = 0;
        const maxAttempts = 100; // 10 seconds
        
        while (typeof firebase === 'undefined' && attempts < maxAttempts) {
            await new Promise(resolve => setTimeout(resolve, 100));
            attempts++;
        }
        
        if (typeof firebase === 'undefined') {
            throw new Error('Firebase SDK failed to load within 10 seconds. Please check your internet connection.');
        }
    }

    /**
     * Wait for secure keys to be loaded
     */
    async waitForSecureKeys() {
        let attempts = 0;
        const maxAttempts = 100; // 10 seconds
        
        while (!window.secureKeyManager && attempts < maxAttempts) {
            await new Promise(resolve => setTimeout(resolve, 100));
            attempts++;
        }
        
        if (!window.secureKeyManager) {
            throw new Error('Secure key manager not available');
        }
        
        // Wait for keys to be fetched
        await window.secureKeyManager.fetchKeys();
    }

    /**
     * Get Firebase configuration from secure keys
     */
    async getFirebaseConfig() {
        try {
            if (window.__DEBUG_KEYS__) console.log('Fetching keys from secure key manager...');
            const keys = await window.secureKeyManager.getKeys();
            if (window.__DEBUG_KEYS__) {
                console.log('Keys fetched successfully');
                console.log('Keys structure:', Object.keys(keys));
            }
            
            if (!keys.firebase) {
                console.error('❌ Firebase configuration missing from keys');
                console.error('Available keys:', Object.keys(keys));
                throw new Error('❌ Firebase configuration is required but not found in secure keys. Please add Firebase configuration to your Gist.');
            }
            
            if (window.__DEBUG_KEYS__) {
                console.log('✅ Firebase config found in keys');
                console.log('Firebase config fields:', Object.keys(keys.firebase));
            }
            
            // Debug: Check if API key looks valid
            if (keys.firebase.apiKey) {
                if (window.__DEBUG_KEYS__) console.log('✅ Firebase API key present');
                if (window.__DEBUG_KEYS__ && keys.firebase.apiKey.length < 30) {
                    console.warn('⚠️ Firebase API key seems too short');
                }
            } else {
                console.error('❌ Firebase API key is missing');
            }
            
            return keys.firebase;
            
        } catch (error) {
            console.error('Error getting Firebase configuration:', error);
            console.error('Error details:', error.message);
            throw new Error('❌ Failed to load Firebase configuration from secure keys. Please check your Gist configuration.');
        }
    }

    /**
     * Wait for Firebase to be initialized
     */
    async waitForInit() {
        let attempts = 0;
        const maxAttempts = 150; // up to 15 seconds to accommodate CDN + Gist fetch
        
        while (!this.isInitialized && attempts < maxAttempts) {
            await new Promise(resolve => setTimeout(resolve, 100));
            attempts++;
        }
        
        if (!this.isInitialized) {
            throw new Error('❌ Firebase is required but not initialized. Please check your configuration and internet connection.');
        }
        
        return true;
    }
    
    /**
     * Check if Firebase is available and initialized
     */
    isFirebaseAvailable() {
        return this.isInitialized && typeof firebase !== 'undefined';
    }

    /**
     * Create user with email and password
     */
    async createUser(email, password, userData) {
        await this.waitForInit();
        
        try {
            // Create user in Firebase Auth using modular SDK
            const userCredential = await firebase.createUserWithEmailAndPassword(this.auth, email, password);
            const user = userCredential.user;
            
            // Store additional user data in Firestore
            const userDocRef = firebase.doc(this.db, 'users', user.uid);
            await firebase.setDoc(userDocRef, {
                ...userData,
                uid: user.uid,
                email: user.email,
                role: userData.role || 'user',
                isVerified: false,
                status: 'pending',
                usernameLower: (userData.username || '').toLowerCase(),
                emailLower: (user.email || '').toLowerCase(),
                createdAt: new Date().toISOString(),
                updatedAt: new Date().toISOString(),
                lastLogin: null,
                loginCount: 0
            });
            
            // Send email verification
            await firebase.sendEmailVerification(user);
            
            console.log('User created successfully:', user.uid);
            return user;
            
        } catch (error) {
            console.error('Error creating user:', error);
            throw error;
        }
    }

    /**
     * Sign in user with email and password
     */
    async signInUser(email, password) {
        await this.waitForInit();
        
        try {
            const userCredential = await firebase.signInWithEmailAndPassword(this.auth, email, password);
            const user = userCredential.user;
            
            // Update last login and login count
            await this.updateUserLastLogin(user.uid);
            
            if (window.__DEBUG_AUTH__) console.log('User signed in successfully:', user.uid);
            return user;
            
        } catch (error) {
            console.error('Error signing in user:', error);
            throw error;
        }
    }

    /**
     * Find a user by email (case-insensitive)
     */
    async findUserByEmail(email){
        await this.waitForInit();
        try{
            const emailLower = (email||'').toLowerCase();
            const q = firebase.query(
                firebase.collection(this.db,'users'),
                firebase.where('emailLower','==', emailLower),
                firebase.limit(1)
            );
            const snap = await firebase.getDocs(q);
            let data = null;
            snap.forEach(doc=>{ data = { id: doc.id, ...doc.data() }; });
            return data;
        }catch(e){ return null; }
    }

    /**
     * Increment failed login attempts; returns new count
     */
    async incrementFailedLogin(uid){
        await this.waitForInit();
        try{
            const ref = firebase.doc(this.db,'users', uid);
            const doc = await firebase.getDoc(ref);
            const curr = doc.exists()? (doc.data().failedLoginAttempts||0) : 0;
            const next = curr + 1;
            const updates = { failedLoginAttempts: next, lastFailedLoginAt: new Date().toISOString() };
            if (next >= 5){
                const lockMs = 15*60*1000;
                updates.lockoutUntil = new Date(Date.now()+lockMs).toISOString();
            }
            await firebase.updateDoc(ref, updates);
            return next;
        }catch(e){ return null; }
    }

    /**
     * Reset failed login attempts to 0
     */
    async resetFailedLogin(uid){
        await this.waitForInit();
        try{
            const ref = firebase.doc(this.db,'users', uid);
            await firebase.updateDoc(ref, { failedLoginAttempts: 0, lockoutUntil: null, updatedAt: new Date().toISOString() });
            return true;
        }catch(e){ return false; }
    }

    /**
     * Ensure a users/{uid} document exists; create with fallback data if missing
     */
    async ensureUserDoc(uid, seed = {}){
        await this.waitForInit();
        try{
            const ref = firebase.doc(this.db,'users', uid);
            const snap = await firebase.getDoc(ref);
            if (!snap.exists()){
                const base = {
                    uid,
                    username: seed.username || '',
                    email: seed.email || '',
                    role: seed.role || 'user',
                    isVerified: !!seed.isVerified,
                    status: seed.status || 'approved',
                    usernameLower: (seed.username||'').toLowerCase(),
                    emailLower: (seed.email||'').toLowerCase(),
                    createdAt: new Date().toISOString(),
                    updatedAt: new Date().toISOString(),
                    loginCount: 0
                };
                await firebase.setDoc(ref, base);
            }
            return true;
        }catch(e){ return false; }
    }

    /**
     * Sign out user
     */
    async signOutUser() {
        await this.waitForInit();
        
        try {
            await firebase.signOut(this.auth);
            if (window.__DEBUG_AUTH__) console.log('User signed out successfully');
        } catch (error) {
            console.error('Error signing out user:', error);
            throw error;
        }
    }

    /**
     * Get current user
     */
    async getCurrentUser() {
        await this.waitForInit();
        return this.auth.currentUser;
    }

    /**
     * Get user data from Firestore
     */
    async getUserData(uid) {
        await this.waitForInit();
        
        try {
            const userDocRef = firebase.doc(this.db, 'users', uid);
            const docSnap = await firebase.getDoc(userDocRef);
            if (docSnap.exists()) {
                return docSnap.data();
            } else {
                return null;
            }
        } catch (error) {
            console.error('Error getting user data:', error);
            throw error;
        }
    }

    /**
     * Follow / Unfollow and feed helpers
     */
    async followUser(followerUid, targetUid){
        await this.waitForInit();
        const ref = firebase.doc(this.db, 'subscriptions', followerUid, 'following', targetUid);
        await firebase.setDoc(ref, { uid: targetUid, followedAt: new Date().toISOString() });
    }

    async unfollowUser(followerUid, targetUid){
        await this.waitForInit();
        const ref = firebase.doc(this.db, 'subscriptions', followerUid, 'following', targetUid);
        await firebase.deleteDoc(ref);
    }

    async getFollowingIds(followerUid){
        await this.waitForInit();
        const col = firebase.collection(this.db, 'subscriptions', followerUid, 'following');
        const snap = await firebase.getDocs(col);
        const ids = [];
        snap.forEach(d=> ids.push(d.id));
        return ids;
    }

    async getFeedPosts(meUid, followingIds = [], limitPerAuthor = 10){
        await this.waitForInit();
        const posts = [];
        // Always include my posts first
        try{
            const qMine = firebase.query(
                firebase.collection(this.db,'posts'),
                firebase.where('authorId','==', meUid),
                firebase.orderBy('createdAtTS','desc'),
                firebase.limit(limitPerAuthor)
            );
            const sMine = await firebase.getDocs(qMine);
            sMine.forEach(d=> posts.push(d.data()));
        }catch{
            const q2 = firebase.query(firebase.collection(this.db,'posts'), firebase.where('authorId','==', meUid));
            const s2 = await firebase.getDocs(q2); s2.forEach(d=> posts.push(d.data()));
        }
        // Fetch followed authors, up to a reasonable number per batch due to Firestore limits
        for (const uid of (followingIds||[]).slice(0,20)){
            try{
                const q = firebase.query(
                    firebase.collection(this.db,'posts'),
                    firebase.where('authorId','==', uid),
                    firebase.where('visibility','==','public'),
                    firebase.orderBy('createdAtTS','desc'),
                    firebase.limit(5)
                );
                const s = await firebase.getDocs(q); s.forEach(d=> posts.push(d.data()));
            }catch{
                const q3 = firebase.query(firebase.collection(this.db,'posts'), firebase.where('authorId','==', uid));
                const s3 = await firebase.getDocs(q3); s3.forEach(d=> posts.push(d.data()));
            }
        }
        posts.sort((a,b)=> (b.createdAtTS?.toMillis?.()||0) - (a.createdAtTS?.toMillis?.()||0) || new Date(b.createdAt||0) - new Date(a.createdAt||0));
        return posts.slice(0,50);
    }

    /**
     * Post interactions: likes and comments
     */
    async likePost(postId, userUid){
        await this.waitForInit();
        const ref = firebase.doc(this.db, 'posts', postId, 'likes', userUid);
        await firebase.setDoc(ref, { uid: userUid, likedAt: new Date().toISOString() });
    }

    async unlikePost(postId, userUid){
        await this.waitForInit();
        const ref = firebase.doc(this.db, 'posts', postId, 'likes', userUid);
        await firebase.deleteDoc(ref);
    }

    async hasLiked(postId, userUid){
        await this.waitForInit();
        const ref = firebase.doc(this.db, 'posts', postId, 'likes', userUid);
        const snap = await firebase.getDoc(ref);
        return snap.exists();
    }

    async getPostStats(postId){
        await this.waitForInit();
        const likesCol = firebase.collection(this.db, 'posts', postId, 'likes');
        const likeSnap = await firebase.getDocs(likesCol);
        const repostsCol = firebase.collection(this.db, 'posts', postId, 'reposts');
        let repostSnap = { size: 0 };
        try { repostSnap = await firebase.getDocs(repostsCol); } catch(_){ }
        return { likes: likeSnap.size, reposts: repostSnap.size };
    }

    async addComment(postId, userUid, text, parentId = null){
        await this.waitForInit();
        const ref = firebase.doc(firebase.collection(this.db, 'posts', postId, 'comments'));
        await firebase.setDoc(ref, { id: ref.id, authorId: userUid, text, parentId, createdAt: new Date().toISOString(), createdAtTS: firebase.serverTimestamp() });
        return ref.id;
    }

    async repost(postId, userUid){
        await this.waitForInit();
        const ref = firebase.doc(this.db, 'posts', postId, 'reposts', userUid);
        await firebase.setDoc(ref, { uid: userUid, repostedAt: new Date().toISOString() });
    }

    async unRepost(postId, userUid){
        await this.waitForInit();
        const ref = firebase.doc(this.db, 'posts', postId, 'reposts', userUid);
        await firebase.deleteDoc(ref);
    }

    async hasReposted(postId, userUid){
        await this.waitForInit();
        const ref = firebase.doc(this.db, 'posts', postId, 'reposts', userUid);
        const snap = await firebase.getDoc(ref);
        return snap.exists();
    }

    async getComments(postId, limitN = 10){
        await this.waitForInit();
        let q;
        try{
            q = firebase.query(
                firebase.collection(this.db, 'posts', postId, 'comments'),
                firebase.orderBy('createdAtTS', 'desc'),
                firebase.limit(limitN)
            );
        }catch(_){
            q = firebase.query(
                firebase.collection(this.db, 'posts', postId, 'comments'),
                firebase.orderBy('createdAt', 'desc'),
                firebase.limit(limitN)
            );
        }
        const snap = await firebase.getDocs(q);
        const out = []; snap.forEach(d=> out.push(d.data()));
        return out;
    }

    async updatePost(postId, updates){
        await this.waitForInit();
        const ref = firebase.doc(this.db, 'posts', postId);
        const payload = { ...updates, updatedAt: new Date().toISOString() };
        await firebase.updateDoc(ref, payload);
    }

    async deletePost(postId){
        await this.waitForInit();
        const ref = firebase.doc(this.db, 'posts', postId);
        await firebase.deleteDoc(ref);
    }

    async updateComment(postId, commentId, newText){
        await this.waitForInit();
        const ref = firebase.doc(this.db, 'posts', postId, 'comments', commentId);
        await firebase.updateDoc(ref, { text: newText, updatedAt: new Date().toISOString() });
    }

    async deleteComment(postId, commentId){
        await this.waitForInit();
        const ref = firebase.doc(this.db, 'posts', postId, 'comments', commentId);
        await firebase.deleteDoc(ref);
    }

    async getTrendingPosts(excludeUid, limitN = 10){
        await this.waitForInit();
        // Fallback simple: fetch recent posts and sort by like count client-side
        const q = firebase.query(
            firebase.collection(this.db,'posts'),
            firebase.where('visibility','==','public'),
            firebase.orderBy('createdAtTS','desc'),
            firebase.limit(50)
        );
        const snap = await firebase.getDocs(q);
        const items = [];
        for (const d of snap.docs){
            const p = d.data();
            if (p.authorId === excludeUid) continue;
            const stats = await this.getPostStats(p.id);
            items.push({ ...p, _likes: stats.likes });
        }
        items.sort((a,b)=> (b._likes||0) - (a._likes||0) || (b.createdAtTS?.toMillis?.()||0) - (a.createdAtTS?.toMillis?.()||0) || new Date(b.createdAt||0) - new Date(a.createdAt||0));
        return items.slice(0, limitN);
    }

    /**
     * Update user data in Firestore
     */
    async updateUserData(uid, data) {
        await this.waitForInit();
        
        try {
            const userDocRef = firebase.doc(this.db, 'users', uid);
            const update = { ...data, updatedAt: new Date().toISOString() };
            if (Object.prototype.hasOwnProperty.call(data, 'username')) {
                update.usernameLower = (data.username || '').toLowerCase();
            }
            if (Object.prototype.hasOwnProperty.call(data, 'email')) {
                update.emailLower = (data.email || '').toLowerCase();
            }
            await firebase.updateDoc(userDocRef, update);
            console.log('User data updated successfully');
        } catch (error) {
            console.error('Error updating user data:', error);
            throw error;
        }
    }

    /**
     * Get all users (admin only)
     */
    async getAllUsers() {
        await this.waitForInit();
        
        try {
            const usersCollectionRef = firebase.collection(this.db, 'users');
            const snapshot = await firebase.getDocs(usersCollectionRef);
            const users = [];
            snapshot.forEach(doc => {
                users.push({
                    id: doc.id,
                    ...doc.data()
                });
            });
            return users;
        } catch (error) {
            console.error('Error getting all users:', error);
            throw error;
        }
    }

    /**
     * Delete user
     */
    async deleteUser(uid) {
        await this.waitForInit();
        
        try {
            // Delete from Firestore
            const userDocRef = firebase.doc(this.db, 'users', uid);
            await firebase.deleteDoc(userDocRef);
            
            // Delete from Auth (requires re-authentication)
            const user = this.auth.currentUser;
            if (user && user.uid === uid) {
                await user.delete();
            }
            
            console.log('User deleted successfully');
        } catch (error) {
            console.error('Error deleting user:', error);
            throw error;
        }
    }

    /**
     * Send email verification
     */
    async sendEmailVerification() {
        await this.waitForInit();
        
        try {
            const user = this.auth.currentUser;
            if (user && !user.emailVerified) {
                await firebase.sendEmailVerification(user);
                console.log('Email verification sent');
            }
        } catch (error) {
            console.error('Error sending email verification:', error);
            throw error;
        }
    }

    /**
     * Send password reset email
     */
    async sendPasswordResetEmail(email) {
        await this.waitForInit();
        
        try {
            await firebase.sendPasswordResetEmail(this.auth, email);
            console.log('Password reset email sent');
        } catch (error) {
            console.error('Error sending password reset email:', error);
            throw error;
        }
    }

    /**
     * Verify password reset code
     */
    async verifyPasswordResetCode(code) {
        await this.waitForInit();
        
        try {
            const email = await firebase.verifyPasswordResetCode(this.auth, code);
            return email;
        } catch (error) {
            console.error('Error verifying password reset code:', error);
            throw error;
        }
    }

    /**
     * Confirm password reset
     */
    async confirmPasswordReset(code, newPassword) {
        await this.waitForInit();
        
        try {
            await firebase.confirmPasswordReset(this.auth, code, newPassword);
            console.log('Password reset confirmed');
        } catch (error) {
            console.error('Error confirming password reset:', error);
            throw error;
        }
    }

    /**
     * Update user's last login timestamp and increment login count
     */
    async updateUserLastLogin(uid) {
        try {
            const userDocRef = firebase.doc(this.db, 'users', uid);
            await firebase.updateDoc(userDocRef, {
                lastLogin: new Date().toISOString(),
                loginCount: firebase.increment(1),
                updatedAt: new Date().toISOString()
            });
            console.log('User last login updated:', uid);
        } catch (error) {
            console.error('Error updating user last login:', error);
        }
    }

    /**
     * Approve user (admin function)
     */
    async approveUser(uid) {
        await this.waitForInit();
        
        try {
            const userDocRef = firebase.doc(this.db, 'users', uid);
            await firebase.updateDoc(userDocRef, {
                status: 'approved',
                isVerified: true,
                updatedAt: new Date().toISOString()
            });
            console.log('User approved:', uid);
        } catch (error) {
            console.error('Error approving user:', error);
            throw error;
        }
    }

    /**
     * Reject user (admin function)
     */
    async rejectUser(uid) {
        await this.waitForInit();
        
        try {
            const userDocRef = firebase.doc(this.db, 'users', uid);
            await firebase.updateDoc(userDocRef, {
                status: 'rejected',
                updatedAt: new Date().toISOString()
            });
            console.log('User rejected:', uid);
        } catch (error) {
            console.error('Error rejecting user:', error);
            throw error;
        }
    }

    /**
     * Update user profile
     */
    async updateUserProfile(uid, profileData) {
        await this.waitForInit();
        
        try {
            const userDocRef = firebase.doc(this.db, 'users', uid);
            await firebase.updateDoc(userDocRef, {
                ...profileData,
                updatedAt: new Date().toISOString()
            });
            console.log('User profile updated:', uid);
        } catch (error) {
            console.error('Error updating user profile:', error);
            throw error;
        }
    }

    /**
     * Get user statistics
     */
    async getUserStats() {
        await this.waitForInit();
        
        try {
            const usersCollectionRef = firebase.collection(this.db, 'users');
            const snapshot = await firebase.getDocs(usersCollectionRef);
            
            const stats = {
                total: 0,
                pending: 0,
                approved: 0,
                rejected: 0,
                verified: 0,
                unverified: 0
            };
            
            snapshot.forEach(doc => {
                const userData = doc.data();
                stats.total++;
                
                if (userData.status === 'pending') stats.pending++;
                else if (userData.status === 'approved') stats.approved++;
                else if (userData.status === 'rejected') stats.rejected++;
                
                if (userData.isVerified) stats.verified++;
                else stats.unverified++;
            });
            
            return stats;
        } catch (error) {
            console.error('Error getting user stats:', error);
            throw error;
        }
    }

    /**
     * Safely call a Firebase Callable Function (no-op if Functions unavailable)
     */
    async callFunction(name, payload = {}) {
        try {
            await this.waitForInit();
            if (!this.functions) return null;
            // Prefer modular API if available
            if (window.firebaseModular && window.firebaseModular.httpsCallable) {
                const callable = window.firebaseModular.httpsCallable(this.functions, name);
                const res = await callable(payload);
                return (res && res.data) || null;
            }
            // Compat fallback if provided on global firebase
            if (firebase.httpsCallable) {
                const callable = firebase.httpsCallable(this.functions, name);
                const res = await callable(payload);
                return (res && res.data) || null;
            }
            return null;
        } catch (e) {
            console.warn('Callable function failed:', name, e?.message || e);
            return null;
        }
    }

    /**
     * Search users by username or email
     */
    async searchUsers(searchTerm) {
        await this.waitForInit();
        
        try {
            const usersCollectionRef = firebase.collection(this.db, 'users');

            const term = (searchTerm || '').toLowerCase();
            // Prefix queries for case-insensitive matching
            let results = [];
            try {
                // Try exact equality first for usernameLower/emailLower
                const exactUser = firebase.query(
                    usersCollectionRef,
                    firebase.where('usernameLower', '==', term)
                );
                const exactEmail = firebase.query(
                    usersCollectionRef,
                    firebase.where('emailLower', '==', term)
                );
                const [exU, exE] = await Promise.all([
                    firebase.getDocs(exactUser),
                    firebase.getDocs(exactEmail)
                ]);
                const exactSet = new Map();
                exU.forEach(doc => exactSet.set(doc.id, { id: doc.id, ...doc.data() }));
                exE.forEach(doc => exactSet.set(doc.id, { id: doc.id, ...doc.data() }));
                if (exactSet.size > 0) {
                    return Array.from(exactSet.values());
                }
                const qUsernameLower = firebase.query(
                    usersCollectionRef,
                    firebase.where('usernameLower', '>=', term),
                    firebase.where('usernameLower', '<=', term + '\\uf8ff')
                );
                const qEmailLower = firebase.query(
                    usersCollectionRef,
                    firebase.where('emailLower', '>=', term),
                    firebase.where('emailLower', '<=', term + '\\uf8ff')
                );
                const [snapU, snapE] = await Promise.all([
                    firebase.getDocs(qUsernameLower),
                    firebase.getDocs(qEmailLower)
                ]);
                const set = new Map();
                snapU.forEach(doc => set.set(doc.id, { id: doc.id, ...doc.data() }));
                snapE.forEach(doc => set.set(doc.id, { id: doc.id, ...doc.data() }));
                results = Array.from(set.values());
                // Client-side fallback if still empty or fields missing: fuzzy contains/subsequence
                if (results.length === 0 && term) {
                    const snapAll = await firebase.getDocs(usersCollectionRef);
                    const matches = [];
                    const contains = (s)=> (s||'').toLowerCase().includes(term);
                    const isSubseq = (s)=>{ const t=term; let i=0; for (const ch of (s||'').toLowerCase()){ if (ch===t[i]) i++; if (i===t.length) return true; } return t.length===0; };
                    snapAll.forEach(doc => {
                        const d = doc.data() || {};
                        const u = (d.usernameLower || (d.username||'').toLowerCase());
                        const em = (d.emailLower || (d.email||'').toLowerCase());
                        if (contains(u) || contains(em) || isSubseq(u) || isSubseq(em)) matches.push({ id: doc.id, ...d });
                    });
                    results = matches;
                }
            } catch (e) {
                // Client-side fallback if composite queries are restricted
                const snapAll = await firebase.getDocs(usersCollectionRef);
                const matches = [];
                snapAll.forEach(doc => {
                    const d = doc.data() || {};
                    const u = (d.usernameLower || (d.username||'').toLowerCase());
                    const em = (d.emailLower || (d.email||'').toLowerCase());
                    // Allow contains or ordered subsequence fuzzy match
                    const contains = (s)=> s && s.includes(term);
                    const isSubseq = (s)=>{
                        let i=0; for (const ch of s){ if (ch===term[i]) i++; if (i===term.length) return true; } return term.length===0;
                    };
                    if (contains(u) || contains(em) || isSubseq(u) || isSubseq(em)) matches.push({ id: doc.id, ...d });
                });
                results = matches;
            }
            return results;
        } catch (error) {
            console.error('Error searching users:', error);
            throw error;
        }
    }

    /**
     * Get users with pagination
     */
    async getUsersWithPagination(limit = 20, startAfter = null) {
        await this.waitForInit();
        
        try {
            const usersCollectionRef = firebase.collection(this.db, 'users');
            let query = firebase.query(
                usersCollectionRef,
                firebase.orderBy('createdAt', 'desc'),
                firebase.limit(limit)
            );
            
            if (startAfter) {
                query = firebase.query(
                    usersCollectionRef,
                    firebase.orderBy('createdAt', 'desc'),
                    firebase.startAfter(startAfter),
                    firebase.limit(limit)
                );
            }
            
            const snapshot = await firebase.getDocs(query);
            const users = [];
            
            snapshot.forEach(doc => {
                users.push({
                    id: doc.id,
                    ...doc.data()
                });
            });
            
            return {
                users,
                lastDoc: snapshot.docs[snapshot.docs.length - 1],
                hasMore: snapshot.docs.length === limit
            };
        } catch (error) {
            console.error('Error getting users with pagination:', error);
            throw error;
        }
    }

    /**
     * Migrate existing users from localStorage to Firebase
     */
    async migrateUsers() {
        try {
            console.log('Starting user migration to Firebase...');
            
            // Get existing users from localStorage
            const existingUsers = await window.authManager.getUsers();
            console.log('Found existing users:', existingUsers.length);
            
            let migratedCount = 0;
            
            for (const user of existingUsers) {
                try {
                    // Check if user already exists in Firebase
                    const existingUser = await this.auth.fetchSignInMethodsForEmail(user.email);
                    
                    if (existingUser.length === 0) {
                        // Create user in Firebase
                        const userCredential = await firebase.createUserWithEmailAndPassword(this.auth,
                            user.email,
                            'tempPassword123!'
                        );
                        
                        // Store user data in Firestore
                        const newUserDocRef = firebase.doc(this.db, 'users', userCredential.user.uid);
                        await firebase.setDoc(newUserDocRef, {
                            uid: userCredential.user.uid,
                            username: user.username,
                            email: user.email,
                            role: user.role,
                            isVerified: user.isVerified || false,
                            status: user.status || 'pending',
                            createdAt: user.createdAt ? new Date(user.createdAt) : new Date().toISOString(),
                            updatedAt: new Date().toISOString(),
                            migratedFromLocalStorage: true
                        });
                        
                        migratedCount++;
                        console.log(`Migrated user: ${user.email}`);
                    } else {
                        console.log(`User already exists in Firebase: ${user.email}`);
                    }
                    
                } catch (error) {
                    console.error(`Error migrating user ${user.email}:`, error);
                }
            }
            
            console.log(`Migration completed. ${migratedCount} users migrated.`);
            return migratedCount;
            
        } catch (error) {
            console.error('Migration error:', error);
            throw error;
        }
    }

    // Duplicate deleteUser removed; use the unified method above

    /**
     * Connectivity check without exposing key material
     */
    async testFirebaseAPIKey() {
        try {
            console.log('🔍 Testing Firebase connectivity...');
            
            const firebaseConfig = await this.getFirebaseConfig();
            if (!firebaseConfig.apiKey) {
                console.error('❌ Firebase API key missing');
                return false;
            }
            
            // Test the API key by making a simple request to Firebase Auth
            const testUrl = `https://www.googleapis.com/identitytoolkit/v3/relyingparty/verifyAssertion?key=${firebaseConfig.apiKey}`;
            
            const response = await fetch(testUrl, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    identifier: 'test@example.com',
                    continueUri: 'https://example.com'
                })
            });
            
            if (response.ok) {
                console.log('✅ Firebase endpoint reachable');
                return true;
            } else {
                console.warn('⚠️ Firebase connectivity test failed. Status:', response.status);
                return false;
            }
            
        } catch (error) {
            console.error('❌ Error testing Firebase API key:', error);
            return false;
        }
    }
}

// Create global instance
window.firebaseService = new FirebaseService();

// Add global test function
window.testFirebase = function() {
    console.log('=== Testing Firebase ===');
    console.log('Firebase SDK available:', typeof firebase !== 'undefined');
    if (typeof firebase !== 'undefined') {
        console.log('Firebase version:', firebase.SDK_VERSION);
        console.log('Firebase services:', Object.keys(firebase));
    }
    console.log('Firebase service available:', !!window.firebaseService);
    console.log('Firebase service initialized:', window.firebaseService?.isInitialized);
    
    // Test secure key manager
    if (window.secureKeyManager) {
        console.log('Secure key manager available:', !!window.secureKeyManager);
        window.secureKeyManager.getKeys().then(keys => {
            console.log('Keys structure:', Object.keys(keys));
            console.log('Has Firebase config:', !!keys.firebase);
            if (keys.firebase) {
                console.log('Firebase config fields:', Object.keys(keys.firebase));
                console.log('Firebase project ID:', keys.firebase.projectId);
            }
        }).catch(error => {
            console.error('Error fetching keys:', error);
        });
    } else {
        console.error('Secure key manager not available');
    }
};

// Add comprehensive test function
window.testCompleteSetup = async function() {
    console.log('=== Testing Complete Firebase Setup ===');
    
    try {
        // Test 1: Firebase SDK
        console.log('1. Testing Firebase SDK...');
        if (typeof firebase === 'undefined') {
            console.error('❌ Firebase SDK not loaded');
            return false;
        }
        console.log('✅ Firebase SDK loaded');
        console.log('Firebase version:', firebase.SDK_VERSION);
        
        // Test 2: Secure Key Manager
        console.log('2. Testing Secure Key Manager...');
        if (!window.secureKeyManager) {
            console.error('❌ Secure Key Manager not available');
            return false;
        }
        console.log('✅ Secure Key Manager available');
        
        // Test 3: Firebase Configuration
        console.log('3. Testing Firebase Configuration...');
        const keys = await window.secureKeyManager.getKeys();
        if (!keys.firebase) {
            console.error('❌ Firebase config not found in Gist');
            return false;
        }
        console.log('✅ Firebase config found');
        console.log('Project ID:', keys.firebase.projectId);
        console.log('Config fields:', Object.keys(keys.firebase));
        
        // Test 4: Firebase Service
        console.log('4. Testing Firebase Service...');
        if (!window.firebaseService) {
            console.error('❌ Firebase Service not available');
            return false;
        }
        console.log('✅ Firebase Service available');
        
        // Test 5: Firebase Initialization
        console.log('5. Testing Firebase Initialization...');
        let attempts = 0;
        const maxAttempts = 50;
        while (!window.firebaseService.isInitialized && attempts < maxAttempts) {
            await new Promise(resolve => setTimeout(resolve, 100));
            attempts++;
        }
        
        if (window.firebaseService.isInitialized) {
            console.log('✅ Firebase Service initialized');
            console.log('Firebase App:', window.firebaseService.app?.name);
            console.log('Firebase Auth:', !!window.firebaseService.auth);
            console.log('Firebase Firestore:', !!window.firebaseService.db);
        } else {
            console.error('❌ Firebase Service failed to initialize');
            return false;
        }
        
        // Test 6: Firebase Database Operations
        console.log('6. Testing Firebase Database Operations...');
        try {
            // Test user stats
            const stats = await window.firebaseService.getUserStats();
            console.log('✅ User stats retrieved:', stats);
            
            // Test user collection access
            const users = await window.firebaseService.getAllUsers();
            console.log('✅ Users collection accessed:', users.length, 'users');
            
            console.log('✅ All database operations working');
        } catch (dbError) {
            console.error('❌ Database operations failed:', dbError);
            return false;
        }
        
        // Test 7: Firebase Auth Operations
        console.log('7. Testing Firebase Auth Operations...');
        try {
            const currentUser = window.firebaseService.auth.currentUser;
            console.log('✅ Auth service accessible');
            console.log('Current user:', currentUser ? currentUser.email : 'None');
        } catch (authError) {
            console.error('❌ Auth operations failed:', authError);
            return false;
        }
        
        console.log('🎉 All Firebase tests passed! System is ready to use.');
        return true;
        
    } catch (error) {
        console.error('❌ Test failed:', error);
        return false;
    }
};

// Add simple test function that should work
window.testFirebaseSimple = async function() {
    console.log('=== Simple Firebase Test ===');
    
    try {
        // Wait for Firebase to be ready
        if (!window.firebaseService || !window.firebaseService.isInitialized) {
            console.log('Waiting for Firebase to initialize...');
            let attempts = 0;
            while ((!window.firebaseService || !window.firebaseService.isInitialized) && attempts < 50) {
                await new Promise(resolve => setTimeout(resolve, 100));
                attempts++;
            }
        }
        
        if (!window.firebaseService || !window.firebaseService.isInitialized) {
            console.error('❌ Firebase not ready');
            return false;
        }
        
        console.log('✅ Firebase is ready!');
        console.log('App:', window.firebaseService.app?.name);
        console.log('Auth:', !!window.firebaseService.auth);
        console.log('Firestore:', !!window.firebaseService.db);
        
        return true;
    } catch (error) {
        console.error('❌ Test failed:', error);
        return false;
    }
};

// Add global migration function
window.startMigration = async function() {
    console.log('=== Starting User Migration ===');
    try {
        if (window.migrationHelper) {
            await window.migrationHelper.startMigration();
        } else {
            console.log('Migration helper not available - Firebase not configured');
        }
    } catch (error) {
        console.error('Migration failed:', error);
    }
};

// Add global cleanup function
window.cleanupLocalStorage = async function() {
    console.log('=== Cleaning Up Local Storage ===');
    try {
        if (window.migrationHelper) {
            await window.migrationHelper.cleanupLocalStorage();
        } else {
            console.log('Migration helper not available');
        }
    } catch (error) {
        console.error('Cleanup failed:', error);
    }
};

// Add global password reset function
window.sendPasswordResetEmails = async function() {
    console.log('=== Sending Password Reset Emails ===');
    try {
        if (window.migrationHelper) {
            await window.migrationHelper.sendPasswordResetEmails();
        } else {
            console.log('Migration helper not available');
        }
    } catch (error) {
        console.error('Password reset emails failed:', error);
    }
};
