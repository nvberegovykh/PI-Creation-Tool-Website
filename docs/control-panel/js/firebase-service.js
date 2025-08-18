/**
 * Firebase Service for Liber Apps Control Panel
 * Handles user authentication and data storage with Firebase
 * Updated for Firebase Modular SDK v10.9.0
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
     * Initialize Firebase
     */
    async init() {
        try {
            console.log('=== Firebase Service Initialization ===');
            
            // Check if Firebase SDK is available
            if (typeof firebase === 'undefined') {
                console.error('❌ Firebase SDK not loaded!');
                console.error('Firebase SDK failed to load. This could be due to:');
                console.error('1. Network connectivity issues');
                console.error('2. Firewall blocking Firebase URLs');
                console.error('3. Browser security settings');
                console.error('4. CDN availability issues');
                console.error('5. Script loading order issues');
                this.isInitialized = false;
                return;
            }
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
            console.log('✅ Firebase config validation passed');
            console.log('Firebase config has all essential fields:', essentialFields);

            // Initialize Firebase with modular SDK
            console.log('Initializing Firebase app...');
            this.app = firebase.initializeApp(firebaseConfig);
            console.log('Firebase app created with name:', this.app.name);

            // Initialize services with modular SDK
            console.log('Initializing Firebase services...');
            this.auth = firebase.auth(this.app);
            this.db = firebase.firestore(this.app);
            
            // Enable offline persistence for Firestore
            try {
                await this.db.enablePersistence({
                    synchronizeTabs: true
                });
                console.log('✅ Firestore offline persistence enabled');
            } catch (error) {
                console.warn('⚠️ Firestore offline persistence failed:', error.message);
            }
            
            this.isInitialized = true;
            console.log('✅ Firebase initialized successfully');

            // Set up auth state listener
            firebase.onAuthStateChanged(this.auth, (user) => {
                console.log('Auth state changed:', user ? 'User logged in' : 'User logged out');
                if (user) {
                    console.log('Current user:', user.email);
                    console.log('User UID:', user.uid);
                    console.log('Email verified:', user.emailVerified);
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
            console.log('Fetching keys from secure key manager...');
            const keys = await window.secureKeyManager.getKeys();
            console.log('Keys fetched successfully');
            console.log('Keys structure:', Object.keys(keys));
            
            if (!keys.firebase) {
                console.error('❌ Firebase configuration missing from keys');
                console.error('Available keys:', Object.keys(keys));
                throw new Error('❌ Firebase configuration is required but not found in secure keys. Please add Firebase configuration to your Gist.');
            }
            
            console.log('✅ Firebase config found in keys');
            console.log('Firebase config fields:', Object.keys(keys.firebase));
            
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
        const maxAttempts = 50;
        
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
                createdAt: new Date().toISOString(),
                updatedAt: new Date().toISOString()
            });
            
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
            
            console.log('User signed in successfully:', user.uid);
            return user;
            
        } catch (error) {
            console.error('Error signing in user:', error);
            throw error;
        }
    }

    /**
     * Sign out user
     */
    async signOutUser() {
        await this.waitForInit();
        
        try {
            await this.auth.signOut();
            console.log('User signed out successfully');
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
     * Update user data in Firestore
     */
    async updateUserData(uid, data) {
        await this.waitForInit();
        
        try {
            await this.db.collection('users').doc(uid).update({
                ...data,
                updatedAt: firebase.firestore.FieldValue.serverTimestamp()
            });
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
            await this.db.collection('users').doc(uid).delete();
            
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
            const email = await this.auth.verifyPasswordResetCode(code);
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
            await this.auth.confirmPasswordReset(code, newPassword);
            console.log('Password reset confirmed');
        } catch (error) {
            console.error('Error confirming password reset:', error);
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
                        const userCredential = await this.auth.createUserWithEmailAndPassword(
                            user.email, 
                            'tempPassword123!' // Temporary password
                        );
                        
                        // Store user data in Firestore
                        await this.db.collection('users').doc(userCredential.user.uid).set({
                            uid: userCredential.user.uid,
                            username: user.username,
                            email: user.email,
                            role: user.role,
                            isVerified: user.isVerified || false,
                            status: user.status || 'pending',
                            createdAt: user.createdAt ? new Date(user.createdAt) : firebase.firestore.FieldValue.serverTimestamp(),
                            updatedAt: firebase.firestore.FieldValue.serverTimestamp(),
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
    console.log('=== Testing Complete Setup ===');
    
    try {
        // Test 1: Firebase SDK
        console.log('1. Testing Firebase SDK...');
        if (typeof firebase === 'undefined') {
            console.error('❌ Firebase SDK not loaded');
            return false;
        }
        console.log('✅ Firebase SDK loaded');
        
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
        
        console.log('🎉 All tests passed! Firebase is ready to use.');
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
