/**
 * Firebase Service for Liber Apps Control Panel
 * Handles user authentication and data storage with Firebase
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
            
            // Wait for secure keys to be loaded
            console.log('Waiting for secure keys...');
            await this.waitForSecureKeys();
            console.log('Secure keys loaded');
            
            // Get Firebase configuration from secure keys
            console.log('Getting Firebase config...');
            const firebaseConfig = await this.getFirebaseConfig();
            console.log('Firebase config loaded:', !!firebaseConfig);
            
            if (!firebaseConfig) {
                console.log('Firebase not configured - using local storage only');
                this.isInitialized = false;
                return;
            }

            // Initialize Firebase
            console.log('Initializing Firebase app...');
            if (!firebase.apps.length) {
                this.app = firebase.initializeApp(firebaseConfig);
                console.log('Firebase app created');
            } else {
                this.app = firebase.app();
                console.log('Firebase app already exists');
            }

            // Initialize services
            console.log('Initializing Firebase services...');
            this.auth = firebase.auth();
            this.db = firebase.firestore();
            
            this.isInitialized = true;
            console.log('✅ Firebase initialized successfully');

            // Set up auth state listener
            this.auth.onAuthStateChanged((user) => {
                console.log('Auth state changed:', user ? 'User logged in' : 'User logged out');
                if (user) {
                    console.log('Current user:', user.email);
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
            const keys = await window.secureKeyManager.getKeys();
            
            if (!keys.firebase) {
                console.log('Firebase configuration not found in secure keys - using local storage only');
                return null;
            }
            
            return keys.firebase;
            
        } catch (error) {
            console.error('Error getting Firebase configuration:', error);
            return null;
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
            throw new Error('Firebase initialization timeout');
        }
    }

    /**
     * Create user with email and password
     */
    async createUser(email, password, userData) {
        await this.waitForInit();
        
        try {
            // Create user in Firebase Auth
            const userCredential = await this.auth.createUserWithEmailAndPassword(email, password);
            const user = userCredential.user;
            
            // Store additional user data in Firestore
            await this.db.collection('users').doc(user.uid).set({
                ...userData,
                uid: user.uid,
                email: user.email,
                createdAt: firebase.firestore.FieldValue.serverTimestamp(),
                updatedAt: firebase.firestore.FieldValue.serverTimestamp()
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
            const userCredential = await this.auth.signInWithEmailAndPassword(email, password);
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
            const doc = await this.db.collection('users').doc(uid).get();
            if (doc.exists) {
                return doc.data();
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
            const snapshot = await this.db.collection('users').get();
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
                await user.sendEmailVerification();
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
            await this.auth.sendPasswordResetEmail(email);
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
