/**
 * Migration Helper for Liber Apps Control Panel
 * Helps migrate users from localStorage to Firebase
 */

class MigrationHelper {
    constructor() {
        this.migrationComplete = false;
    }

    /**
     * Start the migration process
     */
    async startMigration() {
        console.log('=== Starting Migration to Firebase ===');
        
        try {
            // Wait for Firebase to be ready
            await this.waitForFirebase();
            
            // Check if migration is needed
            const needsMigration = await this.checkMigrationNeeded();
            
            if (!needsMigration) {
                console.log('Migration not needed - Firebase already has users');
                return false;
            }
            
            // Perform migration
            const migratedCount = await this.performMigration();
            
            console.log(`Migration completed successfully! ${migratedCount} users migrated.`);
            this.migrationComplete = true;
            
            return true;
            
        } catch (error) {
            console.error('Migration failed:', error);
            return false;
        }
    }

    /**
     * Wait for Firebase to be initialized
     */
    async waitForFirebase() {
        console.log('Waiting for Firebase to initialize...');
        
        let attempts = 0;
        const maxAttempts = 100; // 10 seconds
        
        while (!window.firebaseService && attempts < maxAttempts) {
            await new Promise(resolve => setTimeout(resolve, 100));
            attempts++;
        }
        
        if (!window.firebaseService) {
            throw new Error('Firebase service not available');
        }
        
        // Wait for Firebase to be fully initialized
        await window.firebaseService.waitForInit();
        console.log('Firebase is ready');
    }

    /**
     * Check if migration is needed
     */
    async checkMigrationNeeded() {
        try {
            // Get users from Firebase
            const firebaseUsers = await window.firebaseService.getAllUsers();
            
            // Get users from localStorage
            const localUsers = await window.authManager.getUsers();
            
            console.log(`Firebase users: ${firebaseUsers.length}`);
            console.log(`Local users: ${localUsers.length}`);
            
            // If Firebase has users, migration might not be needed
            if (firebaseUsers.length > 0) {
                console.log('Firebase already has users - checking if migration is needed...');
                
                // Check if any local users are not in Firebase
                for (const localUser of localUsers) {
                    const existsInFirebase = firebaseUsers.some(fbUser => fbUser.email === localUser.email);
                    if (!existsInFirebase) {
                        console.log(`User ${localUser.email} exists locally but not in Firebase - migration needed`);
                        return true;
                    }
                }
                
                console.log('All local users exist in Firebase - migration not needed');
                return false;
            }
            
            // If Firebase is empty but local has users, migration is needed
            if (localUsers.length > 0) {
                console.log('Firebase is empty but local has users - migration needed');
                return true;
            }
            
            console.log('No users to migrate');
            return false;
            
        } catch (error) {
            console.error('Error checking migration status:', error);
            return false;
        }
    }

    /**
     * Perform the actual migration
     */
    async performMigration() {
        console.log('Starting user migration...');
        
        try {
            // Get existing users from localStorage
            const localUsers = await window.authManager.getUsers();
            console.log(`Found ${localUsers.length} users to migrate`);
            
            let migratedCount = 0;
            let failedCount = 0;
            
            for (const user of localUsers) {
                try {
                    console.log(`Migrating user: ${user.email}`);
                    
                    // Check if user already exists in Firebase
                    const existingMethods = await window.firebaseService.auth.fetchSignInMethodsForEmail(user.email);
                    
                    if (existingMethods.length > 0) {
                        console.log(`User ${user.email} already exists in Firebase - skipping`);
                        continue;
                    }
                    
                    // Create user in Firebase Auth
                    const userCredential = await window.firebaseService.auth.createUserWithEmailAndPassword(
                        user.email, 
                        'TempPassword123!' // Temporary password - user will need to reset
                    );
                    
                    // Store user data in Firestore
                    await window.firebaseService.db.collection('users').doc(userCredential.user.uid).set({
                        uid: userCredential.user.uid,
                        username: user.username,
                        email: user.email,
                        role: user.role || 'user',
                        isVerified: user.isVerified || false,
                        status: user.status || 'pending',
                        createdAt: user.createdAt ? new Date(user.createdAt) : firebase.firestore.FieldValue.serverTimestamp(),
                        updatedAt: firebase.firestore.FieldValue.serverTimestamp(),
                        migratedFromLocalStorage: true,
                        needsPasswordReset: true // Flag to indicate user needs to reset password
                    });
                    
                    migratedCount++;
                    console.log(`✅ Successfully migrated: ${user.email}`);
                    
                } catch (error) {
                    failedCount++;
                    console.error(`❌ Failed to migrate ${user.email}:`, error.message);
                }
            }
            
            console.log(`Migration summary: ${migratedCount} successful, ${failedCount} failed`);
            return migratedCount;
            
        } catch (error) {
            console.error('Migration error:', error);
            throw error;
        }
    }

    /**
     * Clean up localStorage after successful migration
     */
    async cleanupLocalStorage() {
        if (!this.migrationComplete) {
            console.log('Migration not completed - skipping cleanup');
            return;
        }
        
        try {
            console.log('Cleaning up localStorage...');
            
            // Remove user data from localStorage
            localStorage.removeItem('liber_users');
            
            console.log('localStorage cleanup completed');
            
        } catch (error) {
            console.error('Cleanup error:', error);
        }
    }

    /**
     * Send password reset emails to migrated users
     */
    async sendPasswordResetEmails() {
        try {
            console.log('Sending password reset emails to migrated users...');
            
            const users = await window.firebaseService.getAllUsers();
            const migratedUsers = users.filter(user => user.migratedFromLocalStorage && user.needsPasswordReset);
            
            console.log(`Found ${migratedUsers.length} migrated users who need password reset`);
            
            for (const user of migratedUsers) {
                try {
                    await window.firebaseService.sendPasswordResetEmail(user.email);
                    console.log(`Password reset email sent to: ${user.email}`);
                    
                    // Update user to mark password reset email as sent
                    await window.firebaseService.updateUserData(user.uid, {
                        passwordResetEmailSent: true,
                        passwordResetEmailSentAt: firebase.firestore.FieldValue.serverTimestamp()
                    });
                    
                } catch (error) {
                    console.error(`Failed to send password reset email to ${user.email}:`, error);
                }
            }
            
            console.log('Password reset emails sent');
            
        } catch (error) {
            console.error('Error sending password reset emails:', error);
        }
    }
}

// Create global instance
window.migrationHelper = new MigrationHelper();

// Add migration functions to global scope
window.startMigration = async function() {
    return await window.migrationHelper.startMigration();
};

window.cleanupLocalStorage = async function() {
    return await window.migrationHelper.cleanupLocalStorage();
};

window.sendPasswordResetEmails = async function() {
    return await window.migrationHelper.sendPasswordResetEmails();
};
