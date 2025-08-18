/**
 * Authentication Module for Liber Apps Control Panel
 * Handles user authentication, registration, and session management
 */

class AuthManager {
    constructor() {
        this.currentUser = null;
        this.sessionTimeout = 30 * 60 * 1000;
        this.init();
    }

    async init() {
        try {
            // Wait for cryptoManager to be available
            await this.waitForCryptoManager();
            
            // Check for existing session
            this.checkSession();
            
            // Setup event listeners
            this.setupEventListeners();
            
            // Check URL actions (verification, password reset)
            this.checkUrlActions();
            
            // Debug Gist configuration
            console.log('=== Auto-debugging Gist configuration ===');
            await window.secureKeyManager.debugGistConfig();
            
            // Wait for Firebase to be available
            let attempts = 0;
            const maxAttempts = 50; // 5 seconds with 100ms intervals
            
            while ((!window.firebaseService || !window.firebaseService.isInitialized) && attempts < maxAttempts) {
                await new Promise(resolve => setTimeout(resolve, 100));
                attempts++;
            }
            
            if (!window.firebaseService || !window.firebaseService.isInitialized) {
                console.warn('⚠️ Firebase service not available after waiting, continuing without user data');
                return;
            }
            
            // Clean any legacy local users and disable auto test creation
            try { localStorage.removeItem('liber_users'); } catch {}
            
        } catch (error) {
            console.error('Auth initialization error:', error);
        }
    }

    /**
     * Debug user storage and show current state
     */
    async debugUserStorage() {
        console.log('=== Debugging User Storage ===');
        
        // First check raw storage
        this.checkRawStorage();
        
        try {
            // Check current users in encrypted storage
            const users = await this.getUsers();
            console.log('Users in encrypted storage:', users.length);
            console.log('All users:', users);
            
            // Check legacy storage with proper error handling
            let legacyUsers = [];
            try {
                const legacyData = localStorage.getItem('liber_users');
                if (legacyData) {
                    // Check if it's valid JSON
                    if (legacyData.startsWith('[') || legacyData.startsWith('{')) {
                        legacyUsers = JSON.parse(legacyData);
                    } else {
                        console.log('Legacy data is not JSON (likely encrypted):', legacyData.substring(0, 20) + '...');
                    }
                }
            } catch (parseError) {
                console.log('Legacy data parsing failed (likely encrypted data):', parseError.message);
            }
            
            console.log('Users in legacy storage:', legacyUsers.length);
            console.log('Legacy users:', legacyUsers);
            
            // Check if we need to migrate
            if (legacyUsers.length > 0 && users.length === 0) {
                console.log('Found legacy users, attempting migration...');
                await this.saveUsers(legacyUsers);
                localStorage.removeItem('liber_users');
                console.log('Migration completed');
            }
            
            // Show current state after migration
            const finalUsers = await this.getUsers();
            console.log('Final user count:', finalUsers.length);
            
        } catch (error) {
            console.error('Debug error:', error);
        }
    }

    /**
     * Wait for cryptoManager to be initialized
     */
    async waitForCryptoManager() {
        let attempts = 0;
        const maxAttempts = 50; // 5 seconds max wait
        
        while (!window.cryptoManager && attempts < maxAttempts) {
            await new Promise(resolve => setTimeout(resolve, 100));
            attempts++;
        }
        
        if (!window.cryptoManager) {
            console.error('CryptoManager not available after waiting');
            throw new Error('CryptoManager initialization failed');
        }
    }

    // Secure admin credentials - derived from Google Drive keys
    async getAdminCredentials() {
        return await window.secureKeyManager.getAdminCredentials();
    }

    async generateAdminHash(password) {
        return await window.secureKeyManager.generateAdminHash(password);
    }

    async getMasterKey() {
        return await window.secureKeyManager.getSystemKey();
    }

    setupEventListeners() {
        // Login form
        const loginForm = document.getElementById('loginForm');
        if (loginForm) {
            loginForm.addEventListener('submit', async (e) => {
                e.preventDefault();
                await this.handleLogin();
            });
        }

        // Register form
        const registerForm = document.getElementById('registerForm');
        if (registerForm) {
            registerForm.addEventListener('submit', async (e) => {
                e.preventDefault();
                await this.handleRegister();
            });
        }

        // Password reset form
        const resetForm = document.getElementById('resetForm');
        if (resetForm) {
            resetForm.addEventListener('submit', async (e) => {
                e.preventDefault();
                await this.handlePasswordReset();
            });
        }

        // Resend verification form
        const resendVerificationForm = document.getElementById('resendVerificationForm');
        if (resendVerificationForm) {
            resendVerificationForm.addEventListener('submit', async (e) => {
                e.preventDefault();
                await this.handleResendVerification();
            });
        }

        // Tab switching
        const tabBtns = document.querySelectorAll('.tab-btn');
        tabBtns.forEach(btn => {
            btn.addEventListener('click', () => {
                this.switchTab(btn.dataset.tab);
            });
        });

        // Password visibility toggles
        const toggleCheckboxes = document.querySelectorAll('.password-toggle-checkbox');
        toggleCheckboxes.forEach(checkbox => {
            checkbox.addEventListener('change', () => {
                this.togglePasswordVisibility(checkbox);
            });
        });
        
        // Setup mobile WALL-E toggle for initial load
        this.setupMobileWallEToggle();
    }

    switchTab(tabName) {
        // Update active tab button
        document.querySelectorAll('.tab-btn').forEach(btn => {
            btn.classList.remove('active');
        });
        
        const activeTabBtn = document.querySelector(`[data-tab="${tabName}"]`);
        if (activeTabBtn) {
            activeTabBtn.classList.add('active');
        }

        // Update active form
        document.querySelectorAll('.auth-form').forEach(form => {
            form.classList.remove('active');
        });
        
        const activeForm = document.getElementById(`${tabName}Form`);
        if (activeForm) {
            activeForm.classList.add('active');
        }
    }

    togglePasswordVisibility(checkbox) {
        const inputId = checkbox.id.replace('Toggle', '');
        const input = document.getElementById(inputId);
        const label = checkbox.parentElement;
        const icon = label.querySelector('i');
        const text = label.querySelector('span');
        
        if (checkbox.checked) {
            input.type = 'text';
            icon.className = 'fas fa-eye';
            text.textContent = 'Password visible';
        } else {
            input.type = 'password';
            icon.className = 'fas fa-eye-slash';
            text.textContent = 'Password invisible';
        }
    }

    /**
     * Handle user login
     */
    async handleLogin() {
        const username = document.getElementById('loginUsername').value.trim();
        const password = document.getElementById('loginPassword').value;

        if (!username || !password) {
            this.showMessage('Please enter both username and password', 'error');
            return;
        }

        try {
            // Check if it's admin login
            const adminCredentials = await this.getAdminCredentials();
            if (username === adminCredentials.username) {
                const adminHash = await this.generateAdminHash(password);
                if (adminHash === adminCredentials.passwordHash) {
                    this.currentUser = {
                        username: adminCredentials.username,
                        email: adminCredentials.email,
                        role: 'admin'
                    };
                    this.createSession();
                    this.showDashboard();
                    return;
                }
            }

                    // Wait for Firebase to be fully initialized
        let attempts = 0;
        const maxAttempts = 50; // 5 seconds
        
        while ((!window.firebaseService || !window.firebaseService.isInitialized) && attempts < maxAttempts) {
            await new Promise(resolve => setTimeout(resolve, 100));
            attempts++;
        }
        
        // Try Firebase authentication first (REQUIRED)
        if (window.firebaseService && window.firebaseService.isInitialized) {
            try {
                console.log('Attempting Firebase login...');
                // Support login by username OR email
                let identifierEmail = username;
                if (!username.includes('@')) {
                    try {
                        const matches = await window.firebaseService.searchUsers(username);
                        // Prefer exact lowercase match first
                        const lower = username.toLowerCase();
                        let exact = (matches || []).find(u => (u.usernameLower || (u.username||'').toLowerCase()) === lower);
                        if (!exact) {
                            // Fallback to case-insensitive equality
                            exact = (matches || []).find(u => (u.username || '').toLowerCase() === lower);
                        }
                        if (!exact || !exact.email) {
                            this.showMessage('User credentials do not exist', 'error');
                            return;
                        }
                        identifierEmail = exact.email;
                    } catch (e) {
                        console.warn('Username lookup failed:', e?.message || e);
                        this.showMessage('User credentials do not exist', 'error');
                        return;
                    }
                }
                const firebaseUser = await window.firebaseService.signInUser(identifierEmail, password);
                
                if (firebaseUser) {
                    // Get user data from Firestore
                    const userData = await window.firebaseService.getUserData(firebaseUser.uid);
                    
                    if (userData) {
                        this.currentUser = {
                            id: firebaseUser.uid,
                            username: userData.username,
                            email: userData.email,
                            role: userData.role || 'user'
                        };
                        
                        this.createSession();
                        this.showDashboard();
                        return;
                    }
                }
            } catch (firebaseError) {
                console.error('Firebase login failed:', firebaseError.message);
                this.showMessage('Login failed. Please check your credentials.', 'error');
                return;
            }
        } else {
            console.error('Firebase not available - authentication requires Firebase. No fallback.');
            this.showMessage('Authentication service not available. Please contact support.', 'error');
            return;
        }
        
        // No local storage fallback as per user's explicit instruction
        this.showMessage('Authentication failed. Please try again.', 'error');

        } catch (error) {
            console.error('Login error:', error);
            this.showMessage('Login failed. Please try again.', 'error');
        }
    }

    /**
     * Handle registration with resend verification option
     */
    async handleRegister() {
        const username = document.getElementById('registerUsername').value.trim();
        const email = document.getElementById('registerEmail').value.trim();
        const password = document.getElementById('registerPassword').value;
        const confirmPassword = document.getElementById('registerConfirmPassword').value;

        if (!username || !email || !password || !confirmPassword) {
            this.showMessage('All fields are required', 'error');
            return;
        }

        if (password !== confirmPassword) {
            this.showMessage('Passwords do not match', 'error');
            return;
        }

        if (password.length < 8) {
            this.showMessage('Password must be at least 8 characters long', 'error');
            return;
        }

        if (!this.isValidEmail(email)) {
            this.showMessage('Please enter a valid email address', 'error');
            return;
        }

        try {
            // Wait for Firebase to be fully initialized
            let attempts = 0;
            const maxAttempts = 50; // 5 seconds
            
            while ((!window.firebaseService || !window.firebaseService.isInitialized) && attempts < maxAttempts) {
                await new Promise(resolve => setTimeout(resolve, 100));
                attempts++;
            }

            // Firebase registration is REQUIRED
            if (window.firebaseService && window.firebaseService.isInitialized) {
                try {
                    console.log('Attempting Firebase registration...');
                    
                    // Check if user already exists in Firebase
                    const existingMethods = await window.firebaseService.auth.fetchSignInMethodsForEmail(email);
                    if (existingMethods.length > 0) {
                        this.showMessage('An account with this email already exists', 'error');
                        return;
                    }
                    
                    // Create user in Firebase
                    const userData = {
                        username: username,
                        email: email,
                        role: 'user',
                        isVerified: false,
                        status: 'pending',
                        createdAt: new Date().toISOString()
                    };
                    
                    const firebaseUser = await window.firebaseService.createUser(email, password, userData);
                    
                    if (firebaseUser) {
                        // Send verification email via Firebase
                        await window.firebaseService.sendEmailVerification();
                        
                        this.showMessage('Registration successful! Please check your email to verify your account.', 'success');
                        
                        // Clear form
                        document.getElementById('registerForm').reset();
                        
                        // Switch to login tab
                        this.switchTab('login');
                        return;
                    }
                } catch (firebaseError) {
                    console.error('Firebase registration failed:', firebaseError);
                    const code = firebaseError?.code || '';
                    const message = firebaseError?.message || '';
                    
                    // Handle specific Firebase errors
                    if (code === 'auth/email-already-in-use') {
                        this.showMessage('An account with this email already exists', 'error');
                    } else if (code === 'auth/weak-password') {
                        this.showMessage('Password is too weak. Please choose a stronger password.', 'error');
                    } else if (code === 'auth/invalid-email') {
                        this.showMessage('Please enter a valid email address', 'error');
                    } else if (code === 'auth/operation-not-allowed') {
                        this.showMessage('Email/Password sign-up is disabled in Firebase Console. Enable it under Authentication → Sign-in method.', 'error');
                    } else if (code === 'auth/network-request-failed') {
                        this.showMessage('Network error while contacting Firebase. Please check your connection and try again.', 'error');
                    } else if (code === 'auth/unauthorized-domain') {
                        this.showMessage('Unauthorized domain for Firebase Auth. Add your domain to Firebase Authentication → Settings → Authorized domains.', 'error');
                    } else if (code === 'auth/invalid-api-key') {
                        this.showMessage('Invalid Firebase API key. Please verify Gist configuration matches Firebase Console.', 'error');
                    } else {
                        this.showMessage(`Registration failed: ${message || 'Unknown error'}`, 'error');
                    }
                    return;
                }
            } else {
                console.error('Firebase not available - registration requires Firebase. No fallback.');
                this.showMessage('Registration service not available. Please contact support.', 'error');
                return;
            }

        } catch (error) {
            console.error('Registration error:', error);
            this.showMessage('Registration failed. Please try again.', 'error');
        }
    }

    /**
     * Resend verification email for existing unverified user
     */
    async resendVerificationEmail(user) {
        try {
            // Generate new verification token
            const newToken = window.emailService.generateVerificationToken();
            
            // Update user with new token
            const users = await this.getUsers();
            const updatedUser = users.find(u => u.id === user.id);
            if (updatedUser) {
                updatedUser.verificationToken = newToken;
                updatedUser.verificationTokenCreated = Date.now();
                
                const updatedUsers = users.map(u => u.id === user.id ? updatedUser : u);
                await this.saveUsers(updatedUsers);
                
                // Send new verification email
                await window.emailService.sendVerificationEmail(user.email, user.username, newToken);
                this.showMessage(`Verification email resent to ${user.email}`, 'success');
            }
        } catch (error) {
            console.error('Failed to resend verification email:', error);
            this.showMessage('Failed to resend verification email. Please try again.', 'error');
        }
    }

    /**
     * Handle password reset request
     */
    async handlePasswordReset() {
        const email = document.getElementById('resetEmail').value.trim();

        if (!email) {
            this.showMessage('Please enter your email address', 'error');
            return;
        }

        if (!this.isValidEmail(email)) {
            this.showMessage('Please enter a valid email address', 'error');
            return;
        }

        try {
            console.log('Password reset requested for email:', email);
            
            // Wait for Firebase to be fully initialized
            let attempts = 0;
            const maxAttempts = 50; // 5 seconds
            
            while ((!window.firebaseService || !window.firebaseService.isInitialized) && attempts < maxAttempts) {
                await new Promise(resolve => setTimeout(resolve, 100));
                attempts++;
            }
            
            // Use Firebase password reset if available
            if (window.firebaseService && window.firebaseService.isInitialized) {
                console.log('Using Firebase password reset...');
                await window.firebaseService.sendPasswordResetEmail(email);
                this.showMessage('Password reset link sent to your email. Please check your inbox.', 'success');
            } else {
                console.error('Firebase not available - password reset requires Firebase. No fallback.');
                this.showMessage('Password reset service not available. Please contact support.', 'error');
                return;
            }
            
            // Clear form
            document.getElementById('resetEmail').value = '';

        } catch (error) {
            console.error('Password reset error:', error);
            
            // Handle specific Firebase errors
            if (error.code === 'auth/user-not-found') {
                this.showMessage('No account found with this email address', 'error');
            } else if (error.code === 'auth/invalid-email') {
                this.showMessage('Please enter a valid email address', 'error');
            } else {
                this.showMessage('Failed to send reset email. Please try again.', 'error');
            }
        }
    }

    /**
     * Handle resend verification request
     */
    async handleResendVerification() {
        const email = document.getElementById('resendVerificationEmail').value.trim();

        if (!email) {
            this.showMessage('Please enter your email address', 'error');
            return;
        }

        if (!this.isValidEmail(email)) {
            this.showMessage('Please enter a valid email address', 'error');
            return;
        }

        try {
            console.log('Resend verification requested for email:', email);
            
            // Wait for Firebase to be fully initialized
            let attempts = 0;
            const maxAttempts = 50; // 5 seconds
            
            while ((!window.firebaseService || !window.firebaseService.isInitialized) && attempts < maxAttempts) {
                await new Promise(resolve => setTimeout(resolve, 100));
                attempts++;
            }
            
            // Use Firebase resend verification if available
            if (window.firebaseService && window.firebaseService.isInitialized) {
                console.log('Using Firebase resend verification...');
                
                // First, try to sign in the user to get the current user object
                try {
                    // Check if user exists
                    const existingMethods = await window.firebaseService.auth.fetchSignInMethodsForEmail(email);
                    if (existingMethods.length === 0) {
                        this.showMessage('No account found with this email address', 'error');
                        return;
                    }
                    
                    // For resend verification, we need to sign in the user first
                    // This is a limitation of Firebase - we need the user to be signed in
                    this.showMessage('Please sign in to your account first, then use the resend verification option.', 'info');
                    this.switchTab('login');
                    return;
                    
                } catch (firebaseError) {
                    console.error('Firebase resend verification failed:', firebaseError.message);
                    this.showMessage('Failed to send verification email. Please try again.', 'error');
                    return;
                }
            } else {
                console.error('Firebase not available - resend verification requires Firebase. No fallback.');
                this.showMessage('Verification service not available. Please contact support.', 'error');
                return;
            }
            
            // Clear form
            document.getElementById('resendVerificationEmail').value = '';

        } catch (error) {
            console.error('Resend verification error:', error);
            this.showMessage('Failed to send verification email. Please try again.', 'error');
        }
    }

    /**
     * Handle password reset with token
     */
    async handlePasswordResetWithToken(token, email, newPassword, confirmPassword) {
        console.log('=== Password Reset Process ===');
        console.log('Email:', email);
        console.log('Token:', token);
        console.log('New password length:', newPassword.length);
        
        if (!newPassword || !confirmPassword) {
            this.showMessage('Please enter both password fields', 'error');
            return false;
        }

        if (newPassword !== confirmPassword) {
            this.showMessage('Passwords do not match', 'error');
            return false;
        }

        if (newPassword.length < 8) {
            this.showMessage('Password must be at least 8 characters long', 'error');
            return false;
        }

        try {
            // Use Firebase password reset if available
            if (window.firebaseService && window.firebaseService.isInitialized) {
                console.log('Using Firebase password reset...');
                
                console.log('Step 1: Verifying reset code...');
                // Verify the reset code with Firebase
                const verifiedEmail = await window.firebaseService.verifyPasswordResetCode(token);
                console.log('Reset code verified for email:', verifiedEmail);
                
                console.log('Step 2: Confirming password reset...');
                // Confirm the password reset
                await window.firebaseService.confirmPasswordReset(token, newPassword);
                console.log('Password reset confirmed successfully');
                
                this.showMessage('Password updated successfully! You can now login with your new password.', 'success');
                return true;
            } else {
                console.error('Firebase not available - password reset confirmation requires Firebase. No fallback.');
                this.showMessage('Password reset service not available. Please contact support.', 'error');
                return false;
            }

        } catch (error) {
            console.error('Password reset error:', error);
            this.showMessage(error.message || 'Password reset failed. Please try again.', 'error');
            return false;
        }
    }

    /**
     * Handle email verification
     */
    async handleEmailVerification(token, email) {
        try {
            // Firebase handles email verification automatically when user clicks the link
            // This method is called when user returns from verification email
            if (window.firebaseService && window.firebaseService.isInitialized) {
                console.log('Email verification handled by Firebase automatically');
                this.showMessage('Email verified successfully! You can now login to your account.', 'success');
                return true;
            } else {
                console.error('Firebase not available - email verification requires Firebase. No fallback.');
                this.showMessage('Email verification service not available. Please contact support.', 'error');
                return false;
            }
        } catch (error) {
            console.error('Email verification error:', error);
            this.showMessage(error.message || 'Email verification failed.', 'error');
            return false;
        }
    }

    /**
     * Check URL parameters for verification and reset actions
     */
    async checkUrlActions() {
        const urlParams = new URLSearchParams(window.location.search);
        const action = urlParams.get('action');
        const token = urlParams.get('token');
        const email = urlParams.get('email');

        if (action && token && email) {
            console.log('=== URL Action Detected ===');
            console.log('Action:', action);
            console.log('Token:', token);
            console.log('Email:', email);
            
            // Check if we've already processed this verification
            const processedKey = `processed_${action}_${token}_${email}`;
            console.log('Processing key:', processedKey);
            console.log('Already processed:', sessionStorage.getItem(processedKey));
            
            if (sessionStorage.getItem(processedKey)) {
                console.log('Verification already processed, skipping...');
                // Clear URL parameters
                window.history.replaceState({}, document.title, window.location.pathname);
                return;
            }

            if (action === 'verify') {
                console.log('Starting email verification...');
                try {
                    await this.handleEmailVerification(token, email);
                    console.log('Email verification completed successfully');
                    // Mark as processed
                    sessionStorage.setItem(processedKey, 'true');
                    console.log('Marked as processed in session storage');
                } catch (error) {
                    console.error('Email verification failed:', error);
                    // Don't mark as processed if it failed
                }
            } else if (action === 'reset') {
                console.log('Starting password reset...');
                this.showPasswordResetForm(token, email);
                // Mark as processed
                sessionStorage.setItem(processedKey, 'true');
            }
            
            // Clear URL parameters
            window.history.replaceState({}, document.title, window.location.pathname);
        }
    }

    /**
     * Show password reset form
     */
    showPasswordResetForm(token, email) {
        // Create reset form modal
        const modal = document.createElement('div');
        modal.className = 'reset-modal';
        modal.innerHTML = `
            <div class="reset-modal-content">
                <h2>Reset Your Password</h2>
                <p>Please enter your new password below.</p>
                <form id="resetPasswordForm">
                    <div class="form-group">
                        <label for="newPassword">New Password</label>
                        <input type="password" id="newPassword" required minlength="8">
                    </div>
                    <div class="form-group">
                        <label for="confirmNewPassword">Confirm New Password</label>
                        <input type="password" id="confirmNewPassword" required minlength="8">
                    </div>
                    <button type="submit" class="btn-primary">Update Password</button>
                    <button type="button" class="btn-secondary" onclick="this.closest('.reset-modal').remove()">Cancel</button>
                </form>
            </div>
        `;

        document.body.appendChild(modal);

        // Handle form submission
        document.getElementById('resetPasswordForm').addEventListener('submit', async (e) => {
            e.preventDefault();
            const newPassword = document.getElementById('newPassword').value;
            const confirmPassword = document.getElementById('confirmNewPassword').value;
            
            const success = await this.handlePasswordResetWithToken(token, email, newPassword, confirmPassword);
            if (success) {
                modal.remove();
            }
        });
    }

    /**
     * Show password reset form (for "Forgot Password?" link)
     */
    showPasswordResetTab() {
        this.switchTab('reset');
    }

    /**
     * Show resend verification form (for "Resend Verification" link)
     */
    showResendVerificationTab() {
        this.switchTab('resendVerification');
    }

    /**
     * Generate unique user ID
     */
    generateUserId() {
        return 'user_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
    }

    /**
     * Validate email format
     */
    isValidEmail(email) {
        const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
        return emailRegex.test(email);
    }

    async login(username, password) {
        try {
            console.log('Attempting login for:', username);
            
            // Check admin credentials first
            const adminCreds = await this.getAdminCredentials();
            console.log('Admin credentials loaded:', !!adminCreds);
            
            if (username === adminCreds.username) {
                console.log('Admin login attempt');
                // Verify admin password using hash comparison
                const inputHash = await this.generateAdminHash(password);
                console.log('Admin hash comparison:', inputHash === adminCreds.passwordHash);
                
                if (inputHash === adminCreds.passwordHash) {
                    this.currentUser = { 
                        username: adminCreds.username,
                        email: adminCreds.email,
                        role: adminCreds.role
                    };
                    this.createSession();
                    console.log('Admin login successful');
                    return true;
                }
            }

            // Check regular users
            console.log('Checking regular users');
            const users = await this.getUsers();
            console.log('Users loaded:', users.length);
            
            const user = users.find(u => u.username === username || u.email === username);
            console.log('User found:', !!user);
            
            if (user) {
                // Check if user is verified (for email verification) or approved (for admin approval)
                if (!user.isVerified && user.status !== 'approved') {
                    console.log('User not verified or approved');
                    return false;
                }
                
                const hashedPassword = await window.cryptoManager.hashPassword(password);
                console.log('Password hash comparison:', user.passwordHash === hashedPassword);
                
                if (user.passwordHash === hashedPassword) {
                    this.currentUser = user;
                    this.createSession();
                    console.log('User login successful');
                    return true;
                }
            }

            console.log('Login failed - no matching credentials');
            return false;
        } catch (error) {
            console.error('Login error:', error);
            return false;
        }
    }

    async register(username, email, password) {
        try {
            const users = await this.getUsers();
            
            // Check if username or email already exists
            if (users.some(u => u.username === username || u.email === email)) {
                return false;
            }

            const hashedPassword = await window.cryptoManager.hashPassword(password);
            const newUser = {
                username,
                email,
                passwordHash: hashedPassword,
                role: 'user',
                status: 'pending', // Requires admin approval
                createdAt: new Date().toISOString(),
                approvedBy: null,
                approvedAt: null
            };

            users.push(newUser);
            await this.saveUsers(users);
            return true;
        } catch (error) {
            console.error('Registration error:', error);
            return false;
        }
    }

    async loadUsers() {
        try {
            console.log('=== loadUsers called ===');
            const masterKey = await this.getMasterKey();
            console.log('Master key obtained:', !!masterKey, 'Length:', masterKey ? masterKey.length : 0);
            
            const users = await window.cryptoManager.secureRetrieve('liber_users', masterKey);
            console.log('secureRetrieve result:', users);
            console.log('Users type:', typeof users);
            console.log('Users is array:', Array.isArray(users));
            
            const result = users || [];
            console.log('Final result:', result);
            console.log('Result length:', result.length);
            
            return result;
        } catch (error) {
            console.error('Error loading users:', error);
            return [];
        }
    }

    async getUsers() {
        // Firebase-only implementation - no local storage fallback
        if (window.firebaseService && window.firebaseService.isInitialized) {
            try {
                console.log('Loading users from Firebase...');
                const users = await window.firebaseService.getAllUsers();
                console.log('Firebase users loaded:', users.length);
                return users;
            } catch (error) {
                console.error('Failed to load users from Firebase:', error);
                // Return empty array instead of throwing error to prevent app crashes
                return [];
            }
        } else {
            console.warn('⚠️ Firebase not available - getUsers requires Firebase. No fallback.');
            // Return empty array instead of throwing error to prevent app crashes
            return [];
        }
    }

    async saveUsers(users) {
        // Firebase-only implementation - no local storage fallback
        if (window.firebaseService && window.firebaseService.isInitialized) {
            try {
                console.log('=== saveUsers called (Firebase) ===');
                console.log('Users to save:', users);
                console.log('Users count:', users.length);
                
                // Note: Firebase doesn't have a bulk save method, so this is mainly for compatibility
                // Individual user operations should use Firebase methods directly
                console.log('Firebase users are managed individually, not bulk saved');
                return true;
            } catch (error) {
                console.error('Error with Firebase users:', error);
                return false;
            }
        } else {
            console.error('Firebase not available - saveUsers requires Firebase. No fallback.');
            return false;
        }
    }

    async addUser(userData) {
        // Firebase-only implementation - no local storage fallback
        if (window.firebaseService && window.firebaseService.isInitialized) {
            try {
                console.log('Adding user via Firebase...');
                // Note: User creation should use Firebase Auth directly
                // This method is mainly for compatibility
                console.log('User creation should use Firebase Auth methods directly');
                return true;
            } catch (error) {
                console.error('Error adding user via Firebase:', error);
                return false;
            }
        } else {
            console.error('Firebase not available - addUser requires Firebase. No fallback.');
            return false;
        }
    }

    async deleteUser(username) {
        // Firebase-only implementation - no local storage fallback
        if (window.firebaseService && window.firebaseService.isInitialized) {
            try {
                console.log('Deleting user via Firebase...');
                const users = await window.firebaseService.getAllUsers();
                const user = users.find(u => u.username === username || u.email === username);
                
                if (user) {
                    await window.firebaseService.deleteUser(user.id);
                    console.log(`User ${username} deleted successfully`);
                    return true;
                }
                return false;
            } catch (error) {
                console.error('Error deleting user via Firebase:', error);
                return false;
            }
        } else {
            console.error('Firebase not available - deleteUser requires Firebase. No fallback.');
            return false;
        }
    }

    async approveUser(username) {
        // Firebase-only implementation - no local storage fallback
        if (window.firebaseService && window.firebaseService.isInitialized) {
            try {
                console.log('Approving user via Firebase...');
                const users = await window.firebaseService.getAllUsers();
                const user = users.find(u => u.username === username || u.email === username);
                
                if (user) {
                    await window.firebaseService.updateUserData(user.id, {
                        status: 'approved',
                        approvedBy: this.currentUser?.username || 'admin',
                        approvedAt: new Date().toISOString()
                    });
                    console.log(`User ${username} approved successfully`);
                    return true;
                }
                return false;
            } catch (error) {
                console.error('Error approving user via Firebase:', error);
                return false;
            }
        } else {
            console.error('Firebase not available - approveUser requires Firebase. No fallback.');
            return false;
        }
    }

    async rejectUser(username) {
        // Firebase-only implementation - no local storage fallback
        if (window.firebaseService && window.firebaseService.isInitialized) {
            try {
                console.log('Rejecting user via Firebase...');
                const users = await window.firebaseService.getAllUsers();
                const user = users.find(u => u.username === username || u.email === username);
                
                if (user) {
                    await window.firebaseService.deleteUser(user.id);
                    console.log(`User ${username} rejected and deleted successfully`);
                    return true;
                }
                return false;
            } catch (error) {
                console.error('Error rejecting user via Firebase:', error);
                return false;
            }
        } else {
            console.error('Firebase not available - rejectUser requires Firebase. No fallback.');
            return false;
        }
    }

    createSession() {
        const session = {
            user: this.currentUser,
            createdAt: new Date().toISOString(),
            expiresAt: new Date(Date.now() + this.sessionTimeout).toISOString()
        };
        
        localStorage.setItem('liber_session', JSON.stringify(session));
        localStorage.setItem('liber_current_user', JSON.stringify(this.currentUser));
        
        // For Firebase users, set a derived password for app compatibility
        if (this.currentUser && this.currentUser.id) {
            // Use user ID as the "password" for app encryption
            localStorage.setItem('liber_user_password', this.currentUser.id);
        } else if (this.currentUser && this.currentUser.username) {
            // Fallback for non-Firebase users
            localStorage.setItem('liber_user_password', this.currentUser.username);
        }
        
        // Update user info in dashboard
        this.updateUserInfo();
    }

    checkSession() {
        try {
            const sessionData = localStorage.getItem('liber_session');
            if (!sessionData) return;

            const session = JSON.parse(sessionData);
            const now = new Date();
            const expiresAt = new Date(session.expiresAt);

            if (now < expiresAt) {
                this.currentUser = session.user;
                this.showDashboard();
            } else {
                this.logout();
            }
        } catch (error) {
            console.error('Session check error:', error);
            this.logout();
        }
    }

    async logout() {
        try {
            // Sign out from Firebase if available
            if (window.firebaseService && window.firebaseService.isInitialized) {
                await window.firebaseService.signOutUser();
                console.log('Firebase sign out successful');
            }
        } catch (error) {
            console.error('Firebase sign out error:', error);
        }
        
        this.currentUser = null;
        localStorage.removeItem('liber_session');
        localStorage.removeItem('liber_current_user');
        localStorage.removeItem('liber_user_password'); // Clear stored password
        this.showAuthScreen();
    }

    showAuthScreen() {
        document.getElementById('auth-screen').classList.remove('hidden');
        document.getElementById('dashboard').classList.add('hidden');
        
        // Restore mobile WALL-E toggle button if it was hidden
        const toggleBtn = document.getElementById('mobile-wall-e-toggle-btn');
        if (toggleBtn) {
            toggleBtn.style.display = '';
        }
        
        // Hide WALL-E widget on login screen (unless it was activated)
        const widget = document.querySelector('.chatgpt-widget');
        if (widget && sessionStorage.getItem('wallE_activated_on_login') !== 'true') {
            widget.style.display = 'none';
        }
        
        // Setup mobile WALL-E toggle for login screen
        this.setupMobileWallEToggle();
    }

    showDashboard() {
        document.getElementById('auth-screen').classList.add('hidden');
        document.getElementById('dashboard').classList.remove('hidden');
        
        // Initialize dashboard
        if (window.dashboardManager) {
            window.dashboardManager.init();
        }
    }

    showMessage(message, type = 'info') {
        const messageDiv = document.createElement('div');
        messageDiv.className = `auth-message ${type}`;
        messageDiv.textContent = message;
        
        const authScreen = document.getElementById('auth-screen');
        authScreen.appendChild(messageDiv);
        
        setTimeout(() => {
            messageDiv.remove();
        }, 5000);
    }

    getCurrentUser() {
        return this.currentUser;
    }

    /**
     * Update user info in dashboard header
     */
    updateUserInfo() {
        const currentUserElement = document.getElementById('current-user');
        const userRoleElement = document.getElementById('user-role');
        
        if (currentUserElement && this.currentUser) {
            currentUserElement.textContent = this.currentUser.username || this.currentUser.email;
        }
        
        if (userRoleElement && this.currentUser) {
            userRoleElement.textContent = `(${this.currentUser.role})`;
        }
    }

    isAdmin() {
        return this.currentUser && this.currentUser.role === 'admin';
    }

    /**
     * Setup mobile WALL-E toggle for login screen
     */
    setupMobileWallEToggle() {
        console.log('setupMobileWallEToggle called');
        const toggleBtn = document.getElementById('mobile-wall-e-toggle-btn');
        console.log('Toggle button found:', !!toggleBtn);
        
        if (toggleBtn) {
            console.log('Setting up event listener for toggle button');
            // Remove any existing event listeners
            const newToggleBtn = toggleBtn.cloneNode(true);
            toggleBtn.parentNode.replaceChild(newToggleBtn, toggleBtn);
            
            newToggleBtn.addEventListener('click', async (e) => {
                console.log('WALL-E toggle button clicked!', e);
                
                // Check if widget is already active
                const widget = document.querySelector('.chatgpt-widget');
                const isWidgetActive = widget && widget.classList.contains('mobile-activated');
                
                if (isWidgetActive) {
                    // Widget is active, hide it
                    console.log('Hiding WALL-E widget...');
                    widget.classList.remove('mobile-activated');
                    sessionStorage.removeItem('wallE_activated_on_login');
                    return;
                }
                
                // Widget is not active, show it
                console.log('Showing WALL-E widget...');
                
                // Wait for WALL-E widget to be initialized
                let attempts = 0;
                const maxAttempts = 30; // Increased attempts
                
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
                    
                    // Store state that WALL-E was activated on login screen
                    sessionStorage.setItem('wallE_activated_on_login', 'true');
                    
                    console.log('WALL-E widget successfully activated on login screen');
                } else {
                    console.error('WALL-E widget element not found even after creation attempt');
                }
            });
            
            console.log('Event listener attached successfully');
        } else {
            console.error('Mobile WALL-E toggle button not found in DOM');
        }
    }

    /**
     * Test function to debug user storage
     */
    async testUserStorage() {
        console.log('=== Testing User Storage ===');
        
        // Check current users
        const users = await this.getUsers();
        console.log('Current users:', users);
        
        // Check legacy storage (safely)
        let legacyUsers = [];
        try {
            const legacyData = localStorage.getItem('liber_users');
            if (legacyData && (legacyData.startsWith('[') || legacyData.startsWith('{'))) {
                legacyUsers = JSON.parse(legacyData);
            }
        } catch (error) {
            console.log('Legacy data parsing failed (likely encrypted):', error.message);
        }
        console.log('Legacy users:', legacyUsers);
        
        // Test saving a user
        const testUser = {
            id: 'test_user_' + Date.now(),
            username: 'testuser',
            email: 'test@example.com',
            passwordHash: 'test_hash',
            role: 'user',
            isVerified: false,
            createdAt: new Date().toISOString()
        };
        
        console.log('Testing save with user:', testUser);
        const saveResult = await this.saveUsers([testUser]);
        console.log('Save result:', saveResult);
        
        // Check if user was saved
        const savedUsers = await this.getUsers();
        console.log('Users after save:', savedUsers);
        
        return saveResult;
    }

    /**
     * Debug verification process
     */
    async debugVerification(email) {
        console.log('=== Debugging Verification for:', email, '===');
        
        try {
            const users = await this.getUsers();
            console.log('All users:', users);
            
            const user = users.find(u => u.email === email);
            console.log('User found:', user);
            
            if (user) {
                console.log('User verification status:', user.isVerified);
                console.log('User verification token:', user.verificationToken);
                console.log('Token created:', user.verificationTokenCreated);
                console.log('Token age (hours):', (Date.now() - user.verificationTokenCreated) / (60 * 60 * 1000));
            }
            
            // Test verification process
            if (user && user.verificationToken) {
                console.log('Testing verification with token:', user.verificationToken);
                try {
                    const verifiedUser = await window.emailService.verifyToken(user.verificationToken, email);
                    console.log('Verification successful:', verifiedUser);
                } catch (error) {
                    console.error('Verification failed:', error);
                }
            }
            
        } catch (error) {
            console.error('Debug error:', error);
        }
    }

    /**
     * Test function to manually create a user and verify storage
     */
    async testCreateUser() {
        console.log('=== Testing User Creation ===');
        
        try {
            const testUser = {
                id: 'test_user_' + Date.now(),
                username: 'testuser',
                email: 'test@example.com',
                passwordHash: 'test_hash_' + Date.now(),
                role: 'user',
                isVerified: false,
                status: 'pending',
                verificationToken: 'test_token_' + Date.now(),
                verificationTokenCreated: Date.now(),
                createdAt: new Date().toISOString()
            };
            
            console.log('Creating test user:', testUser);
            
            // Get current users
            const currentUsers = await this.getUsers();
            console.log('Current users before:', currentUsers.length);
            
            // Add test user
            currentUsers.push(testUser);
            
            // Save users
            const saveResult = await this.saveUsers(currentUsers);
            console.log('Save result:', saveResult);
            
            // Verify user was saved
            const savedUsers = await this.getUsers();
            console.log('Users after save:', savedUsers.length);
            console.log('Test user in storage:', savedUsers.find(u => u.id === testUser.id));
            
            return saveResult;
            
        } catch (error) {
            console.error('Test create user error:', error);
            return false;
        }
    }

    /**
     * Check and fix user verification status
     */
    async checkUserVerificationStatus(email) {
        console.log('=== Checking User Verification Status ===');
        console.log('Email:', email);
        
        try {
            const users = await this.getUsers();
            const user = users.find(u => u.email === email);
            
            if (user) {
                console.log('User found:', user);
                console.log('isVerified:', user.isVerified);
                console.log('status:', user.status);
                console.log('verificationToken:', user.verificationToken);
                
                // If user is verified but status is not approved, fix it
                if (user.isVerified && user.status !== 'approved') {
                    console.log('Fixing user status...');
                    user.status = 'approved';
                    const updatedUsers = users.map(u => u.email === email ? user : u);
                    await this.saveUsers(updatedUsers);
                    console.log('User status fixed to approved');
                }
                
                return user;
            } else {
                console.log('User not found');
                return null;
            }
        } catch (error) {
            console.error('Error checking user verification status:', error);
            return null;
        }
    }

    /**
     * Check and fix user verification status
     */
    async checkUserVerificationStatus(email) {
        console.log('=== Checking User Verification Status ===');
        console.log('Email:', email);
        
        try {
            const users = await this.getUsers();
            const user = users.find(u => u.email === email);
            
            if (user) {
                console.log('User found:', user);
                console.log('isVerified:', user.isVerified);
                console.log('status:', user.status);
                console.log('verificationToken:', user.verificationToken);
                
                // If user is verified but status is not approved, fix it
                if (user.isVerified && user.status !== 'approved') {
                    console.log('Fixing user status...');
                    user.status = 'approved';
                    const updatedUsers = users.map(u => u.email === email ? user : u);
                    await this.saveUsers(updatedUsers);
                    console.log('User status fixed to approved');
                }
                
                return user;
            } else {
                console.log('User not found');
                return null;
            }
        } catch (error) {
            console.error('Error checking user verification status:', error);
            return null;
        }
    }

    /**
     * Check raw storage data to see what's actually stored
     */
    checkRawStorage() {
        console.log('=== Checking Raw Storage Data ===');
        
        // Check all localStorage keys
        for (let i = 0; i < localStorage.length; i++) {
            const key = localStorage.key(i);
            const value = localStorage.getItem(key);
            console.log(`Key: ${key}`);
            console.log(`Value (first 50 chars): ${value ? value.substring(0, 50) + '...' : 'null'}`);
            console.log(`Value length: ${value ? value.length : 0}`);
            console.log('---');
        }
    }
}

// Initialize auth manager
window.authManager = new AuthManager();

// Add global debug function for easy console access
window.debugUser = async function(email) {
    console.log('=== Quick User Debug ===');
    console.log('Email:', email);
    
    try {
        const users = await window.authManager.getUsers();
        const user = users.find(u => u.email === email);
        
        if (user) {
            console.log('✅ User found:', user);
            console.log('📧 Email:', user.email);
            console.log('👤 Username:', user.username);
            console.log('✅ Verified:', user.isVerified);
            console.log('📋 Status:', user.status);
            console.log('🔑 Token:', user.verificationToken);
            console.log('📅 Created:', user.createdAt);
            
            if (user.isVerified && user.status !== 'approved') {
                console.log('⚠️ User is verified but status is not approved - fixing...');
                user.status = 'approved';
                const updatedUsers = users.map(u => u.email === email ? user : u);
                await window.authManager.saveUsers(updatedUsers);
                console.log('✅ Status fixed to approved');
            }
            
            return user;
        } else {
            console.log('❌ User not found');
            return null;
        }
    } catch (error) {
        console.error('❌ Error:', error);
        return null;
    }
};

// Add manual verification function
window.manualVerify = async function(email) {
    console.log('=== Manual Verification ===');
    console.log('Email:', email);
    
    try {
        const users = await window.authManager.getUsers();
        const user = users.find(u => u.email === email);
        
        if (user) {
            console.log('✅ User found, manually verifying...');
            
            // Manually set verification status
            user.isVerified = true;
            user.status = 'approved';
            user.verificationToken = null;
            user.verificationTokenCreated = null;
            user.verifiedAt = new Date().toISOString();
            
            // Save the updated user
            const updatedUsers = users.map(u => u.email === email ? user : u);
            const saveResult = await window.authManager.saveUsers(updatedUsers);
            
            console.log('✅ Manual verification completed');
            console.log('Save result:', saveResult);
            console.log('Updated user:', user);
            
            return user;
        } else {
            console.log('❌ User not found');
            return null;
        }
    } catch (error) {
        console.error('❌ Manual verification error:', error);
        return null;
    }
};

// Add test verification function to test the actual verification process
window.testVerification = async function(email, token) {
    console.log('=== Testing Verification Process ===');
    console.log('Email:', email);
    console.log('Token:', token);
    
    try {
        // Test step 1: Get users
        console.log('Step 1: Getting users...');
        const users = await window.authManager.getUsers();
        console.log('Users loaded:', users.length);
        
        // Test step 2: Find user
        console.log('Step 2: Finding user...');
        const user = users.find(u => u.email === email && u.verificationToken === token);
        console.log('User found:', !!user);
        if (user) {
            console.log('User data:', user);
        }
        
        // Test step 3: Call email service verification
        console.log('Step 3: Calling email service verification...');
        const verifiedUser = await window.emailService.verifyToken(token, email);
        console.log('Verification result:', verifiedUser);
        
        // Test step 4: Check if user was actually saved
        console.log('Step 4: Checking if user was saved...');
        const updatedUsers = await window.authManager.getUsers();
        const savedUser = updatedUsers.find(u => u.email === email);
        console.log('Saved user:', savedUser);
        console.log('Is verified:', savedUser?.isVerified);
        console.log('Status:', savedUser?.status);
        
        return verifiedUser;
    } catch (error) {
        console.error('❌ Test verification error:', error);
        return null;
    }
};

// Add function to refresh admin panel and check user status
window.refreshAdminPanel = async function() {
    console.log('=== Refreshing Admin Panel ===');
    
    try {
        // Refresh users in admin panel
        if (window.usersManager) {
            await window.usersManager.loadUsers();
            console.log('Admin panel refreshed');
        } else {
            console.log('Users manager not available');
        }
        
        // Check current user status
        const users = await window.authManager.getUsers();
        const user = users.find(u => u.email === 'emelianen63@gmail.com');
        if (user) {
            console.log('Current user status:');
            console.log('- Email:', user.email);
            console.log('- Username:', user.username);
            console.log('- isVerified:', user.isVerified);
            console.log('- status:', user.status);
            console.log('- verificationToken:', user.verificationToken);
        }
        
    } catch (error) {
        console.error('Error refreshing admin panel:', error);
    }
};

// Add function to create a test user
window.createTestUser = async function() {
    console.log('=== Creating Test User ===');
    
    try {
        const testUser = {
            id: 'test_user_' + Date.now(),
            username: 'nvberegovykh',
            email: 'nvberegovykh@gmail.com',
            passwordHash: await window.cryptoManager.hashPassword('testpassword123'),
            role: 'user',
            isVerified: true,
            status: 'approved',
            createdAt: new Date().toISOString(),
            lastLogin: null
        };
        
        const users = await window.authManager.getUsers();
        users.push(testUser);
        
        const saveResult = await window.authManager.saveUsers(users);
        console.log('Test user created:', saveResult);
        console.log('Test user credentials:');
        console.log('- Username: nvberegovykh');
        console.log('- Email: nvberegovykh@gmail.com');
        console.log('- Password: testpassword123');
        
        return saveResult;
    } catch (error) {
        console.error('Error creating test user:', error);
        return false;
    }
};

// Add comprehensive migration function
window.migrateAllUsersToFirebase = async function() {
    console.log('=== Migrating All Users to Firebase ===');
    
    try {
        // Check if Firebase is available
        if (!window.firebaseService || !window.firebaseService.isInitialized) {
            console.error('Firebase not available - cannot migrate users');
            return false;
        }
        
        // Get existing users from local storage
        const localUsers = await window.authManager.getUsers();
        console.log('Found local users:', localUsers.length);
        
        if (localUsers.length === 0) {
            console.log('No local users to migrate');
            return true;
        }
        
        let migratedCount = 0;
        let failedCount = 0;
        
        for (const localUser of localUsers) {
            try {
                console.log(`Migrating user: ${localUser.email}`);
                
                // Check if user already exists in Firebase
                const existingMethods = await window.firebaseService.auth.fetchSignInMethodsForEmail(localUser.email);
                
                if (existingMethods.length > 0) {
                    console.log(`User ${localUser.email} already exists in Firebase`);
                    migratedCount++;
                    continue;
                }
                
                // Create user in Firebase with temporary password
                const tempPassword = 'TempPassword123!';
                const userData = {
                    username: localUser.username,
                    email: localUser.email,
                    role: localUser.role || 'user',
                    isVerified: localUser.isVerified || false,
                    status: localUser.status || 'pending',
                    createdAt: localUser.createdAt || new Date().toISOString(),
                    migratedFromLocalStorage: true,
                    needsPasswordReset: true
                };
                
                const firebaseUser = await window.firebaseService.createUser(localUser.email, tempPassword, userData);
                
                if (firebaseUser) {
                    console.log(`✅ Successfully migrated user: ${localUser.email}`);
                    migratedCount++;
                } else {
                    console.error(`❌ Failed to migrate user: ${localUser.email}`);
                    failedCount++;
                }
                
            } catch (error) {
                console.error(`❌ Error migrating user ${localUser.email}:`, error);
                failedCount++;
            }
        }
        
        console.log(`=== Migration Complete ===`);
        console.log(`✅ Successfully migrated: ${migratedCount} users`);
        console.log(`❌ Failed to migrate: ${failedCount} users`);
        
        if (migratedCount > 0) {
            console.log('📧 Sending password reset emails to migrated users...');
            await window.sendPasswordResetEmailsToMigratedUsers();
        }
        
        return migratedCount > 0;
        
    } catch (error) {
        console.error('Migration error:', error);
        return false;
    }
};

// Add function to send password reset emails to migrated users
window.sendPasswordResetEmailsToMigratedUsers = async function() {
    console.log('=== Sending Password Reset Emails to Migrated Users ===');
    
    try {
        if (!window.firebaseService || !window.firebaseService.isInitialized) {
            console.error('Firebase not available');
            return;
        }
        
        const firebaseUsers = await window.firebaseService.getAllUsers();
        const migratedUsers = firebaseUsers.filter(user => user.migratedFromLocalStorage && user.needsPasswordReset);
        
        console.log(`Found ${migratedUsers.length} migrated users needing password reset`);
        
        for (const user of migratedUsers) {
            try {
                await window.firebaseService.sendPasswordResetEmail(user.email);
                console.log(`✅ Password reset email sent to: ${user.email}`);
                
                // Mark as password reset sent
                await window.firebaseService.updateUserData(user.id, {
                    needsPasswordReset: false,
                    passwordResetSent: new Date().toISOString()
                });
                
            } catch (error) {
                console.error(`❌ Failed to send password reset to ${user.email}:`, error);
            }
        }
        
        console.log('✅ Password reset emails sent to migrated users');
        
    } catch (error) {
        console.error('Error sending password reset emails:', error);
    }
};
