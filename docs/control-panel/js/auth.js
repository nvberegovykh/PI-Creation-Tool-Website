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
        // Wait for cryptoManager to be available
        await this.waitForCryptoManager();
        this.checkSession();
        this.setupEventListeners();
        this.checkUrlActions(); // Check for verification and reset actions
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

            // Check regular users
            const users = JSON.parse(localStorage.getItem('liber_users') || '[]');
            const user = users.find(u => u.username === username || u.email === username);

            if (!user) {
                this.showMessage('Invalid username or password', 'error');
                return;
            }

            // Check if email is verified
            if (!user.isVerified) {
                this.showMessage('Please verify your email address before logging in. Check your inbox for the verification link.', 'error');
                return;
            }

            // Verify password
            const isValidPassword = await window.cryptoManager.verifyPassword(password, user.passwordHash);
            if (!isValidPassword) {
                this.showMessage('Invalid username or password', 'error');
                return;
            }

            // Update last login
            user.lastLogin = new Date().toISOString();
            const updatedUsers = users.map(u => u.id === user.id ? user : u);
            localStorage.setItem('liber_users', JSON.stringify(updatedUsers));

            // Set current user
            this.currentUser = {
                id: user.id,
                username: user.username,
                email: user.email,
                role: user.role
            };

            this.createSession();
            this.showDashboard();

        } catch (error) {
            console.error('Login error:', error);
            this.showMessage('Login failed. Please try again.', 'error');
        }
    }

    /**
     * Handle user registration with email verification
     */
    async handleRegister() {
        const username = document.getElementById('registerUsername').value.trim();
        const email = document.getElementById('registerEmail').value.trim();
        const password = document.getElementById('registerPassword').value;
        const confirmPassword = document.getElementById('registerConfirmPassword').value;

        // Validation
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
            // Check if user already exists
            const users = JSON.parse(localStorage.getItem('liber_users') || '[]');
            const existingUser = users.find(u => u.username === username || u.email === email);
            
            if (existingUser) {
                this.showMessage('Username or email already exists', 'error');
                return;
            }

            // Hash password
            const hashedPassword = await window.cryptoManager.hashPassword(password);
            
            // Generate verification token
            const verificationToken = window.emailService.generateVerificationToken();
            
            // Create new user
            const newUser = {
                id: this.generateUserId(),
                username: username,
                email: email,
                passwordHash: hashedPassword,
                role: 'user',
                isVerified: false,
                verificationToken: verificationToken,
                verificationTokenCreated: Date.now(),
                createdAt: new Date().toISOString(),
                lastLogin: null
            };

            // Add user to storage
            users.push(newUser);
            localStorage.setItem('liber_users', JSON.stringify(users));

            // Send verification email
            try {
                console.log('Sending verification email to:', email);
                console.log('Email service available:', !!window.emailService);
                
                await window.emailService.sendVerificationEmail(email, username, verificationToken);
                console.log('Verification email sent successfully');
                
                this.showMessage('Registration successful! Please check your email to verify your account.', 'success');
                
                // Clear form
                document.getElementById('registerForm').reset();
                
                // Switch to login tab
                this.switchTab('login');
            } catch (emailError) {
                console.error('Failed to send verification email:', emailError);
                this.showMessage('Registration successful, but verification email could not be sent. Please contact support.', 'warning');
            }

        } catch (error) {
            console.error('Registration error:', error);
            this.showMessage('Registration failed. Please try again.', 'error');
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
            
            // Check if email service is available
            if (!window.emailService) {
                console.error('Email service not available');
                this.showMessage('Email service not available. Please try again later.', 'error');
                return;
            }

            const users = JSON.parse(localStorage.getItem('liber_users') || '[]');
            const user = users.find(u => u.email === email);

            if (!user) {
                this.showMessage('If an account with this email exists, a reset link will be sent.', 'info');
                return;
            }

            console.log('User found, generating reset token...');

            // Generate reset token
            const resetToken = window.emailService.generateResetToken();
            
            // Update user with reset token
            user.resetToken = resetToken;
            user.resetTokenCreated = Date.now();
            
            const updatedUsers = users.map(u => u.email === email ? user : u);
            localStorage.setItem('liber_users', JSON.stringify(updatedUsers));

            console.log('Sending password reset email...');

            // Send reset email
            await window.emailService.sendPasswordResetEmail(email, user.username, resetToken);
            this.showMessage('Password reset link sent to your email. Please check your inbox.', 'success');
            
            // Clear form
            document.getElementById('resetEmail').value = '';

        } catch (error) {
            console.error('Password reset error:', error);
            this.showMessage('Failed to send reset email. Please try again.', 'error');
        }
    }

    /**
     * Handle password reset with token
     */
    async handlePasswordResetWithToken(token, email, newPassword, confirmPassword) {
        if (!newPassword || !confirmPassword) {
            this.showMessage('Please enter both password fields', 'error');
            return;
        }

        if (newPassword !== confirmPassword) {
            this.showMessage('Passwords do not match', 'error');
            return;
        }

        if (newPassword.length < 8) {
            this.showMessage('Password must be at least 8 characters long', 'error');
            return;
        }

        try {
            // Verify reset token
            await window.emailService.verifyResetToken(token, email);
            
            // Update password
            await window.emailService.updatePassword(email, newPassword);
            
            this.showMessage('Password updated successfully! You can now login with your new password.', 'success');
            return true;

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
            const user = await window.emailService.verifyToken(token, email);
            this.showMessage('Email verified successfully! You can now login to your account.', 'success');
            return true;
        } catch (error) {
            console.error('Email verification error:', error);
            this.showMessage(error.message || 'Email verification failed.', 'error');
            return false;
        }
    }

    /**
     * Check URL parameters for verification and reset actions
     */
    checkUrlActions() {
        const urlParams = new URLSearchParams(window.location.search);
        const action = urlParams.get('action');
        const token = urlParams.get('token');
        const email = urlParams.get('email');

        if (action && token && email) {
            if (action === 'verify') {
                this.handleEmailVerification(token, email);
            } else if (action === 'reset') {
                this.showPasswordResetForm(token, email);
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
            
            const user = users.find(u => u.username === username && u.status === 'approved');
            console.log('User found:', !!user);
            
            if (user) {
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
            const masterKey = await this.getMasterKey();
            const users = await window.cryptoManager.secureRetrieve('liber_users', masterKey);
            return users || [];
        } catch (error) {
            console.error('Error loading users:', error);
            return [];
        }
    }

    async getUsers() {
        return await this.loadUsers();
    }

    async saveUsers(users) {
        try {
            const masterKey = await this.getMasterKey();
            return await window.cryptoManager.secureStore('liber_users', users, masterKey);
        } catch (error) {
            console.error('Error saving users:', error);
            return false;
        }
    }

    async addUser(userData) {
        try {
            const users = await this.getUsers();
            users.push(userData);
            return await this.saveUsers(users);
        } catch (error) {
            console.error('Error adding user:', error);
            return false;
        }
    }

    async deleteUser(username) {
        try {
            const users = await this.getUsers();
            const filteredUsers = users.filter(u => u.username !== username);
            return await this.saveUsers(filteredUsers);
        } catch (error) {
            console.error('Error deleting user:', error);
            return false;
        }
    }

    async approveUser(username) {
        try {
            const users = await this.getUsers();
            const user = users.find(u => u.username === username);
            if (user) {
                user.status = 'approved';
                user.approvedBy = this.currentUser.username;
                user.approvedAt = new Date().toISOString();
                return await this.saveUsers(users);
            }
            return false;
        } catch (error) {
            console.error('Error approving user:', error);
            return false;
        }
    }

    async rejectUser(username) {
        try {
            const users = await this.getUsers();
            const filteredUsers = users.filter(u => u.username !== username);
            return await this.saveUsers(filteredUsers);
        } catch (error) {
            console.error('Error rejecting user:', error);
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

    logout() {
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
}

// Initialize auth manager
window.authManager = new AuthManager();
