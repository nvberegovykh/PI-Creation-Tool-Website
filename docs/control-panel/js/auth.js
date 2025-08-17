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
    }

    switchTab(tabName) {
        // Update active tab button
        document.querySelectorAll('.tab-btn').forEach(btn => {
            btn.classList.remove('active');
        });
        document.querySelector(`[data-tab="${tabName}"]`).classList.add('active');

        // Update active form
        document.querySelectorAll('.auth-form').forEach(form => {
            form.classList.remove('active');
        });
        document.getElementById(`${tabName}Form`).classList.add('active');
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

    async handleLogin() {
        const username = document.getElementById('loginUsername').value.trim();
        const password = document.getElementById('loginPassword').value;

        if (!username || !password) {
            this.showMessage('Please fill in all fields', 'error');
            return;
        }

        const success = await this.login(username, password);
        if (success) {
            // Store user password temporarily for app authentication
            localStorage.setItem('liber_user_password', password);
            this.showMessage('Login successful!', 'success');
            setTimeout(() => {
                this.showDashboard();
            }, 1000);
        } else {
            this.showMessage('Invalid credentials', 'error');
        }
    }

    async handleRegister() {
        const username = document.getElementById('registerUsername').value.trim();
        const email = document.getElementById('registerEmail').value.trim();
        const password = document.getElementById('registerPassword').value;
        const confirmPassword = document.getElementById('confirmPassword').value;

        if (!username || !email || !password || !confirmPassword) {
            this.showMessage('Please fill in all fields', 'error');
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

        const success = await this.register(username, email, password);
        if (success) {
            this.showMessage('Registration request submitted. Please wait for admin approval.', 'success');
            // Clear form
            document.getElementById('registerForm').reset();
        } else {
            this.showMessage('Registration failed. Username or email may already exist.', 'error');
        }
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
                let widget = document.querySelector('.chatgpt-widget');
                if (!widget && window.wallE.createChatInterface) {
                    console.log('Forcing widget creation...');
                    window.wallE.createChatInterface();
                    widget = document.querySelector('.chatgpt-widget');
                }
                
                if (widget) {
                    console.log('WALL-E widget found, showing it...');
                    
                    // Use CSS class instead of inline styles for better compatibility
                    widget.classList.add('mobile-activated');
                    
                    // Expand the widget
                    if (window.wallE && typeof window.wallE.expandChat === 'function') {
                        console.log('Expanding WALL-E widget...');
                        window.wallE.expandChat();
                    }
                    
                    // Hide the toggle button temporarily
                    newToggleBtn.style.display = 'none';
                    
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
