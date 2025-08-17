/**
 * Dashboard Module for Liber Apps Control Panel
 * Handles navigation, overview, and dashboard functionality
 */

class DashboardManager {
    constructor() {
        this.currentSection = 'overview';
        this.init();
    }

    /**
     * Initialize dashboard
     */
    init() {
        this.setupEventListeners();
        this.loadOverview();
        this.updateNavigation();
    }

    /**
     * Setup dashboard event listeners
     */
    setupEventListeners() {
        // Desktop navigation buttons
        const navBtns = document.querySelectorAll('.nav-btn');
        navBtns.forEach(btn => {
            btn.addEventListener('click', () => this.switchSection(btn.dataset.section));
        });

        // Mobile navigation buttons
        const mobileNavBtns = document.querySelectorAll('.mobile-nav-btn');
        mobileNavBtns.forEach(btn => {
            if (btn.id === 'mobile-wall-e-btn') {
                // WALL-E button - toggle the WALL-E widget
                btn.addEventListener('click', () => this.toggleWallEWidget());
            } else {
                // Regular navigation buttons
                btn.addEventListener('click', () => this.switchSection(btn.dataset.section));
            }
        });

        // Logout button
        document.getElementById('logout-btn').addEventListener('click', () => {
            authManager.logout();
        });

        // Settings form handlers
        this.setupSettingsHandlers();
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
        document.getElementById(`${section}-section`).classList.add('active');

        this.currentSection = section;

        // Load section-specific content
        switch (section) {
            case 'overview':
                this.loadOverview();
                break;
            case 'apps':
                if (window.appsManager) {
                    window.appsManager.loadApps();
                }
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
        try {
            // Update apps count
            const appsCount = await this.getAppsCount();
            document.getElementById('apps-count').textContent = appsCount;

            // Update users count
            const usersCount = await this.getUsersCount();
            document.getElementById('users-count').textContent = usersCount;

            // Update last login
            const currentUser = authManager.getCurrentUser();
            if (currentUser && currentUser.lastLogin) {
                const lastLogin = new Date(currentUser.lastLogin);
                document.getElementById('last-login').textContent = this.formatDate(lastLogin);
            } else {
                document.getElementById('last-login').textContent = 'Never';
            }

            // Update system status
            document.getElementById('system-status').textContent = 'Online';
            document.getElementById('system-status').className = 'status-online';

        } catch (error) {
            console.error('Error loading overview:', error);
            this.showError('Failed to load overview data');
        }
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
            const users = await authManager.getUsers();
            return users.length + 1; // +1 for admin
        } catch (error) {
            console.error('Error getting users count:', error);
            return 0;
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
        if (window.wallE && typeof window.wallE.toggleChat === 'function') {
            window.wallE.toggleChat();
            
            // Update mobile WALL-E button state
            const mobileWallEBtn = document.getElementById('mobile-wall-e-btn');
            if (mobileWallEBtn) {
                mobileWallEBtn.classList.toggle('active');
            }
        } else {
            console.warn('WALL-E widget not available');
        }
    }

    /**
     * Initialize keyboard shortcuts
     */
    initKeyboardShortcuts() {
        this.setupKeyboardShortcuts();
    }
}

// Create global instance
window.dashboardManager = new DashboardManager();

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = DashboardManager;
}
