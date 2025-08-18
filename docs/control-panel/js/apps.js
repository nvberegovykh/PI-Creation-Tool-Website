/**
 * Apps Module for Liber Apps Control Panel
 * Handles app discovery, launching, and management
 */

class AppsManager {
    constructor() {
        this.apps = [];
        this.filteredApps = [];
        this.currentCategory = 'all';
        this.searchTerm = '';
        this.init();
    }

    /**
     * Initialize apps manager
     */
    init() {
        this.setupEventListeners();
        this.loadApps();
    }

    /**
     * Setup event listeners
     */
    setupEventListeners() {
        // Auto-refresh if enabled
        this.setupAutoRefresh();
    }

    /**
     * Setup search functionality
     */
    setupSearchFunctionality() {
        // Remove existing event listeners to prevent duplicates
        const searchInput = document.getElementById('app-search');
        if (searchInput) {
            // Clone the element to remove all event listeners
            const newSearchInput = searchInput.cloneNode(true);
            searchInput.parentNode.replaceChild(newSearchInput, searchInput);
            
            // Add new event listener
            newSearchInput.addEventListener('input', (e) => {
                this.searchTerm = e.target.value.toLowerCase();
                this.filterApps();
                this.renderApps();
            });

            // Add keydown event for better UX
            newSearchInput.addEventListener('keydown', (e) => {
                if (e.key === 'Escape') {
                    this.clearSearch();
                }
            });

            // Add clear button functionality
            const searchContainer = newSearchInput.closest('.search-container');
            if (searchContainer) {
                // Remove existing clear button if any
                const existingClearBtn = searchContainer.querySelector('.search-clear');
                if (existingClearBtn) {
                    existingClearBtn.remove();
                }

                // Add clear button
                const clearBtn = document.createElement('button');
                clearBtn.className = 'search-clear';
                clearBtn.innerHTML = '<i class="fas fa-times"></i>';
                
                clearBtn.addEventListener('click', () => {
                    this.clearSearch();
                });

                searchContainer.appendChild(clearBtn);

                // Show/hide clear button based on input value
                newSearchInput.addEventListener('input', () => {
                    if (newSearchInput.value) {
                        clearBtn.classList.add('visible');
                    } else {
                        clearBtn.classList.remove('visible');
                    }
                });
            }
        }
    }

    /**
     * Clear search
     */
    clearSearch() {
        const searchInput = document.getElementById('app-search');
        if (searchInput) {
            searchInput.value = '';
        }
        this.searchTerm = '';
        this.filterApps();
        this.renderApps();
    }

    /**
     * Setup auto-refresh functionality
     */
    setupAutoRefresh() {
        const settings = dashboardManager.getSettings();
        if (settings.autoRefreshApps) {
            // Refresh apps every 5 minutes
            setInterval(() => {
                this.loadApps();
            }, 5 * 60 * 1000);
        }
    }

    /**
     * Load apps from the apps directory
     */
    async loadApps() {
        try {
            // Show loading state
            this.showLoadingState();

            // Setup search functionality when apps section is loaded
            this.setupSearchFunctionality();

            // In a real implementation, this would scan the apps directory
            // For now, we'll use sample apps
            this.apps = await this.getSampleApps();
            
            this.filterApps();
            this.renderApps();
            this.updateAppsCount();

        } catch (error) {
            console.error('Error loading apps:', error);
            this.showError('Failed to load apps');
        }
    }

    /**
     * Get sample apps (replace with actual directory scanning)
     */
    async getSampleApps() {
        // Only include apps that actually exist in the apps folder
        const availableApps = [
            {
                id: 'calculator',
                name: 'Calculator',
                description: 'A calculator application for mathematical operations.',
                version: '1.0.0',
                category: 'utilities',
                icon: 'fas fa-calculator',
                status: 'online',
                path: 'apps/calculator/index.html',
                author: 'Liber Apps',
                lastUpdated: '2025-08-16',
                logo: null
            },
            {
                id: 'invoice-generator',
                name: 'Invoice Generator',
                description: 'Create, manage, and download invoices.',
                version: '1.0.0',
                category: 'business',
                icon: 'fas fa-file-invoice-dollar',
                status: 'online',
                path: 'apps/invoices-app/invoices.html',
                author: 'Liber Apps',
                lastUpdated: '2025-08-16',
                logo: null
            }
            ,
            {
                id: 'secure-chat',
                name: 'Connections',
                description: 'End-to-end encrypted chat with file sharing.',
                version: '1.0.0',
                category: 'communication',
                icon: 'fas fa-comments',
                status: 'online',
                path: 'apps/secure-chat/index.html',
                author: 'Liber Apps',
                lastUpdated: '2025-08-18',
                logo: null
            }
        ];

        // Filter out apps that don't have actual HTML files
        const validApps = [];
        for (const app of availableApps) {
            try {
                const response = await fetch(app.path);
                if (response.ok) {
                    validApps.push(app);
                }
            } catch (error) {
                console.warn(`App ${app.name} not found at ${app.path}`);
            }
        }

        return validApps;
    }

    /**
     * Filter apps based on search term and category
     */
    filterApps() {
        this.filteredApps = this.apps.filter(app => {
            const matchesSearch = this.searchTerm === '' || 
                                app.name.toLowerCase().includes(this.searchTerm) ||
                                app.description.toLowerCase().includes(this.searchTerm) ||
                                app.category.toLowerCase().includes(this.searchTerm);
            const matchesCategory = this.currentCategory === 'all' || app.category === this.currentCategory;
            
            return matchesSearch && matchesCategory;
        });
    }

    /**
     * Render apps in the grid
     */
    renderApps() {
        const appsGrid = document.getElementById('apps-grid');
        if (!appsGrid) return;

        if (this.filteredApps.length === 0) {
            appsGrid.innerHTML = this.getEmptyStateHTML();
            return;
        }

        appsGrid.innerHTML = this.filteredApps.map(app => this.getAppCardHTML(app)).join('');
        
        // Add click handlers to app cards
        this.setupAppCardEventListeners();
    }

    /**
     * Get app card HTML
     */
    getAppCardHTML(app) {
        const settings = dashboardManager.getSettings();
        const showDescriptions = settings.showAppDescriptions;
        
        // Get the current pathname to determine the base path
        const currentPath = window.location.pathname;
        const basePath = currentPath.includes('/control-panel') ? '/control-panel' : '';
        const appUrl = `${window.location.origin}${basePath}/${app.path}`;
        
        return `
            <div class="app-card" data-app-id="${app.id}">
                <div class="app-header">
                    <div class="app-icon">
                        ${app.logo ? `<img src="${app.logo}" alt="${app.name}" class="app-logo" onerror="this.style.display='none'; this.nextElementSibling.style.display='block';">` : ''}
                        <i class="${app.icon}" ${app.logo ? 'style="display: none;"' : ''}></i>
                    </div>
                    <div class="app-info">
                        <h3>${app.name}</h3>
                        <div class="app-version">v${app.version}</div>
                    </div>
                </div>
                ${showDescriptions ? `<div class="app-description">${app.description}</div>` : ''}
                <div class="app-meta">
                    <div class="app-status ${app.status}">
                        <span class="status-dot"></span>
                        ${app.status.charAt(0).toUpperCase() + app.status.slice(1)}
                    </div>
                    <div class="app-actions">
                        <button class="app-btn launch-btn" data-app-id="${app.id}">
                            <i class="fas fa-rocket"></i> Launch
                        </button>
                        <button class="app-btn secondary info-btn" data-app-id="${app.id}">
                            <i class="fas fa-info-circle"></i> Info
                        </button>
                        <!-- Mobile-friendly direct link (hidden on desktop) -->
                        <a href="${appUrl}" class="app-btn mobile-launch-link" target="_blank" style="display: none;">
                            <i class="fas fa-external-link-alt"></i> Open
                        </a>
                    </div>
                </div>
            </div>
        `;
    }

    /**
     * Get empty state HTML
     */
    getEmptyStateHTML() {
        return `
            <div class="apps-empty">
                <i class="fas fa-search"></i>
                <h3>No apps found</h3>
                <p>Try adjusting your search terms or category filter to find what you're looking for.</p>
            </div>
        `;
    }

    /**
     * Setup app card event listeners
     */
    setupAppCardEventListeners() {
        // Launch buttons
        const launchBtns = document.querySelectorAll('.launch-btn');
        launchBtns.forEach(btn => {
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                const appId = btn.dataset.appId;
                this.launchApp(appId);
            });
        });

        // Info buttons
        const infoBtns = document.querySelectorAll('.info-btn');
        infoBtns.forEach(btn => {
            btn.addEventListener('click', (e) => {
                e.stopPropagation();
                const appId = btn.dataset.appId;
                this.showAppInfo(appId);
            });
        });

        // Card clicks
        const appCards = document.querySelectorAll('.app-card');
        appCards.forEach(card => {
            card.addEventListener('click', () => {
                const appId = card.dataset.appId;
                this.launchApp(appId);
            });
        });
    }

    /**
     * Launch an app
     */
    launchApp(appId) {
        const app = this.apps.find(a => a.id === appId);
        if (!app) {
            this.showError('App not found');
            return;
        }

        if (app.status === 'offline') {
            this.showError('This app is currently offline');
            return;
        }

        if (app.status === 'maintenance') {
            this.showWarning('This app is under maintenance. Some features may be unavailable.');
        }

        try {
            // Get the current pathname to determine the base path
            const currentPath = window.location.pathname;
            const basePath = currentPath.includes('/control-panel') ? '/control-panel' : '';
            const appUrl = `${window.location.origin}${basePath}/${app.path}`;
            
            // Show success message
            this.showSuccess(`Launching ${app.name}...`);
            
            // Open app immediately (no delay for mobile compatibility)
            const newWindow = window.open(appUrl, '_blank');
            
            // Check if popup was blocked (especially on mobile)
            if (!newWindow || newWindow.closed || typeof newWindow.closed === 'undefined') {
                // Popup was blocked, try alternative approach for mobile
                if (this.isMobileDevice()) {
                    // For mobile, try to navigate in the same window
                    this.showWarning('Popup blocked. Opening app in current window...');
                    setTimeout(() => {
                        window.location.href = appUrl;
                    }, 500);
                } else {
                    this.showError('Popup blocked. Please allow popups for this site.');
                }
            }

        } catch (error) {
            console.error('Error launching app:', error);
            this.showError('Failed to launch app');
        }
    }

    /**
     * Check if device is mobile
     */
    isMobileDevice() {
        return /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent) ||
               window.innerWidth <= 768;
    }

    /**
     * Show app information
     */
    showAppInfo(appId) {
        const app = this.apps.find(a => a.id === appId);
        if (!app) {
            this.showError('App not found');
            return;
        }

        const modal = this.createModal();
        modal.innerHTML = `
            <div class="modal-header">
                <h3>${app.name}</h3>
                <button class="modal-close">&times;</button>
            </div>
            <div class="modal-body">
                <div class="app-info-details">
                    <div class="app-info-header">
                        <div class="app-icon large">
                            <i class="${app.icon}"></i>
                        </div>
                        <div class="app-info-text">
                            <h4>${app.name}</h4>
                            <p class="app-version">Version ${app.version}</p>
                            <p class="app-author">by ${app.author}</p>
                        </div>
                    </div>
                    <div class="app-description-full">
                        <h5>Description</h5>
                        <p>${app.description}</p>
                    </div>
                    <div class="app-details">
                        <div class="detail-item">
                            <span class="detail-label">Category:</span>
                            <span class="detail-value">${app.category}</span>
                        </div>
                        <div class="detail-item">
                            <span class="detail-label">Status:</span>
                            <span class="detail-value status-${app.status}">${app.status}</span>
                        </div>
                        <div class="detail-item">
                            <span class="detail-label">Last Updated:</span>
                            <span class="detail-value">${new Date(app.lastUpdated).toLocaleDateString()}</span>
                        </div>
                        <div class="detail-item">
                            <span class="detail-label">Path:</span>
                            <span class="detail-value">${app.path}</span>
                        </div>
                    </div>
                </div>
                <div class="modal-actions">
                    <button class="btn secondary" onclick="this.closest('.modal-overlay').remove()">Close</button>
                    <button class="btn" onclick="appsManager.launchApp('${app.id}'); this.closest('.modal-overlay').remove()">
                        <i class="fas fa-rocket"></i> Launch App
                    </button>
                </div>
            </div>
        `;

        // Setup modal close
        const closeBtn = modal.querySelector('.modal-close');
        closeBtn.addEventListener('click', () => modal.remove());

        // Close on overlay click
        modal.addEventListener('click', (e) => {
            if (e.target === modal) modal.remove();
        });
    }

    /**
     * Create modal element
     */
    createModal() {
        const modal = document.createElement('div');
        modal.className = 'modal-overlay';
        modal.style.cssText = `
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0, 0, 0, 0.8);
            display: flex;
            justify-content: center;
            align-items: center;
            z-index: 2000;
            padding: 20px;
        `;

        const modalContent = document.createElement('div');
        modalContent.className = 'modal';
        modalContent.style.cssText = `
            background: var(--secondary-bg);
            border: 1px solid var(--border-color);
            border-radius: var(--border-radius);
            max-width: 600px;
            width: 100%;
            max-height: 80vh;
            overflow-y: auto;
            box-shadow: var(--shadow-hover);
            animation: slideUp 0.3s ease-out;
        `;

        modal.appendChild(modalContent);
        document.body.appendChild(modal);

        return modalContent;
    }

    /**
     * Show loading state
     */
    showLoadingState() {
        const appsGrid = document.getElementById('apps-grid');
        if (!appsGrid) return;

        appsGrid.innerHTML = Array(6).fill(0).map(() => `
            <div class="app-card loading">
                <div class="app-header">
                    <div class="app-icon"></div>
                    <div class="app-info">
                        <h3></h3>
                        <div class="app-version"></div>
                    </div>
                </div>
                <div class="app-description"></div>
                <div class="app-meta">
                    <div class="app-status"></div>
                    <div class="app-actions"></div>
                </div>
            </div>
        `).join('');
    }

    /**
     * Update apps count
     */
    updateAppsCount() {
        const countElement = document.getElementById('apps-count');
        if (countElement) {
            countElement.textContent = this.apps.length;
        }
    }

    /**
     * Get all apps
     */
    getApps() {
        return Promise.resolve(this.apps);
    }

    /**
     * Get app by ID
     */
    getAppById(appId) {
        return this.apps.find(app => app.id === appId);
    }

    /**
     * Show success message
     */
    showSuccess(message) {
        if (window.dashboardManager) {
            window.dashboardManager.showSuccess(message);
        }
    }

    /**
     * Show error message
     */
    showError(message) {
        if (window.dashboardManager) {
            window.dashboardManager.showError(message);
        }
    }

    /**
     * Show warning message
     */
    showWarning(message) {
        if (window.dashboardManager) {
            window.dashboardManager.showNotification(message, 'warning');
        }
    }
}

// Create global instance
window.appsManager = new AppsManager();

// Export for module systems
if (typeof module !== 'undefined' && module.exports) {
    module.exports = AppsManager;
}
