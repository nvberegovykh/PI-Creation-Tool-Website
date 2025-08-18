/**
 * Users Module for Liber Apps Control Panel
 * Handles user management for admin users
 */

class UsersManager {
    constructor() {
        this.users = [];
        this.init();
    }

    init() {
        this.loadUsers();
        this.setupEventListeners();
    }

    setupEventListeners() {
        // Add user form
        const addUserForm = document.getElementById('add-user-form');
        if (addUserForm) {
            addUserForm.addEventListener('submit', (e) => {
                e.preventDefault();
                this.handleAddUser();
            });
        }

        // Search functionality
        const searchInput = document.getElementById('user-search');
        if (searchInput) {
            searchInput.addEventListener('input', (e) => {
                this.filterUsers(e.target.value.toLowerCase());
            });
        }
    }

    async loadUsers() {
        try {
            // Firebase is REQUIRED for user management
            if (window.firebaseService && window.firebaseService.isInitialized) {
                try {
                    console.log('Loading users from Firebase...');
                    this.users = await window.firebaseService.getAllUsers();
                    console.log('Firebase users loaded:', this.users.length);
                } catch (firebaseError) {
                    console.error('Firebase users loading failed:', firebaseError.message);
                    this.showError('Failed to load users from Firebase');
                    return;
                }
            } else {
                console.error('Firebase not available - user management requires Firebase');
                this.showError('User management service not available');
                return;
            }
            
            this.renderUsers();
            this.updateUserCount();
        } catch (error) {
            console.error('Error loading users:', error);
            this.showError('Failed to load users');
        }
    }

    renderUsers() {
        const usersList = document.getElementById('users-list');
        if (!usersList) return;

        if (this.users.length === 0) {
            usersList.innerHTML = `
                <div class="empty-state">
                    <i class="fas fa-users"></i>
                    <h3>No Users Found</h3>
                    <p>Add your first user to get started.</p>
                </div>
            `;
            return;
        }

        // Separate pending and approved users - FIXED to show all users
        const pendingUsers = this.users.filter(user => user.status === 'pending' || (!user.status && !user.isVerified));
        const approvedUsers = this.users.filter(user => user.status === 'approved' || user.isVerified);

        let html = '';

        // Show pending users first (admin approval needed)
        if (pendingUsers.length > 0) {
            html += `
                <div class="users-section">
                    <h3>Pending Approval (${pendingUsers.length})</h3>
                    <div class="users-grid">
                        ${pendingUsers.map(user => this.getUserCardHTML(user, true)).join('')}
                    </div>
                </div>
            `;
        }

        // Show approved users
        if (approvedUsers.length > 0) {
            html += `
                <div class="users-section">
                    <h3>Approved Users (${approvedUsers.length})</h3>
                    <div class="users-grid">
                        ${approvedUsers.map(user => this.getUserCardHTML(user, false)).join('')}
                    </div>
                </div>
            `;
        }

        usersList.innerHTML = html;

        // Add event listeners to action buttons
        this.setupUserActionListeners();
    }

    getUserCardHTML(user, isPending) {
        const createdAt = new Date(user.createdAt).toLocaleDateString();
        
        // Determine status based on both old status field and new isVerified field - FIXED
        let statusClass, statusText;
        if (user.status === 'approved' || user.isVerified) {
            statusClass = 'approved';
            statusText = 'Approved';
        } else {
            statusClass = 'pending';
            statusText = 'Pending';
        }
        
        let actionsHTML = '';
        
        if (isPending) {
            actionsHTML = `
                <button class="btn btn-success btn-sm approve-btn" data-username="${user.username}">
                    <i class="fas fa-check"></i> Approve
                </button>
                <button class="btn btn-danger btn-sm reject-btn" data-username="${user.username}">
                    <i class="fas fa-times"></i> Reject
                </button>
            `;
        } else {
            actionsHTML = `
                <button class="btn btn-danger btn-sm delete-btn" data-username="${user.username}">
                    <i class="fas fa-trash"></i> Delete
                </button>
            `;
        }

        return `
            <div class="user-card ${statusClass}">
                <div class="user-header">
                    <div class="user-avatar">
                        <i class="fas fa-user"></i>
                    </div>
                    <div class="user-info">
                        <h4>${user.username}</h4>
                        <p class="user-email">${user.email}</p>
                        <span class="user-status ${statusClass}">${statusText}</span>
                    </div>
                </div>
                <div class="user-details">
                    <div class="detail-item">
                        <span class="label">Role:</span>
                        <span class="value">${user.role}</span>
                    </div>
                    <div class="detail-item">
                        <span class="label">Created:</span>
                        <span class="value">${createdAt}</span>
                    </div>
                    ${user.approvedBy ? `
                        <div class="detail-item">
                            <span class="label">Approved by:</span>
                            <span class="value">${user.approvedBy}</span>
                        </div>
                    ` : ''}
                </div>
                <div class="user-actions">
                    ${actionsHTML}
                </div>
            </div>
        `;
    }

    setupUserActionListeners() {
        // Approve buttons
        document.querySelectorAll('.approve-btn').forEach(btn => {
            btn.addEventListener('click', async () => {
                const username = btn.dataset.username;
                await this.approveUser(username);
            });
        });

        // Reject buttons
        document.querySelectorAll('.reject-btn').forEach(btn => {
            btn.addEventListener('click', async () => {
                const username = btn.dataset.username;
                await this.rejectUser(username);
            });
        });

        // Delete buttons
        document.querySelectorAll('.delete-btn').forEach(btn => {
            btn.addEventListener('click', async () => {
                const username = btn.dataset.username;
                await this.deleteUser(username);
            });
        });
    }

    async handleAddUser() {
        const username = document.getElementById('new-username').value.trim();
        const email = document.getElementById('new-email').value.trim();
        const password = document.getElementById('new-password').value;
        const role = document.getElementById('new-role').value;

        if (!username || !email || !password) {
            this.showError('Please fill in all required fields');
            return;
        }

        if (password.length < 8) {
            this.showError('Password must be at least 8 characters long');
            return;
        }

        try {
            const hashedPassword = await window.cryptoManager.hashPassword(password);
            const newUser = {
                username,
                email,
                passwordHash: hashedPassword,
                role,
                status: 'approved', // Admin-created users are auto-approved
                createdAt: new Date().toISOString(),
                approvedBy: window.authManager.getCurrentUser().username,
                approvedAt: new Date().toISOString()
            };

            const success = await window.authManager.addUser(newUser);
            if (success) {
                this.showSuccess('User added successfully');
                document.getElementById('add-user-form').reset();
                await this.loadUsers();
            } else {
                this.showError('Failed to add user. Username or email may already exist.');
            }
        } catch (error) {
            console.error('Error adding user:', error);
            this.showError('Failed to add user');
        }
    }

    async approveUser(username) {
        if (!confirm(`Are you sure you want to approve ${username}?`)) {
            return;
        }

        try {
            const success = await window.authManager.approveUser(username);
            if (success) {
                this.showSuccess(`User ${username} approved successfully`);
                await this.loadUsers();
            } else {
                this.showError('Failed to approve user');
            }
        } catch (error) {
            console.error('Error approving user:', error);
            this.showError('Failed to approve user');
        }
    }

    async rejectUser(username) {
        if (!confirm(`Are you sure you want to reject ${username}? This action cannot be undone.`)) {
            return;
        }

        try {
            const success = await window.authManager.rejectUser(username);
            if (success) {
                this.showSuccess(`User ${username} rejected successfully`);
                await this.loadUsers();
            } else {
                this.showError('Failed to reject user');
            }
        } catch (error) {
            console.error('Error rejecting user:', error);
            this.showError('Failed to reject user');
        }
    }

    async deleteUser(username) {
        if (username === window.authManager.getCurrentUser().username) {
            this.showError('You cannot delete your own account');
            return;
        }

        if (!confirm(`Are you sure you want to delete ${username}? This action cannot be undone.`)) {
            return;
        }

        try {
            const success = await window.authManager.deleteUser(username);
            if (success) {
                this.showSuccess(`User ${username} deleted successfully`);
                await this.loadUsers();
            } else {
                this.showError('Failed to delete user');
            }
        } catch (error) {
            console.error('Error deleting user:', error);
            this.showError('Failed to delete user');
        }
    }

    filterUsers(searchTerm) {
        const userCards = document.querySelectorAll('.user-card');
        userCards.forEach(card => {
            const username = card.querySelector('h4').textContent.toLowerCase();
            const email = card.querySelector('.user-email').textContent.toLowerCase();
            
            if (username.includes(searchTerm) || email.includes(searchTerm)) {
                card.style.display = 'block';
            } else {
                card.style.display = 'none';
            }
        });
    }

    updateUserCount() {
        const countElement = document.getElementById('users-count');
        if (countElement) {
            const totalUsers = this.users.length;
            const pendingUsers = this.users.filter(u => u.status === 'pending' || (!u.status && !u.isVerified)).length;
            countElement.textContent = `${totalUsers} total (${pendingUsers} pending)`;
        }
    }

    showSuccess(message) {
        if (window.dashboardManager) {
            window.dashboardManager.showSuccess(message);
        }
    }

    showError(message) {
        if (window.dashboardManager) {
            window.dashboardManager.showError(message);
        }
    }

    getUsers() {
        return this.users;
    }
}

// Create global instance
window.usersManager = new UsersManager();
