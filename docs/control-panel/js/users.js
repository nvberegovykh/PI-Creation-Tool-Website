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
        // Delay loading users until an authenticated admin is present to avoid UI alerts on refresh
        const tryLoad = async () => {
            try {
                if (window.firebaseService && window.firebaseService.isInitialized && window.authManager) {
                    const cu = window.firebaseService.auth.currentUser;
                    if (cu) {
                        // Check admin role
                        try {
                            const docRef = window.firebase.doc(window.firebaseService.db, 'users', cu.uid);
                            const snap = await window.firebase.getDoc(docRef);
                            if (snap.exists() && (snap.data().role === 'admin')) {
                                await this.loadUsers();
                                return;
                            }
                        } catch (_) {}
                    }
                }
            } catch (_) {}
            // Retry shortly; avoid spamming errors
            setTimeout(tryLoad, 500);
        };
        tryLoad();
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
            // Wait for Firebase to be fully initialized
            let attempts = 0;
            const maxAttempts = 50; // 5 seconds
            
            while ((!window.firebaseService || !window.firebaseService.isInitialized) && attempts < maxAttempts) {
                await new Promise(resolve => setTimeout(resolve, 100));
                attempts++;
            }
            
            if (window.firebaseService && window.firebaseService.isInitialized) {
                try {
                    console.log('Loading users from Firebase...');
                    // Get users with pagination and stats
                    const result = await window.firebaseService.getUsersWithPagination(50);
                    this.users = result.users;
                    console.log('Firebase users loaded:', this.users.length);
                    
                    // Update user statistics
                    this.updateUserStats();
                } catch (firebaseError) {
                    console.warn('Firebase users loading failed (suppressed on UI):', firebaseError.message);
                    return;
                }
            } else {
                console.warn('Firebase not available yet - delaying user list load.');
                this.users = []; // Ensure users array is empty if Firebase is not available
                return;
            }
                
            this.renderUsers();
            this.updateUserCount();
        } catch (error) {
            console.error('Error loading users:', error);
            this.showError('Failed to load users');
        }
    }

    /**
     * Update user statistics
     */
    async updateUserStats() {
        try {
            if (window.firebaseService && window.firebaseService.isInitialized) {
                const stats = await window.firebaseService.getUserStats();
                const statsElement = document.getElementById('users-count');
                if (statsElement) {
                    statsElement.textContent = `${stats.total} total (${stats.pending} pending)`;
                }
            }
        } catch (error) {
            console.error('Error updating user stats:', error);
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

        // Separate pending and approved users
        // Pending: not verified and not explicitly approved
        const pendingUsers = this.users.filter(user => (!user.isVerified) && (user.status !== 'approved'));
        // Approved: verified or explicitly approved
        const approvedUsers = this.users.filter(user => (user.isVerified) || (user.status === 'approved'));

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
                <button class="btn btn-success btn-sm approve-btn" data-uid="${user.id || user.uid}">
                    <i class="fas fa-check"></i> Approve
                </button>
                <button class="btn btn-danger btn-sm reject-btn" data-uid="${user.id || user.uid}">
                    <i class="fas fa-times"></i> Reject
                </button>
            `;
        } else {
            actionsHTML = `
                <button class="btn btn-danger btn-sm delete-btn" data-uid="${user.id || user.uid}">
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
                        <span class="value">
                            <select class="role-select" data-uid="${user.id || user.uid}">
                                <option value="user" ${user.role === 'user' ? 'selected' : ''}>User</option>
                                <option value="admin" ${user.role === 'admin' ? 'selected' : ''}>Admin</option>
                            </select>
                            <button class="btn btn-secondary btn-sm save-role-btn" data-uid="${user.id || user.uid}">Save</button>
                        </span>
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
                const uid = btn.dataset.uid;
                await this.approveUser(uid);
            });
        });

        // Reject buttons
        document.querySelectorAll('.reject-btn').forEach(btn => {
            btn.addEventListener('click', async () => {
                const uid = btn.dataset.uid;
                await this.rejectUser(uid);
            });
        });

        // Delete buttons
        document.querySelectorAll('.delete-btn').forEach(btn => {
            btn.addEventListener('click', async () => {
                const uid = btn.dataset.uid;
                await this.deleteUser(uid);
            });
        });

        // Save role buttons
        document.querySelectorAll('.save-role-btn').forEach(btn => {
            btn.addEventListener('click', async () => {
                const uid = btn.dataset.uid;
                const select = document.querySelector(`.role-select[data-uid="${uid}"]`);
                if (!select) return;
                await this.updateUserRole(uid, select.value);
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

    async approveUser(uid) {
        if (!confirm(`Are you sure you want to approve this user?`)) {
            return;
        }

        try {
            if (window.firebaseService && window.firebaseService.isFirebaseAvailable()) {
                await window.firebaseService.approveUser(uid);
                this.showSuccess('User approved successfully');
                await this.loadUsers();
            } else {
                this.showError('Firebase service not available');
            }
        } catch (error) {
            console.error('Error approving user:', error);
            this.showError('Failed to approve user');
        }
    }

    async updateUserRole(uid, role) {
        try {
            if (window.firebaseService && window.firebaseService.isFirebaseAvailable()) {
                const docRef = window.firebase.doc(window.firebaseService.db, 'users', uid);
                await window.firebase.updateDoc(docRef, {
                    role,
                    updatedAt: new Date().toISOString()
                });
                this.showSuccess('Role updated');
                await this.loadUsers();
            } else {
                this.showError('Firebase service not available');
            }
        } catch (error) {
            console.error('Error updating user role:', error);
            this.showError('Failed to update role');
        }
    }

    async rejectUser(uid) {
        if (!confirm(`Are you sure you want to reject this user? This action cannot be undone.`)) {
            return;
        }

        try {
            if (window.firebaseService && window.firebaseService.isFirebaseAvailable()) {
                await window.firebaseService.rejectUser(uid);
                this.showSuccess('User rejected successfully');
                await this.loadUsers();
            } else {
                this.showError('Firebase service not available');
            }
        } catch (error) {
            console.error('Error rejecting user:', error);
            this.showError('Failed to reject user');
        }
    }

    async deleteUser(uid) {
        const currentUser = window.authManager.getCurrentUser();
        if (currentUser && (currentUser.id === uid || currentUser.uid === uid)) {
            this.showError('You cannot delete your own account');
            return;
        }

        if (!confirm(`Are you sure you want to delete this user? This action cannot be undone.`)) {
            return;
        }

        try {
            if (window.firebaseService && window.firebaseService.isFirebaseAvailable()) {
                await window.firebaseService.deleteUser(uid);
                this.showSuccess('User deleted successfully');
                await this.loadUsers();
            } else {
                this.showError('Firebase service not available');
            }
        } catch (error) {
            console.error('Error deleting user:', error);
            this.showError('Failed to delete user');
        }
    }

    async filterUsers(searchTerm) {
        if (!searchTerm || searchTerm.length < 2) {
            // If search term is too short, show all users
            await this.loadUsers();
            return;
        }

        try {
            if (window.firebaseService && window.firebaseService.isFirebaseAvailable()) {
                const searchResults = await window.firebaseService.searchUsers(searchTerm);
                this.users = searchResults;
                this.renderUsers();
                this.updateUserCount();
            } else {
                // Fallback to client-side filtering
                this.filterUsersClientSide(searchTerm);
            }
        } catch (error) {
            console.error('Error searching users:', error);
            // Fallback to client-side filtering
            this.filterUsersClientSide(searchTerm);
        }
    }

    filterUsersClientSide(searchTerm) {
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
