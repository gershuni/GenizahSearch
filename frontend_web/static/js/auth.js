/**
 * Authentication Module
 * Handles login, registration, and session management
 */

class AuthManager {
    constructor() {
        this.currentUser = null;
        this.init();
    }

    async init() {
        if (api.isAuthenticated()) {
            await this.loadCurrentUser();
        }
        this.updateUI();
        this.setupEventListeners();
    }

    async loadCurrentUser() {
        try {
            this.currentUser = await api.getCurrentUser();
            return this.currentUser;
        } catch (error) {
            console.error('Failed to load user:', error);
            api.logout();
            this.currentUser = null;
            return null;
        }
    }

    updateUI() {
        const loginBtn = document.getElementById('login-btn');
        const registerBtn = document.getElementById('register-btn');
        const userMenu = document.getElementById('user-menu');
        const usernameDisplay = document.getElementById('username-display');
        const navReview = document.getElementById('nav-review');

        if (this.currentUser) {
            // Show user menu, hide login/register
            loginBtn?.classList.add('d-none');
            registerBtn?.classList.add('d-none');
            userMenu?.classList.remove('d-none');

            if (usernameDisplay) {
                usernameDisplay.textContent = this.currentUser.username;
            }

            // Show review link for reviewers/editors/admins
            if (this.canReview()) {
                navReview?.classList.remove('d-none');
                this.updatePendingCount();
            }
        } else {
            // Show login/register, hide user menu
            loginBtn?.classList.remove('d-none');
            registerBtn?.classList.remove('d-none');
            userMenu?.classList.add('d-none');
        }
    }

    setupEventListeners() {
        // Logout button
        const logoutBtn = document.getElementById('logout-btn');
        if (logoutBtn) {
            logoutBtn.addEventListener('click', (e) => {
                e.preventDefault();
                this.logout();
            });
        }
    }

    async login(email, password) {
        try {
            await api.login(email, password);
            await this.loadCurrentUser();
            this.updateUI();
            showAlert('success', 'התחברת בהצלחה!');
            return true;
        } catch (error) {
            showAlert('danger', error.message);
            return false;
        }
    }

    async register(userData) {
        try {
            await api.register(userData);
            showAlert('success', 'ההרשמה הצליחה! כעת ניתן להתחבר.');
            return true;
        } catch (error) {
            showAlert('danger', error.message);
            return false;
        }
    }

    logout() {
        api.logout();
        this.currentUser = null;
        this.updateUI();
        showAlert('info', 'התנתקת מהמערכת');
        // Redirect to home if on protected page
        if (window.location.pathname.includes('/my-') ||
            window.location.pathname.includes('/profile')) {
            window.location.href = '/';
        }
    }

    isAuthenticated() {
        return !!this.currentUser;
    }

    canSubmitCorrections() {
        if (!this.currentUser) return false;
        return ['contributor', 'reviewer', 'editor', 'admin'].includes(this.currentUser.role);
    }

    canReview() {
        if (!this.currentUser) return false;
        return ['reviewer', 'editor', 'admin'].includes(this.currentUser.role);
    }

    canEditAny() {
        if (!this.currentUser) return false;
        return ['editor', 'admin'].includes(this.currentUser.role);
    }

    isAdmin() {
        return this.currentUser?.role === 'admin';
    }

    async updatePendingCount() {
        if (!this.canReview()) return;

        try {
            const stats = await api.getCorrectionStats();
            const pendingBadge = document.querySelector('.pending-count');
            if (pendingBadge && stats.pending > 0) {
                pendingBadge.textContent = stats.pending;
                pendingBadge.classList.remove('d-none');
            }
        } catch (error) {
            console.error('Failed to load pending count:', error);
        }
    }
}

// Create global auth manager
const auth = new AuthManager();

// Helper function to show alerts
function showAlert(type, message, duration = 5000) {
    const container = document.getElementById('alerts-container');
    if (!container) return;

    const alertId = 'alert-' + Date.now();
    const alertHtml = `
        <div id="${alertId}" class="alert alert-${type} alert-dismissible fade show" role="alert">
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        </div>
    `;

    container.insertAdjacentHTML('beforeend', alertHtml);

    // Auto-dismiss after duration
    if (duration > 0) {
        setTimeout(() => {
            const alert = document.getElementById(alertId);
            if (alert) {
                alert.remove();
            }
        }, duration);
    }
}

// Require authentication for certain pages
function requireAuth() {
    if (!auth.isAuthenticated()) {
        showAlert('warning', 'יש להתחבר כדי לגשת לדף זה');
        setTimeout(() => {
            window.location.href = '/login?redirect=' + encodeURIComponent(window.location.pathname);
        }, 1500);
        return false;
    }
    return true;
}

// Require specific role
function requireRole(role) {
    if (!requireAuth()) return false;

    const roleHierarchy = {
        'guest': 0,
        'contributor': 1,
        'reviewer': 2,
        'editor': 3,
        'admin': 4
    };

    const userLevel = roleHierarchy[auth.currentUser?.role] || 0;
    const requiredLevel = roleHierarchy[role] || 0;

    if (userLevel < requiredLevel) {
        showAlert('danger', 'אין לך הרשאה לגשת לדף זה');
        return false;
    }

    return true;
}
