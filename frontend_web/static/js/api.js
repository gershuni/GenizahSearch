/**
 * Genizah Corrections API Client
 * Handles all communication with the backend API
 */

const API_BASE = '/api/v1';

class GenizahAPI {
    constructor() {
        this.token = localStorage.getItem('access_token');
        this.refreshToken = localStorage.getItem('refresh_token');
    }

    // Helper method for API calls
    async request(endpoint, options = {}) {
        const url = `${API_BASE}${endpoint}`;
        const headers = {
            'Content-Type': 'application/json',
            ...options.headers
        };

        if (this.token) {
            headers['Authorization'] = `Bearer ${this.token}`;
        }

        try {
            const response = await fetch(url, {
                ...options,
                headers
            });

            // Handle token refresh if needed
            if (response.status === 401 && this.refreshToken) {
                const refreshed = await this.refreshAccessToken();
                if (refreshed) {
                    headers['Authorization'] = `Bearer ${this.token}`;
                    return fetch(url, { ...options, headers });
                }
            }

            return response;
        } catch (error) {
            console.error('API request failed:', error);
            throw error;
        }
    }

    async get(endpoint) {
        const response = await this.request(endpoint);
        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.detail || 'Request failed');
        }
        return response.json();
    }

    async post(endpoint, data) {
        const response = await this.request(endpoint, {
            method: 'POST',
            body: JSON.stringify(data)
        });
        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.detail || 'Request failed');
        }
        return response.json();
    }

    async put(endpoint, data) {
        const response = await this.request(endpoint, {
            method: 'PUT',
            body: JSON.stringify(data)
        });
        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.detail || 'Request failed');
        }
        return response.json();
    }

    async delete(endpoint) {
        const response = await this.request(endpoint, {
            method: 'DELETE'
        });
        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.detail || 'Request failed');
        }
        return response.json();
    }

    // Authentication
    async login(email, password) {
        const response = await fetch(`${API_BASE}/auth/login`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ email, password })
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.detail || 'Login failed');
        }

        const data = await response.json();
        this.setTokens(data.access_token, data.refresh_token);
        return data;
    }

    async register(userData) {
        const response = await fetch(`${API_BASE}/auth/register`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(userData)
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.detail || 'Registration failed');
        }

        return response.json();
    }

    async refreshAccessToken() {
        try {
            const response = await fetch(`${API_BASE}/auth/refresh`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ refresh_token: this.refreshToken })
            });

            if (response.ok) {
                const data = await response.json();
                this.setTokens(data.access_token, data.refresh_token);
                return true;
            }
        } catch (e) {
            console.error('Token refresh failed:', e);
        }

        this.logout();
        return false;
    }

    setTokens(accessToken, refreshToken) {
        this.token = accessToken;
        this.refreshToken = refreshToken;
        localStorage.setItem('access_token', accessToken);
        if (refreshToken) {
            localStorage.setItem('refresh_token', refreshToken);
        }
    }

    logout() {
        this.token = null;
        this.refreshToken = null;
        localStorage.removeItem('access_token');
        localStorage.removeItem('refresh_token');
    }

    isAuthenticated() {
        return !!this.token;
    }

    async getCurrentUser() {
        return this.get('/auth/me');
    }

    async changePassword(currentPassword, newPassword, confirmPassword) {
        return this.post('/auth/change-password', {
            current_password: currentPassword,
            new_password: newPassword,
            confirm_password: confirmPassword
        });
    }

    // Users
    async getUserProfile(username) {
        return this.get(`/users/profile/${username}`);
    }

    async getUserStats(username) {
        return this.get(`/users/profile/${username}/stats`);
    }

    async updateProfile(data) {
        return this.put('/users/me', data);
    }

    async getLeaderboard(limit = 10) {
        return this.get(`/users/leaderboard/top?limit=${limit}`);
    }

    // Corrections
    async createCorrection(data) {
        return this.post('/corrections/', data);
    }

    async getCorrection(id) {
        return this.get(`/corrections/${id}`);
    }

    async updateCorrection(id, data) {
        return this.put(`/corrections/${id}`, data);
    }

    async deleteCorrection(id) {
        return this.delete(`/corrections/${id}`);
    }

    async submitCorrection(id, notes = null) {
        return this.post(`/corrections/${id}/submit`, { notes });
    }

    async reviewCorrection(id, action, notes = null, rejectionReason = null) {
        return this.post(`/corrections/${id}/review`, {
            action,
            review_notes: notes,
            rejection_reason: rejectionReason
        });
    }

    async voteOnCorrection(id, voteValue) {
        return this.post(`/corrections/${id}/vote`, { vote_value: voteValue });
    }

    async listCorrections(params = {}) {
        const queryString = new URLSearchParams(params).toString();
        return this.get(`/corrections/?${queryString}`);
    }

    async getMyCorrections(status = null, page = 1, pageSize = 20) {
        const params = { page, page_size: pageSize };
        if (status) params.status = status;
        const queryString = new URLSearchParams(params).toString();
        return this.get(`/corrections/my?${queryString}`);
    }

    async getPendingCorrections(page = 1, pageSize = 20) {
        return this.get(`/corrections/pending?page=${page}&page_size=${pageSize}`);
    }

    async getDocumentCorrections(documentId, includeDrafts = false) {
        return this.get(`/corrections/document/${documentId}?include_drafts=${includeDrafts}`);
    }

    async getCorrectionStats() {
        return this.get('/corrections/stats');
    }

    // Comments
    async createComment(data) {
        return this.post('/comments/', data);
    }

    async getComment(id) {
        return this.get(`/comments/${id}`);
    }

    async updateComment(id, content) {
        return this.put(`/comments/${id}`, { content });
    }

    async deleteComment(id) {
        return this.delete(`/comments/${id}`);
    }

    async getDocumentComments(documentId, page = 1, pageSize = 50) {
        return this.get(`/comments/document/${documentId}?page=${page}&page_size=${pageSize}`);
    }

    async getCorrectionComments(correctionId, page = 1, pageSize = 50) {
        return this.get(`/comments/correction/${correctionId}?page=${page}&page_size=${pageSize}`);
    }

    async getCommentThread(id) {
        return this.get(`/comments/${id}/thread`);
    }

    async resolveComment(id) {
        return this.post(`/comments/${id}/resolve`, {});
    }

    async reactToComment(id, reactionType) {
        return this.post(`/comments/${id}/react`, { reaction_type: reactionType });
    }

    // Documents
    async getDocumentStats(documentId) {
        return this.get(`/documents/${documentId}/stats`);
    }

    async getDocumentMetadata(documentId) {
        return this.get(`/documents/${documentId}/metadata`);
    }

    async updateDocumentMetadata(documentId, data) {
        return this.put(`/documents/${documentId}/metadata`, data);
    }

    async getCorrectedText(documentId, originalText) {
        return this.post(`/documents/${documentId}/corrected-text`, { original_text: originalText });
    }

    async recordDocumentView(documentId) {
        return this.post(`/documents/${documentId}/view`, {});
    }

    async getFeaturedDocuments(limit = 10) {
        return this.get(`/documents/featured?limit=${limit}`);
    }

    async getMostCorrectedDocuments(limit = 10) {
        return this.get(`/documents/most-corrected?limit=${limit}`);
    }
}

// Create global API instance
const api = new GenizahAPI();
