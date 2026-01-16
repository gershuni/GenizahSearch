/**
 * Main JavaScript - UI Components and Utilities
 */

// Status badge helper
function getStatusBadge(status) {
    const statusMap = {
        'draft': { class: 'badge-draft', text: 'טיוטה' },
        'pending': { class: 'badge-pending', text: 'ממתין לבדיקה' },
        'under_review': { class: 'badge-under_review', text: 'בבדיקה' },
        'approved': { class: 'badge-approved', text: 'אושר' },
        'rejected': { class: 'badge-rejected', text: 'נדחה' },
        'needs_revision': { class: 'badge-needs_revision', text: 'דורש תיקון' },
        'superseded': { class: 'badge-draft', text: 'הוחלף' },
        'merged': { class: 'badge-approved', text: 'מוזג' }
    };
    const info = statusMap[status] || { class: 'badge-secondary', text: status };
    return `<span class="badge ${info.class}">${info.text}</span>`;
}

// Correction type helper
function getCorrectionType(type) {
    const typeMap = {
        'text_correction': 'תיקון טקסט',
        'text_addition': 'הוספת טקסט',
        'text_deletion': 'מחיקת טקסט',
        'metadata': 'תיקון מטאדאטה',
        'translation': 'תרגום',
        'reading_suggestion': 'הצעת קריאה',
        'paleographic': 'הערה פליאוגרפית',
        'uncertain': 'קריאה לא ברורה'
    };
    return typeMap[type] || type;
}

// Format date
function formatDate(dateStr) {
    if (!dateStr) return '';
    const date = new Date(dateStr);
    return date.toLocaleDateString('he-IL', {
        year: 'numeric',
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
    });
}

// Create correction card
function createCorrectionCard(correction, showActions = true) {
    const voteScore = correction.vote_score || (correction.upvotes - correction.downvotes);
    const voteClass = voteScore > 0 ? 'positive' : (voteScore < 0 ? 'negative' : '');
    const userVoteUp = correction.user_vote === 1 ? 'voted-up' : '';
    const userVoteDown = correction.user_vote === -1 ? 'voted-down' : '';

    const actionsHtml = showActions ? `
        <div class="d-flex align-items-center gap-2">
            <div class="vote-buttons d-flex align-items-center">
                <button class="vote-btn ${userVoteUp}" onclick="voteCorrection(${correction.id}, 1)" title="הצבעה חיובית">
                    <i class="bi bi-chevron-up"></i>
                </button>
                <span class="vote-score ${voteClass}">${voteScore}</span>
                <button class="vote-btn ${userVoteDown}" onclick="voteCorrection(${correction.id}, -1)" title="הצבעה שלילית">
                    <i class="bi bi-chevron-down"></i>
                </button>
            </div>
            <a href="/correction/${correction.id}" class="btn btn-sm btn-outline-primary">
                <i class="bi bi-eye"></i> פרטים
            </a>
        </div>
    ` : '';

    const authorHtml = correction.author ? `
        <a href="/profile/${correction.author.username}" class="text-decoration-none">
            ${correction.author.full_name || correction.author.username}
        </a>
    ` : 'אנונימי';

    const confidencePercent = Math.round((correction.confidence_score || 0) * 100);
    const confidenceClass = confidencePercent >= 70 ? 'high' : (confidencePercent >= 40 ? 'medium' : 'low');

    return `
        <div class="card correction-card status-${correction.status} mb-3 fade-in">
            <div class="card-body">
                <div class="d-flex justify-content-between align-items-start mb-2">
                    <div>
                        ${getStatusBadge(correction.status)}
                        <span class="badge bg-secondary ms-1">${getCorrectionType(correction.correction_type)}</span>
                    </div>
                    <small class="text-muted">${formatDate(correction.created_at)}</small>
                </div>

                <div class="diff-container mb-3">
                    <div class="mb-2">
                        <strong>מקור:</strong>
                        <span class="diff-original hebrew-text">${escapeHtml(correction.original_text)}</span>
                    </div>
                    <div>
                        <strong>תיקון:</strong>
                        <span class="diff-corrected hebrew-text">${escapeHtml(correction.corrected_text)}</span>
                    </div>
                </div>

                ${correction.notes ? `
                    <div class="mb-2">
                        <small class="text-muted"><i class="bi bi-chat-text"></i> ${escapeHtml(correction.notes)}</small>
                    </div>
                ` : ''}

                ${correction.source_reference ? `
                    <div class="mb-2">
                        <small><i class="bi bi-book"></i> ${escapeHtml(correction.source_reference)}</small>
                    </div>
                ` : ''}

                <div class="d-flex justify-content-between align-items-center">
                    <div>
                        <small class="text-muted">
                            <i class="bi bi-person"></i> ${authorHtml}
                            ${correction.line_number ? `| שורה ${correction.line_number}` : ''}
                        </small>
                        <div class="confidence-meter mt-1" style="width: 100px" title="רמת ביטחון: ${confidencePercent}%">
                            <div class="bar ${confidenceClass}" style="width: ${confidencePercent}%"></div>
                        </div>
                    </div>
                    ${actionsHtml}
                </div>
            </div>
        </div>
    `;
}

// Create comment card
function createCommentCard(comment, allowActions = true) {
    const reactionSummary = comment.reactions_summary || {};
    const userReactions = comment.user_reactions || [];

    const reactionsHtml = `
        <div class="d-flex gap-1 flex-wrap">
            ${['like', 'helpful', 'insightful', 'thanks'].map(type => {
                const count = reactionSummary[type] || 0;
                const isActive = userReactions.includes(type);
                const icons = {
                    'like': 'bi-hand-thumbs-up',
                    'helpful': 'bi-lightbulb',
                    'insightful': 'bi-star',
                    'thanks': 'bi-heart'
                };
                return `
                    <button class="reaction-btn ${isActive ? 'active' : ''}"
                            onclick="reactToComment(${comment.id}, '${type}')"
                            ${!allowActions ? 'disabled' : ''}>
                        <i class="bi ${icons[type]}"></i>
                        ${count > 0 ? `<span class="ms-1">${count}</span>` : ''}
                    </button>
                `;
            }).join('')}
        </div>
    `;

    const authorHtml = comment.author ? `
        <a href="/profile/${comment.author.username}" class="fw-bold text-decoration-none">
            ${comment.author.full_name || comment.author.username}
        </a>
    ` : 'אנונימי';

    return `
        <div class="card comment-card ${comment.is_pinned ? 'pinned' : ''} ${comment.is_resolved ? 'resolved' : ''} mb-2">
            <div class="card-body py-2">
                <div class="d-flex justify-content-between align-items-start">
                    <div>
                        ${authorHtml}
                        <small class="text-muted ms-2">${formatDate(comment.created_at)}</small>
                        ${comment.is_edited ? '<small class="text-muted">(נערך)</small>' : ''}
                        ${comment.is_pinned ? '<span class="badge bg-warning ms-1">מוצמד</span>' : ''}
                        ${comment.is_resolved ? '<span class="badge bg-success ms-1">נפתר</span>' : ''}
                    </div>
                    <div class="dropdown" ${!allowActions ? 'style="display:none"' : ''}>
                        <button class="btn btn-sm btn-link" data-bs-toggle="dropdown">
                            <i class="bi bi-three-dots"></i>
                        </button>
                        <ul class="dropdown-menu dropdown-menu-end">
                            <li><a class="dropdown-item" href="#" onclick="replyToComment(${comment.id})">
                                <i class="bi bi-reply"></i> הגב
                            </a></li>
                            ${comment.author_id === auth.currentUser?.id ? `
                                <li><a class="dropdown-item" href="#" onclick="editComment(${comment.id})">
                                    <i class="bi bi-pencil"></i> ערוך
                                </a></li>
                                <li><a class="dropdown-item text-danger" href="#" onclick="deleteComment(${comment.id})">
                                    <i class="bi bi-trash"></i> מחק
                                </a></li>
                            ` : ''}
                        </ul>
                    </div>
                </div>
                <div class="mt-2 hebrew-text">${escapeHtml(comment.content)}</div>
                <div class="mt-2">
                    ${reactionsHtml}
                </div>
            </div>
        </div>
    `;
}

// HTML escape helper
function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Vote on correction
async function voteCorrection(correctionId, voteValue) {
    if (!auth.isAuthenticated()) {
        showAlert('warning', 'יש להתחבר כדי להצביע');
        return;
    }

    try {
        const result = await api.voteOnCorrection(correctionId, voteValue);
        // Refresh the correction card
        const card = document.querySelector(`[data-correction-id="${correctionId}"]`);
        if (card) {
            card.outerHTML = createCorrectionCard(result);
        }
    } catch (error) {
        showAlert('danger', error.message);
    }
}

// React to comment
async function reactToComment(commentId, reactionType) {
    if (!auth.isAuthenticated()) {
        showAlert('warning', 'יש להתחבר כדי להגיב');
        return;
    }

    try {
        await api.reactToComment(commentId, reactionType);
        // Reload comments if needed
        if (typeof loadComments === 'function') {
            loadComments();
        }
    } catch (error) {
        showAlert('danger', error.message);
    }
}

// Loading indicator
function showLoading() {
    const overlay = document.createElement('div');
    overlay.className = 'loading-overlay';
    overlay.id = 'loading-overlay';
    overlay.innerHTML = `
        <div class="spinner-border text-primary" role="status">
            <span class="visually-hidden">טוען...</span>
        </div>
    `;
    document.body.appendChild(overlay);
}

function hideLoading() {
    const overlay = document.getElementById('loading-overlay');
    if (overlay) {
        overlay.remove();
    }
}

// Pagination component
function createPagination(currentPage, totalPages, onPageChange) {
    if (totalPages <= 1) return '';

    let html = '<nav><ul class="pagination justify-content-center">';

    // Previous button
    html += `
        <li class="page-item ${currentPage === 1 ? 'disabled' : ''}">
            <a class="page-link" href="#" onclick="${onPageChange}(${currentPage - 1}); return false;">
                <i class="bi bi-chevron-right"></i>
            </a>
        </li>
    `;

    // Page numbers
    const startPage = Math.max(1, currentPage - 2);
    const endPage = Math.min(totalPages, currentPage + 2);

    if (startPage > 1) {
        html += `<li class="page-item"><a class="page-link" href="#" onclick="${onPageChange}(1); return false;">1</a></li>`;
        if (startPage > 2) {
            html += '<li class="page-item disabled"><span class="page-link">...</span></li>';
        }
    }

    for (let i = startPage; i <= endPage; i++) {
        html += `
            <li class="page-item ${i === currentPage ? 'active' : ''}">
                <a class="page-link" href="#" onclick="${onPageChange}(${i}); return false;">${i}</a>
            </li>
        `;
    }

    if (endPage < totalPages) {
        if (endPage < totalPages - 1) {
            html += '<li class="page-item disabled"><span class="page-link">...</span></li>';
        }
        html += `<li class="page-item"><a class="page-link" href="#" onclick="${onPageChange}(${totalPages}); return false;">${totalPages}</a></li>`;
    }

    // Next button
    html += `
        <li class="page-item ${currentPage === totalPages ? 'disabled' : ''}">
            <a class="page-link" href="#" onclick="${onPageChange}(${currentPage + 1}); return false;">
                <i class="bi bi-chevron-left"></i>
            </a>
        </li>
    `;

    html += '</ul></nav>';
    return html;
}

// Initialize tooltips
document.addEventListener('DOMContentLoaded', function() {
    // Initialize Bootstrap tooltips
    const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]');
    tooltipTriggerList.forEach(el => new bootstrap.Tooltip(el));
});
