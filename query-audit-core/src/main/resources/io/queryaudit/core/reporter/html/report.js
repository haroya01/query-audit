// Keyboard shortcut: 'e' to expand all, 'c' to collapse all
document.addEventListener('keydown', function(e) {
    if (e.ctrlKey || e.metaKey || e.altKey) return;
    if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
    var details = document.querySelectorAll('details.test-card, details.method-card, details.method');
    if (e.key === 'e') {
        details.forEach(function(d) { d.open = true; });
    } else if (e.key === 'c') {
        details.forEach(function(d) { d.open = false; });
    }
});

document.addEventListener('DOMContentLoaded', function() {
    var className = document.body.dataset['class'] || '';
    var storeKey = className ? 'qg-checked:' + className : 'qg-checked';
    var hashKey = className ? 'qg-report-hash:' + className : 'qg-report-hash';

    var reportHash = document.querySelector('meta[name="qg-report-hash"]');
    if (reportHash) {
        var hash = reportHash.content;
        var prevHash = localStorage.getItem(hashKey);
        if (prevHash && prevHash !== hash) {
            localStorage.removeItem(storeKey);
            if (className) localStorage.removeItem('qg-class-reviewed:' + className);
        }
        localStorage.setItem(hashKey, hash);
    }

    var checks = document.querySelectorAll('.issue-check');
    var store = JSON.parse(localStorage.getItem(storeKey) || '{}');

    function updateClassStatus() {
        if (!className) return;
        var methods = document.querySelectorAll('.method');
        var hasAnyIssue = false;
        var allMethodsReviewed = true;
        methods.forEach(function(m) {
            var issues = m.querySelectorAll('.issues .issue');
            if (issues.length === 0) return;
            hasAnyIssue = true;
            if (!m.classList.contains('all-reviewed')) allMethodsReviewed = false;
        });
        if (!hasAnyIssue) return;
        var key = 'qg-class-reviewed:' + className;
        if (allMethodsReviewed) {
            localStorage.setItem(key, 'true');
            var bar = document.querySelector('.summary-bar');
            if (bar) {
                bar.querySelectorAll('.stat.error, .stat.warning').forEach(function(s) {
                    s.style.textDecoration = 'line-through';
                    s.style.opacity = '0.5';
                });
                var reviewed = bar.querySelector('.class-reviewed-status');
                if (reviewed) reviewed.style.display = '';
            }
        } else {
            localStorage.removeItem(key);
            var bar = document.querySelector('.summary-bar');
            if (bar) {
                bar.querySelectorAll('.stat.error, .stat.warning').forEach(function(s) {
                    s.style.textDecoration = '';
                    s.style.opacity = '';
                });
                var reviewed = bar.querySelector('.class-reviewed-status');
                if (reviewed) reviewed.style.display = 'none';
            }
        }
    }

    function updateMethodCard(issueEl) {
        var method = issueEl.closest('.method');
        if (!method) return;
        var allIssues = method.querySelectorAll('.issues .issue');
        if (allIssues.length === 0) return;
        var allResolved = true;
        allIssues.forEach(function(iss) {
            if (!iss.classList.contains('resolved')) allResolved = false;
        });
        if (allResolved) {
            method.classList.add('all-reviewed');
            var summary = method.querySelector(':scope > summary');
            if (summary && !summary.querySelector('.review-badge')) {
                var badge = document.createElement('span');
                badge.className = 'review-badge';
                badge.textContent = 'Reviewed';
                summary.appendChild(badge);
            }
        } else {
            method.classList.remove('all-reviewed');
            var existing = method.querySelector('.review-badge');
            if (existing) existing.remove();
        }
        updateClassStatus();
    }

    checks.forEach(function(cb) {
        var key = cb.dataset.key;
        if (store[key]) {
            cb.checked = true;
            cb.closest('.issue').classList.add('resolved');
        }
        cb.addEventListener('change', function() {
            if (cb.checked) {
                store[key] = true;
                cb.closest('.issue').classList.add('resolved');
            } else {
                delete store[key];
                cb.closest('.issue').classList.remove('resolved');
            }
            localStorage.setItem(storeKey, JSON.stringify(store));
            updateMethodCard(cb.closest('.issue'));
        });
    });

    document.querySelectorAll('.method').forEach(function(m) {
        var firstIssue = m.querySelector('.issues .issue');
        if (firstIssue) updateMethodCard(firstIssue);
    });
});

// Deep-link handler: open the target test-card details and scroll to it
function openAndScroll(event, href) {
    // For same-page anchors, handle directly
    var hashIdx = href.indexOf('#');
    if (hashIdx < 0) return; // no anchor, let browser handle
    var anchor = href.substring(hashIdx + 1);
    var el = document.getElementById(anchor);
    if (el) {
        event.preventDefault();
        // Open the details element if it's collapsed
        if (el.tagName === 'DETAILS' && !el.open) {
            el.open = true;
        }
        el.scrollIntoView({ behavior: 'smooth', block: 'center' });
        // Flash highlight
        el.style.transition = 'box-shadow 0.3s';
        el.style.boxShadow = '0 0 0 3px var(--color-info)';
        setTimeout(function() { el.style.boxShadow = ''; }, 2000);
    }
    // For cross-page links (ClassName.html#test-xxx), let browser navigate
}

// On page load, check if URL has a hash and open the target
document.addEventListener('DOMContentLoaded', function() {
    if (window.location.hash) {
        var el = document.getElementById(window.location.hash.substring(1));
        if (el && el.tagName === 'DETAILS') {
            el.open = true;
            el.scrollIntoView({ behavior: 'smooth', block: 'center' });
            el.style.transition = 'box-shadow 0.3s';
            el.style.boxShadow = '0 0 0 3px var(--color-info)';
            setTimeout(function() { el.style.boxShadow = ''; }, 2000);
        }
    }
});

document.addEventListener('DOMContentLoaded', function() {
    if (document.body.dataset['class']) return;
    var rows = document.querySelectorAll('.classes-table tbody tr[data-class]');
    var currentClasses = new Set();
    rows.forEach(function(row) {
        var cls = row.dataset['class'];
        var expectedHash = row.dataset.hash;
        currentClasses.add(cls);
        var storedHash = localStorage.getItem('qg-report-hash:' + cls);
        if (storedHash && expectedHash && storedHash !== expectedHash) {
            localStorage.removeItem('qg-class-reviewed:' + cls);
            localStorage.removeItem('qg-checked:' + cls);
            localStorage.removeItem('qg-report-hash:' + cls);
        }
        if (localStorage.getItem('qg-class-reviewed:' + cls) === 'true') {
            row.classList.remove('row-fail');
            row.classList.add('row-pass');
            var dot = row.querySelector('.status-dot');
            if (dot) {
                dot.classList.remove('error-dot', 'warning-dot');
                dot.classList.add('ok-dot');
            }
            var badge = row.querySelector('.badge');
            if (badge) {
                badge.classList.remove('badge-error', 'badge-warning');
                badge.classList.add('badge-acknowledged');
            }
        }
    });
    Object.keys(localStorage).forEach(function(k) {
        if (k.startsWith('qg-class-reviewed:') || k.startsWith('qg-checked:') || k.startsWith('qg-report-hash:')) {
            var cls = k.substring(k.indexOf(':') + 1);
            if (!currentClasses.has(cls)) localStorage.removeItem(k);
        }
    });
});
