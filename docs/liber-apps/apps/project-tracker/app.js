(function () {
  'use strict';

  const BASE_FOLDERS = ['docs', 'images', 'video'];
  const STATUS_COLORS = {
    submitted: '#2196F3',
    in_progress: '#FF9800',
    review: '#9C27B0',
    completed: '#4CAF50',
    on_hold: '#607D8B'
  };

  const state = { projects: [], selectedProject: null, library: [], members: [] };

  function byId(id) {
    return document.getElementById(id);
  }

  function getFirebaseService() {
    try {
      for (const w of [window.parent, window.top].filter(Boolean)) {
        if (w !== window && w.firebaseService && w.firebaseService.isInitialized)
          return w.firebaseService;
      }
    } catch (_) {}
    return window.firebaseService;
  }

  function getChatUrl(connId) {
    try {
      const loc = window.location;
      const path = loc.pathname.replace(/project-tracker\/[^?]*/, 'secure-chat/index.html');
      return loc.origin + path + '?connId=' + encodeURIComponent(connId);
    } catch (_) {}
    return '#';
  }

  function escapeHtml(s) {
    const div = document.createElement('div');
    div.textContent = s;
    return div.innerHTML;
  }

  function showMain() {
    byId('tracker-detail').classList.add('hidden');
    byId('tracker-main').classList.remove('hidden');
    byId('tracker-loading').classList.add('hidden');
  }

  function showDetail() {
    byId('tracker-main').classList.add('hidden');
    byId('tracker-detail').classList.remove('hidden');
    byId('tracker-loading').classList.add('hidden');
  }

  function showLoading() {
    byId('tracker-main').classList.add('hidden');
    byId('tracker-detail').classList.add('hidden');
    byId('tracker-loading').classList.remove('hidden');
  }

  function renderStatusBadge(status, color) {
    const c = color || STATUS_COLORS[status] || '#6b7280';
    return `<span class="status-badge" style="background:${c}33;border:1px solid ${c}"><span style="background:${c};width:8px;height:8px;border-radius:50%;display:inline-block;flex-shrink:0"></span> ${escapeHtml(String(status || 'unknown').replace(/_/g, ' '))}</span>`;
  }

  function renderProjects() {
    const grid = byId('projects-grid');
    const empty = byId('projects-empty');
    if (!grid) return;

    if (!state.projects.length) {
      grid.innerHTML = '';
      if (empty) empty.classList.remove('hidden');
      return;
    }

    if (empty) empty.classList.add('hidden');
    grid.innerHTML = state.projects
      .map(
        (p) => `
      <div class="project-card" data-project-id="${escapeHtml(p.id)}">
        <h3>${escapeHtml(p.name || 'Untitled')}</h3>
        ${renderStatusBadge(p.status, p.statusColor)}
        ${p.description ? `<div class="project-desc">${escapeHtml(p.description.slice(0, 120))}${p.description.length > 120 ? '…' : ''}</div>` : ''}
      </div>`
      )
      .join('');

    grid.querySelectorAll('.project-card').forEach((el) => {
      el.addEventListener('click', () => {
        const id = el.getAttribute('data-project-id');
        openProject(id);
      });
    });
  }

  async function openProject(projectId) {
    const project = state.projects.find((p) => p.id === projectId);
    if (!project) return;

    state.selectedProject = project;
    showDetail();

    byId('detail-title').textContent = project.name || 'Project';
    byId('detail-status').outerHTML = renderStatusBadge(project.status, project.statusColor);
    byId('detail-updated').textContent = project.updatedAt
      ? 'Updated ' + new Date(project.updatedAt).toLocaleDateString()
      : '';
    byId('detail-description').textContent = project.description || 'No description.';

    const chatUrl = project.chatConnId ? getChatUrl(project.chatConnId) : '#';
    const chatLink = byId('detail-chat-link');
    chatLink.href = chatUrl;
    chatLink.style.display = project.chatConnId ? '' : 'none';

    const fs = getFirebaseService();
    const isOwner = fs?.auth?.currentUser?.uid === project.ownerId;
    const membersSection = byId('members-section');
    if (membersSection) {
      if (isOwner) {
        membersSection.classList.remove('hidden');
        await loadMembers(projectId);
        renderMembers();
        bindMemberActions(projectId);
      } else {
        membersSection.classList.add('hidden');
      }
    }

    await loadLibrary(projectId);
    renderLibrary(state.selectedProject ? byId('library-tabs').querySelector('.lib-tab.active')?.dataset?.folder || 'record_in' : 'record_in');
  }

  function notify(msg, type) {
    try {
      if (window.parent?.dashboardManager?.showNotification) {
        window.parent.dashboardManager.showNotification(msg, type || 'success');
      } else {
        alert(msg);
      }
    } catch (_) {
      alert(msg);
    }
  }

  async function loadMembers(projectId) {
    const fs = getFirebaseService();
    if (!fs?.callFunction) {
      state.members = [];
      return;
    }
    try {
      const res = await fs.callFunction('getProjectMembers', { projectId });
      state.members = res?.members || [];
    } catch (e) {
      console.warn('[Project Tracker] loadMembers failed', e);
      state.members = [];
    }
  }

  function renderMembers() {
    const list = byId('members-list');
    if (!list) return;
    if (!state.members.length) {
      list.innerHTML = '<p class="members-empty-hint">No members yet. Add by email above.</p>';
      return;
    }
    list.innerHTML = state.members
      .map(
        (m) => `
      <div class="member-row" data-uid="${escapeHtml(m.id)}">
        <span class="${m.isVerified ? 'verified-badge' : 'unverified-badge'}">${m.isVerified ? 'Verified' : 'Unverified'}</span>
        <span class="member-name">${escapeHtml(m.username || m.email || m.id)}</span>
        ${m.isOwner ? '<span class="owner-tag">(owner)</span>' : ''}
        ${m.isOwner ? '' : `<button type="button" class="member-remove" data-uid="${escapeHtml(m.id)}" title="Remove">Remove</button>`}
      </div>
    `
      )
      .join('');
  }

  function bindMemberActions(projectId) {
    const addBtn = byId('add-member-btn');
    const emailInput = byId('member-email');
    if (addBtn && emailInput) {
      addBtn.onclick = async () => {
        const email = (emailInput.value || '').trim();
        if (!email) return;
        addBtn.disabled = true;
        try {
          const fs = getFirebaseService();
          const res = await fs.callFunction('addProjectMember', { projectId, email });
          if (res?.ok) {
            await loadMembers(projectId);
            renderMembers();
            emailInput.value = '';
            notify(res.added ? 'Member added.' : 'User already a member.');
          }
        } catch (e) {
          notify(e?.message || 'Failed to add member', 'error');
        } finally {
          addBtn.disabled = false;
        }
      };
    }
    byId('members-list')?.querySelectorAll('.member-remove').forEach((btn) => {
      btn.onclick = async () => {
        const uid = btn.getAttribute('data-uid');
        if (!uid) return;
        if (!confirm('Remove this member from the project?')) return;
        btn.disabled = true;
        try {
          const fs = getFirebaseService();
          const res = await fs.callFunction('removeProjectMember', { projectId, userId: uid });
          if (res?.ok) {
            await loadMembers(projectId);
            renderMembers();
            bindMemberActions(projectId);
            notify('Member removed.');
          }
        } catch (e) {
          notify(e?.message || 'Failed to remove member', 'error');
        } finally {
          btn.disabled = false;
        }
      };
    });
  }

  async function loadLibrary(projectId) {
    const fs = getFirebaseService();
    if (!fs || !fs.db) {
      state.library = [];
      return;
    }

    try {
      const libRef = firebase.collection(fs.db, 'projects', projectId, 'library');
      const q = firebase.query(libRef);
      const snap = await firebase.getDocs(q);
      state.library = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
    } catch (e) {
      console.warn('[Project Tracker] loadLibrary failed', e);
      state.library = [];
    }
  }

  function renderLibrary(folderPrefix) {
    const content = byId('library-content');
    const empty = byId('library-empty');
    if (!content) return;

    const files = state.library.filter((f) => f.type === 'file' && f.folderPath && f.folderPath.startsWith(folderPrefix));
    const bySub = {};
    for (const f of files) {
      const sub = f.folderPath.replace(folderPrefix + '/', '').split('/')[0] || 'docs';
      if (!bySub[sub]) bySub[sub] = [];
      bySub[sub].push(f);
    }

    const subs = BASE_FOLDERS.filter((s) => bySub[s] && bySub[s].length);
    if (subs.length === 0 && Object.keys(bySub).length === 0) {
      content.innerHTML = '';
      if (empty) empty.classList.remove('hidden');
      return;
    }

    if (empty) empty.classList.add('hidden');
    let html = '';
    for (const sub of BASE_FOLDERS) {
      const list = bySub[sub] || [];
      const other = Object.entries(bySub).filter(([k]) => !BASE_FOLDERS.includes(k));
      const extras = sub === 'docs' ? other.flatMap(([, v]) => v) : [];
      const all = [...list, ...extras];
      if (all.length === 0) continue;
      html += `<div class="lib-subfolder"><div class="lib-folder-label">${escapeHtml(sub)}</div>`;
      for (const f of all) {
        html += `<div class="lib-file-row"><i class="fas fa-file"></i><a href="#" data-storage-path="${escapeHtml(f.storagePath || '')}" target="_blank" rel="noopener">${escapeHtml(f.name || 'file')}</a></div>`;
      }
      html += '</div>';
    }
    content.innerHTML = html || '';

    content.querySelectorAll('a[data-storage-path]').forEach((a) => {
      const path = a.getAttribute('data-storage-path');
      if (!path) return;
      a.addEventListener('click', async (e) => {
        e.preventDefault();
        const fs = getFirebaseService();
        if (!fs || !fs.storage) return;
        try {
          const r = firebase.ref(fs.storage, path);
          const url = await firebase.getDownloadURL(r);
          window.open(url, '_blank', 'noopener');
        } catch (err) {
          console.warn('[Project Tracker] getDownloadURL failed', err);
        }
      });
    });
  }

  async function loadProjects() {
    const fs = getFirebaseService();
    if (!fs || !fs.isInitialized) {
      showLoading();
      setTimeout(loadProjects, 300);
      return;
    }

    const user = fs.auth?.currentUser;
    if (!user) {
      byId('tracker-loading').innerHTML = '<p>Please log in to view your projects.</p>';
      return;
    }

    try {
      const qOwner = firebase.query(
        firebase.collection(fs.db, 'projects'),
        firebase.where('ownerId', '==', user.uid),
        firebase.orderBy('updatedAt', 'desc'),
        firebase.limit(50)
      );
      const qMember = firebase.query(
        firebase.collection(fs.db, 'projects'),
        firebase.where('memberIds', 'array-contains', user.uid),
        firebase.orderBy('updatedAt', 'desc'),
        firebase.limit(50)
      );
      const [snapOwner, snapMember] = await Promise.all([firebase.getDocs(qOwner), firebase.getDocs(qMember)]);
      const byId = new Map();
      [...snapOwner.docs, ...snapMember.docs].forEach((d) => { byId.set(d.id, { id: d.id, ...d.data() }); });
      state.projects = Array.from(byId.values()).sort((a, b) => new Date(b.updatedAt || 0) - new Date(a.updatedAt || 0));
      showMain();
      renderProjects();
    } catch (e) {
      console.error('[Project Tracker] loadProjects failed', e);
      byId('tracker-loading').innerHTML = '<p>Failed to load projects.</p>';
    }
  }

  function init() {
    const backBtn = byId('back-btn');
    if (backBtn) {
      backBtn.addEventListener('click', () => {
        if (state.selectedProject) {
          state.selectedProject = null;
          showMain();
        } else {
          try {
            if (window.parent && window.parent !== window) {
              window.parent.postMessage({ type: 'liber:close-app-shell' }, '*');
            }
          } catch (_) {}
        }
      });
    }

    byId('library-tabs')?.addEventListener('click', (e) => {
      const tab = e.target.closest('.lib-tab');
      if (!tab) return;
      byId('library-tabs').querySelectorAll('.lib-tab').forEach((t) => t.classList.remove('active'));
      tab.classList.add('active');
      const folder = tab.dataset.folder;
      renderLibrary(folder);
    });

    const tryLoad = () => {
      if (window.firebase && window.firebaseService && window.firebaseService.isInitialized) {
        loadProjects();
      } else {
        setTimeout(tryLoad, 200);
      }
    };
    tryLoad();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
