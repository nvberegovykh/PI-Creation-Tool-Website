(function () {
  'use strict';

  const STATUS_COLORS = { submitted: '#2196F3', in_progress: '#FF9800', review: '#9C27B0', completed: '#4CAF50', on_hold: '#607D8B' };
  const BASE_FOLDERS = ['record_in/docs', 'record_in/images', 'record_in/video', 'record_out/docs', 'record_out/images', 'record_out/video'];

  const state = { projects: [], users: [], selectedProjectId: null };

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

  function fb() {
    const fs = getFirebaseService();
    return (fs && fs.firebase) ? fs.firebase : (typeof firebase !== 'undefined' ? firebase : window.firebase);
  }

  function escapeHtml(s) {
    const div = document.createElement('div');
    div.textContent = s;
    return div.innerHTML;
  }

  function notify(msg, type) {
    if (window.parent && window.parent.dashboardManager) {
      window.parent.dashboardManager.showNotification(msg, type || 'success');
    } else {
      alert(msg);
    }
  }

  async function loadUsers() {
    const fs = getFirebaseService();
    if (!fs || !fs.db) return;
    try {
      const q = fb().query(fb().collection(fs.db, 'users'), fb().limit(200));
      const snap = await fb().getDocs(q);
      state.users = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
    } catch (e) {
      console.warn('[Project Manager] loadUsers failed', e);
    }
  }

  function renderOwnerSelect(selectedId) {
    const input = byId('project-owner-search');
    const hidden = byId('project-owner');
    if (!input || !hidden) return;
    hidden.value = selectedId || '';
    const u = getUserById(selectedId);
    input.value = u ? (u.username || u.email || u.id) : '';
  }

  function filterUsers(search, excludeIds) {
    const q = (search || '').toLowerCase().trim();
    const exclude = new Set(excludeIds || []);
    let list = state.users.filter((u) => !exclude.has(u.id));
    if (q) {
      list = list.filter((u) =>
        (u.email || '').toLowerCase().includes(q) ||
        (u.username || '').toLowerCase().includes(q) ||
        (u.id || '').toLowerCase().includes(q)
      );
    }
    return list.slice(0, 30);
  }

  function showOwnerDropdown() {
    const input = byId('project-owner-search');
    const dd = byId('project-owner-dropdown');
    if (!input || !dd) return;
    const q = input.value.trim();
    const list = filterUsers(q);
    if (list.length === 0 && !state.users.length) {
      dd.innerHTML = '<div class="user-dropdown-item user-dropdown-empty">No users loaded yet.</div>';
      dd.classList.remove('hidden');
    } else {
      dd.innerHTML = list.map((u) => `<div class="user-dropdown-item" data-uid="${escapeHtml(u.id)}">${escapeHtml(u.username || u.email || u.id)}${u.email ? ` <span class="user-email">(${escapeHtml(u.email)})</span>` : ''}</div>`).join('');
      dd.classList.toggle('hidden', list.length === 0);
    }
    dd.querySelectorAll('.user-dropdown-item').forEach((el) => {
      el.addEventListener('click', () => {
        const uid = el.getAttribute('data-uid');
        const u = getUserById(uid);
        if (u) {
          byId('project-owner').value = uid;
          input.value = u.username || u.email || uid;
          dd.classList.add('hidden');
          const ownerId = uid;
          const others = getProjectMemberIds().filter((id) => id !== ownerId);
          renderProjectMembers(ownerId ? [ownerId, ...others] : others, ownerId || undefined);
          showAddUserDropdown();
        }
      });
    });
  }

  function showAddUserDropdown() {
    const input = byId('project-add-user-search');
    const dd = byId('project-add-user-dropdown');
    if (!input || !dd) return;
    const exclude = getProjectMemberIds();
    const list = filterUsers(input.value.trim(), exclude);
    if (list.length === 0 && !state.users.length) {
      dd.innerHTML = '<div class="user-dropdown-item user-dropdown-empty">No users loaded yet.</div>';
      dd.classList.remove('hidden');
    } else if (list.length === 0) {
      dd.innerHTML = '<div class="user-dropdown-item user-dropdown-empty">No matching users or all already added.</div>';
      dd.classList.remove('hidden');
    } else {
      dd.innerHTML = list.map((u) => `<div class="user-dropdown-item" data-uid="${escapeHtml(u.id)}">${escapeHtml(u.username || u.email || u.id)}${u.email ? ` <span class="user-email">(${escapeHtml(u.email)})</span>` : ''}</div>`).join('');
      dd.classList.remove('hidden');
    }
    dd.querySelectorAll('.user-dropdown-item').forEach((el) => {
      el.addEventListener('click', () => {
        const uid = el.getAttribute('data-uid');
        addProjectMember(uid);
        input.value = '';
        dd.classList.add('hidden');
        showAddUserDropdown();
      });
    });
  }

  function getUserById(uid) {
    return state.users.find((u) => u.id === uid) || null;
  }

  function renderProjectMembers(memberIds, ownerId) {
    const list = byId('project-members-list');
    if (!list) return;
    const ids = Array.isArray(memberIds) ? [...memberIds] : ownerId ? [ownerId] : [];
    const seen = new Set();
    const rows = [];
    if (ownerId && !seen.has(ownerId)) {
      seen.add(ownerId);
      const u = getUserById(ownerId);
      const verified = !!(u?.isVerified);
      rows.push({ uid: ownerId, name: u?.username || u?.email || 'User', verified, isOwner: true });
    }
    ids.forEach((uid) => {
      if (seen.has(uid) || uid === ownerId) return;
      seen.add(uid);
      const u = getUserById(uid);
      rows.push({ uid, name: u?.username || u?.email || 'User', verified: !!(u?.isVerified), isOwner: false });
    });
    list.innerHTML = rows.map((r) => `
      <div class="member-row" data-uid="${escapeHtml(r.uid)}">
        <span class="${r.verified ? 'verified-badge' : 'unverified-badge'}">${r.verified ? 'Verified' : 'Unverified'}</span>
        <span class="member-name">${escapeHtml(r.name)}</span>
        ${r.isOwner ? '<span class="owner-tag">(owner)</span>' : ''}
        ${r.isOwner ? '' : `<button type="button" class="member-remove" data-uid="${escapeHtml(r.uid)}" title="Remove">Remove</button>`}
      </div>
    `).join('');
    list.querySelectorAll('.member-remove').forEach((btn) => {
      btn.addEventListener('click', () => removeProjectMember(btn.dataset.uid));
    });
  }

  function renderAddUserSelect(excludeIds) {
    const input = byId('project-add-user-search');
    if (input) input.value = '';
    const dd = byId('project-add-user-dropdown');
    if (dd) dd.classList.add('hidden');
  }

  function getProjectMemberIds() {
    const list = byId('project-members-list');
    if (!list) return [];
    return Array.from(list.querySelectorAll('.member-row')).map((r) => r.dataset.uid).filter(Boolean);
  }

  function removeProjectMember(uid) {
    const rows = byId('project-members-list')?.querySelectorAll('.member-row');
    if (!rows) return;
    const row = Array.from(rows).find((r) => r.dataset.uid === uid);
    if (row) row.remove();
    renderAddUserSelect(getProjectMemberIds());
  }

  function addProjectMember(uid) {
    if (!uid) return;
    const current = getProjectMemberIds();
    if (current.includes(uid)) return;
    const u = getUserById(uid);
    if (!u) return;
    const list = byId('project-members-list');
    if (!list) return;
    const div = document.createElement('div');
    div.className = 'member-row';
    div.dataset.uid = uid;
    div.innerHTML = `
      <span class="${u.isVerified ? 'verified-badge' : 'unverified-badge'}">${u.isVerified ? 'Verified' : 'Unverified'}</span>
      <span class="member-name">${escapeHtml(u.username || u.email || 'User')}</span>
      <button type="button" class="member-remove" data-uid="${escapeHtml(uid)}" title="Remove">Remove</button>
    `;
    div.querySelector('.member-remove').addEventListener('click', () => removeProjectMember(uid));
    list.appendChild(div);
    renderAddUserSelect(getProjectMemberIds());
  }

  async function loadProjects() {
    const fs = getFirebaseService();
    if (!fs || !fs.isInitialized) return;
    const me = fs.auth?.currentUser;
    if (!me) return;
    try {
      const q = fb().query(
        fb().collection(fs.db, 'projects'),
        fb().orderBy('updatedAt', 'desc'),
        fb().limit(100)
      );
      const snap = await fb().getDocs(q);
      state.projects = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
    } catch (e) {
      console.error('[Project Manager] loadProjects failed', e);
      state.projects = [];
    }
  }

  async function getAdminUids() {
    const fs = getFirebaseService();
    if (!fs || !fs.db) return [];
    try {
      const q = fb().query(fb().collection(fs.db, 'users'), fb().where('role', '==', 'admin'), fb().limit(20));
      const snap = await fb().getDocs(q);
      return snap.docs.map((d) => d.id);
    } catch (_) {}
    return [];
  }

  async function createProjectChat(ownerId, projectName, additionalMemberIds) {
    const fs = getFirebaseService();
    const me = fs?.auth?.currentUser?.uid;
    if (!me || !fs?.db) throw new Error('Not authenticated');
    const memberIds = [ownerId];
    (additionalMemberIds || []).forEach((id) => { if (!memberIds.includes(id)) memberIds.push(id); });
    const connRef = fb().collection(fs.db, 'chatConnections').doc();
    await fb().setDoc(connRef, {
      participants: memberIds,
      memberIds,
      groupName: projectName || 'Project',
      updatedAt: new Date().toISOString(),
      participantUsernames: []
    });
    return connRef.id;
  }

  function renderProjects() {
    const list = byId('projects-list');
    if (!list) return;
    const search = (byId('project-search')?.value || '').toLowerCase();
    const statusFilter = byId('status-filter')?.value || '';
    let filtered = state.projects;
    if (search) filtered = filtered.filter((p) => (p.name || '').toLowerCase().includes(search) || (p.description || '').toLowerCase().includes(search));
    if (statusFilter) filtered = filtered.filter((p) => (p.status || '') === statusFilter);

    list.innerHTML = filtered.map((p) => {
      const color = p.statusColor || STATUS_COLORS[p.status] || '#6b7280';
      const owner = getUserById(p.ownerId);
      const ownerVerified = !!(owner?.isVerified);
      const ownerLabel = owner ? (owner.username || owner.email || 'User') : '—';
      const verifiedBadge = owner ? `<span class="owner-${ownerVerified ? 'verified' : 'unverified'}">${ownerVerified ? 'verified' : 'unverified'}</span>` : '';
      return `<div class="project-row" data-project-id="${escapeHtml(p.id)}">
        <span class="status-badge" style="background:${color}33;border:1px solid ${color}"><span style="background:${color};width:8px;height:8px;border-radius:50%;display:inline-block"></span> ${escapeHtml(String(p.status || 'unknown').replace(/_/g, ' '))}</span>
        <div class="project-name">${escapeHtml(p.name || 'Untitled')}</div>
        <div class="project-meta">${escapeHtml(ownerLabel)} ${verifiedBadge} · ${p.updatedAt ? new Date(p.updatedAt).toLocaleDateString() : ''}</div>
        <div class="project-actions">
          <button class="btn-icon" data-action="edit" data-project-id="${escapeHtml(p.id)}" title="Edit"><i class="fas fa-pencil-alt"></i></button>
          <button class="btn-icon" data-action="library" data-project-id="${escapeHtml(p.id)}" title="Library"><i class="fas fa-folder"></i></button>
          <a href="#" class="btn-icon" data-action="chat" data-conn-id="${escapeHtml(p.chatConnId || '')}" title="Chat"><i class="fas fa-comments"></i></a>
        </div>
      </div>`;
    }).join('');

    list.querySelectorAll('[data-action="edit"]').forEach((btn) => {
      btn.addEventListener('click', (e) => {
        e.preventDefault();
        const id = btn.getAttribute('data-project-id');
        const p = state.projects.find((pr) => pr.id === id);
        if (p) showProjectForm(p);
      });
    });
    list.querySelectorAll('[data-action="library"]').forEach((btn) => {
      btn.addEventListener('click', (e) => {
        e.preventDefault();
        const id = btn.getAttribute('data-project-id');
        const p = state.projects.find((pr) => pr.id === id);
        if (p) showLibrary(p);
      });
    });
    list.querySelectorAll('[data-action="chat"]').forEach((a) => {
      a.addEventListener('click', (e) => {
        e.preventDefault();
        const connId = a.getAttribute('data-conn-id');
        if (!connId) return;
        const path = window.location.pathname.replace(/project-manager\/[^?]*/, 'secure-chat/index.html');
        window.open(window.location.origin + path + '?connId=' + encodeURIComponent(connId), '_blank');
      });
    });
  }

  async function showProjectForm(project) {
    state.selectedProjectId = project ? project.id : null;
    byId('project-form-panel').style.display = '';
    byId('manager-main').style.display = 'none';
    byId('library-panel').style.display = 'none';
    byId('project-form-title').textContent = project ? 'Edit Project' : 'New Project';
    byId('project-id').value = project ? project.id : '';
    byId('project-name').value = project ? (project.name || '') : '';
    byId('project-description').value = project ? (project.description || '') : '';
    byId('project-status').value = project ? (project.status || 'submitted') : 'submitted';
    byId('project-status-color').value = project ? (project.statusColor || STATUS_COLORS[project.status] || '') : '';
    if (!state.users.length) await loadUsers();
    renderOwnerSelect(project ? project.ownerId : '');
    const memberIds = project?.memberIds ? [...project.memberIds] : (project?.ownerId ? [project.ownerId] : []);
    const ownerId = project ? project.ownerId : (byId('project-owner')?.value?.trim() || '');
    renderProjectMembers(memberIds.length ? memberIds : (ownerId ? [ownerId] : []), ownerId || undefined);
    renderAddUserSelect(memberIds.length ? memberIds : []);
  }

  function hideProjectForm() {
    byId('project-form-panel').style.display = 'none';
    byId('manager-main').style.display = '';
  }

  async function onSaveProject(e) {
    e.preventDefault();
    const fs = getFirebaseService();
    const me = fs?.auth?.currentUser;
    if (!me) {
      notify('Please log in', 'error');
      return;
    }
    const id = byId('project-id').value.trim();
    const name = byId('project-name').value.trim();
    const description = byId('project-description').value.trim();
    const status = byId('project-status').value;
    const statusColor = byId('project-status-color').value.trim();
    const ownerId = byId('project-owner').value.trim();
    const allMemberIds = getProjectMemberIds();
    const memberIds = [ownerId].filter(Boolean);
    allMemberIds.forEach((uid) => { if (uid !== ownerId && !memberIds.includes(uid)) memberIds.push(uid); });
    if (!name) {
      notify('Project name is required', 'error');
      return;
    }
    if (!ownerId) {
      notify('Please select an owner (search by email or name)', 'error');
      return;
    }
    const now = new Date().toISOString();
    try {
      if (id) {
        const ref = fb().doc(fs.db, 'projects', id);
        await fb().updateDoc(ref, { name, description, status, statusColor: statusColor || null, memberIds, updatedAt: now });
        const proj = state.projects.find((p) => p.id === id);
        const chatConnId = proj?.chatConnId;
        if (chatConnId) {
          try {
            const connRef = fb().doc(fs.db, 'chatConnections', chatConnId);
            const snap = await fb().getDoc(connRef);
            if (snap.exists()) {
              await fb().updateDoc(connRef, {
                groupName: name,
                participants: memberIds,
                memberIds,
                updatedAt: now
              });
            }
          } catch (_) {}
        }
        notify('Project updated');
      } else {
        const finalOwnerId = ownerId || me.uid;
        const otherMembers = memberIds.filter((uid) => uid !== finalOwnerId);
        const chatConnId = await createProjectChat(finalOwnerId, name, otherMembers);
        const projectRef = fb().collection(fs.db, 'projects').doc();
        const finalMemberIds = memberIds.length ? memberIds : [finalOwnerId];
        await fb().setDoc(projectRef, {
          name,
          description,
          status,
          statusColor: statusColor || STATUS_COLORS[status] || null,
          ownerId: finalOwnerId,
          memberIds: finalMemberIds,
          chatConnId,
          createdAt: now,
          updatedAt: now,
          requestData: null
        });
        await fb().updateDoc(fb().doc(fs.db, 'chatConnections', chatConnId), { projectId: projectRef.id });
        notify('Project created');
      }
      hideProjectForm();
      await loadProjects();
      renderProjects();
    } catch (err) {
      notify(err?.message || 'Failed to save', 'error');
    }
  }

  function showLibrary(project) {
    state.selectedProjectId = project.id;
    byId('manager-main').style.display = 'none';
    byId('project-form-panel').style.display = 'none';
    byId('library-panel').style.display = '';
    byId('library-project-name').textContent = project.name || 'Project';
    loadLibraryContent();
  }

  async function loadLibraryContent() {
    const fs = getFirebaseService();
    const folder = byId('library-folder')?.value || 'record_in/docs';
    const content = byId('library-content');
    if (!content || !state.selectedProjectId || !fs?.db) return;
    try {
      const q = fb().query(
        fb().collection(fs.db, 'projects', state.selectedProjectId, 'library'),
        fb().where('folderPath', '==', folder)
      );
      const snap = await fb().getDocs(q);
      const items = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
      content.innerHTML = items
        .filter((i) => i.type === 'file')
        .map((i) => `<div class="lib-item"><i class="fas fa-file"></i><span class="lib-name">${escapeHtml(i.name || 'file')}</span><a href="#" data-path="${escapeHtml(i.storagePath || '')}">Download</a></div>`)
        .join('');
      content.querySelectorAll('a[data-path]').forEach((a) => {
        a.addEventListener('click', async (e) => {
          e.preventDefault();
          const path = a.getAttribute('data-path');
          if (!path) return;
          try {
            const r = fb().ref(fs.storage, path);
            const url = await fb().getDownloadURL(r);
            window.open(url, '_blank');
          } catch (err) {
            notify('Download failed', 'error');
          }
        });
      });
    } catch (e) {
      content.innerHTML = '<p>Failed to load library</p>';
    }
  }

  async function uploadToLibrary(file) {
    const fs = getFirebaseService();
    const projectId = state.selectedProjectId;
    const folder = byId('library-folder')?.value || 'record_in/docs';
    if (!fs?.storage || !projectId || !file) return;
    const fname = (file.name || 'file').replace(/[^a-zA-Z0-9._-]/g, '_').slice(0, 80);
    const storagePath = `projects/${projectId}/library/${folder}/${Date.now()}_${fname}`;
    const ref = fb().ref(fs.storage, storagePath);
    await fb().uploadBytes(ref, file, { contentType: file.type || 'application/octet-stream' });
    const libRef = fb().collection(fs.db, 'projects', projectId, 'library').doc();
    await fb().setDoc(libRef, {
      folderPath: folder,
      name: fname,
      storagePath,
      type: 'file',
      createdAt: new Date().toISOString(),
      createdBy: fs.auth?.currentUser?.uid
    });
  }

  async function createSubfolder(name) {
    const fs = getFirebaseService();
    const projectId = state.selectedProjectId;
    const base = byId('library-folder')?.value || 'record_in/docs';
    if (!name || !projectId || !fs?.db) return;
    const folderPath = base + '/' + name.replace(/[^a-zA-Z0-9_-]/g, '_');
    const libRef = fb().collection(fs.db, 'projects', projectId, 'library').doc();
    await fb().setDoc(libRef, {
      folderPath,
      name,
      type: 'folder',
      createdAt: new Date().toISOString(),
      createdBy: fs.auth?.currentUser?.uid
    });
    notify('Folder created');
    loadLibraryContent();
  }

  function init() {
    byId('back-btn')?.addEventListener('click', () => {
      try {
        if (window.parent && window.parent !== window) {
          window.parent.postMessage({ type: 'liber:close-app-shell' }, '*');
        }
      } catch (_) {}
    });

    byId('project-add-btn')?.addEventListener('click', () => showProjectForm(null));
    byId('project-cancel')?.addEventListener('click', () => hideProjectForm());
    byId('project-form')?.addEventListener('submit', onSaveProject);
    byId('project-add-member-btn')?.addEventListener('click', () => { byId('project-add-user-search')?.focus(); showAddUserDropdown(); });
    byId('project-owner-search')?.addEventListener('input', () => showOwnerDropdown());
    byId('project-owner-search')?.addEventListener('focus', () => showOwnerDropdown());
    byId('project-owner-search')?.addEventListener('blur', () => setTimeout(() => byId('project-owner-dropdown')?.classList.add('hidden'), 150));
    byId('project-add-user-search')?.addEventListener('input', () => showAddUserDropdown());
    byId('project-add-user-search')?.addEventListener('focus', () => showAddUserDropdown());
    byId('project-add-user-search')?.addEventListener('blur', () => setTimeout(() => byId('project-add-user-dropdown')?.classList.add('hidden'), 150));

    byId('project-search')?.addEventListener('input', () => renderProjects());
    byId('status-filter')?.addEventListener('change', () => renderProjects());

    byId('library-back')?.addEventListener('click', () => {
      byId('library-panel').style.display = 'none';
      byId('manager-main').style.display = '';
    });
    byId('library-folder')?.addEventListener('change', () => loadLibraryContent());

    byId('library-create-folder')?.addEventListener('click', () => {
      const name = prompt('Subfolder name:');
      if (name) createSubfolder(name);
    });

    const uploadZone = byId('library-upload-zone');
    const uploadInput = byId('library-upload-input');
    if (uploadZone && uploadInput) {
      uploadZone.addEventListener('click', () => uploadInput.click());
      uploadZone.addEventListener('dragover', (e) => { e.preventDefault(); uploadZone.classList.add('dragover'); });
      uploadZone.addEventListener('dragleave', () => uploadZone.classList.remove('dragover'));
      uploadZone.addEventListener('drop', async (e) => {
        e.preventDefault();
        uploadZone.classList.remove('dragover');
        const files = Array.from(e.dataTransfer?.files || []);
        for (const f of files) {
          try {
            await uploadToLibrary(f);
            notify('Uploaded ' + f.name);
          } catch (err) {
            notify('Failed: ' + f.name, 'error');
          }
        }
        loadLibraryContent();
      });
      uploadInput.addEventListener('change', async (e) => {
        const files = Array.from(e.target.files || []);
        e.target.value = '';
        for (const f of files) {
          try {
            await uploadToLibrary(f);
            notify('Uploaded ' + f.name);
          } catch (err) {
            notify('Failed: ' + f.name, 'error');
          }
        }
        loadLibraryContent();
      });
    }

    async function isCurrentUserAdmin() {
      try {
        const parent = window.parent || window.top;
        if (parent && parent.dashboardManager?._isAdminSession) return true;
        if (parent && parent.authManager?.isAdmin?.()) return true;
      } catch (_) {}
      const fs = getFirebaseService();
      const me = fs?.auth?.currentUser;
      if (!me || !fs?.db) return false;
      const userDoc = await fb().getDoc(fb().doc(fs.db, 'users', me.uid));
      const role = userDoc?.exists ? (userDoc.data()?.role || 'user') : 'user';
      return String(role).toLowerCase() === 'admin';
    }

    const tryInit = async () => {
      if (!window.firebase || !window.firebaseService?.isInitialized) {
        setTimeout(tryInit, 200);
        return;
      }
      const fs = getFirebaseService();
      if (!fs?.auth?.currentUser) {
        notify('Please log in first', 'error');
        showAccessDenied('Please log in to access Project Manager.');
        return;
      }
      const isAdmin = await isCurrentUserAdmin();
      if (!isAdmin) {
        showAccessDenied('Project Manager is for administrators only. Use Project Tracker to view your projects.');
        return;
      }
      await loadUsers();
      await loadProjects();
      renderProjects();
    };

    function showAccessDenied(message) {
      const app = document.querySelector('.manager-app');
      if (!app) return;
      app.innerHTML = `
        <header class="manager-header">
          <h1><i class="fas fa-cogs"></i> Project Manager</h1>
        </header>
        <div class="panel" style="text-align:center;padding:2rem;">
          <p style="color:var(--error-color,#c62828);margin:1rem 0;">${escapeHtml(message)}</p>
          <button id="back-btn" class="btn btn-secondary"><i class="fas fa-arrow-left"></i> Back</button>
        </div>
      `;
      const backBtn = document.getElementById('back-btn');
      if (backBtn) backBtn.addEventListener('click', () => { try { if (window.parent?.history) window.parent.history.back(); else window.history.back(); } catch(_) { window.location.href = '../'; } });
    }
    tryInit();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
