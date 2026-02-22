(function () {
  'use strict';
  const PREFIX = '[Gallery Control]';
  function reportToParent(label, err, extra) {
    const isError = err instanceof Error;
    const msg = isError ? (err.message || '') : (err ? String(err) : '');
    const stack = isError && err.stack ? err.stack : '';
    if (isError) console.error(PREFIX, label, msg, extra || '');
    try {
      const target = window.parent || window.top;
      if (target && target !== window && target.postMessage) {
        target.postMessage({ type: 'liber:gallery-error', label, message: msg, stack, extra: extra ? JSON.stringify(extra) : '', isError: isError }, '*');
      }
    } catch (_) {}
  }

  const state = {
    projects: [],
    selectedProjectId: '',
    items: [],
    selectedItemId: '',
    editingProject: false,
    editingItem: false,
    projectMediaFiles: [],
    editingProjectItems: [],
    projectItemsToRemove: []
  };

  const byId = (id) => document.getElementById(id);

  function getFirebaseService() {
    try {
      for (const w of [window.parent, window.top].filter(Boolean)) {
        if (w !== window && w.firebaseService && w.firebaseService.isInitialized) return w.firebaseService;
      }
    } catch (_) {}
    return window.firebaseService;
  }

  const notify = (msg, type) => {
    console.warn(PREFIX, type || 'info', msg);
    if (type === 'error') reportToParent('notify', new Error(msg), { msg });
    if (window.parent && window.parent.dashboardManager) {
      window.parent.dashboardManager.showNotification(msg, type || 'success');
    } else {
      alert(msg);
    }
  };

  function getSelectedProject() {
    return state.projects.find((p) => p.id === state.selectedProjectId) || null;
  }

  function getSelectedItem() {
    return state.items.find((i) => i.id === state.selectedItemId) || null;
  }

  function showProjectForm(mode) {
    state.editingProject = mode === 'edit';
    const panel = byId('project-upload-panel');
    const heading = byId('project-form-heading');
    const saveBtn = byId('project-save-btn');
    const cancelBtn = byId('project-cancel');
    const deleteBtn = byId('project-delete');
    const mediaWrap = byId('project-upload-media');
    panel.style.display = '';
    heading.textContent = state.editingProject ? 'Edit Project' : 'New Project';
    saveBtn.textContent = state.editingProject ? 'Update Project' : 'Save Project';
    cancelBtn.style.display = state.editingProject ? '' : 'none';
    deleteBtn.style.display = state.editingProject ? '' : 'none';
    if (mediaWrap) { mediaWrap.style.display = ''; mediaWrap.style.visibility = 'visible'; mediaWrap.style.opacity = '1'; }
  }

  function hideProjectForm() {
    byId('project-upload-panel').style.display = 'none';
    state.editingProject = false;
  }

  function showItemForm(mode) {
    state.editingItem = mode === 'edit';
    const panel = byId('item-upload-panel');
    const cancelBtn = byId('item-cancel');
    const deleteBtn = byId('item-delete');
    panel.style.display = '';
    cancelBtn.style.display = state.editingItem ? '' : 'none';
    deleteBtn.style.display = state.editingItem ? '' : 'none';
  }

  function hideItemForm() {
    byId('item-upload-panel').style.display = 'none';
    state.editingItem = false;
  }

  function resetProjectForm() {
    byId('project-id').value = '';
    byId('project-title').value = '';
    byId('project-year').value = '';
    byId('project-description').value = '';
    byId('project-cover-policy').value = 'first';
    byId('project-published').checked = false;
    state.selectedProjectId = '';
    state.projectMediaFiles = [];
    state.editingProjectItems = [];
    state.projectItemsToRemove = [];
    renderProjectMediaPreviews();
  }

  function resetItemForm() {
    byId('item-id').value = '';
    byId('item-type').value = 'image';
    byId('item-caption').value = '';
    byId('item-text').value = '';
    byId('item-sort-order').value = '0';
    byId('item-published').checked = false;
    byId('item-file').value = '';
    state.selectedItemId = '';
    syncItemTypeFields();
  }

  function fillProjectForm(project) {
    byId('project-id').value = project.id || '';
    byId('project-title').value = project.title || '';
    byId('project-year').value = project.year || '';
    byId('project-description').value = project.description || '';
    byId('project-cover-policy').value = project.coverPolicy || 'first';
    byId('project-published').checked = !!project.isPublished;
    state.selectedProjectId = project.id || '';
    state.editingProjectItems = Array.from(project.items || []);
    state.projectMediaFiles = [];
    state.projectItemsToRemove = [];
    renderProjectMediaPreviews();
  }

  function fillItemForm(item) {
    byId('item-id').value = item.id || '';
    byId('item-type').value = item.type || 'image';
    byId('item-caption').value = item.caption || '';
    byId('item-text').value = item.text || '';
    byId('item-sort-order').value = String(item.sortOrder || 0);
    byId('item-published').checked = !!item.isPublished;
    byId('item-file').value = '';
    syncItemTypeFields();
  }

  function addProjectMediaFiles(files) {
    const acceptable = ['image/', 'video/'];
    for (const f of Array.from(files || [])) {
      if (acceptable.some((t) => f.type.startsWith(t))) state.projectMediaFiles.push(f);
    }
    renderProjectMediaPreviews();
  }

  function renderProjectMediaPreviews() {
    const previews = byId('project-media-previews');
    if (!previews) return;
    previews.innerHTML = '';
    const toRemove = state.projectItemsToRemove || [];
    (state.editingProjectItems || []).filter((it) => !toRemove.includes(it.id)).forEach((item) => {
      const wrap = document.createElement('div');
      wrap.className = 'media-preview-item';
      const url = item.thumbUrl || item.url || '';
      if (item.type === 'video' || /\.(mp4|webm|ogg)$/i.test(url)) {
        const vid = document.createElement('video');
        vid.src = url;
        vid.muted = true;
        vid.playsInline = true;
        wrap.appendChild(vid);
      } else if (url) {
        const img = document.createElement('img');
        img.src = url;
        img.alt = item.caption || 'media';
        wrap.appendChild(img);
      } else {
        wrap.innerHTML = '<i class="fas fa-align-left"></i><span>' + (item.type || 'item') + '</span>';
      }
      const rm = document.createElement('button');
      rm.type = 'button';
      rm.className = 'media-remove-btn';
      rm.innerHTML = '&times;';
      rm.title = 'Remove';
      rm.onclick = () => {
        state.projectItemsToRemove = state.projectItemsToRemove || [];
        if (!state.projectItemsToRemove.includes(item.id)) state.projectItemsToRemove.push(item.id);
        renderProjectMediaPreviews();
      };
      wrap.appendChild(rm);
      previews.appendChild(wrap);
    });
    (state.projectMediaFiles || []).forEach((f) => {
      const wrap = document.createElement('div');
      wrap.className = 'media-preview-item';
      if (f.type.startsWith('image/')) {
        const img = document.createElement('img');
        img.src = URL.createObjectURL(f);
        img.alt = f.name;
        wrap.appendChild(img);
      } else if (f.type.startsWith('video/')) {
        const vid = document.createElement('video');
        vid.src = URL.createObjectURL(f);
        vid.muted = true;
        vid.playsInline = true;
        wrap.appendChild(vid);
      } else {
        wrap.textContent = f.name;
        wrap.style.fontSize = '11px';
      }
      const rm = document.createElement('button');
      rm.type = 'button';
      rm.className = 'media-remove-btn';
      rm.innerHTML = '&times;';
      rm.title = 'Remove';
      rm.onclick = () => {
        state.projectMediaFiles = state.projectMediaFiles.filter((x) => x !== f);
        renderProjectMediaPreviews();
      };
      wrap.appendChild(rm);
      previews.appendChild(wrap);
    });
  }

  function syncItemTypeFields() {
    const type = byId('item-type').value;
    const wrap = byId('item-file-wrap');
    wrap.style.display = type === 'text' ? 'none' : '';
  }

  function firstVisualUrl(project) {
    return project.coverUrl || (project.items && project.items[0] && (project.items[0].url || project.items[0].thumbUrl)) || '';
  }

  function renderProjectCards() {
    const host = byId('projects-cards');
    host.innerHTML = state.projects.map((p) => {
      const cover = firstVisualUrl(p);
      const mediaHtml = cover
        ? (cover.match(/\.(mp4|webm|ogg)$/i) ? `<video src="${cover}" muted playsinline></video>` : `<img src="${cover}" alt="">`)
        : '<div class="gc-card-placeholder"><i class="fas fa-image"></i></div>';
      return (
        `<div class="gc-project-card" data-project-id="${p.id}">` +
        `<div class="gc-card-preview">${mediaHtml}</div>` +
        `<div class="gc-card-info">` +
        `<strong>${p.title || 'Untitled'}</strong>` +
        `<span>${p.year || '-'} • ${p.isPublished ? 'Published' : 'Draft'}</span>` +
        `</div>` +
        `<div class="gc-card-actions">` +
        `<button class="gc-btn-edit" data-project-id="${p.id}" title="Edit"><i class="fas fa-pencil-alt"></i></button>` +
        `<button class="gc-btn-delete" data-project-id="${p.id}" title="Delete"><i class="fas fa-trash"></i></button>` +
        `</div>` +
        `</div>`
      );
    }).join('');

    host.querySelectorAll('.gc-btn-edit').forEach((btn) => {
      btn.addEventListener('click', async (e) => {
        e.stopPropagation();
        const id = btn.getAttribute('data-project-id');
        let p = state.projects.find((pr) => pr.id === id);
        if (p) {
          try {
            const svc = getFirebaseService();
            p.items = await svc.getGalleryItems(p.id, { publishedOnly: false });
          } catch (_) {}
          fillProjectForm(p);
          showProjectForm('edit');
          setTimeout(() => byId('project-upload-media')?.scrollIntoView({ behavior: 'smooth', block: 'nearest' }), 100);
        }
      });
    });
    host.querySelectorAll('.gc-btn-delete').forEach((btn) => {
      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        const id = btn.getAttribute('data-project-id');
        const p = state.projects.find((pr) => pr.id === id);
        if (p) onDeleteProject(p);
      });
    });
  }

  function renderItemCards() {
    const host = byId('items-cards');
    host.innerHTML = state.items.map((item) => {
      let preview = '';
      if (item.type === 'text') {
        preview = '<div class="gc-item-placeholder"><i class="fas fa-align-left"></i></div>';
      } else if (item.url) {
        preview = item.url.match(/\.(mp4|webm|ogg)$/i)
          ? `<video src="${item.url}" muted playsinline></video>`
          : `<img src="${item.thumbUrl || item.url}" alt="">`;
      } else {
        preview = '<div class="gc-item-placeholder"><i class="fas fa-file"></i></div>';
      }
      return (
        `<div class="gc-item-card" data-item-id="${item.id}">` +
        `<div class="gc-card-preview">${preview}</div>` +
        `<div class="gc-card-info">` +
        `<strong>${item.caption || item.type || 'Item'}</strong>` +
        `<span>${item.type} • ${item.isPublished ? 'Published' : 'Draft'}</span>` +
        `</div>` +
        `<div class="gc-card-actions">` +
        `<button class="gc-btn-edit" data-item-id="${item.id}" title="Edit"><i class="fas fa-pencil-alt"></i></button>` +
        `<button class="gc-btn-delete" data-item-id="${item.id}" title="Delete"><i class="fas fa-trash"></i></button>` +
        `</div>` +
        `</div>`
      );
    }).join('');

    host.querySelectorAll('.gc-btn-edit[data-item-id]').forEach((btn) => {
      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        const id = btn.getAttribute('data-item-id');
        const item = state.items.find((i) => i.id === id);
        if (item) {
          fillItemForm(item);
          showItemForm('edit');
        }
      });
    });
    host.querySelectorAll('.gc-btn-delete[data-item-id]').forEach((btn) => {
      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        const id = btn.getAttribute('data-item-id');
        const item = state.items.find((i) => i.id === id);
        if (item) onDeleteItem(item);
      });
    });
  }

  async function ensureFirebaseReady() {
    let attempts = 0;
    const hasService = () => {
      const svc = getFirebaseService();
      return svc && svc.isInitialized;
    };
    while (!hasService() && attempts < 150) {
      await new Promise((resolve) => setTimeout(resolve, 100));
      attempts += 1;
    }
    if (!hasService()) {
      throw new Error('Firebase failed to initialize');
    }
  }

  async function loadProjects() {
    try {
      const svc = getFirebaseService();
      state.projects = await svc.getGalleryProjects({ publishedOnly: false });
      for (const p of state.projects) {
        p.items = await svc.getGalleryItems(p.id, { publishedOnly: false });
      }
    } catch (err) {
      reportToParent('loadProjects', err, { uid: getFirebaseService()?.auth?.currentUser?.uid });
      notify(err.message || 'Failed to load projects', 'error');
      state.projects = [];
    }
    renderProjectCards();
  }

  async function loadItems() {
    if (!state.selectedProjectId) {
      state.items = [];
      byId('item-section').style.display = 'none';
      renderItemCards();
      return;
    }
    try {
      const svc = getFirebaseService();
      state.items = await svc.getGalleryItems(state.selectedProjectId, { publishedOnly: false });
    } catch (err) {
      reportToParent('loadItems', err, { projectId: state.selectedProjectId });
      notify(err.message || 'Failed to load items', 'error');
      state.items = [];
    }
    const proj = getSelectedProject();
    byId('item-section').style.display = '';
    byId('current-project-title').textContent = proj ? proj.title || 'Project' : '';
    hideItemForm();
    resetItemForm();
    renderItemCards();
  }

  async function uploadMediaFile(file, projectId, itemId) {
    const svc = getFirebaseService();
    const user = svc.auth?.currentUser;
    if (!user?.uid) throw new Error('You must be signed in to upload');
    if (!file || typeof file.name !== 'string') throw new Error('Invalid file');
    if (!svc?.app) throw new Error('Firebase app not ready');
    const fb = (window.parent && window.parent !== window && window.parent.firebase) ? window.parent.firebase : window.firebase;
    if (!fb?.getStorage || !fb?.ref) throw new Error('Storage not available');
    const storage = fb.getStorage(svc.app);
    const ext = (file.name.split('.').pop() || 'bin').toLowerCase();
    const path = `gallery/${user.uid}/${projectId}/${itemId}/media_0.${ext}`;
    const storageRef = fb.ref(storage, path);
    await fb.uploadBytes(storageRef, file, { contentType: file.type || 'application/octet-stream' });
    const url = await fb.getDownloadURL(storageRef);
    return url;
  }

  async function onSaveProject(e) {
    e.preventDefault();
    const current = getSelectedProject();
    const svc = getFirebaseService();
    let uid = svc.auth.currentUser?.uid || '';
    if (!uid) {
      for (let i = 0; i < 30; i++) {
        await new Promise((r) => setTimeout(r, 200));
        uid = svc.auth.currentUser?.uid || '';
        if (uid) break;
      }
    }
    if (!uid) {
      notify('You must be signed in. Please log in on the main page first, then open Gallery Control again.', 'error');
      return;
    }
    const payload = {
      title: byId('project-title').value.trim(),
      year: byId('project-year').value.trim(),
      description: byId('project-description').value.trim(),
      coverPolicy: byId('project-cover-policy').value,
      isPublished: byId('project-published').checked,
      ownerId: uid
    };
    if (!payload.title) {
      notify('Project title is required', 'warning');
      return;
    }
    const mediaFiles = Array.from(state.projectMediaFiles || []);
    try {
      if (current) {
        for (const itemId of (state.projectItemsToRemove || [])) {
          try { await svc.deleteGalleryItem(current.id, itemId); } catch (_) {}
        }
        const orders = (state.editingProjectItems || []).map((i) => Number(i.sortOrder) + 1);
        const baseOrder = orders.length ? Math.max(0, ...orders) : 0;
        for (let i = 0; i < mediaFiles.length; i++) {
          const file = mediaFiles[i];
          if (!(file instanceof File)) continue;
          const isVideo = file.type.startsWith('video/');
          const itemPayload = { type: isVideo ? 'video' : 'image', sortOrder: baseOrder + i, isPublished: payload.isPublished, ownerId: uid };
          const item = await svc.createGalleryItem(current.id, itemPayload);
          const url = await uploadMediaFile(file, current.id, item.id);
          await svc.updateGalleryItem(current.id, item.id, { url, thumbUrl: url });
        }
        await svc.updateGalleryProject(current.id, payload);
        notify('Project updated');
      } else {
        const created = await svc.createGalleryProject(payload);
        state.selectedProjectId = created.id;
        byId('project-id').value = created.id;
        for (let i = 0; i < mediaFiles.length; i++) {
          const file = mediaFiles[i];
          if (!(file instanceof File)) continue;
          const isVideo = file.type.startsWith('video/');
          const itemPayload = { type: isVideo ? 'video' : 'image', sortOrder: i, isPublished: payload.isPublished, ownerId: uid };
          const item = await svc.createGalleryItem(created.id, itemPayload);
          const url = await uploadMediaFile(file, created.id, item.id);
          await svc.updateGalleryItem(created.id, item.id, { url, thumbUrl: url });
        }
        state.projectMediaFiles = [];
        renderProjectMediaPreviews();
        notify(mediaFiles.length ? `Project created with ${mediaFiles.length} media` : 'Project created');
      }
      resetProjectForm();
      hideProjectForm();
      await loadProjects();
      if (state.selectedProjectId) await loadItems();
    } catch (err) {
      reportToParent('onSaveProject', err, { uid: svc?.auth?.currentUser?.uid, title: payload?.title });
      notify(err.message || String(err), 'error');
    }
  }

  async function onDeleteProject(project) {
    if (!project) project = getSelectedProject();
    if (!project) return;
    const ok = confirm(`Delete project "${project.title || project.id}"?`);
    if (!ok) return;
    try {
      await getFirebaseService().deleteGalleryProject(project.id);
      resetProjectForm();
      hideProjectForm();
      state.selectedProjectId = '';
      byId('item-section').style.display = 'none';
      await loadProjects();
      state.items = [];
      renderItemCards();
      notify('Project deleted');
    } catch (err) {
      reportToParent('onDeleteProject', err);
      notify(err.message || String(err), 'error');
    }
  }

  async function onSaveItem(e) {
    e.preventDefault();
    if (!state.selectedProjectId) {
      notify('Select a project first', 'warning');
      return;
    }
    const current = getSelectedItem();
    const type = byId('item-type').value;
    const svc = getFirebaseService();
    const payload = {
      type,
      caption: byId('item-caption').value.trim(),
      text: byId('item-text').value.trim(),
      sortOrder: Number(byId('item-sort-order').value || 0),
      isPublished: byId('item-published').checked,
      ownerId: svc.auth.currentUser?.uid || ''
    };
    const file = byId('item-file').files[0];

    try {
      if (current) {
        const updates = { ...payload };
        if (type !== 'text' && file) {
          updates.url = await uploadMediaFile(file, state.selectedProjectId, current.id);
          updates.thumbUrl = updates.url;
        }
        await svc.updateGalleryItem(state.selectedProjectId, current.id, updates);
        notify('Item updated');
      } else {
        const created = await svc.createGalleryItem(state.selectedProjectId, payload);
        if (type !== 'text' && file) {
          const mediaUrl = await uploadMediaFile(file, state.selectedProjectId, created.id);
          await svc.updateGalleryItem(state.selectedProjectId, created.id, { url: mediaUrl, thumbUrl: mediaUrl });
        }
      }
      resetItemForm();
      hideItemForm();
      await loadItems();
      await loadProjects();
    } catch (err) {
      reportToParent('onSaveItem', err);
      notify(err.message || String(err), 'error');
    }
  }

  async function onDeleteItem(item) {
    if (!item) item = getSelectedItem();
    if (!item || !state.selectedProjectId) return;
    const ok = confirm(`Delete item "${item.caption || item.id}"?`);
    if (!ok) return;
    try {
      await getFirebaseService().deleteGalleryItem(state.selectedProjectId, item.id);
      resetItemForm();
      hideItemForm();
      await loadItems();
      await loadProjects();
      notify('Item deleted');
    } catch (err) {
      reportToParent('onDeleteItem', err);
      notify(err.message || String(err), 'error');
    }
  }

  function wireActions() {
    byId('back-btn').addEventListener('click', () => {
      if (window.parent) {
        window.parent.postMessage({ type: 'liber:close-app-shell' }, '*');
      } else {
        window.location.href = '../../index.html#apps';
      }
    });
    byId('item-type').addEventListener('change', syncItemTypeFields);

    byId('project-add-btn').addEventListener('click', () => {
      resetProjectForm();
      showProjectForm('create');
    });
    byId('project-cancel').addEventListener('click', () => {
      resetProjectForm();
      hideProjectForm();
    });
    byId('project-delete').addEventListener('click', () => onDeleteProject());
    byId('project-form').addEventListener('submit', (e) => onSaveProject(e));

    const projectMedia = byId('project-media');
    const uploadZone = byId('project-upload-zone');
    if (projectMedia && uploadZone) {
      uploadZone.addEventListener('click', (e) => { if (!e.target.closest('.media-remove-btn')) projectMedia.click(); });
      projectMedia.addEventListener('change', () => { addProjectMediaFiles(projectMedia.files); projectMedia.value = ''; });
      uploadZone.addEventListener('dragover', (e) => { e.preventDefault(); e.stopPropagation(); uploadZone.classList.add('drag-over'); });
      uploadZone.addEventListener('dragleave', () => uploadZone.classList.remove('drag-over'));
      uploadZone.addEventListener('drop', (e) => {
        e.preventDefault();
        e.stopPropagation();
        uploadZone.classList.remove('drag-over');
        addProjectMediaFiles(e.dataTransfer?.files);
      });
      document.addEventListener('paste', (e) => {
        const panel = byId('project-upload-panel');
        if (!panel || panel.style.display === 'none' || state.editingProject) return;
        if (e.clipboardData?.files?.length) addProjectMediaFiles(e.clipboardData.files);
      });
    }

    byId('item-add-btn').addEventListener('click', () => {
      resetItemForm();
      showItemForm('create');
    });
    byId('item-cancel').addEventListener('click', () => {
      resetItemForm();
      hideItemForm();
    });
    byId('item-delete').addEventListener('click', () => onDeleteItem());
    byId('item-form').addEventListener('submit', (e) => onSaveItem(e));

    byId('projects-cards').addEventListener('click', (e) => {
      const card = e.target.closest('.gc-project-card');
      if (!card || e.target.closest('.gc-card-actions')) return;
      const id = card.getAttribute('data-project-id');
      const p = state.projects.find((pr) => pr.id === id);
      if (p) {
        state.selectedProjectId = p.id;
        byId('current-project-title').textContent = p.title || 'Project';
        byId('item-section').style.display = '';
        loadItems();
      }
    });
  }

  async function init() {
    try {
      wireActions();
      await ensureFirebaseReady();
      hideProjectForm();
      await loadProjects();
      syncItemTypeFields();
    } catch (err) {
      reportToParent('init', err, { hasFirebase: !!getFirebaseService() });
      notify(err.message || String(err), 'error');
    }
  }

  reportToParent('started', { message: 'loaded', isError: false }, { ts: Date.now() });
  init().catch((err) => {
    console.error('[Gallery] init:', err);
    notify(err.message || 'Failed to initialize', 'error');
  });
})();
