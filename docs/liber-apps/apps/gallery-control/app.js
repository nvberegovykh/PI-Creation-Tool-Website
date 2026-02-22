(function () {
  const state = {
    projects: [],
    selectedProjectId: '',
    items: [],
    selectedItemId: '',
    editingProject: false,
    editingItem: false
  };

  const byId = (id) => document.getElementById(id);
  const notify = (msg, type) => {
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
    panel.style.display = '';
    heading.textContent = state.editingProject ? 'Edit Project' : 'New Project';
    saveBtn.textContent = state.editingProject ? 'Update Project' : 'Save Project';
    cancelBtn.style.display = state.editingProject ? '' : 'none';
    deleteBtn.style.display = state.editingProject ? '' : 'none';
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
      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        const id = btn.getAttribute('data-project-id');
        const p = state.projects.find((pr) => pr.id === id);
        if (p) {
          fillProjectForm(p);
          showProjectForm('edit');
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
    while (!(window.firebaseService && window.firebaseService.isInitialized) && attempts < 150) {
      await new Promise((resolve) => setTimeout(resolve, 100));
      attempts += 1;
    }
    if (!(window.firebaseService && window.firebaseService.isInitialized)) {
      throw new Error('Firebase failed to initialize');
    }
  }

  async function loadProjects() {
    try {
      state.projects = await window.firebaseService.getGalleryProjects({ publishedOnly: false });
      for (const p of state.projects) {
        p.items = await window.firebaseService.getGalleryItems(p.id, { publishedOnly: false });
      }
    } catch (err) {
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
      state.items = await window.firebaseService.getGalleryItems(state.selectedProjectId, { publishedOnly: false });
    } catch (err) {
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
    const user = window.firebaseService.auth.currentUser;
    if (!user || !user.uid) throw new Error('You must be signed in');
    const storage = firebase.getStorage();
    const ext = (file.name.split('.').pop() || 'bin').toLowerCase();
    const path = `gallery/${user.uid}/${projectId}/${itemId}/media_0.${ext}`;
    const ref = firebase.ref(storage, path);
    await firebase.uploadBytes(ref, file, { contentType: file.type || 'application/octet-stream' });
    return firebase.getDownloadURL(ref);
  }

  async function onSaveProject(e) {
    e.preventDefault();
    const current = getSelectedProject();
    const payload = {
      title: byId('project-title').value.trim(),
      year: byId('project-year').value.trim(),
      description: byId('project-description').value.trim(),
      coverPolicy: byId('project-cover-policy').value,
      isPublished: byId('project-published').checked,
      ownerId: window.firebaseService.auth.currentUser?.uid || ''
    };
    if (!payload.title) {
      notify('Project title is required', 'warning');
      return;
    }
    try {
      if (current) {
        await window.firebaseService.updateGalleryProject(current.id, payload);
        notify('Project updated');
      } else {
        const created = await window.firebaseService.createGalleryProject(payload);
        state.selectedProjectId = created.id;
        byId('project-id').value = created.id;
        notify('Project created');
      }
      resetProjectForm();
      hideProjectForm();
      await loadProjects();
      if (state.selectedProjectId) await loadItems();
    } catch (err) {
      notify(err.message || String(err), 'error');
    }
  }

  async function onDeleteProject(project) {
    if (!project) project = getSelectedProject();
    if (!project) return;
    const ok = confirm(`Delete project "${project.title || project.id}"?`);
    if (!ok) return;
    try {
      await window.firebaseService.deleteGalleryProject(project.id);
      resetProjectForm();
      hideProjectForm();
      state.selectedProjectId = '';
      byId('item-section').style.display = 'none';
      await loadProjects();
      state.items = [];
      renderItemCards();
      notify('Project deleted');
    } catch (err) {
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
    const payload = {
      type,
      caption: byId('item-caption').value.trim(),
      text: byId('item-text').value.trim(),
      sortOrder: Number(byId('item-sort-order').value || 0),
      isPublished: byId('item-published').checked,
      ownerId: window.firebaseService.auth.currentUser?.uid || ''
    };
    const file = byId('item-file').files[0];

    try {
      if (current) {
        const updates = { ...payload };
        if (type !== 'text' && file) {
          updates.url = await uploadMediaFile(file, state.selectedProjectId, current.id);
          updates.thumbUrl = updates.url;
        }
        await window.firebaseService.updateGalleryItem(state.selectedProjectId, current.id, updates);
        notify('Item updated');
      } else {
        const created = await window.firebaseService.createGalleryItem(state.selectedProjectId, payload);
        if (type !== 'text' && file) {
          const mediaUrl = await uploadMediaFile(file, state.selectedProjectId, created.id);
          await window.firebaseService.updateGalleryItem(state.selectedProjectId, created.id, { url: mediaUrl, thumbUrl: mediaUrl });
        }
      }
      resetItemForm();
      hideItemForm();
      await loadItems();
      await loadProjects();
    } catch (err) {
      notify(err.message || String(err), 'error');
    }
  }

  async function onDeleteItem(item) {
    if (!item) item = getSelectedItem();
    if (!item || !state.selectedProjectId) return;
    const ok = confirm(`Delete item "${item.caption || item.id}"?`);
    if (!ok) return;
    try {
      await window.firebaseService.deleteGalleryItem(state.selectedProjectId, item.id);
      resetItemForm();
      hideItemForm();
      await loadItems();
      await loadProjects();
      notify('Item deleted');
    } catch (err) {
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
      notify(err.message || String(err), 'error');
    }
  }

  init();
})();
