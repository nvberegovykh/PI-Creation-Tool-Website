(function () {
  const state = {
    projects: [],
    selectedProjectId: '',
    items: [],
    selectedItemId: ''
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

  function resetProjectForm() {
    byId('project-id').value = '';
    byId('project-title').value = '';
    byId('project-year').value = '';
    byId('project-description').value = '';
    byId('project-layout-tags').value = '';
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
    byId('project-layout-tags').value = Array.isArray(project.layoutTags) ? project.layoutTags.join(', ') : '';
    byId('project-cover-policy').value = project.coverPolicy || 'first';
    byId('project-published').checked = !!project.isPublished;
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

  function renderProjects() {
    const host = byId('projects-list');
    host.innerHTML = state.projects.map((p) => {
      const active = p.id === state.selectedProjectId ? 'active' : '';
      return (
        `<div class="list-item ${active}" data-project-id="${p.id}">` +
        `<div><strong>${p.title || 'Untitled Project'}</strong></div>` +
        `<div class="list-item__meta">${p.year || '-'} • ${p.isPublished ? 'Published' : 'Draft'}</div>` +
        '</div>'
      );
    }).join('');
    Array.from(host.querySelectorAll('[data-project-id]')).forEach((el) => {
      el.addEventListener('click', async () => {
        state.selectedProjectId = el.getAttribute('data-project-id') || '';
        const p = getSelectedProject();
        if (p) fillProjectForm(p);
        await loadItems();
        renderProjects();
      });
    });
  }

  function renderItems() {
    const host = byId('items-list');
    host.innerHTML = state.items.map((item) => {
      const active = item.id === state.selectedItemId ? 'active' : '';
      return (
        `<div class="list-item ${active}" data-item-id="${item.id}">` +
        `<div><strong>${item.type || 'item'}</strong> • ${(item.caption || 'No caption')}</div>` +
        `<div class="list-item__meta">sort: ${item.sortOrder || 0} • ${item.isPublished ? 'Published' : 'Draft'}</div>` +
        '</div>'
      );
    }).join('');
    Array.from(host.querySelectorAll('[data-item-id]')).forEach((el) => {
      el.addEventListener('click', () => {
        state.selectedItemId = el.getAttribute('data-item-id') || '';
        const item = getSelectedItem();
        if (item) fillItemForm(item);
        renderItems();
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
    state.projects = await window.firebaseService.getGalleryProjects({ publishedOnly: false });
    if (!state.selectedProjectId && state.projects[0]) {
      state.selectedProjectId = state.projects[0].id;
      fillProjectForm(state.projects[0]);
    }
    renderProjects();
  }

  async function loadItems() {
    if (!state.selectedProjectId) {
      state.items = [];
      renderItems();
      return;
    }
    state.items = await window.firebaseService.getGalleryItems(state.selectedProjectId, { publishedOnly: false });
    if (state.items[0]) {
      state.selectedItemId = state.items[0].id;
      fillItemForm(state.items[0]);
    } else {
      resetItemForm();
    }
    renderItems();
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
      layoutTags: byId('project-layout-tags').value.split(',').map((s) => s.trim()).filter(Boolean),
      coverPolicy: byId('project-cover-policy').value,
      isPublished: byId('project-published').checked,
      ownerId: window.firebaseService.auth.currentUser?.uid || ''
    };
    if (!payload.title) {
      notify('Project title is required', 'warning');
      return;
    }
    if (current) {
      await window.firebaseService.updateGalleryProject(current.id, payload);
      notify('Project updated');
    } else {
      const created = await window.firebaseService.createGalleryProject(payload);
      state.selectedProjectId = created.id;
      byId('project-id').value = created.id;
      notify('Project created');
    }
    await loadProjects();
    await loadItems();
  }

  async function onDeleteProject() {
    const current = getSelectedProject();
    if (!current) return;
    const ok = confirm(`Delete project "${current.title || current.id}"?`);
    if (!ok) return;
    await window.firebaseService.deleteGalleryProject(current.id);
    resetProjectForm();
    resetItemForm();
    await loadProjects();
    await loadItems();
    notify('Project deleted');
  }

  async function onSaveItem(e) {
    e.preventDefault();
    if (!state.selectedProjectId) {
      notify('Select or create a project first', 'warning');
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
        await window.firebaseService.updateGalleryItem(state.selectedProjectId, created.id, {
          url: mediaUrl,
          thumbUrl: mediaUrl
        });
      }
      state.selectedItemId = created.id;
      notify('Item created');
    }
    await loadItems();
  }

  async function onDeleteItem() {
    const item = getSelectedItem();
    if (!item || !state.selectedProjectId) return;
    const ok = confirm(`Delete item "${item.caption || item.id}"?`);
    if (!ok) return;
    await window.firebaseService.deleteGalleryItem(state.selectedProjectId, item.id);
    resetItemForm();
    await loadItems();
    notify('Item deleted');
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
    byId('project-form').addEventListener('submit', (e) => onSaveProject(e).catch((err) => notify(err.message || String(err), 'error')));
    byId('item-form').addEventListener('submit', (e) => onSaveItem(e).catch((err) => notify(err.message || String(err), 'error')));
    byId('project-new').addEventListener('click', () => {
      resetProjectForm();
      resetItemForm();
      renderProjects();
    });
    byId('item-new').addEventListener('click', () => {
      resetItemForm();
      renderItems();
    });
    byId('project-delete').addEventListener('click', () => onDeleteProject().catch((err) => notify(err.message || String(err), 'error')));
    byId('item-delete').addEventListener('click', () => onDeleteItem().catch((err) => notify(err.message || String(err), 'error')));
  }

  async function init() {
    try {
      wireActions();
      await ensureFirebaseReady();
      await loadProjects();
      await loadItems();
      syncItemTypeFields();
    } catch (err) {
      notify(err.message || String(err), 'error');
    }
  }

  init();
})();
