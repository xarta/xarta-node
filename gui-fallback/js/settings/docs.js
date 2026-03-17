/* ── Docs tab ─────────────────────────────────────────────────────────────── */

let _docsAll       = [];   // array of DocOut records
let _docsActiveId  = null; // currently open doc_id
let _docsDirty     = false; // unsaved changes in the textarea
let _docsPreview   = false; // preview mode active

// ── List view state ──────────────────────────────────────────────────────────
let _docsGroups    = [];   // array of DocGroupOut records
let _docsListView  = false; // true = list view is visible
let _docsDragId    = null;  // doc_id currently being dragged
let _groupDragId   = null;  // group_id currently being dragged

// ── Load + Sidebar ───────────────────────────────────────────────────────────

async function _docsLoadGroups() {
  try {
    const r = await apiFetch('/api/v1/doc-groups');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    _docsGroups = await r.json();
  } catch (e) {
    console.error('doc-groups: failed to load', e);
    _docsGroups = [];
  }
}

async function loadDocs() {
  await Promise.all([
    apiFetch('/api/v1/docs').then(r => r.ok ? r.json() : []).then(d => { _docsAll = d; }).catch(() => { _docsAll = []; }),
    _docsLoadGroups(),
  ]);
  _docsRenderSidebar();
  // Re-open the active doc if we had one
  if (_docsActiveId && _docsAll.find(d => d.doc_id === _docsActiveId)) {
    _docsShowPane(_docsActiveId);
  } else if (_docsActiveId) {
    // Active doc was deleted — reset
    _docsActiveId = null;
    _docsHidePane();
  }
  // Refresh list view if it's currently visible
  if (_docsListView) _docsRenderList();
}

function _docsRenderSidebar() {
  const sidebar = document.getElementById('docs-sidebar');
  // Show items tagged with "menu", sorted by sort_order then label
  const menuDocs = _docsAll
    .filter(d => (d.tags || '').split(',').map(t => t.trim()).includes('menu'))
    .sort((a, b) => (a.sort_order - b.sort_order) || a.label.localeCompare(b.label));
  sidebar.innerHTML = '';
  if (!menuDocs.length) {
    sidebar.innerHTML = '<span style="font-size:12px;color:var(--text-dim)">No docs tagged "menu".</span>';
    return;
  }
  menuDocs.forEach(doc => {
    const btn = document.createElement('button');
    const isActive = doc.doc_id === _docsActiveId;
    btn.className = 'secondary' + (isActive ? ' active' : '');
    btn.style.cssText = 'padding:4px 12px;font-size:13px;white-space:nowrap';
    if (isActive) {
      btn.style.background = 'var(--accent)';
      btn.style.color = '#fff';
      btn.style.borderColor = 'var(--accent)';
    }
    btn.title = doc.description || doc.label;
    btn.textContent = doc.label;
    btn.onclick = () => docsSelectDoc(doc.doc_id);
    sidebar.appendChild(btn);
  });
}

// ── Selection / auto-save ────────────────────────────────────────────────────

async function docsSelectDoc(docId) {
  if (docId === _docsActiveId) return; // already open
  // Auto-save dirty content before switching
  if (_docsDirty && _docsActiveId) {
    await docsSave(true /* silent */);
  }
  _docsActiveId = docId;
  _docsPreview = false;
  _docsRenderSidebar();
  await _docsOpenDoc(docId);
}

async function _docsOpenDoc(docId) {
  const errEl = document.getElementById('docs-error');
  errEl.hidden = true;
  _docsHidePane();
  try {
    const r = await apiFetch(`/api/v1/docs/${docId}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const doc = await r.json();
    _docsFillPane(doc);
    _docsDirty = false;
  } catch (e) {
    errEl.textContent = `Failed to load document: ${e.message}`;
    errEl.hidden = false;
  }
}

function _docsFillPane(doc) {
  document.getElementById('docs-active-label').textContent = doc.label;
  document.getElementById('docs-active-desc').textContent  = doc.description || '';
  const editor  = document.getElementById('docs-editor');
  const preview = document.getElementById('docs-preview');
  editor.value = doc.content || '';
  // Reset to edit mode
  editor.style.display  = 'block';
  preview.style.display = 'none';
  _docsPreview = false;
  document.getElementById('docs-preview-btn').textContent = '\ud83d\udc41 Preview';
  document.getElementById('docs-status').hidden = true;
  // Track changes
  editor.oninput = () => { _docsDirty = true; };
}

function _docsShowPane(docId) {
  // Just refresh the pane header without re-fetching content
  const doc = _docsAll.find(d => d.doc_id === docId);
  if (!doc) return;
  document.getElementById('docs-active-label').textContent = doc.label;
  document.getElementById('docs-active-desc').textContent  = doc.description || '';
}

function _docsHidePane() {
  document.getElementById('docs-active-label').textContent = '';
  document.getElementById('docs-active-desc').textContent  = '';
  document.getElementById('docs-editor').value = '';
  document.getElementById('docs-preview').innerHTML = '';
  document.getElementById('docs-preview').style.display = 'none';
  document.getElementById('docs-editor').style.display = 'block';
  _docsPreview = false;
  document.getElementById('docs-preview-btn').textContent = '\ud83d\udc41 Preview';
}

// ── Save ─────────────────────────────────────────────────────────────────────

async function docsSave(silent = false) {
  if (!_docsActiveId) return;
  const btn    = document.getElementById('docs-save-btn');
  const status = document.getElementById('docs-status');
  const editor = document.getElementById('docs-editor');
  btn.disabled = true;
  if (!silent) { status.hidden = true; }
  try {
    const r = await apiFetch(`/api/v1/docs/${_docsActiveId}/content`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ content: editor.value }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    _docsDirty = false;
    if (!silent) {
      status.textContent = '\u2713 Saved';
      status.style.color = 'var(--accent)';
      status.hidden = false;
      setTimeout(() => { status.hidden = true; }, 3000);
    }
  } catch (e) {
    if (!silent) {
      status.textContent = `\u2717 Save failed: ${e.message}`;
      status.style.color = '#f87171';
      status.hidden = false;
    }
  } finally {
    btn.disabled = false;
  }
}

async function docsRefreshContent() {
  if (!_docsActiveId) return;
  if (_docsDirty) {
    if (!confirm('You have unsaved changes. Discard and reload from disk?')) return;
  }
  _docsDirty = false;
  await _docsOpenDoc(_docsActiveId);
}

// ── Image blob URL cache (shared with docs-images.js) ────────────────────────

const _docsImgCache = {};

async function _docsImgBlobUrl(imageId) {
  if (_docsImgCache[imageId]) return _docsImgCache[imageId];
  try {
    const r = await apiFetch(`/api/v1/doc-images/${imageId}/file`);
    if (!r.ok) return null;
    const blob = await r.blob();
    _docsImgCache[imageId] = URL.createObjectURL(blob);
    return _docsImgCache[imageId];
  } catch { return null; }
}

// ── Preview ───────────────────────────────────────────────────────────────────

async function docsTogglePreview() {
  _docsPreview = !_docsPreview;
  const btn     = document.getElementById('docs-preview-btn');
  const editor  = document.getElementById('docs-editor');
  const preview = document.getElementById('docs-preview');
  if (_docsPreview) {
    await _docsRenderPreview();
    editor.style.display  = 'none';
    preview.style.display = 'block';
    btn.textContent = '\u270f Edit';
  } else {
    preview.style.display = 'none';
    editor.style.display  = 'block';
    btn.textContent = '\ud83d\udc41 Preview';
  }
}

async function _docsRenderPreview() {
  const preview = document.getElementById('docs-preview');
  preview.innerHTML = _mdToHtml(document.getElementById('docs-editor').value);
  for (const img of preview.querySelectorAll('img[data-doc-img]')) {
    const url = await _docsImgBlobUrl(img.dataset.docImg);
    if (url) img.src = url;
  }
}

// ── New Doc modal ─────────────────────────────────────────────────────────────

function _docsPopulateGroupSelect(currentGroupId) {
  const sel = document.getElementById('docs-modal-group');
  if (!sel) return;
  sel.innerHTML = '<option value="">— Undefined Group —</option>';
  _docsGroups.forEach(g => {
    const opt = document.createElement('option');
    opt.value = g.group_id;
    opt.textContent = g.name;
    if (g.group_id === currentGroupId) opt.selected = true;
    sel.appendChild(opt);
  });
}

function openNewDocModal() {
  _docsModalMode('new');
  document.getElementById('docs-modal-label').value = '';
  document.getElementById('docs-modal-desc').value  = '';
  document.getElementById('docs-modal-tags').value  = 'menu';
  document.getElementById('docs-modal-path').value  = 'docs/';
  document.getElementById('docs-modal-order').value = String(_docsAll.length * 10);
  document.getElementById('docs-modal-initial').value = '';
  _docsPopulateGroupSelect(null);
  document.getElementById('docs-modal').showModal();
}

// ── Edit doc metadata modal ───────────────────────────────────────────────────

function openEditDocModal() {
  if (!_docsActiveId) return;
  const doc = _docsAll.find(d => d.doc_id === _docsActiveId);
  if (!doc) return;
  _docsModalMode('edit');
  document.getElementById('docs-modal-label').value = doc.label;
  document.getElementById('docs-modal-desc').value  = doc.description || '';
  document.getElementById('docs-modal-tags').value  = doc.tags || '';
  document.getElementById('docs-modal-path').value  = doc.path;
  document.getElementById('docs-modal-order').value = String(doc.sort_order);
  document.getElementById('docs-modal-initial').value = '';
  _docsPopulateGroupSelect(doc.group_id || null);
  document.getElementById('docs-modal').showModal();
}

function _docsModalMode(mode) {
  const title   = document.getElementById('docs-modal-title');
  const initRow = document.getElementById('docs-modal-init-row');
  const submit  = document.getElementById('docs-modal-submit');
  if (mode === 'new') {
    title.textContent   = 'New Document';
    initRow.style.display = '';
    submit.textContent  = 'Create';
    submit.onclick      = _docsModalSubmit;
  } else {
    title.textContent   = 'Edit Document Metadata';
    initRow.style.display = 'none';
    submit.textContent  = 'Save';
    submit.onclick      = _docsModalSubmitEdit;
  }
  document.getElementById('docs-modal-error').textContent = '';
  document.getElementById('docs-modal-error').hidden = true;
}

async function _docsModalSubmit() {
  const label   = document.getElementById('docs-modal-label').value.trim();
  const desc    = document.getElementById('docs-modal-desc').value.trim();
  const tags    = document.getElementById('docs-modal-tags').value.trim();
  const path    = document.getElementById('docs-modal-path').value.trim();
  const order   = parseInt(document.getElementById('docs-modal-order').value, 10) || 0;
  const initial = document.getElementById('docs-modal-initial').value;
  const groupId = document.getElementById('docs-modal-group')?.value || '';
  const errEl   = document.getElementById('docs-modal-error');
  if (!label) { errEl.textContent = 'Label is required.'; errEl.hidden = false; return; }
  if (!path)  { errEl.textContent = 'File path is required.'; errEl.hidden = false; return; }
  const submit = document.getElementById('docs-modal-submit');
  submit.disabled = true;
  try {
    const body = { label, description: desc || null, tags: tags || null, path, sort_order: order, group_id: groupId || null };
    if (initial) body.initial_content = initial;
    const r = await apiFetch('/api/v1/docs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!r.ok) { const d = await r.json().catch(() => ({})); throw new Error(d.detail || `HTTP ${r.status}`); }
    const created = await r.json();
    document.getElementById('docs-modal').close();
    await loadDocs();
    docsSelectDoc(created.doc_id);
  } catch (e) {
    errEl.textContent = `Error: ${e.message}`;
    errEl.hidden = false;
  } finally {
    submit.disabled = false;
  }
}

async function _docsModalSubmitEdit() {
  const label   = document.getElementById('docs-modal-label').value.trim();
  const desc    = document.getElementById('docs-modal-desc').value.trim();
  const tags    = document.getElementById('docs-modal-tags').value.trim();
  const path    = document.getElementById('docs-modal-path').value.trim();
  const order   = parseInt(document.getElementById('docs-modal-order').value, 10) || 0;
  const groupId = document.getElementById('docs-modal-group')?.value ?? '';
  const errEl   = document.getElementById('docs-modal-error');
  if (!label) { errEl.textContent = 'Label is required.'; errEl.hidden = false; return; }
  const submit = document.getElementById('docs-modal-submit');
  submit.disabled = true;
  try {
    const r = await apiFetch(`/api/v1/docs/${_docsActiveId}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ label, description: desc || null, tags: tags || null, path: path || null, sort_order: order, group_id: groupId }),
    });
    if (!r.ok) { const d = await r.json().catch(() => ({})); throw new Error(d.detail || `HTTP ${r.status}`); }
    document.getElementById('docs-modal').close();
    await loadDocs();
  } catch (e) {
    errEl.textContent = `Error: ${e.message}`;
    errEl.hidden = false;
  } finally {
    submit.disabled = false;
  }
}

// ── Delete confirmation ───────────────────────────────────────────────────────

function openDeleteDocModal() {
  if (!_docsActiveId) return;
  const doc = _docsAll.find(d => d.doc_id === _docsActiveId);
  if (!doc) return;
  document.getElementById('docs-delete-name').textContent = doc.label;
  document.getElementById('docs-delete-path').textContent = doc.path;
  document.getElementById('docs-delete-file-chk').checked = false;
  document.getElementById('docs-delete-error').hidden = true;
  document.getElementById('docs-delete-modal').showModal();
}

async function submitDeleteDoc() {
  const deleteFile = document.getElementById('docs-delete-file-chk').checked;
  const errEl = document.getElementById('docs-delete-error');
  const btn   = document.getElementById('docs-delete-confirm-btn');
  btn.disabled = true;
  try {
    const url = `/api/v1/docs/${_docsActiveId}${deleteFile ? '?delete_file=true' : ''}`;
    const r = await apiFetch(url, { method: 'DELETE' });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    document.getElementById('docs-delete-modal').close();
    _docsActiveId = null;
    _docsHidePane();
    await loadDocs();
  } catch (e) {
    errEl.textContent = `Error: ${e.message}`;
    errEl.hidden = false;
  } finally {
    btn.disabled = false;
  }
}

// ── List view toggle ──────────────────────────────────────────────────────────

async function docsToggleList() {
  _docsListView = !_docsListView;
  const btn = document.getElementById('docs-list-btn');
  const editorPane = document.getElementById('docs-editor-pane');
  const listPane   = document.getElementById('docs-list-pane');
  if (_docsListView) {
    await loadDocs();
    editorPane.style.display = 'none';
    listPane.style.display   = 'flex';
    _docsRenderList();
    if (btn) { btn.style.background = 'var(--accent)'; btn.style.color = '#fff'; btn.style.borderColor = 'var(--accent)'; }
  } else {
    listPane.style.display   = 'none';
    editorPane.style.display = 'flex';
    if (btn) { btn.style.background = ''; btn.style.color = ''; btn.style.borderColor = ''; }
  }
}

function _docsListClose() {
  if (!_docsListView) return;
  _docsListView = false;
  document.getElementById('docs-list-pane').style.display   = 'none';
  document.getElementById('docs-editor-pane').style.display = 'flex';
  const btn = document.getElementById('docs-list-btn');
  if (btn) { btn.style.background = ''; btn.style.color = ''; btn.style.borderColor = ''; }
}

// ── List view rendering ───────────────────────────────────────────────────────

function _docsRenderList() {
  const pane = document.getElementById('docs-list-pane');
  if (!pane) return;
  pane.innerHTML = '';

  const header = document.createElement('div');
  header.style.cssText = 'display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;flex-shrink:0';
  header.innerHTML = `
    <span style="font-size:12px;color:var(--text-dim)">Drag documents to reorder or move between groups. Drag group headings to reorder groups.</span>
    <button onclick="docsListAddGroup()">+ Add Group</button>
  `;
  pane.appendChild(header);

  const container = document.createElement('div');
  container.id = 'docs-list-container';
  container.style.cssText = 'display:flex;flex-direction:column;gap:10px;flex:1';
  pane.appendChild(container);

  const sortedGroups = [..._docsGroups].sort((a, b) => (a.sort_order - b.sort_order) || a.name.localeCompare(b.name));
  sortedGroups.forEach(g => container.appendChild(_docsRenderGroupBlock(g)));
  container.appendChild(_docsRenderGroupBlock(null)); // Undefined Group always last
}

function _docsRenderGroupBlock(group) {
  const isUndefined = group === null;
  const groupId     = group ? group.group_id : null;
  const groupName   = group ? group.name : 'Undefined Group';

  const block = document.createElement('div');
  block.className = 'docs-group-block';
  block.dataset.groupId = groupId || '__undefined__';
  block.style.cssText = 'background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);overflow:hidden';

  // ── Group header ──
  const groupHdr = document.createElement('div');
  groupHdr.style.cssText = 'display:flex;align-items:center;gap:6px;padding:7px 10px;background:var(--bg2,#16161e);border-bottom:1px solid var(--border);font-weight:600;font-size:13px;min-height:34px';

  if (!isUndefined) {
    groupHdr.draggable = true;
    groupHdr.style.cursor = 'grab';
    groupHdr.ondragstart = e => {
      _groupDragId = groupId;
      e.dataTransfer.effectAllowed = 'move';
      block.style.opacity = '0.5';
    };
    groupHdr.ondragend = () => {
      _groupDragId = null;
      block.style.opacity = '';
      document.querySelectorAll('.docs-group-block').forEach(b => b.style.outline = '');
    };
    groupHdr.innerHTML = `
      <span style="color:var(--text-dim);font-size:15px;user-select:none">≡</span>
      <span style="flex:1">${esc(groupName)}</span>
      <button class="secondary" style="padding:2px 8px;font-size:12px" onclick="docsListEditGroup('${groupId}','${esc(groupName)}')">✎</button>
      <button class="secondary" style="padding:2px 8px;font-size:12px;color:#f87171" onclick="docsListDeleteGroup('${groupId}','${esc(groupName)}')">🗑</button>
    `;
  } else {
    groupHdr.innerHTML = `<span style="color:var(--accent);margin-right:4px">📁</span><span style="color:var(--text-dim);font-style:italic">Undefined Group</span>`;
  }

  // ── Group drag-over (groups reordering) ──
  block.ondragover = e => {
    if (_groupDragId && _groupDragId !== groupId) {
      e.preventDefault();
      block.style.outline = '2px solid var(--accent)';
    } else if (_docsDragId) {
      e.preventDefault();
    }
  };
  block.ondragleave = e => {
    if (!block.contains(e.relatedTarget)) block.style.outline = '';
  };
  block.ondrop = e => {
    e.preventDefault();
    block.style.outline = '';
    if (_groupDragId && _groupDragId !== groupId && !isUndefined) {
      _docsDropGroupBefore(_groupDragId, groupId);
    }
    // doc drops are handled by the inner docsList zone
  };

  block.appendChild(groupHdr);

  // ── Docs list zone ──
  const docsList = document.createElement('div');
  docsList.dataset.groupId = groupId || '__undefined__';
  docsList.style.cssText = 'display:flex;flex-direction:column;min-height:30px';

  const groupDocs = _docsAll
    .filter(d => isUndefined ? !d.group_id : d.group_id === groupId)
    .sort((a, b) => (a.sort_order - b.sort_order) || a.label.localeCompare(b.label));

  if (groupDocs.length === 0) {
    const empty = document.createElement('div');
    empty.style.cssText = 'padding:8px 14px;font-size:12px;color:var(--text-dim);font-style:italic';
    empty.textContent = 'No documents — drop one here';
    docsList.appendChild(empty);
  } else {
    groupDocs.forEach(doc => docsList.appendChild(_docsRenderDocRow(doc)));
  }

  // Drop on the empty area of the docs zone → move to end of this group
  docsList.ondragover = e => {
    if (_docsDragId) {
      e.preventDefault();
      docsList.style.background = 'rgba(99,102,241,.07)';
    }
  };
  docsList.ondragleave = e => {
    if (!docsList.contains(e.relatedTarget)) docsList.style.background = '';
  };
  docsList.ondrop = e => {
    e.stopPropagation();
    docsList.style.background = '';
    if (_docsDragId) _docsDropDocOnGroup(_docsDragId, groupId);
  };

  block.appendChild(docsList);
  return block;
}

function _docsRenderDocRow(doc) {
  const row = document.createElement('div');
  row.dataset.docId = doc.doc_id;
  row.draggable = true;
  row.style.cssText = 'display:flex;align-items:center;gap:8px;padding:5px 10px;border-bottom:1px solid rgba(255,255,255,.04);cursor:default;transition:background .1s';

  row.addEventListener('mouseenter', () => { row.style.background = 'rgba(255,255,255,.03)'; });
  row.addEventListener('mouseleave', () => { row.style.background = ''; });

  row.ondragstart = e => {
    _docsDragId = doc.doc_id;
    e.dataTransfer.effectAllowed = 'move';
    setTimeout(() => { row.style.opacity = '0.4'; }, 0);
  };
  row.ondragend = () => {
    _docsDragId = null;
    row.style.opacity = '';
    document.querySelectorAll('.docs-doc-dropline').forEach(el => el.style.borderTop = '');
  };
  // Drop before this doc
  row.ondragover = e => {
    if (_docsDragId && _docsDragId !== doc.doc_id) {
      e.preventDefault();
      e.stopPropagation();
      row.style.borderTop = '2px solid var(--accent)';
    }
  };
  row.ondragleave = () => { row.style.borderTop = ''; };
  row.ondrop = e => {
    e.preventDefault();
    e.stopPropagation();
    row.style.borderTop = '';
    if (_docsDragId && _docsDragId !== doc.doc_id) _docsDropDocBeforeDoc(_docsDragId, doc.doc_id);
  };
  row.className = 'docs-doc-dropline';

  row.innerHTML = `
    <span style="color:var(--text-dim);font-size:15px;user-select:none;cursor:grab">≡</span>
    <span style="flex:1;font-size:13px">${esc(doc.label)}</span>
    ${doc.description ? `<span style="font-size:11px;color:var(--text-dim);max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(doc.description)}</span>` : ''}
    <button class="secondary" style="padding:2px 8px;font-size:12px;flex-shrink:0" onclick="docsListOpenDoc('${doc.doc_id}')">Open</button>
  `;
  return row;
}

// ── List actions ──────────────────────────────────────────────────────────────

function docsListOpenDoc(docId) {
  _docsListClose();
  docsSelectDoc(docId);
}

async function docsListAddGroup() {
  const name = prompt('Group name:');
  if (!name || !name.trim()) return;
  // Sort order = current max + 10
  const maxSort = _docsGroups.length > 0 ? Math.max(..._docsGroups.map(g => g.sort_order)) : -10;
  try {
    const r = await apiFetch('/api/v1/doc-groups', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: name.trim(), sort_order: maxSort + 10 }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    await loadDocs();
  } catch (e) { alert(`Failed to add group: ${e.message}`); }
}

async function docsListEditGroup(groupId, currentName) {
  const name = prompt('Rename group:', currentName);
  if (name === null || name === currentName) return;
  if (!name.trim()) { alert('Group name cannot be empty.'); return; }
  try {
    const r = await apiFetch(`/api/v1/doc-groups/${groupId}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: name.trim() }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    await loadDocs();
  } catch (e) { alert(`Failed to rename group: ${e.message}`); }
}

async function docsListDeleteGroup(groupId, name) {
  const docsInGroup = _docsAll.filter(d => d.group_id === groupId).length;
  const msg = docsInGroup > 0
    ? `Delete group "${name}"? The ${docsInGroup} document(s) in it will move to Undefined Group.`
    : `Delete group "${name}"?`;
  if (!confirm(msg)) return;
  try {
    const r = await apiFetch(`/api/v1/doc-groups/${groupId}`, { method: 'DELETE' });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    await loadDocs();
  } catch (e) { alert(`Failed to delete group: ${e.message}`); }
}

// ── Drag & drop handlers ──────────────────────────────────────────────────────

async function _docsDropDocOnGroup(draggedDocId, targetGroupId) {
  const dragged = _docsAll.find(d => d.doc_id === draggedDocId);
  if (!dragged) return;
  if ((dragged.group_id || null) === targetGroupId) return; // already in this group, no-op (for header drops)
  const groupDocs = _docsAll.filter(d => targetGroupId ? d.group_id === targetGroupId : !d.group_id);
  const maxSort   = groupDocs.length > 0 ? Math.max(...groupDocs.map(d => d.sort_order)) : -10;
  try {
    await apiFetch(`/api/v1/docs/${draggedDocId}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sort_order: maxSort + 10, group_id: targetGroupId || '' }),
    });
    await loadDocs();
  } catch (e) { console.error('docs drop on group failed', e); }
}

async function _docsDropDocBeforeDoc(draggedDocId, targetDocId) {
  const dragged = _docsAll.find(d => d.doc_id === draggedDocId);
  const target  = _docsAll.find(d => d.doc_id === targetDocId);
  if (!dragged || !target) return;

  const targetGroupId = target.group_id || null;
  // All docs in target's group except the dragged one, sorted
  const peers = _docsAll
    .filter(d => d.doc_id !== draggedDocId && ((targetGroupId ? d.group_id === targetGroupId : !d.group_id)))
    .sort((a, b) => (a.sort_order - b.sort_order) || a.label.localeCompare(b.label));

  // Insert dragged before target
  const insertIdx = peers.findIndex(d => d.doc_id === targetDocId);
  if (insertIdx === -1) peers.push(dragged);
  else peers.splice(insertIdx, 0, dragged);

  // Assign new sort_orders 0, 10, 20, ...
  const updates = peers.map((d, i) => ({ doc_id: d.doc_id, sort_order: i * 10, group_id: targetGroupId }));
  const changed = updates.filter(u => {
    const orig = _docsAll.find(d => d.doc_id === u.doc_id);
    return !orig || orig.sort_order !== u.sort_order || (orig.group_id || null) !== u.group_id;
  });

  try {
    await Promise.all(changed.map(u => apiFetch(`/api/v1/docs/${u.doc_id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sort_order: u.sort_order, group_id: u.group_id || '' }),
    })));
    await loadDocs();
  } catch (e) { console.error('docs reorder failed', e); }
}

async function _docsDropGroupBefore(draggedGroupId, targetGroupId) {
  const dragged = _docsGroups.find(g => g.group_id === draggedGroupId);
  const target  = _docsGroups.find(g => g.group_id === targetGroupId);
  if (!dragged || !target) return;

  const groups = [..._docsGroups]
    .filter(g => g.group_id !== draggedGroupId)
    .sort((a, b) => (a.sort_order - b.sort_order) || a.name.localeCompare(b.name));

  const insertIdx = groups.findIndex(g => g.group_id === targetGroupId);
  if (insertIdx === -1) groups.push(dragged);
  else groups.splice(insertIdx, 0, dragged);

  const updates = groups.map((g, i) => ({ group_id: g.group_id, sort_order: i * 10 }));
  const changed = updates.filter(u => {
    const orig = _docsGroups.find(g => g.group_id === u.group_id);
    return !orig || orig.sort_order !== u.sort_order;
  });

  try {
    await Promise.all(changed.map(u => apiFetch(`/api/v1/doc-groups/${u.group_id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sort_order: u.sort_order }),
    })));
    await loadDocs();
  } catch (e) { console.error('group reorder failed', e); }
}

// ── Markdown renderer (shared) ────────────────────────────────────────────────

function _mdToHtml(md) {
  // Minimal but functional markdown renderer — no external deps
  const esc2 = s => s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  const lines = md.split('\n');
  let html = '';
  let inUl = false, inOl = false, inCode = false, codeLang = '', codeBuf = '';
  const closeList = () => {
    if (inUl) { html += '</ul>'; inUl = false; }
    if (inOl) { html += '</ol>'; inOl = false; }
  };
  for (let i = 0; i < lines.length; i++) {
    let line = lines[i];
    if (!inCode && /^```/.test(line)) {
      closeList();
      inCode = true; codeLang = line.slice(3).trim(); codeBuf = ''; continue;
    }
    if (inCode) {
      if (/^```/.test(line)) {
        const langAttr = codeLang ? ` class="language-${esc2(codeLang)}"` : '';
        html += `<pre style="background:var(--bg);border:1px solid var(--border);border-radius:var(--radius);padding:12px 14px;overflow-x:auto;font-size:12.5px;line-height:1.5"><code${langAttr}>${esc2(codeBuf)}</code></pre>`;
        inCode = false; codeBuf = '';
      } else { codeBuf += line + '\n'; }
      continue;
    }
    const hm = line.match(/^(#{1,6})\s+(.*)/);
    if (hm) {
      closeList();
      const lvl = hm[1].length;
      const sizes   = ['1.6em','1.35em','1.15em','1em','0.95em','0.9em'];
      const margins = ['24px 0 10px','20px 0 8px','16px 0 6px','14px 0 4px','12px 0 4px','10px 0 4px'];
      html += `<h${lvl} style="font-size:${sizes[lvl-1]};font-weight:700;margin:${margins[lvl-1]};color:var(--text);border-bottom:${lvl<=2?'1px solid var(--border)':'none'};padding-bottom:${lvl<=2?'6px':'0'}">${_inlineMd(hm[2])}</h${lvl}>`;
      continue;
    }
    if (/^(\*{3,}|-{3,}|_{3,})$/.test(line.trim())) {
      closeList();
      html += '<hr style="border:none;border-top:1px solid var(--border);margin:16px 0">';
      continue;
    }
    const ulm = line.match(/^(\s*)[-*+]\s+(.*)/);
    if (ulm) {
      if (!inUl) { closeList(); html += '<ul style="margin:6px 0 6px 20px;padding:0">'; inUl = true; }
      html += `<li style="margin:3px 0">${_inlineMd(ulm[2])}</li>`;
      continue;
    }
    const olm = line.match(/^(\s*)\d+\.\s+(.*)/);
    if (olm) {
      if (!inOl) { closeList(); html += '<ol style="margin:6px 0 6px 20px;padding:0">'; inOl = true; }
      html += `<li style="margin:3px 0">${_inlineMd(olm[2])}</li>`;
      continue;
    }
    const bqm = line.match(/^>\s*(.*)/);
    if (bqm) {
      closeList();
      html += `<blockquote style="margin:8px 0;padding:8px 14px;border-left:3px solid var(--accent);background:var(--surface);color:var(--text-dim);font-style:italic">${_inlineMd(bqm[1])}</blockquote>`;
      continue;
    }
    closeList();
    if (!line.trim()) { html += '<div style="height:8px"></div>'; continue; }
    html += `<p style="margin:4px 0">${_inlineMd(line)}</p>`;
  }
  if (inCode) html += `<pre style="background:var(--bg);border:1px solid var(--border);border-radius:var(--radius);padding:12px 14px;overflow-x:auto"><code>${esc2(codeBuf)}</code></pre>`;
  closeList();
  return html;
}

function _inlineMd(s) {
  return s
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
    .replace(/`([^`]+)`/g, (_,c) => `<code style="background:var(--surface);border:1px solid var(--border);border-radius:3px;padding:1px 5px;font-size:0.88em">${c.replace(/&lt;/g,'<').replace(/&gt;/g,'>')}</code>`)
    .replace(/\*\*\*([^*]+)\*\*\*/g, '<strong><em>$1</em></strong>')
    .replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>')
    .replace(/__([^_]+)__/g, '<strong>$1</strong>')
    .replace(/\*([^*]+)\*/g, '<em>$1</em>')
    .replace(/_([^_]+)_/g, '<em>$1</em>')
    .replace(/~~([^~]+)~~/g, '<del>$1</del>')
    .replace(/!\[([^\]]*)\]\(([^)]+)\)/g, (_, alt, src) => {
      const m = src.match(/\/api\/v1\/doc-images\/([a-f0-9-]+)\/file/);
      return `<img${m ? ` data-doc-img="${m[1]}"` : ''} src="${src}" alt="${alt}" style="max-width:100%;border-radius:4px;margin:8px 0;display:block" />`;
    })
    .replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2" style="color:var(--accent);text-decoration:underline" target="_blank" rel="noopener noreferrer">$1</a>');
}
