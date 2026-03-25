/* ── Docs tab ─────────────────────────────────────────────────────────────── */

let _docsAll       = [];   // array of DocOut records
let _docsActiveId  = null; // currently open doc_id
let _docsDirty     = false; // unsaved changes in the textarea
let _docsPreview   = false; // preview mode active for current doc (kept in sync with _docsViewModes)
const _docsViewModes = {}; // doc_id → true (preview) | false (edit); default preview on first open

// ── List view state ──────────────────────────────────────────────────────────
let _docsGroups    = [];   // array of DocGroupOut records
let _docsDragId    = null;  // doc_id currently being dragged
let _groupDragId   = null;  // group_id currently being dragged
let _docsCurrentModalMode = 'new'; // 'new' | 'edit'

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
  if (typeof docsHistInit === 'function') docsHistInit(_docsActiveId);
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
  // Restore the most recent history doc when opening Docs tab on a fresh UI state.
  if (!_docsActiveId && typeof docsHistCurrent === 'function') {
    const histDocId = docsHistCurrent();
    if (histDocId && _docsAll.find(d => d.doc_id === histDocId)) {
      _docsActiveId = histDocId;
      if (!(histDocId in _docsViewModes)) _docsViewModes[histDocId] = true;
      _docsPreview = _docsViewModes[histDocId];
      _docsRenderSidebar();
      await _docsOpenDoc(histDocId);
    }
  }
  // Refresh list view if it's currently the active tab
  if (document.getElementById('tab-docs-list')?.classList.contains('active')) _docsRenderList();
  _docsUpdateHistoryButtons();
}

function _docsLabelById(docId) {
  const doc = _docsAll.find(d => d.doc_id === docId);
  return doc ? doc.label : null;
}

function _docsUpdateHistoryButtons() {
  const backBtn = document.getElementById('docs-back-btn');
  const fwdBtn = document.getElementById('docs-forward-btn');
  const hint = document.getElementById('docs-history-hint');
  if (!backBtn || !fwdBtn || !hint) return;

  const canBack = typeof docsHistCanBack === 'function' ? docsHistCanBack() : false;
  const canFwd = typeof docsHistCanForward === 'function' ? docsHistCanForward() : false;
  const backId = typeof docsHistPeekBack === 'function' ? docsHistPeekBack() : null;
  const fwdId = typeof docsHistPeekForward === 'function' ? docsHistPeekForward() : null;
  const stats = typeof docsHistStats === 'function' ? docsHistStats() : { back: 0, forward: 0 };

  backBtn.disabled = !canBack;
  fwdBtn.disabled = !canFwd;
  backBtn.title = canBack ? `Back to ${_docsLabelById(backId) || 'previous doc'}` : 'No previous doc in history';
  fwdBtn.title = canFwd ? `Forward to ${_docsLabelById(fwdId) || 'next doc'}` : 'No forward doc in history';

  const parts = [];
  if (canBack) parts.push(`Back: ${_docsLabelById(backId) || 'doc'}`);
  if (canFwd) parts.push(`Forward: ${_docsLabelById(fwdId) || 'doc'}`);
  const countText = `(${stats.back}/${stats.forward})`;
  hint.textContent = parts.length ? `${parts.join(' • ')} ${countText}` : `History idle ${countText}`;
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

async function docsSelectDoc(docId, opts = {}) {
  const fromHistory = !!opts.fromHistory;
  const force = !!opts.force;
  if (!force && docId === _docsActiveId) {
    _docsUpdateHistoryButtons();
    return true; // already open
  }
  // Auto-save dirty content before switching
  if (_docsDirty && _docsActiveId) {
    await docsSave(true /* silent */);
  }
  _docsActiveId = docId;
  // Default to preview on first open; restore last-used mode on revisits
  if (!(docId in _docsViewModes)) _docsViewModes[docId] = true;
  _docsPreview = _docsViewModes[docId];
  _docsRenderSidebar();
  const ok = await _docsOpenDoc(docId);
  if (ok && !fromHistory && typeof docsHistRecordDirect === 'function') {
    docsHistRecordDirect(docId);
  }
  _docsUpdateHistoryButtons();
  return ok;
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
    return true;
  } catch (e) {
    errEl.textContent = `Failed to load document: ${e.message}`;
    errEl.hidden = false;
    return false;
  }
}

async function docsHistoryBack() {
  if (!(typeof docsHistStepBack === 'function')) return;
  const target = docsHistStepBack();
  if (!target) {
    _docsUpdateHistoryButtons();
    return;
  }
  const ok = await docsSelectDoc(target, { fromHistory: true, force: true });
  if (!ok && typeof docsHistRemoveDoc === 'function') {
    docsHistRemoveDoc(target);
    _docsUpdateHistoryButtons();
  }
}

async function docsHistoryForward() {
  if (!(typeof docsHistStepForward === 'function')) return;
  const target = docsHistStepForward();
  if (!target) {
    _docsUpdateHistoryButtons();
    return;
  }
  const ok = await docsSelectDoc(target, { fromHistory: true, force: true });
  if (!ok && typeof docsHistRemoveDoc === 'function') {
    docsHistRemoveDoc(target);
    _docsUpdateHistoryButtons();
  }
}

function _docsFillPane(doc) {
  document.getElementById('docs-active-label').textContent = doc.label;
  document.getElementById('docs-active-desc').textContent  = doc.description || '';
  const editor  = document.getElementById('docs-editor');
  const preview = document.getElementById('docs-preview');
  editor.value = doc.content || '';
  document.getElementById('docs-status').hidden = true;
  // Apply stored view mode (default: preview)
  if (_docsPreview) {
    // Render preview immediately
    preview.innerHTML = _mdToHtml(editor.value);
    _docsResolvePrevImgs(preview);
    editor.style.display  = 'none';
    preview.style.display = 'block';
    const pb1 = document.getElementById('docs-preview-btn');
    if (pb1) pb1.textContent = '✏ Edit';
  } else {
    editor.style.display  = 'block';
    preview.style.display = 'none';
    const pb2 = document.getElementById('docs-preview-btn');
    if (pb2) pb2.textContent = '👁 Preview';
  }
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
  // Note: do NOT reset _docsPreview here — per-doc view mode is managed by _docsViewModes
  const pb = document.getElementById('docs-preview-btn');
  if (pb) pb.textContent = '👁 Preview';
  // Note: do NOT clear _docsViewModes here — keep per-doc memory across hide/show cycles
}

// ── Save ─────────────────────────────────────────────────────────────────────

async function docsSave(silent = false) {
  if (!_docsActiveId) return;
  const btn    = document.getElementById('docs-save-btn');
  const status = document.getElementById('docs-status');
  const editor = document.getElementById('docs-editor');
  if (btn) btn.disabled = true;
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
    if (btn) btn.disabled = false;
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
  // Remember the choice for this doc
  if (_docsActiveId) _docsViewModes[_docsActiveId] = _docsPreview;
  const btn     = document.getElementById('docs-preview-btn');
  const editor  = document.getElementById('docs-editor');
  const preview = document.getElementById('docs-preview');
  if (_docsPreview) {
    await _docsRenderPreview();
    editor.style.display  = 'none';
    preview.style.display = 'block';
    if (btn) btn.textContent = '✏ Edit';
  } else {
    preview.style.display = 'none';
    editor.style.display  = 'block';
    if (btn) btn.textContent = '👁 Preview';
  }
}

async function _docsRenderPreview() {
  const preview = document.getElementById('docs-preview');
  preview.innerHTML = _mdToHtml(document.getElementById('docs-editor').value);
  await _docsResolvePrevImgs(preview);
}

async function _docsResolvePrevImgs(preview) {
  for (const img of preview.querySelectorAll('img[data-doc-img]')) {
    const url = await _docsImgBlobUrl(img.dataset.docImg);
    if (url) img.src = url;
  }
}

// ── New Doc modal ─────────────────────────────────────────────────────────────

function _docsPopulateGroupSelect(currentGroupId, selectId = 'docs-modal-group') {
  const sel = document.getElementById(selectId);
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
  HubModal.open(document.getElementById('docs-modal'));
  setTimeout(() => document.getElementById('docs-modal-label').focus(), 50);
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
  HubModal.open(document.getElementById('docs-modal'));
  setTimeout(() => document.getElementById('docs-modal-label').focus(), 50);
}

function _docsModalMode(mode) {
  _docsCurrentModalMode = mode;
  const title   = document.getElementById('docs-modal-title');
  const initRow = document.getElementById('docs-modal-init-row');
  const submit  = document.getElementById('docs-modal-submit');
  if (mode === 'new') {
    title.textContent     = 'New Document';
    initRow.style.display = '';
    submit.textContent    = 'Create';
  } else {
    title.textContent     = 'Edit Document Metadata';
    initRow.style.display = 'none';
    submit.textContent    = 'Save';
  }
  document.getElementById('docs-modal-error').textContent = '';
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
  if (!label) { errEl.textContent = 'Label is required.'; return; }
  if (!path)  { errEl.textContent = 'File path is required.'; return; }
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
    HubModal.close(document.getElementById('docs-modal'));
    await loadDocs();
    docsSelectDoc(created.doc_id);
  } catch (e) {
    errEl.textContent = `Error: ${e.message}`;
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
  if (!label) { errEl.textContent = 'Label is required.'; return; }
  const submit = document.getElementById('docs-modal-submit');
  submit.disabled = true;
  try {
    const r = await apiFetch(`/api/v1/docs/${_docsActiveId}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ label, description: desc || null, tags: tags || null, path: path || null, sort_order: order, group_id: groupId }),
    });
    if (!r.ok) { const d = await r.json().catch(() => ({})); throw new Error(d.detail || `HTTP ${r.status}`); }
    HubModal.close(document.getElementById('docs-modal'));
    await loadDocs();
  } catch (e) {
    errEl.textContent = `Error: ${e.message}`;
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
  const deletingDocId = _docsActiveId;
  const deleteFile = document.getElementById('docs-delete-file-chk').checked;
  const errEl = document.getElementById('docs-delete-error');
  const btn   = document.getElementById('docs-delete-confirm-btn');
  btn.disabled = true;
  try {
    const url = `/api/v1/docs/${_docsActiveId}${deleteFile ? '?delete_file=true' : ''}`;
    const r = await apiFetch(url, { method: 'DELETE' });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    document.getElementById('docs-delete-modal').close();
    if (typeof docsHistRemoveDoc === 'function') {
      _docsActiveId = docsHistRemoveDoc(deletingDocId) || null;
    } else {
      _docsActiveId = null;
    }
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

// ── List view rendering ───────────────────────────────────────────────────────

function _docsRenderList() {
  const pane = document.getElementById('docs-list-pane');
  if (!pane) return;
  pane.innerHTML = '';

  const header = document.createElement('div');
  header.style.cssText = 'display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;flex-shrink:0';
  header.innerHTML = `
    <span style="font-size:12px;color:var(--text-dim)">Drag documents to reorder or move between groups. Drag group headings to reorder groups.</span>
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
    ${doc.description ? `<span style="font-size:11px;color:var(--text-dim);flex:2;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(doc.description)}</span>` : ''}
    <button class="secondary" style="padding:2px 8px;font-size:12px;flex-shrink:0" onclick="docsListCopyLink('${doc.doc_id}')" title="Copy Markdown link">🔗 Copy Link</button>
    <button class="secondary" style="padding:2px 8px;font-size:12px;flex-shrink:0;color:#f87171" onclick="docsListDeleteDoc('${doc.doc_id}')" title="Delete document">🗑</button>
    <button class="secondary" style="padding:2px 8px;font-size:12px;flex-shrink:0" onclick="docsListOpenDoc('${doc.doc_id}')">Open</button>
  `;
  return row;
}

// ── List actions ──────────────────────────────────────────────────────────────

function docsListCopyLink(docId) {
  const doc = _docsAll.find(d => d.doc_id === docId);
  if (!doc) return;
  const base = doc.path ? doc.path.split('/').pop() : '';
  const md = `[${doc.label}](${base})`;
  navigator.clipboard.writeText(md).then(() => {
    const status = document.getElementById('docs-status');
    if (status) {
      status.textContent = `\u2713 Copied: ${md}`;
      status.style.color = 'var(--accent)';
      status.hidden = false;
      setTimeout(() => { status.hidden = true; }, 3000);
    }
  });
}

function docsListDeleteDoc(docId) {
  // Route through the existing delete modal — temporarily set active doc
  const prev = _docsActiveId;
  _docsActiveId = docId;
  openDeleteDocModal();
  // If user cancels the modal the active doc may be wrong, so patch the modal's
  // cancel button to restore it
  const modal = document.getElementById('docs-delete-modal');
  const cancelHandler = () => {
    if (_docsActiveId === docId && docId !== prev) _docsActiveId = prev;
    modal.removeEventListener('close', cancelHandler);
  };
  modal.addEventListener('close', cancelHandler);
}

function docsListOpenDoc(docId) {
  switchTab('docs');
  SettingsMenuConfig.updateActiveTab('docs');
  if (docId === _docsActiveId) {
    // docsSelectDoc would bail early — force a fresh load instead
    _docsOpenDoc(docId);
  } else {
    docsSelectDoc(docId);
  }
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
  const _isTableRow = s => /^\s*\|.*\|\s*$/.test(s);
  const _isTableSeparator = s => /^\s*\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)+\|?\s*$/.test(s);
  const _tableCells = s => {
    const core = s.trim().replace(/^\|/, '').replace(/\|$/, '');
    return core.split('|').map(c => c.trim());
  };
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

    // GFM-style tables
    if (_isTableRow(line) && i + 1 < lines.length && _isTableSeparator(lines[i + 1])) {
      closeList();
      const headerCells = _tableCells(line);
      i += 1; // Skip separator row
      const bodyRows = [];
      while (i + 1 < lines.length && _isTableRow(lines[i + 1])) {
        i += 1;
        bodyRows.push(_tableCells(lines[i]));
      }

      let tableHtml = '<div style="overflow-x:auto;margin:10px 0"><table style="width:100%;border-collapse:collapse;font-size:13px">';
      tableHtml += '<thead><tr>';
      for (const cell of headerCells) {
        tableHtml += `<th style="text-align:left;padding:7px 9px;border:1px solid var(--border);background:var(--bg)">${_inlineMd(cell)}</th>`;
      }
      tableHtml += '</tr></thead><tbody>';
      for (const row of bodyRows) {
        tableHtml += '<tr>';
        for (let c = 0; c < headerCells.length; c++) {
          tableHtml += `<td style="padding:7px 9px;border:1px solid var(--border)">${_inlineMd(row[c] || '')}</td>`;
        }
        tableHtml += '</tr>';
      }
      tableHtml += '</tbody></table></div>';
      html += tableHtml;
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
    .replace(/\[([^\]]+)\]\(([^)]+)\)/g, (_, text, href) => {
      // Internal doc link: relative .md path that isn't an absolute URL
      const isDocLink = href && !href.match(/^https?:\/\/|^\/\//) && /\.md$/i.test(href);
      if (isDocLink) {
        const safeHref = href.replace(/\\/g,'\\\\').replace(/'/g,"\\'");
        return `<a href="#" onclick="docsOpenByPath('${safeHref}'); return false;" style="color:var(--accent);text-decoration:underline;text-decoration-style:dashed" title="Open: ${href}">${text}</a>`;
      }
      return `<a href="${href}" style="color:var(--accent);text-decoration:underline" target="_blank" rel="noopener noreferrer">${text}</a>`;
    })
    // Auto-link bare https:// URLs not already inside an HTML attribute value
    .replace(/(?<![="'])https?:\/\/[^\s<>")\]]+/g, url => {
      const m = url.match(/^(.+?)([.,;:!?)]+)$/);
      const href  = m ? m[1] : url;
      const trail = m ? m[2] : '';
      return `<a href="${href}" style="color:var(--accent);text-decoration:underline" target="_blank" rel="noopener noreferrer">${href}</a>${trail}`;
    });
}

// Navigate to a doc by relative path or basename (called from inline preview links)
function docsOpenByPath(href) {
  const base = href.split('/').pop().toLowerCase();
  const doc = _docsAll.find(d => {
    if (!d.path) return false;
    return d.path.toLowerCase() === href.toLowerCase() ||
           d.path.split('/').pop().toLowerCase() === base;
  });
  if (!doc) {
    const status = document.getElementById('docs-status');
    if (status) {
      status.textContent = `\u2717 Doc not found: ${href}`;
      status.style.color = '#f87171';
      status.hidden = false;
      setTimeout(() => { status.hidden = true; }, 4000);
    }
    return;
  }
  if (doc.doc_id === _docsActiveId) {
    _docsOpenDoc(doc.doc_id);
  } else {
    docsSelectDoc(doc.doc_id);
  }
}

// ── Add existing document modal ─────────────────────────────────────────────

let _addDocAllFiles  = []; // all unregistered paths from API
let _addDocFiltered  = []; // currently filtered subset

async function openAddDocModal() {
  // Reset form fields
  document.getElementById('add-doc-filter').value      = '';
  document.getElementById('add-doc-modal-label').value = '';
  document.getElementById('add-doc-modal-desc').value  = '';
  document.getElementById('add-doc-modal-tags').value  = 'menu';
  document.getElementById('add-doc-modal-order').value = String(_docsAll.length * 10);
  document.getElementById('add-doc-modal-error').textContent = '';
  _docsPopulateGroupSelect(null, 'add-doc-modal-group');

  const fileList  = document.getElementById('add-doc-file-list');
  const countEl   = document.getElementById('add-doc-file-count');
  fileList.innerHTML = '<option disabled value="">Loading…</option>';
  countEl.textContent = '';
  _addDocAllFiles = [];
  _addDocFiltered = [];

  HubModal.open(document.getElementById('add-doc-modal'));

  try {
    const r = await apiFetch('/api/v1/docs/unregistered');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    _addDocAllFiles = await r.json();
  } catch (e) {
    fileList.innerHTML = '<option disabled value="">Failed to load files</option>';
    document.getElementById('add-doc-modal-error').textContent = `Error loading files: ${e.message}`;
    return;
  }

  _addDocApplyFilter();
  setTimeout(() => document.getElementById('add-doc-filter').focus(), 50);
}

function _addDocApplyFilter() {
  const term = (document.getElementById('add-doc-filter').value || '').toLowerCase();
  _addDocFiltered = _addDocAllFiles.filter(p => !term || p.toLowerCase().includes(term));

  const sel  = document.getElementById('add-doc-file-list');
  const prev = sel.value;
  sel.innerHTML = '';
  _addDocFiltered.forEach(p => {
    const opt = document.createElement('option');
    opt.value = p;
    opt.textContent = p;
    sel.appendChild(opt);
  });
  if (prev && _addDocFiltered.includes(prev)) sel.value = prev;

  const shown = _addDocFiltered.length;
  const total = _addDocAllFiles.length;
  const countEl = document.getElementById('add-doc-file-count');
  if (total === 0) {
    countEl.textContent = 'No unregistered .md files found.';
  } else if (shown === total) {
    countEl.textContent = `${total} unregistered file${total === 1 ? '' : 's'}`;
  } else {
    countEl.textContent = `${shown} of ${total} files`;
  }
}

function _addDocAutoFillLabel(path) {
  if (!path) return;
  const labelEl = document.getElementById('add-doc-modal-label');
  const basename = path.split('/').pop().replace(/\.[^.]+$/, '');
  labelEl.value = basename.replace(/[-_]/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

async function _addDocSubmit() {
  const fileList = document.getElementById('add-doc-file-list');
  const path     = fileList.value;
  const label    = document.getElementById('add-doc-modal-label').value.trim();
  const desc     = document.getElementById('add-doc-modal-desc').value.trim();
  const tags     = document.getElementById('add-doc-modal-tags').value.trim();
  const order    = parseInt(document.getElementById('add-doc-modal-order').value, 10) || 0;
  const groupId  = document.getElementById('add-doc-modal-group')?.value || '';
  const errEl    = document.getElementById('add-doc-modal-error');

  if (!path)  { errEl.textContent = 'Please select a file from the list.'; return; }
  if (!label) { errEl.textContent = 'Label is required.'; return; }

  const submit = document.getElementById('add-doc-modal-submit');
  submit.disabled = true;
  errEl.textContent = '';
  try {
    // Do NOT send initial_content — we're registering an existing file, not creating one.
    const body = { label, description: desc || null, tags: tags || null, path, sort_order: order, group_id: groupId || null };
    const r = await apiFetch('/api/v1/docs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!r.ok) { const d = await r.json().catch(() => ({})); throw new Error(d.detail || `HTTP ${r.status}`); }
    const created = await r.json();
    HubModal.close(document.getElementById('add-doc-modal'));
    await loadDocs();
    docsSelectDoc(created.doc_id);
  } catch (e) {
    errEl.textContent = `Error: ${e.message}`;
  } finally {
    submit.disabled = false;
  }
}

document.addEventListener('DOMContentLoaded', () => {
  // New / Edit doc modal submit
  const docsSubmitBtn = document.getElementById('docs-modal-submit');
  if (docsSubmitBtn) {
    docsSubmitBtn.addEventListener('click', () => {
      if (_docsCurrentModalMode === 'new') _docsModalSubmit();
      else _docsModalSubmitEdit();
    });
  }

  // Add existing doc modal wiring
  const addDocFilter = document.getElementById('add-doc-filter');
  if (addDocFilter) addDocFilter.addEventListener('input', _addDocApplyFilter);

  const addDocList = document.getElementById('add-doc-file-list');
  if (addDocList) addDocList.addEventListener('change', () => _addDocAutoFillLabel(addDocList.value));

  const addDocSubmit = document.getElementById('add-doc-modal-submit');
  if (addDocSubmit) addDocSubmit.addEventListener('click', _addDocSubmit);
});
