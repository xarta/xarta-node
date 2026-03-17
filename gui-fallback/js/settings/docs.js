/* ── Docs tab ─────────────────────────────────────────────────────────────── */

let _docsAll       = [];   // array of DocOut records
let _docsActiveId  = null; // currently open doc_id
let _docsDirty     = false; // unsaved changes in the textarea
let _docsPreview   = false; // preview mode active

// ── Load + Sidebar ───────────────────────────────────────────────────────────

async function loadDocs() {
  try {
    const r = await apiFetch('/api/v1/docs');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    _docsAll = await r.json();
  } catch (e) {
    console.error('docs: failed to load list', e);
    _docsAll = [];
  }
  _docsRenderSidebar();
  // Re-open the active doc if we had one
  if (_docsActiveId && _docsAll.find(d => d.doc_id === _docsActiveId)) {
    _docsShowPane(_docsActiveId);
  } else if (_docsActiveId) {
    // Active doc was deleted — reset
    _docsActiveId = null;
    _docsHidePane();
  }
}

function _docsRenderSidebar() {
  const sidebar = document.getElementById('docs-sidebar');
  // Show items tagged with "menu", sorted by sort_order then label
  const menuDocs = _docsAll
    .filter(d => (d.tags || '').split(',').map(t => t.trim()).includes('menu'))
    .sort((a, b) => (a.sort_order - b.sort_order) || a.label.localeCompare(b.label));
  sidebar.innerHTML = '';
  if (!menuDocs.length) {
    sidebar.innerHTML = '<p style="font-size:12px;color:var(--text-dim);padding:4px 8px">No docs tagged "menu".</p>';
    return;
  }
  menuDocs.forEach(doc => {
    const btn = document.createElement('button');
    btn.className = 'docs-nav-item' + (doc.doc_id === _docsActiveId ? ' active' : '');
    btn.style.cssText = 'width:100%;text-align:left;padding:7px 10px;font-size:13px;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);cursor:pointer;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;transition:background 0.15s';
    if (doc.doc_id === _docsActiveId) {
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
  // Show pane, hide empty state
  document.getElementById('docs-editor-pane').hidden = false;
  document.getElementById('docs-empty-state').hidden  = true;
  // Track changes
  editor.oninput = () => { _docsDirty = true; };
}

function _docsShowPane(docId) {
  // Just refresh the pane header without re-fetching content
  const doc = _docsAll.find(d => d.doc_id === docId);
  if (!doc) return;
  document.getElementById('docs-active-label').textContent = doc.label;
  document.getElementById('docs-active-desc').textContent  = doc.description || '';
  document.getElementById('docs-editor-pane').hidden = false;
  document.getElementById('docs-empty-state').hidden  = true;
}

function _docsHidePane() {
  document.getElementById('docs-editor-pane').hidden = true;
  document.getElementById('docs-empty-state').hidden  = false;
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

// ── Preview ───────────────────────────────────────────────────────────────────

function docsTogglePreview() {
  _docsPreview = !_docsPreview;
  const btn     = document.getElementById('docs-preview-btn');
  const editor  = document.getElementById('docs-editor');
  const preview = document.getElementById('docs-preview');
  if (_docsPreview) {
    preview.innerHTML    = _mdToHtml(editor.value);
    editor.style.display = 'none';
    preview.style.display = 'block';
    btn.textContent = '\u270f Edit';
  } else {
    preview.style.display = 'none';
    editor.style.display  = 'block';
    btn.textContent = '\ud83d\udc41 Preview';
  }
}

// ── New Doc modal ─────────────────────────────────────────────────────────────

function openNewDocModal() {
  _docsModalMode('new');
  document.getElementById('docs-modal-label').value = '';
  document.getElementById('docs-modal-desc').value  = '';
  document.getElementById('docs-modal-tags').value  = 'menu';
  document.getElementById('docs-modal-path').value  = 'docs/';
  document.getElementById('docs-modal-order').value = String(_docsAll.length * 10);
  document.getElementById('docs-modal-initial').value = '';
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
  const errEl   = document.getElementById('docs-modal-error');
  if (!label) { errEl.textContent = 'Label is required.'; errEl.hidden = false; return; }
  if (!path)  { errEl.textContent = 'File path is required.'; errEl.hidden = false; return; }
  const submit = document.getElementById('docs-modal-submit');
  submit.disabled = true;
  try {
    const body = { label, description: desc || null, tags: tags || null, path, sort_order: order };
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
  const label  = document.getElementById('docs-modal-label').value.trim();
  const desc   = document.getElementById('docs-modal-desc').value.trim();
  const tags   = document.getElementById('docs-modal-tags').value.trim();
  const path   = document.getElementById('docs-modal-path').value.trim();
  const order  = parseInt(document.getElementById('docs-modal-order').value, 10) || 0;
  const errEl  = document.getElementById('docs-modal-error');
  if (!label) { errEl.textContent = 'Label is required.'; errEl.hidden = false; return; }
  const submit = document.getElementById('docs-modal-submit');
  submit.disabled = true;
  try {
    const r = await apiFetch(`/api/v1/docs/${_docsActiveId}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ label, description: desc || null, tags: tags || null, path: path || null, sort_order: order }),
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
    .replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2" style="color:var(--accent);text-decoration:underline" target="_blank" rel="noopener noreferrer">$1</a>');
}
