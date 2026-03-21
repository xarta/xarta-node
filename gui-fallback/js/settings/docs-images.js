// ── Docs Images Modal ────────────────────────────────────────────────────────
// _docsImgCache  and  _docsImgBlobUrl()  are defined in docs.js (global scope)

let _docImagesAll      = [];
let _docImagesFilter   = 'all'; // 'all' | 'unused'
let _docImagesTagFilter = '';
let _docImagesPage     = 0;
const _DOC_IMG_PAGE_SIZE = 6;
let _docImgDescTimers  = {};

function openDocImagesModal() {
  document.getElementById('doc-img-upload-file').value       = '';
  document.getElementById('doc-img-upload-desc').value       = '';
  _docImagesFilter    = 'all';
  _docImagesTagFilter = '';
  _docImagesPage      = 0;
  document.querySelectorAll('.doc-img-filter-btn').forEach(b => {
    b.classList.toggle('active', b.dataset.filter === 'all');
  });
  const tagSel = document.getElementById('doc-img-tag-filter');
  if (tagSel) tagSel.value = '';
  _loadDocImages();
}

async function _loadDocImages() {
  const url = _docImagesFilter === 'unused'
    ? '/api/v1/doc-images?unused=true'
    : '/api/v1/doc-images';
  try {
    const r = await apiFetch(url);
    if (!r.ok) { _docImagesAll = []; }
    else        { _docImagesAll = await r.json(); }
  } catch { _docImagesAll = []; }
  _renderDocImagesList();
}

function _docImagesSetFilter(f) {
  _docImagesFilter    = f;
  _docImagesTagFilter = '';
  _docImagesPage      = 0;
  const tagSel = document.getElementById('doc-img-tag-filter');
  if (tagSel) tagSel.value = '';
  document.querySelectorAll('.doc-img-filter-btn').forEach(b => {
    b.classList.toggle('active', b.dataset.filter === f);
  });
  _loadDocImages();
}

function _docImagesSetTagFilter(tag) {
  _docImagesTagFilter = tag;
  _docImagesPage      = 0;
  _renderDocImagesList();
}

function _getAllTags() {
  const set = new Set();
  _docImagesAll.forEach(img => {
    if (Array.isArray(img.tags)) img.tags.forEach(t => t && set.add(t));
  });
  return [...set].sort();
}

function _renderDocImagesList() {
  const list = document.getElementById('doc-images-list');

  // Apply tag filter on top of the already-fetched list
  const items = _docImagesTagFilter
    ? _docImagesAll.filter(img => Array.isArray(img.tags) && img.tags.includes(_docImagesTagFilter))
    : _docImagesAll;

  // Refresh tag filter select with live tags
  const tagSel = document.getElementById('doc-img-tag-filter');
  if (tagSel) {
    const allTags = _getAllTags();
    const cur = _docImagesTagFilter;
    tagSel.innerHTML = '<option value="">All tags</option>';
    allTags.forEach(t => {
      const opt = document.createElement('option');
      opt.value = t; opt.textContent = t;
      if (t === cur) opt.selected = true;
      tagSel.appendChild(opt);
    });
  }

  if (!items.length) {
    list.innerHTML = '<p style="color:var(--text-muted);padding:12px 0">No images found.</p>';
    return;
  }

  // Paging
  const totalPages = Math.ceil(items.length / _DOC_IMG_PAGE_SIZE);
  if (_docImagesPage >= totalPages) _docImagesPage = Math.max(0, totalPages - 1);
  const pageItems = items.slice(
    _docImagesPage * _DOC_IMG_PAGE_SIZE,
    (_docImagesPage + 1) * _DOC_IMG_PAGE_SIZE
  );

  list.innerHTML = '';
  pageItems.forEach(img => list.appendChild(_buildImgRow(img)));

  // Pagination controls
  if (totalPages > 1) {
    const pager = document.createElement('div');
    pager.style.cssText =
      'display:flex;align-items:center;justify-content:center;gap:10px;' +
      'padding:12px 0 4px;border-top:1px solid var(--border);margin-top:4px';
    const prevBtn = document.createElement('button');
    prevBtn.className    = 'secondary';
    prevBtn.style.cssText = 'padding:3px 12px';
    prevBtn.textContent  = '‹ Prev';
    prevBtn.disabled     = _docImagesPage === 0;
    prevBtn.onclick      = () => { _docImagesPage--; _renderDocImagesList(); };
    const info = document.createElement('span');
    info.style.cssText = 'font-size:13px;color:var(--text-muted);min-width:90px;text-align:center';
    info.textContent   = `Page ${_docImagesPage + 1} of ${totalPages}`;
    const nextBtn = document.createElement('button');
    nextBtn.className    = 'secondary';
    nextBtn.style.cssText = 'padding:3px 12px';
    nextBtn.textContent  = 'Next ›';
    nextBtn.disabled     = _docImagesPage >= totalPages - 1;
    nextBtn.onclick      = () => { _docImagesPage++; _renderDocImagesList(); };
    pager.appendChild(prevBtn);
    pager.appendChild(info);
    pager.appendChild(nextBtn);
    list.appendChild(pager);
  }
}

function _buildImgRow(img) {
  const row = document.createElement('div');
  row.className  = 'doc-img-row';
  row.id         = `doc-img-row-${img.image_id}`;
  row.style.cssText =
    'display:flex;align-items:flex-start;gap:12px;padding:10px 0;' +
    'border-bottom:1px solid var(--border);flex-wrap:wrap';

  // thumbnail
  const thumb = document.createElement('img');
  thumb.alt         = img.filename;
  thumb.style.cssText = 'width:64px;height:64px;object-fit:cover;border-radius:4px;flex-shrink:0;background:var(--bg2)';
  thumb.src         = ''; // will be set async
  _loadThumb(img.image_id, thumb);

  // meta + controls
  const meta = document.createElement('div');
  meta.style.cssText = 'flex:1;min-width:0;display:flex;flex-direction:column;gap:6px';

  const nameRow = document.createElement('div');
  nameRow.style.cssText = 'display:flex;align-items:center;gap:8px;flex-wrap:wrap';
  const nameSpan = document.createElement('span');
  nameSpan.style.cssText = 'font-weight:600;word-break:break-all';
  nameSpan.textContent   = img.filename;
  const sizePill = document.createElement('span');
  sizePill.style.cssText = 'font-size:11px;color:var(--text-muted)';
  sizePill.textContent   = _fmtBytes(img.file_size || 0) + (img.created_at ? '  •  ' + img.created_at.slice(0,10) : '');
  nameRow.appendChild(nameSpan);
  nameRow.appendChild(sizePill);

  // description input
  const descInput = document.createElement('input');
  descInput.type        = 'text';
  descInput.placeholder = 'Description…';
  descInput.value       = img.description || '';
  descInput.style.cssText = 'width:100%;max-width:340px;padding:4px 8px;border-radius:4px;border:1px solid var(--border);background:var(--bg2);color:var(--text);font-size:13px';
  descInput.addEventListener('input', () => {
    clearTimeout(_docImgDescTimers[img.image_id]);
    _docImgDescTimers[img.image_id] = setTimeout(
      () => _saveDocImgDesc(img.image_id, descInput.value), 800
    );
  });

  // tags input + known-tag pills
  const tagsWrap = document.createElement('div');
  tagsWrap.style.cssText = 'display:flex;flex-direction:column;gap:4px';
  const tagsInput = document.createElement('input');
  tagsInput.type        = 'text';
  tagsInput.placeholder = 'Tags (comma-separated)…';
  tagsInput.value       = Array.isArray(img.tags) ? img.tags.join(', ') : '';
  tagsInput.style.cssText = 'width:100%;max-width:340px;padding:4px 8px;border-radius:4px;border:1px solid var(--border);background:var(--bg2);color:var(--text);font-size:13px';
  tagsInput.addEventListener('input', () => {
    clearTimeout(_docImgDescTimers['tags_' + img.image_id]);
    _docImgDescTimers['tags_' + img.image_id] = setTimeout(
      () => _saveDocImgTags(img.image_id, tagsInput.value), 800
    );
  });
  tagsWrap.appendChild(tagsInput);
  const knownTags = _getAllTags();
  if (knownTags.length) {
    const pills = document.createElement('div');
    pills.style.cssText = 'display:flex;flex-wrap:wrap;gap:4px';
    knownTags.forEach(tag => {
      const pill = document.createElement('button');
      pill.className    = 'secondary';
      pill.style.cssText = 'font-size:10px;padding:1px 7px;border-radius:10px';
      pill.textContent  = tag;
      pill.onclick = () => {
        const current = tagsInput.value.split(',').map(t => t.trim()).filter(Boolean);
        if (!current.includes(tag)) {
          current.push(tag);
          tagsInput.value = current.join(', ');
          clearTimeout(_docImgDescTimers['tags_' + img.image_id]);
          _docImgDescTimers['tags_' + img.image_id] = setTimeout(
            () => _saveDocImgTags(img.image_id, tagsInput.value), 800
          );
        }
      };
      pills.appendChild(pill);
    });
    tagsWrap.appendChild(pills);
  }

  // markdown snippet row
  const snipRow = document.createElement('div');
  snipRow.style.cssText = 'display:flex;align-items:center;gap:6px;flex-wrap:wrap';
  const snip = `/api/v1/doc-images/${img.image_id}/file`;
  const mdSnip = `![${img.filename}](${snip})`;
  const snipCode = document.createElement('code');
  snipCode.style.cssText =
    'font-size:11px;padding:2px 6px;background:var(--bg2);border-radius:3px;' +
    'word-break:break-all;color:var(--text-muted);max-width:280px;display:inline-block;overflow:hidden;white-space:nowrap;text-overflow:ellipsis';
  snipCode.title       = mdSnip;
  snipCode.textContent = mdSnip;
  const copyBtn = document.createElement('button');
  copyBtn.className   = 'secondary';
  copyBtn.style.cssText = 'font-size:11px;padding:2px 8px';
  copyBtn.textContent = '📋 Copy';
  copyBtn.onclick     = () => {
    navigator.clipboard.writeText(mdSnip);
    copyBtn.textContent = '✓ Copied';
    setTimeout(() => { copyBtn.textContent = '📋 Copy'; }, 1500);
  };
  snipRow.appendChild(snipCode);
  snipRow.appendChild(copyBtn);

  // delete button
  const delContainer = document.createElement('div');
  const delBtn = document.createElement('button');
  delBtn.className   = 'danger';
  delBtn.style.cssText = 'font-size:12px;padding:3px 10px';
  delBtn.textContent = '🗑 Delete';
  delBtn.onclick     = () => _showDocImgDeleteConfirm(delContainer, img);
  delContainer.appendChild(delBtn);

  meta.appendChild(nameRow);
  meta.appendChild(descInput);
  meta.appendChild(tagsWrap);
  meta.appendChild(snipRow);
  meta.appendChild(delContainer);

  row.appendChild(thumb);
  row.appendChild(meta);
  return row;
}

async function _loadThumb(imageId, imgEl) {
  const url = await _docsImgBlobUrl(imageId);
  if (url) imgEl.src = url;
}

function _showDocImgDeleteConfirm(container, img) {
  container.innerHTML = '';
  const note = document.createElement('span');
  note.style.cssText  = 'font-size:12px;color:var(--text-muted);margin-right:6px';
  note.textContent    = 'Delete:';
  const recBtn = document.createElement('button');
  recBtn.className    = 'danger';
  recBtn.style.cssText = 'font-size:12px;padding:3px 10px;margin-right:4px';
  recBtn.textContent  = 'Record only';
  recBtn.onclick      = () => _doDeleteDocImg(img, false);
  const fileBtn = document.createElement('button');
  fileBtn.className   = 'danger';
  fileBtn.style.cssText = 'font-size:12px;padding:3px 10px;margin-right:4px';
  fileBtn.textContent = '+ File';
  fileBtn.onclick     = () => _doDeleteDocImg(img, true);
  const cancelBtn = document.createElement('button');
  cancelBtn.className = 'secondary';
  cancelBtn.style.cssText = 'font-size:12px;padding:3px 10px';
  cancelBtn.textContent   = '✗';
  cancelBtn.onclick       = () => {
    container.innerHTML = '';
    const delBtn = document.createElement('button');
    delBtn.className   = 'danger';
    delBtn.style.cssText = 'font-size:12px;padding:3px 10px';
    delBtn.textContent = '🗑 Delete';
    delBtn.onclick     = () => _showDocImgDeleteConfirm(container, img);
    container.appendChild(delBtn);
  };
  container.appendChild(note);
  container.appendChild(recBtn);
  container.appendChild(fileBtn);
  container.appendChild(cancelBtn);
}

async function _doDeleteDocImg(img, deleteFile) {
  const url = deleteFile
    ? `/api/v1/doc-images/${img.image_id}?delete_file=true`
    : `/api/v1/doc-images/${img.image_id}`;
  const r = await apiFetch(url, { method: 'DELETE' });
  if (!r.ok) { alert('Delete failed'); return; }
  if (deleteFile && _docsImgCache[img.image_id]) {
    URL.revokeObjectURL(_docsImgCache[img.image_id]);
    delete _docsImgCache[img.image_id];
  }
  const row = document.getElementById(`doc-img-row-${img.image_id}`);
  if (row) row.remove();
  _docImagesAll = _docImagesAll.filter(i => i.image_id !== img.image_id);
  if (!_docImagesAll.length) _renderDocImagesList();
}

async function _saveDocImgDesc(imageId, description) {
  await apiFetch(`/api/v1/doc-images/${imageId}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ description }),
  });
}

async function _saveDocImgTags(imageId, tagsStr) {
  const tags = tagsStr.split(',').map(t => t.trim()).filter(Boolean);
  const img = _docImagesAll.find(i => i.image_id === imageId);
  if (img) img.tags = tags;
  await apiFetch(`/api/v1/doc-images/${imageId}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ tags }),
  });
  // Refresh tag filter select to pick up any newly created tags
  const tagSel = document.getElementById('doc-img-tag-filter');
  if (tagSel) {
    const allTags = _getAllTags();
    const cur = tagSel.value;
    tagSel.innerHTML = '<option value="">All tags</option>';
    allTags.forEach(t => {
      const opt = document.createElement('option');
      opt.value = t; opt.textContent = t;
      if (t === cur) opt.selected = true;
      tagSel.appendChild(opt);
    });
  }
}

async function submitDocImageUpload() {
  const fileInput = document.getElementById('doc-img-upload-file');
  const desc      = document.getElementById('doc-img-upload-desc').value.trim();
  const statusEl  = document.getElementById('doc-img-upload-status');
  if (!fileInput.files.length) {
    statusEl.textContent = 'No file selected.'; return;
  }
  const fd = new FormData();
  fd.append('file', fileInput.files[0]);
  fd.append('description', desc);
  statusEl.textContent = 'Uploading…';
  try {
    const r = await apiFetch('/api/v1/doc-images', { method: 'POST', body: fd });
    if (!r.ok) {
      const err = await r.json().catch(() => ({}));
      statusEl.textContent = 'Error: ' + (err.detail || r.status);
      return;
    }
    const newImg = await r.json();
    statusEl.textContent = '✓ Uploaded';
    fileInput.value  = '';
    document.getElementById('doc-img-upload-desc').value = '';
    // copy snippet to clipboard automatically
    const mdSnip = `![${newImg.filename}](/api/v1/doc-images/${newImg.image_id}/file)`;
    navigator.clipboard.writeText(mdSnip).catch(() => {});
    // prepend to list and re-render respecting paging
    _docImagesAll.unshift(newImg);
    _docImagesPage = 0;
    _renderDocImagesList();
    setTimeout(() => { statusEl.textContent = ''; }, 3000);
  } catch (e) {
    statusEl.textContent = 'Error: ' + e.message;
  }
}

function _fmtBytes(b) {
  if (b < 1024) return b + ' B';
  if (b < 1048576) return (b / 1024).toFixed(1) + ' KB';
  return (b / 1048576).toFixed(1) + ' MB';
}
