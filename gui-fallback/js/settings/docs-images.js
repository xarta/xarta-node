// ── Docs Images Modal ────────────────────────────────────────────────────────
// _docsImgCache  and  _docsImgBlobUrl()  are defined in docs.js (global scope)

let _docImagesAll    = [];
let _docImagesFilter = 'all'; // 'all' | 'unused'
let _docImgDescTimers = {};

function openDocImagesModal() {
  document.getElementById('doc-images-modal').style.display = 'flex';
  document.getElementById('doc-img-upload-file').value       = '';
  document.getElementById('doc-img-upload-desc').value       = '';
  _docImagesFilter = 'all';
  document.querySelectorAll('.doc-img-filter-btn').forEach(b => {
    b.classList.toggle('active', b.dataset.filter === 'all');
  });
  _loadDocImages();
}

function closeDocImagesModal() {
  document.getElementById('doc-images-modal').style.display = 'none';
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
  _docImagesFilter = f;
  document.querySelectorAll('.doc-img-filter-btn').forEach(b => {
    b.classList.toggle('active', b.dataset.filter === f);
  });
  _loadDocImages();
}

function _renderDocImagesList() {
  const list = document.getElementById('doc-images-list');
  if (!_docImagesAll.length) {
    list.innerHTML = '<p style="color:var(--text-muted);padding:12px 0">No images found.</p>';
    return;
  }
  list.innerHTML = '';
  _docImagesAll.forEach(img => list.appendChild(_buildImgRow(img)));
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
    // prepend to list
    _docImagesAll.unshift(newImg);
    const list = document.getElementById('doc-images-list');
    const row  = _buildImgRow(newImg);
    list.insertBefore(row, list.firstChild);
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
