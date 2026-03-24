// form-controls.js — Form Controls management page for Blueprints Settings
// xarta-node Blueprints GUI
//
// Manages the form_controls table: assign sound/icon assets to any form control
// (input, select, toggle, button, etc.) identified by a data-fc-key string.
//
// Pattern: each uniquely-identified interactive element in the GUI can carry a
// data-fc-key attribute.  That key is registered here with a sound and/or icon
// asset, drawn from the same shared /fallback-ui/assets/ pool as Nav Items.
//
// Bulk upload: refer to the Nav Items page — uploads made there go into the
// same shared assets folder and are immediately available here.

'use strict';

let _fcItems = [];

// ── Bootstrap ─────────────────────────────────────────────────────────────────

async function loadFormControls() {
    const statusEl = document.getElementById('fc-status');
    try {
        const resp = await apiFetch('/api/v1/form-controls');
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        _fcItems = await resp.json();
        renderFormControls();
        if (statusEl) statusEl.textContent = '';
    } catch (e) {
        if (statusEl) {
            statusEl.textContent = `✗ ${e.message}`;
            statusEl.style.color = 'var(--danger,#f85149)';
        }
    }
}

// ── Table rendering ───────────────────────────────────────────────────────────

function renderFormControls() {
    const container = document.getElementById('fc-container');
    if (!container) return;
    container.innerHTML = '';

    // Add New button row
    const toolbar = document.createElement('div');
    toolbar.className = 'fc-toolbar';
    const addBtn = document.createElement('button');
    addBtn.className = 'btn-small';
    addBtn.textContent = '➕ Add Control Key';
    addBtn.addEventListener('click', _fcAddNew);
    toolbar.appendChild(addBtn);

    const uploadHint = document.createElement('p');
    uploadHint.className = 'fc-upload-hint';
    uploadHint.innerHTML =
        '💡 <strong>To upload new icon or sound files</strong>, use the ' +
        '<a href="#" id="fc-goto-nav-items">Nav Items</a> page — ' +
        'both pages share the same asset folder.';
    uploadHint.querySelector('#fc-goto-nav-items').addEventListener('click', (e) => {
        e.preventDefault();
        if (typeof window.switchTab === 'function') window.switchTab('nav-items');
    });
    toolbar.appendChild(uploadHint);
    container.appendChild(toolbar);

    if (!_fcItems.length) {
        const empty = document.createElement('p');
        empty.style.cssText = 'font-size:13px;color:var(--text-dim);padding:12px 0;margin:0';
        empty.textContent = 'No form controls registered yet. Click "Add Control Key" to define the first one.';
        container.appendChild(empty);
        return;
    }

    const table = document.createElement('table');
    table.className = 'data-table fc-table';
    table.innerHTML = `
        <thead>
            <tr>
                <th>Icon</th>
                <th>Control Key</th>
                <th>Label</th>
                <th>Type</th>
                <th>Context</th>
                <th>Icon Asset</th>
                <th>Sound (On / Off)</th>
                <th>Notes</th>
                <th>Actions</th>
            </tr>
        </thead>
        <tbody id="fc-tbody"></tbody>
    `;
    container.appendChild(table);

    const tbody = table.querySelector('#fc-tbody');
    const sorted = [..._fcItems].sort((a, b) =>
        (a.label || a.control_key).localeCompare(b.label || b.control_key)
    );
    for (const item of sorted) {
        tbody.appendChild(_fcRowEl(item));
    }
}

// ── Icon resolution ───────────────────────────────────────────────────────────

function _resolvedIconHtml(item) {
    if (item.icon_asset) {
        const ts = item.updated_at ? new Date(item.updated_at).getTime() : 0;
        return `<img class="fc-icon-preview menu-icon"
            src="/fallback-ui/assets/${_esc(item.icon_asset)}?v=${ts}"
            alt="" data-fallback="1">`;
    }
    return `<span class="fc-icon-placeholder" title="No icon assigned">—</span>`;
}

// ── Row builder ───────────────────────────────────────────────────────────────

function _fcRowEl(item) {
    const tr = document.createElement('tr');
    tr.dataset.controlId = item.control_id;

    const typeLabel = item.control_type || '';

    tr.innerHTML = `
        <td class="fc-col-icon">${_resolvedIconHtml(item)}</td>
        <td class="fc-col-key"><code>${_esc(item.control_key)}</code></td>
        <td class="fc-col-label"><span class="fc-field" data-field="label">${_esc(item.label)}</span></td>
        <td class="fc-col-type"><span class="fc-field" data-field="control_type">${_esc(typeLabel)}</span></td>
        <td class="fc-col-context"><span class="fc-field" data-field="context">${_esc(item.context || '')}</span></td>
        <td class="fc-col-icon-asset">
            <span class="fc-field fc-asset-path" data-field="icon_asset">${_esc(item.icon_asset || '')}</span>
            <div class="fc-asset-actions">
                <label class="btn-small secondary fc-upload-label" title="Upload new icon">
                    ⬆ <input type="file" class="fc-file-input"
                        data-control-id="${item.control_id}" data-asset-type="icons"
                        accept=".svg,.png,.ico,.jpg,.jpeg,.webp" style="display:none">
                </label>
                <button class="btn-small secondary fc-browse-btn"
                    data-control-id="${item.control_id}" data-asset-type="icons"
                    title="Choose from existing icons">📋</button>
                ${item.icon_asset
                    ? `<button class="btn-small secondary fc-clear-asset"
                           data-control-id="${item.control_id}" data-field="icon_asset"
                           title="Clear icon">✕</button>`
                    : ''}
            </div>
        </td>
        <td class="fc-col-sound-asset">
            <div class="fc-sound-slot">
                <small style="color:var(--text-dim);font-size:10px">On:</small>
                <span class="fc-field fc-asset-path" data-field="sound_asset">${_esc(item.sound_asset || '')}</span>
                <div class="fc-asset-actions">
                    <label class="btn-small secondary fc-upload-label" title="Upload on-sound">
                        ⬆ <input type="file" class="fc-file-input"
                            data-control-id="${item.control_id}" data-asset-type="sounds"
                            accept=".wav,.mp3,.ogg,.flac,.webm,.m4a" style="display:none">
                    </label>
                    <button class="btn-small secondary fc-browse-btn"
                        data-control-id="${item.control_id}" data-asset-type="sounds"
                        title="Choose on-sound">📋</button>
                    ${item.sound_asset
                        ? `<button class="btn-small secondary fc-sound-play"
                               data-sound-path="${_esc(item.sound_asset)}"
                               data-updated="${_esc(item.updated_at || '')}"
                               title="Preview on-sound">▶</button>`
                        : ''}
                    ${item.sound_asset
                        ? `<button class="btn-small secondary fc-clear-asset"
                               data-control-id="${item.control_id}" data-field="sound_asset"
                               title="Clear on-sound">✕</button>`
                        : ''}
                </div>
            </div>
            <div class="fc-sound-slot" style="margin-top:4px">
                <small style="color:var(--text-dim);font-size:10px">Off:</small>
                <span class="fc-field fc-asset-path" data-field="sound_asset_off">${_esc(item.sound_asset_off || '')}</span>
                <div class="fc-asset-actions">
                    <label class="btn-small secondary fc-upload-label" title="Upload off-sound">
                        ⬆ <input type="file" class="fc-file-input"
                            data-control-id="${item.control_id}" data-asset-type="sounds_off"
                            accept=".wav,.mp3,.ogg,.flac,.webm,.m4a" style="display:none">
                    </label>
                    <button class="btn-small secondary fc-browse-btn"
                        data-control-id="${item.control_id}" data-asset-type="sounds_off"
                        title="Choose off-sound">📋</button>
                    ${item.sound_asset_off
                        ? `<button class="btn-small secondary fc-sound-play"
                               data-sound-path="${_esc(item.sound_asset_off)}"
                               data-updated="${_esc(item.updated_at || '')}"
                               title="Preview off-sound">▶</button>`
                        : ''}
                    ${item.sound_asset_off
                        ? `<button class="btn-small secondary fc-clear-asset"
                               data-control-id="${item.control_id}" data-field="sound_asset_off"
                               title="Clear off-sound">✕</button>`
                        : ''}
                </div>
            </div>
        </td>
        <td class="fc-col-notes"><span class="fc-field" data-field="notes">${_esc(item.notes || '')}</span></td>
        <td class="fc-col-actions">
            <button class="btn-small secondary fc-edit-btn"
                data-control-id="${item.control_id}" title="Edit">✏️</button>
            <button class="btn-small secondary fc-delete-btn"
                data-control-id="${item.control_id}" title="Delete">🗑</button>
        </td>
    `;

    // File upload inputs
    tr.querySelectorAll('.fc-file-input').forEach(inp => {
        inp.addEventListener('change', () => _fcUploadAsset(inp));
    });

    // Browse existing assets
    tr.querySelectorAll('.fc-browse-btn').forEach(btn => {
        btn.addEventListener('click', () => _fcOpenPicker(btn.dataset.controlId, btn.dataset.assetType));
    });

    // Sound preview
    tr.querySelectorAll('.fc-sound-play').forEach(btn => {
        btn.addEventListener('click', () => {
            const path = btn.dataset.soundPath;
            const ts   = btn.dataset.updated ? new Date(btn.dataset.updated).getTime() : 0;
            if (path && typeof SoundManager !== 'undefined') {
                SoundManager.preview(`/fallback-ui/assets/${path}?v=${ts}`);
            }
        });
    });

    // Clear asset buttons
    tr.querySelectorAll('.fc-clear-asset').forEach(btn => {
        btn.addEventListener('click', () => _fcClearAsset(btn.dataset.controlId, btn.dataset.field));
    });

    // Edit button
    tr.querySelectorAll('.fc-edit-btn').forEach(btn => {
        btn.addEventListener('click', () => _fcEditRow(btn.dataset.controlId));
    });

    // Delete button
    tr.querySelectorAll('.fc-delete-btn').forEach(btn => {
        btn.addEventListener('click', () => _fcDeleteRow(btn.dataset.controlId));
    });

    // Icon img error fallback
    tr.querySelectorAll('.fc-icon-preview').forEach(img => {
        img.addEventListener('error', function () {
            if (!this.dataset.usedFallback) {
                this.dataset.usedFallback = '1';
                this.src = '/fallback-ui/assets/icons/fallback.svg';
            }
        });
    });

    return tr;
}

// ── Add new control ───────────────────────────────────────────────────────────

async function _fcAddNew() {
    const key = prompt('Control key (e.g. bookmarks.filter.archived):');
    if (!key || !key.trim()) return;
    const label = prompt('Label (human-readable name):');
    if (!label || !label.trim()) return;
    const controlType = prompt('Control type (input / select / toggle / button / checkbox / range / textarea — leave blank to skip):') || '';
    const context = prompt('Context (where this control appears — leave blank to skip):') || '';

    const statusEl = document.getElementById('fc-status');
    try {
        const resp = await apiFetch('/api/v1/form-controls', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                control_key:  key.trim(),
                label:        label.trim(),
                control_type: controlType.trim() || null,
                context:      context.trim() || null,
            }),
        });
        if (!resp.ok) {
            const err = await resp.json().catch(() => ({ detail: `HTTP ${resp.status}` }));
            throw new Error(err.detail || `HTTP ${resp.status}`);
        }
        const created = await resp.json();
        _fcItems.push(created);
        renderFormControls();
        if (typeof FormControlManager !== 'undefined') FormControlManager.reload();
        if (statusEl) { statusEl.textContent = `✓ Created "${created.label}"`; statusEl.style.color = 'var(--ok,#3fb950)'; }
    } catch (e) {
        if (statusEl) { statusEl.textContent = `✗ ${e.message}`; statusEl.style.color = 'var(--danger,#f85149)'; }
    }
}

// ── Inline edit dialog ────────────────────────────────────────────────────────

async function _fcEditRow(controlId) {
    const item = _fcItems.find(i => i.control_id === controlId);
    if (!item) return;

    const label       = prompt('Label:', item.label);
    if (label === null) return;
    const controlType = prompt('Control type:', item.control_type || '');
    if (controlType === null) return;
    const context     = prompt('Context:', item.context || '');
    if (context === null) return;
    const notes       = prompt('Notes:', item.notes || '');
    if (notes === null) return;

    const statusEl = document.getElementById('fc-status');
    try {
        const resp = await apiFetch(`/api/v1/form-controls/${controlId}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                label:        label.trim() || item.label,
                control_type: controlType.trim() || null,
                context:      context.trim()  || null,
                notes:        notes.trim()    || null,
            }),
        });
        if (!resp.ok) {
            const err = await resp.json().catch(() => ({ detail: `HTTP ${resp.status}` }));
            throw new Error(err.detail || `HTTP ${resp.status}`);
        }
        const updated = await resp.json();
        const idx = _fcItems.findIndex(i => i.control_id === controlId);
        if (idx !== -1) _fcItems[idx] = updated;
        renderFormControls();
        if (typeof FormControlManager !== 'undefined') FormControlManager.reload();
        if (statusEl) { statusEl.textContent = `✓ Updated "${updated.label}"`; statusEl.style.color = 'var(--ok,#3fb950)'; }
    } catch (e) {
        if (statusEl) { statusEl.textContent = `✗ ${e.message}`; statusEl.style.color = 'var(--danger,#f85149)'; }
    }
}

// ── Delete ────────────────────────────────────────────────────────────────────

async function _fcDeleteRow(controlId) {
    const item = _fcItems.find(i => i.control_id === controlId);
    if (!item) return;
    if (!confirm(`Delete form control "${item.label}" (key: ${item.control_key})?\n\nThis removes only the DB entry — the key will simply have no sound/icon until re-created.`)) return;

    const statusEl = document.getElementById('fc-status');
    try {
        const resp = await apiFetch(`/api/v1/form-controls/${controlId}`, { method: 'DELETE' });
        if (!resp.ok && resp.status !== 204) {
            const err = await resp.json().catch(() => ({ detail: `HTTP ${resp.status}` }));
            throw new Error(err.detail || `HTTP ${resp.status}`);
        }
        _fcItems = _fcItems.filter(i => i.control_id !== controlId);
        renderFormControls();
        if (typeof FormControlManager !== 'undefined') FormControlManager.reload();
        if (statusEl) { statusEl.textContent = `✓ Deleted "${item.label}"`; statusEl.style.color = 'var(--ok,#3fb950)'; }
    } catch (e) {
        if (statusEl) { statusEl.textContent = `✗ ${e.message}`; statusEl.style.color = 'var(--danger,#f85149)'; }
    }
}

// ── Asset upload ──────────────────────────────────────────────────────────────

async function _fcUploadAsset(inp) {
    const file      = inp.files[0];
    const controlId = inp.dataset.controlId;
    const assetType = inp.dataset.assetType;
    if (!file || !controlId) return;

    const statusEl = document.getElementById('fc-status');
    if (statusEl) { statusEl.textContent = '⏳ Uploading…'; statusEl.style.color = ''; }

    const form = new FormData();
    form.append('file', file);
    // sounds_off uses the same file folder as sounds; only the DB column differs
    form.append('asset_type', assetType === 'sounds_off' ? 'sounds' : assetType);

    try {
        // Step 1: upload the file (returns path, no row update)
        const upResp = await apiFetch('/api/v1/form-controls/upload-asset', { method: 'POST', body: form });
        if (!upResp.ok) {
            const err = await upResp.json().catch(() => ({ detail: `HTTP ${upResp.status}` }));
            throw new Error(err.detail || `HTTP ${upResp.status}`);
        }
        const { path } = await upResp.json();

        // Step 2: assign path to the control row
        const assignForm = new FormData();
        assignForm.append('control_id', controlId);
        assignForm.append('asset_path', path);
        assignForm.append('asset_type', assetType);  // may be sounds_off
        const assignResp = await apiFetch('/api/v1/form-controls/assign-asset', { method: 'POST', body: assignForm });
        if (!assignResp.ok) {
            const err = await assignResp.json().catch(() => ({ detail: `HTTP ${assignResp.status}` }));
            throw new Error(err.detail || `HTTP ${assignResp.status}`);
        }
        const result = await assignResp.json();
        const field  = assetType === 'icons' ? 'icon_asset' : (assetType === 'sounds_off' ? 'sound_asset_off' : 'sound_asset');
        const idx    = _fcItems.findIndex(i => i.control_id === controlId);
        if (idx !== -1) _fcItems[idx][field] = result.path;

        renderFormControls();
        if (typeof FormControlManager !== 'undefined') FormControlManager.reload();
        inp.value = '';
        if (statusEl) { statusEl.textContent = `✓ Uploaded and assigned ${path}`; statusEl.style.color = 'var(--ok,#3fb950)'; }
    } catch (e) {
        if (statusEl) { statusEl.textContent = `✗ ${e.message}`; statusEl.style.color = 'var(--danger,#f85149)'; }
    }
}

// ── Clear asset field ─────────────────────────────────────────────────────────

async function _fcClearAsset(controlId, field) {
    const statusEl = document.getElementById('fc-status');
    try {
        const resp = await apiFetch(`/api/v1/form-controls/${controlId}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ [field]: null }),
        });
        if (!resp.ok) {
            const err = await resp.json().catch(() => ({ detail: `HTTP ${resp.status}` }));
            throw new Error(err.detail || `HTTP ${resp.status}`);
        }
        const updated = await resp.json();
        const idx = _fcItems.findIndex(i => i.control_id === controlId);
        if (idx !== -1) _fcItems[idx] = updated;
        renderFormControls();
        if (typeof FormControlManager !== 'undefined') FormControlManager.reload();
        if (statusEl) { statusEl.textContent = `✓ Cleared ${field}`; statusEl.style.color = 'var(--ok,#3fb950)'; }
    } catch (e) {
        if (statusEl) { statusEl.textContent = `✗ ${e.message}`; statusEl.style.color = 'var(--danger,#f85149)'; }
    }
}

// ── Asset picker modal ────────────────────────────────────────────────────────

let _fcPickerControlId  = null;
let _fcPickerAssetType  = null;

async function _fcOpenPicker(controlId, assetType) {
    _fcPickerControlId = controlId;
    _fcPickerAssetType = assetType;

    const modal  = _fcGetOrCreatePickerModal();
    const title  = modal.querySelector('#fc-picker-title');
    const grid   = modal.querySelector('#fc-picker-grid');
    const status = modal.querySelector('#fc-picker-status');

    title.textContent = `Choose ${assetType === 'icons' ? 'icon' : (assetType === 'sounds_off' ? 'off sound' : 'sound')}`;
    grid.innerHTML = '<p style="color:var(--text-dim);font-size:12px">Loading…</p>';
    status.textContent = '';
    modal.showModal();

    try {
        // Browse from the shared nav_items asset listing (same folder)
        // sounds_off still browses the sounds folder
        const browseType = assetType === 'sounds_off' ? 'sounds' : assetType;
        const resp = await apiFetch(`/api/v1/form-controls/assets?type=${browseType}`);
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const assets = await resp.json();

        if (!assets.length) {
            grid.innerHTML =
                '<p style="color:var(--text-dim);font-size:12px">No assets uploaded yet. ' +
                'Use the Nav Items page to upload assets.</p>';
            return;
        }

        grid.innerHTML = '';
        for (const asset of assets) {
            const card = document.createElement('div');
            card.className = 'fc-picker-card';
            card.dataset.path = asset.path;

            if (assetType === 'icons') {
                card.innerHTML = `
                    <img class="fc-picker-thumb" src="${_esc(asset.url)}" alt="${_esc(asset.filename)}">
                    <span class="fc-picker-name">${_esc(asset.filename)}</span>
                `;
            } else {
                card.innerHTML = `
                    <button class="fc-picker-play btn-small secondary"
                        data-url="${_esc(asset.url)}" title="Preview">▶</button>
                    <span class="fc-picker-name">${_esc(asset.filename)}</span>
                    <small style="color:var(--text-dim)">${_fmtBytes(asset.size)}</small>
                `;
                card.querySelector('.fc-picker-play').addEventListener('click', (e) => {
                    e.stopPropagation();
                    const url = e.currentTarget.dataset.url;
                    if (typeof SoundManager !== 'undefined') SoundManager.preview(url);
                });
            }

            card.addEventListener('click', () => _fcPickerSelect(asset.path));
            grid.appendChild(card);
        }
    } catch (e) {
        grid.innerHTML =
            `<p style="color:var(--danger,#f85149);font-size:12px">✗ ${_esc(e.message)}</p>`;
    }
}

function _fcGetOrCreatePickerModal() {
    let modal = document.getElementById('fc-picker-modal');
    if (modal) return modal;

    modal = document.createElement('dialog');
    modal.id = 'fc-picker-modal';
    modal.className = 'ni-picker-modal';    // reuse nav-items picker CSS
    modal.innerHTML = `
        <div class="ni-picker-header">
            <span id="fc-picker-title">Choose asset</span>
            <button class="btn-small secondary" id="fc-picker-close">✕ Close</button>
        </div>
        <p id="fc-picker-status" style="font-size:12px;margin:4px 0 8px"></p>
        <div id="fc-picker-grid" class="ni-picker-grid"></div>
    `;
    document.body.appendChild(modal);

    modal.querySelector('#fc-picker-close').addEventListener('click', () => modal.close());
    modal.addEventListener('click', (e) => { if (e.target === modal) modal.close(); });
    return modal;
}

async function _fcPickerSelect(assetPath) {
    const modal  = document.getElementById('fc-picker-modal');
    const status = modal ? modal.querySelector('#fc-picker-status') : null;
    if (status) { status.textContent = '⏳ Assigning…'; status.style.color = ''; }

    const form = new FormData();
    form.append('control_id', _fcPickerControlId);
    form.append('asset_path', assetPath);
    form.append('asset_type', _fcPickerAssetType);

    try {
        const resp = await apiFetch('/api/v1/form-controls/assign-asset', { method: 'POST', body: form });
        if (!resp.ok) {
            const err = await resp.json().catch(() => ({ detail: `HTTP ${resp.status}` }));
            throw new Error(err.detail || `HTTP ${resp.status}`);
        }
        const result = await resp.json();
        const field  = _fcPickerAssetType === 'icons' ? 'icon_asset' : (_fcPickerAssetType === 'sounds_off' ? 'sound_asset_off' : 'sound_asset');
        const idx    = _fcItems.findIndex(i => i.control_id === _fcPickerControlId);
        if (idx !== -1) _fcItems[idx][field] = result.path;

        if (modal) modal.close();
        renderFormControls();
        if (typeof FormControlManager !== 'undefined') FormControlManager.reload();

        const pageStatus = document.getElementById('fc-status');
        if (pageStatus) { pageStatus.textContent = `✓ Assigned ${result.path}`; pageStatus.style.color = 'var(--ok,#3fb950)'; }
    } catch (e) {
        if (status) { status.textContent = `✗ ${_esc(e.message)}`; status.style.color = 'var(--danger,#f85149)'; }
    }
}

// ── Utilities (re-use from nav-items scope if present; else define locally) ───

function _esc(s) {
    if (s === null || s === undefined) return '';
    return String(s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function _fmtBytes(b) {
    if (b < 1024)       return `${b} B`;
    if (b < 1048576)    return `${(b / 1024).toFixed(1)} KB`;
    return `${(b / 1048576).toFixed(1)} MB`;
}
