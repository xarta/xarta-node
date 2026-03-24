// nav-items.js — Nav Items management page for Blueprints Settings
// xarta-node Blueprints GUI
//
// Displays all nav_items from the database grouped by menu_group.
// Supports inline editing of label, icon_emoji, icon_asset, sound_asset, sort_order.
// File upload buttons per row for icon and sound assets.
// "Browse existing" picker to assign already-uploaded assets without re-uploading.
// Bulk upload: drop a .zip / .7z / .tar.gz archive to extract into the assets folder.
// Icon preview column showing the resolved icon (asset > emoji > fallback).
// Sound preview/play button per row.

'use strict';

let _navItems = [];

// ── Bootstrap ─────────────────────────────────────────────────────────────────

async function loadNavItems() {
    const statusEl = document.getElementById('nav-items-status');
    try {
        // First pass: fetch current state to discover which groups need seeding
        const r0 = await apiFetch('/api/v1/nav-items');
        const current = r0.ok ? await r0.json() : [];
        const seededGroups = new Set(current.map(i => i.menu_group));

        // Trigger auto-seed for any absent group by calling that group's menu config
        const cfgMap = {
            probes:    typeof ProbesMenuConfig    !== 'undefined' ? ProbesMenuConfig    : null,
            synthesis: typeof SynthesisMenuConfig !== 'undefined' ? SynthesisMenuConfig : null,
            settings:  typeof SettingsMenuConfig  !== 'undefined' ? SettingsMenuConfig  : null,
        };
        const seedTasks = [];
        for (const [group, cfg] of Object.entries(cfgMap)) {
            if (!seededGroups.has(group) && cfg) {
                seedTasks.push(cfg.loadNavItemsFromDB());
            }
        }
        if (seedTasks.length) {
            if (statusEl) { statusEl.textContent = `⏳ Seeding ${seedTasks.length} group(s)…`; statusEl.style.color = ''; }
            await Promise.all(seedTasks);
        }

        // Second pass: fetch final state
        const r1 = await apiFetch('/api/v1/nav-items');
        if (!r1.ok) throw new Error(`HTTP ${r1.status}`);
        _navItems = await r1.json();
        renderNavItems();
        if (statusEl) statusEl.textContent = '';
    } catch (e) {
        if (statusEl) { statusEl.textContent = `✗ ${e.message}`; statusEl.style.color = 'var(--danger,#f85149)'; }
    }
}

// ── Table rendering ───────────────────────────────────────────────────────────

function renderNavItems() {
    const container = document.getElementById('nav-items-container');
    if (!container) return;
    container.innerHTML = '';

    // Bulk upload panel (top of page, above the item tables)
    container.appendChild(_niBulkUploadPanelEl());

    const groups = ['probes', 'synthesis', 'settings'];
    for (const group of groups) {
        const groupItems = _navItems
            .filter(i => i.menu_group === group)
            .sort((a, b) => a.sort_order - b.sort_order || a.item_key.localeCompare(b.item_key));

        const section = document.createElement('div');
        section.className = 'ni-group-section';

        const h = document.createElement('h3');
        h.className = 'ni-group-header';
        h.textContent = group.charAt(0).toUpperCase() + group.slice(1);
        section.appendChild(h);

        if (!groupItems.length) {
            const msg = document.createElement('p');
            msg.style.cssText = 'font-size:12px;color:var(--text-dim);padding:6px 0;margin:0';
            msg.textContent = 'No items seeded yet.';
            section.appendChild(msg);
            container.appendChild(section);
            continue;
        }

        const table = document.createElement('table');
        table.className = 'data-table ni-table';
        table.innerHTML = `
            <thead>
                <tr>
                    <th title="Resolved icon">Icon</th>
                    <th>Key</th>
                    <th>Label</th>
                    <th>Page Label</th>
                    <th>Emoji</th>
                    <th>Icon Asset</th>
                    <th>Sound Asset</th>
                    <th>Order</th>
                    <th>Type</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody id="ni-tbody-${group}"></tbody>
        `;
        section.appendChild(table);
        container.appendChild(section);

        const tbody = table.querySelector(`#ni-tbody-${group}`);
        for (const item of groupItems) {
            tbody.appendChild(_niRowEl(item));
        }
    }
}

// ── Bulk upload panel ─────────────────────────────────────────────────────────

function _niBulkUploadPanelEl() {
    const panel = document.createElement('div');
    panel.className = 'ni-bulk-panel';
    panel.innerHTML = `
        <div class="ni-bulk-header">Bulk asset upload</div>
        <p class="ni-bulk-desc">Upload a <strong>.zip</strong>, <strong>.7z</strong>, or <strong>.tar.gz</strong> archive.
        Files with matching extensions are extracted into the icons or sounds folder automatically.</p>
        <div class="ni-bulk-row">
            <select id="ni-bulk-type" style="padding:4px 8px;border-radius:4px;border:1px solid var(--border);background:var(--surface);color:var(--text);font-size:13px">
                <option value="icons">Icons (.svg .png .ico .jpg .webp)</option>
                <option value="sounds">Sounds (.wav .mp3 .ogg .flac .webm .m4a)</option>
            </select>
            <label class="btn-small secondary ni-upload-label" style="cursor:pointer">
                📦 Choose archive
                <input type="file" id="ni-bulk-file" accept=".zip,.7z,.tar,.gz,.bz2,.tgz,.txz,.tar.gz,.tar.bz2,.tar.xz" style="display:none">
            </label>
            <span id="ni-bulk-filename" style="font-size:12px;color:var(--text-dim)">No file chosen</span>
            <button id="ni-bulk-upload-btn" class="btn-small" disabled>Upload &amp; extract</button>
        </div>
        <p id="ni-bulk-status" style="font-size:12px;margin:4px 0 0"></p>
    `;

    const fileInput = panel.querySelector('#ni-bulk-file');
    const filenameSpan = panel.querySelector('#ni-bulk-filename');
    const uploadBtn = panel.querySelector('#ni-bulk-upload-btn');
    const statusEl = panel.querySelector('#ni-bulk-status');

    fileInput.addEventListener('change', () => {
        const f = fileInput.files[0];
        filenameSpan.textContent = f ? f.name : 'No file chosen';
        uploadBtn.disabled = !f;
    });

    uploadBtn.addEventListener('click', async () => {
        const file = fileInput.files[0];
        const assetType = panel.querySelector('#ni-bulk-type').value;
        if (!file) return;

        uploadBtn.disabled = true;
        statusEl.textContent = '⏳ Uploading and extracting…';
        statusEl.style.color = '';

        const form = new FormData();
        form.append('file', file);
        form.append('asset_type', assetType);
        try {
            const resp = await apiFetch('/api/v1/nav-items/upload-bulk', { method: 'POST', body: form });
            if (!resp.ok) {
                const err = await resp.json().catch(() => ({ detail: `HTTP ${resp.status}` }));
                throw new Error(err.detail || `HTTP ${resp.status}`);
            }
            const result = await resp.json();
            const names = result.extracted.join(', ') || '(none)';
            if (result.count === 0) {
                statusEl.textContent = `⚠ No matching files found in archive. Check the type dropdown matches your archive contents.`;
                statusEl.style.color = 'var(--warn,#d29922)';
            } else {
                statusEl.textContent = `✓ Extracted ${result.count} file(s): ${names}`;
                statusEl.style.color = 'var(--ok,#3fb950)';
            }
            fileInput.value = '';
            filenameSpan.textContent = 'No file chosen';
            uploadBtn.disabled = true;
        } catch (e) {
            statusEl.textContent = `✗ ${e.message}`;
            statusEl.style.color = 'var(--danger,#f85149)';
            uploadBtn.disabled = false;
        }
    });

    return panel;
}

// ── Row builder ───────────────────────────────────────────────────────────────

function _resolvedIconHtml(item) {
    if (item.icon_asset) {
        const ts = item.updated_at ? new Date(item.updated_at).getTime() : 0;
        return `<img class="ni-icon-preview menu-icon" src="/fallback-ui/assets/${item.icon_asset}?v=${ts}" alt=""
            data-fallback="1">`;
    }
    if (item.icon_emoji) {
        return `<span class="ni-icon-emoji">${item.icon_emoji}</span>`;
    }
    return `<img class="ni-icon-preview menu-icon" src="/fallback-ui/assets/icons/fallback.svg" alt="?">`;
}

function _niRowEl(item) {
    const tr = document.createElement('tr');
    tr.dataset.itemId = item.item_id;
    tr.innerHTML = `
        <td class="ni-col-icon">${_resolvedIconHtml(item)}</td>
        <td class="ni-col-key"><code>${_esc(item.item_key)}</code>${item.is_fn ? ' <span class="ni-fn-badge">fn</span>' : ''}</td>
        <td class="ni-col-label"><span class="ni-field" data-field="label">${_esc(item.label)}</span></td>
        <td class="ni-col-page-label"><span class="ni-field" data-field="page_label">${_esc(item.page_label || '')}</span></td>
        <td class="ni-col-emoji"><span class="ni-field" data-field="icon_emoji">${_esc(item.icon_emoji || '')}</span></td>
        <td class="ni-col-icon-asset">
            <span class="ni-field ni-asset-path" data-field="icon_asset">${_esc(item.icon_asset || '')}</span>
            <div class="ni-asset-actions">
                <label class="btn-small secondary ni-upload-label" title="Upload new icon file">
                    ⬆ <input type="file" class="ni-file-input" data-item-id="${item.item_id}" data-asset-type="icons"
                        accept=".svg,.png,.ico,.jpg,.jpeg,.webp" style="display:none">
                </label>
                <button class="btn-small secondary ni-browse-btn" data-item-id="${item.item_id}" data-asset-type="icons" title="Choose from existing icons">📋</button>
                ${item.icon_asset ? `<button class="btn-small secondary ni-clear-asset" data-item-id="${item.item_id}" data-field="icon_asset" title="Clear icon asset">✕</button>` : ''}
            </div>
        </td>
        <td class="ni-col-sound-asset">
            <span class="ni-field ni-asset-path" data-field="sound_asset">${_esc(item.sound_asset || '')}</span>
            <div class="ni-asset-actions">
                <label class="btn-small secondary ni-upload-label" title="Upload new sound file">
                    ⬆ <input type="file" class="ni-file-input" data-item-id="${item.item_id}" data-asset-type="sounds"
                        accept=".wav,.mp3,.ogg,.flac,.webm,.m4a" style="display:none">
                </label>
                <button class="btn-small secondary ni-browse-btn" data-item-id="${item.item_id}" data-asset-type="sounds" title="Choose from existing sounds">📋</button>
                ${item.sound_asset ? `<button class="btn-small secondary ni-sound-play" data-sound-path="${item.sound_asset}" data-updated="${item.updated_at || ''}" title="Preview sound">▶</button>` : ''}
                ${item.sound_asset ? `<button class="btn-small secondary ni-clear-asset" data-item-id="${item.item_id}" data-field="sound_asset" title="Clear sound asset">✕</button>` : ''}
            </div>
        </td>
        <td class="ni-col-order"><span class="ni-field" data-field="sort_order">${item.sort_order}</span></td>
        <td class="ni-col-type">${item.fn_key ? `<small>${_esc(item.fn_key)}</small>` : 'nav'}</td>
        <td class="ni-col-actions">
            <button class="btn-small secondary ni-edit-btn" data-item-id="${item.item_id}" title="Edit">✏️</button>
        </td>
    `;

    // Wire upload inputs
    tr.querySelectorAll('.ni-file-input').forEach(inp => {
        inp.addEventListener('change', () => _niUploadAsset(inp));
    });

    // Wire "browse existing" buttons
    tr.querySelectorAll('.ni-browse-btn').forEach(btn => {
        btn.addEventListener('click', () => _niOpenPicker(btn.dataset.itemId, btn.dataset.assetType));
    });

    // Wire sound preview
    tr.querySelectorAll('.ni-sound-play').forEach(btn => {
        btn.addEventListener('click', () => {
            const path = btn.dataset.soundPath;
            const ts = btn.dataset.updated ? new Date(btn.dataset.updated).getTime() : 0;
            if (path && typeof SoundManager !== 'undefined') {
                SoundManager.preview(`/fallback-ui/assets/${path}?v=${ts}`);
            }
        });
    });

    // Wire clear buttons
    tr.querySelectorAll('.ni-clear-asset').forEach(btn => {
        btn.addEventListener('click', () => _niClearAsset(btn.dataset.itemId, btn.dataset.field));
    });

    // Wire edit button
    tr.querySelectorAll('.ni-edit-btn').forEach(btn => {
        btn.addEventListener('click', () => _niEditRow(btn.dataset.itemId));
    });

    // Icon img fallback
    tr.querySelectorAll('.ni-icon-preview').forEach(img => {
        img.addEventListener('error', function() {
            if (!this.dataset.usedFallback) {
                this.dataset.usedFallback = '1';
                this.src = '/fallback-ui/assets/icons/fallback.svg';
            }
        });
    });

    return tr;
}

// ── Asset picker modal ────────────────────────────────────────────────────────

let _pickerItemId = null;
let _pickerAssetType = null;

async function _niOpenPicker(itemId, assetType) {
    _pickerItemId = itemId;
    _pickerAssetType = assetType;

    const modal = _niGetOrCreatePickerModal();
    const title = modal.querySelector('#ni-picker-title');
    const grid  = modal.querySelector('#ni-picker-grid');
    const status = modal.querySelector('#ni-picker-status');

    title.textContent = `Choose ${assetType === 'icons' ? 'icon' : 'sound'}`;
    grid.innerHTML = '<p style="color:var(--text-dim);font-size:12px">Loading…</p>';
    status.textContent = '';
    modal.showModal();

    try {
        const resp = await apiFetch(`/api/v1/nav-items/assets?type=${assetType}`);
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const assets = await resp.json();

        if (!assets.length) {
            grid.innerHTML = '<p style="color:var(--text-dim);font-size:12px">No assets uploaded yet.</p>';
            return;
        }

        grid.innerHTML = '';
        for (const asset of assets) {
            const card = document.createElement('div');
            card.className = 'ni-picker-card';
            card.dataset.path = asset.path;

            if (assetType === 'icons') {
                card.innerHTML = `
                    <img class="ni-picker-thumb" src="${_esc(asset.url)}" alt="${_esc(asset.filename)}">
                    <span class="ni-picker-name">${_esc(asset.filename)}</span>
                `;
            } else {
                // sounds — show filename and play button
                card.innerHTML = `
                    <button class="ni-picker-play btn-small secondary" data-url="${_esc(asset.url)}" title="Preview">▶</button>
                    <span class="ni-picker-name">${_esc(asset.filename)}</span>
                    <small style="color:var(--text-dim)">${_fmtBytes(asset.size)}</small>
                `;
                card.querySelector('.ni-picker-play').addEventListener('click', (e) => {
                    e.stopPropagation();
                    const url = e.currentTarget.dataset.url;
                    if (typeof SoundManager !== 'undefined') SoundManager.preview(url);
                });
            }

            card.addEventListener('click', () => _niPickerSelect(asset.path));
            grid.appendChild(card);
        }
    } catch (e) {
        grid.innerHTML = `<p style="color:var(--danger,#f85149);font-size:12px">✗ ${_esc(e.message)}</p>`;
    }
}

function _niGetOrCreatePickerModal() {
    let modal = document.getElementById('ni-picker-modal');
    if (modal) return modal;

    modal = document.createElement('dialog');
    modal.id = 'ni-picker-modal';
    modal.className = 'ni-picker-modal';
    modal.innerHTML = `
        <div class="ni-picker-header">
            <span id="ni-picker-title">Choose asset</span>
            <button class="btn-small secondary" id="ni-picker-close">✕ Close</button>
        </div>
        <p id="ni-picker-status" style="font-size:12px;margin:4px 0 8px"></p>
        <div id="ni-picker-grid" class="ni-picker-grid"></div>
    `;
    document.body.appendChild(modal);

    modal.querySelector('#ni-picker-close').addEventListener('click', () => modal.close());
    // Close on backdrop click
    modal.addEventListener('click', (e) => { if (e.target === modal) modal.close(); });
    return modal;
}

async function _niPickerSelect(assetPath) {
    const modal = document.getElementById('ni-picker-modal');
    const status = modal ? modal.querySelector('#ni-picker-status') : null;
    if (status) { status.textContent = '⏳ Assigning…'; status.style.color = ''; }

    const form = new FormData();
    form.append('item_id', _pickerItemId);
    form.append('asset_path', assetPath);
    form.append('asset_type', _pickerAssetType);

    try {
        const resp = await apiFetch('/api/v1/nav-items/assign-asset', { method: 'POST', body: form });
        if (!resp.ok) {
            const err = await resp.json().catch(() => ({ detail: `HTTP ${resp.status}` }));
            throw new Error(err.detail || `HTTP ${resp.status}`);
        }
        const result = await resp.json();

        // Update local cache
        const field = _pickerAssetType === 'icons' ? 'icon_asset' : 'sound_asset';
        const idx = _navItems.findIndex(i => i.item_id === _pickerItemId);
        if (idx !== -1) _navItems[idx][field] = result.path;

        if (modal) modal.close();
        renderNavItems();
        _niReloadNavConfig(_pickerItemId);

        const pageStatus = document.getElementById('nav-items-status');
        if (pageStatus) {
            pageStatus.textContent = `✓ Assigned: ${result.path}`;
            pageStatus.style.color = 'var(--ok,#3fb950)';
            setTimeout(() => { pageStatus.textContent = ''; }, 2500);
        }
    } catch (e) {
        if (status) { status.textContent = `✗ ${e.message}`; status.style.color = 'var(--danger,#f85149)'; }
    }
}

// ── Edit / save ───────────────────────────────────────────────────────────────

function _niEditRow(itemId) {
    const item = _navItems.find(i => i.item_id === itemId);
    if (!item) return;

    const fields = {
        label:       prompt('Label:', item.label),
        page_label:  prompt('Page Label:', item.page_label || ''),
        icon_emoji:  prompt('Icon Emoji (e.g. 🔥):', item.icon_emoji || ''),
        sort_order:  prompt('Sort Order (number):', String(item.sort_order)),
    };

    if (Object.values(fields).some(v => v === null)) return;

    _niSaveItem(itemId, {
        label:      fields.label.trim() || item.label,
        page_label: fields.page_label.trim() || null,
        icon_emoji: fields.icon_emoji.trim() || null,
        sort_order: parseInt(fields.sort_order, 10) || 0,
    });
}

function _niSaveItem(itemId, update) {
    const statusEl = document.getElementById('nav-items-status');
    apiFetch(`/api/v1/nav-items/${itemId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(update),
    })
    .then(r => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
    })
    .then(updated => {
        const idx = _navItems.findIndex(i => i.item_id === itemId);
        if (idx !== -1) _navItems[idx] = updated;
        renderNavItems();
        _niReloadNavConfig(itemId);
        if (statusEl) { statusEl.textContent = '✓ Saved'; statusEl.style.color = 'var(--ok,#3fb950)'; }
        setTimeout(() => { if (statusEl) statusEl.textContent = ''; }, 2500);
    })
    .catch(e => {
        if (statusEl) { statusEl.textContent = `✗ ${e.message}`; statusEl.style.color = 'var(--danger,#f85149)'; }
    });
}

function _niClearAsset(itemId, field) {
    _niSaveItem(itemId, { [field]: '' });
}

// ── File upload (single asset) ────────────────────────────────────────────────

async function _niUploadAsset(input) {
    const itemId = input.dataset.itemId;
    const assetType = input.dataset.assetType;
    const file = input.files[0];
    if (!file || !itemId || !assetType) return;

    const statusEl = document.getElementById('nav-items-status');
    if (statusEl) { statusEl.textContent = '⏳ Uploading…'; statusEl.style.color = ''; }

    const form = new FormData();
    form.append('file', file);
    form.append('item_id', itemId);
    form.append('asset_type', assetType);

    try {
        const resp = await apiFetch('/api/v1/nav-items/upload-asset', { method: 'POST', body: form });
        if (!resp.ok) {
            const err = await resp.json().catch(() => ({ detail: `HTTP ${resp.status}` }));
            throw new Error(err.detail || `HTTP ${resp.status}`);
        }
        const result = await resp.json();
        const field = assetType === 'icons' ? 'icon_asset' : 'sound_asset';
        const idx = _navItems.findIndex(i => i.item_id === itemId);
        if (idx !== -1) _navItems[idx][field] = result.path;
        renderNavItems();
        _niReloadNavConfig(itemId);
        if (statusEl) { statusEl.textContent = `✓ Uploaded: ${result.path}`; statusEl.style.color = 'var(--ok,#3fb950)'; }
        setTimeout(() => { if (statusEl) statusEl.textContent = ''; }, 3000);
    } catch (e) {
        if (statusEl) { statusEl.textContent = `✗ Upload failed: ${e.message}`; statusEl.style.color = 'var(--danger,#f85149)'; }
    }
    input.value = '';
}

// ── Utilities ─────────────────────────────────────────────────────────────────

function _esc(s) {
    return String(s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

function _fmtBytes(n) {
    if (n < 1024) return `${n} B`;
    if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
    return `${(n / 1024 / 1024).toFixed(1)} MB`;
}

// Reload the navbar for the group that owns itemId so icon/label changes
// are visible immediately without a full page refresh.
function _niReloadNavConfig(itemId) {
    const item = _navItems.find(i => i.item_id === itemId);
    if (!item) return;
    const cfgMap = {
        probes:    typeof ProbesMenuConfig    !== 'undefined' ? ProbesMenuConfig    : null,
        synthesis: typeof SynthesisMenuConfig !== 'undefined' ? SynthesisMenuConfig : null,
        settings:  typeof SettingsMenuConfig  !== 'undefined' ? SettingsMenuConfig  : null,
    };
    const navCfg = cfgMap[item.menu_group];
    if (navCfg) navCfg.loadNavItemsFromDB();
}

