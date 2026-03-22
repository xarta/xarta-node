/* ── Manual Links ────────────────────────────────────────────────────────── */

let _manualLinksView = 'rendered';   // 'table' | 'rendered' — default to rendered
let _editingLinkId   = null;      // null = add mode, string = edit mode
let _mlFilter    = '';               // table filter text
let _mlSort      = { col: null, dir: 1 }; // active sort column + direction (1=asc, -1=desc)
let _mlGroupBy   = 'none';          // 'none' | 'group' | 'host'
let _mlCollapsed = new Set();       // collapsed group keys

/* ── View toggle ─────────────────────────────────────────────────────────── */

function manualLinksShowView(view) {
  _manualLinksView = view;
  document.getElementById('ml-table-view').style.display    = view === 'table'    ? '' : 'none';
  document.getElementById('ml-rendered-view').style.display = view === 'rendered' ? '' : 'none';
  if (typeof SynthesisMenuConfig !== 'undefined') SynthesisMenuConfig.updateActiveTab('manual-links-' + view);
  if (view === 'rendered') renderManualLinksRendered();
  if (view === 'table')    renderManualLinksTable();
}

/* ── Load + render table ─────────────────────────────────────────────────── */

async function loadManualLinks() {
  const err = document.getElementById('ml-error');
  if (err) err.hidden = true;
  try {
    const r = await apiFetch('/api/v1/manual-links');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    _manualLinks = await r.json();
    renderManualLinksTable();
    if (_manualLinksView === 'rendered') renderManualLinksRendered();
  } catch (e) {
    if (err) { err.textContent = `Failed to load manual links: ${e.message}`; err.hidden = false; }
  }
}

function renderManualLinksTable() {
  const tbody = document.getElementById('ml-tbody');
  if (!tbody) return;

  // Filter
  const q = (document.getElementById('ml-filter')?.value || '').toLowerCase().trim();
  let rows = q
    ? _manualLinks.filter(l => [
        l.label, l.vlan_ip, l.vlan_uri, l.tailnet_ip, l.tailnet_uri,
        l.group_name, l.pve_host, l.vm_name, l.lxc_name, l.location, l.notes
      ].some(v => v && v.toLowerCase().includes(q)))
    : [..._manualLinks];

  const _clearArrows = () =>
    ['label','addr','group','order','host','notes'].forEach(c => {
      const el = document.getElementById(`ml-arrow-${c}`);
      if (el) { el.textContent = '⇕'; el.classList.remove('active'); }
    });

  if (!rows.length) {
    tbody.innerHTML = `<tr class="empty-row"><td colspan="8">${q ? 'No matches.' : 'No links yet — click + Add link'}</td></tr>`;
    _clearArrows();
    return;
  }

  // Sort
  if (_mlSort.col) {
    rows.sort((a, b) => {
      const av = _mlGetSortVal(a, _mlSort.col);
      const bv = _mlGetSortVal(b, _mlSort.col);
      return (av < bv ? -1 : av > bv ? 1 : 0) * _mlSort.dir;
    });
  }

  // Update sort arrows
  ['label','addr','group','order','host','notes'].forEach(c => {
    const el = document.getElementById(`ml-arrow-${c}`);
    if (!el) return;
    if (_mlSort.col === c) {
      el.textContent = _mlSort.dir === 1 ? '▲' : '▼';
      el.classList.add('active');
    } else {
      el.textContent = '⇕';
      el.classList.remove('active');
    }
  });

  // Row HTML builder
  function rowHtml(lnk) {
    const addrParts = [];
    if (lnk.vlan_ip)     addrParts.push(`<span class="badge" title="VLAN IP">${esc(lnk.vlan_ip)}</span>`);
    if (lnk.vlan_uri)    addrParts.push(`<span class="badge" title="VLAN URI">${esc(lnk.vlan_uri)}</span>`);
    if (lnk.tailnet_ip)  addrParts.push(`<span class="badge" title="Tailnet IP">${esc(lnk.tailnet_ip)}</span>`);
    if (lnk.tailnet_uri) addrParts.push(`<span class="badge" title="Tailnet URI">${esc(lnk.tailnet_uri)}</span>`);

    const hostParts = [];
    if (lnk.pve_host)    hostParts.push(`PVE: ${esc(lnk.pve_host)}`);
    if (lnk.is_internet) hostParts.push(`<span class="badge" style="background:var(--accent-dim)">internet</span>`);
    if (lnk.vm_id)       hostParts.push(`VM ${esc(lnk.vm_id)}${lnk.vm_name ? ` (${esc(lnk.vm_name)})` : ''}`);
    if (lnk.lxc_id)      hostParts.push(`LXC ${esc(lnk.lxc_id)}${lnk.lxc_name ? ` (${esc(lnk.lxc_name)})` : ''}`);
    if (lnk.location)    hostParts.push(`<span style="color:var(--text-dim);font-size:11px">${esc(lnk.location)}</span>`);

    return `<tr>
      <td style="font-family:monospace;font-size:11px;color:var(--text-dim);max-width:80px;overflow:hidden;text-overflow:ellipsis" title="${esc(lnk.link_id)}">${esc(lnk.link_id.slice(0,8))}</td>
      <td style="max-width:160px">${lnk.icon ? `<span style="margin-right:4px">${esc(lnk.icon)}</span>` : ''}${lnk.label ? `<strong>${esc(lnk.label)}</strong>` : '<span style="color:var(--text-dim)">—</span>'}</td>
      <td style="max-width:200px">${addrParts.join(' ') || '<span style="color:var(--text-dim)">—</span>'}</td>
      <td>${lnk.group_name ? esc(lnk.group_name) : '<span style="color:var(--text-dim)">—</span>'}</td>
      <td>${lnk.sort_order}</td>
      <td style="font-size:12px">${hostParts.join(', ') || '<span style="color:var(--text-dim)">—</span>'}</td>
      <td style="max-width:200px;font-size:12px;color:var(--text-dim)">${lnk.notes ? esc(lnk.notes) : ''}</td>
      <td style="white-space:nowrap">
        <button class="secondary" style="padding:2px 8px;font-size:12px" onclick="openManualLinkModal('${esc(lnk.link_id)}')">Edit</button>
        <button class="secondary" style="padding:2px 8px;font-size:12px;color:var(--err)" onclick="deleteManualLink('${esc(lnk.link_id)}')">Del</button>
      </td>
    </tr>`;
  }

  if (_mlGroupBy === 'none') {
    tbody.innerHTML = rows.map(rowHtml).join('');
    return;
  }

  // Grouped rendering
  const keys = [];
  const map  = {};
  rows.forEach(lnk => {
    const k = _mlGroupKey(lnk);
    if (!map[k]) { map[k] = []; keys.push(k); }
    map[k].push(lnk);
  });

  let html = '';
  keys.forEach(k => {
    const collapsed = _mlCollapsed.has(k);
    html += `<tr class="ml-group-hdr" data-gkey="${esc(k)}" onclick="mlToggleGroup(this.dataset.gkey)">
      <td colspan="8">${collapsed ? '▶' : '▼'} ${esc(k)} <span style="font-weight:400;opacity:.6">(${map[k].length})</span></td>
    </tr>`;
    if (!collapsed) html += map[k].map(rowHtml).join('');
  });
  tbody.innerHTML = html;
}

/* ── Table helpers: sort / filter / group ────────────────────────────────── */

function mlSetGroupBy(by) {
  _mlGroupBy = by;
  _mlCollapsed.clear();
  ['none','group','host'].forEach(k =>
    document.getElementById(`ml-grp-${k}`)?.classList.toggle('active', k === by));
  renderManualLinksTable();
}

function mlSortBy(col) {
  _mlSort.dir = (_mlSort.col === col) ? _mlSort.dir * -1 : 1;
  _mlSort.col = col;
  renderManualLinksTable();
}

function mlToggleGroup(key) {
  if (_mlCollapsed.has(key)) _mlCollapsed.delete(key);
  else _mlCollapsed.add(key);
  renderManualLinksTable();
}

function _mlGetSortVal(lnk, col) {
  switch (col) {
    case 'label': return (lnk.label || '').toLowerCase();
    case 'addr':  return (lnk.vlan_uri || lnk.vlan_ip || lnk.tailnet_uri || lnk.tailnet_ip || '').toLowerCase();
    case 'group': return (lnk.group_name || '').toLowerCase();
    case 'order': return lnk.sort_order ?? 0;
    case 'host':  return (lnk.pve_host || lnk.vm_name || lnk.lxc_name || lnk.location || '').toLowerCase();
    case 'notes': return (lnk.notes || '').toLowerCase();
    default: return '';
  }
}

function _mlGroupKey(lnk) {
  if (_mlGroupBy === 'group') return lnk.group_name || '(no group)';
  if (_mlGroupBy === 'host')  return lnk.pve_host || lnk.vm_name || lnk.lxc_name || lnk.location || '(no host)';
  return '';
}

/* ── Rendered view ───────────────────────────────────────────────────────── */

function renderManualLinksRendered() {
  const container = document.getElementById('ml-rendered-body');
  if (!container) return;
  if (!_manualLinks.length) {
    container.innerHTML = '<p style="color:var(--text-dim);font-size:13px">No links defined yet.</p>';
    return;
  }

  // Separate top-level from children
  const topLevel  = _manualLinks.filter(l => !l.parent_id);
  const childMap  = {};
  _manualLinks.filter(l => l.parent_id).forEach(l => {
    if (!childMap[l.parent_id]) childMap[l.parent_id] = [];
    childMap[l.parent_id].push(l);
  });

  // Group top-level items
  const groups = {};
  const ungrouped = [];
  topLevel.forEach(l => {
    if (l.group_name) {
      if (!groups[l.group_name]) groups[l.group_name] = [];
      groups[l.group_name].push(l);
    } else {
      ungrouped.push(l);
    }
  });

  const sortByOrder = arr =>
    [...arr].sort((a, b) => (a.sort_order - b.sort_order) || (a.label || '').localeCompare(b.label || ''));

  function renderLink(lnk) {
    const icon = lnk.icon ? `<span style="margin-right:6px;font-size:1.1em">${esc(lnk.icon)}</span>` : '';
    const labelHtml = lnk.label ? `<span style="font-weight:600">${icon}${esc(lnk.label)}</span>` : `${icon}<span style="color:var(--text-dim);font-style:italic">untitled</span>`;

    // Tooltip detail rows shared by all address chips on this item
    const sharedRows = [];
    if (lnk.pve_host)    sharedRows.push(`<div class="ml-tip-row"><span class="ml-tip-lbl">PVE host</span><span>${esc(lnk.pve_host)}</span></div>`);
    if (lnk.is_internet) sharedRows.push(`<div class="ml-tip-row"><span class="ml-tip-lbl">Network</span><span>internet</span></div>`);
    if (lnk.vm_id)       sharedRows.push(`<div class="ml-tip-row"><span class="ml-tip-lbl">VM</span><span>${esc(lnk.vm_id)}${lnk.vm_name ? ` (${esc(lnk.vm_name)})` : ''}</span></div>`);
    if (lnk.lxc_id)      sharedRows.push(`<div class="ml-tip-row"><span class="ml-tip-lbl">LXC</span><span>${esc(lnk.lxc_id)}${lnk.lxc_name ? ` (${esc(lnk.lxc_name)})` : ''}</span></div>`);
    if (lnk.location)    sharedRows.push(`<div class="ml-tip-row"><span class="ml-tip-lbl">Location</span><span>${esc(lnk.location)}</span></div>`);
    if (lnk.notes)       sharedRows.push(`<div class="ml-tip-row" style="max-width:280px"><span class="ml-tip-lbl">Notes</span><span style="white-space:normal">${esc(lnk.notes)}</span></div>`);;

    const mkAnchor = addr => {
      const hasScheme = /^https?:\/\//i.test(addr);
      const href = hasScheme ? addr : `http://${addr}`;
      return `<a href="${esc(href)}" target="_blank" rel="noopener noreferrer"
        style="color:var(--accent);text-decoration:none;font-family:monospace;font-size:13px">${esc(addr)}</a>`;
    };

    const mkChip = (primary, tipRows) =>
      `<span class="ml-tip">${mkAnchor(primary)}<div class="ml-tip-body">${tipRows.join('')}</div></span>`;

    const addrChips = [];

    // VLAN — prefer URI; show IP in tooltip if both present
    if (lnk.vlan_uri || lnk.vlan_ip) {
      const primary = lnk.vlan_uri || lnk.vlan_ip;
      const rows = [`<div class="ml-tip-row"><span class="ml-tip-lbl">VLAN</span><span style="font-family:monospace">${esc(primary)}</span></div>`];
      if (lnk.vlan_uri && lnk.vlan_ip)
        rows.push(`<div class="ml-tip-row"><span class="ml-tip-lbl">IP</span><span style="font-family:monospace">${esc(lnk.vlan_ip)}</span></div>`);
      rows.push(...sharedRows);
      addrChips.push(mkChip(primary, rows));
    }

    // Tailnet — prefer URI; show IP in tooltip if both present
    if (lnk.tailnet_uri || lnk.tailnet_ip) {
      const primary = lnk.tailnet_uri || lnk.tailnet_ip;
      const rows = [`<div class="ml-tip-row"><span class="ml-tip-lbl">Tailnet</span><span style="font-family:monospace">${esc(primary)}</span></div>`];
      if (lnk.tailnet_uri && lnk.tailnet_ip)
        rows.push(`<div class="ml-tip-row"><span class="ml-tip-lbl">IP</span><span style="font-family:monospace">${esc(lnk.tailnet_ip)}</span></div>`);
      rows.push(...sharedRows);
      addrChips.push(mkChip(primary, rows));
    }

    const children = sortByOrder(childMap[lnk.link_id] || []);

    return `<li style="margin-bottom:12px;list-style:none">
      <div style="display:flex;flex-wrap:wrap;align-items:baseline;gap:8px 16px">
        <span style="min-width:160px">${labelHtml}</span>
        ${addrChips.join('')}
        ${!addrChips.length ? '<span style="color:var(--text-dim);font-size:12px;font-style:italic">no addresses</span>' : ''}
      </div>
      ${children.length ? `<ul style="margin:6px 0 0 16px;padding:0">${children.map(renderLink).join('')}</ul>` : ''}
    </li>`;
  }

  let html = '';
  if (ungrouped.length) {
    html += `<section style="margin-bottom:24px">
      <ul style="margin:0;padding:0">${sortByOrder(ungrouped).map(renderLink).join('')}</ul>
    </section>`;
  }
  Object.keys(groups).sort().forEach(g => {
    html += `<section style="margin-bottom:24px">
      <h3 style="font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.7px;color:var(--text-dim);
                 border-bottom:1px solid var(--border);padding-bottom:6px;margin-bottom:10px">${esc(g)}</h3>
      <ul style="margin:0;padding:0">${sortByOrder(groups[g]).map(renderLink).join('')}</ul>
    </section>`;
  });

  container.innerHTML = html;
}

/* ── Modal: Add / Edit ───────────────────────────────────────────────────── */

function openManualLinkModal(linkId) {
  _editingLinkId = linkId || null;
  const dlg = document.getElementById('ml-modal');
  document.getElementById('ml-modal-title').textContent = linkId ? 'Edit link' : 'Add link';
  const modalErr = document.getElementById('ml-modal-error');
  if (modalErr) modalErr.hidden = true;

  const defaults = {
    link_id: '', vlan_ip: '', vlan_uri: '', tailnet_ip: '', tailnet_uri: '',
    label: '', icon: '', group_name: '', parent_id: '', sort_order: 0,
    pve_host: '', is_internet: 0, vm_id: '', vm_name: '', lxc_id: '', lxc_name: '', notes: '',
  };
  const lnk = linkId ? (_manualLinks.find(l => l.link_id === linkId) || defaults) : defaults;

  // Populate parent dropdown
  const parentSel = document.getElementById('ml-parent-id');
  parentSel.innerHTML = '<option value="">— none —</option>' +
    _manualLinks
      .filter(l => l.link_id !== linkId)
      .map(l => `<option value="${esc(l.link_id)}"${lnk.parent_id === l.link_id ? ' selected' : ''}>${esc(l.label || l.link_id.slice(0,8))}</option>`)
      .join('');

  const fields = ['vlan_ip','vlan_uri','tailnet_ip','tailnet_uri','label','icon','group_name','sort_order','pve_host','vm_id','vm_name','lxc_id','lxc_name','location','notes'];
  fields.forEach(f => {
    const el = document.getElementById(`ml-${f.replace(/_/g,'-')}`);
    if (el) el.value = lnk[f] !== null && lnk[f] !== undefined ? lnk[f] : '';
  });
  document.getElementById('ml-is-internet').checked = !!lnk.is_internet;
  parentSel.value = lnk.parent_id || '';

  dlg.showModal();
}

async function submitManualLink() {
  const modalErr = document.getElementById('ml-modal-error');
  if (modalErr) modalErr.hidden = true;
  const get = id => document.getElementById(id)?.value?.trim() ?? '';
  const body = {
    vlan_ip:     get('ml-vlan-ip')     || null,
    vlan_uri:    get('ml-vlan-uri')    || null,
    tailnet_ip:  get('ml-tailnet-ip')  || null,
    tailnet_uri: get('ml-tailnet-uri') || null,
    label:       get('ml-label')       || null,
    icon:        get('ml-icon')        || null,
    group_name:  get('ml-group-name')  || null,
    parent_id:   get('ml-parent-id')   || null,
    sort_order:  parseInt(get('ml-sort-order') || '0', 10),
    pve_host:    get('ml-pve-host')    || null,
    is_internet: document.getElementById('ml-is-internet').checked ? 1 : 0,
    vm_id:       get('ml-vm-id')       || null,
    vm_name:     get('ml-vm-name')     || null,
    lxc_id:      get('ml-lxc-id')      || null,
    lxc_name:    get('ml-lxc-name')    || null,
    location:    get('ml-location')    || null,
    notes:       get('ml-notes')       || null,
  };

  try {
    if (_editingLinkId) {
      const r = await apiFetch(`/api/v1/manual-links/${_editingLinkId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!r.ok) throw new Error((await r.json()).detail || `HTTP ${r.status}`);
    } else {
      const r = await apiFetch('/api/v1/manual-links', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!r.ok) throw new Error((await r.json()).detail || `HTTP ${r.status}`);
    }
    document.getElementById('ml-modal').close();
    await loadManualLinks();
  } catch (e) {
    if (modalErr) { modalErr.textContent = e.message; modalErr.hidden = false; }
  }
}

async function deleteManualLink(linkId) {
  if (!confirm('Delete this link?')) return;
  const err = document.getElementById('ml-error');
  if (err) err.hidden = true;
  try {
    const r = await apiFetch(`/api/v1/manual-links/${linkId}`, { method: 'DELETE' });
    if (!r.ok) throw new Error((await r.json()).detail || `HTTP ${r.status}`);
    await loadManualLinks();
  } catch (e) {
    if (err) { err.textContent = e.message; err.hidden = false; }
  }
}

/* ── Helper: setEl (local fallback if not in utils.js) ──────────────────── */
// No setEl in this codebase — direct DOM manipulation used instead (see above)
