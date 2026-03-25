let _arpManualEditId = null;

async function loadArpManual() {
  const err = document.getElementById('arp-manual-error');
  if (err) err.hidden = true;
  try {
    const r = await apiFetch('/api/v1/arp-manual');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    _arpManual = await r.json();
    renderArpManual();
  } catch (e) {
    if (err) { err.textContent = `Failed to load Manual ARP: ${e.message}`; err.hidden = false; }
  }
}

function renderArpManual() {
  const tbody = document.getElementById('arp-manual-tbody');
  if (!tbody) return;
  if (!_arpManual.length) {
    tbody.innerHTML = '<tr class="empty-row"><td colspan="5">No entries yet — click "+ Add entry" to add one.</td></tr>';
    return;
  }
  tbody.innerHTML = _arpManual.map(e => `<tr>
    <td><code>${esc(e.ip_address)}</code></td>
    <td><code>${esc(e.mac_address)}</code></td>
    <td>${esc(e.notes || '')}</td>
    <td style="color:var(--text-dim);font-size:11px">${esc((e.updated_at || '').slice(0,16).replace('T',' '))}</td>
    <td style="white-space:nowrap">
      <button class="secondary" style="padding:1px 6px;font-size:11px" data-arp-edit="${esc(e.entry_id)}">✎ Edit</button>
      <button class="secondary" style="padding:1px 6px;font-size:11px;color:#f87171;border-color:#f87171;margin-left:4px" data-arp-del="${esc(e.entry_id)}">&#x2715;</button>
    </td>
  </tr>`).join('');
}

function addArpManualEntry() {
  _openArpManualEditModal(null);
}

function _openArpManualEditModal(entry_id) {
  const modal = document.getElementById('arp-manual-edit-modal');
  if (!modal) return;
  _arpManualEditId = entry_id;
  const entry = entry_id ? _arpManual.find(e => e.entry_id === entry_id) : null;
  document.getElementById('arp-manual-edit-title').textContent = entry ? 'Edit entry' : 'Add entry';
  document.getElementById('arp-manual-edit-ip').value    = entry ? (entry.ip_address  || '') : '';
  document.getElementById('arp-manual-edit-mac').value   = entry ? (entry.mac_address || '') : '';
  document.getElementById('arp-manual-edit-notes').value = entry ? (entry.notes       || '') : '';
  document.getElementById('arp-manual-edit-error').textContent = '';
  const saveBtn = document.getElementById('arp-manual-edit-save-btn');
  if (saveBtn) saveBtn.disabled = false;
  HubModal.open(modal);
  setTimeout(() => document.getElementById('arp-manual-edit-ip').focus(), 50);
}

async function _submitArpManualEdit() {
  const modal   = document.getElementById('arp-manual-edit-modal');
  const errEl   = document.getElementById('arp-manual-edit-error');
  const saveBtn = document.getElementById('arp-manual-edit-save-btn');
  const ip    = document.getElementById('arp-manual-edit-ip').value.trim();
  const mac   = document.getElementById('arp-manual-edit-mac').value.trim();
  const notes = document.getElementById('arp-manual-edit-notes').value.trim();
  errEl.textContent = '';
  if (!ip)  { errEl.textContent = 'IP Address is required.'; return; }
  if (!mac) { errEl.textContent = 'MAC Address is required.'; return; }
  if (saveBtn) saveBtn.disabled = true;
  try {
    const isEdit = !!_arpManualEditId;
    const url    = isEdit ? `/api/v1/arp-manual/${encodeURIComponent(_arpManualEditId)}` : '/api/v1/arp-manual';
    const r = await apiFetch(url, {
      method: isEdit ? 'PUT' : 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ ip_address: ip, mac_address: mac, notes: notes || null }),
    });
    if (!r.ok) throw new Error((await r.json()).detail || `HTTP ${r.status}`);
    HubModal.close(modal);
    _arpManual = [];
    await loadArpManual();
  } catch (e) {
    errEl.textContent = `Failed to ${_arpManualEditId ? 'update' : 'add'} entry: ${e.message}`;
    if (saveBtn) saveBtn.disabled = false;
  }
}

async function deleteArpManualEntry(entry_id) {
  const entry = _arpManual.find(e => e.entry_id === entry_id);
  const ip = entry ? entry.ip_address : entry_id;
  if (!confirm(`Delete manual ARP entry for ${ip}?`)) return;
  try {
    const r = await apiFetch(`/api/v1/arp-manual/${encodeURIComponent(entry_id)}`, { method: 'DELETE' });
    if (!r.ok) throw new Error((await r.json()).detail || `HTTP ${r.status}`);
    _arpManual = _arpManual.filter(e => e.entry_id !== entry_id);
    renderArpManual();
  } catch (e) { alert(`Failed to delete entry: ${e.message}`); }
}

document.addEventListener('DOMContentLoaded', () => {
  const saveBtn = document.getElementById('arp-manual-edit-save-btn');
  if (saveBtn) saveBtn.addEventListener('click', _submitArpManualEdit);

  const tbody = document.getElementById('arp-manual-tbody');
  if (tbody) {
    tbody.addEventListener('click', e => {
      const editBtn = e.target.closest('[data-arp-edit]');
      if (editBtn) { _openArpManualEditModal(editBtn.dataset.arpEdit); return; }
      const delBtn = e.target.closest('[data-arp-del]');
      if (delBtn) { deleteArpManualEntry(delBtn.dataset.arpDel); }
    });
  }
});
