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
    tbody.innerHTML = '<tr class="empty-row"><td colspan="5">No entries yet — click “+ Add entry” to add one.</td></tr>';
    return;
  }
  tbody.innerHTML = _arpManual.map(e => `<tr>
    <td><code>${esc(e.ip_address)}</code></td>
    <td><code>${esc(e.mac_address)}</code></td>
    <td>${esc(e.notes || '')}</td>
    <td style="color:var(--text-dim);font-size:11px">${esc((e.updated_at || '').slice(0,16).replace('T',' '))}</td>
    <td style="white-space:nowrap">
      <button class="secondary" style="padding:1px 6px;font-size:11px" onclick="editArpManualEntry('${esc(e.entry_id)}')">✎ Edit</button>
      <button class="secondary" style="padding:1px 6px;font-size:11px;color:#f87171;border-color:#f87171;margin-left:4px" onclick="deleteArpManualEntry('${esc(e.entry_id)}')">&#x2715;</button>
    </td>
  </tr>`).join('');
}

async function addArpManualEntry() {
  const ip  = prompt('IP Address:');
  if (!ip) return;
  const mac = prompt('MAC Address:');
  if (!mac) return;
  const notes = prompt('Notes (optional):', '') ?? '';
  try {
    const r = await apiFetch('/api/v1/arp-manual', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ ip_address: ip, mac_address: mac, notes: notes || null }),
    });
    if (!r.ok) throw new Error((await r.json()).detail || `HTTP ${r.status}`);
    _arpManual = [];
    await loadArpManual();
  } catch (e) { alert(`Failed to add entry: ${e.message}`); }
}

async function editArpManualEntry(entry_id) {
  const entry = _arpManual.find(e => e.entry_id === entry_id);
  if (!entry) return;
  const ip  = prompt('IP Address:', entry.ip_address);
  if (ip === null) return;
  const mac = prompt('MAC Address:', entry.mac_address);
  if (mac === null) return;
  const notes = prompt('Notes (optional):', entry.notes || '');
  if (notes === null) return;
  try {
    const r = await apiFetch(`/api/v1/arp-manual/${encodeURIComponent(entry_id)}`, {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ ip_address: ip || null, mac_address: mac || null, notes: notes || null }),
    });
    if (!r.ok) throw new Error((await r.json()).detail || `HTTP ${r.status}`);
    await loadArpManual();
  } catch (e) { alert(`Failed to update entry: ${e.message}`); }
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
