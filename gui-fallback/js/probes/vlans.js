async function loadVlans() {
  const err = document.getElementById('vlans-error');
  if (err) err.hidden = true;
  try {
    const r = await apiFetch('/api/v1/vlans');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    _vlans = await r.json();
    renderVlans();
  } catch (e) {
    if (err) { err.textContent = `Failed to load VLANs: ${e.message}`; err.hidden = false; }
  }
}

function renderVlans() {
  const tbody = document.getElementById('vlans-tbody');
  if (!tbody) return;
  if (!_vlans.length) {
    tbody.innerHTML = '<tr class="empty-row"><td colspan="5">No VLANs discovered yet — run a Proxmox Config probe first.</td></tr>';
    return;
  }
  tbody.innerHTML = _vlans.map(v => {
    const srcTag = v.cidr_inferred
      ? '<span style="color:#94a3b8;font-size:11px">inferred</span>'
      : '<span style="color:#4ade80;font-size:11px">confirmed</span>';
    return `<tr>
      <td><strong>${esc(String(v.vlan_id))}</strong></td>
      <td><code>${esc(v.cidr || '—')}</code></td>
      <td>${srcTag}</td>
      <td><span id="vlan-desc-${v.vlan_id}">${esc(v.description || '')}</span></td>
      <td>
        <button class="secondary" style="padding:1px 6px;font-size:11px"
          onclick="editVlan(${v.vlan_id}, '${esc(v.cidr || '')}', '${esc(v.description || '')}')">
          ✎ Edit
        </button>
      </td>
    </tr>`;
  }).join('');
}

async function editVlan(vlan_id, currentCidr, currentDesc) {
  const cidr = prompt(`VLAN ${vlan_id} — CIDR:`, currentCidr);
  if (cidr === null) return;
  const description = prompt(`VLAN ${vlan_id} — Description (optional):`, currentDesc);
  if (description === null) return;
  try {
    const r = await apiFetch(`/api/v1/vlans/${vlan_id}`, {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ cidr: cidr || null, description: description || null }),
    });
    if (!r.ok) throw new Error((await r.json()).detail || `HTTP ${r.status}`);
    await loadVlans();
  } catch (e) {
    alert(`Failed to save VLAN: ${e.message}`);
  }
}
