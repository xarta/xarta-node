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
          data-vlan-id="${v.vlan_id}" data-vlan-cidr="${esc(v.cidr || '')}" data-vlan-desc="${esc(v.description || '')}">
          ✎ Edit
        </button>
      </td>
    </tr>`;
  }).join('');

  tbody.querySelectorAll('button[data-vlan-id]').forEach(btn => {
    btn.addEventListener('click', () =>
      openVlanModal(+btn.dataset.vlanId, btn.dataset.vlanCidr, btn.dataset.vlanDesc)
    );
  });
}

let _editingVlanId = null;

function openVlanModal(vlan_id, currentCidr, currentDesc) {
  _editingVlanId = vlan_id;
  document.getElementById('vlan-modal-title').textContent = `Edit VLAN ${vlan_id}`;
  document.getElementById('vlan-modal-cidr').value = currentCidr || '';
  document.getElementById('vlan-modal-desc').value = currentDesc || '';
  document.getElementById('vlan-modal-error').textContent = '';
  HubModal.open(document.getElementById('vlan-modal'));
}

async function submitVlanEdit() {
  const errEl = document.getElementById('vlan-modal-error');
  const cidr = document.getElementById('vlan-modal-cidr').value.trim();
  const description = document.getElementById('vlan-modal-desc').value.trim();
  errEl.textContent = '';
  try {
    const r = await apiFetch(`/api/v1/vlans/${_editingVlanId}`, {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ cidr: cidr || null, description: description || null }),
    });
    if (!r.ok) throw new Error((await r.json()).detail || `HTTP ${r.status}`);
    HubModal.close(document.getElementById('vlan-modal'));
    await loadVlans();
  } catch (e) {
    errEl.textContent = `Failed to save VLAN: ${e.message}`;
  }
}

document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('vlan-modal-save-btn')?.addEventListener('click', submitVlanEdit);
});
