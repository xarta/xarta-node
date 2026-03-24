/* ── Machines ─────────────────────────────────────────────────────────── */
let _mchFilterTimer = null;  // debounce handle for machine-search

document.addEventListener('DOMContentLoaded', () => {
  const mchSearch = document.getElementById('machine-search');
  if (mchSearch) {
    mchSearch.addEventListener('input', () => {
      clearTimeout(_mchFilterTimer);
      _mchFilterTimer = setTimeout(renderMachines, 250);
    });
  }
  if (typeof ResponsiveLayout !== 'undefined') {
    ResponsiveLayout.registerTabControls('machines', 'pg-ctrl-machines');
  }
});
async function loadMachines() {
  const err = document.getElementById('machines-error');
  err.hidden = true;
  try {
    const r = await apiFetch('/api/v1/machines');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    _machines = await r.json();
    renderMachines();
  } catch (e) {
    err.textContent = `Failed to load machines: ${e.message}`;
    err.hidden = false;
  }
}

function renderMachines() {
  const q = (document.getElementById('machine-search').value || '').toLowerCase();
  const tbody = document.getElementById('machines-tbody');
  const visible = _machines.filter(m =>
    !q || (m.machine_id || '').toLowerCase().includes(q) ||
    (m.name || '').toLowerCase().includes(q) ||
    (m.type || '').toLowerCase().includes(q) ||
    (m.machine_kind || '').toLowerCase().includes(q) ||
    (m.description || '').toLowerCase().includes(q) ||
    JSON.stringify(m.ip_addresses || []).toLowerCase().includes(q)
  );
  if (!visible.length) {
    tbody.innerHTML = '<tr class="empty-row"><td colspan="8">' +
      (_machines.length ? 'No matching machines.' : 'No machines registered.') + '</td></tr>';
    return;
  }
  tbody.innerHTML = visible.map(m => {
    const ips = (m.ip_addresses || []).map(ip => `<span class="ip-chip">${esc(ip)}</span>`).join(' ');
    const kind = (m.machine_kind || '').toLowerCase();
    const kindCls = ['proxmox','lxc','vm','docker'].includes(kind) ? `kind-${kind}` : 'kind-default';
    const statusCls = (m.status||'') === 'active' ? 'status-deployed' :
                      (m.status||'') === 'stopped' ? 'status-planned' : '';
    return `<tr>
      <td><code style="font-size:12px;color:var(--text-dim)">${esc(m.machine_id)}</code></td>
      <td><strong>${esc(m.name || '')}</strong></td>
      <td>${esc(m.type || '')}</td>
      <td><span class="kind-badge ${kindCls}">${esc(m.machine_kind || '—')}</span></td>
      <td>${esc(m.platform || '')}</td>
      <td><code style="font-size:11px;color:var(--text-dim)">${esc(m.parent_machine_id || '—')}</code></td>
      <td><span class="${statusCls}">${esc(m.status || '')}</span></td>
      <td>${ips || '<span style="color:var(--text-dim)">—</span>'}</td>
    </tr>`;
  }).join('');
}
