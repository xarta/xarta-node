/* ── PVE Hosts ─────────────────────────────────────────────────────────── */
const _LS_PVE_HOSTS    = 'bp_pve_hosts';
const _LS_PVE_HOSTS_TS = 'bp_pve_hosts_ts';
const _PVE_HOSTS_TTL   = 3_600_000; // 1 hour

function _savePveHostsCache(hosts) {
  try {
    localStorage.setItem(_LS_PVE_HOSTS, JSON.stringify(hosts));
    localStorage.setItem(_LS_PVE_HOSTS_TS, String(Date.now()));
  } catch (_) {}
}

async function loadPveHosts() {
  const err = document.getElementById('pve-hosts-error');
  err.hidden = true;
  try {
    const r = await apiFetch('/api/v1/pve-hosts');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    _pveHosts = await r.json();
    _savePveHostsCache(_pveHosts);
    renderPveHosts();
  } catch (e) {
    err.textContent = `Failed to load PVE hosts: ${e.message}`;
    err.hidden = false;
  }
}

function renderPveHosts() {
  const tbody = document.getElementById('pve-hosts-tbody');
  if (!_pveHosts.length) {
    tbody.innerHTML = '<tr class="empty-row"><td colspan="8">No PVE hosts found — run the scan first.</td></tr>';
    return;
  }
  tbody.innerHTML = _pveHosts.map(h => {
    const scanned   = (h.last_scanned || '—').replace('T',' ').slice(0,19);
    const ssh       = h.ssh_reachable ? '✅' : '—';
    const name      = esc(h.pve_name || h.hostname || h.pve_id);
    const tailnetIp = esc(h.tailnet_ip || '—');
    const id        = h.pve_id;
    return `<tr>
      <td><code>${esc(h.ip_address)}</code></td>
      <td>${name}</td>
      <td><code>${tailnetIp}</code></td>
      <td>${esc(h.version || '—')}</td>
      <td>${h.port || 8006}</td>
      <td>${ssh}</td>
      <td style="white-space:nowrap;color:var(--text-dim)">${esc(scanned)}</td>
      <td style="white-space:nowrap">
        <button class="secondary" style="padding:2px 8px;font-size:11px"
          data-pve-edit="${id}">Edit</button>
        <button class="secondary" style="padding:2px 8px;font-size:11px;color:#f87171"
          data-pve-del="${id}">Del</button>
      </td>
    </tr>`;
  }).join('');
}

async function pveHostDelete(pveId, btn) {
  if (!confirm(`Delete PVE host ${pveId}?`)) return;
  if (btn) btn.disabled = true;
  try {
    const r = await apiFetch(`/api/v1/pve-hosts/${encodeURIComponent(pveId)}`, { method: 'DELETE' });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    _pveHosts = _pveHosts.filter(h => h.pve_id !== pveId);
    _savePveHostsCache(_pveHosts);
    renderPveHosts();
  } catch (e) {
    if (btn) btn.disabled = false;
    alert(`Delete failed: ${e.message}`);
  }
}

let _pveEditingId = null;

function _openPveHostEditModal(pveId) {
  const host = _pveHosts.find(h => h.pve_id === pveId);
  if (!host) return;
  _pveEditingId = pveId;
  document.getElementById('pve-host-edit-name').value    = host.pve_name    || '';
  document.getElementById('pve-host-edit-tailnet').value = host.tailnet_ip  || '';
  document.getElementById('pve-host-edit-error').textContent = '';
  document.getElementById('pve-host-edit-save-btn').disabled = false;
  HubModal.open(document.getElementById('pve-host-edit-modal'));
  document.getElementById('pve-host-edit-name').focus();
}

async function _submitPveHostEdit() {
  const pveId = _pveEditingId;
  if (!pveId) return;
  const errorEl  = document.getElementById('pve-host-edit-error');
  const saveBtn   = document.getElementById('pve-host-edit-save-btn');
  const newName    = document.getElementById('pve-host-edit-name').value.trim();
  const newTailnet = document.getElementById('pve-host-edit-tailnet').value.trim();
  errorEl.textContent = '';
  saveBtn.disabled = true;
  try {
    const r = await apiFetch(`/api/v1/pve-hosts/${encodeURIComponent(pveId)}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ pve_name: newName || null, tailnet_ip: newTailnet || null }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const updated = await r.json();
    const idx = _pveHosts.findIndex(h => h.pve_id === pveId);
    if (idx !== -1) _pveHosts[idx] = updated;
    _savePveHostsCache(_pveHosts);
    renderPveHosts();
    HubModal.close(document.getElementById('pve-host-edit-modal'));
  } catch (e) {
    errorEl.textContent = `Save failed: ${e.message}`;
    saveBtn.disabled = false;
  }
}

async function scanPveHosts() {
  const btn    = document.getElementById('pve-scan-btn');
  const status = document.getElementById('pve-scan-status');
  if (btn) { btn.disabled = true; btn.textContent = '⟳ Scanning…'; }
  status.hidden = true;
  try {
    const r = await apiFetch('/api/v1/pve-hosts/scan', { method: 'POST' });
    const d = await r.json();
    if (!r.ok) throw new Error(d.detail || `HTTP ${r.status}`);
    const tailnetNote = (d.tailnet_resolved != null)
      ? (d.tailnet_resolved > 0
        ? `, tailnet: ${d.tailnet_resolved}/${d.found} resolved`
        : ` — tailnet IPs not resolved (use Edit to set manually)`)
      : '';
    status.textContent = `✓ Scanned ${d.ips_checked} IPs — found: ${d.found} (created: ${d.created}, updated: ${d.updated})${tailnetNote}`;
    status.style.color = 'var(--accent)';
    status.hidden = false;
    _pveHosts = [];
    await loadPveHosts();
  } catch (e) {
    status.textContent = `✗ Scan failed: ${e.message}`;
    status.style.color = '#f87171';
    status.hidden = false;
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = '▶ Scan for Proxmox'; }
  }
}

document.addEventListener('DOMContentLoaded', () => {
  // Table event delegation — Edit and Del buttons
  document.getElementById('pve-hosts-tbody')?.addEventListener('click', e => {
    const editBtn = e.target.closest('[data-pve-edit]');
    const delBtn  = e.target.closest('[data-pve-del]');
    if (editBtn) _openPveHostEditModal(editBtn.dataset.pveEdit);
    if (delBtn)  pveHostDelete(delBtn.dataset.pveDel, delBtn);
  });

  // Edit modal Save button
  document.getElementById('pve-host-edit-save-btn')?.addEventListener('click', _submitPveHostEdit);
});
