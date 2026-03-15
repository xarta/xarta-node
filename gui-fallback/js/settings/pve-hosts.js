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
          onclick="pveHostEdit('${id}', this)">Edit</button>
        <button class="secondary" style="padding:2px 8px;font-size:11px;color:#f87171"
          onclick="pveHostDelete('${id}', this)">Del</button>
      </td>
    </tr>`;
  }).join('');
}

async function pveHostDelete(pveId, btn) {
  if (!confirm(`Delete PVE host ${pveId}?`)) return;
  btn.disabled = true;
  try {
    const r = await apiFetch(`/api/v1/pve-hosts/${encodeURIComponent(pveId)}`, { method: 'DELETE' });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    _pveHosts = _pveHosts.filter(h => h.pve_id !== pveId);
    _savePveHostsCache(_pveHosts);
    renderPveHosts();
  } catch (e) {
    btn.disabled = false;
    alert(`Delete failed: ${e.message}`);
  }
}

async function pveHostEdit(pveId, btn) {
  const host = _pveHosts.find(h => h.pve_id === pveId);
  if (!host) return;
  const newName = prompt('PVE name (e.g. pveXXX):', host.pve_name || '');
  if (newName === null) return;
  const newTailnet = prompt('Tailnet IP (leave blank to clear):', host.tailnet_ip || '');
  if (newTailnet === null) return;
  btn.disabled = true;
  try {
    const r = await apiFetch(`/api/v1/pve-hosts/${encodeURIComponent(pveId)}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ pve_name: newName.trim() || null, tailnet_ip: newTailnet.trim() || null }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const updated = await r.json();
    const idx = _pveHosts.findIndex(h => h.pve_id === pveId);
    if (idx !== -1) _pveHosts[idx] = updated;
    _savePveHostsCache(_pveHosts);
    renderPveHosts();
  } catch (e) {
    btn.disabled = false;
    alert(`Edit failed: ${e.message}`);
  }
}

async function scanPveHosts() {
  const btn    = document.getElementById('pve-scan-btn');
  const status = document.getElementById('pve-scan-status');
  btn.disabled = true;
  btn.textContent = '⟳ Scanning…';
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
    btn.disabled = false;
    btn.textContent = '▶ Scan for Proxmox';
  }
}
