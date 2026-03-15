/* ── pfSense DNS ──────────────────────────────────────────────────────── */
async function loadPfSenseDns() {
  const err = document.getElementById('dns-error');
  err.hidden = true;
  checkProbeStatus();   // update button state every time tab is visited
  try {
    const r = await apiFetch('/api/v1/pfsense-dns');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    _pfsenseDns = await r.json();
    renderPfSenseDns();
  } catch (e) {
    err.textContent = `Failed to load DNS entries: ${e.message}`;
    err.hidden = false;
  }
}

async function checkProbeStatus() {
  const btn    = document.getElementById('dns-probe-btn');
  const status = document.getElementById('dns-probe-status');
  try {
    const r = await apiFetch('/api/v1/pfsense-dns/probe/status');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const d = await r.json();
    if (d.configured) {
      btn.disabled = false;
      btn.title = '';
      // Only clear the status line if it's showing a config message (not a probe result)
      if (status.dataset.type === 'config') { status.hidden = true; }
    } else {
      btn.disabled = true;
      btn.title = d.reason;
      status.textContent = `⚠ Probe unavailable: ${d.reason}`;
      status.style.color = 'var(--text-dim)';
      status.dataset.type = 'config';
      status.hidden = false;
    }
  } catch (e) {
    btn.disabled = true;
    btn.title = `Could not check probe status: ${e.message}`;
  }
}

function renderPfSenseDns() {
  const q = (document.getElementById('dns-search').value || '').toLowerCase();
  const hideInactive = document.getElementById('dns-hide-inactive').checked;
  const rows = _pfsenseDns.filter(d =>
    (!hideInactive || d.active) && (
      (d.ip_address || '').toLowerCase().includes(q) ||
      (d.fqdn || '').toLowerCase().includes(q) ||
      (d.record_type || '').toLowerCase().includes(q) ||
      (d.source || '').toLowerCase().includes(q) ||
      (d.mac_address || '').toLowerCase().includes(q)
    )
  );
  const tbody = document.getElementById('dns-tbody');
  if (!rows.length) {
    const msg = hideInactive ? 'No active DNS entries match the filter.' : 'No DNS entries found.';
    tbody.innerHTML = `<tr class="empty-row"><td colspan="10">${msg}</td></tr>`;
    return;
  }
  tbody.innerHTML = rows.map(d => {
    const active = d.active ? '✓' : '✗';
    const seen    = (d.last_seen    || '—').replace('T',' ').slice(0,19);
    const probed  = (d.last_probed  || '—').replace('T',' ').slice(0,19);
    const checked = (d.last_ping_check || '—').replace('T',' ').slice(0,19);
    let pingCell;
    if (d.ping_ms == null) {
      pingCell = `<td style="text-align:right;color:var(--text-dim)">—</td>`;
    } else if (d.ping_ms < 10) {
      pingCell = `<td style="text-align:right;color:var(--ok)">${d.ping_ms.toFixed(1)}</td>`;
    } else if (d.ping_ms < 100) {
      pingCell = `<td style="text-align:right;color:var(--warn)">${d.ping_ms.toFixed(1)}</td>`;
    } else {
      pingCell = `<td style="text-align:right;color:var(--err)">${d.ping_ms.toFixed(1)}</td>`;
    }
    return `<tr>
      <td><code>${esc(d.ip_address || '')}</code></td>
      <td>${esc(d.fqdn || '')}</td>
      <td>${esc(d.record_type || '')}</td>
      <td>${esc(d.source || '')}</td>
      <td><code>${esc(d.mac_address || '—')}</code></td>
      <td style="text-align:center">${active}</td>
      <td style="white-space:nowrap;color:var(--text-dim)">${esc(seen)}</td>
      <td style="white-space:nowrap;color:var(--text-dim)">${esc(probed)}</td>
      ${pingCell}
      <td style="white-space:nowrap;color:var(--text-dim)">${esc(checked)}</td>
    </tr>`;
  }).join('');
}

async function probePfSense() {
  const btn    = document.getElementById('dns-probe-btn');
  const status = document.getElementById('dns-probe-status');
  btn.disabled = true;
  btn.textContent = '⟳ Probing…';
  status.hidden = true;
  try {
    const r = await apiFetch('/api/v1/pfsense-dns/probe', { method: 'POST' });
    const d = await r.json();
    if (!r.ok) throw new Error(d.detail || `HTTP ${r.status}`);
    const macs = d.mac_enriched ?? d.mac_addresses_found ?? 0;
    status.textContent = `✓ Probe complete — created: ${d.created ?? 0}, updated: ${d.updated ?? 0}, MACs enriched: ${macs}`;
    status.style.color = 'var(--accent)';
    status.dataset.type = 'probe';
    status.hidden = false;
    _pfsenseDns = [];
    await loadPfSenseDns();
  } catch (e) {
    status.textContent = `✗ Probe failed: ${e.message}`;
    status.style.color = '#f87171';
    status.dataset.type = 'probe';
    status.hidden = false;
  } finally {
    btn.disabled = false;
    btn.textContent = '▶ Probe pfSense';
  }
}

async function pingSweep() {
  const btn    = document.getElementById('dns-sweep-btn');
  const status = document.getElementById('dns-sweep-status');
  btn.disabled = true;
  btn.textContent = '⟳ Sweeping…';
  status.hidden = true;
  try {
    const r = await apiFetch('/api/v1/pfsense-dns/ping-sweep', { method: 'POST' });
    const d = await r.json();
    if (!r.ok) throw new Error(d.detail || `HTTP ${r.status}`);
    status.textContent = `✓ Sweep complete — reached: ${d.reached}/${d.ips_checked}, MACs found: ${d.macs_found}`;
    status.style.color = 'var(--accent)';
    status.hidden = false;
    _pfsenseDns = [];
    await loadPfSenseDns();
  } catch (e) {
    status.textContent = `✗ Sweep failed: ${e.message}`;
    status.style.color = '#f87171';
    status.hidden = false;
  } finally {
    btn.disabled = false;
    btn.textContent = '▶ Ping Sweep';
  }
}

/* ── Proxmox Config ───────────────────────────────────────────────────── */
