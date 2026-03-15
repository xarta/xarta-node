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

  // Preserve which groups are open across re-renders (only when not filtering)
  const openGroups = new Set();
  if (!q) {
    tbody.querySelectorAll('[data-dns-group-hdr]').forEach(el => {
      if (el.dataset.dnsGroupOpen === '1') openGroups.add(el.dataset.dnsGroupHdr);
    });
  }

  // Sort numerically by IP, then by FQDN within each group
  rows.sort((a, b) => {
    const c = _ipCmp(a.ip_address || '', b.ip_address || '');
    return c !== 0 ? c : (a.fqdn || '').localeCompare(b.fqdn || '');
  });

  // Group by IP address
  const groups = new Map();
  for (const d of rows) {
    const ip = d.ip_address || '';
    if (!groups.has(ip)) groups.set(ip, []);
    groups.get(ip).push(d);
  }

  const html = [];
  for (const [ip, records] of groups) {
    const safeip = 'dg' + ip.replace(/\./g, '_');
    const isOpen = q.length > 0 || openGroups.has(safeip);
    const activeCount = records.filter(r => r.active).length;
    const mac  = records.find(r => r.mac_address)?.mac_address || '—';
    const bestPing = records.reduce(
      (b, r) => r.ping_ms != null && (b == null || r.ping_ms < b) ? r.ping_ms : b, null);
    let pingSummary = '';
    if (bestPing != null) {
      const pc = bestPing < 10 ? 'var(--ok)' : bestPing < 100 ? 'var(--warn)' : 'var(--err)';
      pingSummary = ` &middot; <span style="color:${pc}">${bestPing.toFixed(1)} ms</span>`;
    }

    // Group header row
    html.push(`<tr data-dns-group-hdr="${safeip}" data-dns-group-open="${isOpen ? '1' : '0'}"
        style="cursor:pointer;background:var(--surface);border-top:2px solid var(--border)"
        onclick="toggleDnsGroup('${safeip}')">
      <td style="font-weight:600"><span id="dns-grp-arrow-${safeip}" style="font-size:10px;color:var(--text-dim);margin-right:5px">${isOpen ? '▼' : '▶'}</span><code>${esc(ip)}</code></td>
      <td colspan="3" style="color:var(--text-dim);font-size:12px">${records.length} record${records.length !== 1 ? 's' : ''}${activeCount < records.length ? ` &middot; ${activeCount} active` : ''}${pingSummary}</td>
      <td><code style="font-size:11px">${esc(mac)}</code></td>
      <td style="text-align:center">${activeCount > 0 ? '<span style="color:var(--ok)">✓</span>' : '<span style="color:var(--err)">✗</span>'}</td>
      <td colspan="4"></td>
    </tr>`);

    // Detail rows for this IP
    for (const d of records) {
      const active  = d.active ? '<span style="color:var(--ok)">✓</span>' : '<span style="color:var(--text-dim)">✗</span>';
      const seen    = (d.last_seen       || '—').replace('T',' ').slice(0,19);
      const probed  = (d.last_probed     || '—').replace('T',' ').slice(0,19);
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
      html.push(`<tr data-dns-ip="${safeip}" style="display:${isOpen ? 'table-row' : 'none'}">
        <td style="padding-left:20px;color:var(--text-dim);font-size:11px">↳</td>
        <td>${esc(d.fqdn || '')}</td>
        <td>${esc(d.record_type || '')}</td>
        <td>${esc(d.source || '')}</td>
        <td><code style="font-size:11px">${esc(d.mac_address || '—')}</code></td>
        <td style="text-align:center">${active}</td>
        <td style="white-space:nowrap;color:var(--text-dim)">${esc(seen)}</td>
        <td style="white-space:nowrap;color:var(--text-dim)">${esc(probed)}</td>
        ${pingCell}
        <td style="white-space:nowrap;color:var(--text-dim)">${esc(checked)}</td>
      </tr>`);
    }
  }
  tbody.innerHTML = html.join('');
}

function toggleDnsGroup(safeip) {
  const hdr   = document.querySelector(`[data-dns-group-hdr="${safeip}"]`);
  const rows  = document.querySelectorAll(`[data-dns-ip="${safeip}"]`);
  const arrow = document.getElementById(`dns-grp-arrow-${safeip}`);
  const isOpen = hdr && hdr.dataset.dnsGroupOpen === '1';
  rows.forEach(r => r.style.display = isOpen ? 'none' : 'table-row');
  if (hdr)   hdr.dataset.dnsGroupOpen = isOpen ? '0' : '1';
  if (arrow) arrow.textContent = isOpen ? '▶' : '▼';
}

function _ipCmp(a, b) {
  const pa = a.split('.').map(n => parseInt(n, 10) || 0);
  const pb = b.split('.').map(n => parseInt(n, 10) || 0);
  for (let i = 0; i < 4; i++) {
    const d = (pa[i] || 0) - (pb[i] || 0);
    if (d !== 0) return d;
  }
  return 0;
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
