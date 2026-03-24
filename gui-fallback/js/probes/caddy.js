document.addEventListener('DOMContentLoaded', () => {
  let _caddyFilterTimer = null;
  const searchEl = document.getElementById('caddy-search');
  if (searchEl) searchEl.addEventListener('input', () => {
    clearTimeout(_caddyFilterTimer);
    _caddyFilterTimer = setTimeout(renderCaddyConfigs, 250);
  });
  if (typeof ResponsiveLayout !== 'undefined') {
    ResponsiveLayout.registerTabControls('caddy-configs', 'pg-ctrl-caddy-configs');
  }
});

async function loadCaddyConfigs() {
  const err = document.getElementById('caddy-error');
  err.hidden = true;
  checkCaddyProbeStatus();
  try {
    const r = await apiFetch('/api/v1/caddy-configs');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    _caddyConfigs = await r.json();
    renderCaddyConfigs();
  } catch (e) {
    err.textContent = `Failed to load Caddy configs: ${e.message}`;
    err.hidden = false;
  }
}

async function checkCaddyProbeStatus() {
  const btn = document.getElementById('caddy-probe-btn');
  const status = document.getElementById('caddy-probe-status');
  try {
    const r = await apiFetch('/api/v1/caddy-configs/probe/status');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const d = await r.json();
    if (btn) btn.disabled = !d.configured;
    if (!d.configured) {
      if (btn) btn.title = d.reason;
      status.textContent = `⚠ Probe unavailable: ${d.reason}`;
      status.style.color = 'var(--text-dim)';
      status.hidden = false;
    }
  } catch (e) {
    if (btn) btn.disabled = true;
  }
}

function renderCaddyConfigs() {
  const q = (document.getElementById('caddy-search').value || '').toLowerCase();
  const rows = _caddyConfigs.filter(d =>
    (d.pve_host         || '').toLowerCase().includes(q) ||
    (d.source_lxc_name  || '').toLowerCase().includes(q) ||
    (d.source_vmid      || '').toLowerCase().includes(q) ||
    (d.domains_json     || '').toLowerCase().includes(q) ||
    (d.upstreams_json   || '').toLowerCase().includes(q)
  );
  const tbody = document.getElementById('caddy-tbody');
  if (!rows.length) {
    tbody.innerHTML = `<tr class="empty-row"><td colspan="7">No Caddy configs found.</td></tr>`;
    return;
  }
  tbody.innerHTML = rows.map(d => {
    const probed = (d.last_probed || '—').replace('T',' ').slice(0,19);
    let domains = '—';
    try { const a = JSON.parse(d.domains_json || ''); domains = Array.isArray(a) ? a.join(', ') : '—'; } catch(_) {}
    let upstreams = '—';
    try { const a = JSON.parse(d.upstreams_json || ''); upstreams = Array.isArray(a) ? a.join(', ') : '—'; } catch(_) {}
    return `<tr>
      <td><code>${esc(d.pve_host || '')}</code></td>
      <td>${esc(d.source_vmid || '')}</td>
      <td>${esc(d.source_lxc_name || '—')}</td>
      <td style="font-size:11px;color:var(--text-dim)">${esc(d.caddyfile_path || '—')}</td>
      <td style="font-size:12px">${esc(domains)}</td>
      <td style="font-size:12px">${esc(upstreams)}</td>
      <td style="white-space:nowrap;color:var(--text-dim)">${esc(probed)}</td>
    </tr>`;
  }).join('');
}

async function probeCaddyConfigs() {
  const btn    = document.getElementById('caddy-probe-btn');
  const status = document.getElementById('caddy-probe-status');
  if (btn) { btn.disabled = true; btn.textContent = '⟳ Probing…'; }
  status.hidden = true;
  try {
    const r = await apiFetch('/api/v1/caddy-configs/probe', { method: 'POST' });
    const d = await r.json();
    if (!r.ok) throw new Error(d.detail || `HTTP ${r.status}`);
    status.textContent = `✓ Done — created: ${d.created ?? 0}, updated: ${d.updated ?? 0}, hosts: ${d.pve_hosts_probed ?? 0}`;
    status.style.color = 'var(--accent)';
    status.hidden = false;
    _caddyConfigs = [];
    await loadCaddyConfigs();
  } catch (e) {
    status.textContent = `✗ Probe failed: ${e.message}`;
    status.style.color = '#f87171';
    status.hidden = false;
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = '▶ Probe Caddy'; }
  }
}
