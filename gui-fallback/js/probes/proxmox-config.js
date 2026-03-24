/* ── Proxmox Config ───────────────────────────────────────────────────────── */

let _pveFilterTimer = null;  // debounce handle for pve-search input

document.addEventListener('DOMContentLoaded', () => {
  const pveSearch = document.getElementById('pve-search');
  if (pveSearch) {
    pveSearch.addEventListener('input', () => {
      clearTimeout(_pveFilterTimer);
      _pveFilterTimer = setTimeout(renderProxmoxConfig, 250);
    });
  }

  const wire = (id, fn) => { const el = document.getElementById(id); if (el) el.addEventListener('click', fn); };
  wire('pve-probe-btn',                 () => probeProxmoxConfig());
  wire('pve-enrich-btn',                () => enrichNetsFromPfsense());
  wire('pve-enrich-arp-btn',            () => enrichFromPfsenseArp());
  wire('pve-findips-btn',               () => findIpsByArp());
  wire('pve-findips-pve-btn',           () => findIpsViaPve());
  wire('pve-findips-qemu-btn',          () => findIpsViaQemuAgent());
  wire('pve-findips-pfsense-sweep-btn', () => findIpsViaPfsenseSweep());
  wire('pve-probe-services-btn',        () => probeVmServices());

  if (typeof ResponsiveLayout !== 'undefined') {
    ResponsiveLayout.registerTabControls('proxmox-config', 'pg-ctrl-proxmox-config');
  }
});

async function findIpsByArp() {
  const btn    = document.getElementById('pve-findips-btn');
  const status = document.getElementById('pve-probe-status');
  btn.disabled = true;
  btn.textContent = '⏳ Scanning…';
  status.hidden = true;
  try {
    const r = await apiFetch('/api/v1/proxmox-nets/find-ips-by-arp', { method: 'POST' });
    const d = await r.json();
    if (!r.ok) throw new Error(d.detail || `HTTP ${r.status}`);
    if (d.scanned === 0) {
      status.textContent = `⚠ ${d.message}`;
      status.style.color = 'var(--text-dim)';
    } else {
      status.textContent = `✓ ARP scan complete — ${d.message}`;
      status.style.color = 'var(--accent)';
      await loadProxmoxNets();
      renderProxmoxConfig();
    }
    status.hidden = false;
  } catch (e) {
    status.textContent = `✗ ARP scan failed: ${e.message}`;
    status.style.color = '#f87171';
    status.hidden = false;
  } finally {
    btn.disabled = false;
    btn.textContent = '🔎 Find IPs by ARP';
  }
}

async function findIpsViaPve() {
  const btn    = document.getElementById('pve-findips-pve-btn');
  const status = document.getElementById('pve-probe-status');
  btn.disabled = true;
  btn.textContent = '⏳ Scanning via PVE…';
  status.hidden = true;
  try {
    const r = await apiFetch('/api/v1/proxmox-nets/find-ips-via-pve', { method: 'POST' });
    const d = await r.json();
    if (!r.ok) throw new Error(d.detail || `HTTP ${r.status}`);
    status.textContent = `✓ PVE scan complete — ${d.message}`;
    status.style.color = 'var(--accent)';
    status.hidden = false;
    if (d.found > 0 || d.vlans_added?.length) {
      await loadProxmoxNets();
      renderProxmoxConfig();
      await loadVlans();
    }
  } catch (e) {
    status.textContent = `✗ PVE scan failed: ${e.message}`;
    status.style.color = '#f87171';
    status.hidden = false;
  } finally {
    btn.disabled = false;
    btn.textContent = '🔎 Find IPs via PVE';
  }
}

async function enrichFromPfsenseArp() {
  const btn    = document.getElementById('pve-enrich-arp-btn');
  const status = document.getElementById('pve-probe-status');
  btn.disabled = true;
  btn.textContent = '⏳ Reading pfSense ARP…';
  status.hidden = true;
  try {
    const r = await apiFetch('/api/v1/proxmox-nets/enrich-from-pfsense-arp', { method: 'POST' });
    const d = await r.json();
    if (!r.ok) throw new Error(d.detail || `HTTP ${r.status}`);
    status.textContent = `✓ pfSense ARP — ${d.message}`;
    status.style.color = 'var(--accent)';
    status.hidden = false;
    if (d.enriched > 0) {
      await loadProxmoxNets();
      renderProxmoxConfig();
      await loadVlans();
    }
  } catch (e) {
    status.textContent = `✗ pfSense ARP failed: ${e.message}`;
    status.style.color = '#f87171';
    status.hidden = false;
  } finally {
    btn.disabled = false;
    btn.textContent = '⇑ Fill IPs from pfSense ARP';
  }
}

async function findIpsViaQemuAgent() {
  const btn    = document.getElementById('pve-findips-qemu-btn');
  const status = document.getElementById('pve-probe-status');
  btn.disabled = true;
  btn.textContent = '⏳ Querying QEMU agents…';
  status.hidden = true;
  try {
    const r = await apiFetch('/api/v1/proxmox-nets/find-ips-via-qemu-agent', { method: 'POST' });
    const d = await r.json();
    if (!r.ok) throw new Error(d.detail || `HTTP ${r.status}`);
    status.textContent = `✓ QEMU agent — ${d.message}`;
    status.style.color = 'var(--accent)';
    status.hidden = false;
    if (d.found > 0) { await loadProxmoxNets(); renderProxmoxConfig(); }
  } catch (e) {
    status.textContent = `✗ QEMU agent failed: ${e.message}`;
    status.style.color = '#f87171';
    status.hidden = false;
  } finally {
    btn.disabled = false;
    btn.textContent = '\uD83D\uDD17 Find IPs via QEMU Agent';
  }
}

async function findIpsViaPfsenseSweep() {
  const btn    = document.getElementById('pve-findips-pfsense-sweep-btn');
  const status = document.getElementById('pve-probe-status');
  btn.disabled = true;
  btn.textContent = '⏳ pfSense sweep…';
  status.hidden = true;
  try {
    const r = await apiFetch('/api/v1/proxmox-nets/find-ips-via-pfsense-sweep', { method: 'POST' });
    const d = await r.json();
    if (!r.ok) throw new Error(d.detail || `HTTP ${r.status}`);
    status.textContent = `✓ pfSense sweep — ${d.message}`;
    status.style.color = 'var(--accent)';
    status.hidden = false;
    if (d.found > 0) { await loadProxmoxNets(); renderProxmoxConfig(); await loadVlans(); }
  } catch (e) {
    status.textContent = `✗ pfSense sweep failed: ${e.message}`;
    status.style.color = '#f87171';
    status.hidden = false;
  } finally {
    btn.disabled = false;
    btn.textContent = '\uD83D\uDD0E Find IPs via pfSense sweep';
  }
}

async function deleteProxmoxNet(net_id, net_key) {
  if (!confirm(`Delete NIC "${net_key}" (${net_id})?\nThis removes it from Blueprints only — not from Proxmox.`)) return;
  try {
    const r = await apiFetch(`/api/v1/proxmox-nets/${encodeURIComponent(net_id)}`, { method: 'DELETE' });
    if (!r.ok && r.status !== 204) { const d = await r.json().catch(()=>{}); throw new Error(d?.detail || `HTTP ${r.status}`); }
    await loadProxmoxNets();
    renderProxmoxConfig();
  } catch (e) {
    alert(`Failed to delete NIC: ${e.message}`);
  }
}

async function deleteProxmoxConfig(config_id) {
  if (!confirm(`Delete "${config_id}" from Blueprints?\nThis removes it from all nodes — the VM/LXC itself is unaffected.`)) return;
  try {
    const r = await apiFetch(`/api/v1/proxmox-config/${encodeURIComponent(config_id)}`, { method: 'DELETE' });
    if (!r.ok && r.status !== 204) { const d = await r.json().catch(()=>{}); throw new Error(d?.detail || `HTTP ${r.status}`); }
    await loadProxmoxConfig();
  } catch (e) {
    alert(`Failed to delete config: ${e.message}`);
  }
}

async function loadProxmoxConfig() {
  const err = document.getElementById('pve-error');
  err.hidden = true;
  checkProxmoxProbeStatus();
  try {
    const r = await apiFetch('/api/v1/proxmox-config');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    _proxmoxConfig = await r.json();
    await loadProxmoxNets();
    renderProxmoxConfig();
    loadVlans(); // refresh VLAN tab whenever proxmox config is loaded
  } catch (e) {
    err.textContent = `Failed to load Proxmox config: ${e.message}`;
    err.hidden = false;
  }
}

async function checkProxmoxProbeStatus() {
  const btn = document.getElementById('pve-probe-btn');
  const status = document.getElementById('pve-probe-status');
  try {
    const r = await apiFetch('/api/v1/proxmox-config/probe/status');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const d = await r.json();
    btn.disabled = !d.configured;
    if (!d.configured) {
      btn.title = d.reason;
      status.textContent = `⚠ Probe unavailable: ${d.reason}`;
      status.style.color = 'var(--text-dim)';
      status.hidden = false;
    }
  } catch (e) {
    btn.disabled = true;
    btn.title = `Could not check probe status: ${e.message}`;
  }
}

function renderProxmoxConfig() {
  const q = (document.getElementById('pve-search').value || '').toLowerCase();
  const rows = _proxmoxConfig.filter(d =>
    (d.pve_name || '').toLowerCase().includes(q) ||
    String(d.vmid || '').toLowerCase().includes(q) ||
    (d.name     || '').toLowerCase().includes(q) ||
    (d.ip_address || '').toLowerCase().includes(q) ||
    (d.tags     || '').toLowerCase().includes(q)
  );
  const tbody = document.getElementById('pve-tbody');
  const stepsToggleBtn     = document.getElementById('pve-steps-toggle-btn');
  const expandAllBtn       = document.getElementById('pve-expand-all-btn');
  const collapseAllBtn     = document.getElementById('pve-collapse-all-btn');
  if (!rows.length) {
    tbody.innerHTML = `<tr class="empty-row"><td colspan="12">No Proxmox configs found.</td></tr>`;
    if (stepsToggleBtn) stepsToggleBtn.hidden = true;
    if (expandAllBtn)   expandAllBtn.hidden   = true;
    if (collapseAllBtn) collapseAllBtn.hidden = true;
    return;
  }

  // Preserve open PVE groups across re-renders (only when not filtering)
  const openPveGroups = new Set();
  if (!q) {
    tbody.querySelectorAll('[data-pve-group-hdr]').forEach(el => {
      if (el.dataset.pveGroupOpen === '1') openPveGroups.add(el.dataset.pveGroupHdr);
    });
  }

  const hasAnyNets = rows.some(d => (_proxmoxNetsMap[d.config_id] || []).length > 0);
  if (stepsToggleBtn) stepsToggleBtn.hidden = false;
  if (expandAllBtn)   expandAllBtn.hidden   = !hasAnyNets;
  if (collapseAllBtn) collapseAllBtn.hidden = !hasAnyNets;

  // Sort by pve_name then vmid
  rows.sort((a, b) => {
    const c = (a.pve_name || '').localeCompare(b.pve_name || '');
    return c !== 0 ? c : (a.vmid || 0) - (b.vmid || 0);
  });

  // Group by pve_name
  const groups = new Map();
  for (const d of rows) {
    const pve = d.pve_name || '';
    if (!groups.has(pve)) groups.set(pve, []);
    groups.get(pve).push(d);
  }

  const html = [];
  for (const [pve, pveRows] of groups) {
    const safePve = 'pg' + pve.replace(/[^a-zA-Z0-9]/g, '_');
    const isOpen  = q.length > 0 || openPveGroups.has(safePve);

    // Build type summary for group header
    const lxcCnt  = pveRows.filter(d => d.vm_type === 'lxc').length;
    const qemuCnt = pveRows.filter(d => d.vm_type === 'qemu').length;
    const typeSummary = [
      qemuCnt ? `${qemuCnt} VM${qemuCnt !== 1 ? 's' : ''}` : '',
      lxcCnt  ? `${lxcCnt} LXC${lxcCnt !== 1 ? 's' : ''}` : '',
    ].filter(Boolean).join(', ') || `${pveRows.length} machine${pveRows.length !== 1 ? 's' : ''}`;

    // PVE group header row
    html.push(`<tr data-pve-group-hdr="${safePve}" data-pve-group-open="${isOpen ? '1' : '0'}"
        style="cursor:pointer;background:var(--surface);border-top:2px solid var(--border)"
        onclick="togglePveGroup('${safePve}')">
      <td colspan="12" style="padding:7px 10px;font-weight:600"><span id="pve-grp-arrow-${safePve}" style="font-size:10px;color:var(--text-dim);margin-right:6px">${isOpen ? '▼' : '▶'}</span><code>${esc(pve)}</code><span style="font-size:11px;font-weight:normal;color:var(--text-dim);margin-left:8px">${typeSummary}</span></td>
    </tr>`);

    for (const d of pveRows) {
      const probed  = (d.last_probed || '—').replace('T',' ').slice(0,19);
      const safeid  = d.config_id.replace(/[^a-zA-Z0-9_-]/g, '_');
      const nets    = _proxmoxNetsMap[d.config_id] || [];

      // Networks cell: toggle button + first IP
      let netsCell;
      if (nets.length === 0) {
        netsCell = d.ip_address ? `<code>${esc(d.ip_address)}</code>` : '<span style="color:var(--text-dim)">—</span>';
      } else {
        const firstIp = nets[0].ip_address || '—';
        netsCell = `<button class="secondary" style="padding:1px 5px;font-size:11px;margin-right:4px" onclick="toggleNets('${safeid}')" id="nets-btn-${safeid}">▶ ${nets.length}</button><code>${esc(firstIp)}</code>`;
      }

      // Detected services badges — driven from JSON columns
      const badges = [];
      const _dockge    = (() => { try { return JSON.parse(d.dockge_json    || '[]'); } catch(e) { return []; } })();
      const _portainer = (() => { try { return JSON.parse(d.portainer_json || '[]'); } catch(e) { return []; } })();
      const _caddy     = (() => { try { return JSON.parse(d.caddy_json     || '[]'); } catch(e) { return []; } })();
      if (d.has_docker) badges.push('<span class="tag" style="background:#1d4ed8;color:#fff">docker</span>');
      _portainer.forEach(p => badges.push(`<span class="tag" style="background:#0f766e;color:#fff" title="${esc(p.data_dir||p.method||'')}">portainer</span>`));
      _caddy.forEach(c    => badges.push(`<span class="tag" style="background:#15803d;color:#fff" title="${esc(c.caddyfile||'')}">caddy</span>`));
      _dockge.forEach(g   => badges.push(`<span class="tag" style="background:#92400e;color:#fff" title="${esc(g.stacks_dir||g.container||'')}">dockge</span>`));
      const detectedHtml = badges.length ? badges.join(' ') : '<span style="color:var(--text-dim)">—</span>';

      const mainRow = `<tr data-pve-group="${safePve}" data-vm-row="${safeid}" style="display:${isOpen ? 'table-row' : 'none'}">
        <td><code>${esc(d.pve_name || '')}</code></td>
        <td>${esc(String(d.vmid || ''))}</td>
        <td>${esc(d.vm_type || '')}</td>
        <td>${esc(d.name || '')}</td>
        <td>${esc(d.status || '—')}</td>
        <td style="text-align:right">${d.cores ?? '—'}</td>
        <td style="text-align:right">${d.memory_mb ?? '—'}</td>
        <td>${netsCell}</td>
        <td>${detectedHtml}</td>
        <td>${esc(d.tags || '—')}</td>
        <td style="white-space:nowrap;color:var(--text-dim)">${esc(probed)}</td>
        <td style="text-align:right;white-space:nowrap"><button class="secondary" style="padding:1px 6px;font-size:11px;color:#f87171;border-color:#f87171" onclick="deleteProxmoxConfig('${esc(d.config_id)}')">Del</button></td>
      </tr>`;

      let detailRow = '';
      if (nets.length > 0) {
        const netsHtml = nets.map(n => {
          const srcTag = n.ip_source && n.ip_source !== 'conf'
            ? ` <span style="color:#94a3b8;font-size:10px">(${esc(n.ip_source)})</span>` : '';
          return `<tr style="background:var(--bg-alt,#161b22)">
            <td style="padding:2px 4px 2px 16px;color:var(--text-dim);font-size:11px;white-space:nowrap">${esc(n.net_key)} <button class="secondary" style="padding:1px 5px;font-size:10px;color:#f87171;border-color:#f87171;margin-left:4px" onclick="deleteProxmoxNet('${esc(n.net_id)}','${esc(n.net_key)}')">&#x2715;</button></td>
            <td colspan="2"><code style="font-size:11px">${esc(n.ip_address || '—')}</code>${srcTag}</td>
            <td><code style="font-size:11px">${esc(n.mac_address || '—')}</code></td>
            <td style="font-size:11px">${n.vlan_tag ?? '—'}</td>
            <td style="font-size:11px;color:var(--text-dim)">${esc(n.bridge || '—')}</td>
            <td style="font-size:11px;color:var(--text-dim)">${esc(n.model || '—')}</td>
            <td colspan="4"></td>
          </tr>`;
        }).join('');
        detailRow = `<tr id="nets-detail-${safeid}" data-pve-group="${safePve}" data-nets-detail="1" style="display:none">
          <td colspan="12" style="padding:0;border-top:1px solid var(--border,#30363d)">
            <table style="width:100%;border-collapse:collapse">
              <thead><tr style="font-size:10px;color:var(--text-dim);background:var(--bg-darker,#0d1117)">
                <th style="padding:2px 4px 2px 16px;text-align:left">NIC</th>
                <th colspan="2" style="text-align:left">IP</th>
                <th style="text-align:left">MAC</th>
                <th style="text-align:left">VLAN</th>
                <th style="text-align:left">Bridge</th>
                <th style="text-align:left">Model</th>
                <th colspan="4"></th>
              </tr></thead>
              <tbody>${netsHtml}</tbody>
            </table>
          </td>
        </tr>`;
      }
      html.push(mainRow, detailRow);
    }
  }
  tbody.innerHTML = html.join('');
}

function toggleNets(safeid) {
  const detail = document.getElementById(`nets-detail-${safeid}`);
  const btn    = document.getElementById(`nets-btn-${safeid}`);
  if (!detail) return;
  const open = detail.style.display !== 'none';
  detail.style.display = open ? 'none' : 'table-row';
  if (btn) btn.textContent = btn.textContent.replace(open ? '▼' : '▶', open ? '▶' : '▼');
}

function togglePveGroup(safePve) {
  const hdr     = document.querySelector(`[data-pve-group-hdr="${safePve}"]`);
  const vmRows  = document.querySelectorAll(`[data-pve-group="${safePve}"]:not([data-nets-detail])`);
  const allRows = document.querySelectorAll(`[data-pve-group="${safePve}"]`);
  const arrow   = document.getElementById(`pve-grp-arrow-${safePve}`);
  const isOpen  = hdr && hdr.dataset.pveGroupOpen === '1';
  if (isOpen) {
    allRows.forEach(r => r.style.display = 'none');
    if (hdr)   hdr.dataset.pveGroupOpen = '0';
    if (arrow) arrow.textContent = '▶';
  } else {
    vmRows.forEach(r => r.style.display = 'table-row');
    if (hdr)   hdr.dataset.pveGroupOpen = '1';
    if (arrow) arrow.textContent = '▼';
  }
}

function setAllNets(open) {
  if (open) {
    // Expand all PVE groups first so VM rows are visible
    document.querySelectorAll('[data-pve-group-hdr]').forEach(hdr => {
      if (hdr.dataset.pveGroupOpen !== '1') togglePveGroup(hdr.dataset.pveGroupHdr);
    });
  }
  document.querySelectorAll('[id^="nets-detail-"]').forEach(detail => {
    const safeid = detail.id.replace('nets-detail-', '');
    const btn    = document.getElementById(`nets-btn-${safeid}`);
    detail.style.display = open ? 'table-row' : 'none';
    if (btn) btn.textContent = btn.textContent.replace(open ? '▶' : '▼', open ? '▼' : '▶');
  });
  if (!open) {
    // Collapse all PVE groups too — mirrors what Expand All opened
    document.querySelectorAll('[data-pve-group-hdr]').forEach(hdr => {
      if (hdr.dataset.pveGroupOpen === '1') togglePveGroup(hdr.dataset.pveGroupHdr);
    });
  }
}

async function loadProxmoxNets() {
  try {
    const r = await apiFetch('/api/v1/proxmox-nets');
    if (!r.ok) return;
    const nets = await r.json();
    _proxmoxNetsMap = {};
    for (const n of nets) {
      if (!_proxmoxNetsMap[n.config_id]) _proxmoxNetsMap[n.config_id] = [];
      _proxmoxNetsMap[n.config_id].push(n);
    }
  } catch (e) { /* non-fatal */ }
}

async function enrichNetsFromPfsense() {
  const btn    = document.getElementById('pve-enrich-btn');
  const status = document.getElementById('pve-probe-status');
  btn.disabled = true;
  status.hidden = true;
  try {
    const r = await apiFetch('/api/v1/proxmox-nets/enrich-from-pfsense', { method: 'POST' });
    const d = await r.json();
    if (!r.ok) throw new Error(d.detail || `HTTP ${r.status}`);
    status.textContent = `⇑ Enriched ${d.enriched ?? 0} of ${d.checked ?? 0} net interfaces from pfSense`;
    status.style.color = 'var(--accent)';
    status.hidden = false;
    await loadProxmoxNets();
    renderProxmoxConfig();
  } catch (e) {
    status.textContent = `✗ Enrich failed: ${e.message}`;
    status.style.color = '#f87171';
    status.hidden = false;
  } finally {
    btn.disabled = false;
  }
}

function togglePveSteps() {
  const panel = document.getElementById('pve-steps-panel');
  const btn   = document.getElementById('pve-steps-toggle-btn');
  const open  = panel.style.display !== 'none';
  panel.style.display = open ? 'none' : 'flex';
  if (btn) btn.innerHTML = open ? '&#9881; Steps &#9663;' : '&#9881; Steps &#9652;';
}

async function fullProbeProxmox() {
  const btn    = document.getElementById('pve-full-probe-btn');
  const status = document.getElementById('pve-probe-status');
  if (btn) { btn.disabled = true; btn.textContent = '⏳ Running…'; }
  status.style.color = 'var(--text-dim)';
  status.hidden = false;

  // [label, url, refreshNets after step]
  const STEPS = [
    ['1/8 Probing PVE configs',        '/api/v1/proxmox-config/probe',                    false],
    ['2/8 Fill IPs from pfSense',      '/api/v1/proxmox-nets/enrich-from-pfsense',         true],
    ['3/8 Fill IPs from pfSense ARP',  '/api/v1/proxmox-nets/enrich-from-pfsense-arp',     true],
    ['4/8 Find IPs by ARP scan',       '/api/v1/proxmox-nets/find-ips-by-arp',             true],
    ['5/8 Find IPs via PVE API',       '/api/v1/proxmox-nets/find-ips-via-pve',            true],
    ['6/8 Find IPs via QEMU agents',   '/api/v1/proxmox-nets/find-ips-via-qemu-agent',     true],
    ['7/8 Find IPs via pfSense sweep', '/api/v1/proxmox-nets/find-ips-via-pfsense-sweep',  true],
    ['8/8 Probe VM services via SSH',  '/api/v1/proxmox-config/probe-services',            false],
  ];

  const stepResults = [];
  for (const [label, url, refreshNets] of STEPS) {
    status.textContent = `⏳ ${label}…`;
    try {
      const r = await apiFetch(url, { method: 'POST' });
      const d = await r.json().catch(() => ({}));
      if (!r.ok) {
        stepResults.push(`⚠ ${label}: ${d.detail || 'HTTP ' + r.status}`);
      } else {
        const msg = d.message || '';
        stepResults.push(`✓ ${label}${msg ? ': ' + msg : ''}`);
        if (refreshNets) { await loadProxmoxNets(); renderProxmoxConfig(); }
      }
    } catch (e) {
      stepResults.push(`✗ ${label}: ${e.message}`);
    }
  }
  await loadProxmoxConfig();
  const ok = stepResults.filter(s => s.startsWith('✓')).length;
  status.textContent = `⚡ Full probe done — ${ok}/${STEPS.length} steps succeeded`;
  status.style.color = ok >= STEPS.length - 1 ? 'var(--accent)' : '#f59e0b';
  if (btn) { btn.disabled = false; btn.textContent = '⚡ Full Probe'; }
  console.log('Full probe results:', stepResults);
}

async function probeVmServices() {
  const btn    = document.getElementById('pve-probe-services-btn');
  const status = document.getElementById('pve-probe-status');
  btn.disabled = true;
  btn.textContent = '⏳ Probing VM services…';
  status.hidden = true;
  try {
    const r = await apiFetch('/api/v1/proxmox-config/probe-services', { method: 'POST' });
    const d = await r.json();
    if (!r.ok) throw new Error(d.detail || `HTTP ${r.status}`);
    status.textContent = `✓ VM services — ${d.message}`;
    status.style.color = 'var(--accent)';
    status.hidden = false;
    if (d.updated > 0) { await loadProxmoxConfig(); }
  } catch (e) {
    status.textContent = `✗ VM services probe failed: ${e.message}`;
    status.style.color = '#f87171';
    status.hidden = false;
  } finally {
    btn.disabled = false;
    btn.textContent = '&#128065; Probe VM Services';
  }
}

async function probeProxmoxConfig() {
  const btn    = document.getElementById('pve-probe-btn');
  const status = document.getElementById('pve-probe-status');
  btn.disabled = true;
  btn.textContent = '⟳ Probing…';
  status.hidden = true;
  try {
    const r = await apiFetch('/api/v1/proxmox-config/probe', { method: 'POST' });
    const d = await r.json();
    if (!r.ok) throw new Error(d.detail || `HTTP ${r.status}`);
    const nc = d.nets_created ?? 0, nu = d.nets_updated ?? 0;
    status.textContent = `✓ Done — ${d.total ?? 0} VMs (${d.created ?? 0} new, ${d.updated ?? 0} updated)`
      + (nc + nu ? `; ${nc + nu} net interfaces (${nc} new, ${nu} updated)` : '');
    status.style.color = 'var(--accent)';
    status.hidden = false;
    _proxmoxConfig = [];
    await loadProxmoxConfig();
  } catch (e) {
    status.textContent = `✗ Probe failed: ${e.message}`;
    status.style.color = '#f87171';
    status.hidden = false;
  } finally {
    btn.disabled = false;
    btn.textContent = '▶ Probe PVE';
  }
}
