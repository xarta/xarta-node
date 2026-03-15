/* ── Dockge Stacks ────────────────────────────────────────────────────── */
/* ── Dockge Stacks ──────────────────────────────────────────────────────── */
async function loadDockgeStacks() {
  const err = document.getElementById('dockge-error');
  err.hidden = true;
  checkDockgeProbeStatus();
  try {
    const [stacksRes, svcsRes] = await Promise.all([
      apiFetch('/api/v1/dockge-stacks'),
      apiFetch('/api/v1/dockge-stacks/services'),
    ]);
    if (!stacksRes.ok) throw new Error(`HTTP ${stacksRes.status}`);
    _dockgeStacks = await stacksRes.json();
    _dockgeServicesMap = {};
    if (svcsRes.ok) {
      const svcs = await svcsRes.json();
      for (const s of svcs) {
        if (!_dockgeServicesMap[s.stack_id]) _dockgeServicesMap[s.stack_id] = [];
        _dockgeServicesMap[s.stack_id].push(s);
      }
    }
    const hasStacks = _dockgeStacks.length > 0;
    const expandAllBtn   = document.getElementById('dockge-expand-all-btn');
    const collapseAllBtn = document.getElementById('dockge-collapse-all-btn');
    if (expandAllBtn)   expandAllBtn.hidden   = !hasStacks;
    if (collapseAllBtn) collapseAllBtn.hidden = !hasStacks;
    renderDockgeStacks();
  } catch (e) {
    err.textContent = `Failed to load Dockge stacks: ${e.message}`;
    err.hidden = false;
  }
}

async function checkDockgeProbeStatus() {
  const btn    = document.getElementById('dockge-probe-btn');
  const status = document.getElementById('dockge-probe-status');
  try {
    const r = await apiFetch('/api/v1/dockge-stacks/probe/status');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const d = await r.json();
    btn.disabled = !d.configured;
    if (!d.configured) {
      btn.title = d.reason;
      status.textContent = `⚠️ Probe unavailable: ${d.reason}`;
      status.style.color = 'var(--text-dim)';
      status.hidden = false;
    }
  } catch (e) {
    btn.disabled = true;
  }
}

function toggleDockgeServices(safeid) {
  const detail = document.getElementById(`dockge-svc-${safeid}`);
  const btn    = document.getElementById(`dockge-svc-btn-${safeid}`);
  if (!detail) return;
  const open = detail.style.display !== 'none';
  detail.style.display = open ? 'none' : 'table-row';
  if (btn) btn.textContent = btn.textContent.replace(open ? '▼' : '▶', open ? '▶' : '▼');
}

function setAllDockgeServices(open) {
  document.querySelectorAll('[id^="dockge-svc-"]').forEach(detail => {
    const safeid = detail.id.replace('dockge-svc-', '');
    const btn = document.getElementById(`dockge-svc-btn-${safeid}`);
    detail.style.display = open ? 'table-row' : 'none';
    if (btn) btn.textContent = btn.textContent.replace(open ? '▶' : '▼', open ? '▼' : '▶');
  });
}

function _parentBadge(ctx, stackName) {
  if (!ctx || ctx === 'unknown') return '<span style="color:var(--text-dim)">—</span>';
  const colours = {
    'dockge-stack':    '#92400e',
    'docker-compose':  '#1e3a5f',
    'docker-run':      '#374151',
    'portainer-stack': '#064e3b',
    'native':          '#3b0764',
  };
  const col   = colours[ctx] || '#374151';
  const label = stackName ? `${ctx}: ${stackName}` : ctx;
  return `<span class="tag" style="background:${col};color:#fff">${esc(label)}</span>`;
}

function _vmTypeBadge(vmType) {
  if (!vmType) return '';
  const col = vmType === 'lxc' ? '#1e3a8a' : vmType === 'qemu' ? '#4c1d95' : '#374151';
  return `<span class="tag" style="background:${col};color:#fff">${esc(vmType)}</span>`;
}

function renderDockgeStacks() {
  const q = (document.getElementById('dockge-search').value || '').toLowerCase();
  const hideObs = document.getElementById('dockge-hide-obsolete').checked;
  const rows = _dockgeStacks.filter(d =>
    (!hideObs || !d.obsolete) &&
    (
      (d.source_vmid      || '').toString().includes(q) ||
      (d.source_lxc_name  || '').toLowerCase().includes(q) ||
      (d.stack_name       || '').toLowerCase().includes(q) ||
      (d.status           || '').toLowerCase().includes(q) ||
      (d.parent_context   || '').toLowerCase().includes(q) ||
      (d.ip_address       || '').toLowerCase().includes(q) ||
      (d.notes            || '').toLowerCase().includes(q)
    )
  );
  const tbody = document.getElementById('dockge-tbody');
  if (!rows.length) {
    tbody.innerHTML = `<tr class="empty-row"><td colspan="13">No Dockge stacks found.</td></tr>`;
    return;
  }
  tbody.innerHTML = rows.map(d => {
    const probed      = (d.last_probed || '—').replace('T',' ').slice(0,19);
    const safeid      = (d.stack_id || '').replace(/[^a-zA-Z0-9_-]/g,'_');
    const svcs        = _dockgeServicesMap[d.stack_id] || [];
    const svcCount    = svcs.length;
    const envBadge    = d.env_file_exists ? '<span style="color:var(--ok)">✓</span>' : '<span style="color:var(--text-dim)">✗</span>';
    const typeBadge   = _vmTypeBadge(d.vm_type);
    const parentBadge = _parentBadge(d.parent_context, d.parent_stack_name);
    const statusCol   = d.status === 'running' ? 'var(--ok)'
                      : d.status === 'stopped'  ? '#f87171'
                      : d.status === 'partial'  ? '#fbbf24'
                      : 'var(--text-dim)';
    const obsBadge    = d.obsolete
      ? `<button class="secondary" title="Mark as active" style="padding:1px 5px;font-size:11px;color:#f87171" onclick="toggleDockgeObsolete('${esc(d.stack_id)}')">obs</button>`
      : `<button class="secondary" title="Mark as obsolete" style="padding:1px 5px;font-size:11px;color:var(--text-dim)" onclick="toggleDockgeObsolete('${esc(d.stack_id)}')">—</button>`;
    const notesCell   = `<span style="font-size:11px;color:var(--text-dim);cursor:pointer" title="Double-click to edit" ondblclick="editDockgeNote('${esc(d.stack_id)}', this)">${esc(d.notes||'')}</span>`;

    // Toggle button (shows service count)
    const toggleCell = svcCount > 0
      ? `<button class="secondary" id="dockge-svc-btn-${safeid}" style="padding:1px 5px;font-size:11px" onclick="toggleDockgeServices('${safeid}')">&#9658; ${svcCount}</button>`
      : `<span style="color:var(--text-dim)">${svcCount}</span>`;

    // Services expandable sub-row
    const svcSubRow = svcCount > 0 ? `
      <tr id="dockge-svc-${safeid}" style="display:none">
        <td colspan="13" style="padding:0 0 4px 28px;background:var(--bg-el)">
          <table style="width:100%;font-size:12px;border-collapse:collapse">
            <thead><tr style="color:var(--text-dim);border-bottom:1px solid var(--border)">
              <th style="padding:3px 8px;text-align:left">Service</th>
              <th style="padding:3px 8px;text-align:left">Image</th>
              <th style="padding:3px 8px;text-align:left">Ports</th>
              <th style="padding:3px 8px;text-align:left">State</th>
              <th style="padding:3px 8px;text-align:left">Container ID</th>
            </tr></thead>
            <tbody>
              ${svcs.map(sv => {
                let ports = '—';
                try { const pp = JSON.parse(sv.ports_json||'[]'); ports = Array.isArray(pp) && pp.length ? pp.join(', ') : '—'; } catch(_){}
                const stateCol = sv.container_state === 'running' ? 'var(--ok)'
                               : sv.container_state === 'exited'  ? '#f87171'
                               : 'var(--text-dim)';
                return `<tr>
                  <td style="padding:2px 8px"><strong>${esc(sv.service_name||'')}</strong></td>
                  <td style="padding:2px 8px;color:var(--text-dim);font-size:11px">${esc(sv.image||'—')}</td>
                  <td style="padding:2px 8px">${esc(ports)}</td>
                  <td style="padding:2px 8px;color:${stateCol}">${esc(sv.container_state||'—')}</td>
                  <td style="padding:2px 8px;font-size:11px;color:var(--text-dim)">${esc(sv.container_id||'—')}</td>
                </tr>`;
              }).join('')}
            </tbody>
          </table>
        </td>
      </tr>` : '';

    return `<tr>
      <td>${toggleCell}</td>
      <td><code>${esc(d.pve_host||'')}</code></td>
      <td>${esc(d.source_vmid||'')}</td>
      <td>${typeBadge}</td>
      <td>${esc(d.source_lxc_name||'—')}</td>
      <td><strong>${esc(d.stack_name||'')}</strong></td>
      <td style="color:${statusCol}">${esc(d.status||'—')}</td>
      <td style="text-align:center">${envBadge}</td>
      <td style="font-size:11px;color:var(--text-dim)">${esc(d.vm_type==='lxc'?'LXC':d.vm_type==='qemu'?'VM':'—')}</td>
      <td>${parentBadge}</td>
      <td style="text-align:center">${obsBadge}</td>
      <td style="max-width:180px">${notesCell}</td>
      <td style="white-space:nowrap;color:var(--text-dim)">${esc(probed)}</td>
    </tr>${svcSubRow}`;
  }).join('');
}

async function toggleDockgeObsolete(stackId) {
  const row = _dockgeStacks.find(d => d.stack_id === stackId);
  if (!row) return;
  const newVal = row.obsolete ? 0 : 1;
  try {
    const r = await apiFetch(`/api/v1/dockge-stacks/${encodeURIComponent(stackId)}`, {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({obsolete: newVal}),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    row.obsolete = newVal;
    renderDockgeStacks();
  } catch (e) {
    alert(`Failed to update obsolete flag: ${e.message}`);
  }
}

async function editDockgeNote(stackId, el) {
  const row = _dockgeStacks.find(d => d.stack_id === stackId);
  if (!row) return;
  const newNote = prompt('Edit note for "' + stackId + '":', row.notes || '');
  if (newNote === null) return; // cancelled
  try {
    const r = await apiFetch(`/api/v1/dockge-stacks/${encodeURIComponent(stackId)}`, {
      method: 'PUT',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({notes: newNote}),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    row.notes = newNote;
    renderDockgeStacks();
  } catch (e) {
    alert(`Failed to save note: ${e.message}`);
  }
}

async function probeDockgeStacks() {
  const btn    = document.getElementById('dockge-probe-btn');
  const status = document.getElementById('dockge-probe-status');
  btn.disabled = true;
  btn.textContent = '⟳ Probing…';
  status.hidden = true;
  try {
    const r = await apiFetch('/api/v1/dockge-stacks/probe', { method: 'POST' });
    const d = await r.json();
    if (!r.ok) throw new Error(d.detail || `HTTP ${r.status}`);
    status.textContent = `✓ Done — stacks: ${d.stacks_total??0} (new: ${d.stacks_created??0}), services: ${d.services_total??0}, machines: ${d.machines_probed??0}`;
    status.style.color = 'var(--accent)';
    status.hidden = false;
    _dockgeStacks = [];
    await loadDockgeStacks();
  } catch (e) {
    status.textContent = `✗ Probe failed: ${e.message}`;
    status.style.color = '#f87171';
    status.hidden = false;
  } finally {
    btn.disabled = false;
    btn.textContent = '▶ Probe Dockge';
  }
}
