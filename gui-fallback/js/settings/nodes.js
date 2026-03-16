/* ── Nodes ────────────────────────────────────────────────────────────── */
async function retouchTable(btn) {
  const sel = document.getElementById('retouch-table-select');
  const status = document.getElementById('retouch-status');
  const table = sel ? sel.value : '';
  if (!table) { if (status) { status.textContent = 'Select a table first'; status.style.color = 'var(--warn)'; } return; }
  if (btn) btn.disabled = true;
  if (status) { status.textContent = 'Retouching…'; status.style.color = 'var(--text-dim)'; }
  try {
    const r = await apiFetch(`/api/v1/sync/retouch/${encodeURIComponent(table)}`, { method: 'POST' });
    if (!r.ok) throw new Error((await r.json()).detail || `HTTP ${r.status}`);
    const data = await r.json();
    if (status) { status.textContent = `✓ ${data.requeued} rows re-queued from ${data.table}`; status.style.color = 'var(--ok)'; }
  } catch (e) {
    if (status) { status.textContent = `✗ ${e.message}`; status.style.color = 'var(--err)'; }
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function loadNodes() {
  const err = document.getElementById('nodes-error');
  err.hidden = true;
  try {
    const r = await apiFetch('/api/v1/nodes');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    _nodes = await r.json();
    _saveDiagNodes(_nodes);  // cache peer list for offline diagnostics
    renderNodes();
    enrichNodeVersions();
    enrichNodePctStatus();
  } catch (e) {
    err.textContent = `Failed to load nodes: ${e.message}`;
    err.hidden = false;
  }
}

function renderNodes() {
  const tbody = document.getElementById('nodes-tbody');
  if (!_nodes.length) {
    tbody.innerHTML = '<tr class="empty-row"><td colspan="7">No nodes registered.</td></tr>';
    return;
  }
  tbody.innerHTML = _nodes.flatMap(n => {
    const nameStyle = n.fleet_peer === false ? ' style="text-decoration:line-through;opacity:0.55"' : '';
    const pending = n.pending_count || 0;
    const pendingBadge = pending > 0
      ? `<span style="background:var(--badge-warn,#b45309);color:#fff;border-radius:4px;padding:1px 6px;font-size:11px">${pending}</span>`
      : `<span style="color:var(--text-dim)">—</span>`;
    const safeid = n.node_id.replace(/[^a-zA-Z0-9_-]/g, '_');
    const addrs = (n.addresses || []).map(a => `<span class="ip-chip">${esc(a)}</span>`).join('<br>');
    const ph = n.primary_hostname || '';
    const th = n.tailnet_hostname || '';
    const hostnamesHtml = ph
      ? `<span class="ip-chip">${esc(ph)}</span>${th ? `<br><span class="ip-chip" style="opacity:0.75">${esc(th)}</span>` : ''}`
      : '<span style="color:var(--text-dim)">—</span>';
    const mainRow = `<tr>
      <td style="white-space:nowrap"><strong title="${esc(n.node_id)}"${nameStyle}>${esc(n.display_name || n.node_id)}</strong></td>
      <td>${addrs || '<span style="color:var(--text-dim)">—</span>'}</td>
      <td>${hostnamesHtml}</td>
      <td id="node-gen-${safeid}" style="font-size:12px;color:var(--text-dim)">…</td>
      <td id="node-ver-${safeid}" style="font-size:12px;color:var(--text-dim)">…</td>
      <td>${pendingBadge}</td>
      <td style="width:200px;max-width:200px"><div style="display:flex;flex-wrap:wrap;gap:3px">
        <button class="secondary" style="font-size:11px;padding:2px 7px" onclick="nodeRestart('${esc(n.node_id)}',this)" title="restart blueprints-app service">&#8635; Restart</button>
        <button class="secondary" style="font-size:11px;padding:2px 7px" onclick="nodeGitPull('${esc(n.node_id)}',this)" title="git pull outer on this node">&#8593; Pull</button>
        <button class="secondary" style="font-size:11px;padding:2px 7px" onclick="nodePurgeQueue('${esc(n.node_id)}',this)" title="purge unsent sync queue entries">&#128465; Queue</button>
        <button id="node-pct-${safeid}" class="secondary" style="font-size:11px;padding:2px 7px" onclick="nodePct('${esc(n.node_id)}',this)" title="start or stop LXC via pct on PVE host" data-pct-status="unknown">&#x23FB; Stop</button>
        <button class="secondary" style="font-size:11px;padding:2px 7px;color:var(--danger,#f85149)" onclick="nodeDeleteRow('${esc(n.node_id)}',this)" title="remove this node from DB">&#10005; Delete</button>
      </div></td>
    </tr>`;
    return mainRow;
  }).join('');
}



async function enrichNodeVersions() {
  for (const n of _nodes) {
    const safeid = n.node_id.replace(/[^a-zA-Z0-9_-]/g, '_');
    const genCell = document.getElementById(`node-gen-${safeid}`);
    const verCell = document.getElementById(`node-ver-${safeid}`);
    // Self: use a relative URL (same origin, through Caddy).
    // Peers: use ui_url (Caddy HTTPS) — port 8080 is firewalled from the browser.
    // Fall back to addresses[0] only if ui_url absent (pre-firewall nodes).
    const isSelf = n.node_id === _selfNodeId;
    const healthUrl = isSelf
      ? '/health'
      : n.ui_url
        ? `${n.ui_url.replace(/\/$/, '')}/health`
        : (n.addresses && n.addresses[0] ? `${n.addresses[0].replace(/\/$/, '')}/health` : null);
    if (!healthUrl) {
      if (genCell) genCell.textContent = '—';
      if (verCell) verCell.textContent = '—';
      continue;
    }
    try {
      const r = await apiFetch(healthUrl, { signal: AbortSignal.timeout(4000) });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const d = await r.json();
      if (genCell) { genCell.textContent = d.gen ?? '—'; genCell.style.color = ''; }
      if (verCell) { verCell.textContent = d.commit || '—'; verCell.style.color = ''; verCell.title = ''; }
    } catch {
      if (genCell) { genCell.textContent = '?'; genCell.style.color = 'var(--text-dim)'; }
      if (verCell) { verCell.textContent = '?'; verCell.style.color = 'var(--text-dim)'; }
    }
  }
}

async function fleetUpdate(btn) {
  if (!confirm('Trigger git pull (public + private repos) on this node and queue for all fleet peers?\n\nAll nodes will pull latest code and restart if there are new commits.')) return;
  const statusEl = document.getElementById('fleet-update-status');
  btn.disabled = true;
  const orig = btn.innerHTML;
  btn.innerHTML = '&#8987; Updating…';
  statusEl.textContent = '';
  statusEl.style.color = '';
  try {
    const r = await apiFetch('/api/v1/sync/git-pull', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ scope: 'both' }),
    });
    if (r.ok) {
      statusEl.textContent = '✓ Queued for all nodes — peers will pull and restart shortly';
      statusEl.style.color = 'var(--ok,#3fb950)';
      setTimeout(() => { loadNodes(); }, 4000);
    } else {
      statusEl.textContent = `✗ HTTP ${r.status}`;
      statusEl.style.color = 'var(--danger,#f85149)';
    }
  } catch (e) {
    statusEl.textContent = `✗ ${e.message}`;
    statusEl.style.color = 'var(--danger,#f85149)';
  }
  btn.disabled = false;
  btn.innerHTML = orig;
  setTimeout(() => { statusEl.textContent = ''; statusEl.style.color = ''; }, 10000);
}

async function nodeRestart(nodeId, btn) {
  if (!confirm(`Restart blueprints-app on ${nodeId}?`)) return;
  const orig = btn.innerHTML;
  btn.disabled = true; btn.textContent = '…';
  try {
    const r = await apiFetch(`/api/v1/nodes/${encodeURIComponent(nodeId)}/restart`, { method: 'POST' });
    btn.innerHTML = r.ok ? '&#10003; Sent' : `&#10007; ${r.status}`;
    btn.style.color = r.ok ? 'var(--ok,#3fb950)' : 'var(--danger,#f85149)';
  } catch {
    btn.innerHTML = '&#10007; err'; btn.style.color = 'var(--danger,#f85149)';
  }
  setTimeout(() => { btn.disabled = false; btn.innerHTML = orig; btn.style.color = ''; }, 3000);
}

async function nodeGitPull(nodeId, btn) {
  if (!confirm(`Trigger git pull (outer) on ${nodeId}?`)) return;
  const orig = btn.innerHTML;
  btn.disabled = true; btn.textContent = '…';
  try {
    const r = await apiFetch(`/api/v1/nodes/${encodeURIComponent(nodeId)}/git-pull`, { method: 'POST' });
    btn.innerHTML = r.ok ? '&#10003; Pull' : `&#10007; ${r.status}`;
    btn.style.color = r.ok ? 'var(--ok,#3fb950)' : 'var(--danger,#f85149)';
  } catch {
    btn.innerHTML = '&#10007; err'; btn.style.color = 'var(--danger,#f85149)';
  }
  setTimeout(() => { btn.disabled = false; btn.innerHTML = orig; btn.style.color = ''; }, 3000);
}

async function nodePurgeQueue(nodeId, btn) {
  const n = _nodes.find(x => x.node_id === nodeId);
  const cnt = n ? (n.pending_count || 0) : '?';
  if (!confirm(`Purge ${cnt} unsent sync queue entries for ${nodeId}?`)) return;
  const orig = btn.innerHTML;
  btn.disabled = true; btn.textContent = '…';
  try {
    const r = await apiFetch(`/api/v1/nodes/${encodeURIComponent(nodeId)}/sync-queue`, { method: 'DELETE' });
    btn.innerHTML = r.ok ? '&#10003; Done' : `&#10007; ${r.status}`;
    btn.style.color = r.ok ? 'var(--ok,#3fb950)' : 'var(--danger,#f85149)';
    if (r.ok) { _nodes = []; setTimeout(loadNodes, 500); }
  } catch {
    btn.innerHTML = '&#10007; err'; btn.style.color = 'var(--danger,#f85149)';
    setTimeout(() => { btn.disabled = false; btn.innerHTML = orig; btn.style.color = ''; }, 3000);
  }
}

async function nodeDeleteRow(nodeId, btn) {
  if (!confirm(`Delete node "${nodeId}" from this node's DB?\n\nThis does not purge the sync queue — use Purge Queue first if needed.`)) return;
  btn.disabled = true; btn.textContent = '…';
  try {
    const r = await apiFetch(`/api/v1/nodes/${encodeURIComponent(nodeId)}`, { method: 'DELETE' });
    if (r.ok) { _nodes = []; loadNodes(); return; }
    btn.textContent = `✗ ${r.status}`; btn.style.color = 'var(--danger,#f85149)';
    btn.disabled = false;
  } catch {
    btn.textContent = '✗ err'; btn.style.color = 'var(--danger,#f85149)';
    btn.disabled = false;
  }
}

async function enrichNodePctStatus() {
  await Promise.all(_nodes.map(async n => {
    const safeid = n.node_id.replace(/[^a-zA-Z0-9_-]/g, '_');
    const btn = document.getElementById(`node-pct-${safeid}`);
    if (!btn || btn.disabled) return;  // skip buttons mid-action
    try {
      const r = await apiFetch(`/api/v1/nodes/${encodeURIComponent(n.node_id)}/pct-status`);
      if (!r.ok) { btn.dataset.pctStatus = 'unknown'; return; }
      const d = await r.json();
      const st = (d.status || 'unknown').toLowerCase();
      btn.dataset.pctStatus = st;
      if (st === 'running') {
        btn.innerHTML = '&#9632; Stop';
        btn.title = `Stop LXC ${d.vmid} on ${d.pve_host}`;
      } else if (st === 'stopped') {
        btn.innerHTML = '&#9654; Start';
        btn.title = `Start LXC ${d.vmid} on ${d.pve_host}`;
      } else {
        btn.innerHTML = '&#x23FB; PCT';
        btn.title = 'start or stop LXC via pct on PVE host (status unknown)';
      }
    } catch {
      btn.dataset.pctStatus = 'unknown';
    }
  }));
}

async function nodePct(nodeId, btn) {
  const currentStatus = btn.dataset.pctStatus || 'unknown';
  let action;
  if (currentStatus === 'running') {
    if (!confirm(`Stop LXC for ${nodeId}?\n\nThis will shut down the container via pct stop.`)) return;
    action = 'stop';
  } else if (currentStatus === 'stopped') {
    if (!confirm(`Start LXC for ${nodeId}?`)) return;
    action = 'start';
  } else {
    const choice = prompt(`PCT action for ${nodeId}:\nEnter "start" or "stop"`);
    if (!choice) return;
    action = choice.trim().toLowerCase();
    if (action !== 'start' && action !== 'stop') { alert('Invalid action. Enter "start" or "stop".'); return; }
  }
  const orig = btn.innerHTML;
  btn.disabled = true; btn.textContent = '…';
  try {
    const r = await apiFetch(`/api/v1/nodes/${encodeURIComponent(nodeId)}/pct`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ action }),
    });
    if (r.ok) {
      btn.innerHTML = '&#10003; Done';
      btn.style.color = 'var(--ok,#3fb950)';
      btn.dataset.pctStatus = action === 'start' ? 'running' : 'stopped';
      setTimeout(() => {
        btn.disabled = false;
        btn.innerHTML = action === 'start' ? '&#9632; Stop' : '&#9654; Start';
        btn.style.color = '';
        btn.dataset.pctStatus = action === 'start' ? 'running' : 'stopped';
      }, 3000);
    } else {
      const err = await r.json().catch(() => ({}));
      btn.innerHTML = `&#10007; ${r.status}`;
      btn.style.color = 'var(--danger,#f85149)';
      btn.title = err.detail || r.statusText;
      setTimeout(() => { btn.disabled = false; btn.innerHTML = orig; btn.style.color = ''; }, 4000);
    }
  } catch {
    btn.innerHTML = '&#10007; err'; btn.style.color = 'var(--danger,#f85149)';
    setTimeout(() => { btn.disabled = false; btn.innerHTML = orig; btn.style.color = ''; }, 3000);
  }
}
