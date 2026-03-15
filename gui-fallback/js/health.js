/* ── Health / header stats ────────────────────────────────────────────── */
async function loadHealth() {
  try {
    const r = await apiFetch('/health');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const d = await r.json();
    _nodeName   = d.node_name || d.node_id || '';
    _selfNodeId  = d.node_id || _nodeName;
    try { localStorage.setItem(_LS_DIAG_NODE, _selfNodeId); } catch (_) {}
    // Close the diagnostic modal if health is now OK
    const dm = document.getElementById('diag-modal');
    if (dm && dm.open) dm.close();
    document.getElementById('nn-gen').textContent = d.gen ?? '—';
    const ok = d.integrity_ok;
    document.getElementById('nn-integrity').innerHTML = ok
      ? `<span class="badge badge-ok">OK</span>`
      : `<button class="badge badge-err badge-btn" onclick="openIntegrityModal()">FAILED</button>`;
    lookupHostParent(d.node_id || _nodeName);
  } catch (e) {
    console.warn('health check failed:', e);
    // Network error (backend offline) → run connectivity diagnostic
    if (e instanceof TypeError) { showConnectivityDiagnostic(); }
  }
  // Key badge — best-effort, does not block main health display
  try {
    const kr = await apiFetch('/api/v1/keys/status');
    if (kr.ok) {
      const kd = await kr.json();
      updateKeyBadge(kd.keys);
    }
  } catch (_) {}
}

function updateKeyBadge(keys) {
  const total   = keys.length;
  const present = keys.filter(k => k.present).length;
  const badge   = document.getElementById('keys-badge');
  if (!badge) return;
  badge.style.display = '';
  badge.textContent = `\u2A3F ${present}/${total}`;
  if (present === total) {
    badge.className = 'badge badge-ok badge-btn';
  } else if (present === 0) {
    badge.className = 'badge badge-err badge-btn';
  } else {
    badge.className = 'badge badge-btn';
    badge.style.background = '#3d2e10';
    badge.style.color = 'var(--warn)';
  }
}

async function openIntegrityModal() {
  const modal = document.getElementById('integrity-modal');
  const diag  = document.getElementById('integrity-diag');
  diag.innerHTML = '<span class="spinner"></span> Loading&hellip;';
  modal.showModal();
  try {
    const [hr, sr] = await Promise.all([
      apiFetch('/health'),
      apiFetch('/api/v1/sync/status'),
    ]);
    const h = hr.ok ? await hr.json() : null;
    const s = sr.ok ? await sr.json() : null;
    const rows = [];
    if (h) {
      rows.push(['Node', `${h.node_name || h.node_id}`]);
      rows.push(['Gen', h.gen]);
      rows.push(['Commit', h.commit ? `${h.commit}` : '—']);
    }
    if (s) {
      rows.push(['Last write at', s.last_write_at || '—']);
      rows.push(['Last write by', s.last_write_by || '—']);
      rows.push(['Peers known', s.peer_count]);
      const depths = s.queue_depths || {};
      const depthStr = Object.keys(depths).length
        ? Object.entries(depths).map(([k,v]) => `${k}: ${v}`).join(', ')
        : 'none';
      rows.push(['Pending queue', depthStr]);
    }
    diag.innerHTML = rows.map(([k,v]) =>
      `<div style="display:flex;gap:12px;border-bottom:1px solid var(--border);padding:4px 0;">
        <span style="min-width:130px;color:var(--text-dim);font-size:12px;text-transform:uppercase;
              letter-spacing:.4px;">${k}</span>
        <span style="font-family:monospace;font-size:13px;color:var(--text);">${v}</span>
      </div>`
    ).join('');
  } catch (e) {
    diag.textContent = `Could not load diagnostics: ${e.message}`;
  }
}

async function lookupHostParent(nodeName) {
  const cachedParent = localStorage.getItem(_LS_DIAG_HOST);
  const cachedTs     = parseInt(localStorage.getItem(_LS_DIAG_HOST_TS) || '0', 10);
  const isStale      = (Date.now() - cachedTs) > _HOST_TTL_MS;

  if (cachedParent && !isStale) {
    // Fresh cache — update the header display without any API call
    document.getElementById('header-host').textContent = `\u25C6 ${cachedParent}`;
    return;
  }

  // Cache absent or older than 1 h — fetch fresh
  try {
    const r = await apiFetch('/api/v1/machines');
    if (r.ok) _machines = await r.json();
    const name   = (nodeName || '').toLowerCase();
    const m      = _machines.find(m => (m.name || '').toLowerCase() === name);
    const parent = (m && m.parent_machine_id) ? m.parent_machine_id : 'Unknown';
    document.getElementById('header-host').textContent = `\u25C6 ${parent}`;
    if (parent !== 'Unknown') {
      try {
        localStorage.setItem(_LS_DIAG_HOST, parent);
        localStorage.setItem(_LS_DIAG_HOST_TS, String(Date.now()));
      } catch (_) {}
    }
  } catch (_) {
    // On failure fall back to whatever is cached (even if stale), or Unknown
    const fallback = cachedParent || 'Unknown';
    document.getElementById('header-host').textContent = `\u25C6 ${fallback}`;
  }
}
