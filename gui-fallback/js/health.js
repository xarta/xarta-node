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
    const integBadge = ok
      ? `<button class="badge badge-ok badge-btn">OK</button>`
      : `<button class="badge badge-err badge-btn">FAILED</button>`;
    document.getElementById('nn-integrity').innerHTML = integBadge;
    // Wire integrity badge click via addEventListener (avoids inline onclick)
    const integBtn = document.querySelector('#nn-integrity .badge-btn');
    if (integBtn) integBtn.addEventListener('click', () => openIntegrityModal(ok));
    // Mirror to compact meta
    const integCompact = document.getElementById('nn-integrity-compact');
    if (integCompact) {
      integCompact.innerHTML = integBadge;
      const integCompactBtn = integCompact.querySelector('.badge-btn');
      if (integCompactBtn) integCompactBtn.addEventListener('click', () => openIntegrityModal(ok));
    }
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
  // Mirror to compact meta keys slot
  const keysCompact = document.getElementById('nn-keys-compact');
  if (keysCompact) {
    keysCompact.innerHTML = badge.outerHTML;
    const compactBtn = keysCompact.querySelector('.badge-btn');
    if (compactBtn) {
      compactBtn.style.display = '';
      compactBtn.addEventListener('click', () => {
        switchGroup('settings');
        switchTab('keys');
        SettingsMenuConfig.updateActiveTab('keys');
      });
    }
  }
}

async function openIntegrityModal(isOk = false) {
  const modal = document.getElementById('integrity-modal');
  const diag  = document.getElementById('integrity-diag');

  // Adapt modal to OK vs FAILED state
  modal.style.borderColor = isOk ? 'var(--ok,#3fb950)' : 'var(--err,#f85149)';
  document.getElementById('integrity-modal-title').innerHTML =
    isOk ? '&#10003; Integrity OK' : '&#9888; Integrity FAILED';
  document.getElementById('integrity-modal-title').style.color =
    isOk ? 'var(--ok,#3fb950)' : 'var(--err,#f85149)';
  document.getElementById('integrity-modal-intro').innerHTML = isOk
    ? 'This node\'s database is <strong>healthy</strong>. Sync is active and peer writes are accepted. The readings below confirm the current state.'
    : 'This node\'s database has been marked as <strong>degraded</strong>. Sync is paused \u2014 no outgoing writes will be sent to peers, and this node will not accept incoming sync actions until recovery.';
  document.getElementById('integrity-modal-cause').style.display = isOk ? 'none' : '';
  document.getElementById('integrity-modal-recovery').style.display = isOk ? 'none' : '';

  diag.innerHTML = '<span class="spinner"></span> Loading&hellip;';
  HubModal.open(modal);
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

function _setHostParentEl(parent) {
  const el = document.getElementById('header-host');
  if (!el) return;
  if (!parent || parent === 'Unknown') {
    el.innerHTML = `&#9670; ${parent || 'Unknown'}`;
    _setHostCompact(parent || 'Unknown', null);
    return;
  }
  // Build Proxmox URL: replace the first hostname segment with the parent name,
  // keeping the same domain as the current page.
  const parts  = window.location.hostname.split('.');
  const domain = parts.length > 1 ? parts.slice(1).join('.') : window.location.hostname;
  const pveUrl = `https://${parent}.${domain}:8006`;
  el.innerHTML = `&#9670; <a href="${pveUrl}" target="_blank" rel="noopener"
    style="color:inherit;text-decoration:none;border-bottom:1px dotted currentColor;cursor:pointer"
    title="Open Proxmox: ${pveUrl}">${parent}</a>`;
  _setHostCompact(parent, pveUrl);
}

function _setHostCompact(parent, pveUrl) {
  const el = document.getElementById('nn-host-compact');
  if (!el) return;
  if (!pveUrl) {
    el.innerHTML = `&#9670; ${parent}`;
    return;
  }
  el.innerHTML = `&#9670; <a href="${pveUrl}" target="_blank" rel="noopener"
    style="color:inherit;text-decoration:none;border-bottom:1px dotted currentColor;cursor:pointer"
    title="Open Proxmox: ${pveUrl}">${parent}</a>`;
}

async function lookupHostParent(nodeName) {
  const cachedParent = localStorage.getItem(_LS_DIAG_HOST);
  const cachedTs     = parseInt(localStorage.getItem(_LS_DIAG_HOST_TS) || '0', 10);
  const isStale      = (Date.now() - cachedTs) > _HOST_TTL_MS;

  if (cachedParent && !isStale) {
    _setHostParentEl(cachedParent);
    return;
  }

  // Cache absent or older than 1 h — fetch fresh
  try {
    const r = await apiFetch('/api/v1/machines');
    if (r.ok) _machines = await r.json();
    const name   = (nodeName || '').toLowerCase();
    const m      = _machines.find(m => (m.name || '').toLowerCase() === name);
    const parent = (m && m.parent_machine_id) ? m.parent_machine_id : 'Unknown';
    _setHostParentEl(parent);
    if (parent !== 'Unknown') {
      try {
        localStorage.setItem(_LS_DIAG_HOST, parent);
        localStorage.setItem(_LS_DIAG_HOST_TS, String(Date.now()));
      } catch (_) {}
    }
  } catch (_) {
    // On failure fall back to whatever is cached (even if stale), or Unknown
    _setHostParentEl(cachedParent || 'Unknown');
  }
}
