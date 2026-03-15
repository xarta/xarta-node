/* ── SSH Targets ───────────────────────────────────────────────────────── */
async function loadSshTargets() {
  const err = document.getElementById('ssh-targets-error');
  if (err) err.hidden = true;
  try {
    const r = await apiFetch('/api/v1/ssh-targets');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    _sshTargets = await r.json();
    renderSshTargets();
  } catch (e) {
    if (err) { err.textContent = `Failed to load SSH Targets: ${e.message}`; err.hidden = false; }
  }
}

function renderSshTargets() {
  const tbody = document.getElementById('ssh-targets-tbody');
  if (!tbody) return;
  if (!_sshTargets.length) {
    tbody.innerHTML = '<tr><td colspan="8" style="color:var(--text-dim);text-align:center">No entries — click Rebuild to populate from config</td></tr>';
    return;
  }
  const TYPE_COLOURS = {
    'lxc-fleet': '#1d4ed8', 'lxc': '#0f766e', 'qemu': '#7c3aed',
    'citadel': '#b91c1c', 'pve': '#92400e', 'pfsense': '#15803d'
  };
  tbody.innerHTML = _sshTargets.map(e => {
    const colour = TYPE_COLOURS[e.host_type] || '#555';
    return `<tr>
      <td><code>${esc(e.ip_address)}</code></td>
      <td>${esc(e.host_name || '\u2014')}</td>
      <td><span class="tag" style="background:${colour};color:#fff">${esc(e.host_type || '?')}</span></td>
      <td><code style="font-size:11px">${esc(e.key_env_var)}</code></td>
      <td><code>${esc(e.source_ip || '\u2014')}</code></td>
      <td style="font-size:11px;color:var(--text-dim)">${esc(e.notes || '\u2014')}</td>
      <td style="font-size:11px">${esc((e.updated_at||'').slice(0,16).replace('T',' '))}</td>
      <td><button class="secondary" onclick="deleteSshTarget('${esc(e.ip_address)}')" style="padding:2px 7px;font-size:11px">✕</button></td>
    </tr>`;
  }).join('');
}

async function rebuildSshTargets() {
  const btn = document.getElementById('ssh-targets-rebuild-btn');
  const status = document.getElementById('ssh-targets-status');
  btn.disabled = true; btn.textContent = '\u23F3 Rebuilding\u2026';
  try {
    const r = await apiFetch('/api/v1/ssh-targets/rebuild', { method: 'POST' });
    const d = await r.json();
    if (!r.ok) throw new Error(d.detail || `HTTP ${r.status}`);
    status.textContent = `\u2713 ${d.message}`;
    _sshTargets = [];
    await loadSshTargets();
  } catch (e) {
    status.textContent = `\u2717 Rebuild failed: ${e.message}`;
  } finally {
    btn.disabled = false; btn.textContent = '\u21BA Rebuild from config';
  }
}

async function deleteSshTarget(ip) {
  if (!confirm(`Remove ssh_target for ${ip}?`)) return;
  try {
    const r = await apiFetch(`/api/v1/ssh-targets/${encodeURIComponent(ip)}`, { method: 'DELETE' });
    if (!r.ok) throw new Error((await r.json()).detail || `HTTP ${r.status}`);
    _sshTargets = _sshTargets.filter(e => e.ip_address !== ip);
    renderSshTargets();
  } catch (e) { alert(`Failed to delete: ${e.message}`); }
}
