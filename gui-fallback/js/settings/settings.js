/* ── Settings ─────────────────────────────────────────────────────────── */
async function loadSettings() {
  await loadSettingsCidr();
  const err = document.getElementById('settings-error');
  err.hidden = true;
  try {
    const r = await apiFetch('/api/v1/settings');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    _settings = await r.json();
    renderSettings();
  } catch (e) {
    err.textContent = `Failed to load settings: ${e.message}`;
    err.hidden = false;
  }
}

function renderSettings() {
  const tbody = document.getElementById('settings-tbody');
  if (!_settings.length) {
    tbody.innerHTML = '<tr class="empty-row"><td colspan="5">No settings yet — add one above.</td></tr>';
    return;
  }
  tbody.innerHTML = _settings.map(s => {
    const updated = (s.updated_at || '—').replace('T', ' ').slice(0, 19);
    const keyEsc  = esc(s.key);
    const valEsc  = esc(s.value || '');
    const descEsc = esc(s.description || '');
    return `<tr>
      <td><code style="font-size:12px">${keyEsc}</code></td>
      <td>${valEsc}</td>
      <td style="color:var(--text-dim)">${descEsc}</td>
      <td style="white-space:nowrap;color:var(--text-dim)">${esc(updated)}</td>
      <td style="white-space:nowrap">
        <button class="secondary" style="font-size:12px;padding:3px 10px"
          onclick="editSetting('${keyEsc}','${valEsc}','${descEsc}')">Edit</button>
        <button style="font-size:12px;padding:3px 10px;background:var(--err);border-color:var(--err);color:#fff"
          onclick="deleteSetting('${keyEsc}')">&#10005;</button>
      </td>
    </tr>`;
  }).join('');
}

function openAddSettingModal() {
  ['setting-key','setting-val','setting-desc'].forEach(id => {
    document.getElementById(id).value = '';
  });
  document.getElementById('setting-modal-title').textContent = 'Add setting';
  document.getElementById('setting-key').readOnly = false;
  document.getElementById('setting-error').hidden = true;
  document.getElementById('setting-modal').showModal();
}

function editSetting(key, value, description) {
  document.getElementById('setting-key').value  = key;
  document.getElementById('setting-val').value  = value;
  document.getElementById('setting-desc').value = description;
  document.getElementById('setting-modal-title').textContent = 'Edit setting';
  document.getElementById('setting-key').readOnly = true;
  document.getElementById('setting-error').hidden = true;
  document.getElementById('setting-modal').showModal();
}

async function submitSetting() {
  const key  = document.getElementById('setting-key').value.trim();
  const val  = document.getElementById('setting-val').value.trim();
  const desc = document.getElementById('setting-desc').value.trim();
  const err  = document.getElementById('setting-error');
  if (!key) { err.textContent = 'Key is required'; err.hidden = false; return; }
  try {
    const r = await apiFetch(`/api/v1/settings/${encodeURIComponent(key)}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ value: val, description: desc || null }),
    });
    if (!r.ok) { const t = await r.text(); throw new Error(t); }
    document.getElementById('setting-modal').close();
    _settings = [];
    await loadSettings();
  } catch (e) {
    err.textContent = `Error: ${e.message}`;
    err.hidden = false;
  }
}

async function deleteSetting(key) {
  if (!confirm(`Delete setting "${key}"?`)) return;
  try {
    const r = await apiFetch(`/api/v1/settings/${encodeURIComponent(key)}`, { method: 'DELETE' });
    if (!r.ok && r.status !== 204) throw new Error(`HTTP ${r.status}`);
    _settings = [];
    await loadSettings();
  } catch (e) {
    alert(`Delete failed: ${e.message}`);
  }
}

async function loadSettingsCidr() {
  try {
    const r = await apiFetch('/api/v1/settings/mgmt_cidr');
    if (r.ok) {
      const d = await r.json();
      document.getElementById('settings-cidr').value = d.value || '';
    }
  } catch (_) {}
}

async function saveCidr() {
  const input  = document.getElementById('settings-cidr');
  const status = document.getElementById('settings-status');
  const cidr   = input.value.trim();
  if (!cidr) { status.textContent = '⚠ Enter a CIDR first'; status.hidden = false; return; }
  try {
    const r = await apiFetch('/api/v1/settings/mgmt_cidr', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ value: cidr, description: 'Management network CIDR for PVE host scanning' }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    status.textContent = '✓ Saved';
    status.style.color = 'var(--accent)';
    status.hidden = false;
    setTimeout(() => { status.hidden = true; }, 3000);
  } catch (e) {
    status.textContent = `✗ ${e.message}`;
    status.style.color = '#f87171';
    status.hidden = false;
  }
}

/* ── Sound enabled toggle ─────────────────────────────────────────────── */
function initSoundToggle() {
  const checkbox = document.getElementById('sound-enabled-toggle');
  if (!checkbox) return;
  const current = getFrontendSetting('sound_enabled', 'false') === 'true';
  checkbox.checked = current;
}

async function saveSoundEnabled(enabled) {
  const statusEl = document.getElementById('sound-enabled-status');
  try {
    const r = await apiFetch('/api/v1/settings/frontend-settings/sound_enabled', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ value: enabled ? 'true' : 'false', description: 'Enable sound effects for nav item clicks' }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    // Update localStorage cache immediately
    await loadFrontendSettings();
    // Apply to the live SoundManager
    if (typeof SoundManager !== 'undefined') SoundManager.setEnabled(enabled);
    if (statusEl) { statusEl.textContent = `✓ Sound ${enabled ? 'on' : 'off'}`; }
    setTimeout(() => { if (statusEl) statusEl.textContent = ''; }, 2500);
  } catch (e) {
    if (statusEl) { statusEl.textContent = `✗ ${e.message}`; }
    // Revert the checkbox
    const checkbox = document.getElementById('sound-enabled-toggle');
    if (checkbox) checkbox.checked = !enabled;
  }
}
