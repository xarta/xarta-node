/* ── Backups ───────────────────────────────────────────────────────────── */
async function loadBackups() {
  const tbody = document.getElementById('backup-tbody');
  const err   = document.getElementById('backup-error');
  err.hidden = true;
  try {
    const r = await apiFetch('/api/v1/backup');
    if (!r.ok) {
      if (r.status === 503) {
        tbody.innerHTML = '<tr class="empty-row"><td colspan="4">Backups not configured on this node.</td></tr>';
        return;
      }
      throw new Error(`HTTP ${r.status}`);
    }
    const d = await r.json();
    const list = d.backups || [];
    if (!list.length) {
      tbody.innerHTML = '<tr class="empty-row"><td colspan="4">No backups yet — create one above.</td></tr>';
      return;
    }
    tbody.innerHTML = list.map(b => {
      const kb = (b.size_bytes / 1024).toFixed(1);
      const ts = (b.created_at || '').replace('T', ' ').slice(0, 19) + ' UTC';
      return `<tr>
        <td><code style="font-size:12px">${esc(b.filename)}</code></td>
        <td style="white-space:nowrap">${esc(kb)} KB</td>
        <td style="white-space:nowrap;color:var(--text-dim)">${esc(ts)}</td>
        <td style="white-space:nowrap">
          <button class="btn-restore secondary" onclick="confirmRestore('${esc(b.filename)}', false)">Restore</button>
          <button class="btn-force"              onclick="confirmRestore('${esc(b.filename)}', true)">&#9888; Force restore</button>
          <button class="secondary" style="color:var(--danger,#f85149)" onclick="deleteBackup('${esc(b.filename)}', this)">&#10005; Delete</button>
        </td>
      </tr>`;
    }).join('');
  } catch (e) {
    err.textContent = `Failed to load backups: ${e.message}`;
    err.hidden = false;
  }
}

async function deleteBackup(filename, btn) {
  if (!confirm(`Delete backup ${filename}?\n\nThis cannot be undone.`)) return;
  const orig = btn.innerHTML;
  btn.disabled = true; btn.textContent = '…';
  try {
    const r = await apiFetch(`/api/v1/backup/${encodeURIComponent(filename)}`, { method: 'DELETE' });
    if (r.ok) {
      await loadBackups();
    } else {
      btn.innerHTML = `&#10007; ${r.status}`; btn.style.color = 'var(--danger,#f85149)';
      setTimeout(() => { btn.disabled = false; btn.innerHTML = orig; btn.style.color = ''; }, 3000);
    }
  } catch (e) {
    btn.innerHTML = '&#10007; err'; btn.style.color = 'var(--danger,#f85149)';
    setTimeout(() => { btn.disabled = false; btn.innerHTML = orig; btn.style.color = ''; }, 3000);
  }
}

async function createBackup(btn) {
  const origText = btn.textContent;
  btn.disabled = true;
  btn.textContent = 'Creating…';
  const err = document.getElementById('backup-error');
  err.hidden = true;
  try {
    const r = await apiFetch('/api/v1/backup', { method: 'POST' });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const d = await r.json();
    await loadBackups();
    // Flash a brief confirmation
    btn.textContent = '✓ Done';
    setTimeout(() => { btn.textContent = origText; btn.disabled = false; }, 2000);
  } catch (e) {
    err.textContent = `Backup failed: ${e.message}`;
    err.hidden = false;
    btn.textContent = origText;
    btn.disabled = false;
  }
}

function confirmRestore(filename, force) {
  document.getElementById('restore-filename').textContent = filename;
  document.getElementById('restore-warn-normal').hidden = force;
  document.getElementById('restore-warn-force').hidden  = !force;
  document.getElementById('restore-result').hidden = true;
  document.getElementById('restore-result').textContent = '';
  document.getElementById('restore-modal').dataset.filename = filename;
  document.getElementById('restore-modal').dataset.force = force ? 'true' : 'false';
  document.getElementById('restore-modal').showModal();
}

async function submitRestore() {
  const modal    = document.getElementById('restore-modal');
  const filename = modal.dataset.filename;
  const force    = modal.dataset.force === 'true';
  const btn      = document.getElementById('restore-confirm-btn');
  const resultEl = document.getElementById('restore-result');
  btn.disabled = true;
  btn.textContent = 'Restoring…';
  resultEl.hidden = true;
  try {
    const url = `/api/v1/backup/restore/${encodeURIComponent(filename)}${force ? '?force=true' : ''}`;
    const r = await apiFetch(url, { method: 'POST' });
    const d = await r.json();
    if (!r.ok) throw new Error(d.detail || `HTTP ${r.status}`);
    let msg = `✓ Restored from ${d.restored_from}. Gen: ${d.gen_before} → ${d.gen_after}.`;
    if (d.warning) msg += `\n⚠ ${d.warning}`;
    resultEl.textContent = msg;
    resultEl.className = 'restore-result';
    resultEl.hidden = false;
    btn.textContent = 'Done';
    // Refresh health + sync after restore
    setTimeout(() => { loadHealth(); loadSyncStatus(); loadBackups(); }, 500);
  } catch (e) {
    resultEl.textContent = `Error: ${e.message}`;
    resultEl.className = 'restore-result force-box';
    resultEl.hidden = false;
    btn.textContent = 'Restore';
    btn.disabled = false;
  }
}
