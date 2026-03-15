/* ── Sync status ──────────────────────────────────────────────────────── */
async function loadSyncStatus() {
  const grid = document.getElementById('sync-grid');
  const err = document.getElementById('sync-error');
  err.hidden = true;
  try {
    const r = await apiFetch('/api/v1/sync/status');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const d = await r.json();
    document.getElementById('nn-peers').textContent = d.peer_count;
    const qs = d.queue_depths || {};
    const items = [
      ['Gen', d.gen],
      ['Last write by', d.last_write_by],
      ['Last write at', (d.last_write_at||'').replace('T',' ').slice(0,19)],
    ];
    Object.entries(qs).forEach(([peer, depth]) => {
      items.push([`Queue → ${peer}`, depth === 0 ? '✓ clear' : `${depth} pending`]);
    });
    if (!Object.keys(qs).length) items.push(['Peers', '(none registered)']);
    grid.innerHTML = items.map(([k,v]) => `
      <div class="sync-item"><span style="color:var(--text-dim)">${esc(String(k))}</span>
        <strong>${esc(String(v))}</strong></div>`).join('');
  } catch (e) {
    err.textContent = `Sync status unavailable: ${e.message}`;
    err.hidden = false;
    grid.innerHTML = '';
  }
}
