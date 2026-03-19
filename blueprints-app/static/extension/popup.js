/* popup.js — Blueprints Bookmarks Extension popup */

function showPanel(name) {
  document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById(`panel-${name}`).classList.add('active');
  document.getElementById(`tab-${name}-btn`).classList.add('active');
  if (name === 'search') document.getElementById('search-q').focus();
}

// ── Save ──────────────────────────────────────────────────────────────────

async function saveCurrent() {
  const statusEl = document.getElementById('save-status');
  statusEl.textContent = '';
  statusEl.className = 'status';

  const url   = document.getElementById('save-url').value.trim();
  const title = document.getElementById('save-title').value.trim();
  const tags  = document.getElementById('save-tags').value.split(',').map(t => t.trim()).filter(Boolean);
  const notes = document.getElementById('save-notes').value.trim();

  if (!url) {
    statusEl.textContent = 'URL required.';
    statusEl.className = 'status err';
    return;
  }

  try {
    const r = await apiFetch('/api/v1/bookmarks', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url, title: title || null, tags, notes: notes || null, source: 'extension' }),
    });
    if (!r.ok) {
      const d = await r.json().catch(() => ({}));
      throw new Error(d.detail || `HTTP ${r.status}`);
    }
    statusEl.textContent = '✓ Saved!';
    statusEl.className = 'status ok';
    setTimeout(() => window.close(), 1200);
  } catch (e) {
    statusEl.textContent = `Save failed: ${e.message}`;
    statusEl.className = 'status err';
  }
}

// ── Search ────────────────────────────────────────────────────────────────

async function runSearch() {
  const q = document.getElementById('search-q').value.trim();
  const statusEl  = document.getElementById('search-status');
  const resultsEl = document.getElementById('search-results');
  if (!q) return;
  statusEl.textContent = 'Searching…';
  statusEl.className = 'status';
  resultsEl.innerHTML = '';

  try {
    const r = await apiFetch(`/api/v1/bookmarks/search?q=${encodeURIComponent(q)}&limit=15`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    statusEl.textContent = `${data.count} result${data.count === 1 ? '' : 's'}`;
    statusEl.className = 'status';
    renderResults(data.results);
  } catch (e) {
    statusEl.textContent = `Search failed: ${e.message}`;
    statusEl.className = 'status err';
  }
}

function renderResults(results) {
  const el = document.getElementById('search-results');
  if (!results.length) {
    el.innerHTML = '<div style="font-size:12px;color:var(--text-dim)">No results.</div>';
    return;
  }
  el.innerHTML = results.map(r => {
    const tags = (r.tags || []).map(t => `<span class="tag-pill">${esc(t)}</span>`).join('');
    return `<div class="result-item" onclick="openUrl('${esc(r.url)}')">
      <div class="result-title">${esc(r.title || r.url)}</div>
      <div class="result-url">${esc(r.url || '')}</div>
      ${tags ? `<div class="result-tags">${tags}</div>` : ''}
    </div>`;
  }).join('');
}

function openUrl(url) {
  chrome.tabs.create({ url });
}

function esc(s) {
  return String(s || '').replace(/[&<>"']/g, c =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

// ── Init — populate save panel from active tab ────────────────────────────

document.addEventListener('DOMContentLoaded', async () => {
  try {
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    if (tab) {
      document.getElementById('save-url').value   = tab.url   || '';
      document.getElementById('save-title').value = tab.title || '';
    }
  } catch (_) {}
});
