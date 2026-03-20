/* ── Bookmarks (browser-links) ───────────────────────────────────────── */

let _bmSearchTimer = null;
let _bmSearchActive = false;

// ── Load / Refresh ──────────────────────────────────────────────────────

async function loadBookmarks() {
  const err = document.getElementById('bm-error');
  err.hidden = true;
  const archived = document.getElementById('bm-show-archived')?.checked ? 1 : 0;
  try {
    const r = await apiFetch(`/api/v1/bookmarks?archived=${archived}&limit=2000`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    _bookmarks = await r.json();
    _bmSearchActive = false;
    document.getElementById('bm-search-status').hidden = true;
    await _loadBookmarkTags();
    renderBookmarks();
  } catch (e) {
    err.textContent = `Failed to load bookmarks: ${e.message}`;
    err.hidden = false;
  }
}

async function _loadBookmarkTags() {
  try {
    const r = await apiFetch('/api/v1/bookmarks/tags');
    if (!r.ok) return;
    const tags = await r.json();
    const sel = document.getElementById('bm-tag-filter');
    const prev = sel.value;
    sel.innerHTML = '<option value="">All tags</option>' +
      tags.map(t => `<option value="${esc(t)}">${esc(t)}</option>`).join('');
    sel.value = prev;
  } catch (_) {}
}

// ── Search ──────────────────────────────────────────────────────────────

function _bmSearchDebounce() {
  clearTimeout(_bmSearchTimer);
  const q = (document.getElementById('bm-search').value || '').trim();
  if (!q) {
    _bmSearchActive = false;
    document.getElementById('bm-search-status').hidden = true;
    renderBookmarks();
    return;
  }
  _bmSearchTimer = setTimeout(() => _runBmSearch(q), 500);
}

async function _runBmSearch(q) {
  try {
    const r = await apiFetch(`/api/v1/bookmarks/search?q=${encodeURIComponent(q)}&limit=50`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    _bmSearchActive = true;
    const status = document.getElementById('bm-search-status');
    status.textContent = `SeekDB: ${data.count} result${data.count === 1 ? '' : 's'} for "${q}"`;
    status.hidden = false;
    _renderBmSearchResults(data.results);
  } catch (e) {
    const err = document.getElementById('bm-error');
    err.textContent = `Search failed: ${e.message}`;
    err.hidden = false;
  }
}

function _renderBmSearchResults(results) {
  const tbody = document.getElementById('bm-tbody');
  if (!results.length) {
    tbody.innerHTML = '<tr class="empty-row"><td colspan="8">No results found.</td></tr>';
    return;
  }
  tbody.innerHTML = results.map(r => {
    const isBookmark = r.item_type !== 'visit';
    const icon = isBookmark ? '&#128278;' : '&#128065;';
    const tags = (r.tags || []).map(t => _bmTagPill(t)).join(' ');
    const editBtns = isBookmark ? `
      <button class="secondary" style="padding:1px 6px;font-size:11px"
        onclick="openBookmarkModal('${esc(r.id)}')">&#9998;</button>
      <button class="secondary" style="padding:1px 6px;font-size:11px;color:#f87171;border-color:#f87171;margin-left:2px"
        onclick="deleteBookmark('${esc(r.id)}','${esc(r.title || r.url)}')">&#x2715;</button>` : '';
    return `<tr>
      <td style="text-align:center">${icon}</td>
      <td><a href="${esc(r.url)}" target="_blank" rel="noopener noreferrer" style="color:var(--accent)">${esc(r.title || r.url)}</a></td>
      <td style="font-size:11px;color:var(--text-dim);max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(r.url)}">${_bmTruncUrl(r.url)}</td>
      <td style="font-size:11px">${tags}</td>
      <td style="font-size:12px;color:var(--text-dim);max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(r.description || r.notes || '')}</td>
      <td style="font-size:11px;color:var(--text-dim)">${esc(r.source || '')}</td>
      <td style="font-size:11px;color:var(--text-dim);white-space:nowrap">${_bmFmtDate(r.visited_at || '')}</td>
      <td style="white-space:nowrap">${editBtns}</td>
    </tr>`;
  }).join('');
}

// ── Render table (local filter, no SeekDB) ──────────────────────────────

function renderBookmarks() {
  if (_bmSearchActive) return;
  const q = (document.getElementById('bm-search')?.value || '').toLowerCase();
  const tagFilter = document.getElementById('bm-tag-filter')?.value || '';
  let rows = _bookmarks;
  if (q) {
    rows = rows.filter(b =>
      (b.title || '').toLowerCase().includes(q) ||
      (b.url || '').toLowerCase().includes(q) ||
      (b.description || '').toLowerCase().includes(q) ||
      (b.notes || '').toLowerCase().includes(q) ||
      (b.tags || []).some(t => t.toLowerCase().includes(q))
    );
  }
  if (tagFilter) {
    rows = rows.filter(b => (b.tags || []).includes(tagFilter));
  }
  const tbody = document.getElementById('bm-tbody');
  if (!rows.length) {
    tbody.innerHTML = '<tr class="empty-row"><td colspan="8">No bookmarks found.</td></tr>';
    return;
  }
  tbody.innerHTML = rows.map(b => {
    const tags = (b.tags || []).map(t => _bmTagPill(t)).join(' ');
    const archiveStyle = b.archived ? 'opacity:0.55' : '';
    return `<tr style="${archiveStyle}">
      <td style="text-align:center">&#128278;</td>
      <td><a href="${esc(b.url)}" target="_blank" rel="noopener noreferrer" style="color:var(--accent)">${esc(b.title || b.url)}</a></td>
      <td style="font-size:11px;color:var(--text-dim);max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(b.url)}">${_bmTruncUrl(b.url)}</td>
      <td style="font-size:11px">${tags}</td>
      <td style="font-size:12px;color:var(--text-dim);max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(b.description || '')}</td>
      <td style="font-size:11px;color:var(--text-dim)">${esc(b.source || 'manual')}</td>
      <td style="font-size:11px;color:var(--text-dim);white-space:nowrap">${_bmFmtDate(b.created_at || '')}</td>
      <td style="white-space:nowrap">
        <button class="secondary" style="padding:1px 6px;font-size:11px"
          onclick="openBookmarkModal('${esc(b.bookmark_id)}')">&#9998;</button>
        <button class="secondary" style="padding:1px 6px;font-size:11px;color:#f87171;border-color:#f87171;margin-left:2px"
          onclick="deleteBookmark('${esc(b.bookmark_id)}','${esc(b.title || b.url)}')">&#x2715;</button>
      </td>
    </tr>`;
  }).join('');
}

// ── Visits ──────────────────────────────────────────────────────────────

async function loadVisits() {
  const err = document.getElementById('bm-error');
  err.hidden = true;
  try {
    const r = await apiFetch('/api/v1/bookmarks/visits?limit=1000');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    _bmVisits = await r.json();
    renderVisits();
  } catch (e) {
    err.textContent = `Failed to load visits: ${e.message}`;
    err.hidden = false;
  }
}

function renderVisits() {
  const q = (document.getElementById('bm-visit-search')?.value || '').toLowerCase();
  let rows = _bmVisits;
  if (q) {
    rows = rows.filter(v =>
      (v.title || '').toLowerCase().includes(q) ||
      (v.url || '').toLowerCase().includes(q) ||
      (v.domain || '').toLowerCase().includes(q)
    );
  }
  const tbody = document.getElementById('bm-visits-tbody');
  if (!rows.length) {
    tbody.innerHTML = '<tr class="empty-row"><td colspan="6">No visit history.</td></tr>';
    return;
  }
  tbody.innerHTML = rows.map(v => {
    const dwell = v.dwell_seconds ? `${v.dwell_seconds}s` : '—';
    const saveBtn = v.bookmark_id ? '' :
      `<button class="secondary" style="padding:1px 6px;font-size:11px"
        onclick="promoteVisitToBookmark('${esc(v.url)}','${esc(v.title || '')}')">&#128278; Save</button>`;
    return `<tr>
      <td>${esc(v.title || '')}</td>
      <td style="font-size:11px;color:var(--text-dim);max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(v.url)}">${_bmTruncUrl(v.url)}</td>
      <td style="font-size:11px;color:var(--text-dim)">${esc(v.source || '')}</td>
      <td style="font-size:11px;color:var(--text-dim)">${dwell}</td>
      <td style="font-size:11px;color:var(--text-dim);white-space:nowrap">${_bmFmtDate(v.visited_at || '')}</td>
      <td style="white-space:nowrap">${saveBtn}</td>
    </tr>`;
  }).join('');
}

function _bmToggleVisits() {
  const showVisits = document.getElementById('bm-show-visits')?.checked;
  document.getElementById('bm-main-view').style.display = showVisits ? 'none' : '';
  document.getElementById('bm-visits-view').style.display = showVisits ? '' : 'none';
  if (showVisits && !_bmVisits.length) loadVisits();
}

function _bmToggleSetup() {
  const panel = document.getElementById('bm-setup-panel');
  if (!panel) return;
  panel.style.display = panel.style.display === 'none' ? '' : 'none';
}

function promoteVisitToBookmark(url, title) {
  openBookmarkModal(null);
  document.getElementById('bm-modal-url').value = url || '';
  document.getElementById('bm-modal-title-input').value = title || '';
}

// ── Add / Edit modal ────────────────────────────────────────────────────

async function openBookmarkModal(id) {
  const modal = document.getElementById('bm-modal');
  document.getElementById('bm-modal-id').value = id || '';
  document.getElementById('bm-modal-heading').textContent = id ? 'Edit bookmark' : 'Add bookmark';
  document.getElementById('bm-modal-error').hidden = true;
  if (id) {
    try {
      const r = await apiFetch(`/api/v1/bookmarks/${id}`);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const b = await r.json();
      document.getElementById('bm-modal-url').value         = b.url || '';
      document.getElementById('bm-modal-title-input').value = b.title || '';
      document.getElementById('bm-modal-desc').value        = b.description || '';
      document.getElementById('bm-modal-tags').value        = (b.tags || []).join(', ');
      document.getElementById('bm-modal-folder').value      = b.folder || '';
      document.getElementById('bm-modal-notes').value       = b.notes || '';
      document.getElementById('bm-modal-archived').checked  = b.archived || false;
    } catch (e) {
      document.getElementById('bm-modal-error').textContent = `Failed to load: ${e.message}`;
      document.getElementById('bm-modal-error').hidden = false;
    }
  } else {
    document.getElementById('bm-modal-url').value         = '';
    document.getElementById('bm-modal-title-input').value = '';
    document.getElementById('bm-modal-desc').value        = '';
    document.getElementById('bm-modal-tags').value        = '';
    document.getElementById('bm-modal-folder').value      = '';
    document.getElementById('bm-modal-notes').value       = '';
    document.getElementById('bm-modal-archived').checked  = false;
  }
  modal.showModal();
}

async function saveBookmark() {
  const id    = document.getElementById('bm-modal-id').value;
  const url   = document.getElementById('bm-modal-url').value.trim();
  const errEl = document.getElementById('bm-modal-error');
  errEl.hidden = true;
  if (!url) { errEl.textContent = 'URL is required.'; errEl.hidden = false; return; }

  const body = {
    url,
    title:       document.getElementById('bm-modal-title-input').value.trim() || null,
    description: document.getElementById('bm-modal-desc').value.trim() || null,
    tags:        document.getElementById('bm-modal-tags').value.split(',').map(t => t.trim()).filter(Boolean),
    folder:      document.getElementById('bm-modal-folder').value.trim() || null,
    notes:       document.getElementById('bm-modal-notes').value.trim() || null,
    source:      id ? undefined : 'manual',
    archived:    document.getElementById('bm-modal-archived').checked,
  };
  if (body.source === undefined) delete body.source;

  try {
    const r = id
      ? await apiFetch(`/api/v1/bookmarks/${id}`,
          { method: 'PUT',  headers: {'Content-Type':'application/json'}, body: JSON.stringify(body) })
      : await apiFetch('/api/v1/bookmarks',
          { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(body) });
    if (!r.ok) { const d = await r.json().catch(() => ({})); throw new Error(d.detail || `HTTP ${r.status}`); }
    document.getElementById('bm-modal').close();
    await loadBookmarks();
  } catch (e) {
    errEl.textContent = `Save failed: ${e.message}`;
    errEl.hidden = false;
  }
}

async function deleteBookmark(id, title) {
  if (!confirm(`Delete bookmark "${title}"?`)) return;
  try {
    const r = await apiFetch(`/api/v1/bookmarks/${id}`, { method: 'DELETE' });
    if (!r.ok && r.status !== 204) throw new Error(`HTTP ${r.status}`);
    _bookmarks = _bookmarks.filter(b => b.bookmark_id !== id);
    renderBookmarks();
  } catch (e) {
    const err = document.getElementById('bm-error');
    err.textContent = `Delete failed: ${e.message}`;
    err.hidden = false;
  }
}

// ── HTML Import (client-side parse + POST to /api/v1/bookmarks/import) ──

async function importBookmarksFile(input) {
  const file = input.files[0];
  if (!file) return;
  const statusEl = document.getElementById('bm-import-status');
  statusEl.textContent = `Parsing ${esc(file.name)}…`;
  statusEl.style.color = 'var(--text-dim)';
  statusEl.hidden = false;
  input.value = '';  // reset so same file can be re-selected

  try {
    const html = await file.text();
    const bookmarks = _parseNetscapeBookmarks(html);
    if (!bookmarks.length) {
      statusEl.textContent = 'No bookmarks found in file.';
      statusEl.style.color = 'var(--warn)';
      return;
    }
    statusEl.textContent = `Importing ${bookmarks.length} bookmarks…`;
    const r = await apiFetch('/api/v1/bookmarks/import', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ bookmarks, skip_duplicates: true }),
    });
    if (!r.ok) { const d = await r.json().catch(() => ({})); throw new Error(d.detail || `HTTP ${r.status}`); }
    const result = await r.json();
    statusEl.textContent = `Done — imported ${result.imported}, skipped ${result.skipped_duplicates} duplicates.`;
    statusEl.style.color = 'var(--ok)';
    await loadBookmarks();
  } catch (e) {
    statusEl.textContent = `Import failed: ${e.message}`;
    statusEl.style.color = 'var(--err)';
  }
}

// Parse Netscape bookmark HTML format (used by Edge, Chrome, Firefox)
function _parseNetscapeBookmarks(html) {
  const bookmarks = [];
  const folderStack = [];
  let pendingFolder = null;
  // Match DL enter/exit, H3 folder headings, and A bookmark links
  const re = /<DL[^>]*>|<\/DL[^>]*>|<H3[^>]*>([\s\S]*?)<\/H3>|<A\s([^>]+)>([\s\S]*?)<\/A>/gi;
  let m;
  while ((m = re.exec(html)) !== null) {
    const full = m[0];
    if (/^<DL/i.test(full)) {
      folderStack.push(pendingFolder || '');
      pendingFolder = null;
      continue;
    }
    if (/^<\/DL/i.test(full)) {
      folderStack.pop();
      continue;
    }
    if (/^<H3/i.test(full)) {
      pendingFolder = _bmStripHtml(m[1] || '');
      continue;
    }
    if (/^<A\s/i.test(full)) {
      const attrs = m[2] || '';
      const text  = m[3] || '';
      const hrefM = /HREF="([^"]+)"/i.exec(attrs);
      if (!hrefM) continue;
      const url = hrefM[1];
      if (!url || /^(javascript:|about:)/i.test(url)) continue;
      const title = _bmStripHtml(text) || url;
      const folderParts = folderStack.filter(Boolean);
      const folder = folderParts.join('/') || null;
      const tags = folderParts
        .map(f => f.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, ''))
        .filter(Boolean);
      bookmarks.push({ url, title, folder, tags, description: null, notes: null, favicon_url: null, source: 'import' });
    }
  }
  return bookmarks;
}

function _bmStripHtml(html) {
  return (html || '')
    .replace(/<[^>]+>/g, '')
    .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"').replace(/&#39;/g, "'")
    .trim();
}

// ── Utilities ────────────────────────────────────────────────────────────

function _bmTagPill(tag) {
  return `<span style="display:inline-block;background:rgba(99,102,241,.18);color:#a5b4fc;border:1px solid rgba(99,102,241,.3);border-radius:3px;padding:0 5px;font-size:10px;margin:1px">${esc(tag)}</span>`;
}

function _bmTruncUrl(url) {
  try {
    const u = new URL(url);
    const path = u.pathname + u.search;
    const trunc = path.length > 40 ? path.slice(0, 38) + '\u2026' : path;
    return esc(u.host + trunc);
  } catch (_) {
    return esc((url || '').slice(0, 50));
  }
}

function _bmFmtDate(iso) {
  if (!iso) return '\u2014';
  try {
    return new Date(iso).toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' });
  } catch (_) {
    return iso;
  }
}
