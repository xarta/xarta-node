/* ── Bookmarks (browser-links) ───────────────────────────────────────── */

let _bmSearchTimer = null;      // server SeekDB search debounce
let _bmRenderTimer = null;      // client-side filter render debounce
let _bmSearchActive = false;
let _bmExcludedTags = new Set(); // tags excluded from embeddings — also skipped in client-side keyword filter
let _bmSortCol = 'created_at';
let _bmSortDir = 'desc';
let _bmColResizeDone = false;
let _bmAllTags = [];
let _bmTagCounts = {};       // {tag -> {active, archived}}
let _bmCurrentExclTags = []; // source of truth; kept in sync with server

async function _bmDownloadExtension(btn) {
  const orig = btn.textContent;
  btn.disabled = true;
  btn.textContent = 'Downloading…';
  try {
    const r = await apiFetch('/api/v1/bookmarks/extension-download');
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const blob = await r.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'blueprints-bookmarks-extension.zip';
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  } catch (e) {
    alert(`Download failed: ${e.message}`);
  } finally {
    btn.disabled = false;
    btn.textContent = orig;
  }
}

// ── Load / Refresh ──────────────────────────────────────────────────────

async function loadBookmarks() {
  const err = document.getElementById('bm-error');
  err.hidden = true;
  const archived = document.getElementById('bm-show-archived')?.checked ? 1 : 0;
  try {
    const limit = parseInt(getFrontendSetting('bm_fetch_limit', 10000), 10);
    const r = await apiFetch(`/api/v1/bookmarks?archived=${archived}&limit=${limit}`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    _bookmarks = await r.json();
    _bmSearchActive = false;
    document.getElementById('bm-search-status').hidden = true;
    await Promise.all([_loadBookmarkTags(), _loadExcludedTags()]);
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

async function _loadExcludedTags() {
  try {
    const r = await apiFetch('/api/v1/bookmarks/embedding-config');
    if (!r.ok) return;
    const cfg = await r.json();
    _bmExcludedTags = new Set((cfg.excluded_tags || []).map(t => t.toLowerCase()));
  } catch (_) {}
}

// ── Search ──────────────────────────────────────────────────────────────

function _bmSearchDebounce() {
  clearTimeout(_bmSearchTimer);
  clearTimeout(_bmRenderTimer);
  const q = (document.getElementById('bm-search').value || '').trim();
  if (!q) {
    _bmSearchActive = false;
    document.getElementById('bm-search-status').hidden = true;
    renderBookmarks();
    return;
  }
  // Debounce client-side filter (250ms) — fast enough to feel responsive,
  // slow enough to avoid rebuilding the full table on every keystroke.
  _bmRenderTimer = setTimeout(() => {
    _bmSearchActive = false;
    renderBookmarks();
  }, 250);
  // Debounce server SeekDB search (600ms) — fires after typing pauses.
  _bmSearchTimer = setTimeout(() => _runBmSearch(q), 600);
}

async function _runBmSearch(q) {
  try {
    const r = await apiFetch(`/api/v1/bookmarks/search?q=${encodeURIComponent(q)}&limit=50`);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    const status = document.getElementById('bm-search-status');
    if (data.count > 0) {
      _bmSearchActive = true;
      status.textContent = `SeekDB: ${data.count} result${data.count === 1 ? '' : 's'} for "${q}"`;
      status.hidden = false;
      _renderBmSearchResults(data.results);
    } else {
      // SeekDB has no results (likely not yet indexed) — keep client-side filter
      _bmSearchActive = false;
      status.textContent = `SeekDB: 0 results for "${q}"`;
      status.hidden = false;
    }
  } catch (e) {
    // SeekDB unavailable — client-side filter already showing, suppress error
    _bmSearchActive = false;
  }
}

function _renderBmSearchResults(results) {
  const status = document.getElementById('bm-search-status');
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
      <button class="secondary" style="padding:1px 6px;font-size:11px;color:var(--text-dim);border-color:var(--border);margin-left:2px" title="Archive"
        onclick="archiveBookmark('${esc(r.id)}', false)">&#128229;</button>
      <button class="secondary" style="padding:1px 6px;font-size:11px;color:#f87171;border-color:#f87171;margin-left:2px"
        onclick="deleteBookmark('${esc(r.id)}','${esc(r.title || r.url)}')">&#x2715;</button>` : '';
    return `<tr>
      <td style="text-align:center">${icon}</td>
      <td><a href="${esc(r.url)}" target="_blank" rel="noopener noreferrer" style="color:var(--accent)">${esc(r.title || r.url)}</a></td>
      <td style="font-size:11px;color:var(--text-dim);max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(r.url)}">${_bmTruncUrl(r.url)}</td>
      <td style="font-size:11px">${tags}</td>
      <td style="font-size:12px;color:var(--text-dim);max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(r.description || r.notes || '')}</td>
      <td style="font-size:11px;color:var(--text-dim)">${esc(r.source || '')}</td>
      <td style="font-size:11px;color:var(--text-dim);white-space:nowrap">${_bmFmtDate(isBookmark ? (r.created_at || '') : (r.visited_at || ''))}</td>
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
      (b.tags || []).some(t => !_bmExcludedTags.has(t.toLowerCase()) && t.toLowerCase().includes(q))
    );
  }
  if (tagFilter) {
    rows = rows.filter(b => (b.tags || []).includes(tagFilter));
  }
  // Apply sort
  if (_bmSortCol) {
    rows = [...rows].sort((a, b) => {
      const av = _bmSortVal(a, _bmSortCol);
      const bv = _bmSortVal(b, _bmSortCol);
      return _bmSortDir === 'asc' ? av.localeCompare(bv) : bv.localeCompare(av);
    });
  }
  const tbody = document.getElementById('bm-tbody');
  const status = document.getElementById('bm-search-status');
  status.textContent = rows.length + ' bookmark' + (rows.length === 1 ? '' : 's');
  status.hidden = false;
  if (!rows.length) {
    tbody.innerHTML = '<tr class="empty-row"><td colspan="8">No bookmarks found.</td></tr>';
    _bmUpdateSortHeaders();
    return;
  }
  tbody.innerHTML = rows.map(b => {
    const tags = (b.tags || []).map(t => _bmTagPill(t)).join(' ');
    const archiveStyle = b.archived ? 'opacity:0.55' : '';
    const archBtn = b.archived
      ? `<button class="secondary" style="padding:1px 6px;font-size:11px;color:var(--ok);border-color:var(--ok);margin-left:2px" title="Restore from archive"
          onclick="archiveBookmark('${esc(b.bookmark_id)}', true)">&#128228;</button>`
      : `<button class="secondary" style="padding:1px 6px;font-size:11px;color:var(--text-dim);border-color:var(--border);margin-left:2px" title="Archive"
          onclick="archiveBookmark('${esc(b.bookmark_id)}', false)">&#128229;</button>`;
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
        ${archBtn}
        <button class="secondary" style="padding:1px 6px;font-size:11px;color:#f87171;border-color:#f87171;margin-left:2px"
          onclick="deleteBookmark('${esc(b.bookmark_id)}','${esc(b.title || b.url)}')">&#x2715;</button>
      </td>
    </tr>`;
  }).join('');
  _bmUpdateSortHeaders();
  _bmInitColResize();
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
  const opening = panel.style.display === 'none';
  panel.style.display = opening ? '' : 'none';
  if (opening) _bmPopulateExtUrls();
}

let _bmReindexPollTimer = null;

function _bmToggleEmbedCfg() {
  const panel = document.getElementById('bm-embed-panel');
  if (!panel) return;
  const opening = panel.style.display === 'none';
  panel.style.display = opening ? '' : 'none';
  if (opening) _bmLoadEmbedCfg();
}

async function _bmLoadEmbedCfg() {
  try {
    const r = await apiFetch('/api/v1/bookmarks/embedding-config');
    if (!r.ok) return;
    const cfg = await r.json();
    _bmRenderExclTags(cfg.excluded_tags || []);
    _bmCurrentExclTags = cfg.excluded_tags || [];
    const thr = document.getElementById('bm-domain-threshold');
    if (thr) thr.value = cfg.domain_threshold ?? 3;
    const analyzeStatus = document.getElementById('bm-analyze-status');
    if (analyzeStatus && cfg.rare_domains_count != null)
      analyzeStatus.textContent = `${cfg.rare_domains_count} rare domains stored`;
  } catch (_) {}
  // Populate tag datalist from the full tag list
  _bmPopulateExclTagDatalist();
  // Also check if reindex is already running (survives page refresh)
  _bmPollReindexProgress();
}

// ── Embedding config panel ────────────────────────────────────────────────────

async function _bmPopulateExclTagDatalist() {
  try {
    const r = await apiFetch('/api/v1/bookmarks/tags-with-counts');
    if (!r.ok) return;
    const rows = await r.json(); // [{tag, active, archived}, ...]
    _bmTagCounts = {};
    _bmAllTags = rows.map(row => {
      _bmTagCounts[row.tag] = { active: row.active, archived: row.archived };
      return row.tag;
    });
  } catch (_) {}
}

function _bmOpenExclTagModal() {
  const modal = document.getElementById('bm-excl-tag-modal');
  if (!modal) return;
  document.getElementById('bm-excl-modal-search').value = '';
  document.getElementById('bm-excl-modal-status').textContent = '';
  const excluded = new Set(_bmCurrentExclTags);
  _bmRenderExclTagModalList(excluded, '');
  _bmUpdateExclModalCount();
  modal.showModal();
  document.getElementById('bm-excl-modal-search').focus();
}

function _bmRenderExclTagModalList(excluded, filter) {
  const container = document.getElementById('bm-excl-modal-list');
  if (!container) return;
  const f = filter.toLowerCase().trim();
  // Union: all known tags + any excluded tags that have no bookmarks (orphans)
  const allKnown = new Set(_bmAllTags);
  const combined = [..._bmAllTags];
  for (const t of excluded) { if (!allKnown.has(t)) combined.unshift(t); }
  const visible = f ? combined.filter(t => t.includes(f)) : combined;
  // Excluded tags sorted to top, then alpha
  visible.sort((a, b) => {
    const ae = excluded.has(a), be = excluded.has(b);
    if (ae !== be) return ae ? -1 : 1;
    return a.localeCompare(b);
  });
  container.innerHTML = visible.map(tag => {
    const checked = excluded.has(tag) ? 'checked' : '';
    const c = _bmTagCounts[tag] || { active: 0, archived: 0 };
    const activeTxt = `<span class="bm-tc-active" title="active">${c.active}</span>`;
    const archTxt   = `<span class="bm-tc-arch"   title="archived">${c.archived}</span>`;
    return `<label><input type="checkbox" data-tag="${esc(tag)}" ${checked} /><span class="bm-tc-name">${esc(tag)}</span><span class="bm-tc-counts">${activeTxt}${archTxt}</span></label>`;
  }).join('');
}

function _bmGetExclTagModalSelected() {
  return Array.from(
    document.querySelectorAll('#bm-excl-modal-list input[type=checkbox]:checked')
  ).map(cb => cb.dataset.tag);
}

function _bmUpdateExclModalCount() {
  const checked = document.querySelectorAll('#bm-excl-modal-list input[type=checkbox]:checked').length;
  const total   = document.querySelectorAll('#bm-excl-modal-list input[type=checkbox]').length;
  const el = document.getElementById('bm-excl-modal-count');
  if (el) el.textContent = `${checked} excluded • ${total} shown`;
}

function _bmRenderExclTags(tags) {
  const list = document.getElementById('bm-excl-tag-list');
  if (!list) return;
  list.innerHTML = '';
  (tags || []).forEach(tag => {
    const chip = document.createElement('span');
    chip.style.cssText = 'display:inline-flex;align-items:center;gap:4px;background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:2px 8px;font-size:12px';
    chip.dataset.tag = tag;
    chip.innerHTML = `${esc(tag)} <button data-remove-tag="${esc(tag)}" style="background:none;border:none;cursor:pointer;font-size:13px;line-height:1;padding:0;color:var(--text-dim)">&#10005;</button>`;
    list.appendChild(chip);
  });
}

function _bmGetExclTags() {
  const list = document.getElementById('bm-excl-tag-list');
  if (!list) return [];
  return Array.from(list.querySelectorAll('[data-tag]')).map(el => el.dataset.tag);
}

function _bmInitEmbedPanel() {
  const panel = document.getElementById('bm-embed-panel');
  if (!panel) return;

  document.getElementById('bm-embed-close-btn')?.addEventListener('click', () => {
    panel.style.display = 'none';
  });

  // Open tag exclusion modal
  document.getElementById('bm-excl-tag-edit-btn')?.addEventListener('click', () => {
    if (!_bmAllTags.length) {
      _bmPopulateExclTagDatalist().then(() => _bmOpenExclTagModal());
    } else {
      _bmOpenExclTagModal();
    }
  });

  // Remove tag chip via event delegation
  document.getElementById('bm-excl-tag-list')?.addEventListener('click', e => {
    const btn = e.target.closest('[data-remove-tag]');
    if (!btn) return;
    _bmCurrentExclTags = _bmCurrentExclTags.filter(t => t !== btn.dataset.removeTag);
    _bmRenderExclTags(_bmCurrentExclTags);
  });

  // Save excluded tags (chip-level; updates server + state)
  document.getElementById('bm-excl-tag-save-btn')?.addEventListener('click', async () => {
    const statusEl = document.getElementById('bm-excl-tag-status');
    statusEl.textContent = 'Saving…';
    try {
      const r = await apiFetch('/api/v1/bookmarks/embedding-config', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ excluded_tags: _bmCurrentExclTags }),
      });
      statusEl.textContent = r.ok ? '✓ Saved' : `Error ${r.status}`;
    } catch (e) {
      statusEl.textContent = `Error: ${e.message}`;
    }
    setTimeout(() => { statusEl.textContent = ''; }, 3000);
  });

  // Modal: filter input re-renders list preserving checked state
  document.getElementById('bm-excl-modal-list')?.addEventListener('change', _bmUpdateExclModalCount);
  document.getElementById('bm-excl-modal-search')?.addEventListener('input', e => {
    const selected = new Set(_bmGetExclTagModalSelected());
    _bmRenderExclTagModalList(selected, e.target.value);
    _bmUpdateExclModalCount();
  });

  // Modal: Apply & Save
  document.getElementById('bm-excl-modal-apply-btn')?.addEventListener('click', async () => {
    const statusEl = document.getElementById('bm-excl-modal-status');
    const applyBtn = document.getElementById('bm-excl-modal-apply-btn');
    applyBtn.disabled = true;
    statusEl.textContent = 'Saving…';
    const tags = _bmGetExclTagModalSelected();
    try {
      const r = await apiFetch('/api/v1/bookmarks/embedding-config', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ excluded_tags: tags }),
      });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      _bmCurrentExclTags = tags;
      _bmRenderExclTags(tags);
      document.getElementById('bm-excl-tag-modal').close();
    } catch (e) {
      statusEl.textContent = `Error: ${e.message}`;
      applyBtn.disabled = false;
    }
  });

  // Restore apply button state when modal closes (ESC or cancel)
  document.getElementById('bm-excl-tag-modal')?.addEventListener('close', () => {
    const applyBtn = document.getElementById('bm-excl-modal-apply-btn');
    if (applyBtn) { applyBtn.disabled = false; }
    document.getElementById('bm-excl-modal-status').textContent = '';
  });

  // Analyse domains
  document.getElementById('bm-analyze-domains-btn')?.addEventListener('click', async () => {
    const btn = document.getElementById('bm-analyze-domains-btn');
    const statusEl = document.getElementById('bm-analyze-status');
    const threshold = parseInt(document.getElementById('bm-domain-threshold')?.value || '3', 10);
    btn.disabled = true;
    statusEl.textContent = 'Analysing…';
    try {
      // Save threshold first
      await apiFetch('/api/v1/bookmarks/embedding-config', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ domain_threshold: threshold }),
      });
      const r = await apiFetch('/api/v1/bookmarks/analyze-domains', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ domain_threshold: threshold }),
      });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const data = await r.json();
      statusEl.textContent = `✓ ${data.rare_domains_count} rare domains found (threshold ≤${data.threshold})`;
    } catch (e) {
      statusEl.textContent = `Error: ${e.message}`;
    } finally {
      btn.disabled = false;
    }
  });

  // Reindex all
  document.getElementById('bm-reindex-btn')?.addEventListener('click', async () => {
    const statusEl = document.getElementById('bm-reindex-status');
    statusEl.textContent = 'Starting…';
    try {
      const r = await apiFetch('/api/v1/bookmarks/reindex', { method: 'POST' });
      if (!r.ok) {
        const err = await r.json().catch(() => ({}));
        throw new Error(err.detail || `HTTP ${r.status}`);
      }
      statusEl.textContent = 'Running…';
      _bmPollReindexProgress();
    } catch (e) {
      statusEl.textContent = `Error: ${e.message}`;
    }
  });
}

function _bmPollReindexProgress() {
  if (_bmReindexPollTimer) return; // already polling
  _bmReindexPollTimer = setInterval(_bmCheckReindexProgress, 1500);
  _bmCheckReindexProgress();
}

async function _bmCheckReindexProgress() {
  try {
    const r = await apiFetch('/api/v1/bookmarks/reindex-progress');
    if (!r.ok) return;
    const state = await r.json();
    const wrap = document.getElementById('bm-reindex-progress-wrap');
    const bar = document.getElementById('bm-reindex-progress-bar');
    const label = document.getElementById('bm-reindex-progress-label');
    const statusEl = document.getElementById('bm-reindex-status');
    const btn = document.getElementById('bm-reindex-btn');

    if (state.running || state.total > 0) {
      if (wrap) wrap.style.display = '';
      const pct = state.total > 0 ? Math.round((state.done / state.total) * 100) : 0;
      if (bar) bar.style.width = `${pct}%`;
      if (label) label.textContent = `${state.done} / ${state.total} (${pct}%)`;
      if (btn) btn.disabled = state.running;
      if (state.running) {
        if (statusEl) statusEl.textContent = 'Running…';
      } else {
        // Completed
        if (statusEl) {
          statusEl.textContent = state.error
            ? `✗ Failed: ${state.error}`
            : `✓ Done — ${state.done} bookmarks re-embedded`;
          statusEl.style.color = state.error ? 'var(--err)' : 'var(--ok,#4caf50)';
        }
        if (label) label.textContent = state.error ? '' : `${state.done} / ${state.total} (100%)`;
        if (btn) btn.disabled = false;
        clearInterval(_bmReindexPollTimer);
        _bmReindexPollTimer = null;
      }
    } else {
      // Not running, nothing to show
      clearInterval(_bmReindexPollTimer);
      _bmReindexPollTimer = null;
    }
  } catch (_) {
    clearInterval(_bmReindexPollTimer);
    _bmReindexPollTimer = null;
  }
}

async function _bmPopulateExtUrls() {
  const loadingEl = document.getElementById('bm-ext-url-loading');
  const urlsEl    = document.getElementById('bm-ext-urls');
  if (!urlsEl || urlsEl.dataset.loaded) return;

  // Always include the URL the browser is currently using — it's working by definition
  const urls = [{ label: 'This page (current network)', url: window.location.origin }];

  // Also fetch peer nodes to show Tailscale URL if available
  try {
    const r = await apiFetch('/api/v1/nodes/self');
    if (r.ok) {
      const self = await r.json();
      if (self.tailnet_hostname) {
        const tsUrl = `https://${self.tailnet_hostname}`;
        if (tsUrl !== window.location.origin) {
          urls.push({ label: 'Tailscale', url: tsUrl });
        }
      }
      if (self.primary_hostname) {
        const lanUrl = `https://${self.primary_hostname}`;
        if (!urls.some(u => u.url === lanUrl)) {
          urls.push({ label: 'LAN hostname', url: lanUrl });
        }
      }
    }
  } catch (_) { /* non-fatal */ }

  const rows = urls.map(u =>
    `<div style="display:flex;align-items:center;gap:8px;margin-top:3px">` +
    `<span style="color:var(--text-dim);min-width:160px">${esc(u.label)}:</span>` +
    `<code style="background:rgba(255,255,255,.07);padding:2px 7px;border-radius:3px;user-select:all;cursor:text">${esc(u.url)}</code>` +
    `</div>`
  ).join('');

  if (loadingEl) loadingEl.style.display = 'none';
  urlsEl.innerHTML = rows;
  urlsEl.style.display = '';
  urlsEl.dataset.loaded = '1';
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

// ── Archive / restore bookmark ──────────────────────────────────────────

async function archiveBookmark(id, currentArchived) {
  try {
    const r = await apiFetch(`/api/v1/bookmarks/${id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ archived: !currentArchived }),
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    await loadBookmarks();
  } catch (e) {
    const err = document.getElementById('bm-error');
    err.textContent = `Archive failed: ${e.message}`;
    err.hidden = false;
  }
}

// ── Sort helpers ────────────────────────────────────────────────────────

function _bmSortBy(col) {
  if (_bmSortCol === col) {
    _bmSortDir = _bmSortDir === 'asc' ? 'desc' : 'asc';
  } else {
    _bmSortCol = col;
    _bmSortDir = 'asc';
  }
  renderBookmarks();
}

function _bmSortVal(b, col) {
  if (col === 'tags') return (b.tags || []).join(',').toLowerCase();
  const v = b[col];
  return v !== null && v !== undefined ? String(v).toLowerCase() : '';
}

function _bmUpdateSortHeaders() {
  document.querySelectorAll('#bm-main-view .bm-sort-arrow').forEach(span => {
    const col = span.dataset.col;
    if (col === _bmSortCol) {
      span.textContent = _bmSortDir === 'asc' ? ' \u2191' : ' \u2193';
      span.classList.add('active');
    } else {
      span.textContent = '\u21C5';
      span.classList.remove('active');
    }
  });
}

// ── Column resize ───────────────────────────────────────────────────────

function _bmInitColResize() {
  if (_bmColResizeDone) return;
  const table = document.querySelector('#bm-main-view table');
  if (!table) return;
  _bmColResizeDone = true;
  table.querySelectorAll('thead th').forEach(th => {
    const resizer = document.createElement('div');
    resizer.className = 'bm-col-resize';
    th.appendChild(resizer);
    // Prevent resize click from triggering column sort
    resizer.addEventListener('click', e => { e.preventDefault(); e.stopPropagation(); });
    let startX = 0, startW = 0;
    resizer.addEventListener('mousedown', e => {
      e.preventDefault();
      e.stopPropagation();
      startX = e.clientX;
      startW = th.offsetWidth;
      resizer.classList.add('dragging');
      const onMove = ev => {
        const w = Math.max(40, startW + ev.clientX - startX);
        th.style.width = w + 'px';
      };
      const onUp = () => {
        resizer.classList.remove('dragging');
        document.removeEventListener('mousemove', onMove);
        document.removeEventListener('mouseup', onUp);
      };
      document.addEventListener('mousemove', onMove);
      document.addEventListener('mouseup', onUp);
    });
  });
}

// ── Auto-archive dead links ─────────────────────────────────────────────

async function _bmAutoArchiveDead(btn) {
  const panel = document.getElementById('bm-deadlink-panel');
  const statusEl = document.getElementById('bm-deadlink-status');
  const resultsEl = document.getElementById('bm-deadlink-results');
  const total = _bookmarks.length;
  statusEl.textContent = `Checking ${total} bookmark${total === 1 ? '' : 's'} for dead links\u2026 (may take a minute)`;
  statusEl.style.color = 'var(--text-dim)';
  resultsEl.textContent = '';
  panel.style.display = '';
  btn.disabled = true;
  const orig = btn.textContent;
  btn.textContent = '\u27F3 Checking\u2026';
  try {
    const r = await apiFetch('/api/v1/bookmarks/check-dead-links', { method: 'POST' });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const data = await r.json();
    statusEl.textContent = `Done \u2014 checked ${data.checked}`;
    statusEl.style.color = 'var(--text)';
    if (data.archived > 0) {
      resultsEl.innerHTML = ` \u00B7 <span style="color:var(--warn)">${data.archived} dead link${data.archived === 1 ? '' : 's'} archived</span>`;
      if (data.errors > 0) resultsEl.innerHTML += ` \u00B7 <span style="color:var(--text-dim)">${data.errors} error${data.errors === 1 ? '' : 's'}</span>`;
      await loadBookmarks();
    } else {
      resultsEl.innerHTML = ` \u00B7 <span style="color:var(--ok)">no dead links found</span>`;
      if (data.errors > 0) resultsEl.innerHTML += ` \u00B7 <span style="color:var(--text-dim)">${data.errors} error${data.errors === 1 ? '' : 's'}</span>`;
    }
  } catch (e) {
    statusEl.textContent = `Check failed: ${e.message}`;
    statusEl.style.color = 'var(--err)';
    resultsEl.textContent = '';
  } finally {
    btn.disabled = false;
    btn.textContent = orig;
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
    if (!r.ok) { const d = await r.json().catch(() => ({})); const det = d.detail; const msg = Array.isArray(det) ? `Validation error: ${det[0]?.msg || JSON.stringify(det[0])} (and ${det.length - 1} more)` : det || `HTTP ${r.status}`; throw new Error(msg); }
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
      bookmarks.push({ url, title, folder: folder ?? '', tags, description: '', notes: '', favicon_url: '', source: 'import' });
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
